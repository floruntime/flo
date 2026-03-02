//! Barrier Alignment
//!
//! Implements barrier alignment for multi-input operators (Chandy-Lamport).
//!
//! When an operator has multiple inputs, it must wait until ALL inputs
//! have sent a barrier before snapshotting state. Records from inputs that
//! have already sent their barrier are buffered until alignment is complete.

const std = @import("std");
const Allocator = std.mem.Allocator;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;

// =============================================================================
// AlignmentResult
// =============================================================================

pub const AlignmentResult = enum {
    /// All inputs have sent their barrier — proceed with snapshot
    aligned,
    /// Still waiting for barrier from other inputs — buffer records
    buffering,
};

// =============================================================================
// BarrierAligner
// =============================================================================

/// Aligns checkpoint barriers across multiple inputs.
///
/// For single-input operators (Phase 3), this is trivially:
///   - First barrier → aligned immediately.
///
/// For multi-input operators, this will:
///   - Track which inputs have sent barriers
///   - Buffer records from "fast" inputs (barrier already received)
///   - When all inputs aligned → release buffered records + trigger snapshot
pub const BarrierAligner = struct {
    allocator: Allocator,
    num_inputs: u32,
    checkpoint_id: ?u64,
    /// Bitmask of inputs that have sent their barrier
    barrier_received: u64,
    /// Per-input record buffers for aligned inputs
    input_buffers: []RecordBuffer,

    const Self = @This();

    pub const RecordBuffer = struct {
        records: std.ArrayListUnmanaged(ProcessingRecord),

        pub fn init() RecordBuffer {
            return .{ .records = .{} };
        }

        pub fn deinit(self: *RecordBuffer, allocator: Allocator) void {
            self.records.deinit(allocator);
        }

        pub fn append(self: *RecordBuffer, allocator: Allocator, rec: ProcessingRecord) !void {
            try self.records.append(allocator, rec);
        }

        pub fn drain(self: *RecordBuffer) []const ProcessingRecord {
            const items = self.records.items;
            self.records = .{};
            return items;
        }
    };

    pub fn init(allocator: Allocator, num_inputs: u32) !Self {
        if (num_inputs > 64) return error.TooManyInputs;

        const buffers = try allocator.alloc(RecordBuffer, num_inputs);
        for (buffers) |*b| {
            b.* = RecordBuffer.init();
        }

        return .{
            .allocator = allocator,
            .num_inputs = num_inputs,
            .checkpoint_id = null,
            .barrier_received = 0,
            .input_buffers = buffers,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.input_buffers) |*b| {
            b.deinit(self.allocator);
        }
        self.allocator.free(self.input_buffers);
    }

    /// Process a barrier arriving from a specific input channel.
    ///
    /// Returns:
    /// - .aligned: all inputs have sent barriers → snapshot now
    /// - .buffering: still waiting for other inputs
    pub fn processBarrier(self: *Self, input_index: u16, checkpoint_id: u64) !AlignmentResult {
        // New checkpoint — reset alignment state
        if (self.checkpoint_id == null or self.checkpoint_id.? != checkpoint_id) {
            self.reset();
            self.checkpoint_id = checkpoint_id;
        }

        if (input_index >= self.num_inputs) return error.InvalidInputIndex;

        const mask: u64 = @as(u64, 1) << @intCast(input_index);
        self.barrier_received |= mask;

        if (self.isAligned()) {
            return .aligned;
        }
        return .buffering;
    }

    /// Buffer a record from an input that has already sent its barrier.
    /// These records belong to the NEXT checkpoint epoch.
    pub fn bufferRecord(self: *Self, input_index: u16, rec: ProcessingRecord) !void {
        if (input_index >= self.num_inputs) return error.InvalidInputIndex;
        try self.input_buffers[input_index].append(self.allocator, rec);
    }

    /// Check if a specific input has already sent its barrier.
    pub fn hasBarrier(self: *const Self, input_index: u16) bool {
        const mask: u64 = @as(u64, 1) << @intCast(input_index);
        return (self.barrier_received & mask) != 0;
    }

    /// Check if all inputs have sent their barrier.
    pub fn isAligned(self: *const Self) bool {
        const all_mask = (@as(u64, 1) << @intCast(self.num_inputs)) - 1;
        return (self.barrier_received & all_mask) == all_mask;
    }

    /// Drain all buffered records from all inputs (after alignment).
    /// Returns the records in input order. Caller should process these
    /// before continuing with new records.
    pub fn drainBuffered(self: *Self, allocator: Allocator) ![]ProcessingRecord {
        var result: std.ArrayListUnmanaged(ProcessingRecord) = .{};
        errdefer result.deinit(allocator);

        for (self.input_buffers) |*buf| {
            const drained = buf.drain();
            for (drained) |rec| {
                try result.append(allocator, rec);
            }
            // Drain returned the backing array, free it
            allocator.free(drained);
        }

        return try result.toOwnedSlice(allocator);
    }

    /// Reset alignment state for a new checkpoint.
    pub fn reset(self: *Self) void {
        self.checkpoint_id = null;
        self.barrier_received = 0;
        for (self.input_buffers) |*buf| {
            buf.records.clearRetainingCapacity();
        }
    }
};

// =============================================================================
// Tests
// =============================================================================

test "BarrierAligner single input trivial alignment" {
    const allocator = std.testing.allocator;
    var aligner = try BarrierAligner.init(allocator, 1);
    defer aligner.deinit();

    const result = try aligner.processBarrier(0, 1);
    try std.testing.expectEqual(AlignmentResult.aligned, result);
}

test "BarrierAligner two inputs" {
    const allocator = std.testing.allocator;
    var aligner = try BarrierAligner.init(allocator, 2);
    defer aligner.deinit();

    // First input sends barrier
    const r1 = try aligner.processBarrier(0, 1);
    try std.testing.expectEqual(AlignmentResult.buffering, r1);
    try std.testing.expect(aligner.hasBarrier(0));
    try std.testing.expect(!aligner.hasBarrier(1));

    // Second input sends barrier → aligned
    const r2 = try aligner.processBarrier(1, 1);
    try std.testing.expectEqual(AlignmentResult.aligned, r2);
    try std.testing.expect(aligner.isAligned());
}

test "BarrierAligner buffer records" {
    const allocator = std.testing.allocator;
    var aligner = try BarrierAligner.init(allocator, 2);
    defer aligner.deinit();

    // Input 0 sends barrier
    _ = try aligner.processBarrier(0, 1);

    // Buffer a record from input 0 (post-barrier, belongs to next epoch)
    try aligner.bufferRecord(0, ProcessingRecord.init("k", "v", 100));

    // Input 1 sends barrier → aligned
    _ = try aligner.processBarrier(1, 1);

    // Drain buffered records
    const buffered = try aligner.drainBuffered(allocator);
    defer allocator.free(buffered);
    try std.testing.expectEqual(@as(usize, 1), buffered.len);
    try std.testing.expectEqualStrings("k", buffered[0].key);
}

test "BarrierAligner reset on new checkpoint" {
    const allocator = std.testing.allocator;
    var aligner = try BarrierAligner.init(allocator, 2);
    defer aligner.deinit();

    // Checkpoint 1: partial
    _ = try aligner.processBarrier(0, 1);
    try std.testing.expect(aligner.hasBarrier(0));

    // Checkpoint 2: resets state
    _ = try aligner.processBarrier(0, 2);
    try std.testing.expect(aligner.hasBarrier(0));
    try std.testing.expect(!aligner.hasBarrier(1)); // Reset cleared input 1
}
