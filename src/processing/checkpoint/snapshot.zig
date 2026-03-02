//! Checkpoint Snapshot
//!
//! Captures and restores a complete processing checkpoint: all operator
//! states plus source offsets. This is the unit of checkpoint data that
//! gets persisted and restored.
//!
//! Workflow:
//!   capture() → iterate operators → snapshotState each → collect results
//!   restore() → iterate operators → restoreState each → reset source offsets

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("../operator.zig").Operator;
const Source = @import("../endpoints/source.zig").Source;

// =============================================================================
// OperatorStateEntry
// =============================================================================

/// Snapshot of a single operator's state at checkpoint time.
pub const OperatorStateEntry = struct {
    /// Operator name (identifies which operator this state belongs to)
    operator_name: []const u8,
    /// Serialized state data (null = stateless operator, no data)
    state_data: ?[]u8,

    pub fn deinit(self: *OperatorStateEntry, allocator: Allocator) void {
        if (self.state_data) |d| allocator.free(d);
        allocator.free(self.operator_name);
    }
};

// =============================================================================
// CheckpointSnapshot
// =============================================================================

/// A complete snapshot of processing state at a checkpoint boundary.
///
/// Contains:
/// - Operator states: serialized state from each operator's snapshotState()
/// - Source offsets: serialized source positions from source's snapshotOffsets()
///
/// This is the data that gets persisted to CheckpointStore and loaded on recovery.
pub const CheckpointSnapshot = struct {
    checkpoint_id: u64,
    timestamp_ms: i64,
    operator_states: []OperatorStateEntry,
    source_offsets: ?[]u8,
    allocator: Allocator,

    const Self = @This();

    /// Capture a checkpoint snapshot from the current processing state.
    ///
    /// Iterates all operators, calling snapshotState() on each.
    /// Also captures source offsets via snapshotOffsets().
    pub fn capture(
        allocator: Allocator,
        checkpoint_id: u64,
        operators: []const Operator,
        source: ?Source,
    ) !Self {
        const timestamp_ms = std.time.milliTimestamp();

        // Snapshot each operator
        var states: std.ArrayListUnmanaged(OperatorStateEntry) = .{};
        errdefer {
            for (states.items) |*s| s.deinit(allocator);
            states.deinit(allocator);
        }

        for (operators) |op| {
            const name = try allocator.dupe(u8, op.getName());
            errdefer allocator.free(name);

            const data = try op.snapshotState(checkpoint_id, allocator);

            try states.append(allocator, .{
                .operator_name = name,
                .state_data = data,
            });
        }

        // Snapshot source offsets
        var offsets: ?[]u8 = null;
        if (source) |src| {
            offsets = try src.snapshotOffsets(allocator);
        }

        return .{
            .checkpoint_id = checkpoint_id,
            .timestamp_ms = timestamp_ms,
            .operator_states = try states.toOwnedSlice(allocator),
            .source_offsets = offsets,
            .allocator = allocator,
        };
    }

    /// Restore operator states and source offsets from this snapshot.
    pub fn restore(
        self: *const Self,
        operators: []const Operator,
        source: ?Source,
    ) !void {
        // Restore operator states by matching name
        for (self.operator_states) |entry| {
            if (entry.state_data) |data| {
                // Find matching operator
                for (operators) |op| {
                    if (std.mem.eql(u8, op.getName(), entry.operator_name)) {
                        try op.restoreState(self.checkpoint_id, data);
                        break;
                    }
                }
            }
        }

        // Restore source offsets
        if (self.source_offsets) |offsets| {
            if (source) |src| {
                try src.restoreOffsets(offsets);
            }
        }
    }

    /// Get the number of stateful operators in this snapshot.
    pub fn statefulCount(self: *const Self) usize {
        var count: usize = 0;
        for (self.operator_states) |entry| {
            if (entry.state_data != null) count += 1;
        }
        return count;
    }

    pub fn deinit(self: *Self) void {
        for (self.operator_states) |*entry| {
            var e = entry.*;
            e.deinit(self.allocator);
        }
        self.allocator.free(self.operator_states);
        if (self.source_offsets) |o| self.allocator.free(o);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "CheckpointSnapshot captures stateless operators" {
    const allocator = std.testing.allocator;
    const PassthroughOperator = @import("../operators/passthrough.zig").PassthroughOperator;

    var pass_op = PassthroughOperator.init("test-pass");
    const ops = [_]Operator{pass_op.operator()};

    var snapshot = try CheckpointSnapshot.capture(allocator, 1, &ops, null);
    defer snapshot.deinit();

    try std.testing.expectEqual(@as(u64, 1), snapshot.checkpoint_id);
    try std.testing.expectEqual(@as(usize, 1), snapshot.operator_states.len);
    // Stateless operator → null state
    try std.testing.expect(snapshot.operator_states[0].state_data == null);
    try std.testing.expectEqual(@as(usize, 0), snapshot.statefulCount());
}

test "CheckpointSnapshot captures source offsets" {
    const allocator = std.testing.allocator;
    const SliceSource = @import("../endpoints/source.zig").SliceSource;
    const ProcessingRecord = @import("../record.zig").ProcessingRecord;

    const records = [_]ProcessingRecord{
        ProcessingRecord.init("k1", "v1", 10),
        ProcessingRecord.init("k2", "v2", 20),
    };
    var src = SliceSource.init("test-src", &records);

    // Advance source past first record
    _ = try src.source().poll();

    var snapshot = try CheckpointSnapshot.capture(allocator, 42, &.{}, src.source());
    defer snapshot.deinit();

    try std.testing.expectEqual(@as(u64, 42), snapshot.checkpoint_id);
    try std.testing.expect(snapshot.source_offsets != null);

    // Source offset should be 8 bytes (u64 serialization from SliceSource)
    try std.testing.expectEqual(@as(usize, 8), snapshot.source_offsets.?.len);
}
