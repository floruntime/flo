//! Source Interface
//!
//! Sources produce records by reading from external systems
//! (Flo-Streams, files, network). A source is the entry point
//! of a processing topology.

const std = @import("std");
const Allocator = std.mem.Allocator;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const StreamElement = record_mod.StreamElement;
const CheckpointBarrier = record_mod.CheckpointBarrier;

// =============================================================================
// Source - Interface via vtable
// =============================================================================

/// Source interface for reading records into the processing pipeline.
///
/// Sources are polled by the runtime — each call to poll() returns the
/// next StreamElement (record, watermark, barrier, or end_of_stream).
pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Poll for the next element. Returns null when no data is available yet.
        poll: *const fn (ptr: *anyopaque) anyerror!?StreamElement,
        /// Return the source name
        getName: *const fn (ptr: *anyopaque) []const u8,
        /// Close the source and release resources
        close: *const fn (ptr: *anyopaque) void,
        /// Snapshot source offsets for checkpointing. Returns serialized offset data.
        snapshotOffsets: *const fn (ptr: *anyopaque, allocator: Allocator) anyerror!?[]u8,
        /// Restore source to offsets from a checkpoint.
        restoreOffsets: *const fn (ptr: *anyopaque, data: []const u8) anyerror!void,
        /// Notify source that a checkpoint completed (for offset commit).
        notifyCheckpointComplete: *const fn (ptr: *anyopaque, checkpoint_id: u64) anyerror!void,
    };

    pub fn poll(self: Source) !?StreamElement {
        return self.vtable.poll(self.ptr);
    }

    pub fn getName(self: Source) []const u8 {
        return self.vtable.getName(self.ptr);
    }

    pub fn close(self: Source) void {
        return self.vtable.close(self.ptr);
    }

    pub fn snapshotOffsets(self: Source, allocator: Allocator) !?[]u8 {
        return self.vtable.snapshotOffsets(self.ptr, allocator);
    }

    pub fn restoreOffsets(self: Source, data: []const u8) !void {
        return self.vtable.restoreOffsets(self.ptr, data);
    }

    pub fn notifyCheckpointComplete(self: Source, checkpoint_id: u64) !void {
        return self.vtable.notifyCheckpointComplete(self.ptr, checkpoint_id);
    }
};

// =============================================================================
// SliceSource - In-memory source for testing
// =============================================================================

/// A simple source that emits records from a pre-built slice.
/// Useful for unit testing processing pipelines.
pub const SliceSource = struct {
    name: []const u8,
    records: []const ProcessingRecord,
    position: usize,
    finished: bool,

    const Self = @This();

    pub fn init(name: []const u8, records: []const ProcessingRecord) Self {
        return .{
            .name = name,
            .records = records,
            .position = 0,
            .finished = false,
        };
    }

    pub fn source(self: *Self) Source {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Source.VTable{
        .poll = pollFn,
        .getName = getNameFn,
        .close = closeFn,
        .snapshotOffsets = snapshotOffsetsFn,
        .restoreOffsets = restoreOffsetsFn,
        .notifyCheckpointComplete = notifyCheckpointCompleteFn,
    };

    fn pollFn(ptr: *anyopaque) !?StreamElement {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.position < self.records.len) {
            const rec = self.records[self.position];
            self.position += 1;
            return StreamElement{ .record = rec };
        }
        if (!self.finished) {
            self.finished = true;
            return StreamElement.end_of_stream;
        }
        return null;
    }

    fn getNameFn(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn closeFn(_: *anyopaque) void {}

    fn snapshotOffsetsFn(ptr: *anyopaque, alloc: Allocator) !?[]u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        // Serialize position as u64
        const buf = try alloc.alloc(u8, @sizeOf(u64));
        const pos: u64 = @intCast(self.position);
        @memcpy(buf[0..@sizeOf(u64)], std.mem.asBytes(&pos));
        return buf;
    }

    fn restoreOffsetsFn(ptr: *anyopaque, data: []const u8) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (data.len >= @sizeOf(u64)) {
            const pos = std.mem.bytesToValue(u64, data[0..@sizeOf(u64)]);
            self.position = @intCast(pos);
            self.finished = false;
        }
    }

    fn notifyCheckpointCompleteFn(_: *anyopaque, _: u64) !void {}

    /// Reset to beginning (for re-processing in tests)
    pub fn reset(self: *Self) void {
        self.position = 0;
        self.finished = false;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "SliceSource emits all records then end_of_stream" {
    const records = [_]ProcessingRecord{
        ProcessingRecord.init("k1", "v1", 100),
        ProcessingRecord.init("k2", "v2", 200),
    };

    var src = SliceSource.init("test-source", &records);
    const s = src.source();

    // First two polls return records
    const e1 = (try s.poll()).?;
    try std.testing.expect(e1 == .record);
    try std.testing.expectEqualStrings("k1", e1.record.key);

    const e2 = (try s.poll()).?;
    try std.testing.expect(e2 == .record);
    try std.testing.expectEqualStrings("k2", e2.record.key);

    // Third poll returns end_of_stream
    const e3 = (try s.poll()).?;
    try std.testing.expect(e3 == .end_of_stream);

    // Fourth poll returns null
    const e4 = try s.poll();
    try std.testing.expect(e4 == null);
}

test "SliceSource snapshot and restore" {
    const records = [_]ProcessingRecord{
        ProcessingRecord.init("k1", "v1", 100),
        ProcessingRecord.init("k2", "v2", 200),
        ProcessingRecord.init("k3", "v3", 300),
    };

    var src = SliceSource.init("test-source", &records);
    const s = src.source();

    // Consume two records
    _ = try s.poll();
    _ = try s.poll();

    // Snapshot
    const snap = try s.snapshotOffsets(std.testing.allocator);
    try std.testing.expect(snap != null);
    defer std.testing.allocator.free(snap.?);

    // Restore into a new source
    var src2 = SliceSource.init("test-source-2", &records);
    const s2 = src2.source();
    try s2.restoreOffsets(snap.?);

    // Should resume from position 2
    const e = (try s2.poll()).?;
    try std.testing.expect(e == .record);
    try std.testing.expectEqualStrings("k3", e.record.key);
}
