//! Sink Interface
//!
//! Sinks consume records from the end of a processing pipeline,
//! writing them to external systems (Flo-Streams, Flo-KV, network).

const std = @import("std");
const Allocator = std.mem.Allocator;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;

// =============================================================================
// Sink - Interface via vtable
// =============================================================================

/// Sink interface for writing records out of the processing pipeline.
pub const Sink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Write a record to the sink
        write: *const fn (ptr: *anyopaque, record: ProcessingRecord) anyerror!void,
        /// Flush any buffered data
        flush: *const fn (ptr: *anyopaque) anyerror!void,
        /// Return the sink name
        getName: *const fn (ptr: *anyopaque) []const u8,
        /// Close the sink and release resources
        close: *const fn (ptr: *anyopaque) void,
        /// Notify sink that a checkpoint completed (for two-phase commit sinks).
        notifyCheckpointComplete: *const fn (ptr: *anyopaque, checkpoint_id: u64) anyerror!void,
    };

    pub fn write(self: Sink, rec: ProcessingRecord) !void {
        return self.vtable.write(self.ptr, rec);
    }

    pub fn flush(self: Sink) !void {
        return self.vtable.flush(self.ptr);
    }

    pub fn getName(self: Sink) []const u8 {
        return self.vtable.getName(self.ptr);
    }

    pub fn close(self: Sink) void {
        return self.vtable.close(self.ptr);
    }

    pub fn notifyCheckpointComplete(self: Sink, checkpoint_id: u64) !void {
        return self.vtable.notifyCheckpointComplete(self.ptr, checkpoint_id);
    }
};

// =============================================================================
// CollectingSink - Buffers records in-memory for testing
// =============================================================================

/// A sink that collects all written records into a buffer.
/// Useful for testing processing pipelines end-to-end.
pub const CollectingSink = struct {
    name: []const u8,
    records: std.ArrayList(ProcessingRecord),
    allocator: Allocator,
    flush_count: u64,

    const Self = @This();

    pub fn init(allocator: Allocator, name: []const u8) Self {
        return .{
            .name = name,
            .records = .empty,
            .allocator = allocator,
            .flush_count = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.records.deinit(self.allocator);
    }

    pub fn sink(self: *Self) Sink {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Sink.VTable{
        .write = writeFn,
        .flush = flushFn,
        .getName = getNameFn,
        .close = closeFn,
        .notifyCheckpointComplete = notifyCheckpointCompleteFn,
    };

    fn writeFn(ptr: *anyopaque, rec: ProcessingRecord) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        try self.records.append(self.allocator, rec);
    }

    fn flushFn(ptr: *anyopaque) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.flush_count += 1;
    }

    fn getNameFn(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn closeFn(_: *anyopaque) void {}

    fn notifyCheckpointCompleteFn(_: *anyopaque, _: u64) !void {}

    /// Get collected records (for assertions in tests)
    pub fn collected(self: *const Self) []const ProcessingRecord {
        return self.records.items;
    }

    /// Number of collected records
    pub fn count(self: *const Self) usize {
        return self.records.items.len;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "CollectingSink collects written records" {
    const allocator = std.testing.allocator;
    var cs = CollectingSink.init(allocator, "test-sink");
    defer cs.deinit();
    const s = cs.sink();

    try s.write(ProcessingRecord.init("k1", "v1", 100));
    try s.write(ProcessingRecord.init("k2", "v2", 200));

    try std.testing.expectEqual(@as(usize, 2), cs.count());
    const records = cs.collected();
    try std.testing.expectEqualStrings("k1", records[0].key);
    try std.testing.expectEqualStrings("k2", records[1].key);
}

test "CollectingSink flush increments counter" {
    const allocator = std.testing.allocator;
    var cs = CollectingSink.init(allocator, "test-sink");
    defer cs.deinit();
    const s = cs.sink();

    try s.flush();
    try s.flush();
    try std.testing.expectEqual(@as(u64, 2), cs.flush_count);
}

test "CollectingSink getName" {
    const allocator = std.testing.allocator;
    var cs = CollectingSink.init(allocator, "my-sink");
    defer cs.deinit();
    const s = cs.sink();
    try std.testing.expectEqualStrings("my-sink", s.getName());
}
