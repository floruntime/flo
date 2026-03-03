//! Output Collector
//!
//! Collects records emitted by operators and forwards them downstream.
//! Operators call collector.emit() to send records downstream.
//! The collector buffers records for the chain to consume.

const std = @import("std");
const Allocator = std.mem.Allocator;
const record_mod = @import("record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;

// =============================================================================
// OutputCollector - Buffered downstream record emission
// =============================================================================

/// Collects records emitted by an operator during processElement.
///
/// After processElement returns, the chain reads collector.records
/// and feeds them to the next operator. The collector is cleared
/// before each processElement call.
pub const OutputCollector = struct {
    /// Buffered output records (main output)
    records: std.ArrayList(ProcessingRecord),
    /// Allocator for output buffer management
    allocator: Allocator,
    /// Number of records emitted since last reset
    emit_count: u64,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .records = .{},
            .allocator = allocator,
            .emit_count = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.clearRetained();
        self.records.deinit(self.allocator);
    }

    /// Emit a record to the main output
    pub fn emit(self: *Self, rec: ProcessingRecord) !void {
        try self.records.append(self.allocator, rec);
        self.emit_count += 1;
    }

    /// Emit a record with a new key (re-keying for downstream keyBy)
    pub fn emitWithKey(self: *Self, key: []const u8, value: []const u8, event_time_ms: i64) !void {
        try self.records.append(self.allocator, ProcessingRecord.init(key, value, event_time_ms));
        self.emit_count += 1;
    }

    /// Clear the output buffer for the next operator invocation.
    pub fn clear(self: *Self) void {
        self.clearRetained();
        self.records.clearRetainingCapacity();
    }

    /// Drain: return collected records and reset.
    /// Caller borrows the slice — valid until next emit/clear.
    pub fn drain(self: *Self) []const ProcessingRecord {
        return self.records.items;
    }

    /// Number of records currently buffered
    pub fn count(self: *const Self) usize {
        return self.records.items.len;
    }

    fn clearRetained(self: *Self) void {
        for (self.records.items) |rec| {
            if (rec.owns_memory) {
                rec.deinit(self.allocator);
            }
        }
    }
};

// =============================================================================
// Tests
// =============================================================================

test "OutputCollector emit and drain" {
    const allocator = std.testing.allocator;
    var collector = OutputCollector.init(allocator);
    defer collector.deinit();

    try collector.emit(ProcessingRecord.init("k1", "v1", 100));
    try collector.emit(ProcessingRecord.init("k2", "v2", 200));

    try std.testing.expectEqual(@as(usize, 2), collector.count());

    const records = collector.drain();
    try std.testing.expectEqual(@as(usize, 2), records.len);
    try std.testing.expectEqualStrings("k1", records[0].key);
    try std.testing.expectEqualStrings("k2", records[1].key);
}

test "OutputCollector clear resets buffer" {
    const allocator = std.testing.allocator;
    var collector = OutputCollector.init(allocator);
    defer collector.deinit();

    try collector.emit(ProcessingRecord.init("k", "v", 0));
    try std.testing.expectEqual(@as(usize, 1), collector.count());

    collector.clear();
    try std.testing.expectEqual(@as(usize, 0), collector.count());
}

test "OutputCollector emitWithKey" {
    const allocator = std.testing.allocator;
    var collector = OutputCollector.init(allocator);
    defer collector.deinit();

    try collector.emitWithKey("user:42", "payload", 5000);
    const records = collector.drain();
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqualStrings("user:42", records[0].key);
    try std.testing.expectEqualStrings("payload", records[0].value);
    try std.testing.expectEqual(@as(i64, 5000), records[0].event_time_ms);
}
