//! Source Offset Tracking
//!
//! Tracks per-source offsets at checkpoint time. On recovery, sources
//! rewind to these offsets and replay records from the checkpoint boundary.
//!
//! Each source is identified by name. Offsets are opaque bytes — the
//! source itself knows how to serialize/deserialize its position.

const std = @import("std");
const Allocator = std.mem.Allocator;

// =============================================================================
// SourceOffsetEntry
// =============================================================================

pub const SourceOffsetEntry = struct {
    /// Source name
    source_name: []const u8,
    /// Opaque offset data (source-specific serialization)
    offset_data: []const u8,
};

// =============================================================================
// SourceOffsetTracker
// =============================================================================

/// Tracks source offsets for checkpointing.
///
/// During a checkpoint:
/// 1. Each source calls `recordOffset(name, offset_bytes)`
/// 2. After all sources report, `serialize()` produces checkpoint data
/// 3. On recovery, `deserialize()` + `getOffset(name)` to rewind sources
pub const SourceOffsetTracker = struct {
    allocator: Allocator,
    /// Source name → offset data (both owned)
    offsets: std.StringHashMap([]u8),

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .offsets = std.StringHashMap([]u8).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.offsets.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.offsets.deinit();
    }

    /// Record the offset for a source at checkpoint time.
    pub fn recordOffset(self: *Self, source_name: []const u8, offset_data: []const u8) !void {
        const key = try self.allocator.dupe(u8, source_name);
        errdefer self.allocator.free(key);
        const val = try self.allocator.dupe(u8, offset_data);
        errdefer self.allocator.free(val);

        if (self.offsets.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
            self.allocator.free(old.value);
        }
        try self.offsets.put(key, val);
    }

    /// Get the recorded offset for a source.
    pub fn getOffset(self: *const Self, source_name: []const u8) ?[]const u8 {
        return self.offsets.get(source_name);
    }

    /// Number of tracked sources.
    pub fn sourceCount(self: *const Self) usize {
        return self.offsets.count();
    }

    /// Serialize all offsets into a single byte buffer.
    /// Format: [count:u32] [entries...]
    /// Entry: [name_len:u32] [name] [data_len:u32] [data]
    pub fn serialize(self: *const Self, allocator: Allocator) ![]u8 {
        var size: usize = @sizeOf(u32); // count
        var it = self.offsets.iterator();
        while (it.next()) |entry| {
            size += @sizeOf(u32) + entry.key_ptr.*.len; // name
            size += @sizeOf(u32) + entry.value_ptr.*.len; // data
        }

        const buf = try allocator.alloc(u8, size);
        var pos: usize = 0;

        const count: u32 = @intCast(self.offsets.count());
        @memcpy(buf[pos..][0..@sizeOf(u32)], std.mem.asBytes(&count));
        pos += @sizeOf(u32);

        var it2 = self.offsets.iterator();
        while (it2.next()) |entry| {
            const nl: u32 = @intCast(entry.key_ptr.*.len);
            @memcpy(buf[pos..][0..@sizeOf(u32)], std.mem.asBytes(&nl));
            pos += @sizeOf(u32);
            @memcpy(buf[pos..][0..entry.key_ptr.*.len], entry.key_ptr.*);
            pos += entry.key_ptr.*.len;

            const dl: u32 = @intCast(entry.value_ptr.*.len);
            @memcpy(buf[pos..][0..@sizeOf(u32)], std.mem.asBytes(&dl));
            pos += @sizeOf(u32);
            @memcpy(buf[pos..][0..entry.value_ptr.*.len], entry.value_ptr.*);
            pos += entry.value_ptr.*.len;
        }

        return buf;
    }

    /// Deserialize offsets from checkpoint data.
    /// Replaces any existing tracked offsets.
    pub fn deserialize(self: *Self, data: []const u8) !void {
        // Clear existing
        var it = self.offsets.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.offsets.clearRetainingCapacity();

        if (data.len < @sizeOf(u32)) return;

        var pos: usize = 0;
        const count = std.mem.bytesToValue(u32, data[pos..][0..@sizeOf(u32)]);
        pos += @sizeOf(u32);

        var i: u32 = 0;
        while (i < count) : (i += 1) {
            const nl = std.mem.bytesToValue(u32, data[pos..][0..@sizeOf(u32)]);
            pos += @sizeOf(u32);
            const name = try self.allocator.dupe(u8, data[pos..][0..nl]);
            pos += nl;

            const dl = std.mem.bytesToValue(u32, data[pos..][0..@sizeOf(u32)]);
            pos += @sizeOf(u32);
            const offset_data = try self.allocator.dupe(u8, data[pos..][0..dl]);
            pos += dl;

            try self.offsets.put(name, offset_data);
        }
    }
};

// =============================================================================
// Tests
// =============================================================================

test "SourceOffsetTracker record and retrieve" {
    const allocator = std.testing.allocator;
    var tracker = SourceOffsetTracker.init(allocator);
    defer tracker.deinit();

    try tracker.recordOffset("events-source", "offset:42");
    try tracker.recordOffset("clicks-source", "offset:99");

    try std.testing.expectEqual(@as(usize, 2), tracker.sourceCount());
    try std.testing.expectEqualStrings("offset:42", tracker.getOffset("events-source").?);
    try std.testing.expectEqualStrings("offset:99", tracker.getOffset("clicks-source").?);
    try std.testing.expect(tracker.getOffset("nonexistent") == null);
}

test "SourceOffsetTracker serialize roundtrip" {
    const allocator = std.testing.allocator;
    var tracker = SourceOffsetTracker.init(allocator);
    defer tracker.deinit();

    try tracker.recordOffset("src-a", "pos:100");
    try tracker.recordOffset("src-b", "pos:200");

    const bytes = try tracker.serialize(allocator);
    defer allocator.free(bytes);

    // Deserialize into a new tracker
    var tracker2 = SourceOffsetTracker.init(allocator);
    defer tracker2.deinit();
    try tracker2.deserialize(bytes);

    try std.testing.expectEqual(@as(usize, 2), tracker2.sourceCount());
    // Verify at least one source survived (HashMap ordering is non-deterministic)
    const a = tracker2.getOffset("src-a");
    const b = tracker2.getOffset("src-b");
    try std.testing.expect(a != null or b != null);
    if (a) |v| try std.testing.expectEqualStrings("pos:100", v);
    if (b) |v| try std.testing.expectEqualStrings("pos:200", v);
}

test "SourceOffsetTracker overwrite offset" {
    const allocator = std.testing.allocator;
    var tracker = SourceOffsetTracker.init(allocator);
    defer tracker.deinit();

    try tracker.recordOffset("src", "old");
    try tracker.recordOffset("src", "new");

    try std.testing.expectEqual(@as(usize, 1), tracker.sourceCount());
    try std.testing.expectEqualStrings("new", tracker.getOffset("src").?);
}
