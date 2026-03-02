//! Side Output Manager
//!
//! Enables operators to emit records to named secondary output channels
//! in addition to the main output. This is Flink's OutputTag equivalent.
//!
//! Side outputs are used for:
//! - Late data routing (records arriving after window close)
//! - Error/dead-letter routing
//! - Multi-way splits from a single operator
//!
//! Usage in an operator:
//!   try ctx.sideOutput("late-data", record);
//!   try ctx.sideOutput("errors", error_record);

const std = @import("std");
const Allocator = std.mem.Allocator;
const record_mod = @import("record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;

// =============================================================================
// OutputTag - Named side output identifier (like Flink's OutputTag<T>)
// =============================================================================

/// Identifies a named side output channel.
pub const OutputTag = struct {
    name: []const u8,

    pub fn init(name: []const u8) OutputTag {
        return .{ .name = name };
    }

    pub fn eql(self: OutputTag, other: OutputTag) bool {
        return std.mem.eql(u8, self.name, other.name);
    }
};

// =============================================================================
// SideOutputManager
// =============================================================================

/// Manages named side output channels.
///
/// Operators emit to side outputs by name. The chain reads all side
/// outputs after processElement returns and routes them to the correct
/// downstream topology.
pub const SideOutputManager = struct {
    allocator: Allocator,
    /// Tag name → buffered records
    channels: std.StringHashMap(std.ArrayList(ProcessingRecord)),
    /// Total records emitted across all side outputs
    total_emitted: u64,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .channels = std.StringHashMap(std.ArrayList(ProcessingRecord)).init(allocator),
            .total_emitted = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.channels.iterator();
        while (it.next()) |entry| {
            for (entry.value_ptr.items) |rec| {
                if (rec.owns_memory) rec.deinit(self.allocator);
            }
            entry.value_ptr.deinit(self.allocator);
        }
        self.channels.deinit();
    }

    /// Emit a record to a named side output channel.
    pub fn emit(self: *Self, tag_name: []const u8, rec: ProcessingRecord) !void {
        const gop = try self.channels.getOrPut(tag_name);
        if (!gop.found_existing) {
            gop.value_ptr.* = .empty;
        }
        try gop.value_ptr.append(self.allocator, rec);
        self.total_emitted += 1;
    }

    /// Get all records emitted to a specific side output.
    /// Returns null if no records were emitted to that tag.
    pub fn getOutput(self: *const Self, tag_name: []const u8) ?[]const ProcessingRecord {
        if (self.channels.get(tag_name)) |list| {
            if (list.items.len == 0) return null;
            return list.items;
        }
        return null;
    }

    /// Get the number of records in a side output channel.
    pub fn outputCount(self: *const Self, tag_name: []const u8) usize {
        if (self.channels.get(tag_name)) |list| {
            return list.items.len;
        }
        return 0;
    }

    /// Clear all side output buffers (called between processElement invocations).
    pub fn clear(self: *Self) void {
        var it = self.channels.iterator();
        while (it.next()) |entry| {
            for (entry.value_ptr.items) |rec| {
                if (rec.owns_memory) rec.deinit(self.allocator);
            }
            entry.value_ptr.clearRetainingCapacity();
        }
    }

    /// Get a list of all tag names that have output.
    pub fn activeTagNames(self: *const Self, allocator: Allocator) ![][]const u8 {
        var names: std.ArrayListUnmanaged([]const u8) = .{};
        var it = self.channels.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.items.len > 0) {
                try names.append(allocator, entry.key_ptr.*);
            }
        }
        return try names.toOwnedSlice(allocator);
    }

    /// Total records emitted across all side outputs since creation.
    pub fn totalEmitted(self: *const Self) u64 {
        return self.total_emitted;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "SideOutputManager emit and retrieve" {
    const allocator = std.testing.allocator;
    var som = SideOutputManager.init(allocator);
    defer som.deinit();

    try som.emit("late-data", ProcessingRecord.init("k1", "v1", 100));
    try som.emit("errors", ProcessingRecord.init("k2", "err", 200));
    try som.emit("late-data", ProcessingRecord.init("k3", "v3", 300));

    try std.testing.expectEqual(@as(usize, 2), som.outputCount("late-data"));
    try std.testing.expectEqual(@as(usize, 1), som.outputCount("errors"));
    try std.testing.expectEqual(@as(usize, 0), som.outputCount("nonexistent"));
    try std.testing.expectEqual(@as(u64, 3), som.totalEmitted());

    const late = som.getOutput("late-data").?;
    try std.testing.expectEqualStrings("k1", late[0].key);
    try std.testing.expectEqualStrings("k3", late[1].key);
}

test "SideOutputManager clear resets buffers" {
    const allocator = std.testing.allocator;
    var som = SideOutputManager.init(allocator);
    defer som.deinit();

    try som.emit("out", ProcessingRecord.init("k", "v", 0));
    try std.testing.expectEqual(@as(usize, 1), som.outputCount("out"));

    som.clear();
    try std.testing.expectEqual(@as(usize, 0), som.outputCount("out"));

    // Total emitted is cumulative (not reset)
    try std.testing.expectEqual(@as(u64, 1), som.totalEmitted());
}

test "SideOutputManager activeTagNames" {
    const allocator = std.testing.allocator;
    var som = SideOutputManager.init(allocator);
    defer som.deinit();

    try som.emit("alpha", ProcessingRecord.init("k", "v", 0));
    try som.emit("beta", ProcessingRecord.init("k", "v", 0));

    const names = try som.activeTagNames(allocator);
    defer allocator.free(names);

    try std.testing.expectEqual(@as(usize, 2), names.len);
}
