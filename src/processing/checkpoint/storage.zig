//! Checkpoint Storage
//!
//! Stores and retrieves checkpoint metadata and operator state snapshots.
//! In-memory implementation with well-defined key schema.
//!
//! Key schema (logical):
//!   checkpoint:meta:{checkpoint_id}                    → CheckpointMeta
//!   checkpoint:state:{checkpoint_id}:{operator_name}   → operator state bytes
//!   checkpoint:offsets:{checkpoint_id}                  → source offset bytes
//!   checkpoint:latest                                   → latest completed checkpoint_id
//!
//! Production persistence to Flo-KV will be wired through the storage layer
//! once the UAL/projection pipeline is in place.

const std = @import("std");
const Allocator = std.mem.Allocator;

// =============================================================================
// CheckpointStatus
// =============================================================================

pub const CheckpointStatus = enum(u8) {
    in_progress = 0,
    completed = 1,
    failed = 2,
};

// =============================================================================
// CheckpointMeta
// =============================================================================

/// Metadata for a single checkpoint instance.
pub const CheckpointMeta = struct {
    checkpoint_id: u64,
    timestamp_ms: i64,
    status: CheckpointStatus,
    /// Number of operators that have acknowledged this checkpoint
    acked_operators: u32,
    /// Total operators expected to acknowledge
    total_operators: u32,
    /// Whether source offsets have been saved
    offsets_saved: bool,

    pub fn isComplete(self: *const CheckpointMeta) bool {
        return self.acked_operators >= self.total_operators and self.offsets_saved;
    }

    /// Serialize to bytes
    pub fn serialize(self: *const CheckpointMeta, allocator: Allocator) ![]u8 {
        const size = @sizeOf(u64) + @sizeOf(i64) + @sizeOf(u8) + @sizeOf(u32) * 2 + @sizeOf(u8);
        const buf = try allocator.alloc(u8, size);
        var pos: usize = 0;

        @memcpy(buf[pos..][0..@sizeOf(u64)], std.mem.asBytes(&self.checkpoint_id));
        pos += @sizeOf(u64);
        @memcpy(buf[pos..][0..@sizeOf(i64)], std.mem.asBytes(&self.timestamp_ms));
        pos += @sizeOf(i64);
        buf[pos] = @intFromEnum(self.status);
        pos += 1;
        @memcpy(buf[pos..][0..@sizeOf(u32)], std.mem.asBytes(&self.acked_operators));
        pos += @sizeOf(u32);
        @memcpy(buf[pos..][0..@sizeOf(u32)], std.mem.asBytes(&self.total_operators));
        pos += @sizeOf(u32);
        buf[pos] = if (self.offsets_saved) 1 else 0;

        return buf;
    }

    /// Deserialize from bytes
    pub fn deserialize(data: []const u8) ?CheckpointMeta {
        const min_size = @sizeOf(u64) + @sizeOf(i64) + 1 + @sizeOf(u32) * 2 + 1;
        if (data.len < min_size) return null;

        var pos: usize = 0;
        const checkpoint_id = std.mem.bytesToValue(u64, data[pos..][0..@sizeOf(u64)]);
        pos += @sizeOf(u64);
        const timestamp_ms = std.mem.bytesToValue(i64, data[pos..][0..@sizeOf(i64)]);
        pos += @sizeOf(i64);
        const status: CheckpointStatus = @enumFromInt(data[pos]);
        pos += 1;
        const acked = std.mem.bytesToValue(u32, data[pos..][0..@sizeOf(u32)]);
        pos += @sizeOf(u32);
        const total = std.mem.bytesToValue(u32, data[pos..][0..@sizeOf(u32)]);
        pos += @sizeOf(u32);
        const offsets_saved = data[pos] != 0;

        return .{
            .checkpoint_id = checkpoint_id,
            .timestamp_ms = timestamp_ms,
            .status = status,
            .acked_operators = acked,
            .total_operators = total,
            .offsets_saved = offsets_saved,
        };
    }
};

// =============================================================================
// CheckpointStore - In-memory checkpoint storage
// =============================================================================

/// In-memory checkpoint storage.
///
/// Stores checkpoint metadata, operator state snapshots, and source offsets.
/// Will be backed by Flo-KV persistence once the UAL/projection pipeline
/// is wired — for now, purely in-memory is sufficient for processing
/// checkpoint correctness.
pub const CheckpointStore = struct {
    allocator: Allocator,
    /// Keyed by "{checkpoint_id}" → CheckpointMeta (serialized)
    meta_store: std.StringHashMap([]u8),
    /// Keyed by "{checkpoint_id}:{operator_name}" → operator state bytes
    state_store: std.StringHashMap([]u8),
    /// Keyed by "{checkpoint_id}" → source offset bytes
    offset_store: std.StringHashMap([]u8),
    /// Latest completed checkpoint ID
    latest_completed: ?u64,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .meta_store = std.StringHashMap([]u8).init(allocator),
            .state_store = std.StringHashMap([]u8).init(allocator),
            .offset_store = std.StringHashMap([]u8).init(allocator),
            .latest_completed = null,
        };
    }

    /// Convenience alias for tests
    pub fn initForTesting(allocator: Allocator) Self {
        return init(allocator);
    }

    pub fn deinit(self: *Self) void {
        self.freeMap(&self.meta_store);
        self.freeMap(&self.state_store);
        self.freeMap(&self.offset_store);
    }

    fn freeMap(self: *Self, map: *std.StringHashMap([]u8)) void {
        var it = map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        map.deinit();
    }

    // ----- Meta operations -----

    pub fn saveMeta(self: *Self, meta: *const CheckpointMeta) !void {
        const key = try std.fmt.allocPrint(self.allocator, "{d}", .{meta.checkpoint_id});
        errdefer self.allocator.free(key);
        const val = try meta.serialize(self.allocator);
        errdefer self.allocator.free(val);

        // Remove old entry if exists
        if (self.meta_store.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
            self.allocator.free(old.value);
        }
        try self.meta_store.put(key, val);
    }

    pub fn getMeta(self: *Self, checkpoint_id: u64) !?CheckpointMeta {
        const key = try std.fmt.allocPrint(self.allocator, "{d}", .{checkpoint_id});
        defer self.allocator.free(key);
        if (self.meta_store.get(key)) |val| {
            return CheckpointMeta.deserialize(val);
        }
        return null;
    }

    // ----- Operator state operations -----

    pub fn saveOperatorState(self: *Self, checkpoint_id: u64, operator_name: []const u8, data: []const u8) !void {
        const key = try std.fmt.allocPrint(self.allocator, "{d}:{s}", .{ checkpoint_id, operator_name });
        errdefer self.allocator.free(key);
        const val = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(val);

        if (self.state_store.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
            self.allocator.free(old.value);
        }
        try self.state_store.put(key, val);
    }

    pub fn getOperatorState(self: *Self, checkpoint_id: u64, operator_name: []const u8) !?[]const u8 {
        const key = try std.fmt.allocPrint(self.allocator, "{d}:{s}", .{ checkpoint_id, operator_name });
        defer self.allocator.free(key);
        return self.state_store.get(key);
    }

    // ----- Source offset operations -----

    pub fn saveSourceOffsets(self: *Self, checkpoint_id: u64, data: []const u8) !void {
        const key = try std.fmt.allocPrint(self.allocator, "{d}", .{checkpoint_id});
        errdefer self.allocator.free(key);
        const val = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(val);

        if (self.offset_store.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
            self.allocator.free(old.value);
        }
        try self.offset_store.put(key, val);
    }

    pub fn getSourceOffsets(self: *Self, checkpoint_id: u64) !?[]const u8 {
        const key = try std.fmt.allocPrint(self.allocator, "{d}", .{checkpoint_id});
        defer self.allocator.free(key);
        return self.offset_store.get(key);
    }

    // ----- Completion tracking -----

    pub fn markCompleted(self: *Self, checkpoint_id: u64) !void {
        if (self.latest_completed == null or checkpoint_id > self.latest_completed.?) {
            self.latest_completed = checkpoint_id;
        }
        // Update meta status
        if (try self.getMeta(checkpoint_id)) |meta| {
            var updated = meta;
            updated.status = .completed;
            try self.saveMeta(&updated);
        }
    }

    pub fn getLatestCompleted(self: *const Self) ?u64 {
        return self.latest_completed;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "CheckpointMeta serialize roundtrip" {
    const allocator = std.testing.allocator;
    const meta = CheckpointMeta{
        .checkpoint_id = 42,
        .timestamp_ms = 123456,
        .status = .in_progress,
        .acked_operators = 2,
        .total_operators = 5,
        .offsets_saved = true,
    };

    const bytes = try meta.serialize(allocator);
    defer allocator.free(bytes);

    const restored = CheckpointMeta.deserialize(bytes).?;
    try std.testing.expectEqual(@as(u64, 42), restored.checkpoint_id);
    try std.testing.expectEqual(@as(i64, 123456), restored.timestamp_ms);
    try std.testing.expectEqual(CheckpointStatus.in_progress, restored.status);
    try std.testing.expectEqual(@as(u32, 2), restored.acked_operators);
    try std.testing.expectEqual(@as(u32, 5), restored.total_operators);
    try std.testing.expect(restored.offsets_saved);
}

test "CheckpointMeta isComplete" {
    var meta = CheckpointMeta{
        .checkpoint_id = 1,
        .timestamp_ms = 0,
        .status = .in_progress,
        .acked_operators = 2,
        .total_operators = 3,
        .offsets_saved = true,
    };
    try std.testing.expect(!meta.isComplete()); // 2/3 acked

    meta.acked_operators = 3;
    try std.testing.expect(meta.isComplete()); // 3/3 acked + offsets saved

    meta.offsets_saved = false;
    try std.testing.expect(!meta.isComplete()); // offsets not saved
}

test "CheckpointStore save and restore meta" {
    const allocator = std.testing.allocator;
    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();

    const meta = CheckpointMeta{
        .checkpoint_id = 7,
        .timestamp_ms = 99000,
        .status = .in_progress,
        .acked_operators = 0,
        .total_operators = 3,
        .offsets_saved = false,
    };

    try store.saveMeta(&meta);
    const restored = (try store.getMeta(7)).?;
    try std.testing.expectEqual(@as(u64, 7), restored.checkpoint_id);
    try std.testing.expectEqual(@as(i64, 99000), restored.timestamp_ms);
}

test "CheckpointStore operator state" {
    const allocator = std.testing.allocator;
    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();

    try store.saveOperatorState(1, "window-op", "window-state-bytes");
    const state = (try store.getOperatorState(1, "window-op")).?;
    try std.testing.expectEqualStrings("window-state-bytes", state);

    // Non-existent
    const missing = try store.getOperatorState(1, "nonexistent");
    try std.testing.expect(missing == null);
}

test "CheckpointStore source offsets" {
    const allocator = std.testing.allocator;
    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();

    try store.saveSourceOffsets(5, "offset-data");
    const offsets = (try store.getSourceOffsets(5)).?;
    try std.testing.expectEqualStrings("offset-data", offsets);
}

test "CheckpointStore completion tracking" {
    const allocator = std.testing.allocator;
    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();

    try std.testing.expect(store.getLatestCompleted() == null);

    const meta = CheckpointMeta{
        .checkpoint_id = 3,
        .timestamp_ms = 0,
        .status = .in_progress,
        .acked_operators = 0,
        .total_operators = 1,
        .offsets_saved = false,
    };
    try store.saveMeta(&meta);
    try store.markCompleted(3);

    try std.testing.expectEqual(@as(?u64, 3), store.getLatestCompleted());

    // Later checkpoint
    try store.markCompleted(5);
    try std.testing.expectEqual(@as(?u64, 5), store.getLatestCompleted());
}
