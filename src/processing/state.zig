//! Keyed State Management
//!
//! Provides Flink-compatible state abstractions for stateful operators.
//! Backed by an in-memory HashMap per shard (thread-per-shard = no locks).
//!
//! Key Schema (namespaced isolation):
//!   proc:{job_id}:{operator_id}:kv:{user_key}          — ValueState
//!   proc:{job_id}:{operator_id}:list:{user_key}:{idx}  — ListState
//!   proc:{job_id}:{operator_id}:map:{user_key}:{mk}    — MapState
//!
//! Each operator instance gets isolated state through key prefixing.
//! No mutex needed — thread-per-shard means single-threaded access.

const std = @import("std");
const Allocator = std.mem.Allocator;

// =============================================================================
// StateBackend - Simple in-memory HashMap state backend
// =============================================================================

/// In-memory state backend backed by a HashMap.
/// Each shard gets its own StateBackend instance (no cross-shard sharing).
pub const StateBackend = struct {
    allocator: Allocator,
    store: std.StringHashMap([]u8),

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .store = std.StringHashMap([]u8).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.store.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.store.deinit();
    }

    pub fn get(self: *const Self, key: []const u8) ?[]const u8 {
        return self.store.get(key);
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        const gop = try self.store.getOrPut(key);
        if (gop.found_existing) {
            self.allocator.free(gop.value_ptr.*);
        } else {
            gop.key_ptr.* = try self.allocator.dupe(u8, key);
        }
        gop.value_ptr.* = try self.allocator.dupe(u8, value);
    }

    pub fn delete(self: *Self, key: []const u8) void {
        if (self.store.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
            self.allocator.free(kv.value);
        }
    }

    pub fn count(self: *const Self) usize {
        return self.store.count();
    }
};

// =============================================================================
// KeyedStateAccess - Scoped accessor for operator state
// =============================================================================

/// Provides an operator with access to keyed state.
///
/// Each operator gets a KeyedStateAccess scoped to its job + operator ID.
/// All state keys are automatically prefixed, ensuring isolation between
/// operators and jobs sharing the same StateBackend.
///
/// Usage:
///   var state = KeyedStateAccess.init(allocator, backend, "my-job", "word-count");
///   var counter = state.getValueState();
///   try counter.put("hello", "5");
pub const KeyedStateAccess = struct {
    allocator: Allocator,
    backend: *StateBackend,
    /// Prefix: "proc:{job_id}:{operator_id}:"
    prefix: []const u8,

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        backend: *StateBackend,
        job_id: []const u8,
        operator_id: []const u8,
    ) !Self {
        const prefix = try std.fmt.allocPrint(allocator, "proc:{s}:{s}:", .{ job_id, operator_id });
        return .{
            .allocator = allocator,
            .backend = backend,
            .prefix = prefix,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.prefix);
    }

    /// Get a ValueState handle for simple key-value state
    pub fn getValueState(self: *Self) ValueState {
        return ValueState{
            .backend = self.backend,
            .prefix = self.prefix,
            .allocator = self.allocator,
        };
    }

    /// Get a ListState handle for append-only list state
    pub fn getListState(self: *Self) ListState {
        return ListState{
            .backend = self.backend,
            .prefix = self.prefix,
            .allocator = self.allocator,
        };
    }

    /// Get a MapState handle for nested key-value state
    pub fn getMapState(self: *Self) MapState {
        return MapState{
            .backend = self.backend,
            .prefix = self.prefix,
            .allocator = self.allocator,
        };
    }
};

// =============================================================================
// ValueState - Single value per key (like Flink's ValueState<T>)
// =============================================================================

/// Single-value keyed state.
///
/// Stored as: `{prefix}kv:{user_key}` → value bytes
pub const ValueState = struct {
    backend: *StateBackend,
    prefix: []const u8,
    allocator: Allocator,

    const Self = @This();

    /// Get the value for a key. Returns null if not set.
    pub fn get(self: *const Self, key: []const u8) !?[]const u8 {
        var key_buf: [512]u8 = undefined;
        const full_key = self.formatKey(key, &key_buf) orelse return error.KeyTooLong;
        return self.backend.get(full_key);
    }

    /// Set the value for a key
    pub fn put(self: *const Self, key: []const u8, value: []const u8) !void {
        var key_buf: [512]u8 = undefined;
        const full_key = self.formatKey(key, &key_buf) orelse return error.KeyTooLong;
        try self.backend.put(full_key, value);
    }

    /// Delete the value for a key
    pub fn delete(self: *const Self, key: []const u8) !void {
        var key_buf: [512]u8 = undefined;
        const full_key = self.formatKey(key, &key_buf) orelse return error.KeyTooLong;
        self.backend.delete(full_key);
    }

    fn formatKey(self: *const Self, user_key: []const u8, buf: []u8) ?[]const u8 {
        const needed = self.prefix.len + 3 + user_key.len; // "kv:" + key
        if (needed > buf.len) return null;
        @memcpy(buf[0..self.prefix.len], self.prefix);
        @memcpy(buf[self.prefix.len .. self.prefix.len + 3], "kv:");
        @memcpy(buf[self.prefix.len + 3 .. needed], user_key);
        return buf[0..needed];
    }
};

// =============================================================================
// ListState - Append-only list per key (like Flink's ListState<T>)
// =============================================================================

/// Append-only list keyed state.
///
/// Stored as: `{prefix}list:{user_key}:{index}` → element bytes
///            `{prefix}list:{user_key}:_len` → count as string
pub const ListState = struct {
    backend: *StateBackend,
    prefix: []const u8,
    allocator: Allocator,

    const Self = @This();

    /// Append a value to the list for a key
    pub fn add(self: *const Self, key: []const u8, value: []const u8) !void {
        // Read current length
        const len = try self.getLength(key);
        // Write element at index
        var key_buf: [512]u8 = undefined;
        const elem_key = self.formatElemKey(key, len, &key_buf) orelse return error.KeyTooLong;
        try self.backend.put(elem_key, value);
        // Update length
        var len_key_buf: [512]u8 = undefined;
        const len_key = self.formatLenKey(key, &len_key_buf) orelse return error.KeyTooLong;
        var len_buf: [20]u8 = undefined;
        const len_str = std.fmt.bufPrint(&len_buf, "{d}", .{len + 1}) catch unreachable;
        try self.backend.put(len_key, len_str);
    }

    /// Get the number of elements in the list
    pub fn getLength(self: *const Self, key: []const u8) !u64 {
        var len_key_buf: [512]u8 = undefined;
        const len_key = self.formatLenKey(key, &len_key_buf) orelse return error.KeyTooLong;
        const len_str = self.backend.get(len_key) orelse return 0;
        return std.fmt.parseInt(u64, len_str, 10) catch 0;
    }

    /// Get element at index (returns null if out of range)
    pub fn getAt(self: *const Self, key: []const u8, index: u64) !?[]const u8 {
        var key_buf: [512]u8 = undefined;
        const elem_key = self.formatElemKey(key, index, &key_buf) orelse return error.KeyTooLong;
        return self.backend.get(elem_key);
    }

    /// Clear the list for a key (sets length to 0)
    pub fn clear(self: *const Self, key: []const u8) !void {
        var len_key_buf: [512]u8 = undefined;
        const len_key = self.formatLenKey(key, &len_key_buf) orelse return error.KeyTooLong;
        try self.backend.put(len_key, "0");
    }

    fn formatLenKey(self: *const Self, user_key: []const u8, buf: []u8) ?[]const u8 {
        const suffix = ":_len";
        const needed = self.prefix.len + 5 + user_key.len + suffix.len; // "list:" + key + ":_len"
        if (needed > buf.len) return null;
        var pos: usize = 0;
        @memcpy(buf[pos .. pos + self.prefix.len], self.prefix);
        pos += self.prefix.len;
        @memcpy(buf[pos .. pos + 5], "list:");
        pos += 5;
        @memcpy(buf[pos .. pos + user_key.len], user_key);
        pos += user_key.len;
        @memcpy(buf[pos .. pos + suffix.len], suffix);
        pos += suffix.len;
        return buf[0..pos];
    }

    fn formatElemKey(self: *const Self, user_key: []const u8, index: u64, buf: []u8) ?[]const u8 {
        // "list:{key}:{index}" — build in tmp to avoid overlap
        var tmp: [512]u8 = undefined;
        var pos: usize = 0;
        @memcpy(tmp[pos .. pos + self.prefix.len], self.prefix);
        pos += self.prefix.len;
        @memcpy(tmp[pos .. pos + 5], "list:");
        pos += 5;
        @memcpy(tmp[pos .. pos + user_key.len], user_key);
        pos += user_key.len;
        tmp[pos] = ':';
        pos += 1;
        const idx_str = std.fmt.bufPrint(tmp[pos..], "{d}", .{index}) catch return null;
        pos += idx_str.len;
        if (pos > buf.len) return null;
        @memcpy(buf[0..pos], tmp[0..pos]);
        return buf[0..pos];
    }
};

// =============================================================================
// MapState - Nested key-value per key (like Flink's MapState<UK, UV>)
// =============================================================================

/// Nested key-value map keyed state.
///
/// Stored as: `{prefix}map:{user_key}:{map_key}` → value bytes
pub const MapState = struct {
    backend: *StateBackend,
    prefix: []const u8,
    allocator: Allocator,

    const Self = @This();

    /// Get a value from the map
    pub fn get(self: *const Self, key: []const u8, map_key: []const u8) !?[]const u8 {
        var key_buf: [512]u8 = undefined;
        const full_key = self.formatKey(key, map_key, &key_buf) orelse return error.KeyTooLong;
        return self.backend.get(full_key);
    }

    /// Put a value in the map
    pub fn put(self: *const Self, key: []const u8, map_key: []const u8, value: []const u8) !void {
        var key_buf: [512]u8 = undefined;
        const full_key = self.formatKey(key, map_key, &key_buf) orelse return error.KeyTooLong;
        try self.backend.put(full_key, value);
    }

    /// Delete a value from the map
    pub fn delete(self: *const Self, key: []const u8, map_key: []const u8) !void {
        var key_buf: [512]u8 = undefined;
        const full_key = self.formatKey(key, map_key, &key_buf) orelse return error.KeyTooLong;
        self.backend.delete(full_key);
    }

    fn formatKey(self: *const Self, user_key: []const u8, map_key: []const u8, buf: []u8) ?[]const u8 {
        // "map:{key}:{map_key}"
        const needed = self.prefix.len + 4 + user_key.len + 1 + map_key.len;
        if (needed > buf.len) return null;
        var pos: usize = 0;
        @memcpy(buf[pos .. pos + self.prefix.len], self.prefix);
        pos += self.prefix.len;
        @memcpy(buf[pos .. pos + 4], "map:");
        pos += 4;
        @memcpy(buf[pos .. pos + user_key.len], user_key);
        pos += user_key.len;
        buf[pos] = ':';
        pos += 1;
        @memcpy(buf[pos .. pos + map_key.len], map_key);
        pos += map_key.len;
        return buf[0..pos];
    }
};

// =============================================================================
// Tests
// =============================================================================

test "ValueState put and get" {
    const allocator = std.testing.allocator;

    var backend = StateBackend.init(allocator);
    defer backend.deinit();

    var ksa = try KeyedStateAccess.init(allocator, &backend, "job1", "op1");
    defer ksa.deinit();

    var vs = ksa.getValueState();
    try vs.put("count", "42");

    const val = try vs.get("count");
    try std.testing.expectEqualStrings("42", val.?);

    // Non-existent key returns null
    const missing = try vs.get("missing");
    try std.testing.expect(missing == null);
}

test "ValueState delete" {
    const allocator = std.testing.allocator;

    var backend = StateBackend.init(allocator);
    defer backend.deinit();

    var ksa = try KeyedStateAccess.init(allocator, &backend, "job1", "op1");
    defer ksa.deinit();

    var vs = ksa.getValueState();
    try vs.put("k", "v");
    try vs.delete("k");

    const val = try vs.get("k");
    try std.testing.expect(val == null);
}

test "ValueState isolation between operators" {
    const allocator = std.testing.allocator;

    var backend = StateBackend.init(allocator);
    defer backend.deinit();

    var ksa1 = try KeyedStateAccess.init(allocator, &backend, "job1", "op-A");
    defer ksa1.deinit();
    var ksa2 = try KeyedStateAccess.init(allocator, &backend, "job1", "op-B");
    defer ksa2.deinit();

    var vs1 = ksa1.getValueState();
    var vs2 = ksa2.getValueState();

    try vs1.put("x", "from-A");
    try vs2.put("x", "from-B");

    const a = try vs1.get("x");
    try std.testing.expectEqualStrings("from-A", a.?);

    const b = try vs2.get("x");
    try std.testing.expectEqualStrings("from-B", b.?);
}

test "ListState add and get" {
    const allocator = std.testing.allocator;

    var backend = StateBackend.init(allocator);
    defer backend.deinit();

    var ksa = try KeyedStateAccess.init(allocator, &backend, "job1", "op1");
    defer ksa.deinit();

    var ls = ksa.getListState();
    try ls.add("events", "a");
    try ls.add("events", "b");
    try ls.add("events", "c");

    const len = try ls.getLength("events");
    try std.testing.expectEqual(@as(u64, 3), len);

    const e0 = try ls.getAt("events", 0);
    try std.testing.expectEqualStrings("a", e0.?);

    const e2 = try ls.getAt("events", 2);
    try std.testing.expectEqualStrings("c", e2.?);
}

test "ListState clear" {
    const allocator = std.testing.allocator;

    var backend = StateBackend.init(allocator);
    defer backend.deinit();

    var ksa = try KeyedStateAccess.init(allocator, &backend, "job1", "op1");
    defer ksa.deinit();

    var ls = ksa.getListState();
    try ls.add("k", "x");
    try ls.clear("k");

    const len = try ls.getLength("k");
    try std.testing.expectEqual(@as(u64, 0), len);
}

test "MapState put and get" {
    const allocator = std.testing.allocator;

    var backend = StateBackend.init(allocator);
    defer backend.deinit();

    var ksa = try KeyedStateAccess.init(allocator, &backend, "job1", "op1");
    defer ksa.deinit();

    var ms = ksa.getMapState();
    try ms.put("user:42", "name", "Alice");
    try ms.put("user:42", "age", "30");

    const name = try ms.get("user:42", "name");
    try std.testing.expectEqualStrings("Alice", name.?);

    const age = try ms.get("user:42", "age");
    try std.testing.expectEqualStrings("30", age.?);

    // Non-existent map key
    const missing = try ms.get("user:42", "email");
    try std.testing.expect(missing == null);
}

test "MapState delete" {
    const allocator = std.testing.allocator;

    var backend = StateBackend.init(allocator);
    defer backend.deinit();

    var ksa = try KeyedStateAccess.init(allocator, &backend, "job1", "op1");
    defer ksa.deinit();

    var ms = ksa.getMapState();
    try ms.put("k", "mk", "v");
    try ms.delete("k", "mk");

    const val = try ms.get("k", "mk");
    try std.testing.expect(val == null);
}
