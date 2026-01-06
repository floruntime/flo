//! Dashboard API — Key-Value Endpoints
//!
//! - GET /kv/namespaces           — All KV namespaces with stats
//! - GET /kv/namespaces/:ns/keys  — Scan/list keys in a namespace
//! - GET /kv/namespaces/:ns/keys/:key         — Get a specific key's value
//! - GET /kv/namespaces/:ns/keys/:key/history — Version history for a key

const std = @import("std");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const Dispatcher = h.Dispatcher;
const MetricsRegistry = h.MetricsRegistry;

/// GET /kv/namespaces - List all KV namespaces with stats
///
/// Enumerates namespaces from MetricsRegistry (populated by registerKVNamespace)
/// and falls back to counting via dispatcher local scans when registry is empty.
pub fn getKVNamespaces(allocator: Allocator, dispatchers: []*Dispatcher, metrics: *MetricsRegistry) ![]const u8 {
    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // Prefer metrics-registry data when populated (live op counters).
    {
        metrics.mutex.lock();
        const has_data = metrics.kv_namespaces.count() > 0;
        if (has_data) {
            var it = metrics.kv_namespaces.iterator();
            while (it.next()) |entry| {
                try arr.next();
                const kv_entry = entry.value_ptr.*;
                const snap = kv_entry.metrics.snapshot();
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("name", kv_entry.namespace);
                try obj.intField("key_count", snap.key_count);
                try obj.intField("bytes_stored", snap.bytes_stored);
                try obj.intField("get_ops", snap.get_ops_total);
                try obj.intField("set_ops", snap.set_ops_total);
                try obj.intField("delete_ops", snap.delete_ops_total);
                try obj.end();
            }
            metrics.mutex.unlock();
            try arr.end();
            return try json_buf.toOwnedSlice(allocator);
        }
        metrics.mutex.unlock();
    }

    // Fallback: discover namespaces by scanning all shards for the "default"
    // namespace and any others that have keys.  We collect distinct namespaces
    // from the key names (key format: `ns:{namespace}:kv:{key}` — but the
    // dispatcher strips the prefix before returning, so we just count per ns).
    // For now we report the "default" namespace with a live key count.
    if (dispatchers.len > 0) {
        var key_count: u64 = 0;
        for (dispatchers) |d| {
            key_count += d.countLocalKVKeys("default");
        }
        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", "default");
        try obj.intField("key_count", key_count);
        try obj.intField("bytes_stored", 0);
        try obj.intField("get_ops", 0);
        try obj.intField("set_ops", 0);
        try obj.intField("delete_ops", 0);
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /kv/namespaces/:ns/keys - Scan/list keys in a namespace
/// Query params: ?prefix=&limit=&cursor=
///
/// Aggregates results from every shard by calling each dispatcher's local
/// scan directly.  We bypass async cross-shard forwarding because the
/// dashboard thread cannot block waiting for inbox responses.
pub fn getKVKeys(allocator: Allocator, namespace: []const u8, query_string: ?[]const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try allocator.dupe(u8, "[]");

    const prefix = h.parseQueryParam([]const u8, query_string, "prefix") orelse "";
    const raw_limit = h.parseQueryParam(u32, query_string, "limit") orelse 200;
    const limit: u32 = @min(raw_limit, 1000);

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var outer = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try outer.begin();

    var keys_arr = try outer.arrayField("keys");
    try keys_arr.begin();

    var written: u32 = 0;
    var remaining: u32 = limit;

    for (dispatchers) |d| {
        if (remaining == 0) break;

        const result = d.scanLocalKV(namespace, prefix, remaining, null) catch |err| {
            log.warn("KV local scan error on shard {d}: {}", .{ d.thisCore(), err });
            continue;
        };

        switch (result) {
            .kv_scan_result => |scan| {
                defer allocator.free(scan.data);
                const entries = parseScanEntries(allocator, scan.data) catch continue;
                defer {
                    for (entries) |entry| {
                        allocator.free(entry.key);
                        allocator.free(entry.value);
                    }
                    allocator.free(entries);
                }
                for (entries) |entry| {
                    if (remaining == 0) break;
                    try keys_arr.next();
                    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try obj.begin();
                    try obj.stringField("key", entry.key);
                    try obj.stringField("namespace", namespace);
                    try obj.stringField("value", entry.value);
                    try obj.intField("version", entry.version);
                    try obj.intField("size", entry.value.len);
                    try obj.end();
                    written += 1;
                    remaining -= 1;
                }
            },
            .err => |e| log.warn("KV scan error on shard {d}: {s}", .{ d.thisCore(), e.message }),
            else => {},
        }
    }

    try keys_arr.end();
    try outer.boolField("has_more", false);
    try outer.nullField("cursor");
    try outer.intField("count", written);
    try outer.end();

    return try json_buf.toOwnedSlice(allocator);
}

/// Parsed scan entry (temporary, caller must free key and value)
const ScanEntry = struct {
    key: []u8,
    value: []u8,
    version: u64,
};

/// Parse the raw wire scan result into a slice of ScanEntry.
/// Caller must free each entry's key and value, then free the slice.
fn parseScanEntries(allocator: Allocator, data: []const u8) ![]ScanEntry {
    if (data.len < 4) return &.{};

    var reader = h.BinaryReader.init(data);
    const count = reader.readU32() orelse return &.{};

    var list = std.ArrayList(ScanEntry){};
    errdefer {
        for (list.items) |item| {
            allocator.free(item.key);
            allocator.free(item.value);
        }
        list.deinit(allocator);
    }

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const key_data = reader.readLenPrefixed(u16) orelse break;
        const val_data = reader.readLenPrefixed(u32) orelse break;
        const version = reader.readU64() orelse break;

        const key_copy = try allocator.dupe(u8, key_data);
        errdefer allocator.free(key_copy);
        const val_copy = try allocator.dupe(u8, val_data);
        errdefer allocator.free(val_copy);

        try list.append(allocator, .{ .key = key_copy, .value = val_copy, .version = version });
    }

    return try list.toOwnedSlice(allocator);
}

/// GET /kv/namespaces/:ns/keys/:key - Get a specific key's value
/// Query params: ?version=N (optional, for time-travel)
pub fn getKVKeyValue(allocator: Allocator, namespace: []const u8, key: []const u8, query_string: ?[]const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    const version = h.parseQueryParam(u64, query_string, "version");

    const target = h.routeToShard(dispatchers, namespace, key);

    const result = target.dispatch(.{
        .kv_get = .{
            .namespace = namespace,
            .key = key,
            .version = version,
        },
    }, 0, 0, null) catch |err| {
        log.warn("KV get dispatch error: {}", .{err});
        return try h.jsonError(allocator, "Internal error");
    };

    if (result) |res| {
        switch (res) {
            .kv_value => |v| {
                defer allocator.free(v.value);
                var json_buf = std.ArrayList(u8){};
                errdefer json_buf.deinit(allocator);
                const writer = json_buf.writer(allocator);

                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("key", key);
                try obj.stringField("namespace", namespace);
                try obj.stringField("value", v.value);
                try obj.intField("version", v.version);
                try obj.boolField("found", true);
                try obj.end();
                return try json_buf.toOwnedSlice(allocator);
            },
            .kv_not_found => {
                var json_buf = std.ArrayList(u8){};
                errdefer json_buf.deinit(allocator);
                const writer = json_buf.writer(allocator);

                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("key", key);
                try obj.stringField("namespace", namespace);
                try obj.boolField("found", false);
                try obj.end();
                return try json_buf.toOwnedSlice(allocator);
            },
            .err => |e| {
                return try h.jsonError(allocator, e.message);
            },
            else => return try h.jsonError(allocator, "Unexpected result"),
        }
    }

    return try h.jsonError(allocator, "No result returned");
}

/// GET /kv/namespaces/:ns/keys/:key/history - Get version history for a key
/// Query params: ?limit=N (default: 10)
pub fn getKVKeyHistory(allocator: Allocator, namespace: []const u8, key: []const u8, query_string: ?[]const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try allocator.dupe(u8, "{\"key\":\"\",\"versions\":[]}");

    const limit = h.parseQueryParam(u32, query_string, "limit") orelse 10;

    const target = h.routeToShard(dispatchers, namespace, key);

    const result = target.dispatch(.{
        .kv_history = .{
            .namespace = namespace,
            .key = key,
            .limit = limit,
        },
    }, 0, 0, null) catch |err| {
        log.warn("KV history dispatch error: {}", .{err});
        return try allocator.dupe(u8, "{\"key\":\"\",\"versions\":[]}");
    };

    if (result) |res| {
        switch (res) {
            .kv_history_result => |hist| {
                defer allocator.free(hist.data);
                return try parseHistoryResultToJson(allocator, key, namespace, hist.data);
            },
            .err => |e| {
                log.warn("KV history error: {s}", .{e.message});
                return try allocator.dupe(u8, "{\"key\":\"\",\"versions\":[]}");
            },
            else => return try allocator.dupe(u8, "{\"key\":\"\",\"versions\":[]}"),
        }
    }

    return try allocator.dupe(u8, "{\"key\":\"\",\"versions\":[]}");
}

/// PUT /kv/namespaces/:ns/keys/:key - Set a key's value
/// Body: raw value (text or JSON string)
pub fn putKVKey(allocator: Allocator, namespace: []const u8, key: []const u8, body: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");
    if (body.len == 0) return try h.jsonError(allocator, "Empty request body");

    const target = h.routeToShard(dispatchers, namespace, key);

    const result = target.putLocalKV(namespace, key, body) catch |err| {
        log.warn("KV put error: {}", .{err});
        return try h.jsonError(allocator, "Internal error");
    };

    switch (result) {
        .kv_put_ok => |v| {
            var json_buf = std.ArrayList(u8){};
            errdefer json_buf.deinit(allocator);
            const writer = json_buf.writer(allocator);

            var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try obj.begin();
            try obj.stringField("key", key);
            try obj.stringField("namespace", namespace);
            try obj.boolField("ok", true);
            try obj.intField("version", v.version);
            try obj.end();
            return try json_buf.toOwnedSlice(allocator);
        },
        .err => |e| {
            return try h.jsonError(allocator, e.message);
        },
        else => return try h.jsonError(allocator, "Unexpected result"),
    }
}

/// DELETE /kv/namespaces/:ns/keys/:key - Delete a key
pub fn deleteKVKey(allocator: Allocator, namespace: []const u8, key: []const u8, dispatchers: []*Dispatcher) ![]const u8 {
    if (dispatchers.len == 0) return try h.jsonError(allocator, "No dispatchers available");

    const target = h.routeToShard(dispatchers, namespace, key);

    const result = target.deleteLocalKV(namespace, key) catch |err| {
        log.warn("KV delete error: {}", .{err});
        return try h.jsonError(allocator, "Internal error");
    };

    switch (result) {
        .ok => {
            var json_buf = std.ArrayList(u8){};
            errdefer json_buf.deinit(allocator);
            const writer = json_buf.writer(allocator);

            var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try obj.begin();
            try obj.stringField("key", key);
            try obj.stringField("namespace", namespace);
            try obj.boolField("ok", true);
            try obj.end();
            return try json_buf.toOwnedSlice(allocator);
        },
        .err => |e| {
            return try h.jsonError(allocator, e.message);
        },
        else => return try h.jsonError(allocator, "Unexpected result"),
    }
}

// =============================================================================
// Wire Format Parsers
// =============================================================================

/// Parse KV scan result wire format into JSON
/// Wire format: [count:u32] ([key_len:u16][key][value_len:u32][value][version:u64])* [has_more:u8] [cursor_len:u16][cursor]?
fn parseScanResultToJson(allocator: Allocator, namespace: []const u8, data: []const u8) ![]const u8 {
    if (data.len < 4) return try allocator.dupe(u8, "[]");

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var reader = h.BinaryReader.init(data);
    const count = reader.readU32() orelse return try allocator.dupe(u8, "[]");

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();

    var arr = try obj.arrayField("keys");
    try arr.begin();

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const key_data = reader.readLenPrefixed(u16) orelse break;
        const value_data = reader.readLenPrefixed(u32) orelse break;
        const value_len = @as(u32, @intCast(value_data.len));
        const version = reader.readU64() orelse break;

        try arr.next();
        var entry = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try entry.begin();
        try entry.stringField("key", key_data);
        try entry.stringField("namespace", namespace);
        try entry.stringField("value", value_data);
        try entry.intField("version", version);
        try entry.intField("size", value_len);
        try entry.end();
    }

    try arr.end();

    // Parse has_more and cursor
    var has_more: bool = false;
    var next_cursor: []const u8 = "";
    if (reader.readByte()) |hm| {
        has_more = hm == 1;
        if (reader.readLenPrefixed(u16)) |cursor_data| {
            if (cursor_data.len > 0) next_cursor = cursor_data;
        }
    }

    try obj.boolField("has_more", has_more);
    if (next_cursor.len > 0) {
        var hex_buf: [512]u8 = undefined;
        const hex_len = @min(next_cursor.len * 2, hex_buf.len);
        var hex_pos: usize = 0;
        for (next_cursor) |byte| {
            if (hex_pos + 2 > hex_len) break;
            _ = std.fmt.bufPrint(hex_buf[hex_pos..][0..2], "{x:0>2}", .{byte}) catch break;
            hex_pos += 2;
        }
        try obj.stringField("cursor", hex_buf[0..hex_pos]);
    } else {
        try obj.nullField("cursor");
    }

    try obj.intField("count", i);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// Parse KV history result wire format into JSON
/// Wire format: [count:u32] ([value_len:u32][value][version:u32][lsn:u64])*
fn parseHistoryResultToJson(allocator: Allocator, key: []const u8, namespace: []const u8, data: []const u8) ![]const u8 {
    if (data.len < 4) return try allocator.dupe(u8, "{\"key\":\"\",\"versions\":[]}");

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var reader = h.BinaryReader.init(data);
    const count = reader.readU32() orelse return try allocator.dupe(u8, "{\"key\":\"\",\"versions\":[]}");

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("key", key);
    try obj.stringField("namespace", namespace);

    var arr = try obj.arrayField("versions");
    try arr.begin();

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const value_data = reader.readLenPrefixed(u32) orelse break;
        const value_len = value_data.len;
        const version = reader.readU32() orelse break;
        const lsn = reader.readU64() orelse break;

        try arr.next();
        var entry = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try entry.begin();
        try entry.intField("version", version);
        try entry.intField("lsn", lsn);
        try entry.stringField("value", value_data);
        try entry.boolField("deleted", value_len == 0);
        try entry.end();
    }

    try arr.end();
    try obj.intField("version_count", i);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}
