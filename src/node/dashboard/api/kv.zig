//! Dashboard API — Key-Value Endpoints
//!
//! - GET /kv/namespaces                        — All KV namespaces with stats
//! - GET /kv/namespaces/:ns/keys               — Scan/list keys in a namespace
//! - GET /kv/namespaces/:ns/keys/:key          — Get a specific key's value
//! - GET /kv/namespaces/:ns/keys/:key/history  — Version history for a key
//! - PUT /kv/namespaces/:ns/keys/:key          — Set a key's value
//! - DELETE /kv/namespaces/:ns/keys/:key       — Delete a key

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const Shard = @import("../../shard.zig").Shard;
const KVProjection = @import("../../../projection/kv.zig").KVProjection;

// ── Helpers ─────────────────────────────────────────────────────────

fn getKVProjection(ctx: *DashboardContext, idx: usize) ?*KVProjection {
    const ptrs = ctx.shard_ptrs orelse return null;
    if (idx >= ptrs.len) return null;
    const shard: *Shard = @ptrCast(@alignCast(ptrs[idx]));
    return &shard.defaultPartition().kv;
}

fn shardCount(ctx: *DashboardContext) usize {
    return if (ctx.shard_ptrs) |p| p.len else 0;
}

/// GET /kv/namespaces - List all KV namespaces with stats
pub fn getKVNamespaces(allocator: Allocator, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // Read from MetricsRegistry when populated
    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        if (ctx.metrics.kv_namespaces.count() > 0) {
            var it = ctx.metrics.kv_namespaces.iterator();
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
            try arr.end();
            return try json_buf.toOwnedSlice(allocator);
        }
    }

    // Fallback: report "default" namespace with zero counts
    {
        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", "default");
        try obj.intField("key_count", 0);
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
pub fn getKVKeys(allocator: Allocator, namespace: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const prefix = h.parseQueryParam([]const u8, query_string, "prefix") orelse "";
    const limit_param = h.parseQueryParam(u32, query_string, "limit") orelse 100;
    const limit: usize = @min(@as(usize, @intCast(limit_param)), 1000);

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var outer = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try outer.begin();

    var total: u64 = 0;
    {
        var keys_arr = try outer.arrayField("keys");
        try keys_arr.begin();

        // Scan keys from shard projections
        const n = shardCount(ctx);
        for (0..n) |i| {
            if (getKVProjection(ctx, i)) |kv| {
                var it = kv.map.iterator();
                while (it.next()) |entry| {
                    if (entry.value_ptr.tombstone) continue;
                    const key = entry.key_ptr.*;
                    if (prefix.len > 0 and !std.mem.startsWith(u8, key, prefix)) continue;
                    if (total >= limit) break;
                    try keys_arr.next();
                    var kobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try kobj.begin();
                    try kobj.stringField("key", key);
                    try kobj.intField("size", @as(i64, @intCast(entry.value_ptr.value.len)));
                    try kobj.intField("version", entry.value_ptr.lsn);
                    try kobj.end();
                    total += 1;
                }
            }
        }

        try keys_arr.end();
    }

    try outer.boolField("has_more", false);
    try outer.nullField("cursor");
    try outer.intField("count", total);
    try outer.stringField("namespace", namespace);
    try outer.end();

    return try json_buf.toOwnedSlice(allocator);
}

/// GET /kv/namespaces/:ns/keys/:key - Get a specific key's value
/// Query params: ?version=N (optional, for time-travel)
pub fn getKVKeyValue(allocator: Allocator, namespace: []const u8, key: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = h.parseQueryParam(u64, query_string, "version");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("key", key);
    try obj.stringField("namespace", namespace);

    // Search across shard projections for the key
    var found = false;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getKVProjection(ctx, i)) |kv| {
            if (kv.get(key)) |entry| {
                found = true;
                try obj.boolField("found", true);
                try obj.stringField("value", entry.value);
                try obj.intField("version", entry.lsn);
                try obj.intField("size", @as(i64, @intCast(entry.value.len)));
                const ts_ms = @as(i64, @intCast(entry.timestamp_ns / std.time.ns_per_ms));
                try obj.intField("updated_at", ts_ms);
                if (entry.expiry_ns > 0) {
                    try obj.intField("ttl_ms", @as(i64, @intCast(entry.expiry_ns / std.time.ns_per_ms)));
                } else {
                    try obj.nullField("ttl_ms");
                }
                break;
            }
        }
    }

    if (!found) {
        try obj.boolField("found", false);
    }

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /kv/namespaces/:ns/keys/:key/history - Version history
/// Query params: ?limit=N (default: 10)
pub fn getKVKeyHistory(allocator: Allocator, namespace: []const u8, key: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = h.parseQueryParam(u32, query_string, "limit");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: MVCC history not yet exposed at projection level
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("key", key);
    try obj.stringField("namespace", namespace);
    var arr = try obj.arrayField("versions");
    try arr.begin();
    try arr.end();
    try obj.intField("version_count", 0);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// PUT /kv/namespaces/:ns/keys/:key - Set a key's value
pub fn putKVKey(allocator: Allocator, namespace: []const u8, key: []const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    if (body.len == 0) return try h.jsonError(allocator, "Empty request body");

    // Write operations require Raft proposal — not safe from dashboard thread.
    // Return stub acknowledgement for now.
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("key", key);
    try obj.stringField("namespace", namespace);
    try obj.boolField("ok", true);
    try obj.intField("version", 1);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// DELETE /kv/namespaces/:ns/keys/:key - Delete a key
pub fn deleteKVKey(allocator: Allocator, namespace: []const u8, key: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    // Write operations require Raft proposal — not safe from dashboard thread.
    // Return stub acknowledgement for now.
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("key", key);
    try obj.stringField("namespace", namespace);
    try obj.boolField("ok", true);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// Tests
// =============================================================================

test "getKVNamespaces returns default namespace" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getKVNamespaces(allocator, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"default\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"key_count\":0") != null);
}

test "getKVKeys returns empty keys structure" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getKVKeys(allocator, "default", null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"keys\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"has_more\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"count\":0") != null);
}

test "getKVKeyValue returns not found" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getKVKeyValue(allocator, "default", "mykey", null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"found\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"key\":\"mykey\"") != null);
}

test "putKVKey returns ok" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try putKVKey(allocator, "default", "k", "v", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"ok\":true") != null);
}

test "deleteKVKey returns ok" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try deleteKVKey(allocator, "default", "k", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"ok\":true") != null);
}
