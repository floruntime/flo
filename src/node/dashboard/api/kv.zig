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
const client_mod = @import("../../../cli/client/mod.zig");
const schema = @import("../../../protocol/schema.zig");
const kv_ops = @import("../../../protocol/kv_ops.zig");
const loopback = @import("loopback.zig");
const Reader = loopback.Reader;
const readFailed = loopback.readFailed;

/// GET /kv/namespaces - List all KV namespaces with stats
pub fn getKVNamespaces(allocator: Allocator, ctx: *DashboardContext) ![]const u8 {
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

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
            return try json_aw.toOwnedSlice();
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
    return try json_aw.toOwnedSlice();
}

/// The most keys a listing page holds: one mget answers their sizes.
const MAX_PAGE: u32 = kv_ops.MAX_MGET;

/// GET /kv/namespaces/:ns/keys - List keys in a namespace, a page at a time
/// Query params: ?prefix=&limit=&cursor= (the hex cursor the last page gave)
/// Read as a client over loopback (`kv.scan`, then `kv.mget` for sizes and
/// versions): the projections are their shards' alone to read.
pub fn getKVKeys(allocator: Allocator, namespace: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    // The console sends the prefix percent-encoded; a NUL in it would spell
    // another namespace's keys, as in a key.
    var prefix_buf: [@import("../../../protocol/ops.zig").MAX_KEY]u8 = undefined;
    const prefix = if (h.parseQueryParam([]const u8, query_string, "prefix")) |raw|
        h.percentDecode(&prefix_buf, raw) orelse return try h.jsonError(allocator, "prefix must be a percent-encoded key prefix")
    else
        "";
    if (std.mem.indexOfScalar(u8, prefix, 0) != null) return try h.jsonError(allocator, "prefix must not hold a NUL");
    const limit: u32 = @min(h.parseQueryParam(u32, query_string, "limit") orelse 100, MAX_PAGE);
    var cursor_buf: [kv_ops.MAX_CURSOR]u8 = undefined;
    const cursor: []const u8 = if (h.parseQueryParam([]const u8, query_string, "cursor")) |hex|
        std.fmt.hexToBytes(&cursor_buf, hex) catch return try h.jsonError(allocator, "cursor must be the hex cursor a page gave")
    else
        "";

    var reader = Reader.open(allocator, ctx) catch |err| return try readFailed(allocator, "KV scan", err);
    defer reader.deinit();

    var scanned = reader.call(kv_ops.op("kv.scan"), namespace, prefix, .{ .limit = @max(limit, 1), .cursor = cursor }) catch |err|
        return try readFailed(allocator, "KV scan", err);
    defer scanned.deinit();
    if (scanned.isError()) return try h.jsonError(allocator, scanned.errorMessage());
    var diag: schema.Diagnostic = .{};
    const page = kv_ops.op("kv.scan").response.decodeAll(scanned.data, &diag) catch
        return try h.jsonError(allocator, "KV scan: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var outer = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try outer.begin();

    var total: u64 = 0;
    {
        var keys_arr = try outer.arrayField("keys");
        try keys_arr.begin();
        // The page's names, then their sizes and versions in as many mgets
        // as the answers need. A key deleted in between is left out.
        const Keys = kv_ops.op("kv.mget").request.field_list[0].T;
        const keys_buf = try allocator.alloc(u8, Keys.max_size);
        defer allocator.free(keys_buf);
        var names: [MAX_PAGE][]const u8 = undefined;
        var n: usize = 0;
        var it = page.names.iterator();
        while (it.next() catch return try h.jsonError(allocator, "KV scan: malformed answer")) |name| : (n += 1) {
            if (n == names.len) return try h.jsonError(allocator, "KV scan: more names than asked for");
            names[n] = name;
        }
        var from: usize = 0;
        while (from < n) {
            var list = Keys.Builder.init(keys_buf);
            for (names[from..n]) |name| if (!(try list.add(name))) break;
            var got = reader.call(kv_ops.op("kv.mget"), namespace, "", .{ .keys = list.value(), .routing_key = null }) catch |err|
                return try readFailed(allocator, "KV mget", err);
            defer got.deinit();
            if (got.isError()) return try h.jsonError(allocator, got.errorMessage());
            const answer = kv_ops.op("kv.mget").response.decodeAll(got.data, &diag) catch
                return try h.jsonError(allocator, "KV mget: malformed answer");
            var entries = answer.entries.iterator();
            var i = from;
            while (entries.next() catch return try h.jsonError(allocator, "KV mget: malformed answer")) |e| : (i += 1) {
                if (i >= n) return try h.jsonError(allocator, "KV mget: malformed answer");
                if (!e.found) continue;
                try keys_arr.next();
                var kobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try kobj.begin();
                try kobj.stringField("key", names[i]);
                try kobj.intField("size", @as(i64, @intCast(e.value.len)));
                try kobj.intField("version", e.version);
                try kobj.end();
                total += 1;
            }
            const answered = answer.next orelse list.count;
            if (answered == 0) return try h.jsonError(allocator, "KV mget: no progress");
            from += answered;
        }
        try keys_arr.end();
    }

    try outer.boolField("has_more", page.cursor != null);
    if (page.cursor) |next| {
        // Hex, as the CLI prints it; at most MAX_CURSOR bytes by the schema.
        var hex: [2 * kv_ops.MAX_CURSOR]u8 = undefined;
        const digits = "0123456789abcdef";
        for (next, 0..) |b, i| {
            hex[2 * i] = digits[b >> 4];
            hex[2 * i + 1] = digits[b & 0xf];
        }
        try outer.stringField("cursor", hex[0 .. 2 * next.len]);
    } else try outer.nullField("cursor");
    try outer.intField("count", total);
    try outer.stringField("namespace", namespace);
    try outer.end();
    return try json_aw.toOwnedSlice();
}

/// GET /kv/namespaces/:ns/keys/:key - Get a specific key's value
pub fn getKVKeyValue(allocator: Allocator, namespace: []const u8, key: []const u8, ctx: *DashboardContext) ![]const u8 {
    var reader = Reader.open(allocator, ctx) catch |err| return try readFailed(allocator, "KV get", err);
    defer reader.deinit();
    var got = reader.call(kv_ops.op("kv.get"), namespace, key, .{ .wait_ms = 0, .wait_for = .exists, .routing_key = null, .txn = null }) catch |err|
        return try readFailed(allocator, "KV get", err);
    defer got.deinit();

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("key", key);
    try obj.stringField("namespace", namespace);
    if (got.isNotFound()) {
        try obj.boolField("found", false);
    } else {
        if (got.isError()) return try h.jsonError(allocator, got.errorMessage());
        var diag: schema.Diagnostic = .{};
        const answer = kv_ops.op("kv.get").response.decodeAll(got.data, &diag) catch
            return try h.jsonError(allocator, "KV get: malformed answer");
        try obj.boolField("found", true);
        try obj.stringField("value", answer.value);
        try obj.intField("version", answer.version);
        try obj.intField("size", @as(i64, @intCast(answer.value.len)));
        if (answer.expires_at_ms) |at| try obj.intField("expires_at_ms", at) else try obj.nullField("expires_at_ms");
    }
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /kv/namespaces/:ns/keys/:key/history - Version history, newest first
/// Query params: ?limit=N (default 10, at most kv_ops.MAX_HISTORY) and
/// ?before_version=V, for the page of versions older than V; `more` says
/// whether older kept versions follow.
pub fn getKVKeyHistory(allocator: Allocator, namespace: []const u8, key: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const limit = std.math.clamp(h.parseQueryParam(u32, query_string, "limit") orelse 10, 1, kv_ops.MAX_HISTORY);

    var reader = Reader.open(allocator, ctx) catch |err| return try readFailed(allocator, "KV history", err);
    defer reader.deinit();
    const before_version = h.parseQueryParam(u64, query_string, "before_version");
    var got = reader.call(kv_ops.op("kv.history"), namespace, key, .{ .limit = limit, .before_version = before_version, .routing_key = null }) catch |err|
        return try readFailed(allocator, "KV history", err);
    defer got.deinit();

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("key", key);
    try obj.stringField("namespace", namespace);

    var version_count: i64 = 0;
    var more = false;
    var arr = try obj.arrayField("versions");
    try arr.begin();
    if (!got.isNotFound()) {
        if (got.isError()) return try h.jsonError(allocator, got.errorMessage());
        var diag: schema.Diagnostic = .{};
        const answer = kv_ops.op("kv.history").response.decodeAll(got.data, &diag) catch
            return try h.jsonError(allocator, "KV history: malformed answer");
        var it = answer.versions.iterator();
        while (it.next() catch return try h.jsonError(allocator, "KV history: malformed answer")) |ver| {
            try arr.next();
            var vobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try vobj.begin();
            try vobj.intField("version", ver.version);
            try vobj.intField("timestamp_ms", ver.at_ms);
            try vobj.intField("size", @as(i64, @intCast(ver.value.len)));
            try vobj.boolField("tombstone", ver.deleted);
            try vobj.end();
            version_count += 1;
        }
        more = answer.more;
    }
    try arr.end();
    try obj.intField("version_count", version_count);
    try obj.boolField("more", more);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// PUT /kv/namespaces/:ns/keys/:key - Set a key's value
/// Body: { "value": "<bytes>", "ttl_ms": <int|null>, "nx": <bool> }
pub fn putKVKey(allocator: Allocator, namespace: []const u8, key: []const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    if (body.len == 0) return try h.jsonError(allocator, "Empty request body");

    var parsed = std.json.parseFromSlice(std.json.Value, allocator, body, .{}) catch
        return try h.jsonError(allocator, "Invalid JSON body");
    defer parsed.deinit();
    if (parsed.value != .object) return try h.jsonError(allocator, "Body must be a JSON object");
    const obj_in = parsed.value.object;
    // A misnamed field would be ignored, and its option silently dropped.
    for (obj_in.keys()) |name| {
        for ([_][]const u8{ "value", "ttl_ms", "nx" }) |known| {
            if (std.mem.eql(u8, name, known)) break;
        } else {
            const msg = try std.fmt.allocPrint(allocator, "unknown field \"{s}\"", .{name});
            defer allocator.free(msg);
            return try h.jsonError(allocator, msg);
        }
    }

    var value: []const u8 = "";
    if (obj_in.get("value")) |v| switch (v) {
        .string => |s| value = s,
        else => {},
    };
    var opts: client_mod.kv.set_op.request.Value = .{ .value = value, .ttl_ms = null, .condition = .always, .cas_version = null, .routing_key = null, .txn = null };
    // A TTL that can't be read is refused: ignored, the key would never
    // expire.
    if (obj_in.get("ttl_ms")) |t| switch (t) {
        .integer => |i| {
            if (i < 0) return try h.jsonError(allocator, "ttl_ms must not be negative");
            if (i > 0) opts.ttl_ms = @intCast(i);
        },
        .null => {},
        else => return try h.jsonError(allocator, "ttl_ms must be a whole number of milliseconds"),
    };
    if (obj_in.get("nx")) |n| switch (n) {
        .bool => |b| opts.condition = if (b) .if_absent else .always,
        else => {},
    };

    var client = loopback.connect(allocator, ctx) catch
        return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.kv.set(&client, namespace, key, opts) catch
        return try h.jsonError(allocator, "KV set failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());
    var diag: schema.Diagnostic = .{};
    const answer = client_mod.kv.set_op.response.decodeAll(resp.data, &diag) catch
        return try h.jsonError(allocator, "KV set: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("key", key);
    try obj.stringField("namespace", namespace);
    try obj.boolField("ok", true);
    try obj.intField("version", @as(i64, @intCast(answer.version orelse 0)));
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// DELETE /kv/namespaces/:ns/keys/:key - Delete a key
pub fn deleteKVKey(allocator: Allocator, namespace: []const u8, key: []const u8, ctx: *DashboardContext) ![]const u8 {
    var client = loopback.connect(allocator, ctx) catch
        return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.kv.call(&client, "kv.delete", namespace, key, .{ .cas_version = null, .routing_key = null, .txn = null }) catch
        return try h.jsonError(allocator, "KV delete failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("key", key);
    try obj.stringField("namespace", namespace);
    try obj.boolField("ok", true);
    try obj.end();
    return try json_aw.toOwnedSlice();
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

test "getKVKeys refuses a cursor that isn't hex before connecting" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here → connect refused

    const result = try getKVKeys(allocator, "default", "cursor=zz", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "cursor must be the hex cursor a page gave") != null);
}

test "getKVKeyValue surfaces loopback connect failure (no node in unit test)" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    ctx.listen_port = 1; // nothing listens here → connect refused

    const result = try getKVKeyValue(allocator, "default", "mykey", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}

test "putKVKey rejects empty body" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try putKVKey(allocator, "default", "k", "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}

test "putKVKey rejects invalid JSON body" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try putKVKey(allocator, "default", "k", "not json", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}

test "deleteKVKey surfaces loopback connect failure (no node in unit test)" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here → connect refused

    const result = try deleteKVKey(allocator, "default", "k", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}
