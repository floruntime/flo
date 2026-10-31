//! Dashboard API — Namespace Endpoints
//!
//! Namespaces are the cross-cutting organizational primitive.
//!
//! - GET /namespaces             — List all namespaces with resource counts
//! - GET /namespaces/:name       — Namespace detail
//! - GET /namespaces/:ns/streams — Streams in namespace
//! - GET /namespaces/:ns/queues  — Queues in namespace
//! - GET /namespaces/:ns/kv      — KV stats for namespace

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const loopback = @import("loopback.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const namespace_ops = @import("../../../protocol/namespace_ops.zig");
const ns_keys = @import("../../../namespace/handler.zig");
const kv_ops = @import("../../../protocol/kv_ops.zig");
const schema = @import("../../../protocol/schema.zig");

/// Key names a namespace's detail shows; the KV view pages through the rest.
const KV_KEYS_SHOWN: u32 = 100;
const client_mod = @import("../../../cli/client/mod.zig");

/// GET /namespaces - List all namespaces with resource counts. The
/// registry's names come from `namespace.list`. The counts of definitions,
/// jobs and actions are null: counting them would walk three lists for
/// every namespace (up to 4096) on the dashboard's one thread, so they
/// wait until the namespace list carries them.
pub fn getNamespaces(allocator: Allocator, ctx: *DashboardContext) ![]const u8 {
    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "namespace list", err);
    defer reader.deinit();

    // Every name is copied: the answers and the metrics registry free theirs.
    var names_arena = std.heap.ArenaAllocator.init(allocator);
    defer names_arena.deinit();
    const names = names_arena.allocator();
    // In the arena too, which frees it.
    var ns_set: std.StringArrayHashMapUnmanaged(void) = .empty;

    readNamespaceNames(&reader, names, &ns_set) catch |err| return try loopback.readFailed(allocator, "namespace list", err);

    // Implicit namespaces, from data writes the metrics registry saw.
    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var kv_it = ctx.metrics.kv_namespaces.iterator();
        while (kv_it.next()) |entry| try putName(&ns_set, names, entry.value_ptr.*.namespace);
        var q_it = ctx.metrics.queues.iterator();
        while (q_it.next()) |entry| try putName(&ns_set, names, entry.value_ptr.*.namespace);
        var s_it = ctx.metrics.streams.iterator();
        while (s_it.next()) |entry| try putName(&ns_set, names, entry.value_ptr.*.namespace);
    }
    try putName(&ns_set, names, "default");

    const sorted = ns_set.keys();
    std.mem.sortUnstable([]const u8, sorted, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    for (sorted) |ns_name| {
        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", ns_name);
        try obj.intField("stream_count", countStreamsInNamespace(ctx, ns_name));
        try obj.intField("queue_count", countQueuesInNamespace(ctx, ns_name));
        try obj.intField("kv_count", countKVInNamespace(ctx, ns_name));
        try obj.nullField("workflow_count");
        try obj.nullField("processing_count");
        try obj.nullField("action_count");
        try obj.intField("created_at", 0);
        try obj.boolField("is_system", std.mem.startsWith(u8, ns_name, "_"));
        try obj.end();
    }

    try arr.end();
    return try json_aw.toOwnedSlice();
}

fn putName(set: *std.StringArrayHashMapUnmanaged(void), names: Allocator, name: []const u8) !void {
    if (set.contains(name)) return;
    try set.put(names, try names.dupe(u8, name), {});
}

/// Every name in the namespace registry, a page at a time to the last.
pub fn readNamespaceNames(reader: *loopback.Reader, names: Allocator, set: *std.StringArrayHashMapUnmanaged(void)) !void {
    const list_op = namespace_ops.op("namespace.list");
    var cursor: []const u8 = "";
    while (true) {
        var got = try reader.call(list_op, "", "", .{ .limit = namespace_ops.MAX_LIST, .cursor = cursor });
        defer got.deinit();
        if (got.isError()) return error.Refused;
        var diag: schema.Diagnostic = .{};
        const page = try list_op.response.decodeAll(got.data, &diag);
        var it = page.names.iterator();
        while (try it.next()) |name| try putName(set, names, name);
        const next = page.cursor orelse return;
        if (std.mem.eql(u8, next, cursor)) return error.CursorRepeats;
        cursor = try names.dupe(u8, next);
    }
}

/// POST /namespaces — Create a new namespace. Proposed over the node's own
/// protocol port like any client's create: the registry belongs to its
/// shard's thread, and a create must be logged.
pub fn createNamespace(allocator: Allocator, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    // Parse the name from JSON body: {"name": "..."}
    const name = extractName(body) orelse {
        return h.jsonError(allocator, "missing or invalid \"name\" field");
    };
    if (ns_keys.nameRefusal(name)) |why| return h.jsonError(allocator, why);

    var client = loopback.connect(allocator, ctx) catch
        return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.namespace.create(&client, name) catch
        return try h.jsonError(allocator, "namespace create failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    // Return success
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);
    try obj.boolField("ok", true);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

fn extractName(body: []const u8) ?[]const u8 {
    // Simple JSON extraction: find "name":"..." or "name": "..."
    const needle = "\"name\"";
    const idx = std.mem.indexOf(u8, body, needle) orelse return null;
    const after = body[idx + needle.len ..];
    // Skip whitespace and colon
    var i: usize = 0;
    while (i < after.len and (after[i] == ' ' or after[i] == ':' or after[i] == '\t')) : (i += 1) {}
    if (i >= after.len or after[i] != '"') return null;
    i += 1; // skip opening quote
    const start = i;
    while (i < after.len and after[i] != '"') : (i += 1) {}
    if (i >= after.len) return null;
    return after[start..i];
}

/// GET /namespaces/:name - Namespace detail with resource arrays
pub fn getNamespaceDetail(allocator: Allocator, name: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);

    // streams array
    {
        var streams_arr = try obj.arrayField("streams");
        try streams_arr.begin();

        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var seen = std.StringHashMap(void).init(allocator);
        defer seen.deinit();

        var it = ctx.metrics.streams.iterator();
        while (it.next()) |entry| {
            const s = entry.value_ptr.*;
            if (!std.mem.eql(u8, s.namespace, name)) continue;
            const gop = try seen.getOrPut(s.topic);
            if (!gop.found_existing) {
                try streams_arr.next();
                var sobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try sobj.begin();
                try sobj.stringField("name", s.topic);
                try sobj.stringField("namespace", s.namespace);
                try sobj.intField("partitions", 1);
                try sobj.intField("ingest_rate", 0);
                try sobj.intField("reads", 0);
                try sobj.nullField("retention");
                try sobj.end();
            }
        }

        try streams_arr.end();
    }

    // queues array
    {
        var queues_arr = try obj.arrayField("queues");
        try queues_arr.begin();

        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var it = ctx.metrics.queues.iterator();
        while (it.next()) |entry| {
            const q = entry.value_ptr.*;
            if (!std.mem.eql(u8, q.namespace, name)) continue;
            try queues_arr.next();
            const snap = q.metrics.snapshot();
            var qobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try qobj.begin();
            try qobj.stringField("name", q.queue);
            try qobj.stringField("namespace", q.namespace);
            try qobj.intField("enqueued", snap.enqueue_ops_total);
            try qobj.intField("dequeued", snap.dequeue_ops_total);
            try qobj.intField("bytes_total", snap.enqueue_bytes_total);
            try qobj.end();
        }

        try queues_arr.end();
    }

    // The first page of the namespace's key names, read as a client over
    // loopback: the projections are their shards' alone to read. A read
    // that fails says why, beside the names it got.
    {
        var kv_arr = try obj.arrayField("kv_keys");
        try kv_arr.begin();
        var more = false;
        var failed: ?[]const u8 = null;
        if (loopback.Reader.open(allocator, ctx)) |reader_| {
            var reader = reader_;
            defer reader.deinit();
            if (reader.call(kv_ops.op("kv.scan"), name, "", .{ .limit = KV_KEYS_SHOWN, .cursor = "" })) |resp_| {
                var resp = resp_;
                defer resp.deinit();
                // A namespace never created holds no keys.
                if (resp.isNotFound()) {} else if (resp.isError()) {
                    failed = "KV scan refused";
                } else {
                    var diag: schema.Diagnostic = .{};
                    if (kv_ops.op("kv.scan").response.decodeAll(resp.data, &diag)) |page| {
                        var it = page.names.iterator();
                        while (true) {
                            const key = it.next() catch {
                                failed = "KV scan: malformed answer";
                                break;
                            } orelse break;
                            try kv_arr.next();
                            var kobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                            try kobj.begin();
                            try kobj.stringField("key", key);
                            try kobj.end();
                        }
                        more = page.cursor != null;
                    } else |_| failed = "KV scan: malformed answer";
                }
            } else |err| failed = timedOutOr(err, "KV scan failed");
        } else |err| failed = timedOutOr(err, "Loopback connect failed");
        try kv_arr.end();
        try obj.boolField("kv_keys_more", more);
        if (failed) |why| try obj.stringField("kv_keys_error", why);
    }

    try obj.end();
    return try json_aw.toOwnedSlice();
}

fn timedOutOr(err: anyerror, otherwise: []const u8) []const u8 {
    return switch (err) {
        error.OutOfTime, error.ConnectionTimedOut => "KV scan: the node didn't answer in time",
        else => otherwise,
    };
}

/// GET /namespaces/:ns/streams - Streams in namespace
pub fn getNamespaceStreams(allocator: Allocator, namespace: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var seen = std.StringHashMap(void).init(allocator);
        defer seen.deinit();

        var it = ctx.metrics.streams.iterator();
        while (it.next()) |entry| {
            const s = entry.value_ptr.*;
            if (!std.mem.eql(u8, s.namespace, namespace)) continue;

            const gop = try seen.getOrPut(s.topic);
            if (!gop.found_existing) {
                try arr.next();
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("name", s.topic);
                try obj.stringField("namespace", s.namespace);
                try obj.intField("partitions", 1);
                try obj.end();
            }
        }
    }

    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// GET /namespaces/:ns/queues - Queues in namespace
pub fn getNamespaceQueues(allocator: Allocator, namespace: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var it = ctx.metrics.queues.iterator();
        while (it.next()) |entry| {
            const q = entry.value_ptr.*;
            if (!std.mem.eql(u8, q.namespace, namespace)) continue;

            try arr.next();
            var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try obj.begin();
            try obj.stringField("name", q.queue);
            try obj.stringField("namespace", q.namespace);
            try obj.end();
        }
    }

    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// GET /namespaces/:ns/kv - KV stats for namespace (also used for /kv/namespaces/:ns)
pub fn getNamespaceKV(allocator: Allocator, namespace: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("namespace", namespace);

    // Look for namespace in metrics
    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var found = false;
        var it = ctx.metrics.kv_namespaces.iterator();
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.*.namespace, namespace)) {
                const snap = entry.value_ptr.*.metrics.snapshot();
                try obj.intField("key_count", snap.key_count);
                try obj.intField("bytes_stored", snap.bytes_stored);
                try obj.intField("get_ops", snap.get_ops_total);
                try obj.intField("set_ops", snap.set_ops_total);
                try obj.intField("delete_ops", snap.delete_ops_total);
                found = true;
                break;
            }
        }

        if (!found) {
            try obj.intField("key_count", 0);
            try obj.intField("bytes_stored", 0);
            try obj.intField("get_ops", 0);
            try obj.intField("set_ops", 0);
            try obj.intField("delete_ops", 0);
        }
    }

    try obj.end();
    return try json_aw.toOwnedSlice();
}

// =============================================================================
// Helpers
// =============================================================================

fn countStreamsInNamespace(ctx: *DashboardContext, namespace: []const u8) u32 {
    ctx.metrics.mutex.lock();
    defer ctx.metrics.mutex.unlock();

    var count: u32 = 0;
    var it = ctx.metrics.streams.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.value_ptr.*.namespace, namespace)) count += 1;
    }
    return count;
}

fn countQueuesInNamespace(ctx: *DashboardContext, namespace: []const u8) u32 {
    ctx.metrics.mutex.lock();
    defer ctx.metrics.mutex.unlock();

    var count: u32 = 0;
    var it = ctx.metrics.queues.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.value_ptr.*.namespace, namespace)) count += 1;
    }
    return count;
}

fn countKVInNamespace(ctx: *DashboardContext, namespace: []const u8) u64 {
    // As the shards published it: their projections are theirs alone to read.
    return ctx.metrics.kvLiveKeys(namespace);
}

// =============================================================================
// Tests
// =============================================================================

test "getNamespaces asks the node, and says when no node answers" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here: a connect would be refused

    const result = try getNamespaces(allocator, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "{\"error\":\"namespace list failed: "));
}

test "getNamespaceDetail returns detail object with arrays" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getNamespaceDetail(allocator, "default", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"default\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"streams\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"queues\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"kv_keys\":[]") != null);
}
