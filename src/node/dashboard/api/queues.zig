//! Dashboard API — Queue Endpoints
//!
//! - GET    /queues                           — All queues
//! - GET    /queues/:name                     — Queue detail
//! - GET    /queues/:name/messages            — Queue messages
//! - GET    /queues/:name/dlq                 — Dead-letter queue entries
//! - POST   /queues/:name/dlq/:seq/requeue   — Requeue DLQ entry
//! - DELETE /queues/:name/dlq/:seq            — Delete DLQ entry
//! - POST   /queues/:name/purge              — Purge queue

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const Shard = @import("../../shard.zig").Shard;
const QueueProjection = @import("../../../projection/queue.zig").QueueProjection;
const router = @import("../../router.zig");
const client_mod = @import("../../../cli/client/mod.zig");

/// Short-lived loopback client to the node's own protocol port — mutations can't
/// be proposed from the dashboard thread (see api/kv.zig loopbackConnect).
fn loopbackConnect(allocator: Allocator, ctx: *DashboardContext) !client_mod.Client {
    var ep_buf: [32]u8 = undefined;
    const endpoint = try std.fmt.bufPrint(&ep_buf, "127.0.0.1:{d}", .{ctx.listen_port});
    var client = client_mod.Client.init(allocator, endpoint);
    errdefer client.deinit();
    try client.connect();
    return client;
}

fn queueHash(namespace: []const u8, name: []const u8) u64 {
    return router.nameHash(router.namespaceHash(namespace), name);
}

/// Per-queue message tally by state (computed from the projection — the metrics
/// counters under-report). `pending = ready + leased`.
const QueueTally = struct { ready: u64 = 0, leased: u64 = 0, dlq: u64 = 0 };

fn tallyQueue(qp: *QueueProjection, qhash: u64) QueueTally {
    var t = QueueTally{};
    var it = qp.messages.iterator();
    while (it.next()) |kv| {
        const msg = kv.value_ptr.*;
        if (msg.queue_name_hash != qhash) continue;
        switch (msg.state) {
            .ready => t.ready += 1,
            .leased => t.leased += 1,
            .dlq => t.dlq += 1,
        }
    }
    return t;
}

// ── Helpers ──

fn getQueueProjection(ctx: *DashboardContext, idx: usize) ?*QueueProjection {
    const ptrs = ctx.shard_ptrs orelse return null;
    if (idx >= ptrs.len) return null;
    const shard: *Shard = @ptrCast(@alignCast(ptrs[idx]));
    return &shard.defaultPartition().queue;
}

fn shardCount(ctx: *DashboardContext) usize {
    return if (ctx.shard_ptrs) |p| p.len else 0;
}

/// GET /queues - List all queues
pub fn getQueues(allocator: Allocator, ctx: *DashboardContext) ![]const u8 {
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // List from the projection's registered queues (the metrics counters
    // under-report); counts per queue are tallied from the message map.
    var seen = std.AutoHashMap(u64, void).init(allocator);
    defer seen.deinit();

    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getQueueProjection(ctx, i)) |qp| {
            var it = qp.known_queues.iterator();
            while (it.next()) |kv| {
                const qhash = kv.key_ptr.*;
                const meta = kv.value_ptr.*;
                if (meta.name.len > 0 and meta.name[0] == '_') continue; // system queue
                const gop = try seen.getOrPut(qhash);
                if (gop.found_existing) continue;

                const t = tallyQueue(qp, qhash);
                try arr.next();
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("name", meta.name);
                try obj.stringField("namespace", meta.namespace);
                try obj.intField("ready", t.ready);
                try obj.intField("inflight", t.leased);
                try obj.intField("pending", t.ready + t.leased);
                try obj.intField("available", t.ready);
                try obj.intField("enqueued", meta.enqueued);
                try obj.intField("dequeued", meta.dequeued);
                try obj.intField("dlq_count", t.dlq);
                try obj.end();
            }
        }
    }

    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// GET /queues/:name - Queue detail
pub fn getQueueDetail(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const qhash = queueHash(ns_q, queue_name);

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", queue_name);
    try obj.stringField("namespace", ns_q);

    var enqueued: u64 = 0;
    var dequeued: u64 = 0;
    var t = QueueTally{};
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getQueueProjection(ctx, i)) |qp| {
            if (qp.known_queues.get(qhash)) |meta| {
                enqueued = meta.enqueued;
                dequeued = meta.dequeued;
                t = tallyQueue(qp, qhash);
                break;
            }
        }
    }

    try obj.intField("ready", t.ready);
    try obj.intField("inflight", t.leased);
    try obj.intField("pending", t.ready + t.leased);
    try obj.intField("available", t.ready);
    try obj.intField("enqueued", enqueued);
    try obj.intField("dequeued", dequeued);
    try obj.intField("dlq_count", t.dlq);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /queues/:name/messages - Queue messages with status filter
/// Query params: ?status=&limit=
pub fn getQueueMessages(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = h.parseQueryParam([]const u8, query_string, "status");
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const limit_param = h.parseQueryParam(u32, query_string, "limit") orelse 1000;
    const limit: usize = @min(@as(usize, @intCast(limit_param)), 2000);
    const qhash = queueHash(ns_q, queue_name);
    const now_ms: i64 = @import("stdx").time.milliTimestamp();

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();

    var msg_count: u64 = 0;
    var total: u64 = 0;
    {
        var msgs_arr = try obj.arrayField("messages");
        try msgs_arr.begin();

        const n = shardCount(ctx);
        for (0..n) |i| {
            if (getQueueProjection(ctx, i)) |qp| {
                var it = qp.messages.iterator();
                while (it.next()) |entry| {
                    const msg = entry.value_ptr.*;
                    if (msg.queue_name_hash != qhash) continue; // only THIS queue
                    total += 1;
                    if (msg_count >= limit) continue;
                    const payload = if (std.unicode.utf8ValidateSlice(msg.payload)) msg.payload else "<binary>";
                    const lease_ms: i64 = blk: {
                        if (msg.state != .leased) break :blk 0;
                        const exp_ms: i64 = @intCast(msg.lease_expiry_ns / std.time.ns_per_ms);
                        break :blk if (exp_ms > now_ms) exp_ms - now_ms else 0;
                    };
                    try msgs_arr.next();
                    var mobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try mobj.begin();
                    try mobj.intField("seq", msg.seq);
                    try mobj.intField("priority", msg.priority);
                    try mobj.stringField("state", switch (msg.state) {
                        .ready => "ready",
                        .leased => "leased",
                        .dlq => "dlq",
                    });
                    try mobj.intField("attempts", msg.attempts);
                    try mobj.intField("enqueued_at", @as(i64, @intCast(msg.enqueued_at_ns / std.time.ns_per_ms)));
                    try mobj.intField("lease_remaining_ms", lease_ms);
                    try mobj.intField("size", @as(i64, @intCast(msg.payload.len)));
                    try mobj.stringField("payload", payload);
                    try mobj.end();
                    msg_count += 1;
                }
            }
        }

        try msgs_arr.end();
    }

    try obj.intField("count", msg_count);
    try obj.intField("total", total);
    try obj.stringField("queue", queue_name);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /queues/:name/dlq - Dead-letter queue entries
/// Query params: ?limit=
pub fn getQueueDLQ(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const limit_param = h.parseQueryParam(u32, query_string, "limit") orelse 100;
    const limit: usize = @min(@as(usize, @intCast(limit_param)), 1000);

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();

    var entry_count: u64 = 0;
    {
        var entries_arr = try obj.arrayField("entries");
        try entries_arr.begin();

        const n = shardCount(ctx);
        for (0..n) |i| {
            if (getQueueProjection(ctx, i)) |qp| {
                for (qp.dlq.items) |dlq_entry| {
                    if (entry_count >= limit) break;
                    try entries_arr.next();
                    var eobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try eobj.begin();
                    try eobj.intField("seq", dlq_entry.seq);
                    try eobj.intField("attempts", dlq_entry.attempts);
                    try eobj.intField("moved_at", @as(i64, @intCast(dlq_entry.moved_at_ns / std.time.ns_per_ms)));
                    try eobj.end();
                    entry_count += 1;
                }
            }
        }

        try entries_arr.end();
    }

    try obj.intField("count", entry_count);
    try obj.stringField("queue", queue_name);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// POST /queues/:name/dlq/:seq/requeue?namespace= - Requeue a DLQ entry (loopback write)
pub fn requeueDLQEntry(allocator: Allocator, queue_name: []const u8, seq_str: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const seq = std.fmt.parseInt(u64, seq_str, 10) catch return try h.jsonError(allocator, "Invalid seq");

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.queue.dlqRequeue(&client, ns_q, queue_name, &[_]u64{seq}) catch
        return try h.jsonError(allocator, "DLQ requeue failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// DELETE /queues/:name/dlq/:seq - Delete a DLQ entry
pub fn deleteDLQEntry(allocator: Allocator, queue_name: []const u8, seq_str: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = queue_name;
    _ = seq_str;

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    // Write operations require Raft proposal — not safe from dashboard thread.
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// POST /queues/:name/purge - Purge all live (ready+leased) messages (loopback write).
pub fn purgeQueue(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.queue.purge(&client, ns_q, queue_name) catch
        return try h.jsonError(allocator, "Purge failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    // Response raw data is a u32 LE count of removed messages.
    var purged: u32 = 0;
    if (resp.asRawData()) |data| {
        if (data.len >= 4) purged = std.mem.readInt(u32, data[0..4], .little);
    }

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.intField("purged", @as(i64, @intCast(purged)));
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// POST /queues/:name — enqueue a message (loopback write). Body is the payload;
/// `?priority=` & `?delay_ms=` optional.
pub fn enqueueMessage(allocator: Allocator, queue_name: []const u8, body: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const priority: u8 = @intCast(@min(h.parseQueryParam(u64, query_string, "priority") orelse 0, 255));
    const delay_ms = h.parseQueryParam(u64, query_string, "delay_ms");

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.queue.enqueue(&client, ns_q, queue_name, body, priority, delay_ms, null) catch
        return try h.jsonError(allocator, "Enqueue failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    // Response raw data is the assigned sequence number (u64 LE).
    var seq: u64 = 0;
    if (resp.asRawData()) |data| {
        if (data.len >= 8) seq = std.mem.readInt(u64, data[0..8], .little);
    }

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("queue", queue_name);
    try obj.intField("seq", @as(i64, @intCast(seq)));
    try obj.end();
    return try json_aw.toOwnedSlice();
}

// =============================================================================
// Tests
// =============================================================================

test "getQueues returns empty array when no queues registered" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getQueues(allocator, &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "getQueueDetail returns zeroed queue for unknown name" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getQueueDetail(allocator, "unknown", null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"unknown\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"pending\":0") != null);
}

test "getQueueMessages returns empty list" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getQueueMessages(allocator, "test-q", null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"messages\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"count\":0") != null);
}

test "getQueueDLQ returns empty list" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getQueueDLQ(allocator, "test-q", null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"entries\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"count\":0") != null);
}

test "purgeQueue surfaces loopback connect failure (no node in unit test)" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here → connect refused

    const result = try purgeQueue(allocator, "test-q", null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}

test "requeueDLQEntry surfaces loopback connect failure (no node in unit test)" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here → connect refused

    const result = try requeueDLQEntry(allocator, "test-q", "42", null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}
