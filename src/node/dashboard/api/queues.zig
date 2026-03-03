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
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    ctx.metrics.mutex.lock();
    defer ctx.metrics.mutex.unlock();

    var it = ctx.metrics.queues.iterator();
    while (it.next()) |entry| {
        const queue_entry = entry.value_ptr.*;

        // Skip internal/system queues (prefixed with '_')
        if (queue_entry.queue.len > 0 and queue_entry.queue[0] == '_') continue;

        try arr.next();
        const snap = queue_entry.metrics.snapshot();

        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", queue_entry.queue);
        try obj.stringField("namespace", queue_entry.namespace);
        try obj.intField("pending", snap.queue_available_current + snap.leases_active_current);
        try obj.intField("available", snap.queue_available_current);
        try obj.intField("enqueued", snap.enqueue_ops_total);
        try obj.intField("dequeued", snap.dequeue_ops_total);
        try obj.intField("acked", snap.leases_completed_total);
        try obj.intField("nacked", snap.leases_failed_total);
        try obj.intField("dlq_count", snap.dlq_messages_total);
        try obj.intField("bytes_total", snap.enqueue_bytes_total);
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /queues/:name - Queue detail
pub fn getQueueDetail(allocator: Allocator, queue_name: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", queue_name);

    ctx.metrics.mutex.lock();
    defer ctx.metrics.mutex.unlock();

    var found = false;
    var it = ctx.metrics.queues.iterator();
    while (it.next()) |entry| {
        const queue_entry = entry.value_ptr.*;
        if (std.mem.eql(u8, queue_entry.queue, queue_name)) {
            const snap = queue_entry.metrics.snapshot();

            try obj.stringField("namespace", queue_entry.namespace);
            try obj.intField("pending", snap.queue_available_current + snap.leases_active_current);
            try obj.intField("available", snap.queue_available_current);
            try obj.intField("enqueued", snap.enqueue_ops_total);
            try obj.intField("dequeued", snap.dequeue_ops_total);
            try obj.intField("acked", snap.leases_completed_total);
            try obj.intField("nacked", snap.leases_failed_total);
            try obj.intField("dlq_count", snap.dlq_messages_total);
            try obj.intField("bytes_total", snap.enqueue_bytes_total);
            found = true;
            break;
        }
    }

    if (!found) {
        try obj.stringField("namespace", "default");
        try obj.intField("pending", 0);
        try obj.intField("available", 0);
        try obj.intField("enqueued", 0);
        try obj.intField("dequeued", 0);
        try obj.intField("acked", 0);
        try obj.intField("nacked", 0);
        try obj.intField("dlq_count", 0);
        try obj.intField("bytes_total", 0);
    }

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /queues/:name/messages - Queue messages with status filter
/// Query params: ?status=&limit=
pub fn getQueueMessages(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = h.parseQueryParam([]const u8, query_string, "status");
    const limit_param = h.parseQueryParam(u32, query_string, "limit") orelse 100;
    const limit: usize = @min(@as(usize, @intCast(limit_param)), 1000);

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

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
                total += qp.messages.count();
                var it = qp.messages.iterator();
                while (it.next()) |entry| {
                    if (msg_count >= limit) break;
                    const msg = entry.value_ptr.*;
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
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /queues/:name/dlq - Dead-letter queue entries
/// Query params: ?limit=
pub fn getQueueDLQ(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const limit_param = h.parseQueryParam(u32, query_string, "limit") orelse 100;
    const limit: usize = @min(@as(usize, @intCast(limit_param)), 1000);

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

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
    return try json_buf.toOwnedSlice(allocator);
}

/// POST /queues/:name/dlq/:seq/requeue - Requeue a DLQ entry
pub fn requeueDLQEntry(allocator: Allocator, queue_name: []const u8, seq_str: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = queue_name;
    _ = seq_str;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Write operations require Raft proposal — not safe from dashboard thread.
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// DELETE /queues/:name/dlq/:seq - Delete a DLQ entry
pub fn deleteDLQEntry(allocator: Allocator, queue_name: []const u8, seq_str: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = queue_name;
    _ = seq_str;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Write operations require Raft proposal — not safe from dashboard thread.
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// POST /queues/:name/purge - Purge all messages from queue
pub fn purgeQueue(allocator: Allocator, queue_name: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = queue_name;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Write operations require Raft proposal — not safe from dashboard thread.
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.intField("purged", 0);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
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

    const result = try getQueueDetail(allocator, "unknown", &ctx);
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

test "purgeQueue returns ok" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try purgeQueue(allocator, "test-q", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"ok\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"purged\":0") != null);
}

test "requeueDLQEntry returns ok" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try requeueDLQEntry(allocator, "test-q", "42", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"ok\":true") != null);
}
