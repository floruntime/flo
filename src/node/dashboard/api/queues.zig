//! Dashboard API — Queue Endpoints
//!
//! - GET    /queues                           — All queues
//! - GET    /queues/:name                     — Queue detail
//! - GET    /queues/:name/messages            — Queue messages
//! - GET    /queues/:name/dlq                 — Dead-letter queue count
//! - POST   /queues/:name/dlq/:seq/requeue   — Requeue DLQ entry
//! - DELETE /queues/:name/dlq/:seq            — Delete DLQ entry
//! - POST   /queues/:name/purge              — Purge queue
//!
//! The dashboard asks each shard, as any client does: every read goes over
//! loopback to the node's own protocol port, and the shard that owns the
//! queue answers it.

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const loopback = @import("loopback.zig");
const schema = @import("../../../protocol/schema.zig");
const queue_ops = @import("../../../protocol/queue_ops.zig");
const namespaces_api = @import("namespaces.zig");
const output = @import("../../../cli/output.zig");
const client_mod = @import("../../../cli/client/mod.zig");
const WireReader = client_mod.WireReader;

/// `queue_list` entries: [name_len:u32][name][ns_len:u32][ns]
/// [ready:u64][leased:u64][enqueued:u64][dequeued:u64][dlq:u64].
const queue_list_columns = [_]output.WireColumn{
    .{ .field = "name", .header = "", .field_type = .str_u32 },
    .{ .field = "namespace", .header = "", .field_type = .str_u32 },
    .{ .field = "ready", .header = "", .field_type = .uint_u64 },
    .{ .field = "leased", .header = "", .field_type = .uint_u64 },
    .{ .field = "enqueued", .header = "", .field_type = .uint_u64 },
    .{ .field = "dequeued", .header = "", .field_type = .uint_u64 },
    .{ .field = "dlq", .header = "", .field_type = .uint_u64 },
};

const QueueListAsker = struct {
    namespace: []const u8,
    pub fn ask(self: QueueListAsker, client: *client_mod.Client, cursor: ?[]const u8) !client_mod.Response {
        return client_mod.queue.list(client, self.namespace, null, cursor);
    }
};

/// GET /queues - Every namespace's queues, or `?namespace=`'s alone,
/// from `queue_list`, which asks every shard: each queue's messages ready
/// and leased (`inflight`), as the queue keeps them.
pub fn getQueues(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "queue list", err);
    defer reader.deinit();
    var arena: std.heap.ArenaAllocator = .init(allocator);
    defer arena.deinit();
    var namespaces: std.StringArrayHashMapUnmanaged(void) = .empty;
    if (h.parseQueryParam([]const u8, query_string, "namespace")) |ns| {
        try namespaces.put(arena.allocator(), ns, {});
    } else {
        try namespaces.put(arena.allocator(), "default", {});
        namespaces_api.readNamespaceNames(&reader, arena.allocator(), &namespaces) catch |err|
            return try loopback.readFailed(allocator, "queue list", err);
    }

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    const malformed = "queue list: malformed answer";
    for (namespaces.keys()) |namespace| {
        var pages = reader.listAll(allocator, &queue_list_columns, QueueListAsker{ .namespace = namespace }) catch |err|
            return try loopback.readFailed(allocator, "queue list", err);
        defer pages.deinit();
        var r = WireReader.init(pages.list());
        const count = r.readU32() orelse 0;
        for (0..count) |_| {
            const name = r.readLengthPrefixed(u32) orelse return try h.jsonError(allocator, malformed);
            const ns = r.readLengthPrefixed(u32) orelse return try h.jsonError(allocator, malformed);
            var v: [5]u64 = undefined;
            for (&v) |*x| x.* = r.readU64() orelse return try h.jsonError(allocator, malformed);
            const ready, const leased, const enqueued, const dequeued, const dlq = v;
            if (name.len > 0 and name[0] == '_') continue; // system queue
            try arr.next();
            var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try obj.begin();
            try obj.stringField("name", name);
            try obj.stringField("namespace", ns);
            try obj.intField("ready", ready);
            try obj.intField("inflight", leased);
            try obj.intField("pending", ready + leased);
            try obj.intField("available", ready);
            try obj.intField("enqueued", enqueued);
            try obj.intField("dequeued", dequeued);
            try obj.intField("dlq_count", dlq);
            try obj.end();
        }
    }

    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// A `queue_stats` answer, nine u64s: [enqueued][dequeued][acked][nacked]
/// [dlq_total][leases_expired][ready][leased][dlq_current].
const QueueStats = struct { enqueued: u64, dequeued: u64, ready: u64, leased: u64, dlq: u64 };

fn parseStats(data: []const u8) ?QueueStats {
    var r = WireReader.init(data);
    var v: [9]u64 = undefined;
    for (&v) |*x| x.* = r.readU64() orelse return null;
    return .{ .enqueued = v[0], .dequeued = v[1], .ready = v[6], .leased = v[7], .dlq = v[8] };
}

/// GET /queues/:name - Queue detail, from `queue_stats` (answered by the
/// shard that owns the queue). An unknown queue answers zeros.
pub fn getQueueDetail(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "queue stats", err);
    defer reader.deinit();
    const client = reader.next() catch |err| return try loopback.readFailed(allocator, "queue stats", err);
    var got = client.sendRequest(.queue_stats, ns_q, queue_name, "") catch |err|
        return try loopback.readFailed(allocator, "queue stats", err);
    defer got.deinit();
    if (got.isError() and !got.isNotFound()) return try h.jsonError(allocator, got.errorMessage());
    // A namespace never created holds the queue as any unknown one: zeros.
    const s: QueueStats = if (got.isNotFound()) .{ .enqueued = 0, .dequeued = 0, .ready = 0, .leased = 0, .dlq = 0 } else parseStats(got.data) orelse return try h.jsonError(allocator, "queue stats: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", queue_name);
    try obj.stringField("namespace", ns_q);
    try obj.intField("ready", s.ready);
    try obj.intField("inflight", s.leased);
    try obj.intField("pending", s.ready + s.leased);
    try obj.intField("available", s.ready);
    try obj.intField("enqueued", s.enqueued);
    try obj.intField("dequeued", s.dequeued);
    try obj.intField("dlq_count", s.dlq);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /queues/:name/messages?namespace=&limit= - The queue's next ready
/// messages, in the order a dequeue takes them, none taken (`queue.peek`,
/// on the queue's leader). Up to `limit` (default 20, at most 100), fewer
/// when their payloads fill the answer; `more` says more may follow.
pub fn getQueueMessages(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const limit = std.math.clamp(h.parseQueryParam(u32, query_string, "limit") orelse 20, 1, queue_ops.MAX_PEEK);

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "queue peek", err);
    defer reader.deinit();
    const Peek = queue_ops.op("queue.peek");
    var got = reader.call(Peek, ns_q, queue_name, .{ .limit = limit }) catch |err| return try loopback.readFailed(allocator, "queue peek", err);
    defer got.deinit();

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("queue", queue_name);
    {
        var arr = try obj.arrayField("messages");
        try arr.begin();
        // A namespace never created holds none.
        if (!got.isNotFound()) {
            if (got.isError()) return try h.jsonError(allocator, got.errorMessage());
            var diag: schema.Diagnostic = .{};
            const page = Peek.response.decodeAll(got.data, &diag) catch return try h.jsonError(allocator, "queue peek: malformed answer");
            var it = page.messages.iterator();
            while (it.next() catch return try h.jsonError(allocator, "queue peek: malformed answer")) |m| {
                try arr.next();
                var mobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try mobj.begin();
                try mobj.intField("seq", m.seq);
                try mobj.intField("priority", m.priority);
                try mobj.intField("attempts", m.attempts);
                try mobj.intField("enqueued_at", m.enqueued_at_ms);
                try mobj.intField("size", m.size);
                try mobj.stringField("payload", m.payload);
                try mobj.boolField("truncated", m.size > m.payload.len);
                try mobj.end();
            }
            try arr.end();
            try obj.boolField("more", page.more);
            try obj.end();
            return try json_aw.toOwnedSlice();
        }
        try arr.end();
    }
    try obj.boolField("more", false);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /queues/:name/dlq?namespace=&limit=&after= - A page of the queue's
/// dead letters, oldest first (`queue_dlq_list`): up to `limit` (at most
/// 100), after the entry whose seq is `after`, the last of the page before.
pub fn getQueueDLQ(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const limit = std.math.clamp(h.parseQueryParam(u32, query_string, "limit") orelse 100, 1, 100);
    const after = h.parseQueryParam(u64, query_string, "after");

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "queue dlq", err);
    defer reader.deinit();
    const client = reader.next() catch |err| return try loopback.readFailed(allocator, "queue dlq", err);
    var got = client_mod.queue.dlqList(client, ns_q, queue_name, limit, after) catch |err|
        return try loopback.readFailed(allocator, "queue dlq", err);
    defer got.deinit();
    if (got.isError() and !got.isNotFound()) return try h.jsonError(allocator, got.errorMessage());
    // A namespace never created holds none.
    const empty: client_mod.queue.DlqPage = .{ .count = 0, .entries = "", .total = 0, .more = false, .last_seq = 0 };
    const page = if (got.isNotFound()) empty else client_mod.queue.DlqPage.decode(got.data) orelse
        return try h.jsonError(allocator, "queue dlq: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    {
        var arr = try obj.arrayField("entries");
        try arr.begin();
        var it = page.iterator();
        while (it.next()) |d| {
            try arr.next();
            var e = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try e.begin();
            try e.intField("seq", d.seq);
            try e.intField("attempts", d.attempts);
            try e.intField("priority", d.priority);
            try e.intField("enqueued_at", d.enqueued_at_ms);
            try e.intField("moved_at", d.moved_at_ms);
            try e.intField("size", d.size);
            try e.stringField("payload", d.payload);
            try e.boolField("truncated", d.size > d.payload.len);
            try e.end();
        }
        try arr.end();
    }
    try obj.intField("count", page.total);
    try obj.boolField("has_more", page.more);
    if (page.more) try obj.intField("next_after", page.last_seq) else try obj.nullField("next_after");
    try obj.stringField("queue", queue_name);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// POST /queues/:name/dlq/:seq/requeue?namespace= - Requeue a DLQ entry (loopback write)
pub fn requeueDLQEntry(allocator: Allocator, queue_name: []const u8, seq_str: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const seq = std.fmt.parseInt(u64, seq_str, 10) catch return try h.jsonError(allocator, "Invalid seq");

    var client = loopback.connect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
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

/// DELETE /queues/:name/dlq/:seq - The server can't delete a dead letter
/// yet, so this refuses rather than answer ok.
pub fn deleteDLQEntry(allocator: Allocator, queue_name: []const u8, seq_str: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = queue_name;
    _ = seq_str;
    return try h.jsonError(allocator, "DLQ entries can't be deleted yet");
}

/// POST /queues/:name/purge - Purge all live (ready+leased) messages (loopback write).
pub fn purgeQueue(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var client = loopback.connect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
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
/// `?priority=` optional.
pub fn enqueueMessage(allocator: Allocator, queue_name: []const u8, body: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const priority: u8 = @intCast(@min(h.parseQueryParam(u64, query_string, "priority") orelse 0, 255));
    // Queues have no delayed delivery; a delay asked for is refused, not dropped.
    if (h.parseQueryParam([]const u8, query_string, "delay_ms") != null)
        return try h.jsonError(allocator, "delay_ms is not supported: queues have no delayed delivery");

    var client = loopback.connect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.queue.enqueue(&client, ns_q, queue_name, body, priority) catch
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

test "queue reads say the node didn't answer, never an empty list" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here → connect refused

    const answers = [_][]const u8{
        try getQueues(allocator, null, &ctx),
        try getQueueDetail(allocator, "q", null, &ctx),
        try getQueueDLQ(allocator, "q", null, &ctx),
    };
    defer for (answers) |a| allocator.free(a);
    for (answers) |a| {
        try std.testing.expect(std.mem.startsWith(u8, a, "{\"error\":"));
        try std.testing.expect(std.mem.indexOf(u8, a, "failed") != null);
    }
}

test "queue messages say the node didn't answer, and DLQ deletes that they can't be done yet" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    // Nothing listens on its port: an endpoint that opened a connection
    // would answer the failure instead.
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1;

    const msgs = try getQueueMessages(allocator, "q", null, &ctx);
    defer allocator.free(msgs);
    try std.testing.expect(std.mem.startsWith(u8, msgs, "{\"error\":\"queue peek failed"));
    const del = try deleteDLQEntry(allocator, "q", "1", &ctx);
    defer allocator.free(del);
    try std.testing.expectEqualStrings("{\"error\":\"DLQ entries can't be deleted yet\"}", del);
}

test "queue stats answers parse; a short one doesn't" {
    var buf: [72]u8 = undefined;
    for (0..9) |i| std.mem.writeInt(u64, buf[i * 8 ..][0..8], i + 1, .little);
    const s = parseStats(&buf).?;
    try std.testing.expectEqual(@as(u64, 1), s.enqueued);
    try std.testing.expectEqual(@as(u64, 2), s.dequeued);
    try std.testing.expectEqual(@as(u64, 7), s.ready);
    try std.testing.expectEqual(@as(u64, 8), s.leased);
    try std.testing.expectEqual(@as(u64, 9), s.dlq);
    try std.testing.expect(parseStats(buf[0..64]) == null);
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
