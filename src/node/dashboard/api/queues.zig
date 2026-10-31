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
const namespaces_api = @import("namespaces.zig");
const output = @import("../../../cli/output.zig");
const client_mod = @import("../../../cli/client/mod.zig");
const WireReader = client_mod.WireReader;

/// `queue_list` entries: [name_len:u32][name][ns_len:u32][ns]
/// [pending:u64][available:u64][enqueued:u64][dequeued:u64][dlq:u64].
const queue_list_columns = [_]output.WireColumn{
    .{ .field = "name", .header = "", .field_type = .str_u32 },
    .{ .field = "namespace", .header = "", .field_type = .str_u32 },
    .{ .field = "pending", .header = "", .field_type = .uint_u64 },
    .{ .field = "available", .header = "", .field_type = .uint_u64 },
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
/// from `queue_list`, which asks every shard. Its counts are enqueued less
/// dequeued, so `ready`, `inflight` and `available` (ready alone) are null.
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
            const pending, _, const enqueued, const dequeued, const dlq = v;
            if (name.len > 0 and name[0] == '_') continue; // system queue
            try arr.next();
            var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try obj.begin();
            try obj.stringField("name", name);
            try obj.stringField("namespace", ns);
            try obj.nullField("ready");
            try obj.nullField("inflight");
            try obj.intField("pending", pending);
            try obj.nullField("available");
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
    if (got.isError()) return try h.jsonError(allocator, got.errorMessage());
    const s = parseStats(got.data) orelse return try h.jsonError(allocator, "queue stats: malformed answer");

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

/// GET /queues/:name/messages - No op lists a queue's messages yet, so this
/// says so rather than answer an empty list.
pub fn getQueueMessages(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = queue_name;
    _ = query_string;
    _ = ctx;
    return try h.jsonError(allocator, "queue messages are not available yet");
}

/// GET /queues/:name/dlq?namespace= - The queue's dead-letter count, from
/// `queue_dlq_list`. No op lists the entries yet: `entries` is null.
pub fn getQueueDLQ(allocator: Allocator, queue_name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "queue dlq", err);
    defer reader.deinit();
    const client = reader.next() catch |err| return try loopback.readFailed(allocator, "queue dlq", err);
    var got = client_mod.queue.dlqList(client, ns_q, queue_name) catch |err|
        return try loopback.readFailed(allocator, "queue dlq", err);
    defer got.deinit();
    if (got.isError()) return try h.jsonError(allocator, got.errorMessage());
    // [count:u32 = 0][total_dlq:u64]: a summary, no entries.
    var r = WireReader.init(got.data);
    _ = r.readU32() orelse return try h.jsonError(allocator, "queue dlq: malformed answer");
    const total = r.readU64() orelse return try h.jsonError(allocator, "queue dlq: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.nullField("entries");
    try obj.intField("count", total);
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

test "queue messages and DLQ deletes say they are not available yet" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1;

    const msgs = try getQueueMessages(allocator, "q", null, &ctx);
    defer allocator.free(msgs);
    try std.testing.expectEqualStrings("{\"error\":\"queue messages are not available yet\"}", msgs);
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
