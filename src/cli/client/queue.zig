//! Queue Client Operations
//!
//! Queue operations for the Flo CLI client.
//! All functions take a *Client and namespace as parameters.

const std = @import("std");
const base = @import("base.zig");
const wire = @import("../../util/wire.zig");
const WireReader = wire.WireReader;
const Client = base.Client;
const Response = base.Response;
const proto = @import("../../protocol/proto.zig");
const FixedWireWriter = wire.FixedWireWriter;

/// Enqueue a message to a queue
pub fn enqueue(
    client: *Client,
    namespace: []const u8,
    queue: []const u8,
    payload: []const u8,
    priority: u8,
    delay_ms: u64,
) !Response {
    var options_buf: [16]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);
    try builder.addU8(.priority, priority);
    if (delay_ms > 0) try builder.addU64(.delay_ms, delay_ms);

    return client.sendRequestWithOptions(.queue_enqueue, namespace, queue, payload, builder.getOptions());
}

/// Purge a queue's messages, ready, leased and dead. The response's raw data
/// is a u32 LE count of removed messages.
pub fn purge(client: *Client, namespace: []const u8, queue: []const u8) !Response {
    return client.sendRequest(.queue_purge, namespace, queue, "");
}

const queue_ops = @import("../../protocol/queue_ops.zig");

/// Take up to `count` of a queue's next ready messages, leased to
/// `consumer` for `lease_ms` (null: the server's 30 s), or with `ack`
/// completed as they're taken. `block_ms`: null or 0 answers at once, more
/// waits that long for a message (the server refuses more than 5 minutes).
pub fn dequeue(client: *Client, namespace: []const u8, queue: []const u8, count: u32, block_ms: ?u32, ack: bool, consumer: []const u8, lease_ms: ?u64) !Response {
    const ms: u64 = block_ms orelse 0;
    // Read for as long as the server waits, plus 5 s.
    if (ms > 0) client.setReadTimeoutSec(@intCast(ms / 1000 + 5));
    return client.call(queue_ops.op("queue.dequeue"), namespace, queue, .{ .consumer = consumer, .count = count, .lease_ms = lease_ms, .block_ms = ms, .ack = ack });
}

/// A dequeued message's delivery, as the CLI prints and takes it:
/// `term.index.seq.deliveries`.
pub const DeliveryId = struct {
    term: u64,
    index: u64,
    seq: u64,
    deliveries: u32,

    pub fn parse(s: []const u8) ?DeliveryId {
        var it = std.mem.splitScalar(u8, s, '.');
        const term = std.fmt.parseInt(u64, it.next() orelse return null, 10) catch return null;
        const index = std.fmt.parseInt(u64, it.next() orelse return null, 10) catch return null;
        const seq = std.fmt.parseInt(u64, it.next() orelse return null, 10) catch return null;
        const deliveries = std.fmt.parseInt(u32, it.next() orelse return null, 10) catch return null;
        if (it.next() != null) return null;
        return .{ .term = term, .index = index, .seq = seq, .deliveries = deliveries };
    }

    pub fn format(self: DeliveryId, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try w.print("{d}.{d}.{d}.{d}", .{ self.term, self.index, self.seq, self.deliveries });
    }
};

const Items = @import("../../protocol/schema.zig").List(queue_ops.Item, queue_ops.MAX_BATCH);

/// `ids`, which must name one delivery, as a report names its messages.
fn itemsOf(buf: []u8, ids: []const DeliveryId) !Items.Value {
    if (ids.len == 0 or ids.len > queue_ops.MAX_BATCH) return error.InvalidRequest;
    var b = Items.Builder.init(buf);
    for (ids) |id| {
        if (id.term != ids[0].term or id.index != ids[0].index) return error.InvalidRequest;
        if (!(b.add(.{ .seq = id.seq, .deliveries = id.deliveries }) catch false)) return error.InvalidRequest;
    }
    return b.value();
}

/// Complete messages of one delivery: they leave the queue.
pub fn complete(client: *Client, namespace: []const u8, queue: []const u8, consumer: []const u8, ids: []const DeliveryId) !Response {
    var buf: [Items.max_size]u8 = undefined;
    const items = try itemsOf(&buf, ids);
    return client.call(queue_ops.op("queue.complete"), namespace, queue, .{ .consumer = consumer, .term = ids[0].term, .index = ids[0].index, .messages = items });
}

/// Fail messages of one delivery: ready again after `delay_ms`, or dead
/// at once with `dead` or once their attempts are spent.
pub fn fail(client: *Client, namespace: []const u8, queue: []const u8, consumer: []const u8, ids: []const DeliveryId, delay_ms: u64, dead: bool, error_message: []const u8) !Response {
    var buf: [Items.max_size]u8 = undefined;
    const items = try itemsOf(&buf, ids);
    return client.call(queue_ops.op("queue.fail"), namespace, queue, .{ .consumer = consumer, .term = ids[0].term, .index = ids[0].index, .messages = items, .delay_ms = delay_ms, .dead = dead, .@"error" = error_message });
}

/// Keep messages of one delivery: their lease runs on.
pub fn touch(client: *Client, namespace: []const u8, queue: []const u8, consumer: []const u8, ids: []const DeliveryId) !Response {
    var buf: [Items.max_size]u8 = undefined;
    const items = try itemsOf(&buf, ids);
    return client.call(queue_ops.op("queue.touch"), namespace, queue, .{ .consumer = consumer, .term = ids[0].term, .index = ids[0].index, .messages = items });
}

/// A page of up to `limit` dead letters, past `after` (the cursor the page
/// before ended at).
pub fn dlqList(client: *Client, namespace: []const u8, queue: []const u8, limit: u32, after: ?queue_ops.DeadCursor.Value) !Response {
    return client.call(queue_ops.op("queue.dlq_list"), namespace, queue, .{ .limit = limit, .after = after });
}

/// Requeue dead letters by seq.
pub fn dlqRequeue(client: *Client, namespace: []const u8, queue: []const u8, seqs: []const u64) !Response {
    var buf: [queue_ops.MAX_BATCH * 9]u8 = undefined;
    return client.call(queue_ops.op("queue.dlq_requeue"), namespace, queue, .{ .messages = try seqList(&buf, seqs), .all = false, .limit = queue_ops.MAX_BATCH });
}

/// Delete dead letters by seq, for good.
pub fn dlqDelete(client: *Client, namespace: []const u8, queue: []const u8, seqs: []const u64) !Response {
    var buf: [queue_ops.MAX_BATCH * 9]u8 = undefined;
    return client.call(queue_ops.op("queue.dlq_delete"), namespace, queue, .{ .messages = try seqList(&buf, seqs), .all = false, .limit = queue_ops.MAX_BATCH, .dry_run = false });
}

const schema = @import("../../protocol/schema.zig");
pub const Seqs = schema.List(schema.Int(u64), queue_ops.MAX_BATCH);

/// `seqs` as an op's list of them.
pub fn seqList(buf: []u8, seqs: []const u64) !Seqs.Value {
    if (seqs.len > queue_ops.MAX_BATCH) return error.TooManyMessages;
    var w: schema.Writer = .{ .buf = buf };
    for (seqs) |s| try schema.Int(u64).encode(&w, s);
    return .{ .bytes = buf[0..w.pos], .count = @intCast(seqs.len) };
}

/// List queues in a namespace
/// Returns pre-serialized wire format:
/// [count:u32] ([name_len:u32][name][ns_len:u32][ns][pending:u64][available:u64][enqueued:u64][dequeued:u64][dlq:u64])* [has_more:u8] [cursor_len:u16][cursor]
pub fn list(client: *Client, namespace: []const u8, limit: ?u32, cursor: ?[]const u8) !Response {
    var value_buf: [base.WALK_VALUE_MAX]u8 = undefined;
    return client.sendRequest(.queue_list, namespace, "", try base.walkValue(&value_buf, limit, cursor));
}
