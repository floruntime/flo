//! Queue Client Operations
//!
//! Queue operations for the Flo CLI client.
//! All functions take a *Client and namespace as parameters.

const std = @import("std");
const base = @import("base.zig");
const wire = @import("../../util/wire.zig");
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
) !Response {
    var options_buf: [8]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);
    try builder.addU8(.priority, priority);

    return client.sendRequestWithOptions(.queue_enqueue, namespace, queue, payload, builder.getOptions());
}

/// Purge all live (ready + leased) messages from a queue. The response's raw
/// data is a u32 LE count of removed messages.
pub fn purge(client: *Client, namespace: []const u8, queue: []const u8) !Response {
    return client.sendRequest(.queue_purge, namespace, queue, "");
}

/// Dequeue messages from a queue
/// block_ms: null or 0 = no blocking, >0 = block for N ms (the server refuses more than 5 minutes)
pub fn dequeue(client: *Client, namespace: []const u8, queue: []const u8, count: u32, block_ms: ?u32) !Response {
    var options_buf: [48]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    try builder.addU32(.count, count);

    // Only add block_ms option if blocking is requested
    if (block_ms) |ms| {
        try builder.addU32(.block_ms, ms);
        // Read for as long as the server waits, plus 5 s; 0 does not wait.
        if (ms > 0) client.setReadTimeoutSec(ms / 1000 + 5);
    }

    return client.sendRequestWithOptions(.queue_dequeue, namespace, queue, "", builder.getOptions());
}

/// Acknowledge message processing (complete)
pub fn ack(client: *Client, namespace: []const u8, queue: []const u8, seqs: []const u64) !Response {
    // Format: [count:u32][seq:u64]*
    var writer = FixedWireWriter(4096).init();
    try writer.writeU64ArrayWithCount(seqs);

    return client.sendRequest(.queue_complete, namespace, queue, writer.bytes());
}

/// Negative acknowledge (return to queue or send to DLQ)
pub fn nack(client: *Client, namespace: []const u8, queue: []const u8, seqs: []const u64) !Response {
    // Format: [count:u32][seq:u64]*
    var writer = FixedWireWriter(4096).init();
    try writer.writeU64ArrayWithCount(seqs);

    return client.sendRequest(.queue_fail, namespace, queue, writer.bytes());
}

/// A queue's dead-letter count; listing the messages themselves isn't
/// supported yet.
pub fn dlqList(client: *Client, namespace: []const u8, queue: []const u8) !Response {
    return client.sendRequest(.queue_dlq_list, namespace, queue, "");
}

/// Requeue messages from DLQ
pub fn dlqRequeue(client: *Client, namespace: []const u8, queue: []const u8, seqs: []const u64) !Response {
    // Format: [count:u32][seq:u64]*
    var writer = FixedWireWriter(4096).init();
    try writer.writeU64ArrayWithCount(seqs);

    return client.sendRequest(.queue_dlq_requeue, namespace, queue, writer.bytes());
}

/// Peek at messages without creating leases
pub fn peek(client: *Client, namespace: []const u8, queue: []const u8, count: u32) !Response {
    var options_buf: [16]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    try builder.addU32(.count, count);

    return client.sendRequestWithOptions(.queue_peek, namespace, queue, "", builder.getOptions());
}

/// List queues in a namespace
/// Returns pre-serialized wire format:
/// [count:u32] ([name_len:u32][name][ns_len:u32][ns][pending:u64][available:u64][enqueued:u64][dequeued:u64][dlq:u64])* [has_more:u8] [cursor_len:u16][cursor]
pub fn list(client: *Client, namespace: []const u8, limit: ?u32, cursor: ?[]const u8) !Response {
    var value_buf: [base.WALK_VALUE_MAX]u8 = undefined;
    return client.sendRequest(.queue_list, namespace, "", try base.walkValue(&value_buf, limit, cursor));
}
