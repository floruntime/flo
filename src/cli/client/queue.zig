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
    delay_ms: ?u64,
    dedup_key: ?[]const u8,
) !Response {
    var options_buf: [64]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    // Add priority
    try builder.addU8(.priority, priority);

    // Add delay if specified
    if (delay_ms) |delay| {
        try builder.addU64(.delay_ms, delay);
    }

    // Add dedup_key if specified
    if (dedup_key) |key| {
        try builder.addBytes(.dedup_key, key);
    }

    return client.sendRequestWithOptions(.queue_enqueue, namespace, queue, payload, builder.getOptions());
}

/// Dequeue messages from a queue
/// block_ms: null = no blocking, 0 = block forever, >0 = block for N ms
pub fn dequeue(client: *Client, namespace: []const u8, queue: []const u8, count: u32, timeout_ms: u32, block_ms: ?u32) !Response {
    var options_buf: [48]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    try builder.addU32(.count, count);
    try builder.addU32(.visibility_timeout_ms, timeout_ms);

    // Only add block_ms option if blocking is requested
    if (block_ms) |ms| {
        try builder.addU32(.block_ms, ms);
        // Adjust socket read timeout for blocking requests
        if (ms == 0) {
            client.setReadTimeoutSec(0); // infinite
        } else {
            client.setReadTimeoutSec(ms / 1000 + 5);
        }
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
pub fn nack(client: *Client, namespace: []const u8, queue: []const u8, seqs: []const u64, to_dlq: bool) !Response {
    var options_buf: [8]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    try builder.addU8(.send_to_dlq, if (to_dlq) 1 else 0);

    // Format: [count:u32][seq:u64]*
    var writer = FixedWireWriter(4096).init();
    try writer.writeU64ArrayWithCount(seqs);

    return client.sendRequestWithOptions(.queue_fail, namespace, queue, writer.bytes(), builder.getOptions());
}

/// List DLQ messages
pub fn dlqList(client: *Client, namespace: []const u8, queue: []const u8, limit: u32) !Response {
    var options_buf: [16]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    try builder.addU32(.limit, limit);

    return client.sendRequestWithOptions(.queue_dlq_list, namespace, queue, "", builder.getOptions());
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

/// Touch (renew lease) for messages to prevent timeout
/// extend_ms: 0 = reset to original timeout, >0 = extend by N ms
pub fn touch(client: *Client, namespace: []const u8, queue: []const u8, seqs: []const u64, extend_ms: u32) !Response {
    // Format: [count:u32][seq:u64]*
    var writer = FixedWireWriter(4096).init();
    try writer.writeU64ArrayWithCount(seqs);

    if (extend_ms > 0) {
        var options_buf: [16]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        try builder.addU32(.extend_ms, extend_ms);
        return client.sendRequestWithOptions(.queue_touch, namespace, queue, writer.bytes(), builder.getOptions());
    } else {
        return client.sendRequest(.queue_touch, namespace, queue, writer.bytes());
    }
}

/// List queues in a namespace
/// Returns pre-serialized wire format:
/// [count:u32] ([name_len:u32][name][ns_len:u32][ns][pending:u64][available:u64][enqueued:u64][dequeued:u64][dlq:u64])* [has_more:u8] [cursor_len:u16][cursor]
pub fn list(client: *Client, namespace: []const u8, limit: ?u32, cursor: ?[]const u8) !Response {
    var options_buf: [64]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (limit) |l| {
        try builder.addU32(.limit, l);
    }

    if (cursor) |c| {
        try builder.addBytes(.cursor, c);
    }

    return client.sendRequestWithOptions(.queue_list, namespace, "", "", builder.getOptions());
}
