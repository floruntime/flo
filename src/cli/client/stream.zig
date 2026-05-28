//! Stream Client Operations
//!
//! Stream operations for the Flo CLI client.
//! All functions take a *Client and namespace/stream parameters.
//!
//! Batch appends are sent as a single request with wire format:
//! [count:u32][payload_len:u32][payload][header_count:u16][key_len:u16][key][value_len:u16][value]...

const std = @import("std");
const base = @import("base.zig");
const wire = @import("../../util/wire.zig");
const Client = base.Client;
const Response = base.Response;
const proto = @import("../../protocol/proto.zig");
const WireWriter = wire.WireWriter;
const WireReader = wire.WireReader;
const FixedWireWriter = wire.FixedWireWriter;
const StreamID = @import("../../stream/stream_id.zig").StreamID;

/// A key-value header for stream records
pub const Header = struct {
    key: []const u8,
    value: []const u8,
};

/// Result from appending records
pub const AppendResult = struct {
    count: usize,
    first_id: StreamID,
    last_id: StreamID,
};

/// Append records to a stream as a single batch request.
/// Wire format: [count:u32][payload_len:u32][payload][header_count:u16][key_len:u16][key][value_len:u16][value]...
/// Returns the result of the batch append.
pub fn append(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    payloads: []const []const u8,
    headers: ?[]const []const Header,
) !AppendResult {
    return appendEx(client, namespace, stream, payloads, headers, null, null);
}

/// Extended append with partition_key and/or explicit partition
pub fn appendEx(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    payloads: []const []const u8,
    headers: ?[]const []const Header,
    partition_key: ?[]const u8,
    partition: ?u32,
) !AppendResult {
    if (payloads.len == 0) {
        return error.EmptyBatch;
    }

    // Build wire format using WireWriter
    var writer = WireWriter.init(client.allocator);
    defer writer.deinit();

    // Write record count
    try writer.writeU32(@intCast(payloads.len));

    // Write each record: [payload_len:u32][payload][header_count:u16][headers...]
    for (payloads, 0..) |payload, i| {
        try writer.writeLengthPrefixed(u32, payload);

        // Write headers for this record
        const rec_headers: []const Header = if (headers) |hdrs| (if (i < hdrs.len) hdrs[i] else &.{}) else &.{};
        try writer.writeHeaders(rec_headers);
    }

    // Build options for partition_key and partition using TLV OptionsBuilder
    var options_buf: [128]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (partition_key) |pk| {
        try builder.addString(.partition_key, pk);
    }

    if (partition) |p| {
        try builder.addU32(.partition, p);
    }

    const options: []const u8 = builder.getOptions();

    // Send single batch request with options
    var response = try client.sendRequestWithOptions(.stream_append, namespace, stream, writer.bytes(), options);
    defer response.deinit();

    if (response.isError()) {
        return error.ServerError;
    }

    var result = AppendResult{
        .count = payloads.len,
        .first_id = .{ .timestamp_ms = 0, .sequence = 0 },
        .last_id = .{ .timestamp_ms = 0, .sequence = 0 },
    };

    // Parse response: [sequence:u64][timestamp_ms:i64] - 16 bytes
    // Note: Tag byte is NOT included in client response (only in cross-core messages)
    if (response.data.len >= 16) {
        var reader = WireReader.init(response.data);
        const first_seq = reader.readU64() orelse 0;
        const ts_raw = reader.readI64() orelse 0;
        const ts: u64 = @intCast(@max(ts_raw, 0));
        result.first_id = .{ .timestamp_ms = ts, .sequence = first_seq };
        // Last ID: same timestamp, sequence = first + count - 1
        result.last_id = .{ .timestamp_ms = ts, .sequence = first_seq + payloads.len - 1 };
    }

    return result;
}

/// Start mode for stream reads (simplified: tail or StreamID)
pub const StartMode = enum(u8) {
    tail = 1, // Start from end of stream
    stream_id = 2, // Start from specific StreamID (timestamp_ms + sequence)
};

/// Read records from a stream
/// Returns raw Response - caller should parse the data field
pub fn read(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    start_mode: StartMode,
    start_id: ?StreamID,
    end_id: ?StreamID,
    count: ?u32,
    block_ms: ?u32,
    partition: ?u32,
    partition_key: ?[]const u8,
) !Response {
    var options_buf: [192]u8 = undefined; // Increased size for partition_key + two StreamIDs
    var builder = proto.OptionsBuilder.init(&options_buf);

    // StreamID-native protocol options (simplified: tail or stream_id only)
    switch (start_mode) {
        .tail => {
            try builder.addFlag(.stream_tail);
        },
        .stream_id => {
            // Use stream_start with full StreamID (timestamp_ms, sequence)
            const sid = start_id orelse StreamID{ .timestamp_ms = 0, .sequence = 0 };
            try builder.addStreamId(.stream_start, sid.timestamp_ms, sid.sequence);
        },
    }

    // Add end StreamID if specified
    if (end_id) |eid| {
        try builder.addStreamId(.stream_end, eid.timestamp_ms, eid.sequence);
    }

    if (count) |c| {
        try builder.addU32(.count, c);
    }

    if (block_ms) |ms| {
        try builder.addU32(.block_ms, ms);
        // Adjust socket read timeout for blocking requests
        if (ms == 0) {
            client.setReadTimeoutSec(0); // infinite
        } else {
            client.setReadTimeoutSec(ms / 1000 + 5);
        }
    }

    // Add partition if specified
    if (partition) |p| {
        try builder.addU32(.partition, p);
    }

    // Add partition_key if specified (for hash-based partition routing)
    if (partition_key) |pk| {
        try builder.addString(.partition_key, pk);
    }

    // Send StreamID in value field as 2x u64 (fallback for servers that don't parse options)
    var value_writer = FixedWireWriter(16).init();
    const sid = start_id orelse StreamID{ .timestamp_ms = 0, .sequence = 0 };
    value_writer.writeU64(sid.timestamp_ms) catch {};
    value_writer.writeU64(sid.sequence) catch {};

    return client.sendRequestWithOptions(.stream_read, namespace, stream, value_writer.bytes(), builder.getOptions());
}

/// Get stream information (length, first/last sequence, etc.)
/// NOTE: stream_info is not yet implemented on the server
pub fn info(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
) !Response {
    return client.sendRequest(.stream_info, namespace, stream, "");
}

/// Trim stream using retention policies
/// Supports: max_len (count), min_id (StreamID), max_age_seconds, max_bytes, dry_run
pub fn trim(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    max_len: ?u64,
    min_id: ?StreamID,
    max_age_seconds: ?u64,
    max_bytes: ?u64,
    dry_run: bool,
) !Response {
    var options_buf: [64]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (max_len) |len| {
        try builder.addU64(.limit, len);
    }

    if (min_id) |sid| {
        try builder.addStreamId(.stream_start, sid.timestamp_ms, sid.sequence);
    }

    if (max_age_seconds) |age| {
        try builder.addU64(.max_age_seconds, age);
    }

    if (max_bytes) |bytes| {
        try builder.addU64(.max_bytes, bytes);
    }

    if (dry_run) {
        try builder.addFlag(.dry_run);
    }

    return client.sendRequestWithOptions(.stream_trim, namespace, stream, "", builder.getOptions());
}

/// List all streams in a namespace
pub fn list(
    client: *Client,
    namespace: []const u8,
    limit: ?u32,
    cursor: ?[]const u8,
) !Response {
    var options_buf: [64]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (limit) |l| {
        try builder.addU32(.limit, l);
    }

    if (cursor) |c| {
        try builder.addBytes(.cursor, c);
    }

    return client.sendRequestWithOptions(.stream_list, namespace, "", "", builder.getOptions());
}

/// Join a consumer group
pub fn groupJoin(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    consumer: []const u8,
) !Response {
    // Wire format: [group_len:u16][group][consumer_len:u16][consumer]
    var writer = FixedWireWriter(512).init();
    try writer.writePair(u16, u16, group, consumer);

    return client.sendRequest(.stream_group_join, namespace, stream, writer.bytes());
}

/// Read from a consumer group
pub fn groupRead(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    consumer: []const u8,
    count: ?u32,
    block_ms: ?u32,
) !Response {
    return groupReadWithOptions(
        client,
        namespace,
        stream,
        group,
        consumer,
        count,
        block_ms,
        .{},
    );
}

/// Consumer group read options
pub const GroupReadOptions = struct {
    /// Consumer group mode: 0=shared, 1=exclusive, 2=key_shared
    mode: ?u8 = null,
    /// Max standby consumers in exclusive mode (null=unlimited, 0=singleton)
    max_standbys: ?u16 = null,
    /// Number of hash slots for key_shared mode
    num_slots: ?u16 = null,
    /// Time before unacked message auto-redelivers (ms)
    ack_timeout_ms: ?u32 = null,
    /// Max delivery attempts before DLQ (0 = unlimited)
    max_deliver: ?u8 = null,
    /// Delay before NACK'd message becomes visible (ms)
    redelivery_delay_ms: ?u32 = null,
    /// Auto-ack on delivery (at-most-once)
    no_ack: bool = false,
};

/// Read from a consumer group with advanced options
pub fn groupReadWithOptions(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    consumer: []const u8,
    count: ?u32,
    block_ms: ?u32,
    opts: GroupReadOptions,
) !Response {
    var options_buf: [64]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (count) |c| {
        try builder.addU32(.count, c);
    }

    if (block_ms) |ms| {
        try builder.addU32(.block_ms, ms);
        // Adjust socket read timeout for blocking requests
        if (ms == 0) {
            client.setReadTimeoutSec(0); // infinite
        } else {
            client.setReadTimeoutSec(ms / 1000 + 5);
        }
    }

    if (opts.mode) |m| {
        try builder.addU8(.subscription_mode, m);
    }

    if (opts.max_standbys) |m| {
        try builder.addU16(.max_standbys, m);
    }

    if (opts.num_slots) |s| {
        try builder.addU16(.num_slots, s);
    }

    if (opts.ack_timeout_ms) |ms| {
        try builder.addU32(.ack_timeout_ms, ms);
    }

    if (opts.max_deliver) |m| {
        try builder.addU8(.max_deliver, m);
    }

    if (opts.redelivery_delay_ms) |ms| {
        try builder.addU32(.redelivery_delay_ms, ms);
    }

    if (opts.no_ack) {
        try builder.addFlag(.no_ack);
    }

    // Wire format: [group_len:u16][group][consumer_len:u16][consumer]
    var writer = FixedWireWriter(512).init();
    try writer.writePair(u16, u16, group, consumer);

    return client.sendRequestWithOptions(.stream_group_read, namespace, stream, writer.bytes(), builder.getOptions());
}

/// Acknowledge messages in a consumer group
pub fn groupAck(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    consumer: []const u8,
    ids: []const StreamID,
) !Response {
    // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][timestamp_ms:u64][sequence:u64]*
    var writer = FixedWireWriter(4096).init();
    try writer.writeLengthPrefixed(u16, group);
    try writer.writeLengthPrefixed(u16, consumer);
    try writer.writeU32(@intCast(ids.len));
    for (ids) |id| {
        try writer.writeU64(id.timestamp_ms);
        try writer.writeU64(id.sequence);
    }

    return client.sendRequest(.stream_group_ack, namespace, stream, writer.bytes());
}

/// Leave a consumer group
pub fn groupLeave(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    consumer: []const u8,
) !Response {
    // Wire format: [group_len:u16][group][consumer_len:u16][consumer]
    var writer = FixedWireWriter(512).init();
    try writer.writePair(u16, u16, group, consumer);

    return client.sendRequest(.stream_group_leave, namespace, stream, writer.bytes());
}

/// Nack messages (release back for redelivery)
pub fn groupNack(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    consumer: []const u8,
    ids: []const StreamID,
) !Response {
    return groupNackWithDelay(client, namespace, stream, group, consumer, ids, null);
}

/// Nack messages with optional redelivery delay
pub fn groupNackWithDelay(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    consumer: []const u8,
    ids: []const StreamID,
    redelivery_delay_ms: ?u32,
) !Response {
    // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][timestamp_ms:u64][sequence:u64]*
    var writer = FixedWireWriter(4096).init();
    try writer.writeLengthPrefixed(u16, group);
    try writer.writeLengthPrefixed(u16, consumer);
    try writer.writeU32(@intCast(ids.len));
    for (ids) |id| {
        try writer.writeU64(id.timestamp_ms);
        try writer.writeU64(id.sequence);
    }

    if (redelivery_delay_ms) |ms| {
        var options_buf: [16]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        try builder.addU32(.redelivery_delay_ms, ms);
        return client.sendRequestWithOptions(.stream_group_nack, namespace, stream, writer.bytes(), builder.getOptions());
    }

    return client.sendRequest(.stream_group_nack, namespace, stream, writer.bytes());
}

/// Get pending messages for a consumer group
pub fn groupPending(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
) !Response {
    return groupPendingForConsumer(client, namespace, stream, group, null);
}

/// Get pending messages for a consumer group, optionally filtered to a
/// single consumer's PEL. `consumer == null` returns the whole group's PEL.
pub fn groupPendingForConsumer(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    consumer: ?[]const u8,
) !Response {
    // Wire format: [group_len:u16][group]([consumer_len:u16][consumer])?
    var writer = FixedWireWriter(512).init();
    try writer.writeLengthPrefixed(u16, group);
    if (consumer) |c| {
        try writer.writeLengthPrefixed(u16, c);
    }

    return client.sendRequest(.stream_group_pending, namespace, stream, writer.bytes());
}

/// Claim pending entries (FLO-102) — cursor-based PEL scan.
///
/// Scans the group's PEL from `start_id` in StreamID order and claims up to
/// `count` entries whose idle time ≥ `min_idle_ms` for `consumer`, returning
/// the records (payload + headers) plus a trailing 16-byte next-cursor.
///
/// Drain own pending (reconnect): `min_idle_ms = 0`, `start_id = StreamID.MIN`.
/// Steal from idle consumers (rebalance): `min_idle_ms > 0`.
pub fn groupClaim(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    consumer: []const u8,
    min_idle_ms: u32,
    start_id: StreamID,
    count: u32,
) !Response {
    // Wire: [group_len:u16][group][consumer_len:u16][consumer]
    //       [min_idle_ms:u32][start_ts:u64][start_seq:u64][count:u32]
    var writer = FixedWireWriter(512).init();
    try writer.writeLengthPrefixed(u16, group);
    try writer.writeLengthPrefixed(u16, consumer);
    try writer.writeU32(min_idle_ms);
    try writer.writeU64(start_id.timestamp_ms);
    try writer.writeU64(start_id.sequence);
    try writer.writeU32(count);

    return client.sendRequest(.stream_group_claim, namespace, stream, writer.bytes());
}

/// Options for creating a consumer group
pub const GroupCreateOptions = struct {
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    mode: u8 = 0, // 0=shared, 1=exclusive, 2=key_shared
    num_slots: u16 = 256,
    max_standbys: ?u16 = null,
    ack_timeout_ms: u32 = 30_000,
    max_deliver: u8 = 10,
    redelivery_delay_ms: u32 = 0,
    no_ack: bool = false,
};

/// Create a consumer group with configuration
pub fn groupCreate(client: *Client, opts: GroupCreateOptions) !Response {
    // Wire format: [group_len:u16][group]
    var writer = FixedWireWriter(256).init();
    try writer.writeLengthPrefixed(u16, opts.group);

    // Build options
    var options_buf: [64]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    // Mode
    try builder.addU8(.subscription_mode, opts.mode);

    // num_slots
    try builder.addU16(.num_slots, opts.num_slots);

    // max_standbys (only if not unlimited)
    if (opts.max_standbys) |ms| {
        try builder.addU16(.max_standbys, ms);
    }

    // ack_timeout_ms
    try builder.addU32(.ack_timeout_ms, opts.ack_timeout_ms);

    // max_deliver
    try builder.addU8(.max_deliver, opts.max_deliver);

    // redelivery_delay_ms
    if (opts.redelivery_delay_ms > 0) {
        try builder.addU32(.redelivery_delay_ms, opts.redelivery_delay_ms);
    }

    // no_ack
    if (opts.no_ack) {
        try builder.addFlag(.no_ack);
    }

    return client.sendRequestWithOptions(
        .stream_group_create,
        opts.namespace,
        opts.stream,
        writer.bytes(),
        builder.getOptions(),
    );
}

/// Delete a consumer group and all its state
/// Removes config, offset, lease, pending messages, slots, standbys
pub fn groupDelete(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
) !Response {
    // Wire format: [group_len:u16][group]
    var writer = FixedWireWriter(256).init();
    try writer.writeLengthPrefixed(u16, group);

    return client.sendRequest(.stream_group_delete, namespace, stream, writer.bytes());
}

/// Result from touch operation
pub const TouchResult = struct {
    touched_count: u32,
};

/// Touch messages to extend their ack deadline
/// This resets the delivered_at timestamp, giving the consumer more time to ack.
pub fn groupTouch(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    consumer: []const u8,
    ids: []const StreamID,
    extend_ms: ?u32,
) !TouchResult {
    // Wire format: [group_len:u16][group][consumer_len:u16][consumer][count:u32][timestamp_ms:u64][sequence:u64]*
    var writer = FixedWireWriter(4096).init();
    try writer.writePair(u16, u16, group, consumer);
    try writer.writeU32(@intCast(ids.len));
    for (ids) |id| {
        try writer.writeU64(id.timestamp_ms);
        try writer.writeU64(id.sequence);
    }

    var response: Response = undefined;
    if (extend_ms) |ms| {
        var options_buf: [16]u8 = undefined;
        var builder = proto.OptionsBuilder.init(&options_buf);
        try builder.addU32(.extend_ack_ms, ms);
        response = try client.sendRequestWithOptions(.stream_group_touch, namespace, stream, writer.bytes(), builder.getOptions());
    } else {
        response = try client.sendRequest(.stream_group_touch, namespace, stream, writer.bytes());
    }
    defer response.deinit();

    if (response.isError()) {
        return error.ServerError;
    }

    // Response is [touched_count:u32]
    var result = TouchResult{ .touched_count = 0 };
    if (response.data.len >= 4) {
        result.touched_count = std.mem.readInt(u32, response.data[0..4], .little);
    }

    return result;
}

/// Create a stream with specified partition count
pub fn create(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    partition_count: u32,
    retention_count: ?u64,
    retention_age: ?u64,
    retention_bytes: ?u64,
) !Response {
    var writer = FixedWireWriter(128).init();
    try writer.writeU32(partition_count);

    // Build options for retention policies using TLV OptionsBuilder
    var options_buf: [32]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (retention_count) |rc| {
        try builder.addU64(.retention_count, rc);
    } else if (retention_age) |ra| {
        try builder.addU64(.retention_age, ra);
    } else if (retention_bytes) |rb| {
        try builder.addU64(.retention_bytes, rb);
    }

    const options: []const u8 = builder.getOptions();

    return client.sendRequestWithOptions(.stream_create, namespace, stream, writer.bytes(), options);
}

/// Alter stream configuration (retention policy)
pub fn alter(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    retention_count: ?u64,
    retention_age: ?u64,
    retention_bytes: ?u64,
) !Response {
    // Build options for retention policies using TLV OptionsBuilder
    var options_buf: [32]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    if (retention_count) |rc| {
        try builder.addU64(.retention_count, rc);
    } else if (retention_age) |ra| {
        try builder.addU64(.retention_age, ra);
    } else if (retention_bytes) |rb| {
        try builder.addU64(.retention_bytes, rb);
    }

    const options: []const u8 = builder.getOptions();

    return client.sendRequestWithOptions(.stream_alter, namespace, stream, "", options);
}
