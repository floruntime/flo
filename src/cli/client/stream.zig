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
        client.keepError(response);
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
        // Read for as long as the server waits, plus 5 s; 0 does not wait.
        if (ms > 0) client.setReadTimeoutSec(ms / 1000 + 5);
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
/// Supports: max_len (count), min_id (StreamID), max_age_seconds, dry_run.
/// A dry run removes nothing and answers what the trim would remove.
pub fn trim(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    max_len: ?u64,
    min_id: ?StreamID,
    max_age_seconds: ?u64,
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

    if (dry_run) {
        try builder.addFlag(.dry_run);
    }

    return client.sendRequestWithOptions(.stream_trim, namespace, stream, "", builder.getOptions());
}

/// A trim answer: the records removed (or, for a dry run, that would be) and
/// the first sequence left.
pub const Trimmed = struct { removed: u64, first_seq: u64 };

pub fn parseTrimmed(response: Response) ?Trimmed {
    const data = response.asRawData() orelse return null;
    if (data.len != 16) return null;
    return .{
        .removed = std.mem.readInt(u64, data[0..8], .little),
        .first_seq = std.mem.readInt(u64, data[8..16], .little),
    };
}

/// Delete a stream entirely (records + metadata + name registry). Consumer
/// groups are namespace-level and are left intact. A non-empty stream requires
/// `force`. Idempotent: deleting a missing stream succeeds.
pub fn delete(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    force: bool,
) !Response {
    var options_buf: [16]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);
    if (force) {
        try builder.addFlag(.force);
    }
    return client.sendRequestWithOptions(.stream_delete, namespace, stream, "", builder.getOptions());
}

/// List all streams in a namespace
pub fn list(
    client: *Client,
    namespace: []const u8,
    limit: ?u32,
    cursor: ?[]const u8,
) !Response {
    var value_buf: [base.WALK_VALUE_MAX]u8 = undefined;
    return client.sendRequest(.stream_list, namespace, "", try base.walkValue(&value_buf, limit, cursor));
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
    var options_buf: [32]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);
    if (count) |c| try builder.addU32(.count, c);
    if (block_ms) |ms| {
        try builder.addU32(.block_ms, ms);
        // Read for as long as the server waits, plus 5 s; 0 does not wait.
        if (ms > 0) client.setReadTimeoutSec(ms / 1000 + 5);
    }
    // Wire format: [group_len:u16][group][consumer_len:u16][consumer]
    var writer = FixedWireWriter(512).init();
    try writer.writePair(u16, u16, group, consumer);
    return client.sendRequestWithOptions(.stream_group_read, namespace, stream, writer.bytes(), builder.getOptions());
}

/// Room for one read's worth of records (at most a read's count) plus
/// group, consumer and delivery.
const ITEM_LIST_BYTES = 2 + 1024 + 2 + 1024 + 8 + 8 + 4 + 20 * @as(usize, @import("../../protocol/proto.zig").MAX_STREAM_READ_RECORDS);

/// A record a report names: its id, and the deliveries it came with.
pub const GroupItem = struct { id: StreamID, deliveries: u32 };

/// What a report is: an ack, a nack or a touch.
pub const GroupReport = enum { ack, nack, touch };

/// Options a nack takes.
pub const NackOptions = struct { delay_ms: u64 = 0, dead: bool = false, error_text: []const u8 = "" };

/// Report on one delivery's records: `[group][consumer][term:u64][index:u64]`
/// `[count:u32]([ms:u64][seq:u64][deliveries:u32])*`. The answer is
/// `[applied:u32][refused:u32]([ms:u64][seq:u64][why:u8])*`, a touch's after
/// `[expires_at_ms:i64]`.
pub fn groupReport(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    consumer: []const u8,
    kind: GroupReport,
    term: u64,
    index: u64,
    items: []const GroupItem,
    nack: NackOptions,
) !Response {
    var writer = FixedWireWriter(ITEM_LIST_BYTES).init();
    try writer.writeLengthPrefixed(u16, group);
    try writer.writeLengthPrefixed(u16, consumer);
    try writer.writeU64(term);
    try writer.writeU64(index);
    try writer.writeU32(@intCast(items.len));
    for (items) |it| {
        try writer.writeU64(it.id.timestamp_ms);
        try writer.writeU64(it.id.sequence);
        try writer.writeU32(it.deliveries);
    }
    var options_buf: [64 + 1024]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);
    if (kind == .nack) {
        if (nack.delay_ms > 0) try builder.addU64(.delay_ms, nack.delay_ms);
        if (nack.dead) try builder.addFlag(.dead);
        if (nack.error_text.len > 0) try builder.addString(.error_text, nack.error_text);
    }
    const op: proto.OpCode = switch (kind) {
        .ack => .stream_group_ack,
        .nack => .stream_group_nack,
        .touch => .stream_group_touch,
    };
    return client.sendRequestWithOptions(op, namespace, stream, writer.bytes(), builder.getOptions());
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

/// Get consumer-group info: [pel_count:u64][member_count:u32][created_at_ns:u64].
pub fn groupInfo(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
) !Response {
    // Wire format: [group_len:u16][group]
    var writer = FixedWireWriter(256).init();
    try writer.writeLengthPrefixed(u16, group);

    return client.sendRequest(.stream_group_info, namespace, stream, writer.bytes());
}

/// The records a consumer group has leased out, optionally only those one
/// consumer holds. `consumer == null` returns every holder's.
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

/// Options for creating a consumer group
pub const GroupCreateOptions = struct {
    namespace: []const u8,
    stream: []const u8,
    group: []const u8,
    /// The lease: a record not acked this long is redelivered (5 s to 12 h).
    ack_timeout_ms: u32 = 30_000,
    /// The attempts: a record is dead once nacked or expired this many times.
    max_deliver: u8 = 5,
};

/// Create a consumer group with configuration
pub fn groupCreate(client: *Client, opts: GroupCreateOptions) !Response {
    // Wire format: [group_len:u16][group]
    var writer = FixedWireWriter(256).init();
    try writer.writeLengthPrefixed(u16, opts.group);

    // Build options
    var options_buf: [64]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    // ack_timeout_ms
    try builder.addU32(.ack_timeout_ms, opts.ack_timeout_ms);

    // max_deliver
    try builder.addU8(.max_deliver, opts.max_deliver);

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

/// Create a stream with specified partition count
pub fn create(
    client: *Client,
    namespace: []const u8,
    stream: []const u8,
    partition_count: u32,
    retention_count: ?u64,
    retention_age: ?u64,
) !Response {
    var writer = FixedWireWriter(128).init();
    try writer.writeU32(partition_count);

    // Build options for retention policies using TLV OptionsBuilder
    var options_buf: [32]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    // Each bound given is sent; the server enforces both.
    if (retention_count) |rc| try builder.addU64(.retention_count, rc);
    if (retention_age) |ra| try builder.addU64(.retention_age, ra);

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
) !Response {
    // Build options for retention policies using TLV OptionsBuilder
    var options_buf: [32]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    // Each bound given is sent; the server enforces both.
    if (retention_count) |rc| try builder.addU64(.retention_count, rc);
    if (retention_age) |ra| try builder.addU64(.retention_age, ra);

    const options: []const u8 = builder.getOptions();

    return client.sendRequestWithOptions(.stream_alter, namespace, stream, "", options);
}
