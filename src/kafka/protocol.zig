//! Kafka Wire Protocol — API Request/Response Builders
//!
//! Builds and parses the subset of Kafka APIs needed by KafkaSource:
//! ApiVersions, Metadata, Fetch, ListOffsets, OffsetCommit, OffsetFetch.
//!
//! Each API has an encode*Request and decode*Response function pair.
//! Version negotiation selects the appropriate encoding per connection.

const std = @import("std");
const Allocator = std.mem.Allocator;
const codec = @import("codec.zig");
const KafkaReader = codec.KafkaReader;
const KafkaWriter = codec.KafkaWriter;
const ApiKey = codec.ApiKey;
const ApiVersionRange = codec.ApiVersionRange;

const log = @import("stdx").log;

// =============================================================================
// Kafka Error Codes
// =============================================================================

pub const ErrorCode = enum(i16) {
    none = 0,
    offset_out_of_range = 1,
    corrupt_message = 2,
    unknown_topic_or_partition = 3,
    invalid_fetch_size = 4,
    leader_not_available = 5,
    not_leader_or_follower = 6,
    request_timed_out = 7,
    broker_not_available = 8,
    replica_not_available = 9,
    message_too_large = 10,
    stale_controller_epoch = 11,
    offset_metadata_too_large = 12,
    group_load_in_progress = 14,
    group_coordinator_not_available = 15,
    not_coordinator = 16,
    topic_authorization_failed = 29,
    sasl_authentication_failed = 58,
    unknown_server_error = -1,
    _,

    pub fn isRetryable(self: ErrorCode) bool {
        return switch (self) {
            .leader_not_available,
            .not_leader_or_follower,
            .request_timed_out,
            .replica_not_available,
            .broker_not_available,
            .group_load_in_progress,
            .group_coordinator_not_available,
            .not_coordinator,
            => true,
            else => false,
        };
    }

    pub fn toStr(self: ErrorCode) []const u8 {
        return switch (self) {
            .none => "NONE",
            .offset_out_of_range => "OFFSET_OUT_OF_RANGE",
            .unknown_topic_or_partition => "UNKNOWN_TOPIC_OR_PARTITION",
            .leader_not_available => "LEADER_NOT_AVAILABLE",
            .not_leader_or_follower => "NOT_LEADER_OR_FOLLOWER",
            .request_timed_out => "REQUEST_TIMED_OUT",
            .sasl_authentication_failed => "SASL_AUTHENTICATION_FAILED",
            .topic_authorization_failed => "TOPIC_AUTHORIZATION_FAILED",
            else => "UNKNOWN_ERROR",
        };
    }
};

// =============================================================================
// ApiVersions (API Key 18)
// =============================================================================

pub const ApiVersionsResponse = struct {
    error_code: ErrorCode,
    api_versions: [64]ApiVersionRange,
    num_versions: usize,
};

/// Encode ApiVersions v0–v3 request body (empty body for all versions).
pub fn encodeApiVersionsRequest(writer: *KafkaWriter, api_version: i16) !void {
    if (api_version >= 3) {
        // v3+ (flexible): client_software_name + client_software_version + tagged_fields
        try writer.writeCompactString("flo-kafka-source");
        try writer.writeCompactString("0.1.0");
        try writer.writeTaggedFields();
    }
    // v0-v2: empty body
}

/// Decode ApiVersions response.
pub fn decodeApiVersionsResponse(data: []const u8, api_version: i16) !ApiVersionsResponse {
    var reader = KafkaReader.init(data);
    var result = ApiVersionsResponse{
        .error_code = .none,
        .api_versions = [_]ApiVersionRange{.{}} ** 64,
        .num_versions = 0,
    };

    result.error_code = @enumFromInt(try reader.readInt16());

    if (api_version >= 3) {
        // Flexible version: compact array
        const count = try reader.readCompactArrayLen();
        var i: u32 = 0;
        while (i < count) : (i += 1) {
            const api_key = try reader.readInt16();
            const min_ver = try reader.readInt16();
            const max_ver = try reader.readInt16();
            try reader.readTaggedFields(); // per-entry tagged fields
            if (api_key >= 0 and api_key < 64) {
                const idx: usize = @intCast(api_key);
                result.api_versions[idx] = .{ .min_version = min_ver, .max_version = max_ver };
                result.num_versions += 1;
            }
        }
        // throttle_time_ms (v1+)
        _ = try reader.readInt32();
        try reader.readTaggedFields(); // top-level
    } else {
        // Non-flexible: int32 array
        const count = try reader.readArrayLen();
        if (count < 0) return result;
        var i: i32 = 0;
        while (i < count) : (i += 1) {
            const api_key = try reader.readInt16();
            const min_ver = try reader.readInt16();
            const max_ver = try reader.readInt16();
            if (api_key >= 0 and api_key < 64) {
                const idx: usize = @intCast(api_key);
                result.api_versions[idx] = .{ .min_version = min_ver, .max_version = max_ver };
                result.num_versions += 1;
            }
        }
        // throttle_time_ms (v1+)
        if (api_version >= 1 and reader.remaining() >= 4) {
            _ = try reader.readInt32();
        }
    }

    return result;
}

// =============================================================================
// Metadata (API Key 3)
// =============================================================================

pub const BrokerInfo = struct {
    node_id: i32,
    host: []const u8,
    port: i32,
};

pub const PartitionInfo = struct {
    error_code: ErrorCode,
    partition_id: i32,
    leader_id: i32,
};

pub const TopicMetadata = struct {
    error_code: ErrorCode,
    name: []const u8,
    partitions: []PartitionInfo,
};

pub const MetadataResponse = struct {
    brokers: []BrokerInfo,
    topics: []TopicMetadata,
    allocator: Allocator,

    pub fn deinit(self: *MetadataResponse) void {
        for (self.topics) |topic| {
            self.allocator.free(topic.partitions);
        }
        self.allocator.free(self.topics);
        self.allocator.free(self.brokers);
    }
};

/// Encode Metadata request for a single topic (v1–v12).
pub fn encodeMetadataRequest(writer: *KafkaWriter, topic: []const u8, api_version: i16) !void {
    const is_flex = api_version >= 9;

    if (is_flex) {
        // compact array of topics
        try writer.writeUnsignedVarInt(2); // 1 topic (N+1)
        try writer.writeCompactString(topic);
        try writer.writeTaggedFields(); // per-topic tagged fields
        // allow_auto_topic_creation (v4+)
        try writer.writeByte(0); // false
        // include_topic_authorized_operations (v8+)
        if (api_version >= 8) try writer.writeByte(0);
        try writer.writeTaggedFields(); // top-level
    } else {
        // regular array of topics
        try writer.writeInt32(1); // 1 topic
        try writer.writeString(topic);
        // allow_auto_topic_creation (v4+)
        if (api_version >= 4) try writer.writeByte(0);
    }
}

/// Decode Metadata response (v1–v12).
pub fn decodeMetadataResponse(data: []const u8, api_version: i16, allocator: Allocator) !MetadataResponse {
    var reader = KafkaReader.init(data);
    const is_flex = api_version >= 9;

    // Throttle time (v3+)
    if (api_version >= 3) _ = try reader.readInt32();

    // Brokers
    const broker_count: usize = if (is_flex)
        @intCast(try reader.readCompactArrayLen())
    else blk: {
        const c = try reader.readArrayLen();
        break :blk if (c < 0) 0 else @intCast(c);
    };

    var brokers = try allocator.alloc(BrokerInfo, broker_count);
    errdefer allocator.free(brokers);

    for (0..broker_count) |i| {
        const node_id = try reader.readInt32();
        const host = if (is_flex)
            (try reader.readCompactString()) orelse ""
        else
            (try reader.readString()) orelse "";
        const port = try reader.readInt32();
        // rack (v1+)
        if (is_flex) {
            _ = try reader.readCompactString();
            try reader.readTaggedFields();
        } else {
            if (api_version >= 1) _ = try reader.readString();
        }
        brokers[i] = .{ .node_id = node_id, .host = host, .port = port };
    }

    // Cluster ID (v2+) — skip
    if (api_version >= 2) {
        if (is_flex)
            _ = try reader.readCompactString()
        else
            _ = try reader.readString();
    }
    // Controller ID (v1+) — skip
    if (api_version >= 1) _ = try reader.readInt32();

    // Topics
    const topic_count: usize = if (is_flex)
        @intCast(try reader.readCompactArrayLen())
    else blk: {
        const c = try reader.readArrayLen();
        break :blk if (c < 0) 0 else @intCast(c);
    };

    var topics = try allocator.alloc(TopicMetadata, topic_count);
    errdefer {
        for (topics[0..topic_count]) |t| allocator.free(t.partitions);
        allocator.free(topics);
    }

    for (0..topic_count) |ti| {
        const topic_error: ErrorCode = @enumFromInt(try reader.readInt16());
        const topic_name = if (is_flex)
            (try reader.readCompactString()) orelse ""
        else
            (try reader.readString()) orelse "";

        // is_internal (v1+)
        if (api_version >= 1) _ = try reader.readInt8();

        // Partitions
        const part_count: usize = if (is_flex)
            @intCast(try reader.readCompactArrayLen())
        else blk: {
            const c = try reader.readArrayLen();
            break :blk if (c < 0) 0 else @intCast(c);
        };

        var partitions = try allocator.alloc(PartitionInfo, part_count);
        errdefer allocator.free(partitions);

        for (0..part_count) |pi| {
            const part_error: ErrorCode = @enumFromInt(try reader.readInt16());
            const part_id = try reader.readInt32();
            const leader_id = try reader.readInt32();
            // leader_epoch (v7+)
            if (api_version >= 7) _ = try reader.readInt32();
            // replicas
            const replica_count: usize = if (is_flex)
                @intCast(try reader.readCompactArrayLen())
            else blk: {
                const c = try reader.readArrayLen();
                break :blk if (c < 0) 0 else @intCast(c);
            };
            for (0..replica_count) |_| _ = try reader.readInt32();
            // isr
            const isr_count: usize = if (is_flex)
                @intCast(try reader.readCompactArrayLen())
            else blk: {
                const c = try reader.readArrayLen();
                break :blk if (c < 0) 0 else @intCast(c);
            };
            for (0..isr_count) |_| _ = try reader.readInt32();
            // offline_replicas (v5+)
            if (api_version >= 5) {
                const offline_count: usize = if (is_flex)
                    @intCast(try reader.readCompactArrayLen())
                else blk: {
                    const c = try reader.readArrayLen();
                    break :blk if (c < 0) 0 else @intCast(c);
                };
                for (0..offline_count) |_| _ = try reader.readInt32();
            }
            if (is_flex) try reader.readTaggedFields();
            partitions[pi] = .{ .error_code = part_error, .partition_id = part_id, .leader_id = leader_id };
        }

        // topic_authorized_operations (v8+) — skip
        if (api_version >= 8) _ = try reader.readInt32();
        if (is_flex) try reader.readTaggedFields();

        topics[ti] = .{ .error_code = topic_error, .name = topic_name, .partitions = partitions };
    }

    if (is_flex) reader.readTaggedFields() catch {};

    return .{ .brokers = brokers, .topics = topics, .allocator = allocator };
}

// =============================================================================
// Fetch (API Key 1)
// =============================================================================

pub const FetchPartitionRequest = struct {
    partition_id: i32,
    fetch_offset: i64,
    partition_max_bytes: i32,
};

/// Encode a Fetch request body (v4–v16).
pub fn encodeFetchRequest(
    writer: *KafkaWriter,
    api_version: i16,
    topic: []const u8,
    partitions: []const FetchPartitionRequest,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
) !void {
    const is_flex = api_version >= 12;

    // replica_id
    try writer.writeInt32(-1); // -1 = consumer
    // max_wait_ms
    try writer.writeInt32(max_wait_ms);
    // min_bytes
    try writer.writeInt32(min_bytes);
    // max_bytes (v3+)
    if (api_version >= 3) try writer.writeInt32(max_bytes);
    // isolation_level (v4+)
    if (api_version >= 4) try writer.writeByte(@bitCast(isolation_level));
    // session_id (v7+)
    if (api_version >= 7) try writer.writeInt32(0); // no fetch session
    // session_epoch (v7+)
    if (api_version >= 7) try writer.writeInt32(-1); // no session

    // Topics array
    if (is_flex) {
        try writer.writeUnsignedVarInt(2); // 1 topic (N+1)
        try writer.writeCompactString(topic);
    } else {
        try writer.writeInt32(1); // 1 topic
        try writer.writeString(topic);
    }

    // Partitions array
    if (is_flex) {
        try writer.writeUnsignedVarInt(@intCast(partitions.len + 1)); // N+1
    } else {
        try writer.writeInt32(@intCast(partitions.len));
    }
    for (partitions) |p| {
        try writer.writeInt32(p.partition_id);
        // current_leader_epoch (v9+)
        if (api_version >= 9) try writer.writeInt32(-1); // unknown
        try writer.writeInt64(p.fetch_offset);
        // log_start_offset (v5+)
        if (api_version >= 5) try writer.writeInt64(-1);
        try writer.writeInt32(p.partition_max_bytes);
        if (is_flex) try writer.writeTaggedFields();
    }

    if (is_flex) try writer.writeTaggedFields(); // topic-level

    // forgotten_topics (v7+)
    if (api_version >= 7) {
        if (is_flex) {
            try writer.writeUnsignedVarInt(1); // empty compact array (N+1=1 means 0)
        } else {
            try writer.writeInt32(0); // empty array
        }
    }

    // rack_id (v11+)
    if (api_version >= 11) {
        if (is_flex) {
            try writer.writeCompactString("");
        } else {
            try writer.writeString("");
        }
    }

    if (is_flex) try writer.writeTaggedFields(); // top-level
}

/// Decoded partition data from a Fetch response.
pub const FetchPartitionResponse = struct {
    partition_id: i32,
    error_code: ErrorCode,
    high_watermark: i64,
    records_data: ?[]const u8, // raw RecordBatch bytes (may contain multiple batches)
};

/// Decoded Fetch response.
pub const FetchResponse = struct {
    throttle_time_ms: i32,
    error_code: ErrorCode,
    partitions: []FetchPartitionResponse,
    allocator: Allocator,

    pub fn deinit(self: *FetchResponse) void {
        self.allocator.free(self.partitions);
    }
};

/// Decode a Fetch response (v4–v16). Returns partition-level data.
pub fn decodeFetchResponse(data: []const u8, api_version: i16, allocator: Allocator) !FetchResponse {
    var reader = KafkaReader.init(data);
    const is_flex = api_version >= 12;

    // throttle_time_ms
    const throttle = try reader.readInt32();

    // error_code (v7+)
    var top_error: ErrorCode = .none;
    if (api_version >= 7) top_error = @enumFromInt(try reader.readInt16());

    // session_id (v7+)
    if (api_version >= 7) _ = try reader.readInt32();

    // Topics array
    const topic_count: usize = if (is_flex)
        @intCast(try reader.readCompactArrayLen())
    else blk: {
        const c = try reader.readArrayLen();
        break :blk if (c < 0) 0 else @intCast(c);
    };

    // Collect all partitions across all topics
    var all_partitions: std.ArrayList(FetchPartitionResponse) = .{};
    defer all_partitions.deinit(allocator);

    for (0..topic_count) |_| {
        // topic name
        if (is_flex)
            _ = try reader.readCompactString()
        else
            _ = try reader.readString();

        // Partitions
        const part_count: usize = if (is_flex)
            @intCast(try reader.readCompactArrayLen())
        else blk: {
            const c = try reader.readArrayLen();
            break :blk if (c < 0) 0 else @intCast(c);
        };

        for (0..part_count) |_| {
            const part_id = try reader.readInt32();
            const part_error: ErrorCode = @enumFromInt(try reader.readInt16());
            const high_watermark = try reader.readInt64();
            // last_stable_offset (v4+)
            if (api_version >= 4) _ = try reader.readInt64();
            // log_start_offset (v5+)
            if (api_version >= 5) _ = try reader.readInt64();
            // aborted_transactions (v4+)
            if (api_version >= 4) {
                const aborted_count: usize = if (is_flex)
                    @intCast(try reader.readCompactArrayLen())
                else blk: {
                    const c = try reader.readArrayLen();
                    break :blk if (c < 0) 0 else @intCast(c);
                };
                for (0..aborted_count) |_| {
                    _ = try reader.readInt64(); // producer_id
                    _ = try reader.readInt64(); // first_offset
                    if (is_flex) try reader.readTaggedFields();
                }
            }
            // preferred_read_replica (v11+)
            if (api_version >= 11) _ = try reader.readInt32();

            // Records (bytes)
            const records_data = if (is_flex)
                try reader.readCompactBytes()
            else
                try reader.readBytes();

            if (is_flex) try reader.readTaggedFields();

            try all_partitions.append(allocator, .{
                .partition_id = part_id,
                .error_code = part_error,
                .high_watermark = high_watermark,
                .records_data = records_data,
            });
        }

        if (is_flex) try reader.readTaggedFields(); // topic-level
    }

    if (is_flex) reader.readTaggedFields() catch {};

    return .{
        .throttle_time_ms = throttle,
        .error_code = top_error,
        .partitions = try all_partitions.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}

// =============================================================================
// ListOffsets (API Key 2)
// =============================================================================

pub const ListOffsetsPartitionResponse = struct {
    partition_id: i32,
    error_code: ErrorCode,
    offset: i64,
    timestamp: i64,
};

pub const ListOffsetsResponse = struct {
    partitions: []ListOffsetsPartitionResponse,
    allocator: Allocator,

    pub fn deinit(self: *ListOffsetsResponse) void {
        self.allocator.free(self.partitions);
    }
};

/// Encode ListOffsets request (v1–v8).
pub fn encodeListOffsetsRequest(
    writer: *KafkaWriter,
    api_version: i16,
    topic: []const u8,
    partition_id: i32,
    timestamp: i64, // -1 = latest, -2 = earliest
    isolation_level: i8,
) !void {
    const is_flex = api_version >= 6;

    // replica_id
    try writer.writeInt32(-1);
    // isolation_level (v2+)
    if (api_version >= 2) try writer.writeByte(@bitCast(isolation_level));

    // Topics array
    if (is_flex) {
        try writer.writeUnsignedVarInt(2); // 1 topic (N+1)
        try writer.writeCompactString(topic);
    } else {
        try writer.writeInt32(1);
        try writer.writeString(topic);
    }

    // Partitions array
    if (is_flex) {
        try writer.writeUnsignedVarInt(2); // 1 partition (N+1)
    } else {
        try writer.writeInt32(1);
    }
    try writer.writeInt32(partition_id);
    // current_leader_epoch (v4+)
    if (api_version >= 4) try writer.writeInt32(-1);
    try writer.writeInt64(timestamp);
    if (is_flex) try writer.writeTaggedFields(); // partition tagged fields

    if (is_flex) try writer.writeTaggedFields(); // topic tagged fields
    if (is_flex) try writer.writeTaggedFields(); // top-level
}

/// Decode ListOffsets response (v1–v8).
pub fn decodeListOffsetsResponse(data: []const u8, api_version: i16, allocator: Allocator) !ListOffsetsResponse {
    var reader = KafkaReader.init(data);
    const is_flex = api_version >= 6;

    // throttle_time_ms (v2+)
    if (api_version >= 2) _ = try reader.readInt32();

    // Topics
    const topic_count: usize = if (is_flex)
        @intCast(try reader.readCompactArrayLen())
    else blk: {
        const c = try reader.readArrayLen();
        break :blk if (c < 0) 0 else @intCast(c);
    };

    var parts: std.ArrayList(ListOffsetsPartitionResponse) = .{};
    defer parts.deinit(allocator);

    for (0..topic_count) |_| {
        // topic name
        if (is_flex)
            _ = try reader.readCompactString()
        else
            _ = try reader.readString();

        const part_count: usize = if (is_flex)
            @intCast(try reader.readCompactArrayLen())
        else blk: {
            const c = try reader.readArrayLen();
            break :blk if (c < 0) 0 else @intCast(c);
        };

        for (0..part_count) |_| {
            const part_id = try reader.readInt32();
            const err: ErrorCode = @enumFromInt(try reader.readInt16());
            const ts = try reader.readInt64();
            const offset = try reader.readInt64();
            // leader_epoch (v4+)
            if (api_version >= 4) _ = try reader.readInt32();
            if (is_flex) try reader.readTaggedFields();
            try parts.append(allocator, .{
                .partition_id = part_id,
                .error_code = err,
                .offset = offset,
                .timestamp = ts,
            });
        }
        if (is_flex) try reader.readTaggedFields();
    }

    if (is_flex) reader.readTaggedFields() catch {};

    return .{
        .partitions = try parts.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}

// =============================================================================
// OffsetCommit (API Key 8)
// =============================================================================

/// Encode OffsetCommit request (v2–v9).
pub fn encodeOffsetCommitRequest(
    writer: *KafkaWriter,
    api_version: i16,
    group_id: []const u8,
    topic: []const u8,
    partition_id: i32,
    committed_offset: i64,
) !void {
    const is_flex = api_version >= 8;

    // group_id
    if (is_flex)
        try writer.writeCompactString(group_id)
    else
        try writer.writeString(group_id);

    // generation_id (v1+)
    try writer.writeInt32(-1);
    // member_id
    if (is_flex)
        try writer.writeCompactString("")
    else
        try writer.writeString("");
    // group_instance_id (v7+)
    if (api_version >= 7) {
        if (is_flex)
            try writer.writeCompactString(null)
        else
            try writer.writeString(null);
    }

    // Topics array
    if (is_flex) {
        try writer.writeUnsignedVarInt(2); // 1 topic
        try writer.writeCompactString(topic);
    } else {
        try writer.writeInt32(1);
        try writer.writeString(topic);
    }

    // Partitions
    if (is_flex)
        try writer.writeUnsignedVarInt(2) // 1 partition
    else
        try writer.writeInt32(1);

    try writer.writeInt32(partition_id);
    try writer.writeInt64(committed_offset);
    // leader_epoch (v6+)
    if (api_version >= 6) try writer.writeInt32(-1);
    // committed_metadata
    if (is_flex)
        try writer.writeCompactString(null)
    else
        try writer.writeString(null);

    if (is_flex) {
        try writer.writeTaggedFields(); // partition
        try writer.writeTaggedFields(); // topic
        try writer.writeTaggedFields(); // top-level
    }
}

// =============================================================================
// OffsetFetch (API Key 9)
// =============================================================================

pub const OffsetFetchPartitionResponse = struct {
    partition_id: i32,
    committed_offset: i64,
    error_code: ErrorCode,
};

pub const OffsetFetchResponse = struct {
    partitions: []OffsetFetchPartitionResponse,
    allocator: Allocator,

    pub fn deinit(self: *OffsetFetchResponse) void {
        self.allocator.free(self.partitions);
    }
};

/// Encode OffsetFetch request (v1–v9).
pub fn encodeOffsetFetchRequest(
    writer: *KafkaWriter,
    api_version: i16,
    group_id: []const u8,
    topic: []const u8,
    partition_ids: []const i32,
) !void {
    const is_flex = api_version >= 6;

    // group_id
    if (is_flex)
        try writer.writeCompactString(group_id)
    else
        try writer.writeString(group_id);

    // Topics array
    if (is_flex) {
        try writer.writeUnsignedVarInt(2); // 1 topic
        try writer.writeCompactString(topic);
    } else {
        try writer.writeInt32(1);
        try writer.writeString(topic);
    }

    // Partitions
    if (is_flex)
        try writer.writeUnsignedVarInt(@intCast(partition_ids.len + 1))
    else
        try writer.writeInt32(@intCast(partition_ids.len));

    for (partition_ids) |pid| {
        try writer.writeInt32(pid);
        if (is_flex) try writer.writeTaggedFields();
    }
    if (is_flex) try writer.writeTaggedFields(); // topic
    // require_stable (v7+)
    if (api_version >= 7) try writer.writeByte(0);
    if (is_flex) try writer.writeTaggedFields(); // top-level
}

/// Decode OffsetFetch response (v1–v9).
pub fn decodeOffsetFetchResponse(data: []const u8, api_version: i16, allocator: Allocator) !OffsetFetchResponse {
    var reader = KafkaReader.init(data);
    const is_flex = api_version >= 6;

    // throttle_time_ms (v3+)
    if (api_version >= 3) _ = try reader.readInt32();

    // Topics
    const topic_count: usize = if (is_flex)
        @intCast(try reader.readCompactArrayLen())
    else blk: {
        const c = try reader.readArrayLen();
        break :blk if (c < 0) 0 else @intCast(c);
    };

    var parts: std.ArrayList(OffsetFetchPartitionResponse) = .{};
    defer parts.deinit(allocator);

    for (0..topic_count) |_| {
        if (is_flex)
            _ = try reader.readCompactString()
        else
            _ = try reader.readString();

        const part_count: usize = if (is_flex)
            @intCast(try reader.readCompactArrayLen())
        else blk: {
            const c = try reader.readArrayLen();
            break :blk if (c < 0) 0 else @intCast(c);
        };

        for (0..part_count) |_| {
            const part_id = try reader.readInt32();
            const committed = try reader.readInt64();
            // leader_epoch (v5+)
            if (api_version >= 5) _ = try reader.readInt32();
            // metadata
            if (is_flex)
                _ = try reader.readCompactString()
            else
                _ = try reader.readString();
            const err: ErrorCode = @enumFromInt(try reader.readInt16());
            if (is_flex) try reader.readTaggedFields();
            try parts.append(allocator, .{
                .partition_id = part_id,
                .committed_offset = committed,
                .error_code = err,
            });
        }
        if (is_flex) try reader.readTaggedFields();
    }

    // error_code (v2+)
    if (api_version >= 2 and reader.remaining() >= 2) _ = try reader.readInt16();
    if (is_flex) reader.readTaggedFields() catch {};

    return .{
        .partitions = try parts.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}

// =============================================================================
// Tests
// =============================================================================

test "encodeApiVersionsRequest v0 is empty body" {
    var writer = KafkaWriter.init(std.testing.allocator);
    defer writer.deinit();
    try encodeApiVersionsRequest(&writer, 0);
    try std.testing.expectEqual(@as(usize, 0), writer.len());
}

test "ErrorCode retryable" {
    try std.testing.expect(ErrorCode.leader_not_available.isRetryable());
    try std.testing.expect(ErrorCode.not_leader_or_follower.isRetryable());
    try std.testing.expect(ErrorCode.request_timed_out.isRetryable());
    try std.testing.expect(!ErrorCode.none.isRetryable());
    try std.testing.expect(!ErrorCode.unknown_topic_or_partition.isRetryable());
    try std.testing.expect(!ErrorCode.sasl_authentication_failed.isRetryable());
}

test "encodeMetadataRequest non-flexible" {
    var writer = KafkaWriter.init(std.testing.allocator);
    defer writer.deinit();
    try encodeMetadataRequest(&writer, "test-topic", 1);
    // Should start with array count = 1
    const written = writer.getWritten();
    try std.testing.expect(written.len > 4);
    const arr_count = std.mem.readInt(i32, written[0..4], .big);
    try std.testing.expectEqual(@as(i32, 1), arr_count);
}

test "encodeFetchRequest v4 structure" {
    var writer = KafkaWriter.init(std.testing.allocator);
    defer writer.deinit();
    const parts = [_]FetchPartitionRequest{
        .{ .partition_id = 0, .fetch_offset = 42, .partition_max_bytes = 262144 },
    };
    try encodeFetchRequest(&writer, 4, "my-topic", &parts, 100, 1, 1048576, 0);
    try std.testing.expect(writer.len() > 20);
}

test "encodeListOffsetsRequest v1 structure" {
    var writer = KafkaWriter.init(std.testing.allocator);
    defer writer.deinit();
    try encodeListOffsetsRequest(&writer, 1, "test-topic", 0, -1, 0);
    try std.testing.expect(writer.len() > 10);
}
