const std = @import("std");
const Allocator = std.mem.Allocator;

/// Flo Protocol
/// Binary wire protocol with 24-byte header, CRC32 validation, and namespace support
///
/// Design decisions:
/// - Magic: "FLO\0" (0x004F4C46) - null-terminated string, recognizable in hex dumps
/// - Header: 24 bytes - aligned, efficient, with version field + CRC32 for validation
/// - Namespace support - critical for multi-tenancy and partition routing
/// - Full opcode space defined (0x00-0x4F) - implement incrementally without breaking changes
/// Protocol magic number: "FLO\0" (0x004F4C46 in little-endian)
/// Null-terminated string representation for clean identification
pub const MAGIC: u32 = 0x004F4C46;

/// Protocol version (for future evolution)
pub const VERSION: u8 = 0x01;

/// Operation codes organized by functional area
/// Full opcode space defined now, implement incrementally
pub const OpCode = enum(u8) {
    // System Operations (0x00 - 0x0F)
    ping = 0x00,
    pong = 0x01,
    error_response = 0x02,
    auth = 0x03,
    set_durability = 0x04, // Set per-connection durability mode
    ok = 0x05, // Generic OK response

    // Streams (0x10 - 0x1F)
    stream_append = 0x10,
    stream_read = 0x11,
    stream_trim = 0x12,
    stream_info = 0x13,
    stream_append_response = 0x14,
    stream_read_response = 0x15,
    stream_event = 0x16, // Server-push for subscriptions
    stream_subscribe = 0x17, // Subscribe to stream (WebSocket continuous push)
    stream_unsubscribe = 0x18, // Unsubscribe from stream
    stream_subscribed = 0x19, // Response: subscription confirmed
    stream_unsubscribed = 0x1A, // Response: unsubscription confirmed
    stream_list = 0x1B, // List all streams in namespace
    stream_list_response = 0x1C,
    stream_create = 0x1D, // Create stream with partition count
    stream_create_response = 0x1E,
    stream_alter = 0x1F, // Alter stream configuration (retention policy)

    // Stream Consumer Groups (0x20 - 0x2F)
    stream_group_create = 0x20, // Create consumer group with configuration
    stream_group_join = 0x21,
    stream_group_leave = 0x22,
    stream_group_read = 0x23,
    stream_group_ack = 0x24,
    stream_group_claim = 0x25,
    stream_group_pending = 0x26,
    stream_group_configure_sweeper = 0x27,
    stream_group_read_response = 0x28,
    stream_group_nack = 0x29,
    stream_group_touch = 0x2A, // Extend ack deadline for pending messages
    stream_group_info = 0x2B, // Get consumer group info (config + consumers)
    stream_group_delete = 0x2C, // Delete consumer group

    // Flo-KV Operations (0x30 - 0x3F)
    kv_put = 0x30,
    kv_get = 0x31,
    kv_delete = 0x32,
    kv_scan = 0x33,
    kv_history = 0x34,
    kv_get_response = 0x35,
    kv_put_response = 0x36,
    kv_scan_response = 0x37,
    kv_history_response = 0x38,

    // Transactions (0x39 - 0x3B)
    kv_begin_txn = 0x39,
    kv_commit_txn = 0x3A,
    kv_rollback_txn = 0x3B,

    // Snapshots (0x3C - 0x3F)
    kv_snapshot_create = 0x3C,
    kv_snapshot_get = 0x3D,
    kv_snapshot_release = 0x3E,
    kv_snapshot_create_response = 0x3F,

    // Queues (0x40 - 0x5F)
    queue_enqueue = 0x40,
    queue_dequeue = 0x41,
    queue_complete = 0x42,
    queue_extend_lease = 0x43,
    queue_fail = 0x44,
    queue_fail_auto = 0x45, // Fail with auto retry/DLQ based on config
    queue_dlq_list = 0x46,
    queue_dlq_delete = 0x47,
    queue_dlq_requeue = 0x48,
    queue_dlq_stats = 0x49,
    queue_promote_due = 0x4A,
    queue_stats = 0x4B, // Get queue statistics
    queue_peek = 0x4C, // Peek without consuming
    queue_touch = 0x4D, // Extend lease timeout (renew)
    queue_batch_enqueue = 0x4E, // Batch enqueue multiple messages
    queue_purge = 0x4F, // Delete all messages from queue

    // Queue responses (0x50 - 0x5F)
    queue_enqueue_response = 0x50,
    queue_dequeue_response = 0x51,
    queue_dlq_list_response = 0x52,
    queue_stats_response = 0x53,
    queue_peek_response = 0x54,
    queue_touch_response = 0x55,
    queue_batch_enqueue_response = 0x56,
    queue_purge_response = 0x57,
    queue_list = 0x58, // List all queues in namespace
    queue_list_response = 0x59,

    // Actions (0x60 - 0x6F) — action definitions + task dispatch
    action_register = 0x60, // Register an action (user or WASM)
    action_invoke = 0x61, // Invoke an action (create run)
    action_status = 0x62, // Get action run status
    action_list = 0x63, // List registered actions
    action_delete = 0x64, // Delete/disable an action
    action_await = 0x65, // Worker blocks waiting for task assignment
    action_complete = 0x66, // Worker completes a task
    action_fail = 0x67, // Worker fails a task (with optional retry)
    action_touch = 0x68, // Extend task lease
    action_register_response = 0x69,
    action_invoke_response = 0x6A,
    action_status_response = 0x6B,
    action_list_response = 0x6C,
    action_task_assignment = 0x6D, // Push task to worker

    // Workers (0x70 - 0x7F) — physical worker tracking & health
    worker_register = 0x70, // Register worker with type + metadata
    worker_heartbeat = 0x71, // Worker heartbeat / health ping
    worker_deregister = 0x72, // Remove worker from registry
    worker_list = 0x73, // List all workers (with health)
    worker_info = 0x74, // Get single worker details
    worker_register_response = 0x75,
    worker_list_response = 0x76,
    worker_info_response = 0x77,
    worker_drain = 0x78, // Drain a worker (stop new task assignments)

    // Workflows (0x80 - 0x8F)
    workflow_create = 0x80, // Create workflow from YAML definition
    workflow_start = 0x81, // Start a workflow run
    workflow_signal = 0x82, // Send signal to running workflow
    workflow_cancel = 0x83, // Cancel a workflow run
    workflow_status = 0x84, // Get workflow run status
    workflow_history = 0x85, // Get workflow run history
    workflow_list_runs = 0x86, // List workflow runs
    workflow_get_definition = 0x87, // Get workflow definition
    workflow_create_response = 0x88,
    workflow_start_response = 0x89,
    workflow_status_response = 0x8A,
    workflow_history_response = 0x8B,
    workflow_list_runs_response = 0x8C,
    workflow_get_definition_response = 0x8D,
    workflow_disable = 0x8E,
    workflow_enable = 0x8F,
    workflow_disable_response = 0x90,
    workflow_enable_response = 0x91,
    workflow_list_definitions = 0x92,
    workflow_list_definitions_response = 0x93,

    // Cluster Management (0xA0 - 0xAF)
    cluster_status = 0xA0, // Get cluster status (leader, term, health)
    cluster_members = 0xA1, // List cluster members
    cluster_join = 0xA2, // Request to join cluster
    cluster_leave = 0xA3, // Request to leave cluster gracefully
    cluster_transfer_leader = 0xA4, // Transfer leadership to another node
    cluster_add_node = 0xA5, // Admin: add node to cluster (leader only)
    cluster_remove_node = 0xA6, // Admin: remove node from cluster (leader only)
    cluster_status_response = 0xA8,
    cluster_members_response = 0xA9,
    cluster_join_response = 0xAA,

    // Namespace Management (0xB0 - 0xBF)
    namespace_create = 0xB0, // Create a new namespace
    namespace_delete = 0xB1, // Delete an existing namespace
    namespace_list = 0xB2, // List all namespaces
    namespace_info = 0xB3, // Get namespace info/config
    namespace_create_response = 0xB4,
    namespace_delete_response = 0xB5,
    namespace_list_response = 0xB6,
    namespace_info_response = 0xB7,
    namespace_config_set = 0xB8, // Set namespace configuration (admin-only)
    namespace_config_get = 0xB9, // Get namespace configuration
    namespace_config_set_response = 0xBA,
    namespace_config_get_response = 0xBB,

    // Processing / Stream Processing (0xC0 - 0xCF)
    processing_submit = 0xC0, // Submit a processing job
    processing_stop = 0xC1, // Gracefully stop a processing job
    processing_cancel = 0xC2, // Force cancel a processing job
    processing_status = 0xC3, // Get processing job status
    processing_list = 0xC4, // List processing jobs
    processing_savepoint = 0xC6, // Trigger a savepoint
    processing_restore = 0xC7, // Restore from a savepoint
    processing_rescale = 0xC8, // Rescale job parallelism
    processing_submit_response = 0xC9,
    processing_stop_response = 0xCA,
    processing_cancel_response = 0xCB,
    processing_status_response = 0xCC,
    processing_list_response = 0xCD,
    processing_savepoint_response = 0xCF,
    processing_restore_response = 0xD0,
    processing_rescale_response = 0xD1,

    // Time-Series Operations (0xE0-0xEF)
    ts_write = 0xE0, // Write data point(s) to a time-series
    ts_read = 0xE1, // Read raw data points from a time-series
    ts_query = 0xE2, // Aggregated query over a time range
    ts_floql = 0xE3, // FloQL query string
    ts_list = 0xE4, // List measurements or series
    ts_delete = 0xE5, // Delete a series and its metadata
    ts_retention = 0xE6, // Configure retention / downsampling policy
    ts_write_response = 0xE7,
    ts_read_response = 0xE8,
    ts_query_response = 0xE9,
    ts_floql_response = 0xEA,
    ts_list_response = 0xEB,
    ts_delete_response = 0xEC,
    ts_retention_response = 0xED,

    _,
};

/// Status codes for responses
pub const StatusCode = enum(u8) {
    ok = 0,
    error_generic = 1,
    not_found = 2,
    bad_request = 3,
    cross_core_transaction = 4,
    no_active_transaction = 5,
    group_locked = 6, // Exclusive consumer group mode
    unauthorized = 7,
    conflict = 8,
    internal_error = 9,
    overloaded = 10,
    rate_limited = 11, // Request rate limit exceeded (WebSocket)

    _,
};

// =============================================================================
// TLV Options System
// =============================================================================
// Options are encoded as: [tag: u8] [len: u8] [data: len bytes]
// This allows operation-specific parameters without polluting the base Request.

/// Option tags for TLV-encoded operation parameters
/// Organized by feature area to allow extensibility
pub const OptionTag = enum(u8) {
    // KV Options (0x01 - 0x0F)
    ttl_seconds = 0x01, // u64: Time-to-live in seconds (0 = no expiration)
    cas_version = 0x02, // u64: Expected version for compare-and-swap
    if_not_exists = 0x03, // void: Only set if key doesn't exist (NX)
    if_exists = 0x04, // void: Only set if key exists (XX)
    limit = 0x05, // u32: Maximum number of results for scan/list operations
    keys_only = 0x06, // u8: Skip values in scan response (0/1)
    cursor = 0x07, // bytes: Pagination cursor (ShardWalker format)
    routing_key = 0x08, // string: Explicit routing key for shard co-location (overrides key-based routing)

    // Queue Options (0x10 - 0x1F)
    priority = 0x10, // u8: Message priority (0-255, higher = more urgent)
    delay_ms = 0x11, // u64: Delay before message becomes visible
    visibility_timeout_ms = 0x12, // u32: How long message is invisible after dequeue
    dedup_key = 0x13, // string: Deduplication key
    max_retries = 0x14, // u8: Maximum retry attempts before DLQ
    count = 0x15, // u32: Number of messages to dequeue
    send_to_dlq = 0x16, // u8: Whether to send failed messages to DLQ (0/1)
    block_ms = 0x17, // u32: Block timeout - wait until exists (0=forever, like queue dequeue)
    wait_ms = 0x18, // u32: Watch timeout - wait for NEXT version change (0=forever)

    // Stream Options (0x20 - 0x2F) - StreamID-native ONLY
    // All stream positioning uses StreamID (timestamp_ms + sequence) - no legacy offset/timestamp modes
    // 0x20 reserved
    stream_start = 0x21, // [16]u8: Start StreamID for reads (inclusive)
    stream_end = 0x22, // [16]u8: End StreamID for reads (inclusive)
    stream_tail = 0x23, // void: Flag indicating tail read (start from end of stream)
    partition = 0x24, // u32: Explicit partition index
    partition_key = 0x25, // string: Key for partition routing
    max_age_seconds = 0x26, // u64: Maximum age in seconds for retention
    max_bytes = 0x27, // u64: Maximum size in bytes for retention
    dry_run = 0x28, // void: Flag to preview what would be deleted without deleting
    retention_count = 0x29, // u64: Retention policy - max event count
    retention_age = 0x2A, // u64: Retention policy - max age in seconds
    retention_bytes = 0x2B, // u64: Retention policy - max bytes

    // Consumer Group Options (0x30 - 0x3F)
    ack_timeout_ms = 0x30, // u32: Time before unacked message auto-redelivers (overrides default)
    max_deliver = 0x31, // u8: Max delivery attempts before DLQ (default: 10, 0=unlimited)
    subscription_mode = 0x32, // u8: 0=shared, 1=exclusive, 2=key_shared
    redelivery_delay_ms = 0x33, // u32: Delay before NACK'd message becomes visible again
    consumer_timeout_ms = 0x34, // u32: Remove consumer from group if no activity (session timeout)
    no_ack = 0x35, // void: Auto-ack on delivery (at-most-once semantics)
    idle_timeout_ms = 0x36, // u64: Min idle time for claiming stuck messages (XCLAIM-style)
    max_ack_pending = 0x37, // u32: Max unacked messages per consumer (backpressure)
    extend_ack_ms = 0x38, // u32: Amount of time to extend ack deadline (for touch)
    max_standbys = 0x39, // u16: Max standby consumers in exclusive mode (0=singleton, null=unlimited)
    num_slots = 0x3A, // u16: Number of hash slots for key_shared mode (default: 256)

    // Worker/Action Options (0x40 - 0x4F)
    worker_id = 0x40, // string: Worker identifier
    extend_ms = 0x41, // u32: Lease extension time in milliseconds
    max_tasks = 0x42, // u32: Maximum tasks to return in batch
    retry = 0x43, // u8: Whether to retry on failure (0/1)

    // Workflow Options (0x50 - 0x5F)
    timeout_ms = 0x50, // u64: Workflow/activity timeout
    retry_policy = 0x51, // bytes: Serialized retry policy
    correlation_id = 0x52, // string: Correlation ID for tracing
    subscription_id = 0x53, // u64: Subscription ID for stream subscriptions

    // Time-Series Options (0x60 - 0x6F)
    ts_from_ms = 0x60, // i64: Start of time range (inclusive, unix ms)
    ts_to_ms = 0x61, // i64: End of time range (inclusive, 0 = now)
    ts_window_ms = 0x62, // i64: Aggregation window size (ms)
    ts_aggregation = 0x63, // string: Aggregation function name (avg, sum, count, min, max)
    ts_field = 0x64, // string: Field name filter (empty = "value")
    ts_tags = 0x65, // string: Comma-separated tag filters "key=val,key2=val2"
    ts_precision = 0x66, // u8: Timestamp precision (0=ns, 1=us, 2=ms, 3=s)
    ts_timestamp = 0x67, // i64: Explicit timestamp for write (0 = server-assigned)
    ts_raw_ttl = 0x68, // string: Raw data TTL (e.g., "7d")
    ts_downsample = 0x69, // string: Downsample rule (e.g., "1m:avg:30d")
    ts_batch = 0x6A, // void: Flag indicating batch/line-protocol mode

    _,
};

/// A single TLV option
pub const Option = struct {
    tag: OptionTag,
    data: []const u8,

    /// Get option value as u8
    pub fn asU8(self: Option) ?u8 {
        if (self.data.len != 1) return null;
        return self.data[0];
    }

    /// Get option value as u16
    pub fn asU16(self: Option) ?u16 {
        if (self.data.len != 2) return null;
        return std.mem.readInt(u16, self.data[0..2], .little);
    }

    /// Get option value as u32
    pub fn asU32(self: Option) ?u32 {
        if (self.data.len != 4) return null;
        return std.mem.readInt(u32, self.data[0..4], .little);
    }

    /// Get option value as u64
    pub fn asU64(self: Option) ?u64 {
        if (self.data.len != 8) return null;
        return std.mem.readInt(u64, self.data[0..8], .little);
    }

    /// Get option value as i64
    pub fn asI64(self: Option) ?i64 {
        if (self.data.len != 8) return null;
        return std.mem.readInt(i64, self.data[0..8], .little);
    }

    /// Get option value as string
    pub fn asString(self: Option) []const u8 {
        return self.data;
    }

    /// Check if this is a void/flag option (present = true)
    pub fn isFlag(self: Option) bool {
        return self.data.len == 0;
    }

    /// Get option value as StreamID (16 bytes: timestamp_ms BE + sequence BE)
    /// Returns null if data is not exactly 16 bytes
    pub fn asStreamId(self: Option) ?struct { timestamp_ms: u64, sequence: u64 } {
        if (self.data.len != 16) return null;
        return .{
            .timestamp_ms = std.mem.readInt(u64, self.data[0..8], .big),
            .sequence = std.mem.readInt(u64, self.data[8..16], .big),
        };
    }
};

/// Helper for building options into a buffer
pub const OptionsBuilder = struct {
    buffer: []u8,
    offset: usize = 0,

    pub fn init(buffer: []u8) OptionsBuilder {
        return .{ .buffer = buffer };
    }

    /// Add a u8 option
    pub fn addU8(self: *OptionsBuilder, tag: OptionTag, value: u8) !void {
        try self.ensureCapacity(3);
        self.buffer[self.offset] = @intFromEnum(tag);
        self.buffer[self.offset + 1] = 1; // length
        self.buffer[self.offset + 2] = value;
        self.offset += 3;
    }

    /// Add a u16 option
    pub fn addU16(self: *OptionsBuilder, tag: OptionTag, value: u16) !void {
        try self.ensureCapacity(4);
        self.buffer[self.offset] = @intFromEnum(tag);
        self.buffer[self.offset + 1] = 2; // length
        std.mem.writeInt(u16, self.buffer[self.offset + 2 ..][0..2], value, .little);
        self.offset += 4;
    }

    /// Add a u32 option
    pub fn addU32(self: *OptionsBuilder, tag: OptionTag, value: u32) !void {
        try self.ensureCapacity(6);
        self.buffer[self.offset] = @intFromEnum(tag);
        self.buffer[self.offset + 1] = 4; // length
        std.mem.writeInt(u32, self.buffer[self.offset + 2 ..][0..4], value, .little);
        self.offset += 6;
    }

    /// Add a u64 option
    pub fn addU64(self: *OptionsBuilder, tag: OptionTag, value: u64) !void {
        try self.ensureCapacity(10);
        self.buffer[self.offset] = @intFromEnum(tag);
        self.buffer[self.offset + 1] = 8; // length
        std.mem.writeInt(u64, self.buffer[self.offset + 2 ..][0..8], value, .little);
        self.offset += 10;
    }

    /// Add an i64 option
    pub fn addI64(self: *OptionsBuilder, tag: OptionTag, value: i64) !void {
        try self.ensureCapacity(10);
        self.buffer[self.offset] = @intFromEnum(tag);
        self.buffer[self.offset + 1] = 8; // length
        std.mem.writeInt(i64, self.buffer[self.offset + 2 ..][0..8], value, .little);
        self.offset += 10;
    }

    /// Add a string option
    pub fn addString(self: *OptionsBuilder, tag: OptionTag, value: []const u8) !void {
        if (value.len > 255) return error.OptionValueTooLarge;
        try self.ensureCapacity(2 + value.len);
        self.buffer[self.offset] = @intFromEnum(tag);
        self.buffer[self.offset + 1] = @intCast(value.len);
        @memcpy(self.buffer[self.offset + 2 ..][0..value.len], value);
        self.offset += 2 + value.len;
    }

    /// Add a bytes option (alias for addString, for clarity)
    pub fn addBytes(self: *OptionsBuilder, tag: OptionTag, value: []const u8) !void {
        return self.addString(tag, value);
    }

    /// Add a flag option (presence indicates true)
    pub fn addFlag(self: *OptionsBuilder, tag: OptionTag) !void {
        try self.ensureCapacity(2);
        self.buffer[self.offset] = @intFromEnum(tag);
        self.buffer[self.offset + 1] = 0; // length = 0 for flags
        self.offset += 2;
    }

    /// Add a StreamID option (16 bytes: timestamp_ms BE + sequence BE)
    /// StreamID is encoded in big-endian for lexicographic sorting
    pub fn addStreamId(self: *OptionsBuilder, tag: OptionTag, timestamp_ms: u64, sequence: u64) !void {
        try self.ensureCapacity(2 + 16);
        self.buffer[self.offset] = @intFromEnum(tag);
        self.buffer[self.offset + 1] = 16; // length
        std.mem.writeInt(u64, self.buffer[self.offset + 2 ..][0..8], timestamp_ms, .big);
        std.mem.writeInt(u64, self.buffer[self.offset + 10 ..][0..8], sequence, .big);
        self.offset += 18;
    }

    /// Get the built options slice
    pub fn getOptions(self: *const OptionsBuilder) []const u8 {
        return self.buffer[0..self.offset];
    }

    fn ensureCapacity(self: *OptionsBuilder, needed: usize) !void {
        if (self.offset + needed > self.buffer.len) {
            return error.OptionsBufferTooSmall;
        }
    }
};

/// Iterator for parsing options from a buffer
pub const OptionsIterator = struct {
    data: []const u8,
    offset: usize = 0,

    pub fn init(data: []const u8) OptionsIterator {
        return .{ .data = data };
    }

    pub fn next(self: *OptionsIterator) ?Option {
        if (self.offset + 2 > self.data.len) return null;

        const tag: OptionTag = @enumFromInt(self.data[self.offset]);
        const len = self.data[self.offset + 1];

        if (self.offset + 2 + len > self.data.len) return null;

        const option = Option{
            .tag = tag,
            .data = self.data[self.offset + 2 ..][0..len],
        };

        self.offset += 2 + len;
        return option;
    }

    /// Find a specific option by tag
    pub fn find(self: *OptionsIterator, tag: OptionTag) ?Option {
        self.offset = 0; // Reset to start
        while (self.next()) |opt| {
            if (opt.tag == tag) return opt;
        }
        return null;
    }
};

/// Protocol flags (u8 is sufficient)
pub const Flags = packed struct(u8) {
    compressed: bool = false, // Payload is LZ4 compressed
    streaming: bool = false, // Part of multi-frame response
    no_ack: bool = false, // Fire-and-forget (no response expected)
    _reserved: u5 = 0,
};

/// Request header (24 bytes total)
/// Fields ordered to avoid padding: 8-byte aligned first, then 4-byte, then 1-byte
pub const RequestHeader = extern struct {
    magic: u32, // 0-3 (4 bytes)
    payload_length: u32, // 4-7 (4 bytes)
    request_id: u64, // 8-15 (8 bytes) - must be 8-byte aligned
    crc32: u32, // 16-19 (4 bytes)
    version: u8, // 20 (1 byte)
    op_code: u8, // 21 (1 byte)
    flags: u8, // 22 (1 byte)
    reserved: u8, // 23 (1 byte) - must be 0

    pub fn validate(self: RequestHeader) !void {
        if (self.magic != MAGIC) {
            return error.InvalidMagic;
        }
        if (self.version != VERSION) {
            return error.UnsupportedVersion;
        }
        if (self.reserved != 0) {
            return error.InvalidReservedField;
        }
        // Validate reasonable sizes (prevent DoS)
        if (self.payload_length > 100 * 1024 * 1024) { // 100MB max
            return error.PayloadTooLarge;
        }
    }

    /// Compute CRC32 for header (excluding the crc32 field itself) + payload
    pub fn computeCRC32(self: RequestHeader, payload: []const u8) u32 {
        var hasher = std.hash.Crc32.init();

        // Hash header as bytes, but skip the crc32 field (bytes 16-19)
        const header_bytes = std.mem.asBytes(&self);
        // Hash bytes 0-15 (magic, payload_length, request_id)
        hasher.update(header_bytes[0..16]);
        // Skip bytes 16-19 (crc32 field)
        // Hash bytes 20-23 (version, op_code, flags, reserved)
        hasher.update(header_bytes[20..24]);

        // Hash payload
        hasher.update(payload);

        return hasher.final();
    }
};

/// Response header (24 bytes, matches request header structure)
pub const ResponseHeader = extern struct {
    magic: u32, // 0-3 (4 bytes)
    data_len: u32, // 4-7 (4 bytes)
    request_id: u64, // 8-15 (8 bytes) - must be 8-byte aligned
    crc32: u32, // 16-19 (4 bytes)
    version: u8, // 20 (1 byte)
    status: u8, // 21 (1 byte)
    flags: u8, // 22 (1 byte)
    reserved: u8, // 23 (1 byte)

    pub fn validate(self: ResponseHeader) !void {
        if (self.magic != MAGIC) {
            return error.InvalidMagic;
        }
        if (self.version != VERSION) {
            return error.UnsupportedVersion;
        }
    }

    pub fn computeCRC32(self: ResponseHeader, data: []const u8) u32 {
        var hasher = std.hash.Crc32.init();

        // Hash header as bytes, but skip the crc32 field (bytes 16-19)
        const header_bytes = std.mem.asBytes(&self);
        // Hash bytes 0-15 (magic, data_len, request_id)
        hasher.update(header_bytes[0..16]);
        // Skip bytes 16-19 (crc32 field)
        // Hash bytes 20-23 (version, status, flags, reserved)
        hasher.update(header_bytes[20..24]);

        hasher.update(data);

        return hasher.final();
    }
};

/// Request with namespace support and TLV options
/// Payload format: [namespace_len: u16] [namespace] [key_len: u16] [key] [value_len: u32] [value] [options_len: u16] [options...]
/// Options are TLV encoded: [tag: u8] [len: u8] [data: len bytes]
pub const Request = struct {
    header: RequestHeader,
    namespace: []const u8,
    key: []const u8,
    value: []const u8,
    options: []const u8 = "", // TLV-encoded options blob

    /// Parse request from wire format
    pub fn parse(data: []const u8) !Request {
        const header_size = @sizeOf(RequestHeader);
        if (data.len < header_size) {
            return error.IncompleteRequest;
        }

        // Parse header
        const header = @as(*align(1) const RequestHeader, @ptrCast(data.ptr)).*;
        try header.validate();

        // Verify total size
        const expected_size = header_size + header.payload_length;
        if (data.len < expected_size) {
            return error.IncompleteRequest;
        }

        // Extract payload
        const payload = data[header_size..expected_size];

        // Verify CRC32
        const computed_crc = header.computeCRC32(payload);
        if (computed_crc != header.crc32) {
            return error.InvalidChecksum;
        }

        // Handle system commands with no payload (ping, etc.)
        if (payload.len == 0) {
            return Request{
                .header = header,
                .namespace = "",
                .key = "",
                .value = "",
                .options = "",
            };
        }

        // Parse payload: namespace_len(u16) + namespace + key_len(u16) + key + value_len(u32) + value + options_len(u16) + options
        var offset: usize = 0;

        // Namespace
        if (payload.len < offset + 2) return error.IncompletePayload;
        const namespace_len = std.mem.readInt(u16, payload[offset..][0..2], .little);
        offset += 2;

        if (payload.len < offset + namespace_len) return error.IncompletePayload;
        const namespace = payload[offset..][0..namespace_len];
        offset += namespace_len;

        // Key
        if (payload.len < offset + 2) return error.IncompletePayload;
        const key_len = std.mem.readInt(u16, payload[offset..][0..2], .little);
        offset += 2;

        if (payload.len < offset + key_len) return error.IncompletePayload;
        const key = payload[offset..][0..key_len];
        offset += key_len;

        // Value
        if (payload.len < offset + 4) return error.IncompletePayload;
        const value_len = std.mem.readInt(u32, payload[offset..][0..4], .little);
        offset += 4;

        if (payload.len < offset + value_len) return error.IncompletePayload;
        const value = payload[offset..][0..value_len];
        offset += value_len;

        // Options (optional - if there's more data)
        var options: []const u8 = "";
        if (offset + 2 <= payload.len) {
            const options_len = std.mem.readInt(u16, payload[offset..][0..2], .little);
            offset += 2;

            if (payload.len >= offset + options_len) {
                options = payload[offset..][0..options_len];
            }
        }

        return Request{
            .header = header,
            .namespace = namespace,
            .key = key,
            .value = value,
            .options = options,
        };
    }

    /// Serialize request to wire format
    pub fn serialize(self: Request, buffer: []u8) ![]const u8 {
        const header_size = @sizeOf(RequestHeader);

        // Calculate payload size (always include options_len field)
        const payload_size = 2 + self.namespace.len + // namespace_len + namespace
            2 + self.key.len + // key_len + key
            4 + self.value.len + // value_len + value
            2 + self.options.len; // options_len + options

        const total_size = header_size + payload_size;

        if (buffer.len < total_size) {
            return error.BufferTooSmall;
        }

        // Build payload first (needed for CRC32)
        var payload_offset: usize = 0;
        var payload_buffer = buffer[header_size..total_size];

        // Write namespace
        std.mem.writeInt(u16, payload_buffer[payload_offset..][0..2], @intCast(self.namespace.len), .little);
        payload_offset += 2;
        @memcpy(payload_buffer[payload_offset..][0..self.namespace.len], self.namespace);
        payload_offset += self.namespace.len;

        // Write key
        std.mem.writeInt(u16, payload_buffer[payload_offset..][0..2], @intCast(self.key.len), .little);
        payload_offset += 2;
        @memcpy(payload_buffer[payload_offset..][0..self.key.len], self.key);
        payload_offset += self.key.len;

        // Write value
        std.mem.writeInt(u32, payload_buffer[payload_offset..][0..4], @intCast(self.value.len), .little);
        payload_offset += 4;
        if (self.value.len > 0) {
            @memcpy(payload_buffer[payload_offset..][0..self.value.len], self.value);
            payload_offset += self.value.len;
        }

        // Write options
        std.mem.writeInt(u16, payload_buffer[payload_offset..][0..2], @intCast(self.options.len), .little);
        payload_offset += 2;
        if (self.options.len > 0) {
            @memcpy(payload_buffer[payload_offset..][0..self.options.len], self.options);
            payload_offset += self.options.len;
        }

        // Write header with correct payload_length FIRST (needed for CRC32 computation)
        var header = self.header;
        header.payload_length = @intCast(payload_size);
        header.crc32 = 0; // Zero out CRC32 before computing

        // Compute CRC32 with the correct payload_length set
        const crc = header.computeCRC32(payload_buffer[0..payload_offset]);
        header.crc32 = crc;

        @memcpy(buffer[0..header_size], std.mem.asBytes(&header));

        return buffer[0..total_size];
    }

    pub fn getOpCode(self: Request) OpCode {
        return @enumFromInt(self.header.op_code);
    }

    pub fn getFlags(self: Request) Flags {
        return @bitCast(self.header.flags);
    }

    /// Get an iterator over the TLV options
    pub fn getOptionsIterator(self: Request) OptionsIterator {
        return OptionsIterator.init(self.options);
    }

    /// Find a specific option by tag
    pub fn findOption(self: Request, tag: OptionTag) ?Option {
        var iter = self.getOptionsIterator();
        return iter.find(tag);
    }

    /// Get TTL option if present (convenience method)
    pub fn getTtlSeconds(self: Request) ?u64 {
        if (self.findOption(.ttl_seconds)) |opt| {
            return opt.asU64();
        }
        return null;
    }

    /// Get CAS version option if present (convenience method)
    pub fn getCasVersion(self: Request) ?u64 {
        if (self.findOption(.cas_version)) |opt| {
            return opt.asU64();
        }
        return null;
    }

    /// Get limit option if present (convenience method)
    pub fn getLimit(self: Request) ?u32 {
        if (self.findOption(.limit)) |opt| {
            return opt.asU32();
        }
        return null;
    }

    /// Get count option if present (convenience method)
    pub fn getCount(self: Request) ?u32 {
        if (self.findOption(.count)) |opt| {
            return opt.asU32();
        }
        return null;
    }

    /// Get visibility_timeout_ms option if present (convenience method)
    pub fn getVisibilityTimeoutMs(self: Request) ?u32 {
        if (self.findOption(.visibility_timeout_ms)) |opt| {
            return opt.asU32();
        }
        return null;
    }

    /// Get keys_only option if present (convenience method)
    pub fn getKeysOnly(self: Request) bool {
        if (self.findOption(.keys_only)) |opt| {
            if (opt.asU8()) |v| return v != 0;
        }
        return false;
    }

    /// Get send_to_dlq option if present (convenience method)
    pub fn getSendToDlq(self: Request) bool {
        if (self.findOption(.send_to_dlq)) |opt| {
            if (opt.asU8()) |v| return v != 0;
        }
        return false;
    }

    /// Get priority option if present (convenience method)
    pub fn getPriority(self: Request) ?u8 {
        if (self.findOption(.priority)) |opt| {
            return opt.asU8();
        }
        return null;
    }

    /// Get delay_ms option if present (convenience method)
    pub fn getDelayMs(self: Request) ?u64 {
        if (self.findOption(.delay_ms)) |opt| {
            return opt.asU64();
        }
        return null;
    }

    /// Get dedup_key option if present (convenience method)
    pub fn getDedupKey(self: Request) ?[]const u8 {
        if (self.findOption(.dedup_key)) |opt| {
            return opt.asString();
        }
        return null;
    }

    /// Get block_ms option if present (convenience method)
    pub fn getBlockMs(self: Request) ?u32 {
        if (self.findOption(.block_ms)) |opt| {
            return opt.asU32();
        }
        return null;
    }

    /// Get routing_key option if present (convenience method for shard co-location)
    pub fn getRoutingKey(self: Request) ?[]const u8 {
        if (self.findOption(.routing_key)) |opt| {
            return opt.asString();
        }
        return null;
    }

    /// Get wait_ms option (watch for NEXT version change)
    pub fn getWaitMs(self: Request) ?u32 {
        if (self.findOption(.wait_ms)) |opt| {
            return opt.asU32();
        }
        return null;
    }

    /// Get if_not_exists option (NX - only set if key doesn't exist)
    pub fn getIfNotExists(self: Request) bool {
        if (self.findOption(.if_not_exists)) |opt| {
            return opt.isFlag() or (opt.asU8() orelse 0) != 0;
        }
        return false;
    }

    /// Get if_exists option (XX - only set if key exists)
    pub fn getIfExists(self: Request) bool {
        if (self.findOption(.if_exists)) |opt| {
            return opt.isFlag() or (opt.asU8() orelse 0) != 0;
        }
        return false;
    }
};

/// Response envelope
pub const Response = struct {
    header: ResponseHeader,
    prefix: ?u64 = null, // Optional 8-byte prefix (e.g. version) to prepend to data
    data: []const u8,

    /// Instance method: serialize this response to a buffer
    pub fn serialize(self: Response, buffer: []u8) ![]const u8 {
        const header_size = @sizeOf(ResponseHeader);
        var payload_len = self.data.len;
        if (self.prefix) |_| payload_len += 8;

        const total_size = header_size + payload_len;

        if (buffer.len < total_size) {
            return error.BufferTooSmall;
        }

        // Prepare payload in buffer first (for CRC)
        var payload_offset: usize = header_size;

        if (self.prefix) |p| {
            std.mem.writeInt(u64, buffer[payload_offset..][0..8], p, .little);
            payload_offset += 8;
        }

        @memcpy(buffer[payload_offset..][0..self.data.len], self.data);

        // Compute CRC32 on the full payload (prefix + data)
        var header = self.header;
        header.data_len = @intCast(payload_len);
        header.crc32 = header.computeCRC32(buffer[header_size..total_size]);

        // Write header
        @memcpy(buffer[0..header_size], std.mem.asBytes(&header));

        return buffer[0..total_size];
    }

    /// Static method: create and serialize a response in one step
    pub fn serializeNew(status: StatusCode, request_id: u64, data: []const u8, buffer: []u8) ![]const u8 {
        const resp = Response.init(request_id, status, data);
        return resp.serialize(buffer);
    }

    pub fn parse(data: []const u8) !Response {
        const header_size = @sizeOf(ResponseHeader);
        if (data.len < header_size) {
            return error.IncompleteResponse;
        }

        const header = @as(*align(1) const ResponseHeader, @ptrCast(data.ptr)).*;
        try header.validate();

        const expected_size = header_size + header.data_len;
        if (data.len < expected_size) {
            return error.IncompleteResponse;
        }

        const response_data = data[header_size..][0..header.data_len];

        // Verify CRC32
        const computed_crc = header.computeCRC32(response_data);
        if (computed_crc != header.crc32) {
            return error.InvalidChecksum;
        }

        return Response{
            .header = header,
            .data = response_data,
        };
    }

    pub fn getStatus(self: Response) StatusCode {
        return @enumFromInt(self.header.status);
    }

    // Backwards-compatible init methods (for tests written before V2)
    pub fn init(request_id: u64, status: StatusCode, data: []const u8) Response {
        return Response{
            .header = .{
                .magic = MAGIC,
                .version = VERSION,
                .status = @intFromEnum(status),
                .flags = 0,
                .reserved = 0,
                .data_len = @intCast(data.len),
                .request_id = request_id,
                .crc32 = 0, // Will be computed during serialization
            },
            .data = data,
        };
    }

    pub fn initError(request_id: u64, status: StatusCode) Response {
        return Response.init(request_id, status, &.{});
    }
};

comptime {
    // Verify header sizes at compile time
    if (@sizeOf(RequestHeader) != 24) {
        @compileError("RequestHeader must be exactly 24 bytes");
    }
    if (@sizeOf(ResponseHeader) != 24) {
        @compileError("ResponseHeader must be exactly 24 bytes");
    }
}

test "OpCode validation" {
    // Verify opcode names exist
    _ = OpCode.kv_put;
    _ = OpCode.kv_get;
    _ = OpCode.kv_delete;
}

test "TLV OptionsBuilder and Iterator" {
    var buffer: [64]u8 = undefined;
    var builder = OptionsBuilder.init(&buffer);

    // Build some options
    try builder.addU64(.ttl_seconds, 3600);
    try builder.addU8(.priority, 5);
    try builder.addString(.dedup_key, "abc123");
    try builder.addFlag(.if_not_exists);

    const options = builder.getOptions();

    // Parse them back
    var iter = OptionsIterator.init(options);

    // TTL
    const ttl_opt = iter.next().?;
    try std.testing.expectEqual(OptionTag.ttl_seconds, ttl_opt.tag);
    try std.testing.expectEqual(@as(u64, 3600), ttl_opt.asU64().?);

    // Priority
    const priority_opt = iter.next().?;
    try std.testing.expectEqual(OptionTag.priority, priority_opt.tag);
    try std.testing.expectEqual(@as(u8, 5), priority_opt.asU8().?);

    // Dedup key
    const dedup_opt = iter.next().?;
    try std.testing.expectEqual(OptionTag.dedup_key, dedup_opt.tag);
    try std.testing.expectEqualStrings("abc123", dedup_opt.asString());

    // Flag
    const flag_opt = iter.next().?;
    try std.testing.expectEqual(OptionTag.if_not_exists, flag_opt.tag);
    try std.testing.expect(flag_opt.isFlag());

    // No more options
    try std.testing.expect(iter.next() == null);
}

test "TLV OptionsIterator find" {
    var buffer: [64]u8 = undefined;
    var builder = OptionsBuilder.init(&buffer);

    try builder.addU64(.ttl_seconds, 7200);
    try builder.addU8(.priority, 10);

    const options = builder.getOptions();
    var iter = OptionsIterator.init(options);

    // Find priority (not first)
    const priority = iter.find(.priority).?;
    try std.testing.expectEqual(@as(u8, 10), priority.asU8().?);

    // Find TTL (after reset by find)
    const ttl = iter.find(.ttl_seconds).?;
    try std.testing.expectEqual(@as(u64, 7200), ttl.asU64().?);

    // Find non-existent
    try std.testing.expect(iter.find(.cas_version) == null);
}

test "Request with TLV options serialization" {
    var options_buf: [32]u8 = undefined;
    var builder = OptionsBuilder.init(&options_buf);
    try builder.addU64(.ttl_seconds, 86400); // 1 day

    const request = Request{
        .header = .{
            .magic = MAGIC,
            .version = VERSION,
            .op_code = @intFromEnum(OpCode.kv_put),
            .flags = 0,
            .reserved = 0,
            .payload_length = 0,
            .request_id = 42,
            .crc32 = 0,
        },
        .namespace = "test",
        .key = "mykey",
        .value = "myvalue",
        .options = builder.getOptions(),
    };

    var buffer: [256]u8 = undefined;
    const serialized = try request.serialize(&buffer);

    // Parse it back
    const parsed = try Request.parse(serialized);

    try std.testing.expectEqualStrings("test", parsed.namespace);
    try std.testing.expectEqualStrings("mykey", parsed.key);
    try std.testing.expectEqualStrings("myvalue", parsed.value);
    try std.testing.expectEqual(@as(u64, 86400), parsed.getTtlSeconds().?);
}

test "Response with prefix_u64 serialization" {
    var resp = Response.init(123, .ok, "hello");
    resp.prefix = 999;

    var buffer: [256]u8 = undefined;
    const serialized = try resp.serialize(&buffer);

    // Parse it back
    const parsed = try Response.parse(serialized);

    try std.testing.expectEqual(StatusCode.ok, parsed.getStatus());
    try std.testing.expectEqual(@as(u64, 123), parsed.header.request_id);

    // The parsed data should include the prefix
    try std.testing.expectEqual(8 + 5, parsed.data.len);

    const version = std.mem.readInt(u64, parsed.data[0..8], .little);
    try std.testing.expectEqual(@as(u64, 999), version);

    const value = parsed.data[8..];
    try std.testing.expectEqualStrings("hello", value);
}
