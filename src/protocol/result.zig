//! CommandResult - Unified result type for all Flo operations
//!
//! This module defines the canonical result representation returned by all
//! command handlers across Layers 1-3. Results are serialized for cross-core
//! messaging and protocol responses.

const std = @import("std");
const flo_proto = @import("proto.zig");

/// Unified result type for all Flo command responses
pub const CommandResult = union(enum) {
    // =========================================================================
    // Generic Responses
    // =========================================================================

    /// Simple OK acknowledgment
    ok: void,

    /// Request is pending (blocked) - no response sent yet
    pending: void,

    /// The write is in the log and waits for commit; the module's
    /// responder answers once it has applied. Carries what `propose`
    /// returned.
    parked: @import("../raft/types.zig").ProposeResult,

    /// Error response
    err: Error,

    // =========================================================================
    // Layer 1: KV Responses
    // =========================================================================

    /// Value response (for GET, with optional version for time-travel)
    kv_value: struct {
        value: []const u8,
        version: u64,
    },

    /// Value not found
    kv_not_found: void,

    /// Put response with assigned version
    kv_put_ok: struct {
        version: u64,
    },

    /// Compare-and-swap failed (version mismatch)
    kv_cas_failed: struct {
        current_version: u64,
    },

    /// Conditional put failed (if_not_exists or if_exists condition not met)
    /// Returns conflict status to client
    kv_condition_not_met: void,

    /// Scan response with key-value pairs (pre-serialized)
    /// Wire format: [count:u32] ([key_len:u16][key][value_len:u32][value][version:u64])* [has_more:u8] [cursor_len:u16][cursor]?
    /// If keys_only=true, version is omitted from wire format.
    kv_scan_result: struct {
        /// Pre-serialized wire data
        data: []const u8,
    },

    /// History response with version entries (pre-serialized)
    /// Wire format: [count:u32] ([value_len:u32][value][version:u64])*
    kv_history_result: struct {
        /// Pre-serialized wire data
        data: []const u8,
    },

    /// Multi-GET response (pre-serialized)
    /// Wire format: [count:u32] ([status:u8][key_len:u16][key][version:u64][value_len:u32][value])*
    /// status: 0 = found, 2 = not_found (value_len=0, version=0 when not found)
    kv_mget_result: struct {
        /// Pre-serialized wire data
        data: []const u8,
    },

    /// kv_begin_txn reply.
    /// Wire format: [txn_id:u64 LE][pinned_hash:u64 LE]
    kv_txn_begin_ok: struct {
        txn_id: u64,
        pinned_hash: u64,
    },

    /// kv_commit_txn reply (success). Carries the index assigned by Raft to
    /// the batched UAL entry — clients can use it as a watermark for "all my
    /// writes are now visible".
    /// Wire format: [commit_index:u64 LE][op_count:u16 LE]
    kv_txn_commit_ok: struct {
        commit_index: u64,
        op_count: u16,
    },

    // =========================================================================
    // Layer 1: Stream Responses
    // =========================================================================

    /// Append response with assigned StreamID
    stream_append_ok: struct {
        sequence: u64,
        timestamp_ms: i64,
    },

    /// Read response with messages (pre-serialized)
    /// Wire format: [count:u32]([sequence:u64][timestamp_ms:i64][tier:u8][partition:u32][key_present:u8][payload_len:u32][payload][header_count:u32])*
    stream_messages: struct {
        /// Pre-serialized wire data
        data: []const u8,
        /// Pagination cursor: the StreamID to pass as --start for the next read
        next_timestamp_ms: u64,
        next_sequence: u64,
    },

    /// Stream info response
    /// Wire format: [first_ts:u64][first_seq:u64][last_ts:u64][last_seq:u64][count:u64][bytes:u64][partition_count:u32][retention_age_s:u64][retention_count:u64][retention_bytes:u64]
    stream_info: struct {
        first_timestamp_ms: u64 = 0,
        first_seq: u64 = 0,
        last_timestamp_ms: u64 = 0,
        last_seq: u64 = 0,
        count: u64,
        bytes: u64,
        partition_count: u32 = 1,
        retention_age_s: u64 = 0,
        retention_count: u64 = 0,
        retention_bytes: u64 = 0,
    },

    /// Stream trim response
    /// Wire format: [deleted_count:u64][first_seq:u64]: logical records removed
    /// (or, for a dry run, that would be) and the first sequence left.
    stream_trimmed: struct {
        deleted_count: u64,
        first_seq: u64,
    },

    /// Stream list response
    /// Wire format: [count:u32] ([name_len:u32][name][partition_count:u32])* [has_more:u8] [cursor_len:u16][cursor]?
    /// Cursor follows ShardWalker format for cross-shard iteration.
    streams_listed: struct {
        /// Pre-serialized wire data (includes has_more and cursor)
        data: []const u8,
    },

    // =========================================================================
    // Layer 1: Consumer Group Responses
    // =========================================================================

    /// Group join response with assigned partitions
    group_joined: struct {
        generation_id: u64,
        assigned_partitions: []const u32,
    },

    /// Group read response with messages (pre-serialized)
    group_messages: struct {
        /// Pre-serialized wire data
        data: []const u8,
    },

    /// Group pending response with sequence numbers (pre-serialized)
    /// Wire format: [count:u32][seq:u64]*
    group_pending: struct {
        /// Pre-serialized wire data
        data: []const u8,
    },

    /// Group touch response with touched count
    group_touch: struct {
        /// Number of messages whose deadline was extended
        touched_count: u32,
        /// Pre-serialized wire data
        data: []const u8,
    },

    // =========================================================================
    // Layer 1: Queue Responses
    // =========================================================================

    /// Enqueue response with message ID
    queue_enqueued: struct {
        message_id: []const u8,
    },

    /// Dequeue/Complete response with messages (pre-serialized)
    /// Wire format: [count:u32] ([seq:u64][payload_len:u32][payload][enqueued_at:i64][delivery_count:u32][priority:u8])*
    queue_messages: struct {
        /// Pre-serialized wire data
        data: []const u8,
    },

    /// Peek response with messages (pre-serialized, same format as queue_messages)
    /// Wire format: [count:u32] ([seq:u64][payload_len:u32][payload][enqueued_at:i64][delivery_count:u32][priority:u8])*
    queue_peek_messages: struct {
        /// Pre-serialized wire data
        data: []const u8,
    },

    /// DLQ list response (pre-serialized)
    /// Wire format: [count:u32] ([seq:u64][payload_len:u32][payload][enqueued_at:i64][delivery_count:u32][priority:u8])* [total_count:u64]
    queue_dlq_messages: struct {
        /// Pre-serialized wire data (includes total_count at end)
        data: []const u8,
    },

    /// Queue purge response — number of messages removed.
    queue_purged: struct {
        count: u32,
    },

    /// Queue list response (pre-serialized)
    /// Wire format: [count:u32] ([name_len:u32][name][ns_len:u32][ns][pending:u64][available:u64][enqueued:u64][dequeued:u64][dlq:u64])*
    queues_listed: struct {
        /// Pre-serialized wire data
        data: []const u8,
    },

    // =========================================================================
    // Layer 2: Action Responses
    // =========================================================================

    /// Action registered successfully
    action_registered: struct {
        name: []const u8,
        version: []const u8,
    },

    /// Action invoked successfully
    action_invoked: struct {
        run_id: []const u8,
        /// Estimated queue position (if queued)
        queue_position: ?u32 = null,
    },

    /// Action run status
    action_run_status: struct {
        run_id: []const u8,
        status: ActionRunStatus,
        /// When action run was created
        created_at: i64,
        /// When action started executing (null if pending)
        started_at: ?i64,
        /// When action completed (null if not complete)
        completed_at: ?i64,
        /// Output data (if completed)
        output: ?[]const u8,
        /// Error message (if failed)
        error_message: ?[]const u8,
        /// Current retry attempt
        retry_count: u32,
    },

    /// Action list response
    action_list_result: struct {
        /// Pre-serialized wire data
        data: []const u8,
        /// Cursor for pagination (null if no more)
        cursor: ?[]const u8,
    },

    /// Action deleted/disabled
    action_deleted: void,

    // =========================================================================
    // Layer 2: Worker Responses
    // =========================================================================

    /// Worker registered
    worker_registered: struct {
        worker_id: []const u8,
        heartbeat_interval_ms: u32,
    },

    /// Worker list response
    workers_listed: struct {
        /// Pre-serialized wire data
        data: []const u8,
        /// Cursor for pagination (null if no more)
        cursor: ?[]const u8,
    },

    // =========================================================================
    // Layer 3: Workflow Responses
    // =========================================================================

    /// Workflow created
    workflow_created: struct {
        workflow_name: []const u8,
    },

    /// Workflow started
    workflow_started: struct {
        run_id: []const u8,
        already_exists: bool = false,
    },

    /// Workflow signaled
    workflow_signaled: void,

    /// Workflow cancelled
    workflow_cancelled: void,

    /// Workflow status result
    workflow_status_result: struct {
        data: []const u8, // JSON-encoded status
    },

    /// Workflow history result
    workflow_history_result: struct {
        data: []const u8, // JSON-encoded history events
    },

    /// Workflow list runs result
    workflow_list_runs_result: struct {
        data: []const u8, // JSON-encoded list of runs
    },

    /// Workflow definition result
    workflow_definition_result: struct {
        definition_yaml: []const u8,
    },

    /// Workflow disabled
    workflow_disabled: void,

    /// Workflow enabled
    workflow_enabled: void,

    /// Workflow list definitions result
    workflow_list_definitions_result: struct {
        data: []const u8, // JSON-encoded list of definition summaries
    },

    // =========================================================================
    // Cluster Management Responses
    // =========================================================================

    /// Cluster status response
    cluster_status: struct {
        node_id: u32,
        leader_id: u32,
        term: u64,
        state: ClusterState,
        member_count: u32,
    },

    /// Cluster members response
    cluster_members: struct {
        /// Pre-serialized wire data
        /// Format: [count: u32] + [node_id: u32][state: u8][addr_len: u16][addr: bytes]...
        data: []const u8,
    },

    /// Cluster join response
    cluster_join_ok: struct {
        assigned_node_id: u32,
        leader_id: u32,
    },

    // =========================================================================
    // Namespace Management Responses
    // =========================================================================

    /// Namespace creation succeeded
    namespace_created: void,

    /// Namespace deletion succeeded
    namespace_deleted: void,

    /// List of namespaces
    namespace_list: struct {
        /// Pre-serialized wire data
        /// Format: [count: u32] + [name_len: u16][name: bytes]...
        data: []const u8,
    },

    /// Namespace info response
    namespace_info: struct {
        exists: bool,
        name: []const u8,
    },

    // =========================================================================
    // Processing Results
    // =========================================================================

    /// Processing job submitted successfully
    processing_submitted: struct {
        /// The assigned job ID
        job_id: []const u8,
    },

    /// Processing job stopped
    processing_stopped: void,

    /// Processing job cancelled
    processing_cancelled: void,

    /// Processing job status
    processing_status_result: struct {
        /// Pre-serialized status data (JSON)
        data: []const u8,
    },

    /// List of processing jobs
    processing_list_result: struct {
        /// Pre-serialized list data (JSON)
        data: []const u8,
        /// Cursor for pagination (null if no more)
        cursor: ?[]const u8 = null,
    },

    /// Savepoint created
    processing_savepoint_result: struct {
        /// The savepoint ID
        savepoint_id: []const u8,
    },

    /// Processing job restored from savepoint
    processing_restored: void,

    /// Processing job rescaled
    processing_rescaled: void,

    // =========================================================================
    // Layer 4: Time-Series Responses
    // =========================================================================

    /// Write succeeded
    ts_write_ok: struct {
        series_hash: u64,
        timestamp_ms: i64,
        sequence: u64,
    },

    /// Read result with raw data points (pre-serialized)
    /// Wire format: [count:u32] per point: [timestamp_ms:i64 LE][value:f64 LE]
    ts_read_result: struct {
        data: []const u8,
    },

    /// Query result with aggregated buckets (pre-serialized)
    /// Wire format: [series_count:u32] per series:
    ///   [key_len:u32][key][bucket_count:u32] per bucket: [window_start_ms:i64][value:f64]
    ts_query_result: struct {
        data: []const u8,
    },

    /// List result (measurements or series index keys)
    ts_list_result: struct {
        data: []const u8,
    },

    /// Retention config result (current retention policy for a measurement)
    /// Wire format: [raw_ttl_ms:u64][rule_count:u32] per rule:
    ///   [window_ms:u64][agg_len:u32][agg:bytes][ttl_ms:u64]
    ts_retention_result: struct {
        data: []const u8,
    },

    /// FloQL query result (serialised SeriesSet)
    ts_floql_result: struct {
        data: []const u8,
    },

    // =========================================================================
    // Supporting Types
    // =========================================================================

    pub const ClusterState = enum(u8) {
        follower = 0,
        candidate = 1,
        leader = 2,
    };

    pub const Error = struct {
        code: ErrorCode,
        message: []const u8,
    };

    pub const ErrorCode = enum(u16) {
        // Generic errors (0x0000-0x00FF)
        unknown = 0x0000,
        invalid_request = 0x0001,
        unauthorized = 0x0002,
        not_found = 0x0003,
        already_exists = 0x0004,
        timeout = 0x0005,
        internal_error = 0x0006,
        unavailable = 0x0007,
        overloaded = 0x0008,

        // KV errors (0x0100-0x01FF)
        kv_key_too_large = 0x0100,
        kv_value_too_large = 0x0101,
        kv_namespace_not_found = 0x0102,
        kv_txn_unknown = 0x0110, // unknown or already-finalized txn id
        kv_txn_cross_shard = 0x0111, // op key doesn't hash to txn's pinned partition
        kv_txn_too_large = 0x0112, // exceeded ops/payload cap inside txn
        kv_txn_timeout = 0x0113, // txn idle/total timeout reached server-side
        kv_txn_unsupported_op = 0x0114, // op not allowed inside a txn (scan, mget, json, history)

        // Stream errors (0x0200-0x02FF)
        stream_not_found = 0x0200,
        stream_offset_out_of_range = 0x0201,
        stream_partition_not_found = 0x0202,
        conflict = 0x0203, // Exclusive lease held by another consumer

        // Queue errors (0x0300-0x03FF)
        queue_not_found = 0x0300,
        queue_message_too_large = 0x0301,
        queue_duplicate_message = 0x0302,

        // Consumer group errors (0x0400-0x04FF)
        group_not_found = 0x0400,
        group_rebalancing = 0x0401,
        group_consumer_not_found = 0x0402,

        // Worker errors (0x0500-0x05FF)
        worker_not_found = 0x0500,
        task_not_found = 0x0502,

        // Workflow errors (0x0600-0x06FF)
        workflow_not_found = 0x0600,
        workflow_already_completed = 0x0601,
        workflow_cancelled = 0x0602,
        workflow_disabled = 0x0603,

        // Cluster errors (0x0800-0x08FF)
        not_leader = 0x0800,
        no_leader = 0x0801,
        partition_unavailable = 0x0802,
        replication_timeout = 0x0803,
        quorum_not_reached = 0x0804,
        partition_moved = 0x0805,

        // Namespace errors (0x0900-0x09FF)
        namespace_not_empty = 0x0900,

        /// The wire status a client is told. No `else`: a new code must
        /// say what it is to a client, in every module at once.
        pub fn toStatus(code: ErrorCode) flo_proto.StatusCode {
            return switch (code) {
                .invalid_request,
                .kv_key_too_large,
                .kv_value_too_large,
                .kv_txn_cross_shard,
                .kv_txn_too_large,
                .kv_txn_unsupported_op,
                .stream_offset_out_of_range,
                .queue_message_too_large,
                .workflow_disabled,
                => .bad_request,
                .unauthorized => .unauthorized,
                .not_found,
                .kv_namespace_not_found,
                .kv_txn_unknown,
                .stream_not_found,
                .stream_partition_not_found,
                .queue_not_found,
                .group_not_found,
                .group_consumer_not_found,
                .worker_not_found,
                .task_not_found,
                .workflow_not_found,
                => .not_found,
                .already_exists,
                .conflict,
                .queue_duplicate_message,
                .namespace_not_empty,
                .workflow_already_completed,
                .workflow_cancelled,
                => .conflict,
                .unavailable,
                .not_leader,
                .no_leader,
                .partition_unavailable,
                .replication_timeout,
                .quorum_not_reached,
                .partition_moved,
                => .unavailable,
                .overloaded => .overloaded,
                .unknown,
                .timeout,
                .internal_error,
                .kv_txn_timeout,
                .group_rebalancing,
                => .internal_error,
            };
        }
    };

    pub const KVEntry = struct {
        key: []const u8,
        value: []const u8,
        version: u64,
    };

    pub const HistoryEntry = struct {
        value: []const u8,
        version: u32,
        lsn: u64,
    };

    pub const StreamMessage = struct {
        sequence: u64,
        timestamp_ms: i64,
        partition: u32,
        key: ?[]const u8,
        payload: []const u8,
        headers: ?[]const Header,
    };

    pub const Header = struct {
        key: []const u8,
        value: []const u8,
    };

    pub const GroupMessage = struct {
        message: StreamMessage,
        delivery_count: u32,
    };

    pub const QueueMessage = struct {
        seq: u64,
        payload: []const u8,
        enqueued_at: i64,
        delivery_count: u32,
        priority: u8,
    };

    /// Status of an action run
    pub const ActionRunStatus = enum(u8) {
        pending = 0,
        running = 1,
        completed = 2,
        failed = 3,
        cancelled = 4,
        timed_out = 5,
    };

    pub const WorkflowStatus = enum(u8) {
        running = 0,
        completed = 1,
        failed = 2,
        cancelled = 3,
        timed_out = 4,
    };

    // NOTE: CircuitState moved to src/workflow/plan_types.zig as CircuitBreakerState
};
