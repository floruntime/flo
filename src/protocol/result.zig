//! CommandResult - Unified result type for all Flo operations
//!
//! This module defines the canonical result representation returned by all
//! command handlers across Layers 1-3. Results are serialized for cross-core
//! messaging and protocol responses.

const std = @import("std");
const Allocator = std.mem.Allocator;
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

    /// Pong response to ping
    pong: void,

    /// Authentication successful response
    /// Returns user_id and locked namespace (if any) from validated token
    auth_ok: struct {
        user_id: ?[]const u8,
        namespace: ?[]const u8,
    },

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

    /// Transaction started successfully
    kv_txn_started: void,

    /// Transaction committed with operation count
    kv_txn_committed: struct {
        operations: u32,
    },

    /// Transaction rolled back
    kv_txn_rolled_back: void,

    /// Snapshot created successfully
    kv_snapshot_created: struct {
        snapshot_id: u64,
        lsn: u64,
    },

    /// Snapshot released successfully
    kv_snapshot_released: void,

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
    /// Wire format: [deleted_count:u64][first_seq:u64] (new first sequence after trim)
    stream_trimmed: struct {
        deleted_count: u64,
        first_seq: u64,
    },

    /// Stream subscription confirmed
    /// Wire format: [subscription_id:u64][start_seq:u64]
    subscribed: struct {
        subscription_id: u64,
        start_seq: u64,
    },

    /// Stream subscription ended
    /// Wire format: [subscription_id:u64]
    unsubscribed: struct {
        subscription_id: u64,
    },

    /// Stream list response
    /// Wire format: [count:u32] ([name_len:u32][name][partition_count:u32])* [has_more:u8] [cursor_len:u16][cursor]?
    /// Cursor follows ShardWalker format for cross-shard iteration.
    streams_listed: struct {
        /// Pre-serialized wire data (includes has_more and cursor)
        data: []const u8,
    },

    /// Stream event (server-push for subscriptions)
    stream_event: struct {
        stream: []const u8,
        message: StreamMessage,
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

    /// Touch/extend lease response with count of affected messages
    queue_touched: struct {
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
        allocated: bool = false,
    },

    /// Action invoked successfully
    action_invoked: struct {
        run_id: []const u8,
        /// Estimated queue position (if queued)
        queue_position: ?u32 = null,
        allocated: bool = false,
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
        allocated: bool = false,
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
        allocated: bool = false,
    },

    /// Task assignment (may be null if no tasks available)
    task_assignment: ?Task,

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
        allocated: bool = false,
    },

    /// Workflow started
    workflow_started: struct {
        run_id: []const u8,
        already_exists: bool = false,
        allocated: bool = false,
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
        allocated: bool = false,
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
        /// If true, data was allocated and should be freed
        allocated: bool = false,
    },

    /// Namespace info response
    namespace_info: struct {
        exists: bool,
        name: []const u8,
        allocated: bool = false,
    },

    /// Namespace config set succeeded
    namespace_config_set: void,

    /// Namespace config get response (pre-serialized settings TLV)
    namespace_config_get: struct {
        data: []const u8,
        allocated: bool = false,
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
        /// Set to true if message was allocated and needs to be freed
        /// Static strings (from code) should have this set to false
        allocated: bool = false,
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

        // KV errors (0x0100-0x01FF)
        kv_key_too_large = 0x0100,
        kv_value_too_large = 0x0101,
        kv_namespace_not_found = 0x0102,
        kv_txn_already_active = 0x0110,
        kv_txn_not_active = 0x0111,
        kv_txn_cross_core = 0x0112,
        kv_txn_conflict = 0x0113, // Optimistic locking conflict detected
        kv_snapshot_not_found = 0x0120, // Snapshot ID not found

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

    pub const Task = struct {
        task_id: []const u8,
        task_type: []const u8,
        payload: []const u8,
        created_at: i64,
        attempt: u32,
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

    /// Free any owned memory in this result
    /// Call this after the result has been fully processed (e.g., after Response is sent)
    pub fn deinit(self: CommandResult, allocator: Allocator) void {
        switch (self) {
            .kv_value => |v| {
                if (v.value.len > 0) {
                    // The value was allocated by KV.get() and needs to be freed
                    allocator.free(v.value);
                }
            },
            .err => |e| {
                // Only free if the message was allocated (deserialized from network)
                // Static string literals have allocated=false (default)
                if (e.allocated and e.message.len > 0) {
                    allocator.free(e.message);
                }
            },
            .auth_ok => |a| {
                // Free user_id and namespace if they were allocated (from deserialization)
                if (a.user_id) |uid| {
                    if (uid.len > 0) allocator.free(uid);
                }
                if (a.namespace) |ns| {
                    if (ns.len > 0) allocator.free(ns);
                }
            },
            // Pre-serialized result types - just free the data blob
            .kv_scan_result => |r| {
                if (r.data.len > 0) allocator.free(r.data);
            },
            .kv_history_result => |h| {
                if (h.data.len > 0) allocator.free(h.data);
            },
            .kv_mget_result => |b| {
                if (b.data.len > 0) allocator.free(b.data);
            },
            .stream_messages => |m| {
                if (m.data.len > 0) allocator.free(m.data);
            },
            .streams_listed => |s| {
                if (s.data.len > 0) allocator.free(s.data);
            },
            .group_messages => |g| {
                if (g.data.len > 0) allocator.free(g.data);
            },
            .group_pending => |g| {
                if (g.data.len > 0) allocator.free(g.data);
            },
            .group_touch => |g| {
                if (g.data.len > 0) allocator.free(g.data);
            },
            .queue_messages => |q| {
                if (q.data.len > 0) allocator.free(q.data);
            },
            .queue_peek_messages => |q| {
                if (q.data.len > 0) allocator.free(q.data);
            },
            .queue_dlq_messages => |q| {
                if (q.data.len > 0) allocator.free(q.data);
            },
            .queue_enqueued => |q| {
                if (q.message_id.len > 0) allocator.free(q.message_id);
            },
            .queues_listed => |s| {
                if (s.data.len > 0) allocator.free(s.data);
            },
            .namespace_list => |n| {
                if (n.allocated and n.data.len > 0) allocator.free(n.data);
            },
            .namespace_info => |n| {
                if (n.allocated and n.name.len > 0) allocator.free(n.name);
            },
            .namespace_config_get => |n| {
                if (n.allocated and n.data.len > 0) allocator.free(n.data);
            },
            .action_list_result => |a| {
                if (a.data.len > 0) allocator.free(a.data);
                if (a.cursor) |c| allocator.free(c);
            },
            .action_registered => |a| {
                if (a.allocated) {
                    allocator.free(a.name);
                    allocator.free(a.version);
                }
            },
            .action_invoked => |a| {
                if (a.allocated) {
                    allocator.free(a.run_id);
                }
            },
            .action_run_status => |a| {
                if (a.allocated) {
                    allocator.free(a.run_id);
                    if (a.output) |o| allocator.free(o);
                    if (a.error_message) |e| allocator.free(e);
                }
            },
            .workers_listed => |w| {
                if (w.data.len > 0) allocator.free(w.data);
                if (w.cursor) |c| allocator.free(c);
            },
            .worker_registered => |w| {
                if (w.allocated) {
                    allocator.free(w.worker_id);
                }
            },
            .workflow_created => |w| {
                if (w.allocated) {
                    allocator.free(w.workflow_name);
                }
            },
            .workflow_started => |w| {
                if (w.allocated) {
                    allocator.free(w.run_id);
                }
            },
            .workflow_status_result => |w| {
                if (w.data.len > 0) allocator.free(w.data);
            },
            .workflow_history_result => |w| {
                if (w.data.len > 0) allocator.free(w.data);
            },
            .workflow_list_runs_result => |r| {
                if (r.data.len > 0) allocator.free(r.data);
            },
            .workflow_definition_result => |w| {
                if (w.allocated) {
                    if (w.definition_yaml.len > 0) allocator.free(w.definition_yaml);
                }
            },
            .workflow_list_definitions_result => |r| {
                if (r.data.len > 0) allocator.free(r.data);
            },
            .stream_event => |s| {
                allocator.free(s.stream);
                // Free StreamMessage sub-fields allocated by deserializeStreamMessage
                if (s.message.key) |k| allocator.free(k);
                allocator.free(s.message.payload);
                if (s.message.headers) |hdrs| {
                    for (hdrs) |h| {
                        allocator.free(h.key);
                        allocator.free(h.value);
                    }
                    allocator.free(hdrs);
                }
            },
            .group_joined => |g| {
                allocator.free(g.assigned_partitions);
            },
            .cluster_members => |c| {
                if (c.data.len > 0) allocator.free(c.data);
            },
            .processing_submitted => |p| {
                if (p.job_id.len > 0) allocator.free(p.job_id);
            },
            .processing_status_result => |p| {
                if (p.data.len > 0) allocator.free(p.data);
            },
            .processing_list_result => |p| {
                if (p.data.len > 0) allocator.free(p.data);
                if (p.cursor) |c| allocator.free(c);
            },
            .processing_savepoint_result => |p| {
                if (p.savepoint_id.len > 0) allocator.free(p.savepoint_id);
            },
            // Time-series results - pre-serialized data blobs
            .ts_read_result => |r| {
                if (r.data.len > 0) allocator.free(r.data);
            },
            .ts_query_result => |r| {
                if (r.data.len > 0) allocator.free(r.data);
            },
            .ts_list_result => |r| {
                if (r.data.len > 0) allocator.free(r.data);
            },
            .ts_retention_result => |r| {
                if (r.data.len > 0) allocator.free(r.data);
            },
            .ts_floql_result => |r| {
                if (r.data.len > 0) allocator.free(r.data);
            },
            .task_assignment => |maybe_task| {
                if (maybe_task) |t| {
                    // readSlice() allocates even for zero-length slices, so always free
                    allocator.free(t.task_id);
                    allocator.free(t.task_type);
                    allocator.free(t.payload);
                }
            },
            // Remaining variants have no heap-allocated fields
            else => {},
        }
    }

    /// Get the opcode for this result
    pub fn opcode(self: CommandResult) flo_proto.OpCode {
        return switch (self) {
            .ok => .ok,
            .pending => .ok, // Pending results are not sent over wire, but map to OK if forced
            .pong => .pong,
            .auth_ok => .auth, // Auth success response
            .err => .error_response,

            .kv_value, .kv_not_found => .kv_get_response,
            .kv_put_ok, .kv_cas_failed, .kv_condition_not_met => .kv_put_response,
            .kv_scan_result => .kv_scan_response,
            .kv_history_result => .kv_history_response,
            .kv_mget_result => .kv_mget_response,
            .kv_txn_started, .kv_txn_committed, .kv_txn_rolled_back => .ok,
            .kv_snapshot_created => .kv_snapshot_create_response,
            .kv_snapshot_released => .ok,

            .stream_append_ok => .stream_append_response,
            .stream_messages => .stream_read_response,
            .stream_info => .ok, // Uses generic OK opcode with data
            .stream_trimmed => .ok, // Uses generic OK opcode with data
            .subscribed => .ok, // Uses generic OK opcode with data
            .unsubscribed => .ok, // Uses generic OK opcode with data
            .streams_listed => .stream_list_response,
            .stream_event => .stream_event,

            .group_joined => .ok,
            .group_messages => .stream_group_read_response,
            .group_pending => .stream_group_pending,
            .group_touch => .ok, // Uses generic OK opcode with count

            .queue_enqueued => .queue_enqueue_response,
            .queue_messages => .queue_dequeue_response,
            .queue_peek_messages => .queue_peek_response,
            .queue_dlq_messages => .queue_dlq_list_response,
            .queue_touched => .queue_touch_response,
            .queues_listed => .queue_list_response,

            .action_registered => .action_register_response,
            .action_invoked => .action_invoke_response,
            .action_run_status => .action_status_response,
            .action_list_result => .action_list_response,
            .action_deleted => .ok,

            .worker_registered => .worker_register_response,
            .task_assignment => .action_task_assignment,
            .workers_listed => .worker_list_response,

            .workflow_created => .workflow_create_response,
            .workflow_started => .workflow_start_response,
            .workflow_signaled => .ok,
            .workflow_cancelled => .ok,
            .workflow_status_result => .workflow_status_response,
            .workflow_history_result => .workflow_history_response,
            .workflow_list_runs_result => .workflow_list_runs_response,
            .workflow_definition_result => .workflow_get_definition_response,
            .workflow_disabled => .workflow_disable_response,
            .workflow_enabled => .workflow_enable_response,
            .workflow_list_definitions_result => .workflow_list_definitions_response,

            // Cluster results
            .cluster_status => .cluster_status_response,
            .cluster_members => .cluster_members_response,
            .cluster_join_ok => .cluster_join_response,

            // Namespace results
            .namespace_created => .namespace_create_response,
            .namespace_deleted => .namespace_delete_response,
            .namespace_list => .namespace_list_response,
            .namespace_info => .namespace_info_response,
            .namespace_config_set => .namespace_config_set_response,
            .namespace_config_get => .namespace_config_get_response,

            // Processing results
            .processing_submitted => .processing_submit_response,
            .processing_stopped => .processing_stop_response,
            .processing_cancelled => .processing_cancel_response,
            .processing_status_result => .processing_status_response,
            .processing_list_result => .processing_list_response,
            .processing_savepoint_result => .processing_savepoint_response,
            .processing_restored => .processing_restore_response,
            .processing_rescaled => .processing_rescale_response,

            // Time-series results
            .ts_write_ok => .ts_write_response,
            .ts_read_result => .ts_read_response,
            .ts_query_result => .ts_query_response,
            .ts_list_result => .ts_list_response,
            .ts_retention_result => .ts_retention_response,
            .ts_floql_result => .ts_floql_response,
        };
    }

    /// Calculate serialized size for cross-core messaging
    pub fn serializedSize(self: CommandResult) usize {
        return switch (self) {
            .ok, .pending, .pong, .kv_not_found, .kv_txn_started, .kv_txn_rolled_back, .kv_snapshot_released, .kv_condition_not_met => 1,
            .auth_ok => |a| 1 + 1 + (if (a.user_id) |u| 4 + u.len else @as(usize, 0)) + 1 + (if (a.namespace) |n| 4 + n.len else @as(usize, 0)),
            .kv_txn_committed => 1 + 4,
            .kv_snapshot_created => 1 + 8 + 8,
            .err => |e| 1 + 2 + 4 + e.message.len,

            .kv_value => |v| 1 + 4 + v.value.len + 8,
            .kv_put_ok => |v| 1 + 8 + if (v.version > 0) @as(usize, 0) else 0,
            .kv_cas_failed => 1 + 8,
            // Pre-serialized types: tag + length prefix + data blob
            .kv_scan_result => |r| 1 + 4 + r.data.len,
            .kv_history_result => |h| 1 + 4 + h.data.len,
            .kv_mget_result => |b| 1 + 4 + b.data.len,

            .stream_append_ok => 1 + 8 + 8,
            .stream_messages => |m| 1 + 4 + m.data.len + 16, // tag + len + data + next_timestamp_ms + next_sequence
            .stream_info => 1 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 8 + 8 + 8, // tag + first_ts + first_seq + last_ts + last_seq + count + bytes + partition_count + retention_age_s + retention_count + retention_bytes
            .stream_trimmed => 1 + 8 + 8, // tag + deleted_count + first_seq
            .subscribed => 1 + 8 + 8, // tag + subscription_id + start_seq
            .unsubscribed => 1 + 8, // tag + subscription_id
            .streams_listed => |s| 1 + 4 + s.data.len, // tag + len + data
            .stream_event => |e| 1 + 4 + e.stream.len + streamMessageSize(e.message),

            .group_joined => |g| 1 + 8 + 4 + g.assigned_partitions.len * 4,
            .group_messages => |g| 1 + 4 + g.data.len,
            .group_pending => |g| 1 + 4 + g.data.len,
            .group_touch => |g| 1 + 4 + g.data.len,

            .queue_enqueued => |q| 1 + 4 + q.message_id.len,
            .queue_messages => |m| 1 + 4 + m.data.len, // tag + len + data
            .queue_peek_messages => |m| 1 + 4 + m.data.len, // tag + len + data
            .queue_dlq_messages => |m| 1 + 4 + m.data.len, // tag + len + data (total_count is embedded)
            .queue_touched => 1 + 4, // tag + count
            .queues_listed => |s| 1 + 4 + s.data.len, // tag + len + data

            .action_registered => |a| 1 + 4 + a.name.len + 4 + a.version.len,
            .action_invoked => |a| blk: {
                var size: usize = 1 + 4 + a.run_id.len + 1;
                if (a.queue_position != null) size += 4;
                size += 1; // has_output (always false now)
                break :blk size;
            },
            .action_run_status => |s| blk: {
                var size: usize = 1 + 4 + s.run_id.len + 1 + 8 + 9 + 9 + 4; // tag + run_id + status + created_at + has_started + has_completed + retry_count
                if (s.started_at != null) size += 8;
                if (s.completed_at != null) size += 8;
                if (s.output) |o| size += 4 + o.len else size += 1;
                if (s.error_message) |e| size += 4 + e.len else size += 1;
                break :blk size;
            },
            .action_list_result => |a| 1 + 4 + a.data.len + 1 + if (a.cursor) |c| 4 + c.len else @as(usize, 0),
            .action_deleted => 1,

            .worker_registered => |w| 1 + 4 + w.worker_id.len + 4,
            .task_assignment => |t| blk: {
                if (t) |task| {
                    break :blk 1 + 1 + 4 + task.task_id.len + 4 + task.task_type.len + 4 + task.payload.len + 8 + 4;
                }
                break :blk 1 + 1;
            },
            .workers_listed => |w| 1 + 4 + w.data.len + 1 + if (w.cursor) |c| 4 + c.len else @as(usize, 0),

            .workflow_created => |w| 1 + 4 + w.workflow_name.len,
            .workflow_started => |w| 1 + 4 + w.run_id.len + 1,
            .workflow_signaled => 1,
            .workflow_cancelled => 1,
            .workflow_status_result => |s| 1 + 4 + s.data.len,
            .workflow_history_result => |h| 1 + 4 + h.data.len,
            .workflow_list_runs_result => |l| 1 + 4 + l.data.len,
            .workflow_definition_result => |d| 1 + 4 + d.definition_yaml.len,
            .workflow_disabled => 1,
            .workflow_enabled => 1,
            .workflow_list_definitions_result => |l| 1 + 4 + l.data.len,

            // Cluster results
            .cluster_status => 1 + 4 + 4 + 8 + 1 + 4, // tag + node_id + leader_id + term + state + member_count
            .cluster_members => |m| 1 + 4 + m.data.len, // tag + len + data
            .cluster_join_ok => 1 + 4 + 4, // tag + assigned_node_id + leader_id

            // Namespace results
            .namespace_created, .namespace_deleted, .namespace_config_set => 1, // just tag
            .namespace_list => |n| 1 + 4 + n.data.len, // tag + len + data
            .namespace_info => |n| 1 + 1 + 2 + n.name.len, // tag + exists + name_len + name
            .namespace_config_get => |n| 1 + 4 + n.data.len, // tag + len + data

            // Processing results
            .processing_submitted => |p| 1 + 4 + p.job_id.len, // tag + len + job_id
            .processing_stopped, .processing_cancelled, .processing_restored, .processing_rescaled => 1, // just tag
            .processing_status_result => |p| 1 + 4 + p.data.len, // tag + len + data
            .processing_list_result => |p| 1 + 4 + p.data.len + 1 + if (p.cursor) |c| 4 + c.len else @as(usize, 0),
            .processing_savepoint_result => |p| 1 + 4 + p.savepoint_id.len, // tag + len + savepoint_id

            // Time-series results
            .ts_write_ok => 1 + 8 + 8 + 8, // tag + series_hash + timestamp_ms + sequence
            .ts_read_result => |r| 1 + 4 + r.data.len, // tag + len + data
            .ts_query_result => |r| 1 + 4 + r.data.len, // tag + len + data
            .ts_list_result => |r| 1 + 4 + r.data.len, // tag + len + data
            .ts_retention_result => |r| 1 + 4 + r.data.len, // tag + len + data
            .ts_floql_result => |r| 1 + 4 + r.data.len, // tag + len + data
        };
    }

    fn streamMessageSize(msg: StreamMessage) usize {
        var size: usize = 8 + 8 + 4 + 4 + msg.payload.len + 1;
        if (msg.key) |k| size += 4 + k.len;
        if (msg.headers) |hdrs| {
            size += 4;
            for (hdrs) |h| {
                size += 4 + h.key.len + 4 + h.value.len;
            }
        }
        return size;
    }

    /// Serialize result to buffer for cross-core messaging
    pub fn serializeInto(self: CommandResult, buffer: []u8) void {
        var stream: std.Io.Writer = .fixed(buffer);
        self.serialize(&stream) catch unreachable;
    }

    fn serialize(self: CommandResult, writer: anytype) !void {
        // Write result tag
        try writer.writeByte(@intFromEnum(std.meta.activeTag(self)));

        switch (self) {
            .ok, .pending, .pong, .kv_not_found, .kv_txn_started, .kv_txn_rolled_back, .kv_snapshot_released, .kv_condition_not_met => {},
            .auth_ok => |a| {
                // Write has_user_id flag + optional user_id
                if (a.user_id) |uid| {
                    try writer.writeByte(1);
                    try writeSlice(writer, uid);
                } else {
                    try writer.writeByte(0);
                }
                // Write has_namespace flag + optional namespace
                if (a.namespace) |ns| {
                    try writer.writeByte(1);
                    try writeSlice(writer, ns);
                } else {
                    try writer.writeByte(0);
                }
            },
            .kv_txn_committed => |c| {
                try writer.writeInt(u32, c.operations, .little);
            },
            .kv_snapshot_created => |s| {
                try writer.writeInt(u64, s.snapshot_id, .little);
                try writer.writeInt(u64, s.lsn, .little);
            },

            .err => |e| {
                try writer.writeInt(u16, @intFromEnum(e.code), .little);
                try writeSlice(writer, e.message);
            },

            .kv_value => |v| {
                try writeSlice(writer, v.value);
                try writer.writeInt(u64, v.version, .little);
            },
            .kv_put_ok => |v| {
                try writer.writeInt(u64, v.version, .little);
            },
            .kv_cas_failed => |v| {
                try writer.writeInt(u64, v.current_version, .little);
            },
            .kv_scan_result => |r| {
                // Just write the pre-serialized data blob
                try writeSlice(writer, r.data);
            },
            .kv_history_result => |h| {
                try writeSlice(writer, h.data);
            },
            .kv_mget_result => |b| {
                try writeSlice(writer, b.data);
            },

            .stream_append_ok => |a| {
                try writer.writeInt(u64, a.sequence, .little);
                try writer.writeInt(i64, a.timestamp_ms, .little);
            },
            .stream_messages => |m| {
                try writeSlice(writer, m.data);
                try writer.writeInt(u64, m.next_timestamp_ms, .little);
                try writer.writeInt(u64, m.next_sequence, .little);
            },
            .stream_info => |i| {
                try writer.writeInt(u64, i.first_timestamp_ms, .little);
                try writer.writeInt(u64, i.first_seq, .little);
                try writer.writeInt(u64, i.last_timestamp_ms, .little);
                try writer.writeInt(u64, i.last_seq, .little);
                try writer.writeInt(u64, i.count, .little);
                try writer.writeInt(u64, i.bytes, .little);
                try writer.writeInt(u32, i.partition_count, .little);
                try writer.writeInt(u64, i.retention_age_s, .little);
                try writer.writeInt(u64, i.retention_count, .little);
                try writer.writeInt(u64, i.retention_bytes, .little);
            },
            .stream_trimmed => |t| {
                try writer.writeInt(u64, t.deleted_count, .little);
                try writer.writeInt(u64, t.first_seq, .little);
            },
            .subscribed => |s| {
                try writer.writeInt(u64, s.subscription_id, .little);
                try writer.writeInt(u64, s.start_seq, .little);
            },
            .unsubscribed => |u| {
                try writer.writeInt(u64, u.subscription_id, .little);
            },
            .streams_listed => |s| {
                try writeSlice(writer, s.data);
            },
            .stream_event => |e| {
                try writeSlice(writer, e.stream);
                try serializeStreamMessage(writer, e.message);
            },

            .group_joined => |g| {
                try writer.writeInt(u64, g.generation_id, .little);
                try writer.writeInt(u32, @intCast(g.assigned_partitions.len), .little);
                for (g.assigned_partitions) |p| {
                    try writer.writeInt(u32, p, .little);
                }
            },
            .group_messages => |g| {
                try writeSlice(writer, g.data);
            },
            .group_pending => |g| {
                try writeSlice(writer, g.data);
            },
            .group_touch => |g| {
                try writeSlice(writer, g.data);
            },

            .queue_enqueued => |q| {
                try writeSlice(writer, q.message_id);
            },
            .queue_messages => |m| {
                try writeSlice(writer, m.data);
            },
            .queue_peek_messages => |m| {
                try writeSlice(writer, m.data);
            },
            .queue_dlq_messages => |m| {
                try writeSlice(writer, m.data);
            },
            .queue_touched => |t| {
                try writer.writeInt(u32, t.count, .little);
            },
            .queues_listed => |s| {
                try writeSlice(writer, s.data);
            },

            .action_registered => |a| {
                try writeSlice(writer, a.name);
                try writeSlice(writer, a.version);
            },
            .action_invoked => |a| {
                try writeSlice(writer, a.run_id);
                if (a.queue_position) |pos| {
                    try writer.writeByte(1);
                    try writer.writeInt(u32, pos, .little);
                } else {
                    try writer.writeByte(0);
                }
                try writer.writeByte(0); // has_output (always false)
            },
            .action_run_status => |s| {
                try writeSlice(writer, s.run_id);
                try writer.writeByte(@intFromEnum(s.status));
                try writer.writeInt(i64, s.created_at, .little);
                try writeOptionalI64(writer, s.started_at);
                try writeOptionalI64(writer, s.completed_at);
                try writeOptionalSlice(writer, s.output);
                try writeOptionalSlice(writer, s.error_message);
                try writer.writeInt(u32, s.retry_count, .little);
            },
            .action_list_result => |a| {
                try writeSlice(writer, a.data);
                try writeOptionalSlice(writer, a.cursor);
            },
            .action_deleted => {},

            .worker_registered => |w| {
                try writeSlice(writer, w.worker_id);
                try writer.writeInt(u32, w.heartbeat_interval_ms, .little);
            },
            .task_assignment => |t| {
                if (t) |task| {
                    try writer.writeByte(1);
                    try writeSlice(writer, task.task_id);
                    try writeSlice(writer, task.task_type);
                    try writeSlice(writer, task.payload);
                    try writer.writeInt(i64, task.created_at, .little);
                    try writer.writeInt(u32, task.attempt, .little);
                } else {
                    try writer.writeByte(0);
                }
            },
            .workers_listed => |w| {
                try writeSlice(writer, w.data);
                try writeOptionalSlice(writer, w.cursor);
            },

            .workflow_created => |w| {
                try writeSlice(writer, w.workflow_name);
            },
            .workflow_started => |w| {
                try writeSlice(writer, w.run_id);
                try writer.writeByte(if (w.already_exists) 1 else 0);
            },
            .workflow_signaled => {},
            .workflow_cancelled => {},
            .workflow_status_result => |s| {
                try writeSlice(writer, s.data);
            },
            .workflow_history_result => |h| {
                try writeSlice(writer, h.data);
            },
            .workflow_list_runs_result => |l| {
                try writeSlice(writer, l.data);
            },
            .workflow_definition_result => |d| {
                try writeSlice(writer, d.definition_yaml);
            },
            .workflow_disabled => {},
            .workflow_enabled => {},
            .workflow_list_definitions_result => |l| {
                try writeSlice(writer, l.data);
            },

            // Cluster results
            .cluster_status => |s| {
                try writer.writeInt(u32, s.node_id, .little);
                try writer.writeInt(u32, s.leader_id, .little);
                try writer.writeInt(u64, s.term, .little);
                try writer.writeByte(@intFromEnum(s.state));
                try writer.writeInt(u32, s.member_count, .little);
            },
            .cluster_members => |m| {
                try writeSlice(writer, m.data);
            },
            .cluster_join_ok => |j| {
                try writer.writeInt(u32, j.assigned_node_id, .little);
                try writer.writeInt(u32, j.leader_id, .little);
            },

            // Namespace results
            .namespace_created, .namespace_deleted, .namespace_config_set => {},
            .namespace_list => |n| {
                try writeSlice(writer, n.data);
            },
            .namespace_info => |n| {
                try writer.writeByte(if (n.exists) 1 else 0);
                try writer.writeInt(u16, @intCast(n.name.len), .little);
                try writer.writeAll(n.name);
            },
            .namespace_config_get => |n| {
                try writeSlice(writer, n.data);
            },

            // Processing results
            .processing_submitted => |p| {
                try writeSlice(writer, p.job_id);
            },
            .processing_stopped, .processing_cancelled, .processing_restored, .processing_rescaled => {},
            .processing_status_result => |p| {
                try writeSlice(writer, p.data);
            },
            .processing_list_result => |p| {
                try writeSlice(writer, p.data);
                try writeOptionalSlice(writer, p.cursor);
            },
            .processing_savepoint_result => |p| {
                try writeSlice(writer, p.savepoint_id);
            },

            // Time-series results
            .ts_write_ok => |t| {
                try writer.writeInt(u64, t.series_hash, .little);
                try writer.writeInt(i64, t.timestamp_ms, .little);
                try writer.writeInt(u64, t.sequence, .little);
            },
            .ts_read_result => |r| {
                try writeSlice(writer, r.data);
            },
            .ts_query_result => |r| {
                try writeSlice(writer, r.data);
            },
            .ts_list_result => |r| {
                try writeSlice(writer, r.data);
            },
            .ts_retention_result => |r| {
                try writeSlice(writer, r.data);
            },
            .ts_floql_result => |r| {
                try writeSlice(writer, r.data);
            },
        }
    }

    fn serializeStreamMessage(writer: anytype, msg: StreamMessage) !void {
        try writer.writeInt(u64, msg.sequence, .little);
        try writer.writeInt(i64, msg.timestamp_ms, .little);
        try writer.writeInt(u32, msg.partition, .little);
        try writeOptionalSlice(writer, msg.key);
        try writeSlice(writer, msg.payload);
        if (msg.headers) |hdrs| {
            try writer.writeInt(u32, @intCast(hdrs.len), .little);
            for (hdrs) |h| {
                try writeSlice(writer, h.key);
                try writeSlice(writer, h.value);
            }
        } else {
            try writer.writeInt(u32, 0, .little);
        }
    }

    fn writeSlice(writer: anytype, slice: []const u8) !void {
        try writer.writeInt(u32, @intCast(slice.len), .little);
        try writer.writeAll(slice);
    }

    fn writeOptionalSlice(writer: anytype, slice: ?[]const u8) !void {
        if (slice) |s| {
            try writer.writeByte(1);
            try writeSlice(writer, s);
        } else {
            try writer.writeByte(0);
        }
    }

    fn writeOptionalI64(writer: anytype, value: ?i64) !void {
        if (value) |v| {
            try writer.writeByte(1);
            try writer.writeInt(i64, v, .little);
        } else {
            try writer.writeByte(0);
        }
    }

    /// Deserialize result from buffer
    pub fn deserialize(buffer: []const u8, allocator: Allocator) !CommandResult {
        var stream: std.Io.Reader = .fixed(buffer);
        return deserializeFromReader(&stream, allocator);
    }

    fn deserializeFromReader(reader: anytype, allocator: Allocator) !CommandResult {
        const tag_byte = try reader.takeByte();
        const tag: std.meta.Tag(CommandResult) = @enumFromInt(tag_byte);

        return switch (tag) {
            .ok => .{ .ok = {} },
            .pending => .{ .pending = {} },
            .pong => .{ .pong = {} },
            .auth_ok => blk: {
                const has_user_id = try reader.takeByte();
                const user_id: ?[]const u8 = if (has_user_id == 1) try readSlice(reader, allocator) else null;
                const has_namespace = try reader.takeByte();
                const namespace: ?[]const u8 = if (has_namespace == 1) try readSlice(reader, allocator) else null;
                break :blk .{ .auth_ok = .{ .user_id = user_id, .namespace = namespace } };
            },
            .kv_not_found => .{ .kv_not_found = {} },
            .kv_txn_started => .{ .kv_txn_started = {} },
            .kv_txn_rolled_back => .{ .kv_txn_rolled_back = {} },
            .kv_snapshot_released => .{ .kv_snapshot_released = {} },
            .kv_condition_not_met => .{ .kv_condition_not_met = {} },
            .kv_txn_committed => .{ .kv_txn_committed = .{
                .operations = try reader.takeInt(u32, .little),
            } },
            .kv_snapshot_created => .{ .kv_snapshot_created = .{
                .snapshot_id = try reader.takeInt(u64, .little),
                .lsn = try reader.takeInt(u64, .little),
            } },

            .err => .{
                .err = .{
                    .code = @enumFromInt(try reader.takeInt(u16, .little)),
                    .message = try readSlice(reader, allocator),
                    .allocated = true, // Mark as allocated since we read from network
                },
            },

            .kv_value => .{ .kv_value = .{
                .value = try readSlice(reader, allocator),
                .version = try reader.takeInt(u64, .little),
            } },
            .kv_put_ok => .{ .kv_put_ok = .{
                .version = try reader.takeInt(u64, .little),
            } },
            .kv_cas_failed => .{ .kv_cas_failed = .{
                .current_version = try reader.takeInt(u64, .little),
            } },
            .kv_scan_result => .{ .kv_scan_result = .{
                .data = try readSlice(reader, allocator),
            } },
            .kv_history_result => .{ .kv_history_result = .{
                .data = try readSlice(reader, allocator),
            } },
            .kv_mget_result => .{ .kv_mget_result = .{
                .data = try readSlice(reader, allocator),
            } },

            .stream_append_ok => .{ .stream_append_ok = .{
                .sequence = try reader.takeInt(u64, .little),
                .timestamp_ms = try reader.takeInt(i64, .little),
            } },
            .stream_messages => .{ .stream_messages = .{
                .data = try readSlice(reader, allocator),
                .next_timestamp_ms = try reader.takeInt(u64, .little),
                .next_sequence = try reader.takeInt(u64, .little),
            } },
            .stream_info => .{ .stream_info = .{
                .first_timestamp_ms = try reader.takeInt(u64, .little),
                .first_seq = try reader.takeInt(u64, .little),
                .last_timestamp_ms = try reader.takeInt(u64, .little),
                .last_seq = try reader.takeInt(u64, .little),
                .count = try reader.takeInt(u64, .little),
                .bytes = try reader.takeInt(u64, .little),
                .partition_count = try reader.takeInt(u32, .little),
                .retention_age_s = try reader.takeInt(u64, .little),
                .retention_count = try reader.takeInt(u64, .little),
                .retention_bytes = try reader.takeInt(u64, .little),
            } },
            .stream_trimmed => .{ .stream_trimmed = .{
                .deleted_count = try reader.takeInt(u64, .little),
                .first_seq = try reader.takeInt(u64, .little),
            } },
            .subscribed => .{ .subscribed = .{
                .subscription_id = try reader.takeInt(u64, .little),
                .start_seq = try reader.takeInt(u64, .little),
            } },
            .unsubscribed => .{ .unsubscribed = .{
                .subscription_id = try reader.takeInt(u64, .little),
            } },
            .streams_listed => .{ .streams_listed = .{
                .data = try readSlice(reader, allocator),
            } },
            .stream_event => .{ .stream_event = .{
                .stream = try readSlice(reader, allocator),
                .message = try deserializeStreamMessage(reader, allocator),
            } },

            .group_joined => blk: {
                const gen_id = try reader.takeInt(u64, .little);
                const count = try reader.takeInt(u32, .little);
                const partitions = try allocator.alloc(u32, count);
                for (partitions) |*p| {
                    p.* = try reader.takeInt(u32, .little);
                }
                break :blk .{ .group_joined = .{
                    .generation_id = gen_id,
                    .assigned_partitions = partitions,
                } };
            },
            .group_messages => .{ .group_messages = .{
                .data = try readSlice(reader, allocator),
            } },
            .group_pending => .{ .group_pending = .{
                .data = try readSlice(reader, allocator),
            } },
            .group_touch => blk: {
                const data = try readSlice(reader, allocator);
                const count = if (data.len >= 4) std.mem.readInt(u32, data[0..4], .little) else 0;
                break :blk .{ .group_touch = .{
                    .touched_count = count,
                    .data = data,
                } };
            },

            .queue_enqueued => .{ .queue_enqueued = .{
                .message_id = try readSlice(reader, allocator),
            } },
            .queue_messages => .{ .queue_messages = .{
                .data = try readSlice(reader, allocator),
            } },
            .queue_peek_messages => .{ .queue_peek_messages = .{
                .data = try readSlice(reader, allocator),
            } },
            .queue_dlq_messages => .{ .queue_dlq_messages = .{
                .data = try readSlice(reader, allocator),
            } },
            .queue_touched => .{ .queue_touched = .{
                .count = try reader.takeInt(u32, .little),
            } },
            .queues_listed => .{ .queues_listed = .{
                .data = try readSlice(reader, allocator),
            } },

            .action_registered => .{ .action_registered = .{
                .name = try readSlice(reader, allocator),
                .version = try readSlice(reader, allocator),
                .allocated = true,
            } },
            .action_invoked => blk: {
                const run_id = try readSlice(reader, allocator);
                const has_pos = try reader.takeByte() != 0;
                const queue_pos = if (has_pos) try reader.takeInt(u32, .little) else null;
                const has_output = (reader.takeByte() catch 0) != 0;
                if (has_output) {
                    // Skip output bytes for wire compatibility
                    const out = try readSlice(reader, allocator);
                    allocator.free(out);
                }
                break :blk .{ .action_invoked = .{
                    .run_id = run_id,
                    .queue_position = queue_pos,
                    .allocated = true,
                } };
            },
            .action_run_status => .{ .action_run_status = .{
                .run_id = try readSlice(reader, allocator),
                .status = @enumFromInt(try reader.takeByte()),
                .created_at = try reader.takeInt(i64, .little),
                .started_at = try readOptionalI64(reader),
                .completed_at = try readOptionalI64(reader),
                .output = try readOptionalSlice(reader, allocator),
                .error_message = try readOptionalSlice(reader, allocator),
                .retry_count = try reader.takeInt(u32, .little),
                .allocated = true,
            } },
            .action_list_result => .{ .action_list_result = .{
                .data = try readSlice(reader, allocator),
                .cursor = try readOptionalSlice(reader, allocator),
            } },
            .action_deleted => .{ .action_deleted = {} },

            .worker_registered => .{ .worker_registered = .{
                .worker_id = try readSlice(reader, allocator),
                .heartbeat_interval_ms = try reader.takeInt(u32, .little),
                .allocated = true,
            } },
            .task_assignment => blk: {
                const present = try reader.takeByte() != 0;
                if (present) {
                    break :blk .{ .task_assignment = .{
                        .task_id = try readSlice(reader, allocator),
                        .task_type = try readSlice(reader, allocator),
                        .payload = try readSlice(reader, allocator),
                        .created_at = try reader.takeInt(i64, .little),
                        .attempt = try reader.takeInt(u32, .little),
                    } };
                }
                break :blk .{ .task_assignment = null };
            },
            .workers_listed => .{ .workers_listed = .{
                .data = try readSlice(reader, allocator),
                .cursor = try readOptionalSlice(reader, allocator),
            } },

            .workflow_created => .{ .workflow_created = .{
                .workflow_name = try readSlice(reader, allocator),
                .allocated = true,
            } },
            .workflow_started => .{ .workflow_started = .{
                .run_id = try readSlice(reader, allocator),
                .already_exists = try reader.takeByte() != 0,
                .allocated = true,
            } },
            .workflow_signaled => .{ .workflow_signaled = {} },
            .workflow_cancelled => .{ .workflow_cancelled = {} },
            .workflow_status_result => .{ .workflow_status_result = .{
                .data = try readSlice(reader, allocator),
            } },
            .workflow_history_result => .{ .workflow_history_result = .{
                .data = try readSlice(reader, allocator),
            } },
            .workflow_list_runs_result => .{ .workflow_list_runs_result = .{
                .data = try readSlice(reader, allocator),
            } },
            .workflow_definition_result => .{ .workflow_definition_result = .{
                .definition_yaml = try readSlice(reader, allocator),
                .allocated = true,
            } },
            .workflow_disabled => .{ .workflow_disabled = {} },
            .workflow_enabled => .{ .workflow_enabled = {} },
            .workflow_list_definitions_result => .{ .workflow_list_definitions_result = .{
                .data = try readSlice(reader, allocator),
            } },

            // Cluster results
            .cluster_status => .{ .cluster_status = .{
                .node_id = try reader.takeInt(u32, .little),
                .leader_id = try reader.takeInt(u32, .little),
                .term = try reader.takeInt(u64, .little),
                .state = @enumFromInt(try reader.takeByte()),
                .member_count = try reader.takeInt(u32, .little),
            } },
            .cluster_members => .{ .cluster_members = .{
                .data = try readSlice(reader, allocator),
            } },
            .cluster_join_ok => .{ .cluster_join_ok = .{
                .assigned_node_id = try reader.takeInt(u32, .little),
                .leader_id = try reader.takeInt(u32, .little),
            } },

            // Namespace results
            .namespace_created => .{ .namespace_created = {} },
            .namespace_deleted => .{ .namespace_deleted = {} },
            .namespace_list => .{ .namespace_list = .{
                .data = try readSlice(reader, allocator),
                .allocated = true,
            } },
            .namespace_info => blk: {
                const exists = (try reader.takeByte()) != 0;
                const name_len = try reader.takeInt(u16, .little);
                const name = try allocator.alloc(u8, name_len);
                try reader.readSliceAll(name);
                break :blk .{ .namespace_info = .{
                    .exists = exists,
                    .name = name,
                    .allocated = true,
                } };
            },
            .namespace_config_set => .{ .namespace_config_set = {} },
            .namespace_config_get => .{ .namespace_config_get = .{
                .data = try readSlice(reader, allocator),
                .allocated = true,
            } },

            // Processing results
            .processing_submitted => .{ .processing_submitted = .{
                .job_id = try readSlice(reader, allocator),
            } },
            .processing_stopped => .{ .processing_stopped = {} },
            .processing_cancelled => .{ .processing_cancelled = {} },
            .processing_status_result => .{ .processing_status_result = .{
                .data = try readSlice(reader, allocator),
            } },
            .processing_list_result => .{ .processing_list_result = .{
                .data = try readSlice(reader, allocator),
                .cursor = try readOptionalSlice(reader, allocator),
            } },
            .processing_savepoint_result => .{ .processing_savepoint_result = .{
                .savepoint_id = try readSlice(reader, allocator),
            } },
            .processing_restored => .{ .processing_restored = {} },
            .processing_rescaled => .{ .processing_rescaled = {} },

            // Time-series results
            .ts_write_ok => .{ .ts_write_ok = .{
                .series_hash = try reader.takeInt(u64, .little),
                .timestamp_ms = try reader.takeInt(i64, .little),
                .sequence = try reader.takeInt(u64, .little),
            } },
            .ts_read_result => .{ .ts_read_result = .{
                .data = try readSlice(reader, allocator),
            } },
            .ts_query_result => .{ .ts_query_result = .{
                .data = try readSlice(reader, allocator),
            } },
            .ts_list_result => .{ .ts_list_result = .{
                .data = try readSlice(reader, allocator),
            } },
            .ts_retention_result => .{ .ts_retention_result = .{
                .data = try readSlice(reader, allocator),
            } },
            .ts_floql_result => .{ .ts_floql_result = .{
                .data = try readSlice(reader, allocator),
            } },
        };
    }

    fn deserializeStreamMessage(reader: anytype, allocator: Allocator) !StreamMessage {
        const sequence = try reader.takeInt(u64, .little);
        const timestamp_ms = try reader.takeInt(i64, .little);
        const partition = try reader.takeInt(u32, .little);
        const key = try readOptionalSlice(reader, allocator);
        const payload = try readSlice(reader, allocator);
        const header_count = try reader.takeInt(u32, .little);
        var headers: ?[]Header = null;
        if (header_count > 0) {
            const hdrs = try allocator.alloc(Header, header_count);
            for (hdrs) |*h| {
                h.key = try readSlice(reader, allocator);
                h.value = try readSlice(reader, allocator);
            }
            headers = hdrs;
        }
        return .{
            .sequence = sequence,
            .timestamp_ms = timestamp_ms,
            .partition = partition,
            .key = key,
            .payload = payload,
            .headers = headers,
        };
    }

    fn readSlice(reader: anytype, allocator: Allocator) ![]const u8 {
        const len = try reader.takeInt(u32, .little);
        const slice = try allocator.alloc(u8, len);
        try reader.readSliceAll(slice);
        return slice;
    }

    fn readOptionalSlice(reader: anytype, allocator: Allocator) !?[]const u8 {
        const present = try reader.takeByte();
        if (present != 0) {
            return try readSlice(reader, allocator);
        }
        return null;
    }

    fn readOptionalI64(reader: anytype) !?i64 {
        const present = try reader.takeByte();
        if (present != 0) {
            return try reader.takeInt(i64, .little);
        }
        return null;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "CommandResult.ok serialization" {
    const result = CommandResult{ .ok = {} };
    try std.testing.expectEqual(@as(usize, 1), result.serializedSize());

    var buffer: [16]u8 = undefined;
    result.serializeInto(&buffer);

    const deserialized = try CommandResult.deserialize(&buffer, std.testing.allocator);
    try std.testing.expect(deserialized == .ok);
}

test "CommandResult.err serialization" {
    const allocator = std.testing.allocator;

    const result = CommandResult{ .err = .{
        .code = .not_found,
        .message = "key not found",
    } };

    const size = result.serializedSize();
    const buffer = try allocator.alloc(u8, size);
    defer allocator.free(buffer);

    result.serializeInto(buffer);

    const deserialized = try CommandResult.deserialize(buffer, allocator);
    defer allocator.free(deserialized.err.message);

    try std.testing.expectEqual(CommandResult.ErrorCode.not_found, deserialized.err.code);
    try std.testing.expectEqualStrings("key not found", deserialized.err.message);
}

test "CommandResult.kv_value serialization" {
    const allocator = std.testing.allocator;

    const result = CommandResult{ .kv_value = .{
        .value = "test_value",
        .version = 42,
    } };

    const size = result.serializedSize();
    const buffer = try allocator.alloc(u8, size);
    defer allocator.free(buffer);

    result.serializeInto(buffer);

    const deserialized = try CommandResult.deserialize(buffer, allocator);
    defer allocator.free(deserialized.kv_value.value);

    try std.testing.expectEqualStrings("test_value", deserialized.kv_value.value);
    try std.testing.expectEqual(@as(u64, 42), deserialized.kv_value.version);
}

test "CommandResult.opcode mapping" {
    const flo = @import("proto.zig");

    try std.testing.expectEqual(flo.OpCode.ok, (CommandResult{ .ok = {} }).opcode());
    try std.testing.expectEqual(flo.OpCode.pong, (CommandResult{ .pong = {} }).opcode());
    try std.testing.expectEqual(flo.OpCode.error_response, (CommandResult{ .err = .{ .code = .unknown, .message = "" } }).opcode());
    try std.testing.expectEqual(flo.OpCode.kv_get_response, (CommandResult{ .kv_value = .{ .value = "", .version = 0 } }).opcode());
}
