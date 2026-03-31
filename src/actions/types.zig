//! Action Types
//!
//! Core type definitions for Flo Actions (Layer 2).
//!
//! Actions are the **universal unit of work** - callable units of business logic
//! that bridge Layer 1 primitives (Streams, Queues, KV) and Layer 3 applications
//! (Workflows, Failover, Flo-Processing).
//!
//! # Key Types
//!
//! - `ActionMeta`: Action registry metadata (type, config, limits)
//! - `ActionType`: Action execution type
//! - `ActionRun`: Action execution state
//! - `WorkerMeta`: Worker registration metadata
//!
//! # Design Principles
//!
//! 1. **Unified Registry**: All actions share the same registry
//! 2. **Queue-Based Invocation**: All actions invoked via Layer 1 Queues
//! 3. **Layer 0 Storage**: State persisted directly to storage engine

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const WireWriter = @import("../util/wire.zig").WireWriter;
const WireReader = @import("../util/wire.zig").WireReader;

// =============================================================================
// Action Type
// =============================================================================

/// Type of action execution
pub const ActionType = enum(u8) {
    /// User-hosted: Worker runs on user's infrastructure.
    /// User provides: Go/Python/Node function.
    /// Flo provides: Queue-based task delivery, retry, monitoring.
    user = 0,

    pub fn toString(self: ActionType) []const u8 {
        return switch (self) {
            .user => "user",
        };
    }

    pub fn fromU8(v: u8) ActionType {
        return switch (v) {
            0 => .user,
            else => .user, // default to user for unknown values
        };
    }
};

// =============================================================================
// Action Run Status
// =============================================================================

/// Status of an action execution
pub const RunStatus = enum(u8) {
    /// Queued, waiting to be picked up by worker
    pending = 0,
    /// Currently being executed by a worker
    running = 1,
    /// Completed successfully
    completed = 2,
    /// Failed after exhausting retries
    failed = 3,
    /// Cancelled by user
    cancelled = 4,
    /// Timed out
    timed_out = 5,

    pub fn toString(self: RunStatus) []const u8 {
        return switch (self) {
            .pending => "pending",
            .running => "running",
            .completed => "completed",
            .failed => "failed",
            .cancelled => "cancelled",
            .timed_out => "timed_out",
        };
    }
};

// =============================================================================
// Action Outcome (Business Result)
// =============================================================================

/// Business outcome of an action execution.
/// Separate from RunStatus (lifecycle) - an action can be "completed" (worker done)
/// but have outcome "pending" (awaiting external confirmation).
pub const Outcome = enum(u8) {
    /// Action succeeded
    success = 0,
    /// Action failed
    failure = 1,
    /// Action initiated but awaiting external completion (async pattern)
    /// Use action_report_outcome to set final outcome
    pending = 2,
    /// Action timed out
    timeout = 3,
    /// Action was cancelled
    cancelled = 4,

    pub fn toString(self: Outcome) []const u8 {
        return switch (self) {
            .success => "success",
            .failure => "failure",
            .pending => "pending",
            .timeout => "timeout",
            .cancelled => "cancelled",
        };
    }

    pub fn fromString(s: []const u8) ?Outcome {
        if (mem.eql(u8, s, "success")) return .success;
        if (mem.eql(u8, s, "failure")) return .failure;
        if (mem.eql(u8, s, "pending")) return .pending;
        if (mem.eql(u8, s, "timeout")) return .timeout;
        if (mem.eql(u8, s, "cancelled")) return .cancelled;
        return null;
    }
};

// =============================================================================
// Action Metadata (Registry Entry)
// =============================================================================

/// Action metadata stored in registry
/// Key: ns/{namespace}/action/{action_name}
///
/// Wire format (version 1):
/// [version:u8][name_len:u16][name][namespace_len:u16][namespace]
/// [version_str_len:u16][version_str][type:u8][owner_len:u16][owner]
/// [has_description:u8][description_len:u16]?[description]?
/// [timeout_ms:u32][max_retries:u32][retry_delay_ms:u32]
/// [max_input_size:u32][max_output_size:u32]
/// [created_at:i64][updated_at:i64][enabled:u8]
/// [has_trigger_stream:u8][trigger_stream_len:u16]?[trigger_stream]?
/// [has_trigger_group:u8][trigger_group_len:u16]?[trigger_group]?
pub const ActionMeta = struct {
    /// Action name (unique within namespace)
    name: []const u8,
    /// Namespace for isolation
    namespace: []const u8,
    /// Action version (semantic versioning)
    version: []const u8,
    /// Action type (user)
    action_type: ActionType,
    /// Owner identifier (user ID or organization)
    owner: []const u8,
    /// Human-readable description
    description: ?[]const u8 = null,

    // Resource limits
    /// Execution timeout in milliseconds
    timeout_ms: u32 = 30_000,
    /// Maximum retries before moving to DLQ
    max_retries: u32 = 3,
    /// Retry delay base in milliseconds (exponential backoff)
    retry_delay_ms: u32 = 1000,
    /// Maximum input payload size in bytes
    max_input_size: u32 = 1024 * 1024, // 1MB
    /// Maximum output payload size in bytes
    max_output_size: u32 = 1024 * 1024, // 1MB

    // Stream-trigger configuration (for Flo-Processing)
    /// Source stream that triggers this action
    trigger_stream: ?[]const u8 = null,
    /// Consumer group for the trigger (for partitioned processing)
    trigger_group: ?[]const u8 = null,

    // Queue-trigger configuration — auto-invoke from a user queue
    /// User queue to poll; each dequeued message becomes `input` for an invocation
    trigger_queue: ?[]const u8 = null,

    // Metadata
    /// Creation timestamp (Unix ms)
    created_at: i64,
    /// Last update timestamp (Unix ms)
    updated_at: i64,
    /// Whether action is enabled
    enabled: bool = true,

    const ENCODING_VERSION: u8 = 1;

    /// Encode action metadata to bytes using WireWriter
    pub fn encode(self: ActionMeta, allocator: Allocator) ![]u8 {
        var writer = WireWriter.init(allocator);
        errdefer writer.deinit();

        // Version byte
        try writer.writeU8(ENCODING_VERSION);

        // Core fields
        try writer.writeLengthPrefixed(u16, self.name);
        try writer.writeLengthPrefixed(u16, self.namespace);
        try writer.writeLengthPrefixed(u16, self.version);
        try writer.writeU8(@intFromEnum(self.action_type));
        try writer.writeLengthPrefixed(u16, self.owner);

        // Optional description
        if (self.description) |desc| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u16, desc);
        } else {
            try writer.writeU8(0);
        }

        // Resource limits
        try writer.writeU32(self.timeout_ms);
        try writer.writeU32(self.max_retries);
        try writer.writeU32(self.retry_delay_ms);
        try writer.writeU32(self.max_input_size);
        try writer.writeU32(self.max_output_size);

        // Timestamps
        try writer.writeI64(self.created_at);
        try writer.writeI64(self.updated_at);
        try writer.writeU8(if (self.enabled) 1 else 0);

        // WASM configuration (removed — write zero markers for wire compat)
        try writer.writeU8(0); // has_wasm_module
        try writer.writeU8(0); // has_wasm_entrypoint
        try writer.writeU8(0); // has_wasm_memory_limit

        // Stream-trigger configuration
        if (self.trigger_stream) |s| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u16, s);
        } else {
            try writer.writeU8(0);
        }
        if (self.trigger_group) |g| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u16, g);
        } else {
            try writer.writeU8(0);
        }

        // Queue-trigger configuration (appended for backward compat)
        if (self.trigger_queue) |q| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u16, q);
        } else {
            try writer.writeU8(0);
        }

        return writer.toOwnedSlice();
    }

    /// Decode action metadata from bytes using WireReader
    pub fn decode(allocator: Allocator, data: []const u8) !ActionMeta {
        var reader = WireReader.init(data);

        // Version check
        const version = reader.readU8() orelse return error.InvalidData;
        if (version > ENCODING_VERSION) return error.UnsupportedVersion;

        // Core fields
        const name = reader.readLengthPrefixed(u16) orelse return error.InvalidData;
        const namespace = reader.readLengthPrefixed(u16) orelse return error.InvalidData;
        const ver = reader.readLengthPrefixed(u16) orelse return error.InvalidData;
        const action_type: ActionType = @enumFromInt(reader.readU8() orelse return error.InvalidData);
        const owner = reader.readLengthPrefixed(u16) orelse return error.InvalidData;

        // Optional description
        const has_desc = reader.readU8() orelse return error.InvalidData;
        const description: ?[]const u8 = if (has_desc == 1)
            reader.readLengthPrefixed(u16) orelse return error.InvalidData
        else
            null;

        // Resource limits
        const timeout_ms = reader.readU32() orelse return error.InvalidData;
        const max_retries = reader.readU32() orelse return error.InvalidData;
        const retry_delay_ms = reader.readU32() orelse return error.InvalidData;
        const max_input_size = reader.readU32() orelse return error.InvalidData;
        const max_output_size = reader.readU32() orelse return error.InvalidData;

        // Timestamps
        const created_at = reader.readI64() orelse return error.InvalidData;
        const updated_at = reader.readI64() orelse return error.InvalidData;
        const enabled = (reader.readU8() orelse return error.InvalidData) == 1;

        // WASM configuration (removed — skip for wire compat)
        const has_wasm_module = reader.readU8() orelse return error.InvalidData;
        if (has_wasm_module == 1) {
            _ = reader.readLengthPrefixed(u16) orelse return error.InvalidData;
        }

        const has_wasm_entrypoint = reader.readU8() orelse return error.InvalidData;
        if (has_wasm_entrypoint == 1) {
            _ = reader.readLengthPrefixed(u16) orelse return error.InvalidData;
        }

        const has_wasm_memory_limit = reader.readU8() orelse return error.InvalidData;
        if (has_wasm_memory_limit == 1) {
            _ = reader.readU32() orelse return error.InvalidData;
        }

        // Stream-trigger configuration
        const has_trigger_stream = reader.readU8() orelse return error.InvalidData;
        const trigger_stream: ?[]const u8 = if (has_trigger_stream == 1)
            reader.readLengthPrefixed(u16) orelse return error.InvalidData
        else
            null;

        const has_trigger_group = reader.readU8() orelse return error.InvalidData;
        const trigger_group: ?[]const u8 = if (has_trigger_group == 1)
            reader.readLengthPrefixed(u16) orelse return error.InvalidData
        else
            null;

        // Queue-trigger configuration (may be absent in older data)
        const trigger_queue: ?[]const u8 = if (reader.readU8()) |has_tq| blk: {
            break :blk if (has_tq == 1)
                reader.readLengthPrefixed(u16) orelse return error.InvalidData
            else
                null;
        } else null;

        // Allocate copies for ownership
        const owned_name = try allocator.dupe(u8, name);
        errdefer allocator.free(owned_name);

        const owned_namespace = try allocator.dupe(u8, namespace);
        errdefer allocator.free(owned_namespace);

        const owned_ver = try allocator.dupe(u8, ver);
        errdefer allocator.free(owned_ver);

        const owned_owner = try allocator.dupe(u8, owner);
        errdefer allocator.free(owned_owner);

        const owned_desc: ?[]const u8 = if (description) |d|
            try allocator.dupe(u8, d)
        else
            null;

        const owned_trigger_stream: ?[]const u8 = if (trigger_stream) |s|
            try allocator.dupe(u8, s)
        else
            null;

        const owned_trigger_group: ?[]const u8 = if (trigger_group) |g|
            try allocator.dupe(u8, g)
        else
            null;

        const owned_trigger_queue: ?[]const u8 = if (trigger_queue) |q|
            try allocator.dupe(u8, q)
        else
            null;

        return ActionMeta{
            .name = owned_name,
            .namespace = owned_namespace,
            .version = owned_ver,
            .action_type = action_type,
            .owner = owned_owner,
            .description = owned_desc,
            .timeout_ms = timeout_ms,
            .max_retries = max_retries,
            .retry_delay_ms = retry_delay_ms,
            .max_input_size = max_input_size,
            .max_output_size = max_output_size,
            .trigger_stream = owned_trigger_stream,
            .trigger_group = owned_trigger_group,
            .trigger_queue = owned_trigger_queue,
            .created_at = created_at,
            .updated_at = updated_at,
            .enabled = enabled,
        };
    }

    /// Free allocated memory
    pub fn deinit(self: *ActionMeta, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.namespace);
        allocator.free(self.version);
        allocator.free(self.owner);
        if (self.description) |d| allocator.free(d);
        if (self.trigger_stream) |s| allocator.free(s);
        if (self.trigger_group) |g| allocator.free(g);
        if (self.trigger_queue) |q| allocator.free(q);
    }
};

// =============================================================================
// Action Run State
// =============================================================================

/// Action execution state
/// Key: ns/{namespace}/action_run/{run_id}
///
/// Wire format (version 1):
/// [version:u8][run_id_len:u16][run_id][action_name_len:u16][action_name]
/// [namespace_len:u16][namespace][status:u8][input_len:u32][input]
/// [has_output:u8][output_len:u32]?[output]?
/// [has_error:u8][error_len:u16]?[error]?
/// [has_worker:u8][worker_len:u16]?[worker]?
/// [attempt:u32][has_queue_seq:u8][queue_seq:u64]?
/// [created_at:i64][has_started:u8][started_at:i64]?[has_completed:u8][completed_at:i64]?
/// [has_caller:u8][caller_len:u16]?[caller]?[has_callback:u8][callback_len:u16]?[callback]?
/// [outcome:u8][has_correlation_id:u8][correlation_id_len:u16]?[correlation_id]?
pub const ActionRun = struct {
    /// Unique run identifier
    run_id: []const u8,
    /// Action being executed
    action_name: []const u8,
    /// Namespace
    namespace: []const u8,
    /// Current lifecycle status (is worker done processing?)
    status: RunStatus,
    /// Input payload
    input: []const u8,
    /// Output payload (set on completion)
    output: ?[]const u8 = null,
    /// Error message (set on failure)
    error_message: ?[]const u8 = null,

    /// Worker ID processing this run (if running)
    worker_id: ?[]const u8 = null,
    /// Current attempt number (1-based)
    attempt: u32 = 1,

    /// Queue sequence number for dequeue tracking
    queue_seq: ?u64 = null,

    // Timestamps
    created_at: i64,
    started_at: ?i64 = null,
    completed_at: ?i64 = null,

    // Caller context (for async result retrieval)
    caller_id: ?[]const u8 = null,
    callback_queue: ?[]const u8 = null,

    /// Business outcome (success/failure/pending)
    /// Separate from status - action can be completed but outcome pending (async)
    outcome: Outcome = .pending,
    /// Correlation ID for async action completion via action_report_outcome
    correlation_id: ?[]const u8 = null,
    /// Label selector (JSON). Only workers with matching labels can claim this task.
    required_labels: ?[]const u8 = null,

    const ENCODING_VERSION: u8 = 1;

    /// Encode run state to bytes
    pub fn encode(self: ActionRun, allocator: Allocator) ![]u8 {
        var writer = WireWriter.init(allocator);
        errdefer writer.deinit();

        // Version byte
        try writer.writeU8(ENCODING_VERSION);

        // Core fields
        try writer.writeLengthPrefixed(u16, self.run_id);
        try writer.writeLengthPrefixed(u16, self.action_name);
        try writer.writeLengthPrefixed(u16, self.namespace);
        try writer.writeU8(@intFromEnum(self.status));
        try writer.writeLengthPrefixed(u32, self.input);

        // Optional output
        if (self.output) |out| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u32, out);
        } else {
            try writer.writeU8(0);
        }

        // Optional error
        if (self.error_message) |err| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u16, err);
        } else {
            try writer.writeU8(0);
        }

        // Optional worker
        if (self.worker_id) |w| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u16, w);
        } else {
            try writer.writeU8(0);
        }

        // Attempt and queue_seq
        try writer.writeU32(self.attempt);
        if (self.queue_seq) |seq| {
            try writer.writeU8(1);
            try writer.writeU64(seq);
        } else {
            try writer.writeU8(0);
        }

        // Timestamps
        try writer.writeI64(self.created_at);
        if (self.started_at) |t| {
            try writer.writeU8(1);
            try writer.writeI64(t);
        } else {
            try writer.writeU8(0);
        }
        if (self.completed_at) |t| {
            try writer.writeU8(1);
            try writer.writeI64(t);
        } else {
            try writer.writeU8(0);
        }

        // Caller context
        if (self.caller_id) |c| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u16, c);
        } else {
            try writer.writeU8(0);
        }
        if (self.callback_queue) |q| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u16, q);
        } else {
            try writer.writeU8(0);
        }

        // Outcome and correlation_id (version 4+)
        try writer.writeU8(@intFromEnum(self.outcome));
        if (self.correlation_id) |cid| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u16, cid);
        } else {
            try writer.writeU8(0);
        }

        // Required labels (optional)
        if (self.required_labels) |labels| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u16, labels);
        } else {
            try writer.writeU8(0);
        }

        return writer.toOwnedSlice();
    }

    /// Decode run state from bytes
    pub fn decode(allocator: Allocator, data: []const u8) !ActionRun {
        var reader = WireReader.init(data);

        const version = reader.readU8() orelse return error.InvalidData;
        if (version != ENCODING_VERSION) return error.UnsupportedVersion;

        const run_id = reader.readLengthPrefixed(u16) orelse return error.InvalidData;
        const action_name = reader.readLengthPrefixed(u16) orelse return error.InvalidData;
        const namespace = reader.readLengthPrefixed(u16) orelse return error.InvalidData;
        const status: RunStatus = @enumFromInt(reader.readU8() orelse return error.InvalidData);
        const input = reader.readLengthPrefixed(u32) orelse return error.InvalidData;

        // Optional output
        const has_output = reader.readU8() orelse return error.InvalidData;
        const output: ?[]const u8 = if (has_output == 1)
            reader.readLengthPrefixed(u32) orelse return error.InvalidData
        else
            null;

        // Optional error
        const has_error = reader.readU8() orelse return error.InvalidData;
        const error_message: ?[]const u8 = if (has_error == 1)
            reader.readLengthPrefixed(u16) orelse return error.InvalidData
        else
            null;

        // Optional worker
        const has_worker = reader.readU8() orelse return error.InvalidData;
        const worker_id: ?[]const u8 = if (has_worker == 1)
            reader.readLengthPrefixed(u16) orelse return error.InvalidData
        else
            null;

        // Attempt and queue_seq
        const attempt = reader.readU32() orelse return error.InvalidData;
        const has_queue_seq = reader.readU8() orelse return error.InvalidData;
        const queue_seq: ?u64 = if (has_queue_seq == 1)
            reader.readU64() orelse return error.InvalidData
        else
            null;

        // Timestamps
        const created_at = reader.readI64() orelse return error.InvalidData;
        const has_started = reader.readU8() orelse return error.InvalidData;
        const started_at: ?i64 = if (has_started == 1)
            reader.readI64() orelse return error.InvalidData
        else
            null;
        const has_completed = reader.readU8() orelse return error.InvalidData;
        const completed_at: ?i64 = if (has_completed == 1)
            reader.readI64() orelse return error.InvalidData
        else
            null;

        // Caller context
        const has_caller = reader.readU8() orelse return error.InvalidData;
        const caller_id: ?[]const u8 = if (has_caller == 1)
            reader.readLengthPrefixed(u16) orelse return error.InvalidData
        else
            null;
        const has_callback = reader.readU8() orelse return error.InvalidData;
        const callback_queue: ?[]const u8 = if (has_callback == 1)
            reader.readLengthPrefixed(u16) orelse return error.InvalidData
        else
            null;

        // Outcome and correlation_id
        const outcome: Outcome = @enumFromInt(reader.readU8() orelse return error.InvalidData);
        const has_correlation = reader.readU8() orelse return error.InvalidData;
        const correlation_id: ?[]const u8 = if (has_correlation == 1)
            reader.readLengthPrefixed(u16) orelse return error.InvalidData
        else
            null;

        // Optional required_labels (may not be present in older data)
        const has_required_labels = reader.readU8() orelse 0;
        const required_labels: ?[]const u8 = if (has_required_labels == 1)
            reader.readLengthPrefixed(u16) orelse null
        else
            null;

        // Allocate copies for ownership
        return ActionRun{
            .run_id = try allocator.dupe(u8, run_id),
            .action_name = try allocator.dupe(u8, action_name),
            .namespace = try allocator.dupe(u8, namespace),
            .status = status,
            .input = try allocator.dupe(u8, input),
            .output = if (output) |o| try allocator.dupe(u8, o) else null,
            .error_message = if (error_message) |e| try allocator.dupe(u8, e) else null,
            .worker_id = if (worker_id) |w| try allocator.dupe(u8, w) else null,
            .attempt = attempt,
            .queue_seq = queue_seq,
            .created_at = created_at,
            .started_at = started_at,
            .completed_at = completed_at,
            .caller_id = if (caller_id) |c| try allocator.dupe(u8, c) else null,
            .callback_queue = if (callback_queue) |q| try allocator.dupe(u8, q) else null,
            .outcome = outcome,
            .correlation_id = if (correlation_id) |cid| try allocator.dupe(u8, cid) else null,
            .required_labels = if (required_labels) |l| try allocator.dupe(u8, l) else null,
        };
    }

    /// Free allocated memory
    pub fn deinit(self: *ActionRun, allocator: Allocator) void {
        allocator.free(self.run_id);
        allocator.free(self.action_name);
        allocator.free(self.namespace);
        allocator.free(self.input);
        if (self.output) |o| allocator.free(o);
        if (self.error_message) |e| allocator.free(e);
        if (self.worker_id) |w| allocator.free(w);
        if (self.caller_id) |c| allocator.free(c);
        if (self.callback_queue) |q| allocator.free(q);
        if (self.correlation_id) |cid| allocator.free(cid);
        if (self.required_labels) |l| allocator.free(l);
    }
};

// =============================================================================
// Worker Metadata
// =============================================================================

/// Worker metadata stored in registry
/// Key: ns/{namespace}/worker/{worker_id}
pub const WorkerMeta = struct {
    /// Unique worker identifier
    worker_id: []const u8,
    /// Namespace
    namespace: []const u8,
    /// Task types this worker can handle (comma-separated in encoded form)
    task_types: []const []const u8,
    /// Worker labels (JSON string for extensibility, e.g. {"gpu":true})
    labels: ?[]const u8 = null,
    /// Current load (0-100 percentage)
    current_load: u8 = 0,
    /// Maximum concurrent tasks
    max_concurrent: u32 = 10,
    /// Current active task count
    active_tasks: u32 = 0,

    // Connection info
    /// Connection file descriptor (for push notifications)
    connection_fd: ?i32 = null,

    // Health tracking
    /// Last seen timestamp (Unix ms) - updated on any worker interaction
    last_seen: i64,
    /// Registration timestamp (Unix ms)
    registered_at: i64,
    /// Whether worker is considered healthy
    healthy: bool = true,

    const ENCODING_VERSION: u8 = 1;

    /// Encode worker metadata to bytes
    pub fn encode(self: WorkerMeta, allocator: Allocator) ![]u8 {
        var writer = WireWriter.init(allocator);
        errdefer writer.deinit();

        // Version byte
        try writer.writeU8(ENCODING_VERSION);

        // Core fields
        try writer.writeLengthPrefixed(u16, self.worker_id);
        try writer.writeLengthPrefixed(u16, self.namespace);

        // Task types array
        try writer.writeU32(@intCast(self.task_types.len));
        for (self.task_types) |tt| {
            try writer.writeLengthPrefixed(u16, tt);
        }

        // Optional labels
        if (self.labels) |cap| {
            try writer.writeU8(1);
            try writer.writeLengthPrefixed(u16, cap);
        } else {
            try writer.writeU8(0);
        }

        // Load info
        try writer.writeU8(self.current_load);
        try writer.writeU32(self.max_concurrent);
        try writer.writeU32(self.active_tasks);

        // Connection info
        if (self.connection_fd) |fd| {
            try writer.writeU8(1);
            // Write i32 as bytes (WireWriter doesn't have writeI32)
            var buf: [4]u8 = undefined;
            std.mem.writeInt(i32, &buf, fd, .little);
            try writer.writeSlice(&buf);
        } else {
            try writer.writeU8(0);
        }

        // Health tracking
        try writer.writeI64(self.last_seen);
        try writer.writeI64(self.registered_at);
        try writer.writeU8(if (self.healthy) 1 else 0);

        return writer.toOwnedSlice();
    }

    /// Decode worker metadata from bytes
    pub fn decode(allocator: Allocator, data: []const u8) !WorkerMeta {
        var reader = WireReader.init(data);

        const version = reader.readU8() orelse return error.InvalidData;
        if (version != ENCODING_VERSION) return error.UnsupportedVersion;

        const worker_id = reader.readLengthPrefixed(u16) orelse return error.InvalidData;
        const namespace = reader.readLengthPrefixed(u16) orelse return error.InvalidData;

        // Task types array
        const task_types_len = reader.readU32() orelse return error.InvalidData;
        var task_types = try allocator.alloc([]const u8, task_types_len);
        errdefer {
            for (task_types) |tt| allocator.free(tt);
            allocator.free(task_types);
        }

        for (0..task_types_len) |i| {
            const tt = reader.readLengthPrefixed(u16) orelse return error.InvalidData;
            task_types[i] = try allocator.dupe(u8, tt);
        }

        // Optional labels
        const has_cap = reader.readU8() orelse return error.InvalidData;
        const labels: ?[]const u8 = if (has_cap == 1)
            reader.readLengthPrefixed(u16) orelse return error.InvalidData
        else
            null;

        // Load info
        const current_load = reader.readU8() orelse return error.InvalidData;
        const max_concurrent = reader.readU32() orelse return error.InvalidData;
        const active_tasks = reader.readU32() orelse return error.InvalidData;

        // Connection info
        const has_conn = reader.readU8() orelse return error.InvalidData;
        const connection_fd: ?i32 = if (has_conn == 1) blk: {
            const bytes = reader.readBytes(4) orelse return error.InvalidData;
            break :blk std.mem.readInt(i32, bytes, .little);
        } else null;

        // Health tracking
        const last_seen = reader.readI64() orelse return error.InvalidData;
        const registered_at = reader.readI64() orelse return error.InvalidData;
        const healthy = (reader.readU8() orelse return error.InvalidData) == 1;

        return WorkerMeta{
            .worker_id = try allocator.dupe(u8, worker_id),
            .namespace = try allocator.dupe(u8, namespace),
            .task_types = task_types,
            .labels = if (labels) |c| try allocator.dupe(u8, c) else null,
            .current_load = current_load,
            .max_concurrent = max_concurrent,
            .active_tasks = active_tasks,
            .connection_fd = connection_fd,
            .last_seen = last_seen,
            .registered_at = registered_at,
            .healthy = healthy,
        };
    }

    /// Free allocated memory
    pub fn deinit(self: *WorkerMeta, allocator: Allocator) void {
        allocator.free(self.worker_id);
        allocator.free(self.namespace);
        for (self.task_types) |tt| {
            allocator.free(tt);
        }
        allocator.free(self.task_types);
        if (self.labels) |c| allocator.free(c);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "ActionMeta encode/decode roundtrip" {
    const allocator = std.testing.allocator;

    var meta = ActionMeta{
        .name = "send-email",
        .namespace = "prod",
        .version = "1.0.0",
        .action_type = .user,
        .owner = "user@example.com",
        .description = "Sends email notifications",
        .timeout_ms = 60000,
        .max_retries = 5,
        .created_at = 1699999999000,
        .updated_at = 1699999999000,
    };

    const encoded = try meta.encode(allocator);
    defer allocator.free(encoded);

    var decoded = try ActionMeta.decode(allocator, encoded);
    defer decoded.deinit(allocator);

    try std.testing.expectEqualStrings("send-email", decoded.name);
    try std.testing.expectEqualStrings("prod", decoded.namespace);
    try std.testing.expectEqual(ActionType.user, decoded.action_type);
    try std.testing.expectEqual(@as(u32, 60000), decoded.timeout_ms);
}

test "ActionMeta encode/decode roundtrip with trigger_queue" {
    const allocator = std.testing.allocator;

    var meta = ActionMeta{
        .name = "process-payment",
        .namespace = "finance",
        .version = "2.0.0",
        .action_type = .user,
        .owner = "team-payments",
        .trigger_stream = "txn-events",
        .trigger_group = "payment-cg",
        .trigger_queue = "pending-payments",
        .created_at = 1700000000000,
        .updated_at = 1700000000000,
    };

    const encoded = try meta.encode(allocator);
    defer allocator.free(encoded);

    var decoded = try ActionMeta.decode(allocator, encoded);
    defer decoded.deinit(allocator);

    try std.testing.expectEqualStrings("process-payment", decoded.name);
    try std.testing.expectEqualStrings("finance", decoded.namespace);
    try std.testing.expectEqual(ActionType.user, decoded.action_type);
    try std.testing.expectEqualStrings("txn-events", decoded.trigger_stream.?);
    try std.testing.expectEqualStrings("payment-cg", decoded.trigger_group.?);
    try std.testing.expectEqualStrings("pending-payments", decoded.trigger_queue.?);
}

test "ActionMeta decode backward compat (no trigger_queue)" {
    // Simulate v1 data without trigger_queue appended
    const allocator = std.testing.allocator;

    var meta_v1 = ActionMeta{
        .name = "old-action",
        .namespace = "default",
        .version = "1.0.0",
        .action_type = .user,
        .owner = "legacy",
        .created_at = 1699000000000,
        .updated_at = 1699000000000,
    };

    // Encode with current code (includes trigger_queue=null → writeU8(0))
    const encoded = try meta_v1.encode(allocator);
    defer allocator.free(encoded);

    var decoded = try ActionMeta.decode(allocator, encoded);
    defer decoded.deinit(allocator);

    try std.testing.expect(decoded.trigger_queue == null);
    try std.testing.expect(decoded.trigger_stream == null);
    try std.testing.expectEqualStrings("old-action", decoded.name);
}

test "ActionRun encode/decode roundtrip" {
    const allocator = std.testing.allocator;

    var run = ActionRun{
        .run_id = "run-abc123",
        .action_name = "send-email",
        .namespace = "prod",
        .status = .running,
        .input = "{\"to\":\"test@example.com\"}",
        .worker_id = "worker-1",
        .attempt = 2,
        .queue_seq = 12345,
        .created_at = 1699999999000,
        .started_at = 1699999999100,
    };

    const encoded = try run.encode(allocator);
    defer allocator.free(encoded);

    var decoded = try ActionRun.decode(allocator, encoded);
    defer decoded.deinit(allocator);

    try std.testing.expectEqualStrings("run-abc123", decoded.run_id);
    try std.testing.expectEqualStrings("send-email", decoded.action_name);
    try std.testing.expectEqual(RunStatus.running, decoded.status);
    try std.testing.expectEqual(@as(u32, 2), decoded.attempt);
    try std.testing.expectEqual(@as(u64, 12345), decoded.queue_seq.?);
}

test "WorkerMeta encode/decode roundtrip" {
    const allocator = std.testing.allocator;

    const task_types = try allocator.alloc([]const u8, 2);
    defer allocator.free(task_types);
    task_types[0] = "send-email";
    task_types[1] = "process-image";

    var worker = WorkerMeta{
        .worker_id = "worker-1",
        .namespace = "prod",
        .task_types = task_types,
        .labels = "{\"gpu\":true}",
        .current_load = 50,
        .max_concurrent = 20,
        .active_tasks = 10,
        .connection_fd = 42,
        .last_seen = 1699999999000,
        .registered_at = 1699999990000,
    };

    const encoded = try worker.encode(allocator);
    defer allocator.free(encoded);

    var decoded = try WorkerMeta.decode(allocator, encoded);
    defer decoded.deinit(allocator);

    try std.testing.expectEqualStrings("worker-1", decoded.worker_id);
    try std.testing.expectEqual(@as(usize, 2), decoded.task_types.len);
    try std.testing.expectEqualStrings("send-email", decoded.task_types[0]);
    try std.testing.expectEqual(@as(u8, 50), decoded.current_load);
    try std.testing.expectEqual(@as(i32, 42), decoded.connection_fd.?);
}
