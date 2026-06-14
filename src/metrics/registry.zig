const std = @import("std");
const Allocator = std.mem.Allocator;
const Atomic = std.atomic.Value;
const StreamMetrics = @import("../stream/metrics.zig").Metrics;

// =============================================================================
// Queue Metrics — imported from src/queue/metrics.zig (rich per-queue metrics)
// =============================================================================
pub const QueueMetrics = @import("../queue/metrics.zig").Metrics;

// =============================================================================
// KV Metrics
// =============================================================================

/// KV namespace-level metrics for observability
pub const KVMetrics = struct {
    /// Total keys (approximate, updated on set/delete)
    key_count: Atomic(u64) = Atomic(u64).init(0),
    /// Total GET operations
    get_ops_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total SET operations
    set_ops_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total DELETE operations
    delete_ops_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total bytes stored (approximate)
    bytes_stored: Atomic(u64) = Atomic(u64).init(0),

    pub fn init() KVMetrics {
        return .{};
    }

    pub fn recordGet(self: *KVMetrics) void {
        _ = self.get_ops_total.fetchAdd(1, .monotonic);
    }

    pub fn recordSet(self: *KVMetrics, bytes: u64, is_new: bool) void {
        _ = self.set_ops_total.fetchAdd(1, .monotonic);
        _ = self.bytes_stored.fetchAdd(bytes, .monotonic);
        if (is_new) {
            _ = self.key_count.fetchAdd(1, .monotonic);
        }
    }

    pub fn recordDelete(self: *KVMetrics) void {
        _ = self.delete_ops_total.fetchAdd(1, .monotonic);
        // key_count decremented if key existed (caller should check)
    }

    pub fn decrementKeyCount(self: *KVMetrics) void {
        _ = self.key_count.fetchSub(1, .monotonic);
    }

    pub const Snapshot = struct {
        key_count: u64,
        get_ops_total: u64,
        set_ops_total: u64,
        delete_ops_total: u64,
        bytes_stored: u64,
    };

    pub fn snapshot(self: *const KVMetrics) Snapshot {
        return .{
            .key_count = self.key_count.load(.monotonic),
            .get_ops_total = self.get_ops_total.load(.monotonic),
            .set_ops_total = self.set_ops_total.load(.monotonic),
            .delete_ops_total = self.delete_ops_total.load(.monotonic),
            .bytes_stored = self.bytes_stored.load(.monotonic),
        };
    }
};

/// Tiered log metrics for observability
/// Tracks read hit/miss statistics per storage tier (hot/warm/cold)
/// Thread-safe: Uses atomic operations for all counters
pub const TieredLogMetrics = struct {
    /// Reads served from hot tier (RAM)
    hot_hits: Atomic(u64) = Atomic(u64).init(0),
    /// Reads served from warm tier (local disk)
    warm_hits: Atomic(u64) = Atomic(u64).init(0),
    /// Reads served from cold tier (S3/Azure Blob)
    cold_hits: Atomic(u64) = Atomic(u64).init(0),
    /// Reads that found no entry
    misses: Atomic(u64) = Atomic(u64).init(0),
    /// Total read operations
    total_reads: Atomic(u64) = Atomic(u64).init(0),

    pub fn init() TieredLogMetrics {
        return .{};
    }

    pub fn recordHotHit(self: *TieredLogMetrics) void {
        _ = self.hot_hits.fetchAdd(1, .monotonic);
        _ = self.total_reads.fetchAdd(1, .monotonic);
    }

    pub fn recordWarmHit(self: *TieredLogMetrics) void {
        _ = self.warm_hits.fetchAdd(1, .monotonic);
        _ = self.total_reads.fetchAdd(1, .monotonic);
    }

    pub fn recordColdHit(self: *TieredLogMetrics) void {
        _ = self.cold_hits.fetchAdd(1, .monotonic);
        _ = self.total_reads.fetchAdd(1, .monotonic);
    }

    pub fn recordMiss(self: *TieredLogMetrics) void {
        _ = self.misses.fetchAdd(1, .monotonic);
        _ = self.total_reads.fetchAdd(1, .monotonic);
    }

    pub const Snapshot = struct {
        hot_hits: u64,
        warm_hits: u64,
        cold_hits: u64,
        misses: u64,
        total_reads: u64,

        /// Calculate hot tier hit rate (0.0 - 1.0)
        pub fn hotHitRate(self: Snapshot) f64 {
            if (self.total_reads == 0) return 0.0;
            return @as(f64, @floatFromInt(self.hot_hits)) / @as(f64, @floatFromInt(self.total_reads));
        }

        /// Calculate warm tier hit rate (0.0 - 1.0)
        pub fn warmHitRate(self: Snapshot) f64 {
            if (self.total_reads == 0) return 0.0;
            return @as(f64, @floatFromInt(self.warm_hits)) / @as(f64, @floatFromInt(self.total_reads));
        }

        /// Calculate cold tier hit rate (0.0 - 1.0)
        pub fn coldHitRate(self: Snapshot) f64 {
            if (self.total_reads == 0) return 0.0;
            return @as(f64, @floatFromInt(self.cold_hits)) / @as(f64, @floatFromInt(self.total_reads));
        }

        /// Calculate miss rate (0.0 - 1.0)
        pub fn missRate(self: Snapshot) f64 {
            if (self.total_reads == 0) return 0.0;
            return @as(f64, @floatFromInt(self.misses)) / @as(f64, @floatFromInt(self.total_reads));
        }
    };

    pub fn snapshot(self: *const TieredLogMetrics) Snapshot {
        return .{
            .hot_hits = self.hot_hits.load(.monotonic),
            .warm_hits = self.warm_hits.load(.monotonic),
            .cold_hits = self.cold_hits.load(.monotonic),
            .misses = self.misses.load(.monotonic),
            .total_reads = self.total_reads.load(.monotonic),
        };
    }

    /// Reset all counters (useful for benchmarking windows)
    pub fn reset(self: *TieredLogMetrics) void {
        self.hot_hits.store(0, .monotonic);
        self.warm_hits.store(0, .monotonic);
        self.cold_hits.store(0, .monotonic);
        self.misses.store(0, .monotonic);
        self.total_reads.store(0, .monotonic);
    }
};

// =============================================================================
// Workflow Metrics
// =============================================================================

/// Workflow engine metrics for observability
pub const WorkflowMetrics = struct {
    /// Current active workflow runs
    active_runs: Atomic(u64) = Atomic(u64).init(0),
    /// Total workflows started
    started_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total workflows completed successfully
    completed_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total workflows failed
    failed_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total workflows cancelled
    cancelled_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total workflows timed out
    timed_out_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total signals delivered
    signals_delivered_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total timers fired
    timers_fired_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total steps executed
    steps_executed_total: Atomic(u64) = Atomic(u64).init(0),
    /// Active scheduled workflows (cron/interval)
    active_schedules: Atomic(u64) = Atomic(u64).init(0),

    pub fn init() WorkflowMetrics {
        return .{};
    }

    pub fn recordStarted(self: *WorkflowMetrics) void {
        _ = self.started_total.fetchAdd(1, .monotonic);
        _ = self.active_runs.fetchAdd(1, .monotonic);
    }

    pub fn recordCompleted(self: *WorkflowMetrics) void {
        _ = self.completed_total.fetchAdd(1, .monotonic);
        const active = self.active_runs.load(.monotonic);
        if (active > 0) _ = self.active_runs.fetchSub(1, .monotonic);
    }

    pub fn recordFailed(self: *WorkflowMetrics) void {
        _ = self.failed_total.fetchAdd(1, .monotonic);
        const active = self.active_runs.load(.monotonic);
        if (active > 0) _ = self.active_runs.fetchSub(1, .monotonic);
    }

    pub fn recordCancelled(self: *WorkflowMetrics) void {
        _ = self.cancelled_total.fetchAdd(1, .monotonic);
        const active = self.active_runs.load(.monotonic);
        if (active > 0) _ = self.active_runs.fetchSub(1, .monotonic);
    }

    pub fn recordTimedOut(self: *WorkflowMetrics) void {
        _ = self.timed_out_total.fetchAdd(1, .monotonic);
        const active = self.active_runs.load(.monotonic);
        if (active > 0) _ = self.active_runs.fetchSub(1, .monotonic);
    }

    pub fn recordSignalDelivered(self: *WorkflowMetrics) void {
        _ = self.signals_delivered_total.fetchAdd(1, .monotonic);
    }

    pub fn recordTimerFired(self: *WorkflowMetrics) void {
        _ = self.timers_fired_total.fetchAdd(1, .monotonic);
    }

    pub fn recordStepExecuted(self: *WorkflowMetrics) void {
        _ = self.steps_executed_total.fetchAdd(1, .monotonic);
    }

    pub fn scheduleAdded(self: *WorkflowMetrics) void {
        _ = self.active_schedules.fetchAdd(1, .monotonic);
    }

    pub fn scheduleRemoved(self: *WorkflowMetrics) void {
        const active = self.active_schedules.load(.monotonic);
        if (active > 0) _ = self.active_schedules.fetchSub(1, .monotonic);
    }

    pub const Snapshot = struct {
        active_runs: u64,
        started_total: u64,
        completed_total: u64,
        failed_total: u64,
        cancelled_total: u64,
        timed_out_total: u64,
        signals_delivered_total: u64,
        timers_fired_total: u64,
        steps_executed_total: u64,
        active_schedules: u64,
    };

    pub fn snapshot(self: *const WorkflowMetrics) Snapshot {
        return .{
            .active_runs = self.active_runs.load(.monotonic),
            .started_total = self.started_total.load(.monotonic),
            .completed_total = self.completed_total.load(.monotonic),
            .failed_total = self.failed_total.load(.monotonic),
            .cancelled_total = self.cancelled_total.load(.monotonic),
            .timed_out_total = self.timed_out_total.load(.monotonic),
            .signals_delivered_total = self.signals_delivered_total.load(.monotonic),
            .timers_fired_total = self.timers_fired_total.load(.monotonic),
            .steps_executed_total = self.steps_executed_total.load(.monotonic),
            .active_schedules = self.active_schedules.load(.monotonic),
        };
    }
};

// =============================================================================
// Processing Metrics
// =============================================================================

/// Processing engine metrics for observability (singleton — one per node)
/// Follows the same pattern as WorkflowMetrics: aggregate counters for all jobs.
pub const ProcessingMetrics = struct {
    /// Current active (running) jobs
    active_jobs: Atomic(u64) = Atomic(u64).init(0),
    /// Total jobs submitted
    jobs_submitted_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total jobs completed (graceful stop)
    jobs_completed_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total jobs failed
    jobs_failed_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total jobs cancelled
    jobs_cancelled_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total records processed across all jobs
    records_processed_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total records dropped (late data, rejected)
    records_dropped_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total checkpoints completed
    checkpoints_completed_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total processing errors
    errors_total: Atomic(u64) = Atomic(u64).init(0),

    pub fn init() ProcessingMetrics {
        return .{};
    }

    pub fn recordSubmitted(self: *ProcessingMetrics) void {
        _ = self.jobs_submitted_total.fetchAdd(1, .monotonic);
        _ = self.active_jobs.fetchAdd(1, .monotonic);
    }

    pub fn recordCompleted(self: *ProcessingMetrics) void {
        _ = self.jobs_completed_total.fetchAdd(1, .monotonic);
        const active = self.active_jobs.load(.monotonic);
        if (active > 0) _ = self.active_jobs.fetchSub(1, .monotonic);
    }

    pub fn recordFailed(self: *ProcessingMetrics) void {
        _ = self.jobs_failed_total.fetchAdd(1, .monotonic);
        const active = self.active_jobs.load(.monotonic);
        if (active > 0) _ = self.active_jobs.fetchSub(1, .monotonic);
    }

    pub fn recordCancelled(self: *ProcessingMetrics) void {
        _ = self.jobs_cancelled_total.fetchAdd(1, .monotonic);
        const active = self.active_jobs.load(.monotonic);
        if (active > 0) _ = self.active_jobs.fetchSub(1, .monotonic);
    }

    pub fn recordProcessed(self: *ProcessingMetrics, count: u64) void {
        _ = self.records_processed_total.fetchAdd(count, .monotonic);
    }

    pub fn recordDropped(self: *ProcessingMetrics) void {
        _ = self.records_dropped_total.fetchAdd(1, .monotonic);
    }

    pub fn recordCheckpoint(self: *ProcessingMetrics) void {
        _ = self.checkpoints_completed_total.fetchAdd(1, .monotonic);
    }

    pub fn recordError(self: *ProcessingMetrics) void {
        _ = self.errors_total.fetchAdd(1, .monotonic);
    }

    pub const Snapshot = struct {
        active_jobs: u64,
        jobs_submitted_total: u64,
        jobs_completed_total: u64,
        jobs_failed_total: u64,
        jobs_cancelled_total: u64,
        records_processed_total: u64,
        records_dropped_total: u64,
        checkpoints_completed_total: u64,
        errors_total: u64,
    };

    pub fn snapshot(self: *const ProcessingMetrics) Snapshot {
        return .{
            .active_jobs = self.active_jobs.load(.monotonic),
            .jobs_submitted_total = self.jobs_submitted_total.load(.monotonic),
            .jobs_completed_total = self.jobs_completed_total.load(.monotonic),
            .jobs_failed_total = self.jobs_failed_total.load(.monotonic),
            .jobs_cancelled_total = self.jobs_cancelled_total.load(.monotonic),
            .records_processed_total = self.records_processed_total.load(.monotonic),
            .records_dropped_total = self.records_dropped_total.load(.monotonic),
            .checkpoints_completed_total = self.checkpoints_completed_total.load(.monotonic),
            .errors_total = self.errors_total.load(.monotonic),
        };
    }
};

// =============================================================================
// Replication Metrics
// =============================================================================

/// Replication metrics (singleton — one per node).
///
/// Surfaces the otherwise-silent failure modes of best-effort broadcast
/// replication (issue #16): a follower that misses a committed entry, an
/// over-size entry that is never broadcast, and a per-peer send that fails.
/// These are detection-only — they make divergence *observable*; they do not
/// repair it. The matching loud log lines live at the call sites.
pub const ReplicationMetrics = struct {
    /// Distinct gap events observed on a follower (received index > expected).
    follower_gaps_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total entries inferred missing across all gaps (sum of gap widths).
    follower_entries_missing_total: Atomic(u64) = Atomic(u64).init(0),
    /// Over-size committed entries never broadcast to any peer (leader side).
    broadcast_oversize_skipped_total: Atomic(u64) = Atomic(u64).init(0),
    /// Per-peer broadcast sends that failed and were not retried (leader side).
    broadcast_send_failures_total: Atomic(u64) = Atomic(u64).init(0),
    /// Follower index at which a gap was last observed (gauge, debugging aid).
    last_gap_received_index: Atomic(u64) = Atomic(u64).init(0),

    pub fn init() ReplicationMetrics {
        return .{};
    }

    pub fn recordFollowerGap(self: *ReplicationMetrics, missing: u64, received_index: u64) void {
        _ = self.follower_gaps_total.fetchAdd(1, .monotonic);
        _ = self.follower_entries_missing_total.fetchAdd(missing, .monotonic);
        self.last_gap_received_index.store(received_index, .monotonic);
    }

    pub fn recordOversizeSkipped(self: *ReplicationMetrics) void {
        _ = self.broadcast_oversize_skipped_total.fetchAdd(1, .monotonic);
    }

    pub fn recordSendFailure(self: *ReplicationMetrics) void {
        _ = self.broadcast_send_failures_total.fetchAdd(1, .monotonic);
    }

    pub const Snapshot = struct {
        follower_gaps_total: u64,
        follower_entries_missing_total: u64,
        broadcast_oversize_skipped_total: u64,
        broadcast_send_failures_total: u64,
        last_gap_received_index: u64,
    };

    pub fn snapshot(self: *const ReplicationMetrics) Snapshot {
        return .{
            .follower_gaps_total = self.follower_gaps_total.load(.monotonic),
            .follower_entries_missing_total = self.follower_entries_missing_total.load(.monotonic),
            .broadcast_oversize_skipped_total = self.broadcast_oversize_skipped_total.load(.monotonic),
            .broadcast_send_failures_total = self.broadcast_send_failures_total.load(.monotonic),
            .last_gap_received_index = self.last_gap_received_index.load(.monotonic),
        };
    }
};

/// Server-level metrics (connections, subscriptions, commands, etc.)
pub const ServerMetrics = struct {
    /// Current active connections
    connections: Atomic(u64) = Atomic(u64).init(0),
    /// Current active subscriptions
    subscriptions: Atomic(u64) = Atomic(u64).init(0),
    /// Total commands processed
    commands_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total bytes received
    bytes_received: Atomic(u64) = Atomic(u64).init(0),
    /// Total bytes sent
    bytes_sent: Atomic(u64) = Atomic(u64).init(0),
    /// Server start timestamp (ms)
    start_time_ms: i64,

    pub fn init() ServerMetrics {
        return .{
            .start_time_ms = @import("stdx").time.milliTimestamp(),
        };
    }

    pub fn connectionOpened(self: *ServerMetrics) void {
        _ = self.connections.fetchAdd(1, .monotonic);
    }

    pub fn connectionClosed(self: *ServerMetrics) void {
        _ = self.connections.fetchSub(1, .monotonic);
    }

    pub fn subscriptionAdded(self: *ServerMetrics) void {
        _ = self.subscriptions.fetchAdd(1, .monotonic);
    }

    pub fn subscriptionRemoved(self: *ServerMetrics) void {
        _ = self.subscriptions.fetchSub(1, .monotonic);
    }

    pub fn recordCommand(self: *ServerMetrics) void {
        _ = self.commands_total.fetchAdd(1, .monotonic);
    }

    pub fn recordBytesReceived(self: *ServerMetrics, bytes: u64) void {
        _ = self.bytes_received.fetchAdd(bytes, .monotonic);
    }

    pub fn recordBytesSent(self: *ServerMetrics, bytes: u64) void {
        _ = self.bytes_sent.fetchAdd(bytes, .monotonic);
    }

    pub fn uptimeSeconds(self: *const ServerMetrics) u64 {
        const now = @import("stdx").time.milliTimestamp();
        return @intCast(@divFloor(now - self.start_time_ms, 1000));
    }

    pub const Snapshot = struct {
        connections: u64,
        subscriptions: u64,
        commands_total: u64,
        bytes_received: u64,
        bytes_sent: u64,
        uptime_seconds: u64,
    };

    pub fn snapshot(self: *const ServerMetrics) Snapshot {
        return .{
            .connections = self.connections.load(.monotonic),
            .subscriptions = self.subscriptions.load(.monotonic),
            .commands_total = self.commands_total.load(.monotonic),
            .bytes_received = self.bytes_received.load(.monotonic),
            .bytes_sent = self.bytes_sent.load(.monotonic),
            .uptime_seconds = self.uptimeSeconds(),
        };
    }
};

// =============================================================================
// Per-Shard Metrics
// =============================================================================

/// Per-shard metrics for the thread-per-shard architecture.
///
/// Each CPU-pinned shard thread updates its own ShardMetrics instance via
/// lock-free atomics. The dedicated metrics HTTP thread reads all shard
/// instances during Prometheus export — no mutex contention on the hot path.
///
/// Usage:
/// ```zig
/// // During node startup (once)
/// try registry.initShards(num_cpus);
///
/// // In each shard thread
/// const my_metrics = registry.shardMetrics(shard_id).?;
/// my_metrics.recordCommand();
/// my_metrics.recordBytesReceived(1024);
/// ```
pub const ShardMetrics = struct {
    shard_id: u16,

    /// Current active connections owned by this shard
    connections: Atomic(u64) = Atomic(u64).init(0),
    /// Total commands processed on this shard
    commands_total: Atomic(u64) = Atomic(u64).init(0),
    /// Total bytes received on this shard
    bytes_received: Atomic(u64) = Atomic(u64).init(0),
    /// Total bytes sent from this shard
    bytes_sent: Atomic(u64) = Atomic(u64).init(0),
    /// Current subscriptions on this shard
    subscriptions: Atomic(u64) = Atomic(u64).init(0),
    /// Total partitions owned by this shard
    partitions: Atomic(u64) = Atomic(u64).init(0),
    /// Reactor event loop iterations (measures utilization)
    reactor_loops: Atomic(u64) = Atomic(u64).init(0),
    /// Total inbox messages processed
    inbox_processed: Atomic(u64) = Atomic(u64).init(0),
    /// Current inbox messages pending (snapshot gauge)
    inbox_pending: Atomic(u64) = Atomic(u64).init(0),

    pub fn init(id: u16) ShardMetrics {
        return .{ .shard_id = id };
    }

    // --- Convenience update methods (called by shard threads) ---

    pub fn connectionOpened(self: *ShardMetrics) void {
        _ = self.connections.fetchAdd(1, .monotonic);
    }

    pub fn connectionClosed(self: *ShardMetrics) void {
        _ = self.connections.fetchSub(1, .monotonic);
    }

    pub fn recordCommand(self: *ShardMetrics) void {
        _ = self.commands_total.fetchAdd(1, .monotonic);
    }

    pub fn recordBytesReceived(self: *ShardMetrics, bytes: u64) void {
        _ = self.bytes_received.fetchAdd(bytes, .monotonic);
    }

    pub fn recordBytesSent(self: *ShardMetrics, bytes: u64) void {
        _ = self.bytes_sent.fetchAdd(bytes, .monotonic);
    }

    pub fn subscriptionAdded(self: *ShardMetrics) void {
        _ = self.subscriptions.fetchAdd(1, .monotonic);
    }

    pub fn subscriptionRemoved(self: *ShardMetrics) void {
        _ = self.subscriptions.fetchSub(1, .monotonic);
    }

    pub fn setPartitions(self: *ShardMetrics, count: u64) void {
        self.partitions.store(count, .monotonic);
    }

    pub fn recordReactorLoop(self: *ShardMetrics) void {
        _ = self.reactor_loops.fetchAdd(1, .monotonic);
    }

    pub fn recordInboxProcessed(self: *ShardMetrics, count: u64) void {
        _ = self.inbox_processed.fetchAdd(count, .monotonic);
    }

    pub fn setInboxPending(self: *ShardMetrics, count: u64) void {
        self.inbox_pending.store(count, .monotonic);
    }

    pub const Snapshot = struct {
        shard_id: u16,
        connections: u64,
        commands_total: u64,
        bytes_received: u64,
        bytes_sent: u64,
        subscriptions: u64,
        partitions: u64,
        reactor_loops: u64,
        inbox_processed: u64,
        inbox_pending: u64,
    };

    pub fn snapshot(self: *const ShardMetrics) Snapshot {
        return .{
            .shard_id = self.shard_id,
            .connections = self.connections.load(.monotonic),
            .commands_total = self.commands_total.load(.monotonic),
            .bytes_received = self.bytes_received.load(.monotonic),
            .bytes_sent = self.bytes_sent.load(.monotonic),
            .subscriptions = self.subscriptions.load(.monotonic),
            .partitions = self.partitions.load(.monotonic),
            .reactor_loops = self.reactor_loops.load(.monotonic),
            .inbox_processed = self.inbox_processed.load(.monotonic),
            .inbox_pending = self.inbox_pending.load(.monotonic),
        };
    }
};

/// MetricsRegistry is the global metrics aggregator for all Flo primitives
///
/// Design:
/// - Node layer creates ONE registry
/// - Registry owns all metrics instances
/// - Primitives (streams, queues, KV) register and get metrics pointers
/// - /metrics endpoint calls registry.exportPrometheus()
///
/// Usage:
/// ```zig
/// // Node initialization
/// var registry = MetricsRegistry.init(allocator);
/// defer registry.deinit();
///
/// // Register stream
/// const stream_metrics = try registry.registerStream("prod", "orders", 0);
///
/// // Open stream with metrics
/// var log = try Log.open(allocator, engine, .{
///     .namespace = "prod",
///     .topic = "orders",
///     .partition = 0,
///     .metrics = stream_metrics,
/// });
///
/// // Export all metrics (for Prometheus scraping)
/// const prometheus_text = try registry.exportPrometheus(allocator);
/// defer allocator.free(prometheus_text);
/// ```
pub const MetricsRegistry = struct {
    allocator: Allocator,

    /// Server-level metrics (connections, commands, etc.)
    server: ServerMetrics,

    /// Registered stream metrics
    /// Key format: "{namespace}:{topic}:{partition}"
    streams: std.StringHashMap(StreamEntry),

    /// Registered queue metrics
    /// Key format: "{namespace}:{queue}"
    queues: std.StringHashMap(QueueEntry),

    /// Registered KV namespace metrics
    /// Key format: "{namespace}"
    kv_namespaces: std.StringHashMap(KVEntry),

    /// Registered tiered log metrics
    /// Key: group_id (Raft group)
    tiered_logs: std.AutoHashMap(u32, TieredLogEntry),

    /// Workflow engine metrics (singleton - one per node)
    workflow: WorkflowMetrics,

    /// Processing engine metrics (singleton - one per node)
    processing: ProcessingMetrics,

    /// Replication metrics (singleton - one per node)
    replication: ReplicationMetrics,

    /// Per-shard metrics for the thread-per-shard architecture.
    /// Indexed by shard_id. Null until initShards() is called.
    shard_counters: ?[]ShardMetrics,

    /// Number of configured shards (0 until initShards() is called)
    num_shards: u32,

    mutex: @import("stdx").Mutex,

    pub const StreamEntry = struct {
        namespace: []const u8,
        topic: []const u8,
        partition: u32,
        metrics: StreamMetrics,
    };

    pub const QueueEntry = struct {
        namespace: []const u8,
        queue: []const u8,
        metrics: QueueMetrics,
    };

    pub const KVEntry = struct {
        namespace: []const u8,
        metrics: KVMetrics,
    };

    pub const TieredLogEntry = struct {
        group_id: u32,
        metrics: TieredLogMetrics,
    };

    pub fn init(allocator: Allocator) MetricsRegistry {
        return MetricsRegistry{
            .allocator = allocator,
            .server = ServerMetrics.init(),
            .streams = std.StringHashMap(StreamEntry).init(allocator),
            .queues = std.StringHashMap(QueueEntry).init(allocator),
            .kv_namespaces = std.StringHashMap(KVEntry).init(allocator),
            .tiered_logs = std.AutoHashMap(u32, TieredLogEntry).init(allocator),
            .workflow = WorkflowMetrics.init(),
            .processing = ProcessingMetrics.init(),
            .replication = ReplicationMetrics.init(),
            .shard_counters = null,
            .num_shards = 0,
            .mutex = @import("stdx").Mutex{},
        };
    }

    pub fn deinit(self: *MetricsRegistry) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Free shard counters
        if (self.shard_counters) |counters| {
            self.allocator.free(counters);
            self.shard_counters = null;
        }

        // Free stream keys and entries
        var stream_key_iter = self.streams.keyIterator();
        while (stream_key_iter.next()) |key| {
            self.allocator.free(key.*);
        }
        var stream_value_iter = self.streams.valueIterator();
        while (stream_value_iter.next()) |entry| {
            self.allocator.free(entry.namespace);
            self.allocator.free(entry.topic);
        }
        self.streams.deinit();

        // Free queue keys and entries
        var queue_key_iter = self.queues.keyIterator();
        while (queue_key_iter.next()) |key| {
            self.allocator.free(key.*);
        }
        var queue_value_iter = self.queues.valueIterator();
        while (queue_value_iter.next()) |entry| {
            self.allocator.free(entry.namespace);
            self.allocator.free(entry.queue);
        }
        self.queues.deinit();

        // Free KV namespace keys and entries
        var kv_key_iter = self.kv_namespaces.keyIterator();
        while (kv_key_iter.next()) |key| {
            self.allocator.free(key.*);
        }
        var kv_value_iter = self.kv_namespaces.valueIterator();
        while (kv_value_iter.next()) |entry| {
            self.allocator.free(entry.namespace);
        }
        self.kv_namespaces.deinit();

        // Free tiered log entries (no string keys)
        self.tiered_logs.deinit();
    }

    /// Register a stream and return a pointer to its metrics
    ///
    /// Returns: Pointer to StreamMetrics (owned by registry, valid until deinit)
    ///
    /// Thread-safe: Multiple threads can register different streams concurrently
    pub fn registerStream(
        self: *MetricsRegistry,
        namespace: []const u8,
        topic: []const u8,
        partition: u32,
    ) !*StreamMetrics {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Create unique key
        const key = try std.fmt.allocPrint(
            self.allocator,
            "{s}:{s}:{d}",
            .{ namespace, topic, partition },
        );
        errdefer self.allocator.free(key);

        // Check if already registered
        if (self.streams.getPtr(key)) |existing| {
            // Already exists - free key and return existing metrics
            self.allocator.free(key);
            return &existing.metrics;
        }

        // Create new entry
        const entry = StreamEntry{
            .namespace = try self.allocator.dupe(u8, namespace),
            .topic = try self.allocator.dupe(u8, topic),
            .partition = partition,
            .metrics = StreamMetrics.init(),
        };

        try self.streams.put(key, entry);

        // Return pointer to metrics (safe because HashMap owns the entry)
        return &self.streams.getPtr(key).?.metrics;
    }

    /// Remove a stream's metrics entry (mirror of `registerStream`). No-op if
    /// the stream was never registered. Frees the owned key, namespace, topic.
    pub fn unregisterStream(
        self: *MetricsRegistry,
        namespace: []const u8,
        topic: []const u8,
        partition: u32,
    ) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const key = std.fmt.allocPrint(
            self.allocator,
            "{s}:{s}:{d}",
            .{ namespace, topic, partition },
        ) catch return;
        defer self.allocator.free(key);

        if (self.streams.fetchRemove(key)) |kv| {
            self.allocator.free(@constCast(kv.key));
            self.allocator.free(kv.value.namespace);
            self.allocator.free(kv.value.topic);
        }
    }

    /// Register a queue and return a pointer to its metrics
    ///
    /// Returns: Pointer to QueueMetrics (owned by registry, valid until deinit)
    ///
    /// Thread-safe: Multiple threads can register different queues concurrently
    pub fn registerQueue(
        self: *MetricsRegistry,
        namespace: []const u8,
        queue: []const u8,
    ) !*QueueMetrics {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Create unique key
        const key = try std.fmt.allocPrint(
            self.allocator,
            "{s}:{s}",
            .{ namespace, queue },
        );
        errdefer self.allocator.free(key);

        // Check if already registered
        if (self.queues.getPtr(key)) |existing| {
            self.allocator.free(key);
            return &existing.metrics;
        }

        // Create new entry
        const entry = QueueEntry{
            .namespace = try self.allocator.dupe(u8, namespace),
            .queue = try self.allocator.dupe(u8, queue),
            .metrics = QueueMetrics.init(),
        };

        try self.queues.put(key, entry);
        return &self.queues.getPtr(key).?.metrics;
    }

    /// Register a KV namespace and return a pointer to its metrics
    ///
    /// Returns: Pointer to KVMetrics (owned by registry, valid until deinit)
    ///
    /// Thread-safe: Multiple threads can register different namespaces concurrently
    pub fn registerKVNamespace(
        self: *MetricsRegistry,
        namespace: []const u8,
    ) !*KVMetrics {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Key is just the namespace name
        const key = try self.allocator.dupe(u8, namespace);
        errdefer self.allocator.free(key);

        // Check if already registered
        if (self.kv_namespaces.getPtr(key)) |existing| {
            self.allocator.free(key);
            return &existing.metrics;
        }

        // Create new entry
        const entry = KVEntry{
            .namespace = try self.allocator.dupe(u8, namespace),
            .metrics = KVMetrics.init(),
        };

        try self.kv_namespaces.put(key, entry);
        return &self.kv_namespaces.getPtr(key).?.metrics;
    }

    /// Register a tiered log (Raft group) and return a pointer to its metrics
    ///
    /// Returns: Pointer to TieredLogMetrics (owned by registry, valid until deinit)
    ///
    /// Thread-safe: Multiple threads can register different logs concurrently
    pub fn registerTieredLog(self: *MetricsRegistry, group_id: u32) !*TieredLogMetrics {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check if already registered
        if (self.tiered_logs.getPtr(group_id)) |existing| {
            return &existing.metrics;
        }

        // Create new entry
        const entry = TieredLogEntry{
            .group_id = group_id,
            .metrics = TieredLogMetrics.init(),
        };

        try self.tiered_logs.put(group_id, entry);
        return &self.tiered_logs.getPtr(group_id).?.metrics;
    }

    /// Initialize per-shard metrics counters.
    /// Must be called once during node startup with the number of shards.
    /// Each shard thread then obtains its ShardMetrics via shardMetrics(shard_id).
    pub fn initShards(self: *MetricsRegistry, num_shards_arg: u32) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.shard_counters != null) return error.AlreadyInitialized;
        if (num_shards_arg == 0) return error.InvalidShardCount;

        const counters = try self.allocator.alloc(ShardMetrics, num_shards_arg);
        for (counters, 0..) |*c, i| {
            c.* = ShardMetrics.init(@intCast(i));
        }
        self.shard_counters = counters;
        self.num_shards = num_shards_arg;
    }

    /// Get per-shard metrics for a specific shard.
    /// Returns null if initShards() has not been called or shard_id is out of range.
    pub fn shardMetrics(self: *MetricsRegistry, shard_id: u16) ?*ShardMetrics {
        const counters = self.shard_counters orelse return null;
        if (shard_id >= counters.len) return null;
        return &counters[shard_id];
    }

    /// Get number of configured shards.
    pub fn shardCount(self: *MetricsRegistry) u32 {
        return self.num_shards;
    }

    /// Export all metrics in Prometheus text format
    ///
    /// Returns: Allocated string (caller must free)
    ///
    /// Thread-safe: Reads metrics atomically
    pub fn exportPrometheus(self: *MetricsRegistry, allocator: Allocator) ![]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var aw: std.Io.Writer.Allocating = .init(allocator);
        errdefer aw.deinit();
        const writer = &aw.writer;

        // Export server metrics first
        try writeServerMetrics(writer, self.server.snapshot());

        // Export per-shard metrics
        if (self.shard_counters) |counters| {
            try writer.print("\n# HELP flo_shards_total Total number of shards\n", .{});
            try writer.print("# TYPE flo_shards_total gauge\n", .{});
            try writer.print("flo_shards_total {d}\n", .{counters.len});

            for (counters) |*shard| {
                try writeShardMetrics(writer, shard.snapshot());
            }
        }

        // Export stream metrics
        var iter = self.streams.valueIterator();
        while (iter.next()) |entry| {
            const snapshot = entry.metrics.snapshot();

            // Format labels
            const labels = try std.fmt.allocPrint(
                allocator,
                "namespace=\"{s}\",topic=\"{s}\",partition=\"{d}\"",
                .{ entry.namespace, entry.topic, entry.partition },
            );
            defer allocator.free(labels);

            // Write metrics
            try writeStreamMetrics(writer, snapshot, labels);
        }

        // Export tiered log metrics
        var tiered_iter = self.tiered_logs.valueIterator();
        while (tiered_iter.next()) |entry| {
            const snapshot = entry.metrics.snapshot();
            try writeTieredLogMetrics(writer, snapshot, entry.group_id);
        }

        // Export queue metrics
        var queue_iter = self.queues.valueIterator();
        while (queue_iter.next()) |entry| {
            const q_snapshot = entry.metrics.snapshot();

            const q_labels = try std.fmt.allocPrint(
                allocator,
                "namespace=\"{s}\",queue=\"{s}\"",
                .{ entry.namespace, entry.queue },
            );
            defer allocator.free(q_labels);

            try writeQueueMetrics(writer, q_snapshot, q_labels);
        }

        // Export workflow metrics
        try writeWorkflowMetrics(writer, self.workflow.snapshot());

        // Export processing metrics
        try writeProcessingMetrics(writer, self.processing.snapshot());

        // Export replication metrics
        try writeReplicationMetrics(writer, self.replication.snapshot());

        return aw.toOwnedSlice();
    }

    /// Get stream count (for monitoring/debugging)
    pub fn streamCount(self: *MetricsRegistry) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.streams.count();
    }

    /// Get queue count (for monitoring/debugging)
    pub fn queueCount(self: *MetricsRegistry) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.queues.count();
    }

    /// Get KV namespace count (for monitoring/debugging)
    pub fn kvNamespaceCount(self: *MetricsRegistry) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.kv_namespaces.count();
    }
};

/// Write server metrics in Prometheus format
fn writeServerMetrics(writer: anytype, snapshot: ServerMetrics.Snapshot) !void {
    // Connection metrics
    try writer.print("# HELP flo_connections_current Current number of active connections\n", .{});
    try writer.print("# TYPE flo_connections_current gauge\n", .{});
    try writer.print("flo_connections_current {d}\n", .{snapshot.connections});

    try writer.print("# HELP flo_subscriptions_current Current number of active stream subscriptions\n", .{});
    try writer.print("# TYPE flo_subscriptions_current gauge\n", .{});
    try writer.print("flo_subscriptions_current {d}\n", .{snapshot.subscriptions});

    // Command metrics
    try writer.print("# HELP flo_commands_total Total number of commands processed\n", .{});
    try writer.print("# TYPE flo_commands_total counter\n", .{});
    try writer.print("flo_commands_total {d}\n", .{snapshot.commands_total});

    // Network metrics
    try writer.print("# HELP flo_bytes_received_total Total bytes received from clients\n", .{});
    try writer.print("# TYPE flo_bytes_received_total counter\n", .{});
    try writer.print("flo_bytes_received_total {d}\n", .{snapshot.bytes_received});

    try writer.print("# HELP flo_bytes_sent_total Total bytes sent to clients\n", .{});
    try writer.print("# TYPE flo_bytes_sent_total counter\n", .{});
    try writer.print("flo_bytes_sent_total {d}\n", .{snapshot.bytes_sent});

    // Uptime
    try writer.print("# HELP flo_uptime_seconds Server uptime in seconds\n", .{});
    try writer.print("# TYPE flo_uptime_seconds gauge\n", .{});
    try writer.print("flo_uptime_seconds {d}\n", .{snapshot.uptime_seconds});

    try writer.print("\n", .{}); // Separator before stream metrics
}

/// Write stream metrics in Prometheus format
fn writeStreamMetrics(
    writer: anytype,
    snapshot: StreamMetrics.Snapshot,
    labels: []const u8,
) !void {
    // Append metrics
    try writer.print("flo_stream_append_ops_total{{{s}}} {d}\n", .{ labels, snapshot.append_ops_total });
    try writer.print("flo_stream_append_records_total{{{s}}} {d}\n", .{ labels, snapshot.append_records_total });
    try writer.print("flo_stream_append_bytes_total{{{s}}} {d}\n", .{ labels, snapshot.append_bytes_total });
    try writer.print("flo_stream_append_errors_total{{{s}}} {d}\n", .{ labels, snapshot.append_errors_total });
    try writer.print("flo_stream_append_oversized_total{{{s}}} {d}\n", .{ labels, snapshot.append_oversized_total });

    // Read metrics
    try writer.print("flo_stream_read_ops_total{{{s}}} {d}\n", .{ labels, snapshot.read_ops_total });
    try writer.print("flo_stream_read_records_total{{{s}}} {d}\n", .{ labels, snapshot.read_records_total });
    try writer.print("flo_stream_read_bytes_total{{{s}}} {d}\n", .{ labels, snapshot.read_bytes_total });
    try writer.print("flo_stream_read_errors_total{{{s}}} {d}\n", .{ labels, snapshot.read_errors_total });
    try writer.print("flo_stream_read_empty_total{{{s}}} {d}\n", .{ labels, snapshot.read_empty_total });

    // Blocking read metrics
    try writer.print("flo_stream_blocking_readers_current{{{s}}} {d}\n", .{ labels, snapshot.blocking_readers_current });
    try writer.print("flo_stream_blocking_read_waits_total{{{s}}} {d}\n", .{ labels, snapshot.blocking_read_waits_total });
    try writer.print("flo_stream_blocking_read_timeouts_total{{{s}}} {d}\n", .{ labels, snapshot.blocking_read_timeouts_total });
    try writer.print("flo_stream_blocking_read_wakeups_total{{{s}}} {d}\n", .{ labels, snapshot.blocking_read_wakeups_total });

    // Consumer group metrics
    try writer.print("flo_stream_consumer_group_reads_total{{{s}}} {d}\n", .{ labels, snapshot.consumer_group_reads_total });
    try writer.print("flo_stream_consumer_group_claims_total{{{s}}} {d}\n", .{ labels, snapshot.consumer_group_claims_total });
    try writer.print("flo_stream_consumer_group_acks_total{{{s}}} {d}\n", .{ labels, snapshot.consumer_group_acks_total });
    try writer.print("flo_stream_consumer_group_autoclaims_total{{{s}}} {d}\n", .{ labels, snapshot.consumer_group_autoclaims_total });
    try writer.print("flo_stream_consumer_group_pending_current{{{s}}} {d}\n", .{ labels, snapshot.consumer_group_pending_current });

    // Stream state
    try writer.print("flo_stream_last_seq{{{s}}} {d}\n", .{ labels, snapshot.stream_last_seq });
    try writer.print("flo_stream_trim_ops_total{{{s}}} {d}\n", .{ labels, snapshot.trim_ops_total });
    try writer.print("flo_stream_trim_records_total{{{s}}} {d}\n", .{ labels, snapshot.trim_records_total });
    try writer.print("flo_stream_trim_bytes_total{{{s}}} {d}\n", .{ labels, snapshot.trim_bytes_total });
}

/// Write queue metrics in Prometheus format (rich per-queue metrics from queue/metrics.zig)
fn writeQueueMetrics(
    writer: anytype,
    snapshot: QueueMetrics.Snapshot,
    labels: []const u8,
) !void {
    // Enqueue metrics
    try writer.print("flo_queue_enqueue_ops_total{{{s}}} {d}\n", .{ labels, snapshot.enqueue_ops_total });
    try writer.print("flo_queue_enqueue_messages_total{{{s}}} {d}\n", .{ labels, snapshot.enqueue_messages_total });
    try writer.print("flo_queue_enqueue_bytes_total{{{s}}} {d}\n", .{ labels, snapshot.enqueue_bytes_total });
    try writer.print("flo_queue_enqueue_delayed_total{{{s}}} {d}\n", .{ labels, snapshot.enqueue_delayed_total });
    try writer.print("flo_queue_enqueue_errors_total{{{s}}} {d}\n", .{ labels, snapshot.enqueue_errors_total });
    try writer.print("flo_queue_enqueue_dedup_rejected_total{{{s}}} {d}\n", .{ labels, snapshot.enqueue_dedup_rejected_total });

    // Dequeue metrics
    try writer.print("flo_queue_dequeue_ops_total{{{s}}} {d}\n", .{ labels, snapshot.dequeue_ops_total });
    try writer.print("flo_queue_dequeue_messages_total{{{s}}} {d}\n", .{ labels, snapshot.dequeue_messages_total });
    try writer.print("flo_queue_dequeue_bytes_total{{{s}}} {d}\n", .{ labels, snapshot.dequeue_bytes_total });
    try writer.print("flo_queue_dequeue_empty_total{{{s}}} {d}\n", .{ labels, snapshot.dequeue_empty_total });
    try writer.print("flo_queue_dequeue_errors_total{{{s}}} {d}\n", .{ labels, snapshot.dequeue_errors_total });

    // Lease metrics
    try writer.print("flo_queue_leases_active{{{s}}} {d}\n", .{ labels, snapshot.leases_active_current });
    try writer.print("flo_queue_leases_created_total{{{s}}} {d}\n", .{ labels, snapshot.leases_created_total });
    try writer.print("flo_queue_leases_completed_total{{{s}}} {d}\n", .{ labels, snapshot.leases_completed_total });
    try writer.print("flo_queue_leases_failed_total{{{s}}} {d}\n", .{ labels, snapshot.leases_failed_total });
    try writer.print("flo_queue_leases_expired_total{{{s}}} {d}\n", .{ labels, snapshot.leases_expired_total });
    try writer.print("flo_queue_leases_extended_total{{{s}}} {d}\n", .{ labels, snapshot.leases_extended_total });

    // DLQ metrics
    try writer.print("flo_queue_dlq_messages_total{{{s}}} {d}\n", .{ labels, snapshot.dlq_messages_total });
    try writer.print("flo_queue_dlq_requeue_total{{{s}}} {d}\n", .{ labels, snapshot.dlq_requeue_total });
    try writer.print("flo_queue_dlq_delete_total{{{s}}} {d}\n", .{ labels, snapshot.dlq_delete_total });
    try writer.print("flo_queue_dlq_messages_current{{{s}}} {d}\n", .{ labels, snapshot.dlq_messages_current });

    // Sweeper metrics
    try writer.print("flo_queue_sweeper_runs_total{{{s}}} {d}\n", .{ labels, snapshot.sweeper_runs_total });
    try writer.print("flo_queue_sweeper_reclaimed_total{{{s}}} {d}\n", .{ labels, snapshot.sweeper_reclaimed_total });
    try writer.print("flo_queue_sweeper_orphaned_total{{{s}}} {d}\n", .{ labels, snapshot.sweeper_orphaned_total });
    try writer.print("flo_queue_sweeper_scanned_total{{{s}}} {d}\n", .{ labels, snapshot.sweeper_scanned_total });
    try writer.print("flo_queue_sweeper_errors_total{{{s}}} {d}\n", .{ labels, snapshot.sweeper_errors_total });

    // Delayed message metrics
    try writer.print("flo_queue_delayed_promoted_total{{{s}}} {d}\n", .{ labels, snapshot.delayed_promoted_total });
    try writer.print("flo_queue_delayed_messages_current{{{s}}} {d}\n", .{ labels, snapshot.delayed_messages_current });

    // Queue state
    try writer.print("flo_queue_available_current{{{s}}} {d}\n", .{ labels, snapshot.queue_available_current });
    try writer.print("flo_queue_last_seq{{{s}}} {d}\n", .{ labels, snapshot.queue_last_seq });
}

/// Write tiered log metrics in Prometheus format
fn writeTieredLogMetrics(
    writer: anytype,
    snapshot: TieredLogMetrics.Snapshot,
    group_id: u32,
) !void {
    // Tier hit metrics
    try writer.print("# HELP flo_tiered_log_hot_hits_total Reads served from hot tier (RAM)\n", .{});
    try writer.print("# TYPE flo_tiered_log_hot_hits_total counter\n", .{});
    try writer.print("flo_tiered_log_hot_hits_total{{group_id=\"{d}\"}} {d}\n", .{ group_id, snapshot.hot_hits });

    try writer.print("# HELP flo_tiered_log_warm_hits_total Reads served from warm tier (local disk)\n", .{});
    try writer.print("# TYPE flo_tiered_log_warm_hits_total counter\n", .{});
    try writer.print("flo_tiered_log_warm_hits_total{{group_id=\"{d}\"}} {d}\n", .{ group_id, snapshot.warm_hits });

    try writer.print("# HELP flo_tiered_log_cold_hits_total Reads served from cold tier (S3/Azure)\n", .{});
    try writer.print("# TYPE flo_tiered_log_cold_hits_total counter\n", .{});
    try writer.print("flo_tiered_log_cold_hits_total{{group_id=\"{d}\"}} {d}\n", .{ group_id, snapshot.cold_hits });

    try writer.print("# HELP flo_tiered_log_misses_total Reads that found no entry\n", .{});
    try writer.print("# TYPE flo_tiered_log_misses_total counter\n", .{});
    try writer.print("flo_tiered_log_misses_total{{group_id=\"{d}\"}} {d}\n", .{ group_id, snapshot.misses });

    try writer.print("# HELP flo_tiered_log_reads_total Total read operations\n", .{});
    try writer.print("# TYPE flo_tiered_log_reads_total counter\n", .{});
    try writer.print("flo_tiered_log_reads_total{{group_id=\"{d}\"}} {d}\n", .{ group_id, snapshot.total_reads });

    // Hit rate gauges (computed from counters)
    try writer.print("# HELP flo_tiered_log_hot_hit_rate Hot tier hit rate (0.0-1.0)\n", .{});
    try writer.print("# TYPE flo_tiered_log_hot_hit_rate gauge\n", .{});
    try writer.print("flo_tiered_log_hot_hit_rate{{group_id=\"{d}\"}} {d:.4}\n", .{ group_id, snapshot.hotHitRate() });
}

/// Write workflow metrics in Prometheus format
fn writeWorkflowMetrics(writer: anytype, snapshot: WorkflowMetrics.Snapshot) !void {
    try writer.print("\n# HELP flo_workflow_active_runs Current active workflow runs\n", .{});
    try writer.print("# TYPE flo_workflow_active_runs gauge\n", .{});
    try writer.print("flo_workflow_active_runs {d}\n", .{snapshot.active_runs});

    try writer.print("# HELP flo_workflow_started_total Total workflows started\n", .{});
    try writer.print("# TYPE flo_workflow_started_total counter\n", .{});
    try writer.print("flo_workflow_started_total {d}\n", .{snapshot.started_total});

    try writer.print("# HELP flo_workflow_completed_total Total workflows completed successfully\n", .{});
    try writer.print("# TYPE flo_workflow_completed_total counter\n", .{});
    try writer.print("flo_workflow_completed_total {d}\n", .{snapshot.completed_total});

    try writer.print("# HELP flo_workflow_failed_total Total workflows failed\n", .{});
    try writer.print("# TYPE flo_workflow_failed_total counter\n", .{});
    try writer.print("flo_workflow_failed_total {d}\n", .{snapshot.failed_total});

    try writer.print("# HELP flo_workflow_cancelled_total Total workflows cancelled\n", .{});
    try writer.print("# TYPE flo_workflow_cancelled_total counter\n", .{});
    try writer.print("flo_workflow_cancelled_total {d}\n", .{snapshot.cancelled_total});

    try writer.print("# HELP flo_workflow_timed_out_total Total workflows timed out\n", .{});
    try writer.print("# TYPE flo_workflow_timed_out_total counter\n", .{});
    try writer.print("flo_workflow_timed_out_total {d}\n", .{snapshot.timed_out_total});

    try writer.print("# HELP flo_workflow_signals_delivered_total Total signals delivered\n", .{});
    try writer.print("# TYPE flo_workflow_signals_delivered_total counter\n", .{});
    try writer.print("flo_workflow_signals_delivered_total {d}\n", .{snapshot.signals_delivered_total});

    try writer.print("# HELP flo_workflow_timers_fired_total Total timers fired\n", .{});
    try writer.print("# TYPE flo_workflow_timers_fired_total counter\n", .{});
    try writer.print("flo_workflow_timers_fired_total {d}\n", .{snapshot.timers_fired_total});

    try writer.print("# HELP flo_workflow_steps_executed_total Total workflow steps executed\n", .{});
    try writer.print("# TYPE flo_workflow_steps_executed_total counter\n", .{});
    try writer.print("flo_workflow_steps_executed_total {d}\n", .{snapshot.steps_executed_total});

    try writer.print("# HELP flo_workflow_active_schedules Current active scheduled workflows\n", .{});
    try writer.print("# TYPE flo_workflow_active_schedules gauge\n", .{});
    try writer.print("flo_workflow_active_schedules {d}\n", .{snapshot.active_schedules});
}

/// Write processing metrics in Prometheus format
fn writeProcessingMetrics(writer: anytype, snapshot: ProcessingMetrics.Snapshot) !void {
    try writer.print("\n# HELP flo_processing_active_jobs Current active processing jobs\n", .{});
    try writer.print("# TYPE flo_processing_active_jobs gauge\n", .{});
    try writer.print("flo_processing_active_jobs {d}\n", .{snapshot.active_jobs});

    try writer.print("# HELP flo_processing_jobs_submitted_total Total processing jobs submitted\n", .{});
    try writer.print("# TYPE flo_processing_jobs_submitted_total counter\n", .{});
    try writer.print("flo_processing_jobs_submitted_total {d}\n", .{snapshot.jobs_submitted_total});

    try writer.print("# HELP flo_processing_jobs_completed_total Total processing jobs completed\n", .{});
    try writer.print("# TYPE flo_processing_jobs_completed_total counter\n", .{});
    try writer.print("flo_processing_jobs_completed_total {d}\n", .{snapshot.jobs_completed_total});

    try writer.print("# HELP flo_processing_jobs_failed_total Total processing jobs failed\n", .{});
    try writer.print("# TYPE flo_processing_jobs_failed_total counter\n", .{});
    try writer.print("flo_processing_jobs_failed_total {d}\n", .{snapshot.jobs_failed_total});

    try writer.print("# HELP flo_processing_jobs_cancelled_total Total processing jobs cancelled\n", .{});
    try writer.print("# TYPE flo_processing_jobs_cancelled_total counter\n", .{});
    try writer.print("flo_processing_jobs_cancelled_total {d}\n", .{snapshot.jobs_cancelled_total});

    try writer.print("# HELP flo_processing_records_processed_total Total records processed\n", .{});
    try writer.print("# TYPE flo_processing_records_processed_total counter\n", .{});
    try writer.print("flo_processing_records_processed_total {d}\n", .{snapshot.records_processed_total});

    try writer.print("# HELP flo_processing_records_dropped_total Total records dropped\n", .{});
    try writer.print("# TYPE flo_processing_records_dropped_total counter\n", .{});
    try writer.print("flo_processing_records_dropped_total {d}\n", .{snapshot.records_dropped_total});

    try writer.print("# HELP flo_processing_checkpoints_completed_total Total checkpoints completed\n", .{});
    try writer.print("# TYPE flo_processing_checkpoints_completed_total counter\n", .{});
    try writer.print("flo_processing_checkpoints_completed_total {d}\n", .{snapshot.checkpoints_completed_total});

    try writer.print("# HELP flo_processing_errors_total Total processing errors\n", .{});
    try writer.print("# TYPE flo_processing_errors_total counter\n", .{});
    try writer.print("flo_processing_errors_total {d}\n", .{snapshot.errors_total});
}

/// Write replication metrics in Prometheus format
fn writeReplicationMetrics(writer: anytype, snapshot: ReplicationMetrics.Snapshot) !void {
    try writer.print("\n# HELP flo_replication_follower_gaps_total Replication gaps detected on a follower (received index > expected)\n", .{});
    try writer.print("# TYPE flo_replication_follower_gaps_total counter\n", .{});
    try writer.print("flo_replication_follower_gaps_total {d}\n", .{snapshot.follower_gaps_total});

    try writer.print("# HELP flo_replication_follower_entries_missing_total Entries inferred missing across all follower gaps\n", .{});
    try writer.print("# TYPE flo_replication_follower_entries_missing_total counter\n", .{});
    try writer.print("flo_replication_follower_entries_missing_total {d}\n", .{snapshot.follower_entries_missing_total});

    try writer.print("# HELP flo_replication_broadcast_oversize_skipped_total Over-size entries never broadcast to any peer\n", .{});
    try writer.print("# TYPE flo_replication_broadcast_oversize_skipped_total counter\n", .{});
    try writer.print("flo_replication_broadcast_oversize_skipped_total {d}\n", .{snapshot.broadcast_oversize_skipped_total});

    try writer.print("# HELP flo_replication_broadcast_send_failures_total Per-peer broadcast sends that failed and were not retried\n", .{});
    try writer.print("# TYPE flo_replication_broadcast_send_failures_total counter\n", .{});
    try writer.print("flo_replication_broadcast_send_failures_total {d}\n", .{snapshot.broadcast_send_failures_total});

    try writer.print("# HELP flo_replication_last_gap_received_index Follower index at which a gap was last observed\n", .{});
    try writer.print("# TYPE flo_replication_last_gap_received_index gauge\n", .{});
    try writer.print("flo_replication_last_gap_received_index {d}\n", .{snapshot.last_gap_received_index});
}

/// Write per-shard metrics in Prometheus format
fn writeShardMetrics(writer: anytype, snap: ShardMetrics.Snapshot) !void {
    try writer.print("flo_shard_connections{{shard_id=\"{d}\"}} {d}\n", .{ snap.shard_id, snap.connections });
    try writer.print("flo_shard_commands_total{{shard_id=\"{d}\"}} {d}\n", .{ snap.shard_id, snap.commands_total });
    try writer.print("flo_shard_bytes_received_total{{shard_id=\"{d}\"}} {d}\n", .{ snap.shard_id, snap.bytes_received });
    try writer.print("flo_shard_bytes_sent_total{{shard_id=\"{d}\"}} {d}\n", .{ snap.shard_id, snap.bytes_sent });
    try writer.print("flo_shard_subscriptions{{shard_id=\"{d}\"}} {d}\n", .{ snap.shard_id, snap.subscriptions });
    try writer.print("flo_shard_partitions{{shard_id=\"{d}\"}} {d}\n", .{ snap.shard_id, snap.partitions });
    try writer.print("flo_shard_reactor_loops_total{{shard_id=\"{d}\"}} {d}\n", .{ snap.shard_id, snap.reactor_loops });
    try writer.print("flo_shard_inbox_processed_total{{shard_id=\"{d}\"}} {d}\n", .{ snap.shard_id, snap.inbox_processed });
    try writer.print("flo_shard_inbox_pending{{shard_id=\"{d}\"}} {d}\n", .{ snap.shard_id, snap.inbox_pending });
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "registry: register and export stream metrics" {
    const allocator = testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    // Register stream
    const metrics1 = try registry.registerStream("prod", "orders", 0);
    const metrics2 = try registry.registerStream("prod", "orders", 1);
    const metrics3 = try registry.registerStream("test", "events", 0);

    // Simulate some operations
    metrics1.recordAppend(10, 1024);
    metrics2.recordAppend(5, 512);
    metrics3.recordRead(3, 256);

    // Export
    const prometheus_text = try registry.exportPrometheus(allocator);
    defer allocator.free(prometheus_text);

    // Verify output contains all streams
    try testing.expect(std.mem.indexOf(u8, prometheus_text, "namespace=\"prod\",topic=\"orders\",partition=\"0\"") != null);
    try testing.expect(std.mem.indexOf(u8, prometheus_text, "namespace=\"prod\",topic=\"orders\",partition=\"1\"") != null);
    try testing.expect(std.mem.indexOf(u8, prometheus_text, "namespace=\"test\",topic=\"events\",partition=\"0\"") != null);

    // Verify metrics values
    try testing.expect(std.mem.indexOf(u8, prometheus_text, "flo_stream_append_records_total") != null);
    try testing.expect(std.mem.indexOf(u8, prometheus_text, "flo_stream_read_records_total") != null);
}

test "registry: duplicate registration returns same metrics" {
    const allocator = testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    const metrics1 = try registry.registerStream("prod", "orders", 0);
    const metrics2 = try registry.registerStream("prod", "orders", 0);

    // Should be the same pointer
    try testing.expectEqual(metrics1, metrics2);

    // Updating one should update the other
    metrics1.recordAppend(10, 1024);
    const snap = metrics2.snapshot();
    try testing.expectEqual(@as(u64, 10), snap.append_records_total);
}

test "registry: stream count" {
    const allocator = testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    try testing.expectEqual(@as(usize, 0), registry.streamCount());

    _ = try registry.registerStream("prod", "orders", 0);
    try testing.expectEqual(@as(usize, 1), registry.streamCount());

    _ = try registry.registerStream("prod", "orders", 1);
    try testing.expectEqual(@as(usize, 2), registry.streamCount());

    // Duplicate registration doesn't increase count
    _ = try registry.registerStream("prod", "orders", 0);
    try testing.expectEqual(@as(usize, 2), registry.streamCount());
}

test "registry: per-shard metrics" {
    const allocator = testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    // Before initShards, shardMetrics returns null
    try testing.expect(registry.shardMetrics(0) == null);
    try testing.expectEqual(@as(u32, 0), registry.shardCount());

    // Initialize 4 shards
    try registry.initShards(4);
    try testing.expectEqual(@as(u32, 4), registry.shardCount());

    // Access shard metrics
    const shard0 = registry.shardMetrics(0).?;
    const shard1 = registry.shardMetrics(1).?;

    // Out of range returns null
    try testing.expect(registry.shardMetrics(4) == null);
    try testing.expect(registry.shardMetrics(100) == null);

    // Update shard counters
    shard0.connectionOpened();
    shard0.connectionOpened();
    shard0.recordCommand();
    shard0.recordBytesReceived(1024);
    shard1.connectionOpened();
    shard1.recordCommand();
    shard1.recordCommand();
    shard1.recordBytesSent(2048);

    // Verify snapshots
    const snap0 = shard0.snapshot();
    try testing.expectEqual(@as(u64, 2), snap0.connections);
    try testing.expectEqual(@as(u64, 1), snap0.commands_total);
    try testing.expectEqual(@as(u64, 1024), snap0.bytes_received);
    try testing.expectEqual(@as(u16, 0), snap0.shard_id);

    const snap1 = shard1.snapshot();
    try testing.expectEqual(@as(u64, 1), snap1.connections);
    try testing.expectEqual(@as(u64, 2), snap1.commands_total);
    try testing.expectEqual(@as(u64, 2048), snap1.bytes_sent);
    try testing.expectEqual(@as(u16, 1), snap1.shard_id);
}

test "registry: per-shard prometheus export" {
    const allocator = testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    try registry.initShards(2);

    // Add some shard activity
    const shard0 = registry.shardMetrics(0).?;
    shard0.connectionOpened();
    shard0.recordCommand();

    const shard1 = registry.shardMetrics(1).?;
    shard1.recordBytesReceived(512);

    const text = try registry.exportPrometheus(allocator);
    defer allocator.free(text);

    // Verify per-shard metrics appear
    try testing.expect(std.mem.indexOf(u8, text, "flo_shards_total 2") != null);
    try testing.expect(std.mem.indexOf(u8, text, "flo_shard_connections{shard_id=\"0\"} 1") != null);
    try testing.expect(std.mem.indexOf(u8, text, "flo_shard_commands_total{shard_id=\"0\"} 1") != null);
    try testing.expect(std.mem.indexOf(u8, text, "flo_shard_bytes_received_total{shard_id=\"1\"} 512") != null);
}

test "registry: double initShards fails" {
    const allocator = testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    try registry.initShards(4);
    try testing.expectError(error.AlreadyInitialized, registry.initShards(2));
}

test "registry: shard metrics connection open/close" {
    const allocator = testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    try registry.initShards(1);
    const shard = registry.shardMetrics(0).?;

    shard.connectionOpened();
    shard.connectionOpened();
    shard.connectionOpened();
    try testing.expectEqual(@as(u64, 3), shard.snapshot().connections);

    shard.connectionClosed();
    try testing.expectEqual(@as(u64, 2), shard.snapshot().connections);
}

test "registry: replication metrics gap + broadcast counters" {
    const allocator = testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    // Two gap events: 3 entries missing (indices 5..7) then 1 (index 9).
    registry.replication.recordFollowerGap(3, 8);
    registry.replication.recordFollowerGap(1, 10);
    registry.replication.recordOversizeSkipped();
    registry.replication.recordSendFailure();
    registry.replication.recordSendFailure();

    const snap = registry.replication.snapshot();
    try testing.expectEqual(@as(u64, 2), snap.follower_gaps_total);
    try testing.expectEqual(@as(u64, 4), snap.follower_entries_missing_total);
    try testing.expectEqual(@as(u64, 1), snap.broadcast_oversize_skipped_total);
    try testing.expectEqual(@as(u64, 2), snap.broadcast_send_failures_total);
    try testing.expectEqual(@as(u64, 10), snap.last_gap_received_index);

    const text = try registry.exportPrometheus(allocator);
    defer allocator.free(text);
    try testing.expect(std.mem.indexOf(u8, text, "flo_replication_follower_gaps_total 2") != null);
    try testing.expect(std.mem.indexOf(u8, text, "flo_replication_follower_entries_missing_total 4") != null);
    try testing.expect(std.mem.indexOf(u8, text, "flo_replication_broadcast_oversize_skipped_total 1") != null);
    try testing.expect(std.mem.indexOf(u8, text, "flo_replication_broadcast_send_failures_total 2") != null);
}

test "registry: shard metrics reactor and inbox" {
    const allocator = testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    try registry.initShards(2);
    const shard = registry.shardMetrics(0).?;

    shard.recordReactorLoop();
    shard.recordReactorLoop();
    shard.recordInboxProcessed(10);
    shard.setInboxPending(5);
    shard.setPartitions(8);

    const snap = shard.snapshot();
    try testing.expectEqual(@as(u64, 2), snap.reactor_loops);
    try testing.expectEqual(@as(u64, 10), snap.inbox_processed);
    try testing.expectEqual(@as(u64, 5), snap.inbox_pending);
    try testing.expectEqual(@as(u64, 8), snap.partitions);
}
