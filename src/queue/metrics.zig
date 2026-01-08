///! Queue Metrics Module
///!
///! Provides comprehensive metrics for monitoring queue system performance and behavior.
///!
///! # Overview
///!
///! This module tracks all queue operations including enqueue/dequeue, leases, DLQ operations,
///! sweeper activity, and delayed message handling. All metrics use atomic operations for
///! thread-safety with zero allocation overhead.
///!
///! # Usage
///!
///! ## Basic Integration
///!
///! ```zig
///! const Queue = @import("queue.zig").Queue;
///! const Metrics = @import("metrics.zig").Metrics;
///!
///! // Initialize metrics
///! var metrics = Metrics.init();
///!
///! // Open queue with metrics
///! var queue = try Queue.openWithOptions(allocator, engine, "app", "orders", 0, .{
///!     .metrics = &metrics,
///! });
///! defer queue.close();
///!
///! // Use queue normally - metrics are recorded automatically
///! const seq = try queue.enqueue("order:123", "payload", 10, 0, now_ms);
///! const messages = try queue.dequeue("processor", 10, 30_000, now_ms);
///! try queue.complete("processor", &[_]u64{seq});
///!
///! // Access metrics
///! const total_enqueued = metrics.enqueue_messages_total.load(.monotonic);
///! const active_leases = metrics.leases_active_current.load(.monotonic);
///! ```
///!
///! ## Snapshot and Export
///!
///! ```zig
///! // Take consistent snapshot
///! const snap = metrics.snapshot();
///!
///! // Export as Prometheus format
///! const prom_text = try snap.formatPrometheus(allocator, "production", "orders");
///! defer allocator.free(prom_text);
///!
///! // Serve metrics endpoint
///! try response.writeAll(prom_text);
///! ```
///!
///! ## Available Metrics
///!
///! - **Enqueue**: ops, messages, bytes, delayed count, errors
///! - **Dequeue**: ops, messages, bytes, empty results, errors
///! - **Leases**: active (gauge), created, completed, failed, expired, extended
///! - **DLQ**: messages moved, requeued, deleted, current count (gauge)
///! - **Sweeper**: runs, reclaimed, orphaned, scanned, errors
///! - **Delayed**: promoted count, current delayed (gauge)
///! - **Queue State**: last_seq (gauge), available messages (gauge)
///!
///! # Design Principles
///!
///! - **Zero allocation**: All operations use atomic increments/decrements only
///! - **Thread-safe**: Safe for concurrent access via atomic operations
///! - **Prometheus-compatible**: Counter/gauge types with standard naming
///! - **Minimal overhead**: Atomic operations add <10ns per metric update
///!
///! # See Also
///!
///! - `queue.zig`: Queue implementation with integrated metrics
///! - `sweeper.zig`: Background sweeper with metrics support

const std = @import("std");
const Atomic = std.atomic.Value;

pub const Metrics = struct {
    // =========================================================================
    // Enqueue Metrics
    // =========================================================================

    /// Total number of enqueue operations
    enqueue_ops_total: Atomic(u64),

    /// Total number of messages enqueued
    enqueue_messages_total: Atomic(u64),

    /// Total bytes enqueued (header + payload)
    enqueue_bytes_total: Atomic(u64),

    /// Total delayed messages enqueued
    enqueue_delayed_total: Atomic(u64),

    /// Failed enqueue operations
    enqueue_errors_total: Atomic(u64),

    /// Enqueue rejected due to duplicate dedup key
    enqueue_dedup_rejected_total: Atomic(u64),

    // =========================================================================
    // Dequeue Metrics
    // =========================================================================

    /// Total number of dequeue operations
    dequeue_ops_total: Atomic(u64),

    /// Total number of messages dequeued
    dequeue_messages_total: Atomic(u64),

    /// Total bytes dequeued
    dequeue_bytes_total: Atomic(u64),

    /// Dequeue operations that returned empty results
    dequeue_empty_total: Atomic(u64),

    /// Failed dequeue operations
    dequeue_errors_total: Atomic(u64),

    // =========================================================================
    // Lease Metrics
    // =========================================================================

    /// Number of leases currently active (gauge)
    leases_active_current: Atomic(u64),

    /// Total leases created
    leases_created_total: Atomic(u64),

    /// Total leases completed successfully
    leases_completed_total: Atomic(u64),

    /// Total leases failed
    leases_failed_total: Atomic(u64),

    /// Total leases expired and reclaimed
    leases_expired_total: Atomic(u64),

    /// Total lease extension operations
    leases_extended_total: Atomic(u64),

    // =========================================================================
    // Dead Letter Queue (DLQ) Metrics
    // =========================================================================

    /// Total messages moved to DLQ
    dlq_messages_total: Atomic(u64),

    /// Total messages requeued from DLQ
    dlq_requeue_total: Atomic(u64),

    /// Total DLQ messages deleted
    dlq_delete_total: Atomic(u64),

    /// Current DLQ message count (gauge)
    dlq_messages_current: Atomic(u64),

    // =========================================================================
    // Sweeper Metrics
    // =========================================================================

    /// Total sweeper runs
    sweeper_runs_total: Atomic(u64),

    /// Total leases reclaimed by sweeper
    sweeper_reclaimed_total: Atomic(u64),

    /// Total orphaned leases cleaned up
    sweeper_orphaned_total: Atomic(u64),

    /// Total entries scanned by sweeper
    sweeper_scanned_total: Atomic(u64),

    /// Total sweeper errors
    sweeper_errors_total: Atomic(u64),

    // =========================================================================
    // Delayed Message Metrics
    // =========================================================================

    /// Total delayed messages promoted
    delayed_promoted_total: Atomic(u64),

    /// Current delayed message count (gauge)
    delayed_messages_current: Atomic(u64),

    // =========================================================================
    // Queue State Metrics
    // =========================================================================

    /// Current last sequence number (gauge)
    queue_last_seq: Atomic(u64),

    /// Current available message count (gauge)
    queue_available_current: Atomic(u64),

    pub fn init() Metrics {
        return Metrics{
            .enqueue_ops_total = Atomic(u64).init(0),
            .enqueue_messages_total = Atomic(u64).init(0),
            .enqueue_bytes_total = Atomic(u64).init(0),
            .enqueue_delayed_total = Atomic(u64).init(0),
            .enqueue_errors_total = Atomic(u64).init(0),
            .enqueue_dedup_rejected_total = Atomic(u64).init(0),

            .dequeue_ops_total = Atomic(u64).init(0),
            .dequeue_messages_total = Atomic(u64).init(0),
            .dequeue_bytes_total = Atomic(u64).init(0),
            .dequeue_empty_total = Atomic(u64).init(0),
            .dequeue_errors_total = Atomic(u64).init(0),

            .leases_active_current = Atomic(u64).init(0),
            .leases_created_total = Atomic(u64).init(0),
            .leases_completed_total = Atomic(u64).init(0),
            .leases_failed_total = Atomic(u64).init(0),
            .leases_expired_total = Atomic(u64).init(0),
            .leases_extended_total = Atomic(u64).init(0),

            .dlq_messages_total = Atomic(u64).init(0),
            .dlq_requeue_total = Atomic(u64).init(0),
            .dlq_delete_total = Atomic(u64).init(0),
            .dlq_messages_current = Atomic(u64).init(0),

            .sweeper_runs_total = Atomic(u64).init(0),
            .sweeper_reclaimed_total = Atomic(u64).init(0),
            .sweeper_orphaned_total = Atomic(u64).init(0),
            .sweeper_scanned_total = Atomic(u64).init(0),
            .sweeper_errors_total = Atomic(u64).init(0),

            .delayed_promoted_total = Atomic(u64).init(0),
            .delayed_messages_current = Atomic(u64).init(0),

            .queue_last_seq = Atomic(u64).init(0),
            .queue_available_current = Atomic(u64).init(0),
        };
    }

    // =========================================================================
    // Metric Update Methods
    // =========================================================================

    /// Record a successful enqueue operation
    /// Note: queue_available_current is updated by updateAvailableCount() from metadata
    pub fn recordEnqueue(self: *Metrics, message_count: usize, byte_count: usize, is_delayed: bool) void {
        _ = self.enqueue_ops_total.fetchAdd(1, .monotonic);
        _ = self.enqueue_messages_total.fetchAdd(message_count, .monotonic);
        _ = self.enqueue_bytes_total.fetchAdd(byte_count, .monotonic);
        if (is_delayed) {
            _ = self.enqueue_delayed_total.fetchAdd(message_count, .monotonic);
            _ = self.delayed_messages_current.fetchAdd(message_count, .monotonic);
        }
        // Note: queue_available_current is updated by updateAvailableCount() from queue metadata
    }

    /// Record an enqueue error
    pub fn recordEnqueueError(self: *Metrics) void {
        _ = self.enqueue_errors_total.fetchAdd(1, .monotonic);
    }

    /// Record a dedup rejection (duplicate message)
    pub fn recordDedupRejected(self: *Metrics) void {
        _ = self.enqueue_dedup_rejected_total.fetchAdd(1, .monotonic);
    }

    /// Record a successful dequeue operation
    /// Note: queue_available_current is updated by updateAvailableCount() from metadata
    pub fn recordDequeue(self: *Metrics, message_count: usize, byte_count: usize) void {
        _ = self.dequeue_ops_total.fetchAdd(1, .monotonic);
        _ = self.dequeue_messages_total.fetchAdd(message_count, .monotonic);
        _ = self.dequeue_bytes_total.fetchAdd(byte_count, .monotonic);
        // Note: queue_available_current is updated by updateAvailableCount() from queue metadata
        _ = self.leases_active_current.fetchAdd(message_count, .monotonic);
        _ = self.leases_created_total.fetchAdd(message_count, .monotonic);
    }

    /// Record an empty dequeue
    pub fn recordEmptyDequeue(self: *Metrics) void {
        _ = self.dequeue_ops_total.fetchAdd(1, .monotonic);
        _ = self.dequeue_empty_total.fetchAdd(1, .monotonic);
    }

    /// Record a dequeue error
    pub fn recordDequeueError(self: *Metrics) void {
        _ = self.dequeue_errors_total.fetchAdd(1, .monotonic);
    }

    /// Record lease completion
    pub fn recordComplete(self: *Metrics, count: usize) void {
        _ = self.leases_completed_total.fetchAdd(count, .monotonic);
        // Use saturating subtraction to prevent underflow
        const current = self.leases_active_current.load(.monotonic);
        if (current >= count) {
            _ = self.leases_active_current.fetchSub(count, .monotonic);
        } else {
            self.leases_active_current.store(0, .monotonic);
        }
    }

    /// Record lease failure (moved to DLQ)
    pub fn recordFail(self: *Metrics, count: usize) void {
        _ = self.leases_failed_total.fetchAdd(count, .monotonic);
        // Use saturating subtraction to prevent underflow
        const current = self.leases_active_current.load(.monotonic);
        if (current >= count) {
            _ = self.leases_active_current.fetchSub(count, .monotonic);
        } else {
            self.leases_active_current.store(0, .monotonic);
        }
        _ = self.dlq_messages_total.fetchAdd(count, .monotonic);
        _ = self.dlq_messages_current.fetchAdd(count, .monotonic);
    }

    /// Record lease extension
    pub fn recordExtend(self: *Metrics, count: usize) void {
        _ = self.leases_extended_total.fetchAdd(count, .monotonic);
    }

    /// Record DLQ requeue
    pub fn recordDLQRequeue(self: *Metrics, count: usize) void {
        _ = self.dlq_requeue_total.fetchAdd(count, .monotonic);
        // Use saturating subtraction to prevent underflow
        const current = self.dlq_messages_current.load(.monotonic);
        if (current >= count) {
            _ = self.dlq_messages_current.fetchSub(count, .monotonic);
        } else {
            self.dlq_messages_current.store(0, .monotonic);
        }
        _ = self.queue_available_current.fetchAdd(count, .monotonic);
    }

    /// Record DLQ delete
    pub fn recordDLQDelete(self: *Metrics, count: usize) void {
        _ = self.dlq_delete_total.fetchAdd(count, .monotonic);
        // Use saturating subtraction to prevent underflow
        const current = self.dlq_messages_current.load(.monotonic);
        if (current >= count) {
            _ = self.dlq_messages_current.fetchSub(count, .monotonic);
        } else {
            self.dlq_messages_current.store(0, .monotonic);
        }
    }

    /// Record sweeper run
    pub fn recordSweeperRun(self: *Metrics, reclaimed: usize, orphaned: usize, scanned: usize) void {
        _ = self.sweeper_runs_total.fetchAdd(1, .monotonic);
        _ = self.sweeper_reclaimed_total.fetchAdd(reclaimed, .monotonic);
        _ = self.sweeper_orphaned_total.fetchAdd(orphaned, .monotonic);
        _ = self.sweeper_scanned_total.fetchAdd(scanned, .monotonic);
        _ = self.leases_expired_total.fetchAdd(reclaimed + orphaned, .monotonic);
        // Use saturating subtraction to prevent underflow
        const total_expired = reclaimed + orphaned;
        const current = self.leases_active_current.load(.monotonic);
        if (current >= total_expired) {
            _ = self.leases_active_current.fetchSub(total_expired, .monotonic);
        } else {
            self.leases_active_current.store(0, .monotonic);
        }
        _ = self.queue_available_current.fetchAdd(reclaimed, .monotonic);
    }

    /// Record sweeper error
    pub fn recordSweeperError(self: *Metrics) void {
        _ = self.sweeper_errors_total.fetchAdd(1, .monotonic);
    }

    /// Record delayed message promotion
    /// Note: queue_available_current is updated by updateAvailableCount() from metadata
    pub fn recordPromoteDue(self: *Metrics, count: usize) void {
        _ = self.delayed_promoted_total.fetchAdd(count, .monotonic);
        // Use saturating subtraction to prevent underflow
        const current = self.delayed_messages_current.load(.monotonic);
        if (current >= count) {
            _ = self.delayed_messages_current.fetchSub(count, .monotonic);
        } else {
            self.delayed_messages_current.store(0, .monotonic);
        }
        // Note: queue_available_current is updated by updateAvailableCount() from queue metadata
    }

    /// Update queue last_seq gauge
    pub fn updateLastSeq(self: *Metrics, seq: u64) void {
        self.queue_last_seq.store(seq, .monotonic);
    }

    /// Update queue available count gauge
    pub fn updateAvailableCount(self: *Metrics, count: u64) void {
        self.queue_available_current.store(count, .monotonic);
    }

    // =========================================================================
    // Snapshot & Export
    // =========================================================================

    /// Snapshot of all metrics at a point in time
    pub const Snapshot = struct {
        // Enqueue metrics
        enqueue_ops_total: u64,
        enqueue_messages_total: u64,
        enqueue_bytes_total: u64,
        enqueue_delayed_total: u64,
        enqueue_errors_total: u64,
        enqueue_dedup_rejected_total: u64,

        // Dequeue metrics
        dequeue_ops_total: u64,
        dequeue_messages_total: u64,
        dequeue_bytes_total: u64,
        dequeue_empty_total: u64,
        dequeue_errors_total: u64,

        // Lease metrics
        leases_active_current: u64,
        leases_created_total: u64,
        leases_completed_total: u64,
        leases_failed_total: u64,
        leases_expired_total: u64,
        leases_extended_total: u64,

        // DLQ metrics
        dlq_messages_total: u64,
        dlq_requeue_total: u64,
        dlq_delete_total: u64,
        dlq_messages_current: u64,

        // Sweeper metrics
        sweeper_runs_total: u64,
        sweeper_reclaimed_total: u64,
        sweeper_orphaned_total: u64,
        sweeper_scanned_total: u64,
        sweeper_errors_total: u64,

        // Delayed message metrics
        delayed_promoted_total: u64,
        delayed_messages_current: u64,

        // Queue state
        queue_last_seq: u64,
        queue_available_current: u64,

        /// Format snapshot as Prometheus-style text
        pub fn formatPrometheus(self: Snapshot, allocator: std.mem.Allocator, namespace: []const u8, queue: []const u8) ![]u8 {
            var buf: std.ArrayListUnmanaged(u8) = .{};
            errdefer buf.deinit(allocator);

            const writer = buf.writer(allocator);

            // Enqueue metrics
            try writer.print("flo_queue_enqueue_ops_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.enqueue_ops_total });
            try writer.print("flo_queue_enqueue_messages_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.enqueue_messages_total });
            try writer.print("flo_queue_enqueue_bytes_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.enqueue_bytes_total });
            try writer.print("flo_queue_enqueue_delayed_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.enqueue_delayed_total });
            try writer.print("flo_queue_enqueue_errors_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.enqueue_errors_total });
            try writer.print("flo_queue_enqueue_dedup_rejected_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.enqueue_dedup_rejected_total });

            // Dequeue metrics
            try writer.print("flo_queue_dequeue_ops_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.dequeue_ops_total });
            try writer.print("flo_queue_dequeue_messages_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.dequeue_messages_total });
            try writer.print("flo_queue_dequeue_bytes_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.dequeue_bytes_total });
            try writer.print("flo_queue_dequeue_empty_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.dequeue_empty_total });
            try writer.print("flo_queue_dequeue_errors_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.dequeue_errors_total });

            // Lease metrics
            try writer.print("flo_queue_leases_active_current{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.leases_active_current });
            try writer.print("flo_queue_leases_created_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.leases_created_total });
            try writer.print("flo_queue_leases_completed_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.leases_completed_total });
            try writer.print("flo_queue_leases_failed_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.leases_failed_total });
            try writer.print("flo_queue_leases_expired_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.leases_expired_total });
            try writer.print("flo_queue_leases_extended_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.leases_extended_total });

            // DLQ metrics
            try writer.print("flo_queue_dlq_messages_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.dlq_messages_total });
            try writer.print("flo_queue_dlq_requeue_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.dlq_requeue_total });
            try writer.print("flo_queue_dlq_delete_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.dlq_delete_total });
            try writer.print("flo_queue_dlq_messages_current{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.dlq_messages_current });

            // Sweeper metrics
            try writer.print("flo_queue_sweeper_runs_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.sweeper_runs_total });
            try writer.print("flo_queue_sweeper_reclaimed_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.sweeper_reclaimed_total });
            try writer.print("flo_queue_sweeper_orphaned_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.sweeper_orphaned_total });
            try writer.print("flo_queue_sweeper_scanned_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.sweeper_scanned_total });
            try writer.print("flo_queue_sweeper_errors_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.sweeper_errors_total });

            // Delayed message metrics
            try writer.print("flo_queue_delayed_promoted_total{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.delayed_promoted_total });
            try writer.print("flo_queue_delayed_messages_current{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.delayed_messages_current });

            // Queue state
            try writer.print("flo_queue_last_seq{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.queue_last_seq });
            try writer.print("flo_queue_available_current{{namespace=\"{s}\",queue=\"{s}\"}} {d}\n", .{ namespace, queue, self.queue_available_current });

            return buf.toOwnedSlice(allocator);
        }
    };

    /// Capture a consistent snapshot of all metrics
    pub fn snapshot(self: *const Metrics) Snapshot {
        return Snapshot{
            .enqueue_ops_total = self.enqueue_ops_total.load(.monotonic),
            .enqueue_messages_total = self.enqueue_messages_total.load(.monotonic),
            .enqueue_bytes_total = self.enqueue_bytes_total.load(.monotonic),
            .enqueue_delayed_total = self.enqueue_delayed_total.load(.monotonic),
            .enqueue_errors_total = self.enqueue_errors_total.load(.monotonic),
            .enqueue_dedup_rejected_total = self.enqueue_dedup_rejected_total.load(.monotonic),

            .dequeue_ops_total = self.dequeue_ops_total.load(.monotonic),
            .dequeue_messages_total = self.dequeue_messages_total.load(.monotonic),
            .dequeue_bytes_total = self.dequeue_bytes_total.load(.monotonic),
            .dequeue_empty_total = self.dequeue_empty_total.load(.monotonic),
            .dequeue_errors_total = self.dequeue_errors_total.load(.monotonic),

            .leases_active_current = self.leases_active_current.load(.monotonic),
            .leases_created_total = self.leases_created_total.load(.monotonic),
            .leases_completed_total = self.leases_completed_total.load(.monotonic),
            .leases_failed_total = self.leases_failed_total.load(.monotonic),
            .leases_expired_total = self.leases_expired_total.load(.monotonic),
            .leases_extended_total = self.leases_extended_total.load(.monotonic),

            .dlq_messages_total = self.dlq_messages_total.load(.monotonic),
            .dlq_requeue_total = self.dlq_requeue_total.load(.monotonic),
            .dlq_delete_total = self.dlq_delete_total.load(.monotonic),
            .dlq_messages_current = self.dlq_messages_current.load(.monotonic),

            .sweeper_runs_total = self.sweeper_runs_total.load(.monotonic),
            .sweeper_reclaimed_total = self.sweeper_reclaimed_total.load(.monotonic),
            .sweeper_orphaned_total = self.sweeper_orphaned_total.load(.monotonic),
            .sweeper_scanned_total = self.sweeper_scanned_total.load(.monotonic),
            .sweeper_errors_total = self.sweeper_errors_total.load(.monotonic),

            .delayed_promoted_total = self.delayed_promoted_total.load(.monotonic),
            .delayed_messages_current = self.delayed_messages_current.load(.monotonic),

            .queue_last_seq = self.queue_last_seq.load(.monotonic),
            .queue_available_current = self.queue_available_current.load(.monotonic),
        };
    }
};
