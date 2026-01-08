//! Stream Metrics Collection
//!
//! Simple counters and gauges for stream operations.
//! Thread-safe via atomic operations.
//!
//! This module provides metrics compatible with the metrics registry for
//! Prometheus-style monitoring.

const std = @import("std");
const Atomic = std.atomic.Value;

/// Metrics for a single log/partition
/// Field names match Prometheus export expectations
pub const Metrics = struct {
    // Append metrics
    append_ops_total: Atomic(u64) = Atomic(u64).init(0),
    append_records_total: Atomic(u64) = Atomic(u64).init(0),
    append_bytes_total: Atomic(u64) = Atomic(u64).init(0),
    append_errors_total: Atomic(u64) = Atomic(u64).init(0),
    append_oversized_total: Atomic(u64) = Atomic(u64).init(0),

    // Read metrics
    read_ops_total: Atomic(u64) = Atomic(u64).init(0),
    read_records_total: Atomic(u64) = Atomic(u64).init(0),
    read_bytes_total: Atomic(u64) = Atomic(u64).init(0),
    read_errors_total: Atomic(u64) = Atomic(u64).init(0),
    read_empty_total: Atomic(u64) = Atomic(u64).init(0),

    // Blocking read metrics
    blocking_readers_current: Atomic(u64) = Atomic(u64).init(0),
    blocking_read_waits_total: Atomic(u64) = Atomic(u64).init(0),
    blocking_read_timeouts_total: Atomic(u64) = Atomic(u64).init(0),
    blocking_read_wakeups_total: Atomic(u64) = Atomic(u64).init(0),

    // Consumer group metrics
    consumer_group_reads_total: Atomic(u64) = Atomic(u64).init(0),
    consumer_group_claims_total: Atomic(u64) = Atomic(u64).init(0),
    consumer_group_acks_total: Atomic(u64) = Atomic(u64).init(0),
    consumer_group_autoclaims_total: Atomic(u64) = Atomic(u64).init(0),
    consumer_group_pending_current: Atomic(u64) = Atomic(u64).init(0),

    // Stream state
    stream_last_seq: Atomic(u64) = Atomic(u64).init(0),
    trim_ops_total: Atomic(u64) = Atomic(u64).init(0),
    trim_records_total: Atomic(u64) = Atomic(u64).init(0),
    trim_bytes_total: Atomic(u64) = Atomic(u64).init(0),

    /// Initialize with default values
    pub fn init() Metrics {
        return .{};
    }

    /// Non-atomic snapshot of metrics (for reporting)
    pub const Snapshot = struct {
        // Append metrics
        append_ops_total: u64,
        append_records_total: u64,
        append_bytes_total: u64,
        append_errors_total: u64,
        append_oversized_total: u64,

        // Read metrics
        read_ops_total: u64,
        read_records_total: u64,
        read_bytes_total: u64,
        read_errors_total: u64,
        read_empty_total: u64,

        // Blocking read metrics
        blocking_readers_current: u64,
        blocking_read_waits_total: u64,
        blocking_read_timeouts_total: u64,
        blocking_read_wakeups_total: u64,

        // Consumer group metrics
        consumer_group_reads_total: u64,
        consumer_group_claims_total: u64,
        consumer_group_acks_total: u64,
        consumer_group_autoclaims_total: u64,
        consumer_group_pending_current: u64,

        // Stream state
        stream_last_seq: u64,
        trim_ops_total: u64,
        trim_records_total: u64,
        trim_bytes_total: u64,
    };

    /// Record an append operation
    pub fn recordAppend(self: *Metrics, records: u64, bytes: u64) void {
        _ = self.append_ops_total.fetchAdd(1, .monotonic);
        _ = self.append_records_total.fetchAdd(records, .monotonic);
        _ = self.append_bytes_total.fetchAdd(bytes, .monotonic);
    }

    /// Record an append error
    pub fn recordAppendError(self: *Metrics) void {
        _ = self.append_errors_total.fetchAdd(1, .monotonic);
    }

    /// Record an oversized append attempt
    pub fn recordOversized(self: *Metrics) void {
        _ = self.append_oversized_total.fetchAdd(1, .monotonic);
    }

    /// Record a read operation
    pub fn recordRead(self: *Metrics, records: u64, bytes: u64) void {
        _ = self.read_ops_total.fetchAdd(1, .monotonic);
        _ = self.read_records_total.fetchAdd(records, .monotonic);
        _ = self.read_bytes_total.fetchAdd(bytes, .monotonic);
    }

    /// Record a read error
    pub fn recordReadError(self: *Metrics) void {
        _ = self.read_errors_total.fetchAdd(1, .monotonic);
    }

    /// Record an empty read (no results)
    pub fn recordEmptyRead(self: *Metrics) void {
        _ = self.read_empty_total.fetchAdd(1, .monotonic);
    }

    /// Increment blocking readers count
    pub fn incWaiting(self: *Metrics) void {
        _ = self.blocking_readers_current.fetchAdd(1, .monotonic);
        _ = self.blocking_read_waits_total.fetchAdd(1, .monotonic);
    }

    /// Decrement blocking readers count
    pub fn decWaiting(self: *Metrics) void {
        _ = self.blocking_readers_current.fetchSub(1, .monotonic);
    }

    /// Record a blocking read timeout
    pub fn recordTimeout(self: *Metrics) void {
        _ = self.blocking_read_timeouts_total.fetchAdd(1, .monotonic);
    }

    /// Record a blocking read wakeup
    pub fn recordWakeup(self: *Metrics) void {
        _ = self.blocking_read_wakeups_total.fetchAdd(1, .monotonic);
    }

    /// Record consumer group read
    pub fn recordGroupRead(self: *Metrics, records: u64) void {
        _ = self.consumer_group_reads_total.fetchAdd(records, .monotonic);
    }

    /// Record consumer group claim
    pub fn recordClaim(self: *Metrics, count: u64) void {
        _ = self.consumer_group_claims_total.fetchAdd(count, .monotonic);
    }

    /// Record ACKs
    pub fn recordAck(self: *Metrics, count: u64) void {
        _ = self.consumer_group_acks_total.fetchAdd(count, .monotonic);
        _ = self.consumer_group_pending_current.fetchSub(@min(count, self.consumer_group_pending_current.load(.monotonic)), .monotonic);
    }

    /// Record NACKs (just track pending, no separate counter)
    pub fn recordNack(self: *Metrics, count: u64) void {
        _ = self;
        _ = count;
        // NACKs just return to pending, no counter needed
    }

    /// Record auto-claim
    pub fn recordAutoclaim(self: *Metrics, count: u64) void {
        _ = self.consumer_group_autoclaims_total.fetchAdd(count, .monotonic);
    }

    /// Record a pending message
    pub fn recordPending(self: *Metrics, count: u64) void {
        _ = self.consumer_group_pending_current.fetchAdd(count, .monotonic);
    }

    /// Update last sequence number
    pub fn updateLastSeq(self: *Metrics, seq: u64) void {
        self.stream_last_seq.store(seq, .monotonic);
    }

    /// Record trimmed events
    pub fn recordTrim(self: *Metrics, records: u64, bytes: u64) void {
        _ = self.trim_ops_total.fetchAdd(1, .monotonic);
        _ = self.trim_records_total.fetchAdd(records, .monotonic);
        _ = self.trim_bytes_total.fetchAdd(bytes, .monotonic);
    }

    /// Get a snapshot of current metrics
    pub fn snapshot(self: *const Metrics) Snapshot {
        return .{
            .append_ops_total = self.append_ops_total.load(.monotonic),
            .append_records_total = self.append_records_total.load(.monotonic),
            .append_bytes_total = self.append_bytes_total.load(.monotonic),
            .append_errors_total = self.append_errors_total.load(.monotonic),
            .append_oversized_total = self.append_oversized_total.load(.monotonic),

            .read_ops_total = self.read_ops_total.load(.monotonic),
            .read_records_total = self.read_records_total.load(.monotonic),
            .read_bytes_total = self.read_bytes_total.load(.monotonic),
            .read_errors_total = self.read_errors_total.load(.monotonic),
            .read_empty_total = self.read_empty_total.load(.monotonic),

            .blocking_readers_current = self.blocking_readers_current.load(.monotonic),
            .blocking_read_waits_total = self.blocking_read_waits_total.load(.monotonic),
            .blocking_read_timeouts_total = self.blocking_read_timeouts_total.load(.monotonic),
            .blocking_read_wakeups_total = self.blocking_read_wakeups_total.load(.monotonic),

            .consumer_group_reads_total = self.consumer_group_reads_total.load(.monotonic),
            .consumer_group_claims_total = self.consumer_group_claims_total.load(.monotonic),
            .consumer_group_acks_total = self.consumer_group_acks_total.load(.monotonic),
            .consumer_group_autoclaims_total = self.consumer_group_autoclaims_total.load(.monotonic),
            .consumer_group_pending_current = self.consumer_group_pending_current.load(.monotonic),

            .stream_last_seq = self.stream_last_seq.load(.monotonic),
            .trim_ops_total = self.trim_ops_total.load(.monotonic),
            .trim_records_total = self.trim_records_total.load(.monotonic),
            .trim_bytes_total = self.trim_bytes_total.load(.monotonic),
        };
    }

    /// Reset all counters
    pub fn reset(self: *Metrics) void {
        self.append_ops_total.store(0, .monotonic);
        self.append_records_total.store(0, .monotonic);
        self.append_bytes_total.store(0, .monotonic);
        self.append_errors_total.store(0, .monotonic);
        self.append_oversized_total.store(0, .monotonic);

        self.read_ops_total.store(0, .monotonic);
        self.read_records_total.store(0, .monotonic);
        self.read_bytes_total.store(0, .monotonic);
        self.read_errors_total.store(0, .monotonic);
        self.read_empty_total.store(0, .monotonic);

        self.blocking_readers_current.store(0, .monotonic);
        self.blocking_read_waits_total.store(0, .monotonic);
        self.blocking_read_timeouts_total.store(0, .monotonic);
        self.blocking_read_wakeups_total.store(0, .monotonic);

        self.consumer_group_reads_total.store(0, .monotonic);
        self.consumer_group_claims_total.store(0, .monotonic);
        self.consumer_group_acks_total.store(0, .monotonic);
        self.consumer_group_autoclaims_total.store(0, .monotonic);
        self.consumer_group_pending_current.store(0, .monotonic);

        self.stream_last_seq.store(0, .monotonic);
        self.trim_ops_total.store(0, .monotonic);
        self.trim_records_total.store(0, .monotonic);
        self.trim_bytes_total.store(0, .monotonic);
    }
};

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "Metrics: record append" {
    var m = Metrics{};
    m.recordAppend(10, 1024);
    m.recordAppend(5, 512);

    const s = m.snapshot();
    try testing.expectEqual(@as(u64, 2), s.append_ops_total);
    try testing.expectEqual(@as(u64, 15), s.append_records_total);
    try testing.expectEqual(@as(u64, 1536), s.append_bytes_total);
}

test "Metrics: record read" {
    var m = Metrics{};
    m.recordRead(100, 8192);
    m.recordEmptyRead();

    const s = m.snapshot();
    try testing.expectEqual(@as(u64, 1), s.read_ops_total);
    try testing.expectEqual(@as(u64, 100), s.read_records_total);
    try testing.expectEqual(@as(u64, 8192), s.read_bytes_total);
    try testing.expectEqual(@as(u64, 1), s.read_empty_total);
}

test "Metrics: blocking reads" {
    var m = Metrics{};
    m.incWaiting();
    m.incWaiting();
    m.decWaiting();
    m.recordTimeout();
    m.recordWakeup();

    const s = m.snapshot();
    try testing.expectEqual(@as(u64, 1), s.blocking_readers_current);
    try testing.expectEqual(@as(u64, 2), s.blocking_read_waits_total);
    try testing.expectEqual(@as(u64, 1), s.blocking_read_timeouts_total);
    try testing.expectEqual(@as(u64, 1), s.blocking_read_wakeups_total);
}

test "Metrics: consumer groups" {
    var m = Metrics{};
    m.recordPending(10);
    m.recordGroupRead(5);
    m.recordAck(3);
    m.recordClaim(2);

    const s = m.snapshot();
    try testing.expectEqual(@as(u64, 7), s.consumer_group_pending_current);
    try testing.expectEqual(@as(u64, 5), s.consumer_group_reads_total);
    try testing.expectEqual(@as(u64, 3), s.consumer_group_acks_total);
    try testing.expectEqual(@as(u64, 2), s.consumer_group_claims_total);
}

test "Metrics: trim" {
    var m = Metrics{};
    m.recordTrim(100, 4096);
    m.recordTrim(50, 2048);

    const s = m.snapshot();
    try testing.expectEqual(@as(u64, 2), s.trim_ops_total);
    try testing.expectEqual(@as(u64, 150), s.trim_records_total);
    try testing.expectEqual(@as(u64, 6144), s.trim_bytes_total);
}

test "Metrics: reset" {
    var m = Metrics{};
    m.recordAppend(10, 1024);
    m.recordRead(5, 512);
    m.reset();

    const s = m.snapshot();
    try testing.expectEqual(@as(u64, 0), s.append_ops_total);
    try testing.expectEqual(@as(u64, 0), s.read_ops_total);
}
