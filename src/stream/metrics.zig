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

    // Read metrics
    read_ops_total: Atomic(u64) = Atomic(u64).init(0),
    read_records_total: Atomic(u64) = Atomic(u64).init(0),
    read_bytes_total: Atomic(u64) = Atomic(u64).init(0),
    read_empty_total: Atomic(u64) = Atomic(u64).init(0),

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

        // Read metrics
        read_ops_total: u64,
        read_records_total: u64,
        read_bytes_total: u64,
        read_empty_total: u64,
    };

    /// Record an append operation
    pub fn recordAppend(self: *Metrics, records: u64, bytes: u64) void {
        _ = self.append_ops_total.fetchAdd(1, .monotonic);
        _ = self.append_records_total.fetchAdd(records, .monotonic);
        _ = self.append_bytes_total.fetchAdd(bytes, .monotonic);
    }

    /// Record a read operation
    pub fn recordRead(self: *Metrics, records: u64, bytes: u64) void {
        _ = self.read_ops_total.fetchAdd(1, .monotonic);
        _ = self.read_records_total.fetchAdd(records, .monotonic);
        _ = self.read_bytes_total.fetchAdd(bytes, .monotonic);
    }

    /// Record an empty read (no results)
    pub fn recordEmptyRead(self: *Metrics) void {
        _ = self.read_empty_total.fetchAdd(1, .monotonic);
    }

    /// Get a snapshot of current metrics
    pub fn snapshot(self: *const Metrics) Snapshot {
        return .{
            .append_ops_total = self.append_ops_total.load(.monotonic),
            .append_records_total = self.append_records_total.load(.monotonic),
            .append_bytes_total = self.append_bytes_total.load(.monotonic),

            .read_ops_total = self.read_ops_total.load(.monotonic),
            .read_records_total = self.read_records_total.load(.monotonic),
            .read_bytes_total = self.read_bytes_total.load(.monotonic),
            .read_empty_total = self.read_empty_total.load(.monotonic),
        };
    }

    /// Reset all counters
    pub fn reset(self: *Metrics) void {
        self.append_ops_total.store(0, .monotonic);
        self.append_records_total.store(0, .monotonic);
        self.append_bytes_total.store(0, .monotonic);

        self.read_ops_total.store(0, .monotonic);
        self.read_records_total.store(0, .monotonic);
        self.read_bytes_total.store(0, .monotonic);
        self.read_empty_total.store(0, .monotonic);
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

test "Metrics: reset" {
    var m = Metrics{};
    m.recordAppend(10, 1024);
    m.recordRead(5, 512);
    m.reset();

    const s = m.snapshot();
    try testing.expectEqual(@as(u64, 0), s.append_ops_total);
    try testing.expectEqual(@as(u64, 0), s.read_ops_total);
}
