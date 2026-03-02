//! Processing Metrics
//!
//! Centralized metrics aggregation for the stream processing engine.
//! Collects per-operator, per-pipeline, and per-window metrics.
//!
//! Design goals:
//! - Zero-allocation in the hot path (pre-allocated counters)
//! - Lock-free per-shard (each shard has its own ProcessingMetrics)
//! - Snapshot-friendly for checkpoint integration

const std = @import("std");

// =============================================================================
// LatencyHistogram - Simple fixed-bucket histogram
// =============================================================================

/// Fixed-bucket latency histogram for measuring processing times.
/// Buckets: [0-1ms), [1-5ms), [5-10ms), [10-50ms), [50-100ms), [100-500ms), [500ms+)
pub const LatencyHistogram = struct {
    buckets: [7]u64 = .{ 0, 0, 0, 0, 0, 0, 0 },
    total_ns: u64 = 0,
    count: u64 = 0,
    min_ns: u64 = std.math.maxInt(u64),
    max_ns: u64 = 0,

    const bucket_boundaries_ns = [_]u64{
        1_000_000, // 1ms
        5_000_000, // 5ms
        10_000_000, // 10ms
        50_000_000, // 50ms
        100_000_000, // 100ms
        500_000_000, // 500ms
        // anything above → bucket[6]
    };

    pub fn record(self: *LatencyHistogram, latency_ns: u64) void {
        self.total_ns += latency_ns;
        self.count += 1;
        self.min_ns = @min(self.min_ns, latency_ns);
        self.max_ns = @max(self.max_ns, latency_ns);

        // Find bucket
        for (bucket_boundaries_ns, 0..) |boundary, i| {
            if (latency_ns < boundary) {
                self.buckets[i] += 1;
                return;
            }
        }
        self.buckets[6] += 1; // overflow bucket
    }

    pub fn meanNs(self: *const LatencyHistogram) u64 {
        if (self.count == 0) return 0;
        return self.total_ns / self.count;
    }

    pub fn reset(self: *LatencyHistogram) void {
        self.buckets = .{ 0, 0, 0, 0, 0, 0, 0 };
        self.total_ns = 0;
        self.count = 0;
        self.min_ns = std.math.maxInt(u64);
        self.max_ns = 0;
    }
};

// =============================================================================
// ThroughputCounter - Records per second measurement
// =============================================================================

/// Sliding-window throughput counter (counts per interval).
pub const ThroughputCounter = struct {
    count: u64 = 0,
    interval_start_ms: i64 = std.math.minInt(i64),
    last_rate: f64 = 0,
    interval_ms: i64,

    pub fn init(interval_ms: i64) ThroughputCounter {
        return .{ .interval_ms = interval_ms };
    }

    /// Increment the counter. Returns true if interval rolled over.
    pub fn increment(self: *ThroughputCounter, current_time_ms: i64) bool {
        if (self.interval_start_ms == std.math.minInt(i64)) {
            self.interval_start_ms = current_time_ms;
        }

        self.count += 1;

        if (current_time_ms - self.interval_start_ms >= self.interval_ms) {
            const elapsed_s = @as(f64, @floatFromInt(current_time_ms - self.interval_start_ms)) / 1000.0;
            if (elapsed_s > 0) {
                self.last_rate = @as(f64, @floatFromInt(self.count)) / elapsed_s;
            }
            self.count = 0;
            self.interval_start_ms = current_time_ms;
            return true;
        }
        return false;
    }

    /// Get the last computed rate (records per second)
    pub fn rate(self: *const ThroughputCounter) f64 {
        return self.last_rate;
    }
};

// =============================================================================
// BackpressureGauge
// =============================================================================

/// Tracks backpressure state for an operator.
pub const BackpressureGauge = struct {
    /// Ratio of time spent waiting vs processing (0.0 to 1.0)
    busy_ratio: f64 = 0,
    /// Total time spent busy (ns)
    busy_ns: u64 = 0,
    /// Total time spent idle/waiting (ns)
    idle_ns: u64 = 0,

    pub fn recordBusy(self: *BackpressureGauge, duration_ns: u64) void {
        self.busy_ns += duration_ns;
        self.updateRatio();
    }

    pub fn recordIdle(self: *BackpressureGauge, duration_ns: u64) void {
        self.idle_ns += duration_ns;
        self.updateRatio();
    }

    fn updateRatio(self: *BackpressureGauge) void {
        const total = self.busy_ns + self.idle_ns;
        if (total == 0) {
            self.busy_ratio = 0;
        } else {
            self.busy_ratio = @as(f64, @floatFromInt(self.busy_ns)) / @as(f64, @floatFromInt(total));
        }
    }

    /// True if operator is backpressured (busy > 80% of time)
    pub fn isBackpressured(self: *const BackpressureGauge) bool {
        return self.busy_ratio > 0.8;
    }

    pub fn reset(self: *BackpressureGauge) void {
        self.busy_ratio = 0;
        self.busy_ns = 0;
        self.idle_ns = 0;
    }
};

// =============================================================================
// PipelineMetrics - Aggregate metrics for a processing pipeline
// =============================================================================

/// Aggregated metrics for an entire processing pipeline.
pub const PipelineMetrics = struct {
    /// Input throughput (records/sec at source)
    input_throughput: ThroughputCounter,
    /// Output throughput (records/sec at sink)
    output_throughput: ThroughputCounter,
    /// End-to-end processing latency
    e2e_latency: LatencyHistogram = .{},
    /// Watermark lag (ms behind real time)
    watermark_lag_ms: i64 = 0,
    /// Number of checkpoints completed
    checkpoints_completed: u64 = 0,
    /// Last checkpoint duration (ms)
    last_checkpoint_duration_ms: i64 = 0,
    /// Records dropped (late data, errors)
    records_dropped: u64 = 0,

    pub fn init(throughput_interval_ms: i64) PipelineMetrics {
        return .{
            .input_throughput = ThroughputCounter.init(throughput_interval_ms),
            .output_throughput = ThroughputCounter.init(throughput_interval_ms),
        };
    }

    pub fn recordInput(self: *PipelineMetrics, current_time_ms: i64) void {
        _ = self.input_throughput.increment(current_time_ms);
    }

    pub fn recordOutput(self: *PipelineMetrics, current_time_ms: i64) void {
        _ = self.output_throughput.increment(current_time_ms);
    }

    pub fn recordCheckpoint(self: *PipelineMetrics, duration_ms: i64) void {
        self.checkpoints_completed += 1;
        self.last_checkpoint_duration_ms = duration_ms;
    }

    pub fn recordDrop(self: *PipelineMetrics) void {
        self.records_dropped += 1;
    }

    pub fn updateWatermarkLag(self: *PipelineMetrics, watermark_ms: i64, current_time_ms: i64) void {
        self.watermark_lag_ms = current_time_ms - watermark_ms;
    }
};

// =============================================================================
// ThroughputHistory - Ring buffer of recent throughput data points
// =============================================================================

/// Fixed-size ring buffer storing the last N throughput data points.
/// Each point is a (timestamp_ms, records_per_sec) pair, suitable for
/// sparkline charts in the dashboard UI.
pub const ThroughputHistory = struct {
    const CAPACITY = 60; // last 60 data points (~1 min at 1/sec)

    entries: [CAPACITY]ThroughputPoint = [_]ThroughputPoint{.{ .timestamp_ms = 0, .records_per_sec = 0 }} ** CAPACITY,
    head: usize = 0,
    count: usize = 0,

    pub const ThroughputPoint = struct {
        timestamp_ms: i64,
        records_per_sec: f64,
    };

    /// Append a data point. Overwrites oldest entry when buffer is full.
    pub fn push(self: *ThroughputHistory, timestamp_ms: i64, records_per_sec: f64) void {
        self.entries[self.head] = .{
            .timestamp_ms = timestamp_ms,
            .records_per_sec = records_per_sec,
        };
        self.head = (self.head + 1) % CAPACITY;
        if (self.count < CAPACITY) self.count += 1;
    }

    /// Return number of data points stored.
    pub fn len(self: *const ThroughputHistory) usize {
        return self.count;
    }

    /// Get data point at logical index (0 = oldest).
    pub fn get(self: *const ThroughputHistory, index: usize) ?ThroughputPoint {
        if (index >= self.count) return null;
        const start = if (self.count < CAPACITY) 0 else self.head;
        const actual = (start + index) % CAPACITY;
        return self.entries[actual];
    }
};

// =============================================================================
// Tests
// =============================================================================

test "LatencyHistogram basic" {
    var h = LatencyHistogram{};

    h.record(500_000); // 0.5ms → bucket[0]
    h.record(3_000_000); // 3ms → bucket[1]
    h.record(800_000_000); // 800ms → bucket[6]

    try std.testing.expectEqual(@as(u64, 3), h.count);
    try std.testing.expectEqual(@as(u64, 500_000), h.min_ns);
    try std.testing.expectEqual(@as(u64, 800_000_000), h.max_ns);
    try std.testing.expectEqual(@as(u64, 1), h.buckets[0]); // <1ms
    try std.testing.expectEqual(@as(u64, 1), h.buckets[1]); // 1-5ms
    try std.testing.expectEqual(@as(u64, 1), h.buckets[6]); // 500ms+
}

test "LatencyHistogram mean" {
    var h = LatencyHistogram{};
    h.record(2_000_000); // 2ms
    h.record(4_000_000); // 4ms
    try std.testing.expectEqual(@as(u64, 3_000_000), h.meanNs()); // 3ms mean
}

test "ThroughputCounter rate calculation" {
    var tc = ThroughputCounter.init(1000); // 1-second intervals
    _ = tc.increment(100);
    _ = tc.increment(300);
    _ = tc.increment(500);
    _ = tc.increment(700);
    _ = tc.increment(900);
    const rolled = tc.increment(1200); // past interval

    try std.testing.expect(rolled);
    // 6 increments over ~1.1 seconds
    try std.testing.expect(tc.rate() > 4.0);
}

test "BackpressureGauge detection" {
    var bp = BackpressureGauge{};
    bp.recordBusy(900);
    bp.recordIdle(100);
    try std.testing.expect(bp.isBackpressured()); // 90% busy

    bp.reset();
    bp.recordBusy(500);
    bp.recordIdle(500);
    try std.testing.expect(!bp.isBackpressured()); // 50% busy
}

test "PipelineMetrics integration" {
    var pm = PipelineMetrics.init(1000);
    pm.recordInput(0);
    pm.recordInput(100);
    pm.recordOutput(50);
    pm.recordCheckpoint(250);
    pm.recordDrop();

    try std.testing.expectEqual(@as(u64, 1), pm.checkpoints_completed);
    try std.testing.expectEqual(@as(i64, 250), pm.last_checkpoint_duration_ms);
    try std.testing.expectEqual(@as(u64, 1), pm.records_dropped);
}
