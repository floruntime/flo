//! Late Data Handling
//!
//! Manages records that arrive after the watermark has already
//! passed their event time window. In Flink, this is configured
//! via .allowedLateness() on a window.
//!
//! Behaviour:
//! 1. If record.event_time_ms < watermark - allowed_lateness_ms → DROP
//! 2. If record.event_time_ms < watermark but within allowed lateness → LATE (re-fire window)
//! 3. Otherwise → ON_TIME
//!
//! Late records can be optionally routed to a side output for monitoring.

const std = @import("std");
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;

// =============================================================================
// LateRecordClassification
// =============================================================================

pub const RecordTimeliness = enum {
    /// Record arrived within the expected window time
    on_time,
    /// Record is late but within the allowed lateness
    late,
    /// Record is too late — dropped
    dropped,
};

// =============================================================================
// LatenessTracker
// =============================================================================

/// Tracks allowed lateness for window operators and classifies
/// incoming records as on-time, late, or dropped.
pub const LatenessTracker = struct {
    /// Maximum allowed lateness in ms (0 = no late data allowed)
    allowed_lateness_ms: i64,
    /// Optional tag bit to set on late records (null = no tagging)
    late_tag_bit: ?u5,
    /// Counter: on-time records
    on_time_count: u64 = 0,
    /// Counter: late but accepted records
    late_count: u64 = 0,
    /// Counter: dropped (too late) records
    dropped_count: u64 = 0,

    const Self = @This();

    pub fn init(allowed_lateness_ms: i64) Self {
        return .{
            .allowed_lateness_ms = allowed_lateness_ms,
            .late_tag_bit = null,
        };
    }

    /// Create a tracker that tags late records with a specific bit
    pub fn initWithTag(allowed_lateness_ms: i64, tag_bit: u5) Self {
        return .{
            .allowed_lateness_ms = allowed_lateness_ms,
            .late_tag_bit = tag_bit,
        };
    }

    /// Classify a record against the current watermark and window end.
    pub fn classify(
        self: *Self,
        event_time_ms: i64,
        window_end_ms: i64,
        current_watermark_ms: i64,
    ) RecordTimeliness {
        if (event_time_ms >= window_end_ms) {
            // Record doesn't belong in this window at all
            return .dropped;
        }

        if (current_watermark_ms < window_end_ms) {
            // Window hasn't fired yet — on time
            self.on_time_count += 1;
            return .on_time;
        }

        // Window has fired. Check if within allowed lateness.
        if (current_watermark_ms <= window_end_ms + self.allowed_lateness_ms) {
            self.late_count += 1;
            return .late;
        }

        self.dropped_count += 1;
        return .dropped;
    }

    /// Tag a record as late if a tag bit is configured.
    /// Returns true if the record was tagged, false if no tag configured.
    pub fn tagAsLate(self: *Self, rec: *ProcessingRecord) bool {
        const bit = self.late_tag_bit orelse return false;
        rec.addTag(bit);
        return true;
    }

    /// Total records seen
    pub fn totalSeen(self: *const Self) u64 {
        return self.on_time_count + self.late_count + self.dropped_count;
    }

    /// Percentage of late records (0.0 to 1.0)
    pub fn lateRatio(self: *const Self) f64 {
        const total = self.totalSeen();
        if (total == 0) return 0.0;
        return @as(f64, @floatFromInt(self.late_count)) / @as(f64, @floatFromInt(total));
    }
};

// =============================================================================
// Tests
// =============================================================================

test "LatenessTracker on-time records" {
    var tracker = LatenessTracker.init(0);

    // Window [0, 5000), watermark at 3000, record at 2000
    const result = tracker.classify(2000, 5000, 3000);
    try std.testing.expectEqual(RecordTimeliness.on_time, result);
    try std.testing.expectEqual(@as(u64, 1), tracker.on_time_count);
}

test "LatenessTracker late but accepted" {
    // Allow 2000ms lateness
    var tracker = LatenessTracker.init(2000);

    // Window [0, 5000), watermark at 6000 (window fired),
    // but 6000 <= 5000 + 2000 = 7000, so accepted as late
    const result = tracker.classify(3000, 5000, 6000);
    try std.testing.expectEqual(RecordTimeliness.late, result);
    try std.testing.expectEqual(@as(u64, 1), tracker.late_count);
}

test "LatenessTracker dropped too late" {
    var tracker = LatenessTracker.init(1000);

    // Window [0, 5000), watermark at 7000
    // 7000 > 5000 + 1000 = 6000, so dropped
    const result = tracker.classify(3000, 5000, 7000);
    try std.testing.expectEqual(RecordTimeliness.dropped, result);
    try std.testing.expectEqual(@as(u64, 1), tracker.dropped_count);
}

test "LatenessTracker zero allowed lateness" {
    var tracker = LatenessTracker.init(0);

    // Window fired (watermark >= window_end), no lateness allowed
    const result = tracker.classify(3000, 5000, 5000);
    try std.testing.expectEqual(RecordTimeliness.late, result);

    // Just past → dropped
    const result2 = tracker.classify(3000, 5000, 5001);
    try std.testing.expectEqual(RecordTimeliness.dropped, result2);
}

test "LatenessTracker late ratio" {
    var tracker = LatenessTracker.init(5000);

    _ = tracker.classify(100, 5000, 1000); // on-time
    _ = tracker.classify(100, 5000, 1000); // on-time
    _ = tracker.classify(100, 5000, 6000); // late
    _ = tracker.classify(100, 5000, 20000); // dropped

    try std.testing.expectEqual(@as(u64, 4), tracker.totalSeen());
    try std.testing.expectApproxEqAbs(@as(f64, 0.25), tracker.lateRatio(), 0.001);
}

test "LatenessTracker with tag bit" {
    const tracker = LatenessTracker.initWithTag(5000, 3);
    try std.testing.expectEqual(@as(?u5, 3), tracker.late_tag_bit);
}

test "LatenessTracker tagAsLate sets bit" {
    var tracker = LatenessTracker.initWithTag(5000, 2);
    var rec = ProcessingRecord.init("k", "v", 100);
    try std.testing.expectEqual(@as(u32, 0), rec.tags);
    const tagged = tracker.tagAsLate(&rec);
    try std.testing.expect(tagged);
    try std.testing.expect(rec.hasTag(2));
    try std.testing.expectEqual(@as(u32, 4), rec.tags); // 1 << 2 = 4
}

test "LatenessTracker tagAsLate no bit configured" {
    var tracker = LatenessTracker.init(5000);
    var rec = ProcessingRecord.init("k", "v", 100);
    const tagged = tracker.tagAsLate(&rec);
    try std.testing.expect(!tagged);
    try std.testing.expectEqual(@as(u32, 0), rec.tags);
}
