//! Window Assigners
//!
//! Determines which window(s) an element belongs to based on its event time.
//! Matches Flink's window assigner model:
//!
//! - Tumbling: fixed-size, non-overlapping windows
//! - Sliding: fixed-size, overlapping windows (size + slide)
//! - Global: single window for all elements (useful with custom triggers)
//! - Count: window defined by element count rather than time
//!
//! Session windows are handled separately in session.zig because they
//! require per-key state and dynamic merging.

const std = @import("std");

// =============================================================================
// TimeWindow — represents a single window instance [start, end)
// =============================================================================

pub const TimeWindow = struct {
    /// Inclusive start timestamp in milliseconds
    start_ms: i64,
    /// Exclusive end timestamp in milliseconds
    end_ms: i64,

    /// Window duration
    pub fn duration(self: TimeWindow) i64 {
        return self.end_ms - self.start_ms;
    }

    /// Does this window contain the given timestamp? [start, end)
    pub fn contains(self: TimeWindow, timestamp_ms: i64) bool {
        return timestamp_ms >= self.start_ms and timestamp_ms < self.end_ms;
    }

    /// Serialize as "start_end" for use as a state key component
    pub fn toKeyPart(self: TimeWindow, buf: []u8) ?[]const u8 {
        const result = std.fmt.bufPrint(buf, "{d}_{d}", .{ self.start_ms, self.end_ms }) catch return null;
        return result;
    }

    /// Equality check (both start and end match)
    pub fn eql(self: TimeWindow, other: TimeWindow) bool {
        return self.start_ms == other.start_ms and self.end_ms == other.end_ms;
    }
};

// =============================================================================
// WindowAssigner — determines which windows a record belongs to
// =============================================================================

pub const WindowAssigner = union(enum) {
    /// Tumbling: fixed-size, non-overlapping windows
    tumbling: TumblingConfig,
    /// Sliding: fixed-size windows that overlap by slide_ms
    sliding: SlidingConfig,
    /// Global: all elements go to one window
    global: void,
    /// Count: windows defined by element count, not time
    count: CountConfig,

    pub const TumblingConfig = struct {
        size_ms: i64,
        offset_ms: i64 = 0,
    };

    pub const SlidingConfig = struct {
        size_ms: i64,
        slide_ms: i64,
        offset_ms: i64 = 0,
    };

    pub const CountConfig = struct {
        max_count: u64,
    };

    /// Assign event-time timestamp to window(s). Returns number assigned.
    /// Writes into `out` buffer (caller provides stack storage).
    pub fn assignWindows(self: WindowAssigner, event_time_ms: i64, out: []TimeWindow) usize {
        switch (self) {
            .tumbling => |cfg| {
                if (out.len == 0) return 0;
                const adjusted = event_time_ms - cfg.offset_ms;
                const start = @divFloor(adjusted, cfg.size_ms) * cfg.size_ms + cfg.offset_ms;
                out[0] = .{
                    .start_ms = start,
                    .end_ms = start + cfg.size_ms,
                };
                return 1;
            },
            .sliding => |cfg| {
                const adjusted = event_time_ms - cfg.offset_ms;
                // A record belongs to all windows where start <= event_time < start + size
                // Number of windows = ceil(size / slide)
                var count: usize = 0;
                const first_start = @divFloor(adjusted, cfg.slide_ms) * cfg.slide_ms + cfg.offset_ms;
                // Walk backwards to find all windows that contain this timestamp
                var start = first_start;
                while (start > event_time_ms - cfg.size_ms) : (start -= cfg.slide_ms) {
                    if (count >= out.len) break;
                    const win = TimeWindow{
                        .start_ms = start,
                        .end_ms = start + cfg.size_ms,
                    };
                    if (win.contains(event_time_ms)) {
                        out[count] = win;
                        count += 1;
                    }
                    if (start <= cfg.offset_ms) break;
                }
                return count;
            },
            .global => {
                if (out.len == 0) return 0;
                out[0] = .{
                    .start_ms = std.math.minInt(i64),
                    .end_ms = std.math.maxInt(i64),
                };
                return 1;
            },
            .count => {
                // Count windows don't use time-based assignment
                // They're managed by the WindowOperator's count tracking
                if (out.len == 0) return 0;
                out[0] = .{
                    .start_ms = 0,
                    .end_ms = 0, // Placeholder — actual window ID set by operator
                };
                return 1;
            },
        }
    }
};

// =============================================================================
// Tests
// =============================================================================

test "Tumbling window assignment" {
    const assigner = WindowAssigner{
        .tumbling = .{ .size_ms = 5000 }, // 5-second windows
    };

    var out: [4]TimeWindow = undefined;

    // t=2000 → window [0, 5000)
    const n1 = assigner.assignWindows(2000, &out);
    try std.testing.expectEqual(@as(usize, 1), n1);
    try std.testing.expectEqual(@as(i64, 0), out[0].start_ms);
    try std.testing.expectEqual(@as(i64, 5000), out[0].end_ms);

    // t=5000 → window [5000, 10000)
    const n2 = assigner.assignWindows(5000, &out);
    try std.testing.expectEqual(@as(usize, 1), n2);
    try std.testing.expectEqual(@as(i64, 5000), out[0].start_ms);
    try std.testing.expectEqual(@as(i64, 10000), out[0].end_ms);

    // t=9999 → window [5000, 10000)
    const n3 = assigner.assignWindows(9999, &out);
    try std.testing.expectEqual(@as(usize, 1), n3);
    try std.testing.expectEqual(@as(i64, 5000), out[0].start_ms);
}

test "Tumbling window with offset" {
    const assigner = WindowAssigner{
        .tumbling = .{ .size_ms = 10000, .offset_ms = 3000 },
    };
    var out: [4]TimeWindow = undefined;

    // t=4000, offset=3000 → adjusted=1000, window starts at 3000
    const n = assigner.assignWindows(4000, &out);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(i64, 3000), out[0].start_ms);
    try std.testing.expectEqual(@as(i64, 13000), out[0].end_ms);
}

test "Sliding window assignment" {
    const assigner = WindowAssigner{
        .sliding = .{ .size_ms = 10000, .slide_ms = 5000 },
    };
    var out: [8]TimeWindow = undefined;

    // t=7000 → windows [0, 10000) and [5000, 15000)
    const n = assigner.assignWindows(7000, &out);
    try std.testing.expectEqual(@as(usize, 2), n);

    // Both windows should contain timestamp 7000
    var found_0_10k = false;
    var found_5k_15k = false;
    for (out[0..n]) |w| {
        if (w.start_ms == 0 and w.end_ms == 10000) found_0_10k = true;
        if (w.start_ms == 5000 and w.end_ms == 15000) found_5k_15k = true;
    }
    try std.testing.expect(found_0_10k);
    try std.testing.expect(found_5k_15k);
}

test "Global window assignment" {
    const assigner = WindowAssigner{ .global = {} };
    var out: [4]TimeWindow = undefined;

    const n = assigner.assignWindows(12345, &out);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(std.math.minInt(i64), out[0].start_ms);
    try std.testing.expectEqual(std.math.maxInt(i64), out[0].end_ms);
}

test "TimeWindow contains" {
    const w = TimeWindow{ .start_ms = 100, .end_ms = 200 };
    try std.testing.expect(w.contains(100));
    try std.testing.expect(w.contains(150));
    try std.testing.expect(!w.contains(200)); // exclusive end
    try std.testing.expect(!w.contains(99));
}

test "TimeWindow toKeyPart" {
    const w = TimeWindow{ .start_ms = 1000, .end_ms = 2000 };
    var buf: [64]u8 = undefined;
    const key = w.toKeyPart(&buf).?;
    try std.testing.expectEqualStrings("1000_2000", key);
}
