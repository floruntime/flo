//! Window Triggers
//!
//! Triggers decide when a window should fire (evaluate its function
//! and emit results). Matches Flink's trigger model.
//!
//! - EventTime: fire when watermark >= window.end (default)
//! - ProcessingTime: fire when wall clock >= window.end
//! - Count: fire when element count reaches threshold
//! - Continuous: fire at regular intervals

const std = @import("std");
const TimeWindow = @import("assigner.zig").TimeWindow;

// =============================================================================
// TriggerResult - What happens when a trigger fires
// =============================================================================

pub const TriggerResult = enum {
    /// Do nothing — keep accumulating
    continue_,
    /// Fire the window function, keep window contents
    fire,
    /// Discard window contents without firing
    purge,
    /// Fire the window function, then discard contents
    fire_and_purge,
};

// =============================================================================
// TriggerType - Supported trigger strategies
// =============================================================================

pub const TriggerType = union(enum) {
    /// Fire when watermark passes window end (default for event-time)
    event_time: void,
    /// Fire when processing time reaches window end
    processing_time: void,
    /// Fire when element count reaches threshold
    count: CountConfig,
    /// Fire at regular intervals (for continuous aggregation)
    continuous: ContinuousConfig,

    pub const CountConfig = struct {
        threshold: u64,
    };

    pub const ContinuousConfig = struct {
        interval_ms: i64,
    };

    /// Evaluate whether this trigger should fire.
    ///
    /// For event_time: fires when current_watermark >= window.end_ms
    /// For processing_time: fires when processing_time >= window.end_ms
    /// For count: fires when element_count >= threshold
    pub fn shouldFire(
        self: TriggerType,
        window: TimeWindow,
        current_watermark_ms: i64,
        processing_time_ms: i64,
        element_count: u64,
    ) TriggerResult {
        switch (self) {
            .event_time => {
                if (current_watermark_ms >= window.end_ms) {
                    return .fire_and_purge;
                }
                return .continue_;
            },
            .processing_time => {
                if (processing_time_ms >= window.end_ms) {
                    return .fire_and_purge;
                }
                return .continue_;
            },
            .count => |cfg| {
                if (element_count >= cfg.threshold) {
                    return .fire_and_purge;
                }
                return .continue_;
            },
            .continuous => |cfg| {
                // Fire at regular intervals based on processing time.
                if (processing_time_ms >= window.end_ms) {
                    return .fire_and_purge;
                }
                const elapsed = processing_time_ms - window.start_ms;
                if (elapsed > 0 and @mod(elapsed, cfg.interval_ms) == 0) {
                    return .fire;
                }
                return .continue_;
            },
        }
    }
};

// =============================================================================
// Tests
// =============================================================================

test "EventTime trigger fires when watermark passes window end" {
    const trigger = TriggerType{ .event_time = {} };
    const window = TimeWindow{ .start_ms = 0, .end_ms = 5000 };

    // Watermark hasn't reached window end
    try std.testing.expectEqual(TriggerResult.continue_, trigger.shouldFire(window, 4999, 0, 0));

    // Watermark at window end — fire
    try std.testing.expectEqual(TriggerResult.fire_and_purge, trigger.shouldFire(window, 5000, 0, 0));

    // Watermark past window end — fire
    try std.testing.expectEqual(TriggerResult.fire_and_purge, trigger.shouldFire(window, 6000, 0, 0));
}

test "Count trigger fires at threshold" {
    const trigger = TriggerType{ .count = .{ .threshold = 3 } };
    const window = TimeWindow{ .start_ms = 0, .end_ms = 5000 };

    try std.testing.expectEqual(TriggerResult.continue_, trigger.shouldFire(window, 0, 0, 1));
    try std.testing.expectEqual(TriggerResult.continue_, trigger.shouldFire(window, 0, 0, 2));
    try std.testing.expectEqual(TriggerResult.fire_and_purge, trigger.shouldFire(window, 0, 0, 3));
    try std.testing.expectEqual(TriggerResult.fire_and_purge, trigger.shouldFire(window, 0, 0, 10));
}

test "ProcessingTime trigger fires on wall clock" {
    const trigger = TriggerType{ .processing_time = {} };
    const window = TimeWindow{ .start_ms = 1000, .end_ms = 2000 };

    try std.testing.expectEqual(TriggerResult.continue_, trigger.shouldFire(window, 0, 1500, 0));
    try std.testing.expectEqual(TriggerResult.fire_and_purge, trigger.shouldFire(window, 0, 2000, 0));
}

test "Continuous trigger fires at interval boundaries" {
    const trigger = TriggerType{ .continuous = .{ .interval_ms = 1000 } };
    const window = TimeWindow{ .start_ms = 0, .end_ms = 5000 };

    // At window start (elapsed=0) — don't fire
    try std.testing.expectEqual(TriggerResult.continue_, trigger.shouldFire(window, 0, 0, 0));

    // Mid-interval — don't fire
    try std.testing.expectEqual(TriggerResult.continue_, trigger.shouldFire(window, 0, 500, 0));

    // At first interval boundary — fire
    try std.testing.expectEqual(TriggerResult.fire, trigger.shouldFire(window, 0, 1000, 0));

    // Between intervals — don't fire
    try std.testing.expectEqual(TriggerResult.continue_, trigger.shouldFire(window, 0, 1500, 0));

    // At second interval boundary — fire
    try std.testing.expectEqual(TriggerResult.fire, trigger.shouldFire(window, 0, 2000, 0));

    // At window end — fire_and_purge
    try std.testing.expectEqual(TriggerResult.fire_and_purge, trigger.shouldFire(window, 0, 5000, 0));

    // Past window end — fire_and_purge
    try std.testing.expectEqual(TriggerResult.fire_and_purge, trigger.shouldFire(window, 0, 6000, 0));
}
