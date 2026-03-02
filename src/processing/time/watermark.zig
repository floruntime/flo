//! Watermark Generation
//!
//! Watermarks signal event-time progress through the DAG:
//! "All events with timestamp <= W have arrived."
//!
//! Strategies (matching Flink):
//!   - none: No watermarks (processing-time only)
//!   - ascending: Assumes monotonically ascending timestamps
//!   - bounded_out_of_order: Allows configurable delay for late events
//!
//! WatermarkGenerator wraps a strategy and produces Watermark signals
//! as records flow through.

const std = @import("std");
const record_mod = @import("../record.zig");
const Watermark = record_mod.Watermark;

// =============================================================================
// WatermarkStrategy
// =============================================================================

pub const WatermarkStrategy = union(enum) {
    /// No watermarks — event time is not tracked
    none: void,
    /// Assumes timestamps are monotonically ascending.
    /// Watermark = max observed timestamp
    ascending: void,
    /// Allows out-of-order events up to max_delay_ms.
    /// Watermark = max observed timestamp - max_delay_ms
    bounded_out_of_order: BoundedConfig,

    pub const BoundedConfig = struct {
        /// Maximum expected disorder (milliseconds)
        max_delay_ms: i64,
    };
};

// =============================================================================
// WatermarkGenerator
// =============================================================================

/// Generates watermarks from observed event timestamps.
///
/// Call `observeEvent()` for each record, then `currentWatermark()`
/// to get the latest watermark value. The generator tracks the maximum
/// observed event time and applies the strategy to compute the watermark.
///
/// Typical usage in a source operator:
///   for each record:
///     generator.observeEvent(record.event_time_ms)
///     if periodic interval: emit Watermark(generator.currentWatermark())
pub const WatermarkGenerator = struct {
    strategy: WatermarkStrategy,
    /// Maximum observed event timestamp so far
    max_timestamp_ms: i64,
    /// Last emitted watermark value
    last_watermark_ms: i64,
    /// Counter for periodic emission (not time-based, record-based)
    records_since_emit: u64,
    /// Emit a watermark every N records (0 = never auto-emit)
    emit_interval_records: u64,

    const Self = @This();

    pub fn init(strategy: WatermarkStrategy) Self {
        return .{
            .strategy = strategy,
            .max_timestamp_ms = std.math.minInt(i64),
            .last_watermark_ms = std.math.minInt(i64),
            .records_since_emit = 0,
            .emit_interval_records = 0,
        };
    }

    /// Create a generator with periodic watermark emission
    pub fn initWithInterval(strategy: WatermarkStrategy, emit_every_n: u64) Self {
        var gen = init(strategy);
        gen.emit_interval_records = emit_every_n;
        return gen;
    }

    /// Observe an event's timestamp and update internal tracking.
    pub fn observeEvent(self: *Self, event_time_ms: i64) void {
        if (event_time_ms > self.max_timestamp_ms) {
            self.max_timestamp_ms = event_time_ms;
        }
        self.records_since_emit += 1;
    }

    /// Compute the current watermark value based on the strategy.
    pub fn currentWatermark(self: *const Self) i64 {
        return switch (self.strategy) {
            .none => std.math.minInt(i64),
            .ascending => self.max_timestamp_ms,
            .bounded_out_of_order => |cfg| blk: {
                const wm = self.max_timestamp_ms - cfg.max_delay_ms;
                break :blk if (wm < std.math.minInt(i64) + cfg.max_delay_ms)
                    std.math.minInt(i64)
                else
                    wm;
            },
        };
    }

    /// Check if it's time to emit a watermark (based on record count).
    /// Returns the watermark to emit, or null if not yet.
    pub fn shouldEmit(self: *Self) ?Watermark {
        if (self.emit_interval_records == 0) return null;
        if (self.records_since_emit < self.emit_interval_records) return null;

        const wm = self.currentWatermark();
        if (wm > self.last_watermark_ms) {
            self.last_watermark_ms = wm;
            self.records_since_emit = 0;
            return Watermark.init(wm);
        }
        return null;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "Ascending strategy tracks max timestamp" {
    var gen = WatermarkGenerator.init(.{ .ascending = {} });

    gen.observeEvent(1000);
    try std.testing.expectEqual(@as(i64, 1000), gen.currentWatermark());

    gen.observeEvent(3000);
    try std.testing.expectEqual(@as(i64, 3000), gen.currentWatermark());

    // Out-of-order: watermark doesn't go backward
    gen.observeEvent(2000);
    try std.testing.expectEqual(@as(i64, 3000), gen.currentWatermark());
}

test "Bounded out-of-order subtracts delay" {
    var gen = WatermarkGenerator.init(.{
        .bounded_out_of_order = .{ .max_delay_ms = 2000 },
    });

    gen.observeEvent(5000);
    try std.testing.expectEqual(@as(i64, 3000), gen.currentWatermark());

    gen.observeEvent(7000);
    try std.testing.expectEqual(@as(i64, 5000), gen.currentWatermark());
}

test "None strategy always returns minInt" {
    var gen = WatermarkGenerator.init(.{ .none = {} });
    gen.observeEvent(9999);
    try std.testing.expectEqual(std.math.minInt(i64), gen.currentWatermark());
}

test "Periodic emission based on record count" {
    var gen = WatermarkGenerator.initWithInterval(.{ .ascending = {} }, 3);

    gen.observeEvent(100);
    try std.testing.expectEqual(@as(?Watermark, null), gen.shouldEmit());

    gen.observeEvent(200);
    try std.testing.expectEqual(@as(?Watermark, null), gen.shouldEmit());

    gen.observeEvent(300);
    const wm = gen.shouldEmit();
    try std.testing.expect(wm != null);
    try std.testing.expectEqual(@as(i64, 300), wm.?.timestamp_ms);

    // Counter reset — need 3 more
    gen.observeEvent(400);
    try std.testing.expectEqual(@as(?Watermark, null), gen.shouldEmit());
}
