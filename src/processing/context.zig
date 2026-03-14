//! Operator Context
//!
//! Provides operators with access to output collection, timing,
//! keyed state, timer services, and side outputs.
//!
//! The OperatorContext is created per-operator invocation and gives
//! each operator a consistent interface to the processing runtime.

const std = @import("std");
const Allocator = std.mem.Allocator;
const OutputCollector = @import("collector.zig").OutputCollector;
const record_mod = @import("record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const KeyedStateAccess = @import("state.zig").KeyedStateAccess;
const TimerService = @import("time/timer.zig").TimerService;

// =============================================================================
// OperatorMetrics - Per-operator counters
// =============================================================================

/// Simple metrics for a single operator instance
pub const OperatorMetrics = struct {
    /// Records processed by this operator
    records_in: u64 = 0,
    /// Records emitted by this operator
    records_out: u64 = 0,
    /// Processing time in nanoseconds (cumulative)
    processing_time_ns: u64 = 0,
    /// Errors encountered
    errors: u64 = 0,
    /// Last processing timestamp
    last_processed_ms: i64 = 0,

    pub fn recordProcessed(self: *OperatorMetrics) void {
        self.records_in += 1;
    }

    pub fn recordEmitted(self: *OperatorMetrics, count: u64) void {
        self.records_out += count;
    }

    pub fn recordError(self: *OperatorMetrics) void {
        self.errors += 1;
    }
};

// =============================================================================
// OperatorContext - Runtime context passed to operators
// =============================================================================

/// Context available to operators during processing.
pub const OperatorContext = struct {
    /// Emit records downstream
    collector: *OutputCollector,
    /// Per-operator metrics
    metrics: *OperatorMetrics,
    /// Allocator for temporary allocations during processing
    allocator: Allocator,
    /// Current processing time (wall clock, milliseconds)
    current_processing_time_ms: i64,
    /// Current watermark (event time progress)
    current_watermark_ms: i64,
    /// Operator name (for logging/debugging)
    operator_name: []const u8,

    /// Keyed state access (for stateful operators, null if stateless)
    keyed_state: ?*KeyedStateAccess = null,
    /// Timer service (for event-time / processing-time timers)
    timer_service: ?*TimerService = null,

    /// Convenience: emit a record downstream
    pub fn emit(self: *OperatorContext, rec: ProcessingRecord) !void {
        try self.collector.emit(rec);
    }

    /// Convenience: emit with re-keying
    pub fn emitWithKey(self: *OperatorContext, key: []const u8, value: []const u8, event_time_ms: i64) !void {
        try self.collector.emitWithKey(key, value, event_time_ms);
    }

    /// Get current processing time (wall clock)
    pub fn processingTime(self: *const OperatorContext) i64 {
        return self.current_processing_time_ms;
    }

    /// Get current watermark
    pub fn watermark(self: *const OperatorContext) i64 {
        return self.current_watermark_ms;
    }

    /// Get keyed state access (returns null if operator is stateless)
    pub fn getKeyedState(self: *OperatorContext) ?*KeyedStateAccess {
        return self.keyed_state;
    }

    /// Get timer service (returns null if timers not configured)
    pub fn getTimerService(self: *OperatorContext) ?*TimerService {
        return self.timer_service;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "OperatorContext emit through collector" {
    const allocator = std.testing.allocator;
    var collector = OutputCollector.init(allocator);
    defer collector.deinit();

    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 1000,
        .current_watermark_ms = 500,
        .operator_name = "test-op",
    };

    try ctx.emit(ProcessingRecord.init("k", "v", 100));
    try std.testing.expectEqual(@as(usize, 1), collector.count());
    try std.testing.expectEqual(@as(i64, 1000), ctx.processingTime());
    try std.testing.expectEqual(@as(i64, 500), ctx.watermark());
}

test "OperatorMetrics counting" {
    var m = OperatorMetrics{};
    m.recordProcessed();
    m.recordProcessed();
    m.recordEmitted(3);
    m.recordError();

    try std.testing.expectEqual(@as(u64, 2), m.records_in);
    try std.testing.expectEqual(@as(u64, 3), m.records_out);
    try std.testing.expectEqual(@as(u64, 1), m.errors);
}
