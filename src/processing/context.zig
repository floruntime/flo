//! Operator Context
//!
//! Provides operators with access to output collection, timing,
//! and (in later phases) keyed state and timer services.
//!
//! The OperatorContext is created per-operator invocation and gives
//! each operator a consistent interface to the processing runtime.
//!
//! ## Phasing
//!
//! Phase 1 (current): emit(), metrics, timing  — stateless operators only
//! Phase 2+: keyed_state, timer_service         — stateful operators
//! Phase 4+: side_outputs                        — branching pipelines

const std = @import("std");
const Allocator = std.mem.Allocator;
const OutputCollector = @import("collector.zig").OutputCollector;
const record_mod = @import("record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;

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

    // Phase 2+ fields (keyed state, timers, side outputs) will be added
    // when the corresponding modules are ported. These are nullable optional
    // fields on the operator interface — callers check before use.

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
