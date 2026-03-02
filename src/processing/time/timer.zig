//! Timer Service
//!
//! Provides event-time and processing-time timers for operators.
//! Timers fire callbacks when time advances past the registered timestamp.
//!
//! Event-time timers: fire when watermark >= timer timestamp
//! Processing-time timers: fire when wall clock >= timer timestamp
//!
//! In-memory timer queues (per-operator, per-shard).
//! Timer state will be checkpointed for recovery.

const std = @import("std");
const Allocator = std.mem.Allocator;
const record_mod = @import("../record.zig");
const Watermark = record_mod.Watermark;

// =============================================================================
// TimerEntry
// =============================================================================

pub const TimerEntry = struct {
    /// The timestamp at which this timer should fire
    timestamp_ms: i64,
    /// The key this timer is associated with (owned)
    key: []const u8,
    /// User-assigned namespace for grouping timers
    namespace: []const u8,
};

// =============================================================================
// TimerService
// =============================================================================

/// Manages event-time and processing-time timers for an operator.
///
/// Timers are stored in sorted order (min-heap behavior via sorted insertion).
/// When the watermark or processing time advances, `advanceEventTime` /
/// `advanceProcessingTime` returns all fired timers.
///
/// Usage:
///   var timers = try TimerService.init(allocator);
///   try timers.registerEventTimeTimer(5000, "user:42", "window");
///   const fired = timers.advanceEventTime(6000);
///   for (fired) |entry| { ... process fired timer ... }
pub const TimerService = struct {
    allocator: Allocator,
    /// Event-time timers, sorted by timestamp ascending
    event_time_timers: std.ArrayList(TimerEntry),
    /// Processing-time timers, sorted by timestamp ascending
    processing_time_timers: std.ArrayList(TimerEntry),
    /// Last advanced event time
    current_event_time_ms: i64,
    /// Last advanced processing time
    current_processing_time_ms: i64,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .event_time_timers = .empty,
            .processing_time_timers = .empty,
            .current_event_time_ms = std.math.minInt(i64),
            .current_processing_time_ms = std.math.minInt(i64),
        };
    }

    pub fn deinit(self: *Self) void {
        self.freeTimerList(&self.event_time_timers);
        self.freeTimerList(&self.processing_time_timers);
    }

    fn freeTimerList(self: *Self, list: *std.ArrayList(TimerEntry)) void {
        for (list.items) |entry| {
            if (entry.key.len > 0) self.allocator.free(entry.key);
            if (entry.namespace.len > 0) self.allocator.free(entry.namespace);
        }
        list.deinit(self.allocator);
    }

    /// Register an event-time timer. Fires when watermark >= timestamp_ms.
    pub fn registerEventTimeTimer(
        self: *Self,
        timestamp_ms: i64,
        key: []const u8,
        namespace: []const u8,
    ) !void {
        try self.insertSorted(&self.event_time_timers, .{
            .timestamp_ms = timestamp_ms,
            .key = if (key.len > 0) try self.allocator.dupe(u8, key) else &.{},
            .namespace = if (namespace.len > 0) try self.allocator.dupe(u8, namespace) else &.{},
        });
    }

    /// Register a processing-time timer. Fires when wall clock >= timestamp_ms.
    pub fn registerProcessingTimeTimer(
        self: *Self,
        timestamp_ms: i64,
        key: []const u8,
        namespace: []const u8,
    ) !void {
        try self.insertSorted(&self.processing_time_timers, .{
            .timestamp_ms = timestamp_ms,
            .key = if (key.len > 0) try self.allocator.dupe(u8, key) else &.{},
            .namespace = if (namespace.len > 0) try self.allocator.dupe(u8, namespace) else &.{},
        });
    }

    /// Advance event time. Returns the number of timers that fired.
    /// Fired timers are removed from the queue.
    pub fn advanceEventTime(self: *Self, watermark_ms: i64) usize {
        self.current_event_time_ms = watermark_ms;
        return self.countFired(&self.event_time_timers, watermark_ms);
    }

    /// Advance processing time. Returns the number of timers that fired.
    pub fn advanceProcessingTime(self: *Self, processing_time_ms: i64) usize {
        self.current_processing_time_ms = processing_time_ms;
        return self.countFired(&self.processing_time_timers, processing_time_ms);
    }

    /// Drain fired event-time timers up to the given timestamp.
    /// Frees owned memory of fired entries and shifts remaining left.
    pub fn drainFiredEventTime(self: *Self, watermark_ms: i64) void {
        self.drainFired(&self.event_time_timers, watermark_ms);
    }

    /// Drain fired processing-time timers up to the given timestamp.
    pub fn drainFiredProcessingTime(self: *Self, processing_time_ms: i64) void {
        self.drainFired(&self.processing_time_timers, processing_time_ms);
    }

    /// Number of pending event-time timers
    pub fn eventTimeTimerCount(self: *const Self) usize {
        return self.event_time_timers.items.len;
    }

    /// Number of pending processing-time timers
    pub fn processingTimeTimerCount(self: *const Self) usize {
        return self.processing_time_timers.items.len;
    }

    // =========================================================================
    // Internal
    // =========================================================================

    fn insertSorted(self: *Self, list: *std.ArrayList(TimerEntry), entry: TimerEntry) !void {
        // Find insertion point (binary search by timestamp)
        var lo: usize = 0;
        var hi: usize = list.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (list.items[mid].timestamp_ms <= entry.timestamp_ms) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        try list.insert(self.allocator, lo, entry);
    }

    fn countFired(_: *const Self, list: *const std.ArrayList(TimerEntry), time_ms: i64) usize {
        var count: usize = 0;
        for (list.items) |entry| {
            if (entry.timestamp_ms <= time_ms) {
                count += 1;
            } else {
                break; // Sorted — no more will match
            }
        }
        return count;
    }

    /// Drain all entries with timestamp_ms <= time_ms from the front.
    /// Frees their owned memory and shrinks the list.
    fn drainFired(self: *Self, list: *std.ArrayList(TimerEntry), time_ms: i64) void {
        var fired: usize = 0;
        for (list.items) |entry| {
            if (entry.timestamp_ms <= time_ms) {
                fired += 1;
            } else {
                break;
            }
        }

        if (fired == 0) return;

        // Free owned memory of fired entries
        for (list.items[0..fired]) |entry| {
            if (entry.key.len > 0) self.allocator.free(entry.key);
            if (entry.namespace.len > 0) self.allocator.free(entry.namespace);
        }

        // Shift remaining entries left
        const remaining = list.items.len - fired;
        if (remaining > 0) {
            std.mem.copyForwards(TimerEntry, list.items[0..remaining], list.items[fired..list.items.len]);
        }
        list.items.len = remaining;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "TimerService event-time timer fires on watermark" {
    const allocator = std.testing.allocator;
    var ts = TimerService.init(allocator);
    defer ts.deinit();

    try ts.registerEventTimeTimer(5000, "key-a", "window");
    try ts.registerEventTimeTimer(3000, "key-b", "window");
    try ts.registerEventTimeTimer(8000, "key-c", "window");

    try std.testing.expectEqual(@as(usize, 3), ts.eventTimeTimerCount());

    // Advance to 4000 — only the 3000 timer fires
    const fired1 = ts.advanceEventTime(4000);
    try std.testing.expectEqual(@as(usize, 1), fired1);

    // Drain removes fired entries
    ts.drainFiredEventTime(4000);
    try std.testing.expectEqual(@as(usize, 2), ts.eventTimeTimerCount());

    // Advance to 6000 — the 5000 timer fires
    const fired2 = ts.advanceEventTime(6000);
    try std.testing.expectEqual(@as(usize, 1), fired2);
    ts.drainFiredEventTime(6000);
    try std.testing.expectEqual(@as(usize, 1), ts.eventTimeTimerCount());
}

test "TimerService processing-time timer" {
    const allocator = std.testing.allocator;
    var ts = TimerService.init(allocator);
    defer ts.deinit();

    try ts.registerProcessingTimeTimer(1000, "k", "ns");
    try ts.registerProcessingTimeTimer(2000, "k", "ns");

    const fired = ts.advanceProcessingTime(1500);
    try std.testing.expectEqual(@as(usize, 1), fired);

    ts.drainFiredProcessingTime(1500);
    try std.testing.expectEqual(@as(usize, 1), ts.processingTimeTimerCount());
}

test "TimerService maintains sorted order" {
    const allocator = std.testing.allocator;
    var ts = TimerService.init(allocator);
    defer ts.deinit();

    // Insert out of order
    try ts.registerEventTimeTimer(5000, "", "");
    try ts.registerEventTimeTimer(1000, "", "");
    try ts.registerEventTimeTimer(3000, "", "");

    // Should be sorted: 1000, 3000, 5000
    try std.testing.expectEqual(@as(i64, 1000), ts.event_time_timers.items[0].timestamp_ms);
    try std.testing.expectEqual(@as(i64, 3000), ts.event_time_timers.items[1].timestamp_ms);
    try std.testing.expectEqual(@as(i64, 5000), ts.event_time_timers.items[2].timestamp_ms);
}

test "TimerService empty drain" {
    const allocator = std.testing.allocator;
    var ts = TimerService.init(allocator);
    defer ts.deinit();

    const fired = ts.advanceEventTime(10000);
    try std.testing.expectEqual(@as(usize, 0), fired);
}
