//! Window Operator
//!
//! Ties together WindowAssigner + Trigger + WindowFunction into a
//! complete Operator implementation. Manages active window state
//! in-memory (per-shard, no locking needed).
//!
//! Architecture:
//!   processElement → assign to window(s) → update accumulator → check trigger
//!   processWatermark → advance watermark → fire matured event-time windows
//!
//! State lifecycle:
//!   1. First element creates a new ActiveWindowEntry
//!   2. Subsequent elements update the accumulator via reduce/aggregate
//!   3. When trigger fires → emit result → purge entry (fire_and_purge)
//!   4. Purged entries remain as stubs (count=0); reclaimed on deinit

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("../operator.zig").Operator;
const OperatorContext = @import("../context.zig").OperatorContext;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const Watermark = record_mod.Watermark;
const TimeWindow = @import("assigner.zig").TimeWindow;
const WindowAssigner = @import("assigner.zig").WindowAssigner;
const TriggerType = @import("trigger.zig").TriggerType;
const TriggerResult = @import("trigger.zig").TriggerResult;
const winfn = @import("function.zig");
const WindowFunction = winfn.WindowFunction;

// =============================================================================
// WindowOperator
// =============================================================================

pub const WindowOperator = struct {
    name: []const u8,
    assigner: WindowAssigner,
    trigger: TriggerType,
    window_fn: WindowFunction,
    allocator: Allocator,
    /// Active (and purged-stub) window entries
    active_windows: std.ArrayList(ActiveWindowEntry),

    pub const ActiveWindowEntry = struct {
        /// Keyed stream key (owned; empty after purge)
        key: []const u8,
        window: TimeWindow,
        /// Accumulator value (owned; null after purge or before first element)
        accumulator: ?[]const u8,
        /// Number of elements in the window (0 = purged stub)
        count: u64,
    };

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        name: []const u8,
        assigner: WindowAssigner,
        trigger: TriggerType,
        window_fn: WindowFunction,
    ) Self {
        return .{
            .name = name,
            .assigner = assigner,
            .trigger = trigger,
            .window_fn = window_fn,
            .allocator = allocator,
            .active_windows = .{},
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.active_windows.items) |aw| {
            if (aw.count > 0) {
                self.allocator.free(aw.key);
                if (aw.accumulator) |acc| self.allocator.free(acc);
            }
        }
        self.active_windows.deinit(self.allocator);
    }

    pub fn operator(self: *Self) Operator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    /// Number of live (non-purged) windows
    pub fn activeCount(self: *const Self) usize {
        var n: usize = 0;
        for (self.active_windows.items) |aw| {
            if (aw.count > 0) n += 1;
        }
        return n;
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    fn findWindow(self: *Self, key: []const u8, window: TimeWindow) ?usize {
        for (self.active_windows.items, 0..) |aw, i| {
            if (aw.count > 0 and std.mem.eql(u8, aw.key, key) and aw.window.eql(window)) {
                return i;
            }
        }
        return null;
    }

    /// Apply the window function to combine current accumulator with new value.
    /// Returns a BORROWED slice — caller must dupe before freeing inputs.
    fn applyFn(self: *Self, current: ?[]const u8, value: []const u8) []const u8 {
        return switch (self.window_fn) {
            .reduce => |f| {
                if (current) |acc| return f(acc, value);
                return value; // First element: accumulator = element itself
            },
            .aggregate => |fns| {
                const acc = current orelse fns.createAccumulator();
                return fns.add(acc, value);
            },
        };
    }

    /// Extract the final output from an accumulator.
    fn getResultVal(self: *Self, acc: []const u8) []const u8 {
        return switch (self.window_fn) {
            .reduce => acc,
            .aggregate => |fns| fns.getResult(acc),
        };
    }

    /// Emit an owned (deep-copied) result record so it survives window cleanup.
    fn emitResult(self: *Self, key: []const u8, value: []const u8, event_time_ms: i64, ctx: *OperatorContext) !void {
        try ctx.emit(.{
            .key = try self.allocator.dupe(u8, key),
            .value = try self.allocator.dupe(u8, value),
            .event_time_ms = event_time_ms,
            .source = record_mod.SourceRef.EMPTY,
            .headers = &.{},
            .owns_memory = true,
        });
    }

    /// Purge a window entry — free owned memory, mark as stub.
    fn purgeEntry(self: *Self, aw: *ActiveWindowEntry) void {
        self.allocator.free(aw.key);
        aw.key = &.{};
        if (aw.accumulator) |acc| {
            self.allocator.free(acc);
            aw.accumulator = null;
        }
        aw.count = 0;
    }

    // =========================================================================
    // Vtable implementation
    // =========================================================================

    const vtable = Operator.VTable{
        .processElement = processElementFn,
        .processWatermark = processWatermarkFn,
        .getName = getNameFn,
        .close = closeFn,
        .snapshotState = snapshotStateFn,
        .restoreState = restoreStateFn,
    };

    fn processElementFn(ptr: *anyopaque, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var windows_buf: [16]TimeWindow = undefined;
        const num_windows = self.assigner.assignWindows(rec.event_time_ms, &windows_buf);

        for (windows_buf[0..num_windows]) |window| {
            if (self.findWindow(rec.key, window)) |i| {
                // ---- Update existing window ----
                const aw = &self.active_windows.items[i];
                const new_acc = self.applyFn(aw.accumulator, rec.value);
                // Dupe BEFORE freeing old (new_acc may borrow from old)
                const owned_new = try self.allocator.dupe(u8, new_acc);
                if (aw.accumulator) |old| self.allocator.free(old);
                aw.accumulator = owned_new;
                aw.count += 1;

                const result = self.trigger.shouldFire(
                    window,
                    ctx.watermark(),
                    ctx.processingTime(),
                    aw.count,
                );
                if (result == .fire or result == .fire_and_purge) {
                    try self.emitResult(aw.key, self.getResultVal(owned_new), window.end_ms, ctx);
                }
                if (result == .fire_and_purge or result == .purge) {
                    self.purgeEntry(aw);
                }
            } else {
                // ---- First element in a new window ----
                const first_acc = self.applyFn(null, rec.value);
                const owned_acc = try self.allocator.dupe(u8, first_acc);
                const owned_key = try self.allocator.dupe(u8, rec.key);
                errdefer self.allocator.free(owned_key);
                errdefer self.allocator.free(owned_acc);

                try self.active_windows.append(self.allocator, .{
                    .key = owned_key,
                    .window = window,
                    .accumulator = owned_acc,
                    .count = 1,
                });

                const result = self.trigger.shouldFire(
                    window,
                    ctx.watermark(),
                    ctx.processingTime(),
                    1,
                );
                if (result == .fire or result == .fire_and_purge) {
                    try self.emitResult(owned_key, self.getResultVal(owned_acc), window.end_ms, ctx);
                }
                if (result == .fire_and_purge or result == .purge) {
                    self.purgeEntry(&self.active_windows.items[self.active_windows.items.len - 1]);
                }
            }
        }
    }

    fn processWatermarkFn(ptr: *anyopaque, watermark_val: Watermark, ctx: *OperatorContext) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        ctx.current_watermark_ms = watermark_val.timestamp_ms;

        // Check all live windows against the new watermark
        for (self.active_windows.items) |*aw| {
            if (aw.count == 0) continue; // purged stub

            const result = self.trigger.shouldFire(
                aw.window,
                watermark_val.timestamp_ms,
                ctx.processingTime(),
                aw.count,
            );
            if (result == .fire or result == .fire_and_purge) {
                if (aw.accumulator) |acc| {
                    try self.emitResult(aw.key, self.getResultVal(acc), aw.window.end_ms, ctx);
                }
            }
            if (result == .fire_and_purge or result == .purge) {
                self.purgeEntry(aw);
            }
        }
    }

    fn getNameFn(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn closeFn(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    /// Snapshot: serialize all active (non-purged) window entries.
    /// Format: [count:u32] [entries...]
    /// Entry: [key_len:u32] [key] [start:i64] [end:i64] [acc_len:u32] [acc] [count:u64]
    fn snapshotStateFn(ptr: *anyopaque, _: u64, alloc: Allocator) anyerror!?[]u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Count live entries
        var live: u32 = 0;
        for (self.active_windows.items) |aw| {
            if (aw.count > 0) live += 1;
        }
        if (live == 0) return null;

        // Calculate size
        var size: usize = @sizeOf(u32); // entry count
        for (self.active_windows.items) |aw| {
            if (aw.count == 0) continue;
            size += @sizeOf(u32) + aw.key.len; // key_len + key
            size += @sizeOf(i64) * 2; // start + end
            const acc_len: usize = if (aw.accumulator) |a| a.len else 0;
            size += @sizeOf(u32) + acc_len; // acc_len + acc
            size += @sizeOf(u64); // count
        }

        const buf = try alloc.alloc(u8, size);
        var pos: usize = 0;

        // Write count
        @memcpy(buf[pos..][0..@sizeOf(u32)], std.mem.asBytes(&live));
        pos += @sizeOf(u32);

        for (self.active_windows.items) |aw| {
            if (aw.count == 0) continue;

            // key
            const kl: u32 = @intCast(aw.key.len);
            @memcpy(buf[pos..][0..@sizeOf(u32)], std.mem.asBytes(&kl));
            pos += @sizeOf(u32);
            @memcpy(buf[pos..][0..aw.key.len], aw.key);
            pos += aw.key.len;

            // window
            @memcpy(buf[pos..][0..@sizeOf(i64)], std.mem.asBytes(&aw.window.start_ms));
            pos += @sizeOf(i64);
            @memcpy(buf[pos..][0..@sizeOf(i64)], std.mem.asBytes(&aw.window.end_ms));
            pos += @sizeOf(i64);

            // accumulator
            const acc_data = aw.accumulator orelse &[0]u8{};
            const al: u32 = @intCast(acc_data.len);
            @memcpy(buf[pos..][0..@sizeOf(u32)], std.mem.asBytes(&al));
            pos += @sizeOf(u32);
            if (acc_data.len > 0) {
                @memcpy(buf[pos..][0..acc_data.len], acc_data);
                pos += acc_data.len;
            }

            // count
            @memcpy(buf[pos..][0..@sizeOf(u64)], std.mem.asBytes(&aw.count));
            pos += @sizeOf(u64);
        }

        return buf;
    }

    /// Restore: deserialize window entries from checkpoint data.
    fn restoreStateFn(ptr: *anyopaque, _: u64, data: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Clear existing state
        for (self.active_windows.items) |aw| {
            if (aw.count > 0) {
                self.allocator.free(aw.key);
                if (aw.accumulator) |acc| self.allocator.free(acc);
            }
        }
        self.active_windows.clearRetainingCapacity();

        if (data.len < @sizeOf(u32)) return;

        var pos: usize = 0;
        const entry_count = std.mem.bytesToValue(u32, data[pos..][0..@sizeOf(u32)]);
        pos += @sizeOf(u32);

        var i: u32 = 0;
        while (i < entry_count) : (i += 1) {
            // key
            const kl = std.mem.bytesToValue(u32, data[pos..][0..@sizeOf(u32)]);
            pos += @sizeOf(u32);
            const key = try self.allocator.dupe(u8, data[pos..][0..kl]);
            pos += kl;

            // window
            const start = std.mem.bytesToValue(i64, data[pos..][0..@sizeOf(i64)]);
            pos += @sizeOf(i64);
            const end = std.mem.bytesToValue(i64, data[pos..][0..@sizeOf(i64)]);
            pos += @sizeOf(i64);

            // accumulator
            const al = std.mem.bytesToValue(u32, data[pos..][0..@sizeOf(u32)]);
            pos += @sizeOf(u32);
            const acc = if (al > 0) try self.allocator.dupe(u8, data[pos..][0..al]) else null;
            pos += al;

            // count
            const count = std.mem.bytesToValue(u64, data[pos..][0..@sizeOf(u64)]);
            pos += @sizeOf(u64);

            try self.active_windows.append(self.allocator, .{
                .key = key,
                .window = .{ .start_ms = start, .end_ms = end },
                .accumulator = acc,
                .count = count,
            });
        }
    }
};

// =============================================================================
// Tests
// =============================================================================

const OutputCollector = @import("../collector.zig").OutputCollector;
const OperatorMetrics = @import("../context.zig").OperatorMetrics;

/// Reduce function: keep the longest string
fn keepLongest(a: []const u8, b: []const u8) []const u8 {
    if (b.len > a.len) return b;
    return a;
}

fn makeContext(
    allocator: Allocator,
    collector: *OutputCollector,
    metrics: *OperatorMetrics,
) OperatorContext {
    return .{
        .collector = collector,
        .metrics = metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-window",
    };
}

test "WindowOperator count trigger fires after threshold" {
    const allocator = std.testing.allocator;

    var win_op = WindowOperator.init(
        allocator,
        "count-window",
        .{ .tumbling = .{ .size_ms = 5000 } },
        .{ .count = .{ .threshold = 3 } },
        .{ .reduce = &keepLongest },
    );
    defer win_op.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = makeContext(allocator, &collector, &metrics);

    const op = win_op.operator();

    // First two records: accumulate, no fire
    try op.processElement(ProcessingRecord.init("a", "x", 1000), &ctx);
    try std.testing.expectEqual(@as(usize, 0), collector.count());

    try op.processElement(ProcessingRecord.init("a", "yy", 2000), &ctx);
    try std.testing.expectEqual(@as(usize, 0), collector.count());

    // Third record: count=3 → fire_and_purge
    try op.processElement(ProcessingRecord.init("a", "zzz", 3000), &ctx);
    try std.testing.expectEqual(@as(usize, 1), collector.count());

    const results = collector.drain();
    try std.testing.expectEqualStrings("a", results[0].key);
    try std.testing.expectEqualStrings("zzz", results[0].value); // longest
    try std.testing.expectEqual(@as(i64, 5000), results[0].event_time_ms); // window end

    // Window purged
    try std.testing.expectEqual(@as(usize, 0), win_op.activeCount());
}

test "WindowOperator event-time trigger fires on watermark" {
    const allocator = std.testing.allocator;

    var win_op = WindowOperator.init(
        allocator,
        "event-window",
        .{ .tumbling = .{ .size_ms = 5000 } },
        .{ .event_time = {} },
        .{ .reduce = &keepLongest },
    );
    defer win_op.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = makeContext(allocator, &collector, &metrics);

    const op = win_op.operator();

    // Send two records into window [0, 5000)
    try op.processElement(ProcessingRecord.init("b", "hello", 1000), &ctx);
    try op.processElement(ProcessingRecord.init("b", "hi", 2000), &ctx);
    try std.testing.expectEqual(@as(usize, 0), collector.count());
    try std.testing.expectEqual(@as(usize, 1), win_op.activeCount());

    // Watermark before window end → no fire
    try op.processWatermark(Watermark.init(4999), &ctx);
    try std.testing.expectEqual(@as(usize, 0), collector.count());

    // Watermark at window end → fire
    try op.processWatermark(Watermark.init(5000), &ctx);
    try std.testing.expectEqual(@as(usize, 1), collector.count());

    const results = collector.drain();
    try std.testing.expectEqualStrings("b", results[0].key);
    try std.testing.expectEqualStrings("hello", results[0].value); // "hello" > "hi"
    try std.testing.expectEqual(@as(i64, 5000), results[0].event_time_ms);

    // Window purged
    try std.testing.expectEqual(@as(usize, 0), win_op.activeCount());
}

test "WindowOperator multiple keys in same window" {
    const allocator = std.testing.allocator;

    var win_op = WindowOperator.init(
        allocator,
        "multi-key-window",
        .{ .tumbling = .{ .size_ms = 10000 } },
        .{ .event_time = {} },
        .{ .reduce = &keepLongest },
    );
    defer win_op.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = makeContext(allocator, &collector, &metrics);

    const op = win_op.operator();

    // Two different keys in the same time window
    try op.processElement(ProcessingRecord.init("alice", "data-a", 1000), &ctx);
    try op.processElement(ProcessingRecord.init("bob", "data-b", 2000), &ctx);
    try std.testing.expectEqual(@as(usize, 2), win_op.activeCount());

    // Watermark fires both windows
    try op.processWatermark(Watermark.init(10000), &ctx);
    try std.testing.expectEqual(@as(usize, 2), collector.count());
    try std.testing.expectEqual(@as(usize, 0), win_op.activeCount());
}
