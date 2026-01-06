//! JSON Aggregate Operator (Declarative)
//!
//! A declarative aggregation operator that accumulates numeric values
//! extracted from JSON-encoded record values. Unlike AggregateOperator
//! (which takes Zig function pointers), this operator is configurable
//! entirely from YAML job definitions.
//!
//! Supported aggregation functions:
//!   - `sum`   — running sum of field values
//!   - `count` — count of records (field is optional)
//!   - `avg`   — arithmetic mean (sum / count)
//!   - `min`   — minimum observed value
//!   - `max`   — maximum observed value
//!
//! Window modes:
//!   - **Running** (no `window` config) — emit updated aggregate on every record
//!   - **Tumbling time** (`window: tumbling`, `window_size: <seconds>`) —
//!     accumulate per-key, emit on watermark when window closes
//!   - **Count-based** (`window: count`, `window_size: <n>`) —
//!     accumulate per-key, emit after every N records
//!
//! Output format (JSON):
//!   `{"value": <result>, "count": <n>}`
//!
//! Records are grouped by their key. If you need custom grouping,
//! chain a `keyby` operator before the aggregate.
//!
//! YAML examples:
//!   ```yaml
//!   # Running total (emit on every record)
//!   - type: aggregate
//!     name: running-total
//!     function: sum
//!     field: "$.amount"
//!
//!   # Count records per key in 60-second tumbling windows
//!   - type: aggregate
//!     name: event-counter
//!     function: count
//!     window: tumbling
//!     window_size: 60
//!
//!   # Average every 100 records per key
//!   - type: aggregate
//!     name: batch-avg
//!     function: avg
//!     field: "$.latency_ms"
//!     window: count
//!     window_size: 100
//!   ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("../operator.zig").Operator;
const OperatorContext = @import("../context.zig").OperatorContext;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const Watermark = record_mod.Watermark;
const log = @import("stdx").log;

pub const JsonAggregateOperator = struct {
    name: []const u8,
    allocator: Allocator,

    // -- Configuration (immutable after init) --

    function: AggFunction,
    /// Pre-split JSONPath field segments (null for count without field)
    field_segments: ?[]const []const u8,
    window: WindowConfig,

    // -- Per-key state --

    groups: std.StringArrayHashMapUnmanaged(Group),

    // =========================================================================
    // Types
    // =========================================================================

    pub const AggFunction = enum {
        sum,
        count,
        avg,
        min,
        max,

        pub fn fromString(s: []const u8) ?AggFunction {
            if (std.mem.eql(u8, s, "sum")) return .sum;
            if (std.mem.eql(u8, s, "count")) return .count;
            if (std.mem.eql(u8, s, "avg")) return .avg;
            if (std.mem.eql(u8, s, "min")) return .min;
            if (std.mem.eql(u8, s, "max")) return .max;
            return null;
        }

        pub fn toStr(self: AggFunction) []const u8 {
            return switch (self) {
                .sum => "sum",
                .count => "count",
                .avg => "avg",
                .min => "min",
                .max => "max",
            };
        }
    };

    pub const WindowConfig = union(enum) {
        /// Running aggregate — emit on every input record
        none,
        /// Tumbling time window — emit when watermark closes window (ms)
        tumbling_time: i64,
        /// Count-based window — emit after N records per key
        tumbling_count: u64,
    };

    pub const Group = struct {
        sum: f64 = 0,
        count: u64 = 0,
        min: f64 = std.math.floatMax(f64),
        max: f64 = -std.math.floatMax(f64),
        window_start_ms: i64 = 0,

        fn reset(self: *Group) void {
            const ws = self.window_start_ms;
            self.* = .{ .window_start_ms = ws };
        }

        fn addValue(self: *Group, val: f64) void {
            self.sum += val;
            self.count += 1;
            if (val < self.min) self.min = val;
            if (val > self.max) self.max = val;
        }

        fn addCount(self: *Group) void {
            self.count += 1;
        }

        fn getResult(self: *const Group, function: AggFunction) f64 {
            return switch (function) {
                .sum => self.sum,
                .count => @floatFromInt(self.count),
                .avg => if (self.count > 0) self.sum / @as(f64, @floatFromInt(self.count)) else 0,
                .min => if (self.count > 0) self.min else 0,
                .max => if (self.count > 0) self.max else 0,
            };
        }
    };

    const Self = @This();

    // =========================================================================
    // Lifecycle
    // =========================================================================

    pub fn init(
        allocator: Allocator,
        name: []const u8,
        function: AggFunction,
        field_expression: ?[]const u8,
        window: WindowConfig,
    ) !Self {
        // Split JSONPath field expression into segments
        const segments: ?[]const []const u8 = if (field_expression) |expr| blk: {
            const field_path = if (std.mem.startsWith(u8, expr, "$."))
                expr[2..]
            else
                expr;

            var seg_list: std.ArrayListUnmanaged([]const u8) = .{};
            var iter = std.mem.splitScalar(u8, field_path, '.');
            while (iter.next()) |seg| {
                if (seg.len > 0) try seg_list.append(allocator, seg);
            }
            break :blk try seg_list.toOwnedSlice(allocator);
        } else null;

        return .{
            .name = name,
            .allocator = allocator,
            .function = function,
            .field_segments = segments,
            .window = window,
            .groups = .{},
        };
    }

    pub fn deinit(self: *Self) void {
        // Free duped key strings
        for (self.groups.keys()) |k| {
            self.allocator.free(k);
        }
        self.groups.deinit(self.allocator);

        // Free field segments
        if (self.field_segments) |segs| {
            self.allocator.free(segs);
        }
    }

    /// Return an Operator interface backed by this aggregate operator
    pub fn operator(self: *Self) Operator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    // =========================================================================
    // Vtable
    // =========================================================================

    const vtable = Operator.VTable{
        .processElement = processElement,
        .processWatermark = processWatermark,
        .getName = getName,
        .close = closeFn,
        .snapshotState = snapshotState,
        .restoreState = restoreState,
    };

    fn processElement(ptr: *anyopaque, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Extract numeric value (for non-count functions)
        const numeric_val: ?f64 = if (self.function != .count)
            self.extractNumericField(rec.value) orelse return // skip non-numeric records
        else
            null;

        // Get or create group for this key
        const group = try self.getOrCreateGroup(rec.key, rec.event_time_ms);

        // Update accumulator
        if (numeric_val) |val| {
            group.addValue(val);
        } else {
            group.addCount();
        }

        // Emit based on window mode
        switch (self.window) {
            .none => {
                // Running aggregate — emit on every record
                try self.emitResult(rec.key, group, rec.event_time_ms, ctx);
            },
            .tumbling_count => |threshold| {
                if (group.count >= threshold) {
                    try self.emitResult(rec.key, group, rec.event_time_ms, ctx);
                    group.reset();
                }
            },
            .tumbling_time => {
                // Time windows emit on watermark, not on record arrival
            },
        }
    }

    fn processWatermark(ptr: *anyopaque, wm: Watermark, ctx: *OperatorContext) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        switch (self.window) {
            .tumbling_time => |window_size_ms| {
                // Close windows where watermark has passed the window end
                for (self.groups.keys(), self.groups.values()) |key, *group| {
                    const window_end = group.window_start_ms + window_size_ms;
                    if (wm.timestamp_ms >= window_end and group.count > 0) {
                        try self.emitResult(key, group, window_end, ctx);
                        group.reset();
                        group.window_start_ms = window_end;
                    }
                }
            },
            else => {},
        }
    }

    fn getName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn closeFn(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    // =========================================================================
    // State serialization (checkpoint support)
    // =========================================================================

    fn snapshotState(ptr: *anyopaque, _: u64, alloc: Allocator) anyerror!?[]u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.groups.count() == 0) return null;

        // Serialize as: num_groups | (key_len | key | Group fields)*
        var total: usize = 4; // u32 group count
        for (self.groups.keys()) |key| {
            total += 4 + key.len; // u32 key_len + key bytes
            total += 8 + 8 + 8 + 8 + 8; // sum(f64) + count(u64) + min(f64) + max(f64) + window_start(i64)
        }

        const buf = try alloc.alloc(u8, total);
        var offset: usize = 0;

        // Group count
        @memcpy(buf[offset..][0..4], std.mem.asBytes(&@as(u32, @intCast(self.groups.count()))));
        offset += 4;

        for (self.groups.keys(), self.groups.values()) |key, group| {
            // Key length + key
            @memcpy(buf[offset..][0..4], std.mem.asBytes(&@as(u32, @intCast(key.len))));
            offset += 4;
            @memcpy(buf[offset..][0..key.len], key);
            offset += key.len;

            // Group fields (fixed-size binary)
            @memcpy(buf[offset..][0..8], std.mem.asBytes(&group.sum));
            offset += 8;
            @memcpy(buf[offset..][0..8], std.mem.asBytes(&group.count));
            offset += 8;
            @memcpy(buf[offset..][0..8], std.mem.asBytes(&group.min));
            offset += 8;
            @memcpy(buf[offset..][0..8], std.mem.asBytes(&group.max));
            offset += 8;
            @memcpy(buf[offset..][0..8], std.mem.asBytes(&group.window_start_ms));
            offset += 8;
        }

        return buf;
    }

    fn restoreState(ptr: *anyopaque, _: u64, data: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (data.len < 4) return;
        var offset: usize = 0;

        const num_groups = std.mem.bytesToValue(u32, data[offset..][0..4]);
        offset += 4;

        for (0..num_groups) |_| {
            if (offset + 4 > data.len) return;
            const key_len = std.mem.bytesToValue(u32, data[offset..][0..4]);
            offset += 4;

            if (offset + key_len + 40 > data.len) return;
            const key = data[offset..][0..key_len];
            offset += key_len;

            var group: Group = .{};
            group.sum = std.mem.bytesToValue(f64, data[offset..][0..8]);
            offset += 8;
            group.count = std.mem.bytesToValue(u64, data[offset..][0..8]);
            offset += 8;
            group.min = std.mem.bytesToValue(f64, data[offset..][0..8]);
            offset += 8;
            group.max = std.mem.bytesToValue(f64, data[offset..][0..8]);
            offset += 8;
            group.window_start_ms = std.mem.bytesToValue(i64, data[offset..][0..8]);
            offset += 8;

            const key_dupe = try self.allocator.dupe(u8, key);
            try self.groups.put(self.allocator, key_dupe, group);
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    fn getOrCreateGroup(self: *Self, key: []const u8, event_time_ms: i64) !*Group {
        if (self.groups.getPtr(key)) |existing| {
            // For tumbling time windows: if event belongs to a new window, emit old & reset
            switch (self.window) {
                .tumbling_time => |window_size_ms| {
                    const event_window_start = @divFloor(event_time_ms, window_size_ms) * window_size_ms;
                    if (event_window_start != existing.window_start_ms and existing.count > 0) {
                        // New window period — reset accumulator
                        existing.reset();
                        existing.window_start_ms = event_window_start;
                    }
                },
                else => {},
            }
            return existing;
        }

        // New key — dupe and insert
        const key_dupe = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_dupe);

        const window_start: i64 = switch (self.window) {
            .tumbling_time => |window_size_ms| @divFloor(event_time_ms, window_size_ms) * window_size_ms,
            else => 0,
        };

        try self.groups.put(self.allocator, key_dupe, .{ .window_start_ms = window_start });
        return self.groups.getPtr(key_dupe).?;
    }

    fn emitResult(self: *Self, key: []const u8, group: *const Group, event_time_ms: i64, ctx: *OperatorContext) !void {
        const result = group.getResult(self.function);

        // Format output as JSON: {"value": <result>, "count": <n>}
        const output = try std.fmt.allocPrint(
            self.allocator,
            "{{\"value\":{d},\"count\":{d}}}",
            .{ result, group.count },
        );
        errdefer self.allocator.free(output);

        // Dupe key to match owned-memory contract
        const key_dupe = try self.allocator.dupe(u8, key);

        // Emit as an owned record — collector will free via clearRetained
        try ctx.emit(.{
            .key = key_dupe,
            .value = output,
            .event_time_ms = event_time_ms,
            .source = record_mod.SourceRef.EMPTY,
            .headers = &.{},
            .owns_memory = true,
        });
    }

    fn extractNumericField(self: *const Self, value: []const u8) ?f64 {
        const segments = self.field_segments orelse return null;

        const parsed = std.json.parseFromSlice(
            std.json.Value,
            self.allocator,
            value,
            .{},
        ) catch return null;
        defer parsed.deinit();

        var current = parsed.value;
        for (segments) |seg| {
            switch (current) {
                .object => |obj| {
                    current = obj.get(seg) orelse return null;
                },
                else => return null,
            }
        }

        return switch (current) {
            .integer => |n| @as(f64, @floatFromInt(n)),
            .float => |f| f,
            else => null,
        };
    }

    // =========================================================================
    // Test helpers
    // =========================================================================

    /// Get the current group state for a key (for testing)
    pub fn getGroup(self: *const Self, key: []const u8) ?Group {
        return self.groups.get(key);
    }

    /// Get the number of active groups (for testing)
    pub fn groupCount(self: *const Self) usize {
        return self.groups.count();
    }
};

// =============================================================================
// Tests
// =============================================================================

const OutputCollector = @import("../collector.zig").OutputCollector;
const OperatorMetrics = @import("../context.zig").OperatorMetrics;

test "JsonAggregateOperator — getName" {
    const allocator = std.testing.allocator;
    var agg = try JsonAggregateOperator.init(allocator, "hourly-sum", .sum, "$.amount", .none);
    defer agg.deinit();

    const op = agg.operator();
    try std.testing.expectEqualStrings("hourly-sum", op.getName());
}

test "JsonAggregateOperator — running sum" {
    const allocator = std.testing.allocator;
    var agg = try JsonAggregateOperator.init(allocator, "total", .sum, "$.amount", .none);
    defer agg.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-agg",
    };

    const op = agg.operator();

    try op.processElement(ProcessingRecord.init("k1", "{\"amount\":10}", 100), &ctx);
    try std.testing.expectEqual(@as(usize, 1), collector.count());

    try op.processElement(ProcessingRecord.init("k1", "{\"amount\":25.5}", 200), &ctx);
    try std.testing.expectEqual(@as(usize, 2), collector.count());

    // Check the aggregate state
    const group = agg.getGroup("k1").?;
    try std.testing.expectEqual(@as(f64, 35.5), group.sum);
    try std.testing.expectEqual(@as(u64, 2), group.count);

    collector.clear();
}

test "JsonAggregateOperator — running count" {
    const allocator = std.testing.allocator;
    var agg = try JsonAggregateOperator.init(allocator, "counter", .count, null, .none);
    defer agg.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-agg",
    };

    const op = agg.operator();

    try op.processElement(ProcessingRecord.init("k1", "any-data", 100), &ctx);
    try op.processElement(ProcessingRecord.init("k1", "more-data", 200), &ctx);
    try op.processElement(ProcessingRecord.init("k2", "other", 300), &ctx);

    try std.testing.expectEqual(@as(usize, 3), collector.count());
    try std.testing.expectEqual(@as(u64, 2), agg.getGroup("k1").?.count);
    try std.testing.expectEqual(@as(u64, 1), agg.getGroup("k2").?.count);
    try std.testing.expectEqual(@as(usize, 2), agg.groupCount());

    collector.clear();
}

test "JsonAggregateOperator — running avg" {
    const allocator = std.testing.allocator;
    var agg = try JsonAggregateOperator.init(allocator, "avg-latency", .avg, "$.latency", .none);
    defer agg.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-agg",
    };

    const op = agg.operator();

    try op.processElement(ProcessingRecord.init("svc", "{\"latency\":10}", 100), &ctx);
    try op.processElement(ProcessingRecord.init("svc", "{\"latency\":30}", 200), &ctx);

    const group = agg.getGroup("svc").?;
    try std.testing.expectEqual(@as(f64, 20), group.getResult(.avg));

    collector.clear();
}

test "JsonAggregateOperator — min and max" {
    const allocator = std.testing.allocator;
    var agg_min = try JsonAggregateOperator.init(allocator, "min-price", .min, "$.price", .none);
    defer agg_min.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-agg",
    };

    const op = agg_min.operator();

    try op.processElement(ProcessingRecord.init("k", "{\"price\":50}", 100), &ctx);
    try op.processElement(ProcessingRecord.init("k", "{\"price\":20}", 200), &ctx);
    try op.processElement(ProcessingRecord.init("k", "{\"price\":80}", 300), &ctx);

    const group = agg_min.getGroup("k").?;
    try std.testing.expectEqual(@as(f64, 20), group.getResult(.min));
    try std.testing.expectEqual(@as(f64, 80), group.getResult(.max));

    collector.clear();
}

test "JsonAggregateOperator — count-based tumbling window" {
    const allocator = std.testing.allocator;
    var agg = try JsonAggregateOperator.init(
        allocator,
        "batch-sum",
        .sum,
        "$.v",
        .{ .tumbling_count = 3 },
    );
    defer agg.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-agg",
    };

    const op = agg.operator();

    // First 2 records — no emission yet (threshold = 3)
    try op.processElement(ProcessingRecord.init("k1", "{\"v\":1}", 100), &ctx);
    try op.processElement(ProcessingRecord.init("k1", "{\"v\":2}", 200), &ctx);
    try std.testing.expectEqual(@as(usize, 0), collector.count());

    // Third record triggers emission
    try op.processElement(ProcessingRecord.init("k1", "{\"v\":3}", 300), &ctx);
    try std.testing.expectEqual(@as(usize, 1), collector.count());

    // Group should be reset after emission
    const group = agg.getGroup("k1").?;
    try std.testing.expectEqual(@as(u64, 0), group.count);
    try std.testing.expectEqual(@as(f64, 0), group.sum);

    collector.clear();
}

test "JsonAggregateOperator — tumbling time window on watermark" {
    const allocator = std.testing.allocator;
    // 10-second tumbling window (10_000 ms)
    var agg = try JsonAggregateOperator.init(
        allocator,
        "windowed",
        .sum,
        "$.v",
        .{ .tumbling_time = 10_000 },
    );
    defer agg.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-agg",
    };

    const op = agg.operator();

    // Records in first window [0, 10_000)
    try op.processElement(ProcessingRecord.init("k1", "{\"v\":5}", 1000), &ctx);
    try op.processElement(ProcessingRecord.init("k1", "{\"v\":3}", 5000), &ctx);
    try std.testing.expectEqual(@as(usize, 0), collector.count()); // no emission yet

    // Watermark at 10_000 — closes the [0, 10_000) window
    try op.processWatermark(.{ .timestamp_ms = 10_000, .source_index = 0 }, &ctx);
    try std.testing.expectEqual(@as(usize, 1), collector.count());

    // Group should be reset for next window
    const group = agg.getGroup("k1").?;
    try std.testing.expectEqual(@as(u64, 0), group.count);
    try std.testing.expectEqual(@as(i64, 10_000), group.window_start_ms);

    collector.clear();
}

test "JsonAggregateOperator — nested JSON field" {
    const allocator = std.testing.allocator;
    var agg = try JsonAggregateOperator.init(allocator, "nested", .sum, "$.stats.value", .none);
    defer agg.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-agg",
    };

    const op = agg.operator();

    try op.processElement(ProcessingRecord.init("k", "{\"stats\":{\"value\":42}}", 100), &ctx);
    const group = agg.getGroup("k").?;
    try std.testing.expectEqual(@as(f64, 42), group.sum);

    collector.clear();
}

test "JsonAggregateOperator — skips non-numeric / missing field" {
    const allocator = std.testing.allocator;
    var agg = try JsonAggregateOperator.init(allocator, "strict", .sum, "$.amount", .none);
    defer agg.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-agg",
    };

    const op = agg.operator();

    // Missing field — record skipped
    try op.processElement(ProcessingRecord.init("k", "{\"other\":1}", 100), &ctx);
    try std.testing.expectEqual(@as(usize, 0), collector.count());
    try std.testing.expectEqual(@as(usize, 0), agg.groupCount());

    // Invalid JSON — record skipped
    try op.processElement(ProcessingRecord.init("k", "not-json", 200), &ctx);
    try std.testing.expectEqual(@as(usize, 0), collector.count());

    // Non-numeric field — record skipped
    try op.processElement(ProcessingRecord.init("k", "{\"amount\":\"text\"}", 300), &ctx);
    try std.testing.expectEqual(@as(usize, 0), collector.count());

    collector.clear();
}

test "JsonAggregateOperator — multiple keys grouping" {
    const allocator = std.testing.allocator;
    var agg = try JsonAggregateOperator.init(allocator, "per-user", .sum, "$.amount", .none);
    defer agg.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-agg",
    };

    const op = agg.operator();

    try op.processElement(ProcessingRecord.init("alice", "{\"amount\":100}", 100), &ctx);
    try op.processElement(ProcessingRecord.init("bob", "{\"amount\":200}", 200), &ctx);
    try op.processElement(ProcessingRecord.init("alice", "{\"amount\":50}", 300), &ctx);

    try std.testing.expectEqual(@as(f64, 150), agg.getGroup("alice").?.sum);
    try std.testing.expectEqual(@as(f64, 200), agg.getGroup("bob").?.sum);
    try std.testing.expectEqual(@as(usize, 2), agg.groupCount());

    collector.clear();
}

test "JsonAggregateOperator — snapshot and restore" {
    const allocator = std.testing.allocator;
    var agg = try JsonAggregateOperator.init(allocator, "snap", .sum, "$.v", .none);
    defer agg.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test-agg",
    };

    const op = agg.operator();

    try op.processElement(ProcessingRecord.init("k1", "{\"v\":10}", 100), &ctx);
    try op.processElement(ProcessingRecord.init("k2", "{\"v\":20}", 200), &ctx);

    // Snapshot
    const snapshot = try op.snapshotState(1, allocator);
    try std.testing.expect(snapshot != null);
    defer allocator.free(snapshot.?);

    // Create a new operator and restore
    var agg2 = try JsonAggregateOperator.init(allocator, "snap2", .sum, "$.v", .none);
    defer agg2.deinit();

    const op2 = agg2.operator();
    try op2.restoreState(1, snapshot.?);

    try std.testing.expectEqual(@as(usize, 2), agg2.groupCount());
    try std.testing.expectEqual(@as(f64, 10), agg2.getGroup("k1").?.sum);
    try std.testing.expectEqual(@as(f64, 20), agg2.getGroup("k2").?.sum);

    collector.clear();
}
