//! Classify Operator
//!
//! Evaluates rules against each record and adds tag bits for matching
//! conditions. Records continue flowing through the chain — classify
//! never drops or forks, it only labels.
//!
//! Tags compose orthogonally: a single record can match multiple rules
//! and carry multiple tag bits. Downstream sinks filter by AND-matching
//! their `required_tags` mask against `record.tags`.
//!
//! YAML example:
//!   ```yaml
//!   operators:
//!     - type: classify
//!       name: label-records
//!       default_tag: unmatched     # optional: tag when no rules match
//!       rules:
//!         - condition: "value_contains:error"
//!           tag: errors
//!         - condition: "json:amount>10000"
//!           tag: high-value
//!   ```
//!
//! Config format (from parser):
//!   - `rule.0.condition` / `rule.0.tag` (flattened pairs)
//!   OR
//!   - `condition_0` / `tag_0`, `condition_1` / `tag_1` (indexed pairs)
//!
//! The operator reuses ExprFilterOperator's condition parsing for
//! consistent expression support across filter and classify.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("../operator.zig").Operator;
const noOpSnapshot = @import("../operator.zig").noOpSnapshot;
const log = @import("stdx").log;
const noOpRestore = @import("../operator.zig").noOpRestore;
const OperatorContext = @import("../context.zig").OperatorContext;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const Watermark = record_mod.Watermark;
const ExprFilterOperator = @import("expr_filter.zig").ExprFilterOperator;

/// A single classification rule: condition + tag bit to set.
pub const Rule = struct {
    condition: ExprFilterOperator,
    tag_bit: u5,
};

pub const ClassifyOperator = struct {
    name: []const u8,
    rules: []Rule,
    default_tag_bit: ?u5,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, name: []const u8, rules: []Rule, default_tag_bit: ?u5) Self {
        return .{
            .name = name,
            .rules = rules,
            .default_tag_bit = default_tag_bit,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        // Free owned condition strings (duped in createClassify)
        for (self.rules) |*rule| {
            self.allocator.free(rule.condition.condition);
        }
        self.allocator.free(self.rules);
        // Free owned name (duped in createClassify)
        self.allocator.free(self.name);
    }

    /// Return an Operator interface backed by this ClassifyOperator
    pub fn operator(self: *Self) Operator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Operator.VTable{
        .processElement = processElement,
        .processWatermark = processWatermark,
        .getName = getName,
        .close = close,
        .snapshotState = noOpSnapshot,
        .restoreState = noOpRestore,
    };

    fn processElement(ptr: *anyopaque, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Evaluate every rule; set tag bits for matching conditions.
        // Record is always emitted — classify never drops.
        var tagged_rec = rec;
        var any_matched = false;
        log.debug("classify '{s}': processing record value[{d}]={s}", .{ self.name, rec.value.len, if (rec.value.len > 200) rec.value[0..200] else rec.value });
        for (self.rules) |*rule| {
            const matched = rule.condition.evaluate(rec);
            log.debug("classify '{s}': rule bit={d} cond='{s}' matched={}", .{ self.name, rule.tag_bit, rule.condition.condition, matched });
            if (matched) {
                tagged_rec.addTag(rule.tag_bit);
                any_matched = true;
            }
        }
        // Apply default tag when no rules matched
        if (!any_matched) {
            if (self.default_tag_bit) |bit| {
                tagged_rec.addTag(bit);
            }
        }
        try ctx.emit(tagged_rec);
    }

    fn processWatermark(_: *anyopaque, _: Watermark, _: *OperatorContext) !void {}

    fn getName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn close(_: *anyopaque) void {}
};

// =============================================================================
// Tests
// =============================================================================

test "ClassifyOperator single rule match" {
    const allocator = std.testing.allocator;
    const OutputCollector = @import("../collector.zig").OutputCollector;
    const OperatorMetrics = @import("../context.zig").OperatorMetrics;

    var rules = try allocator.alloc(Rule, 1);
    rules[0] = .{
        .condition = ExprFilterOperator.init("rule0", "value_contains:error"),
        .tag_bit = 0,
    };

    var op = ClassifyOperator.init(allocator, "test-classify", rules, null);
    defer op.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test",
    };

    var iface = op.operator();

    // Matching record
    try iface.processElement(ProcessingRecord.init("k", "error happened", 100), &ctx);
    try std.testing.expectEqual(@as(usize, 1), collector.count());
    const out = collector.drain();
    try std.testing.expect(out[0].hasTag(0));
    try std.testing.expectEqual(@as(u32, 1), out[0].tags);
}

test "ClassifyOperator no rules match — record still emitted" {
    const allocator = std.testing.allocator;
    const OutputCollector = @import("../collector.zig").OutputCollector;
    const OperatorMetrics = @import("../context.zig").OperatorMetrics;

    var rules = try allocator.alloc(Rule, 1);
    rules[0] = .{
        .condition = ExprFilterOperator.init("rule0", "value_contains:error"),
        .tag_bit = 0,
    };

    var op = ClassifyOperator.init(allocator, "test-classify", rules, null);
    defer op.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test",
    };

    var iface = op.operator();

    // Non-matching record — should still be emitted with tags=0
    try iface.processElement(ProcessingRecord.init("k", "all good", 100), &ctx);
    try std.testing.expectEqual(@as(usize, 1), collector.count());
    const out = collector.drain();
    try std.testing.expectEqual(@as(u32, 0), out[0].tags);
}

test "ClassifyOperator multiple rules compose" {
    const allocator = std.testing.allocator;
    const OutputCollector = @import("../collector.zig").OutputCollector;
    const OperatorMetrics = @import("../context.zig").OperatorMetrics;

    var rules = try allocator.alloc(Rule, 3);
    rules[0] = .{
        .condition = ExprFilterOperator.init("r0", "value_contains:error"),
        .tag_bit = 0,
    };
    rules[1] = .{
        .condition = ExprFilterOperator.init("r1", "value_contains:critical"),
        .tag_bit = 1,
    };
    rules[2] = .{
        .condition = ExprFilterOperator.init("r2", "not_empty"),
        .tag_bit = 2,
    };

    var op = ClassifyOperator.init(allocator, "multi-classify", rules, null);
    defer op.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test",
    };

    var iface = op.operator();

    // Record matching rules 0 and 2 (error + not_empty)
    try iface.processElement(ProcessingRecord.init("k", "error happened", 100), &ctx);
    const out1 = collector.drain();
    try std.testing.expect(out1[0].hasTag(0)); // error
    try std.testing.expect(!out1[0].hasTag(1)); // not critical
    try std.testing.expect(out1[0].hasTag(2)); // not_empty
    try std.testing.expectEqual(@as(u32, 5), out1[0].tags); // bits 0 + 2 = 1 + 4 = 5

    // Record matching all 3 rules
    try iface.processElement(ProcessingRecord.init("k", "critical error alert", 200), &ctx);
    const out2 = collector.drain();
    try std.testing.expectEqual(@as(usize, 2), out2.len); // both records in collector
    try std.testing.expectEqual(@as(u32, 7), out2[1].tags); // bits 0 + 1 + 2 = 7
    try std.testing.expect(out2[1].hasAllTags(0b111));
}

test "ClassifyOperator default tag on unmatched records" {
    const allocator = std.testing.allocator;
    const OutputCollector = @import("../collector.zig").OutputCollector;
    const OperatorMetrics = @import("../context.zig").OperatorMetrics;

    var rules = try allocator.alloc(Rule, 1);
    rules[0] = .{
        .condition = ExprFilterOperator.init("r0", "value_contains:error"),
        .tag_bit = 0,
    };

    // default_tag_bit = 3 — applied when no rules match
    var op = ClassifyOperator.init(allocator, "default-classify", rules, 3);
    defer op.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test",
    };

    var iface = op.operator();

    // Record matching a rule — default should NOT be applied
    try iface.processElement(ProcessingRecord.init("k", "error occurred", 100), &ctx);
    const out1 = collector.drain();
    try std.testing.expect(out1[0].hasTag(0));
    try std.testing.expect(!out1[0].hasTag(3));
    try std.testing.expectEqual(@as(u32, 1), out1[0].tags);

    // Record not matching any rule — default tag bit 3 applied
    try iface.processElement(ProcessingRecord.init("k", "all good", 200), &ctx);
    const out2 = collector.drain();
    try std.testing.expect(!out2[1].hasTag(0));
    try std.testing.expect(out2[1].hasTag(3));
    try std.testing.expectEqual(@as(u32, 8), out2[1].tags); // bit 3 = 8
}

test "ClassifyOperator default tag null — unmatched gets no tags" {
    const allocator = std.testing.allocator;
    const OutputCollector = @import("../collector.zig").OutputCollector;
    const OperatorMetrics = @import("../context.zig").OperatorMetrics;

    var rules = try allocator.alloc(Rule, 1);
    rules[0] = .{
        .condition = ExprFilterOperator.init("r0", "value_contains:error"),
        .tag_bit = 0,
    };

    // No default tag
    var op = ClassifyOperator.init(allocator, "no-default", rules, null);
    defer op.deinit();

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 0,
        .current_watermark_ms = 0,
        .operator_name = "test",
    };

    var iface = op.operator();

    // Unmatched record — tags stay 0
    try iface.processElement(ProcessingRecord.init("k", "all good", 100), &ctx);
    const out = collector.drain();
    try std.testing.expectEqual(@as(u32, 0), out[0].tags);
}

test "ProcessingRecord tag helpers" {
    var rec = ProcessingRecord.init("k", "v", 0);
    try std.testing.expectEqual(@as(u32, 0), rec.tags);

    rec.addTag(0);
    try std.testing.expect(rec.hasTag(0));
    try std.testing.expectEqual(@as(u32, 1), rec.tags);

    rec.addTag(4);
    try std.testing.expect(rec.hasTag(4));
    try std.testing.expectEqual(@as(u32, 17), rec.tags); // 1 + 16

    try std.testing.expect(rec.hasAllTags(0b10001)); // bits 0 and 4
    try std.testing.expect(!rec.hasAllTags(0b11)); // bit 1 not set
}
