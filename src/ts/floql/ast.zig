/// FloQL AST — Abstract Syntax Tree for the FloQL query language.
///
/// FloQL grammar (LL(1)):
///   query     = source ("|" stage)*
///   source    = measurement ["{" filter "}"] "[" range "]"
///   stage     = function "(" args ")"
///   filter    = tag_match ("," tag_match)*
///   tag_match = key op value
///   range     = duration | absolute ".." absolute
///
/// The AST is produced by the parser and consumed by the executor.
/// All string values borrow from the original query source.
const std = @import("std");
const Allocator = std.mem.Allocator;

// ============================================================================
// Top-Level Types
// ============================================================================

/// A complete FloQL query: source + pipeline stages.
pub const Query = struct {
    source: Source,
    stages: []const Stage,

    pub fn deinit(self: *Query, allocator: Allocator) void {
        self.source.deinit(allocator);
        for (self.stages) |*stage| {
            @constCast(stage).deinit(allocator);
        }
        if (self.stages.len > 0) {
            allocator.free(self.stages);
        }
    }
};

/// Source selector: measurement{filter}[range]
pub const Source = struct {
    measurement: []const u8,
    filters: []const TagFilter = &.{},
    range: TimeRange,

    pub fn deinit(self: *Source, allocator: Allocator) void {
        if (self.filters.len > 0) {
            allocator.free(self.filters);
        }
    }
};

/// Tag filter: key op value
pub const TagFilter = struct {
    key: []const u8,
    op: FilterOp,
    value: []const u8,
};

/// Filter comparison operators
pub const FilterOp = enum {
    eq, // =
    neq, // !=
    regex, // =~
    nregex, // !~
};

/// Time range specification.
/// Duration-only: [1h] means "last 1 hour from now".
/// Explicit: [from..to] for absolute timestamps or durations.
pub const TimeRange = struct {
    /// Duration in milliseconds (for relative ranges like "1h", "30m")
    duration_ms: i64 = 0,

    /// Explicit from/to (ms since epoch, 0 = unset → use duration_ms)
    from_ms: i64 = 0,
    to_ms: i64 = 0,
};

// ============================================================================
// Pipeline Stages
// ============================================================================

/// A pipeline stage: each takes a SeriesSet and produces a SeriesSet.
pub const Stage = union(enum) {
    window: WindowStage,
    aggregate: AggStage,
    rate: RateStage,
    delta: DeltaStage,
    where_filter: WhereStage,
    group_by: GroupByStage,
    math: MathStage,
    topk: TopKStage,
    bottomk: BottomKStage,
    alias_stage: AliasStage,
    field: FieldStage,
    abs_stage: void,
    ceil_stage: void,
    floor_stage: void,
    round_stage: RoundStage,
    percentile: PercentileStage,
    first: void,
    last: void,

    pub fn deinit(self: *Stage, allocator: Allocator) void {
        _ = self;
        _ = allocator;
    }
};

pub const WindowStage = struct {
    /// Window duration in ms (e.g., 5m = 300000)
    interval_ms: i64,
};

/// Aggregation function applied per window bucket.
pub const AggStage = struct {
    function: AggFunction,
};

pub const AggFunction = enum {
    avg,
    sum,
    count,
    min,
    max,

    pub fn fromString(s: []const u8) ?AggFunction {
        const map = std.StaticStringMap(AggFunction).initComptime(.{
            .{ "avg", .avg },
            .{ "sum", .sum },
            .{ "count", .count },
            .{ "min", .min },
            .{ "max", .max },
        });
        return map.get(s);
    }

    pub fn toString(self: AggFunction) []const u8 {
        return switch (self) {
            .avg => "avg",
            .sum => "sum",
            .count => "count",
            .min => "min",
            .max => "max",
        };
    }
};

pub const RateStage = struct {
    /// Rate calculation interval in ms (e.g., 1m = 60000)
    interval_ms: i64,
};

pub const DeltaStage = struct {
    placeholder: u8 = 0,
};

pub const WhereStage = struct {
    predicate: Predicate,
};

pub const Predicate = struct {
    op: CompareOp,
    value: f64,
};

pub const CompareOp = enum {
    gt, // >
    gte, // >=
    lt, // <
    lte, // <=
    eq, // ==
    neq, // !=
};

pub const GroupByStage = struct {
    tag: []const u8,
};

/// Simple arithmetic operation applied to each point's value.
///
/// Syntax: math(value * 100), math(/ 1024), math(+ 273.15)
/// Supports: +, -, *, /, %
pub const MathStage = struct {
    op: MathOp,
    operand: f64,
};

pub const MathOp = enum {
    add,
    sub,
    mul,
    div,
    mod,
};

pub const TopKStage = struct {
    k: u32,
};

pub const BottomKStage = struct {
    k: u32,
};

pub const AliasStage = struct {
    name: []const u8,
};

pub const FieldStage = struct {
    name: []const u8,
};

pub const RoundStage = struct {
    decimals: u32,
};

pub const PercentileStage = struct {
    percentile: f64, // 0.0 - 100.0
};

// ============================================================================
// Duration Parsing
// ============================================================================

/// Parse a FloQL duration literal: "30s", "5m", "1h", "1d", "7d"
/// Returns duration in milliseconds, or null if invalid.
pub fn parseDuration(s: []const u8) ?i64 {
    if (s.len < 2) return null;
    const num_str = s[0 .. s.len - 1];
    const unit = s[s.len - 1];
    const num = std.fmt.parseInt(i64, num_str, 10) catch return null;
    if (num <= 0) return null;
    return switch (unit) {
        's' => num * 1000,
        'm' => num * 60 * 1000,
        'h' => num * 3600 * 1000,
        'd' => num * 86400 * 1000,
        else => null,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "ast_duration_parse" {
    const testing = std.testing;
    try testing.expectEqual(@as(i64, 30000), parseDuration("30s").?);
    try testing.expectEqual(@as(i64, 300000), parseDuration("5m").?);
    try testing.expectEqual(@as(i64, 3600000), parseDuration("1h").?);
    try testing.expectEqual(@as(i64, 86400000), parseDuration("1d").?);
    try testing.expectEqual(@as(i64, 604800000), parseDuration("7d").?);
    try testing.expect(parseDuration("") == null);
    try testing.expect(parseDuration("5") == null);
    try testing.expect(parseDuration("abc") == null);
    try testing.expect(parseDuration("0s") == null);
}

test "ast_agg_function_from_string" {
    try std.testing.expectEqual(AggFunction.avg, AggFunction.fromString("avg").?);
    try std.testing.expectEqual(AggFunction.sum, AggFunction.fromString("sum").?);
    try std.testing.expectEqual(AggFunction.count, AggFunction.fromString("count").?);
    try std.testing.expectEqual(AggFunction.min, AggFunction.fromString("min").?);
    try std.testing.expectEqual(AggFunction.max, AggFunction.fromString("max").?);
    try std.testing.expect(AggFunction.fromString("median") == null);
}
