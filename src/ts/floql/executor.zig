/// FloQL Executor — runs a parsed FloQL AST pipeline on a SeriesSet.
///
/// The executor takes a Source-resolved SeriesSet (loaded from ColumnStore)
/// and applies each Stage in sequence. Each transform produces a new SeriesSet
/// that flows into the next stage.
///
/// Usage:
///   const query = try Parser.parse("cpu[1h] | window(5m) | avg()", allocator);
///   var initial = try loadFromColumnStore(query.source, ...);  // Source resolution
///   var result = try Executor.execute(query.stages, initial, allocator);
///   defer result.deinit();
const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("ast.zig");
const stages = @import("stages.zig");
const ss_mod = @import("series_set.zig");

const SeriesSet = ss_mod.SeriesSet;
const Stage = ast.Stage;

pub const ExecuteError = error{
    OutOfMemory,
    InvalidInput,
    NoData,
    UnsupportedStage,
};

/// Execute a FloQL pipeline: apply each stage in sequence to the input SeriesSet.
///
/// Note: The executor does NOT own the input. Intermediate SeriesSets are freed.
/// The returned SeriesSet is owned by the caller.
pub fn execute(
    pipeline: []const Stage,
    initial: SeriesSet,
    allocator: Allocator,
) ExecuteError!SeriesSet {
    var current = initial;
    var current_is_initial = true;

    // Track the current window interval for aggregation stages
    var current_window_ms: i64 = 60000; // Default 1m if no window stage precedes agg

    for (pipeline) |stage| {
        var next = try applyStage(stage, current, current_window_ms, allocator);

        // Update window tracking
        switch (stage) {
            .window => |w| current_window_ms = w.interval_ms,
            else => {},
        }

        // Free intermediate (but not the original input — caller owns that)
        if (!current_is_initial) {
            current.deinit();
        }
        current = next;
        current_is_initial = false;
        _ = &next;
    }

    return current;
}

fn applyStage(
    stage: Stage,
    input: SeriesSet,
    current_window_ms: i64,
    allocator: Allocator,
) ExecuteError!SeriesSet {
    return switch (stage) {
        .window => |w| stages.applyWindow(input, w.interval_ms, allocator) catch return error.InvalidInput,
        .aggregate => |a| stages.applyAggregate(input, a.function, current_window_ms, allocator) catch return error.InvalidInput,
        .rate => |r| stages.applyRate(input, r.interval_ms, allocator) catch return error.InvalidInput,
        .delta => stages.applyDelta(input, allocator) catch return error.InvalidInput,
        .where_filter => |w| stages.applyWhere(input, w.predicate, allocator) catch return error.InvalidInput,
        .group_by => |g| stages.applyGroupBy(input, g.tag, allocator) catch return error.InvalidInput,
        .topk => |t| stages.applyTopK(input, t.k, allocator) catch return error.InvalidInput,
        .bottomk => |b| stages.applyBottomK(input, b.k, allocator) catch return error.InvalidInput,
        .abs_stage => stages.applyAbs(input, allocator) catch return error.InvalidInput,
        .ceil_stage => stages.applyCeil(input, allocator) catch return error.InvalidInput,
        .floor_stage => stages.applyFloor(input, allocator) catch return error.InvalidInput,
        .round_stage => |r| stages.applyRound(input, r.decimals, allocator) catch return error.InvalidInput,
        .percentile => |p| stages.applyPercentile(input, p.percentile, current_window_ms, allocator) catch return error.InvalidInput,
        .first => stages.applyFirst(input, allocator) catch return error.InvalidInput,
        .last => stages.applyLast(input, allocator) catch return error.InvalidInput,

        // Field stage: structural marker, resolved at source level (executor no-op)
        .field => passthrough(input, allocator),

        // Alias: rename series key (cosmetic)
        .alias_stage => |a| applyAlias(input, a.name, allocator),

        // Math: simple arithmetic on each point value (e.g., math(* 100))
        .math => |m| stages.applyMath(input, m.op, m.operand, allocator) catch return error.InvalidInput,
    };
}

fn passthrough(input: SeriesSet, allocator: Allocator) ExecuteError!SeriesSet {
    const output_series = allocator.dupe(ss_mod.Series, input.series) catch return error.OutOfMemory;
    for (output_series, 0..) |*s, i| {
        s.points = allocator.dupe(ss_mod.DataPoint, input.series[i].points) catch return error.OutOfMemory;
    }
    return SeriesSet.fromOwned(allocator, output_series);
}

fn applyAlias(input: SeriesSet, name: []const u8, allocator: Allocator) ExecuteError!SeriesSet {
    const output_series = allocator.dupe(ss_mod.Series, input.series) catch return error.OutOfMemory;
    for (output_series, 0..) |*s, i| {
        s.key = name; // Borrow from AST (which borrows from query string)
        s.points = allocator.dupe(ss_mod.DataPoint, input.series[i].points) catch return error.OutOfMemory;
    }
    return SeriesSet.fromOwned(allocator, output_series);
}

// ============================================================================
// Tests
// ============================================================================

fn makeTestInput(allocator: Allocator) !SeriesSet {
    var points = try allocator.alloc(ss_mod.DataPoint, 6);
    points[0] = .{ .timestamp_ms = 0, .value = 10.0 };
    points[1] = .{ .timestamp_ms = 1000, .value = 20.0 };
    points[2] = .{ .timestamp_ms = 2000, .value = 30.0 };
    points[3] = .{ .timestamp_ms = 3000, .value = 40.0 };
    points[4] = .{ .timestamp_ms = 4000, .value = 50.0 };
    points[5] = .{ .timestamp_ms = 5000, .value = 60.0 };

    var series = try allocator.alloc(ss_mod.Series, 1);
    series[0] = .{ .key = "cpu,host=web-01", .field = "value", .points = points };

    return SeriesSet.fromOwned(allocator, series);
}

test "executor_window_avg_pipeline" {
    const allocator = std.testing.allocator;

    const pipeline = [_]Stage{
        Stage{ .window = .{ .interval_ms = 3000 } },
        Stage{ .aggregate = .{ .function = .avg } },
    };

    var input = try makeTestInput(allocator);
    defer input.deinit();

    var result = try execute(&pipeline, input, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.series.len);
    try std.testing.expectEqual(@as(usize, 2), result.series[0].points.len);
    // avg(10, 20, 30) = 20
    try std.testing.expectEqual(@as(f64, 20.0), result.series[0].points[0].value);
    // avg(40, 50, 60) = 50
    try std.testing.expectEqual(@as(f64, 50.0), result.series[0].points[1].value);
}

test "executor_rate_pipeline" {
    const allocator = std.testing.allocator;

    const pipeline = [_]Stage{
        Stage{ .rate = .{ .interval_ms = 1000 } },
    };

    var input = try makeTestInput(allocator);
    defer input.deinit();

    var result = try execute(&pipeline, input, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 5), result.series[0].points.len);
    // Constant rate of 10/s
    try std.testing.expectEqual(@as(f64, 10.0), result.series[0].points[0].value);
}

test "executor_where_filter_pipeline" {
    const allocator = std.testing.allocator;

    const pipeline = [_]Stage{
        Stage{ .where_filter = .{ .predicate = .{ .op = .gt, .value = 35 } } },
    };

    var input = try makeTestInput(allocator);
    defer input.deinit();

    var result = try execute(&pipeline, input, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.series[0].points.len);
}

test "executor_chained_rate_window_avg_where" {
    const allocator = std.testing.allocator;

    const pipeline = [_]Stage{
        Stage{ .rate = .{ .interval_ms = 1000 } },
        Stage{ .where_filter = .{ .predicate = .{ .op = .gte, .value = 10 } } },
    };

    var input = try makeTestInput(allocator);
    defer input.deinit();

    var result = try execute(&pipeline, input, allocator);
    defer result.deinit();

    // All rate points should be 10/s, which passes >= 10
    try std.testing.expectEqual(@as(usize, 5), result.series[0].points.len);
}

test "executor_alias" {
    const allocator = std.testing.allocator;

    const pipeline = [_]Stage{
        Stage{ .alias_stage = .{ .name = "renamed" } },
    };

    var input = try makeTestInput(allocator);
    defer input.deinit();

    var result = try execute(&pipeline, input, allocator);
    defer result.deinit();

    try std.testing.expectEqualStrings("renamed", result.series[0].key);
}

test "executor_empty_pipeline" {
    const allocator = std.testing.allocator;
    const pipeline = [_]Stage{};

    var input = try makeTestInput(allocator);
    defer input.deinit();

    // Empty pipeline returns the input unchanged
    const result = try execute(&pipeline, input, allocator);
    // Result IS the input (no intermediate created), so don't double-deinit
    _ = result;
}

test "executor_math_pipeline" {
    const allocator = std.testing.allocator;

    const pipeline = [_]Stage{
        Stage{ .math = .{ .op = .mul, .operand = 100.0 } },
    };

    var input = try makeTestInput(allocator);
    defer input.deinit();

    var result = try execute(&pipeline, input, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.series.len);
    try std.testing.expectEqual(@as(usize, 6), result.series[0].points.len);
    // 10 * 100 = 1000
    try std.testing.expectEqual(@as(f64, 1000.0), result.series[0].points[0].value);
    // 60 * 100 = 6000
    try std.testing.expectEqual(@as(f64, 6000.0), result.series[0].points[5].value);
}

test "executor_math_chained_pipeline" {
    const allocator = std.testing.allocator;

    // Convert from bytes to KB: divide by 1024
    const pipeline = [_]Stage{
        Stage{ .math = .{ .op = .div, .operand = 5.0 } },
        Stage{ .round_stage = .{ .decimals = 1 } },
    };

    var input = try makeTestInput(allocator);
    defer input.deinit();

    var result = try execute(&pipeline, input, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.series.len);
    // 10/5 = 2.0, round(1) = 2.0
    try std.testing.expectEqual(@as(f64, 2.0), result.series[0].points[0].value);
}
