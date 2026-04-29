/// FloQL Pipeline Stages — Transform implementations for SeriesSet.
///
/// Each stage function takes a SeriesSet and produces a new SeriesSet.
/// Stages are composable: the output of one stage is the input to the next.
const std = @import("std");
const Allocator = std.mem.Allocator;
const ast = @import("ast.zig");
const ss_mod = @import("series_set.zig");

const SeriesSet = ss_mod.SeriesSet;
const Series = ss_mod.Series;
const DataPoint = ss_mod.DataPoint;

pub const StageError = error{
    OutOfMemory,
    InvalidInput,
    NoData,
};

// ============================================================================
// Window Stage — bucket points by time interval
// ============================================================================

/// Group points into time windows. Each window gets one output point
/// with value = NaN (to be filled by a subsequent aggregation stage).
/// If no aggregation follows, the last point in each bucket is kept.
pub fn applyWindow(input: SeriesSet, interval_ms: i64, allocator: Allocator) StageError!SeriesSet {
    // Window is a declarative stage — it establishes the bucketing interval
    // for subsequent aggregate stages. The actual bucketing is performed by
    // applyAggregate using the current_window_ms context tracked by the executor.
    // Here we simply pass through all points unchanged.
    _ = interval_ms;

    var output_series = allocator.alloc(Series, input.series.len) catch return error.OutOfMemory;

    for (input.series, 0..) |s, i| {
        output_series[i] = .{
            .key = s.key,
            .field = s.field,
            .points = (allocator.dupe(DataPoint, s.points) catch return error.OutOfMemory),
            .tags = s.tags,
        };
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

const BucketAcc = struct {
    sum: f64 = 0,
    count: u64 = 0,
    min: f64 = std.math.floatMax(f64),
    max: f64 = -std.math.floatMax(f64),
    last: f64 = 0,
};

// ============================================================================
// Aggregate Stage — reduce windowed points
// ============================================================================

/// Apply aggregation to a windowed (or raw) SeriesSet.
/// Recomputes bucket values using the specified function.
pub fn applyAggregate(input: SeriesSet, func: ast.AggFunction, interval_ms: i64, allocator: Allocator) StageError!SeriesSet {
    if (interval_ms <= 0) return error.InvalidInput;

    var output_series = allocator.alloc(Series, input.series.len) catch return error.OutOfMemory;

    for (input.series, 0..) |s, i| {
        if (s.points.len == 0) {
            output_series[i] = .{ .key = s.key, .field = s.field, .points = &.{}, .tags = s.tags };
            continue;
        }

        const min_ts = s.points[0].timestamp_ms;
        const max_ts = s.points[s.points.len - 1].timestamp_ms;
        const bucket_count: usize = @intCast(@divTrunc(max_ts - min_ts, interval_ms) + 1);

        var buckets = allocator.alloc(BucketAcc, bucket_count) catch return error.OutOfMemory;
        defer allocator.free(buckets);
        for (buckets) |*b| b.* = BucketAcc{};

        for (s.points) |p| {
            const idx: usize = @intCast(@divTrunc(p.timestamp_ms - min_ts, interval_ms));
            if (idx < bucket_count) {
                buckets[idx].sum += p.value;
                buckets[idx].count += 1;
                buckets[idx].min = @min(buckets[idx].min, p.value);
                buckets[idx].max = @max(buckets[idx].max, p.value);
                buckets[idx].last = p.value;
            }
        }

        var points: std.ArrayList(DataPoint) = .empty;
        for (buckets, 0..) |b, bi| {
            if (b.count > 0) {
                const val: f64 = switch (func) {
                    .avg => b.sum / @as(f64, @floatFromInt(b.count)),
                    .sum => b.sum,
                    .count => @floatFromInt(b.count),
                    .min => b.min,
                    .max => b.max,
                };
                points.append(allocator, .{
                    .timestamp_ms = min_ts + @as(i64, @intCast(bi)) * interval_ms,
                    .value = val,
                }) catch return error.OutOfMemory;
            }
        }

        output_series[i] = .{
            .key = s.key,
            .field = s.field,
            .points = (allocator.dupe(DataPoint, points.items) catch return error.OutOfMemory),
            .tags = s.tags,
        };
        points.deinit(allocator);
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

// ============================================================================
// Rate Stage — per-second rate of change (for counters)
// ============================================================================

/// Compute per-second rate of change. Handles counter resets.
pub fn applyRate(input: SeriesSet, interval_ms: i64, allocator: Allocator) StageError!SeriesSet {
    _ = interval_ms; // Rate is always per-second regardless of interval

    var output_series = allocator.alloc(Series, input.series.len) catch return error.OutOfMemory;

    for (input.series, 0..) |s, i| {
        if (s.points.len < 2) {
            output_series[i] = .{ .key = s.key, .field = s.field, .points = &.{}, .tags = s.tags };
            continue;
        }

        var points: std.ArrayList(DataPoint) = .empty;
        var pi: usize = 1;
        while (pi < s.points.len) : (pi += 1) {
            const prev = s.points[pi - 1];
            const curr = s.points[pi];
            const dt_sec = @as(f64, @floatFromInt(curr.timestamp_ms - prev.timestamp_ms)) / 1000.0;

            if (dt_sec <= 0) continue;

            var delta = curr.value - prev.value;
            if (delta < 0) delta = curr.value; // Counter reset: treat as starting from 0

            points.append(allocator, .{
                .timestamp_ms = curr.timestamp_ms,
                .value = delta / dt_sec,
            }) catch return error.OutOfMemory;
        }

        output_series[i] = .{
            .key = s.key,
            .field = s.field,
            .points = (allocator.dupe(DataPoint, points.items) catch return error.OutOfMemory),
            .tags = s.tags,
        };
        points.deinit(allocator);
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

// ============================================================================
// Delta Stage — difference between consecutive points
// ============================================================================

pub fn applyDelta(input: SeriesSet, allocator: Allocator) StageError!SeriesSet {
    var output_series = allocator.alloc(Series, input.series.len) catch return error.OutOfMemory;

    for (input.series, 0..) |s, i| {
        if (s.points.len < 2) {
            output_series[i] = .{ .key = s.key, .field = s.field, .points = &.{}, .tags = s.tags };
            continue;
        }

        var points: std.ArrayList(DataPoint) = .empty;
        var pi: usize = 1;
        while (pi < s.points.len) : (pi += 1) {
            points.append(allocator, .{
                .timestamp_ms = s.points[pi].timestamp_ms,
                .value = s.points[pi].value - s.points[pi - 1].value,
            }) catch return error.OutOfMemory;
        }

        output_series[i] = .{
            .key = s.key,
            .field = s.field,
            .points = (allocator.dupe(DataPoint, points.items) catch return error.OutOfMemory),
            .tags = s.tags,
        };
        points.deinit(allocator);
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

// ============================================================================
// Where Stage — filter points by value predicate
// ============================================================================

pub fn applyWhere(input: SeriesSet, predicate: ast.Predicate, allocator: Allocator) StageError!SeriesSet {
    var output_series = allocator.alloc(Series, input.series.len) catch return error.OutOfMemory;

    for (input.series, 0..) |s, i| {
        var points: std.ArrayList(DataPoint) = .empty;

        for (s.points) |p| {
            const pass = switch (predicate.op) {
                .gt => p.value > predicate.value,
                .gte => p.value >= predicate.value,
                .lt => p.value < predicate.value,
                .lte => p.value <= predicate.value,
                .eq => p.value == predicate.value,
                .neq => p.value != predicate.value,
            };
            if (pass) {
                points.append(allocator, p) catch return error.OutOfMemory;
            }
        }

        output_series[i] = .{
            .key = s.key,
            .field = s.field,
            .points = (allocator.dupe(DataPoint, points.items) catch return error.OutOfMemory),
            .tags = s.tags,
        };
        points.deinit(allocator);
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

// ============================================================================
// Group By Stage — merge series by tag value
// ============================================================================

/// Merge series that share the same value of the specified tag into one
/// combined series per distinct tag value.
///
/// Example: Given series `cpu,host=web-01,dc=a` and `cpu,host=web-01,dc=b`,
/// `group_by(host)` produces a single `cpu,host=web-01` with interleaved
/// points sorted by timestamp.
///
/// Series whose tags do not contain the specified tag are collected into an
/// unnamed group keyed `{measurement},{tag}=_untagged_`.
pub fn applyGroupBy(input: SeriesSet, tag: []const u8, allocator: Allocator) StageError!SeriesSet {
    if (input.series.len == 0) {
        return SeriesSet.fromOwned(allocator, allocator.alloc(Series, 0) catch return error.OutOfMemory);
    }

    // --- Phase 1: bucket series indices by tag value ---
    // Map: tag_value -> list of indices into input.series
    var groups: std.array_hash_map.String(std.ArrayListUnmanaged(usize)) = .empty;
    defer {
        var it = groups.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(allocator);
        }
        groups.deinit(allocator);
    }

    for (input.series, 0..) |s, idx| {
        // Find the value of the requested tag on this series
        var tag_value: []const u8 = "_untagged_";
        for (s.tags) |t| {
            if (std.mem.eql(u8, t.key, tag)) {
                tag_value = t.value;
                break;
            }
        }

        const gop = groups.getOrPut(allocator, tag_value) catch return error.OutOfMemory;
        if (!gop.found_existing) {
            gop.value_ptr.* = .empty;
        }
        gop.value_ptr.append(allocator, idx) catch return error.OutOfMemory;
    }

    // --- Phase 2: build one output series per group ---
    var output_series = allocator.alloc(Series, groups.count()) catch return error.OutOfMemory;

    var gi: usize = 0;
    var group_it = groups.iterator();
    while (group_it.next()) |entry| {
        const tag_value = entry.key_ptr.*;
        const member_indices = entry.value_ptr.items;

        // Derive measurement from the first member's key (strip tags suffix)
        const first = input.series[member_indices[0]];
        const measurement = extractMeasurement(first.key);

        // Build grouped key: "{measurement},{tag}={value}"
        const grouped_key = std.fmt.allocPrint(allocator, "{s},{s}={s}", .{
            measurement, tag, tag_value,
        }) catch return error.OutOfMemory;

        // Merge points from all group members
        var merged_points: std.ArrayListUnmanaged(DataPoint) = .empty;
        for (member_indices) |si| {
            for (input.series[si].points) |p| {
                merged_points.append(allocator, p) catch return error.OutOfMemory;
            }
        }

        // Sort merged points by timestamp for correct downstream processing
        std.mem.sort(DataPoint, merged_points.items, {}, struct {
            fn lessThan(_: void, a: DataPoint, b: DataPoint) bool {
                return a.timestamp_ms < b.timestamp_ms;
            }
        }.lessThan);

        // Build a single-element tags array with just the grouped tag
        const out_tags = allocator.alloc(Series.Tag, 1) catch return error.OutOfMemory;
        out_tags[0] = .{ .key = tag, .value = tag_value };

        output_series[gi] = .{
            .key = grouped_key,
            .key_owned = true,
            .field = first.field,
            .points = (allocator.dupe(DataPoint, merged_points.items) catch return error.OutOfMemory),
            .tags = out_tags,
        };
        merged_points.deinit(allocator);
        gi += 1;
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

/// Extract the measurement name from a canonical series key.
/// "cpu,host=web-01,dc=a" -> "cpu"
/// "cpu" -> "cpu"
fn extractMeasurement(key: []const u8) []const u8 {
    return if (std.mem.indexOfScalar(u8, key, ',')) |comma|
        key[0..comma]
    else
        key;
}

// ============================================================================
// TopK / BottomK — series ranking
// ============================================================================

pub fn applyTopK(input: SeriesSet, k: u32, allocator: Allocator) StageError!SeriesSet {
    return applyRanking(input, k, true, allocator);
}

pub fn applyBottomK(input: SeriesSet, k: u32, allocator: Allocator) StageError!SeriesSet {
    return applyRanking(input, k, false, allocator);
}

fn applyRanking(input: SeriesSet, k: u32, descending: bool, allocator: Allocator) StageError!SeriesSet {
    if (input.series.len <= k) {
        // Return all series (copy)
        const output_series = allocator.dupe(Series, input.series) catch return error.OutOfMemory;
        for (output_series, 0..) |*s, i| {
            s.points = allocator.dupe(DataPoint, input.series[i].points) catch return error.OutOfMemory;
        }
        return SeriesSet.fromOwned(allocator, output_series);
    }

    // Compute aggregate for each series (sum of all points as score)
    const SeriesScore = struct {
        index: usize,
        score: f64,
    };
    var scores = allocator.alloc(SeriesScore, input.series.len) catch return error.OutOfMemory;
    defer allocator.free(scores);

    for (input.series, 0..) |s, i| {
        var sum: f64 = 0;
        for (s.points) |p| sum += p.value;
        scores[i] = .{ .index = i, .score = sum };
    }

    // Sort by score
    const lessFn = struct {
        fn desc(ctx: void, a: SeriesScore, b: SeriesScore) bool {
            _ = ctx;
            return a.score > b.score;
        }
        fn asc(ctx: void, a: SeriesScore, b: SeriesScore) bool {
            _ = ctx;
            return a.score < b.score;
        }
    };

    if (descending) {
        std.mem.sort(SeriesScore, scores, {}, lessFn.desc);
    } else {
        std.mem.sort(SeriesScore, scores, {}, lessFn.asc);
    }

    // Take top k
    const take = @min(k, @as(u32, @intCast(scores.len)));
    var output_series = allocator.alloc(Series, take) catch return error.OutOfMemory;
    for (scores[0..take], 0..) |sc, i| {
        const src = input.series[sc.index];
        output_series[i] = .{
            .key = src.key,
            .field = src.field,
            .points = (allocator.dupe(DataPoint, src.points) catch return error.OutOfMemory),
            .tags = src.tags,
        };
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

// ============================================================================
// Math Stages: abs, ceil, floor, round
// ============================================================================

pub fn applyAbs(input: SeriesSet, allocator: Allocator) StageError!SeriesSet {
    return applyPointwiseTransform(input, allocator, struct {
        fn transform(v: f64) f64 {
            return @abs(v);
        }
    }.transform);
}

pub fn applyCeil(input: SeriesSet, allocator: Allocator) StageError!SeriesSet {
    return applyPointwiseTransform(input, allocator, struct {
        fn transform(v: f64) f64 {
            return @ceil(v);
        }
    }.transform);
}

pub fn applyFloor(input: SeriesSet, allocator: Allocator) StageError!SeriesSet {
    return applyPointwiseTransform(input, allocator, struct {
        fn transform(v: f64) f64 {
            return @floor(v);
        }
    }.transform);
}

pub fn applyRound(input: SeriesSet, decimals: u32, allocator: Allocator) StageError!SeriesSet {
    if (decimals == 0) {
        return applyPointwiseTransform(input, allocator, struct {
            fn transform(v: f64) f64 {
                return @round(v);
            }
        }.transform);
    }

    // For non-zero decimals: round(v * 10^d) / 10^d
    const factor = std.math.pow(f64, 10.0, @floatFromInt(decimals));

    var output_series = allocator.alloc(Series, input.series.len) catch return error.OutOfMemory;

    for (input.series, 0..) |s, i| {
        var points = allocator.alloc(DataPoint, s.points.len) catch return error.OutOfMemory;
        for (s.points, 0..) |p, pi| {
            points[pi] = .{
                .timestamp_ms = p.timestamp_ms,
                .value = @round(p.value * factor) / factor,
            };
        }
        output_series[i] = .{ .key = s.key, .field = s.field, .points = points, .tags = s.tags };
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

fn applyPointwiseTransform(input: SeriesSet, allocator: Allocator, comptime transform: fn (f64) f64) StageError!SeriesSet {
    var output_series = allocator.alloc(Series, input.series.len) catch return error.OutOfMemory;

    for (input.series, 0..) |s, i| {
        var points = allocator.alloc(DataPoint, s.points.len) catch return error.OutOfMemory;
        for (s.points, 0..) |p, pi| {
            points[pi] = .{ .timestamp_ms = p.timestamp_ms, .value = transform(p.value) };
        }
        output_series[i] = .{ .key = s.key, .field = s.field, .points = points, .tags = s.tags };
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

// ============================================================================
// Math Stage — arithmetic operations on values
// ============================================================================

/// Apply a simple arithmetic operation to each point: value OP operand.
/// Supports: +, -, *, /, % (modulo).
///
/// Example: math(* 100) multiplies every value by 100.
pub fn applyMath(input: SeriesSet, op: ast.MathOp, operand: f64, allocator: Allocator) StageError!SeriesSet {
    var output_series = allocator.alloc(Series, input.series.len) catch return error.OutOfMemory;

    for (input.series, 0..) |s, i| {
        var points = allocator.alloc(DataPoint, s.points.len) catch return error.OutOfMemory;
        for (s.points, 0..) |p, pi| {
            const v: f64 = switch (op) {
                .add => p.value + operand,
                .sub => p.value - operand,
                .mul => p.value * operand,
                .div => if (operand != 0) p.value / operand else p.value,
                .mod => if (operand != 0) @mod(p.value, operand) else p.value,
            };
            points[pi] = .{ .timestamp_ms = p.timestamp_ms, .value = v };
        }
        output_series[i] = .{ .key = s.key, .field = s.field, .points = points, .tags = s.tags };
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

// ============================================================================
// Percentile Stage
// ============================================================================

/// Compute the P-th percentile per window bucket.
/// Requires windowed input (points already bucketed).
pub fn applyPercentile(input: SeriesSet, p: f64, interval_ms: i64, allocator: Allocator) StageError!SeriesSet {
    if (interval_ms <= 0) return error.InvalidInput;

    var output_series = allocator.alloc(Series, input.series.len) catch return error.OutOfMemory;

    for (input.series, 0..) |s, i| {
        if (s.points.len == 0) {
            output_series[i] = .{ .key = s.key, .field = s.field, .points = &.{}, .tags = s.tags };
            continue;
        }

        const min_ts = s.points[0].timestamp_ms;
        const max_ts = s.points[s.points.len - 1].timestamp_ms;
        const bucket_count: usize = @intCast(@divTrunc(max_ts - min_ts, interval_ms) + 1);

        // Collect values per bucket
        var bucket_values: std.ArrayList(std.ArrayList(f64)) = .empty;
        defer {
            for (bucket_values.items) |*bv| bv.deinit(allocator);
            bucket_values.deinit(allocator);
        }
        var bi: usize = 0;
        while (bi < bucket_count) : (bi += 1) {
            var list: std.ArrayList(f64) = .empty;
            _ = &list;
            bucket_values.append(allocator, list) catch return error.OutOfMemory;
        }

        for (s.points) |pt| {
            const idx: usize = @intCast(@divTrunc(pt.timestamp_ms - min_ts, interval_ms));
            if (idx < bucket_count) {
                bucket_values.items[idx].append(allocator, pt.value) catch return error.OutOfMemory;
            }
        }

        var points: std.ArrayList(DataPoint) = .empty;
        for (bucket_values.items, 0..) |*bv, bvi| {
            if (bv.items.len == 0) continue;

            // Sort values
            std.mem.sort(f64, bv.items, {}, struct {
                fn lessThan(ctx: void, a: f64, b: f64) bool {
                    _ = ctx;
                    return a < b;
                }
            }.lessThan);

            // Compute percentile index
            const rank = (p / 100.0) * @as(f64, @floatFromInt(bv.items.len - 1));
            const idx_f = @floor(rank);
            const idx: usize = @intFromFloat(idx_f);
            const frac = rank - idx_f;

            var val: f64 = undefined;
            if (idx + 1 < bv.items.len) {
                val = bv.items[idx] * (1.0 - frac) + bv.items[idx + 1] * frac;
            } else {
                val = bv.items[idx];
            }

            points.append(allocator, .{
                .timestamp_ms = min_ts + @as(i64, @intCast(bvi)) * interval_ms,
                .value = val,
            }) catch return error.OutOfMemory;
        }

        output_series[i] = .{
            .key = s.key,
            .field = s.field,
            .points = (allocator.dupe(DataPoint, points.items) catch return error.OutOfMemory),
            .tags = s.tags,
        };
        points.deinit(allocator);
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

// ============================================================================
// First / Last — return first or last point per series
// ============================================================================

pub fn applyFirst(input: SeriesSet, allocator: Allocator) StageError!SeriesSet {
    var output_series = allocator.alloc(Series, input.series.len) catch return error.OutOfMemory;

    for (input.series, 0..) |s, i| {
        if (s.points.len == 0) {
            output_series[i] = .{ .key = s.key, .field = s.field, .points = &.{}, .tags = s.tags };
            continue;
        }
        var points = allocator.alloc(DataPoint, 1) catch return error.OutOfMemory;
        points[0] = s.points[0];
        output_series[i] = .{ .key = s.key, .field = s.field, .points = points, .tags = s.tags };
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

pub fn applyLast(input: SeriesSet, allocator: Allocator) StageError!SeriesSet {
    var output_series = allocator.alloc(Series, input.series.len) catch return error.OutOfMemory;

    for (input.series, 0..) |s, i| {
        if (s.points.len == 0) {
            output_series[i] = .{ .key = s.key, .field = s.field, .points = &.{}, .tags = s.tags };
            continue;
        }
        var points = allocator.alloc(DataPoint, 1) catch return error.OutOfMemory;
        points[0] = s.points[s.points.len - 1];
        output_series[i] = .{ .key = s.key, .field = s.field, .points = points, .tags = s.tags };
    }

    return SeriesSet.fromOwned(allocator, output_series);
}

// ============================================================================
// Tests
// ============================================================================

fn makeTestSeries(allocator: Allocator) !SeriesSet {
    var points = try allocator.alloc(DataPoint, 6);
    points[0] = .{ .timestamp_ms = 0, .value = 10.0 };
    points[1] = .{ .timestamp_ms = 1000, .value = 20.0 };
    points[2] = .{ .timestamp_ms = 2000, .value = 30.0 };
    points[3] = .{ .timestamp_ms = 3000, .value = 40.0 };
    points[4] = .{ .timestamp_ms = 4000, .value = 50.0 };
    points[5] = .{ .timestamp_ms = 5000, .value = 60.0 };

    var series = try allocator.alloc(Series, 1);
    series[0] = .{ .key = "test", .field = "value", .points = points };

    return SeriesSet.fromOwned(allocator, series);
}

test "stages_window" {
    const allocator = std.testing.allocator;
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var result = try applyWindow(input, 2000, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.series.len);
    // Window is a passthrough — all 6 points preserved
    try std.testing.expectEqual(@as(usize, 6), result.series[0].points.len);
}

test "stages_aggregate_avg" {
    const allocator = std.testing.allocator;
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var result = try applyAggregate(input, .avg, 3000, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.series.len);
    // 6 points over 6s with 3s window = 2 buckets
    try std.testing.expectEqual(@as(usize, 2), result.series[0].points.len);
    // First bucket: avg(10, 20, 30) = 20
    try std.testing.expectEqual(@as(f64, 20.0), result.series[0].points[0].value);
    // Second bucket: avg(40, 50, 60) = 50
    try std.testing.expectEqual(@as(f64, 50.0), result.series[0].points[1].value);
}

test "stages_aggregate_sum" {
    const allocator = std.testing.allocator;
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var result = try applyAggregate(input, .sum, 3000, allocator);
    defer result.deinit();

    // First bucket: sum(10, 20, 30) = 60
    try std.testing.expectEqual(@as(f64, 60.0), result.series[0].points[0].value);
    // Second bucket: sum(40, 50, 60) = 150
    try std.testing.expectEqual(@as(f64, 150.0), result.series[0].points[1].value);
}

test "stages_rate" {
    const allocator = std.testing.allocator;
    // Counter: 10, 20, 30, 40, 50, 60 at 1s intervals
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var result = try applyRate(input, 1000, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.series.len);
    try std.testing.expectEqual(@as(usize, 5), result.series[0].points.len);
    // rate = (20-10)/1 = 10/s
    try std.testing.expectEqual(@as(f64, 10.0), result.series[0].points[0].value);
}

test "stages_delta" {
    const allocator = std.testing.allocator;
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var result = try applyDelta(input, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 5), result.series[0].points.len);
    // Each delta = 10
    for (result.series[0].points) |p| {
        try std.testing.expectEqual(@as(f64, 10.0), p.value);
    }
}

test "stages_where_gt" {
    const allocator = std.testing.allocator;
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var result = try applyWhere(input, .{ .op = .gt, .value = 35.0 }, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.series[0].points.len);
    try std.testing.expectEqual(@as(f64, 40.0), result.series[0].points[0].value);
}

test "stages_abs" {
    const allocator = std.testing.allocator;
    var points = try allocator.alloc(DataPoint, 3);
    points[0] = .{ .timestamp_ms = 0, .value = -5.0 };
    points[1] = .{ .timestamp_ms = 1000, .value = 3.0 };
    points[2] = .{ .timestamp_ms = 2000, .value = -7.0 };

    var series = try allocator.alloc(Series, 1);
    series[0] = .{ .key = "test", .field = "value", .points = points };

    var input = SeriesSet.fromOwned(allocator, series);
    defer input.deinit();

    var result = try applyAbs(input, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(f64, 5.0), result.series[0].points[0].value);
    try std.testing.expectEqual(@as(f64, 3.0), result.series[0].points[1].value);
    try std.testing.expectEqual(@as(f64, 7.0), result.series[0].points[2].value);
}

test "stages_round_zero_decimals" {
    const allocator = std.testing.allocator;
    var points = try allocator.alloc(DataPoint, 3);
    points[0] = .{ .timestamp_ms = 0, .value = 3.14 };
    points[1] = .{ .timestamp_ms = 1000, .value = 2.7 };
    points[2] = .{ .timestamp_ms = 2000, .value = -1.5 };

    var series = try allocator.alloc(Series, 1);
    series[0] = .{ .key = "test", .field = "value", .points = points };

    var input = SeriesSet.fromOwned(allocator, series);
    defer input.deinit();

    var result = try applyRound(input, 0, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(f64, 3.0), result.series[0].points[0].value);
    try std.testing.expectEqual(@as(f64, 3.0), result.series[0].points[1].value);
    try std.testing.expectEqual(@as(f64, -2.0), result.series[0].points[2].value);
}

test "stages_round_with_decimals" {
    const allocator = std.testing.allocator;
    var points = try allocator.alloc(DataPoint, 3);
    points[0] = .{ .timestamp_ms = 0, .value = 3.14159 };
    points[1] = .{ .timestamp_ms = 1000, .value = 2.71828 };
    points[2] = .{ .timestamp_ms = 2000, .value = -1.555 };

    var series = try allocator.alloc(Series, 1);
    series[0] = .{ .key = "test", .field = "value", .points = points };

    var input = SeriesSet.fromOwned(allocator, series);
    defer input.deinit();

    var result = try applyRound(input, 2, allocator);
    defer result.deinit();

    try std.testing.expectApproxEqAbs(@as(f64, 3.14), result.series[0].points[0].value, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.72), result.series[0].points[1].value, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, -1.56), result.series[0].points[2].value, 0.001);
}

test "stages_math_multiply" {
    const allocator = std.testing.allocator;
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var result = try applyMath(input, .mul, 100.0, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 6), result.series[0].points.len);
    try std.testing.expectEqual(@as(f64, 1000.0), result.series[0].points[0].value); // 10 * 100
    try std.testing.expectEqual(@as(f64, 2000.0), result.series[0].points[1].value); // 20 * 100
    try std.testing.expectEqual(@as(f64, 6000.0), result.series[0].points[5].value); // 60 * 100
}

test "stages_math_divide" {
    const allocator = std.testing.allocator;
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var result = try applyMath(input, .div, 5.0, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(f64, 2.0), result.series[0].points[0].value); // 10 / 5
    try std.testing.expectEqual(@as(f64, 4.0), result.series[0].points[1].value); // 20 / 5
}

test "stages_math_add" {
    const allocator = std.testing.allocator;
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var result = try applyMath(input, .add, 273.15, allocator);
    defer result.deinit();

    try std.testing.expectApproxEqAbs(@as(f64, 283.15), result.series[0].points[0].value, 0.001); // 10 + 273.15
}

test "stages_math_div_by_zero" {
    const allocator = std.testing.allocator;
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var result = try applyMath(input, .div, 0.0, allocator);
    defer result.deinit();

    // Division by zero returns original value (safe fallback)
    try std.testing.expectEqual(@as(f64, 10.0), result.series[0].points[0].value);
}

test "stages_math_modulo" {
    const allocator = std.testing.allocator;
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var result = try applyMath(input, .mod, 15.0, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(f64, 10.0), result.series[0].points[0].value); // 10 % 15 = 10
    try std.testing.expectEqual(@as(f64, 5.0), result.series[0].points[1].value); // 20 % 15 = 5
}

test "stages_first_last" {
    const allocator = std.testing.allocator;
    var input = try makeTestSeries(allocator);
    defer input.deinit();

    var first_result = try applyFirst(input, allocator);
    defer first_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), first_result.series[0].points.len);
    try std.testing.expectEqual(@as(f64, 10.0), first_result.series[0].points[0].value);

    var last_result = try applyLast(input, allocator);
    defer last_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), last_result.series[0].points.len);
    try std.testing.expectEqual(@as(f64, 60.0), last_result.series[0].points[0].value);
}

test "stages_topk" {
    const allocator = std.testing.allocator;

    // Create 3 series with different totals
    var p1 = try allocator.alloc(DataPoint, 1);
    p1[0] = .{ .timestamp_ms = 0, .value = 100 };
    var p2 = try allocator.alloc(DataPoint, 1);
    p2[0] = .{ .timestamp_ms = 0, .value = 300 };
    var p3 = try allocator.alloc(DataPoint, 1);
    p3[0] = .{ .timestamp_ms = 0, .value = 200 };

    var series = try allocator.alloc(Series, 3);
    series[0] = .{ .key = "a", .field = "value", .points = p1 };
    series[1] = .{ .key = "b", .field = "value", .points = p2 };
    series[2] = .{ .key = "c", .field = "value", .points = p3 };

    var input = SeriesSet.fromOwned(allocator, series);
    defer input.deinit();

    var result = try applyTopK(input, 2, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.series.len);
    // Top 2 by score: b (300), c (200)
    try std.testing.expectEqual(@as(f64, 300.0), result.series[0].points[0].value);
    try std.testing.expectEqual(@as(f64, 200.0), result.series[1].points[0].value);
}

// ============================================================================
// Group By Tests
// ============================================================================

test "stages_group_by_merges_by_tag" {
    const allocator = std.testing.allocator;

    // Two series sharing host=web-01 but different dc values
    var p1 = try allocator.alloc(DataPoint, 2);
    p1[0] = .{ .timestamp_ms = 1000, .value = 10.0 };
    p1[1] = .{ .timestamp_ms = 3000, .value = 30.0 };

    var p2 = try allocator.alloc(DataPoint, 2);
    p2[0] = .{ .timestamp_ms = 2000, .value = 20.0 };
    p2[1] = .{ .timestamp_ms = 4000, .value = 40.0 };

    // Third series with host=web-02
    var p3 = try allocator.alloc(DataPoint, 1);
    p3[0] = .{ .timestamp_ms = 1500, .value = 99.0 };

    const tags_a = try allocator.alloc(Series.Tag, 2);
    tags_a[0] = .{ .key = "host", .value = "web-01" };
    tags_a[1] = .{ .key = "dc", .value = "us-east" };

    const tags_b = try allocator.alloc(Series.Tag, 2);
    tags_b[0] = .{ .key = "host", .value = "web-01" };
    tags_b[1] = .{ .key = "dc", .value = "eu-west" };

    const tags_c = try allocator.alloc(Series.Tag, 1);
    tags_c[0] = .{ .key = "host", .value = "web-02" };

    var series = try allocator.alloc(Series, 3);
    series[0] = .{ .key = "cpu,host=web-01,dc=us-east", .field = "value", .points = p1, .tags = tags_a };
    series[1] = .{ .key = "cpu,host=web-01,dc=eu-west", .field = "value", .points = p2, .tags = tags_b };
    series[2] = .{ .key = "cpu,host=web-02", .field = "value", .points = p3, .tags = tags_c };

    var input = SeriesSet.fromOwned(allocator, series);
    defer input.deinit();

    var result = try applyGroupBy(input, "host", allocator);
    defer result.deinit();

    // Should produce 2 groups: host=web-01 (merged), host=web-02
    try std.testing.expectEqual(@as(usize, 2), result.series.len);

    // Find the web-01 group and web-02 group by key
    var web01: ?Series = null;
    var web02: ?Series = null;
    for (result.series) |s| {
        if (std.mem.indexOf(u8, s.key, "web-01") != null) web01 = s;
        if (std.mem.indexOf(u8, s.key, "web-02") != null) web02 = s;
    }

    // web-01 group: 4 points merged and sorted by timestamp
    try std.testing.expect(web01 != null);
    try std.testing.expectEqual(@as(usize, 4), web01.?.points.len);
    try std.testing.expectEqual(@as(i64, 1000), web01.?.points[0].timestamp_ms);
    try std.testing.expectEqual(@as(i64, 2000), web01.?.points[1].timestamp_ms);
    try std.testing.expectEqual(@as(i64, 3000), web01.?.points[2].timestamp_ms);
    try std.testing.expectEqual(@as(i64, 4000), web01.?.points[3].timestamp_ms);

    // web-02 group: 1 point
    try std.testing.expect(web02 != null);
    try std.testing.expectEqual(@as(usize, 1), web02.?.points.len);
    try std.testing.expectEqual(@as(f64, 99.0), web02.?.points[0].value);

    // Grouped key format
    try std.testing.expectEqualStrings("cpu,host=web-01", web01.?.key);
    try std.testing.expectEqualStrings("cpu,host=web-02", web02.?.key);

    // Tags on output should contain only the grouped tag
    try std.testing.expectEqual(@as(usize, 1), web01.?.tags.len);
    try std.testing.expectEqualStrings("host", web01.?.tags[0].key);
    try std.testing.expectEqualStrings("web-01", web01.?.tags[0].value);
}

test "stages_group_by_untagged_series" {
    const allocator = std.testing.allocator;

    // One series has the tag, the other doesn't
    var p1 = try allocator.alloc(DataPoint, 1);
    p1[0] = .{ .timestamp_ms = 1000, .value = 5.0 };

    var p2 = try allocator.alloc(DataPoint, 1);
    p2[0] = .{ .timestamp_ms = 2000, .value = 7.0 };

    const tags_a = try allocator.alloc(Series.Tag, 1);
    tags_a[0] = .{ .key = "host", .value = "web-01" };

    var series = try allocator.alloc(Series, 2);
    series[0] = .{ .key = "cpu,host=web-01", .field = "value", .points = p1, .tags = tags_a };
    series[1] = .{ .key = "cpu", .field = "value", .points = p2, .tags = &.{} };

    var input = SeriesSet.fromOwned(allocator, series);
    defer input.deinit();

    var result = try applyGroupBy(input, "host", allocator);
    defer result.deinit();

    // 2 groups: host=web-01 and host=_untagged_
    try std.testing.expectEqual(@as(usize, 2), result.series.len);
}

test "stages_group_by_empty_input" {
    const allocator = std.testing.allocator;

    var input = SeriesSet.fromOwned(allocator, try allocator.alloc(Series, 0));
    defer input.deinit();

    var result = try applyGroupBy(input, "host", allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.series.len);
}

test "stages_extractMeasurement" {
    try std.testing.expectEqualStrings("cpu", extractMeasurement("cpu,host=web-01,dc=a"));
    try std.testing.expectEqualStrings("cpu", extractMeasurement("cpu"));
    try std.testing.expectEqualStrings("http_requests", extractMeasurement("http_requests,method=GET"));
}
