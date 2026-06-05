//! Dashboard API — Time-Series Endpoints
//!
//! - GET  /timeseries                       — All measurements with stats
//! - GET  /timeseries/:measurement          — Measurement detail (fields, series count)
//! - GET  /timeseries/:measurement/data     — Query data points
//! - POST /timeseries/floql                 — Execute FloQL query

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const Method = @import("../../../util/http/mod.zig").Method;
const Shard = @import("../../shard.zig").Shard;
const TSProjection = @import("../../../projection/ts.zig").TSProjection;
const router = @import("../../router.zig");

// ── Helpers ──

fn getTSProjection(ctx: *DashboardContext, idx: usize) ?*TSProjection {
    const ptrs = ctx.shard_ptrs orelse return null;
    if (idx >= ptrs.len) return null;
    const shard: *Shard = @ptrCast(@alignCast(ptrs[idx]));
    return &shard.defaultPartition().ts;
}

// TS series keys are `[namespace_hash:4 LE][measurement]\x00[field]` (see
// projection/ts.zig). The 4-byte prefix may contain 0x00, so always parse the
// measurement from offset 4.
fn keyNsHash(key: []const u8) u32 {
    if (key.len < 4) return 0;
    return std.mem.readInt(u32, key[0..4], .little);
}
fn keyMeasurement(key: []const u8) []const u8 {
    if (key.len < 4) return key;
    const rest = key[4..];
    const sep = std.mem.indexOfScalar(u8, rest, 0) orelse rest.len;
    return rest[0..sep];
}

fn shardCount(ctx: *DashboardContext) usize {
    return if (ctx.shard_ptrs) |p| p.len else 0;
}

/// GET /timeseries — List all measurements with field counts and stats
pub fn getMeasurements(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const ns_hash = router.namespaceHash(ns);

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // Collect unique measurement names from all shard TS projections (this namespace)
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getTSProjection(ctx, i)) |ts| {
            var buf: [256][]const u8 = undefined;
            const count = ts.scanMeasurementNames(ns_hash, &buf);
            for (buf[0..count]) |meas_name| {
                const gop = try seen.getOrPut(meas_name);
                if (gop.found_existing) continue;

                // Tally per-measurement series (= write buffers, one per
                // measurement+field) and points across all shards, scoped to
                // this namespace (key = [ns_hash:4][measurement]\x00[field]).
                var series_count: usize = 0;
                var points: u64 = 0;
                for (0..n) |j| {
                    if (getTSProjection(ctx, j)) |tsj| {
                        var it = tsj.buffers.iterator();
                        while (it.next()) |kv| {
                            const key = kv.key_ptr.*;
                            if (keyNsHash(key) == ns_hash and std.mem.eql(u8, keyMeasurement(key), meas_name)) {
                                series_count += 1;
                                points += kv.value_ptr.len();
                            }
                        }
                    }
                }

                try arr.next();
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("name", meas_name);
                try obj.intField("series_count", series_count);
                try obj.intField("field_count", series_count);
                try obj.intField("points", points);
                try obj.end();
            }
        }
    }

    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// GET /timeseries/:measurement — Measurement detail
pub fn getMeasurementDetail(allocator: Allocator, measurement: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const ns_hash = router.namespaceHash(ns);

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", measurement);
    try obj.stringField("namespace", ns);

    // Collect field names and series count for this namespace+measurement.
    var field_set = std.StringHashMap(void).init(allocator);
    defer field_set.deinit();
    var total_series: usize = 0;
    var total_points: u64 = 0;

    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getTSProjection(ctx, i)) |ts| {
            var it = ts.buffers.iterator();
            while (it.next()) |kv| {
                const key = kv.key_ptr.*;
                if (keyNsHash(key) != ns_hash) continue;
                const meas = keyMeasurement(key);
                if (std.mem.eql(u8, meas, measurement)) {
                    total_series += 1;
                    total_points += kv.value_ptr.len();
                    // field = bytes after `[ns:4][measurement]\x00`
                    const field_off = 4 + meas.len + 1;
                    if (field_off <= key.len) {
                        try field_set.put(key[field_off..], {});
                    }
                }
            }
        }
    }

    try obj.intField("field_count", field_set.count());
    {
        var fields_arr = try obj.arrayField("fields");
        try fields_arr.begin();
        var fit = field_set.iterator();
        while (fit.next()) |fe| {
            try fields_arr.next();
            var fobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try fobj.begin();
            try fobj.stringField("name", fe.key_ptr.*);
            try fobj.stringField("type", "float");
            try fobj.end();
        }
        try fields_arr.end();
    }

    try obj.intField("series_count", total_series);
    {
        var series_arr = try obj.arrayField("series");
        try series_arr.begin();
        try series_arr.end();
    }
    try obj.nullField("retention");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /timeseries/:measurement/data — Query data points
pub fn getSeriesData(allocator: Allocator, measurement: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const field = h.parseQueryParam([]const u8, query_string, "field") orelse "value";
    const ns_hash = router.namespaceHash(h.parseQueryParam([]const u8, query_string, "namespace") orelse "default");
    const from_str = h.parseQueryParam([]const u8, query_string, "from");
    const to_str = h.parseQueryParam([]const u8, query_string, "to");
    const window = h.parseQueryParam(u64, query_string, "window") orelse 0;
    const aggregation = h.parseQueryParam([]const u8, query_string, "aggregation") orelse "none";

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("measurement", measurement);
    try obj.stringField("field", field);
    try obj.stringField("aggregation", aggregation);
    try obj.intField("window_ms", @as(i64, @intCast(window)));

    const from_val: i64 = if (from_str) |s| std.fmt.parseInt(i64, s, 10) catch 0 else 0;
    const to_val: i64 = if (to_str) |s| std.fmt.parseInt(i64, s, 10) catch 0 else 0;
    try obj.intField("from_ms", from_val);
    try obj.intField("to_ms", to_val);

    // Query data from shard TS projections
    {
        var series_arr = try obj.arrayField("series");
        try series_arr.begin();

        // Convert ms → ns for projection query
        const from_ns: u64 = if (from_val > 0) @intCast(from_val * std.time.ns_per_ms) else 0;
        const to_ns: u64 = if (to_val > 0) @intCast(to_val * std.time.ns_per_ms) else std.math.maxInt(u64);

        const StoredPoint = @import("../../../projection/ts.zig").StoredPoint;
        const n = shardCount(ctx);
        for (0..n) |i| {
            if (getTSProjection(ctx, i)) |ts| {
                var point_buf: [1024]StoredPoint = undefined;
                const result = ts.queryRange(ns_hash, measurement, field, from_ns, to_ns, &point_buf) catch continue;
                if (result.points_in_buffer > 0) {
                    for (point_buf[0..result.points_in_buffer]) |pt| {
                        try series_arr.next();
                        var pobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                        try pobj.begin();
                        try pobj.intField("timestamp", @as(i64, @intCast(pt.timestamp_ns / std.time.ns_per_ms)));

                        // Write float using fixed precision
                        try pobj.floatField("value", pt.field_value);
                        try pobj.end();
                    }
                }
            }
        }

        try series_arr.end();
    }

    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// POST /timeseries/floql — Execute FloQL query
pub fn executeFloql(allocator: Allocator, method: Method, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = method;

    // Query can come from POST body or GET ?q= param
    const query_text = if (body.len > 0) body else (h.parseQueryParam([]const u8, query_string, "q") orelse "");
    if (query_text.len == 0) return try h.jsonError(allocator, "Empty query");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    // FloQL execution requires parser + executor wiring — return query echo for now.
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("query", query_text);
    var series_arr = try obj.arrayField("series");
    try series_arr.begin();
    try series_arr.end();
    try obj.end();
    return try json_aw.toOwnedSlice();
}

// =============================================================================
// Tests
// =============================================================================

test "getMeasurements returns empty array" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getMeasurements(allocator, null, &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "getMeasurementDetail returns stub" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getMeasurementDetail(allocator, "cpu", null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"cpu\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"fields\":[]") != null);
}

test "executeFloql returns empty for query" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try executeFloql(allocator, .POST, null, "cpu{host=web-01}[1h]", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"series\":[]") != null);
}

test "executeFloql rejects empty query" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try executeFloql(allocator, .POST, null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}
