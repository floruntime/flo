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
const floql_parser = @import("../../../ts/floql/parser.zig");
const floql_executor = @import("../../../ts/floql/executor.zig");
const ss_mod = @import("../../../ts/floql/series_set.zig");
const floql_ast = @import("../../../ts/floql/ast.zig");
const ts_projection = @import("../../../projection/ts.zig");

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

/// Turn FloQL source tag filters into an exact tag-set hash. A hash can only
/// answer exact-set equality, so this applies only when every filter is `=`;
/// `!=`, `=~`, `!~` and partial tag sets return null (unfiltered) rather than
/// silently matching nothing. Mirrors `TSHandler.floqlTagFilter` — real
/// partial/regex tag matching needs the tag dictionary (#24 part 2b).
fn floqlTagFilter(filters: []const floql_ast.TagFilter, scratch: []u8) ?u64 {
    if (filters.len == 0) return null;
    var pairs_buf: [32]ts_projection.TagPair = undefined;
    if (filters.len > pairs_buf.len) return null;
    for (filters, 0..) |f, i| {
        if (f.op != .eq) return null;
        pairs_buf[i] = .{ .key = f.key, .value = f.value };
    }
    return ts_projection.canonicalTagHashPairs(pairs_buf[0..filters.len], scratch);
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
    // `?tags=host=web-01,env=prod` selects one tag-series; absent = every tag-series.
    const tag_filter: ?u64 = if (h.parseQueryParam([]const u8, query_string, "tags")) |t|
        (if (t.len > 0) ts_projection.canonicalTagHash(t) else null)
    else
        null;
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
                const result = ts.queryRange(ns_hash, measurement, field, tag_filter, from_ns, to_ns, &point_buf) catch continue;
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

/// POST /timeseries/floql — Execute a FloQL query.
///
/// Mirrors the wire handler (`ts/handler.zig` handleFloQL): parse → resolve the
/// source into a SeriesSet → run the pipeline stages → serialize. The one
/// difference is source resolution: the wire handler reads a single shard's
/// projection, while the dashboard must merge points across every shard.
///
/// Query text comes from the POST body or `?q=`; `?namespace=` selects the
/// namespace.
pub fn executeFloql(allocator: Allocator, method: Method, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = method;

    // A POST body is taken verbatim; a `?q=` param arrives percent-encoded
    // (the console sends encodeURIComponent), so it must be decoded before the
    // parser ever sees it — otherwise `cpu%5B1h%5D` fails to parse.
    var q_decode_buf: [4096]u8 = undefined;
    const query_text = if (body.len > 0)
        body
    else if (h.parseQueryParam([]const u8, query_string, "q")) |raw|
        (if (raw.len <= q_decode_buf.len) h.percentDecode(&q_decode_buf, raw) else raw)
    else
        "";
    if (query_text.len == 0) return try h.jsonError(allocator, "Empty query");
    const ns_hash = router.namespaceHash(h.parseQueryParam([]const u8, query_string, "namespace") orelse "default");

    // 1. Parse
    var query = floql_parser.Parser.parse(query_text, allocator) catch
        return try h.jsonError(allocator, "FloQL parse error");
    defer query.deinit(allocator);

    const measurement = query.source.measurement;
    if (measurement.len == 0) return try h.jsonError(allocator, "FloQL: measurement name is required");

    // 2. Time range — the AST carries ms, the projection stores ns.
    const now_ms: i64 = @import("stdx").time.milliTimestamp();
    var from_ns: u64 = 0;
    var to_ns: u64 = std.math.maxInt(u64);
    if (query.source.range.duration_ms > 0) {
        const from_ms = now_ms - query.source.range.duration_ms;
        from_ns = if (from_ms > 0) @intCast(from_ms * std.time.ns_per_ms) else 0;
        to_ns = @intCast(now_ms * std.time.ns_per_ms);
    } else if (query.source.range.from_ms > 0) {
        from_ns = @intCast(query.source.range.from_ms * std.time.ns_per_ms);
        if (query.source.range.to_ms > 0) to_ns = @intCast(query.source.range.to_ms * std.time.ns_per_ms);
    }

    // 3. Field — from a `field()` stage when present, else the default.
    var field_name: []const u8 = "value";
    for (query.stages) |stage| {
        switch (stage) {
            .field => |f| {
                field_name = f.name;
                break;
            },
            else => {},
        }
    }

    // 4. Resolve the source across every shard.
    //
    // Source tag filters are applied here when they are expressible as an exact
    // tag-set hash (all `=`); `!=`/regex/partial sets stay unfiltered — see
    // floqlTagFilter.
    var tag_scratch: [1024]u8 = undefined;
    const src_tag_filter = floqlTagFilter(query.source.filters, &tag_scratch);
    const StoredPoint = @import("../../../projection/ts.zig").StoredPoint;
    var points: std.ArrayList(ss_mod.DataPoint) = .empty;
    defer points.deinit(allocator);
    {
        const n = shardCount(ctx);
        for (0..n) |i| {
            if (getTSProjection(ctx, i)) |ts| {
                var point_buf: [4096]StoredPoint = undefined;
                const qr = ts.queryRange(ns_hash, measurement, field_name, src_tag_filter, from_ns, to_ns, &point_buf) catch continue;
                for (point_buf[0..qr.points_in_buffer]) |sp| {
                    points.append(allocator, .{
                        .timestamp_ms = @intCast(sp.timestamp_ns / std.time.ns_per_ms),
                        .value = sp.field_value,
                    }) catch break;
                }
            }
        }
    }
    // Per-shard results are each ordered, but interleave once merged; window/
    // rate/delta stages assume a time-ordered input.
    std.mem.sort(ss_mod.DataPoint, points.items, {}, struct {
        fn lt(_: void, a: ss_mod.DataPoint, b: ss_mod.DataPoint) bool {
            return a.timestamp_ms < b.timestamp_ms;
        }
    }.lt);

    // 5. Build the initial SeriesSet (one series — see the tag caveat below).
    const dp_slice = allocator.dupe(ss_mod.DataPoint, points.items) catch
        return try h.jsonError(allocator, "FloQL: out of memory");
    const series_slice = allocator.alloc(ss_mod.Series, 1) catch {
        allocator.free(dp_slice);
        return try h.jsonError(allocator, "FloQL: out of memory");
    };
    series_slice[0] = .{ .key = measurement, .field = field_name, .points = dp_slice };
    var initial = ss_mod.SeriesSet.fromOwned(allocator, series_slice);

    // 6. Run the pipeline. `execute` returns `initial` unchanged for an empty
    //    pipeline; otherwise it returns a fresh set and does NOT free `initial`
    //    (freeing it in the empty case would double-free).
    var result_set = floql_executor.execute(query.stages, initial, allocator) catch {
        initial.deinit();
        return try h.jsonError(allocator, "FloQL execution failed");
    };
    defer result_set.deinit();
    if (query.stages.len > 0) initial.deinit();

    // 7. Serialize
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("query", query_text);
    {
        var series_arr = try obj.arrayField("series");
        try series_arr.begin();
        for (result_set.series) |s| {
            try series_arr.next();
            var sobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try sobj.begin();
            try sobj.stringField("key", s.key);
            try sobj.stringField("field", s.field);
            try sobj.intField("point_count", @as(i64, @intCast(s.points.len)));
            {
                var tags_arr = try sobj.arrayField("tags");
                try tags_arr.begin();
                for (s.tags) |t| {
                    try tags_arr.next();
                    var tobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try tobj.begin();
                    try tobj.stringField("key", t.key);
                    try tobj.stringField("value", t.value);
                    try tobj.end();
                }
                try tags_arr.end();
            }
            {
                var pts_arr = try sobj.arrayField("points");
                try pts_arr.begin();
                for (s.points) |p| {
                    try pts_arr.next();
                    var pobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try pobj.begin();
                    try pobj.intField("timestamp", p.timestamp_ms);
                    try pobj.floatField("value", p.value);
                    try pobj.end();
                }
                try pts_arr.end();
            }
            try sobj.end();
        }
        try series_arr.end();
    }
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

test "executeFloql parses and executes a query" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    // No shards wired here, so the source resolves to zero points — but the
    // query must still parse and run the pipeline, echoing the query back and
    // emitting a real (single, empty) series rather than the old `[]` stub.
    const result = try executeFloql(allocator, .POST, null, "cpu{host=web-01}[1h]", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"query\":\"cpu{host=web-01}[1h]\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"key\":\"cpu\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"point_count\":0") != null);
}

test "executeFloql surfaces a parse error" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try executeFloql(allocator, .POST, null, "|||not a query|||", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
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
