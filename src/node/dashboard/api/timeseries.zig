//! Dashboard API — Time-Series Endpoints
//!
//! - GET  /timeseries                       — All measurements
//! - GET  /timeseries/:measurement          — Measurement detail
//! - GET  /timeseries/:measurement/data     — Query data points
//! - POST /timeseries/floql                 — Execute FloQL query
//!
//! The dashboard asks each shard, as any client does: every read is a
//! request over loopback to the node's own protocol port.

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const Method = @import("../../../util/http/mod.zig").Method;
const floql_parser = @import("../../../ts/floql/parser.zig");
const loopback = @import("loopback.zig");
const client_mod = @import("../../../cli/client/mod.zig");
const output = @import("../../../cli/output.zig");
const WireReader = @import("../../../util/wire.zig").WireReader;

/// `ts_list`'s entry: `[name_len:u16][name]`.
const measurement_columns = [_]output.WireColumn{
    .{ .field = "name", .header = "NAME", .field_type = .str_u16 },
};

const MeasurementLister = struct {
    namespace: []const u8,
    pub fn ask(self: MeasurementLister, client: *client_mod.Client, cursor: ?[]const u8) !client_mod.Response {
        return client_mod.ts.list(client, self.namespace, null, cursor);
    }
};

/// GET /timeseries — The namespace's measurements (`ts_list`). No op
/// answers a measurement's series, fields or points yet, so those are null.
pub fn getMeasurements(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "time-series list", err);
    defer reader.deinit();
    var pages = reader.listAll(allocator, &measurement_columns, MeasurementLister{ .namespace = ns }) catch |err|
        return try loopback.readFailed(allocator, "time-series list", err);
    defer pages.deinit();

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();
    var r = WireReader.init(pages.list());
    _ = r.readU32();
    for (0..pages.count) |_| {
        // Each page's rows were checked whole as they were taken.
        const name = r.readLengthPrefixed(u16) orelse break;
        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", name);
        try obj.nullField("series_count");
        try obj.nullField("field_count");
        try obj.nullField("points");
        try obj.end();
    }
    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// GET /timeseries/:measurement — Measurement detail. No op answers a
/// measurement's fields or series yet, so those are null; nothing is read.
pub fn getMeasurementDetail(allocator: Allocator, measurement: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", measurement);
    try obj.stringField("namespace", ns);
    try obj.nullField("field_count");
    try obj.nullField("fields");
    try obj.nullField("series_count");
    try obj.nullField("series");
    try obj.nullField("retention");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /timeseries/:measurement/data — One field's raw points (`ts_read`,
/// answered by the measurement's shard). `?tags=k=v,k=v` selects matching
/// tag-series; `?from=` / `?to=` bound it in ms. `window` and `aggregation`
/// are echoed back, not applied.
pub fn getSeriesData(allocator: Allocator, measurement: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const field = h.parseQueryParam([]const u8, query_string, "field") orelse "value";
    const tags = h.parseQueryParam([]const u8, query_string, "tags") orelse "";
    const from_str = h.parseQueryParam([]const u8, query_string, "from");
    const to_str = h.parseQueryParam([]const u8, query_string, "to");
    const window = h.parseQueryParam(u64, query_string, "window") orelse 0;
    const aggregation = h.parseQueryParam([]const u8, query_string, "aggregation") orelse "none";
    const from_val: i64 = if (from_str) |s| std.fmt.parseInt(i64, s, 10) catch 0 else 0;
    const to_val: i64 = if (to_str) |s| std.fmt.parseInt(i64, s, 10) catch 0 else 0;

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "time-series read", err);
    defer reader.deinit();
    const client = reader.next() catch |err| return try loopback.readFailed(allocator, "time-series read", err);
    var got = client_mod.ts.read(client, ns, measurement, .{
        .tags = tags,
        .field = field,
        .from_ms = @max(from_val, 0),
        .to_ms = @max(to_val, 0),
    }) catch |err| return try loopback.readFailed(allocator, "time-series read", err);
    defer got.deinit();
    if (got.isError() and !got.isNotFound()) return try h.jsonError(allocator, got.errorMessage());
    // A namespace never created holds no points.
    const points: []const u8 = if (got.isNotFound()) "" else got.data;

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("measurement", measurement);
    try obj.stringField("field", field);
    try obj.stringField("aggregation", aggregation);
    try obj.intField("window_ms", @as(i64, @intCast(@min(window, std.math.maxInt(i64)))));
    try obj.intField("from_ms", from_val);
    try obj.intField("to_ms", to_val);
    {
        // [count:u32]([timestamp_ms:i64][value:f64])*
        var series_arr = try obj.arrayField("series");
        try series_arr.begin();
        var r = WireReader.init(points);
        const count = r.readU32() orelse 0;
        for (0..count) |_| {
            const ts_ms = r.readI64() orelse break;
            const bits = r.readU64() orelse break;
            try series_arr.next();
            var pobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try pobj.begin();
            try pobj.intField("timestamp", ts_ms);
            try pobj.floatField("value", @bitCast(bits));
            try pobj.end();
        }
        try series_arr.end();
    }
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// POST /timeseries/floql — Execute a FloQL query (`ts_floql`), on the
/// shard that holds the query's measurement.
///
/// Query text comes from the POST body or `?q=`; `?namespace=` selects the
/// namespace. The answer carries no tags, so each series' `tags` is empty.
pub fn executeFloql(allocator: Allocator, method: Method, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = method;

    // A POST body is taken verbatim; a `?q=` param arrives percent-encoded
    // (the console sends encodeURIComponent), so it must be decoded before the
    // parser ever sees it — otherwise `cpu%5B1h%5D` fails to parse.
    var q_decode_buf: [4096]u8 = undefined;
    const query_text = if (body.len > 0)
        body
    else if (h.parseQueryParam([]const u8, query_string, "q")) |raw|
        (h.percentDecode(&q_decode_buf, raw) orelse raw)
    else
        "";
    if (query_text.len == 0) return try h.jsonError(allocator, "Empty query");
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    // Parsed here only for its measurement: the request is routed by it, as
    // a write to the measurement was.
    var query = floql_parser.Parser.parse(query_text, allocator) catch
        return try h.jsonError(allocator, "FloQL parse error");
    defer query.deinit(allocator);
    const measurement = query.source.measurement;
    if (measurement.len == 0) return try h.jsonError(allocator, "FloQL: measurement name is required");

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "FloQL query", err);
    defer reader.deinit();
    const client = reader.next() catch |err| return try loopback.readFailed(allocator, "FloQL query", err);
    var got = client.sendRequest(.ts_floql, ns, measurement, query_text) catch |err|
        return try loopback.readFailed(allocator, "FloQL query", err);
    defer got.deinit();
    if (got.isError() and !got.isNotFound()) return try h.jsonError(allocator, got.errorMessage());
    // A namespace never created holds no series: a set of none.
    const set: []const u8 = if (got.isNotFound()) &.{ 0, 0, 0, 0 } else got.data;
    if (!wholeSeriesSet(set)) return try h.jsonError(allocator, "FloQL query: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("query", query_text);
    {
        var series_arr = try obj.arrayField("series");
        try series_arr.begin();
        var r = WireReader.init(set);
        const series_count = r.readU32() orelse 0;
        for (0..series_count) |_| {
            const key = r.readLengthPrefixed(u32).?;
            const field = r.readLengthPrefixed(u32).?;
            const point_count = r.readU32().?;
            try series_arr.next();
            var sobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try sobj.begin();
            try sobj.stringField("key", key);
            try sobj.stringField("field", field);
            try sobj.intField("point_count", @as(i64, point_count));
            {
                var tags_arr = try sobj.arrayField("tags");
                try tags_arr.begin();
                try tags_arr.end();
            }
            {
                var pts_arr = try sobj.arrayField("points");
                try pts_arr.begin();
                for (0..point_count) |_| {
                    const ts_ms = r.readI64().?;
                    const bits = r.readU64().?;
                    try pts_arr.next();
                    var pobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try pobj.begin();
                    try pobj.intField("timestamp", ts_ms);
                    try pobj.floatField("value", @bitCast(bits));
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

/// Whether `data` is a whole encoded SeriesSet: `[series_count:u32]`, then
/// per series `[key_len:u32][key][field_len:u32][field][point_count:u32]`
/// and that many `[timestamp_ms:i64][value:f64]`.
fn wholeSeriesSet(data: []const u8) bool {
    var r = WireReader.init(data);
    const series_count = r.readU32() orelse return false;
    for (0..series_count) |_| {
        _ = r.readLengthPrefixed(u32) orelse return false;
        _ = r.readLengthPrefixed(u32) orelse return false;
        const point_count = r.readU32() orelse return false;
        if (r.remainingLen() / 16 < point_count) return false;
        _ = r.skip(@as(usize, point_count) * 16);
    }
    return true;
}

// =============================================================================
// Tests
// =============================================================================

/// A context whose loopback connect is refused: nothing listens on port 1.
fn unreachableContext(allocator: Allocator, metrics: *h.MetricsRegistry) DashboardContext {
    var ctx = DashboardContext.init(allocator, metrics, 1);
    ctx.listen_port = 1;
    return ctx;
}

test "getMeasurements says the list failed when the node can't be reached" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = unreachableContext(allocator, &metrics);

    const result = try getMeasurements(allocator, null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "{\"error\":\"time-series list"));
}

test "getMeasurementDetail answers what it knows and reads nothing" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = unreachableContext(allocator, &metrics);

    const result = try getMeasurementDetail(allocator, "cpu", null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"cpu\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"fields\":null") != null);
}

test "executeFloql parses the query before it asks the node" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = unreachableContext(allocator, &metrics);

    // A query that parses goes to the node; none answers here.
    const asked = try executeFloql(allocator, .POST, null, "cpu{host=web-01}[1h]", &ctx);
    defer allocator.free(asked);
    try std.testing.expect(std.mem.startsWith(u8, asked, "{\"error\":\"FloQL query"));

    const bad = try executeFloql(allocator, .POST, null, "|||not a query|||", &ctx);
    defer allocator.free(bad);
    try std.testing.expectEqualStrings("{\"error\":\"FloQL parse error\"}", bad);
}

test "wholeSeriesSet refuses a cut answer" {
    // One series "m"/"value" with one point.
    const whole = [_]u8{ 1, 0, 0, 0, 1, 0, 0, 0, 'm', 5, 0, 0, 0, 'v', 'a', 'l', 'u', 'e', 1, 0, 0, 0 } ++ @as([16]u8, @splat(0));
    try std.testing.expect(wholeSeriesSet(&whole));
    try std.testing.expect(!wholeSeriesSet(whole[0 .. whole.len - 1]));
    try std.testing.expect(!wholeSeriesSet(&.{}));
}

test "executeFloql rejects empty query" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = unreachableContext(allocator, &metrics);

    const result = try executeFloql(allocator, .POST, null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}
