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

/// GET /timeseries — List all measurements with field counts and stats
pub fn getMeasurements(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = h.parseQueryParam([]const u8, query_string, "namespace");

    // TODO: Wire to shard inbox for TS measurement list
    return try allocator.dupe(u8, "[]");
}

/// GET /timeseries/:measurement — Measurement detail
pub fn getMeasurementDetail(allocator: Allocator, measurement: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = h.parseQueryParam([]const u8, query_string, "namespace");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for TS measurement info
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", measurement);
    try obj.intField("field_count", 0);
    var fields_arr = try obj.arrayField("fields");
    try fields_arr.begin();
    try fields_arr.end();
    try obj.intField("series_count", 0);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /timeseries/:measurement/data — Query data points
pub fn getSeriesData(allocator: Allocator, measurement: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = h.parseQueryParam([]const u8, query_string, "field");
    _ = h.parseQueryParam([]const u8, query_string, "from");
    _ = h.parseQueryParam([]const u8, query_string, "to");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for TS data query
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("measurement", measurement);
    var data_arr = try obj.arrayField("data");
    try data_arr.begin();
    try data_arr.end();
    try obj.intField("count", 0);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// POST /timeseries/floql — Execute FloQL query
pub fn executeFloql(allocator: Allocator, method: Method, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = method;

    // Query can come from POST body or GET ?q= param
    const query_text = if (body.len > 0) body else (h.parseQueryParam([]const u8, query_string, "q") orelse "");
    if (query_text.len == 0) return try h.jsonError(allocator, "Empty query");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for FloQL execution
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("query", query_text);
    var results_arr = try obj.arrayField("results");
    try results_arr.begin();
    try results_arr.end();
    try obj.intField("count", 0);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
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
    try std.testing.expect(std.mem.indexOf(u8, result, "\"results\":[]") != null);
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
