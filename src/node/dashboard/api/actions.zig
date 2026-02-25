//! Dashboard API — Actions Endpoints
//!
//! - GET  /actions                      — List registered actions
//! - GET  /actions/:name                — Action detail (metadata, trigger info)
//! - GET  /actions/:name/runs           — Execution history
//! - POST /actions/:name/invoke         — Invoke action (async)
//! - GET  /workers                      — List WASM workers

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;

/// GET /actions — List all registered actions with run counts
pub fn getActions(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = h.parseQueryParam([]const u8, query_string, "namespace");

    // TODO: Wire to shard inbox for action list
    return try allocator.dupe(u8, "[]");
}

/// GET /actions/:name — Action detail (metadata, trigger info)
pub fn getActionDetail(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = h.parseQueryParam([]const u8, query_string, "namespace");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for action detail
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);
    try obj.stringField("status", "unknown");
    try obj.stringField("type", "wasm");
    var triggers_arr = try obj.arrayField("triggers");
    try triggers_arr.begin();
    try triggers_arr.end();
    try obj.intField("total_runs", 0);
    try obj.intField("successful_runs", 0);
    try obj.intField("failed_runs", 0);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /actions/:name/runs — Execution history
pub fn getActionRuns(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = name;
    _ = h.parseQueryParam(u64, query_string, "limit");
    _ = h.parseQueryParam(u64, query_string, "offset");

    // TODO: Wire to shard inbox for action run history
    return try allocator.dupe(u8, "[]");
}

/// POST /actions/:name/invoke — Invoke action
pub fn invokeAction(allocator: Allocator, name: []const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for action invocation
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("action", name);
    try obj.stringField("status", "not_wired");
    try obj.intField("input_size", @as(i64, @intCast(body.len)));
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /workers — List WASM workers
pub fn getWorkers(allocator: Allocator, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    // TODO: Wire to shard inbox for worker list
    return try allocator.dupe(u8, "[]");
}

// =============================================================================
// Tests
// =============================================================================

test "getActions returns empty array" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getActions(allocator, null, &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "getActionDetail returns stub" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getActionDetail(allocator, "my_action", null, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"my_action\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"type\":\"wasm\"") != null);
}

test "getWorkers returns empty array" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getWorkers(allocator, &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}
