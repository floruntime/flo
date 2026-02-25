//! Dashboard API — Workflow Endpoints
//!
//! - GET    /workflow/definitions          — List workflow definitions
//! - POST   /workflow/definitions          — Create definition
//! - GET    /workflow/definitions/:name    — Get definition
//! - PUT    /workflow/definitions/:name/enable  — Enable workflow
//! - PUT    /workflow/definitions/:name/disable — Disable workflow
//! - GET    /workflow/runs                 — List runs
//! - POST   /workflow/runs                 — Start run
//! - GET    /workflow/runs/:id             — Run status
//! - GET    /workflow/runs/:id/history     — Run step history
//! - DELETE /workflow/runs/:id             — Cancel run
//! - POST   /workflow/runs/:id/signal      — Send signal

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const Method = @import("../../../util/http/mod.zig").Method;

/// Router for /workflow/* requests
pub fn handleWorkflowRequest(allocator: Allocator, method: Method, path: []const u8, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    // /workflow/definitions
    if (std.mem.eql(u8, path, "/definitions")) {
        return switch (method) {
            .GET => listDefinitions(allocator, query_string, ctx),
            .POST => createDefinition(allocator, body, ctx),
            else => h.jsonError(allocator, "Method not allowed"),
        };
    }

    // /workflow/runs (exact match)
    if (std.mem.eql(u8, path, "/runs")) {
        return switch (method) {
            .GET => listRuns(allocator, query_string, ctx),
            .POST => startRun(allocator, body, ctx),
            else => h.jsonError(allocator, "Method not allowed"),
        };
    }

    // /workflow/definitions/:name[/enable|/disable]
    if (std.mem.startsWith(u8, path, "/definitions/")) {
        const rest = path["/definitions/".len..];
        if (std.mem.endsWith(u8, rest, "/enable")) {
            const name = rest[0 .. rest.len - "/enable".len];
            return enableWorkflow(allocator, name, ctx);
        }
        if (std.mem.endsWith(u8, rest, "/disable")) {
            const name = rest[0 .. rest.len - "/disable".len];
            return disableWorkflow(allocator, name, ctx);
        }
        // /workflow/definitions/:name
        return getDefinition(allocator, rest, ctx);
    }

    // /workflow/runs/:id[/history|/signal]
    if (std.mem.startsWith(u8, path, "/runs/")) {
        const rest = path["/runs/".len..];
        if (std.mem.endsWith(u8, rest, "/history")) {
            const run_id = rest[0 .. rest.len - "/history".len];
            return getRunHistory(allocator, run_id, ctx);
        }
        if (std.mem.endsWith(u8, rest, "/signal")) {
            const run_id = rest[0 .. rest.len - "/signal".len];
            return signalRun(allocator, run_id, body, ctx);
        }
        // DELETE /workflow/runs/:id or GET /workflow/runs/:id
        return switch (method) {
            .DELETE => cancelRun(allocator, rest, ctx),
            .GET => getRunStatus(allocator, rest, ctx),
            else => h.jsonError(allocator, "Method not allowed"),
        };
    }

    return h.jsonError(allocator, "Not found");
}

// ---------------------------------------------------------------------------
// Definitions
// ---------------------------------------------------------------------------

fn listDefinitions(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = h.parseQueryParam([]const u8, query_string, "namespace");

    // TODO: Wire to shard inbox for workflow definitions
    return try allocator.dupe(u8, "[]");
}

fn createDefinition(allocator: Allocator, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    if (body.len == 0) return try h.jsonError(allocator, "Empty definition body");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for workflow creation
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("status", "not_wired");
    try obj.intField("body_size", @as(i64, @intCast(body.len)));
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn getDefinition(allocator: Allocator, name: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // TODO: Wire to shard inbox for workflow definition detail
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);
    try obj.stringField("status", "unknown");
    try obj.boolField("enabled", false);
    try obj.intField("run_count", 0);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn enableWorkflow(allocator: Allocator, name: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);
    try obj.boolField("enabled", true);
    try obj.stringField("status", "not_wired");
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn disableWorkflow(allocator: Allocator, name: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);
    try obj.boolField("enabled", false);
    try obj.stringField("status", "not_wired");
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

fn listRuns(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = h.parseQueryParam([]const u8, query_string, "workflow");
    _ = h.parseQueryParam([]const u8, query_string, "status");
    _ = h.parseQueryParam(u64, query_string, "limit");

    // TODO: Wire to shard inbox for workflow run list, use WorkflowMetrics
    return try allocator.dupe(u8, "[]");
}

fn startRun(allocator: Allocator, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    if (body.len == 0) return try h.jsonError(allocator, "Empty run request body");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("status", "not_wired");
    try obj.intField("body_size", @as(i64, @intCast(body.len)));
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn getRunStatus(allocator: Allocator, run_id: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("run_id", run_id);
    try obj.stringField("status", "unknown");
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn getRunHistory(allocator: Allocator, run_id: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = run_id;

    // TODO: Wire to shard inbox for workflow run step history
    return try allocator.dupe(u8, "[]");
}

fn cancelRun(allocator: Allocator, run_id: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("run_id", run_id);
    try obj.stringField("status", "not_wired");
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn signalRun(allocator: Allocator, run_id: []const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("run_id", run_id);
    try obj.stringField("status", "not_wired");
    try obj.intField("signal_size", @as(i64, @intCast(body.len)));
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// Tests
// =============================================================================

test "handleWorkflowRequest routes definitions" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleWorkflowRequest(allocator, .GET, "/definitions", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "handleWorkflowRequest routes runs" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleWorkflowRequest(allocator, .GET, "/runs", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "handleWorkflowRequest get definition detail" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleWorkflowRequest(allocator, .GET, "/definitions/my-workflow", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"my-workflow\"") != null);
}

test "handleWorkflowRequest cancel run" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleWorkflowRequest(allocator, .DELETE, "/runs/run-123", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"run_id\":\"run-123\"") != null);
}
