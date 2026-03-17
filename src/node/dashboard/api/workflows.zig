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
const Shard = @import("../../shard.zig").Shard;
const WorkflowHandler = @import("../../../workflow/handler.zig").WorkflowHandler;

// ── Helpers ──

fn getShard(ctx: *DashboardContext, idx: usize) ?*Shard {
    const ptrs = ctx.shard_ptrs orelse return null;
    if (idx >= ptrs.len) return null;
    return @ptrCast(@alignCast(ptrs[idx]));
}

fn shardCount(ctx: *DashboardContext) usize {
    return if (ctx.shard_ptrs) |p| p.len else 0;
}

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
    _ = h.parseQueryParam([]const u8, query_string, "namespace");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // Collect definitions from all shards (de-duplicate by name)
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            const wh = shard.workflow_handler;
            var it = wh.definitions.iterator();
            while (it.next()) |entry| {
                const rec = entry.value_ptr;
                const gop = try seen.getOrPut(rec.name_owned);
                if (!gop.found_existing) {
                    try arr.next();
                    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try obj.begin();
                    try obj.stringField("name", rec.name_owned);
                    try obj.stringField("version", rec.version_owned);
                    try obj.intField("created_at", rec.created_at_ms);
                    // Check if disabled
                    const is_disabled = wh.disabled.contains(entry.key_ptr.*);
                    try obj.boolField("enabled", !is_disabled);
                    try obj.end();
                }
            }
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn createDefinition(allocator: Allocator, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    if (body.len == 0) return try h.jsonError(allocator, "Empty definition body");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Write operations require Raft proposal — not safe from dashboard thread
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("status", "not_wired");
    try obj.intField("body_size", @as(i64, @intCast(body.len)));
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn getDefinition(allocator: Allocator, name: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Search shards for this definition
    var found_rec: ?*const WorkflowHandler.DefinitionRecord = null;
    var found_handler: ?*const WorkflowHandler = null;
    var found_key: ?[]const u8 = null;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            const wh = shard.workflow_handler;
            // Definitions are keyed by "namespace:name"; try the name directly first
            if (wh.definitions.getPtr(name)) |rec| {
                found_rec = rec;
                found_handler = wh;
                found_key = name;
                break;
            }
            // Also search by matching the record's name_owned field
            var it = wh.definitions.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.value_ptr.name_owned, name)) {
                    found_rec = entry.value_ptr;
                    found_handler = wh;
                    found_key = entry.key_ptr.*;
                    break;
                }
            }
            if (found_rec != null) break;
        }
    }

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);

    if (found_rec) |rec| {
        try obj.stringField("version", rec.version_owned);
        try obj.intField("created_at", rec.created_at_ms);
        const is_disabled = if (found_handler) |wh_ptr| (if (found_key) |k| wh_ptr.disabled.contains(k) else false) else false;
        try obj.boolField("enabled", !is_disabled);
        try obj.stringField("status", if (is_disabled) "disabled" else "active");

        // Count runs for this workflow
        var run_count: u64 = 0;
        for (0..n) |i| {
            if (getShard(ctx, i)) |shard| {
                var rit = shard.workflow_handler.runs.iterator();
                while (rit.next()) |re| {
                    if (std.mem.eql(u8, re.value_ptr.workflow_name_owned, name)) {
                        run_count += 1;
                    }
                }
            }
        }
        try obj.intField("run_count", @as(i64, @intCast(run_count)));
    } else {
        try obj.stringField("status", "unknown");
        try obj.boolField("enabled", false);
        try obj.intField("run_count", 0);
    }

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
    const workflow_filter = h.parseQueryParam([]const u8, query_string, "workflow");
    const status_filter = h.parseQueryParam([]const u8, query_string, "status");
    const limit = h.parseQueryParam(u64, query_string, "limit") orelse 100;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    var count: u64 = 0;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (count >= limit) break;
        if (getShard(ctx, i)) |shard| {
            var it = shard.workflow_handler.runs.iterator();
            while (it.next()) |entry| {
                if (count >= limit) break;
                const run = entry.value_ptr;
                // Filter by workflow name if specified
                if (workflow_filter) |wf| {
                    if (!std.mem.eql(u8, run.workflow_name_owned, wf)) continue;
                }
                // Filter by status if specified
                const status_str = run.status.toString();
                if (status_filter) |sf| {
                    if (!std.mem.eql(u8, status_str, sf)) continue;
                }
                try arr.next();
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("run_id", run.run_id_owned);
                try obj.stringField("workflow", run.workflow_name_owned);
                try obj.stringField("version", run.workflow_version_owned);
                try obj.stringField("status", status_str);
                try obj.intField("created_at", run.created_at_ms);
                if (run.started_at_ms) |t| {
                    try obj.intField("started_at", t);
                } else {
                    try obj.nullField("started_at");
                }
                if (run.completed_at_ms) |t| {
                    try obj.intField("completed_at", t);
                } else {
                    try obj.nullField("completed_at");
                }
                try obj.end();
                count += 1;
            }
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
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
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Search for the run across shards
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            const run_ptr = shard.workflow_handler.runs.getPtr(run_id) orelse blk: {
                // Fallback: linear scan by run_id_owned
                var rit = shard.workflow_handler.runs.iterator();
                while (rit.next()) |entry| {
                    if (std.mem.eql(u8, entry.value_ptr.run_id_owned, run_id)) {
                        break :blk entry.value_ptr;
                    }
                }
                break :blk null;
            };
            if (run_ptr) |run| {
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("run_id", run.run_id_owned);
                try obj.stringField("status", run.status.toString());
                if (run.current_step_name_owned) |step| {
                    try obj.stringField("current_step", step);
                } else {
                    try obj.nullField("current_step");
                }
                if (run.started_at_ms) |t| {
                    try obj.intField("started_at_ms", t);
                }
                if (run.completed_at_ms) |t| {
                    try obj.intField("updated_at_ms", t);
                }
                try obj.end();
                return try json_buf.toOwnedSlice(allocator);
            }
        }
    }

    // Not found
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("error", "Run not found");
    try obj.stringField("run_id", run_id);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

fn getRunHistory(allocator: Allocator, run_id: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // Search for the run across shards
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            if (shard.workflow_handler.runs.getPtr(run_id)) |run| {
                // Found the run — emit its history events
                for (run.history.items) |event| {
                    try arr.next();
                    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try obj.begin();
                    try obj.stringField("event_type", event.event_type_owned);
                    try obj.stringField("detail", event.detail_owned);
                    try obj.intField("timestamp", event.timestamp_ms);
                    try obj.end();
                }
                break; // found it, no need to search more shards
            }
            // Also search by record's run_id field
            var rit = shard.workflow_handler.runs.iterator();
            while (rit.next()) |entry| {
                if (std.mem.eql(u8, entry.value_ptr.run_id_owned, run_id)) {
                    for (entry.value_ptr.history.items) |event| {
                        try arr.next();
                        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                        try obj.begin();
                        try obj.stringField("event_type", event.event_type_owned);
                        try obj.stringField("detail", event.detail_owned);
                        try obj.intField("timestamp", event.timestamp_ms);
                        try obj.end();
                    }
                    break;
                }
            }
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
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
