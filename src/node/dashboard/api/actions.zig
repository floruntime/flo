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
const Shard = @import("../../shard.zig").Shard;
const ActionsHandler = @import("../../../actions/handler.zig").ActionsHandler;

// ── Helpers ──

fn getShard(ctx: *DashboardContext, idx: usize) ?*Shard {
    const ptrs = ctx.shard_ptrs orelse return null;
    if (idx >= ptrs.len) return null;
    return @ptrCast(@alignCast(ptrs[idx]));
}

fn shardCount(ctx: *DashboardContext) usize {
    return if (ctx.shard_ptrs) |p| p.len else 0;
}

/// GET /actions — List all registered actions with run counts
pub fn getActions(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = h.parseQueryParam([]const u8, query_string, "namespace");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // Collect actions from all shards (de-duplicate by name)
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            const ah = shard.actions_handler;
            var it = ah.actions.iterator();
            while (it.next()) |entry| {
                const rec = entry.value_ptr;
                const gop = try seen.getOrPut(rec.name_owned);
                if (!gop.found_existing) {
                    try arr.next();
                    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try obj.begin();
                    try obj.stringField("name", rec.name_owned);
                    try obj.stringField("type", if (rec.action_type == 1) "wasm" else "user");
                    try obj.intField("version", @as(i64, @intCast(rec.version)));
                    try obj.boolField("enabled", rec.enabled);
                    try obj.intField("created_at", @as(i64, @intCast(rec.created_at_ns / std.time.ns_per_ms)));
                    try obj.end();
                }
            }
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /actions/:name — Action detail (metadata, trigger info)
pub fn getActionDetail(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = h.parseQueryParam([]const u8, query_string, "namespace");

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Search shards for this action
    var found_rec: ?*const ActionsHandler.ActionRecord = null;
    var found_handler: ?*const ActionsHandler = null;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            const ah = shard.actions_handler;
            if (ah.actions.getPtr(name)) |rec| {
                found_rec = rec;
                found_handler = ah;
                break;
            }
        }
    }

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);

    if (found_rec) |rec| {
        try obj.stringField("status", if (rec.enabled) "active" else "disabled");
        try obj.stringField("type", if (rec.action_type == 1) "wasm" else "user");
        try obj.intField("version", @as(i64, @intCast(rec.version)));
        try obj.intField("created_at", @as(i64, @intCast(rec.created_at_ns / std.time.ns_per_ms)));

        // Count runs for this action across all shards
        var total_runs: u64 = 0;
        var successful: u64 = 0;
        var failed: u64 = 0;
        for (0..n) |i| {
            if (getShard(ctx, i)) |shard| {
                var rit = shard.actions_handler.runs.iterator();
                while (rit.next()) |re| {
                    if (std.mem.eql(u8, re.value_ptr.action_name_owned, name)) {
                        total_runs += 1;
                        if (re.value_ptr.status == .completed) successful += 1;
                        if (re.value_ptr.status == .failed) failed += 1;
                    }
                }
            }
        }
        try obj.intField("total_runs", @as(i64, @intCast(total_runs)));
        try obj.intField("successful_runs", @as(i64, @intCast(successful)));
        try obj.intField("failed_runs", @as(i64, @intCast(failed)));
    } else {
        try obj.stringField("status", "unknown");
        try obj.stringField("type", "wasm");
        try obj.intField("total_runs", 0);
        try obj.intField("successful_runs", 0);
        try obj.intField("failed_runs", 0);
    }

    {
        var triggers_arr = try obj.arrayField("triggers");
        try triggers_arr.begin();
        try triggers_arr.end();
    }
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /actions/:name/runs — Execution history
pub fn getActionRuns(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const limit = h.parseQueryParam(u64, query_string, "limit") orelse 100;
    _ = h.parseQueryParam(u64, query_string, "offset");

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
            var it = shard.actions_handler.runs.iterator();
            while (it.next()) |entry| {
                if (count >= limit) break;
                const run = entry.value_ptr;
                if (!std.mem.eql(u8, run.action_name_owned, name)) continue;
                try arr.next();
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("run_id", run.run_id_owned);
                try obj.stringField("action", run.action_name_owned);
                try obj.stringField("status", @tagName(run.status));
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

/// POST /actions/:name/invoke — Invoke action
pub fn invokeAction(allocator: Allocator, name: []const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Write operations require Raft proposal — not safe from dashboard thread
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

    // No worker tracking at handler level yet
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
