//! Dashboard API — Actions Endpoints
//!
//! - GET  /actions                      — List registered actions
//! - GET  /actions/:name                — Action detail (metadata, trigger info)
//! - GET  /actions/:name/runs           — Execution history
//! - POST /actions/:name/invoke         — Invoke action (async)

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const Shard = @import("../../shard.zig").Shard;
const ActionsHandler = @import("../../../actions/handler.zig").ActionsHandler;
const WorkerHandler = @import("../../../worker/handler.zig").WorkerHandler;
const workers = @import("worker.zig");

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
    const ns_filter = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

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
                // Filter by namespace
                if (!std.mem.eql(u8, rec.namespace_owned, ns_filter)) continue;
                const gop = try seen.getOrPut(rec.name_owned);
                if (!gop.found_existing) {
                    // Count runs for this action across all shards
                    var counts = h.RunCounts{};
                    var worker_count: u64 = 0;
                    for (0..n) |si| {
                        if (getShard(ctx, si)) |s| {
                            var rit = s.actions_handler.runs.iterator();
                            while (rit.next()) |re| {
                                if (std.mem.eql(u8, re.value_ptr.action_name_owned, rec.name_owned)) {
                                    counts.total += 1;
                                    switch (re.value_ptr.status) {
                                        .pending => counts.pending += 1,
                                        .running => counts.running += 1,
                                        .completed => counts.completed += 1,
                                        .failed => counts.failed += 1,
                                        .cancelled => counts.cancelled += 1,
                                        .timed_out => counts.timed_out += 1,
                                    }
                                }
                            }
                            // Count workers that handle this action
                            var wit = s.worker_handler.workers.iterator();
                            while (wit.next()) |we| {
                                for (we.value_ptr.processes.items) |p| {
                                    if (p.kind == .action and std.mem.eql(u8, p.name_owned, rec.name_owned)) {
                                        worker_count += 1;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    try arr.next();
                    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try obj.begin();
                    try obj.stringField("name", rec.name_owned);
                    try obj.stringField("namespace", rec.namespace_owned);
                    try obj.stringField("type", if (rec.action_type == 1) "wasm" else "user");
                    try obj.stringField("owner", "");
                    try obj.stringField("description", "");
                    try obj.intField("version", @as(i64, @intCast(rec.version)));
                    try obj.boolField("enabled", rec.enabled);
                    try obj.intField("timeout_ms", 30000);
                    try obj.intField("max_retries", 3);
                    try obj.intField("created_at", @as(i64, @intCast(rec.created_at_ns / std.time.ns_per_ms)));
                    try obj.intField("updated_at", @as(i64, @intCast(rec.created_at_ns / std.time.ns_per_ms)));
                    try obj.intField("worker_count", @as(i64, @intCast(worker_count)));
                    if (rec.action_type == 1) {
                        const wasm_size: u64 = if (rec.wasm_blob_owned) |b| b.len else 0;
                        try obj.intField("wasm_module_size", @as(i64, @intCast(wasm_size)));
                    }
                    {
                        var runs_obj = try obj.objectField("runs");
                        try runs_obj.begin();
                        try runs_obj.intField("total", @as(i64, @intCast(counts.total)));
                        try runs_obj.intField("pending", @as(i64, @intCast(counts.pending)));
                        try runs_obj.intField("running", @as(i64, @intCast(counts.running)));
                        try runs_obj.intField("completed", @as(i64, @intCast(counts.completed)));
                        try runs_obj.intField("failed", @as(i64, @intCast(counts.failed)));
                        try runs_obj.intField("cancelled", @as(i64, @intCast(counts.cancelled)));
                        try runs_obj.intField("timed_out", @as(i64, @intCast(counts.timed_out)));
                        try runs_obj.end();
                    }
                    try obj.end();
                }
            }
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /actions/:name — Action detail (metadata, trigger info, run counts, workers, recent runs)
pub fn getActionDetail(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_filter = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Search shards for this action (matching namespace)
    var found_rec: ?*const ActionsHandler.ActionRecord = null;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            if (shard.actions_handler.actions.getPtr(name)) |rec| {
                if (std.mem.eql(u8, rec.namespace_owned, ns_filter)) {
                    found_rec = rec;
                    break;
                }
            }
        }
    }

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);

    if (found_rec) |rec| {
        try obj.stringField("namespace", rec.namespace_owned);
        try obj.stringField("type", if (rec.action_type == 1) "wasm" else "user");
        try obj.stringField("owner", "");
        try obj.stringField("description", "");
        try obj.intField("version", @as(i64, @intCast(rec.version)));
        try obj.boolField("enabled", rec.enabled);
        try obj.intField("timeout_ms", 30000);
        try obj.intField("max_retries", 3);
        try obj.intField("retry_delay_ms", 1000);
        try obj.intField("created_at", @as(i64, @intCast(rec.created_at_ns / std.time.ns_per_ms)));
        try obj.intField("updated_at", @as(i64, @intCast(rec.created_at_ns / std.time.ns_per_ms)));
        if (rec.action_type == 1) {
            const wasm_size: u64 = if (rec.wasm_blob_owned) |b| b.len else 0;
            try obj.intField("wasm_module_size", @as(i64, @intCast(wasm_size)));
        }

        // Count runs for this action across all shards
        var counts = h.RunCounts{};
        for (0..n) |i| {
            if (getShard(ctx, i)) |shard| {
                var rit = shard.actions_handler.runs.iterator();
                while (rit.next()) |re| {
                    if (std.mem.eql(u8, re.value_ptr.action_name_owned, name)) {
                        counts.total += 1;
                        switch (re.value_ptr.status) {
                            .pending => counts.pending += 1,
                            .running => counts.running += 1,
                            .completed => counts.completed += 1,
                            .failed => counts.failed += 1,
                            .cancelled => counts.cancelled += 1,
                            .timed_out => counts.timed_out += 1,
                        }
                    }
                }
            }
        }

        // runs object
        {
            var runs_obj = try obj.objectField("runs");
            try runs_obj.begin();
            try runs_obj.intField("total", @as(i64, @intCast(counts.total)));
            try runs_obj.intField("pending", @as(i64, @intCast(counts.pending)));
            try runs_obj.intField("running", @as(i64, @intCast(counts.running)));
            try runs_obj.intField("completed", @as(i64, @intCast(counts.completed)));
            try runs_obj.intField("failed", @as(i64, @intCast(counts.failed)));
            try runs_obj.intField("cancelled", @as(i64, @intCast(counts.cancelled)));
            try runs_obj.intField("timed_out", @as(i64, @intCast(counts.timed_out)));
            try runs_obj.end();
        }

        // recent_runs — last 20 runs for this action
        {
            var recent_arr = try obj.arrayField("recent_runs");
            try recent_arr.begin();
            var rcount: u32 = 0;
            for (0..n) |i| {
                if (rcount >= 20) break;
                if (getShard(ctx, i)) |shard| {
                    var rit = shard.actions_handler.runs.iterator();
                    while (rit.next()) |re| {
                        if (rcount >= 20) break;
                        const run = re.value_ptr;
                        if (!std.mem.eql(u8, run.action_name_owned, name)) continue;
                        try recent_arr.next();
                        var robj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                        try robj.begin();
                        try robj.stringField("run_id", run.run_id_owned);
                        try robj.stringField("status", @tagName(run.status));
                        try robj.intField("attempt", 1);
                        try robj.intField("created_at", run.created_at_ms);
                        if (run.started_at_ms) |t| {
                            try robj.intField("started_at", t);
                        } else {
                            try robj.nullField("started_at");
                        }
                        if (run.completed_at_ms) |t| {
                            try robj.intField("completed_at", t);
                        } else {
                            try robj.nullField("completed_at");
                        }
                        if (run.worker_id_owned) |wid| {
                            try robj.stringField("worker_id", wid);
                        } else {
                            try robj.nullField("worker_id");
                        }
                        if (run.error_owned) |err_msg| {
                            try robj.stringField("error", err_msg);
                        } else {
                            try robj.nullField("error");
                        }
                        if (run.result_owned) |res| {
                            try robj.stringField("outcome", res);
                        } else {
                            try robj.nullField("outcome");
                        }
                        if (run.input_owned) |inp| {
                            // Truncate input to 2KB for overview
                            const max_len = @min(inp.len, 2048);
                            try robj.stringField("input", inp[0..max_len]);
                        } else {
                            try robj.nullField("input");
                        }
                        if (run.result_owned) |res| {
                            // Truncate output to 2KB for overview
                            const max_len = @min(res.len, 2048);
                            try robj.stringField("output", res[0..max_len]);
                        } else {
                            try robj.nullField("output");
                        }
                        try robj.stringField("source", switch (run.source) {
                            1 => "workflow",
                            2 => "trigger",
                            else => "direct",
                        });
                        try robj.end();
                        rcount += 1;
                    }
                }
            }
            try recent_arr.end();
        }

        // workers — workers that handle this action
        {
            var workers_arr = try obj.arrayField("workers");
            try workers_arr.begin();
            for (0..n) |i| {
                if (getShard(ctx, i)) |shard| {
                    var wit = shard.worker_handler.workers.iterator();
                    while (wit.next()) |we| {
                        const w = we.value_ptr;
                        for (w.processes.items) |p| {
                            if (p.kind == .action and std.mem.eql(u8, p.name_owned, name)) {
                                try workers_arr.next();
                                try workers.writeWorkerJson(writer, w);
                                break;
                            }
                        }
                    }
                }
            }
            try workers_arr.end();
        }
    } else {
        try obj.stringField("namespace", "default");
        try obj.stringField("type", "user");
        try obj.stringField("owner", "");
        try obj.stringField("description", "");
        try obj.intField("version", 0);
        try obj.boolField("enabled", false);
        try obj.intField("timeout_ms", 0);
        try obj.intField("max_retries", 0);
        try obj.intField("retry_delay_ms", 0);
        try obj.intField("created_at", 0);
        try obj.intField("updated_at", 0);
        {
            var runs_obj = try obj.objectField("runs");
            try runs_obj.begin();
            try runs_obj.intField("total", 0);
            try runs_obj.intField("pending", 0);
            try runs_obj.intField("running", 0);
            try runs_obj.intField("completed", 0);
            try runs_obj.intField("failed", 0);
            try runs_obj.intField("cancelled", 0);
            try runs_obj.intField("timed_out", 0);
            try runs_obj.end();
        }
        {
            var recent_arr = try obj.arrayField("recent_runs");
            try recent_arr.begin();
            try recent_arr.end();
        }
        {
            var workers_arr = try obj.arrayField("workers");
            try workers_arr.begin();
            try workers_arr.end();
        }
    }

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /actions/:name/runs — Execution history
pub fn getActionRuns(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const limit = h.parseQueryParam(u64, query_string, "limit") orelse 100;
    const offset = h.parseQueryParam(u64, query_string, "offset") orelse 0;

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    var seen: u64 = 0;
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
                seen += 1;
                if (seen <= offset) continue;
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
                if (run.worker_id_owned) |wid| {
                    try obj.stringField("worker_id", wid);
                } else {
                    try obj.nullField("worker_id");
                }
                if (run.error_owned) |err_msg| {
                    try obj.stringField("error", err_msg);
                } else {
                    try obj.nullField("error");
                }
                if (run.result_owned) |res| {
                    try obj.stringField("outcome", res);
                } else {
                    try obj.nullField("outcome");
                }
                if (run.input_owned) |inp| {
                    const max_len = @min(inp.len, 2048);
                    try obj.stringField("input", inp[0..max_len]);
                } else {
                    try obj.nullField("input");
                }
                if (run.result_owned) |res| {
                    const max_len = @min(res.len, 2048);
                    try obj.stringField("output", res[0..max_len]);
                } else {
                    try obj.nullField("output");
                }
                try obj.stringField("source", switch (run.source) {
                    1 => "workflow",
                    2 => "trigger",
                    else => "direct",
                });
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
    try std.testing.expect(std.mem.indexOf(u8, result, "\"type\":\"user\"") != null);
}
