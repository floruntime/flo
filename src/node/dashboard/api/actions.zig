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
const client_mod = @import("../../../cli/client/mod.zig");

// ── Helpers ──

fn getShard(ctx: *DashboardContext, idx: usize) ?*Shard {
    const ptrs = ctx.shard_ptrs orelse return null;
    if (idx >= ptrs.len) return null;
    return @ptrCast(@alignCast(ptrs[idx]));
}

/// Short-lived loopback client to the node's own protocol port — mutations can't
/// be proposed from the dashboard thread (see api/kv.zig loopbackConnect).
fn loopbackConnect(allocator: Allocator, ctx: *DashboardContext) !client_mod.Client {
    var ep_buf: [32]u8 = undefined;
    const endpoint = try std.fmt.bufPrint(&ep_buf, "127.0.0.1:{d}", .{ctx.listen_port});
    var client = client_mod.Client.init(allocator, endpoint);
    errdefer client.deinit();
    try client.connect();
    return client;
}

fn shardCount(ctx: *DashboardContext) usize {
    return if (ctx.shard_ptrs) |p| p.len else 0;
}

const LatencyStats = struct { count: u64 = 0, avg_ms: i64 = 0, p99_ms: i64 = 0 };

/// Real invocation latency for an action, derived from completed run records
/// (`completed_at_ms - started_at_ms`). avg is over all completed runs; p99 is
/// over a bounded sample (the first `LAT_CAP` completed runs found).
fn computeLatency(allocator: Allocator, ctx: *DashboardContext, name: []const u8) LatencyStats {
    const LAT_CAP: usize = 4096;
    const buf = allocator.alloc(i64, LAT_CAP) catch return .{};
    defer allocator.free(buf);

    var n_samples: usize = 0;
    var sum: i64 = 0;
    var total: u64 = 0;
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            var rit = shard.actions_handler.runs.iterator();
            while (rit.next()) |re| {
                const run = re.value_ptr;
                if (!std.mem.eql(u8, run.action_name_owned, name)) continue;
                const started = run.started_at_ms orelse continue;
                const completed = run.completed_at_ms orelse continue;
                if (completed < started) continue;
                const lat = completed - started;
                total += 1;
                sum += lat;
                if (n_samples < LAT_CAP) {
                    buf[n_samples] = lat;
                    n_samples += 1;
                }
            }
        }
    }

    if (total == 0) return .{};
    var stats = LatencyStats{ .count = total, .avg_ms = @divTrunc(sum, @as(i64, @intCast(total))) };
    if (n_samples > 0) {
        std.mem.sort(i64, buf[0..n_samples], {}, std.sort.asc(i64));
        const idx = @min((n_samples * 99) / 100, n_samples - 1);
        stats.p99_ms = buf[idx];
    }
    return stats;
}

/// Emit a `latency` JSON object (avg/p99 in ms + sample count).
fn writeLatency(obj: anytype, stats: LatencyStats) !void {
    var lat_obj = try obj.objectField("latency");
    try lat_obj.begin();
    try lat_obj.intField("count", @as(i64, @intCast(stats.count)));
    try lat_obj.intField("avg_ms", stats.avg_ms);
    try lat_obj.intField("p99_ms", stats.p99_ms);
    try lat_obj.end();
}

/// GET /actions — List all registered actions with run counts
pub fn getActions(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_filter = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

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
                    try obj.stringField("type", rec.action_type.toString());
                    try obj.stringField("owner", rec.owner_owned);
                    try obj.stringField("description", "");
                    try obj.intField("version", @as(i64, @intCast(rec.version)));
                    try obj.boolField("enabled", rec.enabled);
                    try obj.intField("timeout_ms", @as(i64, @intCast(rec.timeout_ms)));
                    try obj.intField("max_retries", @as(i64, @intCast(rec.max_retries)));
                    try obj.intField("created_at", @as(i64, @intCast(rec.created_at_ns / std.time.ns_per_ms)));
                    try obj.intField("updated_at", @as(i64, @intCast(rec.created_at_ns / std.time.ns_per_ms)));
                    try obj.intField("worker_count", @as(i64, @intCast(worker_count)));
                    try writeLatency(&obj, computeLatency(allocator, ctx, rec.name_owned));
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
    return try json_aw.toOwnedSlice();
}

/// GET /actions/:name — Action detail (metadata, trigger info, run counts, workers, recent runs)
pub fn getActionDetail(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_filter = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

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
        try obj.stringField("type", rec.action_type.toString());
        try obj.stringField("owner", rec.owner_owned);
        try obj.stringField("description", "");
        try obj.intField("version", @as(i64, @intCast(rec.version)));
        try obj.boolField("enabled", rec.enabled);
        try obj.intField("timeout_ms", @as(i64, @intCast(rec.timeout_ms)));
        try obj.intField("max_retries", @as(i64, @intCast(rec.max_retries)));
        try obj.intField("retry_delay_ms", 1000); // not persisted — see gap log
        try obj.intField("created_at", @as(i64, @intCast(rec.created_at_ns / std.time.ns_per_ms)));
        try obj.intField("updated_at", @as(i64, @intCast(rec.created_at_ns / std.time.ns_per_ms)));
        try writeLatency(&obj, computeLatency(allocator, ctx, name));

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
                        if (run.caller_run_id_owned) |crid| {
                            try robj.stringField("caller_run_id", crid);
                        } else {
                            try robj.nullField("caller_run_id");
                        }
                        if (run.caller_workflow_name_owned) |cwn| {
                            try robj.stringField("caller_workflow", cwn);
                        } else {
                            try robj.nullField("caller_workflow");
                        }
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
        // Action not registered — return stub with empty defaults
        try obj.stringField("namespace", ns_filter);
        try obj.stringField("type", "user");
        try obj.stringField("owner", "");
        try obj.stringField("description", "");
        try obj.intField("version", 0);
        try obj.boolField("enabled", false);
        try obj.intField("timeout_ms", 30000);
        try obj.intField("max_retries", 3);
        try obj.intField("retry_delay_ms", 1000);
        try obj.intField("created_at", 0);
        try obj.intField("updated_at", 0);
        try writeLatency(&obj, .{});
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
    return try json_aw.toOwnedSlice();
}

/// GET /actions/:name/runs — Execution history
pub fn getActionRuns(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const limit = h.parseQueryParam(u64, query_string, "limit") orelse 100;
    const offset = h.parseQueryParam(u64, query_string, "offset") orelse 0;
    const ns_filter = h.parseQueryParam([]const u8, query_string, "namespace");
    _ = ns_filter; // runs are already filtered by action name

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

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
                if (run.caller_run_id_owned) |crid| {
                    try obj.stringField("caller_run_id", crid);
                } else {
                    try obj.nullField("caller_run_id");
                }
                if (run.caller_workflow_name_owned) |cwn| {
                    try obj.stringField("caller_workflow", cwn);
                } else {
                    try obj.nullField("caller_workflow");
                }
                try obj.end();
                count += 1;
            }
        }
    }

    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// POST /actions/:name/invoke?namespace= — Invoke action (loopback write)
/// Mutations can't be proposed from the dashboard thread, so we issue the real
/// `action_invoke` over a short-lived loopback client to the node's own protocol
/// port (same path as the CLI). The async invoke returns a run_id immediately.
pub fn invokeAction(allocator: Allocator, name: []const u8, body: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const input = if (body.len > 0) body else "{}";

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.action.invoke(&client, ns_q, name, input, null, null, null) catch
        return try h.jsonError(allocator, "Action invoke failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    // Response wire format: [run_id_len:u16][run_id][has_output:u8]...
    var run_id: []const u8 = "";
    if (resp.asRawData()) |data| {
        if (data.len >= 2) {
            const run_id_len = std.mem.readInt(u16, data[0..2], .little);
            const run_id_end = @as(usize, 2) + run_id_len;
            if (data.len >= run_id_end and run_id_len > 0) run_id = data[2..run_id_end];
        }
    }

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("action", name);
    try obj.stringField("namespace", ns_q);
    try obj.stringField("status", "invoked");
    try obj.stringField("run_id", run_id);
    try obj.end();
    return try json_aw.toOwnedSlice();
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
