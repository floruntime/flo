//! Dashboard API — Worker Endpoints
//!
//! - GET  /workers      — List registered workers
//! - GET  /workers/:id  — Worker detail

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const Shard = @import("../../shard.zig").Shard;
const worker_handler_mod = @import("../../../worker/handler.zig");
const WorkerHandler = worker_handler_mod.WorkerHandler;
const WorkerRecord = worker_handler_mod.WorkerRecord;

// ── Helpers ──

fn getShard(ctx: *DashboardContext, idx: usize) ?*Shard {
    const ptrs = ctx.shard_ptrs orelse return null;
    if (idx >= ptrs.len) return null;
    return @ptrCast(@alignCast(ptrs[idx]));
}

fn shardCount(ctx: *DashboardContext) usize {
    return if (ctx.shard_ptrs) |p| p.len else 0;
}

/// GET /workers — List registered workers across all shards
pub fn getWorkers(allocator: Allocator, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // De-duplicate workers across shards (each worker hashes to one shard, but be safe)
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            const wh = shard.worker_handler;
            var it = wh.workers.iterator();
            while (it.next()) |entry| {
                const w = entry.value_ptr;
                const gop = try seen.getOrPut(w.id_owned);
                if (!gop.found_existing) {
                    try arr.next();
                    try writeWorkerJson(writer, w);
                }
            }
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /workers/:id — Single worker detail
pub fn getWorkerDetail(allocator: Allocator, worker_id: []const u8, ctx: *DashboardContext) ![]const u8 {
    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            if (shard.worker_handler.workers.getPtr(worker_id)) |w| {
                var json_buf: std.ArrayList(u8) = .empty;
                errdefer json_buf.deinit(allocator);
                const writer = json_buf.writer(allocator);
                try writeWorkerJson(writer, w);
                return try json_buf.toOwnedSlice(allocator);
            }
        }
    }
    return h.jsonError(allocator, "Worker not found");
}

/// Serialize a WorkerRecord to JSON. Public so actions.zig can embed workers in action detail.
pub fn writeWorkerJson(writer: anytype, w: *const WorkerRecord) !void {
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("worker_id", w.id_owned);
    try obj.stringField("status", @tagName(w.status));
    try obj.stringField("worker_type", if (w.worker_type == .action) "action" else "stream");
    if (w.machine_id_owned) |mid| {
        try obj.stringField("machine_id", mid);
    } else {
        try obj.nullField("machine_id");
    }
    try obj.intField("current_load", @as(i64, @intCast(w.current_load)));
    try obj.intField("max_concurrent", @as(i64, @intCast(w.max_concurrency)));
    try obj.intField("tasks_completed", @as(i64, @intCast(w.tasks_completed)));
    try obj.intField("tasks_failed", @as(i64, @intCast(w.tasks_failed)));
    try obj.intField("last_seen", w.last_heartbeat_ms);
    try obj.intField("registered_at", w.registered_at_ms);
    if (w.metadata_owned) |meta| {
        try obj.stringField("metadata", meta);
    } else {
        try obj.nullField("metadata");
    }

    // Processes array
    {
        var parr = try obj.arrayField("processes");
        try parr.begin();
        for (w.processes.items) |p| {
            try parr.next();
            var pobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try pobj.begin();
            try pobj.stringField("name", p.name_owned);
            try pobj.stringField("kind", if (p.kind == .action) "action" else "stream_consumer");
            try pobj.intField("run_count", @as(i64, @intCast(p.run_count)));
            try pobj.intField("fail_count", @as(i64, @intCast(p.fail_count)));
            try pobj.intField("last_run_at", p.last_run_at_ms);
            try pobj.end();
        }
        try parr.end();
    }

    try obj.end();
}

// =============================================================================
// Tests
// =============================================================================

test "getWorkers returns empty array" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getWorkers(allocator, &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}
