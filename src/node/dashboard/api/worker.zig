//! Dashboard API — Worker Endpoints
//!
//! - GET  /workers      — List a namespace's workers, a page at a time
//! - GET  /workers/:id  — Worker detail

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const loopback = @import("loopback.zig");
const schema = @import("../../../protocol/schema.zig");
const worker_ops = @import("../../../protocol/worker_ops.zig");

/// GET /workers — A namespace's workers in id order, a page at a time.
/// Query params: ?namespace= (default "default"), ?limit= (at most a page's
/// worth), ?cursor= (the last id of the page before, as that page gave it). Read as a client over loopback
/// (`worker.list`, which gathers every shard's workers).
pub fn getWorkers(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const limit = std.math.clamp(h.parseQueryParam(u32, query_string, "limit") orelse worker_ops.MAX_LIST, 1, worker_ops.MAX_LIST);
    var cursor_buf: [worker_ops.WorkerId.max_size]u8 = undefined;
    const cursor = if (h.parseQueryParam([]const u8, query_string, "cursor")) |raw|
        h.percentDecode(&cursor_buf, raw) orelse return try h.jsonError(allocator, "cursor must be the id a page gave")
    else
        "";
    // Registration refuses an id holding a NUL, so such a cursor came from
    // no page and names no worker: say so rather than list from it.
    if (std.mem.indexOfScalar(u8, cursor, 0) != null) return try h.jsonError(allocator, "cursor must not hold a NUL");

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "worker list", err);
    defer reader.deinit();
    var got = reader.call(worker_ops.op("worker.list"), ns, "", .{ .limit = limit, .cursor = cursor, .action = "" }) catch |err|
        return try loopback.readFailed(allocator, "worker list", err);
    defer got.deinit();
    // A namespace never created has no workers.
    if (got.isNotFound()) return try allocator.dupe(u8, "{\"workers\":[],\"has_more\":false,\"cursor\":null}");
    if (got.isError()) return try h.jsonError(allocator, got.errorMessage());
    var diag: schema.Diagnostic = .{};
    const page = worker_ops.op("worker.list").response.decodeAll(got.data, &diag) catch
        return try h.jsonError(allocator, "worker list: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var outer = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try outer.begin();
    {
        var arr = try outer.arrayField("workers");
        try arr.begin();
        var it = page.workers.iterator();
        while (it.next() catch return try h.jsonError(allocator, "worker list: malformed answer")) |w| {
            try arr.next();
            try writeWorkerRow(writer, w, ns);
        }
        try arr.end();
    }
    try outer.boolField("has_more", page.cursor != null);
    if (page.cursor) |next| try outer.stringField("cursor", next) else try outer.nullField("cursor");
    try outer.end();
    return try json_aw.toOwnedSlice();
}

/// One worker as a page of the worker list gives it.
pub fn writeWorkerRow(writer: anytype, w: anytype, namespace: []const u8) !void {
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("worker_id", w.id);
    try obj.stringField("status", @tagName(w.status));
    try obj.stringField("worker_type", @tagName(w.worker_type));
    try obj.stringField("namespace", namespace);
    if (w.machine_id) |m| try obj.stringField("machine_id", m) else try obj.nullField("machine_id");
    try obj.intField("current_load", @as(i64, w.current_load));
    try obj.intField("max_concurrent", @as(i64, w.max_concurrency));
    try obj.intField("tasks_completed", @as(i64, @intCast(w.tasks_completed)));
    try obj.intField("tasks_failed", @as(i64, @intCast(w.tasks_failed)));
    try obj.intField("last_seen", w.last_heartbeat_ms);
    try obj.intField("registered_at", w.registered_at_ms);
    try obj.end();
}

/// Every worker in `namespace` that runs action `action`, as rows of `arr`,
/// read a page at a time to the last.
pub fn writeActionWorkers(reader: *loopback.Reader, arr: anytype, writer: anytype, namespace: []const u8, action: []const u8) !void {
    var cursor_buf: [worker_ops.WorkerId.max_size]u8 = undefined;
    var cursor: []const u8 = "";
    while (true) {
        var got = try reader.call(worker_ops.op("worker.list"), namespace, "", .{ .limit = worker_ops.MAX_LIST, .cursor = cursor, .action = action });
        defer got.deinit();
        // A namespace never created has no workers.
        if (got.isNotFound()) return;
        if (got.isError()) return error.Refused;
        var diag: schema.Diagnostic = .{};
        const page = try worker_ops.op("worker.list").response.decodeAll(got.data, &diag);
        var it = page.workers.iterator();
        while (try it.next()) |w| {
            try arr.next();
            try writeWorkerRow(writer, w, namespace);
        }
        const next = page.cursor orelse return;
        @memcpy(cursor_buf[0..next.len], next);
        cursor = cursor_buf[0..next.len];
    }
}

/// GET /workers/:id — One worker, with what it registered and runs. Read as
/// a client over loopback (`worker.info`).
pub fn getWorkerDetail(allocator: Allocator, worker_id: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "worker info", err);
    defer reader.deinit();
    var got = reader.call(worker_ops.op("worker.info"), ns, worker_id, .{}) catch |err|
        return try loopback.readFailed(allocator, "worker info", err);
    defer got.deinit();
    if (got.isNotFound()) return try h.jsonError(allocator, "Worker not found");
    if (got.isError()) return try h.jsonError(allocator, got.errorMessage());
    var diag: schema.Diagnostic = .{};
    const info = worker_ops.op("worker.info").response.decodeAll(got.data, &diag) catch
        return try h.jsonError(allocator, "worker info: malformed answer");
    const w = info.worker;

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("worker_id", w.id);
    try obj.stringField("status", @tagName(w.status));
    try obj.stringField("worker_type", @tagName(w.worker_type));
    try obj.stringField("namespace", ns);
    if (w.machine_id) |m| try obj.stringField("machine_id", m) else try obj.nullField("machine_id");
    try obj.intField("current_load", @as(i64, w.current_load));
    try obj.intField("max_concurrent", @as(i64, w.max_concurrency));
    try obj.intField("tasks_completed", @as(i64, @intCast(w.tasks_completed)));
    try obj.intField("tasks_failed", @as(i64, @intCast(w.tasks_failed)));
    try obj.intField("last_seen", w.last_heartbeat_ms);
    try obj.intField("registered_at", w.registered_at_ms);
    if (info.metadata) |m| try obj.stringField("metadata", m) else try obj.nullField("metadata");
    {
        var parr = try obj.arrayField("processes");
        try parr.begin();
        var it = info.processes.iterator();
        while (it.next() catch return try h.jsonError(allocator, "worker info: malformed answer")) |p| {
            try parr.next();
            var pobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try pobj.begin();
            try pobj.stringField("name", p.name);
            try pobj.stringField("kind", @tagName(p.kind));
            try pobj.intField("run_count", @as(i64, @intCast(p.run_count)));
            try pobj.intField("fail_count", @as(i64, @intCast(p.fail_count)));
            try pobj.intField("last_run_at", p.last_run_at_ms);
            try pobj.end();
        }
        try parr.end();
    }
    try obj.end();
    return try json_aw.toOwnedSlice();
}

// =============================================================================
// Tests
// =============================================================================

test "getWorkers refuses a cursor holding a NUL before it connects" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here: a connect would be refused

    const result = try getWorkers(allocator, "cursor=a%00b", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{\"error\":\"cursor must not hold a NUL\"}", result);
}
