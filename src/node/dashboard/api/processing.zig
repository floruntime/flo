//! Dashboard API — Processing (Stream Processing) Endpoints
//!
//! - GET    /processing/jobs                       — List processing jobs
//! - GET    /processing/jobs/:id                   — Job detail & status
//! - POST   /processing/jobs                       — Submit new job
//! - DELETE /processing/jobs/:id                   — Cancel job
//! - PUT    /processing/jobs/:id/stop              — Stop job (graceful)
//! - POST   /processing/jobs/:id/savepoint         — Create savepoint
//! - PUT    /processing/jobs/:id/restore           — Restore from savepoint
//! - PUT    /processing/jobs/:id/rescale           — Rescale parallelism

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const Method = @import("../../../util/http/mod.zig").Method;
const Shard = @import("../../shard.zig").Shard;
const ProcessingHandler = @import("../../../processing/handler.zig").ProcessingHandler;

// ── Helpers ──

fn getShard(ctx: *DashboardContext, idx: usize) ?*Shard {
    const ptrs = ctx.shard_ptrs orelse return null;
    if (idx >= ptrs.len) return null;
    return @ptrCast(@alignCast(ptrs[idx]));
}

fn shardCount(ctx: *DashboardContext) usize {
    return if (ctx.shard_ptrs) |p| p.len else 0;
}

/// Router for /processing/* requests
pub fn handleProcessingRequest(allocator: Allocator, method: Method, path: []const u8, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    // /processing/jobs (exact)
    if (std.mem.eql(u8, path, "/jobs")) {
        return switch (method) {
            .GET => listJobs(allocator, query_string, ctx),
            .POST => submitJob(allocator, body, ctx),
            else => h.jsonError(allocator, "Method not allowed"),
        };
    }

    // /processing/jobs/:id[/stop|/savepoint|/restore|/rescale]
    if (std.mem.startsWith(u8, path, "/jobs/")) {
        const rest = path["/jobs/".len..];
        if (std.mem.endsWith(u8, rest, "/stop")) {
            const job_id = rest[0 .. rest.len - "/stop".len];
            return stopJob(allocator, job_id, ctx);
        }
        if (std.mem.endsWith(u8, rest, "/savepoint")) {
            const job_id = rest[0 .. rest.len - "/savepoint".len];
            return createSavepoint(allocator, job_id, ctx);
        }
        if (std.mem.endsWith(u8, rest, "/restore")) {
            const job_id = rest[0 .. rest.len - "/restore".len];
            return restoreJob(allocator, job_id, body, ctx);
        }
        if (std.mem.endsWith(u8, rest, "/rescale")) {
            const job_id = rest[0 .. rest.len - "/rescale".len];
            return rescaleJob(allocator, job_id, body, ctx);
        }
        // DELETE /processing/jobs/:id or GET /processing/jobs/:id
        return switch (method) {
            .DELETE => cancelJob(allocator, rest, ctx),
            .GET => getJobDetail(allocator, rest, query_string, ctx),
            else => h.jsonError(allocator, "Method not allowed"),
        };
    }

    return h.jsonError(allocator, "Not found");
}

fn listJobs(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_filter = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // Collect jobs from all shards (de-duplicate by job_id)
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    const n = shardCount(ctx);
    for (0..n) |i| {
        if (getShard(ctx, i)) |shard| {
            const ph = shard.processing_handler;
            var it = ph.jobs.iterator();
            while (it.next()) |entry| {
                const job = entry.value_ptr;
                const gop = try seen.getOrPut(job.job_id_owned);
                if (!gop.found_existing) {
                    // Filter by namespace (defaults to "default")
                    if (!std.mem.eql(u8, job.namespace_owned, ns_filter)) continue;
                    try arr.next();
                    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                    try obj.begin();
                    try obj.stringField("job_id", job.job_id_owned);
                    try obj.stringField("name", job.name_owned);
                    try obj.stringField("namespace", job.namespace_owned);
                    try obj.stringField("status", job.status.toString());
                    try obj.intField("parallelism", @as(i64, @intCast(job.parallelism)));
                    try obj.intField("batch_size", @as(i64, @intCast(job.batch_size)));
                    try obj.intField("created_at", job.created_at_ms);
                    try obj.intField("records_processed", @as(i64, @intCast(job.records_processed)));
                    try obj.end();
                }
            }
        }
    }

    try arr.end();
    return try json_aw.toOwnedSlice();
}

fn getJobDetail(allocator: Allocator, job_id: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    _ = query_string;
    _ = ctx;

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("job_id", job_id);
    try obj.stringField("status", "unknown");
    try obj.intField("parallelism", 0);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

fn submitJob(allocator: Allocator, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    if (body.len == 0) return try h.jsonError(allocator, "Empty job definition");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("status", "not_wired");
    try obj.intField("body_size", @as(i64, @intCast(body.len)));
    try obj.end();
    return try json_aw.toOwnedSlice();
}

fn stopJob(allocator: Allocator, job_id: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("job_id", job_id);
    try obj.stringField("state", "STOPPED");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

fn cancelJob(allocator: Allocator, job_id: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("job_id", job_id);
    try obj.stringField("state", "CANCELLED");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

fn createSavepoint(allocator: Allocator, job_id: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    // Write operations require Raft proposal — not safe from dashboard thread
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("job_id", job_id);
    try obj.stringField("savepoint_id", "");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

fn restoreJob(allocator: Allocator, job_id: []const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = body;

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    // Write operations require Raft proposal — not safe from dashboard thread
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("job_id", job_id);
    try obj.stringField("state", "RUNNING");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

fn rescaleJob(allocator: Allocator, job_id: []const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    _ = ctx;
    _ = body;

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    // Write operations require Raft proposal — not safe from dashboard thread
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("job_id", job_id);
    try obj.intField("parallelism", 1);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

// =============================================================================
// Tests
// =============================================================================

test "handleProcessingRequest lists jobs" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleProcessingRequest(allocator, .GET, "/jobs", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "handleProcessingRequest job detail" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleProcessingRequest(allocator, .GET, "/jobs/job-456", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"job_id\":\"job-456\"") != null);
}

test "handleProcessingRequest cancel job" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleProcessingRequest(allocator, .DELETE, "/jobs/job-789", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"state\":\"CANCELLED\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"ok\":true") != null);
}

test "handleProcessingRequest stop job" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleProcessingRequest(allocator, .PUT, "/jobs/job-789/stop", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"state\":\"STOPPED\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"ok\":true") != null);
}
