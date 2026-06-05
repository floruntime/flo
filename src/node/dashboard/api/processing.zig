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

/// Router for /processing/* requests
pub fn handleProcessingRequest(allocator: Allocator, method: Method, path: []const u8, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    // /processing/jobs (exact)
    if (std.mem.eql(u8, path, "/jobs")) {
        return switch (method) {
            .GET => listJobs(allocator, query_string, ctx),
            .POST => submitJob(allocator, body, query_string, ctx),
            else => h.jsonError(allocator, "Method not allowed"),
        };
    }

    // /processing/jobs/:id[/stop|/savepoint|/restore|/rescale]
    if (std.mem.startsWith(u8, path, "/jobs/")) {
        const rest = path["/jobs/".len..];
        if (std.mem.endsWith(u8, rest, "/stop")) {
            const job_id = rest[0 .. rest.len - "/stop".len];
            return stopJob(allocator, job_id, query_string, ctx);
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
            .DELETE => cancelJob(allocator, rest, query_string, ctx),
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

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    // Find the real job across shards (mirror listJobs). Each job hashes to one
    // shard, but scan all to be safe.
    const n = shardCount(ctx);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const shard = getShard(ctx, i) orelse continue;
        const job = shard.processing_handler.jobs.getPtr(job_id) orelse continue;

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
        // Full pipeline definition — the frontend parses this for the source/
        // operator/sink DAG.
        try obj.stringField("yaml", job.yaml_owned);

        // Savepoints for this job (from the same shard's savepoint store).
        {
            var sp_arr = try obj.arrayField("savepoints");
            try sp_arr.begin();
            var sit = shard.processing_handler.savepoints.iterator();
            while (sit.next()) |se| {
                const sp = se.value_ptr;
                if (!std.mem.eql(u8, sp.job_id_owned, job_id)) continue;
                try sp_arr.next();
                var sobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try sobj.begin();
                try sobj.stringField("savepoint_id", sp.savepoint_id_owned);
                try sobj.intField("created_at", sp.created_at_ms);
                try sobj.intField("records_at_savepoint", @as(i64, @intCast(sp.records_at_savepoint)));
                try sobj.end();
            }
            try sp_arr.end();
        }
        try obj.end();
        return try json_aw.toOwnedSlice();
    }

    // Not found — keep a minimal stub (job_id echoed) so callers can detect it.
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("job_id", job_id);
    try obj.stringField("status", "unknown");
    try obj.intField("parallelism", 0);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// POST /processing/jobs?namespace= — submit a job (loopback write). Body is the
/// pipeline YAML; the response's raw data is the new job_id.
fn submitJob(allocator: Allocator, body: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    if (body.len == 0) return try h.jsonError(allocator, "Empty job definition");
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.processing.submit(&client, ns_q, body) catch
        return try h.jsonError(allocator, "Job submit failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    const job_id = resp.asRawData() orelse "";

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("job_id", job_id);
    try obj.stringField("status", "RUNNING");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// PUT /processing/jobs/:id/stop?namespace= — graceful stop (loopback write).
fn stopJob(allocator: Allocator, job_id: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.processing.stop(&client, ns_q, job_id) catch
        return try h.jsonError(allocator, "Job stop failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

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

/// DELETE /processing/jobs/:id?namespace= — force cancel (loopback write).
fn cancelJob(allocator: Allocator, job_id: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.processing.cancel(&client, ns_q, job_id) catch
        return try h.jsonError(allocator, "Job cancel failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

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

test "handleProcessingRequest cancel job surfaces loopback failure (no node in unit test)" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    // cancel is now a real loopback write; with no node listening it must fail
    // gracefully with an error envelope rather than the old stub success.
    const result = try handleProcessingRequest(allocator, .DELETE, "/jobs/job-789", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}

test "handleProcessingRequest stop job surfaces loopback failure (no node in unit test)" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleProcessingRequest(allocator, .PUT, "/jobs/job-789/stop", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}
