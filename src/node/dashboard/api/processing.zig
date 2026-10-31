//! Dashboard API — Processing (Stream Processing) Endpoints
//!
//! - GET    /processing/jobs                       — List processing jobs
//! - GET    /processing/jobs/:id                   — Job detail & status
//! - POST   /processing/jobs                       — Submit new job
//! - DELETE /processing/jobs/:id                   — Cancel job
//! - PUT    /processing/jobs/:id/stop              — Stop job (graceful)

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;
const Method = @import("../../../util/http/mod.zig").Method;
const client_mod = @import("../../../cli/client/mod.zig");
const loopback = @import("loopback.zig");
const output = @import("../../../cli/output.zig");
const WireReader = @import("../../../util/wire.zig").WireReader;

/// Short-lived loopback client to the node's own protocol port — mutations can't
/// be proposed from the dashboard thread (see api/kv.zig loopbackConnect).
fn loopbackConnect(allocator: Allocator, ctx: *DashboardContext) !client_mod.Client {
    var ep_buf: [32]u8 = undefined;
    const endpoint = try ctx.clientEndpoint(&ep_buf);
    var client = client_mod.Client.init(allocator, endpoint);
    errdefer client.deinit();
    try client.connect();
    return client;
}

/// `processing_list`'s entry: `[name_len:u16][name][job_id_len:u16][job_id]
/// [status_len:u16][status][created_at:i64]`.
const job_columns = [_]output.WireColumn{
    .{ .field = "name", .header = "NAME", .field_type = .str_u16 },
    .{ .field = "job_id", .header = "JOB ID", .field_type = .str_u16 },
    .{ .field = "status", .header = "STATUS", .field_type = .str_u16 },
    .{ .field = "created_at_ms", .header = "", .field_type = .int_i64 },
};

const JobLister = struct {
    namespace: []const u8,
    pub fn ask(self: JobLister, client: *client_mod.Client, cursor: ?[]const u8) !client_mod.Response {
        return client_mod.processing.list(client, self.namespace, null, cursor);
    }
};

/// A listed job's status reads `FAILED: <reason>` or `RUNNING (<note>)`;
/// the status alone is its first word.
fn statusWord(listed: []const u8) []const u8 {
    const end = std.mem.indexOfAny(u8, listed, ": ") orelse listed.len;
    return listed[0..end];
}

/// `processing_status`'s status byte, as the job's status reads.
fn statusName(code: u8) []const u8 {
    return switch (code) {
        0 => "RUNNING",
        1 => "STOPPED",
        2 => "CANCELLED",
        3 => "FAILED",
        4 => "COMPLETED",
        else => "UNKNOWN",
    };
}

/// Router for /processing/* requests
pub fn handleProcessingRequest(allocator: Allocator, method: Method, path: []const u8, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    // /processing/jobs (exact)
    if (std.mem.eql(u8, path, "/jobs")) {
        return switch (method) {
            .GET => listJobs(allocator, query_string, ctx),
            .POST => submitJob(allocator, body, query_string, ctx),
            else => error.MethodNotAllowed,
        };
    }

    // /processing/jobs/:id[/stop]
    if (std.mem.startsWith(u8, path, "/jobs/")) {
        const rest = path["/jobs/".len..];
        if (std.mem.endsWith(u8, rest, "/stop")) {
            if (method != .PUT) return error.MethodNotAllowed;
            const job_id = rest[0 .. rest.len - "/stop".len];
            return stopJob(allocator, job_id, query_string, ctx);
        }
        // DELETE /processing/jobs/:id or GET /processing/jobs/:id
        return switch (method) {
            .DELETE => cancelJob(allocator, rest, query_string, ctx),
            .GET => getJobDetail(allocator, rest, query_string, ctx),
            else => error.MethodNotAllowed,
        };
    }

    return h.jsonError(allocator, "Not found");
}

/// `processing_status`'s answer: [job_id_len:u16][job_id][name_len:u16][name]
/// [status:u8][batch_size:u32][records_processed:u64][created_at:i64]
/// [reason_len:u16][reason].
const JobStatus = struct {
    id: []const u8,
    name: []const u8,
    status: u8,
    batch_size: u32,
    records: u64,
    created_at: i64,
    reason: []const u8,

    fn decode(data: []const u8) ?JobStatus {
        var r = WireReader.init(data);
        const st: JobStatus = .{
            .id = r.readLengthPrefixed(u16) orelse return null,
            .name = r.readLengthPrefixed(u16) orelse return null,
            .status = r.readU8() orelse return null,
            .batch_size = r.readU32() orelse return null,
            .records = r.readU64() orelse return null,
            .created_at = r.readI64() orelse return null,
            .reason = r.readLengthPrefixed(u16) orelse return null,
        };
        return if (r.remainingLen() == 0) st else null;
    }
};

/// GET /processing/jobs — The namespace's jobs (`processing_list`). The
/// list answers no counts or live metrics, so those are null.
fn listJobs(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "processing job list", err);
    defer reader.deinit();
    var pages = reader.listAll(allocator, &job_columns, JobLister{ .namespace = ns }) catch |err|
        return try loopback.readFailed(allocator, "processing job list", err);
    defer pages.deinit();

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();
    var r = WireReader.init(pages.list());
    _ = r.readU32();
    for (0..pages.count) |_| {
        // Each page's rows were checked whole as they were taken.
        const name = r.readLengthPrefixed(u16) orelse break;
        const job_id = r.readLengthPrefixed(u16) orelse break;
        const status = r.readLengthPrefixed(u16) orelse break;
        const created_at = r.readI64() orelse break;
        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("job_id", job_id);
        try obj.stringField("name", name);
        try obj.stringField("namespace", ns);
        try obj.stringField("status", statusWord(status));
        try obj.nullField("batch_size");
        try obj.intField("created_at", created_at);
        try obj.nullField("records_processed");
        try obj.nullField("metrics");
        try obj.end();
    }
    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// GET /processing/jobs/:id?namespace= — One job (`processing_status`, on
/// the job's shard). Its definition and live metrics are null: no op
/// answers them yet.
fn getJobDetail(allocator: Allocator, job_id: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "processing job status", err);
    defer reader.deinit();
    const client = reader.next() catch |err| return try loopback.readFailed(allocator, "processing job status", err);
    var got = client_mod.processing.status(client, ns, job_id) catch |err|
        return try loopback.readFailed(allocator, "processing job status", err);
    defer got.deinit();

    // Not found: a stub, the id echoed, so callers can tell.
    if (got.isNotFound()) return try unknownJob(allocator, job_id);
    if (got.isError()) return try h.jsonError(allocator, got.errorMessage());
    const st = JobStatus.decode(got.data) orelse
        return try h.jsonError(allocator, "processing job status: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("job_id", st.id);
    try obj.stringField("name", st.name);
    try obj.stringField("namespace", ns);
    try obj.stringField("status", statusName(st.status));
    if (st.reason.len > 0) try obj.stringField("reason", st.reason) else try obj.nullField("reason");
    try obj.intField("batch_size", @as(i64, st.batch_size));
    try obj.intField("created_at", st.created_at);
    try obj.intField("records_processed", @as(i64, @intCast(st.records)));
    try obj.nullField("metrics");
    try obj.nullField("yaml");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

fn unknownJob(allocator: Allocator, job_id: []const u8) ![]const u8 {
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("job_id", job_id);
    try obj.stringField("status", "unknown");
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

// =============================================================================
// Tests
// =============================================================================

/// A context whose loopback connect is refused: nothing listens on port 1.
fn unreachableContext(allocator: Allocator, metrics: *h.MetricsRegistry) DashboardContext {
    var ctx = DashboardContext.init(allocator, metrics, 1);
    ctx.listen_port = 1;
    return ctx;
}

test "handleProcessingRequest says the job list failed when the node can't be reached" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = unreachableContext(allocator, &metrics);

    const result = try handleProcessingRequest(allocator, .GET, "/jobs", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "{\"error\":\"processing job list"));
}

test "handleProcessingRequest says the job status failed when the node can't be reached" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = unreachableContext(allocator, &metrics);

    const result = try handleProcessingRequest(allocator, .GET, "/jobs/job-456", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "{\"error\":\"processing job status"));
}

test "processing: a listed status reads as its first word, and a status answer decodes whole" {
    try std.testing.expectEqualStrings("FAILED", statusWord("FAILED: source gone"));
    try std.testing.expectEqualStrings("RUNNING", statusWord("RUNNING (held here: x)"));
    try std.testing.expectEqualStrings("STOPPED", statusWord("STOPPED"));

    const answer = [_]u8{ 1, 0, 'j', 1, 0, 'n', 3, 10, 0, 0, 0 } ++ [_]u8{ 7, 0, 0, 0, 0, 0, 0, 0 } ++ [_]u8{ 9, 0, 0, 0, 0, 0, 0, 0 } ++ [_]u8{ 2, 0, 'n', 'o' };
    const st = JobStatus.decode(&answer).?;
    try std.testing.expectEqualStrings("j", st.id);
    try std.testing.expectEqualStrings("FAILED", statusName(st.status));
    try std.testing.expectEqual(@as(u32, 10), st.batch_size);
    try std.testing.expectEqual(@as(u64, 7), st.records);
    try std.testing.expectEqualStrings("no", st.reason);
    try std.testing.expect(JobStatus.decode(answer[0 .. answer.len - 1]) == null);
}

test "handleProcessingRequest cancel job surfaces loopback failure (no node in unit test)" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = unreachableContext(allocator, &metrics);

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
    var ctx = unreachableContext(allocator, &metrics);

    const result = try handleProcessingRequest(allocator, .PUT, "/jobs/job-789/stop", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}
