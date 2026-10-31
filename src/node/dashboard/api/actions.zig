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
const client_mod = @import("../../../cli/client/mod.zig");
const loopback = @import("loopback.zig");
const worker_api = @import("worker.zig");
const output = @import("../../../cli/output.zig");
const schema = @import("../../../protocol/schema.zig");
const action_ops = @import("../../../protocol/action_ops.zig");
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

/// Runs an action's detail shows.
const RECENT_RUNS: u32 = 20;
/// The most runs one page of `/runs` shows: each is asked its status, a
/// read on the dashboard's one thread.
const MAX_RUNS_PAGE: u64 = 100;

/// `action_list`'s entry: `[key_len:u16][key][value_len:u32 = 0]`.
const action_columns = [_]output.WireColumn{
    .{ .field = "name", .header = "NAME", .field_type = .str_u16 },
    .{ .field = "value", .header = "", .field_type = .str_u32 },
};

const ActionLister = struct {
    namespace: []const u8,
    pub fn ask(self: ActionLister, client: *client_mod.Client, cursor: ?[]const u8) !client_mod.Response {
        return client_mod.action.list(client, self.namespace, null, cursor);
    }
};

/// The fields no op answers about an action yet, as null.
fn writeUnknownMetadata(obj: anytype) !void {
    try obj.nullField("type");
    try obj.nullField("owner");
    try obj.stringField("description", "");
    try obj.nullField("version");
    try obj.nullField("enabled");
    try obj.nullField("timeout_ms");
    try obj.nullField("max_retries");
}

/// GET /actions — The namespace's actions (`action_list`). The list
/// answers names only: metadata, run counts and latency are null.
pub fn getActions(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "action list", err);
    defer reader.deinit();
    var pages = reader.listAll(allocator, &action_columns, ActionLister{ .namespace = ns }) catch |err|
        return try loopback.readFailed(allocator, "action list", err);
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
        _ = r.readLengthPrefixed(u32) orelse break;
        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", name);
        try obj.stringField("namespace", ns);
        try writeUnknownMetadata(&obj);
        try obj.nullField("created_at");
        try obj.nullField("updated_at");
        try obj.nullField("latency");
        try obj.nullField("runs");
        try obj.end();
    }
    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// One run as `action_list_runs` lists it.
const ListedRun = struct {
    run_id: []const u8,
    action: []const u8,
    status: u8,
    created_at: i64,
    started_at: ?i64,
    completed_at: ?i64,

    fn statusName(self: ListedRun) []const u8 {
        const s = std.enums.fromInt(action_ops.RunStatus, self.status) orelse return "unknown";
        return @tagName(s);
    }
};

/// `action_list_runs`'s answer: [count:u32] then per run [run_id_len:u16]
/// [run_id][action_len:u16][action][status:u8][created_at:i64]
/// [has:u8][started_at:i64]?[has:u8][completed_at:i64]?.
const RunList = struct {
    reader: WireReader,
    left: u32,

    fn init(data: []const u8) RunList {
        var r = WireReader.init(data);
        const count = r.readU32() orelse 0;
        return .{ .reader = r, .left = count };
    }

    fn next(self: *RunList) error{Malformed}!?ListedRun {
        if (self.left == 0) return null;
        self.left -= 1;
        const r = &self.reader;
        return .{
            .run_id = r.readLengthPrefixed(u16) orelse return error.Malformed,
            .action = r.readLengthPrefixed(u16) orelse return error.Malformed,
            .status = r.readU8() orelse return error.Malformed,
            .created_at = r.readI64() orelse return error.Malformed,
            .started_at = try optionalI64(r),
            .completed_at = try optionalI64(r),
        };
    }

    fn optionalI64(r: *WireReader) error{Malformed}!?i64 {
        const has = r.readU8() orelse return error.Malformed;
        if (has == 0) return null;
        return r.readI64() orelse error.Malformed;
    }

    /// Whether every run the count names is there whole.
    fn whole(data: []const u8) bool {
        var runs = RunList.init(data);
        while (runs.next() catch return false) |_| {}
        return true;
    }
};

const RunsShape = enum { recent, page };

/// One run, from its listing and its `action.status` (its attempt, worker,
/// output and error). What no op answers about a run yet — its input,
/// source and caller — is null.
fn writeRun(reader: *loopback.Reader, writer: anytype, namespace: []const u8, run: ListedRun, shape: RunsShape) !void {
    var got = try reader.call(action_ops.op("action.status"), namespace, run.run_id, .{});
    defer got.deinit();
    var diag: schema.Diagnostic = .{};
    // Gone since it was listed, or refused: its listing alone.
    const st = if (got.isError()) null else action_ops.op("action.status").response.decodeAll(got.data, &diag) catch null;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("run_id", run.run_id);
    if (shape == .page) try obj.stringField("action", run.action);
    try obj.stringField("status", run.statusName());
    if (shape == .recent) {
        if (st) |s| try obj.intField("attempt", @as(i64, s.attempt)) else try obj.nullField("attempt");
    }
    try obj.intField("created_at", run.created_at);
    if (run.started_at) |t| try obj.intField("started_at", t) else try obj.nullField("started_at");
    if (run.completed_at) |t| try obj.intField("completed_at", t) else try obj.nullField("completed_at");
    const s = st orelse {
        for ([_][]const u8{ "worker_id", "error", "outcome", "input", "output", "source", "caller_run_id", "caller_workflow" }) |f| try obj.nullField(f);
        return obj.end();
    };
    if (s.worker_id) |w| try obj.stringField("worker_id", w) else try obj.nullField("worker_id");
    if (s.error_message) |e| try obj.stringField("error", e) else try obj.nullField("error");
    if (s.output) |o| try obj.stringField("outcome", o) else try obj.nullField("outcome");
    try obj.nullField("input");
    // Cut to 2 KiB for the overview.
    if (s.output) |o| try obj.stringField("output", o[0..@min(o.len, 2048)]) else try obj.nullField("output");
    try obj.nullField("source");
    try obj.nullField("caller_run_id");
    try obj.nullField("caller_workflow");
    try obj.end();
}

/// GET /actions/:name — Action detail. No op answers an action's metadata,
/// run counts or latency yet, so those are null. Its recent runs come from
/// `action_list_runs` on the action's shard; its workers from `worker.list`.
pub fn getActionDetail(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "action detail", err);
    defer reader.deinit();
    const client = reader.next() catch |err| return try loopback.readFailed(allocator, "action runs", err);
    var listed = client_mod.action.listRuns(client, ns, name, RECENT_RUNS) catch |err|
        return try loopback.readFailed(allocator, "action runs", err);
    defer listed.deinit();
    if (listed.isError()) return try h.jsonError(allocator, listed.errorMessage());
    if (!RunList.whole(listed.data)) return try h.jsonError(allocator, "action runs: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    defer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);
    try obj.stringField("namespace", ns);
    try writeUnknownMetadata(&obj);
    try obj.nullField("retry_delay_ms");
    try obj.nullField("created_at");
    try obj.nullField("updated_at");
    try obj.nullField("latency");
    try obj.nullField("runs");
    {
        var recent_arr = try obj.arrayField("recent_runs");
        try recent_arr.begin();
        var runs = RunList.init(listed.data);
        while (runs.next() catch unreachable) |run| {
            try recent_arr.next();
            writeRun(&reader, writer, ns, run, .recent) catch |err| return try loopback.readFailed(allocator, "action run status", err);
        }
        try recent_arr.end();
    }
    {
        var workers_arr = try obj.arrayField("workers");
        try workers_arr.begin();
        worker_api.writeActionWorkers(&reader, &workers_arr, writer, ns, name) catch |err|
            return try loopback.readFailed(allocator, "action workers", err);
        try workers_arr.end();
    }
    try obj.end();
    return try allocator.dupe(u8, json_aw.written());
}

/// GET /actions/:name/runs — Execution history (`action_list_runs`, on the
/// action's shard). The op takes a count and no offset, so a page past
/// `offset` asks for `offset + limit` runs and skips the first `offset`.
pub fn getActionRuns(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const limit = @min(h.parseQueryParam(u64, query_string, "limit") orelse 100, MAX_RUNS_PAGE);
    const offset = h.parseQueryParam(u64, query_string, "offset") orelse 0;
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const want: u32 = @intCast(@min(offset +| limit, std.math.maxInt(u32)));

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "action runs", err);
    defer reader.deinit();
    const client = reader.next() catch |err| return try loopback.readFailed(allocator, "action runs", err);
    var listed = client_mod.action.listRuns(client, ns, name, want) catch |err|
        return try loopback.readFailed(allocator, "action runs", err);
    defer listed.deinit();
    if (listed.isError()) return try h.jsonError(allocator, listed.errorMessage());
    if (!RunList.whole(listed.data)) return try h.jsonError(allocator, "action runs: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    defer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();
    var runs = RunList.init(listed.data);
    var seen: u64 = 0;
    while (runs.next() catch unreachable) |run| {
        seen += 1;
        if (seen <= offset) continue;
        try arr.next();
        writeRun(&reader, writer, ns, run, .page) catch |err| return try loopback.readFailed(allocator, "action run status", err);
    }
    try arr.end();
    return try allocator.dupe(u8, json_aw.written());
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
    var resp = client_mod.action.invoke(&client, ns_q, name, input, null) catch
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

/// A context whose loopback connect is refused: nothing listens on port 1.
fn unreachableContext(allocator: Allocator, metrics: *h.MetricsRegistry) DashboardContext {
    var ctx = DashboardContext.init(allocator, metrics, 1);
    ctx.listen_port = 1;
    return ctx;
}

test "action reads say they failed when the node can't be reached" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = unreachableContext(allocator, &metrics);

    const list = try getActions(allocator, null, &ctx);
    defer allocator.free(list);
    try std.testing.expect(std.mem.startsWith(u8, list, "{\"error\":\"action list"));
    const detail = try getActionDetail(allocator, "my_action", null, &ctx);
    defer allocator.free(detail);
    try std.testing.expect(std.mem.startsWith(u8, detail, "{\"error\":\"action detail"));
    const runs = try getActionRuns(allocator, "my_action", "offset=5", &ctx);
    defer allocator.free(runs);
    try std.testing.expect(std.mem.startsWith(u8, runs, "{\"error\":\"action runs"));
}

test "action_list_runs' answer decodes run by run, and a cut one is refused" {
    // One run: "r1" of "a", completed (2), created at 5, started at 6, not completed.
    const answer = [_]u8{ 1, 0, 0, 0, 2, 0, 'r', '1', 1, 0, 'a', 2 } ++ [_]u8{ 5, 0, 0, 0, 0, 0, 0, 0 } ++ [_]u8{1} ++ [_]u8{ 6, 0, 0, 0, 0, 0, 0, 0 } ++ [_]u8{0};
    try std.testing.expect(RunList.whole(&answer));
    var runs = RunList.init(&answer);
    const run = (try runs.next()).?;
    try std.testing.expectEqualStrings("r1", run.run_id);
    try std.testing.expectEqualStrings("completed", run.statusName());
    try std.testing.expectEqual(@as(?i64, 6), run.started_at);
    try std.testing.expectEqual(@as(?i64, null), run.completed_at);
    try std.testing.expect((try runs.next()) == null);
    try std.testing.expect(!RunList.whole(answer[0 .. answer.len - 1]));
}
