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
const RunStatus = @import("../../../workflow/types.zig").RunStatus;
const wf_parser = @import("../../../workflow/parser.zig");
const wf_definition = @import("../../../workflow/definition.zig");
const client_mod = @import("../../../cli/client/mod.zig");
const loopback = @import("loopback.zig");
const cli_output = @import("../../../cli/output.zig");
const WireReader = @import("../../../util/wire.zig").WireReader;

// ── Helpers ──

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

/// Router for /workflow/* requests
pub fn handleWorkflowRequest(allocator: Allocator, method: Method, path: []const u8, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    // /workflow/definitions
    if (std.mem.eql(u8, path, "/definitions")) {
        return switch (method) {
            .GET => listDefinitions(allocator, query_string, ctx),
            .POST => createDefinition(allocator, query_string, body, ctx),
            else => error.MethodNotAllowed,
        };
    }

    // /workflow/runs (exact match)
    if (std.mem.eql(u8, path, "/runs")) {
        return switch (method) {
            .GET => listRuns(allocator, query_string, ctx),
            .POST => startRun(allocator, query_string, body, ctx),
            else => error.MethodNotAllowed,
        };
    }

    // /workflow/definitions/:name[/enable|/disable]
    if (std.mem.startsWith(u8, path, "/definitions/")) {
        const rest = path["/definitions/".len..];
        if (std.mem.endsWith(u8, rest, "/enable")) {
            if (method != .PUT) return error.MethodNotAllowed;
            const name = rest[0 .. rest.len - "/enable".len];
            return enableWorkflow(allocator, name, query_string, ctx);
        }
        if (std.mem.endsWith(u8, rest, "/disable")) {
            if (method != .PUT) return error.MethodNotAllowed;
            const name = rest[0 .. rest.len - "/disable".len];
            return disableWorkflow(allocator, name, query_string, ctx);
        }
        // /workflow/definitions/:name
        if (method != .GET) return error.MethodNotAllowed;
        return getDefinition(allocator, rest, query_string, ctx);
    }

    // /workflow/runs/:id[/history|/signal]
    if (std.mem.startsWith(u8, path, "/runs/")) {
        const rest = path["/runs/".len..];
        if (std.mem.endsWith(u8, rest, "/history")) {
            if (method != .GET) return error.MethodNotAllowed;
            const run_id = rest[0 .. rest.len - "/history".len];
            return getRunHistory(allocator, run_id, query_string, ctx);
        }
        if (std.mem.endsWith(u8, rest, "/signal")) {
            if (method != .POST) return error.MethodNotAllowed;
            const run_id = rest[0 .. rest.len - "/signal".len];
            return signalRun(allocator, run_id, query_string, body, ctx);
        }
        // DELETE /workflow/runs/:id or GET /workflow/runs/:id
        return switch (method) {
            .DELETE => cancelRun(allocator, rest, query_string, ctx),
            .GET => getRunStatus(allocator, rest, query_string, ctx),
            else => error.MethodNotAllowed,
        };
    }

    return h.jsonError(allocator, "Not found");
}

// ---------------------------------------------------------------------------
// Definitions
// ---------------------------------------------------------------------------

/// A `workflow_list_definitions` entry: [name:u16][version:u16][created_at:i64].
const definition_columns: []const cli_output.WireColumn = &.{
    .{ .field = "name", .header = "", .field_type = .str_u16 },
    .{ .field = "version", .header = "", .field_type = .str_u16 },
    .{ .field = "created_at", .header = "", .field_type = .int_i64 },
};

/// Asks for one page of a namespace's definitions, for `Reader.listAll`.
const DefinitionsAsker = struct {
    namespace: []const u8,

    pub fn ask(self: DefinitionsAsker, client: *client_mod.Client, cursor: ?[]const u8) !client_mod.Response {
        return client_mod.workflow.listDefinitions(client, self.namespace, null, cursor);
    }
};

const Definition = struct { name: []const u8, version: []const u8, created_at: i64 };

/// The definitions of a `[count:u32]entries` list, as `listAll` gathers it.
const Definitions = struct {
    reader: WireReader,
    left: u32,

    fn init(list: []const u8) Definitions {
        var reader = WireReader.init(list);
        const count = reader.readU32() orelse 0;
        return .{ .reader = reader, .left = count };
    }

    fn next(self: *Definitions) error{Truncated}!?Definition {
        if (self.left == 0) return null;
        self.left -= 1;
        const name = self.reader.readLengthPrefixed(u16) orelse return error.Truncated;
        const version = self.reader.readLengthPrefixed(u16) orelse return error.Truncated;
        const created_at = self.reader.readI64() orelse return error.Truncated;
        return .{ .name = name, .version = version, .created_at = created_at };
    }
};

/// Definitions whose YAML one list request fetches for its step metadata:
/// each is a read on the dashboard's one thread. The rest show null
/// metadata until the list entry carries it.
const DEFINITIONS_WITH_METADATA: usize = 100;

/// GET /workflow/definitions?namespace= (and /workflows) — a namespace's
/// definitions, with the step metadata of each one's YAML. The dashboard asks
/// each shard, as any client does: the list walk for name, version and
/// created_at, then the YAML of the first `DEFINITIONS_WITH_METADATA`. No op
/// says whether one is enabled, so `enabled` is null.
fn listDefinitions(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "workflow definitions", err);
    defer reader.deinit();
    var pages = reader.listAll(allocator, definition_columns, DefinitionsAsker{ .namespace = ns }) catch |err|
        return try loopback.readFailed(allocator, "workflow definitions", err);
    defer pages.deinit();

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    var defs = Definitions.init(pages.list());
    var fetched: usize = 0;
    while (defs.next() catch return try h.jsonError(allocator, "workflow definitions: malformed answer")) |d| {
        if (fetched == DEFINITIONS_WITH_METADATA) {
            try arr.next();
            var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try obj.begin();
            try obj.stringField("name", d.name);
            try obj.stringField("version", d.version);
            try obj.intField("created_at", d.created_at);
            try obj.nullField("enabled");
            for ([_][]const u8{ "step_count", "plan_count", "has_schedule", "has_trigger", "start_step", "terminals", "steps" }) |f| try obj.nullField(f);
            try obj.end();
            continue;
        }
        fetched += 1;
        var got = client_mod.workflow.getDefinition(reader.next() catch |err| return try loopback.readFailed(allocator, "workflow definition", err), ns, d.name, null) catch |err|
            return try loopback.readFailed(allocator, "workflow definition", err);
        defer got.deinit();
        // Deleted since the list was read.
        if (got.isNotFound()) continue;
        if (got.isError()) return try h.jsonError(allocator, got.errorMessage());

        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", d.name);
        try obj.stringField("version", d.version);
        try obj.intField("created_at", d.created_at);
        try obj.nullField("enabled");
        if (wf_parser.parseWorkflow(allocator, got.data, null)) |parsed| {
            var def = parsed;
            defer def.deinit(allocator);
            try def.writeJsonMeta(&obj);
        } else |_| {
            try wf_definition.WorkflowDefinition.writeJsonMetaEmpty(&obj);
        }
        try obj.end();
    }

    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// Extract a top-level `name:` value from a YAML/JSON workflow definition, used
/// only for hash-routing the create to the owning shard (the server re-parses
/// and validates the full definition). Returns "" if not found.
fn extractWorkflowName(content: []const u8) []const u8 {
    var line_it = std.mem.splitScalar(u8, content, '\n');
    while (line_it.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        const key = if (std.mem.startsWith(u8, trimmed, "name:"))
            trimmed["name:".len..]
        else if (std.mem.startsWith(u8, trimmed, "\"name\":"))
            trimmed["\"name\":".len..]
        else
            continue;
        return std.mem.trim(u8, key, " \t\r\"',");
    }
    return "";
}

/// POST /workflow/definitions?namespace= — create/define a workflow (loopback
/// write). Body is the YAML/JSON definition.
fn createDefinition(allocator: Allocator, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    if (body.len == 0) return try h.jsonError(allocator, "Empty definition body");
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    const wf_name = extractWorkflowName(body);
    if (wf_name.len == 0) return try h.jsonError(allocator, "definition is missing a top-level name");

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.workflow.create(&client, ns_q, wf_name, body) catch
        return try h.jsonError(allocator, "Workflow create failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("name", wf_name);
    try obj.stringField("status", "created");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /workflow/definitions/:name?namespace= — one definition's YAML, with
/// its version and created_at from the list walk. The dashboard asks each
/// shard, as any client does. No op says whether it is enabled or how many
/// runs it has had, so `enabled`, `status` and `run_count` are null.
fn getDefinition(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "workflow definition", err);
    defer reader.deinit();
    var got = client_mod.workflow.getDefinition(reader.next() catch |err| return try loopback.readFailed(allocator, "workflow definition", err), ns, name, null) catch |err|
        return try loopback.readFailed(allocator, "workflow definition", err);
    defer got.deinit();
    if (got.isError() and !got.isNotFound()) return try h.jsonError(allocator, got.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);

    if (got.isNotFound()) {
        try obj.stringField("status", "unknown");
    } else {
        var pages = reader.listAll(allocator, definition_columns, DefinitionsAsker{ .namespace = ns }) catch |err|
            return try loopback.readFailed(allocator, "workflow definitions", err);
        defer pages.deinit();
        var listed: ?Definition = null;
        var defs = Definitions.init(pages.list());
        while (defs.next() catch return try h.jsonError(allocator, "workflow definitions: malformed answer")) |d| {
            if (std.mem.eql(u8, d.name, name)) {
                listed = d;
                break;
            }
        }
        if (listed) |d| {
            try obj.stringField("version", d.version);
            try obj.intField("created_at", d.created_at);
        } else {
            try obj.nullField("version");
            try obj.nullField("created_at");
        }
        try obj.nullField("status");
        try obj.stringField("definition_yaml", got.data);
    }
    try obj.nullField("enabled");
    try obj.nullField("run_count");

    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// PUT /workflow/definitions/:name/enable?namespace=&version= (loopback write)
fn enableWorkflow(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const version = h.parseQueryParam([]const u8, query_string, "version");

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.workflow.enable(&client, ns_q, name, version) catch
        return try h.jsonError(allocator, "Enable failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("name", name);
    try obj.boolField("enabled", true);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// PUT /workflow/definitions/:name/disable?namespace=&version= (loopback write)
fn disableWorkflow(allocator: Allocator, name: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const version = h.parseQueryParam([]const u8, query_string, "version");

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.workflow.disable(&client, ns_q, name, version) catch
        return try h.jsonError(allocator, "Disable failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("name", name);
    try obj.boolField("enabled", false);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

/// One read through the loopback client, its wait what is left of the request's time.
fn ask(reader: *loopback.Reader, comptime f: anytype, args: anytype) !client_mod.Response {
    return @call(.auto, f, .{try reader.next()} ++ args);
}

/// A run's version, or null for "latest": the run doesn't say which
/// version that was.
fn knownVersion(version: []const u8) ?[]const u8 {
    return if (std.mem.eql(u8, version, "latest")) null else version;
}

/// A run's input or output: the JSON itself when it parses, else the text
/// as a string. A run takes any bytes as input, and written raw they could
/// break the answer or add fields to it.
fn jsonOrString(allocator: Allocator, obj: anytype, writer: *std.Io.Writer, name: []const u8, value: ?[]const u8) !void {
    const v = value orelse return obj.nullField(name);
    if (std.json.validate(allocator, v) catch false) {
        try obj.field(name);
        try writer.writeAll(v);
    } else try obj.stringField(name, v);
}

/// Rows of one run list given their version, step, times and wait, from a
/// `workflow_status` each: every one is a read on the dashboard's one
/// thread. Later rows show those fields as null.
const RUNS_WITH_STATUS: usize = 100;

const ListedRun = struct { run_id: []const u8, workflow: []const u8, status: []const u8, created_at: i64 };

/// The rows of a `workflow_list_runs` answer:
/// [count:u32]([run_id:u16][workflow:u16][status:u16][created_at:i64])*, then
/// [has_more:u8][cursor_len:u16][cursor] (`trailer`, once the rows are read).
const ListedRuns = struct {
    reader: WireReader,
    left: u32,

    fn init(answer: []const u8) ListedRuns {
        var reader = WireReader.init(answer);
        const count = reader.readU32() orelse 0;
        return .{ .reader = reader, .left = count };
    }

    fn next(self: *ListedRuns) error{Truncated}!?ListedRun {
        if (self.left == 0) return null;
        self.left -= 1;
        const run_id = self.reader.readLengthPrefixed(u16) orelse return error.Truncated;
        const workflow = self.reader.readLengthPrefixed(u16) orelse return error.Truncated;
        const status = self.reader.readLengthPrefixed(u16) orelse return error.Truncated;
        const created_at = self.reader.readI64() orelse return error.Truncated;
        return .{ .run_id = run_id, .workflow = workflow, .status = status, .created_at = created_at };
    }

    /// The cursor of the page after this one, or null on the last.
    fn trailer(self: *ListedRuns) error{Truncated}!?[]const u8 {
        std.debug.assert(self.left == 0);
        const more = (self.reader.readU8() orelse return error.Truncated) != 0;
        const cursor = self.reader.readLengthPrefixed(u16) orelse return error.Truncated;
        return if (more and cursor.len > 0) cursor else null;
    }
};

/// The longest run-list cursor the server sends, and its hex form in a URL.
const MAX_RUN_CURSOR = @import("../../../workflow/list_runs.zig").MAX_CURSOR;

/// A runs page's cursor as the dashboard hands it out: the server's own,
/// in hex, so it travels in a URL as it is. The server's is a created_at
/// (8 bytes) and a run id, so a shorter one was never sent.
fn decodeRunCursor(buf: *[MAX_RUN_CURSOR]u8, hex: []const u8) error{BadCursor}![]const u8 {
    if (hex.len % 2 != 0 or hex.len / 2 <= 8 or hex.len / 2 > buf.len) return error.BadCursor;
    return std.fmt.hexToBytes(buf[0 .. hex.len / 2], hex) catch error.BadCursor;
}

/// A `workflow_status` answer: [run_id:u16][workflow:u16][version:u16]
/// [status:u8][current_step:u16][input:u32][created_at:i64]
/// [has:u8][started_at:i64]? [has:u8][completed_at:i64]?
/// [has:u8][wait_signal:u16]? [has:u8][output:u32]?
const RunState = struct {
    run_id: []const u8,
    workflow: []const u8,
    version: []const u8,
    status: []const u8,
    current_step: []const u8,
    input: []const u8,
    created_at: i64,
    started_at: ?i64,
    completed_at: ?i64,
    wait_signal: ?[]const u8,
    output: ?[]const u8,

    fn parse(answer: []const u8) ?RunState {
        var r = WireReader.init(answer);
        var run: RunState = undefined;
        run.run_id = r.readLengthPrefixed(u16) orelse return null;
        run.workflow = r.readLengthPrefixed(u16) orelse return null;
        run.version = r.readLengthPrefixed(u16) orelse return null;
        const status = std.enums.fromInt(RunStatus, r.readU8() orelse return null) orelse return null;
        run.status = status.toString();
        run.current_step = r.readLengthPrefixed(u16) orelse return null;
        run.input = r.readLengthPrefixed(u32) orelse return null;
        run.created_at = r.readI64() orelse return null;
        run.started_at = if ((r.readU8() orelse return null) != 0) r.readI64() orelse return null else null;
        run.completed_at = if ((r.readU8() orelse return null) != 0) r.readI64() orelse return null else null;
        run.wait_signal = if ((r.readU8() orelse return null) != 0) r.readLengthPrefixed(u16) orelse return null else null;
        run.output = if ((r.readU8() orelse return null) != 0) r.readLengthPrefixed(u32) orelse return null else null;
        return run;
    }
};

/// A wait on an action's result is an internal signal; it shows as "action".
fn waitType(signal: []const u8) []const u8 {
    return if (std.mem.startsWith(u8, signal, "_action_done:")) "action" else signal;
}

const HistoryEvent = struct { event_type: []const u8, detail: []const u8, timestamp: i64 };

/// The events of a `workflow_history` answer:
/// [count:u32]([type:u16][detail:u16][timestamp:i64])*[has_more:u8][cursor_len:u16].
const HistoryEvents = struct {
    reader: WireReader,
    left: u32,

    fn init(answer: []const u8) HistoryEvents {
        var reader = WireReader.init(answer);
        const count = reader.readU32() orelse 0;
        return .{ .reader = reader, .left = count };
    }

    fn next(self: *HistoryEvents) error{Truncated}!?HistoryEvent {
        if (self.left == 0) return null;
        self.left -= 1;
        const event_type = self.reader.readLengthPrefixed(u16) orelse return error.Truncated;
        const detail = self.reader.readLengthPrefixed(u16) orelse return error.Truncated;
        const timestamp = self.reader.readI64() orelse return error.Truncated;
        return .{ .event_type = event_type, .detail = detail, .timestamp = timestamp };
    }

    /// Whether the server held events back; read once every event is.
    fn hasMore(self: *HistoryEvents) error{Truncated}!bool {
        return (self.reader.readU8() orelse return error.Truncated) != 0;
    }
};

/// What a run's whole history says of it: how it was started, how many
/// events it has, and the error it failed with.
const HistorySummary = struct {
    triggered_by: ?[]const u8 = null,
    event_count: ?i64 = null,
    error_message: ?[]const u8 = null,

    fn of(answer: []const u8) error{Truncated}!HistorySummary {
        var events = HistoryEvents.init(answer);
        var summary: HistorySummary = .{};
        var count: i64 = 0;
        var last: ?HistoryEvent = null;
        while (try events.next()) |e| {
            if (count == 0) summary.triggered_by = if (std.mem.eql(u8, e.event_type, "schedule_started"))
                "schedule"
            else if (std.mem.eql(u8, e.event_type, "trigger_started"))
                "stream"
            else
                "manual";
            count += 1;
            last = e;
        }
        if (try events.hasMore()) return summary;
        summary.event_count = count;
        if (last) |e| {
            if (std.mem.eql(u8, e.event_type, "workflow_failed") or std.mem.eql(u8, e.event_type, "step_failed"))
                summary.error_message = e.detail;
        }
        return summary;
    }
};

fn optionalInt(obj: anytype, name: []const u8, value: ?i64) !void {
    if (value) |v| try obj.intField(name, v) else try obj.nullField(name);
}

fn optionalString(obj: anytype, name: []const u8, value: ?[]const u8) !void {
    if (value) |v| try obj.stringField(name, v) else try obj.nullField(name);
}

/// GET /workflow/runs?namespace=&workflow=&status=&search=&cursor=&limit= —
/// one page of a namespace's runs, newest first. The dashboard asks each
/// shard, as any client does: the run list for id, workflow, status and
/// start, then a `workflow_status` for each of the first `RUNS_WITH_STATUS`
/// rows. The answer is `{runs, has_more, next_cursor}`: a page can end short
/// of `limit` with more after it, so only `has_more` says the list is whole.
/// `next_cursor` is the server's cursor in hex; a `?cursor=` that isn't one
/// is a 400. No op answers triggered_by, error, parent_run_id, terminal_name
/// or history_event_count per row, so they are null.
fn listRuns(allocator: Allocator, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const workflow = h.parseQueryParam([]const u8, query_string, "workflow") orelse "";
    const status_filter = h.parseQueryParam([]const u8, query_string, "status");
    const search = h.parseQueryParam([]const u8, query_string, "search");
    var cursor_buf: [MAX_RUN_CURSOR]u8 = undefined;
    const cursor: ?[]const u8 = if (h.parseQueryParam([]const u8, query_string, "cursor")) |hex| try decodeRunCursor(&cursor_buf, hex) else null;
    const limit: u32 = @intCast(@min(h.parseQueryParam(u64, query_string, "limit") orelse 100, std.math.maxInt(u32)));

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "workflow runs", err);
    defer reader.deinit();
    var got = ask(&reader, client_mod.workflow.listRuns, .{ ns, workflow, limit, status_filter, cursor, search }) catch |err|
        return try loopback.readFailed(allocator, "workflow runs", err);
    defer got.deinit();
    // A namespace never created holds no runs, as an empty one doesn't.
    if (got.isNotFound()) return try allocator.dupe(u8, "{\"runs\":[],\"has_more\":false,\"next_cursor\":null}");
    if (got.isError()) return try h.jsonError(allocator, got.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var page = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try page.begin();
    var arr = try page.arrayField("runs");
    try arr.begin();

    var rows = ListedRuns.init(got.data);
    var asked: usize = 0;
    while (rows.next() catch return try h.jsonError(allocator, "workflow runs: malformed answer")) |row| {
        var state_got: ?client_mod.Response = null;
        defer if (state_got) |*s| s.deinit();
        var state: ?RunState = null;
        if (asked < RUNS_WITH_STATUS) {
            asked += 1;
            state_got = ask(&reader, client_mod.workflow.status, .{ ns, row.run_id }) catch |err|
                return try loopback.readFailed(allocator, "workflow run status", err);
            const s = state_got.?;
            // Not found: removed since the list was read; its row keeps what the list said.
            if (!s.isError()) {
                state = RunState.parse(s.data) orelse return try h.jsonError(allocator, "workflow run status: malformed answer");
            } else if (!s.isNotFound()) {
                return try h.jsonError(allocator, s.errorMessage());
            }
        }

        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("run_id", row.run_id);
        try obj.stringField("workflow", row.workflow);
        try optionalString(&obj, "version", if (state) |st| knownVersion(st.version) else null);
        try obj.stringField("status", row.status);
        try obj.nullField("triggered_by");
        try optionalString(&obj, "current_step", if (state) |st| st.current_step else null);
        // The list's created_at is when the run started, or was made if it hasn't.
        try obj.intField("started_at", row.created_at);
        const completed_at: ?i64 = if (state) |st| st.completed_at else null;
        try optionalInt(&obj, "completed_at", completed_at);
        try optionalInt(&obj, "duration_ms", if (completed_at) |t| t - row.created_at else null);
        try optionalString(&obj, "wait_type", if (state) |st| (if (st.wait_signal) |w| waitType(w) else null) else null);
        for ([_][]const u8{ "parent_run_id", "terminal_name", "error", "history_event_count" }) |f| try obj.nullField(f);
        try obj.end();
    }
    try arr.end();

    const next = rows.trailer() catch return try h.jsonError(allocator, "workflow runs: malformed answer");
    try page.boolField("has_more", next != null);
    if (next) |c| {
        var hex_buf: [2 * MAX_RUN_CURSOR]u8 = undefined;
        const hex = std.fmt.bufPrint(&hex_buf, "{x}", .{c}) catch unreachable; // sized for the longest
        try page.stringField("next_cursor", hex);
    } else try page.nullField("next_cursor");
    try page.end();
    return try json_aw.toOwnedSlice();
}

/// POST /workflow/runs?namespace=&workflow=&version= (loopback write). The request
/// body is the input JSON; workflow name + version come from the query string (so
/// the dashboard thread doesn't have to parse JSON). Response carries the run_id.
fn startRun(allocator: Allocator, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const workflow = h.parseQueryParam([]const u8, query_string, "workflow") orelse
        return try h.jsonError(allocator, "Missing ?workflow=");
    const version = h.parseQueryParam([]const u8, query_string, "version") orelse "latest";
    const input = if (body.len > 0) body else "{}";

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.workflow.start(&client, ns_q, workflow, version, input, null, null) catch
        return try h.jsonError(allocator, "Workflow start failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    const run_id = resp.asRawData() orelse "";

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("run_id", run_id);
    try obj.stringField("workflow", workflow);
    try obj.stringField("status", "started");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /workflow/runs/:id?namespace= (and /workflows/:id) — one run. The
/// dashboard asks each shard, as any client does: `workflow_status` for the
/// run, and its `workflow_history` for how it was started, its event count
/// and the error it failed with. No op answers step results, pending signals
/// or search attributes, so `step_results`, `pending_signals` and
/// `search_attributes` are null.
fn getRunStatus(allocator: Allocator, run_id: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "workflow run status", err);
    defer reader.deinit();
    var got = ask(&reader, client_mod.workflow.status, .{ ns, run_id }) catch |err|
        return try loopback.readFailed(allocator, "workflow run status", err);
    defer got.deinit();
    if (got.isNotFound()) return error.NotFound;
    if (got.isError()) return try h.jsonError(allocator, got.errorMessage());
    const run = RunState.parse(got.data) orelse return try h.jsonError(allocator, "workflow run status: malformed answer");

    var hist = ask(&reader, client_mod.workflow.history, .{ ns, run_id, std.math.maxInt(u32) }) catch |err|
        return try loopback.readFailed(allocator, "workflow run history", err);
    defer hist.deinit();
    // Not found: removed since its status was read; the history fields stay null.
    if (hist.isError() and !hist.isNotFound()) return try h.jsonError(allocator, hist.errorMessage());
    const summary: HistorySummary = if (hist.isError())
        .{}
    else
        HistorySummary.of(hist.data) catch return try h.jsonError(allocator, "workflow run history: malformed answer");

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("run_id", run.run_id);
    try obj.stringField("workflow", run.workflow);
    try optionalString(&obj, "version", knownVersion(run.version));
    try obj.stringField("status", run.status);
    try obj.stringField("current_step", run.current_step);
    try optionalInt(&obj, "started_at_ms", run.started_at);
    try optionalInt(&obj, "completed_at", run.completed_at);
    try optionalInt(&obj, "updated_at_ms", run.completed_at);
    try optionalInt(&obj, "duration_ms", if (run.completed_at) |t| t - (run.started_at orelse run.created_at) else null);
    try optionalString(&obj, "error_message", summary.error_message);
    try jsonOrString(allocator, &obj, writer, "input", if (run.input.len > 0) run.input else null);
    try jsonOrString(allocator, &obj, writer, "output", run.output);
    try obj.nullField("step_results");
    try optionalString(&obj, "triggered_by", summary.triggered_by);
    try obj.nullField("search_attributes");
    try obj.nullField("pending_signals");
    try optionalInt(&obj, "history_event_count", summary.event_count);
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// GET /workflow/runs/:id/history?namespace=&limit= (and /workflows/:id/history)
/// — a run's events, oldest first. The dashboard asks each shard, as any
/// client does.
fn getRunHistory(allocator: Allocator, run_id: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const limit: u32 = @intCast(@min(h.parseQueryParam(u64, query_string, "limit") orelse std.math.maxInt(u32), std.math.maxInt(u32)));

    var reader = loopback.Reader.open(allocator, ctx) catch |err| return try loopback.readFailed(allocator, "workflow run history", err);
    defer reader.deinit();
    var got = ask(&reader, client_mod.workflow.history, .{ ns, run_id, limit }) catch |err|
        return try loopback.readFailed(allocator, "workflow run history", err);
    defer got.deinit();
    if (got.isNotFound()) return error.NotFound;
    if (got.isError()) return try h.jsonError(allocator, got.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();
    var events = HistoryEvents.init(got.data);
    while (events.next() catch return try h.jsonError(allocator, "workflow run history: malformed answer")) |e| {
        try arr.next();
        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("event_type", e.event_type);
        try obj.stringField("step_name", e.detail);
        try obj.intField("timestamp", e.timestamp);
        try obj.end();
    }
    try arr.end();
    return try json_aw.toOwnedSlice();
}

/// DELETE /workflow/runs/:id?namespace=&reason= (loopback write)
fn cancelRun(allocator: Allocator, run_id: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const reason = h.parseQueryParam([]const u8, query_string, "reason");

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.workflow.cancel(&client, ns_q, run_id, reason) catch
        return try h.jsonError(allocator, "Cancel failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("run_id", run_id);
    try obj.stringField("status", "cancelled");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

/// POST /workflow/runs/:id/signal?namespace=&signal=<type> — deliver a signal
/// to a running workflow (loopback write). Body is the signal payload.
fn signalRun(allocator: Allocator, run_id: []const u8, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    const ns_q = h.parseQueryParam([]const u8, query_string, "namespace") orelse "default";
    const signal_type = h.parseQueryParam([]const u8, query_string, "signal") orelse
        h.parseQueryParam([]const u8, query_string, "type") orelse
        return try h.jsonError(allocator, "Missing ?signal=<type>");
    const payload: ?[]const u8 = if (body.len > 0) body else null;

    var client = loopbackConnect(allocator, ctx) catch return try h.jsonError(allocator, "Loopback connect failed");
    defer client.deinit();
    var resp = client_mod.workflow.signal(&client, ns_q, run_id, signal_type, payload) catch
        return try h.jsonError(allocator, "Signal failed");
    defer resp.deinit();
    if (resp.isError()) return try h.jsonError(allocator, resp.errorMessage());

    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.boolField("ok", true);
    try obj.stringField("run_id", run_id);
    try obj.stringField("signal", signal_type);
    try obj.stringField("status", "signaled");
    try obj.end();
    return try json_aw.toOwnedSlice();
}

// =============================================================================
// Tests
// =============================================================================

test "handleWorkflowRequest routes definitions to the list walk, which says when no node answers" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here: a connect would be refused

    const result = try handleWorkflowRequest(allocator, .GET, "/definitions", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "{\"error\":\"workflow definitions failed: "));
}

test "handleWorkflowRequest routes runs, a run and its history to their reads, which say when no node answers" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here: a connect would be refused

    const cases = [_]struct { path: []const u8, said: []const u8 }{
        .{ .path = "/runs", .said = "{\"error\":\"workflow runs failed: " },
        .{ .path = "/runs/run-1", .said = "{\"error\":\"workflow run status failed: " },
        .{ .path = "/runs/run-1/history", .said = "{\"error\":\"workflow run history failed: " },
    };
    for (cases) |c| {
        const result = try handleWorkflowRequest(allocator, .GET, c.path, "namespace=other", "", &ctx);
        defer allocator.free(result);
        try std.testing.expect(std.mem.startsWith(u8, result, c.said));
    }
}

test "RunState reads a workflow_status answer, its optional fields absent or present" {
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    for ([_][]const u8{ "r1", "wf", "1.0" }) |s| {
        try w.writeInt(u16, @intCast(s.len), .little);
        try w.writeAll(s);
    }
    try w.writeByte(@backingInt(RunStatus.waiting));
    try w.writeInt(u16, 4, .little);
    try w.writeAll("step");
    try w.writeInt(u32, 2, .little);
    try w.writeAll("{}");
    try w.writeInt(i64, 10, .little);
    try w.writeByte(1);
    try w.writeInt(i64, 20, .little);
    try w.writeByte(0);
    try w.writeByte(1);
    try w.writeInt(u16, 18, .little);
    try w.writeAll("_action_done:a-123");
    try w.writeByte(0);

    const run = RunState.parse(w.buffered()).?;
    try std.testing.expectEqualStrings("r1", run.run_id);
    try std.testing.expectEqualStrings("1.0", run.version);
    try std.testing.expectEqualStrings("waiting", run.status);
    try std.testing.expectEqualStrings("step", run.current_step);
    try std.testing.expectEqual(@as(?i64, 20), run.started_at);
    try std.testing.expectEqual(@as(?i64, null), run.completed_at);
    try std.testing.expectEqualStrings("action", waitType(run.wait_signal.?));
    try std.testing.expectEqual(@as(?[]const u8, null), run.output);
    // Cut short anywhere, it is no answer.
    try std.testing.expectEqual(@as(?RunState, null), RunState.parse(w.buffered()[0 .. w.buffered().len - 1]));
}

test "HistorySummary: how a run started, its event count, and the error it failed with" {
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try w.writeInt(u32, 2, .little);
    for ([_][2][]const u8{ .{ "schedule_started", "" }, .{ "step_failed", "boom" } }, 0..) |e, i| {
        try w.writeInt(u16, @intCast(e[0].len), .little);
        try w.writeAll(e[0]);
        try w.writeInt(u16, @intCast(e[1].len), .little);
        try w.writeAll(e[1]);
        try w.writeInt(i64, @intCast(i), .little);
    }
    try w.writeByte(0);
    try w.writeInt(u16, 0, .little);

    const summary = try HistorySummary.of(w.buffered());
    try std.testing.expectEqualStrings("schedule", summary.triggered_by.?);
    try std.testing.expectEqual(@as(?i64, 2), summary.event_count);
    try std.testing.expectEqualStrings("boom", summary.error_message.?);
}

test "handleWorkflowRequest routes a definition's detail to its get, which says when no node answers" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here: a connect would be refused

    const result = try handleWorkflowRequest(allocator, .GET, "/definitions/my-workflow", "namespace=other", "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "{\"error\":\"workflow definition failed: "));
}

test "handleWorkflowRequest cancel run surfaces loopback failure (no node in unit test)" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    ctx.listen_port = 1; // nothing listens here: a connect would be refused

    // cancel is now a real loopback write; with no node listening it must fail
    // gracefully with an error envelope rather than the old stub success.
    const result = try handleWorkflowRequest(allocator, .DELETE, "/runs/run-123", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}

test "a runs page's cursor travels as hex, and anything else is refused" {
    var buf: [MAX_RUN_CURSOR]u8 = undefined;
    try std.testing.expectEqualSlices(u8, &.{ 1, 0, 0, 0, 0, 0, 0, 0, 'r' }, try decodeRunCursor(&buf, "010000000000000072"));
    // No run id after the created_at, or not hex at all.
    for ([_][]const u8{ "", "01", "0100000000000000", "abc", "zz", "run-17" }) |bad| try std.testing.expectError(error.BadCursor, decodeRunCursor(&buf, bad));
    // Longer than any cursor the server sends.
    var long: [2 * MAX_RUN_CURSOR + 2]u8 = @splat('a');
    try std.testing.expectError(error.BadCursor, decodeRunCursor(&buf, &long));
}

test "a run's input is its JSON when it parses, else a string" {
    const allocator = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    var obj = json.ObjectBuilder(*std.Io.Writer).init(&aw.writer);
    try obj.begin();
    try jsonOrString(allocator, &obj, &aw.writer, "a", "{\"x\":1}");
    try jsonOrString(allocator, &obj, &aw.writer, "b", "not json\",\"evil\":\"1");
    try jsonOrString(allocator, &obj, &aw.writer, "c", null);
    try obj.end();
    try std.testing.expectEqualStrings("{\"a\":{\"x\":1},\"b\":\"not json\\\",\\\"evil\\\":\\\"1\",\"c\":null}", aw.written());
}
