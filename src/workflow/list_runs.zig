//! A workflow's runs, newest first, a page at a time. A list of every
//! workflow's runs asks each shard for its page (`system.runs_page`, a
//! gather); each answers from its own runs on its own thread, and the shard
//! that took the request merges the pages by start time and run id. A list
//! of one workflow's runs is read on the shard that holds them.
//!
//! The client's cursor is `[created_at:i64][run_id]`, the last run sent.

const std = @import("std");
const proto = @import("../protocol/proto.zig");
const ops = @import("../protocol/ops.zig");
const schema = @import("../protocol/schema.zig");
const system_ops = @import("../protocol/system_ops.zig");
const table_run = @import("../node/table_run.zig");
const gather_mod = @import("../node/gather.zig");
const Shard = @import("../node/shard.zig").Shard;
const Connection = @import("../node/connection.zig").Connection;
const WorkflowHandler = @import("handler.zig").WorkflowHandler;

const PageOp = table_run.op("system.runs_page");
const PageRequest = PageOp.request;
pub const PageResponse = PageOp.response;
const Runs = system_ops.Runs;
pub const RunAt = system_ops.RunAt;

/// The most runs one page holds, however many are asked for.
pub const MAX_LIMIT: u32 = system_ops.MAX_PAGE;
pub const DEFAULT_LIMIT: u32 = 100;

pub const MAX_CURSOR: usize = 8 + system_ops.MAX_RUN_ID;

/// What the client's answer holds besides its entries: the count, then
/// `[has_more:u8][cursor_len:u16][cursor]`.
const FRAME: usize = 4 + 1 + 2 + MAX_CURSOR;

/// The bytes a page's entries may take in the client's answer.
const ROOM: usize = ops.MAX_RESPONSE_BODY - FRAME;

/// What a list of runs asks for, checked.
pub const Query = struct {
    limit: u32,
    /// Empty for any status.
    status: []const u8,
    /// Lowercased; empty for no search.
    search: []const u8,
    after: ?RunAt.Value,
};

/// The run a client's cursor names; null for bytes this server never sent.
pub fn decodeCursor(cursor: []const u8) ?RunAt.Value {
    if (cursor.len <= 8 or cursor.len > MAX_CURSOR) return null;
    return .{ .created_at = std.mem.readInt(i64, cursor[0..8], .little), .run_id = cursor[8..] };
}

fn encodeCursor(buf: []u8, at: RunAt.Value) []const u8 {
    std.mem.writeInt(i64, buf[0..8], at.created_at, .little);
    @memcpy(buf[8..][0..at.run_id.len], at.run_id);
    return buf[0 .. 8 + at.run_id.len];
}

/// Whether `a` comes before `b`: newer first, then by id.
fn before(a: RunAt.Value, b: RunAt.Value) bool {
    if (a.created_at != b.created_at) return a.created_at > b.created_at;
    return std.mem.lessThan(u8, a.run_id, b.run_id);
}

// ── The owner's side ─────────────────────────────────────────────────────

/// This shard's page of the runs that match `q`, after `q.after`, from its
/// own runs, into `buf`: at most `q.limit` runs, their entries at most
/// `room` bytes.
pub fn ownPage(handler: *WorkflowHandler, namespace: []const u8, workflow: []const u8, q: Query, room: u32, buf: []u8) error{OutOfMemory}!PageResponse.Value {
    const allocator = handler.allocator;
    var runs: std.ArrayListUnmanaged(WorkflowHandler.RunInfo) = .empty;
    defer runs.deinit(allocator);
    try handler.collectMatchingRuns(&runs, namespace, workflow, if (q.status.len == 0) null else q.status, if (q.search.len == 0) null else q.search);
    var kept: usize = 0;
    for (runs.items) |r| {
        if (q.after) |after| if (!before(after, atOf(r))) continue;
        runs.items[kept] = r;
        kept += 1;
    }
    const page = runs.items[0..kept];
    std.mem.sortUnstable(WorkflowHandler.RunInfo, page, {}, struct {
        fn lessThan(_: void, a: WorkflowHandler.RunInfo, b: WorkflowHandler.RunInfo) bool {
            return before(atOf(a), atOf(b));
        }
    }.lessThan);

    var b = Runs.Builder.init(buf);
    var used: usize = 0;
    for (page, 0..) |r, i| {
        var eb: [system_ops.MAX_RUN_ENTRY]u8 = undefined;
        const entry = writeEntry(&eb, r);
        if (i == q.limit or used + entry.len > room or !(b.add(.{ .at = atOf(r), .entry = entry }) catch false))
            return .{ .runs = b.value(), .more = true };
        used += entry.len;
    }
    return .{ .runs = b.value(), .more = false };
}

fn atOf(r: WorkflowHandler.RunInfo) RunAt.Value {
    return .{ .created_at = r.created_at, .run_id = r.run_id };
}

/// `[run_id_len:u16][run_id][workflow_len:u16][workflow][status_len:u16][status][created_at:i64]`
fn writeEntry(buf: []u8, r: WorkflowHandler.RunInfo) []const u8 {
    var w: std.Io.Writer = .fixed(buf);
    // Each part is under its bound: run ids from the start request's u16,
    // workflow names checked at create, statuses literals.
    for ([_][]const u8{ r.run_id, r.workflow, r.status }) |field| {
        w.writeInt(u16, @intCast(field.len), .little) catch unreachable;
        w.writeAll(field) catch unreachable;
    }
    w.writeInt(i64, r.created_at, .little) catch unreachable;
    return w.buffered();
}

/// `system.runs_page`, on the shard asked: its page of the runs.
pub fn handlePage(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: proto.Request) void {
    const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
    const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
    var diag: schema.Diagnostic = .{};
    const r = PageRequest.decodeAll(req.value, &diag) catch
        return shard.sendRefusal(conn, req.header.request_id, .bad_request, .{ .reason = diag.reason, .ran = .no, .message = "bad request: a runs page that doesn't decode" });
    const buf = shard.allocator.alloc(u8, Runs.max_size) catch return outOfMemory(shard, conn, req.header.request_id);
    defer shard.allocator.free(buf);
    const q: Query = .{ .limit = @min(r.limit, MAX_LIMIT), .status = r.status, .search = r.search, .after = r.after };
    const v = ownPage(shard.workflow_handler, req.namespace, req.key, q, r.room, buf) catch
        return outOfMemory(shard, conn, req.header.request_id);
    table_run.answer(PageResponse, shard, conn, req.header.request_id, v);
}

// ── The client's answer ──────────────────────────────────────────────────

/// `[count:u32](entry)*[has_more:u8][cursor_len:u16][cursor]`, the cursor
/// the last run sent when more follow.
fn answer(shard: *Shard, conn: *Connection, request_id: u64, entries: []const []const u8, last: ?RunAt.Value, more: bool) void {
    var body: std.Io.Writer.Allocating = .init(shard.allocator);
    defer body.deinit();
    frame(&body.writer, entries, last, more) catch return outOfMemory(shard, conn, request_id);
    shard.sendOkResponse(conn, request_id, body.written());
}

fn frame(w: *std.Io.Writer, entries: []const []const u8, last: ?RunAt.Value, more: bool) std.Io.Writer.Error!void {
    try w.writeInt(u32, @intCast(entries.len), .little);
    for (entries) |e| try w.writeAll(e);
    var cb: [MAX_CURSOR]u8 = undefined;
    const cursor: []const u8 = if (more) if (last) |at| encodeCursor(&cb, at) else "" else "";
    try w.writeByte(@intFromBool(cursor.len > 0));
    try w.writeInt(u16, @intCast(cursor.len), .little);
    try w.writeAll(cursor);
}

/// One shard's runs: this one's page, answered as it is.
pub fn answerOwn(shard: *Shard, conn: *Connection, req: proto.Request, q: Query) void {
    const buf = shard.allocator.alloc(u8, Runs.max_size) catch return outOfMemory(shard, conn, req.header.request_id);
    defer shard.allocator.free(buf);
    const page = ownPage(shard.workflow_handler, req.namespace, req.key, q, @intCast(ROOM), buf) catch
        return outOfMemory(shard, conn, req.header.request_id);
    var entries: [MAX_LIMIT][]const u8 = undefined;
    var last: ?RunAt.Value = null;
    var n: usize = 0;
    var it = page.runs.iterator();
    while (it.next() catch return corrupt(shard, conn, req.header.request_id)) |run| : (n += 1) {
        entries[n] = run.entry;
        last = run.at;
    }
    answer(shard, conn, req.header.request_id, entries[0..n], last, page.more);
}

// ── The asker's side ─────────────────────────────────────────────────────

/// A page under way across every shard: what the merge needs once all
/// have answered.
const Merge = struct {
    limit: u32,

    pub const gather_op: gather_mod.Op = .{ .merge = merge, .deinit = free };

    fn free(state: *anyopaque, allocator: std.mem.Allocator) void {
        allocator.destroy(@as(*Merge, @ptrCast(@alignCast(state))));
    }

    fn merge(shard: *Shard, conn: *Connection, request_id: u64, state: *anyopaque, bodies: []const []const u8) anyerror!void {
        const self: *Merge = @ptrCast(@alignCast(state));
        const allocator = shard.allocator;
        const pages = try allocator.alloc(PageResponse.Value, bodies.len);
        defer allocator.free(pages);
        for (bodies, pages) |body, *page| {
            var diag: schema.Diagnostic = .{};
            page.* = try PageResponse.decodeAll(body, &diag);
        }
        const entries = try allocator.alloc([]const u8, self.limit);
        defer allocator.free(entries);
        const m = mergeRuns(pages, self.limit, ROOM, entries) catch |err| return switch (err) {
            error.NoProgress => noProgress(shard, conn, request_id),
            error.Corrupt => corrupt(shard, conn, request_id),
        };
        answer(shard, conn, request_id, entries[0..m.taken], m.last, m.more);
    }
};

pub const Merged = struct { taken: usize, last: ?RunAt.Value, more: bool };

/// The shards' pages' runs in the list's order, into `entries`: at most
/// `limit`, their entries at most `room` bytes, and none past the last run
/// a shard sent when it holds more, since its next may come before
/// another's. A shard that holds more but sent nothing would stop the list
/// where it stands: `error.NoProgress`.
pub fn mergeRuns(pages: []const PageResponse.Value, limit: u32, room: usize, entries: [][]const u8) error{ Corrupt, NoProgress }!Merged {
    std.debug.assert(entries.len >= limit);
    var bound: ?RunAt.Value = null;
    for (pages) |page| if (page.more) {
        var it = page.runs.iterator();
        var last: ?RunAt.Value = null;
        while (try it.next()) |run| last = run.at;
        const at = last orelse return error.NoProgress;
        if (bound == null or before(at, bound.?)) bound = at;
    };

    var heads: [gather_mod.MAX_PARTS]?system_ops.Run.Value = undefined;
    var its: [gather_mod.MAX_PARTS]Runs.Iterator = undefined;
    for (pages, 0..) |page, i| {
        its[i] = page.runs.iterator();
        heads[i] = try its[i].next();
    }
    var m: Merged = .{ .taken = 0, .last = null, .more = bound != null };
    var used: usize = 0;
    while (true) {
        var first: ?usize = null;
        for (heads[0..pages.len], 0..) |h, i| if (h) |run| {
            if (first == null or before(run.at, heads[first.?].?.at)) first = i;
        };
        const i = first orelse break;
        const run = heads[i].?;
        if (bound) |b| if (before(b, run.at)) break;
        if (m.taken == limit or used + run.entry.len > room) {
            m.more = true;
            break;
        }
        entries[m.taken] = run.entry;
        m.taken += 1;
        used += run.entry.len;
        m.last = run.at;
        heads[i] = try its[i].next();
    }
    if (m.more and m.taken == 0) return error.NoProgress;
    return m;
}

pub const Begun = union(enum) {
    asked,
    refused: table_run.Refused,
};

const OUT_OF_MEMORY: table_run.Refused = .{ .status = .internal_error, .refusal = .{ .reason = .internal, .ran = .no, .message = "internal error: out of memory" } };

/// Ask every shard for its page of the runs `q` asks for, the merge
/// answering. Each shard's page has its share of one answer's room, and at
/// least a run's worth, so every shard can send one.
pub fn start(shard: *Shard, conn: *Connection, req: proto.Request, q: Query) Begun {
    const allocator = shard.allocator;
    const n: u16 = @intCast(shard.shardCount());
    const room: u32 = @intCast(@max(ROOM / n, system_ops.MAX_RUN_ENTRY));
    const state = allocator.create(Merge) catch return .{ .refused = OUT_OF_MEMORY };
    state.* = .{ .limit = q.limit };
    var gather_owns = false;
    defer if (!gather_owns) Merge.free(state, allocator);

    const parts = allocator.alloc(gather_mod.Part, n) catch return .{ .refused = OUT_OF_MEMORY };
    defer allocator.free(parts);
    const ask = allocator.alloc(u8, PageRequest.max_size) catch return .{ .refused = OUT_OF_MEMORY };
    defer allocator.free(ask);
    const ask_body = PageRequest.encodeInto(ask, .{ .status = q.status, .search = q.search, .after = q.after, .limit = q.limit, .room = room }) catch return .{ .refused = OUT_OF_MEMORY };

    const buf = allocator.alloc(u8, Runs.max_size) catch return .{ .refused = OUT_OF_MEMORY };
    defer allocator.free(buf);
    const v = ownPage(shard.workflow_handler, req.namespace, req.key, q, room, buf) catch return .{ .refused = OUT_OF_MEMORY };
    const size = PageResponse.encodedSize(v) catch return .{ .refused = OUT_OF_MEMORY };
    const own = allocator.alloc(u8, size) catch return .{ .refused = OUT_OF_MEMORY };
    var own_owned = false;
    defer if (!own_owned) allocator.free(own);
    _ = PageResponse.encodeInto(own, v) catch unreachable; // sized above

    for (parts, 0..) |*p, i| {
        const s: u16 = @intCast(i);
        p.* = if (s == shard.id) .{ .owner = s, .body = own } else .{ .owner = s, .body = ask_body, .op = @backingInt(proto.OpCode.system_runs_page) };
    }
    gather_owns = true;
    own_owned = true;
    shard.startGather(conn, req, parts, &Merge.gather_op, state) catch |err| return .{ .refused = switch (err) {
        error.GatherBusy => .{ .status = .overloaded, .refusal = .{ .reason = .shard_busy, .ran = .no, .message = "overloaded: too many requests across shards are under way; this request was not run — retry" } },
        error.OutOfMemory => OUT_OF_MEMORY,
    } };
    return .asked;
}

fn noProgress(shard: *Shard, conn: *Connection, request_id: u64) void {
    shard.sendRefusal(conn, request_id, .internal_error, proto.notRun("internal error: a shard's page of this list made no progress"));
}

fn corrupt(shard: *Shard, conn: *Connection, request_id: u64) void {
    shard.sendRefusal(conn, request_id, .internal_error, proto.notRun("internal error: a runs page didn't decode"));
}

fn outOfMemory(shard: *Shard, conn: *Connection, request_id: u64) void {
    shard.sendRefusal(conn, request_id, OUT_OF_MEMORY.status, OUT_OF_MEMORY.refusal);
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

const TestRun = struct { at: i64, id: []const u8 };

/// A shard's page of `runs`, each entry its id, into `buf`.
fn testPage(buf: []u8, runs: []const TestRun, more: bool) !PageResponse.Value {
    var b = Runs.Builder.init(buf);
    for (runs) |r| try testing.expect(try b.add(.{ .at = .{ .created_at = r.at, .run_id = r.id }, .entry = r.id }));
    return .{ .runs = b.value(), .more = more };
}

fn expectMerged(want: []const []const u8, got: []const []const u8) !void {
    try testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| try testing.expectEqualStrings(w, g);
}

test "list runs: shards' pages merge newest first, by id among runs started together" {
    var a: [512]u8 = undefined;
    var b: [512]u8 = undefined;
    const pages = [_]PageResponse.Value{
        try testPage(&a, &.{ .{ .at = 30, .id = "a30" }, .{ .at = 20, .id = "b20" } }, false),
        try testPage(&b, &.{ .{ .at = 20, .id = "a20" }, .{ .at = 10, .id = "a10" } }, false),
    };
    var entries: [8][]const u8 = undefined;
    const m = try mergeRuns(&pages, 8, 1 << 20, &entries);
    try expectMerged(&.{ "a30", "a20", "b20", "a10" }, entries[0..m.taken]);
    try testing.expect(!m.more);
}

test "list runs: the merge takes nothing past the last run of a shard that holds more" {
    var a: [512]u8 = undefined;
    var b: [512]u8 = undefined;
    // The first shard's next run may start at 85, before the second's 80.
    const pages = [_]PageResponse.Value{
        try testPage(&a, &.{ .{ .at = 100, .id = "x" }, .{ .at = 90, .id = "y" } }, true),
        try testPage(&b, &.{ .{ .at = 95, .id = "p" }, .{ .at = 80, .id = "q" } }, false),
    };
    var entries: [8][]const u8 = undefined;
    const m = try mergeRuns(&pages, 8, 1 << 20, &entries);
    try expectMerged(&.{ "x", "p", "y" }, entries[0..m.taken]);
    try testing.expect(m.more);
    try testing.expectEqualStrings("y", m.last.?.run_id);
    try testing.expectEqual(@as(i64, 90), m.last.?.created_at);
}

test "list runs: the merge stops at the limit and at the room, with more to come" {
    var a: [512]u8 = undefined;
    const pages = [_]PageResponse.Value{try testPage(&a, &.{ .{ .at = 3, .id = "r3" }, .{ .at = 2, .id = "r2" }, .{ .at = 1, .id = "r1" } }, false)};
    var entries: [8][]const u8 = undefined;
    const by_limit = try mergeRuns(&pages, 2, 1 << 20, &entries);
    try expectMerged(&.{ "r3", "r2" }, entries[0..by_limit.taken]);
    try testing.expect(by_limit.more);
    const by_room = try mergeRuns(&pages, 8, 5, &entries);
    try expectMerged(&.{ "r3", "r2" }, entries[0..by_room.taken]);
    try testing.expect(by_room.more);
}

test "list runs: a shard that holds more but sent no run is refused, not asked again forever" {
    var a: [512]u8 = undefined;
    var b: [512]u8 = undefined;
    const pages = [_]PageResponse.Value{
        try testPage(&a, &.{.{ .at = 5, .id = "r5" }}, false),
        try testPage(&b, &.{}, true),
    };
    var entries: [8][]const u8 = undefined;
    try testing.expectError(error.NoProgress, mergeRuns(&pages, 8, 1 << 20, &entries));
}

test "list runs: a cursor is the last run sent, and bytes no answer carried are none" {
    var buf: [MAX_CURSOR]u8 = undefined;
    const at: RunAt.Value = .{ .created_at = -7, .run_id = "wfr-1" };
    const back = decodeCursor(encodeCursor(&buf, at)).?;
    try testing.expectEqual(at.created_at, back.created_at);
    try testing.expectEqualStrings(at.run_id, back.run_id);
    try testing.expect(decodeCursor("wfr-1") == null);
    try testing.expect(decodeCursor(&@as([8]u8, @splat(0))) == null);
}
