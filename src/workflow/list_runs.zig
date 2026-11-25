//! A workflow's runs, newest first, a page at a time. A list of every
//! workflow's runs asks each shard for its page (`system.runs_page`, a
//! gather); each answers from its own runs on its own thread, and the shard
//! that took the request merges the pages by start time and run id. A list
//! of one workflow's runs is read on the shard that holds them.
//!
//! A shard walks its index of runs (`runs_machine.RunAtFam`, or
//! `RunAllFam` for every workflow) from the cursor, in the list's order,
//! reading each run to filter it, and stops at its limit, its room, or
//! `WALK_MAX` rows: a page costs that much whatever the namespace holds.
//!
//! The client's cursor is `[created_at:i64][run_id]`, where the next page
//! starts after.

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
const NamespaceId = @import("../namespace/table.zig").Id;
const runs_machine = @import("runs_machine.zig");
const view_mod = @import("../contract/view.zig");
const log = @import("stdx").log.scoped(.workflow);

const PageOp = table_run.op("system.runs_page");
const PageRequest = PageOp.request;
pub const PageResponse = PageOp.response;
const Runs = system_ops.Runs;
pub const RunAt = system_ops.RunAt;

/// The most runs one page holds, however many are asked for.
pub const MAX_LIMIT: u32 = system_ops.MAX_PAGE;
pub const DEFAULT_LIMIT: u32 = 100;

pub const MAX_CURSOR: usize = 8 + system_ops.MAX_RUN_ID;
comptime {
    std.debug.assert(MAX_CURSOR <= std.math.maxInt(u16));
    std.debug.assert(system_ops.MAX_RUN_ID == runs_machine.MAX_RUN_ID);
}

/// The most index rows one shard's page reads, shown or filtered out.
pub const WALK_MAX: u32 = 4096;

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

/// The run a client's cursor names; null for a malformed one: one with no
/// id, an id longer than a run's, or a start time before 1970. It builds an
/// index key, so it is held to what a run's own key can be.
pub fn decodeCursor(cursor: []const u8) ?RunAt.Value {
    if (cursor.len <= 8 or cursor.len > MAX_CURSOR) return null;
    const at: RunAt.Value = .{ .created_at = std.mem.readInt(i64, cursor[0..8], .little), .run_id = cursor[8..] };
    return if (validAt(at)) at else null;
}

/// Whether `at` is a place a run can be: what a key may be built from.
pub fn validAt(at: RunAt.Value) bool {
    return at.created_at >= 0 and at.run_id.len > 0 and at.run_id.len <= system_ops.MAX_RUN_ID;
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
/// `room` bytes, reading at most `WALK_MAX` index rows.
///
/// With more, `stop` is the last row consumed: shown, or filtered out. A
/// row read only to find the page full isn't consumed, so the next page
/// starts at it. A page says more only once it has consumed a row: its
/// limit is at least one and its room at least one entry.
pub fn ownPage(handler: *WorkflowHandler, ns: NamespaceId, workflow: []const u8, q: Query, room: u32, buf: []u8) error{OutOfMemory}!PageResponse.Value {
    std.debug.assert(q.limit > 0);
    var arena = std.heap.ArenaAllocator.init(handler.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const rv = handler.defsRead();
    if (workflow.len > 0) {
        const prefix = try runs_machine.runAtPrefix(a, ns, workflow);
        const after: ?[]const u8 = if (q.after) |c| try runs_machine.runAtKey(a, ns, workflow, c.created_at, c.run_id) else null;
        return walk(runs_machine.RunAtFam, handler, rv, ns, prefix, after, q, room, buf);
    }
    const prefix = try a.dupe(u8, &ns.bytes());
    const after: ?[]const u8 = if (q.after) |c| try runs_machine.runAllKey(a, ns, c.created_at, c.run_id) else null;
    return walk(runs_machine.RunAllFam, handler, rv, ns, prefix, after, q, room, buf);
}

fn walk(comptime F: type, handler: *WorkflowHandler, rv: view_mod.ReadView, ns: NamespaceId, prefix: []const u8, after: ?[]const u8, q: Query, room: u32, buf: []u8) error{OutOfMemory}!PageResponse.Value {
    var it = if (after) |k| rv.rangeAfter(F, prefix, k) else rv.range(F, prefix);
    var b = Runs.Builder.init(buf);
    var used: usize = 0;
    var shown: u32 = 0;
    var read: u32 = 0;
    var stop: ?RunAt.Value = null;
    var kb: [4 + system_ops.MAX_RUN_ID]u8 = undefined;
    while (it.next() catch unreachable) |item| { // written by the machine alone
        if (read == WALK_MAX) return .{ .runs = b.value(), .more = true, .stop = stop };
        // `prefix ‖ ~created_ms ‖ run`.
        const tail = item.key[prefix.len..];
        const created_at: i64 = @intCast(std.math.maxInt(u64) - std.mem.readInt(u64, tail[0..8], .big));
        // The id is the key's own: the store holds it while the page is
        // answered, as the walk and its answer run in one call.
        const at: RunAt.Value = .{ .created_at = created_at, .run_id = tail[8..] };
        const run_key = view_mod.idKey(&kb, ns, at.run_id) catch unreachable; // bounded at start
        const run = switch (handler.listedRun(run_key, at.run_id, q.status, q.search)) {
            .shown => |r| r,
            .filtered => {
                read += 1;
                stop = at;
                continue;
            },
            .missing => {
                missingRun(handler, at);
                read += 1;
                stop = at;
                continue;
            },
        };
        var eb: [system_ops.MAX_RUN_ENTRY]u8 = undefined;
        const entry = writeEntry(&eb, run);
        const added = if (shown == q.limit or used + entry.len > room) false else b.add(.{ .at = at, .entry = entry }) catch |err| {
            unlistable(handler, at, err);
            read += 1;
            stop = at;
            continue;
        };
        if (!added) {
            // Its limit is at least one and its room one entry: it has
            // consumed a row, so a page with more says where it stopped.
            std.debug.assert(stop != null);
            return .{ .runs = b.value(), .more = true, .stop = stop };
        }
        used += entry.len;
        shown += 1;
        read += 1;
        stop = at;
    }
    return .{ .runs = b.value(), .more = false, .stop = null };
}

/// An index row whose run isn't there: a Flo bug, said once per shard,
/// and skipped.
fn missingRun(handler: *WorkflowHandler, at: RunAt.Value) void {
    handler.missing_runs += 1;
    if (handler.missing_runs_logged > 0) return;
    handler.missing_runs_logged += 1;
    // A Flo bug, so an error; a test that plants one expects it.
    if (@import("builtin").is_test)
        log.warn("workflow run list: run {s} is indexed but not there; skipped", .{at.run_id})
    else
        log.err("workflow run list: run {s} is indexed but not there; skipped", .{at.run_id});
}

/// A run whose entry breaks the page's schema (a Flo bug): logged, and
/// skipped like a missing one, so one bad run never fails every page.
fn unlistable(handler: *WorkflowHandler, at: RunAt.Value, err: anyerror) void {
    handler.missing_runs += 1;
    if (handler.unlistable_said) return;
    handler.unlistable_said = true;
    if (@import("builtin").is_test)
        log.warn("workflow run list: run {s} can't be listed ({s}); skipped", .{ at.run_id, @errorName(err) })
    else
        log.err("workflow run list: run {s} can't be listed ({s}); skipped", .{ at.run_id, @errorName(err) });
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
    if (r.after) |at| if (!validAt(at))
        return shard.sendRefusal(conn, req.header.request_id, .bad_request, .{ .reason = .out_of_range, .ran = .no, .message = "bad request: a runs page after a place no run can be" });
    // At least one run and room for one, so a page with more has consumed
    // a row, whatever the asker sent.
    const q: Query = .{ .limit = std.math.clamp(r.limit, 1, MAX_LIMIT), .status = r.status, .search = r.search, .after = r.after };
    const v = ownPage(shard.workflow_handler, req.ns_id, req.key, q, @max(r.room, system_ops.MAX_RUN_ENTRY), buf) catch
        return outOfMemory(shard, conn, req.header.request_id);
    table_run.answer(PageResponse, shard, conn, req.header.request_id, v);
}

// ── The client's answer ──────────────────────────────────────────────────

/// `[count:u32](entry)*[has_more:u8][cursor_len:u16][cursor]`: with more,
/// the cursor the next page starts after. A page may hold no entries and
/// more: its shards read their bound and matched none.
fn answer(shard: *Shard, conn: *Connection, request_id: u64, entries: []const []const u8, cursor: ?RunAt.Value) void {
    var body: std.Io.Writer.Allocating = .init(shard.allocator);
    defer body.deinit();
    frame(&body.writer, entries, cursor) catch return outOfMemory(shard, conn, request_id);
    shard.sendOkResponse(conn, request_id, body.written());
}

fn frame(w: *std.Io.Writer, entries: []const []const u8, cursor: ?RunAt.Value) std.Io.Writer.Error!void {
    try w.writeInt(u32, @intCast(entries.len), .little);
    for (entries) |e| try w.writeAll(e);
    var cb: [MAX_CURSOR]u8 = undefined;
    const c: []const u8 = if (cursor) |at| encodeCursor(&cb, at) else "";
    try w.writeByte(@intFromBool(cursor != null));
    try w.writeInt(u16, @intCast(c.len), .little);
    try w.writeAll(c);
}

/// One shard's runs: this one's page, answered as it is.
pub fn answerOwn(shard: *Shard, conn: *Connection, req: proto.Request, q: Query) void {
    const buf = shard.allocator.alloc(u8, Runs.max_size) catch return outOfMemory(shard, conn, req.header.request_id);
    defer shard.allocator.free(buf);
    const page = ownPage(shard.workflow_handler, req.ns_id, req.key, q, @intCast(ROOM), buf) catch
        return outOfMemory(shard, conn, req.header.request_id);
    var entries: [MAX_LIMIT][]const u8 = undefined;
    var n: usize = 0;
    var it = page.runs.iterator();
    while (it.next() catch return corrupt(shard, conn, req.header.request_id)) |run| : (n += 1) entries[n] = run.entry;
    answer(shard, conn, req.header.request_id, entries[0..n], if (page.more) page.stop else null);
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
        const m = mergeRuns(pages, self.limit, ROOM, entries) catch return corrupt(shard, conn, request_id);
        answer(shard, conn, request_id, entries[0..m.taken], m.cursor);
    }
};

pub const Merged = struct { taken: usize, cursor: ?RunAt.Value };

/// The shards' pages' runs in the list's order, into `entries`: at most
/// `limit`, their entries at most `room` bytes, and none past the bound,
/// the least `stop` of the shards holding more: past it, a shard's next
/// run may come before another's. Each such shard sent every run at or
/// before its stop. The cursor is the last run taken when the merge
/// stopped on its own limit or room, else the bound; none when no shard
/// holds more. Each shard holding more consumed a row, so the cursor
/// moves on, and a page may hold no runs.
pub fn mergeRuns(pages: []const PageResponse.Value, limit: u32, room: usize, entries: [][]const u8) error{Corrupt}!Merged {
    std.debug.assert(entries.len >= limit);
    var bound: ?RunAt.Value = null;
    for (pages) |page| if (page.more) {
        const at = page.stop orelse return error.Corrupt;
        if (bound == null or before(at, bound.?)) bound = at;
    };

    var heads: [gather_mod.MAX_PARTS]?system_ops.Run.Value = undefined;
    var its: [gather_mod.MAX_PARTS]Runs.Iterator = undefined;
    for (pages, 0..) |page, i| {
        its[i] = page.runs.iterator();
        heads[i] = try its[i].next();
    }
    var taken: usize = 0;
    var last: ?RunAt.Value = null;
    var used: usize = 0;
    while (true) {
        var first: ?usize = null;
        for (heads[0..pages.len], 0..) |h, i| if (h) |run| {
            if (first == null or before(run.at, heads[first.?].?.at)) first = i;
        };
        const i = first orelse break;
        const run = heads[i].?;
        if (bound) |b| if (before(b, run.at)) break;
        if (taken == limit or used + run.entry.len > room)
            return .{ .taken = taken, .cursor = last orelse bound };
        entries[taken] = run.entry;
        taken += 1;
        used += run.entry.len;
        last = run.at;
        heads[i] = try its[i].next();
    }
    return .{ .taken = taken, .cursor = bound };
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
    const v = ownPage(shard.workflow_handler, req.ns_id, req.key, q, room, buf) catch return .{ .refused = OUT_OF_MEMORY };
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

fn corrupt(shard: *Shard, conn: *Connection, request_id: u64) void {
    shard.sendRefusal(conn, request_id, .internal_error, proto.notRun("internal error: a runs page didn't decode"));
}

fn outOfMemory(shard: *Shard, conn: *Connection, request_id: u64) void {
    shard.sendRefusal(conn, request_id, OUT_OF_MEMORY.status, OUT_OF_MEMORY.refusal);
}

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

const TestRun = struct { at: i64, id: []const u8 };

/// A shard's page of `runs`, each entry its id, into `buf`; with more, its
/// stop `stop`, or its last run.
fn testPage(buf: []u8, runs: []const TestRun, more: bool) !PageResponse.Value {
    return testPageTo(buf, runs, more, null);
}

fn testPageTo(buf: []u8, runs: []const TestRun, more: bool, stop: ?TestRun) !PageResponse.Value {
    var b = Runs.Builder.init(buf);
    for (runs) |r| try testing.expect(try b.add(.{ .at = .{ .created_at = r.at, .run_id = r.id }, .entry = r.id }));
    const last: ?TestRun = stop orelse if (runs.len > 0) runs[runs.len - 1] else null;
    const at: ?RunAt.Value = if (more) if (last) |l| RunAt.Value{ .created_at = l.at, .run_id = l.id } else null else null;
    return .{ .runs = b.value(), .more = more, .stop = at };
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
    try testing.expect(m.cursor == null);
}

test "list runs: the merge takes nothing past the stop of a shard that holds more" {
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
    try testing.expectEqualStrings("y", m.cursor.?.run_id);
    try testing.expectEqual(@as(i64, 90), m.cursor.?.created_at);
}

test "list runs: the merge stops at the limit and at the room, its cursor the last run it took" {
    var a: [512]u8 = undefined;
    const pages = [_]PageResponse.Value{try testPage(&a, &.{ .{ .at = 3, .id = "r3" }, .{ .at = 2, .id = "r2" }, .{ .at = 1, .id = "r1" } }, false)};
    var entries: [8][]const u8 = undefined;
    const by_limit = try mergeRuns(&pages, 2, 1 << 20, &entries);
    try expectMerged(&.{ "r3", "r2" }, entries[0..by_limit.taken]);
    try testing.expectEqualStrings("r2", by_limit.cursor.?.run_id);
    const by_room = try mergeRuns(&pages, 8, 5, &entries);
    try expectMerged(&.{ "r3", "r2" }, entries[0..by_room.taken]);
    try testing.expectEqualStrings("r2", by_room.cursor.?.run_id);
}

test "list runs: a shard that read its bound and matched nothing sends no run, and the cursor moves to its stop" {
    var a: [512]u8 = undefined;
    var b: [512]u8 = undefined;
    // The second shard read rows down to 50 and showed none of them.
    const pages = [_]PageResponse.Value{
        try testPage(&a, &.{.{ .at = 5, .id = "r5" }}, false),
        try testPageTo(&b, &.{}, true, .{ .at = 50, .id = "s50" }),
    };
    var entries: [8][]const u8 = undefined;
    const m = try mergeRuns(&pages, 8, 1 << 20, &entries);
    try testing.expectEqual(@as(usize, 0), m.taken);
    try testing.expectEqual(@as(i64, 50), m.cursor.?.created_at);
    try testing.expectEqualStrings("s50", m.cursor.?.run_id);
    // A shard holding more must say where it stopped.
    const broken = [_]PageResponse.Value{.{ .runs = (Runs.Builder.init(&b)).value(), .more = true, .stop = null }};
    try testing.expectError(error.Corrupt, mergeRuns(&broken, 8, 1 << 20, &entries));
}

test "list runs: runs started in one ms with ids of other lengths, on two shards, are each listed once, in byte order" {
    // Shard 1 holds "b" and "ab"; shard 2 holds "c" and "zz", all at 7. By
    // byte order: ab, b, c, zz. A page of two, then the rest.
    var a: [512]u8 = undefined;
    var b: [512]u8 = undefined;
    var entries: [8][]const u8 = undefined;
    const first = [_]PageResponse.Value{
        try testPage(&a, &.{ .{ .at = 7, .id = "ab" }, .{ .at = 7, .id = "b" } }, false),
        try testPage(&b, &.{ .{ .at = 7, .id = "c" }, .{ .at = 7, .id = "zz" } }, false),
    };
    const m = try mergeRuns(&first, 2, 1 << 20, &entries);
    try expectMerged(&.{ "ab", "b" }, entries[0..m.taken]);
    // Each shard's next page starts after the cursor, in the same order.
    const cursor = m.cursor.?;
    const rest_b: []const TestRun = &.{ .{ .at = 7, .id = "c" }, .{ .at = 7, .id = "zz" } };
    for (rest_b) |r| try testing.expect(before(cursor, .{ .created_at = r.at, .run_id = r.id }));
    // And the index keys sort the same way: no length before the id.
    const k_b = try runs_machine.runAllKey(testing.allocator, .default, 7, "b");
    defer testing.allocator.free(k_b);
    const k_c = try runs_machine.runAllKey(testing.allocator, .default, 7, "c");
    defer testing.allocator.free(k_c);
    const k_ab = try runs_machine.runAllKey(testing.allocator, .default, 7, "ab");
    defer testing.allocator.free(k_ab);
    try testing.expect(std.mem.lessThan(u8, k_ab, k_b));
    try testing.expect(std.mem.lessThan(u8, k_b, k_c));
}

test "list runs: a cursor round-trips; one with no id, a stamp before 1970 or an id past a run's is refused" {
    var buf: [MAX_CURSOR + 8]u8 = undefined;
    const at: RunAt.Value = .{ .created_at = 7, .run_id = "wfr-1" };
    const back = decodeCursor(encodeCursor(&buf, at)).?;
    try testing.expectEqual(at.created_at, back.created_at);
    try testing.expectEqualStrings(at.run_id, back.run_id);
    try testing.expect(decodeCursor("wfr-1") == null);
    try testing.expect(decodeCursor(&@as([8]u8, @splat(0))) == null);
    try testing.expect(decodeCursor(encodeCursor(&buf, .{ .created_at = -7, .run_id = "wfr-1" })) == null);
    const long: [system_ops.MAX_RUN_ID + 1]u8 = @splat('x');
    std.mem.writeInt(i64, buf[0..8], 7, .little);
    @memcpy(buf[8..][0..long.len], &long);
    try testing.expect(decodeCursor(buf[0 .. 8 + long.len]) == null);
}
