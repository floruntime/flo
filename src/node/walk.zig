//! A list across shards, asked of each. The shard that takes the request
//! asks a window of shards for their pages (`system.walk_page`, a gather);
//! each answers from its own state on its own thread, entries already in
//! the list's answer encoding; the asker merges them in shard order and
//! frames the client's answer. No shard reads another's state.

const std = @import("std");
const proto = @import("../protocol/proto.zig");
const ops = @import("../protocol/ops.zig");
const system_ops = @import("../protocol/system_ops.zig");
const schema = @import("../protocol/schema.zig");
const table_run = @import("table_run.zig");
const gather_mod = @import("gather.zig");
const dispatcher_mod = @import("dispatcher.zig");
const handler_mod = @import("../namespace/handler.zig");
const Shard = @import("shard.zig").Shard;
const Connection = @import("connection.zig").Connection;
const StreamProjection = @import("../projection/stream.zig").StreamProjection;
const QueueHandler = @import("../queue/handler.zig").QueueHandler;
const ProcessingHandler = @import("../processing/handler.zig").ProcessingHandler;
const WorkflowHandler = @import("../workflow/handler.zig").WorkflowHandler;

const NameWalker = dispatcher_mod.NameWalker;
const OpCode = proto.OpCode;
const PageOp = table_run.op("system.walk_page");
pub const Response = PageOp.response;
const Entries = system_ops.Entries;

/// Shards one page asks: enough that a sparse list fills a page, few
/// enough to bound what the asker holds. The parts' entries share one
/// answer's room, but each part's body also carries its names, and may
/// reach `Entries.max_size`, so a page holds up to WINDOW of those while
/// it merges.
pub const WINDOW: u16 = 8;

/// What the client's answer holds besides its entries: the count, then
/// `[has_more:u8][cursor_len:u16][cursor]`.
const FRAME: usize = 4 + 1 + 2 + 2 + system_ops.MAX_LOCAL_CURSOR;

/// The bytes a page's entries may take in the client's answer.
const ROOM: usize = ops.MAX_RESPONSE_BODY - FRAME;

comptime {
    // Each shard's share of the room holds the largest entry, so a shard
    // with an item left always sends one.
    std.debug.assert(ROOM / WINDOW >= system_ops.MAX_ENTRY);
}

// ── The owner's side ─────────────────────────────────────────────────────

pub const PageError = error{ NotAWalk, ScanShort, EntryTooLarge };

/// This shard's page of `op`'s list, from its own state, into `buf`: its
/// entries take at most `room` bytes of the client's answer. An item the
/// list names but holds no entry for is listed with an empty one, so
/// offsets count it as the scan did.
pub fn ownPage(shard: *Shard, op: u16, namespace: []const u8, filter: []const u8, cursor: ?[]const u8, skip: u32, limit: u32, room: u32, buf: []u8) PageError!Response.Value {
    if (op >= proto.MAX_OPCODES) return error.NotAWalk;
    const ctx = shard.dispatcher.walk_context[op] orelse return error.NotAWalk;
    const walker = shard.dispatcher.walker(op, @intCast(shard.shardCount())) orelse return error.NotAWalk;
    _ = dispatcher_mod.takeScanShort();
    // A page of nothing resumes where it was asked from, and the walk
    // would ask again forever: a limit of 0 is one.
    const page = walker.localPage(ctx, namespace, filter, cursor, skip, std.math.clamp(limit, 1, system_ops.MAX_PAGE));
    if (dispatcher_mod.takeScanShort()) return error.ScanShort;

    var entries = Entries.Builder.init(buf);
    var more = page.more;
    var used: usize = 0;
    for (page.items) |name| {
        var eb: [system_ops.MAX_ENTRY]u8 = undefined;
        var w: std.Io.Writer = .fixed(&eb);
        try writeEntry(@fromBackingInt(@intCast(op)), ctx, namespace, name, &w);
        const entry = w.buffered();
        // Out of room: the rest is the next page's.
        if (used + entry.len > room or !(entries.add(.{ .name = name, .entry = entry }) catch return error.EntryTooLarge)) {
            more = true;
            break;
        }
        used += entry.len;
    }
    return .{ .entries = entries.value(), .more = more };
}

/// `name`'s entry as `op`'s answer lists it, read from `ctx`, this shard's
/// own; nothing for a name with no entry.
fn writeEntry(op: OpCode, ctx: *anyopaque, namespace: []const u8, name: []const u8, w: *std.Io.Writer) PageError!void {
    writeEntryTo(op, ctx, namespace, name, w) catch return error.EntryTooLarge;
}

fn writeEntryTo(op: OpCode, ctx: *anyopaque, namespace: []const u8, name: []const u8, w: *std.Io.Writer) (std.Io.Writer.Error || PageError)!void {
    switch (op) {
        // [name_len:u32][name][partition_count:u32], partitions from the
        // stream's metadata, keyed by the qualified name.
        .stream_list => {
            const stream: *const StreamProjection = @ptrCast(@alignCast(ctx));
            var qbuf: [handler_mod.MAX_QUALIFIED_KEY]u8 = undefined;
            const pc = if (handler_mod.qualifyKey(&qbuf, namespace, name)) |q| stream.getPartitionCount(q) else |_| 1;
            try w.writeInt(u32, @intCast(name.len), .little);
            try w.writeAll(name);
            try w.writeInt(u32, pc, .little);
        },
        // [name_len:u32][name][ns_len:u32][ns][pending][available][enqueued][dequeued][dlq], u64 each.
        .queue_list => {
            const handler: *QueueHandler = @ptrCast(@alignCast(ctx));
            const want_ns = if (namespace.len > 0) namespace else "default";
            var meta: struct { ns: []const u8, enqueued: u64 = 0, dequeued: u64 = 0, dlq: u64 = 0 } = .{ .ns = namespace };
            var it = handler.queue.known_queues.iterator();
            while (it.next()) |entry| {
                const m = entry.value_ptr;
                if (!std.mem.eql(u8, m.name, name) or !std.mem.eql(u8, m.namespace, want_ns)) continue;
                meta = .{ .ns = m.namespace, .enqueued = m.enqueued, .dequeued = m.dequeued, .dlq = @intCast(handler.queue.deadLetters(entry.key_ptr.*).len) };
                break;
            }
            const pending = if (meta.enqueued > meta.dequeued) meta.enqueued - meta.dequeued else 0;
            try w.writeInt(u32, @intCast(name.len), .little);
            try w.writeAll(name);
            try w.writeInt(u32, @intCast(meta.ns.len), .little);
            try w.writeAll(meta.ns);
            for ([_]u64{ pending, pending, meta.enqueued, meta.dequeued, meta.dlq }) |v| try w.writeInt(u64, v, .little);
        },
        .processing_list => {
            const handler: *const ProcessingHandler = @ptrCast(@alignCast(ctx));
            const job = handler.jobNamed(namespace, name) orelse return;
            if (ProcessingHandler.entrySize(job) > w.buffer.len) return error.EntryTooLarge;
            try ProcessingHandler.writeEntry(job, w);
        },
        // [name_len:u16][name][version_len:u16][version][created_at:i64]
        .workflow_list_definitions => {
            const handler: *WorkflowHandler = @ptrCast(@alignCast(ctx));
            var it = handler.definitions.iterator();
            while (it.next()) |entry| {
                const def = entry.value_ptr;
                if (!std.mem.eql(u8, def.name_owned, name)) continue;
                if (namespace.len > 0) {
                    const map_key = entry.key_ptr.*;
                    if (!std.mem.startsWith(u8, map_key, namespace)) continue;
                    if (map_key.len <= namespace.len or map_key[namespace.len] != ':') continue;
                }
                try w.writeInt(u16, @intCast(def.name_owned.len), .little);
                try w.writeAll(def.name_owned);
                try w.writeInt(u16, @intCast(def.version_owned.len), .little);
                try w.writeAll(def.version_owned);
                try w.writeInt(i64, def.created_at_ms, .little);
                return;
            }
        },
        // [key_len:u16][key][value_len:u32 = 0]
        .action_list => {
            try w.writeInt(u16, @intCast(name.len), .little);
            try w.writeAll(name);
            try w.writeInt(u32, 0, .little);
        },
        // [name_len:u16][name]
        else => {
            try w.writeInt(u16, @intCast(name.len), .little);
            try w.writeAll(name);
        },
    }
}

/// `system.walk_page`, on the shard asked: its page, answered from a
/// buffer as large as the page may be, freed once the answer is written.
pub fn handlePage(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: proto.Request) void {
    const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
    const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
    var diag: schema.Diagnostic = .{};
    const r = PageOp.request.decodeAll(req.value, &diag) catch
        return shard.sendRefusal(conn, req.header.request_id, .bad_request, .{ .reason = diag.reason, .ran = .no, .message = "bad request: a walk page that doesn't decode" });
    const buf = shard.allocator.alloc(u8, Entries.max_size) catch
        return outOfMemoryFor(shard, conn, req.header.request_id);
    defer shard.allocator.free(buf);
    const v = ownPage(shard, r.op, req.namespace, req.key, if (r.cursor.len == 0) null else r.cursor, r.skip, r.limit, r.room, buf) catch |err| {
        const why = refusal(err);
        return shard.sendRefusal(conn, req.header.request_id, why.status, why.refusal);
    };
    table_run.answer(Response, shard, conn, req.header.request_id, v);
}

pub fn refusal(err: PageError) table_run.Refused {
    return switch (err) {
        error.NotAWalk => .{ .status = .bad_request, .refusal = proto.notRun("bad request: that op lists nothing across shards") },
        error.ScanShort => .{ .status = .overloaded, .refusal = proto.notRun("overloaded: no memory to list this page; retry") },
        error.EntryTooLarge => .{ .status = .internal_error, .refusal = proto.notRun("internal error: a listed item's name (over 4096 bytes) or entry (over 16 KiB) is larger than a list page holds") },
    };
}

// ── The asker's side ─────────────────────────────────────────────────────

/// A page under way: what the merge needs once every part has answered.
const Walk = struct {
    op: u16,
    /// The client's cursor, owned: the request's bytes don't outlive it.
    cursor: []u8,
    limit: u32,

    pub const gather_op: gather_mod.Op = .{ .merge = merge, .deinit = free };

    fn free(state: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *Walk = @ptrCast(@alignCast(state));
        allocator.free(self.cursor);
        allocator.destroy(self);
    }

    fn merge(shard: *Shard, conn: *Connection, request_id: u64, state: *anyopaque, bodies: []const []const u8) anyerror!void {
        const self: *Walk = @ptrCast(@alignCast(state));
        const allocator = shard.allocator;
        const resume_at = NameWalker.startOf(if (self.cursor.len == 0) null else self.cursor);

        // Each part's names, for the merge, and its entries, for the answer.
        var name_count: usize = 0;
        const decoded = try allocator.alloc(Response.Value, bodies.len);
        defer allocator.free(decoded);
        for (bodies, decoded) |body, *d| {
            var diag: schema.Diagnostic = .{};
            d.* = try Response.decodeAll(body, &diag);
            name_count += d.entries.count;
        }
        const names = try allocator.alloc([]const u8, name_count);
        defer allocator.free(names);
        const entries = try allocator.alloc([]const u8, name_count);
        defer allocator.free(entries);
        const pages = try allocator.alloc(NameWalker.Page, bodies.len);
        defer allocator.free(pages);
        var at: usize = 0;
        for (decoded, pages) |d, *page| {
            const from = at;
            var it = d.entries.iterator();
            while (try it.next()) |e| {
                names[at] = e.name;
                entries[at] = e.entry;
                at += 1;
            }
            page.* = .{ .items = names[from..at], .more = d.more };
        }

        const walker = shard.dispatcher.walker(self.op, @intCast(shard.shardCount())).?;
        const taken = try allocator.alloc([]const u8, name_count);
        defer allocator.free(taken);
        var cursor_buf: [2 + system_ops.MAX_LOCAL_CURSOR]u8 = undefined;
        const r = walker.mergePages(resume_at, pages, self.limit, taken, &cursor_buf);
        // Every page asked holds a name when its shard has one left (`ownPage`
        // asks for at least one, and each shard's room holds the largest), so
        // a merge always moves past the cursor it was asked from.
        if (r.items.len == 0) if (r.next_cursor) |next| std.debug.assert(!std.mem.eql(u8, next, self.cursor));

        // The pages were taken in order, so what was taken is their entries'
        // prefix. A name two shards both list is answered once.
        var kept: usize = 0;
        for (entries[0..r.items.len], 0..) |entry, i| {
            if (entry.len == 0) continue;
            const dup = for (names[0..kept]) |earlier| {
                if (std.mem.eql(u8, earlier, names[i])) break true;
            } else false;
            if (dup) continue;
            names[kept] = names[i];
            entries[kept] = entry;
            kept += 1;
        }
        if (self.op == @backingInt(OpCode.kv_scan)) return answerScan(shard, conn, request_id, names[0..kept], r.next_cursor);

        var body: std.Io.Writer.Allocating = .init(allocator);
        defer body.deinit();
        const w = &body.writer;
        try w.writeInt(u32, @intCast(kept), .little);
        for (entries[0..kept]) |entry| try w.writeAll(entry);
        const cursor = r.next_cursor orelse "";
        try w.writeByte(@intFromBool(r.next_cursor != null));
        try w.writeInt(u16, @intCast(cursor.len), .little);
        try w.writeAll(cursor);
        shard.sendOkResponse(conn, request_id, body.written());
    }
};

const Scan = table_run.op("kv.scan");
const ScanNames = Scan.response.field_list[0].T;

/// kv.scan's answer: the names, then the cursor. Each shard's page kept
/// to its share of one answer's room, so they fit.
fn answerScan(shard: *Shard, conn: *Connection, request_id: u64, names: []const []const u8, cursor: ?[]const u8) void {
    const buf = shard.allocator.alloc(u8, ScanNames.max_size) catch return outOfMemoryFor(shard, conn, request_id);
    defer shard.allocator.free(buf);
    var b = ScanNames.Builder.init(buf);
    for (names) |name| if (!(b.add(name) catch false))
        return shard.sendRefusal(conn, request_id, .internal_error, proto.notRun("internal error: a scan's pages hold more than one answer"));
    table_run.answer(Scan.response, shard, conn, request_id, .{ .names = b.value(), .cursor = cursor });
}

/// How `start` left the request.
pub const Begun = union(enum) {
    /// The pages are asked for; the merge answers.
    asked,
    /// The cursor is past the last shard: the page is empty and the last.
    past_end,
    refused: table_run.Refused,
};

const OUT_OF_MEMORY: table_run.Refused = .{ .status = .internal_error, .refusal = .{ .reason = .internal, .ran = .no, .message = "internal error: out of memory" } };

/// Ask the shards from the cursor's for their pages of `req`'s list, the
/// merge answering. This shard's own page is read here. `limit` is at least
/// 1: a list's default stands for 0, and kv.scan's schema refuses it.
pub fn start(shard: *Shard, conn: *Connection, req: proto.Request, shard_count: u16, limit: u32, cursor: ?[]const u8) Begun {
    std.debug.assert(limit > 0);
    const begin = NameWalker.startOf(cursor);
    if (begin.shard >= shard_count) return .past_end;
    // No answer of this server's names a resume point that long.
    if (begin.local_cursor) |lc| if (lc.len > system_ops.MAX_LOCAL_CURSOR)
        return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun("bad request: the cursor is longer than any this server sends") } };
    const last = @min(@as(u32, begin.shard) + WINDOW, shard_count);
    const n: u16 = @intCast(last - begin.shard);
    const room: u32 = @intCast(ROOM / n);
    const op = req.header.op_code;
    const Request = PageOp.request;

    const state = shard.allocator.create(Walk) catch return .{ .refused = OUT_OF_MEMORY };
    const cursor_copy = shard.allocator.dupe(u8, cursor orelse "") catch {
        shard.allocator.destroy(state);
        return .{ .refused = OUT_OF_MEMORY };
    };
    state.* = .{ .op = op, .cursor = cursor_copy, .limit = limit };
    const local_copy: []const u8 = (NameWalker.startOf(if (cursor_copy.len == 0) null else cursor_copy).local_cursor) orelse "";
    var gather_owns_state = false;
    defer if (!gather_owns_state) Walk.free(state, shard.allocator);

    var parts: [WINDOW]gather_mod.Part = undefined;
    var bodies: [WINDOW][Request.max_size]u8 = undefined;
    var own: ?[]u8 = null;
    defer if (!gather_owns_state) if (own) |b| shard.allocator.free(b);
    for (0..n) |i| {
        const s: u16 = begin.shard + @as(u16, @intCast(i));
        const first = i == 0;
        const lc: []const u8 = if (first) local_copy else "";
        const skip: u32 = if (first) begin.skip else 0;
        if (s == shard.id) {
            const page_buf = shard.allocator.alloc(u8, Entries.max_size) catch return .{ .refused = OUT_OF_MEMORY };
            defer shard.allocator.free(page_buf);
            const v = ownPage(shard, op, req.namespace, req.key, if (lc.len == 0) null else lc, skip, limit, room, page_buf) catch |err|
                return .{ .refused = refusal(err) };
            const size = Response.encodedSize(v) catch return .{ .refused = OUT_OF_MEMORY };
            own = shard.allocator.alloc(u8, size) catch return .{ .refused = OUT_OF_MEMORY };
            _ = Response.encodeInto(own.?, v) catch unreachable; // sized above
            parts[i] = .{ .owner = s, .body = own.? };
        } else {
            const body = Request.encodeInto(&bodies[i], .{ .op = op, .cursor = lc, .skip = skip, .limit = limit, .room = room }) catch return .{ .refused = OUT_OF_MEMORY };
            parts[i] = .{ .owner = s, .body = body, .op = @backingInt(OpCode.system_walk_page) };
        }
    }
    gather_owns_state = true;
    shard.startGather(conn, req, parts[0..n], &Walk.gather_op, state) catch |err| return .{ .refused = switch (err) {
        error.GatherBusy => .{ .status = .overloaded, .refusal = .{ .reason = .shard_busy, .ran = .no, .message = "overloaded: too many requests across shards are under way; this request was not run — retry" } },
        error.OutOfMemory => OUT_OF_MEMORY,
    } };
    return .asked;
}

/// A list's empty last page: [count:u32 = 0][has_more:u8 = 0][cursor_len:u16 = 0].
pub fn answerEmpty(shard: *Shard, conn: *Connection, request_id: u64) void {
    const empty: [7]u8 = @splat(0);
    shard.sendOkResponse(conn, request_id, &empty);
}

fn outOfMemoryFor(shard: *Shard, conn: *Connection, request_id: u64) void {
    shard.sendRefusal(conn, request_id, OUT_OF_MEMORY.status, OUT_OF_MEMORY.refusal);
}
