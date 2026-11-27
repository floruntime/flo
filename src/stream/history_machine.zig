//! A run's history on the contract: where each run's events are in the
//! log, a fixed number of rows per run however long it ran. The events
//! themselves stay in the log, in the `history_chunk` messages its applies
//! sent its own group on the history lane (`channels.Lane.history`); a row
//! here locates one by its inbound's index and the message's place in it,
//! which is unique (`channels.zig`).
//!
//! Two families, one per producer, so each takes its producer's budget
//! class: `history.wf` (a workflow's runs) and `history.act` (an action's).
//! Each keys a run's rows by its incarnation: its owner (the workflow or
//! action, by name hash), its run id and the stamp it was made at, so a
//! run id chosen again never meets an earlier run's rows.
//!
//! - `L` rows, one per landed chunk while the run is live, oldest first,
//!   each a locator. At 16, one `history_index` folds them into one row
//!   that lists them: sent to this group itself, so its seq comes after
//!   the chunks already on the lane, and serialized by a marker (`P`), so
//!   only one fold is in flight per run.
//! - `F`, kept from the run's last chunk (`final`) until its rows have
//!   folded to one: when it ended, and how often a fold was lost.
//! - `E`, one per ended run, keyed by its owner and when it ended so
//!   retention walks a workflow's or action's ended runs oldest first: its
//!   one remaining row's locator.
//!
//! - `O`, a deleted run whose drop was lost three times: its rows stay,
//!   unreachable once its run is gone, and are counted until a later drop
//!   of that incarnation (a bulk delete) takes them and this row.
//!
//! A chunk or fold a repair skipped is recorded, not dropped: a skipped
//! chunk lands as a `lost` row for its events, so reads say they're gone;
//! a skipped fold leaves its rows as they were. A run whose fold is lost
//! three times ends pointing at its live rows as they are (`mode` live):
//! correct, but not one row. A skipped drop is sent again, its count in
//! its own header, up to three times; then its run is marked `O`.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const schema = @import("../protocol/schema.zig");
const he = @import("../protocol/history_entries.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const router = @import("../node/router.zig");
const plant = @import("../plant.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const View = view_mod.View;
const ReadView = view_mod.ReadView;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const NamespaceId = router.NamespaceId;

/// One row's value, whichever kind its key's tag says: a live row's
/// locator; a marker's nothing but its owner; a final row's end and lost
/// folds; an ended run's end and remaining row.
pub const Row = schema.Record(&.{
    // The owner's name as stored: a reader filters on it, as two owners'
    // names can hash alike.
    .{ .name = "owner", .T = schema.Bytes(he.MAX_OWNER) },
    .{ .name = "ual_index", .T = Int(u64) },
    .{ .name = "part", .T = Int(u8) },
    .{ .name = "first_seq", .T = Int(u64) },
    .{ .name = "last_seq", .T = Int(u64) },
    .{ .name = "byte_len", .T = Int(u32) },
    .{ .name = "kind", .T = Int(u8) },
    .{ .name = "completed_ms", .T = Int(u64) },
    .{ .name = "retries", .T = Int(u8) },
    .{ .name = "mode", .T = Int(u8) },
    // The log bytes this row keeps referenced: a chunk's whole message, a
    // fold's own and every row it folded, an ended run's last row's; an
    // orphan mark's, every row its run holds, kept current as later chunks
    // and folds land. Markers and final rows hold none. A namespace's usage
    // is the sum over its rows.
    .{ .name = "total", .T = Int(u64) },
});

/// A namespace's history on this shard, as its rows count it: the bytes
/// its live and ended rows reference (orphaned runs' rows among them), and
/// its orphaned runs with their bytes.
pub const Usage = schema.Record(&.{
    .{ .name = "bytes", .T = Int(u64) },
    .{ .name = "orphans", .T = Int(u32) },
    .{ .name = "orphan_bytes", .T = Int(u64) },
});

/// How an ended run's row points at its history.
pub const Mode = enum(u8) {
    /// At its one remaining row, whose locator `E` carries.
    one = 0,
    /// At its live rows, left as they were: its folds were lost.
    live = 1,
};

pub const Workflows = view_mod.Family(70, "history.wf", .id, delivery.Stored(Row));
pub const Actions = view_mod.Family(71, "history.act", .id, delivery.Stored(Row));
/// One row per namespace, keyed by the namespace alone: both families'.
pub const UsageFam = view_mod.Family(76, "history.usage", .id, delivery.Stored(Usage));

/// How many times a run's last fold may be lost before it ends with its
/// live rows as they are.
pub const MAX_FOLD_RETRIES = 3;

/// How many times a run's drop may be lost before its rows are marked
/// orphaned. A drop's resend count rides in its header's `first_seq`,
/// which a drop doesn't otherwise read: its rows may be gone, so nothing
/// else could keep it.
pub const MAX_DROP_RETRIES = 3;

// ─── Keys ────────────────────────────────────────────────────────────────

pub const Tag = enum(u8) { live = 'L', marker = 'P', final = 'F', ended = 'E', orphan = 'O' };

/// A run's incarnation as its keys name it, after the tag: its owner's
/// name hash, its run id (length first, so no run's key is another's
/// prefix) and the stamp it was made at.
fn incarnationLen(run: []const u8) usize {
    return 8 + 2 + run.len + 8;
}

fn writeIncarnation(buf: []u8, owner: []const u8, run: []const u8, created_ns: u64) []u8 {
    buf[0..8].* = ownerHash(owner);
    std.mem.writeInt(u16, buf[8..10], @intCast(run.len), .big);
    @memcpy(buf[10..][0..run.len], run);
    std.mem.writeInt(u64, buf[10 + run.len ..][0..8], created_ns, .big);
    return buf[0..incarnationLen(run)];
}

pub fn ownerHash(owner: []const u8) [8]u8 {
    var h: [8]u8 = undefined;
    std.mem.writeInt(u64, &h, std.hash.Wyhash.hash(0x4157, owner), .big);
    return h;
}

/// The key after a namespace for a run's `tag` row (not an ended one's),
/// in `a`: the tag, then the incarnation.
fn runKey(a: std.mem.Allocator, tag: Tag, owner: []const u8, run: []const u8, created_ns: u64) error{OutOfMemory}![]u8 {
    const buf = try a.alloc(u8, 1 + incarnationLen(run));
    buf[0] = @backingInt(tag);
    _ = writeIncarnation(buf[1..], owner, run, created_ns);
    return buf;
}

/// A live row's key after a namespace: the run's, then where the row's
/// payload is, so a run's rows sort in landing order.
fn liveKey(a: std.mem.Allocator, owner: []const u8, run: []const u8, created_ns: u64, ual_index: u64, part: u8) error{OutOfMemory}![]u8 {
    const base = try runKey(a, .live, owner, run, created_ns);
    const buf = try a.alloc(u8, base.len + 9);
    @memcpy(buf[0..base.len], base);
    std.mem.writeInt(u64, buf[base.len..][0..8], ual_index, .big);
    buf[base.len + 8] = part;
    return buf;
}

/// An ended run's key after a namespace: its owner's hash, when it ended,
/// then the run's id and stamp, so an owner's ended runs sort oldest first.
fn endedKey(a: std.mem.Allocator, owner: []const u8, run: []const u8, created_ns: u64, completed_ms: u64) error{OutOfMemory}![]u8 {
    const buf = try a.alloc(u8, 1 + 8 + 8 + 2 + run.len + 8);
    buf[0] = @backingInt(Tag.ended);
    buf[1..9].* = ownerHash(owner);
    std.mem.writeInt(u64, buf[9..17], completed_ms, .big);
    std.mem.writeInt(u16, buf[17..19], @intCast(run.len), .big);
    @memcpy(buf[19..][0..run.len], run);
    std.mem.writeInt(u64, buf[19 + run.len ..][0..8], created_ns, .big);
    return buf;
}

/// An orphan mark's whole key, for a test that plants one.
pub fn orphanRowKey(a: std.mem.Allocator, ns: NamespaceId, owner: []const u8, run: []const u8, created_ns: u64) error{OutOfMemory}![]u8 {
    if (!@import("builtin").is_test) @compileError("orphanRowKey is for tests");
    return inNs(a, ns, try runKey(a, .orphan, owner, run, created_ns));
}

/// A live row's whole key, for a test that plants one.
pub fn liveRowKey(a: std.mem.Allocator, ns: NamespaceId, owner: []const u8, run: []const u8, created_ns: u64, ual_index: u64, part: u8) error{OutOfMemory}![]u8 {
    if (!@import("builtin").is_test) @compileError("liveRowKey is for tests");
    return inNs(a, ns, try liveKey(a, owner, run, created_ns, ual_index, part));
}

fn inNs(a: std.mem.Allocator, ns: NamespaceId, k: []const u8) error{OutOfMemory}![]u8 {
    const buf = try a.alloc(u8, 4 + k.len);
    return view_mod.idKey(buf, ns, k) catch unreachable;
}

// ─── Entries ─────────────────────────────────────────────────────────────

fn typeOf(e: *const Entry) EntryType {
    return @fromBackingInt(@intCast(e.header.entry_type));
}

/// Any history entry's header: the fields every one leads with.
pub const Head = struct {
    ns: NamespaceId,
    family: he.Family,
    owner: []const u8,
    run: []const u8,
    created_ns: u64,
    first_seq: u64,
    last_seq: u64,
    final: bool,
    completed_ms: u64,
};

fn headOf(v: anytype) ?Head {
    const family = std.enums.fromInt(he.Family, v.family) orelse return null;
    if (v.ns == 0 or v.owner.len == 0 or v.run.len == 0) return null;
    return .{ .ns = .of(v.ns), .family = family, .owner = v.owner, .run = v.run, .created_ns = v.created_ns, .first_seq = v.first_seq, .last_seq = v.last_seq, .final = v.final, .completed_ms = v.completed_ms };
}

/// An entry's header, or null for one that doesn't decode or names no run.
pub fn headOfEntry(e: *const Entry) ?Head {
    var diag: schema.Diagnostic = .{};
    return switch (typeOf(e)) {
        .history_chunk => headOf(he.Chunk.decodeAll(e.payload, &diag) catch return null),
        .history_index => headOf(he.Index.decodeAll(e.payload, &diag) catch return null),
        .history_drop => headOf(he.Drop.decodeAll(e.payload, &diag) catch return null),
        else => null,
    };
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const h = headOfEntry(e) orelse return;
    const a = out.allocator;
    const fam: u16 = familyId(h.family);
    out.addRangeIn(fam, h.ns, try runKey(a, .live, h.owner, h.run, h.created_ns)) catch |err| switch (err) {
        error.NoNamespace => return,
        else => |o| return o,
    };
    try out.addIn(fam, h.ns, try runKey(a, .marker, h.owner, h.run, h.created_ns));
    try out.addIn(fam, h.ns, try runKey(a, .final, h.owner, h.run, h.created_ns));
    try out.addIn(fam, h.ns, try runKey(a, .orphan, h.owner, h.run, h.created_ns));
    // The ended row a run moves to is keyed by when it ended, which a fold
    // landing after its last chunk reads from the final row, not its own
    // header: the owner's ended rows, all of them, as a range.
    var eb: [9]u8 = undefined;
    eb[0] = @backingInt(Tag.ended);
    eb[1..9].* = ownerHash(h.owner);
    out.addRangeIn(fam, h.ns, &eb) catch unreachable;
    try out.addIn(UsageFam.ID, h.ns, "");
}

fn familyId(f: he.Family) u16 {
    return switch (f) {
        .workflow => Workflows.ID,
        .action => Actions.ID,
    };
}

/// Why a history message can't apply, or null: one its sender built wrong.
pub fn malformed(e: *const Entry) ?[]const u8 {
    _ = headOfEntry(e) orelse return "bad request: a history entry that names no run";
    if (typeOf(e) == .history_index) {
        var diag: schema.Diagnostic = .{};
        const v = he.Index.decodeAll(e.payload, &diag) catch unreachable;
        if (v.locators.count == 0) return "bad request: a history index that lists no rows";
    }
    return null;
}

fn commandNamespace(e: *const Entry) ?u32 {
    const h = headOfEntry(e) orelse return null;
    return h.ns.int();
}

// ─── Apply ───────────────────────────────────────────────────────────────

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    // Only a run's own apply sends these, on the history lane: one in a
    // client's entry, or as another kind of message, is a Flo bug.
    const msg = view.message orelse return error.Halt;
    if (e.header.flags & entry_mod.Flags.HISTORY == 0) return error.Halt;
    const h = headOfEntry(e) orelse return out.refuse(.bad_request, .malformed, "bad request: a history entry that names no run");
    // A run's history goes to its owner's slot, and a fold back to the slot
    // its chunk came to: one that came to another is misrouted or forged,
    // and would key rows a move of that slot couldn't find.
    if (router.nsKeySlot(h.ns, h.owner) != msg.slot) return error.Halt;
    const before = usageBytes(view, h.ns);
    try switch (h.family) {
        .workflow => applyIn(Workflows, view, e, msg, h),
        .action => applyIn(Actions, view, e, msg, h),
    };
    easedBy(view, h.ns, before, e.payload.len);
}

fn usageBytes(view: *View, ns: NamespaceId) u64 {
    var kb: [4]u8 = undefined;
    const k = view_mod.idKey(&kb, ns, "") catch unreachable;
    return if (view.get(UsageFam, k) catch null) |u| u.v.bytes else 0;
}

/// A history message's bytes leave the lane as it's taken: one whose apply
/// added less than them to its namespace's usage (a lost chunk, a fold left
/// unread or skipped, a drop) lowered the namespace's in-flight history,
/// which may ease its quota. A chunk or fold that lands adds exactly its
/// bytes, so ordinary traffic eases nothing.
fn easedBy(view: *View, ns: NamespaceId, before: u64, taken: u64) void {
    if (usageBytes(view, ns) < before +| taken) view.quota_eased.add(ns.int());
}

fn applyIn(comptime F: type, view: *View, e: *const Entry, msg: view_mod.Message, h: Head) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    switch (typeOf(e)) {
        .history_chunk => {
            const v = he.Chunk.decodeAll(e.payload, &diag) catch return error.Halt;
            try land(F, view, e, msg, h, .chunk, @intCast(v.records.len));
        },
        .history_index => {
            const v = he.Index.decodeAll(e.payload, &diag) catch return error.Halt;
            try fold(F, view, e, msg, h, v.locators);
        },
        .history_drop => try drop(F, view, h),
        else => return error.Halt,
    }
}

fn row(owner: []const u8) Row.Value {
    return .{ .owner = owner, .ual_index = 0, .part = 0, .first_seq = 0, .last_seq = 0, .byte_len = 0, .kind = 0, .completed_ms = 0, .retries = 0, .mode = 0, .total = 0 };
}

/// How a namespace's usage moves in one apply.
const Move = struct {
    add: u64 = 0,
    sub: u64 = 0,
    orphans_add: u32 = 0,
    orphans_sub: u32 = 0,
    orphan_bytes_add: u64 = 0,
    orphan_bytes_sub: u64 = 0,
};

fn moveUsage(view: *View, ns: NamespaceId, m: Move) ApplyError!void {
    const k = try inNs(view.arena, ns, "");
    var u: Usage.Value = if (try view.get(UsageFam, k)) |got| got.v else .{ .bytes = 0, .orphans = 0, .orphan_bytes = 0 };
    // A subtract past 0 is drift a VOPR rule finds: counted, never a halt,
    // which every replica would hit again on replay.
    const drift = &view.history_usage_drift;
    u.bytes = subOrDrift(u64, drift, u.bytes +| m.add, m.sub);
    u.orphans = subOrDrift(u32, drift, u.orphans +| m.orphans_add, m.orphans_sub);
    u.orphan_bytes = subOrDrift(u64, drift, u.orphan_bytes +| m.orphan_bytes_add, m.orphan_bytes_sub);
    if (u.bytes == 0 and u.orphans == 0 and u.orphan_bytes == 0) {
        if (try view.get(UsageFam, k) != null) try view.delete(UsageFam, k);
        return;
    }
    try view.put(UsageFam, k, .{ .v = u });
}

fn subOrDrift(comptime T: type, drift: *u64, have: T, take: T) T {
    if (take > have) {
        drift.* += 1;
        return 0;
    }
    return have - take;
}

/// A chunk's row, or a lost one for its events: the run's next live row,
/// then the fold or end it may make due.
fn land(comptime F: type, view: *View, e: *const Entry, msg: view_mod.Message, h: Head, kind: he.Kind, byte_len: u32) ApplyError!void {
    const a = view.arena;
    var r = row(h.owner);
    // A lost chunk's row references nothing a read can return.
    r.total = if (kind == .lost) 0 else e.payload.len;
    r.ual_index = e.header.index;
    r.part = msg.ordinal + 1;
    r.first_seq = h.first_seq;
    r.last_seq = h.last_seq;
    r.byte_len = byte_len;
    r.kind = @backingInt(kind);
    try view.put(F, try inNs(a, h.ns, try liveKey(a, h.owner, h.run, h.created_ns, r.ual_index, r.part)), .{ .v = r });
    try moveUsage(view, h.ns, .{ .add = r.total });
    try orphanGrows(F, view, h, r.total);
    if (h.final) {
        var f = row(h.owner);
        f.completed_ms = h.completed_ms;
        try view.put(F, try inNs(a, h.ns, try runKey(a, .final, h.owner, h.run, h.created_ns)), .{ .v = f });
    }
    try settle(F, view, msg, h);
}

/// A fold landed: the rows it lists become the one row it is, if its run
/// still waits on it. One for a run dropped since, or whose marker a lost
/// fold cleared, changes nothing: its message is left unread in the log.
fn fold(comptime F: type, view: *View, e: *const Entry, msg: view_mod.Message, h: Head, locators: he.Locators.Value) ApplyError!void {
    const a = view.arena;
    const marker = try inNs(a, h.ns, try runKey(a, .marker, h.owner, h.run, h.created_ns));
    if (try view.get(F, marker) == null) return;
    try view.delete(F, marker);
    var first: u64 = std.math.maxInt(u64);
    var last: u64 = 0;
    var folded: u64 = 0;
    var it = locators.iterator();
    while (it.next() catch return error.Halt) |l| {
        const k = try inNs(a, h.ns, try liveKey(a, h.owner, h.run, h.created_ns, l.ual_index, l.part));
        // Each row it lists is one the marker kept from folding twice.
        const got = (try view.get(F, k)) orelse return error.Halt;
        folded +|= got.v.total;
        try view.delete(F, k);
        if (l.kind != @backingInt(he.Kind.lost) or l.last_seq != 0) {
            first = @min(first, l.first_seq);
            last = @max(last, l.last_seq);
        }
    }
    var r = row(h.owner);
    r.ual_index = e.header.index;
    r.part = msg.ordinal + 1;
    r.first_seq = if (first == std.math.maxInt(u64)) 0 else first;
    r.last_seq = last;
    r.byte_len = @intCast(e.payload.len);
    r.kind = @backingInt(he.Kind.index);
    // The rows it folded move into it; only its own message is new.
    r.total = (if (comptime plant.active == .history_fold_total_dropped) 0 else folded) +| e.payload.len;
    try view.put(F, try inNs(a, h.ns, try liveKey(a, h.owner, h.run, h.created_ns, r.ual_index, r.part)), .{ .v = r });
    try moveUsage(view, h.ns, .{ .add = e.payload.len });
    try orphanGrows(F, view, h, e.payload.len);
    try settle(F, view, msg, h);
}

/// What a run's rows need next, with no fold in flight: an ended run's
/// move to one row, by as many folds as it takes, or a live run's fold of
/// its oldest 16.
fn settle(comptime F: type, view: *View, msg: view_mod.Message, h: Head) ApplyError!void {
    const a = view.arena;
    if (try view.get(F, try inNs(a, h.ns, try runKey(a, .marker, h.owner, h.run, h.created_ns))) != null) return;
    const final_key = try inNs(a, h.ns, try runKey(a, .final, h.owner, h.run, h.created_ns));
    const final = try view.get(F, final_key);
    var rows: [he.INDEX_FANOUT]Row.Value = undefined;
    var n: usize = 0;
    var more = false;
    var it = try view.range(F, try inNs(a, h.ns, try runKey(a, .live, h.owner, h.run, h.created_ns)));
    while (try it.next()) |item| {
        if (n == rows.len) {
            more = true;
            break;
        }
        rows[n] = item.value.v;
        n += 1;
    }
    if (final) |f| {
        if (n == 0) return error.Halt;
        if (n == 1 and !more) return end(F, view, h, f.v.completed_ms, rows[0]);
        return sendFold(F, view, msg, h, f.v.completed_ms, rows[0..n]);
    }
    if (n == rows.len) try sendFold(F, view, msg, h, h.completed_ms, rows[0..n]);
}

/// Folds `rows` into one: a `history_index` this group sends itself, on
/// the history lane to the slot its chunks came to, and the marker that
/// holds any other fold of the run until it lands.
fn sendFold(comptime F: type, view: *View, msg: view_mod.Message, h: Head, completed_ms: u64, rows: []const Row.Value) ApplyError!void {
    const a = view.arena;
    var lb: [he.Locators.max_size]u8 = undefined;
    var w: schema.Writer = .{ .buf = &lb };
    for (rows) |r| he.Locator.encode(&w, .{ .ual_index = r.ual_index, .part = r.part, .first_seq = r.first_seq, .last_seq = r.last_seq, .byte_len = r.byte_len, .kind = r.kind }) catch return error.Halt;
    const buf = try a.alloc(u8, he.Index.max_size);
    const payload = he.Index.encodeInto(buf, .{ .ns = h.ns.int(), .family = @backingInt(h.family), .owner = h.owner, .run = h.run, .created_ns = h.created_ns, .first_seq = 0, .last_seq = 0, .final = false, .completed_ms = completed_ms, .locators = .{ .bytes = w.written(), .count = @intCast(rows.len) } }) catch return error.Halt;
    try view.sendFlagged(.user(h.ns), msg.slot, @backingInt(EntryType.history_index), entry_mod.Flags.HISTORY, payload);
    try view.put(F, try inNs(a, h.ns, try runKey(a, .marker, h.owner, h.run, h.created_ns)), .{ .v = row(h.owner) });
}

/// An ended run's move to its one ended row, pointing at `last`, its one
/// remaining row: the live and final rows go.
fn end(comptime F: type, view: *View, h: Head, completed_ms: u64, last: Row.Value) ApplyError!void {
    const a = view.arena;
    var r = last;
    r.owner = h.owner;
    r.completed_ms = completed_ms;
    r.mode = @backingInt(Mode.one);
    const ek = try inNs(a, h.ns, try endedKey(a, h.owner, h.run, h.created_ns, completed_ms));
    try releaseEnded(F, view, h.ns, ek);
    try view.put(F, ek, .{ .v = r });
    try view.delete(F, try inNs(a, h.ns, try liveKey(a, h.owner, h.run, h.created_ns, last.ual_index, last.part)));
    try view.delete(F, try inNs(a, h.ns, try runKey(a, .final, h.owner, h.run, h.created_ns)));
}

/// Every row of a run's incarnation, gone, and the bytes they referenced
/// off its namespace's usage. Its bytes stay in the log.
fn drop(comptime F: type, view: *View, h: Head) ApplyError!void {
    const a = view.arena;
    var m: Move = .{};
    var doomed: std.ArrayListUnmanaged([]const u8) = .empty;
    var it = try view.range(F, try inNs(a, h.ns, try runKey(a, .live, h.owner, h.run, h.created_ns)));
    while (try it.next()) |item| {
        try doomed.append(a, item.key);
        m.sub +|= item.value.v.total;
    }
    for (doomed.items) |k| try view.delete(F, k);
    try view.delete(F, try inNs(a, h.ns, try runKey(a, .marker, h.owner, h.run, h.created_ns)));
    try view.delete(F, try inNs(a, h.ns, try runKey(a, .final, h.owner, h.run, h.created_ns)));
    const ended = try inNs(a, h.ns, try endedKey(a, h.owner, h.run, h.created_ns, h.completed_ms));
    if (try view.get(F, ended)) |got| m.sub +|= got.v.total;
    try view.delete(F, ended);
    const orphan = try inNs(a, h.ns, try runKey(a, .orphan, h.owner, h.run, h.created_ns));
    if (try view.get(F, orphan)) |got| {
        m.orphans_sub = 1;
        m.orphan_bytes_sub = got.v.total;
    }
    try view.delete(F, orphan);
    if (comptime plant.active == .history_drop_unread) return;
    try moveUsage(view, h.ns, m);
}

/// A run marked orphaned whose rows reference `bytes` more (a chunk or fold
/// that landed after its mark): its mark and its namespace's orphaned bytes
/// grow with them.
fn orphanGrows(comptime F: type, view: *View, h: Head, bytes: u64) ApplyError!void {
    if (bytes == 0) return;
    const k = try inNs(view.arena, h.ns, try runKey(view.arena, .orphan, h.owner, h.run, h.created_ns));
    var o = (try view.get(F, k)) orelse return;
    o.v.total +|= bytes;
    try view.put(F, k, o);
    try moveUsage(view, h.ns, .{ .orphan_bytes_add = bytes });
}

/// What an ended row put at `key` replaces: one already there (a second
/// final chunk, which no producer sends) has its total taken off usage, so
/// a replace never counts twice.
fn releaseEnded(comptime F: type, view: *View, ns: NamespaceId, key: []const u8) ApplyError!void {
    if (try view.get(F, key)) |old| try moveUsage(view, ns, .{ .sub = old.v.total });
}

/// The bytes `h`'s run's rows still reference: its live rows and its ended
/// row, if any.
fn runTotal(comptime F: type, view: *View, h: Head) ApplyError!u64 {
    const a = view.arena;
    var total: u64 = 0;
    var it = try view.range(F, try inNs(a, h.ns, try runKey(a, .live, h.owner, h.run, h.created_ns)));
    while (try it.next()) |item| total +|= item.value.v.total;
    if (try view.get(F, try inNs(a, h.ns, try endedKey(a, h.owner, h.run, h.created_ns, h.completed_ms)))) |got| total +|= got.v.total;
    return total;
}

/// `h`'s drop, sent again to this group on the history lane, its resend
/// count `retries`.
fn sendDrop(view: *View, msg: view_mod.Message, h: Head, retries: u64) ApplyError!void {
    const buf = try view.arena.alloc(u8, he.Drop.max_size);
    const payload = he.Drop.encodeInto(buf, .{ .ns = h.ns.int(), .family = @backingInt(h.family), .owner = h.owner, .run = h.run, .created_ns = h.created_ns, .first_seq = retries, .last_seq = 0, .final = false, .completed_ms = h.completed_ms }) catch return error.Halt;
    try view.sendFlagged(.user(h.ns), msg.slot, @backingInt(EntryType.history_drop), entry_mod.Flags.HISTORY, payload);
}

// ─── A repair's skips ────────────────────────────────────────────────────

/// A history message a repair skipped: a chunk lands as a lost row for its
/// events, so a read says they're gone; a fold leaves its rows as they
/// were, and is sent again if its run has ended, up to its retries, after
/// which the run ends at its live rows; a drop changes nothing, and is
/// sent again up to its retries, after which its run is marked `O`.
fn skipped(view: *View, e: *const Entry, _: []const u8) ApplyError!void {
    const msg = view.message orelse return;
    const h = headOfEntry(e) orelse return;
    const before = usageBytes(view, h.ns);
    try switch (h.family) {
        .workflow => skippedIn(Workflows, view, e, msg, h),
        .action => skippedIn(Actions, view, e, msg, h),
    };
    easedBy(view, h.ns, before, e.payload.len);
}

fn skippedIn(comptime F: type, view: *View, e: *const Entry, msg: view_mod.Message, h: Head) ApplyError!void {
    const a = view.arena;
    switch (typeOf(e)) {
        .history_chunk => try land(F, view, e, msg, h, .lost, 0),
        .history_index => {
            const marker = try inNs(a, h.ns, try runKey(a, .marker, h.owner, h.run, h.created_ns));
            if (try view.get(F, marker) == null) return;
            try view.delete(F, marker);
            const final_key = try inNs(a, h.ns, try runKey(a, .final, h.owner, h.run, h.created_ns));
            const final = (try view.get(F, final_key)) orelse return;
            var f = final.v;
            f.retries += 1;
            if (f.retries < MAX_FOLD_RETRIES) {
                try view.put(F, final_key, .{ .v = f });
                return settle(F, view, msg, h);
            }
            // Past its retries: the run ends at its live rows as they are.
            var r = row(h.owner);
            r.completed_ms = f.completed_ms;
            r.mode = @backingInt(Mode.live);
            const ek = try inNs(a, h.ns, try endedKey(a, h.owner, h.run, h.created_ns, f.completed_ms));
            try releaseEnded(F, view, h.ns, ek);
            try view.put(F, ek, .{ .v = r });
            try view.delete(F, final_key);
        },
        .history_drop => {
            if (comptime plant.active == .history_drop_not_resent) return;
            if (h.first_seq < MAX_DROP_RETRIES - 1) return sendDrop(view, msg, h, h.first_seq + 1);
            // Past its retries: its rows stay, marked, until a later drop,
            // and are counted as orphaned. A mark already there (a later
            // drop lost too) counts nothing again.
            const orphan = try inNs(a, h.ns, try runKey(a, .orphan, h.owner, h.run, h.created_ns));
            if (try view.get(F, orphan) != null) return;
            var o = row(h.owner);
            o.completed_ms = h.completed_ms;
            o.retries = MAX_DROP_RETRIES;
            o.total = try runTotal(F, view, h);
            try view.put(F, orphan, .{ .v = o });
            try moveUsage(view, h.ns, .{ .orphans_add = 1, .orphan_bytes_add = o.total });
        },
        else => {},
    }
}

pub const machine: machine_mod.Machine = .{
    .name = "history",
    .entry_types = &.{ .history_chunk, .history_index, .history_drop },
    .families = &.{ machine_mod.info(Workflows), machine_mod.info(Actions), machine_mod.info(UsageFam) },
    .keys = keys,
    .apply = apply,
    .malformed = malformed,
    .namespace = commandNamespace,
    .skipped = skipped,
};

// ─── Reads ───────────────────────────────────────────────────────────────

/// A run's history rows, oldest first, as a read walks them: its ended
/// row's one locator, or its live rows (an ended run whose folds were
/// lost, or a run not ended yet). Index rows list rows of their own,
/// which the reader resolves from the log.
pub const Rows = struct {
    family: he.Family,
    one: ?he.Locator.Value = null,
    owner: []const u8,
    it: ?view_mod.ReadRange(Workflows) = null,
    it_act: ?view_mod.ReadRange(Actions) = null,

    pub fn next(self: *Rows) ?he.Locator.Value {
        if (self.one) |l| {
            self.one = null;
            return l;
        }
        while (true) {
            const v = self.nextRow() orelse return null;
            if (!std.mem.eql(u8, v.owner, self.owner)) continue;
            return locatorOf(v);
        }
    }

    fn nextRow(self: *Rows) ?Row.Value {
        if (self.it) |*i| {
            const item = (i.next() catch null) orelse return null;
            return item.value.v;
        }
        if (self.it_act) |*i| {
            const item = (i.next() catch null) orelse return null;
            return item.value.v;
        }
        return null;
    }
};

fn locatorOf(v: Row.Value) he.Locator.Value {
    return .{ .ual_index = v.ual_index, .part = v.part, .first_seq = v.first_seq, .last_seq = v.last_seq, .byte_len = v.byte_len, .kind = v.kind };
}

/// `run`'s rows of incarnation `created_ns`, with `completed_ms` its end if
/// it has ended (its summary keeps that), or null for a live run.
pub fn rowsOf(read: ReadView, a: std.mem.Allocator, family: he.Family, ns: NamespaceId, owner: []const u8, run: []const u8, created_ns: u64, completed_ms: ?u64) error{OutOfMemory}!Rows {
    var rows: Rows = .{ .family = family, .owner = owner };
    if (completed_ms) |at| {
        const k = try inNs(a, ns, try endedKey(a, owner, run, created_ns, at));
        const got = switch (family) {
            .workflow => read.get(Workflows, k) catch null,
            .action => read.get(Actions, k) catch null,
        };
        if (got) |g| if (std.mem.eql(u8, g.v.owner, owner) and g.v.mode == @backingInt(Mode.one)) {
            rows.one = locatorOf(g.v);
            return rows;
        };
    }
    const prefix = try inNs(a, ns, try runKey(a, .live, owner, run, created_ns));
    switch (family) {
        .workflow => rows.it = read.range(Workflows, prefix),
        .action => rows.it_act = read.range(Actions, prefix),
    }
    return rows;
}

/// `run`'s ended row of incarnation `created_ns`, ended at `completed_ms`,
/// if it has one and it is `run`'s owner's.
pub fn endedRow(read: ReadView, a: std.mem.Allocator, family: he.Family, ns: NamespaceId, owner: []const u8, run: []const u8, created_ns: u64, completed_ms: u64) ?Row.Value {
    const k = inNs(a, ns, endedKey(a, owner, run, created_ns, completed_ms) catch return null) catch return null;
    const got = switch (family) {
        .workflow => read.get(Workflows, k) catch null,
        .action => read.get(Actions, k) catch null,
    };
    const g = got orelse return null;
    return if (std.mem.eql(u8, g.v.owner, owner)) g.v else null;
}

/// The orphan mark of `run`'s incarnation `created_ns`: its drop was lost
/// past its retries, so its rows stay. Null for none.
pub fn orphanRow(read: ReadView, a: std.mem.Allocator, family: he.Family, ns: NamespaceId, owner: []const u8, run: []const u8, created_ns: u64) ?Row.Value {
    const k = inNs(a, ns, runKey(a, .orphan, owner, run, created_ns) catch return null) catch return null;
    const got = switch (family) {
        .workflow => read.get(Workflows, k) catch null,
        .action => read.get(Actions, k) catch null,
    };
    const g = got orelse return null;
    return if (std.mem.eql(u8, g.v.owner, owner)) g.v else null;
}

/// An orphan mark found under an owner: the incarnation it marks and
/// when that run ended.
pub const Orphan = struct { run: []const u8, created_ns: u64, completed_ms: u64 };

/// The most of an owner's orphans `orphansOf` counts past those it returns.
pub const MAX_ORPHANS_COUNTED: u32 = 1024;

/// Up to `out.len` of `owner`'s orphan marks in `ns`, as committed, in key
/// order; and how many more of the owner's it found and left. Filtered on
/// the owner each mark stores, as two names can hash alike. `skip` says
/// which to leave out (one whose drop is already on its way).
pub fn orphansOf(read: ReadView, a: std.mem.Allocator, family: he.Family, ns: NamespaceId, owner: []const u8, out: []Orphan, skip: anytype) error{OutOfMemory}!struct { n: usize, left: u32 } {
    var pb: [1 + 8]u8 = undefined;
    pb[0] = @backingInt(Tag.orphan);
    pb[1..9].* = ownerHash(owner);
    const prefix = try inNs(a, ns, &pb);
    var n: usize = 0;
    var left: u32 = 0;
    switch (family) {
        inline else => |f| {
            const F = if (f == .workflow) Workflows else Actions;
            var it = read.range(F, prefix);
            while (it.next() catch null) |item| {
                if (!std.mem.eql(u8, item.value.v.owner, owner)) continue;
                // `4 ‖ 'O' ‖ hash ‖ len ‖ run ‖ created_ns`, as `runKey` wrote it.
                const k = item.key[prefix.len..];
                if (k.len < 2 + 8) continue;
                const len = std.mem.readInt(u16, k[0..2], .big);
                if (k.len != 2 + len + 8) continue;
                const o: Orphan = .{ .run = k[2..][0..len], .created_ns = std.mem.readInt(u64, k[2 + len ..][0..8], .big), .completed_ms = item.value.v.completed_ms };
                if (skip.has(o)) continue;
                if (n < out.len) {
                    out[n] = .{ .run = try a.dupe(u8, o.run), .created_ns = o.created_ns, .completed_ms = o.completed_ms };
                    n += 1;
                } else {
                    left +|= 1;
                    // Enough to say some are left: a count past it is no
                    // more use to the caller, and the walk stays bounded.
                    if (left >= MAX_ORPHANS_COUNTED) break;
                }
            }
        },
    }
    return .{ .n = n, .left = left };
}

/// The orphans a leader's bulk deletes have sent a drop for and whose mark
/// is still there: none is proposed again while its drop is on the lane.
/// Each bulk delete first prunes the ones whose mark is gone (the drop
/// landed), so `MAX` bounds drops in flight; at `MAX` a bulk delete takes
/// no orphans and counts them as left. Leader memory, cleared at a new term.
///
/// A drop can't be lost within a term: it's sent from a committed apply
/// into the logged outbox and delivered in order on the history lane. Only
/// a repair skip loses one, and the history machine resends a skipped drop
/// itself before it leaves the orphan marked. So clearing at a new term is
/// a backstop for a mark that outlived its drop, not how one is retried.
/// A hash two orphans share holds both back until the one sent lands.
pub const OrphansSent = struct {
    term: u64 = 0,
    /// Each sent orphan's mark key, by its hash.
    rows: std.AutoHashMapUnmanaged(u64, []u8) = .empty,

    pub const MAX = 4096;

    pub fn deinit(self: *OrphansSent, a: std.mem.Allocator) void {
        self.clear(a);
        self.rows.deinit(a);
    }

    fn clear(self: *OrphansSent, a: std.mem.Allocator) void {
        var it = self.rows.valueIterator();
        while (it.next()) |k| a.free(k.*);
        self.rows.clearRetainingCapacity();
    }

    /// Forget what an earlier term sent.
    pub fn at(self: *OrphansSent, a: std.mem.Allocator, term: u64) void {
        if (self.term == term) return;
        self.term = term;
        self.clear(a);
    }

    /// Forget those whose mark is gone from `read`: their drop landed.
    pub fn prune(self: *OrphansSent, a: std.mem.Allocator, read: ReadView, family: he.Family) error{OutOfMemory}!void {
        var gone: std.ArrayListUnmanaged(u64) = .empty;
        defer gone.deinit(a);
        var it = self.rows.iterator();
        while (it.next()) |e| {
            const marked = switch (family) {
                inline else => |f| (read.get(if (f == .workflow) Workflows else Actions, e.value_ptr.*) catch continue) != null,
            };
            if (!marked) try gone.append(a, e.key_ptr.*);
        }
        for (gone.items) |h| if (self.rows.fetchRemove(h)) |kv| a.free(kv.value);
    }

    pub fn full(self: *const OrphansSent) bool {
        return self.rows.count() >= MAX;
    }

    pub fn has(self: *const OrphansSent, ns: NamespaceId, owner: []const u8, o: Orphan) bool {
        return self.rows.contains(id(ns, owner, o));
    }

    pub fn note(self: *OrphansSent, a: std.mem.Allocator, ns: NamespaceId, owner: []const u8, o: Orphan) error{OutOfMemory}!void {
        if (self.full()) return;
        const gop = try self.rows.getOrPut(a, id(ns, owner, o));
        if (gop.found_existing) return;
        errdefer self.rows.removeByPtr(gop.key_ptr);
        const rk = try runKey(a, .orphan, owner, o.run, o.created_ns);
        defer a.free(rk);
        gop.value_ptr.* = try inNs(a, ns, rk);
    }

    fn id(ns: NamespaceId, owner: []const u8, o: Orphan) u64 {
        var h = std.hash.Wyhash.init(0x4f52);
        h.update(std.mem.asBytes(&ns.int()));
        h.update(owner);
        h.update(&.{0});
        h.update(o.run);
        h.update(std.mem.asBytes(&o.created_ns));
        return h.final();
    }
};

/// Whether `run`'s incarnation `created_ns` holds any row: live, marker,
/// final, orphan, or ended at `completed_ms`.
pub fn holdsAny(read: ReadView, a: std.mem.Allocator, family: he.Family, ns: NamespaceId, owner: []const u8, run: []const u8, created_ns: u64, completed_ms: u64) error{OutOfMemory}!bool {
    if (endedRow(read, a, family, ns, owner, run, created_ns, completed_ms) != null) return true;
    return switch (family) {
        .workflow => holdsAnyIn(Workflows, read, a, ns, owner, run, created_ns),
        .action => holdsAnyIn(Actions, read, a, ns, owner, run, created_ns),
    };
}

fn holdsAnyIn(comptime F: type, read: ReadView, a: std.mem.Allocator, ns: NamespaceId, owner: []const u8, run: []const u8, created_ns: u64) error{OutOfMemory}!bool {
    for ([_]Tag{ .live, .marker, .final, .orphan }) |tag| {
        var it = read.range(F, try inNs(a, ns, try runKey(a, tag, owner, run, created_ns)));
        if ((it.next() catch return true) != null) return true;
    }
    return false;
}

/// Why `F`'s rows break what a run's history keeps, or null: a run with
/// its ended row has no live, marker or final row besides. A chunk landing
/// after a run ended would make it live again.
pub fn ruleOf(comptime F: type, read: ReadView, a: std.mem.Allocator) error{OutOfMemory}!?[]const u8 {
    var it = read.range(F, "");
    while (it.next() catch return "a history row could not be read") |item| {
        const k = item.key;
        // `ns ‖ E ‖ owner hash ‖ completed_ms ‖ run len ‖ run ‖ created_ns`
        if (k.len < 4 + 1 + 8 + 8 + 2 or k[4] != @backingInt(Tag.ended)) continue;
        const run_len = std.mem.readInt(u16, k[21..23], .big);
        if (k.len != 23 + run_len + 8) return "an ended history row's key is not one";
        // The run's own rows: its tag, then its incarnation.
        for ([_]Tag{ .live, .marker, .final }) |tag| {
            const prefix = try a.alloc(u8, 4 + 1 + 8 + 2 + run_len + 8);
            @memcpy(prefix[0..4], k[0..4]);
            prefix[4] = @backingInt(tag);
            @memcpy(prefix[5..13], k[5..13]);
            @memcpy(prefix[13..], k[21..]);
            var rows = read.range(F, prefix);
            if ((rows.next() catch return "a history row could not be read") != null) return "a run with its ended row has live, marker or final rows besides";
        }
    }
    return null;
}

/// A namespace's usage row on this shard, or zeros for none.
pub fn usageOf(read: ReadView, ns: NamespaceId) Usage.Value {
    var kb: [4]u8 = undefined;
    const k = view_mod.idKey(&kb, ns, "") catch unreachable;
    const got = read.get(UsageFam, k) catch null;
    return if (got) |g| g.v else .{ .bytes = 0, .orphans = 0, .orphan_bytes = 0 };
}

/// Why the usage rows aren't what the history rows sum to, or null: each
/// namespace's bytes are its live and ended rows' totals over both
/// families, and its orphans its orphan marks and their totals.
pub fn usageRule(read: ReadView, a: std.mem.Allocator) error{OutOfMemory}!?[]const u8 {
    var sums: std.AutoHashMapUnmanaged(u32, Usage.Value) = .empty;
    inline for (.{ Workflows, Actions }) |F| {
        var it = read.range(F, "");
        while (it.next() catch return "a history row could not be read") |item| {
            const k = item.key;
            if (k.len < 5) return "a history row's key is not one";
            const ns = std.mem.readInt(u32, k[0..4], .big);
            const e = try sums.getOrPut(a, ns);
            if (!e.found_existing) e.value_ptr.* = .{ .bytes = 0, .orphans = 0, .orphan_bytes = 0 };
            switch (k[4]) {
                @backingInt(Tag.live), @backingInt(Tag.ended) => e.value_ptr.bytes +|= item.value.v.total,
                @backingInt(Tag.orphan) => {
                    e.value_ptr.orphans += 1;
                    e.value_ptr.orphan_bytes +|= item.value.v.total;
                },
                else => {},
            }
        }
    }
    var seen: usize = 0;
    var it = read.range(UsageFam, "");
    while (it.next() catch return "a usage row could not be read") |item| {
        if (item.key.len != 4) return "a usage row's key is not a namespace";
        const want = sums.get(std.mem.readInt(u32, item.key[0..4], .big)) orelse return "a namespace with no history rows has a usage row";
        seen += 1;
        const got = item.value.v;
        if (got.bytes != want.bytes) return "a namespace's history usage is not the sum of its rows' totals";
        if (got.orphans != want.orphans or got.orphan_bytes != want.orphan_bytes) return "a namespace's orphaned history is not its orphan marks'";
    }
    // A namespace whose rows sum to nothing keeps no row.
    var vit = sums.valueIterator();
    var nonzero: usize = 0;
    while (vit.next()) |v| {
        if (v.bytes != 0 or v.orphans != 0 or v.orphan_bytes != 0) nonzero += 1;
    }
    if (nonzero != seen) return "a namespace's history rows count bytes its usage row doesn't";
    return null;
}

/// The last seq `run`'s rows hold, landed or known lost, of incarnation
/// `created_ns` (`completed_ms` as for `rowsOf`); null for none landed. A
/// fold's row covers its rows' seqs, so this reads rows only, never the log.
pub fn landedThrough(read: ReadView, a: std.mem.Allocator, family: he.Family, ns: NamespaceId, owner: []const u8, run: []const u8, created_ns: u64, completed_ms: ?u64) ?u64 {
    var rows = rowsOf(read, a, family, ns, owner, run, created_ns, completed_ms) catch return null;
    var last: ?u64 = null;
    while (rows.next()) |l| last = @max(last orelse 0, l.last_seq);
    return last;
}

// ─── Tests ───────────────────────────────────────────────────────────────

const testing = std.testing;
const store_mod = @import("../contract/store.zig");
const channels = @import("../contract/channels.zig");
const ce = @import("../protocol/channel_entries.zig");

const Ch = channels.Channels(&.{machine});

/// One group of one, its history lane to itself: chunks sent as a run's
/// apply sends them, and every message on the lane delivered back, one
/// inbound each, as its driver would.
const H = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,
    /// The inbound seqs to skip, as a repair names them.
    skip: []const u64 = &.{},
    fn init() H {
        var t: H = .{ .store = store_mod.Store.init(testing.allocator), .scratch = std.heap.ArenaAllocator.init(testing.allocator) };
        t.store.place = .{ .group = 0, .count = 1 };
        t.store.strict_keys = true;
        return t;
    }

    fn deinit(t: *H) void {
        t.store.deinit();
        t.scratch.deinit();
    }

    fn read(t: *H) ReadView {
        return .{ .store = &t.store };
    }

    const Run = struct { family: he.Family = .action, ns: NamespaceId = .default, owner: []const u8 = "act", run: []const u8 = "r1", created_ns: u64 = 1 };

    /// Sends chunk `first..last` of `r`, as its apply would, `final` with
    /// its end at `completed_ms`.
    fn chunk(t: *H, r: Run, first: u64, last: u64, final: bool, completed_ms: u64) !void {
        var buf: [he.Chunk.max_size]u8 = undefined;
        const p = try he.Chunk.encodeInto(&buf, .{ .ns = r.ns.int(), .family = @backingInt(r.family), .owner = r.owner, .run = r.run, .created_ns = r.created_ns, .first_seq = first, .last_seq = last, .final = final, .completed_ms = completed_ms, .records = "events" });
        try t.sendOwn(r, .history_chunk, p);
    }

    fn dropOf(t: *H, r: Run, completed_ms: u64) !void {
        var buf: [he.Drop.max_size]u8 = undefined;
        const p = try he.Drop.encodeInto(&buf, .{ .ns = r.ns.int(), .family = @backingInt(r.family), .owner = r.owner, .run = r.run, .created_ns = r.created_ns, .first_seq = 0, .last_seq = 0, .final = false, .completed_ms = completed_ms });
        try t.sendOwn(r, .history_drop, p);
    }

    fn sendOwn(t: *H, r: Run, etype: EntryType, payload: []const u8) !void {
        t.index += 1;
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        var ks = KeySet.init(arena.allocator());
        var view = View.init(&t.store, arena.allocator(), &ks);
        try view.sendFlagged(.user(r.ns), @intCast(router.nsKeySlot(r.ns, r.owner)), @backingInt(etype), entry_mod.Flags.HISTORY, payload);
        try channels.writeSends(&view, t.index);
        try view.commit();
    }

    /// Every message on the history lane delivered, one inbound each, until
    /// none is left: what the folds send comes round too.
    fn pump(t: *H) !void {
        return t.pumpAtMost(std.math.maxInt(usize));
    }

    /// `pump`, delivering at most `n` messages.
    fn pumpAtMost(t: *H, n: usize) !void {
        var left = n;
        while (left > 0) : (left -= 1) {
            const next = channels.metaOf(t.read(), 0, .history).next_seq;
            const applied = if (channels.inOf(t.read(), 0, .history)) |i| i.applied else 0;
            if (applied + 1 >= next) return;
            const seq = applied + 1;
            var kb: [channels.OUT_KEY]u8 = undefined;
            const o = (try t.read().get(channels.OutFam, channels.outKey(&kb, 0, .history, seq))).?.v;
            var mb: [he.Chunk.max_size + 64]u8 = undefined;
            var w: schema.Writer = .{ .buf = &mb };
            try ce.Message.encode(&w, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = 0, .flags = o.flags, .origin_kind = o.origin_kind, .origin_ns = o.origin_ns, .payload = o.payload });
            var ib: [he.Chunk.max_size + 128]u8 = undefined;
            const inbound = try ce.Inbound.encodeInto(&ib, .{ .from = 0, .lane = @backingInt(channels.Lane.history), .first_seq = seq, .messages = .{ .bytes = w.written(), .count = 1 } });
            t.index += 1;
            var h = std.mem.zeroes(entry_mod.Header);
            h.entry_type = @backingInt(EntryType.channel_inbound);
            h.index = t.index;
            h.payload_len = @intCast(inbound.len);
            const e: Entry = .{ .header = h, .payload = inbound };
            if (std.mem.indexOfScalar(u64, t.skip, seq) != null) t.store.inbound_skip = .all(t.index);
            defer t.store.inbound_skip = null;
            var res = Result.start(&e);
            try machine_mod.applyEntry(&Ch.machine, &t.store, &t.scratch, &e, &res);
        }
    }

    /// How many messages of `etype` this group has sent itself.
    fn sent(t: *H, etype: EntryType) usize {
        var n: usize = 0;
        const next = channels.metaOf(t.read(), 0, .history).next_seq;
        var seq: u64 = channels.metaOf(t.read(), 0, .history).acked + 1;
        while (seq < next) : (seq += 1) {
            var kb: [channels.OUT_KEY]u8 = undefined;
            const o = (t.read().get(channels.OutFam, channels.outKey(&kb, 0, .history, seq)) catch null) orelse continue;
            if (o.v.entry_type == @backingInt(etype)) n += 1;
        }
        return n;
    }

    fn countTag(t: *H, r: Run, tag: Tag) usize {
        return switch (r.family) {
            .workflow => t.countTagIn(Workflows, r, tag),
            .action => t.countTagIn(Actions, r, tag),
        };
    }

    fn countTagIn(t: *H, comptime F: type, r: Run, tag: Tag) usize {
        var buf: [1 + 4 + 1 + 8 + 2 + he.MAX_RUN + 8]u8 = undefined;
        const k = runKey(testing.allocator, tag, r.owner, r.run, r.created_ns) catch unreachable;
        defer testing.allocator.free(k);
        const p = view_mod.idKey(&buf, r.ns, k) catch unreachable;
        var it = t.read().range(F, p);
        var n: usize = 0;
        while (it.next() catch null) |_| n += 1;
        return n;
    }

    fn ended(t: *H, r: Run, completed_ms: u64) ?Row.Value {
        const k = endedKey(testing.allocator, r.owner, r.run, r.created_ns, completed_ms) catch unreachable;
        defer testing.allocator.free(k);
        var buf: [4 + 1 + 8 + 8 + 2 + he.MAX_RUN + 8]u8 = undefined;
        const key = view_mod.idKey(&buf, r.ns, k) catch unreachable;
        const got = switch (r.family) {
            .workflow => t.read().get(Workflows, key) catch null,
            .action => t.read().get(Actions, key) catch null,
        };
        return if (got) |g| g.v else null;
    }

    /// The live rows of `r`, oldest first.
    fn live(t: *H, r: Run, out: []Row.Value) []Row.Value {
        var buf: [1 + 4 + 1 + 8 + 2 + he.MAX_RUN + 8]u8 = undefined;
        const k = runKey(testing.allocator, .live, r.owner, r.run, r.created_ns) catch unreachable;
        defer testing.allocator.free(k);
        const p = view_mod.idKey(&buf, r.ns, k) catch unreachable;
        var n: usize = 0;
        switch (r.family) {
            inline else => |f| {
                var it = t.read().range(if (f == .workflow) Workflows else Actions, p);
                while (it.next() catch null) |item| : (n += 1) out[n] = item.value.v;
            },
        }
        return out[0..n];
    }

    fn checksum(t: *H) u64 {
        var h = std.hash.Wyhash.init(0);
        for ([_]u16{ Workflows.ID, Actions.ID }) |id| h.update(std.mem.asBytes(&store_mod.checksum(&t.store, id)));
        return h.final();
    }
};

test "history: a live run's chunks land as rows in order, and at 16 one fold makes them one, so its rows stay few" {
    for ([_][]const u8{ "act", "a}b", "{x", "x{y}z" }) |owner| {
        var t = H.init();
        defer t.deinit();
        const r: H.Run = .{ .owner = owner };
        for (0..15) |i| try t.chunk(r, 2 * i + 1, 2 * i + 2, false, 0);
        try t.pump();
        try testing.expectEqual(@as(usize, 15), t.countTag(r, .live));
        try testing.expectEqual(@as(usize, 0), t.countTag(r, .marker));
        // The 16th: a fold of the 16, sent and landed; one row covers them.
        try t.chunk(r, 31, 32, false, 0);
        try t.pump();
        var rows: [32]Row.Value = undefined;
        const got = t.live(r, &rows);
        try testing.expectEqual(@as(usize, 1), got.len);
        try testing.expectEqual(@as(u8, @backingInt(he.Kind.index)), got[0].kind);
        try testing.expectEqual(@as(u64, 1), got[0].first_seq);
        try testing.expectEqual(@as(u64, 32), got[0].last_seq);
        try testing.expectEqual(@as(usize, 0), t.countTag(r, .marker));
        // On: 15 more, and the fold of those 15 and the index.
        for (0..15) |i| try t.chunk(r, 33 + i, 33 + i, false, 0);
        try t.pump();
        const again = t.live(r, &rows);
        try testing.expectEqual(@as(usize, 1), again.len);
        try testing.expectEqual(@as(u64, 47), again[0].last_seq);
    }
}

test "history: chunks landing while a fold is on its way add rows, and only one fold is in flight per run" {
    var t = H.init();
    defer t.deinit();
    const r: H.Run = .{};
    for (0..16) |i| try t.chunk(r, i + 1, i + 1, false, 0);
    // Five more sent before the fold the 16th makes: the lane delivers
    // them first.
    for (0..5) |i| try t.chunk(r, 17 + i, 17 + i, false, 0);
    try t.pump();
    var rows: [32]Row.Value = undefined;
    const got = t.live(r, &rows);
    try testing.expectEqual(@as(usize, 6), got.len);
    try testing.expectEqual(@as(u8, @backingInt(he.Kind.index)), got[got.len - 1].kind);
    try testing.expectEqual(@as(usize, 0), t.countTag(r, .marker));
    // One fold in all: the five that landed while it was on its way sent
    // none of their own.
    try testing.expectEqual(@as(usize, 1), t.sent(.history_index));
}

test "history: a drop that lands before a fold already on its way leaves nothing, and the fold changes nothing" {
    var t = H.init();
    defer t.deinit();
    const r: H.Run = .{};
    for (0..16) |i| try t.chunk(r, i + 1, i + 1, false, 0);
    // The 16th lands and sends its fold, behind the drop on the lane.
    try t.pumpAtMost(15);
    try t.dropOf(r, 0);
    try t.pump();
    try testing.expectEqual(@as(usize, 1), t.sent(.history_index));
    try testing.expectEqual(@as(usize, 0), t.countTag(r, .live));
    try testing.expectEqual(@as(usize, 0), t.countTag(r, .marker));
    try testing.expectEqual(@as(usize, 0), t.countTag(r, .final));
}

test "history: a fold listing a row its run doesn't hold halts, as the marker keeps that from happening" {
    var t = H.init();
    defer t.deinit();
    const r: H.Run = .{};
    for (0..16) |i| try t.chunk(r, i + 1, i + 1, false, 0);
    // All 16 land; the fold they send waits on the lane.
    try t.pumpAtMost(16);
    try testing.expectEqual(@as(usize, 1), t.countTag(r, .marker));
    // One of the rows it lists gone, as no apply removes it.
    var rows: [16]Row.Value = undefined;
    const got = t.live(r, &rows);
    {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        const k = try liveRowKey(arena.allocator(), r.ns, r.owner, r.run, r.created_ns, got[3].ual_index, got[3].part);
        var ks = KeySet.init(arena.allocator());
        try ks.add(Actions.ID, "", k);
        var view = View.init(&t.store, arena.allocator(), &ks);
        view.strict = false;
        try view.delete(Actions, k);
        try view.commit();
    }
    try testing.expectError(error.Halt, t.pump());
}

test "history: an ended run moves to one ended row: at once from one row, by a fold from several, and by as many folds as it takes" {
    for ([_]usize{ 1, 2, 16, 37 }) |chunks| for ([_]he.Family{ .action, .workflow }) |family| {
        var t = H.init();
        defer t.deinit();
        const r: H.Run = .{ .family = family };
        for (0..chunks) |i| try t.chunk(r, i + 1, i + 1, i + 1 == chunks, 900);
        try t.pump();
        try testing.expectEqual(@as(usize, 0), t.countTag(r, .live));
        try testing.expectEqual(@as(usize, 0), t.countTag(r, .final));
        try testing.expectEqual(@as(usize, 0), t.countTag(r, .marker));
        const e = t.ended(r, 900).?;
        try testing.expectEqual(@as(u8, @backingInt(Mode.one)), e.mode);
        try testing.expectEqual(@as(u8, @backingInt(if (chunks == 1) he.Kind.chunk else he.Kind.index)), e.kind);
        try testing.expectEqual(@as(u64, 1), e.first_seq);
        try testing.expectEqual(@as(u64, chunks), e.last_seq);
    };
}

test "history: a skipped chunk lands as lost for its events; a skipped fold is sent again, and past its retries the run ends at its live rows" {
    {
        var t = H.init();
        defer t.deinit();
        const r: H.Run = .{};
        try t.chunk(r, 1, 4, false, 0);
        try t.chunk(r, 5, 9, false, 0);
        t.skip = &.{2};
        try t.pump();
        var rows: [4]Row.Value = undefined;
        const got = t.live(r, &rows);
        try testing.expectEqual(@as(usize, 2), got.len);
        try testing.expectEqual(@as(u8, @backingInt(he.Kind.lost)), got[1].kind);
        try testing.expectEqual(@as(u64, 5), got[1].first_seq);
        try testing.expectEqual(@as(u64, 9), got[1].last_seq);
    }
    for ([_]usize{ 1, 2, 3 }) |losses| {
        var t = H.init();
        defer t.deinit();
        const r: H.Run = .{};
        try t.chunk(r, 1, 1, false, 0);
        try t.chunk(r, 2, 2, true, 700);
        // Seq 3 is the first fold, 4 its resend, 5 the next.
        const skips = [_]u64{ 3, 4, 5 };
        t.skip = skips[0..losses];
        try t.pump();
        const e = t.ended(r, 700).?;
        try testing.expectEqual(@as(usize, 0), t.countTag(r, .final));
        try testing.expectEqual(@as(usize, 0), t.countTag(r, .marker));
        if (losses < MAX_FOLD_RETRIES) {
            try testing.expectEqual(@as(u8, @backingInt(Mode.one)), e.mode);
            try testing.expectEqual(@as(usize, 0), t.countTag(r, .live));
        } else {
            try testing.expectEqual(@as(u8, @backingInt(Mode.live)), e.mode);
            try testing.expectEqual(@as(usize, 2), t.countTag(r, .live));
        }
    }
}

test "history: a drop takes every row of its run's incarnation and no other's" {
    var t = H.init();
    defer t.deinit();
    const a: H.Run = .{};
    const again: H.Run = .{ .created_ns = 2 };
    const other: H.Run = .{ .owner = "other" };
    for ([_]H.Run{ a, again, other }) |r| {
        for (0..3) |i| try t.chunk(r, i + 1, i + 1, false, 0);
    }
    try t.chunk(a, 4, 4, true, 50);
    try t.pump();
    try testing.expect(t.ended(a, 50) != null);
    try t.dropOf(a, 50);
    try t.pump();
    try testing.expect(t.ended(a, 50) == null);
    try testing.expectEqual(@as(usize, 0), t.countTag(a, .live));
    try testing.expectEqual(@as(usize, 3), t.countTag(again, .live));
    try testing.expectEqual(@as(usize, 3), t.countTag(other, .live));
    // A live run's drop too.
    try t.dropOf(again, 0);
    try t.pump();
    try testing.expectEqual(@as(usize, 0), t.countTag(again, .live));
    try testing.expectEqual(@as(usize, 3), t.countTag(other, .live));
}

test "history: a skipped drop is sent again, and past its retries its run is marked orphaned until a later drop" {
    for ([_]usize{ 1, 2, 3 }) |losses| {
        var t = H.init();
        defer t.deinit();
        const r: H.Run = .{};
        const other: H.Run = .{ .owner = "other" };
        try t.chunk(r, 1, 1, true, 50);
        try t.chunk(other, 1, 1, true, 50);
        try t.pump();
        try testing.expect(t.ended(r, 50) != null);
        // Seq 3 is the drop, 4 and 5 its resends.
        const skips = [_]u64{ 3, 4, 5 };
        t.skip = skips[0..losses];
        try t.dropOf(r, 50);
        try t.pump();
        try testing.expect(t.ended(other, 50) != null);
        try testing.expectEqual(@as(usize, 0), t.countTag(other, .orphan));
        if (losses < MAX_DROP_RETRIES) {
            try testing.expect(t.ended(r, 50) == null);
            try testing.expectEqual(@as(usize, 0), t.countTag(r, .orphan));
        } else {
            try testing.expect(t.ended(r, 50) != null);
            try testing.expectEqual(@as(usize, 1), t.countTag(r, .orphan));
            // A later drop of that incarnation takes its rows and the mark.
            t.skip = &.{};
            try t.dropOf(r, 50);
            try t.pump();
            try testing.expect(t.ended(r, 50) == null);
            try testing.expectEqual(@as(usize, 0), t.countTag(r, .orphan));
        }
    }
}

test "history: a skipped drop whose count is past any it could reach marks its run orphaned, not wraps" {
    var t = H.init();
    defer t.deinit();
    const r: H.Run = .{};
    try t.chunk(r, 1, 1, true, 50);
    try t.pump();
    var buf: [he.Drop.max_size]u8 = undefined;
    const p = try he.Drop.encodeInto(&buf, .{ .ns = r.ns.int(), .family = @backingInt(r.family), .owner = r.owner, .run = r.run, .created_ns = r.created_ns, .first_seq = std.math.maxInt(u64), .last_seq = 0, .final = false, .completed_ms = 50 });
    // Seq 2 is that drop.
    t.skip = &.{2};
    try t.sendOwn(r, .history_drop, p);
    try t.pump();
    try testing.expectEqual(@as(usize, 1), t.countTag(r, .orphan));
}

/// The usage rule over `t`'s rows, as VOPR checks it.
fn expectUsageRule(t: *H) !void {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    if (try usageRule(t.read(), arena.allocator())) |why| {
        std.debug.print("usage rule: {s}\n", .{why});
        return error.TestUnexpectedResult;
    }
}

test "history usage: a chunk counts its whole message; folds, ends and drops keep each namespace's usage its rows' sum, and a drop takes it to none" {
    for ([_]usize{ 1, 2, 16, 37 }) |chunks| for ([_]he.Family{ .action, .workflow }) |family| for ([_]bool{ false, true }) |ends| {
        var t = H.init();
        defer t.deinit();
        const r: H.Run = .{ .family = family };
        const elsewhere: H.Run = .{ .family = family, .ns = .of(17), .owner = "other" };
        for (0..chunks) |i| {
            try t.chunk(r, i + 1, i + 1, ends and i + 1 == chunks, 900);
            try t.chunk(elsewhere, i + 1, i + 1, false, 0);
            if (i == 0) {
                // One chunk landed: its message's bytes, header and all.
                try t.pumpAtMost(1);
                var buf: [he.Chunk.max_size]u8 = undefined;
                const one = try he.Chunk.encodeInto(&buf, .{ .ns = r.ns.int(), .family = @backingInt(r.family), .owner = r.owner, .run = r.run, .created_ns = r.created_ns, .first_seq = 1, .last_seq = 1, .final = ends and chunks == 1, .completed_ms = 900, .records = "events" });
                try testing.expectEqual(@as(u64, one.len), usageOf(t.read(), r.ns).bytes);
            }
        }
        try t.pump();
        try expectUsageRule(&t);
        const before = usageOf(t.read(), elsewhere.ns);
        try testing.expect(usageOf(t.read(), r.ns).bytes > 0);
        try t.dropOf(r, if (ends) 900 else 0);
        try t.pump();
        try expectUsageRule(&t);
        // Its namespace keeps no row; the other's is as it was.
        const nb = r.ns.bytes();
        try testing.expect((try t.read().get(UsageFam, &nb)) == null);
        try testing.expectEqual(before.bytes, usageOf(t.read(), elsewhere.ns).bytes);
    };
}

test "history usage: a lost chunk counts nothing, and a run ended at its live rows keeps their bytes" {
    const hq = @import("../budget/history_quota.zig");
    var t = H.init();
    defer t.deinit();
    const r: H.Run = .{};
    try t.chunk(r, 1, 1, false, 0);
    // A chunk that lands moves its bytes from the lane to usage: the
    // namespace's in-flight history, what its quota holds steps at, stays
    // where it was, and nothing is eased.
    const in_flight = hq.inFlight(t.read(), r.ns);
    try testing.expect(in_flight > 0);
    try t.pump();
    try testing.expectEqual(in_flight, hq.inFlight(t.read(), r.ns));
    try testing.expect(!t.store.quota_eased.any());
    try t.chunk(r, 2, 2, true, 700);
    // Seq 2 lost; then its fold (3) and both resends (4, 5).
    t.skip = &.{ 2, 3, 4, 5 };
    try t.pump();
    // Lost, its bytes left the lane for nothing: the namespace is eased.
    try testing.expect(t.store.quota_eased.has(r.ns.int()));
    try testing.expect(!t.store.quota_eased.has(r.ns.int() + 1));
    var rows: [4]Row.Value = undefined;
    const got = t.live(r, &rows);
    try testing.expectEqual(@as(usize, 2), got.len);
    try testing.expectEqual(@as(u64, 0), got[1].total);
    try testing.expectEqual(@as(u8, @backingInt(Mode.live)), t.ended(r, 700).?.mode);
    try testing.expectEqual(@as(u64, 0), t.ended(r, 700).?.total);
    try testing.expectEqual(got[0].total, usageOf(t.read(), r.ns).bytes);
    try expectUsageRule(&t);
    t.skip = &.{};
    try t.dropOf(r, 700);
    try t.pump();
    try testing.expectEqual(@as(u64, 0), usageOf(t.read(), r.ns).bytes);
    try expectUsageRule(&t);
}

test "history usage: a chunk or a fold that lands eases nothing; a drop that lands eases its namespace alone" {
    var t = H.init();
    defer t.deinit();
    const r: H.Run = .{};
    const elsewhere: H.Run = .{ .ns = .of(17), .owner = "other" };
    try t.chunk(elsewhere, 1, 1, false, 0);
    // Sixteen land and a fold makes them one row: each adds its own bytes
    // to usage as it leaves the lane.
    for (0..16) |i| try t.chunk(r, i + 1, i + 1, false, 0);
    try t.pump();
    var rows: [2]Row.Value = undefined;
    try testing.expectEqual(@as(usize, 1), t.live(r, &rows).len);
    try testing.expect(!t.store.quota_eased.any());
    // A drop takes the run's rows and adds nothing: eased, its own alone.
    try t.dropOf(r, 0);
    try t.pump();
    try testing.expect(t.store.quota_eased.has(r.ns.int()));
    try testing.expect(!t.store.quota_eased.has(elsewhere.ns.int()));
}

test "history usage: an orphaned run is counted once with its rows' bytes, however often its drop is lost, and a later drop takes both" {
    var t = H.init();
    defer t.deinit();
    const r: H.Run = .{};
    try t.chunk(r, 1, 1, false, 0);
    try t.chunk(r, 2, 2, true, 50);
    try t.pump();
    const bytes = usageOf(t.read(), r.ns).bytes;
    // Seqs 4..6: the drop and its two resends; then 7..9 a second drop's.
    t.skip = &.{ 4, 5, 6, 7, 8, 9 };
    for (0..2) |_| {
        try t.dropOf(r, 50);
        try t.pump();
        const u = usageOf(t.read(), r.ns);
        try testing.expectEqual(@as(u32, 1), u.orphans);
        try testing.expectEqual(bytes, u.orphan_bytes);
        try testing.expectEqual(bytes, u.bytes);
        try expectUsageRule(&t);
    }
    t.skip = &.{};
    try t.dropOf(r, 50);
    try t.pump();
    const u = usageOf(t.read(), r.ns);
    try testing.expectEqual(@as(u64, 0), u.bytes);
    try testing.expectEqual(@as(u32, 0), u.orphans);
    try testing.expectEqual(@as(u64, 0), u.orphan_bytes);
    try expectUsageRule(&t);
}

test "history usage: an orphaned run's mark grows with the chunks and folds that land after it" {
    var t = H.init();
    defer t.deinit();
    const r: H.Run = .{};
    for (0..7) |i| try t.chunk(r, i + 1, i + 1, false, 0);
    try t.pump();
    // Its drop and both resends lost (seqs 8..10): orphaned at 7 rows.
    t.skip = &.{ 8, 9, 10 };
    try t.dropOf(r, 0);
    try t.pump();
    t.skip = &.{};
    try testing.expectEqual(@as(usize, 1), t.countTag(r, .orphan));
    // Nine more land after the mark, and the fold at 16 with them.
    for (0..9) |i| try t.chunk(r, 8 + i, 8 + i, false, 0);
    try t.pump();
    const u = usageOf(t.read(), r.ns);
    try testing.expectEqual(u.bytes, u.orphan_bytes);
    try expectUsageRule(&t);
}

test "history usage: an ended row put over one already there (a second final chunk) takes the first's bytes off, so none count twice" {
    var t = H.init();
    defer t.deinit();
    const r: H.Run = .{};
    try t.chunk(r, 1, 1, true, 50);
    try t.pump();
    try t.chunk(r, 2, 2, true, 50);
    try t.pump();
    try expectUsageRule(&t);
    try testing.expectEqual(@as(u64, 0), t.store.history_usage_drift);
}

test "history usage: however many runs land and go in one namespace, it keeps one usage row, its size unchanged" {
    var t = H.init();
    defer t.deinit();
    var size: ?u64 = null;
    var rb: [16]u8 = undefined;
    for (0..400) |i| {
        const r: H.Run = .{ .run = try std.fmt.bufPrint(&rb, "r{d}", .{i}) };
        // 20 chunks a run, its last final: folds and an end each.
        for (0..20) |c| try t.chunk(r, c + 1, c + 1, c == 19, 50);
        try t.pump();
        // Every other run deleted, so the row rises and falls.
        if (i % 2 == 1) {
            try t.dropOf(r, 50);
            try t.pump();
        }
        const f = t.store.family(UsageFam.ID);
        try testing.expectEqual(@as(usize, 1), f.count);
        if (size) |sz| try testing.expectEqual(sz, f.encoded_bytes) else size = f.encoded_bytes;
    }
    try expectUsageRule(&t);
}

test "history usage: a subtract past what a namespace holds is counted as drift and saturates, never halts" {
    var t = H.init();
    defer t.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var ks = KeySet.init(arena.allocator());
    try ks.addIn(UsageFam.ID, .default, "");
    var view = View.init(&t.store, arena.allocator(), &ks);
    try moveUsage(&view, .default, .{ .add = 5, .sub = 9, .orphans_sub = 1 });
    try view.commit();
    try testing.expectEqual(@as(u64, 2), t.store.history_usage_drift);
    try testing.expectEqual(@as(u64, 0), usageOf(t.read(), .default).bytes);
}

test "history: a run's reads see only its own incarnation's rows, and an ended run's one row" {
    var t = H.init();
    defer t.deinit();
    const first: H.Run = .{};
    const again: H.Run = .{ .created_ns = 2 };
    try t.chunk(first, 1, 1, true, 10);
    try t.chunk(again, 1, 3, false, 0);
    try t.pump();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var ended_rows = try rowsOf(t.read(), arena.allocator(), .action, .default, "act", "r1", 1, 10);
    try testing.expectEqual(@as(u64, 1), ended_rows.next().?.last_seq);
    try testing.expect(ended_rows.next() == null);
    var live_rows = try rowsOf(t.read(), arena.allocator(), .action, .default, "act", "r1", 2, null);
    try testing.expectEqual(@as(u64, 3), live_rows.next().?.last_seq);
    try testing.expect(live_rows.next() == null);
}

test "history: every replica of the same messages and skips holds the same rows" {
    var a = H.init();
    defer a.deinit();
    var b = H.init();
    defer b.deinit();
    for ([_]*H{ &a, &b }) |t| {
        t.skip = &.{ 7, 19 };
        const r: H.Run = .{};
        for (0..30) |i| try t.chunk(r, i + 1, i + 1, i == 29, 77);
        try t.pump();
    }
    try testing.expectEqual(a.checksum(), b.checksum());
}

test "history: a run's history message to a slot that isn't its owner's halts its apply" {
    var t = H.init();
    defer t.deinit();
    var buf: [he.Chunk.max_size]u8 = undefined;
    const p = try he.Chunk.encodeInto(&buf, .{ .ns = 1, .family = 1, .owner = "act", .run = "r1", .created_ns = 1, .first_seq = 1, .last_seq = 1, .final = false, .completed_ms = 0, .records = "x" });
    t.index += 1;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var ks = KeySet.init(arena.allocator());
    var view = View.init(&t.store, arena.allocator(), &ks);
    const wrong: u16 = @intCast((router.nsKeySlot(.default, "act") + 1) % router.SLOTS);
    try view.sendFlagged(.user(.default), wrong, @backingInt(EntryType.history_chunk), entry_mod.Flags.HISTORY, p);
    try channels.writeSends(&view, t.index);
    try view.commit();
    try testing.expectError(error.Halt, t.pump());
    try testing.expectEqual(@as(u64, 0), t.store.family(Actions.ID).count);
}

test "history: an entry of a run's history outside a message halts its apply" {
    var t = H.init();
    defer t.deinit();
    var buf: [he.Chunk.max_size]u8 = undefined;
    const p = try he.Chunk.encodeInto(&buf, .{ .ns = 1, .family = 1, .owner = "act", .run = "r1", .created_ns = 1, .first_seq = 1, .last_seq = 1, .final = false, .completed_ms = 0, .records = "x" });
    var h = std.mem.zeroes(entry_mod.Header);
    h.entry_type = @backingInt(EntryType.history_chunk);
    h.index = 1;
    h.flags = entry_mod.Flags.HISTORY;
    h.payload_len = @intCast(p.len);
    const e: Entry = .{ .header = h, .payload = p };
    var res = Result.start(&e);
    try testing.expectError(error.Halt, machine_mod.applyEntry(&machine, &t.store, &t.scratch, &e, &res));
}

test "history: every row's slot follows from the owner it stores, as its chunks were routed, braces and all" {
    const slot_check = @import("../contract/slot_check.zig");
    const Derive = struct {
        fn slot(_: void, _: *store_mod.Store, key: []const u8, value: []const u8) ?u32 {
            const v = (delivery.Stored(Row).decode(value) catch return null).v;
            return router.nsKeySlot(slot_check.nsOf(key), v.owner);
        }
    };
    for ([_][]const u8{ "a}b", "{x", "x{y}z", "{t}rest" }) |owner| {
        var t = H.init();
        defer t.deinit();
        const r: H.Run = .{ .owner = owner };
        for (0..20) |i| try t.chunk(r, i + 1, i + 1, false, 0);
        const ended: H.Run = .{ .owner = owner, .run = "r2" };
        for (0..3) |i| try t.chunk(ended, i + 1, i + 1, i == 2, 9);
        try t.pump();
        const n = try slot_check.expectFamily(&t.store, Actions.ID, router.nsKeySlot(.default, owner), {}, Derive.slot);
        try testing.expect(n > 0);
    }
}

test "history machine: a run with its ended row and a chunk landed after it breaks the rule VOPR checks" {
    var t = H.init();
    defer t.deinit();
    const r: H.Run = .{};
    try t.chunk(r, 0, 1, false, 0);
    try t.chunk(r, 2, 3, true, 50);
    try t.pump();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    try testing.expect(t.ended(r, 50) != null);
    try testing.expect(try ruleOf(Actions, t.read(), arena.allocator()) == null);
    try testing.expect(endedRow(t.read(), arena.allocator(), .action, r.ns, r.owner, r.run, r.created_ns, 50) != null);
    try testing.expect(endedRow(t.read(), arena.allocator(), .action, r.ns, r.owner, r.run, r.created_ns, 51) == null);
    // A chunk after its end makes the run live again.
    try t.chunk(r, 4, 4, false, 0);
    try t.pump();
    try testing.expectEqualStrings("a run with its ended row has live, marker or final rows besides", (try ruleOf(Actions, t.read(), arena.allocator())).?);
}

test "history machine: a leader's sent orphans are held back for their term only, and none past MAX" {
    var sent: OrphansSent = .{};
    defer sent.deinit(testing.allocator);
    sent.at(testing.allocator, 3);
    const o: Orphan = .{ .run = "r1", .created_ns = 7, .completed_ms = 10 };
    try testing.expect(!sent.has(.default, "wf", o));
    try sent.note(testing.allocator, .default, "wf", o);
    try testing.expect(sent.has(.default, "wf", o));
    // Another owner, namespace or incarnation is another orphan.
    try testing.expect(!sent.has(.default, "wf2", o));
    try testing.expect(!sent.has(.of(17), "wf", o));
    try testing.expect(!sent.has(.default, "wf", .{ .run = "r1", .created_ns = 8, .completed_ms = 10 }));
    // The same term keeps it; a new one forgets it.
    sent.at(testing.allocator, 3);
    try testing.expect(sent.has(.default, "wf", o));
    sent.at(testing.allocator, 4);
    try testing.expect(!sent.has(.default, "wf", o));
    // Full, it takes no more.
    var i: u64 = 0;
    while (!sent.full()) : (i += 1) try sent.note(testing.allocator, .default, "wf", .{ .run = "r", .created_ns = i, .completed_ms = 0 });
    try sent.note(testing.allocator, .default, "wf", o);
    try testing.expect(!sent.has(.default, "wf", o));
    try testing.expectEqual(@as(usize, OrphansSent.MAX), sent.rows.count());
}
