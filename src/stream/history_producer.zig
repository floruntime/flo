//! How a run's apply records its history: each event is encoded onto the
//! run's pending chunk, and a chunk is sent on the history lane to the
//! run's own group once full, or when the run ends. Workflow runs and
//! action runs both record theirs this way; only the row that holds the
//! pending chunk differs.

const std = @import("std");
const schema = @import("../protocol/schema.zig");
const he = @import("../protocol/history_entries.zig");
const action_ops = @import("../protocol/action_ops.zig");
const persistence = @import("../storage/persistence.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const view_mod = @import("../contract/view.zig");
const NamespaceId = @import("../namespace/table.zig").Id;

const View = view_mod.View;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;

/// The most event bytes a run holds before sending them: a chunk. An event
/// over half of it goes at once, with those before it.
pub const HIST_CHUNK: usize = 4 << 10;

/// The longest an event's type or detail is: each comes from one entry.
pub const MAX_EVENT = persistence.MAX_PERSIST_PAYLOAD;
pub const MAX_DETAIL = persistence.MAX_PERSIST_PAYLOAD;
/// The longest an event's extra is: an action's outcome.
pub const MAX_EXTRA = action_ops.MAX_OUTCOME;

/// One event of a run's history, as its chunk carries it. `extra` is empty
/// but for an action's settle, where it is the outcome.
pub const HistRec = Record(&.{
    .{ .name = "seq", .T = Int(u64) },
    .{ .name = "event", .T = Bytes(MAX_EVENT) },
    .{ .name = "detail", .T = Bytes(MAX_DETAIL) },
    .{ .name = "extra", .T = Bytes(MAX_EXTRA) },
    .{ .name = "at_ms", .T = Int(i64) },
});

/// The bytes one event's record takes, its type, detail and extra at most
/// these.
pub fn recordBound(event: usize, detail: usize, extra: usize) usize {
    return 8 + 4 + event + 4 + detail + 2 + extra + 8;
}

comptime {
    std.debug.assert(HistRec.max_size == recordBound(MAX_EVENT, MAX_DETAIL, MAX_EXTRA));
    std.debug.assert(he.MAX_CHUNK_RECORDS >= HIST_CHUNK);
}

/// The run a history belongs to, and the group its chunks go to: its own.
pub const Owner = struct {
    family: he.Family,
    ns: NamespaceId,
    owner: []const u8,
    run: []const u8,
    /// The stamp of the entry that made the run: its incarnation.
    created_ns: u64,
    slot: u16,
};

/// A run's history as its row holds it: the seq the next event takes, the
/// first not yet sent, and those since, encoded.
pub const Hist = struct {
    next: u64,
    flushed: u64,
    pending: []const u8,
};

pub const Event = struct {
    event: []const u8,
    detail: []const u8,
    extra: []const u8 = "",
    at_ms: i64,
};

/// How an event joins the pending chunk.
pub const Mode = enum {
    /// Sent first when it wouldn't fit there, and with it when it is over
    /// half a chunk.
    flush_on_size,
    /// Held for the run's last chunk, which the same apply sends: the apply
    /// that ends a run sends one chunk.
    hold,
};

/// `ev` onto `h` at its next seq. One that won't encode is a Flo bug: every
/// event's fields are bounded by their source, under the record's.
pub fn append(view: *View, o: Owner, h: *Hist, ev: Event, mode: Mode) ApplyError!void {
    const a = view.arena;
    const rec: HistRec.Value = .{ .seq = h.next, .event = ev.event, .detail = ev.detail, .extra = ev.extra, .at_ms = ev.at_ms };
    const buf = try a.alloc(u8, HistRec.encodedSize(rec) catch return error.Halt);
    var w: schema.Writer = .{ .buf = buf };
    HistRec.encode(&w, rec) catch return error.Halt;
    const bytes = w.written();
    if (mode == .flush_on_size and h.pending.len + bytes.len > HIST_CHUNK and h.pending.len > 0) try flush(view, o, h, null);
    const joined = try a.alloc(u8, h.pending.len + bytes.len);
    @memcpy(joined[0..h.pending.len], h.pending);
    @memcpy(joined[h.pending.len..], bytes);
    h.pending = joined;
    h.next += 1;
    if (mode == .flush_on_size and bytes.len > HIST_CHUNK / 2) try flush(view, o, h, null);
}

/// `h`'s pending events sent as one chunk; with `ended_ms` the last, the
/// run having ended then. A last chunk goes even with no events, so the
/// run's rows move to its one ended row. `keep_flushed` leaves the sent
/// seqs unmarked: a planted bug's only.
pub fn flush(view: *View, o: Owner, h: *Hist, ended_ms: ?i64) ApplyError!void {
    return flushKeeping(view, o, h, ended_ms, false);
}

pub fn flushKeeping(view: *View, o: Owner, h: *Hist, ended_ms: ?i64, keep_flushed: bool) ApplyError!void {
    const final = ended_ms != null;
    if (h.pending.len == 0 and !final) return;
    const a = view.arena;
    // A run's events are its own seqs: none pending is an empty range at
    // the next one.
    const last = if (h.next > h.flushed) h.next - 1 else h.flushed;
    const buf = try a.alloc(u8, he.Chunk.max_size);
    const payload = he.Chunk.encodeInto(buf, .{
        .ns = o.ns.int(),
        .family = @backingInt(o.family),
        .owner = o.owner,
        .run = o.run,
        .created_ns = o.created_ns,
        .first_seq = h.flushed,
        .last_seq = last,
        .final = final,
        .completed_ms = if (ended_ms) |ms| @intCast(@max(ms, 0)) else 0,
        .records = h.pending,
    }) catch return error.Halt;
    try view.sendFlagged(.user(o.ns), o.slot, @backingInt(entry_mod.EntryType.history_chunk), entry_mod.Flags.HISTORY, payload);
    if (!keep_flushed) h.flushed = h.next;
    h.pending = "";
}

/// A deleted run's history dropped: a `history_drop` to its owner's group
/// on the history lane, after any chunk of its already there.
/// `completed_ms` is when the run ended, which keys its ended row.
pub fn sendDrop(view: *View, o: Owner, completed_ms: i64) ApplyError!void {
    const buf = try view.arena.alloc(u8, he.Drop.max_size);
    const payload = he.Drop.encodeInto(buf, .{
        .ns = o.ns.int(),
        .family = @backingInt(o.family),
        .owner = o.owner,
        .run = o.run,
        .created_ns = o.created_ns,
        .first_seq = 0,
        .last_seq = 0,
        .final = false,
        .completed_ms = @intCast(@max(completed_ms, 0)),
    }) catch return error.Halt;
    try view.sendFlagged(.user(o.ns), o.slot, @backingInt(entry_mod.EntryType.history_drop), entry_mod.Flags.HISTORY, payload);
}

/// The bytes a chunk message carries for `records` bytes of events, its
/// header at its largest.
pub fn chunkBound(records: usize) usize {
    return he.Chunk.max_size - he.MAX_CHUNK_RECORDS + records;
}

const testing = std.testing;

test "history producer: a record's bound is its encoding at every field's largest" {
    const ev: [MAX_EVENT]u8 = @splat('e');
    const d: [MAX_DETAIL]u8 = @splat('d');
    const x: [MAX_EXTRA]u8 = @splat('x');
    try testing.expectEqual(recordBound(MAX_EVENT, MAX_DETAIL, MAX_EXTRA), try HistRec.encodedSize(.{ .seq = std.math.maxInt(u64), .event = &ev, .detail = &d, .extra = &x, .at_ms = -1 }));
}
