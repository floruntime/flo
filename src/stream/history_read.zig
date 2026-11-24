//! A run's history as a read answers it, for either producer: the events
//! landed in its rows and those its run still holds pending, merged by seq,
//! one page at a time. `workflow.history` and `action.history` answer this
//! one shape.

const std = @import("std");
const Allocator = std.mem.Allocator;
const schema = @import("../protocol/schema.zig");
const ops = @import("../protocol/ops.zig");
const he = @import("../protocol/history_entries.zig");
const hp = @import("history_producer.zig");
const history_machine = @import("history_machine.zig");
const contract_view = @import("../contract/view.zig");
const StreamHandler = @import("handler.zig").StreamHandler;
const NamespaceId = @import("../namespace/table.zig").Id;

const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;

pub const Window = StreamHandler.HistoryWindow;

/// How long a history read is held for its chunks to land, or come back
/// from disk, before it is answered with what landed.
pub const HOLD_MS: u32 = 5_000;

/// The most events one page holds, whatever its limit asks.
pub const MAX_PAGE: u32 = 1000;

/// Event type of a gap in a run's history: events a repair skipped, or the
/// log no longer holds (`LOST`), or not landed by the read's deadline
/// (`MISSING`).
pub const LOST = "history_lost";
pub const MISSING = "history_missing";

/// One event on the wire. Each field's bound is its record's, so every
/// event a run recorded is answered whole.
pub const Event = Record(&.{
    .{ .name = "event_type", .T = Bytes(hp.MAX_EVENT), .doc = "What happened" },
    .{ .name = "detail", .T = Bytes(hp.MAX_DETAIL), .doc = "Its detail: an input, a result, an error, a worker" },
    .{ .name = "extra", .T = Bytes(hp.MAX_EXTRA), .doc = "An action's settle: the outcome its worker named; empty for every other event" },
    .{ .name = "timestamp_ms", .T = Int(i64), .doc = "When, in Unix milliseconds" },
});

/// The most bytes one page's events take: the answer's bound, less its
/// count and trailer. One event always fits.
pub const PAGE_BYTES: u32 = ops.MAX_RESPONSE_BODY - 2 - 1 - (1 + 8);
comptime {
    std.debug.assert(Event.max_size <= PAGE_BYTES);
}

pub const Request = Record(&.{
    .{ .name = "limit", .T = schema.Defaulted(Int(u32), 100), .doc = "The most events to answer, at most 1000" },
    .{ .name = "cursor", .T = schema.Optional(Int(u64)), .doc = "Where to go on from: the cursor the last page answered" },
    .{ .name = "newest_first", .T = schema.Defaulted(schema.Bool, false), .doc = "Newest first, going back from the cursor" },
});

pub const Events = schema.BoundedList(Event, MAX_PAGE, PAGE_BYTES);

/// The record bytes a read copies before it stops: past twice a page, the
/// page is full. A record is its event's answer plus a fixed overhead (its
/// seq, and its lengths' widths), at most two fifths of the smallest
/// record, so an answer is at least three fifths of its record: twice a
/// page of records answers more than a page.
pub const READ_BYTES: usize = 2 * @as(usize, PAGE_BYTES);
comptime {
    const overhead = hp.HistRec.min_size - Event.min_size;
    std.debug.assert(5 * overhead <= 2 * hp.HistRec.min_size);
}

pub const Response = Record(&.{
    .{ .name = "events", .T = Events, .doc = "The events, in the order asked" },
    .{ .name = "has_more", .T = schema.Bool, .doc = "Whether more follow" },
    .{ .name = "cursor", .T = schema.Optional(Int(u64)), .doc = "The cursor that asks for them" },
});

/// A history read, as asked and as a held one keeps it.
pub const Query = struct {
    limit: u32 = 100,
    cursor: ?u64 = null,
    newest_first: bool = false,

    pub fn window(q: Query) Window {
        if (q.newest_first) return .{ .to = q.cursor orelse std.math.maxInt(u64), .newest_first = true };
        return .{ .from = q.cursor orelse 0 };
    }

    /// What a held read keeps of it beside its limit and its cursor (the
    /// waiter's `min_version`): whether it reads newest first, and has a
    /// cursor at all.
    pub fn flags(q: Query) u32 {
        return @as(u32, @intFromBool(q.newest_first)) | @as(u32, @intFromBool(q.cursor != null)) << 1;
    }

    pub fn fromHeld(limit: u32, flag_bits: u32, cursor: u64) Query {
        return .{ .limit = limit, .cursor = if (flag_bits & 2 != 0) cursor else null, .newest_first = flag_bits & 1 != 0 };
    }
};

/// The run whose history is read, as its row stands.
pub const Source = struct {
    family: he.Family,
    ns: NamespaceId,
    owner: []const u8,
    run: []const u8,
    created_ns: u64,
    /// When it ended, or null while it runs.
    ended_ms: ?u64,
    /// The first seq it hasn't sent, and those since, encoded.
    flushed: u64,
    pending: []const u8,
};

/// One event and the seqs it covers: its own, or a gap's, within what was
/// asked.
pub const SeqEvent = struct {
    seq: u64,
    last: u64,
    event_type: []const u8,
    detail: []const u8,
    extra: []const u8 = "",
    timestamp_ms: i64,

    fn one(r: hp.HistRec.Value) SeqEvent {
        return .{ .seq = r.seq, .last = r.seq, .event_type = r.event, .detail = r.detail, .extra = r.extra, .timestamp_ms = r.at_ms };
    }

    /// A gap's event over `first..last`, cut to `window`, or null for none
    /// of it in the window.
    fn gap(a: Allocator, window: Window, event_type: []const u8, comptime fmt: []const u8, first: u64, last: u64) error{OutOfMemory}!?SeqEvent {
        if (!window.wants(first, last)) return null;
        const lo = @max(first, window.from);
        const hi = @min(last, window.to - 1);
        return .{ .seq = lo, .last = hi, .event_type = event_type, .detail = try std.fmt.allocPrint(a, fmt, .{ lo, hi }), .timestamp_ms = 0 };
    }

    fn wire(e: SeqEvent) Event.Value {
        return .{ .event_type = e.event_type, .detail = e.detail, .extra = e.extra, .timestamp_ms = e.timestamp_ms };
    }
};

/// A page: its events in the order asked, and, when more follow, the cursor
/// that asks for them.
pub const Page = struct { events: []const SeqEvent, more: ?u64 };

pub const Answer = union(enum) {
    page: Page,
    /// Not all of it here yet: a chunk in flight to its rows, or one being
    /// read back from disk. Held, then asked again.
    wait,
    /// At its deadline, a chunk still being read back from disk: answered
    /// retryable, never as a short page that looks whole.
    disk_late,
};

fn wireSize(e: SeqEvent) usize {
    return Event.encodedSize(e.wire()) catch unreachable; // every field within its bound
}

/// `src`'s history as applied, at most `asked` events from `window`, read
/// from `stream`'s rows and `rv`'s history families. A read whose chunks
/// haven't all landed waits, unless it is `final` (its deadline): then the
/// rest is said missing. Points into `a`.
pub fn read(a: Allocator, stream: *StreamHandler, rv: contract_view.ReadView, src: Source, window: Window, asked: u32, final: bool) error{OutOfMemory}!Answer {
    const limit = std.math.clamp(asked, 1, MAX_PAGE);
    // One piece past the page says more follow; twice a page's record
    // bytes fill it, whatever they hold, an event's answer taking at least
    // three fifths of its record (`READ_BYTES`).
    const landed = try stream.readHistory(a, src.family, src.ns, src.owner, src.run, src.created_ns, src.ended_ms, window, @as(usize, limit) + 1, READ_BYTES);
    if (landed.pending and !final) return .wait;
    if (landed.pending) return .disk_late;
    // Every seq below `flushed` was sent: those past what landed are still
    // on their way.
    const through = history_machine.landedThrough(rv, a, src.family, src.ns, src.owner, src.run, src.created_ns, src.ended_ms);
    const landed_next: u64 = if (through) |t| t + 1 else 0;
    const in_flight = landed_next < src.flushed;

    var landed_events: std.ArrayListUnmanaged(SeqEvent) = .empty;
    for (landed.pieces) |piece| {
        if (piece.lost) {
            if (try SeqEvent.gap(a, window, LOST, "events {d}-{d} were lost", piece.first_seq, piece.last_seq)) |g| try landed_events.append(a, g);
            continue;
        }
        const mark = landed_events.items.len;
        try decodeRecords(a, piece.records, window, &landed_events);
        if (window.newest_first) std.mem.reverse(SeqEvent, landed_events.items[mark..]);
    }
    var missing: std.ArrayListUnmanaged(SeqEvent) = .empty;
    if (in_flight) if (try SeqEvent.gap(a, window, MISSING, "events {d}-{d} had not landed in time", landed_next, src.flushed - 1)) |g| try missing.append(a, g);
    var pending: std.ArrayListUnmanaged(SeqEvent) = .empty;
    try decodeRecords(a, src.pending, window, &pending);

    // Oldest first: landed, then missing, then pending; newest first, the
    // reverse.
    var all: std.ArrayListUnmanaged(SeqEvent) = .empty;
    if (window.newest_first) {
        std.mem.reverse(SeqEvent, pending.items);
        try all.appendSlice(a, pending.items);
        try all.appendSlice(a, missing.items);
        try all.appendSlice(a, landed_events.items);
    } else {
        try all.appendSlice(a, landed_events.items);
        try all.appendSlice(a, missing.items);
        try all.appendSlice(a, pending.items);
    }
    // At most `limit` events, and no more than a page's bytes.
    var n: usize = 0;
    var bytes: usize = 0;
    while (n < @min(all.items.len, limit)) : (n += 1) {
        bytes += wireSize(all.items[n]);
        if (bytes > PAGE_BYTES) break;
    }
    const more = all.items.len > n or landed.pieces.len > limit;
    const page = all.items[0..n];
    // Held only when what isn't landed yet is in this page: one the events
    // in hand fill, or wholly before or after the gap, is answered now.
    if (!final and missing.items.len > 0) for (page) |e| {
        if (std.mem.eql(u8, e.event_type, MISSING) and e.seq == missing.items[0].seq) return .wait;
    };
    // The next page starts past every seq this one covered, a gap's too.
    const next: ?u64 = if (!more or page.len == 0) null else if (window.newest_first) page[page.len - 1].seq else page[page.len - 1].last + 1;
    return .{ .page = .{ .events = page, .more = next } };
}

/// The events `records` (a chunk's, or a run's pending) holds within
/// `window`, oldest first, onto `out`.
pub fn decodeRecords(a: Allocator, records: []const u8, window: Window, out: *std.ArrayListUnmanaged(SeqEvent)) error{OutOfMemory}!void {
    var diag: schema.Diagnostic = .{};
    var r: schema.Reader = .{ .bytes = records, .diag = &diag };
    while (r.pos < records.len) {
        // What a producer encoded: one that doesn't decode ends what is read
        // of it.
        const rec = hp.HistRec.decode(&r, "event") catch return;
        if (rec.seq < window.from or rec.seq >= window.to) continue;
        try out.append(a, SeqEvent.one(rec));
    }
}

/// `page` as the answer's value, its events encoded into `a`.
pub fn response(a: Allocator, page: Page) error{OutOfMemory}!Response.Value {
    var size: usize = 0;
    for (page.events) |e| size += wireSize(e);
    const buf = try a.alloc(u8, size);
    var w: schema.Writer = .{ .buf = buf };
    for (page.events) |e| Event.encode(&w, e.wire()) catch unreachable; // sized above
    return .{ .events = .{ .bytes = w.written(), .count = @intCast(page.events.len) }, .has_more = page.more != null, .cursor = page.more };
}

/// The waiter key of a held read of `run` in `family`: the family, then
/// the run's key in its namespace.
pub const KEY_LEN = 1 + 4 + he.MAX_RUN;
pub fn waiterKey(buf: *[KEY_LEN]u8, family: he.Family, ns: NamespaceId, run: []const u8) error{TooLong}![]const u8 {
    if (run.len > he.MAX_RUN) return error.TooLong;
    buf[0] = @backingInt(family);
    const k = contract_view.idKey(buf[1..], ns, run) catch return error.TooLong;
    return buf[0 .. 1 + k.len];
}

/// The family and run key a waiter key names.
pub fn ofWaiterKey(key: []const u8) ?struct { family: he.Family, run_key: []const u8 } {
    if (key.len < 1 + 4) return null;
    const family = std.enums.fromInt(he.Family, key[0]) orelse return null;
    return .{ .family = family, .run_key = key[1..] };
}

const testing = std.testing;

test "history read: an event's every field at its largest fits a page" {
    const ev: [hp.MAX_EVENT]u8 = @splat('e');
    const d: [hp.MAX_DETAIL]u8 = @splat('d');
    const x: [hp.MAX_EXTRA]u8 = @splat('x');
    const e: SeqEvent = .{ .seq = 0, .last = 0, .event_type = &ev, .detail = &d, .extra = &x, .timestamp_ms = 1 };
    try testing.expect(wireSize(e) <= PAGE_BYTES);
    const v = try response(testing.allocator, .{ .events = &.{e}, .more = 7 });
    defer testing.allocator.free(v.events.bytes);
    const buf = try testing.allocator.alloc(u8, try Response.encodedSize(v));
    defer testing.allocator.free(buf);
    const bytes = try Response.encodeInto(buf, v);
    var diag: schema.Diagnostic = .{};
    const back = try Response.decodeAll(bytes, &diag);
    var it = back.events.iterator();
    const got = (try it.next()).?;
    try testing.expectEqual(@as(usize, hp.MAX_DETAIL), got.detail.len);
    try testing.expectEqual(@as(usize, hp.MAX_EXTRA), got.extra.len);
    try testing.expectEqual(@as(?u64, 7), back.cursor);
}
