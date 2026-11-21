//! Channels: a write to a slot another group owns is a message, sent in the
//! apply that causes it and applied once, in order, by its receiver.
//!
//! - **Sending is state.** An entry's sends (`View.send`) are written to its
//!   outbox in the same commit: `out|to|seq`, with the sending entry's index,
//!   and `next_seq` moves on. A send to this group's own slots goes through
//!   its own channel and comes back as its own `inbound` entry.
//! - **Receiving is an entry.** `channel_inbound{from, first_seq, messages}`
//!   applies each message through the machine that owns its entry type, as
//!   if it were that entry, at the inbound's index and stamp. A message at
//!   or below `in_hwm` is a duplicate and skipped; one past `in_hwm + 1` is a
//!   gap, which no correct delivery makes, and halts. A refused message
//!   undoes only its own writes and still takes its seq, and so does one
//!   for a slot this group doesn't own, or one whose own sends are past an
//!   apply's limits. What a message sends is written under the inbound's
//!   index, as if sent by an entry of its own.
//! - **One index, many messages.** Every message in an inbound applies at
//!   its index and term. What a machine keys by them stays unique: a KV
//!   write's `lsn` is informational (its `version` orders the key's
//!   writes); a delivery's `{term, index}` token is always checked with
//!   the item's delivery count, which every delivery raises, so two
//!   deliveries in one inbound still differ; a record located by index
//!   takes `(index, view.message.ordinal)`.
//! - **Trims** drop what the receiver durably applied.
//! - **Lanes.** Each pair of groups has three channels, each its own seqs,
//!   outbox, marks and trims: `sink` for a sink's messages (`Flags.SINK`),
//!   the only ones that park and the only ones a full target holds back;
//!   `history` for the messages a run's history rides (`Flags.HISTORY`),
//!   only ever to its sender's own group; and `main` for the rest. So no
//!   message waits behind a sink's whose target is full.
//!
//! Delivering, acks and resends are the leader's (`channel_driver.zig`):
//! nothing here is node-local, so every replica and every replay agree.

const std = @import("std");
const schema = @import("../protocol/schema.zig");
const ce = @import("../protocol/channel_entries.zig");
const view_mod = @import("view.zig");
const machine_mod = @import("machine.zig");
const delivery = @import("delivery.zig");
const persistence = @import("../storage/persistence.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const node_router = @import("../node/router.zig");
const store_mod = @import("store.zig");
const ns_table = @import("../namespace/table.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const View = view_mod.View;
const ReadView = view_mod.ReadView;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Record = schema.Record;
const Stored = delivery.Stored;

/// Most sends one apply makes, and their bytes in all.
pub const MAX_SENDS = 64;
pub const MAX_SEND_BYTES = 1 << 20;

/// How many `(index, in_hwm)` points a receiver keeps per sender.
pub const RING = 64;

// ─── Stored records ────────────────────────────────────────────────────────

/// A sent message, until its receiver durably applied it.
pub const Out = Record(&.{
    .{ .name = "slot", .T = Int(u16) },
    .{ .name = "entry_type", .T = Int(u8) },
    .{ .name = "layout_epoch", .T = Int(u32) },
    .{ .name = "flags", .T = Int(u16) },
    .{ .name = "origin_kind", .T = Int(u8) },
    .{ .name = "origin_ns", .T = Int(u32) },
    // The sending entry's index: delivered only once that is durable.
    .{ .name = "sent_at", .T = Int(u64) },
    .{ .name = "payload", .T = schema.Bytes(ce.MAX_MESSAGE_PAYLOAD) },
});

/// A channel's sending side: the next seq to give, and the highest seq its
/// receiver durably applied.
pub const Meta = Record(&.{
    .{ .name = "next_seq", .T = Int(u64) },
    .{ .name = "acked", .T = Int(u64) },
    // Payload bytes sent and not yet trimmed: what the outbox to this peer
    // holds, against its cap, held to before anything is sent to it.
    .{ .name = "bytes", .T = Int(u64) },
});

pub const Point = Record(&.{
    .{ .name = "index", .T = Int(u64) },
    .{ .name = "seq", .T = Int(u64) },
});

/// A channel's receiving side: the highest seq applied, and where it stood
/// at recent indexes, so a durable ack can name what's applied as of a
/// durable index. A point dropped off the ring becomes the floor; losing
/// points only makes the answer lower, never wrong.
pub const In = Record(&.{
    .{ .name = "applied", .T = Int(u64) },
    .{ .name = "floor_index", .T = Int(u64) },
    .{ .name = "floor_seq", .T = Int(u64) },
    .{ .name = "points", .T = schema.List(Point, RING) },
});

pub const OutFam = view_mod.Family(55, "channel.out", .shard, Stored(Out));
pub const MetaFam = view_mod.Family(56, "channel.out_meta", .shard, Stored(Meta));
pub const InFam = view_mod.Family(57, "channel.in", .shard, Stored(In));

/// A message parked under its target, which was full for it (`park`); a
/// target's count of parked messages and bytes, and since when it has had
/// any; and the shard's totals. One record shape for all three, by key:
/// `p` target from seq, `m` target, and `t`.
pub const Parked = Record(&.{
    .{ .name = "entry_type", .T = Int(u8) },
    .{ .name = "flags", .T = Int(u16) },
    // A parked message: when it parked. A target's: since when it has had
    // any parked, without a break.
    .{ .name = "since_ns", .T = Int(u64) },
    .{ .name = "count", .T = Int(u64) },
    .{ .name = "bytes", .T = Int(u64) },
    .{ .name = "payload", .T = schema.Bytes(ce.MAX_MESSAGE_PAYLOAD) },
});

pub const ParkFam = view_mod.Family(59, "channel.parked", .shard, Stored(Parked));

/// What one namespace's messages to one peer hold of its outbox, in
/// payload bytes: what its share is held to before it sends (`shareHeld`). A record only
/// while it holds any, so the family is as sparse as the traffic.
pub const NsBytes = Record(&.{
    .{ .name = "bytes", .T = Int(u64) },
});

pub const OutNsFam = view_mod.Family(54, "channel.out_ns", .shard, Stored(NsBytes));

pub const families = [_]machine_mod.FamilyInfo{ machine_mod.info(OutFam), machine_mod.info(MetaFam), machine_mod.info(InFam), machine_mod.info(ParkFam), machine_mod.info(OutNsFam) };

/// Per-namespace outbox keys live under a namespace of their own, so a trim
/// can declare the range of one peer's.
pub const OUT_NS = "~outns";
const OUT_NS_KEY = 1 + OUT_NS.len + CHAN_LEN + 4;

fn outNsKey(buf: *[OUT_NS_KEY]u8, to: u16, lane: Lane, ns: ns_table.Id) []const u8 {
    var raw: [CHAN_LEN + 4]u8 = undefined;
    raw[0..CHAN_LEN].* = chanBytes(to, lane);
    raw[CHAN_LEN..].* = ns.bytes();
    return view_mod.key(buf, OUT_NS, &raw) catch unreachable;
}

/// The prefix of every per-namespace outbox record to `to` on `lane`; the
/// namespace id is the key's last four bytes.
pub fn outNsPrefix(buf: *[1 + OUT_NS.len + CHAN_LEN]u8, to: u16, lane: Lane) []const u8 {
    const b = chanBytes(to, lane);
    return view_mod.key(buf, OUT_NS, &b) catch unreachable;
}

/// The payload bytes namespace `ns`'s messages to `to` on `lane` hold of
/// its outbox.
pub fn nsBytesOf(read: ReadView, to: u16, lane: Lane, ns: ns_table.Id) u64 {
    var kb: [OUT_NS_KEY]u8 = undefined;
    const v = read.get(OutNsFam, outNsKey(&kb, to, lane, ns)) catch null;
    return if (v) |x| x.v.bytes else 0;
}

/// Parked keys live under a namespace of their own, so an unpark can declare
/// the range of one target's parked messages.
pub const PARK_NS = "~park";
/// Most parked bytes under one target, and in a shard. Past either, its
/// receiver answers `busy` for that target's messages (`admits`).
pub const PARKED_TARGET_MAX: u64 = 16 << 20;
pub const PARKED_SHARD_MAX: u64 = 128 << 20;
/// Tests lower the caps rather than park megabytes.
pub var test_parked_target_max: ?u64 = null;
pub var test_parked_shard_max: ?u64 = null;
/// VOPR's planted bug `no_park_behind`: a message for a target with room is
/// applied even while earlier ones to it are parked. Never set in a server.
pub var mutant_no_park_behind = false;

pub const TARGET_LEN = ce.TARGET_LEN;
pub const Target = [ce.TARGET_LEN]u8;

/// What a receiving leader has proposed and not yet applied of sink
/// messages that may park: to one target, and to all.
pub const InFlight = struct { target: u64 = 0, total: u64 = 0 };

/// The target entry `e` writes as parking keys it, or null for none: its
/// machine's tag (the machine's first entry type, which no other machine
/// owns), then the machine's own name for it.
fn targetOf(owner: *const machine_mod.Machine, e: *const Entry) ?Target {
    const tf = owner.target orelse return null;
    const own = tf(e) orelse return null;
    var t: Target = undefined;
    t[0] = @backingInt(owner.entry_types[0]);
    t[1..].* = own;
    return t;
}

fn parkedRaw(buf: *[1 + ce.TARGET_LEN + 2 + 8]u8, target: Target, from: u16, seq: u64) []const u8 {
    buf[0] = 'p';
    buf[1..][0..ce.TARGET_LEN].* = target;
    std.mem.writeInt(u16, buf[1 + ce.TARGET_LEN ..][0..2], from, .big);
    std.mem.writeInt(u64, buf[1 + ce.TARGET_LEN + 2 ..][0..8], seq, .big);
    return buf;
}

fn targetRaw(buf: *[1 + ce.TARGET_LEN]u8, target: Target) []const u8 {
    buf[0] = 'm';
    buf[1..].* = target;
    return buf;
}

const TOTALS_RAW = "t";

/// A parked key as the family stores it: under `PARK_NS`.
fn parkKey(buf: []u8, raw: []const u8) []const u8 {
    return view_mod.key(buf, PARK_NS, raw) catch unreachable;
}

const PARK_KEY_MAX = 1 + PARK_NS.len + 1 + ce.TARGET_LEN + 2 + 8;

const empty_parked: Parked.Value = .{ .entry_type = 0, .flags = 0, .since_ns = 0, .count = 0, .bytes = 0, .payload = "" };

/// A target's parked count, bytes and since; zeros for none.
pub fn targetParked(read: ReadView, target: Target) Parked.Value {
    var rb: [1 + ce.TARGET_LEN]u8 = undefined;
    var kb: [PARK_KEY_MAX]u8 = undefined;
    const v = read.get(ParkFam, parkKey(&kb, targetRaw(&rb, target))) catch null;
    return if (v) |x| x.v else empty_parked;
}

/// The shard's parked count and bytes; zeros for none.
pub fn shardParked(read: ReadView) Parked.Value {
    var kb: [PARK_KEY_MAX]u8 = undefined;
    const v = read.get(ParkFam, parkKey(&kb, TOTALS_RAW)) catch null;
    return if (v) |x| x.v else empty_parked;
}

/// The keys parking a message under `target` may touch.
fn parkKeys(out: *KeySet, target: Target, from: u16, seq: u64) error{OutOfMemory}!void {
    var pb: [1 + ce.TARGET_LEN + 2 + 8]u8 = undefined;
    var rb: [1 + ce.TARGET_LEN]u8 = undefined;
    try out.add(ParkFam.ID, PARK_NS, parkedRaw(&pb, target, from, seq));
    try out.add(ParkFam.ID, PARK_NS, targetRaw(&rb, target));
    try out.add(ParkFam.ID, PARK_NS, TOTALS_RAW);
}

/// Parks a message under `target`: kept whole, by sender and seq, until an
/// unpark applies it. `stamp` is when.
fn park(view: *View, target: Target, from: u16, seq: u64, m: ce.Message.Value, stamp: u64) ApplyError!void {
    var pb: [1 + ce.TARGET_LEN + 2 + 8]u8 = undefined;
    var kb: [PARK_KEY_MAX]u8 = undefined;
    try view.put(ParkFam, parkKey(&kb, parkedRaw(&pb, target, from, seq)), .{ .v = .{ .entry_type = m.entry_type, .flags = m.flags, .since_ns = stamp, .count = 1, .bytes = m.payload.len, .payload = m.payload } });
    try addParked(view, target, 1, @intCast(m.payload.len), stamp);
}

/// Moves a target's and the shard's parked counts by `n` messages of
/// `bytes`, up or down.
fn addParked(view: *View, target: Target, n: i64, bytes: i64, stamp: u64) ApplyError!void {
    var rb: [1 + ce.TARGET_LEN]u8 = undefined;
    var kb: [PARK_KEY_MAX]u8 = undefined;
    const tk = parkKey(&kb, targetRaw(&rb, target));
    var t: Parked.Value = if (try view.get(ParkFam, tk)) |v| v.v else empty_parked;
    if (t.count == 0) t.since_ns = stamp;
    t.count = @intCast(@as(i64, @intCast(t.count)) + n);
    t.bytes = @intCast(@as(i64, @intCast(t.bytes)) + bytes);
    if (t.count == 0) try view.delete(ParkFam, tk) else try view.put(ParkFam, tk, .{ .v = t });
    var sb: [PARK_KEY_MAX]u8 = undefined;
    const sk = parkKey(&sb, TOTALS_RAW);
    var s: Parked.Value = if (try view.get(ParkFam, sk)) |v| v.v else empty_parked;
    s.count = @intCast(@as(i64, @intCast(s.count)) + n);
    s.bytes = @intCast(@as(i64, @intCast(s.bytes)) + bytes);
    if (s.count == 0) try view.delete(ParkFam, sk) else try view.put(ParkFam, sk, .{ .v = s });
}

// ─── Keys ──────────────────────────────────────────────────────────────────

/// A peer group as a key names it: the group, then its incarnation, which
/// is 0 until a split gives a group a new one.
pub const PEER_LEN = 4;
/// A channel: its peer group, then its lane.
pub const CHAN_LEN = PEER_LEN + 1;
pub const CHAN_KEY = 1 + CHAN_LEN;
pub const OUT_KEY = 1 + CHAN_LEN + 8;

/// A channel's lane. Each has its own seqs, outbox and marks, so none
/// waits on another.
pub const Lane = enum(u8) {
    main = 0,
    /// A run's history, and nothing else: only `Flags.HISTORY` messages,
    /// only to the sender's own group, and none parks.
    history = 1,
    /// A sink's writes (`Flags.SINK`): the only messages that park, and the
    /// only ones a full target's pressure holds back.
    sink = 2,

    pub const all = [_]Lane{ .main, .history, .sink };

    /// The lane a message with `flags` rides. One flagged both a history's
    /// and a sink's is a Flo bug, refused where it is sent and taken.
    pub fn of(flags: u16) Lane {
        if (flags & entry_mod.Flags.HISTORY != 0) return .history;
        if (flags & entry_mod.Flags.SINK != 0) return .sink;
        return .main;
    }

    pub fn from(byte: u8) ?Lane {
        return std.enums.fromInt(Lane, byte);
    }
};
pub const LANES = Lane.all.len;

pub fn peerBytes(peer: u16) [PEER_LEN]u8 {
    var b: [PEER_LEN]u8 = undefined;
    std.mem.writeInt(u16, b[0..2], peer, .big);
    std.mem.writeInt(u16, b[2..4], 0, .big);
    return b;
}

pub fn chanBytes(peer: u16, lane: Lane) [CHAN_LEN]u8 {
    var b: [CHAN_LEN]u8 = undefined;
    b[0..PEER_LEN].* = peerBytes(peer);
    b[PEER_LEN] = @backingInt(lane);
    return b;
}

/// An outbox record's key after its namespace: the receiver and lane,
/// then the seq, big-endian so a range is in seq order.
pub fn outBytes(to: u16, lane: Lane, seq: u64) [CHAN_LEN + 8]u8 {
    var b: [CHAN_LEN + 8]u8 = undefined;
    b[0..CHAN_LEN].* = chanBytes(to, lane);
    std.mem.writeInt(u64, b[CHAN_LEN..][0..8], seq, .big);
    return b;
}

/// A channel's peer group and lane, under no namespace.
pub fn chanKey(buf: *[CHAN_KEY]u8, peer: u16, lane: Lane) []const u8 {
    const b = chanBytes(peer, lane);
    return view_mod.key(buf, "", &b) catch unreachable;
}

pub fn outKey(buf: *[OUT_KEY]u8, to: u16, lane: Lane, seq: u64) []const u8 {
    const b = outBytes(to, lane, seq);
    return view_mod.key(buf, "", &b) catch unreachable;
}

/// The prefix of every outbox record to `to` on `lane`.
pub fn outPrefix(buf: *[CHAN_KEY]u8, to: u16, lane: Lane) []const u8 {
    return chanKey(buf, to, lane);
}

// ─── Sending ───────────────────────────────────────────────────────────────

/// Why an apply's sends are refused, or null.
pub fn tooMuchSent(sends: []const view_mod.Send) ?[]const u8 {
    if (sends.len > MAX_SENDS) return "bad request: an apply sends at most 64 messages";
    var bytes: usize = 0;
    for (sends) |s| {
        if (s.payload.len > ce.MAX_MESSAGE_PAYLOAD) return "bad request: a message is larger than one entry holds";
        bytes += s.payload.len;
    }
    if (bytes > MAX_SEND_BYTES) return "bad request: an apply sends at most 1 MiB";
    return null;
}

/// The group that owns `slot`, as the node's router says.
pub fn ownerOf(place: view_mod.Place, slot: u16) u16 {
    return node_router.Router.init(place.count, place.group).slotToShard(slot);
}

/// The view's sends into the outbox, under the sending entry's `index`.
pub fn writeSends(view: *View, index: u64) ApplyError!void {
    const place = view.place orelse return error.Halt;
    if (place.count == 0) return error.Halt;
    for (view.sends.items) |s| {
        // A slot past the table is a Flo bug in the sender.
        if (s.slot >= node_router.SLOTS) return error.Halt;
        const to = ownerOf(place, s.slot);
        const lane = Lane.of(s.flags);
        // History stays on its own group, and never parks: a history
        // message to another, or one that is also a sink's, is a Flo bug.
        if (lane == .history and (to != place.group or s.flags & entry_mod.Flags.SINK != 0)) return error.Halt;
        var mk: [CHAN_KEY]u8 = undefined;
        const meta_key = chanKey(&mk, to, lane);
        var meta: Meta.Value = if (try view.getOwn(MetaFam, meta_key)) |m| m.v else .{ .next_seq = 1, .acked = 0, .bytes = 0 };
        var ok: [OUT_KEY]u8 = undefined;
        try view.putOwn(OutFam, outKey(&ok, to, lane, meta.next_seq), .{ .v = .{
            .slot = s.slot,
            .entry_type = s.entry_type,
            .layout_epoch = 0,
            .flags = s.flags,
            .origin_kind = @backingInt(s.origin.kind),
            .origin_ns = s.origin.ns.int(),
            .sent_at = index,
            .payload = s.payload,
        } });
        meta.next_seq += 1;
        meta.bytes += s.payload.len;
        try view.putOwn(MetaFam, meta_key, .{ .v = meta });
        var nk: [OUT_NS_KEY]u8 = undefined;
        const ns_key = outNsKey(&nk, to, lane, s.origin.ns);
        const held: u64 = if (try view.getOwn(OutNsFam, ns_key)) |v| v.v.bytes else 0;
        try view.putOwn(OutNsFam, ns_key, .{ .v = .{ .bytes = held + s.payload.len } });
    }
    view.sends.clearRetainingCapacity();
}

// ─── Reading, for the driver ───────────────────────────────────────────────

pub fn metaOf(read: ReadView, to: u16, lane: Lane) Meta.Value {
    var mk: [CHAN_KEY]u8 = undefined;
    const m = read.get(MetaFam, chanKey(&mk, to, lane)) catch null;
    return if (m) |v| v.v else .{ .next_seq = 1, .acked = 0, .bytes = 0 };
}

/// The payload bytes the outbox to `to` holds, over both lanes: what its
/// caps are held to.
pub fn bytesTo(read: ReadView, to: u16) u64 {
    var n: u64 = 0;
    for (Lane.all) |lane| n += metaOf(read, to, lane).bytes;
    return n;
}

/// What namespace `ns`'s messages to `to` hold of its outbox, over both
/// lanes.
pub fn nsBytesTo(read: ReadView, to: u16, ns: ns_table.Id) u64 {
    var n: u64 = 0;
    for (Lane.all) |lane| n += nsBytesOf(read, to, lane, ns);
    return n;
}

pub fn inOf(read: ReadView, from: u16, lane: Lane) ?In.Value {
    var ik: [CHAN_KEY]u8 = undefined;
    const v = read.get(InFam, chanKey(&ik, from, lane)) catch null;
    return if (v) |x| x.v else null;
}

/// The highest seq from `from` applied as of index `durable`: what a
/// durable ack may say.
pub fn appliedAsOf(in: In.Value, durable: u64) u64 {
    var best: ?u64 = null;
    var it = in.points.iterator();
    while (it.next() catch null) |p| {
        if (p.index <= durable) best = p.seq;
    }
    if (best) |b| return b;
    return if (in.floor_index <= durable) in.floor_seq else 0;
}

// ─── The machine ───────────────────────────────────────────────────────────

fn typeOf(e: *const Entry) ?EntryType {
    return std.enums.fromInt(EntryType, e.header.entry_type);
}

/// How many messages `target` has parked, as this view sees it.
fn targetParkedIn(view: *View, target: Target) ApplyError!u64 {
    var rb: [1 + ce.TARGET_LEN]u8 = undefined;
    var kb: [PARK_KEY_MAX]u8 = undefined;
    return if (try view.get(ParkFam, parkKey(&kb, targetRaw(&rb, target)))) |v| v.v.count else 0;
}

/// A parked message as the entry its target applies: the unpark's header,
/// with the message's type, flags and payload.
fn unparkedEntry(e: *const Entry, u: ce.Unparked.Value) Entry {
    var h = e.header;
    h.entry_type = u.entry_type;
    h.flags = u.flags;
    h.payload_len = @intCast(u.payload.len);
    return .{ .header = h, .payload = u.payload };
}

/// Why message `e` can't be applied by `owner` for the namespace it names,
/// or null: an id no namespace could have (`.invalid`, or past the
/// table's capacity). Read from the payload alone, so every replica agrees.
/// Whether a namespace is live isn't checked: no shard but shard 0 logs
/// namespaces, so that can't be decided at apply here.
fn namespaceRefusal(owner: *const machine_mod.Machine, e: *const Entry) ?[]const u8 {
    const nf = owner.namespace orelse return null;
    const id = nf(e) orelse return null;
    if (id == ns_table.INVALID.int() or id >= ns_table.CAPACITY) return "bad request: a message naming a namespace id none could have";
    return null;
}

/// A message as the entry its target applies: the inbound's header, with
/// the message's type, flags and payload.
fn asEntry(e: *const Entry, m: ce.Message.Value) Entry {
    var h = e.header;
    h.entry_type = m.entry_type;
    h.flags = m.flags;
    h.payload_len = @intCast(m.payload.len);
    return .{ .header = h, .payload = m.payload };
}

/// An origin refusal: why, and whether for a reserved namespace, which is
/// refused as unauthorized rather than as built wrong.
const OriginRefused = struct { why: []const u8, reserved: bool = false };

/// Why a message `m`, for `owner`, may not be applied for its origin, or null: its origin has no kind or
/// names no namespace, a user sends for a reserved one, its payload names
/// a namespace other than its origin's, or the keys its owner declares for
/// it reach outside that namespace. Its sender built it wrong, so it is
/// refused alone and counted as the sender's bug. Run before it parks or
/// applies. The check binds the keys declared; that an apply stays inside
/// what it declared is the applier's existing contract (`strict_keys` in
/// debug builds), across every message of the inbound.
fn originRefusal(comptime scopes: anytype, owner: ?*const machine_mod.Machine, as: *const Entry, m: ce.Message.Value, arena: std.mem.Allocator) error{OutOfMemory}!?OriginRefused {
    const kind = std.enums.fromInt(view_mod.Origin.Kind, m.origin_kind) orelse return .{ .why = "bad request: a message with no origin" };
    const ns: ns_table.Id = .of(m.origin_ns);
    if (ns == .invalid or m.origin_ns >= ns_table.CAPACITY) return .{ .why = "bad request: a message from no namespace" };
    if (ns == ns_table.SYSTEM and kind != .system) return .{ .why = "unauthorized: a user's message to the system's namespace", .reserved = true };
    const o = owner orelse return null;
    if (o.namespace) |nf| if (nf(as)) |named| if (named != m.origin_ns) return .{ .why = "bad request: a message writing outside its origin's namespace" };
    var declared = KeySet.init(arena);
    try o.keys(as, &declared);
    for ([_][]const view_mod.FamKey{ declared.exact.items, declared.ranges.items }) |list| for (list) |k| {
        const scope = scopes[k.fam] orelse return .{ .why = "bad request: a message reaching a family no machine declares" };
        if (!keyInside(scope, kind, ns, k.key)) return .{ .why = "bad request: a message reaching keys outside its origin's namespace" };
    };
    return null;
}

/// Whether a message from origin `kind` in `ns` may reach `key` (or the
/// keys it prefixes) of a family of `scope`.
fn keyInside(scope: view_mod.Scope, kind: view_mod.Origin.Kind, ns: ns_table.Id, key: []const u8) bool {
    const id = ns.bytes();
    return switch (scope) {
        .id => key.len >= id.len and std.mem.eql(u8, key[0..id.len], &id),
        .naming => |at| key.len >= 1 + at + id.len and key[0] == 0 and std.mem.eql(u8, key[1 + at ..][0..id.len], &id),
        .aggregate => true,
        .shard => kind == .system,
    };
}

/// Each family's scope by id, over `machines`' families; null for an id
/// none declares, which no message may reach: refused, whatever its origin.
fn scopesOf(comptime machines: []const machine_mod.Machine) [store_mod.MAX_FAMILIES]?view_mod.Scope {
    var out: [store_mod.MAX_FAMILIES]?view_mod.Scope = @splat(null);
    for (machines) |mm| for (mm.families) |f| {
        out[f.id] = f.scope;
    };
    return out;
}

/// The channels machine over `targets`, the machines a message may be for.
pub fn Channels(comptime targets: []const machine_mod.Machine) type {
    return struct {
        const scopes = scopesOf(targets);

        fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
            var diag: schema.Diagnostic = .{};
            switch (typeOf(e) orelse return) {
                .channel_inbound => {
                    const p = ce.Inbound.decodeAll(e.payload, &diag) catch return;
                    const b = chanBytes(p.from, Lane.from(p.lane) orelse return);
                    try out.add(InFam.ID, "", &b);
                    var it = p.messages.iterator();
                    var seq = p.first_seq;
                    while (it.next() catch return) |m| : (seq += 1) {
                        const t = std.enums.fromInt(EntryType, m.entry_type) orelse continue;
                        const owner = machine_mod.ownerOf(targets, t) orelse continue;
                        const as = asEntry(e, m);
                        try owner.keys(&as, out);
                        // A sink's, which may park.
                        if (m.flags & entry_mod.Flags.SINK != 0) if (targetOf(owner, &as)) |target| try parkKeys(out, target, p.from, seq);
                    }
                },
                .channel_unpark => {
                    const p = ce.Unpark.decodeAll(e.payload, &diag) catch return;
                    if (p.target.len != ce.TARGET_LEN) return;
                    const target: Target = p.target[0..ce.TARGET_LEN].*;
                    // Each sender's parked messages under the target, to
                    // check each is its sender's next.
                    var rb: [1 + ce.TARGET_LEN]u8 = undefined;
                    _ = targetRaw(&rb, target);
                    rb[0] = 'p';
                    out.addRange(ParkFam.ID, PARK_NS, &rb) catch |err| switch (err) {
                        error.NoNamespace => unreachable,
                        else => |o| return o,
                    };
                    var it = p.messages.iterator();
                    while (it.next() catch return) |u| {
                        const t = std.enums.fromInt(EntryType, u.entry_type) orelse continue;
                        const owner = machine_mod.ownerOf(targets, t) orelse continue;
                        const as = unparkedEntry(e, u);
                        try owner.keys(&as, out);
                        try parkKeys(out, target, u.from, u.seq);
                    }
                },
                .channel_trim => {
                    const p = ce.Trim.decodeAll(e.payload, &diag) catch return;
                    const lane = Lane.from(p.lane) orelse return;
                    const b = chanBytes(p.to, lane);
                    try out.add(MetaFam.ID, "", &b);
                    // Each namespace's share of what it deletes.
                    out.addRange(OutNsFam.ID, OUT_NS, &b) catch |err| switch (err) {
                        error.NoNamespace => unreachable,
                        else => |o| return o,
                    };
                    if (p.through < p.first or p.through - p.first >= ce.MAX_TRIM_SPAN) return;
                    var seq = p.first;
                    while (seq <= p.through) : (seq += 1) {
                        const k = outBytes(p.to, lane, seq);
                        try out.add(OutFam.ID, "", &k);
                    }
                },
                else => {},
            }
        }

        fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
            switch (typeOf(e) orelse return error.Halt) {
                .channel_inbound => try applyInbound(view, e),
                .channel_trim => try applyTrim(view, e),
                .channel_unpark => try applyUnpark(view, e),
                else => {},
            }
            _ = out;
        }

        fn applyInbound(view: *View, e: *const Entry) ApplyError!void {
            var diag: schema.Diagnostic = .{};
            const store = view.store;
            store.messages.clearRetainingCapacity();
            store.applying = null;
            const skip = store.inbound_skip;
            const place = view.place orelse return error.Halt;
            // Only a leader makes these, from its own outbox's frames: one that
            // doesn't decode is a Flo bug.
            const p = ce.Inbound.decodeAll(e.payload, &diag) catch return error.Halt;
            const lane = Lane.from(p.lane) orelse return error.Halt;
            // History only ever comes from this group itself (`writeSends`).
            if (lane == .history and p.from != place.group) return error.Halt;
            var ik: [CHAN_KEY]u8 = undefined;
            const in_key = chanKey(&ik, p.from, lane);
            const before: ?In.Value = if (try view.get(InFam, in_key)) |v| v.v else null;
            var applied = if (before) |b| b.applied else 0;
            var seq = p.first_seq;
            var ordinal: u8 = 0;
            var it = p.messages.iterator();
            while (it.next() catch return error.Halt) |m| : ({
                seq += 1;
                ordinal += 1;
            }) {
                if (seq <= applied) continue;
                if (seq != applied + 1) return error.Halt;
                // A repair skipped it: answered, never applied, and its seq
                // is taken, so a resend is a duplicate.
                if (skip) |*sk| if (sk.skips(seq)) {
                    try skipMessage(view, e, m, p.from, lane, seq, ordinal, sk);
                    applied = seq;
                    continue;
                };
                store.applying = .{ .from = p.from, .seq = seq, .lane = lane };
                // Only a leader's own frames make these: a flag no message
                // may carry, one on the other lane's, or a sink's on the
                // history lane (where nothing parks: parked keys carry no
                // lane) is a Flo bug in the sender.
                if (m.flags & ~view_mod.MESSAGE_FLAGS != 0) return error.Halt;
                if (Lane.of(m.flags) != lane) return error.Halt;
                if (lane == .history and m.flags & entry_mod.Flags.SINK != 0) return error.Halt;
                const t = std.enums.fromInt(EntryType, m.entry_type) orelse return error.Halt;
                const owner = machine_mod.ownerOf(targets, t);
                if (owner == null) return error.Halt;
                const as = asEntry(e, m);
                const mark = view.mark();
                view.message = .{ .from = p.from, .seq = seq, .ordinal = ordinal };
                defer view.message = null;
                var r = Result.start(&as);
                var bug: ?store_mod.SenderBug = null;
                // A message for a slot this group doesn't own was sent to
                // the wrong group: refused, never applied over another's state.
                if (m.slot >= node_router.SLOTS or ownerOf(place, m.slot) != place.group) {
                    r.refuse(.bad_request, .malformed, "bad request: a message for a slot this group doesn't own");
                    bug = .wrong_slot;
                } else if (if (owner) |o| (if (o.malformed) |f| f(&as) else null) else null) |why| {
                    // Its payload can't apply: built wrong.
                    r.refuse(.bad_request, .malformed, why);
                    bug = .bad_payload;
                } else if (if (owner) |o| namespaceRefusal(o, &as) else null) |why| {
                    // A namespace id no namespace could have: built wrong.
                    r.refuse(.bad_request, .malformed, why);
                    bug = .bad_payload;
                } else if (try originRefusal(scopes, owner, &as, m, view.arena)) |o| {
                    r.refuse(if (o.reserved) .unauthorized else .bad_request, if (o.reserved) .reserved_key else .malformed, o.why);
                    bug = .wrong_origin;
                } else if (owner) |o| {
                    const sink = m.flags & entry_mod.Flags.SINK != 0;
                    const target = targetOf(o, &as);
                    // A sink's write to a target with messages parked takes
                    // it after them: it parks behind, even if it has room now.
                    if (sink and target != null and !mutant_no_park_behind and try targetParkedIn(view, target.?) > 0) {
                        try park(view, target.?, p.from, seq, m, e.header.timestamp_ns);
                        applied = seq;
                        continue;
                    }
                    try o.apply(view, &as, &r);
                    // A sink's write its target is full for: parked, not
                    // lost; it takes its seq, and the channel goes on.
                    if (sink and target != null) if (r.refused()) |rf| if (rf.status == .overloaded) {
                        try view.rollback(mark);
                        try park(view, target.?, p.from, seq, m, e.header.timestamp_ns);
                        applied = seq;
                        continue;
                    };
                    // Its sends are held to an apply's limits on their own,
                    // and leave with it.
                    if (r.refused() == null) if (tooMuchSent(view.sends.items[mark.sends..])) |why| r.refuse(.bad_request, .malformed, why);
                }
                if (r.refused() != null) try view.rollback(mark) else try writeSends(view, e.header.index);
                try store.messages.append(store.allocator, .{ .entry = as, .result = r, .ordinal = ordinal, .from = p.from, .seq = seq, .sender_bug = bug });
                store.applying = null;
                applied = seq;
            }
            if (before != null and applied == before.?.applied) return;
            try view.put(InFam, in_key, .{ .v = try withPoint(view.arena, before, applied, e.header.index) });
        }

        /// A message a repair skipped: refused `skipped_by_repair`, naming the
        /// halted entry and the message, and answered as its type says
        /// (`Machine.skipped`): a request's sender is told, an answer to this
        /// shard's own call settles it. Its sends leave under the inbound's
        /// index, as an applied message's do.
        fn skipMessage(view: *View, e: *const Entry, m: ce.Message.Value, from: u16, lane: Lane, seq: u64, ordinal: u8, sk: *const store_mod.InboundSkip) ApplyError!void {
            const store = view.store;
            const as = asEntry(e, m);
            var r = Result.start(&as);
            var buf: [@import("../protocol/action_entries.zig").MAX_SKIP_WHY]u8 = undefined;
            // A message skipped with the rest of its inbound says which one
            // halted the group, so its caller can tell it wasn't the cause.
            // A history message's seq is its lane's, so the lane is named.
            const on: []const u8 = if (lane == .history) ", history lane" else "";
            const why = (if (sk.whole and sk.halt_seq != 0 and sk.halt_seq != seq)
                std.fmt.bufPrint(&buf, "skipped by repair (entry {d}, halted at message {d}; message {d} from shard {d}{s})", .{ sk.index, sk.halt_seq, seq, from, on })
            else
                std.fmt.bufPrint(&buf, "skipped by repair (entry {d}, message {d} from shard {d}{s})", .{ sk.index, seq, from, on })) catch "skipped by repair";
            r.refuse(.conflict, .skipped_by_repair, why);
            const t = std.enums.fromInt(EntryType, m.entry_type);
            const owner = if (t) |x| machine_mod.ownerOf(targets, x) else null;
            if (owner) |o| if (o.skipped) |answer| {
                view.message = .{ .from = from, .seq = seq, .ordinal = ordinal };
                defer view.message = null;
                try answer(view, &as, why);
                try writeSends(view, e.header.index);
            };
            try store.messages.append(store.allocator, .{ .entry = as, .result = r, .ordinal = ordinal, .from = from, .seq = seq, .skipped = true });
        }

        /// A target's parked messages its leader saw room for: each still its
        /// sender's next parked one is applied through its machine, as it
        /// would have been on arrival, in order, until one doesn't fit.
        /// That one and those after stay parked. One its target refuses for
        /// good leaves parking refused, reported as any message is.
        fn applyUnpark(view: *View, e: *const Entry) ApplyError!void {
            var diag: schema.Diagnostic = .{};
            const store = view.store;
            store.messages.clearRetainingCapacity();
            const p = ce.Unpark.decodeAll(e.payload, &diag) catch return error.Halt;
            if (p.target.len != ce.TARGET_LEN) return error.Halt;
            const target: Target = p.target[0..ce.TARGET_LEN].*;
            var ordinal: u8 = 0;
            var it = p.messages.iterator();
            while (it.next() catch return error.Halt) |u| : (ordinal += 1) {
                var pb: [1 + ce.TARGET_LEN + 2 + 8]u8 = undefined;
                var kb: [PARK_KEY_MAX]u8 = undefined;
                const raw = parkedRaw(&pb, target, u.from, u.seq);
                const key = parkKey(&kb, raw);
                // Applied by an earlier unpark, or not its sender's next: a
                // stale unpark stops here, and changes nothing more.
                if (try view.get(ParkFam, key) == null) break;
                if (!try firstOfSender(view, raw)) break;
                const t = std.enums.fromInt(EntryType, u.entry_type) orelse return error.Halt;
                const owner = machine_mod.ownerOf(targets, t) orelse return error.Halt;
                const as = unparkedEntry(e, u);
                const mark = view.mark();
                view.message = .{ .from = u.from, .seq = u.seq, .ordinal = ordinal };
                defer view.message = null;
                var r = Result.start(&as);
                if (namespaceRefusal(owner, &as)) |why| r.refuse(.bad_request, .malformed, why) else try owner.apply(view, &as, &r);
                if (r.refused()) |rf| if (rf.status == .overloaded) {
                    try view.rollback(mark);
                    break;
                };
                if (r.refused() == null) if (tooMuchSent(view.sends.items[mark.sends..])) |why| r.refuse(.bad_request, .malformed, why);
                if (r.refused() != null) try view.rollback(mark) else try writeSends(view, e.header.index);
                try view.delete(ParkFam, key);
                try addParked(view, target, -1, -@as(i64, @intCast(u.payload.len)), e.header.timestamp_ns);
                try store.messages.append(store.allocator, .{ .entry = as, .result = r, .ordinal = ordinal, .from = u.from, .seq = u.seq });
            }
        }

        /// A message as an entry, for its machine's `target` and `room`,
        /// which read only its type and payload.
        fn bare(etype: u8, flags: u16, payload: []const u8) Entry {
            var h = std.mem.zeroes(entry_mod.Header);
            h.entry_type = etype;
            h.flags = flags;
            h.payload_len = @intCast(payload.len);
            return .{ .header = h, .payload = payload };
        }

        /// Whether this group's leader takes message `m` in now. Only a
        /// sink's message to a target may park. Past the shard's parked
        /// cap none is taken; past its target's, none for that target.
        /// The driver proposes a batch's messages before the first not
        /// taken, and that one and those after wait at their sender
        /// (`busy`), so only a sink to a full target, or one with as much
        /// on its way as would fill it, pushes back.
        ///
        /// Parked bytes are read from committed state, and the bytes this
        /// leader proposed to the target and hasn't applied (`in_flight`,
        /// leader-local) count beside them, so a cap is passed by at most
        /// one message: the one admitted below it. A new leader proposes
        /// only once its predecessor's inbounds have applied, so none is
        /// in flight uncounted.
        pub fn admits(read: ReadView, m: ce.Message.Value, in_flight: InFlight) bool {
            const target = sinkTarget(m) orelse return true;
            if (shardParked(read).bytes + in_flight.total >= (test_parked_shard_max orelse PARKED_SHARD_MAX)) return false;
            const parked = targetParked(read, target);
            if (parked.count == 0 and in_flight.target == 0) return true;
            return parked.bytes + in_flight.target < (test_parked_target_max orelse PARKED_TARGET_MAX);
        }

        /// The target a sink's message `m` may park under, or null for one
        /// that never parks: not a sink's, or for a machine with no targets.
        pub fn sinkTarget(m: ce.Message.Value) ?Target {
            if (m.flags & entry_mod.Flags.SINK == 0) return null;
            const t = std.enums.fromInt(EntryType, m.entry_type) orelse return null;
            const owner = machine_mod.ownerOf(targets, t) orelse return null;
            const as = bare(m.entry_type, m.flags, m.payload);
            return targetOf(owner, &as);
        }

        /// What this group has parked: messages, bytes, targets, and the
        /// target that has had some parked longest, named into `buf`.
        pub const Summary = struct { messages: u64 = 0, bytes: u64 = 0, targets: u64 = 0, oldest_since_ns: u64 = 0, oldest: []const u8 = "" };

        pub fn parkedSummary(read: ReadView, buf: []u8) Summary {
            const totals = shardParked(read);
            var s: Summary = .{ .messages = totals.count, .bytes = totals.bytes };
            if (totals.count == 0) return s;
            var mk: [PARK_KEY_MAX]u8 = undefined;
            var it = read.range(ParkFam, parkKey(&mk, "m"));
            var oldest_target: ?Target = null;
            while (it.next() catch null) |rec| {
                const raw = rec.key[1 + PARK_NS.len ..];
                if (raw.len != 1 + ce.TARGET_LEN) continue;
                s.targets += 1;
                if (oldest_target == null or rec.value.v.since_ns < s.oldest_since_ns) {
                    s.oldest_since_ns = rec.value.v.since_ns;
                    oldest_target = raw[1..][0..ce.TARGET_LEN].*;
                }
            }
            // Named from its first parked message, as its machine names it.
            if (oldest_target) |t| {
                var pb: [1 + ce.TARGET_LEN]u8 = undefined;
                pb[0] = 'p';
                pb[1..].* = t;
                var kb: [PARK_KEY_MAX]u8 = undefined;
                var pit = read.range(ParkFam, parkKey(&kb, &pb));
                if (pit.next() catch null) |first| {
                    if (std.enums.fromInt(EntryType, first.value.v.entry_type)) |et| if (machine_mod.ownerOf(targets, et)) |owner| if (owner.label) |label| {
                        const as = bare(first.value.v.entry_type, first.value.v.flags, first.value.v.payload);
                        s.oldest = label(&as, buf);
                    };
                }
            }
            return s;
        }

        /// An unpark to propose: its entry, the target it is for, and
        /// whether it took as many messages as one entry holds, so more of
        /// that target's may be waiting behind it.
        pub const NextUnpark = struct { entry: []const u8, target: Target, full: bool };

        /// The unpark this group's leader should propose next, encoded in
        /// `buf` (over a message list built in `msgs`), or null: the first
        /// target after `after`, in key order and round again, with parked
        /// messages whose first one has room now, with its parked messages
        /// in order, as many as one entry takes. Starting past the last one
        /// unparked, a target that always has room can't starve those after
        /// it. Apply decides how many of them fit.
        pub fn nextUnpark(read: ReadView, buf: []u8, msgs: []u8, after: ?Target) ?NextUnpark {
            var mk: [PARK_KEY_MAX]u8 = undefined;
            const prefix = parkKey(&mk, "m");
            var ab: [1 + ce.TARGET_LEN]u8 = undefined;
            var akb: [PARK_KEY_MAX]u8 = undefined;
            const after_key: ?[]const u8 = if (after) |t| parkKey(&akb, targetRaw(&ab, t)) else null;
            // Those after `after`, then from the first up to it.
            for (0..2) |pass| {
                if (pass == 1 and after_key == null) break;
                var targets_it = if (pass == 0 and after_key != null) read.rangeAfter(ParkFam, prefix, after_key.?) else read.range(ParkFam, prefix);
                while (targets_it.next() catch return null) |rec| {
                    if (pass == 1 and std.mem.order(u8, rec.key, after_key.?) == .gt) break;
                    const raw = rec.key[1 + PARK_NS.len ..];
                    if (raw.len != 1 + ce.TARGET_LEN) continue;
                    const target: Target = raw[1..][0..ce.TARGET_LEN].*;
                    if (unparkOf(read, target, buf, msgs)) |u| return u;
                }
            }
            return null;
        }

        /// `target`'s unpark, if its first parked message has room now.
        fn unparkOf(read: ReadView, target: Target, buf: []u8, msgs: []u8) ?NextUnpark {
            var pb: [1 + ce.TARGET_LEN]u8 = undefined;
            pb[0] = 'p';
            pb[1..].* = target;
            var kb: [PARK_KEY_MAX]u8 = undefined;
            var it = read.range(ParkFam, parkKey(&kb, &pb));
            var w: schema.Writer = .{ .buf = msgs };
            var n: u32 = 0;
            var bytes: usize = 0;
            var full = false;
            while (it.next() catch return null) |p| {
                const praw = p.key[1 + PARK_NS.len ..];
                const from = std.mem.readInt(u16, praw[1 + ce.TARGET_LEN ..][0..2], .big);
                const seq = std.mem.readInt(u64, praw[1 + ce.TARGET_LEN + 2 ..][0..8], .big);
                if (n == 0) {
                    const t = std.enums.fromInt(EntryType, p.value.v.entry_type) orelse return null;
                    const owner = machine_mod.ownerOf(targets, t) orelse return null;
                    const room = owner.room orelse return null;
                    const as = bare(p.value.v.entry_type, p.value.v.flags, p.value.v.payload);
                    if (!room(read, &as)) return null;
                }
                if (n == ce.MAX_BATCH or (n > 0 and bytes + p.value.v.payload.len > MAX_SEND_BYTES)) {
                    full = true;
                    break;
                }
                ce.Unparked.encode(&w, .{ .from = from, .seq = seq, .entry_type = p.value.v.entry_type, .flags = p.value.v.flags, .payload = p.value.v.payload }) catch {
                    full = true;
                    break;
                };
                n += 1;
                bytes += p.value.v.payload.len;
            }
            if (n == 0) return null;
            const entry = ce.Unpark.encodeInto(buf, .{ .target = &target, .messages = .{ .bytes = w.written(), .count = n } }) catch return null;
            return .{ .entry = entry, .target = target, .full = full };
        }

        /// Whether `raw` is the first parked key of its sender under its
        /// target: per-sender order.
        fn firstOfSender(view: *View, raw: []const u8) ApplyError!bool {
            var kb: [PARK_KEY_MAX]u8 = undefined;
            const prefix = parkKey(&kb, raw[0 .. 1 + ce.TARGET_LEN + 2]);
            var it = try view.range(ParkFam, prefix);
            const first = (try it.next()) orelse return false;
            var fb: [PARK_KEY_MAX]u8 = undefined;
            return std.mem.eql(u8, first.key, parkKey(&fb, raw));
        }

        fn applyTrim(view: *View, e: *const Entry) ApplyError!void {
            var diag: schema.Diagnostic = .{};
            const p = ce.Trim.decodeAll(e.payload, &diag) catch return error.Halt;
            const lane = Lane.from(p.lane) orelse return error.Halt;
            var mk: [CHAN_KEY]u8 = undefined;
            const meta_key = chanKey(&mk, p.to, lane);
            var meta: Meta.Value = if (try view.get(MetaFam, meta_key)) |m| m.v else .{ .next_seq = 1, .acked = 0, .bytes = 0 };
            // A trim an earlier leader proposed may be stale: a no-op.
            if (p.through <= meta.acked) return;
            if (p.through >= meta.next_seq or p.through < p.first or p.through - p.first >= ce.MAX_TRIM_SPAN) return error.Halt;
            // Everything below `first` must be gone already.
            if (p.first > meta.acked + 1) return error.Halt;
            var seq = meta.acked + 1;
            while (seq <= p.through) : (seq += 1) {
                var ok: [OUT_KEY]u8 = undefined;
                const key = outKey(&ok, p.to, lane, seq);
                const out = (try view.get(OutFam, key)) orelse return error.Halt;
                meta.bytes -= out.v.payload.len;
                var nk: [OUT_NS_KEY]u8 = undefined;
                const ns_key = outNsKey(&nk, p.to, lane, .of(out.v.origin_ns));
                const held = (try view.get(OutNsFam, ns_key)) orelse return error.Halt;
                if (held.v.bytes < out.v.payload.len) return error.Halt;
                if (held.v.bytes == out.v.payload.len) try view.delete(OutNsFam, ns_key) else try view.put(OutNsFam, ns_key, .{ .v = .{ .bytes = held.v.bytes - out.v.payload.len } });
                try view.delete(OutFam, key);
            }
            meta.acked = p.through;
            try view.put(MetaFam, meta_key, .{ .v = meta });
        }

        pub const machine: machine_mod.Machine = .{
            .name = "channels",
            .entry_types = &.{ .channel_inbound, .channel_trim, .channel_unpark },
            .families = &families,
            .keys = keys,
            .apply = apply,
        };
    };
}

/// `before`'s points with `(index, applied)` added; past `RING`, the oldest
/// point becomes the floor.
fn withPoint(arena: std.mem.Allocator, before: ?In.Value, applied: u64, index: u64) ApplyError!In.Value {
    var pts: std.ArrayListUnmanaged(Point.Value) = .empty;
    var floor_index: u64 = 0;
    var floor_seq: u64 = 0;
    if (before) |b| {
        floor_index = b.floor_index;
        floor_seq = b.floor_seq;
        var it = b.points.iterator();
        while (it.next() catch return error.Halt) |p| try pts.append(arena, p);
    }
    try pts.append(arena, .{ .index = index, .seq = applied });
    var drop: usize = 0;
    if (pts.items.len > RING) {
        drop = pts.items.len - RING;
        const last_dropped = pts.items[drop - 1];
        floor_index = last_dropped.index;
        floor_seq = last_dropped.seq;
    }
    const kept = pts.items[drop..];
    var w: schema.Writer = .{ .buf = try arena.alloc(u8, kept.len * Point.max_size) };
    for (kept) |p| Point.encode(&w, p) catch return error.Halt;
    return .{
        .applied = applied,
        .floor_index = floor_index,
        .floor_seq = floor_seq,
        .points = .{ .bytes = w.written(), .count = @intCast(kept.len) },
    };
}

// ─── Tests ─────────────────────────────────────────────────────────────────

const testing = std.testing;
const probe = @import("channel_probe.zig");

const Probed = Channels(&.{probe.machine});

const Group = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,
    buf: [4096]u8 = undefined,

    fn init(group: u16, count: u16) Group {
        var g: Group = .{ .store = store_mod.Store.init(testing.allocator), .scratch = std.heap.ArenaAllocator.init(testing.allocator) };
        g.store.place = .{ .group = group, .count = count };
        g.store.strict_keys = true;
        return g;
    }

    fn deinit(g: *Group) void {
        g.store.deinit();
        g.scratch.deinit();
    }

    fn entry(g: *Group, etype: entry_mod.EntryType, payload: []const u8) Entry {
        g.index += 1;
        var h = std.mem.zeroes(entry_mod.Header);
        h.entry_type = @backingInt(etype);
        h.index = g.index;
        h.payload_len = @intCast(payload.len);
        return .{ .header = h, .payload = payload };
    }

    fn run(g: *Group, m: *const machine_mod.Machine, etype: entry_mod.EntryType, payload: []const u8) ApplyError!Result {
        const e = g.entry(etype, payload);
        var r = Result.start(&e);
        try machine_mod.applyEntry(m, &g.store, &g.scratch, &e, &r);
        return r;
    }

    fn probeTo(g: *Group, slot: u16, body: []const u8) !void {
        const bytes = try ce.Probe.encodeInto(&g.buf, .{ .slot = slot, .body = body });
        const r = try g.run(&probe.machine, .channel_probe, bytes);
        try testing.expect(r.refused() == null);
    }

    fn read(g: *Group) ReadView {
        return .{ .store = &g.store };
    }

    fn outRecord(g: *Group, to: u16, seq: u64) ?Out.Value {
        return g.outOn(to, .sink, seq);
    }

    fn outOn(g: *Group, to: u16, lane: Lane, seq: u64) ?Out.Value {
        var k: [OUT_KEY]u8 = undefined;
        const v = g.read().get(OutFam, outKey(&k, to, lane, seq)) catch return null;
        return if (v) |x| x.v else null;
    }

    fn got(g: *Group, name: []const u8) ?Got {
        var kb: [1 + probe.NAME_LEN]u8 = undefined;
        const v = g.read().get(probe.GotFam, probe.gotKey(&kb, name)) catch return null;
        return if (v) |x| .{ .from = x.v.from, .seq = x.v.seq } else null;
    }
};

const Got = struct { from: u16, seq: u64 };

/// An inbound entry from `from`'s outbox to `to`, seqs `first..first+n-1`.
fn inboundFrom(sender: *Group, from: u16, to: u16, first: u64, n: u64, buf: []u8) ![]const u8 {
    return inboundOn(sender, from, to, .sink, first, n, buf);
}

/// `inboundFrom`, from `lane`'s outbox, on that lane.
fn inboundOn(sender: *Group, from: u16, to: u16, lane: Lane, first: u64, n: u64, buf: []u8) ![]const u8 {
    const S = struct {
        var messages: [8192]u8 = undefined;
    };
    var mw: schema.Writer = .{ .buf = &S.messages };
    var seq = first;
    while (seq < first + n) : (seq += 1) {
        const o = sender.outOn(to, lane, seq).?;
        try ce.Message.encode(&mw, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = o.layout_epoch, .flags = o.flags, .origin_kind = o.origin_kind, .origin_ns = o.origin_ns, .payload = o.payload });
    }
    return ce.Inbound.encodeInto(buf, .{ .from = from, .lane = @backingInt(lane), .first_seq = first, .messages = .{ .bytes = mw.written(), .count = @intCast(n) } }) catch unreachable;
}

test "channels: a history message rides its own lane, with its own seqs, outbox, mark and trims; the main lane's go on beside it" {
    // Group 0 of 2 owns the even slots: these go to itself.
    var a = Group.init(0, 2);
    defer a.deinit();
    try a.probeTo(2, "message-one-0001");
    try a.probeTo(2, "hist-one-000000H");
    try a.probeTo(2, "message-two-0002");
    try a.probeTo(2, "hist-two-000000H");
    try testing.expectEqual(@as(u64, 3), metaOf(a.read(), 0, .sink).next_seq);
    try testing.expectEqual(@as(u64, 3), metaOf(a.read(), 0, .history).next_seq);
    try testing.expectEqual(entry_mod.Flags.HISTORY, a.outOn(0, .history, 1).?.flags);
    try testing.expectEqual(entry_mod.Flags.SINK, a.outOn(0, .sink, 1).?.flags);
    // Their namespace's bytes are counted per lane.
    const one = a.outOn(0, .history, 1).?.payload.len;
    try testing.expectEqual(2 * one, nsBytesOf(a.read(), 0, .history, probe.PROBE_ORIGIN.ns));
    try testing.expectEqual(2 * one, metaOf(a.read(), 0, .history).bytes);
    // The history lane's second message applies with nothing from the main
    // lane applied: each lane keeps its own mark.
    var buf: [8192]u8 = undefined;
    _ = try a.run(&Probed.machine, .channel_inbound, try inboundOn(&a, 0, 0, .history, 1, 2, &buf));
    try testing.expectEqual(@as(u64, 2), inOf(a.read(), 0, .history).?.applied);
    try testing.expect(inOf(a.read(), 0, .sink) == null);
    try testing.expectEqual(@as(u64, 2), a.got("hist-two-000000H").?.seq);
    try testing.expect(a.got("message-one-0001") == null);
    _ = try a.run(&Probed.machine, .channel_inbound, try inboundOn(&a, 0, 0, .sink, 1, 1, &buf));
    try testing.expectEqual(@as(u64, 1), inOf(a.read(), 0, .sink).?.applied);
    // A trim of one lane leaves the other's outbox as it was.
    var tb: [64]u8 = undefined;
    _ = try a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&tb, .{ .to = 0, .lane = @backingInt(Lane.history), .first = 1, .through = 2 }));
    try testing.expectEqual(@as(u64, 2), metaOf(a.read(), 0, .history).acked);
    try testing.expectEqual(@as(u64, 0), metaOf(a.read(), 0, .history).bytes);
    try testing.expectEqual(@as(u64, 0), nsBytesOf(a.read(), 0, .history, probe.PROBE_ORIGIN.ns));
    try testing.expectEqual(@as(u64, 0), metaOf(a.read(), 0, .sink).acked);
    try testing.expect(a.outOn(0, .sink, 1) != null and a.outOn(0, .sink, 2) != null);
}

test "channels: a history message to another group, or a history message that is also a sink's, halts its sender's apply" {
    var a = Group.init(0, 2);
    defer a.deinit();
    // Slot 1 is group 1's.
    try testing.expectError(error.Halt, a.run(&probe.machine, .channel_probe, try ce.Probe.encodeInto(&a.buf, .{ .slot = 1, .body = "hist-other-0000H" })));
    try testing.expectEqual(@as(u64, 0), a.store.family(OutFam.ID).count);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var keys = KeySet.init(arena.allocator());
    var view = View.init(&a.store, arena.allocator(), &keys);
    try view.sendFlagged(probe.PROBE_ORIGIN, 2, @backingInt(EntryType.channel_probe), entry_mod.Flags.HISTORY | entry_mod.Flags.SINK, "x");
    try testing.expectError(error.Halt, writeSends(&view, 1));
}

test "channels: a history inbound from another group, or carrying a sink's message, halts and commits nothing" {
    var b = Group.init(1, 2);
    defer b.deinit();
    const before = replicaState(&b);
    var mb: [256]u8 = undefined;
    var buf: [512]u8 = undefined;
    var pb: [64]u8 = undefined;
    const probe_payload = try ce.Probe.encodeInto(&pb, .{ .slot = 1, .body = "hist-other-0000H" });
    for ([_]struct { from: u16, flags: u16 }{
        // From group 0, on group 1's history lane: history is a group's own.
        .{ .from = 0, .flags = entry_mod.Flags.HISTORY },
        // Its own, but a sink's too: nothing parks on the history lane.
        .{ .from = 1, .flags = entry_mod.Flags.HISTORY | entry_mod.Flags.SINK },
    }) |c| {
        var w: schema.Writer = .{ .buf = &mb };
        try ce.Message.encode(&w, .{ .slot = 1, .entry_type = @backingInt(EntryType.channel_probe), .layout_epoch = 0, .flags = c.flags, .origin_kind = @backingInt(probe.PROBE_ORIGIN.kind), .origin_ns = probe.PROBE_ORIGIN.ns.int(), .payload = probe_payload });
        const inbound = try ce.Inbound.encodeInto(&buf, .{ .from = c.from, .lane = @backingInt(Lane.history), .first_seq = 1, .messages = .{ .bytes = w.written(), .count = 1 } });
        try testing.expectError(error.Halt, b.run(&Probed.machine, .channel_inbound, inbound));
    }
    try testing.expectEqual(before, replicaState(&b));
}

test "channels: a repair's skip of a history inbound names its lane, and takes only that lane's seq" {
    var a = Group.init(0, 2);
    defer a.deinit();
    try a.probeTo(2, "hist-one-000000H");
    try a.probeTo(2, "message-one-0001");
    var buf: [8192]u8 = undefined;
    a.store.inbound_skip = .all(a.index + 1);
    _ = try a.run(&Probed.machine, .channel_inbound, try inboundOn(&a, 0, 0, .history, 1, 1, &buf));
    a.store.inbound_skip = null;
    try testing.expectEqual(@as(u64, 1), inOf(a.read(), 0, .history).?.applied);
    try testing.expect(inOf(a.read(), 0, .sink) == null);
    try testing.expect(a.got("hist-one-000000H") == null);
    const skipped = a.store.messages.items[0];
    try testing.expect(skipped.skipped);
    try testing.expect(std.mem.endsWith(u8, skipped.result.refused().?.refusal.message, "from shard 0, history lane)"));
    // The main lane's seq 1 is untouched by it.
    _ = try a.run(&Probed.machine, .channel_inbound, try inboundOn(&a, 0, 0, .sink, 1, 1, &buf));
    try testing.expectEqual(@as(u64, 1), a.got("message-one-0001").?.seq);
}

test "channels: an inbound whose message's flags are the other lane's, or naming a lane there isn't, halts and commits nothing" {
    var a = Group.init(0, 2);
    defer a.deinit();
    try a.probeTo(2, "message-one-0001");
    try a.probeTo(2, "hist-one-000000H");
    var buf: [8192]u8 = undefined;
    const before = replicaState(&a);
    // The main lane's message, said to be on the history lane.
    const main_on_history = @constCast(try inboundOn(&a, 0, 0, .sink, 1, 1, &buf));
    main_on_history[2] = @backingInt(Lane.history);
    try testing.expectError(error.Halt, a.run(&Probed.machine, .channel_inbound, main_on_history));
    // And the other way.
    const history_on_main = @constCast(try inboundOn(&a, 0, 0, .history, 1, 1, &buf));
    history_on_main[2] = @backingInt(Lane.sink);
    try testing.expectError(error.Halt, a.run(&Probed.machine, .channel_inbound, history_on_main));
    const no_lane = @constCast(try inboundOn(&a, 0, 0, .sink, 1, 1, &buf));
    no_lane[2] = 7;
    try testing.expectError(error.Halt, a.run(&Probed.machine, .channel_inbound, no_lane));
    try testing.expectEqual(before, replicaState(&a));
}

test "channels: a send is written to the outbox with its entry, in seq order per receiver" {
    var a = Group.init(0, 2);
    defer a.deinit();
    // Slots 1 and 3 are group 1's; 2 is this group's own.
    try a.probeTo(1, "message-one-0001");
    try a.probeTo(3, "message-two-0002");
    try a.probeTo(2, "message-own-0003");
    try testing.expectEqual(@as(u64, 3), metaOf(a.read(), 1, .sink).next_seq);
    try testing.expectEqual(@as(u64, 2), metaOf(a.read(), 0, .sink).next_seq);
    const first = a.outRecord(1, 1).?;
    try testing.expectEqual(@as(u64, 1), first.sent_at);
    try testing.expectEqual(@as(u16, 1), first.slot);
    try testing.expectEqual(@as(u64, 2), a.outRecord(1, 2).?.sent_at);
    // A send to this group's own slot goes through its own channel.
    try testing.expectEqual(@as(u64, 3), a.outRecord(0, 1).?.sent_at);
}

test "channels: a receiver applies each message once and in order, and halts on a gap" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "message-one-0001");
    try a.probeTo(1, "message-two-0002");
    try a.probeTo(1, "message-thr-0003");
    var buf: [8192]u8 = undefined;
    // 1..2, then 1..3 again (a resend): 3 is new, 1 and 2 are skipped.
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 2, &buf));
    try testing.expectEqual(@as(u64, 2), inOf(b.read(), 0, .sink).?.applied);
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 3, &buf));
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0, .sink).?.applied);
    try testing.expectEqual(Got{ .from = 0, .seq = 1 }, b.got("message-one-0001").?);
    try testing.expectEqual(Got{ .from = 0, .seq = 3 }, b.got("message-thr-0003").?);
    // An all-duplicate batch changes nothing.
    const index_before = inOf(b.read(), 0, .sink).?;
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 2, 2, &buf));
    try testing.expectEqual(index_before.applied, inOf(b.read(), 0, .sink).?.applied);
    // A batch past a gap halts, and changes nothing.
    try a.probeTo(1, "message-fou-0004");
    try a.probeTo(1, "message-fiv-0005");
    try testing.expectError(error.Halt, b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 5, 1, &buf)));
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0, .sink).?.applied);
    try testing.expect(b.got("message-fiv-0005") == null);
}

test "channels: a refused message undoes only its own writes, and still takes its seq" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "message-one-0001");
    try a.probeTo(1, "refuse-this-0002");
    try a.probeTo(1, "message-thr-0003");
    var buf: [8192]u8 = undefined;
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 3, &buf));
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0, .sink).?.applied);
    try testing.expect(b.got("message-one-0001") != null);
    try testing.expect(b.got("refuse-this-0002") == null);
    // The target's own refusal: not the sender's bug.
    try testing.expectEqual(@as(?store_mod.SenderBug, null), b.store.messages.items[1].sender_bug);
    try testing.expect(b.got("message-thr-0003") != null);
}

test "channels: a repair's skip of an inbound takes every seq it holds, answers each skipped by repair, and a resend is a duplicate" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "message-one-0001");
    try a.probeTo(1, "halt-now----0002");
    try a.probeTo(1, "message-thr-0003");
    var buf: [8192]u8 = undefined;
    // Unrepaired, the inbound halts at its second message, naming it.
    try testing.expectError(error.Halt, b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 3, &buf)));
    try testing.expectEqual(store_mod.Applying{ .from = 0, .seq = 2, .lane = .sink }, b.store.applying.?);
    try testing.expect(inOf(b.read(), 0, .sink) == null);
    // Skipped whole: no message applies, every seq is taken and answered.
    b.store.inbound_skip = .all(77);
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 3, &buf));
    b.store.inbound_skip = null;
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0, .sink).?.applied);
    try testing.expect(b.got("message-one-0001") == null and b.got("message-thr-0003") == null);
    try testing.expectEqual(@as(usize, 3), b.store.messages.items.len);
    for (b.store.messages.items) |*m| {
        try testing.expect(m.skipped);
        const r = m.result.refused().?;
        try testing.expectEqual(@import("../protocol/proto.zig").Reason.skipped_by_repair, r.refusal.reason);
        try testing.expect(std.mem.indexOf(u8, r.refusal.message, "skipped by repair (entry 77") != null);
    }
    // Sent again, they are duplicates: nothing more applies.
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 3, &buf));
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0, .sink).?.applied);
    try testing.expect(b.got("message-one-0001") == null);
}

/// Every channel and probe family's canonical checksum, folded: equal on
/// two replicas exactly when they hold the same state.
fn replicaState(g: *Group) u64 {
    var h = std.hash.Wyhash.init(0);
    for (families) |f| h.update(std.mem.asBytes(&store_mod.checksum(&g.store, f.id)));
    for (probe.machine.families) |f| h.update(std.mem.asBytes(&store_mod.checksum(&g.store, f.id)));
    return h.final();
}

test "channels: a skipped inbound leaves every replica the same, whether it halted first or only applies the skip" {
    var a = Group.init(0, 2);
    defer a.deinit();
    try a.probeTo(1, "message-one-0001");
    try a.probeTo(1, "halt-now----0002");
    try a.probeTo(1, "message-thr-0003");
    var buf: [8192]u8 = undefined;
    const inbound = try inboundFrom(&a, 0, 1, 1, 3, &buf);
    // Three replicas of the receiving group.
    var live = Group.init(1, 2);
    defer live.deinit();
    var halted_first = Group.init(1, 2);
    defer halted_first.deinit();
    var booted = Group.init(1, 2);
    defer booted.deinit();
    // One halted on it live before the repair committed; the others take the
    // skip as they reach it (live, or replaying it at boot).
    try testing.expectError(error.Halt, halted_first.run(&Probed.machine, .channel_inbound, inbound));
    // The same entry, at the same index, comes round again.
    halted_first.index -= 1;
    for ([_]*Group{ &live, &halted_first, &booted }) |g| {
        g.store.inbound_skip = .one(77, 2);
        _ = try g.run(&Probed.machine, .channel_inbound, inbound);
        g.store.inbound_skip = null;
    }
    const want = replicaState(&live);
    try testing.expectEqual(want, replicaState(&halted_first));
    try testing.expectEqual(want, replicaState(&booted));
    for ([_]*Group{ &halted_first, &booted }) |g| {
        try testing.expectEqual(inOf(live.read(), 0, .sink).?.applied, inOf(g.read(), 0, .sink).?.applied);
        try testing.expectEqual(live.store.messages.items.len, g.store.messages.items.len);
        for (live.store.messages.items, g.store.messages.items) |*x, *y| {
            try testing.expectEqual(x.skipped, y.skipped);
            try testing.expectEqual(x.seq, y.seq);
        }
    }
}

test "channels: a repair's skip of one message of an inbound applies the others" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "message-one-0001");
    try a.probeTo(1, "halt-now----0002");
    try a.probeTo(1, "message-thr-0003");
    var buf: [8192]u8 = undefined;
    b.store.inbound_skip = .one(77, 2);
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 3, &buf));
    b.store.inbound_skip = null;
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0, .sink).?.applied);
    try testing.expect(b.got("message-one-0001") != null);
    try testing.expect(b.got("message-thr-0003") != null);
    try testing.expect(!b.store.messages.items[0].skipped);
    try testing.expect(b.store.messages.items[1].skipped);
    try testing.expect(!b.store.messages.items[2].skipped);
    try testing.expectEqual(@as(?store_mod.Applying, null), b.store.applying);
}

test "channels: an inbound whose messages halt it in turn is skipped a message at a time, then whole, and a whole skip's answers name the halting message" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "message-one-0001");
    try a.probeTo(1, "halt-now----0002");
    try a.probeTo(1, "halt-now----0003");
    try a.probeTo(1, "halt-now----0004");
    try a.probeTo(1, "message-fiv-0005");
    var buf: [8192]u8 = undefined;
    const inbound = try inboundFrom(&a, 0, 1, 1, 5, &buf);
    // Message 2 skipped: the inbound halts again, at 3, applying nothing.
    var skip = store_mod.InboundSkip.one(77, 2);
    b.store.inbound_skip = skip;
    try testing.expectError(error.Halt, b.run(&Probed.machine, .channel_inbound, inbound));
    try testing.expectEqual(store_mod.Applying{ .from = 0, .seq = 3, .lane = .sink }, b.store.applying.?);
    try testing.expect(inOf(b.read(), 0, .sink) == null);
    // 2 and 3 skipped: it halts at 4.
    b.index -= 1;
    skip.seqs[1] = 3;
    skip.n = 2;
    b.store.inbound_skip = skip;
    try testing.expectError(error.Halt, b.run(&Probed.machine, .channel_inbound, inbound));
    try testing.expectEqual(store_mod.Applying{ .from = 0, .seq = 4, .lane = .sink }, b.store.applying.?);
    // Then the whole entry, the group halted at 4: every message is skipped,
    // and one that didn't halt it says which did.
    b.index -= 1;
    skip.whole = true;
    skip.halt_seq = 4;
    b.store.inbound_skip = skip;
    _ = try b.run(&Probed.machine, .channel_inbound, inbound);
    b.store.inbound_skip = null;
    try testing.expectEqual(@as(u64, 5), inOf(b.read(), 0, .sink).?.applied);
    try testing.expect(b.got("message-one-0001") == null and b.got("message-fiv-0005") == null);
    try testing.expectEqual(@as(usize, 5), b.store.messages.items.len);
    try testing.expectEqualStrings("skipped by repair (entry 77, halted at message 4; message 1 from shard 0)", b.store.messages.items[0].result.refused().?.refusal.message);
    try testing.expectEqualStrings("skipped by repair (entry 77, message 4 from shard 0)", b.store.messages.items[3].result.refused().?.refusal.message);
}

test "channels: a message's sends are held to an apply's limits on their own: past them, only it is refused, and the channel goes on" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "relay-this-00001");
    try a.probeTo(1, "flood-this-00002");
    try a.probeTo(1, "message-thr-0003");
    var buf: [8192]u8 = undefined;
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 3, &buf));
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0, .sink).?.applied);
    try testing.expect(b.got("relay-this-00001") != null);
    try testing.expect(b.got("flood-this-00002") == null);
    try testing.expect(b.got("message-thr-0003") != null);
    // The relay's one send is in b's outbox, under the inbound's index;
    // the flood sent nothing.
    try testing.expectEqual(@as(u64, 2), metaOf(b.read(), 1, .sink).next_seq);
    try testing.expectEqual(b.index, b.outRecord(1, 1).?.sent_at);
    // Each message was reported with its result, the flood's refused.
    const reported = b.store.messages.items;
    try testing.expectEqual(@as(usize, 3), reported.len);
    try testing.expect(reported[0].result.refused() == null);
    try testing.expect(reported[1].result.refused() != null);
    try testing.expect(reported[2].result.refused() == null);
    // The next inbound follows on, with no halt.
    try a.probeTo(1, "message-fou-0004");
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 4, 1, &buf));
    try testing.expectEqual(@as(u64, 4), inOf(b.read(), 0, .sink).?.applied);
}

test "channels: a message for a slot its receiver doesn't own is refused, and takes its seq" {
    var a = Group.init(0, 2);
    defer a.deinit();
    // b is group 1 of 3: slot 1 is its own, slot 3 is group 0's.
    var b = Group.init(1, 3);
    defer b.deinit();
    try a.probeTo(1, "message-one-0001");
    try a.probeTo(3, "message-two-0002");
    var buf: [8192]u8 = undefined;
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 2, &buf));
    try testing.expectEqual(@as(u64, 2), inOf(b.read(), 0, .sink).?.applied);
    try testing.expect(b.got("message-one-0001") != null);
    try testing.expect(b.got("message-two-0002") == null);
    try testing.expect(b.store.messages.items[1].result.refused() != null);
    try testing.expectEqual(@as(?store_mod.SenderBug, .wrong_slot), b.store.messages.items[1].sender_bug);
}

test "channels: a message applies with the flags its send gave it, not its inbound's" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    const sink = entry_mod.Flags.SINK;
    {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        var keys = KeySet.init(arena.allocator());
        var view = View.init(&a.store, arena.allocator(), &keys);
        var pb: [64]u8 = undefined;
        try view.sendFlagged(probe.PROBE_ORIGIN, 1, @backingInt(EntryType.channel_probe), sink, try ce.Probe.encodeInto(&pb, .{ .slot = 1, .body = "message-snk-0001" }));
        try writeSends(&view, 1);
        try view.commit();
    }
    try testing.expectEqual(sink, a.outRecord(1, 1).?.flags);
    var buf: [8192]u8 = undefined;
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 1, &buf));
    try testing.expect(b.got("message-snk-0001") != null);
    try testing.expectEqual(sink, b.store.messages.items[0].entry.header.flags);
}

test "channels: a message carrying a flag no message may carry halts its inbound, which commits nothing" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "message-one-0001");
    const o = a.outRecord(1, 1).?;
    var mw: schema.Writer = .{ .buf = try testing.allocator.alloc(u8, 1024) };
    defer testing.allocator.free(mw.buf);
    try ce.Message.encode(&mw, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = 0, .flags = entry_mod.Flags.SINK, .origin_kind = o.origin_kind, .origin_ns = o.origin_ns, .payload = o.payload });
    try ce.Message.encode(&mw, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = 0, .flags = entry_mod.Flags.FORCE, .origin_kind = o.origin_kind, .origin_ns = o.origin_ns, .payload = o.payload });
    var buf: [2048]u8 = undefined;
    const inbound = try ce.Inbound.encodeInto(&buf, .{ .from = 0, .lane = 2, .first_seq = 1, .messages = .{ .bytes = mw.written(), .count = 2 } });
    try testing.expectError(error.Halt, b.run(&Probed.machine, .channel_inbound, inbound));
    try testing.expect(b.got("message-one-0001") == null);
    try testing.expect(inOf(b.read(), 0, .sink) == null);
}

/// The inbound of `a`'s outbox to `b` from `first` through `last`, applied
/// by `b`.
fn deliver(a: *Group, b: *Group, first: u64, last: u64) !void {
    var buf: [8192]u8 = undefined;
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(a, 0, 1, first, last - first + 1, &buf));
}

/// `b`'s next unpark, applied; false if it has none.
fn unparkNext(b: *Group) !bool {
    var buf: [8192]u8 = undefined;
    var msgs: [8192]u8 = undefined;
    const next = Probed.nextUnpark(b.read(), &buf, &msgs, null) orelse return false;
    _ = try b.run(&Probed.machine, .channel_unpark, next.entry);
    return true;
}

fn probeTarget(slot: u16) Target {
    var t: Target = @splat(0);
    t[0] = @backingInt(probe.machine.entry_types[0]);
    t[1..6].* = "probe".*;
    std.mem.writeInt(u16, t[6..8], slot, .big);
    return t;
}

test "channels: a sink's message to a full target parks, later ones for it park behind, and other targets go on" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "fill-slot1-00001");
    try a.probeTo(1, "message-two-0002");
    try a.probeTo(3, "message-thr-0003");
    try a.probeTo(1, "message-fou-0004");
    try deliver(&a, &b, 1, 4);
    // Every message took its seq: the channel goes on.
    try testing.expectEqual(@as(u64, 4), inOf(b.read(), 0, .sink).?.applied);
    try testing.expect(b.got("message-two-0002") == null);
    try testing.expect(b.got("message-fou-0004") == null);
    // Slot 3 is another target: applied at once.
    try testing.expectEqual(Got{ .from = 0, .seq = 3 }, b.got("message-thr-0003").?);
    try testing.expectEqual(@as(u64, 2), targetParked(b.read(), probeTarget(1)).count);
    try testing.expectEqual(@as(u64, 2), shardParked(b.read()).count);
    // Still full: nothing to unpark.
    try testing.expect(!try unparkNext(&b));
    // Drained: one unpark applies both, in order, and parking empties.
    try a.probeTo(1, "drain-slot1-0005");
    try deliver(&a, &b, 5, 5);
    try testing.expect(try unparkNext(&b));
    try testing.expectEqual(Got{ .from = 0, .seq = 2 }, b.got("message-two-0002").?);
    try testing.expectEqual(Got{ .from = 0, .seq = 4 }, b.got("message-fou-0004").?);
    try testing.expectEqual(@as(u64, 0), targetParked(b.read(), probeTarget(1)).count);
    try testing.expectEqual(@as(u64, 0), shardParked(b.read()).count);
    try testing.expect(!try unparkNext(&b));
}

test "channels: a message parked behind others parks even with room, and one its target refuses for good leaves parking refused" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "fill-slot1-00001");
    try a.probeTo(1, "message-two-0002");
    try a.probeTo(1, "drain-slot1-0003");
    // Room again, but a message is parked: this one parks behind it.
    try a.probeTo(1, "refuse-this-0004");
    try a.probeTo(1, "message-fiv-0005");
    try deliver(&a, &b, 1, 5);
    try testing.expectEqual(@as(u64, 3), targetParked(b.read(), probeTarget(1)).count);
    try testing.expect(try unparkNext(&b));
    try testing.expectEqual(@as(u64, 0), targetParked(b.read(), probeTarget(1)).count);
    try testing.expect(b.got("message-two-0002") != null);
    try testing.expect(b.got("refuse-this-0004") == null);
    try testing.expect(b.got("message-fiv-0005") != null);
    // Reported in order, the refused one with its refusal.
    const reported = b.store.messages.items;
    try testing.expectEqual(@as(usize, 3), reported.len);
    try testing.expect(reported[0].result.refused() == null);
    try testing.expect(reported[1].result.refused() != null);
    try testing.expect(reported[2].result.refused() == null);
}

test "channels: an unpark applies only each sender's next parked message, and stops at a full target" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "fill-slot1-00001");
    try a.probeTo(1, "message-two-0002");
    try a.probeTo(1, "message-thr-0003");
    try deliver(&a, &b, 1, 3);
    var mb: [1024]u8 = undefined;
    var buf: [2048]u8 = undefined;
    const t = probeTarget(1);
    // Out of order: seq 3 before 2. Stale at its first message: nothing applies.
    {
        var w: schema.Writer = .{ .buf = &mb };
        for ([_]u64{ 3, 2 }) |seq| {
            const o = a.outRecord(1, seq).?;
            try ce.Unparked.encode(&w, .{ .from = 0, .seq = seq, .entry_type = o.entry_type, .flags = o.flags, .payload = o.payload });
        }
        _ = try b.run(&Probed.machine, .channel_unpark, try ce.Unpark.encodeInto(&buf, .{ .target = &t, .messages = .{ .bytes = w.written(), .count = 2 } }));
        try testing.expectEqual(@as(u64, 2), targetParked(b.read(), t).count);
    }
    // In order, but the target is still full: the first doesn't fit, and
    // nothing moves.
    {
        var w: schema.Writer = .{ .buf = &mb };
        const o = a.outRecord(1, 2).?;
        try ce.Unparked.encode(&w, .{ .from = 0, .seq = 2, .entry_type = o.entry_type, .flags = o.flags, .payload = o.payload });
        _ = try b.run(&Probed.machine, .channel_unpark, try ce.Unpark.encodeInto(&buf, .{ .target = &t, .messages = .{ .bytes = w.written(), .count = 1 } }));
        try testing.expectEqual(@as(u64, 2), targetParked(b.read(), t).count);
        try testing.expect(b.got("message-two-0002") == null);
    }
}

test "channels: past a target's parked cap, only that target's messages wait at their sender" {
    test_parked_target_max = 16;
    defer test_parked_target_max = null;
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "fill-slot1-00001");
    try a.probeTo(1, "message-two-0002");
    try deliver(&a, &b, 1, 2);
    try testing.expect(targetParked(b.read(), probeTarget(1)).bytes >= 16);
    const full = a.outRecord(1, 2).?;
    try testing.expect(!Probed.admits(b.read(), .{ .slot = full.slot, .entry_type = full.entry_type, .layout_epoch = 0, .flags = full.flags, .origin_kind = full.origin_kind, .origin_ns = full.origin_ns, .payload = full.payload }, .{}));
    // Another target on the same channel is taken.
    try a.probeTo(3, "message-thr-0003");
    const other = a.outRecord(1, 3).?;
    try testing.expect(Probed.admits(b.read(), .{ .slot = other.slot, .entry_type = other.entry_type, .layout_epoch = 0, .flags = other.flags, .origin_kind = other.origin_kind, .origin_ns = other.origin_ns, .payload = other.payload }, .{}));
}

test "channels: past the shard's parked cap, a sink's message to any target waits at its sender, even one with nothing parked" {
    test_parked_shard_max = 16;
    defer test_parked_shard_max = null;
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "fill-slot1-00001");
    try a.probeTo(1, "message-two-0002");
    try deliver(&a, &b, 1, 2);
    try testing.expect(shardParked(b.read()).bytes >= 16);
    try a.probeTo(3, "message-thr-0003");
    const other = a.outRecord(1, 3).?;
    try testing.expectEqual(@as(u64, 0), targetParked(b.read(), probeTarget(3)).count);
    try testing.expect(!Probed.admits(b.read(), .{ .slot = other.slot, .entry_type = other.entry_type, .layout_epoch = 0, .flags = other.flags, .origin_kind = other.origin_kind, .origin_ns = other.origin_ns, .payload = other.payload }, .{}));
    // Not a sink's: it never parks, so it is taken.
    try testing.expect(Probed.admits(b.read(), .{ .slot = other.slot, .entry_type = other.entry_type, .layout_epoch = 0, .flags = 0, .origin_kind = other.origin_kind, .origin_ns = other.origin_ns, .payload = other.payload }, .{}));
}

test "channels: only a sink's message parks behind; another for the same target, on the main lane, goes to it" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "fill-slot1-00001");
    try a.probeTo(1, "message-two-0002");
    try a.probeTo(1, "drain-slot1-0003");
    var buf: [4096]u8 = undefined;
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 3, &buf));
    try testing.expectEqual(@as(u64, 1), targetParked(b.read(), probeTarget(1)).count);
    // A message to the same target that isn't a sink's rides the main lane,
    // and applies with the sink's parked.
    try a.probeTo(1, "message-fou-000M");
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundOn(&a, 0, 1, .main, 1, 1, &buf));
    try testing.expectEqual(@as(u64, 1), targetParked(b.read(), probeTarget(1)).count);
    try testing.expectEqual(Got{ .from = 0, .seq = 1 }, b.got("message-fou-000M").?);
    try testing.expect(b.got("message-two-0002") == null);
}

test "channels: unparks take turns among targets with room, each starting past the last one's target" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "fill-slot1-00001");
    try a.probeTo(3, "fill-slot3-00002");
    try a.probeTo(1, "message-one-0003");
    try a.probeTo(3, "message-thr-0004");
    try a.probeTo(1, "drain-slot1-0005");
    try a.probeTo(3, "drain-slot3-0006");
    try deliver(&a, &b, 1, 6);
    try testing.expectEqual(@as(u64, 1), targetParked(b.read(), probeTarget(1)).count);
    try testing.expectEqual(@as(u64, 1), targetParked(b.read(), probeTarget(3)).count);
    var buf: [8192]u8 = undefined;
    var msgs: [8192]u8 = undefined;
    // From the start, the first in key order; past it, the next; past the
    // last, round to the first again.
    try testing.expectEqualSlices(u8, &probeTarget(1), &Probed.nextUnpark(b.read(), &buf, &msgs, null).?.target);
    try testing.expectEqualSlices(u8, &probeTarget(3), &Probed.nextUnpark(b.read(), &buf, &msgs, probeTarget(1)).?.target);
    try testing.expectEqualSlices(u8, &probeTarget(1), &Probed.nextUnpark(b.read(), &buf, &msgs, probeTarget(3)).?.target);
    // Taking turns, both drain.
    var after: ?Target = probeTarget(1);
    while (Probed.nextUnpark(b.read(), &buf, &msgs, after)) |next| {
        after = next.target;
        try testing.expect(!next.full);
        _ = try b.run(&Probed.machine, .channel_unpark, next.entry);
    }
    try testing.expectEqual(@as(u64, 0), shardParked(b.read()).count);
    try testing.expect(b.got("message-one-0003") != null and b.got("message-thr-0004") != null);
}

test "channels: a durable ack names what was applied as of a durable index" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    var buf: [8192]u8 = undefined;
    var seq: u64 = 1;
    // One message per inbound, RING + 6 of them: the oldest points fall to the floor.
    while (seq <= RING + 6) : (seq += 1) {
        var name: [16]u8 = undefined;
        _ = std.fmt.bufPrint(&name, "message-{d:0>8}", .{seq}) catch unreachable;
        try a.probeTo(1, &name);
        _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, seq, 1, &buf));
    }
    const in = inOf(b.read(), 0, .sink).?;
    try testing.expectEqual(@as(u64, RING + 6), in.applied);
    // b's index n applied seq n.
    try testing.expectEqual(@as(u64, RING + 6), appliedAsOf(in, RING + 6));
    try testing.expectEqual(@as(u64, 40), appliedAsOf(in, 40));
    // Below the ring, the floor: lower than the truth, never higher.
    try testing.expectEqual(@as(u64, 6), appliedAsOf(in, 6));
    try testing.expectEqual(@as(u64, 0), appliedAsOf(in, 3));
}

test "channels: a trim drops what was acked; a stale one is a no-op, and one past what was sent halts" {
    var a = Group.init(0, 2);
    defer a.deinit();
    try a.probeTo(1, "message-one-0001");
    try a.probeTo(1, "message-two-0002");
    try a.probeTo(1, "message-thr-0003");
    var buf: [64]u8 = undefined;
    _ = try a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&buf, .{ .to = 1, .lane = 2, .first = 1, .through = 2 }));
    try testing.expectEqual(@as(u64, 2), metaOf(a.read(), 1, .sink).acked);
    try testing.expect(a.outRecord(1, 1) == null and a.outRecord(1, 2) == null);
    try testing.expect(a.outRecord(1, 3) != null);
    _ = try a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&buf, .{ .to = 1, .lane = 2, .first = 1, .through = 1 }));
    try testing.expectEqual(@as(u64, 2), metaOf(a.read(), 1, .sink).acked);
    try testing.expectError(error.Halt, a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&buf, .{ .to = 1, .lane = 2, .first = 3, .through = 4 })));
}

test "channels: an outbox counts the payload bytes it holds, from its sends to its trims" {
    var a = Group.init(0, 2);
    defer a.deinit();
    try a.probeTo(1, "message-one-0001");
    const one = a.outRecord(1, 1).?.payload.len;
    try a.probeTo(1, "message-two-0002");
    try a.probeTo(1, "message-thr-0003");
    try testing.expectEqual(3 * one, metaOf(a.read(), 1, .sink).bytes);
    // And its origin's namespace holds all of it.
    try testing.expectEqual(3 * one, nsBytesOf(a.read(), 1, .sink, probe.PROBE_ORIGIN.ns));
    var buf: [64]u8 = undefined;
    _ = try a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&buf, .{ .to = 1, .lane = 2, .first = 1, .through = 2 }));
    try testing.expectEqual(one, metaOf(a.read(), 1, .sink).bytes);
    try testing.expectEqual(one, nsBytesOf(a.read(), 1, .sink, probe.PROBE_ORIGIN.ns));
    _ = try a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&buf, .{ .to = 1, .lane = 2, .first = 3, .through = 3 }));
    try testing.expectEqual(@as(u64, 0), metaOf(a.read(), 1, .sink).bytes);
    // Emptied, its record goes.
    try testing.expectEqual(@as(usize, 0), a.store.family(OutNsFam.ID).count);
}

test "channels: an apply that sends too much is refused, and sends nothing" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var sends: [MAX_SENDS + 1]view_mod.Send = undefined;
    for (&sends) |*s| s.* = .{ .origin = .user(.default), .slot = 1, .entry_type = 0, .payload = "x" };
    try testing.expect(tooMuchSent(sends[0..MAX_SENDS]) == null);
    try testing.expect(tooMuchSent(&sends) != null);
}

test "channels: a key is inside its origin's namespace by its family's scope" {
    const seven: ns_table.Id = .of(7);
    const eight: ns_table.Id = .of(8);
    var kb: [32]u8 = undefined;
    // Led by the id.
    try testing.expect(keyInside(.id, .user, seven, try view_mod.idKey(&kb, seven, "k")));
    try testing.expect(!keyInside(.id, .user, seven, try view_mod.idKey(&kb, eight, "k")));
    // Naming it after an expiry: a deadline's key.
    var db: [32]u8 = undefined;
    std.mem.writeInt(u64, db[0..8], 12345, .big);
    db[8..12].* = seven.bytes();
    const deadline = try view_mod.key(&kb, "", db[0..12]);
    try testing.expect(keyInside(.{ .naming = 8 }, .user, seven, deadline));
    try testing.expect(!keyInside(.{ .naming = 8 }, .user, eight, deadline));
    // A key too short to name one is outside.
    try testing.expect(!keyInside(.{ .naming = 8 }, .user, seven, deadline[0..10]));
    // Totals over every namespace: any; the shard's own: the system only.
    try testing.expect(keyInside(.aggregate, .user, eight, "anything"));
    try testing.expect(!keyInside(.shard, .user, seven, "x"));
    try testing.expect(keyInside(.shard, .system, seven, "x"));
}

test "channels: a message naming one namespace that reaches another's keys, or a user's to the system's, is refused as its sender's bug" {
    const kv_machine = @import("../kv/machine.zig").machine;
    const kv_entries = @import("../protocol/kv_entries.zig");
    const Kv = Channels(&.{kv_machine});
    var mb: [4096]u8 = undefined;
    var w: schema.Writer = .{ .buf = &mb };
    // From namespace 7: a put in 7, one in 8, and one to the system's.
    const puts = [_]struct { key_ns: ns_table.Id, origin: ns_table.Id }{
        .{ .key_ns = .of(7), .origin = .of(7) },
        .{ .key_ns = .of(8), .origin = .of(7) },
        .{ .key_ns = ns_table.SYSTEM, .origin = ns_table.SYSTEM },
    };
    var pbs: [3][128]u8 = undefined;
    for (puts, &pbs) |p, *pb| {
        var kb: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&kb, "{s}k", .{&p.key_ns.bytes()});
        const put = try kv_entries.Put.encodeInto(pb, .{ .key = key, .value = "v", .ttl_ms = null, .condition = .always, .cas_version = null });
        try ce.Message.encode(&w, .{ .slot = 0, .entry_type = @backingInt(EntryType.kv_put), .layout_epoch = 0, .flags = 0, .origin_kind = @backingInt(view_mod.Origin.Kind.user), .origin_ns = p.origin.int(), .payload = put });
    }
    var ib: [8192]u8 = undefined;
    const inbound = try ce.Inbound.encodeInto(&ib, .{ .from = 0, .lane = 0, .first_seq = 1, .messages = .{ .bytes = w.written(), .count = puts.len } });
    var h = std.mem.zeroes(entry_mod.Header);
    h.entry_type = @backingInt(EntryType.channel_inbound);
    h.index = 9;
    h.payload_len = @intCast(inbound.len);
    const e: Entry = .{ .header = h, .payload = inbound };
    var st = store_mod.Store.init(testing.allocator);
    defer st.deinit();
    st.place = .{ .group = 0, .count = 1 };
    var scratch = std.heap.ArenaAllocator.init(testing.allocator);
    defer scratch.deinit();
    var out = Result.start(&e);
    try machine_mod.applyEntry(&Kv.machine, &st, &scratch, &e, &out);
    const reported = st.messages.items;
    try testing.expectEqual(@as(usize, 3), reported.len);
    try testing.expectEqual(@as(?store_mod.SenderBug, null), reported[0].sender_bug);
    try testing.expectEqual(@as(?store_mod.SenderBug, .wrong_origin), reported[1].sender_bug);
    try testing.expectEqualStrings("bad request: a message reaching keys outside its origin's namespace", reported[1].result.refused().?.refusal.message);
    try testing.expectEqual(@as(?store_mod.SenderBug, .wrong_origin), reported[2].sender_bug);
    try testing.expectEqual(@import("../protocol/proto.zig").StatusCode.unauthorized, reported[2].result.refused().?.status);
    // Only the first was applied.
    try testing.expectEqual(@as(usize, 1), st.family(@import("../kv/machine.zig").Values.ID).count);
}

/// A test machine sending, for `kv_put` payload `[ns u32][slot u16][body]`,
/// one probe to `slot` as namespace `ns`'s user.
fn tenantKeys(_: *const Entry, _: *KeySet) error{OutOfMemory}!void {}
fn tenantApply(view: *View, e: *const Entry, _: *Result) ApplyError!void {
    const ns: ns_table.Id = .of(std.mem.readInt(u32, e.payload[0..4], .big));
    const slot = std.mem.readInt(u16, e.payload[4..6], .big);
    try view.send(.user(ns), slot, @backingInt(EntryType.channel_probe), e.payload[6..]);
}
const tenant_machine: machine_mod.Machine = .{ .name = "tenant", .entry_types = &.{.kv_put}, .families = &.{}, .keys = tenantKeys, .apply = tenantApply };

fn tenantSend(g: *Group, ns: ns_table.Id, slot: u16, body: []const u8) !void {
    var pb: [64]u8 = undefined;
    std.mem.writeInt(u32, pb[0..4], ns.int(), .big);
    std.mem.writeInt(u16, pb[4..6], slot, .big);
    @memcpy(pb[6..][0..body.len], body);
    const r = try g.run(&tenant_machine, .kv_put, pb[0 .. 6 + body.len]);
    try testing.expect(r.refused() == null);
}

test "channels: two namespaces' bytes in one outbox are counted apart, and each one's trimmed alone" {
    var a = Group.init(0, 2);
    defer a.deinit();
    const seven: ns_table.Id = .of(17);
    const eight: ns_table.Id = .of(18);
    try tenantSend(&a, seven, 1, "message-one-0001");
    try tenantSend(&a, seven, 1, "message-two-0002");
    try tenantSend(&a, eight, 1, "message-thr-0003");
    const one = a.outOn(1, .main, 1).?.payload.len;
    try testing.expectEqual(3 * one, metaOf(a.read(), 1, .main).bytes);
    try testing.expectEqual(2 * one, nsBytesOf(a.read(), 1, .main, seven));
    try testing.expectEqual(one, nsBytesOf(a.read(), 1, .main, eight));
    var buf: [64]u8 = undefined;
    // Trimming one of each namespace's, then the rest of the first's.
    _ = try a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&buf, .{ .to = 1, .lane = 0, .first = 1, .through = 1 }));
    try testing.expectEqual(one, nsBytesOf(a.read(), 1, .main, seven));
    try testing.expectEqual(one, nsBytesOf(a.read(), 1, .main, eight));
    _ = try a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&buf, .{ .to = 1, .lane = 0, .first = 2, .through = 2 }));
    try testing.expectEqual(@as(u64, 0), nsBytesOf(a.read(), 1, .main, seven));
    try testing.expectEqual(one, nsBytesOf(a.read(), 1, .main, eight));
    try testing.expectEqual(@as(usize, 1), a.store.family(OutNsFam.ID).count);
    _ = try a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&buf, .{ .to = 1, .lane = 0, .first = 3, .through = 3 }));
    try testing.expectEqual(@as(usize, 0), a.store.family(OutNsFam.ID).count);
    try testing.expectEqual(@as(u64, 0), metaOf(a.read(), 1, .main).bytes);
}

const TestMessage = struct { entry_type: EntryType, kind: view_mod.Origin.Kind, ns: ns_table.Id, payload: []const u8 };

/// `msgs`, from shard 0 to slot 0, applied in one inbound by `m` on a
/// store of one group: what each was reported as.
fn deliverAll(m: *const machine_mod.Machine, st: *store_mod.Store, scratch: *std.heap.ArenaAllocator, msgs: []const TestMessage) ![]const store_mod.AppliedMessage {
    var mb: [8192]u8 = undefined;
    var w: schema.Writer = .{ .buf = &mb };
    for (msgs) |x| try ce.Message.encode(&w, .{ .slot = 0, .entry_type = @backingInt(x.entry_type), .layout_epoch = 0, .flags = 0, .origin_kind = @backingInt(x.kind), .origin_ns = x.ns.int(), .payload = x.payload });
    var ib: [16384]u8 = undefined;
    const inbound = try ce.Inbound.encodeInto(&ib, .{ .from = 0, .lane = 0, .first_seq = 1, .messages = .{ .bytes = w.written(), .count = @intCast(msgs.len) } });
    var h = std.mem.zeroes(entry_mod.Header);
    h.entry_type = @backingInt(EntryType.channel_inbound);
    h.index = 9;
    h.payload_len = @intCast(inbound.len);
    const e: Entry = .{ .header = h, .payload = inbound };
    var out = Result.start(&e);
    try machine_mod.applyEntry(m, st, scratch, &e, &out);
    return st.messages.items;
}

test "channels: a queue write naming a namespace not its origin's is refused, by its namespace or by its keys; a delivery from its own holds a deadline inside it" {
    const queue_mod = @import("../queue/machine.zig");
    const qe = @import("../protocol/queue_entries.zig");
    const Q = Channels(&.{queue_mod.machine});
    const mine: ns_table.Id = .of(17);
    const other: ns_table.Id = .of(18);
    var pbs: [4][256]u8 = undefined;
    var msgs: [4]TestMessage = undefined;
    for ([_]ns_table.Id{ mine, other }, 0..) |ns, i| msgs[i] = .{ .entry_type = .queue_enqueue, .kind = .user, .ns = mine, .payload = try qe.Enqueue.encodeInto(&pbs[i], .{ .ns = ns.int(), .queue = "q", .priority = 0, .delay_ms = 0, .payload = "body", .max_messages = 16, .max_bytes = 1 << 20, .shard_held_bytes = 1 << 30 }) };
    // A delivery names no namespace the hook reads: its keys do, its
    // lease's deadline by the namespace's id after the expiry.
    for ([_]ns_table.Id{ mine, other }, 2..) |ns, i| msgs[i] = .{ .entry_type = .queue_deliver, .kind = .user, .ns = mine, .payload = try qe.Deliver.encodeInto(&pbs[i], .{ .ns = ns.int(), .queue = "q", .count = 1, .max_bytes = 1 << 20, .holder = "c1", .lease_ms = 60_000 }) };
    var st = store_mod.Store.init(testing.allocator);
    defer st.deinit();
    st.place = .{ .group = 0, .count = 1 };
    st.strict_keys = true;
    var scratch = std.heap.ArenaAllocator.init(testing.allocator);
    defer scratch.deinit();
    const reported = try deliverAll(&Q.machine, &st, &scratch, &msgs);
    try testing.expectEqual(@as(usize, 4), reported.len);
    for (reported, [_]?store_mod.SenderBug{ null, .wrong_origin, null, .wrong_origin }) |r, bug| try testing.expectEqual(bug, r.sender_bug);
    try testing.expectEqualStrings("bad request: a message writing outside its origin's namespace", reported[1].result.refused().?.refusal.message);
    try testing.expectEqualStrings("bad request: a message reaching keys outside its origin's namespace", reported[3].result.refused().?.refusal.message);
    try testing.expect(reported[0].result.refused() == null and reported[2].result.refused() == null);
    try testing.expectEqual(@as(usize, 1), st.family(queue_mod.Msgs.ID).count);
    try testing.expectEqual(@as(usize, 1), st.family(queue_mod.Deadlines.ID).count);
}

test "channels: a user's message to the shard's own state is refused as its sender's bug; the system's applies" {
    const settings = @import("../settings/machine.zig");
    const S = Channels(&.{settings.machine});
    var pb: [16]u8 = undefined;
    const share = try settings.SetShare.encodeInto(&pb, .{ .share_permille = 500 });
    const msgs = [_]TestMessage{
        .{ .entry_type = .settings_pool_share, .kind = .user, .ns = ns_table.DEFAULT, .payload = share },
        .{ .entry_type = .settings_pool_share, .kind = .system, .ns = ns_table.DEFAULT, .payload = share },
    };
    var st = store_mod.Store.init(testing.allocator);
    defer st.deinit();
    st.place = .{ .group = 0, .count = 1 };
    st.strict_keys = true;
    var scratch = std.heap.ArenaAllocator.init(testing.allocator);
    defer scratch.deinit();
    const reported = try deliverAll(&S.machine, &st, &scratch, &msgs);
    try testing.expectEqual(@as(?store_mod.SenderBug, .wrong_origin), reported[0].sender_bug);
    try testing.expectEqualStrings("bad request: a message reaching keys outside its origin's namespace", reported[0].result.refused().?.refusal.message);
    try testing.expectEqual(@as(?store_mod.SenderBug, null), reported[1].sender_bug);
    try testing.expect(reported[1].result.refused() == null);
    try testing.expectEqual(@as(usize, 1), st.family(settings.PolicyFam.ID).count);
}

/// A test message's origin: the default namespace's user.
const TEST_USER: u8 = @backingInt(@import("view.zig").Origin.Kind.user);
const TEST_DEFAULT_NS: u32 = @import("../namespace/table.zig").DEFAULT.int();

/// A test machine whose one entry declares only a range of the queue's
/// deadlines, in the namespace its payload's first four bytes name: no
/// `.id` key refuses it first.
fn deadlineOnlyKeys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const queue_mod = @import("../queue/machine.zig");
    var b: [20]u8 = undefined;
    const ns: ns_table.Id = .of(std.mem.readInt(u32, e.payload[0..4], .big));
    out.addRangeNaming(queue_mod.Deadlines.ID, 8, queue_mod.deadlinePrefix(&b, 12345, ns, queue_mod.hashOf("q"))) catch |err| switch (err) {
        error.NoNamespace => return,
        else => |o| return o,
    };
}
fn deadlineOnlyApply(_: *View, _: *const Entry, _: *Result) ApplyError!void {}
const deadline_only: machine_mod.Machine = .{ .name = "deadline-only", .entry_types = &.{.queue_expire}, .families = &.{machine_mod.info(@import("../queue/machine.zig").Deadlines)}, .keys = deadlineOnlyKeys, .apply = deadlineOnlyApply };

test "channels: a message declaring only a deadline in another namespace is refused at that deadline" {
    const D = Channels(&.{deadline_only});
    var pbs: [2][4]u8 = undefined;
    var msgs: [2]TestMessage = undefined;
    for ([_]ns_table.Id{ .of(17), .of(18) }, &pbs, &msgs) |ns, *pb, *m| {
        std.mem.writeInt(u32, pb, ns.int(), .big);
        m.* = .{ .entry_type = .queue_expire, .kind = .user, .ns = .of(17), .payload = pb };
    }
    var st = store_mod.Store.init(testing.allocator);
    defer st.deinit();
    st.place = .{ .group = 0, .count = 1 };
    st.strict_keys = true;
    var scratch = std.heap.ArenaAllocator.init(testing.allocator);
    defer scratch.deinit();
    const reported = try deliverAll(&D.machine, &st, &scratch, &msgs);
    try testing.expectEqual(@as(?store_mod.SenderBug, null), reported[0].sender_bug);
    try testing.expectEqual(@as(?store_mod.SenderBug, .wrong_origin), reported[1].sender_bug);
}
