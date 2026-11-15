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
    // The sending entry's index: delivered only once that is durable.
    .{ .name = "sent_at", .T = Int(u64) },
    .{ .name = "payload", .T = schema.Bytes(ce.MAX_MESSAGE_PAYLOAD) },
});

/// A channel's sending side: the next seq to give, and the highest seq its
/// receiver durably applied.
pub const Meta = Record(&.{
    .{ .name = "next_seq", .T = Int(u64) },
    .{ .name = "acked", .T = Int(u64) },
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

pub const OutFam = view_mod.Family(55, "channel.out", Stored(Out));
pub const MetaFam = view_mod.Family(56, "channel.out_meta", Stored(Meta));
pub const InFam = view_mod.Family(57, "channel.in", Stored(In));

pub const families = [_]machine_mod.FamilyInfo{ machine_mod.info(OutFam), machine_mod.info(MetaFam), machine_mod.info(InFam) };

// ─── Keys ──────────────────────────────────────────────────────────────────

/// A peer group as a key names it: the group, then its incarnation, which
/// is 0 until a split gives a group a new one.
pub const PEER_LEN = 4;
pub const GROUP_KEY = 1 + PEER_LEN;
pub const OUT_KEY = 1 + PEER_LEN + 8;

pub fn peerBytes(peer: u16) [PEER_LEN]u8 {
    var b: [PEER_LEN]u8 = undefined;
    std.mem.writeInt(u16, b[0..2], peer, .big);
    std.mem.writeInt(u16, b[2..4], 0, .big);
    return b;
}

/// An outbox record's key after its namespace: the receiver, then the seq,
/// big-endian so a range is in seq order.
pub fn outBytes(to: u16, seq: u64) [PEER_LEN + 8]u8 {
    var b: [PEER_LEN + 8]u8 = undefined;
    b[0..PEER_LEN].* = peerBytes(to);
    std.mem.writeInt(u64, b[PEER_LEN..][0..8], seq, .big);
    return b;
}

/// A channel's peer group, under no namespace.
pub fn groupKey(buf: *[GROUP_KEY]u8, peer: u16) []const u8 {
    const b = peerBytes(peer);
    return view_mod.key(buf, "", &b) catch unreachable;
}

pub fn outKey(buf: *[OUT_KEY]u8, to: u16, seq: u64) []const u8 {
    const b = outBytes(to, seq);
    return view_mod.key(buf, "", &b) catch unreachable;
}

/// The prefix of every outbox record to `to`.
pub fn outPrefix(buf: *[GROUP_KEY]u8, to: u16) []const u8 {
    return groupKey(buf, to);
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
        var mk: [GROUP_KEY]u8 = undefined;
        const meta_key = groupKey(&mk, to);
        var meta: Meta.Value = if (try view.getOwn(MetaFam, meta_key)) |m| m.v else .{ .next_seq = 1, .acked = 0 };
        var ok: [OUT_KEY]u8 = undefined;
        try view.putOwn(OutFam, outKey(&ok, to, meta.next_seq), .{ .v = .{
            .slot = s.slot,
            .entry_type = s.entry_type,
            .layout_epoch = 0,
            .flags = s.flags,
            .sent_at = index,
            .payload = s.payload,
        } });
        meta.next_seq += 1;
        try view.putOwn(MetaFam, meta_key, .{ .v = meta });
    }
    view.sends.clearRetainingCapacity();
}

// ─── Reading, for the driver ───────────────────────────────────────────────

pub fn metaOf(read: ReadView, to: u16) Meta.Value {
    var mk: [GROUP_KEY]u8 = undefined;
    const m = read.get(MetaFam, groupKey(&mk, to)) catch null;
    return if (m) |v| v.v else .{ .next_seq = 1, .acked = 0 };
}

pub fn inOf(read: ReadView, from: u16) ?In.Value {
    var ik: [GROUP_KEY]u8 = undefined;
    const v = read.get(InFam, groupKey(&ik, from)) catch null;
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

/// A message as the entry its target applies: the inbound's header, with
/// the message's type, flags and payload.
fn asEntry(e: *const Entry, m: ce.Message.Value) Entry {
    var h = e.header;
    h.entry_type = m.entry_type;
    h.flags = m.flags;
    h.payload_len = @intCast(m.payload.len);
    return .{ .header = h, .payload = m.payload };
}

/// A message type applied after its inbound commits (`ChannelsWith`), and
/// the check of its payload made before that commit: why it won't apply, or
/// null. It covers every reason its applier could halt but running out of
/// memory, and whatever its client path would refuse, so a sender's bad
/// message is refused alone, in the inbound, and never reaches its applier.
pub const AfterCommit = struct {
    entry_type: EntryType,
    refusal: *const fn (payload: []const u8) ?[]const u8,
};

/// The channels machine over `targets`, the machines a message may be for.
pub fn Channels(comptime targets: []const machine_mod.Machine) type {
    return ChannelsWith(targets, &.{});
}

/// `Channels`, with `after_commit` the entry types a message may also be
/// whose applier works outside the view (a stream's append, kept in its
/// projection and read back from the log): the inbound takes their seq and
/// reports them, and the shard applies each once the inbound has committed,
/// in order, live and in replay alike. So within one inbound such a
/// message's state lands after every view message's, whatever their seqs: a
/// view message never sees an earlier one's (a deliver after an append in
/// the same batch doesn't hand out that record; the next deliver does).
/// The shard's after-apply steps then run per message in seq order, over
/// the whole inbound's state.
pub fn ChannelsWith(comptime targets: []const machine_mod.Machine, comptime after_commit: []const AfterCommit) type {
    comptime for (after_commit) |a| std.debug.assert(machine_mod.ownerOf(targets, a.entry_type) == null);
    return struct {
        fn afterCommitOf(t: EntryType) ?AfterCommit {
            inline for (after_commit) |a| if (a.entry_type == t) return a;
            return null;
        }

        fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
            var diag: schema.Diagnostic = .{};
            switch (typeOf(e) orelse return) {
                .channel_inbound => {
                    const p = ce.Inbound.decodeAll(e.payload, &diag) catch return;
                    const b = peerBytes(p.from);
                    try out.add(InFam.ID, "", &b);
                    var it = p.messages.iterator();
                    while (it.next() catch return) |m| {
                        const t = std.enums.fromInt(EntryType, m.entry_type) orelse continue;
                        const owner = machine_mod.ownerOf(targets, t) orelse continue;
                        const as = asEntry(e, m);
                        try owner.keys(&as, out);
                    }
                },
                .channel_trim => {
                    const p = ce.Trim.decodeAll(e.payload, &diag) catch return;
                    const b = peerBytes(p.to);
                    try out.add(MetaFam.ID, "", &b);
                    if (p.through < p.first or p.through - p.first >= ce.MAX_TRIM_SPAN) return;
                    var seq = p.first;
                    while (seq <= p.through) : (seq += 1) {
                        const k = outBytes(p.to, seq);
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
                else => {},
            }
            _ = out;
        }

        fn applyInbound(view: *View, e: *const Entry) ApplyError!void {
            var diag: schema.Diagnostic = .{};
            const store = view.store;
            store.messages.clearRetainingCapacity();
            const place = view.place orelse return error.Halt;
            // Only a leader makes these, from its own outbox's frames: one that
            // doesn't decode is a Flo bug.
            const p = ce.Inbound.decodeAll(e.payload, &diag) catch return error.Halt;
            var ik: [GROUP_KEY]u8 = undefined;
            const in_key = groupKey(&ik, p.from);
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
                // Only a leader's own frames make these: a flag no message
                // may carry is a Flo bug in the sender.
                if (m.flags & ~view_mod.MESSAGE_FLAGS != 0) return error.Halt;
                const t = std.enums.fromInt(EntryType, m.entry_type) orelse return error.Halt;
                const owner = machine_mod.ownerOf(targets, t);
                const after = if (owner == null) afterCommitOf(t) else null;
                const later = after != null;
                if (owner == null and !later) return error.Halt;
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
                } else if (after) |a| {
                    // Checked now: past the commit its applier may only run
                    // out of memory.
                    if (a.refusal(m.payload)) |why| {
                        r.refuse(.bad_request, .malformed, why);
                        bug = .bad_payload;
                    }
                } else if (owner) |o| {
                    try o.apply(view, &as, &r);
                    // Its sends are held to an apply's limits on their own,
                    // and leave with it.
                    if (r.refused() == null) if (tooMuchSent(view.sends.items[mark.sends..])) |why| r.refuse(.bad_request, .malformed, why);
                }
                if (r.refused() != null) try view.rollback(mark) else try writeSends(view, e.header.index);
                try store.messages.append(store.allocator, .{ .entry = as, .result = r, .ordinal = ordinal, .after_commit = later and r.refused() == null, .from = p.from, .sender_bug = bug });
                applied = seq;
            }
            if (before != null and applied == before.?.applied) return;
            try view.put(InFam, in_key, .{ .v = try withPoint(view.arena, before, applied, e.header.index) });
        }

        fn applyTrim(view: *View, e: *const Entry) ApplyError!void {
            var diag: schema.Diagnostic = .{};
            const p = ce.Trim.decodeAll(e.payload, &diag) catch return error.Halt;
            var mk: [GROUP_KEY]u8 = undefined;
            const meta_key = groupKey(&mk, p.to);
            var meta: Meta.Value = if (try view.get(MetaFam, meta_key)) |m| m.v else .{ .next_seq = 1, .acked = 0 };
            // A trim an earlier leader proposed may be stale: a no-op.
            if (p.through <= meta.acked) return;
            if (p.through >= meta.next_seq or p.through < p.first or p.through - p.first >= ce.MAX_TRIM_SPAN) return error.Halt;
            // Everything below `first` must be gone already.
            if (p.first > meta.acked + 1) return error.Halt;
            var seq = meta.acked + 1;
            while (seq <= p.through) : (seq += 1) {
                var ok: [OUT_KEY]u8 = undefined;
                try view.delete(OutFam, outKey(&ok, p.to, seq));
            }
            meta.acked = p.through;
            try view.put(MetaFam, meta_key, .{ .v = meta });
        }

        pub const machine: machine_mod.Machine = .{
            .name = "channels",
            .entry_types = &.{ .channel_inbound, .channel_trim },
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
        var k: [OUT_KEY]u8 = undefined;
        const v = g.read().get(OutFam, outKey(&k, to, seq)) catch return null;
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
    const S = struct {
        var messages: [8192]u8 = undefined;
    };
    var mw: schema.Writer = .{ .buf = &S.messages };
    var seq = first;
    while (seq < first + n) : (seq += 1) {
        const o = sender.outRecord(to, seq).?;
        try ce.Message.encode(&mw, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = o.layout_epoch, .flags = o.flags, .payload = o.payload });
    }
    return ce.Inbound.encodeInto(buf, .{ .from = from, .first_seq = first, .messages = .{ .bytes = mw.written(), .count = @intCast(n) } }) catch unreachable;
}

test "channels: a send is written to the outbox with its entry, in seq order per receiver" {
    var a = Group.init(0, 2);
    defer a.deinit();
    // Slots 1 and 3 are group 1's; 2 is this group's own.
    try a.probeTo(1, "message-one-0001");
    try a.probeTo(3, "message-two-0002");
    try a.probeTo(2, "message-own-0003");
    try testing.expectEqual(@as(u64, 3), metaOf(a.read(), 1).next_seq);
    try testing.expectEqual(@as(u64, 2), metaOf(a.read(), 0).next_seq);
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
    try testing.expectEqual(@as(u64, 2), inOf(b.read(), 0).?.applied);
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 3, &buf));
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0).?.applied);
    try testing.expectEqual(Got{ .from = 0, .seq = 1 }, b.got("message-one-0001").?);
    try testing.expectEqual(Got{ .from = 0, .seq = 3 }, b.got("message-thr-0003").?);
    // An all-duplicate batch changes nothing.
    const index_before = inOf(b.read(), 0).?;
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 2, 2, &buf));
    try testing.expectEqual(index_before.applied, inOf(b.read(), 0).?.applied);
    // A batch past a gap halts, and changes nothing.
    try a.probeTo(1, "message-fou-0004");
    try a.probeTo(1, "message-fiv-0005");
    try testing.expectError(error.Halt, b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 5, 1, &buf)));
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0).?.applied);
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
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0).?.applied);
    try testing.expect(b.got("message-one-0001") != null);
    try testing.expect(b.got("refuse-this-0002") == null);
    // The target's own refusal: not the sender's bug.
    try testing.expectEqual(@as(?store_mod.SenderBug, null), b.store.messages.items[1].sender_bug);
    try testing.expect(b.got("message-thr-0003") != null);
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
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0).?.applied);
    try testing.expect(b.got("relay-this-00001") != null);
    try testing.expect(b.got("flood-this-00002") == null);
    try testing.expect(b.got("message-thr-0003") != null);
    // The relay's one send is in b's outbox, under the inbound's index;
    // the flood sent nothing.
    try testing.expectEqual(@as(u64, 2), metaOf(b.read(), 1).next_seq);
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
    try testing.expectEqual(@as(u64, 4), inOf(b.read(), 0).?.applied);
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
    try testing.expectEqual(@as(u64, 2), inOf(b.read(), 0).?.applied);
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
        try view.sendFlagged(1, @backingInt(EntryType.channel_probe), sink, try ce.Probe.encodeInto(&pb, .{ .slot = 1, .body = "message-snk-0001" }));
        try writeSends(&view, 1);
        try view.commit();
    }
    try testing.expectEqual(sink, a.outRecord(1, 1).?.flags);
    var buf: [8192]u8 = undefined;
    _ = try b.run(&Probed.machine, .channel_inbound, try inboundFrom(&a, 0, 1, 1, 1, &buf));
    try testing.expect(b.got("message-snk-0001") != null);
    try testing.expectEqual(sink, b.store.messages.items[0].entry.header.flags);
}

/// The probe machine, with stream appends applied after the commit.
const Later = ChannelsWith(&.{probe.machine}, &.{.{ .entry_type = .stream_append, .refusal = @import("../projection/stream.zig").appendRefusal }});

/// A stream append's command payload, as an append logs it: one record.
fn appendCommand(buf: []u8) []const u8 {
    return appendCommandOf(buf, 1);
}

/// A stream append's command payload whose batch claims `count` records,
/// holding one: `[partition][count]([len]["x"][header_count])`.
fn appendCommandOf(buf: []u8, count: u32) []const u8 {
    var value: [4 + 4 + 4 + 1 + 2]u8 = undefined;
    std.mem.writeInt(u32, value[0..4], 0, .little);
    std.mem.writeInt(u32, value[4..8], count, .little);
    std.mem.writeInt(u32, value[8..12], 1, .little);
    value[12] = 'x';
    std.mem.writeInt(u16, value[13..15], 0, .little);
    return (entry_mod.buildCommandEntry(.stream_append, 0, 1, 1, 0, .default, "s", &value, buf) orelse unreachable).payload;
}

test "channels: an after-commit message its applier would halt on, or its client path refuse, is refused alone before the commit" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var b = Group.init(1, 2);
    defer b.deinit();
    try a.probeTo(1, "message-one-0001");
    var mw: schema.Writer = .{ .buf = try testing.allocator.alloc(u8, 1024) };
    defer testing.allocator.free(mw.buf);
    const o = a.outRecord(1, 1).?;
    try ce.Message.encode(&mw, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = 0, .flags = 0, .payload = o.payload });
    // Not a command payload: past the commit, the stream applier would halt.
    try ce.Message.encode(&mw, .{ .slot = 1, .entry_type = @backingInt(EntryType.stream_append), .layout_epoch = 0, .flags = 0, .payload = "x" });
    // A command, but its batch claims more records than an append may carry.
    var over: [64]u8 = undefined;
    try ce.Message.encode(&mw, .{ .slot = 1, .entry_type = @backingInt(EntryType.stream_append), .layout_epoch = 0, .flags = 0, .payload = appendCommandOf(&over, @import("../projection/stream.zig").MAX_BATCH_RECORDS + 1) });
    var buf: [2048]u8 = undefined;
    const inbound = try ce.Inbound.encodeInto(&buf, .{ .from = 0, .first_seq = 1, .messages = .{ .bytes = mw.written(), .count = 3 } });
    _ = try b.run(&Later.machine, .channel_inbound, inbound);
    // Each refused alone, before the commit, and never handed to its
    // applier; the probe before them applied, and all three took their seq.
    try testing.expect(b.got("message-one-0001") != null);
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0).?.applied);
    const reported = b.store.messages.items;
    try testing.expect(reported[1].result.refused() != null and !reported[1].after_commit);
    try testing.expect(reported[2].result.refused() != null and !reported[2].after_commit);
    // Counted as the sender's bugs, apart from the probe's own refusals.
    try testing.expectEqual(@as(?store_mod.SenderBug, null), reported[0].sender_bug);
    try testing.expectEqual(@as(?store_mod.SenderBug, .bad_payload), reported[1].sender_bug);
    try testing.expectEqual(@as(?store_mod.SenderBug, .bad_payload), reported[2].sender_bug);
    try testing.expectEqual(@as(u16, 0), reported[1].from);
}

test "channels: a message whose applier runs after the commit takes its seq and is reported for it, unless refused" {
    var b = Group.init(1, 3);
    defer b.deinit();
    var mw: schema.Writer = .{ .buf = try testing.allocator.alloc(u8, 1024) };
    defer testing.allocator.free(mw.buf);
    var cb: [64]u8 = undefined;
    const append = appendCommand(&cb);
    // Slot 1 is b's; slot 3 is group 0's.
    for ([_]u16{ 1, 3, 1 }) |slot| try ce.Message.encode(&mw, .{ .slot = slot, .entry_type = @backingInt(EntryType.stream_append), .layout_epoch = 0, .flags = 0, .payload = append });
    var buf: [2048]u8 = undefined;
    const inbound = try ce.Inbound.encodeInto(&buf, .{ .from = 0, .first_seq = 1, .messages = .{ .bytes = mw.written(), .count = 3 } });
    _ = try b.run(&Later.machine, .channel_inbound, inbound);
    try testing.expectEqual(@as(u64, 3), inOf(b.read(), 0).?.applied);
    const reported = b.store.messages.items;
    try testing.expectEqual(@as(usize, 3), reported.len);
    for (reported, [_]bool{ true, false, true }, 0..) |m, later, i| {
        try testing.expectEqual(later, m.after_commit);
        try testing.expectEqual(@as(u8, @intCast(i)), m.ordinal);
    }
    // A type with no applier at all is still a Flo bug.
    try testing.expectError(error.Halt, b.run(&Probed.machine, .channel_inbound, try ce.Inbound.encodeInto(&buf, .{ .from = 2, .first_seq = 1, .messages = .{ .bytes = mw.written(), .count = 3 } })));
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
    try ce.Message.encode(&mw, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = 0, .flags = entry_mod.Flags.SINK, .payload = o.payload });
    try ce.Message.encode(&mw, .{ .slot = o.slot, .entry_type = o.entry_type, .layout_epoch = 0, .flags = entry_mod.Flags.FORCE, .payload = o.payload });
    var buf: [2048]u8 = undefined;
    const inbound = try ce.Inbound.encodeInto(&buf, .{ .from = 0, .first_seq = 1, .messages = .{ .bytes = mw.written(), .count = 2 } });
    try testing.expectError(error.Halt, b.run(&Probed.machine, .channel_inbound, inbound));
    try testing.expect(b.got("message-one-0001") == null);
    try testing.expect(inOf(b.read(), 0) == null);
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
    const in = inOf(b.read(), 0).?;
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
    _ = try a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&buf, .{ .to = 1, .first = 1, .through = 2 }));
    try testing.expectEqual(@as(u64, 2), metaOf(a.read(), 1).acked);
    try testing.expect(a.outRecord(1, 1) == null and a.outRecord(1, 2) == null);
    try testing.expect(a.outRecord(1, 3) != null);
    _ = try a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&buf, .{ .to = 1, .first = 1, .through = 1 }));
    try testing.expectEqual(@as(u64, 2), metaOf(a.read(), 1).acked);
    try testing.expectError(error.Halt, a.run(&Probed.machine, .channel_trim, try ce.Trim.encodeInto(&buf, .{ .to = 1, .first = 3, .through = 4 })));
}

test "channels: an apply that sends too much is refused, and sends nothing" {
    var a = Group.init(0, 2);
    defer a.deinit();
    var sends: [MAX_SENDS + 1]view_mod.Send = undefined;
    for (&sends) |*s| s.* = .{ .slot = 1, .entry_type = 0, .payload = "x" };
    try testing.expect(tooMuchSent(sends[0..MAX_SENDS]) == null);
    try testing.expect(tooMuchSent(&sends) != null);
}
