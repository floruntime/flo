//! Consumer-group entries for the VOPR workload, made as the leader's
//! handlers make them: from the proposing leader's own applied state, so a
//! deliver names the ready records its leader sees and a report names a
//! live delivery. Reports are spoiled now and then (a stale token, another
//! holder, a wrong delivery count, a record gone) so the machine's
//! per-record refusals run too. Streams grow by appends, are trimmed and
//! now and then deleted, so floors move, generations turn and cleanup runs.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const ge = @import("../protocol/group_entries.zig");
const ae = @import("../protocol/action_entries.zig");
const qe = @import("../protocol/queue_entries.zig");
const gm = @import("../stream/group_machine.zig");
const sm = @import("../stream/machine.zig");
const stream_proj = @import("../projection/stream.zig");
const store_mod = @import("../contract/store.zig");
const view_mod = @import("../contract/view.zig");
const schema = @import("../protocol/schema.zig");
const gf = @import("group_family.zig");
const Leader = @import("action_workload.zig").Leader;

const EntryType = entry_mod.EntryType;
const StreamID = gm.StreamID;
const ConsumerGroups = gf.ConsumerGroups;
const NS = gf.NS;
const STREAMS = gf.STREAMS;
const GROUPS = gf.GROUPS;

pub const Made = struct { entry_type: EntryType, payload: []const u8, flags: u16 = 0 };

/// Holders whose name starts so never report: their leases expire.
const DEAD = "x";
/// Holders whose name starts so hold their records past half the lease
/// before they report: they keep leases alive, extending.
const SLOW = "s";
const HOLDERS = [_][]const u8{ "c0", "c1", "s0", "x0" };
/// A stream stops growing past this many records: appends wait for trims.
const MAX_RECORDS: u64 = 64;
const LEASES = [_]u64{ ae.MIN_LEASE_MS, 2 * ae.MIN_LEASE_MS, ge.DEFAULT_LEASE_MS };

const Kind = enum { append, deliver, settle, release, extend, expire, clean, trim, create, delete, stream_delete, steal, requeue, drop, stream_clean, stream_forget };
/// Out of 100.
const WEIGHTS = [_]u8{ 14, 18, 11, 11, 9, 9, 7, 3, 4, 2, 2, 2, 3, 2, 2, 1 };
/// Percent of reports spoiled.
const SPOIL: u8 = 15;

/// A group entry for op `op_id`, in `buf`, at most `max_len` bytes, from
/// the leader's applied state (`g` and its `store`); null when the leader
/// has nothing of the kind drawn to propose.
pub fn make(g: *ConsumerGroups, store: *store_mod.Store, l: Leader, r: std.Random, buf: []u8, max_len: usize) ?Made {
    g.bind(store);
    const out = buf[0..@min(buf.len, max_len)];
    var roll = r.uintLessThan(u8, 100);
    const kind: Kind = for (WEIGHTS, 0..) |w, i| {
        if (roll < w) break @fromBackingInt(@intCast(i));
        roll -= w;
    } else .append;
    var s = r.uintLessThan(usize, STREAMS.len);
    const grp = r.uintLessThan(usize, GROUPS.len);
    // The duty's cleans and forgets go mostly to tombstones, as its walk
    // does; and a tombstone is mostly left to them, so that some are
    // forgotten before an append makes the stream again.
    const tomb = tombstoneOf(store, r);
    if (kind == .stream_clean or kind == .stream_forget) if (tomb) |t| {
        s = t;
    };
    if (kind == .append and isTombstone(store, s) and r.uintLessThan(u8, 8) != 0) return null;
    return switch (kind) {
        .append => append(g, out, r, s),
        .deliver => deliver(g, store, out, l, r, s, grp),
        .settle, .release, .extend => report(store, out, kind, l, r, s, grp),
        .expire => expire(store, out, l, r),
        .clean => clean(out),
        .trim => trim(g, out, r, s),
        .create => create(store, out, r, s, grp),
        // A delete takes every record its group holds, so it is rare.
        .delete => if (r.uintLessThan(u8, 4) == 0) delete(out, s, grp) else null,
        // Deletes and the appends after them make streams again over their
        // tombstones, while the duty's cleans and forgets race them.
        .stream_delete => if (r.uintLessThan(u8, 4) == 0) streamDelete(out, r, s) else null,
        .stream_clean => streamClean(out, s),
        .stream_forget => streamForget(store, out, r, s),
        .steal => steal(store, out, l, r, s, grp),
        .requeue, .drop => dead(store, out, kind, r, s, grp),
    } catch null;
}

fn isTombstone(store: *store_mod.Store, s: usize) bool {
    const m = sm.metaAt(.{ .store = store }, gm.streamOf(NS, STREAMS[s])) orelse return false;
    return !m.exists;
}

/// A tombstoned stream, from a random start, if any is.
fn tombstoneOf(store: *store_mod.Store, r: std.Random) ?usize {
    const start = r.uintLessThan(usize, STREAMS.len);
    for (0..STREAMS.len) |i| {
        const s = (start + i) % STREAMS.len;
        if (isTombstone(store, s)) return s;
    }
    return null;
}

fn recordsOf(g: *ConsumerGroups, s: usize) u64 {
    const sk = gm.streamOf(NS, STREAMS[s]);
    const first = g.stream.streamFirstId(sk);
    if (first.eql(StreamID.MIN)) return 0;
    var ids: [MAX_RECORDS + 1]StreamID = undefined;
    return g.stream.streamIdsAfter(sk, g.stream.streamRemovedThrough(sk), &ids);
}

fn append(g: *ConsumerGroups, out: []u8, r: std.Random, s: usize) !?Made {
    if (recordsOf(g, s) >= MAX_RECORDS) return null;
    var v: [gf.MAX_APPEND_VALUE]u8 = undefined;
    return .{ .entry_type = .stream_append, .payload = gf.streamEntry(out, STREAMS[s], gf.appendValue(&v, 1 + r.uintLessThan(u32, 3))) orelse return null };
}

fn trim(g: *ConsumerGroups, out: []u8, r: std.Random, s: usize) !?Made {
    const sk = gm.streamOf(NS, STREAMS[s]);
    var ids: [MAX_RECORDS + 1]StreamID = undefined;
    const n = g.stream.streamIdsAfter(sk, g.stream.streamRemovedThrough(sk), &ids);
    // Through some of the oldest quarter, and only once the stream has
    // grown: leases on newer records live long enough to be extended.
    if (n < MAX_RECORDS / 2) return null;
    const at = r.uintLessThan(usize, n / 4);
    var kb: [stream_proj.TrimBound.MAX_SIZE]u8 = undefined;
    const key = (stream_proj.TrimBound{ .id = ids[@min(at, n - 1)] }).encode(&kb);
    const hash = gf.streamHash(STREAMS[s]);
    return .{ .entry_type = .stream_trim, .payload = gf.streamEntry(out, key, &hash) orelse return null };
}

/// Half of them forced, as `--force` sends it: one unforced of a stream
/// holding records is refused.
fn streamDelete(out: []u8, r: std.Random, s: usize) !?Made {
    return .{ .entry_type = .stream_delete, .payload = gf.streamEntry(out, STREAMS[s], "") orelse return null, .flags = if (r.boolean()) entry_mod.Flags.FORCE else 0 };
}

/// The retention duty's clean of a stream, marked or not.
fn streamClean(out: []u8, s: usize) !?Made {
    const hash = gf.streamHash(STREAMS[s]);
    return .{ .entry_type = .stream_clean, .payload = gf.streamEntry(out, "", &hash) orelse return null };
}

/// The duty's forget of a stream's tombstone, at the generation the leader
/// holds; now and then another, which changes nothing.
fn streamForget(store: *store_mod.Store, out: []u8, r: std.Random, s: usize) !?Made {
    const held = sm.metaAt(.{ .store = store }, gm.streamOf(NS, STREAMS[s]));
    var gen: u64 = if (held) |m| m.gen else 0;
    if (r.uintLessThan(u8, 4) == 0) gen +%= 1;
    var value: [16]u8 = undefined;
    value[0..8].* = gf.streamHash(STREAMS[s]);
    std.mem.writeInt(u64, value[8..16], gen, .little);
    return .{ .entry_type = .stream_forget, .payload = gf.streamEntry(out, "", &value) orelse return null };
}

fn create(store: *store_mod.Store, out: []u8, r: std.Random, s: usize, grp: usize) !?Made {
    // Mostly the settings it has, so a create of a group there changes
    // nothing; now and then others, refused.
    var lease = LEASES[r.uintLessThan(usize, LEASES.len)];
    var attempts: u32 = 1 + r.uintLessThan(u32, 4);
    if (ConsumerGroups.meta(store, STREAMS[s], GROUPS[grp])) |m| if (r.uintLessThan(u8, 4) != 0) {
        lease = m.lease_ms;
        attempts = m.max_attempts;
    };
    const p = try ge.Create.encodeInto(out, .{ .ns = NS.int(), .stream = STREAMS[s], .group = GROUPS[grp], .lease_ms = lease, .max_attempts = attempts });
    return .{ .entry_type = .group_create, .payload = p };
}

fn delete(out: []u8, s: usize, grp: usize) !?Made {
    const p = try ge.Delete.encodeInto(out, .{ .ns = NS.int(), .stream = STREAMS[s], .group = GROUPS[grp] });
    return .{ .entry_type = .group_delete, .payload = p };
}

/// The group's ready records due by the leader's clock, in its ready keys'
/// order, as the handler chooses them.
fn readyDue(store: *store_mod.Store, s: usize, grp: usize, now: u64, out: []StreamID) usize {
    const scope = gm.scopeOf(NS, STREAMS[s], GROUPS[grp]);
    var pb: [gm.SCOPE_LEN + 1]u8 = undefined;
    var kb: [4 + gm.SCOPE_LEN + 1]u8 = undefined;
    var it = (view_mod.ReadView{ .store = store }).range(gm.Ready, gm.storeKey(&kb, NS, gm.readyPrefix(&pb, &scope)));
    var n: usize = 0;
    while (it.next() catch null) |entry| {
        if (n == out.len) break;
        const k = gm.parseReadyKey(entry.key[4..], gm.SCOPE_LEN) orelse continue;
        if (k.due_ns > now) break;
        out[n] = gm.recordOf(k.id);
        n += 1;
    }
    return n;
}

fn deliver(g: *ConsumerGroups, store: *store_mod.Store, out: []u8, l: Leader, r: std.Random, s: usize, grp: usize) !?Made {
    const sk = gm.streamOf(NS, STREAMS[s]);
    var ready: [8]StreamID = undefined;
    const n = readyDue(store, s, grp, l.now_ns, &ready);
    var rb: [8 * ge.RecordId.max_size]u8 = undefined;
    var w: schema.Writer = .{ .buf = &rb };
    for (ready[0..n]) |rec| try ge.RecordId.encode(&w, .{ .ms = rec.timestamp_ms, .seq = rec.sequence });
    // The bound is the stream's last record the leader holds; now and then
    // one short, so a deliver leaves the newest for the next.
    var through = g.stream.streamLastId(sk);
    if (r.uintLessThan(u8, 5) == 0 and through.sequence > 0) through.sequence -= 1;
    const holder = HOLDERS[r.uintLessThan(usize, HOLDERS.len)];
    // Mostly the default caps; now and then the group's or the shard's
    // exactly at its edge, by the leader's state.
    var max_held: u32 = qe.DEFAULT_MAX_MESSAGES;
    var max_held_bytes: u64 = qe.DEFAULT_MAX_BYTES;
    var shard_held: u64 = qe.DEFAULT_SHARD_HELD_BYTES;
    const m = ConsumerGroups.meta(store, STREAMS[s], GROUPS[grp]);
    switch (r.uintLessThan(u8, 12)) {
        0 => max_held = @intCast(if (m) |x| x.ready + x.leased + x.dead + r.uintLessThan(u64, 2) else 1),
        1 => max_held_bytes = (if (m) |x| x.held_bytes else 0) + gm.heldBy(0) * r.uintLessThan(u64, 2),
        2 => shard_held = shardHeld(store) + gm.heldBy(0) * r.uintLessThan(u64, 2),
        else => {},
    }
    const p = try ge.Deliver.encodeInto(out, .{
        .ns = NS.int(),
        .stream = STREAMS[s],
        .group = GROUPS[grp],
        .count = 1 + r.uintLessThan(u32, 4),
        // Some small, so the answer's budget stops a deliver short.
        .max_bytes = if (r.uintLessThan(u8, 4) == 0) 64 else 1 << 20,
        .holder = holder,
        // A slow holder's lease is the shortest, so it passes half while
        // the holder still holds it.
        .lease_ms = if (std.mem.startsWith(u8, holder, SLOW)) ae.MIN_LEASE_MS else LEASES[r.uintLessThan(usize, LEASES.len)],
        .create = r.uintLessThan(u8, 8) != 0,
        .ready = .{ .bytes = rb[0..w.pos], .count = @intCast(n) },
        .through_ms = through.timestamp_ms,
        .through_seq = through.sequence,
        .max_held = max_held,
        .max_held_bytes = max_held_bytes,
        .shard_held_bytes = shard_held,
    });
    return .{ .entry_type = .group_deliver, .payload = p };
}

fn shardHeld(store: *store_mod.Store) u64 {
    const qm = @import("../queue/machine.zig");
    const bytes = store.get(qm.Helds.ID, qm.HELD_KEY) orelse return 0;
    return (qm.Helds.Value.decode(bytes) catch return 0).v.bytes;
}

/// One delivery the leader sees live, by a holder that reports.
const Delivery = struct {
    holder: [8]u8 = undefined,
    holder_len: usize = 0,
    token: ae.Token.Value = .{ .term = 0, .index = 0 },
    expiry_ns: u64 = 0,
    lease_ms: u64 = 0,
    delivered_ns: u64 = 0,
    records: [8]StreamID = undefined,
    deliveries: [8]u32 = undefined,
    n: usize = 0,

    fn holderName(d: *const Delivery) []const u8 {
        return d.holder[0..d.holder_len];
    }
};

/// The live delivery of the group's first leased record whose holder
/// reports, and its other records under the same lease.
fn liveDelivery(store: *store_mod.Store, s: usize, grp: usize, r: std.Random, now: u64, only_past_half: bool) ?Delivery {
    const scope = gm.scopeOf(NS, STREAMS[s], GROUPS[grp]);
    var kb: [4 + gm.SCOPE_LEN]u8 = undefined;
    var it = (view_mod.ReadView{ .store = store }).range(gm.Items, gm.storeKey(&kb, NS, &scope));
    var d: ?Delivery = null;
    // Not always the first: a later delivery is sometimes the one reported.
    var skip = r.uintLessThan(u8, 3);
    while (it.next() catch null) |entry| {
        const v = entry.value.v;
        if (v.status != .leased or std.mem.startsWith(u8, v.holder, DEAD) or v.holder.len > 8) continue;
        // An extend that moves a lease is one sent with half of it or less
        // left, as a holder's keepalive is; a slow holder reports nothing
        // else before then.
        const past_half = v.expiry_ns -| now <= (v.lease_ms *| std.time.ns_per_ms) / 2;
        // A slow holder settles or releases a record only once it has kept
        // the lease alive past its first term.
        const extended = v.expiry_ns > v.delivered_ns +| v.lease_ms *| std.time.ns_per_ms;
        const slow = std.mem.startsWith(u8, v.holder, SLOW);
        if (d == null and only_past_half and !past_half) continue;
        if (d == null and !only_past_half and slow and !extended) continue;
        if (d == null) {
            if (skip > 0) {
                skip -= 1;
                continue;
            }
            var x: Delivery = .{ .token = v.token, .expiry_ns = v.expiry_ns, .lease_ms = v.lease_ms, .delivered_ns = v.delivered_ns, .holder_len = v.holder.len };
            @memcpy(x.holder[0..v.holder.len], v.holder);
            d = x;
        }
        const x = &d.?;
        if (x.n == x.records.len) break;
        if (v.token.term != x.token.term or v.token.index != x.token.index or !std.mem.eql(u8, v.holder, x.holderName())) continue;
        x.records[x.n] = gm.recordOf(entry.key[4..]);
        x.deliveries[x.n] = v.deliveries;
        x.n += 1;
    }
    return d;
}

fn report(store: *store_mod.Store, out: []u8, kind: Kind, l: Leader, r: std.Random, s: usize, grp: usize) !?Made {
    const extend_late = kind == .extend and r.uintLessThan(u8, 8) != 0;
    var d = liveDelivery(store, s, grp, r, l.now_ns, extend_late) orelse liveDelivery(store, s, grp, r, l.now_ns, false) orelse return null;
    var holder = d.holderName();
    if (r.uintLessThan(u8, 100) < SPOIL) switch (r.uintLessThan(u8, 4)) {
        0 => d.token.index +%= 1,
        1 => holder = "c9",
        2 => d.deliveries[0] +%= 1,
        // A record the group never held.
        else => d.records[0] = .{ .timestamp_ms = 1, .sequence = 1 << 40 },
    };
    // A few of the delivery's records, the first always.
    const n = 1 + r.uintLessThan(usize, d.n);
    var ib: [8 * ge.Item.max_size]u8 = undefined;
    var w: schema.Writer = .{ .buf = &ib };
    for (d.records[0..n], d.deliveries[0..n]) |rec, dv| try ge.Item.encode(&w, .{ .ms = rec.timestamp_ms, .seq = rec.sequence, .deliveries = dv });
    const items: ge.Items.Value = .{ .bytes = ib[0..w.pos], .count = @intCast(n) };
    const common: ge.Settle.Value = .{ .ns = NS.int(), .stream = STREAMS[s], .group = GROUPS[grp], .holder = holder, .token = d.token, .expiry_ns = d.expiry_ns, .items = items };
    switch (kind) {
        .settle => return .{ .entry_type = .group_settle, .payload = try ge.Settle.encodeInto(out, common) },
        .extend => return .{ .entry_type = .group_extend, .payload = try ge.Extend.encodeInto(out, .{ .ns = common.ns, .stream = common.stream, .group = common.group, .holder = common.holder, .token = common.token, .expiry_ns = common.expiry_ns, .items = common.items, .held_since_ns = d.delivered_ns, .lease_ms = d.lease_ms }) },
        else => {
            // A nack mostly, plain, delayed, with an error or dead; an
            // abandon or the leader's unread now and then.
            const roll = r.uintLessThan(u8, 10);
            const ended: ge.HandBack = if (roll < 7) .failed else if (roll < 9) .abandoned else .unread;
            // Now and then a hand-back with a failure's options as well,
            // from a proposer other than the handler: refused at apply.
            const mixed = ended != .failed and r.uintLessThan(u8, 6) == 0;
            const failed = ended == .failed;
            // Which of a failure's options a mixed hand-back carries.
            const pick = if (mixed) r.uintLessThan(u8, 3) else 0;
            return .{ .entry_type = .group_release, .payload = try ge.Release.encodeInto(out, .{
                .ns = common.ns,
                .stream = common.stream,
                .group = common.group,
                .holder = common.holder,
                .token = common.token,
                .expiry_ns = common.expiry_ns,
                .items = common.items,
                .delay_ms = if (mixed) (if (pick == 0) 500 else 0) else if (failed and r.uintLessThan(u8, 3) == 0) r.uintLessThan(u64, 3_000) else 0,
                .dead = if (mixed) pick == 1 else failed and r.uintLessThan(u8, 10) == 0,
                .@"error" = if (mixed) (if (pick == 2) "boom" else "") else if (failed and r.boolean()) "boom" else "",
                .ended = ended,
            }) };
        },
    }
}

fn expire(store: *store_mod.Store, out: []u8, l: Leader, r: std.Random) !?Made {
    var ib: [17 * ge.ExpireItem.max_size]u8 = undefined;
    var w: schema.Writer = .{ .buf = &ib };
    var n: u32 = 0;
    // As many as the entry's room holds, a spoiled one's too.
    const room: u32 = @intCast(@min(16, (out.len -| 64) / ge.ExpireItem.max_size));
    if (room < 2) return null;
    // Live leases past due first, then keys a settle or a cleanup left: the
    // left ones are oldest, and would crowd out the leases that must end.
    for ([_]bool{ true, false }) |live_pass| {
        var it = gm.pastDue(.{ .store = store }, l.now_ns);
        while (it.next()) |d| {
            if (n == room - 1) break;
            if (liveLease(store, d.key.ns, d.key.id, d.key.expiry_ns) != live_pass) continue;
            try ge.ExpireItem.encode(&w, .{ .ns = d.key.ns.int(), .id = d.key.id, .expiry_ns = d.key.expiry_ns });
            n += 1;
        }
    }
    // Now and then a live lease not yet due as well, as a leader whose
    // clock ran ahead names it: the machine must leave it.
    if (r.uintLessThan(u8, 100) < SPOIL) {
        var all = (view_mod.ReadView{ .store = store }).range(gm.Deadlines, "");
        while (all.next() catch null) |item| {
            const k = gm.parseDeadlineKey(item.key[1..]) orelse continue;
            if (k.expiry_ns <= l.now_ns) continue;
            try ge.ExpireItem.encode(&w, .{ .ns = k.ns.int(), .id = k.id, .expiry_ns = k.expiry_ns });
            n += 1;
            break;
        }
    }
    if (n == 0) return null;
    return .{ .entry_type = .group_expire, .payload = try ge.Expire.encodeInto(out, .{ .items = .{ .bytes = ib[0..w.pos], .count = n } }) };
}

/// Whether a deadline key is its record's live lease.
fn liveLease(store: *store_mod.Store, ns: gm.NamespaceId, id: []const u8, expiry_ns: u64) bool {
    var b: [4 + gm.ID_LEN]u8 = undefined;
    const got = (view_mod.ReadView{ .store = store }).get(gm.Items, gm.storeKey(&b, ns, id)) catch return false;
    const v = (got orelse return false).v;
    return v.status == .leased and v.expiry_ns == expiry_ns;
}

fn clean(out: []u8) !?Made {
    // Every group's scope: the machine cleans what a gone group, or a
    // trim, left, and nothing else.
    var ib: [STREAMS.len * GROUPS.len * ge.CleanItem.max_size]u8 = undefined;
    var w: schema.Writer = .{ .buf = &ib };
    var n: u32 = 0;
    for (STREAMS) |s| for (GROUPS) |grp| {
        try ge.CleanItem.encode(&w, .{ .ns = NS.int(), .scope = &gm.scopeOf(NS, s, grp) });
        n += 1;
    };
    return .{ .entry_type = .group_clean, .payload = try ge.Clean.encodeInto(out, .{ .items = .{ .bytes = ib[0..w.pos], .count = n } }) };
}

/// An operator's steal of a leased record: mostly one idle long enough,
/// now and then too short an idle time to take it, or a stale count.
fn steal(store: *store_mod.Store, out: []u8, l: Leader, r: std.Random, s: usize, grp: usize) !?Made {
    const scope = gm.scopeOf(NS, STREAMS[s], GROUPS[grp]);
    var kb: [4 + gm.SCOPE_LEN]u8 = undefined;
    var it = (view_mod.ReadView{ .store = store }).range(gm.Items, gm.storeKey(&kb, NS, &scope));
    var rec: ?StreamID = null;
    var deliveries: u32 = 0;
    var idle_ns: u64 = 0;
    while (it.next() catch null) |entry| {
        const v = entry.value.v;
        if (v.status != .leased) continue;
        rec = gm.recordOf(entry.key[4..]);
        deliveries = v.deliveries;
        idle_ns = l.now_ns -| v.touched_ns;
        if (r.boolean()) break;
    }
    const target = rec orelse return null;
    const idle_ms = idle_ns / std.time.ns_per_ms;
    const min_idle: u64 = if (r.uintLessThan(u8, 4) == 0) idle_ms + 1 + r.uintLessThan(u64, 1_000) else @max(1, idle_ms / 2);
    if (r.uintLessThan(u8, 100) < SPOIL) deliveries +%= 1;
    var ib: [ge.Item.max_size]u8 = undefined;
    var w: schema.Writer = .{ .buf = &ib };
    try ge.Item.encode(&w, .{ .ms = target.timestamp_ms, .seq = target.sequence, .deliveries = deliveries });
    const p = try ge.Steal.encodeInto(out, .{ .ns = NS.int(), .stream = STREAMS[s], .group = GROUPS[grp], .holder = "c1", .lease_ms = LEASES[r.uintLessThan(usize, LEASES.len)], .min_idle_ms = @min(min_idle, ae.MAX_LEASE_MS), .items = .{ .bytes = ib[0..w.pos], .count = 1 } });
    return .{ .entry_type = .group_steal, .payload = p };
}

/// An operator's requeue or drop of a group's dead letters, now and then
/// naming a live record as well: refused, not dead.
fn dead(store: *store_mod.Store, out: []u8, kind: Kind, r: std.Random, s: usize, grp: usize) !?Made {
    const scope = gm.scopeOf(NS, STREAMS[s], GROUPS[grp]);
    var kb: [4 + gm.SCOPE_LEN]u8 = undefined;
    var it = (view_mod.ReadView{ .store = store }).range(gm.Items, gm.storeKey(&kb, NS, &scope));
    var ib: [4 * ge.RecordId.max_size]u8 = undefined;
    var w: schema.Writer = .{ .buf = &ib };
    var n: u32 = 0;
    // Room for one live record, now and then.
    var live_room = r.uintLessThan(u8, 100) < SPOIL;
    while (it.next() catch null) |entry| {
        if (n == 3) break;
        const v = entry.value.v;
        if (v.status != .dead) {
            if (!live_room) continue;
            live_room = false;
        }
        const rec = gm.recordOf(entry.key[4..]);
        try ge.RecordId.encode(&w, .{ .ms = rec.timestamp_ms, .seq = rec.sequence });
        n += 1;
    }
    if (n == 0) return null;
    const p = try ge.DeadItems.encodeInto(out, .{ .ns = NS.int(), .stream = STREAMS[s], .group = GROUPS[grp], .items = .{ .bytes = ib[0..w.pos], .count = n } });
    return .{ .entry_type = if (kind == .requeue) .group_requeue else .group_drop, .payload = p };
}
