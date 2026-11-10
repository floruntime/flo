//! Queue entries for the VOPR workload, made as the leader's handler makes
//! them: from the proposing leader's own applied queue state, so a report
//! names a live delivery. Every report kind is also spoiled now and then (a
//! stale token, a wrong delivery count, another consumer, a resend, a
//! message that's gone) so the machine's per-message refusals are
//! exercised too. The leader's state can lag the log, so a valid report
//! may still find its delivery gone when it applies, as on a real leader.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const qe = @import("../protocol/queue_entries.zig");
const ae = @import("../protocol/action_entries.zig");
const qm = @import("../queue/machine.zig");
const families = @import("families.zig");
const Leader = @import("action_workload.zig").Leader;

const EntryType = entry_mod.EntryType;
const NamespaceId = qm.NamespaceId;

const NS: NamespaceId = .default;
const QUEUES = [_][]const u8{ "q0", "q1" };
/// Consumers whose name starts so die holding their messages: nothing
/// reports for them, and their leases expire.
const DEAD = "x";
/// Consumers that hold their messages longer than a lease, extending.
const SLOW = "s";
/// Consumers that dequeue with ack: their deliver is followed by a
/// settle of the whole delivery, as the handler proposes one.
const ACK = "a";
const MAX_GRACE_MS: u64 = 30_000;
/// A queue stops growing past this many messages: enqueues wait for the
/// consumers, so a deliver's picker always sees every due message.
const MAX_HELD: u64 = 48;

pub const Made = struct { entry_type: EntryType, payload: []const u8 };

const Kind = enum { enqueue, deliver, settle, release, extend, expire, purge };
/// Out of 100.
const WEIGHTS = [_]u8{ 30, 24, 16, 10, 6, 13, 1 };
/// Percent of entries spoiled, by kind.
const SPOIL = [_]u8{ 0, 0, 15, 15, 12, 15, 0 };

/// A queue entry for op `op_id`, in `buf`, at most `max_len` bytes; null
/// when the leader has nothing of the kind drawn to propose.
pub fn make(q: *families.Queues, l: Leader, r: std.Random, op_id: u64, buf: []u8, max_len: usize) ?Made {
    const out = buf[0..@min(buf.len, max_len)];
    // An acked delivery's settle comes first: the handler proposes it as
    // soon as the deliver applies.
    // A lagging leader would propose it again until it applies; half the
    // time is enough to keep acked messages from ever being released.
    if (r.boolean()) if (ackSettle(q, out, l.term) catch null) |m| return m;
    var roll = r.uintLessThan(u8, 100);
    const kind: Kind = for (WEIGHTS, 0..) |w, i| {
        if (roll < w) break @fromBackingInt(@intCast(i));
        roll -= w;
    } else .enqueue;
    const spoil = r.uintLessThan(u8, 100) < SPOIL[@backingInt(kind)];
    const queue = QUEUES[r.uintLessThan(usize, QUEUES.len)];
    var hex: [17]u8 = undefined;
    const tag = std.fmt.bufPrint(&hex, "{x}", .{op_id}) catch unreachable;
    return switch (kind) {
        .enqueue => enqueue(q, out, r, queue, tag),
        .deliver => deliver(q, out, l, r, queue, tag),
        .settle, .release, .extend => report(q, out, kind, l, r, queue, spoil),
        .expire => expire(q, out, l, r, spoil),
        // Rare: a purge ends every lease in its queue, the dead holders'
        // too, which then never expire.
        .purge => if (r.uintLessThan(u8, 10) == 0) purge(out, queue) else null,
    } catch null;
}

fn held(q: *families.Queues, queue: []const u8) u64 {
    const m = q.meta(NS, qm.hashOf(queue)) orelse return 0;
    return m.ready + m.leased + m.dead;
}

fn enqueue(q: *families.Queues, out: []u8, r: std.Random, queue: []const u8, tag: []const u8) !?Made {
    if (held(q, queue) >= MAX_HELD) return null;
    var payload_buf: [64]u8 = undefined;
    @memcpy(payload_buf[0..tag.len], tag);
    @memset(payload_buf[tag.len..], '.');
    const p = try qe.Enqueue.encodeInto(out, .{
        .ns = NS.int(),
        .queue = queue,
        .priority = r.uintLessThan(u32, 4),
        // A few held back, so a priority's head is sometimes not due.
        .delay_ms = if (r.uintLessThan(u8, 5) == 0) r.uintLessThan(u64, 4_000) else 0,
        // Of varied length, so a deliver's byte budget stops it short.
        .payload = payload_buf[0 .. tag.len + r.uintLessThan(usize, 40)],
    });
    return .{ .entry_type = .queue_enqueue, .payload = p };
}

fn deliver(q: *families.Queues, out: []u8, l: Leader, r: std.Random, queue: []const u8, tag: []const u8) !?Made {
    // The leader proposes only when something is due by its clock.
    if (!anyDue(q, queue, l.now_ns)) return null;
    var hb: [24]u8 = undefined;
    const who = r.uintLessThan(u8, 8);
    const dead = who <= 2;
    // A dead consumer takes several at the full budget: its leases share
    // an expiry, so one expire names several live, due leases at once.
    const count: u32 = if (dead) r.intRangeAtMost(u32, 2, 5) else r.intRangeAtMost(u32, 1, 5);
    const p = try qe.Deliver.encodeInto(out, .{
        .ns = NS.int(),
        .queue = queue,
        .count = count,
        // Now and then a budget a few messages pass, as a large answer's
        // room does, or exactly what the first `count` take, so the budget's
        // edge is reached.
        .max_bytes = if (dead) 1 << 20 else switch (r.uintLessThan(u8, 8)) {
            0, 1 => r.intRangeAtMost(u32, 1, 100),
            2 => exactBudget(q, queue, l.now_ns, count),
            else => 1 << 20,
        },
        .holder = try std.fmt.bufPrint(&hb, "{s}{s}", .{ switch (who) {
            0, 1, 2 => DEAD,
            3 => SLOW,
            4 => ACK,
            else => "c",
        }, tag }),
        .lease_ms = r.intRangeAtMost(u64, 5_000, 8_000),
    });
    return .{ .entry_type = .queue_deliver, .payload = p };
}

/// What the first `count` due ready messages' payloads take, in the order a
/// deliver takes them: a budget they fit exactly.
fn exactBudget(q: *families.Queues, queue: []const u8, now: u64, count: u32) u32 {
    const Due = struct { priority: u32, due_ns: u64, seq: u64, size: u32 };
    var due: [MAX_HELD]Due = undefined;
    var n: usize = 0;
    var pb: [12]u8 = undefined;
    const hash = qm.hashOf(queue);
    var it = q.read().range(qm.Msgs, qm.storeKey(&pb, NS, &hash));
    while (it.next() catch null) |item| {
        const v = item.value.v;
        if (v.status != .ready or v.due_ns > now or n == due.len) continue;
        due[n] = .{ .priority = v.priority, .due_ns = v.due_ns, .seq = qm.seqOf(item.key[4..]), .size = @intCast(v.payload.len) };
        n += 1;
    }
    std.mem.sort(Due, due[0..n], {}, struct {
        fn less(_: void, x: Due, y: Due) bool {
            if (x.priority != y.priority) return x.priority < y.priority;
            if (x.due_ns != y.due_ns) return x.due_ns < y.due_ns;
            return x.seq < y.seq;
        }
    }.less);
    var sum: u32 = 0;
    for (due[0..@min(n, count)]) |d| sum += d.size;
    return @max(sum, 1);
}

fn anyDue(q: *families.Queues, queue: []const u8, now: u64) bool {
    var pb: [12]u8 = undefined;
    const hash = qm.hashOf(queue);
    var it = q.read().range(qm.Msgs, qm.storeKey(&pb, NS, &hash));
    while (it.next() catch null) |item| if (item.value.v.status == .ready and item.value.v.due_ns <= now) return true;
    return false;
}

/// A leased message of `queue` the leader holds, picked at random, and the
/// others leased under the same delivery.
const Picked = struct { seq: u64, msg: qm.Msg.Value };

fn pick(q: *families.Queues, r: std.Random, queue: []const u8, want: *const fn (qm.Msg.Value) bool) ?Picked {
    var n: u32 = 0;
    var chosen: ?Picked = null;
    var pb: [12]u8 = undefined;
    const hash = qm.hashOf(queue);
    var it = q.read().range(qm.Msgs, qm.storeKey(&pb, NS, &hash));
    while (it.next() catch null) |item| {
        if (!want(item.value.v)) continue;
        n += 1;
        if (r.uintLessThan(u32, n) != 0) continue;
        chosen = .{ .seq = qm.seqOf(item.key[4..]), .msg = item.value.v };
    }
    return chosen;
}

fn isReported(v: qm.Msg.Value) bool {
    return v.status == .leased and !std.mem.startsWith(u8, v.holder, DEAD) and !std.mem.startsWith(u8, v.holder, ACK);
}

fn isAcked(v: qm.Msg.Value) bool {
    return v.status == .leased and std.mem.startsWith(u8, v.holder, ACK);
}

/// The items of one delivery: `p`'s message and, now and then, the others
/// leased with it.
fn itemsOf(q: *families.Queues, buf: []u8, queue: []const u8, p: Picked, all: bool, deliveries_off: u32) !qe.Items.Value {
    var w: usize = 0;
    var n: u32 = 0;
    w += (try qe.Item.encodeInto(buf[w..], .{ .seq = p.seq, .deliveries = p.msg.deliveries -| deliveries_off })).len;
    n += 1;
    if (all) {
        var pb: [12]u8 = undefined;
        const hash = qm.hashOf(queue);
        var it = q.read().range(qm.Msgs, qm.storeKey(&pb, NS, &hash));
        while (it.next() catch null) |item| {
            const v = item.value.v;
            const seq = qm.seqOf(item.key[4..]);
            if (seq == p.seq or v.status != .leased or v.token.index != p.msg.token.index or v.token.term != p.msg.token.term) continue;
            if (n == qe.MAX_BATCH) break;
            w += (try qe.Item.encodeInto(buf[w..], .{ .seq = seq, .deliveries = v.deliveries })).len;
            n += 1;
        }
    }
    return .{ .bytes = buf[0..w], .count = n };
}

/// The settle of a delivery made with ack in this leader's term, if one is
/// still leased. The handler proposes it from the leader that delivered,
/// and a step-down drops it: a later leader never does.
fn ackSettle(q: *families.Queues, out: []u8, term: u64) !?Made {
    for (QUEUES) |queue| {
        // The first acked delivery of this term, by seq.
        var pb: [12]u8 = undefined;
        const hash = qm.hashOf(queue);
        var it = q.read().range(qm.Msgs, qm.storeKey(&pb, NS, &hash));
        const p: Picked = while (it.next() catch null) |item| {
            if (isAcked(item.value.v) and item.value.v.token.term == term) break .{ .seq = qm.seqOf(item.key[4..]), .msg = item.value.v };
        } else continue;
        var ib: [qe.Items.max_size]u8 = undefined;
        const items = try itemsOf(q, &ib, queue, p, true, 0);
        const payload = try qe.Settle.encodeInto(out, .{ .ns = NS.int(), .queue = queue, .holder = p.msg.holder, .token = p.msg.token, .expiry_ns = p.msg.expiry_ns, .items = items });
        return .{ .entry_type = .queue_settle, .payload = payload };
    }
    return null;
}

/// A settle, release or extend of a live delivery, as its consumer sends it;
/// spoiled, of a delivery that moved on, by another consumer, of a message
/// that's gone, or resent.
fn report(q: *families.Queues, out: []u8, kind: Kind, l: Leader, r: std.Random, queue: []const u8, spoil: bool) !?Made {
    const p = pick(q, r, queue, isReported) orelse return null;
    const v = p.msg;
    const lease_ns = v.lease_ms * std.time.ns_per_ms;
    if (!spoil) {
        if (kind == .extend and v.expiry_ns -| l.now_ns > lease_ns / 2) return null;
        if (kind != .extend and std.mem.startsWith(u8, v.holder, SLOW) and l.now_ns -| v.delivered_ns < lease_ns) return null;
    }
    var holder = v.holder;
    var token = v.token;
    var deliveries_off: u32 = 0;
    var gone = false;
    if (spoil) switch (r.uintLessThan(u8, 4)) {
        0 => token.index -|= 1,
        1 => deliveries_off = 1,
        2 => holder = "c-other",
        else => gone = true,
    };
    var ib: [qe.Items.max_size]u8 = undefined;
    var items = try itemsOf(q, &ib, queue, p, r.boolean(), deliveries_off);
    if (gone) {
        // A message never enqueued, or long since completed.
        var gb: [qe.Item.max_size]u8 = undefined;
        items = .{ .bytes = try qe.Item.encodeInto(&gb, .{ .seq = std.math.maxInt(u32), .deliveries = 1 }), .count = 1 };
        @memcpy(ib[0..items.bytes.len], items.bytes);
        items.bytes = ib[0..items.bytes.len];
    }
    const payload = switch (kind) {
        .settle => try qe.Settle.encodeInto(out, .{ .ns = NS.int(), .queue = queue, .holder = holder, .token = token, .expiry_ns = v.expiry_ns, .items = items }),
        .release => try qe.Release.encodeInto(out, .{
            .ns = NS.int(),
            .queue = queue,
            .holder = holder,
            .token = token,
            .expiry_ns = v.expiry_ns,
            .items = items,
            .delay_ms = if (r.boolean()) 0 else r.uintLessThan(u64, 3_000),
            .dead = r.uintLessThan(u8, 6) == 0,
            .@"error" = "boom",
        }),
        else => try qe.Extend.encodeInto(out, .{
            .ns = NS.int(),
            .queue = queue,
            .holder = holder,
            .token = token,
            // A leader that saw an older expiry: the lease's real key is
            // left behind, stale once it moves.
            .expiry_ns = if (r.uintLessThan(u8, 4) == 0) v.expiry_ns - 1 else v.expiry_ns,
            .held_since_ns = v.delivered_ns,
            .lease_ms = v.lease_ms,
            .items = items,
        }),
    };
    return .{ .entry_type = switch (kind) {
        .settle => .queue_settle,
        .release => .queue_release,
        else => .queue_extend,
    }, .payload = payload };
}

/// The duty's expire: deadline keys past due, a live lease's only after
/// the term's grace.
/// Spoiled, it names a live lease not yet due, which the machine leaves.
fn expire(q: *families.Queues, out: []u8, l: Leader, r: std.Random, spoil: bool) !?Made {
    var items: [ae.MAX_EXPIRE_ITEMS * qe.ExpireItem.max_size]u8 = undefined;
    var w: usize = 0;
    var n: u32 = 0;
    var live_named: u32 = 0;
    var it = qm.pastDue(q.read(), if (spoil) std.math.maxInt(u64) else l.now_ns);
    while (it.next()) |due| {
        if (n == ae.MAX_EXPIRE_ITEMS) break;
        const k = due.key;
        if (k.id.len != qe.ID_LEN) continue;
        const m = q.msg(k.ns, k.id[0..8].*, qm.seqOf(k.id));
        const live = if (m) |v| v.status == .leased and v.expiry_ns == k.expiry_ns else false;
        if (k.expiry_ns > l.now_ns) {
            // Not yet due: the duty stops here; a spoiled entry names the
            // first live one, which the machine must leave alone.
            if (spoil and live) {
                w += (try qe.ExpireItem.encodeInto(items[w..], .{ .ns = k.ns.int(), .id = k.id, .expiry_ns = k.expiry_ns })).len;
                n += 1;
            }
            if (!spoil or live) break;
            continue;
        }
        if (live) {
            const grace = @as(u64, @min(m.?.lease_ms, MAX_GRACE_MS)) * std.time.ns_per_ms;
            if (l.now_ns < l.term_since_ns +| grace) continue;
            live_named += 1;
        }
        w += (try qe.ExpireItem.encodeInto(items[w..], .{ .ns = k.ns.int(), .id = k.id, .expiry_ns = k.expiry_ns })).len;
        n += 1;
    }
    if (n == 0) return null;
    // Half the time the duty waits until it can end several live leases at
    // once, so an expire naming more than one is common.
    if (!spoil and live_named < 2 and r.boolean()) return null;
    const payload = try qe.Expire.encodeInto(out, .{ .items = .{ .bytes = items[0..w], .count = n } });
    return .{ .entry_type = .queue_expire, .payload = payload };
}

fn purge(out: []u8, queue: []const u8) !?Made {
    const p = try qe.Purge.encodeInto(out, .{ .ns = NS.int(), .queue = queue });
    return .{ .entry_type = .queue_purge, .payload = p };
}
