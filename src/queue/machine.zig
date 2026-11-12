//! Queues on the contract: a queue's messages are families, changed only
//! through a `View` in `apply`, so a refused entry changes nothing and
//! every replica holds the same messages, leases and dead letters.
//!
//! - `queue.meta`: each queue, keyed (ns id, name hash): its name, its next
//!   seq and its counts.
//! - `queue.msg`: each message, keyed (ns id, name hash, seq).
//! - `queue.dead`: each dead message, keyed (ns id, name hash, when it
//!   died, seq), so a queue's dead letters page oldest first.
//! - `queue.held`: one key, the bytes every queue on the shard
//!   holds, ready, leased and dead.
//! - `queue.ready`, `queue.deadline`, `queue.request`: the delivery model's
//!   (`contract/delivery.zig`). A queue's ready keys order by priority, then
//!   due time, then seq.
//!
//! A deliver names no messages: it leases the first due ready ones of its
//! queue as this replica holds them, which is what every replica holds. A
//! report names its messages and their delivery's token, and applies to
//! each it still holds; the rest are answered by what became of them.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const schema = @import("../protocol/schema.zig");
const ae = @import("../protocol/action_entries.zig");
const qe = @import("../protocol/queue_entries.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const router = @import("../node/router.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const View = view_mod.View;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;
const Stored = delivery.Stored;

pub const NamespaceId = router.NamespaceId;

pub const Meta = Record(&.{
    .{ .name = "name", .T = qe.QueueName },
    .{ .name = "next_seq", .T = Int(u64) },
    .{ .name = "ready", .T = Int(u64) },
    .{ .name = "leased", .T = Int(u64) },
    .{ .name = "dead", .T = Int(u64) },
    // What its messages hold, ready, leased and dead: each one's payload,
    // its error text and `PER_MESSAGE` (`heldBy`).
    .{ .name = "held_bytes", .T = Int(u64) },
    // Since the queue was first written to.
    .{ .name = "enqueued", .T = Int(u64) },
    .{ .name = "delivered", .T = Int(u64) },
    .{ .name = "settled", .T = Int(u64) },
    .{ .name = "released", .T = Int(u64) },
    .{ .name = "dead_lettered", .T = Int(u64) },
    .{ .name = "expired", .T = Int(u64) },
});

pub const Status = enum(u8) { ready = 0, leased = 1, dead = 2 };
pub const Ended = delivery.Ended;

pub const Msg = Record(&.{
    // Its queue's name hash: the scope of its ready and deadline keys.
    .{ .name = "queue", .T = Bytes(8) },
    .{ .name = "priority", .T = Int(u32) },
    .{ .name = "status", .T = schema.Enum(Status) },
    .{ .name = "payload", .T = Bytes(qe.MAX_MESSAGE) },
    .{ .name = "enqueued_ns", .T = Int(u64) },
    // Its ready key's due time, while ready.
    .{ .name = "due_ns", .T = Int(u64) },
    .{ .name = "deliveries", .T = Int(u32) },
    .{ .name = "failures", .T = Int(u32) },
    // The latest delivery: its holder, token, lease, when it began and
    // until when it runs.
    .{ .name = "holder", .T = ae.Holder },
    .{ .name = "request_key", .T = ae.RequestKey },
    .{ .name = "token", .T = ae.Token },
    .{ .name = "lease_ms", .T = Int(u64) },
    .{ .name = "delivered_ns", .T = Int(u64) },
    .{ .name = "expiry_ns", .T = Int(u64) },
    .{ .name = "ended", .T = schema.Enum(Ended) },
    .{ .name = "started_ns", .T = Int(u64) },
    .{ .name = "error", .T = Bytes(qe.MAX_ERROR) },
    // When it died, while dead: its dead key's time.
    .{ .name = "dead_ns", .T = Int(u64) },
});

const Empty = delivery.Empty;

/// A message's leases: the delivery model, keyed by namespace id.
const M = delivery.Model(.{
    .ready_id = 33,
    .ready_name = "queue.ready",
    .deadline_id = 34,
    .deadline_name = "queue.deadline",
    .request_id = 35,
    .request_name = "queue.request",
    .scope = "queue",
    .max_scope = 8,
    .max_id = qe.ID_LEN,
    .order = "priority",
    .noun = "message",
});

pub const Held = Record(&.{
    .{ .name = "bytes", .T = Int(u64) },
});

pub const Metas = view_mod.Family(30, "queue.meta", Stored(Meta));
pub const Msgs = view_mod.Family(31, "queue.msg", Stored(Msg));
pub const Dead = view_mod.Family(32, "queue.dead", Empty);
pub const Helds = view_mod.Family(36, "queue.held", Stored(Held));
/// `queue.held`'s one key, in no namespace.
pub const HELD_KEY = "\x00held";
pub const Ready = M.Ready;
pub const Deadlines = M.Deadlines;

pub const readyPrefix = M.readyPrefix;
pub const readyKeyOf = M.readyKeyOf;
pub const parseReadyKey = M.parseReadyKey;
pub const parseDeadlineKey = M.parseDeadlineKey;
pub const pastDue = M.pastDue;
pub const deadlineKey = M.deadlineKey;
const after = delivery.after;
const extendedTo = delivery.extendedTo;

/// A queue nothing was written to yet.
pub fn emptyMeta(name: []const u8) Meta.Value {
    return .{ .name = name, .next_seq = 1, .ready = 0, .leased = 0, .dead = 0, .held_bytes = 0, .enqueued = 0, .delivered = 0, .settled = 0, .released = 0, .dead_lettered = 0, .expired = 0 };
}

// ─── Keys ────────────────────────────────────────────────────────────────

/// A queue's name hash, as its keys lead with it.
pub fn hashOf(name: []const u8) [8]u8 {
    const k = router.nsNameKey(.invalid, name);
    return k[4..12].*;
}

/// A message's id in its namespace: its queue's name hash, then its seq.
pub fn idOf(hash: [8]u8, seq: u64) [qe.ID_LEN]u8 {
    var id: [qe.ID_LEN]u8 = undefined;
    id[0..8].* = hash;
    std.mem.writeInt(u64, id[8..16], seq, .big);
    return id;
}

pub fn seqOf(id: []const u8) u64 {
    return std.mem.readInt(u64, id[8..16], .big);
}

/// `[hash][died:u64 BE][seq:u64 BE]`: a queue's dead letters, oldest first.
pub fn deadKey(hash: [8]u8, dead_ns: u64, seq: u64) [24]u8 {
    var k: [24]u8 = undefined;
    k[0..8].* = hash;
    std.mem.writeInt(u64, k[8..16], dead_ns, .big);
    std.mem.writeInt(u64, k[16..24], seq, .big);
    return k;
}

/// A stored ready key's priority level: its namespace's id, its queue,
/// the NUL and its priority.
pub const LEVEL_LEN = 4 + 8 + 1 + 4;

/// A key past every ready key of `key`'s priority level, and before the
/// next level's: a picker seeks past a level whose head isn't due.
pub fn pastLevel(arena: std.mem.Allocator, key: []const u8) error{OutOfMemory}![]const u8 {
    return pastLevelInto(try arena.create([PAST_LEVEL_LEN]u8), key);
}

pub const PAST_LEVEL_LEN = LEVEL_LEN + 8 + qe.ID_LEN + 1;

pub fn pastLevelInto(buf: *[PAST_LEVEL_LEN]u8, key: []const u8) []const u8 {
    @memcpy(buf[0..LEVEL_LEN], key[0..LEVEL_LEN]);
    @memset(buf[LEVEL_LEN..], 0xff);
    return buf;
}

/// A deadline key's prefix: its expiry, its namespace's id, its queue.
fn deadlinePrefix(buf: *[20]u8, expiry_ns: u64, ns: NamespaceId, hash: [8]u8) []const u8 {
    std.mem.writeInt(u64, buf[0..8], expiry_ns, .big);
    buf[8..12].* = ns.bytes();
    buf[12..20].* = hash;
    return buf;
}

fn typeOf(e: *const Entry) EntryType {
    return @fromBackingInt(@intCast(e.header.entry_type));
}

/// A queue's own keys in every family an entry may change at will:
/// its meta, its messages, its ready and dead keys.
fn queueKeys(out: *KeySet, ns: NamespaceId, hash: [8]u8) error{OutOfMemory}!void {
    out.addIn(Metas.ID, ns, &hash) catch |e| return e;
    var pb: [9]u8 = undefined;
    inline for (.{ Msgs.ID, Dead.ID }) |fam| out.addRangeIn(fam, ns, &hash) catch |e| switch (e) {
        error.NoNamespace => return,
        else => |o| return o,
    };
    out.addRangeIn(Ready.ID, ns, readyPrefix(&pb, &hash)) catch |e| switch (e) {
        error.NoNamespace => return,
        else => |o| return o,
    };
}

/// The deadline keys a report's lease holds at `expiry_ns`.
fn leaseKeys(out: *KeySet, ns: NamespaceId, hash: [8]u8, expiry_ns: u64) error{OutOfMemory}!void {
    var b: [20]u8 = undefined;
    out.addRangeNaming(Deadlines.ID, 8, deadlinePrefix(&b, expiry_ns, ns, hash)) catch |e| switch (e) {
        error.NoNamespace => return,
        else => |o| return o,
    };
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    var diag: schema.Diagnostic = .{};
    const stamp = e.header.timestamp_ns;
    switch (typeOf(e)) {
        .queue_enqueue => {
            const p = qe.Enqueue.decodeAll(e.payload, &diag) catch return;
            try queueKeys(out, .of(p.ns), hashOf(p.queue));
            try out.add(Helds.ID, "", HELD_KEY[1..]);
        },
        .queue_deliver => {
            const p = qe.Deliver.decodeAll(e.payload, &diag) catch return;
            const hash = hashOf(p.queue);
            try queueKeys(out, .of(p.ns), hash);
            try leaseKeys(out, .of(p.ns), hash, after(stamp, p.lease_ms));
        },
        .queue_settle => {
            const p = qe.Settle.decodeAll(e.payload, &diag) catch return;
            const hash = hashOf(p.queue);
            try queueKeys(out, .of(p.ns), hash);
            try leaseKeys(out, .of(p.ns), hash, p.expiry_ns);
            try out.add(Helds.ID, "", HELD_KEY[1..]);
        },
        .queue_release => {
            const p = qe.Release.decodeAll(e.payload, &diag) catch return;
            const hash = hashOf(p.queue);
            try queueKeys(out, .of(p.ns), hash);
            try leaseKeys(out, .of(p.ns), hash, p.expiry_ns);
            try out.add(Helds.ID, "", HELD_KEY[1..]);
        },
        .queue_extend => {
            const p = qe.Extend.decodeAll(e.payload, &diag) catch return;
            const hash = hashOf(p.queue);
            try queueKeys(out, .of(p.ns), hash);
            try leaseKeys(out, .of(p.ns), hash, p.expiry_ns);
            try leaseKeys(out, .of(p.ns), hash, extendedTo(stamp, p.lease_ms, p.held_since_ns));
        },
        .queue_expire => {
            const p = qe.Expire.decodeAll(e.payload, &diag) catch return;
            try out.add(Helds.ID, "", HELD_KEY[1..]);
            var it = p.items.iterator();
            while (it.next() catch return) |item| {
                if (item.id.len != qe.ID_LEN) continue;
                const hash = item.id[0..8].*;
                try queueKeys(out, .of(item.ns), hash);
                try leaseKeys(out, .of(item.ns), hash, item.expiry_ns);
            }
        },
        .queue_purge => {
            const p = qe.Purge.decodeAll(e.payload, &diag) catch return;
            try queueKeys(out, .of(p.ns), hashOf(p.queue));
            try out.add(Helds.ID, "", HELD_KEY[1..]);
        },
        else => {},
    }
}

// ─── Apply ───────────────────────────────────────────────────────────────

pub const NO_QUEUE = "not found: no such queue";
/// An enqueue past its queue's caps, or its shard's.
pub const FULL = "overloaded: queue '{s}' is full: it holds {d} messages (at most {d}) and {d} held bytes (at most {d}), ready, leased and dead; consume or purge it";
pub const SHARD_FULL = "overloaded: this shard's queues and groups hold {d} held bytes, at most {d}; consume or purge some";

/// What a message holds against the caps.
pub fn heldBy(payload_len: usize, error_len: usize) u64 {
    return payload_len + error_len + qe.PER_MESSAGE;
}

/// The bytes every queue on the shard holds (`heldBy`).
pub fn heldOf(view: *View) ApplyError!u64 {
    return if (try view.get(Helds, HELD_KEY)) |h| h.v.bytes else 0;
}

/// Take `size` bytes off the shard's held bytes.
fn unhold(view: *View, size: u64) ApplyError!void {
    const held = try heldOf(view);
    if (size > held) return error.Halt;
    try view.put(Helds, HELD_KEY, .{ .v = .{ .bytes = held - size } });
}
pub const NAME_TAKEN = "conflict: another queue's name hashes as this one's; choose another name";

/// The most ready keys one deliver looks at: past it, a deliver takes what
/// it found. A priority whose head isn't due is passed in one seek, so this
/// bounds the seeks too: one per priority level, of which a u8 priority
/// (`proto.Request.getPriority`) gives 256. A wider priority would let
/// delayed levels use up the looks before a due one.
pub const MAX_LOOKED_AT: usize = 256;

/// Why a report applied nothing to one of its messages.
pub const Why = @import("../protocol/queue_ops.zig").Why;

fn why(m: Msg.Value, holder: []const u8, deliveries: u32, token: ae.Token.Value) Why {
    if (m.deliveries != deliveries or m.token.term != token.term or m.token.index != token.index) return .delivered_again;
    if (!std.mem.eql(u8, m.holder, holder)) return .other_holder;
    return switch (m.ended) {
        .expired => .expired,
        .released, .abandoned => .released,
        else => .not_held,
    };
}

/// Answers: enqueue, the message's seq (u64); deliver, how many it leased
/// (u32) then each one's seq (u64); settle, release and extend, how many
/// they applied to (u32), extend then the expiry it leaves (u64), and then
/// each message they didn't (seq u64, `Why` u8); expire, the leases it
/// ended (u32); purge, the messages it removed (u64).
fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    const stamp = e.header.timestamp_ns;
    switch (typeOf(e)) {
        .queue_enqueue => {
            const p = qe.Enqueue.decodeAll(e.payload, &diag) catch return error.Halt;
            const ns: NamespaceId = .of(p.ns);
            if (ns == .invalid) return error.Halt;
            const hash = hashOf(p.queue);
            const mk = try M.inNs(view, ns, &hash);
            var meta: Meta.Value = if (try view.get(Metas, mk)) |m| m.v else emptyMeta(p.queue);
            if (!std.mem.eql(u8, meta.name, p.queue)) return out.refuse(.conflict, .unclassified, NAME_TAKEN);
            const size = heldBy(p.payload.len, 0);
            const held = meta.ready + meta.leased + meta.dead;
            if (held + 1 > p.max_messages or meta.held_bytes + size > p.max_bytes)
                return out.refuseFmt(.overloaded, .queue_full, FULL, .{ p.queue, held, p.max_messages, meta.held_bytes, p.max_bytes });
            var shard_held = try heldOf(view);
            if (shard_held + size > p.shard_held_bytes)
                return out.refuseFmt(.overloaded, .queue_full, SHARD_FULL, .{ shard_held, p.shard_held_bytes });
            shard_held += size;
            try view.put(Helds, HELD_KEY, .{ .v = .{ .bytes = shard_held } });
            const seq = meta.next_seq;
            meta.next_seq += 1;
            meta.ready += 1;
            meta.held_bytes += size;
            meta.enqueued += 1;
            const id = idOf(hash, seq);
            const msg: Msg.Value = .{
                .queue = &hash,
                .priority = p.priority,
                .status = .ready,
                .payload = p.payload,
                .enqueued_ns = stamp,
                .due_ns = after(stamp, p.delay_ms),
                .deliveries = 0,
                .failures = 0,
                .holder = "",
                .request_key = null,
                .token = .{ .term = 0, .index = 0 },
                .lease_ms = 0,
                .delivered_ns = 0,
                .expiry_ns = 0,
                .ended = .none,
                .started_ns = 0,
                .@"error" = "",
                .dead_ns = 0,
            };
            try view.put(Msgs, try M.inNs(view, ns, &id), .{ .v = msg });
            var rb: [M.MAX_KEY]u8 = undefined;
            try view.put(Ready, try M.inNs(view, ns, M.readyKeyOf(&rb, msg, msg.due_ns, &id)), .{});
            try view.put(Metas, mk, .{ .v = meta });
            out.putInt(u64, seq);
        },
        .queue_deliver => {
            const p = qe.Deliver.decodeAll(e.payload, &diag) catch return error.Halt;
            const ns: NamespaceId = .of(p.ns);
            const hash = hashOf(p.queue);
            const mk = try M.inNs(view, ns, &hash);
            var meta = (try view.get(Metas, mk)) orelse return out.putInt(u32, 0);
            if (!std.mem.eql(u8, meta.v.name, p.queue)) return out.putInt(u32, 0);
            // The first due ready keys, read before any is taken: a deliver
            // moves each it takes out of this range. A priority whose first
            // key isn't due yet holds none that is: one seek passes it.
            var chosen: [qe.MAX_BATCH][qe.ID_LEN]u8 = undefined;
            var n: usize = 0;
            var bytes: u64 = 0;
            var pb: [9]u8 = undefined;
            const prefix = try M.inNs(view, ns, readyPrefix(&pb, &hash));
            var looked: usize = 0;
            var past: ?[]const u8 = null;
            pick: while (true) {
                var it = if (past) |a| try view.rangeAfter(Ready, prefix, a) else try view.range(Ready, prefix);
                while (try it.next()) |item| {
                    if (n == p.count or looked == MAX_LOOKED_AT) break :pick;
                    looked += 1;
                    const k = parseReadyKey(item.key[4..], 8) orelse return error.Halt;
                    if (k.due_ns > stamp) {
                        past = try pastLevel(view.arena, item.key);
                        continue :pick;
                    }
                    if (k.id.len != qe.ID_LEN) return error.Halt;
                    const msg = (try view.get(Msgs, try M.inNs(view, ns, k.id))) orelse return error.Halt;
                    if (n > 0 and bytes + msg.v.payload.len > p.max_bytes) break :pick;
                    bytes += msg.v.payload.len;
                    chosen[n] = k.id[0..qe.ID_LEN].*;
                    n += 1;
                }
                break;
            }
            const token: ae.Token.Value = .{ .term = e.header.term, .index = e.header.index };
            out.putInt(u32, @intCast(n));
            for (chosen[0..n]) |*id| {
                const key = try M.inNs(view, ns, id);
                var msg = (try view.get(Msgs, key)).?;
                try M.deliver(view, ns, id, &msg.v, p.holder, null, token, p.lease_ms, stamp);
                try view.put(Msgs, key, msg);
                out.putInt(u64, seqOf(id));
            }
            meta.v.ready -= n;
            meta.v.leased += n;
            meta.v.delivered += n;
            try view.put(Metas, mk, meta);
        },
        .queue_settle => {
            const p = qe.Settle.decodeAll(e.payload, &diag) catch return error.Halt;
            try report(view, out, .of(p.ns), p.queue, p.holder, p.token, p.items, .{ .settle = {} }, p.expiry_ns, stamp);
        },
        .queue_release => {
            const p = qe.Release.decodeAll(e.payload, &diag) catch return error.Halt;
            try report(view, out, .of(p.ns), p.queue, p.holder, p.token, p.items, .{ .release = .{ .delay_ms = p.delay_ms, .dead = p.dead, .@"error" = p.@"error" } }, p.expiry_ns, stamp);
        },
        .queue_extend => {
            const p = qe.Extend.decodeAll(e.payload, &diag) catch return error.Halt;
            try report(view, out, .of(p.ns), p.queue, p.holder, p.token, p.items, .{ .extend = .{ .lease_ms = p.lease_ms, .held_since_ns = p.held_since_ns } }, p.expiry_ns, stamp);
        },
        .queue_expire => {
            const p = qe.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
            var it = p.items.iterator();
            var ended: u32 = 0;
            while (it.next() catch return error.Halt) |item| {
                if (try expireOne(view, item, stamp)) ended += 1;
            }
            out.putInt(u32, ended);
        },
        .queue_purge => {
            const p = qe.Purge.decodeAll(e.payload, &diag) catch return error.Halt;
            const ns: NamespaceId = .of(p.ns);
            const hash = hashOf(p.queue);
            const mk = try M.inNs(view, ns, &hash);
            var meta = (try view.get(Metas, mk)) orelse return out.putInt(u64, 0);
            if (!std.mem.eql(u8, meta.v.name, p.queue)) return out.putInt(u64, 0);
            var removed: u64 = 0;
            // A leased message's deadline key stays: the expire duty drops it
            // once past due, finding no message.
            inline for (.{ Msgs, Ready, Dead }) |F| {
                var pb: [9]u8 = undefined;
                const prefix = if (F == Ready) readyPrefix(&pb, &hash) else &hash;
                var it = try view.range(F, try M.inNs(view, ns, prefix));
                while (try it.next()) |item| {
                    try view.delete(F, item.key);
                    if (F == Msgs) removed += 1;
                }
            }
            try unhold(view, meta.v.held_bytes);
            meta.v.ready = 0;
            meta.v.leased = 0;
            meta.v.dead = 0;
            meta.v.held_bytes = 0;
            try view.put(Metas, mk, meta);
            out.putInt(u64, removed);
        },
        else => return error.Halt,
    }
}

const Report = union(enum) {
    settle,
    release: struct { delay_ms: u64, dead: bool, @"error": []const u8 },
    extend: struct { lease_ms: u64, held_since_ns: u64 },
};

/// A report's messages, each applied to on its own: one it holds changes,
/// and one it doesn't is answered by what became of it. A resent release
/// whose first applied counts as applied; a resent settle finds its
/// message gone, as the settle removed it, and is answered gone.
fn report(view: *View, out: *Result, ns: NamespaceId, queue: []const u8, holder: []const u8, token: ae.Token.Value, items: qe.Items.Value, kind: Report, seen_expiry: u64, stamp: u64) ApplyError!void {
    const hash = hashOf(queue);
    const mk = try M.inNs(view, ns, &hash);
    const found = try view.get(Metas, mk);
    var meta = if (found) |m| (if (std.mem.eql(u8, m.v.name, queue)) m else null) else null;
    var applied: u32 = 0;
    var to: u64 = 0;
    // Refused items are said after the count: room for each, then filled.
    var refused: [qe.MAX_BATCH]struct { seq: u64, why: Why } = undefined;
    var nr: usize = 0;
    var it = items.iterator();
    while (it.next() catch return error.Halt) |item| {
        const id = idOf(hash, item.seq);
        const key = try M.inNs(view, ns, &id);
        var msg = (if (meta != null) try view.get(Msgs, key) else null) orelse {
            refused[nr] = .{ .seq = item.seq, .why = .gone };
            nr += 1;
            continue;
        };
        const held = delivery.holds(msg.v, holder, item.deliveries, token);
        if (!held) {
            const same = delivery.sameDelivery(msg.v, holder, item.deliveries, token);
            const repeat = same and switch (kind) {
                .settle => false,
                .release => msg.v.ended == .released,
                .extend => false,
            };
            if (repeat) {
                applied += 1;
                continue;
            }
            refused[nr] = .{ .seq = item.seq, .why = why(msg.v, holder, item.deliveries, token) };
            nr += 1;
            continue;
        }
        const m = &meta.?.v;
        switch (kind) {
            .settle => {
                try M.endLease(view, ns, &id, msg.v, holder, null, seen_expiry);
                try view.delete(Msgs, key);
                const size = heldBy(msg.v.payload.len, msg.v.@"error".len);
                if (size > m.held_bytes) return error.Halt;
                m.held_bytes -= size;
                try unhold(view, size);
                m.leased -= 1;
                m.settled += 1;
            },
            .release => |r| {
                try M.endLease(view, ns, &id, msg.v, holder, null, seen_expiry);
                msg.v.ended = .released;
                msg.v.failures +|= 1;
                try reholdError(view, m, msg.v.@"error".len, r.@"error".len);
                msg.v.@"error" = r.@"error";
                m.leased -= 1;
                m.released += 1;
                if (r.dead or msg.v.failures >= qe.MAX_ATTEMPTS) {
                    try die(view, ns, &id, &msg.v, m, stamp);
                } else {
                    try M.readyAgain(view, ns, &id, &msg.v, after(stamp, r.delay_ms));
                    m.ready += 1;
                }
                try view.put(Msgs, key, msg);
            },
            .extend => |x| {
                if (x.lease_ms != msg.v.lease_ms or x.held_since_ns != msg.v.delivered_ns) {
                    refused[nr] = .{ .seq = item.seq, .why = .delivered_again };
                    nr += 1;
                    continue;
                }
                if (try M.extend(view, ns, &id, &msg.v, seen_expiry, stamp)) |t| {
                    to = t;
                    try view.put(Msgs, key, msg);
                } else to = msg.v.expiry_ns;
            },
        }
        applied += 1;
    }
    if (meta) |m| if (applied > 0) try view.put(Metas, mk, m);
    out.putInt(u32, applied);
    if (kind == .extend) out.putInt(u64, to);
    for (refused[0..nr]) |r| {
        out.putInt(u64, r.seq);
        out.putInt(u8, @backingInt(r.why));
    }
}

/// `msg` dead at `stamp`, out of its queue's ready messages for good. It
/// stays held: a full queue refuses enqueues until its dead go.
fn die(view: *View, ns: NamespaceId, id: []const u8, msg: *Msg.Value, meta: *Meta.Value, stamp: u64) ApplyError!void {
    const hash = id[0..8].*;
    msg.status = .dead;
    msg.dead_ns = stamp;
    const dk = deadKey(hash, stamp, seqOf(id));
    try view.put(Dead, try M.inNs(view, ns, &dk), .{});
    meta.dead += 1;
    meta.dead_lettered += 1;
}

const EXPIRED = "lease expired";

/// A message's error text replaced: what it holds moves by the difference,
/// its queue's and its shard's. Never refused: an error is at most
/// `qe.MAX_ERROR` bytes.
fn reholdError(view: *View, meta: *Meta.Value, old_len: usize, new_len: usize) ApplyError!void {
    if (new_len >= old_len) {
        const more = new_len - old_len;
        meta.held_bytes += more;
        try view.put(Helds, HELD_KEY, .{ .v = .{ .bytes = (try heldOf(view)) + more } });
    } else {
        const less = old_len - new_len;
        if (less > meta.held_bytes) return error.Halt;
        meta.held_bytes -= less;
        try unhold(view, less);
    }
}

/// One expire item, past due by the log's clock: its key goes, and if it
/// is the message's live deadline, the lease ends as a failure.
fn expireOne(view: *View, item: qe.ExpireItem.Value, stamp: u64) ApplyError!bool {
    if (item.expiry_ns > stamp or item.id.len != qe.ID_LEN) return false;
    const ns: NamespaceId = .of(item.ns);
    const hash = item.id[0..8].*;
    const key = try M.inNs(view, ns, item.id);
    const found = try view.get(Msgs, key);
    if (!try M.expireKey(view, ns, item.id, &hash, item.expiry_ns, if (found) |f| f.v else null, stamp)) return false;
    var msg = found.?;
    const mk = try M.inNs(view, ns, &hash);
    var meta = (try view.get(Metas, mk)) orelse return error.Halt;
    msg.v.ended = .expired;
    msg.v.failures +|= 1;
    try reholdError(view, &meta.v, msg.v.@"error".len, EXPIRED.len);
    msg.v.@"error" = EXPIRED;
    meta.v.leased -= 1;
    meta.v.expired += 1;
    if (msg.v.failures >= qe.MAX_ATTEMPTS) {
        try die(view, ns, item.id, &msg.v, &meta.v, stamp);
    } else {
        try M.readyAgain(view, ns, item.id, &msg.v, stamp);
        meta.v.ready += 1;
    }
    try view.put(Msgs, key, msg);
    try view.put(Metas, mk, meta);
    return true;
}

pub const machine: machine_mod.Machine = .{
    .name = "queues",
    .entry_types = &.{ .queue_enqueue, .queue_deliver, .queue_settle, .queue_release, .queue_extend, .queue_expire, .queue_purge },
    .families = &.{ machine_mod.info(Metas), machine_mod.info(Msgs), machine_mod.info(Dead), machine_mod.info(Ready), machine_mod.info(Deadlines), machine_mod.info(Helds) },
    .keys = keys,
    .apply = apply,
};

// ─── Reads ───────────────────────────────────────────────────────────────

/// `k` in `ns`, as these families store it.
pub fn storeKey(buf: []u8, ns: NamespaceId, k: []const u8) []const u8 {
    return view_mod.idKey(buf, ns, k) catch unreachable;
}

// ─── Tests ───────────────────────────────────────────────────────────────

/// Queue state on its own, for tests: entries applied through the machine
/// as a shard applies them, one index after another, and kept as a log
/// another store can replay.
pub const TestQueues = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    log: std.ArrayListUnmanaged(Logged) = .empty,
    index: u64 = 0,
    term: u64 = 1,

    const store_mod = @import("../contract/store.zig");

    const Logged = struct { etype: EntryType, term: u64, index: u64, stamp: u64, payload: []u8 };

    pub fn init(allocator: std.mem.Allocator) TestQueues {
        var store = store_mod.Store.init(allocator);
        store.strict_keys = true;
        return .{ .store = store, .scratch = std.heap.ArenaAllocator.init(allocator) };
    }

    pub fn deinit(self: *TestQueues) void {
        for (self.log.items) |l| self.store.allocator.free(l.payload);
        self.log.deinit(self.store.allocator);
        self.store.deinit();
        self.scratch.deinit();
    }

    /// `body` as the next entry of type `etype`, stamped `stamp_ns`.
    pub fn apply(self: *TestQueues, comptime etype: EntryType, body: anytype, stamp_ns: u64) !Result {
        const P = qe.Payload(@backingInt(etype));
        const payload = try self.store.allocator.alloc(u8, try P.encodedSize(body));
        _ = try P.encodeInto(payload, body);
        self.index += 1;
        try self.log.append(self.store.allocator, .{ .etype = etype, .term = self.term, .index = self.index, .stamp = stamp_ns, .payload = payload });
        return self.applyLogged(self.log.items[self.log.items.len - 1]);
    }

    fn applyLogged(self: *TestQueues, l: Logged) !Result {
        const e = entry_mod.buildEntry(l.etype, entry_mod.Flags.NONE, l.term, l.index, l.stamp, l.payload);
        var out: Result = .{};
        out.reset(&e);
        try machine_mod.applyEntry(&machine, &self.store, &self.scratch, &e, &out);
        return out;
    }

    pub fn replayInto(self: *TestQueues, other: *TestQueues) !void {
        for (self.log.items) |l| _ = try other.applyLogged(l);
    }

    fn read(self: *TestQueues) view_mod.ReadView {
        return .{ .store = &self.store };
    }

    pub fn msg(self: *TestQueues, ns: NamespaceId, queue: []const u8, seq: u64) ?Msg.Value {
        var b: [4 + qe.ID_LEN]u8 = undefined;
        const id = idOf(hashOf(queue), seq);
        const got = self.read().get(Msgs, storeKey(&b, ns, &id)) catch unreachable;
        return if (got) |m| m.v else null;
    }

    pub fn meta(self: *TestQueues, ns: NamespaceId, queue: []const u8) ?Meta.Value {
        var b: [12]u8 = undefined;
        const hash = hashOf(queue);
        const got = self.read().get(Metas, storeKey(&b, ns, &hash)) catch unreachable;
        return if (got) |m| m.v else null;
    }

    pub fn count(self: *TestQueues, comptime F: type) usize {
        var n: usize = 0;
        var it = self.store.range(F.ID, "");
        while (it.next()) |_| n += 1;
        return n;
    }

    pub fn sums(self: *TestQueues) [6]u64 {
        var out: [6]u64 = undefined;
        inline for (.{ Metas, Msgs, Dead, Ready, Deadlines, Helds }, 0..) |F, i| out[i] = store_mod.checksum(&self.store, F.ID);
        return out;
    }

    /// The expire duty's tick at `stamp`: one expire naming every key past
    /// due, as the leader proposes it.
    pub fn duty(self: *TestQueues, stamp: u64) !Result {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        defer buf.deinit(self.store.allocator);
        var n: u32 = 0;
        var it = pastDue(self.read(), stamp);
        while (it.next()) |d| {
            var ib: [qe.ExpireItem.max_size]u8 = undefined;
            try buf.appendSlice(self.store.allocator, try qe.ExpireItem.encodeInto(&ib, .{ .ns = d.key.ns.int(), .id = d.key.id, .expiry_ns = d.key.expiry_ns }));
            n += 1;
        }
        return self.apply(.queue_expire, qe.Expire.Value{ .items = .{ .bytes = buf.items, .count = n } }, stamp);
    }
};

const testing = std.testing;
const proto_status = @import("../protocol/proto.zig").StatusCode;
const ms = std.time.ns_per_ms;
const LEASE_MS = ae.MIN_LEASE_MS;
const LEASE = LEASE_MS * ms;
const NS: NamespaceId = .default;

/// An enqueue at the default caps.
pub fn enqueueValue(ns: NamespaceId, queue: []const u8, priority: u32, payload: []const u8) qe.Enqueue.Value {
    return .{ .ns = ns.int(), .queue = queue, .priority = priority, .delay_ms = 0, .payload = payload, .max_messages = qe.DEFAULT_MAX_MESSAGES, .max_bytes = qe.DEFAULT_MAX_BYTES, .shard_held_bytes = qe.DEFAULT_SHARD_HELD_BYTES };
}

fn enqueue(t: *TestQueues, queue: []const u8, priority: u32, payload: []const u8, stamp: u64) !u64 {
    const r = try t.apply(.queue_enqueue, enqueueValue(NS, queue, priority, payload), stamp);
    try testing.expect(r.refused() == null);
    var rd = r.reader();
    return rd.int(u64).?;
}

/// The seqs a deliver of `count` from "q" leased at `stamp`.
fn deliver(t: *TestQueues, count: u32, stamp: u64, out: []u64) !usize {
    const r = try t.apply(.queue_deliver, qe.Deliver.Value{ .ns = NS.int(), .queue = "q", .count = count, .max_bytes = 1 << 20, .holder = "c1", .lease_ms = LEASE_MS }, stamp);
    try testing.expect(r.refused() == null);
    var rd = r.reader();
    const n = rd.int(u32).?;
    for (out[0..n]) |*s| s.* = rd.int(u64).?;
    return n;
}

fn itemsOf(buf: []u8, t: *TestQueues, seqs: []const u64) !qe.Items.Value {
    var w: usize = 0;
    for (seqs) |s| {
        const m = t.msg(NS, "q", s).?;
        w += (try qe.Item.encodeInto(buf[w..], .{ .seq = s, .deliveries = m.deliveries })).len;
    }
    return .{ .bytes = buf[0..w], .count = @intCast(seqs.len) };
}

/// A report's answer: how many applied, then each refused (seq, why).
const Answer = struct {
    applied: u32,
    refused: [qe.MAX_BATCH]struct { seq: u64, why: Why } = undefined,
    n: usize = 0,

    fn of(r: Result, extend: bool) Answer {
        var rd = r.reader();
        var a: Answer = .{ .applied = rd.int(u32).? };
        if (extend) _ = rd.int(u64).?;
        while (rd.int(u64)) |s| {
            a.refused[a.n] = .{ .seq = s, .why = @fromBackingInt(@intCast(rd.int(u8).?)) };
            a.n += 1;
        }
        return a;
    }
};

fn settle(t: *TestQueues, seqs: []const u64, stamp: u64) !Answer {
    var b: [qe.Items.max_size]u8 = undefined;
    const m = t.msg(NS, "q", seqs[0]).?;
    const r = try t.apply(.queue_settle, qe.Settle.Value{ .ns = NS.int(), .queue = "q", .holder = "c1", .token = m.token, .expiry_ns = m.expiry_ns, .items = try itemsOf(&b, t, seqs) }, stamp);
    return Answer.of(r, false);
}

fn release(t: *TestQueues, seqs: []const u64, dead: bool, stamp: u64) !Answer {
    var b: [qe.Items.max_size]u8 = undefined;
    const m = t.msg(NS, "q", seqs[0]).?;
    const r = try t.apply(.queue_release, qe.Release.Value{ .ns = NS.int(), .queue = "q", .holder = "c1", .token = m.token, .expiry_ns = m.expiry_ns, .items = try itemsOf(&b, t, seqs), .delay_ms = 0, .dead = dead, .@"error" = "boom" }, stamp);
    return Answer.of(r, false);
}

test "queue machine: a deliver leases the first due ready messages, by priority then due time then seq" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    const a = try enqueue(&t, "q", 5, "a", 10);
    const b = try enqueue(&t, "q", 0, "b", 20);
    const c = try enqueue(&t, "q", 0, "c", 30);
    const d = try enqueue(&t, "q", 0, "d", 40);
    var got: [4]u64 = undefined;
    try testing.expectEqual(@as(usize, 1), try deliver(&t, 1, 50, &got));
    try testing.expectEqual(b, got[0]);
    // b back, but not due for a second: c, d, then a go first.
    var ib: [qe.Items.max_size]u8 = undefined;
    const mb = t.msg(NS, "q", b).?;
    _ = try t.apply(.queue_release, qe.Release.Value{ .ns = NS.int(), .queue = "q", .holder = "c1", .token = mb.token, .expiry_ns = mb.expiry_ns, .items = try itemsOf(&ib, &t, &.{b}), .delay_ms = 1000, .dead = false, .@"error" = "" }, 60);
    const n = try deliver(&t, 4, 100, &got);
    try testing.expectEqualSlices(u64, &.{ c, d, a }, got[0..n]);
    const m = t.msg(NS, "q", c).?;
    try testing.expectEqual(Status.leased, m.status);
    try testing.expectEqual(@as(u32, 1), m.deliveries);
    try testing.expectEqual(@as(u64, 100 + LEASE), m.expiry_ns);
    const meta = t.meta(NS, "q").?;
    try testing.expectEqual(@as(u64, 1), meta.ready);
    try testing.expectEqual(@as(u64, 3), meta.leased);
    // Once due, b goes; then the queue has nothing ready, and isn't refused.
    try testing.expectEqual(@as(usize, 1), try deliver(&t, 4, 60 + 1000 * ms, &got));
    try testing.expectEqual(b, got[0]);
    try testing.expectEqual(@as(usize, 0), try deliver(&t, 4, 60 + 1000 * ms, &got));
}

test "queue machine: a delayed message at the head of its priority holds back only that priority" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    const urgent = try enqueue(&t, "q", 0, "urgent", 10);
    var lower: [3]u64 = undefined;
    for (&lower, 0..) |*seq, i| seq.* = try enqueue(&t, "q", 1, "lower", 20 + i);
    var got: [4]u64 = undefined;
    _ = try deliver(&t, 1, 50, &got);
    try testing.expectEqual(urgent, got[0]);
    // Back at priority 0, not due for a minute: priority 1 still goes, in
    // seq order.
    var ib: [qe.Items.max_size]u8 = undefined;
    const m = t.msg(NS, "q", urgent).?;
    _ = try t.apply(.queue_release, qe.Release.Value{ .ns = NS.int(), .queue = "q", .holder = "c1", .token = m.token, .expiry_ns = m.expiry_ns, .items = try itemsOf(&ib, &t, &.{urgent}), .delay_ms = 60_000, .dead = false, .@"error" = "" }, 60);
    const n = try deliver(&t, 4, 100, &got);
    try testing.expectEqualSlices(u64, &lower, got[0..n]);
}

test "queue machine: a priority of delayed messages longer than a deliver looks at is passed in one seek" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    for (0..300) |i| _ = try enqueue(&t, "q", 0, "later", 10 + i);
    // Each taken, then released for a minute.
    var stamp: u64 = 1000;
    for (0..3) |_| {
        var got: [100]u64 = undefined;
        const n = try deliver(&t, 100, stamp, &got);
        var ib: [qe.Items.max_size]u8 = undefined;
        const m = t.msg(NS, "q", got[0]).?;
        _ = try t.apply(.queue_release, qe.Release.Value{ .ns = NS.int(), .queue = "q", .holder = "c1", .token = m.token, .expiry_ns = m.expiry_ns, .items = try itemsOf(&ib, &t, got[0..n]), .delay_ms = 60_000, .dead = false, .@"error" = "" }, stamp + 1);
        stamp += 10;
    }
    const now_due = try enqueue(&t, "q", 1, "now", stamp);
    var got: [1]u64 = undefined;
    try testing.expectEqual(@as(usize, 1), try deliver(&t, 1, stamp + 1, &got));
    try testing.expectEqual(now_due, got[0]);
}

test "queue machine: a deliver stops where the next payload would pass its byte budget" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    _ = try enqueue(&t, "q", 0, "aaaa", 10);
    _ = try enqueue(&t, "q", 0, "bbbb", 20);
    const r = try t.apply(.queue_deliver, qe.Deliver.Value{ .ns = NS.int(), .queue = "q", .count = 10, .max_bytes = 6, .holder = "c1", .lease_ms = LEASE_MS }, 100);
    var rd = r.reader();
    try testing.expectEqual(@as(u32, 1), rd.int(u32).?);
}

test "queue machine: a settle removes what its holder holds, and answers each other message by what became of it" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    const a = try enqueue(&t, "q", 0, "a", 10);
    const b = try enqueue(&t, "q", 0, "b", 20);
    var got: [2]u64 = undefined;
    _ = try deliver(&t, 2, 100, &got);
    const ans = try settle(&t, &.{a}, 200);
    try testing.expectEqual(@as(u32, 1), ans.applied);
    try testing.expect(t.msg(NS, "q", a) == null);
    // A gone message, and a stale token, are each answered.
    var ib: [qe.Items.max_size]u8 = undefined;
    const m = t.msg(NS, "q", b).?;
    var w: usize = 0;
    w += (try qe.Item.encodeInto(ib[w..], .{ .seq = a, .deliveries = 1 })).len;
    w += (try qe.Item.encodeInto(ib[w..], .{ .seq = b, .deliveries = 1 })).len;
    const r = try t.apply(.queue_settle, qe.Settle.Value{ .ns = NS.int(), .queue = "q", .holder = "c1", .token = .{ .term = m.token.term, .index = m.token.index + 7 }, .expiry_ns = m.expiry_ns, .items = .{ .bytes = ib[0..w], .count = 2 } }, 300);
    const stale = Answer.of(r, false);
    try testing.expectEqual(@as(u32, 0), stale.applied);
    try testing.expectEqual(@as(usize, 2), stale.n);
    try testing.expectEqual(Why.gone, stale.refused[0].why);
    try testing.expectEqual(Why.delivered_again, stale.refused[1].why);
    try testing.expectEqual(Status.leased, t.msg(NS, "q", b).?.status);
}

test "queue machine: a message released until its attempts are spent is dead" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    const a = try enqueue(&t, "q", 0, "a", 10);
    var got: [1]u64 = undefined;
    var stamp: u64 = 100;
    for (0..qe.MAX_ATTEMPTS) |i| {
        try testing.expectEqual(@as(usize, 1), try deliver(&t, 1, stamp, &got));
        const ans = try release(&t, &.{a}, false, stamp + 1);
        try testing.expectEqual(@as(u32, 1), ans.applied);
        const m = t.msg(NS, "q", a).?;
        try testing.expectEqual(@as(u32, @intCast(i + 1)), m.failures);
        stamp += 10;
    }
    try testing.expectEqual(Status.dead, t.msg(NS, "q", a).?.status);
    try testing.expectEqual(@as(usize, 1), t.count(Dead));
    try testing.expectEqual(@as(usize, 0), try deliver(&t, 1, stamp, &got));
    const meta = t.meta(NS, "q").?;
    try testing.expectEqual(@as(u64, 1), meta.dead);
    try testing.expectEqual(@as(u64, 0), meta.ready + meta.leased);
}

test "queue machine: a release to dead letters skips the attempts left" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    const a = try enqueue(&t, "q", 0, "a", 10);
    var got: [1]u64 = undefined;
    _ = try deliver(&t, 1, 100, &got);
    _ = try release(&t, &.{a}, true, 200);
    try testing.expectEqual(Status.dead, t.msg(NS, "q", a).?.status);
}

test "queue machine: a lease past its deadline ends at the duty's expire, and the message goes again" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    const a = try enqueue(&t, "q", 0, "a", 10);
    var got: [1]u64 = undefined;
    _ = try deliver(&t, 1, 100, &got);
    // Not yet due: nothing ends.
    var r = try t.duty(100 + LEASE - 1);
    try testing.expectEqual(@as(usize, 1), t.count(Deadlines));
    r = try t.duty(100 + LEASE);
    var rd = r.reader();
    try testing.expectEqual(@as(u32, 1), rd.int(u32).?);
    try testing.expectEqual(@as(usize, 0), t.count(Deadlines));
    const m = t.msg(NS, "q", a).?;
    try testing.expectEqual(Status.ready, m.status);
    try testing.expectEqual(Ended.expired, m.ended);
    // The expired holder's settle is stale; the next delivery is the 2nd.
    const ans = try settle(&t, &.{a}, 100 + LEASE + 1);
    try testing.expectEqual(@as(u32, 0), ans.applied);
    _ = try deliver(&t, 1, 100 + LEASE + 2, &got);
    try testing.expectEqual(@as(u32, 2), t.msg(NS, "q", a).?.deliveries);
}

test "queue machine: a message whose leases keep running out is dead once its attempts are spent" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    const a = try enqueue(&t, "q", 0, "a", 10);
    var got: [1]u64 = undefined;
    var stamp: u64 = 100;
    for (0..qe.MAX_ATTEMPTS) |i| {
        try testing.expectEqual(@as(usize, 1), try deliver(&t, 1, stamp, &got));
        stamp += LEASE;
        _ = try t.duty(stamp);
        try testing.expectEqual(@as(u32, @intCast(i + 1)), t.msg(NS, "q", a).?.failures);
        stamp += 1;
    }
    const m = t.msg(NS, "q", a).?;
    try testing.expectEqual(Status.dead, m.status);
    try testing.expectEqualStrings("lease expired", m.@"error");
    try testing.expectEqual(@as(usize, 1), t.count(Dead));
    try testing.expectEqual(@as(usize, 0), t.count(Ready));
    try testing.expectEqual(@as(usize, 0), try deliver(&t, 1, stamp, &got));
    const meta = t.meta(NS, "q").?;
    try testing.expectEqual(@as(u64, 1), meta.dead);
    try testing.expectEqual(@as(u64, qe.MAX_ATTEMPTS), meta.expired);
}

test "queue machine: an extend moves a lease out once half of it is gone, and the old key goes" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    const a = try enqueue(&t, "q", 0, "a", 10);
    var got: [1]u64 = undefined;
    _ = try deliver(&t, 1, 100, &got);
    const m = t.msg(NS, "q", a).?;
    var ib: [qe.Items.max_size]u8 = undefined;
    const it = (try qe.Item.encodeInto(&ib, .{ .seq = a, .deliveries = 1 })).len;
    const body: qe.Extend.Value = .{ .ns = NS.int(), .queue = "q", .holder = "c1", .token = m.token, .expiry_ns = m.expiry_ns, .held_since_ns = m.delivered_ns, .lease_ms = m.lease_ms, .items = .{ .bytes = ib[0..it], .count = 1 } };
    // Early: the lease stands.
    var r = try t.apply(.queue_extend, body, 200);
    try testing.expectEqual(m.expiry_ns, t.msg(NS, "q", a).?.expiry_ns);
    r = try t.apply(.queue_extend, body, 100 + LEASE / 2 + 1);
    var rd = r.reader();
    try testing.expectEqual(@as(u32, 1), rd.int(u32).?);
    try testing.expectEqual(@as(u64, 100 + LEASE / 2 + 1 + LEASE), rd.int(u64).?);
    try testing.expectEqual(@as(usize, 1), t.count(Deadlines));
}

test "queue machine: the same name in two namespaces is two queues, and a purge empties one" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    _ = try enqueue(&t, "q", 0, "a", 10);
    _ = try t.apply(.queue_enqueue, enqueueValue(.of(7), "q", 0, "x"), 20);
    var got: [1]u64 = undefined;
    _ = try deliver(&t, 1, 100, &got);
    const r = try t.apply(.queue_purge, qe.Purge.Value{ .ns = NS.int(), .queue = "q" }, 200);
    var rd = r.reader();
    try testing.expectEqual(@as(u64, 1), rd.int(u64).?);
    try testing.expect(t.msg(NS, "q", 1) == null);
    try testing.expect(t.msg(.of(7), "q", 1) != null);
    // The purged lease's key is the duty's to drop.
    _ = try t.duty(100 + LEASE);
    try testing.expectEqual(@as(usize, 0), t.count(Deadlines));
}

fn shardHeldOf(t: *TestQueues) u64 {
    const bytes = t.store.get(Helds.ID, HELD_KEY) orelse return 0;
    return (Stored(Held).decode(bytes) catch unreachable).v.bytes;
}

const proto_reason = @import("../protocol/reason.zig");

/// What "abcd" holds, as a ready message with no error.
const ABCD = heldBy(4, 0);

test "queue machine: an enqueue past its queue's message or byte cap is refused, and dead letters count" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    var v = enqueueValue(NS, "q", 0, "abcd");
    v.max_messages = 2;
    v.max_bytes = 100_000;
    const first = try t.apply(.queue_enqueue, v, 10);
    var fr = first.reader();
    const a = fr.int(u64).?;
    _ = try t.apply(.queue_enqueue, v, 11);
    // Two held: the third is refused, and changes nothing.
    const before = t.sums();
    const full = try t.apply(.queue_enqueue, v, 12);
    try testing.expectEqual(proto_status.overloaded, full.refused().?.status);
    try testing.expectEqual(proto_reason.Reason.queue_full, full.refused().?.refusal.reason);
    try testing.expectEqual(proto_reason.Ran.no, full.refused().?.refusal.ran);
    try testing.expect(std.mem.indexOf(u8, full.refused().?.refusal.message, "queue 'q' is full: it holds 2 messages (at most 2)") != null);
    try testing.expectEqual(before, t.sums());
    // The shard's total, with this queue's own caps still clear: the same
    // reason, and nothing changes.
    var w = enqueueValue(NS, "other", 0, "abcd");
    // Two held already: a total of one more byte than they hold refuses.
    w.shard_held_bytes = 2 * ABCD + ABCD - 1;
    const shard_full = try t.apply(.queue_enqueue, w, 13);
    try testing.expectEqual(proto_status.overloaded, shard_full.refused().?.status);
    try testing.expectEqual(proto_reason.Reason.queue_full, shard_full.refused().?.refusal.reason);
    try testing.expectEqual(proto_reason.Ran.no, shard_full.refused().?.refusal.ran);
    try testing.expect(std.mem.indexOf(u8, shard_full.refused().?.refusal.message, "this shard's queues and groups hold") != null);
    try testing.expectEqual(before, t.sums());
    // A dead letter still counts, its error text too; a settle frees its
    // slot and bytes.
    var got: [2]u64 = undefined;
    _ = try deliver(&t, 2, 100, &got);
    _ = try release(&t, &.{a}, true, 200);
    try testing.expectEqual(2 * ABCD + "boom".len, t.meta(NS, "q").?.held_bytes);
    try testing.expect((try t.apply(.queue_enqueue, v, 210)).refused() != null);
    _ = try settle(&t, &.{got[1]}, 300);
    try testing.expect((try t.apply(.queue_enqueue, v, 310)).refused() == null);
    const held = t.meta(NS, "q").?.held_bytes;
    try testing.expectEqual(2 * ABCD + "boom".len, held);
    // The byte cap, at its edge: one byte short refuses, exact takes it.
    v.max_messages = 100;
    v.max_bytes = held + ABCD - 1;
    try testing.expect((try t.apply(.queue_enqueue, v, 320)).refused() != null);
    v.max_bytes = held + ABCD;
    try testing.expect((try t.apply(.queue_enqueue, v, 330)).refused() == null);
}

test "queue machine: an empty message holds its overhead, so empty messages fill a shard too" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    var v = enqueueValue(NS, "q", 0, "");
    v.shard_held_bytes = 3 * qe.PER_MESSAGE;
    var stamp: u64 = 10;
    for (0..3) |_| {
        try testing.expect((try t.apply(.queue_enqueue, v, stamp)).refused() == null);
        stamp += 1;
    }
    try testing.expectEqual(3 * qe.PER_MESSAGE, shardHeldOf(&t));
    const full = try t.apply(.queue_enqueue, v, stamp);
    try testing.expect(std.mem.indexOf(u8, full.refused().?.refusal.message, "this shard's queues and groups hold 1536 held bytes, at most 1536") != null);
}

test "queue machine: a message's error text is held, replaced as it fails again, and given back with it" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    const a = try enqueue(&t, "q", 0, "abcd", 10);
    var got: [1]u64 = undefined;
    _ = try deliver(&t, 1, 100, &got);
    _ = try release(&t, &.{a}, false, 200);
    try testing.expectEqual(ABCD + "boom".len, t.meta(NS, "q").?.held_bytes);
    // Its lease runs out: the error is "lease expired" now.
    _ = try deliver(&t, 1, 300, &got);
    _ = try t.duty(300 + LEASE);
    try testing.expectEqual(ABCD + "lease expired".len, t.meta(NS, "q").?.held_bytes);
    try testing.expectEqual(ABCD + "lease expired".len, shardHeldOf(&t));
    _ = try deliver(&t, 1, 400 + LEASE, &got);
    _ = try settle(&t, &got, 500 + LEASE);
    try testing.expectEqual(@as(u64, 0), t.meta(NS, "q").?.held_bytes);
    try testing.expectEqual(@as(u64, 0), shardHeldOf(&t));
}

test "queue machine: one expire ends every live lease it names past its deadline" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    _ = try enqueue(&t, "q", 0, "a", 10);
    _ = try enqueue(&t, "q", 0, "b", 11);
    _ = try enqueue(&t, "q", 0, "c", 12);
    var got: [3]u64 = undefined;
    try testing.expectEqual(@as(usize, 3), try deliver(&t, 3, 100, &got));
    // One deadline for all three: one expire names them all, and all end.
    const r = try t.duty(100 + LEASE);
    var rd = r.reader();
    try testing.expectEqual(@as(u32, 3), rd.int(u32).?);
    for (got) |seq| try testing.expectEqual(Status.ready, t.msg(NS, "q", seq).?.status);
    try testing.expectEqual(@as(u64, 3), t.meta(NS, "q").?.ready);
    try testing.expectEqual(@as(usize, 0), t.count(Deadlines));
}

test "queue machine: the shard's held bytes count every queue, and a settle or purge gives them back" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    var v = enqueueValue(NS, "q", 0, "abcd");
    v.shard_held_bytes = 2 * ABCD;
    _ = try t.apply(.queue_enqueue, v, 10);
    var other = enqueueValue(.of(7), "other", 0, "abcd");
    other.shard_held_bytes = 2 * ABCD;
    _ = try t.apply(.queue_enqueue, other, 11);
    try testing.expectEqual(2 * ABCD, shardHeldOf(&t));
    const full = try t.apply(.queue_enqueue, v, 12);
    try testing.expect(std.mem.indexOf(u8, full.refused().?.refusal.message, "this shard's queues and groups hold 1032 held bytes, at most 1032") != null);
    var got: [1]u64 = undefined;
    _ = try deliver(&t, 1, 100, &got);
    _ = try settle(&t, &got, 200);
    try testing.expectEqual(ABCD, shardHeldOf(&t));
    _ = try t.apply(.queue_purge, qe.Purge.Value{ .ns = 7, .queue = "other" }, 300);
    try testing.expectEqual(@as(u64, 0), shardHeldOf(&t));
    try testing.expect((try t.apply(.queue_enqueue, v, 310)).refused() == null);
}

test "queue machine: dead letters past the old cap of 10,000 are all kept" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    const a = try enqueue(&t, "q", 0, "a", 10);
    var got: [1]u64 = undefined;
    _ = try deliver(&t, 1, 100, &got);
    _ = try release(&t, &.{a}, true, 200);
    // Stand the meta's dead count in for 10,000 more.
    var mk: [12]u8 = undefined;
    const hash = hashOf("q");
    var meta = t.meta(NS, "q").?;
    meta.dead = 10_000;
    const k = storeKey(&mk, NS, &hash);
    var w: std.Io.Writer.Allocating = .init(testing.allocator);
    defer w.deinit();
    try (Stored(Meta){ .v = meta }).encode(&w.writer);
    var rit = t.store.range(Metas.ID, k);
    const rec = rit.next().?;
    t.store.replace(Metas.ID, &rec.node, try testing.allocator.dupe(u8, w.written()));
    const b = try enqueue(&t, "q", 0, "b", 300);
    _ = try deliver(&t, 1, 400, &got);
    _ = try release(&t, &.{b}, true, 500);
    try testing.expectEqual(Status.dead, t.msg(NS, "q", a).?.status);
    try testing.expectEqual(@as(usize, 2), t.count(Dead));
}

test "queue machine: a replica that replays the log holds the same state" {
    var t = TestQueues.init(testing.allocator);
    defer t.deinit();
    const a = try enqueue(&t, "q", 1, "a", 10);
    _ = try enqueue(&t, "q", 0, "b", 20);
    var got: [2]u64 = undefined;
    _ = try deliver(&t, 2, 100, &got);
    _ = try release(&t, &.{a}, false, 150);
    _ = try t.duty(100 + LEASE);
    var other = TestQueues.init(testing.allocator);
    defer other.deinit();
    try t.replayInto(&other);
    try testing.expectEqual(t.sums(), other.sums());
}

test "queue machine: a ready message's encoded state is its payload and a fixed few hundred bytes" {
    const store_mod = @import("../contract/store.zig");
    var m = emptyMsgForTest();
    const fixed = try Msg.encodedSize(m);
    const hundred: [100]u8 = @splat(0x78);
    m.payload = &hundred;
    try testing.expectEqual(fixed + 100, try Msg.encodedSize(m));
    // Its record and its ready key's, each a key, a value and a record.
    var rb: [M.MAX_KEY]u8 = undefined;
    const ready_key = 4 + M.readyKeyOf(&rb, m, 0, &idOf(hashOf("q"), 1)).len;
    const per_message = fixed + (4 + qe.ID_LEN) + ready_key + 2 * (@sizeOf(store_mod.Record) + store_mod.RECORD_OVERHEAD);
    // 309 bytes as encoded state; measured in RAM (macOS, ReleaseSafe,
    // 100,000 messages of 100 bytes), 336 bytes besides the payload.
    try testing.expect(per_message < 400);
}

fn emptyMsgForTest() Msg.Value {
    return .{ .queue = "01234567", .priority = 0, .status = .ready, .payload = "", .enqueued_ns = 0, .due_ns = 0, .deliveries = 0, .failures = 0, .holder = "", .request_key = null, .token = .{ .term = 0, .index = 0 }, .lease_ms = 0, .delivered_ns = 0, .expiry_ns = 0, .ended = .none, .started_ns = 0, .@"error" = "", .dead_ns = 0 };
}
