//! Consumer groups on the contract: what a group has delivered of its
//! stream is families, changed only through a `View` in `apply`, so a
//! refused entry changes nothing and every replica holds the same leases,
//! cursor and dead letters.
//!
//! - `group.meta`: each group, keyed (ns id, scope): its stream's and its
//!   own name, its lease and attempts, its cursor, and its counts. A
//!   group's scope is its stream's name hash (its `StreamKey` past the
//!   namespace id the family key already leads with), then its own name's.
//! - `group.item`: each record the group delivered and hasn't settled,
//!   keyed (ns id, scope, record id). A record past the cursor has none: it
//!   is new, and the next deliver takes it after the ready ones.
//! - `group.dead`: each dead record, keyed (ns id, scope, when it died,
//!   record id), so a group's dead letters page oldest first.
//! - `group.stream`: each stream with groups, keyed by its `StreamKey` in
//!   no namespace: the last record a trim cut (the floor), and its generation,
//!   which a delete of the stream moves on. The stream's trims and deletes
//!   write it as they apply (`observe`), one key whatever its groups hold.
//!   A group of an older generation is gone; one's records at or below the
//!   floor are trimmed. Both leave by `group.clean` entries, a bounded few
//!   at a time.
//! - `group.ready`, `group.deadline`: the delivery model's
//!   (`contract/delivery.zig`). A group's ready keys order by due time, then
//!   record id.
//!
//! A deliver names no records: it leases the group's due ready records,
//! then the stream's past its cursor, as this replica holds them at the
//! entry's index, which is what every replica holds (`contract/streams.zig`).
//! A report names its records and their delivery's token, and applies to
//! each it still holds; the rest are answered by what became of them.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const schema = @import("../protocol/schema.zig");
const ae = @import("../protocol/action_entries.zig");
const ge = @import("../protocol/group_entries.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const streams_mod = @import("../contract/streams.zig");
const router = @import("../node/router.zig");
const stream_proj = @import("../projection/stream.zig");
pub const StreamKey = stream_proj.StreamKey;

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
const Empty = delivery.Empty;
const Streams = streams_mod.Streams;

pub const StreamID = streams_mod.StreamID;
pub const NamespaceId = router.NamespaceId;
pub const SCOPE_LEN = ge.SCOPE_LEN;
pub const ID_LEN = ge.ID_LEN;
pub const Scope = [SCOPE_LEN]u8;

pub const Meta = Record(&.{
    .{ .name = "stream", .T = ge.StreamName },
    .{ .name = "group", .T = ge.GroupName },
    .{ .name = "lease_ms", .T = Int(u64) },
    .{ .name = "max_attempts", .T = Int(u32) },
    .{ .name = "created_ns", .T = Int(u64) },
    // Its stream's generation when it was created; another, or `GONE`, and
    // the group is gone.
    .{ .name = "gen", .T = Int(u64) },
    // The last record past which every one was delivered once.
    .{ .name = "cursor_ms", .T = Int(u64) },
    .{ .name = "cursor_seq", .T = Int(u64) },
    .{ .name = "ready", .T = Int(u64) },
    .{ .name = "leased", .T = Int(u64) },
    .{ .name = "dead", .T = Int(u64) },
    // What its records hold, ready, leased and dead: each one's error text
    // and the per-message overhead (`heldBy`); the stream holds payloads.
    .{ .name = "held_bytes", .T = Int(u64) },
    // Since the group was created.
    .{ .name = "delivered", .T = Int(u64) },
    .{ .name = "settled", .T = Int(u64) },
    .{ .name = "released", .T = Int(u64) },
    .{ .name = "dead_lettered", .T = Int(u64) },
    .{ .name = "expired", .T = Int(u64) },
    // Records a trim cut, cleaned out of ready, leased or dead.
    .{ .name = "trimmed", .T = Int(u64) },
});

/// A stream's floor and generation, as its groups read them. Kept only
/// while groups of it are, live or gone: `groups` counts their metas.
pub const StreamRec = Record(&.{
    .{ .name = "floor_ms", .T = Int(u64) },
    .{ .name = "floor_seq", .T = Int(u64) },
    .{ .name = "gen", .T = Int(u64) },
    .{ .name = "groups", .T = Int(u64) },
});

/// A deleted group's generation: never a stream's.
pub const GONE = std.math.maxInt(u64);

pub const Status = enum(u8) { ready = 0, leased = 1, dead = 2 };
pub const Ended = delivery.Ended;

pub const Item = Record(&.{
    // Its group's scope: the scope of its ready and deadline keys.
    .{ .name = "scope", .T = Bytes(SCOPE_LEN) },
    .{ .name = "status", .T = schema.Enum(Status) },
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
    .{ .name = "error", .T = Bytes(ge.MAX_ERROR) },
    // When it died, while dead: its dead key's time.
    .{ .name = "dead_ns", .T = Int(u64) },
});

/// A record's leases: the delivery model, keyed by namespace id.
const M = delivery.Model(.{
    .ready_id = 43,
    .ready_name = "group.ready",
    .deadline_id = 44,
    .deadline_name = "group.deadline",
    .request_id = 45,
    .request_name = "group.request",
    .scope = "scope",
    .max_scope = SCOPE_LEN,
    .max_id = ID_LEN,
    .noun = "record",
});

pub const Metas = view_mod.Family(40, "group.meta", Stored(Meta));
pub const Items = view_mod.Family(41, "group.item", Stored(Item));
pub const Dead = view_mod.Family(42, "group.dead", Empty);
pub const Streams_ = view_mod.Family(46, "group.stream", Stored(StreamRec));
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

/// Why a report applied nothing to one of its records.
pub const Why = @import("../protocol/queue_ops.zig").Why;

comptime {
    // A deliver's answer: its count, then each record's id and deliveries.
    std.debug.assert(4 + ge.MAX_BATCH * (8 + 8 + 4) <= persistence.Result.RESULT_MAX);
    // A report's: its count, then each refused record's id and why.
    std.debug.assert(4 + 8 + ge.MAX_BATCH * (8 + 8 + 1) <= persistence.Result.RESULT_MAX);
}

pub const NO_GROUP = "not found: consumer group not found";
pub const SETTINGS_DIFFER = "conflict: group '{s}' exists with a lease (ack timeout) of {d} ms and {d} attempts (max deliver); a create can't change them";
pub const NAME_TAKEN = "conflict: another group's name hashes as this one's; choose another name";

// ─── Keys ────────────────────────────────────────────────────────────────

/// A name's hash, as a group's keys hold it.
fn hashOf(name: []const u8) [8]u8 {
    const k = router.nsNameKey(.invalid, name);
    return k[4..12].*;
}

/// The stream, as the stream projection keys it.
pub fn streamOf(ns: NamespaceId, stream: []const u8) StreamKey {
    return stream_proj.streamKey(ns, stream);
}

/// A group's scope: its stream's name hash, then its own name's.
pub fn scopeOf(ns: NamespaceId, stream: []const u8, group: []const u8) Scope {
    var s: Scope = undefined;
    s[0..8].* = streamOf(ns, stream)[4..12].*;
    s[8..16].* = hashOf(group);
    return s;
}

/// The stream a group of `ns` with this scope reads.
pub fn streamOfScope(ns: NamespaceId, scope: []const u8) StreamKey {
    var k: StreamKey = undefined;
    k[0..4].* = ns.bytes();
    k[4..12].* = scope[0..8].*;
    return k;
}

/// A record's id in its namespace: its group's scope, then the record's.
pub fn idOf(scope: Scope, record: StreamID) [ID_LEN]u8 {
    var id: [ID_LEN]u8 = undefined;
    id[0..SCOPE_LEN].* = scope;
    std.mem.writeInt(u64, id[16..24], record.timestamp_ms, .big);
    std.mem.writeInt(u64, id[24..32], record.sequence, .big);
    return id;
}

pub fn recordOf(id: []const u8) StreamID {
    return .{ .timestamp_ms = std.mem.readInt(u64, id[16..24], .big), .sequence = std.mem.readInt(u64, id[24..32], .big) };
}

/// `[scope][died:u64 BE][record id]`: a group's dead letters, oldest first.
pub fn deadKey(scope: Scope, dead_ns: u64, record: StreamID) [SCOPE_LEN + 24]u8 {
    var k: [SCOPE_LEN + 24]u8 = undefined;
    k[0..SCOPE_LEN].* = scope;
    std.mem.writeInt(u64, k[16..24], dead_ns, .big);
    std.mem.writeInt(u64, k[24..32], record.timestamp_ms, .big);
    std.mem.writeInt(u64, k[32..40], record.sequence, .big);
    return k;
}

fn cursorOf(m: Meta.Value) StreamID {
    return .{ .timestamp_ms = m.cursor_ms, .sequence = m.cursor_seq };
}

/// A group nothing was delivered from yet.
pub fn newMeta(stream: []const u8, group: []const u8, lease_ms: u64, max_attempts: u32, stamp: u64, gen: u64) Meta.Value {
    return .{ .stream = stream, .group = group, .lease_ms = lease_ms, .max_attempts = max_attempts, .created_ns = stamp, .gen = gen, .cursor_ms = 0, .cursor_seq = 0, .ready = 0, .leased = 0, .dead = 0, .held_bytes = 0, .delivered = 0, .settled = 0, .released = 0, .dead_lettered = 0, .expired = 0, .trimmed = 0 };
}

/// A stream's record's key: its `StreamKey`, in no namespace.
pub fn recKey(buf: *[13]u8, stream: StreamKey) []const u8 {
    buf[0] = 0;
    buf[1..13].* = stream;
    return buf;
}

pub const NO_STREAM_REC: StreamRec.Value = .{ .floor_ms = 0, .floor_seq = 0, .gen = 0, .groups = 0 };

fn streamRec(view: *View, sk: StreamKey) ApplyError!StreamRec.Value {
    var b: [13]u8 = undefined;
    return if (try view.get(Streams_, recKey(&b, sk))) |r| r.v else NO_STREAM_REC;
}

/// A group of the stream made: its record counts it, made at the stream's
/// trim so far when the stream had none.
fn addGroup(view: *View, sk: StreamKey) ApplyError!StreamRec.Value {
    var b: [13]u8 = undefined;
    const key = recKey(&b, sk);
    var rec = if (try view.get(Streams_, key)) |r| r.v else blk: {
        const cut = (try view.streams()).removedThrough(sk);
        break :blk StreamRec.Value{ .floor_ms = cut.timestamp_ms, .floor_seq = cut.sequence, .gen = 0, .groups = 0 };
    };
    rec.groups += 1;
    try view.put(Streams_, key, .{ .v = rec });
    return rec;
}

/// A group's meta dropped: its stream's record goes with its last.
fn dropGroup(view: *View, sk: StreamKey) ApplyError!void {
    var b: [13]u8 = undefined;
    const key = recKey(&b, sk);
    var rec = ((try view.get(Streams_, key)) orelse return error.Halt).v;
    try less(&rec.groups);
    if (rec.groups == 0) return view.delete(Streams_, key);
    try view.put(Streams_, key, .{ .v = rec });
}

pub fn floorOf(r: StreamRec.Value) StreamID {
    return .{ .timestamp_ms = r.floor_ms, .sequence = r.floor_seq };
}

/// Whether a group's meta is of its stream's current generation.
pub fn live(m: Meta.Value, r: StreamRec.Value) bool {
    return m.gen == r.gen;
}

const qm = @import("../queue/machine.zig");
const Helds = qm.Helds;
const HELD_KEY = qm.HELD_KEY;

/// What a record a group holds counts against the caps: its error text and
/// the per-message overhead, as a queue's message does but its payload,
/// which the stream holds.
pub fn heldBy(error_len: usize) u64 {
    return qm.heldBy(0, error_len);
}

/// Bytes a group stops holding: its own and the shard's.
fn unhold(view: *View, meta: *Meta.Value, size: u64) ApplyError!void {
    if (size > meta.held_bytes) return error.Halt;
    meta.held_bytes -= size;
    const held = try qm.heldOf(view);
    if (size > held) return error.Halt;
    try view.put(Helds, HELD_KEY, .{ .v = .{ .bytes = held - size } });
}

fn hold(view: *View, meta: *Meta.Value, size: u64) ApplyError!void {
    meta.held_bytes += size;
    try view.put(Helds, HELD_KEY, .{ .v = .{ .bytes = (try qm.heldOf(view)) + size } });
}

/// A record's error text replaced: what it holds moves by the difference.
fn reholdError(view: *View, meta: *Meta.Value, old_len: usize, new_len: usize) ApplyError!void {
    if (new_len >= old_len) return hold(view, meta, new_len - old_len);
    return unhold(view, meta, old_len - new_len);
}

pub const FULL = "overloaded: group '{s}' on stream '{s}' is full: it holds {d} records (at most {d}) and {d} held bytes (at most {d}), leased, ready and dead; ack or nack them";
pub const SHARD_FULL = "overloaded: this shard's queues and groups hold {d} held bytes, at most {d}; ack, consume or purge some";

pub const BEING_REMOVED = "unavailable: this group's records from before it was deleted, or its stream was, are still being removed; retry";

fn typeOf(e: *const Entry) EntryType {
    return @fromBackingInt(@intCast(e.header.entry_type));
}

/// A deadline key's prefix: its expiry, its namespace's id, its scope.
fn deadlinePrefix(buf: *[12 + SCOPE_LEN]u8, expiry_ns: u64, ns: NamespaceId, scope: []const u8) []const u8 {
    std.mem.writeInt(u64, buf[0..8], expiry_ns, .big);
    buf[8..12].* = ns.bytes();
    @memcpy(buf[12..], scope[0..SCOPE_LEN]);
    return buf;
}

/// A group's own keys in every family an entry may change at will.
fn groupKeys(out: *KeySet, ns: NamespaceId, scope: []const u8) error{OutOfMemory}!void {
    try out.add(Helds.ID, "", HELD_KEY[1..]);
    var sb: [13]u8 = undefined;
    try out.add(Streams_.ID, "", recKey(&sb, streamOfScope(ns, scope))[1..]);
    out.addIn(Metas.ID, ns, scope) catch |e| return e;
    var pb: [SCOPE_LEN + 1]u8 = undefined;
    inline for (.{ Items.ID, Dead.ID }) |fam| out.addRangeIn(fam, ns, scope) catch |e| switch (e) {
        error.NoNamespace => return,
        else => |o| return o,
    };
    out.addRangeIn(Ready.ID, ns, readyPrefix(&pb, scope)) catch |e| switch (e) {
        error.NoNamespace => return,
        else => |o| return o,
    };
}

/// The deadline keys a group's leases hold at `expiry_ns`.
fn leaseKeys(out: *KeySet, ns: NamespaceId, scope: []const u8, expiry_ns: u64) error{OutOfMemory}!void {
    var b: [12 + SCOPE_LEN]u8 = undefined;
    out.addRangeNaming(Deadlines.ID, 8, deadlinePrefix(&b, expiry_ns, ns, scope)) catch |e| switch (e) {
        error.NoNamespace => return,
        else => |o| return o,
    };
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    var diag: schema.Diagnostic = .{};
    const stamp = e.header.timestamp_ns;
    switch (typeOf(e)) {
        inline .group_create, .group_delete => |t| {
            const p = ge.Payload(@backingInt(t)).decodeAll(e.payload, &diag) catch return;
            const ns: NamespaceId = .of(p.ns);
            try groupKeys(out, ns, &scopeOf(ns, p.stream, p.group));
        },
        .group_deliver => {
            const p = ge.Deliver.decodeAll(e.payload, &diag) catch return;
            const ns: NamespaceId = .of(p.ns);
            const scope = scopeOf(ns, p.stream, p.group);
            try groupKeys(out, ns, &scope);
            try leaseKeys(out, ns, &scope, after(stamp, p.lease_ms));
        },
        inline .group_settle, .group_release, .group_extend => |t| {
            const p = ge.Payload(@backingInt(t)).decodeAll(e.payload, &diag) catch return;
            const ns: NamespaceId = .of(p.ns);
            const scope = scopeOf(ns, p.stream, p.group);
            try groupKeys(out, ns, &scope);
            try leaseKeys(out, ns, &scope, p.expiry_ns);
            if (t == .group_extend) try leaseKeys(out, ns, &scope, extendedTo(stamp, p.lease_ms, p.held_since_ns));
        },
        .group_expire => {
            const p = ge.Expire.decodeAll(e.payload, &diag) catch return;
            var it = p.items.iterator();
            while (it.next() catch return) |item| {
                if (item.id.len != ID_LEN) continue;
                const ns: NamespaceId = .of(item.ns);
                try groupKeys(out, ns, item.id[0..SCOPE_LEN]);
                try leaseKeys(out, ns, item.id[0..SCOPE_LEN], item.expiry_ns);
            }
        },
        .group_clean => {
            const p = ge.Clean.decodeAll(e.payload, &diag) catch return;
            var it = p.items.iterator();
            while (it.next() catch return) |item| if (item.scope.len == SCOPE_LEN) try groupKeys(out, .of(item.ns), item.scope);
        },
        else => {},
    }
}

// ─── Apply ───────────────────────────────────────────────────────────────

/// The group named, or null and the refusal said: none, or another's name
/// in its slot.
fn metaOf(view: *View, out: *Result, ns: NamespaceId, scope: []const u8, stream: []const u8, group: []const u8) ApplyError!?Meta.Value {
    const m = (try view.get(Metas, try M.inNs(view, ns, scope))) orelse {
        out.refuse(.not_found, .not_found, NO_GROUP);
        return null;
    };
    if (!std.mem.eql(u8, m.v.stream, stream) or !std.mem.eql(u8, m.v.group, group)) {
        out.refuse(.not_found, .not_found, NO_GROUP);
        return null;
    }
    return m.v;
}

fn why(m: Item.Value, holder: []const u8, deliveries: u32, token: ae.Token.Value) Why {
    if (m.deliveries != deliveries or m.token.term != token.term or m.token.index != token.index) return .delivered_again;
    if (!std.mem.eql(u8, m.holder, holder)) return .other_holder;
    return switch (m.ended) {
        .expired => .expired,
        .released, .abandoned => .released,
        else => .not_held,
    };
}

/// The appends a deliver's answer carries, each counted once at its bound.
const Budget = struct {
    max: u64,
    used: u64 = 0,
    seen: [ge.MAX_BATCH]StreamID = undefined,
    n: usize = 0,

    /// Whether `record` fits, counting its append if this is the first of
    /// it taken; the first record always fits. Null when the stream no
    /// longer holds it.
    fn take(self: *Budget, streams: Streams, sk: StreamKey, record: StreamID, first: bool) ?bool {
        const a = streams.appendOf(sk, record) orelse return null;
        for (self.seen[0..self.n]) |s| if (s.eql(a.first)) return true;
        if (!first and self.used + a.answer_bytes > self.max) return false;
        self.used += a.answer_bytes;
        self.seen[self.n] = a.first;
        self.n += 1;
        return true;
    }
};

/// Answers: create, whether it made the group (u8); deliver, how many it
/// leased (u32) then each one's id (ms u64, seq u64) and deliveries (u32);
/// settle, release and extend, how many they applied to (u32), extend then
/// the expiry it leaves (u64), and then each record they didn't (ms u64,
/// seq u64, `Why` u8); expire, the leases it ended (u32); delete, the
/// records it dropped (u64).
fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    const stamp = e.header.timestamp_ns;
    switch (typeOf(e)) {
        .group_create => {
            const p = ge.Create.decodeAll(e.payload, &diag) catch return error.Halt;
            const ns: NamespaceId = .of(p.ns);
            if (ns == .invalid) return error.Halt;
            const scope = scopeOf(ns, p.stream, p.group);
            const mk = try M.inNs(view, ns, &scope);
            const rec = try streamRec(view, streamOf(ns, p.stream));
            if (try view.get(Metas, mk)) |m| {
                if (!live(m.v, rec)) return out.refuse(.unavailable, .unclassified, BEING_REMOVED);
                if (!std.mem.eql(u8, m.v.stream, p.stream) or !std.mem.eql(u8, m.v.group, p.group)) return out.refuse(.conflict, .unclassified, NAME_TAKEN);
                // The same settings: it exists. Others: refused, naming its
                // own, since a create can't change them.
                if (m.v.lease_ms != p.lease_ms or m.v.max_attempts != p.max_attempts)
                    return out.refuseFmt(.conflict, .unclassified, SETTINGS_DIFFER, .{ p.group, m.v.lease_ms, m.v.max_attempts });
                return out.putInt(u8, 0);
            }
            const made = try addGroup(view, streamOf(ns, p.stream));
            try view.put(Metas, mk, .{ .v = newMeta(p.stream, p.group, p.lease_ms, p.max_attempts, stamp, made.gen) });
            out.putInt(u8, 1);
        },
        .group_delete => {
            const p = ge.Delete.decodeAll(e.payload, &diag) catch return error.Halt;
            const ns: NamespaceId = .of(p.ns);
            const scope = scopeOf(ns, p.stream, p.group);
            var m = (try metaOf(view, out, ns, &scope, p.stream, p.group)) orelse return;
            if (!live(m, try streamRec(view, streamOf(ns, p.stream)))) return out.refuse(.not_found, .not_found, NO_GROUP);
            const held = m.ready + m.leased + m.dead;
            if (held == 0) {
                // Nothing to clean: gone in this apply.
                try view.delete(Metas, try M.inNs(view, ns, &scope));
                try dropGroup(view, streamOf(ns, p.stream));
            } else {
                // Gone now; its records leave by cleanup, a few at a time.
                m.gen = GONE;
                try view.put(Metas, try M.inNs(view, ns, &scope), .{ .v = m });
            }
            out.putInt(u64, held);
        },
        .group_deliver => try deliverApply(view, e, out),
        .group_settle => {
            const p = ge.Settle.decodeAll(e.payload, &diag) catch return error.Halt;
            try report(view, out, p, .settle, stamp);
        },
        .group_release => {
            const p = ge.Release.decodeAll(e.payload, &diag) catch return error.Halt;
            try report(view, out, p, .{ .release = .{ .delay_ms = p.delay_ms, .dead = p.dead, .@"error" = p.@"error", .free = p.free } }, stamp);
        },
        .group_extend => {
            const p = ge.Extend.decodeAll(e.payload, &diag) catch return error.Halt;
            try report(view, out, p, .{ .extend = .{ .lease_ms = p.lease_ms, .held_since_ns = p.held_since_ns } }, stamp);
        },
        .group_expire => {
            const p = ge.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
            var it = p.items.iterator();
            var ended: u32 = 0;
            while (it.next() catch return error.Halt) |item| {
                if (try expireOne(view, item, stamp)) ended += 1;
            }
            out.putInt(u32, ended);
        },
        .group_clean => {
            const p = ge.Clean.decodeAll(e.payload, &diag) catch return error.Halt;
            var it = p.items.iterator();
            var removed: u64 = 0;
            while (it.next() catch return error.Halt) |item| {
                if (item.scope.len != SCOPE_LEN) continue;
                removed += try cleanOne(view, .of(item.ns), item.scope[0..SCOPE_LEN].*);
            }
            out.putInt(u64, removed);
        },
        else => return error.Halt,
    }
}

/// Up to `ge.CLEAN_RECORDS` records a group no longer holds removed, with
/// their ready and dead keys; their deadline keys are the expire duty's to
/// drop, finding no record. A gone group's go, then its meta; a live one's
/// at or below its stream's floor, counted trimmed. Returns how many.
/// One fewer of a count the meta keeps: none left to take is a bug.
fn less(n: *u64) ApplyError!void {
    if (n.* == 0) return error.Halt;
    n.* -= 1;
}

fn lessU32(n: *u32) ApplyError!void {
    if (n.* == 0) return error.Halt;
    n.* -= 1;
}

fn cleanOne(view: *View, ns: NamespaceId, scope: Scope) ApplyError!u64 {
    const mk = try M.inNs(view, ns, &scope);
    var meta = (try view.get(Metas, mk)) orelse return 0;
    const rec = try streamRec(view, streamOfScope(ns, &scope));
    const gone = !live(meta.v, rec);
    const floor = floorOf(rec);
    var removed: u64 = 0;
    var it = try view.range(Items, try M.inNs(view, ns, &scope));
    var more = false;
    while (try it.next()) |entry| {
        if (removed == ge.CLEAN_RECORDS) {
            more = true;
            break;
        }
        const record = recordOf(entry.key[4..]);
        if (!gone and record.greaterThan(floor)) break;
        const v = entry.value.v;
        var b: M.KeyBuf = undefined;
        switch (v.status) {
            .ready => {
                try view.delete(Ready, try M.inNs(view, ns, readyKeyOf(&b, v, v.due_ns, entry.key[4..])));
                try less(&meta.v.ready);
            },
            .leased => try less(&meta.v.leased),
            .dead => {
                const dk = deadKey(scope, v.dead_ns, record);
                try view.delete(Dead, try M.inNs(view, ns, &dk));
                try less(&meta.v.dead);
            },
        }
        try view.delete(Items, entry.key);
        try unhold(view, &meta.v, heldBy(v.@"error".len));
        if (!gone) meta.v.trimmed += 1;
        removed += 1;
    }
    if (gone and !more) {
        if (meta.v.held_bytes != 0) return error.Halt;
        try view.delete(Metas, mk);
        try dropGroup(view, streamOfScope(ns, &scope));
    } else if (removed > 0) {
        try view.put(Metas, mk, meta);
    }
    return removed;
}

fn deliverApply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    const stamp = e.header.timestamp_ns;
    const p = ge.Deliver.decodeAll(e.payload, &diag) catch return error.Halt;
    const ns: NamespaceId = .of(p.ns);
    if (ns == .invalid) return error.Halt;
    const scope = scopeOf(ns, p.stream, p.group);
    const mk = try M.inNs(view, ns, &scope);
    const sk = streamOf(ns, p.stream);
    const rec = try streamRec(view, sk);
    const streams = try view.streams();
    const found = try view.get(Metas, mk);
    const made = found == null;
    var meta: Meta.Value = if (found) |m| m.v else blk: {
        if (!p.create) return out.refuse(.not_found, .not_found, NO_GROUP);
        break :blk newMeta(p.stream, p.group, ge.DEFAULT_LEASE_MS, ge.DEFAULT_MAX_ATTEMPTS, stamp, rec.gen);
    };
    if (!live(meta, rec)) return out.refuse(.unavailable, .unclassified, BEING_REMOVED);
    // A stream with no groups yet has no record: its floor is its trim.
    const floor = if (rec.groups == 0) streams.removedThrough(sk) else floorOf(rec);
    if (!std.mem.eql(u8, meta.stream, p.stream) or !std.mem.eql(u8, meta.group, p.group)) return out.refuse(.conflict, .unclassified, NAME_TAKEN);
    var budget: Budget = .{ .max = p.max_bytes };
    const token: ae.Token.Value = .{ .term = e.header.term, .index = e.header.index };

    // The ready records the leader chose, each still ready and due, and
    // not trimmed: an expire or a nack since may have moved one, and is
    // passed over.
    var chosen: [ge.MAX_BATCH]StreamID = undefined;
    var n: usize = 0;
    var from_ready: usize = 0;
    {
        var it = p.ready.iterator();
        while (it.next() catch return error.Halt) |named| {
            if (n == p.count) break;
            const record: StreamID = .{ .timestamp_ms = named.ms, .sequence = named.seq };
            if (!record.greaterThan(floor)) continue;
            // Named twice: taken once.
            if (for (chosen[0..n]) |c| {
                if (c.eql(record)) break true;
            } else false) continue;
            const id = idOf(scope, record);
            const v = (try view.get(Items, try M.inNs(view, ns, &id))) orelse continue;
            if (v.v.status != .ready or v.v.due_ns > stamp) continue;
            const fits = budget.take(streams, sk, record, n == 0) orelse continue;
            if (!fits) break;
            chosen[n] = record;
            n += 1;
        }
        from_ready = n;
    }
    // Then the stream's records past the cursor, while the group and the
    // shard stay within the entry's caps: each becomes a record it holds.
    var fresh: [ge.MAX_BATCH]StreamID = undefined;
    const from = if (floor.greaterThan(cursorOf(meta))) floor else cursorOf(meta);
    const got = streams.idsAfter(sk, from, fresh[0 .. p.count - n]);
    var took_fresh: usize = 0;
    const each = heldBy(0);
    const shard_held = try qm.heldOf(view);
    var capped = false;
    const through: StreamID = .{ .timestamp_ms = p.through_ms, .sequence = p.through_seq };
    for (fresh[0..got]) |record| {
        // Past what the leader holds the payloads of: left for the next.
        if (record.greaterThan(through)) break;
        const held = meta.ready + meta.leased + meta.dead + took_fresh;
        const bytes = each * (took_fresh + 1);
        if (held + 1 > p.max_held or meta.held_bytes + bytes > p.max_held_bytes or shard_held + bytes > p.shard_held_bytes) {
            capped = true;
            break;
        }
        const fits = budget.take(streams, sk, record, n == 0) orelse break;
        if (!fits) break;
        chosen[n] = record;
        n += 1;
        took_fresh += 1;
    }
    // Full, with nothing ready: refused, so the reader knows why.
    if (n == 0 and capped) {
        const held = meta.ready + meta.leased + meta.dead;
        if (held + 1 > p.max_held or meta.held_bytes + each > p.max_held_bytes)
            return out.refuseFmt(.overloaded, .unclassified, FULL, .{ p.group, p.stream, held, p.max_held, meta.held_bytes, p.max_held_bytes });
        return out.refuseFmt(.overloaded, .unclassified, SHARD_FULL, .{ shard_held, p.shard_held_bytes });
    }
    if (took_fresh > 0) try hold(view, &meta, each * took_fresh);

    out.putInt(u32, @intCast(n));
    for (chosen[0..n], 0..) |record, i| {
        const id = idOf(scope, record);
        const key = try M.inNs(view, ns, &id);
        var item: Item.Value = if (i < from_ready) ((try view.get(Items, key)) orelse return error.Halt).v else .{
            .scope = &scope,
            .status = .ready,
            .due_ns = stamp,
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
        try M.deliver(view, ns, &id, &item, p.holder, null, token, p.lease_ms, stamp);
        try view.put(Items, key, .{ .v = item });
        out.putInt(u64, record.timestamp_ms);
        out.putInt(u64, record.sequence);
        out.putInt(u32, item.deliveries);
    }
    if (took_fresh > 0) {
        const last = chosen[n - 1];
        meta.cursor_ms = last.timestamp_ms;
        meta.cursor_seq = last.sequence;
    }
    meta.ready -= from_ready;
    meta.leased += n;
    meta.delivered += n;
    if (made) _ = try addGroup(view, sk);
    try view.put(Metas, mk, .{ .v = meta });
}

const Report = union(enum) {
    settle,
    release: struct { delay_ms: u64, dead: bool, @"error": []const u8, free: bool },
    extend: struct { lease_ms: u64, held_since_ns: u64 },
};

/// A report's records, each applied to on its own: one it holds changes,
/// and one it doesn't is answered by what became of it. A resent release
/// whose first applied counts as applied; a resent settle finds its record
/// gone, as the settle removed it, and is answered gone.
fn report(view: *View, out: *Result, p: anytype, kind: Report, stamp: u64) ApplyError!void {
    const ns: NamespaceId = .of(p.ns);
    const scope = scopeOf(ns, p.stream, p.group);
    const mk = try M.inNs(view, ns, &scope);
    const found = try view.get(Metas, mk);
    const rec = try streamRec(view, streamOf(ns, p.stream));
    var meta = if (found) |m| (if (live(m.v, rec) and std.mem.eql(u8, m.v.stream, p.stream) and std.mem.eql(u8, m.v.group, p.group)) m else null) else null;
    // No such group: the whole report is refused, not each record gone.
    if (meta == null) return out.refuse(.not_found, .not_found, NO_GROUP);
    var applied: u32 = 0;
    var to: u64 = 0;
    // Refused items are said after the count: room for each, then filled.
    var refused: [ge.MAX_BATCH]struct { record: StreamID, why: Why } = undefined;
    var nr: usize = 0;
    var it = p.items.iterator();
    while (it.next() catch return error.Halt) |named| {
        const record: StreamID = .{ .timestamp_ms = named.ms, .sequence = named.seq };
        const id = idOf(scope, record);
        const key = try M.inNs(view, ns, &id);
        var item = (if (meta != null) try view.get(Items, key) else null) orelse {
            refused[nr] = .{ .record = record, .why = .gone };
            nr += 1;
            continue;
        };
        if (!delivery.holds(item.v, p.holder, named.deliveries, p.token)) {
            const same = delivery.sameDelivery(item.v, p.holder, named.deliveries, p.token);
            const repeat = same and switch (kind) {
                .settle, .extend => false,
                .release => item.v.ended == .released,
            };
            if (repeat) {
                applied += 1;
                continue;
            }
            refused[nr] = .{ .record = record, .why = why(item.v, p.holder, named.deliveries, p.token) };
            nr += 1;
            continue;
        }
        const m = &meta.?.v;
        switch (kind) {
            .settle => {
                try M.endLease(view, ns, &id, item.v, p.holder, null, p.expiry_ns);
                try view.delete(Items, key);
                try unhold(view, m, heldBy(item.v.@"error".len));
                m.leased -= 1;
                m.settled += 1;
            },
            .release => |r| {
                try M.endLease(view, ns, &id, item.v, p.holder, null, p.expiry_ns);
                if (r.free) {
                    // Handed back by the leader, never seen: no failure,
                    // and not a delivery.
                    item.v.ended = .abandoned;
                    try lessU32(&item.v.deliveries);
                    m.leased -= 1;
                    try M.readyAgain(view, ns, &id, &item.v, stamp);
                    m.ready += 1;
                    try view.put(Items, key, item);
                    applied += 1;
                    continue;
                }
                item.v.ended = .released;
                item.v.failures +|= 1;
                try reholdError(view, m, item.v.@"error".len, r.@"error".len);
                item.v.@"error" = r.@"error";
                m.leased -= 1;
                m.released += 1;
                if (r.dead or item.v.failures >= m.max_attempts) {
                    try die(view, ns, &scope, record, &item.v, m, stamp);
                } else {
                    try M.readyAgain(view, ns, &id, &item.v, after(stamp, r.delay_ms));
                    m.ready += 1;
                }
                try view.put(Items, key, item);
            },
            .extend => |x| {
                if (x.lease_ms != item.v.lease_ms or x.held_since_ns != item.v.delivered_ns) {
                    refused[nr] = .{ .record = record, .why = .delivered_again };
                    nr += 1;
                    continue;
                }
                if (try M.extend(view, ns, &id, &item.v, p.expiry_ns, stamp)) |t| {
                    to = t;
                    try view.put(Items, key, item);
                } else to = item.v.expiry_ns;
            },
        }
        applied += 1;
    }
    if (meta) |m| if (applied > 0) try view.put(Metas, mk, m);
    out.putInt(u32, applied);
    if (kind == .extend) out.putInt(u64, to);
    for (refused[0..nr]) |r| {
        out.putInt(u64, r.record.timestamp_ms);
        out.putInt(u64, r.record.sequence);
        out.putInt(u8, @backingInt(r.why));
    }
}

/// `item` dead at `stamp`, out of its group's ready records for good.
fn die(view: *View, ns: NamespaceId, scope: *const Scope, record: StreamID, item: *Item.Value, meta: *Meta.Value, stamp: u64) ApplyError!void {
    item.status = .dead;
    item.dead_ns = stamp;
    const dk = deadKey(scope.*, stamp, record);
    try view.put(Dead, try M.inNs(view, ns, &dk), .{});
    meta.dead += 1;
    meta.dead_lettered += 1;
}

const EXPIRED = "lease expired";

/// One expire item, past due by the log's clock: its key goes, and if it
/// is the record's live deadline, the lease ends as a failure.
fn expireOne(view: *View, item: ge.ExpireItem.Value, stamp: u64) ApplyError!bool {
    if (item.expiry_ns > stamp or item.id.len != ID_LEN) return false;
    const ns: NamespaceId = .of(item.ns);
    const scope: Scope = item.id[0..SCOPE_LEN].*;
    const key = try M.inNs(view, ns, item.id);
    const found = try view.get(Items, key);
    if (!try M.expireKey(view, ns, item.id, &scope, item.expiry_ns, if (found) |f| f.v else null, stamp)) return false;
    var rec = found.?;
    const mk = try M.inNs(view, ns, &scope);
    var meta = (try view.get(Metas, mk)) orelse return error.Halt;
    rec.v.ended = .expired;
    rec.v.failures +|= 1;
    try reholdError(view, &meta.v, rec.v.@"error".len, EXPIRED.len);
    rec.v.@"error" = EXPIRED;
    meta.v.leased -= 1;
    meta.v.expired += 1;
    if (rec.v.failures >= meta.v.max_attempts) {
        try die(view, ns, &scope, recordOf(item.id), &rec.v, &meta.v, stamp);
    } else {
        try M.readyAgain(view, ns, item.id, &rec.v, stamp);
        meta.v.ready += 1;
    }
    try view.put(Items, key, rec);
    try view.put(Metas, mk, meta);
    return true;
}

/// The stream an observed entry names. A trim's value is the stream's
/// name hash under the entry's namespace; a delete's key is its name.
fn observedStream(e: *const Entry) ?StreamKey {
    const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return null;
    return switch (typeOf(e)) {
        .stream_trim => if (cmd.value.len != 8) null else blk: {
            var k: StreamKey = undefined;
            k[0..4].* = cmd.namespace_id.bytes();
            k[4..12].* = cmd.value[0..8].*;
            break :blk k;
        },
        .stream_delete => if (cmd.key.len == 0) null else streamOf(cmd.namespace_id, cmd.key),
        else => null,
    };
}

fn observeKeys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const sk = observedStream(e) orelse return;
    var sb: [13]u8 = undefined;
    try out.add(Streams_.ID, "", recKey(&sb, sk)[1..]);
}

/// A stream trimmed, or deleted, as the stream applier applied it: its
/// record's floor moves to the last record the stream lost, or its
/// generation moves on. One key, whatever its groups hold; their records
/// leave by cleanup. A stream with no groups has no record to move.
fn observe(view: *View, e: *const Entry) ApplyError!void {
    const sk = observedStream(e) orelse return error.Halt;
    var sb: [13]u8 = undefined;
    // No groups of it: nothing to follow.
    var rec = ((try view.get(Streams_, recKey(&sb, sk))) orelse return).v;
    switch (typeOf(e)) {
        .stream_trim => {
            const cut = (try view.streams()).removedThrough(sk);
            if (!cut.greaterThan(floorOf(rec))) return;
            rec.floor_ms = cut.timestamp_ms;
            rec.floor_seq = cut.sequence;
        },
        .stream_delete => {
            rec.gen += 1;
            rec.floor_ms = 0;
            rec.floor_seq = 0;
        },
        else => return error.Halt,
    }
    try view.put(Streams_, recKey(&sb, sk), .{ .v = rec });
}

pub const machine: machine_mod.Machine = .{
    .name = "groups",
    .entry_types = &.{ .group_create, .group_delete, .group_deliver, .group_settle, .group_release, .group_extend, .group_expire, .group_clean },
    .families = &.{ machine_mod.info(Metas), machine_mod.info(Items), machine_mod.info(Dead), machine_mod.info(Ready), machine_mod.info(Deadlines), machine_mod.info(Streams_) },
    .keys = keys,
    .apply = apply,
    .observes = &.{ .stream_trim, .stream_delete },
    .observe_keys = observeKeys,
    .observe = observe,
};

// ─── Reads ───────────────────────────────────────────────────────────────

/// `k` in `ns`, as these families store it.
pub fn storeKey(buf: []u8, ns: NamespaceId, k: []const u8) []const u8 {
    return view_mod.idKey(buf, ns, k) catch unreachable;
}

// ─── Tests ───────────────────────────────────────────────────────────────

/// Group state on its own, for tests: a stream projection to read, and
/// entries applied through the machine as a shard applies them, one index
/// after another, kept as a log another can replay over the same stream.
pub const TestGroups = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    stream: stream_mod.StreamProjection,
    log: std.ArrayListUnmanaged(Logged) = .empty,
    index: u64 = 0,
    term: u64 = 1,

    const store_mod = @import("../contract/store.zig");
    const stream_mod = @import("../projection/stream.zig");

    /// What the log holds: a group entry, or a stream's change the replica
    /// makes to its own stream as the shard would apply it.
    const Op = union(enum) {
        entry,
        append: struct { stream: []const u8, at_ms: u64, records: u32 },
        trim: struct { stream: []const u8, up_to: StreamID, stamp: u64 },
        delete: struct { stream: []const u8, stamp: u64 },
    };
    const Logged = struct { etype: EntryType = .group_create, term: u64 = 0, index: u64 = 0, stamp: u64 = 0, payload: []u8 = &.{}, op: Op = .entry };

    pub fn init(allocator: std.mem.Allocator) TestGroups {
        var store = store_mod.Store.init(allocator);
        store.strict_keys = true;
        return .{ .store = store, .scratch = std.heap.ArenaAllocator.init(allocator), .stream = stream_mod.StreamProjection.init(allocator) };
    }

    pub fn deinit(self: *TestGroups) void {
        for (self.log.items) |l| self.store.allocator.free(l.payload);
        self.log.deinit(self.store.allocator);
        self.store.deinit();
        self.scratch.deinit();
        self.stream.deinit();
    }

    /// An append of `records` records to the default namespace's `stream`,
    /// stamped `ms`: its first record's id.
    pub fn append(self: *TestGroups, stream: []const u8, at_ms: u64, records: u32) !StreamID {
        try self.log.append(self.store.allocator, .{ .op = .{ .append = .{ .stream = stream, .at_ms = at_ms, .records = records } } });
        self.index += 1;
        return self.stream.appendToStreamAt(streamOf(.default, stream), self.index, 0, at_ms, records, 10 * records);
    }

    pub fn apply(self: *TestGroups, comptime etype: EntryType, body: anytype, stamp_ns: u64) !Result {
        const P = ge.Payload(@backingInt(etype));
        const payload = try self.store.allocator.alloc(u8, try P.encodedSize(body));
        _ = try P.encodeInto(payload, body);
        self.index += 1;
        try self.log.append(self.store.allocator, .{ .etype = etype, .term = self.term, .index = self.index, .stamp = stamp_ns, .payload = payload });
        return self.applyLogged(self.log.items[self.log.items.len - 1]);
    }

    fn applyLogged(self: *TestGroups, l: Logged) !Result {
        const e = entry_mod.buildEntry(l.etype, entry_mod.Flags.NONE, l.term, l.index, l.stamp, l.payload);
        var out: Result = .{};
        out.reset(&e);
        try machine_mod.applyEntryWith(&machine, &self.store, &self.scratch, .{ .proj = &self.stream }, &e, &out);
        return out;
    }

    /// A stream entry the stream applier applied, followed as the shard
    /// follows it.
    fn observed(self: *TestGroups, etype: EntryType, key: []const u8, value: []const u8, stamp_ns: u64) !void {
        var buf: [256]u8 = undefined;
        const cmd: entry_mod.CommandPayload = .{ .namespace_id = .default, .key_length = @intCast(key.len), .value_length = @intCast(value.len), .key = key, .value = value };
        const n = cmd.serialize(&buf).?;
        self.index += 1;
        const e = entry_mod.buildEntry(etype, entry_mod.Flags.NONE, self.term, self.index, stamp_ns, buf[0..n]);
        try machine_mod.observeEntry(&machine, &self.store, &self.scratch, .{ .proj = &self.stream }, &e);
    }

    /// The default namespace's `stream` trimmed through `up_to`, as the
    /// stream applier and then the group machine apply it.
    pub fn trim(self: *TestGroups, stream: []const u8, up_to: StreamID, stamp_ns: u64) !void {
        try self.log.append(self.store.allocator, .{ .op = .{ .trim = .{ .stream = stream, .up_to = up_to, .stamp = stamp_ns } } });
        const sk = streamOf(.default, stream);
        _ = self.stream.trimStream(sk, up_to);
        // As the stream handler writes a trim: the name hash, under the
        // entry's namespace.
        try self.observed(.stream_trim, "bound", sk[4..12], stamp_ns);
    }

    pub fn deleteStream(self: *TestGroups, stream: []const u8, stamp_ns: u64) !void {
        try self.log.append(self.store.allocator, .{ .op = .{ .delete = .{ .stream = stream, .stamp = stamp_ns } } });
        var nb: [stream_proj.MAX_NAME_KEY]u8 = undefined;
        _ = self.stream.deleteStream(streamOf(.default, stream), stream_proj.nameKey(&nb, .default, stream).?);
        try self.observed(.stream_delete, stream, "", stamp_ns);
    }

    /// One cleanup of the default namespace's group, as the duty proposes it.
    pub fn clean(self: *TestGroups, stream: []const u8, group: []const u8, stamp_ns: u64) !u64 {
        var ib: [ge.CleanItem.max_size]u8 = undefined;
        const named = try ge.CleanItem.encodeInto(&ib, .{ .ns = NamespaceId.default.int(), .scope = &scopeOf(.default, stream, group) });
        const r = try self.apply(.group_clean, ge.Clean.Value{ .items = .{ .bytes = named, .count = 1 } }, stamp_ns);
        var rd = r.reader();
        return rd.int(u64).?;
    }

    /// Replay this log into a fresh `other`: its stream's changes and its
    /// group entries, in order, at the same indexes.
    pub fn replayInto(self: *TestGroups, other: *TestGroups) !void {
        for (self.log.items) |l| switch (l.op) {
            .entry => {
                other.index = l.index;
                _ = try other.applyLogged(l);
            },
            .append => |a| _ = try other.append(a.stream, a.at_ms, a.records),
            .trim => |t| try other.trim(t.stream, t.up_to, t.stamp),
            .delete => |d| try other.deleteStream(d.stream, d.stamp),
        };
    }

    fn read(self: *TestGroups) view_mod.ReadView {
        return .{ .store = &self.store };
    }

    pub fn item(self: *TestGroups, stream: []const u8, group: []const u8, record: StreamID) ?Item.Value {
        var b: [4 + ID_LEN]u8 = undefined;
        const id = idOf(scopeOf(.default, stream, group), record);
        const got = self.read().get(Items, storeKey(&b, .default, &id)) catch unreachable;
        return if (got) |m| m.v else null;
    }

    pub fn meta(self: *TestGroups, stream: []const u8, group: []const u8) ?Meta.Value {
        var b: [4 + SCOPE_LEN]u8 = undefined;
        const scope = scopeOf(.default, stream, group);
        const got = self.read().get(Metas, storeKey(&b, .default, &scope)) catch unreachable;
        return if (got) |m| m.v else null;
    }

    pub fn count(self: *TestGroups, comptime F: type) usize {
        var n: usize = 0;
        var it = self.store.range(F.ID, "");
        while (it.next()) |_| n += 1;
        return n;
    }

    pub fn sums(self: *TestGroups) [6]u64 {
        var out: [6]u64 = undefined;
        inline for (.{ Metas, Items, Dead, Ready, Deadlines, Streams_ }, 0..) |F, i| out[i] = store_mod.checksum(&self.store, F.ID);
        return out;
    }

    /// The expire duty's tick at `stamp`: one expire naming every key past
    /// due, as the leader proposes it.
    pub fn duty(self: *TestGroups, stamp: u64) !Result {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        defer buf.deinit(self.store.allocator);
        var n: u32 = 0;
        var it = pastDue(self.read(), stamp);
        while (it.next()) |d| {
            var ib: [ge.ExpireItem.max_size]u8 = undefined;
            try buf.appendSlice(self.store.allocator, try ge.ExpireItem.encodeInto(&ib, .{ .ns = d.key.ns.int(), .id = d.key.id, .expiry_ns = d.key.expiry_ns }));
            n += 1;
        }
        return self.apply(.group_expire, ge.Expire.Value{ .items = .{ .bytes = buf.items, .count = n } }, stamp);
    }
};

const testing = std.testing;
const qe_caps = @import("../protocol/queue_entries.zig");
const ms = std.time.ns_per_ms;
const LEASE_MS = ae.MIN_LEASE_MS;
const LEASE = LEASE_MS * ms;
const NS: NamespaceId = .default;

/// The first number an answer holds.
fn answerInt(comptime T: type, r: Result) T {
    var rd = r.reader();
    return rd.int(T).?;
}

fn sid(t: u64, seq: u64) StreamID {
    return .{ .timestamp_ms = t, .sequence = seq };
}

/// A deliver's answer: the records, each with its deliveries.
const Delivered = struct {
    n: usize = 0,
    records: [ge.MAX_BATCH]StreamID = undefined,
    deliveries: [ge.MAX_BATCH]u32 = undefined,
    token: ae.Token.Value = .{ .term = 0, .index = 0 },
};

/// The ready records a leader would choose for "s"/"g" at `stamp`: due,
/// oldest first.
fn readyChoice(buf: []u8, t: *TestGroups, stamp: u64) ge.RecordIds.Value {
    var w: usize = 0;
    var n: u32 = 0;
    var pb: [SCOPE_LEN + 1]u8 = undefined;
    var kb: [4 + SCOPE_LEN + 1]u8 = undefined;
    var it = t.store.range(Ready.ID, storeKey(&kb, .default, readyPrefix(&pb, &scopeOf(.default, "s", "g"))));
    while (it.next()) |rec| {
        const k = parseReadyKey(rec.key()[4..], SCOPE_LEN).?;
        if (k.due_ns > stamp or n == ge.MAX_BATCH) break;
        const id = recordOf(k.id);
        w += (ge.RecordId.encodeInto(buf[w..], .{ .ms = id.timestamp_ms, .seq = id.sequence }) catch unreachable).len;
        n += 1;
    }
    return .{ .bytes = buf[0..w], .count = n };
}

fn deliver(t: *TestGroups, count: u32, max_bytes: u32, holder: []const u8, stamp: u64) !Delivered {
    var rb: [ge.MAX_BATCH * ge.RecordId.max_size]u8 = undefined;
    const r = try t.apply(.group_deliver, ge.Deliver.Value{ .ns = NS.int(), .stream = "s", .group = "g", .count = count, .max_bytes = max_bytes, .holder = holder, .lease_ms = LEASE_MS, .create = true, .ready = readyChoice(&rb, t, stamp), .through_ms = std.math.maxInt(u64), .through_seq = std.math.maxInt(u64), .max_held = qe_caps.DEFAULT_MAX_MESSAGES, .max_held_bytes = qe_caps.DEFAULT_MAX_BYTES, .shard_held_bytes = qe_caps.DEFAULT_SHARD_HELD_BYTES }, stamp);
    try testing.expect(r.refused() == null);
    var rd = r.reader();
    var d: Delivered = .{ .token = .{ .term = t.term, .index = t.index } };
    d.n = rd.int(u32).?;
    for (0..d.n) |i| {
        d.records[i] = sid(rd.int(u64).?, rd.int(u64).?);
        d.deliveries[i] = rd.int(u32).?;
    }
    return d;
}

fn itemsOf(buf: []u8, t: *TestGroups, records: []const StreamID) !ge.Items.Value {
    var w: usize = 0;
    for (records) |r| {
        const it = t.item("s", "g", r).?;
        w += (try ge.Item.encodeInto(buf[w..], .{ .ms = r.timestamp_ms, .seq = r.sequence, .deliveries = it.deliveries })).len;
    }
    return .{ .bytes = buf[0..w], .count = @intCast(records.len) };
}

/// A report's answer: how many applied, then each refused (record, why).
const Answer = struct {
    applied: u32,
    refused: [16]struct { record: StreamID, why: Why } = undefined,
    n: usize = 0,

    fn of(r: Result, extend: bool) Answer {
        var rd = r.reader();
        var a: Answer = .{ .applied = rd.int(u32).? };
        if (extend) _ = rd.int(u64).?;
        while (rd.int(u64)) |m| : (a.n += 1) a.refused[a.n] = .{ .record = sid(m, rd.int(u64).?), .why = @fromBackingInt(rd.int(u8).?) };
        return a;
    }
};

fn settle(t: *TestGroups, d: Delivered, records: []const StreamID, holder: []const u8, stamp: u64) !Answer {
    var buf: [512]u8 = undefined;
    const exp = if (t.item("s", "g", records[0])) |i| i.expiry_ns else 0;
    var w: usize = 0;
    for (records) |r| {
        const deliveries = for (d.records[0..d.n], d.deliveries[0..d.n]) |x, n| {
            if (x.eql(r)) break n;
        } else 1;
        w += (try ge.Item.encodeInto(buf[w..], .{ .ms = r.timestamp_ms, .seq = r.sequence, .deliveries = deliveries })).len;
    }
    const r = try t.apply(.group_settle, ge.Settle.Value{ .ns = NS.int(), .stream = "s", .group = "g", .holder = holder, .token = d.token, .expiry_ns = exp, .items = .{ .bytes = buf[0..w], .count = @intCast(records.len) } }, stamp);
    return Answer.of(r, false);
}

fn release(t: *TestGroups, d: Delivered, records: []const StreamID, dead: bool, stamp: u64) !Answer {
    var buf: [512]u8 = undefined;
    const items = try itemsOf(&buf, t, records);
    const exp = t.item("s", "g", records[0]).?.expiry_ns;
    const r = try t.apply(.group_release, ge.Release.Value{ .ns = NS.int(), .stream = "s", .group = "g", .holder = "c1", .token = d.token, .expiry_ns = exp, .items = items, .delay_ms = 0, .dead = dead, .@"error" = "boom", .free = false }, stamp);
    return Answer.of(r, false);
}

test "group machine: a deliver takes the stream's records past its cursor, one per record, across appends" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 3);
    _ = try t.append("s", 200, 2);
    const d = try deliver(&t, 4, 1 << 20, "c1", 10 * ms);
    try testing.expectEqual(@as(usize, 4), d.n);
    try testing.expect(d.records[0].eql(sid(100, 0)));
    try testing.expect(d.records[3].eql(sid(200, 0)));
    const m = t.meta("s", "g").?;
    try testing.expectEqual(@as(u64, 4), m.leased);
    try testing.expect(cursorOf(m).eql(sid(200, 0)));
    // The next takes the rest, then nothing.
    const e = try deliver(&t, 4, 1 << 20, "c2", 11 * ms);
    try testing.expectEqual(@as(usize, 1), e.n);
    try testing.expect(e.records[0].eql(sid(200, 1)));
    try testing.expectEqual(@as(usize, 0), (try deliver(&t, 4, 1 << 20, "c2", 12 * ms)).n);
}

test "group machine: a deliver takes no more than its leader chose: the ready records it names, the stream's through its bound" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    const first = try t.append("s", 100, 2);
    _ = try t.append("s", 200, 2);
    // The leader held only the first append: the second, appended since, waits.
    var none: [0]u8 = .{};
    const r = try t.apply(.group_deliver, ge.Deliver.Value{ .ns = NS.int(), .stream = "s", .group = "g", .count = 10, .max_bytes = 1 << 20, .holder = "c1", .lease_ms = LEASE_MS, .create = true, .ready = .{ .bytes = &none, .count = 0 }, .through_ms = first.timestamp_ms, .through_seq = first.sequence + 1, .max_held = qe_caps.DEFAULT_MAX_MESSAGES, .max_held_bytes = qe_caps.DEFAULT_MAX_BYTES, .shard_held_bytes = qe_caps.DEFAULT_SHARD_HELD_BYTES }, 10 * ms);
    try testing.expectEqual(@as(u32, 2), answerInt(u32, r));
    const d: Delivered = .{ .n = 2, .records = .{ first, sid(first.timestamp_ms, first.sequence + 1) } ++ @as([ge.MAX_BATCH - 2]StreamID, undefined), .deliveries = .{ 1, 1 } ++ @as([ge.MAX_BATCH - 2]u32, undefined), .token = .{ .term = t.term, .index = t.index } };

    // Both handed back free: ready again at once, no failure counted.
    var ib: [512]u8 = undefined;
    const items = try itemsOf(&ib, &t, d.records[0..2]);
    const exp = t.item("s", "g", first).?.expiry_ns;
    const back = try t.apply(.group_release, ge.Release.Value{ .ns = NS.int(), .stream = "s", .group = "g", .holder = "c1", .token = d.token, .expiry_ns = exp, .items = items, .delay_ms = 0, .dead = false, .@"error" = "", .free = true }, 11 * ms);
    try testing.expectEqual(@as(u32, 2), Answer.of(back, false).applied);
    try testing.expectEqual(@as(u32, 0), t.item("s", "g", first).?.failures);
    try testing.expectEqual(@as(u32, 0), t.item("s", "g", first).?.deliveries);
    try testing.expectEqual(Status.ready, t.item("s", "g", first).?.status);

    // A deliver naming one of the two ready records takes that one, then
    // fresh records, never the ready one it didn't name.
    var rb: [ge.RecordId.max_size]u8 = undefined;
    const one = try ge.RecordId.encodeInto(&rb, .{ .ms = first.timestamp_ms, .seq = first.sequence + 1 });
    const r2 = try t.apply(.group_deliver, ge.Deliver.Value{ .ns = NS.int(), .stream = "s", .group = "g", .count = 10, .max_bytes = 1 << 20, .holder = "c2", .lease_ms = LEASE_MS, .create = true, .ready = .{ .bytes = one, .count = 1 }, .through_ms = std.math.maxInt(u64), .through_seq = std.math.maxInt(u64), .max_held = qe_caps.DEFAULT_MAX_MESSAGES, .max_held_bytes = qe_caps.DEFAULT_MAX_BYTES, .shard_held_bytes = qe_caps.DEFAULT_SHARD_HELD_BYTES }, 12 * ms);
    try testing.expectEqual(@as(u32, 3), answerInt(u32, r2));
    try testing.expectEqual(Status.ready, t.item("s", "g", first).?.status);
    try testing.expectEqual(@as(u32, 1), t.item("s", "g", sid(first.timestamp_ms, first.sequence + 1)).?.deliveries);
}

test "group machine: a report to a group that isn't there is refused not_found, not answered record by record" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 2);
    const d = try deliver(&t, 2, 1 << 20, "c1", 10 * ms);
    var buf: [512]u8 = undefined;
    const items = try itemsOf(&buf, &t, d.records[0..2]);
    // Another group's name, and then this one's once it's deleted.
    for ([_][]const u8{ "other", "g" }) |group| {
        if (std.mem.eql(u8, group, "g")) _ = try t.apply(.group_delete, ge.Delete.Value{ .ns = NS.int(), .stream = "s", .group = "g" }, 20 * ms);
        const r = try t.apply(.group_settle, ge.Settle.Value{ .ns = NS.int(), .stream = "s", .group = group, .holder = "c1", .token = d.token, .expiry_ns = 0, .items = items }, 21 * ms);
        try testing.expectEqual(@import("../protocol/proto.zig").StatusCode.not_found, r.refused().?.status);
    }
}

test "group machine: acking one record of an append acks only that record" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 3);
    const d = try deliver(&t, 3, 1 << 20, "c1", 10 * ms);
    const a = try settle(&t, d, &.{sid(100, 1)}, "c1", 20 * ms);
    try testing.expectEqual(@as(u32, 1), a.applied);
    try testing.expect(t.item("s", "g", sid(100, 1)) == null);
    try testing.expectEqual(Status.leased, t.item("s", "g", sid(100, 0)).?.status);
    try testing.expectEqual(Status.leased, t.item("s", "g", sid(100, 2)).?.status);
    try testing.expectEqual(@as(u64, 2), t.meta("s", "g").?.leased);
    // A resend finds it gone; another holder holds nothing.
    try testing.expectEqual(Why.gone, (try settle(&t, d, &.{sid(100, 1)}, "c1", 21 * ms)).refused[0].why);
    try testing.expectEqual(Why.other_holder, (try settle(&t, d, &.{sid(100, 0)}, "c9", 22 * ms)).refused[0].why);
}

test "group machine: a nacked record is served first by the next deliver, and dead once its attempts are spent" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 2);
    var d = try deliver(&t, 1, 1 << 20, "c1", 10 * ms);
    for (0..ge.DEFAULT_MAX_ATTEMPTS - 1) |i| {
        _ = try release(&t, d, &.{sid(100, 0)}, false, (20 + 10 * i) * ms);
        // Ready again, it comes before the record past the cursor.
        d = try deliver(&t, 1, 1 << 20, "c1", (25 + 10 * i) * ms);
        try testing.expect(d.records[0].eql(sid(100, 0)));
    }
    _ = try release(&t, d, &.{sid(100, 0)}, false, 100 * ms);
    const it = t.item("s", "g", sid(100, 0)).?;
    try testing.expectEqual(Status.dead, it.status);
    try testing.expectEqual(@as(u32, ge.DEFAULT_MAX_ATTEMPTS), it.failures);
    try testing.expectEqual(@as(usize, 1), t.count(Dead));
    const m = t.meta("s", "g").?;
    try testing.expectEqual(@as(u64, 1), m.dead);
    try testing.expectEqual(@as(u64, 0), m.ready);
    // The dead one isn't delivered; the next is.
    try testing.expect((try deliver(&t, 2, 1 << 20, "c1", 110 * ms)).records[0].eql(sid(100, 1)));
}

test "group machine: a lease past its deadline ends at the duty's expire, and the record goes again" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 1);
    const d = try deliver(&t, 1, 1 << 20, "c1", 10 * ms);
    try testing.expectEqual(@as(usize, 1), t.count(Deadlines));
    var r = try t.duty(10 * ms + LEASE - 1);
    try testing.expectEqual(@as(u32, 0), answerInt(u32, r));
    r = try t.duty(10 * ms + LEASE);
    try testing.expectEqual(@as(u32, 1), answerInt(u32, r));
    const it = t.item("s", "g", sid(100, 0)).?;
    try testing.expectEqual(Status.ready, it.status);
    try testing.expectEqual(Ended.expired, it.ended);
    // The old holder's ack is stale; the next deliver takes it.
    try testing.expectEqual(Why.expired, (try settle(&t, d, &.{sid(100, 0)}, "c1", 10 * ms + LEASE + 1)).refused[0].why);
    const e = try deliver(&t, 1, 1 << 20, "c2", 10 * ms + LEASE + 2);
    try testing.expectEqual(@as(u32, 2), e.deliveries[0]);
}

test "group machine: a deliver's budget counts each append once, and the first record always goes" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 3);
    _ = try t.append("s", 200, 3);
    const bound: u32 = @intCast(stream_answer(&t, sid(100, 0)));
    // One append's bound: its three records fit, the next append's don't.
    try testing.expectEqual(@as(usize, 3), (try deliver(&t, 6, bound, "c1", 10 * ms)).n);
    // A budget below one append still takes one record.
    try testing.expectEqual(@as(usize, 3), (try deliver(&t, 6, 1, "c1", 11 * ms)).n);
}

fn stream_answer(t: *TestGroups, record: StreamID) usize {
    const s: Streams = .{ .proj = &t.stream };
    return s.appendOf(streamOf(.default, "s"), record).?.answer_bytes;
}

test "group machine: a create of an existing group with its settings changes nothing, with others is refused, a deliver to none without create is refused, and a delete drops its records" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 2);
    const c = try t.apply(.group_create, ge.Create.Value{ .ns = NS.int(), .stream = "s", .group = "g", .lease_ms = LEASE_MS, .max_attempts = 3 }, 1 * ms);
    try testing.expectEqual(@as(u8, 1), answerInt(u8, c));
    const again = try t.apply(.group_create, ge.Create.Value{ .ns = NS.int(), .stream = "s", .group = "g", .lease_ms = LEASE_MS, .max_attempts = 3 }, 2 * ms);
    try testing.expectEqual(@as(u8, 0), answerInt(u8, again));
    // Other settings: refused, naming the group's own; it keeps them.
    const other = try t.apply(.group_create, ge.Create.Value{ .ns = NS.int(), .stream = "s", .group = "g", .lease_ms = 600_000, .max_attempts = 3 }, 2 * ms);
    try testing.expect(std.mem.indexOf(u8, other.refused().?.refusal.message, "exists with a lease (ack timeout) of 5000 ms and 3 attempts") != null);
    try testing.expectEqual(@as(u64, LEASE_MS), t.meta("s", "g").?.lease_ms);
    const none = try t.apply(.group_deliver, ge.Deliver.Value{ .ns = NS.int(), .stream = "s", .group = "other", .count = 1, .max_bytes = 1 << 20, .holder = "c1", .lease_ms = LEASE_MS, .create = false, .ready = .{ .bytes = "", .count = 0 }, .through_ms = std.math.maxInt(u64), .through_seq = std.math.maxInt(u64), .max_held = qe_caps.DEFAULT_MAX_MESSAGES, .max_held_bytes = qe_caps.DEFAULT_MAX_BYTES, .shard_held_bytes = qe_caps.DEFAULT_SHARD_HELD_BYTES }, 3 * ms);
    try testing.expectEqual(@import("../protocol/proto.zig").StatusCode.not_found, none.refused().?.status);
    _ = try deliver(&t, 2, 1 << 20, "c1", 4 * ms);
    const del = try t.apply(.group_delete, ge.Delete.Value{ .ns = NS.int(), .stream = "s", .group = "g" }, 5 * ms);
    try testing.expectEqual(@as(u64, 2), answerInt(u64, del));
    _ = try t.clean("s", "g", 6 * ms);
    try testing.expect(t.meta("s", "g") == null);
    try testing.expectEqual(@as(usize, 0), t.count(Items));
    // Its leases' keys are the duty's to drop.
    _ = try t.duty(4 * ms + LEASE);
    try testing.expectEqual(@as(usize, 0), t.count(Deadlines));
}

test "group machine: a stream's record is kept only while groups of it are" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 1);
    _ = try t.append("s", 200, 2);
    // No groups: a trim keeps nothing.
    try t.trim("s", sid(100, 0), 5 * ms);
    try testing.expectEqual(@as(usize, 0), t.count(Streams_));
    // Made at the stream's trim so far.
    _ = try t.apply(.group_create, ge.Create.Value{ .ns = NS.int(), .stream = "s", .group = "g", .lease_ms = LEASE_MS, .max_attempts = 3 }, 6 * ms);
    _ = try t.apply(.group_create, ge.Create.Value{ .ns = NS.int(), .stream = "s", .group = "h", .lease_ms = LEASE_MS, .max_attempts = 3 }, 6 * ms);
    try testing.expectEqual(@as(usize, 1), t.count(Streams_));
    const d = try deliver(&t, 5, 1 << 20, "c1", 10 * ms);
    try testing.expectEqual(@as(usize, 2), d.n);
    try testing.expect(d.records[0].eql(sid(200, 0)));
    // One group's delete, with nothing held: the other keeps the record.
    _ = try t.apply(.group_delete, ge.Delete.Value{ .ns = NS.int(), .stream = "s", .group = "h" }, 11 * ms);
    try testing.expectEqual(@as(usize, 1), t.count(Streams_));
    // The stream's delete, then the last group's cleanup: gone.
    try t.deleteStream("s", 12 * ms);
    try testing.expectEqual(@as(u64, 2), try t.clean("s", "g", 13 * ms));
    try testing.expectEqual(@as(usize, 0), t.count(Metas));
    try testing.expectEqual(@as(usize, 0), t.count(Streams_));
}

test "group machine: a replica that replays the log, the stream's trims and deletes with it, holds the same state" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 3);
    const d = try deliver(&t, 2, 1 << 20, "c1", 10 * ms);
    _ = try release(&t, d, &.{sid(100, 0)}, false, 20 * ms);
    _ = try settle(&t, d, &.{sid(100, 1)}, "c1", 21 * ms);
    _ = try deliver(&t, 3, 1 << 20, "c2", 30 * ms);
    _ = try t.duty(30 * ms + LEASE);
    // A trim and its cleanup, then the stream's delete and its.
    try t.trim("s", sid(100, 1), 40 * ms);
    _ = try t.clean("s", "g", 41 * ms);
    _ = try t.append("s", 200, 2);
    try t.deleteStream("s", 50 * ms);
    _ = try t.clean("s", "g", 51 * ms);
    var other = TestGroups.init(testing.allocator);
    defer other.deinit();
    try t.replayInto(&other);
    try testing.expectEqual(t.sums(), other.sums());
}

test "group machine: a trim's records are never handed out, and cleanup moves them to trimmed, a bounded few at a time" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 3);
    _ = try t.append("s", 200, 3);
    const d = try deliver(&t, 6, 1 << 20, "c1", 10 * ms);
    // The first append's three are nacked: ready again.
    _ = try release(&t, d, &.{ sid(100, 0), sid(100, 1), sid(100, 2) }, false, 20 * ms);
    try t.trim("s", sid(100, 0), 30 * ms);
    // At or below the floor: still counted until cleaned, but never delivered.
    try testing.expectEqual(@as(u64, 3), t.meta("s", "g").?.ready);
    try testing.expectEqual(@as(usize, 0), (try deliver(&t, 6, 1 << 20, "c2", 40 * ms)).n);
    try testing.expectEqual(@as(u64, 3), try t.clean("s", "g", 50 * ms));
    const m = t.meta("s", "g").?;
    try testing.expectEqual(@as(u64, 0), m.ready);
    try testing.expectEqual(@as(u64, 3), m.leased);
    try testing.expectEqual(@as(u64, 3), m.trimmed);
    try testing.expectEqual(@as(u64, 0), try t.clean("s", "g", 60 * ms));
}

test "group machine: a stream's delete leaves its groups gone at once, cleaned out a bounded few at a time, and a group of that name can't come back until they are" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, ge.CLEAN_RECORDS + 4);
    var left: u32 = ge.CLEAN_RECORDS + 4;
    while (left > 0) {
        const n = (try deliver(&t, @min(left, ge.MAX_BATCH), 1 << 30, "c1", 10 * ms)).n;
        left -= @intCast(n);
    }
    try t.deleteStream("s", 20 * ms);
    // Gone: a read of it, or a create, waits for the cleanup.
    const again = try t.apply(.group_deliver, ge.Deliver.Value{ .ns = NS.int(), .stream = "s", .group = "g", .count = 1, .max_bytes = 1 << 20, .holder = "c1", .lease_ms = LEASE_MS, .create = true, .ready = .{ .bytes = "", .count = 0 }, .through_ms = std.math.maxInt(u64), .through_seq = std.math.maxInt(u64), .max_held = qe_caps.DEFAULT_MAX_MESSAGES, .max_held_bytes = qe_caps.DEFAULT_MAX_BYTES, .shard_held_bytes = qe_caps.DEFAULT_SHARD_HELD_BYTES }, 30 * ms);
    try testing.expectEqual(@import("../protocol/proto.zig").StatusCode.unavailable, again.refused().?.status);
    try testing.expectEqual(@as(u64, ge.CLEAN_RECORDS), try t.clean("s", "g", 40 * ms));
    try testing.expect(t.meta("s", "g") != null);
    try testing.expectEqual(@as(u64, 4), try t.clean("s", "g", 50 * ms));
    try testing.expect(t.meta("s", "g") == null);
    try testing.expectEqual(@as(usize, 0), t.count(Items));
    // A stream of that name again: a new group, from its first record.
    _ = try t.append("s", 100, 1);
    const fresh = try deliver(&t, 4, 1 << 20, "c2", 60 * ms);
    try testing.expectEqual(@as(usize, 1), fresh.n);
    try testing.expect(fresh.records[0].eql(sid(100, 0)));
}

test "group machine: a group's delete is gone at once, its records cleaned after" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 2);
    _ = try deliver(&t, 2, 1 << 20, "c1", 10 * ms);
    const del = try t.apply(.group_delete, ge.Delete.Value{ .ns = NS.int(), .stream = "s", .group = "g" }, 20 * ms);
    try testing.expectEqual(@as(u64, 2), answerInt(u64, del));
    const none = try t.apply(.group_delete, ge.Delete.Value{ .ns = NS.int(), .stream = "s", .group = "g" }, 21 * ms);
    try testing.expect(none.refused() != null);
    try testing.expectEqual(@as(u64, 2), try t.clean("s", "g", 30 * ms));
    try testing.expect(t.meta("s", "g") == null);
}

test "group machine: a group takes new records only within its caps, and a settle gives their bytes back" {
    var t = TestGroups.init(testing.allocator);
    defer t.deinit();
    _ = try t.append("s", 100, 3);
    var v: ge.Deliver.Value = .{ .ns = NS.int(), .stream = "s", .group = "g", .count = 3, .max_bytes = 1 << 20, .holder = "c1", .lease_ms = LEASE_MS, .create = true, .ready = .{ .bytes = "", .count = 0 }, .through_ms = std.math.maxInt(u64), .through_seq = std.math.maxInt(u64), .max_held = 2, .max_held_bytes = qe_caps.DEFAULT_MAX_BYTES, .shard_held_bytes = qe_caps.DEFAULT_SHARD_HELD_BYTES };
    const first = try t.apply(.group_deliver, v, 10 * ms);
    try testing.expectEqual(@as(u32, 2), answerInt(u32, first));
    try testing.expectEqual(2 * heldBy(0), t.meta("s", "g").?.held_bytes);
    // Full, nothing ready: refused, and the reader is told why.
    v.count = 1;
    const full = try t.apply(.group_deliver, v, 11 * ms);
    try testing.expect(std.mem.indexOf(u8, full.refused().?.refusal.message, "group 'g' on stream 's' is full: it holds 2 records (at most 2)") != null);
    // An ack makes room, and gives its bytes back.
    var d: Delivered = .{ .token = .{ .term = t.term, .index = t.index - 1 }, .n = 1 };
    d.records[0] = sid(100, 0);
    d.deliveries[0] = 1;
    _ = try settle(&t, d, &.{sid(100, 0)}, "c1", 12 * ms);
    try testing.expectEqual(heldBy(0), t.meta("s", "g").?.held_bytes);
    try testing.expectEqual(@as(u32, 1), answerInt(u32, try t.apply(.group_deliver, v, 13 * ms)));
}
