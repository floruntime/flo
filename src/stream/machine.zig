//! Streams on the contract: which streams exist, their names, settings and
//! counters, and each append's place in the log. The appended payloads stay
//! in the log; everything else is here, in apply, so a snapshot carries it
//! and every replica holds the same.
//!
//! - `stream.meta`: each stream, keyed (ns id, name hash): its name and
//!   settings, its last id, its floor (the last record a trim cut), its
//!   generation (moved on by a delete), and its running totals. A delete
//!   of a stream whose groups still count leaves it as a tombstone
//!   (`exists` false) holding the generation, so those groups read as gone.
//! - `stream.record`: each append, keyed (ns id, name hash, first record
//!   id): where its payload is (log index and part), its partition, how
//!   many records and bytes it holds, and the totals appended before it,
//!   so counts above any floor are two seeks, not a walk.
//! - `stream.retention`: each stream with a retention policy, so the
//!   retention duty visits only those.
//! - `stream.reclaim`: each stream holding appends at or below its floor
//!   that a trim left for later: a trim is whole at once (the floor moves,
//!   and reads and groups go by it), and its appends are removed a bounded
//!   number at a time, by that trim and then by the retention duty's trims
//!   of the stream to its floor.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const schema = @import("../protocol/schema.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const router = @import("../node/router.zig");
const proto = @import("../protocol/proto.zig");
const stream_mod = @import("../projection/stream.zig");
const MAX_QUALIFIED_KEY = @import("../protocol/limits.zig").MAX_QUALIFIED_KEY;

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
pub const StreamID = stream_mod.StreamID;
pub const StreamKey = stream_mod.StreamKey;
pub const StreamRecord = stream_mod.StreamRecord;

pub const NamespaceId = router.NamespaceId;

pub const Meta = Record(&.{
    .{ .name = "name", .T = schema.Bytes(MAX_QUALIFIED_KEY) },
    .{ .name = "partition_count", .T = Int(u32) },
    // 0 is no limit.
    .{ .name = "retention_age_s", .T = Int(u64) },
    .{ .name = "retention_count", .T = Int(u64) },
    // The stamp of the create or first append that made it.
    .{ .name = "created_ns", .T = Int(u64) },
    // Its settings were asked for, by a create or an alter, rather than
    // implied by an append: a create of it must ask for the same ones.
    .{ .name = "explicit", .T = schema.Bool },
    // False for a tombstone: a deleted stream whose groups still count.
    .{ .name = "exists", .T = schema.Bool },
    // Moved on by each delete: a group made at another is gone.
    .{ .name = "gen", .T = Int(u64) },
    // The last record id handed out; ids rise from it.
    .{ .name = "last_ms", .T = Int(u64) },
    .{ .name = "last_seq", .T = Int(u64) },
    // The last record a trim cut, never lowered while the stream exists:
    // every record at or below it is gone, reclaimed or not.
    .{ .name = "floor_ms", .T = Int(u64) },
    .{ .name = "floor_seq", .T = Int(u64) },
    // Every record and byte ever appended to it, since it was made.
    .{ .name = "appended_records", .T = Int(u64) },
    .{ .name = "appended_bytes", .T = Int(u64) },
});

/// An append's place: fixed size, no strings.
pub const Rec = Record(&.{
    .{ .name = "ual_index", .T = Int(u64) },
    // 0 for a `stream_append` itself, `m + 1` for message `m` of an inbound.
    .{ .name = "part", .T = Int(u8) },
    .{ .name = "partition", .T = Int(u32) },
    .{ .name = "record_count", .T = Int(u32) },
    .{ .name = "byte_len", .T = Int(u32) },
    // What the stream had appended before this append.
    .{ .name = "records_before", .T = Int(u64) },
    .{ .name = "bytes_before", .T = Int(u64) },
});

pub const Metas = view_mod.Family(47, "stream.meta", .id, Stored(Meta));
pub const Retained = view_mod.Family(48, "stream.retention", .id, delivery.Empty);
pub const Records = view_mod.Family(49, "stream.record", .id, Stored(Rec));
pub const Reclaim = view_mod.Family(53, "stream.reclaim", .id, delivery.Empty);

/// The most appends one trim removes; past it, the trim is whole all the
/// same (the floor moved), and the stream is marked in `stream.reclaim`
/// for the retention duty, which trims it to its floor until none remain.
pub const MAX_TRIM_APPENDS: usize = 1024;

/// How many groups count the stream: a tombstone is forgotten only at 0.
pub fn groupsOf(read: ReadView, sk: StreamKey) u64 {
    var sb: [13]u8 = undefined;
    const r = (read.get(gm.Streams_, gm.recKey(&sb, sk)) catch return 0) orelse return 0;
    return r.v.groups;
}

/// The appends a stream still holds at or below `floor`, counting at most
/// `cap`: what the retention duty has left to remove.
pub fn heldThrough(read: ReadView, sk: StreamKey, floor: StreamID, cap: usize) usize {
    var it = read.range(Records, &sk);
    var n: usize = 0;
    while (n < cap) : (n += 1) {
        const item = (it.next() catch null) orelse break;
        if (idOfKey(item.key).greaterThan(floor)) break;
    }
    return n;
}

/// A `stream_config` entry's value: a create or an alter. In an alter a 0
/// keeps what the stream has.
pub const StreamConfig = struct {
    op: enum(u8) { create, alter },
    partition_count: u32,
    retention_age_s: u64,
    retention_count: u64,

    pub const SIZE = 1 + 4 + 8 + 8;

    pub fn encode(c: StreamConfig, out: *[SIZE]u8) void {
        out[0] = @backingInt(c.op);
        std.mem.writeInt(u32, out[1..5], c.partition_count, .little);
        std.mem.writeInt(u64, out[5..13], c.retention_age_s, .little);
        std.mem.writeInt(u64, out[13..21], c.retention_count, .little);
    }

    pub fn decode(bytes: []const u8) ?StreamConfig {
        if (bytes.len != SIZE or bytes[0] > 1) return null;
        return .{
            .op = @fromBackingInt(@intCast(bytes[0])),
            .partition_count = std.mem.readInt(u32, bytes[1..5], .little),
            .retention_age_s = std.mem.readInt(u64, bytes[5..13], .little),
            .retention_count = std.mem.readInt(u64, bytes[13..21], .little),
        };
    }
};

pub const NO_STREAM = "stream not found";
pub const SETTINGS_DIFFER = "conflict: the stream exists with other settings; change them with stream alter";
pub const NAME_TAKEN = "conflict: another stream's name hashes as this one's; choose another name";
pub const NOT_EMPTY = "stream is not empty; pass --force to delete";

/// A stream's key within its namespace: its name's hash, as its records'
/// `StreamKey` holds it.
pub fn hashOf(name: []const u8) [8]u8 {
    const k = router.nsNameKey(.invalid, name);
    return k[4..12].*;
}

/// `k` in `ns`, as these families store it.
pub fn storeKey(buf: []u8, ns: NamespaceId, k: []const u8) []const u8 {
    return view_mod.idKey(buf, ns, k) catch unreachable;
}

pub fn hasRetention(m: Meta.Value) bool {
    return m.retention_age_s > 0 or m.retention_count > 0;
}

pub fn floorOf(m: Meta.Value) StreamID {
    return .{ .timestamp_ms = m.floor_ms, .sequence = m.floor_seq };
}

pub fn lastOf(m: Meta.Value) StreamID {
    return .{ .timestamp_ms = m.last_ms, .sequence = m.last_seq };
}

/// An append's key: its stream's key, then its first record's id.
pub const RECORD_KEY_LEN = 12 + 16;
pub fn recordKey(buf: *[RECORD_KEY_LEN]u8, sk: StreamKey, id: StreamID) []const u8 {
    buf[0..12].* = sk;
    std.mem.writeInt(u64, buf[12..20], id.timestamp_ms, .big);
    std.mem.writeInt(u64, buf[20..28], id.sequence, .big);
    return buf;
}

pub fn idOfKey(k: []const u8) StreamID {
    return .{ .timestamp_ms = std.mem.readInt(u64, k[12..20], .big), .sequence = std.mem.readInt(u64, k[20..28], .big) };
}

/// The id right after `id`, for a bound that takes `id` itself.
/// The greatest id has none: it stays, and no append's first id is it.
fn successor(id: StreamID) StreamID {
    if (id.sequence != std.math.maxInt(u64)) return .{ .timestamp_ms = id.timestamp_ms, .sequence = id.sequence + 1 };
    if (id.timestamp_ms == std.math.maxInt(u64)) return id;
    return .{ .timestamp_ms = id.timestamp_ms + 1, .sequence = 0 };
}

pub fn recordOf(k: []const u8, v: Rec.Value) StreamRecord {
    return .{ .id = idOfKey(k), .ual_index = v.ual_index, .part = v.part, .partition_index = v.partition, .record_count = v.record_count, .byte_len = v.byte_len };
}

/// The ids an append of `n` records takes after `last`, at `ts_ms`: as
/// `StreamIdGenerator.nextBatchAt` hands them out.
pub fn idsFor(last: StreamID, ts_ms: u64, n: u32) struct { first: StreamID, last: StreamID } {
    const first: StreamID = if (ts_ms > last.timestamp_ms) .{ .timestamp_ms = ts_ms, .sequence = 0 } else successor(last);
    return .{ .first = first, .last = .{ .timestamp_ms = first.timestamp_ms, .sequence = first.sequence + n - 1 } };
}

// ─── Reads ───────────────────────────────────────────────────────────────

/// The stream `name` in `ns`, if it exists. A record whose name differs is
/// another stream's, whose name hashes alike: not this one.
pub fn metaOf(read: ReadView, ns: NamespaceId, name: []const u8) ?Meta.Value {
    const hash = hashOf(name);
    var kb: [12]u8 = undefined;
    const got = (read.get(Metas, storeKey(&kb, ns, &hash)) catch return null) orelse return null;
    if (!got.v.exists) return null;
    return if (std.mem.eql(u8, got.v.name, name)) got.v else null;
}

/// A stream's meta by its key, tombstone or not.
pub fn metaAt(read: ReadView, sk: StreamKey) ?Meta.Value {
    const got = (read.get(Metas, &sk) catch return null) orelse return null;
    return got.v;
}

/// A stream's partition count: 1 for one that doesn't exist.
pub fn partitionCount(read: ReadView, ns: NamespaceId, name: []const u8) u32 {
    return if (metaOf(read, ns, name)) |m| @max(1, m.partition_count) else 1;
}

/// The first append past `after` (its first id after it), and what the
/// stream appended before it; null when none is.
fn firstPast(read: ReadView, sk: StreamKey, after: StreamID) ?struct { key: []const u8, v: Rec.Value } {
    var kb: [RECORD_KEY_LEN]u8 = undefined;
    var it = read.rangeAfter(Records, &sk, recordKey(&kb, sk, after));
    const item = (it.next() catch return null) orelse return null;
    return .{ .key = item.key, .v = item.value.v };
}

/// The records and bytes a stream holds above `floor`.
pub fn countAbove(read: ReadView, sk: StreamKey, m: Meta.Value, floor: StreamID) struct { records: u64, bytes: u64 } {
    const first = firstPast(read, sk, floor) orelse return .{ .records = 0, .bytes = 0 };
    return .{ .records = m.appended_records - first.v.records_before, .bytes = m.appended_bytes - first.v.bytes_before };
}

/// The append holding record `id` (its first id at or before it, its last
/// at or after), if the stream holds one and it is above the floor.
pub fn appendOf(read: ReadView, sk: StreamKey, m: Meta.Value, id: StreamID) ?StreamRecord {
    if (!id.greaterThan(floorOf(m))) return null;
    var kb: [RECORD_KEY_LEN]u8 = undefined;
    var it = read.rangeBefore(Records, &sk, recordKey(&kb, sk, successor(id)));
    const item = (it.next() catch return null) orelse return null;
    const rec = recordOf(item.key, item.value.v);
    return if (stream_mod.lastRecordOf(rec).lessThan(id)) null else rec;
}

/// A stream's appends holding records after `after`, oldest first, none at
/// or below the floor: the append holding `after` first when it holds
/// records past it, so a cursor inside an append resumes in it.
pub const Appends = struct {
    read: ReadView,
    sk: StreamKey,
    head: ?StreamRecord,
    after_key: [RECORD_KEY_LEN]u8,
    it: ?view_mod.ReadRange(Records) = null,

    pub fn init(read: ReadView, sk: StreamKey, m: Meta.Value, after: StreamID) Appends {
        const floor = floorOf(m);
        const from = if (floor.greaterThan(after)) floor else after;
        var a: Appends = .{ .read = read, .sk = sk, .head = null, .after_key = undefined };
        _ = recordKey(&a.after_key, sk, from);
        if (appendOf(read, sk, m, from)) |rec| if (stream_mod.lastRecordOf(rec).greaterThan(from)) {
            a.head = rec;
            _ = recordKey(&a.after_key, sk, rec.id);
        };
        return a;
    }

    pub fn next(self: *Appends) ?StreamRecord {
        if (self.head) |h| {
            self.head = null;
            return h;
        }
        if (self.it == null) self.it = self.read.rangeAfter(Records, &self.sk, &self.after_key);
        const item = (self.it.?.next() catch return null) orelse return null;
        return recordOf(item.key, item.value.v);
    }
};

// ─── Keys ────────────────────────────────────────────────────────────────

fn typeOf(e: *const Entry) EntryType {
    return @fromBackingInt(@intCast(e.header.entry_type));
}

fn commandOf(e: *const Entry) ?entry_mod.CommandPayload {
    return entry_mod.CommandPayload.deserialize(e.payload);
}

/// The stream an entry names: a trim's key is its bound and its value the
/// name's hash; a clean's value is the hash, and a forget's the hash and
/// the generation it forgets; every other names it by its key.
fn streamOfEntry(e: *const Entry) ?struct { ns: NamespaceId, hash: [8]u8 } {
    const cmd = commandOf(e) orelse return null;
    if (cmd.namespace_id == .invalid) return null;
    switch (typeOf(e)) {
        .stream_trim, .stream_clean => {
            if (cmd.value.len != 8) return null;
            return .{ .ns = cmd.namespace_id, .hash = cmd.value[0..8].* };
        },
        .stream_forget => {
            if (cmd.value.len != 16) return null;
            return .{ .ns = cmd.namespace_id, .hash = cmd.value[0..8].* };
        },
        else => {},
    }
    if (cmd.key.len == 0) return null;
    return .{ .ns = cmd.namespace_id, .hash = hashOf(cmd.key) };
}

fn streamKeys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const s = streamOfEntry(e) orelse return;
    try out.addIn(Metas.ID, s.ns, &s.hash);
    try out.addIn(Retained.ID, s.ns, &s.hash);
    try out.addIn(Reclaim.ID, s.ns, &s.hash);
    out.addRangeIn(Records.ID, s.ns, &s.hash) catch |err| switch (err) {
        error.NoNamespace => return,
        else => |o| return o,
    };
    // A forget reads whether groups still count its stream.
    if (typeOf(e) == .stream_forget) {
        var sb: [13]u8 = undefined;
        try out.add(gm.Streams_.ID, "", gm.recKey(&sb, s.ns.bytes() ++ s.hash)[1..]);
    }
}

// ─── Apply ───────────────────────────────────────────────────────────────

const gm = @import("group_machine.zig");

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    // Every stream is in a namespace: an entry naming none touches nothing.
    if (streamOfEntry(e) == null) return out.refuse(.bad_request, .malformed, "bad request: a stream entry that names no stream in a namespace");
    return switch (typeOf(e)) {
        .stream_config => configApply(view, e, out),
        .stream_append => appendApply(view, e, out),
        .stream_trim => trimApply(view, e, out),
        .stream_delete => deleteApply(view, e, out),
        .stream_clean => cleanApply(view, e, out),
        .stream_forget => forgetApply(view, e, out),
        else => error.Halt,
    };
}

fn inNs(view: *View, ns: NamespaceId, k: []const u8) ApplyError![]const u8 {
    const buf = try view.arena.alloc(u8, 4 + k.len);
    return view_mod.idKey(buf, ns, k) catch unreachable;
}

fn putMeta(view: *View, sk: StreamKey, m: Meta.Value) ApplyError!void {
    const key = try view.arena.dupe(u8, &sk);
    try view.put(Metas, key, .{ .v = m });
    if (m.exists and hasRetention(m)) try view.put(Retained, key, .{}) else try view.delete(Retained, key);
}

/// A stream made by its first append or a create: no settings asked. Made
/// again over a tombstone, it keeps only the tombstone's generation, last
/// id, floor and running totals: its ids continue past the old stream's,
/// so every old record is at or below the floor (read as gone, and
/// removed by the retention duty) and nothing above it, the new stream's
/// records, can share a key with one.
fn newMeta(name: []const u8, stamp: u64, tomb: ?Meta.Value) Meta.Value {
    var m: Meta.Value = .{ .name = name, .partition_count = 1, .retention_age_s = 0, .retention_count = 0, .created_ns = stamp, .explicit = false, .exists = true, .gen = 0, .last_ms = 0, .last_seq = 0, .floor_ms = 0, .floor_seq = 0, .appended_records = 0, .appended_bytes = 0 };
    if (tomb) |t| {
        m.gen = t.gen;
        m.last_ms = t.last_ms;
        m.last_seq = t.last_seq;
        m.floor_ms = t.floor_ms;
        m.floor_seq = t.floor_seq;
        m.appended_records = t.appended_records;
        m.appended_bytes = t.appended_bytes;
    }
    return m;
}

/// A create of a stream with settings (from a create or an alter) is
/// answered ok if it asks for the same ones, as a retry does, and refused
/// if not: changing them is alter's. A create of one an append implied,
/// never altered, gives it the settings asked.
fn configApply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    const cmd = commandOf(e) orelse return error.Halt;
    if (cmd.namespace_id == .invalid or cmd.key.len == 0) return error.Halt;
    const c = StreamConfig.decode(cmd.value) orelse return error.Halt;
    const sk: StreamKey = cmd.namespace_id.bytes() ++ hashOf(cmd.key);
    const found: ?Meta.Value = if (try view.get(Metas, &sk)) |f| f.v else null;
    const live: ?Meta.Value = if (found) |f| (if (f.exists) f else null) else null;
    if (live) |f| if (!std.mem.eql(u8, f.name, cmd.key)) return out.refuse(.conflict, .unclassified, NAME_TAKEN);
    const meta: Meta.Value = switch (c.op) {
        .create => blk: {
            if (live) |m| if (m.explicit) {
                if (m.partition_count == @max(1, c.partition_count) and m.retention_age_s == c.retention_age_s and m.retention_count == c.retention_count) return;
                return out.refuse(.conflict, .unclassified, SETTINGS_DIFFER);
            };
            var m = live orelse newMeta(cmd.key, e.header.timestamp_ns, found);
            m.partition_count = @max(1, c.partition_count);
            m.retention_age_s = c.retention_age_s;
            m.retention_count = c.retention_count;
            m.explicit = true;
            break :blk m;
        },
        .alter => blk: {
            var m = live orelse return out.refuse(.not_found, .not_found, NO_STREAM);
            if (c.retention_age_s > 0) m.retention_age_s = c.retention_age_s;
            if (c.retention_count > 0) m.retention_count = c.retention_count;
            // It has settings now: a create must ask for them, as of one
            // a create made, or it would drop them.
            m.explicit = true;
            break :blk m;
        },
    };
    try putMeta(view, sk, meta);
}

/// Why an append's payload can't apply, or null: a channel counts one its
/// sender built wrong.
pub fn malformed(e: *const Entry) ?[]const u8 {
    return stream_mod.appendRefusal(e.payload);
}

/// One append: ids after the stream's last, from the entry's stamp, and its
/// place in the log. A name whose hash another stream's record holds lands
/// in that stream, as one `StreamKey` names both. Answers its first id
/// (seq, then ms).
fn appendApply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    if (malformed(e)) |why| return out.refuse(.bad_request, .malformed, why);
    const cmd = commandOf(e).?;
    const sk: StreamKey = cmd.namespace_id.bytes() ++ hashOf(cmd.key);
    const found: ?Meta.Value = if (try view.get(Metas, &sk)) |f| f.v else null;
    var m = if (found) |f| (if (f.exists) f else newMeta(cmd.key, e.header.timestamp_ns, f)) else newMeta(cmd.key, e.header.timestamp_ns, null);
    const av = stream_mod.decodeAppendValue(cmd.value);
    const n = @max(1, stream_mod.batchRecordCount(av.payload));
    const ids = idsFor(lastOf(m), e.header.timestamp_ns / std.time.ns_per_ms, n);
    const part: u8 = if (view.message) |msg| stream_mod.partOf(msg.ordinal) else 0;
    var kb: [RECORD_KEY_LEN]u8 = undefined;
    const key = try view.arena.dupe(u8, recordKey(&kb, sk, ids.first));
    try view.put(Records, key, .{ .v = .{ .ual_index = e.header.index, .part = part, .partition = av.partition_index, .record_count = n, .byte_len = @intCast(av.payload.len), .records_before = m.appended_records, .bytes_before = m.appended_bytes } });
    m.last_ms = ids.last.timestamp_ms;
    m.last_seq = ids.last.sequence;
    m.appended_records += n;
    m.appended_bytes += av.payload.len;
    try putMeta(view, sk, m);
    out.putInt(u64, ids.first.sequence);
    out.putInt(u64, ids.first.timestamp_ms);
}

/// A trim: the floor moves at once to the last record of the last append
/// the bound reaches (an age resolves to an id by the entry's stamp, so
/// finding the cut needs no walk), and reads and groups go by it from here.
/// Its appends are then removed, at most `MAX_TRIM_APPENDS`; the rest are
/// marked in `stream.reclaim` for the retention duty. Answers the records it cut and the first
/// record left (seq, 0 for none), whole however many remain to remove.
fn trimApply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    const cmd = commandOf(e) orelse return error.Halt;
    if (cmd.value.len != 8) return error.Halt;
    const sk: StreamKey = cmd.namespace_id.bytes() ++ cmd.value[0..8].*;
    const bound = stream_mod.TrimBound.decode(cmd.key) orelse return error.Halt;
    const found: ?Meta.Value = if (try view.get(Metas, &sk)) |f| f.v else null;
    var m = (if (found) |f| (if (f.exists) f else null) else null) orelse {
        out.putInt(u64, 0);
        out.putInt(u64, 0);
        return;
    };
    const old_floor = floorOf(m);
    var new_floor = old_floor;
    if (bound.resolve(e.header.timestamp_ns)) |to| {
        var kb: [RECORD_KEY_LEN]u8 = undefined;
        if (try view.lastBefore(Records, &sk, recordKey(&kb, sk, successor(to)))) |cut| {
            const last = stream_mod.lastRecordOf(recordOf(cut.key, cut.value.v));
            if (last.greaterThan(new_floor)) new_floor = last;
        }
    }
    const before = try countAboveView(view, sk, m, old_floor);
    const after = try countAboveView(view, sk, m, new_floor);
    m.floor_ms = new_floor.timestamp_ms;
    m.floor_seq = new_floor.sequence;
    try putMeta(view, sk, m);
    _ = try reclaim(view, sk, new_floor, false);
    out.putInt(u64, before.records - after.records);
    out.putInt(u64, if (after.first) |f| f.sequence else 0);
}

fn countAboveView(view: *View, sk: StreamKey, m: Meta.Value, floor: StreamID) ApplyError!struct { records: u64, first: ?StreamID } {
    var kb: [RECORD_KEY_LEN]u8 = undefined;
    var it = try view.rangeAfter(Records, &sk, recordKey(&kb, sk, floor));
    const item = (try it.next()) orelse return .{ .records = 0, .first = null };
    return .{ .records = m.appended_records - item.value.v.records_before, .first = idOfKey(item.key) };
}

/// Remove up to `MAX_TRIM_APPENDS` of a stream's appends at or below
/// `floor`, oldest first, and mark it in `stream.reclaim` while any remain,
/// or for a tombstone (`keep_mark`) until it is forgotten. How many it
/// removed.
fn reclaim(view: *View, sk: StreamKey, floor: StreamID, keep_mark: bool) ApplyError!usize {
    var it = try view.range(Records, &sk);
    var n: usize = 0;
    var more = false;
    var doomed: [MAX_TRIM_APPENDS][]const u8 = undefined;
    while (try it.next()) |item| {
        if (idOfKey(item.key).greaterThan(floor)) break;
        if (n == MAX_TRIM_APPENDS) {
            more = true;
            break;
        }
        doomed[n] = item.key;
        n += 1;
    }
    for (doomed[0..n]) |k| try view.delete(Records, k);
    const rk = try view.arena.dupe(u8, &sk);
    if (more or keep_mark) try view.put(Reclaim, rk, .{}) else try view.delete(Reclaim, rk);
    return n;
}

/// A delete: refused while the stream holds records above its floor,
/// unless forced. It leaves a tombstone at the next generation, so groups
/// of the old one read as gone, with its floor moved to its last id, so
/// every record it held is gone at once and a stream made again takes ids
/// past them. The tombstone is marked in `stream.reclaim`: the retention
/// duty removes its records a bounded batch at a time (`stream_clean`),
/// then forgets it once no group counts it (`stream_forget`).
fn deleteApply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    const cmd = commandOf(e) orelse return error.Halt;
    if (cmd.key.len == 0 or cmd.namespace_id == .invalid) return error.Halt;
    const sk: StreamKey = cmd.namespace_id.bytes() ++ hashOf(cmd.key);
    const m = ((try view.get(Metas, &sk)) orelse return).v;
    if (!m.exists) return;
    // In log order, so an append committed before the delete counts on
    // every replica, whatever its proposer had applied.
    if (e.header.flags & entry_mod.Flags.FORCE == 0 and (try countAboveView(view, sk, m, floorOf(m))).records > 0)
        return out.refuse(.conflict, .unclassified, NOT_EMPTY);
    const key = try view.arena.dupe(u8, &sk);
    try view.delete(Retained, key);
    var tomb = newMeta("", m.created_ns, m);
    tomb.exists = false;
    tomb.gen = m.gen + 1;
    const last = lastOf(m);
    if (last.greaterThan(floorOf(m))) {
        tomb.floor_ms = last.timestamp_ms;
        tomb.floor_seq = last.sequence;
    }
    try view.put(Metas, key, .{ .v = tomb });
    try view.put(Reclaim, key, .{});
}

/// The retention duty's batch for a marked stream: up to `MAX_TRIM_APPENDS`
/// of its appends at or below its floor removed, a live stream's or a
/// tombstone's. A live stream's mark goes with its last batch; a
/// tombstone's stays until it is forgotten. Answers how many it removed.
fn cleanApply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    const cmd = commandOf(e) orelse return error.Halt;
    const sk: StreamKey = cmd.namespace_id.bytes() ++ cmd.value[0..8].*;
    const m = ((try view.get(Metas, &sk)) orelse return out.putInt(u64, 0)).v;
    const removed = try reclaim(view, sk, floorOf(m), !m.exists);
    out.putInt(u64, removed);
}

/// The retention duty's drop of a tombstone: only if it is still one, at
/// the generation the entry names, holding no appends, with no group
/// counting it. A forget a recreate, a new group or an unfinished clean
/// overtook changes nothing. Answers 1 if it forgot, 0 if not.
fn forgetApply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    const cmd = commandOf(e) orelse return error.Halt;
    const sk: StreamKey = cmd.namespace_id.bytes() ++ cmd.value[0..8].*;
    const gen = std.mem.readInt(u64, cmd.value[8..16], .little);
    const m = ((try view.get(Metas, &sk)) orelse return out.putInt(u64, 0)).v;
    if (m.exists or m.gen != gen) return out.putInt(u64, 0);
    var sb: [13]u8 = undefined;
    if (try view.get(gm.Streams_, gm.recKey(&sb, sk))) |r| if (r.v.groups > 0) return out.putInt(u64, 0);
    var held = try view.range(Records, &sk);
    if ((try held.next()) != null) return out.putInt(u64, 0);
    const key = try view.arena.dupe(u8, &sk);
    try view.delete(Metas, key);
    try view.delete(Reclaim, key);
    out.putInt(u64, 1);
}

pub const machine: machine_mod.Machine = .{
    .name = "streams",
    .entry_types = &.{ .stream_config, .stream_append, .stream_trim, .stream_delete, .stream_clean, .stream_forget },
    .families = &.{ machine_mod.info(Metas), machine_mod.info(Retained), machine_mod.info(Records), machine_mod.info(Reclaim) },
    .keys = streamKeys,
    .apply = apply,
    .namespace = commandNamespace,
    .malformed = malformed,
};

fn commandNamespace(e: *const Entry) ?u32 {
    const cmd = commandOf(e) orelse return null;
    return cmd.namespace_id.int();
}

// ─── Tests ───────────────────────────────────────────────────────────────

const store_mod = @import("../contract/store.zig");
const testing = std.testing;

/// Streams on a store of their own, every entry applied through the
/// machine, as the shard applies it. For tests here and elsewhere.
pub const TestStreams = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,

    pub fn init() TestStreams {
        return .{ .store = store_mod.Store.init(testing.allocator), .scratch = std.heap.ArenaAllocator.init(testing.allocator) };
    }

    pub fn deinit(t: *TestStreams) void {
        t.store.deinit();
        t.scratch.deinit();
    }

    pub fn read(t: *TestStreams) ReadView {
        return .{ .store = &t.store };
    }

    pub fn streams(t: *TestStreams) @import("../contract/streams.zig").Streams {
        return .{ .read = t.read() };
    }

    fn run(t: *TestStreams, etype: EntryType, flags: u16, key: []const u8, value: []const u8, stamp_ns: u64) !Result {
        t.index += 1;
        var pb: [1024]u8 = undefined;
        const e = entry_mod.buildCommandEntry(etype, flags, 1, t.index, stamp_ns, .default, key, value, &pb).?;
        var out = Result.start(&e);
        try machine_mod.applyEntry(&machine, &t.store, &t.scratch, &e, &out);
        return out;
    }

    /// An append of `count` records of `payload_len` bytes each at `ms`
    /// (it stores `4 + count * (payload_len + 6)`); its first id.
    pub fn append(t: *TestStreams, name: []const u8, ms: u64, count: u32, payload_len: u32) !StreamID {
        var vb: [1024]u8 = undefined;
        const out = try t.run(.stream_append, 0, name, stream_mod.appendValueOf(&vb, count, payload_len), ms * std.time.ns_per_ms);
        try testing.expect(out.refused() == null);
        var r = out.reader();
        const seq = r.int(u64).?;
        return .{ .timestamp_ms = r.int(u64).?, .sequence = seq };
    }

    /// A trim of `name` to `bound`, at `stamp_ns`: the records it cut and
    /// the first record's seq it left.
    pub fn trimAt(t: *TestStreams, name: []const u8, bound: stream_mod.TrimBound, stamp_ns: u64) !struct { deleted: u64, first_seq: u64 } {
        var kb: [stream_mod.TrimBound.MAX_SIZE]u8 = undefined;
        const hash = hashOf(name);
        const out = try t.run(.stream_trim, 0, bound.encode(&kb), &hash, stamp_ns);
        try testing.expect(out.refused() == null);
        var r = out.reader();
        return .{ .deleted = r.int(u64).?, .first_seq = r.int(u64).? };
    }

    pub fn trim(t: *TestStreams, name: []const u8, bound: stream_mod.TrimBound) !struct { deleted: u64, first_seq: u64 } {
        const got = try t.trimAt(name, bound, std.math.maxInt(u32) * std.time.ns_per_ms);
        return .{ .deleted = got.deleted, .first_seq = got.first_seq };
    }

    pub fn delete(t: *TestStreams, name: []const u8, force: bool) !Result {
        return t.run(.stream_delete, if (force) entry_mod.Flags.FORCE else 0, name, "", 1);
    }

    /// The retention duty's clean of `name`: how many appends it removed.
    pub fn clean(t: *TestStreams, name: []const u8) !u64 {
        const hash = hashOf(name);
        const out = try t.run(.stream_clean, 0, "", &hash, 1);
        try testing.expect(out.refused() == null);
        var r = out.reader();
        return r.int(u64).?;
    }

    /// The retention duty's forget of `name`'s tombstone at `gen`: whether
    /// it forgot.
    pub fn forget(t: *TestStreams, name: []const u8, gen: u64) !bool {
        var value: [16]u8 = undefined;
        value[0..8].* = hashOf(name);
        std.mem.writeInt(u64, value[8..16], gen, .little);
        const out = try t.run(.stream_forget, 0, "", &value, 1);
        try testing.expect(out.refused() == null);
        var r = out.reader();
        return r.int(u64).? == 1;
    }

    /// `name`'s `stream.meta` record, a tombstone's too.
    pub fn stored(t: *TestStreams, name: []const u8) ?Meta.Value {
        return metaAt(t.read(), sk(name));
    }

    pub fn config(t: *TestStreams, c: StreamConfig, name: []const u8, stamp: u64) !Result {
        var value: [StreamConfig.SIZE]u8 = undefined;
        c.encode(&value);
        return t.run(.stream_config, 0, name, &value, stamp);
    }

    pub fn meta(t: *TestStreams, name: []const u8) ?Meta.Value {
        return metaOf(t.read(), .default, name);
    }

    pub fn sk(name: []const u8) StreamKey {
        return stream_mod.streamKey(.default, name);
    }

    /// Whether the retention index names `name`'s stream.
    pub fn retained(t: *TestStreams, name: []const u8) bool {
        const k = sk(name);
        return t.store.get(Retained.ID, &k) != null;
    }

    /// The appends `name`'s stream holds, at or below its floor or not.
    pub fn held(t: *TestStreams, name: []const u8) usize {
        const k = sk(name);
        var it = t.read().range(Records, &k);
        var n: usize = 0;
        while (it.next() catch null) |_| n += 1;
        return n;
    }

    /// The streams the retention index names: each must have retention.
    pub fn retainedCount(t: *TestStreams) !usize {
        var n: usize = 0;
        var it = t.read().range(Retained, "");
        while (try it.next()) |item| {
            const m = (try t.read().get(Metas, item.key)) orelse return error.TestUnexpectedResult;
            try testing.expect(hasRetention(m.v));
            n += 1;
        }
        return n;
    }
};

fn create(pc: u32, age_s: u64, count: u64) StreamConfig {
    return .{ .op = .create, .partition_count = pc, .retention_age_s = age_s, .retention_count = count };
}

fn alter(age_s: u64, count: u64) StreamConfig {
    return .{ .op = .alter, .partition_count = 0, .retention_age_s = age_s, .retention_count = count };
}

fn sid(ms: u64, seq: u64) StreamID {
    return .{ .timestamp_ms = ms, .sequence = seq };
}

test "streams machine: a create makes a stream with its settings; the same create again is ok, other settings are refused, and an alter merges" {
    var t = TestStreams.init();
    defer t.deinit();
    try testing.expect((try t.config(create(3, 60, 0), "s", 10)).refused() == null);
    const m = t.meta("s").?;
    try testing.expectEqual(@as(u32, 3), m.partition_count);
    try testing.expectEqual(@as(u64, 60), m.retention_age_s);
    try testing.expectEqual(@as(u64, 10), m.created_ns);
    try testing.expect(m.explicit);
    try testing.expect((try t.config(create(3, 60, 0), "s", 20)).refused() == null);
    try testing.expectEqual(proto.StatusCode.conflict, (try t.config(create(1, 0, 0), "s", 30)).refused().?.status);
    try testing.expect((try t.config(alter(0, 7), "s", 40)).refused() == null);
    const merged = t.meta("s").?;
    try testing.expectEqual(@as(u32, 3), merged.partition_count);
    try testing.expectEqual(@as(u64, 60), merged.retention_age_s);
    try testing.expectEqual(@as(u64, 7), merged.retention_count);
    try testing.expectEqual(proto.StatusCode.not_found, (try t.config(alter(1, 0), "nope", 50)).refused().?.status);
    try testing.expect(t.meta("nope") == null);
}

test "streams machine: a first append makes its stream with no settings, a later one changes only its counters, and a create then gives it settings" {
    var t = TestStreams.init();
    defer t.deinit();
    _ = try t.append("s", 10, 1, 4);
    const m = t.meta("s").?;
    try testing.expect(!m.explicit);
    try testing.expectEqual(@as(u32, 1), m.partition_count);
    try testing.expectEqual(@as(u64, 10 * std.time.ns_per_ms), m.created_ns);
    _ = try t.append("s", 20, 1, 4);
    try testing.expectEqual(@as(u64, 10 * std.time.ns_per_ms), t.meta("s").?.created_ns);
    try testing.expect((try t.config(create(2, 0, 9), "s", 30)).refused() == null);
    const made = t.meta("s").?;
    try testing.expect(made.explicit);
    try testing.expectEqual(@as(u32, 2), made.partition_count);
    try testing.expectEqual(@as(u64, 10 * std.time.ns_per_ms), made.created_ns);
    try testing.expectEqual(@as(u64, 2), made.appended_records);
}

test "streams machine: the retention index names exactly the streams with a retention policy, through create, alter and delete" {
    var t = TestStreams.init();
    defer t.deinit();
    try testing.expect((try t.config(create(1, 0, 0), "plain", 1)).refused() == null);
    try testing.expect((try t.config(create(1, 30, 0), "aged", 2)).refused() == null);
    _ = try t.append("implied", 3, 1, 4);
    try testing.expect(!t.retained("plain") and t.retained("aged") and !t.retained("implied"));
    try testing.expectEqual(@as(usize, 1), try t.retainedCount());
    try testing.expect((try t.config(alter(0, 4), "plain", 4)).refused() == null);
    try testing.expect((try t.config(alter(0, 2), "implied", 5)).refused() == null);
    try testing.expect((try t.config(create(1, 99, 0), "aged", 6)).refused() != null);
    try testing.expect(t.retained("plain") and t.retained("implied"));
    try testing.expectEqual(@as(usize, 3), try t.retainedCount());
    // A delete takes the stream and its policy.
    try testing.expect((try t.delete("aged", false)).refused() == null);
    try testing.expect(t.meta("aged") == null and !t.retained("aged"));
    try testing.expectEqual(@as(usize, 2), try t.retainedCount());
    // Made again, it starts with none.
    _ = try t.append("aged", 8, 1, 4);
    try testing.expect(!t.retained("aged") and !t.meta("aged").?.explicit);
    try testing.expectEqual(@as(usize, 2), try t.retainedCount());
}

test "streams machine: a create of a name whose hash another stream's record holds is refused" {
    var t = TestStreams.init();
    defer t.deinit();
    try testing.expect((try t.config(create(1, 0, 0), "s", 1)).refused() == null);
    // Another name under the same key, as a hash collision would put it.
    const key = TestStreams.sk("s");
    var other = t.meta("s").?;
    other.name = "other";
    const bytes = try t.store.allocator.alloc(u8, try Meta.encodedSize(other));
    _ = try Meta.encodeInto(bytes, other);
    t.store.replace(Metas.ID, t.store.find(Metas.ID, &key).?, bytes);
    try testing.expectEqual(proto.StatusCode.conflict, (try t.config(create(1, 0, 0), "s", 2)).refused().?.status);
    try testing.expect(t.meta("s") == null);
}

test "streams machine: an alter gives a stream an append made its settings, so a create that asks for others is refused and the policy stays" {
    var t = TestStreams.init();
    defer t.deinit();
    _ = try t.append("s", 1, 1, 4);
    try testing.expect((try t.config(alter(3600, 0), "s", 2)).refused() == null);
    try testing.expect(t.meta("s").?.explicit);
    try testing.expectEqual(proto.StatusCode.conflict, (try t.config(create(1, 0, 0), "s", 3)).refused().?.status);
    try testing.expectEqual(@as(u64, 3600), t.meta("s").?.retention_age_s);
    try testing.expect(t.retained("s"));
    try testing.expect((try t.config(create(0, 3600, 0), "s", 4)).refused() == null);
}

test "streams machine: appends take ids as the id generator hands them out, from the entry's stamp, one per record" {
    var t = TestStreams.init();
    defer t.deinit();
    try testing.expect((try t.append("s", 100, 3, 4)).eql(sid(100, 0)));
    // The same millisecond: past the batch's last record.
    try testing.expect((try t.append("s", 100, 2, 4)).eql(sid(100, 3)));
    // A clock behind: still past the last.
    try testing.expect((try t.append("s", 90, 1, 4)).eql(sid(100, 5)));
    try testing.expect((try t.append("s", 200, 1, 4)).eql(sid(200, 0)));
    const m = t.meta("s").?;
    try testing.expect(lastOf(m).eql(sid(200, 0)));
    try testing.expectEqual(@as(u64, 7), m.appended_records);
    // Each append stores 4 + 10 bytes a record.
    try testing.expectEqual(@as(u64, 86), m.appended_bytes);
    const above = countAbove(t.read(), TestStreams.sk("s"), m, floorOf(m));
    try testing.expectEqual(@as(u64, 7), above.records);
    try testing.expectEqual(@as(u64, 86), above.bytes);
}

test "streams machine: a trim moves the floor at once, answers what it cut and what is left, and counts above the floor are exact before its appends are removed" {
    var t = TestStreams.init();
    defer t.deinit();
    for (0..5) |i| _ = try t.append("s", 100 + i, 2, 4);
    // Through the third append, by an id inside it: whole appends go.
    const got = try t.trim("s", .{ .id = sid(102, 0) });
    try testing.expectEqual(@as(u64, 6), got.deleted);
    try testing.expectEqual(@as(u64, 0), got.first_seq);
    const m = t.meta("s").?;
    try testing.expect(floorOf(m).eql(sid(102, 1)));
    try testing.expectEqual(@as(u64, 4), countAbove(t.read(), TestStreams.sk("s"), m, floorOf(m)).records);
    try testing.expectEqual(@as(usize, 2), t.held("s"));
    // An age bound resolves to an id by the entry's stamp, so finding the
    // cut needs no walk: records older than 1 ms at 105 ms are those before 104 ms.
    const aged = try t.trimAt("s", .{ .age_ms = 1 }, 105 * std.time.ns_per_ms);
    try testing.expectEqual(@as(u64, 2), aged.deleted);
    try testing.expect(floorOf(t.meta("s").?).eql(sid(103, 1)));
    // Below the floor: nothing.
    try testing.expectEqual(@as(u64, 0), (try t.trim("s", .{ .id = sid(101, 0) })).deleted);
}

test "streams machine: a trim past MAX_TRIM_APPENDS is whole at once, and its appends are removed a batch at a time" {
    var t = TestStreams.init();
    defer t.deinit();
    const n = MAX_TRIM_APPENDS + 10;
    for (0..n) |i| _ = try t.append("s", 100 + i, 1, 4);
    const got = try t.trim("s", .{ .id = sid(100 + n - 2, 0) });
    try testing.expectEqual(@as(u64, n - 1), got.deleted);
    const m = t.meta("s").?;
    try testing.expectEqual(@as(u64, 1), countAbove(t.read(), TestStreams.sk("s"), m, floorOf(m)).records);
    // Removed the most it may; the rest wait, marked.
    try testing.expectEqual(@as(usize, n - MAX_TRIM_APPENDS), t.held("s"));
    const k = TestStreams.sk("s");
    try testing.expect(t.store.get(Reclaim.ID, &k) != null);
    // A trim to the floor removes the rest and clears the mark.
    try testing.expectEqual(@as(u64, 0), (try t.trim("s", .{ .id = floorOf(m) })).deleted);
    try testing.expectEqual(@as(usize, 1), t.held("s"));
    try testing.expect(t.store.get(Reclaim.ID, &k) == null);
}

/// The same tree: each node's key and priority, and its children's, alike.
fn sameTree(a: ?*store_mod.Tree.Node, b: ?*store_mod.Tree.Node) bool {
    const x = a orelse return b == null;
    const y = b orelse return false;
    return std.mem.eql(u8, x.key, y.key) and x.priority == y.priority and
        sameTree(x.children[0], y.children[0]) and sameTree(x.children[1], y.children[1]);
}

test "streams machine: appends linked by their stream's hint build the tree a descent builds, through trims and deletes" {
    var plain = TestStreams.init();
    defer plain.deinit();
    var hinted = TestStreams.init();
    defer hinted.deinit();
    hinted.store.setTailPrefix(Records.ID, @sizeOf(StreamKey));
    const names = [_][]const u8{ "a", "b", "c" };
    for ([_]*TestStreams{ &plain, &hinted }) |t| {
        var ms: u64 = 100;
        for (0..3) |round| {
            // Appends interleaved across the streams, some in one ms.
            for (0..40) |i| {
                _ = try t.append(names[i % names.len], ms, @intCast(1 + i % 3), 4);
                if (i % 4 == 0) ms += 1;
            }
            // A trim's unlinks, then appends after them.
            _ = try t.trim(names[round % names.len], .{ .id = sid(ms - 5, 0) });
            for (0..10) |i| _ = try t.append(names[i % names.len], ms + i, 1, 4);
            ms += 10;
        }
        // A delete's range unlink, then the stream made again by an append.
        _ = try t.delete("b", true);
        for (0..12) |i| _ = try t.append(names[i % names.len], ms + i, 2, 4);
    }
    try testing.expect(hinted.store.tails.count() > 0);
    try testing.expect(sameTree(plain.store.family(Records.ID).tree.root, hinted.store.family(Records.ID).tree.root));
    try testing.expectEqual(store_mod.checksum(&plain.store, Records.ID), store_mod.checksum(&hinted.store, Records.ID));
}

test "streams machine: a delete is refused while records are above the floor unless forced, leaves a tombstone, and its appends go by cleans" {
    var t = TestStreams.init();
    defer t.deinit();
    _ = try t.append("s", 100, 2, 4);
    try testing.expectEqual(proto.StatusCode.conflict, (try t.delete("s", false)).refused().?.status);
    _ = try t.trim("s", .{ .id = sid(100, 1) });
    // Trimmed empty: it goes, as a tombstone at the next generation.
    try testing.expect((try t.delete("s", false)).refused() == null);
    try testing.expect(t.meta("s") == null);
    try testing.expectEqual(@as(u64, 1), t.stored("s").?.gen);

    const n = MAX_TRIM_APPENDS + 5;
    for (0..n) |i| _ = try t.append("f", 100 + i, 1, 4);
    try testing.expect((try t.delete("f", true)).refused() == null);
    // Gone at once: nothing reads, nothing counts.
    try testing.expect(t.meta("f") == null);
    const k = TestStreams.sk("f");
    try testing.expectEqual(@as(u64, 0), countAbove(t.read(), k, t.stored("f").?, floorOf(t.stored("f").?)).records);
    // Its appends leave a bounded batch at a time; the tombstone stays
    // marked for its forget.
    try testing.expectEqual(@as(u64, MAX_TRIM_APPENDS), try t.clean("f"));
    try testing.expectEqual(@as(u64, 5), try t.clean("f"));
    try testing.expectEqual(@as(usize, 0), t.held("f"));
    try testing.expect(t.store.get(Reclaim.ID, &k) != null);
    // A forget of another generation changes nothing; of its own, it goes.
    try testing.expect(!try t.forget("f", 0));
    try testing.expect(try t.forget("f", 1));
    try testing.expect(t.stored("f") == null);
    try testing.expect(t.store.get(Reclaim.ID, &k) == null);
    // A delete of a stream that isn't there is ok.
    try testing.expect((try t.delete("nope", false)).refused() == null);
}

test "streams machine: a stream made again in the same ms after a delete reads only its own records, with ids past the old ones" {
    var t = TestStreams.init();
    defer t.deinit();
    _ = try t.append("s", 100, 3, 4);
    const old_last = lastOf(t.meta("s").?);
    try testing.expect((try t.delete("s", true)).refused() == null);
    // Made again by an append in the same ms, before any clean.
    const first = try t.append("s", 100, 2, 4);
    try testing.expect(first.greaterThan(old_last));
    const m = t.meta("s").?;
    try testing.expectEqual(@as(u64, 1), m.gen);
    try testing.expectEqual(@as(u64, 2), countAbove(t.read(), TestStreams.sk("s"), m, floorOf(m)).records);
    var it = Appends.init(t.read(), TestStreams.sk("s"), m, floorOf(m));
    try testing.expect(it.next().?.id.eql(first));
    try testing.expect(it.next() == null);
    // The old append is still held, below the floor, until a clean.
    try testing.expectEqual(@as(usize, 2), t.held("s"));
    try testing.expectEqual(@as(u64, 1), try t.clean("s"));
    try testing.expectEqual(@as(usize, 1), t.held("s"));
    // A forget raced by the recreate changes nothing.
    try testing.expect(!try t.forget("s", 1));
    try testing.expect(t.meta("s") != null);
}

test "streams machine: a recreate keeps only the tombstone's generation, last id and floor; its settings are its create's" {
    var t = TestStreams.init();
    defer t.deinit();
    try testing.expect((try t.config(.{ .op = .create, .partition_count = 4, .retention_age_s = 60, .retention_count = 9 }, "s", 1)).refused() == null);
    _ = try t.append("s", 100, 2, 4);
    try testing.expect((try t.delete("s", true)).refused() == null);
    try testing.expect(!t.retained("s"));
    try testing.expect((try t.config(.{ .op = .create, .partition_count = 2, .retention_age_s = 0, .retention_count = 0 }, "s", 7)).refused() == null);
    const m = t.meta("s").?;
    try testing.expectEqual(@as(u32, 2), m.partition_count);
    try testing.expectEqual(@as(u64, 0), m.retention_age_s);
    try testing.expectEqual(@as(u64, 0), m.retention_count);
    try testing.expectEqual(@as(u64, 7), m.created_ns);
    try testing.expectEqual(@as(u64, 1), m.gen);
    try testing.expect(floorOf(m).eql(sid(100, 1)));
    try testing.expectEqual(@as(u64, 0), countAbove(t.read(), TestStreams.sk("s"), m, floorOf(m)).records);

    // Made again by an append, it is made at that append's stamp: a reader
    // that keeps the stream's created_ns can tell it from the old one.
    try testing.expect((try t.delete("s", true)).refused() == null);
    _ = try t.append("s", 300, 1, 4);
    const again = t.meta("s").?;
    try testing.expectEqual(@as(u64, 300 * std.time.ns_per_ms), again.created_ns);
    try testing.expect(again.created_ns != m.created_ns);
    try testing.expectEqual(@as(u64, 2), again.gen);
}

test "streams machine: ids still rise across a delete and a recreate with the clock moved back" {
    var t = TestStreams.init();
    defer t.deinit();
    _ = try t.append("s", 500, 2, 4);
    const old_last = lastOf(t.meta("s").?);
    try testing.expect((try t.delete("s", true)).refused() == null);
    // The clock has gone back 400 ms.
    const first = try t.append("s", 100, 1, 4);
    try testing.expect(first.greaterThan(old_last));
    const next = try t.append("s", 101, 1, 4);
    try testing.expect(next.greaterThan(first));
}
