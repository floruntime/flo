//! KV on the contract: its state is three families, changed only through a
//! `View` in `apply`, so a refused entry changes nothing.
//!
//! - `kv.value`: each key's current version (value, version, lsn, term,
//!   stamp, expiry, tombstone, type) and its history's count and bytes.
//! - `kv.history`: each key's older versions, keyed by version so a key's
//!   chain is oldest first; past either bound the oldest go.
//! - `kv.ns_counts`: per namespace, its live keys, the bytes it stores,
//!   its history's charge and the versions evicted. Kept by the same
//!   entries that change the keys, so every replica counts the same.
//!
//! An entry names its keys namespace-qualified (`ns\0key`; a bare key is
//! the default namespace's).

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const schema = @import("../protocol/schema.zig");
const kv_entries = @import("../protocol/kv_entries.zig");
const kv_ops = @import("../protocol/kv_ops.zig");
const limits = @import("../protocol/limits.zig");
const time_units = @import("../util/time_units.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");

const Entry = entry_mod.Entry;
const View = view_mod.View;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;

/// A stored value's kind: opaque bytes, an 8-byte counter, a JSON document.
pub const ValueType = enum(u8) { string = 0, counter = 1, json = 2 };

/// A key's current version.
pub const Current = struct {
    version: u64,
    lsn: u64,
    term: u64,
    timestamp_ns: u64,
    /// 0 for none.
    expiry_ns: u64,
    tombstone: bool,
    value_type: ValueType,
    /// Its older versions in `kv.history`, and what they are charged.
    history_count: u32,
    history_bytes: u64,
    /// Empty for a tombstone.
    value: []const u8,

    const FIXED = 8 * 5 + 1 + 1 + 4 + 8;

    pub fn encode(self: Current, w: *std.Io.Writer) std.Io.Writer.Error!void {
        var b: [FIXED]u8 = undefined;
        std.mem.writeInt(u64, b[0..8], self.version, .little);
        std.mem.writeInt(u64, b[8..16], self.lsn, .little);
        std.mem.writeInt(u64, b[16..24], self.term, .little);
        std.mem.writeInt(u64, b[24..32], self.timestamp_ns, .little);
        std.mem.writeInt(u64, b[32..40], self.expiry_ns, .little);
        b[40] = @intFromBool(self.tombstone);
        b[41] = @backingInt(self.value_type);
        std.mem.writeInt(u32, b[42..46], self.history_count, .little);
        std.mem.writeInt(u64, b[46..54], self.history_bytes, .little);
        try w.writeAll(&b);
        try w.writeAll(self.value);
    }

    pub fn decode(bytes: []const u8) error{Malformed}!Current {
        if (bytes.len < FIXED or bytes[40] > 1) return error.Malformed;
        return .{
            .version = std.mem.readInt(u64, bytes[0..8], .little),
            .lsn = std.mem.readInt(u64, bytes[8..16], .little),
            .term = std.mem.readInt(u64, bytes[16..24], .little),
            .timestamp_ns = std.mem.readInt(u64, bytes[24..32], .little),
            .expiry_ns = std.mem.readInt(u64, bytes[32..40], .little),
            .tombstone = bytes[40] != 0,
            .value_type = std.enums.fromInt(ValueType, bytes[41]) orelse return error.Malformed,
            .history_count = std.mem.readInt(u32, bytes[42..46], .little),
            .history_bytes = std.mem.readInt(u64, bytes[46..54], .little),
            .value = bytes[FIXED..],
        };
    }

    /// Whether its TTL had run out by `at_ns`.
    pub fn expiredAt(self: Current, at_ns: u64) bool {
        return self.expiry_ns > 0 and self.expiry_ns <= at_ns;
    }
};

/// An older version of a key.
pub const Version = struct {
    version: u64,
    lsn: u64,
    term: u64,
    timestamp_ns: u64,
    tombstone: bool,
    value: []const u8,

    const FIXED = 8 * 4 + 1;

    pub fn encode(self: Version, w: *std.Io.Writer) std.Io.Writer.Error!void {
        var b: [FIXED]u8 = undefined;
        std.mem.writeInt(u64, b[0..8], self.version, .little);
        std.mem.writeInt(u64, b[8..16], self.lsn, .little);
        std.mem.writeInt(u64, b[16..24], self.term, .little);
        std.mem.writeInt(u64, b[24..32], self.timestamp_ns, .little);
        b[32] = @intFromBool(self.tombstone);
        try w.writeAll(&b);
        try w.writeAll(self.value);
    }

    pub fn decode(bytes: []const u8) error{Malformed}!Version {
        if (bytes.len < FIXED or bytes[32] > 1) return error.Malformed;
        return .{
            .version = std.mem.readInt(u64, bytes[0..8], .little),
            .lsn = std.mem.readInt(u64, bytes[8..16], .little),
            .term = std.mem.readInt(u64, bytes[16..24], .little),
            .timestamp_ns = std.mem.readInt(u64, bytes[24..32], .little),
            .tombstone = bytes[32] != 0,
            .value = bytes[FIXED..],
        };
    }

    /// What it counts against its key's history bound.
    pub fn charge(self: Version) u64 {
        return self.value.len + kv_ops.HISTORY_ENTRY_COST;
    }

    fn of(c: Current) Version {
        return .{ .version = c.version, .lsn = c.lsn, .term = c.term, .timestamp_ns = c.timestamp_ns, .tombstone = c.tombstone, .value = if (c.tombstone) "" else c.value };
    }
};

/// A namespace's counts.
pub const Counts = struct {
    /// Keys written and not deleted. A key past its TTL counts until it is
    /// deleted or overwritten.
    live: u64 = 0,
    /// Key and current value bytes, tombstones and expired values
    /// included; history excluded.
    bytes: u64 = 0,
    /// Older versions' charge.
    history_bytes: u64 = 0,
    /// Older versions evicted by either bound, ever.
    history_evicted: u64 = 0,

    pub fn encode(self: Counts, w: *std.Io.Writer) std.Io.Writer.Error!void {
        var b: [32]u8 = undefined;
        std.mem.writeInt(u64, b[0..8], self.live, .little);
        std.mem.writeInt(u64, b[8..16], self.bytes, .little);
        std.mem.writeInt(u64, b[16..24], self.history_bytes, .little);
        std.mem.writeInt(u64, b[24..32], self.history_evicted, .little);
        try w.writeAll(&b);
    }

    pub fn decode(bytes: []const u8) error{Malformed}!Counts {
        if (bytes.len != 32) return error.Malformed;
        return .{
            .live = std.mem.readInt(u64, bytes[0..8], .little),
            .bytes = std.mem.readInt(u64, bytes[8..16], .little),
            .history_bytes = std.mem.readInt(u64, bytes[16..24], .little),
            .history_evicted = std.mem.readInt(u64, bytes[24..32], .little),
        };
    }
};

// Each KV entry's code is its UAL type's: `kv.put` is `kv_put`, so a
// renumbering on either side fails the build instead of mis-decoding.
comptime {
    for (kv_entries.table) |e| {
        const tag = "kv_" ++ e.name["kv.".len..];
        if (@backingInt(@field(entry_mod.EntryType, tag)) != e.type_code) @compileError("KV entry '" ++ e.name ++ "' has code " ++ std.fmt.comptimePrint("{d}", .{e.type_code}) ++ ", but " ++ tag ++ " is another");
    }
}

pub const Values = view_mod.Family(10, "kv.value", Current);
pub const History = view_mod.Family(11, "kv.history", Version);
pub const NsCounts = view_mod.Family(12, "kv.ns_counts", Counts);

// ─── Keys ────────────────────────────────────────────────────────────────

/// A namespace-qualified key split: what precedes its separator, or the
/// default namespace for a bare key.
pub const Qualified = struct {
    ns: []const u8,
    key: []const u8,
    /// The qualified key's length: what it counts against its namespace
    /// beside its value.
    len: usize,

    pub fn of(qkey: []const u8) Qualified {
        const sep = std.mem.indexOfScalar(u8, qkey, 0) orelse return .{ .ns = "default", .key = qkey, .len = qkey.len };
        return .{ .ns = qkey[0..sep], .key = qkey[sep + 1 ..], .len = qkey.len };
    }

    fn keyBytes(q: Qualified) u64 {
        return q.len;
    }
};

/// The longest key a family here stores: a qualified key's parts, framed,
/// and a version.
pub const MAX_KEY = 1 + limits.MAX_NAMESPACE_NAME + 2 + limits.MAX_QUALIFIED_KEY + 8;

pub fn valueKey(buf: []u8, q: Qualified) []const u8 {
    return view_mod.key(buf, q.ns, q.key) catch unreachable;
}

/// The prefix of `q`'s versions in `kv.history`: its key, length-framed so
/// no other key's versions share it.
pub fn historyPrefix(buf: []u8, q: Qualified) []const u8 {
    var framed: [2 + limits.MAX_QUALIFIED_KEY]u8 = undefined;
    return view_mod.key(buf, q.ns, frame(&framed, q.key)) catch unreachable;
}

fn frame(buf: []u8, k: []const u8) []const u8 {
    std.mem.writeInt(u16, buf[0..2], @intCast(k.len), .big);
    @memcpy(buf[2..][0..k.len], k);
    return buf[0 .. 2 + k.len];
}

/// `q`'s version `version` in `kv.history`. Big-endian, so versions sort
/// oldest first.
pub fn historyKey(buf: []u8, q: Qualified, version: u64) []const u8 {
    const prefix = historyPrefix(buf, q);
    std.mem.writeInt(u64, buf[prefix.len..][0..8], version, .big);
    return buf[0 .. prefix.len + 8];
}

pub fn countsKey(buf: []u8, ns: []const u8) []const u8 {
    return view_mod.key(buf, ns, "") catch unreachable;
}

fn declare(out: *KeySet, qkey: []const u8) error{OutOfMemory}!void {
    const q = Qualified.of(qkey);
    var framed: [2 + limits.MAX_QUALIFIED_KEY]u8 = undefined;
    try out.add(Values.ID, q.ns, q.key);
    out.addRange(History.ID, q.ns, frame(&framed, q.key)) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        // A key with no namespace: apply halts on it before touching anything.
        error.NoNamespace => {},
    };
    try out.add(NsCounts.ID, q.ns, "");
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    var diag: schema.Diagnostic = .{};
    switch (@as(entry_mod.EntryType, @fromBackingInt(@intCast(e.header.entry_type)))) {
        .kv_put => try declare(out, (kv_entries.Put.decodeAll(e.payload, &diag) catch return).key),
        .kv_delete => try declare(out, (kv_entries.Delete.decodeAll(e.payload, &diag) catch return).key),
        .kv_incr => try declare(out, (kv_entries.Incr.decodeAll(e.payload, &diag) catch return).key),
        .kv_expire => try declare(out, (kv_entries.Expire.decodeAll(e.payload, &diag) catch return).key),
        .kv_json_patch => try declare(out, (kv_entries.JsonPatch.decodeAll(e.payload, &diag) catch return).key),
        .kv_batch => {
            const batch = kv_entries.Batch.decodeAll(e.payload, &diag) catch return;
            var it = batch.ops.iterator();
            while (it.next() catch return) |op| try declare(out, op.key);
        },
        else => {},
    }
}

// ─── Apply ───────────────────────────────────────────────────────────────

pub const INCR_OVERFLOW = "bad request: delta would take the counter past an i64";
pub const NOT_A_COUNTER = "bad request: the key's value isn't an 8-byte counter";

/// The entry's place in the log, which every write it makes records.
const Stamp = struct {
    index: u64,
    term: u64,
    ns: u64,

    fn of(e: *const Entry) Stamp {
        return .{ .index = e.header.index, .term = e.header.term, .ns = e.header.timestamp_ns };
    }
};

/// `qkey` split, or a halt for a key with no namespace (`\x00key`): no
/// handler writes one, and its keys couldn't be declared.
fn split(qkey: []const u8) ApplyError!Qualified {
    const q = Qualified.of(qkey);
    if (q.ns.len == 0) return error.Halt;
    return q;
}

/// Answers: put, the key's version; delete, whether the key was there
/// (u8); incr, its version and value; expire, nothing; batch, its op
/// count (u16); a JSON patch, the key's new version, 0 when it deleted the
/// key. A payload that won't decode is a bug in the proposer, and
/// halts. An incr that overflows or meets a non-counter, and an expire of a
/// key that's gone, are refused; inside a batch they refuse the whole
/// batch, so a transaction applies whole or not at all.
fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    const at = Stamp.of(e);
    switch (@as(entry_mod.EntryType, @fromBackingInt(@intCast(e.header.entry_type)))) {
        .kv_put => {
            const p = kv_entries.Put.decodeAll(e.payload, &diag) catch return error.Halt;
            const version = try put(view, try split(p.key), p.value, .string, at, time_units.expiryAfter(at.ns, p.ttl_ms orelse 0));
            out.putInt(u64, version);
        },
        .kv_delete => {
            const p = kv_entries.Delete.decodeAll(e.payload, &diag) catch return error.Halt;
            out.putInt(u8, @intFromBool(try delete(view, try split(p.key), at)));
        },
        .kv_incr => {
            const p = kv_entries.Incr.decodeAll(e.payload, &diag) catch return error.Halt;
            const r = incr(view, try split(p.key), p.delta, at) catch |err| return refused(out, err, "");
            out.putInt(u64, r.version);
            out.putInt(i64, r.value);
        },
        .kv_expire => {
            const p = kv_entries.Expire.decodeAll(e.payload, &diag) catch return error.Halt;
            expire(view, try split(p.key), time_units.expiryAfter(at.ns, p.ttl_ms), at) catch |err| return refused(out, err, "");
        },
        .kv_json_patch => {
            const p = kv_entries.JsonPatch.decodeAll(e.payload, &diag) catch return error.Halt;
            out.putInt(u64, try jsonPatch(view, try split(p.key), p.path, p.document, at, out));
        },
        .kv_batch => {
            // Decoded and checked whole before any op applies.
            const batch = kv_entries.Batch.decodeAll(e.payload, &diag) catch return error.Halt;
            var check = batch.ops.iterator();
            var ops: u16 = 0;
            while (check.next() catch return error.Halt) |op| {
                if (kv_entries.batchOpRule(op) != null) return error.Halt;
                _ = try split(op.key);
                ops += 1;
            }
            var it = batch.ops.iterator();
            var n: u16 = 0;
            while (it.next() catch return error.Halt) |op| : (n += 1) {
                const q = try split(op.key);
                switch (op.kind) {
                    .put => _ = try put(view, q, op.value.?, .string, at, time_units.expiryAfter(at.ns, op.ttl_ms orelse 0)),
                    .delete => _ = try delete(view, q, at),
                    .incr => _ = incr(view, q, op.delta.?, at) catch |err| {
                        var buf: [48]u8 = undefined;
                        return refused(out, err, std.fmt.bufPrint(&buf, "the transaction's op {d}: ", .{n + 1}) catch "");
                    },
                    .expire => expire(view, q, time_units.expiryAfter(at.ns, op.ttl_ms.?), at) catch |err| {
                        var buf: [48]u8 = undefined;
                        return refused(out, err, std.fmt.bufPrint(&buf, "the transaction's op {d}: ", .{n + 1}) catch "");
                    },
                }
            }
            out.putInt(u16, ops);
        },
        else => return error.Halt,
    }
}

/// Refuse `out` for an op's own failure; any other error is returned.
fn refused(out: *Result, err: anyerror, prefix: []const u8) ApplyError!void {
    if (refuseOp(out, err, prefix)) |e| return e;
}

fn refuseOp(out: *Result, err: anyerror, prefix: []const u8) ?ApplyError {
    const why: struct { status: @import("../protocol/proto.zig").StatusCode, reason: @import("../protocol/proto.zig").Reason, text: []const u8 } = switch (err) {
        error.Overflow => .{ .status = .bad_request, .reason = .out_of_range, .text = INCR_OVERFLOW },
        error.NotACounter => .{ .status = .bad_request, .reason = .wrong_type, .text = NOT_A_COUNTER },
        error.NotFound => .{ .status = .not_found, .reason = .not_found, .text = "not found" },
        else => return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.Io => error.Io,
            else => error.Halt,
        },
    };
    if (prefix.len == 0) {
        out.refuse(why.status, why.reason, why.text);
    } else {
        out.refuseFmt(why.status, why.reason, "{s}{s}; nothing in the transaction applied", .{ prefix, why.text });
    }
    return null;
}

fn counts(view: *View, ns: []const u8) ApplyError!Counts {
    var kb: [MAX_KEY]u8 = undefined;
    return (try view.get(NsCounts, countsKey(&kb, ns))) orelse .{};
}

fn putCounts(view: *View, ns: []const u8, c: Counts) ApplyError!void {
    var kb: [MAX_KEY]u8 = undefined;
    try view.put(NsCounts, countsKey(&kb, ns), c);
}

/// Move `cur` onto `q`'s history, then evict the oldest while either bound
/// is exceeded, keeping the newest. Returns `cur`'s history fields as they
/// now are.
fn pushVersion(view: *View, q: Qualified, cur: Current, c: *Counts) ApplyError!struct { count: u32, bytes: u64 } {
    var kb: [MAX_KEY]u8 = undefined;
    const old = Version.of(cur);
    try view.put(History, historyKey(&kb, q, old.version), old);
    var n = cur.history_count + 1;
    var bytes = cur.history_bytes + old.charge();
    c.history_bytes += old.charge();
    while (n > 1 and (n > kv_ops.HISTORY_MAX_VERSIONS or bytes > kv_ops.HISTORY_MAX_BYTES)) {
        var pb: [MAX_KEY]u8 = undefined;
        var r = try view.range(History, historyPrefix(&pb, q));
        const oldest = (try r.next()) orelse return error.Halt;
        try view.delete(History, oldest.key);
        n -= 1;
        bytes -= oldest.value.charge();
        c.history_bytes -|= oldest.value.charge();
        c.history_evicted += 1;
    }
    return .{ .count = n, .bytes = bytes };
}

/// Write `value` as `q`'s new version; returns that version.
fn put(view: *View, q: Qualified, value: []const u8, value_type: ValueType, at: Stamp, expiry_ns: u64) ApplyError!u64 {
    var kb: [MAX_KEY]u8 = undefined;
    const vk = valueKey(&kb, q);
    var c = try counts(view, q.ns);
    var next: Current = .{ .version = 1, .lsn = at.index, .term = at.term, .timestamp_ns = at.ns, .expiry_ns = expiry_ns, .tombstone = false, .value_type = value_type, .history_count = 0, .history_bytes = 0, .value = value };
    if (try view.get(Values, vk)) |cur| {
        if (cur.tombstone) c.live += 1;
        c.bytes -|= cur.value.len;
        const h = try pushVersion(view, q, cur, &c);
        next.version = cur.version + 1;
        next.history_count = h.count;
        next.history_bytes = h.bytes;
    } else {
        c.live += 1;
        c.bytes += q.keyBytes();
    }
    c.bytes += value.len;
    try view.put(Values, vk, next);
    try putCounts(view, q.ns, c);
    return next.version;
}

/// Tombstone `q`; returns whether it held a live, unexpired value.
fn delete(view: *View, q: Qualified, at: Stamp) ApplyError!bool {
    var kb: [MAX_KEY]u8 = undefined;
    const vk = valueKey(&kb, q);
    var c = try counts(view, q.ns);
    var next: Current = .{ .version = 1, .lsn = at.index, .term = at.term, .timestamp_ns = at.ns, .expiry_ns = 0, .tombstone = true, .value_type = .string, .history_count = 0, .history_bytes = 0, .value = "" };
    var existed = false;
    if (try view.get(Values, vk)) |cur| {
        existed = !cur.tombstone and !cur.expiredAt(at.ns);
        if (!cur.tombstone) c.live -|= 1;
        c.bytes -|= cur.value.len;
        const h = try pushVersion(view, q, cur, &c);
        next.version = cur.version + 1;
        next.expiry_ns = cur.expiry_ns;
        next.value_type = cur.value_type;
        next.history_count = h.count;
        next.history_bytes = h.bytes;
    } else {
        // A tombstone for a key never seen here.
        c.bytes += q.keyBytes();
    }
    try view.put(Values, vk, next);
    try putCounts(view, q.ns, c);
    return existed;
}

/// Add `delta` to `q`'s counter, made at 0 if it has none (or one whose
/// TTL ran out by this entry's stamp, on every replica alike).
fn incr(view: *View, q: Qualified, delta: i64, at: Stamp) (ApplyError || error{ Overflow, NotACounter })!struct { version: u64, value: i64 } {
    var kb: [MAX_KEY]u8 = undefined;
    var current: i64 = 0;
    if (try view.get(Values, valueKey(&kb, q))) |cur| {
        if (!cur.tombstone and !cur.expiredAt(at.ns)) {
            if (cur.value.len != 8) return error.NotACounter;
            current = std.mem.readInt(i64, cur.value[0..8], .little);
        }
    }
    const result = std.math.add(i64, current, delta) catch return error.Overflow;
    var buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &buf, result, .little);
    return .{ .version = try put(view, q, &buf, .counter, at, 0), .value = result };
}

/// Set (`document`) or delete (none) the JSON at `path` in `q`'s document,
/// merged with its value as this entry finds it. `$` on a missing key
/// writes the whole document; `$` with none deletes the key. As any whole
/// write, it clears the key's TTL. Returns the new version, 0 when the key
/// was deleted; a path or document the merge can't take is refused, and
/// answers nothing.
fn jsonPatch(view: *View, q: Qualified, path: []const u8, document: ?[]const u8, at: Stamp, out: *Result) ApplyError!u64 {
    const json_path = @import("../util/json_path.zig");
    const handler = @import("handler.zig");
    var kb: [MAX_KEY]u8 = undefined;
    const cur = try view.get(Values, valueKey(&kb, q));
    const live = if (cur) |c| !c.tombstone and !c.expiredAt(at.ns) else false;
    const whole = std.mem.eql(u8, path, "$");
    const refuse = struct {
        fn f(o: *Result, err: anyerror) u64 {
            const r = handler.KVHandler.refusal(switch (err) {
                error.NotFound => error.NotFound,
                error.TooLarge => error.TooLarge,
                else => handler.KVHandler.jsonError(@errorCast(err)),
            });
            o.refuse(r.status, r.refusal.reason, r.refusal.message);
            return 0;
        }
    }.f;
    if (document) |doc| {
        const merged = if (live)
            json_path.jsonPathSet(view.arena, cur.?.value, path, doc) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return refuse(out, err),
            }
        else if (whole) doc else return refuse(out, error.NotFound);
        // The merge may grow the document past what a key holds.
        if (merged.len > kv_ops.MAX_VALUE) return refuse(out, error.TooLarge);
        return put(view, q, merged, .json, at, 0);
    }
    if (!live) return refuse(out, error.NotFound);
    if (whole) {
        _ = try delete(view, q, at);
        return 0;
    }
    const rest = json_path.jsonPathDel(view.arena, cur.?.value, path) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return refuse(out, err),
    };
    return put(view, q, rest, .json, at, 0);
}

/// Set `q`'s TTL without a new version: a TTL isn't a value. 0 clears it.
fn expire(view: *View, q: Qualified, expiry_ns: u64, at: Stamp) (ApplyError || error{NotFound})!void {
    var kb: [MAX_KEY]u8 = undefined;
    const vk = valueKey(&kb, q);
    var cur = (try view.get(Values, vk)) orelse return error.NotFound;
    if (cur.tombstone or cur.expiredAt(at.ns)) return error.NotFound;
    cur.expiry_ns = expiry_ns;
    cur.lsn = at.index;
    cur.term = at.term;
    cur.timestamp_ns = at.ns;
    try view.put(Values, vk, cur);
}

/// Whether `t` is one of KV's entry types.
pub fn owns(t: entry_mod.EntryType) bool {
    for (machine.entry_types) |u| if (u == t) return true;
    return false;
}

pub const machine: machine_mod.Machine = .{
    .name = "kv",
    .entry_types = &.{ .kv_put, .kv_delete, .kv_batch, .kv_incr, .kv_expire, .kv_json_patch },
    .families = &.{ machine_mod.info(Values), machine_mod.info(History), machine_mod.info(NsCounts) },
    .keys = keys,
    .apply = apply,
};

/// KV state on its own, for tests: a store, the scratch an apply stages
/// in, and entries applied through the machine as a shard applies them.
pub const TestKv = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,

    const store_mod = @import("../contract/store.zig");

    pub fn init(allocator: std.mem.Allocator) TestKv {
        return .{ .store = store_mod.Store.init(allocator), .scratch = std.heap.ArenaAllocator.init(allocator) };
    }

    pub fn deinit(self: *TestKv) void {
        self.store.deinit();
        self.scratch.deinit();
    }

    /// Reads over it; good while `self` stays put.
    pub fn state(self: *TestKv) @import("state.zig").KvState {
        return .{ .store = &self.store };
    }

    /// Apply `e` as the next entry; its answer or refusal.
    pub fn apply(self: *TestKv, e: *const Entry) ApplyError!Result {
        self.index = @max(self.index, e.header.index);
        var out: Result = .{};
        out.reset(e);
        try machine_mod.applyEntry(&machine, &self.store, &self.scratch, e, &out);
        return out;
    }

    /// A put of `value` at `qkey`, as the next entry, stamped `stamp`.
    pub fn put(self: *TestKv, qkey: []const u8, value: []const u8, stamp: u64) !void {
        const body: kv_entries.Put.Value = .{ .key = qkey, .value = value, .ttl_ms = null };
        const buf = try self.store.allocator.alloc(u8, try kv_entries.Put.encodedSize(body));
        defer self.store.allocator.free(buf);
        const p = try kv_entries.Put.encodeInto(buf, body);
        const r = try self.apply(&entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, self.index + 1, stamp, p));
        std.debug.assert(r.refused() == null);
    }
};
