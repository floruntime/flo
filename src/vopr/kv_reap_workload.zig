//! KV keys that go: written with short TTLs or deleted, reaped from the
//! leader's own applied store as its reap duty would, and written again,
//! some with a CAS naming a version a key had before it went. In a
//! namespace of their own, so the version floor a reap raises moves no
//! other workload key's versions.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const store_mod = @import("../contract/store.zig");
const view_mod = @import("../contract/view.zig");
const schema = @import("../protocol/schema.zig");
const kv_entries = @import("../protocol/kv_entries.zig");
const kv_machine = @import("../kv/machine.zig");
const kv_ops = @import("../protocol/kv_ops.zig");
const NamespaceId = @import("../namespace/table.zig").Id;

pub const NS: NamespaceId = .of(29);
pub const KEYS = 6;

/// Key `i` as state keeps it.
pub fn key(i: usize) *const [4 + 2]u8 {
    return &STORED[i];
}

const STORED = blk: {
    var out: [KEYS][6]u8 = undefined;
    for (&out, 0..) |*k, i| k.* = NS.bytes() ++ [2]u8{ 'g', '0' + i };
    break :blk out;
};

/// Whether `cur` has gone by `at_ns`, judged here and not by the machine:
/// its TTL ran out, or it is a tombstone `TOMBSTONE_KEEP_MS` old.
pub fn gone(cur: kv_machine.Current, at_ns: u64) bool {
    if (cur.tombstone) return at_ns - @min(at_ns, cur.timestamp_ns) >= kv_ops.TOMBSTONE_KEEP_MS * std.time.ns_per_ms;
    return cur.expiry_ns != 0 and at_ns >= cur.expiry_ns;
}

pub const Made = struct { entry_type: entry_mod.EntryType, payload: []const u8 };

/// An entry from `store`, the leader's applied KV, at its clock `now_ns`.
pub fn make(r: std.Random, buf: []u8, store: *store_mod.Store, now_ns: u64) ?Made {
    const roll = r.uintLessThan(u8, 100);
    if (roll < 25) if (reap(buf, store, now_ns)) |m| return m;
    const k = key(r.uintLessThan(usize, KEYS));
    if (roll < 40) {
        const p = kv_entries.Delete.encodeInto(buf, .{ .key = k, .cas_version = null }) catch return null;
        return .{ .entry_type = .kv_delete, .payload = p };
    }
    var vb: [16]u8 = undefined;
    const value = vb[0..r.intRangeAtMost(usize, 1, vb.len)];
    @memset(value, 'v');
    // Most go soon; some never do.
    const ttl_ms: ?u64 = if (r.uintLessThan(u8, 100) < 70) r.intRangeAtMost(u64, 1, 800) else null;
    // Now and then a CAS at a low version: stale once its key has gone.
    const cas: ?u64 = if (roll >= 85) r.intRangeAtMost(u64, 1, 8) else null;
    const p = kv_entries.Put.encodeInto(buf, .{ .key = k, .value = value, .ttl_ms = ttl_ms, .condition = .always, .cas_version = cas }) catch return null;
    return .{ .entry_type = .kv_put, .payload = p };
}

/// The leader's reap of every key here gone by `now_ns`; null for none.
fn reap(buf: []u8, store: *store_mod.Store, now_ns: u64) ?Made {
    const rv: view_mod.ReadView = .{ .store = store };
    var named: [KEYS]kv_entries.ReapKey.Value = undefined;
    var n: usize = 0;
    for (0..KEYS) |i| {
        const cur = (rv.get(kv_machine.Values, key(i)) catch null) orelse continue;
        if (!gone(cur, now_ns)) continue;
        named[n] = .{ .key = key(i) };
        n += 1;
    }
    if (n == 0) return null;
    var w: schema.Writer = .{ .buf = buf };
    kv_entries.encodeReap(&w, named[0..n]) catch return null;
    return .{ .entry_type = .kv_reap, .payload = w.written() };
}
