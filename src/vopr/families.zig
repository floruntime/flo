//! VOPR state families: toy state machines each replica applies, so the
//! checker can compare applied state, not only the log.
//!
//! A family's checksum is a hash of its state in a canonical order (keys
//! in key order, each value field by field), never the order its map
//! happens to hold them in. Two replicas that applied the same history
//! must agree on it, and so must a replica that rebuilt by replay after a
//! restart and one that applied the same entries live.
//!
//! `Mutant` plants the bugs this check exists to catch, one at a time.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");

const Allocator = std.mem.Allocator;
const HASH_SEED: u64 = 0xFA81_17E5;

pub const Mutant = enum {
    none,
    /// Apply reads the node's clock instead of the entry's stamp.
    node_clock,
    /// Whether an entry is refused depends on the replica (here, whether
    /// it leads), not on the entry and the state alone.
    local_refusal,
    /// The checksum walks the map in its own order. A replica rebuilt by
    /// replay reserves its map up front, as boot does, so its order
    /// differs from a replica that grew its map live.
    map_order,
    /// Part of an entry's effect runs only when it applies live (a
    /// post-apply hook that owns state), so replay rebuilds less.
    live_only_effect,
};

/// What a family is told about the replica applying an entry. Only a
/// mutant reads it: a correct apply depends on the entry and the state.
pub const Replica = struct {
    mutant: Mutant,
    node_now: u64,
    leader: bool,
    live: bool,
};

pub const Outcome = enum { ok, refused };

/// Counters over a small key space, written by every workload op: a
/// `kv_delete` removes its key, any other op adds to it. A write is
/// refused when its condition (fresh keys only, for a third of payloads)
/// fails; its staged write is then discarded.
pub const Counter = struct {
    pub const KEYS = 8;

    pub const Value = struct {
        count: u64,
        /// A fold over the payloads written, in log order.
        fold: u64,
        /// The stamp of the last write.
        stamp: u64,
    };

    map: std.AutoHashMapUnmanaged(u64, Value) = .empty,

    pub fn deinit(self: *Counter, allocator: Allocator) void {
        self.map.deinit(allocator);
    }

    /// Empty, as a restarted replica starts. `reserve` sizes the map for
    /// the entries replay will apply, as boot reserves its index map.
    pub fn reset(self: *Counter, allocator: Allocator, reserve: u32) void {
        self.map.clearRetainingCapacity();
        if (reserve > 0) self.map.ensureTotalCapacity(allocator, reserve) catch @panic("sim OOM");
    }

    pub fn owns(entry_type: u8) bool {
        return switch (@as(entry_mod.EntryType, @fromBackingInt(entry_type))) {
            .kv_put, .kv_delete, .stream_append, .queue_enqueue, .ts_write => true,
            else => false,
        };
    }

    pub fn apply(self: *Counter, allocator: Allocator, e: *const entry_mod.Entry, r: Replica) Outcome {
        const h = std.hash.Wyhash.hash(HASH_SEED, e.payload);
        const key = h % KEYS;
        if (e.header.entry_type == @backingInt(entry_mod.EntryType.kv_delete)) {
            return if (self.map.remove(key)) .ok else .refused;
        }
        const prev = self.map.get(key);
        var conditional = h % 3 == 0;
        if (r.mutant == .local_refusal) conditional = conditional and r.leader;
        if (conditional and prev != null) return .refused;
        // Staged: nothing is written until the entry is known to be ok.
        const base: Value = prev orelse .{ .count = 0, .fold = 0, .stamp = 0 };
        var staged: Value = .{
            .count = base.count + 1,
            .fold = std.hash.Wyhash.hash(base.fold, std.mem.asBytes(&h)),
            .stamp = if (r.mutant == .node_clock) r.node_now else e.header.timestamp_ns,
        };
        if (r.mutant == .live_only_effect and !r.live) staged.fold = base.fold;
        self.map.put(allocator, key, staged) catch @panic("sim OOM");
        return .ok;
    }

    pub fn checksum(self: *const Counter, mutant: Mutant) u64 {
        var hasher = std.hash.Wyhash.init(HASH_SEED);
        if (mutant == .map_order) {
            var it = self.map.iterator();
            while (it.next()) |kv| hashPair(&hasher, kv.key_ptr.*, kv.value_ptr.*);
            return hasher.final();
        }
        var keys: [KEYS]u64 = undefined;
        var n: usize = 0;
        var it = self.map.keyIterator();
        while (it.next()) |k| : (n += 1) keys[n] = k.*;
        std.mem.sortUnstable(u64, keys[0..n], {}, std.sort.asc(u64));
        for (keys[0..n]) |k| hashPair(&hasher, k, self.map.get(k).?);
        return hasher.final();
    }

    fn hashPair(hasher: *std.hash.Wyhash, key: u64, v: Value) void {
        hasher.update(std.mem.asBytes(&key));
        hasher.update(std.mem.asBytes(&v.count));
        hasher.update(std.mem.asBytes(&v.fold));
        hasher.update(std.mem.asBytes(&v.stamp));
    }
};

const testing = std.testing;

fn put(c: *Counter, payload: []const u8, stamp: u64, r: Replica) Outcome {
    var e = entry_mod.buildEntry(.kv_put, 0, 1, 1, stamp, payload);
    e.header.crc32c = e.computeCrc();
    return c.apply(testing.allocator, &e, r);
}

test "families: the checksum is the same whatever the map's capacity" {
    const live: Replica = .{ .mutant = .none, .node_now = 0, .leader = false, .live = true };
    var a: Counter = .{};
    defer a.deinit(testing.allocator);
    var b: Counter = .{};
    defer b.deinit(testing.allocator);
    b.reset(testing.allocator, 4096);
    var payload: [8]u8 = undefined;
    for (0..64) |i| {
        std.mem.writeInt(u64, &payload, i, .little);
        try testing.expectEqual(put(&a, &payload, i + 1, live), put(&b, &payload, i + 1, live));
    }
    try testing.expectEqual(a.checksum(.none), b.checksum(.none));
    try testing.expect(a.checksum(.map_order) != b.checksum(.map_order));
}

test "families: a refused put leaves the state as it was" {
    const live: Replica = .{ .mutant = .none, .node_now = 0, .leader = false, .live = true };
    var c: Counter = .{};
    defer c.deinit(testing.allocator);
    var payload: [8]u8 = undefined;
    // A conditional payload, put twice: the second finds its key taken.
    var i: u64 = 0;
    while (std.hash.Wyhash.hash(HASH_SEED, std.mem.asBytes(&i)) % 3 != 0) i += 1;
    std.mem.writeInt(u64, &payload, i, .little);
    try testing.expectEqual(Outcome.ok, put(&c, &payload, 1, live));
    const before = c.checksum(.none);
    try testing.expectEqual(Outcome.refused, put(&c, &payload, 2, live));
    try testing.expectEqual(before, c.checksum(.none));
}
