//! VOPR state families: toy state machines each replica applies, so the
//! checker can compare applied state, not only the log.
//!
//! A family's checksum is a hash of its state in a canonical order (keys
//! in key order, each value field by field), never the order its map
//! happens to hold them in. Two replicas that applied the same history
//! must agree on it, and so must a replica that rebuilt by replay after a
//! restart and one that applied the same entries live.
//!
//! `Kv` is the server's own KV machine on a store of its own, compared by
//! the contract's canonical checksum of each of its families.
//!
//! `Mutant` plants the bugs this check exists to catch, one at a time.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("../contract/store.zig");
const machine_mod = @import("../contract/machine.zig");
const kv_machine = @import("../kv/machine.zig");

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
    /// A replica rebuilding by replay skips KV's batches.
    kv_replay_skips_batches,
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

/// KV as the server applies it: its machine, on a store of this replica's
/// own. The store is strict, so an applier touching a key its entry didn't
/// declare fails the run; VOPR builds ReleaseSafe, where a store would only
/// count it.
pub const Kv = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,

    /// The families compared, in the order `checksums` returns them.
    pub const FAMILIES = [_]u16{ kv_machine.Values.ID, kv_machine.History.ID, kv_machine.NsCounts.ID };

    pub fn init(allocator: Allocator) Kv {
        var kv: Kv = .{ .store = store_mod.Store.init(allocator), .scratch = std.heap.ArenaAllocator.init(allocator) };
        kv.store.strict_keys = true;
        return kv;
    }

    pub fn deinit(self: *Kv) void {
        self.store.deinit();
        self.scratch.deinit();
    }

    /// Empty, as a restarted replica starts; still strict.
    pub fn reset(self: *Kv) void {
        self.store.clear();
        std.debug.assert(self.store.strict_keys);
    }

    pub fn owns(entry_type: u8) bool {
        return kv_machine.owns(@fromBackingInt(entry_type));
    }

    /// Apply `e` as the shard does. A refusal is an answer, not an error;
    /// an error (a halt, out of memory) is one a workload entry must never
    /// meet.
    pub fn apply(self: *Kv, e: *const entry_mod.Entry, r: Replica) persistence.ApplyError!void {
        if (r.mutant == .kv_replay_skips_batches and !r.live and e.header.entry_type == @backingInt(entry_mod.EntryType.kv_batch)) return;
        var out: persistence.Result = .{};
        out.reset(e);
        try machine_mod.applyEntry(&kv_machine.machine, &self.store, &self.scratch, e, &out);
    }

    pub fn checksums(self: *Kv) [FAMILIES.len]u64 {
        var sums: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &sums) |fam, *sum| sum.* = store_mod.checksum(&self.store, fam);
        return sums;
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

test "families: a KV replica's store is strict, and stays so once reset" {
    var kv = Kv.init(testing.allocator);
    defer kv.deinit();
    try testing.expect(kv.store.strict_keys);
    var body_buf: [64]u8 = undefined;
    const p = try @import("../protocol/kv_entries.zig").Put.encodeInto(&body_buf, .{ .key = "k", .value = "v", .ttl_ms = null });
    var e = entry_mod.buildEntry(.kv_put, 0, 1, 1, 1, p);
    e.header.crc32c = e.computeCrc();
    const empty = kv.checksums();
    try kv.apply(&e, .{ .mutant = .none, .node_now = 0, .leader = false, .live = true });
    try testing.expect(kv.checksums()[0] != empty[0]);
    kv.reset();
    try testing.expect(kv.store.strict_keys);
    try testing.expectEqual(empty, kv.checksums());
}
