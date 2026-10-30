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
//! `Namespaces` is shard 0's own namespace handler, its creates applied
//! through the handler's replay path.
//!
//! `Mutant` plants the bugs this check exists to catch, one at a time.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("../contract/store.zig");
const machine_mod = @import("../contract/machine.zig");
const kv_machine = @import("../kv/machine.zig");
const view_mod = @import("../contract/view.zig");
const kv_entries = @import("../protocol/kv_entries.zig");
const schema = @import("../protocol/schema.zig");
const ns_handler = @import("../namespace/handler.zig");

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
    /// The contract commits a refused entry's staged writes, the same on
    /// every replica: no comparison sees it, so `Kv.apply` checks refusals
    /// itself.
    kv_refused_commits,
    /// Shard 0's leader proposes a create before it has applied every
    /// earlier term's entry, so it may choose an id an earlier leader's
    /// create already took.
    ns_ungated,
    /// The same, once the leader's commit index has passed every earlier
    /// term's entry, applied or not: what only a leader with an unapplied
    /// committed tail can show.
    ns_ready_on_commit,
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
    /// What an entry did, for a run's counts (`Counts`).
    pub const Effect = enum {
        other,
        /// Refused before staging anything.
        refused,
        /// A batch refused, which has staged writes by then.
        staged_refused,
        /// A JSON patch merged into the value: a set below `$`.
        merged,
        /// A JSON patch that deleted a field.
        field_deleted,
    };

    /// Entries by outcome, each counted once however many replicas apply it.
    pub const Counts = struct {
        refusals: u64 = 0,
        staged_refusals: u64 = 0,
        merges: u64 = 0,
        field_deletes: u64 = 0,

        pub fn count(self: *Counts, o: Effect) void {
            switch (o) {
                .other => {},
                .refused => self.refusals += 1,
                .staged_refused => {
                    self.refusals += 1;
                    self.staged_refusals += 1;
                },
                .merged => self.merges += 1,
                .field_deleted => self.field_deletes += 1,
            }
        }
    };

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

    /// Apply `e` as the shard does. A refusal is an answer, not an error,
    /// but it must leave every family as it was: `RefusalChangedState` if
    /// one moved. Any other error (a halt, out of memory) is one a workload
    /// entry must never meet.
    pub fn apply(self: *Kv, e: *const entry_mod.Entry, r: Replica) (persistence.ApplyError || error{RefusalChangedState})!Effect {
        if (r.mutant == .kv_replay_skips_batches and !r.live and e.header.entry_type == @backingInt(entry_mod.EntryType.kv_batch)) return .other;
        var out: persistence.Result = .{};
        out.reset(e);
        const before = self.commits();
        if (r.mutant == .kv_refused_commits) {
            try self.applyCommittingRefusals(e, &out);
        } else {
            try machine_mod.applyEntry(&kv_machine.machine, &self.store, &self.scratch, e, &out);
        }
        if (out.refused() == null) return patchEffect(e);
        if (!std.mem.eql(u64, &before, &self.commits())) return error.RefusalChangedState;
        return if (e.header.entry_type == @backingInt(entry_mod.EntryType.kv_batch)) .staged_refused else .refused;
    }

    /// An applied JSON patch: merged into the value (a set below `$`), or a
    /// field deleted. A set at `$` writes the whole value and merges nothing.
    fn patchEffect(e: *const entry_mod.Entry) Effect {
        if (e.header.entry_type != @backingInt(entry_mod.EntryType.kv_json_patch)) return .other;
        var diag: schema.Diagnostic = .{};
        const p = kv_entries.JsonPatch.decodeAll(e.payload, &diag) catch return .other;
        if (std.mem.eql(u8, p.path, "$")) return .other;
        return if (p.document != null) .merged else .field_deleted;
    }

    /// `machine.applyEntry` without its guard against committing a refused
    /// entry: the bug `kv_refused_commits` plants.
    fn applyCommittingRefusals(self: *Kv, e: *const entry_mod.Entry, out: *persistence.Result) persistence.ApplyError!void {
        _ = self.scratch.reset(.retain_capacity);
        const arena = self.scratch.allocator();
        var keys = view_mod.KeySet.init(arena);
        try kv_machine.machine.keys(e, &keys);
        var view = view_mod.View.init(&self.store, arena, &keys);
        try kv_machine.machine.apply(&view, e, out);
        try view.commit();
    }

    /// Each family's count of records linked, replaced or unlinked.
    fn commits(self: *const Kv) [FAMILIES.len]u64 {
        var out: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &out) |fam, *c| c.* = self.store.families[fam].commits;
        return out;
    }

    pub fn checksums(self: *Kv) [FAMILIES.len]u64 {
        var sums: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &sums) |fam, *sum| sum.* = store_mod.checksum(&self.store, fam);
        return sums;
    }
};

/// Shard 0's namespace registry: the server's handler, on a table of its
/// own. The simulator proposes creates through it as shard 0's leader does
/// (`proposeCreateOn`) and applies them through its replay path. Nothing
/// is published: shard 0's create path reads its registry, never the table.
pub const Namespaces = struct {
    handler: ns_handler.NamespaceHandler,

    pub const Effect = enum { created, stale_id, refused };

    /// Creates by outcome, each counted once however many replicas apply it.
    pub const Counts = struct {
        created: u64 = 0,
        stale_ids: u64 = 0,
        refusals: u64 = 0,

        pub fn count(self: *Counts, e: Effect) void {
            switch (e) {
                .created => self.created += 1,
                .stale_id => self.stale_ids += 1,
                .refused => self.refusals += 1,
            }
        }
    };

    pub fn init(allocator: Allocator) Namespaces {
        return .{ .handler = ns_handler.NamespaceHandler.init(allocator) catch @panic("sim OOM") };
    }

    pub fn deinit(self: *Namespaces) void {
        self.handler.deinit();
    }

    /// Empty, as a restarted replica starts.
    pub fn reset(self: *Namespaces) void {
        const allocator = self.handler.allocator;
        self.handler.deinit();
        self.* = init(allocator);
    }

    pub fn owns(entry_type: u8) bool {
        return entry_type == @backingInt(entry_mod.EntryType.namespace_create);
    }

    /// Apply `e` as shard 0 does. An error is one a workload create must
    /// never meet.
    pub fn apply(self: *Namespaces, e: *const entry_mod.Entry) persistence.ApplyError!Effect {
        var out: persistence.Result = .{};
        out.reset(e);
        try self.handler.replayEntry(e, &out);
        const r = out.refused() orelse return .created;
        return if (std.mem.eql(u8, r.refusal.message, ns_handler.NamespaceHandler.STALE_ID_MESSAGE)) .stale_id else .refused;
    }

    /// The registry, the creates this log refused, and the next id, in no
    /// order of the maps': each name and each refused hash is hashed alone
    /// and the hashes summed.
    pub fn checksum(self: *const Namespaces) u64 {
        const h = &self.handler;
        var sum: u64 = std.hash.Wyhash.hash(HASH_SEED, std.mem.asBytes(&h.next_id));
        var it = h.namespaces.iterator();
        while (it.next()) |kv| {
            var hasher = std.hash.Wyhash.init(HASH_SEED);
            hasher.update(kv.key_ptr.*);
            const m = kv.value_ptr.*;
            hasher.update(std.mem.asBytes(&[3]u64{ m.id, m.created_at_ns, m.created_index }));
            sum +%= hasher.final();
        }
        var refused = h.refused_hashes.keyIterator();
        while (refused.next()) |hash| sum +%= std.hash.Wyhash.hash(HASH_SEED +% 1, std.mem.asBytes(hash));
        return sum;
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
    try testing.expectEqual(Kv.Effect.other, try kv.apply(&e, .{ .mutant = .none, .node_now = 0, .leader = false, .live = true }));
    try testing.expect(kv.checksums()[0] != empty[0]);
    kv.reset();
    try testing.expect(kv.store.strict_keys);
    try testing.expectEqual(empty, kv.checksums());
}

fn nsCreate(n: *Namespaces, index: u64, name: []const u8, id: u32) !Namespaces.Effect {
    var buf: [256]u8 = undefined;
    const p = try @import("../protocol/namespace_entries.zig").Create.encodeInto(&buf, .{ .name = name, .id = id });
    var e = entry_mod.buildEntry(.namespace_create, 0, 1, index, index, p);
    e.header.crc32c = e.computeCrc();
    return n.apply(&e);
}

test "families: a namespace create refused for its id folds into the checksum, and resets" {
    const first = @import("../namespace/table.zig").FIRST_ALLOCATED;
    var a = Namespaces.init(testing.allocator);
    defer a.deinit();
    var b = Namespaces.init(testing.allocator);
    defer b.deinit();
    const empty = a.checksum();
    try testing.expectEqual(Namespaces.Effect.created, try nsCreate(&a, 1, "orders", first));
    try testing.expectEqual(Namespaces.Effect.created, try nsCreate(&b, 1, "orders", first));
    try testing.expectEqual(a.checksum(), b.checksum());
    const created = a.checksum();
    // "ledger" asks for the id "orders" took: refused on both alike, and
    // the refusal is part of the state.
    try testing.expectEqual(Namespaces.Effect.stale_id, try nsCreate(&a, 2, "ledger", first));
    try testing.expect(a.checksum() != created);
    try testing.expect(a.checksum() != b.checksum());
    try testing.expectEqual(Namespaces.Effect.stale_id, try nsCreate(&b, 2, "ledger", first));
    try testing.expectEqual(a.checksum(), b.checksum());
    a.reset();
    try testing.expectEqual(empty, a.checksum());
}
