//! A shard's memory-pool use: the resident bytes of the families the pool
//! is charged for, read from the store's own per-family counters.
//!
//! The store keeps each family's encoded size and record count in its
//! commits (`Store.link`/`replace`/`unlink`), on every replica, replay
//! included, and a refused entry leaves both untouched. So the pool's use is
//! derived, never charged by hand: no apply can miscount it, and every
//! replica at the same applied index reads the same figure.
//!
//! Every family a registered machine declares is classified below as
//! charged or exempt; one in neither fails the build.

const std = @import("std");
const store_mod = @import("../contract/store.zig");
const machines = @import("../contract/machines.zig");
const plant = @import("../plant.zig");

const Store = store_mod.Store;

/// What one record holds in memory beyond its key and value bytes: the
/// `Record` (treap node and value slice) and the allocator's bookkeeping for
/// its three allocations (record, key, value). A fixed figure, the same on
/// every build, because a replicated decision reads it: change it as you
/// would a data format.
///
/// Derived, not fitted: each of the three allocations costs at most one
/// glibc minimum chunk (32 B) beyond its bytes, plus the `Record` itself.
/// Measured on Linux across keys of 5-40 B and values of 0-4097 B it is
/// 88-142 B. It models glibc's heap chunks only: a value past the mmap
/// threshold (128 KiB) is page-rounded, up to about 4 KiB more, under 3% of
/// such a value, and the pool doesn't count that.
pub const RESIDENT_RECORD: u64 = ALLOCATIONS * GLIBC_MIN_CHUNK + @sizeOf(store_mod.Record);

const ALLOCATIONS = 3;
const GLIBC_MIN_CHUNK = 32;

comptime {
    // The figure counts the store's encoding of a record's lengths as part of
    // its own overhead, so it must cover at least that.
    std.debug.assert(RESIDENT_RECORD >= store_mod.RECORD_OVERHEAD);
}

/// The families the pool is charged for: client data and the metadata a
/// client's creates leave.
pub const CHARGED = [_]u16{
    // namespace: name, meta (shard 0 holds the table)
    4,  5,
    // processing: job, cursor
    9,  63,
    // kv: value, history, ns_counts
    10, 11,
    12,
    // actions: def, run, delivery ready/deadline/request
    20,
    21, 22,
    23, 24,
    // queue: meta, msg, dead, delivery ready/deadline/request, held
    30, 31,
    32, 33,
    34, 35,
    36,
    // group: meta, item, dead, delivery ready/deadline/request, stream
    40,
    41, 42,
    43, 44,
    45, 46,
    // stream: meta, retention, record, reclaim
    47, 48,
    49, 53,
    // ts: series, point, field
    60, 61,
    62,
    // channels: out_ns, out, out_meta, parked. Never refused, but counted, so
    // full outboxes hold back client writes.
    54,
    55, 56,
    59,
    // history.act: an action run's history rows, charged where its runs are
    71,
    // step.calls; workflow parents, def, run
    25, 26,
    27, 28,
    // workflow: idem, per_def, out, sig
    64, 65,
    66, 67,
    // workflow: run_at, run_all; action: run_at. The lists' indexes
    72, 73,
    74,
    // action.counts: an action's runs by where they are
    75,
};

/// The families the pool is not charged for: the node's own state, and a
/// workflow run's history, which is bounded on disk instead.
pub const EXEMPT = [_]u16{
    // settings: policy, node, budget, bind
    50, 51,
    52, 68,
    // channels: in (one record per sender group)
    57,
    // history.wf: a workflow run's history rows, bounded on disk instead
    70,
};

fn listed(comptime set: []const u16, comptime id: u16) bool {
    for (set) |x| if (x == id) return true;
    return false;
}

/// The charged families, as a set the store's counters are summed over.
pub const charged: store_mod.FamilySet = blk: {
    var set: store_mod.FamilySet = .empty;
    for (CHARGED) |id| set.set(id);
    if (plant.active == .pool_counts_exempt_family) set.set(70);
    break :blk set;
};

comptime {
    @setEvalBranchQuota(100_000);
    var seen: store_mod.FamilySet = .empty;
    for (machines.list) |m| for (m.families) |f| {
        const c = listed(&CHARGED, f.id);
        const e = listed(&EXEMPT, f.id);
        if (c and e) @compileError(std.fmt.comptimePrint("family {d} ({s}) is both charged and exempt", .{ f.id, f.name }));
        if (!c and !e) @compileError(std.fmt.comptimePrint("family {d} ({s}) is neither charged nor exempt by the memory pool: classify it in budget/pool.zig", .{ f.id, f.name }));
        seen.set(f.id);
    };
    for (CHARGED ++ EXEMPT) |id| if (!seen.isSet(id)) @compileError(std.fmt.comptimePrint("family {d} is classified by the memory pool but no machine declares it", .{id}));
}

/// The channel families the pool is charged for. A sink's admission leaves
/// them out (`dataUseOf`): two shards over share mostly from what they send
/// each other would otherwise hold each other's sinks for good.
pub const CHANNEL = [_]u16{ 54, 55, 56, 59 };

/// The charged families but the channels'.
pub const data: store_mod.FamilySet = blk: {
    var set = charged;
    for (CHANNEL) |id| set.unset(id);
    break :blk set;
};

comptime {
    for (CHANNEL) |id| if (!listed(&CHARGED, id)) @compileError(std.fmt.comptimePrint("channel family {d} isn't charged", .{id}));
}

/// One family's charged bytes.
fn familyUse(store: *const Store, id: u16) u64 {
    const f = store.family(id);
    return f.encoded_bytes + f.count * (RESIDENT_RECORD - store_mod.RECORD_OVERHEAD);
}

/// The shard's pool use: every charged family's records, their keys, values
/// and `RESIDENT_RECORD` each.
pub fn useOf(store: *const Store) u64 {
    var total: u64 = 0;
    var it = charged.iterator(.{});
    while (it.next()) |id| total += familyUse(store, @intCast(id));
    return total;
}

/// The shard's pool use but its channels': what a sink's message is admitted
/// against.
pub fn dataUseOf(store: *const Store) u64 {
    var total: u64 = 0;
    var it = data.iterator(.{});
    while (it.next()) |id| total += familyUse(store, @intCast(id));
    return total;
}

/// The same figure from a walk of every charged record, read off the table
/// itself: what `useOf` must equal, whatever wrote the store. O(records), so
/// a checker samples it.
pub fn recount(store: *Store) u64 {
    var total: u64 = 0;
    for (CHARGED) |id| {
        var r = store.range(id, "");
        while (r.next()) |rec| total += rec.key().len + rec.value.len + RESIDENT_RECORD;
    }
    return total;
}

const testing = std.testing;

test "pool: a sink is admitted against use but the channels'" {
    for (CHANNEL) |id| try testing.expect(charged.isSet(id) and !data.isSet(id));
    var store = Store.init(testing.allocator);
    defer store.deinit();
    const kv = try store.allocRecord("k");
    store.link(10, kv, try testing.allocator.dupe(u8, "v"));
    const out = try store.allocRecord("o");
    store.link(55, out, try testing.allocator.dupe(u8, "outbox"));
    try testing.expectEqual(familyUse(&store, 10), dataUseOf(&store));
    try testing.expectEqual(familyUse(&store, 10) + familyUse(&store, 55), useOf(&store));
}

test "pool: channel outboxes and parked messages are charged; inboxes are not" {
    // Full outboxes must show in the pool; an inbox is one record per sender.
    for ([_]u16{ 54, 55, 56, 59 }) |id| try testing.expect(charged.isSet(id));
    try testing.expect(!charged.isSet(57));
}

test "pool: a record's resident figure covers the store's layout of it" {
    // The layout `RESIDENT_RECORD` was calibrated against. A change here
    // moves real memory per record: calibrate again (on Linux) and change the
    // figure as a format change, or confirm it still holds.
    try testing.expectEqual(@as(usize, 64), @sizeOf(store_mod.Record));
    // The literal replicas agree on; it moves only with the derivation above.
    try testing.expectEqual(@as(u64, 160), RESIDENT_RECORD);

    // Three allocations per record: the record, its key and its value.
    var counting = CountingAllocator{ .child = testing.allocator };
    var store = Store.init(counting.allocator());
    defer store.deinit();
    const before = counting.allocs;
    const rec = try store.allocRecord("key");
    store.link(10, rec, try counting.allocator().dupe(u8, "value"));
    try testing.expectEqual(@as(usize, 3), counting.allocs - before);
}

test "pool: use is what the charged records hold, and exempt families add nothing" {
    var store = Store.init(testing.allocator);
    defer store.deinit();
    try testing.expectEqual(@as(u64, 0), useOf(&store));

    store.link(10, try store.allocRecord("k1"), try testing.allocator.dupe(u8, "value"));
    store.link(31, try store.allocRecord("m"), try testing.allocator.dupe(u8, "payload!"));
    const want = (2 + 5 + RESIDENT_RECORD) + (1 + 8 + RESIDENT_RECORD);
    try testing.expectEqual(want, useOf(&store));
    try testing.expectEqual(want, recount(&store));

    // An exempt family's records aren't charged.
    store.link(70, try store.allocRecord("row"), try testing.allocator.dupe(u8, "a workflow run's history"));
    try testing.expectEqual(want, useOf(&store));

    // A smaller value gives bytes back; a removed record gives all of it.
    store.replace(10, store.find(10, "k1").?, try testing.allocator.dupe(u8, "v"));
    try testing.expectEqual(want - 4, useOf(&store));
    store.unlink(10, store.find(10, "k1").?);
    try testing.expectEqual(@as(u64, 1 + 8 + RESIDENT_RECORD), useOf(&store));
    try testing.expectEqual(useOf(&store), recount(&store));
}

/// Counts allocations, for the layout test.
const CountingAllocator = struct {
    child: std.mem.Allocator,
    allocs: usize = 0,

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.allocs += 1;
        return self.child.rawAlloc(len, alignment, ret);
    }

    fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.child.rawFree(buf, alignment, ret);
    }
};
