//! Partition — owns a UAL and its projections
//!
//! Each Partition represents one Raft group's data. The Shard owns
//! multiple Partitions (those where `partition_id % shard_count == shard_id`).
//!
//! Data flow:
//!   propose(entry) → UAL.append → ProjectionRouter.apply → advance applied_index

const contract_store = @import("../contract/store.zig");
const std = @import("std");
const Allocator = std.mem.Allocator;
const entry_mod = @import("ual/entry.zig");
const kv_entries = @import("../protocol/kv_entries.zig");
const ual_mod = @import("ual/ual.zig");
const tombstones = @import("tombstones.zig");
const router_mod = @import("../projection/router.zig");
const kv_state = @import("../kv/state.zig");
const kv_machine = @import("../kv/machine.zig");
const stream_mod = @import("../projection/stream.zig");
const ts_mod = @import("../projection/ts.zig");
const log = @import("stdx").log;

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const UAL = ual_mod.UAL;
const ProjectionRouter = router_mod.ProjectionRouter;

// ═══════════════════════════════════════════════════════════════════════════════
// Partition
// ═══════════════════════════════════════════════════════════════════════════════

pub const Partition = struct {
    /// Partition identity.
    id: u32,
    group_id: u32,

    /// Storage.
    ual: UAL,
    router: ProjectionRouter,

    /// Projections (owned by partition).
    /// KV's state as reads see it: the KV machine's families in `store`.
    kv: kv_state.KvState,
    stream: stream_mod.StreamProjection,
    ts: ts_mod.TSProjection,

    /// The state of every family a contract machine keeps, and the scratch
    /// one entry's staged writes live in.
    store: contract_store.Store,
    /// Logical bytes taken since the last snapshot bar: each entry's header
    /// and payload, for every index, refused and passed-over ones too.
    bar_bytes: u64 = 0,
    /// The last bar's index; 0 before the first.
    last_bar: u64 = 0,
    /// The highest index counted toward a bar.
    bar_counted_through: u64 = 0,
    apply_scratch: std.heap.ArenaAllocator,

    /// Raft state placeholders (real Raft added in Phase 3).
    current_term: u64,
    committed_index: u64,

    /// The highest allocated namespace id an entry applied here named,
    /// bound on this shard or not: state here may be keyed by it.
    highest_namespace: u32 = 0,

    /// Warm store — payload copies for entries evicted from the hot ring buffer.
    /// Populated on every non-KV apply() so reads can fall back to warm when UAL evicts.
    warm_store: std.AutoHashMapUnmanaged(u64, []const u8),
    /// Under a budget the warm store holds steady, evicting as it adds;
    /// this keeps its tombstones bounded.
    warm_sweep: tombstones.Sweep = .{},

    /// The warm store's indices, lowest first, for eviction without a scan
    /// of the map. Every index in the store is here. An index put again
    /// (which the ring's increasing indices rule out outside tests) leaves
    /// a stale entry, skipped when it reaches the top.
    warm_order: std.PriorityQueue(u64, void, lowerIndexFirst) = .empty,

    /// Warm store memory tracking (bounded by warm_budget).
    warm_bytes_used: usize,
    warm_budget: usize,

    /// Lifecycle.
    allocator: Allocator,

    pub const DEFAULT_UAL_CAPACITY: usize = 64 * 1024 * 1024; // 64 MB
    pub const DEFAULT_GROUP_OFFSET: u32 = 1000;
    pub const DEFAULT_WARM_BUDGET: usize = 32 * 1024 * 1024; // 32 MB

    // ── Construction ────────────────────────────────────────────────────

    pub fn init(allocator: Allocator, partition_id: u32, ual_capacity: usize, max_hot_entries: u64) !Partition {
        var ual = try UAL.init(allocator, ual_capacity, max_hot_entries);
        errdefer ual.deinit();

        const stream = stream_mod.StreamProjection.init(allocator);
        const ts = ts_mod.TSProjection.init(allocator, .{});

        var part = Partition{
            .id = partition_id,
            .group_id = partition_id + DEFAULT_GROUP_OFFSET,
            .ual = ual,
            .router = ProjectionRouter.init(),
            // Pointed at `store` once the partition is where it stays.
            .kv = .{ .store = undefined },
            .stream = stream,
            .ts = ts,
            .store = contract_store.Store.init(allocator),
            .apply_scratch = std.heap.ArenaAllocator.init(allocator),
            .current_term = 0,
            .committed_index = 0,
            .warm_store = .{},
            .warm_bytes_used = 0,
            .warm_budget = DEFAULT_WARM_BUDGET,
            .allocator = allocator,
        };

        // NOTE: Do NOT register projections here — the struct will move
        // when returned by value, invalidating self pointers.
        // Call wireProjections() after the Partition is at its final address.
        _ = &part;

        return part;
    }

    /// Register projection handles with the router.
    /// MUST be called after init(), once the Partition is at its final address.
    pub fn wireProjections(self: *Partition) void {
        self.kv.store = &self.store;
        self.router.registerTS(self.ts.projectionHandle());
    }

    /// Set the warm store budget (max bytes before eviction).
    pub fn setWarmBudget(self: *Partition, budget: usize) void {
        self.warm_budget = budget;
    }

    pub fn deinit(self: *Partition) void {
        // Free warm store payload copies
        var wit = self.warm_store.iterator();
        while (wit.next()) |kv| {
            self.allocator.free(@constCast(kv.value_ptr.*));
        }
        self.warm_store.deinit(self.allocator);
        self.warm_order.deinit(self.allocator);

        self.kv.deinit(self.allocator);
        self.apply_scratch.deinit();
        self.store.deinit();
        self.ts.deinit();
        self.stream.deinit();
        self.ual.deinit();
    }

    // ── Write Path ──────────────────────────────────────────────────────

    /// `applyRouted`, for a caller that needs only the index: tests and
    /// the handlers' no-shard paths.
    pub fn apply(self: *Partition, e: *const Entry) !u64 {
        var out = router_mod.Result.start(e);
        return (try self.applyRouted(e, &out)).index;
    }

    /// `failed`: why the projection the router owns could not apply it.
    pub const Applied = struct { index: u64, route: router_mod.ApplyResult, failed: ?router_mod.ApplyError = null };

    /// Apply a committed entry: route it to its projection and, for every
    /// type but KV, append it to the hot ring and warm store. Returns the
    /// entry's index and what the router made of it, which the shard counts
    /// when a projection failed.
    pub fn applyRouted(self: *Partition, e: *const Entry, out: *router_mod.Result) !Applied {
        return self.take(e, out, .apply);
    }

    /// Take an entry a repair skips as a halt on it left the partition: in
    /// the hot ring and warm store, and in no projection. A replica that
    /// halted on it live and one that boots past it then hold the same. The
    /// router isn't told: the next entry it applies moves it past.
    pub fn passOverSkipped(self: *Partition, e: *const Entry) error{OutOfMemory}!void {
        var out: router_mod.Result = .{};
        _ = self.take(e, &out, .pass_over) catch |err| switch (err) {
            // Refused before anything was taken, as the halt on it was.
            error.EntryTooLarge, error.IndexNotIncreasing => return,
            else => |e2| return e2,
        };
    }

    fn take(self: *Partition, e: *const Entry, out: *router_mod.Result, how: enum { apply, pass_over }) !Applied {
        // The hot ring and the warm store exist for the stream read path,
        // which fetches record payloads by index. KV's families hold its
        // values and nothing reads a KV payload back from the partition
        // log, so KV entries skip both copies.
        const is_kv = kv_machine.owns(@fromBackingInt(@intCast(e.header.entry_type)));
        const index = if (is_kv) e.header.index else try self.ual.append(e);

        // Route to projections via the real router (idempotent)
        var failed: ?router_mod.ApplyError = null;
        const route = switch (how) {
            .apply => self.router.apply(e, out) catch |err| blk: {
                failed = err;
                break :blk .skipped_no_projection;
            },
            .pass_over => .skipped_no_projection,
        };

        // Track committed index
        if (e.header.index > self.committed_index) {
            self.committed_index = e.header.index;
        }
        self.countForBar(e);

        log.debug("Partition: applied entry, partition_id={d}, index={d}, type={d}", .{ self.id, e.header.index, e.header.entry_type });

        // Save payload to warm store (survives UAL hot ring eviction).
        // Uses the entry's own index as key.
        if (e.payload.len > 0 and !is_kv) {
            const copy = self.allocator.dupe(u8, e.payload) catch return .{ .index = index, .route = route, .failed = failed };
            // Room in the order first, so every index stored is in it.
            self.warm_order.ensureUnusedCapacity(self.allocator, 1) catch {
                self.allocator.free(copy);
                return .{ .index = index, .route = route, .failed = failed };
            };

            // Free old copy if this index was already in warm store (idempotent apply)
            if (self.warm_store.fetchRemove(e.header.index)) |old| {
                self.warm_sweep.noteRemoved([]const u8, &self.warm_store);
                self.warm_bytes_used -= old.value.len;
                self.allocator.free(@constCast(old.value));
            }

            self.warm_store.put(self.allocator, e.header.index, copy) catch {
                self.allocator.free(copy);
                return .{ .index = index, .route = route, .failed = failed };
            };
            self.warm_bytes_used += copy.len;
            self.noteWarmIndex(e.header.index);

            // Evict oldest warm entries if over budget
            self.evictWarmIfNeeded();
        }

        return .{ .index = index, .route = route, .failed = failed };
    }

    /// Bars fall at least this many logical bytes apart.
    pub const BAR_MIN_BYTES: u64 = 64 * 1024 * 1024;

    /// The bytes since the last bar after which the next one falls: at least
    /// `BAR_MIN_BYTES`, and at least the state a snapshot would encode, so a
    /// snapshot costs no more than the log written since the last.
    pub fn barThreshold(self: *const Partition) u64 {
        return @max(barMinBytes(), self.store.encodedBytes());
    }

    /// Tests only: a smaller minimum, so a bar comes within a few entries.
    /// Global, so a shard that replays its log at boot sees it too.
    pub var test_bar_min_bytes: ?u64 = null;

    fn barMinBytes() u64 {
        if (@import("builtin").is_test) {
            if (test_bar_min_bytes) |b| return b;
        }
        return BAR_MIN_BYTES;
    }

    /// Counts an entry just taken towards the next bar, and makes it the bar
    /// if it reaches it. Every replica takes the same entries over the same
    /// state, so every replica finds the same bars.
    /// Each index counts once, however often it is taken: a KV index skips
    /// the ring that refuses a second take, so one applied and then halted
    /// on is taken again when a repair passes over it, where a replay that
    /// knows of the repair takes it only once.
    /// The threshold reads the state before the entry applies: the shard
    /// runs the entry's machine after the take.
    fn countForBar(self: *Partition, e: *const Entry) void {
        if (e.header.index <= self.bar_counted_through) return;
        self.bar_counted_through = e.header.index;
        self.bar_bytes += entry_mod.HEADER_SIZE + e.payload.len;
        if (self.bar_bytes < self.barThreshold()) return;
        self.last_bar = e.header.index;
        self.bar_bytes = 0;
    }

    fn lowerIndexFirst(_: void, a: u64, b: u64) std.math.Order {
        return std.math.order(a, b);
    }

    /// An index just put in the warm store, in eviction order. The caller
    /// made room for it.
    fn noteWarmIndex(self: *Partition, index: u64) void {
        self.warm_order.push(self.allocator, index) catch unreachable;
        // Replaced indices leave stale entries; once they outnumber the live
        // ones, start again from the map.
        if (self.warm_order.count() > 2 * self.warm_store.count() + 1024) self.rebuildWarmOrder();
    }

    /// Drops the stale entries: the order holds more than the store, so
    /// its capacity fits the store and nothing is allocated.
    fn rebuildWarmOrder(self: *Partition) void {
        self.warm_order.clearRetainingCapacity();
        var it = self.warm_store.keyIterator();
        while (it.next()) |k| self.warm_order.push(self.allocator, k.*) catch unreachable;
    }

    /// Evict oldest entries from the warm store when over budget: the lowest
    /// UAL index first, from `warm_order`, never a scan of the map.
    fn evictWarmIfNeeded(self: *Partition) void {
        if (self.warm_budget == 0) return; // 0 = unlimited
        while (self.warm_bytes_used > self.warm_budget and self.warm_store.count() > 0) {
            // Every stored index is in the order, so it isn't empty here.
            const lowest = self.warm_order.pop() orelse unreachable;
            // Stale: replaced since, or gone already.
            const removed = self.warm_store.fetchRemove(lowest) orelse continue;
            self.warm_sweep.noteRemoved([]const u8, &self.warm_store);
            self.warm_bytes_used -= removed.value.len;
            self.allocator.free(@constCast(removed.value));
        }
    }

    // ── Read Path ───────────────────────────────────────────────────────

    /// Read an entry from the UAL by index (zero-copy when possible).
    pub fn read(self: *const Partition, index: u64) ?Entry {
        return self.ual.read(index);
    }

    /// Read an entry with copy (handles ring wrap-around).
    pub fn readCopy(self: *const Partition, index: u64, payload_buf: []u8) ?Entry {
        return self.ual.readCopy(index, payload_buf);
    }

    /// Check if an entry exists in the hot ring.
    pub fn contains(self: *const Partition, index: u64) bool {
        return self.ual.contains(index);
    }

    /// Read a payload from the warm store (entries evicted from hot ring).
    /// Returns the raw entry payload bytes, or null if not found.
    pub fn readPayloadWarm(self: *const Partition, index: u64) ?[]const u8 {
        return self.warm_store.get(index);
    }

    /// Check if an entry is still in the hot ring buffer.
    /// Uses contains() (index-map membership), not read(): read() returns null
    /// for a boundary-wrapping payload, which would falsely report a live entry
    /// as evicted.
    pub fn isInHot(self: *const Partition, index: u64) bool {
        return self.ual.contains(index);
    }

    // ── Projection accessors ────────────────────────────────────────────

    /// A KV key's live value.
    pub fn kvGet(self: *Partition, key: []const u8) ?[]const u8 {
        const entry = self.kv.get(key) orelse return null;
        return entry.value;
    }

    /// Get the stream high water mark.
    pub fn streamHWM(self: *const Partition) u64 {
        return self.stream.highWaterMark();
    }

    // ── Snapshot ────────────────────────────────────────────────────────

    // ── Queries ─────────────────────────────────────────────────────────

    /// The last applied UAL index (projections are up to date through this).
    pub fn appliedIndex(self: *const Partition) u64 {
        return self.router.applied_index;
    }

    /// Number of entries currently in the hot ring.
    pub fn entryCount(self: *const Partition) u64 {
        return self.ual.entry_count;
    }

    /// UAL hot ring memory usage.
    pub fn ualUsed(self: *const Partition) u64 {
        return self.ual.used();
    }

    /// Warm store memory usage in bytes.
    pub fn warmUsed(self: *const Partition) usize {
        return self.warm_bytes_used;
    }

    /// Number of entries in the warm store.
    pub fn warmCount(self: *const Partition) usize {
        return self.warm_store.count();
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn makeEntry(entry_type: EntryType, index: u64, term: u64, payload: []const u8) Entry {
    return entry_mod.buildEntry(
        entry_type,
        entry_mod.Flags.NONE,
        term,
        index,
        0, // timestamp
        payload,
    );
}

test "partition: init and deinit" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 42, 4096, 0);
    defer part.deinit();
    part.wireProjections();

    try testing.expectEqual(@as(u32, 42), part.id);
    try testing.expectEqual(@as(u32, 1042), part.group_id);
    try testing.expectEqual(@as(u64, 0), part.appliedIndex());
    try testing.expectEqual(@as(u64, 0), part.entryCount());
}

/// KV entry `entry_type` carrying `payload`, at `index`, stamped `ts`.
fn kvTestEntry(buf: []u8, comptime entry_type: EntryType, payload: kv_entries.Payload(@backingInt(entry_type)).Value, index: u64, ts: u64) entry_mod.Entry {
    const bytes = kv_entries.Payload(@backingInt(entry_type)).encodeInto(buf, payload) catch unreachable;
    var e = entry_mod.buildEntry(entry_type, entry_mod.Flags.NONE, 1, index, ts, bytes);
    e.header.crc32c = e.computeCrc();
    return e;
}

test "partition: apply and read entries" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    // Apply a KV put
    var payload_buf: [128]u8 = undefined;
    const e1 = kvTestEntry(&payload_buf, .kv_put, .{ .key = @import("../namespace/table.zig").DEFAULT.bytes() ++ "key1", .value = "val1", .ttl_ms = null, .condition = .always, .cas_version = null }, 1, 1000);
    const idx1 = try part.apply(&e1);
    try testing.expectEqual(@as(u64, 1), idx1);

    // Apply a stream append
    var e2 = makeEntry(.stream_append, 2, 1, "stream-data");
    e2.header.crc32c = e2.computeCrc();
    const idx2 = try part.apply(&e2);
    try testing.expectEqual(@as(u64, 2), idx2);

    // The stream entry reads back from the ring; the KV entry is held by the
    // projection alone and never enters it.
    const read2 = part.read(2).?;
    try testing.expectEqual(@as(u8, @backingInt(EntryType.stream_append)), read2.header.entry_type);
    try testing.expect(part.read(1) == null);

    try testing.expectEqual(@as(u64, 2), part.appliedIndex());
    try testing.expectEqual(@as(u64, 2), part.committed_index);
}

test "partition: contains check" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 4096, 0);
    defer part.deinit();
    part.wireProjections();

    try testing.expect(!part.contains(1));

    var e1 = makeEntry(.stream_append, 1, 1, "data");
    e1.header.crc32c = e1.computeCrc();
    _ = try part.apply(&e1);

    try testing.expect(part.contains(1));
    try testing.expect(!part.contains(2));
}

test "partition: warm store byte tracking" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    try testing.expectEqual(@as(usize, 0), part.warmUsed());
    try testing.expectEqual(@as(usize, 0), part.warmCount());

    var e1 = makeEntry(.stream_append, 1, 1, "hello-world");
    e1.header.crc32c = e1.computeCrc();
    _ = try part.apply(&e1);

    try testing.expect(part.warmUsed() > 0);
    try testing.expectEqual(@as(usize, 1), part.warmCount());
}

test "partition: warm store eviction under budget" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    // Set a very tight warm budget (100 bytes)
    part.setWarmBudget(100);

    // Apply entries with payloads that accumulate beyond 100 bytes. Not KV:
    // KV entries never reach the warm store.
    var i: u64 = 1;
    while (i <= 20) : (i += 1) {
        var e = makeEntry(.stream_append, i, 1, "0123456789"); // 10 bytes each
        e.header.crc32c = e.computeCrc();
        _ = try part.apply(&e);
    }

    // After eviction, warm usage should be at or under budget
    try testing.expect(part.warmUsed() <= 100);
    // Oldest entries should have been evicted
    try testing.expectEqual(@as(usize, 10), part.warmCount());
    try testing.expect(part.warm_store.get(20) != null and part.warm_store.get(10) == null);
}

test "partition: warm eviction takes the lowest index first, whatever order the indices came in, replaced ones included" {
    var part = try Partition.init(testing.allocator, 0, 8192, 0);
    defer part.deinit();
    // Put by hand in any order, with 5 put twice (a replaced copy), so the
    // order holds a stale entry for it.
    for ([_]u64{ 5, 3, 9, 1, 5, 7 }) |i| {
        if (part.warm_store.fetchRemove(i)) |old| {
            part.warm_bytes_used -= old.value.len;
            part.allocator.free(@constCast(old.value));
        }
        try part.warm_store.put(part.allocator, i, try part.allocator.dupe(u8, "0123456789"));
        part.warm_bytes_used += 10;
        try part.warm_order.ensureUnusedCapacity(part.allocator, 1);
        part.noteWarmIndex(i);
    }
    // Room for two: 1, 3 and 5 go, lowest first, never 7 or 9.
    part.setWarmBudget(25);
    part.evictWarmIfNeeded();
    try testing.expectEqual(@as(usize, 2), part.warmCount());
    try testing.expect(part.warm_store.contains(7) and part.warm_store.contains(9));
    // The stale 5 is passed over, and 7 is next.
    part.setWarmBudget(15);
    part.evictWarmIfNeeded();
    try testing.expectEqual(@as(usize, 1), part.warmCount());
    try testing.expect(part.warm_store.contains(9));
}

test "partition: every index the warm store holds is in its eviction order" {
    var part = try Partition.init(testing.allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();
    part.setWarmBudget(100);
    var i: u64 = 1;
    while (i <= 40) : (i += 1) {
        var e = makeEntry(.stream_append, i, 1, "0123456789");
        e.header.crc32c = e.computeCrc();
        _ = try part.apply(&e);
        try testing.expectEqual(part.warm_store.count(), part.warm_order.count());
    }
    try testing.expectEqual(@as(usize, 10), part.warmCount());
}

test "partition: indices put again leave stale entries in the eviction order, bounded" {
    var part = try Partition.init(testing.allocator, 0, 8192, 0);
    defer part.deinit();
    // Ten indices put over and over: each put again leaves a stale entry.
    for (0..5000) |n| {
        const idx: u64 = n % 10;
        if (part.warm_store.fetchRemove(idx)) |old| {
            part.warm_bytes_used -= old.value.len;
            part.allocator.free(@constCast(old.value));
        }
        try part.warm_order.ensureUnusedCapacity(part.allocator, 1);
        try part.warm_store.put(part.allocator, idx, try part.allocator.dupe(u8, "0123456789"));
        part.warm_bytes_used += 10;
        part.noteWarmIndex(idx);
        try testing.expect(part.warm_order.count() <= 2 * part.warm_store.count() + 1024);
    }
    try testing.expectEqual(@as(usize, 10), part.warmCount());
}

test "partition: a warm store held at its budget keeps its tombstones bounded" {
    var part = try Partition.init(testing.allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();
    // 1,000 held: a table large enough that tombstones alone could fill
    // more than half of it.
    part.setWarmBudget(10_000);

    var i: u64 = 1;
    while (i <= 100_000) : (i += 1) {
        var e = makeEntry(.stream_append, i, 1, "0123456789");
        e.header.crc32c = e.computeCrc();
        _ = try part.apply(&e);
        try testing.expect(tombstones.count([]const u8, &part.warm_store) <= part.warm_store.capacity() / 2);
    }
    try testing.expectEqual(@as(usize, 1000), part.warmCount());
    var live: u64 = 100_000 - 999;
    while (live <= 100_000) : (live += 1) try testing.expectEqualStrings("0123456789", part.warm_store.get(live).?);
}

test "partition: warm budget zero means unlimited" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    // Set budget to 0 (unlimited)
    part.setWarmBudget(0);

    // Apply many entries — no eviction should happen
    var i: u64 = 1;
    while (i <= 50) : (i += 1) {
        var e = makeEntry(.stream_append, i, 1, "0123456789ABCDEF"); // 16 bytes each
        e.header.crc32c = e.computeCrc();
        _ = try part.apply(&e);
    }

    // All 50 entries should be in warm store
    try testing.expectEqual(@as(usize, 50), part.warmCount());
    try testing.expectEqual(@as(usize, 50 * 16), part.warmUsed());
}

test "partition: kv entries reach the projection but neither the ring nor the warm store" {
    const allocator = testing.allocator;
    var part = try Partition.init(allocator, 0, 64 * 1024, 0);
    defer part.deinit();

    var e1 = makeEntry(.kv_put, 1, 1, "kv-payload-that-would-otherwise-be-cached");
    try testing.expectEqual(@as(u64, 1), try part.apply(&e1));
    try testing.expectEqual(@as(u64, 0), part.warm_bytes_used);
    try testing.expect(part.readPayloadWarm(1) == null);
    try testing.expect(!part.ual.contains(1));
    try testing.expectEqual(@as(u64, 1), part.committed_index);

    var e2 = makeEntry(.stream_append, 2, 1, "stream-payload");
    _ = try part.apply(&e2);
    try testing.expectEqual(@as(u64, "stream-payload".len), part.warm_bytes_used);
    try testing.expect(part.readPayloadWarm(2) != null);
}

/// Applies `entries` in order and returns the bars found, in order.
/// Applies `entries`, noting each bar and the state's size where it fell.
fn barsOf(part: *Partition, entries: []const Entry, out: []u64, states: []u64) ![]u64 {
    var n: usize = 0;
    var last: u64 = part.last_bar;
    for (entries) |*e| {
        _ = try part.apply(e);
        if (part.last_bar != last) {
            last = part.last_bar;
            out[n] = last;
            states[n] = part.store.encodedBytes();
            n += 1;
        }
    }
    return out[0..n];
}

test "partition: two replicas applying the same log find the same bars, every index counted once" {
    const allocator = testing.allocator;
    Partition.test_bar_min_bytes = 600;
    defer Partition.test_bar_min_bytes = null;
    // Stream appends of varying sizes, empty entries and KV puts. Taken
    // alone, with no machine run, the state stays empty: the shard's test
    // of two replicas moves it.
    const n = 80;
    var payloads: [n][64]u8 = undefined;
    var kv_bufs: [n][160]u8 = undefined;
    var entries: [n]Entry = undefined;
    const ns = @import("../namespace/table.zig").DEFAULT.bytes();
    for (&entries, 0..) |*e, i| {
        const len = (i * 7) % 60;
        @memset(payloads[i][0..len], 'p');
        if (i % 5 == 0) {
            e.* = makeEntry(.raft_noop, i + 1, 1, "");
        } else if (i % 3 == 0) {
            var key: [ns.len + 2]u8 = undefined;
            @memcpy(key[0..ns.len], &ns);
            key[ns.len] = 'k';
            key[ns.len + 1] = '0' + @as(u8, @intCast(i % 7));
            e.* = kvTestEntry(&kv_bufs[i], .kv_put, .{ .key = &key, .value = payloads[i][0..len], .ttl_ms = null, .condition = .always, .cas_version = null }, i + 1, 1000 + i);
            continue;
        } else {
            e.* = makeEntry(.stream_append, i + 1, 1, payloads[i][0..len]);
        }
        e.header.crc32c = e.computeCrc();
    }
    var bars: [2][n]u64 = undefined;
    var states: [2][n]u64 = undefined;
    var found: [2][]u64 = undefined;
    for (0..2) |r| {
        var part = try Partition.init(allocator, 0, 8192, 0);
        defer part.deinit();
        part.wireProjections();
        found[r] = try barsOf(&part, &entries, &bars[r], &states[r]);
    }
    try testing.expect(found[0].len >= 3);
    try testing.expectEqualSlices(u64, found[0], found[1]);
    try testing.expectEqualSlices(u64, states[0][0..found[0].len], states[1][0..found[1].len]);

    // The count is the entries' headers and payloads: a bar is the first
    // index at which it reaches the larger of 600 and the state before it.
    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();
    var since: u64 = 0;
    var next_bar: usize = 0;
    for (&entries) |*e| {
        const threshold = @max(600, part.store.encodedBytes());
        _ = try part.apply(e);
        since += entry_mod.HEADER_SIZE + e.payload.len;
        if (next_bar < found[0].len and found[0][next_bar] == e.header.index) {
            try testing.expect(since >= threshold);
            since = 0;
            next_bar += 1;
        } else try testing.expect(since < threshold);
    }

    // An index taken again (refused by the ring) isn't counted twice.
    var again = try Partition.init(allocator, 0, 8192, 0);
    defer again.deinit();
    again.wireProjections();
    _ = try again.apply(&entries[1]);
    const after_one = again.bar_bytes;
    try testing.expectError(error.IndexNotIncreasing, again.apply(&entries[1]));
    try testing.expectEqual(after_one, again.bar_bytes);
}

test "partition: a bar falls no sooner than the state a snapshot would encode" {
    const allocator = testing.allocator;
    Partition.test_bar_min_bytes = 100;
    defer Partition.test_bar_min_bytes = null;
    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();
    // State bigger than the minimum: the threshold is the state's size.
    const rec = try part.store.allocRecord("key");
    const big = @as([400]u8, @splat('v'));
    part.store.link(1, rec, try allocator.dupe(u8, &big));
    try testing.expect(part.store.encodedBytes() > 400);
    try testing.expectEqual(part.store.encodedBytes(), part.barThreshold());
    const payload = @as([80]u8, @splat('x'));
    var e = makeEntry(.stream_append, 1, 1, &payload);
    e.header.crc32c = e.computeCrc();
    _ = try part.apply(&e);
    // 120 bytes taken: past the minimum, short of the state.
    try testing.expectEqual(@as(u64, 0), part.last_bar);
    // Enough more to reach the state's size makes a bar.
    var i: u64 = 2;
    while (part.last_bar == 0) : (i += 1) {
        var more = makeEntry(.stream_append, i, 1, &payload);
        more.header.crc32c = more.computeCrc();
        _ = try part.apply(&more);
    }
    try testing.expect((i - 1) * 120 >= part.store.encodedBytes());
}

test "partition: an entry a repair skips, passed over, counts toward the bar as an applied one does" {
    const allocator = testing.allocator;
    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();
    var e = makeEntry(.stream_append, 1, 1, "passed over");
    e.header.crc32c = e.computeCrc();
    try part.passOverSkipped(&e);
    try testing.expectEqual(@as(u64, entry_mod.HEADER_SIZE + "passed over".len), part.bar_bytes);
}

test "partition: a KV index the router has applied already, taken again, isn't counted again" {
    const allocator = testing.allocator;
    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();
    var payload_buf: [128]u8 = undefined;
    const e = kvTestEntry(&payload_buf, .kv_put, .{ .key = @import("../namespace/table.zig").DEFAULT.bytes() ++ "key1", .value = "val1", .ttl_ms = null, .condition = .always, .cas_version = null }, 1, 1000);
    _ = try part.apply(&e);
    const once = part.bar_bytes;
    try testing.expectEqual(@as(u64, entry_mod.HEADER_SIZE + e.payload.len), once);
    // KV skips the ring, so nothing refuses it before the router does.
    _ = try part.apply(&e);
    try testing.expectEqual(once, part.bar_bytes);
}

test "partition: a KV index taken and then passed over by a repair counts once, as a replay that passes over it alone does" {
    const allocator = testing.allocator;
    var payload_buf: [128]u8 = undefined;
    const e = kvTestEntry(&payload_buf, .kv_put, .{ .key = @import("../namespace/table.zig").DEFAULT.bytes() ++ "key1", .value = "val1", .ttl_ms = null, .condition = .always, .cas_version = null }, 1, 1000);
    // Live: applied (and halted on), then passed over by the repair.
    var live = try Partition.init(allocator, 0, 8192, 0);
    defer live.deinit();
    live.wireProjections();
    _ = try live.apply(&e);
    try live.passOverSkipped(&e);
    // A replay that knows of the repair only passes over it.
    var replayed = try Partition.init(allocator, 0, 8192, 0);
    defer replayed.deinit();
    replayed.wireProjections();
    try replayed.passOverSkipped(&e);
    try testing.expectEqual(replayed.bar_bytes, live.bar_bytes);
    try testing.expectEqual(@as(u64, entry_mod.HEADER_SIZE + e.payload.len), live.bar_bytes);
}
