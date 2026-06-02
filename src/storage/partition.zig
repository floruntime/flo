//! Partition — owns a UAL, projections, and snapshot state
//!
//! Each Partition represents one Raft group's data. The Shard owns
//! multiple Partitions (those where `partition_id % shard_count == shard_id`).
//!
//! Data flow:
//!   propose(entry) → UAL.append → ProjectionRouter.apply → advance applied_index
//!
//! Snapshot flow:
//!   snapshot() → serialize projections → atomic .fsnap write → MANIFEST update
//!
//! Recovery flow:
//!   recover() → load .fsnap → restore projections → replay UAL from snapshot+1

const std = @import("std");
const Allocator = std.mem.Allocator;
const entry_mod = @import("ual/entry.zig");
const ual_mod = @import("ual/ual.zig");
const snapshot_mod = @import("snapshot.zig");
const router_mod = @import("../projection/router.zig");
const kv_mod = @import("../projection/kv.zig");
const queue_mod = @import("../projection/queue.zig");
const stream_mod = @import("../projection/stream.zig");
const ts_mod = @import("../projection/ts.zig");
const cold_mod = @import("cold/tier_manager.zig");
const reader_mod = @import("ual/reader.zig");
const memory_mod = @import("memory.zig");
const log = @import("stdx").log;

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const UAL = ual_mod.UAL;
const ProjectionRouter = router_mod.ProjectionRouter;
const ColdTierManager = cold_mod.ColdTierManager;
const SegmentReader = reader_mod.SegmentReader;
const MemoryController = memory_mod.MemoryController;

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
    kv: kv_mod.KVProjection,
    queue: queue_mod.QueueProjection,
    stream: stream_mod.StreamProjection,
    ts: ts_mod.TSProjection,

    /// Raft state placeholders (real Raft added in Phase 3).
    current_term: u64,
    committed_index: u64,

    /// Warm store — payload copies for entries evicted from the hot ring buffer.
    /// Populated on every apply() so reads can fall back to warm when UAL evicts.
    warm_store: std.AutoHashMapUnmanaged(u64, []const u8),

    /// Warm store memory tracking (bounded by warm_budget).
    warm_bytes_used: usize,
    warm_budget: usize,

    /// Cold tier manager (optional — set via wireColdTier after init).
    /// Provides on-demand download of cold segments for historical reads.
    cold_tier: ?*ColdTierManager,

    /// Memory controller reference (optional — set via wireMemoryController).
    memory_controller: ?*MemoryController,

    /// Lifecycle.
    allocator: Allocator,
    ual_capacity: usize,

    pub const DEFAULT_UAL_CAPACITY: usize = 64 * 1024 * 1024; // 64 MB
    pub const DEFAULT_GROUP_OFFSET: u32 = 1000;
    pub const DEFAULT_WARM_BUDGET: usize = 32 * 1024 * 1024; // 32 MB

    // ── Construction ────────────────────────────────────────────────────

    pub fn init(allocator: Allocator, partition_id: u32, ual_capacity: usize, max_hot_entries: u64) !Partition {
        var ual = try UAL.init(allocator, ual_capacity, max_hot_entries);
        errdefer ual.deinit();

        const kv = kv_mod.KVProjection.init(allocator, 0); // 0 = no memory limit
        const queue = queue_mod.QueueProjection.init(allocator, .{});
        const stream = stream_mod.StreamProjection.init(allocator);
        const ts = ts_mod.TSProjection.init(allocator, .{});

        var part = Partition{
            .id = partition_id,
            .group_id = partition_id + DEFAULT_GROUP_OFFSET,
            .ual = ual,
            .router = ProjectionRouter.init(),
            .kv = kv,
            .queue = queue,
            .stream = stream,
            .ts = ts,
            .current_term = 0,
            .committed_index = 0,
            .warm_store = .{},
            .warm_bytes_used = 0,
            .warm_budget = DEFAULT_WARM_BUDGET,
            .cold_tier = null,
            .memory_controller = null,
            .allocator = allocator,
            .ual_capacity = ual_capacity,
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
        self.router.registerKV(self.kv.projectionHandle());
        self.router.registerQueue(self.queue.projectionHandle());
        self.router.registerTS(self.ts.projectionHandle());
    }

    /// Wire a ColdTierManager for on-demand cold reads and manifest loading.
    /// Called after init() when cold storage is configured.
    pub fn wireColdTier(self: *Partition, manager: *ColdTierManager) void {
        self.cold_tier = manager;
    }

    /// Wire a MemoryController for warm_store budget enforcement.
    /// Called after init() when memory budgets are configured.
    pub fn wireMemoryController(self: *Partition, controller: *MemoryController) void {
        self.memory_controller = controller;
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

        self.ts.deinit();
        self.stream.deinit();
        self.queue.deinit();
        self.kv.deinit();
        self.ual.deinit();
    }

    // ── Write Path ──────────────────────────────────────────────────────

    /// Apply a committed entry: append to UAL and route to projections.
    /// Returns the UAL index assigned to the entry.
    pub fn apply(self: *Partition, e: *const Entry) !u64 {
        const index = try self.ual.append(e);

        // Route to projections via the real router (idempotent)
        _ = self.router.apply(e);

        // Track committed index
        if (e.header.index > self.committed_index) {
            self.committed_index = e.header.index;
        }

        log.debug("Partition: applied entry, partition_id={d}, index={d}, type={d}", .{ self.id, e.header.index, e.header.entry_type });

        // Save payload to warm store (survives UAL hot ring eviction).
        // Uses the entry's own index as key.
        if (e.payload.len > 0) {
            const copy = self.allocator.dupe(u8, e.payload) catch return index;

            // Free old copy if this index was already in warm store (idempotent apply)
            if (self.warm_store.fetchRemove(e.header.index)) |old| {
                self.warm_bytes_used -= old.value.len;
                self.allocator.free(@constCast(old.value));
            }

            self.warm_store.put(self.allocator, e.header.index, copy) catch {
                self.allocator.free(copy);
                return index;
            };
            self.warm_bytes_used += copy.len;

            // Evict oldest warm entries if over budget
            self.evictWarmIfNeeded();
        }

        return index;
    }

    /// Evict oldest entries from the warm store when over budget.
    /// Removes entries with the lowest UAL index first.
    fn evictWarmIfNeeded(self: *Partition) void {
        if (self.warm_budget == 0) return; // 0 = unlimited
        while (self.warm_bytes_used > self.warm_budget and self.warm_store.count() > 0) {
            // Find the entry with the lowest index
            var min_index: u64 = std.math.maxInt(u64);
            var wit = self.warm_store.iterator();
            while (wit.next()) |kv| {
                if (kv.key_ptr.* < min_index) {
                    min_index = kv.key_ptr.*;
                }
            }
            if (min_index == std.math.maxInt(u64)) break;

            // Remove it
            if (self.warm_store.fetchRemove(min_index)) |removed| {
                self.warm_bytes_used -= removed.value.len;
                self.allocator.free(@constCast(removed.value));
            }
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

    /// Check if a UAL index is available in cold storage.
    pub fn isInCold(self: *const Partition, index: u64) bool {
        const ct = self.cold_tier orelse return false;
        return ct.isInCold(index);
    }

    /// Read an entry from cold storage on demand.
    /// Downloads the cold segment, parses it, and returns the entry.
    /// Caller gets a zero-copy view into the downloaded segment data;
    /// the segment data itself is returned via `segment_out` so the
    /// caller can manage its lifetime.
    ///
    /// Returns null if:
    /// - No cold tier is configured
    /// - The index is not in the cold manifest
    /// - The segment download or parse fails
    pub fn readFromCold(self: *Partition, index: u64) ?Entry {
        const ct = self.cold_tier orelse return null;

        // Download the cold segment containing this index
        const segment_data = ct.downloadSegment(index) catch return null;
        defer self.allocator.free(segment_data);

        // Parse the segment and find the entry
        const reader = SegmentReader.init(segment_data) catch return null;
        return reader.findByIndex(index);
    }

    /// Tiered read — tries hot ring, then warm store, then cold storage.
    /// For cold reads, downloads the segment, finds the entry, and copies
    /// the payload into the provided buffer so it outlives the segment.
    /// Returns the entry with payload pointing into `payload_buf` (or
    /// null if not found anywhere).
    pub fn readTiered(self: *Partition, index: u64, payload_buf: []u8) ?Entry {
        // 1. Hot ring. Use readCopy, not the zero-copy read: read() returns null
        //    for a payload that wraps the ring boundary, which would make a live
        //    hot entry look "not found" and fall through to warm/cold (or vanish
        //    if not yet flushed). readCopy reconstructs wrapped payloads and also
        //    honours this function's contract that the payload lives in payload_buf.
        if (self.ual.readCopy(index, payload_buf)) |entry| return entry;

        // 2. Warm store (payload copy, still local)
        if (self.warm_store.get(index)) |warm_payload| {
            // We have the payload but no full entry header from warm.
            // Construct a minimal entry. The caller typically only needs
            // the payload for stream reads and TS queries.
            _ = warm_payload;
            // For warm reads, the caller uses readPayloadWarm() directly.
            // readTiered returns a full Entry only from hot or cold.
        }

        // 3. Cold storage (on-demand download)
        const ct = self.cold_tier orelse return null;
        const segment_data = ct.downloadSegment(index) catch return null;
        defer self.allocator.free(segment_data);

        const reader = SegmentReader.init(segment_data) catch return null;
        const entry = reader.findByIndex(index) orelse return null;

        // Copy payload into caller's buffer so it outlives the segment
        if (entry.payload.len > 0 and entry.payload.len <= payload_buf.len) {
            @memcpy(payload_buf[0..entry.payload.len], entry.payload);
            var result = entry;
            result.payload = payload_buf[0..entry.payload.len];
            return result;
        }

        return entry;
    }

    // ── Projection accessors ────────────────────────────────────────────

    /// Get a value from the KV projection.
    pub fn kvGet(self: *Partition, key: []const u8) ?[]const u8 {
        const entry = self.kv.get(key) orelse return null;
        return entry.value;
    }

    /// Get the stream high water mark.
    pub fn streamHWM(self: *const Partition) u64 {
        return self.stream.highWaterMark();
    }

    /// Get the queue ready count.
    pub fn queueReady(self: *const Partition) usize {
        return self.queue.readyCount();
    }

    // ── Snapshot ────────────────────────────────────────────────────────

    /// Create a snapshot of the current partition state.
    /// Serializes all four projections into separate sections.
    /// Returns the sealed snapshot bytes. Caller owns the allocation.
    pub fn snapshot(self: *Partition) ![]u8 {
        const applied = self.router.applied_index;
        const timestamp = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
        log.debug("Partition: taking snapshot, partition_id={d}, applied_index={d}", .{ self.id, applied });

        var builder = snapshot_mod.SnapshotBuilder.init(
            self.allocator,
            self.id,
            applied,
            self.current_term,
            timestamp,
        );
        defer builder.deinit();

        // ── KV Projection ──────────────────────────────────────────────
        const kv_data = try self.kv.serialize(self.allocator);
        defer self.allocator.free(kv_data);
        try builder.addSection(.kv, kv_data);

        // ── Queue Projection ───────────────────────────────────────────
        const queue_data = try self.queue.serialize(self.allocator);
        defer self.allocator.free(queue_data);
        try builder.addSection(.queue, queue_data);

        // ── Stream Projection ──────────────────────────────────────────
        const stream_data = try self.stream.serialize(self.allocator);
        defer self.allocator.free(stream_data);
        try builder.addSection(.stream, stream_data);

        // ── TS Projection ──────────────────────────────────────────────
        const ts_data = try self.ts.serialize(self.allocator);
        defer self.allocator.free(ts_data);
        try builder.addSection(.ts, ts_data);

        return try builder.seal();
    }

    /// Recover partition state from a snapshot.
    /// Deserializes all projection sections and restores metadata.
    /// Returns the snapshot's applied UAL index (replay UAL from index+1).
    pub fn recover(self: *Partition, snapshot_data: []const u8) !u64 {
        const reader = try snapshot_mod.SnapshotReader.init(snapshot_data);

        // Restore metadata from snapshot header
        const snap_index = reader.snapshotIndex();
        log.debug("Partition: recovering from snapshot, partition_id={d}, snap_index={d}, data_len={d}", .{ self.id, snap_index, snapshot_data.len });
        self.current_term = reader.snapshotTerm();
        self.router.applied_index = snap_index;
        self.committed_index = snap_index; // at snapshot time, committed == applied

        // ── KV Projection ──────────────────────────────────────────────
        if (reader.findSection(.kv)) |kv_ref| {
            try self.kv.deserialize(kv_ref.data);
        }

        // ── Queue Projection ───────────────────────────────────────────
        if (reader.findSection(.queue)) |queue_ref| {
            try self.queue.deserialize(queue_ref.data);
        }

        // ── Stream Projection ──────────────────────────────────────────
        if (reader.findSection(.stream)) |stream_ref| {
            try self.stream.deserialize(stream_ref.data);
        }

        // ── TS Projection ──────────────────────────────────────────────
        if (reader.findSection(.ts)) |ts_ref| {
            try self.ts.deserialize(ts_ref.data);
        }

        return snap_index;
    }

    /// Load the cold manifest from a directory path (step 2d in recovery).
    /// This is metadata-only — no cold data is fetched.
    /// Called by Shard after snapshot + warm replay, before accepting traffic.
    pub fn loadColdManifest(self: *Partition, cold_dir: []const u8) !void {
        const ct = self.cold_tier orelse return;
        try ct.loadManifest(cold_dir);
    }

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

    /// Number of segments tracked in cold storage.
    pub fn coldSegmentCount(self: *const Partition) usize {
        const ct = self.cold_tier orelse return 0;
        return ct.segmentCount();
    }

    /// Total memory across all projections.
    pub fn projectionMemory(self: *Partition) usize {
        return self.router.totalMemoryUsage();
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

test "partition: apply and read entries" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    // Apply a KV put (use CommandPayload format so projection can parse)
    var payload_buf: [128]u8 = undefined;
    const e1 = entry_mod.buildCommandEntry(.kv_put, 0, 1, 1, 1000, 0, "key1", "val1", &payload_buf) orelse unreachable;
    const idx1 = try part.apply(&e1);
    try testing.expectEqual(@as(u64, 1), idx1);

    // Apply a stream append
    var e2 = makeEntry(.stream_append, 2, 1, "stream-data");
    e2.header.crc32c = e2.computeCrc();
    const idx2 = try part.apply(&e2);
    try testing.expectEqual(@as(u64, 2), idx2);

    // Read back from UAL
    const read1 = part.read(1).?;
    try testing.expectEqual(@as(u8, @intFromEnum(EntryType.kv_put)), read1.header.entry_type);

    try testing.expectEqual(@as(u64, 2), part.appliedIndex());
    try testing.expectEqual(@as(u64, 2), part.committed_index);
}

test "partition: KV projection wired through router" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    // Apply a KV put with proper CommandPayload
    var payload_buf: [128]u8 = undefined;
    const e1 = entry_mod.buildCommandEntry(.kv_put, 0, 1, 1, 1000, 0, "mykey", "myvalue", &payload_buf) orelse unreachable;
    _ = try part.apply(&e1);

    // Verify the KV projection received the entry
    const val = part.kvGet("mykey");
    try testing.expect(val != null);
    try testing.expectEqualStrings("myvalue", val.?);
}

test "partition: KV put then delete" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    // Put
    var buf1: [128]u8 = undefined;
    const e1 = entry_mod.buildCommandEntry(.kv_put, 0, 1, 1, 1000, 0, "k", "v", &buf1) orelse unreachable;
    _ = try part.apply(&e1);
    try testing.expect(part.kvGet("k") != null);

    // Delete
    var buf2: [128]u8 = undefined;
    const e2 = entry_mod.buildCommandEntry(.kv_delete, 0, 1, 2, 2000, 0, "k", "", &buf2) orelse unreachable;
    _ = try part.apply(&e2);
    try testing.expect(part.kvGet("k") == null);
}

test "partition: queue projection wired through router" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    // Enqueue via UAL entry
    var buf1: [128]u8 = undefined;
    const e1 = entry_mod.buildCommandEntry(.queue_enqueue, 0, 1, 1, 1000, 0, "q1", "msg", &buf1) orelse unreachable;
    _ = try part.apply(&e1);

    try testing.expectEqual(@as(u64, 1), part.queue.stats.enqueued);
}

test "partition: idempotent apply" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    var buf: [128]u8 = undefined;
    const e1 = entry_mod.buildCommandEntry(.kv_put, 0, 1, 1, 1000, 0, "k", "v", &buf) orelse unreachable;
    _ = try part.apply(&e1);

    // Apply same entry again — projection router should skip it
    _ = try part.apply(&e1);

    // Router stats should show 1 applied, 1 skipped
    try testing.expectEqual(@as(u64, 1), part.router.stats.entries_applied);
}

test "partition: snapshot and recover" {
    const allocator = testing.allocator;

    // Create partition and apply some entries
    var part = try Partition.init(allocator, 5, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    var buf1: [128]u8 = undefined;
    const e1 = entry_mod.buildCommandEntry(.kv_put, 0, 1, 1, 1000, 0, "k1", "v1", &buf1) orelse unreachable;
    _ = try part.apply(&e1);

    var buf2: [128]u8 = undefined;
    const e2 = entry_mod.buildCommandEntry(.kv_put, 0, 1, 2, 2000, 0, "k2", "v2", &buf2) orelse unreachable;
    _ = try part.apply(&e2);

    // Take snapshot
    const snap_data = try part.snapshot();
    defer allocator.free(snap_data);

    // Create a fresh partition and recover from snapshot
    var part2 = try Partition.init(allocator, 5, 8192, 0);
    defer part2.deinit();
    part2.wireProjections();

    const snap_index = try part2.recover(snap_data);
    try testing.expect(snap_index > 0);

    // Applied index and committed_index restored
    try testing.expectEqual(part.router.applied_index, part2.router.applied_index);
    try testing.expectEqual(part.committed_index, part2.committed_index);
}

test "partition: contains check" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 4096, 0);
    defer part.deinit();
    part.wireProjections();

    try testing.expect(!part.contains(1));

    var e1 = makeEntry(.kv_put, 1, 1, "data");
    e1.header.crc32c = e1.computeCrc();
    _ = try part.apply(&e1);

    try testing.expect(part.contains(1));
    try testing.expect(!part.contains(2));
}

test "partition: projection memory tracking" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    var buf: [128]u8 = undefined;
    const e1 = entry_mod.buildCommandEntry(.kv_put, 0, 1, 1, 1000, 0, "key", "val", &buf) orelse unreachable;
    _ = try part.apply(&e1);

    try testing.expect(part.projectionMemory() > 0);
}

test "partition: warm store byte tracking" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192, 0);
    defer part.deinit();
    part.wireProjections();

    try testing.expectEqual(@as(usize, 0), part.warmUsed());
    try testing.expectEqual(@as(usize, 0), part.warmCount());

    var e1 = makeEntry(.kv_put, 1, 1, "hello-world");
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

    // Apply entries with payloads that accumulate beyond 100 bytes
    var i: u64 = 1;
    while (i <= 20) : (i += 1) {
        var e = makeEntry(.kv_put, i, 1, "0123456789"); // 10 bytes each
        e.header.crc32c = e.computeCrc();
        _ = try part.apply(&e);
    }

    // After eviction, warm usage should be at or under budget
    try testing.expect(part.warmUsed() <= 100);
    // Oldest entries should have been evicted
    try testing.expect(part.warmCount() <= 10);
}

test "partition: cold tier not wired returns defaults" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 4096, 0);
    defer part.deinit();
    part.wireProjections();

    // No cold tier wired — should return safe defaults
    try testing.expect(!part.isInCold(42));
    try testing.expectEqual(@as(usize, 0), part.coldSegmentCount());
    try testing.expect(part.readFromCold(42) == null);
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
        var e = makeEntry(.kv_put, i, 1, "0123456789ABCDEF"); // 16 bytes each
        e.header.crc32c = e.computeCrc();
        _ = try part.apply(&e);
    }

    // All 50 entries should be in warm store
    try testing.expectEqual(@as(usize, 50), part.warmCount());
    try testing.expectEqual(@as(usize, 50 * 16), part.warmUsed());
}
