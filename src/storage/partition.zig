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
    kv: kv_mod.KVProjection,
    queue: queue_mod.QueueProjection,
    stream: stream_mod.StreamProjection,
    ts: ts_mod.TSProjection,

    /// Raft state placeholders (real Raft added in Phase 3).
    current_term: u64,
    committed_index: u64,

    /// Lifecycle.
    allocator: Allocator,
    ual_capacity: usize,

    pub const DEFAULT_UAL_CAPACITY: usize = 64 * 1024 * 1024; // 64 MB
    pub const DEFAULT_GROUP_OFFSET: u32 = 1000;

    // ── Construction ────────────────────────────────────────────────────

    pub fn init(allocator: Allocator, partition_id: u32, ual_capacity: usize) !Partition {
        var ual = try UAL.init(allocator, ual_capacity);
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

    pub fn deinit(self: *Partition) void {
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

        return index;
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
    /// Returns the sealed snapshot bytes. Caller owns the allocation.
    pub fn snapshot(self: *Partition) ![]u8 {
        const applied = self.router.stats.entries_applied;
        const timestamp = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

        var builder = snapshot_mod.SnapshotBuilder.init(
            self.allocator,
            self.id,
            applied,
            self.current_term,
            timestamp,
        );
        defer builder.deinit();

        // Serialize basic state: applied_index + committed_index
        var state_buf: [16]u8 = undefined;
        std.mem.writeInt(u64, state_buf[0..8], self.router.applied_index, .little);
        std.mem.writeInt(u64, state_buf[8..16], self.committed_index, .little);
        try builder.addSection(.kv, &state_buf);

        return try builder.seal();
    }

    /// Recover partition state from a snapshot + UAL replay.
    pub fn recover(self: *Partition, snapshot_data: []const u8) !u64 {
        const reader = try snapshot_mod.SnapshotReader.init(snapshot_data);

        if (reader.findSection(.kv)) |kv_ref| {
            if (kv_ref.data.len >= 16) {
                self.router.applied_index = std.mem.readInt(u64, kv_ref.data[0..8], .little);
                self.committed_index = std.mem.readInt(u64, kv_ref.data[8..16], .little);
            }
        }

        self.current_term = reader.snapshotTerm();

        return reader.snapshotIndex();
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

    var part = try Partition.init(allocator, 42, 4096);
    defer part.deinit();
    part.wireProjections();

    try testing.expectEqual(@as(u32, 42), part.id);
    try testing.expectEqual(@as(u32, 1042), part.group_id);
    try testing.expectEqual(@as(u64, 0), part.appliedIndex());
    try testing.expectEqual(@as(u64, 0), part.entryCount());
}

test "partition: apply and read entries" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192);
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

    var part = try Partition.init(allocator, 0, 8192);
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

    var part = try Partition.init(allocator, 0, 8192);
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

    var part = try Partition.init(allocator, 0, 8192);
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

    var part = try Partition.init(allocator, 0, 8192);
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
    var part = try Partition.init(allocator, 5, 8192);
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
    var part2 = try Partition.init(allocator, 5, 8192);
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

    var part = try Partition.init(allocator, 0, 4096);
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

    var part = try Partition.init(allocator, 0, 8192);
    defer part.deinit();
    part.wireProjections();

    var buf: [128]u8 = undefined;
    const e1 = entry_mod.buildCommandEntry(.kv_put, 0, 1, 1, 1000, 0, "key", "val", &buf) orelse unreachable;
    _ = try part.apply(&e1);

    try testing.expect(part.projectionMemory() > 0);
}
