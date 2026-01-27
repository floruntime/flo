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

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const UAL = ual_mod.UAL;

// ═══════════════════════════════════════════════════════════════════════════════
// ProjectionRouter (stub — real projections added in Phase 4)
// ═══════════════════════════════════════════════════════════════════════════════

/// Fans committed UAL entries to the appropriate projection.
/// Phase 2.6: stub implementation that tracks applied_index only.
/// Phase 4 will add real KV/Queue/TS projection pointers.
pub const ProjectionRouter = struct {
    applied_index: u64,
    /// Count of entries routed per type (for testing / metrics).
    routed_counts: [256]u64,

    pub fn init() ProjectionRouter {
        return .{
            .applied_index = 0,
            .routed_counts = .{0} ** 256,
        };
    }

    /// Apply a committed entry. Idempotent — skips if already applied.
    pub fn apply(self: *ProjectionRouter, e: *const Entry) void {
        if (e.header.index <= self.applied_index) return;

        // Track routing (Phase 4 will dispatch to real projections)
        self.routed_counts[e.header.entry_type] += 1;
        self.applied_index = e.header.index;
    }

    /// Serialize projection state for snapshot.
    /// Phase 2.6: returns a summary of routed counts.
    pub fn serializeState(self: *const ProjectionRouter, allocator: Allocator) ![]u8 {
        // Simple format: applied_index(u64) + 256 counts (u64 each) = 2056 bytes
        const size = @sizeOf(u64) + 256 * @sizeOf(u64);
        const buf = try allocator.alloc(u8, size);
        // Write applied_index
        @memcpy(buf[0..8], std.mem.asBytes(&self.applied_index));
        // Write routed counts
        for (0..256) |i| {
            const offset = 8 + i * 8;
            @memcpy(buf[offset..][0..8], std.mem.asBytes(&self.routed_counts[i]));
        }
        return buf;
    }

    /// Restore projection state from snapshot data.
    pub fn restoreState(self: *ProjectionRouter, data: []const u8) void {
        const min_size = @sizeOf(u64) + 256 * @sizeOf(u64);
        if (data.len < min_size) return;

        self.applied_index = std.mem.readInt(u64, data[0..8], .little);
        for (0..256) |i| {
            const offset = 8 + i * 8;
            self.routed_counts[i] = std.mem.readInt(u64, data[offset..][0..8], .little);
        }
    }

    /// Count of entries routed for a specific type.
    pub fn routedCount(self: *const ProjectionRouter, entry_type: u8) u64 {
        return self.routed_counts[entry_type];
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Partition
// ═══════════════════════════════════════════════════════════════════════════════

pub const Partition = struct {
    /// Partition identity.
    id: u32,
    group_id: u32,

    /// Storage.
    ual: UAL,
    projections: ProjectionRouter,

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

        return .{
            .id = partition_id,
            .group_id = partition_id + DEFAULT_GROUP_OFFSET,
            .ual = ual,
            .projections = ProjectionRouter.init(),
            .current_term = 0,
            .committed_index = 0,
            .allocator = allocator,
            .ual_capacity = ual_capacity,
        };
    }

    pub fn deinit(self: *Partition) void {
        self.ual.deinit();
    }

    // ── Write Path ──────────────────────────────────────────────────────

    /// Apply a committed entry: append to UAL and route to projections.
    /// Returns the UAL index assigned to the entry.
    pub fn apply(self: *Partition, e: *const Entry) !u64 {
        const index = try self.ual.append(e);

        // Route to projections (idempotent)
        self.projections.apply(e);

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

    // ── Snapshot ────────────────────────────────────────────────────────

    /// Create a snapshot of the current partition state.
    /// Returns the sealed snapshot bytes. Caller owns the allocation.
    pub fn snapshot(self: *Partition) ![]u8 {
        const applied = self.projections.applied_index;
        const timestamp = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;

        var builder = snapshot_mod.SnapshotBuilder.init(
            self.allocator,
            self.id,
            applied,
            self.current_term,
            timestamp,
        );
        defer builder.deinit();

        // Serialize projection state
        const proj_state = try self.projections.serializeState(self.allocator);
        defer self.allocator.free(proj_state);
        try builder.addSection(.kv, proj_state);

        return try builder.seal();
    }

    /// Recover partition state from a snapshot + UAL replay.
    pub fn recover(self: *Partition, snapshot_data: []const u8) !u64 {
        const reader = try snapshot_mod.SnapshotReader.init(snapshot_data);

        // Restore projection state
        if (reader.findSection(.kv)) |kv_ref| {
            self.projections.restoreState(kv_ref.data);
        }

        // Restore metadata
        self.current_term = reader.snapshotTerm();
        self.committed_index = reader.snapshotIndex();

        return reader.snapshotIndex();
    }

    // ── Queries ─────────────────────────────────────────────────────────

    /// The last applied UAL index (projections are up to date through this).
    pub fn appliedIndex(self: *const Partition) u64 {
        return self.projections.applied_index;
    }

    /// Number of entries currently in the hot ring.
    pub fn entryCount(self: *const Partition) u64 {
        return self.ual.entry_count;
    }

    /// UAL hot ring memory usage.
    pub fn ualUsed(self: *const Partition) u64 {
        return self.ual.used();
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

    try testing.expectEqual(@as(u32, 42), part.id);
    try testing.expectEqual(@as(u32, 1042), part.group_id);
    try testing.expectEqual(@as(u64, 0), part.appliedIndex());
    try testing.expectEqual(@as(u64, 0), part.entryCount());
}

test "partition: apply and read entries" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192);
    defer part.deinit();

    // Apply a KV put
    var e1 = makeEntry(.kv_put, 1, 1, "key1value1");
    e1.header.crc32c = e1.computeCrc();
    const idx1 = try part.apply(&e1);
    try testing.expectEqual(@as(u64, 1), idx1);

    // Apply a stream append
    var e2 = makeEntry(.stream_append, 2, 1, "stream-data");
    e2.header.crc32c = e2.computeCrc();
    const idx2 = try part.apply(&e2);
    try testing.expectEqual(@as(u64, 2), idx2);

    // Read back
    const read1 = part.read(1).?;
    try testing.expectEqual(@as(u8, @intFromEnum(EntryType.kv_put)), read1.header.entry_type);

    try testing.expectEqual(@as(u64, 2), part.appliedIndex());
    try testing.expectEqual(@as(u64, 2), part.committed_index);
}

test "partition: projection routing counts" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192);
    defer part.deinit();

    // Apply entries of different types
    var e1 = makeEntry(.kv_put, 1, 1, "data");
    e1.header.crc32c = e1.computeCrc();
    _ = try part.apply(&e1);

    var e2 = makeEntry(.kv_put, 2, 1, "data");
    e2.header.crc32c = e2.computeCrc();
    _ = try part.apply(&e2);

    var e3 = makeEntry(.queue_enqueue, 3, 1, "data");
    e3.header.crc32c = e3.computeCrc();
    _ = try part.apply(&e3);

    try testing.expectEqual(@as(u64, 2), part.projections.routedCount(@intFromEnum(EntryType.kv_put)));
    try testing.expectEqual(@as(u64, 1), part.projections.routedCount(@intFromEnum(EntryType.queue_enqueue)));
    try testing.expectEqual(@as(u64, 0), part.projections.routedCount(@intFromEnum(EntryType.ts_write)));
}

test "partition: idempotent apply" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 8192);
    defer part.deinit();

    var e1 = makeEntry(.kv_put, 1, 1, "data");
    e1.header.crc32c = e1.computeCrc();
    _ = try part.apply(&e1);

    // Apply same entry again — projection should skip it
    _ = try part.apply(&e1);

    // Should only count once in routed counts
    try testing.expectEqual(@as(u64, 1), part.projections.routedCount(@intFromEnum(EntryType.kv_put)));
}

test "partition: snapshot and recover" {
    const allocator = testing.allocator;

    // Create partition and apply some entries
    var part = try Partition.init(allocator, 5, 8192);
    defer part.deinit();

    var e1 = makeEntry(.kv_put, 1, 1, "key1val1");
    e1.header.crc32c = e1.computeCrc();
    _ = try part.apply(&e1);

    var e2 = makeEntry(.kv_put, 2, 1, "key2val2");
    e2.header.crc32c = e2.computeCrc();
    _ = try part.apply(&e2);

    var e3 = makeEntry(.queue_enqueue, 3, 2, "msg1");
    e3.header.crc32c = e3.computeCrc();
    _ = try part.apply(&e3);

    // Take snapshot
    const snap_data = try part.snapshot();
    defer allocator.free(snap_data);

    // Create a fresh partition and recover from snapshot
    var part2 = try Partition.init(allocator, 5, 8192);
    defer part2.deinit();

    const snap_index = try part2.recover(snap_data);
    try testing.expectEqual(@as(u64, 3), snap_index);

    // Projection state restored
    try testing.expectEqual(@as(u64, 3), part2.appliedIndex());
    try testing.expectEqual(@as(u64, 2), part2.projections.routedCount(@intFromEnum(EntryType.kv_put)));
    try testing.expectEqual(@as(u64, 1), part2.projections.routedCount(@intFromEnum(EntryType.queue_enqueue)));
}

test "partition: recover then replay" {
    const allocator = testing.allocator;

    // Original partition with 5 entries
    var orig = try Partition.init(allocator, 10, 16384);
    defer orig.deinit();

    var entries: [5]Entry = undefined;
    for (0..5) |i| {
        entries[i] = makeEntry(
            .kv_put,
            @intCast(i + 1),
            1,
            "data",
        );
        entries[i].header.crc32c = entries[i].computeCrc();
        _ = try orig.apply(&entries[i]);
    }

    // Snapshot at index 3 (simulate snapshotting mid-way)
    orig.projections.applied_index = 3; // pretend only 3 were snapshotted
    const snap_data = try orig.snapshot();
    defer allocator.free(snap_data);

    // Recover to a fresh partition
    var recovered = try Partition.init(allocator, 10, 16384);
    defer recovered.deinit();

    const snap_idx = try recovered.recover(snap_data);
    try testing.expectEqual(@as(u64, 3), snap_idx);

    // Replay entries 4 and 5
    var e4 = makeEntry(.kv_put, 4, 1, "data");
    e4.header.crc32c = e4.computeCrc();
    _ = try recovered.apply(&e4);

    var e5 = makeEntry(.queue_enqueue, 5, 1, "queue-msg");
    e5.header.crc32c = e5.computeCrc();
    _ = try recovered.apply(&e5);

    try testing.expectEqual(@as(u64, 5), recovered.appliedIndex());
    try testing.expectEqual(@as(u64, 5), recovered.committed_index);
}

test "partition: contains check" {
    const allocator = testing.allocator;

    var part = try Partition.init(allocator, 0, 4096);
    defer part.deinit();

    try testing.expect(!part.contains(1));

    var e1 = makeEntry(.kv_put, 1, 1, "data");
    e1.header.crc32c = e1.computeCrc();
    _ = try part.apply(&e1);

    try testing.expect(part.contains(1));
    try testing.expect(!part.contains(2));
}

test "projection router: serialize and restore round-trip" {
    const allocator = testing.allocator;

    var router = ProjectionRouter.init();

    // Simulate some applies
    var e1 = makeEntry(.kv_put, 1, 1, "data");
    e1.header.crc32c = e1.computeCrc();
    router.apply(&e1);

    var e2 = makeEntry(.ts_write, 2, 1, "data");
    e2.header.crc32c = e2.computeCrc();
    router.apply(&e2);

    // Serialize
    const state = try router.serializeState(allocator);
    defer allocator.free(state);

    // Restore into fresh router
    var router2 = ProjectionRouter.init();
    router2.restoreState(state);

    try testing.expectEqual(router.applied_index, router2.applied_index);
    try testing.expectEqual(
        router.routedCount(@intFromEnum(EntryType.kv_put)),
        router2.routedCount(@intFromEnum(EntryType.kv_put)),
    );
    try testing.expectEqual(
        router.routedCount(@intFromEnum(EntryType.ts_write)),
        router2.routedCount(@intFromEnum(EntryType.ts_write)),
    );
}
