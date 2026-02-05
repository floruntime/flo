//! Projection Router — fans committed UAL entries to the correct projection.
//!
//! The router sits between the Partition (which owns the UAL and Raft log)
//! and the individual projection engines. When Raft commits an entry, the
//! Partition calls `router.apply(entry)` which dispatches by EntryType.
//!
//! Routing table:
//!   kv_put, kv_delete, kv_batch     → KVProjection
//!   cg_commit, cg_create, cg_delete → KVProjection (consumer group state as KV)
//!   queue_enqueue, queue_ack, etc.  → QueueProjection
//!   ts_write, ts_write_batch        → TSProjection
//!   stream_append, stream_trim      → None (UAL direct reads, zero-copy)
//!   raft_config, raft_noop          → None (consensus layer)
//!   raft_snapshot                    → Snapshot installation
//!   checkpoint                      → None (processing runtime)
//!
//! The router maintains `applied_index` for idempotency — entries at or below
//! this index are silently skipped.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;

// ═══════════════════════════════════════════════════════════════════════════════
// Projection Interface
// ═══════════════════════════════════════════════════════════════════════════════

/// Common interface for all projection engines.
/// Each projection must implement these operations.
pub const ProjectionVTable = struct {
    /// Apply a committed UAL entry to this projection.
    applyFn: *const fn (ctx: *anyopaque, entry: *const Entry) ApplyError!void,

    /// Return current memory usage in bytes.
    memoryUsageFn: *const fn (ctx: *anyopaque) usize,
};

pub const ApplyError = error{
    InvalidPayload,
    OutOfMemory,
    CapacityExceeded,
};

/// Type-erased projection handle.
pub const ProjectionHandle = struct {
    ctx: *anyopaque,
    vtable: ProjectionVTable,

    pub fn apply(self: ProjectionHandle, entry: *const Entry) ApplyError!void {
        return self.vtable.applyFn(self.ctx, entry);
    }

    pub fn memoryUsage(self: ProjectionHandle) usize {
        return self.vtable.memoryUsageFn(self.ctx);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Apply Result — what happened when we applied an entry
// ═══════════════════════════════════════════════════════════════════════════════

pub const ApplyResult = enum(u8) {
    /// Entry was routed to a projection and applied.
    applied,
    /// Entry was skipped (already applied, index <= applied_index).
    skipped_already_applied,
    /// Entry type does not route to any projection (stream, raft control, etc.).
    skipped_no_projection,
    /// Snapshot installation triggered.
    snapshot_triggered,
    /// Error applying to projection.
    apply_error,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Projection Router
// ═══════════════════════════════════════════════════════════════════════════════

pub const ProjectionRouter = struct {
    /// Last applied UAL index — for idempotency.
    applied_index: u64,

    /// Registered projection engines (optional — set to null if not present).
    kv: ?ProjectionHandle,
    queue: ?ProjectionHandle,
    ts: ?ProjectionHandle,
    // Stream has no projection — reads go directly to UAL.

    /// Stats for observability.
    stats: Stats,

    pub const Stats = struct {
        entries_applied: u64 = 0,
        entries_skipped: u64 = 0,
        entries_no_projection: u64 = 0,
        entries_error: u64 = 0,
        snapshots_triggered: u64 = 0,
        kv_entries: u64 = 0,
        queue_entries: u64 = 0,
        ts_entries: u64 = 0,
    };

    pub fn init() ProjectionRouter {
        return .{
            .applied_index = 0,
            .kv = null,
            .queue = null,
            .ts = null,
            .stats = .{},
        };
    }

    /// Register the KV projection.
    pub fn registerKV(self: *ProjectionRouter, handle: ProjectionHandle) void {
        self.kv = handle;
    }

    /// Register the Queue projection.
    pub fn registerQueue(self: *ProjectionRouter, handle: ProjectionHandle) void {
        self.queue = handle;
    }

    /// Register the TS projection.
    pub fn registerTS(self: *ProjectionRouter, handle: ProjectionHandle) void {
        self.ts = handle;
    }

    /// Apply a single committed entry. Routes by EntryType.
    /// Returns what happened.
    pub fn apply(self: *ProjectionRouter, entry: *const Entry) ApplyResult {
        // Idempotency: skip already-applied entries
        if (entry.header.index <= self.applied_index) {
            self.stats.entries_skipped += 1;
            return .skipped_already_applied;
        }

        const entry_type: EntryType = @enumFromInt(entry.header.entry_type);
        const target = routeTarget(entry_type);

        const result = switch (target) {
            .kv => self.applyTo(self.kv, entry, &self.stats.kv_entries),
            .queue => self.applyTo(self.queue, entry, &self.stats.queue_entries),
            .ts => self.applyTo(self.ts, entry, &self.stats.ts_entries),
            .none => blk: {
                self.stats.entries_no_projection += 1;
                break :blk .skipped_no_projection;
            },
            .snapshot => blk: {
                self.stats.snapshots_triggered += 1;
                break :blk .snapshot_triggered;
            },
        };

        // Advance applied_index regardless of routing outcome
        self.applied_index = entry.header.index;
        return result;
    }

    /// Apply a batch of committed entries (e.g., after Raft commit advances).
    pub fn applyBatch(self: *ProjectionRouter, entries: []const Entry) BatchResult {
        var result = BatchResult{};
        for (entries) |*entry| {
            const r = self.apply(entry);
            switch (r) {
                .applied => result.applied += 1,
                .skipped_already_applied => result.skipped += 1,
                .skipped_no_projection => result.no_projection += 1,
                .snapshot_triggered => result.snapshots += 1,
                .apply_error => result.errors += 1,
            }
        }
        return result;
    }

    pub const BatchResult = struct {
        applied: u32 = 0,
        skipped: u32 = 0,
        no_projection: u32 = 0,
        snapshots: u32 = 0,
        errors: u32 = 0,

        pub fn total(self: BatchResult) u32 {
            return self.applied + self.skipped + self.no_projection +
                self.snapshots + self.errors;
        }
    };

    /// Total memory usage across all registered projections.
    pub fn totalMemoryUsage(self: *const ProjectionRouter) usize {
        var total: usize = 0;
        if (self.kv) |h| total += h.memoryUsage();
        if (self.queue) |h| total += h.memoryUsage();
        if (self.ts) |h| total += h.memoryUsage();
        return total;
    }

    // ─── Internal ──────────────────────────────────────────────────────────

    fn applyTo(
        self: *ProjectionRouter,
        handle: ?ProjectionHandle,
        entry: *const Entry,
        counter: *u64,
    ) ApplyResult {
        if (handle) |h| {
            h.apply(entry) catch {
                self.stats.entries_error += 1;
                return .apply_error;
            };
            counter.* += 1;
            self.stats.entries_applied += 1;
            return .applied;
        }
        // Projection not registered yet — count as no_projection
        self.stats.entries_no_projection += 1;
        return .skipped_no_projection;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Routing Target
// ═══════════════════════════════════════════════════════════════════════════════

const RouteTarget = enum(u8) {
    kv,
    queue,
    ts,
    none,
    snapshot,
};

/// Determine which projection an entry type routes to.
pub fn routeTarget(entry_type: EntryType) RouteTarget {
    return switch (entry_type) {
        // KV + consumer group state
        .kv_put, .kv_delete, .kv_batch => .kv,
        .cg_commit, .cg_create, .cg_delete => .kv,

        // Queue
        .queue_enqueue, .queue_ack, .queue_nack, .queue_lease => .queue,

        // TimeSeries
        .ts_write, .ts_write_batch => .ts,

        // Stream — no projection (UAL direct reads)
        .stream_append, .stream_trim => .none,

        // Raft control — consensus layer
        .raft_config, .raft_noop => .none,

        // Snapshot installation
        .raft_snapshot => .snapshot,

        // Checkpoint — processing runtime
        .checkpoint => .none,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

/// Test projection that records applied entries for verification.
const TestProjection = struct {
    apply_count: u32 = 0,
    last_index: u64 = 0,
    memory: usize = 1024,

    fn applyFn(ctx: *anyopaque, entry: *const Entry) ApplyError!void {
        const self: *TestProjection = @ptrCast(@alignCast(ctx));
        self.apply_count += 1;
        self.last_index = entry.header.index;
    }

    fn memoryUsageFn(ctx: *anyopaque) usize {
        const self: *TestProjection = @ptrCast(@alignCast(ctx));
        return self.memory;
    }

    fn handle(self: *TestProjection) ProjectionHandle {
        return .{
            .ctx = @ptrCast(self),
            .vtable = .{
                .applyFn = applyFn,
                .memoryUsageFn = memoryUsageFn,
            },
        };
    }
};

/// Test projection that always returns an error.
const ErrorProjection = struct {
    fn applyFn(_: *anyopaque, _: *const Entry) ApplyError!void {
        return error.InvalidPayload;
    }

    fn memoryUsageFn(_: *anyopaque) usize {
        return 0;
    }

    fn handle(self: *ErrorProjection) ProjectionHandle {
        return .{
            .ctx = @ptrCast(self),
            .vtable = .{
                .applyFn = applyFn,
                .memoryUsageFn = memoryUsageFn,
            },
        };
    }
};

fn makeEntry(entry_type: EntryType, index: u64) Entry {
    return entry_mod.buildEntry(entry_type, entry_mod.Flags.NONE, 1, index, 0, "test");
}

test "router: routing table correctness" {
    // KV types
    try testing.expectEqual(RouteTarget.kv, routeTarget(.kv_put));
    try testing.expectEqual(RouteTarget.kv, routeTarget(.kv_delete));
    try testing.expectEqual(RouteTarget.kv, routeTarget(.kv_batch));

    // Consumer group → KV
    try testing.expectEqual(RouteTarget.kv, routeTarget(.cg_commit));
    try testing.expectEqual(RouteTarget.kv, routeTarget(.cg_create));
    try testing.expectEqual(RouteTarget.kv, routeTarget(.cg_delete));

    // Queue
    try testing.expectEqual(RouteTarget.queue, routeTarget(.queue_enqueue));
    try testing.expectEqual(RouteTarget.queue, routeTarget(.queue_ack));
    try testing.expectEqual(RouteTarget.queue, routeTarget(.queue_nack));
    try testing.expectEqual(RouteTarget.queue, routeTarget(.queue_lease));

    // TimeSeries
    try testing.expectEqual(RouteTarget.ts, routeTarget(.ts_write));
    try testing.expectEqual(RouteTarget.ts, routeTarget(.ts_write_batch));

    // Stream — no projection
    try testing.expectEqual(RouteTarget.none, routeTarget(.stream_append));
    try testing.expectEqual(RouteTarget.none, routeTarget(.stream_trim));

    // Raft control — no projection
    try testing.expectEqual(RouteTarget.none, routeTarget(.raft_config));
    try testing.expectEqual(RouteTarget.none, routeTarget(.raft_noop));

    // Snapshot
    try testing.expectEqual(RouteTarget.snapshot, routeTarget(.raft_snapshot));

    // Checkpoint
    try testing.expectEqual(RouteTarget.none, routeTarget(.checkpoint));
}

test "router: kv entry routed to kv projection" {
    var kv_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerKV(kv_proj.handle());

    const entry = makeEntry(.kv_put, 1);
    const result = router.apply(&entry);

    try testing.expectEqual(ApplyResult.applied, result);
    try testing.expectEqual(@as(u32, 1), kv_proj.apply_count);
    try testing.expectEqual(@as(u64, 1), kv_proj.last_index);
    try testing.expectEqual(@as(u64, 1), router.applied_index);
    try testing.expectEqual(@as(u64, 1), router.stats.kv_entries);
}

test "router: queue entry routed to queue projection" {
    var queue_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerQueue(queue_proj.handle());

    const entry = makeEntry(.queue_enqueue, 1);
    const result = router.apply(&entry);

    try testing.expectEqual(ApplyResult.applied, result);
    try testing.expectEqual(@as(u32, 1), queue_proj.apply_count);
    try testing.expectEqual(@as(u64, 1), router.stats.queue_entries);
}

test "router: ts entry routed to ts projection" {
    var ts_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerTS(ts_proj.handle());

    const entry = makeEntry(.ts_write, 1);
    const result = router.apply(&entry);

    try testing.expectEqual(ApplyResult.applied, result);
    try testing.expectEqual(@as(u32, 1), ts_proj.apply_count);
    try testing.expectEqual(@as(u64, 1), router.stats.ts_entries);
}

test "router: consumer group entry routed to kv projection" {
    var kv_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerKV(kv_proj.handle());

    const entry = makeEntry(.cg_commit, 1);
    const result = router.apply(&entry);

    try testing.expectEqual(ApplyResult.applied, result);
    try testing.expectEqual(@as(u32, 1), kv_proj.apply_count);
    try testing.expectEqual(@as(u64, 1), router.stats.kv_entries);
}

test "router: stream entry has no projection" {
    var router = ProjectionRouter.init();
    const entry = makeEntry(.stream_append, 1);
    const result = router.apply(&entry);

    try testing.expectEqual(ApplyResult.skipped_no_projection, result);
    try testing.expectEqual(@as(u64, 1), router.stats.entries_no_projection);
}

test "router: raft noop has no projection" {
    var router = ProjectionRouter.init();
    const entry = makeEntry(.raft_noop, 1);
    const result = router.apply(&entry);

    try testing.expectEqual(ApplyResult.skipped_no_projection, result);
}

test "router: raft snapshot triggers installation" {
    var router = ProjectionRouter.init();
    const entry = makeEntry(.raft_snapshot, 1);
    const result = router.apply(&entry);

    try testing.expectEqual(ApplyResult.snapshot_triggered, result);
    try testing.expectEqual(@as(u64, 1), router.stats.snapshots_triggered);
}

test "router: idempotency — duplicate entries skipped" {
    var kv_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerKV(kv_proj.handle());

    const entry = makeEntry(.kv_put, 1);

    // Apply once
    _ = router.apply(&entry);
    try testing.expectEqual(@as(u32, 1), kv_proj.apply_count);

    // Apply same index again — should be skipped
    const result = router.apply(&entry);
    try testing.expectEqual(ApplyResult.skipped_already_applied, result);
    try testing.expectEqual(@as(u32, 1), kv_proj.apply_count); // unchanged
    try testing.expectEqual(@as(u64, 1), router.stats.entries_skipped);
}

test "router: entries must be applied in order" {
    var kv_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerKV(kv_proj.handle());

    // Apply index 1, 2, 3 in order
    _ = router.apply(&makeEntry(.kv_put, 1));
    _ = router.apply(&makeEntry(.kv_delete, 2));
    _ = router.apply(&makeEntry(.kv_put, 3));

    try testing.expectEqual(@as(u32, 3), kv_proj.apply_count);
    try testing.expectEqual(@as(u64, 3), router.applied_index);

    // Index 2 again — skipped
    const r = router.apply(&makeEntry(.kv_put, 2));
    try testing.expectEqual(ApplyResult.skipped_already_applied, r);
    try testing.expectEqual(@as(u32, 3), kv_proj.apply_count);
}

test "router: apply error counted but index advances" {
    var err_proj = ErrorProjection{};
    var router = ProjectionRouter.init();
    router.registerKV(err_proj.handle());

    const entry = makeEntry(.kv_put, 1);
    const result = router.apply(&entry);

    try testing.expectEqual(ApplyResult.apply_error, result);
    try testing.expectEqual(@as(u64, 1), router.stats.entries_error);
    // Index still advances — we don't retry
    try testing.expectEqual(@as(u64, 1), router.applied_index);
}

test "router: unregistered projection acts as no_projection" {
    var router = ProjectionRouter.init();
    // No projections registered

    const entry = makeEntry(.kv_put, 1);
    const result = router.apply(&entry);

    // KV projection not registered — treated as no projection
    try testing.expectEqual(ApplyResult.skipped_no_projection, result);
    try testing.expectEqual(@as(u64, 1), router.applied_index);
}

test "router: batch apply" {
    var kv_proj = TestProjection{};
    var queue_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerKV(kv_proj.handle());
    router.registerQueue(queue_proj.handle());

    const entries = [_]Entry{
        makeEntry(.kv_put, 1),
        makeEntry(.queue_enqueue, 2),
        makeEntry(.stream_append, 3), // no projection
        makeEntry(.kv_delete, 4),
        makeEntry(.raft_noop, 5), // no projection
    };

    const result = router.applyBatch(&entries);

    try testing.expectEqual(@as(u32, 3), result.applied); // kv_put + queue + kv_delete
    try testing.expectEqual(@as(u32, 2), result.no_projection); // stream + noop
    try testing.expectEqual(@as(u32, 5), result.total());
    try testing.expectEqual(@as(u64, 5), router.applied_index);
    try testing.expectEqual(@as(u32, 2), kv_proj.apply_count);
    try testing.expectEqual(@as(u32, 1), queue_proj.apply_count);
}

test "router: total memory usage" {
    var kv_proj = TestProjection{ .memory = 2048 };
    var queue_proj = TestProjection{ .memory = 4096 };
    var router = ProjectionRouter.init();
    router.registerKV(kv_proj.handle());
    router.registerQueue(queue_proj.handle());

    try testing.expectEqual(@as(usize, 6144), router.totalMemoryUsage());
}

test "router: mixed entry types across all projections" {
    var kv_proj = TestProjection{};
    var queue_proj = TestProjection{};
    var ts_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerKV(kv_proj.handle());
    router.registerQueue(queue_proj.handle());
    router.registerTS(ts_proj.handle());

    _ = router.apply(&makeEntry(.kv_put, 1));
    _ = router.apply(&makeEntry(.queue_enqueue, 2));
    _ = router.apply(&makeEntry(.ts_write, 3));
    _ = router.apply(&makeEntry(.cg_commit, 4)); // → KV
    _ = router.apply(&makeEntry(.queue_ack, 5));
    _ = router.apply(&makeEntry(.ts_write_batch, 6));

    try testing.expectEqual(@as(u32, 2), kv_proj.apply_count); // kv_put + cg_commit
    try testing.expectEqual(@as(u32, 2), queue_proj.apply_count); // enqueue + ack
    try testing.expectEqual(@as(u32, 2), ts_proj.apply_count); // write + write_batch
    try testing.expectEqual(@as(u64, 6), router.stats.entries_applied);
}
