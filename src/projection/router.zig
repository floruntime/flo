//! Projection Router — fans committed UAL entries to the correct projection.
//!
//! The router sits between the Partition (which owns the UAL and Raft log)
//! and the individual projection engines. When Raft commits an entry, the
//! Partition calls `router.apply(entry)` which dispatches by EntryType.
//!
//! Routing table:
//!   ts_write, ts_write_batch        → TSProjection
//!   stream_append, stream_trim      → None (UAL direct reads, zero-copy)
//!   group_*                         → None (the group machine)
//!   raft_config, raft_noop          → None (consensus layer)
//!   raft_snapshot                    → Snapshot installation
//!   checkpoint                      → None (processing runtime)
//!
//! The router maintains `applied_index` for idempotency — entries at or below
//! this index are silently skipped.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");

const persistence = @import("../storage/persistence.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
pub const Result = persistence.Result;
pub const ApplyError = persistence.ApplyError;

// ═══════════════════════════════════════════════════════════════════════════════
// Projection Interface
// ═══════════════════════════════════════════════════════════════════════════════

/// Common interface for all projection engines.
/// Each projection must implement these operations.
pub const ProjectionVTable = struct {
    /// Apply a committed UAL entry to this projection, recording its answer.
    applyFn: *const fn (ctx: *anyopaque, entry: *const Entry, result: *Result) ApplyError!void,
};

/// Type-erased projection handle.
pub const ProjectionHandle = struct {
    ctx: *anyopaque,
    vtable: ProjectionVTable,

    pub fn apply(self: ProjectionHandle, entry: *const Entry, result: *Result) ApplyError!void {
        return self.vtable.applyFn(self.ctx, entry, result);
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
};

// ═══════════════════════════════════════════════════════════════════════════════
// Projection Router
// ═══════════════════════════════════════════════════════════════════════════════

pub const ProjectionRouter = struct {
    /// Last applied UAL index — for idempotency.
    applied_index: u64,
    /// The latest stamp applied. A snapshot records it, so a node restored
    /// from one stamps after it.
    applied_stamp: u64 = 0,

    /// Registered projection engines (optional — set to null if not present).
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
        ts_entries: u64 = 0,
    };

    pub fn init() ProjectionRouter {
        return .{
            .applied_index = 0,
            .ts = null,
            .stats = .{},
        };
    }

    /// Register the TS projection.
    pub fn registerTS(self: *ProjectionRouter, handle: ProjectionHandle) void {
        self.ts = handle;
    }

    /// Apply a single committed entry. Routes by EntryType, and returns
    /// what happened or why its projection could not apply it.
    pub fn apply(self: *ProjectionRouter, entry: *const Entry, out: *Result) ApplyError!ApplyResult {
        // Idempotency: skip already-applied entries
        if (entry.header.index <= self.applied_index) {
            self.stats.entries_skipped += 1;
            return .skipped_already_applied;
        }

        const entry_type: EntryType = @fromBackingInt(@intCast(entry.header.entry_type));
        const target = routeTarget(entry_type);

        // Advanced whatever the outcome: an entry is never applied twice.
        defer {
            self.applied_index = @max(self.applied_index, entry.header.index);
            self.applied_stamp = @max(self.applied_stamp, entry.header.timestamp_ns);
        }
        return switch (target) {
            .ts => self.applyTo(self.ts, entry, out, &self.stats.ts_entries),
            .none => blk: {
                self.stats.entries_no_projection += 1;
                break :blk .skipped_no_projection;
            },
            .snapshot => blk: {
                self.stats.snapshots_triggered += 1;
                break :blk .snapshot_triggered;
            },
        };
    }

    /// Apply a batch of committed entries (e.g., after Raft commit advances).
    pub fn applyBatch(self: *ProjectionRouter, entries: []const Entry) BatchResult {
        var result = BatchResult{};
        for (entries) |*entry| {
            var out = Result.start(entry);
            const r = self.apply(entry, &out) catch {
                result.errors += 1;
                continue;
            };
            switch (r) {
                .applied => result.applied += 1,
                .skipped_already_applied => result.skipped += 1,
                .skipped_no_projection => result.no_projection += 1,
                .snapshot_triggered => result.snapshots += 1,
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

    // ─── Internal ──────────────────────────────────────────────────────────

    fn applyTo(
        self: *ProjectionRouter,
        handle: ?ProjectionHandle,
        entry: *const Entry,
        out: *Result,
        counter: *u64,
    ) ApplyError!ApplyResult {
        if (handle) |h| {
            h.apply(entry, out) catch |err| {
                self.stats.entries_error += 1;
                return err;
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
    ts,
    none,
    snapshot,
};

/// Determine which projection an entry type routes to.
pub fn routeTarget(entry_type: EntryType) RouteTarget {
    return switch (entry_type) {
        // KV: its contract machine (kv/machine.zig).
        .kv_put, .kv_delete, .kv_batch, .kv_incr, .kv_expire, .kv_json_patch => .none,

        // Never written.
        .cg_create => .none,

        // Queues: their contract machine (queue/machine.zig).
        .queue_enqueue, .queue_deliver, .queue_settle, .queue_release, .queue_extend, .queue_expire, .queue_purge => .none,

        // Consumer groups: their contract machine (stream/group_machine.zig).
        .group_create, .group_delete, .group_deliver, .group_settle, .group_release, .group_extend, .group_expire, .group_clean => .none,

        // TimeSeries
        .ts_write, .ts_write_batch, .ts_delete, .ts_retention => .ts,

        // Stream — no projection (UAL direct reads)
        .stream_append, .stream_trim, .stream_delete, .stream_config => .none,

        // Raft control — consensus layer
        .raft_config, .raft_noop, .repair_skip => .none,

        // Snapshot installation
        .raft_snapshot => .snapshot,

        // Registry-owned: applied by the owning handler's callback, never by
        // this router (Shard.assertOneApplier enforces it).
        .workflow_create, .workflow_start, .workflow_complete, .workflow_enabled, .workflow_step_started => .none,
        .namespace_create, .namespace_bind => .none,
        // Actions: their contract machine (actions/machine.zig).
        .action_register, .action_delete, .action_invoke, .action_deliver, .action_settle, .action_release, .action_extend, .action_expire => .none,
        .processing_submit, .processing_stop, .processing_cancel, .processing_checkpoint, .processing_remove => .none,
        // Cluster settings: their contract machine (settings/machine.zig).
        .settings_pool_share, .settings_node_mem, .settings_node_del => .none,
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

    fn applyFn(ctx: *anyopaque, entry: *const Entry, _: *Result) ApplyError!void {
        const self: *TestProjection = @ptrCast(@alignCast(ctx));
        self.apply_count += 1;
        self.last_index = entry.header.index;
    }

    fn handle(self: *TestProjection) ProjectionHandle {
        return .{
            .ctx = @ptrCast(self),
            .vtable = .{ .applyFn = applyFn },
        };
    }
};

/// Test projection that always returns an error.
const ErrorProjection = struct {
    fn applyFn(_: *anyopaque, _: *const Entry, _: *Result) ApplyError!void {
        return error.Halt;
    }

    fn handle(self: *ErrorProjection) ProjectionHandle {
        return .{
            .ctx = @ptrCast(self),
            .vtable = .{ .applyFn = applyFn },
        };
    }
};

fn applyOne(router: *ProjectionRouter, entry: *const Entry) ApplyError!ApplyResult {
    var out = Result.start(entry);
    return router.apply(entry, &out);
}

fn makeEntry(entry_type: EntryType, index: u64) Entry {
    return entry_mod.buildEntry(entry_type, entry_mod.Flags.NONE, 1, index, 0, "test");
}

test "router: routing table correctness" {
    // KV: a contract machine's, not a projection's
    try testing.expectEqual(RouteTarget.none, routeTarget(.kv_put));
    try testing.expectEqual(RouteTarget.none, routeTarget(.kv_delete));
    try testing.expectEqual(RouteTarget.none, routeTarget(.kv_batch));

    // Consumer group → none (applied via stream handler replay registry)
    try testing.expectEqual(RouteTarget.none, routeTarget(.cg_create));

    // Queues: a contract machine's
    try testing.expectEqual(RouteTarget.none, routeTarget(.queue_enqueue));
    try testing.expectEqual(RouteTarget.none, routeTarget(.queue_deliver));

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
}

test "router: ts entry routed to ts projection" {
    var ts_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerTS(ts_proj.handle());

    const entry = makeEntry(.ts_write, 1);
    const result = try applyOne(&router, &entry);

    try testing.expectEqual(ApplyResult.applied, result);
    try testing.expectEqual(@as(u32, 1), ts_proj.apply_count);
    try testing.expectEqual(@as(u64, 1), router.stats.ts_entries);
}

test "router: consumer group entry not routed to a projection" {
    // A consumer group's entries are its machine's, never a projection's.
    var q_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerTS(q_proj.handle());

    const entry = makeEntry(.group_settle, 1);
    const result = try applyOne(&router, &entry);

    try testing.expectEqual(ApplyResult.skipped_no_projection, result);
    try testing.expectEqual(@as(u32, 0), q_proj.apply_count);
    try testing.expectEqual(@as(u64, 0), router.stats.ts_entries);
}

test "router: stream entry has no projection" {
    var router = ProjectionRouter.init();
    const entry = makeEntry(.stream_append, 1);
    const result = try applyOne(&router, &entry);

    try testing.expectEqual(ApplyResult.skipped_no_projection, result);
    try testing.expectEqual(@as(u64, 1), router.stats.entries_no_projection);
}

test "router: raft noop has no projection" {
    var router = ProjectionRouter.init();
    const entry = makeEntry(.raft_noop, 1);
    const result = try applyOne(&router, &entry);

    try testing.expectEqual(ApplyResult.skipped_no_projection, result);
}

test "router: raft snapshot triggers installation" {
    var router = ProjectionRouter.init();
    const entry = makeEntry(.raft_snapshot, 1);
    const result = try applyOne(&router, &entry);

    try testing.expectEqual(ApplyResult.snapshot_triggered, result);
    try testing.expectEqual(@as(u64, 1), router.stats.snapshots_triggered);
}

test "router: idempotency — duplicate entries skipped" {
    var q_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerTS(q_proj.handle());

    const entry = makeEntry(.ts_write, 1);

    // Apply once
    _ = try applyOne(&router, &entry);
    try testing.expectEqual(@as(u32, 1), q_proj.apply_count);

    // Apply same index again — should be skipped
    const result = try applyOne(&router, &entry);
    try testing.expectEqual(ApplyResult.skipped_already_applied, result);
    try testing.expectEqual(@as(u32, 1), q_proj.apply_count); // unchanged
    try testing.expectEqual(@as(u64, 1), router.stats.entries_skipped);
}

test "router: entries must be applied in order" {
    var q_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerTS(q_proj.handle());

    // Apply index 1, 2, 3 in order
    _ = try applyOne(&router, &makeEntry(.ts_write, 1));
    _ = try applyOne(&router, &makeEntry(.ts_write_batch, 2));
    _ = try applyOne(&router, &makeEntry(.ts_write, 3));

    try testing.expectEqual(@as(u32, 3), q_proj.apply_count);
    try testing.expectEqual(@as(u64, 3), router.applied_index);

    // Index 2 again — skipped
    const r = try applyOne(&router, &makeEntry(.ts_write, 2));
    try testing.expectEqual(ApplyResult.skipped_already_applied, r);
    try testing.expectEqual(@as(u32, 3), q_proj.apply_count);
}

test "router: apply error counted but index advances" {
    var err_proj = ErrorProjection{};
    var router = ProjectionRouter.init();
    router.registerTS(err_proj.handle());

    const entry = makeEntry(.ts_write, 1);
    try testing.expectError(error.Halt, applyOne(&router, &entry));
    try testing.expectEqual(@as(u64, 1), router.stats.entries_error);
    // Index still advances — we don't retry
    try testing.expectEqual(@as(u64, 1), router.applied_index);
}

test "router: unregistered projection acts as no_projection" {
    var router = ProjectionRouter.init();
    // No projections registered

    const entry = makeEntry(.ts_write, 1);
    const result = try applyOne(&router, &entry);

    // TS projection not registered — treated as no projection
    try testing.expectEqual(ApplyResult.skipped_no_projection, result);
    try testing.expectEqual(@as(u64, 1), router.applied_index);
}

test "router: batch apply" {
    var ts_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerTS(ts_proj.handle());

    const entries = [_]Entry{
        makeEntry(.ts_write, 1),
        makeEntry(.queue_enqueue, 2), // a contract machine's
        makeEntry(.stream_append, 3), // no projection
        makeEntry(.ts_delete, 4),
        makeEntry(.raft_noop, 5), // no projection
    };

    const result = router.applyBatch(&entries);

    try testing.expectEqual(@as(u32, 2), result.applied); // ts_write + ts_delete
    try testing.expectEqual(@as(u32, 3), result.no_projection); // queue + stream + noop
    try testing.expectEqual(@as(u32, 5), result.total());
    try testing.expectEqual(@as(u64, 5), router.applied_index);
    try testing.expectEqual(@as(u32, 2), ts_proj.apply_count);
}

test "router: mixed entry types across all projections" {
    var ts_proj = TestProjection{};
    var router = ProjectionRouter.init();
    router.registerTS(ts_proj.handle());

    _ = try applyOne(&router, &makeEntry(.kv_put, 1)); // → none (the KV machine)
    _ = try applyOne(&router, &makeEntry(.queue_enqueue, 2)); // → none (the queue machine)
    _ = try applyOne(&router, &makeEntry(.ts_write, 3));
    _ = try applyOne(&router, &makeEntry(.group_settle, 4)); // → none (the group machine)
    _ = try applyOne(&router, &makeEntry(.ts_write_batch, 5));

    try testing.expectEqual(@as(u32, 2), ts_proj.apply_count); // write + write_batch
    try testing.expectEqual(@as(u64, 2), router.stats.entries_applied);
    try testing.expectEqual(@as(u64, 3), router.stats.entries_no_projection);
}
