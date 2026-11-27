//! Projection Router — fans committed UAL entries to the correct projection.
//!
//! The router sits between the Partition (which owns the UAL and Raft log)
//! and the individual projection engines. When Raft commits an entry, the
//! Partition calls `router.apply(entry)` which dispatches by EntryType.
//!
//! Routing table:
//!   ts_*                            → None (the time-series machine)
//!   stream_append, stream_trim      → None (UAL direct reads, zero-copy)
//!   group_*                         → None (the group machine)
//!   raft_config, raft_noop          → None (consensus layer)
//!   raft_snapshot                    → Snapshot installation
//!   processing_sink_batch           → None (the sinks machine)
//!
//! The router maintains `applied_index` for idempotency — entries at or below
//! this index are silently skipped.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;

// ═══════════════════════════════════════════════════════════════════════════════
// Apply Result — what happened when we applied an entry
// ═══════════════════════════════════════════════════════════════════════════════

pub const ApplyResult = enum(u8) {
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

    pub fn init() ProjectionRouter {
        return .{ .applied_index = 0 };
    }

    /// Route a single committed entry by its EntryType, and say what
    /// happened.
    pub fn apply(self: *ProjectionRouter, entry: *const Entry) ApplyResult {
        // Idempotency: skip already-applied entries
        if (entry.header.index <= self.applied_index) return .skipped_already_applied;
        // Advanced whatever the outcome: an entry is never applied twice.
        defer {
            self.applied_index = @max(self.applied_index, entry.header.index);
            self.applied_stamp = @max(self.applied_stamp, entry.header.timestamp_ns);
        }
        return switch (routeTarget(@fromBackingInt(@intCast(entry.header.entry_type)))) {
            .none => .skipped_no_projection,
            .snapshot => .snapshot_triggered,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Routing Target
// ═══════════════════════════════════════════════════════════════════════════════

const RouteTarget = enum(u8) {
    none,
    snapshot,
};

/// Determine which projection an entry type routes to.
pub fn routeTarget(entry_type: EntryType) RouteTarget {
    return switch (entry_type) {
        // KV: its contract machine (kv/machine.zig).
        .kv_put, .kv_delete, .kv_batch, .kv_incr, .kv_expire, .kv_json_patch, .kv_reap => .none,

        // Never written.
        .cg_create => .none,

        // Queues: their contract machine (queue/machine.zig).
        .queue_enqueue, .queue_deliver, .queue_settle, .queue_release, .queue_extend, .queue_expire, .queue_purge, .queue_requeue, .queue_drop => .none,

        // Consumer groups: their contract machine (stream/group_machine.zig).
        .group_create, .group_delete, .group_deliver, .group_settle, .group_release, .group_extend, .group_expire, .group_clean, .group_steal, .group_requeue, .group_drop => .none,

        // Time series: their contract machine (ts/machine.zig).
        .ts_write, .ts_write_batch, .ts_delete, .ts_retention => .none,

        // Stream — no projection (UAL direct reads)
        .stream_append, .stream_trim, .stream_delete, .stream_config, .stream_clean, .stream_forget => .none,
        // A run's history: the stream machine's history families.
        .history_chunk, .history_index, .history_drop => .none,

        // Raft control — consensus layer
        .raft_config, .raft_noop, .repair_skip => .none,

        // Snapshot installation
        .raft_snapshot => .snapshot,

        // Workflows: their contract machines (workflow/defs_machine.zig,
        // workflow/runs_machine.zig).
        .workflow_create, .workflow_start, .workflow_complete, .workflow_enabled, .workflow_step, .workflow_signal, .workflow_run_delete, .workflow_runs_delete => .none,
        .namespace_create, .namespace_alter => .none,
        // Actions: their contract machine (actions/machine.zig).
        .action_register, .action_delete, .action_invoke, .action_deliver, .action_settle, .action_release, .action_extend, .action_expire, .action_run_delete, .action_runs_delete => .none,
        // Calls: the runs machine (workflow/runs_machine.zig).
        .step_call_retry, .step_call_clear, .call_reply, .call_done => .none,
        // A child workflow's start and refusal: the runs machine.
        .child_start, .child_refuse => .none,
        // A hand-back: sent by the actions machine; applied, after its
        // commit, by the actions handler.
        .action_abandon_send, .action_abandon => .none,
        .processing_submit, .processing_stop, .processing_cancel, .processing_remove => .none,
        // Its contract machine (processing/sinks_machine.zig).
        .processing_sink_batch => .none,
        // Cluster settings: their contract machine (settings/machine.zig).
        .settings_pool_share, .settings_node_mem, .settings_node_del, .settings_budget_clean, .settings_budget_bind, .settings_quota_bind => .none,
        .channel_inbound, .channel_trim, .channel_probe, .channel_unpark => .none,
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn makeEntry(entry_type: EntryType, index: u64) Entry {
    return entry_mod.buildEntry(entry_type, entry_mod.Flags.NONE, 1, index, 0, "test");
}

test "router: no entry type routes to a projection but a snapshot" {
    // Every data entry is a contract machine's or a registry applier's.
    inline for (.{ .kv_put, .cg_create, .queue_enqueue, .group_settle, .ts_write, .ts_write_batch, .ts_delete, .ts_retention, .stream_append, .raft_config, .raft_noop }) |t|
        try testing.expectEqual(RouteTarget.none, routeTarget(t));
    try testing.expectEqual(RouteTarget.snapshot, routeTarget(.raft_snapshot));
}

test "router: an entry with no projection is passed" {
    var router = ProjectionRouter.init();
    const entry = makeEntry(.ts_write, 1);
    try testing.expectEqual(ApplyResult.skipped_no_projection, router.apply(&entry));
    try testing.expectEqual(@as(u64, 1), router.applied_index);
}

test "router: raft snapshot triggers installation" {
    var router = ProjectionRouter.init();
    const entry = makeEntry(.raft_snapshot, 1);
    try testing.expectEqual(ApplyResult.snapshot_triggered, router.apply(&entry));
}

test "router: an index at or below the last is skipped" {
    var router = ProjectionRouter.init();
    _ = router.apply(&makeEntry(.ts_write, 1));
    _ = router.apply(&makeEntry(.stream_append, 2));
    _ = router.apply(&makeEntry(.ts_write, 3));
    try testing.expectEqual(@as(u64, 3), router.applied_index);
    try testing.expectEqual(ApplyResult.skipped_already_applied, router.apply(&makeEntry(.ts_write, 2)));
}
