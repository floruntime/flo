//! Checkpoint Coordinator
//!
//! Orchestrates the Chandy-Lamport distributed snapshot protocol:
//!
//! 1. Trigger checkpoint → assign new checkpoint_id
//! 2. Inject CheckpointBarrier into sources
//! 3. Sources save offsets → forward barrier downstream
//! 4. Operators receive barrier → snapshot state → forward barrier
//! 5. Sinks receive barrier → acknowledge
//! 6. All acks received → mark checkpoint complete

const std = @import("std");
const Allocator = std.mem.Allocator;
const record_mod = @import("../record.zig");
const CheckpointBarrier = record_mod.CheckpointBarrier;
const storage_mod = @import("storage.zig");
const CheckpointStore = storage_mod.CheckpointStore;
const CheckpointMeta = storage_mod.CheckpointMeta;
const CheckpointStatus = storage_mod.CheckpointStatus;

// =============================================================================
// PendingCheckpoint
// =============================================================================

/// Tracks in-flight checkpoint acknowledgment state.
pub const PendingCheckpoint = struct {
    checkpoint_id: u64,
    start_time_ms: i64,
    total_operators: u32,
    acked_operators: u32,
    offsets_saved: bool,
    sink_acked: bool,

    pub fn isComplete(self: *const PendingCheckpoint) bool {
        return self.acked_operators >= self.total_operators and
            self.offsets_saved and self.sink_acked;
    }
};

// =============================================================================
// CheckpointCoordinator
// =============================================================================

/// Coordinates the checkpoint lifecycle for a processing chain.
///
/// Usage:
///   1. Create coordinator with operator count + store
///   2. Call triggerCheckpoint() to start a new checkpoint
///   3. Chain processes barrier → calls acknowledgeOperator/Source/Sink
///   4. When all acks received, checkpoint is marked complete
pub const CheckpointCoordinator = struct {
    allocator: Allocator,
    store: *CheckpointStore,
    next_checkpoint_id: u64,
    num_operators: u32,
    /// Currently in-progress checkpoint (null = none pending)
    pending: ?PendingCheckpoint,
    /// Count of completed checkpoints
    completed_count: u64,
    /// Last completed checkpoint ID
    last_completed_id: ?u64,

    const Self = @This();

    pub fn init(allocator: Allocator, store: *CheckpointStore, num_operators: u32) Self {
        return .{
            .allocator = allocator,
            .store = store,
            .next_checkpoint_id = 1,
            .num_operators = num_operators,
            .pending = null,
            .completed_count = 0,
            .last_completed_id = null,
        };
    }

    /// Trigger a new checkpoint. Returns the barrier to inject into sources.
    ///
    /// Fails if there's already a pending checkpoint (one at a time).
    pub fn triggerCheckpoint(self: *Self) !CheckpointBarrier {
        if (self.pending != null) return error.CheckpointAlreadyPending;

        const checkpoint_id = self.next_checkpoint_id;
        self.next_checkpoint_id += 1;
        const now = std.time.milliTimestamp();

        // Create pending tracking
        self.pending = .{
            .checkpoint_id = checkpoint_id,
            .start_time_ms = now,
            .total_operators = self.num_operators,
            .acked_operators = 0,
            .offsets_saved = false,
            .sink_acked = false,
        };

        // Store initial meta
        const meta = CheckpointMeta{
            .checkpoint_id = checkpoint_id,
            .timestamp_ms = now,
            .status = .in_progress,
            .acked_operators = 0,
            .total_operators = self.num_operators,
            .offsets_saved = false,
        };
        try self.store.saveMeta(&meta);

        // Return barrier for injection
        return .{
            .checkpoint_id = checkpoint_id,
            .timestamp_ms = now,
            .checkpoint_type = .full,
        };
    }

    /// Acknowledge that an operator has completed its snapshot.
    pub fn acknowledgeOperator(self: *Self, checkpoint_id: u64, operator_name: []const u8, state_data: ?[]const u8) !void {
        const pending = &(self.pending orelse return error.NoCheckpointPending);
        if (pending.checkpoint_id != checkpoint_id) return error.CheckpointIdMismatch;

        // Store operator state if non-null
        if (state_data) |data| {
            try self.store.saveOperatorState(checkpoint_id, operator_name, data);
        }

        pending.acked_operators += 1;
        try self.tryComplete();
    }

    /// Acknowledge that source offsets have been saved.
    pub fn acknowledgeSource(self: *Self, checkpoint_id: u64, offset_data: []const u8) !void {
        const pending = &(self.pending orelse return error.NoCheckpointPending);
        if (pending.checkpoint_id != checkpoint_id) return error.CheckpointIdMismatch;

        try self.store.saveSourceOffsets(checkpoint_id, offset_data);
        pending.offsets_saved = true;
        try self.tryComplete();
    }

    /// Acknowledge that the sink has received the barrier.
    pub fn acknowledgeSink(self: *Self, checkpoint_id: u64) !void {
        const pending = &(self.pending orelse return error.NoCheckpointPending);
        if (pending.checkpoint_id != checkpoint_id) return error.CheckpointIdMismatch;

        pending.sink_acked = true;
        try self.tryComplete();
    }

    /// Check if all acks received and finalize the checkpoint.
    fn tryComplete(self: *Self) !void {
        const pending = self.pending orelse return;
        if (!pending.isComplete()) return;

        // Mark complete in store
        try self.store.markCompleted(pending.checkpoint_id);

        self.last_completed_id = pending.checkpoint_id;
        self.completed_count += 1;
        self.pending = null;
    }

    /// Returns true if a checkpoint is currently in progress.
    pub fn isPending(self: *const Self) bool {
        return self.pending != null;
    }

    /// Get the current pending checkpoint ID, if any.
    pub fn pendingId(self: *const Self) ?u64 {
        if (self.pending) |p| return p.checkpoint_id;
        return null;
    }

    /// Get the last successfully completed checkpoint ID.
    pub fn lastCompletedId(self: *const Self) ?u64 {
        return self.last_completed_id;
    }

    /// Get the total number of completed checkpoints.
    pub fn completedCount(self: *const Self) u64 {
        return self.completed_count;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "CheckpointCoordinator trigger and complete" {
    const allocator = std.testing.allocator;
    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();

    var coord = CheckpointCoordinator.init(allocator, &store, 2);

    // Trigger checkpoint
    const barrier = try coord.triggerCheckpoint();
    try std.testing.expectEqual(@as(u64, 1), barrier.checkpoint_id);
    try std.testing.expect(coord.isPending());

    // Acknowledge operators
    try coord.acknowledgeOperator(1, "map-op", null);
    try std.testing.expect(coord.isPending());

    try coord.acknowledgeOperator(1, "filter-op", "some-state");
    try std.testing.expect(coord.isPending()); // Still needs source + sink

    // Acknowledge source
    try coord.acknowledgeSource(1, "offset-data");
    try std.testing.expect(coord.isPending()); // Still needs sink

    // Acknowledge sink
    try coord.acknowledgeSink(1);
    try std.testing.expect(!coord.isPending()); // Complete!

    try std.testing.expectEqual(@as(?u64, 1), coord.lastCompletedId());
    try std.testing.expectEqual(@as(u64, 1), coord.completedCount());
}

test "CheckpointCoordinator reject double trigger" {
    const allocator = std.testing.allocator;
    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();

    var coord = CheckpointCoordinator.init(allocator, &store, 1);
    _ = try coord.triggerCheckpoint();

    // Second trigger should fail
    const result = coord.triggerCheckpoint();
    try std.testing.expectError(error.CheckpointAlreadyPending, result);
}

test "CheckpointCoordinator sequential checkpoints" {
    const allocator = std.testing.allocator;
    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();

    var coord = CheckpointCoordinator.init(allocator, &store, 0); // No operators

    // First checkpoint (no operators, just source + sink)
    const b1 = try coord.triggerCheckpoint();
    try coord.acknowledgeSource(b1.checkpoint_id, "off1");
    try coord.acknowledgeSink(b1.checkpoint_id);
    try std.testing.expectEqual(@as(?u64, 1), coord.lastCompletedId());

    // Second checkpoint
    const b2 = try coord.triggerCheckpoint();
    try std.testing.expectEqual(@as(u64, 2), b2.checkpoint_id);
    try coord.acknowledgeSource(b2.checkpoint_id, "off2");
    try coord.acknowledgeSink(b2.checkpoint_id);
    try std.testing.expectEqual(@as(?u64, 2), coord.lastCompletedId());
    try std.testing.expectEqual(@as(u64, 2), coord.completedCount());
}

test "CheckpointCoordinator stores operator state" {
    const allocator = std.testing.allocator;
    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();

    var coord = CheckpointCoordinator.init(allocator, &store, 1);
    _ = try coord.triggerCheckpoint();

    try coord.acknowledgeOperator(1, "window-op", "window-state-bytes");
    try coord.acknowledgeSource(1, "offsets");
    try coord.acknowledgeSink(1);

    // Verify state was stored
    const state = (try store.getOperatorState(1, "window-op")).?;
    try std.testing.expectEqualStrings("window-state-bytes", state);
}
