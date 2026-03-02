//! Recovery Manager
//!
//! Restores processing state from a completed checkpoint:
//!
//! 1. Find latest completed checkpoint in CheckpointStore
//! 2. Load operator state snapshots → call restoreState on each operator
//! 3. Load source offsets → rewind sources to checkpoint boundary
//! 4. Chain resumes processing from the restored position
//!
//! This achieves exactly-once semantics: state is rolled back and source
//! replays records from the checkpoint offset, reprocessing any records
//! that were in-flight when the failure occurred.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("../operator.zig").Operator;
const Source = @import("../endpoints/source.zig").Source;
const Sink = @import("../endpoints/sink.zig").Sink;
const storage_mod = @import("storage.zig");
const CheckpointStore = storage_mod.CheckpointStore;
const CheckpointMeta = storage_mod.CheckpointMeta;
const offsets_mod = @import("offsets.zig");
const SourceOffsetTracker = offsets_mod.SourceOffsetTracker;

// =============================================================================
// RecoveryResult
// =============================================================================

pub const RecoveryResult = struct {
    /// The checkpoint ID that was restored from
    checkpoint_id: u64,
    /// Number of operators whose state was restored
    operators_restored: u32,
    /// Whether source offsets were restored
    offsets_restored: bool,
};

// =============================================================================
// RecoveryManager
// =============================================================================

/// Manages recovery from checkpoint state.
///
/// Usage:
///   var rm = RecoveryManager.init(allocator, &store);
///   if (try rm.recover(operators, source)) |result| {
///       // State restored, resume processing
///   } else {
///       // No checkpoint found, start from scratch
///   }
pub const RecoveryManager = struct {
    allocator: Allocator,
    store: *CheckpointStore,

    const Self = @This();

    pub fn init(allocator: Allocator, store: *CheckpointStore) Self {
        return .{
            .allocator = allocator,
            .store = store,
        };
    }

    /// Find the latest completed checkpoint ID.
    pub fn findLatestCheckpoint(self: *Self) ?u64 {
        return self.store.getLatestCompleted();
    }

    /// Recover processing state from the latest completed checkpoint.
    ///
    /// Returns RecoveryResult if a checkpoint was found and restored,
    /// null if no checkpoint exists (fresh start).
    pub fn recover(
        self: *Self,
        operators: []const Operator,
        source: ?Source,
    ) !?RecoveryResult {
        const checkpoint_id = self.findLatestCheckpoint() orelse return null;

        // Verify checkpoint meta exists and is completed
        const meta = (try self.store.getMeta(checkpoint_id)) orelse return null;
        if (meta.status != .completed) return null;

        var operators_restored: u32 = 0;

        // Restore each operator's state
        for (operators) |op| {
            const name = op.getName();
            if (try self.store.getOperatorState(checkpoint_id, name)) |state_data| {
                try op.restoreState(checkpoint_id, state_data);
                operators_restored += 1;
            }
        }

        // Restore source offsets
        var offsets_restored = false;
        if (try self.store.getSourceOffsets(checkpoint_id)) |offset_data| {
            if (source) |src| {
                try src.restoreOffsets(offset_data);
                offsets_restored = true;
            }
        }

        return .{
            .checkpoint_id = checkpoint_id,
            .operators_restored = operators_restored,
            .offsets_restored = offsets_restored,
        };
    }

    /// Recover from a specific checkpoint ID (not necessarily the latest).
    pub fn recoverFrom(
        self: *Self,
        checkpoint_id: u64,
        operators: []const Operator,
        source: ?Source,
    ) !?RecoveryResult {
        const meta = (try self.store.getMeta(checkpoint_id)) orelse return null;
        if (meta.status != .completed) return null;

        var operators_restored: u32 = 0;

        for (operators) |op| {
            const name = op.getName();
            if (try self.store.getOperatorState(checkpoint_id, name)) |state_data| {
                try op.restoreState(checkpoint_id, state_data);
                operators_restored += 1;
            }
        }

        var offsets_restored = false;
        if (try self.store.getSourceOffsets(checkpoint_id)) |offset_data| {
            if (source) |src| {
                try src.restoreOffsets(offset_data);
                offsets_restored = true;
            }
        }

        return .{
            .checkpoint_id = checkpoint_id,
            .operators_restored = operators_restored,
            .offsets_restored = offsets_restored,
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

test "RecoveryManager no checkpoint returns null" {
    const allocator = std.testing.allocator;
    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();

    var rm = RecoveryManager.init(allocator, &store);
    const result = try rm.recover(&.{}, null);
    try std.testing.expect(result == null);
}

test "RecoveryManager recover from completed checkpoint" {
    const allocator = std.testing.allocator;
    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();

    // Simulate a completed checkpoint
    const meta = CheckpointMeta{
        .checkpoint_id = 5,
        .timestamp_ms = 1000,
        .status = .completed,
        .acked_operators = 1,
        .total_operators = 1,
        .offsets_saved = true,
    };
    try store.saveMeta(&meta);
    try store.markCompleted(5);
    try store.saveOperatorState(5, "test-map", "operator-state");
    try store.saveSourceOffsets(5, "source-offsets");

    var rm = RecoveryManager.init(allocator, &store);

    // Recover with no actual operators (just tests store lookup)
    const result = (try rm.recover(&.{}, null)).?;
    try std.testing.expectEqual(@as(u64, 5), result.checkpoint_id);
    try std.testing.expectEqual(@as(u32, 0), result.operators_restored); // No operator matched
    try std.testing.expect(!result.offsets_restored); // No source provided
}

test "RecoveryManager recover with source" {
    const allocator = std.testing.allocator;
    const SliceSource = @import("../endpoints/source.zig").SliceSource;
    const ProcessingRecord = @import("../record.zig").ProcessingRecord;

    var store = CheckpointStore.initForTesting(allocator);
    defer store.deinit();

    const records = [_]ProcessingRecord{
        ProcessingRecord.init("k1", "v1", 10),
        ProcessingRecord.init("k2", "v2", 20),
        ProcessingRecord.init("k3", "v3", 30),
    };
    var src = SliceSource.init("test-src", &records);

    // Advance past first 2 records
    _ = try src.source().poll();
    _ = try src.source().poll();

    // Save offsets (position=2) via snapshot
    const off_data = (try src.source().snapshotOffsets(allocator)).?;
    defer allocator.free(off_data);

    // Complete checkpoint
    const meta = CheckpointMeta{
        .checkpoint_id = 3,
        .timestamp_ms = 999,
        .status = .completed,
        .acked_operators = 0,
        .total_operators = 0,
        .offsets_saved = true,
    };
    try store.saveMeta(&meta);
    try store.markCompleted(3);
    try store.saveSourceOffsets(3, off_data);

    // Advance past third record (position=3 now)
    _ = try src.source().poll();

    // Recover → should rewind source to position 2
    var rm = RecoveryManager.init(allocator, &store);
    const result = (try rm.recover(&.{}, src.source())).?;
    try std.testing.expectEqual(@as(u64, 3), result.checkpoint_id);
    try std.testing.expect(result.offsets_restored);

    // Source should now produce k3 (position was rewound to 2)
    const element = try src.source().poll();
    try std.testing.expect(element != null);
    switch (element.?) {
        .record => |rec| try std.testing.expectEqualStrings("k3", rec.key),
        else => return error.UnexpectedElement,
    }
}
