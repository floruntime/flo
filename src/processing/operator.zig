//! Operator Interface
//!
//! Defines the core operator contract using Zig's vtable pattern.
//! All operators (map, filter, window, etc.) implement this interface.
//!
//! Design:
//! - Vtable-based polymorphism
//! - processElement: main data path — transform input records
//! - processWatermark: handle watermark advancement (triggers windows, timers)
//! - snapshotState/restoreState: checkpointing (Phase 3)
//! - close: cleanup resources

const std = @import("std");
const Allocator = std.mem.Allocator;
const record_mod = @import("record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const Watermark = record_mod.Watermark;
const OperatorContext = @import("context.zig").OperatorContext;

// =============================================================================
// Default no-op checkpoint functions for stateless operators
// =============================================================================

/// Default snapshotState: returns null (no state to snapshot).
/// Used by stateless operators (map, filter, flatmap, keyby).
pub fn noOpSnapshot(_: *anyopaque, _: u64, _: Allocator) anyerror!?[]u8 {
    return null;
}

/// Default restoreState: does nothing.
/// Used by stateless operators.
pub fn noOpRestore(_: *anyopaque, _: u64, _: []const u8) anyerror!void {}

// =============================================================================
// Operator - Core interface via vtable
// =============================================================================

/// Core operator interface.
///
/// All processing operators implement this interface. The processing chain
/// calls processElement for each input record, passing an OperatorContext
/// that the operator uses to emit output records.
pub const Operator = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Process a single input record.
        /// The operator should emit zero or more records via ctx.emit().
        processElement: *const fn (
            ptr: *anyopaque,
            record: ProcessingRecord,
            ctx: *OperatorContext,
        ) anyerror!void,

        /// Handle watermark advancement.
        processWatermark: *const fn (
            ptr: *anyopaque,
            watermark: Watermark,
            ctx: *OperatorContext,
        ) anyerror!void,

        /// Return the operator name (for logging, metrics, topology display)
        getName: *const fn (ptr: *anyopaque) []const u8,

        /// Cleanup resources when the operator is shut down
        close: *const fn (ptr: *anyopaque) void,

        /// Snapshot operator state for a checkpoint.
        /// Returns serialized state bytes, or null if operator is stateless.
        snapshotState: *const fn (ptr: *anyopaque, checkpoint_id: u64, allocator: Allocator) anyerror!?[]u8,

        /// Restore operator state from a checkpoint.
        restoreState: *const fn (ptr: *anyopaque, checkpoint_id: u64, data: []const u8) anyerror!void,
    };

    /// Process a record through this operator
    pub fn processElement(self: Operator, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        return self.vtable.processElement(self.ptr, rec, ctx);
    }

    /// Handle a watermark
    pub fn processWatermark(self: Operator, wm: Watermark, ctx: *OperatorContext) !void {
        return self.vtable.processWatermark(self.ptr, wm, ctx);
    }

    /// Get the operator name
    pub fn getName(self: Operator) []const u8 {
        return self.vtable.getName(self.ptr);
    }

    /// Close the operator
    pub fn close(self: Operator) void {
        return self.vtable.close(self.ptr);
    }

    /// Snapshot operator state for checkpointing
    pub fn snapshotState(self: Operator, checkpoint_id: u64, allocator: Allocator) !?[]u8 {
        return self.vtable.snapshotState(self.ptr, checkpoint_id, allocator);
    }

    /// Restore operator state from a checkpoint
    pub fn restoreState(self: Operator, checkpoint_id: u64, data: []const u8) !void {
        return self.vtable.restoreState(self.ptr, checkpoint_id, data);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "Operator vtable signature compiles" {
    const T = Operator.VTable;
    try std.testing.expect(@sizeOf(T) > 0);
}
