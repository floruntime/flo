//! Operator Interface
//!
//! The contract every processing operator implements, as a vtable:
//! processElement transforms one input record, emitting zero or more
//! through the context; close releases what the operator holds.

const std = @import("std");
const record_mod = @import("record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const OperatorContext = @import("context.zig").OperatorContext;

/// Core operator interface.
///
/// The pipeline calls processElement for each input record, passing an
/// OperatorContext that the operator uses to emit output records.
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

        /// Return the operator name (for logging, metrics, topology display)
        getName: *const fn (ptr: *anyopaque) []const u8,

        /// Cleanup resources when the operator is shut down
        close: *const fn (ptr: *anyopaque) void,
    };

    /// Process a record through this operator
    pub fn processElement(self: Operator, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        return self.vtable.processElement(self.ptr, rec, ctx);
    }

    /// Get the operator name
    pub fn getName(self: Operator) []const u8 {
        return self.vtable.getName(self.ptr);
    }

    /// Close the operator
    pub fn close(self: Operator) void {
        return self.vtable.close(self.ptr);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "Operator vtable signature compiles" {
    const T = Operator.VTable;
    try std.testing.expect(@sizeOf(T) > 0);
}
