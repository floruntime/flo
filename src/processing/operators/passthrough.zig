//! Passthrough Operator
//!
//! An identity operator that emits every input record unchanged.
//! Useful for testing, debugging, and pipeline placeholders.
//!
//! YAML example:
//!   ```yaml
//!   operators:
//!     - type: passthrough
//!       name: debug-tap
//!   ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("../operator.zig").Operator;
const OperatorContext = @import("../context.zig").OperatorContext;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;

pub const PassthroughOperator = struct {
    name: []const u8,

    const Self = @This();

    pub fn init(name: []const u8) Self {
        return .{ .name = name };
    }

    /// Return an Operator interface backed by this PassthroughOperator
    pub fn operator(self: *Self) Operator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Operator.VTable{
        .processElement = processElement,
        .getName = getName,
        .close = close,
    };

    fn processElement(_: *anyopaque, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        try ctx.emit(rec);
    }

    fn getName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn close(_: *anyopaque) void {}
};

// =============================================================================
// Tests
// =============================================================================

test "PassthroughOperator — getName" {
    var op = PassthroughOperator.init("debug-tap");
    const iface = op.operator();
    try std.testing.expectEqualStrings("debug-tap", iface.getName());
}
