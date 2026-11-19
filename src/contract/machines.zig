//! The state machines this build runs, in the order their families
//! encode. An entry type a machine here owns is applied by it alone; the
//! rest still go to the projection router or the registry until they move.

const machine = @import("machine.zig");

/// The machines a message from another group may be for.
const targets = [_]machine.Machine{
    @import("../kv/machine.zig").machine,
    @import("../actions/machine.zig").machine,
    @import("../actions/calls_machine.zig").machine,
    @import("../workflow/children_machine.zig").machine,
    @import("../queue/machine.zig").machine,
    @import("../stream/group_machine.zig").machine,
    @import("../stream/machine.zig").machine,
    @import("../settings/machine.zig").machine,
    @import("../ts/machine.zig").machine,
};

/// A hand-back's namespace, and why one can't be applied.
fn abandonNamespace(payload: []const u8) ?u32 {
    var diag: @import("../protocol/schema.zig").Diagnostic = .{};
    const p = @import("../protocol/action_entries.zig").Abandon.decodeAll(payload, &diag) catch return null;
    return p.ns;
}

fn abandonRefusal(payload: []const u8) ?[]const u8 {
    var diag: @import("../protocol/schema.zig").Diagnostic = .{};
    _ = @import("../protocol/action_entries.zig").Abandon.decodeAll(payload, &diag) catch return "bad request: a hand-back that doesn't decode";
    return null;
}

/// One message type is applied outside a view, once the inbound commits: a
/// task handed back, whose run's leader ends the lease from its own record.
pub const Channels = @import("channels.zig").ChannelsWith(&targets, &.{
    .{ .entry_type = .action_abandon, .refusal = abandonRefusal, .namespace = abandonNamespace },
});

pub const list = targets ++ [_]machine.Machine{ Channels.machine, @import("../processing/sinks_machine.zig").machine, @import("../namespace/machine.zig").machine, @import("../workflow/defs_machine.zig").machine };

/// Families only a test machine writes: no production family may take
/// their ids either, so the check below sees them too.
const test_only = machine.Machine{
    .name = "test-only families",
    .entry_types = &.{},
    .families = &.{machine.info(@import("channel_probe.zig").GotFam)},
    .keys = struct {
        fn f(_: *const @import("../storage/ual/entry.zig").Entry, _: *@import("view.zig").KeySet) error{OutOfMemory}!void {}
    }.f,
    .apply = struct {
        fn f(_: *@import("view.zig").View, _: *const @import("../storage/ual/entry.zig").Entry, _: *@import("../storage/persistence.zig").Result) @import("../storage/persistence.zig").ApplyError!void {}
    }.f,
};

comptime {
    machine.check(&(list ++ [_]machine.Machine{test_only}));
}
