//! The state machines this build runs, in the order their families
//! encode. An entry type a machine here owns is applied by it alone; the
//! rest are the Raft replica's (`raft_config`) or apply nothing.

const machine = @import("machine.zig");

/// The machines a message from another group may be for.
const targets = [_]machine.Machine{
    @import("../kv/machine.zig").machine,
    @import("../actions/machine.zig").machine,
    @import("../workflow/runs_machine.zig").machine,
    @import("../queue/machine.zig").machine,
    @import("../stream/group_machine.zig").machine,
    @import("../stream/machine.zig").machine,
    @import("../stream/history_machine.zig").machine,
    @import("../settings/machine.zig").machine,
    @import("../ts/machine.zig").machine,
};

pub const Channels = @import("channels.zig").Channels(&targets);

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
