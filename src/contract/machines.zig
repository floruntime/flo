//! The state machines this build runs, in the order their families
//! encode. An entry type a machine here owns is applied by it alone; the
//! rest still go to the projection router or the registry until they move.

const machine = @import("machine.zig");

/// The machines a message from another group may be for.
const targets = [_]machine.Machine{
    @import("../kv/machine.zig").machine,
    @import("../actions/machine.zig").machine,
    @import("../queue/machine.zig").machine,
    @import("../stream/group_machine.zig").machine,
    @import("../settings/machine.zig").machine,
    @import("../ts/machine.zig").machine,
};

/// A stream append is the one message type applied outside a view: the
/// shard runs its applier once the inbound commits.
pub const list = targets ++ [_]machine.Machine{@import("channels.zig").ChannelsWith(&targets, &.{
    .{ .entry_type = .stream_append, .refusal = @import("../projection/stream.zig").appendRefusal },
}).machine};

comptime {
    machine.check(&list);
}
