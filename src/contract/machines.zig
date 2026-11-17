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
    @import("../settings/machine.zig").machine,
    @import("../ts/machine.zig").machine,
};

/// The namespace a command payload names (a stream's append).
fn commandNamespace(payload: []const u8) ?u32 {
    const cmd = @import("../storage/ual/entry.zig").CommandPayload.deserialize(payload) orelse return null;
    return cmd.namespace_id.int();
}

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

/// Two message types are applied outside a view, once the inbound
/// commits: a stream append, by the shard's stream applier; and a task
/// handed back, whose run's leader ends the lease from its own record.
pub const Channels = @import("channels.zig").ChannelsWith(&targets, &.{
    .{ .entry_type = .stream_append, .refusal = @import("../projection/stream.zig").appendRefusal, .namespace = commandNamespace },
    .{ .entry_type = .action_abandon, .refusal = abandonRefusal, .namespace = abandonNamespace },
});

pub const list = targets ++ [_]machine.Machine{ Channels.machine, @import("../processing/sinks_machine.zig").machine, @import("../stream/machine.zig").machine };

comptime {
    machine.check(&list);
}
