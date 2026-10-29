//! The state machines this build runs, in the order their families
//! encode. An entry type a machine here owns is applied by it alone; the
//! rest still go to the projection router or the registry until they move.

const machine = @import("machine.zig");

pub const list = [_]machine.Machine{};

comptime {
    machine.check(&list);
}
