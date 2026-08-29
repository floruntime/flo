//! FloVOPR — deterministic simulation & scenario testing
//!
//! One seed reproduces an entire cluster run: `Scenario.fromSeed` derives
//! the fault/workload profile (swarm testing) and `Workload.synthesize`
//! derives every op payload.
//!
//! Test/tooling surface: exported from `lib.zig`, but nothing in the
//! server binary references it.

pub const scenario = @import("scenario.zig");
pub const Scenario = scenario.Scenario;
pub const HardStateMode = scenario.HardStateMode;
pub const DurabilityMode = scenario.DurabilityMode;

pub const workload = @import("workload.zig");
pub const Workload = workload.Workload;
pub const Op = workload.Op;
pub const OpState = workload.OpState;

test {
    _ = scenario;
    _ = workload;
}
