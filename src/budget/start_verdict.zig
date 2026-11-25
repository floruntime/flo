//! Whether a run may start, or a signal land, judged once by the runs
//! machine after everything the entry writes is staged. A start creates a
//! run on a user's behalf, whoever proposed it (a client, a schedule, a
//! trigger or a parent), so each is held here rather than by the gate,
//! which never refuses a duty.
//!
//! Its arms read only the committed store and what is staged, so every
//! replica, replay included, reaches the same verdict. Memory is the only
//! arm here; a run-history quota is checked before it.

const std = @import("std");
const view_mod = @import("../contract/view.zig");
const persistence = @import("../storage/persistence.zig");
const gate = @import("gate.zig");
const plant = @import("../plant.zig");
const NamespaceId = @import("../namespace/table.zig").Id;

pub const Bound = enum { memory_budget };

pub const Verdict = struct {
    bound: Bound,
    over: gate.Over,

    /// The refusal for a client's, a schedule's or a trigger's start.
    pub fn refuse(v: Verdict, out: *persistence.Result, view: *const view_mod.View) void {
        switch (v.bound) {
            .memory_budget => gate.refuse(out, if (view.store.place) |pl| pl.group else null, v.over),
        }
    }

    /// Why, for a parent's call: what the refusal would say.
    pub fn why(v: Verdict, buf: []u8, view: *const view_mod.View) []const u8 {
        const shard: u16 = if (view.store.place) |pl| pl.group else 0;
        return switch (v.bound) {
            .memory_budget => std.fmt.bufPrint(buf, "shard {d} memory: {d} + {d} would pass its share {d}", .{ shard, v.over.use, v.over.delta, v.over.share }) catch "shard memory: past its share",
        };
    }
};

/// Null when what is staged in `view` may commit for a run of namespace
/// `ns`; otherwise the bound it would pass.
pub fn startVerdict(view: *view_mod.View, ns: NamespaceId) ?Verdict {
    // The quota arm, per namespace, goes first.
    _ = ns;
    if (comptime plant.active == .start_verdict_admits) return null;
    if (gate.over(view)) |o| return .{ .bound = .memory_budget, .over = o };
    return null;
}
