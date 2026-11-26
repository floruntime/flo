//! A shard leader's walk of the node's namespace table for the quota parts
//! to bind (`settings.quota_bind`): which namespaces' bound part differs
//! from what shard 0 published, a stretch of the table a call. The shard
//! runs it each tick; the VOPR runs the same walk against its own stores.

const std = @import("std");
const view_mod = @import("../contract/view.zig");
const ns_table = @import("../namespace/table.zig");
const settings_machine = @import("machine.zig");
const settings_entries = @import("../protocol/settings_entries.zig");

/// The namespaces one call looks at.
pub const PER_CALL: u32 = 1024;

/// A namespace's quota over every shard, as each of `shards` holds it:
/// split evenly, rounded up; none stays none.
pub fn part(quota: u64, shards: u32) u64 {
    if (quota == settings_machine.NO_QUOTA) return quota;
    return std.math.divCeil(u64, quota, @max(shards, 1)) catch unreachable;
}

pub const Walk = struct {
    /// Where the walk goes on from.
    cursor: u32 = 0,
    /// The table's quota epoch and the leader's term when the walk now
    /// under way began, and whether it has found anything to bind; and the
    /// epoch and term of the last whole walk that found nothing, which
    /// lets the walk stop until either moves.
    walk: struct { epoch: u64 = 0, term: u64 = 0, found: bool = true } = .{},
    clean: ?struct { epoch: u64, term: u64 } = null,
    /// Ids looked at, since start: for tests.
    looked: u64 = 0,

    /// Whether a whole walk at `epoch` and `term` found every part bound.
    pub fn isClean(w: *const Walk, epoch: u64, term: u64) bool {
        const c = w.clean orelse return false;
        return c.epoch == epoch and c.term == term;
    }

    /// The namespaces whose part bound in `read` is due, into `out`,
    /// walking on from the cursor: how many. A namespace is due when its
    /// part differs from the bound one and what was published is no older
    /// than what is bound; none published (index 0) is never due.
    pub fn due(w: *Walk, names: *const ns_table.Table, read: view_mod.ReadView, term: u64, shards: u32, out: []settings_entries.QuotaItem.Value) usize {
        const epoch = names.quotaEpoch();
        if (w.clean) |c| {
            if (c.epoch == epoch and c.term == term) return 0;
            w.clean = null;
        }
        var n: usize = 0;
        var looked: u32 = 0;
        while (n < out.len and looked < PER_CALL) : (looked += 1) {
            if (w.cursor == 0) {
                // A walk ends: clean if it found nothing at one epoch and term.
                if (!w.walk.found and w.walk.epoch == epoch and w.walk.term == term) {
                    w.clean = .{ .epoch = epoch, .term = term };
                    w.walk = .{ .epoch = epoch, .term = term, .found = true };
                    return n;
                }
                w.walk = .{ .epoch = epoch, .term = term, .found = false };
            }
            const id: ns_table.Id = .of(w.cursor);
            w.cursor = (w.cursor + 1) % ns_table.CAPACITY;
            w.looked += 1;
            const q = names.quotaOf(id) orelse continue;
            const p = part(q.quota, shards);
            if (settings_machine.boundQuota(read, id)) |b| {
                if (b.part_bytes == p or q.index < b.published_index) continue;
            }
            out[n] = .{ .ns = id.int(), .part_bytes = p, .published_index = q.index };
            n += 1;
            w.walk.found = true;
        }
        return n;
    }
};
