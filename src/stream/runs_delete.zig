//! What the two bulk deletes of ended runs share, a workflow's and an
//! action's (`workflow_runs_delete`, `action_runs_delete`): reading their
//! ended indexes, and which orphans an answer counts.

const std = @import("std");
const ae = @import("../protocol/action_entries.zig");
const hm = @import("history_machine.zig");

/// The time an ended-index key under `prefix_len` bytes says its run ended.
pub fn endedMsOf(key: []const u8, prefix_len: usize) i64 {
    if (key.len < prefix_len + 8) return std.math.maxInt(i64);
    return @intCast(@min(std.mem.readInt(u64, key[prefix_len..][0..8], .big), std.math.maxInt(i64)));
}

/// The orphans a bulk delete's answer leaves out of its count: those that
/// ended at or after its bound, and those it drops itself.
pub const Skip = struct {
    before_ms: i64,
    taken: @FieldType(ae.RunsDelete.Value, "orphans"),

    pub fn has(self: Skip, o: hm.Orphan) bool {
        if (@as(i64, @intCast(@min(o.completed_ms, std.math.maxInt(i64)))) >= self.before_ms) return true;
        var it = self.taken.iterator();
        while (it.next() catch return false) |t| {
            if (t.created_ns == o.created_ns and std.mem.eql(u8, t.run, o.run)) return true;
        }
        return false;
    }
};
