//! The op table and the entry table this build serves, and the hash every
//! client and peer must share with it. A request frame carries the hash in
//! its header and a peer carries it in its hello; any other hash is refused.

const std = @import("std");
const ops = @import("ops.zig");
const entries = @import("entries.zig");

pub const op_table = [_]ops.Op{};
pub const entry_table = [_]entries.Entry{};

comptime {
    ops.check(&op_table);
    entries.check(&entry_table);
}

/// Bump this in any change to a request, answer or log-entry layout that is
/// still coded by hand rather than declared in the tables above: the table
/// hash can't see those layouts, so without a bump a client or node built
/// before the change would be misread instead of refused. It goes once every
/// layout is declared in a table.
pub const HAND_LAYOUT_EPOCH: u32 = 1;

pub const HASH: u64 = hash(&op_table, &entry_table, HAND_LAYOUT_EPOCH);

fn hash(comptime op_list: []const ops.Op, comptime entry_list: []const entries.Entry, comptime epoch: u32) u64 {
    comptime {
        const tables = ops.tableHash(op_list, entry_list);
        var h = std.hash.Wyhash.init(tables);
        h.update(std.mem.asBytes(&epoch));
        return h.final();
    }
}

test "table: the hash changes with the hand layout epoch" {
    try std.testing.expect(comptime hash(&op_table, &entry_table, HAND_LAYOUT_EPOCH) != hash(&op_table, &entry_table, HAND_LAYOUT_EPOCH + 1));
    try std.testing.expectEqual(HASH, comptime hash(&op_table, &entry_table, HAND_LAYOUT_EPOCH));
}
