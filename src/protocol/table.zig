//! The op table and the entry table this build serves, and the hash every
//! client and peer must share with it. A request frame carries the hash in
//! its header and a peer carries it in its hello; any other hash is refused.

const std = @import("std");
const ops = @import("ops.zig");
const entries = @import("entries.zig");
const kv_ops = @import("kv_ops.zig");
const action_ops = @import("action_ops.zig");
const worker_ops = @import("worker_ops.zig");
const namespace_ops = @import("namespace_ops.zig");
const system_ops = @import("system_ops.zig");
const kv_entries = @import("kv_entries.zig");
const namespace_entries = @import("namespace_entries.zig");
const golden = @import("golden.zig");
const proto = @import("proto.zig");
const EntryType = @import("../storage/ual/entry.zig").EntryType;

pub const op_table = system_ops.table ++ namespace_ops.table ++ kv_ops.table ++ action_ops.table ++ worker_ops.table;
/// The groups the table's ops form under their families.
pub const op_groups = kv_ops.groups;
pub const entry_table = kv_entries.table ++ namespace_entries.table;

comptime {
    ops.check(&op_table);
    entries.check(&entry_table);
}

/// Whether `code` is a table op: its request is `[namespace][key][body]`,
/// the body coded by its schema.
pub fn isOp(code: u16) bool {
    inline for (op_table) |op| if (op.code == code) return true;
    return false;
}

/// Whether table op `code` is sent only by another shard or node.
pub fn isInternal(code: u16) bool {
    inline for (op_table) |op| if (op.code == code) return op.origin == .internal;
    return false;
}

/// Whether table op `code` administers the node.
pub fn isAdmin(code: u16) bool {
    inline for (op_table) |op| if (op.code == code) return op.class == .admin;
    return false;
}

/// Whether table op `code` may create its namespace.
pub fn creates(code: u16) bool {
    inline for (op_table) |op| if (op.code == code) return op.creates;
    return false;
}

/// Bump this in any change to a layout still coded by hand with no golden
/// vector (`golden.NO_VECTOR_YET`): the hash can't see those, so without a
/// bump a client or node built before the change would be misread instead
/// of refused. The op codes, entry types, status codes and refusal reasons
/// are hashed by name and value, and the golden vectors by their bytes, so
/// adding one of those or changing a vectored layout needs no bump. It goes
/// once that list is empty.
pub const HAND_LAYOUT_EPOCH: u32 = 6;

pub const HASH: u64 = hash(&op_table, &entry_table, HAND_LAYOUT_EPOCH);

fn hash(comptime op_list: []const ops.Op, comptime entry_list: []const entries.Entry, comptime epoch: u32) u64 {
    comptime {
        @setEvalBranchQuota(1_000_000);
        const tables = ops.tableHash(op_list, entry_list);
        var h = std.hash.Wyhash.init(tables);
        h.update(std.mem.asBytes(&epoch));
        ops.hashEnum(&h, proto.OpCode);
        ops.hashEnum(&h, EntryType);
        ops.hashEnum(&h, proto.StatusCode);
        golden.hash(&h);
        return h.final();
    }
}

test "table: the hash changes with the hand layout epoch" {
    try std.testing.expect(comptime hash(&op_table, &entry_table, HAND_LAYOUT_EPOCH) != hash(&op_table, &entry_table, HAND_LAYOUT_EPOCH + 1));
    try std.testing.expectEqual(HASH, comptime hash(&op_table, &entry_table, HAND_LAYOUT_EPOCH));
}
