//! The op table and the entry table this build serves, and the hash every
//! client and peer must share with it. A request frame carries the hash in
//! its header and a peer carries it in its hello; any other hash is refused.

const std = @import("std");
const ops = @import("ops.zig");
const entries = @import("entries.zig");
const kv_ops = @import("kv_ops.zig");
const action_ops = @import("action_ops.zig");
const worker_ops = @import("worker_ops.zig");
const processing_ops = @import("processing_ops.zig");
const ts_ops = @import("ts_ops.zig");
const stream_ops = @import("stream_ops.zig");
const queue_ops = @import("queue_ops.zig");
const namespace_ops = @import("namespace_ops.zig");
const system_ops = @import("system_ops.zig");
const kv_entries = @import("kv_entries.zig");
const namespace_entries = @import("namespace_entries.zig");
const action_entries = @import("action_entries.zig");
const queue_entries = @import("queue_entries.zig");
const group_entries = @import("group_entries.zig");
const golden = @import("golden.zig");
const proto = @import("proto.zig");
const EntryType = @import("../storage/ual/entry.zig").EntryType;

pub const op_table = system_ops.table ++ namespace_ops.table ++ kv_ops.table ++ stream_ops.table ++ queue_ops.table ++ ts_ops.table ++ action_ops.table ++ worker_ops.table ++ processing_ops.table;
/// The groups the table's ops form under their families.
pub const op_groups = kv_ops.groups;
pub const entry_table = kv_entries.table ++ namespace_entries.table ++ action_entries.table ++ queue_entries.table ++ group_entries.table;

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

/// Whether table op `code` concerns the server, not a namespace.
pub fn serverScoped(code: u16) bool {
    inline for (op_table) |op| if (op.code == code) return op.scope == .server;
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
pub const HAND_LAYOUT_EPOCH: u32 = 10;

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
        ops.hashEnum(&h, proto.StorageTier);
        golden.hash(&h);
        return h.final();
    }
}

test "table: the hash changes with the hand layout epoch" {
    try std.testing.expect(comptime hash(&op_table, &entry_table, HAND_LAYOUT_EPOCH) != hash(&op_table, &entry_table, HAND_LAYOUT_EPOCH + 1));
    try std.testing.expectEqual(HASH, comptime hash(&op_table, &entry_table, HAND_LAYOUT_EPOCH));
}

test "no stored layout names a shard or group: a record's place comes from its key's slot" {
    // A slot can move between groups, and a group can split, so a stored
    // field naming one goes stale. Scans the files that declare what is
    // logged or kept (entry layouts, the machines' records) for a field
    // named for a shard, group or node. Replica machinery (src/raft) names
    // nodes by design and isn't scanned.
    const stdx = @import("stdx");
    const a = std.testing.allocator;
    // Quantities, not places: a cap on what the group holds.
    const exempt = [_][]const u8{"shard_held_bytes"};
    const words = [_][]const u8{ "shard", "group_id", "group_index", "node_id" };
    var stack: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (stack.items) |p| a.free(p);
        stack.deinit(a);
    }
    try stack.append(a, try a.dupe(u8, "src"));
    var i: usize = 0;
    var files: usize = 0;
    var found: usize = 0;
    while (i < stack.items.len) : (i += 1) {
        const dir_path = stack.items[i];
        var dir = try stdx.fs.openDir(dir_path, .{ .iterate = true });
        defer stdx.fs.closeDir(dir);
        var it = dir.iterate();
        while (try it.next(stdx.io.instance())) |entry| {
            const path = try std.fs.path.join(a, &.{ dir_path, entry.name });
            if (entry.kind == .directory) {
                if (std.mem.eql(u8, path, "src/raft")) {
                    a.free(path);
                    continue;
                }
                try stack.append(a, path);
                continue;
            }
            defer a.free(path);
            const durable = std.mem.endsWith(u8, entry.name, "_entries.zig") or
                std.mem.eql(u8, entry.name, "machine.zig") or
                std.mem.eql(u8, entry.name, "group_machine.zig") or
                std.mem.eql(u8, entry.name, "delivery.zig");
            if (!durable) continue;
            const source = try stdx.fs.readFileAlloc(a, path, 1 << 20);
            defer a.free(source);
            files += 1;
            var lines = std.mem.splitScalar(u8, source, '\n');
            var line_no: usize = 0;
            while (lines.next()) |line| {
                line_no += 1;
                const at = std.mem.indexOf(u8, line, ".name = \"") orelse continue;
                const rest = line[at + ".name = \"".len ..];
                const name = rest[0 .. std.mem.indexOfScalar(u8, rest, '"') orelse continue];
                const named = for (words) |w| {
                    if (std.mem.indexOf(u8, name, w) != null) break true;
                } else false;
                const allowed = for (exempt) |e| {
                    if (std.mem.eql(u8, name, e)) break true;
                } else false;
                if (named and !allowed) {
                    std.debug.print("{s}:{d}: stored field \"{s}\" names a place; derive it from the record's key\n", .{ path, line_no, name });
                    found += 1;
                }
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 0), found);
    try std.testing.expect(files >= 10);
}
