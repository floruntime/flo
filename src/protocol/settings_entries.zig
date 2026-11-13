//! The cluster-settings log entries: the memory-budget pool share and each
//! node's reported RAM and reserve. They are durable, replayed records, so
//! their layouts are pinned here against the data format — a later change to
//! one is a format change, caught by the entry-table hash.

const std = @import("std");
const schema = @import("schema.zig");
const entries = @import("entries.zig");
const limits = @import("limits.zig");

const Int = schema.Int;
const Record = schema.Record;

/// `settings_pool_share`: the share of each node's RAM-after-reserve the pool
/// may use, in per-mille (checked ≤ 1000 at apply).
pub const Share = Record(&.{
    .{ .name = "share_permille", .T = Int(u16) },
});

/// `settings_node_mem`: a node's id and its figures.
pub const NodeMem = Record(&.{
    .{ .name = "node_id", .T = Int(u32) },
    .{ .name = "ram_bytes", .T = Int(u64) },
    .{ .name = "reserve", .T = Int(u64) },
});

/// `settings_node_del`: the node whose row to drop.
pub const DelNode = Record(&.{
    .{ .name = "node_id", .T = Int(u32) },
});

pub const table = [_]entries.Entry{
    .{ .name = "settings.pool_share", .type_code = 0x90, .payload = Share, .max_payload = Share.max_size, .doc = "The memory-budget pool share, per-mille of each node's RAM after its reserve." },
    .{ .name = "settings.node_mem", .type_code = 0x91, .payload = NodeMem, .max_payload = NodeMem.max_size, .doc = "A node's reported RAM and reserve for the memory budget." },
    .{ .name = "settings.node_del", .type_code = 0x92, .payload = DelNode, .max_payload = DelNode.max_size, .doc = "A node's memory row removed when it leaves the membership." },
};

/// The payload schema of entry type `code`.
pub fn Payload(comptime code: u8) type {
    for (table) |e| if (e.type_code == code) return e.payload;
    @compileError(std.fmt.comptimePrint("no settings entry with type 0x{x}", .{code}));
}

comptime {
    entries.check(&table);
    for (table) |e| std.debug.assert(e.max_payload <= limits.MAX_LOG_ENTRY_PAYLOAD);
}
