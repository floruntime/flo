//! The namespace family's log entry: a create, logged on shard 0, the
//! namespaces' one owner, naming the id it gives the namespace. Other
//! shards' entries carry only the id; they read the name from the node's
//! namespace table, which shard 0 publishes.

const std = @import("std");
const schema = @import("schema.zig");
const entries = @import("entries.zig");
const namespace_ops = @import("namespace_ops.zig");

const Record = schema.Record;

pub const Id = schema.Int(u32);

pub const Create = Record(&.{
    .{ .name = "name", .T = namespace_ops.Name },
    .{ .name = "id", .T = Id },
});

pub const table = [_]entries.Entry{
    .{ .name = "namespace.create", .type_code = 0x60, .payload = Create, .max_payload = Create.max_size, .doc = "A namespace created, with its id." },
};

/// The payload schema of entry type `code`.
pub fn Payload(comptime code: u8) type {
    for (table) |e| if (e.type_code == code) return e.payload;
    @compileError(std.fmt.comptimePrint("no namespace entry with type 0x{x}", .{code}));
}

comptime {
    entries.check(&table);
}
