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

/// A namespace's run-history quota set: the default, none, or `bytes` over
/// every shard. The built-in "default" namespace may be named.
pub const Alter = Record(&.{
    .{ .name = "name", .T = namespace_ops.Name },
    .{ .name = "mode", .T = schema.Enum(namespace_ops.QuotaMode) },
    .{ .name = "bytes", .T = schema.Int(u64) },
});

/// A quota of none, as the log and the store hold it; the wire says none
/// by omitting the figure.
pub const NO_QUOTA: u64 = std.math.maxInt(u64);

pub const table = [_]entries.Entry{
    .{ .name = "namespace.create", .type_code = 0x60, .payload = Create, .max_payload = Create.max_size, .doc = "A namespace created, with its id." },
    .{ .name = "namespace.alter", .type_code = 0x64, .payload = Alter, .max_payload = Alter.max_size, .doc = "A namespace's run-history quota set." },
};

/// The payload schema of entry type `code`.
pub fn Payload(comptime code: u8) type {
    for (table) |e| if (e.type_code == code) return e.payload;
    @compileError(std.fmt.comptimePrint("no namespace entry with type 0x{x}", .{code}));
}

comptime {
    entries.check(&table);
}
