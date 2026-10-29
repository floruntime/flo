//! The namespace family's log entries. A create is logged on shard 0, the
//! namespace's one owner, and names the id it gives the namespace. A bind
//! is logged on any other shard before its first write in that namespace,
//! so the shard's own log says which id a name is: replaying it never
//! needs shard 0.

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

pub const Bind = Record(&.{
    .{ .name = "id", .T = Id },
    .{ .name = "name", .T = namespace_ops.Name },
});

pub const table = [_]entries.Entry{
    .{ .name = "namespace.create", .type_code = 0x60, .payload = Create, .max_payload = Create.max_size, .doc = "A namespace created, with its id." },
    .{ .name = "namespace.bind", .type_code = 0x63, .payload = Bind, .max_payload = Bind.max_size, .doc = "A shard's record that a name is that id, before the shard's first write in it." },
};

/// The payload schema of entry type `code`.
pub fn Payload(comptime code: u8) type {
    for (table) |e| if (e.type_code == code) return e.payload;
    @compileError(std.fmt.comptimePrint("no namespace entry with type 0x{x}", .{code}));
}

comptime {
    entries.check(&table);
}
