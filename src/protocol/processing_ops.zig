//! The processing family's ops on the table. The namespace travels in the
//! envelope, so it isn't a field.

const std = @import("std");
const ops = @import("ops.zig");
const schema = @import("schema.zig");

const Record = schema.Record;

pub const table = [_]ops.Op{
    .{
        .name = "processing.remove",
        .code = 0x365,
        .class = .write,
        .route = .run_id,
        .request = Record(&.{}),
        .response = Record(&.{}),
        .cli = .{ .expose = .hand, .key = .{ .name = "job_id", .doc = "The job's id" }, .examples = &.{"flo processing remove job-123"} },
        .doc = "Remove a stopped, cancelled or failed job, freeing its slot. A running job is refused: stop or cancel it first.",
    },
};

/// The op in this table named `name`; a name not there fails the build.
pub fn op(comptime name: []const u8) ops.Op {
    for (table) |o| if (std.mem.eql(u8, o.name, name)) return o;
    @compileError("no processing op named '" ++ name ++ "'");
}

comptime {
    ops.check(&table);
}
