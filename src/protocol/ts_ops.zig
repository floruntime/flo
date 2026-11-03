//! The time-series family's ops on the table. The namespace and key travel
//! in the envelope, so they aren't fields; the key is the measurement.

const std = @import("std");
const ops = @import("ops.zig");
const schema = @import("schema.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Optional = schema.Optional;
const Record = schema.Record;

/// The most field names `ts.info` lists; past it, `fields_capped` is set.
pub const MAX_INFO_FIELDS: u32 = 256;
/// The bytes those names may take, so the answer fits one response body.
pub const MAX_INFO_FIELD_BYTES: u32 = 128 * 1024;

pub const FieldName = Bytes(std.math.maxInt(u16));
pub const FieldNames = schema.BoundedList(FieldName, MAX_INFO_FIELDS, MAX_INFO_FIELD_BYTES);

pub const table = [_]ops.Op{
    .{
        .name = "ts.info",
        .code = 0x1A7,
        .class = .read,
        // On the measurement's shard, where its series are.
        .route = .key,
        .request = Record(&.{}),
        .response = Record(&.{
            .{ .name = "series", .T = Int(u32), .doc = "Its series: each field of each tag set" },
            .{ .name = "field_count", .T = Int(u32), .doc = "Its distinct fields, counted up to 256" },
            .{ .name = "fields_capped", .T = schema.Bool, .doc = "It has more fields than were counted or listed" },
            .{ .name = "fields", .T = FieldNames, .doc = "Its fields' names, up to 256 and 128 KiB of them" },
            .{ .name = "points", .T = Int(u64), .doc = "Its points, flushed and buffered" },
            .{ .name = "oldest_ms", .T = Optional(Int(i64)), .doc = "Its oldest point's time, in Unix milliseconds; none without points" },
            .{ .name = "newest_ms", .T = Optional(Int(i64)), .doc = "Its newest point's time" },
        }),
        .cli = .{ .expose = .command, .key = .{ .name = "measurement", .doc = "The measurement's name" }, .render = .record, .examples = &.{"flo ts info cpu"} },
        .doc = "A measurement's series, fields, points and the times of its oldest and newest points. A measurement with no series is not found.",
    },
};

/// The op in this table named `name`; a name not there fails the build.
pub fn op(comptime name: []const u8) ops.Op {
    for (table) |o| if (std.mem.eql(u8, o.name, name)) return o;
    @compileError("no ts op named '" ++ name ++ "'");
}

comptime {
    ops.check(&table);
}

test "ts.info's largest answer fits one response body" {
    try std.testing.expect(op("ts.info").response.max_size <= ops.MAX_RESPONSE_BODY);
}
