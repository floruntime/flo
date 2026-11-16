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

/// The most points one `ts.read` page holds.
pub const MAX_READ: u32 = 4096;
/// A tag filter's longest text.
pub const MAX_TAG_FILTER: u32 = 1024;
/// Which way a read goes through the points.
pub const Order = enum(u8) { oldest_first, newest_first };
/// A read's cursor: the last point of the page before, its time and its
/// position (log index, then place in its entry), each u64 big-endian.
pub const CURSOR_LEN: u32 = 16;
const Cursor = Bytes(CURSOR_LEN);
const Time = schema.Range(i64, 0, std.math.maxInt(i64));

pub const Point = Record(&.{
    .{ .name = "time_ns", .T = Int(i64), .doc = "Its time, in Unix nanoseconds" },
    .{ .name = "value", .T = schema.Float(true), .doc = "Its value" },
});
pub const Points = schema.BoundedList(Point, MAX_READ, @intCast(ops.MAX_RESPONSE_BODY - @sizeOf(u32) - Optional(Cursor).max_size));

pub const table = [_]ops.Op{
    .{
        .name = "ts.read",
        .code = 0x1A1,
        .class = .read,
        .route = .key,
        .request = Record(&.{
            .{ .name = "field", .T = schema.Defaulted(FieldName, "value"), .doc = "The field to read" },
            .{ .name = "tags", .T = schema.Defaulted(Bytes(MAX_TAG_FILTER), ""), .doc = "Only series whose tags include every key=value pair of this comma-separated list" },
            .{ .name = "from_ns", .T = Optional(Time), .doc = "Its earliest time, inclusive, in Unix nanoseconds; none for the first point" },
            .{ .name = "to_ns", .T = Optional(Time), .doc = "Its latest time, inclusive; none for the last point" },
            .{ .name = "limit", .T = schema.Defaulted(schema.Range(u32, 1, MAX_READ), MAX_READ), .doc = "Most points on the page" },
            .{ .name = "order", .T = schema.Defaulted(schema.Enum(Order), .oldest_first), .doc = "oldest_first, or newest_first" },
            .{ .name = "cursor", .T = schema.Defaulted(Cursor, ""), .doc = "Where the last page stopped: the cursor it answered; empty to start" },
        }),
        .response = Record(&.{
            .{ .name = "points", .T = Points, .doc = "Points of every matching series, merged in time order (then write order), or newest first" },
            .{ .name = "cursor", .T = Optional(Cursor), .doc = "Where the next page starts; none on the last page" },
        }),
        .cli = .{ .expose = .hand },
        .doc = "A measurement field's points, a page at a time, oldest or newest first.",
    },
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
            .{ .name = "points", .T = Int(u64), .doc = "Its points" },
            .{ .name = "oldest_ns", .T = Optional(Int(i64)), .doc = "Its oldest point's time, in Unix nanoseconds; none without points" },
            .{ .name = "newest_ns", .T = Optional(Int(i64)), .doc = "Its newest point's time" },
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

test "ts.info's and ts.read's largest answers fit one response body" {
    try std.testing.expect(op("ts.info").response.max_size <= ops.MAX_RESPONSE_BODY);
    try std.testing.expect(op("ts.read").response.max_size <= ops.MAX_RESPONSE_BODY);
}
