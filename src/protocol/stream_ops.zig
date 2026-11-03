//! The stream family's ops on the table. The namespace and key travel in
//! the envelope, so they aren't fields; the key is the stream.
//!
//! A group's members, its pending entries and when each member was last
//! delivered to live only in the leader's memory: joins and leaves aren't
//! logged. These ops run on the leader, and a leader change starts them
//! again.

const std = @import("std");
const ops = @import("ops.zig");
const schema = @import("schema.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Optional = schema.Optional;
const Record = schema.Record;

/// The most groups one `stream.group.list` answer holds.
pub const MAX_GROUPS: u32 = 256;
/// The most members one `stream.group.members` answer holds.
pub const MAX_MEMBERS: u32 = 1024;

pub const Name = Bytes(ops.MAX_KEY);

pub const Group = Record(&.{
    .{ .name = "name", .T = Name },
    .{ .name = "members", .T = Int(u32) },
    .{ .name = "pending", .T = Int(u64) },
    .{ .name = "created_at_ms", .T = Int(i64) },
    .{ .name = "ack_timeout_ms", .T = Int(u32) },
    .{ .name = "max_deliver", .T = Int(u8) },
    .{ .name = "last_delivered_ms", .T = Int(u64) },
    .{ .name = "last_delivered_seq", .T = Int(u64) },
});

pub const MemberState = enum(u8) { active = 0, leaving = 1 };

pub const Member = Record(&.{
    .{ .name = "id", .T = Name },
    .{ .name = "state", .T = schema.Enum(MemberState) },
    .{ .name = "pending", .T = Int(u32) },
    .{ .name = "joined_at_ms", .T = Int(i64) },
    .{ .name = "last_active_ms", .T = Optional(Int(i64)) },
});

/// An answer's `more` flag, after its list.
const more_size = schema.Bool.max_size;

pub const Groups = schema.BoundedList(Group, MAX_GROUPS, @intCast(ops.MAX_RESPONSE_BODY - @sizeOf(u32) - more_size));
pub const Members = schema.BoundedList(Member, MAX_MEMBERS, @intCast(ops.MAX_RESPONSE_BODY - @sizeOf(u32) - more_size));

pub const table = [_]ops.Op{
    .{
        .name = "stream.group.list",
        .code = 0x15D,
        .class = .read,
        // On the stream's shard, answered by its leader, which alone holds
        // members and pending entries.
        .at_leader = true,
        .route = .key,
        .request = Record(&.{}),
        .response = Record(&.{
            .{ .name = "groups", .T = Groups, .doc = "The stream's consumer groups, as many as fit" },
            .{ .name = "more", .T = schema.Bool, .doc = "The stream has more groups than the answer holds" },
        }),
        // Generated, under the hand-written `stream group`.
        .cli = .{ .expose = .hand, .key = .{ .name = "stream", .doc = "The stream's name" }, .render = .record, .examples = &.{"flo stream group list orders"} },
        .doc = "A stream's consumer groups: members, pending entries, settings and the last record delivered. Members and pending are the leader's, and a leader change starts them again.",
    },
    .{
        .name = "stream.group.members",
        .code = 0x15E,
        .class = .read,
        .at_leader = true,
        .route = .key,
        .request = Record(&.{
            .{ .name = "group", .T = Name, .doc = "The group" },
        }),
        .response = Record(&.{
            .{ .name = "members", .T = Members, .doc = "The group's members, as many as fit" },
            .{ .name = "more", .T = schema.Bool, .doc = "The group has more members than the answer holds" },
        }),
        .cli = .{ .expose = .hand, .key = .{ .name = "stream", .doc = "The stream's name" }, .args = &.{"group"}, .render = .record, .examples = &.{"flo stream group members orders billing"} },
        .doc = "A consumer group's members: each one's state, its pending entries, when it joined and when it was last delivered to. All of it is the leader's, and a leader change starts it again.",
    },
};

/// The op in this table named `name`; a name not there fails the build.
pub fn op(comptime name: []const u8) ops.Op {
    for (table) |o| if (std.mem.eql(u8, o.name, name)) return o;
    @compileError("no stream op named '" ++ name ++ "'");
}

comptime {
    ops.check(&table);
}

test "the group answers fit one response body" {
    try std.testing.expect(op("stream.group.list").response.max_size <= ops.MAX_RESPONSE_BODY);
    try std.testing.expect(op("stream.group.members").response.max_size <= ops.MAX_RESPONSE_BODY);
}
