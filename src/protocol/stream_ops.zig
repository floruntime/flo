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

const ge = @import("group_entries.zig");
const queue_ops = @import("queue_ops.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Optional = schema.Optional;
const Record = schema.Record;
const Defaulted = schema.Defaulted;

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

/// A record a steal names: its id and the deliveries its holder has.
pub const Leased = Record(&.{
    .{ .name = "ms", .T = Int(u64), .doc = "The record's id: its ms" },
    .{ .name = "seq", .T = Int(u64), .doc = "The record's id: its seq" },
    .{ .name = "deliveries", .T = Int(u32), .doc = "Its deliveries, as its holder has them" },
});

/// A record by id.
pub const RecordRef = Record(&.{
    .{ .name = "ms", .T = Int(u64), .doc = "The record's id: its ms" },
    .{ .name = "seq", .T = Int(u64), .doc = "The record's id: its seq" },
});

/// A record an operator command applied nothing to, and why.
pub const Refused = Record(&.{
    .{ .name = "ms", .T = Int(u64), .doc = "The record's id: its ms" },
    .{ .name = "seq", .T = Int(u64), .doc = "The record's id: its seq" },
    .{ .name = "why", .T = schema.Enum(queue_ops.Why), .doc = "Why: gone, delivered_again, expired, released, not_held, not_idle (a steal) or not_dead (a requeue or delete)" },
    .{ .name = "reason", .T = Bytes(128), .doc = "The same, said for a person" },
});

/// The most dead letters one list answers, and of each one's error.
pub const MAX_DLQ_PAGE: u32 = 100;
pub const MAX_DLQ_ERROR: u32 = 256;

pub const DeadRecord = Record(&.{
    .{ .name = "ms", .T = Int(u64), .doc = "The record's id: its ms" },
    .{ .name = "seq", .T = Int(u64), .doc = "The record's id: its seq" },
    .{ .name = "deliveries", .T = Int(u32), .doc = "Times it was delivered" },
    .{ .name = "failures", .T = Int(u32), .doc = "Times it failed: nacked or its lease ran out" },
    .{ .name = "dead_at_ms", .T = Int(i64), .doc = "When it died, in Unix milliseconds" },
    .{ .name = "error", .T = Bytes(MAX_DLQ_ERROR), .doc = "Its last error, its first 256 bytes" },
});

/// Where a dead-letter list goes on: past this dead letter, in the order
/// they died.
pub const DeadCursor = Record(&.{
    .{ .name = "dead_ns", .T = Int(u64), .doc = "When the last one listed died, in ns" },
    .{ .name = "ms", .T = Int(u64), .doc = "Its id: its ms" },
    .{ .name = "seq", .T = Int(u64), .doc = "Its id: its seq" },
});

pub const DeadPage = schema.BoundedList(DeadRecord, MAX_DLQ_PAGE, @intCast(ops.MAX_RESPONSE_BODY - @sizeOf(u32) - Optional(DeadCursor).max_size));

const dead_request = [_]schema.Field{
    .{ .name = "group", .T = Name, .doc = "The group" },
    .{ .name = "records", .T = Optional(schema.List(RecordRef, ge.MAX_BATCH)), .doc = "The dead letters, by id; none with all" },
    .{ .name = "all", .T = Defaulted(schema.Bool, false), .doc = "The oldest dead letters instead, a batch of up to `limit`: `more` says whether others remain" },
    .{ .name = "limit", .T = Defaulted(schema.Range(u32, 1, ge.MAX_BATCH), ge.MAX_BATCH), .doc = "With all, the most this batch takes" },
};

const dead_answer = [_]schema.Field{
    .{ .name = "applied", .T = Int(u32), .doc = "How many it applied to" },
    .{ .name = "refused", .T = schema.List(Refused, ge.MAX_BATCH), .doc = "The rest, and why" },
    .{ .name = "more", .T = schema.Bool, .doc = "With all: dead letters remain past this batch" },
};
pub const Members = schema.BoundedList(Member, MAX_MEMBERS, @intCast(ops.MAX_RESPONSE_BODY - @sizeOf(u32) - more_size));

pub const table = [_]ops.Op{
    .{
        .name = "stream.group.list",
        .code = 0x15D,
        .class = .read,
        // On the stream's shard, answered by its leader, which has applied
        // every lease it granted.
        .at_leader = true,
        .route = .key,
        .request = Record(&.{}),
        .response = Record(&.{
            .{ .name = "groups", .T = Groups, .doc = "The stream's consumer groups, as many as fit" },
            .{ .name = "more", .T = schema.Bool, .doc = "The stream has more groups than the answer holds" },
        }),
        // Generated, under the hand-written `stream group`.
        .cli = .{ .expose = .hand, .key = .{ .name = "stream", .doc = "The stream's name" }, .render = .record, .examples = &.{"flo stream group list orders"} },
        .doc = "A stream's consumer groups: each one's members (the consumers holding its records now), its records out, its settings and the last record its cursor took. All of it is logged, so a leader change keeps it.",
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
        .doc = "A consumer group's members: the consumers holding its records now, each with how many it holds, when it first took one it still holds, and when it last took one. All of it is logged, so a leader change keeps it.",
    },
    .{
        .name = "stream.group.steal",
        .code = 0x15F,
        .class = .write,
        .route = .key,
        .request = Record(&.{
            .{ .name = "group", .T = Name, .doc = "The group" },
            .{ .name = "consumer", .T = ge.Holder, .doc = "Who takes them" },
            .{ .name = "min_idle_ms", .T = schema.Range(u64, 1, ge.MAX_LEASE_MS), .doc = "Take only records idle this long: idle is the time since the holder last touched it (its read, a steal, or any touch, even one that leaves the lease as it stands)" },
            .{ .name = "lease_ms", .T = Optional(ge.LeaseMs), .doc = "The lease they're taken for; the group's lease when none is named" },
            .{ .name = "records", .T = schema.List(Leased, ge.MAX_BATCH), .doc = "The records, by id and the deliveries their holder has" },
        }),
        .response = Record(&.{
            .{ .name = "term", .T = Int(u64), .doc = "The new delivery's term" },
            .{ .name = "index", .T = Int(u64), .doc = "The new delivery's index" },
            .{ .name = "expires_at_ms", .T = Int(i64), .doc = "When the new lease runs out without a touch, in Unix milliseconds; 0 when none was taken" },
            .{ .name = "stolen", .T = schema.List(Leased, ge.MAX_BATCH), .doc = "The records taken, with their new deliveries" },
            .{ .name = "refused", .T = schema.List(Refused, ge.MAX_BATCH), .doc = "The rest, and why" },
        }),
        .cli = .{ .expose = .hand, .key = .{ .name = "stream", .doc = "The stream's name" } },
        .doc = "Take records another consumer holds and has left idle: each gets a new delivery and lease, its failures kept. Idle is the time since its holder last touched it (its read, a steal, or any touch, even one that leaves the lease as it stands), so a holder that touches more often than the idle time named is never stolen from. An operator's command: a read takes expired records by itself.",
    },
    .{
        .name = "stream.group.dlq_list",
        .code = 0x160,
        .class = .read,
        .at_leader = true,
        .route = .key,
        .request = Record(&.{
            .{ .name = "group", .T = Name, .doc = "The group" },
            .{ .name = "limit", .T = Defaulted(schema.Range(u32, 1, MAX_DLQ_PAGE), 20), .doc = "Most dead letters to show" },
            .{ .name = "after", .T = Optional(DeadCursor), .doc = "Go on past this one, as the last answer's cursor named it" },
        }),
        .response = Record(&.{
            .{ .name = "dead", .T = DeadPage, .doc = "The group's dead letters, oldest dead first" },
            .{ .name = "cursor", .T = Optional(DeadCursor), .doc = "Where the next page starts; none on the last page" },
        }),
        .cli = .{ .expose = .hand, .key = .{ .name = "stream", .doc = "The stream's name" } },
        .doc = "A consumer group's dead letters, oldest dead first, a page at a time, each with its deliveries, failures, when it died and its last error.",
    },
    .{
        .name = "stream.group.dlq_requeue",
        .code = 0x161,
        .class = .write,
        .route = .key,
        .request = Record(&dead_request),
        .response = Record(&dead_answer),
        .cli = .{ .expose = .hand, .key = .{ .name = "stream", .doc = "The stream's name" } },
        .doc = "Make dead letters ready again at once, with their attempts reset: their failures go to 0, their deliveries and last error stay. One trimmed from the stream since is gone.",
    },
    .{
        .name = "stream.group.dlq_delete",
        .code = 0x162,
        .class = .write,
        .route = .key,
        .request = Record(&(dead_request ++ [_]schema.Field{
            .{ .name = "dry_run", .T = Defaulted(schema.Bool, false), .doc = "Say what it would delete, and delete nothing" },
        })),
        .response = Record(&dead_answer),
        .cli = .{ .expose = .hand, .key = .{ .name = "stream", .doc = "The stream's name" } },
        .doc = "Delete dead letters for good. With dry_run, `applied` is how many it would delete, and nothing is logged.",
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
    inline for (.{ "stream.group.steal", "stream.group.dlq_list", "stream.group.dlq_requeue", "stream.group.dlq_delete" }) |name| try std.testing.expect(op(name).response.max_size <= ops.MAX_RESPONSE_BODY);
}
