//! The queue family's ops on the table. The namespace and key travel in
//! the envelope, so they aren't fields; the key is the queue.

const std = @import("std");
const ops = @import("ops.zig");
const schema = @import("schema.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;
const Defaulted = schema.Defaulted;

/// The most messages one peek asks for.
pub const MAX_PEEK: u32 = 100;
/// The most of a message's payload a peek shows; `size` is its whole length.
pub const MAX_PEEK_PAYLOAD: u32 = 4096;

pub const Peeked = Record(&.{
    .{ .name = "seq", .T = Int(u64) },
    .{ .name = "priority", .T = Int(u32) },
    .{ .name = "attempts", .T = Int(u32) },
    .{ .name = "enqueued_at_ms", .T = Int(i64) },
    .{ .name = "size", .T = Int(u32) },
    .{ .name = "payload", .T = Bytes(MAX_PEEK_PAYLOAD) },
});

const more_size = schema.Bool.max_size;

/// As many as one answer holds: a full page of the largest payloads
/// doesn't fit, so a page stops where the next one wouldn't.
pub const PeekedList = schema.BoundedList(Peeked, MAX_PEEK, @intCast(ops.MAX_RESPONSE_BODY - @sizeOf(u32) - more_size));

pub const table = [_]ops.Op{
    .{
        .name = "queue.peek",
        .code = 0x180,
        .class = .read,
        // On the queue's shard, answered by its leader, which holds its leases.
        .at_leader = true,
        .route = .key,
        .request = Record(&.{
            .{ .name = "limit", .T = Defaulted(schema.Range(u32, 1, MAX_PEEK), 20), .doc = "Most messages to show" },
        }),
        .response = Record(&.{
            .{ .name = "messages", .T = PeekedList, .doc = "The next ready messages, in the order a dequeue takes them" },
            .{ .name = "more", .T = schema.Bool, .doc = "More may follow: the limit, the answer's room or a run of taken messages stopped it" },
        }),
        .cli = .{ .expose = .command, .key = .{ .name = "queue", .doc = "The queue's name" }, .render = .record, .examples = &.{"flo queue peek orders --limit 5"} },
        .doc = "The next ready messages of a queue, in dequeue order, without taking them. Each shows up to 4 KiB of its payload.",
    },
};

/// The op in this table named `name`; a name not there fails the build.
pub fn op(comptime name: []const u8) ops.Op {
    for (table) |o| if (std.mem.eql(u8, o.name, name)) return o;
    @compileError("no queue op named '" ++ name ++ "'");
}

comptime {
    ops.check(&table);
}

test "a peek's answer fits one response body, though a full page of the largest payloads doesn't" {
    try std.testing.expect(op("queue.peek").response.max_size <= ops.MAX_RESPONSE_BODY);
    try std.testing.expect(MAX_PEEK * Peeked.max_size > PeekedList.byte_bound.?);
}
