//! The queue family's ops on the table. The namespace and key travel in
//! the envelope, so they aren't fields; the key is the queue.

const std = @import("std");
const ops = @import("ops.zig");
const schema = @import("schema.zig");
const qe = @import("queue_entries.zig");

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

/// The most messages one dequeue takes, and one report names.
pub const MAX_BATCH: u32 = qe.MAX_BATCH;
/// The longest a dequeue waits for a message.
pub const MAX_WAIT_MS: u64 = 300_000;
pub const ConsumerId = Bytes(@intCast(ops.MAX_KEY));

/// A dequeued message. With the answer's term and index, its seq and
/// deliveries name its delivery, and a complete, fail or touch names it
/// back; one naming any other delivery of it is refused stale.
pub const Message = Record(&.{
    .{ .name = "seq", .T = Int(u64), .doc = "Its seq, in its queue" },
    .{ .name = "priority", .T = Int(u32), .doc = "Its priority: lower goes first" },
    .{ .name = "deliveries", .T = Int(u32), .doc = "Its deliveries, this one included" },
    .{ .name = "enqueued_at_ms", .T = Int(i64), .doc = "When it was enqueued, in Unix milliseconds" },
    .{ .name = "payload", .T = Bytes(qe.MAX_MESSAGE), .doc = "Its payload" },
});

/// As many as one answer holds: a dequeue stops where the next message
/// wouldn't fit.
const DEQUEUE_FIXED = 8 + 8 + 8;
pub const MESSAGES_BYTES: u32 = @intCast(ops.MAX_RESPONSE_BODY - DEQUEUE_FIXED - @sizeOf(u32));
pub const Messages = schema.BoundedList(Message, MAX_BATCH, MESSAGES_BYTES);

/// A message a report names: its seq and the deliveries it came with.
pub const Item = Record(&.{
    .{ .name = "seq", .T = Int(u64), .doc = "The message's seq" },
    .{ .name = "deliveries", .T = Int(u32), .doc = "Its deliveries, as dequeued" },
});

/// A message a report applied nothing to, and why.
pub const Refused = Record(&.{
    .{ .name = "seq", .T = Int(u64), .doc = "The message's seq" },
    .{ .name = "reason", .T = Bytes(128), .doc = "What became of it: completed, delivered again, expired, or gone" },
});

const report_fields = [_]schema.Field{
    .{ .name = "consumer", .T = Defaulted(ConsumerId, ""), .doc = "The consumer reporting: the one that dequeued them, or the report is refused" },
    .{ .name = "term", .T = Int(u64), .doc = "The dequeue's term" },
    .{ .name = "index", .T = Int(u64), .doc = "The dequeue's index" },
    .{ .name = "messages", .T = schema.List(Item, MAX_BATCH), .doc = "The messages, by seq and deliveries" },
};

const report_answer = [_]schema.Field{
    .{ .name = "applied", .T = Int(u32), .doc = "How many it applied to" },
    .{ .name = "refused", .T = schema.List(Refused, MAX_BATCH), .doc = "The rest, and why" },
};

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
    .{
        .name = "queue.dequeue",
        .code = 0x181,
        .class = .write,
        .takes_items = true,
        .wait = .{ .field = "block_ms", .default_ms = 0, .max_ms = MAX_WAIT_MS },
        .route = .key,
        .request = Record(&.{
            .{ .name = "consumer", .T = Defaulted(ConsumerId, ""), .doc = "Who takes them: a complete, fail or touch must name the same" },
            .{ .name = "count", .T = Defaulted(schema.Range(u32, 1, MAX_BATCH), 1), .doc = "Most messages to take" },
            .{ .name = "lease_ms", .T = schema.Optional(qe.LeaseMs), .doc = "How long they're held without a touch; 30 s when none is named" },
            .{ .name = "block_ms", .T = Defaulted(schema.DurationMs(MAX_WAIT_MS), 0), .doc = "How long to wait for a message; 0 answers at once" },
            .{ .name = "ack", .T = Defaulted(schema.Bool, false), .doc = "Complete them as they're taken, a one-shot consume; if the leader fails in between, they come back once their lease runs out" },
        }),
        .response = Record(&.{
            .{ .name = "messages", .T = Messages, .doc = "The messages taken, or none" },
            .{ .name = "term", .T = Int(u64), .doc = "The delivery's term" },
            .{ .name = "index", .T = Int(u64), .doc = "The delivery's index" },
            .{ .name = "expires_at_ms", .T = Int(i64), .doc = "When the lease runs out without a touch, in Unix milliseconds" },
        }),
        .cli = .{ .expose = .hand, .key = .{ .name = "queue", .doc = "The queue's name" } },
        .doc = "Take a queue's next ready messages, leased until completed, failed or expired.",
    },
    .{
        .name = "queue.complete",
        .code = 0x182,
        .class = .write,
        .route = .key,
        .request = Record(&report_fields),
        .response = Record(&report_answer),
        .cli = .{ .expose = .hand, .key = .{ .name = "queue", .doc = "The queue's name" } },
        .doc = "Complete dequeued messages: they leave the queue.",
    },
    .{
        .name = "queue.fail",
        .code = 0x183,
        .class = .write,
        .route = .key,
        .request = Record(&(report_fields ++ [_]schema.Field{
            .{ .name = "delay_ms", .T = Defaulted(schema.DurationMs(qe.MAX_DELAY_MS), 0), .doc = "How long before they're ready again" },
            .{ .name = "dead", .T = Defaulted(schema.Bool, false), .doc = "Dead-letter them now, whatever attempts are left" },
            .{ .name = "error", .T = Defaulted(Bytes(qe.MAX_ERROR), ""), .doc = "Why they failed" },
        })),
        .response = Record(&report_answer),
        .cli = .{ .expose = .hand, .key = .{ .name = "queue", .doc = "The queue's name" } },
        .doc = "Fail dequeued messages: ready again, or dead once their attempts are spent.",
    },
    .{
        .name = "queue.touch",
        .code = 0x184,
        .class = .write,
        .route = .key,
        .request = Record(&report_fields),
        .response = Record(&([_]schema.Field{
            .{ .name = "expires_at_ms", .T = Int(i64), .doc = "When the lease runs out without another touch, in Unix milliseconds" },
        } ++ report_answer)),
        .cli = .{ .expose = .hand, .key = .{ .name = "queue", .doc = "The queue's name" } },
        .doc = "Keep dequeued messages: with half their lease or less left, it runs a full lease from now, up to 12 h from their dequeue.",
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

test "a dequeue's answer fits one response body, and holds one message of the largest" {
    try std.testing.expect(op("queue.dequeue").response.max_size <= ops.MAX_RESPONSE_BODY);
    try std.testing.expect(Message.max_size <= MESSAGES_BYTES);
}
