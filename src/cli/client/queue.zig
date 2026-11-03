//! Queue Client Operations
//!
//! Queue operations for the Flo CLI client.
//! All functions take a *Client and namespace as parameters.

const std = @import("std");
const base = @import("base.zig");
const wire = @import("../../util/wire.zig");
const WireReader = wire.WireReader;
const Client = base.Client;
const Response = base.Response;
const proto = @import("../../protocol/proto.zig");
const FixedWireWriter = wire.FixedWireWriter;

/// Enqueue a message to a queue
pub fn enqueue(
    client: *Client,
    namespace: []const u8,
    queue: []const u8,
    payload: []const u8,
    priority: u8,
) !Response {
    var options_buf: [8]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);
    try builder.addU8(.priority, priority);

    return client.sendRequestWithOptions(.queue_enqueue, namespace, queue, payload, builder.getOptions());
}

/// Purge all live (ready + leased) messages from a queue. The response's raw
/// data is a u32 LE count of removed messages.
pub fn purge(client: *Client, namespace: []const u8, queue: []const u8) !Response {
    return client.sendRequest(.queue_purge, namespace, queue, "");
}

/// Dequeue messages from a queue
/// block_ms: null or 0 = no blocking, >0 = block for N ms (the server refuses more than 5 minutes)
pub fn dequeue(client: *Client, namespace: []const u8, queue: []const u8, count: u32, block_ms: ?u32) !Response {
    var options_buf: [48]u8 = undefined;
    var builder = proto.OptionsBuilder.init(&options_buf);

    try builder.addU32(.count, count);

    // Only add block_ms option if blocking is requested
    if (block_ms) |ms| {
        try builder.addU32(.block_ms, ms);
        // Read for as long as the server waits, plus 5 s; 0 does not wait.
        if (ms > 0) client.setReadTimeoutSec(ms / 1000 + 5);
    }

    return client.sendRequestWithOptions(.queue_dequeue, namespace, queue, "", builder.getOptions());
}

/// Acknowledge message processing (complete)
pub fn ack(client: *Client, namespace: []const u8, queue: []const u8, seqs: []const u64) !Response {
    // Format: [count:u32][seq:u64]*
    var writer = FixedWireWriter(4096).init();
    try writer.writeU64ArrayWithCount(seqs);

    return client.sendRequest(.queue_complete, namespace, queue, writer.bytes());
}

/// Negative acknowledge (return to queue or send to DLQ)
pub fn nack(client: *Client, namespace: []const u8, queue: []const u8, seqs: []const u64) !Response {
    // Format: [count:u32][seq:u64]*
    var writer = FixedWireWriter(4096).init();
    try writer.writeU64ArrayWithCount(seqs);

    return client.sendRequest(.queue_fail, namespace, queue, writer.bytes());
}

/// A queue's dead-letter count; listing the messages themselves isn't
/// supported yet.
/// A page of up to `limit` dead letters, after the one whose seq is
/// `after` (the last of the page before).
pub fn dlqList(client: *Client, namespace: []const u8, queue: []const u8, limit: u32, after: ?u64) !Response {
    var value: [12]u8 = undefined;
    std.mem.writeInt(u32, value[0..4], limit, .little);
    if (after) |seq| std.mem.writeInt(u64, value[4..12], seq, .little);
    return client.sendRequest(.queue_dlq_list, namespace, queue, value[0..if (after != null) 12 else 4]);
}

/// Requeue messages from DLQ
pub fn dlqRequeue(client: *Client, namespace: []const u8, queue: []const u8, seqs: []const u64) !Response {
    // Format: [count:u32][seq:u64]*
    var writer = FixedWireWriter(4096).init();
    try writer.writeU64ArrayWithCount(seqs);

    return client.sendRequest(.queue_dlq_requeue, namespace, queue, writer.bytes());
}

/// List queues in a namespace
/// Returns pre-serialized wire format:
/// [count:u32] ([name_len:u32][name][ns_len:u32][ns][pending:u64][available:u64][enqueued:u64][dequeued:u64][dlq:u64])* [has_more:u8] [cursor_len:u16][cursor]
pub fn list(client: *Client, namespace: []const u8, limit: ?u32, cursor: ?[]const u8) !Response {
    var value_buf: [base.WALK_VALUE_MAX]u8 = undefined;
    return client.sendRequest(.queue_list, namespace, "", try base.walkValue(&value_buf, limit, cursor));
}

/// A `queue_dlq_list` page:
///   [count:u32] ([seq:u64][size:u32][len:u32][payload][enqueued_at_ms:i64]
///                [attempts:u32][priority:u8][moved_at_ms:i64])*
///   [total:u64][has_more:u8][last_seq:u64]
pub const DlqPage = struct {
    count: u32,
    entries: []const u8,
    total: u64,
    more: bool,
    last_seq: u64,

    pub const Entry = struct {
        seq: u64,
        /// The payload's whole length; `payload` is at most 4 KiB of it.
        size: u32,
        payload: []const u8,
        enqueued_at_ms: i64,
        attempts: u32,
        priority: u8,
        moved_at_ms: i64,
    };

    /// The page, or null when it isn't one whole.
    pub fn decode(data: []const u8) ?DlqPage {
        var r = WireReader.init(data);
        const count = r.readU32() orelse return null;
        const start = r.pos;
        for (0..count) |_| _ = readEntry(&r) orelse return null;
        const entries = data[start..r.pos];
        const total = r.readU64() orelse return null;
        const more = (r.readU8() orelse return null) != 0;
        const last = r.readU64() orelse return null;
        if (r.remainingLen() != 0) return null;
        return .{ .count = count, .entries = entries, .total = total, .more = more, .last_seq = last };
    }

    fn readEntry(r: *WireReader) ?Entry {
        const seq = r.readU64() orelse return null;
        const size = r.readU32() orelse return null;
        return .{
            .seq = seq,
            .size = size,
            .payload = r.readLengthPrefixed(u32) orelse return null,
            .enqueued_at_ms = r.readI64() orelse return null,
            .attempts = r.readU32() orelse return null,
            .priority = r.readU8() orelse return null,
            .moved_at_ms = r.readI64() orelse return null,
        };
    }

    pub const Iterator = struct {
        r: WireReader,

        pub fn next(it: *Iterator) ?Entry {
            if (it.r.remainingLen() == 0) return null;
            return readEntry(&it.r);
        }
    };

    pub fn iterator(self: DlqPage) Iterator {
        return .{ .r = WireReader.init(self.entries) };
    }
};

test "a dead-letter page decodes entry by entry, and a cut one is refused" {
    const le = std.mem.toBytes;
    const page = le(@as(u32, 1)) ++ le(@as(u64, 7)) ++ le(@as(u32, 5000)) ++ le(@as(u32, 2)) ++ "hi".* ++
        le(@as(i64, 10)) ++ le(@as(u32, 3)) ++ [_]u8{1} ++ le(@as(i64, 20)) ++ le(@as(u64, 9)) ++ [_]u8{1} ++ le(@as(u64, 7));
    const p = DlqPage.decode(&page).?;
    try std.testing.expectEqual(@as(u64, 9), p.total);
    try std.testing.expect(p.more);
    try std.testing.expectEqual(@as(u64, 7), p.last_seq);
    var it = p.iterator();
    const e = it.next().?;
    try std.testing.expectEqualStrings("hi", e.payload);
    try std.testing.expectEqual(@as(u32, 5000), e.size);
    try std.testing.expectEqual(@as(u32, 3), e.attempts);
    try std.testing.expect(it.next() == null);
    for (0..page.len) |n| try std.testing.expect(DlqPage.decode(page[0..n]) == null);
}
