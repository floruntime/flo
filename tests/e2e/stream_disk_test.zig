//! A stream's history past the hot ring and the warm store is read back
//! from its sealed segments, by plain and group reads alike.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");
const proto = src.protocol.proto;
const Client = src.cli_client.Client;
const stream = src.cli_client.stream;

/// About 42 MiB, in appends under the 64 KiB each may carry: past a 2 MiB
/// hot ring and the 32 MiB warm store. Written faster than async durability
/// flushes, so the oldest may still be in the segment writer's buffer.
const APPENDS = 700;
const RECORD_BYTES = 60 * 1024;

/// A read, retried while it is refused retryable: a payload read back from
/// disk can take longer than a group read waits.
fn readUntilServed(conn: *Client, comptime f: anytype, args: anytype) !src.cli_client.Response {
    var tries: usize = 0;
    while (true) : (tries += 1) {
        const r = try @call(.auto, f, .{conn} ++ args);
        if (r.status != .unavailable or tries == 40) return r;
        var done = r;
        done.deinit();
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }
}

/// The id of the last record in a read's answer:
/// [count]([sequence:u64][timestamp_ms:i64][tier:u8][partition:u32]
/// [key_present:u8][payload_len:u32][payload][header_count:u32])*, with no
/// headers in this test.
fn lastId(data: []const u8) !src.cli_client.StreamID {
    const n = try recordCount(data);
    var pos: usize = 4;
    var id: src.cli_client.StreamID = .{ .timestamp_ms = 0, .sequence = 0 };
    for (0..n) |_| {
        id = .{ .sequence = std.mem.readInt(u64, data[pos..][0..8], .little), .timestamp_ms = std.mem.readInt(u64, data[pos + 8 ..][0..8], .little) };
        pos += 8 + 8 + 1 + 4 + 1;
        const len = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4 + len;
        if (std.mem.readInt(u32, data[pos..][0..4], .little) != 0) return error.UnexpectedHeaders;
        pos += 4;
    }
    return id;
}

fn recordCount(data: []const u8) !u32 {
    if (data.len < 4) return error.ShortAnswer;
    return std.mem.readInt(u32, data[0..4], .little);
}

test "e2e/stream-disk: records past the warm store are read back from their segments, by plain and group reads" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .tiered_log = .{ .hot_buffer_capacity = 2 * 1024 * 1024 } } });
    defer ctx.deinit();
    var conn = Client.init(testing.allocator, ctx.endpoint);
    defer conn.deinit();
    try conn.connect();

    // The group before the appends, so it starts at the first.
    {
        var r = try stream.groupCreate(&conn, .{ .namespace = "", .stream = "s", .group = "g" });
        defer r.deinit();
        try testing.expectEqual(proto.StatusCode.ok, r.status);
    }
    const record = try testing.allocator.alloc(u8, RECORD_BYTES);
    defer testing.allocator.free(record);
    var first_seq: ?u64 = null;
    var first_ts: u64 = 0;
    for (0..APPENDS) |i| {
        @memset(record, @as(u8, @intCast('a' + i % 26)));
        const r = try stream.append(&conn, "", "s", &.{record}, null);
        if (first_seq == null) {
            first_seq = r.last_id.sequence;
            first_ts = r.last_id.timestamp_ms;
        }
    }

    // Cold, a group read first: refused retryable while its payloads are
    // read back, with nothing delivered, then the very first record. Had a
    // refused read delivered, the cursor would be past it.
    var group = try readUntilServed(&conn, stream.groupRead, .{ "", "s", "g", "c1", @as(?u32, 5), @as(?u32, null) });
    defer group.deinit();
    try testing.expectEqual(proto.StatusCode.ok, group.status);
    // [term:u64][index:u64][expires_at_ms:i64][count:u32] then the records.
    try testing.expect(try recordCount(group.data[24..]) >= 1);
    try testing.expectEqual(first_seq.?, std.mem.readInt(u64, group.data[28..36], .little));
    try testing.expectEqual(first_ts, std.mem.readInt(u64, group.data[36..44], .little));
    const term = std.mem.readInt(u64, group.data[0..8], .little);
    const index = std.mem.readInt(u64, group.data[8..16], .little);

    // A plain read from the start: the very first record, none skipped,
    // from the durable log. [count] then [sequence:u64][timestamp_ms:i64][tier:u8]...
    var plain = try readUntilServed(&conn, stream.read, .{ "", "s", stream.StartMode.stream_id, null, null, @as(?u32, 5), @as(?u32, null), @as(?u32, null), @as(?[]const u8, null) });
    defer plain.deinit();
    try testing.expectEqual(proto.StatusCode.ok, plain.status);
    // As many whole records as an answer holds: at 60 KiB, one or two.
    try testing.expect(try recordCount(plain.data) >= 1);
    try testing.expectEqual(first_seq.?, std.mem.readInt(u64, plain.data[4..12], .little));
    try testing.expectEqual(first_ts, std.mem.readInt(u64, plain.data[12..20], .little));
    try testing.expectEqual(@backingInt(proto.StorageTier.disk), plain.data[4 + 8 + 8]);
    try testing.expect(std.mem.indexOf(u8, plain.data, "aaaa") != null);

    // Read on past the cache's budget (8 MiB, about 136 records here), so
    // the first ones are evicted: a claim of them is cold too.
    var at = src.cli_client.StreamID{ .timestamp_ms = first_ts, .sequence = first_seq.? };
    var walked: usize = 0;
    while (walked < 200) {
        var r = try readUntilServed(&conn, stream.read, .{ "", "s", stream.StartMode.stream_id, at, null, @as(?u32, 5), @as(?u32, null), @as(?u32, null), @as(?[]const u8, null) });
        defer r.deinit();
        try testing.expectEqual(proto.StatusCode.ok, r.status);
        const n = try recordCount(r.data);
        try testing.expect(n >= 1);
        walked += n;
        at = try lastId(r.data);
    }

    // c1 nacks the first record; c2 reads it again, cold: delivered only
    // once its payload is back.
    {
        const items = [_]stream.GroupItem{.{ .id = .{ .timestamp_ms = first_ts, .sequence = first_seq.? }, .deliveries = 1 }};
        var r = try stream.groupReport(&conn, "", "s", "g", "c1", .nack, term, index, &items, .{});
        defer r.deinit();
        try testing.expectEqual(proto.StatusCode.ok, r.status);
        try testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, r.data[0..4], .little));
    }
    var again = try readUntilServed(&conn, stream.groupRead, .{ "", "s", "g", "c2", @as(?u32, 1), @as(?u32, null) });
    defer again.deinit();
    try testing.expectEqual(proto.StatusCode.ok, again.status);
    try testing.expectEqual(@as(u32, 1), try recordCount(again.data[24..]));
    try testing.expectEqual(first_seq.?, std.mem.readInt(u64, again.data[28..36], .little));
    try testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, again.data[again.data.len - 4 ..][0..4], .little));
}
