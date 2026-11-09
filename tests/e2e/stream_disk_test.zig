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
    {
        var r = try stream.groupJoin(&conn, "", "s", "g", "c1");
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

    // From the start: the first append's record, read from disk.
    var plain = try readUntilServed(&conn, stream.read, .{ "", "s", stream.StartMode.stream_id, null, null, @as(?u32, 5), @as(?u32, null), @as(?u32, null), @as(?[]const u8, null) });
    defer plain.deinit();
    try testing.expectEqual(proto.StatusCode.ok, plain.status);
    // As many whole records as an answer holds: at 60 KiB, one or two.
    try testing.expect(try recordCount(plain.data) >= 1);
    // [count] then [sequence:u64][timestamp_ms:i64][tier:u8]...: the very
    // first record, none skipped, from the durable log.
    try testing.expectEqual(first_seq.?, std.mem.readInt(u64, plain.data[4..12], .little));
    try testing.expectEqual(first_ts, std.mem.readInt(u64, plain.data[12..20], .little));
    try testing.expectEqual(@backingInt(proto.StorageTier.disk), plain.data[4 + 8 + 8]);
    try testing.expect(std.mem.indexOf(u8, plain.data, "aaaa") != null);

    // A group read gets them too, without skipping a delivery.
    var group = try readUntilServed(&conn, stream.groupRead, .{ "", "s", "g", "c1", @as(?u32, 5), @as(?u32, null) });
    defer group.deinit();
    try testing.expectEqual(proto.StatusCode.ok, group.status);
    try testing.expect(try recordCount(group.data) >= 1);
    try testing.expect(std.mem.indexOf(u8, group.data, "aaaa") != null);
}
