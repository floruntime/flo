//! Every list op pages: a request's value is `[limit:u32][cursor]`, and the
//! answer ends `[has_more:u8][cursor_len:u16][cursor]`. Each family is paged
//! two at a time and must return every name exactly once.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;

/// How one entry of a family's list answer is laid out; returns the name
/// and advances `pos` past the entry.
const Entry = *const fn (data: []const u8, pos: *usize) []const u8;

fn nameU16(d: []const u8, pos: *usize) []const u8 {
    const n = std.mem.readInt(u16, d[pos.*..][0..2], .little);
    const name = d[pos.* + 2 ..][0..n];
    pos.* += 2 + n;
    return name;
}

/// [key_len:u16][key][value_len:u32]
fn scanEntry(d: []const u8, pos: *usize) []const u8 {
    const name = nameU16(d, pos);
    pos.* += 4;
    return name;
}

/// [name_len:u32][name][partition_count:u32]
fn streamEntry(d: []const u8, pos: *usize) []const u8 {
    const n = std.mem.readInt(u32, d[pos.*..][0..4], .little);
    const name = d[pos.* + 4 ..][0..n];
    pos.* += 4 + n + 4;
    return name;
}

/// [name_len:u32][name][ns_len:u32][ns][pending,available,enqueued,dequeued,dlq:u64]
fn queueEntry(d: []const u8, pos: *usize) []const u8 {
    const n = std.mem.readInt(u32, d[pos.*..][0..4], .little);
    const name = d[pos.* + 4 ..][0..n];
    pos.* += 4 + n;
    const ns = std.mem.readInt(u32, d[pos.*..][0..4], .little);
    pos.* += 4 + ns + 5 * 8;
    return name;
}

fn listPage(ctx: *stdx.testing.TestContext, op: proto.OpCode, cursor: []const u8, out: []u8) ![]const u8 {
    var value: [4 + 256]u8 = undefined;
    std.mem.writeInt(u32, value[0..4], 2, .little);
    @memcpy(value[4..][0..cursor.len], cursor);
    var h: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&h), 0);
    h.magic = proto.MAGIC;
    h.version = proto.VERSION;
    h.op_code = @intFromEnum(op);
    h.request_id = 1;
    const req: proto.Request = .{ .header = h, .namespace = "default", .key = "", .value = value[0 .. 4 + cursor.len], .options = "" };
    var frame_buf: [1024]u8 = undefined;
    const frame = try req.serialize(&frame_buf);

    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    if (std.c.write(fd, frame.ptr, frame.len) != @as(isize, @intCast(frame.len))) return error.ShortWrite;
    var got: usize = 0;
    for (0..300) |_| {
        const rc = std.c.read(fd, out[got..].ptr, out.len - got);
        if (rc > 0) got += @intCast(rc) else if (rc == 0) break;
        if (proto.Response.parse(out[0..got])) |r| {
            try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), r.header.status);
            return r.data;
        } else |_| {}
        stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    return error.NoResponse;
}

/// Pages `op` two at a time and checks each of `want` comes back once.
fn expectPagesThrough(ctx: *stdx.testing.TestContext, op: proto.OpCode, entry: Entry, want: []const []const u8) !void {
    var seen = [_]usize{0} ** 16;
    var cursor_buf: [256]u8 = undefined;
    var cursor: []const u8 = "";
    var pages: usize = 0;
    var out: [16 * 1024]u8 = undefined;
    while (pages < 50) : (pages += 1) {
        const d = try listPage(ctx, op, cursor, &out);
        const count = std.mem.readInt(u32, d[0..4], .little);
        try testing.expect(count <= 2);
        var pos: usize = 4;
        for (0..count) |_| {
            const name = entry(d, &pos);
            for (want, 0..) |w, i| if (std.mem.eql(u8, w, name)) {
                seen[i] += 1;
            };
        }
        const has_more = d[pos] != 0;
        const clen = std.mem.readInt(u16, d[pos + 1 ..][0..2], .little);
        if (!has_more) break;
        @memcpy(cursor_buf[0..clen], d[pos + 3 ..][0..clen]);
        cursor = cursor_buf[0..clen];
    }
    for (want, 0..) |_, i| try testing.expectEqual(@as(usize, 1), seen[i]);
}

const NAMES = [_][]const u8{ "pg-a", "pg-b", "pg-c", "pg-d", "pg-e", "pg-f", "pg-g" };

test "e2e/list: every list family pages by cursor, each name once" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();

    for (NAMES) |n| {
        try ctx.exec(&.{ "stream", "append", n, "x" });
        try ctx.exec(&.{ "queue", "enqueue", n, "x" });
        try ctx.exec(&.{ "action", "register", n });
        try ctx.exec(&.{ "kv", "set", n, "x" });
        try ctx.exec(&.{ "ts", "write", n, "--value", "1" });
        var ns: [16]u8 = undefined;
        try ctx.exec(&.{ "ns", "create", std.fmt.bufPrint(&ns, "{s}-ns", .{n}) catch unreachable });
    }

    try expectPagesThrough(ctx, .stream_list, streamEntry, &NAMES);
    try expectPagesThrough(ctx, .queue_list, queueEntry, &NAMES);
    try expectPagesThrough(ctx, .action_list, scanEntry, &NAMES);
    try expectPagesThrough(ctx, .kv_scan, scanEntry, &NAMES);
    try expectPagesThrough(ctx, .ts_list, nameU16, &NAMES);
    const ns_names = [_][]const u8{ "pg-a-ns", "pg-b-ns", "pg-c-ns", "pg-d-ns", "pg-e-ns", "pg-f-ns", "pg-g-ns" };
    try expectPagesThrough(ctx, .namespace_list, nameU16, &ns_names);
}

test "e2e/list: the CLI's --limit is the server's page size" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();
    for (NAMES) |n| try ctx.exec(&.{ "stream", "append", n, "x" });

    var r = try ctx.cli.run(&.{ "stream", "list", "--limit", "3", "-o", "json" });
    defer r.deinit();
    try testing.expectEqual(@as(usize, 3), r.stdoutCount("\"name\":\"pg-"));
}
