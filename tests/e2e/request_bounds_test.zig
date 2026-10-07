//! Requests whose sizes sit at the edges of the wire format are refused or
//! answered, and the node keeps serving afterwards.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;

const Reply = struct { status: u8, data: []const u8 };

/// Sends one frame as given and returns the reply: these frames are ones the
/// CLI client can't produce.
fn send(ctx: *stdx.testing.TestContext, frame: []const u8, out: []u8) !Reply {
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    if (std.c.write(fd, frame.ptr, frame.len) != @as(isize, @intCast(frame.len))) return error.ShortWrite;
    var got: usize = 0;
    var waited: u32 = 0;
    while (waited < 300) : (waited += 1) {
        const rc = std.c.read(fd, out[got..].ptr, out.len - got);
        if (rc > 0) got += @intCast(rc) else if (rc == 0) break;
        if (proto.Response.parse(out[0..got])) |resp| return .{ .status = resp.header.status, .data = resp.data } else |_| {}
        stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    return error.NoResponse;
}

fn header(op: proto.OpCode) proto.RequestHeader {
    var h: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&h), 0);
    h.magic = proto.MAGIC;
    h.version = proto.VERSION;
    h.op_code = @intFromEnum(op);
    h.request_id = 7;
    return h;
}

fn request(op: proto.OpCode, namespace: []const u8, key: []const u8, value: []const u8) ![]u8 {
    const req: proto.Request = .{ .header = header(op), .namespace = namespace, .key = key, .value = value, .options = "" };
    const buf = try testing.allocator.alloc(u8, @sizeOf(proto.RequestHeader) + 10 + namespace.len + key.len + value.len);
    errdefer testing.allocator.free(buf);
    _ = try req.serialize(buf);
    return buf;
}

fn kvAlive(ctx: *stdx.testing.TestContext) !bool {
    try ctx.exec(&.{ "kv", "set", "alive", "yes" });
    return std.mem.indexOf(u8, try ctx.execCapture(&.{ "kv", "get", "alive" }), "yes") != null;
}

test "e2e/bounds: a name too long for a log entry is refused, and the log still replays" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const key = try testing.allocator.alloc(u8, 65530);
    defer testing.allocator.free(key);
    @memset(key, 'k');
    var point: [8]u8 = undefined;
    std.mem.writeInt(u64, &point, @bitCast(@as(f64, 1.5)), .little);

    // A group name that is a group-create's whole value: an empty
    // length-prefixed name falls back to the raw value.
    const group = try testing.allocator.alloc(u8, 65542);
    defer testing.allocator.free(group);
    @memset(group, 'g');
    group[0] = 0;
    group[1] = 0;

    var out: [4096]u8 = undefined;
    for ([_]struct { op: proto.OpCode, key: []const u8, value: []const u8 }{
        .{ .op = .stream_group_create, .key = "s", .value = group },
        .{ .op = .stream_append, .key = key, .value = "x" },
        .{ .op = .ts_write, .key = key, .value = &point },
    }) |c| {
        const frame = try request(c.op, "", c.key, c.value);
        defer testing.allocator.free(frame);
        const r = try send(ctx, frame, &out);
        try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), r.status);
        try testing.expect(std.mem.indexOf(u8, r.data, "key or name too long") != null);
    }
    try testing.expect(try kvAlive(ctx));
    try ctx.restartServer();
    try testing.expect(try kvAlive(ctx));
}

test "e2e/bounds: a request without the options trailer is answered when it is forwarded to another shard" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();

    // [ns_len=0][key_len=2]"kN"[value_len=0], and no options_len.
    for (0..16) |i| {
        var h = header(.kv_get);
        const payload = [_]u8{ 0, 0, 2, 0, 'k', 'a' + @as(u8, @intCast(i)), 0, 0, 0, 0 };
        h.payload_length = payload.len;
        h.crc32 = h.computeCRC32(&payload);
        var frame: [@sizeOf(proto.RequestHeader) + payload.len]u8 = undefined;
        @memcpy(frame[0..@sizeOf(proto.RequestHeader)], std.mem.asBytes(&h));
        @memcpy(frame[@sizeOf(proto.RequestHeader)..], &payload);
        var out: [4096]u8 = undefined;
        try testing.expectEqual(@intFromEnum(proto.StatusCode.not_found), (try send(ctx, &frame, &out)).status);
    }
    try testing.expect(try kvAlive(ctx));
}

test "e2e/bounds: a dashboard key path longer than a key is not found, and the dashboard still answers" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var long = try http.get("/api/v1/kv/namespaces/default/keys/" ++ "a" ** 5000 ++ "%41");
    defer long.deinit();
    try testing.expectEqual(@as(u16, 404), long.status);

    try ctx.exec(&.{ "kv", "set", "k", "v" });
    var ok = try http.get("/api/v1/kv/namespaces/default/keys/k");
    defer ok.deinit();
    try testing.expectEqual(@as(u16, 200), ok.status);
}
