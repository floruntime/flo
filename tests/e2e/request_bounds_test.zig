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
    const put = try request(.kv_put, "", "alive", "yes");
    defer testing.allocator.free(put);
    const get = try request(.kv_get, "", "alive", "");
    defer testing.allocator.free(get);
    var out: [4096]u8 = undefined;
    if ((try send(ctx, put, &out)).status != @intFromEnum(proto.StatusCode.ok)) return false;
    const r = try send(ctx, get, &out);
    return r.status == @intFromEnum(proto.StatusCode.ok) and std.mem.endsWith(u8, r.data, "yes");
}

test "e2e/bounds: a stream, series or group name over 4096 bytes is refused, and the log still replays" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const key = try testing.allocator.alloc(u8, 65530);
    defer testing.allocator.free(key);
    @memset(key, 'k');
    var point: [8]u8 = undefined;
    std.mem.writeInt(u64, &point, @bitCast(@as(f64, 1.5)), .little);

    // A group-create's value is the length-prefixed group name; one this
    // long is refused before it is proposed.
    const group = try testing.allocator.alloc(u8, 2 + 5000);
    defer testing.allocator.free(group);
    std.mem.writeInt(u16, group[0..2], 5000, .little);
    @memset(group[2..], 'g');

    var out: [4096]u8 = undefined;
    for ([_]struct { op: proto.OpCode, key: []const u8, value: []const u8, why: []const u8 = "key or name too long" }{
        .{ .op = .stream_group_create, .key = "s", .value = group, .why = "stream + group name too long" },
        .{ .op = .stream_append, .key = key, .value = "x", .why = "stream name too long" },
        .{ .op = .ts_write, .key = key, .value = &point },
    }) |c| {
        const frame = try request(c.op, "", c.key, c.value);
        defer testing.allocator.free(frame);
        const r = try send(ctx, frame, &out);
        try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), r.status);
        try testing.expect(std.mem.indexOf(u8, r.data, c.why) != null);
    }
    try testing.expect(try kvAlive(ctx));
    try ctx.restartServer();
    try testing.expect(try kvAlive(ctx));
}

test "e2e/bounds: a request without the options trailer is answered when it is forwarded to another shard" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();

    // [ns_len=0][key_len=2]"kN"[value_len=0], and no options_len; 16 keys so
    // some route to another of the 4 shards.
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

test "e2e/bounds: a percent-encoded dashboard key path longer than any key is not found, and the dashboard still answers" {
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

test "e2e/bounds: a ts write value that isn't one finite f64 is refused by name" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var nan: [8]u8 = undefined;
    std.mem.writeInt(u64, &nan, @bitCast(std.math.nan(f64)), .little);
    var inf: [8]u8 = undefined;
    std.mem.writeInt(u64, &inf, @bitCast(std.math.inf(f64)), .little);

    var out: [4096]u8 = undefined;
    for ([_]struct { value: []const u8, why: []const u8 }{
        .{ .value = "1234567", .why = "ts write: value must be 8 bytes (f64, little-endian)" },
        .{ .value = "1234.5678", .why = "ts write: value must be 8 bytes (f64, little-endian)" },
        .{ .value = &nan, .why = "ts write: value must be a finite number" },
        .{ .value = &inf, .why = "ts write: value must be a finite number" },
    }) |c| {
        const frame = try request(.ts_write, "", "m", c.value);
        defer testing.allocator.free(frame);
        const r = try send(ctx, frame, &out);
        try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), r.status);
        try testing.expectEqualStrings(c.why, r.data);
    }

    var read = try ctx.cli.run(&.{ "ts", "read", "m", "--from", "0", "-o", "raw" });
    defer read.deinit();
    try testing.expectEqualStrings("(no data)\n", read.stdout);
}

test "e2e/bounds: an op no handler serves is refused as unknown, and the connection keeps serving" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();

    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    var out: [4096]u8 = undefined;
    for ([_]struct { op: u16, why: []const u8 }{
        .{ .op = @intFromEnum(proto.OpCode.queue_touch), .why = "" },
        .{ .op = 0x0FFE, .why = "unknown op 0xffe" },
        .{ .op = 0xFFFF, .why = "unknown op 0xffff" },
    }) |c| {
        var h = header(.kv_get);
        h.op_code = c.op;
        const payload = [_]u8{ 0, 0, 1, 0, 'k', 0, 0, 0, 0, 0, 0 };
        h.payload_length = payload.len;
        h.crc32 = h.computeCRC32(&payload);
        var frame: [@sizeOf(proto.RequestHeader) + payload.len]u8 = undefined;
        @memcpy(frame[0..@sizeOf(proto.RequestHeader)], std.mem.asBytes(&h));
        @memcpy(frame[@sizeOf(proto.RequestHeader)..], &payload);
        if (std.c.write(fd, &frame, frame.len) != @as(isize, @intCast(frame.len))) return error.ShortWrite;

        var got: usize = 0;
        const r = for (0..300) |_| {
            const rc = std.c.read(fd, out[got..].ptr, out.len - got);
            if (rc > 0) got += @intCast(rc) else if (rc == 0) return error.Closed;
            if (proto.Response.parse(out[0..got])) |r| break r else |_| {}
            stdx.time.sleep(10 * std.time.ns_per_ms);
        } else return error.NoResponse;
        try testing.expectEqual(got, @sizeOf(proto.ResponseHeader) + r.data.len);
        try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), r.header.status);
        var want: [32]u8 = undefined;
        try testing.expectEqualStrings(if (c.why.len > 0) c.why else std.fmt.bufPrint(&want, "unknown op 0x{x}", .{c.op}) catch unreachable, r.data);
    }
    try testing.expect(try kvAlive(ctx));
}
