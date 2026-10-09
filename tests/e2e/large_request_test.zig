//! A request larger than a connection's starting read buffer is read whole
//! and answered, however the bytes arrive.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;

test "e2e/read: a request larger than the read buffer is answered" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Over 64 KiB in one frame: an append the log refuses as too large,
    // which only an answer can say.
    // One record, framed as a batch so it gets past the batch check.
    const value = try testing.allocator.alloc(u8, 70_000);
    defer testing.allocator.free(value);
    std.mem.writeInt(u32, value[0..4], 1, .little);
    std.mem.writeInt(u32, value[4..8], @intCast(value.len - 10), .little);
    @memset(value[8 .. value.len - 2], 'v');
    std.mem.writeInt(u16, value[value.len - 2 ..][0..2], 0, .little);
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(proto.OpCode.stream_append);
    header.request_id = 7;
    const req: proto.Request = .{ .header = header, .namespace = "", .key = "big", .value = value, .options = "" };
    const buf = try testing.allocator.alloc(u8, @sizeOf(proto.RequestHeader) + 10 + 3 + value.len);
    defer testing.allocator.free(buf);
    const frame = try req.serialize(buf);

    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    // Sent before the server reads any of it, so it all waits in the socket.
    var sent: usize = 0;
    while (sent < frame.len) {
        const rc = std.c.write(fd, frame[sent..].ptr, frame.len - sent);
        if (rc <= 0) return error.ShortWrite;
        sent += @intCast(rc);
    }
    var out: [1024]u8 = undefined;
    var got: usize = 0;
    const resp = for (0..300) |_| {
        const rc = std.c.read(fd, out[got..].ptr, out.len - got);
        if (rc > 0) got += @intCast(rc) else if (rc == 0) break null;
        if (proto.Response.parse(out[0..got])) |r| break r else |_| {}
        stdx.time.sleep(10 * std.time.ns_per_ms);
    } else null;
    try testing.expect(resp != null);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), resp.?.header.status);
    try testing.expect(std.mem.indexOf(u8, resp.?.data, "too large to write") != null);
}
