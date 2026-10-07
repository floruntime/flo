//! Times a client supplies — TTLs, ages, range bounds, FloQL durations — are
//! converted without trapping: a value too large to represent is refused, or,
//! for a range bound, means no bound. The node answers each and keeps serving.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;

const Reply = struct { status: u8, data: []const u8 };

/// Sends a hand-built request: the CLI client can't encode these values.
fn rawCall(ctx: *stdx.testing.TestContext, op: proto.OpCode, key: []const u8, value: []const u8, options: []const u8, out: []u8) !Reply {
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(op);
    header.request_id = 7;
    const req: proto.Request = .{ .header = header, .namespace = "default", .key = key, .value = value, .options = options };
    var buf: [1024]u8 = undefined;
    const bytes = try req.serialize(&buf);
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    if (std.c.write(fd, bytes.ptr, bytes.len) != @as(isize, @intCast(bytes.len))) return error.ShortWrite;
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

fn option(buf: []u8, tag: proto.OptionTag, comptime T: type, value: T) ![]const u8 {
    var b = proto.OptionsBuilder.init(buf);
    if (T == u64) try b.addU64(tag, value) else try b.addI64(tag, value);
    return b.getOptions();
}

fn expectRefused(r: Reply, why: []const u8) !void {
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), r.status);
    try testing.expect(std.mem.indexOf(u8, r.data, why) != null);
}

test "e2e/time: a TTL too large to represent is refused" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "kv", "set", "k", "v" });

    var out: [4096]u8 = undefined;
    var obuf: [32]u8 = undefined;
    try expectRefused(try rawCall(ctx, .kv_put, "k", "v2", try option(&obuf, .ttl_seconds, u64, std.math.maxInt(u64)), &out), "ttl too large");
    var ttl: [8]u8 = undefined;
    std.mem.writeInt(u64, &ttl, std.math.maxInt(u64), .little);
    try expectRefused(try rawCall(ctx, .kv_touch, "k", &ttl, "", &out), "ttl too large");

    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "kv", "get", "k" }), "v") != null);
}

test "e2e/time: an age or range bound past the clock's range trims or reads nothing" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "s", "kept" });
    try ctx.exec(&.{ "ts", "write", "m", "--value", "1" });

    var out: [4096]u8 = undefined;
    var obuf: [32]u8 = undefined;
    const trim = try rawCall(ctx, .stream_trim, "s", "", try option(&obuf, .max_age_seconds, u64, std.math.maxInt(u64)), &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), trim.status);
    const read = try rawCall(ctx, .ts_read, "m", "", try option(&obuf, .ts_from_ms, i64, std.math.maxInt(i64)), &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), read.status);
    const query = try rawCall(ctx, .ts_floql, "", "m[20000000000000]", "", &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), query.status);

    var r = try ctx.cli.run(&.{ "stream", "read", "s", "--start", "0-0" });
    defer r.deinit();
    try testing.expect(r.stdoutContains("kept"));
}

test "e2e/time: a FloQL duration too large to represent is refused" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "ts", "write", "m", "--value", "1" });

    var out: [4096]u8 = undefined;
    const query = try rawCall(ctx, .ts_floql, "", "m[106751991168d]", "", &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), query.status);
    try expectRefused(try rawCall(ctx, .ts_retention, "m", "106751991168d", "", &out), "invalid retention duration");

    try ctx.exec(&.{ "kv", "set", "alive", "yes" });
    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "kv", "get", "alive" }), "yes") != null);
}
