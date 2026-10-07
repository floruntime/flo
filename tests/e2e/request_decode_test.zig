//! Requests whose length or type fields don't fit what follows are answered,
//! and the node keeps serving. Each frame here is one the CLI can't produce.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;

const Reply = struct { status: u8, data: []const u8 };

fn rawCall(ctx: *stdx.testing.TestContext, op: proto.OpCode, namespace: []const u8, key: []const u8, value: []const u8, out: []u8) !Reply {
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(op);
    header.request_id = 7;
    const req: proto.Request = .{ .header = header, .namespace = namespace, .key = key, .value = value, .options = "" };
    const buf = try testing.allocator.alloc(u8, @sizeOf(proto.RequestHeader) + 10 + namespace.len + key.len + value.len);
    defer testing.allocator.free(buf);
    const bytes = try req.serialize(buf);
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    var sent: usize = 0;
    while (sent < bytes.len) {
        const rc = std.c.write(fd, bytes[sent..].ptr, bytes.len - sent);
        if (rc <= 0) return error.ShortWrite;
        sent += @intCast(rc);
    }
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

fn alive(ctx: *stdx.testing.TestContext) !void {
    var out: [512]u8 = undefined;
    _ = try rawCall(ctx, .kv_put, "", "alive", "yes", &out);
    const r = try rawCall(ctx, .kv_get, "", "alive", "", &out);
    try testing.expectEqual(@intFromEnum(proto.StatusCode.ok), r.status);
    try testing.expect(std.mem.endsWith(u8, r.data, "yes"));
}

const Case = struct { name: []const u8, op: proto.OpCode, ns: []const u8 = "", key: []const u8, value: []const u8, status: proto.StatusCode };

test "e2e/decode: requests whose lengths or types don't fit are answered, and the node keeps serving" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "action", "register", "act" });

    const cases = [_]Case{
        // A name length of 0xFFFE: two plus it overflows sixteen bits.
        .{ .name = "complete", .op = .action_complete, .key = "w", .value = &.{ 0xfe, 0xff }, .status = .bad_request },
        .{ .name = "fail", .op = .action_fail, .key = "w", .value = &.{ 0xfe, 0xff }, .status = .bad_request },
        .{ .name = "touch", .op = .action_touch, .key = "w", .value = &.{ 0xfe, 0xff }, .status = .bad_request },
        .{ .name = "await", .op = .action_await, .key = "w", .value = &.{ 1, 0, 0, 0, 0xff, 0xff }, .status = .bad_request },
        // [priority][delay_ms:i64][has_caller][has_idem=1][idem_len=0xFFFF]:
        // a header that doesn't parse is taken as plain input, as before.
        .{ .name = "invoke", .op = .action_invoke, .key = "act", .value = &.{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff }, .status = .ok },
        .{ .name = "json_set", .op = .kv_json_set, .key = "k", .value = &.{ 0xfe, 0xff }, .status = .bad_request },
        // An unknown worker type.
        .{ .name = "worker", .op = .worker_register, .key = "w", .value = &.{2}, .status = .ok },
        // One setting with an unknown tag.
        .{ .name = "config", .op = .namespace_config_set, .key = "default", .value = &.{ 1, 8 }, .status = .bad_request },
        .{ .name = "info", .op = .namespace_info, .key = "a" ** 129, .value = "", .status = .bad_request },
    };
    var out: [4096]u8 = undefined;
    for (cases) |c| {
        const r = rawCall(ctx, c.op, c.ns, c.key, c.value, &out) catch |err| {
            std.debug.print("case {s}: {}\n", .{ c.name, err });
            return err;
        };
        if (r.status != @intFromEnum(c.status)) std.debug.print("case {s}: status {d} {s}\n", .{ c.name, r.status, r.data });
        try testing.expectEqual(@intFromEnum(c.status), r.status);
    }
    try alive(ctx);
}
