//! A client built from another protocol version or op table is refused in
//! its own version byte, told both builds, and disconnected; the node keeps
//! serving everyone else.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;

/// Sends one ping header as a foreign client would, then reads until the
/// server closes the connection. Returns how many bytes came back.
fn foreignPing(ctx: *stdx.testing.TestContext, version: u8, table_hash: u64, out: []u8) !usize {
    var header = proto.RequestHeader.init(.ping, 11);
    header.version = version;
    header.table_hash = table_hash;
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, ctx.getPort(), 1000);
    defer _ = std.c.close(fd);
    const bytes = std.mem.asBytes(&header);
    if (std.c.write(fd, bytes.ptr, bytes.len) != bytes.len) return error.ShortWrite;
    var got: usize = 0;
    var waited: u32 = 0;
    while (waited < 300) : (waited += 1) {
        const rc = std.c.read(fd, out[got..].ptr, out.len - got);
        if (rc == 0) return got;
        if (rc > 0) got += @intCast(rc) else stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    return error.NotClosed;
}

test "e2e/table: a client from another table or protocol is refused readably and disconnected" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    var table_text: [128]u8 = undefined;
    const cases = [_]struct { version: u8, table_hash: u64, text: []const u8 }{
        .{ .version = proto.VERSION, .table_hash = proto.TABLE_HASH +% 1, .text = try std.fmt.bufPrint(&table_text, "server table 0x{x:0>16}, client table 0x{x:0>16}: upgrade the client", .{ proto.TABLE_HASH, proto.TABLE_HASH +% 1 }) },
        .{ .version = 1, .table_hash = 0, .text = std.fmt.comptimePrint("server protocol {d}, client protocol 1: upgrade the client", .{proto.VERSION}) },
        .{ .version = proto.VERSION +% 1, .table_hash = proto.TABLE_HASH, .text = std.fmt.comptimePrint("server protocol {d}, client protocol {d}: upgrade the client", .{ proto.VERSION, proto.VERSION +% 1 }) },
    };
    for (cases) |case| {
        var out: [512]u8 = undefined;
        const n = try foreignPing(ctx, case.version, case.table_hash, &out);
        try testing.expectEqual(@sizeOf(proto.ResponseHeader) + proto.Refusal.HEADER_LEN + case.text.len, n);
        const answer = std.mem.bytesToValue(proto.ResponseHeader, out[0..@sizeOf(proto.ResponseHeader)]);
        try testing.expectEqual(proto.MAGIC, answer.magic);
        try testing.expectEqual(case.version, answer.version);
        try testing.expectEqual(proto.TABLE_HASH, answer.table_hash);
        try testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), answer.status);
        try testing.expectEqual(@as(u64, 11), answer.request_id);
        const refusal = try proto.Refusal.decode(out[@sizeOf(proto.ResponseHeader)..n]);
        try testing.expectEqualStrings(case.text, refusal.message);
        try testing.expectEqual(proto.Reason.table_mismatch, refusal.reason);
        try testing.expectEqual(proto.Ran.no, refusal.ran);
    }

    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var metrics = try http.get("/metrics");
    defer metrics.deinit();
    try testing.expectEqual(@as(u16, 200), metrics.status);
    try testing.expect(std.mem.indexOf(u8, metrics.body, "\nflo_client_version_refused_total 3\n") != null);

    // Everyone else is still served.
    try ctx.exec(&.{ "kv", "set", "after", "yes" });
    var got = try ctx.cli.run(&.{ "kv", "get", "after" });
    defer got.deinit();
    try testing.expectEqual(@as(u8, 0), got.exit_code);
}
