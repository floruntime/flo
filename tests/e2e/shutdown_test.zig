//! A server told to stop under load stops promptly, and every write it
//! acknowledged is there when it starts again.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const proto = @import("src").protocol.proto;
const kv_ops = @import("src").protocol.kv_ops;

const CLIENTS = 4;

/// Put keys `c<client>-<i>` as request id i + 1 over one connection, `batch`
/// in flight, until the server stops answering; each id answered ok goes in
/// `acked`. The key's shard may not be this connection's, so answers come
/// back in any order: only an id says which write was acknowledged.
fn putUntilClosed(port: u16, client: usize, acked: *std.ArrayList(u64)) void {
    const batch = 256;
    const fd = stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 1000) catch return;
    defer _ = std.c.close(fd);
    var out: [batch * 128]u8 = undefined;
    var in: [batch * 64 + 4096]u8 = undefined;
    var body_buf: [16]u8 = undefined;
    const body = kv_ops.plainSetBody(&body_buf, "v") catch return;
    var next: usize = 0;
    while (true) {
        var used: usize = 0;
        for (0..batch) |i| {
            var key_buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "c{d}-{d}", .{ client, next + i }) catch return;
            const req: proto.Request = .{ .header = proto.RequestHeader.init(.kv_put, next + i + 1), .namespace = proto.DEFAULT_NAMESPACE, .key = key, .value = body, .options = "" };
            used += (req.serialize(out[used..]) catch return).len;
        }
        var sent: usize = 0;
        while (sent < used) {
            const rc = std.c.write(fd, out[sent..].ptr, used - sent);
            if (rc <= 0) return;
            sent += @intCast(rc);
        }
        var got: usize = 0;
        var off: usize = 0;
        var answered: usize = 0;
        while (answered < batch) {
            if (proto.Response.parse(in[off..got])) |r| {
                if (r.header.status == @backingInt(proto.StatusCode.ok)) acked.append(std.heap.page_allocator, r.header.request_id) catch return;
                off += @sizeOf(proto.ResponseHeader) + r.header.data_len;
                answered += 1;
                continue;
            } else |_| {}
            if (off > 0) {
                std.mem.copyForwards(u8, in[0 .. got - off], in[off..got]);
                got -= off;
                off = 0;
            }
            const rc = std.c.read(fd, in[got..].ptr, in.len - got);
            if (rc == 0) return;
            if (rc < 0) {
                if (std.c._errno().* != @backingInt(std.c.E.AGAIN)) return;
                stdx.time.sleep(std.time.ns_per_ms / 4);
                continue;
            }
            got += @intCast(rc);
        }
        next += batch;
    }
}

/// Whether `kv get`'s table has a row of exactly `key`, `value` and a version.
fn hasRow(stdout: []const u8, key: []const u8, value: []const u8) bool {
    var lines = std.mem.splitScalar(u8, stdout, '\n');
    while (lines.next()) |line| {
        var fields = std.mem.tokenizeAny(u8, line, " \t");
        const k = fields.next() orelse continue;
        const v = fields.next() orelse continue;
        const version = fields.next() orelse continue;
        _ = std.fmt.parseInt(u64, version, 10) catch continue;
        if (std.mem.eql(u8, k, key) and std.mem.eql(u8, v, value) and fields.next() == null) return true;
    }
    return false;
}

test "e2e/shutdown: SIGTERM under load stops the server promptly, and every acknowledged write survives it" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 4, .durability = .sync },
    });
    defer ctx.deinit();

    var acked: [CLIENTS]std.ArrayList(u64) = @splat(.empty);
    defer for (&acked) |*a| a.deinit(std.heap.page_allocator);
    var threads: [CLIENTS]std.Thread = undefined;
    for (&threads, 0..) |*t, c| t.* = try std.Thread.spawn(.{}, putUntilClosed, .{ ctx.getPort(), c, &acked[c] });
    stdx.time.sleep(2 * std.time.ns_per_s);

    const pid = ctx.server.process.?.id;
    ctx.server.sent_term = true;
    _ = std.c.kill(pid, .TERM);
    const t0 = stdx.time.monotonicMs();
    var exited = false;
    while (stdx.time.monotonicMs() - t0 < 10_000) {
        var status: c_int = 0;
        if (std.c.waitpid(pid, &status, std.c.W.NOHANG) == pid) {
            exited = true;
            ctx.server.reaped = true;
            ctx.server.exit_status = status;
            break;
        }
        stdx.time.sleep(20 * std.time.ns_per_ms);
    }
    for (threads) |t| t.join();
    if (!exited) {
        _ = std.c.kill(pid, .KILL);
        std.debug.print("server still running 10 s after SIGTERM\n", .{});
        return error.TestUnexpectedResult;
    }

    try ctx.restartServer();
    // Each client's last acknowledged keys, the ones a lost flush would
    // take first, read back.
    for (&acked, 0..) |*a, c| {
        try testing.expect(a.items.len > 0);
        std.mem.sort(u64, a.items, {}, std.sort.asc(u64));
        for (a.items[a.items.len -| 20..]) |id| {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "c{d}-{d}", .{ c, id - 1 });
            var r = try ctx.cli.run(&.{ "kv", "get", key });
            defer r.deinit();
            if (!hasRow(r.stdout, key, "v")) {
                std.debug.print("acknowledged {s} ({d} acked) is missing after the restart: stdout '{s}' stderr '{s}'\n", .{ key, a.items.len, r.stdout, r.stderr });
                return error.TestUnexpectedResult;
            }
        }
    }
}
