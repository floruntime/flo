//! A member's boot under async durability: what it flushed may be history a
//! crashed quorum lost and a leader rewrote, so it applies nothing until a
//! leader commits it, and serves no reads until then.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ClusterContext = stdx.testing.ClusterContext;
const kv_ops = @import("src").protocol.kv_ops;
const proto = @import("src").protocol.proto;

/// `GET path` on the metrics port: the status code and the body's first
/// word, as "503 catching_up". Null if the node doesn't answer.
fn ready(cluster: *ClusterContext, node: usize, buf: []u8) ?[]const u8 {
    const port = cluster.getServer(node).?.metrics_port;
    const fd = stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 200) catch return null;
    defer _ = std.c.close(fd);
    const req = "GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n";
    if (std.c.write(fd, req, req.len) != req.len) return null;
    var raw: [512]u8 = undefined;
    var got: usize = 0;
    // The socket does not block: read until the server closes it.
    const deadline = stdx.time.milliTimestamp() + 1000;
    while (got < raw.len and stdx.time.milliTimestamp() < deadline) {
        const rc = std.c.read(fd, raw[got..].ptr, raw.len - got);
        if (rc == 0) break;
        if (rc < 0) {
            stdx.time.sleep(std.time.ns_per_ms / 4);
            continue;
        }
        got += @intCast(rc);
    }
    const answer = raw[0..got];
    if (answer.len < 12) return null;
    const body = if (std.mem.indexOf(u8, answer, "\r\n\r\n")) |i| std.mem.trim(u8, answer[i + 4 ..], "\r\n ") else return null;
    var words = std.mem.tokenizeAny(u8, body, " \n\"{}:,");
    var first = words.next() orelse "";
    // A JSON body names its state after a key.
    if (std.mem.eql(u8, first, "state")) first = words.next() orelse "";
    return std.fmt.bufPrint(buf, "{s} {s}", .{ answer[9..12], first }) catch null;
}

test "e2e/async-boot: a lone survivor serves no reads until a quorum is back, then the group's data" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .async_flush, .metrics_enabled = true });
    defer cluster.deinit();
    errdefer cluster.printLogs();

    try cluster.execOnWithRetry(0, &.{ "kv", "set", "k", "v" }, 20, 250);
    for (0..3) |i| cluster.allocator.free(try cluster.pollUntilContains(i, &.{ "kv", "get", "k" }, "v", 40, 250));

    // A clean stop flushes; every node goes down.
    cluster.stopNode(1);
    cluster.stopNode(2);
    cluster.stopNode(0);
    try cluster.restartNode(0);

    // Alone it hears no leader: it has the entry on disk, but serves none
    // of it, and isn't ready.
    var refused = try cluster.clis[0].?.run(&.{ "kv", "get", "k" });
    defer refused.deinit();
    try stdx.testing.assertFailed(refused);
    try testing.expect(refused.contains("restarted and is catching up with its group's leader"));
    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("503 catching_up", ready(&cluster, 0, &buf) orelse "no answer");

    // A quorum again: a leader commits the log, and the survivor serves it.
    try cluster.restartNode(1);
    cluster.allocator.free(try cluster.pollUntilContains(0, &.{ "kv", "get", "k" }, "v", 80, 250));
    var tries: usize = 0;
    while (tries < 40) : (tries += 1) {
        if (std.mem.eql(u8, ready(&cluster, 0, &buf) orelse "", "200 ready")) break;
        stdx.time.sleep(250 * std.time.ns_per_ms);
    }
    try testing.expectEqualStrings("200 ready", ready(&cluster, 0, &buf) orelse "no answer");
}

/// `count` puts of "v" to `port`, pipelined `batch` at a time; refused
/// answers fail it.
fn putMany(port: u16, count: usize, batch: usize) !void {
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 1000);
    defer _ = std.c.close(fd);
    const out = try testing.allocator.alloc(u8, batch * 128);
    defer testing.allocator.free(out);
    const in = try testing.allocator.alloc(u8, batch * 64 + 4096);
    defer testing.allocator.free(in);
    var body_buf: [16]u8 = undefined;
    const body = try kv_ops.plainSetBody(&body_buf, "v");

    var next: usize = 0;
    while (next < count) {
        const n = @min(batch, count - next);
        var used: usize = 0;
        for (0..n) |i| {
            var key_buf: [24]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "bulk-{d}", .{next + i});
            const req: proto.Request = .{ .header = proto.RequestHeader.init(.kv_put, next + i + 1), .namespace = "", .key = key, .value = body, .options = "" };
            used += (try req.serialize(out[used..])).len;
        }
        var sent: usize = 0;
        while (sent < used) {
            const rc = std.c.write(fd, out[sent..].ptr, used - sent);
            if (rc <= 0) return error.ShortWrite;
            sent += @intCast(rc);
        }
        var got: usize = 0;
        var answered: usize = 0;
        var off: usize = 0;
        while (answered < n) {
            if (proto.Response.parse(in[off..got])) |r| {
                if (r.header.status != @backingInt(proto.StatusCode.ok)) return error.PutRefused;
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
            if (rc == 0) return error.ConnectionClosed;
            if (rc < 0) {
                // The socket does not block; wait for the answers.
                if (std.c._errno().* != @backingInt(std.c.E.AGAIN)) return error.ReadFailed;
                stdx.time.sleep(std.time.ns_per_ms / 4);
                continue;
            }
            got += @intCast(rc);
        }
        next += n;
    }
}

test "e2e/async-boot: after a whole-cluster restart a member refuses reads until it has applied, and never answers missing" {
    // At info: a line per replayed entry would make each boot outlast the
    // harness's wait for it.
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .async_flush, .metrics_enabled = true, .log_level = "info" });
    defer cluster.deinit();
    errdefer cluster.printLogs();

    // A log long enough that applying it takes many passes: the window in
    // which a member that thought itself caught up would answer from
    // nothing is then wide, every round.
    try putMany(cluster.getServer(0).?.getPort(), 20_000, 512);
    for (0..3) |i| cluster.allocator.free(try cluster.pollUntilContains(i, &.{ "kv", "get", "bulk-19999" }, "v", 80, 250));

    for (0..3) |round| {
        cluster.stopNode(1);
        cluster.stopNode(2);
        cluster.stopNode(0);
        // Node 2 first, alone; then node 0 without the harness's settling
        // pause, so the election and the new leader's first appends
        // (commit 0, then its noop) happen while node 2 is read.
        try cluster.restartNode(2);
        try cluster.getServer(0).?.startWithTimeout(60_000);
        var refusals: usize = 0;
        var tries: usize = 0;
        while (true) : (tries += 1) {
            // The last key written applies last: the widest window.
            var got = try cluster.clis[2].?.run(&.{ "kv", "get", "bulk-19999" });
            defer got.deinit();
            if (got.stdoutContains("v")) break;
            if (!got.contains("restarted and is catching up")) {
                std.debug.print("round {d}: node 2 answered: {s}{s}\n", .{ round, got.stdout, got.stderr });
                return error.ReadNotRefused;
            }
            refusals += 1;
            if (tries == 1500) return error.NeverCaughtUp;
            stdx.time.sleep(20 * std.time.ns_per_ms);
        }
        try testing.expect(refusals > 0);
        // The rest back, for the next round; node 0 again through the
        // harness, so its client follows the new port.
        cluster.stopNode(0);
        try cluster.restartNode(0);
        try cluster.restartNode(1);
        for (0..3) |i| cluster.allocator.free(try cluster.pollUntilContains(i, &.{ "kv", "get", "bulk-19999" }, "v", 80, 250));
    }
}
