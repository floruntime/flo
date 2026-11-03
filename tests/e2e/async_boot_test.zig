//! A member's boot under async durability: what it flushed may be history a
//! crashed quorum lost and a leader rewrote, so it applies nothing until a
//! leader commits it, and serves no reads until then.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ClusterContext = stdx.testing.ClusterContext;

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
