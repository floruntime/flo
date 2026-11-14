//! `[queues] max_messages`, `max_bytes` and `shard_held_bytes`: what a
//! queue, and a shard's queues, may hold. Past one, an enqueue is refused
//! `overloaded`; the leader's caps decide, and a node whose own differ says
//! so.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ClusterContext = stdx.testing.ClusterContext;
const CliRunner = stdx.testing.CliRunner;
const ServerProcess = stdx.testing.ServerProcess;

const Node = struct {
    server: *ServerProcess,
    cli: *CliRunner,
    ep: []const u8,

    fn start(config: ServerProcess.ServerConfig) !Node {
        const server = try ServerProcess.initWithConfig(testing.allocator, config);
        errdefer server.deinit();
        try server.start();
        const ep = try server.getEndpoint(testing.allocator);
        errdefer testing.allocator.free(ep);
        return .{ .server = server, .cli = try CliRunner.init(testing.allocator, server.flo_binary, ep), .ep = ep };
    }

    fn deinit(self: Node) void {
        self.cli.deinit();
        testing.allocator.free(self.ep);
        self.server.deinit();
    }

    /// Enqueue `payload` to `queue`: its id on success, else null with the
    /// refusal checked against `refused`.
    fn enqueue(self: Node, queue: []const u8, payload: []const u8, refused: ?[]const u8) !void {
        var out = try self.cli.run(&.{ "queue", "enqueue", queue, payload });
        defer out.deinit();
        if (refused) |says| {
            if (!out.contains(says)) std.debug.print("expected the refusal to say: {s}\ngot: {s}{s}\n", .{ says, out.stdout, out.stderr });
            try testing.expect(out.contains(says));
            // Its own reason, and the CLI's exit for a refusal to retry.
            if (!out.contains("[overloaded/queue_full]")) std.debug.print("expected [overloaded/queue_full]: {s}{s}\n", .{ out.stdout, out.stderr });
            try testing.expect(out.contains("[overloaded/queue_full]"));
            try testing.expectEqual(@as(u8, 4), out.exit_code);
        } else {
            if (out.exit_code != 0 or out.contains("overloaded")) std.debug.print("enqueue refused: {s}{s}\n", .{ out.stdout, out.stderr });
            try testing.expectEqual(@as(u8, 0), out.exit_code);
            try testing.expect(!out.contains("overloaded"));
        }
    }
};

test "e2e/queue-cap: a lowered max_messages refuses the next enqueue, and a purge makes room" {
    const node = try Node.start(.{ .shards = 1, .extra_config = "[queues]\nmax_messages = 2\n" });
    defer node.deinit();
    errdefer node.server.dumpLogTail();
    try node.enqueue("q", "one", null);
    try node.enqueue("q", "two", null);
    try node.enqueue("q", "three", "queue 'q' is full: it holds 2 messages (at most 2)");
    // One leased: a purge takes it too.
    var taken = try node.cli.run(&.{ "queue", "dequeue", "q" });
    defer taken.deinit();
    try testing.expect(taken.stdoutContains("one"));
    // Another queue has its own count.
    try node.enqueue("other", "one", null);
    var purge = try node.cli.run(&.{ "queue", "purge", "q" });
    defer purge.deinit();
    try testing.expectEqual(@as(u8, 0), purge.exit_code);
    try testing.expect(purge.stdoutContains("Purged: 2"));
    try node.enqueue("q", "three", null);
}

test "e2e/queue-cap: a lowered max_bytes refuses the message that would pass it" {
    const node = try Node.start(.{ .shards = 1, .extra_config = "[queues]\nmax_bytes = 65536\n" });
    defer node.deinit();
    errdefer node.server.dumpLogTail();
    const big = try testing.allocator.alloc(u8, 60_000);
    defer testing.allocator.free(big);
    @memset(big, 'x');
    try node.enqueue("q", big, null);
    // A message holds its payload and 512 bytes besides.
    try node.enqueue("q", big[0..10_000], "queue 'q' is full: it holds 1 messages (at most 100000) and 60512 held bytes (at most 65536)");
    try node.enqueue("q", big[0..4_512], null);
}

test "e2e/queue-cap: a lowered shard_held_bytes counts every queue on the shard" {
    const node = try Node.start(.{ .shards = 1, .extra_config = "[queues]\nshard_held_bytes = 65536\n" });
    defer node.deinit();
    errdefer node.server.dumpLogTail();
    const big = try testing.allocator.alloc(u8, 60_000);
    defer testing.allocator.free(big);
    @memset(big, 'x');
    try node.enqueue("a", big, null);
    try node.enqueue("b", big[0..10_000], "this shard's queues and groups hold 60512 held bytes, at most 65536");
}

test "e2e/queue-cap: empty messages across many queues fill a shard, each holding its overhead" {
    const node = try Node.start(.{ .shards = 1, .extra_config = "[queues]\nshard_held_bytes = 65536\n" });
    defer node.deinit();
    errdefer node.server.dumpLogTail();
    // 65536 / 512: 128 empty messages fit, one to a queue.
    var name_buf: [16]u8 = undefined;
    for (0..128) |i| try node.enqueue(try std.fmt.bufPrint(&name_buf, "q{d}", .{i}), "", null);
    try node.enqueue("q128", "", "this shard's queues and groups hold 65536 held bytes, at most 65536");
}

fn expectRefused(config: ServerProcess.ServerConfig, says: []const u8) !void {
    const node = try ServerProcess.initWithConfig(testing.allocator, config);
    defer node.deinit();
    node.dump_log_on_failure = false;
    try testing.expectError(error.ServerNotReady, node.startWithTimeout(3000));
    if (!try node.logsContain(says)) {
        std.debug.print("expected the log to say: {s}\n", .{says});
        return error.TestUnexpectedResult;
    }
}

test "e2e/queue-cap: caps outside their range are refused at start, not clamped" {
    try expectRefused(.{ .shards = 1, .extra_config = "[queues]\nmax_messages = 0\n" }, "[queues] max_messages = 0: must be 1 to 16777216");
    try expectRefused(.{ .shards = 1, .extra_config = "[queues]\nmax_bytes = 1073741825\n" }, "[queues] max_bytes = 1073741825: must be 65536 to 1073741824");
    try expectRefused(.{ .shards = 1, .extra_config = "[queues]\nshard_held_bytes = 1024\n" }, "[queues] shard_held_bytes = 1024: must be 65536 to 68719476736");
}

/// `flo_queue_cap_mismatch_total` on `node`'s /metrics; null if it doesn't
/// answer or doesn't say.
fn mismatches(cluster: *ClusterContext, node: usize) ?u64 {
    const port = cluster.getServer(node).?.metrics_port;
    const fd = stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 200) catch return null;
    defer _ = std.c.close(fd);
    const req = "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n";
    if (std.c.write(fd, req, req.len) != req.len) return null;
    const raw = testing.allocator.alloc(u8, 1 << 20) catch return null;
    defer testing.allocator.free(raw);
    var got: usize = 0;
    // The socket does not block: read until the server closes it.
    const deadline = stdx.time.milliTimestamp() + 2000;
    while (got < raw.len and stdx.time.milliTimestamp() < deadline) {
        const rc = std.c.read(fd, raw[got..].ptr, raw.len - got);
        if (rc == 0) break;
        if (rc < 0) {
            stdx.time.sleep(std.time.ns_per_ms / 4);
            continue;
        }
        got += @intCast(rc);
    }
    const name = "\nflo_queue_cap_mismatch_total ";
    const at = std.mem.indexOf(u8, raw[0..got], name) orelse return null;
    const rest = raw[at + name.len .. got];
    const end = std.mem.indexOfScalar(u8, rest, '\n') orelse rest.len;
    return std.fmt.parseInt(u64, std.mem.trim(u8, rest[0..end], " \r"), 10) catch null;
}

test "e2e/queue-cap: nodes with other caps than the leader's apply its enqueues, and say so" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .sync, .metrics_enabled = true, .node_config = .{
        "[queues]\nmax_messages = 1000\n",
        "[queues]\nmax_messages = 2000\n",
        "[queues]\nmax_messages = 3000\n",
        null,
        null,
    } });
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try cluster.execOnWithRetry(0, &.{ "queue", "enqueue", "q", "m" }, 20, 250);
    // Whichever node leads, the other two applied under caps not their own.
    const SAYS = "not this node's [queues] settings";
    var tries: usize = 0;
    while (true) : (tries += 1) {
        var warned: usize = 0;
        for (0..3) |n| {
            if (cluster.getServer(n)) |s| {
                if (try s.logsContain(SAYS)) warned += 1;
            }
        }
        if (warned == 2) break;
        if (tries == 80) return error.MismatchNeverSaid;
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    // And counted, on the same two.
    var counted: usize = 0;
    for (0..3) |n| if ((mismatches(&cluster, n) orelse 0) >= 1) {
        counted += 1;
    };
    try testing.expectEqual(@as(usize, 2), counted);
}

test "e2e/queue-cap: a group read past its caps is refused group_held_full until its records are acked" {
    const node = try Node.start(.{ .shards = 1, .extra_config = "[queues]\nmax_messages = 2\n" });
    defer node.deinit();
    errdefer node.server.dumpLogTail();
    var append = try node.cli.run(&.{ "stream", "append", "s", "a", "b", "c" });
    defer append.deinit();
    try testing.expectEqual(@as(u8, 0), append.exit_code);
    var first = try node.cli.run(&.{ "stream", "group", "read", "s", "--group", "g", "--consumer", "w", "--limit", "3" });
    defer first.deinit();
    try testing.expectEqual(@as(u8, 0), first.exit_code);
    // Two held, none acked: the next read takes nothing, and says why.
    var full = try node.cli.run(&.{ "stream", "group", "read", "s", "--group", "g", "--consumer", "w" });
    defer full.deinit();
    if (!full.contains("[overloaded/group_held_full]")) std.debug.print("expected [overloaded/group_held_full]: {s}{s}\n", .{ full.stdout, full.stderr });
    try testing.expect(full.contains("[overloaded/group_held_full]"));
    try testing.expect(full.contains("group 'g' on stream 's' is full: it holds 2 records (at most 2)"));
    try testing.expectEqual(@as(u8, 4), full.exit_code);
}
