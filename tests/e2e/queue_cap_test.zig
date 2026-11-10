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
            try testing.expect(out.contains("overloaded"));
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
    try node.enqueue("q", big[0..10_000], "queue 'q' is full: it holds 1 messages (at most 100000) and 60000 payload bytes (at most 65536)");
    try node.enqueue("q", big[0..5_536], null);
}

test "e2e/queue-cap: a lowered shard_held_bytes counts every queue on the shard" {
    const node = try Node.start(.{ .shards = 1, .extra_config = "[queues]\nshard_held_bytes = 65536\n" });
    defer node.deinit();
    errdefer node.server.dumpLogTail();
    const big = try testing.allocator.alloc(u8, 60_000);
    defer testing.allocator.free(big);
    @memset(big, 'x');
    try node.enqueue("a", big, null);
    try node.enqueue("b", big[0..10_000], "this shard's queues hold 60000 payload bytes, at most 65536");
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

test "e2e/queue-cap: nodes with other caps than the leader's apply its enqueues, and say so" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .sync, .node_config = .{
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
}
