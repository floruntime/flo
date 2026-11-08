//! A stream delete without `--force` is decided as its entry applies, in log
//! order, so an append racing it on another node is never lost: the delete
//! either goes first, on an empty stream, or is refused.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");
const ClusterContext = stdx.testing.ClusterContext;
const Client = src.cli_client.Client;
const proto = src.protocol.proto;

const ROUNDS = 150;
const ONE_RECORD = "\x01\x00\x00\x00" ++ "\x01\x00\x00\x00" ++ "a" ++ "\x00\x00";

fn leaderOf(cluster: *ClusterContext) !usize {
    for (0..200) |_| {
        for (0..3) |n| {
            var out = cluster.clis[n].?.run(&.{ "cluster", "status", "-o", "json" }) catch continue;
            defer out.deinit();
            if (out.stdoutContains("\"role\":\"leader\"")) return n;
        }
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return error.NoLeader;
}

/// One writer: a connection to one node, sending each round's op on its key as
/// soon as the round opens.
const Racer = struct {
    endpoint: []const u8,
    op: proto.OpCode,
    round: *std.atomic.Value(u32),
    arrived: *std.atomic.Value(u32),
    answers: [ROUNDS]proto.StatusCode = undefined,
    failed: bool = false,

    fn run(self: *Racer) void {
        self.write() catch {
            self.failed = true;
        };
    }

    fn write(self: *Racer) !void {
        var conn = Client.init(testing.allocator, self.endpoint);
        defer conn.deinit();
        try conn.connect();
        for (0..ROUNDS) |i| {
            _ = self.arrived.fetchAdd(1, .acq_rel);
            while (self.round.load(.acquire) <= i) std.atomic.spinLoopHint();
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "race-{d}", .{i});
            var r = try conn.sendRequest(self.op, "", key, if (self.op == .stream_append) ONE_RECORD else "");
            defer r.deinit();
            self.answers[i] = r.status;
            // Answered at once in an election: don't spend the rounds on it.
            if (r.status == .unavailable) stdx.time.sleep(100 * std.time.ns_per_ms);
        }
    }
};

test "e2e/stream-delete: an append racing a delete without --force on another node is never lost" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "up", "v" }, 40, 250);
    const leader = try leaderOf(&cluster);
    var round = std.atomic.Value(u32).init(0);
    var arrived = std.atomic.Value(u32).init(0);
    // The append on the leader, the delete forwarded to it: it arrives just
    // after, while the append has not yet applied.
    var appender: Racer = .{ .endpoint = cluster.endpoints[leader].?, .op = .stream_append, .round = &round, .arrived = &arrived };
    var deleter: Racer = .{ .endpoint = cluster.endpoints[(leader + 1) % 3].?, .op = .stream_delete, .round = &round, .arrived = &arrived };
    const ta = try std.Thread.spawn(.{}, Racer.run, .{&appender});
    const td = try std.Thread.spawn(.{}, Racer.run, .{&deleter});
    for (0..ROUNDS) |i| {
        while (arrived.load(.acquire) < (i + 1) * 2 and !appender.failed and !deleter.failed) std.atomic.spinLoopHint();
        round.store(@intCast(i + 1), .release);
    }
    ta.join();
    td.join();
    try testing.expect(!appender.failed and !deleter.failed);

    var reader = Client.init(testing.allocator, cluster.endpoints[leader].?);
    defer reader.deinit();
    try reader.connect();
    var decided: usize = 0;
    var refused: usize = 0;
    for (appender.answers, deleter.answers, 0..) |appended, deleted, i| {
        errdefer std.debug.print("round {d}: append {t}, delete {t}\n", .{ i, appended, deleted });
        if (appended != .ok) continue;
        decided += 1;
        // Either it applied first, so the stream wasn't empty, or after.
        try testing.expect(deleted == .ok or deleted == .conflict or deleted == .unavailable);
        if (deleted == .conflict) refused += 1;
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "race-{d}", .{i});
        var info = try reader.sendRequest(.stream_info, "", key, "");
        defer info.deinit();
        try testing.expectEqual(proto.StatusCode.ok, info.status);
        const data = info.asRawData() orelse return error.NoInfo;
        try testing.expectEqual(@as(u64, 1), std.mem.readInt(u64, data[32..40], .little));
    }
    try testing.expect(decided >= ROUNDS / 2);
    // The race was run, not won by one side every time.
    try testing.expect(refused > 0);
}
