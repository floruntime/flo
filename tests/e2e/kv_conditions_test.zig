//! KV's conditions are decided as each entry applies, in log order, so two
//! writers racing across a real cluster can't both pass, and a new leader's
//! answer counts what its predecessor committed.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");
const ClusterContext = stdx.testing.ClusterContext;
const Client = src.cli_client.Client;
const proto = src.protocol.proto;

const ROUNDS = 150;

fn isLeader(cluster: *ClusterContext, node: usize) bool {
    if (cluster.clis[node] == null) return false;
    var out = cluster.clis[node].?.run(&.{ "cluster", "status", "-o", "json" }) catch return false;
    defer out.deinit();
    return out.stdoutContains("\"role\":\"leader\"");
}

/// A node that leads, once one does.
fn leaderOf(cluster: *ClusterContext) !usize {
    for (0..200) |_| {
        for (0..3) |n| if (isLeader(cluster, n)) return n;
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return error.NoLeader;
}

const Answer = struct { status: proto.StatusCode, reason: proto.Reason };

const Mode = enum { if_absent, cas };

/// One writer: a connection to one node, writing each round's key as soon as
/// the round opens.
const Racer = struct {
    endpoint: []const u8,
    value: []const u8,
    mode: Mode,
    round: *std.atomic.Value(u32),
    arrived: *std.atomic.Value(u32),
    answers: [ROUNDS]Answer = undefined,
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
            const key = try std.fmt.bufPrint(&key_buf, "race-{t}-{d}", .{ self.mode, i });
            var r = try src.cli_client.kv.set(&conn, "", key, .{
                .value = self.value,
                .ttl_ms = null,
                .condition = if (self.mode == .if_absent) .if_absent else .always,
                .cas_version = if (self.mode == .cas) 1 else null,
                .routing_key = null,
                .txn = null,
            });
            defer r.deinit();
            self.answers[i] = .{ .status = r.status, .reason = r.reason() };
            // Answered at once in an election: don't spend the rounds on it.
            if (r.status == .unavailable) stdx.time.sleep(100 * std.time.ns_per_ms);
        }
    }
};

/// Two writers on two nodes, one the leader and one forwarding to it, race
/// for each key. Exactly one wins each; the other is told why.
fn race(cluster: *ClusterContext, mode: Mode, want: proto.Reason) !void {
    const leader = try leaderOf(cluster);
    const follower = (leader + 1) % 3;
    var round = std.atomic.Value(u32).init(0);
    var arrived = std.atomic.Value(u32).init(0);
    var a: Racer = .{ .endpoint = cluster.endpoints[leader].?, .value = "from-a", .mode = mode, .round = &round, .arrived = &arrived };
    var b: Racer = .{ .endpoint = cluster.endpoints[follower].?, .value = "from-b", .mode = mode, .round = &round, .arrived = &arrived };
    const ta = try std.Thread.spawn(.{}, Racer.run, .{&a});
    const tb = try std.Thread.spawn(.{}, Racer.run, .{&b});
    for (0..ROUNDS) |i| {
        while (arrived.load(.acquire) < (i + 1) * 2 and !a.failed and !b.failed) std.atomic.spinLoopHint();
        round.store(@intCast(i + 1), .release);
    }
    ta.join();
    tb.join();
    try testing.expect(!a.failed and !b.failed);

    var decided: usize = 0;
    for (a.answers, b.answers, 0..) |x, y, i| {
        const winners = @as(u32, @intFromBool(x.status == .ok)) + @intFromBool(y.status == .ok);
        errdefer std.debug.print("round {d}: {any} {any}\n", .{ i, x, y });
        try testing.expect(winners <= 1);
        // An election mid-race: that writer's answer says nothing of the key.
        if (x.status == .unavailable or y.status == .unavailable) continue;
        decided += 1;
        try testing.expectEqual(@as(u32, 1), winners);
        const loser = if (x.status == .ok) y else x;
        try testing.expectEqual(proto.StatusCode.conflict, loser.status);
        try testing.expectEqual(want, loser.reason);

        // Every node holds the winner's value.
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "race-{t}-{d}", .{ mode, i });
        const value = if (x.status == .ok) a.value else b.value;
        if (i % 25 == 0) for (0..3) |n| cluster.allocator.free(try cluster.pollUntilContains(n, &.{ "kv", "get", key }, value, 40, 100));
    }
    try testing.expect(decided >= ROUNDS / 2);
}

test "e2e/kv-conditions: two writers racing to set a key if absent: one wins, the other gets conflict" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "up", "v" }, 40, 250);
    try race(&cluster, .if_absent, .condition_failed);
}

test "e2e/kv-conditions: two writers racing with one cas_version: one wins, the other gets conflict" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "up", "v" }, 40, 250);
    // Every key at version 1, for both writers to name.
    const leader = try leaderOf(&cluster);
    var conn = Client.init(testing.allocator, cluster.endpoints[leader].?);
    defer conn.deinit();
    try conn.connect();
    for (0..ROUNDS) |i| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "race-cas-{d}", .{i});
        var r = try src.cli_client.kv.set(&conn, "", key, .{ .value = "start", .ttl_ms = null, .condition = .always, .cas_version = null, .routing_key = null, .txn = null });
        defer r.deinit();
        try testing.expectEqual(proto.StatusCode.ok, r.status);
    }
    try race(&cluster, .cas, .cas_mismatch);
}

test "e2e/kv-conditions: after failover, a condition counts every write the old leader acknowledged" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "up", "v" }, 40, 250);
    const leader = try leaderOf(&cluster);

    // Written as fast as they answer, up to the moment the leader goes.
    const WRITES = 2000;
    var conn = Client.init(testing.allocator, cluster.endpoints[leader].?);
    defer conn.deinit();
    try conn.connect();
    for (0..WRITES) |i| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "acked-{d}", .{i});
        var r = try src.cli_client.kv.set(&conn, "", key, .{ .value = "old-leader", .ttl_ms = null, .condition = .always, .cas_version = null, .routing_key = null, .txn = null });
        defer r.deinit();
        try testing.expectEqual(proto.StatusCode.ok, r.status);
    }
    cluster.killNode(leader);

    // At once, the newest first: those a new leader is likeliest not to have
    // applied. None may be taken as absent.
    const survivor = (leader + 1) % 3;
    var client = Client.init(testing.allocator, cluster.endpoints[survivor].?);
    defer client.deinit();
    try client.connect();
    var i: usize = WRITES;
    var checked: usize = 0;
    while (i > WRITES - 200) {
        i -= 1;
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "acked-{d}", .{i});
        for (0..400) |_| {
            var r = try src.cli_client.kv.set(&client, "", key, .{ .value = "new-writer", .ttl_ms = null, .condition = .if_absent, .cas_version = null, .routing_key = null, .txn = null });
            defer r.deinit();
            errdefer std.debug.print("{s}: status {t}\n", .{ key, r.status });
            try testing.expect(r.status != .ok);
            if (r.status == .unavailable or r.status == .overloaded) {
                stdx.time.sleep(50 * std.time.ns_per_ms);
                continue;
            }
            try testing.expectEqual(proto.StatusCode.conflict, r.status);
            try testing.expectEqual(proto.Reason.condition_failed, r.reason());
            checked += 1;
            break;
        }
    }
    try testing.expectEqual(@as(usize, 200), checked);
}
