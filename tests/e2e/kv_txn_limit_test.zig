//! `[kv] max_txn_ops` and `max_txn_bytes`, and their flags: what a transaction
//! may hold. The leader that buffers it decides; replicas apply what it
//! committed, whatever their own values.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ClusterContext = stdx.testing.ClusterContext;
const CliRunner = stdx.testing.CliRunner;
const ServerProcess = stdx.testing.ServerProcess;

const TOO_LARGE = "/txn_too_large]";

/// A transaction begun through `cli`, its id in `buf`. Null if the node
/// refused, as it does with no leader.
fn begin(cli: *CliRunner, routing: []const u8, buf: []u8) !?[]const u8 {
    var out = try cli.run(&.{ "kv", "txn", "begin", routing });
    defer out.deinit();
    const prefix = "txn: ";
    const at = std.mem.indexOf(u8, out.stdout, prefix) orelse return null;
    var end = at + prefix.len;
    while (end < out.stdout.len and std.ascii.isDigit(out.stdout[end])) end += 1;
    return try std.fmt.bufPrint(buf, "{s}", .{out.stdout[at + prefix.len .. end]});
}

const Fill = struct { accepted: usize, too_large: bool };

/// Puts `<prefix><i>` = `v<i>` into the transaction until `want` are in or one
/// is refused for the transaction's size. Null on any other refusal.
fn fill(cli: *CliRunner, txn: []const u8, routing: []const u8, prefix: []const u8, want: usize) !?Fill {
    for (0..want) |i| {
        var key_buf: [32]u8 = undefined;
        var val_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "{s}{d}", .{ prefix, i });
        const val = try std.fmt.bufPrint(&val_buf, "v{d}", .{i});
        var out = try cli.run(&.{ "kv", "set", key, val, "--routing-key", routing, "--txn", txn });
        defer out.deinit();
        if (out.contains(TOO_LARGE)) return .{ .accepted = i, .too_large = true };
        if (out.exit_code == 0) continue;
        std.debug.print("put {d} refused: {s}\n", .{ i, out.stderr });
        return null;
    }
    return .{ .accepted = want, .too_large = false };
}

/// The transaction takes exactly `limit` ops, refuses the next for its size,
/// and commits what it took.
fn expectLimit(cli: *CliRunner, limit: usize) !void {
    var buf: [32]u8 = undefined;
    const txn = (try begin(cli, "r", &buf)) orelse return error.BeginRefused;
    const got = (try fill(cli, txn, "r", "k", limit + 1)) orelse return error.PutRefused;
    try testing.expectEqual(limit, got.accepted);
    try testing.expect(got.too_large);
    var commit = try cli.run(&.{ "kv", "txn", "commit", txn });
    defer commit.deinit();
    try testing.expectEqual(@as(u8, 0), commit.exit_code);
    var ops_buf: [32]u8 = undefined;
    try testing.expect(commit.stdoutContains(try std.fmt.bufPrint(&ops_buf, "ops: {d}", .{limit})));
}

fn expectLimitOn(config: ServerProcess.ServerConfig, limit: usize) !void {
    const node = try ServerProcess.initWithConfig(testing.allocator, config);
    defer node.deinit();
    try node.start();
    errdefer node.dumpLogTail();
    const ep = try node.getEndpoint(testing.allocator);
    defer testing.allocator.free(ep);
    const cli = try CliRunner.init(testing.allocator, node.flo_binary, ep);
    defer cli.deinit();
    try expectLimit(cli, limit);
}

test "e2e/kv/txn-limit: a transaction holds 32 ops unless the config says otherwise, lower or higher" {
    try expectLimitOn(.{ .shards = 1 }, 32);
    try expectLimitOn(.{ .shards = 1, .extra_config = "[kv]\nmax_txn_ops = 8\n" }, 8);
    try expectLimitOn(.{ .shards = 1, .extra_config = "[kv]\nmax_txn_ops = 40\n" }, 40);
}

test "e2e/kv/txn-limit: --max-txn-ops moves the limit, and beats the config file" {
    try expectLimitOn(.{ .shards = 1, .extra_args = &.{ "--max-txn-ops", "4" } }, 4);
    try expectLimitOn(.{ .shards = 1, .extra_args = &.{ "--max-txn-ops", "36" } }, 36);
    try expectLimitOn(.{ .shards = 1, .extra_config = "[kv]\nmax_txn_ops = 40\n", .extra_args = &.{ "--max-txn-ops", "6" } }, 6);
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

/// A transaction takes `fits` values of `size` bytes each and refuses the
/// next for its size.
fn expectBytes(config: ServerProcess.ServerConfig, size: usize, fits: usize) !void {
    const node = try ServerProcess.initWithConfig(testing.allocator, config);
    defer node.deinit();
    try node.start();
    errdefer node.dumpLogTail();
    const ep = try node.getEndpoint(testing.allocator);
    defer testing.allocator.free(ep);
    const cli = try CliRunner.init(testing.allocator, node.flo_binary, ep);
    defer cli.deinit();
    const value = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(value);
    @memset(value, 'x');
    var buf: [32]u8 = undefined;
    const txn = (try begin(cli, "r", &buf)) orelse return error.BeginRefused;
    for (0..fits + 1) |i| {
        var key_buf: [16]u8 = undefined;
        var out = try cli.run(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "b{d}", .{i}), value, "--routing-key", "r", "--txn", txn });
        defer out.deinit();
        if (i == fits) {
            if (!out.contains(TOO_LARGE)) std.debug.print("put {d}: {s}\n", .{ i, out.stderr });
            try testing.expect(out.contains(TOO_LARGE));
        } else {
            if (out.exit_code != 0 or out.contains(TOO_LARGE)) std.debug.print("put {d}: {s}\n", .{ i, out.stderr });
            try testing.expectEqual(@as(u8, 0), out.exit_code);
        }
    }
    var commit = try cli.run(&.{ "kv", "txn", "commit", txn });
    defer commit.deinit();
    try testing.expectEqual(@as(u8, 0), commit.exit_code);
}

test "e2e/kv/txn-limit: a transaction holds 256 KiB unless the config says otherwise, lower or higher" {
    // One argument stays under what Linux lets a process take.
    try expectBytes(.{ .shards = 1 }, 100 * 1024, 2);
    try expectBytes(.{ .shards = 1, .extra_config = "[kv]\nmax_txn_bytes = 4096\n" }, 1500, 2);
    try expectBytes(.{ .shards = 1, .extra_config = "[kv]\nmax_txn_bytes = 524288\n" }, 100 * 1024, 5);
}

test "e2e/kv/txn-limit: --max-txn-bytes moves the limit, and beats the config file" {
    try expectBytes(.{ .shards = 1, .extra_args = &.{ "--max-txn-bytes", "2048" } }, 1500, 1);
    try expectBytes(.{ .shards = 1, .extra_args = &.{ "--max-txn-bytes", "409600" } }, 100 * 1024, 3);
    try expectBytes(.{ .shards = 1, .extra_config = "[kv]\nmax_txn_bytes = 524288\n", .extra_args = &.{ "--max-txn-bytes", "2048" } }, 1500, 1);
}

test "e2e/kv/txn-limit: a max_txn_bytes outside 1 byte to 1 MiB is refused at start, not clamped" {
    try expectRefused(.{ .shards = 1, .extra_config = "[kv]\nmax_txn_bytes = 0\n" }, "[kv] max_txn_bytes = 0: must be 1 to 1048576");
    try expectRefused(.{ .shards = 1, .extra_config = "[kv]\nmax_txn_bytes = 1048577\n" }, "[kv] max_txn_bytes = 1048577: must be 1 to 1048576");
    try expectRefused(.{ .shards = 1, .extra_args = &.{ "--max-txn-bytes", "0" } }, "--max-txn-bytes 0: must be 1 to 1048576");
    try expectRefused(.{ .shards = 1, .extra_args = &.{ "--max-txn-bytes", "1048577" } }, "--max-txn-bytes 1048577: must be 1 to 1048576");
}

test "e2e/kv/txn-limit: every shard of a node takes both settings" {
    const node = try ServerProcess.initWithConfig(testing.allocator, .{ .shards = 4, .extra_config = "[kv]\nmax_txn_ops = 3\nmax_txn_bytes = 2048\n" });
    defer node.deinit();
    try node.start();
    errdefer node.dumpLogTail();
    const ep = try node.getEndpoint(testing.allocator);
    defer testing.allocator.free(ep);
    const cli = try CliRunner.init(testing.allocator, node.flo_binary, ep);
    defer cli.deinit();
    const value = try testing.allocator.alloc(u8, 1500);
    defer testing.allocator.free(value);
    @memset(value, 'x');

    // Routing keys until a transaction has been refused on each shard, which
    // a transaction's id names in its top 16 bits.
    var seen: [4]bool = @splat(false);
    var attempt: usize = 0;
    while (std.mem.indexOfScalar(bool, &seen, false) != null) : (attempt += 1) {
        if (attempt == 64) return error.ShardNeverReached;
        var routing_buf: [16]u8 = undefined;
        const routing = try std.fmt.bufPrint(&routing_buf, "r{d}", .{attempt});
        var buf: [32]u8 = undefined;
        const txn = (try begin(cli, routing, &buf)) orelse return error.BeginRefused;
        const shard: usize = @intCast((try std.fmt.parseInt(u64, txn, 10)) >> 48);
        if (seen[shard]) continue;
        seen[shard] = true;
        // Ops: three small ones, and the fourth refused.
        const got = (try fill(cli, txn, routing, "s", 4)) orelse return error.PutRefused;
        try testing.expectEqual(@as(usize, 3), got.accepted);
        try testing.expect(got.too_large);
        // Bytes: one large value, and the next refused.
        var byte_buf: [32]u8 = undefined;
        const bytes = (try begin(cli, routing, &byte_buf)) orelse return error.BeginRefused;
        var first = try cli.run(&.{ "kv", "set", "l0", value, "--routing-key", routing, "--txn", bytes });
        defer first.deinit();
        try testing.expectEqual(@as(u8, 0), first.exit_code);
        var second = try cli.run(&.{ "kv", "set", "l1", value, "--routing-key", routing, "--txn", bytes });
        defer second.deinit();
        try testing.expect(second.contains(TOO_LARGE));
    }
}

test "e2e/kv/txn-limit: a max_txn_ops outside 1 to 256 is refused at start, not clamped" {
    try expectRefused(.{ .shards = 1, .extra_config = "[kv]\nmax_txn_ops = 0\n" }, "[kv] max_txn_ops = 0: must be 1 to 256");
    try expectRefused(.{ .shards = 1, .extra_config = "[kv]\nmax_txn_ops = 257\n" }, "[kv] max_txn_ops = 257: must be 1 to 256");
    try expectRefused(.{ .shards = 1, .extra_args = &.{ "--max-txn-ops", "0" } }, "--max-txn-ops 0: must be 1 to 256");
    try expectRefused(.{ .shards = 1, .extra_args = &.{ "--max-txn-ops", "257" } }, "--max-txn-ops 257: must be 1 to 256");
}

fn isLeader(cluster: *ClusterContext, node: usize) bool {
    if (cluster.clis[node] == null) return false;
    var out = cluster.clis[node].?.run(&.{ "cluster", "status", "-o", "json" }) catch return false;
    defer out.deinit();
    return out.stdoutContains("\"role\":\"leader\"");
}

/// Every one of `count` keys reads its own value on `node`.
fn expectData(cluster: *ClusterContext, node: usize, count: usize) !void {
    for (0..count) |i| {
        var key_buf: [32]u8 = undefined;
        var want_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "m{d}", .{i});
        const want = try std.fmt.bufPrint(&want_buf, "\"value\":\"v{d}\"", .{i});
        cluster.allocator.free(try cluster.pollUntilContains(node, &.{ "kv", "get", key, "--routing-key", "r", "--output", "json" }, want, 80, 100));
    }
}

test "e2e/kv/txn-limit: nodes with different max_txn_ops apply what a leader committed, and the leader's value decides" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .sync, .node_config = .{
        "[kv]\nmax_txn_ops = 64\n",
        "[kv]\nmax_txn_ops = 32\n",
        "[kv]\nmax_txn_ops = 16\n",
        null,
        null,
    } });
    defer cluster.deinit();
    errdefer cluster.printLogs();

    // The test needs the node with the highest value leading.
    var moves: usize = 0;
    while (!isLeader(&cluster, 0)) : (moves += 1) {
        if (moves == 5) return error.NodeZeroNeverLed;
        const other: usize = if (isLeader(&cluster, 1)) 1 else 2;
        cluster.stopNode(other);
        stdx.time.sleep(1500 * std.time.ns_per_ms);
        try cluster.restartNode(other);
        stdx.time.sleep(1500 * std.time.ns_per_ms);
    }

    // More ops than either follower's value allows.
    var buf: [32]u8 = undefined;
    const txn = (try begin(cluster.clis[0].?, "r", &buf)) orelse return error.BeginRefused;
    const got = (try fill(cluster.clis[0].?, txn, "r", "m", 40)) orelse return error.PutRefused;
    try testing.expectEqual(@as(usize, 40), got.accepted);
    var commit = try cluster.clis[0].?.run(&.{ "kv", "txn", "commit", txn });
    defer commit.deinit();
    try testing.expectEqual(@as(u8, 0), commit.exit_code);
    try testing.expect(commit.stdoutContains("ops: 40"));
    for (0..3) |node| try expectData(&cluster, node, 40);

    // A node with a lower value takes over: the same size is now refused, at
    // its own value. Leadership moves until each of the two has led, so a
    // joiner's value can't go unread because the other won.
    const LIMITS = [_]usize{ 64, 32, 16 };
    cluster.killNode(0);
    var down: usize = 0;
    var led: [3]bool = .{ true, false, false };
    var tries: usize = 0;
    while (!(led[1] and led[2])) : (tries += 1) {
        if (tries == 400) return error.NodeNeverLed;
        stdx.time.sleep(250 * std.time.ns_per_ms);
        const leader: usize = for (0..3) |n| {
            if (n != down and isLeader(&cluster, n)) break n;
        } else continue;
        if (!led[leader]) {
            var again_buf: [32]u8 = undefined;
            const again = (try begin(cluster.clis[leader].?, "r", &again_buf)) orelse continue;
            // A leader still settling refuses otherwise: try again.
            const next = (try fill(cluster.clis[leader].?, again, "r", "n", 40)) orelse continue;
            // If leadership moved meanwhile, this measured another node.
            if (!isLeader(&cluster, leader)) continue;
            try testing.expect(next.too_large);
            try testing.expectEqual(LIMITS[leader], next.accepted);
            var rollback = try cluster.clis[leader].?.run(&.{ "kv", "txn", "rollback", again });
            defer rollback.deinit();
            led[leader] = true;
            // What the old leader committed is all still there.
            for (0..3) |n| if (n != down) try expectData(&cluster, n, 40);
        }
        if (led[1] and led[2]) break;
        // The node that was down comes back, and this leader goes.
        try cluster.restartNode(down);
        cluster.stopNode(leader);
        down = leader;
    }
}
