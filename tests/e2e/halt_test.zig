//! A shard that reaches an entry every replica fails the same way halts
//! there: its writes are refused saying where and why, a write committed
//! past it is told its outcome is pending, its reads are still served, and
//! a restart comes up halted at the same entry. The halting entry comes
//! from a fault only a `-Dtest-faults` build carries; without one these
//! skip.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");
const test_faults = src.node.test_faults;
const HALT_KEY = test_faults.HALT_KEY;
const apply_marker = src.node.apply_marker;

/// A node's `/status`, parsed.
fn nodeStatus(http: anytype) !std.json.Parsed(std.json.Value) {
    var resp = try http.get("/status");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    return std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{});
}

fn haltIndex(parsed: std.json.Parsed(std.json.Value), shard: usize) ?i64 {
    const s = parsed.value.object.get("shards").?.array.items[shard].object;
    if (!std.mem.eql(u8, s.get("state").?.string, "halted")) return null;
    return s.get("halt_index").?.integer;
}

test "e2e/halt: a lone node's shard halts at the entry, refuses writes saying why, serves reads, and stays halted across a restart" {
    if (!test_faults.enabled) return error.SkipZigTest;
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 1, .durability = .sync, .metrics_enabled = true },
    });
    defer ctx.deinit();
    try ctx.exec(&.{ "kv", "set", "before", "kept" });

    // The halting write committed: it is pending, not refused, and must
    // not be resent.
    {
        var r = try ctx.execResult(&.{ "kv", "set", HALT_KEY, "x" });
        defer r.deinit();
        try testing.expectEqual(@as(u8, 6), r.exit_code);
        try testing.expect(std.mem.indexOf(u8, r.stderr, "halted at your write") != null);
        try testing.expect(std.mem.indexOf(u8, r.stderr, "it will not run") != null);
        try testing.expect(std.mem.indexOf(u8, r.stderr, "[outcome_pending/shard_halted]") != null);
    }
    // Every later write is refused with where and why.
    {
        var r = try ctx.execResult(&.{ "kv", "set", "after", "v" });
        defer r.deinit();
        try testing.expectEqual(@as(u8, 3), r.exit_code);
        try testing.expect(std.mem.indexOf(u8, r.stderr, "shard 0 is halted at entry") != null);
        try testing.expect(std.mem.indexOf(u8, r.stderr, "Restarting won't help") != null);
        try testing.expect(std.mem.indexOf(u8, r.stderr, "[unavailable/shard_halted]") != null);
    }
    // Reads are served from the state before the halt.
    try testing.expectEqualStrings("kept", try ctx.execCapture(&.{ "kv", "get", "before" }));

    var metrics = try ctx.createMetricsHttp();
    const before = try nodeStatus(metrics);
    defer before.deinit();
    const at = haltIndex(before, 0) orelse return error.NotHalted;
    try testing.expect(std.mem.indexOf(u8, before.value.object.get("shards").?.array.items[0].object.get("reason").?.string, "its applier refused it") != null);
    // A halted shard keeps the node ready: every replica halts with it.
    try testing.expect(before.value.object.get("ready").?.bool);
    metrics.deinit();

    try ctx.restartServer();
    metrics = try ctx.createMetricsHttp();
    defer metrics.deinit();
    const after = try nodeStatus(metrics);
    defer after.deinit();
    try testing.expectEqual(@as(?i64, at), haltIndex(after, 0));
    {
        var r = try ctx.execResult(&.{ "kv", "set", "after", "v" });
        defer r.deinit();
        try testing.expectEqual(@as(u8, 3), r.exit_code);
        try testing.expect(std.mem.indexOf(u8, r.stderr, "[unavailable/shard_halted]") != null);
    }
    try testing.expectEqualStrings("kept", try ctx.execCapture(&.{ "kv", "get", "before" }));
}

/// The node whose member list has heard every member halted at `at`: the
/// leader, which alone hears the others' halts.
fn leaderHeardHalts(cluster: anytype, at: i64) !usize {
    var needle_buf: [48]u8 = undefined;
    const needle = try std.fmt.bufPrint(&needle_buf, "\"halt\":{{\"index\":{d},", .{at});
    var tries: usize = 0;
    while (tries < 80) : (tries += 1) {
        for (0..3) |i| {
            const out = cluster.execCaptureAnyOn(i, &.{ "cluster", "members", "-o", "json" }) catch continue;
            defer testing.allocator.free(out);
            if (std.mem.count(u8, out, needle) == 3) return i;
        }
        stdx.time.sleep(250 * std.time.ns_per_ms);
    }
    cluster.printLogs();
    return error.NoLeaderHeardAllHalts;
}

/// `flo cluster repair skip` through node `node`'s admin socket.
fn repairSkip(cluster: anytype, node: usize, at: i64, yes: bool) !stdx.testing.CommandResult {
    var index_buf: [24]u8 = undefined;
    const index = try std.fmt.bufPrint(&index_buf, "{d}", .{at});
    const config = cluster.getServer(node).?.config_file;
    const cli = cluster.clis[node].?;
    return if (yes)
        cli.run(&.{ "cluster", "repair", "skip", "--index", index, "--config", config, "--reason", "e2e", "--yes" })
    else
        cli.run(&.{ "cluster", "repair", "skip", "--index", index, "--config", config });
}

test "e2e/halt: every node of a three-node group halts at the same entry" {
    if (!test_faults.enabled) return error.SkipZigTest;
    var cluster = try stdx.testing.ClusterContext.init(testing.allocator, .{ .node_count = 3, .metrics_enabled = true });
    defer cluster.deinit();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "before", "kept" }, 20, 250);

    // Written through any node, it reaches the leader and commits there.
    {
        const cli = cluster.clis[0].?;
        var r = try cli.run(&.{ "kv", "set", HALT_KEY, "x" });
        defer r.deinit();
        try testing.expectEqual(@as(u8, 6), r.exit_code);
    }

    // Each node halts at the same entry, as its own apply reaches it.
    var at: ?i64 = null;
    for (0..3) |i| {
        const port = cluster.getServer(i).?.metrics_port;
        const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", port);
        defer http.deinit();
        var tries: usize = 0;
        const index = while (tries < 80) : (tries += 1) {
            const parsed = try nodeStatus(http);
            defer parsed.deinit();
            if (haltIndex(parsed, 0)) |h| break h;
            stdx.time.sleep(250 * std.time.ns_per_ms);
        } else {
            cluster.printLogs();
            return error.NodeNotHalted;
        };
        if (at) |a| try testing.expectEqual(a, index) else at = index;
    }
    // Reads are served on every node.
    for (0..3) |i| {
        const out = try cluster.execCaptureAnyOn(i, &.{ "kv", "get", "before" });
        defer testing.allocator.free(out);
        try testing.expectEqualStrings("kept", std.mem.trim(u8, out, &std.ascii.whitespace));
    }
    // None diverged: the leader halted there too, so it told no one it
    // halted alone.
    for (0..3) |i| {
        const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", cluster.getServer(i).?.metrics_port);
        defer http.deinit();
        const parsed = try nodeStatus(http);
        defer parsed.deinit();
        try testing.expectEqual(at, haltIndex(parsed, 0));
    }
    // And the leader has heard all three halts.
    _ = try leaderHeardHalts(&cluster, at.?);
}

test "e2e/halt: a repair from any member's admin socket skips the entry the group halted at; every node goes on past it, across a restart" {
    if (!test_faults.enabled) return error.SkipZigTest;
    var cluster = try stdx.testing.ClusterContext.init(testing.allocator, .{ .node_count = 3, .metrics_enabled = true });
    defer cluster.deinit();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "before", "kept" }, 20, 250);
    {
        var r = try cluster.clis[0].?.run(&.{ "kv", "set", HALT_KEY, "x" });
        defer r.deinit();
        try testing.expectEqual(@as(u8, 6), r.exit_code);
    }
    var at: i64 = 0;
    {
        const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", cluster.getServer(0).?.metrics_port);
        defer http.deinit();
        var tries: usize = 0;
        at = while (tries < 80) : (tries += 1) {
            const parsed = try nodeStatus(http);
            defer parsed.deinit();
            if (haltIndex(parsed, 0)) |h| break h;
            stdx.time.sleep(250 * std.time.ns_per_ms);
        } else return error.NodeNotHalted;
    }
    const leader = try leaderHeardHalts(&cluster, at);
    const follower = (leader + 1) % 3;

    // Only its owner may connect to the socket.
    const st = try stdx.fs.statFile(cluster.getServer(follower).?.admin_socket, .{});
    try testing.expectEqual(@as(u32, 0o600), @as(u32, @intCast(@backingInt(st.permissions))) & 0o777);

    // The dry run shows the entry and who halted there, and changes nothing.
    {
        var r = try repairSkip(&cluster, follower, at, false);
        defer r.deinit();
        if (r.exit_code != 0) std.debug.print("dry run: {s}{s}\n", .{ r.stdout, r.stderr });
        try testing.expectEqual(@as(u8, 0), r.exit_code);
        try testing.expect(r.stdoutContains("of 3 voters"));
        try testing.expect(r.stdoutContains("Would append a repair"));
    }
    _ = try leaderHeardHalts(&cluster, at);

    // Over a client's TCP connection it is refused as admin-only, unrun.
    {
        var conn = src.cli_client.Client.init(testing.allocator, cluster.getNodeEndpoint(follower).?);
        defer conn.deinit();
        try conn.connect();
        var body: [9]u8 = undefined;
        std.mem.writeInt(u64, body[0..8], @intCast(at), .little);
        body[8] = 1;
        var resp = try conn.sendRequest(.cluster_repair_skip, "", "", &body);
        defer resp.deinit();
        try testing.expectEqual(src.protocol.proto.StatusCode.unauthorized, resp.status);
        try testing.expectEqual(src.protocol.proto.Reason.admin_only, resp.reason());
        try testing.expect(std.mem.indexOf(u8, resp.errorMessage(), "admin socket") != null);
    }
    _ = try leaderHeardHalts(&cluster, at);

    // With --yes, from a follower: forwarded to the leader, and committed.
    {
        var r = try repairSkip(&cluster, follower, at, true);
        defer r.deinit();
        if (r.exit_code != 0) std.debug.print("repair: {s}{s}\n", .{ r.stdout, r.stderr });
        try testing.expectEqual(@as(u8, 0), r.exit_code);
        try testing.expect(r.stdoutContains("is skipped (repair at index"));
    }
    // Every node goes on past it: the write never ran, and new ones are taken.
    for (0..3) |i| {
        const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", cluster.getServer(i).?.metrics_port);
        defer http.deinit();
        var tries: usize = 0;
        while (tries < 80) : (tries += 1) {
            const parsed = try nodeStatus(http);
            defer parsed.deinit();
            if (haltIndex(parsed, 0) == null) break;
            stdx.time.sleep(250 * std.time.ns_per_ms);
        } else {
            cluster.printLogs();
            return error.StillHalted;
        }
    }
    try cluster.execOnWithRetry(follower, &.{ "kv", "set", "after", "taken" }, 20, 250);
    for (0..3) |i| {
        const out = try cluster.pollUntilContains(i, &.{ "kv", "get", "after" }, "taken", 40, 250);
        testing.allocator.free(out);
        var r = try cluster.clis[i].?.run(&.{ "kv", "get", HALT_KEY });
        defer r.deinit();
        try testing.expect(r.exit_code != 0 and !r.stdoutContains("x"));
    }
    // A restart passes over it the same way.
    cluster.stopNode(follower);
    try cluster.restartNode(follower);
    const out = try cluster.pollUntilContains(follower, &.{ "kv", "get", "after" }, "taken", 40, 250);
    testing.allocator.free(out);
    {
        const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", cluster.getServer(follower).?.metrics_port);
        defer http.deinit();
        const parsed = try nodeStatus(http);
        defer parsed.deinit();
        try testing.expectEqual(@as(?i64, null), haltIndex(parsed, 0));
    }
}

test "e2e/halt: a follower that halts where its leader applied halted alone and is diverged (a build with no version says it can't tell); the group goes on" {
    if (!test_faults.enabled) return error.SkipZigTest;
    const off: []const [2][]const u8 = &.{.{ test_faults.OFF_ENV, "1" }};
    var env: @FieldType(stdx.testing.ClusterConfig, "node_env") = @splat(&.{});
    // Node 2 alone halts at the fault; the seed, which leads, applies it.
    env[0] = off;
    env[1] = off;
    var cluster = try stdx.testing.ClusterContext.init(testing.allocator, .{ .node_count = 3, .metrics_enabled = true, .node_env = env });
    defer cluster.deinit();
    testing.allocator.free(try cluster.pollUntilContains(0, &.{ "cluster", "status", "-o", "json" }, "\"role\":\"leader\"", 40, 250));
    try cluster.execOnWithRetry(0, &.{ "kv", "set", HALT_KEY, "x" }, 20, 250);

    // A build with a version knows the leader's is its own: halted alone,
    // so diverged. A dev or dirty one can't tell, so it stays halted and
    // says both ways out.
    const versioned = apply_marker.build_hash != 0;
    const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", cluster.getServer(2).?.metrics_port);
    defer http.deinit();
    var tries: usize = 0;
    while (tries < 80) : (tries += 1) {
        const parsed = try nodeStatus(http);
        defer parsed.deinit();
        const s = parsed.value.object.get("shards").?.array.items[0].object;
        const state = s.get("state").?.string;
        const why = if (s.get("reason")) |r| r.string else "";
        if (versioned and std.mem.eql(u8, state, "diverged")) {
            try testing.expect(std.mem.indexOf(u8, why, "halted alone") != null);
            break;
        }
        if (!versioned and std.mem.eql(u8, state, "halted") and std.mem.indexOf(u8, why, "can't be told apart") != null) break;
        try testing.expect(!std.mem.eql(u8, state, "diverged") or versioned);
        stdx.time.sleep(250 * std.time.ns_per_ms);
    } else {
        cluster.printLogs();
        return error.NoVerdict;
    }
    // The write applied where it ran, and the group takes writes.
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "after", "taken" }, 20, 250);
    // A repair of it is refused: the leader isn't halted there.
    if (!versioned) {
        const parsed = try nodeStatus(http);
        defer parsed.deinit();
        const at = haltIndex(parsed, 0).?;
        for (0..3) |i| {
            var r = try repairSkip(&cluster, i, at, true);
            defer r.deinit();
            try testing.expectEqual(@as(u8, 3), r.exit_code);
            try testing.expect(std.mem.indexOf(u8, r.stderr, "Upgrade them") != null or std.mem.indexOf(u8, r.stderr, "not halted") != null);
        }
    }
    for (0..2) |i| {
        const out = try cluster.pollUntilContains(i, &.{ "kv", "get", "after" }, "taken", 40, 250);
        defer testing.allocator.free(out);
        const x = try cluster.execCaptureAnyOn(i, &.{ "kv", "get", HALT_KEY });
        defer testing.allocator.free(x);
        try testing.expectEqualStrings("x", std.mem.trim(u8, x, &std.ascii.whitespace));
    }
}
