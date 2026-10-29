//! A shard that reaches an entry every replica fails the same way halts
//! there: its writes are refused saying where and why, a write committed
//! past it is told its outcome is pending, its reads are still served, and
//! a restart comes up halted at the same entry. The halting entry comes
//! from a fault only a `-Dtest-faults` build carries; without one these
//! skip.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const test_faults = @import("src").node.test_faults;
const HALT_KEY = test_faults.HALT_KEY;

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
    // Every node stays halted, none diverged: the leader halted there too,
    // so it tells no one it halted alone.
    stdx.time.sleep(2 * std.time.ns_per_s);
    for (0..3) |i| {
        const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", cluster.getServer(i).?.metrics_port);
        defer http.deinit();
        const parsed = try nodeStatus(http);
        defer parsed.deinit();
        try testing.expectEqual(at, haltIndex(parsed, 0));
    }
}

test "e2e/halt: a follower that halts where its leader applied, on the same build, halted alone and is diverged; the group goes on" {
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

    const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", cluster.getServer(2).?.metrics_port);
    defer http.deinit();
    var tries: usize = 0;
    while (tries < 80) : (tries += 1) {
        const parsed = try nodeStatus(http);
        defer parsed.deinit();
        const s = parsed.value.object.get("shards").?.array.items[0].object;
        if (std.mem.eql(u8, s.get("state").?.string, "diverged")) {
            try testing.expect(std.mem.indexOf(u8, s.get("reason").?.string, "halted alone") != null);
            break;
        }
        stdx.time.sleep(250 * std.time.ns_per_ms);
    } else {
        cluster.printLogs();
        return error.NotDiverged;
    }
    // The write applied where it ran, and the group takes writes.
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "after", "taken" }, 20, 250);
    for (0..2) |i| {
        const out = try cluster.pollUntilContains(i, &.{ "kv", "get", "after" }, "taken", 40, 250);
        defer testing.allocator.free(out);
        const x = try cluster.execCaptureAnyOn(i, &.{ "kv", "get", HALT_KEY });
        defer testing.allocator.free(x);
        try testing.expectEqualStrings("x", std.mem.trim(u8, x, &std.ascii.whitespace));
    }
}
