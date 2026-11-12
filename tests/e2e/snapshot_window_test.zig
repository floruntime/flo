//! A snapshot bar suspends apply on every replica while Raft runs on: writes
//! keep committing and are answered once apply resumes, and leadership holds.
//! Until snapshots exist a window opens only at a test fault's entry, so these
//! skip in a build without `-Dtest-faults`.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");
const test_faults = src.node.test_faults;

const WINDOW_KEY = test_faults.Fault.snapshot_window.key();

fn shardStatus(cluster: *stdx.testing.ClusterContext, node: usize) !std.json.Parsed(std.json.Value) {
    const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", cluster.getServer(node).?.metrics_port);
    defer http.deinit();
    var resp = try http.get("/status");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    return std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{});
}

fn shard0(parsed: std.json.Parsed(std.json.Value)) std.json.ObjectMap {
    return parsed.value.object.get("shards").?.array.items[0].object;
}

/// The node whose shard 0 leads, waiting for one.
fn leaderNode(cluster: *stdx.testing.ClusterContext) !usize {
    for (0..80) |_| {
        for (0..3) |i| {
            const parsed = try shardStatus(cluster, i);
            defer parsed.deinit();
            if (std.mem.eql(u8, shard0(parsed).get("role").?.string, "leader")) return i;
        }
        stdx.time.sleep(250 * std.time.ns_per_ms);
    }
    return error.NoLeader;
}

test "e2e/snapshot: on a three-node group every node suspends apply at the bar; a write during it is answered once apply resumes, and the leader keeps leading" {
    if (!test_faults.enabled) return error.SkipZigTest;
    var cluster = try stdx.testing.ClusterContext.init(testing.allocator, .{ .node_count = 3, .metrics_enabled = true });
    defer cluster.deinit();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "before", "kept" }, 20, 250);
    const leader = try leaderNode(&cluster);

    // The bar: a set every replica applies, then holds apply for a while.
    // The CLI exits 0 on a refusal too, so each write is read back exactly.
    try cluster.execOn(leader, &.{ "kv", "set", WINDOW_KEY, "bar-value-7f3c" });

    // Each node shows the same bar while it holds apply.
    var bar: ?i64 = null;
    for (0..3) |i| {
        const seen = for (0..60) |_| {
            const parsed = try shardStatus(&cluster, i);
            defer parsed.deinit();
            if (shard0(parsed).get("snapshot")) |w| break w.object.get("bar").?.integer;
            stdx.time.sleep(25 * std.time.ns_per_ms);
        } else {
            cluster.printLogs();
            return error.NoWindow;
        };
        if (bar) |b| try testing.expectEqual(b, seen) else bar = seen;
    }

    // Sent while the leader holds apply, it commits, waits, and is answered.
    try cluster.execOn(leader, &.{ "kv", "set", "during", "written-while-suspended-91ab" });
    for (0..3) |i| {
        const out = try cluster.pollAnyUntilContains(i, &.{ "kv", "get", "during" }, "written-while-suspended-91ab", 40, 250);
        defer testing.allocator.free(out);
        try testing.expectEqualStrings("written-while-suspended-91ab", std.mem.trim(u8, out, &std.ascii.whitespace));
        const bar_out = try cluster.execCaptureAnyOn(i, &.{ "kv", "get", WINDOW_KEY });
        defer testing.allocator.free(bar_out);
        try testing.expectEqualStrings("bar-value-7f3c", std.mem.trim(u8, bar_out, &std.ascii.whitespace));
    }

    // Same leader, and every window closed with apply caught up.
    try testing.expectEqual(leader, try leaderNode(&cluster));
    for (0..3) |i| {
        for (0..40) |_| {
            const parsed = try shardStatus(&cluster, i);
            defer parsed.deinit();
            const s = shard0(parsed);
            if (s.get("snapshot") == null and s.get("last_applied").?.integer == s.get("commit_index").?.integer) break;
            stdx.time.sleep(100 * std.time.ns_per_ms);
        } else {
            cluster.printLogs();
            return error.WindowNotClosed;
        }
    }
}
