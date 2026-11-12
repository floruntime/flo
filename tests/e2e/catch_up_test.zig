//! A follower that falls below its leader's ring catches up from the leader's
//! sealed segments, read off the leader's shard thread.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");
const proto = src.protocol.proto;
const Client = src.cli_client.Client;
const ClusterContext = stdx.testing.ClusterContext;

fn leaderOf(cluster: *ClusterContext) !usize {
    for (0..200) |_| {
        for (0..3) |n| {
            if (cluster.clis[n] == null) continue;
            var out = cluster.clis[n].?.run(&.{ "cluster", "status", "-o", "json" }) catch continue;
            defer out.deinit();
            if (out.stdoutContains("\"role\":\"leader\"")) return n;
        }
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return error.NoLeader;
}

test "e2e/catch-up: a follower stopped past the leader's ring catches up from its segments" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .sync, .tiered_log = .{ .hot_buffer_capacity = 2 * 1024 * 1024 } });
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "up", "v" }, 40, 250);
    const leader = try leaderOf(&cluster);
    const behind = (leader + 1) % 3;
    cluster.stopNode(behind);

    // About 8 MiB written while it is down: four times the leader's ring.
    var conn = Client.init(testing.allocator, cluster.endpoints[leader].?);
    defer conn.deinit();
    try conn.connect();
    const value = try testing.allocator.alloc(u8, 16 * 1024);
    defer testing.allocator.free(value);
    @memset(value, 'v');
    var key_buf: [32]u8 = undefined;
    const WRITES = 512;
    for (0..WRITES) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "k-{d}", .{i});
        var r = try src.cli_client.kv.set(&conn, "", key, .{ .value = value, .ttl_ms = null, .condition = .always, .cas_version = null, .txn = null });
        defer r.deinit();
        try testing.expectEqual(proto.StatusCode.ok, r.status);
    }
    var last_buf: [32]u8 = undefined;
    const last = try std.fmt.bufPrint(&last_buf, "k-{d}", .{WRITES - 1});
    try cluster.restartNode(behind);
    cluster.allocator.free(try cluster.pollUntilContains(behind, &.{ "kv", "get", last }, "vvvv", 120, 250));
    // And the first, which only the segments still held.
    cluster.allocator.free(try cluster.pollUntilContains(behind, &.{ "kv", "get", "k-0" }, "vvvv", 20, 250));
}
