//! A read on a new leader: what the old leader acknowledged is there, or the
//! read is refused for a retry. It is never answered as missing.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ClusterContext = stdx.testing.ClusterContext;

fn isLeader(cluster: *ClusterContext, node: usize) bool {
    var out = cluster.clis[node].?.run(&.{ "cluster", "status", "-o", "json" }) catch return false;
    defer out.deinit();
    return out.stdoutContains("\"role\":\"leader\"");
}

test "e2e/kv/cluster: a write the old leader acknowledged is never missing on the new leader" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .sync, .log_level = "info" });
    defer cluster.deinit();
    errdefer cluster.printLogs();

    for (0..3) |round| {
        // The leader, whichever it is now.
        var tries: usize = 0;
        const old: usize = while (true) : (tries += 1) {
            if (tries == 200) return error.NoLeader;
            const found: ?usize = for (0..3) |n| {
                if (cluster.getServer(n).?.process != null and isLeader(&cluster, n)) break n;
            } else null;
            if (found) |n| break n;
            stdx.time.sleep(50 * std.time.ns_per_ms);
        };
        var key_buf: [16]u8 = undefined;
        var val_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "acked-{d}", .{round});
        const want = try std.fmt.bufPrint(&val_buf, "\"value\":\"v{d}\"", .{round});
        const val = want[9 .. want.len - 1];
        var set = try cluster.clis[old].?.run(&.{ "kv", "set", key, val });
        defer set.deinit();
        try testing.expectEqual(@as(u8, 0), set.exit_code);

        // Acknowledged, and at once the leader is gone.
        cluster.killNode(old);

        // Whichever node leads next: the value, or a refusal to retry.
        var answered = false;
        tries = 0;
        while (!answered) : (tries += 1) {
            if (tries == 400) return error.NeverAnswered;
            for (0..3) |n| {
                if (n == old or !isLeader(&cluster, n)) continue;
                var got = try cluster.clis[n].?.run(&.{ "kv", "get", key, "--output", "json" });
                defer got.deinit();
                if (got.stdoutContains(want)) {
                    answered = true;
                } else if (!got.contains("leader_unknown")) {
                    // Deposed while asked: it's a follower now, which may lag.
                    if (!isLeader(&cluster, n)) continue;
                    std.debug.print("round {d}: node {d} leads and answered: {s}{s}\n", .{ round, n, got.stdout, got.stderr });
                    return error.ReadNotRefused;
                }
                break;
            }
            if (!answered) stdx.time.sleep(5 * std.time.ns_per_ms);
        }
        try cluster.restartNode(old);
    }
}
