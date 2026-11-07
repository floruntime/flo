//! A write on a new leader that its handler chooses by reading state: what the
//! old leader acknowledged counts, or the write is refused for a retry. It is
//! never chosen as though that hadn't happened.
//!
//! The window is milliseconds long, so the writes go over sockets held open from
//! before the kill: a CLI process per write arrives after it has closed.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");
const proto = src.protocol.proto;
const Client = src.cli_client.Client;
const ClusterContext = stdx.testing.ClusterContext;

const STREAMS = 400;
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

/// Ask on `client` until something other than a retryable refusal answers.
fn settled(client: *Client, op: proto.OpCode, key: []const u8, value: []const u8) !src.cli_client.Response {
    for (0..600) |_| {
        const r = try client.sendRequest(op, "", key, value);
        if (r.status != .unavailable and r.status != .overloaded) return r;
        var done = r;
        done.deinit();
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }
    return error.NeverSettled;
}

test "e2e/leader-write: after failover, a delete without --force never drops a stream the old leader acknowledged" {
    var cluster = try ClusterContext.init(testing.allocator, .{ .node_count = 3, .durability = .sync });
    defer cluster.deinit();
    errdefer cluster.printLogs();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "up", "v" }, 40, 250);
    const old = try leaderOf(&cluster);

    var survivors: [2]Client = undefined;
    for (&survivors, 1..) |*c, i| {
        c.* = Client.init(testing.allocator, cluster.endpoints[(old + i) % 3].?);
        try c.connect();
    }
    defer for (&survivors) |*c| c.deinit();

    var writer = Client.init(testing.allocator, cluster.endpoints[old].?);
    defer writer.deinit();
    try writer.connect();
    var key_buf: [32]u8 = undefined;
    for (0..STREAMS) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "acked-{d}", .{i});
        var r = try writer.sendRequest(.stream_append, "", key, ONE_RECORD);
        defer r.deinit();
        try testing.expectEqual(proto.StatusCode.ok, r.status);
    }
    cluster.killNode(old);

    // Newest first: those a new leader is likeliest not to have applied. One
    // survivor leads and the other forwards, so both paths are asked.
    var i: usize = STREAMS;
    while (i > 0) {
        i -= 1;
        const key = try std.fmt.bufPrint(&key_buf, "acked-{d}", .{i});
        var r = try settled(&survivors[i % 2], .stream_delete, key, "");
        defer r.deinit();
        errdefer std.debug.print("{s}: status {t}, {t}\n", .{ key, r.status, r.reason() });
        try testing.expectEqual(proto.StatusCode.conflict, r.status);
    }
}
