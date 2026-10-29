//! An entry whose apply crashes the node, on every replica, would crash it
//! again on every restart. Instead the crash is recorded, and the restart
//! comes up with that entry's shard halted at it; the other shards serve.
//! The crashing entries come from faults only a `-Dtest-faults` build
//! carries; without one these skip.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const test_faults = @import("src").node.test_faults;
const apply_marker = @import("src").node.apply_marker;

/// A node's `/status`, parsed.
fn nodeStatus(port: u16) !std.json.Parsed(std.json.Value) {
    const http = try stdx.testing.HttpRunner.init(testing.allocator, "127.0.0.1", port);
    defer http.deinit();
    var resp = try http.get("/status");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    return std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{});
}

const Halted = struct { shard: usize, index: i64 };

/// The one shard halted for a crash in apply, after checking every other
/// shard is not halted.
fn crashedShard(port: u16) !Halted {
    const parsed = try nodeStatus(port);
    defer parsed.deinit();
    var found: ?Halted = null;
    for (parsed.value.object.get("shards").?.array.items, 0..) |s, i| {
        if (!std.mem.eql(u8, s.object.get("state").?.string, "halted")) continue;
        if (found != null) return error.MoreThanOneShardHalted;
        const reason = s.object.get("reason").?.string;
        if (std.mem.indexOf(u8, reason, "applying it crashed this node") == null) {
            std.debug.print("halt reason: {s}\n", .{reason});
            return error.WrongHaltReason;
        }
        found = .{ .shard = i, .index = s.object.get("halt_index").?.integer };
    }
    return found orelse error.NoShardHalted;
}

/// A lone node with two shards crashes on `fault`'s key, and comes up, twice,
/// with that key's shard halted at it and the other shard serving.
fn crashOnce(fault: test_faults.Fault) !void {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 2, .durability = .sync, .metrics_enabled = true },
    });
    defer ctx.deinit();
    try ctx.exec(&.{ "kv", "set", "before", "kept" });

    {
        var r = try ctx.execResult(&.{ "kv", "set", fault.key(), "x" });
        defer r.deinit();
    }
    _ = try ctx.server.awaitCrash(15_000);

    var at: ?Halted = null;
    for (0..2) |_| {
        try ctx.restartServer();
        const h = try crashedShard(ctx.server.metrics_port);
        if (at) |a| {
            try testing.expectEqual(a.shard, h.shard);
            try testing.expectEqual(a.index, h.index);
        } else at = h;
        // Reads are served, from either shard.
        try testing.expectEqualStrings("kept", try ctx.execCapture(&.{ "kv", "get", "before" }));
    }
    try testing.expect(try ctx.server.logsContain("crashed this node before the restart"));
}

test "e2e/apply-crash: a panic in apply comes back up halted at the entry, on its shard alone, across restarts" {
    if (!test_faults.enabled) return error.SkipZigTest;
    try crashOnce(.panic);
}

test "e2e/apply-crash: a memory fault in apply comes back up halted at the entry" {
    if (!test_faults.enabled) return error.SkipZigTest;
    try crashOnce(.segv);
}

test "e2e/apply-crash: a panic in the triggers and metrics after apply comes back up halted at the entry" {
    if (!test_faults.enabled) return error.SkipZigTest;
    try crashOnce(.panic_after_apply);
}

/// After a crash in a step only this node takes, the restart records
/// nothing: `key`'s set applies again at boot and every shard serves.
fn servesAfterPlainCrash(ctx: *stdx.testing.TestContext, key: []const u8) !void {
    _ = try ctx.server.awaitCrash(15_000);
    try ctx.restartServer();
    try testing.expectError(error.NoShardHalted, crashedShard(ctx.server.metrics_port));
    try testing.expectEqualStrings("x", try ctx.execCapture(&.{ "kv", "get", key }));
    try ctx.exec(&.{ "kv", "set", "after", "kept" });
    try testing.expectEqualStrings("kept", try ctx.execCapture(&.{ "kv", "get", "after" }));
    try testing.expect(!try ctx.server.logsContain("crashed this node before the restart"));
}

test "e2e/apply-crash: a panic in the leader's own step after apply is a plain crash, and the node comes back serving" {
    if (!test_faults.enabled) return error.SkipZigTest;
    const key = test_faults.Fault.panic_leader_step.key();
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 2, .durability = .sync, .metrics_enabled = true },
    });
    defer ctx.deinit();
    {
        var r = try ctx.execResult(&.{ "kv", "set", key, "x" });
        defer r.deinit();
    }
    try servesAfterPlainCrash(ctx, key);
}

test "e2e/apply-crash: a panic answering a waiting read is a plain crash, and the node comes back serving" {
    if (!test_faults.enabled) return error.SkipZigTest;
    const key = test_faults.Fault.panic_answering_waiter.key();
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 2, .durability = .sync, .metrics_enabled = true },
    });
    defer ctx.deinit();
    var waiting = try ctx.cli.runAsync(&.{ "kv", "get", key, "--wait-ms", "10000" });
    defer waiting.deinit();
    // Time for the read to register its wait.
    stdx.time.sleep(200 * std.time.ns_per_ms);
    {
        var r = try ctx.execResult(&.{ "kv", "set", key, "x" });
        defer r.deinit();
    }
    // The waiting read is answered by the crash, not with the value.
    try servesAfterPlainCrash(ctx, key);
    var got = try waiting.wait();
    defer got.deinit();
}

test "e2e/apply-crash: a crash another build retries at boot, and that crashes it again, halts the next boot" {
    if (!test_faults.enabled) return error.SkipZigTest;
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 2, .durability = .sync, .metrics_enabled = true },
    });
    defer ctx.deinit();
    {
        var r = try ctx.execResult(&.{ "kv", "set", test_faults.Fault.panic.key(), "x" });
        defer r.deinit();
    }
    _ = try ctx.server.awaitCrash(15_000);
    try ctx.restartServer();
    const first = try crashedShard(ctx.server.metrics_port);

    // An upgrade finds the record from another build and retries the entry
    // in replay, where it crashes again.
    ctx.server.stop();
    var dir_buf: [512]u8 = undefined;
    const shard_dir = try std.fmt.bufPrint(&dir_buf, "{s}/{d:0>5}", .{ ctx.server.getDataDir(), first.shard });
    try apply_marker.relabelForTest(shard_dir, "an older build");
    try ctx.server.spawn();
    _ = try ctx.server.awaitCrash(15_000);
    try testing.expect(try ctx.server.logsContain("crashed build an older build; retrying it on"));

    try ctx.restartServer();
    const again = try crashedShard(ctx.server.metrics_port);
    try testing.expectEqual(first.shard, again.shard);
    try testing.expectEqual(first.index, again.index);
}

test "e2e/apply-crash: every node of a three-node group crashes on the entry and comes up halted at it" {
    if (!test_faults.enabled) return error.SkipZigTest;
    var cluster = try stdx.testing.ClusterContext.init(testing.allocator, .{ .node_count = 3, .metrics_enabled = true });
    defer cluster.deinit();
    try cluster.execOnWithRetry(0, &.{ "kv", "set", "before", "kept" }, 20, 250);
    {
        var r = try cluster.clis[0].?.run(&.{ "kv", "set", test_faults.Fault.panic.key(), "x" });
        defer r.deinit();
    }
    // A node crashes when its apply reaches the entry. A follower may learn
    // it committed only from the next leader, so each crashed node is
    // restarted until all three are up, halted at the same entry.
    const DEADLINE_MS = 60_000;
    const started = stdx.time.monotonicMs();
    var halted_at: [3]?i64 = @splat(null);
    var round: usize = 0;
    while (halted_at[0] == null or halted_at[1] == null or halted_at[2] == null) : (round += 1) {
        if (stdx.time.monotonicMs() - started > DEADLINE_MS) {
            cluster.printLogs();
            std.debug.print("[apply-crash] halted after {d} ms: {any}\n", .{ DEADLINE_MS, halted_at });
            return error.NodesNotHaltedInTime;
        }
        for (0..3) |i| {
            if (halted_at[i] != null) continue;
            const server = cluster.getServer(i).?;
            if (server.awaitCrash(100)) |sig| {
                std.debug.print("[apply-crash] round {d}, {d} ms: node {d} died of signal {d}; restarting it\n", .{ round, stdx.time.monotonicMs() - started, i, sig });
                cluster.stopNode(i);
                try cluster.restartNode(i);
                continue;
            } else |_| {}
            halted_at[i] = (crashedShard(server.metrics_port) catch continue).index;
        }
        stdx.time.sleep(250 * std.time.ns_per_ms);
    }
    std.debug.print("[apply-crash] all three halted at entry {d} in {d} ms\n", .{ halted_at[0].?, stdx.time.monotonicMs() - started });
    try testing.expectEqual(halted_at[0].?, halted_at[1].?);
    try testing.expectEqual(halted_at[0].?, halted_at[2].?);
}
