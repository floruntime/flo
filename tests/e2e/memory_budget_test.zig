//! The per-node memory budget on a default single node (no cluster). The
//! shard-0 leader proposes the genesis pool-share and its own row in its tick,
//! the budget derives, and it shows on the dashboard — the single node is the
//! main mode, so the budget has to appear there without a cluster.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const src = @import("src");

test "e2e/memory_budget: a single node shows a non-zero budget" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .node_id = 7 },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // The budget derives in the server's own tick after boot (propose → apply
    // → derive), with no client activity, so poll until it appears.
    var budget: i64 = 0;
    var governing: i64 = 0;
    var provisional: i64 = -1;
    var tries: usize = 0;
    while (tries < 100) : (tries += 1) {
        var resp = try http.get("/api/v1/metrics");
        defer resp.deinit();
        try testing.expectEqual(@as(u16, 200), resp.status);

        const parsed = std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{}) catch {
            stdx.time.sleep(50 * std.time.ns_per_ms);
            continue;
        };
        defer parsed.deinit();

        if (parsed.value.object.get("memory_budget")) |mb| {
            budget = mb.object.get("budget_bytes").?.integer;
            governing = mb.object.get("governing_node").?.integer;
            provisional = mb.object.get("provisional_node").?.integer;
            if (budget > 0) break;
        }
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }

    try testing.expect(budget > 0); // the budget appears without a cluster
    try testing.expectEqual(@as(i64, 7), governing); // this node (id 7) is the sole data-holder and governs
    try testing.expectEqual(@as(i64, 0), provisional); // its own row applied, so not provisional
}

/// The node's pool use as the dashboard shows it, and shard 0's.
fn poolUse(http: anytype) !struct { node: i64, shard0: i64 } {
    var resp = try http.get("/api/v1/metrics");
    defer resp.deinit();
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{});
    defer parsed.deinit();
    const mb = parsed.value.object.get("memory_budget").?;
    const by_shard = mb.object.get("pool_use_by_shard").?.array.items;
    return .{ .node = mb.object.get("pool_use_bytes").?.integer, .shard0 = by_shard[0].integer };
}

test "e2e/memory_budget: the pool's use rises with what a client writes and falls when it deletes it" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    const keys = 50;
    const value: []const u8 = &@as([2000]u8, @splat('v'));
    var key_buf: [32]u8 = undefined;
    const before = (try poolUse(http)).node;
    for (0..keys) |i| try ctx.exec(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "pool-{d}", .{i}), value });

    // Published each tick: poll until the writes show.
    var during: i64 = 0;
    for (0..100) |_| {
        during = (try poolUse(http)).node;
        if (during >= before + keys * value.len) break;
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }
    try testing.expect(during >= before + keys * value.len);

    for (0..keys) |i| try ctx.exec(&.{ "kv", "delete", try std.fmt.bufPrint(&key_buf, "pool-{d}", .{i}) });
    // A delete frees the value: what's left is the tombstones, far less.
    var after: i64 = during;
    for (0..100) |_| {
        after = (try poolUse(http)).node;
        if (after < before + keys * value.len / 4) break;
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }
    try testing.expect(after < before + keys * value.len / 4);
    // The node's figure is its shards', summed.
    const now = try poolUse(http);
    try testing.expect(now.node >= now.shard0);
}

test "e2e/memory_budget: each shard binds its equal share of the budget" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .shards = 4 },
    });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // Each shard's leader binds once shard 0 has recorded a clean budget, in
    // the server's own ticks: poll until every shard shows a share.
    var bound = false;
    for (0..100) |_| {
        var resp = try http.get("/api/v1/metrics");
        defer resp.deinit();
        const parsed = std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{}) catch {
            stdx.time.sleep(50 * std.time.ns_per_ms);
            continue;
        };
        defer parsed.deinit();
        const mb = parsed.value.object.get("memory_budget").?.object;
        const budget = mb.get("budget_bytes").?.integer;
        const shares = mb.get("shares").?.array.items;
        try testing.expectEqual(@as(usize, 4), shares.len);
        var all = budget > 0;
        for (shares) |sh| {
            const share = sh.object.get("share_bytes") orelse {
                // Unbound says so, and claims no share.
                try testing.expectEqualStrings("off: no budget bound", sh.object.get("enforcement").?.string);
                all = false;
                continue;
            };
            try testing.expectEqual(@divFloor(budget, @as(i64, @intCast(shares.len))), share.integer);
            try testing.expect(!sh.object.get("over_share").?.bool);
        }
        if (all) {
            bound = true;
            break;
        }
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }
    try testing.expect(bound);
}

/// Whether a CLI run's output says its write was past its shard's share.
fn pastShare(r: anytype) bool {
    return std.mem.indexOf(u8, r.stdout, "would pass its share") != null or std.mem.indexOf(u8, r.stderr, "would pass its share") != null;
}

test "e2e/memory_budget: past its share a shard refuses a write, takes a delete, and takes writes again once under" {
    // The faults binary binds this share in place of the budget's.
    const env: []const [2][]const u8 = &.{.{ src.node.test_faults.SHARE_ENV, "262144" }};
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .metrics_enabled = true, .faults = true },
        .env = env,
    });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    // Bound in the server's own ticks: wait for the share to show.
    for (0..100) |_| {
        var resp = try http.get("/api/v1/metrics");
        defer resp.deinit();
        if (std.mem.indexOf(u8, resp.body, "\"share_bytes\":262144") != null) break;
        stdx.time.sleep(50 * std.time.ns_per_ms);
    } else return error.ShareNeverBound;

    const value: []const u8 = &@as([2000]u8, @splat('v'));
    var key_buf: [32]u8 = undefined;
    var taken: usize = 0;
    var refused = false;
    while (taken < 400) : (taken += 1) {
        var r = try ctx.execResult(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "fill-{d}", .{taken}), value });
        defer r.deinit();
        if (pastShare(r)) {
            refused = true;
            break;
        }
    }
    try testing.expect(refused);
    // 256 KiB of 2 KiB values, with what the node holds besides.
    try testing.expect(taken > 50 and taken < 130);
    // Nothing of the refused write is kept.
    {
        var r = try ctx.execResult(&.{ "kv", "get", try std.fmt.bufPrint(&key_buf, "fill-{d}", .{taken}) });
        defer r.deinit();
        try testing.expect(std.mem.indexOf(u8, r.stdout, "vvvv") == null);
    }
    // A delete over the share is taken, and frees room for the next write.
    for (0..4) |i| try ctx.exec(&.{ "kv", "delete", try std.fmt.bufPrint(&key_buf, "fill-{d}", .{i}) });
    var r = try ctx.execResult(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "fill-{d}", .{taken}), value });
    defer r.deinit();
    try testing.expect(!pastShare(r));
    var got = try ctx.execResult(&.{ "kv", "get", try std.fmt.bufPrint(&key_buf, "fill-{d}", .{taken}) });
    defer got.deinit();
    try testing.expect(std.mem.indexOf(u8, got.stdout, "vvvv") != null);
    // The refusal is counted.
    var metrics = try ctx.createMetricsHttp();
    defer metrics.deinit();
    var resp = try metrics.get("/metrics");
    defer resp.deinit();
    try testing.expect(std.mem.indexOf(u8, resp.body, "flo_shard_memory_refusals_total{shard_id=\"0\"} 1\n") != null);
}
