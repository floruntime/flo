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

test "e2e/memory_budget: past its share a shard takes a stream alter that adds retention, and its use falls, while a stream create is refused" {
    const env: []const [2][]const u8 = &.{.{ src.node.test_faults.SHARE_ENV, "262144" }};
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .faults = true },
        .env = env,
    });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    for (0..100) |_| {
        var resp = try http.get("/api/v1/metrics");
        defer resp.deinit();
        if (std.mem.indexOf(u8, resp.body, "\"share_bytes\":262144") != null) break;
        stdx.time.sleep(50 * std.time.ns_per_ms);
    } else return error.ShareNeverBound;

    // A stream with appends and no retention, then the shard filled.
    for (0..40) |_| try ctx.exec(&.{ "stream", "append", "keep", "a-record-the-user-no-longer-needs" });
    const value: []const u8 = &@as([2000]u8, @splat('v'));
    var key_buf: [32]u8 = undefined;
    var refused = false;
    for (0..400) |i| {
        var r = try ctx.execResult(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "fill-{d}", .{i}), value });
        defer r.deinit();
        if (pastShare(r)) {
            refused = true;
            break;
        }
    }
    try testing.expect(refused);
    // Then small writes, till not even one fits: a stream's create adds more.
    refused = false;
    for (0..400) |i| {
        var r = try ctx.execResult(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "tight-{d}", .{i}), "small" });
        defer r.deinit();
        if (pastShare(r)) {
            refused = true;
            break;
        }
    }
    try testing.expect(refused);

    // A new stream would grow the shard: refused.
    {
        var r = try ctx.execResult(&.{ "stream", "create", "more", "--retention-count", "2" });
        defer r.deinit();
        try testing.expect(pastShare(r));
    }
    // Retention on the stream it holds is how the user frees memory: taken.
    const at_share = (try poolUse(http)).node;
    {
        var r = try ctx.execResult(&.{ "stream", "alter", "keep", "--retention-count", "2", "--output", "json" });
        defer r.deinit();
        try testing.expect(!pastShare(r));
        try testing.expect(r.contains("\"status\":\"altered\""));
    }
    // The retention duty removes all but two appends: the use falls.
    var after = at_share;
    for (0..200) |_| {
        after = (try poolUse(http)).node;
        if (after < at_share) break;
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }
    try testing.expect(after < at_share);
}

test "e2e/memory_budget: past its share a shard refuses a workflow's create, keeping nothing of it" {
    const env: []const [2][]const u8 = &.{.{ src.node.test_faults.SHARE_ENV, "262144" }};
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .faults = true },
        .env = env,
    });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    for (0..100) |_| {
        var resp = try http.get("/api/v1/metrics");
        defer resp.deinit();
        if (std.mem.indexOf(u8, resp.body, "\"share_bytes\":262144") != null) break;
        stdx.time.sleep(50 * std.time.ns_per_ms);
    } else return error.ShareNeverBound;

    // Definitions of 4 KiB each, till a shard holding them is full.
    const pad: []const u8 = &@as([4000]u8, @splat('x'));
    var name_buf: [32]u8 = undefined;
    var file_buf: [48]u8 = undefined;
    var refused_name: ?[]const u8 = null;
    var taken: usize = 0;
    while (taken < 400) : (taken += 1) {
        const name = try std.fmt.bufPrint(&name_buf, "mbwf-{d:0>4}", .{taken});
        const yaml = try std.fmt.allocPrint(testing.allocator, "kind: Workflow\nname: {s}\nversion: 1.0.0\nstart:\n  run: \"@actions/validate\"\n  transitions:\n    success: flo.Completed\n    failure: flo.Failed\n# {s}\n", .{ name, pad });
        defer testing.allocator.free(yaml);
        const path = try stdx.testing.writeTempYaml(testing.allocator, yaml, try std.fmt.bufPrint(&file_buf, "memory-budget-{s}.yaml", .{name}));
        defer stdx.testing.cleanupTempFile(testing.allocator, path);
        var r = try ctx.execResult(&.{ "workflow", "create", "-f", path });
        defer r.deinit();
        if (pastShare(r)) {
            // Refused with its status and reason, not only the text.
            try testing.expectEqual(@as(u8, 3), r.exit_code);
            try testing.expect(r.stderrContains("[limit/memory_budget]"));
            refused_name = name;
            break;
        }
    }
    try testing.expect(refused_name != null);
    try testing.expect(taken > 10);
    // The refused definition isn't kept; the first is.
    var list = try ctx.execResult(&.{ "workflow", "list" });
    defer list.deinit();
    try testing.expect(std.mem.indexOf(u8, list.stdout, refused_name.?) == null);
    try testing.expect(std.mem.indexOf(u8, list.stdout, "mbwf-0000") != null);
}

test "e2e/memory_budget: past its share a shard holds a run's child, its parent waits rather than fails, and the child runs once memory frees" {
    const env: []const [2][]const u8 = &.{.{ src.node.test_faults.SHARE_ENV, "262144" }};
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .faults = true },
        .env = env,
    });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    for (0..100) |_| {
        var resp = try http.get("/api/v1/metrics");
        defer resp.deinit();
        if (std.mem.indexOf(u8, resp.body, "\"share_bytes\":262144") != null) break;
        stdx.time.sleep(50 * std.time.ns_per_ms);
    } else return error.ShareNeverBound;

    // A child that ends on its own, and a parent that calls it once told to.
    const kid = "kind: Workflow\nname: mb-kid\nversion: 1.0.0\nstart:\n  wait_for_signal:\n    type: go\n    timeout_ms: 100\n  transitions:\n    success: flo.Completed\n    timeout: flo.Completed\n";
    const par = "kind: Workflow\nname: mb-par\nversion: 1.0.0\nstart:\n  wait_for_signal:\n    type: go\n  transitions:\n    success: call\nsteps:\n  call:\n    run: \"@workflow/mb-kid\"\n    transitions:\n      success: flo.Completed\n      failure: flo.Failed\n";
    for ([_][2][]const u8{ .{ kid, "memory-budget-kid.yaml" }, .{ par, "memory-budget-par.yaml" } }) |d| {
        const path = try stdx.testing.writeTempYaml(testing.allocator, d[0], d[1]);
        defer stdx.testing.cleanupTempFile(testing.allocator, path);
        try ctx.exec(&.{ "workflow", "create", "-f", path });
    }
    try ctx.exec(&.{ "workflow", "start", "mb-par", "{}", "--run-id", "mb-par-1" });

    // Fill the shard to its share.
    const value: []const u8 = &@as([2000]u8, @splat('v'));
    var key_buf: [32]u8 = undefined;
    var filled: usize = 0;
    while (filled < 400) : (filled += 1) {
        var r = try ctx.execResult(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "wfill-{d}", .{filled}), value });
        defer r.deinit();
        if (pastShare(r)) break;
    }
    try testing.expect(filled < 400);
    // A new run is refused there, with its status and reason.
    {
        var r = try ctx.execResult(&.{ "workflow", "start", "mb-kid", "{}", "--run-id", "mb-refused" });
        defer r.deinit();
        try testing.expectEqual(@as(u8, 3), r.exit_code);
        try testing.expect(r.stderrContains("[limit/memory_budget]"));
    }
    // The parent goes on to its call: the child is held, the parent waits.
    try ctx.exec(&.{ "workflow", "signal", "mb-par-1", "--type", "go" });
    var held = false;
    for (0..100) |_| {
        var h = try ctx.execResult(&.{ "workflow", "history", "mb-par-1" });
        defer h.deinit();
        try testing.expect(std.mem.indexOf(u8, h.stdout, "child_refused") == null);
        if (std.mem.indexOf(u8, h.stdout, "call_held_over_budget") != null) {
            held = true;
            break;
        }
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }
    try testing.expect(held);
    // Memory frees: asked again, the child runs and ends, and so does the
    // parent.
    for (0..filled) |i| try ctx.exec(&.{ "kv", "delete", try std.fmt.bufPrint(&key_buf, "wfill-{d}", .{i}) });
    var done = false;
    for (0..300) |_| {
        var h = try ctx.execResult(&.{ "workflow", "history", "mb-par-1" });
        defer h.deinit();
        if (std.mem.indexOf(u8, h.stdout, "child_completed") != null) {
            done = true;
            break;
        }
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    try testing.expect(done);
}

test "e2e/memory_budget: past its share a shard refuses a signal, but takes one that wakes its run" {
    const env: []const [2][]const u8 = &.{.{ src.node.test_faults.SHARE_ENV, "262144" }};
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .faults = true },
        .env = env,
    });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    for (0..100) |_| {
        var resp = try http.get("/api/v1/metrics");
        defer resp.deinit();
        if (std.mem.indexOf(u8, resp.body, "\"share_bytes\":262144") != null) break;
        stdx.time.sleep(50 * std.time.ns_per_ms);
    } else return error.ShareNeverBound;

    const wf = "kind: Workflow\nname: mb-wait\nversion: 1.0.0\nstart:\n  wait_for_signal:\n    type: go\n  transitions:\n    success: flo.Completed\n";
    const path = try stdx.testing.writeTempYaml(testing.allocator, wf, "memory-budget-wait.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, path);
    try ctx.exec(&.{ "workflow", "create", "-f", path });
    try ctx.exec(&.{ "workflow", "start", "mb-wait", "{}", "--run-id", "mb-wait-1" });
    // It waits on "go" before the shard fills.
    var waiting = false;
    for (0..100) |_| {
        var h = try ctx.execResult(&.{ "workflow", "history", "mb-wait-1" });
        defer h.deinit();
        if (std.mem.indexOf(u8, h.stdout, "waiting_for_signal") != null) {
            waiting = true;
            break;
        }
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }
    try testing.expect(waiting);

    const value: []const u8 = &@as([2000]u8, @splat('v'));
    var key_buf: [32]u8 = undefined;
    var filled: usize = 0;
    while (filled < 400) : (filled += 1) {
        var r = try ctx.execResult(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "sfill-{d}", .{filled}), value });
        defer r.deinit();
        if (pastShare(r)) break;
    }
    try testing.expect(filled < 400);
    // One it doesn't wait for, larger than a fill value, so past the share
    // whatever room the last fill left: refused, with its status and reason.
    {
        const payload = "{\"p\":\"" ++ @as([4000]u8, @splat('p')) ++ "\"}";
        var r = try ctx.execResult(&.{ "workflow", "signal", "mb-wait-1", "--type", "other", "--payload", payload });
        defer r.deinit();
        try testing.expectEqual(@as(u8, 3), r.exit_code);
        try testing.expect(r.stderrContains("[limit/memory_budget]"));
    }
    // The one it waits for: taken, and the run ends.
    try ctx.exec(&.{ "workflow", "signal", "mb-wait-1", "--type", "go" });
    var done = false;
    for (0..100) |_| {
        var h = try ctx.execResult(&.{ "workflow", "history", "mb-wait-1" });
        defer h.deinit();
        if (std.mem.indexOf(u8, h.stdout, "signal_matched") != null) {
            done = true;
            break;
        }
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }
    try testing.expect(done);
}
