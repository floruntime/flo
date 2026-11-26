//! Namespace End-to-End Tests
//!
//! Tests namespace management: create, delete, list, info
//! Uses FloTestContext convenience methods.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

// =============================================================================
// Basic Operations
// =============================================================================

test "e2e/namespace: list shows default after kv operation" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Any KV operation implicitly creates "default" namespace
    try ctx.exec(&.{ "kv", "set", "testkey", "testvalue" });

    // Verify default namespace exists
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, "default") != null);
}

test "e2e/namespace: create and list" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create a new namespace
    try ctx.exec(&.{ "ns", "create", "myapp" });

    // Verify it appears in the list
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, "myapp") != null);
    // As JSON, an object per name, in name order.
    var json = try ctx.cli.run(&.{ "ns", "ls", "-o", "json" });
    defer json.deinit();
    try testing.expectEqual(@as(u8, 0), json.exit_code);
    try testing.expectEqualStrings("[{\"name\":\"default\"},{\"name\":\"myapp\"}]\n", json.stdout);
    // "default" was never created.
    var builtin = try ctx.cli.run(&.{ "ns", "info", "default" });
    defer builtin.deinit();
    try testing.expectEqual(@as(u8, 0), builtin.exit_code);
    try testing.expect(std.mem.startsWith(u8, builtin.stdout, "Namespace:  default\nCreated at: built in\nHistory quota: 10.0 GiB over every shard (the default)\n"));
}

test "e2e/namespace: create multiple namespaces" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create multiple namespaces
    try ctx.exec(&.{ "ns", "create", "prod" });
    try ctx.exec(&.{ "ns", "create", "staging" });
    try ctx.exec(&.{ "ns", "create", "dev" });

    // Verify all appear in the list
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, "prod") != null);
    try testing.expect(std.mem.indexOf(u8, list, "staging") != null);
    try testing.expect(std.mem.indexOf(u8, list, "dev") != null);
}

test "e2e/namespace: create duplicate fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create a namespace
    try ctx.exec(&.{ "ns", "create", "dup_test" });

    // Second create should fail
    var result = try ctx.cli.run(&.{ "ns", "create", "dup_test" });
    defer result.deinit();

    try testing.expectEqual(@as(u8, 3), result.exit_code);
    try stdx.testing.assertStderrContains(result, "Error: namespace already exists [conflict]");
}

test "e2e/namespace: info" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create a namespace
    try ctx.exec(&.{ "ns", "create", "infotest" });

    // Get info
    var result = try ctx.cli.run(&.{ "ns", "info", "infotest" });
    defer result.deinit();

    try testing.expectEqual(@as(u8, 0), result.exit_code);
    try stdx.testing.assertStdoutContains(result, "Namespace:  infotest\nCreated at: ");
    // Created just now: a real time, not zero.
    const at = std.mem.indexOf(u8, result.stdout, "Created at: ").? + "Created at: ".len;
    const end = std.mem.indexOfScalarPos(u8, result.stdout, at, ' ').?;
    try testing.expect(try std.fmt.parseInt(i64, result.stdout[at..end], 10) > 1_700_000_000_000);
}

test "e2e/namespace: info non-existent fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "ns", "info", "nonexistent_ns_67890" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 1), result.exit_code);
    try stdx.testing.assertStderrContains(result, "Error: namespace not found [no_namespace/not_found]");
}

// =============================================================================
// Namespace Isolation
// =============================================================================

test "e2e/namespace: kv isolation between namespaces" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create two namespaces
    try ctx.exec(&.{ "ns", "create", "ns_a" });
    try ctx.exec(&.{ "ns", "create", "ns_b" });

    // Set same key in different namespaces
    try ctx.exec(&.{ "kv", "set", "shared_key", "value_a", "-n", "ns_a" });
    try ctx.exec(&.{ "kv", "set", "shared_key", "value_b", "-n", "ns_b" });

    // Verify values are isolated
    const value_a = try ctx.execCapture(&.{ "kv", "get", "shared_key", "-n", "ns_a" });
    const value_b = try ctx.execCapture(&.{ "kv", "get", "shared_key", "-n", "ns_b" });

    try testing.expect(std.mem.indexOf(u8, value_a, "value_a") != null);
    try testing.expect(std.mem.indexOf(u8, value_b, "value_b") != null);
}

// =============================================================================
// Internal Namespaces (--all flag)
// =============================================================================

// =============================================================================
// Cluster Tests (Multi-Node)
// =============================================================================

const ClusterContext = stdx.testing.ClusterContext;

test "e2e/namespace/cluster: kv operations work across cluster with namespaces" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Write from node 0
    try cluster.execOn(0, &.{ "kv", "set", "cluster_key", "from_node_0" });

    // Read from node 1 (retry to tolerate replication delay under load)
    const value1 = try cluster.execCaptureOnWithRetry(1, &.{ "kv", "get", "cluster_key" }, 5, 500);
    defer testing.allocator.free(value1);
    try testing.expect(std.mem.indexOf(u8, value1, "from_node_0") != null);

    // Read from node 2
    const value2 = try cluster.execCaptureOnWithRetry(2, &.{ "kv", "get", "cluster_key" }, 5, 500);
    defer testing.allocator.free(value2);
    try testing.expect(std.mem.indexOf(u8, value2, "from_node_0") != null);
}

// =============================================================================
// Persistence / Restart
// =============================================================================

test "e2e/namespace: create survives server restart" {
    // Use sync durability to ensure namespace state is on disk before response
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    // Create a namespace
    try ctx.exec(&.{ "ns", "create", "restart_test" });

    // Verify it exists before restart
    const before = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, before, "restart_test") != null);

    // Restart the server
    try ctx.restartServer();

    // Verify namespace still exists after restart
    const after = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, after, "restart_test") != null);

    // Verify namespace is fully functional — KV ops should work
    try ctx.exec(&.{ "kv", "set", "mykey", "myval", "-n", "restart_test" });
    const val = try ctx.execCapture(&.{ "kv", "get", "mykey", "-n", "restart_test" });
    try testing.expect(std.mem.indexOf(u8, val, "myval") != null);
}

test "e2e/namespace: the CLI has no way to set a namespace setting" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "cfg_ns" });
    var result = try ctx.cli.run(&.{ "ns", "config", "cfg_ns", "--set", "stream_retention_s=86400" });
    defer result.deinit();
    try stdx.testing.assertStderrContains(result, "unknown flag --set");
}

test "e2e/namespace: delete is refused, and the namespace and its data stay" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const ns = "keep-me";
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "kv", "set", "k", "v", "-n", ns });
    try ctx.exec(&.{ "stream", "append", "orders", "order-1", "-n", ns });
    try ctx.exec(&.{ "queue", "enqueue", "jobs", "job-1", "-n", ns });

    for ([_][]const u8{ ns, "default" }) |name| {
        var r = try ctx.cli.run(&.{ "ns", "delete", name });
        defer r.deinit();
        try testing.expectEqual(@as(u8, 3), r.exit_code);
        try stdx.testing.assertStderrContains(r, "namespace delete isn't supported yet");
    }
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, ns) != null);
    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "kv", "get", "k", "-n", ns }), "v") != null);
    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "stream", "info", "orders", "-n", ns }), "Records: 1") != null);
    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "queue", "dequeue", "jobs", "-n", ns }), "job-1") != null);
}

// An empty namespace and "default" are one namespace everywhere: a KV key
// set under one is read under the other whichever shard it lands on, and a
// stream appended under one is the same stream under the other.
test "e2e/namespace: an empty namespace is the default namespace on every shard" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();

    var key_buf: [16]u8 = undefined;
    var value_buf: [16]u8 = undefined;
    for (0..16) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "k{d}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "v{d}", .{i});
        const write_ns, const read_ns = if (i % 2 == 0) .{ "", "default" } else .{ "default", "" };
        try ctx.exec(&.{ "kv", "set", key, value, "-n", write_ns });
        // The answer is the value alone.
        const out = try ctx.execCapture(&.{ "kv", "get", key, "-n", read_ns });
        try testing.expectEqualStrings(value, std.mem.trimEnd(u8, out, "\n"));
    }

    try ctx.exec(&.{ "stream", "append", "s", "from-empty", "-n", "" });
    try ctx.exec(&.{ "stream", "append", "s", "from-default", "-n", "default" });
    for ([_][]const u8{ "", "default" }) |ns| {
        const out = try ctx.execCapture(&.{ "stream", "read", "s", "--start", "0-0", "--limit", "10", "-n", ns });
        try testing.expect(std.mem.indexOf(u8, out, "from-empty") != null);
        try testing.expect(std.mem.indexOf(u8, out, "from-default") != null);
    }
}

// =============================================================================
// One owner: shard 0 creates every namespace, every shard uses it
// =============================================================================

test "e2e/namespace: on 4 shards, a namespace made by a create or by a first write is usable on every shard, listed, and survives restarts" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();
    var kbuf: [16]u8 = undefined;
    var vbuf: [16]u8 = undefined;
    // "made" by an explicit create, "first" by its first writes: 16 keys,
    // queues and streams each, spread over every shard.
    try ctx.exec(&.{ "ns", "create", "made" });
    for ([_][]const u8{ "made", "first" }) |ns| {
        for (0..16) |i| {
            const k = try std.fmt.bufPrint(&kbuf, "k{d}", .{i});
            try ctx.exec(&.{ "kv", "set", k, try std.fmt.bufPrint(&vbuf, "{s}{d}", .{ ns, i }), "-n", ns });
            try ctx.exec(&.{ "queue", "enqueue", k, "job", "-n", ns });
            try ctx.exec(&.{ "stream", "append", k, "rec", "-n", ns });
        }
    }
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, "made") != null);
    try testing.expect(std.mem.indexOf(u8, list, "first") != null);

    // Twice: a clean stop, then a kill -9 right after another create.
    for (0..2) |round| {
        if (round == 0) {
            try ctx.restartServer();
        } else {
            try ctx.exec(&.{ "ns", "create", "late" });
            ctx.server.sent_kill = true;
            _ = std.c.kill(ctx.server.process.?.id, .KILL);
            try ctx.restartServer();
            // Acked, so on disk.
            var info = try ctx.cli.run(&.{ "ns", "info", "late" });
            defer info.deinit();
            try testing.expectEqual(@as(u8, 0), info.exit_code);
        }
        for ([_][]const u8{ "made", "first" }) |ns| {
            var info = try ctx.cli.run(&.{ "ns", "info", ns });
            defer info.deinit();
            try testing.expectEqual(@as(u8, 0), info.exit_code);
            for (0..16) |i| {
                const k = try std.fmt.bufPrint(&kbuf, "k{d}", .{i});
                const got = try ctx.execCapture(&.{ "kv", "get", k, "-n", ns });
                try testing.expect(std.mem.indexOf(u8, got, try std.fmt.bufPrint(&vbuf, "{s}{d}", .{ ns, i })) != null);
            }
            // Every queue and stream replayed under its own namespace, none
            // under "default".
            const queues = try ctx.execCapture(&.{ "queue", "list", "-n", ns });
            try testing.expect(std.mem.indexOf(u8, queues, "k15") != null);
            const streams = try ctx.execCapture(&.{ "stream", "list", "-n", ns });
            try testing.expect(std.mem.indexOf(u8, streams, "k15") != null);
        }
        const default_queues = try ctx.execCapture(&.{ "queue", "list" });
        try testing.expect(std.mem.indexOf(u8, default_queues, "k15") == null);
    }
}

test "e2e/namespace: on 4 shards, creates through connections on different shards give each name one id, and a second create is refused" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();
    var nbuf: [16]u8 = undefined;
    // Each CLI call is a new connection, landing on any shard.
    for (0..16) |i| try ctx.exec(&.{ "ns", "create", try std.fmt.bufPrint(&nbuf, "team{d}", .{i}) });
    for (0..16) |i| {
        var again = try ctx.cli.run(&.{ "ns", "create", try std.fmt.bufPrint(&nbuf, "team{d}", .{i}) });
        defer again.deinit();
        try testing.expectEqual(@as(u8, 3), again.exit_code);
        try stdx.testing.assertStderrContains(again, "Error: namespace already exists [conflict]");
    }
    var json = try ctx.cli.run(&.{ "ns", "ls", "-o", "json" });
    defer json.deinit();
    try testing.expectEqual(@as(u8, 0), json.exit_code);
    // default and the 16, each once.
    try testing.expectEqual(@as(usize, 17), std.mem.count(u8, json.stdout, "\"name\""));
}

test "e2e/namespace: on 4 shards, a read in a namespace never created is refused, of every kind, and a write's namespace is readable at once on every shard" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();

    const reads = [_][]const []const u8{
        &.{ "kv", "get", "k", "-n", "never" },
        &.{ "kv", "scan", "-n", "never" },
        &.{ "stream", "read", "s", "-n", "never" },
        &.{ "stream", "list", "-n", "never" },
        &.{ "queue", "list", "-n", "never" },
        &.{ "ts", "read", "m", "-n", "never" },
        &.{ "action", "list", "-n", "never" },
        &.{ "workflow", "list", "-n", "never" },
    };
    for (reads) |args| {
        var r = try ctx.cli.run(args);
        defer r.deinit();
        if (std.mem.indexOf(u8, r.stderr, "namespace not found [no_namespace/not_found]") == null) std.debug.print("{s}: {s}{s}\n", .{ args[0], r.stdout, r.stderr });
        try testing.expect(std.mem.indexOf(u8, r.stderr, "namespace not found [no_namespace/not_found]") != null);
    }

    // A write creates its namespace through whichever shard took it; a read
    // of another key, owned by any shard, then finds the namespace.
    for (0..12) |i| {
        var ns: [16]u8 = undefined;
        const name = try std.fmt.bufPrint(&ns, "fresh-{d}", .{i});
        try ctx.exec(&.{ "kv", "set", "a", "v", "-n", name });
        for ([_][]const u8{ "b", "c", "d", "e" }) |key| {
            var r = try ctx.cli.run(&.{ "kv", "get", key, "-n", name });
            defer r.deinit();
            try testing.expect(std.mem.indexOf(u8, r.stderr, "no_namespace") == null);
            try testing.expect(std.mem.indexOf(u8, r.stderr, "not found [not_found/not_found]") != null);
        }
    }
}

test "e2e/namespace/cluster: a namespace created through one node is readable through every other at once" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    for (0..6) |i| {
        var ns: [16]u8 = undefined;
        const name = try std.fmt.bufPrint(&ns, "cl-{d}", .{i});
        try cluster.execOn(i % cluster.node_count, &.{ "ns", "create", name });
        // No retry: a node behind asks the leader and waits for its own
        // table, rather than answer that the namespace isn't there.
        for (0..cluster.node_count) |node| {
            var r = try cluster.clis[node].?.run(&.{ "kv", "get", "k", "-n", name });
            defer r.deinit();
            if (std.mem.indexOf(u8, r.stderr, "not found [not_found/not_found]") == null) std.debug.print("node {d}, {s}: {s}{s}\n", .{ node, name, r.stdout, r.stderr });
            try testing.expect(std.mem.indexOf(u8, r.stderr, "not found [not_found/not_found]") != null);
        }
    }
    // One never created is refused through every node.
    for (0..cluster.node_count) |node| {
        var r = try cluster.clis[node].?.run(&.{ "kv", "get", "k", "-n", "cl-never" });
        defer r.deinit();
        try testing.expect(std.mem.indexOf(u8, r.stderr, "[no_namespace/not_found]") != null);
    }
}

test "e2e/namespace: a read the leader answers in a namespace never created is refused no_namespace, as other reads are" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    // stream info and the DLQ list answered no_namespace already; peek and
    // the group list, sent to the leader, answered empty before they were
    // reads like these.
    const reads = [_][]const []const u8{
        &.{ "queue", "peek", "q1", "-n", "never-made" },
        &.{ "stream", "group", "list", "s1", "-n", "never-made" },
        &.{ "stream", "group", "members", "s1", "g1", "-n", "never-made" },
        &.{ "queue", "dlq", "list", "q1", "-n", "never-made" },
        &.{ "stream", "info", "s1", "-n", "never-made" },
    };
    for (reads) |args| {
        var r = try ctx.cli.run(args);
        defer r.deinit();
        if (!r.contains("[no_namespace/")) std.debug.print("{s}: {s}{s}\n", .{ args[0], r.stdout, r.stderr });
        try testing.expect(r.contains("[no_namespace/"));
    }
}

/// `flo namespace info <name> -o json`'s answer, parsed.
const QuotaInfo = struct {
    history_quota: ?u64,
    history_quota_own: bool,
    shards: []const struct { shard: u16, bound: bool, part_bytes: ?u64, usage_bytes: u64 },
};

fn quotaInfo(ctx: *stdx.testing.TestContext, name: []const u8) !std.json.Parsed(QuotaInfo) {
    var r = try ctx.cli.run(&.{ "namespace", "info", name, "-o", "json" });
    defer r.deinit();
    return std.json.parseFromSlice(QuotaInfo, testing.allocator, r.stdout, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
}

/// Polls `name`'s info until every shard has bound `part` (null for none).
fn expectBound(ctx: *stdx.testing.TestContext, name: []const u8, shards: usize, part: ?u64) !void {
    var tries: usize = 0;
    while (tries < 100) : (tries += 1) {
        const info = try quotaInfo(ctx, name);
        defer info.deinit();
        try testing.expectEqual(shards, info.value.shards.len);
        var all = true;
        for (info.value.shards) |s| {
            if (!s.bound or !std.meta.eql(s.part_bytes, part)) all = false;
        }
        if (all) return;
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return error.NotBound;
}

test "e2e/namespace: a namespace's run-history quota is bound on every shard, its part shown by info; an alter rebinds it, none binds none, and a quota below the floor or of 0 is refused" {
    const shards = 4;
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = shards } });
    defer ctx.deinit();
    try ctx.exec(&.{ "namespace", "create", "hq" });
    // The default, 10 GiB, split over the shards and rounded up.
    try expectBound(ctx, "hq", shards, (10 << 30) / shards);
    {
        const info = try quotaInfo(ctx, "hq");
        defer info.deinit();
        try testing.expectEqual(@as(?u64, 10 << 30), info.value.history_quota);
        try testing.expect(!info.value.history_quota_own);
    }
    try ctx.exec(&.{ "namespace", "alter", "hq", "--history-quota", "20GiB", "--config", ctx.server.config_file });
    try expectBound(ctx, "hq", shards, (20 << 30) / shards);
    try ctx.exec(&.{ "namespace", "alter", "hq", "--history-quota", "none", "--config", ctx.server.config_file });
    try expectBound(ctx, "hq", shards, null);
    {
        const info = try quotaInfo(ctx, "hq");
        defer info.deinit();
        try testing.expectEqual(@as(?u64, null), info.value.history_quota);
        try testing.expect(info.value.history_quota_own);
    }
    // Back to the default: its part is the default's again, and its own no
    // longer.
    try ctx.exec(&.{ "namespace", "alter", "hq", "--history-quota", "default", "--config", ctx.server.config_file });
    try expectBound(ctx, "hq", shards, (10 << 30) / shards);
    {
        const info = try quotaInfo(ctx, "hq");
        defer info.deinit();
        try testing.expect(!info.value.history_quota_own);
    }
    try ctx.exec(&.{ "namespace", "alter", "hq", "--history-quota", "none", "--config", ctx.server.config_file });
    try expectBound(ctx, "hq", shards, null);
    // The built-in default namespace takes one too.
    try expectBound(ctx, "default", shards, (10 << 30) / shards);
    // Below one run's reserve on every shard, refused by the server by
    // name; 0 refused by the CLI before it asks.
    var low = try ctx.cli.run(&.{ "namespace", "alter", "hq", "--history-quota", "1KiB", "--config", ctx.server.config_file });
    defer low.deinit();
    try stdx.testing.assertContains(low, "a history quota is at least");
    try stdx.testing.assertContains(low, "[bad_request/out_of_range]");
    var zero = try ctx.cli.run(&.{ "namespace", "alter", "hq", "--history-quota", "0", "--config", ctx.server.config_file });
    defer zero.deinit();
    try stdx.testing.assertContains(zero, "--history-quota is a number of bytes above 0");
    var huge = try ctx.cli.run(&.{ "namespace", "alter", "hq", "--history-quota", "18446744073709551615", "--config", ctx.server.config_file });
    defer huge.deinit();
    try stdx.testing.assertContains(huge, "--history-quota is a number of bytes above 0");
    var missing = try ctx.cli.run(&.{ "namespace", "alter", "never-made", "--history-quota", "20GiB", "--config", ctx.server.config_file });
    defer missing.deinit();
    try stdx.testing.assertContains(missing, "not found");
    // The client port refuses an alter: it administers the node.
    {
        const src = @import("src");
        var conn = src.cli_client.Client.init(testing.allocator, ctx.endpoint);
        defer conn.deinit();
        try conn.connect();
        var r = try src.cli_client.namespace.alter(&conn, "hq", .bytes, 40 << 30);
        defer r.deinit();
        try testing.expectEqual(src.protocol.proto.StatusCode.unauthorized, r.status);
        try testing.expectEqual(src.protocol.proto.Reason.admin_only, r.reason());
    }
    // On the admin socket, bytes with another mode is a malformed request,
    // not one out of range.
    {
        const src = @import("src");
        var conn = src.cli_client.Client.init(testing.allocator, ctx.server.admin_socket);
        defer conn.deinit();
        try conn.connectUnix(ctx.server.admin_socket);
        var r = try src.cli_client.namespace.alter(&conn, "hq", .none, 40 << 30);
        defer r.deinit();
        try testing.expectEqual(src.protocol.proto.StatusCode.bad_request, r.status);
        try testing.expectEqual(src.protocol.proto.Reason.malformed, r.reason());
    }
    // -n names no namespace here: the one altered is the name given.
    var named = try ctx.cli.run(&.{ "namespace", "alter", "hq", "--history-quota", "20GiB", "-n", "other", "--config", ctx.server.config_file });
    defer named.deinit();
    try stdx.testing.assertContains(named, "-n/--namespace isn't read here");
    // A refused alter changed nothing.
    try expectBound(ctx, "hq", shards, null);
}

test "e2e/namespace: quotas set, rounded up per shard, and their binds come back from the log after a restart" {
    const shards = 4;
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = shards } });
    defer ctx.deinit();
    try ctx.exec(&.{ "namespace", "create", "hq-a" });
    try ctx.exec(&.{ "namespace", "create", "hq-b" });
    try ctx.exec(&.{ "namespace", "create", "hq-d" });
    // A figure, then back to the default: the default's part, under an
    // index past the figure's, so a restart can't bring the figure back.
    try ctx.exec(&.{ "namespace", "alter", "hq-d", "--history-quota", "30GiB", "--config", ctx.server.config_file });
    try expectBound(ctx, "hq-d", shards, (30 << 30) / shards);
    try ctx.exec(&.{ "namespace", "alter", "hq-d", "--history-quota", "default", "--config", ctx.server.config_file });
    try expectBound(ctx, "hq-d", shards, (10 << 30) / shards);
    // A quota that doesn't divide by the shards: each part rounds up.
    const odd: u64 = (20 << 30) + 1;
    var qb: [32]u8 = undefined;
    try ctx.exec(&.{ "namespace", "alter", "hq-a", "--history-quota", try std.fmt.bufPrint(&qb, "{d}", .{odd}), "--config", ctx.server.config_file });
    try ctx.exec(&.{ "namespace", "alter", "hq-b", "--history-quota", "none", "--config", ctx.server.config_file });
    const part = (odd + shards - 1) / shards;
    try expectBound(ctx, "hq-a", shards, part);
    try expectBound(ctx, "hq-b", shards, null);
    try ctx.restartServer();
    // Replayed: the settings and every shard's bind, with no transient
    // default part on the way.
    {
        const info = try quotaInfo(ctx, "hq-d");
        defer info.deinit();
        try testing.expectEqual(@as(?u64, 10 << 30), info.value.history_quota);
        try testing.expect(!info.value.history_quota_own);
        for (info.value.shards) |s| try testing.expectEqual(@as(?u64, (10 << 30) / shards), s.part_bytes);
    }
    {
        const info = try quotaInfo(ctx, "hq-a");
        defer info.deinit();
        try testing.expectEqual(@as(?u64, odd), info.value.history_quota);
        for (info.value.shards) |s| try testing.expectEqual(@as(?u64, part), s.part_bytes);
    }
    {
        const info = try quotaInfo(ctx, "hq-b");
        defer info.deinit();
        try testing.expectEqual(@as(?u64, null), info.value.history_quota);
        for (info.value.shards) |s| {
            try testing.expect(s.bound);
            try testing.expectEqual(@as(?u64, null), s.part_bytes);
        }
    }
}

test "e2e/namespace: on three nodes, an alter through any member's admin socket, the followers' too, is carried to the leader and bound" {
    var cluster = try stdx.testing.ClusterContext.init(testing.allocator, .{ .node_count = 3 });
    defer cluster.deinit();
    try cluster.execOnWithRetry(0, &.{ "namespace", "create", "hq-c" }, 20, 250);
    // Each member in turn sets its own figure, and each is bound before the
    // next: at least two of them are followers.
    for (0..3) |node| {
        var qb: [16]u8 = undefined;
        const q = try std.fmt.bufPrint(&qb, "{d}GiB", .{20 + node});
        var tries: usize = 0;
        while (true) : (tries += 1) {
            cluster.execAdminOn(node, &.{ "namespace", "alter", "hq-c", "--history-quota", q }) catch |err| {
                if (tries >= 20) return err;
                stdx.time.sleep(250 * std.time.ns_per_ms);
                continue;
            };
            break;
        }
        const bytes: u64 = (20 + node) << 30;
        var want_buf: [96]u8 = undefined;
        const want = try std.fmt.bufPrint(&want_buf, "\"bound\":true,\"part_bytes\":{d}", .{bytes});
        var seen = false;
        var polls: usize = 0;
        while (!seen and polls < 100) : (polls += 1) {
            const out = cluster.execCaptureAnyOn(node, &.{ "namespace", "info", "hq-c", "-o", "json" }) catch {
                stdx.time.sleep(100 * std.time.ns_per_ms);
                continue;
            };
            defer testing.allocator.free(out);
            seen = std.mem.indexOf(u8, out, want) != null;
            if (!seen) stdx.time.sleep(100 * std.time.ns_per_ms);
        }
        try testing.expect(seen);
    }
}
