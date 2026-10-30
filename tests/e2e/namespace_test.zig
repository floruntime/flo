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
    try testing.expectEqualStrings("Namespace:  default\nCreated at: built in\n", builtin.stdout);
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
//
// NOTE: Currently namespace metadata is NOT replicated across cluster nodes.
// Each node maintains its own MetadataCache for namespaces.
// These tests verify basic cluster operations by creating namespaces on
// the node that needs them. Full cluster-wide namespace replication via
// Raft is planned for a future release.
//
// TODO: Implement namespace replication via Raft log
// =============================================================================

const ClusterContext = stdx.testing.ClusterContext;

test "e2e/namespace/cluster: kv operations work across cluster with namespaces" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // In current architecture, namespaces are node-local
    // KV operations auto-create "default" namespace on each node
    // So we can test KV cluster operations which implicitly use namespaces

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
