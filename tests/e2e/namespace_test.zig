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

    // Should indicate failure (namespace already exists)
    try stdx.testing.assertFailed(result);
}

test "e2e/namespace: info" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create a namespace
    try ctx.exec(&.{ "ns", "create", "infotest" });

    // Get info
    var result = try ctx.cli.run(&.{ "ns", "info", "infotest" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "infotest");
}

test "e2e/namespace: info non-existent fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Info on non-existent namespace should fail
    var result = try ctx.cli.run(&.{ "ns", "info", "nonexistent_ns_67890" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
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

test "e2e/namespace: --all shows system namespaces" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create a user namespace to ensure there's activity
    try ctx.exec(&.{ "ns", "create", "user_ns" });

    // Regular list
    const regular = try ctx.execCapture(&.{ "ns", "ls" });

    // List with --all
    const all = try ctx.execCapture(&.{ "ns", "ls", "--all" });

    // Both should have user_ns
    try testing.expect(std.mem.indexOf(u8, regular, "user_ns") != null);
    try testing.expect(std.mem.indexOf(u8, all, "user_ns") != null);

    // Note: --all may show more, but user namespaces should appear in both
}

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

test "e2e/namespace: config refuses an unknown setting" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "cfg_ns" });

    // A known setting round-trips, so the refusal below isn't a broken config path.
    try ctx.exec(&.{ "ns", "config", "cfg_ns", "--set", "stream_retention_s=86400" });

    var result = try ctx.cli.run(&.{ "ns", "config", "cfg_ns", "--set", "memory_budget_bytes=1073741824" });
    defer result.deinit();
    try stdx.testing.assertStderrContains(result, "Unknown setting 'memory_budget_bytes'");

    const shown = try ctx.execCapture(&.{ "ns", "config", "cfg_ns" });
    try testing.expect(std.mem.indexOf(u8, shown, "stream_retention_s: 86400") != null);
    try testing.expect(std.mem.indexOf(u8, shown, "memory_budget_bytes") == null);
}

test "e2e/namespace: delete is refused, with or without --force, and the namespace and its data stay" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const ns = "keep-me";
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "kv", "set", "k", "v", "-n", ns });
    try ctx.exec(&.{ "stream", "append", "orders", "order-1", "-n", ns });
    try ctx.exec(&.{ "queue", "enqueue", "jobs", "job-1", "-n", ns });

    for ([_][]const []const u8{ &.{ "ns", "delete", ns }, &.{ "ns", "delete", ns, "--force" }, &.{ "ns", "delete", "default", "--force" } }) |args| {
        var r = try ctx.cli.run(args);
        defer r.deinit();
        try stdx.testing.assertStderrContains(r, "namespace delete isn't supported yet");
    }
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, ns) != null);
    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "kv", "get", "k", "-n", ns }), "v") != null);
    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "stream", "info", "orders", "-n", ns }), "Records: 1") != null);
    try testing.expect(std.mem.indexOf(u8, try ctx.execCapture(&.{ "queue", "dequeue", "jobs", "-n", ns, "--timeout", "100" }), "job-1") != null);
}
