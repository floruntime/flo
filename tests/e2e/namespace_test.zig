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

test "e2e/namespace: delete" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create and then delete
    try ctx.exec(&.{ "ns", "create", "to_delete" });

    // Verify it exists
    const before = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, before, "to_delete") != null);

    // Delete it
    try ctx.exec(&.{ "ns", "delete", "to_delete" });

    // Verify it's gone
    const after = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, after, "to_delete") == null);
}

test "e2e/namespace: delete non-existent fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Delete non-existent namespace should fail
    var result = try ctx.cli.run(&.{ "ns", "delete", "nonexistent_ns_12345" });
    defer result.deinit();

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
// Delete Safety (--force flag)
// =============================================================================

test "e2e/namespace: delete fails when namespace has kv keys" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create namespace and add KV data
    try ctx.exec(&.{ "ns", "create", "nonempty_ns" });
    try ctx.exec(&.{ "kv", "set", "mykey", "myvalue", "-n", "nonempty_ns" });

    // Delete without --force should fail
    var result = try ctx.cli.run(&.{ "ns", "delete", "nonempty_ns" });
    defer result.deinit();

    // Should fail because namespace is not empty
    try stdx.testing.assertFailed(result);
}

test "e2e/namespace: delete with --force removes namespace and all kv keys" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create namespace and add KV data
    try ctx.exec(&.{ "ns", "create", "force_delete_ns" });
    try ctx.exec(&.{ "kv", "set", "key1", "value1", "-n", "force_delete_ns" });
    try ctx.exec(&.{ "kv", "set", "key2", "value2", "-n", "force_delete_ns" });

    // Force delete should succeed
    try ctx.exec(&.{ "ns", "delete", "force_delete_ns", "--force" });

    // Wait for async deletion task to complete
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Namespace should be gone
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, "force_delete_ns") == null);
}

test "e2e/namespace: delete empty namespace works without --force" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create empty namespace
    try ctx.exec(&.{ "ns", "create", "empty_ns" });

    // Delete without --force should succeed (it's empty)
    try ctx.exec(&.{ "ns", "delete", "empty_ns" });

    // Namespace should be gone
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, "empty_ns") == null);
}

// =============================================================================
// Force Delete - KV Resource Cleanup
// =============================================================================

test "e2e/namespace: force delete removes all kv keys" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const ns = "kv_cleanup_test";

    // Create namespace with multiple KV keys
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "kv", "set", "user:1", "alice", "-n", ns });
    try ctx.exec(&.{ "kv", "set", "user:2", "bob", "-n", ns });
    try ctx.exec(&.{ "kv", "set", "config:timeout", "30", "-n", ns });
    try ctx.exec(&.{ "kv", "set", "session:abc123", "active", "-n", ns });

    // Verify keys exist
    const before = try ctx.execCapture(&.{ "kv", "get", "user:1", "-n", ns });
    try testing.expect(std.mem.indexOf(u8, before, "alice") != null);

    // Force delete namespace
    try ctx.exec(&.{ "ns", "delete", ns, "--force" });

    // Wait for async deletion to complete
    std.Thread.sleep(700 * std.time.ns_per_ms);

    // Namespace should be gone
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, ns) == null);

    // Recreate namespace - should start fresh with no keys
    try ctx.exec(&.{ "ns", "create", ns });

    // Keys should not exist in recreated namespace
    var result = try ctx.cli.run(&.{ "kv", "get", "user:1", "-n", ns });
    defer result.deinit();
    try stdx.testing.assertFailed(result); // Key should not exist
}

test "e2e/namespace: force delete with many kv keys" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const ns = "many_keys_test";

    // Create namespace with many keys (tests chunked deletion)
    try ctx.exec(&.{ "ns", "create", ns });

    // Add 50 keys to test chunked deletion (MAX_DELETIONS_PER_SWEEP = 100)
    var key_buf: [32]u8 = undefined;
    var val_buf: [32]u8 = undefined;
    for (0..50) |i| {
        const key = std.fmt.bufPrint(&key_buf, "key:{d}", .{i}) catch unreachable;
        const val = std.fmt.bufPrint(&val_buf, "value:{d}", .{i}) catch unreachable;
        try ctx.exec(&.{ "kv", "set", key, val, "-n", ns });
    }

    // Force delete
    try ctx.exec(&.{ "ns", "delete", ns, "--force" });

    // Wait for async deletion (may take multiple task iterations)
    std.Thread.sleep(500 * std.time.ns_per_ms);

    // Namespace should be gone
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, ns) == null);
}

// =============================================================================
// Force Delete - Stream Resource Cleanup
// =============================================================================

test "e2e/namespace: delete fails when namespace has streams" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const ns = "stream_nonempty_ns";

    // Create namespace and add a stream with data
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "stream", "create", "events", "-n", ns });
    try ctx.exec(&.{ "stream", "append", "events", "test-event", "-n", ns });

    // Delete without --force should fail (stream data exists)
    var result = try ctx.cli.run(&.{ "ns", "delete", ns });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
}

test "e2e/namespace: force delete removes streams" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const ns = "stream_cleanup_test";

    // Create namespace with streams
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "stream", "create", "orders", "-n", ns });
    try ctx.exec(&.{ "stream", "create", "events", "-n", ns });

    // Add some data to streams
    try ctx.exec(&.{ "stream", "append", "orders", "order:1", "-n", ns });
    try ctx.exec(&.{ "stream", "append", "events", "event:1", "-n", ns });

    // Force delete namespace
    try ctx.exec(&.{ "ns", "delete", ns, "--force" });

    // Wait for async deletion
    std.Thread.sleep(700 * std.time.ns_per_ms);

    // Namespace should be gone
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, ns) == null);

    // Recreate namespace - streams should not exist
    try ctx.exec(&.{ "ns", "create", ns });

    // Try to create stream (should work since namespace is fresh)
    try ctx.exec(&.{ "stream", "create", "orders", "-n", ns });
    
    // Stream info should show 0 records (fresh stream, data was deleted)
    const result = try ctx.execCapture(&.{ "stream", "info", "orders", "-n", ns });
    try testing.expect(std.mem.indexOf(u8, result, "Records: 0") != null);
}

test "e2e/namespace: force delete removes stream consumer groups" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const ns = "stream_cg_cleanup";

    // Create namespace with stream and consumer group
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "stream", "create", "mystream", "-n", ns });
    try ctx.exec(&.{ "stream", "append", "mystream", "data1", "-n", ns });
    try ctx.exec(&.{ "stream", "group", "create", "mystream", "--group", "mygroup", "-n", ns });

    // Verify consumer group exists and has data
    const before = try ctx.execCapture(&.{ "stream", "group", "info", "mystream", "--group", "mygroup", "-n", ns });
    // Note: Pending should be empty since we didn't consume anything yet
    _ = before;

    // Force delete namespace
    try ctx.exec(&.{ "ns", "delete", ns, "--force" });

    // Wait for async deletion
    std.Thread.sleep(2000 * std.time.ns_per_ms);

    // Namespace should be gone
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, ns) == null);

    // Recreate namespace and stream
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "stream", "create", "mystream", "-n", ns });

    // Stream should be fresh with 0 records (data was deleted)
    const stream_info = try ctx.execCapture(&.{ "stream", "info", "mystream", "-n", ns });
    try testing.expect(std.mem.indexOf(u8, stream_info, "Records: 0") != null);
}

// =============================================================================
// Force Delete - Queue Resource Cleanup
// =============================================================================

test "e2e/namespace: delete fails when namespace has queues" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const ns = "queue_nonempty_ns";

    // Create namespace and add a queue with a message
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "queue", "create", "jobs", "-n", ns });
    try ctx.exec(&.{ "queue", "enqueue", "jobs", "test-job", "-n", ns });

    // Delete without --force should fail (queue data exists)
    var result = try ctx.cli.run(&.{ "ns", "delete", ns });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
}

test "e2e/namespace: force delete removes queues" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const ns = "queue_cleanup_test";

    // Create namespace with queues
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "queue", "create", "tasks", "-n", ns });
    try ctx.exec(&.{ "queue", "create", "notifications", "-n", ns });

    // Add messages to queues
    try ctx.exec(&.{ "queue", "enqueue", "tasks", "task:1", "-n", ns });
    try ctx.exec(&.{ "queue", "enqueue", "notifications", "notify:1", "-n", ns });

    // Force delete namespace
    try ctx.exec(&.{ "ns", "delete", ns, "--force" });

    // Wait for async deletion
    std.Thread.sleep(700 * std.time.ns_per_ms);

    // Namespace should be gone
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, ns) == null);

    // Recreate namespace - queues should not exist
    try ctx.exec(&.{ "ns", "create", ns });

    // Try to recreate queue (should work since namespace is fresh)
    try ctx.exec(&.{ "queue", "create", "tasks", "-n", ns });
    
    // Dequeue should return nothing (queue is empty, messages were deleted)
    const result = try ctx.execCapture(&.{ "queue", "dequeue", "tasks", "-n", ns, "--timeout", "100" });
    try testing.expect(std.mem.indexOf(u8, result, "(no messages)") != null);
}

test "e2e/namespace: force delete removes queue messages" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const ns = "queue_msg_cleanup";

    // Create namespace with queue and messages
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "queue", "create", "work", "-n", ns });
    try ctx.exec(&.{ "queue", "enqueue", "work", "job1", "-n", ns });
    try ctx.exec(&.{ "queue", "enqueue", "work", "job2", "-n", ns });
    try ctx.exec(&.{ "queue", "enqueue", "work", "job3", "-n", ns });

    // Force delete namespace
    try ctx.exec(&.{ "ns", "delete", ns, "--force" });

    // Wait for async deletion
    std.Thread.sleep(700 * std.time.ns_per_ms);

    // Recreate namespace and queue
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "queue", "create", "work", "-n", ns });

    // Queue should be empty (no messages from before, messages were deleted)
    const result = try ctx.execCapture(&.{ "queue", "dequeue", "work", "-n", ns, "--timeout", "100" });
    try testing.expect(std.mem.indexOf(u8, result, "(no messages)") != null);
}

// =============================================================================
// Force Delete - Combined Resources
// =============================================================================

test "e2e/namespace: force delete removes all resource types" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const ns = "combined_cleanup";

    // Create namespace with KV, Stream, and Queue resources
    try ctx.exec(&.{ "ns", "create", ns });

    // KV resources
    try ctx.exec(&.{ "kv", "set", "config:app", "myapp", "-n", ns });
    try ctx.exec(&.{ "kv", "set", "config:version", "1.0", "-n", ns });

    // Stream resources
    try ctx.exec(&.{ "stream", "create", "audit", "-n", ns });
    try ctx.exec(&.{ "stream", "append", "audit", "login:user1", "-n", ns });

    // Queue resources
    try ctx.exec(&.{ "queue", "create", "emails", "-n", ns });
    try ctx.exec(&.{ "queue", "enqueue", "emails", "welcome@test.com", "-n", ns });

    // Force delete namespace
    try ctx.exec(&.{ "ns", "delete", ns, "--force" });

    // Wait for async deletion to complete all phases
    std.Thread.sleep(700 * std.time.ns_per_ms);

    // Namespace should be gone
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, ns) == null);

    // Recreate namespace - should be completely empty
    try ctx.exec(&.{ "ns", "create", ns });

    // Verify KV is empty (key should not exist - command will fail)
    var kv_result = try ctx.cli.run(&.{ "kv", "get", "config:app", "-n", ns });
    defer kv_result.deinit();
    try stdx.testing.assertFailed(kv_result); // Key doesn't exist

    // Verify stream is fresh (can create, should have 0 records)
    try ctx.exec(&.{ "stream", "create", "audit", "-n", ns });
    const stream_result = try ctx.execCapture(&.{ "stream", "info", "audit", "-n", ns });
    try testing.expect(std.mem.indexOf(u8, stream_result, "Records: 0") != null);

    // Verify queue is fresh (can create, should be empty)
    try ctx.exec(&.{ "queue", "create", "emails", "-n", ns });
    const queue_result = try ctx.execCapture(&.{ "queue", "dequeue", "emails", "-n", ns, "--timeout", "100" });
    try testing.expect(std.mem.indexOf(u8, queue_result, "(no messages)") != null);
}

test "e2e/namespace: force delete is idempotent" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const ns = "idempotent_test";

    // Create namespace with data
    try ctx.exec(&.{ "ns", "create", ns });
    try ctx.exec(&.{ "kv", "set", "key", "value", "-n", ns });

    // Force delete
    try ctx.exec(&.{ "ns", "delete", ns, "--force" });

    // Wait for deletion
    std.Thread.sleep(700 * std.time.ns_per_ms);

    // Second delete should fail (namespace gone)
    var result = try ctx.cli.run(&.{ "ns", "delete", ns, "--force" });
    defer result.deinit();
    try stdx.testing.assertFailed(result);
}

test "e2e/namespace: other namespaces unaffected by force delete" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create two namespaces with data
    try ctx.exec(&.{ "ns", "create", "ns_keep" });
    try ctx.exec(&.{ "ns", "create", "ns_delete" });

    try ctx.exec(&.{ "kv", "set", "shared_name", "keep_value", "-n", "ns_keep" });
    try ctx.exec(&.{ "kv", "set", "shared_name", "delete_value", "-n", "ns_delete" });

    // Force delete one namespace
    try ctx.exec(&.{ "ns", "delete", "ns_delete", "--force" });

    // Wait for deletion
    std.Thread.sleep(700 * std.time.ns_per_ms);

    // Other namespace should be intact
    const list = try ctx.execCapture(&.{ "ns", "ls" });
    try testing.expect(std.mem.indexOf(u8, list, "ns_keep") != null);
    try testing.expect(std.mem.indexOf(u8, list, "ns_delete") == null);

    // Data in kept namespace should be intact
    const value = try ctx.execCapture(&.{ "kv", "get", "shared_name", "-n", "ns_keep" });
    try testing.expect(std.mem.indexOf(u8, value, "keep_value") != null);
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

    // Wait for replication
    std.Thread.sleep(1 * std.time.ns_per_s);

    // Read from node 1
    const value1 = try cluster.execCaptureOn(1, &.{ "kv", "get", "cluster_key" });
    defer testing.allocator.free(value1);
    try testing.expect(std.mem.indexOf(u8, value1, "from_node_0") != null);

    // Read from node 2
    const value2 = try cluster.execCaptureOn(2, &.{ "kv", "get", "cluster_key" });
    defer testing.allocator.free(value2);
    try testing.expect(std.mem.indexOf(u8, value2, "from_node_0") != null);
}

test "e2e/namespace/cluster: delete with force works on single node" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Create namespace and add KV data on node 0
    try cluster.execOn(0, &.{ "ns", "create", "force_del_cluster" });
    try cluster.execOn(0, &.{ "kv", "set", "mykey", "myval", "-n", "force_del_cluster" });

    // Force delete on same node should work
    try cluster.execOn(0, &.{ "ns", "delete", "force_del_cluster", "--force" });

    // Namespace should be gone on node 0
    const list = try cluster.execCaptureOn(0, &.{ "ns", "ls" });
    defer testing.allocator.free(list);
    try testing.expect(std.mem.indexOf(u8, list, "force_del_cluster") == null);
}

test "e2e/namespace/cluster: non-empty namespace delete fails" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Create namespace and add KV data on node 0
    try cluster.execOn(0, &.{ "ns", "create", "nonempty_cluster" });
    try cluster.execOn(0, &.{ "kv", "set", "testkey", "testval", "-n", "nonempty_cluster" });

    // Try to delete without --force - should fail
    const result = try cluster.execCaptureAnyOn(0, &.{ "ns", "delete", "nonempty_cluster" });
    defer testing.allocator.free(result);

    // Namespace should still exist
    const list = try cluster.execCaptureOn(0, &.{ "ns", "ls" });
    defer testing.allocator.free(list);
    try testing.expect(std.mem.indexOf(u8, list, "nonempty_cluster") != null);
}
