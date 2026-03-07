//! KV End-to-End Tests
//!
//! Tests the complete path: CLI → TCP → Node → Handler → Storage
//! Ported from tests/cli/test_kv.sh
//!
//! Uses FloTestContext convenience methods:
//! - ctx.exec()       - fire-and-forget, asserts success
//! - ctx.execCapture() - returns stdout, auto memory management
//! - ctx.cli.run()    - full control when needed

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

// =============================================================================
// Helper: Parse version from JSON output
// =============================================================================

fn parseVersion(stdout: []const u8) ?u64 {
    const prefix = "\"version\":";
    const idx = std.mem.indexOf(u8, stdout, prefix) orelse return null;
    const start = idx + prefix.len;
    var end = start;
    while (end < stdout.len and std.ascii.isDigit(stdout[end])) {
        end += 1;
    }
    if (start == end) return null;
    return std.fmt.parseInt(u64, stdout[start..end], 10) catch null;
}

// =============================================================================
// Basic Operations
// =============================================================================

test "e2e/kv: set and get basic" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv set key1 "hello world"
    try ctx.exec(&.{ "kv", "set", "key1", "hello world" });

    // flo kv get key1
    const value = try ctx.execCapture(&.{ "kv", "get", "key1" });
    try testing.expect(std.mem.indexOf(u8, value, "hello world") != null);
}

test "e2e/kv: get non-existent key returns nil" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv get nonexistent --format table
    var result = try ctx.cli.run(&.{ "kv", "get", "nonexistent", "--format", "table" });
    defer result.deinit();
    try stdx.testing.assertContains(result, "(nil)");
}

test "e2e/kv: multiple keys" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv set user:1 '{"name":"alice"}'
    try ctx.exec(&.{ "kv", "set", "user:1", "{\"name\":\"alice\"}" });
    try ctx.exec(&.{ "kv", "set", "user:2", "{\"name\":\"bob\"}" });
    try ctx.exec(&.{ "kv", "set", "user:3", "{\"name\":\"charlie\"}" });

    // flo kv get user:2
    const value = try ctx.execCapture(&.{ "kv", "get", "user:2" });
    try testing.expect(std.mem.indexOf(u8, value, "bob") != null);
}

test "e2e/kv: ls lists all keys" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Set up test data
    try ctx.exec(&.{ "kv", "set", "ls_alpha", "a" });
    try ctx.exec(&.{ "kv", "set", "ls_beta", "b" });
    try ctx.exec(&.{ "kv", "set", "ls_gamma", "c" });

    // flo kv ls
    var result = try ctx.cli.run(&.{ "kv", "ls" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "ls_alpha");
    try stdx.testing.assertContains(result, "ls_beta");
    try stdx.testing.assertContains(result, "ls_gamma");
}

test "e2e/kv: ls with no keys shows empty" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv ls on fresh server — should succeed with no keys
    var result = try ctx.cli.run(&.{ "kv", "ls" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "no keys");
}

test "e2e/kv: list with prefix" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Set up test data
    try ctx.exec(&.{ "kv", "set", "user:1", "alice" });
    try ctx.exec(&.{ "kv", "set", "user:2", "bob" });
    try ctx.exec(&.{ "kv", "set", "user:3", "charlie" });
    try ctx.exec(&.{ "kv", "set", "other:1", "other" });

    // flo kv list --prefix user:
    var result = try ctx.cli.run(&.{ "kv", "list", "--prefix", "user:" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "user:1");
    try stdx.testing.assertContains(result, "user:2");
    try stdx.testing.assertContains(result, "user:3");
}

test "e2e/kv: ls with --limit" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Set up several keys
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "limit_key_{d}", .{i}) catch unreachable;
        try ctx.exec(&.{ "kv", "set", key, "v" });
    }

    // flo kv ls --limit 3
    var result = try ctx.cli.run(&.{ "kv", "ls", "--limit", "3" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

test "e2e/kv: delete" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv set to_delete value
    try ctx.exec(&.{ "kv", "set", "to_delete", "value" });

    // Verify it exists
    const before = try ctx.execCapture(&.{ "kv", "get", "to_delete" });
    try testing.expect(std.mem.indexOf(u8, before, "value") != null);

    // flo kv delete to_delete
    try ctx.exec(&.{ "kv", "delete", "to_delete" });

    // Verify it's gone
    var after = try ctx.cli.run(&.{ "kv", "get", "to_delete", "--format", "table" });
    defer after.deinit();
    try stdx.testing.assertContains(after, "(nil)");
}

test "e2e/kv: overwrite value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "key1", "original value" });
    try ctx.exec(&.{ "kv", "set", "key1", "updated value" });

    const value = try ctx.execCapture(&.{ "kv", "get", "key1" });
    try testing.expect(std.mem.indexOf(u8, value, "updated value") != null);
}

// =============================================================================
// Output Formats
// =============================================================================

test "e2e/kv: json output format" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "json_key", "json value" });

    // flo kv get json_key --format json
    var result = try ctx.cli.run(&.{ "kv", "get", "json_key", "--format", "json" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "json value");
    try stdx.testing.assertContains(result, "version");
}

// =============================================================================
// Conditional Operations: --nx (Not eXists)
// =============================================================================

test "e2e/kv: set --nx on new key succeeds" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv set nx_test_key first_value --nx
    try ctx.exec(&.{ "kv", "set", "nx_test_key", "first_value", "--nx" });

    // Verify value was set
    const value = try ctx.execCapture(&.{ "kv", "get", "nx_test_key" });
    try testing.expect(std.mem.indexOf(u8, value, "first_value") != null);
}

test "e2e/kv: set --nx on existing key fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // First set
    try ctx.exec(&.{ "kv", "set", "nx_existing", "first_value" });

    // flo kv set nx_existing second_value --nx (should fail)
    var result = try ctx.cli.run(&.{ "kv", "set", "nx_existing", "second_value", "--nx" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
    try stdx.testing.assertContains(result, "already exists");
}

test "e2e/kv: set --nx preserves original value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "nx_preserve", "original" });

    // flo kv set nx_preserve new --nx (should fail, but we don't assert)
    var r2 = try ctx.cli.run(&.{ "kv", "set", "nx_preserve", "new", "--nx" });
    defer r2.deinit();

    // Value should still be original
    const value = try ctx.execCapture(&.{ "kv", "get", "nx_preserve" });
    try testing.expect(std.mem.indexOf(u8, value, "original") != null);
}

// =============================================================================
// Conditional Operations: --xx (eXists)
// =============================================================================

test "e2e/kv: set --xx on existing key succeeds" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "xx_test_key", "first_value" });

    // flo kv set xx_test_key updated_value --xx
    try ctx.exec(&.{ "kv", "set", "xx_test_key", "updated_value", "--xx" });

    // Verify update
    const value = try ctx.execCapture(&.{ "kv", "get", "xx_test_key" });
    try testing.expect(std.mem.indexOf(u8, value, "updated_value") != null);
}

test "e2e/kv: set --xx on non-existent key fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv set xx_nonexistent some_value --xx
    var result = try ctx.cli.run(&.{ "kv", "set", "xx_nonexistent", "some_value", "--xx" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
    try stdx.testing.assertContains(result, "does not exist");
}

test "e2e/kv: set --nx --xx fails (mutually exclusive)" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv set some_key value --nx --xx
    var result = try ctx.cli.run(&.{ "kv", "set", "some_key", "value", "--nx", "--xx" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
    try stdx.testing.assertContains(result, "Cannot use both");
}

// =============================================================================
// Conditional Operations: --cas (Compare-and-Swap)
// =============================================================================

test "e2e/kv: set --cas with correct version succeeds" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "cas_test_key", "initial" });

    // flo kv get cas_test_key --format json (to get version)
    const json_output = try ctx.execCapture(&.{ "kv", "get", "cas_test_key", "--format", "json" });

    const version = parseVersion(json_output) orelse return error.NoVersion;
    var version_buf: [32]u8 = undefined;
    const version_str = try std.fmt.bufPrint(&version_buf, "{d}", .{version});

    // flo kv set cas_test_key updated_via_cas --cas <version>
    try ctx.exec(&.{ "kv", "set", "cas_test_key", "updated_via_cas", "--cas", version_str });

    // Verify update
    const value = try ctx.execCapture(&.{ "kv", "get", "cas_test_key" });
    try testing.expect(std.mem.indexOf(u8, value, "updated_via_cas") != null);
}

test "e2e/kv: set --cas with wrong version fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "cas_wrong", "initial" });

    // flo kv set cas_wrong should_not_work --cas 999999
    var result = try ctx.cli.run(&.{ "kv", "set", "cas_wrong", "should_not_work", "--cas", "999999" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
    try stdx.testing.assertContains(result, "Version mismatch");
}

test "e2e/kv: failed CAS preserves original value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "cas_preserve", "original_value" });

    // flo kv set cas_preserve new_value --cas 999999 (wrong version)
    var r2 = try ctx.cli.run(&.{ "kv", "set", "cas_preserve", "new_value", "--cas", "999999" });
    defer r2.deinit();

    // Value should still be original
    const value = try ctx.execCapture(&.{ "kv", "get", "cas_preserve" });
    try testing.expect(std.mem.indexOf(u8, value, "original_value") != null);
}

test "e2e/kv: set --cas --nx fails (incompatible)" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv set cas_nx_key value --cas 1 --nx
    var result = try ctx.cli.run(&.{ "kv", "set", "cas_nx_key", "value", "--cas", "1", "--nx" });
    defer result.deinit();

    try stdx.testing.assertFailed(result);
    try stdx.testing.assertContains(result, "Cannot use --cas with --nx");
}

// =============================================================================
// Edge Cases
// =============================================================================

test "e2e/kv: empty value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv set empty_value_key ""
    try ctx.exec(&.{ "kv", "set", "empty_value_key", "" });

    // flo kv get empty_value_key
    var result = try ctx.cli.run(&.{ "kv", "get", "empty_value_key" });
    defer result.deinit();

    // Empty value behavior depends on implementation
    // Either null (not found) or empty string - both acceptable
}

test "e2e/kv: special characters in key" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "key:with:colons", "value1" });
    try ctx.exec(&.{ "kv", "set", "key.with.dots", "value2" });
    try ctx.exec(&.{ "kv", "set", "key-with-dashes", "value3" });
    try ctx.exec(&.{ "kv", "set", "key_with_underscores", "value4" });

    // Verify each
    const v1 = try ctx.execCapture(&.{ "kv", "get", "key:with:colons" });
    try testing.expect(std.mem.indexOf(u8, v1, "value1") != null);

    const v2 = try ctx.execCapture(&.{ "kv", "get", "key.with.dots" });
    try testing.expect(std.mem.indexOf(u8, v2, "value2") != null);
}

test "e2e/kv: large value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create a reasonably large value (1KB)
    var large_value: [1024]u8 = undefined;
    for (&large_value, 0..) |*c, i| {
        c.* = @intCast('a' + (i % 26));
    }

    // flo kv set large_key <1KB of data>
    try ctx.exec(&.{ "kv", "set", "large_key", &large_value });

    // flo kv get large_key
    const value = try ctx.execCapture(&.{ "kv", "get", "large_key" });
    try testing.expect(value.len >= 1024);
}

// =============================================================================
// TTL (Time-To-Live) Operations
// =============================================================================

test "e2e/kv: set with TTL expires after timeout" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv set ttl_key ttl_value --ttl 1 (1 second)
    try ctx.exec(&.{ "kv", "set", "ttl_key", "ttl_value", "--ttl", "1" });

    // Immediately readable
    const before = try ctx.execCapture(&.{ "kv", "get", "ttl_key" });
    try testing.expect(std.mem.indexOf(u8, before, "ttl_value") != null);

    // Wait for expiration (1.5 seconds to be safe)
    std.Thread.sleep(1500 * std.time.ns_per_ms);

    // Should be expired now
    var after = try ctx.cli.run(&.{ "kv", "get", "ttl_key", "--format", "table" });
    defer after.deinit();
    try stdx.testing.assertContains(after, "(nil)");
}

test "e2e/kv: set with TTL 0 means no expiration" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv set no_ttl_key value --ttl 0 (explicit no TTL)
    try ctx.exec(&.{ "kv", "set", "no_ttl_key", "permanent_value", "--ttl", "0" });

    // Small wait to verify it doesn't expire immediately
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Should still be readable
    const value = try ctx.execCapture(&.{ "kv", "get", "no_ttl_key" });
    try testing.expect(std.mem.indexOf(u8, value, "permanent_value") != null);
}

test "e2e/kv: overwrite resets TTL" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Set with short TTL
    try ctx.exec(&.{ "kv", "set", "ttl_reset_key", "initial", "--ttl", "1" });

    // Overwrite with no TTL (should persist)
    try ctx.exec(&.{ "kv", "set", "ttl_reset_key", "updated" });

    // Wait past original TTL
    std.Thread.sleep(1500 * std.time.ns_per_ms);

    // Should still exist (TTL was reset)
    const value = try ctx.execCapture(&.{ "kv", "get", "ttl_reset_key" });
    try testing.expect(std.mem.indexOf(u8, value, "updated") != null);
}

test "e2e/kv: TTL with conditional --nx" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv set ttl_nx_key value --ttl 2 --nx
    try ctx.exec(&.{ "kv", "set", "ttl_nx_key", "first", "--ttl", "2", "--nx" });

    // Verify value exists
    const v1 = try ctx.execCapture(&.{ "kv", "get", "ttl_nx_key" });
    try testing.expect(std.mem.indexOf(u8, v1, "first") != null);

    // Try to set again with --nx (should fail)
    var result = try ctx.cli.run(&.{ "kv", "set", "ttl_nx_key", "second", "--ttl", "2", "--nx" });
    defer result.deinit();
    try stdx.testing.assertFailed(result);

    // Wait for expiration
    std.Thread.sleep(2500 * std.time.ns_per_ms);

    // Now should be able to set with --nx
    try ctx.exec(&.{ "kv", "set", "ttl_nx_key", "third", "--ttl", "2", "--nx" });

    const v3 = try ctx.execCapture(&.{ "kv", "get", "ttl_nx_key" });
    try testing.expect(std.mem.indexOf(u8, v3, "third") != null);
}

// =============================================================================
// Delete Operations
// =============================================================================

test "e2e/kv: delete non-existent key" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo kv delete never_existed
    // Deleting non-existent key should not error (idempotent)
    var result = try ctx.cli.run(&.{ "kv", "delete", "never_existed" });
    defer result.deinit();
    // May succeed or return not found - both are acceptable
}

test "e2e/kv: delete then get returns nil" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "delete_me", "value" });
    try ctx.exec(&.{ "kv", "delete", "delete_me" });

    // flo kv get delete_me --format table
    var result = try ctx.cli.run(&.{ "kv", "get", "delete_me", "--format", "table" });
    defer result.deinit();
    try stdx.testing.assertContains(result, "(nil)");
}

// =============================================================================
// Concurrent Access (basic)
// =============================================================================

test "e2e/kv: sequential operations" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Perform many sequential operations
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "seq_key_{d}", .{i}) catch unreachable;

        var val_buf: [32]u8 = undefined;
        const val = std.fmt.bufPrint(&val_buf, "seq_value_{d}", .{i}) catch unreachable;

        // flo kv set seq_key_<i> seq_value_<i>
        try ctx.exec(&.{ "kv", "set", key, val });
    }

    // Verify a few
    const v50 = try ctx.execCapture(&.{ "kv", "get", "seq_key_50" });
    try testing.expect(std.mem.indexOf(u8, v50, "seq_value_50") != null);

    const v99 = try ctx.execCapture(&.{ "kv", "get", "seq_key_99" });
    try testing.expect(std.mem.indexOf(u8, v99, "seq_value_99") != null);
}

// =============================================================================
// Durability & Crash Recovery
// =============================================================================

test "e2e/kv: data persists across server restart (sync durability)" {
    // Use sync durability to ensure data is on disk before server responds
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    // Set multiple keys before restart
    try ctx.exec(&.{ "kv", "set", "persist_key_1", "persist_value_1" });
    try ctx.exec(&.{ "kv", "set", "persist_key_2", "persist_value_2" });
    try ctx.exec(&.{ "kv", "set", "persist_key_3", "persist_value_3" });

    // Verify all data is readable before restart
    const before1 = try ctx.execCapture(&.{ "kv", "get", "persist_key_1" });
    try testing.expect(std.mem.indexOf(u8, before1, "persist_value_1") != null);

    const before2 = try ctx.execCapture(&.{ "kv", "get", "persist_key_2" });
    try testing.expect(std.mem.indexOf(u8, before2, "persist_value_2") != null);

    // Restart the server
    try ctx.restartServer();

    // With sync durability, all data MUST persist across restart
    // This is the critical persistence test - not just server recovery

    // Verify ALL pre-restart data is still available
    const after1 = try ctx.execCapture(&.{ "kv", "get", "persist_key_1" });
    try testing.expect(std.mem.indexOf(u8, after1, "persist_value_1") != null);

    const after2 = try ctx.execCapture(&.{ "kv", "get", "persist_key_2" });
    try testing.expect(std.mem.indexOf(u8, after2, "persist_value_2") != null);

    const after3 = try ctx.execCapture(&.{ "kv", "get", "persist_key_3" });
    try testing.expect(std.mem.indexOf(u8, after3, "persist_value_3") != null);

    // Verify new writes work after restart
    try ctx.exec(&.{ "kv", "set", "post_restart_key", "post_restart_value" });
    const post = try ctx.execCapture(&.{ "kv", "get", "post_restart_key" });
    try testing.expect(std.mem.indexOf(u8, post, "post_restart_value") != null);
}

test "e2e/kv: immediate consistency after restart (regression)" {
    // Regression test: After server restart, the first few GET/SET operations
    // were returning stale data before the system "stabilized".
    // This tests that read-after-write consistency is immediate, not eventual.
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    // Set initial value before restart
    try ctx.exec(&.{ "kv", "set", "consistency_key", "initial_value" });

    // Verify it's set
    const before = try ctx.execCapture(&.{ "kv", "get", "consistency_key" });
    try testing.expect(std.mem.indexOf(u8, before, "initial_value") != null);

    // Restart the server
    try ctx.restartServer();

    // Immediately after restart, do rapid set/get cycles
    // Each SET should be immediately visible to the next GET
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        var value_buf: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buf, "value_iteration_{d}", .{i});

        // SET the new value
        try ctx.exec(&.{ "kv", "set", "consistency_key", value });

        // Immediately GET - should return what we just set, not stale data
        const got = try ctx.execCapture(&.{ "kv", "get", "consistency_key" });

        // Critical assertion: read-after-write must be consistent
        if (std.mem.indexOf(u8, got, value) == null) {
            std.debug.print("\n[CONSISTENCY FAILURE] iteration {d}:\n", .{i});
            std.debug.print("  Expected: {s}\n", .{value});
            std.debug.print("  Got:      {s}\n", .{got});
            return error.ConsistencyViolation;
        }
    }

    // Also test that the final value persists correctly
    const final = try ctx.execCapture(&.{ "kv", "get", "consistency_key" });
    try testing.expect(std.mem.indexOf(u8, final, "value_iteration_9") != null);
}

test "e2e/kv: multiple keys consistency after restart" {
    // Test that different keys (potentially routed to different shards) are
    // all consistent immediately after restart
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    // Set values on multiple keys (likely different shards due to hashing)
    const keys = [_][]const u8{ "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta" };

    for (keys, 0..) |key, i| {
        var value_buf: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buf, "pre_restart_{d}", .{i});
        try ctx.exec(&.{ "kv", "set", key, value });
    }

    // Restart
    try ctx.restartServer();

    // Immediately update all keys and verify consistency
    for (keys, 0..) |key, i| {
        var value_buf: [32]u8 = undefined;
        const new_value = try std.fmt.bufPrint(&value_buf, "post_restart_{d}", .{i});

        // SET new value
        try ctx.exec(&.{ "kv", "set", key, new_value });

        // GET should return new value immediately
        const got = try ctx.execCapture(&.{ "kv", "get", key });

        if (std.mem.indexOf(u8, got, new_value) == null) {
            std.debug.print("\n[MULTI-KEY CONSISTENCY FAILURE] key={s}:\n", .{key});
            std.debug.print("  Expected: {s}\n", .{new_value});
            std.debug.print("  Got:      {s}\n", .{got});
            return error.ConsistencyViolation;
        }
    }
}

// =============================================================================
// 3-Node Cluster Tests
// =============================================================================
//
// Multi-node Raft clustering is IMPLEMENTED and working. These tests exercise:
// - Cluster formation via --join flag
// - Leader election and re-election
// - Data replication across nodes (AppendEntries RPC)
// - Read-after-write consistency from any node
// - Node failure tolerance (2/3 quorum)
//
// Known issue: The "writes continue after leader failure" test is flaky due to
// a peer reconnection backoff bug - after leader kill, remaining nodes may take
// too long to clear BackoffPending state and re-establish commit quorum.
// See: LEADER_FAILOVER_FIX.md for details.
//
// To run cluster tests:
//   zig build test-e2e -Dtest-filter="cluster"
// =============================================================================

// Use shared ClusterContext from stdx.testing
const ClusterContext = stdx.testing.ClusterContext;

test "e2e/kv/cluster: write on node1 readable from node2 and node3" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Write on node 1
    try cluster.execOn(0, &.{ "kv", "set", "cluster_key_1", "value_from_node1" });

    // Small delay for replication
    std.Thread.sleep(500 * std.time.ns_per_ms);

    // Read from node 2
    const value2 = try cluster.execCaptureOn(1, &.{ "kv", "get", "cluster_key_1" });
    defer testing.allocator.free(value2);
    try testing.expect(std.mem.indexOf(u8, value2, "value_from_node1") != null);

    // Read from node 3
    const value3 = try cluster.execCaptureOn(2, &.{ "kv", "get", "cluster_key_1" });
    defer testing.allocator.free(value3);
    try testing.expect(std.mem.indexOf(u8, value3, "value_from_node1") != null);
}

test "e2e/kv/cluster: data available after node1 dies" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Write on node 0
    try cluster.execOn(0, &.{ "kv", "set", "survive_key", "must_survive" });

    // Small delay for replication
    std.Thread.sleep(500 * std.time.ns_per_ms);

    // Verify replication before killing
    const before_value = try cluster.execCaptureOn(1, &.{ "kv", "get", "survive_key" });
    defer testing.allocator.free(before_value);

    // Kill node 0
    cluster.stopNode(0);

    // Small delay for signal propagation
    std.Thread.sleep(500 * std.time.ns_per_ms);

    // Give cluster time to detect failure and re-elect leader
    std.Thread.sleep(2 * std.time.ns_per_s);

    // Data should still be readable from node 2
    const value2 = try cluster.execCaptureOn(1, &.{ "kv", "get", "survive_key" });
    defer testing.allocator.free(value2);
    try testing.expect(std.mem.indexOf(u8, value2, "must_survive") != null);

    // Data should still be readable from node 3
    const value3 = try cluster.execCaptureOn(2, &.{ "kv", "get", "survive_key" });
    defer testing.allocator.free(value3);
    try testing.expect(std.mem.indexOf(u8, value3, "must_survive") != null);
}

test "e2e/kv/cluster: writes continue after leader failure" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    std.debug.print("\n=== Log file paths ===\n", .{});
    for (0..3) |i| {
        if (cluster.servers[i]) |s| {
            std.debug.print("Node {d}: {s}\n", .{ i, s.log_file_path });
        }
    }
    std.debug.print("======================\n", .{});

    // Initial write on node 1
    try cluster.execOn(0, &.{ "kv", "set", "before_failure", "initial_value" });
    std.debug.print("Write to node 0 succeeded\n", .{});

    // Small delay for replication
    std.Thread.sleep(500 * std.time.ns_per_ms);

    // Kill node 1 (seed/original leader)
    const kill_time = std.time.milliTimestamp();
    std.debug.print("Killing node 0... (time={d})\n", .{kill_time});
    cluster.stopNode(0);
    const kill_done_time = std.time.milliTimestamp();
    std.debug.print("Kill complete after {d}ms\n", .{kill_done_time - kill_time});

    // Give cluster time to elect new leader
    // Note: Election timeout is 150-300ms, but we need extra time for:
    // - Followers to detect leader absence (1-2 election timeouts)
    // - Pre-vote phase (may fail if peers not yet discovered)
    // - Vote phase (requires majority)
    // - New leader to initialize
    // - Peer connections to re-establish (backoff can add latency)
    // - Raft commit to complete (needs majority acknowledgment)
    std.debug.print("Waiting 10 seconds for new leader election...\n", .{});
    std.Thread.sleep(10 * std.time.ns_per_s);
    const write_attempt_time = std.time.milliTimestamp();
    std.debug.print("10s wait complete, attempting write (total time since kill: {d}ms)\n", .{write_attempt_time - kill_time});

    // New writes should work via node 2 (which should become leader or forward to new leader)
    // Retry with exponential backoff to handle peer connection issues
    // Issue: After leader kill, peer connections may fail with BackoffPending
    // which prevents commit quorum. More retries with longer waits help.
    std.debug.print("Attempting write to node 1...\n", .{});
    var write_success = false;
    var last_err: ?anyerror = null;
    const retry_delays = [_]u64{ 1, 2, 3, 4, 5, 6 }; // 6 retries: 1+2+3+4+5+6 = 21 seconds max
    for (retry_delays, 0..) |delay, attempt| {
        if (attempt > 0) {
            std.debug.print("Retry attempt {d} (waiting {d}s)...\n", .{ attempt + 1, delay });
            std.Thread.sleep(delay * std.time.ns_per_s);
        }
        cluster.execOn(1, &.{ "kv", "set", "after_failure", "new_value" }) catch |err| {
            last_err = err;
            continue;
        };
        write_success = true;
        break;
    }

    if (!write_success) {
        std.debug.print("\n=== Write to node 1 failed after retries, dumping logs ===\n", .{});
        for (0..3) |i| {
            if (cluster.servers[i]) |s| {
                std.debug.print("\n--- Node {d} log ({s}) ---\n", .{ i, s.log_file_path });
                const log_file = std.fs.openFileAbsolute(s.log_file_path, .{}) catch |e| {
                    std.debug.print("Could not open: {}\n", .{e});
                    continue;
                };
                defer log_file.close();
                // Use a larger buffer to capture more logs (256KB)
                var buf: [262144]u8 = undefined;
                const n = log_file.readAll(&buf) catch 0;
                std.debug.print("{s}\n", .{buf[0..n]});
            }
        }
        return last_err orelse error.UnknownError;
    }
    std.debug.print("Write to node 1 succeeded!\n", .{});

    // Both old and new data should be readable from node 3
    // Use retry: after leader kill + re-election, replication may be delayed under load
    const old_value = try cluster.execCaptureOnWithRetry(2, &.{ "kv", "get", "before_failure" }, 5, 1000);
    defer testing.allocator.free(old_value);
    try testing.expect(std.mem.indexOf(u8, old_value, "initial_value") != null);

    const new_value = try cluster.execCaptureOnWithRetry(2, &.{ "kv", "get", "after_failure" }, 5, 1000);
    defer testing.allocator.free(new_value);
    try testing.expect(std.mem.indexOf(u8, new_value, "new_value") != null);
}

test "e2e/kv/cluster: all nodes can write" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Write from each node
    try cluster.execOn(0, &.{ "kv", "set", "from_node1", "value1" });
    try cluster.execOn(1, &.{ "kv", "set", "from_node2", "value2" });
    try cluster.execOn(2, &.{ "kv", "set", "from_node3", "value3" });

    // All values should be readable from any node (retry to tolerate replication delay under load)
    const v1_from_1 = try cluster.execCaptureOnWithRetry(0, &.{ "kv", "get", "from_node1" }, 5, 500);
    defer testing.allocator.free(v1_from_1);
    try testing.expect(std.mem.indexOf(u8, v1_from_1, "value1") != null);

    const v2_from_1 = try cluster.execCaptureOnWithRetry(0, &.{ "kv", "get", "from_node2" }, 5, 500);
    defer testing.allocator.free(v2_from_1);
    try testing.expect(std.mem.indexOf(u8, v2_from_1, "value2") != null);

    const v3_from_1 = try cluster.execCaptureOnWithRetry(0, &.{ "kv", "get", "from_node3" }, 5, 500);
    defer testing.allocator.free(v3_from_1);
    try testing.expect(std.mem.indexOf(u8, v3_from_1, "value3") != null);
}

test "e2e/kv/cluster: conditional operations work across cluster" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Set initial value on node 1
    try cluster.execOn(0, &.{ "kv", "set", "cas_cluster_key", "initial" });

    // Get version from node 2 (retry to tolerate replication delay)
    const json_output = try cluster.execCaptureOnWithRetry(1, &.{ "kv", "get", "cas_cluster_key", "--format", "json" }, 5, 500);
    defer testing.allocator.free(json_output);

    const version = parseVersion(json_output) orelse return error.NoVersion;
    var version_buf: [32]u8 = undefined;
    const version_str = try std.fmt.bufPrint(&version_buf, "{d}", .{version});

    // CAS update from node 3 using version from node 2
    try cluster.execOn(2, &.{ "kv", "set", "cas_cluster_key", "updated_via_cas", "--cas", version_str });

    // Verify update from node 1 (poll: GET succeeds but may return stale data until replication catches up)
    const final = try cluster.pollUntilContains(0, &.{ "kv", "get", "cas_cluster_key" }, "updated_via_cas", 10, 500);
    defer testing.allocator.free(final);
}

test "e2e/kv/cluster: delete replicates across cluster" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Create on node 1
    try cluster.execOn(0, &.{ "kv", "set", "to_delete_cluster", "exists" });

    // Verify from node 2 (retry to tolerate replication delay)
    const before = try cluster.execCaptureOnWithRetry(1, &.{ "kv", "get", "to_delete_cluster" }, 5, 500);
    defer testing.allocator.free(before);
    try testing.expect(std.mem.indexOf(u8, before, "exists") != null);

    // Delete from node 3
    try cluster.execOn(2, &.{ "kv", "delete", "to_delete_cluster" });

    // Verify deletion from node 1 (poll: GET returns stale value until delete replicates)
    const after = try cluster.pollAnyUntilContains(0, &.{ "kv", "get", "to_delete_cluster", "--format", "table" }, "(nil)", 10, 500);
    defer testing.allocator.free(after);
}

// =============================================================================
// Blocking GET Operations
// =============================================================================

test "e2e/kv: blocking get receives value when key is set" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const key = "blocking_test_key";
    const value = "hello_from_blocking";

    // Start blocking GET in background (5 second timeout)
    var async_get = try ctx.cli.runAsync(&.{ "kv", "get", key, "--wait", "5000" });
    defer async_get.deinit();

    // Small delay to ensure blocking GET is registered
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Set the key from another "client"
    try ctx.exec(&.{ "kv", "set", key, value });

    // Wait for blocking GET to complete
    var result = try async_get.wait();
    defer result.deinit();

    // Verify blocking GET received the value
    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, value);
}

test "e2e/kv: blocking get with infinite timeout receives value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const key = "blocking_infinite_key";
    const value = "infinite_wait_value";

    // Start blocking GET with infinite timeout (--wait 0)
    var async_get = try ctx.cli.runAsync(&.{ "kv", "get", key, "--wait", "0" });
    defer async_get.deinit();

    // Small delay to ensure blocking GET is registered
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Set the key
    try ctx.exec(&.{ "kv", "set", key, value });

    // Wait for blocking GET to complete
    var result = try async_get.wait();
    defer result.deinit();

    // Verify blocking GET received the value
    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, value);
}

test "e2e/kv: blocking get on existing key waits for update" {
    // --block semantics = "block for changes" (waits for NEXT version even if key exists) - like stream/queue --block
    // --wait semantics = "wait until exists" (returns immediately if key already present)
    // This test needs --block since we want to wait for the NEXT version change
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const key = "blocking_existing_key";
    const initial_value = "initial";
    const updated_value = "updated";

    // Set the key first
    try ctx.exec(&.{ "kv", "set", key, initial_value });

    // Start blocking for NEXT version change (--block, not --wait)
    var async_get = try ctx.cli.runAsync(&.{ "kv", "get", key, "--block", "5000" });
    defer async_get.deinit();

    // Small delay to ensure watcher is registered
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Update the key to trigger the watcher
    try ctx.exec(&.{ "kv", "set", key, updated_value });

    // Wait for watch to complete
    var result = try async_get.wait();
    defer result.deinit();

    // Should receive the updated value
    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, updated_value);
}

test "e2e/kv: blocking get times out when key not set" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const key = "blocking_timeout_key";

    // Blocking GET with short timeout (500ms) should timeout
    var result = try ctx.cli.run(&.{ "kv", "get", key, "--wait", "500" });
    defer result.deinit();

    // Should return (nil) after timeout, not an error
    try testing.expect(result.stdoutContains("(nil)") or result.stderrContains("timed out") or !result.succeeded());
}

// =============================================================================
// Multi-Shard Tests
// =============================================================================

test "e2e/kv: list returns all keys across shards" {
    // Start server with 4 shards — keys hash to different shards
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 4 },
    });
    defer ctx.deinit();

    // Write 8 distinctly-named keys. With 4 shards and Wyhash routing,
    // these are very likely to land on at least 2 different shards.
    const keys = [_][]const u8{
        "alpha_cpu", "bravo_mem",   "charlie_disk", "delta_net",
        "echo_iops", "foxtrot_lat", "golf_tput",    "hotel_err",
    };

    for (keys) |k| {
        try ctx.exec(&.{ "kv", "set", k, "v" });
    }

    // kv list should return ALL keys regardless of shard placement
    var result = try ctx.cli.run(&.{ "kv", "ls" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);

    // Every key we wrote must appear in the listing
    for (keys) |k| {
        try stdx.testing.assertContains(result, k);
    }
}

test "e2e/kv: list with prefix returns correct subset across shards" {
    // Multi-shard prefix scan — walk infrastructure passes filter to each shard.
    // Each shard applies the prefix filter locally, so all matching keys
    // are returned regardless of which shard they land on.
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 4 },
    });
    defer ctx.deinit();

    // Write keys with two different prefixes — distributed across shards
    try ctx.exec(&.{ "kv", "set", "user:alice", "a" });
    try ctx.exec(&.{ "kv", "set", "user:bob", "b" });
    try ctx.exec(&.{ "kv", "set", "user:charlie", "c" });
    try ctx.exec(&.{ "kv", "set", "order:1001", "x" });
    try ctx.exec(&.{ "kv", "set", "order:1002", "y" });

    // Prefix scan with walk — returns all user: keys from all shards
    var result = try ctx.cli.run(&.{ "kv", "list", "--prefix", "user:" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "user:alice");
    try stdx.testing.assertContains(result, "user:bob");
    try stdx.testing.assertContains(result, "user:charlie");
}

// =============================================================================
// Namespace Isolation
// =============================================================================

test "e2e/kv: same key in different namespaces are independent" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create two namespaces
    try ctx.exec(&.{ "ns", "create", "kv_ns_a" });
    try ctx.exec(&.{ "ns", "create", "kv_ns_b" });

    // Set the same key with different values in different namespaces
    try ctx.exec(&.{ "kv", "set", "config", "value_from_a", "-n", "kv_ns_a" });
    try ctx.exec(&.{ "kv", "set", "config", "value_from_b", "-n", "kv_ns_b" });

    // Each namespace returns its own value
    const val_a = try ctx.execCapture(&.{ "kv", "get", "config", "-n", "kv_ns_a" });
    try testing.expect(std.mem.indexOf(u8, val_a, "value_from_a") != null);

    const val_b = try ctx.execCapture(&.{ "kv", "get", "config", "-n", "kv_ns_b" });
    try testing.expect(std.mem.indexOf(u8, val_b, "value_from_b") != null);
}

test "e2e/kv: delete in one namespace does not affect another" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "kv_del_a" });
    try ctx.exec(&.{ "ns", "create", "kv_del_b" });

    // Set the same key in both namespaces
    try ctx.exec(&.{ "kv", "set", "shared", "alpha", "-n", "kv_del_a" });
    try ctx.exec(&.{ "kv", "set", "shared", "beta", "-n", "kv_del_b" });

    // Delete in namespace A
    try ctx.exec(&.{ "kv", "delete", "shared", "-n", "kv_del_a" });

    // Namespace A should be nil
    var result_a = try ctx.cli.run(&.{ "kv", "get", "shared", "-n", "kv_del_a", "--format", "table" });
    defer result_a.deinit();
    try stdx.testing.assertContains(result_a, "(nil)");

    // Namespace B should still have its value
    const val_b = try ctx.execCapture(&.{ "kv", "get", "shared", "-n", "kv_del_b" });
    try testing.expect(std.mem.indexOf(u8, val_b, "beta") != null);
}

test "e2e/kv: overwrite in one namespace does not affect another" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "kv_ow_a" });
    try ctx.exec(&.{ "ns", "create", "kv_ow_b" });

    // Set same key in both namespaces
    try ctx.exec(&.{ "kv", "set", "setting", "original_a", "-n", "kv_ow_a" });
    try ctx.exec(&.{ "kv", "set", "setting", "original_b", "-n", "kv_ow_b" });

    // Overwrite only in namespace A
    try ctx.exec(&.{ "kv", "set", "setting", "updated_a", "-n", "kv_ow_a" });

    // Namespace A should have the new value
    const val_a = try ctx.execCapture(&.{ "kv", "get", "setting", "-n", "kv_ow_a" });
    try testing.expect(std.mem.indexOf(u8, val_a, "updated_a") != null);

    // Namespace B should still have its original value
    const val_b = try ctx.execCapture(&.{ "kv", "get", "setting", "-n", "kv_ow_b" });
    try testing.expect(std.mem.indexOf(u8, val_b, "original_b") != null);
}

test "e2e/kv: list only shows keys in the requested namespace" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "kv_ls_a" });
    try ctx.exec(&.{ "ns", "create", "kv_ls_b" });

    // Populate different keys in each namespace
    try ctx.exec(&.{ "kv", "set", "onlyInA", "a", "-n", "kv_ls_a" });
    try ctx.exec(&.{ "kv", "set", "onlyInB", "b", "-n", "kv_ls_b" });

    // List namespace A — should see onlyInA but not onlyInB
    var result_a = try ctx.cli.run(&.{ "kv", "ls", "-n", "kv_ls_a" });
    defer result_a.deinit();
    try stdx.testing.assertContains(result_a, "onlyInA");
    try stdx.testing.assertNotContains(result_a, "onlyInB");

    // List namespace B — should see onlyInB but not onlyInA
    var result_b = try ctx.cli.run(&.{ "kv", "ls", "-n", "kv_ls_b" });
    defer result_b.deinit();
    try stdx.testing.assertContains(result_b, "onlyInB");
    try stdx.testing.assertNotContains(result_b, "onlyInA");
}

test "e2e/kv: conditional --nx is per-namespace" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "kv_nx_a" });
    try ctx.exec(&.{ "ns", "create", "kv_nx_b" });

    // Set key with --nx in namespace A
    try ctx.exec(&.{ "kv", "set", "unique", "first", "--nx", "-n", "kv_nx_a" });

    // Same key with --nx in namespace B should succeed (key doesn't exist there)
    try ctx.exec(&.{ "kv", "set", "unique", "second", "--nx", "-n", "kv_nx_b" });

    // Verify both values
    const val_a = try ctx.execCapture(&.{ "kv", "get", "unique", "-n", "kv_nx_a" });
    try testing.expect(std.mem.indexOf(u8, val_a, "first") != null);

    const val_b = try ctx.execCapture(&.{ "kv", "get", "unique", "-n", "kv_nx_b" });
    try testing.expect(std.mem.indexOf(u8, val_b, "second") != null);
}

test "e2e/kv: default namespace is isolated from named namespaces" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "kv_custom" });

    // Set key in default namespace (no -n flag)
    try ctx.exec(&.{ "kv", "set", "mykey", "default_val" });

    // Set same key in custom namespace
    try ctx.exec(&.{ "kv", "set", "mykey", "custom_val", "-n", "kv_custom" });

    // Default namespace read
    const val_default = try ctx.execCapture(&.{ "kv", "get", "mykey" });
    try testing.expect(std.mem.indexOf(u8, val_default, "default_val") != null);

    // Custom namespace read
    const val_custom = try ctx.execCapture(&.{ "kv", "get", "mykey", "-n", "kv_custom" });
    try testing.expect(std.mem.indexOf(u8, val_custom, "custom_val") != null);
}
