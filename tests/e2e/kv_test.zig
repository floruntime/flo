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

/// A read of a key that isn't there: exit 1, named not found.
fn expectNotFound(r: anytype) !void {
    try testing.expectEqual(@as(u8, 1), r.exit_code);
    try testing.expect(std.mem.endsWith(u8, std.mem.trimEnd(u8, r.stderr, "\n"), "[not_found/not_found]"));
}

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
    var result = try ctx.cli.run(&.{ "kv", "get", "nonexistent", "--output", "table" });
    defer result.deinit();
    try expectNotFound(result);
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

/// The names a scan printed with `-o raw`, sorted (a walk returns them
/// shard by shard). Each name is a line.
fn scannedNames(out: []const u8, buf: [][]const u8) [][]const u8 {
    var n: usize = 0;
    var lines = std.mem.tokenizeScalar(u8, out, '\n');
    while (lines.next()) |line| : (n += 1) buf[n] = line;
    std.mem.sort([]const u8, buf[0..n], {}, struct {
        fn less(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.less);
    return buf[0..n];
}

fn expectNames(want: []const []const u8, got: []const []const u8) !void {
    try testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| try testing.expectEqualStrings(w, g);
}

test "e2e/kv: scan lists all keys" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "ls_alpha", "a" });
    try ctx.exec(&.{ "kv", "set", "ls_beta", "b" });
    try ctx.exec(&.{ "kv", "set", "ls_gamma", "c" });

    var result = try ctx.cli.run(&.{ "kv", "scan", "-o", "raw" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 0), result.exit_code);
    var buf: [8][]const u8 = undefined;
    try expectNames(&.{ "ls_alpha", "ls_beta", "ls_gamma" }, scannedNames(result.stdout, &buf));
    // One page: no cursor.
    try testing.expectEqualStrings("", result.stderr);
}

test "e2e/kv: scan with no keys prints nothing" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "kv", "scan" });
    defer result.deinit();

    try testing.expectEqual(@as(u8, 0), result.exit_code);
    try testing.expectEqualStrings("", result.stdout);
}

test "e2e/kv: scan with a prefix lists only the keys starting with it" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "user:1", "alice" });
    try ctx.exec(&.{ "kv", "set", "user:2", "bob" });
    try ctx.exec(&.{ "kv", "set", "user:3", "charlie" });
    try ctx.exec(&.{ "kv", "set", "other:1", "other" });

    var result = try ctx.cli.run(&.{ "kv", "scan", "user:", "-o", "raw" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 0), result.exit_code);
    var buf: [8][]const u8 = undefined;
    try expectNames(&.{ "user:1", "user:2", "user:3" }, scannedNames(result.stdout, &buf));
}

test "e2e/kv: scan with --limit prints a page, and in raw the cursor goes to stderr" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var key_buf: [32]u8 = undefined;
    for (0..10) |i| try ctx.exec(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "limit_key_{d}", .{i}), "v" });

    var result = try ctx.cli.run(&.{ "kv", "scan", "--limit", "3", "-o", "raw" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 0), result.exit_code);
    // stdout is names alone, for `while read`; the cursor is on stderr.
    try testing.expectEqual(@as(usize, 3), std.mem.count(u8, result.stdout, "\n"));
    try testing.expect(std.mem.indexOf(u8, result.stdout, "cursor") == null);
    try testing.expect(std.mem.startsWith(u8, result.stderr, "cursor: "));
}

test "e2e/kv: scan follows its cursor across pages to every key, once each" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var key_buf: [32]u8 = undefined;
    for (0..10) |i| try ctx.exec(&.{ "kv", "set", try std.fmt.bufPrint(&key_buf, "page_key_{d}", .{i}), "v" });

    var seen: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (seen.items) |name| testing.allocator.free(name);
        seen.deinit(testing.allocator);
    }
    var cursor: [128]u8 = undefined;
    var cursor_len: usize = 0;
    var pages: usize = 0;
    while (true) : (pages += 1) {
        if (pages > 20) return error.TooManyPages;
        var args: std.ArrayListUnmanaged([]const u8) = .empty;
        defer args.deinit(testing.allocator);
        try args.appendSlice(testing.allocator, &.{ "kv", "scan", "page_key_", "--limit", "3", "-o", "raw" });
        if (cursor_len > 0) try args.appendSlice(testing.allocator, &.{ "--cursor", cursor[0..cursor_len] });
        var page = try ctx.cli.run(args.items);
        defer page.deinit();
        try testing.expectEqual(@as(u8, 0), page.exit_code);
        var lines = std.mem.tokenizeScalar(u8, page.stdout, '\n');
        while (lines.next()) |name| try seen.append(testing.allocator, try testing.allocator.dupe(u8, name));
        const next = std.mem.trim(u8, page.stderr, "\n");
        if (next.len == 0) break;
        try testing.expect(std.mem.startsWith(u8, next, "cursor: "));
        const hex = next["cursor: ".len..];
        @memcpy(cursor[0..hex.len], hex);
        cursor_len = hex.len;
    }
    try testing.expect(pages >= 3);
    std.mem.sort([]const u8, seen.items, {}, struct {
        fn less(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.less);
    var want: [10][]const u8 = undefined;
    var want_buf: [10][16]u8 = undefined;
    for (0..10) |i| want[i] = try std.fmt.bufPrint(&want_buf[i], "page_key_{d}", .{i});
    try expectNames(&want, seen.items);
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
    var after = try ctx.cli.run(&.{ "kv", "get", "to_delete", "--output", "table" });
    defer after.deinit();
    try expectNotFound(after);
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
    var result = try ctx.cli.run(&.{ "kv", "get", "json_key", "--output", "json" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "json value");
    try stdx.testing.assertContains(result, "version");
}

// =============================================================================
// Conditional Operations: --condition if_absent
// =============================================================================

test "e2e/kv: set --condition if_absent on a new key succeeds" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "nx_test_key", "first_value", "--condition", "if_absent" });

    // Verify value was set
    const value = try ctx.execCapture(&.{ "kv", "get", "nx_test_key" });
    try testing.expect(std.mem.indexOf(u8, value, "first_value") != null);
}

test "e2e/kv: set --condition if_absent on an existing key fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // First set
    try ctx.exec(&.{ "kv", "set", "nx_existing", "first_value" });

    var result = try ctx.cli.run(&.{ "kv", "set", "nx_existing", "second_value", "--condition", "if_absent" });
    defer result.deinit();

    try testing.expectEqual(@as(u8, 3), result.exit_code);
    try testing.expectEqualStrings("Error: conflict: the condition didn't hold [conflict/condition_failed]", std.mem.trimEnd(u8, result.stderr, "\n"));
}

test "e2e/kv: set --condition if_absent preserves the original value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "nx_preserve", "original" });

    var r2 = try ctx.cli.run(&.{ "kv", "set", "nx_preserve", "new", "--condition", "if_absent" });
    defer r2.deinit();

    // Value should still be original
    const value = try ctx.execCapture(&.{ "kv", "get", "nx_preserve" });
    try testing.expect(std.mem.indexOf(u8, value, "original") != null);
}

// =============================================================================
// Conditional Operations: --condition if_present
// =============================================================================

test "e2e/kv: set --condition if_present on an existing key succeeds" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "xx_test_key", "first_value" });

    try ctx.exec(&.{ "kv", "set", "xx_test_key", "updated_value", "--condition", "if_present" });

    // Verify update
    const value = try ctx.execCapture(&.{ "kv", "get", "xx_test_key" });
    try testing.expect(std.mem.indexOf(u8, value, "updated_value") != null);
}

test "e2e/kv: set --condition if_present on a missing key fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "kv", "set", "xx_nonexistent", "some_value", "--condition", "if_present" });
    defer result.deinit();

    try testing.expectEqual(@as(u8, 3), result.exit_code);
    try testing.expectEqualStrings("Error: conflict: the condition didn't hold [conflict/condition_failed]", std.mem.trimEnd(u8, result.stderr, "\n"));
}

test "e2e/kv: set with a --condition it doesn't know is a usage error" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "kv", "set", "some_key", "value", "--condition", "maybe" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 2), result.exit_code);
    try testing.expectEqualStrings("Error: --condition maybe is not one of: always, if_absent, if_present", std.mem.trimEnd(u8, result.stderr, "\n"));
}

// =============================================================================
// Conditional Operations: --cas-version (Compare-and-Swap)
// =============================================================================

test "e2e/kv: set --cas-version with the right version succeeds" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "cas_test_key", "initial" });

    // flo kv get cas_test_key --format json (to get version)
    const json_output = try ctx.execCapture(&.{ "kv", "get", "cas_test_key", "--output", "json" });

    const version = parseVersion(json_output) orelse return error.NoVersion;
    var version_buf: [32]u8 = undefined;
    const version_str = try std.fmt.bufPrint(&version_buf, "{d}", .{version});

    try ctx.exec(&.{ "kv", "set", "cas_test_key", "updated_via_cas", "--cas-version", version_str });

    // Verify update
    const value = try ctx.execCapture(&.{ "kv", "get", "cas_test_key" });
    try testing.expect(std.mem.indexOf(u8, value, "updated_via_cas") != null);
}

test "e2e/kv: set --cas-version with the wrong version fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "cas_wrong", "initial" });

    var result = try ctx.cli.run(&.{ "kv", "set", "cas_wrong", "should_not_work", "--cas-version", "999999" });
    defer result.deinit();

    try testing.expectEqual(@as(u8, 3), result.exit_code);
    try testing.expectEqualStrings("Error: conflict: cas_version isn't the key's version [conflict/cas_mismatch]", std.mem.trimEnd(u8, result.stderr, "\n"));
}

test "e2e/kv: failed CAS preserves original value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "cas_preserve", "original_value" });

    var r2 = try ctx.cli.run(&.{ "kv", "set", "cas_preserve", "new_value", "--cas-version", "999999" });
    defer r2.deinit();

    // Value should still be original
    const value = try ctx.execCapture(&.{ "kv", "get", "cas_preserve" });
    try testing.expect(std.mem.indexOf(u8, value, "original_value") != null);
}

test "e2e/kv: set --cas-version 0 writes only a key that doesn't exist" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "cas_zero", "first", "--cas-version", "0" });
    var again = try ctx.cli.run(&.{ "kv", "set", "cas_zero", "second", "--cas-version", "0" });
    defer again.deinit();
    try testing.expectEqual(@as(u8, 3), again.exit_code);
    const value = try ctx.execCapture(&.{ "kv", "get", "cas_zero", "-o", "raw" });
    try testing.expectEqualStrings("first", std.mem.trimEnd(u8, value, "\n"));
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

    try ctx.exec(&.{ "kv", "set", "ttl_key", "ttl_value", "--ttl-ms", "1s" });

    // Immediately readable
    const before = try ctx.execCapture(&.{ "kv", "get", "ttl_key" });
    try testing.expect(std.mem.indexOf(u8, before, "ttl_value") != null);

    // Wait for expiration (1.5 seconds to be safe)
    @import("stdx").time.sleep(1500 * std.time.ns_per_ms);

    // Should be expired now
    var after = try ctx.cli.run(&.{ "kv", "get", "ttl_key", "--output", "table" });
    defer after.deinit();
    try expectNotFound(after);
}

test "e2e/kv: a TTL in milliseconds expires the key; --ttl-ms refuses a unit it doesn't know, and expire --ttl-ms 0 clears" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Read as seconds, either TTL would outlive the test.
    try ctx.exec(&.{ "kv", "set", "ms_key", "ms_value", "--ttl-ms", "800ms" });
    try ctx.exec(&.{ "kv", "set", "touched_key", "touched_value" });
    try ctx.exec(&.{ "kv", "expire", "touched_key", "--ttl-ms", "800ms" });
    var early = try ctx.cli.run(&.{ "kv", "get", "ms_key", "--output", "table" });
    defer early.deinit();
    try testing.expect(early.stdoutContains("ms_value"));
    var early_touched = try ctx.cli.run(&.{ "kv", "get", "touched_key", "--output", "table" });
    defer early_touched.deinit();
    try testing.expect(early_touched.stdoutContains("touched_value"));
    @import("stdx").time.sleep(2000 * std.time.ns_per_ms);
    var late = try ctx.cli.run(&.{ "kv", "get", "ms_key", "--output", "table" });
    defer late.deinit();
    try expectNotFound(late);
    var late_touched = try ctx.cli.run(&.{ "kv", "get", "touched_key", "--output", "table" });
    defer late_touched.deinit();
    try expectNotFound(late_touched);

    // A unit the flag doesn't know is refused, and nothing set.
    var bare = try ctx.cli.run(&.{ "kv", "set", "bare_key", "v", "--ttl-ms", "1x" });
    defer bare.deinit();
    try testing.expectEqual(@as(u8, 2), bare.exit_code);
    try testing.expect(bare.contains("not a duration"));
    var unset = try ctx.cli.run(&.{ "kv", "get", "bare_key", "--output", "table" });
    defer unset.deinit();
    try expectNotFound(unset);

    // 0 is the one bare number: it clears.
    try ctx.exec(&.{ "kv", "set", "cleared_key", "kept", "--ttl-ms", "1s" });
    try ctx.exec(&.{ "kv", "expire", "cleared_key", "--ttl-ms", "0" });
    @import("stdx").time.sleep(1500 * std.time.ns_per_ms);
    var kept = try ctx.cli.run(&.{ "kv", "get", "cleared_key", "--output", "table" });
    defer kept.deinit();
    try testing.expect(kept.stdoutContains("kept"));
}

test "e2e/kv: get answers when a key's TTL runs out, in ms since the epoch, and none without one" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const Get = struct { version: u64, value: []const u8, expires_at_ms: ?i64 = null };
    const before = stdx.time.milliTimestamp();
    try ctx.exec(&.{ "kv", "set", "ttl_at", "v", "--ttl-ms", "1h" });
    const after = stdx.time.milliTimestamp();
    const timed = try ctx.execCapture(&.{ "kv", "get", "ttl_at", "-o", "json" });
    const t = try std.json.parseFromSlice(Get, testing.allocator, timed, .{});
    defer t.deinit();
    const at = t.value.expires_at_ms orelse return error.NoExpiry;
    try testing.expect(at >= before + 3_600_000 and at <= after + 3_600_000);

    try ctx.exec(&.{ "kv", "set", "no_ttl_at", "v" });
    const plain = try ctx.execCapture(&.{ "kv", "get", "no_ttl_at", "-o", "json" });
    const p = try std.json.parseFromSlice(Get, testing.allocator, plain, .{});
    defer p.deinit();
    try testing.expectEqual(@as(?i64, null), p.value.expires_at_ms);
}

test "e2e/kv: set with TTL 0 means no expiration" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "no_ttl_key", "permanent_value", "--ttl-ms", "0" });

    // Small wait to verify it doesn't expire immediately
    @import("stdx").time.sleep(100 * std.time.ns_per_ms);

    // Should still be readable
    const value = try ctx.execCapture(&.{ "kv", "get", "no_ttl_key" });
    try testing.expect(std.mem.indexOf(u8, value, "permanent_value") != null);
}

test "e2e/kv: overwrite resets TTL" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Set with short TTL
    try ctx.exec(&.{ "kv", "set", "ttl_reset_key", "initial", "--ttl-ms", "1s" });

    // Overwrite with no TTL (should persist)
    try ctx.exec(&.{ "kv", "set", "ttl_reset_key", "updated" });

    // Wait past original TTL
    @import("stdx").time.sleep(1500 * std.time.ns_per_ms);

    // Should still exist (TTL was reset)
    const value = try ctx.execCapture(&.{ "kv", "get", "ttl_reset_key" });
    try testing.expect(std.mem.indexOf(u8, value, "updated") != null);
}

test "e2e/kv: TTL with --condition if_absent" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "ttl_nx_key", "first", "--ttl-ms", "2s", "--condition", "if_absent" });

    // Verify value exists
    const v1 = try ctx.execCapture(&.{ "kv", "get", "ttl_nx_key" });
    try testing.expect(std.mem.indexOf(u8, v1, "first") != null);

    // Again with if_absent: refused while the key lives
    var result = try ctx.cli.run(&.{ "kv", "set", "ttl_nx_key", "second", "--ttl-ms", "2s", "--condition", "if_absent" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 3), result.exit_code);

    // Wait for expiration
    @import("stdx").time.sleep(2500 * std.time.ns_per_ms);

    // Expired, so absent: if_absent writes
    try ctx.exec(&.{ "kv", "set", "ttl_nx_key", "third", "--ttl-ms", "2s", "--condition", "if_absent" });

    const v3 = try ctx.execCapture(&.{ "kv", "get", "ttl_nx_key" });
    try testing.expect(std.mem.indexOf(u8, v3, "third") != null);
}

// =============================================================================
// Delete Operations
// =============================================================================

test "e2e/kv: delete non-existent key" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Deleting a key that doesn't exist succeeds, and says it didn't.
    var result = try ctx.cli.run(&.{ "kv", "delete", "never_existed" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 0), result.exit_code);
    try testing.expectEqualStrings("existed: false\n", result.stdout);
    try ctx.exec(&.{ "kv", "set", "existed", "v" });
    try testing.expectEqualStrings("existed: true", try ctx.execCapture(&.{ "kv", "delete", "existed" }));
}

test "e2e/kv: delete then get returns nil" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "delete_me", "value" });
    try ctx.exec(&.{ "kv", "delete", "delete_me" });

    // flo kv get delete_me --format table
    var result = try ctx.cli.run(&.{ "kv", "get", "delete_me", "--output", "table" });
    defer result.deinit();
    try expectNotFound(result);
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

test "e2e/kv: data persists across restart (async_flush, graceful)" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .async_flush, .shards = 1 },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "async-kv", "async-kv-value" });
    try ctx.restartServer();

    const out = try ctx.execCapture(&.{ "kv", "get", "async-kv" });
    try testing.expect(std.mem.indexOf(u8, out, "async-kv-value") != null);
}

test "e2e/kv: async_flush segment flush before restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .async_flush, .shards = 1 },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "flush-kv", "flush-val" });

    // Wait for background segment_flush task (interval ~1s).
    stdx.time.sleep(1_500 * std.time.ns_per_ms);

    try ctx.restartServer();

    const out = try ctx.execCapture(&.{ "kv", "get", "flush-kv" });
    try testing.expect(std.mem.indexOf(u8, out, "flush-val") != null);
}

test "e2e/kv: persists on same shard after stream append (regression)" {
    // Regression: KV must survive replay when a stream_append shares the shard Raft log.
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync, .shards = 1 },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "mixed-kv", "written-before-stream" });
    try ctx.exec(&.{ "stream", "append", "mixed-stream", "written-after-kv" });

    try ctx.restartServer();

    const out = try ctx.execCapture(&.{ "kv", "get", "mixed-kv" });
    try testing.expect(std.mem.indexOf(u8, out, "written-before-stream") != null);
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
// These tests exercise:
// - Cluster formation via --cluster and --join
// - Leader election and re-election
// - Data replication across nodes
// - Read-after-write consistency from any node
// - Node failure tolerance (2/3 quorum)
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
    @import("stdx").time.sleep(500 * std.time.ns_per_ms);

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
    @import("stdx").time.sleep(500 * std.time.ns_per_ms);

    // Verify replication before killing
    const before_value = try cluster.execCaptureOn(1, &.{ "kv", "get", "survive_key" });
    defer testing.allocator.free(before_value);

    // Kill node 0
    cluster.stopNode(0);

    // Small delay for signal propagation
    @import("stdx").time.sleep(500 * std.time.ns_per_ms);

    // Give cluster time to detect failure and re-elect leader
    @import("stdx").time.sleep(2 * std.time.ns_per_s);

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
    @import("stdx").time.sleep(500 * std.time.ns_per_ms);

    // Kill node 1 (seed/original leader)
    const kill_time = stdx.time.milliTimestamp();
    std.debug.print("Killing node 0... (time={d})\n", .{kill_time});
    cluster.stopNode(0);
    const kill_done_time = stdx.time.milliTimestamp();
    std.debug.print("Kill complete after {d}ms\n", .{kill_done_time - kill_time});

    // The survivors elect a leader within the failover timeout; a write on
    // node 1 is forwarded to it, or answered "electing a leader" until then.
    std.debug.print("Writing to node 1 until the survivors have a leader...\n", .{});
    var write_success = false;
    var last_err: ?anyerror = null;
    var attempt: usize = 0;
    while (attempt < 40) : (attempt += 1) {
        if (attempt > 0) @import("stdx").time.sleep(500 * std.time.ns_per_ms);
        cluster.execOn(1, &.{ "kv", "set", "after_failure", "new_value" }) catch |err| {
            last_err = err;
            continue;
        };
        write_success = true;
        std.debug.print("Write succeeded {d}ms after the kill\n", .{stdx.time.milliTimestamp() - kill_time});
        break;
    }

    if (!write_success) {
        std.debug.print("\n=== Write to node 1 failed after retries, dumping logs ===\n", .{});
        for (0..3) |i| {
            if (cluster.servers[i]) |s| {
                std.debug.print("\n--- Node {d} log ({s}) ---\n", .{ i, s.log_file_path });
                const log_file = stdx.fs.openFileAbsolute(s.log_file_path, .{}) catch |e| {
                    std.debug.print("Could not open: {}\n", .{e});
                    continue;
                };
                defer stdx.fs.closeFile(log_file);
                // Use a larger buffer to capture more logs (256KB)
                var buf: [262144]u8 = undefined;
                const n = stdx.fs.readAll(log_file, &buf) catch 0;
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
    const json_output = try cluster.execCaptureOnWithRetry(1, &.{ "kv", "get", "cas_cluster_key", "--output", "json" }, 5, 500);
    defer testing.allocator.free(json_output);

    const version = parseVersion(json_output) orelse return error.NoVersion;
    var version_buf: [32]u8 = undefined;
    const version_str = try std.fmt.bufPrint(&version_buf, "{d}", .{version});

    // CAS update from node 3 using version from node 2
    try cluster.execOn(2, &.{ "kv", "set", "cas_cluster_key", "updated_via_cas", "--cas-version", version_str });

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
    const after = try cluster.pollAnyUntilContains(0, &.{ "kv", "exists", "to_delete_cluster" }, "exists: false", 10, 500);
    defer testing.allocator.free(after);
}

test "e2e/kv/cluster: a TTL runs out on every node, the leader included" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    const set_at = stdx.time.milliTimestamp();
    try cluster.execOn(0, &.{ "kv", "set", "ttl_everywhere", "short_lived", "--ttl-ms", "3s" });
    for (0..3) |node| {
        const seen = try cluster.pollUntilContains(node, &.{ "kv", "get", "ttl_everywhere" }, "short_lived", 20, 100);
        testing.allocator.free(seen);
    }
    // Past the TTL, with room for the stamp to trail the set by a second.
    const wait_ms = (set_at + 4_500) - stdx.time.milliTimestamp();
    if (wait_ms > 0) stdx.time.sleep(@as(u64, @intCast(wait_ms)) * std.time.ns_per_ms);
    for (0..3) |node| {
        const out = try cluster.execCaptureAnyOn(node, &.{ "kv", "exists", "ttl_everywhere" });
        defer testing.allocator.free(out);
        if (std.mem.indexOf(u8, out, "exists: false") == null) {
            std.debug.print("node {d} still has the key: {s}\n", .{ node, out });
            return error.TestUnexpectedResult;
        }
    }
}

test "e2e/kv/cluster: a replicated leader stamps entries at the wall clock" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Stamps taken seconds apart move with the clock: a leader that could
    // not hear its voters' clocks would stamp each entry 1 ns after the
    // last, frozen at the time the cluster formed.
    const first = try probeStamp(&cluster);
    stdx.time.sleep(3 * std.time.ns_per_s);
    const second = try probeStamp(&cluster);
    if (second - first < 2_500) {
        std.debug.print("stamps {d} and {d} ms are 3 s apart in wall time\n", .{ first, second });
        return error.TestUnexpectedResult;
    }
}

/// Write a point without a timestamp, which takes its entry's stamp, and
/// return that stamp from the answer ("OK (<ms>-<sequence>)"), checked to
/// be within a second or so of the wall clock around the write.
fn probeStamp(cluster: *ClusterContext) !i64 {
    const before = stdx.time.milliTimestamp();
    const out = try cluster.execCaptureOn(0, &.{ "ts", "write", "stamp_probe", "--value", "1" });
    defer testing.allocator.free(out);
    const after = stdx.time.milliTimestamp();
    const open = std.mem.indexOf(u8, out, "OK (") orelse return error.TestUnexpectedResult;
    const dash = std.mem.indexOfScalarPos(u8, out, open, '-') orelse return error.TestUnexpectedResult;
    const stamp_ms = try std.fmt.parseInt(i64, out[open + 4 .. dash], 10);
    if (stamp_ms < before - 1_500 or stamp_ms > after + 1_500) {
        std.debug.print("stamp {d} ms is not near the wall clock ({d}..{d} ms)\n", .{ stamp_ms, before, after });
        return error.TestUnexpectedResult;
    }
    return stamp_ms;
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
    var async_get = try ctx.cli.runAsync(&.{ "kv", "get", key, "--wait-ms", "5000" });
    defer async_get.deinit();

    // Small delay to ensure blocking GET is registered
    @import("stdx").time.sleep(100 * std.time.ns_per_ms);

    // Set the key from another "client"
    try ctx.exec(&.{ "kv", "set", key, value });

    // Wait for blocking GET to complete
    var result = try async_get.wait();
    defer result.deinit();

    // Verify blocking GET received the value
    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, value);
}

test "e2e/kv: the longest wait receives the value, a wait of 0 answers at once, and a longer wait is refused" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const key = "blocking_longest_key";
    const value = "longest_wait_value";

    // For both kinds of wait: 0 does not wait (a missing key answers null
    // at once), and one millisecond over the longest wait is refused, not
    // shortened.
    for ([_][]const u8{ "exists", "change" }) |wait_for| {
        const t0 = @import("stdx").time.monotonicMs();
        var now = try ctx.cli.run(&.{ "kv", "get", "never_set_key", "--wait-ms", "0", "--wait-for", wait_for, "-o", "json" });
        defer now.deinit();
        try expectNotFound(now);
        try testing.expect(@import("stdx").time.monotonicMs() - t0 < 2_000);

        var over = try ctx.cli.run(&.{ "kv", "get", "never_set_key", "--wait-ms", "300001", "--wait-for", wait_for });
        defer over.deinit();
        try testing.expectEqual(@as(u8, 2), over.exit_code);
        try testing.expectEqualStrings("Error: --wait-ms 300001 is out of range (0 to 300000)", std.mem.trimEnd(u8, over.stderr, "\n"));
    }

    // Start a blocking GET with the longest wait
    var async_get = try ctx.cli.runAsync(&.{ "kv", "get", key, "--wait-ms", "300000" });
    defer async_get.deinit();

    // Small delay to ensure blocking GET is registered
    @import("stdx").time.sleep(100 * std.time.ns_per_ms);

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
    // --wait-for change waits for the next version even when the key exists;
    // the default, exists, would answer at once.
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const key = "blocking_existing_key";
    const initial_value = "initial";
    const updated_value = "updated";

    // Set the key first
    try ctx.exec(&.{ "kv", "set", key, initial_value });

    // Wait for the next version
    var async_get = try ctx.cli.runAsync(&.{ "kv", "get", key, "--wait-ms", "5000", "--wait-for", "change" });
    defer async_get.deinit();

    // Small delay to ensure watcher is registered
    @import("stdx").time.sleep(100 * std.time.ns_per_ms);

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
    var result = try ctx.cli.run(&.{ "kv", "get", key, "--wait-ms", "500" });
    defer result.deinit();

    // The wait ends with the key still absent: not found.
    try expectNotFound(result);
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

    // kv scan should return ALL keys regardless of shard placement
    var result = try ctx.cli.run(&.{ "kv", "scan" });
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
    var result = try ctx.cli.run(&.{ "kv", "scan", "user:" });
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
    var result_a = try ctx.cli.run(&.{ "kv", "get", "shared", "-n", "kv_del_a", "--output", "table" });
    defer result_a.deinit();
    try expectNotFound(result_a);

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
    var result_a = try ctx.cli.run(&.{ "kv", "scan", "-n", "kv_ls_a" });
    defer result_a.deinit();
    try stdx.testing.assertContains(result_a, "onlyInA");
    try stdx.testing.assertNotContains(result_a, "onlyInB");

    // List namespace B — should see onlyInB but not onlyInA
    var result_b = try ctx.cli.run(&.{ "kv", "scan", "-n", "kv_ls_b" });
    defer result_b.deinit();
    try stdx.testing.assertContains(result_b, "onlyInB");
    try stdx.testing.assertNotContains(result_b, "onlyInA");
}

test "e2e/kv: --condition if_absent is per-namespace" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "kv_nx_a" });
    try ctx.exec(&.{ "ns", "create", "kv_nx_b" });

    // Set the key if absent in namespace A
    try ctx.exec(&.{ "kv", "set", "unique", "first", "--condition", "if_absent", "-n", "kv_nx_a" });

    // The same key if absent in namespace B succeeds: it doesn't exist there
    try ctx.exec(&.{ "kv", "set", "unique", "second", "--condition", "if_absent", "-n", "kv_nx_b" });

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

// =============================================================================
// Version History (MVCC)
// =============================================================================

test "e2e/kv: history shows multiple versions after overwrites" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Write 3 versions of the same key
    try ctx.exec(&.{ "kv", "set", "hist_key", "version_one" });
    try ctx.exec(&.{ "kv", "set", "hist_key", "version_two" });
    try ctx.exec(&.{ "kv", "set", "hist_key", "version_three" });

    // Newest first: version, when (ms), deleted, value.
    const output = try ctx.execCapture(&.{ "kv", "history", "hist_key", "-o", "raw" });
    var lines = std.mem.tokenizeScalar(u8, output, '\n');
    for ([_][]const u8{ "3\t", "2\t", "1\t" }, [_][]const u8{ "version_three", "version_two", "version_one" }) |version, value| {
        const line = lines.next() orelse return error.TooFewVersions;
        try testing.expect(std.mem.startsWith(u8, line, version));
        try testing.expect(std.mem.endsWith(u8, line, value));
    }
    try testing.expect(lines.next() == null);
    var one = try ctx.cli.run(&.{ "kv", "history", "hist_key", "--limit", "1", "-o", "raw" });
    defer one.deinit();
    try testing.expectEqual(@as(u8, 0), one.exit_code);
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, one.stdout, "\n"));
    try testing.expect(std.mem.startsWith(u8, one.stdout, "3\t"));
}

test "e2e/kv: history finds a key on whichever of 4 shards owns it" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();

    // Sixteen keys spread across the shards; each is read from a client
    // connection on whichever shard accepted it.
    var key_buf: [16]u8 = undefined;
    for (0..16) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "spread-{d}", .{i});
        try ctx.exec(&.{ "kv", "set", key, "one" });
        try ctx.exec(&.{ "kv", "set", key, "two" });
    }
    for (0..16) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "spread-{d}", .{i});
        var result = try ctx.cli.run(&.{ "kv", "history", key, "-o", "raw" });
        defer result.deinit();
        try testing.expectEqual(@as(u8, 0), result.exit_code);
        try testing.expectEqual(@as(usize, 2), std.mem.count(u8, result.stdout, "\n"));
        try testing.expect(std.mem.startsWith(u8, result.stdout, "2\t"));
    }
    // A key written with a routing key lives on that key's shard; history
    // given the same routing key finds it there.
    for (0..16) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "routed-{d}", .{i});
        try ctx.exec(&.{ "kv", "set", key, "one", "-r", "tenant-7" });
        var result = try ctx.cli.run(&.{ "kv", "history", key, "-r", "tenant-7", "-o", "raw" });
        defer result.deinit();
        try testing.expectEqual(@as(u8, 0), result.exit_code);
        try testing.expectEqual(@as(usize, 1), std.mem.count(u8, result.stdout, "\n"));
        try testing.expect(std.mem.endsWith(u8, result.stdout, "\tone\n"));
    }
}

test "e2e/kv: history for non-existent key fails" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "kv", "history", "no_such_key" });
    defer result.deinit();

    try testing.expectEqual(@as(u8, 1), result.exit_code);
}

test "e2e/kv: history after delete preserves prior versions" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "del_hist", "alive_value" });
    try ctx.exec(&.{ "kv", "delete", "del_hist" });

    // The delete is a version too, and the value before it is still there.
    var result = try ctx.cli.run(&.{ "kv", "history", "del_hist", "-o", "raw" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 0), result.exit_code);
    // Newest first: version, when (ms), deleted, value.
    var lines = std.mem.tokenizeScalar(u8, result.stdout, '\n');
    const deleted = lines.next() orelse return error.TooFewVersions;
    try testing.expect(std.mem.startsWith(u8, deleted, "2\t"));
    try testing.expect(std.mem.endsWith(u8, deleted, "\ttrue\t"));
    const alive = lines.next() orelse return error.TooFewVersions;
    try testing.expect(std.mem.startsWith(u8, alive, "1\t"));
    try testing.expect(std.mem.endsWith(u8, alive, "\tfalse\talive_value"));
    try testing.expect(lines.next() == null);
}

// =============================================================================
// Multi-GET (mget) — Single Node
// =============================================================================

test "e2e/kv: mget basic — all keys exist" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "mg_a", "alpha" });
    try ctx.exec(&.{ "kv", "set", "mg_b", "beta" });
    try ctx.exec(&.{ "kv", "set", "mg_c", "gamma" });

    // flo kv mget mg_a mg_b mg_c --format json
    var result = try ctx.cli.run(&.{ "kv", "mget", "mg_a", "mg_b", "mg_c", "--output", "json" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "mg_a");
    try stdx.testing.assertContains(result, "alpha");
    try stdx.testing.assertContains(result, "mg_b");
    try stdx.testing.assertContains(result, "beta");
    try stdx.testing.assertContains(result, "mg_c");
    try stdx.testing.assertContains(result, "gamma");
}

test "e2e/kv: mget partial — some keys missing" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "mg_exists1", "val1" });
    try ctx.exec(&.{ "kv", "set", "mg_exists2", "val2" });

    // mget with mix of existing and non-existing keys
    var result = try ctx.cli.run(&.{ "kv", "mget", "mg_exists1", "mg_missing", "mg_exists2", "--output", "json" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // Existing keys should have values
    try stdx.testing.assertContains(result, "val1");
    try stdx.testing.assertContains(result, "val2");
    // Missing key should be present with null value
    try stdx.testing.assertContains(result, "mg_missing");
    try stdx.testing.assertContains(result, "null");
}

test "e2e/kv: mget all keys missing" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "kv", "mget", "nope_a", "nope_b", "nope_c", "--output", "json" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // All keys null
    try stdx.testing.assertContains(result, "nope_a");
    try stdx.testing.assertContains(result, "nope_b");
    try stdx.testing.assertContains(result, "nope_c");
    try stdx.testing.assertContains(result, "null");
}

test "e2e/kv: mget single key" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "mg_single", "only" });

    var result = try ctx.cli.run(&.{ "kv", "mget", "mg_single", "--output", "json" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "mg_single");
    try stdx.testing.assertContains(result, "only");
}

test "e2e/kv: mget table format" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "tbl_a", "apple" });
    try ctx.exec(&.{ "kv", "set", "tbl_b", "banana" });

    var result = try ctx.cli.run(&.{ "kv", "mget", "tbl_a", "tbl_b", "--output", "table" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "KEY");
    try stdx.testing.assertContains(result, "VALUE");
    try stdx.testing.assertContains(result, "tbl_a");
    try stdx.testing.assertContains(result, "apple");
    try stdx.testing.assertContains(result, "tbl_b");
    try stdx.testing.assertContains(result, "banana");
}

test "e2e/kv: mget reflects latest value after overwrite" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "mg_ow", "original" });
    try ctx.exec(&.{ "kv", "set", "mg_ow", "updated" });

    var result = try ctx.cli.run(&.{ "kv", "mget", "mg_ow", "--output", "json" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "updated");
    try stdx.testing.assertNotContains(result, "original");
}

test "e2e/kv: mget after delete shows nil" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "mg_del_a", "va" });
    try ctx.exec(&.{ "kv", "set", "mg_del_b", "vb" });
    try ctx.exec(&.{ "kv", "delete", "mg_del_a" });

    var result = try ctx.cli.run(&.{ "kv", "mget", "mg_del_a", "mg_del_b", "--output", "json" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // mg_del_a was deleted, should be null
    try stdx.testing.assertContains(result, "null");
    // mg_del_b should still have its value
    try stdx.testing.assertContains(result, "vb");
}

// =============================================================================
// Multi-GET (mget) — Multi-Shard
// =============================================================================

test "e2e/kv: mget asks for every key when they overflow one request" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const a = testing.allocator;

    // 256 keys of 2000 bytes: twice what one request holds.
    var keys: [256][]u8 = undefined;
    for (&keys, 0..) |*k, i| {
        k.* = try a.alloc(u8, 2000);
        @memset(k.*, 'k');
        _ = try std.fmt.bufPrint(k.*[0..3], "{d:0>3}", .{i});
    }
    defer for (keys) |k| a.free(k);
    for ([_]usize{ 0, 130, 255 }) |i| try ctx.exec(&.{ "kv", "set", keys[i], "here" });

    var args: [4 + 256][]const u8 = undefined;
    args[0..4].* = .{ "kv", "mget", "-o", "raw" };
    for (keys, 0..) |k, i| args[4 + i] = k;
    var result = try ctx.cli.run(&args);
    defer result.deinit();
    try testing.expectEqual(@as(u8, 0), result.exit_code);
    var lines = std.mem.splitScalar(u8, std.mem.trimEnd(u8, result.stdout, "\n"), '\n');
    var n: usize = 0;
    while (lines.next()) |line| : (n += 1) {
        try testing.expect(std.mem.startsWith(u8, line, keys[n]));
        const want: []const u8 = if (n == 0 or n == 130 or n == 255) "\there" else "\t(nil)";
        try testing.expect(std.mem.endsWith(u8, line, want));
    }
    try testing.expectEqual(@as(usize, 256), n);
}

test "e2e/kv: mget with a routing key finds keys written with it, and without one finds each key on its own shard" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();

    // Sixteen keys co-located by a routing key, sixteen left to their own
    // shards: across 4 shards most of the first live away from their own.
    var names: [32][16]u8 = undefined;
    var keys: [32][]const u8 = undefined;
    for (&keys, 0..) |*k, i| {
        k.* = try std.fmt.bufPrint(&names[i], "{s}-{d}", .{ if (i < 16) "routed" else "plain", i });
        if (i < 16) try ctx.exec(&.{ "kv", "set", k.*, "r", "-r", "tenant-9" }) else try ctx.exec(&.{ "kv", "set", k.*, "p" });
    }

    var args: [6 + 16][]const u8 = undefined;
    args[0..6].* = .{ "kv", "mget", "-r", "tenant-9", "-o", "raw" };
    @memcpy(args[6..], keys[0..16]);
    var routed = try ctx.cli.run(&args);
    defer routed.deinit();
    try testing.expectEqual(@as(u8, 0), routed.exit_code);
    var want = std.ArrayList(u8).empty;
    defer want.deinit(testing.allocator);
    for (keys[0..16]) |k| try want.print(testing.allocator, "{s}\tr\n", .{k});
    try testing.expectEqualStrings(want.items, routed.stdout);

    var plain_args: [4 + 16][]const u8 = undefined;
    plain_args[0..4].* = .{ "kv", "mget", "-o", "raw" };
    @memcpy(plain_args[4..], keys[16..]);
    var plain = try ctx.cli.run(&plain_args);
    defer plain.deinit();
    try testing.expectEqual(@as(u8, 0), plain.exit_code);
    want.clearRetainingCapacity();
    for (keys[16..]) |k| try want.print(testing.allocator, "{s}\tp\n", .{k});
    try testing.expectEqualStrings(want.items, plain.stdout);
}

test "e2e/kv: mget across shards pages answers too large for one, keeping the order asked" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();
    const a = testing.allocator;

    // Eight values of 60 KB on whichever shards their keys hash to: one
    // answer holds four at most, so the client follows `next`.
    var names: [8][8]u8 = undefined;
    var keys: [8][]const u8 = undefined;
    var vals: [8][]u8 = undefined;
    for (&keys, &vals, 0..) |*k, *v, i| {
        k.* = try std.fmt.bufPrint(&names[i], "big-{d}", .{i});
        v.* = try a.alloc(u8, 60 * 1024);
        @memset(v.*, @intCast('a' + i));
        try ctx.exec(&.{ "kv", "set", k.*, v.* });
    }
    defer for (vals) |v| a.free(v);

    var args: [4 + 8][]const u8 = undefined;
    args[0..4].* = .{ "kv", "mget", "-o", "raw" };
    @memcpy(args[4..], &keys);
    var result = try ctx.cli.run(&args);
    defer result.deinit();
    try testing.expectEqual(@as(u8, 0), result.exit_code);
    var want = std.ArrayList(u8).empty;
    defer want.deinit(a);
    for (keys, vals) |k, v| try want.print(a, "{s}\t{s}\n", .{ k, v });
    try testing.expectEqualStrings(want.items, result.stdout);
}

test "e2e/kv: mget across shards returns all results" {
    // 4 shards — keys hash to different shards via Wyhash
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 4 },
    });
    defer ctx.deinit();

    // Use enough distinctly-named keys to ensure cross-shard distribution
    const keys = [_][]const u8{
        "alpha_cpu", "bravo_mem",   "charlie_disk", "delta_net",
        "echo_iops", "foxtrot_lat", "golf_tput",    "hotel_err",
    };
    const vals = [_][]const u8{
        "v_alpha", "v_bravo",   "v_charlie", "v_delta",
        "v_echo",  "v_foxtrot", "v_golf",    "v_hotel",
    };

    for (keys, vals) |k, v| {
        try ctx.exec(&.{ "kv", "set", k, v });
    }

    // MGET all 8 keys in one request — handler must gather from all shards
    var result = try ctx.cli.run(&.{
        "kv",           "mget",      "alpha_cpu", "bravo_mem",
        "charlie_disk", "delta_net", "echo_iops", "foxtrot_lat",
        "golf_tput",    "hotel_err", "--output",  "json",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    for (keys, vals) |k, v| {
        try stdx.testing.assertContains(result, k);
        try stdx.testing.assertContains(result, v);
    }
}

test "e2e/kv: mget across shards with partial hits" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 4 },
    });
    defer ctx.deinit();

    // Only set some keys — others will miss on various shards
    try ctx.exec(&.{ "kv", "set", "shard_hit_1", "found1" });
    try ctx.exec(&.{ "kv", "set", "shard_hit_3", "found3" });

    var result = try ctx.cli.run(&.{
        "kv",          "mget",         "shard_hit_1", "shard_miss_2",
        "shard_hit_3", "shard_miss_4", "--output",    "json",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "found1");
    try stdx.testing.assertContains(result, "found3");
    try stdx.testing.assertContains(result, "shard_miss_2");
    try stdx.testing.assertContains(result, "shard_miss_4");
    try stdx.testing.assertContains(result, "null");
}

test "e2e/kv: mget with namespace across shards" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .shards = 4 },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "mget_ns" });

    try ctx.exec(&.{ "kv", "set", "ns_key1", "ns_val1", "-n", "mget_ns" });
    try ctx.exec(&.{ "kv", "set", "ns_key2", "ns_val2", "-n", "mget_ns" });
    try ctx.exec(&.{ "kv", "set", "ns_key3", "ns_val3", "-n", "mget_ns" });

    // Also set same keys in default namespace with different values
    try ctx.exec(&.{ "kv", "set", "ns_key1", "default_val" });

    // MGET in the named namespace — should NOT return default namespace values
    var result = try ctx.cli.run(&.{
        "kv", "mget", "ns_key1", "ns_key2", "ns_key3", "-n", "mget_ns", "--output", "json",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "ns_val1");
    try stdx.testing.assertContains(result, "ns_val2");
    try stdx.testing.assertContains(result, "ns_val3");
    try stdx.testing.assertNotContains(result, "default_val");
}

// =============================================================================
// Multi-GET (mget) — Multi-Node Cluster
// =============================================================================

test "e2e/kv/cluster: mget after replication" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Write keys on node 0
    try cluster.execOn(0, &.{ "kv", "set", "cmg_a", "cluster_alpha" });
    try cluster.execOn(0, &.{ "kv", "set", "cmg_b", "cluster_beta" });
    try cluster.execOn(0, &.{ "kv", "set", "cmg_c", "cluster_gamma" });

    // Allow replication
    @import("stdx").time.sleep(500 * std.time.ns_per_ms);

    // MGET from node 1 — keys were written on node 0
    const result1 = try cluster.execCaptureOnWithRetry(1, &.{
        "kv", "mget", "cmg_a", "cmg_b", "cmg_c", "--output", "json",
    }, 5, 500);
    defer testing.allocator.free(result1);

    try testing.expect(std.mem.indexOf(u8, result1, "cluster_alpha") != null);
    try testing.expect(std.mem.indexOf(u8, result1, "cluster_beta") != null);
    try testing.expect(std.mem.indexOf(u8, result1, "cluster_gamma") != null);

    // MGET from node 2
    const result2 = try cluster.execCaptureOnWithRetry(2, &.{
        "kv", "mget", "cmg_a", "cmg_b", "cmg_c", "--output", "json",
    }, 5, 500);
    defer testing.allocator.free(result2);

    try testing.expect(std.mem.indexOf(u8, result2, "cluster_alpha") != null);
    try testing.expect(std.mem.indexOf(u8, result2, "cluster_beta") != null);
    try testing.expect(std.mem.indexOf(u8, result2, "cluster_gamma") != null);
}

test "e2e/kv/cluster: mget reads from all nodes" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Write different keys from different nodes
    try cluster.execOn(0, &.{ "kv", "set", "cmg_n0", "from_node0" });
    try cluster.execOn(1, &.{ "kv", "set", "cmg_n1", "from_node1" });
    try cluster.execOn(2, &.{ "kv", "set", "cmg_n2", "from_node2" });

    // Allow replication
    @import("stdx").time.sleep(500 * std.time.ns_per_ms);

    // MGET all three keys from each node
    for (0..3) |node| {
        const result = try cluster.execCaptureOnWithRetry(@intCast(node), &.{
            "kv", "mget", "cmg_n0", "cmg_n1", "cmg_n2", "--output", "json",
        }, 5, 500);
        defer testing.allocator.free(result);

        try testing.expect(std.mem.indexOf(u8, result, "from_node0") != null);
        try testing.expect(std.mem.indexOf(u8, result, "from_node1") != null);
        try testing.expect(std.mem.indexOf(u8, result, "from_node2") != null);
    }
}

test "e2e/kv/cluster: mget with partial hits across cluster" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Only set 2 of 4 keys
    try cluster.execOn(0, &.{ "kv", "set", "cmg_hit_a", "found_a" });
    try cluster.execOn(1, &.{ "kv", "set", "cmg_hit_b", "found_b" });

    // Allow replication
    @import("stdx").time.sleep(500 * std.time.ns_per_ms);

    // MGET 4 keys (2 exist, 2 missing) from node 2
    const result = try cluster.execCaptureOnWithRetry(2, &.{
        "kv", "mget", "cmg_hit_a", "cmg_miss_x", "cmg_hit_b", "cmg_miss_y", "--output", "json",
    }, 5, 500);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "found_a") != null);
    try testing.expect(std.mem.indexOf(u8, result, "found_b") != null);
    try testing.expect(std.mem.indexOf(u8, result, "cmg_miss_x") != null);
    try testing.expect(std.mem.indexOf(u8, result, "cmg_miss_y") != null);
    try testing.expect(std.mem.indexOf(u8, result, "null") != null);
}

test "e2e/kv/cluster: mget after node failure" {
    var cluster = try ClusterContext.initDefault(testing.allocator);
    defer cluster.deinit();

    // Write keys on node 0
    try cluster.execOn(0, &.{ "kv", "set", "cmg_survive_a", "survives_a" });
    try cluster.execOn(0, &.{ "kv", "set", "cmg_survive_b", "survives_b" });

    // Allow replication to all 3 nodes
    @import("stdx").time.sleep(500 * std.time.ns_per_ms);

    // Kill node 0
    cluster.stopNode(0);

    // Allow re-election
    @import("stdx").time.sleep(3 * std.time.ns_per_s);

    // MGET from surviving node — data should be available
    const result = try cluster.execCaptureOnWithRetry(1, &.{
        "kv", "mget", "cmg_survive_a", "cmg_survive_b", "--output", "json",
    }, 5, 1000);
    defer testing.allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "survives_a") != null);
    try testing.expect(std.mem.indexOf(u8, result, "survives_b") != null);
}

// =============================================================================
// KV Operations: INCR / EXPIRE / EXISTS / JSON.*
// Drives the CLI binary against a live server (CLI → TCP → handler → projection).
// =============================================================================

test "e2e/kv: incr creates and increments counter" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // First incr creates the counter at 1.
    const v1 = try ctx.execCapture(&.{ "kv", "incr", "counter" });
    try testing.expect(std.mem.indexOf(u8, v1, "1") != null);

    // Default delta is +1.
    const v2 = try ctx.execCapture(&.{ "kv", "incr", "counter" });
    try testing.expect(std.mem.indexOf(u8, v2, "2") != null);

    // --delta adds an explicit delta.
    const v3 = try ctx.execCapture(&.{ "kv", "incr", "counter", "--delta", "40" });
    try testing.expect(std.mem.indexOf(u8, v3, "42") != null);

    // Negative delta decrements.
    const v4 = try ctx.execCapture(&.{ "kv", "incr", "counter", "--delta", "-2" });
    try testing.expect(std.mem.indexOf(u8, v4, "40") != null);
}

test "e2e/kv: incr rejects non-counter values" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // SET a string value, then INCR should fail.
    try ctx.exec(&.{ "kv", "set", "not_a_counter", "hello" });

    var result = try ctx.cli.run(&.{ "kv", "incr", "not_a_counter" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 3), result.exit_code);
    try testing.expectEqualStrings("Error: bad request: the key's value isn't an 8-byte counter [bad_request/wrong_type]", std.mem.trimEnd(u8, result.stderr, "\n"));
}

test "e2e/kv: incr on a string whose TTL has run out starts a new counter" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "was_a_string", "hello", "--ttl-ms", "500ms" });
    @import("stdx").time.sleep(1200 * std.time.ns_per_ms);
    var result = try ctx.cli.run(&.{ "kv", "incr", "was_a_string" });
    defer result.deinit();
    try stdx.testing.assertSucceeded(result);
    try testing.expectEqualStrings("1", std.mem.trim(u8, result.stdout, " \n"));
}

test "e2e/kv: exists returns 1 for existing, 0 for missing" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "present", "yes" });

    // Present key → exit 0, and its version.
    var ok = try ctx.cli.run(&.{ "kv", "exists", "present" });
    defer ok.deinit();
    try testing.expectEqual(@as(u8, 0), ok.exit_code);
    try testing.expectEqualStrings("exists: true\nversion: 1\n", ok.stdout);

    // Missing key → exit 1.
    var missing = try ctx.cli.run(&.{ "kv", "exists", "absent" });
    defer missing.deinit();
    try testing.expectEqual(@as(u8, 1), missing.exit_code);
    try testing.expectEqualStrings("exists: false\nversion: 0\n", missing.stdout);
}

test "e2e/kv: expire sets a TTL, --ttl-ms 0 clears it, and --cas-version guards it" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "session", "active" });
    try testing.expectEqualStrings("OK", try ctx.execCapture(&.{ "kv", "expire", "session", "--ttl-ms", "1h" }));
    try testing.expectEqualStrings("OK", try ctx.execCapture(&.{ "kv", "expire", "session", "--ttl-ms", "0" }));
    var stale = try ctx.cli.run(&.{ "kv", "expire", "session", "--ttl-ms", "1h", "--cas-version", "99" });
    defer stale.deinit();
    try testing.expectEqual(@as(u8, 3), stale.exit_code);
    try testing.expect(std.mem.endsWith(u8, std.mem.trimEnd(u8, stale.stderr, "\n"), "[conflict/cas_mismatch]"));
    const value = try ctx.execCapture(&.{ "kv", "get", "session", "-o", "raw" });
    try testing.expectEqualStrings("active", value);
}

test "e2e/kv: expire on a missing key is not found, and --ttl-ms is required" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "kv", "expire", "nosuch", "--ttl-ms", "1m" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 1), result.exit_code);
    try testing.expectEqualStrings("Error: not found [not_found/not_found]", std.mem.trimEnd(u8, result.stderr, "\n"));
    var bare = try ctx.cli.run(&.{ "kv", "expire", "nosuch" });
    defer bare.deinit();
    try testing.expectEqual(@as(u8, 2), bare.exit_code);
}

test "e2e/kv: json set + json get round trip" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Set a full JSON document at root.
    try ctx.exec(&.{ "kv", "json", "set", "user:1", "{\"name\":\"alice\",\"age\":30}" });

    // json get with the default path "$" returns the whole document.
    const root = try ctx.execCapture(&.{ "kv", "json", "get", "user:1" });
    try testing.expect(std.mem.indexOf(u8, root, "alice") != null);
    try testing.expect(std.mem.indexOf(u8, root, "30") != null);

    // json get with a path returns the field.
    const name = try ctx.execCapture(&.{ "kv", "json", "get", "user:1", "--path", "$.name" });
    try testing.expect(std.mem.indexOf(u8, name, "alice") != null);
}

test "e2e/kv: json set updates nested field" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "json", "set", "user:2", "{\"profile\":{\"name\":\"alice\",\"age\":30}}" });

    // Update nested field.
    try ctx.exec(&.{ "kv", "json", "set", "user:2", "31", "--path", "$.profile.age" });

    const age = try ctx.execCapture(&.{ "kv", "json", "get", "user:2", "--path", "$.profile.age" });
    try testing.expect(std.mem.indexOf(u8, age, "31") != null);

    // Original sibling still present.
    const name = try ctx.execCapture(&.{ "kv", "json", "get", "user:2", "--path", "$.profile.name" });
    try testing.expect(std.mem.indexOf(u8, name, "alice") != null);
}

test "e2e/kv: json del removes field, leaves rest intact" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "json", "set", "doc", "{\"a\":1,\"b\":2,\"c\":3}" });
    try ctx.exec(&.{ "kv", "json", "del", "doc", "--path", "$.b" });

    // $.b is gone.
    var missing = try ctx.cli.run(&.{ "kv", "json", "get", "doc", "--path", "$.b" });
    defer missing.deinit();
    try stdx.testing.assertFailed(missing);

    // $.a and $.c remain.
    const a = try ctx.execCapture(&.{ "kv", "json", "get", "doc", "--path", "$.a" });
    try testing.expect(std.mem.indexOf(u8, a, "1") != null);
    const c = try ctx.execCapture(&.{ "kv", "json", "get", "doc", "--path", "$.c" });
    try testing.expect(std.mem.indexOf(u8, c, "3") != null);
}

test "e2e/kv: json array index access and update" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "json", "set", "doc", "{\"tags\":[\"red\",\"green\",\"blue\"]}" });

    const tag = try ctx.execCapture(&.{ "kv", "json", "get", "doc", "--path", "$.tags[1]" });
    try testing.expect(std.mem.indexOf(u8, tag, "green") != null);

    try ctx.exec(&.{ "kv", "json", "set", "doc", "\"yellow\"", "--path", "$.tags[1]" });

    const tag_after = try ctx.execCapture(&.{ "kv", "json", "get", "doc", "--path", "$.tags[1]" });
    try testing.expect(std.mem.indexOf(u8, tag_after, "yellow") != null);
}

test "e2e/kv: json get invalid path errors" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "json", "set", "doc2", "{\"a\":1}" });

    // Path missing leading $ is refused by name.
    var bad = try ctx.cli.run(&.{ "kv", "json", "get", "doc2", "--path", "no.dollar" });
    defer bad.deinit();
    try testing.expectEqual(@as(u8, 3), bad.exit_code);
    try testing.expectEqualStrings("Error: bad request: path isn't a JSON path [bad_request/malformed]", std.mem.trimEnd(u8, bad.stderr, "\n"));

    // Nothing at the path is not found.
    var missing = try ctx.cli.run(&.{ "kv", "json", "get", "doc2", "--path", "$.nope" });
    defer missing.deinit();
    try testing.expectEqual(@as(u8, 1), missing.exit_code);
    try testing.expectEqualStrings("Error: not found: nothing at that path in the document [not_found/not_found]", std.mem.trimEnd(u8, missing.stderr, "\n"));
}

test "e2e/kv: json del root removes whole key" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "json", "set", "ephemeral", "{\"x\":1}" });
    try ctx.exec(&.{ "kv", "json", "del", "ephemeral" });

    // Key should now be missing.
    var result = try ctx.cli.run(&.{ "kv", "exists", "ephemeral" });
    defer result.deinit();
    try stdx.testing.assertFailed(result);
    try stdx.testing.assertStdoutContains(result, "0");
}

test "e2e/kv: json set, get and del at a path, and del at $ removes the key" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "json", "set", "path_doc", "{\"x\":1}" });
    const v1 = try ctx.execCapture(&.{ "kv", "json", "get", "path_doc", "--path", "$.x" });
    try testing.expect(std.mem.indexOf(u8, v1, "1") != null);

    try ctx.exec(&.{ "kv", "json", "set", "path_doc", "2", "--path", "$.x" });
    const v2 = try ctx.execCapture(&.{ "kv", "json", "get", "path_doc", "--path", "$.x" });
    try testing.expect(std.mem.indexOf(u8, v2, "2") != null);

    try ctx.exec(&.{ "kv", "json", "del", "path_doc" });
    var missing = try ctx.cli.run(&.{ "kv", "exists", "path_doc" });
    defer missing.deinit();
    try stdx.testing.assertFailed(missing);
}

// =============================================================================
// Per-Shard Transactions (BEGIN / COMMIT / ROLLBACK)
// =============================================================================

test "e2e/kv: txn begin/put/get-RYW/commit/get" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // BEGIN — pin to a routing key.
    const begin_out = try ctx.execCapture(&.{ "kv", "txn", "begin", "tenant-A" });
    // Output: "txn: <N>\npinned: <M>\n"
    const tid_prefix = "txn: ";
    const idx = std.mem.indexOf(u8, begin_out, tid_prefix) orelse return error.MissingTxnId;
    var end = idx + tid_prefix.len;
    while (end < begin_out.len and std.ascii.isDigit(begin_out[end])) end += 1;
    const tid_str = begin_out[idx + tid_prefix.len .. end];
    try testing.expect(tid_str.len > 0);

    // Stage two writes inside the txn — they should not be visible outside.
    try ctx.exec(&.{ "kv", "set", "txn:k1", "v1", "--routing-key", "tenant-A", "--txn", tid_str });
    try ctx.exec(&.{ "kv", "set", "txn:k2", "v2", "--routing-key", "tenant-A", "--txn", tid_str });

    // RYW: a GET inside the same txn must see staged value.
    const ryw = try ctx.execCapture(&.{ "kv", "get", "txn:k1", "--routing-key", "tenant-A", "--txn", tid_str });
    try testing.expect(std.mem.indexOf(u8, ryw, "v1") != null);

    // Outside the txn, the staged value must not yet be visible.
    var pre_commit = try ctx.cli.run(&.{ "kv", "get", "txn:k1" });
    defer pre_commit.deinit();
    try testing.expectEqual(@as(u8, 1), pre_commit.exit_code);

    // COMMIT — values become visible.
    const commit_out = try ctx.execCapture(&.{ "kv", "txn", "commit", tid_str });
    try testing.expect(std.mem.endsWith(u8, commit_out, "ops: 2"));

    const post = try ctx.execCapture(&.{ "kv", "get", "txn:k1" });
    try testing.expect(std.mem.indexOf(u8, post, "v1") != null);
    const post2 = try ctx.execCapture(&.{ "kv", "get", "txn:k2" });
    try testing.expect(std.mem.indexOf(u8, post2, "v2") != null);
}

test "e2e/kv: txn rollback discards staged ops" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const begin_out = try ctx.execCapture(&.{ "kv", "txn", "begin", "rb-tenant" });
    const tid_prefix = "txn: ";
    const idx = std.mem.indexOf(u8, begin_out, tid_prefix) orelse return error.MissingTxnId;
    var end = idx + tid_prefix.len;
    while (end < begin_out.len and std.ascii.isDigit(begin_out[end])) end += 1;
    const tid_str = begin_out[idx + tid_prefix.len .. end];

    try ctx.exec(&.{ "kv", "set", "rb:k", "should-not-persist", "--routing-key", "rb-tenant", "--txn", tid_str });
    try ctx.exec(&.{ "kv", "txn", "rollback", tid_str });

    var result = try ctx.cli.run(&.{ "kv", "get", "rb:k", "--output", "table" });
    defer result.deinit();
    try expectNotFound(result);
}

test "e2e/kv: txn unknown id is rejected" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // A never-issued id, and one naming a shard the node doesn't have.
    for ([_][]const u8{ "99999", "18446462598732840961" }) |id| {
        var result = try ctx.cli.run(&.{ "kv", "set", "ghost", "value", "--routing-key", "ghost-tenant", "--txn", id });
        defer result.deinit();
        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expectEqualStrings("Error: not found: no such transaction — it committed, rolled back, or was begun in another namespace [not_found/no_such_transaction]", std.mem.trimEnd(u8, result.stderr, "\n"));
    }
}

// =============================================================================
// Raft log and hard state across restarts
// =============================================================================

test "e2e/kv: a 5 KiB value is stored and read back whole" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Larger than the CLI's 4 KiB print buffer. Exact equality: a truncated
    // or empty value must fail, not pass on a substring.
    const value = stdx.repeat("v", 5000);
    try ctx.exec(&.{ "kv", "set", "five_kib", value });
    const got = try ctx.execCapture(&.{ "kv", "get", "five_kib", "-o", "raw" });
    try testing.expectEqualStrings(value, got);
}

const HardStateFields = struct { node_id: u32, term: u64, voted_for: u32 };

/// The shard-0 HARDSTATE file: node id at 8, term at 12, vote at 20.
fn readHardState(data_dir: []const u8) !HardStateFields {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/00000/HARDSTATE", .{data_dir});
    var file_buf: [64]u8 = undefined;
    const bytes = try stdx.fs.readFile(path, &file_buf);
    if (bytes.len != 28) return error.UnexpectedHardStateSize;
    return .{
        .node_id = std.mem.readInt(u32, bytes[8..12], .little),
        .term = std.mem.readInt(u64, bytes[12..20], .little),
        .voted_for = std.mem.readInt(u32, bytes[20..24], .little),
    };
}

fn readHardStateTerm(data_dir: []const u8) !u64 {
    return (try readHardState(data_dir)).term;
}

test "e2e/kv: the durable log starts at index 1 and each restart opens a new persisted term" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();
    const data_dir = ctx.getDataDir();

    // Term 1 was persisted at bootstrap, before any client write, and the
    // vote is for this node's own id: the Raft node id and the stored node
    // id are one id space.
    const hs = try readHardState(data_dir);
    try testing.expectEqual(@as(u64, 1), hs.term);
    try testing.expect(hs.node_id != 0);
    try testing.expectEqual(hs.node_id, hs.voted_for);

    // The first write flushes (sync durability), and the segment it lands
    // in starts at index 1: the leadership noop is durable like any entry.
    try ctx.exec(&.{ "kv", "set", "first_key", "first_value" });
    var seg_buf: [std.fs.max_path_bytes]u8 = undefined;
    const first_seg = try std.fmt.bufPrint(&seg_buf, "{s}/00000/segs/0000000001.flseg", .{data_dir});
    try stdx.fs.access(first_seg, .{});

    try ctx.restartServer();
    try testing.expectEqual(@as(u64, 2), try readHardStateTerm(data_dir));
    const after = try ctx.execCapture(&.{ "kv", "get", "first_key", "-o", "raw" });
    try testing.expectEqualStrings("first_value", after);

    // A write after the restart continues the log at the next index; a log
    // restarted at 1 would overwrite 0000000001.flseg and lose first_key at
    // the next restart.
    try ctx.exec(&.{ "kv", "set", "second_key", "second_value" });
    try ctx.restartServer();
    try testing.expectEqual(@as(u64, 3), try readHardStateTerm(data_dir));
    const first_again = try ctx.execCapture(&.{ "kv", "get", "first_key", "-o", "raw" });
    try testing.expectEqualStrings("first_value", first_again);
    const second = try ctx.execCapture(&.{ "kv", "get", "second_key", "-o", "raw" });
    try testing.expectEqualStrings("second_value", second);
}

test "e2e/kv: a 90 KB transaction batch is applied, not just acked" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Three 30 KB values in one commit: a 90 KB kv_batch entry. It must be
    // readable immediately, not only after a restart replays the segment.
    // `kv txn begin` prints `txn: N` and `pinned: H`, a line each.
    const begin = try ctx.execCapture(&.{ "kv", "txn", "begin", "rk", "-o", "raw" });
    const id_start = (std.mem.indexOf(u8, begin, "txn: ") orelse return error.NoTxnId) + "txn: ".len;
    const id_end = std.mem.indexOfScalarPos(u8, begin, id_start, '\n') orelse begin.len;
    const txn_id = begin[id_start..id_end];
    const value = stdx.repeat("t", 30_000);
    try ctx.exec(&.{ "kv", "set", "txn_a", value, "-t", txn_id, "-r", "rk" });
    try ctx.exec(&.{ "kv", "set", "txn_b", value, "-t", txn_id, "-r", "rk" });
    try ctx.exec(&.{ "kv", "set", "txn_c", value, "-t", txn_id, "-r", "rk" });
    try ctx.exec(&.{ "kv", "txn", "commit", txn_id });
    for ([_][]const u8{ "txn_a", "txn_b", "txn_c" }) |key| {
        const got = try ctx.execCapture(&.{ "kv", "get", key, "-r", "rk", "-o", "raw" });
        try testing.expectEqualStrings(value, got);
    }
}

fn nestedArrays(levels: usize) ![]u8 {
    const out = try testing.allocator.alloc(u8, levels * 2);
    @memset(out[0..levels], '[');
    @memset(out[levels..], ']');
    return out;
}

test "e2e/kv: JSON nesting is limited to 128 levels" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    const refusal = "Error: bad request: JSON nesting is limited to 128 levels [bad_request/field_too_long]";

    const ok = try nestedArrays(128);
    defer testing.allocator.free(ok);
    try ctx.exec(&.{ "kv", "json", "set", "deep128", ok });
    const got = try ctx.execCapture(&.{ "kv", "json", "get", "deep128", "--path", "$" });
    try testing.expect(std.mem.indexOf(u8, got, ok) != null);

    const over = try nestedArrays(129);
    defer testing.allocator.free(over);
    var set = try ctx.cli.run(&.{ "kv", "json", "set", "deep129", over });
    defer set.deinit();
    try testing.expectEqual(@as(u8, 3), set.exit_code);
    try testing.expectEqualStrings(refusal, std.mem.trimEnd(u8, set.stderr, "\n"));

    // A plain set takes any bytes; reading them as JSON is refused.
    const far = try nestedArrays(300);
    defer testing.allocator.free(far);
    try ctx.exec(&.{ "kv", "set", "deep300", far });
    var get = try ctx.cli.run(&.{ "kv", "json", "get", "deep300", "--path", "$" });
    defer get.deinit();
    try testing.expectEqual(@as(u8, 3), get.exit_code);
    try testing.expectEqualStrings(refusal, std.mem.trimEnd(u8, get.stderr, "\n"));

    // The node still serves.
    const again = try ctx.execCapture(&.{ "kv", "json", "get", "deep128", "--path", "$" });
    try testing.expect(std.mem.indexOf(u8, again, ok) != null);
}

test "e2e/kv: a value that isn't UTF-8 is a JSON string with U+FFFD under -o json, and exact under -o raw" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "kv", "set", "bin", "a\xffb\"" });

    var raw = try ctx.cli.run(&.{ "kv", "get", "bin", "-o", "raw" });
    defer raw.deinit();
    try testing.expectEqual(@as(u8, 0), raw.exit_code);
    try testing.expectEqualStrings("a\xffb\"\n", raw.stdout);

    var json = try ctx.cli.run(&.{ "kv", "get", "bin", "-o", "json" });
    defer json.deinit();
    try testing.expectEqual(@as(u8, 0), json.exit_code);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, json.stdout, .{});
    defer parsed.deinit();
    try testing.expectEqualStrings("a\u{fffd}b\"", parsed.value.object.get("value").?.string);
}

// A merge that would grow a document past the largest value a read can
// answer is refused when it's written, so no key is stored that can't be
// read back.
test "e2e/kv: json set refuses a merge past the largest value" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Built up in merges: Linux caps one argument at 128 KiB, so no
    // argument here passes 100 KB. Each merge replaces a field the document
    // already has.
    const filler = try testing.allocator.alloc(u8, 100_000);
    defer testing.allocator.free(filler);
    @memset(filler, 'x');
    try ctx.exec(&.{ "kv", "json", "set", "big", "{\"a\":\"\",\"b\":\"\",\"c\":\"\",\"d\":\"\"}" });
    for ([_]struct { path: []const u8, len: usize }{ .{ .path = "$.a", .len = 100_000 }, .{ .path = "$.b", .len = 100_000 }, .{ .path = "$.c", .len = 51_000 } }) |part| {
        const more = try std.fmt.allocPrint(testing.allocator, "\"{s}\"", .{filler[0..part.len]});
        defer testing.allocator.free(more);
        try ctx.exec(&.{ "kv", "json", "set", "big", more, "--path", part.path });
    }
    // About 251,000 bytes now, under the largest value.
    var before = try ctx.cli.run(&.{ "kv", "get", "big", "--output", "raw" });
    defer before.deinit();
    try testing.expectEqual(@as(u8, 0), before.exit_code);
    try testing.expect(before.stdout.len > 250_000);

    const last = try std.fmt.allocPrint(testing.allocator, "\"{s}\"", .{filler[0..3_000]});
    defer testing.allocator.free(last);
    var grown = try ctx.cli.run(&.{ "kv", "json", "set", "big", last, "--path", "$.d" });
    defer grown.deinit();
    try testing.expectEqual(@as(u8, 3), grown.exit_code);
    const want = std.fmt.comptimePrint("Error: bad request: the value to write is more than a key holds: at most {d} bytes [bad_request/field_too_long]", .{@import("src").protocol.kv_ops.MAX_VALUE});
    try testing.expectEqualStrings(want, std.mem.trimEnd(u8, grown.stderr, "\n"));

    // The document is as it was, and reads back whole.
    var after = try ctx.cli.run(&.{ "kv", "get", "big", "--output", "raw" });
    defer after.deinit();
    try testing.expectEqual(@as(u8, 0), after.exit_code);
    try testing.expectEqualStrings(before.stdout, after.stdout);
}

// Each set at a new path adds a field to the document's object, which grows
// as the fields arrive; the server stays up and the document holds them all.
test "e2e/kv: json set adds field after field to one object" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "json", "set", "grown", "{\"a\":1,\"b\":2}" });
    for ([_][]const u8{ "$.c", "$.d", "$.e", "$.f", "$.g", "$.h", "$.i", "$.j" }) |path| {
        try ctx.exec(&.{ "kv", "json", "set", "grown", "3", "--path", path });
    }
    const doc = try ctx.execCapture(&.{ "kv", "json", "get", "grown" });
    try testing.expectEqualStrings("{\"a\":1,\"b\":2,\"c\":3,\"d\":3,\"e\":3,\"f\":3,\"g\":3,\"h\":3,\"i\":3,\"j\":3}", std.mem.trimEnd(u8, doc, "\n"));
}
