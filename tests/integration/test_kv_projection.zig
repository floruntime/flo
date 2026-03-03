const std = @import("std");
const testing = std.testing;
const src = @import("src");

const KVProjection = src.projection.kv.KVProjection;
const ScanEntry = src.projection.kv.ScanEntry;

test "integration: KV projection 10000 operations" {
    var kv = KVProjection.init(testing.allocator, 0); // 0 = unlimited memory
    defer kv.deinit();

    // Put 10000 keys
    var key_buf: [32]u8 = undefined;
    var val_buf: [64]u8 = undefined;
    for (0..10000) |i| {
        const key = std.fmt.bufPrint(&key_buf, "key-{d:0>6}", .{i}) catch unreachable;
        const value = std.fmt.bufPrint(&val_buf, "value-{d:0>6}-payload", .{i}) catch unreachable;
        try kv.put(key, value, @as(u64, @intCast(i)) + 1, 1, 0, 0);
    }

    // Verify count
    try testing.expectEqual(@as(usize, 10000), kv.count());

    // Scan a batch and verify we get results
    var scan_results: [100]ScanEntry = undefined;
    const scan_count = kv.scan(&scan_results);
    try testing.expectEqual(@as(usize, 100), scan_count);

    // Verify a few specific keys
    {
        const e = kv.get("key-000000");
        try testing.expect(e != null);
        try testing.expectEqualStrings("value-000000-payload", e.?.value);
    }
    {
        const e = kv.get("key-005000");
        try testing.expect(e != null);
        try testing.expectEqualStrings("value-005000-payload", e.?.value);
    }
    {
        const e = kv.get("key-009999");
        try testing.expect(e != null);
        try testing.expectEqualStrings("value-009999-payload", e.?.value);
    }

    // Delete first 5000 keys
    for (0..5000) |i| {
        const key = std.fmt.bufPrint(&key_buf, "key-{d:0>6}", .{i}) catch unreachable;
        try kv.delete(key, @as(u64, @intCast(i)) + 10001, 1, 0);
    }

    // Verify count after deletion
    try testing.expectEqual(@as(usize, 5000), kv.count());

    // Deleted keys should return null
    try testing.expect(kv.get("key-000000") == null);
    try testing.expect(kv.get("key-004999") == null);

    // Remaining keys should still be accessible
    {
        const e = kv.get("key-005000");
        try testing.expect(e != null);
        try testing.expectEqualStrings("value-005000-payload", e.?.value);
    }
    {
        const e = kv.get("key-009999");
        try testing.expect(e != null);
    }

    // Stats should reflect operations
    try testing.expectEqual(@as(u64, 10000), kv.stats.puts);
    try testing.expectEqual(@as(u64, 5000), kv.stats.deletes);
}

test "integration: KV projection TTL expiry" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    // Put a key with a very short TTL (expires 1 ns in the future)
    const now: u64 = @intCast(std.time.nanoTimestamp());
    try kv.put("ephemeral", "gone-soon", 1, 1, now, now + 1);

    // Also put a key with no TTL for comparison
    try kv.put("permanent", "stays-forever", 2, 1, now, 0);

    // Sleep 2ms to ensure the TTL expires
    std.Thread.sleep(2 * std.time.ns_per_ms);

    // Expired key should return null
    try testing.expect(kv.get("ephemeral") == null);

    // Non-expiring key should still be accessible
    const perm = kv.get("permanent");
    try testing.expect(perm != null);
    try testing.expectEqualStrings("stays-forever", perm.?.value);
}

test "integration: KV projection scan with prefix" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    // Put keys with "user:" prefix
    try kv.put("user:alice", "alice-data", 1, 1, 0, 0);
    try kv.put("user:bob", "bob-data", 2, 1, 0, 0);
    try kv.put("user:charlie", "charlie-data", 3, 1, 0, 0);

    // Put keys with "item:" prefix
    try kv.put("item:sword", "damage=50", 4, 1, 0, 0);
    try kv.put("item:shield", "armor=30", 5, 1, 0, 0);

    // Put keys with other prefixes
    try kv.put("session:xyz", "active", 6, 1, 0, 0);
    try kv.put("config:max_conn", "1000", 7, 1, 0, 0);

    try testing.expectEqual(@as(usize, 7), kv.count());

    // Scan with "user:" prefix — should return exactly 3 entries
    var user_results: [10]ScanEntry = undefined;
    const user_count = kv.scanPrefix("user:", &user_results);
    try testing.expectEqual(@as(usize, 3), user_count);

    // Verify all returned entries have the "user:" prefix
    for (0..user_count) |i| {
        try testing.expect(std.mem.startsWith(u8, user_results[i].key, "user:"));
    }

    // Scan with "item:" prefix — should return exactly 2 entries
    var item_results: [10]ScanEntry = undefined;
    const item_count = kv.scanPrefix("item:", &item_results);
    try testing.expectEqual(@as(usize, 2), item_count);

    for (0..item_count) |i| {
        try testing.expect(std.mem.startsWith(u8, item_results[i].key, "item:"));
    }

    // Scan with non-existent prefix — should return 0
    var empty_results: [10]ScanEntry = undefined;
    const empty_count = kv.scanPrefix("nonexistent:", &empty_results);
    try testing.expectEqual(@as(usize, 0), empty_count);

    // Scan with "session:" prefix — should return exactly 1
    var session_results: [10]ScanEntry = undefined;
    const session_count = kv.scanPrefix("session:", &session_results);
    try testing.expectEqual(@as(usize, 1), session_count);
    try testing.expectEqualStrings("session:xyz", session_results[0].key);
    try testing.expectEqualStrings("active", session_results[0].value);
}
