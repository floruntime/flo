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
    const now: u64 = @intCast(@import("stdx").time.nanoTimestamp());
    try kv.put("ephemeral", "gone-soon", 1, 1, now, now + 1);

    // Also put a key with no TTL for comparison
    try kv.put("permanent", "stays-forever", 2, 1, now, 0);

    // Sleep 2ms to ensure the TTL expires
    @import("stdx").time.sleep(2 * std.time.ns_per_ms);

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

test "integration: KV INCR creates counter and increments" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    const v1 = try kv.applyIncr("counter", 1, 1, 1, 0);
    try testing.expectEqual(@as(i64, 1), v1);

    const v2 = try kv.applyIncr("counter", 41, 2, 1, 0);
    try testing.expectEqual(@as(i64, 42), v2);

    const v3 = try kv.applyIncr("counter", -10, 3, 1, 0);
    try testing.expectEqual(@as(i64, 32), v3);

    const entry = kv.get("counter").?;
    try testing.expectEqual(@as(usize, 8), entry.value.len);
    try testing.expectEqual(@as(i64, 32), std.mem.readInt(i64, entry.value[0..8], .little));
}

test "integration: KV INCR rejects non-counter values" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.put("notcounter", "hello", 1, 1, 0, 0);
    try testing.expectError(error.NotACounter, kv.applyIncr("notcounter", 1, 2, 1, 0));
}

test "integration: KV INCR overflow detection" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    var max_buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &max_buf, std.math.maxInt(i64), .little);
    try kv.putWithType("big", &max_buf, 1, 1, 0, 0, src.projection.kv.ValueType.counter);

    try testing.expectError(error.Overflow, kv.applyIncr("big", 1, 2, 1, 0));
}

test "integration: KV TOUCH updates expiry" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    const now: u64 = @intCast(@import("stdx").time.nanoTimestamp());
    try kv.put("doc", "data", 1, 1, now, 0);

    try kv.applyTouch("doc", now + std.time.ns_per_s, 2, 1, now);
    const entry = kv.get("doc").?;
    try testing.expectEqual(now + std.time.ns_per_s, entry.expiry_ns);

    // Persist (expiry=0)
    try kv.applyTouch("doc", 0, 3, 1, now);
    const after = kv.get("doc").?;
    try testing.expectEqual(@as(u64, 0), after.expiry_ns);
}

test "integration: KV TOUCH on missing key" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();
    try testing.expectError(error.NotFound, kv.applyTouch("nope", 0, 1, 1, 0));
}

test "integration: KV snapshot round-trip preserves value_type" {
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    _ = try kv.applyIncr("counter", 100, 1, 1, 0);
    try kv.put("string", "hello", 2, 1, 0, 0);
    try kv.put("json", "{\"k\":1}", 3, 1, 0, 0);

    const snapshot = try kv.serialize(testing.allocator);
    defer testing.allocator.free(snapshot);

    var kv2 = KVProjection.init(testing.allocator, 0);
    defer kv2.deinit();
    try kv2.deserialize(snapshot);

    const counter = kv2.get("counter").?;
    try testing.expectEqual(src.projection.kv.ValueType.counter, counter.value_type);
    try testing.expectEqual(@as(i64, 100), std.mem.readInt(i64, counter.value[0..8], .little));

    const string_entry = kv2.get("string").?;
    try testing.expectEqual(src.projection.kv.ValueType.string, string_entry.value_type);
}

// =============================================================================
// JSON path integration — validates util.json_path against the projection.
// End-to-end coverage of the same opcodes through the CLI lives in
// tests/e2e/kv_test.zig.
// =============================================================================

test "integration: JSON.GET reads root and field via json_path" {
    const json_path = src.util.json_path;
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    const initial = "{\"name\":\"alice\",\"age\":30}";
    try kv.put("user:1", initial, 1, 1, 0, 0);

    const all = try json_path.jsonPathGet(testing.allocator, kv.get("user:1").?.value, "$");
    defer testing.allocator.free(all);
    try testing.expectEqualStrings(initial, all);

    const name = try json_path.jsonPathGet(testing.allocator, kv.get("user:1").?.value, "$.name");
    defer testing.allocator.free(name);
    try testing.expectEqualStrings("\"alice\"", name);
}

test "integration: JSON.SET nested field updates document" {
    const json_path = src.util.json_path;
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    const initial = "{\"profile\":{\"name\":\"alice\",\"age\":30}}";
    try kv.put("user:1", initial, 1, 1, 0, 0);

    const merged = try json_path.jsonPathSet(testing.allocator, kv.get("user:1").?.value, "$.profile.age", "31");
    defer testing.allocator.free(merged);
    try kv.put("user:1", merged, 2, 1, 0, 0);

    const age = try json_path.jsonPathGet(testing.allocator, kv.get("user:1").?.value, "$.profile.age");
    defer testing.allocator.free(age);
    try testing.expectEqualStrings("31", age);
}

test "integration: JSON.DEL removes field, leaves rest intact" {
    const json_path = src.util.json_path;
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.put("doc", "{\"a\":1,\"b\":2,\"c\":3}", 1, 1, 0, 0);

    const merged = try json_path.jsonPathDel(testing.allocator, kv.get("doc").?.value, "$.b");
    defer testing.allocator.free(merged);
    try kv.put("doc", merged, 2, 1, 0, 0);

    try testing.expectError(error.PathNotFound, json_path.jsonPathGet(testing.allocator, kv.get("doc").?.value, "$.b"));
    const a = try json_path.jsonPathGet(testing.allocator, kv.get("doc").?.value, "$.a");
    defer testing.allocator.free(a);
    try testing.expectEqualStrings("1", a);
}

test "integration: JSON path errors propagate" {
    const json_path = src.util.json_path;
    var kv = KVProjection.init(testing.allocator, 0);
    defer kv.deinit();

    try kv.put("doc", "{\"a\":1}", 1, 1, 0, 0);

    try testing.expectError(error.PathNotFound, json_path.jsonPathGet(testing.allocator, kv.get("doc").?.value, "$.missing"));
    try testing.expectError(error.InvalidPath, json_path.jsonPathGet(testing.allocator, kv.get("doc").?.value, "no.dollar"));
    try kv.put("notjson", "raw bytes", 2, 1, 0, 0);
    try testing.expectError(error.InvalidJson, json_path.jsonPathGet(testing.allocator, kv.get("notjson").?.value, "$.x"));
}
