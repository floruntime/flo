//! KV's applied state at volume and over its value types, through the KV
//! machine as a shard applies it.

const std = @import("std");
const testing = std.testing;
const src = @import("src");

const TestKv = src.kv.machine.TestKv;
const entry_mod = src.storage.ual.entry;
const kv_entries = src.protocol.kv_entries;

fn apply(kv: *TestKv, comptime t: entry_mod.EntryType, payload: kv_entries.Payload(@backingInt(t)).Value, stamp: u64) !src.storage.persistence.Result {
    var buf: [256]u8 = undefined;
    const bytes = try kv_entries.Payload(@backingInt(t)).encodeInto(&buf, payload);
    return kv.apply(&entry_mod.buildEntry(t, 0, 1, kv.index + 1, stamp, bytes));
}

test "integration: KV state takes 10000 puts and 5000 deletes, and counts them" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    var key_buf: [32]u8 = undefined;
    var val_buf: [64]u8 = undefined;
    for (0..10000) |i| {
        const key = std.fmt.bufPrint(&key_buf, "key-{d:0>6}", .{i}) catch unreachable;
        const value = std.fmt.bufPrint(&val_buf, "value-{d:0>6}-payload", .{i}) catch unreachable;
        try kv.put(key, value, 0);
    }
    const st = kv.state();
    try testing.expectEqual(@as(u64, 10000), st.liveKeys("default"));
    try testing.expectEqualStrings("value-005000-payload", st.get("key-005000").?.value);
    for (0..5000) |i| {
        const key = std.fmt.bufPrint(&key_buf, "key-{d:0>6}", .{i}) catch unreachable;
        _ = try apply(&kv, .kv_delete, .{ .key = key }, 0);
    }
    try testing.expectEqual(@as(u64, 5000), st.liveKeys("default"));
    try testing.expect(st.get("key-004999") == null);
    try testing.expectEqualStrings("value-009999-payload", st.get("key-009999").?.value);
    // Scans see live keys only, in key order.
    var names: [8][]const u8 = undefined;
    try testing.expectEqual(@as(usize, 8), st.scanKeyNames("default", "", &names));
    try testing.expectEqualStrings("key-005000", names[0]);
    try testing.expectEqualStrings("key-005007", names[7]);
}

test "integration: KV state scans by prefix, in key order" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    for ([_][]const u8{ "user:charlie", "item:sword", "user:alice", "session:xyz", "user:bob", "item:shield", "config:max_conn" }) |k| try kv.put(k, "v", 0);
    const st = kv.state();
    var names: [10][]const u8 = undefined;
    try testing.expectEqual(@as(usize, 3), st.scanKeyNames("default", "user:", &names));
    for (names[0..3], [_][]const u8{ "user:alice", "user:bob", "user:charlie" }) |got, want| try testing.expectEqualStrings(want, got);
    try testing.expectEqual(@as(usize, 2), st.scanKeyNames("default", "item:", &names));
    try testing.expectEqual(@as(usize, 0), st.scanKeyNames("default", "nonexistent:", &names));
    try testing.expectEqual(@as(usize, 7), st.scanKeyNames("default", "", &names));
}

test "integration: KV incr makes a counter and adds to it, and refuses a value that isn't one or would overflow" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    for ([_]i64{ 1, 41, -10 }) |d| _ = try apply(&kv, .kv_incr, .{ .key = "counter", .delta = d }, 0);
    const e = kv.state().get("counter").?;
    try testing.expectEqual(@as(i64, 32), std.mem.readInt(i64, e.value[0..8], .little));
    try testing.expectEqual(src.kv.machine.ValueType.counter, e.value_type);

    try kv.put("notcounter", "hello", 0);
    try testing.expectEqualStrings(src.kv.machine.NOT_A_COUNTER, (try apply(&kv, .kv_incr, .{ .key = "notcounter", .delta = 1 }, 0)).refused().?.refusal.message);
    _ = try apply(&kv, .kv_incr, .{ .key = "big", .delta = std.math.maxInt(i64) }, 0);
    try testing.expectEqualStrings(src.kv.machine.INCR_OVERFLOW, (try apply(&kv, .kv_incr, .{ .key = "big", .delta = 1 }, 0)).refused().?.refusal.message);
}

test "integration: KV expire sets and clears a TTL, and finds nothing on a missing key" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    const stamp: u64 = 1_000 * std.time.ns_per_s;
    try kv.put("doc", "data", stamp);
    _ = try apply(&kv, .kv_expire, .{ .key = "doc", .ttl_ms = 1_000 }, stamp);
    try testing.expectEqual(stamp + std.time.ns_per_s, kv.state().getRaw("doc").?.expiry_ns);
    _ = try apply(&kv, .kv_expire, .{ .key = "doc", .ttl_ms = 0 }, stamp);
    try testing.expectEqual(@as(u64, 0), kv.state().getRaw("doc").?.expiry_ns);
    try testing.expect((try apply(&kv, .kv_expire, .{ .key = "nope", .ttl_ms = 0 }, stamp)).refused() != null);
}

// json_path over values the state holds. The ops end to end are in
// tests/e2e/kv_test.zig.

test "integration: JSON paths read, set and delete over a stored document" {
    const json_path = src.util.json_path;
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    const st = kv.state();
    const initial = "{\"name\":\"alice\",\"profile\":{\"age\":30},\"b\":2}";
    try kv.put("user:1", initial, 0);

    const all = try json_path.jsonPathGet(testing.allocator, st.get("user:1").?.value, "$");
    defer testing.allocator.free(all);
    try testing.expectEqualStrings(initial, all);

    const merged = try json_path.jsonPathSet(testing.allocator, st.get("user:1").?.value, "$.profile.age", "31");
    defer testing.allocator.free(merged);
    try kv.put("user:1", merged, 0);
    const age = try json_path.jsonPathGet(testing.allocator, st.get("user:1").?.value, "$.profile.age");
    defer testing.allocator.free(age);
    try testing.expectEqualStrings("31", age);

    const deleted = try json_path.jsonPathDel(testing.allocator, st.get("user:1").?.value, "$.b");
    defer testing.allocator.free(deleted);
    try kv.put("user:1", deleted, 0);
    try testing.expectError(error.PathNotFound, json_path.jsonPathGet(testing.allocator, st.get("user:1").?.value, "$.b"));
    try testing.expectError(error.InvalidPath, json_path.jsonPathGet(testing.allocator, st.get("user:1").?.value, "no.dollar"));
    try kv.put("notjson", "raw bytes", 0);
    try testing.expectError(error.InvalidJson, json_path.jsonPathGet(testing.allocator, st.get("notjson").?.value, "$.x"));
}
