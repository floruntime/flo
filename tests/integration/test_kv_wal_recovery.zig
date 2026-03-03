const std = @import("std");
const testing = std.testing;
const src = @import("src");

const KVWAL = src.storage.kv_wal.KVWAL;
const KVProjection = src.projection.kv.KVProjection;

test "integration: KV WAL write and recover" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(path);
    const wal_path = try std.fmt.allocPrint(testing.allocator, "{s}/test_recovery.wal", .{path});
    defer testing.allocator.free(wal_path);

    // Phase 1: Write 100 puts and 10 deletes to WAL
    {
        var wal = try KVWAL.init(testing.allocator, wal_path);
        defer wal.deinit();

        // Write 100 keys
        var key_buf: [32]u8 = undefined;
        var val_buf: [64]u8 = undefined;
        for (0..100) |i| {
            const key = std.fmt.bufPrint(&key_buf, "key-{d:0>4}", .{i}) catch unreachable;
            const value = std.fmt.bufPrint(&val_buf, "value-{d:0>4}", .{i}) catch unreachable;
            try wal.appendPut(key, value, 0);
        }

        // Delete 10 keys (keys 0-9)
        for (0..10) |i| {
            const key = std.fmt.bufPrint(&key_buf, "key-{d:0>4}", .{i}) catch unreachable;
            try wal.appendDelete(key);
        }

        wal.sync();
    }

    // Phase 2: Reopen WAL and replay into a fresh KVProjection
    {
        var wal = try KVWAL.init(testing.allocator, wal_path);
        defer wal.deinit();

        var kv = KVProjection.init(testing.allocator, 0);
        defer kv.deinit();

        const count = try wal.replay(&kv, struct {
            fn put(proj: *KVProjection, key: []const u8, value: []const u8, lsn: u64, expiry_ns: u64) !void {
                try proj.put(key, value, lsn, 0, 0, expiry_ns);
            }
        }.put, struct {
            fn del(proj: *KVProjection, key: []const u8, lsn: u64) !void {
                try proj.delete(key, lsn, 0, 0);
            }
        }.del);

        // 100 puts + 10 deletes = 110 records replayed
        try testing.expectEqual(@as(u64, 110), count);

        // Verify 90 live keys remain (100 - 10 deleted)
        try testing.expectEqual(@as(usize, 90), kv.count());

        // Deleted keys (0-9) should return null
        var key_buf: [32]u8 = undefined;
        for (0..10) |i| {
            const key = std.fmt.bufPrint(&key_buf, "key-{d:0>4}", .{i}) catch unreachable;
            try testing.expect(kv.get(key) == null);
        }

        // Remaining keys (10-99) should have correct values
        var val_buf: [64]u8 = undefined;
        for (10..100) |i| {
            const key = std.fmt.bufPrint(&key_buf, "key-{d:0>4}", .{i}) catch unreachable;
            const expected_value = std.fmt.bufPrint(&val_buf, "value-{d:0>4}", .{i}) catch unreachable;
            const entry = kv.get(key);
            try testing.expect(entry != null);
            try testing.expectEqualStrings(expected_value, entry.?.value);
        }
    }
}

test "integration: KV WAL recovery with TTL" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(path);
    const wal_path = try std.fmt.allocPrint(testing.allocator, "{s}/test_ttl.wal", .{path});
    defer testing.allocator.free(wal_path);

    // Write keys with different TTLs
    {
        var wal = try KVWAL.init(testing.allocator, wal_path);
        defer wal.deinit();

        // Key with no TTL
        try wal.appendPut("permanent", "no-expire", 0);

        // Key with far-future TTL (won't expire during test)
        const far_future: u64 = @intCast(std.time.nanoTimestamp());
        try wal.appendPut("long-lived", "expires-later", far_future + 60 * std.time.ns_per_s);

        // Key with already-expired TTL
        try wal.appendPut("already-expired", "gone", 1); // expiry_ns=1 is in the past

        wal.sync();
    }

    // Replay and verify
    {
        var wal = try KVWAL.init(testing.allocator, wal_path);
        defer wal.deinit();

        var kv = KVProjection.init(testing.allocator, 0);
        defer kv.deinit();

        const count = try wal.replay(&kv, struct {
            fn put(proj: *KVProjection, key: []const u8, value: []const u8, lsn: u64, expiry_ns: u64) !void {
                try proj.put(key, value, lsn, 0, 0, expiry_ns);
            }
        }.put, struct {
            fn del(proj: *KVProjection, key: []const u8, lsn: u64) !void {
                try proj.delete(key, lsn, 0, 0);
            }
        }.del);

        try testing.expectEqual(@as(u64, 3), count);

        // Permanent key should be accessible (no TTL)
        const perm = kv.get("permanent");
        try testing.expect(perm != null);
        try testing.expectEqualStrings("no-expire", perm.?.value);

        // Long-lived key should be accessible (TTL in far future)
        const long_lived = kv.get("long-lived");
        try testing.expect(long_lived != null);
        try testing.expectEqualStrings("expires-later", long_lived.?.value);

        // Already-expired key should return null (expiry_ns=1 is way in the past)
        const expired = kv.get("already-expired");
        try testing.expect(expired == null);
    }
}
