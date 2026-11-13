const std = @import("std");
const testing = std.testing;
const src = @import("src");

const Router = src.node.router.Router;
const RouteTarget = src.node.router.RouteTarget;

test "integration: router distributes keys across shards" {
    const shard_count: u16 = 4;
    const router = Router.init(shard_count, 0);

    // Route 10000 random-ish keys and count per-shard distribution
    var per_shard = @as([4]usize, @splat(0));
    var key_buf: [32]u8 = undefined;

    for (0..10000) |i| {
        const key = std.fmt.bufPrint(&key_buf, "random-key-{d}", .{i}) catch unreachable;
        const target = router.routeKey(key);
        switch (target) {
            .local => per_shard[0] += 1,
            .shard => |t| {
                per_shard[t.shard_id] += 1;
            },
        }
    }

    // Verify each shard gets at least 10% of keys (rough uniformity)
    // With 4 shards and 10000 keys, expected ~2500 per shard.
    // 10% = 1000, which is very conservative.
    for (0..4) |s| {
        try testing.expect(per_shard[s] >= 1000);
    }

    // Verify total adds up
    var total: usize = 0;
    for (per_shard) |count| {
        total += count;
    }
    try testing.expectEqual(@as(usize, 10000), total);

    // Verify the ownership invariant: slot % shard_count == shard_id
    for (0..100) |i| {
        const key = std.fmt.bufPrint(&key_buf, "verify-key-{d}", .{i}) catch unreachable;
        const target = router.routeKey(key);
        switch (target) {
            .local => try testing.expectEqual(@as(u32, 0), src.node.router.keySlot(key) % shard_count),
            .shard => |t| try testing.expectEqual(@as(u32, t.shard_id), src.node.router.keySlot(key) % shard_count),
        }
    }
}

test "integration: router same key always routes same" {
    const router = Router.init(4, 0);

    const test_key = "consistent-routing-test-key";

    // Route the same key 100 times
    const first_shard = switch (router.routeKey(test_key)) {
        .local => @as(u16, 0), // local means shard 0 (our local shard)
        .shard => |t| t.shard_id,
    };
    for (1..100) |_| {
        const shard = switch (router.routeKey(test_key)) {
            .local => @as(u16, 0),
            .shard => |t| t.shard_id,
        };
        try testing.expectEqual(first_shard, shard);
    }

    // Also verify multiple different keys get consistent results
    var key_buf: [64]u8 = undefined;
    for (0..50) |i| {
        const key = std.fmt.bufPrint(&key_buf, "stability-key-{d}", .{i}) catch unreachable;
        try testing.expectEqual(router.routeKey(key), router.routeKey(key));
    }
}
