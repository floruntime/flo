//! Partition Router — `hash(key) → partition_id → shard_id`
//!
//! Two-level consistent routing used by all subsystems:
//!
//!   1. Wyhash(routing_key) mod partition_count → partition_id
//!   2. partition_id mod shard_count → shard_id
//!
//! This guarantees the ownership invariant:
//!   partition_id % shard_count == shard_id
//!
//! ## Hash Tags
//!
//! Redis-style `{tag}` support: `"user:{alice}:profile"` and
//! `"user:{alice}:orders"` hash to the same partition.
//!
//! ## Domain Routing
//!
//! Every namespaced key routes by `routeNsKey`: the namespace's id, then
//! the key's routing part (a KV key, or a stream's, queue's, action's or
//! workflow's name). Namespace and cluster ops always go to shard 0.

const std = @import("std");
pub const NamespaceId = @import("../namespace/table.zig").Id;

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

/// Default multiplier: partitions = shard_count * MULTIPLIER
const DEFAULT_PARTITION_MULTIPLIER: u32 = 32;

/// Minimum partition count regardless of shard count
const MIN_PARTITION_COUNT: u32 = 4096;

/// Wyhash seed — must never change (wire compat)
const HASH_SEED: u64 = 0;

// ═══════════════════════════════════════════════════════════════════════════════
// RouteTarget
// ═══════════════════════════════════════════════════════════════════════════════

/// Result of a routing decision.
pub const RouteTarget = union(enum) {
    /// Partition lives on this shard — handle locally.
    local: LocalTarget,
    /// Partition lives on a different shard on the same node — forward via inbox.
    shard: ShardTarget,

    pub const LocalTarget = struct {
        partition_id: u32,
    };

    pub const ShardTarget = struct {
        partition_id: u32,
        shard_id: u16,
    };
};

// ═══════════════════════════════════════════════════════════════════════════════
// Router
// ═══════════════════════════════════════════════════════════════════════════════

pub const Router = struct {
    partition_count: u32,
    shard_count: u16,
    local_shard_id: u16,

    /// Create a router for the given shard.
    pub fn init(partition_count: u32, shard_count: u16, local_shard_id: u16) Router {
        const effective = effectivePartitionCount(partition_count, shard_count);
        return .{
            .partition_count = effective,
            .shard_count = shard_count,
            .local_shard_id = local_shard_id,
        };
    }

    // ─── Core routing ────────────────────────────────────────────────────

    /// Map a 64-bit hash to a partition ID.
    pub fn hashToPartition(self: Router, hash: u64) u32 {
        return @intCast(hash % self.partition_count);
    }

    /// Map a partition ID to a shard ID.
    pub fn partitionToShard(self: Router, partition_id: u32) u16 {
        return @intCast(partition_id % self.shard_count);
    }

    /// Full route: hash → partition → local-or-forward decision (single-node).
    pub fn route(self: Router, hash: u64) RouteTarget {
        const partition_id = self.hashToPartition(hash);
        const shard_id = self.partitionToShard(partition_id);
        if (shard_id == self.local_shard_id) {
            return .{ .local = .{ .partition_id = partition_id } };
        }
        return .{ .shard = .{ .partition_id = partition_id, .shard_id = shard_id } };
    }

    // ─── Convenience: key-based routing ──────────────────────────────────

    /// Route a raw key (with hash tag extraction).
    pub fn routeKey(self: Router, key: []const u8) RouteTarget {
        return self.route(hashKey(key));
    }

    /// Key → partition (convenience).
    pub fn keyToPartition(self: Router, key: []const u8) u32 {
        return self.hashToPartition(hashKey(key));
    }

    /// Namespaced key → partition.
    pub fn keyToPartitionNs(self: Router, ns: NamespaceId, key: []const u8) u32 {
        return self.hashToPartition(routeNsKey(ns, key));
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Hash functions (public — used by domain handlers)
// ═══════════════════════════════════════════════════════════════════════════════

/// Hash a raw key with hash tag extraction.
pub fn hashKey(key: []const u8) u64 {
    const routing_key = extractRoutingKey(key);
    return std.hash.Wyhash.hash(HASH_SEED, routing_key);
}

/// Hash a namespaced key: the namespace's id, fixed width, then the key's
/// routing part (with hash tag extraction).
pub fn routeNsKey(ns: NamespaceId, key: []const u8) u64 {
    var h = std.hash.Wyhash.init(HASH_SEED);
    h.update(&ns.bytes());
    h.update(extractRoutingKey(key));
    return h.final();
}

/// Hash a resource name (stream, queue, measurement) in a namespace, seeded
/// with the namespace's id: the same from a request, which the edge
/// resolved, and from a committed entry, which carries the id.
pub fn nameHash(ns: NamespaceId, name: []const u8) u64 {
    return std.hash.Wyhash.hash(@as(u64, ns.int()), name);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Hash tag extraction
// ═══════════════════════════════════════════════════════════════════════════════

/// Extract routing key from a key with optional `{tag}` hash tag.
///
/// Rules (Redis-compatible):
/// - `"user:{alice}:profile"` → `"alice"`
/// - `"user:{alice}:{bob}"` → `"alice"` (first pair only)
/// - `"no-braces"` → `"no-braces"`
/// - `"{}"` → `""` (empty — all co-locate)
/// - `"{unclosed"` → `"{unclosed"` (full key)
pub fn extractRoutingKey(key: []const u8) []const u8 {
    const open_idx = std.mem.indexOfScalar(u8, key, '{') orelse return key;
    const search_start = open_idx + 1;
    if (search_start >= key.len) return key;
    const close_idx = std.mem.indexOfScalarPos(u8, key, search_start, '}') orelse return key;
    return key[open_idx + 1 .. close_idx];
}

// ═══════════════════════════════════════════════════════════════════════════════
// Partition count auto-configuration
// ═══════════════════════════════════════════════════════════════════════════════

/// Compute effective partition count.
/// If `configured == 0`, auto-derive from shard_count.
pub fn effectivePartitionCount(configured: u32, shard_count: u16) u32 {
    if (configured > 0) return configured;
    const derived = @as(u32, shard_count) * DEFAULT_PARTITION_MULTIPLIER;
    return @max(MIN_PARTITION_COUNT, derived);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Router: hash tag extraction" {
    // Basic tag
    try std.testing.expectEqualStrings("alice", extractRoutingKey("user:{alice}:profile"));
    // First pair only
    try std.testing.expectEqualStrings("alice", extractRoutingKey("user:{alice}:{bob}"));
    // No braces
    try std.testing.expectEqualStrings("no-braces", extractRoutingKey("no-braces"));
    // Empty tag
    try std.testing.expectEqualStrings("", extractRoutingKey("{}"));
    // Unclosed
    try std.testing.expectEqualStrings("{unclosed", extractRoutingKey("{unclosed"));
    // Brace at end
    try std.testing.expectEqualStrings("abc{", extractRoutingKey("abc{"));
    // Normal key
    try std.testing.expectEqualStrings("mykey", extractRoutingKey("mykey"));
}

test "Router: deterministic partition assignment" {
    const router = Router.init(0, 4, 0); // auto partition count, 4 shards, shard 0

    // Same key always maps to same partition
    const p1 = router.keyToPartition("test-key");
    const p2 = router.keyToPartition("test-key");
    try std.testing.expectEqual(p1, p2);

    // Different keys may map to different partitions (statistical, but should differ for these)
    const pa = router.keyToPartition("key-aaa");
    const pb = router.keyToPartition("key-bbb");
    // They CAN be equal, but with 4096 partitions it's unlikely
    _ = pa;
    _ = pb;
}

test "Router: hash tag co-location" {
    const router = Router.init(4096, 8, 0);

    // Keys with same hash tag must land on same partition
    const p1 = router.keyToPartition("user:{alice}:profile");
    const p2 = router.keyToPartition("user:{alice}:orders");
    const p3 = router.keyToPartition("user:{alice}:sessions");
    try std.testing.expectEqual(p1, p2);
    try std.testing.expectEqual(p2, p3);

    // Different tags should (likely) differ
    const p4 = router.keyToPartition("user:{bob}:profile");
    // With 4096 partitions, hash collision is very unlikely
    try std.testing.expect(p1 != p4);
}

test "Router: ownership invariant" {
    // partition_id % shard_count == shard_id
    const shard_count: u16 = 8;
    const router = Router.init(4096, shard_count, 0);

    const keys = [_][]const u8{ "key1", "key2", "key3", "test", "hello", "world" };
    for (keys) |key| {
        const partition = router.keyToPartition(key);
        const shard = router.partitionToShard(partition);
        try std.testing.expectEqual(shard, @as(u16, @intCast(partition % shard_count)));
    }
}

test "Router: route local vs shard" {
    const router = Router.init(4096, 4, 2); // local shard = 2

    // Try many keys and verify routing correctness
    var local_count: usize = 0;
    var remote_count: usize = 0;
    for (0..100) |i| {
        var buf: [16]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key-{d}", .{i}) catch unreachable;
        const target = router.routeKey(key);
        switch (target) {
            .local => |t| {
                try std.testing.expectEqual(@as(u16, 2), @as(u16, @intCast(t.partition_id % 4)));
                local_count += 1;
            },
            .shard => |t| {
                try std.testing.expect(t.shard_id != 2);
                try std.testing.expectEqual(t.shard_id, @as(u16, @intCast(t.partition_id % 4)));
                remote_count += 1;
            },
        }
    }
    // With 4 shards, ~25% should be local
    try std.testing.expect(local_count > 0);
    try std.testing.expect(remote_count > 0);
}

test "Router: namespaced routing" {
    const router = Router.init(4096, 4, 0);

    // Same key, different namespace → different partition
    const p1 = router.keyToPartitionNs(.of(16), "mykey");
    const p2 = router.keyToPartitionNs(.of(17), "mykey");
    try std.testing.expect(p1 != p2);

    // Same namespace + same key → same partition
    const p3 = router.keyToPartitionNs(.of(16), "mykey");
    try std.testing.expectEqual(p1, p3);

    // A hash tag routes within its namespace, not across them.
    try std.testing.expectEqual(routeNsKey(.of(16), "user:{alice}:a"), routeNsKey(.of(16), "user:{alice}:b"));
    try std.testing.expect(routeNsKey(.of(16), "{alice}") != routeNsKey(.of(17), "{alice}"));
}

test "Router: effective partition count" {
    // Explicit config preserved
    try std.testing.expectEqual(@as(u32, 8192), effectivePartitionCount(8192, 4));

    // Auto-derive from shard count
    try std.testing.expectEqual(@as(u32, 4096), effectivePartitionCount(0, 4)); // 4*32=128 < 4096
    try std.testing.expectEqual(@as(u32, 4096), effectivePartitionCount(0, 128)); // 128*32=4096
    try std.testing.expectEqual(@as(u32, 8192), effectivePartitionCount(0, 256)); // 256*32=8192 > 4096
}

test "Router: nameHash is the namespace's own" {
    // Same namespace + same name → same hash (deterministic)
    try std.testing.expectEqual(nameHash(.of(16), "events"), nameHash(.of(16), "events"));
    // Different namespace + same name → different hash (isolated)
    try std.testing.expect(nameHash(.of(16), "events") != nameHash(.of(17), "events"));
    try std.testing.expect(nameHash(.default, "events") != nameHash(.of(16), "events"));
}
