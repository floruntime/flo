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
//! Each domain constructs its hash input differently:
//! - KV: `namespace \0 routing_key`
//! - Stream metadata: `namespace \0 stream_name`
//! - Stream data: `namespace : stream : partition_bytes`
//! - Queue/Actions/Workflow: `namespace \0 entity_name`
//! - Namespace/Cluster ops: always Shard 0
//!
//! ## Ported from
//!
//! `src/node/dispatch/routing.zig` — same Wyhash seed, same hash tag
//! extraction, same two-level mapping.

const std = @import("std");
const PartitionTable = @import("../cluster/partition_table.zig").PartitionTable;
const NodeId = @import("../raft/node.zig").NodeId;

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

/// Default multiplier: partitions = shard_count * MULTIPLIER
const DEFAULT_PARTITION_MULTIPLIER: u32 = 32;

/// Minimum partition count regardless of shard count
const MIN_PARTITION_COUNT: u32 = 4096;

/// Wyhash seed — must never change (wire compat)
const HASH_SEED: u64 = 0;

/// Separator for domain-specific hash inputs (NUL byte, not ':')
const DOMAIN_SEP: u8 = 0x00;

// ═══════════════════════════════════════════════════════════════════════════════
// RouteTarget
// ═══════════════════════════════════════════════════════════════════════════════

/// Result of a routing decision.
pub const RouteTarget = union(enum) {
    /// Partition lives on this shard — handle locally.
    local: LocalTarget,
    /// Partition lives on a different shard on the same node — forward via inbox.
    shard: ShardTarget,
    /// Partition lives on a different node — forward via cluster forwarder.
    remote: RemoteTarget,

    pub const LocalTarget = struct {
        partition_id: u32,
    };

    pub const ShardTarget = struct {
        partition_id: u32,
        shard_id: u16,
    };

    pub const RemoteTarget = struct {
        partition_id: u32,
        node_id: NodeId,
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

    /// Cluster-aware route: hash → partition → check partition table → local/shard/remote.
    /// Falls back to single-node routing when the partition table has no assignment.
    pub fn routeCluster(self: Router, hash: u64, namespace_hash: u32, table: *const PartitionTable) RouteTarget {
        const partition_id = self.hashToPartition(hash);

        // Consult the partition table for cross-node ownership
        if (table.lookup(namespace_hash, @intCast(@as(u32, partition_id) & 0xFFFF))) |result| {
            if (!result.is_local) {
                return .{ .remote = .{
                    .partition_id = partition_id,
                    .node_id = result.leader,
                } };
            }
        }
        // Local to this node — resolve which shard
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

    /// Route a namespaced key (with hash tag extraction).
    pub fn routeNamespacedKey(self: Router, namespace: []const u8, key: []const u8) RouteTarget {
        return self.route(hashKeyWithNamespace(namespace, key));
    }

    /// Key → partition (convenience).
    pub fn keyToPartition(self: Router, key: []const u8) u32 {
        return self.hashToPartition(hashKey(key));
    }

    /// Namespaced key → partition.
    pub fn keyToPartitionNs(self: Router, namespace: []const u8, key: []const u8) u32 {
        return self.hashToPartition(hashKeyWithNamespace(namespace, key));
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

/// Hash a namespaced key: `namespace \0 routing_key`.
pub fn hashKeyWithNamespace(namespace: []const u8, key: []const u8) u64 {
    const routing_key = extractRoutingKey(key);
    var h = std.hash.Wyhash.init(HASH_SEED);
    h.update(namespace);
    h.update(&[_]u8{DOMAIN_SEP});
    h.update(routing_key);
    return h.final();
}

/// Hash for an explicit key (no hash tag extraction): `namespace \0 key`.
pub fn hashExplicitKey(namespace: []const u8, key: []const u8) u64 {
    var h = std.hash.Wyhash.init(HASH_SEED);
    h.update(namespace);
    h.update(&[_]u8{DOMAIN_SEP});
    h.update(key);
    return h.final();
}

/// Hash for stream data partitions: `namespace : stream : partition_bytes`.
pub fn hashStreamPartition(namespace: []const u8, stream: []const u8, partition_id: u32) u64 {
    var h = std.hash.Wyhash.init(HASH_SEED);
    h.update(namespace);
    h.update(":");
    h.update(stream);
    h.update(":");
    h.update(std.mem.asBytes(&partition_id));
    return h.final();
}

/// Compute a compact 32-bit namespace hash for storage in UAL command entries.
/// Returns 0 for empty namespace (backward-compatible: existing entries have 0).
pub fn namespaceHash(namespace: []const u8) u32 {
    if (namespace.len == 0) return 0;
    return @truncate(std.hash.Wyhash.hash(HASH_SEED, namespace));
}

/// Hash a resource name (stream, queue, measurement) incorporating the namespace
/// hash as the Wyhash seed. This allows both live lookups (with full namespace)
/// and recovery from persisted entries (with only the stored namespace_hash: u32).
///
/// Backward-compatible: when namespace_hash=0, produces the same result as
/// `Wyhash.hash(0, name)` which is what pre-isolation code used.
pub fn nameHash(namespace_hash: u32, name: []const u8) u64 {
    return std.hash.Wyhash.hash(@as(u64, namespace_hash), name);
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
            .remote => unreachable, // single-node route never returns remote
        }
    }
    // With 4 shards, ~25% should be local
    try std.testing.expect(local_count > 0);
    try std.testing.expect(remote_count > 0);
}

test "Router: namespaced routing" {
    const router = Router.init(4096, 4, 0);

    // Same key, different namespace → different partition
    const p1 = router.keyToPartitionNs("ns-a", "mykey");
    const p2 = router.keyToPartitionNs("ns-b", "mykey");
    try std.testing.expect(p1 != p2);

    // Same namespace + same key → same partition
    const p3 = router.keyToPartitionNs("ns-a", "mykey");
    try std.testing.expectEqual(p1, p3);
}

test "Router: effective partition count" {
    // Explicit config preserved
    try std.testing.expectEqual(@as(u32, 8192), effectivePartitionCount(8192, 4));

    // Auto-derive from shard count
    try std.testing.expectEqual(@as(u32, 4096), effectivePartitionCount(0, 4)); // 4*32=128 < 4096
    try std.testing.expectEqual(@as(u32, 4096), effectivePartitionCount(0, 128)); // 128*32=4096
    try std.testing.expectEqual(@as(u32, 8192), effectivePartitionCount(0, 256)); // 256*32=8192 > 4096
}

test "Router: stream partition hashing" {
    // Stream data routing is deterministic
    const h1 = hashStreamPartition("prod", "events", 0);
    const h2 = hashStreamPartition("prod", "events", 0);
    try std.testing.expectEqual(h1, h2);

    // Different partition IDs → (likely) different hashes
    const h3 = hashStreamPartition("prod", "events", 1);
    try std.testing.expect(h1 != h3);
}

test "Router: namespaceHash and nameHash" {
    // Default namespace always produces a non-zero hash
    const ns_default = namespaceHash("default");
    try std.testing.expect(ns_default != 0);

    // Non-empty namespace → non-zero
    const ns_a = namespaceHash("prod");
    try std.testing.expect(ns_a != 0);

    // Different namespaces → different hashes
    const ns_b = namespaceHash("staging");
    try std.testing.expect(ns_a != ns_b);

    // nameHash with namespace_hash=0 matches Wyhash.hash(0, name) — backward compat
    const legacy_hash = std.hash.Wyhash.hash(0, "events");
    const new_hash = nameHash(0, "events");
    try std.testing.expectEqual(legacy_hash, new_hash);

    // nameHash with non-zero namespace_hash produces different result — isolation
    const ns_hash = nameHash(ns_a, "events");
    try std.testing.expect(ns_hash != legacy_hash);

    // Same namespace + same name → same hash (deterministic)
    const ns_hash2 = nameHash(ns_a, "events");
    try std.testing.expectEqual(ns_hash, ns_hash2);

    // Different namespace + same name → different hash (isolated)
    const ns_hash3 = nameHash(ns_b, "events");
    try std.testing.expect(ns_hash != ns_hash3);
}

test "Router: routeCluster local fallback" {
    // When partition table has no assignment, routeCluster falls back to single-node routing
    const router = Router.init(4096, 4, 0);
    var table = PartitionTable.init(std.testing.allocator, 1); // local node = 1
    defer table.deinit();

    const target = router.routeCluster(hashKey("mykey"), namespaceHash("default"), &table);
    // Should resolve as local or shard (never remote without assignments)
    switch (target) {
        .local => {},
        .shard => {},
        .remote => return error.TestUnexpectedResult,
    }
}

test "Router: routeCluster remote target" {
    const router = Router.init(4096, 4, 0);
    const ns_hash = namespaceHash("default");

    // This node is node 1
    var table = PartitionTable.init(std.testing.allocator, 1);
    defer table.deinit();

    // Find a partition that the router maps to and assign it to node 2 (remote)
    const hash = hashKey("remote-test-key");
    const partition_id = router.hashToPartition(hash);
    const partition_id_u16: u16 = @intCast(partition_id & 0xFFFF);

    try table.assign(ns_hash, partition_id_u16, 2, &.{});

    const target = router.routeCluster(hash, ns_hash, &table);
    switch (target) {
        .remote => |r| {
            try std.testing.expectEqual(@as(u32, partition_id), r.partition_id);
            try std.testing.expectEqual(@as(NodeId, 2), r.node_id);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "Router: routeCluster local with assignment" {
    const router = Router.init(4096, 4, 0);
    const ns_hash = namespaceHash("default");

    // This node is node 1
    var table = PartitionTable.init(std.testing.allocator, 1);
    defer table.deinit();

    // Assign a partition to node 1 (local)
    const hash = hashKey("local-test-key");
    const partition_id = router.hashToPartition(hash);
    const partition_id_u16: u16 = @intCast(partition_id & 0xFFFF);

    try table.assign(ns_hash, partition_id_u16, 1, &.{});

    const target = router.routeCluster(hash, ns_hash, &table);
    // Should be local or shard (same node), never remote
    switch (target) {
        .local => {},
        .shard => {},
        .remote => return error.TestUnexpectedResult,
    }
}
