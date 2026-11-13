//! Router — `hash(key) → slot → shard`
//!
//! Two-level routing used by all subsystems:
//!
//!   1. Wyhash(routing_key) mod SLOTS → slot
//!   2. slot mod shard_count → shard
//!
//! The slot count is fixed for every node and cluster, so a key's slot never
//! changes; what a slot maps to can. Durable state names a slot, never a
//! shard.
//!
//! ## Hash Tags
//!
//! Redis-style `{tag}` support: `"user:{alice}:profile"` and
//! `"user:{alice}:orders"` hash to the same slot.
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

/// The number of routing slots. Fixed: a key's slot is part of what durable
/// state records (a run id carries its definition's slot), so changing it
/// would re-home every key.
pub const SLOTS: u32 = 4096;

/// Wyhash seed — must never change (wire compat)
const HASH_SEED: u64 = 0;

// ═══════════════════════════════════════════════════════════════════════════════
// RouteTarget
// ═══════════════════════════════════════════════════════════════════════════════

/// Result of a routing decision.
pub const RouteTarget = union(enum) {
    /// Slot lives on this shard — handle locally.
    local: LocalTarget,
    /// Slot lives on a different shard on the same node — forward via inbox.
    shard: ShardTarget,

    pub const LocalTarget = struct {
        slot: u32,
    };

    pub const ShardTarget = struct {
        slot: u32,
        shard_id: u16,
    };
};

// ═══════════════════════════════════════════════════════════════════════════════
// Router
// ═══════════════════════════════════════════════════════════════════════════════

pub const Router = struct {
    shard_count: u16,
    local_shard_id: u16,

    /// Create a router for the given shard.
    pub fn init(shard_count: u16, local_shard_id: u16) Router {
        std.debug.assert(shard_count >= 1 and shard_count <= SLOTS);
        return .{
            .shard_count = shard_count,
            .local_shard_id = local_shard_id,
        };
    }

    // ─── Core routing ────────────────────────────────────────────────────

    /// Map a slot to the shard that owns it.
    pub fn slotToShard(self: Router, s: u32) u16 {
        return @intCast(s % self.shard_count);
    }

    /// Full route: hash → slot → local-or-forward decision (single-node).
    pub fn route(self: Router, hash: u64) RouteTarget {
        const s = slot(hash);
        const shard_id = self.slotToShard(s);
        if (shard_id == self.local_shard_id) {
            return .{ .local = .{ .slot = s } };
        }
        return .{ .shard = .{ .slot = s, .shard_id = shard_id } };
    }

    /// The shard owning a hash's slot.
    pub fn hashToShard(self: Router, hash: u64) u16 {
        return self.slotToShard(slot(hash));
    }

    // ─── Convenience: key-based routing ──────────────────────────────────

    /// Route a raw key (with hash tag extraction).
    pub fn routeKey(self: Router, key: []const u8) RouteTarget {
        return self.route(hashKey(key));
    }
};

/// Map a 64-bit hash to its slot.
pub fn slot(hash: u64) u32 {
    return @intCast(hash % SLOTS);
}

/// Key → slot (with hash tag extraction).
pub fn keySlot(key: []const u8) u32 {
    return slot(hashKey(key));
}

/// Namespaced key → slot.
pub fn nsKeySlot(ns: NamespaceId, key: []const u8) u32 {
    return slot(routeNsKey(ns, key));
}

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

/// The key a named thing (a stream, queue, measurement, group) is kept
/// under in its namespace: the id, then the name's hash, both big-endian,
/// so one namespace's keys sort together and a 64-bit collision can only
/// be between names in one namespace.
pub fn nsNameKey(ns: NamespaceId, name: []const u8) [12]u8 {
    var k: [12]u8 = undefined;
    k[0..4].* = ns.bytes();
    std.mem.writeInt(u64, k[4..12], std.hash.Wyhash.hash(HASH_SEED, name), .big);
    return k;
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

test "Router: a key's slot is deterministic and in range" {
    try std.testing.expectEqual(keySlot("test-key"), keySlot("test-key"));
    for (0..1000) |i| {
        var buf: [16]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key-{d}", .{i}) catch unreachable;
        try std.testing.expect(keySlot(key) < SLOTS);
    }
}

test "Router: hash tag co-location" {
    // Keys with same hash tag must land on same slot
    const p1 = keySlot("user:{alice}:profile");
    try std.testing.expectEqual(p1, keySlot("user:{alice}:orders"));
    try std.testing.expectEqual(p1, keySlot("user:{alice}:sessions"));
    try std.testing.expect(p1 != keySlot("user:{bob}:profile"));
}

test "Router: ownership invariant" {
    // slot % shard_count == shard_id
    const shard_count: u16 = 8;
    const router = Router.init(shard_count, 0);

    const keys = [_][]const u8{ "key1", "key2", "key3", "test", "hello", "world" };
    for (keys) |key| {
        const s = keySlot(key);
        try std.testing.expectEqual(router.slotToShard(s), @as(u16, @intCast(s % shard_count)));
        try std.testing.expectEqual(router.slotToShard(s), router.hashToShard(hashKey(key)));
    }
}

test "Router: up to 128 shards, a key's owner is what it was before slots were fixed" {
    // Before, a router took max(4096, shards * 32) partitions, which is 4096
    // up to 128 shards; above that the owner of a key moves.
    var prng = std.Random.DefaultPrng.init(0x5107);
    const rand = prng.random();
    for ([_]u16{ 1, 2, 3, 4, 7, 16, 64, 128 }) |n| {
        const router = Router.init(n, 0);
        const before = @max(@as(u64, 4096), @as(u64, n) * 32);
        for (0..2000) |_| {
            const h = rand.int(u64);
            try std.testing.expectEqual(@as(u16, @intCast((h % before) % n)), router.hashToShard(h));
        }
    }
}

test "Router: route local vs shard" {
    const router = Router.init(4, 2); // local shard = 2

    var local_count: usize = 0;
    var remote_count: usize = 0;
    for (0..100) |i| {
        var buf: [16]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key-{d}", .{i}) catch unreachable;
        const target = router.routeKey(key);
        switch (target) {
            .local => |t| {
                try std.testing.expectEqual(@as(u16, 2), @as(u16, @intCast(t.slot % 4)));
                local_count += 1;
            },
            .shard => |t| {
                try std.testing.expect(t.shard_id != 2);
                try std.testing.expectEqual(t.shard_id, @as(u16, @intCast(t.slot % 4)));
                remote_count += 1;
            },
        }
    }
    // With 4 shards, ~25% should be local
    try std.testing.expect(local_count > 0);
    try std.testing.expect(remote_count > 0);
}

test "Router: namespaced routing" {
    // Same key, different namespace → different slot
    const p1 = nsKeySlot(.of(16), "mykey");
    try std.testing.expect(p1 != nsKeySlot(.of(17), "mykey"));
    try std.testing.expectEqual(p1, nsKeySlot(.of(16), "mykey"));

    // A hash tag routes within its namespace, not across them.
    try std.testing.expectEqual(routeNsKey(.of(16), "user:{alice}:a"), routeNsKey(.of(16), "user:{alice}:b"));
    try std.testing.expect(routeNsKey(.of(16), "{alice}") != routeNsKey(.of(17), "{alice}"));
}

test "Router: nameHash is the namespace's own" {
    // Same namespace + same name → same hash (deterministic)
    try std.testing.expectEqual(nameHash(.of(16), "events"), nameHash(.of(16), "events"));
    // Different namespace + same name → different hash (isolated)
    try std.testing.expect(nameHash(.of(16), "events") != nameHash(.of(17), "events"));
    try std.testing.expect(nameHash(.default, "events") != nameHash(.of(16), "events"));
}

test "Router: nsNameKey is the id then the name's hash, big-endian" {
    const k = nsNameKey(.of(0x01020304), "orders");
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3, 4 }, k[0..4]);
    try std.testing.expectEqual(std.hash.Wyhash.hash(HASH_SEED, "orders"), std.mem.readInt(u64, k[4..12], .big));
    // One namespace's keys share a prefix and sort after a lower id's.
    try std.testing.expectEqualSlices(u8, k[0..4], nsNameKey(.of(0x01020304), "other")[0..4]);
    try std.testing.expect(std.mem.order(u8, &nsNameKey(.of(16), "zz"), &nsNameKey(.of(17), "aa")) == .lt);
}
