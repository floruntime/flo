//! Cluster Configuration
//!
//! Configuration for Flo's distributed clustering mode.
//! Parsed from [cluster] section in flo.toml.
//!
//! Design: Flo always runs in "cluster mode" - even single nodes.
//! - No seeds = single-node cluster (immediate leader election)
//! - With seeds = multi-node cluster (join existing or bootstrap)
//! This simplifies the codebase and enables seamless scaling.

const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");
const log = @import("stdx").log;

/// Cluster configuration loaded from [cluster] section in flo.toml
pub const ClusterConfig = struct {
    /// Start the peer-facing Raft listener even with no seeds configured.
    ///
    /// The listener exists to accept connections from other nodes, so a node
    /// with no seeds and no replication has nothing to accept and does not
    /// bind it. Set this to bring the listener up anyway — e.g. a seed node
    /// that peers will join before it knows about them.
    enabled: bool = false,

    /// This node's unique ID within the cluster. Read on first boot and stored
    /// in the data dir; a later boot with a different value is refused.
    /// If 0, auto-generated from hostname hash + port
    node_id: u32 = 0,

    /// Human-readable node name for display
    /// Format: "flo-XXXX" (auto-generated) or explicit like "node-1", "east-1"
    /// If null, auto-generated from hostname:port hash
    node_name: ?[]const u8 = null,

    /// Port for Raft RPC communication (inter-node)
    /// 0 = derive from listen_port + 500 (see RuntimeConfig)
    raft_port: u16 = 0,

    /// Seed nodes for cluster discovery (host:port format)
    /// Example: ["192.168.1.10:9500", "192.168.1.11:9500"]
    seeds: []const []const u8 = &.{},

    /// Shared secret every member proves it holds before it is a peer. The
    /// raft port moves terms, membership and log contents, so it is never
    /// open: required whenever the peer listener starts.
    secret: ?[]const u8 = null,

    /// A leader unheard for this long is replaced; election timeouts are
    /// [½, 1] × this and heartbeats a sixth of it.
    failover_timeout_ms: u32 = 1500,

    /// Check if no seeds are configured (used for INITIAL setup decisions).
    /// For runtime checks, use ClusterCoordinator.isSingleNodeCluster() instead.
    /// Single-node = no seeds (or only self in seeds)
    pub fn hasNoSeeds(self: ClusterConfig) bool {
        return self.seeds.len == 0;
    }

    /// Generate a deterministic node ID from hostname and port.
    /// Used when node_id is not explicitly configured.
    /// Algorithm: FNV-1a hash of "hostname:port" truncated to u32
    pub fn generateNodeId(hostname: []const u8, port: u16) u32 {
        var hasher = std.hash.Fnv1a_32.init();
        hasher.update(hostname);
        hasher.update(":");
        var port_buf: [5]u8 = undefined;
        const port_str = std.fmt.bufPrint(&port_buf, "{d}", .{port}) catch "0";
        hasher.update(port_str);
        const hash = hasher.final();
        // Ensure non-zero (0 is reserved for "auto")
        return if (hash == 0) 1 else hash;
    }

    /// Generate a short human-readable node name from node_id (for display).
    /// Like git, we store the full 32-bit hash but display only 6 hex chars.
    /// Format: "flo-XXXXXX" where XXXXXX is lower 24 bits in hex.
    /// Example: node_id 0x0e116ada -> "flo-116ada"
    ///
    /// This provides ~16.7M unique display names while keeping full precision
    /// internally for routing and identification.
    pub fn generateNodeName(buf: *[11]u8, node_id: u32) []const u8 {
        // Display lower 24 bits as 6 hex chars (like git short hashes)
        const short_hash: u24 = @truncate(node_id);
        _ = std.fmt.bufPrint(buf, "flo-{x:0>6}", .{short_hash}) catch "flo-000000";
        return buf[0..10];
    }

    /// Format a node ID for display.
    /// If explicit node_name is set, returns that.
    /// Otherwise generates "flo-XXXXXX" format.
    pub fn formatNodeId(self: ClusterConfig, buf: *[11]u8, node_id: u32) []const u8 {
        if (self.node_name) |name| {
            return name;
        }
        return generateNodeName(buf, node_id);
    }

    /// Get effective node ID (auto-generate if not set)
    pub fn getEffectiveNodeId(self: ClusterConfig, hostname: []const u8) u32 {
        if (self.node_id != 0) return self.node_id;
        return generateNodeId(hostname, self.raft_port);
    }
};

pub const MIN_FAILOVER_TIMEOUT_MS: i64 = 100;

/// Every key `[cluster]` reads. A key not on the list is refused at start,
/// not ignored: a line that parses clean and changes nothing is the bug
/// an operator finds at 2am.
const known_keys = [_][]const u8{ "enabled", "secret", "node_id", "node_name", "raft_port", "seeds", "failover_timeout_ms" };

/// Parse cluster configuration from TOML table
pub fn parseClusterConfig(
    allocator: Allocator,
    table: anytype, // toml.Table
    owned_strings: *std.ArrayListUnmanaged([]const u8),
) !ClusterConfig {
    var config = ClusterConfig{};

    if (table.getBool("enabled")) |e| {
        config.enabled = e;
    }

    if (table.getInt("node_id")) |n| {
        config.node_id = @intCast(n);
    }
    if (table.getString("node_name")) |name| {
        const owned = try allocator.dupe(u8, name);
        try owned_strings.append(allocator, owned);
        config.node_name = owned;
    }
    if (table.getInt("raft_port")) |p| {
        config.raft_port = @intCast(p);
    }
    if (table.getString("secret")) |s| {
        const owned = try allocator.dupe(u8, s);
        try owned_strings.append(allocator, owned);
        config.secret = owned;
    }
    if (table.getInt("failover_timeout_ms")) |t| {
        if (t > std.math.maxInt(u32)) {
            log.err("[cluster] failover_timeout_ms = {d} is not a duration this node can wait", .{t});
            return error.InvalidSetting;
        }
        if (t < MIN_FAILOVER_TIMEOUT_MS) {
            log.err("[cluster] failover_timeout_ms = {d} is too low: below {d} ms a busy disk's fsync looks like a dead leader and the group elects on every stall", .{ t, MIN_FAILOVER_TIMEOUT_MS });
            return error.InvalidSetting;
        }
        config.failover_timeout_ms = @intCast(t);
    }
    var keys = table.entries.keyIterator();
    while (keys.next()) |key| {
        var known = false;
        for (known_keys) |k| known = known or std.mem.eql(u8, k, key.*);
        if (!known) {
            log.err("[cluster] {s} is not a setting; the keys are enabled, secret, node_id, node_name, raft_port, seeds and failover_timeout_ms. Remove the line.", .{key.*});
            return error.UnknownSetting;
        }
    }

    // Seeds: an array of "host:port", or one comma-separated string.
    if (table.getArray("seeds")) |seeds_array| {
        var seeds_list: std.ArrayList([]const u8) = .empty;
        errdefer seeds_list.deinit(allocator);

        for (seeds_array) |item| {
            if (item.asString()) |seed| {
                const owned = try allocator.dupe(u8, seed);
                try owned_strings.append(allocator, owned);
                try seeds_list.append(allocator, owned);
            }
        }

        config.seeds = try seeds_list.toOwnedSlice(allocator);
    } else if (table.getString("seeds")) |seeds_str| {
        var seeds_list: std.ArrayList([]const u8) = .empty;
        errdefer seeds_list.deinit(allocator);

        var iter = std.mem.splitScalar(u8, seeds_str, ',');
        while (iter.next()) |seed| {
            const trimmed = std.mem.trim(u8, seed, &std.ascii.whitespace);
            if (trimmed.len > 0) {
                const owned = try allocator.dupe(u8, trimmed);
                try owned_strings.append(allocator, owned);
                try seeds_list.append(allocator, owned);
            }
        }

        config.seeds = try seeds_list.toOwnedSlice(allocator);
    }

    return config;
}

test "cluster config defaults" {
    const config = ClusterConfig{};
    try std.testing.expect(config.hasNoSeeds()); // No seeds = single node
    try std.testing.expectEqual(@as(u32, 0), config.node_id);
    try std.testing.expectEqual(@as(u16, 0), config.raft_port); // 0 = derive from listen_port
}

test "cluster config single node detection" {
    var config = ClusterConfig{};
    try std.testing.expect(config.hasNoSeeds());

    // With seeds = multi-node
    config.seeds = &[_][]const u8{"192.168.1.10:9500"};
    try std.testing.expect(!config.hasNoSeeds());
}

test "auto node_id generation" {
    const id1 = ClusterConfig.generateNodeId("node1.local", 9500);
    const id2 = ClusterConfig.generateNodeId("node2.local", 9500);
    const id3 = ClusterConfig.generateNodeId("node1.local", 9501);

    // Different hostnames should produce different IDs
    try std.testing.expect(id1 != id2);
    // Different ports should produce different IDs
    try std.testing.expect(id1 != id3);
    // Same input should produce same output
    try std.testing.expectEqual(id1, ClusterConfig.generateNodeId("node1.local", 9500));
    // Should never be 0
    try std.testing.expect(id1 != 0);
    try std.testing.expect(id2 != 0);
}

test "node name generation" {
    var buf: [11]u8 = undefined;

    // Test basic name generation - lower 24 bits used
    const name1 = ClusterConfig.generateNodeName(&buf, 0x12345678);
    try std.testing.expectEqualStrings("flo-345678", name1);

    // Test with different node IDs - lower 24 bits determine the name
    var buf2: [11]u8 = undefined;
    const name2 = ClusterConfig.generateNodeName(&buf2, 0x00ABCDEF);
    try std.testing.expectEqualStrings("flo-abcdef", name2);

    // Test zero padding
    var buf3: [11]u8 = undefined;
    const name3 = ClusterConfig.generateNodeName(&buf3, 0x00000001);
    try std.testing.expectEqualStrings("flo-000001", name3);

    // Test max value (lower 24 bits = 0xFFFFFF)
    var buf4: [11]u8 = undefined;
    const name4 = ClusterConfig.generateNodeName(&buf4, 0xFFFFFFFF);
    try std.testing.expectEqualStrings("flo-ffffff", name4);
}

test "formatNodeId with explicit name" {
    var buf: [11]u8 = undefined;

    // With no explicit name, should generate flo-XXXXXX
    const config1 = ClusterConfig{};
    const name1 = config1.formatNodeId(&buf, 0x12345678);
    try std.testing.expectEqualStrings("flo-345678", name1);

    // With explicit name, should return that name
    const config2 = ClusterConfig{ .node_name = "east-1" };
    const name2 = config2.formatNodeId(&buf, 0x12345678);
    try std.testing.expectEqualStrings("east-1", name2);
}

test "cluster config refuses a key it does not know and a failover below the floor" {
    const toml = @import("toml.zig");
    const allocator = std.testing.allocator;
    var owned: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (owned.items) |o| allocator.free(o);
        owned.deinit(allocator);
    }
    var stale = try toml.parse(allocator, "election_timeout_min_ms = 150\n");
    defer stale.deinit();
    try std.testing.expectError(error.UnknownSetting, parseClusterConfig(allocator, &stale, &owned));
    var low = try toml.parse(allocator, "failover_timeout_ms = 50\n");
    defer low.deinit();
    try std.testing.expectError(error.InvalidSetting, parseClusterConfig(allocator, &low, &owned));
    var fine = try toml.parse(allocator, "failover_timeout_ms = 3000\n");
    defer fine.deinit();
    try std.testing.expectEqual(@as(u32, 3000), (try parseClusterConfig(allocator, &fine, &owned)).failover_timeout_ms);
}
