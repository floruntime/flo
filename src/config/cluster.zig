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
const stdx = @import("stdx");
const log = stdx.log;

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
    /// Port for Raft RPC communication (inter-node)
    /// 0 = derive from listen_port + 500 (see RuntimeConfig)
    raft_port: u16 = 0,

    /// Seed nodes for cluster discovery (host:port format)
    /// Example: ["192.168.1.10:9500", "192.168.1.11:9500"]
    seeds: []const []const u8 = &.{},

    /// Shared secret every member proves it holds before it is a peer. The
    /// raft port moves terms, membership and log contents, so it is never
    /// open: required whenever the peer listener starts, and only in the
    /// form `flo server secret` prints (`secretWellFormed`).
    secret: ?[]const u8 = null,
    /// A file holding the secret instead, readable by its owner alone.
    secret_file: ?[]const u8 = null,

    /// A leader unheard for this long is replaced; election timeouts are
    /// [½, 1] × this and heartbeats a sixth of it.
    failover_timeout_ms: u32 = 1500,

    /// Check if no seeds are configured (used for INITIAL setup decisions).
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

    /// Format a node ID for display as "flo-XXXXXX".
    pub fn formatNodeId(self: ClusterConfig, buf: *[11]u8, node_id: u32) []const u8 {
        _ = self;
        return generateNodeName(buf, node_id);
    }

    /// Get effective node ID (auto-generate if not set)
    pub fn getEffectiveNodeId(self: ClusterConfig, hostname: []const u8) u32 {
        if (self.node_id != 0) return self.node_id;
        return generateNodeId(hostname, self.raft_port);
    }
};

pub const MIN_FAILOVER_TIMEOUT_MS: i64 = 100;

/// What `flo server secret` prints: this prefix, then 32 random bytes in
/// lowercase hex.
pub const SECRET_PREFIX = "flo-secret-";
pub const SECRET_HEX_LEN: usize = 64;

/// Whether `s` has the form `flo server secret` prints. A node cannot tell
/// how a secret was made, so this checks the form: a value typed by hand,
/// or a placeholder, is refused rather than trusted to be strong.
pub fn secretWellFormed(s: []const u8) bool {
    if (s.len != SECRET_PREFIX.len + SECRET_HEX_LEN) return false;
    if (!std.mem.startsWith(u8, s, SECRET_PREFIX)) return false;
    for (s[SECRET_PREFIX.len..]) |c| {
        if (!((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'))) return false;
    }
    return true;
}

/// A new secret, as `flo server secret` prints it.
pub fn newSecret(out: *[SECRET_PREFIX.len + SECRET_HEX_LEN]u8) !void {
    var bytes: [SECRET_HEX_LEN / 2]u8 = undefined;
    try stdx.io.instance().randomSecure(&bytes);
    defer std.crypto.secureZero(u8, &bytes);
    @memcpy(out[0..SECRET_PREFIX.len], SECRET_PREFIX);
    _ = std.fmt.bufPrint(out[SECRET_PREFIX.len..], "{x}", .{&bytes}) catch unreachable;
}

/// Read the secret from `path`: a file only its owner can read or write,
/// holding the secret and at most surrounding whitespace. The mode is
/// checked on the file that is read, not on the path before it is opened.
pub fn readSecretFile(allocator: Allocator, path: []const u8) ![]u8 {
    const file = stdx.fs.openFile(path, .{}) catch |err| {
        log.err("[cluster] secret_file {s} cannot be opened: {s}", .{ path, @errorName(err) });
        return error.InvalidSetting;
    };
    defer stdx.fs.closeFile(file);
    const st = stdx.fs.statHandle(file) catch |err| {
        log.err("[cluster] secret_file {s} cannot be read: {s}", .{ path, @errorName(err) });
        return error.InvalidSetting;
    };
    const mode: u32 = @intCast(@intFromEnum(st.permissions));
    if (mode & 0o077 != 0) {
        log.err("[cluster] secret_file {s} can be read or written by others (mode {o}); chmod 600 it", .{ path, mode & 0o777 });
        return error.InvalidSetting;
    }
    var buf: [256]u8 = undefined;
    defer std.crypto.secureZero(u8, &buf);
    const got = stdx.fs.readAll(file, &buf) catch |err| {
        log.err("[cluster] secret_file {s} cannot be read: {s}", .{ path, @errorName(err) });
        return error.InvalidSetting;
    };
    return allocator.dupe(u8, std.mem.trim(u8, buf[0..got], " \t\r\n"));
}

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
        config.node_id = std.math.cast(u32, n) orelse {
            log.err("[cluster] node_id = {d} is not an id; use 1 to {d}, or 0 for an id derived at first start", .{ n, std.math.maxInt(u32) });
            return error.InvalidSetting;
        };
    }
    if (table.getInt("raft_port")) |p| {
        config.raft_port = std.math.cast(u16, p) orelse {
            log.err("[cluster] raft_port = {d} is not a port; use 1 to 65535, or 0 for listen_port + 500", .{p});
            return error.InvalidSetting;
        };
    }
    if (table.getString("secret")) |s| {
        const owned = try allocator.dupe(u8, s);
        try owned_strings.append(allocator, owned);
        config.secret = owned;
    }
    if (table.getString("secret_file")) |s| {
        const owned = try allocator.dupe(u8, s);
        try owned_strings.append(allocator, owned);
        config.secret_file = owned;
    }
    if (table.getInt("failover_timeout_ms")) |t| {
        if (t > std.math.maxInt(u32)) {
            log.err("[cluster] failover_timeout_ms = {d} is above the maximum {d}", .{ t, std.math.maxInt(u32) });
            return error.InvalidSetting;
        }
        if (t < MIN_FAILOVER_TIMEOUT_MS) {
            log.err("[cluster] failover_timeout_ms = {d} is too low: below {d} ms a busy disk's fsync looks like a dead leader and the group elects on every stall", .{ t, MIN_FAILOVER_TIMEOUT_MS });
            return error.InvalidSetting;
        }
        config.failover_timeout_ms = @intCast(t);
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

    const config1 = ClusterConfig{};
    const name1 = config1.formatNodeId(&buf, 0x12345678);
    try std.testing.expectEqualStrings("flo-345678", name1);
}

test "cluster config refuses a failover below the floor" {
    const toml = @import("toml.zig");
    const allocator = std.testing.allocator;
    var owned: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (owned.items) |o| allocator.free(o);
        owned.deinit(allocator);
    }
    var low = try toml.parse(allocator, "failover_timeout_ms = 50\n");
    defer low.deinit();
    try std.testing.expectError(error.InvalidSetting, parseClusterConfig(allocator, &low, &owned));
    var high = try toml.parse(allocator, "failover_timeout_ms = 5000000000\n");
    defer high.deinit();
    try std.testing.expectError(error.InvalidSetting, parseClusterConfig(allocator, &high, &owned));
    var port = try toml.parse(allocator, "raft_port = 70000\n");
    defer port.deinit();
    try std.testing.expectError(error.InvalidSetting, parseClusterConfig(allocator, &port, &owned));
    var fine = try toml.parse(allocator, "failover_timeout_ms = 3000\n");
    defer fine.deinit();
    try std.testing.expectEqual(@as(u32, 3000), (try parseClusterConfig(allocator, &fine, &owned)).failover_timeout_ms);
}

const testing = std.testing;

test "cluster: only the form `flo server secret` prints is a secret, and it prints that form" {
    var out: [SECRET_PREFIX.len + SECRET_HEX_LEN]u8 = undefined;
    try newSecret(&out);
    try testing.expect(secretWellFormed(&out));
    var again: [SECRET_PREFIX.len + SECRET_HEX_LEN]u8 = undefined;
    try newSecret(&again);
    try testing.expect(!std.mem.eql(u8, &out, &again));
    try testing.expect(!secretWellFormed("s3cret"));
    try testing.expect(!secretWellFormed(""));
    try testing.expect(!secretWellFormed("flo-secret-" ++ "0" ** 63));
    try testing.expect(!secretWellFormed("flo-secret-" ++ "0" ** 65));
    try testing.expect(!secretWellFormed("flo-secret-" ++ "A" ** 64));
    try testing.expect(!secretWellFormed("xyz-secret-" ++ "0" ** 64));
    try testing.expect(secretWellFormed("flo-secret-" ++ "0123456789abcdef" ** 4));
}

test "cluster: a secret file is read only when its owner alone can read it" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const secret = "flo-secret-" ++ "ab" ** 32;
    const dir_path = try stdx.fs.dirRealpathAlloc(tmp.dir, testing.allocator, ".");
    defer testing.allocator.free(dir_path);
    const path = try std.fs.path.join(testing.allocator, &.{ dir_path, "s" });
    {
        const f = try stdx.fs.createFileAbsolute(path, .{});
        defer stdx.fs.closeFile(f);
        try stdx.fs.writeAll(f, secret ++ "\n");
    }
    defer testing.allocator.free(path);
    const path_z = try testing.allocator.dupeZ(u8, path);
    defer testing.allocator.free(path_z);

    try testing.expectEqual(@as(c_int, 0), std.c.chmod(path_z, 0o644));
    try testing.expectError(error.InvalidSetting, readSecretFile(testing.allocator, path));
    try testing.expectEqual(@as(c_int, 0), std.c.chmod(path_z, 0o600));
    const got = try readSecretFile(testing.allocator, path);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(secret, got);
}
