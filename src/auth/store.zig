//! API Key Store — In-memory key store backed by `__internal` KV partition
//!
//! Holds all API keys in a hash map keyed by key-ID. The store is the
//! single source of truth for key validation, lookup, and enumeration.
//!
//! In production, the store is persisted to the `__internal` KV partition
//! which replicates automatically via Raft in a cluster. This module
//! provides the in-memory layer; persistence is handled by the caller
//! (runtime/bootstrap) via serialize/deserialize on the ApiKey struct.

const std = @import("std");
const Allocator = std.mem.Allocator;
const keys = @import("keys.zig");
const log = @import("stdx").log;

pub const KeyStore = struct {
    allocator: Allocator,

    /// API keys indexed by key-ID string
    key_map: std.StringHashMapUnmanaged(keys.ApiKey),

    /// Internal HS256 signing secret for session JWTs (set at bootstrap)
    signing_secret: ?[32]u8,

    /// Whether bootstrap has been performed
    bootstrapped: bool,

    pub fn init(allocator: Allocator) KeyStore {
        return .{
            .allocator = allocator,
            .key_map = .{},
            .signing_secret = null,
            .bootstrapped = false,
        };
    }

    pub fn deinit(self: *KeyStore) void {
        var it = self.key_map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.key_map.deinit(self.allocator);
    }

    /// Bootstrap: create the root admin key and signing secret.
    /// Returns the plaintext root key. Fails if already bootstrapped.
    pub fn bootstrap(self: *KeyStore) ![]const u8 {
        if (self.bootstrapped) return error.AlreadyBootstrapped;

        // Generate root key
        const result = try keys.generateKey(self.allocator, "root", .admin, 0);

        // Store it
        try self.putKey(result.key);

        // Generate signing secret
        self.signing_secret = keys.generateSigningSecret();
        self.bootstrapped = true;

        log.info("Bootstrap complete: root key and signing secret generated", .{});

        return result.plaintext;
    }

    /// Store a key in the map. The key-ID is duped and owned by the store.
    pub fn putKey(self: *KeyStore, key: keys.ApiKey) !void {
        const id = try self.allocator.dupe(u8, key.getId());
        errdefer self.allocator.free(id);
        try self.key_map.put(self.allocator, id, key);
    }

    /// Look up a key by its full plaintext and verify it.
    /// Returns the ApiKey if valid, null if not found or invalid.
    pub fn validateKey(self: *const KeyStore, plaintext: []const u8) ?*const keys.ApiKey {
        var it = self.key_map.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.verifyKey(plaintext)) {
                return entry.value_ptr;
            }
        }
        return null;
    }

    /// Look up a key by its ID prefix.
    pub fn getKeyById(self: *const KeyStore, id: []const u8) ?*const keys.ApiKey {
        return self.key_map.get(id);
    }

    /// Revoke a key by its ID.
    pub fn revokeKey(self: *KeyStore, id: []const u8) !void {
        const entry = self.key_map.getPtr(id) orelse return error.KeyNotFound;
        entry.revoked = true;
        log.info("API key revoked: id={s}", .{id});
    }

    /// List all keys (returns slice of pointers — caller does not own).
    pub fn listKeys(self: *const KeyStore, allocator: Allocator) ![]const keys.ApiKey {
        const num_keys = self.key_map.count();
        if (num_keys == 0) return &.{};

        const list = try allocator.alloc(keys.ApiKey, num_keys);
        var i: usize = 0;
        var it = self.key_map.iterator();
        while (it.next()) |entry| {
            list[i] = entry.value_ptr.*;
            i += 1;
        }
        return list;
    }

    /// Get the signing secret. Returns null if not bootstrapped.
    pub fn getSigningSecret(self: *const KeyStore) ?[]const u8 {
        if (self.signing_secret) |*s| return s;
        return null;
    }

    /// Number of stored keys.
    pub fn count(self: *const KeyStore) usize {
        return self.key_map.count();
    }
};

// =============================================================================
// Tests
// =============================================================================

test "KeyStore bootstrap" {
    var store = KeyStore.init(std.testing.allocator);
    defer store.deinit();

    const root_key = try store.bootstrap();
    defer std.testing.allocator.free(root_key);

    try std.testing.expect(store.bootstrapped);
    try std.testing.expect(store.signing_secret != null);
    try std.testing.expectEqual(@as(usize, 1), store.count());

    // Root key should validate
    const found = store.validateKey(root_key);
    try std.testing.expect(found != null);
    try std.testing.expectEqual(keys.Role.admin, found.?.role);

    // Double bootstrap fails
    try std.testing.expectError(error.AlreadyBootstrapped, store.bootstrap());
}

test "KeyStore create and validate key" {
    var store = KeyStore.init(std.testing.allocator);
    defer store.deinit();

    const result = try keys.generateKey(std.testing.allocator, "test", .operator, 0);
    defer std.testing.allocator.free(result.plaintext);

    try store.putKey(result.key);
    try std.testing.expectEqual(@as(usize, 1), store.count());

    // Validate with correct key
    const found = store.validateKey(result.plaintext);
    try std.testing.expect(found != null);
    try std.testing.expectEqual(keys.Role.operator, found.?.role);

    // Validate with wrong key
    try std.testing.expect(store.validateKey("wrong_key") == null);
}

test "KeyStore revoke key" {
    var store = KeyStore.init(std.testing.allocator);
    defer store.deinit();

    const result = try keys.generateKey(std.testing.allocator, "revoke-test", .viewer, 0);
    defer std.testing.allocator.free(result.plaintext);

    try store.putKey(result.key);
    try std.testing.expect(store.validateKey(result.plaintext) != null);

    // Revoke
    try store.revokeKey(result.key.getId());

    // Should no longer validate
    try std.testing.expect(store.validateKey(result.plaintext) == null);
}

test "KeyStore listKeys" {
    var store = KeyStore.init(std.testing.allocator);
    defer store.deinit();

    const k1 = try keys.generateKey(std.testing.allocator, "a", .admin, 0);
    defer std.testing.allocator.free(k1.plaintext);
    try store.putKey(k1.key);

    const k2 = try keys.generateKey(std.testing.allocator, "b", .viewer, 0);
    defer std.testing.allocator.free(k2.plaintext);
    try store.putKey(k2.key);

    const list = try store.listKeys(std.testing.allocator);
    defer std.testing.allocator.free(list);

    try std.testing.expectEqual(@as(usize, 2), list.len);
}
