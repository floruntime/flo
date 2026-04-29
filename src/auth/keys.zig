//! API Key Generation, Hashing, and Validation
//!
//! Keys use the format `flo_sk_<role>_<random>` for at-a-glance identification.
//! Only the SHA-256 hash of the key is stored — never the plaintext.
//! Comparison is constant-time to prevent timing attacks.

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;

/// Roles for API keys. Maps directly to `matchScope()` patterns in jwt.zig.
pub const Role = enum {
    admin,
    operator,
    viewer,

    pub fn toString(self: Role) []const u8 {
        return switch (self) {
            .admin => "admin",
            .operator => "operator",
            .viewer => "viewer",
        };
    }

    pub fn fromString(s: []const u8) ?Role {
        if (std.mem.eql(u8, s, "admin")) return .admin;
        if (std.mem.eql(u8, s, "operator")) return .operator;
        if (std.mem.eql(u8, s, "viewer")) return .viewer;
        return null;
    }

    /// Return the scope patterns for this role.
    pub fn scopes(self: Role) []const []const u8 {
        return switch (self) {
            .admin => &.{"*:*:*"},
            .operator => &.{ "read:*:*", "write:kv:*", "write:stream:*", "write:queue:*", "write:ts:*" },
            .viewer => &.{"read:*:*"},
        };
    }
};

/// Stored representation of an API key (plaintext key is never stored).
pub const ApiKey = struct {
    /// Key ID — first 16 chars of the full key (e.g. "flo_sk_admin_abc")
    /// Used for listing and identification without exposing the secret.
    id: [key_id_len]u8,
    id_len: u8,

    /// Human-readable name (e.g. "ci-bot", "alice")
    name: [max_name_len]u8,
    name_len: u8,

    /// SHA-256 hash of the full key string
    hash: [32]u8,

    /// Role
    role: Role,

    /// Creation time (Unix timestamp)
    created_at: i64,

    /// Expiration time (Unix timestamp), 0 = never expires
    expires_at: i64,

    /// Whether this key has been revoked
    revoked: bool,

    const key_id_len = 48;
    const max_name_len = 64;

    pub fn getName(self: *const ApiKey) []const u8 {
        return self.name[0..self.name_len];
    }

    pub fn getId(self: *const ApiKey) []const u8 {
        return self.id[0..self.id_len];
    }

    pub fn isExpired(self: *const ApiKey) bool {
        if (self.expires_at == 0) return false;
        return @import("stdx").time.milliTimestamp() > self.expires_at;
    }

    pub fn isValid(self: *const ApiKey) bool {
        return !self.revoked and !self.isExpired();
    }

    /// Constant-time comparison of a candidate key against the stored hash.
    pub fn verifyKey(self: *const ApiKey, candidate: []const u8) bool {
        if (self.revoked) return false;
        if (self.isExpired()) return false;
        const candidate_hash = hashKey(candidate);
        return std.crypto.timing_safe.eql([32]u8, self.hash, candidate_hash);
    }

    /// Serialize to bytes for storage.
    pub fn serialize(self: *const ApiKey, buf: []u8) ![]const u8 {
        if (buf.len < serialized_size) return error.BufferTooSmall;
        var pos: usize = 0;

        // id_len + id
        buf[pos] = self.id_len;
        pos += 1;
        @memcpy(buf[pos..][0..key_id_len], &self.id);
        pos += key_id_len;

        // name_len + name
        buf[pos] = self.name_len;
        pos += 1;
        @memcpy(buf[pos..][0..max_name_len], &self.name);
        pos += max_name_len;

        // hash
        @memcpy(buf[pos..][0..32], &self.hash);
        pos += 32;

        // role
        buf[pos] = @intFromEnum(self.role);
        pos += 1;

        // created_at (i64 LE)
        @memcpy(buf[pos..][0..8], std.mem.asBytes(&self.created_at));
        pos += 8;

        // expires_at (i64 LE)
        @memcpy(buf[pos..][0..8], std.mem.asBytes(&self.expires_at));
        pos += 8;

        // revoked
        buf[pos] = @intFromBool(self.revoked);
        pos += 1;

        return buf[0..pos];
    }

    /// Deserialize from bytes.
    pub fn deserialize(data: []const u8) !ApiKey {
        if (data.len < serialized_size) return error.InvalidData;
        var pos: usize = 0;

        var key: ApiKey = undefined;

        key.id_len = data[pos];
        pos += 1;
        if (key.id_len > key_id_len) return error.InvalidData;
        @memcpy(&key.id, data[pos..][0..key_id_len]);
        pos += key_id_len;

        key.name_len = data[pos];
        pos += 1;
        if (key.name_len > max_name_len) return error.InvalidData;
        @memcpy(&key.name, data[pos..][0..max_name_len]);
        pos += max_name_len;

        @memcpy(&key.hash, data[pos..][0..32]);
        pos += 32;

        key.role = @enumFromInt(data[pos]);
        pos += 1;

        key.created_at = std.mem.bytesAsValue(i64, data[pos..][0..8]).*;
        pos += 8;

        key.expires_at = std.mem.bytesAsValue(i64, data[pos..][0..8]).*;
        pos += 8;

        key.revoked = data[pos] != 0;

        return key;
    }

    pub const serialized_size = 1 + key_id_len + 1 + max_name_len + 32 + 1 + 8 + 8 + 1;
};

/// Length of the random part of the key (bytes, before hex encoding).
const random_bytes = 20;

/// Generate a new API key. Returns the plaintext key string and the ApiKey struct.
/// The caller must display/save the plaintext key — it is not stored.
pub fn generateKey(
    allocator: Allocator,
    name: []const u8,
    role: Role,
    expires_at: i64,
) !struct { plaintext: []const u8, key: ApiKey } {
    // Generate random bytes
    var rand_buf: [random_bytes]u8 = undefined;
    try @import("stdx").io.instance().randomSecure(&rand_buf);

    // Hex-encode the random part
    const hex_buf = std.fmt.bytesToHex(rand_buf, .lower);
    const hex: []const u8 = &hex_buf;

    // Build full key: flo_sk_<role>_<hex>
    const role_str = role.toString();
    const plaintext = try std.fmt.allocPrint(allocator, "flo_sk_{s}_{s}", .{ role_str, hex });

    // Build the stored key struct
    var key: ApiKey = .{
        .id = undefined,
        .id_len = 0,
        .name = undefined,
        .name_len = 0,
        .hash = hashKey(plaintext),
        .role = role,
        .created_at = @import("stdx").time.milliTimestamp(),
        .expires_at = expires_at,
        .revoked = false,
    };

    // Store the ID (prefix of the plaintext key, enough to identify it)
    const id_len: u8 = @intCast(@min(plaintext.len, ApiKey.key_id_len));
    @memset(&key.id, 0);
    @memcpy(key.id[0..id_len], plaintext[0..id_len]);
    key.id_len = id_len;

    // Store the name
    const name_len: u8 = @intCast(@min(name.len, ApiKey.max_name_len));
    @memset(&key.name, 0);
    @memcpy(key.name[0..name_len], name[0..name_len]);
    key.name_len = name_len;

    log.info("API key created: name={s} role={s} expires={d}", .{ name, role_str, expires_at });

    return .{ .plaintext = plaintext, .key = key };
}

/// Generate the internal HS256 signing secret for session JWTs.
/// Returns 32 random bytes.
pub fn generateSigningSecret() [32]u8 {
    var secret: [32]u8 = undefined;
    @import("stdx").io.instance().randomSecure(&secret) catch @panic("randomSecure failed");
    return secret;
}

/// SHA-256 hash of a key string.
pub fn hashKey(key: []const u8) [32]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(key, &hash, .{});
    return hash;
}

/// Extract role from a key prefix (e.g. "flo_sk_admin_..." → .admin).
pub fn roleFromKeyPrefix(key: []const u8) ?Role {
    if (!std.mem.startsWith(u8, key, "flo_sk_")) return null;
    const rest = key[7..]; // after "flo_sk_"
    if (std.mem.startsWith(u8, rest, "admin_")) return .admin;
    if (std.mem.startsWith(u8, rest, "operator_")) return .operator;
    if (std.mem.startsWith(u8, rest, "viewer_")) return .viewer;
    return null;
}

// =============================================================================
// Tests
// =============================================================================

test "generateKey produces valid prefixed key" {
    const result = try generateKey(std.testing.allocator, "test-key", .admin, 0);
    defer std.testing.allocator.free(result.plaintext);

    try std.testing.expect(std.mem.startsWith(u8, result.plaintext, "flo_sk_admin_"));
    try std.testing.expectEqual(Role.admin, result.key.role);
    try std.testing.expect(result.key.isValid());
    try std.testing.expect(result.key.verifyKey(result.plaintext));
    try std.testing.expect(!result.key.verifyKey("wrong_key"));
}

test "generateKey with operator role" {
    const result = try generateKey(std.testing.allocator, "ci-bot", .operator, 0);
    defer std.testing.allocator.free(result.plaintext);

    try std.testing.expect(std.mem.startsWith(u8, result.plaintext, "flo_sk_operator_"));
    try std.testing.expectEqual(Role.operator, result.key.role);
}

test "generateKey with expiration" {
    const result = try generateKey(std.testing.allocator, "temp", .viewer, 1);
    defer std.testing.allocator.free(result.plaintext);

    // expires_at = 1 (Unix epoch + 1s) — already expired
    try std.testing.expect(result.key.isExpired());
    try std.testing.expect(!result.key.isValid());
    try std.testing.expect(!result.key.verifyKey(result.plaintext)); // expired key rejects
}

test "ApiKey revocation" {
    var result = try generateKey(std.testing.allocator, "revokable", .admin, 0);
    defer std.testing.allocator.free(result.plaintext);

    try std.testing.expect(result.key.isValid());
    result.key.revoked = true;
    try std.testing.expect(!result.key.isValid());
    try std.testing.expect(!result.key.verifyKey(result.plaintext)); // revoked key rejects
}

test "ApiKey serialize/deserialize roundtrip" {
    const result = try generateKey(std.testing.allocator, "roundtrip", .operator, 1741500000);
    defer std.testing.allocator.free(result.plaintext);

    var buf: [ApiKey.serialized_size]u8 = undefined;
    _ = try result.key.serialize(&buf);

    const restored = try ApiKey.deserialize(&buf);
    try std.testing.expectEqual(result.key.role, restored.role);
    try std.testing.expectEqual(result.key.created_at, restored.created_at);
    try std.testing.expectEqual(result.key.expires_at, restored.expires_at);
    try std.testing.expect(std.mem.eql(u8, result.key.getName(), restored.getName()));
    try std.testing.expect(std.crypto.timing_safe.eql([32]u8, result.key.hash, restored.hash));
}

test "roleFromKeyPrefix" {
    try std.testing.expectEqual(Role.admin, roleFromKeyPrefix("flo_sk_admin_abc123").?);
    try std.testing.expectEqual(Role.operator, roleFromKeyPrefix("flo_sk_operator_xyz").?);
    try std.testing.expectEqual(Role.viewer, roleFromKeyPrefix("flo_sk_viewer_000").?);
    try std.testing.expect(roleFromKeyPrefix("invalid_key") == null);
    try std.testing.expect(roleFromKeyPrefix("flo_sk_unknown_xxx") == null);
}

test "Role.scopes returns correct patterns" {
    const admin_scopes = Role.admin.scopes();
    try std.testing.expectEqual(@as(usize, 1), admin_scopes.len);
    try std.testing.expect(std.mem.eql(u8, "*:*:*", admin_scopes[0]));

    const op_scopes = Role.operator.scopes();
    try std.testing.expectEqual(@as(usize, 5), op_scopes.len);

    const viewer_scopes = Role.viewer.scopes();
    try std.testing.expectEqual(@as(usize, 1), viewer_scopes.len);
    try std.testing.expect(std.mem.eql(u8, "read:*:*", viewer_scopes[0]));
}

test "hashKey is deterministic" {
    const h1 = hashKey("flo_sk_admin_test123");
    const h2 = hashKey("flo_sk_admin_test123");
    try std.testing.expect(std.crypto.timing_safe.eql([32]u8, h1, h2));

    const h3 = hashKey("flo_sk_admin_other456");
    try std.testing.expect(!std.crypto.timing_safe.eql([32]u8, h1, h3));
}

test "generateSigningSecret produces random bytes" {
    const s1 = generateSigningSecret();
    const s2 = generateSigningSecret();
    // Extremely unlikely to be equal
    try std.testing.expect(!std.crypto.timing_safe.eql([32]u8, s1, s2));
}
