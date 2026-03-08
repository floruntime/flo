//! JWT Parsing and Verification
//!
//! Shared JWT handling for WebSocket authentication.
//! Supports HS256 (symmetric) and RS256 (asymmetric via JWKS) verification.
//!
//! ## Token Format
//! Standard JWT: header.payload.signature (base64url encoded)
//!
//! ## Supported Algorithms
//! - HS256: HMAC-SHA256 with shared secret
//! - RS256: RSASSA-PKCS1-v1_5 with SHA-256 (keys from JWKS endpoint)
//!
//! ## Supported Claims
//! - `sub`: User ID
//! - `flo_namespace`: Locked namespace for connection
//! - `flo_scopes`: Permission scopes array
//! - `exp`: Expiration timestamp (Unix seconds)

const std = @import("std");
const Allocator = std.mem.Allocator;
const JwksClient = @import("jwks.zig").JwksClient;

/// Detected JWT algorithm
pub const Algorithm = enum {
    hs256,
    rs256,
    unknown,
};

/// JWT verification errors
pub const JwtError = error{
    InvalidToken,
    InvalidSignature,
    TokenExpired,
    InvalidAlgorithm,
    MalformedHeader,
    MalformedPayload,
    OutOfMemory,
    KeyNotFound,
    JwksFetchFailed,
};

/// Parsed JWT claims
pub const JwtClaims = struct {
    user_id: ?[]const u8,
    namespace: ?[]const u8,
    scopes: []const []const u8,
    exp: ?i64, // Expiration time (Unix timestamp)

    /// Free all allocated memory in claims
    pub fn deinit(self: *JwtClaims, allocator: Allocator) void {
        if (self.user_id) |uid| allocator.free(uid);
        if (self.namespace) |ns| allocator.free(ns);
        for (self.scopes) |scope| allocator.free(scope);
        if (self.scopes.len > 0) allocator.free(self.scopes);
    }
};

/// Verify JWT signature (HS256) and parse claims
/// If secret is null, skips signature verification (useful for testing)
pub fn verifyAndParse(allocator: Allocator, token: []const u8, secret: ?[]const u8) JwtError!JwtClaims {
    // JWT format: header.payload.signature (base64url encoded)
    var parts = std.mem.splitScalar(u8, token, '.');

    const header_b64 = parts.next() orelse return JwtError.InvalidToken;
    const payload_b64 = parts.next() orelse return JwtError.InvalidToken;
    const signature_b64 = parts.next() orelse return JwtError.InvalidToken;

    // If there are more parts, token is malformed
    if (parts.next() != null) return JwtError.InvalidToken;

    // Verify signature if secret is provided
    if (secret) |jwt_secret| {
        try verifyHs256Signature(header_b64, payload_b64, signature_b64, jwt_secret);
    }

    // Decode and parse header to verify algorithm
    const header_json = decodeBase64Url(allocator, header_b64) catch return JwtError.MalformedHeader;
    defer allocator.free(header_json);

    // Verify algorithm is HS256 (simple string search)
    if (std.mem.indexOf(u8, header_json, "\"HS256\"") == null and
        std.mem.indexOf(u8, header_json, "\"hs256\"") == null)
    {
        // Allow if no secret (testing mode) or if algorithm matches
        if (secret != null) {
            return JwtError.InvalidAlgorithm;
        }
    }

    // Decode payload
    const payload_json = decodeBase64Url(allocator, payload_b64) catch return JwtError.MalformedPayload;
    defer allocator.free(payload_json);

    // Parse claims from JSON
    return parseClaimsFromJson(allocator, payload_json);
}

/// Verify JWT with RS256 signature using JWKS public keys.
/// Automatically extracts `kid` from the JWT header for key selection.
pub fn verifyAndParseRs256(allocator: Allocator, token: []const u8, jwks: *const JwksClient) JwtError!JwtClaims {
    var parts = std.mem.splitScalar(u8, token, '.');

    const header_b64 = parts.next() orelse return JwtError.InvalidToken;
    const payload_b64 = parts.next() orelse return JwtError.InvalidToken;
    const signature_b64 = parts.next() orelse return JwtError.InvalidToken;

    if (parts.next() != null) return JwtError.InvalidToken;

    // Decode header to extract algorithm and kid
    const header_json = decodeBase64Url(allocator, header_b64) catch return JwtError.MalformedHeader;
    defer allocator.free(header_json);

    const alg = detectAlgorithm(header_json);
    if (alg != .rs256) return JwtError.InvalidAlgorithm;

    // Extract kid from header for key selection
    const kid = extractJsonString(header_json, "\"kid\"");

    // Verify RS256 signature via JWKS
    jwks.verifyRs256(header_b64, payload_b64, signature_b64, kid) catch |err| {
        return switch (err) {
            error.KeyNotFound => JwtError.KeyNotFound,
            error.InvalidSignature => JwtError.InvalidSignature,
            error.InvalidKey => JwtError.InvalidSignature,
            error.UnsupportedModulusLength => JwtError.InvalidSignature,
            else => JwtError.InvalidSignature,
        };
    };

    // Decode and parse payload
    const payload_json = decodeBase64Url(allocator, payload_b64) catch return JwtError.MalformedPayload;
    defer allocator.free(payload_json);

    return parseClaimsFromJson(allocator, payload_json);
}

/// Detect the algorithm from a decoded JWT header JSON
pub fn detectAlgorithm(header_json: []const u8) Algorithm {
    if (std.mem.indexOf(u8, header_json, "\"RS256\"") != null or
        std.mem.indexOf(u8, header_json, "\"rs256\"") != null)
    {
        return .rs256;
    }
    if (std.mem.indexOf(u8, header_json, "\"HS256\"") != null or
        std.mem.indexOf(u8, header_json, "\"hs256\"") != null)
    {
        return .hs256;
    }
    return .unknown;
}

/// Parse JWT without signature verification
/// Convenience wrapper for testing or when signature was already verified
pub fn parseUnsafe(allocator: Allocator, token: []const u8) JwtError!JwtClaims {
    return verifyAndParse(allocator, token, null);
}

/// Check if a token has expired
pub fn isExpired(claims: *const JwtClaims) bool {
    if (claims.exp) |exp| {
        const now = std.time.timestamp();
        return now > exp;
    }
    return false; // No expiration = never expires
}

// =============================================================================
// Scope Matching
// =============================================================================

/// Match a scope pattern against action/resource/key
/// Scope format: {action}:{resource_type}:{key_pattern}
/// Examples: "read:kv:*", "write:stream:chat:*", "*:*:*"
///
/// Matching rules:
/// - "*" matches any value in that position
/// - "prefix:*" matches keys starting with "prefix:"
/// - Exact string matches exactly
pub fn matchScope(scope: []const u8, action: []const u8, resource_type: []const u8, key: []const u8) bool {
    var scope_parts = std.mem.splitScalar(u8, scope, ':');

    // Match action
    const scope_action = scope_parts.next() orelse return false;
    if (!std.mem.eql(u8, scope_action, "*") and !std.mem.eql(u8, scope_action, action)) {
        return false;
    }

    // Match resource type
    const scope_resource = scope_parts.next() orelse return false;
    if (!std.mem.eql(u8, scope_resource, "*") and !std.mem.eql(u8, scope_resource, resource_type)) {
        return false;
    }

    // Match key pattern (rest of scope is the pattern)
    const scope_key = scope_parts.rest();
    if (scope_key.len == 0 or std.mem.eql(u8, scope_key, "*")) {
        return true; // Wildcard matches everything
    }

    // Check for prefix wildcard (e.g., "chat:*")
    if (std.mem.endsWith(u8, scope_key, "*")) {
        const prefix = scope_key[0 .. scope_key.len - 1];
        return std.mem.startsWith(u8, key, prefix);
    }

    // Exact match
    return std.mem.eql(u8, scope_key, key);
}

/// Verify HS256 (HMAC-SHA256) signature
fn verifyHs256Signature(
    header_b64: []const u8,
    payload_b64: []const u8,
    signature_b64: []const u8,
    secret: []const u8,
) JwtError!void {
    // Compute expected signature: HMAC-SHA256(header.payload, secret)
    var hmac = std.crypto.auth.hmac.sha2.HmacSha256.init(secret);

    // Feed header.payload (the signing input)
    hmac.update(header_b64);
    hmac.update(".");
    hmac.update(payload_b64);

    var expected_sig: [32]u8 = undefined;
    hmac.final(&expected_sig);

    // Decode provided signature (base64url)
    var provided_sig: [32]u8 = undefined;
    const decoded_len = std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(signature_b64) catch return JwtError.InvalidSignature;
    if (decoded_len != 32) return JwtError.InvalidSignature;

    std.base64.url_safe_no_pad.Decoder.decode(&provided_sig, signature_b64) catch return JwtError.InvalidSignature;

    // Constant-time comparison to prevent timing attacks
    if (!std.crypto.timing_safe.eql([32]u8, expected_sig, provided_sig)) {
        return JwtError.InvalidSignature;
    }
}

/// Decode base64url (JWT uses URL-safe base64 without padding)
fn decodeBase64Url(allocator: Allocator, encoded: []const u8) ![]u8 {
    const decoded_len = std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(encoded) catch return error.InvalidToken;
    const decoded = try allocator.alloc(u8, decoded_len);
    errdefer allocator.free(decoded);

    std.base64.url_safe_no_pad.Decoder.decode(decoded, encoded) catch return error.InvalidToken;
    return decoded;
}

/// Parse JWT claims from JSON payload
/// Extracts: sub (user_id), flo_namespace, flo_scopes, exp
fn parseClaimsFromJson(allocator: Allocator, json: []const u8) JwtError!JwtClaims {
    var claims = JwtClaims{
        .user_id = null,
        .namespace = null,
        .scopes = &.{},
        .exp = null,
    };

    // Simple JSON parsing (handles common JWT payload format)
    // In production, consider using std.json for robustness

    // Extract "sub" claim (user ID)
    if (extractJsonString(json, "\"sub\"")) |sub| {
        claims.user_id = allocator.dupe(u8, sub) catch return JwtError.OutOfMemory;
    }

    // Extract "flo_namespace" claim
    if (extractJsonString(json, "\"flo_namespace\"")) |ns| {
        claims.namespace = allocator.dupe(u8, ns) catch return JwtError.OutOfMemory;
    }

    // Extract "exp" claim (expiration)
    if (extractJsonNumber(json, "\"exp\"")) |exp| {
        claims.exp = exp;
    }

    // Extract "flo_scopes" array (simplified - assumes format: "flo_scopes":["scope1","scope2"])
    if (std.mem.indexOf(u8, json, "\"flo_scopes\"")) |start| {
        const after_key = json[start + 13 ..]; // Skip "flo_scopes":
        if (std.mem.indexOf(u8, after_key, "[")) |arr_start| {
            if (std.mem.indexOf(u8, after_key[arr_start..], "]")) |arr_end| {
                const arr_content = after_key[arr_start + 1 .. arr_start + arr_end];
                claims.scopes = parseJsonStringArray(allocator, arr_content) catch &.{};
            }
        }
    }

    return claims;
}

/// Extract a string value from JSON by key
/// Returns the string content (without quotes)
fn extractJsonString(json: []const u8, key: []const u8) ?[]const u8 {
    const key_pos = std.mem.indexOf(u8, json, key) orelse return null;
    const after_key = json[key_pos + key.len ..];

    // Skip : and whitespace
    var i: usize = 0;
    while (i < after_key.len and (after_key[i] == ':' or after_key[i] == ' ' or after_key[i] == '\t')) : (i += 1) {}

    if (i >= after_key.len or after_key[i] != '"') return null;
    i += 1; // Skip opening quote

    // Find closing quote (handle escaped quotes)
    const start = i;
    while (i < after_key.len) : (i += 1) {
        if (after_key[i] == '\\' and i + 1 < after_key.len) {
            i += 1; // Skip escaped character
            continue;
        }
        if (after_key[i] == '"') {
            return after_key[start..i];
        }
    }

    return null;
}

/// Extract a number value from JSON by key
fn extractJsonNumber(json: []const u8, key: []const u8) ?i64 {
    const key_pos = std.mem.indexOf(u8, json, key) orelse return null;
    const after_key = json[key_pos + key.len ..];

    // Skip : and whitespace
    var i: usize = 0;
    while (i < after_key.len and (after_key[i] == ':' or after_key[i] == ' ' or after_key[i] == '\t')) : (i += 1) {}

    // Handle negative numbers
    const is_negative = i < after_key.len and after_key[i] == '-';
    if (is_negative) i += 1;

    // Parse digits
    const start = i;
    while (i < after_key.len and (after_key[i] >= '0' and after_key[i] <= '9')) : (i += 1) {}

    if (i == start) return null;

    const num_str = if (is_negative) after_key[start - 1 .. i] else after_key[start..i];
    return std.fmt.parseInt(i64, num_str, 10) catch null;
}

/// Parse JSON string array: "scope1","scope2" -> ["scope1", "scope2"]
fn parseJsonStringArray(allocator: Allocator, content: []const u8) ![]const []const u8 {
    var scopes = std.ArrayListUnmanaged([]const u8){};
    errdefer {
        for (scopes.items) |s| allocator.free(s);
        scopes.deinit(allocator);
    }

    var i: usize = 0;
    while (i < content.len) {
        // Skip whitespace and commas
        while (i < content.len and (content[i] == ' ' or content[i] == ',' or content[i] == '\t')) : (i += 1) {}
        if (i >= content.len) break;

        // Expect opening quote
        if (content[i] != '"') {
            i += 1;
            continue;
        }
        i += 1;

        // Find closing quote
        const start = i;
        while (i < content.len and content[i] != '"') : (i += 1) {}
        if (i >= content.len) break;

        const scope = try allocator.dupe(u8, content[start..i]);
        try scopes.append(allocator, scope);
        i += 1; // Skip closing quote
    }

    return scopes.toOwnedSlice(allocator);
}

test "parseUnsafe basic token" {
    const allocator = std.testing.allocator;

    // Token with sub and namespace (no signature verification)
    // Header: {"alg":"HS256","typ":"JWT"}
    // Payload: {"sub":"user123","flo_namespace":"test"}
    const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIiwiZmxvX25hbWVzcGFjZSI6InRlc3QifQ.signature";

    var claims = try parseUnsafe(allocator, token);
    defer claims.deinit(allocator);

    try std.testing.expectEqualStrings("user123", claims.user_id.?);
    try std.testing.expectEqualStrings("test", claims.namespace.?);
}

test "extractJsonString" {
    const json = "{\"sub\":\"user123\",\"name\":\"Test User\"}";

    try std.testing.expectEqualStrings("user123", extractJsonString(json, "\"sub\"").?);
    try std.testing.expectEqualStrings("Test User", extractJsonString(json, "\"name\"").?);
    try std.testing.expect(extractJsonString(json, "\"missing\"") == null);
}

test "extractJsonNumber" {
    const json = "{\"exp\":1234567890,\"iat\":1234567800}";

    try std.testing.expectEqual(@as(i64, 1234567890), extractJsonNumber(json, "\"exp\"").?);
    try std.testing.expectEqual(@as(i64, 1234567800), extractJsonNumber(json, "\"iat\"").?);
    try std.testing.expect(extractJsonNumber(json, "\"missing\"") == null);
}

test "isExpired" {
    var expired_claims = JwtClaims{
        .user_id = null,
        .namespace = null,
        .scopes = &.{},
        .exp = 0, // Expired in 1970
    };

    var valid_claims = JwtClaims{
        .user_id = null,
        .namespace = null,
        .scopes = &.{},
        .exp = std.time.timestamp() + 3600, // 1 hour from now
    };

    var no_exp_claims = JwtClaims{
        .user_id = null,
        .namespace = null,
        .scopes = &.{},
        .exp = null,
    };

    try std.testing.expect(isExpired(&expired_claims));
    try std.testing.expect(!isExpired(&valid_claims));
    try std.testing.expect(!isExpired(&no_exp_claims));
}

test "matchScope wildcard" {
    try std.testing.expect(matchScope("*:*:*", "read", "kv", "anything"));
    try std.testing.expect(matchScope("read:*:*", "read", "kv", "anything"));
    try std.testing.expect(matchScope("read:kv:*", "read", "kv", "anything"));
}

test "matchScope prefix" {
    try std.testing.expect(matchScope("read:stream:chat:*", "read", "stream", "chat:room1"));
    try std.testing.expect(matchScope("read:stream:chat:*", "read", "stream", "chat:room2"));
    try std.testing.expect(!matchScope("read:stream:chat:*", "read", "stream", "private:room1"));
}

test "matchScope exact" {
    try std.testing.expect(matchScope("read:kv:config", "read", "kv", "config"));
    try std.testing.expect(!matchScope("read:kv:config", "read", "kv", "other"));
}

test "matchScope action mismatch" {
    try std.testing.expect(!matchScope("read:kv:*", "write", "kv", "anything"));
}
