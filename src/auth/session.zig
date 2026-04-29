//! Dashboard Session Token — HS256 JWT issuance and verification
//!
//! EKS-style exchange: API key → POST /api/v1/auth/session → short-lived HS256 JWT.
//! The session token is used for all subsequent dashboard REST calls.
//! Signing uses the internal secret generated at bootstrap.

const std = @import("std");
const Allocator = std.mem.Allocator;
const keys = @import("keys.zig");
const log = @import("stdx").log;

pub const SessionError = error{
    InvalidToken,
    TokenExpired,
    InvalidSignature,
    MalformedToken,
    BufferTooSmall,
};

/// Default session TTL: 8 hours
pub const default_ttl_seconds: i64 = 8 * 60 * 60;

/// Session claims embedded in the HS256 JWT.
pub const SessionClaims = struct {
    /// Subject — the API key ID (not the full secret)
    sub: []const u8,
    /// Role
    role: keys.Role,
    /// Issued-at (Unix timestamp)
    iat: i64,
    /// Expiration (Unix timestamp)
    exp: i64,
};

/// Issue a session JWT signed with the internal HS256 secret.
/// Returns the encoded JWT string (caller owns the memory).
pub fn issueSessionToken(
    allocator: Allocator,
    key_id: []const u8,
    role: keys.Role,
    signing_secret: []const u8,
    ttl_seconds: i64,
) ![]const u8 {
    const now = @import("stdx").time.milliTimestamp();
    const exp = now + ttl_seconds;

    // Header: {"alg":"HS256","typ":"JWT"}
    const header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";

    // Build payload JSON
    var issue_payload_buf: [512]u8 = undefined;
    const payload_json = std.fmt.bufPrint(&issue_payload_buf, "{{\"sub\":\"{s}\",\"role\":\"{s}\",\"iat\":{d},\"exp\":{d}}}", .{
        key_id,
        role.toString(),
        now,
        exp,
    }) catch return error.BufferTooSmall;

    // Base64url encode payload
    const payload_b64 = try base64UrlEncode(allocator, payload_json);
    defer allocator.free(payload_b64);

    // Compute HMAC-SHA256 signature over "header.payload"
    var hmac = std.crypto.auth.hmac.sha2.HmacSha256.init(signing_secret);
    hmac.update(header);
    hmac.update(".");
    hmac.update(payload_b64);
    var sig: [32]u8 = undefined;
    hmac.final(&sig);

    // Base64url encode signature
    const sig_b64 = try base64UrlEncode(allocator, &sig);
    defer allocator.free(sig_b64);

    // Combine: header.payload.signature
    const token = try std.fmt.allocPrint(allocator, "{s}.{s}.{s}", .{ header, payload_b64, sig_b64 });

    log.info("Session token issued: key_id={s} role={s} expires_in={d}s", .{ key_id, role.toString(), ttl_seconds });

    return token;
}

/// Verify a session token and extract claims.
/// Returns SessionError on invalid/expired tokens.
///
/// NOTE: `claims.sub` is a slice into a threadlocal buffer that remains
/// valid until the next `verifySessionToken` call on this thread. Copy it
/// if you need to retain it across calls.
threadlocal var payload_buf: [512]u8 = undefined;

pub fn verifySessionToken(
    signing_secret: []const u8,
    token: []const u8,
) SessionError!SessionClaims {
    // Split into header.payload.signature
    var parts = std.mem.splitScalar(u8, token, '.');
    const header_b64 = parts.next() orelse return SessionError.MalformedToken;
    const payload_b64 = parts.next() orelse return SessionError.MalformedToken;
    const sig_b64 = parts.next() orelse return SessionError.MalformedToken;
    if (parts.next() != null) return SessionError.MalformedToken;

    // Verify HMAC-SHA256 signature
    var hmac = std.crypto.auth.hmac.sha2.HmacSha256.init(signing_secret);
    hmac.update(header_b64);
    hmac.update(".");
    hmac.update(payload_b64);
    var expected_sig: [32]u8 = undefined;
    hmac.final(&expected_sig);

    // Decode provided signature
    var provided_sig: [32]u8 = undefined;
    const decoded_len = std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(sig_b64) catch return SessionError.InvalidSignature;
    if (decoded_len != 32) return SessionError.InvalidSignature;
    std.base64.url_safe_no_pad.Decoder.decode(&provided_sig, sig_b64) catch return SessionError.InvalidSignature;

    // Constant-time comparison
    if (!std.crypto.timing_safe.eql([32]u8, expected_sig, provided_sig)) {
        return SessionError.InvalidSignature;
    }

    // Decode payload — buffer is threadlocal so the returned slice in
    // claims.sub remains valid until the next verifySessionToken call on
    // this thread.
    const payload_len = std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(payload_b64) catch return SessionError.MalformedToken;
    if (payload_len > payload_buf.len) return SessionError.MalformedToken;
    std.base64.url_safe_no_pad.Decoder.decode(payload_buf[0..payload_len], payload_b64) catch return SessionError.MalformedToken;
    const payload_json = payload_buf[0..payload_len];

    // Parse claims from JSON (simple string extraction — no allocator needed)
    const sub = extractJsonString(payload_json, "\"sub\"") orelse return SessionError.MalformedToken;
    const role_str = extractJsonString(payload_json, "\"role\"") orelse return SessionError.MalformedToken;
    const role = keys.Role.fromString(role_str) orelse return SessionError.MalformedToken;
    const iat = extractJsonNumber(payload_json, "\"iat\"") orelse return SessionError.MalformedToken;
    const exp = extractJsonNumber(payload_json, "\"exp\"") orelse return SessionError.MalformedToken;

    // Check expiration
    const now = @import("stdx").time.milliTimestamp();
    if (now > exp) return SessionError.TokenExpired;

    return .{
        .sub = sub,
        .role = role,
        .iat = iat,
        .exp = exp,
    };
}

// =============================================================================
// Helpers
// =============================================================================

fn base64UrlEncode(allocator: Allocator, data: []const u8) ![]const u8 {
    const encoded_len = std.base64.url_safe_no_pad.Encoder.calcSize(data.len);
    const buf = try allocator.alloc(u8, encoded_len);
    _ = std.base64.url_safe_no_pad.Encoder.encode(buf, data);
    return buf;
}

/// Extract a string value from JSON given a key.
/// Simple parser — finds "key":"value" pattern. Returns a slice into the input.
fn extractJsonString(json: []const u8, key: []const u8) ?[]const u8 {
    const key_pos = std.mem.indexOf(u8, json, key) orelse return null;
    const after_key = json[key_pos + key.len ..];

    // Skip colon and whitespace
    var i: usize = 0;
    while (i < after_key.len and (after_key[i] == ':' or after_key[i] == ' ' or after_key[i] == '"')) : (i += 1) {}
    if (i == 0) return null;

    // Find closing quote
    const start = i;
    while (i < after_key.len and after_key[i] != '"') : (i += 1) {}
    if (i == start) return null;

    return after_key[start..i];
}

/// Extract a number value from JSON given a key.
fn extractJsonNumber(json: []const u8, key: []const u8) ?i64 {
    const key_pos = std.mem.indexOf(u8, json, key) orelse return null;
    const after_key = json[key_pos + key.len ..];

    // Skip colon and whitespace
    var i: usize = 0;
    while (i < after_key.len and (after_key[i] == ':' or after_key[i] == ' ')) : (i += 1) {}

    // Parse number
    const start = i;
    var negative = false;
    if (i < after_key.len and after_key[i] == '-') {
        negative = true;
        i += 1;
    }
    while (i < after_key.len and after_key[i] >= '0' and after_key[i] <= '9') : (i += 1) {}
    if (i == start or (negative and i == start + 1)) return null;

    return std.fmt.parseInt(i64, after_key[start..i], 10) catch null;
}

// =============================================================================
// Tests
// =============================================================================

test "issue and verify session token roundtrip" {
    const allocator = std.testing.allocator;
    const secret = "test_signing_secret_32bytes_long!";

    const token = try issueSessionToken(allocator, "flo_sk_admin_abc123", .admin, secret, default_ttl_seconds);
    defer allocator.free(token);

    // Token should have 3 parts
    var parts = std.mem.splitScalar(u8, token, '.');
    _ = parts.next().?;
    _ = parts.next().?;
    _ = parts.next().?;
    try std.testing.expect(parts.next() == null);

    // Verify should succeed
    const claims = try verifySessionToken(secret, token);
    try std.testing.expect(std.mem.eql(u8, "flo_sk_admin_abc123", claims.sub));
    try std.testing.expectEqual(keys.Role.admin, claims.role);
    try std.testing.expect(claims.exp > claims.iat);
}

test "verify rejects wrong secret" {
    const allocator = std.testing.allocator;
    const token = try issueSessionToken(allocator, "flo_sk_admin_abc", .admin, "correct_secret_padded_to_32byte!", default_ttl_seconds);
    defer allocator.free(token);

    const result = verifySessionToken("wrong_secret_padded_to_32_bytes!", token);
    try std.testing.expectError(SessionError.InvalidSignature, result);
}

test "verify rejects expired token" {
    const allocator = std.testing.allocator;
    // TTL of -1 means it expired 1 second ago
    const token = try issueSessionToken(allocator, "flo_sk_viewer_xyz", .viewer, "test_secret_padded_to_32bytes!!", -1);
    defer allocator.free(token);

    const result = verifySessionToken("test_secret_padded_to_32bytes!!", token);
    try std.testing.expectError(SessionError.TokenExpired, result);
}

test "verify rejects malformed token" {
    const result = verifySessionToken("secret", "not.a.valid.jwt.with.too.many.parts");
    try std.testing.expectError(SessionError.MalformedToken, result);

    const result2 = verifySessionToken("secret", "onlyonepart");
    try std.testing.expectError(SessionError.MalformedToken, result2);
}

test "extractJsonString" {
    const json = "{\"sub\":\"flo_sk_admin_abc\",\"role\":\"admin\"}";
    const sub = extractJsonString(json, "\"sub\"");
    try std.testing.expect(sub != null);
    try std.testing.expect(std.mem.eql(u8, "flo_sk_admin_abc", sub.?));
}

test "extractJsonNumber" {
    const json = "{\"iat\":1741478400,\"exp\":1741507200}";
    const iat = extractJsonNumber(json, "\"iat\"");
    try std.testing.expect(iat != null);
    try std.testing.expectEqual(@as(i64, 1741478400), iat.?);
}
