//! JWKS (JSON Web Key Set) Client
//!
//! Fetches and caches RSA public keys from a JWKS endpoint for RS256
//! JWT signature verification. Implements key rotation via TTL-based
//! cache refresh.
//!
//! ## JWKS Format (RFC 7517)
//! ```json
//! { "keys": [
//!     { "kty": "RSA", "use": "sig", "kid": "key-1",
//!       "n": "<base64url modulus>", "e": "<base64url exponent>" }
//! ]}
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const Certificate = std.crypto.Certificate;
const rsa = Certificate.rsa;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const JwksError = error{
    FetchFailed,
    InvalidJwks,
    KeyNotFound,
    InvalidKey,
    InvalidSignature,
    OutOfMemory,
    UnsupportedModulusLength,
};

/// A cached RSA public key parsed from JWKS
pub const CachedKey = struct {
    kid: []const u8,
    /// Raw modulus bytes (big-endian, leading zeros stripped)
    modulus: []const u8,
    /// Raw exponent bytes (big-endian)
    exponent: []const u8,
};

/// JWKS client with key caching
pub const JwksClient = struct {
    allocator: Allocator,
    jwks_url: []const u8,
    keys: std.ArrayListUnmanaged(CachedKey),
    last_fetch_ms: i64,
    cache_ttl_ms: i64,

    /// Default cache TTL: 1 hour
    const DEFAULT_TTL_MS: i64 = 3600 * 1000;

    pub fn init(allocator: Allocator, jwks_url: []const u8) JwksClient {
        return .{
            .allocator = allocator,
            .jwks_url = jwks_url,
            .keys = .empty,
            .last_fetch_ms = 0,
            .cache_ttl_ms = DEFAULT_TTL_MS,
        };
    }

    pub fn deinit(self: *JwksClient) void {
        self.clearKeys();
    }

    fn clearKeys(self: *JwksClient) void {
        for (self.keys.items) |key| {
            self.allocator.free(key.kid);
            self.allocator.free(key.modulus);
            self.allocator.free(key.exponent);
        }
        self.keys.deinit(self.allocator);
    }

    /// Find a key by kid. Returns null if not found.
    pub fn findKey(self: *const JwksClient, kid: []const u8) ?*const CachedKey {
        for (self.keys.items) |*key| {
            if (std.mem.eql(u8, key.kid, kid)) return key;
        }
        return null;
    }

    /// Check if cache needs refresh
    pub fn needsRefresh(self: *const JwksClient, now_ms: i64) bool {
        return self.keys.items.len == 0 or
            (now_ms - self.last_fetch_ms) > self.cache_ttl_ms;
    }

    /// Fetch JWKS from the configured URL via HTTP(S)
    pub fn fetch(self: *JwksClient) !void {
        var client: std.http.Client = .{ .allocator = self.allocator };
        defer client.deinit();

        const uri = std.Uri.parse(self.jwks_url) catch return JwksError.FetchFailed;

        var buf: [8192]u8 = undefined;
        var req = client.open(.GET, uri, .{ .server_header_buffer = &buf }) catch return JwksError.FetchFailed;
        defer req.deinit();

        req.send() catch return JwksError.FetchFailed;
        req.wait() catch return JwksError.FetchFailed;

        if (req.status != .ok) return JwksError.FetchFailed;

        const body = req.reader().readAllAlloc(self.allocator, 256 * 1024) catch return JwksError.FetchFailed;
        defer self.allocator.free(body);

        try self.parseJwks(body);
        self.last_fetch_ms = std.time.milliTimestamp();
    }

    /// Parse JWKS JSON and extract RSA signing keys.
    /// Public for testing — normally called by fetch().
    pub fn parseJwks(self: *JwksClient, json: []const u8) !void {
        self.clearKeys();
        self.keys = .empty;

        // Find "keys" array
        const keys_start = std.mem.indexOf(u8, json, "\"keys\"") orelse return JwksError.InvalidJwks;
        const arr_start = std.mem.indexOfPos(u8, json, keys_start, "[") orelse return JwksError.InvalidJwks;

        // Parse each key object in the array
        var pos = arr_start + 1;
        while (pos < json.len) {
            // Skip whitespace
            while (pos < json.len and isWhitespace(json[pos])) pos += 1;
            if (pos >= json.len or json[pos] == ']') break;

            if (json[pos] == '{') {
                const obj_end = findMatchingBrace(json, pos) orelse break;
                const obj = json[pos .. obj_end + 1];

                if (tryParseRsaKey(self.allocator, obj)) |key| {
                    self.keys.append(self.allocator, key) catch {
                        self.allocator.free(key.kid);
                        self.allocator.free(key.modulus);
                        self.allocator.free(key.exponent);
                        return JwksError.OutOfMemory;
                    };
                }

                pos = obj_end + 1;
            } else {
                pos += 1; // skip commas, etc.
            }
        }

        if (self.keys.items.len == 0) return JwksError.InvalidJwks;
    }

    /// Verify RS256 signature using cached keys.
    /// `kid` selects the key; if null, tries the first key.
    pub fn verifyRs256(
        self: *const JwksClient,
        header_b64: []const u8,
        payload_b64: []const u8,
        signature_b64: []const u8,
        kid: ?[]const u8,
    ) JwksError!void {
        const key = if (kid) |k| (self.findKey(k) orelse return JwksError.KeyNotFound) else if (self.keys.items.len > 0) &self.keys.items[0] else return JwksError.KeyNotFound;

        // Decode signature from base64url
        const sig_len = std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(signature_b64) catch return JwksError.InvalidSignature;

        // RS256 signature length = modulus length in bytes
        // Support common RSA key sizes: 2048, 3072, 4096 bits
        if (sig_len == 256) {
            return verifyRs256WithModLen(256, header_b64, payload_b64, signature_b64, key);
        } else if (sig_len == 384) {
            return verifyRs256WithModLen(384, header_b64, payload_b64, signature_b64, key);
        } else if (sig_len == 512) {
            return verifyRs256WithModLen(512, header_b64, payload_b64, signature_b64, key);
        } else {
            return JwksError.UnsupportedModulusLength;
        }
    }
};

fn verifyRs256WithModLen(
    comptime modulus_len: usize,
    header_b64: []const u8,
    payload_b64: []const u8,
    signature_b64: []const u8,
    key: *const CachedKey,
) JwksError!void {
    var sig_bytes: [modulus_len]u8 = undefined;
    std.base64.url_safe_no_pad.Decoder.decode(&sig_bytes, signature_b64) catch return JwksError.InvalidSignature;

    const public_key = rsa.PublicKey.fromBytes(key.exponent, key.modulus) catch return JwksError.InvalidKey;

    // RS256 = RSASSA-PKCS1-v1_5 with SHA-256
    // The message to verify is: header_b64 + "." + payload_b64
    rsa.PKCS1v1_5Signature.concatVerify(
        modulus_len,
        sig_bytes,
        &.{ header_b64, ".", payload_b64 },
        public_key,
        Sha256,
    ) catch return JwksError.InvalidSignature;
}

// ── JWKS JSON Parsing Helpers ───────────────────────────────────────────

/// Try to parse a single RSA signing key from a JSON object
fn tryParseRsaKey(allocator: Allocator, obj: []const u8) ?CachedKey {
    // Must be RSA key type
    const kty = extractJsonString(obj, "\"kty\"") orelse return null;
    if (!std.mem.eql(u8, kty, "RSA")) return null;

    // Must be signing key (use=sig) or no use specified
    if (extractJsonString(obj, "\"use\"")) |use| {
        if (!std.mem.eql(u8, use, "sig")) return null;
    }

    // Must have n (modulus) and e (exponent)
    const n_b64 = extractJsonString(obj, "\"n\"") orelse return null;
    const e_b64 = extractJsonString(obj, "\"e\"") orelse return null;

    // kid is optional but strongly recommended
    const kid_str = extractJsonString(obj, "\"kid\"") orelse "default";

    // Decode modulus and exponent from base64url
    const modulus = decodeBase64UrlAlloc(allocator, n_b64) orelse return null;
    errdefer allocator.free(modulus);

    // Strip leading zero bytes from modulus (ASN.1 integer padding)
    const modulus_trimmed = blk: {
        var offset: usize = 0;
        while (offset < modulus.len and modulus[offset] == 0) offset += 1;
        if (offset == 0) break :blk modulus;
        const trimmed = allocator.dupe(u8, modulus[offset..]) catch return null;
        allocator.free(modulus);
        break :blk trimmed;
    };

    const exponent = decodeBase64UrlAlloc(allocator, e_b64) orelse {
        allocator.free(modulus_trimmed);
        return null;
    };

    const kid = allocator.dupe(u8, kid_str) catch {
        allocator.free(modulus_trimmed);
        allocator.free(exponent);
        return null;
    };

    return .{
        .kid = kid,
        .modulus = modulus_trimmed,
        .exponent = exponent,
    };
}

fn decodeBase64UrlAlloc(allocator: Allocator, encoded: []const u8) ?[]u8 {
    const decoded_len = std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(encoded) catch return null;
    const decoded = allocator.alloc(u8, decoded_len) catch return null;
    std.base64.url_safe_no_pad.Decoder.decode(decoded, encoded) catch {
        allocator.free(decoded);
        return null;
    };
    return decoded;
}

fn extractJsonString(json: []const u8, key: []const u8) ?[]const u8 {
    const key_pos = std.mem.indexOf(u8, json, key) orelse return null;
    const after_key = json[key_pos + key.len ..];

    var i: usize = 0;
    while (i < after_key.len and (after_key[i] == ':' or after_key[i] == ' ' or after_key[i] == '\t')) : (i += 1) {}

    if (i >= after_key.len or after_key[i] != '"') return null;
    i += 1;

    const start = i;
    while (i < after_key.len) : (i += 1) {
        if (after_key[i] == '\\' and i + 1 < after_key.len) {
            i += 1;
            continue;
        }
        if (after_key[i] == '"') return after_key[start..i];
    }
    return null;
}

fn findMatchingBrace(json: []const u8, start: usize) ?usize {
    var depth: usize = 0;
    var i = start;
    var in_string = false;
    while (i < json.len) : (i += 1) {
        if (in_string) {
            if (json[i] == '\\' and i + 1 < json.len) {
                i += 1;
                continue;
            }
            if (json[i] == '"') in_string = false;
            continue;
        }
        switch (json[i]) {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if (depth == 0) return i;
            },
            else => {},
        }
    }
    return null;
}

fn isWhitespace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r';
}

// ═════════════════════════════════════════════════════════════════════════════
// Tests
// ═════════════════════════════════════════════════════════════════════════════

test "parseJwks extracts RSA signing keys" {
    const allocator = std.testing.allocator;
    const jwks_json =
        \\{"keys":[
        \\  {"kty":"RSA","use":"sig","kid":"test-key-1",
        \\   "n":"0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
        \\   "e":"AQAB"},
        \\  {"kty":"EC","use":"sig","kid":"ec-key","crv":"P-256",
        \\   "x":"abc","y":"def"}
        \\]}
    ;

    var client = JwksClient.init(allocator, "https://example.com/.well-known/jwks.json");
    defer client.deinit();

    try client.parseJwks(jwks_json);

    // Should have 1 RSA key (EC key filtered out)
    try std.testing.expectEqual(@as(usize, 1), client.keys.items.len);
    try std.testing.expectEqualStrings("test-key-1", client.keys.items[0].kid);

    // findKey should work
    try std.testing.expect(client.findKey("test-key-1") != null);
    try std.testing.expect(client.findKey("nonexistent") == null);
}

test "parseJwks multiple RSA keys" {
    const allocator = std.testing.allocator;
    const jwks_json =
        \\{"keys":[
        \\  {"kty":"RSA","use":"sig","kid":"key-1",
        \\   "n":"0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
        \\   "e":"AQAB"},
        \\  {"kty":"RSA","use":"sig","kid":"key-2",
        \\   "n":"0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
        \\   "e":"AQAB"}
        \\]}
    ;

    var client = JwksClient.init(allocator, "https://example.com/jwks");
    defer client.deinit();

    try client.parseJwks(jwks_json);
    try std.testing.expectEqual(@as(usize, 2), client.keys.items.len);
    try std.testing.expectEqualStrings("key-1", client.keys.items[0].kid);
    try std.testing.expectEqualStrings("key-2", client.keys.items[1].kid);
}

test "parseJwks rejects non-signing keys" {
    const allocator = std.testing.allocator;
    // Key with use=enc should be filtered out
    const jwks_json =
        \\{"keys":[
        \\  {"kty":"RSA","use":"enc","kid":"enc-key",
        \\   "n":"0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
        \\   "e":"AQAB"}
        \\]}
    ;

    var client = JwksClient.init(allocator, "https://example.com/jwks");
    defer client.deinit();

    const result = client.parseJwks(jwks_json);
    try std.testing.expectError(JwksError.InvalidJwks, result);
}

test "parseJwks rejects empty keys array" {
    const allocator = std.testing.allocator;

    var client = JwksClient.init(allocator, "https://example.com/jwks");
    defer client.deinit();

    try std.testing.expectError(JwksError.InvalidJwks, client.parseJwks("{\"keys\":[]}"));
    try std.testing.expectError(JwksError.InvalidJwks, client.parseJwks("not json"));
}

test "needsRefresh detects stale cache" {
    const allocator = std.testing.allocator;

    var client = JwksClient.init(allocator, "https://example.com/jwks");
    defer client.deinit();

    // Empty cache always needs refresh
    try std.testing.expect(client.needsRefresh(1000));

    // After "fetch" with keys, should not need refresh within TTL
    const jwks_json =
        \\{"keys":[{"kty":"RSA","use":"sig","kid":"k1",
        \\"n":"0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
        \\"e":"AQAB"}]}
    ;
    try client.parseJwks(jwks_json);
    client.last_fetch_ms = 50000;

    // Within TTL
    try std.testing.expect(!client.needsRefresh(50000 + 1000));
    // Beyond TTL
    try std.testing.expect(client.needsRefresh(50000 + JwksClient.DEFAULT_TTL_MS + 1));
}

test "extractJsonString handles various formats" {
    try std.testing.expectEqualStrings("RSA", extractJsonString("{\"kty\":\"RSA\"}", "\"kty\"").?);
    try std.testing.expectEqualStrings("RSA", extractJsonString("{\"kty\" : \"RSA\"}", "\"kty\"").?);
    try std.testing.expect(extractJsonString("{\"kty\":123}", "\"kty\"") == null);
    try std.testing.expect(extractJsonString("{}", "\"kty\"") == null);
}

test "findMatchingBrace" {
    try std.testing.expectEqual(@as(?usize, 8), findMatchingBrace("{\"a\":\"b\"}", 0));
    try std.testing.expectEqual(@as(?usize, 12), findMatchingBrace("{\"a\":{\"b\":1}}", 0));
    try std.testing.expect(findMatchingBrace("{unclosed", 0) == null);
}
