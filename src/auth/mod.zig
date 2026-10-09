//! Auth Module — API keys, session tokens, and key store.
//!
//! Nothing gives the dashboard a key store yet, so no listener authenticates
//! anyone.

pub const keys = @import("keys.zig");
pub const session = @import("session.zig");
pub const store = @import("store.zig");

// Re-export core types
pub const Role = keys.Role;
pub const ApiKey = keys.ApiKey;
pub const KeyStore = store.KeyStore;
pub const SessionClaims = session.SessionClaims;

/// Middleware result for HTTP request authentication.
pub const AuthResult = union(enum) {
    /// Authenticated via API key
    api_key: struct {
        role: keys.Role,
        key_id: []const u8,
    },
    /// Authenticated via session token
    session_token: session.SessionClaims,
    /// No authentication provided
    none,
    /// Authentication failed
    denied: []const u8,
};

/// Authenticate an HTTP request by checking headers.
/// Checks Authorization: Bearer (session token) first, then X-Api-Key.
pub fn authenticateHttpRequest(
    key_store: *const store.KeyStore,
    request: []const u8,
) AuthResult {
    // Try Authorization: Bearer <session_token> first (dashboard)
    if (extractHeader(request, "authorization: bearer ")) |bearer| {
        if (key_store.getSigningSecret()) |secret| {
            if (session.verifySessionToken(secret, bearer)) |claims| {
                return .{ .session_token = claims };
            } else |_| {
                return .{ .denied = "Invalid or expired session token" };
            }
        }
    }

    // Try X-Api-Key header
    if (extractHeader(request, "x-api-key: ")) |api_key| {
        if (key_store.validateKey(api_key)) |key| {
            return .{ .api_key = .{
                .role = key.role,
                .key_id = key.getId(),
            } };
        }
        return .{ .denied = "Invalid API key" };
    }

    return .none;
}

/// Extract a header value from raw HTTP request (case-insensitive key match).
pub fn extractHeader(request: []const u8, header_prefix: []const u8) ?[]const u8 {
    // Search line by line
    var lines = std.mem.splitSequence(u8, request, "\r\n");
    while (lines.next()) |line| {
        if (line.len >= header_prefix.len) {
            if (std.ascii.startsWithIgnoreCase(line, header_prefix)) {
                const value = std.mem.trim(u8, line[header_prefix.len..], " ");
                if (value.len > 0) return value;
            }
        }
    }
    return null;
}

/// Get the role from an AuthResult (if authenticated).
pub fn getRole(result: AuthResult) ?keys.Role {
    return switch (result) {
        .api_key => |ak| ak.role,
        .session_token => |sc| sc.role,
        .none, .denied => null,
    };
}

const std = @import("std");

// =============================================================================
// Tests
// =============================================================================

test "authenticateHttpRequest with API key" {
    var key_store = store.KeyStore.init(std.testing.allocator);
    defer key_store.deinit();

    const result = try keys.generateKey(std.testing.allocator, "test", .operator, 0);
    defer std.testing.allocator.free(result.plaintext);
    try key_store.putKey(result.key);

    // Build a fake HTTP request with X-Api-Key header
    var req_buf: [512]u8 = undefined;
    const req = std.fmt.bufPrint(&req_buf, "GET /api/v1/kv HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: {s}\r\n\r\n", .{result.plaintext}) catch unreachable;

    const auth = authenticateHttpRequest(&key_store, req);
    switch (auth) {
        .api_key => |ak| {
            try std.testing.expectEqual(keys.Role.operator, ak.role);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "authenticateHttpRequest with invalid key" {
    var key_store = store.KeyStore.init(std.testing.allocator);
    defer key_store.deinit();

    const req = "GET /api/v1/kv HTTP/1.1\r\nX-Api-Key: flo_sk_admin_invalid\r\n\r\n";
    const auth = authenticateHttpRequest(&key_store, req);
    switch (auth) {
        .denied => {},
        else => return error.TestUnexpectedResult,
    }
}

test "authenticateHttpRequest with no auth" {
    var key_store = store.KeyStore.init(std.testing.allocator);
    defer key_store.deinit();

    const req = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";
    const auth = authenticateHttpRequest(&key_store, req);
    switch (auth) {
        .none => {},
        else => return error.TestUnexpectedResult,
    }
}

test "getRole" {
    try std.testing.expectEqual(keys.Role.admin, getRole(.{ .api_key = .{ .role = .admin, .key_id = "test" } }).?);
    try std.testing.expect(getRole(.none) == null);
}

test {
    _ = keys;
    _ = session;
    _ = store;
}
