//! Auth Module — API keys, session tokens, and key store
//!
//! Three-layer auth model:
//! - Layer 1: CLI & Dashboard → API keys + roles + session tokens
//! - Layer 2: WebSocket → JWT from external IdP (handled in node/network/jwt.zig)
//! - Layer 3: Binary protocol → Open (trust-the-network)

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

/// Check if a role is authorized for an opcode.
/// Uses scope patterns from Role.scopes(): "*:*:*", "read:*:*", "write:kv:*", etc.
/// Opcode categories are determined by the high nibble of the opcode byte.
pub fn matchScope(role: keys.Role, op_code: u16) bool {
    const scopes = role.scopes();
    for (scopes) |scope| {
        if (std.mem.eql(u8, scope, "*:*:*")) return true;
        // Parse scope pattern: action:subsystem:resource
        var parts = std.mem.splitScalar(u8, scope, ':');
        const action = parts.next() orelse continue;
        const subsystem = parts.next() orelse continue;
        // Determine opcode action (read vs write) from opcode conventions
        const is_read = isReadOpcode(op_code);
        const op_action: []const u8 = if (is_read) "read" else "write";
        if (!std.mem.eql(u8, action, "*") and !std.mem.eql(u8, action, op_action)) continue;
        // Match subsystem
        const op_subsystem = opcodeSubsystem(op_code);
        if (!std.mem.eql(u8, subsystem, "*") and !std.mem.eql(u8, subsystem, op_subsystem)) continue;
        return true;
    }
    return false;
}

/// Classify opcode as read or write based on naming conventions.
fn isReadOpcode(op_code: u16) bool {
    // System/ping/auth are reads
    return switch (op_code) {
        0x001, 0x002, 0x003 => true, // pong, error_response, auth
        else => false, // Default: treat unknown as write (secure default)
    };
}

/// Map opcode to subsystem name for scope matching.
/// Layout: Infra(0x0__), Data(0x1__-0x2__), Compute(0x3__)
fn opcodeSubsystem(op_code: u16) []const u8 {
    return switch (op_code) {
        0x000...0x00F => "system",
        0x010...0x02F => "namespace",
        0x030...0x04F => "cluster",
        // 0x050-0x0FF = infra reserve
        0x100...0x12F => "kv",
        0x130...0x16F => "stream", // streams + consumer groups
        0x170...0x19F => "queue",
        0x1A0...0x1BF => "ts",
        // 0x1C0-0x2FF = data reserve (vectors, documents, geo, counters)
        0x300...0x31F => "actions",
        0x320...0x33F => "worker",
        0x340...0x35F => "workflow",
        0x360...0x37F => "processing",
        // 0x380-0x3FF = compute reserve (emit, future compute)
        else => "unknown",
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

test "matchScope admin allows everything" {
    try std.testing.expect(matchScope(.admin, 0x101)); // kv_get
    try std.testing.expect(matchScope(.admin, 0x100)); // kv_put
    try std.testing.expect(matchScope(.admin, 0x035)); // cluster_add_node
    try std.testing.expect(matchScope(.admin, 0x1A0)); // ts_write
}

test "matchScope viewer allows reads only" {
    // Viewer has "read:*:*" — system opcodes (reads) should pass
    try std.testing.expect(matchScope(.viewer, 0x001)); // pong
    // Viewer should not be able to write
    try std.testing.expect(!matchScope(.viewer, 0x100)); // kv_put (write)
}

test "matchScope operator allows kv/stream/queue/ts writes" {
    try std.testing.expect(matchScope(.operator, 0x100)); // kv_put (write:kv:*)
    try std.testing.expect(matchScope(.operator, 0x130)); // stream_append (write:stream:*)
    try std.testing.expect(matchScope(.operator, 0x170)); // queue_enqueue (write:queue:*)
    try std.testing.expect(matchScope(.operator, 0x1A0)); // ts_write (write:ts:*)
    // Operator should not have cluster access
    try std.testing.expect(!matchScope(.operator, 0x035)); // cluster_add_node
}

test "opcodeSubsystem ranges" {
    try std.testing.expectEqualStrings("system", opcodeSubsystem(0x003));
    try std.testing.expectEqualStrings("namespace", opcodeSubsystem(0x010));
    try std.testing.expectEqualStrings("cluster", opcodeSubsystem(0x030));
    try std.testing.expectEqualStrings("kv", opcodeSubsystem(0x101));
    try std.testing.expectEqualStrings("stream", opcodeSubsystem(0x130));
    try std.testing.expectEqualStrings("stream", opcodeSubsystem(0x150));
    try std.testing.expectEqualStrings("queue", opcodeSubsystem(0x170));
    try std.testing.expectEqualStrings("ts", opcodeSubsystem(0x1A0));
    try std.testing.expectEqualStrings("actions", opcodeSubsystem(0x300));
    try std.testing.expectEqualStrings("worker", opcodeSubsystem(0x320));
    try std.testing.expectEqualStrings("workflow", opcodeSubsystem(0x340));
    try std.testing.expectEqualStrings("processing", opcodeSubsystem(0x360));
}

test {
    _ = keys;
    _ = session;
    _ = store;
}
