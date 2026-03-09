//! WebSocket Connection Handler
//!
//! Bridges the RFC 6455 frame parser to the new Dispatcher architecture.
//! WebSocket clients send Flo-Proto binary messages inside WS binary frames.
//!
//! Connection lifecycle:
//!   1. HTTP Upgrade handshake (auth token extracted from header/query)
//!   2. Binary frames → proto.Request → Dispatcher → proto.Response → WS frame
//!   3. Subscriptions via shard waiter lists (server-push via WS frames)
//!   4. Heartbeat ping/pong for connection liveness
//!
//! Unlike the old monolithic handler, this module is a thin protocol bridge.
//! All business logic lives in the Dispatcher handlers. The WS handler only:
//! - Manages the WS frame layer (upgrade, frame parse/build, close)
//! - Enforces an opcode whitelist (security surface reduction)
//! - Tracks rate limits per session
//! - Tracks subscriptions for cleanup on disconnect

const std = @import("std");
const Allocator = std.mem.Allocator;
const ws = @import("network/websocket.zig");
const proto = @import("../protocol/proto.zig");
const auth = @import("../auth/mod.zig");
const auth_session = @import("../auth/session.zig");

// =============================================================================
// Opcode Whitelist
// =============================================================================

/// Configurable whitelist of opcodes permitted over WebSocket.
/// Defaults allow common read/write/subscribe operations.
/// Administrative and cluster opcodes are denied by default.
pub const OpWhitelist = struct {
    allowed: [256]bool,

    /// Create whitelist with safe defaults for browser clients
    pub fn initDefault() OpWhitelist {
        var wl = OpWhitelist{ .allowed = [_]bool{false} ** 256 };

        // System
        wl.allow(.ping);

        // Streams — read, write, subscribe
        wl.allow(.stream_append);
        wl.allow(.stream_read);
        wl.allow(.stream_info);
        wl.allow(.stream_subscribe);
        wl.allow(.stream_unsubscribe);
        wl.allow(.stream_list);
        wl.allow(.stream_create);
        wl.allow(.stream_group_create);
        wl.allow(.stream_group_read);
        wl.allow(.stream_group_ack);

        // KV — get, put, delete
        wl.allow(.kv_get);
        wl.allow(.kv_put);
        wl.allow(.kv_delete);
        wl.allow(.kv_scan);

        // Queues — enqueue, dequeue, complete
        wl.allow(.queue_enqueue);
        wl.allow(.queue_dequeue);
        wl.allow(.queue_complete);
        wl.allow(.queue_stats);

        return wl;
    }

    /// Create a permissive whitelist (all opcodes allowed)
    pub fn initPermissive() OpWhitelist {
        return .{ .allowed = [_]bool{true} ** 256 };
    }

    /// Check if an opcode is allowed
    pub fn isAllowed(self: *const OpWhitelist, opcode: u8) bool {
        return self.allowed[opcode];
    }

    /// Allow a specific opcode
    pub fn allow(self: *OpWhitelist, opcode: proto.OpCode) void {
        self.allowed[@intFromEnum(opcode)] = true;
    }

    /// Deny a specific opcode
    pub fn deny(self: *OpWhitelist, opcode: proto.OpCode) void {
        self.allowed[@intFromEnum(opcode)] = false;
    }
};

// =============================================================================
// Rate Limiter
// =============================================================================

/// Simple sliding-window rate limiter per WebSocket session
pub const RateLimit = struct {
    window_start: i64 = 0,
    request_count: u32 = 0,
    max_requests: u32 = 1000,
    window_ms: i64 = 1000,

    /// Check and record a request. Returns true if allowed, false if rate limited.
    pub fn check(self: *RateLimit, now_ms: i64) bool {
        if (now_ms - self.window_start >= self.window_ms) {
            // New window
            self.window_start = now_ms;
            self.request_count = 1;
            return true;
        }
        self.request_count += 1;
        return self.request_count <= self.max_requests;
    }

    /// Reset the rate limiter
    pub fn reset(self: *RateLimit) void {
        self.window_start = 0;
        self.request_count = 0;
    }
};

// =============================================================================
// WebSocket Session
// =============================================================================

/// Per-connection WebSocket state. Created when a connection upgrades to WS.
pub const WebSocketSession = struct {
    state: State,
    handler: ws.WebSocketHandler,
    rate_limit: RateLimit,
    subscriptions: std.ArrayListUnmanaged(Subscription),
    user_id: ?[]const u8,
    namespace: []const u8,
    last_ping_sent: i64,
    last_pong_received: i64,

    pub const State = enum {
        awaiting_upgrade,
        connected,
        closing,
        closed,
    };

    /// Tracked subscription for cleanup on disconnect
    pub const Subscription = struct {
        stream_name: []const u8,
        partition: u32,
    };

    pub fn init(allocator: Allocator) WebSocketSession {
        return .{
            .state = .awaiting_upgrade,
            .handler = ws.WebSocketHandler.init(allocator),
            .rate_limit = .{},
            .subscriptions = .{},
            .user_id = null,
            .namespace = "default",
            .last_ping_sent = 0,
            .last_pong_received = std.time.milliTimestamp(),
        };
    }

    pub fn deinit(self: *WebSocketSession, allocator: Allocator) void {
        self.handler.deinit();
        for (self.subscriptions.items) |sub| {
            allocator.free(sub.stream_name);
        }
        self.subscriptions.deinit(allocator);
        if (self.user_id) |uid| allocator.free(uid);
    }
};

// =============================================================================
// Upgrade Handshake
// =============================================================================

/// Result of an HTTP-to-WebSocket upgrade attempt
pub const UpgradeResult = struct {
    /// The 101 Switching Protocols response to send back
    response: []const u8,
    /// Extracted auth token (if present in Authorization header or ?token= query)
    auth_token: ?[]const u8,
};

/// Parse HTTP upgrade request and generate the 101 response.
/// Returns null if the request is not a valid WebSocket upgrade.
pub fn parseUpgrade(allocator: Allocator, request: []const u8) !?UpgradeResult {
    // Must contain the upgrade marker
    if (std.mem.indexOf(u8, request, "Upgrade: websocket") == null and
        std.mem.indexOf(u8, request, "Upgrade: WebSocket") == null)
    {
        return null;
    }

    // Extract Sec-WebSocket-Key
    const key_header = "Sec-WebSocket-Key: ";
    const key_start = std.mem.indexOf(u8, request, key_header) orelse return null;
    const key_line_start = key_start + key_header.len;
    const key_line_end = std.mem.indexOfPos(u8, request, key_line_start, "\r\n") orelse return null;
    const client_key = request[key_line_start..key_line_end];

    // Generate accept key
    const accept_key = ws.generateAcceptKey(client_key);

    // Extract auth token from Authorization header or ?token= query
    var auth_token: ?[]const u8 = null;

    if (std.mem.indexOf(u8, request, "Authorization: Bearer ")) |bearer_start| {
        const token_start = bearer_start + "Authorization: Bearer ".len;
        const token_end = std.mem.indexOfPos(u8, request, token_start, "\r\n") orelse request.len;
        auth_token = request[token_start..token_end];
    } else if (std.mem.indexOf(u8, request, "?token=")) |query_start| {
        const token_start = query_start + "?token=".len;
        // Token ends at space, &, or \r
        var token_end = token_start;
        while (token_end < request.len and
            request[token_end] != ' ' and
            request[token_end] != '&' and
            request[token_end] != '\r')
        {
            token_end += 1;
        }
        auth_token = request[token_start..token_end];
    }

    // Build response
    const response = try std.fmt.allocPrint(allocator,
        "HTTP/1.1 101 Switching Protocols\r\n" ++
        "Upgrade: websocket\r\n" ++
        "Connection: Upgrade\r\n" ++
        "Sec-WebSocket-Accept: {s}\r\n" ++
        "\r\n",
        .{accept_key},
    );

    return .{
        .response = response,
        .auth_token = auth_token,
    };
}

/// Authenticate a WebSocket session using the token extracted during upgrade.
/// If key_store is available, validates the token as an API key or session token.
/// Returns the auth result for role/scope enforcement.
pub fn authenticateSession(key_store: ?*const auth.KeyStore, token: ?[]const u8) auth.AuthResult {
    const ks = key_store orelse return .none;
    const tok = token orelse return .none;

    // Try as session token first (Bearer JWT)
    if (ks.getSigningSecret()) |secret| {
        if (auth_session.verifySessionToken(secret, tok)) |claims| {
            return .{ .session_token = claims };
        } else |_| {}
    }

    // Try as API key
    if (ks.validateKey(tok)) |key| {
        return .{ .api_key = .{
            .role = key.role,
            .key_id = key.getId(),
        } };
    }

    return .{ .denied = "Invalid token" };
}

// =============================================================================
// Message Processing
// =============================================================================

/// Result of processing incoming WS data
pub const HandleResult = union(enum) {
    /// A proto.Request was extracted — dispatch it
    request: RequestInfo,
    /// Need to send a pong response
    pong: []const u8,
    /// Connection is closing — send close frame and clean up
    closing: ws.CloseCode,
    /// Need more data
    need_more: void,
    /// Protocol error
    err: Error,

    pub const RequestInfo = struct {
        opcode: proto.OpCode,
        payload: []const u8,
        request_id: u64,
    };
};

pub const Error = error{
    RateLimited,
    OpcodeDenied,
    InvalidRequest,
    ConnectionClosed,
};

/// Process incoming data on a WebSocket connection.
/// Returns actions the caller should take (dispatch request, send pong, close, etc.)
pub fn processData(
    session: *WebSocketSession,
    data: []u8,
    whitelist: *const OpWhitelist,
) HandleResult {
    switch (session.state) {
        .connected => {},
        .closing, .closed => return .{ .err = Error.ConnectionClosed },
        .awaiting_upgrade => return .{ .err = Error.InvalidRequest },
    }

    // Parse WS frame
    const frame_result = session.handler.processFrame(data);
    if (frame_result.consumed == 0) {
        return switch (frame_result.result) {
            .need_more => .{ .need_more = {} },
            .err => .{ .err = Error.InvalidRequest },
            else => .{ .need_more = {} },
        };
    }

    return switch (frame_result.result) {
        .binary_payload => |payload| blk: {
            // Rate limit check
            const now = std.time.milliTimestamp();
            if (!session.rate_limit.check(now)) {
                break :blk .{ .err = Error.RateLimited };
            }

            // Parse the binary payload as a Flo-Proto request header
            if (payload.len < @sizeOf(proto.RequestHeader)) {
                break :blk .{ .err = Error.InvalidRequest };
            }

            const header_bytes = payload[0..@sizeOf(proto.RequestHeader)];
            const header: *const proto.RequestHeader = @ptrCast(@alignCast(header_bytes.ptr));

            // Opcode whitelist check
            if (!whitelist.isAllowed(header.op_code)) {
                break :blk .{ .err = Error.OpcodeDenied };
            }

            const opcode: proto.OpCode = @enumFromInt(header.op_code);
            const request_payload = payload[@sizeOf(proto.RequestHeader)..];

            break :blk .{ .request = .{
                .opcode = opcode,
                .payload = request_payload,
                .request_id = header.request_id,
            } };
        },
        .ping => |ping_data| .{ .pong = ping_data },
        .pong => blk: {
            session.last_pong_received = std.time.milliTimestamp();
            break :blk .{ .need_more = {} };
        },
        .close => |code| blk: {
            session.state = .closing;
            break :blk .{ .closing = code };
        },
        .need_more => .{ .need_more = {} },
        .err => .{ .err = Error.InvalidRequest },
    };
}

/// Wrap a proto.Response in a WebSocket binary frame for sending
pub fn wrapResponse(allocator: Allocator, response_data: []const u8) ![]u8 {
    return ws.buildFrame(allocator, .binary, response_data, true);
}

/// Build an error response wrapped in a WS binary frame
pub fn wrapError(allocator: Allocator, request_id: u64, status: proto.StatusCode, message: []const u8) ![]u8 {
    // Build a minimal error response payload: request_id + status_code + message
    const payload_len = 8 + 1 + message.len;
    const payload = try allocator.alloc(u8, payload_len);
    defer allocator.free(payload);

    std.mem.writeInt(u64, payload[0..8], request_id, .little);
    payload[8] = @intFromEnum(status);
    @memcpy(payload[9..], message);

    return ws.buildFrame(allocator, .binary, payload, true);
}

/// Check if a session needs a heartbeat ping, or has timed out
pub const HeartbeatAction = enum {
    /// Session is healthy, no action needed
    ok,
    /// Send a ping frame
    send_ping,
    /// Session timed out — close it
    timed_out,
};

pub fn checkHeartbeat(session: *const WebSocketSession, now_ms: i64, ping_interval_ms: i64, timeout_ms: i64) HeartbeatAction {
    if (session.state != .connected) return .ok;

    // Check if pong was received within timeout
    if (session.last_ping_sent > 0 and
        now_ms - session.last_pong_received > timeout_ms)
    {
        return .timed_out;
    }

    // Check if it's time to send a ping
    if (now_ms - session.last_ping_sent >= ping_interval_ms) {
        return .send_ping;
    }

    return .ok;
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "OpWhitelist default allows common operations" {
    const wl = OpWhitelist.initDefault();

    // Allowed by default
    try testing.expect(wl.isAllowed(@intFromEnum(proto.OpCode.ping)));
    try testing.expect(wl.isAllowed(@intFromEnum(proto.OpCode.kv_get)));
    try testing.expect(wl.isAllowed(@intFromEnum(proto.OpCode.kv_put)));
    try testing.expect(wl.isAllowed(@intFromEnum(proto.OpCode.stream_append)));
    try testing.expect(wl.isAllowed(@intFromEnum(proto.OpCode.stream_read)));
    try testing.expect(wl.isAllowed(@intFromEnum(proto.OpCode.stream_subscribe)));
    try testing.expect(wl.isAllowed(@intFromEnum(proto.OpCode.queue_enqueue)));
    try testing.expect(wl.isAllowed(@intFromEnum(proto.OpCode.queue_dequeue)));

    // Denied by default (admin/cluster operations)
    try testing.expect(!wl.isAllowed(@intFromEnum(proto.OpCode.auth)));
    try testing.expect(!wl.isAllowed(@intFromEnum(proto.OpCode.error_response)));
}

test "OpWhitelist allow/deny operations" {
    var wl = OpWhitelist.initDefault();

    // Deny a default-allowed op
    try testing.expect(wl.isAllowed(@intFromEnum(proto.OpCode.kv_delete)));
    wl.deny(.kv_delete);
    try testing.expect(!wl.isAllowed(@intFromEnum(proto.OpCode.kv_delete)));

    // Allow a default-denied op
    try testing.expect(!wl.isAllowed(@intFromEnum(proto.OpCode.auth)));
    wl.allow(.auth);
    try testing.expect(wl.isAllowed(@intFromEnum(proto.OpCode.auth)));
}

test "OpWhitelist permissive allows all" {
    const wl = OpWhitelist.initPermissive();
    try testing.expect(wl.isAllowed(0x00));
    try testing.expect(wl.isAllowed(0xFF));
    try testing.expect(wl.isAllowed(@intFromEnum(proto.OpCode.auth)));
}

test "RateLimit basic window" {
    var rl = RateLimit{
        .max_requests = 3,
        .window_ms = 1000,
    };

    // First 3 should be allowed
    try testing.expect(rl.check(1000));
    try testing.expect(rl.check(1000));
    try testing.expect(rl.check(1000));

    // 4th should be denied
    try testing.expect(!rl.check(1000));

    // New window resets
    try testing.expect(rl.check(2001));
    try testing.expectEqual(@as(u32, 1), rl.request_count);
}

test "RateLimit reset" {
    var rl = RateLimit{ .max_requests = 1, .window_ms = 1000 };

    try testing.expect(rl.check(1000));
    try testing.expect(!rl.check(1000));

    rl.reset();
    try testing.expectEqual(@as(u32, 0), rl.request_count);
    try testing.expectEqual(@as(i64, 0), rl.window_start);
}

test "parseUpgrade valid request" {
    const allocator = testing.allocator;

    const request =
        "GET /ws HTTP/1.1\r\n" ++
        "Host: localhost:4444\r\n" ++
        "Upgrade: websocket\r\n" ++
        "Connection: Upgrade\r\n" ++
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" ++
        "Sec-WebSocket-Version: 13\r\n" ++
        "\r\n";

    const result = try parseUpgrade(allocator, request);
    try testing.expect(result != null);

    const upgrade = result.?;
    defer allocator.free(upgrade.response);

    try testing.expect(std.mem.indexOf(u8, upgrade.response, "101 Switching Protocols") != null);
    try testing.expect(std.mem.indexOf(u8, upgrade.response, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") != null);
    try testing.expect(upgrade.auth_token == null);
}

test "parseUpgrade with bearer token" {
    const allocator = testing.allocator;

    const request =
        "GET /ws HTTP/1.1\r\n" ++
        "Upgrade: websocket\r\n" ++
        "Connection: Upgrade\r\n" ++
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" ++
        "Authorization: Bearer mytoken123\r\n" ++
        "\r\n";

    const result = try parseUpgrade(allocator, request);
    try testing.expect(result != null);

    const upgrade = result.?;
    defer allocator.free(upgrade.response);

    try testing.expectEqualStrings("mytoken123", upgrade.auth_token.?);
}

test "parseUpgrade with query token" {
    const allocator = testing.allocator;

    const request =
        "GET /ws?token=abc123 HTTP/1.1\r\n" ++
        "Upgrade: websocket\r\n" ++
        "Connection: Upgrade\r\n" ++
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" ++
        "\r\n";

    const result = try parseUpgrade(allocator, request);
    const upgrade = result.?;
    defer allocator.free(upgrade.response);

    try testing.expectEqualStrings("abc123", upgrade.auth_token.?);
}

test "parseUpgrade not a websocket request" {
    const allocator = testing.allocator;

    const request =
        "GET /api/v1/stats HTTP/1.1\r\n" ++
        "Host: localhost\r\n" ++
        "\r\n";

    const result = try parseUpgrade(allocator, request);
    try testing.expect(result == null);
}

test "WebSocketSession init and deinit" {
    const allocator = testing.allocator;

    var session = WebSocketSession.init(allocator);
    defer session.deinit(allocator);

    try testing.expectEqual(WebSocketSession.State.awaiting_upgrade, session.state);
    try testing.expect(session.user_id == null);
    try testing.expectEqualStrings("default", session.namespace);
}

test "wrapResponse builds valid WS frame" {
    const allocator = testing.allocator;

    const payload = "hello";
    const frame = try wrapResponse(allocator, payload);
    defer allocator.free(frame);

    // Parse the frame back
    const header = try ws.parseFrameHeader(frame);
    try testing.expect(header.fin);
    try testing.expectEqual(ws.Opcode.binary, header.opcode);
    try testing.expect(!header.masked); // Server frames are not masked
    try testing.expectEqual(@as(u64, 5), header.payload_len);

    // Check payload
    try testing.expectEqualStrings("hello", frame[header.header_len..]);
}

test "checkHeartbeat detects timeout" {
    var session = WebSocketSession.init(testing.allocator);
    defer session.deinit(testing.allocator);
    session.state = .connected;

    // No ping sent yet — should suggest sending one
    session.last_ping_sent = 0;
    session.last_pong_received = 1000;
    const action1 = checkHeartbeat(&session, 31000, 30000, 60000);
    try testing.expectEqual(HeartbeatAction.send_ping, action1);

    // Ping sent, pong received recently — ok
    session.last_ping_sent = 30000;
    session.last_pong_received = 31000;
    const action2 = checkHeartbeat(&session, 32000, 30000, 60000);
    try testing.expectEqual(HeartbeatAction.ok, action2);

    // Ping sent long ago, no pong — timed out
    session.last_ping_sent = 10000;
    session.last_pong_received = 5000;
    const action3 = checkHeartbeat(&session, 70000, 30000, 60000);
    try testing.expectEqual(HeartbeatAction.timed_out, action3);
}
