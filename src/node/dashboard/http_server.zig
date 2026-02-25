//! Dashboard HTTP Server
//!
//! Lightweight HTTP server for the Flo web dashboard.
//! Serves the REST API and embedded static assets (React SPA).
//!
//! Security Model: Network-level (like Redis/Nomad)
//! - Default bind to localhost for safe out-of-box experience
//! - Optional admin_token for basic protection behind VPN/proxy
//!
//! Endpoints:
//! - GET /health         - Health check (always public)
//! - GET /api/v1/*       - REST API for dashboard data (requires admin_token if set)
//! - GET /api/v1/kv/namespaces/:ns/keys/:key/watch — SSE live updates (stub)
//! - GET /api/v1/workflow/namespaces/:ns/runs/:run_id/watch — SSE workflow run updates (stub)
//! - GET /*              - Embedded static files (requires admin_token if set)
//!
//! The dashboard assets are embedded at compile time from web/dist/

const std = @import("std");
const Allocator = std.mem.Allocator;
const api = @import("api.zig");
const assets = @import("assets.zig");
const http = @import("../../util/http/mod.zig");
const DashboardContext = api.DashboardContext;

// =============================================================================
// Server Configuration and Implementation
// =============================================================================

/// Configuration for the dashboard server
pub const DashboardServerConfig = struct {
    port: u16 = 9080,
    bind: []const u8 = "127.0.0.1", // Localhost only by default (safe)
    cors_origins: []const u8 = "*",
    admin_token: []const u8 = "", // Empty = no auth required
};

pub const DashboardServer = struct {
    const Self = @This();

    allocator: Allocator,
    config: DashboardServerConfig,
    ctx: *DashboardContext,
    listener: ?std.posix.socket_t = null,
    thread: ?std.Thread = null,
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(
        allocator: Allocator,
        config: DashboardServerConfig,
        ctx: *DashboardContext,
    ) Self {
        return .{
            .allocator = allocator,
            .config = config,
            .ctx = ctx,
        };
    }

    pub fn deinit(self: *Self) void {
        self.stop();
        if (self.listener) |sock| {
            std.posix.close(sock);
            self.listener = null;
        }
    }

    pub fn start(self: *Self) !void {
        // Create listening socket
        const sock = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        errdefer std.posix.close(sock);

        // Allow address reuse
        const one: u32 = 1;
        try std.posix.setsockopt(sock, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, std.mem.asBytes(&one));

        // Parse bind address
        const bind_ip = parseIpAddress(self.config.bind) catch |err| {
            std.log.err("Invalid dashboard bind address '{s}': {}", .{ self.config.bind, err });
            return error.InvalidBindAddress;
        };

        // Bind to configured address and port
        const addr = std.net.Address.initIp4(bind_ip, self.config.port);
        try std.posix.bind(sock, &addr.any, addr.getOsSockLen());
        try std.posix.listen(sock, 64);

        self.listener = sock;
        self.running.store(true, .release);

        // Start server thread
        self.thread = try std.Thread.spawn(.{}, serverLoop, .{self});

        const auth_status = if (self.config.admin_token.len > 0) " (admin_token required)" else " (no auth)";
        std.log.info("Dashboard server listening on {s}:{d}{s}", .{ self.config.bind, self.config.port, auth_status });
    }

    pub fn stop(self: *Self) void {
        if (!self.running.load(.acquire)) return;

        self.running.store(false, .release);

        // Close listener to unblock accept
        if (self.listener) |sock| {
            // Shutdown first to interrupt any blocked accept()
            std.posix.shutdown(sock, .both) catch {};
            std.posix.close(sock);
            self.listener = null;
        }

        // Wait for thread (should exit quickly now)
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }

        std.log.info("Dashboard server stopped", .{});
    }

    fn serverLoop(self: *Self) void {
        while (self.running.load(.acquire)) {
            const listener = self.listener orelse break;

            var client_addr: std.posix.sockaddr = undefined;
            var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);

            const client = std.posix.accept(listener, &client_addr, &addr_len, 0) catch |err| {
                if (err == error.ConnectionAborted or err == error.SocketNotListening) {
                    // Server shutting down
                    break;
                }
                continue;
            };

            // Handle request (simple synchronous handling for now)
            self.handleConnection(client);
        }
    }

    fn handleConnection(self: *Self, client: std.posix.socket_t) void {
        defer {
            // Graceful close: use SO_LINGER to ensure send buffer is flushed
            // before the kernel sends FIN. Without this, close() on a socket
            // with buffered data sends RST ("Connection reset by peer")
            // which breaks Docker port forwarding.
            const linger = extern struct { l_onoff: c_int, l_linger: c_int }{ .l_onoff = 1, .l_linger = 2 };
            std.posix.setsockopt(client, std.posix.SOL.SOCKET, std.posix.SO.LINGER, std.mem.asBytes(&linger)) catch {};
            std.posix.close(client);
        }

        var buf: [8192]u8 = undefined;
        const n = std.posix.read(client, &buf) catch return;
        if (n == 0) return;

        const request = buf[0..n];

        // Parse HTTP request using shared primitives
        const parsed = http.parseRequest(request) orelse {
            self.sendError(client, .bad_request, "Invalid HTTP request");
            return;
        };

        // Extract body (everything after \r\n\r\n)
        const body: []const u8 = if (std.mem.indexOf(u8, request, "\r\n\r\n")) |headers_end|
            request[headers_end + 4 ..]
        else
            "";

        // Add CORS headers if configured
        const cors_headers = self.getCorsHeaders(request);

        // Handle preflight OPTIONS request
        if (parsed.method == .OPTIONS) {
            self.sendCorsPreflightResponse(client, cors_headers);
            return;
        }

        // Check admin token for protected paths (skip /health which is always public)
        if (!std.mem.eql(u8, parsed.path, "/health")) {
            if (!self.validateAdminToken(request)) {
                self.sendError(client, .unauthorized, "Unauthorized: invalid or missing admin token");
                return;
            }
        }

        // ---- SSE Watch paths: detect and respond with "not wired" ----
        // SSE support will be wired in a future phase via shard inbox.
        if (parsed.method == .GET) {
            if (parsed.pathAfter("/api/v1/")) |api_path| {
                if (parseSSEWatchPath(api_path) != null or parseWorkflowSSEPath(api_path) != null) {
                    self.sendResponse(client, .ok, .json, "{\"error\":\"SSE not yet wired to shard inbox\"}", cors_headers);
                    return;
                }
            }
        }

        // Route request
        if (parsed.pathStartsWith("/api/v1/")) {
            self.handleApiRequest(client, parsed, body, cors_headers);
        } else if (std.mem.eql(u8, parsed.path, "/health")) {
            self.sendResponse(client, .ok, .json, "{\"status\":\"ok\"}", cors_headers);
        } else {
            // Serve embedded static assets
            self.handleStaticRequest(client, parsed.path, cors_headers);
        }
    }

    fn handleApiRequest(self: *Self, client: std.posix.socket_t, parsed: http.ParsedRequest, body: []const u8, cors_headers: ?[]const u8) void {
        const path = parsed.pathAfter("/api/v1/") orelse "";

        const response = api.handleRequest(self.allocator, parsed.method, path, parsed.query_string, body, self.ctx) catch |err| {
            std.log.warn("Dashboard API error: {}", .{err});
            self.sendError(client, .internal_server_error, "Internal server error");
            return;
        };
        defer self.allocator.free(response);

        self.sendResponse(client, .ok, .json, response, cors_headers);
    }

    fn handleStaticRequest(self: *Self, client: std.posix.socket_t, path: []const u8, cors_headers: ?[]const u8) void {
        // Security: prevent path traversal
        if (std.mem.indexOf(u8, path, "..") != null) {
            self.sendError(client, .bad_request, "Invalid path");
            return;
        }

        // Try to get embedded asset (handles SPA routing internally)
        if (assets.get(path)) |asset| {
            self.sendResponseRaw(client, .ok, asset.mime_type, asset.content, cors_headers);
            return;
        }

        // No assets available
        self.sendError(client, .not_found, "Dashboard not available");
    }

    fn sendResponse(self: *Self, client: std.posix.socket_t, status: http.StatusCode, content_type: http.ContentType, body_data: []const u8, cors_headers: ?[]const u8) void {
        self.sendResponseRaw(client, status, content_type.toString(), body_data, cors_headers);
    }

    fn sendResponseRaw(_: *Self, client: std.posix.socket_t, status: http.StatusCode, content_type: []const u8, body_data: []const u8, cors_headers: ?[]const u8) void {
        var hdr_buf: [1024]u8 = undefined;
        const cors = cors_headers orelse "";
        const response = std.fmt.bufPrint(
            &hdr_buf,
            "HTTP/1.1 {s}\r\n" ++
                "Content-Type: {s}\r\n" ++
                "Content-Length: {d}\r\n" ++
                "{s}" ++
                "Connection: close\r\n" ++
                "\r\n",
            .{ status.statusLine(), content_type, body_data.len, cors },
        ) catch return;

        _ = std.posix.write(client, response) catch return;
        _ = std.posix.write(client, body_data) catch return;
    }

    fn sendError(self: *Self, client: std.posix.socket_t, status: http.StatusCode, message: []const u8) void {
        var json_buf: [256]u8 = undefined;
        const body_data = std.fmt.bufPrint(&json_buf, "{{\"error\":\"{s}\"}}", .{message}) catch return;
        self.sendResponse(client, status, .json, body_data, null);
    }

    fn getCorsHeaders(self: *Self, request: []const u8) ?[]const u8 {
        if (self.config.cors_origins.len == 0) return null;

        // Extract Origin header from request
        var lines = std.mem.splitSequence(u8, request, "\r\n");
        while (lines.next()) |line| {
            if (std.ascii.startsWithIgnoreCase(line, "origin:")) {
                const origin = std.mem.trim(u8, line[7..], " ");
                // Check if origin is allowed
                if (std.mem.indexOf(u8, self.config.cors_origins, origin) != null) {
                    return "Access-Control-Allow-Origin: *\r\n" ++
                        "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n" ++
                        "Access-Control-Allow-Headers: Content-Type\r\n";
                }
            }
        }
        return null;
    }

    fn sendCorsPreflightResponse(_: *Self, client: std.posix.socket_t, cors_headers: ?[]const u8) void {
        const cors = cors_headers orelse "";
        var hdr_buf: [512]u8 = undefined;
        const response = std.fmt.bufPrint(
            &hdr_buf,
            "HTTP/1.1 204 No Content\r\n" ++
                "{s}" ++
                "Content-Length: 0\r\n" ++
                "Connection: close\r\n" ++
                "\r\n",
            .{cors},
        ) catch return;
        _ = std.posix.write(client, response) catch return;
    }

    /// Validate admin token if configured.
    /// Returns true if:
    /// - No admin_token is configured (empty string)
    /// - Token matches via query param ?token=xxx
    /// - Token matches via header X-Admin-Token: xxx
    fn validateAdminToken(self: *Self, request: []const u8) bool {
        // No token configured = allow all
        if (self.config.admin_token.len == 0) return true;

        // Check query param: ?token=xxx
        if (std.mem.indexOf(u8, request, "?token=")) |pos| {
            const token_start = pos + 7;
            // Find end of token (space, &, or newline)
            var token_end = token_start;
            while (token_end < request.len) : (token_end += 1) {
                const c = request[token_end];
                if (c == ' ' or c == '&' or c == '\r' or c == '\n') break;
            }
            const token = request[token_start..token_end];
            if (std.mem.eql(u8, token, self.config.admin_token)) return true;
        }

        // Check header: X-Admin-Token: xxx
        var lines = std.mem.splitSequence(u8, request, "\r\n");
        while (lines.next()) |line| {
            if (std.ascii.startsWithIgnoreCase(line, "x-admin-token:")) {
                const token = std.mem.trim(u8, line[14..], " ");
                if (std.mem.eql(u8, token, self.config.admin_token)) return true;
            }
        }

        return false;
    }

    /// Get the actual bound port (useful when port was 0)
    pub fn getBoundPort(self: *const Self) !u16 {
        const sock = self.listener orelse return error.NotListening;
        var addr: std.posix.sockaddr.in = undefined;
        var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
        try std.posix.getsockname(sock, @ptrCast(&addr), &len);
        return std.mem.bigToNative(u16, addr.port);
    }
};

// =============================================================================
// SSE Path Parsers (path detection only — watch loops will be added
// when shard inbox is wired)
// =============================================================================

/// Result of parsing an SSE watch path.
const SSEWatchTarget = struct {
    namespace: []const u8,
    key: []const u8,
};

/// Parse `kv/namespaces/:ns/keys/:key/watch` from the API sub-path.
/// Returns null if the path doesn't match.
fn parseSSEWatchPath(api_path: []const u8) ?SSEWatchTarget {
    const prefix = "kv/namespaces/";
    if (!std.mem.startsWith(u8, api_path, prefix)) return null;
    const rest = api_path[prefix.len..];
    const ns_end = std.mem.indexOf(u8, rest, "/") orelse return null;
    const namespace = rest[0..ns_end];

    const after_ns = rest[ns_end + 1 ..];
    const keys_prefix = "keys/";
    if (!std.mem.startsWith(u8, after_ns, keys_prefix)) return null;
    const key_rest = after_ns[keys_prefix.len..];

    const key_end = std.mem.indexOf(u8, key_rest, "/") orelse return null;
    const key_name = key_rest[0..key_end];
    const sub = key_rest[key_end + 1 ..];

    if (std.mem.eql(u8, sub, "watch")) {
        return .{ .namespace = namespace, .key = key_name };
    }
    return null;
}

/// Result of parsing a workflow SSE watch path.
const WorkflowSSEWatchTarget = struct {
    namespace: []const u8,
    run_id: []const u8,
};

/// Parse `workflow/namespaces/:ns/runs/:run_id/watch` from the API sub-path.
fn parseWorkflowSSEPath(api_path: []const u8) ?WorkflowSSEWatchTarget {
    const prefix = "workflow/namespaces/";
    if (!std.mem.startsWith(u8, api_path, prefix)) return null;
    const rest = api_path[prefix.len..];
    const ns_end = std.mem.indexOf(u8, rest, "/") orelse return null;
    const namespace = rest[0..ns_end];

    const after_ns = rest[ns_end + 1 ..];
    const runs_prefix = "runs/";
    if (!std.mem.startsWith(u8, after_ns, runs_prefix)) return null;
    const run_rest = after_ns[runs_prefix.len..];

    const rid_end = std.mem.indexOf(u8, run_rest, "/") orelse return null;
    const run_id = run_rest[0..rid_end];
    const sub = run_rest[rid_end + 1 ..];

    if (std.mem.eql(u8, sub, "watch")) {
        return .{ .namespace = namespace, .run_id = run_id };
    }
    return null;
}

/// Parse an IPv4 address string into bytes
fn parseIpAddress(addr_str: []const u8) ![4]u8 {
    var result: [4]u8 = undefined;
    var parts = std.mem.splitScalar(u8, addr_str, '.');
    var i: usize = 0;

    while (parts.next()) |part| {
        if (i >= 4) return error.InvalidAddress;
        result[i] = std.fmt.parseInt(u8, part, 10) catch return error.InvalidAddress;
        i += 1;
    }

    if (i != 4) return error.InvalidAddress;
    return result;
}

// =============================================================================
// Tests
// =============================================================================

test "parseIpAddress valid" {
    const addr = try parseIpAddress("127.0.0.1");
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, addr);
}

test "parseIpAddress invalid" {
    const result = parseIpAddress("not.an.ip");
    try std.testing.expectError(error.InvalidAddress, result);
}

test "parseSSEWatchPath correct" {
    const target = parseSSEWatchPath("kv/namespaces/default/keys/mykey/watch");
    try std.testing.expect(target != null);
    try std.testing.expectEqualStrings("default", target.?.namespace);
    try std.testing.expectEqualStrings("mykey", target.?.key);
}

test "parseSSEWatchPath no watch suffix" {
    const target = parseSSEWatchPath("kv/namespaces/default/keys/mykey");
    try std.testing.expect(target == null);
}

test "parseSSEWatchPath unrelated path" {
    const target = parseSSEWatchPath("streams/events");
    try std.testing.expect(target == null);
}

test "parseWorkflowSSEPath correct" {
    const target = parseWorkflowSSEPath("workflow/namespaces/default/runs/run-123/watch");
    try std.testing.expect(target != null);
    try std.testing.expectEqualStrings("default", target.?.namespace);
    try std.testing.expectEqualStrings("run-123", target.?.run_id);
}

test "parseWorkflowSSEPath no watch suffix" {
    const target = parseWorkflowSSEPath("workflow/namespaces/default/runs/run-123");
    try std.testing.expect(target == null);
}

test "DashboardServerConfig defaults" {
    const config = DashboardServerConfig{};
    try std.testing.expectEqual(@as(u16, 9080), config.port);
    try std.testing.expectEqualStrings("127.0.0.1", config.bind);
    try std.testing.expectEqualStrings("*", config.cors_origins);
    try std.testing.expectEqualStrings("", config.admin_token);
}
