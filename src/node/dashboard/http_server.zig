//! Dashboard HTTP Server
//!
//! Lightweight HTTP server for the Flo web dashboard.
//! Serves the REST API and embedded static assets (React SPA).
//!
//! Security Model: API key + session token
//! - Default bind to localhost for safe out-of-box experience
//! - Requires `flo server bootstrap` to generate root API key
//!
//! Endpoints:
//! - GET /health         - Health check (always public)
//! - GET /api/v1/*       - REST API for dashboard data (requires auth)
//! - GET /api/v1/kv/keys/:key/watch?namespace=:ns — SSE live updates (stub)
//! - GET /api/v1/workflow/runs/:run_id/watch?namespace=:ns — SSE workflow run updates (stub)
//! - GET /*              - Embedded static files (requires auth)
//!
//! The dashboard assets are embedded at compile time from web/dist/

const std = @import("std");
const Allocator = std.mem.Allocator;
const api = @import("api.zig");
const assets = @import("assets.zig");
const http = @import("../../util/http/mod.zig");
const DashboardContext = api.DashboardContext;
const log = @import("stdx").log;
const auth = @import("../../auth/mod.zig");
const auth_session = @import("../../auth/session.zig");

// =============================================================================
// Server Configuration and Implementation
// =============================================================================

/// Configuration for the dashboard server
pub const DashboardServerConfig = struct {
    port: u16 = 9080,
    bind: []const u8 = "127.0.0.1", // Localhost only by default (safe)
    cors_origins: []const u8 = "*",
    key_store: ?*auth.KeyStore = null,
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

        const auth_status = if (self.config.key_store != null) " (auth enabled)" else " (no auth — run flo server bootstrap)";
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
            // Auth session endpoints are accessible with API key (not session token)
            if (parsed.pathAfter("/api/v1/auth/")) |auth_path| {
                if (std.mem.eql(u8, auth_path, "session")) {
                    self.handleAuthSession(client, parsed.method, request, body, cors_headers);
                    return;
                }
                if (std.mem.eql(u8, auth_path, "status")) {
                    const required = self.config.key_store != null;
                    const resp = if (required) "{\"required\":true}" else "{\"required\":false}";
                    self.sendResponse(client, .ok, .json, resp, cors_headers);
                    return;
                }
            }

            if (self.config.key_store) |ks| {
                const auth_result = auth.authenticateHttpRequest(ks, request);
                switch (auth_result) {
                    .api_key, .session_token => {},
                    .none => {
                        self.sendError(client, .unauthorized, "Authentication required");
                        return;
                    },
                    .denied => |msg| {
                        self.sendError(client, .unauthorized, msg);
                        return;
                    },
                }
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
            log.debug("Dashboard: API request path={s}", .{parsed.path});
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

    /// Handle POST/DELETE /api/v1/auth/session — exchange API key for session token.
    fn handleAuthSession(self: *Self, client: std.posix.socket_t, method: http.Method, request: []const u8, body: []const u8, cors_headers: ?[]const u8) void {
        const ks = self.config.key_store orelse {
            self.sendError(client, .internal_server_error, "Auth not configured");
            return;
        };

        if (method == .DELETE) {
            // Logout — client clears token, server acknowledges
            self.sendResponse(client, .ok, .json, "{\"status\":\"logged_out\"}", cors_headers);
            return;
        }

        if (method != .POST) {
            self.sendError(client, .method_not_allowed, "Method not allowed");
            return;
        }

        // Extract API key from X-Api-Key header or body {"api_key":"..."}
        const api_key = auth.extractHeader(request, "x-api-key: ") orelse
            extractJsonField(body, "api_key") orelse
            {
                self.sendError(client, .unauthorized, "API key required");
                return;
            };

        // Validate the key
        const found = ks.validateKey(api_key) orelse {
            self.sendError(client, .unauthorized, "Invalid API key");
            return;
        };

        // Issue session token
        const secret = ks.getSigningSecret() orelse {
            self.sendError(client, .internal_server_error, "Server not bootstrapped");
            return;
        };

        const token = auth_session.issueSessionToken(
            self.allocator,
            found.getId(),
            found.role,
            secret,
            auth_session.default_ttl_seconds,
        ) catch {
            self.sendError(client, .internal_server_error, "Failed to create session");
            return;
        };
        defer self.allocator.free(token);

        // Build response JSON
        var resp_buf: [2048]u8 = undefined;
        const resp = std.fmt.bufPrint(&resp_buf, "{{\"token\":\"{s}\",\"role\":\"{s}\",\"expires_in\":{d}}}", .{
            token,
            found.role.toString(),
            auth_session.default_ttl_seconds,
        }) catch {
            self.sendError(client, .internal_server_error, "Response too large");
            return;
        };

        self.sendResponse(client, .ok, .json, resp, cors_headers);
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
// JSON Field Extraction (for auth endpoint body parsing)
// =============================================================================

/// Extract a string value from a JSON body given a field name.
/// Finds `"field":"value"` and returns the value (slice into input).
fn extractJsonField(json: []const u8, field: []const u8) ?[]const u8 {
    // Search for "field"
    var i: usize = 0;
    while (i + field.len + 2 < json.len) : (i += 1) {
        if (json[i] == '"' and
            i + 1 + field.len < json.len and
            std.mem.eql(u8, json[i + 1 ..][0..field.len], field) and
            json[i + 1 + field.len] == '"')
        {
            // Found the key, skip to value
            var j = i + 1 + field.len + 1; // past closing quote
            // Skip colon and whitespace
            while (j < json.len and (json[j] == ':' or json[j] == ' ')) : (j += 1) {}
            // Expect opening quote
            if (j < json.len and json[j] == '"') {
                j += 1;
                const start = j;
                while (j < json.len and json[j] != '"') : (j += 1) {}
                if (j > start) return json[start..j];
            }
        }
    }
    return null;
}

// =============================================================================
// SSE Path Parsers (path detection only — watch loops will be added
// when shard inbox is wired)
// =============================================================================

/// Result of parsing an SSE watch path.
const SSEWatchTarget = struct {
    key: []const u8,
};

/// Parse `kv/keys/:key/watch` from the API sub-path.
/// Namespace comes from the query string (handled by caller).
/// Returns null if the path doesn't match.
fn parseSSEWatchPath(api_path: []const u8) ?SSEWatchTarget {
    const prefix = "kv/keys/";
    if (!std.mem.startsWith(u8, api_path, prefix)) return null;
    const rest = api_path[prefix.len..];

    const key_end = std.mem.indexOf(u8, rest, "/") orelse return null;
    const key_name = rest[0..key_end];
    const sub = rest[key_end + 1 ..];

    if (std.mem.eql(u8, sub, "watch")) {
        return .{ .key = key_name };
    }
    return null;
}

/// Result of parsing a workflow SSE watch path.
const WorkflowSSEWatchTarget = struct {
    run_id: []const u8,
};

/// Parse `workflow/runs/:run_id/watch` from the API sub-path.
/// Namespace comes from the query string (handled by caller).
fn parseWorkflowSSEPath(api_path: []const u8) ?WorkflowSSEWatchTarget {
    const prefix = "workflow/runs/";
    if (!std.mem.startsWith(u8, api_path, prefix)) return null;
    const rest = api_path[prefix.len..];

    const rid_end = std.mem.indexOf(u8, rest, "/") orelse return null;
    const run_id = rest[0..rid_end];
    const sub = rest[rid_end + 1 ..];

    if (std.mem.eql(u8, sub, "watch")) {
        return .{ .run_id = run_id };
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
    const target = parseSSEWatchPath("kv/keys/mykey/watch");
    try std.testing.expect(target != null);
    try std.testing.expectEqualStrings("mykey", target.?.key);
}

test "parseSSEWatchPath no watch suffix" {
    const target = parseSSEWatchPath("kv/keys/mykey");
    try std.testing.expect(target == null);
}

test "parseSSEWatchPath unrelated path" {
    const target = parseSSEWatchPath("streams/events");
    try std.testing.expect(target == null);
}

test "parseWorkflowSSEPath correct" {
    const target = parseWorkflowSSEPath("workflow/runs/run-123/watch");
    try std.testing.expect(target != null);
    try std.testing.expectEqualStrings("run-123", target.?.run_id);
}

test "parseWorkflowSSEPath no watch suffix" {
    const target = parseWorkflowSSEPath("workflow/runs/run-123");
    try std.testing.expect(target == null);
}

test "DashboardServerConfig defaults" {
    const config = DashboardServerConfig{};
    try std.testing.expectEqual(@as(u16, 9080), config.port);
    try std.testing.expectEqualStrings("127.0.0.1", config.bind);
    try std.testing.expectEqualStrings("*", config.cors_origins);
    try std.testing.expect(config.key_store == null);
}

test "extractJsonField basic" {
    const body = "{\"api_key\":\"flo_sk_admin_abc123\",\"other\":\"value\"}";
    const result = extractJsonField(body, "api_key");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("flo_sk_admin_abc123", result.?);
}

test "extractJsonField missing" {
    const body = "{\"other\":\"value\"}";
    try std.testing.expect(extractJsonField(body, "api_key") == null);
}

test "extractJsonField empty body" {
    try std.testing.expect(extractJsonField("", "api_key") == null);
    try std.testing.expect(extractJsonField("{}", "api_key") == null);
}
