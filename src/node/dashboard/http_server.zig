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
    /// Other origins allowed to call the API from a browser, exactly as
    /// the browser sends them (`https://ops.example.com:8443`),
    /// comma-separated. Empty: only the dashboard's own pages may.
    cors_origins: []const u8 = "",
    /// Host names the dashboard answers to besides localhost, 127.0.0.1
    /// and [::1], comma-separated: a page that rebinds its own name to this
    /// address is refused rather than served.
    hosts: []const u8 = "",
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
            _ = std.c.close(sock);
            self.listener = null;
        }
    }

    pub fn start(self: *Self) !void {
        // Create listening socket
        const sock = try @import("stdx").net.sysSocket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        errdefer _ = std.c.close(sock);

        // Allow address reuse
        const one: u32 = 1;
        try std.posix.setsockopt(sock, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, std.mem.asBytes(&one));

        // Parse bind address
        const bind_ip = parseIpAddress(self.config.bind) catch |err| {
            std.log.err("Invalid dashboard bind address '{s}': {}", .{ self.config.bind, err });
            return error.InvalidBindAddress;
        };

        // Bind to configured address and port
        const addr = @import("stdx").net.SocketAddrV4.initIp4(bind_ip, self.config.port);
        try @import("stdx").net.sysBind(sock, addr.anyPtr(), addr.anyLen());
        try @import("stdx").net.sysListen(sock, 64);

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
            _ = std.c.shutdown(sock, 2);
            _ = std.c.close(sock);
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
        const listener = self.listener orelse return;
        http.serve.serve(self.allocator, listener, &self.running, self);
    }

    /// One whole request, from the accept loop.
    pub fn handle(self: *Self, client: std.posix.socket_t, request: []const u8) void {
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

        // Liveness answers anyone: probes name pod or host addresses.
        if (std.mem.eql(u8, parsed.path, "/health")) {
            self.sendResponse(client, .ok, .json, "{\"status\":\"ok\"}", null);
            return;
        }
        if (self.refusal(parsed.method, request)) |r| {
            self.sendError(client, r.status, r.message);
            return;
        }

        var cors_buf: [512]u8 = undefined;
        const cors_headers = self.getCorsHeaders(request, &cors_buf);

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
        } else if (parsed.method == .GET or parsed.method == .HEAD) {
            // Serve embedded static assets
            self.handleStaticRequest(client, parsed.path, cors_headers);
        } else {
            self.sendError(client, .method_not_allowed, "Method not allowed");
        }
    }

    fn handleApiRequest(self: *Self, client: std.posix.socket_t, parsed: http.ParsedRequest, body: []const u8, cors_headers: ?[]const u8) void {
        const path = parsed.pathAfter("/api/v1/") orelse "";

        const response = api.handleRequest(self.allocator, parsed.method, path, parsed.query_string, body, self.ctx) catch |err| {
            if (err == error.NotFound) {
                self.sendError(client, .not_found, "Not found");
                return;
            }
            if (err == error.MethodNotAllowed) {
                self.sendError(client, .method_not_allowed, "Method not allowed");
                return;
            }
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
            if (asset.is_precompressed) {
                self.sendResponsePrecompressed(client, .ok, asset.mime_type, asset.content, cors_headers);
            } else {
                self.sendResponseRaw(client, .ok, asset.mime_type, asset.content, cors_headers);
            }
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

        _ = @import("stdx").net.sysWrite(client, response) catch return;
        _ = @import("stdx").net.sysWrite(client, body_data) catch return;
    }

    fn sendResponsePrecompressed(_: *Self, client: std.posix.socket_t, status: http.StatusCode, content_type: []const u8, body_data: []const u8, cors_headers: ?[]const u8) void {
        var hdr_buf: [1024]u8 = undefined;
        const cors = cors_headers orelse "";
        const response = std.fmt.bufPrint(
            &hdr_buf,
            "HTTP/1.1 {s}\r\n" ++
                "Content-Type: {s}\r\n" ++
                "Content-Encoding: gzip\r\n" ++
                "Content-Length: {d}\r\n" ++
                "{s}" ++
                "Connection: close\r\n" ++
                "\r\n",
            .{ status.statusLine(), content_type, body_data.len, cors },
        ) catch return;

        _ = @import("stdx").net.sysWrite(client, response) catch return;
        _ = @import("stdx").net.sysWrite(client, body_data) catch return;
    }

    fn sendError(self: *Self, client: std.posix.socket_t, status: http.StatusCode, message: []const u8) void {
        var json_buf: [256]u8 = undefined;
        const body_data = std.fmt.bufPrint(&json_buf, "{{\"error\":\"{s}\"}}", .{message}) catch return;
        self.sendResponse(client, status, .json, body_data, null);
    }

    /// The CORS headers for an allowed other origin: that origin echoed,
    /// never `*` and never `null`, with `Vary: Origin` so a cache does not
    /// hand one origin's answer to another.
    fn getCorsHeaders(self: *Self, request: []const u8, buf: []u8) ?[]const u8 {
        const origin = headerValue(request, "origin") orelse return null;
        // "null" is every sandboxed frame and local file: never a grant.
        if (std.mem.eql(u8, origin, "null") or !listed(self.config.cors_origins, origin)) return null;
        return std.fmt.bufPrint(buf, "Access-Control-Allow-Origin: {s}\r\n" ++
            "Vary: Origin\r\n" ++
            "Access-Control-Allow-Methods: GET, POST, PUT, DELETE\r\n" ++
            "Access-Control-Allow-Headers: Content-Type, Authorization\r\n", .{origin}) catch null;
    }

    const Refusal = struct { status: http.StatusCode, message: []const u8 };

    /// Why a request is not served, or null. Any request: the Host must be
    /// one this dashboard answers to, or a page could rebind its own name to
    /// this address and read everything. A change (any method but GET, HEAD
    /// or OPTIONS) must also come from this dashboard's own pages or an
    /// allowed origin, and carry a content type no other site's page can
    /// send without asking first: otherwise any page an operator visits
    /// could purge a queue or invoke an action here.
    fn refusal(self: *const Self, method: http.Method, request: []const u8) ?Refusal {
        const host = headerValue(request, "host") orelse return .{ .status = .bad_request, .message = "Host header required" };
        if (!self.hostAllowed(host)) return .{ .status = .misdirected_request, .message = "Host not served here; add it to [dashboard] hosts" };
        switch (method) {
            .GET, .HEAD, .OPTIONS => return null,
            else => {},
        }
        const same_site = if (headerValue(request, "sec-fetch-site")) |v| std.mem.eql(u8, v, "same-origin") else false;
        const origin_ok = if (headerValue(request, "origin")) |origin|
            originIsHost(origin, host) or (!std.mem.eql(u8, origin, "null") and listed(self.config.cors_origins, origin))
        else
            false;
        if (!same_site and !origin_ok) return .{ .status = .forbidden, .message = "Changes are accepted only from the dashboard's own pages or an allowed origin" };
        const ct = headerValue(request, "content-type") orelse return .{ .status = .unsupported_media_type, .message = "Content-Type required on changes" };
        if (simpleContentType(ct)) return .{ .status = .unsupported_media_type, .message = "Content-Type not accepted on changes; send application/json" };
        return null;
    }

    fn hostAllowed(self: *const Self, host_header: []const u8) bool {
        const name = hostName(host_header);
        for ([_][]const u8{ "localhost", "127.0.0.1", "[::1]" }) |h| {
            if (std.ascii.eqlIgnoreCase(name, h)) return true;
        }
        var it = std.mem.splitScalar(u8, self.config.hosts, ',');
        while (it.next()) |raw| {
            const h = std.mem.trim(u8, raw, " \t");
            if (h.len > 0 and std.ascii.eqlIgnoreCase(name, h)) return true;
        }
        return false;
    }

    fn sendCorsPreflightResponse(_: *Self, client: std.posix.socket_t, cors_headers: ?[]const u8) void {
        const cors = cors_headers orelse "";
        var hdr_buf: [1024]u8 = undefined;
        const response = std.fmt.bufPrint(
            &hdr_buf,
            "HTTP/1.1 204 No Content\r\n" ++
                "{s}" ++
                "Content-Length: 0\r\n" ++
                "Connection: close\r\n" ++
                "\r\n",
            .{cors},
        ) catch return;
        _ = @import("stdx").net.sysWrite(client, response) catch return;
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
        try (if (std.c.getsockname(sock, @ptrCast(&addr), &len) != 0) error.GetsocknameFailed else {});
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
// Request header checks
// =============================================================================

/// The value of header `name` (lower case) in the raw request, trimmed.
fn headerValue(request: []const u8, name: []const u8) ?[]const u8 {
    const head_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse request.len;
    var lines = std.mem.splitSequence(u8, request[0..head_end], "\r\n");
    _ = lines.next(); // request line
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, line[0..colon], " \t"), name)) {
            return std.mem.trim(u8, line[colon + 1 ..], " \t");
        }
    }
    return null;
}

/// `host[:port]` without the port; `[::1]:9002` keeps its brackets.
fn hostName(host_header: []const u8) []const u8 {
    if (std.mem.startsWith(u8, host_header, "[")) {
        const close = std.mem.indexOfScalar(u8, host_header, ']') orelse return host_header;
        return host_header[0 .. close + 1];
    }
    const colon = std.mem.lastIndexOfScalar(u8, host_header, ':') orelse return host_header;
    return host_header[0..colon];
}

/// Whether `origin` is this dashboard itself: `http://` and the Host the
/// request was sent to, exactly.
fn originIsHost(origin: []const u8, host_header: []const u8) bool {
    const prefix = "http://";
    return std.mem.startsWith(u8, origin, prefix) and std.mem.eql(u8, origin[prefix.len..], host_header);
}

/// Whether `value` is one of the comma-separated `list`, exactly.
fn listed(list: []const u8, value: []const u8) bool {
    var it = std.mem.splitScalar(u8, list, ',');
    while (it.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t");
        if (item.len > 0 and std.mem.eql(u8, item, value)) return true;
    }
    return false;
}

/// The content types a page on another site can send without the browser
/// asking first: a change carrying one could come from any site.
fn simpleContentType(content_type: []const u8) bool {
    const media = std.mem.trim(u8, content_type[0 .. std.mem.indexOfScalar(u8, content_type, ';') orelse content_type.len], " \t");
    for ([_][]const u8{ "text/plain", "application/x-www-form-urlencoded", "multipart/form-data" }) |simple| {
        if (std.ascii.eqlIgnoreCase(media, simple)) return true;
    }
    return media.len == 0;
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
    try std.testing.expectEqualStrings("", config.cors_origins);
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

fn testServer(config: DashboardServerConfig) DashboardServer {
    return DashboardServer.init(std.testing.allocator, config, undefined);
}

test "dashboard: a change needs this dashboard's or an allowed origin, and a content type no other page can send" {
    const srv = testServer(.{ .cors_origins = "https://ops.example.com:8443, null" });
    const own = "POST /api/v1/queues/q/purge HTTP/1.1\r\nHost: localhost:9002\r\nOrigin: http://localhost:9002\r\nContent-Type: application/json\r\n\r\n";
    try std.testing.expect(srv.refusal(.POST, own) == null);
    const allowed = "PUT /x HTTP/1.1\r\nHost: localhost:9002\r\nOrigin: https://ops.example.com:8443\r\nContent-Type: application/json; charset=utf-8\r\n\r\n";
    try std.testing.expect(srv.refusal(.PUT, allowed) == null);
    const same_site = "DELETE /x HTTP/1.1\r\nHost: 127.0.0.1:9002\r\nSec-Fetch-Site: same-origin\r\nContent-Type: application/json\r\n\r\n";
    try std.testing.expect(srv.refusal(.DELETE, same_site) == null);

    // Another site, a sandboxed frame (even if someone listed "null"), or
    // no origin at all.
    for ([_][]const u8{ "Origin: http://evil.example\r\n", "Origin: null\r\n", "", "Sec-Fetch-Site: cross-site\r\n", "Origin: http://localhost:9003\r\n" }) |origin| {
        var buf: [256]u8 = undefined;
        const req = try std.fmt.bufPrint(&buf, "POST /x HTTP/1.1\r\nHost: localhost:9002\r\n{s}Content-Type: application/json\r\n\r\n", .{origin});
        try std.testing.expectEqual(http.StatusCode.forbidden, srv.refusal(.POST, req).?.status);
    }
    // Content types any page can send, or none.
    for ([_][]const u8{ "Content-Type: text/plain\r\n", "Content-Type: application/x-www-form-urlencoded\r\n", "Content-Type: Multipart/Form-Data; boundary=b\r\n", "" }) |ct| {
        var buf: [256]u8 = undefined;
        const req = try std.fmt.bufPrint(&buf, "POST /x HTTP/1.1\r\nHost: localhost:9002\r\nOrigin: http://localhost:9002\r\n{s}\r\n", .{ct});
        try std.testing.expectEqual(http.StatusCode.unsupported_media_type, srv.refusal(.POST, req).?.status);
    }
    // A read needs neither.
    try std.testing.expect(srv.refusal(.GET, "GET /api/v1/queues HTTP/1.1\r\nHost: localhost\r\n\r\n") == null);
}

test "dashboard: only the hosts it answers to are served" {
    const srv = testServer(.{ .hosts = "flo.internal, 10.0.1.5" });
    for ([_][]const u8{ "localhost", "LOCALHOST:9002", "127.0.0.1:9002", "[::1]:9002", "flo.internal:9002", "10.0.1.5" }) |host| {
        var buf: [128]u8 = undefined;
        const req = try std.fmt.bufPrint(&buf, "GET / HTTP/1.1\r\nHost: {s}\r\n\r\n", .{host});
        try std.testing.expect(srv.refusal(.GET, req) == null);
    }
    for ([_][]const u8{ "evil.example", "localhost.evil.example", "flo.internal.evil:9002", "127.0.0.2" }) |host| {
        var buf: [128]u8 = undefined;
        const req = try std.fmt.bufPrint(&buf, "GET / HTTP/1.1\r\nHost: {s}\r\n\r\n", .{host});
        try std.testing.expectEqual(http.StatusCode.misdirected_request, srv.refusal(.GET, req).?.status);
    }
    try std.testing.expectEqual(http.StatusCode.bad_request, srv.refusal(.GET, "GET / HTTP/1.1\r\n\r\n").?.status);
}

test "dashboard: CORS grants only a listed origin, echoed with Vary, never null and never to everyone" {
    var srv = testServer(.{ .cors_origins = "https://ops.example.com:8443,null" });
    var buf: [512]u8 = undefined;
    const granted = srv.getCorsHeaders("GET / HTTP/1.1\r\nOrigin: https://ops.example.com:8443\r\n\r\n", &buf).?;
    try std.testing.expect(std.mem.indexOf(u8, granted, "Access-Control-Allow-Origin: https://ops.example.com:8443\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, granted, "Vary: Origin\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, granted, "*") == null);
    try std.testing.expect(srv.getCorsHeaders("GET / HTTP/1.1\r\nOrigin: null\r\n\r\n", &buf) == null);
    try std.testing.expect(srv.getCorsHeaders("GET / HTTP/1.1\r\nOrigin: https://ops.example.com\r\n\r\n", &buf) == null);
    try std.testing.expect(srv.getCorsHeaders("GET / HTTP/1.1\r\n\r\n", &buf) == null);
    srv = testServer(.{});
    try std.testing.expect(srv.getCorsHeaders("GET / HTTP/1.1\r\nOrigin: https://ops.example.com:8443\r\n\r\n", &buf) == null);
}
