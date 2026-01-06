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
//! - GET /api/v1/kv/namespaces/:ns/keys/:key/watch — SSE live updates
//! - GET /api/v1/workflow/namespaces/:ns/runs/:run_id/watch — SSE workflow run updates
//! - GET /*              - Embedded static files (requires admin_token if set)
//!
//! The dashboard assets are embedded at compile time from web/dist/

const std = @import("std");
const Allocator = std.mem.Allocator;
const api = @import("api.zig");
const assets = @import("assets.zig");
const json = @import("../../util/json.zig");
const MetricsRegistry = @import("../../metrics/registry.zig").MetricsRegistry;
const Dispatcher = @import("../dispatch/dispatcher.zig").Dispatcher;
const Core = @import("../core/core.zig").Core;
const http = @import("../../util/http/mod.zig");

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
    dispatchers: []*Dispatcher,
    cores: ?[]*Core, // Optional: for MetadataCache access (cores[0] = Controller)
    metrics: *MetricsRegistry,
    listener: ?std.posix.socket_t = null,
    thread: ?std.Thread = null,
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(
        allocator: Allocator,
        config: DashboardServerConfig,
        dispatchers: []*Dispatcher,
        cores: ?[]*Core,
        metrics: *MetricsRegistry,
    ) Self {
        return .{
            .allocator = allocator,
            .config = config,
            .dispatchers = dispatchers,
            .cores = cores,
            .metrics = metrics,
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
        // Track whether we should close the socket on return.
        // SSE connections transfer ownership to a detached thread.
        var close_on_exit = true;
        defer {
            if (close_on_exit) {
                // Graceful close: use SO_LINGER to ensure send buffer is flushed
                // before the kernel sends FIN. Without this, close() on a socket
                // with buffered data sends RST ("Connection reset by peer")
                // which breaks Docker port forwarding.
                const linger = extern struct { l_onoff: c_int, l_linger: c_int }{ .l_onoff = 1, .l_linger = 2 };
                std.posix.setsockopt(client, std.posix.SOL.SOCKET, std.posix.SO.LINGER, std.mem.asBytes(&linger)) catch {};
                std.posix.close(client);
            }
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

        // ---- SSE Watch: detect before normal routing ----
        // SSE connections are long-lived — we transfer socket ownership to a
        // detached thread and skip the defer-close above.
        if (parsed.method == .GET) {
            if (parsed.pathAfter("/api/v1/")) |api_path| {
                if (parseSSEWatchPath(api_path)) |watch| {
                    close_on_exit = false;
                    self.spawnSSEWatcher(client, watch.namespace, watch.key, cors_headers);
                    return;
                }
                if (parseWorkflowSSEPath(api_path)) |wf_watch| {
                    close_on_exit = false;
                    self.spawnWorkflowSSEWatcher(client, wf_watch.namespace, wf_watch.run_id, cors_headers);
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

        // Route to appropriate handler (method-aware)
        const response = api.handleRequest(self.allocator, parsed.method, path, parsed.query_string, body, self.dispatchers, self.cores, self.metrics) catch |err| {
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

    fn sendResponse(self: *Self, client: std.posix.socket_t, status: http.StatusCode, content_type: http.ContentType, body: []const u8, cors_headers: ?[]const u8) void {
        self.sendResponseRaw(client, status, content_type.toString(), body, cors_headers);
    }

    fn sendResponseRaw(self: *Self, client: std.posix.socket_t, status: http.StatusCode, content_type: []const u8, body: []const u8, cors_headers: ?[]const u8) void {
        _ = self;
        var buf: [1024]u8 = undefined;
        const cors = cors_headers orelse "";
        const response = std.fmt.bufPrint(
            &buf,
            "HTTP/1.1 {s}\r\n" ++
                "Content-Type: {s}\r\n" ++
                "Content-Length: {d}\r\n" ++
                "{s}" ++
                "Connection: close\r\n" ++
                "\r\n",
            .{ status.statusLine(), content_type, body.len, cors },
        ) catch return;

        _ = std.posix.write(client, response) catch return;
        _ = std.posix.write(client, body) catch return;
    }

    fn sendError(self: *Self, client: std.posix.socket_t, status: http.StatusCode, message: []const u8) void {
        var json_buf: [256]u8 = undefined;
        const body = std.fmt.bufPrint(&json_buf, "{{\"error\":\"{s}\"}}", .{message}) catch return;
        self.sendResponse(client, status, .json, body, null);
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
                    // Return CORS headers (stored in static buffer)
                    return "Access-Control-Allow-Origin: *\r\n" ++
                        "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n" ++
                        "Access-Control-Allow-Headers: Content-Type\r\n";
                }
            }
        }
        return null;
    }

    fn sendCorsPreflightResponse(self: *Self, client: std.posix.socket_t, cors_headers: ?[]const u8) void {
        const cors = cors_headers orelse "";
        var buf: [512]u8 = undefined;
        const response = std.fmt.bufPrint(
            &buf,
            "HTTP/1.1 204 No Content\r\n" ++
                "{s}" ++
                "Content-Length: 0\r\n" ++
                "Connection: close\r\n" ++
                "\r\n",
            .{cors},
        ) catch return;
        _ = std.posix.write(client, response) catch return;
        _ = self;
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

    // -----------------------------------------------------------------
    // SSE Watch — spawn a detached thread for a live key subscription
    // -----------------------------------------------------------------

    /// Spawn a detached thread that owns `client` and streams SSE events.
    /// The caller has already set `close_on_exit = false` so the socket
    /// won't be closed by the defer in `handleConnection`.
    fn spawnSSEWatcher(self: *Self, client: std.posix.socket_t, namespace: []const u8, key: []const u8, cors_headers: ?[]const u8) void {
        // Heap-allocate everything the detached thread needs — the
        // stack-local `request` buffer will be gone after we return.
        const ns_copy = self.allocator.dupe(u8, namespace) catch {
            std.posix.close(client);
            return;
        };
        const key_copy = self.allocator.dupe(u8, key) catch {
            self.allocator.free(ns_copy);
            std.posix.close(client);
            return;
        };
        const ctx = self.allocator.create(SSEContext) catch {
            self.allocator.free(ns_copy);
            self.allocator.free(key_copy);
            std.posix.close(client);
            return;
        };
        ctx.* = .{
            .server = self,
            .client = client,
            .namespace = ns_copy,
            .key = key_copy,
            .cors_headers = cors_headers,
        };

        const thread = std.Thread.spawn(.{}, sseWatchLoop, .{ctx}) catch {
            self.allocator.free(ns_copy);
            self.allocator.free(key_copy);
            self.allocator.destroy(ctx);
            std.posix.close(client);
            return;
        };
        thread.detach();
    }

    // -----------------------------------------------------------------
    // SSE Workflow Watch — stream run status/history changes
    // -----------------------------------------------------------------

    fn spawnWorkflowSSEWatcher(self: *Self, client: std.posix.socket_t, namespace: []const u8, run_id: []const u8, cors_headers: ?[]const u8) void {
        const ns_copy = self.allocator.dupe(u8, namespace) catch {
            std.posix.close(client);
            return;
        };
        const rid_copy = self.allocator.dupe(u8, run_id) catch {
            self.allocator.free(ns_copy);
            std.posix.close(client);
            return;
        };
        const ctx = self.allocator.create(WorkflowSSEContext) catch {
            self.allocator.free(ns_copy);
            self.allocator.free(rid_copy);
            std.posix.close(client);
            return;
        };
        ctx.* = .{
            .server = self,
            .client = client,
            .namespace = ns_copy,
            .run_id = rid_copy,
            .cors_headers = cors_headers,
        };

        const thread = std.Thread.spawn(.{}, workflowSSEWatchLoop, .{ctx}) catch {
            self.allocator.free(ns_copy);
            self.allocator.free(rid_copy);
            self.allocator.destroy(ctx);
            std.posix.close(client);
            return;
        };
        thread.detach();
    }
};

// =============================================================================
// Helpers
// =============================================================================

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
// SSE Workflow Watch — parse path and run poll loop
// =============================================================================

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

/// Heap-allocated context for the detached workflow SSE watcher thread.
const WorkflowSSEContext = struct {
    server: *DashboardServer,
    client: std.posix.socket_t,
    namespace: []const u8,
    run_id: []const u8,
    cors_headers: ?[]const u8,
};

fn workflowSSEWatchLoop(ctx: *WorkflowSSEContext) void {
    const server = ctx.server;
    const allocator = server.allocator;

    defer {
        std.log.info("SSE workflow watch ended: ns={s} run={s}", .{ ctx.namespace, ctx.run_id });
        std.posix.close(ctx.client);
        allocator.free(ctx.namespace);
        allocator.free(ctx.run_id);
        allocator.destroy(ctx);
    }

    std.log.info("SSE workflow watch started: ns={s} run={s}", .{ ctx.namespace, ctx.run_id });

    // Disable Nagle so each SSE frame flushes immediately.
    const nodelay: u32 = 1;
    std.posix.setsockopt(ctx.client, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&nodelay)) catch {};

    // ---- Send SSE response headers ----
    const cors = if (ctx.cors_headers) |c| c else "";
    var hdr_buf: [512]u8 = undefined;
    const headers = std.fmt.bufPrint(
        &hdr_buf,
        "HTTP/1.1 200 OK\r\n" ++
            "Content-Type: text/event-stream\r\n" ++
            "Cache-Control: no-cache\r\n" ++
            "X-Accel-Buffering: no\r\n" ++
            "Connection: keep-alive\r\n" ++
            "{s}" ++
            "\r\n",
        .{cors},
    ) catch return;
    _ = std.posix.write(ctx.client, headers) catch return;

    _ = std.posix.write(ctx.client,
        "event: connected\ndata: {\"status\":\"ok\"}\n\n",
    ) catch return;

    // ---- Poll loop ----
    var last_status: ?[]const u8 = null;
    defer if (last_status) |s| allocator.free(s);
    var last_event_count: u64 = 0;
    var heartbeat_counter: u32 = 0;
    var terminal = false;

    while (server.running.load(.acquire) and !terminal) {
        if (server.dispatchers.len == 0) break;

        // Use the first dispatcher for workflow commands (dispatcher routes internally)
        const dispatcher = server.dispatchers[0];
        const request_id: u64 = 0;
        const client_id: u64 = 0;

        // --- Fetch status ---
        const status_result = dispatcher.dispatch(.{
            .workflow_status = .{
                .namespace = ctx.namespace,
                .run_id = ctx.run_id,
            },
        }, client_id, request_id, null) catch {
            std.Thread.sleep(500_000_000);
            heartbeat_counter += 1;
            if (heartbeat_counter >= 30) {
                heartbeat_counter = 0;
                _ = std.posix.write(ctx.client, ":heartbeat\n\n") catch return;
            }
            continue;
        };

        if (status_result) |sr| {
            switch (sr) {
                .workflow_status_result => |s| {
                    const changed = if (last_status) |ls| !std.mem.eql(u8, ls, s.data) else true;
                    if (changed) {
                        // Send status event
                        var frame = std.ArrayList(u8){};
                        defer frame.deinit(allocator);
                        const w = frame.writer(allocator);
                        w.writeAll("event: status\ndata: ") catch continue;
                        w.writeAll(s.data) catch continue;
                        w.writeAll("\n\n") catch continue;
                        _ = std.posix.write(ctx.client, frame.items) catch return;

                        // Track last status
                        if (last_status) |ls| allocator.free(ls);
                        last_status = allocator.dupe(u8, s.data) catch null;

                        // Check terminal state
                        if (std.mem.indexOf(u8, s.data, "\"completed\"") != null or
                            std.mem.indexOf(u8, s.data, "\"failed\"") != null or
                            std.mem.indexOf(u8, s.data, "\"cancelled\"") != null or
                            std.mem.indexOf(u8, s.data, "\"timed_out\"") != null)
                        {
                            terminal = true;
                        }
                    }
                    allocator.free(s.data);
                },
                .err => |e| {
                    // Not found — send error and close
                    var frame = std.ArrayList(u8){};
                    defer frame.deinit(allocator);
                    const w = frame.writer(allocator);
                    w.writeAll("event: error\ndata: {\"error\":\"") catch return;
                    w.writeAll(e.message) catch return;
                    w.writeAll("\"}\n\n") catch return;
                    _ = std.posix.write(ctx.client, frame.items) catch {};
                    return;
                },
                else => {},
            }
        }

        // --- Fetch history for event count ---
        const history_result = dispatcher.dispatch(.{
            .workflow_history = .{
                .namespace = ctx.namespace,
                .run_id = ctx.run_id,
                .limit = 200,
            },
        }, client_id, request_id, null) catch null;

        if (history_result) |hr| {
            switch (hr) {
                .workflow_history_result => |h| {
                    // Count events (number of objects in JSON array)
                    var count: u64 = 0;
                    for (h.data) |c| {
                        if (c == '{') count += 1;
                    }
                    if (count != last_event_count) {
                        last_event_count = count;
                        // Send full history
                        var frame = std.ArrayList(u8){};
                        defer frame.deinit(allocator);
                        const w = frame.writer(allocator);
                        w.writeAll("event: history\ndata: ") catch continue;
                        w.writeAll(h.data) catch continue;
                        w.writeAll("\n\n") catch continue;
                        _ = std.posix.write(ctx.client, frame.items) catch return;
                    }
                    allocator.free(h.data);
                },
                else => {},
            }
        }

        // Heartbeat every ~15 s.
        heartbeat_counter += 1;
        if (heartbeat_counter >= 30) {
            heartbeat_counter = 0;
            _ = std.posix.write(ctx.client, ":heartbeat\n\n") catch return;
        }

        // Send terminal event and stop polling
        if (terminal) {
            _ = std.posix.write(ctx.client, "event: terminal\ndata: {\"done\":true}\n\n") catch {};
            // Give client 1s to process before closing
            std.Thread.sleep(1_000_000_000);
            return;
        }

        std.Thread.sleep(500_000_000); // 500 ms
    }
}

// =============================================================================
// SSE KV Watch — parse path and run poll loop
// =============================================================================
//
// SSE connections are long-lived: the HTTP socket stays open and the server
// pushes `data:` frames whenever the key's version changes. A detached thread
// owns each SSE socket so the main accept-loop is never blocked.
//
// Path: GET /api/v1/kv/namespaces/:ns/keys/:key/watch
//
// Events emitted:
//   event: update   — key value changed (includes full value + version)
//   event: delete   — key was deleted
//   :heartbeat      — keep-alive comment every ~15 s

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

/// Heap-allocated context for the detached SSE watcher thread.
const SSEContext = struct {
    server: *DashboardServer,
    client: std.posix.socket_t,
    namespace: []const u8, // owned
    key: []const u8, // owned
    cors_headers: ?[]const u8, // static/comptime — NOT owned
};

/// Entry point called from `sseWatchLoop` (the detached thread).
/// Polls the KV store for the watched key every 500 ms and pushes SSE
/// frames whenever the version changes (or the key is deleted).
fn sseWatchLoop(ctx: *SSEContext) void {
    const server = ctx.server;
    const allocator = server.allocator;

    defer {
        std.log.info("SSE watch ended: ns={s} key={s}", .{ ctx.namespace, ctx.key });
        // We own the socket — close it when we exit.
        // NOTE: Do NOT call setsockopt(SO_LINGER) here. When the browser
        // closes the SSE connection the socket is in a reset state, and
        // setsockopt may return EINVAL which Zig maps to `unreachable`,
        // crashing the thread. A plain close() is safe and sufficient
        // for long-lived SSE connections.
        std.posix.close(ctx.client);

        // Free heap copies of namespace / key.
        allocator.free(ctx.namespace);
        allocator.free(ctx.key);
        allocator.destroy(ctx);
    }

    std.log.info("SSE watch started: ns={s} key={s}", .{ ctx.namespace, ctx.key });

    // Disable Nagle's algorithm so each write() flushes immediately.
    // Without this, the kernel buffers small SSE events (~200 bytes)
    // waiting for more data, delaying delivery to the browser.
    const nodelay: u32 = 1;
    std.posix.setsockopt(ctx.client, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&nodelay)) catch {};

    // ---- Send SSE response headers ----
    const cors = if (ctx.cors_headers) |c| c else "";
    var hdr_buf: [512]u8 = undefined;
    const headers = std.fmt.bufPrint(
        &hdr_buf,
        "HTTP/1.1 200 OK\r\n" ++
            "Content-Type: text/event-stream\r\n" ++
            "Cache-Control: no-cache\r\n" ++
            "X-Accel-Buffering: no\r\n" ++
            "Connection: keep-alive\r\n" ++
            "{s}" ++
            "\r\n",
        .{cors},
    ) catch return;
    _ = std.posix.write(ctx.client, headers) catch return;

    // Send an initial "connected" event so the frontend knows the
    // stream is alive before the first poll fires.
    _ = std.posix.write(ctx.client,
        "event: connected\ndata: {\"status\":\"ok\"}\n\n",
    ) catch return;

    // ---- Poll loop ----
    var last_version: u64 = 0;
    var heartbeat_counter: u32 = 0;

    while (server.running.load(.acquire)) {
        if (server.dispatchers.len == 0) break;

        const helpers = @import("api/helpers.zig");
        const target = helpers.routeToShard(server.dispatchers, ctx.namespace, ctx.key);

        const maybe_result = target.dispatch(.{
            .kv_get = .{
                .namespace = ctx.namespace,
                .key = ctx.key,
                .version = null,
            },
        }, 0, 0, null) catch |err| {
            std.log.warn("SSE dispatch error for {s}/{s}: {}", .{ ctx.namespace, ctx.key, err });
            std.Thread.sleep(500_000_000);
            heartbeat_counter += 1;
            continue;
        };

        if (maybe_result) |res| {
            switch (res) {
                .kv_value => |v| {
                    defer allocator.free(v.value);
                    if (v.version != last_version) {
                        last_version = v.version;
                        std.log.info("SSE update: {s}/{s} v{d}", .{ ctx.namespace, ctx.key, v.version });
                        // Build JSON via ArrayList + ObjectBuilder so value is
                        // properly escaped (may contain quotes, newlines, etc.).
                        const event_json = buildUpdateEvent(allocator, ctx.key, ctx.namespace, v.value, v.version) catch continue;
                        defer allocator.free(event_json);
                        _ = std.posix.write(ctx.client, event_json) catch return;
                    }
                },
                .kv_not_found => {
                    if (last_version != 0) {
                        last_version = 0;
                        std.log.info("SSE delete: {s}/{s}", .{ ctx.namespace, ctx.key });
                        const event_json = buildDeleteEvent(allocator, ctx.key, ctx.namespace) catch continue;
                        defer allocator.free(event_json);
                        _ = std.posix.write(ctx.client, event_json) catch return;
                    }
                },
                else => {},
            }
        } else {
            std.log.debug("SSE dispatch returned null for {s}/{s}", .{ ctx.namespace, ctx.key });
        }

        // Heartbeat comment every ~15 s (30 × 500 ms) to keep proxies happy.
        heartbeat_counter += 1;
        if (heartbeat_counter >= 30) {
            heartbeat_counter = 0;
            _ = std.posix.write(ctx.client, ":heartbeat\n\n") catch return;
        }

        std.Thread.sleep(500_000_000); // 500 ms
    }
}

/// Build an `event: update\ndata: {...}\n\n` SSE frame with properly
/// escaped JSON (values may contain quotes, newlines, binary, etc.).
fn buildUpdateEvent(allocator: Allocator, key: []const u8, namespace: []const u8, value: []const u8, version: u64) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    try w.writeAll("event: update\ndata: ");

    var obj = json.ObjectBuilder(@TypeOf(w)).init(w);
    try obj.begin();
    try obj.stringField("key", key);
    try obj.stringField("namespace", namespace);
    try obj.stringField("value", value);
    try obj.intField("version", version);
    try obj.boolField("found", true);
    try obj.end();

    try w.writeAll("\n\n");
    return try buf.toOwnedSlice(allocator);
}

/// Build an `event: delete\ndata: {...}\n\n` SSE frame.
fn buildDeleteEvent(allocator: Allocator, key: []const u8, namespace: []const u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    try w.writeAll("event: delete\ndata: ");

    var obj = json.ObjectBuilder(@TypeOf(w)).init(w);
    try obj.begin();
    try obj.stringField("key", key);
    try obj.stringField("namespace", namespace);
    try obj.boolField("found", false);
    try obj.end();

    try w.writeAll("\n\n");
    return try buf.toOwnedSlice(allocator);
}
