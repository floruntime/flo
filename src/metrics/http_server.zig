//! HTTP Metrics Server
//!
//! Lightweight HTTP server for Prometheus metrics scraping.
//! Listens on a dedicated port and serves GET /metrics requests.
//!
//! Usage:
//! ```zig
//! var server = try HttpMetricsServer.init(allocator, 9001, registry);
//! try server.start();
//! // ... later ...
//! server.stop();
//! server.deinit();
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const MetricsRegistry = @import("registry.zig").MetricsRegistry;
const http = @import("../util/http/mod.zig");

pub const HttpMetricsServer = struct {
    const Self = @This();

    allocator: Allocator,
    port: u16,
    registry: *MetricsRegistry,
    listener: ?std.posix.socket_t = null,
    thread: ?std.Thread = null,
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(allocator: Allocator, port: u16, registry: *MetricsRegistry) Self {
        return .{
            .allocator = allocator,
            .port = port,
            .registry = registry,
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
        // Create listening socket. Uses the stdx.net syscall wrappers rather
        // than std.posix.socket/bind/listen/accept, which do not exist in this
        // Zig version — that is why this server never compiled and so was never
        // wired into the runtime.
        const net = @import("stdx").net;
        const sock = try net.sysSocket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        errdefer _ = std.c.close(sock);

        // Allow address reuse
        const one: u32 = 1;
        try std.posix.setsockopt(sock, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, std.mem.asBytes(&one));

        // Bind to port
        const addr = net.SocketAddrV4.initIp4(.{ 0, 0, 0, 0 }, self.port);
        try net.sysBind(sock, addr.anyPtr(), addr.anyLen());
        try net.sysListen(sock, 16);

        self.listener = sock;
        self.running.store(true, .release);

        // Start server thread
        self.thread = try std.Thread.spawn(.{}, serverLoop, .{self});

        std.log.info("Metrics HTTP server listening on port {d}", .{self.port});
    }

    pub fn stop(self: *Self) void {
        if (!self.running.load(.acquire)) return;

        self.running.store(false, .release);

        // Close listener to unblock accept
        if (self.listener) |sock| {
            // Shutdown first to interrupt any blocked accept()
            _ = std.c.shutdown(sock, 2); // SHUT_RDWR
            _ = std.c.close(sock);
            self.listener = null;
        }

        // Wait for thread (should exit quickly now)
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }

        std.log.info("Metrics HTTP server stopped", .{});
    }

    fn serverLoop(self: *Self) void {
        while (self.running.load(.acquire)) {
            const listener = self.listener orelse break;

            var client_addr: std.posix.sockaddr = undefined;
            var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);

            const client = @import("stdx").net.sysAccept(listener, &client_addr, &addr_len, 0) catch |err| {
                if (err == error.ConnectionAborted or err == error.SocketNotListening) {
                    // Server shutting down
                    break;
                }
                continue;
            };
            defer {
                // Graceful close: use SO_LINGER to ensure send buffer is flushed
                const linger = extern struct { l_onoff: c_int, l_linger: c_int }{ .l_onoff = 1, .l_linger = 2 };
                std.posix.setsockopt(client, std.posix.SOL.SOCKET, std.posix.SO.LINGER, std.mem.asBytes(&linger)) catch {};
                _ = std.c.close(client);
            }

            self.handleRequest(client) catch |err| {
                std.log.debug("Metrics HTTP: request error: {}", .{err});
            };
        }
    }

    fn handleRequest(self: *Self, client: std.posix.socket_t) !void {
        var buf: [1024]u8 = undefined;
        const n = try std.posix.read(client, &buf);
        if (n == 0) return;

        const request = buf[0..n];

        // Parse request using shared HTTP primitives
        if (http.parseRequest(request)) |parsed| {
            if (parsed.pathStartsWith("/metrics")) {
                try self.sendMetrics(client);
            } else if (parsed.pathStartsWith("/health") or std.mem.eql(u8, parsed.path, "/")) {
                try self.sendHealth(client);
            } else {
                try http.writeResponse(client, .not_found, .text, "Not Found\n");
            }
        } else {
            // Fallback: simple string match for incomplete parses
            if (std.mem.startsWith(u8, request, "GET /metrics")) {
                try self.sendMetrics(client);
            } else if (std.mem.startsWith(u8, request, "GET /health") or std.mem.startsWith(u8, request, "GET /")) {
                try self.sendHealth(client);
            } else {
                try http.writeResponse(client, .not_found, .text, "Not Found\n");
            }
        }
    }

    fn sendMetrics(self: *Self, client: std.posix.socket_t) !void {
        const body = try self.registry.exportPrometheus(self.allocator);
        defer self.allocator.free(body);

        var header_buf: [256]u8 = undefined;
        const header = http.formatResponseHeaders(&header_buf, .ok, .prometheus, body.len) orelse
            return error.BufferTooSmall;

        _ = try @import("stdx").net.sysWrite(client, header);
        _ = try @import("stdx").net.sysWrite(client, body);
    }

    /// Send JSON health response with shard and uptime info
    fn sendHealth(self: *Self, client: std.posix.socket_t) !void {
        const server_snap = self.registry.server.snapshot();
        var body_buf: [256]u8 = undefined;
        const body = std.fmt.bufPrint(&body_buf, "{{\"status\":\"ok\",\"shards\":{d},\"uptime_seconds\":{d},\"connections\":{d}}}\n", .{
            self.registry.shardCount(),
            server_snap.uptime_seconds,
            server_snap.connections,
        }) catch return http.writeResponse(client, .ok, .text, "ok\n");

        var header_buf: [256]u8 = undefined;
        const header = http.formatResponseHeaders(&header_buf, .ok, .json, body.len) orelse
            return error.BufferTooSmall;

        _ = try @import("stdx").net.sysWrite(client, header);
        _ = try @import("stdx").net.sysWrite(client, body);
    }

    /// Get the actual bound port (useful when port was 0)
    pub fn getBoundPort(self: *const Self) !u16 {
        const sock = self.listener orelse return error.NotListening;
        var addr: std.posix.sockaddr.in = undefined;
        var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
        // std.c rather than std.posix.getsockname, which this Zig version
        // does not expose (see the socket calls in `start`).
        if (std.c.getsockname(sock, @ptrCast(&addr), &len) != 0) return error.GetSockNameFailed;
        return std.mem.bigToNative(u16, addr.port);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "HttpMetricsServer basic functionality" {
    const allocator = std.testing.allocator;

    var registry = @import("registry.zig").MetricsRegistry.init(allocator);
    defer registry.deinit();

    var server = HttpMetricsServer.init(allocator, 0, &registry); // Port 0 = auto-assign
    defer server.deinit();

    try server.start();
    const port = try server.getBoundPort();

    // Make HTTP request. stdx.net rather than std.net.tcpConnectToHost, which
    // this Zig version does not provide.
    const net = @import("stdx").net;
    const stream = try net.tcpConnectToAddress(net.Address.initIp4(.{ 127, 0, 0, 1 }, port));
    defer stream.close();

    _ = try stream.write("GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n");

    // Read until the peer closes. A single read() races the server: the
    // response can arrive split across segments, which made this test fail
    // roughly two runs in three under the full suite.
    var response_buf: [8192]u8 = undefined;
    var response_len: usize = 0;
    while (response_len < response_buf.len) {
        const n = stream.read(response_buf[response_len..]) catch break;
        if (n == 0) break;
        response_len += n;
    }
    const response = response_buf[0..response_len];

    // Verify response
    try std.testing.expect(std.mem.indexOf(u8, response, "200 OK") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "flo_connections_current") != null);

    server.stop();
}
