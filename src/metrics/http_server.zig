//! HTTP Metrics Server
//!
//! Lightweight HTTP server for Prometheus metrics scraping.
//! Listens on a dedicated port and serves GET /metrics requests.
//!
//! Usage:
//! ```zig
//! var server = HttpMetricsServer.init(allocator, 9001, "0.0.0.0", registry);
//! try server.start();
//! // ... later ...
//! server.stop();
//! server.deinit();
//! ```

const std = @import("std");
const stdx = @import("stdx");
const Allocator = std.mem.Allocator;
const MetricsRegistry = @import("registry.zig").MetricsRegistry;
const http = @import("../util/http/mod.zig");

pub const HttpMetricsServer = struct {
    const Self = @This();

    allocator: Allocator,
    port: u16,
    bind: []const u8,
    registry: *MetricsRegistry,
    listener: ?std.posix.socket_t = null,
    thread: ?std.Thread = null,
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(allocator: Allocator, port: u16, bind: []const u8, registry: *MetricsRegistry) Self {
        return .{
            .allocator = allocator,
            .port = port,
            .bind = bind,
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
        // Create listening socket.
        const sock = try stdx.net.sysSocket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
        errdefer _ = std.c.close(sock);

        // Allow address reuse
        const one: u32 = 1;
        try std.posix.setsockopt(sock, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, std.mem.asBytes(&one));

        const bind_ip = stdx.net.parseIp4Bind(self.bind) catch {
            std.log.err("Invalid metrics bind address '{s}'", .{self.bind});
            return error.InvalidBindAddress;
        };
        const addr = stdx.net.SocketAddrV4.initIp4(bind_ip, self.port);
        try stdx.net.sysBind(sock, addr.anyPtr(), addr.anyLen());
        try stdx.net.sysListen(sock, 16);

        self.listener = sock;
        self.running.store(true, .release);

        // On a spawn failure the errdefer above closes `sock`, so clear the
        // fields that would otherwise make a later deinit()->stop() close a
        // recycled fd number.
        self.thread = std.Thread.spawn(.{}, serverLoop, .{self}) catch |err| {
            self.listener = null;
            self.running.store(false, .release);
            return err;
        };

        std.log.info("Metrics HTTP server listening on {s}:{d}", .{ self.bind, self.port });
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

            const client = stdx.net.sysAccept(listener, &client_addr, &addr_len, 0) catch |err| {
                // stdx.net.sysAccept only ever returns AcceptFailed, so testing
                // for ConnectionAborted/SocketNotListening was dead code and
                // every failure fell through to `continue` — a silent 100%-CPU
                // spin under fd exhaustion. Re-check the running flag (which
                // stop() clears before closing the listener) and back off.
                if (!self.running.load(.acquire)) break;
                std.log.debug("Metrics HTTP: accept failed: {s}", .{@errorName(err)});
                @import("stdx").time.sleep(10 * std.time.ns_per_ms);
                continue;
            };
            defer {
                // Graceful close: SO_LINGER so the send buffer is flushed before
                // FIN. Raw std.c rather than std.posix.setsockopt, which maps
                // EBADF/ENOTSOCK/EINVAL to `unreachable` — a panic that
                // `catch {}` cannot catch and that would abort the whole node
                // when a scraper disconnects mid-response and the fd is already
                // invalid. Same reason the dashboard server uses std.c here.
                const linger = extern struct { l_onoff: c_int, l_linger: c_int }{ .l_onoff = 1, .l_linger = 2 };
                _ = std.c.setsockopt(client, std.posix.SOL.SOCKET, std.posix.SO.LINGER, &linger, @sizeOf(@TypeOf(linger)));
                _ = std.c.close(client);
            }

            self.handleRequest(client) catch |err| {
                std.log.debug("Metrics HTTP: request error: {}", .{err});
            };
        }
    }

    /// Write the whole buffer. `sysWrite` does not retry a short write or
    /// EINTR, and SIGINT/SIGTERM are installed without SA_RESTART — a signal
    /// landing mid-write of a large /metrics body would otherwise truncate it
    /// under a Content-Length promising more, hanging the scraper until close.
    fn writeAll(client: std.posix.socket_t, bytes: []const u8) !void {
        var off: usize = 0;
        while (off < bytes.len) {
            const n = try stdx.net.sysWrite(client, bytes[off..]);
            if (n == 0) return error.WriteFailed;
            off += n;
        }
    }

    fn handleRequest(self: *Self, client: std.posix.socket_t) !void {
        // Read until the request is complete. A single read could route on a
        // partial request line (a split inside "GET /metrics" matches neither
        // prefix and 404s a scraper). Same fix the dashboard server needed.
        var buf: [4096]u8 = undefined;
        var total: usize = 0;
        while (total < buf.len) {
            const n = stdx.net.sysRead(client, buf[total..]) catch break;
            if (n == 0) break;
            total += n;
            if (http.getExpectedSize(buf[0..total])) |expected| {
                if (total >= expected) break;
            }
        }
        if (total == 0) return;

        const request = buf[0..total];

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

        try writeAll(client, header);
        try writeAll(client, body);
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

        try writeAll(client, header);
        try writeAll(client, body);
    }

    /// Get the actual bound port (useful when port was 0)
    pub fn getBoundPort(self: *const Self) !u16 {
        const sock = self.listener orelse return error.NotListening;
        var addr: std.posix.sockaddr.in = undefined;
        var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
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

    var server = HttpMetricsServer.init(allocator, 0, "127.0.0.1", &registry); // Port 0 = auto-assign
    defer server.deinit();

    try server.start();
    const port = try server.getBoundPort();

    // Make HTTP request.
    const net = stdx.net;
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
