//! E2E Test HTTP Client
//!
//! HTTP client for testing Flo's REST API endpoints (dashboard, metrics, etc.).
//! Provides a simple interface for making HTTP requests and asserting responses.
//!
//! ## Usage
//! Typically accessed via TestContext.http:
//! ```zig
//! const stdx = @import("stdx");
//!
//! var ctx = try stdx.testing.TestContext.init(allocator);
//! defer ctx.deinit();
//!
//! // Simple GET
//! const resp = try ctx.http.get("/api/v1/status");
//! defer resp.deinit();
//! try std.testing.expectEqual(@as(u16, 200), resp.status);
//!
//! // POST with JSON body
//! const post_resp = try ctx.http.postJson("/api/v1/keys", .{ .key = "foo", .value = "bar" });
//! defer post_resp.deinit();
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

/// HTTP Response from a request
pub const HttpResponse = struct {
    allocator: Allocator,
    status: u16,
    headers: []const Header,
    body: []const u8,

    pub const Header = struct {
        name: []const u8,
        value: []const u8,
    };

    const Self = @This();

    /// Check if response was successful (2xx)
    pub fn succeeded(self: Self) bool {
        return self.status >= 200 and self.status < 300;
    }

    /// Check if body contains a string
    pub fn bodyContains(self: Self, needle: []const u8) bool {
        return std.mem.indexOf(u8, self.body, needle) != null;
    }

    /// Get trimmed body
    pub fn bodyTrimmed(self: Self) []const u8 {
        return std.mem.trim(u8, self.body, &std.ascii.whitespace);
    }

    /// Get header value by name (case-insensitive)
    pub fn getHeader(self: Self, name: []const u8) ?[]const u8 {
        for (self.headers) |hdr| {
            if (std.ascii.eqlIgnoreCase(hdr.name, name)) {
                return hdr.value;
            }
        }
        return null;
    }

    /// Check if content-type is JSON
    pub fn isJson(self: Self) bool {
        const ct = self.getHeader("Content-Type") orelse return false;
        return std.mem.indexOf(u8, ct, "application/json") != null;
    }

    /// Free allocated memory
    pub fn deinit(self: *Self) void {
        if (self.body.len > 0) {
            self.allocator.free(self.body);
        }
        for (self.headers) |hdr| {
            self.allocator.free(hdr.name);
            self.allocator.free(hdr.value);
        }
        self.allocator.free(self.headers);
    }
};

/// HTTP Client for E2E tests
pub const HttpRunner = struct {
    const Self = @This();

    allocator: Allocator,
    host: []const u8,
    port: u16,
    /// API key injected as X-Api-Key header on every request (set when auth is enabled)
    api_key: ?[]const u8 = null,
    /// Session JWT injected as Authorization: Bearer header (set after loginWithApiKey())
    token: ?[]const u8 = null,

    /// Default request timeout (ms)
    pub const DEFAULT_TIMEOUT_MS: u64 = 10_000;

    /// Initialize HTTP runner with host and port
    pub fn init(allocator: Allocator, host: []const u8, port: u16) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .host = try allocator.dupe(u8, host),
            .port = port,
        };
        return self;
    }

    /// Clean up resources
    pub fn deinit(self: *Self) void {
        if (self.api_key) |k| self.allocator.free(k);
        if (self.token) |t| self.allocator.free(t);
        self.allocator.free(self.host);
        self.allocator.destroy(self);
    }

    /// Set the API key for all subsequent requests (injects X-Api-Key header).
    /// Clears any stored session token.
    pub fn setApiKey(self: *Self, key: []const u8) !void {
        if (self.api_key) |old| self.allocator.free(old);
        self.api_key = try self.allocator.dupe(u8, key);
    }

    /// Set a pre-existing session JWT for all subsequent requests (Authorization: Bearer).
    pub fn setToken(self: *Self, jwt: []const u8) !void {
        if (self.token) |old| self.allocator.free(old);
        self.token = try self.allocator.dupe(u8, jwt);
    }

    /// POST /api/v1/auth/session with the provided API key and store the returned JWT.
    /// After this call, all requests will carry Authorization: Bearer <token>.
    pub fn loginWithApiKey(self: *Self, key: []const u8) !void {
        const auth_headers = &[_][2][]const u8{
            .{ "X-Api-Key", key },
        };
        const body = try std.fmt.allocPrint(self.allocator, "{{\"api_key\":\"{s}\"}}", .{key});
        defer self.allocator.free(body);

        var resp = try self.doRequest(.POST, "/api/v1/auth/session", body, auth_headers);
        defer resp.deinit();

        if (resp.status != 200 and resp.status != 201) return error.LoginFailed;

        // Parse token from JSON body: {"token":"<jwt>",...}
        const token_key = "\"token\":\"";
        const start_pos = std.mem.indexOf(u8, resp.body, token_key) orelse return error.TokenNotFound;
        const token_start = start_pos + token_key.len;
        const token_end = std.mem.indexOfScalarPos(u8, resp.body, token_start, '"') orelse return error.TokenNotFound;

        const jwt = resp.body[token_start..token_end];
        if (self.token) |old| self.allocator.free(old);
        self.token = try self.allocator.dupe(u8, jwt);
    }

    /// Make a GET request
    pub fn get(self: *Self, path: []const u8) !HttpResponse {
        return self.request(.GET, path, null, null);
    }

    /// Make a GET request with custom headers
    pub fn getWithHeaders(self: *Self, path: []const u8, headers: []const [2][]const u8) !HttpResponse {
        return self.request(.GET, path, null, headers);
    }

    /// Make a POST request with body
    pub fn post(self: *Self, path: []const u8, body: []const u8) !HttpResponse {
        return self.request(.POST, path, body, null);
    }

    /// Make a POST request with JSON body
    pub fn postJson(self: *Self, path: []const u8, json_body: anytype) !HttpResponse {
        var buf: [8192]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        fbs.writer().print("{f}", .{std.json.fmt(json_body, .{})}) catch return error.JsonSerializationFailed;
        const body = fbs.getWritten();

        const headers = &[_][2][]const u8{
            .{ "Content-Type", "application/json" },
        };
        return self.request(.POST, path, body, headers);
    }

    /// Make a PUT request with body
    pub fn put(self: *Self, path: []const u8, body: []const u8) !HttpResponse {
        return self.request(.PUT, path, body, null);
    }

    /// Make a DELETE request
    pub fn delete(self: *Self, path: []const u8) !HttpResponse {
        return self.request(.DELETE, path, null, null);
    }

    /// HTTP Methods
    pub const Method = enum {
        GET,
        POST,
        PUT,
        DELETE,
        PATCH,
        HEAD,
        OPTIONS,

        pub fn toString(self: Method) []const u8 {
            return switch (self) {
                .GET => "GET",
                .POST => "POST",
                .PUT => "PUT",
                .DELETE => "DELETE",
                .PATCH => "PATCH",
                .HEAD => "HEAD",
                .OPTIONS => "OPTIONS",
            };
        }
    };

    /// Make an HTTP request
    pub fn request(
        self: *Self,
        method: Method,
        path: []const u8,
        body: ?[]const u8,
        extra_headers: ?[]const [2][]const u8,
    ) !HttpResponse {
        return self.doRequest(method, path, body, extra_headers);
    }

    /// Full request with stream access (internal)
    fn doRequest(
        self: *Self,
        method: Method,
        path: []const u8,
        body: ?[]const u8,
        extra_headers: ?[]const [2][]const u8,
    ) !HttpResponse {
        // Connect to server
        const addr = std.net.Address.initIp4(parseIp4(self.host) catch .{ 127, 0, 0, 1 }, self.port);
        const stream = try std.net.tcpConnectToAddress(addr);
        defer stream.close();

        // Build request in a buffer
        var request_buf: [8192]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&request_buf);
        const writer = fbs.writer();

        // Request line
        try writer.print("{s} {s} HTTP/1.1\r\n", .{ method.toString(), path });
        try writer.print("Host: {s}:{d}\r\n", .{ self.host, self.port });
        try writer.writeAll("Connection: close\r\n");

        // Inject auth header: prefer Bearer token, fall back to X-Api-Key
        if (self.token) |tok| {
            try writer.print("Authorization: Bearer {s}\r\n", .{tok});
        } else if (self.api_key) |key| {
            try writer.print("X-Api-Key: {s}\r\n", .{key});
        }

        if (extra_headers) |headers| {
            for (headers) |hdr| {
                try writer.print("{s}: {s}\r\n", .{ hdr[0], hdr[1] });
            }
        }

        if (body) |b| {
            try writer.print("Content-Length: {d}\r\n", .{b.len});
        }

        try writer.writeAll("\r\n");

        // Send request headers
        _ = try stream.write(fbs.getWritten());

        // Send body if present
        if (body) |b| {
            _ = try stream.write(b);
        }

        // Read response
        var response_buf: [65536]u8 = undefined;
        var total_read: usize = 0;

        // Read until connection closes or we have a complete response
        while (total_read < response_buf.len) {
            const bytes_read = stream.read(response_buf[total_read..]) catch |err| switch (err) {
                error.ConnectionResetByPeer => break,
                else => return err,
            };
            if (bytes_read == 0) break;
            total_read += bytes_read;
        }

        const response_data = response_buf[0..total_read];

        // Parse response
        return self.parseResponse(response_data);
    }

    fn parseResponse(self: *Self, data: []const u8) !HttpResponse {
        // Find end of headers
        const header_end = std.mem.indexOf(u8, data, "\r\n\r\n") orelse return error.InvalidResponse;

        // Parse status line
        const status_line_end = std.mem.indexOf(u8, data[0..header_end], "\r\n") orelse return error.InvalidResponse;
        const status_line = data[0..status_line_end];

        // Extract status code (HTTP/1.1 200 OK)
        var parts = std.mem.splitScalar(u8, status_line, ' ');
        _ = parts.next(); // Skip HTTP version
        const status_str = parts.next() orelse return error.InvalidResponse;
        const status = std.fmt.parseInt(u16, status_str, 10) catch return error.InvalidResponse;

        // Parse headers
        var header_list: std.ArrayList(HttpResponse.Header) = .empty;
        errdefer {
            for (header_list.items) |hdr| {
                self.allocator.free(hdr.name);
                self.allocator.free(hdr.value);
            }
            header_list.deinit(self.allocator);
        }

        var header_lines = std.mem.splitSequence(u8, data[status_line_end + 2 .. header_end], "\r\n");
        while (header_lines.next()) |line| {
            if (line.len == 0) continue;
            const colon_pos = std.mem.indexOf(u8, line, ":") orelse continue;
            const name = std.mem.trim(u8, line[0..colon_pos], &std.ascii.whitespace);
            const value = std.mem.trim(u8, line[colon_pos + 1 ..], &std.ascii.whitespace);

            try header_list.append(self.allocator, .{
                .name = try self.allocator.dupe(u8, name),
                .value = try self.allocator.dupe(u8, value),
            });
        }

        // Extract body
        const body_start = header_end + 4;
        const body = if (body_start < data.len)
            try self.allocator.dupe(u8, data[body_start..])
        else
            try self.allocator.dupe(u8, "");

        return HttpResponse{
            .allocator = self.allocator,
            .status = status,
            .headers = try header_list.toOwnedSlice(self.allocator),
            .body = body,
        };
    }
};

/// Parse IPv4 address string to [4]u8
fn parseIp4(addr: []const u8) ![4]u8 {
    var result: [4]u8 = undefined;
    var parts = std.mem.splitScalar(u8, addr, '.');
    var i: usize = 0;

    while (parts.next()) |part| : (i += 1) {
        if (i >= 4) return error.InvalidAddress;
        result[i] = std.fmt.parseInt(u8, part, 10) catch return error.InvalidAddress;
    }

    if (i != 4) return error.InvalidAddress;
    return result;
}

// =============================================================================
// Tests
// =============================================================================

test "HttpResponse: status checks" {
    var resp = HttpResponse{
        .allocator = testing.allocator,
        .status = 200,
        .headers = &.{},
        .body = "",
    };

    try testing.expect(resp.succeeded());

    resp.status = 404;
    try testing.expect(!resp.succeeded());

    resp.status = 201;
    try testing.expect(resp.succeeded());
}

test "HttpResponse: body contains" {
    var resp = HttpResponse{
        .allocator = testing.allocator,
        .status = 200,
        .headers = &.{},
        .body = "hello world",
    };

    try testing.expect(resp.bodyContains("hello"));
    try testing.expect(resp.bodyContains("world"));
    try testing.expect(!resp.bodyContains("missing"));
}

test "parseIp4: valid addresses" {
    const localhost = try parseIp4("127.0.0.1");
    try testing.expectEqual([4]u8{ 127, 0, 0, 1 }, localhost);

    const other = try parseIp4("192.168.1.100");
    try testing.expectEqual([4]u8{ 192, 168, 1, 100 }, other);
}

test "parseIp4: invalid addresses" {
    try testing.expectError(error.InvalidAddress, parseIp4("127.0.0"));
    try testing.expectError(error.InvalidAddress, parseIp4("127.0.0.1.2"));
    try testing.expectError(error.InvalidAddress, parseIp4("abc.def.ghi.jkl"));
}
