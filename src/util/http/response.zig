//! HTTP Response Builder
//!
//! Builds HTTP/1.1 responses with proper headers.

const std = @import("std");

pub const StatusCode = enum(u16) {
    ok = 200,
    created = 201,
    no_content = 204,

    moved_permanently = 301,
    found = 302,
    not_modified = 304,

    bad_request = 400,
    unauthorized = 401,
    forbidden = 403,
    not_found = 404,
    method_not_allowed = 405,
    conflict = 409,
    payload_too_large = 413,
    unprocessable_entity = 422,
    too_many_requests = 429,

    internal_server_error = 500,
    not_implemented = 501,
    bad_gateway = 502,
    service_unavailable = 503,
    gateway_timeout = 504,

    pub fn code(self: StatusCode) u16 {
        return @intFromEnum(self);
    }

    pub fn phrase(self: StatusCode) []const u8 {
        return switch (self) {
            .ok => "OK",
            .created => "Created",
            .no_content => "No Content",
            .moved_permanently => "Moved Permanently",
            .found => "Found",
            .not_modified => "Not Modified",
            .bad_request => "Bad Request",
            .unauthorized => "Unauthorized",
            .forbidden => "Forbidden",
            .not_found => "Not Found",
            .method_not_allowed => "Method Not Allowed",
            .conflict => "Conflict",
            .payload_too_large => "Payload Too Large",
            .unprocessable_entity => "Unprocessable Entity",
            .too_many_requests => "Too Many Requests",
            .internal_server_error => "Internal Server Error",
            .not_implemented => "Not Implemented",
            .bad_gateway => "Bad Gateway",
            .service_unavailable => "Service Unavailable",
            .gateway_timeout => "Gateway Timeout",
        };
    }

    /// Format as "200 OK", "404 Not Found", etc.
    pub fn statusLine(self: StatusCode) []const u8 {
        return switch (self) {
            .ok => "200 OK",
            .created => "201 Created",
            .no_content => "204 No Content",
            .moved_permanently => "301 Moved Permanently",
            .found => "302 Found",
            .not_modified => "304 Not Modified",
            .bad_request => "400 Bad Request",
            .unauthorized => "401 Unauthorized",
            .forbidden => "403 Forbidden",
            .not_found => "404 Not Found",
            .method_not_allowed => "405 Method Not Allowed",
            .conflict => "409 Conflict",
            .payload_too_large => "413 Payload Too Large",
            .unprocessable_entity => "422 Unprocessable Entity",
            .too_many_requests => "429 Too Many Requests",
            .internal_server_error => "500 Internal Server Error",
            .not_implemented => "501 Not Implemented",
            .bad_gateway => "502 Bad Gateway",
            .service_unavailable => "503 Service Unavailable",
            .gateway_timeout => "504 Gateway Timeout",
        };
    }
};

pub const ContentType = enum {
    json,
    text,
    html,
    prometheus,
    octet_stream,
    event_stream,

    pub fn toString(self: ContentType) []const u8 {
        return switch (self) {
            .json => "application/json",
            .text => "text/plain; charset=utf-8",
            .html => "text/html; charset=utf-8",
            .prometheus => "text/plain; version=0.0.4; charset=utf-8",
            .octet_stream => "application/octet-stream",
            .event_stream => "text/event-stream",
        };
    }
};

pub const HttpResponse = struct {
    status: StatusCode,
    content_type: ContentType,
    body: []const u8,
    body_owned: bool,
    headers: std.ArrayListUnmanaged(Header),
    allocator: std.mem.Allocator,

    // Keep-alive support
    keep_alive: bool = true,

    pub const Header = struct {
        name: []const u8,
        value: []const u8,
    };

    pub fn init(allocator: std.mem.Allocator) HttpResponse {
        return .{
            .status = .ok,
            .content_type = .json,
            .body = "",
            .body_owned = false,
            .headers = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *HttpResponse) void {
        if (self.body_owned and self.body.len > 0) {
            self.allocator.free(@constCast(self.body));
        }
        self.headers.deinit(self.allocator);
    }

    pub fn setStatus(self: *HttpResponse, status: StatusCode) *HttpResponse {
        self.status = status;
        return self;
    }

    pub fn setContentType(self: *HttpResponse, ct: ContentType) *HttpResponse {
        self.content_type = ct;
        return self;
    }

    pub fn setBody(self: *HttpResponse, body: []const u8) *HttpResponse {
        self.body = body;
        self.body_owned = false;
        return self;
    }

    /// Set body with ownership transfer - body will be freed on deinit
    pub fn setBodyOwned(self: *HttpResponse, body: []const u8) *HttpResponse {
        self.body = body;
        self.body_owned = true;
        return self;
    }

    pub fn addHeader(self: *HttpResponse, name: []const u8, value: []const u8) !void {
        try self.headers.append(self.allocator, .{ .name = name, .value = value });
    }

    pub fn setKeepAlive(self: *HttpResponse, keep: bool) *HttpResponse {
        self.keep_alive = keep;
        return self;
    }

    /// Serialize response to bytes for sending
    pub fn serialize(self: *const HttpResponse, allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8)) !void {
        var aw: std.Io.Writer.Allocating = .init(allocator);
        defer aw.deinit();
        const writer = &aw.writer;

        // Status line
        try writer.print("HTTP/1.1 {d} {s}\r\n", .{
            @intFromEnum(self.status),
            self.status.phrase(),
        });

        // Standard headers
        try writer.print("Content-Type: {s}\r\n", .{self.content_type.toString()});
        try writer.print("Content-Length: {d}\r\n", .{self.body.len});

        if (self.keep_alive) {
            try writer.writeAll("Connection: keep-alive\r\n");
        } else {
            try writer.writeAll("Connection: close\r\n");
        }

        // CORS headers for browser compatibility
        try writer.writeAll("Access-Control-Allow-Origin: *\r\n");
        try writer.writeAll("Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n");
        try writer.writeAll("Access-Control-Allow-Headers: Content-Type, Authorization\r\n");

        // Custom headers
        for (self.headers.items) |h| {
            try writer.print("{s}: {s}\r\n", .{ h.name, h.value });
        }

        // End of headers
        try writer.writeAll("\r\n");

        // Body
        if (self.body.len > 0) {
            try writer.writeAll(self.body);
        }

        try out.appendSlice(allocator, aw.written());
    }

    /// Convenience: serialize and return owned slice
    pub fn toBytes(self: *const HttpResponse, allocator: std.mem.Allocator) ![]u8 {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        errdefer buf.deinit(allocator);
        try self.serialize(allocator, &buf);
        return buf.toOwnedSlice(allocator);
    }
};

// ============================================================================
// Convenience Builders
// ============================================================================

/// Build a JSON success response
pub fn json(allocator: std.mem.Allocator, body: []const u8) HttpResponse {
    var resp = HttpResponse.init(allocator);
    _ = resp.setStatus(.ok).setContentType(.json).setBody(body);
    return resp;
}

/// Build a JSON error response
pub fn jsonError(allocator: std.mem.Allocator, status: StatusCode, message: []const u8) !HttpResponse {
    var resp = HttpResponse.init(allocator);
    errdefer resp.deinit();
    _ = resp.setStatus(status).setContentType(.json);

    // Build error JSON - allocate body through response's allocator
    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    try aw.writer.print("{{\"error\":\"{s}\",\"status\":{d}}}", .{ message, @intFromEnum(status) });

    // Dupe the body so response owns the memory
    const body = try allocator.dupe(u8, aw.written());
    _ = resp.setBodyOwned(body);

    return resp;
}

/// Build 404 Not Found response
pub fn notFound(allocator: std.mem.Allocator) !HttpResponse {
    return jsonError(allocator, .not_found, "Not Found");
}

/// Build 401 Unauthorized response
pub fn unauthorized(allocator: std.mem.Allocator) !HttpResponse {
    return jsonError(allocator, .unauthorized, "Unauthorized");
}

/// Build 400 Bad Request response
pub fn badRequest(allocator: std.mem.Allocator, message: []const u8) !HttpResponse {
    return jsonError(allocator, .bad_request, message);
}

/// Build 500 Internal Server Error response
pub fn internalError(allocator: std.mem.Allocator) !HttpResponse {
    return jsonError(allocator, .internal_server_error, "Internal Server Error");
}

/// Build 503 Service Unavailable response with custom JSON body
/// Used for health checks that fail
pub fn serviceUnavailable(allocator: std.mem.Allocator, body: []const u8) HttpResponse {
    var resp = HttpResponse.init(allocator);
    _ = resp.setStatus(.service_unavailable).setContentType(.json).setBody(body);
    return resp;
}

/// Build 204 No Content response
pub fn noContent(allocator: std.mem.Allocator) HttpResponse {
    var resp = HttpResponse.init(allocator);
    _ = resp.setStatus(.no_content).setBody("");
    return resp;
}

/// Build 201 Created response with JSON body
pub fn created(allocator: std.mem.Allocator, body: []const u8) HttpResponse {
    var resp = HttpResponse.init(allocator);
    _ = resp.setStatus(.created).setContentType(.json).setBody(body);
    return resp;
}

/// Build CORS preflight response
pub fn corsOptions(allocator: std.mem.Allocator) HttpResponse {
    var resp = HttpResponse.init(allocator);
    _ = resp.setStatus(.no_content).setBody("");
    return resp;
}

/// Build Prometheus metrics response
pub fn prometheus(allocator: std.mem.Allocator, metrics_data: []const u8) HttpResponse {
    var resp = HttpResponse.init(allocator);
    _ = resp.setStatus(.ok).setContentType(.prometheus).setBody(metrics_data);
    return resp;
}

/// Build SSE response headers (body streamed separately)
pub fn sseHeaders(allocator: std.mem.Allocator) HttpResponse {
    var resp = HttpResponse.init(allocator);
    _ = resp.setStatus(.ok).setContentType(.event_stream).setKeepAlive(true);
    return resp;
}

// ============================================================================
// Lightweight Response Formatting (for simple servers)
// ============================================================================

/// Format response headers into a fixed buffer (no allocator needed).
/// Returns the slice of the buffer that was written, or null if buffer too small.
pub fn formatResponseHeaders(buf: []u8, status: StatusCode, content_type: ContentType, body_len: usize) ?[]const u8 {
    var stream: std.Io.Writer = .fixed(buf);
    const writer = &stream;

    writer.print("HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{
        status.statusLine(),
        content_type.toString(),
        body_len,
    }) catch return null;

    return stream.buffered();
}

/// Write a complete HTTP response to a socket (lightweight, for simple servers).
/// Uses a stack buffer for headers, no allocation needed.
pub fn writeResponse(client: std.posix.socket_t, status: StatusCode, content_type: ContentType, body: []const u8) !void {
    var header_buf: [512]u8 = undefined;
    const header = formatResponseHeaders(&header_buf, status, content_type, body.len) orelse
        return error.BufferTooSmall;
    _ = try @import("stdx").net.sysWrite(client, header);
    _ = try @import("stdx").net.sysWrite(client, body);
}

test "build JSON response" {
    const resp = json(std.testing.allocator, "{\"key\":\"value\"}");
    defer @constCast(&resp).deinit();

    const bytes = try resp.toBytes(std.testing.allocator);
    defer std.testing.allocator.free(bytes);

    try std.testing.expect(std.mem.indexOf(u8, bytes, "HTTP/1.1 200 OK") != null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "application/json") != null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "{\"key\":\"value\"}") != null);
}

test "build error response" {
    var resp = try jsonError(std.testing.allocator, .not_found, "Key not found");
    defer resp.deinit();

    const bytes = try resp.toBytes(std.testing.allocator);
    defer std.testing.allocator.free(bytes);

    try std.testing.expect(std.mem.indexOf(u8, bytes, "HTTP/1.1 404 Not Found") != null);
}
