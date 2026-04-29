//! HTTP Request Parser
//!
//! Parses HTTP/1.1 requests from raw bytes.
//! Supports GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH methods.

const std = @import("std");

pub const Method = enum {
    GET,
    POST,
    PUT,
    DELETE,
    OPTIONS,
    HEAD,
    PATCH,
    UNKNOWN,

    pub fn fromString(s: []const u8) Method {
        if (std.mem.eql(u8, s, "GET")) return .GET;
        if (std.mem.eql(u8, s, "POST")) return .POST;
        if (std.mem.eql(u8, s, "PUT")) return .PUT;
        if (std.mem.eql(u8, s, "DELETE")) return .DELETE;
        if (std.mem.eql(u8, s, "OPTIONS")) return .OPTIONS;
        if (std.mem.eql(u8, s, "HEAD")) return .HEAD;
        if (std.mem.eql(u8, s, "PATCH")) return .PATCH;
        return .UNKNOWN;
    }

    pub fn toString(self: Method) []const u8 {
        return switch (self) {
            .GET => "GET",
            .POST => "POST",
            .PUT => "PUT",
            .DELETE => "DELETE",
            .OPTIONS => "OPTIONS",
            .HEAD => "HEAD",
            .PATCH => "PATCH",
            .UNKNOWN => "UNKNOWN",
        };
    }
};

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub const HttpRequest = struct {
    method: Method,
    path: []const u8,
    query_string: ?[]const u8,
    version: []const u8,
    headers: []Header,
    body: ?[]const u8,

    // Common headers cached for quick access
    content_length: ?usize,
    content_type: ?[]const u8,
    authorization: ?[]const u8,
    host: ?[]const u8,
    connection: ?[]const u8,
    accept: ?[]const u8,

    // Backing buffer for parsed data
    arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *HttpRequest) void {
        self.arena.deinit();
    }

    /// Get a header value by name (case-insensitive)
    pub fn getHeader(self: *const HttpRequest, name: []const u8) ?[]const u8 {
        for (self.headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) {
                return h.value;
            }
        }
        return null;
    }

    /// Parse query parameters into key-value pairs
    pub fn parseQuery(self: *const HttpRequest, allocator: std.mem.Allocator) !std.StringHashMap([]const u8) {
        var params = std.StringHashMap([]const u8).init(allocator);
        errdefer params.deinit();

        const qs = self.query_string orelse return params;
        if (qs.len == 0) return params;

        var pairs = std.mem.splitScalar(u8, qs, '&');
        while (pairs.next()) |pair| {
            if (pair.len == 0) continue;
            if (std.mem.indexOfScalar(u8, pair, '=')) |eq_pos| {
                const key = pair[0..eq_pos];
                const value = if (eq_pos + 1 < pair.len) pair[eq_pos + 1 ..] else "";
                try params.put(key, value);
            } else {
                // Key without value
                try params.put(pair, "");
            }
        }
        return params;
    }

    /// Extract Bearer token from Authorization header
    pub fn getBearerToken(self: *const HttpRequest) ?[]const u8 {
        const auth = self.authorization orelse return null;
        const prefix = "Bearer ";
        if (std.mem.startsWith(u8, auth, prefix)) {
            return auth[prefix.len..];
        }
        return null;
    }

    /// Check if this is a WebSocket upgrade request
    pub fn isWebSocketUpgrade(self: *const HttpRequest) bool {
        if (self.method != .GET) return false;

        const upgrade = self.getHeader("Upgrade") orelse return false;
        if (!std.ascii.eqlIgnoreCase(upgrade, "websocket")) return false;

        const connection = self.connection orelse return false;
        // Connection header may contain multiple values
        var iter = std.mem.splitScalar(u8, connection, ',');
        while (iter.next()) |part| {
            const trimmed = std.mem.trim(u8, part, " ");
            if (std.ascii.eqlIgnoreCase(trimmed, "upgrade")) return true;
        }
        return false;
    }
};

pub const ParseError = error{
    InvalidMethod,
    InvalidPath,
    InvalidVersion,
    InvalidHeader,
    HeaderTooLarge,
    BodyTooLarge,
    Incomplete,
    OutOfMemory,
};

/// Maximum header size (64KB)
pub const MAX_HEADER_SIZE: usize = 64 * 1024;

/// Maximum body size for REST API (16MB)
pub const MAX_BODY_SIZE: usize = 16 * 1024 * 1024;

/// Parse HTTP request from raw bytes
/// Returns null if more data is needed
pub fn parse(backing_allocator: std.mem.Allocator, data: []const u8) ParseError!?HttpRequest {
    // Find end of headers
    const header_end = std.mem.indexOf(u8, data, "\r\n\r\n") orelse {
        if (data.len > MAX_HEADER_SIZE) return error.HeaderTooLarge;
        return null; // Need more data
    };

    const header_section = data[0..header_end];
    const body_start = header_end + 4;

    var arena = std.heap.ArenaAllocator.init(backing_allocator);
    errdefer arena.deinit();
    const allocator = arena.allocator();

    // Parse request line
    var lines = std.mem.splitSequence(u8, header_section, "\r\n");
    const request_line = lines.next() orelse return error.InvalidMethod;

    // Parse "METHOD /path HTTP/1.1"
    var parts = std.mem.splitScalar(u8, request_line, ' ');
    const method_str = parts.next() orelse return error.InvalidMethod;
    const full_path = parts.next() orelse return error.InvalidPath;
    const version = parts.next() orelse return error.InvalidVersion;

    const method = Method.fromString(method_str);
    if (method == .UNKNOWN) return error.InvalidMethod;

    // Split path and query string
    var path: []const u8 = full_path;
    var query_string: ?[]const u8 = null;
    if (std.mem.indexOfScalar(u8, full_path, '?')) |qmark| {
        path = full_path[0..qmark];
        query_string = if (qmark + 1 < full_path.len) full_path[qmark + 1 ..] else null;
    }

    // Parse headers
    var headers: std.ArrayListUnmanaged(Header) = .empty;
    var content_length: ?usize = null;
    var content_type: ?[]const u8 = null;
    var authorization: ?[]const u8 = null;
    var host: ?[]const u8 = null;
    var connection: ?[]const u8 = null;
    var accept: ?[]const u8 = null;

    while (lines.next()) |line| {
        if (line.len == 0) continue;

        const colon = std.mem.indexOfScalar(u8, line, ':') orelse return error.InvalidHeader;
        const name = std.mem.trim(u8, line[0..colon], " ");
        const value = std.mem.trim(u8, line[colon + 1 ..], " ");

        try headers.append(allocator, .{ .name = name, .value = value });

        // Cache common headers
        if (std.ascii.eqlIgnoreCase(name, "Content-Length")) {
            content_length = std.fmt.parseInt(usize, value, 10) catch null;
        } else if (std.ascii.eqlIgnoreCase(name, "Content-Type")) {
            content_type = value;
        } else if (std.ascii.eqlIgnoreCase(name, "Authorization")) {
            authorization = value;
        } else if (std.ascii.eqlIgnoreCase(name, "Host")) {
            host = value;
        } else if (std.ascii.eqlIgnoreCase(name, "Connection")) {
            connection = value;
        } else if (std.ascii.eqlIgnoreCase(name, "Accept")) {
            accept = value;
        }
    }

    // Handle body
    var body: ?[]const u8 = null;
    if (content_length) |len| {
        if (len > MAX_BODY_SIZE) return error.BodyTooLarge;
        const available = data.len - body_start;
        if (available < len) {
            arena.deinit();
            return null; // Need more data
        }
        body = data[body_start .. body_start + len];
    }

    return HttpRequest{
        .method = method,
        .path = path,
        .query_string = query_string,
        .version = version,
        .headers = headers.items,
        .body = body,
        .content_length = content_length,
        .content_type = content_type,
        .authorization = authorization,
        .host = host,
        .connection = connection,
        .accept = accept,
        .arena = arena,
    };
}

/// Check if bytes start with an HTTP method (for protocol detection)
pub fn isHttpRequest(data: []const u8) bool {
    if (data.len < 4) return false;

    // Check for common HTTP method prefixes
    inline for (.{
        "GET ",
        "POST",
        "PUT ",
        "DELE", // DELETE
        "OPTI", // OPTIONS
        "HEAD",
        "PATC", // PATCH
    }) |prefix| {
        if (std.mem.startsWith(u8, data, prefix)) return true;
    }
    return false;
}

/// Calculate total request size (headers + body) for buffering
pub fn getExpectedSize(data: []const u8) ?usize {
    const header_end = std.mem.indexOf(u8, data, "\r\n\r\n") orelse return null;
    const body_start = header_end + 4;

    // Find Content-Length header
    const cl_pos = std.mem.indexOf(u8, data[0..header_end], "Content-Length:") orelse
        std.mem.indexOf(u8, data[0..header_end], "content-length:") orelse
        return body_start; // No body

    const cl_start = cl_pos + "Content-Length:".len;
    const cl_line_end = std.mem.indexOfScalarPos(u8, data, cl_start, '\r') orelse return null;
    const cl_value = std.mem.trim(u8, data[cl_start..cl_line_end], " ");

    const content_length = std.fmt.parseInt(usize, cl_value, 10) catch return null;
    return body_start + content_length;
}

test "parse simple GET request" {
    const request_data = "GET /api/v1/kv/mykey HTTP/1.1\r\nHost: localhost:9000\r\nAccept: application/json\r\n\r\n";

    const req = try parse(std.testing.allocator, request_data) orelse return error.Incomplete;
    defer @constCast(&req).deinit();

    try std.testing.expectEqual(Method.GET, req.method);
    try std.testing.expectEqualStrings("/api/v1/kv/mykey", req.path);
    try std.testing.expect(req.query_string == null);
    try std.testing.expectEqualStrings("localhost:9000", req.host.?);
}

test "parse POST with body" {
    const request_data = "POST /api/v1/kv/mykey HTTP/1.1\r\nHost: localhost:9000\r\nContent-Type: application/json\r\nContent-Length: 17\r\n\r\n{\"value\":\"hello\"}";

    const req = try parse(std.testing.allocator, request_data) orelse return error.Incomplete;
    defer @constCast(&req).deinit();

    try std.testing.expectEqual(Method.POST, req.method);
    try std.testing.expectEqualStrings("/api/v1/kv/mykey", req.path);
    try std.testing.expectEqual(@as(usize, 17), req.content_length.?);
    try std.testing.expectEqualStrings("{\"value\":\"hello\"}", req.body.?);
}

test "parse with query string" {
    const request_data = "GET /api/v1/kv?prefix=user:&limit=100 HTTP/1.1\r\nHost: localhost\r\n\r\n";

    const req = try parse(std.testing.allocator, request_data) orelse return error.Incomplete;
    defer @constCast(&req).deinit();

    try std.testing.expectEqualStrings("/api/v1/kv", req.path);
    try std.testing.expectEqualStrings("prefix=user:&limit=100", req.query_string.?);
}

test "isHttpRequest" {
    try std.testing.expect(isHttpRequest("GET /path"));
    try std.testing.expect(isHttpRequest("POST /path"));
    try std.testing.expect(isHttpRequest("DELETE /path"));
    try std.testing.expect(!isHttpRequest("\x46\x4C\x4F\x00")); // FLO magic
    try std.testing.expect(!isHttpRequest(""));
}
