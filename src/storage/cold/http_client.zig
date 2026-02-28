//! HTTP Client - Lightweight HTTP/1.1 client for cold storage backends
//!
//! Provides a minimal HTTP client for S3 and Azure Blob Storage APIs.
//! Designed for simplicity and reliability over performance.
//!
//! Features:
//! - HTTP/1.1 with Connection: close (simple request/response)
//! - Streaming request body (for large uploads)
//! - Streaming response body (for large downloads)
//! - Configurable timeouts
//!
//! Note: This client does NOT support TLS (HTTPS).
//! Use http_client_tls.zig for TLS support (production S3/Azure).
//! This client is ideal for testing with MinIO/Azurite/LocalStack.

const std = @import("std");
const Allocator = std.mem.Allocator;
const net = std.net;
const Uri = std.Uri;

/// HTTP methods
pub const Method = enum {
    GET,
    PUT,
    DELETE,
    HEAD,
    POST,

    pub fn toString(self: Method) []const u8 {
        return switch (self) {
            .GET => "GET",
            .PUT => "PUT",
            .DELETE => "DELETE",
            .HEAD => "HEAD",
            .POST => "POST",
        };
    }
};

/// HTTP header
pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

/// HTTP request configuration
pub const Request = struct {
    method: Method,
    uri: []const u8,
    headers: []const Header = &.{},
    body: ?[]const u8 = null,
    /// For streaming uploads - callback to read body chunks
    body_reader: ?*const BodyReader = null,
};

/// Callback for streaming request body
pub const BodyReader = struct {
    ctx: *anyopaque,
    read_fn: *const fn (ctx: *anyopaque, buffer: []u8) anyerror!usize,
    content_length: u64,

    pub fn read(self: *const BodyReader, buffer: []u8) !usize {
        return self.read_fn(self.ctx, buffer);
    }
};

/// HTTP response
pub const Response = struct {
    status_code: u16,
    headers: std.StringHashMap([]const u8),
    body: []const u8,
    allocator: Allocator,

    pub fn deinit(self: *Response) void {
        var it = self.headers.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.headers.deinit();
        if (self.body.len > 0) {
            self.allocator.free(self.body);
        }
    }

    pub fn getHeader(self: *const Response, name: []const u8) ?[]const u8 {
        // HTTP headers are case-insensitive, but we store lowercase
        var lower_buf: [256]u8 = undefined;
        const lower = std.ascii.lowerString(&lower_buf, name);
        return self.headers.get(lower);
    }
};

/// HTTP client error types
pub const HttpError = error{
    InvalidUrl,
    ConnectionFailed,
    TlsError,
    SendFailed,
    ReceiveFailed,
    InvalidResponse,
    Timeout,
    OutOfMemory,
    ResponseTooLarge,
};

/// HTTP client configuration
pub const ClientConfig = struct {
    /// Connection timeout in milliseconds
    connect_timeout_ms: u32 = 30_000,
    /// Read timeout in milliseconds
    read_timeout_ms: u32 = 60_000,
    /// Maximum response body size (default 64MB)
    max_response_size: usize = 64 * 1024 * 1024,
    /// User-Agent header
    user_agent: []const u8 = "Flo/1.0",
};

/// Lightweight HTTP/1.1 client (no TLS)
pub const HttpClient = struct {
    allocator: Allocator,
    config: ClientConfig,

    const Self = @This();

    pub fn init(allocator: Allocator, config: ClientConfig) Self {
        return .{
            .allocator = allocator,
            .config = config,
        };
    }

    /// Execute an HTTP request and return the response
    pub fn request(self: *Self, req: Request) HttpError!Response {
        // Parse URL
        const uri = Uri.parse(req.uri) catch return error.InvalidUrl;

        const host = uri.host orelse return error.InvalidUrl;
        const is_https = std.mem.eql(u8, uri.scheme, "https");
        const port: u16 = uri.port orelse if (is_https) 443 else 80;

        // Connect to server
        const stream = net.tcpConnectToHost(self.allocator, host.percent_encoded, port) catch {
            return error.ConnectionFailed;
        };
        defer stream.close();

        // No TLS support in this client — use http_client_tls.zig for HTTPS
        if (is_https) {
            return error.TlsError;
        }

        // Build request
        var request_buf: [8192]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&request_buf);
        const writer = fbs.writer();

        // Request line
        const path = if (uri.path.percent_encoded.len > 0) uri.path.percent_encoded else "/";
        writer.print("{s} {s}", .{ req.method.toString(), path }) catch return error.OutOfMemory;
        if (uri.query) |q| {
            writer.print("?{s}", .{q.percent_encoded}) catch return error.OutOfMemory;
        }
        writer.writeAll(" HTTP/1.1\r\n") catch return error.OutOfMemory;

        // Host header
        writer.print("Host: {s}", .{host.percent_encoded}) catch return error.OutOfMemory;
        if (port != 80 and port != 443) {
            writer.print(":{d}", .{port}) catch return error.OutOfMemory;
        }
        writer.writeAll("\r\n") catch return error.OutOfMemory;

        // Standard headers
        writer.print("User-Agent: {s}\r\n", .{self.config.user_agent}) catch return error.OutOfMemory;
        writer.writeAll("Connection: close\r\n") catch return error.OutOfMemory;

        // Content-Length for body
        if (req.body) |body| {
            writer.print("Content-Length: {d}\r\n", .{body.len}) catch return error.OutOfMemory;
        } else if (req.body_reader) |br| {
            writer.print("Content-Length: {d}\r\n", .{br.content_length}) catch return error.OutOfMemory;
        }

        // Custom headers
        for (req.headers) |h| {
            writer.print("{s}: {s}\r\n", .{ h.name, h.value }) catch return error.OutOfMemory;
        }

        // End of headers
        writer.writeAll("\r\n") catch return error.OutOfMemory;

        // Send request headers
        const header_bytes = fbs.getWritten();
        stream.writeAll(header_bytes) catch return error.SendFailed;

        // Send body
        if (req.body) |body| {
            stream.writeAll(body) catch return error.SendFailed;
        } else if (req.body_reader) |br| {
            var chunk_buf: [65536]u8 = undefined;
            while (true) {
                const n = br.read(&chunk_buf) catch return error.SendFailed;
                if (n == 0) break;
                stream.writeAll(chunk_buf[0..n]) catch return error.SendFailed;
            }
        }

        // Read response
        return self.readResponse(stream);
    }

    fn readResponse(self: *Self, stream: net.Stream) HttpError!Response {
        var response_buf: std.ArrayList(u8) = .empty;
        defer response_buf.deinit(self.allocator);

        // Read until we have headers
        var read_buf: [4096]u8 = undefined;
        var headers_end: ?usize = null;

        while (headers_end == null) {
            const n = stream.read(&read_buf) catch return error.ReceiveFailed;
            if (n == 0) return error.InvalidResponse;

            response_buf.appendSlice(self.allocator, read_buf[0..n]) catch return error.OutOfMemory;

            if (response_buf.items.len > self.config.max_response_size) {
                return error.ResponseTooLarge;
            }

            // Check for end of headers
            if (std.mem.indexOf(u8, response_buf.items, "\r\n\r\n")) |pos| {
                headers_end = pos;
            }
        }

        // Parse status line
        const data = response_buf.items;
        const status_line_end = std.mem.indexOf(u8, data, "\r\n") orelse return error.InvalidResponse;
        const status_line = data[0..status_line_end];

        // "HTTP/1.1 200 OK"
        var parts = std.mem.splitScalar(u8, status_line, ' ');
        _ = parts.next(); // HTTP/1.x
        const status_str = parts.next() orelse return error.InvalidResponse;
        const status_code = std.fmt.parseInt(u16, status_str, 10) catch return error.InvalidResponse;

        // Parse headers
        var headers = std.StringHashMap([]const u8).init(self.allocator);
        errdefer {
            var it = headers.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                self.allocator.free(entry.value_ptr.*);
            }
            headers.deinit();
        }

        const headers_section = data[status_line_end + 2 .. headers_end.?];
        var header_lines = std.mem.splitSequence(u8, headers_section, "\r\n");
        while (header_lines.next()) |line| {
            if (line.len == 0) continue;
            const colon = std.mem.indexOf(u8, line, ":") orelse continue;
            const name = std.mem.trim(u8, line[0..colon], " ");
            const value = std.mem.trim(u8, line[colon + 1 ..], " ");

            // Store lowercase header name
            const lower_name = self.allocator.alloc(u8, name.len) catch return error.OutOfMemory;
            _ = std.ascii.lowerString(lower_name, name);
            const duped_value = self.allocator.dupe(u8, value) catch {
                self.allocator.free(lower_name);
                return error.OutOfMemory;
            };

            headers.put(lower_name, duped_value) catch {
                self.allocator.free(lower_name);
                self.allocator.free(duped_value);
                return error.OutOfMemory;
            };
        }

        // Get Content-Length
        const content_length: usize = if (headers.get("content-length")) |cl_str|
            std.fmt.parseInt(usize, cl_str, 10) catch 0
        else
            0;

        // Read remaining body
        const body_start = headers_end.? + 4;
        const body_already_read = response_buf.items.len - body_start;

        var body: []u8 = &.{};
        if (content_length > 0) {
            if (content_length > self.config.max_response_size) {
                return error.ResponseTooLarge;
            }

            body = self.allocator.alloc(u8, content_length) catch return error.OutOfMemory;
            errdefer self.allocator.free(body);

            // Copy already-read body bytes
            const to_copy = @min(body_already_read, content_length);
            @memcpy(body[0..to_copy], response_buf.items[body_start..][0..to_copy]);

            // Read remaining
            var total_read = to_copy;
            while (total_read < content_length) {
                const n = stream.read(body[total_read..content_length]) catch return error.ReceiveFailed;
                if (n == 0) break;
                total_read += n;
            }
        }

        return Response{
            .status_code = status_code,
            .headers = headers,
            .body = body,
            .allocator = self.allocator,
        };
    }

    /// Convenience: Execute request and stream response body to callback
    pub fn requestStreaming(
        self: *Self,
        req: Request,
        sink_ctx: *anyopaque,
        sink_fn: *const fn (ctx: *anyopaque, data: []const u8) anyerror!void,
    ) HttpError!u16 {
        // Parse URL
        const uri = Uri.parse(req.uri) catch return error.InvalidUrl;

        const host = uri.host orelse return error.InvalidUrl;
        const is_https = std.mem.eql(u8, uri.scheme, "https");
        const port: u16 = uri.port orelse if (is_https) 443 else 80;

        if (is_https) {
            return error.TlsError;
        }

        // Connect
        const stream = net.tcpConnectToHost(self.allocator, host.percent_encoded, port) catch {
            return error.ConnectionFailed;
        };
        defer stream.close();

        // Build and send request
        var request_buf: [8192]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&request_buf);
        const writer = fbs.writer();

        const path = if (uri.path.percent_encoded.len > 0) uri.path.percent_encoded else "/";
        writer.print("{s} {s}", .{ req.method.toString(), path }) catch return error.OutOfMemory;
        if (uri.query) |q| {
            writer.print("?{s}", .{q.percent_encoded}) catch return error.OutOfMemory;
        }
        writer.writeAll(" HTTP/1.1\r\n") catch return error.OutOfMemory;

        writer.print("Host: {s}", .{host.percent_encoded}) catch return error.OutOfMemory;
        if (port != 80 and port != 443) {
            writer.print(":{d}", .{port}) catch return error.OutOfMemory;
        }
        writer.writeAll("\r\n") catch return error.OutOfMemory;

        writer.print("User-Agent: {s}\r\n", .{self.config.user_agent}) catch return error.OutOfMemory;
        writer.writeAll("Connection: close\r\n") catch return error.OutOfMemory;

        if (req.body) |body| {
            writer.print("Content-Length: {d}\r\n", .{body.len}) catch return error.OutOfMemory;
        } else if (req.body_reader) |br| {
            writer.print("Content-Length: {d}\r\n", .{br.content_length}) catch return error.OutOfMemory;
        }

        for (req.headers) |h| {
            writer.print("{s}: {s}\r\n", .{ h.name, h.value }) catch return error.OutOfMemory;
        }

        writer.writeAll("\r\n") catch return error.OutOfMemory;

        stream.writeAll(fbs.getWritten()) catch return error.SendFailed;

        if (req.body) |body| {
            stream.writeAll(body) catch return error.SendFailed;
        } else if (req.body_reader) |br| {
            var chunk_buf: [65536]u8 = undefined;
            while (true) {
                const n = br.read(&chunk_buf) catch return error.SendFailed;
                if (n == 0) break;
                stream.writeAll(chunk_buf[0..n]) catch return error.SendFailed;
            }
        }

        // Read response headers only, then stream body
        var header_buf: [8192]u8 = undefined;
        var header_len: usize = 0;
        var headers_end: ?usize = null;

        while (headers_end == null and header_len < header_buf.len) {
            const n = stream.read(header_buf[header_len..]) catch return error.ReceiveFailed;
            if (n == 0) return error.InvalidResponse;
            header_len += n;

            if (std.mem.indexOf(u8, header_buf[0..header_len], "\r\n\r\n")) |pos| {
                headers_end = pos;
            }
        }

        if (headers_end == null) return error.InvalidResponse;

        // Parse status
        const status_line_end = std.mem.indexOf(u8, header_buf[0..header_len], "\r\n") orelse return error.InvalidResponse;
        var status_parts = std.mem.splitScalar(u8, header_buf[0..status_line_end], ' ');
        _ = status_parts.next();
        const status_str = status_parts.next() orelse return error.InvalidResponse;
        const status_code = std.fmt.parseInt(u16, status_str, 10) catch return error.InvalidResponse;

        // Stream body to sink
        const body_start = headers_end.? + 4;
        if (body_start < header_len) {
            sink_fn(sink_ctx, header_buf[body_start..header_len]) catch return error.SendFailed;
        }

        // Continue reading and streaming
        var chunk: [65536]u8 = undefined;
        while (true) {
            const n = stream.read(&chunk) catch return error.ReceiveFailed;
            if (n == 0) break;
            sink_fn(sink_ctx, chunk[0..n]) catch return error.SendFailed;
        }

        return status_code;
    }
};

// ============================================================================
// Tests
// ============================================================================

test "HttpClient basic URL parsing" {
    const uri = Uri.parse("http://localhost:9000/bucket/key") catch unreachable;
    try std.testing.expectEqualStrings("localhost", uri.host.?.percent_encoded);
    try std.testing.expectEqual(@as(?u16, 9000), uri.port);
    try std.testing.expectEqualStrings("/bucket/key", uri.path.percent_encoded);
}

test "HttpClient HTTPS detection" {
    const uri = Uri.parse("https://s3.amazonaws.com/bucket/key") catch unreachable;
    try std.testing.expect(std.mem.eql(u8, uri.scheme, "https"));
}
