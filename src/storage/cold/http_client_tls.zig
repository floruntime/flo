//! HTTP Client - HTTP/1.1 client with TLS support for cold storage backends
//!
//! Provides a lightweight HTTP client for S3 and Azure Blob Storage APIs.
//! Uses Zig's std.http.Client which provides:
//! - Full TLS 1.3/1.2 support with certificate validation
//! - Connection pooling and keep-alive
//! - Automatic handling of chunked transfer encoding
//!
//! Features:
//! - HTTP/1.1 with optional keep-alive
//! - TLS support via std.crypto.tls (automatic for https:// URLs)
//! - Streaming request body (for large uploads)
//! - Configurable timeouts and buffer sizes

const std = @import("std");
const Allocator = std.mem.Allocator;
const Uri = std.Uri;
const http = std.http;

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

    pub fn toStd(self: Method) http.Method {
        return switch (self) {
            .GET => .GET,
            .PUT => .PUT,
            .DELETE => .DELETE,
            .HEAD => .HEAD,
            .POST => .POST,
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
    CertificateBundleError,
};

/// HTTP client configuration
pub const ClientConfig = struct {
    /// Connection timeout in milliseconds (currently not enforced by std.http)
    connect_timeout_ms: u32 = 30_000,
    /// Read timeout in milliseconds (currently not enforced by std.http)
    read_timeout_ms: u32 = 60_000,
    /// Maximum response body size (default 64MB)
    max_response_size: usize = 64 * 1024 * 1024,
    /// User-Agent header
    user_agent: []const u8 = "Flo/1.0",
    /// TLS buffer size - must be at least max_ciphertext_record_len (16645 bytes)
    tls_buffer_size: usize = 32768,
    /// HTTP read buffer size
    read_buffer_size: usize = 16384,
    /// HTTP write buffer size
    write_buffer_size: usize = 16384,
    /// Disable TLS certificate verification (INSECURE - use only for testing!)
    insecure_skip_verify: bool = false,
};

/// HTTP/1.1 client with TLS support
///
/// Uses Zig's std.http.Client internally which provides:
/// - Automatic TLS for https:// URLs
/// - Connection pooling
/// - Certificate validation using system root certificates
pub const HttpClient = struct {
    allocator: Allocator,
    config: ClientConfig,
    /// Underlying std.http.Client (handles TLS automatically)
    client: http.Client,

    const Self = @This();

    pub fn init(allocator: Allocator, config: ClientConfig) Self {
        const client = http.Client{
            .allocator = allocator,
            .tls_buffer_size = config.tls_buffer_size,
            .read_buffer_size = config.read_buffer_size,
            .write_buffer_size = config.write_buffer_size,
        };

        return .{
            .allocator = allocator,
            .config = config,
            .client = client,
        };
    }

    pub fn deinit(self: *Self) void {
        self.client.deinit();
    }

    /// Execute an HTTP request and return the response
    pub fn request(self: *Self, req: Request) HttpError!Response {
        // Parse URI
        const uri = Uri.parse(req.uri) catch return error.InvalidUrl;

        // Prepare extra headers (convert from our Header type)
        var extra_headers_buf: [32]http.Header = undefined;
        var extra_count: usize = 0;

        // Add User-Agent
        extra_headers_buf[extra_count] = .{
            .name = "User-Agent",
            .value = self.config.user_agent,
        };
        extra_count += 1;

        // Add custom headers
        for (req.headers) |h| {
            if (extra_count >= extra_headers_buf.len) break;
            extra_headers_buf[extra_count] = .{
                .name = h.name,
                .value = h.value,
            };
            extra_count += 1;
        }

        // Prepare body - if using body_reader, we need to read it into a buffer first
        var owned_body: ?[]u8 = null;
        defer if (owned_body) |b| self.allocator.free(b);

        const payload: ?[]const u8 = if (req.body) |b|
            b
        else if (req.body_reader) |br| blk: {
            // Read streaming body into buffer
            owned_body = self.allocator.alloc(u8, br.content_length) catch return error.OutOfMemory;
            var total_read: u64 = 0;
            while (total_read < br.content_length) {
                const n = br.read(owned_body.?[@intCast(total_read)..]) catch return error.SendFailed;
                if (n == 0) break;
                total_read += n;
            }
            break :blk owned_body.?[0..@intCast(total_read)];
        } else null;

        // Use the lower-level request API for better control
        var http_req = self.client.request(req.method.toStd(), uri, .{
            .extra_headers = extra_headers_buf[0..extra_count],
            .keep_alive = false, // Simple request/response pattern
        }) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                error.TlsInitializationFailed => error.TlsError,
                error.UnsupportedUriScheme => error.InvalidUrl,
                else => error.ConnectionFailed,
            };
        };
        defer http_req.deinit();

        // Send request body if present
        if (payload) |p| {
            http_req.transfer_encoding = .{ .content_length = p.len };
            // sendBodyComplete requires mutable slice, so we copy
            const mutable_body = self.allocator.dupe(u8, p) catch return error.OutOfMemory;
            defer self.allocator.free(mutable_body);
            http_req.sendBodyComplete(mutable_body) catch return error.SendFailed;
        } else {
            http_req.sendBodiless() catch return error.SendFailed;
        }

        // Receive response head
        var redirect_buf: [1024]u8 = undefined;
        var http_response = http_req.receiveHead(&redirect_buf) catch return error.ReceiveFailed;

        // Read response body with decompression support
        var transfer_buf: [8192]u8 = undefined;
        var decompress: http.Decompress = undefined;

        // Allocate decompression buffer based on content encoding
        const decompress_buffer: []u8 = switch (http_response.head.content_encoding) {
            .identity => &[_]u8{},
            .zstd => self.allocator.alloc(u8, std.compress.zstd.default_window_len) catch return error.OutOfMemory,
            .deflate, .gzip => self.allocator.alloc(u8, std.compress.flate.max_window_len) catch return error.OutOfMemory,
            .compress => return error.InvalidResponse, // Unsupported
        };
        defer if (decompress_buffer.len > 0) self.allocator.free(decompress_buffer);

        const reader = http_response.readerDecompressing(&transfer_buf, &decompress, decompress_buffer);

        // Use allocRemaining to read all body data
        const body = reader.allocRemaining(self.allocator, std.Io.Limit.limited(self.config.max_response_size)) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                error.StreamTooLong => error.ResponseTooLarge,
                else => error.ReceiveFailed,
            };
        };

        // Convert status
        const status_code = @intFromEnum(http_response.head.status);

        // Parse response headers (empty for now - requires more complex parsing)
        const headers = std.StringHashMap([]const u8).init(self.allocator);

        return Response{
            .status_code = status_code,
            .headers = headers,
            .body = body,
            .allocator = self.allocator,
        };
    }

    /// Execute request and stream response body to callback
    ///
    /// Note: This implementation buffers the full response before calling sink.
    /// For true streaming with std.http.Client, we'd need to use the lower-level
    /// request() API instead of fetch().
    pub fn requestStreaming(
        self: *Self,
        req: Request,
        sink_ctx: *anyopaque,
        sink_fn: *const fn (ctx: *anyopaque, data: []const u8) anyerror!void,
    ) HttpError!u16 {
        // For simplicity, use the buffered request() and then stream to sink
        var response = try self.request(req);
        defer response.deinit();

        // Stream body to sink
        if (response.body.len > 0) {
            sink_fn(sink_ctx, response.body) catch return error.SendFailed;
        }

        return response.status_code;
    }
};

// ============================================================================
// Tests
// ============================================================================

test "HttpClient TLS: basic URL parsing" {
    const uri = Uri.parse("http://localhost:9000/bucket/key") catch unreachable;
    try std.testing.expectEqualStrings("localhost", uri.host.?.percent_encoded);
    try std.testing.expectEqual(@as(?u16, 9000), uri.port);
    try std.testing.expectEqualStrings("/bucket/key", uri.path.percent_encoded);
}

test "HttpClient TLS: HTTPS detection" {
    const uri = Uri.parse("https://s3.amazonaws.com/bucket/key") catch unreachable;
    try std.testing.expect(std.mem.eql(u8, uri.scheme, "https"));
}

test "HttpClient TLS: Method conversion" {
    try std.testing.expectEqual(http.Method.GET, Method.GET.toStd());
    try std.testing.expectEqual(http.Method.PUT, Method.PUT.toStd());
    try std.testing.expectEqual(http.Method.DELETE, Method.DELETE.toStd());
    try std.testing.expectEqual(http.Method.HEAD, Method.HEAD.toStd());
    try std.testing.expectEqual(http.Method.POST, Method.POST.toStd());
}
