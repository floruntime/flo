//! Shared HTTP Primitives
//!
//! HTTP/1.1 request parsing and response building utilities.
//! Single source of truth for HTTP types used across:
//! - Main server HTTP handler (port 9000)
//! - Metrics server (port 9001)
//! - Dashboard server (port 9002)

pub const request = @import("request.zig");
pub const response = @import("response.zig");

// Re-export common types at top level for convenience
pub const Method = request.Method;
pub const Header = request.Header;
pub const HttpRequest = request.HttpRequest;
pub const ParseError = request.ParseError;
pub const parse = request.parse;
pub const getExpectedSize = request.getExpectedSize;
pub const isHttpRequest = request.isHttpRequest;

pub const StatusCode = response.StatusCode;
pub const ContentType = response.ContentType;
pub const HttpResponse = response.HttpResponse;

// Response builders
pub const json = response.json;
pub const jsonError = response.jsonError;
pub const notFound = response.notFound;
pub const badRequest = response.badRequest;
pub const unauthorized = response.unauthorized;
pub const internalError = response.internalError;
pub const serviceUnavailable = response.serviceUnavailable;
pub const noContent = response.noContent;
pub const created = response.created;
pub const prometheus = response.prometheus;
pub const sseHeaders = response.sseHeaders;
pub const formatResponseHeaders = response.formatResponseHeaders;
pub const writeResponse = response.writeResponse;

// Lightweight request parsing (no allocation, for simple servers)
/// Parse request from raw bytes - lightweight version for simple servers
/// Returns null if request is incomplete
pub fn parseRequest(data: []const u8) ?ParsedRequest {
    // Find end of headers
    const headers_end = std.mem.indexOf(u8, data, "\r\n\r\n") orelse return null;

    // Parse request line
    const line_end = std.mem.indexOf(u8, data, "\r\n") orelse return null;
    const request_line = data[0..line_end];

    var parts = std.mem.splitScalar(u8, request_line, ' ');
    const method_str = parts.next() orelse return null;
    const path_with_query = parts.next() orelse return null;

    // Split path and query string
    const path: []const u8, const query_string: ?[]const u8 = if (std.mem.indexOfScalar(u8, path_with_query, '?')) |qi|
        .{ path_with_query[0..qi], path_with_query[qi + 1 ..] }
    else
        .{ path_with_query, null };

    return .{
        .method = Method.fromString(method_str),
        .path = path,
        .query_string = query_string,
        .headers_raw = data[line_end + 2 .. headers_end],
    };
}

/// Lightweight parsed request (zero-copy, no allocation)
/// For simple servers that don't need full parsing
pub const ParsedRequest = struct {
    method: Method,
    path: []const u8,
    query_string: ?[]const u8,
    headers_raw: []const u8,

    /// Check if this is a specific path prefix
    pub fn pathStartsWith(self: *const ParsedRequest, prefix: []const u8) bool {
        return std.mem.startsWith(u8, self.path, prefix);
    }

    /// Get path without prefix
    pub fn pathAfter(self: *const ParsedRequest, prefix: []const u8) ?[]const u8 {
        if (std.mem.startsWith(u8, self.path, prefix)) {
            return self.path[prefix.len..];
        }
        return null;
    }
};

const std = @import("std");
