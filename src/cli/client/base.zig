//! Base Client - Core TCP connection and request/response handling
//!
//! This module provides the foundational Client struct and low-level
//! protocol communication. Primitive-specific operations (KV, Queue, etc.)
//! are in separate modules that use this base client.

const std = @import("std");
const Allocator = std.mem.Allocator;
const posix = std.posix;
const proto = @import("../../protocol/proto.zig");
const RequestBuilder = @import("../../protocol/request_builder.zig").RequestBuilder;

/// Default connection timeout in seconds
const CONNECTION_TIMEOUT_SEC: u32 = 5;
/// Default read/write timeout in seconds
const IO_TIMEOUT_SEC: u32 = 30;

/// Response from the server
pub const Response = struct {
    status: proto.StatusCode,
    data: []const u8,
    allocator: Allocator,

    pub fn deinit(self: *Response) void {
        if (self.data.len > 0) {
            self.allocator.free(self.data);
        }
    }

    /// Check if this is an error response
    pub fn isError(self: Response) bool {
        return self.status != .ok;
    }

    /// Check if this is a not-found response
    pub fn isNotFound(self: Response) bool {
        return self.status == .not_found;
    }

    /// Check if this is a conflict response (CAS failure or condition not met)
    pub fn isConflict(self: Response) bool {
        return self.status == .conflict;
    }

    /// Get the data as a string (for KV values)
    /// Wire format for KV GET response: [version: u64][value: bytes]
    /// The version is an 8-byte prefix before the actual value
    pub fn asString(self: Response) ?[]const u8 {
        if (self.status == .ok and self.data.len > 0) {
            // KV GET response has 8-byte version prefix before the value
            if (self.data.len >= 8) {
                // Skip the 8-byte version prefix, return remaining bytes as value
                return self.data[8..];
            }
            // If less than 8 bytes, something is wrong - return empty
            return null;
        }
        return null;
    }

    /// Get the version from a KV GET response
    /// Wire format: [version: u64][value: bytes]
    pub fn getVersion(self: Response) ?u64 {
        if (self.status == .ok and self.data.len >= 8) {
            return std.mem.readInt(u64, self.data[0..8], .little);
        }
        return null;
    }

    /// Get raw response data without parsing (for scan/history responses)
    /// Use this for responses that don't have the 8-byte version prefix
    pub fn asRawData(self: Response) ?[]const u8 {
        if (self.status == .ok and self.data.len > 0) {
            return self.data;
        }
        return null;
    }

    /// Get error message from status code (or server-provided detail)
    pub fn errorMessage(self: Response) []const u8 {
        // If the server sent a detailed error message in the response data, use it
        if (self.status != .ok and self.data.len > 0) {
            return self.data;
        }
        return switch (self.status) {
            .ok => "OK",
            .error_generic => "Generic error",
            .not_found => "Not found",
            .bad_request => "Bad request",
            .cross_core_transaction => "Cross-core transaction not supported",
            .no_active_transaction => "No active transaction",
            .group_locked => "Consumer group is locked (exclusive mode)",
            .unauthorized => "Unauthorized",
            .conflict => "Conflict",
            .internal_error => "Internal server error",
            .overloaded => "Server overloaded",
            .rate_limited => "Rate limit exceeded",
            _ => "Unknown error",
        };
    }
};

/// Flo Protocol Client
pub const Client = struct {
    allocator: Allocator,
    endpoint: []const u8,
    stream: ?std.net.Stream = null,
    builder: RequestBuilder,

    const Self = @This();

    pub fn init(allocator: Allocator, endpoint: []const u8) Self {
        return .{
            .allocator = allocator,
            .endpoint = endpoint,
            .builder = RequestBuilder.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.stream) |s| {
            s.close();
        }
    }

    /// Connect to the Flo server
    pub fn connect(self: *Self) !void {
        // Parse endpoint. Support both "host:port" and IPv6 bracketed form "[::1]:port".
        var host: []const u8 = undefined;
        var port_str: []const u8 = undefined;

        if (self.endpoint.len > 0 and self.endpoint[0] == '[') {
            const close_idx = std.mem.indexOf(u8, self.endpoint, "]") orelse return error.InvalidEndpoint;
            host = self.endpoint[1..close_idx];
            if (close_idx + 1 >= self.endpoint.len or self.endpoint[close_idx + 1] != ':') return error.InvalidEndpoint;
            port_str = self.endpoint[close_idx + 2 ..];
        } else {
            const colon_idx = std.mem.indexOf(u8, self.endpoint, ":") orelse return error.InvalidEndpoint;
            host = self.endpoint[0..colon_idx];
            port_str = self.endpoint[colon_idx + 1 ..];
        }
        const port = try std.fmt.parseInt(u16, port_str, 10);

        // Connect
        const address = try resolveEndpointAddress(host, port);
        self.stream = std.net.tcpConnectToAddress(address) catch |err| {
            return switch (err) {
                error.ConnectionRefused => error.ConnectionRefused,
                error.ConnectionTimedOut => error.ConnectionTimedOut,
                error.NetworkUnreachable => error.NetworkUnreachable,
                else => err,
            };
        };

        // Set socket timeouts so we don't hang forever
        const stream = self.stream.?;
        const read_timeout = posix.timeval{ .sec = @intCast(IO_TIMEOUT_SEC), .usec = 0 };
        const write_timeout = posix.timeval{ .sec = @intCast(IO_TIMEOUT_SEC), .usec = 0 };
        posix.setsockopt(stream.handle, posix.SOL.SOCKET, posix.SO.RCVTIMEO, std.mem.asBytes(&read_timeout)) catch {};
        posix.setsockopt(stream.handle, posix.SOL.SOCKET, posix.SO.SNDTIMEO, std.mem.asBytes(&write_timeout)) catch {};
    }

    /// Set socket read timeout in seconds. Use 0 to disable (wait forever).
    /// Must be called after connect().
    pub fn setReadTimeoutSec(self: *Self, seconds: u32) void {
        const stream = self.stream orelse return;
        if (seconds == 0) {
            // Disable timeout (wait forever)
            const zero_tv = posix.timeval{ .sec = 0, .usec = 0 };
            posix.setsockopt(stream.handle, posix.SOL.SOCKET, posix.SO.RCVTIMEO, std.mem.asBytes(&zero_tv)) catch {};
        } else {
            const tv = posix.timeval{ .sec = @intCast(seconds), .usec = 0 };
            posix.setsockopt(stream.handle, posix.SOL.SOCKET, posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
        }
    }

    /// Send a request and receive response (low-level)
    pub fn sendRequest(
        self: *Self,
        op_code: proto.OpCode,
        namespace: []const u8,
        key: []const u8,
        value: []const u8,
    ) !Response {
        return self.sendRequestWithOptions(op_code, namespace, key, value, "");
    }

    /// Send a request with options and receive response
    pub fn sendRequestWithOptions(
        self: *Self,
        op_code: proto.OpCode,
        namespace: []const u8,
        key: []const u8,
        value: []const u8,
        options: []const u8,
    ) !Response {
        const stream = self.stream orelse return error.NotConnected;

        // Build request using RequestBuilder
        const request = self.builder.build(op_code, namespace, key, value, options);

        // Calculate required buffer size
        const header_size = @sizeOf(proto.RequestHeader);
        const payload_size = 2 + namespace.len + 2 + key.len + 4 + value.len + 2 + options.len;
        const total_size = header_size + payload_size;

        // Use stack buffer for small requests, heap for large (e.g., WASM uploads)
        if (total_size <= 8192) {
            var send_buf: [8192]u8 = undefined;
            const serialized = try request.serialize(&send_buf);
            try stream.writeAll(serialized);
        } else {
            const send_buf = try self.allocator.alloc(u8, total_size);
            defer self.allocator.free(send_buf);
            const serialized = try request.serialize(send_buf);
            try stream.writeAll(serialized);
        }

        // Read response header
        var header_buf: [@sizeOf(proto.ResponseHeader)]u8 = undefined;
        try readExact(stream, &header_buf);

        const response_header = @as(*align(1) const proto.ResponseHeader, @ptrCast(&header_buf)).*;
        try response_header.validate();

        // Read response data
        // Normalize type to mutable slice always; allocate 0 bytes for empty payload
        const data: []u8 = if (response_header.data_len > 0)
            try self.allocator.alloc(u8, response_header.data_len)
        else
            try self.allocator.alloc(u8, 0);

        if (response_header.data_len > 0) {
            errdefer self.allocator.free(data);
            try readExact(stream, data);
        }

        return Response{
            .status = @enumFromInt(response_header.status),
            .data = data,
            .allocator = self.allocator,
        };
    }
};

/// Read exactly n bytes from the stream
fn readExact(stream: std.net.Stream, buf: []u8) !void {
    var total: usize = 0;
    while (total < buf.len) {
        const bytes_read = stream.read(buf[total..]) catch |err| {
            return switch (err) {
                error.WouldBlock => error.ConnectionTimedOut, // SO_RCVTIMEO triggers WouldBlock
                else => err,
            };
        };
        if (bytes_read == 0) return error.UnexpectedEof;
        total += bytes_read;
    }
}

/// Resolve a host (which may be a hostname like `localhost` or an IP) into
/// a std.net.Address suitable for tcpConnectToAddress. This is split out to
/// make it easy to unit test the hostname handling logic without opening a
/// network socket.
pub fn resolveEndpointAddress(host: []const u8, port: u16) !std.net.Address {
    // First try numeric parsing (IPv4 / IPv6 literal)
    const parse_result = std.net.Address.parseIp(host, port) catch {
        // If parsing failed because this is not a numeric IP literal, try
        // resolving via DNS (getAddressList). Use a temporary allocator arena
        // — getAddressList abstracts platform differences and returns usable
        // std.net.Address values for both IPv4 and IPv6.
        var addr_list = try std.net.getAddressList(std.heap.page_allocator, host, port);
        defer addr_list.deinit();

        if (addr_list.addrs.len == 0) return error.HostLacksNetworkAddresses;

        // Prefer IPv4 addresses if present, otherwise return the first address.
        var idx: usize = 0;
        while (idx < addr_list.addrs.len) : (idx += 1) {
            if (addr_list.addrs[idx].any.family == std.posix.AF.INET) {
                return addr_list.addrs[idx];
            }
        }

        return addr_list.addrs[0];
    };

    return parse_result;
}

test "resolveEndpointAddress accepts localhost and numeric addresses" {
    // ensure our helper treats localhost as an acceptable address
    _ = resolveEndpointAddress("localhost", 9000) catch {
        std.testing.expect(false);
        return;
    };

    // ensure numeric address still works (IPv4)
    _ = resolveEndpointAddress("127.0.0.1", 9000) catch {
        std.testing.expect(false);
        return;
    };

    // ensure IPv6 literal parses
    _ = resolveEndpointAddress("::1", 9000) catch {
        std.testing.expect(false);
        return;
    };
}
