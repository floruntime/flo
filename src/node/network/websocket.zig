//! WebSocket Handler with SIMD-Optimized Unmasking
//!
//! Implements RFC 6455 WebSocket protocol for browser SDK support.
//! WebSocket frames carry Flo-Proto binary messages.
//!
//! CRITICAL PERFORMANCE: WebSocket masking is mandatory for client->server.
//! At 1GB/s, scalar XOR burns significant CPU. We use SIMD vectors to
//! process 16 bytes at a time, achieving ~10x speedup.
//!
//! Supported opcodes:
//! - 0x02: Binary frame (Flo-Proto payload)
//! - 0x08: Close frame
//! - 0x09: Ping frame
//! - 0x0A: Pong frame
//!
//! Text frames (0x01) are rejected - we only accept binary.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// WebSocket frame opcodes
pub const Opcode = enum(u4) {
    continuation = 0x00,
    text = 0x01,
    binary = 0x02,
    close = 0x08,
    ping = 0x09,
    pong = 0x0A,
    _,
};

/// WebSocket close codes
pub const CloseCode = enum(u16) {
    normal = 1000,
    going_away = 1001,
    protocol_error = 1002,
    unsupported_data = 1003,
    invalid_payload = 1007,
    policy_violation = 1008,
    message_too_big = 1009,
    internal_error = 1011,
    _,
};

/// WebSocket frame header
pub const FrameHeader = struct {
    fin: bool,
    opcode: Opcode,
    masked: bool,
    payload_len: u64,
    mask_key: ?[4]u8,
    header_len: usize,
};

/// WebSocket parser errors
pub const Error = error{
    IncompleteFrame,
    InvalidOpcode,
    TextFrameNotSupported,
    UnmaskedClientFrame,
    PayloadTooLarge,
    ProtocolError,
    ConnectionClosed,
};

/// Maximum payload size (16 MB)
pub const MAX_PAYLOAD_SIZE: u64 = 16 * 1024 * 1024;

/// Parse WebSocket frame header
/// Returns header info and the number of bytes consumed
pub fn parseFrameHeader(data: []const u8) Error!FrameHeader {
    if (data.len < 2) return Error.IncompleteFrame;

    const byte0 = data[0];
    const byte1 = data[1];

    const fin = (byte0 & 0x80) != 0;
    const opcode: Opcode = @enumFromInt(@as(u4, @truncate(byte0 & 0x0F)));
    const masked = (byte1 & 0x80) != 0;
    const len_byte = byte1 & 0x7F;

    var header_len: usize = 2;
    var payload_len: u64 = undefined;

    if (len_byte <= 125) {
        payload_len = len_byte;
    } else if (len_byte == 126) {
        if (data.len < 4) return Error.IncompleteFrame;
        payload_len = std.mem.readInt(u16, data[2..4], .big);
        header_len = 4;
    } else { // len_byte == 127
        if (data.len < 10) return Error.IncompleteFrame;
        payload_len = std.mem.readInt(u64, data[2..10], .big);
        header_len = 10;
    }

    // Validate payload size
    if (payload_len > MAX_PAYLOAD_SIZE) {
        return Error.PayloadTooLarge;
    }

    // Parse mask key if present
    var mask_key: ?[4]u8 = null;
    if (masked) {
        if (data.len < header_len + 4) return Error.IncompleteFrame;
        mask_key = data[header_len..][0..4].*;
        header_len += 4;
    }

    return .{
        .fin = fin,
        .opcode = opcode,
        .masked = masked,
        .payload_len = payload_len,
        .mask_key = mask_key,
        .header_len = header_len,
    };
}

/// SIMD-optimized WebSocket unmasking
/// Processes 16 bytes at a time using vector XOR for ~10x speedup
pub fn unmaskPayloadSimd(payload: []u8, mask_key: [4]u8) void {
    // Expand 4-byte mask to 16-byte vector (repeated 4x)
    const mask_vec: @Vector(16, u8) = .{
        mask_key[0], mask_key[1], mask_key[2], mask_key[3],
        mask_key[0], mask_key[1], mask_key[2], mask_key[3],
        mask_key[0], mask_key[1], mask_key[2], mask_key[3],
        mask_key[0], mask_key[1], mask_key[2], mask_key[3],
    };

    var i: usize = 0;

    // Process 16 bytes at a time with SIMD
    while (i + 16 <= payload.len) : (i += 16) {
        const chunk: *[16]u8 = payload[i..][0..16];
        const data_vec: @Vector(16, u8) = chunk.*;
        const result = data_vec ^ mask_vec;
        chunk.* = result;
    }

    // Handle remaining bytes (scalar fallback for tail)
    while (i < payload.len) : (i += 1) {
        payload[i] ^= mask_key[i % 4];
    }
}

/// Scalar WebSocket unmasking (for comparison/fallback)
pub fn unmaskPayloadScalar(payload: []u8, mask_key: [4]u8) void {
    for (payload, 0..) |*byte, i| {
        byte.* ^= mask_key[i % 4];
    }
}

/// WebSocket frame parser/handler
pub const WebSocketHandler = struct {
    allocator: Allocator,
    state: State,

    /// Accumulated frame data for fragmented messages
    fragment_buffer: std.ArrayListUnmanaged(u8),
    fragment_opcode: ?Opcode,

    const State = enum {
        ready,
        reading_frame,
        closing,
        closed,
    };

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .state = .ready,
            .fragment_buffer = .{},
            .fragment_opcode = null,
        };
    }

    pub fn deinit(self: *Self) void {
        self.fragment_buffer.deinit(self.allocator);
    }

    /// Result of processing a frame
    pub const FrameResult = union(enum) {
        /// Complete binary payload ready for processing
        binary_payload: []const u8,
        /// Ping received, respond with pong
        ping: []const u8,
        /// Pong received
        pong: []const u8,
        /// Close requested
        close: CloseCode,
        /// Need more data
        need_more: void,
        /// Error occurred
        err: Error,
    };

    /// Process incoming data and return frame result
    /// Returns the frame result and number of bytes consumed
    pub fn processFrame(self: *Self, data: []u8) struct { result: FrameResult, consumed: usize } {
        if (self.state == .closed) {
            return .{ .result = .{ .err = Error.ConnectionClosed }, .consumed = 0 };
        }

        // Parse frame header
        const header = parseFrameHeader(data) catch |err| {
            if (err == Error.IncompleteFrame) {
                return .{ .result = .{ .need_more = {} }, .consumed = 0 };
            }
            return .{ .result = .{ .err = err }, .consumed = 0 };
        };

        // Check if we have the full frame
        const frame_len = header.header_len + @as(usize, @intCast(header.payload_len));
        if (data.len < frame_len) {
            return .{ .result = .{ .need_more = {} }, .consumed = 0 };
        }

        // Extract and unmask payload
        var payload = data[header.header_len..frame_len];

        // Client frames MUST be masked (RFC 6455)
        if (header.masked) {
            unmaskPayloadSimd(payload, header.mask_key.?);
        }

        // Handle frame based on opcode
        const result: FrameResult = switch (header.opcode) {
            .binary => blk: {
                if (header.fin) {
                    // Complete frame
                    if (self.fragment_buffer.items.len > 0) {
                        // This is the final fragment
                        self.fragment_buffer.appendSlice(self.allocator, payload) catch {
                            break :blk .{ .err = Error.ProtocolError };
                        };
                        const complete = self.fragment_buffer.items;
                        break :blk .{ .binary_payload = complete };
                    }
                    break :blk .{ .binary_payload = payload };
                } else {
                    // First fragment
                    self.fragment_opcode = .binary;
                    self.fragment_buffer.appendSlice(self.allocator, payload) catch {
                        break :blk .{ .err = Error.ProtocolError };
                    };
                    break :blk .{ .need_more = {} };
                }
            },
            .continuation => blk: {
                if (self.fragment_opcode == null) {
                    break :blk .{ .err = Error.ProtocolError };
                }
                self.fragment_buffer.appendSlice(self.allocator, payload) catch {
                    break :blk .{ .err = Error.ProtocolError };
                };
                if (header.fin) {
                    const complete = self.fragment_buffer.items;
                    break :blk .{ .binary_payload = complete };
                }
                break :blk .{ .need_more = {} };
            },
            .text => .{ .err = Error.TextFrameNotSupported },
            .ping => .{ .ping = payload },
            .pong => .{ .pong = payload },
            .close => blk: {
                self.state = .closing;
                if (payload.len >= 2) {
                    const code = std.mem.readInt(u16, payload[0..2], .big);
                    break :blk .{ .close = @enumFromInt(code) };
                }
                break :blk .{ .close = .normal };
            },
            _ => .{ .err = Error.InvalidOpcode },
        };

        // Clear fragment buffer if we returned a complete message
        if (result == .binary_payload and header.fin) {
            self.fragment_buffer.clearRetainingCapacity();
            self.fragment_opcode = null;
        }

        return .{ .result = result, .consumed = frame_len };
    }

    /// Reset handler for reuse
    pub fn reset(self: *Self) void {
        self.state = .ready;
        self.fragment_buffer.clearRetainingCapacity();
        self.fragment_opcode = null;
    }
};

/// Build a WebSocket frame (for sending)
pub fn buildFrame(
    allocator: Allocator,
    opcode: Opcode,
    payload: []const u8,
    fin: bool,
) ![]u8 {
    // Calculate frame size
    var header_len: usize = 2;
    if (payload.len > 65535) {
        header_len += 8;
    } else if (payload.len > 125) {
        header_len += 2;
    }

    const frame = try allocator.alloc(u8, header_len + payload.len);

    // Build header
    frame[0] = @as(u8, if (fin) 0x80 else 0) | @as(u8, @intFromEnum(opcode));

    // Payload length (server frames are NOT masked)
    if (payload.len <= 125) {
        frame[1] = @intCast(payload.len);
    } else if (payload.len <= 65535) {
        frame[1] = 126;
        std.mem.writeInt(u16, frame[2..4], @intCast(payload.len), .big);
    } else {
        frame[1] = 127;
        std.mem.writeInt(u64, frame[2..10], payload.len, .big);
    }

    // Copy payload
    @memcpy(frame[header_len..], payload);

    return frame;
}

/// Build a close frame
pub fn buildCloseFrame(allocator: Allocator, code: CloseCode, reason: ?[]const u8) ![]u8 {
    const reason_len = if (reason) |r| r.len else 0;
    const payload_len = 2 + reason_len;

    const payload = try allocator.alloc(u8, payload_len);
    defer if (reason == null) allocator.free(payload);

    std.mem.writeInt(u16, payload[0..2], @intFromEnum(code), .big);
    if (reason) |r| {
        @memcpy(payload[2..], r);
    }

    return buildFrame(allocator, .close, payload[0..payload_len], true);
}

/// Build a pong frame (response to ping)
pub fn buildPongFrame(allocator: Allocator, ping_payload: []const u8) ![]u8 {
    return buildFrame(allocator, .pong, ping_payload, true);
}

// =============================================================================
// HTTP Upgrade Handshake
// =============================================================================

/// WebSocket handshake key GUID (RFC 6455)
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Generate the Sec-WebSocket-Accept header value
pub fn generateAcceptKey(client_key: []const u8) [28]u8 {
    var sha1 = std.crypto.hash.Sha1.init(.{});

    sha1.update(client_key);
    sha1.update(WS_GUID);

    const hash = sha1.finalResult();

    var result: [28]u8 = undefined;
    _ = std.base64.standard.Encoder.encode(&result, &hash);

    return result;
}

/// HTTP upgrade response template
pub const UPGRADE_RESPONSE_TEMPLATE =
    "HTTP/1.1 101 Switching Protocols\r\n" ++
    "Upgrade: websocket\r\n" ++
    "Connection: Upgrade\r\n" ++
    "Sec-WebSocket-Accept: {s}\r\n" ++
    "\r\n";

// =============================================================================
// Tests
// =============================================================================

test "SIMD unmasking correctness" {
    const mask_key = [_]u8{ 0x37, 0xfa, 0x21, 0x3d };

    // Test data (masked)
    var data = [_]u8{
        0x7f ^ 0x37, 0x9f ^ 0xfa, 0x4d ^ 0x21, 0x51 ^ 0x3d,
        0x58 ^ 0x37, 0xaf ^ 0xfa, 0x65 ^ 0x21, 0x44 ^ 0x3d,
        0x48 ^ 0x37, 0x65 ^ 0xfa, 0x6c ^ 0x21, 0x6c ^ 0x3d,
        0x6f ^ 0x37, 0x20 ^ 0xfa, 0x57 ^ 0x21, 0x6f ^ 0x3d,
    };

    unmaskPayloadSimd(&data, mask_key);

    const expected = [_]u8{
        0x7f, 0x9f, 0x4d, 0x51,
        0x58, 0xaf, 0x65, 0x44,
        0x48, 0x65, 0x6c, 0x6c,
        0x6f, 0x20, 0x57, 0x6f,
    };

    try std.testing.expectEqualSlices(u8, &expected, &data);
}

test "SIMD vs scalar equivalence" {
    const mask_key = [_]u8{ 0x12, 0x34, 0x56, 0x78 };

    // Test with various sizes (including non-16-aligned)
    const test_sizes = [_]usize{ 0, 1, 7, 15, 16, 17, 31, 32, 33, 100 };

    for (test_sizes) |size| {
        const simd_data = try std.testing.allocator.alloc(u8, size);
        defer std.testing.allocator.free(simd_data);
        const scalar_data = try std.testing.allocator.alloc(u8, size);
        defer std.testing.allocator.free(scalar_data);

        // Fill with test pattern
        for (simd_data, 0..) |*b, i| {
            b.* = @truncate(i * 17 + 42);
        }
        @memcpy(scalar_data, simd_data);

        unmaskPayloadSimd(simd_data, mask_key);
        unmaskPayloadScalar(scalar_data, mask_key);

        try std.testing.expectEqualSlices(u8, scalar_data, simd_data);
    }
}

test "parseFrameHeader small payload" {
    // Binary frame, FIN=1, masked, payload=5
    const frame = [_]u8{
        0x82, // FIN + binary
        0x85, // masked + len=5
        0x37, 0xfa, 0x21, 0x3d, // mask key
        'H', 'e', 'l', 'l', 'o', // payload (masked)
    };

    const header = try parseFrameHeader(&frame);
    try std.testing.expect(header.fin);
    try std.testing.expectEqual(Opcode.binary, header.opcode);
    try std.testing.expect(header.masked);
    try std.testing.expectEqual(@as(u64, 5), header.payload_len);
    try std.testing.expectEqual(@as(usize, 6), header.header_len);
}

test "parseFrameHeader medium payload" {
    // Binary frame, FIN=1, masked, payload=1000
    var frame: [4 + 4]u8 = undefined;
    frame[0] = 0x82; // FIN + binary
    frame[1] = 0xFE; // masked + 126 (extended length)
    std.mem.writeInt(u16, frame[2..4], 1000, .big);
    frame[4] = 0x12;
    frame[5] = 0x34;
    frame[6] = 0x56;
    frame[7] = 0x78;

    const header = try parseFrameHeader(&frame);
    try std.testing.expectEqual(@as(u64, 1000), header.payload_len);
    try std.testing.expectEqual(@as(usize, 8), header.header_len);
}

test "generateAcceptKey" {
    // Test vector from RFC 6455
    const client_key = "dGhlIHNhbXBsZSBub25jZQ==";
    const accept = generateAcceptKey(client_key);
    try std.testing.expectEqualStrings("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", &accept);
}

test "buildFrame" {
    const allocator = std.testing.allocator;

    const frame = try buildFrame(allocator, .binary, "hello", true);
    defer allocator.free(frame);

    try std.testing.expectEqual(@as(u8, 0x82), frame[0]); // FIN + binary
    try std.testing.expectEqual(@as(u8, 5), frame[1]); // length
    try std.testing.expectEqualStrings("hello", frame[2..7]);
}

test "WebSocketHandler binary frame" {
    const allocator = std.testing.allocator;

    var handler = WebSocketHandler.init(allocator);
    defer handler.deinit();

    // Build a masked binary frame with "test" payload
    var frame = [_]u8{
        0x82, // FIN + binary
        0x84, // masked + len=4
        0x12, 0x34, 0x56, 0x78, // mask key
        't' ^ 0x12, 'e' ^ 0x34, 's' ^ 0x56, 't' ^ 0x78, // masked payload
    };

    const result = handler.processFrame(&frame);
    try std.testing.expectEqual(@as(usize, 10), result.consumed);
    try std.testing.expectEqualStrings("test", result.result.binary_payload);
}
