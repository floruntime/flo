//! Connection — per-client connection struct with protocol detection
//!
//! Each accepted TCP connection is wrapped in a Connection. The Acceptor
//! peeks the first bytes to detect the protocol, then hands off the fd
//! to the correct shard.
//!
//! ## Protocol Detection
//!
//! | First Bytes    | Protocol   |
//! |---------------|------------|
//! | `FLO\0`       | Binary     |
//! | `*` (0x2A)    | RESP       |
//! | `GET ` + ws   | WebSocket  |
//! | HTTP method   | HTTP       |
//!
//! ## Write Coalescing
//!
//! Multiple responses are accumulated in the write buffer and flushed
//! together when the fd becomes writable (reactor callback). This
//! reduces syscall overhead on high-throughput connections.

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// Ring Buffer
// ═══════════════════════════════════════════════════════════════════════════════

/// Power-of-2 ring buffer for I/O. Supports zero-copy reads up to
/// contiguous tail length, wrapping writes, and compact.
pub const RingBuffer = struct {
    buf: []u8,
    read_pos: usize,
    write_pos: usize,
    allocator: std.mem.Allocator,

    const DEFAULT_CAPACITY: usize = 64 * 1024; // 64 KB

    pub fn init(allocator: std.mem.Allocator) !RingBuffer {
        return initWithCapacity(allocator, DEFAULT_CAPACITY);
    }

    pub fn initWithCapacity(allocator: std.mem.Allocator, requested: usize) !RingBuffer {
        // Round up to power of 2
        const cap = std.math.ceilPowerOfTwo(usize, requested) catch requested;
        const buf = try allocator.alloc(u8, cap);
        return .{
            .buf = buf,
            .read_pos = 0,
            .write_pos = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RingBuffer) void {
        self.allocator.free(self.buf);
    }

    /// Bytes available for reading.
    pub fn readable(self: *const RingBuffer) usize {
        return self.write_pos - self.read_pos;
    }

    /// Free space for writing.
    pub fn writable(self: *const RingBuffer) usize {
        return self.buf.len - self.readable();
    }

    /// Peek at readable data (may wrap — returns first contiguous chunk).
    pub fn peek(self: *const RingBuffer) []const u8 {
        if (self.readable() == 0) return &[_]u8{};
        const mask = self.buf.len - 1;
        const start = self.read_pos & mask;
        const end_linear = self.buf.len; // end of physical buffer
        const avail = @min(self.readable(), end_linear - start);
        return self.buf[start .. start + avail];
    }

    /// Consume `n` bytes from the read side.
    pub fn consume(self: *RingBuffer, n: usize) void {
        const actual = @min(n, self.readable());
        self.read_pos += actual;
    }

    /// Write data into the buffer. Returns number of bytes written.
    pub fn write(self: *RingBuffer, data: []const u8) usize {
        const space = self.writable();
        const to_write = @min(data.len, space);
        if (to_write == 0) return 0;

        const mask = self.buf.len - 1;
        const start = self.write_pos & mask;

        // First chunk: start → end of physical buf
        const first_len = @min(to_write, self.buf.len - start);
        @memcpy(self.buf[start .. start + first_len], data[0..first_len]);

        // Second chunk: wrap around
        if (to_write > first_len) {
            const second_len = to_write - first_len;
            @memcpy(self.buf[0..second_len], data[first_len .. first_len + second_len]);
        }

        self.write_pos += to_write;
        return to_write;
    }

    /// Read up to `out.len` bytes, consuming them.
    pub fn read(self: *RingBuffer, out: []u8) usize {
        const avail = self.readable();
        const to_read = @min(out.len, avail);
        if (to_read == 0) return 0;

        const mask = self.buf.len - 1;
        const start = self.read_pos & mask;

        const first_len = @min(to_read, self.buf.len - start);
        @memcpy(out[0..first_len], self.buf[start .. start + first_len]);

        if (to_read > first_len) {
            const second_len = to_read - first_len;
            @memcpy(out[first_len .. first_len + second_len], self.buf[0..second_len]);
        }

        self.read_pos += to_read;
        return to_read;
    }

    /// Compact: if both cursors have advanced far, reset them.
    pub fn compact(self: *RingBuffer) void {
        if (self.read_pos >= self.buf.len) {
            self.write_pos -= self.read_pos;
            self.read_pos = 0;
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Protocol Detection
// ═══════════════════════════════════════════════════════════════════════════════

/// Detected wire protocol.
pub const Protocol = enum(u8) {
    /// Flo native binary protocol (magic `FLO\0`).
    binary = 0,
    /// Redis RESP protocol (first byte `*`).
    resp = 1,
    /// WebSocket (HTTP GET with Upgrade header).
    websocket = 2,
    /// Plain HTTP (REST API / dashboard).
    http = 3,
    /// Not yet determined (need more bytes).
    unknown = 0xFF,
};

/// Flo protocol magic: `FLO\0` = 0x004F4C46 little-endian.
const FLO_MAGIC = [4]u8{ 0x46, 0x4C, 0x4F, 0x00 };

/// RESP array prefix (Redis clients always send `*N\r\n...`).
const RESP_ARRAY: u8 = '*';

/// Detect protocol from peeked bytes (typically first 4+ bytes).
///
/// The Acceptor uses `recv(fd, buf, MSG_PEEK)` to get these bytes
/// without consuming them from the socket buffer.
pub fn detectProtocol(peek_data: []const u8) Protocol {
    if (peek_data.len == 0) return .unknown;

    // RESP: first byte is '*' (array command from redis-cli)
    if (peek_data[0] == RESP_ARRAY) return .resp;

    // Flo binary: first 4 bytes are magic
    if (peek_data.len >= 4 and std.mem.eql(u8, peek_data[0..4], &FLO_MAGIC)) return .binary;

    // HTTP methods: GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH
    if (peek_data.len >= 4) {
        if (std.mem.eql(u8, peek_data[0..4], "GET ")) {
            // Could be WebSocket upgrade or plain HTTP
            // Full detection requires seeing headers; for now mark as HTTP.
            // The upgrade handshake handler will promote to WebSocket.
            return .http;
        }
        if (std.mem.eql(u8, peek_data[0..4], "POST") or
            std.mem.eql(u8, peek_data[0..4], "PUT ") or
            std.mem.eql(u8, peek_data[0..4], "DELE") or
            std.mem.eql(u8, peek_data[0..4], "OPTI") or
            std.mem.eql(u8, peek_data[0..4], "HEAD") or
            std.mem.eql(u8, peek_data[0..4], "PATC"))
        {
            return .http;
        }
    }

    // Not enough data or unrecognized
    if (peek_data.len < 4) return .unknown;

    // Default: assume binary (could be a partial FLO header)
    return .binary;
}

/// Extended protocol detection that can distinguish WebSocket from HTTP
/// by checking for `Upgrade: websocket` header in the peek buffer.
pub fn detectProtocolFull(peek_data: []const u8) Protocol {
    const basic = detectProtocol(peek_data);
    if (basic != .http) return basic;

    // For GET requests, check if it's a WebSocket upgrade
    if (peek_data.len >= 4 and std.mem.eql(u8, peek_data[0..4], "GET ")) {
        // Look for "Upgrade:" header (case-insensitive search)
        if (containsUpgradeWebsocket(peek_data)) return .websocket;
    }

    return .http;
}

/// Check if HTTP headers contain `Upgrade: websocket` (case-insensitive).
fn containsUpgradeWebsocket(data: []const u8) bool {
    // Simple scan for "upgrade:" followed by "websocket"
    const needle_lower = "upgrade:";
    var i: usize = 0;
    while (i + needle_lower.len <= data.len) : (i += 1) {
        var match = true;
        for (0..needle_lower.len) |j| {
            const c = data[i + j];
            const lower_c = if (c >= 'A' and c <= 'Z') c + 32 else c;
            if (lower_c != needle_lower[j]) {
                match = false;
                break;
            }
        }
        if (match) {
            // Found "Upgrade:" — now look for "websocket" after it
            const after = i + needle_lower.len;
            var k = after;
            // Skip whitespace
            while (k < data.len and (data[k] == ' ' or data[k] == '\t')) : (k += 1) {}
            const ws_needle = "websocket";
            if (k + ws_needle.len <= data.len) {
                var ws_match = true;
                for (0..ws_needle.len) |j| {
                    const c = data[k + j];
                    const lower_c = if (c >= 'A' and c <= 'Z') c + 32 else c;
                    if (lower_c != ws_needle[j]) {
                        ws_match = false;
                        break;
                    }
                }
                if (ws_match) return true;
            }
        }
    }
    return false;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Connection State
// ═══════════════════════════════════════════════════════════════════════════════

/// Connection lifecycle state.
pub const State = enum(u8) {
    /// Normal operation — reading requests, sending responses.
    active = 0,
    /// Graceful shutdown — finish pending writes, reject new requests.
    draining = 1,
    /// Pending close — will be cleaned up on next reactor tick.
    closing = 2,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Connection
// ═══════════════════════════════════════════════════════════════════════════════

pub const Connection = struct {
    /// File descriptor (or -1 if not yet assigned).
    fd: i32,

    /// Detected wire protocol.
    protocol: Protocol,

    /// Lifecycle state.
    state: State,

    /// Inbound data buffer.
    read_buf: RingBuffer,

    /// Outbound data buffer (write coalescing).
    write_buf: RingBuffer,

    /// Pinned namespace (from auth or first request).
    namespace: ?[]const u8,

    /// Authenticated user ID.
    user_id: ?[]const u8,

    /// Total requests processed on this connection.
    requests_total: u64,

    /// Requests forwarded to other shards (for migration heuristics).
    forward_count: u64,

    /// Connection creation timestamp (nanos).
    connected_at: i64,

    /// Last activity timestamp (nanos).
    last_active: i64,

    /// Whether the write side has been armed in the reactor.
    write_armed: bool,

    /// Unique connection ID within the shard.
    id: u32,

    /// Set by handlers that intentionally defer the response (e.g. blocking GET).
    /// processRequests checks this to suppress the default "not implemented" error.
    response_deferred: bool,

    pub fn init(allocator: std.mem.Allocator, fd: i32, conn_id: u32) !Connection {
        var read_buf = try RingBuffer.init(allocator);
        errdefer read_buf.deinit();
        var write_buf = try RingBuffer.init(allocator);
        errdefer write_buf.deinit();

        const now = @import("stdx").time.milliTimestamp();
        return .{
            .fd = fd,
            .protocol = .unknown,
            .state = .active,
            .read_buf = read_buf,
            .write_buf = write_buf,
            .namespace = null,
            .user_id = null,
            .requests_total = 0,
            .forward_count = 0,
            .connected_at = now,
            .last_active = now,
            .write_armed = false,
            .id = conn_id,
            .response_deferred = false,
        };
    }

    pub fn deinit(self: *Connection) void {
        self.read_buf.deinit();
        self.write_buf.deinit();
    }

    /// Detect protocol from the read buffer contents.
    pub fn detectAndSetProtocol(self: *Connection) void {
        const data = self.read_buf.peek();
        self.protocol = detectProtocolFull(data);
    }

    /// Queue response data for write coalescing.
    /// Returns the number of bytes queued.
    pub fn queueWrite(self: *Connection, data: []const u8) usize {
        const written = self.write_buf.write(data);
        return written;
    }

    /// Check if there's pending write data.
    pub fn hasPendingWrites(self: *const Connection) bool {
        return self.write_buf.readable() > 0;
    }

    /// Get pending write data (first contiguous chunk).
    pub fn pendingWriteData(self: *const Connection) []const u8 {
        return self.write_buf.peek();
    }

    /// Consume bytes from the write buffer after successful send.
    pub fn consumeWritten(self: *Connection, n: usize) void {
        self.write_buf.consume(n);
    }

    /// Transition to draining state.
    pub fn drain(self: *Connection) void {
        if (self.state == .active) {
            self.state = .draining;
        }
    }

    /// Transition to closing state.
    pub fn close(self: *Connection) void {
        self.state = .closing;
    }

    /// Record a dispatched request.
    pub fn recordRequest(self: *Connection) void {
        self.requests_total += 1;
        self.last_active = @import("stdx").time.milliTimestamp();
    }

    /// Record a forwarded request.
    pub fn recordForward(self: *Connection) void {
        self.forward_count += 1;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Connection: protocol detection — binary (Flo magic)" {
    const data = [_]u8{ 0x46, 0x4C, 0x4F, 0x00, 0x01, 0x02, 0x03 };
    try std.testing.expectEqual(Protocol.binary, detectProtocol(&data));
}

test "Connection: protocol detection — RESP" {
    // Redis client sends: *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n
    const data = "*3\r\n$3\r\nSET\r\n";
    try std.testing.expectEqual(Protocol.resp, detectProtocol(data));
}

test "Connection: protocol detection — HTTP GET" {
    const data = "GET /api/v1/health HTTP/1.1\r\nHost: localhost\r\n\r\n";
    try std.testing.expectEqual(Protocol.http, detectProtocol(data));
}

test "Connection: protocol detection — HTTP POST" {
    const data = "POST /api/v1/kv HTTP/1.1\r\n";
    try std.testing.expectEqual(Protocol.http, detectProtocol(data));
}

test "Connection: protocol detection — HTTP PUT" {
    const data = "PUT /api/v1/kv/key HTTP/1.1\r\n";
    try std.testing.expectEqual(Protocol.http, detectProtocol(data));
}

test "Connection: protocol detection — HTTP DELETE" {
    const data = "DELETE /api/v1/kv/key HTTP/1.1\r\n";
    try std.testing.expectEqual(Protocol.http, detectProtocol(data));
}

test "Connection: protocol detection — WebSocket upgrade" {
    const data = "GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n";
    try std.testing.expectEqual(Protocol.websocket, detectProtocolFull(data));
}

test "Connection: protocol detection — WebSocket case insensitive" {
    const data = "GET /ws HTTP/1.1\r\nHost: localhost\r\nUPGRADE: WebSocket\r\nConnection: Upgrade\r\n\r\n";
    try std.testing.expectEqual(Protocol.websocket, detectProtocolFull(data));
}

test "Connection: protocol detection — unknown (too few bytes)" {
    const data = [_]u8{ 0x46, 0x4C };
    try std.testing.expectEqual(Protocol.unknown, detectProtocol(&data));
}

test "Connection: protocol detection — empty" {
    try std.testing.expectEqual(Protocol.unknown, detectProtocol(""));
}

test "Connection: RingBuffer basic write/read" {
    var rb = try RingBuffer.initWithCapacity(std.testing.allocator, 16);
    defer rb.deinit();

    try std.testing.expectEqual(@as(usize, 0), rb.readable());
    try std.testing.expectEqual(@as(usize, 16), rb.writable());

    const written = rb.write("hello");
    try std.testing.expectEqual(@as(usize, 5), written);
    try std.testing.expectEqual(@as(usize, 5), rb.readable());

    var out: [16]u8 = undefined;
    const n = rb.read(&out);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("hello", out[0..5]);
    try std.testing.expectEqual(@as(usize, 0), rb.readable());
}

test "Connection: RingBuffer wrap-around" {
    var rb = try RingBuffer.initWithCapacity(std.testing.allocator, 8);
    defer rb.deinit();

    // Fill 6 bytes, consume 4, write 6 more (wraps)
    _ = rb.write("abcdef");
    rb.consume(4); // consume "abcd", read_pos=4
    _ = rb.write("ghijkl"); // wraps around

    try std.testing.expectEqual(@as(usize, 8), rb.readable());

    var out: [8]u8 = undefined;
    const n = rb.read(&out);
    try std.testing.expectEqual(@as(usize, 8), n);
    try std.testing.expectEqualStrings("efghijkl", out[0..8]);
}

test "Connection: write coalescing" {
    var conn = try Connection.init(std.testing.allocator, 42, 1);
    defer conn.deinit();

    // Queue multiple small writes
    _ = conn.queueWrite("resp1");
    _ = conn.queueWrite("resp2");
    _ = conn.queueWrite("resp3");

    try std.testing.expect(conn.hasPendingWrites());

    // All data coalesced in write buffer
    const pending = conn.pendingWriteData();
    try std.testing.expectEqualStrings("resp1resp2resp3", pending);

    // Simulate partial flush
    conn.consumeWritten(5); // sent "resp1"
    const remaining = conn.pendingWriteData();
    try std.testing.expectEqualStrings("resp2resp3", remaining);
}

test "Connection: state transitions" {
    var conn = try Connection.init(std.testing.allocator, 42, 1);
    defer conn.deinit();

    try std.testing.expectEqual(State.active, conn.state);

    conn.drain();
    try std.testing.expectEqual(State.draining, conn.state);

    conn.close();
    try std.testing.expectEqual(State.closing, conn.state);
}

test "Connection: detect protocol from read buffer" {
    var conn = try Connection.init(std.testing.allocator, 42, 1);
    defer conn.deinit();

    // Write binary magic into read buffer
    _ = conn.read_buf.write(&[_]u8{ 0x46, 0x4C, 0x4F, 0x00 });
    conn.detectAndSetProtocol();
    try std.testing.expectEqual(Protocol.binary, conn.protocol);
}

test "Connection: detect RESP from read buffer" {
    var conn = try Connection.init(std.testing.allocator, 42, 1);
    defer conn.deinit();

    _ = conn.read_buf.write("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    conn.detectAndSetProtocol();
    try std.testing.expectEqual(Protocol.resp, conn.protocol);
}
