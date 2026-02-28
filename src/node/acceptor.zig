//! Acceptor — dedicated thread for TCP accept + routing hand-off
//!
//! The Acceptor owns the listen socket. For each accepted connection,
//! it peeks the first bytes to determine the target shard, then writes
//! the raw fd integer through the shard's pipe.
//!
//! ## Three Peek Outcomes
//!
//! | Outcome | Condition | Routing |
//! |---------|-----------|---------|
//! | A: Full header | 24-byte RequestHeader | hash(ns, key) → shard |
//! | B: Insufficient | < 24 bytes peeked | Round-robin |
//! | C: HTTP/WS | Starts with `GET` etc. | Dashboard shard (0) |
//!
//! ## FD Hand-Off
//!
//! All threads share the same process fd table, so the Acceptor just
//! writes the `i32` fd through a pipe. The shard's Reactor wakes on
//! the pipe becoming readable, reads the fd, and registers it.

const std = @import("std");
const proto = @import("../protocol/proto.zig");
const Router = @import("router.zig").Router;
const routing = @import("router.zig");
const connection = @import("connection.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Acceptor
// ═══════════════════════════════════════════════════════════════════════════════

pub const Acceptor = struct {
    /// Listen socket fd.
    listen_fd: i32,

    /// Per-shard pipe write ends. Index = shard_id.
    shard_pipes: []i32,

    /// Router for mapping keys to shards.
    router: Router,

    /// Round-robin counter for fallback routing.
    rr_counter: u32,

    /// Number of shards.
    shard_count: u16,

    /// Whether the acceptor should keep running.
    running: std.atomic.Value(bool),

    /// Stats: total connections accepted.
    accepted_total: u64,

    /// Stats: connections routed by key.
    routed_by_key: u64,

    /// Stats: connections round-robined.
    routed_round_robin: u64,

    pub fn init(
        shard_pipes: []i32,
        router: Router,
    ) Acceptor {
        return .{
            .listen_fd = -1,
            .shard_pipes = shard_pipes,
            .router = router,
            .rr_counter = 0,
            .shard_count = @intCast(shard_pipes.len),
            .running = std.atomic.Value(bool).init(false),
            .accepted_total = 0,
            .routed_by_key = 0,
            .routed_round_robin = 0,
        };
    }

    /// Bind and listen on the given port. Returns the listen fd.
    pub fn listen(self: *Acceptor, port: u16) !void {
        const addr = try std.net.Address.resolveIp("0.0.0.0", port);

        const flags: u32 = std.posix.SOCK.STREAM | std.posix.SOCK.NONBLOCK;
        const fd = try std.posix.socket(addr.any.family, flags, std.posix.IPPROTO.TCP);
        errdefer std.posix.close(fd);

        // SO_REUSEADDR for fast restart
        std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1))) catch {};

        try std.posix.bind(fd, &addr.any, addr.getOsSockLen());
        try std.posix.listen(fd, 128);

        self.listen_fd = fd;
    }

    /// Accept one connection and route it to the correct shard.
    /// Returns the target shard_id, or null if no connection was pending.
    pub fn acceptOne(self: *Acceptor) !?u16 {
        const client_fd = std.posix.accept(self.listen_fd, null, null, std.posix.SOCK.NONBLOCK) catch |err| {
            if (err == error.WouldBlock) return null;
            return err;
        };
        errdefer std.posix.close(client_fd);

        // TCP_NODELAY
        setTcpNodelay(client_fd);

        // Peek first bytes for routing
        const target = self.peekAndRoute(client_fd);

        // Hand off fd to shard via pipe
        try self.handoff(client_fd, target);

        self.accepted_total += 1;
        return target;
    }

    /// Peek at the first bytes using MSG_PEEK and determine the target shard.
    fn peekAndRoute(self: *Acceptor, fd: i32) u16 {
        var peek_buf: [128]u8 = undefined;
        const peeked = peekFd(fd, &peek_buf);

        if (peeked.len == 0) {
            return self.roundRobin();
        }

        // Check protocol
        const proto_type = connection.detectProtocol(peeked);
        switch (proto_type) {
            .binary => {
                // Try to parse RequestHeader for routing
                if (peeked.len >= @sizeOf(proto.RequestHeader)) {
                    return self.routeFromHeader(peeked) orelse self.roundRobin();
                }
                return self.roundRobin();
            },
            .http, .websocket => {
                // HTTP/WS → dashboard shard (shard 0)
                self.routed_round_robin += 1;
                return 0;
            },
            .resp => {
                // RESP → round-robin (we can't easily peek the key from RESP framing)
                return self.roundRobin();
            },
            .unknown => {
                return self.roundRobin();
            },
        }
    }

    /// Try to extract routing information from a binary protocol header.
    fn routeFromHeader(self: *Acceptor, data: []const u8) ?u16 {
        if (data.len < @sizeOf(proto.RequestHeader)) return null;

        const header_bytes = data[0..@sizeOf(proto.RequestHeader)];
        const header: *const proto.RequestHeader = @ptrCast(@alignCast(header_bytes.ptr));

        // Validate magic
        if (header.magic != proto.MAGIC) return null;

        // For system commands (ping, auth, etc.), route to shard 0
        if (header.op_code <= 0x0F) {
            self.routed_by_key += 1;
            return 0;
        }

        // Workflow opcodes (0x80-0x93) are centralised on shard 0.
        // workflow_create sends key="" (name is parsed from YAML server-side),
        // so key-based routing would scatter definitions across shards while
        // queries land on a different shard. Pin everything to shard 0.
        if (header.op_code >= 0x80 and header.op_code <= 0x93) {
            self.routed_by_key += 1;
            return 0;
        }

        // Need more than just the header to get namespace+key for routing.
        // If we got enough bytes, parse the payload length and try to extract key.
        // For the initial implementation, treat partial-payload as round-robin.
        const payload_start = @sizeOf(proto.RequestHeader);
        if (data.len <= payload_start) return null;

        // The payload after the header typically starts with namespace-len, namespace,
        // key-len, key. Try to extract at least the key for routing.
        const payload = data[payload_start..];
        const key = extractKeyFromPayload(payload) orelse return null;

        if (key.len > 0) {
            const hash = routing.hashKey(key);
            const partition = self.router.hashToPartition(hash);
            const shard_id = self.router.partitionToShard(partition);
            self.routed_by_key += 1;
            return shard_id;
        }

        return null;
    }

    /// Round-robin shard selection.
    fn roundRobin(self: *Acceptor) u16 {
        const target = @as(u16, @intCast(self.rr_counter % self.shard_count));
        self.rr_counter +%= 1;
        self.routed_round_robin += 1;
        return target;
    }

    /// Hand off an fd to a shard via its pipe.
    fn handoff(self: *Acceptor, client_fd: i32, shard_id: u16) !void {
        const pipe_fd = self.shard_pipes[shard_id];
        const fd_bytes = std.mem.asBytes(&client_fd);
        const written = try std.posix.write(pipe_fd, fd_bytes);
        if (written != @sizeOf(i32)) return error.ShortWrite;
    }

    /// Stop the acceptor loop.
    pub fn stop(self: *Acceptor) void {
        self.running.store(false, .release);
    }

    /// Close the listen socket.
    pub fn close(self: *Acceptor) void {
        if (self.listen_fd >= 0) {
            std.posix.close(self.listen_fd);
            self.listen_fd = -1;
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Peek at a socket's incoming data using MSG_PEEK (non-consuming).
fn peekFd(fd: i32, buf: []u8) []const u8 {
    const rc = std.posix.system.recvfrom(fd, buf.ptr, buf.len, std.posix.MSG.PEEK, null, null);
    if (rc <= 0) return &[_]u8{};
    return buf[0..@intCast(rc)];
}

/// Try to extract a key from the binary payload following the header.
/// Payload format: [ns_len: u16][namespace: ns_len][key_len: u16][key: key_len]...
fn extractKeyFromPayload(payload: []const u8) ?[]const u8 {
    if (payload.len < 2) return null;

    // Namespace length (little-endian u16)
    const ns_len = std.mem.readInt(u16, payload[0..2], .little);
    const ns_end = @as(usize, 2) + ns_len;
    if (payload.len < ns_end + 2) return null;

    // Key length
    const key_len = std.mem.readInt(u16, payload[ns_end..][0..2], .little);
    const key_start = ns_end + 2;
    const key_end = key_start + key_len;
    if (payload.len < key_end) return null;

    return payload[key_start..key_end];
}

/// Set TCP_NODELAY on a socket.
fn setTcpNodelay(fd: i32) void {
    std.posix.setsockopt(fd, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1))) catch {};
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Acceptor: round-robin routing" {
    var pipes: [2]i32 = .{ -1, -1 }; // dummy pipe fds
    const router = Router.init(4096, 2, 0);
    var acceptor = Acceptor.init(&pipes, router);

    // Round-robin should cycle through shards
    const s0 = acceptor.roundRobin();
    const s1 = acceptor.roundRobin();
    const s2 = acceptor.roundRobin();

    try std.testing.expectEqual(@as(u16, 0), s0);
    try std.testing.expectEqual(@as(u16, 1), s1);
    try std.testing.expectEqual(@as(u16, 0), s2);
}

test "Acceptor: extractKeyFromPayload" {
    // Build payload: ns_len=4, ns="test", key_len=5, key="hello"
    var buf: [32]u8 = undefined;
    std.mem.writeInt(u16, buf[0..2], 4, .little); // ns_len
    @memcpy(buf[2..6], "test"); // namespace
    std.mem.writeInt(u16, buf[6..8], 5, .little); // key_len
    @memcpy(buf[8..13], "hello"); // key

    const key = extractKeyFromPayload(&buf);
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("hello", key.?);
}

test "Acceptor: extractKeyFromPayload — too short" {
    const buf = [_]u8{0x00};
    try std.testing.expectEqual(@as(?[]const u8, null), extractKeyFromPayload(&buf));
}

test "Acceptor: listen and accept via TCP" {
    // Create shard pipes
    const pipe0 = try std.posix.pipe();
    defer std.posix.close(pipe0[0]);
    defer std.posix.close(pipe0[1]);

    const pipe1 = try std.posix.pipe();
    defer std.posix.close(pipe1[0]);
    defer std.posix.close(pipe1[1]);

    var shard_pipes = [_]i32{ pipe0[1], pipe1[1] };
    const router = Router.init(4096, 2, 0);
    var acceptor = Acceptor.init(&shard_pipes, router);

    // Listen on ephemeral port
    try acceptor.listen(0);
    defer acceptor.close();

    // Get the port we bound to
    var addr: std.posix.sockaddr.in = undefined;
    var addr_len: std.posix.socklen_t = @sizeOf(@TypeOf(addr));
    try std.posix.getsockname(acceptor.listen_fd, @ptrCast(&addr), &addr_len);
    const port = std.mem.bigToNative(u16, addr.port);

    // Connect a client
    const client = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, std.posix.IPPROTO.TCP);
    defer std.posix.close(client);

    var connect_addr = std.net.Address.resolveIp("127.0.0.1", port) catch unreachable;
    try std.posix.connect(client, &connect_addr.any, connect_addr.getOsSockLen());

    // Accept and route
    const target = try acceptor.acceptOne();
    try std.testing.expect(target != null);

    // Read the handed-off fd from the target shard's pipe
    const pipe_rd = if (target.? == 0) pipe0[0] else pipe1[0];
    var fd_buf: [@sizeOf(i32)]u8 = undefined;
    const n = try std.posix.read(pipe_rd, &fd_buf);
    try std.testing.expectEqual(@as(usize, @sizeOf(i32)), n);

    const handed_fd = std.mem.bytesAsValue(i32, &fd_buf).*;
    defer std.posix.close(handed_fd);
    try std.testing.expect(handed_fd >= 0);

    try std.testing.expectEqual(@as(u64, 1), acceptor.accepted_total);
}
