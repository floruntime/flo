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
//! | A: Full header | 32-byte RequestHeader | hash(ns, key) → shard |
//! | B: Insufficient | < 32 bytes peeked | Round-robin |
//! | C: HTTP/WS | Starts with `GET` etc. | Dashboard shard (0) |
//!
//! ## FD Hand-Off
//!
//! All threads share the same process fd table, so the Acceptor just
//! writes the `i32` fd through a pipe. The shard's Reactor wakes on
//! the pipe becoming readable, reads the fd, and registers it.

const std = @import("std");
const log = @import("stdx").log;
const proto = @import("../protocol/proto.zig");
const Router = @import("router.zig").Router;
const routing = @import("router.zig");
const connection = @import("connection.zig");
const ServerMetrics = @import("../metrics/registry.zig").ServerMetrics;

// ═══════════════════════════════════════════════════════════════════════════════
// Acceptor
// ═══════════════════════════════════════════════════════════════════════════════

/// Set on an fd handed to a shard when it came from the admin socket. Fds
/// are small non-negative numbers, so the bit is free.
pub const ADMIN_TAG: i32 = 1 << 30;

/// Fds a node needs that are not client connections. Roughly fixed: the listen
/// sockets, stdio, the two HTTP servers (64 connections each) and peer links
/// (RaftNode.MAX_PEERS). Per shard: its hand-off pipe and the handful of files
/// it keeps open. Sealed segments are opened to read and closed again, not held
/// open while idle, so they are not in here. The cap leaves this many below the
/// open-file limit so those keep working while connections are held.
pub const RESERVE_FIXED: u32 = 224;
pub const RESERVE_PER_SHARD: u32 = 8;

/// A ceiling on held connections regardless of how high the open-file limit is:
/// each connection costs roughly 125 KiB of RSS (two 64 KiB I/O buffers), so on
/// a host whose limit is very high (a packaged Linux unit allows ~1M) memory
/// would go before descriptors. About 2 GiB of connection buffers. An operator
/// who wants more sets `max_connections`.
pub const MAX_CONNECTIONS_CEILING: u32 = 16_384;

/// The cap when the open-file limit can't be read: a fixed, usable number
/// rather than a derived one (and never 1), with a warning at start.
pub const UNREADABLE_LIMIT_CAP: u32 = 1024;

/// The reserve to leave below the open-file limit, given the shard count.
pub fn connectionReserve(shards: u16) u32 {
    return RESERVE_FIXED + RESERVE_PER_SHARD * @as(u32, shards);
}

/// The connection cap, with whether a floor or fallback worth a warning applied.
pub const Cap = struct {
    value: u32,
    /// The quarter-of-the-limit floor applied — the reserve would have left too
    /// few — so the limit is low and worth raising.
    floored: bool = false,
    /// The limit could not be read; `value` is the fixed fallback.
    limit_unreadable: bool = false,
};

/// A configured value is used as given. Otherwise the cap is the open-file limit
/// minus the reserve, floored at a quarter of the limit (so a low limit still
/// holds a useful number rather than almost none) and bounded by the memory
/// ceiling. An unreadable limit (0) uses a fixed fallback. Pure, so the
/// derivation is tested without a host whose limit varies.
pub fn connectionCap(configured: u32, open_file_limit: u64, shards: u16) Cap {
    if (configured > 0) return .{ .value = configured };
    if (open_file_limit == 0) return .{ .value = UNREADABLE_LIMIT_CAP, .limit_unreadable = true };
    const floor = open_file_limit / 4;
    const after_reserve = open_file_limit -| connectionReserve(shards);
    const base = @max(after_reserve, floor);
    const value: u32 = @intCast(@max(@min(base, @as(u64, MAX_CONNECTIONS_CEILING)), 1));
    return .{ .value = value, .floored = after_reserve < floor };
}

test "acceptor: the connection cap derives from the limit, floors at a quarter, caps at the ceiling, and falls back when unreadable" {
    const E = std.testing.expectEqual;
    // A configured value wins outright.
    try E(@as(u32, 5000), connectionCap(5000, 256, 4).value);
    // Derived: the limit minus the reserve (224 + 8 × shards).
    try E(@as(u32, 10_240 - connectionReserve(4)), connectionCap(0, 10_240, 4).value);
    // The memory ceiling bounds a very high limit (e.g. a packaged Linux unit).
    try E(MAX_CONNECTIONS_CEILING, connectionCap(0, 1_048_576, 4).value);
    // A low limit floors at a quarter, not near-zero, and flags it.
    const low = connectionCap(0, 256, 1); // reserve 232 → 24 after reserve, floor 64
    try E(@as(u32, 64), low.value);
    try E(true, low.floored);
    // An unreadable limit uses the fixed fallback, flagged, never 1.
    const unread = connectionCap(0, 0, 4);
    try E(UNREADABLE_LIMIT_CAP, unread.value);
    try E(true, unread.limit_unreadable);
}

pub const Acceptor = struct {
    /// Listen socket fd.
    listen_fd: i32,

    /// The admin socket's listen fd, or -1 when there is none.
    admin_fd: i32 = -1,

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

    /// The node's live count of held client connections, owned here and
    /// decremented by a shard when one closes — not a metric, so the cap holds
    /// whether or not metrics are on. The acceptor is the only writer that
    /// increments it (reserving a slot before hand-off), so a load-then-reserve
    /// needs no lock. Null only in unit tests that don't exercise the cap.
    active_connections: ?*std.atomic.Value(u32) = null,

    /// Where rejections are counted for `/metrics`, when metrics are on. The cap
    /// itself does not depend on this; it is observation only.
    server_metrics: ?*ServerMetrics = null,

    /// How many connections the node holds at once; 0 means no cap. The runtime
    /// sets it from `connectionCap` after init.
    max_connections: u32 = 0,

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

    /// A socket bound to `bind` (the `[server] bind` address) and `port`,
    /// not yet listening. Binding fixes the port (the OS picks one for 0)
    /// before anything hands it out, while connects are still refused.
    pub fn bindSocket(bind: [4]u8, port: u16) !i32 {
        const stdx_net = @import("stdx").net;
        const addr = stdx_net.SocketAddrV4.initIp4(bind, port);

        const flags: u32 = std.posix.SOCK.STREAM | std.posix.SOCK.NONBLOCK;
        const fd = try stdx_net.sysSocket(std.posix.AF.INET, flags, std.posix.IPPROTO.TCP);
        errdefer _ = std.c.close(fd);

        // SO_REUSEADDR for fast restart
        std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1))) catch {};

        try stdx_net.sysBind(fd, addr.anyPtr(), addr.anyLen());
        return fd;
    }

    /// Listen on `fd`, a socket from `bindSocket`. The acceptor owns it
    /// once this returns.
    pub fn listenOn(self: *Acceptor, fd: i32) !void {
        const stdx_net = @import("stdx").net;
        try stdx_net.sysListen(fd, 128);
        self.listen_fd = fd;
        if (stdx_net.sysLocalIp4(fd)) |local| {
            log.debug("Acceptor listening on {d}.{d}.{d}.{d}:{d} fd={d} shard_count={d}", .{ local.ip4[0], local.ip4[1], local.ip4[2], local.ip4[3], local.port, fd, self.shard_count });
        } else |_| {}
    }

    /// Bind to `bind` and listen on `port` in one step.
    pub fn listen(self: *Acceptor, bind: [4]u8, port: u16) !void {
        const fd = try bindSocket(bind, port);
        errdefer _ = std.c.close(fd);
        try self.listenOn(fd);
    }

    /// Accept one connection and route it to the correct shard.
    /// Returns the target shard_id, or null if no connection was pending.
    pub fn acceptOne(self: *Acceptor) !?u16 {
        const stdx_net = @import("stdx").net;
        const client_fd = stdx_net.sysAccept(self.listen_fd, null, null, std.posix.SOCK.NONBLOCK) catch |err| {
            if (err == error.AcceptFailed) {
                // Could be WouldBlock — surface as null
                const errno = std.posix.errno(@as(c_int, -1));
                if (errno == .AGAIN) return null;
                return err;
            }
            return err;
        };
        errdefer _ = std.c.close(client_fd);

        // At the connection cap, take the connection off the backlog and close
        // it at once rather than hold it: the client is told now instead of
        // hanging, and the node keeps the descriptors and memory its
        // connections, files and peers need. The count is the node's own, not a
        // metric, so the cap holds with metrics off; rejections are counted for
        // /metrics only when they are on.
        if (self.max_connections > 0) if (self.active_connections) |active| {
            if (active.load(.monotonic) >= self.max_connections) {
                _ = std.c.close(client_fd);
                if (self.server_metrics) |sm| sm.recordConnectionRejected();
                return null;
            }
            // Reserve the slot before hand-off so a burst can't overshoot while
            // a shard is still counting the ones already sent; the shard
            // releases it when the connection closes, and the errdefer below
            // releases it if the hand-off fails.
            _ = active.fetchAdd(1, .monotonic);
        };
        errdefer if (self.max_connections > 0) if (self.active_connections) |active| {
            _ = active.fetchSub(1, .monotonic);
        };

        // Make socket non-blocking explicitly (since SOCK.NONBLOCK on accept
        // is not portable across all macOS releases).
        stdx_net.sysFcntlSetNonblocking(client_fd) catch {};

        // TCP_NODELAY
        setTcpNodelay(client_fd);

        // Peek first bytes for routing
        const target = self.peekAndRoute(client_fd);
        log.debug("Acceptor accepted fd={d} → shard {d} (total={d})", .{ client_fd, target, self.accepted_total + 1 });

        // Hand off fd to shard via pipe
        try self.handoff(client_fd, target);

        self.accepted_total += 1;
        return target;
    }

    /// Accept one connection on the admin socket, if one is waiting, and
    /// hand it to shard 0 tagged as an admin connection: admin ops act on
    /// the node, and every node in a cluster runs one shard.
    pub fn acceptAdmin(self: *Acceptor) !void {
        if (self.admin_fd < 0) return;
        const rc = std.c.accept(self.admin_fd, null, null);
        if (rc < 0) return;
        const fd: i32 = @intCast(rc);
        errdefer _ = std.c.close(fd);
        std.debug.assert(fd & ADMIN_TAG == 0);
        @import("stdx").net.sysFcntlSetNonblocking(fd) catch {};
        try self.handoff(fd | ADMIN_TAG, 0);
        self.accepted_total += 1;
    }

    /// Peek at the first bytes using MSG_PEEK and determine the target shard.
    ///
    /// We use a brief `poll()` to detect whether the client has already sent
    /// bytes. If yes, we `MSG_PEEK` to extract the routing key. If the fd is
    /// still idle after the wait, we **must not** call `MSG_PEEK` — under
    /// Docker Desktop's userland proxy (vpnkit) on macOS, performing
    /// `recvfrom(MSG_PEEK)` on an idle accepted fd corrupts the proxy's state
    /// so that subsequent writes on sibling connections from the same source
    /// IP are silently dropped (writes succeed at the container kernel, but
    /// bytes never reach the client). Confirmed empirically: skipping the
    /// `MSG_PEEK` for idle fds takes the docker dual-join from 0/5 → 5/5
    /// passes; native (kqueue) is unaffected.
    ///
    /// Idle connections fall back to round-robin routing; if the request
    /// later turns out to belong on a different shard, the async
    /// `forwardToShard` inbox path forwards it to the data shard.
    fn peekAndRoute(self: *Acceptor, fd: i32) u16 {
        var peek_buf: [128]u8 = undefined;

        // Wait up to 10 ms for data to arrive. Typically resolves in < 1 ms.
        var fds = [_]std.posix.pollfd{.{
            .fd = fd,
            .events = std.posix.POLL.IN,
            .revents = 0,
        }};
        _ = std.posix.poll(&fds, 10) catch {};

        // Only call MSG_PEEK if poll reported readable data. An idle fd
        // (no POLL.IN) gets round-robined without ever touching the
        // kernel's recv buffer — see comment above.
        const ready = (fds[0].revents & std.posix.POLL.IN) != 0;
        if (!ready) {
            return self.roundRobin();
        }

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
            .http => {
                // HTTP → dashboard shard (shard 0)
                self.routed_round_robin += 1;
                return 0;
            },
            .unknown => {
                return self.roundRobin();
            },
        }
    }

    /// Try to extract routing information from a binary protocol header.
    fn routeFromHeader(self: *Acceptor, data: []const u8) ?u16 {
        if (data.len < @sizeOf(proto.RequestHeader)) return null;

        // Copied out: the peek buffer is bytes, with no alignment for the
        // header's u64 fields.
        const header = std.mem.bytesToValue(proto.RequestHeader, data[0..@sizeOf(proto.RequestHeader)]);

        // Not a frame this build can read past its version byte: the shard
        // it lands on refuses it.
        if (header.magic != proto.MAGIC or header.foreign()) return null;

        // For system commands (ping, auth, etc.), route to shard 0
        if (header.op_code <= 0x0F) {
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
        const written = try @import("stdx").net.sysWrite(pipe_fd, fd_bytes);
        if (written != @sizeOf(i32)) return error.ShortWrite;
    }

    /// Stop the acceptor loop.
    pub fn stop(self: *Acceptor) void {
        self.running.store(false, .release);
    }

    /// Close the listen sockets.
    pub fn close(self: *Acceptor) void {
        if (self.listen_fd >= 0) {
            _ = std.c.close(self.listen_fd);
            self.listen_fd = -1;
        }
        if (self.admin_fd >= 0) {
            _ = std.c.close(self.admin_fd);
            self.admin_fd = -1;
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
    @import("stdx").net.setConnectionOption(fd, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1)));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Acceptor: socket options on a closed connection are ignored" {
    const c = std.c;
    const listener = c.socket(c.AF.INET, c.SOCK.STREAM, 0);
    defer _ = c.close(listener);
    var addr: c.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    try std.testing.expectEqual(@as(c_int, 0), c.bind(listener, @ptrCast(&addr), @sizeOf(c.sockaddr.in)));
    try std.testing.expectEqual(@as(c_int, 0), c.listen(listener, 1));
    var len: c.socklen_t = @sizeOf(c.sockaddr.in);
    try std.testing.expectEqual(@as(c_int, 0), c.getsockname(listener, @ptrCast(&addr), &len));

    const dialer = c.socket(c.AF.INET, c.SOCK.STREAM, 0);
    try std.testing.expectEqual(@as(c_int, 0), c.connect(dialer, @ptrCast(&addr), @sizeOf(c.sockaddr.in)));
    const no_linger = extern struct { onoff: c_int, linger: c_int }{ .onoff = 1, .linger = 0 };
    _ = c.setsockopt(dialer, std.posix.SOL.SOCKET, std.posix.SO.LINGER, &no_linger, @sizeOf(@TypeOf(no_linger)));
    _ = c.close(dialer);
    @import("stdx").time.sleep(20 * std.time.ns_per_ms);

    const accepted = c.accept(listener, null, null);
    try std.testing.expect(accepted >= 0);
    defer _ = c.close(accepted);
    setTcpNodelay(accepted);
}

test "Acceptor: at the connection cap, a new connection is accepted and closed, and counted" {
    const c = std.c;
    const listener = c.socket(c.AF.INET, c.SOCK.STREAM, 0);
    defer _ = c.close(listener);
    var addr: c.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    try std.testing.expectEqual(@as(c_int, 0), c.bind(listener, @ptrCast(&addr), @sizeOf(c.sockaddr.in)));
    try std.testing.expectEqual(@as(c_int, 0), c.listen(listener, 4));
    var len: c.socklen_t = @sizeOf(c.sockaddr.in);
    try std.testing.expectEqual(@as(c_int, 0), c.getsockname(listener, @ptrCast(&addr), &len));
    @import("stdx").net.sysFcntlSetNonblocking(listener) catch {};

    var pipes: [1]i32 = .{-1};
    var acceptor = Acceptor.init(&pipes, Router.init(4096, 1, 0));
    acceptor.listen_fd = listener;
    var active = std.atomic.Value(u32).init(2); // already at the cap
    var sm = ServerMetrics.init();
    acceptor.active_connections = &active;
    acceptor.server_metrics = &sm;
    acceptor.max_connections = 2;

    const dialer = c.socket(c.AF.INET, c.SOCK.STREAM, 0);
    defer _ = c.close(dialer);
    try std.testing.expectEqual(@as(c_int, 0), c.connect(dialer, @ptrCast(&addr), @sizeOf(c.sockaddr.in)));

    // The connection is accepted and closed at once, counted, and no slot is
    // reserved (the count is unchanged). Retry until it lands in the backlog.
    var tries: usize = 0;
    while (sm.connections_rejected_total.load(.monotonic) == 0 and tries < 100) : (tries += 1) {
        _ = try acceptor.acceptOne();
        @import("stdx").time.sleep(2 * std.time.ns_per_ms);
    }
    try std.testing.expectEqual(@as(u64, 1), sm.connections_rejected_total.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 2), active.load(.monotonic));
}

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

test "Acceptor: a header at an odd offset in the peek buffer is read, not trapped on" {
    var pipes: [2]i32 = .{ -1, -1 };
    var acceptor = Acceptor.init(&pipes, Router.init(4096, 2, 0));
    const header = proto.RequestHeader.init(@fromBackingInt(@intCast(0x01)), 0);
    // One byte past an 8-byte boundary: a cast would trap in a safe build.
    var buf: [128]u8 align(8) = undefined;
    @memcpy(buf[1..][0..@sizeOf(proto.RequestHeader)], std.mem.asBytes(&header));
    try std.testing.expectEqual(@as(?u16, 0), acceptor.routeFromHeader(buf[1 .. 1 + @sizeOf(proto.RequestHeader)]));

    // A header from another build isn't read for a key.
    var foreign = header;
    foreign.table_hash +%= 1;
    @memcpy(buf[1..][0..@sizeOf(proto.RequestHeader)], std.mem.asBytes(&foreign));
    try std.testing.expectEqual(@as(?u16, null), acceptor.routeFromHeader(buf[1 .. 1 + @sizeOf(proto.RequestHeader)]));
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
    const pipe0 = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe0[0]);
    defer _ = std.c.close(pipe0[1]);

    const pipe1 = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe1[0]);
    defer _ = std.c.close(pipe1[1]);

    var shard_pipes = [_]i32{ pipe0[1], pipe1[1] };
    const router = Router.init(4096, 2, 0);
    var acceptor = Acceptor.init(&shard_pipes, router);

    // Listen on ephemeral port
    try acceptor.listen(.{ 0, 0, 0, 0 }, 0);
    defer acceptor.close();

    // Get the port we bound to
    var addr: std.posix.sockaddr.in = undefined;
    var addr_len: std.posix.socklen_t = @sizeOf(@TypeOf(addr));
    if (std.c.getsockname(acceptor.listen_fd, @ptrCast(&addr), &addr_len) != 0) return error.GetsocknameFailed;
    const port = std.mem.bigToNative(u16, addr.port);

    // Connect a client
    const stdx_net = @import("stdx").net;
    const client = try stdx_net.sysSocket(std.posix.AF.INET, std.posix.SOCK.STREAM, std.posix.IPPROTO.TCP);
    defer _ = std.c.close(client);

    const connect_addr = stdx_net.SocketAddrV4.initIp4(.{ 127, 0, 0, 1 }, port);
    try stdx_net.sysConnect(client, connect_addr.anyPtr(), connect_addr.anyLen());

    // Accept and route — the listen socket is NONBLOCK so we poll briefly
    // to allow the kernel TCP handshake to complete on the server side.
    var target: ?u16 = null;
    for (0..200) |_| {
        target = try acceptor.acceptOne();
        if (target != null) break;
        @import("stdx").time.sleep(1_000_000); // 1ms
    }
    try std.testing.expect(target != null);

    // Read the handed-off fd from the target shard's pipe
    const pipe_rd = if (target.? == 0) pipe0[0] else pipe1[0];
    var fd_buf: [@sizeOf(i32)]u8 = undefined;
    const n = try std.posix.read(pipe_rd, &fd_buf);
    try std.testing.expectEqual(@as(usize, @sizeOf(i32)), n);

    const handed_fd = std.mem.bytesAsValue(i32, &fd_buf).*;
    defer _ = std.c.close(handed_fd);
    try std.testing.expect(handed_fd >= 0);

    try std.testing.expectEqual(@as(u64, 1), acceptor.accepted_total);
}

test "acceptor: listens on the configured bind address" {
    var shard_pipes = [_]i32{ -1, -1 };
    const router = Router.init(4096, 2, 0);
    var acceptor = Acceptor.init(&shard_pipes, router);
    defer if (acceptor.listen_fd >= 0) @import("stdx").net.sysClose(acceptor.listen_fd);
    try acceptor.listen(.{ 127, 0, 0, 1 }, 0);
    const local = try @import("stdx").net.sysLocalIp4(acceptor.listen_fd);
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, local.ip4);
}

test "acceptor: a bound socket refuses connects until it listens" {
    const stdx_net = @import("stdx").net;
    var shard_pipes = [_]i32{ -1, -1 };
    var acceptor = Acceptor.init(&shard_pipes, Router.init(4096, 2, 0));
    const fd = try Acceptor.bindSocket(.{ 127, 0, 0, 1 }, 0);
    var owned = true;
    defer if (owned) stdx_net.sysClose(fd);
    const port = (try stdx_net.sysLocalIp4(fd)).port;
    try std.testing.expect(port != 0);
    try std.testing.expectError(error.ConnectFailed, stdx_net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 200));
    try acceptor.listenOn(fd);
    owned = false;
    defer acceptor.close();
    const client = try stdx_net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 1000);
    _ = std.c.close(client);
}
