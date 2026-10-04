//! Accept loop for the small HTTP listeners (dashboard, metrics).
//!
//! Requests are read without blocking, many connections at once, each with a
//! deadline, so a client that connects and sends nothing slowly holds up no
//! one: `/health` answers while it waits. A request read whole is handed to
//! the handler, one at a time, on the loop's thread. When every slot is
//! taken, the connection that has waited longest for its request is dropped
//! to make room: a newcomer always gets in.

const std = @import("std");
const posix = std.posix;
const stdx = @import("stdx");
const request = @import("request.zig");

/// How long a client has to send its whole request.
pub const READ_TIMEOUT_MS: i64 = 5_000;
/// How long a response write may stall before the connection is dropped.
pub const WRITE_TIMEOUT_S: i64 = 5;
/// Connections being read at once.
pub const MAX_CONNECTIONS: usize = 64;
/// The largest request read; a longer one is handed on cut at this size.
pub const MAX_REQUEST: usize = 8192;

const Conn = struct {
    fd: posix.socket_t = -1,
    deadline_ms: i64 = 0,
    len: usize = 0,
    buf: [MAX_REQUEST]u8 = undefined,
};

/// Serve `listener` until `running` is cleared, handing each whole request
/// to `handler.handle(fd, bytes)`. The socket is blocking with a write
/// timeout while the handler runs, and closed after it returns.
pub fn serve(allocator: std.mem.Allocator, listener: posix.socket_t, running: *const std.atomic.Value(bool), handler: anytype) void {
    const conns = allocator.alloc(Conn, MAX_CONNECTIONS) catch return;
    defer allocator.free(conns);
    for (conns) |*c| c.* = .{};
    defer for (conns) |*c| if (c.fd >= 0) close(c);
    stdx.net.sysFcntlSetNonblocking(listener) catch {};

    var fds: [1 + MAX_CONNECTIONS]posix.pollfd = undefined;
    var slot_of: [1 + MAX_CONNECTIONS]usize = undefined;
    while (running.load(.acquire)) {
        var n: usize = 0;
        fds[n] = .{ .fd = listener, .events = posix.POLL.IN, .revents = 0 };
        n += 1;
        for (conns, 0..) |*c, i| {
            if (c.fd < 0) continue;
            fds[n] = .{ .fd = c.fd, .events = posix.POLL.IN, .revents = 0 };
            slot_of[n] = i;
            n += 1;
        }
        _ = posix.poll(fds[0..n], 100) catch 0;
        if (!running.load(.acquire)) break;

        for (fds[1..n], slot_of[1..n]) |pf, i| {
            if (pf.revents == 0) continue;
            readInto(&conns[i], handler);
        }
        if (fds[0].revents != 0) acceptAll(listener, conns);

        const now = stdx.time.milliTimestamp();
        for (conns) |*c| {
            if (c.fd >= 0 and now > c.deadline_ms) close(c);
        }
    }
}

fn acceptAll(listener: posix.socket_t, conns: []Conn) void {
    while (true) {
        const fd = stdx.net.sysAccept(listener, null, null, 0) catch return;
        stdx.net.sysFcntlSetNonblocking(fd) catch {
            _ = std.c.close(fd);
            continue;
        };
        const slot = freeSlot(conns) orelse oldest(conns);
        if (slot.fd >= 0) close(slot);
        slot.* = .{ .fd = fd, .deadline_ms = stdx.time.milliTimestamp() + READ_TIMEOUT_MS };
    }
}

fn freeSlot(conns: []Conn) ?*Conn {
    for (conns) |*c| if (c.fd < 0) return c;
    return null;
}

fn oldest(conns: []Conn) *Conn {
    var o = &conns[0];
    for (conns) |*c| {
        if (c.deadline_ms < o.deadline_ms) o = c;
    }
    return o;
}

fn readInto(c: *Conn, handler: anytype) void {
    const rc = std.c.read(c.fd, c.buf[c.len..].ptr, c.buf.len - c.len);
    if (rc < 0) {
        if (posix.errno(rc) == .AGAIN) return;
        return close(c);
    }
    if (rc == 0) {
        // The client finished sending: what came is the request.
        if (c.len == 0) return close(c);
        return dispatch(c, handler);
    }
    c.len += @intCast(rc);
    const whole = if (request.getExpectedSize(c.buf[0..c.len])) |expected| c.len >= expected else false;
    if (whole or c.len == c.buf.len) dispatch(c, handler);
}

fn dispatch(c: *Conn, handler: anytype) void {
    defer close(c);
    // The handler writes as before: blocking, but never for long.
    stdx.net.sysFcntlSetBlocking(c.fd) catch return;
    const tv = std.posix.timeval{ .sec = @intCast(WRITE_TIMEOUT_S), .usec = 0 };
    _ = std.c.setsockopt(c.fd, posix.SOL.SOCKET, posix.SO.SNDTIMEO, std.mem.asBytes(&tv), @sizeOf(std.posix.timeval));
    handler.handle(c.fd, c.buf[0..c.len]);
}

fn close(c: *Conn) void {
    // SO_LINGER so a response's last bytes are sent before FIN; raw std.c
    // because the std wrapper panics on an fd the peer already reset.
    const linger = extern struct { l_onoff: c_int, l_linger: c_int }{ .l_onoff = 1, .l_linger = 2 };
    _ = std.c.setsockopt(c.fd, posix.SOL.SOCKET, posix.SO.LINGER, &linger, @sizeOf(@TypeOf(linger)));
    _ = std.c.close(c.fd);
    c.* = .{};
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

const Echo = struct {
    served: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    fn handle(self: *Echo, fd: posix.socket_t, bytes: []const u8) void {
        _ = self.served.fetchAdd(1, .monotonic);
        const body = "ok";
        var hdr: [128]u8 = undefined;
        const h = std.fmt.bufPrint(&hdr, "HTTP/1.1 200 OK\r\nContent-Length: {d}\r\n\r\n", .{body.len}) catch return;
        _ = std.c.write(fd, h.ptr, h.len);
        _ = std.c.write(fd, body.ptr, body.len);
        _ = bytes;
    }
};

fn listenLocal() !struct { fd: posix.socket_t, port: u16 } {
    const fd = try stdx.net.sysSocket(posix.AF.INET, posix.SOCK.STREAM, 0);
    const addr = stdx.net.SocketAddrV4.initIp4(.{ 127, 0, 0, 1 }, 0);
    try stdx.net.sysBind(fd, addr.anyPtr(), addr.anyLen());
    try stdx.net.sysListen(fd, 128);
    return .{ .fd = fd, .port = (try stdx.net.sysLocalIp4(fd)).port };
}

fn get(port: u16) ![]const u8 {
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 1000);
    defer _ = std.c.close(fd);
    const req = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";
    _ = std.c.write(fd, req, req.len);
    var buf: [256]u8 = undefined;
    var got: usize = 0;
    var waited: u64 = 0;
    while (waited < 3000) : (waited += 10) {
        const rc = std.c.read(fd, buf[got..].ptr, buf.len - got);
        if (rc > 0) got += @intCast(rc) else if (rc == 0) break else stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    return if (std.mem.indexOf(u8, buf[0..got], "200 OK") != null) "200" else "none";
}

test "serve: clients that connect and send nothing hold up no one, even past the connection cap" {
    const l = try listenLocal();
    var running = std.atomic.Value(bool).init(true);
    var echo = Echo{};
    const t = try std.Thread.spawn(.{}, serve, .{ testing.allocator, l.fd, &running, &echo });
    defer {
        running.store(false, .release);
        t.join();
        _ = std.c.close(l.fd);
    }
    // More idle connections than there are slots.
    var idle: [MAX_CONNECTIONS + 8]posix.socket_t = undefined;
    for (&idle) |*fd| fd.* = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, l.port, 1000);
    defer for (idle) |fd| {
        _ = std.c.close(fd);
    };
    stdx.time.sleep(200 * std.time.ns_per_ms);
    // A real request still gets its answer, at once.
    const start = stdx.time.milliTimestamp();
    try testing.expectEqualStrings("200", try get(l.port));
    try testing.expect(stdx.time.milliTimestamp() - start < READ_TIMEOUT_MS);
    try testing.expectEqual(@as(u32, 1), echo.served.load(.monotonic));
}

test "serve: a connection that has not sent its whole request in time is closed" {
    const l = try listenLocal();
    var running = std.atomic.Value(bool).init(true);
    var echo = Echo{};
    const t = try std.Thread.spawn(.{}, serve, .{ testing.allocator, l.fd, &running, &echo });
    defer {
        running.store(false, .release);
        t.join();
        _ = std.c.close(l.fd);
    }
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, l.port, 1000);
    defer _ = std.c.close(fd);
    _ = std.c.write(fd, "GET /hea", 8);
    stdx.time.sleep(@intCast((READ_TIMEOUT_MS + 500) * std.time.ns_per_ms));
    var b: [16]u8 = undefined;
    try testing.expectEqual(@as(isize, 0), std.c.read(fd, &b, b.len));
    try testing.expectEqual(@as(u32, 0), echo.served.load(.monotonic));
}
