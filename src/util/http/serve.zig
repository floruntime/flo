//! Accept loop for the small HTTP listeners (dashboard, metrics).
//!
//! Requests are read without blocking, many connections at once, each with a
//! deadline, so a client that sends slowly or not at all holds up no one:
//! `/health` answers while it waits. A request read whole is handed to the
//! handler, one at a time, on the loop's thread; one past the limits, or
//! whose body is not delimited by one plain Content-Length, is refused and
//! never handed on. When every slot is taken, the connection that has
//! waited longest for its request is dropped to make room: a newcomer
//! always gets in. Each response is written within one deadline; responses
//! are written one after another, so clients that read slowly each hold
//! the loop up to that long.

const std = @import("std");
const posix = std.posix;
const stdx = @import("stdx");
const request = @import("request.zig");

/// How long a client has to send its whole request.
pub const READ_TIMEOUT_MS: i64 = 5_000;
/// How long writing one response may take, all of it, before the connection
/// is dropped.
pub const WRITE_TIMEOUT_MS: i64 = 5_000;
/// Connections being read at once.
pub const MAX_CONNECTIONS: usize = 64;
/// The largest request line and headers; past it, 431.
pub const MAX_HEAD: usize = 8192;
/// The largest request, headers and body, a listener that takes bodies
/// should accept; past its limit, 413.
pub const MAX_REQUEST: usize = 1 << 20;

const Conn = struct {
    fd: posix.socket_t = -1,
    deadline_ms: i64 = 0,
    buf: std.ArrayListUnmanaged(u8) = .empty,
};

/// Serve `listener` until `running` is cleared, handing each request, read
/// whole and cut to its Content-Length, to `handler.handle(fd, bytes)`.
/// Requests are at most `max_request` bytes (`MAX_HEAD` for a listener
/// that takes no body). The handler writes with `writeAll`; the connection
/// is closed after it returns.
pub fn serve(allocator: std.mem.Allocator, listener: posix.socket_t, running: *const std.atomic.Value(bool), handler: anytype, max_request: usize) void {
    const conns = allocator.alloc(Conn, MAX_CONNECTIONS) catch {
        std.log.err("http: no memory for connection slots; listener not served", .{});
        return;
    };
    defer allocator.free(conns);
    for (conns) |*c| c.* = .{};
    defer for (conns) |*c| if (c.fd >= 0) close(allocator, c);
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
        _ = posix.poll(fds[0..n], 100) catch {
            // A poll that keeps failing would otherwise spin the thread.
            stdx.time.sleep(10 * std.time.ns_per_ms);
            continue;
        };
        if (!running.load(.acquire)) break;

        for (fds[1..n], slot_of[1..n]) |pf, i| {
            if (pf.revents == 0) continue;
            readInto(allocator, &conns[i], handler, max_request);
        }
        if (fds[0].revents != 0 and !acceptAll(allocator, listener, conns)) {
            // Out of descriptors (or another accept error): the connection
            // stays queued and the listener stays readable, so without a
            // pause every poll returns at once and the thread spins.
            stdx.time.sleep(10 * std.time.ns_per_ms);
        }

        const now = stdx.time.milliTimestamp();
        for (conns) |*c| {
            if (c.fd >= 0 and now > c.deadline_ms) close(allocator, c);
        }
    }
}

/// Accept everything queued; false on an error other than "none left".
fn acceptAll(allocator: std.mem.Allocator, listener: posix.socket_t, conns: []Conn) bool {
    while (true) {
        const rc = std.c.accept(listener, null, null);
        if (rc < 0) switch (posix.errno(rc)) {
            .AGAIN => return true,
            .INTR, .CONNABORTED => continue,
            else => return false,
        };
        const fd: posix.socket_t = @intCast(rc);
        stdx.net.sysFcntlSetNonblocking(fd) catch {
            _ = std.c.close(fd);
            continue;
        };
        const slot = freeSlot(conns) orelse oldest(conns);
        if (slot.fd >= 0) close(allocator, slot);
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

fn readInto(allocator: std.mem.Allocator, c: *Conn, handler: anytype, max_request: usize) void {
    var chunk: [8192]u8 = undefined;
    while (true) {
        const rc = std.c.read(c.fd, &chunk, chunk.len);
        if (rc < 0) switch (posix.errno(rc)) {
            .AGAIN => return,
            .INTR => continue,
            else => return close(allocator, c),
        };
        // The client stopped sending before its request was whole.
        if (rc == 0) return close(allocator, c);
        c.buf.appendSlice(allocator, chunk[0..@intCast(rc)]) catch return close(allocator, c);

        const data = c.buf.items;
        const head_end = std.mem.indexOf(u8, data, "\r\n\r\n") orelse {
            if (data.len > MAX_HEAD) return refuse(allocator, c, "431 Request Header Fields Too Large");
            continue;
        };
        if (head_end + 4 > MAX_HEAD) return refuse(allocator, c, "431 Request Header Fields Too Large");
        const expected = switch (request.framing(data).?) {
            .size => |n| n,
            // A body delimited any other way would be read as one of some
            // other length.
            .transfer_encoding => return refuse(allocator, c, "501 Not Implemented"),
            .bad_length => return refuse(allocator, c, "400 Bad Request"),
        };
        // Never handed on cut short: a truncated body would be stored or
        // applied as if whole.
        if (expected > max_request) return refuse(allocator, c, "413 Content Too Large");
        if (data.len >= expected) return dispatch(allocator, c, handler, data[0..expected]);
        c.buf.ensureTotalCapacity(allocator, expected) catch return close(allocator, c);
    }
}

/// Answer `status` without the handler, and close.
fn refuse(allocator: std.mem.Allocator, c: *Conn, status: []const u8) void {
    var buf: [128]u8 = undefined;
    const resp = std.fmt.bufPrint(&buf, "HTTP/1.1 {s}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", .{status}) catch unreachable;
    write_deadline_ms = stdx.time.milliTimestamp() + WRITE_TIMEOUT_MS;
    defer write_deadline_ms = 0;
    writeAll(c.fd, resp) catch {};
    close(allocator, c);
}

/// The deadline `writeAll` keeps while a handler runs: one for the whole
/// response, however many writes it takes.
threadlocal var write_deadline_ms: i64 = 0;

fn dispatch(allocator: std.mem.Allocator, c: *Conn, handler: anytype, bytes: []const u8) void {
    write_deadline_ms = stdx.time.milliTimestamp() + WRITE_TIMEOUT_MS;
    defer write_deadline_ms = 0;
    handler.handle(c.fd, bytes);
    close(allocator, c);
}

/// Write all of `bytes` to a non-blocking socket, waiting for room until
/// the response's deadline (or `WRITE_TIMEOUT_MS` outside a handler).
pub fn writeAll(fd: posix.socket_t, bytes: []const u8) error{WriteFailed}!void {
    const deadline = if (write_deadline_ms != 0) write_deadline_ms else stdx.time.milliTimestamp() + WRITE_TIMEOUT_MS;
    var off: usize = 0;
    while (off < bytes.len) {
        const rc = std.c.write(fd, bytes[off..].ptr, bytes.len - off);
        if (rc > 0) {
            off += @intCast(rc);
            continue;
        }
        if (rc == 0) return error.WriteFailed;
        switch (posix.errno(rc)) {
            .INTR => continue,
            .AGAIN => {
                const left = deadline - stdx.time.milliTimestamp();
                if (left <= 0) return error.WriteFailed;
                var pfd = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.OUT, .revents = 0 }};
                _ = posix.poll(&pfd, @intCast(@min(left, 1000))) catch {};
            },
            else => return error.WriteFailed,
        }
    }
}

/// A plain close: what was written is still sent, then FIN. The request was
/// read whole first, so no unread input turns the close into a reset.
fn close(allocator: std.mem.Allocator, c: *Conn) void {
    // Raw std.c because the std wrapper panics on an fd the peer already reset.
    _ = std.c.close(c.fd);
    c.buf.deinit(allocator);
    c.* = .{};
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

const Echo = struct {
    served: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    last: [64]u8 = undefined,
    last_len: usize = 0,
    /// Bytes of body to answer with.
    body_len: usize = 2,
    /// When the last answer was given up or finished.
    done_ms: std.atomic.Value(i64) = std.atomic.Value(i64).init(0),
    fn handle(self: *Echo, fd: posix.socket_t, bytes: []const u8) void {
        _ = self.served.fetchAdd(1, .monotonic);
        defer self.done_ms.store(stdx.time.milliTimestamp(), .release);
        const tail = bytes[bytes.len - @min(bytes.len, self.last.len) ..];
        @memcpy(self.last[0..tail.len], tail);
        self.last_len = tail.len;
        var hdr: [128]u8 = undefined;
        const h = std.fmt.bufPrint(&hdr, "HTTP/1.1 200 OK\r\nContent-Length: {d}\r\n\r\n", .{self.body_len}) catch return;
        writeAll(fd, h) catch return;
        var chunk: [4096]u8 = undefined;
        @memset(&chunk, 'o');
        var left = self.body_len;
        while (left > 0) {
            const n = @min(left, chunk.len);
            writeAll(fd, chunk[0..n]) catch return;
            left -= n;
        }
    }
};

const Listening = struct { fd: posix.socket_t, port: u16 };

fn listenLocal() !Listening {
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
    const t = try std.Thread.spawn(.{}, serve, .{ testing.allocator, l.fd, &running, &echo, MAX_REQUEST });
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
    const t = try std.Thread.spawn(.{}, serve, .{ testing.allocator, l.fd, &running, &echo, MAX_REQUEST });
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

const Served = struct {
    l: Listening,
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(true),
    echo: Echo = .{},
    max: usize = MAX_REQUEST,
    thread: std.Thread = undefined,

    fn start(self: *Served) !void {
        self.l = try listenLocal();
        self.thread = try std.Thread.spawn(.{}, serve, .{ testing.allocator, self.l.fd, &self.running, &self.echo, self.max });
    }
    fn stop(self: *Served) void {
        self.running.store(false, .release);
        self.thread.join();
        _ = std.c.close(self.l.fd);
    }
};

/// Send `req` and return the first bytes of the answer, waiting up to 3 s.
fn exchange(port: u16, req: []const u8, out: []u8) ![]const u8 {
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 1000);
    defer _ = std.c.close(fd);
    var off: usize = 0;
    while (off < req.len) {
        const rc = std.c.write(fd, req[off..].ptr, req.len - off);
        if (rc <= 0) break;
        off += @intCast(rc);
    }
    try stdx.net.sysFcntlSetNonblocking(fd);
    var got: usize = 0;
    var waited: u64 = 0;
    while (waited < 3000 and got < out.len) : (waited += 10) {
        const rc = std.c.read(fd, out[got..].ptr, out.len - got);
        if (rc > 0) got += @intCast(rc) else if (rc == 0) break else stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    return out[0..got];
}

test "serve: a request past the limits is answered 413 or 431 and never handled, and a body is cut to its length" {
    var srv = Served{ .l = undefined };
    try srv.start();
    defer srv.stop();
    var out: [256]u8 = undefined;

    const big = "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 2000000\r\n\r\n";
    try testing.expect(std.mem.startsWith(u8, try exchange(srv.l.port, big, &out), "HTTP/1.1 413"));

    const huge_head = "GET / HTTP/1.1\r\nX: " ++ "h" ** (MAX_HEAD + 10) ++ "\r\n\r\n";
    try testing.expect(std.mem.startsWith(u8, try exchange(srv.l.port, huge_head, &out), "HTTP/1.1 431"));

    // A head that never ends is not buffered past the limit either.
    const endless_head = "GET / HTTP/1.1\r\nX: " ++ "h" ** (MAX_HEAD + 10);
    try testing.expect(std.mem.startsWith(u8, try exchange(srv.l.port, endless_head, &out), "HTTP/1.1 431"));

    const bad_length = "POST / HTTP/1.1\r\nContent-Length: lots\r\n\r\n";
    try testing.expect(std.mem.startsWith(u8, try exchange(srv.l.port, bad_length, &out), "HTTP/1.1 400"));
    const disagreeing = "POST / HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 5\r\n\r\nhello";
    try testing.expect(std.mem.startsWith(u8, try exchange(srv.l.port, disagreeing, &out), "HTTP/1.1 400"));
    const chunked = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
    try testing.expect(std.mem.startsWith(u8, try exchange(srv.l.port, chunked, &out), "HTTP/1.1 501"));
    try testing.expectEqual(@as(u32, 0), srv.echo.served.load(.monotonic));

    // Any spelling of the header; what follows the body is not part of it.
    const cut = "POST / HTTP/1.1\r\nCONTENT-length: 3\r\n\r\nabcEXTRA";
    try testing.expect(std.mem.startsWith(u8, try exchange(srv.l.port, cut, &out), "HTTP/1.1 200"));
    try testing.expectEqual(@as(u32, 1), srv.echo.served.load(.monotonic));
    try testing.expect(std.mem.endsWith(u8, srv.echo.last[0..srv.echo.last_len], "\r\n\r\nabc"));
}

test "serve: a client that reads slowly is cut off at the response deadline, not held to it write by write" {
    var srv = Served{ .l = undefined };
    srv.echo.body_len = 16 << 20;
    try srv.start();
    defer srv.stop();

    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, srv.l.port, 1000);
    defer _ = std.c.close(fd);
    const req = "GET / HTTP/1.1\r\nHost: x\r\n\r\n";
    _ = std.c.write(fd, req, req.len);
    // Read a trickle, so every single write makes some progress, until the
    // server gives the answer up. At this pace the whole body would take
    // over a minute; what the kernel buffered may still arrive after.
    const start = stdx.time.milliTimestamp();
    var got: usize = 0;
    var buf: [4096]u8 = undefined;
    while (srv.echo.done_ms.load(.acquire) == 0 and stdx.time.milliTimestamp() - start < 15_000) {
        const rc = std.c.read(fd, &buf, buf.len);
        if (rc > 0) got += @intCast(rc);
        stdx.time.sleep(20 * std.time.ns_per_ms);
    }
    const done = srv.echo.done_ms.load(.acquire);
    try testing.expect(done != 0);
    try testing.expect(done - start < WRITE_TIMEOUT_MS + 1_500);
    try testing.expect(got > 0);
}

test "serve: a listener that takes no body is limited to its head" {
    var srv = Served{ .l = undefined, .max = MAX_HEAD };
    try srv.start();
    defer srv.stop();
    var out: [256]u8 = undefined;
    const body = "POST / HTTP/1.1\r\nContent-Length: 8192\r\n\r\n";
    try testing.expect(std.mem.startsWith(u8, try exchange(srv.l.port, body, &out), "HTTP/1.1 413"));
    try testing.expect(std.mem.startsWith(u8, try exchange(srv.l.port, "GET / HTTP/1.1\r\n\r\n", &out), "HTTP/1.1 200"));
}
