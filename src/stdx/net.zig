//! stdx.net — Sync TCP shim to replace `std.net` (removed in 0.16).
//!
//! Used by the CLI client and other boundary code that was previously
//! built on synchronous `std.net.Stream`. Hot-path code stays on raw
//! `std.posix.system.*` syscalls in the reactor.

const std = @import("std");
const posix = std.posix;
const net_io = std.Io.net;

pub const Address = struct {
    inner: net_io.IpAddress,

    pub const ParseError = error{InvalidIPAddressFormat};

    pub fn parseIp(text: []const u8, port: u16) ParseError!Address {
        const inner = net_io.IpAddress.parse(text, port) catch return error.InvalidIPAddressFormat;
        return .{ .inner = inner };
    }

    pub fn initIp4(bytes: [4]u8, port: u16) Address {
        return .{ .inner = .{ .ip4 = .{ .bytes = bytes, .port = port } } };
    }

    pub fn parseIp4(text: []const u8, port: u16) ParseError!Address {
        const inner = net_io.IpAddress.parseIp4(text, port) catch return error.InvalidIPAddressFormat;
        return .{ .inner = inner };
    }

    pub fn parseIp6(text: []const u8, port: u16) ParseError!Address {
        const inner = net_io.IpAddress.parseIp6(text, port) catch return error.InvalidIPAddressFormat;
        return .{ .inner = inner };
    }

    pub fn family(self: Address) u32 {
        return switch (self.inner) {
            .ip4 => @intCast(posix.AF.INET),
            .ip6 => @intCast(posix.AF.INET6),
        };
    }

    /// Fill a `sockaddr_storage` from this address. Returns the effective
    /// `socklen_t`.
    fn fillSockaddr(self: Address, ss: *posix.sockaddr.storage) posix.socklen_t {
        switch (self.inner) {
            .ip4 => |a| {
                const sin: *posix.sockaddr.in = @ptrCast(@alignCast(ss));
                sin.* = .{
                    .family = posix.AF.INET,
                    .port = std.mem.nativeToBig(u16, a.port),
                    .addr = @bitCast(a.bytes),
                    .zero = .{0} ** 8,
                };
                return @sizeOf(posix.sockaddr.in);
            },
            .ip6 => |a| {
                const sin6: *posix.sockaddr.in6 = @ptrCast(@alignCast(ss));
                sin6.* = .{
                    .family = posix.AF.INET6,
                    .port = std.mem.nativeToBig(u16, a.port),
                    .flowinfo = a.flow,
                    .addr = a.bytes,
                    .scope_id = 0,
                };
                return @sizeOf(posix.sockaddr.in6);
            },
        }
    }
};

pub const Stream = struct {
    handle: posix.fd_t,

    pub const ReadError = error{ WouldBlock, Interrupted, ConnectionResetByPeer, Unexpected };
    pub const WriteError = error{ WouldBlock, Interrupted, BrokenPipe, Unexpected };

    pub fn close(self: Stream) void {
        _ = std.c.close(self.handle);
    }

    pub fn read(self: Stream, buf: []u8) ReadError!usize {
        const n = std.c.read(self.handle, buf.ptr, buf.len);
        if (n < 0) {
            return switch (std.posix.errno(n)) {
                .AGAIN => error.WouldBlock,
                .INTR => error.Interrupted,
                .CONNRESET => error.ConnectionResetByPeer,
                else => error.Unexpected,
            };
        }
        return @intCast(n);
    }

    pub fn readAll(self: Stream, buf: []u8) ReadError!usize {
        var total: usize = 0;
        while (total < buf.len) {
            const n = try self.read(buf[total..]);
            if (n == 0) break;
            total += n;
        }
        return total;
    }

    pub fn readAtLeast(self: Stream, buf: []u8, min_bytes: usize) ReadError!usize {
        std.debug.assert(min_bytes <= buf.len);
        var total: usize = 0;
        while (total < min_bytes) {
            const n = try self.read(buf[total..]);
            if (n == 0) break;
            total += n;
        }
        return total;
    }

    pub fn write(self: Stream, bytes: []const u8) WriteError!usize {
        const n = std.c.write(self.handle, bytes.ptr, bytes.len);
        if (n < 0) {
            return switch (std.posix.errno(n)) {
                .AGAIN => error.WouldBlock,
                .INTR => error.Interrupted,
                .PIPE => error.BrokenPipe,
                else => error.Unexpected,
            };
        }
        return @intCast(n);
    }

    pub fn writeAll(self: Stream, bytes: []const u8) WriteError!void {
        var written: usize = 0;
        while (written < bytes.len) {
            const n = try self.write(bytes[written..]);
            if (n == 0) return error.BrokenPipe;
            written += n;
        }
    }
};

pub const ConnectError = error{
    ConnectionRefused,
    ConnectionTimedOut,
    NetworkUnreachable,
    AddressInUse,
    PermissionDenied,
    Unexpected,
};

pub fn tcpConnectToAddress(address: Address) ConnectError!Stream {
    const fd = std.c.socket(@intCast(address.family()), posix.SOCK.STREAM, 0);
    if (fd < 0) return error.Unexpected;
    errdefer _ = std.c.close(fd);

    var ss: posix.sockaddr.storage = std.mem.zeroes(posix.sockaddr.storage);
    const len = address.fillSockaddr(&ss);
    const rc = std.c.connect(fd, @ptrCast(&ss), len);
    if (rc != 0) {
        return switch (std.posix.errno(rc)) {
            .CONNREFUSED => error.ConnectionRefused,
            .TIMEDOUT => error.ConnectionTimedOut,
            .NETUNREACH, .HOSTUNREACH => error.NetworkUnreachable,
            .ADDRINUSE => error.AddressInUse,
            .ACCES, .PERM => error.PermissionDenied,
            else => error.Unexpected,
        };
    }

    return .{ .handle = fd };
}

/// Resolve `host` (DNS or IP literal) and connect to the first usable address.
pub fn tcpConnectToHost(allocator: std.mem.Allocator, host: []const u8, port: u16) !Stream {
    var list = try getAddressList(allocator, host, port);
    defer list.deinit();
    if (list.addrs.len == 0) return error.UnknownHostName;
    return tcpConnectToAddress(list.addrs[0]);
}

pub const AddressList = struct {
    arena: std.heap.ArenaAllocator,
    addrs: []Address,

    pub fn deinit(self: *AddressList) void {
        self.arena.deinit();
    }
};

/// Resolve a host name (or IP literal) to one or more `Address`es using libc.
pub fn getAddressList(gpa: std.mem.Allocator, host: []const u8, port: u16) !AddressList {
    var arena = std.heap.ArenaAllocator.init(gpa);
    errdefer arena.deinit();
    const aalloc = arena.allocator();

    if (Address.parseIp(host, port)) |addr| {
        const list = try aalloc.alloc(Address, 1);
        list[0] = addr;
        return .{ .arena = arena, .addrs = list };
    } else |_| {}

    var hints: std.c.addrinfo = std.mem.zeroes(std.c.addrinfo);
    hints.family = posix.AF.UNSPEC;
    hints.socktype = posix.SOCK.STREAM;

    const host_z = try aalloc.dupeZ(u8, host);
    var port_buf: [8]u8 = undefined;
    const port_str = try std.fmt.bufPrintZ(&port_buf, "{d}", .{port});

    var res: ?*std.c.addrinfo = null;
    const rc = std.c.getaddrinfo(host_z.ptr, port_str.ptr, &hints, &res);
    if (@intFromEnum(rc) != 0) return error.UnknownHostName;
    defer std.c.freeaddrinfo(res.?);

    var list: std.ArrayList(Address) = .empty;
    var p: ?*std.c.addrinfo = res;
    while (p) |info| : (p = info.next) {
        if (info.addr) |sa| {
            switch (info.family) {
                posix.AF.INET => {
                    const sin: *const posix.sockaddr.in = @ptrCast(@alignCast(sa));
                    const ip4_bytes: [4]u8 = @bitCast(sin.addr);
                    const inner = net_io.IpAddress{ .ip4 = .{
                        .bytes = ip4_bytes,
                        .port = std.mem.bigToNative(u16, sin.port),
                    } };
                    try list.append(aalloc, .{ .inner = inner });
                },
                posix.AF.INET6 => {
                    const sin6: *const posix.sockaddr.in6 = @ptrCast(@alignCast(sa));
                    const inner = net_io.IpAddress{ .ip6 = .{
                        .port = std.mem.bigToNative(u16, sin6.port),
                        .bytes = sin6.addr,
                        .flow = sin6.flowinfo,
                    } };
                    try list.append(aalloc, .{ .inner = inner });
                },
                else => {},
            }
        }
    }

    return .{ .arena = arena, .addrs = try list.toOwnedSlice(aalloc) };
}

// --- Server-side socket helpers (used by acceptor + raft network) ---

pub const SocketAddrV4 = struct {
    sa: posix.sockaddr.in,

    pub fn initIp4(ip4: [4]u8, port: u16) SocketAddrV4 {
        return .{ .sa = .{
            .family = posix.AF.INET,
            .port = std.mem.nativeToBig(u16, port),
            .addr = @bitCast(ip4),
            .zero = .{0} ** 8,
        } };
    }

    pub fn anyPtr(self: *const SocketAddrV4) *const posix.sockaddr {
        return @ptrCast(&self.sa);
    }

    pub fn anyLen(_: SocketAddrV4) posix.socklen_t {
        return @sizeOf(posix.sockaddr.in);
    }
};

pub const SocketError = error{ SocketCreateFailed, BindFailed, ListenFailed, AcceptFailed, ConnectFailed, WriteFailed, ReadFailed, FcntlFailed, Unexpected };

pub fn sysSocket(family: u32, sock_type: u32, protocol: u32) SocketError!posix.socket_t {
    // macOS rejects SOCK.NONBLOCK / SOCK.CLOEXEC in the socket() type field
    // (returns EPROTONOSUPPORT). Strip them and apply via fcntl after creation.
    const builtin = @import("builtin");
    const strip_mask: u32 = if (builtin.os.tag == .macos)
        posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC
    else
        0;
    const want_nonblock = (sock_type & posix.SOCK.NONBLOCK) != 0;
    const want_cloexec = (sock_type & posix.SOCK.CLOEXEC) != 0;
    const real_type = sock_type & ~strip_mask;

    const rc = std.c.socket(@intCast(family), @intCast(real_type), @intCast(protocol));
    if (rc < 0) return error.SocketCreateFailed;
    const fd: posix.socket_t = @intCast(rc);

    if (builtin.os.tag == .macos) {
        if (want_nonblock) {
            const F_GETFL: c_int = 3;
            const F_SETFL: c_int = 4;
            const O_NONBLOCK: c_int = 4;
            const flags = std.c.fcntl(fd, F_GETFL, @as(c_int, 0));
            if (flags >= 0) _ = std.c.fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        }
        if (want_cloexec) {
            const F_SETFD: c_int = 2;
            const FD_CLOEXEC: c_int = 1;
            _ = std.c.fcntl(fd, F_SETFD, FD_CLOEXEC);
        }
    }
    return fd;
}

pub fn sysClose(fd: posix.socket_t) void {
    _ = std.c.close(fd);
}

pub fn sysBind(fd: posix.socket_t, addr: *const posix.sockaddr, len: posix.socklen_t) SocketError!void {
    if (std.c.bind(fd, addr, len) != 0) return error.BindFailed;
}

pub fn sysListen(fd: posix.socket_t, backlog: u31) SocketError!void {
    if (std.c.listen(fd, backlog) != 0) return error.ListenFailed;
}

pub fn sysConnect(fd: posix.socket_t, addr: *const posix.sockaddr, len: posix.socklen_t) SocketError!void {
    if (std.c.connect(fd, addr, len) != 0) return error.ConnectFailed;
}

pub fn sysAccept(
    fd: posix.socket_t,
    addr: ?*posix.sockaddr,
    len: ?*posix.socklen_t,
    _: u32,
) SocketError!posix.socket_t {
    const rc = std.c.accept(fd, addr, len);
    if (rc < 0) return error.AcceptFailed;
    return @intCast(rc);
}

pub fn sysWrite(fd: posix.socket_t, buf: []const u8) SocketError!usize {
    const rc = std.c.write(fd, buf.ptr, buf.len);
    if (rc < 0) return error.WriteFailed;
    return @intCast(rc);
}

pub fn sysRead(fd: posix.socket_t, buf: []u8) SocketError!usize {
    const rc = std.c.read(fd, buf.ptr, buf.len);
    if (rc < 0) return error.ReadFailed;
    return @intCast(rc);
}

/// Put a socket into non-blocking mode.
///
/// O_NONBLOCK is not portable — 0x0004 on macOS/BSD, 0o4000 on Linux — and a
/// wrong value here fails silently: fcntl returns success while the socket
/// stays blocking. `std.posix.O` is target-specific, so bit-casting it is the
/// only safe way to name the flag. F_GETFL/F_SETFL are 3/4 on both.
pub fn sysFcntlSetNonblocking(fd: posix.socket_t) SocketError!void {
    const F_GETFL: c_int = 3;
    const F_SETFL: c_int = 4;
    const nonblock: c_int = @bitCast(std.posix.O{ .NONBLOCK = true });
    const flags = std.c.fcntl(fd, F_GETFL, @as(c_int, 0));
    if (flags < 0) return error.FcntlFailed;
    if (std.c.fcntl(fd, F_SETFL, flags | nonblock) < 0) return error.FcntlFailed;
}

// ── Addresses for listeners and peers ────────────────────────────────

/// A dotted quad or "localhost".
pub fn parseIp4Bind(text: []const u8) error{InvalidAddress}![4]u8 {
    if (std.mem.eql(u8, text, "localhost")) return .{ 127, 0, 0, 1 };
    var out: [4]u8 = undefined;
    var it = std.mem.splitScalar(u8, text, '.');
    var i: usize = 0;
    while (it.next()) |part| : (i += 1) {
        if (i >= 4) return error.InvalidAddress;
        out[i] = std.fmt.parseInt(u8, part, 10) catch return error.InvalidAddress;
    }
    if (i != 4) return error.InvalidAddress;
    return out;
}

/// Resolve a host — an IPv4 literal, "localhost", or a DNS name — to an
/// IPv4 address. Peer links are IPv4 only, so an IPv6-only name fails.
pub fn resolveIp4(allocator: std.mem.Allocator, host: []const u8) error{ UnknownHost, NoIp4Address, OutOfMemory }![4]u8 {
    if (parseIp4Bind(host)) |ip| return ip else |_| {}
    var list = getAddressList(allocator, host, 0) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.UnknownHost,
    };
    defer list.deinit();
    for (list.addrs) |a| switch (a.inner) {
        .ip4 => |v| return v.bytes,
        .ip6 => {},
    };
    return error.NoIp4Address;
}

pub const any_ip4: [4]u8 = .{ 0, 0, 0, 0 };

pub fn isLoopback(ip4: [4]u8) bool {
    return ip4[0] == 127;
}

/// An address a peer can be dialed at: not unspecified, multicast,
/// broadcast, or link-local.
pub fn isUnicastPeerAddress(ip4: [4]u8) bool {
    if (std.mem.eql(u8, &ip4, &any_ip4)) return false;
    if (ip4[0] >= 224) return false; // multicast and above, incl. 255.255.255.255
    if (ip4[0] == 169 and ip4[1] == 254) return false;
    return true;
}

/// Connect with a deadline. The socket is non-blocking on return.
/// A blocking connect() waits out the kernel's SYN timeout — minutes —
/// and the peer loop runs on one thread.
pub fn tcpConnectIp4Timeout(ip4: [4]u8, port: u16, timeout_ms: i32) SocketError!posix.socket_t {
    const fd = try sysSocket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
    errdefer sysClose(fd);
    const addr = SocketAddrV4.initIp4(ip4, port);
    const rc = std.c.connect(fd, addr.anyPtr(), addr.anyLen());
    if (rc != 0) {
        switch (posix.errno(rc)) {
            .INPROGRESS, .AGAIN => {},
            else => return error.ConnectFailed,
        }
        var fds = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.OUT, .revents = 0 }};
        const ready = posix.poll(&fds, timeout_ms) catch return error.ConnectFailed;
        if (ready == 0) return error.ConnectFailed;
        var err: c_int = 0;
        var len: posix.socklen_t = @sizeOf(c_int);
        if (std.c.getsockopt(fd, posix.SOL.SOCKET, posix.SO.ERROR, @ptrCast(&err), &len) != 0) return error.ConnectFailed;
        if (err != 0) return error.ConnectFailed;
    }
    return fd;
}

/// The IPv4 address carried by a sockaddr filled in by accept/getsockname,
/// or null if it is not AF_INET.
pub fn ip4FromSockaddr(sa: *const posix.sockaddr) ?[4]u8 {
    if (sa.family != posix.AF.INET) return null;
    const sin: *const posix.sockaddr.in = @ptrCast(@alignCast(sa));
    return @bitCast(sin.addr);
}

/// The IPv4 address and port a socket is bound to.
pub fn sysLocalIp4(fd: posix.socket_t) SocketError!struct { ip4: [4]u8, port: u16 } {
    var ss: posix.sockaddr.storage = std.mem.zeroes(posix.sockaddr.storage);
    var len: posix.socklen_t = @sizeOf(posix.sockaddr.storage);
    if (std.c.getsockname(fd, @ptrCast(&ss), &len) != 0) return error.Unexpected;
    const sin: *const posix.sockaddr.in = @ptrCast(@alignCast(&ss));
    return .{ .ip4 = @bitCast(sin.addr), .port = std.mem.bigToNative(u16, sin.port) };
}

test "net: parseIp4Bind accepts dotted quads and localhost, rejects the rest" {
    try std.testing.expectEqual([4]u8{ 0, 0, 0, 0 }, try parseIp4Bind("0.0.0.0"));
    try std.testing.expectEqual([4]u8{ 10, 0, 1, 12 }, try parseIp4Bind("10.0.1.12"));
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, try parseIp4Bind("localhost"));
    try std.testing.expectError(error.InvalidAddress, parseIp4Bind("10.0.1"));
    try std.testing.expectError(error.InvalidAddress, parseIp4Bind("flo-2"));
    try std.testing.expectError(error.InvalidAddress, parseIp4Bind("::1"));
    try std.testing.expectError(error.InvalidAddress, parseIp4Bind(""));
}

test "net: resolveIp4 takes literals without DNS and resolves localhost" {
    try std.testing.expectEqual([4]u8{ 192, 168, 1, 5 }, try resolveIp4(std.testing.allocator, "192.168.1.5"));
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, try resolveIp4(std.testing.allocator, "localhost"));
    try std.testing.expectError(error.UnknownHost, resolveIp4(std.testing.allocator, "no-such-host.invalid"));
}

test "net: loopback and unicast classification" {
    try std.testing.expect(isLoopback(.{ 127, 0, 0, 1 }));
    try std.testing.expect(isLoopback(.{ 127, 5, 6, 7 }));
    try std.testing.expect(!isLoopback(.{ 10, 0, 0, 1 }));
    try std.testing.expect(isUnicastPeerAddress(.{ 10, 0, 1, 12 }));
    try std.testing.expect(isUnicastPeerAddress(.{ 127, 0, 0, 1 }));
    try std.testing.expect(!isUnicastPeerAddress(any_ip4));
    try std.testing.expect(!isUnicastPeerAddress(.{ 224, 0, 0, 1 }));
    try std.testing.expect(!isUnicastPeerAddress(.{ 255, 255, 255, 255 }));
    try std.testing.expect(!isUnicastPeerAddress(.{ 169, 254, 1, 1 }));
}

test "net: a connect to a black hole gives up at the deadline" {
    // 192.0.2.0/24 is reserved for documentation and never routed; a network
    // that rejects it outright makes the connect fail sooner, which is also fine.
    const started = @import("time.zig").milliTimestamp();
    try std.testing.expectError(error.ConnectFailed, tcpConnectIp4Timeout(.{ 192, 0, 2, 1 }, 9, 300));
    try std.testing.expect(@import("time.zig").milliTimestamp() - started < 3000);
}

test "net: sysLocalIp4 reports the bound address" {
    const fd = try sysSocket(posix.AF.INET, posix.SOCK.STREAM, 0);
    defer sysClose(fd);
    const addr = SocketAddrV4.initIp4(.{ 127, 0, 0, 1 }, 0);
    try sysBind(fd, addr.anyPtr(), addr.anyLen());
    const local = try sysLocalIp4(fd);
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, local.ip4);
    try std.testing.expect(local.port != 0);
}
