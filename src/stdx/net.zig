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

pub const SocketError = error{ SocketCreateFailed, BindFailed, ListenFailed, AcceptFailed, ConnectFailed, WriteFailed, ReadFailed, FcntlFailed };

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
/// O_NONBLOCK is NOT portable: 0x0004 on macOS/BSD, 0o4000 on Linux. This
/// helper hardcoded the macOS value, so on Linux it set an unrelated flag,
/// fcntl still returned success, and the socket stayed BLOCKING — silently.
/// Every accepted client connection and every Raft peer socket was affected;
/// the Raft network thread then blocked forever in read() on its first peer
/// and stopped accepting anyone else (issue #54).
///
/// `std.posix.O` is target-specific, so bit-casting it gets the right value on
/// every platform. F_GETFL/F_SETFL are 3/4 on both.
pub fn sysFcntlSetNonblocking(fd: posix.socket_t) SocketError!void {
    const F_GETFL: c_int = 3;
    const F_SETFL: c_int = 4;
    const nonblock: c_int = @bitCast(std.posix.O{ .NONBLOCK = true });
    const flags = std.c.fcntl(fd, F_GETFL, @as(c_int, 0));
    if (flags < 0) return error.FcntlFailed;
    if (std.c.fcntl(fd, F_SETFL, flags | nonblock) < 0) return error.FcntlFailed;
}
