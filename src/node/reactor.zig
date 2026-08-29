// Reactor — Unified event loop for a Shard.
//
// Each shard has exactly ONE Reactor that drives ALL async I/O:
//   - Network I/O (client TCP connections)
//   - Acceptor hand-off pipe (new connections)
//   - Raft peer sockets
//   - Inbox notification (cross-shard messages)
//   - Timer (Raft tick, TTL sweep, etc.)
//
// On macOS/FreeBSD: kqueue
// On Linux: io_uring (multi-shot poll, native async I/O)
//
// See: NODE_NETWORK_DESIGN.md §5 — The Reactor

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const linux = if (is_linux) std.os.linux else void;

const is_linux = builtin.os.tag == .linux;
const is_bsd = builtin.os.tag == .macos or builtin.os.tag == .freebsd or
    builtin.os.tag == .openbsd or builtin.os.tag == .netbsd;

/// Sentinel user_data for poll timeout SQEs — filtered out from results.
const TIMEOUT_SENTINEL: u64 = std.math.maxInt(u64);

/// High bit flag for internal operations (poll_update, poll_remove results).
/// CQEs with this flag set in user_data are filtered out — not real poll events.
const INTERNAL_FLAG: u64 = 1 << 63;

// ───── Compatibility wrappers (Zig 0.16: posix.k* functions removed) ─────

const KEventError = error{ Interrupted, Unexpected, SystemResources };

inline fn kqueueCreate() KEventError!i32 {
    const fd = std.c.kqueue();
    if (fd < 0) return error.SystemResources;
    return fd;
}

inline fn keventCall(
    kq: i32,
    changes: []const posix.Kevent,
    events: []posix.Kevent,
    timeout: ?*const posix.timespec,
) KEventError!usize {
    const rc = std.c.kevent(
        kq,
        changes.ptr,
        @intCast(changes.len),
        events.ptr,
        @intCast(events.len),
        timeout,
    );
    if (rc < 0) {
        return switch (std.posix.errno(@as(c_int, -1))) {
            .INTR => error.Interrupted,
            else => error.Unexpected,
        };
    }
    return @intCast(rc);
}

/// Tags for event source identification.
/// Each registered fd carries a tag so the shard loop can dispatch correctly.
pub const Tag = enum(u8) {
    /// Client TCP connection — readable (request data available)
    client_read,
    /// Client TCP connection — writable (send buffer has space)
    client_write,
    /// Client TCP connection — hangup / error
    client_hangup,
    /// Acceptor pipe — new connection to register
    acceptor_pipe,
    /// Raft peer socket — inbound Raft RPC
    raft_read,
    /// Raft peer socket — outbound Raft RPC
    raft_write,
    /// UAL write completion (io_uring async; on kqueue: used as notification)
    ual_completion,
    /// Inbox eventfd / pipe — cross-shard messages pending
    inbox_ready,
    /// Periodic timer for background tasks
    timer,
};

/// Interest flags for event registration.
pub const Interests = packed struct(u8) {
    readable: bool = false,
    writable: bool = false,
    _padding: u6 = 0,
};

/// An event source to register with the Reactor.
pub const EventSource = struct {
    fd: i32,
    tag: Tag,
    interests: Interests,
    /// Opaque user data (e.g., pointer to Connection)
    user_data: usize = 0,
};

/// A fired event returned by poll().
pub const Event = struct {
    fd: i32,
    tag: Tag,
    user_data: usize,
    readable: bool,
    writable: bool,
    err: bool,
    hangup: bool,
};

/// Maximum events returned per poll call.
const MAX_EVENTS = 256;

pub const Reactor = struct {
    /// Backend fd: kqueue on BSD (unused on Linux — io_uring handles it).
    poll_fd: if (is_bsd) i32 else void,
    allocator: Allocator,

    // Map fd → EventSource metadata for dispatch
    sources: std.AutoHashMap(i32, EventSource),

    // io_uring ring instance (Linux only)
    ring: if (is_linux) linux.IoUring else void,

    // Timer bookkeeping — timerfd per timer on Linux,
    // kqueue handles timers natively on BSD.
    timer_fds: if (is_linux) std.AutoHashMap(usize, i32) else void,

    // Reusable event buffers (platform-specific)
    kqueue_buf: if (is_bsd) [MAX_EVENTS]posix.Kevent else void,
    cqe_buf: if (is_linux) [MAX_EVENTS]linux.io_uring_cqe else void,

    // Internal result cache to hold converted events between poll calls
    result_cache: [MAX_EVENTS]Event = undefined,

    const Self = @This();

    pub fn init(allocator: Allocator) !Self {
        if (comptime is_bsd) {
            const kq = try kqueueCreate();
            return .{
                .poll_fd = kq,
                .allocator = allocator,
                .sources = std.AutoHashMap(i32, EventSource).init(allocator),
                .ring = {},
                .timer_fds = {},
                .kqueue_buf = undefined,
                .cqe_buf = {},
            };
        } else if (comptime is_linux) {
            const ring = try linux.IoUring.init(256, 0);
            return .{
                .poll_fd = {},
                .allocator = allocator,
                .sources = std.AutoHashMap(i32, EventSource).init(allocator),
                .ring = ring,
                .timer_fds = std.AutoHashMap(usize, i32).init(allocator),
                .kqueue_buf = {},
                .cqe_buf = undefined,
            };
        } else {
            @compileError("Unsupported OS — Reactor requires macOS/FreeBSD (kqueue) or Linux (io_uring)");
        }
    }

    pub fn deinit(self: *Self) void {
        if (comptime is_linux) {
            // Close all timerfd file descriptors
            var it = self.timer_fds.valueIterator();
            while (it.next()) |tfd_ptr| {
                _ = std.c.close(tfd_ptr.*);
            }
            self.timer_fds.deinit();
            self.ring.deinit();
        }
        self.sources.deinit();
        if (comptime is_bsd) {
            _ = std.c.close(self.poll_fd);
        }
    }

    /// Register an event source (fd + interests).
    pub fn addSource(self: *Self, source: EventSource) !void {
        if (comptime is_bsd) {
            try self.kqueueAddSource(source);
        } else if (comptime is_linux) {
            try self.iouringAddSource(source);
        }
        try self.sources.put(source.fd, source);
    }

    /// Remove an fd from the reactor.
    pub fn removeSource(self: *Self, fd: i32) void {
        if (comptime is_bsd) {
            self.kqueueRemoveSource(fd);
        } else if (comptime is_linux) {
            self.iouringRemoveSource(fd);
        }
        _ = self.sources.remove(fd);
    }

    /// Modify event interests for an existing fd.
    pub fn modifyInterests(self: *Self, fd: i32, interests: Interests) !void {
        if (self.sources.getPtr(fd)) |source| {
            if (comptime is_bsd) {
                try self.kqueueModifyInterests(fd, interests, source);
            } else if (comptime is_linux) {
                try self.iouringModifyInterests(fd, interests, source);
            }
            source.interests = interests;
        }
    }

    /// Arm writable interest for a connection (commonly used after queueing response bytes).
    pub fn armWritable(self: *Self, fd: i32) !void {
        if (self.sources.getPtr(fd)) |source| {
            if (!source.interests.writable) {
                if (comptime is_bsd) {
                    var changelist: [1]posix.Kevent = undefined;
                    changelist[0] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.ADD | posix.system.EV.ENABLE, source.user_data);
                    _ = try keventCall(self.poll_fd, &changelist, &.{}, null);
                } else if (comptime is_linux) {
                    const mask = linux.POLL.IN | linux.POLL.OUT;
                    const flags = linux.IORING_POLL_UPDATE_EVENTS | linux.IORING_POLL_ADD_MULTI;
                    const fd_u64: u64 = @intCast(@as(u32, @bitCast(fd)));
                    _ = try self.ring.poll_update(fd_u64 | INTERNAL_FLAG, fd_u64, fd_u64, mask, flags);
                }
                source.interests.writable = true;
            }
        }
    }

    /// Disarm writable interest (buffer drained, no more data to send).
    pub fn disarmWritable(self: *Self, fd: i32) !void {
        if (self.sources.getPtr(fd)) |source| {
            if (source.interests.writable) {
                if (comptime is_bsd) {
                    var changelist: [1]posix.Kevent = undefined;
                    changelist[0] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.DELETE, 0);
                    _ = try keventCall(self.poll_fd, &changelist, &.{}, null);
                } else if (comptime is_linux) {
                    const mask = if (source.interests.readable) linux.POLL.IN else @as(u32, 0);
                    if (mask == 0) {
                        // No interests left — remove the poll entirely
                        const fd_u64: u64 = @intCast(@as(u32, @bitCast(fd)));
                        _ = try self.ring.poll_remove(fd_u64 | INTERNAL_FLAG, fd_u64);
                    } else {
                        const fd_u64: u64 = @intCast(@as(u32, @bitCast(fd)));
                        const flags = linux.IORING_POLL_UPDATE_EVENTS | linux.IORING_POLL_ADD_MULTI;
                        _ = try self.ring.poll_update(fd_u64 | INTERNAL_FLAG, fd_u64, fd_u64, mask, flags);
                    }
                }
                source.interests.writable = false;
            }
        }
    }

    /// Add a one-shot or repeating timer.
    /// `ident` is a unique timer ID, `interval_ms` is the period.
    pub fn addTimer(self: *Self, ident: usize, interval_ms: u32) !void {
        if (comptime is_bsd) {
            var changelist: [1]posix.Kevent = undefined;
            changelist[0] = .{
                .ident = ident,
                .filter = posix.system.EVFILT.TIMER,
                .flags = posix.system.EV.ADD | posix.system.EV.ENABLE,
                .fflags = 0,
                .data = @intCast(interval_ms),
                .udata = ident,
            };
            _ = try keventCall(self.poll_fd, &changelist, &.{}, null);
        } else if (comptime is_linux) {
            // std.posix.timerfd_create/settime were removed in Zig 0.16; call the
            // raw linux syscalls and map errno the way stdx.net does. This branch
            // is Linux-only, so macOS builds never analysed it (issue #50).
            const tfd_rc = linux.timerfd_create(.MONOTONIC, .{ .CLOEXEC = true });
            if (posix.errno(@as(isize, @bitCast(tfd_rc))) != .SUCCESS) return error.TimerCreateFailed;
            const tfd: i32 = @intCast(tfd_rc);
            errdefer _ = std.c.close(tfd);

            // Convert interval_ms to itimerspec (repeating timer)
            const secs: i64 = @intCast(interval_ms / 1000);
            const nsecs: i64 = @intCast((@as(u64, interval_ms) % 1000) * 1_000_000);
            const spec = linux.itimerspec{
                .it_interval = .{ .sec = secs, .nsec = nsecs },
                .it_value = .{ .sec = secs, .nsec = nsecs },
            };
            const settime_rc = linux.timerfd_settime(tfd, .{}, &spec, null);
            if (posix.errno(@as(isize, @bitCast(settime_rc))) != .SUCCESS) return error.TimerSetFailed;

            // Register timerfd as a source with .timer tag so poll identifies it
            try self.sources.put(tfd, .{
                .fd = tfd,
                .tag = .timer,
                .interests = .{ .readable = true },
                .user_data = ident,
            });

            // Register timerfd with io_uring multi-shot poll
            const tfd_u64: u64 = @intCast(@as(u32, @bitCast(tfd)));
            const sqe = try self.ring.poll_add(tfd_u64, tfd, linux.POLL.IN);
            sqe.len = linux.IORING_POLL_ADD_MULTI;

            // Track timerfd so we can close it in deinit
            try self.timer_fds.put(ident, tfd);
        }
    }

    /// Poll for events. Returns slice of fired events.
    /// `timeout_ms`: 0 = non-blocking, null = block indefinitely
    pub fn poll(self: *Self, timeout_ms: ?u32) ![]const Event {
        if (comptime is_bsd) {
            return self.kqueuePoll(timeout_ms);
        } else if (comptime is_linux) {
            return self.iouringPoll(timeout_ms);
        }
    }

    // ───────────────────────────────────────────────────────────────
    //  kqueue backend (macOS / FreeBSD)
    // ───────────────────────────────────────────────────────────────

    fn kqueueAddSource(self: *Self, source: EventSource) !void {
        var changelist: [2]posix.Kevent = undefined;
        var nchanges: usize = 0;

        if (source.interests.readable) {
            changelist[nchanges] = makeKevent(
                source.fd,
                posix.system.EVFILT.READ,
                posix.system.EV.ADD | posix.system.EV.ENABLE,
                source.user_data,
            );
            nchanges += 1;
        }

        if (source.interests.writable) {
            changelist[nchanges] = makeKevent(
                source.fd,
                posix.system.EVFILT.WRITE,
                posix.system.EV.ADD | posix.system.EV.ENABLE,
                source.user_data,
            );
            nchanges += 1;
        }

        if (nchanges > 0) {
            _ = try keventCall(self.poll_fd, changelist[0..nchanges], &.{}, null);
        }
    }

    fn kqueueRemoveSource(self: *Self, fd: i32) void {
        var changelist: [2]posix.Kevent = undefined;
        changelist[0] = makeKevent(fd, posix.system.EVFILT.READ, posix.system.EV.DELETE, 0);
        changelist[1] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.DELETE, 0);
        _ = keventCall(self.poll_fd, &changelist, &.{}, null) catch {};
    }

    fn kqueueModifyInterests(self: *Self, fd: i32, interests: Interests, source: *EventSource) !void {
        var changelist: [2]posix.Kevent = undefined;
        var nchanges: usize = 0;

        if (interests.readable and !source.interests.readable) {
            changelist[nchanges] = makeKevent(fd, posix.system.EVFILT.READ, posix.system.EV.ADD | posix.system.EV.ENABLE, source.user_data);
            nchanges += 1;
        } else if (!interests.readable and source.interests.readable) {
            changelist[nchanges] = makeKevent(fd, posix.system.EVFILT.READ, posix.system.EV.DELETE, 0);
            nchanges += 1;
        }

        if (interests.writable and !source.interests.writable) {
            changelist[nchanges] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.ADD | posix.system.EV.ENABLE, source.user_data);
            nchanges += 1;
        } else if (!interests.writable and source.interests.writable) {
            changelist[nchanges] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.DELETE, 0);
            nchanges += 1;
        }

        if (nchanges > 0) {
            _ = try keventCall(self.poll_fd, changelist[0..nchanges], &.{}, null);
        }
    }

    fn kqueuePoll(self: *Self, timeout_ms: ?u32) ![]const Event {
        const timeout: ?posix.timespec = if (timeout_ms) |ms| .{
            .sec = @intCast(ms / 1000),
            .nsec = @intCast((@as(u64, ms) % 1000) * 1_000_000),
        } else null;

        const nready = keventCall(
            self.poll_fd,
            &.{}, // no changes
            &self.kqueue_buf,
            if (timeout) |*t| t else null,
        ) catch |err| {
            if (err == error.Interrupted) return &.{};
            return err;
        };

        var count: usize = 0;
        for (self.kqueue_buf[0..nready]) |kev| {
            const fd: i32 = @intCast(kev.ident);

            // Timer events
            if (kev.filter == posix.system.EVFILT.TIMER) {
                self.result_cache[count] = .{
                    .fd = fd,
                    .tag = .timer,
                    .user_data = kev.udata,
                    .readable = false,
                    .writable = false,
                    .err = false,
                    .hangup = false,
                };
                count += 1;
                continue;
            }

            const source = self.sources.get(fd);
            const tag = if (source) |s| s.tag else Tag.client_read;
            const udata = if (source) |s| s.user_data else 0;

            const is_err = (kev.flags & posix.system.EV.ERROR) != 0;
            const is_eof = (kev.flags & posix.system.EV.EOF) != 0;

            self.result_cache[count] = .{
                .fd = fd,
                .tag = tag,
                .user_data = udata,
                .readable = kev.filter == posix.system.EVFILT.READ,
                .writable = kev.filter == posix.system.EVFILT.WRITE,
                .err = is_err,
                .hangup = is_eof and kev.filter == posix.system.EVFILT.READ,
            };
            count += 1;
        }

        return self.result_cache[0..count];
    }

    // ───────────────────────────────────────────────────────────────
    //  io_uring backend (Linux)
    //  Multi-shot poll for network/timer fds, native async I/O.
    // ───────────────────────────────────────────────────────────────

    fn iouringAddSource(self: *Self, source: EventSource) !void {
        var mask: u32 = 0;
        if (source.interests.readable) mask |= linux.POLL.IN;
        if (source.interests.writable) mask |= linux.POLL.OUT;

        if (mask != 0) {
            const fd_u64: u64 = @intCast(@as(u32, @bitCast(source.fd)));
            const sqe = try self.ring.poll_add(fd_u64, source.fd, mask);
            sqe.len = linux.IORING_POLL_ADD_MULTI;
        }
    }

    fn iouringRemoveSource(self: *Self, fd: i32) void {
        const fd_u64: u64 = @intCast(@as(u32, @bitCast(fd)));
        _ = self.ring.poll_remove(fd_u64 | INTERNAL_FLAG, fd_u64) catch {};
    }

    fn iouringModifyInterests(self: *Self, fd: i32, interests: Interests, _: *EventSource) !void {
        var mask: u32 = 0;
        if (interests.readable) mask |= linux.POLL.IN;
        if (interests.writable) mask |= linux.POLL.OUT;

        const fd_u64: u64 = @intCast(@as(u32, @bitCast(fd)));

        if (mask == 0) {
            _ = self.ring.poll_remove(fd_u64 | INTERNAL_FLAG, fd_u64) catch {};
        } else {
            const flags = linux.IORING_POLL_UPDATE_EVENTS | linux.IORING_POLL_ADD_MULTI;
            _ = try self.ring.poll_update(fd_u64 | INTERNAL_FLAG, fd_u64, fd_u64, mask, flags);
        }
    }

    fn iouringPoll(self: *Self, timeout_ms: ?u32) ![]const Event {
        // Submit any pending SQEs (new registrations, poll_updates, etc.)
        _ = try self.ring.submit();

        if (timeout_ms) |ms| {
            if (ms == 0) {
                // Non-blocking: just check for available completions
                const n = try self.ring.copy_cqes(&self.cqe_buf, 0);
                return self.iouringProcessCqes(n);
            }
            // Timed wait: submit a timeout SQE, then block until an event or timeout
            var ts = linux.kernel_timespec{
                .sec = @intCast(ms / 1000),
                .nsec = @intCast((@as(u64, ms) % 1000) * 1_000_000),
            };
            _ = try self.ring.timeout(TIMEOUT_SENTINEL, &ts, 0, 0);
            _ = try self.ring.submit_and_wait(1);
            const n = try self.ring.copy_cqes(&self.cqe_buf, 0);
            return self.iouringProcessCqes(n);
        } else {
            // Block indefinitely until at least one event
            const n = try self.ring.copy_cqes(&self.cqe_buf, 1);
            return self.iouringProcessCqes(n);
        }
    }

    fn iouringProcessCqes(self: *Self, n: u32) ![]const Event {
        var count: usize = 0;
        for (self.cqe_buf[0..n]) |cqe| {
            // Skip timeout sentinel CQEs
            if (cqe.user_data == TIMEOUT_SENTINEL) continue;

            // Skip internal CQEs (poll_update/poll_remove results)
            if (cqe.user_data & INTERNAL_FLAG != 0) continue;

            // Skip error CQEs (e.g., cancelled operations)
            if (cqe.res < 0) continue;

            const fd: i32 = @bitCast(@as(u32, @intCast(cqe.user_data & 0xFFFFFFFF)));

            const source = self.sources.get(fd) orelse continue;

            // Timer: drain the timerfd counter to prevent re-triggering
            if (source.tag == .timer) {
                var buf: [8]u8 = undefined;
                _ = posix.read(fd, &buf) catch {};
            }

            const revents: u32 = @intCast(cqe.res);
            self.result_cache[count] = .{
                .fd = fd,
                .tag = source.tag,
                .user_data = source.user_data,
                .readable = (revents & linux.POLL.IN) != 0,
                .writable = (revents & linux.POLL.OUT) != 0,
                .err = (revents & linux.POLL.ERR) != 0,
                .hangup = (revents & linux.POLL.HUP) != 0,
            };
            count += 1;

            // If multi-shot poll was dropped (IORING_CQE_F_MORE not set),
            // re-arm the poll for this fd.
            if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
                var mask: u32 = 0;
                if (source.interests.readable) mask |= linux.POLL.IN;
                if (source.interests.writable) mask |= linux.POLL.OUT;
                if (mask != 0) {
                    const fd_u64: u64 = @intCast(@as(u32, @bitCast(fd)));
                    const sqe = self.ring.poll_add(fd_u64, fd, mask) catch continue;
                    sqe.len = linux.IORING_POLL_ADD_MULTI;
                }
            }
        }
        return self.result_cache[0..count];
    }

    // ───────────────────────────────────────────────────────────────
    //  Shared helpers
    // ───────────────────────────────────────────────────────────────

    fn makeKevent(fd: i32, filter: i16, flags: u16, udata: usize) posix.Kevent {
        return .{
            .ident = @intCast(fd),
            .filter = filter,
            .flags = flags,
            .fflags = 0,
            .data = 0,
            .udata = udata,
        };
    }
};

// ── Tests ──

test "Reactor: create and destroy" {
    const allocator = std.testing.allocator;
    var reactor = try Reactor.init(allocator);
    defer reactor.deinit();
}

test "Reactor: register pipe fd, verify callback fires" {
    const allocator = std.testing.allocator;

    // Create a pipe
    const pipe = try @import("stdx").io.pipe();
    defer {
        _ = std.c.close(pipe[0]);
        _ = std.c.close(pipe[1]);
    }

    var reactor = try Reactor.init(allocator);
    defer reactor.deinit();

    // Register read end for readable interest
    try reactor.addSource(.{
        .fd = pipe[0],
        .tag = .acceptor_pipe,
        .interests = .{ .readable = true },
        .user_data = 42,
    });

    // Write a byte to make the pipe readable
    _ = std.c.write(pipe[1], "X", 1);

    // Poll — should fire
    const events = try reactor.poll(100);
    try std.testing.expect(events.len >= 1);

    var found = false;
    for (events) |ev| {
        if (ev.fd == pipe[0] and ev.tag == .acceptor_pipe) {
            try std.testing.expect(ev.readable);
            try std.testing.expectEqual(@as(usize, 42), ev.user_data);
            found = true;
        }
    }
    try std.testing.expect(found);
}

test "Reactor: timer fires" {
    const allocator = std.testing.allocator;
    var reactor = try Reactor.init(allocator);
    defer reactor.deinit();

    // Add a 10ms timer
    try reactor.addTimer(99, 10);

    // Sleep a bit then poll
    @import("stdx").time.sleep(20 * std.time.ns_per_ms);

    const events = try reactor.poll(50);
    var timer_fired = false;
    for (events) |ev| {
        if (ev.tag == .timer and ev.user_data == 99) {
            timer_fired = true;
        }
    }
    try std.testing.expect(timer_fired);
}

test "Reactor: add and remove source" {
    const allocator = std.testing.allocator;

    const pipe = try @import("stdx").io.pipe();
    defer {
        _ = std.c.close(pipe[0]);
        _ = std.c.close(pipe[1]);
    }

    var reactor = try Reactor.init(allocator);
    defer reactor.deinit();

    try reactor.addSource(.{
        .fd = pipe[0],
        .tag = .client_read,
        .interests = .{ .readable = true },
    });

    // Should be registered
    try std.testing.expect(reactor.sources.contains(pipe[0]));

    // Remove
    reactor.removeSource(pipe[0]);
    try std.testing.expect(!reactor.sources.contains(pipe[0]));
}

test "Reactor: arm and disarm writable" {
    const allocator = std.testing.allocator;

    const pipe = try @import("stdx").io.pipe();
    defer {
        _ = std.c.close(pipe[0]);
        _ = std.c.close(pipe[1]);
    }

    var reactor = try Reactor.init(allocator);
    defer reactor.deinit();

    // Register with readable only
    try reactor.addSource(.{
        .fd = pipe[1],
        .tag = .client_write,
        .interests = .{ .readable = false, .writable = false },
    });

    // Arm writable
    try reactor.armWritable(pipe[1]);
    {
        const source = reactor.sources.get(pipe[1]).?;
        try std.testing.expect(source.interests.writable);
    }

    // Disarm writable
    try reactor.disarmWritable(pipe[1]);
    {
        const source = reactor.sources.get(pipe[1]).?;
        try std.testing.expect(!source.interests.writable);
    }
}
