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
// On Linux: epoll + timerfd
//
// See: NODE_NETWORK_DESIGN.md §5 — The Reactor

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Allocator = std.mem.Allocator;

const is_linux = builtin.os.tag == .linux;
const is_bsd = builtin.os.tag == .macos or builtin.os.tag == .freebsd or
    builtin.os.tag == .openbsd or builtin.os.tag == .netbsd;

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
    /// Backend fd: kqueue on BSD, epoll on Linux.
    poll_fd: i32,
    allocator: Allocator,

    // Map fd → EventSource metadata for dispatch
    sources: std.AutoHashMap(i32, EventSource),

    // Timer bookkeeping — on Linux we create timerfd per timer,
    // on BSD kqueue handles timers natively.
    timer_fds: if (is_linux) std.AutoHashMap(usize, i32) else void,

    // Reusable event buffers (platform-specific)
    kqueue_buf: if (is_bsd) [MAX_EVENTS]posix.Kevent else void,
    epoll_buf: if (is_linux) [MAX_EVENTS]std.os.linux.epoll_event else void,

    // Internal result cache to hold converted events between poll calls
    result_cache: [MAX_EVENTS]Event = undefined,

    const Self = @This();

    pub fn init(allocator: Allocator) !Self {
        if (comptime is_bsd) {
            const kq = try posix.kqueue();
            return .{
                .poll_fd = kq,
                .allocator = allocator,
                .sources = std.AutoHashMap(i32, EventSource).init(allocator),
                .timer_fds = {},
                .kqueue_buf = undefined,
                .epoll_buf = {},
            };
        } else if (comptime is_linux) {
            const epfd = try posix.epoll_create1(std.os.linux.EPOLL.CLOEXEC);
            return .{
                .poll_fd = epfd,
                .allocator = allocator,
                .sources = std.AutoHashMap(i32, EventSource).init(allocator),
                .timer_fds = std.AutoHashMap(usize, i32).init(allocator),
                .kqueue_buf = {},
                .epoll_buf = undefined,
            };
        } else {
            @compileError("Unsupported OS — Reactor requires macOS/FreeBSD (kqueue) or Linux (epoll)");
        }
    }

    pub fn deinit(self: *Self) void {
        if (comptime is_linux) {
            // Close all timerfd file descriptors
            var it = self.timer_fds.valueIterator();
            while (it.next()) |tfd_ptr| {
                posix.close(tfd_ptr.*);
            }
            self.timer_fds.deinit();
        }
        self.sources.deinit();
        posix.close(self.poll_fd);
    }

    /// Register an event source (fd + interests).
    pub fn addSource(self: *Self, source: EventSource) !void {
        if (comptime is_bsd) {
            try self.kqueueAddSource(source);
        } else if (comptime is_linux) {
            try self.epollAddSource(source);
        }
        try self.sources.put(source.fd, source);
    }

    /// Remove an fd from the reactor.
    pub fn removeSource(self: *Self, fd: i32) void {
        if (comptime is_bsd) {
            self.kqueueRemoveSource(fd);
        } else if (comptime is_linux) {
            self.epollRemoveSource(fd);
        }
        _ = self.sources.remove(fd);
    }

    /// Modify event interests for an existing fd.
    pub fn modifyInterests(self: *Self, fd: i32, interests: Interests) !void {
        if (self.sources.getPtr(fd)) |source| {
            if (comptime is_bsd) {
                try self.kqueueModifyInterests(fd, interests, source);
            } else if (comptime is_linux) {
                try self.epollModifyInterests(fd, interests, source);
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
                    _ = try posix.kevent(self.poll_fd, &changelist, &.{}, null);
                } else if (comptime is_linux) {
                    const events = std.os.linux.EPOLL.IN | std.os.linux.EPOLL.OUT | std.os.linux.EPOLL.RDHUP;
                    var ev = std.os.linux.epoll_event{
                        .events = events,
                        .data = .{ .u64 = @intCast(source.user_data) },
                    };
                    try posix.epoll_ctl(self.poll_fd, std.os.linux.EPOLL.CTL_MOD, fd, &ev);
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
                    _ = try posix.kevent(self.poll_fd, &changelist, &.{}, null);
                } else if (comptime is_linux) {
                    // Keep readable, remove writable
                    const events = if (source.interests.readable) std.os.linux.EPOLL.IN | std.os.linux.EPOLL.RDHUP else @as(u32, 0);
                    var ev = std.os.linux.epoll_event{
                        .events = events,
                        .data = .{ .u64 = @intCast(source.user_data) },
                    };
                    if (events == 0) {
                        posix.epoll_ctl(self.poll_fd, std.os.linux.EPOLL.CTL_DEL, fd, null) catch {};
                    } else {
                        try posix.epoll_ctl(self.poll_fd, std.os.linux.EPOLL.CTL_MOD, fd, &ev);
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
            _ = try posix.kevent(self.poll_fd, &changelist, &.{}, null);
        } else if (comptime is_linux) {
            const tfd = try posix.timerfd_create(.MONOTONIC, .{ .CLOEXEC = true });
            errdefer posix.close(tfd);

            // Convert interval_ms to itimerspec (repeating timer)
            const secs: i64 = @intCast(interval_ms / 1000);
            const nsecs: i64 = @intCast((@as(u64, interval_ms) % 1000) * 1_000_000);
            const spec = std.os.linux.itimerspec{
                .it_interval = .{ .sec = secs, .nsec = nsecs },
                .it_value = .{ .sec = secs, .nsec = nsecs },
            };
            try posix.timerfd_settime(tfd, .{}, &spec, null);

            // Register timerfd with epoll — store ident in user_data
            var ev = std.os.linux.epoll_event{
                .events = std.os.linux.EPOLL.IN,
                .data = .{ .u64 = @intCast(ident) },
            };
            try posix.epoll_ctl(self.poll_fd, std.os.linux.EPOLL.CTL_ADD, tfd, &ev);

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
            return self.epollPoll(timeout_ms);
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
            _ = try posix.kevent(self.poll_fd, changelist[0..nchanges], &.{}, null);
        }
    }

    fn kqueueRemoveSource(self: *Self, fd: i32) void {
        var changelist: [2]posix.Kevent = undefined;
        changelist[0] = makeKevent(fd, posix.system.EVFILT.READ, posix.system.EV.DELETE, 0);
        changelist[1] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.DELETE, 0);
        _ = posix.kevent(self.poll_fd, &changelist, &.{}, null) catch {};
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
            _ = try posix.kevent(self.poll_fd, changelist[0..nchanges], &.{}, null);
        }
    }

    fn kqueuePoll(self: *Self, timeout_ms: ?u32) ![]const Event {
        const timeout: ?posix.timespec = if (timeout_ms) |ms| .{
            .sec = @intCast(ms / 1000),
            .nsec = @intCast((@as(u64, ms) % 1000) * 1_000_000),
        } else null;

        const nready = posix.kevent(
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
    //  epoll backend (Linux)
    // ───────────────────────────────────────────────────────────────

    fn epollAddSource(self: *Self, source: EventSource) !void {
        var events: u32 = std.os.linux.EPOLL.RDHUP;
        if (source.interests.readable) events |= std.os.linux.EPOLL.IN;
        if (source.interests.writable) events |= std.os.linux.EPOLL.OUT;

        var ev = std.os.linux.epoll_event{
            .events = events,
            .data = .{ .u64 = @intCast(source.user_data) },
        };
        try posix.epoll_ctl(self.poll_fd, std.os.linux.EPOLL.CTL_ADD, source.fd, &ev);
    }

    fn epollRemoveSource(self: *Self, fd: i32) void {
        if (comptime is_linux) {
            posix.epoll_ctl(self.poll_fd, std.os.linux.EPOLL.CTL_DEL, fd, null) catch {};
        }
    }

    fn epollModifyInterests(self: *Self, fd: i32, interests: Interests, source: *EventSource) !void {
        var events: u32 = std.os.linux.EPOLL.RDHUP;
        if (interests.readable) events |= std.os.linux.EPOLL.IN;
        if (interests.writable) events |= std.os.linux.EPOLL.OUT;

        var ev = std.os.linux.epoll_event{
            .events = events,
            .data = .{ .u64 = @intCast(source.user_data) },
        };
        try posix.epoll_ctl(self.poll_fd, std.os.linux.EPOLL.CTL_MOD, fd, &ev);
    }

    fn epollPoll(self: *Self, timeout_ms: ?u32) ![]const Event {
        const timeout_i32: i32 = if (timeout_ms) |ms| @intCast(ms) else -1;

        const nready = posix.epoll_wait(self.poll_fd, &self.epoll_buf, timeout_i32);

        // Build a reverse map: timerfd → ident (only needed if we have timers)
        var count: usize = 0;
        for (self.epoll_buf[0..nready]) |eev| {
            // Check if this is a timerfd event
            const udata = eev.data.u64;

            // Check timerfds — if the user_data matches a timer ident, it's a timer
            var is_timer = false;
            if (comptime is_linux) {
                var tfd_it = self.timer_fds.iterator();
                while (tfd_it.next()) |entry| {
                    if (entry.value_ptr.* != 0 and udata == @as(u64, entry.key_ptr.*)) {
                        // Drain the timerfd counter to prevent re-triggering
                        var buf: [8]u8 = undefined;
                        _ = posix.read(entry.value_ptr.*, &buf) catch {};
                        self.result_cache[count] = .{
                            .fd = entry.value_ptr.*,
                            .tag = .timer,
                            .user_data = @intCast(udata),
                            .readable = false,
                            .writable = false,
                            .err = false,
                            .hangup = false,
                        };
                        count += 1;
                        is_timer = true;
                        break;
                    }
                }
            }
            if (is_timer) continue;

            // Find the fd that corresponds to this user_data
            // For epoll we need to reverse-lookup from user_data → fd
            var found_fd: i32 = -1;
            var found_source: ?EventSource = null;
            var src_it = self.sources.iterator();
            while (src_it.next()) |entry| {
                if (entry.value_ptr.user_data == udata) {
                    found_fd = entry.key_ptr.*;
                    found_source = entry.value_ptr.*;
                    break;
                }
            }

            if (found_fd == -1) continue; // Unknown event, skip

            const source = found_source.?;
            const has_in = (eev.events & std.os.linux.EPOLL.IN) != 0;
            const has_out = (eev.events & std.os.linux.EPOLL.OUT) != 0;
            const has_err = (eev.events & std.os.linux.EPOLL.ERR) != 0;
            const has_hup = (eev.events & std.os.linux.EPOLL.HUP) != 0 or
                (eev.events & std.os.linux.EPOLL.RDHUP) != 0;

            self.result_cache[count] = .{
                .fd = found_fd,
                .tag = source.tag,
                .user_data = @intCast(udata),
                .readable = has_in,
                .writable = has_out,
                .err = has_err,
                .hangup = has_hup,
            };
            count += 1;
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
    const pipe = try posix.pipe();
    defer {
        posix.close(pipe[0]);
        posix.close(pipe[1]);
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
    _ = try posix.write(pipe[1], "X");

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
    std.Thread.sleep(20 * std.time.ns_per_ms);

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

    const pipe = try posix.pipe();
    defer {
        posix.close(pipe[0]);
        posix.close(pipe[1]);
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

    const pipe = try posix.pipe();
    defer {
        posix.close(pipe[0]);
        posix.close(pipe[1]);
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
