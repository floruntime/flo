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

const log = std.log.scoped(.reactor);

const is_linux = builtin.target.os.tag == .linux;
const is_bsd = builtin.target.os.tag == .macos or builtin.target.os.tag == .freebsd or
    builtin.target.os.tag == .openbsd or builtin.target.os.tag == .netbsd;

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
    /// Registration generation, assigned by `addSource` and carried in the
    /// backend's event identity (io_uring user_data high bits, kqueue udata). An
    /// fd number is reused across connections; a completion that outlives its fd
    /// (a lingering io_uring multishot CQE, a batched kevent) would otherwise
    /// route by fd to the connection that now holds the number. The poll loop
    /// drops any event whose gen does not match the live source. 0 = never
    /// bumped (timers, which are not reused).
    gen: u32 = 0,
    /// Multishot poll ends that came back as a kernel error (a negative CQE, not
    /// `-ECANCELED`), with no good completion in between. A good completion
    /// resets it; a long enough run means re-arming only spins, so the reactor
    /// gives up. Reset whenever the source is re-registered.
    neg_ends: u8 = 0,
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

    /// Times a multishot poll could not be re-armed, so the fd was put on
    /// `deferred_rearm` to try again rather than left silently un-polled. The
    /// owner reads this for a metric.
    rearm_failures: u64 = 0,

    /// Next registration generation (see `EventSource.gen`). Starts at 1 so a
    /// zero gen means "never registered through addSource" (timers); wraps
    /// within 31 bits so it fits beside the fd in the io_uring user_data.
    next_gen: u32 = 1,

    /// Completions dropped because their generation no longer matched the live
    /// source — a stale event for an fd that has since been closed and reused.
    /// The owner reads this for a metric; nonzero means fd-reuse races are
    /// happening (and being caught).
    stale_events_dropped: u64 = 0,

    /// Monotonic ms of the last deferred-re-arm log, so a burst under load logs
    /// at most once an interval rather than per fd.
    arm_log_ms: u64 = 0,

    /// fds whose multishot re-arm could not be placed this tick (a full
    /// submission queue): re-armed at the start of the next poll, after that
    /// tick's submit has made room. A full SQ under load is then a one-tick
    /// delay for every tag, not a wave of closes; a client fd that still cannot
    /// be armed on the retry is closed loudly. Held as a set of fds — the mask,
    /// tag and user_data are read from the live source at re-arm time, so a
    /// stale mask or a reused fd can never arm the wrong poll — and dropped by
    /// `removeSource`, so a closed fd leaves nothing behind.
    deferred_rearm: std.AutoArrayHashMapUnmanaged(i32, void) = .empty,

    /// Tests only (gated on `builtin.is_test`): when set, the next `pollAdd` or
    /// `pollUpdate` returns this error instead of touching the ring, and clears
    /// it. Every arm path (a fresh registration, `armWritable`, `modifyInterests`,
    /// and the multishot re-arm) runs through those two, so one set exercises the
    /// re-arm-or-defer path. Null in production.
    test_arm_error: ?anyerror = null,

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
            // A bare `error.PermissionDenied` here is nearly undiagnosable: it
            // names neither io_uring nor seccomp, and the usual cause is a
            // container runtime's default seccomp profile blocking the
            // io_uring syscalls.
            const ring = linux.IoUring.init(256, 0) catch |err| {
                switch (err) {
                    error.PermissionDenied, error.SystemOutdated => log.err(
                        "io_uring is unavailable ({s}). Flo's event loop requires io_uring on Linux.\n" ++
                            "  * Under a container runtime's default seccomp profile the io_uring syscalls are blocked.\n" ++
                            "    Run with `--security-opt seccomp=unconfined`, or use a profile that allows\n" ++
                            "    io_uring_setup, io_uring_enter and io_uring_register.\n" ++
                            "  * On kernels older than 5.1, io_uring does not exist and Flo cannot start.",
                        .{@errorName(err)},
                    ),
                    else => log.err("io_uring initialisation failed: {s}", .{@errorName(err)}),
                }
                return err;
            };
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
        self.deferred_rearm.deinit(self.allocator);
        if (comptime is_bsd) {
            _ = std.c.close(self.poll_fd);
        }
    }

    /// Register an event source (fd + interests). Each registration gets a fresh
    /// generation, so a reused fd number is a distinct source and a completion
    /// left over from the previous owner is dropped rather than misdelivered.
    pub fn addSource(self: *Self, source_in: EventSource) !void {
        var source = source_in;
        source.gen = self.nextGen();
        if (comptime is_bsd) {
            try self.kqueueAddSource(source);
        } else if (comptime is_linux) {
            try self.iouringAddSource(source);
        }
        try self.sources.put(source.fd, source);
    }

    /// A fresh registration generation in `[1, 2^31)`, so it never collides with
    /// a timer's 0 and always fits beside the fd in a 64-bit io_uring user_data.
    fn nextGen(self: *Self) u32 {
        const g = self.next_gen;
        self.next_gen = if (self.next_gen >= 0x7FFF_FFFF) 1 else self.next_gen + 1;
        return g;
    }

    /// Pack an fd and its generation into an io_uring poll user_data: fd in the
    /// low 32 bits, gen in bits [32..62]. Bit 63 stays clear for INTERNAL_FLAG.
    fn pollUserData(fd: i32, gen: u32) u64 {
        return @as(u64, @as(u32, @bitCast(fd))) | (@as(u64, gen) << 32);
    }

    /// Remove an fd from the reactor.
    pub fn removeSource(self: *Self, fd: i32) void {
        if (comptime is_bsd) {
            self.kqueueRemoveSource(fd);
        } else if (comptime is_linux) {
            self.iouringRemoveSource(fd);
            // Drop any pending re-arm: the next fd to reuse this number must not
            // inherit it (the drain would arm a second poll on the new one, or
            // close a healthy connection).
            _ = self.deferred_rearm.swapRemove(fd);
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
                    changelist[0] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.ADD | posix.system.EV.ENABLE, source.gen);
                    _ = try keventCall(self.poll_fd, &changelist, &.{}, null);
                } else if (comptime is_linux) {
                    try self.iouringSetInterests(fd, source.interests, .{ .readable = source.interests.readable, .writable = true });
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
                    try self.iouringSetInterests(fd, source.interests, .{ .readable = source.interests.readable, .writable = false });
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
            try self.pollAdd(tfd_u64, tfd, linux.POLL.IN);

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

        // The kevent udata carries the registration generation, not the
        // Connection pointer: the poll loop reads user_data back from the live
        // source, and uses the udata only to drop a stale event after fd reuse.
        if (source.interests.readable) {
            changelist[nchanges] = makeKevent(
                source.fd,
                posix.system.EVFILT.READ,
                posix.system.EV.ADD | posix.system.EV.ENABLE,
                source.gen,
            );
            nchanges += 1;
        }

        if (source.interests.writable) {
            changelist[nchanges] = makeKevent(
                source.fd,
                posix.system.EVFILT.WRITE,
                posix.system.EV.ADD | posix.system.EV.ENABLE,
                source.gen,
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
            changelist[nchanges] = makeKevent(fd, posix.system.EVFILT.READ, posix.system.EV.ADD | posix.system.EV.ENABLE, source.gen);
            nchanges += 1;
        } else if (!interests.readable and source.interests.readable) {
            changelist[nchanges] = makeKevent(fd, posix.system.EVFILT.READ, posix.system.EV.DELETE, 0);
            nchanges += 1;
        }

        if (interests.writable and !source.interests.writable) {
            changelist[nchanges] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.ADD | posix.system.EV.ENABLE, source.gen);
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
            // Drop a stale event whose generation no longer matches the live
            // source — the fd was closed and reused since this kevent was armed.
            if (source) |s| {
                if (s.gen != @as(u32, @intCast(kev.udata & 0x7FFF_FFFF))) {
                    self.stale_events_dropped += 1;
                    continue;
                }
            }
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
            try self.pollAdd(pollUserData(source.fd, source.gen), source.fd, mask);
        }
    }

    fn iouringRemoveSource(self: *Self, fd: i32) void {
        // Target the poll by its full user_data (fd + the live registration's
        // gen): io_uring matches it exactly, so the bare fd would not be found
        // and the multishot would leak, pinning the file.
        const target = pollUserData(fd, if (self.sources.get(fd)) |s| s.gen else 0);
        self.pollRemove(target) catch |err| log.err("reactor: could not stop polling fd {d}: {s}", .{ fd, @errorName(err) });
    }

    // The submission queue is sent to the kernel only when the reactor
    // polls, so a burst of interest changes in one tick (connections pausing
    // and resuming together) can fill it. A change that did not fit would be
    // lost: a connection never read again, or polled twice. Each of these
    // submits what is queued and retries, up to ARM_TRIES times.

    /// How many times an arm submits the queue and tries again before it
    /// returns the error. A full submission queue clears once the kernel takes
    /// it, so a couple of retries almost always find room; past that the arm
    /// fails and the caller must not leave the fd un-armed — a re-arm defers it
    /// to the next tick, a fresh registration closes the new connection.
    const ARM_TRIES = 3;

    fn pollAdd(self: *Self, user_data: u64, fd: i32, mask: u32) !void {
        if (builtin.is_test) {
            if (self.test_arm_error) |e| {
                self.test_arm_error = null;
                return e;
            }
        }
        var tries: u8 = 0;
        while (true) : (tries += 1) {
            if (self.ring.poll_add(user_data, fd, mask)) |sqe| {
                sqe.len = linux.IORING_POLL_ADD_MULTI;
                return;
            } else |err| {
                if (tries + 1 >= ARM_TRIES) return err;
                _ = try self.ring.submit();
            }
        }
    }

    fn pollUpdate(self: *Self, target: u64, mask: u32) !void {
        if (builtin.is_test) {
            if (self.test_arm_error) |e| {
                self.test_arm_error = null;
                return e;
            }
        }
        const flags = linux.IORING_POLL_UPDATE_EVENTS | linux.IORING_POLL_ADD_MULTI;
        var tries: u8 = 0;
        while (true) : (tries += 1) {
            // Find the poll by `target` (fd + gen); its own CQE carries
            // INTERNAL_FLAG so it is skipped. The user_data is unchanged.
            if (self.ring.poll_update(target | INTERNAL_FLAG, target, target, mask, flags)) |_| {
                return;
            } else |err| {
                if (tries + 1 >= ARM_TRIES) return err;
                _ = try self.ring.submit();
            }
        }
    }

    fn pollRemove(self: *Self, target: u64) !void {
        _ = self.ring.poll_remove(target | INTERNAL_FLAG, target) catch {
            _ = try self.ring.submit();
            _ = try self.ring.poll_remove(target | INTERNAL_FLAG, target);
        };
    }

    fn iouringModifyInterests(self: *Self, fd: i32, interests: Interests, source: *EventSource) !void {
        try self.iouringSetInterests(fd, source.interests, interests);
    }

    fn pollMask(interests: Interests) u32 {
        var mask: u32 = 0;
        if (interests.readable) mask |= linux.POLL.IN;
        if (interests.writable) mask |= linux.POLL.OUT;
        return mask;
    }

    /// Move `fd`'s poll from `old` interests to `new`. With none, the poll is
    /// removed, and an update cannot bring a removed poll back: a paused
    /// connection with nothing to send would never be read again. So going
    /// from none to some adds a fresh poll.
    fn iouringSetInterests(self: *Self, fd: i32, old: Interests, new: Interests) !void {
        const from = pollMask(old);
        const to = pollMask(new);
        if (from == to) return;
        // io_uring matches the poll's user_data exactly, so remove, update and
        // the from==0 re-add all target the poll's full identity: the fd plus
        // this registration's gen (only addSource bumps gen, so a re-add keeps
        // it). The bare fd would miss the poll armed by pollUserData.
        const target = pollUserData(fd, if (self.sources.get(fd)) |s| s.gen else 0);
        if (to == 0) {
            try self.pollRemove(target);
        } else if (from == 0) {
            try self.pollAdd(target, fd, to);
            // This fd now has a live poll again. If a dropped multishot left it on
            // the deferred list (it went to no interests and back within a tick),
            // drop that entry, or the next drain would arm a second poll on it.
            _ = self.deferred_rearm.swapRemove(fd);
        } else {
            try self.pollUpdate(target, to);
        }
    }

    fn iouringPoll(self: *Self, timeout_ms: ?u32) ![]const Event {
        // Submit any pending SQEs (new registrations, poll_updates, etc.)
        _ = try self.ring.submit();

        // Now that the submit has drained the SQ, retry any re-arms a previous
        // tick could not fit. Error events it raises (a client fd that fails
        // twice) go at the front of result_cache; this tick's completions follow.
        const start = self.drainDeferredRearm();

        if (timeout_ms) |ms| {
            if (ms == 0) {
                // Non-blocking: just check for available completions
                const n = try self.ring.copy_cqes(&self.cqe_buf, 0);
                return self.iouringProcessCqes(start, n);
            }
            // Timed wait: submit a timeout SQE, then block until an event or timeout
            var ts = linux.kernel_timespec{
                .sec = @intCast(ms / 1000),
                .nsec = @intCast((@as(u64, ms) % 1000) * 1_000_000),
            };
            // count = 1: the timeout completes with the first other event,
            // so a wake does not leave it armed to fire an empty tick later.
            _ = try self.ring.timeout(TIMEOUT_SENTINEL, &ts, 1, 0);
            // A signal landing on this thread (a stop, say) is an empty poll,
            // as on kqueue, not a failed tick.
            _ = self.ring.submit_and_wait(1) catch |err| switch (err) {
                error.SignalInterrupt => return self.result_cache[0..start],
                else => return err,
            };
            const n = try self.ring.copy_cqes(&self.cqe_buf, 0);
            return self.iouringProcessCqes(start, n);
        } else {
            // Block indefinitely until at least one event
            const n = try self.ring.copy_cqes(&self.cqe_buf, 1);
            return self.iouringProcessCqes(start, n);
        }
    }

    fn iouringProcessCqes(self: *Self, start: usize, n: u32) ![]const Event {
        var count: usize = start;
        for (self.cqe_buf[0..n]) |cqe| {
            // Skip timeout sentinel CQEs
            if (cqe.user_data == TIMEOUT_SENTINEL) continue;

            // Skip internal CQEs (poll_update/poll_remove results)
            if (cqe.user_data & INTERNAL_FLAG != 0) continue;

            const fd: i32 = @bitCast(@as(u32, @intCast(cqe.user_data & 0xFFFFFFFF)));
            const source = self.sources.getPtr(fd) orelse continue;

            // Drop a completion whose generation no longer matches: a poll armed
            // for an earlier owner of this fd number, firing after the fd was
            // closed and reused. Routing it by fd would hit the new connection.
            const gen: u32 = @intCast((cqe.user_data >> 32) & 0x7FFF_FFFF);
            if (source.gen != gen) {
                self.stale_events_dropped += 1;
                continue;
            }

            // A negative result ends the multishot poll. -ECANCELED is a
            // deliberate pollRemove (the fd is being dropped) — and older kernels
            // also end a multishot this way when the completion queue is full —
            // so let it go. Any other negative end leaves a source that still
            // wants events un-polled, so re-arm it; but a re-arm the ring accepts
            // and the kernel fails again at once would hot-loop, so after a run
            // of immediate failures give up rather than spin.
            if (cqe.res < 0) {
                if (cqe.res == -@as(i32, @backingInt(linux.E.CANCELED))) continue;
                source.neg_ends +|= 1;
                if (source.neg_ends > MAX_NEG_ENDS) {
                    count = self.giveUpSource(fd, source.*, count);
                } else {
                    self.rearmOrDefer(fd, source.*);
                }
                continue;
            }
            // A good completion: the poll is healthy, so forget earlier failures.
            source.neg_ends = 0;

            // Timer: drain the timerfd counter to prevent re-triggering
            if (source.tag == .timer) {
                var buf: [8]u8 = undefined;
                _ = posix.read(fd, &buf) catch {};
            }

            // A poll that fired just before it was removed or changed still
            // completes: report only what is asked for now, as kqueue does,
            // or a connection taken off reading would be read.
            const revents: u32 = @intCast(cqe.res);
            const event: Event = .{
                .fd = fd,
                .tag = source.tag,
                .user_data = source.user_data,
                .readable = (revents & linux.POLL.IN) != 0 and source.interests.readable,
                .writable = (revents & linux.POLL.OUT) != 0 and source.interests.writable,
                .err = (revents & linux.POLL.ERR) != 0,
                .hangup = (revents & linux.POLL.HUP) != 0,
            };
            if (event.readable or event.writable or event.err or event.hangup) {
                if (count < self.result_cache.len) {
                    self.result_cache[count] = event;
                    count += 1;
                }
            }

            // A multishot poll the kernel dropped (IORING_CQE_F_MORE not set)
            // must be re-armed, or the fd is never polled again.
            if (cqe.flags & linux.IORING_CQE_F_MORE == 0) self.rearmOrDefer(fd, source.*);
        }
        return self.result_cache[0..count];
    }

    /// After `MAX_NEG_ENDS` immediate re-arm failures in a row, stop re-arming
    /// `fd`. A client fd is surfaced as an error event (appended at `count`) so
    /// the shard closes it; an infrastructure fd cannot be closed, so it is
    /// logged loudly and left. Either way its deferred entry is dropped.
    fn giveUpSource(self: *Self, fd: i32, source: EventSource, count: usize) usize {
        _ = self.deferred_rearm.swapRemove(fd);
        if (source.tag == .client_read) {
            if (count < self.result_cache.len) {
                self.result_cache[count] = .{ .fd = fd, .tag = source.tag, .user_data = source.user_data, .readable = false, .writable = false, .err = true, .hangup = false };
                return count + 1;
            }
            return count;
        }
        if (self.armLogDue()) log.err("reactor: fd {d} ({s}) keeps failing to re-arm; giving up on it", .{ fd, @tagName(source.tag) });
        return count;
    }

    /// Re-arm a multishot poll that ended while its source still wants events.
    /// When the submission queue is full the arm is deferred to the next poll,
    /// after its submit makes room, rather than silently lost.
    fn rearmOrDefer(self: *Self, fd: i32, source: EventSource) void {
        const mask = pollMask(source.interests);
        if (mask == 0) return;
        self.pollAdd(pollUserData(fd, source.gen), fd, mask) catch {
            self.rearm_failures += 1;
            self.deferred_rearm.put(self.allocator, fd, {}) catch {
                log.err("reactor: fd {d} could not be re-armed or even deferred; it is no longer polled", .{fd});
                return;
            };
            if (self.armLogDue()) log.warn("reactor: submission queue full, deferred a poll re-arm to the next tick (rearm_failures={d}, deferred={d})", .{ self.rearm_failures, self.deferred_rearm.count() });
        };
    }

    const ARM_LOG_INTERVAL_MS = 1000;
    /// Consecutive immediate re-arm failures tolerated before giving up on an fd.
    const MAX_NEG_ENDS = 3;

    /// Rate-limit the arm-failure logs to one per `ARM_LOG_INTERVAL_MS`.
    fn armLogDue(self: *Self) bool {
        const now = @import("stdx").time.monotonicMs();
        if (now -| self.arm_log_ms < ARM_LOG_INTERVAL_MS) return false;
        self.arm_log_ms = now;
        return true;
    }

    /// At the start of a poll (after its submit has made room), re-arm every fd
    /// a previous tick could not. The mask, tag and user_data come from the live
    /// source — never a stale copy — and a source that is gone or no longer wants
    /// events is simply dropped. One that arms is removed; a client fd that fails
    /// again is surfaced as an error so the shard closes it (rare, loud); an
    /// infrastructure fd (the acceptor pipe, a Raft wake, the inbox) is kept for
    /// the next tick, since it cannot be closed. Returns how many error events it
    /// wrote at the front of `result_cache`.
    fn drainDeferredRearm(self: *Self) usize {
        var errs: usize = 0;
        var i: usize = 0;
        while (i < self.deferred_rearm.count()) {
            const fd = self.deferred_rearm.keys()[i];
            const source = self.sources.get(fd) orelse {
                self.deferred_rearm.swapRemoveAt(i);
                continue;
            };
            const mask = pollMask(source.interests);
            if (mask == 0) {
                self.deferred_rearm.swapRemoveAt(i);
                continue;
            }
            if (self.pollAdd(pollUserData(fd, source.gen), fd, mask)) |_| {
                self.deferred_rearm.swapRemoveAt(i);
            } else |_| {
                if (source.tag == .client_read) {
                    if (errs < self.result_cache.len) {
                        self.result_cache[errs] = .{ .fd = fd, .tag = source.tag, .user_data = source.user_data, .readable = false, .writable = false, .err = true, .hangup = false };
                        errs += 1;
                    }
                    self.deferred_rearm.swapRemoveAt(i);
                } else {
                    i += 1;
                }
            }
        }
        return errs;
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

test "Reactor: a connection not read for a while, with nothing to send, is read again once asked" {
    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer for (pair) |fd| {
        _ = std.c.close(fd);
    };
    var reactor = try Reactor.init(std.testing.allocator);
    defer reactor.deinit();
    try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });
    _ = std.c.write(pair[1], "X", 1);

    const Seen = struct {
        fn of(events: []const Event, fd: i32) ?Event {
            for (events) |ev| if (ev.fd == fd) return ev;
            return null;
        }
    };
    // Reads off, and something to send: only writable is reported, though
    // a byte waits to be read.
    try reactor.modifyInterests(pair[0], .{ .readable = false, .writable = false });
    try reactor.armWritable(pair[0]);
    const writable = Seen.of(try reactor.poll(100), pair[0]).?;
    try std.testing.expect(writable.writable and !writable.readable);
    // Sent: no interest left at all, and nothing is reported.
    try reactor.disarmWritable(pair[0]);
    try std.testing.expect(Seen.of(try reactor.poll(10), pair[0]) == null);
    // Asked to read again: the waiting byte is reported.
    try reactor.modifyInterests(pair[0], .{ .readable = true, .writable = false });
    const readable = Seen.of(try reactor.poll(100), pair[0]).?;
    try std.testing.expect(readable.readable);
}

test "Reactor: more interest changes in one tick than the submission queue holds all take effect" {
    // Only io_uring queues changes; kqueue applies each at once.
    if (comptime !is_linux) return error.SkipZigTest;
    const n = 300; // each loop below makes 300 changes: more than the ring's 256 entries
    var pairs: [n][2]std.posix.fd_t = undefined;
    var opened: usize = 0;
    defer for (pairs[0..opened]) |p| {
        _ = std.c.close(p[0]);
        _ = std.c.close(p[1]);
    };
    var reactor = try Reactor.init(std.testing.allocator);
    defer reactor.deinit();
    for (&pairs) |*p| {
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, p));
        opened += 1;
        try reactor.addSource(.{ .fd = p[0], .tag = .client_read, .interests = .{ .readable = true } });
    }
    _ = try reactor.poll(0);
    for (pairs) |p| try reactor.modifyInterests(p[0], .{ .readable = false, .writable = false });
    for (pairs) |p| try reactor.modifyInterests(p[0], .{ .readable = true, .writable = false });
    for (pairs) |p| try reactor.armWritable(p[0]);
    for (pairs) |p| try reactor.disarmWritable(p[0]);
    for (pairs) |p| _ = std.c.write(p[1], "X", 1);

    var seen = std.AutoHashMap(i32, void).init(std.testing.allocator);
    defer seen.deinit();
    var polls: usize = 0;
    while (seen.count() < n and polls < 50) : (polls += 1) {
        for (try reactor.poll(100)) |ev| {
            if (ev.readable) try seen.put(ev.fd, {});
        }
    }
    try std.testing.expectEqual(@as(usize, n), seen.count());
}

test "Reactor: a dropped multishot re-arm the ring refuses is deferred, then re-armed next tick" {
    // Drives io_uring internals (cqe_buf, poll_add); the kqueue path has no such
    // re-arm. Runs on Linux only.
    if (is_linux) {
        var pair: [2]std.posix.fd_t = undefined;
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
        defer {
            _ = std.c.close(pair[0]);
            _ = std.c.close(pair[1]);
        }
        var reactor = try Reactor.init(std.testing.allocator);
        defer reactor.deinit();
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });

        // A readable completion whose multishot the kernel dropped (no F_MORE):
        // the reactor must re-arm the fd. Make that re-arm's poll_add fail the
        // way a wedged ring would; the fd goes on the deferred list, not left
        // un-polled, and the event still comes back.
        reactor.test_arm_error = error.SubmissionQueueFull;
        reactor.cqe_buf[0] = .{
            .user_data = Reactor.pollUserData(pair[0], reactor.sources.get(pair[0]).?.gen),
            .res = @intCast(linux.POLL.IN),
            .flags = 0,
        };
        const events = try reactor.iouringProcessCqes(0, 1);
        try std.testing.expectEqual(@as(usize, 1), events.len);
        try std.testing.expect(events[0].readable);
        try std.testing.expectEqual(pair[0], events[0].fd);
        try std.testing.expectEqual(@as(u64, 1), reactor.rearm_failures);
        try std.testing.expectEqual(@as(usize, 1), reactor.deferred_rearm.count());

        // Next tick: the ring takes the arm (the one-shot error cleared), so the
        // deferred fd clears with no error event.
        const errs = reactor.drainDeferredRearm();
        try std.testing.expectEqual(@as(usize, 0), errs);
        try std.testing.expectEqual(@as(usize, 0), reactor.deferred_rearm.count());
    } else return error.SkipZigTest;
}

test "Reactor: a client fd whose deferred re-arm fails again surfaces as an error to close it" {
    if (is_linux) {
        var pair: [2]std.posix.fd_t = undefined;
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
        defer {
            _ = std.c.close(pair[0]);
            _ = std.c.close(pair[1]);
        }
        var reactor = try Reactor.init(std.testing.allocator);
        defer reactor.deinit();
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });

        // First failure: the dropped re-arm is deferred.
        reactor.test_arm_error = error.SubmissionQueueFull;
        reactor.cqe_buf[0] = .{ .user_data = Reactor.pollUserData(pair[0], reactor.sources.get(pair[0]).?.gen), .res = @intCast(linux.POLL.IN), .flags = 0 };
        _ = try reactor.iouringProcessCqes(0, 1);
        try std.testing.expectEqual(@as(usize, 1), reactor.deferred_rearm.count());

        // The retry fails too: a client fd is given up on and reported as an
        // error event so the shard closes it; the deferred entry is dropped.
        reactor.test_arm_error = error.SubmissionQueueFull;
        const errs = reactor.drainDeferredRearm();
        try std.testing.expectEqual(@as(usize, 1), errs);
        try std.testing.expect(reactor.result_cache[0].err);
        try std.testing.expectEqual(pair[0], reactor.result_cache[0].fd);
        try std.testing.expectEqual(@as(usize, 0), reactor.deferred_rearm.count());
    } else return error.SkipZigTest;
}

test "Reactor: armWritable surfaces a refused arm so the shard can close the fd" {
    // A guard, not a regression: flushToClient arms writable after queueing a
    // response and closes the fd if the arm fails, and this holds the propagation
    // in place (it passes on the base too). Linux only.
    if (is_linux) {
        var pair: [2]std.posix.fd_t = undefined;
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
        defer {
            _ = std.c.close(pair[0]);
            _ = std.c.close(pair[1]);
        }
        var reactor = try Reactor.init(std.testing.allocator);
        defer reactor.deinit();
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });
        reactor.test_arm_error = error.SubmissionQueueFull;
        try std.testing.expectError(error.SubmissionQueueFull, reactor.armWritable(pair[0]));
        // One-shot cleared; the arm works on the next try.
        try reactor.armWritable(pair[0]);
    } else return error.SkipZigTest;
}

test "Reactor: a deferred re-arm is dropped when its fd is removed, so a reused fd is not double-armed" {
    if (is_linux) {
        var pair: [2]std.posix.fd_t = undefined;
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
        defer {
            _ = std.c.close(pair[0]);
            _ = std.c.close(pair[1]);
        }
        var reactor = try Reactor.init(std.testing.allocator);
        defer reactor.deinit();
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });

        reactor.test_arm_error = error.SubmissionQueueFull;
        reactor.cqe_buf[0] = .{ .user_data = Reactor.pollUserData(pair[0], reactor.sources.get(pair[0]).?.gen), .res = @intCast(linux.POLL.IN), .flags = 0 };
        _ = try reactor.iouringProcessCqes(0, 1);
        try std.testing.expectEqual(@as(usize, 1), reactor.deferred_rearm.count());

        // Removing the fd (the connection closed) drops its deferred entry, so a
        // later connection that reuses the number is not armed a second time.
        reactor.removeSource(pair[0]);
        try std.testing.expectEqual(@as(usize, 0), reactor.deferred_rearm.count());
        try std.testing.expectEqual(@as(usize, 0), reactor.drainDeferredRearm());
    } else return error.SkipZigTest;
}

test "Reactor: re-adding a deferred fd's poll (interests to none and back) clears its deferred entry" {
    if (is_linux) {
        var pair: [2]std.posix.fd_t = undefined;
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
        defer {
            _ = std.c.close(pair[0]);
            _ = std.c.close(pair[1]);
        }
        var reactor = try Reactor.init(std.testing.allocator);
        defer reactor.deinit();
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });

        // Defer a re-arm for the fd.
        reactor.test_arm_error = error.SubmissionQueueFull;
        reactor.cqe_buf[0] = .{ .user_data = Reactor.pollUserData(pair[0], reactor.sources.get(pair[0]).?.gen), .res = @intCast(linux.POLL.IN), .flags = 0 };
        _ = try reactor.iouringProcessCqes(0, 1);
        try std.testing.expectEqual(@as(usize, 1), reactor.deferred_rearm.count());

        // The connection drops to no interests (a pollRemove, -ENOENT on the
        // already-dropped poll) and comes back, which arms a fresh live poll via
        // the from==0 branch. That must clear the deferred entry, or the next
        // drain arms a second multishot poll on the same fd.
        try reactor.modifyInterests(pair[0], .{ .readable = false, .writable = false });
        try reactor.modifyInterests(pair[0], .{ .readable = true, .writable = false });
        try std.testing.expectEqual(@as(usize, 0), reactor.deferred_rearm.count());
        try std.testing.expectEqual(@as(usize, 0), reactor.drainDeferredRearm());
    } else return error.SkipZigTest;
}

test "Reactor: the drain re-arms from the live interests, not the mask captured when it was deferred" {
    if (is_linux) {
        var pair: [2]std.posix.fd_t = undefined;
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
        defer {
            _ = std.c.close(pair[0]);
            _ = std.c.close(pair[1]);
        }
        var reactor = try Reactor.init(std.testing.allocator);
        defer reactor.deinit();
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });

        // Defer a re-arm while the fd wants readable.
        reactor.test_arm_error = error.SubmissionQueueFull;
        reactor.cqe_buf[0] = .{ .user_data = Reactor.pollUserData(pair[0], reactor.sources.get(pair[0]).?.gen), .res = @intCast(linux.POLL.IN), .flags = 0 };
        _ = try reactor.iouringProcessCqes(0, 1);
        try std.testing.expectEqual(@as(usize, 1), reactor.deferred_rearm.count());

        // The source now wants nothing. The drain must read that off the live
        // source, skip the arm and drop the entry — not re-add the readable poll
        // it captured earlier. The injected failure proves no poll_add ran: a
        // stored-mask drain would have called it and consumed the one-shot.
        reactor.sources.getPtr(pair[0]).?.interests = .{ .readable = false, .writable = false };
        reactor.test_arm_error = error.SubmissionQueueFull;
        try std.testing.expectEqual(@as(usize, 0), reactor.drainDeferredRearm());
        try std.testing.expectEqual(@as(usize, 0), reactor.deferred_rearm.count());
        try std.testing.expect(reactor.test_arm_error != null);
    } else return error.SkipZigTest;
}

test "Reactor: a source whose re-arm the kernel fails at once is given up, not hot-looped" {
    if (is_linux) {
        var pair: [2]std.posix.fd_t = undefined;
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
        defer {
            _ = std.c.close(pair[0]);
            _ = std.c.close(pair[1]);
        }
        var reactor = try Reactor.init(std.testing.allocator);
        defer reactor.deinit();
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });

        // Each poll delivers a negative end (not -ECANCELED), as a re-arm the ring
        // accepted but the kernel failed immediately would. The first MAX_NEG_ENDS
        // re-arm silently; the next gives up with an error event instead of
        // spinning forever.
        var k: usize = 0;
        while (k <= Reactor.MAX_NEG_ENDS) : (k += 1) {
            reactor.cqe_buf[0] = .{ .user_data = Reactor.pollUserData(pair[0], reactor.sources.get(pair[0]).?.gen), .res = -5, .flags = 0 };
            const evs = try reactor.iouringProcessCqes(0, 1);
            if (k < Reactor.MAX_NEG_ENDS) {
                try std.testing.expectEqual(@as(usize, 0), evs.len);
            } else {
                try std.testing.expectEqual(@as(usize, 1), evs.len);
                try std.testing.expect(evs[0].err);
                try std.testing.expectEqual(pair[0], evs[0].fd);
            }
        }
    } else return error.SkipZigTest;
}

test "Reactor: re-registering an fd gives it a new generation, and a re-add keeps it" {
    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer {
        _ = std.c.close(pair[0]);
        _ = std.c.close(pair[1]);
    }
    var reactor = try Reactor.init(std.testing.allocator);
    defer reactor.deinit();
    try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });
    const g1 = reactor.sources.get(pair[0]).?.gen;
    try std.testing.expect(g1 != 0);

    // Interests to none and back keeps the same generation (only addSource bumps
    // it), so the re-added poll's completions still match the live source.
    try reactor.modifyInterests(pair[0], .{ .readable = false, .writable = false });
    try reactor.modifyInterests(pair[0], .{ .readable = true, .writable = false });
    try std.testing.expectEqual(g1, reactor.sources.get(pair[0]).?.gen);

    // Removing and re-adding the same fd (a reused number) bumps the generation.
    reactor.removeSource(pair[0]);
    try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });
    try std.testing.expect(reactor.sources.get(pair[0]).?.gen != g1);
}

test "Reactor: a completion for a reused fd's earlier registration is dropped, not misrouted" {
    if (is_linux) {
        var pair: [2]std.posix.fd_t = undefined;
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
        defer {
            _ = std.c.close(pair[0]);
            _ = std.c.close(pair[1]);
        }
        var reactor = try Reactor.init(std.testing.allocator);
        defer reactor.deinit();
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });
        const gen1 = reactor.sources.get(pair[0]).?.gen;
        reactor.removeSource(pair[0]);
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });
        const gen2 = reactor.sources.get(pair[0]).?.gen;
        try std.testing.expect(gen1 != gen2);

        // A lingering completion from the first registration (gen1) must be
        // dropped and counted, not delivered to the new connection.
        reactor.cqe_buf[0] = .{ .user_data = Reactor.pollUserData(pair[0], gen1), .res = @intCast(linux.POLL.IN), .flags = linux.IORING_CQE_F_MORE };
        try std.testing.expectEqual(@as(usize, 0), (try reactor.iouringProcessCqes(0, 1)).len);
        try std.testing.expectEqual(@as(u64, 1), reactor.stale_events_dropped);

        // A completion for the current registration is delivered.
        reactor.cqe_buf[0] = .{ .user_data = Reactor.pollUserData(pair[0], gen2), .res = @intCast(linux.POLL.IN), .flags = linux.IORING_CQE_F_MORE };
        const evs = try reactor.iouringProcessCqes(0, 1);
        try std.testing.expectEqual(@as(usize, 1), evs.len);
        try std.testing.expect(evs[0].readable);
        try std.testing.expectEqual(@as(u64, 1), reactor.stale_events_dropped);
    } else return error.SkipZigTest;
}

test "Reactor: removing an fd takes its poll off the ring, so a reused fd gets no stale event (real ring)" {
    // Exercises a real io_uring: poll_remove/poll_update must target the poll's
    // full user_data (fd + gen), or the multishot is never removed and a reused
    // fd gets duplicate/stale events. Hand-injected CQEs can't catch this.
    if (is_linux) {
        var pair: [2]std.posix.fd_t = undefined;
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
        defer {
            _ = std.c.close(pair[0]);
            _ = std.c.close(pair[1]);
        }
        var reactor = try Reactor.init(std.testing.allocator);
        defer reactor.deinit();

        // Arm, then remove, then re-register the same fd — each flushed to the
        // ring before the next, and all before the socket becomes readable, so
        // only the current registration's poll can fire.
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });
        _ = try reactor.poll(0);
        reactor.removeSource(pair[0]);
        _ = try reactor.poll(0);
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });
        _ = try reactor.poll(0);

        _ = std.c.write(pair[1], "X", 1);
        var saw = false;
        for (try reactor.poll(100)) |ev| {
            if (ev.fd == pair[0] and ev.readable) saw = true;
        }
        try std.testing.expect(saw);
        // The removed poll was really gone, so nothing fired with the old gen.
        try std.testing.expectEqual(@as(u64, 0), reactor.stale_events_dropped);
    } else return error.SkipZigTest;
}

test "Reactor: kqueue drops a stale event whose generation no longer matches" {
    // kqueue only; io_uring has its own real-ring stale-drop test.
    if (is_bsd) {
        var pair: [2]std.posix.fd_t = undefined;
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
        defer {
            _ = std.c.close(pair[0]);
            _ = std.c.close(pair[1]);
        }
        var reactor = try Reactor.init(std.testing.allocator);
        defer reactor.deinit();
        try reactor.addSource(.{ .fd = pair[0], .tag = .client_read, .interests = .{ .readable = true } });

        // The kevent was armed with the original gen; a reuse would give the live
        // source a newer one. Bump it to stand in for that, then fire a readable:
        // the event carries the old gen and must be dropped, not delivered.
        reactor.sources.getPtr(pair[0]).?.gen +%= 1;
        _ = std.c.write(pair[1], "X", 1);
        var saw = false;
        for (try reactor.poll(100)) |ev| {
            if (ev.fd == pair[0] and ev.readable) saw = true;
        }
        try std.testing.expect(!saw);
        try std.testing.expectEqual(@as(u64, 1), reactor.stale_events_dropped);
    } else return error.SkipZigTest;
}
