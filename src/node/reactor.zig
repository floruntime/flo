// Reactor — Unified event loop for a Shard.
//
// Each shard has exactly ONE Reactor that drives ALL async I/O:
//   - Network I/O (client TCP connections)
//   - Acceptor hand-off pipe (new connections)
//   - Raft peer sockets
//   - Inbox notification (cross-shard messages)
//   - Timer (Raft tick, TTL sweep, etc.)
//
// On macOS: kqueue
// On Linux: io_uring (future)
//
// See: NODE_NETWORK_DESIGN.md §5 — The Reactor

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;

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
    kq: i32, // kqueue file descriptor
    allocator: Allocator,

    // Map fd → EventSource metadata for dispatch
    sources: std.AutoHashMap(i32, EventSource),

    // Reusable event buffer
    event_buf: [MAX_EVENTS]posix.Kevent = undefined,

    // Internal result cache to hold converted events between poll calls
    result_cache: [MAX_EVENTS]Event = undefined,

    const Self = @This();

    pub fn init(allocator: Allocator) !Self {
        const kq = try posix.kqueue();
        return .{
            .kq = kq,
            .allocator = allocator,
            .sources = std.AutoHashMap(i32, EventSource).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.sources.deinit();
        posix.close(kq_fd(self.kq));
    }

    /// Register an event source (fd + interests).
    pub fn addSource(self: *Self, source: EventSource) !void {
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
            _ = try posix.kevent(self.kq, changelist[0..nchanges], &.{}, null);
        }

        try self.sources.put(source.fd, source);
    }

    /// Remove an fd from the reactor.
    pub fn removeSource(self: *Self, fd: i32) void {
        // Remove from kqueue (both filters)
        var changelist: [2]posix.Kevent = undefined;
        changelist[0] = makeKevent(fd, posix.system.EVFILT.READ, posix.system.EV.DELETE, 0);
        changelist[1] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.DELETE, 0);

        // Ignore errors — fd may already be closed
        _ = posix.kevent(self.kq, &changelist, &.{}, null) catch {};

        _ = self.sources.remove(fd);
    }

    /// Modify event interests for an existing fd.
    pub fn modifyInterests(self: *Self, fd: i32, interests: Interests) !void {
        if (self.sources.getPtr(fd)) |source| {
            var changelist: [2]posix.Kevent = undefined;
            var nchanges: usize = 0;

            // Toggle readable
            if (interests.readable and !source.interests.readable) {
                changelist[nchanges] = makeKevent(fd, posix.system.EVFILT.READ, posix.system.EV.ADD | posix.system.EV.ENABLE, source.user_data);
                nchanges += 1;
            } else if (!interests.readable and source.interests.readable) {
                changelist[nchanges] = makeKevent(fd, posix.system.EVFILT.READ, posix.system.EV.DELETE, 0);
                nchanges += 1;
            }

            // Toggle writable
            if (interests.writable and !source.interests.writable) {
                changelist[nchanges] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.ADD | posix.system.EV.ENABLE, source.user_data);
                nchanges += 1;
            } else if (!interests.writable and source.interests.writable) {
                changelist[nchanges] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.DELETE, 0);
                nchanges += 1;
            }

            if (nchanges > 0) {
                _ = try posix.kevent(self.kq, changelist[0..nchanges], &.{}, null);
            }

            source.interests = interests;
        }
    }

    /// Arm writable interest for a connection (commonly used after queueing response bytes).
    pub fn armWritable(self: *Self, fd: i32) !void {
        if (self.sources.getPtr(fd)) |source| {
            if (!source.interests.writable) {
                var changelist: [1]posix.Kevent = undefined;
                changelist[0] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.ADD | posix.system.EV.ENABLE, source.user_data);
                _ = try posix.kevent(self.kq, &changelist, &.{}, null);
                source.interests.writable = true;
            }
        }
    }

    /// Disarm writable interest (buffer drained, no more data to send).
    pub fn disarmWritable(self: *Self, fd: i32) !void {
        if (self.sources.getPtr(fd)) |source| {
            if (source.interests.writable) {
                var changelist: [1]posix.Kevent = undefined;
                changelist[0] = makeKevent(fd, posix.system.EVFILT.WRITE, posix.system.EV.DELETE, 0);
                _ = try posix.kevent(self.kq, &changelist, &.{}, null);
                source.interests.writable = false;
            }
        }
    }

    /// Add a one-shot or repeating timer.
    /// `ident` is a unique timer ID, `interval_ms` is the period.
    pub fn addTimer(self: *Self, ident: usize, interval_ms: u32) !void {
        var changelist: [1]posix.Kevent = undefined;
        changelist[0] = .{
            .ident = ident,
            .filter = posix.system.EVFILT.TIMER,
            .flags = posix.system.EV.ADD | posix.system.EV.ENABLE,
            .fflags = 0,
            .data = @intCast(interval_ms),
            .udata = ident,
        };
        _ = try posix.kevent(self.kq, &changelist, &.{}, null);
    }

    /// Poll for events. Returns slice of fired events.
    /// `timeout_ms`: 0 = non-blocking, null = block indefinitely
    pub fn poll(self: *Self, timeout_ms: ?u32) ![]const Event {
        const timeout: ?posix.timespec = if (timeout_ms) |ms| .{
            .sec = @intCast(ms / 1000),
            .nsec = @intCast((@as(u64, ms) % 1000) * 1_000_000),
        } else null;

        const nready = posix.kevent(
            self.kq,
            &.{}, // no changes
            &self.event_buf,
            if (timeout) |*t| t else null,
        ) catch |err| {
            if (err == error.Interrupted) return &.{};
            return err;
        };

        // Convert kqueue events to our Event type
        var result_buf: [MAX_EVENTS]Event = undefined;
        var count: usize = 0;

        for (self.event_buf[0..nready]) |kev| {
            const fd: i32 = @intCast(kev.ident);

            // Timer events
            if (kev.filter == posix.system.EVFILT.TIMER) {
                result_buf[count] = .{
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

            // Look up tag from registered sources
            const source = self.sources.get(fd);
            const tag = if (source) |s| s.tag else Tag.client_read;
            const udata = if (source) |s| s.user_data else 0;

            const is_err = (kev.flags & posix.system.EV.ERROR) != 0;
            const is_eof = (kev.flags & posix.system.EV.EOF) != 0;

            result_buf[count] = .{
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

        // Store in a field so the returned slice stays valid
        self.result_cache = result_buf;
        return self.result_cache[0..count];
    }

    // ── Helpers ──

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

    fn kq_fd(kq: i32) i32 {
        return kq;
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
