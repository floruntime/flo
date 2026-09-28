//! Mailbox — everything another shard may put in front of this one.
//!
//! A shard's mailbox holds two rings and a wake:
//!
//! - `inbox`: requests and one-way messages from other shards.
//! - `replies`: answers to requests this shard sent. Every request that will
//!   be answered reserves a slot here before it is sent (`ReplyPool`), so
//!   an answer fits as long as each request is answered once
//!   (`replies_dropped` counts any breach); the ring is drained before the
//!   inbox.
//! - `wake`: the idle flag, the wake pipe and the dirty flags. A producer
//!   that publishes a message or sets a flag writes the pipe only when the
//!   consumer said it was about to sleep.
//!
//! The mailbox is heap-allocated so its address is stable: peers hold
//! `*Mailbox`, and each ring points at the wake.

const std = @import("std");
const stdx = @import("stdx");
const Atomic = std.atomic.Value;
const inbox_mod = @import("inbox.zig");
const Inbox = inbox_mod.Inbox;

/// Wakes that carry no data: the target only needs to know that something
/// happened since it last looked, so repeats coalesce into one bit.
pub const Flag = enum(u5) {
    /// A stream was appended to: stream triggers poll again.
    stream_appended,
    /// An action run became available: parked workers try to claim.
    action_invoked,

    pub fn bit(self: Flag) u32 {
        return @as(u32, 1) << @intFromEnum(self);
    }
};

pub const Wake = struct {
    /// Read end is the consumer's reactor source; -1 until `init`.
    rd: std.posix.fd_t = -1,
    wr: std.posix.fd_t = -1,
    /// Set by the consumer before it sleeps; cleared when it wakes, or by
    /// the producer that wakes it. Each side stores then loads, all
    /// sequentially consistent (a ring's `commit_tail`, or `flags`, against
    /// `idle`), so either the consumer sees the work before sleeping or the
    /// producer sees `idle` and writes the pipe — never neither, on any
    /// CPU's memory ordering.
    idle: Atomic(bool) = Atomic(bool).init(false),
    /// Dirty flags (`Flag.bit`), set by producers and taken by the consumer.
    flags: Atomic(u32) = Atomic(u32).init(0),

    pub fn init(self: *Wake) !void {
        const fds = try stdx.io.pipe();
        errdefer {
            _ = std.c.close(fds[0]);
            _ = std.c.close(fds[1]);
        }
        try stdx.net.sysFcntlSetNonblocking(fds[0]);
        try stdx.net.sysFcntlSetNonblocking(fds[1]);
        self.rd = fds[0];
        self.wr = fds[1];
    }

    pub fn deinit(self: *Wake) void {
        if (self.rd >= 0) _ = std.c.close(self.rd);
        if (self.wr >= 0) _ = std.c.close(self.wr);
        self.rd = -1;
        self.wr = -1;
    }

    /// Producer, after publishing with a sequentially consistent store:
    /// only the producer that finds the consumer asleep writes, so a busy
    /// consumer costs a load, never a syscall.
    pub fn ring(self: *Wake) void {
        if (self.wr >= 0 and self.idle.load(.seq_cst) and self.idle.swap(false, .seq_cst)) {
            const byte = [_]u8{1};
            _ = std.c.write(self.wr, &byte, 1);
        }
    }

    /// Producer: mark `flag` dirty and wake the consumer if it sleeps.
    pub fn set(self: *Wake, flag: Flag) void {
        _ = self.flags.fetchOr(flag.bit(), .seq_cst);
        self.ring();
    }

    /// Consumer: the flags set since the last take.
    pub fn take(self: *Wake) u32 {
        if (self.flags.load(.monotonic) == 0) return 0;
        return self.flags.swap(0, .seq_cst);
    }

    /// Consumer, after the reactor returns: stop announcing the sleep and
    /// empty the pipe.
    pub fn woke(self: *Wake) void {
        self.idle.store(false, .seq_cst);
        if (self.rd < 0) return;
        var buf: [64]u8 = undefined;
        while (std.c.read(self.rd, &buf, buf.len) > 0) {}
    }
};

pub const Mailbox = struct {
    inbox: Inbox,
    replies: Inbox,
    wake: Wake = .{},

    /// Heap-allocate a mailbox with its wake pipe, rings pointing at it.
    pub fn create(allocator: std.mem.Allocator, inbox_capacity: usize, replies_capacity: usize) !*Mailbox {
        const self = try allocator.create(Mailbox);
        errdefer allocator.destroy(self);
        self.* = .{
            .inbox = try Inbox.init(allocator, inbox_capacity),
            .replies = undefined,
        };
        errdefer self.inbox.deinit();
        self.replies = try Inbox.init(allocator, replies_capacity);
        errdefer self.replies.deinit();
        try self.wake.init();
        self.inbox.wake = &self.wake;
        self.replies.wake = &self.wake;
        return self;
    }

    pub fn destroy(self: *Mailbox, allocator: std.mem.Allocator) void {
        self.inbox.deinit();
        self.replies.deinit();
        self.wake.deinit();
        allocator.destroy(self);
    }

    /// Consumer, before blocking: announce the sleep, then look again at
    /// both rings and the flags. False means something arrived in between —
    /// do not block.
    pub fn prepareSleep(self: *Mailbox) bool {
        self.wake.idle.store(true, .seq_cst);
        if (self.replies.pendingSeqCst() > 0 or self.inbox.pendingSeqCst() > 0 or self.wake.flags.load(.seq_cst) != 0) {
            self.wake.idle.store(false, .seq_cst);
            return false;
        }
        return true;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

fn msg(seq: u64) inbox_mod.Message {
    return .{ .tag = .forward_request, .sequence = seq };
}

fn readable(fd: std.posix.fd_t, timeout_ms: i32) bool {
    var fds = [_]std.posix.pollfd{.{ .fd = fd, .events = std.posix.POLL.IN, .revents = 0 }};
    const ready = std.posix.poll(&fds, timeout_ms) catch return false;
    return ready > 0;
}

test "Mailbox: a message, a reply or a flag wakes a consumer that said it would sleep, and only then" {
    const mb = try Mailbox.create(std.testing.allocator, 16, 16);
    defer mb.destroy(std.testing.allocator);
    var batch: [16]inbox_mod.Message = undefined;

    // Awake consumer: no syscall, no byte.
    try std.testing.expect(mb.inbox.send(msg(1)));
    try std.testing.expect(!readable(mb.wake.rd, 0));
    // Work already waiting in either ring, or a flag: the consumer must not sleep.
    try std.testing.expect(!mb.prepareSleep());
    _ = mb.inbox.drain(&batch);
    try std.testing.expect(mb.replies.send(msg(2)));
    try std.testing.expect(!mb.prepareSleep());
    _ = mb.replies.drain(&batch);
    mb.wake.set(.stream_appended);
    try std.testing.expect(!mb.prepareSleep());
    try std.testing.expectEqual(Flag.stream_appended.bit(), mb.wake.take());

    // Empty and about to sleep: whichever arrives writes the pipe, once.
    for (0..3) |kind| {
        try std.testing.expect(mb.prepareSleep());
        switch (kind) {
            0 => try std.testing.expect(mb.inbox.send(msg(3))),
            1 => try std.testing.expect(mb.replies.send(msg(4))),
            else => mb.wake.set(.action_invoked),
        }
        mb.wake.set(.stream_appended);
        try std.testing.expect(readable(mb.wake.rd, 0));
        var bytes: [8]u8 = undefined;
        try std.testing.expectEqual(@as(isize, 1), std.c.read(mb.wake.rd, &bytes, bytes.len));
        mb.wake.woke();
        try std.testing.expect(!readable(mb.wake.rd, 0));
        _ = mb.inbox.drain(&batch);
        _ = mb.replies.drain(&batch);
        _ = mb.wake.take();
    }

    // Woken by something else with nothing sent: awake, so the next send
    // costs no syscall.
    try std.testing.expect(mb.prepareSleep());
    mb.wake.woke();
    try std.testing.expect(mb.inbox.send(msg(5)));
    try std.testing.expect(!readable(mb.wake.rd, 0));
}

test "Mailbox: nothing sent while the consumer goes to sleep is slept through" {
    const mb = try Mailbox.create(std.testing.allocator, 1024, 1024);
    defer mb.destroy(std.testing.allocator);

    // One producer per way in: messages, replies and flags.
    const Producer = struct {
        fn run(m: *Mailbox, way: usize, n: usize) void {
            var i: usize = 0;
            while (i < n) {
                const sent = switch (way) {
                    0 => m.inbox.send(msg(i)),
                    1 => m.replies.send(msg(i)),
                    else => blk: {
                        // A flag is only taken once seen; wait for that
                        // before setting it again, so each counts once.
                        if (m.wake.flags.load(.seq_cst) != 0) break :blk false;
                        m.wake.set(.action_invoked);
                        break :blk true;
                    },
                };
                if (sent) i += 1;
                var spin: usize = i % 97;
                while (spin > 0) : (spin -= 1) std.atomic.spinLoopHint();
            }
        }
    };
    const N: usize = 5_000;
    var threads: [3]std.Thread = undefined;
    for (&threads, 0..) |*t, way| t.* = try std.Thread.spawn(.{}, Producer.run, .{ mb, way, N });
    var got: usize = 0;
    var batch: [64]inbox_mod.Message = undefined;
    while (got < 3 * N) {
        if (mb.prepareSleep()) {
            // Asleep with nothing seen: work that arrives now must write
            // the pipe. A lost wakeup would sleep the full second.
            const waiting = mb.inbox.pending() + mb.replies.pending() + @intFromBool(mb.wake.flags.load(.seq_cst) != 0);
            if (waiting > 0) try std.testing.expect(readable(mb.wake.rd, 1000));
            _ = readable(mb.wake.rd, 1);
        }
        mb.wake.woke();
        if (mb.wake.take() != 0) got += 1;
        while (true) {
            const n = mb.replies.drain(&batch) + mb.inbox.drain(&batch);
            if (n == 0) break;
            got += n;
        }
    }
    for (&threads) |*t| t.join();
    try std.testing.expectEqual(3 * N, got);
}
