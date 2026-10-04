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
//! - `shares`: each other shard's share of the inbox for the client requests
//!   it forwards, and their cancels. A sender with its share unread waits (its client's
//!   connection left unread) and is woken when the consumer drains some, so
//!   no one shard's clients can fill another's inbox and crowd out the rest.
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
    /// A shard this one waits on drained some of this one's share of its
    /// inbox. It only wakes the shard: waiting connections are looked at
    /// every tick.
    share_returned,

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

/// Inbox room kept out before the shares are cut (`shareFor`).
pub const UNSHARED_ROOM: usize = 64;

pub const Mailbox = struct {
    inbox: Inbox,
    replies: Inbox,
    wake: Wake = .{},
    /// Per sending shard: its client requests unread in `inbox`, and
    /// whether it waits to hear that some were drained.
    shares: []Share,
    /// The most client requests one sender may have unread in `inbox`.
    share: u16,

    /// Each sender writes its own, and the consumer every one: a line each.
    pub const Share = struct {
        unread: Atomic(u16) align(std.atomic.cache_line) = Atomic(u16).init(0),
        wanted: Atomic(bool) = Atomic(bool).init(false),
    };

    /// Half the inbox beyond `UNSHARED_ROOM`, split among the shards that may
    /// send to it. The rest is room for the messages that are not paced
    /// (action starts, shutdown): an action start with no room fails its
    /// workflow step.
    fn shareFor(inbox_capacity: usize, shard_count: u16) u16 {
        const room = (inbox_capacity -| UNSHARED_ROOM) / 2;
        return @intCast(@max(1, room / @max(1, shard_count -| 1)));
    }

    /// Heap-allocate a mailbox with its wake pipe, rings pointing at it, for
    /// a node of `shard_count` shards.
    pub fn create(allocator: std.mem.Allocator, inbox_capacity: usize, replies_capacity: usize, shard_count: u16) !*Mailbox {
        const self = try allocator.create(Mailbox);
        errdefer allocator.destroy(self);
        self.* = .{
            .inbox = try Inbox.init(allocator, inbox_capacity),
            .replies = undefined,
            .shares = &.{},
            .share = shareFor(inbox_capacity, shard_count),
        };
        errdefer self.inbox.deinit();
        self.replies = try Inbox.init(allocator, replies_capacity);
        errdefer self.replies.deinit();
        self.shares = try allocator.alloc(Share, @max(1, shard_count));
        errdefer allocator.free(self.shares);
        @memset(self.shares, .{});
        try self.wake.init();
        self.inbox.wake = &self.wake;
        self.replies.wake = &self.wake;
        return self;
    }

    pub fn destroy(self: *Mailbox, allocator: std.mem.Allocator) void {
        self.inbox.deinit();
        self.replies.deinit();
        allocator.free(self.shares);
        self.wake.deinit();
        allocator.destroy(self);
    }

    /// Sender `from`: how many more of its client requests fit its share.
    pub fn shareRoom(self: *const Mailbox, from: u8) i32 {
        return @as(i32, self.share) - @as(i32, self.shares[from].unread.load(.seq_cst));
    }

    /// Sender `from`: room for one more of its client requests.
    pub fn hasShare(self: *const Mailbox, from: u8) bool {
        return self.shares[from].unread.load(.seq_cst) < self.share;
    }

    /// Sender: put a client request in the inbox on its sender's share.
    /// False when the share is spent or the ring is full: nothing was sent.
    pub fn sendOnShare(self: *Mailbox, m: inbox_mod.Message) bool {
        const s = &self.shares[m.src_shard];
        // Only its sender raises a count, so none passes the share.
        if (s.unread.load(.seq_cst) >= self.share) return false;
        _ = s.unread.fetchAdd(1, .seq_cst);
        var shared = m;
        shared.setOnShare();
        if (self.inbox.send(shared)) return true;
        _ = s.unread.fetchSub(1, .seq_cst);
        return false;
    }

    /// Sender `from`, its share spent: ask to be told when some is drained,
    /// then look again (`hasShare`). Each side stores then loads, both
    /// sequentially consistent, so either the sender sees the room or the
    /// consumer sees the ask.
    pub fn wantShare(self: *Mailbox, from: u8) void {
        self.shares[from].wanted.store(true, .seq_cst);
    }

    /// Consumer, for each message it drains: give back its share. True when
    /// its sender asked to be told (`wantShare`): the caller wakes it.
    pub fn returnShare(self: *Mailbox, m: inbox_mod.Message) bool {
        if (!m.onShare()) return false;
        const s = &self.shares[m.src_shard];
        _ = s.unread.fetchSub(1, .seq_cst);
        return s.wanted.load(.seq_cst) and s.wanted.swap(false, .seq_cst);
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
    const mb = try Mailbox.create(std.testing.allocator, 16, 16, 1);
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
    const mb = try Mailbox.create(std.testing.allocator, 1024, 1024, 1);
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

test "Mailbox: a sender has only its share of the inbox, gets it back as the consumer drains, and is told when it asked" {
    const mb = try Mailbox.create(std.testing.allocator, 1024, 16, 4);
    defer mb.destroy(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, (1024 - UNSHARED_ROOM) / 2 / 3), mb.share);
    var m = msg(1);
    m.src_shard = 2;
    for (0..mb.share) |_| try std.testing.expect(mb.sendOnShare(m));
    try std.testing.expect(!mb.hasShare(2));
    try std.testing.expect(!mb.sendOnShare(m));
    // Another sender still has all of its own.
    var other = msg(2);
    other.src_shard = 3;
    try std.testing.expect(mb.sendOnShare(other));
    try std.testing.expectEqual(@as(usize, mb.share) + 1, mb.inbox.pending());

    // Drained without being asked: the share comes back, nobody is told.
    var batch: [1]inbox_mod.Message = undefined;
    try std.testing.expectEqual(@as(usize, 1), mb.inbox.drain(&batch));
    try std.testing.expect(!mb.returnShare(batch[0]));
    try std.testing.expect(mb.hasShare(2));
    try std.testing.expect(mb.sendOnShare(m));

    // Asked: the next drain of its messages says so, once.
    mb.wantShare(2);
    try std.testing.expectEqual(@as(usize, 1), mb.inbox.drain(&batch));
    try std.testing.expect(mb.returnShare(batch[0]));
    try std.testing.expectEqual(@as(usize, 1), mb.inbox.drain(&batch));
    try std.testing.expect(!mb.returnShare(batch[0]));
    // A message not sent on a share gives nothing back.
    try std.testing.expect(mb.inbox.send(msg(3)));
    try std.testing.expectEqual(@as(u16, mb.share - 2), mb.shares[2].unread.load(.seq_cst));
    var all: [1024]inbox_mod.Message = undefined;
    const n = mb.inbox.drain(&all);
    for (all[0..n]) |d| _ = mb.returnShare(d);
    try std.testing.expectEqual(@as(u16, 0), mb.shares[2].unread.load(.seq_cst));
    try std.testing.expectEqual(@as(u16, 0), mb.shares[3].unread.load(.seq_cst));
    try std.testing.expectEqual(@as(u16, 0), mb.shares[0].unread.load(.seq_cst));
}
