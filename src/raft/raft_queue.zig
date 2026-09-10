//! The bounded queue between the network thread and a shard: frames from
//! authenticated peers, one owned copy each. It is separate from the
//! shard's client inbox so a flood of peer traffic cannot starve clients
//! or the other way round, and it is bounded so a slow shard pushes back
//! on the network (which stops reading its peers above the high watermark)
//! instead of growing without limit — bounded in bytes as well as frames,
//! since a frame may be 4 MiB. A push into an empty queue writes one
//! byte to a pipe the shard has registered with its reactor, so a frame is
//! seen as soon as it arrives rather than at the next poll timeout.

const std = @import("std");
const stdx = @import("stdx");
const MsgType = @import("transport.zig").MsgType;

pub const Frame = struct {
    source_node: u32,
    group_id: u32,
    msg_type: MsgType,
    /// Owned by the frame; the consumer frees it.
    payload: []u8,
};

/// Resident payload bytes: reading peers pauses above HIGH, resumes below
/// LOW; a push past MAX is refused.
pub const HIGH_BYTES: usize = 64 * 1024 * 1024;
pub const LOW_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BYTES: usize = 128 * 1024 * 1024;

pub const RaftQueue = struct {
    allocator: std.mem.Allocator,
    mutex: stdx.Mutex = .{},
    slots: []Frame,
    head: usize = 0,
    len: usize = 0,
    bytes: usize = 0,
    /// Read end, for the shard's reactor; write end, for the producer.
    wake_rd: std.posix.fd_t,
    wake_wr: std.posix.fd_t,
    /// Frames the producer could not queue; each is an entry this node
    /// never applied.
    dropped: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !RaftQueue {
        const slots = try allocator.alloc(Frame, capacity);
        errdefer allocator.free(slots);
        const fds = try stdx.io.pipe();
        errdefer {
            _ = std.c.close(fds[0]);
            _ = std.c.close(fds[1]);
        }
        try stdx.net.sysFcntlSetNonblocking(fds[0]);
        try stdx.net.sysFcntlSetNonblocking(fds[1]);
        return .{ .allocator = allocator, .slots = slots, .wake_rd = fds[0], .wake_wr = fds[1] };
    }

    pub fn deinit(self: *RaftQueue) void {
        while (self.pop()) |f| self.allocator.free(f.payload);
        self.allocator.free(self.slots);
        _ = std.c.close(self.wake_rd);
        _ = std.c.close(self.wake_wr);
    }

    /// Queue a frame the caller owns. False when full: the caller keeps
    /// ownership and frees it. The wake byte is written only when the queue
    /// was empty, so a busy shard is not woken once per frame.
    pub fn push(self: *RaftQueue, frame: Frame) bool {
        var was_empty = false;
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.len == self.slots.len or self.bytes + frame.payload.len > MAX_BYTES) {
                _ = self.dropped.fetchAdd(1, .monotonic);
                return false;
            }
            was_empty = self.len == 0;
            self.slots[(self.head + self.len) % self.slots.len] = frame;
            self.len += 1;
            self.bytes += frame.payload.len;
        }
        if (was_empty) {
            const byte = [_]u8{1};
            _ = std.c.write(self.wake_wr, &byte, 1);
        }
        return true;
    }

    pub fn pop(self: *RaftQueue) ?Frame {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.len == 0) return null;
        const f = self.slots[self.head];
        self.head = (self.head + 1) % self.slots.len;
        self.len -= 1;
        self.bytes -= f.payload.len;
        return f;
    }

    pub fn count(self: *RaftQueue) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.len;
    }

    /// Above this, by frames or by bytes, the network stops reading peers.
    pub fn aboveHigh(self: *RaftQueue) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.len >= self.slots.len * 3 / 4 or self.bytes >= HIGH_BYTES;
    }

    /// Below this, by both, it reads again.
    pub fn belowLow(self: *RaftQueue) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.len <= self.slots.len / 4 and self.bytes <= LOW_BYTES;
    }

    /// Empty the wake pipe; called by the consumer when the read end is
    /// readable, before draining.
    pub fn drainWake(self: *RaftQueue) void {
        var buf: [64]u8 = undefined;
        while (true) {
            const n = std.c.read(self.wake_rd, &buf, buf.len);
            if (n <= 0) return;
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn owned(payload: []const u8) !Frame {
    return .{ .source_node = 1, .group_id = 0, .msg_type = .replicate_entry, .payload = try testing.allocator.dupe(u8, payload) };
}

fn wakePending(fd: std.posix.fd_t) bool {
    var fds = [_]std.posix.pollfd{.{ .fd = fd, .events = std.posix.POLL.IN, .revents = 0 }};
    const ready = std.posix.poll(&fds, 0) catch return false;
    return ready > 0;
}

test "raft queue: bounded, ordered, one wake per idle-to-busy transition" {
    var q = try RaftQueue.init(testing.allocator, 4);
    defer q.deinit();

    try testing.expect(!wakePending(q.wake_rd));
    try testing.expect(q.push(try owned("a")));
    try testing.expect(wakePending(q.wake_rd));
    try testing.expect(q.push(try owned("b")));
    try testing.expect(q.push(try owned("c")));
    try testing.expect(q.aboveHigh());
    try testing.expect(q.push(try owned("d")));
    const extra = try owned("e");
    try testing.expect(!q.push(extra));
    testing.allocator.free(extra.payload);
    try testing.expectEqual(@as(u64, 1), q.dropped.load(.monotonic));

    q.drainWake();
    try testing.expect(!wakePending(q.wake_rd));
    const first = q.pop().?;
    try testing.expectEqualStrings("a", first.payload);
    testing.allocator.free(first.payload);
    // Still busy: popping does not need a wake, and pushing into a
    // non-empty queue writes none.
    try testing.expect(q.push(try owned("f")));
    try testing.expect(!wakePending(q.wake_rd));

    var seen: usize = 0;
    while (q.pop()) |f| {
        seen += 1;
        testing.allocator.free(f.payload);
    }
    try testing.expectEqual(@as(usize, 4), seen);
    try testing.expect(q.belowLow());
    // Empty again: the next push wakes.
    try testing.expect(q.push(try owned("g")));
    try testing.expect(wakePending(q.wake_rd));
}

test "raft queue: bytes bound the queue as well as frames" {
    var q = try RaftQueue.init(testing.allocator, 1024);
    defer q.deinit();
    const big = try testing.allocator.alloc(u8, HIGH_BYTES / 2);
    defer testing.allocator.free(big);
    var i: usize = 0;
    while (i < 2) : (i += 1) {
        try testing.expect(q.push(.{ .source_node = 1, .group_id = 0, .msg_type = .replicate_entry, .payload = try testing.allocator.dupe(u8, big) }));
    }
    // Two frames, but 64 MiB: above the high watermark by bytes alone.
    try testing.expect(q.aboveHigh());
    try testing.expect(!q.belowLow());
    // Past the hard cap a push is refused even with slots free, and the
    // refused copy stays the caller's to free.
    while (true) {
        const f = Frame{ .source_node = 1, .group_id = 0, .msg_type = .replicate_entry, .payload = try testing.allocator.dupe(u8, big) };
        if (!q.push(f)) {
            testing.allocator.free(f.payload);
            break;
        }
    }
    try testing.expect(q.dropped.load(.monotonic) >= 1);
    try testing.expect(q.bytes <= MAX_BYTES);
    while (q.pop()) |f| testing.allocator.free(f.payload);
    try testing.expect(q.belowLow());
}
