//! The node's bulk I/O thread: reads of sealed log segments that would
//! otherwise stall a shard's thread, such as a leader replicating to a peer
//! that has fallen below its ring. A shard hands it a read it has resolved to
//! one sealed segment, and is woken when the result is in.
//!
//! The thread never touches a shard's durable log: it opens the segment file
//! itself, through a one-segment cache of its own. A truncation may rewrite or
//! delete that file under it; the rename leaves an open file reading the old
//! contents whole, and the shard drops a result whose truncation generation
//! has moved, so a read is never torn, only stale.
//!
//! Three lanes: catch-up (a leader's reads for its peers), stream (payloads
//! a stream reader waits on), and background (work nobody waits on, such as
//! snapshots). With several waiting they take turns, so none is starved, and
//! a burst of stream reads never holds a peer's catch-up behind it.

const std = @import("std");
const stdx = @import("stdx");
const entry_mod = @import("ual/entry.zig");
const SegmentReader = @import("ual/reader.zig").SegmentReader;
const durable_log = @import("durable_log.zig");
const CatchUpResult = @import("../raft/log.zig").CatchUpResult;

const log = std.log.scoped(.bulk_io);

pub const MAX_PATH = 512;

pub const Lane = enum { foreground, stream, background };

/// Tells the asker its read is done. Called on the bulk thread.
pub const Notify = struct {
    ctx: *anyopaque,
    done: *const fn (ctx: *anyopaque) void,
};

/// One read of a sealed segment, owned by the shard that asks. While it is
/// `queued` the bulk thread owns its result fields and nothing else touches
/// them; `done` hands them back.
pub const SealedRead = struct {
    state: std.atomic.Value(State) = .init(.idle),
    path: [MAX_PATH]u8 = undefined,
    path_len: usize = 0,
    start: u64 = 0,
    /// The durable log's truncation count when the read was asked for.
    generation: u64 = 0,
    entries: []entry_mod.Entry = &.{},
    arena: []u8 = &.{},
    result: CatchUpResult = .{},
    notify: ?Notify = null,

    pub const State = enum(u8) { idle, queued, done };

    pub fn pathSlice(self: *const SealedRead) []const u8 {
        return self.path[0..self.path_len];
    }
};

/// The most reads waiting in one lane, across the node's shards. Past it a
/// shard reads in place.
pub const QUEUE_CAPACITY = 256;

const Queue = struct {
    items: [QUEUE_CAPACITY]*SealedRead = undefined,
    head: usize = 0,
    len: usize = 0,

    fn push(self: *Queue, r: *SealedRead) bool {
        if (self.len == QUEUE_CAPACITY) return false;
        self.items[(self.head + self.len) % QUEUE_CAPACITY] = r;
        self.len += 1;
        return true;
    }

    fn pop(self: *Queue) ?*SealedRead {
        if (self.len == 0) return null;
        const r = self.items[self.head];
        self.head = (self.head + 1) % QUEUE_CAPACITY;
        self.len -= 1;
        return r;
    }
};

pub const BulkIo = struct {
    allocator: std.mem.Allocator,
    mutex: stdx.Mutex = .{},
    lanes: [3]Queue = .{ .{}, .{}, .{} },
    /// The lane that goes first when both have work; flips after each read.
    turn: Lane = .foreground,
    running: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,
    wake_rd: std.posix.fd_t = -1,
    wake_wr: std.posix.fd_t = -1,
    /// The thread's own one-segment cache, kept for one truncation
    /// generation: a rewrite reuses the file's name.
    cached: ?Cached = null,

    const Cached = struct {
        path: [MAX_PATH]u8,
        path_len: usize,
        generation: u64,
        buf: []u8,
        reader: SegmentReader,
    };

    pub fn init(allocator: std.mem.Allocator) !BulkIo {
        const fds = try stdx.io.pipe();
        return .{ .allocator = allocator, .wake_rd = fds[0], .wake_wr = fds[1] };
    }

    pub fn start(self: *BulkIo) !void {
        self.running.store(true, .release);
        self.thread = try std.Thread.spawn(.{}, loop, .{self});
    }

    /// Finish the read in hand and stop. Reads still queued are given back
    /// idle, unread: their askers have stopped.
    pub fn stop(self: *BulkIo) void {
        if (self.running.swap(false, .acq_rel)) {
            self.wake();
            if (self.thread) |t| t.join();
            self.thread = null;
        }
        self.mutex.lock();
        defer self.mutex.unlock();
        for (&self.lanes) |*q| while (q.pop()) |r| r.state.store(.idle, .release);
    }

    pub fn deinit(self: *BulkIo) void {
        self.stop();
        self.dropCache();
        if (self.wake_rd >= 0) _ = std.c.close(self.wake_rd);
        if (self.wake_wr >= 0) _ = std.c.close(self.wake_wr);
    }

    /// Queue `r`, already filled in and `queued`. False when the lane is
    /// full; the caller then reads in place.
    pub fn submit(self: *BulkIo, r: *SealedRead, lane: Lane) bool {
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (!self.lanes[@backingInt(lane)].push(r)) return false;
        }
        self.wake();
        return true;
    }

    fn wake(self: *BulkIo) void {
        const b = [1]u8{1};
        _ = std.c.write(self.wake_wr, &b, 1);
    }

    /// The next read: the turn's lane first, then the others in order.
    fn next(self: *BulkIo) ?*SealedRead {
        self.mutex.lock();
        defer self.mutex.unlock();
        const lanes = self.lanes.len;
        const first: usize = @backingInt(self.turn);
        for (0..lanes) |k| {
            const at = (first + k) % lanes;
            const r = self.lanes[at].pop() orelse continue;
            self.turn = @fromBackingInt(@as(u2, @intCast((at + 1) % lanes)));
            return r;
        }
        return null;
    }

    fn loop(self: *BulkIo) void {
        while (self.running.load(.acquire)) {
            if (self.next()) |r| {
                self.perform(r);
                continue;
            }
            var fds = [1]std.posix.pollfd{.{ .fd = self.wake_rd, .events = std.posix.POLL.IN, .revents = 0 }};
            _ = std.posix.poll(&fds, -1) catch {};
            var drain: [64]u8 = undefined;
            _ = std.c.read(self.wake_rd, &drain, drain.len);
        }
    }

    fn perform(self: *BulkIo, r: *SealedRead) void {
        r.result = self.read(r);
        // Once `done`, the shard may reuse the read: nothing of it is read
        // after the store.
        const notify = r.notify;
        r.state.store(.done, .release);
        if (notify) |n| n.done(n.ctx);
    }

    /// The read itself: contiguous entries from `r.start` within its one
    /// segment. A failure to open or read it is classified as the shard's own
    /// read would classify it.
    fn read(self: *BulkIo, r: *SealedRead) CatchUpResult {
        const opened = self.open(r.pathSlice(), r.generation) catch |err| {
            return switch (durable_log.DurableLog.readBlockClass(err)) {
                .resource => .{ .blocked = true },
                .disk => .{ .blocked = true, .disk_failing = true, .permission = durable_log.DurableLog.isPermission(err) },
                // Gone, as a truncation deletes a file wholly above its cut:
                // the shard's generation check drops it before reading this.
                .fault => .{},
            };
        };
        const offset = opened.reader.findOffset(r.start) orelse return .{};
        const data = opened.buf[opened.reader.data_start + offset .. opened.reader.data_end];
        return .{ .count = durable_log.copyFrom(data, r.start, r.entries, r.arena) };
    }

    fn open(self: *BulkIo, path: []const u8, generation: u64) !*const Cached {
        if (self.cached) |*c| {
            if (c.generation == generation and std.mem.eql(u8, c.path[0..c.path_len], path)) return c;
        }
        const opened = try SegmentReader.initFromFile(self.allocator, path);
        self.dropCache();
        var c: Cached = .{ .path = undefined, .path_len = path.len, .generation = generation, .buf = opened.buf, .reader = opened.reader };
        @memcpy(c.path[0..path.len], path);
        self.cached = c;
        return &self.cached.?;
    }

    fn dropCache(self: *BulkIo) void {
        if (self.cached) |c| self.allocator.free(c.buf);
        self.cached = null;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "bulk io: with both lanes waiting they take turns, and either runs alone" {
    var b = try BulkIo.init(testing.allocator);
    defer b.deinit();
    var reads: [4]SealedRead = .{ .{}, .{}, .{}, .{} };
    // Not started: nothing is taken but by `next`.
    try testing.expect(b.submit(&reads[0], .foreground));
    try testing.expect(b.submit(&reads[1], .foreground));
    try testing.expect(b.submit(&reads[2], .background));
    try testing.expect(b.submit(&reads[3], .background));
    try testing.expectEqual(&reads[0], b.next().?);
    try testing.expectEqual(&reads[2], b.next().?);
    try testing.expectEqual(&reads[1], b.next().?);
    try testing.expectEqual(&reads[3], b.next().?);
    try testing.expect(b.next() == null);
    try testing.expect(b.submit(&reads[0], .background));
    try testing.expectEqual(&reads[0], b.next().?);
}
