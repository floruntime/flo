//! Stream payloads read back from the shard's durable log: a record whose
//! append has left the hot ring (the warm store only covers an entry that
//! wraps the ring's end, or history on a ring under its budget) is read from
//! its sealed segment, or from the segment writer's buffer when a burst has
//! outrun the flush.
//!
//! A reader asks with its stream and window (`want`), and is answered
//! `pending`. The segment is read on the node's bulk I/O thread, never on the
//! shard's own; one still in the writer's buffer is read in place, with no
//! I/O. Either way the result lands in `drain`, in the shard's tick between
//! requests, so nothing is added to or evicted from the cache while a
//! request holds a payload from it. A landing caches every entry of the
//! asking streams within their windows, and nothing else, so one read serves
//! a whole window and other streams' entries don't flood the cache.
//!
//! An entry landed for a reader is pinned until it has been served or
//! `PIN_MS` passes, so later landings don't evict it before the reader that
//! asked is made again. Each is checked against the log's terms first.

const std = @import("std");
const stdx = @import("stdx");
const entry_mod = @import("../storage/ual/entry.zig");
const DurableLog = @import("../storage/durable_log.zig").DurableLog;
const bulk_io = @import("../storage/bulk_io.zig");
const RaftLog = @import("../raft/log.zig").RaftLog;
const router = @import("../node/router.zig");

const Entry = entry_mod.Entry;
const log = std.log.scoped(.stream_disk);

/// Reads in flight at once per shard.
pub const SLOTS = 8;
/// Entries and payload bytes one read brings back: a run of the segment
/// from the index asked for.
pub const READ_ENTRIES = 256;
pub const READ_BYTES = 1024 * 1024;
pub const DEFAULT_CACHE_BYTES = 8 * 1024 * 1024;
/// Pinned entries may take the cache past its budget up to this many times
/// it; past that a landing caches no more, and its readers ask again once
/// pins are released.
pub const PINNED_BUDGET_TIMES = 4;
/// How long a landed entry is kept for the reader that asked, if unserved.
pub const PIN_MS: u64 = 5_000;
/// After a blocked read or a term mismatch, how long reads are answered
/// blocked at once rather than read again.
pub const BACKOFF_MS: u64 = 250;
/// Windows one read lands for.
pub const MAX_WANTS = 16;

pub const Want = enum {
    /// Being read: hold the read, and make it again once woken.
    pending,
    /// Not in the durable log (below its first segment): the reader skips it.
    absent,
    /// Can't be read right now (a resource or IO failure, or a result that
    /// didn't match the log): the read is answered retryable at once.
    blocked,
};

/// What a reader wants landed: its stream, from an index, at most `limit`
/// of that stream's appends.
pub const Window = struct {
    name_hash: u64,
    from: u64,
    limit: u32,
};

const Cached = struct {
    payload: []u8,
    /// Zero once served, or never pinned.
    pinned_until_ms: u64,
};

const Slot = struct {
    read: bulk_io.SealedRead = .{},
    wants: [MAX_WANTS]Window = undefined,
    n_wants: usize = 0,

    fn add(self: *Slot, w: Window) void {
        for (self.wants[0..self.n_wants]) |*have| {
            if (have.name_hash == w.name_hash and have.from <= w.from) {
                have.limit = @max(have.limit, w.limit);
                return;
            }
        }
        if (self.n_wants == MAX_WANTS) return;
        self.wants[self.n_wants] = w;
        self.n_wants += 1;
    }
};

pub const DiskReads = struct {
    allocator: std.mem.Allocator,
    dl: *DurableLog,
    raft_log: *const RaftLog,
    bulk: ?*bulk_io.BulkIo = null,
    notify: ?bulk_io.Notify = null,
    slots: [SLOTS]Slot = @splat(.{}),
    entry_bufs: [SLOTS][]Entry = @splat(&.{}),
    arenas: [SLOTS][]u8 = @splat(&.{}),
    /// Windows to read in place at the next drain: entries still in the
    /// segment writer's buffer, or every read when there is no bulk thread.
    local: Slot = .{},

    cache: std.AutoHashMapUnmanaged(u64, Cached) = .empty,
    /// Indices in the order they were cached, oldest first, for eviction.
    order: std.ArrayListUnmanaged(u64) = .empty,
    order_head: usize = 0,
    cache_bytes: usize = 0,
    cache_budget: usize = DEFAULT_CACHE_BYTES,

    blocked_until_ms: u64 = 0,
    /// Reads that came back blocked, results dropped for not matching the
    /// log, and entries a full cache couldn't take.
    blocked_reads: u64 = 0,
    mismatches: u64 = 0,
    over_budget: u64 = 0,
    /// Bulk reads made, for the reads-per-record figure.
    reads_made: u64 = 0,
    warned_ms: u64 = 0,
    /// Test seam: the clock.
    now_fn: *const fn () u64 = stdx.time.monotonicMs,

    pub fn init(allocator: std.mem.Allocator, dl: *DurableLog, raft_log: *const RaftLog) DiskReads {
        return .{ .allocator = allocator, .dl = dl, .raft_log = raft_log };
    }

    /// After the bulk thread has stopped: it may write into a queued read's
    /// buffers until then.
    pub fn deinit(self: *DiskReads) void {
        for (self.entry_bufs, self.arenas) |e, a| {
            if (e.len > 0) self.allocator.free(e);
            if (a.len > 0) self.allocator.free(a);
        }
        var it = self.cache.valueIterator();
        while (it.next()) |v| self.allocator.free(v.payload);
        self.cache.deinit(self.allocator);
        self.order.deinit(self.allocator);
    }

    /// The raw entry payload at `index`, as the warm store keeps it, and
    /// served: no longer pinned. Valid until the next `drain`.
    pub fn get(self: *DiskReads, index: u64) ?[]const u8 {
        const c = self.cache.getPtr(index) orelse return null;
        c.pinned_until_ms = 0;
        return c.payload;
    }

    /// Ask for the entry at `index`, for a reader of window `w`. Never lands
    /// or evicts anything: a payload `get` returned stays valid.
    pub fn want(self: *DiskReads, index: u64, w: Window) Want {
        const now = self.now_fn();
        if (now < self.blocked_until_ms) return .blocked;
        for (&self.slots) |*s| {
            if (s.read.state.load(.acquire) == .idle) continue;
            if (index >= s.read.start and index < s.read.start + s.read.entries.len) {
                s.add(w);
                return .pending;
            }
        }
        var path_buf: [bulk_io.MAX_PATH]u8 = undefined;
        const found = self.dl.sealedSource(index, &path_buf) catch return self.blocked(now);
        const src = found orelse {
            // Not sealed: in the writer's buffer, read in place at the next
            // drain, or not in the durable log at all.
            const wr = self.dl.writer;
            if (wr.entry_count == 0 or index < wr.first_index or index > wr.last_index) return .absent;
            return self.inPlace(.{ .name_hash = w.name_hash, .from = index, .limit = w.limit });
        };
        const bulk = self.bulk orelse return self.inPlace(.{ .name_hash = w.name_hash, .from = index, .limit = w.limit });
        const i = self.freeSlot() orelse return .pending;
        if (!self.ensureBuffers(i)) return self.blocked(now);
        const slot = &self.slots[i];
        const n: usize = @intCast(@min(src.limit, READ_ENTRIES));
        slot.n_wants = 0;
        slot.add(.{ .name_hash = w.name_hash, .from = index, .limit = w.limit });
        slot.read = .{
            .path_len = src.path.len,
            .start = index,
            // Keys the bulk thread's file cache: a cut rewrites a file under
            // its own name. Only committed entries are asked for, which no
            // cut reaches; the term check below covers the rest.
            .generation = self.dl.truncations,
            .entries = self.entry_bufs[i][0..n],
            .arena = self.arenas[i],
            .notify = self.notify,
        };
        @memcpy(slot.read.path[0..src.path.len], src.path);
        slot.read.state.store(.queued, .release);
        if (!bulk.submit(&slot.read, .stream)) {
            slot.read.state.store(.idle, .release);
            return self.blocked(now);
        }
        self.reads_made += 1;
        return .pending;
    }

    fn inPlace(self: *DiskReads, w: Window) Want {
        self.local.add(w);
        if (self.notify) |n| n.done(n.ctx);
        return .pending;
    }

    fn blocked(self: *DiskReads, now: u64) Want {
        self.blocked_until_ms = now + BACKOFF_MS;
        return .blocked;
    }

    /// Land every finished read, and make the in-place ones. True when any
    /// landed or came back, so held reads are made again. Called between
    /// requests: it may evict.
    pub fn drain(self: *DiskReads) bool {
        var any = false;
        for (&self.slots) |*s| {
            if (s.read.state.load(.acquire) != .done) continue;
            const r = s.read.result;
            self.land(s.read.entries[0..r.count], r.blocked, r.disk_failing, s.wants[0..s.n_wants]);
            s.n_wants = 0;
            s.read.state.store(.idle, .release);
            any = true;
        }
        if (self.local.n_wants > 0) {
            const wants = self.local.wants;
            const n = self.local.n_wants;
            self.local.n_wants = 0;
            for (wants[0..n]) |w| self.readInPlace(w);
            any = true;
        }
        return any;
    }

    fn readInPlace(self: *DiskReads, w: Window) void {
        var buf: [READ_ENTRIES]Entry = undefined;
        const arena = self.allocator.alloc(u8, READ_BYTES) catch {
            _ = self.blocked(self.now_fn());
            return;
        };
        defer self.allocator.free(arena);
        const r = self.dl.readRange(w.from, &buf, arena);
        self.land(buf[0..r.count], r.blocked, r.disk_failing, &.{w});
    }

    fn land(self: *DiskReads, entries: []const Entry, blocked_read: bool, disk_failing: bool, wants: []const Window) void {
        const now = self.now_fn();
        if (blocked_read) {
            self.blocked_reads += 1;
            _ = self.blocked(now);
            if (disk_failing) self.warn(now, "a stream read from the durable log failed; the disk may be failing", .{});
            return;
        }
        var taken: [MAX_WANTS]u32 = @splat(0);
        for (entries) |*e| {
            if (e.header.entry_type != @backingInt(entry_mod.EntryType.stream_append)) continue;
            const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse continue;
            const name_hash = router.nameHash(cmd.namespace_hash, cmd.key);
            var wanted = false;
            for (wants, 0..) |w, k| {
                if (w.name_hash != name_hash or e.header.index < w.from or taken[k] >= w.limit) continue;
                taken[k] += 1;
                wanted = true;
            }
            if (!wanted) continue;
            if (self.raft_log.entryTerm(e.header.index) != e.header.term) {
                self.mismatches += 1;
                _ = self.blocked(now);
                self.warn(now, "a stream entry read from the durable log at index {d} doesn't match the log's term; holding and reading it again", .{e.header.index});
                continue;
            }
            self.put(e, now);
        }
        // Released pins and served entries make room for the next.
        self.evict(now, 0);
    }

    fn warn(self: *DiskReads, now: u64, comptime fmt: []const u8, args: anytype) void {
        if (self.warned_ms != 0 and now -| self.warned_ms < 10_000) return;
        self.warned_ms = now;
        log.warn(fmt, args);
    }

    /// One entry into the cache, pinned for its reader.
    fn put(self: *DiskReads, e: *const Entry, now: u64) void {
        const index = e.header.index;
        if (self.cache.getPtr(index)) |c| {
            c.pinned_until_ms = now + PIN_MS;
            return;
        }
        self.evict(now, e.payload.len);
        if (self.cache_bytes + e.payload.len > self.cache_budget * PINNED_BUDGET_TIMES) {
            self.over_budget += 1;
            return;
        }
        const copy = self.allocator.dupe(u8, e.payload) catch return;
        self.cache.put(self.allocator, index, .{ .payload = copy, .pinned_until_ms = now + PIN_MS }) catch {
            self.allocator.free(copy);
            return;
        };
        self.order.append(self.allocator, index) catch {};
        self.cache_bytes += copy.len;
    }

    /// Room for `adding` bytes within the budget, oldest unpinned first.
    fn evict(self: *DiskReads, now: u64, adding: usize) void {
        var at = self.order_head;
        while (self.cache_bytes + adding > self.cache_budget and at < self.order.items.len) : (at += 1) {
            const index = self.order.items[at];
            const c = self.cache.get(index) orelse continue;
            if (c.pinned_until_ms > now) continue;
            _ = self.cache.remove(index);
            self.cache_bytes -= c.payload.len;
            self.allocator.free(c.payload);
        }
        // The gone ones off the front of the order list.
        while (self.order_head < self.order.items.len and !self.cache.contains(self.order.items[self.order_head])) self.order_head += 1;
        if (self.order_head > 4096 and self.order_head * 2 > self.order.items.len) {
            const rest = self.order.items[self.order_head..];
            std.mem.copyForwards(u64, self.order.items[0..rest.len], rest);
            self.order.shrinkRetainingCapacity(rest.len);
            self.order_head = 0;
        }
    }

    fn freeSlot(self: *DiskReads) ?usize {
        for (&self.slots, 0..) |*s, i| if (s.read.state.load(.acquire) == .idle) return i;
        return null;
    }

    fn ensureBuffers(self: *DiskReads, i: usize) bool {
        if (self.entry_bufs[i].len == 0) {
            self.entry_bufs[i] = self.allocator.alloc(Entry, READ_ENTRIES) catch return false;
        }
        if (self.arenas[i].len == 0) {
            self.arenas[i] = self.allocator.alloc(u8, READ_BYTES) catch return false;
        }
        return true;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;
const SegmentWriter = @import("../storage/ual/writer.zig").SegmentWriter;

/// A stream append to `stream` in the default namespace.
fn appendEntry(buf: []u8, index: u64, term: u64, stream: []const u8, value: []const u8) Entry {
    const cmd: entry_mod.CommandPayload = .{ .namespace_hash = router.namespaceHash(""), .key_length = @intCast(stream.len), .value_length = @intCast(value.len), .key = stream, .value = value };
    const n = cmd.serialize(buf).?;
    var e = entry_mod.buildEntry(.stream_append, 0, term, index, index * 1000, buf[0..n]);
    e.header.crc32c = e.computeCrc();
    return e;
}

fn hashOf(stream: []const u8) u64 {
    return router.nameHash(router.namespaceHash(""), stream);
}

/// Entries 1-6, alternating streams "a" and "b" (a at odd indices), sealed
/// in one segment, or left in the writer's buffer; and a Raft log holding
/// them, at `term_of_3` for index 3.
const Fixture = struct {
    tmp: testing.TmpDir,
    path: []const u8,
    writer: SegmentWriter,
    dl: DurableLog,
    raft_log: RaftLog,

    fn init(self: *Fixture, term_of_3: u64, sealed: bool) !void {
        self.tmp = testing.tmpDir(.{});
        const real = try stdx.fs.dirRealpathAlloc(self.tmp.dir, testing.allocator, ".");
        defer testing.allocator.free(real);
        self.path = try std.fmt.allocPrint(testing.allocator, "{s}/segs", .{real});
        try stdx.fs.makePath(self.path);
        self.writer = SegmentWriter.init(testing.allocator, 0);
        self.dl = try DurableLog.init(testing.allocator, &self.writer, self.path);
        self.raft_log = try RaftLog.init(testing.allocator, 4096);
        for (1..7) |i| {
            var buf: [64]u8 = undefined;
            const stream: []const u8 = if (i % 2 == 1) "a" else "b";
            const e = appendEntry(&buf, i, 1, stream, "value");
            try self.writer.addEntry(&e);
            var mine_buf: [64]u8 = undefined;
            var mine = appendEntry(&mine_buf, i, if (i == 3) term_of_3 else 1, stream, "value");
            _ = try self.raft_log.append(&mine);
        }
        if (sealed) try self.dl.flush(6);
    }

    fn deinit(self: *Fixture) void {
        self.raft_log.deinit();
        self.dl.deinit();
        self.writer.deinit();
        testing.allocator.free(self.path);
        self.tmp.cleanup();
    }
};

const Done = struct {
    count: std.atomic.Value(u32) = .init(0),
    fn done(ctx: *anyopaque) void {
        const self: *Done = @ptrCast(@alignCast(ctx));
        _ = self.count.fetchAdd(1, .release);
    }
    fn waitFor(self: *Done, n: u32) !void {
        var tries: usize = 0;
        while (self.count.load(.acquire) < n) : (tries += 1) {
            if (tries == 2000) return error.NeverDone;
            stdx.time.sleep(std.time.ns_per_ms);
        }
    }
};

const Clock = struct {
    var now: u64 = 1_000;
    fn get() u64 {
        return now;
    }
};

test "stream disk reads: one read lands a reader's whole window, its stream's entries only, and only at drain" {
    var f: Fixture = undefined;
    try f.init(1, true);
    defer f.deinit();
    var d = DiskReads.init(testing.allocator, &f.dl, &f.raft_log);
    defer d.deinit();
    var bulk = try bulk_io.BulkIo.init(testing.allocator);
    defer bulk.deinit();
    try bulk.start();
    var done: Done = .{};
    d.bulk = &bulk;
    d.notify = .{ .ctx = &done, .done = Done.done };

    const a: Window = .{ .name_hash = hashOf("a"), .from = 1, .limit = 10 };
    try testing.expectEqual(Want.pending, d.want(1, a));
    // Asked again inside the read in flight: no second read.
    try testing.expectEqual(Want.pending, d.want(3, a));
    try done.waitFor(1);
    // Back, but not landed until drain: nothing changes mid-request.
    try testing.expect(d.get(1) == null);
    try testing.expect(d.drain());
    try testing.expect(d.get(1) != null and d.get(3) != null and d.get(5) != null);
    // The other stream's entries were in the run, and weren't cached.
    try testing.expect(d.get(2) == null and d.get(4) == null);
    try testing.expectEqual(@as(u64, 1), d.reads_made);
}

test "stream disk reads: entries still in the writer's buffer are read in place at drain" {
    var f: Fixture = undefined;
    try f.init(1, false);
    defer f.deinit();
    var d = DiskReads.init(testing.allocator, &f.dl, &f.raft_log);
    defer d.deinit();
    const a: Window = .{ .name_hash = hashOf("a"), .from = 1, .limit = 10 };
    try testing.expectEqual(Want.pending, d.want(1, a));
    try testing.expect(d.get(1) == null);
    try testing.expect(d.drain());
    try testing.expect(d.get(1) != null and d.get(5) != null);
    try testing.expectEqual(@as(u64, 0), d.reads_made);
    // Past the writer and every segment: absent.
    try testing.expectEqual(Want.absent, d.want(9, a));
}

test "stream disk reads: an entry whose term isn't the log's is not cached, and reads back off" {
    Clock.now = 1_000;
    var f: Fixture = undefined;
    try f.init(4, true);
    defer f.deinit();
    var d = DiskReads.init(testing.allocator, &f.dl, &f.raft_log);
    defer d.deinit();
    d.now_fn = Clock.get;
    const a: Window = .{ .name_hash = hashOf("a"), .from = 1, .limit = 10 };
    try testing.expectEqual(Want.pending, d.want(1, a));
    _ = d.drain();
    try testing.expect(d.get(3) == null);
    try testing.expectEqual(@as(u64, 1), d.mismatches);
    // Answered blocked at once while backing off, not read again.
    try testing.expectEqual(Want.blocked, d.want(3, a));
    Clock.now += BACKOFF_MS;
    try testing.expectEqual(Want.pending, d.want(3, a));
}

test "stream disk reads: a landed entry stays pinned for its reader until served, then may be evicted" {
    Clock.now = 1_000;
    var f: Fixture = undefined;
    try f.init(1, true);
    defer f.deinit();
    var d = DiskReads.init(testing.allocator, &f.dl, &f.raft_log);
    defer d.deinit();
    d.now_fn = Clock.get;
    // Each entry is 16 bytes: a's three are past the budget, under its cap.
    d.cache_budget = 40;
    try testing.expectEqual(Want.pending, d.want(1, .{ .name_hash = hashOf("a"), .from = 1, .limit = 10 }));
    _ = d.drain();
    // Over its budget, but pinned: all three of a's kept.
    try testing.expect(d.cache.contains(1) and d.cache.contains(3) and d.cache.contains(5));
    // b's landing can't evict them while pinned.
    try testing.expectEqual(Want.pending, d.want(2, .{ .name_hash = hashOf("b"), .from = 2, .limit = 10 }));
    _ = d.drain();
    try testing.expect(d.cache.contains(1) and d.cache.contains(3));
    // Served, 1 is no longer pinned, and the next landing may evict it.
    _ = d.get(1);
    Clock.now += 1;
    try testing.expectEqual(Want.pending, d.want(4, .{ .name_hash = hashOf("b"), .from = 4, .limit = 1 }));
    _ = d.drain();
    try testing.expect(!d.cache.contains(1));
    // Unserved ones are released after PIN_MS.
    Clock.now += PIN_MS;
    try testing.expectEqual(Want.pending, d.want(6, .{ .name_hash = hashOf("b"), .from = 6, .limit = 1 }));
    _ = d.drain();
    try testing.expect(!d.cache.contains(3));
}
