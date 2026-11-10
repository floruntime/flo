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
//!
//! Only the owning shard's thread asks, lands and evicts. Another shard's
//! thread (a processing job, a stream trigger reading the stream it watches)
//! may only copy a cached payload out (`copyFor`), under `mutex`, and ask
//! the owner to fetch one (`askFor`); its read stops there and it reads
//! again on its next poll.

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
/// At least the largest entry the log holds, or one could never land.
pub const READ_BYTES = @max(1024 * 1024, @import("../protocol/limits.zig").MAX_LOG_ENTRY_PAYLOAD);
/// Indices kept as not readable (absent from the durable log, or damaged).
pub const MAX_GONE = 4096;
pub const DEFAULT_CACHE_BYTES = 8 * 1024 * 1024;
/// Pinned entries may take the cache past its budget up to this many times
/// it. Past that a landing caches no more, and reads that need what it
/// dropped are refused retryable for `BACKOFF_MS` rather than read again.
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
    /// Can't be read right now: the read is answered retryable at once,
    /// saying why (`DiskReads.last_block`).
    blocked,
};

/// Why a want was answered blocked.
pub const BlockReason = enum {
    /// A resource or IO failure reading the durable log.
    io,
    /// The cache is full of entries pinned for readers not yet served.
    full,
    /// What was read doesn't match the log's terms.
    mismatch,
    /// A committed entry neither a sealed segment nor the writer holds yet.
    unwritten,
};

/// What a reader wants landed: its stream (`router.nsNameKey` of its
/// namespace id and name), from an index, at most `limit` of that stream's
/// appends.
pub const Window = struct {
    stream: [12]u8,
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
            if (std.mem.eql(u8, &have.stream, &w.stream) and have.from <= w.from) {
                // Its appends past `have.from`, through the end of `w`. An
                // upper bound: the stream's appends between the two may be
                // fewer than the indices between them.
                const reach = w.from - have.from + w.limit;
                have.limit = @intCast(@min(@max(@as(u64, have.limit), reach), std.math.maxInt(u32)));
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

    /// Held while the cache, `gone` or `asked` change, and by any read of
    /// them from a thread other than the owner's.
    mutex: stdx.Mutex = .{},
    /// The owning shard's thread, set as it starts (`setOwner`). Unset with
    /// a bulk thread attached, no thread is the owner yet: another shard's
    /// job takes the copy path. Unset with none (unit tests), every caller
    /// is.
    owner: std.atomic.Value(u64) = .init(0),
    /// Windows other threads asked for, taken by the owner's next drain.
    asked: [MAX_WANTS]Window = undefined,
    n_asked: usize = 0,
    /// Indices that won't be read: not in the durable log, or damaged.
    /// Readers skip and count them, as before reads went to disk.
    gone: std.AutoHashMapUnmanaged(u64, void) = .empty,
    /// The in-place read's buffers, made once.
    local_entries: []Entry = &.{},
    local_arena: []u8 = &.{},

    blocked_until_ms: u64 = 0,
    last_block: BlockReason = .io,
    /// Reads that came back blocked, results dropped for not matching the
    /// log, entries a full cache couldn't take, and indices found damaged.
    blocked_reads: u64 = 0,
    mismatches: u64 = 0,
    over_budget: u64 = 0,
    damaged: u64 = 0,
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
        if (self.local_entries.len > 0) self.allocator.free(self.local_entries);
        if (self.local_arena.len > 0) self.allocator.free(self.local_arena);
        var it = self.cache.valueIterator();
        while (it.next()) |v| self.allocator.free(v.payload);
        self.cache.deinit(self.allocator);
        self.order.deinit(self.allocator);
        self.gone.deinit(self.allocator);
    }

    /// The calling thread owns this cache from now on.
    pub fn setOwner(self: *DiskReads) void {
        self.owner.store(threadId(), .release);
    }

    fn threadId() u64 {
        return @intCast(std.Thread.getCurrentId());
    }

    /// Whether the calling thread is the owning shard's.
    pub fn isOwner(self: *const DiskReads) bool {
        const o = self.owner.load(.acquire);
        if (o == 0) return self.bulk == null;
        return o == threadId();
    }

    /// The raw entry payload at `index`, as the warm store keeps it, and
    /// served: no longer pinned. Valid until the next `drain`. The owner's
    /// thread only.
    pub fn get(self: *DiskReads, index: u64) ?[]const u8 {
        std.debug.assert(self.isOwner());
        self.mutex.lock();
        defer self.mutex.unlock();
        const c = self.cache.getPtr(index) orelse return null;
        c.pinned_until_ms = 0;
        return c.payload;
    }

    /// For another shard's thread: a copy of the cached payload at `index`,
    /// the caller's to free, or null.
    pub fn copyFor(self: *DiskReads, index: u64, allocator: std.mem.Allocator) ?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        const c = self.cache.get(index) orelse return null;
        return allocator.dupe(u8, c.payload) catch null;
    }

    /// For another shard's thread: whether `index` won't be read (not in
    /// the durable log, or damaged), so the job skips it.
    pub fn isGone(self: *DiskReads, index: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.gone.contains(index);
    }

    fn markGone(self: *DiskReads, index: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.gone.count() >= MAX_GONE) self.gone.clearRetainingCapacity();
        self.gone.put(self.allocator, index, {}) catch {};
    }

    /// For another shard's thread: ask the owner to fetch window `w` on its
    /// next tick.
    pub fn askFor(self: *DiskReads, w: Window) void {
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            for (self.asked[0..self.n_asked]) |have| if (std.mem.eql(u8, &have.stream, &w.stream) and have.from == w.from) return;
            if (self.n_asked == MAX_WANTS) return;
            self.asked[self.n_asked] = w;
            self.n_asked += 1;
        }
        if (self.notify) |n| n.done(n.ctx);
    }

    /// Ask for the entry at `index`, for a reader of window `w`. Never lands
    /// or evicts anything: a payload `get` returned stays valid.
    pub fn want(self: *DiskReads, index: u64, w: Window) Want {
        std.debug.assert(self.isOwner());
        // Only the owner writes `gone`, so it reads it unlocked.
        if (self.gone.contains(index)) return .absent;
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
        const found = self.dl.sealedSource(index, &path_buf) catch return self.blocked(now, .io);
        const src = found orelse {
            // Not sealed: in the writer's buffer, read in place at the next
            // drain, or not in the durable log at all.
            const wr = self.dl.writer;
            const last = if (wr.entry_count > 0) wr.last_index else (self.dl.lastSealedIndex() catch return self.blocked(now, .io));
            if (index > last) {
                // A committed entry the writer didn't buffer (the shard has
                // stopped taking writes and said so): retryable, not skipped,
                // and only this read is refused.
                self.warn(now, "a stream entry at index {d} is in neither a sealed segment nor the segment writer's buffer; holding reads of it", .{index});
                self.last_block = .unwritten;
                return .blocked;
            }
            if (wr.entry_count == 0 or index < wr.first_index) return .absent;
            return self.inPlace(.{ .stream = w.stream, .from = index, .limit = w.limit });
        };
        const bulk = self.bulk orelse return self.inPlace(.{ .stream = w.stream, .from = index, .limit = w.limit });
        const i = self.freeSlot() orelse return .pending;
        if (!self.ensureBuffers(i)) return self.blocked(now, .io);
        const slot = &self.slots[i];
        const n: usize = @intCast(@min(src.limit, READ_ENTRIES));
        slot.n_wants = 0;
        slot.add(.{ .stream = w.stream, .from = index, .limit = w.limit });
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
            return self.blocked(now, .io);
        }
        self.reads_made += 1;
        return .pending;
    }

    fn inPlace(self: *DiskReads, w: Window) Want {
        self.local.add(w);
        if (self.notify) |n| n.done(n.ctx);
        return .pending;
    }

    /// A blocked answer for the whole shard's cold reads, for `BACKOFF_MS`.
    fn blocked(self: *DiskReads, now: u64, why: BlockReason) Want {
        self.blocked_until_ms = now + BACKOFF_MS;
        self.last_block = why;
        return .blocked;
    }

    /// Land every finished read, and make the in-place ones. True when any
    /// landed or came back, so held reads are made again. Called between
    /// requests: it may evict.
    pub fn drain(self: *DiskReads) bool {
        std.debug.assert(self.isOwner());
        // What other threads asked for is fetched as the owner's own want;
        // only an index the log doesn't hold is marked gone for them.
        var asked: [MAX_WANTS]Window = undefined;
        const n_asked = blk: {
            self.mutex.lock();
            defer self.mutex.unlock();
            const n = self.n_asked;
            @memcpy(asked[0..n], self.asked[0..n]);
            self.n_asked = 0;
            break :blk n;
        };
        for (asked[0..n_asked]) |w| if (self.want(w.from, w) == .absent) self.markGone(w.from);
        var any = false;
        for (&self.slots) |*s| {
            if (s.read.state.load(.acquire) != .done) continue;
            const r = s.read.result;
            self.land(s.read.start, s.read.entries[0..r.count], r.blocked, r.disk_failing, s.wants[0..s.n_wants]);
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

    /// Read in place, outside the lock: it may open a file, and other
    /// shards' jobs copy under it.
    fn readInPlace(self: *DiskReads, w: Window) void {
        if (self.local_entries.len == 0) self.local_entries = self.allocator.alloc(Entry, READ_ENTRIES) catch &.{};
        if (self.local_arena.len == 0) self.local_arena = self.allocator.alloc(u8, READ_BYTES) catch &.{};
        if (self.local_entries.len == 0 or self.local_arena.len == 0) {
            _ = self.blocked(self.now_fn(), .io);
            return;
        }
        const r = self.dl.readRange(w.from, self.local_entries, self.local_arena);
        self.land(w.from, self.local_entries[0..r.count], r.blocked, r.disk_failing, &.{w});
    }

    fn land(self: *DiskReads, start: u64, entries: []const Entry, blocked_read: bool, disk_failing: bool, wants: []const Window) void {
        const now = self.now_fn();
        if (blocked_read) {
            self.blocked_reads += 1;
            _ = self.blocked(now, .io);
            if (disk_failing) self.warn(now, "a stream read from the durable log failed; the disk may be failing", .{});
            return;
        }
        if (entries.len == 0) {
            // Not blocked, and not even its first entry: the segment holding
            // it is corrupt or missing it. Skipped and counted, as an
            // unreadable record was before reads went to disk, and never
            // read again.
            self.damaged += 1;
            self.markGone(start);
            self.warn(now, "the stream entry at index {d} could not be read from its segment (corrupt or missing); its records are skipped", .{start});
            return;
        }
        self.mutex.lock();
        defer self.mutex.unlock();
        // Room first, then once more after: never a walk per entry.
        self.evict(now);
        var taken: [MAX_WANTS]u32 = @splat(0);
        for (entries) |*e| {
            if (e.header.entry_type != @backingInt(entry_mod.EntryType.stream_append)) continue;
            const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse continue;
            const stream = router.nsNameKey(cmd.namespace_id, cmd.key);
            var wanted = false;
            for (wants, 0..) |w, k| {
                if (!std.mem.eql(u8, &w.stream, &stream) or e.header.index < w.from or taken[k] >= w.limit) continue;
                taken[k] += 1;
                wanted = true;
            }
            if (!wanted) continue;
            if (self.raft_log.entryTerm(e.header.index) != e.header.term) {
                self.mismatches += 1;
                _ = self.blocked(now, .mismatch);
                self.warn(now, "a stream entry read from the durable log at index {d} doesn't match the log's term; holding and reading it again", .{e.header.index});
                continue;
            }
            self.put(e, now);
        }
        self.evict(now);
    }

    fn warn(self: *DiskReads, now: u64, comptime fmt: []const u8, args: anytype) void {
        if (self.warned_ms != 0 and now -| self.warned_ms < 10_000) return;
        self.warned_ms = now;
        log.warn(fmt, args);
    }

    /// One entry into the cache, pinned for its reader. Under the lock.
    fn put(self: *DiskReads, e: *const Entry, now: u64) void {
        const index = e.header.index;
        if (self.cache.getPtr(index)) |c| {
            c.pinned_until_ms = now + PIN_MS;
            return;
        }
        if (self.cache_bytes + e.payload.len > self.cache_budget * PINNED_BUDGET_TIMES) {
            self.over_budget += 1;
            _ = self.blocked(now, .full);
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

    /// Back within the budget, oldest unpinned first. Under the lock.
    fn evict(self: *DiskReads, now: u64) void {
        var at = self.order_head;
        while (self.cache_bytes > self.cache_budget and at < self.order.items.len) : (at += 1) {
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
    const cmd: entry_mod.CommandPayload = .{ .namespace_id = .default, .key_length = @intCast(stream.len), .value_length = @intCast(value.len), .key = stream, .value = value };
    const n = cmd.serialize(buf).?;
    var e = entry_mod.buildEntry(.stream_append, 0, term, index, index * 1000, buf[0..n]);
    e.header.crc32c = e.computeCrc();
    return e;
}

fn keyOf(stream: []const u8) [12]u8 {
    return router.nsNameKey(.default, stream);
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
    d.setOwner();

    const a: Window = .{ .stream = keyOf("a"), .from = 1, .limit = 10 };
    try testing.expectEqual(Want.pending, d.want(1, a));
    // Asked again inside the read in flight: no second read.
    try testing.expectEqual(Want.pending, d.want(3, a));
    try done.waitFor(1);
    // Back, but not landed until drain, whatever is asked meanwhile: a
    // request's payloads are never evicted under it.
    try testing.expectEqual(Want.pending, d.want(5, a));
    try testing.expectEqual(@as(u32, 0), d.cache.count());
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
    const a: Window = .{ .stream = keyOf("a"), .from = 1, .limit = 10 };
    try testing.expectEqual(Want.pending, d.want(1, a));
    try testing.expect(d.get(1) == null);
    try testing.expect(d.drain());
    try testing.expect(d.get(1) != null and d.get(5) != null);
    try testing.expectEqual(@as(u64, 0), d.reads_made);
    // Past the writer and every segment: a committed entry neither holds,
    // held and retried, never skipped as absent.
    try testing.expectEqual(Want.blocked, d.want(9, a));
}

test "stream disk reads: an entry whose term isn't the log's is not cached, and reads back off" {
    Clock.now = 1_000;
    var f: Fixture = undefined;
    try f.init(4, true);
    defer f.deinit();
    var d = DiskReads.init(testing.allocator, &f.dl, &f.raft_log);
    defer d.deinit();
    d.now_fn = Clock.get;
    const a: Window = .{ .stream = keyOf("a"), .from = 1, .limit = 10 };
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
    try testing.expectEqual(Want.pending, d.want(1, .{ .stream = keyOf("a"), .from = 1, .limit = 10 }));
    _ = d.drain();
    // Over its budget, but pinned: all three of a's kept.
    try testing.expect(d.cache.contains(1) and d.cache.contains(3) and d.cache.contains(5));
    // b's landing can't evict them while pinned.
    try testing.expectEqual(Want.pending, d.want(2, .{ .stream = keyOf("b"), .from = 2, .limit = 10 }));
    _ = d.drain();
    try testing.expect(d.cache.contains(1) and d.cache.contains(3));
    // Served, 1 is no longer pinned, and the next landing may evict it.
    _ = d.get(1);
    Clock.now += 1;
    try testing.expectEqual(Want.pending, d.want(4, .{ .stream = keyOf("b"), .from = 4, .limit = 1 }));
    _ = d.drain();
    try testing.expect(!d.cache.contains(1));
    // Unserved ones are released after PIN_MS.
    Clock.now += PIN_MS;
    try testing.expectEqual(Want.pending, d.want(6, .{ .stream = keyOf("b"), .from = 6, .limit = 1 }));
    _ = d.drain();
    try testing.expect(!d.cache.contains(3));
}

test "stream disk reads: a corrupt segment marks its entry damaged once, skipped, never read again" {
    var f: Fixture = undefined;
    try f.init(1, true);
    defer f.deinit();
    // One byte flipped in the sealed segment's data.
    const seg = (try f.dl.segments())[0].path;
    const bytes = try stdx.fs.readFileAlloc(testing.allocator, seg, 1 << 20);
    defer testing.allocator.free(bytes);
    bytes[bytes.len - 8] ^= 0xff;
    {
        const file = try stdx.fs.createFile(seg, .{});
        defer stdx.fs.closeFile(file);
        try stdx.fs.writeAll(file, bytes);
    }
    var d = DiskReads.init(testing.allocator, &f.dl, &f.raft_log);
    defer d.deinit();
    const a: Window = .{ .stream = keyOf("a"), .from = 1, .limit = 10 };
    try testing.expectEqual(Want.pending, d.want(1, a));
    _ = d.drain();
    try testing.expectEqual(@as(u64, 1), d.damaged);
    // Skipped and counted by the reader, as before reads went to disk; not
    // read again, nor held.
    try testing.expectEqual(Want.absent, d.want(1, a));
    try testing.expect(!d.drain());
    try testing.expect(d.isGone(1));
}

test "stream disk reads: an entry as large as the log takes lands, not taken as damaged" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const real = try stdx.fs.dirRealpathAlloc(tmp.dir, testing.allocator, ".");
    defer testing.allocator.free(real);
    const path = try std.fmt.allocPrint(testing.allocator, "{s}/segs", .{real});
    defer testing.allocator.free(path);
    try stdx.fs.makePath(path);
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, path);
    defer dl.deinit();
    var raft_log = try RaftLog.init(testing.allocator, 4 << 20);
    defer raft_log.deinit();

    const limits = @import("../protocol/limits.zig");
    const value = try testing.allocator.alloc(u8, limits.MAX_LOG_ENTRY_PAYLOAD - entry_mod.COMMAND_PREFIX_SIZE - 1);
    defer testing.allocator.free(value);
    @memset(value, 'v');
    const buf = try testing.allocator.alloc(u8, limits.MAX_LOG_ENTRY_PAYLOAD);
    defer testing.allocator.free(buf);
    const e = appendEntry(buf, 1, 1, "a", value);
    try testing.expect(e.payload.len > 1024 * 1024);
    try writer.addEntry(&e);
    var mine = e;
    _ = try raft_log.append(&mine);
    try dl.flush(1);

    var d = DiskReads.init(testing.allocator, &dl, &raft_log);
    defer d.deinit();
    try testing.expectEqual(Want.pending, d.want(1, .{ .stream = keyOf("a"), .from = 1, .limit = 1 }));
    _ = d.drain();
    try testing.expectEqual(@as(u64, 0), d.damaged);
    try testing.expectEqual(e.payload.len, d.get(1).?.len);
}
