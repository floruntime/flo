//! Stream payloads read back from the shard's sealed segments: a record
//! whose append has left both the hot ring and the warm store is still on
//! disk, and a reader gets it from there instead of skipping it.
//!
//! The read itself runs on the node's bulk I/O thread, never on the shard's
//! own: `want` queues it and answers `pending`, and the read that asked is
//! held until the result is back (`drain`), then made again. Results land
//! in a small cache, keyed by log index, that later reads hit directly.
//!
//! Only committed entries are asked for, and those are never cut, but a
//! result is still checked against the log, index and term, before it is
//! cached; one that doesn't match is dropped and counted.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const DurableLog = @import("../storage/durable_log.zig").DurableLog;
const bulk_io = @import("../storage/bulk_io.zig");
const RaftLog = @import("../raft/log.zig").RaftLog;

const Entry = entry_mod.Entry;
const log = std.log.scoped(.stream_disk);

/// Reads in flight at once per shard.
pub const SLOTS = 8;
/// Entries and payload bytes one read brings back: a run of the segment
/// from the index asked for, which a reader going forward wants next.
pub const READ_ENTRIES = 256;
pub const READ_BYTES = 1024 * 1024;
pub const DEFAULT_CACHE_BYTES = 8 * 1024 * 1024;

pub const Want = enum {
    /// Being read: hold the read that asked, and make it again once woken.
    pending,
    /// Not in the durable log either (below its first segment): the reader
    /// skips it as before.
    absent,
    /// Can't be read right now (a resource or IO failure): the held read
    /// answers retryable once its deadline passes.
    blocked,
};

pub const DiskReads = struct {
    allocator: std.mem.Allocator,
    dl: *DurableLog,
    raft_log: *const RaftLog,
    bulk: ?*bulk_io.BulkIo = null,
    notify: ?bulk_io.Notify = null,
    reads: [SLOTS]bulk_io.SealedRead = @splat(.{}),
    entry_bufs: [SLOTS][]Entry = @splat(&.{}),
    arenas: [SLOTS][]u8 = @splat(&.{}),

    cache: std.AutoHashMapUnmanaged(u64, []u8) = .empty,
    /// Indices in the order they were cached, oldest first, for eviction.
    order: std.ArrayListUnmanaged(u64) = .empty,
    order_head: usize = 0,
    cache_bytes: usize = 0,
    cache_budget: usize = DEFAULT_CACHE_BYTES,

    /// Reads that came back blocked, and results dropped for not matching.
    blocked_reads: u64 = 0,
    mismatches: u64 = 0,
    disk_failing: bool = false,

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
        while (it.next()) |v| self.allocator.free(v.*);
        self.cache.deinit(self.allocator);
        self.order.deinit(self.allocator);
    }

    /// The raw entry payload at `index`, as the warm store keeps it.
    pub fn get(self: *const DiskReads, index: u64) ?[]const u8 {
        return self.cache.get(index);
    }

    /// Ask for the entry at `index`. Without a bulk thread it is read in
    /// place, at once.
    pub fn want(self: *DiskReads, index: u64) Want {
        if (self.cache.contains(index)) return .pending;
        for (&self.reads) |*r| {
            const state = r.state.load(.acquire);
            if (state == .idle) continue;
            const end = r.start + r.entries.len;
            if (index >= r.start and index < end) return if (state == .queued) .pending else self.landOne(r, index);
        }
        var path_buf: [bulk_io.MAX_PATH]u8 = undefined;
        // In no sealed segment: still in the writer's buffer, waiting for a
        // flush (under async durability a burst outruns it), which is read
        // in place with no I/O; or not in the durable log at all.
        const src = (self.dl.sealedSource(index, &path_buf) catch return .blocked) orelse return self.readInPlace(index);
        const bulk = self.bulk orelse return self.readInPlace(index);
        const i = self.freeSlot() orelse return .pending;
        if (!self.ensureBuffers(i)) return .blocked;
        const slot = &self.reads[i];
        const n: usize = @intCast(@min(src.limit, READ_ENTRIES));
        slot.* = .{
            .path_len = src.path.len,
            .start = index,
            .generation = self.dl.truncations,
            .entries = self.entry_bufs[i][0..n],
            .arena = self.arenas[i],
            .notify = self.notify,
        };
        @memcpy(slot.path[0..src.path.len], src.path);
        slot.state.store(.queued, .release);
        if (!bulk.submit(slot, .foreground)) {
            slot.state.store(.idle, .release);
            return self.readInPlace(index);
        }
        return .pending;
    }

    /// A finished read covering `index`, landed now: it was done but not yet
    /// drained.
    fn landOne(self: *DiskReads, r: *bulk_io.SealedRead, index: u64) Want {
        self.land(r);
        return if (self.cache.contains(index)) .pending else .absent;
    }

    /// Every finished read into the cache. True when any landed, so held
    /// reads are made again.
    pub fn drain(self: *DiskReads) bool {
        var any = false;
        for (&self.reads) |*r| {
            if (r.state.load(.acquire) != .done) continue;
            self.land(r);
            any = true;
        }
        return any;
    }

    fn land(self: *DiskReads, r: *bulk_io.SealedRead) void {
        defer r.state.store(.idle, .release);
        if (r.result.blocked) {
            self.blocked_reads += 1;
            if (r.result.disk_failing and !self.disk_failing) log.warn("a stream read from segment {s} failed; the disk may be failing", .{r.pathSlice()});
            self.disk_failing = r.result.disk_failing;
            return;
        }
        self.disk_failing = false;
        for (r.entries[0..r.result.count]) |*e| self.put(e);
    }

    /// One entry into the cache, if it is the log's own.
    fn put(self: *DiskReads, e: *const Entry) void {
        const index = e.header.index;
        if (self.raft_log.entryTerm(index) != e.header.term) {
            self.mismatches += 1;
            return;
        }
        if (e.payload.len == 0 or self.cache.contains(index)) return;
        const copy = self.allocator.dupe(u8, e.payload) catch return;
        self.cache.put(self.allocator, index, copy) catch {
            self.allocator.free(copy);
            return;
        };
        self.order.append(self.allocator, index) catch {};
        self.cache_bytes += copy.len;
        self.evict();
    }

    fn evict(self: *DiskReads) void {
        while (self.cache_bytes > self.cache_budget and self.order_head < self.order.items.len) {
            const index = self.order.items[self.order_head];
            self.order_head += 1;
            if (self.cache.fetchRemove(index)) |kv| {
                self.cache_bytes -= kv.value.len;
                self.allocator.free(kv.value);
            }
        }
        // Keep the order list from growing without bound.
        if (self.order_head > 4096 and self.order_head * 2 > self.order.items.len) {
            const rest = self.order.items[self.order_head..];
            std.mem.copyForwards(u64, self.order.items[0..rest.len], rest);
            self.order.shrinkRetainingCapacity(rest.len);
            self.order_head = 0;
        }
    }

    fn readInPlace(self: *DiskReads, index: u64) Want {
        var buf: [READ_ENTRIES]Entry = undefined;
        const arena = self.allocator.alloc(u8, READ_BYTES) catch return .blocked;
        defer self.allocator.free(arena);
        const r = self.dl.readRange(index, &buf, arena);
        if (r.blocked) {
            self.blocked_reads += 1;
            return .blocked;
        }
        for (buf[0..r.count]) |*e| self.put(e);
        return if (self.cache.contains(index)) .pending else .absent;
    }

    fn freeSlot(self: *DiskReads) ?usize {
        for (&self.reads, 0..) |*r, i| if (r.state.load(.acquire) == .idle) return i;
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
const stdx = @import("stdx");
const SegmentWriter = @import("../storage/ual/writer.zig").SegmentWriter;

fn testEntry(index: u64, term: u64, payload: []const u8) Entry {
    var e = entry_mod.buildEntry(.stream_append, 0, term, index, index * 1000, payload);
    e.header.crc32c = e.computeCrc();
    return e;
}

/// Entries 1-3 sealed in a segment, and a Raft log holding them too, so
/// their terms are known.
const Fixture = struct {
    tmp: testing.TmpDir,
    path: []const u8,
    writer: SegmentWriter,
    dl: DurableLog,
    raft_log: RaftLog,

    fn init(self: *Fixture, terms: [3]u64) !void {
        self.tmp = testing.tmpDir(.{});
        const real = try stdx.fs.dirRealpathAlloc(self.tmp.dir, testing.allocator, ".");
        defer testing.allocator.free(real);
        self.path = try std.fmt.allocPrint(testing.allocator, "{s}/segs", .{real});
        try stdx.fs.makePath(self.path);
        self.writer = SegmentWriter.init(testing.allocator, 0);
        self.dl = try DurableLog.init(testing.allocator, &self.writer, self.path);
        self.raft_log = try RaftLog.init(testing.allocator, 4096);
        for ([_][]const u8{ "one", "two", "three" }, 1..) |p, i| {
            const e = testEntry(i, 1, p);
            try self.writer.addEntry(&e);
            var mine = testEntry(i, terms[i - 1], p);
            _ = try self.raft_log.append(&mine);
        }
        try self.dl.flush(3);
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

test "stream disk reads: a payload is read on the bulk thread, pending until it is back, then cached" {
    var f: Fixture = undefined;
    try f.init(.{ 1, 1, 1 });
    defer f.deinit();
    var d = DiskReads.init(testing.allocator, &f.dl, &f.raft_log);
    defer d.deinit();
    var bulk = try bulk_io.BulkIo.init(testing.allocator);
    defer bulk.deinit();
    try bulk.start();
    var done: Done = .{};
    d.bulk = &bulk;
    d.notify = .{ .ctx = &done, .done = Done.done };

    try testing.expect(d.get(2) == null);
    try testing.expectEqual(Want.pending, d.want(2));
    try done.waitFor(1);
    try testing.expect(d.drain());
    try testing.expectEqualStrings("two", d.get(2).?);
    // The run after it came with it.
    try testing.expectEqualStrings("three", d.get(3).?);
    // Not on disk: below or past every segment.
    try testing.expectEqual(Want.absent, d.want(9));
}

test "stream disk reads: an entry whose term isn't the log's is dropped, not cached" {
    var f: Fixture = undefined;
    try f.init(.{ 1, 4, 1 });
    defer f.deinit();
    var d = DiskReads.init(testing.allocator, &f.dl, &f.raft_log);
    defer d.deinit();
    // No bulk thread: read in place.
    try testing.expectEqual(Want.absent, d.want(2));
    try testing.expect(d.get(2) == null);
    try testing.expectEqual(@as(u64, 1), d.mismatches);
    try testing.expectEqualStrings("three", d.get(3).?);
}

test "stream disk reads: the cache keeps to its budget, oldest first" {
    var f: Fixture = undefined;
    try f.init(.{ 1, 1, 1 });
    defer f.deinit();
    var d = DiskReads.init(testing.allocator, &f.dl, &f.raft_log);
    defer d.deinit();
    d.cache_budget = 8;
    d.put(&testEntry(1, 1, "one"));
    d.put(&testEntry(2, 1, "two"));
    try testing.expectEqual(@as(usize, 6), d.cache_bytes);
    // "three" brings it to 11: the oldest, "one", goes.
    d.put(&testEntry(3, 1, "three"));
    try testing.expect(d.get(1) == null);
    try testing.expectEqualStrings("two", d.get(2).?);
    try testing.expectEqualStrings("three", d.get(3).?);
    try testing.expectEqual(@as(usize, 8), d.cache_bytes);
}
