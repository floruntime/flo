//! A shard's reads below its Raft ring, from its durable log. The apply loop
//! reads in place. A leader replicating to a peer that has fallen below the
//! ring hands a sealed segment's read to the node's bulk I/O thread instead,
//! and answers `pending` until it is back, so its tick never waits on the disk.
//!
//! Finished reads are kept by their start index, not by peer: the next pump
//! asks again from the peer's next index and finds it, and one asked from
//! anywhere else is never handed out. A read made before a truncation is
//! dropped unread, whatever it came to, and the log checks each entry's term
//! before it sends it.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const DurableLog = @import("../storage/durable_log.zig").DurableLog;
const bulk_io = @import("../storage/bulk_io.zig");
const raft_log_mod = @import("../raft/log.zig");
const MAX_PEERS = @import("../raft/node.zig").MAX_PEERS;

const Entry = entry_mod.Entry;
const CatchUpResult = raft_log_mod.CatchUpResult;

pub const CatchUp = struct {
    allocator: std.mem.Allocator,
    dl: *DurableLog,
    /// The node's bulk I/O thread; null reads every range in place.
    bulk: ?*bulk_io.BulkIo = null,
    notify: ?bulk_io.Notify = null,
    /// Reads shared by the peers, each kept by its start index: as many
    /// in flight as there are peers, at most.
    reads: [MAX_PEERS]bulk_io.SealedRead = @splat(.{}),
    /// The leadership each read was asked for in; `forget` moves it on.
    asked_in: [MAX_PEERS]u64 = @splat(0),
    leadership: u64 = 0,
    /// Each read's own buffers, allocated the first time it is used.
    entry_bufs: [MAX_PEERS][]Entry = @splat(&.{}),
    arenas: [MAX_PEERS][]u8 = @splat(&.{}),
    max_entries: usize,
    arena_bytes: usize,

    pub fn init(allocator: std.mem.Allocator, dl: *DurableLog, max_entries: usize, arena_bytes: usize) CatchUp {
        return .{ .allocator = allocator, .dl = dl, .max_entries = max_entries, .arena_bytes = arena_bytes };
    }

    /// After the bulk thread has stopped: it may still be writing into a
    /// queued read's buffers until then.
    pub fn deinit(self: *CatchUp) void {
        for (self.entry_bufs, self.arenas) |e, a| {
            if (e.len > 0) self.allocator.free(e);
            if (a.len > 0) self.allocator.free(a);
        }
    }

    pub fn source(self: *CatchUp) raft_log_mod.CatchUpSource {
        return .{ .ctx = self, .read_range = readInPlace, .read_range_async = readAsync };
    }

    /// On losing leadership, every read is dropped: a finished one now, one
    /// still queued once it finishes, unread. A later leadership asks again.
    pub fn forget(self: *CatchUp) void {
        self.leadership += 1;
        for (&self.reads) |*r| {
            if (r.state.load(.acquire) == .done) r.state.store(.idle, .release);
        }
    }

    /// Reads in flight on the bulk thread.
    pub fn inFlight(self: *const CatchUp) usize {
        var n: usize = 0;
        for (&self.reads) |*r| n += @intFromBool(r.state.load(.acquire) == .queued);
        return n;
    }

    fn readInPlace(ctx: *anyopaque, start: u64, buf: []Entry, arena: []u8) CatchUpResult {
        const self: *CatchUp = @ptrCast(@alignCast(ctx));
        const r = self.dl.readRange(start, buf, arena);
        return .{ .count = r.count, .blocked = r.blocked, .disk_failing = r.disk_failing };
    }

    fn readAsync(ctx: *anyopaque, start: u64, buf: []Entry, arena: []u8) CatchUpResult {
        const self: *CatchUp = @ptrCast(@alignCast(ctx));
        const bulk = self.bulk orelse return readInPlace(ctx, start, buf, arena);
        for (&self.reads, self.asked_in) |*r, asked_in| {
            const state = r.state.load(.acquire);
            if (r.start != start or state == .idle) continue;
            if (state == .queued) return .{ .pending = true };
            r.state.store(.idle, .release);
            // Before anything it came to: a fault there may only be a file a
            // cut has deleted since.
            if (r.generation != self.dl.truncations) break;
            if (asked_in != self.leadership) break;
            return copyOut(r, buf, arena);
        }
        var path_buf: [bulk_io.MAX_PATH]u8 = undefined;
        const src = (self.dl.sealedSource(start, &path_buf) catch null) orelse return readInPlace(ctx, start, buf, arena);
        const i = self.freeSlot() orelse return .{ .pending = true };
        if (!self.ensureBuffers(i)) return readInPlace(ctx, start, buf, arena);
        const slot = &self.reads[i];
        const n: usize = @intCast(@min(src.limit, @min(buf.len, self.max_entries)));
        slot.* = .{
            .path_len = src.path.len,
            .start = start,
            .generation = self.dl.truncations,
            .entries = self.entry_bufs[i][0..n],
            .arena = self.arenas[i][0..@min(arena.len, self.arena_bytes)],
            .notify = self.notify,
        };
        @memcpy(slot.path[0..src.path.len], src.path);
        self.asked_in[i] = self.leadership;
        slot.state.store(.queued, .release);
        if (!bulk.submit(slot, .foreground)) {
            slot.state.store(.idle, .release);
            return readInPlace(ctx, start, buf, arena);
        }
        return .{ .pending = true };
    }

    /// An idle read, else a finished one nobody asked for again.
    fn freeSlot(self: *CatchUp) ?usize {
        for (&self.reads, 0..) |*r, i| if (r.state.load(.acquire) == .idle) return i;
        for (&self.reads, 0..) |*r, i| if (r.state.load(.acquire) == .done) return i;
        return null;
    }

    fn ensureBuffers(self: *CatchUp, i: usize) bool {
        if (self.entry_bufs[i].len == 0) {
            self.entry_bufs[i] = self.allocator.alloc(Entry, self.max_entries) catch return false;
        }
        if (self.arenas[i].len == 0) {
            self.arenas[i] = self.allocator.alloc(u8, self.arena_bytes) catch return false;
        }
        return true;
    }

    /// The read's entries into the caller's buffers, as many as fit.
    fn copyOut(r: *const bulk_io.SealedRead, buf: []Entry, arena: []u8) CatchUpResult {
        var used: usize = 0;
        var n: usize = 0;
        while (n < r.result.count and n < buf.len) : (n += 1) {
            const e = r.entries[n];
            if (used + e.payload.len > arena.len) break;
            @memcpy(arena[used..][0..e.payload.len], e.payload);
            buf[n] = .{ .header = e.header, .payload = arena[used..][0..e.payload.len] };
            used += e.payload.len;
        }
        return .{ .count = n, .blocked = r.result.blocked, .disk_failing = r.result.disk_failing };
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

/// Counts the reads the bulk thread says are done.
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

/// A durable log with entries 1-3 sealed and 4-5 in the writer's buffer.
const Fixture = struct {
    tmp: testing.TmpDir,
    path: []const u8,
    writer: SegmentWriter,
    dl: DurableLog,

    fn init(self: *Fixture) !void {
        self.tmp = testing.tmpDir(.{});
        const real = try stdx.fs.dirRealpathAlloc(self.tmp.dir, testing.allocator, ".");
        defer testing.allocator.free(real);
        self.path = try std.fmt.allocPrint(testing.allocator, "{s}/segs", .{real});
        try stdx.fs.makePath(self.path);
        self.writer = SegmentWriter.init(testing.allocator, 0);
        self.dl = try DurableLog.init(testing.allocator, &self.writer, self.path);
        try self.writer.addEntry(&testEntry(1, 1, "one"));
        try self.writer.addEntry(&testEntry(2, 1, "two"));
        try self.writer.addEntry(&testEntry(3, 1, "three"));
        try self.dl.flush(3);
        try self.writer.addEntry(&testEntry(4, 2, "four"));
        try self.writer.addEntry(&testEntry(5, 2, "five"));
    }

    fn deinit(self: *Fixture) void {
        self.dl.deinit();
        self.writer.deinit();
        testing.allocator.free(self.path);
        self.tmp.cleanup();
    }
};

test "catch up: the writer's buffer is read at once; a sealed segment's read is pending, then served" {
    var f: Fixture = undefined;
    try f.init();
    defer f.deinit();
    var cu = CatchUp.init(testing.allocator, &f.dl, 8, 256);
    defer cu.deinit();
    var bulk = try bulk_io.BulkIo.init(testing.allocator);
    defer bulk.deinit();
    try bulk.start();
    var done: Done = .{};
    cu.bulk = &bulk;
    cu.notify = .{ .ctx = &done, .done = Done.done };

    var buf: [8]Entry = undefined;
    var arena: [256]u8 = undefined;
    const in_writer = askAsync(&cu, 4, &buf, &arena);
    try testing.expectEqual(@as(usize, 2), in_writer.count);
    try testing.expectEqualStrings("five", buf[1].payload);

    try testing.expect(askAsync(&cu, 1, &buf, &arena).pending);
    try done.waitFor(1);
    // Asked from elsewhere, it isn't handed out.
    try testing.expect(askAsync(&cu, 2, &buf, &arena).pending);
    const back = askAsync(&cu, 1, &buf, &arena);
    try testing.expectEqual(@as(usize, 3), back.count);
    try testing.expectEqualStrings("one", buf[0].payload);
    try testing.expectEqualStrings("three", buf[2].payload);
    try done.waitFor(2);
}

test "catch up: a read made before a truncation is dropped, whatever it came to" {
    var f: Fixture = undefined;
    try f.init();
    defer f.deinit();
    var cu = CatchUp.init(testing.allocator, &f.dl, 8, 256);
    defer cu.deinit();
    var bulk = try bulk_io.BulkIo.init(testing.allocator);
    defer bulk.deinit();
    try bulk.start();
    var done: Done = .{};
    cu.bulk = &bulk;
    cu.notify = .{ .ctx = &done, .done = Done.done };

    var buf: [8]Entry = undefined;
    var arena: [256]u8 = undefined;
    try testing.expect(askAsync(&cu, 2, &buf, &arena).pending);
    try done.waitFor(1);
    // The cut drops 2 and 3: what the read found is not used, and nothing
    // durable holds 2 any more.
    try f.dl.truncateAfter(1);
    const after = askAsync(&cu, 2, &buf, &arena);
    try testing.expectEqual(@as(usize, 0), after.count);
    try testing.expect(!after.pending and !after.blocked);
    try testing.expectEqual(@as(usize, 0), cu.inFlight());
}

test "catch up: losing leadership drops every read, a queued one once it finishes" {
    var f: Fixture = undefined;
    try f.init();
    defer f.deinit();
    var cu = CatchUp.init(testing.allocator, &f.dl, 8, 256);
    defer cu.deinit();
    var bulk = try bulk_io.BulkIo.init(testing.allocator);
    defer bulk.deinit();
    var done: Done = .{};
    cu.bulk = &bulk;
    cu.notify = .{ .ctx = &done, .done = Done.done };

    var buf: [8]Entry = undefined;
    var arena: [256]u8 = undefined;
    // Queued, not yet read: the thread isn't running.
    try testing.expect(askAsync(&cu, 1, &buf, &arena).pending);
    cu.forget();
    try bulk.start();
    try done.waitFor(1);
    // Read for the old leadership: not handed out, asked again instead.
    try testing.expect(askAsync(&cu, 1, &buf, &arena).pending);
    try done.waitFor(2);
    try testing.expectEqual(@as(usize, 3), askAsync(&cu, 1, &buf, &arena).count);
}

test "catch up: with no bulk thread every read is made in place" {
    var f: Fixture = undefined;
    try f.init();
    defer f.deinit();
    var cu = CatchUp.init(testing.allocator, &f.dl, 8, 256);
    defer cu.deinit();
    var buf: [8]Entry = undefined;
    var arena: [256]u8 = undefined;
    try testing.expectEqual(@as(usize, 3), askAsync(&cu, 1, &buf, &arena).count);
}

fn askAsync(cu: *CatchUp, start: u64, buf: []Entry, arena: []u8) CatchUpResult {
    return cu.source().read_range_async.?(cu, start, buf, arena);
}
