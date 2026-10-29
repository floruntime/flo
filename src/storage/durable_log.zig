//! The durable Raft log of one shard: the segment writer's unflushed buffer
//! plus the sealed segments under `segs/`. Together they hold every entry
//! ever appended; the hot ring holds the newest ones as well. The Raft
//! layer needs three things from the durable copy that the ring cannot
//! give: entries below the ring for catching a peer up, the commit
//! watermark a restart may apply up to, and truncation that reaches the
//! files, so a later flush cannot put a second entry at a truncated index
//! in a newer file that replay would then apply instead of the right one.

const std = @import("std");
const stdx = @import("stdx");
const segment = @import("ual/segment.zig");
const entry_mod = @import("ual/entry.zig");
const SegmentWriter = @import("ual/writer.zig").SegmentWriter;
const SegmentReader = @import("ual/reader.zig").SegmentReader;
const log = stdx.log;

const Entry = entry_mod.Entry;

/// Written before a truncation touches any file and removed after the last
/// one; a boot that finds it finishes the cut before replaying.
pub const INTENT_FILENAME = "TRUNCATE";

pub const SegmentFile = struct {
    first_index: u64,
    last_index: u64,
    commit_index_at_seal: u64,
    /// The header says it holds a repair skip.
    holds_skip: bool = false,
    /// The header would not read; listed only by a recovery listing, with
    /// first_index from the filename and the rest unknown.
    unreadable: bool = false,
    path: []const u8,
};

pub const DurableLog = struct {
    allocator: std.mem.Allocator,
    writer: *SegmentWriter,
    segs_path: []const u8,
    /// Truncations that did not reach every file; each one is a replay
    /// that may apply a suffix the leader discarded.
    truncate_failures: u64 = 0,
    /// The cut the files do not yet reflect, held here because the intent
    /// file may itself be what failed to write. No flush happens while it
    /// is set: a new file above the cut would be indistinguishable, at the
    /// next boot, from one the cut should delete. A flush or a truncation
    /// retries the cut first.
    pending_cut: ?u64 = null,
    /// The sorted listing, valid until the next flush or truncation.
    listing: ?[]SegmentFile = null,
    /// The segment whose header would not read, when listing failed on it.
    bad_header: [128]u8 = undefined,
    bad_header_len: usize = 0,
    /// The segment last read below the ring, kept open: a deferred tail
    /// reads one entry per call, and re-opening and CRC-checking the whole
    /// file each time is quadratic.
    open: ?Open = null,

    const Open = struct {
        first_index: u64,
        last_index: u64,
        buf: []u8,
        reader: SegmentReader,
    };

    pub fn init(allocator: std.mem.Allocator, writer: *SegmentWriter, segs_path: []const u8) !DurableLog {
        return .{
            .allocator = allocator,
            .writer = writer,
            .segs_path = try allocator.dupe(u8, segs_path),
        };
    }

    pub fn deinit(self: *DurableLog) void {
        self.invalidate();
        self.allocator.free(self.segs_path);
    }

    /// Seal the buffered entries into a new segment stamped with the commit
    /// index they were flushed under. Nothing to flush is not an error.
    pub fn flush(self: *DurableLog, commit_index: u64) !void {
        if (self.writer.entry_count == 0) return;
        if (self.pending_cut != null) {
            self.recoverTruncation() catch return error.TruncationIncomplete;
        }
        self.writer.commit_index_at_seal = commit_index;
        // A failed write may still leave the file in place (only the
        // directory sync failed), so drop the cached listing.
        errdefer self.invalidate();
        try self.writer.writeToFile(self.segs_path);
        self.writer.reset();
        self.invalidate();
    }

    /// The highest commit index any sealed segment was flushed under: what
    /// a restart may apply into projections without waiting for commit to
    /// be re-established. Zero when there are no segments.
    pub fn watermark(self: *DurableLog) !u64 {
        const files = try self.segments();
        var mark: u64 = 0;
        for (files) |f| mark = @max(mark, f.commit_index_at_seal);
        return mark;
    }

    /// Copy contiguous entries from `start` upward into `buf`, payloads
    /// packed into `arena`, from wherever they are: the writer's buffer if
    /// it holds `start`, else the sealed segment whose range covers it. Stops
    /// at the end of that source, so a caller wanting more calls again from
    /// the next index. Returns the number of entries copied; zero when
    /// nothing durable holds `start`. An unopenable segment, or one whose
    /// header covers `start` but whose entries do not, is logged. While a
    /// cut is pending the files still hold the discarded suffix, so reads
    /// stop at the cut.
    pub fn readRange(self: *DurableLog, start: u64, buf: []Entry, arena: []u8) usize {
        if (buf.len == 0) return 0;
        const w = self.writer;
        if (w.entry_count > 0 and start >= w.first_index and start <= w.last_index) {
            return copyFrom(w.data.items, start, buf, arena);
        }
        if (self.pending_cut) |cut| {
            if (start > cut) return 0;
        }
        const opened = self.openCovering(start) catch |err| {
            log.err("durable log: cannot read below the ring at index {d}: {s}", .{ start, @errorName(err) });
            return 0;
        } orelse return 0;
        const offset = opened.reader.findOffset(start) orelse {
            log.err("durable log: the segment covering index {d} does not hold it", .{start});
            return 0;
        };
        const data = opened.buf[opened.reader.data_start + offset .. opened.reader.data_end];
        const limit = if (self.pending_cut) |cut| @min(buf.len, @as(usize, @intCast(cut - start + 1))) else buf.len;
        return copyFrom(data, start, buf[0..limit], arena);
    }

    /// Drop every durable entry above `after_index`: the writer's buffer
    /// first, then the sealed segment spanning the cut is rewritten under
    /// its own filename with only the entries at or below (first_index is
    /// the filename, so the name is stable), and every file wholly above
    /// it is deleted. In `sync` durability the tail can span several files.
    /// The intent is recorded on disk first: a cut interrupted by a crash
    /// or an error is finished by the next boot, because a segment still
    /// holding a truncated index would win at replay.
    pub fn truncateAfter(self: *DurableLog, after_index: u64) !void {
        self.writer.truncateAfter(after_index);
        self.invalidate();
        // The log is already cut in memory; any failure from here leaves
        // files that disagree with it, and the cut is remembered until
        // they agree again.
        errdefer {
            self.truncate_failures += 1;
            self.pending_cut = @min(self.pending_cut orelse after_index, after_index);
        }
        if (self.pending_cut != null) try self.recoverTruncation();
        const files = try self.listSegments(true);
        defer self.freeSegments(files);
        if (files.len == 0 or files[files.len - 1].last_index <= after_index) return;
        try self.writeIntent(after_index);
        try self.applyCut(files, after_index);
        try self.clearIntent();
    }

    /// Drop every entry above `after_index` from a stopped shard whose log
    /// may be damaged above it: an offline salvage. The cut is recorded, as
    /// `truncateAfter` records it, and finished by the recovery pass, which
    /// deletes a file wholly above the cut even when its header won't read.
    /// A cut already pending lower is kept. Fails with `SegmentUnreadable`,
    /// before recording anything, when a damaged file holds entries at or
    /// below the cut: a cut recorded there would stop every boot after.
    pub fn salvageAfter(self: *DurableLog, after_index: u64) !void {
        std.debug.assert(self.writer.entry_count == 0);
        const pending = try self.readIntent();
        const cut = @min(after_index, pending orelse after_index);
        {
            const files = try self.listSegments(false);
            defer self.freeSegments(files);
            for (files) |f| {
                if (f.first_index > cut or f.last_index <= cut) continue;
                if (f.unreadable) return error.SegmentUnreadable;
                // The one file the cut rewrites must read whole.
                const opened = SegmentReader.initFromFile(self.allocator, f.path) catch return error.SegmentUnreadable;
                self.allocator.free(opened.buf);
            }
        }
        try self.writeIntent(cut);
        try self.recoverTruncation();
    }

    /// Finish a truncation that did not complete: one a previous run
    /// recorded on disk, or one this run still holds in memory, whichever
    /// cuts lower. A no-op with neither.
    pub fn recoverTruncation(self: *DurableLog) !void {
        const on_disk = try self.readIntent();
        const after_index = if (on_disk) |d| @min(d, self.pending_cut orelse d) else self.pending_cut orelse return;
        log.warn("durable log: finishing a truncation after index {d} that did not complete", .{after_index});
        // Reads since the failed cut may have cached files this pass rewrites.
        self.invalidate();
        const files = try self.listSegments(false);
        defer self.freeSegments(files);
        try self.applyCut(files, after_index);
        // A file whose header wouldn't read was above the cut, or applyCut
        // refused: it is gone now, and so is the damage it named.
        self.bad_header_len = 0;
        try self.clearIntent();
        self.invalidate();
        self.pending_cut = null;
    }

    pub const Dropped = struct { files: usize = 0, bytes: u64 = 0 };

    /// Delete the sealed segments wholly at or below `through`, oldest
    /// first, each unlink synced before the next: a crash leaves the newer
    /// files contiguous, and one whose unlink was lost is dropped again
    /// next time. A segment straddling `through` is kept. Refused above
    /// the watermark, the commit a boot trusts; at or past the newest
    /// segment's end, since that segment carries the watermark; and while a
    /// cut is pending, since the files then disagree with the log. What it
    /// drops is gone: a snapshot must already cover `through`.
    pub fn dropThrough(self: *DurableLog, through: u64) !Dropped {
        if (self.pending_cut != null) return error.CutPending;
        if (through > try self.watermark()) return error.AboveWatermark;
        const files = try self.listSegments(true);
        defer self.freeSegments(files);
        if (files.len > 0 and through >= files[files.len - 1].last_index) return error.WouldDropNewest;
        self.invalidate();
        var dropped: Dropped = .{};
        for (files) |f| {
            if (f.last_index > through) break;
            const size = if (stdx.fs.statFile(f.path, .{})) |st| st.size else |_| 0;
            try stdx.fs.deleteFile(f.path);
            try stdx.fs.syncDir(self.segs_path);
            dropped.files += 1;
            dropped.bytes += size;
        }
        if (dropped.files > 0) log.info("durable log: dropped {d} segment(s), {d} bytes, through index {d}", .{ dropped.files, dropped.bytes, through });
        return dropped;
    }

    fn applyCut(self: *DurableLog, files: []SegmentFile, after_index: u64) !void {
        // The rewrite needs room for a second copy; it goes first so a full
        // disk fails before anything is deleted.
        var rewritten: usize = 0;
        var deleted: usize = 0;
        for (files) |f| {
            if (f.first_index <= after_index and f.last_index > after_index) {
                if (f.unreadable) return error.SegmentUnreadable;
                try self.rewriteBelow(f, after_index);
                rewritten += 1;
            }
        }
        for (files) |f| {
            if (f.first_index > after_index) {
                try stdx.fs.deleteFile(f.path);
                deleted += 1;
            }
        }
        // The intent is cleared next; if these unlinks were still only in
        // memory, a power loss would bring the deleted segments back with no
        // intent left to delete them again. Synced even when this pass
        // deleted nothing: a retry finds the files already gone after an
        // earlier pass unlinked them and then failed to sync.
        try stdx.fs.syncDir(self.segs_path);
        log.info("durable log: truncated after index {d}: {d} segment(s) rewritten, {d} deleted", .{ after_index, rewritten, deleted });
    }

    fn rewriteBelow(self: *DurableLog, f: SegmentFile, after_index: u64) !void {
        const opened = try SegmentReader.initFromFile(self.allocator, f.path);
        defer self.allocator.free(opened.buf);
        var w = SegmentWriter.init(self.allocator, opened.reader.header.partition_id);
        defer w.deinit();
        w.commit_index_at_seal = @min(f.commit_index_at_seal, after_index);
        var offset: usize = 0;
        const data_len = opened.reader.data_end - opened.reader.data_start;
        while (offset < data_len) {
            const e = opened.reader.readEntryAt(offset) orelse break;
            if (e.header.index > after_index) break;
            try w.addEntry(&e);
            offset += e.totalSize();
        }
        // Same first_index, same filename: the rename replaces the file.
        try w.writeToFile(self.segs_path);
    }

    /// The sorted listing, cached until the next flush or truncation.
    pub fn segments(self: *DurableLog) ![]const SegmentFile {
        if (self.listing == null) self.listing = try self.listSegments(true);
        return self.listing.?;
    }

    /// Sealed segments in index order, read fresh from the directory.
    /// Two files holding the same index means a truncation never finished
    /// and replay would pick one silently; `strict` makes that an error, and
    /// a header that won't read too. Recovery is not strict, because it is
    /// there to fix exactly that: it lists a file whose header won't read as
    /// `unreadable`, by its filename, so a cut below it can delete it (the
    /// name is still recorded for `badHeaderSegment`). Caller frees with
    /// `freeSegments`.
    pub fn listSegments(self: *DurableLog, strict: bool) ![]SegmentFile {
        var files: std.ArrayListUnmanaged(SegmentFile) = .empty;
        errdefer {
            for (files.items) |f| self.allocator.free(f.path);
            files.deinit(self.allocator);
        }
        var dir = stdx.fs.openDir(self.segs_path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return files.toOwnedSlice(self.allocator),
            else => return err,
        };
        defer stdx.fs.closeDir(dir);
        const io = stdx.io.instance();
        var iter = dir.iterate();
        while (try iter.next(io)) |de| {
            if (de.kind != .file) continue;
            if (!std.mem.endsWith(u8, de.name, ".flseg")) continue;
            const path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.segs_path, de.name });
            errdefer self.allocator.free(path);
            const hdr = readHeader(path) catch |err| {
                if (err == error.InvalidVersion) {
                    log.err("durable log: {s} was written by a flo whose stored format this build can't read; start from an empty data directory", .{path});
                } else log.err("durable log: cannot read the header of {s}: {s}", .{ path, @errorName(err) });
                const kept = @min(de.name.len, self.bad_header.len);
                @memcpy(self.bad_header[0..kept], de.name[0..kept]);
                self.bad_header_len = kept;
                if (strict) return err;
                const digits = de.name[0 .. de.name.len - ".flseg".len];
                const first = std.fmt.parseInt(u64, digits, 10) catch return err;
                try files.append(self.allocator, .{
                    .first_index = first,
                    .last_index = std.math.maxInt(u64),
                    .commit_index_at_seal = 0,
                    .unreadable = true,
                    .path = path,
                });
                continue;
            };
            try files.append(self.allocator, .{
                .first_index = hdr.first_index,
                .last_index = hdr.last_index,
                .commit_index_at_seal = hdr.commit_index_at_seal,
                .holds_skip = hdr.flags & segment.FLAG_HOLDS_SKIP != 0,
                .path = path,
            });
        }
        std.mem.sort(SegmentFile, files.items, {}, lessByFirstIndex);
        if (strict and files.items.len > 1) {
            for (files.items[1..], 0..) |f, i| {
                const prev = files.items[i];
                if (f.first_index <= prev.last_index) {
                    log.err("durable log: {s} and {s} both hold index {d}; an earlier truncation did not finish and this data directory cannot be replayed safely", .{ prev.path, f.path, f.first_index });
                    return error.OverlappingSegments;
                }
            }
        }
        return files.toOwnedSlice(self.allocator);
    }

    /// The segment `listSegments` stopped at because its header would not
    /// read.
    pub fn badHeaderSegment(self: *const DurableLog) []const u8 {
        return self.bad_header[0..self.bad_header_len];
    }

    /// Delete the `.tmp` files flushes left behind. A segment is written to
    /// `<name>.flseg.tmp` and renamed into place only once it is on disk,
    /// so one still named `.tmp` is a flush a crash cut short: never part
    /// of the log, and the normal tail of an unclean shutdown. Returns how
    /// many went.
    pub fn removeTornTail(self: *DurableLog) !usize {
        const io = stdx.io.instance();
        var removed: usize = 0;
        // One per pass: a directory is not iterated while it changes.
        while (true) {
            var dir = stdx.fs.openDir(self.segs_path, .{ .iterate = true }) catch |err| switch (err) {
                error.FileNotFound => return removed,
                else => return err,
            };
            defer stdx.fs.closeDir(dir);
            var path_buf: [512]u8 = undefined;
            var name_buf: [256]u8 = undefined;
            var found: ?[]const u8 = null;
            var iter = dir.iterate();
            while (try iter.next(io)) |de| {
                if (de.kind != .file or !std.mem.endsWith(u8, de.name, ".flseg.tmp")) continue;
                found = std.fmt.bufPrint(&name_buf, "{s}", .{de.name}) catch return error.PathTooLong;
                break;
            }
            const name = found orelse return removed;
            const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ self.segs_path, name }) catch return error.PathTooLong;
            try stdx.fs.deleteFile(path);
            log.info("durable log: removed {s}, a segment flush an unclean shutdown cut short (never part of the log)", .{name});
            removed += 1;
        }
    }

    pub fn freeSegments(self: *DurableLog, files: []SegmentFile) void {
        for (files) |f| self.allocator.free(f.path);
        self.allocator.free(files);
    }

    fn invalidate(self: *DurableLog) void {
        if (self.listing) |l| {
            self.freeSegments(l);
            self.listing = null;
        }
        if (self.open) |o| {
            self.allocator.free(o.buf);
            self.open = null;
        }
    }

    /// The open segment covering `index`, opening (and caching) it if the
    /// one held does not. Null when no sealed segment holds the index.
    fn openCovering(self: *DurableLog, index: u64) !?*const Open {
        if (self.open) |*o| {
            if (index >= o.first_index and index <= o.last_index) return o;
        }
        const files = try self.segments();
        for (files) |f| {
            if (index < f.first_index or index > f.last_index) continue;
            const opened = try SegmentReader.initFromFile(self.allocator, f.path);
            if (self.open) |o| self.allocator.free(o.buf);
            self.open = .{ .first_index = f.first_index, .last_index = f.last_index, .buf = opened.buf, .reader = opened.reader };
            return &self.open.?;
        }
        return null;
    }

    fn intentPath(self: *DurableLog, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "{s}/{s}", .{ self.segs_path, INTENT_FILENAME }) catch error.PathTooLong;
    }

    fn writeIntent(self: *DurableLog, after_index: u64) !void {
        var path_buf: [512]u8 = undefined;
        const path = try self.intentPath(&path_buf);
        var tmp_buf: [520]u8 = undefined;
        const tmp_path = std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{path}) catch return error.PathTooLong;
        var text: [24]u8 = undefined;
        const line = std.fmt.bufPrint(&text, "{d}\n", .{after_index}) catch unreachable;
        const file = try stdx.fs.createFile(tmp_path, .{});
        defer stdx.fs.closeFile(file);
        try stdx.fs.writeAll(file, line);
        try stdx.fs.sync(file);
        try stdx.fs.renameDurable(tmp_path, path);
    }

    fn clearIntent(self: *DurableLog) !void {
        var path_buf: [512]u8 = undefined;
        const path = try self.intentPath(&path_buf);
        stdx.fs.deleteFile(path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }

    fn readIntent(self: *DurableLog) !?u64 {
        var path_buf: [512]u8 = undefined;
        const path = try self.intentPath(&path_buf);
        var text: [32]u8 = undefined;
        const read = stdx.fs.readFile(path, &text) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        const trimmed = std.mem.trim(u8, read, " \n\r\t");
        return std.fmt.parseInt(u64, trimmed, 10) catch return error.CorruptTruncateIntent;
    }

    fn lessByFirstIndex(_: void, a: SegmentFile, b: SegmentFile) bool {
        return a.first_index < b.first_index;
    }
};

/// Read only the 64-byte header: listing a directory of large segments must
/// not read them.
fn readHeader(path: []const u8) !segment.SegmentHeader {
    const file = try stdx.fs.openFile(path, .{});
    defer stdx.fs.closeFile(file);
    var buf: [segment.HEADER_SIZE]u8 = undefined;
    const n = try stdx.fs.readAll(file, &buf);
    if (n != buf.len) return error.TooSmall;
    const hdr: *const segment.SegmentHeader = @ptrCast(@alignCast(&buf));
    if (!std.mem.eql(u8, &hdr.magic, &segment.HEADER_MAGIC)) return error.InvalidHeaderMagic;
    if (hdr.version != segment.SEGMENT_VERSION) return error.InvalidVersion;
    return hdr.*;
}

/// Copy entries at or above `start` out of a self-delimiting byte run.
fn copyFrom(data: []const u8, start: u64, buf: []Entry, arena: []u8) usize {
    var offset: usize = 0;
    var count: usize = 0;
    var arena_used: usize = 0;
    while (offset < data.len and count < buf.len) {
        const e = Entry.deserialize(data[offset..]) orelse break;
        offset += e.totalSize();
        if (e.header.index < start) continue;
        if (e.header.index != start + count) break;
        if (arena_used + e.payload.len > arena.len) break;
        @memcpy(arena[arena_used..][0..e.payload.len], e.payload);
        buf[count] = .{ .header = e.header, .payload = arena[arena_used..][0..e.payload.len] };
        arena_used += e.payload.len;
        count += 1;
    }
    return count;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn testEntry(index: u64, term: u64, payload: []const u8) Entry {
    var e = entry_mod.buildEntry(.stream_append, 0, term, index, index * 1000, payload);
    e.header.crc32c = e.computeCrc();
    return e;
}

const TestDir = struct {
    tmp: testing.TmpDir,
    path: []const u8,
    fn init() !TestDir {
        var tmp = testing.tmpDir(.{});
        errdefer tmp.cleanup();
        const real = try stdx.fs.dirRealpathAlloc(tmp.dir, testing.allocator, ".");
        errdefer testing.allocator.free(real);
        const path = try std.fmt.allocPrint(testing.allocator, "{s}/segs", .{real});
        testing.allocator.free(real);
        try stdx.fs.makePath(path);
        return .{ .tmp = tmp, .path = path };
    }
    fn deinit(self: *TestDir) void {
        testing.allocator.free(self.path);
        self.tmp.cleanup();
    }
};

fn fileNames(dir_path: []const u8, out: *std.ArrayListUnmanaged([]const u8)) !void {
    var dir = try stdx.fs.openDir(dir_path, .{ .iterate = true });
    defer stdx.fs.closeDir(dir);
    const io = stdx.io.instance();
    var iter = dir.iterate();
    while (try iter.next(io)) |de| {
        try out.append(testing.allocator, try testing.allocator.dupe(u8, de.name));
    }
    std.mem.sort([]const u8, out.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
}

test "durable log: watermark is the highest commit index any segment was sealed under" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();

    try testing.expectEqual(@as(u64, 0), try dl.watermark());
    try writer.addEntry(&testEntry(1, 1, "a"));
    try writer.addEntry(&testEntry(2, 1, "b"));
    try dl.flush(2);
    try writer.addEntry(&testEntry(3, 1, "c"));
    try writer.addEntry(&testEntry(4, 1, "d"));
    try dl.flush(3);
    try testing.expectEqual(@as(u64, 3), try dl.watermark());
}

test "durable log: a segment holding a repair skip says so in its header, and a cut that drops it doesn't" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();

    try writer.addEntry(&testEntry(1, 1, "a"));
    try dl.flush(1);
    try writer.addEntry(&testEntry(2, 1, "b"));
    var skip = entry_mod.buildEntry(.repair_skip, 0, 1, 3, 3000, "s");
    skip.header.crc32c = skip.computeCrc();
    try writer.addEntry(&skip);
    try dl.flush(3);
    {
        const files = try dl.listSegments(true);
        defer dl.freeSegments(files);
        try testing.expect(!files[0].holds_skip);
        try testing.expect(files[1].holds_skip);
    }
    // Rewritten below the skip, the segment no longer holds one.
    try dl.truncateAfter(2);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(u64, 2), files[1].last_index);
    try testing.expect(!files[1].holds_skip);
    // So does a cut inside the writer's buffer.
    var buffered = skip;
    buffered.header.index = 4;
    try writer.addEntry(&buffered);
    try testing.expect(writer.holds_skip);
    writer.truncateAfter(3);
    try testing.expect(!writer.holds_skip);
}

test "durable log: readRange serves the writer buffer, then each sealed segment" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();

    try writer.addEntry(&testEntry(1, 1, "one"));
    try writer.addEntry(&testEntry(2, 1, "two"));
    try writer.addEntry(&testEntry(3, 1, "three"));
    try dl.flush(3);
    try writer.addEntry(&testEntry(4, 2, "four"));
    try writer.addEntry(&testEntry(5, 2, "five"));

    var buf: [8]Entry = undefined;
    var arena: [256]u8 = undefined;
    // From inside the sealed segment: the rest of that segment, not beyond.
    var n = dl.readRange(2, &buf, &arena);
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqual(@as(u64, 2), buf[0].header.index);
    try testing.expectEqualStrings("two", buf[0].payload);
    try testing.expectEqual(@as(u64, 3), buf[1].header.index);
    try testing.expectEqualStrings("three", buf[1].payload);
    // From the writer buffer, with the terms the entries carry.
    n = dl.readRange(4, &buf, &arena);
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqual(@as(u64, 2), buf[0].header.term);
    try testing.expectEqualStrings("five", buf[1].payload);
    // Nothing holds index 6.
    try testing.expectEqual(@as(usize, 0), dl.readRange(6, &buf, &arena));
    // A one-slot buffer gets exactly one.
    try testing.expectEqual(@as(usize, 1), dl.readRange(1, buf[0..1], &arena));
    try testing.expectEqualStrings("one", buf[0].payload);
}

test "durable log: truncateAfter rewrites the sealed tail under its own name and deletes emptied files" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();

    // Three sealed files (1-3, 4-6, 7-8) and a buffered 9-10, as sync
    // durability leaves them, then a leader change truncates after 5.
    try writer.addEntry(&testEntry(1, 1, "a"));
    try writer.addEntry(&testEntry(2, 1, "b"));
    try writer.addEntry(&testEntry(3, 1, "c"));
    try dl.flush(3);
    try writer.addEntry(&testEntry(4, 1, "d"));
    try writer.addEntry(&testEntry(5, 1, "e"));
    try writer.addEntry(&testEntry(6, 1, "f"));
    try dl.flush(5);
    try writer.addEntry(&testEntry(7, 1, "g"));
    try writer.addEntry(&testEntry(8, 1, "h"));
    try dl.flush(5);
    try writer.addEntry(&testEntry(9, 1, "i"));
    try writer.addEntry(&testEntry(10, 1, "j"));

    // Three directory syncs: the intent, the rewrite of 4-6, and the
    // deletion of 7-8.
    const before = stdx.fs.dir_syncs.load(.monotonic);
    try dl.truncateAfter(5);
    try testing.expectEqual(before + 3, stdx.fs.dir_syncs.load(.monotonic));

    try testing.expectEqual(@as(u32, 0), writer.entry_count);
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (names.items) |nm| testing.allocator.free(nm);
        names.deinit(testing.allocator);
    }
    try fileNames(td.path, &names);
    try testing.expectEqual(@as(usize, 2), names.items.len);
    try testing.expectEqualStrings("0000000001.flseg", names.items[0]);
    try testing.expectEqualStrings("0000000004.flseg", names.items[1]);

    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(u64, 3), files[0].last_index);
    try testing.expectEqual(@as(u64, 5), files[1].last_index);
    try testing.expectEqual(@as(u64, 5), files[1].commit_index_at_seal);

    var buf: [8]Entry = undefined;
    var arena: [256]u8 = undefined;
    try testing.expectEqual(@as(usize, 2), dl.readRange(4, &buf, &arena));
    try testing.expectEqual(@as(usize, 0), dl.readRange(6, &buf, &arena));

    // A new index 6 flushed later lands in its own file and reads back.
    try writer.addEntry(&testEntry(6, 2, "F"));
    try dl.flush(6);
    try testing.expectEqual(@as(usize, 1), dl.readRange(6, &buf, &arena));
    try testing.expectEqual(@as(u64, 2), buf[0].header.term);
    try testing.expectEqual(@as(u64, 6), try dl.watermark());
}

test "durable log: truncateAfter inside the writer buffer keeps the prefix and its sparse index consistent" {
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var i: u64 = 1;
    while (i <= 600) : (i += 1) try writer.addEntry(&testEntry(i, 1, "x"));
    try testing.expectEqual(@as(usize, 3), writer.sparse_index.items.len);

    writer.truncateAfter(300);
    try testing.expectEqual(@as(u32, 300), writer.entry_count);
    try testing.expectEqual(@as(u64, 300), writer.last_index);
    try testing.expectEqual(@as(usize, 2), writer.sparse_index.items.len);

    // Sealing and reading back finds every kept index and nothing above.
    const sealed = try writer.seal();
    defer testing.allocator.free(sealed);
    const reader = try SegmentReader.init(sealed);
    try testing.expect(reader.findByIndex(300) != null);
    try testing.expect(reader.findByIndex(257) != null);
    try testing.expect(reader.findByIndex(301) == null);

    writer.truncateAfter(0);
    try testing.expectEqual(@as(u32, 0), writer.entry_count);
}

test "durable log: dropThrough deletes whole segments oldest first, keeps one straddling the index, and stays done after a reopen" {
    var td = try TestDir.init();
    defer td.deinit();
    {
        var writer = SegmentWriter.init(testing.allocator, 0);
        defer writer.deinit();
        var dl = try DurableLog.init(testing.allocator, &writer, td.path);
        defer dl.deinit();
        // Sealed 1-3, 4-6 and 7-8, under commits 3, 6 and 8.
        for (1..9) |i| {
            try writer.addEntry(&testEntry(i, 1, "x"));
            if (i == 3 or i == 6 or i == 8) try dl.flush(i);
        }
        // Nothing above the watermark goes, the newest segment always stays,
        // and a pending cut stops it.
        try testing.expectError(error.AboveWatermark, dl.dropThrough(9));
        try testing.expectError(error.WouldDropNewest, dl.dropThrough(8));
        dl.pending_cut = 8;
        try testing.expectError(error.CutPending, dl.dropThrough(5));
        dl.pending_cut = null;

        // Through 5: 1-3 goes, 4-6 straddles and stays; one directory
        // sync per file.
        const before = stdx.fs.dir_syncs.load(.monotonic);
        const dropped = try dl.dropThrough(5);
        try testing.expectEqual(@as(usize, 1), dropped.files);
        try testing.expect(dropped.bytes > 0);
        try testing.expectEqual(before + 1, stdx.fs.dir_syncs.load(.monotonic));
        var buf: [8]Entry = undefined;
        var arena: [256]u8 = undefined;
        try testing.expectEqual(@as(usize, 0), dl.readRange(2, &buf, &arena));
        try testing.expectEqual(@as(usize, 3), dl.readRange(4, &buf, &arena));
    }
    // Reopened: the listing starts at 4, and dropping through 6 takes 4-6.
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(usize, 2), files.len);
    try testing.expectEqual(@as(u64, 4), files[0].first_index);
    try testing.expectEqual(@as(usize, 1), (try dl.dropThrough(6)).files);
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (names.items) |nm| testing.allocator.free(nm);
        names.deinit(testing.allocator);
    }
    try fileNames(td.path, &names);
    try testing.expectEqual(@as(usize, 1), names.items.len);
    try testing.expectEqualStrings("0000000007.flseg", names.items[0]);
}

test "durable log: a truncation the previous run did not finish is completed before replay" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();

    // Files 1-3 and 4-5 exist; a cut after 2 was recorded and then the
    // process died before touching either file.
    try writer.addEntry(&testEntry(1, 1, "a"));
    try writer.addEntry(&testEntry(2, 1, "b"));
    try writer.addEntry(&testEntry(3, 1, "c"));
    try dl.flush(3);
    try writer.addEntry(&testEntry(4, 1, "d"));
    try writer.addEntry(&testEntry(5, 1, "e"));
    try dl.flush(5);
    try dl.writeIntent(2);

    try dl.recoverTruncation();

    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(usize, 1), files.len);
    try testing.expectEqual(@as(u64, 2), files[0].last_index);
    try testing.expectEqual(@as(u64, 2), files[0].commit_index_at_seal);
    try testing.expect((try dl.readIntent()) == null);
    // Idempotent: a second recovery finds nothing to do.
    try dl.recoverTruncation();
}

test "durable log: a segment from another stored format refuses to be listed" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    try writer.addEntry(&testEntry(1, 1, "a"));
    try dl.flush(1);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);

    // The segment as a build with the previous format wrote it.
    const bytes = try stdx.fs.readFileAlloc(testing.allocator, files[0].path, 1 << 20);
    defer testing.allocator.free(bytes);
    std.mem.writeInt(u16, bytes[@offsetOf(segment.SegmentHeader, "version")..][0..2], segment.SEGMENT_VERSION - 1, .little);
    const file = try stdx.fs.createFile(files[0].path, .{});
    defer stdx.fs.closeFile(file);
    try stdx.fs.writeAll(file, bytes);
    try testing.expectError(error.InvalidVersion, dl.listSegments(true));
}

test "durable log: two segments holding the same index refuse to be listed" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();

    try writer.addEntry(&testEntry(1, 1, "a"));
    try writer.addEntry(&testEntry(2, 1, "b"));
    try writer.addEntry(&testEntry(3, 1, "c"));
    try dl.flush(3);
    // A second file starting inside the first one's range: an unfinished
    // cut whose intent file was lost.
    var stray = SegmentWriter.init(testing.allocator, 0);
    defer stray.deinit();
    try stray.addEntry(&testEntry(3, 2, "C"));
    try stray.writeToFile(td.path);

    try testing.expectError(error.OverlappingSegments, dl.listSegments(true));
    try testing.expectError(error.OverlappingSegments, dl.watermark());
    // Recovery lists without the check, since it exists to repair this.
    const files = try dl.listSegments(false);
    dl.freeSegments(files);
}

test "durable log: the cached listing and open segment are dropped by a flush and a truncation" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();

    try writer.addEntry(&testEntry(1, 1, "a"));
    try writer.addEntry(&testEntry(2, 1, "b"));
    try dl.flush(2);
    var buf: [4]Entry = undefined;
    var arena: [64]u8 = undefined;
    try testing.expectEqual(@as(usize, 2), dl.readRange(1, &buf, &arena));
    try testing.expect(dl.open != null);

    // A new file must be visible to the next read.
    try writer.addEntry(&testEntry(3, 1, "c"));
    try dl.flush(3);
    try testing.expectEqual(@as(usize, 1), dl.readRange(3, &buf, &arena));
    try testing.expectEqualStrings("c", buf[0].payload);

    // A cut inside the open segment must not be served from the old copy.
    try dl.truncateAfter(1);
    try testing.expectEqual(@as(usize, 0), dl.readRange(2, &buf, &arena));
    try testing.expectEqual(@as(usize, 1), dl.readRange(1, &buf, &arena));
}

test "durable log: a truncation that cannot reach the files wedges flushing until it does" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();

    try writer.addEntry(&testEntry(1, 1, "a"));
    try writer.addEntry(&testEntry(2, 1, "b"));
    try writer.addEntry(&testEntry(3, 1, "c"));
    try dl.flush(3);

    // The directory refuses new files, so not even the intent can be
    // recorded; the cut is still remembered.
    const path_z = try testing.allocator.dupeSentinel(u8, td.path, 0);
    defer testing.allocator.free(path_z);
    defer _ = std.c.chmod(path_z, 0o700);
    try testing.expectEqual(@as(c_int, 0), std.c.chmod(path_z, 0o500));
    try testing.expectError(error.AccessDenied, dl.truncateAfter(2));
    try testing.expectEqual(@as(?u64, 2), dl.pending_cut);
    try testing.expectEqual(@as(u64, 1), dl.truncate_failures);
    try writer.addEntry(&testEntry(3, 2, "C"));
    try testing.expectError(error.TruncationIncomplete, dl.flush(3));
    try testing.expectEqual(@as(?u64, 2), dl.pending_cut);
    // The file still holds the discarded 3; a read below the ring must not
    // hand it out while the cut is pending.
    var stale: [4]Entry = undefined;
    var stale_arena: [64]u8 = undefined;
    try testing.expectEqual(@as(usize, 2), dl.readRange(1, &stale, &stale_arena));
    // Index 3 itself comes from the buffer, the replacement, not the file.
    try testing.expectEqual(@as(usize, 1), dl.readRange(3, &stale, &stale_arena));
    try testing.expectEqual(@as(u64, 2), stale[0].header.term);

    // Once the cut can complete, the next flush finishes it first. The
    // buffered replacement was appended after the cut, so it survives.
    try testing.expectEqual(@as(c_int, 0), std.c.chmod(path_z, 0o700));
    try dl.flush(3);
    try testing.expect(dl.pending_cut == null);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(usize, 2), files.len);
    try testing.expectEqual(@as(u64, 2), files[0].last_index);
    try testing.expectEqual(@as(u64, 3), files[1].first_index);
}

test "durable log: a cut that fails after its intent is recorded is finished by the next flush" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();

    try writer.addEntry(&testEntry(1, 1, "a"));
    try writer.addEntry(&testEntry(2, 1, "b"));
    try writer.addEntry(&testEntry(3, 1, "c"));
    try dl.flush(3);
    try writer.addEntry(&testEntry(4, 1, "d"));
    try dl.flush(4);

    // Damage the spanning file's checksum so its rewrite fails after the
    // intent is on disk and before anything is deleted.
    const spanning = try std.fmt.allocPrint(testing.allocator, "{s}/0000000001.flseg", .{td.path});
    defer testing.allocator.free(spanning);
    var bytes = try stdx.fs.readFileAlloc(testing.allocator, spanning, 1 << 20);
    defer testing.allocator.free(bytes);
    const saved = bytes[segment.HEADER_SIZE + 60];
    bytes[segment.HEADER_SIZE + 60] ^= 0xff;
    {
        const f = try stdx.fs.createFile(spanning, .{});
        defer stdx.fs.closeFile(f);
        try stdx.fs.writeAll(f, bytes);
    }
    try testing.expectError(error.InvalidCrc, dl.truncateAfter(2));
    try testing.expectEqual(@as(?u64, 2), dl.pending_cut);
    try testing.expectEqual(@as(u64, 2), (try dl.readIntent()).?);
    // Nothing was deleted before the rewrite failed.
    const before = try dl.listSegments(false);
    try testing.expectEqual(@as(usize, 2), before.len);
    dl.freeSegments(before);

    // Repaired; the next flush completes the cut, then lands its file.
    bytes[segment.HEADER_SIZE + 60] = saved;
    {
        const f = try stdx.fs.createFile(spanning, .{});
        defer stdx.fs.closeFile(f);
        try stdx.fs.writeAll(f, bytes);
    }
    try writer.addEntry(&testEntry(3, 2, "C"));
    try dl.flush(3);
    try testing.expect(dl.pending_cut == null);
    try testing.expect((try dl.readIntent()) == null);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(usize, 2), files.len);
    try testing.expectEqual(@as(u64, 2), files[0].last_index);
    try testing.expectEqual(@as(u64, 3), files[1].first_index);
    try testing.expectEqual(@as(u64, 3), files[1].last_index);
}

test "durable log: a cut that cannot even list the files is remembered" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    try writer.addEntry(&testEntry(1, 1, "a"));
    try writer.addEntry(&testEntry(2, 1, "b"));
    try writer.addEntry(&testEntry(3, 1, "c"));
    try dl.flush(3);

    const path_z = try testing.allocator.dupeSentinel(u8, td.path, 0);
    defer testing.allocator.free(path_z);
    defer _ = std.c.chmod(path_z, 0o700);
    try testing.expectEqual(@as(c_int, 0), std.c.chmod(path_z, 0o000));
    try testing.expect(std.meta.isError(dl.truncateAfter(2)));
    try testing.expectEqual(@as(?u64, 2), dl.pending_cut);
    try testing.expectEqual(@as(c_int, 0), std.c.chmod(path_z, 0o700));
    try dl.recoverTruncation();
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(u64, 2), files[0].last_index);
}

test "durable log: a cut retried after its deletes already happened still syncs the directory before clearing the intent" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();

    // A cut after 3 whose only work was deleting 4-5: that delete happened,
    // the directory sync after it failed, and the intent is still on disk.
    try writer.addEntry(&testEntry(1, 1, "a"));
    try writer.addEntry(&testEntry(2, 1, "b"));
    try writer.addEntry(&testEntry(3, 1, "c"));
    try dl.flush(3);
    try dl.writeIntent(3);

    const before = stdx.fs.dir_syncs.load(.monotonic);
    try dl.recoverTruncation();
    try testing.expectEqual(before + 1, stdx.fs.dir_syncs.load(.monotonic));
    try testing.expect((try dl.readIntent()) == null);
}

fn corruptHeader(path: []const u8) !void {
    const bytes = try stdx.fs.readFileAlloc(testing.allocator, path, 1 << 20);
    defer testing.allocator.free(bytes);
    bytes[0] ^= 0xff;
    const file = try stdx.fs.createFile(path, .{});
    defer stdx.fs.closeFile(file);
    try stdx.fs.writeAll(file, bytes);
}

test "durable log: a salvage deletes a file above the cut whose header won't read, and refuses one it would rewrite" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    for ([_]u64{ 1, 2, 3, 4, 5, 6 }) |i| {
        try writer.addEntry(&testEntry(i, 1, "x"));
        if (i % 2 == 0) try dl.flush(i);
    }
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (names.items) |n| testing.allocator.free(n);
        names.deinit(testing.allocator);
    }
    try fileNames(td.path, &names);
    var path_buf: [512]u8 = undefined;
    // Entries 3-4 sit in a file whose header won't read.
    try corruptHeader(try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ td.path, names.items[1] }));
    try testing.expectError(error.InvalidHeaderMagic, dl.listSegments(true));

    // A cut that would rewrite it is refused, and nothing is recorded: a
    // boot would otherwise stop on it.
    try testing.expectError(error.SegmentUnreadable, dl.salvageAfter(3));
    try testing.expect((try dl.readIntent()) == null);

    try dl.salvageAfter(2);
    try testing.expectEqual(@as(usize, 0), dl.badHeaderSegment().len);
    try testing.expect((try dl.readIntent()) == null);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(usize, 1), files.len);
    try testing.expectEqual(@as(u64, 2), files[0].last_index);
    try testing.expectEqual(@as(u64, 2), try dl.watermark());
}

test "durable log: a salvage below a pending cut records its own, one above keeps the lower, and a refused one leaves the pending cut as it was" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    for ([_]u64{ 1, 2, 3, 4, 5, 6 }) |i| {
        try writer.addEntry(&testEntry(i, 1, "x"));
        if (i % 2 == 0) try dl.flush(i);
    }
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (names.items) |n| testing.allocator.free(n);
        names.deinit(testing.allocator);
    }
    try fileNames(td.path, &names);
    var path_buf: [512]u8 = undefined;

    // Pending at 5; a salvage after 4 cuts there, after 6 keeps the 5.
    try dl.writeIntent(5);
    try dl.salvageAfter(6);
    try testing.expectEqual(@as(u64, 5), try dl.watermark());
    try dl.writeIntent(5);
    try dl.salvageAfter(4);
    try testing.expectEqual(@as(u64, 4), try dl.watermark());

    // Refused on a damaged file it would rewrite: the pending cut stays.
    try corruptHeader(try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ td.path, names.items[0] }));
    try dl.writeIntent(3);
    try testing.expectError(error.SegmentUnreadable, dl.salvageAfter(1));
    try testing.expectEqual(@as(?u64, 3), try dl.readIntent());
}

test "durable log: a recovery listing refuses a damaged file whose name is not an index" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    var path_buf: [512]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/backup.flseg", .{td.path});
    const file = try stdx.fs.createFile(path, .{});
    stdx.fs.closeFile(file);
    try testing.expectError(error.TooSmall, dl.listSegments(false));
}
