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
const PositionedSegment = @import("ual/reader.zig").PositionedSegment;
const checksum_mod = @import("../util/checksum.zig");
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
    /// Sealed: never appended to again. The newest segment is the one that
    /// may not be.
    sealed: bool = true,
    /// The header would not read; listed only by a recovery listing, with
    /// first_index from the filename and the rest unknown.
    unreadable: bool = false,
    path: []const u8,
};

/// The newest segment, open for appends: each flush adds its entries, syncs
/// them and records them in a mark. At `max_segment_bytes` it is sealed.
pub const Active = struct {
    file: std.Io.File,
    fixed: segment.Fixed,
    first_index: u64,
    last_index: u64,
    last_ts_ns: u64,
    data_size: u64,
    entry_count: u32,
    flags: u8,
    commit_index: u64,
    /// The newest mark's sequence number and slot; the next goes in the
    /// other slot.
    seq: u32,
    slot: u1,
    /// Every 256th entry, as a sealed segment's index has them, kept as the
    /// entries are appended: a read seeks by it, and the seal writes it.
    sparse: std.ArrayListUnmanaged(segment.SparseIndexEntry) = .empty,
    /// The indexes of the repair skips it holds, so a cut knows whether any
    /// remain.
    skips: std.ArrayListUnmanaged(u64) = .empty,
    bitmap: u32,
    /// The data's CRC so far, which the seal's footer needs.
    crc: checksum_mod.ChecksumStream,
    /// False after a cut, which can't take bytes back out of a running CRC:
    /// the seal then reads the data back to sum it.
    crc_whole: bool = true,

    fn view(self: *Active) PositionedSegment {
        return .{
            .file = self.file,
            .first_index = self.first_index,
            .last_index = self.last_index,
            .data_start = segment.HEADER_SIZE,
            .data_end = segment.HEADER_SIZE + self.data_size,
            .sparse = self.sparse.items,
        };
    }

    fn close(self: *Active, allocator: std.mem.Allocator) void {
        stdx.fs.closeFile(self.file);
        self.sparse.deinit(allocator);
        self.skips.deinit(allocator);
    }

    /// Count an entry just appended at data offset `offset`. Capacity is
    /// reserved first (`reserve`), so this never fails.
    fn note(self: *Active, e: *const Entry, offset: u64) void {
        if (self.entry_count % segment.SPARSE_INDEX_INTERVAL == 0) self.sparse.appendAssumeCapacity(.{ .index = e.header.index, .offset = @intCast(offset) });
        if (e.header.entry_type == @backingInt(entry_mod.EntryType.repair_skip)) {
            self.skips.appendAssumeCapacity(e.header.index);
            self.flags |= segment.FLAG_HOLDS_SKIP;
        }
        self.entry_count += 1;
        self.last_index = e.header.index;
        self.last_ts_ns = e.header.timestamp_ns;
        self.bitmap = segment.bitmapSet(self.bitmap, e.header.entry_type);
    }

    fn reserve(self: *Active, allocator: std.mem.Allocator, entries: u32) !void {
        try self.sparse.ensureUnusedCapacity(allocator, entries / segment.SPARSE_INDEX_INTERVAL + 1);
        try self.skips.ensureUnusedCapacity(allocator, entries);
    }

    fn mark(self: *const Active, seq: u32) segment.Mark {
        return .{
            .seq = seq,
            .flags = self.flags,
            .data_size = @intCast(self.data_size),
            .entry_count = self.entry_count,
            .last_index = self.last_index,
            .last_ts_ns = self.last_ts_ns,
            .commit_index = self.commit_index,
            .crc32c = 0,
        };
    }
};

pub const DurableLog = struct {
    allocator: std.mem.Allocator,
    writer: *SegmentWriter,
    segs_path: []const u8,
    /// The newest segment, open for appends, once a flush or a read below
    /// the ring has opened it.
    active: ?Active = null,
    /// An active segment is sealed once its data reaches this; tests lower it.
    max_segment_bytes: u64 = segment.DEFAULT_MAX_SEGMENT_SIZE,
    /// Flushes appended, and segments created, for tests and benches.
    appends: u64 = 0,
    creates: u64 = 0,
    /// Truncations that did not reach every file; each one is a replay
    /// that may apply a suffix the leader discarded.
    truncate_failures: u64 = 0,
    /// Truncations begun: a read made elsewhere under an older count may
    /// hold entries a cut has since dropped, and is not used.
    truncations: u64 = 0,
    /// Told of each cut, as it is made: what reads of this log kept above it
    /// may have been replaced.
    on_cut: ?Cut = null,
    /// The cut the files do not yet reflect, held here because the intent
    /// file may itself be what failed to write. No flush happens while it
    /// is set: a new file above the cut would be indistinguishable, at the
    /// next boot, from one the cut should delete. A flush or a truncation
    /// retries the cut first.
    pending_cut: ?u64 = null,
    /// The sorted listing, valid until the next flush or truncation.
    listing: ?[]SegmentFile = null,
    /// Directory listings made, for tests.
    listings: u64 = 0,
    /// The segment whose header would not read, when listing failed on it.
    bad_header: [128]u8 = undefined,
    bad_header_len: usize = 0,
    /// Rate-limits the line said when a segment header can't be read now: a
    /// blocked catch-up re-lists on every retry, so without this it floods.
    /// Null until it is first said, so a block within the first interval of
    /// uptime is not held silent.
    block_warn_ms: ?u64 = null,
    /// Tests only: an error `openCovering` returns when a segment must actually
    /// be opened, standing in for a resource or IO failure from the OS without
    /// provoking a real one. Null in production.
    test_open_error: ?anyerror = null,
    /// The segment last read below the ring, kept open: a deferred tail
    /// reads one entry per call, and re-opening it each time would read its
    /// sparse index again.
    open: ?PositionedSegment = null,
    /// Where reads below the ring land before their entries are copied
    /// out; allocated on the first such read.
    scratch: []u8 = &.{},
    /// Where a read from `start` would come from, for a reader on another
    /// thread: null when the writer's buffer holds it (that read stays here),
    /// or when nothing durable does. The path is copied into `path_buf`;
    /// `limit` is how many entries from `start` it may return, short of a
    /// pending cut.
    pub fn sealedSource(self: *DurableLog, start: u64, path_buf: []u8) !?struct { path: []const u8, limit: u64 } {
        const w = self.writer;
        if (w.entry_count > 0 and start >= w.first_index and start <= w.last_index) return null;
        if (self.pending_cut) |cut| if (start > cut) return null;
        const f = covering(try self.segments(), start) orelse return null;
        // The active segment is read here, where its appends are made.
        if (f.unreadable or !f.sealed) return null;
        if (f.path.len > path_buf.len) return error.PathTooLong;
        @memcpy(path_buf[0..f.path.len], f.path);
        const end = if (self.pending_cut) |cut| @min(cut, f.last_index) else f.last_index;
        return .{ .path = path_buf[0..f.path.len], .limit = end - start + 1 };
    }

    /// The segment holding `index`, by binary search: a listing is in index
    /// order with no two segments overlapping (`listSegments` refuses an
    /// overlap), so only the last one starting at or below `index` can hold
    /// it. A walk below the ring looks one up per segment, and a sync log
    /// seals one segment per flush, so a scan here would cost the listing's
    /// length on every read.
    fn covering(files: []const SegmentFile, index: u64) ?*const SegmentFile {
        var lo: usize = 0;
        var hi: usize = files.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (files[mid].first_index <= index) lo = mid + 1 else hi = mid;
        }
        if (lo == 0) return null;
        const f = &files[lo - 1];
        return if (index <= f.last_index) f else null;
    }

    /// What a read below the ring came to. `blocked` means it could not be
    /// read right now, kept apart from "it isn't there": a hole is damage, a
    /// blocked read is retried. `disk_failing` marks a blocked read whose
    /// cause (an IO error, or an unmapped errno that may be one) should become
    /// damage if it persists, as against a resource shortage that is expected
    /// to pass on its own.
    /// `permission` marks a disk-class block that is a permission the
    /// operator may fix, which every report says plainly.
    pub const ReadResult = struct { count: usize = 0, blocked: bool = false, disk_failing: bool = false, permission: bool = false };

    /// Enough for a run of small entries per read; a bigger entry is read
    /// straight into the caller's arena.
    const SCRATCH_SIZE = 64 * 1024;

    pub fn init(allocator: std.mem.Allocator, writer: *SegmentWriter, segs_path: []const u8) !DurableLog {
        return .{
            .allocator = allocator,
            .writer = writer,
            .segs_path = try allocator.dupe(u8, segs_path),
        };
    }

    pub fn deinit(self: *DurableLog) void {
        self.closeActive();
        self.invalidate();
        self.allocator.free(self.scratch);
        self.allocator.free(self.segs_path);
    }

    /// Append the buffered entries to the active segment and sync them, then
    /// record them in its mark under the commit index they were flushed
    /// under: one fdatasync, no new file, unless this flush starts a
    /// segment. A segment that reaches `max_segment_bytes` is sealed.
    /// Nothing to flush is not an error.
    pub fn flush(self: *DurableLog, commit_index: u64) !void {
        // A pending cut owes a write even when no new entries do, and its write
        // can fail: try it before the empty-buffer return, so a caller tracking
        // flush failures doesn't see this as a clean flush while the disk is
        // still failing the cut.
        if (self.pending_cut != null) {
            self.recoverTruncation() catch return error.TruncationIncomplete;
        }
        if (self.writer.entry_count == 0) return;
        self.writer.commit_index_at_seal = commit_index;
        const a = try self.activeFor(self.writer.first_index, self.writer.first_ts_ns);
        try self.append(a, commit_index);
        self.writer.reset();
        self.listActive();
        if (a.data_size >= self.max_segment_bytes) self.sealActive() catch |err| {
            // The entries are on disk: the flush stands. The seal is tried
            // again at the next flush, over whatever it left past the data.
            log.warn("durable log: could not seal the segment starting at index {d}: {s}; it is sealed at a later flush", .{ a.first_index, @errorName(err) });
        };
    }

    /// The active segment the buffer starting at `first` goes on: the one
    /// open, the newest on disk if it ends just below `first`, or a new one.
    fn activeFor(self: *DurableLog, first: u64, first_ts_ns: u64) !*Active {
        if (self.active) |*a| {
            if (a.last_index + 1 == first) return a;
            if (first <= a.last_index) {
                log.err("durable log: a flush starting at index {d} overlaps the active segment, which ends at {d}", .{ first, a.last_index });
                return error.FlushOverlaps;
            }
            // A gap the writer left: this segment is done; the next starts
            // past it, as a new file would have before.
            try self.sealActive();
        }
        const files = try self.segments();
        if (files.len > 0) {
            const newest = files[files.len - 1];
            if (!newest.sealed and newest.last_index + 1 == first) {
                try self.openActive(newest.path);
                return &self.active.?;
            }
            if (!newest.sealed) try self.sealFile(newest.path);
        }
        try self.createActive(first, first_ts_ns);
        return &self.active.?;
    }

    /// A new segment for entries from `first`: its header with an empty mark,
    /// synced with its directory entry, so the appends after need neither.
    fn createActive(self: *DurableLog, first: u64, first_ts_ns: u64) !void {
        var name_buf: [32]u8 = undefined;
        const name = segment.segmentFilename(&name_buf, first) orelse return error.FilenameTooLong;
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ self.segs_path, name }) catch return error.PathTooLong;
        const fixed = segment.fixedFor(self.writer.partition_id, first, first_ts_ns);
        const empty: segment.Mark = .{ .seq = 1, .flags = 0, .data_size = 0, .entry_count = 0, .last_index = first - 1, .last_ts_ns = 0, .commit_index = 0, .crc32c = 0 };
        const file = try stdx.fs.createFile(path, .{ .read = true, .truncate = true });
        errdefer stdx.fs.closeFile(file);
        try stdx.fs.writeAll(file, &segment.encodeHeader(fixed, empty));
        try stdx.fs.sync(file);
        try stdx.fs.syncDir(self.segs_path);
        self.active = .{
            .file = file,
            .fixed = fixed,
            .first_index = first,
            .last_index = first - 1,
            .last_ts_ns = 0,
            .data_size = 0,
            .entry_count = 0,
            .flags = 0,
            .commit_index = 0,
            .seq = 1,
            .slot = 0,
            .bitmap = 0,
            .crc = checksum_mod.ChecksumStream.init(),
        };
        self.creates += 1;
    }

    /// The unsealed segment at `path`, opened for appends: its entries read
    /// once to rebuild the index, skips and CRC a seal needs.
    fn openActive(self: *DurableLog, path: []const u8) !void {
        const file = try stdx.fs.openFile(path, .{ .mode = .read_write });
        errdefer stdx.fs.closeFile(file);
        var hdr_bytes: [segment.HEADER_SIZE]u8 = undefined;
        if (try stdx.fs.readAt(file, &hdr_bytes, 0) != hdr_bytes.len) return error.ShortRead;
        const hdr = try segment.SegmentHeader.parse(&hdr_bytes);
        if (hdr.slot == null or hdr.sealed()) return error.NotActive;
        const data = try self.allocator.alloc(u8, hdr.data_size);
        defer self.allocator.free(data);
        if (try stdx.fs.readAt(file, data, segment.HEADER_SIZE) != data.len) return error.ShortRead;
        var a: Active = .{
            .file = file,
            .fixed = std.mem.bytesToValue(segment.Fixed, &hdr.fixed_bytes),
            .first_index = hdr.first_index,
            .last_index = hdr.first_index - 1,
            .last_ts_ns = 0,
            .data_size = 0,
            .entry_count = 0,
            .flags = 0,
            .commit_index = hdr.commit_index_at_seal,
            .seq = hdr.seq,
            .slot = hdr.slot.?,
            .bitmap = 0,
            .crc = checksum_mod.ChecksumStream.init(),
        };
        errdefer {
            a.sparse.deinit(self.allocator);
            a.skips.deinit(self.allocator);
        }
        var off: usize = 0;
        while (off < data.len) {
            const e = Entry.deserialize(data[off..]) orelse return error.TornActiveSegment;
            try a.reserve(self.allocator, 1);
            a.note(&e, off);
            off += e.totalSize();
        }
        if (a.last_index != hdr.last_index) return error.TornActiveSegment;
        a.data_size = data.len;
        a.crc.add(data);
        self.active = a;
    }

    /// The writer's buffer appended to `a` and synced, then its mark.
    fn append(self: *DurableLog, a: *Active, commit_index: u64) !void {
        const w = self.writer;
        const data = w.data.items;
        // Room first: once the bytes are on disk, counting them can't fail.
        try a.reserve(self.allocator, w.entry_count);
        try stdx.fs.writeAt(a.file, data, segment.HEADER_SIZE + a.data_size);
        try stdx.fs.sync(a.file);
        self.appends += 1;
        var off: usize = 0;
        while (off < data.len) {
            const e = Entry.deserialize(data[off..]).?;
            a.note(&e, a.data_size + off);
            off += e.totalSize();
        }
        a.crc.add(data);
        a.data_size += data.len;
        a.commit_index = @max(a.commit_index, commit_index);
        // Not synced on its own: the next flush's sync takes it. So the mark
        // on disk may lag the entries by one flush, never lead them, and a
        // crash loses at most that one. Recovery finds the entries past it
        // by their CRCs; the commit index it records lags with it, which
        // only makes a boot more careful: entries above the watermark are
        // applied once commit is re-established, not from replay.
        self.writeMark(a) catch |err| log.warn("durable log: could not record a flush in the segment starting at index {d}: {s}; recovery finds its entries", .{ a.first_index, @errorName(err) });
    }

    /// The next mark, in the slot not holding the newest.
    fn writeMark(self: *DurableLog, a: *Active) !void {
        _ = self;
        const slot: u1 = a.slot ^ 1;
        const m = a.mark(a.seq + 1).withCrc();
        try stdx.fs.writeAt(a.file, std.mem.asBytes(&m), segment.MARK_OFFSETS[slot]);
        a.seq += 1;
        a.slot = slot;
    }

    /// Append the sparse index and footer, sync, then mark it sealed and
    /// sync again: until that mark is on disk the file is still active, and
    /// recovery cuts the index and footer off as a torn tail.
    fn sealActive(self: *DurableLog) !void {
        const a = &(self.active orelse return);
        if (!a.crc_whole) {
            a.crc = try sumData(self.allocator, a.file, a.data_size);
            a.crc_whole = true;
        }
        var tail: std.ArrayList(u8) = .empty;
        defer tail.deinit(self.allocator);
        try SegmentWriter.appendSealTail(self.allocator, &tail, a.sparse.items, @intCast(a.data_size), &a.crc, &a.fixed, a.bitmap);
        try stdx.fs.writeAt(a.file, tail.items, segment.HEADER_SIZE + a.data_size);
        try stdx.fs.sync(a.file);
        a.flags |= segment.FLAG_SEALED;
        try self.writeMark(a);
        try stdx.fs.sync(a.file);
        if (self.listing) |l| {
            if (l.len > 0 and l[l.len - 1].first_index == a.first_index) l[l.len - 1].sealed = true;
        }
        self.closeActive();
    }

    /// Seal an unsealed segment that isn't open: opened, then sealed.
    fn sealFile(self: *DurableLog, path: []const u8) !void {
        try self.openActive(path);
        try self.sealActive();
    }

    /// Close the active segment; the next flush or read below the ring
    /// opens it again.
    pub fn closeActive(self: *DurableLog) void {
        if (self.active) |*a| a.close(self.allocator);
        self.active = null;
    }

    /// The active segment as the cached listing has it, updated in place
    /// after a flush rather than the listing dropped: a read below the ring
    /// would otherwise list the whole directory, opening every header,
    /// after every flush.
    fn listActive(self: *DurableLog) void {
        const old = self.listing orelse return;
        const a = &self.active.?;
        if (old.len > 0 and old[old.len - 1].first_index == a.first_index) {
            const f = &old[old.len - 1];
            f.last_index = a.last_index;
            f.commit_index_at_seal = a.commit_index;
            f.holds_skip = a.flags & segment.FLAG_HOLDS_SKIP != 0;
            f.sealed = false;
            return;
        }
        if (old.len > 0 and a.first_index <= old[old.len - 1].last_index) return self.invalidate();
        var name_buf: [32]u8 = undefined;
        const name = segment.segmentFilename(&name_buf, a.first_index) orelse return self.invalidate();
        const path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.segs_path, name }) catch return self.invalidate();
        const grown = self.allocator.realloc(old, old.len + 1) catch {
            self.allocator.free(path);
            return self.invalidate();
        };
        grown[old.len] = .{
            .first_index = a.first_index,
            .last_index = a.last_index,
            .commit_index_at_seal = a.commit_index,
            .holds_skip = a.flags & segment.FLAG_HOLDS_SKIP != 0,
            .sealed = false,
            .path = path,
        };
        self.listing = grown;
    }

    /// The last index any sealed segment holds; zero with none. The listing
    /// is in index order with no overlap, so it's the last segment's.
    pub fn lastSealedIndex(self: *DurableLog) !u64 {
        const files = try self.segments();
        return if (files.len == 0) 0 else files[files.len - 1].last_index;
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
    /// the next index. The result's `count` is how many were copied; zero
    /// with `blocked` unset means nothing durable holds `start`. A segment
    /// that is listed but cannot be opened or read sets `blocked` (and
    /// `disk_failing` when the cause may be the disk) so the caller retries
    /// rather than reading it as a hole; one whose header covers `start` but
    /// whose entries do not is logged and returns zero. While a cut is
    /// pending the files still hold the discarded suffix, so reads stop at
    /// the cut.
    pub fn readRange(self: *DurableLog, start: u64, buf: []Entry, arena: []u8) ReadResult {
        if (buf.len == 0) return .{};
        const w = self.writer;
        if (w.entry_count > 0 and start >= w.first_index and start <= w.last_index) {
            return .{ .count = copyFrom(w.data.items, start, buf, arena) };
        }
        if (self.pending_cut) |cut| {
            if (start > cut) return .{};
        }
        const limit = if (self.pending_cut) |cut| @min(buf.len, @as(usize, @intCast(cut - start + 1))) else buf.len;
        const read = self.readSealed(start, buf[0..limit], arena) catch |err| {
            // A resource or IO failure means "not now", not "not there": the
            // segment is listed, so the data exists; the caller retries. A
            // corrupt or missing segment is a genuine fault.
            switch (readBlockClass(err)) {
                .resource => return .{ .blocked = true },
                .disk => return .{ .blocked = true, .disk_failing = true, .permission = isPermission(err) },
                .fault => {
                    log.err("durable log: cannot read below the ring at index {d}: {s}", .{ start, @errorName(err) });
                    return .{};
                },
            }
        } orelse return .{};
        if (read.missing) log.err("durable log: the segment covering index {d} does not hold it", .{start});
        return .{ .count = read.count };
    }

    fn readSealed(self: *DurableLog, start: u64, buf: []Entry, arena: []u8) !?PositionedSegment.Read {
        if (self.scratch.len == 0) self.scratch = try self.allocator.alloc(u8, SCRATCH_SIZE);
        if (try self.activeHolding(start)) |a| return try a.view().read(start, buf, arena, self.scratch);
        const opened = try self.openCovering(start) orelse return null;
        return try opened.read(start, buf, arena, self.scratch);
    }

    /// The active segment, if it holds `index`: the one open, or the newest
    /// on disk, unsealed, opened now so reads seek by its index rather than
    /// scan it from the start.
    fn activeHolding(self: *DurableLog, index: u64) !?*Active {
        if (self.active) |*a| return if (index >= a.first_index and index <= a.last_index) a else null;
        const files = try self.segments();
        if (files.len == 0) return null;
        const newest = files[files.len - 1];
        if (newest.sealed or newest.unreadable or index < newest.first_index or index > newest.last_index) return null;
        if (self.test_open_error) |e| return e;
        try self.openActive(newest.path);
        return &self.active.?;
    }

    /// Whether `index` is in the active segment, which only this thread
    /// reads (`sealedSource` doesn't hand it to another).
    pub fn activeHolds(self: *DurableLog, index: u64) !bool {
        const files = try self.segments();
        if (files.len == 0) return false;
        const newest = files[files.len - 1];
        return !newest.sealed and index >= newest.first_index and index <= newest.last_index;
    }

    pub const ReadBlockClass = enum {
        /// A resource shortage expected to pass: hold and retry indefinitely.
        resource,
        /// An IO error, or an unmapped errno that may be one: retry, but let
        /// the caller give up and damage if it persists. A read's `EIO`
        /// surfaces as `error.InputOutput`; an open's `EIO` is unmapped and
        /// surfaces as `error.Unexpected`, as the corruption errnos
        /// (`EUCLEAN`, `EBADMSG`) do too.
        disk,
        /// A corrupt or missing segment: a genuine fault, damage now.
        fault,
    };

    /// Classify a failure to open or read a listed segment.
    /// A permission the operator may fix, which every report names as such.
    pub fn isPermission(err: anyerror) bool {
        return err == error.AccessDenied or err == error.PermissionDenied;
    }

    pub fn readBlockClass(err: anyerror) ReadBlockClass {
        return switch (err) {
            error.ProcessFdQuotaExceeded,
            error.SystemFdQuotaExceeded,
            error.SystemResources,
            error.OutOfMemory,
            => .resource,
            error.InputOutput,
            error.Unexpected,
            // Permissions an operator may fix: retried, and damage only past
            // the disk's grace, never taken as corruption at once.
            error.AccessDenied,
            error.PermissionDenied,
            => .disk,
            // Missing, or not what the file should be: the log is damaged.
            // Anything not named above is treated so too.
            else => .fault,
        };
    }

    /// Drop every durable entry above `after_index`: the writer's buffer
    /// first, then the sealed segment spanning the cut is rewritten under
    /// its own filename with only the entries at or below (first_index is
    /// the filename, so the name is stable), and every file wholly above
    /// it is deleted. In `sync` durability the tail can span several files.
    /// The intent is recorded on disk first: a cut interrupted by a crash
    /// or an error is finished by the next boot, because a segment still
    /// holding a truncated index would win at replay.
    pub const Cut = struct {
        ctx: *anyopaque,
        after: *const fn (ctx: *anyopaque, after_index: u64) void,
    };

    pub fn truncateAfter(self: *DurableLog, after_index: u64) !void {
        self.truncations += 1;
        if (self.on_cut) |c| c.after(c.ctx, after_index);
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
        if (self.active) |*a| {
            // Nothing lies above the active segment, so a cut inside it is
            // that file's alone: cut to the last kept entry and marked,
            // with no intent or rewrite. One below it deletes it whole.
            if (after_index >= a.first_index and after_index < a.last_index) try self.cutActive(after_index) else if (after_index < a.first_index) self.closeActive();
        }
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
                const opened = SegmentReader.initFromFile(self.allocator, f.path) catch |err| {
                    return switch (readBlockClass(err)) {
                        .resource => error.ReadBlocked,
                        .disk => error.DiskFailing,
                        .fault => error.SegmentUnreadable,
                    };
                };
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

    /// Cut the active segment after `after_index`, an index it holds below
    /// its last: its file is cut where the first entry above begins, found
    /// from the index entry at or below it, then marked and synced.
    fn cutActive(self: *DurableLog, after_index: u64) !void {
        const a = &self.active.?;
        // Start at the last indexed entry at or below the cut; its ordinal
        // is its index-entry position times the interval.
        var k: usize = 0;
        for (a.sparse.items, 0..) |e, i| {
            if (e.index > after_index) break;
            k = i;
        }
        var offset: u64 = if (a.sparse.items.len > 0) a.sparse.items[k].offset else 0;
        var ordinal: u32 = @intCast(k * segment.SPARSE_INDEX_INTERVAL);
        var last_ts: u64 = 0;
        var hdr_buf: [entry_mod.HEADER_SIZE]u8 = undefined;
        while (offset < a.data_size) {
            if (try stdx.fs.readAt(a.file, &hdr_buf, segment.HEADER_SIZE + offset) != hdr_buf.len) return error.ShortRead;
            const hdr: *align(1) const entry_mod.Header = @ptrCast(&hdr_buf);
            if (hdr.index > after_index) break;
            last_ts = hdr.timestamp_ns;
            ordinal += 1;
            offset += entry_mod.HEADER_SIZE + @as(u64, hdr.payload_len);
        }
        try stdx.fs.setLength(a.file, segment.HEADER_SIZE + offset);
        a.data_size = offset;
        a.entry_count = ordinal;
        a.last_index = after_index;
        a.last_ts_ns = last_ts;
        a.commit_index = @min(a.commit_index, after_index);
        while (a.sparse.items.len > 0 and a.sparse.items[a.sparse.items.len - 1].offset >= offset) _ = a.sparse.pop();
        while (a.skips.items.len > 0 and a.skips.items[a.skips.items.len - 1] > after_index) _ = a.skips.pop();
        if (a.skips.items.len == 0) a.flags &= ~segment.FLAG_HOLDS_SKIP;
        // The types bitmap stays a superset: it only lets a reader skip a
        // segment, never miss an entry.
        a.crc_whole = false;
        try self.writeMark(a);
        try stdx.fs.sync(a.file);
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
        // A rewrite or delete replaces the file under the open handle.
        if (self.active) |a| if (a.last_index > after_index) self.closeActive();
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
        self.listings += 1;
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
                // A resource or IO failure is "not now", not a bad header: let
                // the caller retry rather than log it as corruption and mark a
                // good segment unreadable for good. Said at most once every 30
                // s, because a blocked catch-up re-lists on every retry.
                // `path`'s errdefer frees it on return.
                // Named, so boot shows the shard offline saying so, not a bare
                // error: the operator's to fix.
                if (isPermission(err)) {
                    const kept = @min(de.name.len, self.bad_header.len);
                    @memcpy(self.bad_header[0..kept], de.name[0..kept]);
                    self.bad_header_len = kept;
                }
                if (readBlockClass(err) != .fault) {
                    const now = stdx.time.monotonicMs();
                    if (self.block_warn_ms == null or now -| self.block_warn_ms.? >= 30_000) {
                        self.block_warn_ms = now;
                        log.warn("durable log: could not read the header of {s} now: {s}; will retry", .{ path, @errorName(err) });
                    }
                    return err;
                }
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
            if (hdr.slot == null) {
                // Neither mark reads: what it holds is unknown until
                // recovery scans it (`recoverActive`).
                log.err("durable log: neither mark in the header of {s} reads", .{path});
                const kept = @min(de.name.len, self.bad_header.len);
                @memcpy(self.bad_header[0..kept], de.name[0..kept]);
                self.bad_header_len = kept;
                if (strict) return error.NoValidMark;
                try files.append(self.allocator, .{ .first_index = hdr.first_index, .last_index = std.math.maxInt(u64), .commit_index_at_seal = 0, .unreadable = true, .path = path });
                continue;
            }
            try files.append(self.allocator, .{
                .first_index = hdr.first_index,
                .last_index = hdr.last_index,
                .commit_index_at_seal = hdr.commit_index_at_seal,
                .holds_skip = hdr.holdsSkip(),
                .sealed = hdr.sealed(),
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

    /// Delete the `.tmp` files rewrites left behind. A whole segment (a
    /// cut's rewrite, an offline tool's) is written to `<name>.flseg.tmp`
    /// and renamed into place only once it is on disk, so one still named
    /// `.tmp` is a write a crash cut short: never part of the log. The
    /// active segment's torn tail is `recoverActive`'s. Returns how many
    /// went.
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

    pub const Recovered = struct {
        /// Unsealed segments whose mark was behind or ahead of their entries,
        /// and bytes cut from them as the tail of a flush a crash interrupted.
        marked: usize = 0,
        cut_bytes: u64 = 0,
        /// Unsealed segments holding no whole entry, deleted.
        removed: usize = 0,
        /// Unsealed segments older than the newest, sealed.
        sealed: usize = 0,
    };

    /// At boot, before anything lists or reads the log: make each unsealed
    /// segment's mark say what its file holds. Its entries are scanned from
    /// the start; past what its mark records, the first that doesn't read
    /// whole, carry its own CRC or follow the one before it ends the
    /// segment, and the rest is cut as a flush a crash interrupted (or a
    /// seal's index and footer, written before the mark that seals it).
    /// Entries past a stale mark are kept: they were synced before their
    /// mark was written. One failing within what the mark records was
    /// written whole, so it is damage, not a torn tail: nothing is cut, and
    /// replay stops there. A segment left with
    /// no entry is deleted; an unsealed one that isn't the newest, which a
    /// run never leaves (a seal is synced before the next segment starts),
    /// is sealed. A header that won't read is left for the listing to
    /// report.
    pub fn recoverActive(self: *DurableLog) !Recovered {
        self.closeActive();
        self.invalidate();
        var out: Recovered = .{};
        const files = try self.listSegments(false);
        defer self.freeSegments(files);
        // Newest first: one left with nothing is deleted, and the one
        // before it is then the newest, still to be appended to.
        var newest = true;
        var i = files.len;
        while (i > 0) {
            i -= 1;
            const f = files[i];
            if (f.sealed and !f.unreadable) {
                newest = false;
                continue;
            }
            const removed = out.removed;
            try self.repairUnsealed(f.path, newest, &out);
            if (out.removed == removed) newest = false;
        }
        self.invalidate();
        return out;
    }

    fn repairUnsealed(self: *DurableLog, path: []const u8, newest: bool, out: *Recovered) !void {
        const file = try stdx.fs.openFile(path, .{ .mode = .read_write });
        var closed = false;
        defer if (!closed) stdx.fs.closeFile(file);
        var hdr_bytes: [segment.HEADER_SIZE]u8 = undefined;
        if (try stdx.fs.readAt(file, &hdr_bytes, 0) != hdr_bytes.len) return;
        const hdr = segment.SegmentHeader.parse(&hdr_bytes) catch return;
        if (hdr.sealed()) return;
        // Neither mark reads (a write tears one block at most, so this is
        // damage to both): rebuilt from the fixed part and its entries,
        // trusting none of them as recorded.
        const size = try stdx.fs.fileLength(file);
        const data = try self.allocator.alloc(u8, @intCast(size - segment.HEADER_SIZE));
        defer self.allocator.free(data);
        if (try stdx.fs.readAt(file, data, segment.HEADER_SIZE) != data.len) return error.ShortRead;
        var off: usize = 0;
        var count: u32 = 0;
        var last_ts: u64 = 0;
        var holds_skip = false;
        while (off < data.len) {
            const e = Entry.deserialize(data[off..]) orelse break;
            if (e.header.index != hdr.first_index + count) break;
            e.validate() catch break;
            last_ts = e.header.timestamp_ns;
            if (e.header.entry_type == @backingInt(entry_mod.EntryType.repair_skip)) holds_skip = true;
            count += 1;
            off += e.totalSize();
        }
        const marked: u64 = if (hdr.slot != null) hdr.data_size else 0;
        if (off < marked) {
            log.err("durable log: {s} holds an entry at byte {d} that fails its check, within what its mark records as written; left for replay to report", .{ path, off });
            return;
        }
        if (count == 0) {
            stdx.fs.closeFile(file);
            closed = true;
            try stdx.fs.deleteFile(path);
            try stdx.fs.syncDir(self.segs_path);
            log.info("durable log: removed {s}, a segment no whole entry reached", .{path});
            out.removed += 1;
            return;
        }
        const last = hdr.first_index + count - 1;
        if (hdr.slot == null) log.warn("durable log: neither mark of {s} reads; rebuilt from its {d} entries, applied once commit is re-established", .{ path, count });
        const in_step = hdr.slot != null and hdr.data_size == off and hdr.last_index == last;
        if (off < data.len) {
            try stdx.fs.setLength(file, segment.HEADER_SIZE + off);
            log.info("durable log: cut {d} bytes past the last whole entry of {s}, the tail of a flush or seal a crash cut short", .{ data.len - off, path });
            out.cut_bytes += data.len - off;
        }
        if (!in_step) {
            const slot: u1 = if (hdr.slot) |sl| sl ^ 1 else 0;
            const m = (segment.Mark{
                .seq = hdr.seq + 1,
                .flags = if (holds_skip) segment.FLAG_HOLDS_SKIP else 0,
                .data_size = @intCast(off),
                .entry_count = count,
                .last_index = last,
                .last_ts_ns = last_ts,
                // What it was flushed under, no higher than what it holds;
                // with no mark, none, and replay waits for commit.
                .commit_index = if (hdr.slot != null) @min(hdr.commit_index_at_seal, last) else 0,
                .crc32c = 0,
            }).withCrc();
            try stdx.fs.writeAt(file, std.mem.asBytes(&m), segment.MARK_OFFSETS[slot]);
            out.marked += 1;
        }
        if (off < data.len or !in_step) try stdx.fs.sync(file);
        stdx.fs.closeFile(file);
        closed = true;
        if (!newest) {
            try self.sealFile(path);
            out.sealed += 1;
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
        self.closeOpen();
    }

    fn closeOpen(self: *DurableLog) void {
        if (self.open) |*o| o.close(self.allocator);
        self.open = null;
    }

    /// The open segment covering `index`, opening (and caching) it if the
    /// one held does not. Null when no sealed segment holds the index.
    fn openCovering(self: *DurableLog, index: u64) !?*const PositionedSegment {
        if (self.open) |*o| {
            if (index >= o.first_index and index <= o.last_index) return o;
        }
        // A cached segment above needs no open and is served even under the
        // injected error; one that must be opened hits it.
        if (self.test_open_error) |e| return e;
        const f = covering(try self.segments(), index) orelse return null;
        const opened = try PositionedSegment.open(self.allocator, f.path);
        self.closeOpen();
        self.open = opened;
        return &self.open.?;
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

/// Read only the header: listing a directory of large segments must not
/// read them.
fn readHeader(path: []const u8) !segment.SegmentHeader {
    const file = try stdx.fs.openFile(path, .{});
    defer stdx.fs.closeFile(file);
    var buf: [segment.HEADER_SIZE]u8 = undefined;
    const n = try stdx.fs.readAll(file, &buf);
    if (n != buf.len) return error.TooSmall;
    return segment.SegmentHeader.parse(&buf);
}

/// The CRC of a segment's first `len` data bytes, read back in chunks.
fn sumData(allocator: std.mem.Allocator, file: std.Io.File, len: u64) !checksum_mod.ChecksumStream {
    var crc = checksum_mod.ChecksumStream.init();
    const chunk = try allocator.alloc(u8, 1 << 20);
    defer allocator.free(chunk);
    var at: u64 = 0;
    while (at < len) {
        const want: usize = @intCast(@min(chunk.len, len - at));
        if (try stdx.fs.readAt(file, chunk[0..want], segment.HEADER_SIZE + at) != want) return error.ShortRead;
        crc.add(chunk[0..want]);
        at += want;
    }
    return crc;
}

/// Copy entries at or above `start` out of a self-delimiting byte run.
pub fn copyFrom(data: []const u8, start: u64, buf: []Entry, arena: []u8) usize {
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

test "durable log: flush tries a pending cut before the empty-buffer return, so a failing cut is not a clean flush" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    dl.max_segment_bytes = 1;
    // Two sealed segments on disk, 1..3 and 4..6; the buffer is empty.
    try writer.addEntry(&testEntry(1, 1, "a"));
    try writer.addEntry(&testEntry(2, 1, "b"));
    try writer.addEntry(&testEntry(3, 1, "c"));
    try dl.flush(3);
    try writer.addEntry(&testEntry(4, 1, "d"));
    try writer.addEntry(&testEntry(5, 1, "e"));
    try writer.addEntry(&testEntry(6, 1, "f"));
    try dl.flush(6);

    // The directory can't be written, so truncating to 4 can't rewrite the
    // 4..6 segment: the cut stays pending and the buffer stays empty.
    var zbuf: [600:0]u8 = undefined;
    const z = try std.fmt.bufPrintSentinel(&zbuf, "{s}", .{td.path}, 0);
    _ = std.c.chmod(z, 0o500);
    defer _ = std.c.chmod(z, 0o700);
    dl.truncateAfter(4) catch {};
    try testing.expect(dl.pending_cut != null);
    try testing.expectEqual(@as(u64, 0), dl.writer.entry_count);

    // An empty-buffer flush must still try the pending cut, and fails — not a
    // clean flush. (Trying the cut only after the empty-buffer return would
    // report success while the disk is still failing it.)
    try testing.expectError(error.TruncationIncomplete, dl.flush(6));

    // Writable again: the flush completes the cut and clears it.
    _ = std.c.chmod(z, 0o700);
    try dl.flush(6);
    try testing.expect(dl.pending_cut == null);
}

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
    dl.max_segment_bytes = 1;

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
    dl.max_segment_bytes = 1;

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
    dl.max_segment_bytes = 1;

    try writer.addEntry(&testEntry(1, 1, "one"));
    try writer.addEntry(&testEntry(2, 1, "two"));
    try writer.addEntry(&testEntry(3, 1, "three"));
    try dl.flush(3);
    try writer.addEntry(&testEntry(4, 2, "four"));
    try writer.addEntry(&testEntry(5, 2, "five"));

    var buf: [8]Entry = undefined;
    var arena: [256]u8 = undefined;
    // From inside the sealed segment: the rest of that segment, not beyond.
    var n = dl.readRange(2, &buf, &arena).count;
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqual(@as(u64, 2), buf[0].header.index);
    try testing.expectEqualStrings("two", buf[0].payload);
    try testing.expectEqual(@as(u64, 3), buf[1].header.index);
    try testing.expectEqualStrings("three", buf[1].payload);
    // From the writer buffer, with the terms the entries carry.
    n = dl.readRange(4, &buf, &arena).count;
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqual(@as(u64, 2), buf[0].header.term);
    try testing.expectEqualStrings("five", buf[1].payload);
    // Nothing holds index 6.
    try testing.expectEqual(@as(usize, 0), dl.readRange(6, &buf, &arena).count);
    // A one-slot buffer gets exactly one.
    try testing.expectEqual(@as(usize, 1), dl.readRange(1, buf[0..1], &arena).count);
    try testing.expectEqualStrings("one", buf[0].payload);
}

test "durable log: every index is found in its own segment among many, and none outside them" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    dl.max_segment_bytes = 1;

    // 40 segments of uneven size: segment k holds k % 3 + 1 entries, from index 5.
    var index: u64 = 5;
    var firsts: [40]u64 = undefined;
    for (0..40) |k| {
        firsts[k] = index;
        for (0..k % 3 + 1) |_| {
            try writer.addEntry(&testEntry(index, 1, "x"));
            index += 1;
        }
        try dl.flush(index - 1);
    }
    const last = index - 1;
    try testing.expectEqual(@as(usize, 40), (try dl.segments()).len);
    try testing.expectEqual(last, try dl.lastSealedIndex());

    var buf: [1]Entry = undefined;
    var arena: [64]u8 = undefined;
    var path_buf: [512]u8 = undefined;
    for (5..last + 1) |i| {
        try testing.expectEqual(@as(usize, 1), dl.readRange(i, &buf, &arena).count);
        try testing.expectEqual(@as(u64, i), buf[0].header.index);
        const src = (try dl.sealedSource(i, &path_buf)).?;
        // The segment it names starts at the last first index at or below i.
        var k: usize = 0;
        while (k + 1 < firsts.len and firsts[k + 1] <= i) k += 1;
        var name_buf: [32]u8 = undefined;
        try testing.expect(std.mem.endsWith(u8, src.path, segment.segmentFilename(&name_buf, firsts[k]).?));
    }
    for ([_]u64{ 0, 1, 4, last + 1, last + 100 }) |i| {
        try testing.expectEqual(@as(usize, 0), dl.readRange(i, &buf, &arena).count);
        try testing.expect((try dl.sealedSource(i, &path_buf)) == null);
    }
}

/// Payload `i` is `i % 7 * 50` bytes of `i`'s low byte, so sizes vary and
/// some are empty.
fn variedPayload(buf: []u8, i: u64) []const u8 {
    const len: usize = @intCast(i % 7 * 50);
    @memset(buf[0..len], @truncate(i));
    return buf[0..len];
}

test "durable log: a positioned read finds every start in a large segment, through any scratch size" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    // 1,000 entries from index 11: several sparse index points.
    var pbuf: [512]u8 = undefined;
    for (11..1011) |i| try writer.addEntry(&testEntry(i, 1, variedPayload(&pbuf, i)));
    try writer.writeToFile(td.path);
    var name_buf: [32]u8 = undefined;
    const path = try std.fmt.allocPrint(testing.allocator, "{s}/{s}", .{ td.path, segment.segmentFilename(&name_buf, 11).? });
    defer testing.allocator.free(path);

    var seg = try PositionedSegment.open(testing.allocator, path);
    defer seg.close(testing.allocator);
    try testing.expect(seg.sparse.len > 1);
    try testing.expect(seg.whole == null);

    var buf: [5]Entry = undefined;
    var arena: [4096]u8 = undefined;
    // A scratch smaller than most entries reads them straight into the arena.
    var small: [64]u8 = undefined;
    var large: [16 * 1024]u8 = undefined;
    for ([_][]u8{ &small, &large }) |scratch| {
        for (11..1011) |start| {
            const got = try seg.read(start, &buf, &arena, scratch);
            const want = @min(5, 1011 - start);
            try testing.expectEqual(want, got.count);
            try testing.expect(!got.missing);
            for (buf[0..got.count], 0..) |e, k| {
                try testing.expectEqual(@as(u64, start + k), e.header.index);
                try testing.expectEqualSlices(u8, variedPayload(&pbuf, start + k), e.payload);
            }
        }
        // Outside the segment: nothing, and not "missing".
        for ([_]u64{ 10, 1011 }) |start| {
            const got = try seg.read(start, &buf, &arena, scratch);
            try testing.expectEqual(@as(usize, 0), got.count);
            try testing.expect(!got.missing);
        }
    }
    // An arena too small for the next payload stops the read there: from
    // 14, payloads of 0, 50 and 100 bytes fit 150, and 17's 150 doesn't.
    const got = try seg.read(14, &buf, arena[0..150], &large);
    try testing.expectEqual(@as(usize, 3), got.count);
}

test "durable log: a positioned read refuses an entry whose bytes don't match its CRC" {
    var td = try TestDir.init();
    defer td.deinit();
    // Two segments: a large one, read by position, and a small one, read whole.
    var name_buf: [32]u8 = undefined;
    var paths: [2][]const u8 = undefined;
    for ([_]u64{ 300, 3 }, [_]u64{ 1, 1001 }, 0..) |n, first, k| {
        var writer = SegmentWriter.init(testing.allocator, 0);
        defer writer.deinit();
        var pbuf: [300]u8 = undefined;
        for (first..first + n) |i| {
            @memset(&pbuf, 'p');
            if (i == first + 1) @memcpy(pbuf[0..6], "second");
            try writer.addEntry(&testEntry(i, 1, &pbuf));
        }
        try writer.writeToFile(td.path);
        paths[k] = try std.fmt.allocPrint(testing.allocator, "{s}/{s}", .{ td.path, segment.segmentFilename(&name_buf, first).? });
        // Flip a byte of the second entry's payload in place.
        const bytes = try stdx.fs.readFileAlloc(testing.allocator, paths[k], 1 << 20);
        defer testing.allocator.free(bytes);
        bytes[std.mem.indexOf(u8, bytes, "second").?] ^= 0xff;
        const f = try stdx.fs.createFile(paths[k], .{});
        defer stdx.fs.closeFile(f);
        try stdx.fs.writeAll(f, bytes);
    }
    defer for (paths) |p| testing.allocator.free(p);

    var seg = try PositionedSegment.open(testing.allocator, paths[0]);
    defer seg.close(testing.allocator);
    try testing.expect(seg.whole == null);
    var buf: [3]Entry = undefined;
    var arena: [1024]u8 = undefined;
    var scratch: [4096]u8 = undefined;
    // Entry 1 is good on its own; a read that reaches entry 2 fails.
    try testing.expectEqual(@as(usize, 1), (try seg.read(1, buf[0..1], &arena, &scratch)).count);
    try testing.expectError(error.InvalidCrc, seg.read(1, &buf, &arena, &scratch));
    try testing.expectError(error.InvalidCrc, seg.read(2, &buf, &arena, &scratch));
    try testing.expectEqual(@as(usize, 3), (try seg.read(3, &buf, &arena, &scratch)).count);
    // The small one is read whole, and its whole-file CRC refuses it.
    try testing.expectError(error.InvalidCrc, PositionedSegment.open(testing.allocator, paths[1]));
    // The durable log reads either as a fault: nothing, and not blocked.
    var dl_writer = SegmentWriter.init(testing.allocator, 0);
    defer dl_writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &dl_writer, td.path);
    defer dl.deinit();
    dl.max_segment_bytes = 1;
    for ([_]u64{ 2, 1002 }) |i| {
        const r = dl.readRange(i, &buf, &arena);
        try testing.expectEqual(@as(usize, 0), r.count);
        try testing.expect(!r.blocked);
    }
}

/// Writes one segment of `payloads` from index `first` into `dir`, and
/// returns its path; the caller frees it.
fn writeTestSegment(dir: []const u8, first: u64, payloads: []const []const u8) ![]const u8 {
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    for (payloads, first..) |p, i| try writer.addEntry(&testEntry(i, 1, p));
    try writer.writeToFile(dir);
    var name_buf: [32]u8 = undefined;
    return std.fmt.allocPrint(testing.allocator, "{s}/{s}", .{ dir, segment.segmentFilename(&name_buf, first).? });
}

/// Applies `edit` to the file at `path` in place.
fn editFile(path: []const u8, edit: *const fn ([]u8) void) !void {
    const bytes = try stdx.fs.readFileAlloc(testing.allocator, path, 4 << 20);
    defer testing.allocator.free(bytes);
    edit(bytes);
    const f = try stdx.fs.createFile(path, .{});
    defer stdx.fs.closeFile(f);
    try stdx.fs.writeAll(f, bytes);
}

test "durable log: a positioned read checks an entry bigger than its scratch, and refuses a bad length or index" {
    var td = try TestDir.init();
    defer td.deinit();
    // 100 KiB payloads: the file is read by position, and each entry is
    // bigger than the 4 KiB scratch, so it's read straight into the arena.
    const big = try testing.allocator.alloc(u8, 100 * 1024);
    defer testing.allocator.free(big);
    var scratch: [4096]u8 = undefined;
    var buf: [3]Entry = undefined;
    const arena = try testing.allocator.alloc(u8, 400 * 1024);
    defer testing.allocator.free(arena);

    const Edits = struct {
        /// A byte of the second entry's payload: its marker.
        fn payload(bytes: []u8) void {
            bytes[std.mem.indexOf(u8, bytes, "SECOND").?] ^= 0xff;
        }
        /// The second entry's length, past the file's end.
        fn length(bytes: []u8) void {
            const at = std.mem.indexOf(u8, bytes, "SECOND").? - entry_mod.HEADER_SIZE + @offsetOf(entry_mod.Header, "payload_len");
            std.mem.writeInt(u32, bytes[at..][0..4], 0xFFFF_FFF0, .little);
        }
        /// The footer's sparse index offset, into the data section.
        fn index(bytes: []u8) void {
            const footer = bytes.len - segment.FOOTER_SIZE;
            std.mem.writeInt(u32, bytes[footer..][0..4], segment.HEADER_SIZE + 10, .little);
        }
        /// A sparse index entry's offset, past the data.
        fn sparse(bytes: []u8) void {
            const footer = bytes.len - segment.FOOTER_SIZE;
            const idx = std.mem.readInt(u32, bytes[footer..][0..4], .little);
            std.mem.writeInt(u32, bytes[idx + 8 ..][0..4], 0xFFFF_FFFF, .little);
        }
        /// The mark's data size, one short (its CRC rewritten to match).
        fn data_size(bytes: []u8) void {
            const at = segment.MARK_OFFSETS[0];
            var m = std.mem.bytesToValue(segment.Mark, bytes[at..][0..segment.MARK_SIZE]);
            m.data_size -= 1;
            @memcpy(bytes[at..][0..segment.MARK_SIZE], std.mem.asBytes(&m.withCrc()));
        }
    };
    var first: u64 = 1;
    for ([_]*const fn ([]u8) void{ Edits.payload, Edits.length, Edits.index, Edits.sparse, Edits.data_size }, 0..) |edit, k| {
        @memset(big, 'b');
        var second = try testing.allocator.dupe(u8, big);
        defer testing.allocator.free(second);
        @memcpy(second[0..6], "SECOND");
        const path = try writeTestSegment(td.path, first, &.{ big, second, big });
        defer testing.allocator.free(path);
        try editFile(path, edit);
        defer first += 10;
        if (k >= 2) {
            try testing.expectError(error.InvalidIndex, PositionedSegment.open(testing.allocator, path));
            continue;
        }
        var seg = try PositionedSegment.open(testing.allocator, path);
        defer seg.close(testing.allocator);
        try testing.expect(seg.whole == null and seg.file != null);
        // The first entry alone is good; a read that reaches the second fails.
        try testing.expectEqual(@as(usize, 1), (try seg.read(first, buf[0..1], arena, &scratch)).count);
        try testing.expectEqualSlices(u8, big, buf[0].payload);
        const want: anyerror = if (k == 0) error.InvalidCrc else error.ShortRead;
        try testing.expectError(want, seg.read(first, &buf, arena, &scratch));
    }
}

test "durable log: a small segment is read whole and holds no file open" {
    var td = try TestDir.init();
    defer td.deinit();
    const path = try writeTestSegment(td.path, 1, &.{ "one", "two" });
    defer testing.allocator.free(path);
    var seg = try PositionedSegment.open(testing.allocator, path);
    defer seg.close(testing.allocator);
    try testing.expect(seg.whole != null and seg.file == null);
    var buf: [2]Entry = undefined;
    var arena: [16]u8 = undefined;
    var scratch: [64]u8 = undefined;
    try testing.expectEqual(@as(usize, 2), (try seg.read(1, &buf, &arena, &scratch)).count);
}

test "durable log: truncateAfter rewrites the sealed tail under its own name and deletes emptied files" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    dl.max_segment_bytes = 1;

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
    try testing.expectEqual(@as(usize, 2), dl.readRange(4, &buf, &arena).count);
    try testing.expectEqual(@as(usize, 0), dl.readRange(6, &buf, &arena).count);

    // A new index 6 flushed later lands in its own file and reads back.
    try writer.addEntry(&testEntry(6, 2, "F"));
    try dl.flush(6);
    try testing.expectEqual(@as(usize, 1), dl.readRange(6, &buf, &arena).count);
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
        dl.max_segment_bytes = 1;
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
        try testing.expectEqual(@as(usize, 0), dl.readRange(2, &buf, &arena).count);
        try testing.expectEqual(@as(usize, 3), dl.readRange(4, &buf, &arena).count);
    }
    // Reopened: the listing starts at 4, and dropping through 6 takes 4-6.
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    dl.max_segment_bytes = 1;
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
    dl.max_segment_bytes = 1;

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
    dl.max_segment_bytes = 1;
    try writer.addEntry(&testEntry(1, 1, "a"));
    try dl.flush(1);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);

    // The segment as a build with the previous format wrote it.
    const bytes = try stdx.fs.readFileAlloc(testing.allocator, files[0].path, 1 << 20);
    defer testing.allocator.free(bytes);
    std.mem.writeInt(u16, bytes[@offsetOf(segment.Fixed, "version")..][0..2], segment.SEGMENT_VERSION - 1, .little);
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
    dl.max_segment_bytes = 1;

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
    // The flush left a listing cached; the stray file came after it.
    dl.invalidate();
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
    dl.max_segment_bytes = 1;

    try writer.addEntry(&testEntry(1, 1, "a"));
    try writer.addEntry(&testEntry(2, 1, "b"));
    try dl.flush(2);
    var buf: [4]Entry = undefined;
    var arena: [64]u8 = undefined;
    try testing.expectEqual(@as(usize, 2), dl.readRange(1, &buf, &arena).count);
    try testing.expect(dl.open != null);

    // A new file must be visible to the next read.
    try writer.addEntry(&testEntry(3, 1, "c"));
    try dl.flush(3);
    try testing.expectEqual(@as(usize, 1), dl.readRange(3, &buf, &arena).count);
    try testing.expectEqualStrings("c", buf[0].payload);

    // A cut inside the open segment must not be served from the old copy.
    try dl.truncateAfter(1);
    try testing.expectEqual(@as(usize, 0), dl.readRange(2, &buf, &arena).count);
    try testing.expectEqual(@as(usize, 1), dl.readRange(1, &buf, &arena).count);
}

test "durable log: a truncation that cannot reach the files wedges flushing until it does" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    dl.max_segment_bytes = 1;

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
    try testing.expectEqual(@as(usize, 2), dl.readRange(1, &stale, &stale_arena).count);
    // Index 3 itself comes from the buffer, the replacement, not the file.
    try testing.expectEqual(@as(usize, 1), dl.readRange(3, &stale, &stale_arena).count);
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
    dl.max_segment_bytes = 1;

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
    dl.max_segment_bytes = 1;
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
    dl.max_segment_bytes = 1;

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
    dl.max_segment_bytes = 1;
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
    dl.max_segment_bytes = 1;
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
    dl.max_segment_bytes = 1;
    var path_buf: [512]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/backup.flseg", .{td.path});
    const file = try stdx.fs.createFile(path, .{});
    stdx.fs.closeFile(file);
    try testing.expectError(error.TooSmall, dl.listSegments(false));
}

test "durable log: a resource or IO read failure is held as blocked, corruption is not" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    dl.max_segment_bytes = 1;
    try writer.addEntry(&testEntry(1, 1, "one"));
    try writer.addEntry(&testEntry(2, 1, "two"));
    try writer.addEntry(&testEntry(3, 1, "three"));
    try dl.flush(3);
    try writer.addEntry(&testEntry(4, 1, "four"));
    try writer.addEntry(&testEntry(5, 1, "five"));
    try dl.flush(5);
    try writer.addEntry(&testEntry(6, 1, "six"));
    try dl.flush(6);
    var buf: [8]Entry = undefined;
    var arena: [256]u8 = undefined;
    // A resource error opening a segment is "not now", not "not there": the
    // read is held as blocked, and a resource shortage is not the disk failing.
    dl.test_open_error = error.SystemResources;
    {
        const r = dl.readRange(1, &buf, &arena);
        try testing.expect(r.blocked and !r.disk_failing and r.count == 0);
    }
    // An IO error is blocked too, and marked as the disk maybe failing.
    dl.test_open_error = error.InputOutput;
    {
        const r = dl.readRange(1, &buf, &arena);
        try testing.expect(r.blocked and r.disk_failing);
    }
    dl.test_open_error = null;
    // Recovered: the first segment reads whole.
    const r2 = dl.readRange(1, &buf, &arena);
    try testing.expectEqual(@as(usize, 3), r2.count);
    {
        // The open segment is cached, so a later read of it is served even
        // while a fresh open would fail; a read that must open the next one is
        // blocked.
        dl.test_open_error = error.SystemResources;
        defer dl.test_open_error = null;
        const cached = dl.readRange(2, &buf, &arena);
        try testing.expect(cached.count == 2 and !cached.blocked);
        const next = dl.readRange(4, &buf, &arena);
        try testing.expect(next.blocked and next.count == 0);
    }
    const files = try dl.segments();
    const path1: []const u8 = try testing.allocator.dupe(u8, files[0].path);
    defer testing.allocator.free(path1);
    const path2: []const u8 = try testing.allocator.dupe(u8, files[1].path);
    defer testing.allocator.free(path2);
    const path3: []const u8 = try testing.allocator.dupe(u8, files[2].path);
    defer testing.allocator.free(path3);
    // A permission error is an operator's to fix: held as a disk-class
    // block, retried, and damage only past the disk's grace.
    {
        var dpath: [600:0]u8 = undefined;
        const z = try std.fmt.bufPrintSentinel(&dpath, "{s}", .{path3}, 0);
        _ = std.c.chmod(z, 0);
        dl.closeOpen();
        const r = dl.readRange(6, &buf, &arena);
        // Root reads through any mode: only check the block where it bites.
        if (std.c.getuid() != 0) try testing.expect(r.blocked and r.disk_failing);
        _ = std.c.chmod(z, 0o644);
    }
    // A truncated segment is a fault, not a block.
    {
        var dpath: [600:0]u8 = undefined;
        const z = try std.fmt.bufPrintSentinel(&dpath, "{s}", .{path2}, 0);
        const tfd = std.c.open(z, .{ .ACCMODE = .WRONLY }, @as(std.c.mode_t, 0));
        _ = std.c.ftruncate(tfd, 80);
        _ = std.c.close(tfd);
        const r = dl.readRange(4, &buf, &arena);
        try testing.expect(!r.blocked and r.count == 0);
    }
    // A removed segment is a fault, not a block.
    {
        try stdx.fs.deleteFile(path3);
        const r = dl.readRange(6, &buf, &arena);
        try testing.expect(!r.blocked and r.count == 0);
    }
    // A bad header is a fault, not a block.
    {
        dl.invalidate();
        var dpath: [600:0]u8 = undefined;
        const z = try std.fmt.bufPrintSentinel(&dpath, "{s}", .{path1}, 0);
        const fd = std.c.open(z, .{ .ACCMODE = .WRONLY }, @as(std.c.mode_t, 0));
        _ = std.c.write(fd, "XXXX", 4);
        _ = std.c.close(fd);
        const r = dl.readRange(1, &buf, &arena);
        try testing.expect(!r.blocked and r.count == 0);
    }
}

test "durable log: a flush adds its segment to the cached listing, so a read after it lists nothing" {
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
    dl.max_segment_bytes = 1;

    const SEGMENTS = 2_000;
    for (1..SEGMENTS + 1) |i| {
        try writer.addEntry(&testEntry(i, 1, "v"));
        try dl.flush(i);
    }
    _ = try dl.segments();
    try testing.expectEqual(@as(u64, 1), dl.listings);
    // One more segment sealed, then a read below the ring's lookup: the
    // listing it finds is the cached one, grown by that segment.
    try writer.addEntry(&testEntry(SEGMENTS + 1, 1, "v"));
    try dl.flush(SEGMENTS + 1);
    var buf: [512]u8 = undefined;
    const t0 = stdx.time.monotonicNs();
    const src = (try dl.sealedSource(SEGMENTS + 1, &buf)).?;
    const first = (try dl.sealedSource(1, &buf)).?;
    const took_us = (stdx.time.monotonicNs() - t0) / std.time.ns_per_us;
    try testing.expectEqual(@as(u64, 1), dl.listings);
    try testing.expectEqual(@as(u64, 1), src.limit);
    try testing.expect(std.mem.endsWith(u8, first.path, "0000000001.flseg"));
    // It matches a fresh listing.
    const t1 = stdx.time.monotonicNs();
    const fresh = try dl.listSegments(true);
    const list_us = (stdx.time.monotonicNs() - t1) / std.time.ns_per_us;
    defer dl.freeSegments(fresh);
    try testing.expectEqual(fresh.len, (try dl.segments()).len);
    try testing.expectEqual(fresh[fresh.len - 1].last_index, (try dl.segments())[fresh.len - 1].last_index);
    std.debug.print("{d} segments: two lookups after a flush in {d} us, no listing; a full listing takes {d} us\n", .{ SEGMENTS + 1, took_us, list_us });
}

test "durable log: readBlockClass sorts a read error into resource, disk, or fault" {
    const R = DurableLog.ReadBlockClass;
    // A resource shortage holds: these clear on their own.
    try testing.expectEqual(R.resource, DurableLog.readBlockClass(error.ProcessFdQuotaExceeded));
    try testing.expectEqual(R.resource, DurableLog.readBlockClass(error.SystemFdQuotaExceeded));
    try testing.expectEqual(R.resource, DurableLog.readBlockClass(error.SystemResources));
    try testing.expectEqual(R.resource, DurableLog.readBlockClass(error.OutOfMemory));
    // An IO error (or an unmapped one that may be one) holds, then becomes
    // damage if it lasts.
    try testing.expectEqual(R.disk, DurableLog.readBlockClass(error.InputOutput));
    try testing.expectEqual(R.disk, DurableLog.readBlockClass(error.Unexpected));
    // Corruption and a missing or unreadable file are faults, read now as a hole.
    try testing.expectEqual(R.fault, DurableLog.readBlockClass(error.InvalidVersion));
    try testing.expectEqual(R.disk, DurableLog.readBlockClass(error.AccessDenied));
    try testing.expectEqual(R.disk, DurableLog.readBlockClass(error.PermissionDenied));
    try testing.expectEqual(R.fault, DurableLog.readBlockClass(error.FileNotFound));
    try testing.expectEqual(R.fault, DurableLog.readBlockClass(error.EndOfStream));
}

// ─── The active segment ───────────────────────────────────────────────────────

/// Flush entries `from` through `to` as one flush under commit `to`.
fn flushRun(dl: *DurableLog, writer: *SegmentWriter, from: u64, to: u64, payload: []const u8) !void {
    var i = from;
    while (i <= to) : (i += 1) try writer.addEntry(&testEntry(i, 1, payload));
    try dl.flush(to);
}

fn countSegments(dir: []const u8) !usize {
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (names.items) |n| testing.allocator.free(n);
        names.deinit(testing.allocator);
    }
    try fileNames(dir, &names);
    var n: usize = 0;
    for (names.items) |name| {
        if (std.mem.endsWith(u8, name, ".flseg")) n += 1;
    }
    return n;
}

test "durable log: flushes append to one active segment, one sync each, and it is sealed once it reaches its size" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    dl.max_segment_bytes = 4096;

    const syncs = stdx.fs.dir_syncs.load(.monotonic);
    for (0..10) |k| try flushRun(&dl, &writer, k * 3 + 1, k * 3 + 3, "payload-bytes");
    // Ten flushes, one file: its directory entry synced once, when made.
    try testing.expectEqual(@as(usize, 1), try countSegments(td.path));
    try testing.expectEqual(@as(u64, 10), dl.appends);
    try testing.expectEqual(@as(u64, 1), dl.creates);
    try testing.expectEqual(syncs + 1, stdx.fs.dir_syncs.load(.monotonic));
    var files = try dl.listSegments(true);
    try testing.expectEqual(@as(u64, 30), files[0].last_index);
    try testing.expectEqual(@as(u64, 30), files[0].commit_index_at_seal);
    try testing.expect(!files[0].sealed);
    dl.freeSegments(files);

    // Past its size it is sealed, and the next flush starts another.
    const big = @as([4096]u8, @splat('B'));
    try flushRun(&dl, &writer, 31, 31, &big);
    try flushRun(&dl, &writer, 32, 33, "after");
    files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(usize, 2), files.len);
    try testing.expect(files[0].sealed and !files[1].sealed);
    try testing.expectEqual(@as(u64, 31), files[0].last_index);
    try testing.expectEqual(@as(u64, 32), files[1].first_index);
    // The sealed one reads whole, its footer checked; every index reads.
    const opened = try SegmentReader.initFromFile(testing.allocator, files[0].path);
    testing.allocator.free(opened.buf);
    var buf: [1]Entry = undefined;
    var arena: [8192]u8 = undefined;
    for (1..34) |i| try testing.expectEqual(@as(usize, 1), dl.readRange(i, &buf, &arena).count);
    // Only the sealed one is handed to another thread.
    var path_buf: [512]u8 = undefined;
    try testing.expect((try dl.sealedSource(5, &path_buf)) != null);
    try testing.expect((try dl.sealedSource(33, &path_buf)) == null);
    try testing.expect(try dl.activeHolds(33));
    try testing.expect(!(try dl.activeHolds(5)));
}

test "durable log: after a restart the newest unsealed segment is appended to, not replaced" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    {
        var dl = try DurableLog.init(testing.allocator, &writer, td.path);
        defer dl.deinit();
        for (0..300) |k| try flushRun(&dl, &writer, k + 1, k + 1, "x");
    }
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    const r = try dl.recoverActive();
    try testing.expectEqual(@as(usize, 0), r.marked + r.removed + r.sealed);
    try flushRun(&dl, &writer, 301, 302, "y");
    try testing.expectEqual(@as(usize, 1), try countSegments(td.path));
    try testing.expectEqual(@as(u64, 0), dl.creates);
    var buf: [400]Entry = undefined;
    var arena: [4096]u8 = undefined;
    try testing.expectEqual(@as(usize, 302), dl.readRange(1, &buf, &arena).count);
    // Reopened, its index was rebuilt: a read seeks, not scans.
    try testing.expect(dl.active.?.sparse.items.len == 2);
}

/// The file's header as it was, written back over it: a crash that kept
/// the entries of the flushes since but lost their marks.
fn restoreHeader(path: []const u8, header: *const [segment.HEADER_SIZE]u8) !void {
    const file = try stdx.fs.openFile(path, .{ .mode = .read_write });
    defer stdx.fs.closeFile(file);
    try stdx.fs.writeAt(file, header, 0);
}

fn readHeaderBytes(path: []const u8) ![segment.HEADER_SIZE]u8 {
    const file = try stdx.fs.openFile(path, .{});
    defer stdx.fs.closeFile(file);
    var out: [segment.HEADER_SIZE]u8 = undefined;
    _ = try stdx.fs.readAt(file, &out, 0);
    return out;
}

fn appendBytes(path: []const u8, bytes: []const u8) !void {
    const file = try stdx.fs.openFile(path, .{ .mode = .read_write });
    defer stdx.fs.closeFile(file);
    try stdx.fs.writeAt(file, bytes, try stdx.fs.fileLength(file));
}

test "durable log: recovery keeps entries past a mark a crash lost, and cuts a torn flush after them" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var path: []u8 = undefined;
    {
        var dl = try DurableLog.init(testing.allocator, &writer, td.path);
        defer dl.deinit();
        try flushRun(&dl, &writer, 1, 3, "first");
        const files = try dl.segments();
        path = try testing.allocator.dupe(u8, files[0].path);
        const early = try readHeaderBytes(path);
        try flushRun(&dl, &writer, 4, 6, "second");
        try flushRun(&dl, &writer, 7, 9, "third");
        // The marks of the last two flushes lost; the entries synced.
        try restoreHeader(path, &early);
        // And half an entry of a fourth flush the crash cut short.
        var torn = testEntry(10, 1, "fourth flush");
        var torn_bytes: [64]u8 = undefined;
        @memcpy(torn_bytes[0..entry_mod.HEADER_SIZE], torn.header.asBytes());
        try appendBytes(path, torn_bytes[0 .. entry_mod.HEADER_SIZE + 3]);
    }
    defer testing.allocator.free(path);
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    const r = try dl.recoverActive();
    try testing.expectEqual(@as(usize, 1), r.marked);
    try testing.expectEqual(@as(u64, entry_mod.HEADER_SIZE + 3), r.cut_bytes);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(u64, 9), files[0].last_index);
    // The commit recorded is the old mark's: the flushes since wait for
    // commit to be re-established.
    try testing.expectEqual(@as(u64, 3), files[0].commit_index_at_seal);
    var buf: [16]Entry = undefined;
    var arena: [1024]u8 = undefined;
    try testing.expectEqual(@as(usize, 9), dl.readRange(1, &buf, &arena).count);
    // Recovered once, it has nothing more to do.
    const again = try dl.recoverActive();
    try testing.expectEqual(@as(usize, 0), again.marked);
    try testing.expectEqual(@as(u64, 0), again.cut_bytes);
}

test "durable log: an entry failing its check within what the mark records is left for replay, not cut" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var path: []u8 = undefined;
    var size: u64 = 0;
    {
        var dl = try DurableLog.init(testing.allocator, &writer, td.path);
        defer dl.deinit();
        try flushRun(&dl, &writer, 1, 3, "first");
        try flushRun(&dl, &writer, 4, 6, "second");
        path = try testing.allocator.dupe(u8, (try dl.segments())[0].path);
    }
    defer testing.allocator.free(path);
    {
        const file = try stdx.fs.openFile(path, .{ .mode = .read_write });
        defer stdx.fs.closeFile(file);
        size = try stdx.fs.fileLength(file);
        // A flipped payload byte in the second entry.
        const at = segment.HEADER_SIZE + entry_mod.HEADER_SIZE + "first".len + entry_mod.HEADER_SIZE + 1;
        try stdx.fs.writeAt(file, "Z", at);
    }
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    const r = try dl.recoverActive();
    try testing.expectEqual(@as(usize, 0), r.marked + r.removed);
    try testing.expectEqual(@as(u64, 0), r.cut_bytes);
    try testing.expectEqual(size, (try stdx.fs.statFile(path, .{})).size);
    try testing.expectEqual(@as(u64, 6), (try dl.segments())[0].last_index);
}

test "durable log: a seal cut short before its mark leaves an active segment, its index and footer cut at recovery" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var tail_len: usize = 0;
    {
        var dl = try DurableLog.init(testing.allocator, &writer, td.path);
        defer dl.deinit();
        try flushRun(&dl, &writer, 1, 4, "entries");
        const a = &dl.active.?;
        // The index and footer written, the mark sealing it not.
        var tail: std.ArrayList(u8) = .empty;
        defer tail.deinit(testing.allocator);
        try SegmentWriter.appendSealTail(testing.allocator, &tail, a.sparse.items, @intCast(a.data_size), &a.crc, &a.fixed, a.bitmap);
        try stdx.fs.writeAt(a.file, tail.items, segment.HEADER_SIZE + a.data_size);
        tail_len = tail.items.len;
    }
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    const r = try dl.recoverActive();
    try testing.expectEqual(@as(u64, tail_len), r.cut_bytes);
    try flushRun(&dl, &writer, 5, 6, "more");
    var buf: [8]Entry = undefined;
    var arena: [512]u8 = undefined;
    try testing.expectEqual(@as(usize, 6), dl.readRange(1, &buf, &arena).count);
}

test "durable log: recovery deletes an active segment no entry reached, and seals one that isn't the newest" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    {
        var dl = try DurableLog.init(testing.allocator, &writer, td.path);
        defer dl.deinit();
        try flushRun(&dl, &writer, 1, 3, "one");
        // An unsealed segment followed by another: never left by a run,
        // made here by hand.
        dl.closeActive();
        try dl.createActive(4, 0);
        try flushRun(&dl, &writer, 4, 5, "two");
        // And a newest made with nothing in it.
        dl.closeActive();
        try dl.createActive(6, 0);
    }
    try testing.expectEqual(@as(usize, 3), try countSegments(td.path));
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    const r = try dl.recoverActive();
    try testing.expectEqual(@as(usize, 1), r.removed);
    try testing.expectEqual(@as(usize, 1), r.sealed);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    // The empty newest gone, the one before it is the newest, left active.
    try testing.expectEqual(@as(usize, 2), files.len);
    try testing.expect(files[0].sealed and !files[1].sealed);
}

test "durable log: a cut inside the active segment shortens it and the next flush continues it; one below it deletes it" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    for (0..4) |k| try flushRun(&dl, &writer, k * 200 + 1, k * 200 + 200, "v");
    const cut_syncs = stdx.fs.dir_syncs.load(.monotonic);
    try dl.truncateAfter(450);
    // No intent and no rewrite: no directory sync, the same file.
    try testing.expectEqual(cut_syncs, stdx.fs.dir_syncs.load(.monotonic));
    try testing.expectEqual(@as(usize, 1), try countSegments(td.path));
    try testing.expectEqual(@as(u64, 450), dl.active.?.last_index);
    try testing.expectEqual(@as(u64, 450), dl.active.?.commit_index);
    // Its entries above the cut are gone; new ones follow it.
    var buf: [1]Entry = undefined;
    var arena: [64]u8 = undefined;
    try testing.expectEqual(@as(usize, 0), dl.readRange(451, &buf, &arena).count);
    try flushRun(&dl, &writer, 451, 460, "w");
    try testing.expectEqual(@as(usize, 1), dl.readRange(455, &buf, &arena).count);
    try testing.expectEqualStrings("w", buf[0].payload);
    // Sealed, its CRC summed again over what the cut kept.
    try dl.sealActive();
    const opened = try SegmentReader.initFromFile(testing.allocator, (try dl.segments())[0].path);
    testing.allocator.free(opened.buf);

    // A cut below a newer active segment deletes it whole.
    try flushRun(&dl, &writer, 461, 470, "z");
    try testing.expectEqual(@as(usize, 2), try countSegments(td.path));
    try dl.truncateAfter(455);
    try testing.expect(dl.active == null);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(usize, 1), files.len);
    try testing.expectEqual(@as(u64, 455), files[0].last_index);
}

test "durable log: a repair skip in the active segment is in its mark, and a cut that drops it takes it out" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    try flushRun(&dl, &writer, 1, 2, "a");
    var skip = entry_mod.buildEntry(.repair_skip, 0, 1, 3, 3000, "skip");
    skip.header.crc32c = skip.computeCrc();
    try writer.addEntry(&skip);
    try dl.flush(3);
    {
        const with = try dl.listSegments(true);
        defer dl.freeSegments(with);
        try testing.expect(with[0].holds_skip);
    }
    try dl.truncateAfter(2);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expect(!files[0].holds_skip);
}

/// Flip a byte in the mark slot that holds the newest mark, or in both.
fn tearMarks(path: []const u8, both: bool) !void {
    const hdr = try readHeaderBytes(path);
    const parsed = try segment.SegmentHeader.parse(&hdr);
    const file = try stdx.fs.openFile(path, .{ .mode = .read_write });
    defer stdx.fs.closeFile(file);
    for (segment.MARK_OFFSETS, 0..) |at, i| {
        if (!both and i != parsed.slot.?) continue;
        var b: [1]u8 = .{hdr[at + 20] ^ 0xff};
        try stdx.fs.writeAt(file, &b, at + 20);
    }
}

test "durable log: a torn newest mark falls back to the older one, and the entries past it are kept" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var path: []u8 = undefined;
    {
        var dl = try DurableLog.init(testing.allocator, &writer, td.path);
        defer dl.deinit();
        try flushRun(&dl, &writer, 1, 3, "first");
        try flushRun(&dl, &writer, 4, 6, "second");
        path = try testing.allocator.dupe(u8, (try dl.segments())[0].path);
    }
    defer testing.allocator.free(path);
    try tearMarks(path, false);
    // The older mark: the first flush's.
    try testing.expectEqual(@as(u64, 3), (try segment.SegmentHeader.parse(&(try readHeaderBytes(path)))).last_index);
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    const r = try dl.recoverActive();
    try testing.expectEqual(@as(usize, 1), r.marked);
    try testing.expectEqual(@as(u64, 0), r.cut_bytes);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(u64, 6), files[0].last_index);
    // The older mark's commit: the lag a lost mark leaves.
    try testing.expectEqual(@as(u64, 3), files[0].commit_index_at_seal);
}

test "durable log: both marks unreadable, the segment is rebuilt from its entries, with no commit trusted" {
    var td = try TestDir.init();
    defer td.deinit();
    var writer = SegmentWriter.init(testing.allocator, 0);
    defer writer.deinit();
    var path: []u8 = undefined;
    {
        var dl = try DurableLog.init(testing.allocator, &writer, td.path);
        defer dl.deinit();
        try flushRun(&dl, &writer, 1, 3, "first");
        try flushRun(&dl, &writer, 4, 6, "second");
        path = try testing.allocator.dupe(u8, (try dl.segments())[0].path);
    }
    defer testing.allocator.free(path);
    try tearMarks(path, true);
    var dl = try DurableLog.init(testing.allocator, &writer, td.path);
    defer dl.deinit();
    // Strictly listed, it is refused until recovered.
    try testing.expectError(error.NoValidMark, dl.listSegments(true));
    const r = try dl.recoverActive();
    try testing.expectEqual(@as(usize, 1), r.marked);
    const files = try dl.listSegments(true);
    defer dl.freeSegments(files);
    try testing.expectEqual(@as(u64, 6), files[0].last_index);
    try testing.expectEqual(@as(u64, 0), files[0].commit_index_at_seal);
    var buf: [8]Entry = undefined;
    var arena: [512]u8 = undefined;
    try testing.expectEqual(@as(usize, 6), dl.readRange(1, &buf, &arena).count);
}
