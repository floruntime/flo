//! UAL Segment Reader — reads entries from a sealed segment
//!
//! Opens a `.flseg` file, validates header and footer, and provides:
//! - O(log N) lookup by UAL index via sparse index binary search
//! - Sequential scan of all entries
//! - Entry-type filtering via the bitmap in the footer

const std = @import("std");
const segment = @import("segment.zig");
const entry_mod = @import("entry.zig");
const checksum_mod = @import("../../util/checksum.zig");

const Entry = entry_mod.Entry;
const ENTRY_HEADER_SIZE = entry_mod.HEADER_SIZE;

// ═══════════════════════════════════════════════════════════════════════════════
// SegmentReader
// ═══════════════════════════════════════════════════════════════════════════════

pub const SegmentReader = struct {
    data: []const u8,
    header: segment.SegmentHeader,
    footer: segment.Footer,
    sparse_index: []const segment.SparseIndexEntry,

    /// Byte offset where entry data begins (after segment header).
    data_start: usize,
    /// Byte offset where entry data ends (before sparse index).
    data_end: usize,

    pub const InitError = segment.SegmentHeader.Error || error{ InvalidFooterMagic, InvalidCrc, InvalidIndex, NoValidMark };

    /// Open a segment from raw bytes (in-memory): a sealed one, its footer
    /// CRC checked, or an active one, read to where its mark says its data
    /// ends. An active segment has no sparse index and no type bitmap; its
    /// entries carry their own CRCs.
    pub fn init(data: []const u8) InitError!SegmentReader {
        const hdr = try segment.SegmentHeader.parse(data);
        if (hdr.slot == null) return error.NoValidMark;
        if (!hdr.sealed()) {
            const end = segment.HEADER_SIZE + @as(usize, hdr.data_size);
            if (end > data.len) return error.TooSmall;
            return .{
                .data = data,
                .header = hdr,
                // Every type may be present.
                .footer = .{ .sparse_index_offset = @intCast(end), .sparse_index_count = 0, .entry_type_bitmap = std.math.maxInt(u32), .bloom_filter_offset = 0, .crc32c = 0, ._padding = @splat(0), .magic = segment.FOOTER_MAGIC },
                .sparse_index = &.{},
                .data_start = segment.HEADER_SIZE,
                .data_end = end,
            };
        }
        if (data.len < segment.HEADER_SIZE + segment.FOOTER_SIZE) return error.TooSmall;

        // Read footer
        const footer_start = data.len - segment.FOOTER_SIZE;
        const ftr: *const segment.Footer = @ptrCast(@alignCast(data[footer_start..][0..segment.FOOTER_SIZE]));
        if (!std.mem.eql(u8, &ftr.magic, &segment.FOOTER_MAGIC)) return error.InvalidFooterMagic;

        // Verify CRC: the data and sparse index, then the fixed header part.
        if (ftr.crc32c != segment.footerCrc(data[segment.HEADER_SIZE..footer_start], &hdr.fixed_bytes)) return error.InvalidCrc;

        // Decode sparse index
        const idx_offset: usize = ftr.sparse_index_offset;
        const idx_count: usize = ftr.sparse_index_count;
        const idx_end = idx_offset + idx_count * segment.SPARSE_ENTRY_SIZE;
        if (idx_offset != segment.HEADER_SIZE + @as(usize, hdr.data_size) or idx_end != footer_start) return error.InvalidIndex;

        const sparse_ptr: [*]const segment.SparseIndexEntry = @ptrCast(@alignCast(data[idx_offset..idx_end]));
        const sparse_index = sparse_ptr[0..idx_count];

        return .{
            .data = data,
            .header = hdr,
            .footer = ftr.*,
            .sparse_index = sparse_index,
            .data_start = segment.HEADER_SIZE,
            .data_end = idx_offset,
        };
    }

    /// Open a segment from a file.
    pub fn initFromFile(allocator: std.mem.Allocator, path: []const u8) !struct { reader: SegmentReader, buf: []u8 } {
        const stdx = @import("stdx");
        const io = stdx.io.instance();
        const file = try stdx.fs.openFile(path, .{});
        defer stdx.fs.closeFile(file);

        const file_size = try file.length(io);
        const buf = try allocator.alloc(u8, @intCast(file_size));
        errdefer allocator.free(buf);

        const read = try stdx.fs.readAll(file, buf);
        if (read != file_size) return error.ShortRead;

        const reader = try init(buf);
        return .{ .reader = reader, .buf = buf };
    }

    /// Check if the segment might contain entries of a given type.
    pub fn hasEntryType(self: *const SegmentReader, entry_type: u8) bool {
        return segment.bitmapHas(self.footer.entry_type_bitmap, entry_type);
    }

    /// Check if a UAL index is in this segment's range.
    pub fn containsIndex(self: *const SegmentReader, index: u64) bool {
        return index >= self.header.first_index and index <= self.header.last_index;
    }

    /// Read the entry at a given byte offset within the data section.
    /// Returns null if the data is truncated or magic doesn't match.
    pub fn readEntryAt(self: *const SegmentReader, data_offset: usize) ?Entry {
        const abs_offset = self.data_start + data_offset;
        if (abs_offset + ENTRY_HEADER_SIZE > self.data_end) return null;

        const remaining = self.data[abs_offset..self.data_end];
        return Entry.deserialize(remaining);
    }

    /// Find an entry by UAL index using the sparse index for O(log N) lookup.
    /// Returns null if not found.
    pub fn findByIndex(self: *const SegmentReader, target_index: u64) ?Entry {
        const offset = self.findOffset(target_index) orelse return null;
        return self.readEntryAt(offset);
    }

    /// Data-section offset of the entry at `target_index`, so a caller can
    /// keep reading sequentially from there.
    pub fn findOffset(self: *const SegmentReader, target_index: u64) ?usize {
        if (!self.containsIndex(target_index)) return null;

        // Binary search sparse index for the largest entry ≤ target
        var start_offset: usize = 0;
        if (self.sparse_index.len > 0) {
            var lo: usize = 0;
            var hi: usize = self.sparse_index.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (self.sparse_index[mid].index <= target_index) {
                    start_offset = self.sparse_index[mid].offset;
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
        }

        // Linear scan from start_offset
        var offset = start_offset;
        while (offset < self.data_end - self.data_start) {
            const entry = self.readEntryAt(offset) orelse break;
            if (entry.header.index == target_index) return offset;
            if (entry.header.index > target_index) break; // past it
            offset += entry.totalSize();
        }

        return null;
    }

    /// Iterate all entries sequentially. Calls `callback` for each.
    /// If filter_type is non-null, only entries of that type are yielded.
    pub fn scan(
        self: *const SegmentReader,
        filter_type: ?u8,
        callback: *const fn (entry: Entry) void,
    ) void {
        // Quick check: if filtering and bitmap says this type isn't here, skip
        if (filter_type) |ft| {
            if (!self.hasEntryType(ft)) return;
        }

        var offset: usize = 0;
        while (offset < self.data_end - self.data_start) {
            const entry = self.readEntryAt(offset) orelse break;
            if (filter_type) |ft| {
                if (entry.header.entry_type == ft) {
                    callback(entry);
                }
            } else {
                callback(entry);
            }
            offset += entry.totalSize();
        }
    }

    /// Collect all entries into a caller-provided slice. Returns count.
    pub fn readAll(self: *const SegmentReader, results: []Entry) usize {
        var count: usize = 0;
        var offset: usize = 0;
        while (offset < self.data_end - self.data_start and count < results.len) {
            const entry = self.readEntryAt(offset) orelse break;
            results[count] = entry;
            count += 1;
            offset += entry.totalSize();
        }
        return count;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// PositionedSegment
// ═══════════════════════════════════════════════════════════════════════════════

/// A sealed segment opened to read a few entries: its file, bounds and
/// sparse index, without its entries. A read seeks to the nearest indexed
/// entry and reads a window from there, so a record costs what is near it,
/// not the whole file. The whole-file CRC can't be checked that way; each
/// entry a read returns is checked against its own CRC instead. A segment
/// of at most `WHOLE_MAX` bytes is read whole when opened, its whole-file
/// CRC checked, as one read costs less than several small ones.
pub const PositionedSegment = struct {
    /// The open file, for a large segment; a small one is closed once read.
    file: ?std.Io.File = null,
    /// The whole file, for a small segment.
    whole: ?[]u8 = null,
    first_index: u64,
    last_index: u64,
    /// File offsets of the entry data section.
    data_start: u64,
    data_end: u64,
    sparse: []segment.SparseIndexEntry,

    pub const WHOLE_MAX = 64 * 1024;

    pub fn open(allocator: std.mem.Allocator, path: []const u8) !PositionedSegment {
        const stdx = @import("stdx");
        const file = try stdx.fs.openFile(path, .{});
        errdefer stdx.fs.closeFile(file);
        const size = try file.length(stdx.io.instance());
        if (size < segment.HEADER_SIZE) return error.TooSmall;
        if (size <= WHOLE_MAX) {
            const whole = try allocator.alloc(u8, @intCast(size));
            errdefer allocator.free(whole);
            if (try stdx.fs.readAt(file, whole, 0) != whole.len) return error.ShortRead;
            const reader = try SegmentReader.init(whole);
            if (reader.header.compression != @backingInt(segment.Compression.none)) return error.Compressed;
            const sparse = try allocator.dupe(segment.SparseIndexEntry, reader.sparse_index);
            // Everything is in memory: no fd is held for it.
            stdx.fs.closeFile(file);
            return .{
                .whole = whole,
                .first_index = reader.header.first_index,
                .last_index = reader.header.last_index,
                .data_start = reader.data_start,
                .data_end = reader.data_end,
                .sparse = sparse,
            };
        }

        var hdr_bytes: [segment.HEADER_SIZE]u8 = undefined;
        if (try stdx.fs.readAt(file, &hdr_bytes, 0) != hdr_bytes.len) return error.ShortRead;
        const hdr = try segment.SegmentHeader.parse(&hdr_bytes);
        if (hdr.slot == null) return error.NoValidMark;
        if (hdr.compression != @backingInt(segment.Compression.none)) return error.Compressed;
        // Active: no index or footer yet; read from the start, to where its
        // mark says the data ends.
        if (!hdr.sealed()) {
            const end = segment.HEADER_SIZE + @as(u64, hdr.data_size);
            if (end > size) return error.TooSmall;
            return .{
                .file = file,
                .first_index = hdr.first_index,
                .last_index = hdr.last_index,
                .data_start = segment.HEADER_SIZE,
                .data_end = end,
                .sparse = try allocator.alloc(segment.SparseIndexEntry, 0),
            };
        }

        var ftr_bytes: [segment.FOOTER_SIZE]u8 = undefined;
        const footer_start = size - segment.FOOTER_SIZE;
        if (try stdx.fs.readAt(file, &ftr_bytes, footer_start) != ftr_bytes.len) return error.ShortRead;
        const ftr: *const segment.Footer = @ptrCast(&ftr_bytes);
        if (!std.mem.eql(u8, &ftr.magic, &segment.FOOTER_MAGIC)) return error.InvalidFooterMagic;

        const idx_offset: u64 = ftr.sparse_index_offset;
        const idx_count: usize = ftr.sparse_index_count;
        if (idx_offset < segment.HEADER_SIZE or idx_offset + idx_count * segment.SPARSE_ENTRY_SIZE != footer_start) return error.InvalidIndex;
        // The header's data size and the footer's index offset must agree.
        if (segment.HEADER_SIZE + @as(u64, hdr.data_size) != idx_offset) return error.InvalidIndex;
        const sparse = try allocator.alloc(segment.SparseIndexEntry, idx_count);
        errdefer allocator.free(sparse);
        const sparse_bytes = std.mem.sliceAsBytes(sparse);
        if (try stdx.fs.readAt(file, sparse_bytes, idx_offset) != sparse_bytes.len) return error.ShortRead;
        for (sparse) |e| if (e.offset > idx_offset - segment.HEADER_SIZE) return error.InvalidIndex;

        return .{
            .file = file,
            .first_index = hdr.first_index,
            .last_index = hdr.last_index,
            .data_start = segment.HEADER_SIZE,
            .data_end = idx_offset,
            .sparse = sparse,
        };
    }

    pub fn close(self: *PositionedSegment, allocator: std.mem.Allocator) void {
        if (self.file) |f| @import("stdx").fs.closeFile(f);
        if (self.whole) |w| allocator.free(w);
        allocator.free(self.sparse);
    }

    /// `dst.len` bytes from file offset `at`: from memory for a small
    /// segment, else from the file.
    fn bytesAt(self: *const PositionedSegment, dst: []u8, at: u64) !void {
        if (self.whole) |w| {
            if (at + dst.len > w.len) return error.ShortRead;
            const from: usize = @intCast(at);
            @memcpy(dst, w[from..][0..dst.len]);
            return;
        }
        if (try @import("stdx").fs.readAt(self.file.?, dst, at) != dst.len) return error.ShortRead;
    }

    /// File offset of the last indexed entry at or below `index`.
    fn seekOffset(self: *const PositionedSegment, index: u64) u64 {
        var offset: u64 = 0;
        var lo: usize = 0;
        var hi: usize = self.sparse.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.sparse[mid].index <= index) {
                offset = self.sparse[mid].offset;
                lo = mid + 1;
            } else hi = mid;
        }
        return self.data_start + offset;
    }

    pub const Read = struct {
        count: usize = 0,
        /// The segment covers `start` but holds no entry with that index.
        missing: bool = false,
    };

    /// Copy contiguous entries from `start` into `buf`, payloads packed into
    /// `arena`, through `scratch` (any size; an entry bigger than it is read
    /// straight into `arena`). Stops at the segment's end, a full `buf` or
    /// `arena`, or a gap. An entry whose CRC doesn't match fails the read.
    pub fn read(self: *const PositionedSegment, start: u64, buf: []Entry, arena: []u8, scratch: []u8) !Read {
        if (start < self.first_index or start > self.last_index or buf.len == 0) return .{};
        std.debug.assert(scratch.len >= ENTRY_HEADER_SIZE);
        var cursor = self.seekOffset(start);
        var count: usize = 0;
        var arena_used: usize = 0;
        while (count < buf.len and cursor < self.data_end) {
            const want: usize = @intCast(@min(scratch.len, self.data_end - cursor));
            // Stray bytes too short for an entry: damage, but what was read
            // before them checked out, so it's returned.
            if (want < ENTRY_HEADER_SIZE) return if (count > 0) .{ .count = count } else error.ShortRead;
            try self.bytesAt(scratch[0..want], cursor);
            var off: usize = 0;
            while (off + ENTRY_HEADER_SIZE <= want) {
                const e = Entry.deserialize(scratch[off..want]) orelse break;
                if (e.header.index >= start) {
                    if (e.header.index != start + count) return .{ .count = count, .missing = count == 0 };
                    if (e.computeCrc() != e.header.crc32c) return error.InvalidCrc;
                    if (arena_used + e.payload.len > arena.len) return .{ .count = count };
                    @memcpy(arena[arena_used..][0..e.payload.len], e.payload);
                    buf[count] = .{ .header = e.header, .payload = arena[arena_used..][0..e.payload.len] };
                    arena_used += e.payload.len;
                    count += 1;
                }
                off += e.totalSize();
                if (count == buf.len) return .{ .count = count };
            }
            if (off > 0) {
                cursor += off;
                continue;
            }
            // The entry here doesn't fit `scratch`: its header does.
            const hdr: *align(1) const entry_mod.Header = @ptrCast(scratch[0..ENTRY_HEADER_SIZE]);
            const total = ENTRY_HEADER_SIZE + @as(u64, hdr.payload_len);
            if (cursor + total > self.data_end) return error.ShortRead;
            if (hdr.index >= start) {
                if (hdr.index != start + count) return .{ .count = count, .missing = count == 0 };
                if (arena_used + hdr.payload_len > arena.len) return .{ .count = count };
                const payload = arena[arena_used..][0..hdr.payload_len];
                try self.bytesAt(payload, cursor + ENTRY_HEADER_SIZE);
                const e: Entry = .{ .header = hdr.*, .payload = payload };
                if (e.computeCrc() != e.header.crc32c) return error.InvalidCrc;
                buf[count] = e;
                arena_used += payload.len;
                count += 1;
            }
            cursor += total;
        }
        return .{ .count = count, .missing = count == 0 };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const writer_mod = @import("writer.zig");

fn makeTestEntry(index: u64, payload: []const u8) Entry {
    return entry_mod.buildEntry(.kv_put, 0, 1, index, @as(u64, index) * 1000, payload);
}

test "reader: open empty segment" {
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0);
    defer w.deinit();

    const sealed = try w.seal();
    defer std.testing.allocator.free(sealed);

    const reader = try SegmentReader.init(sealed);
    try std.testing.expectEqual(@as(u32, 0), reader.header.entry_count);
}

test "reader: write and read back entries" {
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0);
    defer w.deinit();

    const e1 = makeTestEntry(1, "hello");
    const e2 = makeTestEntry(2, "world");
    const e3 = makeTestEntry(3, "!");

    try w.addEntry(&e1);
    try w.addEntry(&e2);
    try w.addEntry(&e3);

    const sealed = try w.seal();
    defer std.testing.allocator.free(sealed);

    const reader = try SegmentReader.init(sealed);
    try std.testing.expectEqual(@as(u32, 3), reader.header.entry_count);

    // Read all entries
    var results: [10]Entry = undefined;
    const count = reader.readAll(&results);
    try std.testing.expectEqual(@as(usize, 3), count);
    try std.testing.expectEqual(@as(u64, 1), results[0].header.index);
    try std.testing.expectEqualStrings("hello", results[0].payload);
    try std.testing.expectEqual(@as(u64, 2), results[1].header.index);
    try std.testing.expectEqual(@as(u64, 3), results[2].header.index);
}

test "reader: findByIndex" {
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0);
    defer w.deinit();

    var i: u64 = 1;
    while (i <= 20) : (i += 1) {
        const entry = makeTestEntry(i, "data");
        try w.addEntry(&entry);
    }

    const sealed = try w.seal();
    defer std.testing.allocator.free(sealed);

    const reader = try SegmentReader.init(sealed);

    // Find middle entry
    const found = reader.findByIndex(10);
    try std.testing.expect(found != null);
    try std.testing.expectEqual(@as(u64, 10), found.?.header.index);

    // Find first
    const first = reader.findByIndex(1);
    try std.testing.expect(first != null);
    try std.testing.expectEqual(@as(u64, 1), first.?.header.index);

    // Find last
    const last = reader.findByIndex(20);
    try std.testing.expect(last != null);

    // Not found
    const nope = reader.findByIndex(999);
    try std.testing.expectEqual(@as(?Entry, null), nope);
}

test "reader: CRC validation rejects corrupt data" {
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0);
    defer w.deinit();

    const entry = makeTestEntry(1, "test");
    try w.addEntry(&entry);

    const sealed = try w.seal();
    defer std.testing.allocator.free(sealed);

    // Corrupt a byte in the entry data
    sealed[segment.HEADER_SIZE + 10] ^= 0xFF;

    const result = SegmentReader.init(sealed);
    try std.testing.expectError(error.InvalidCrc, result);
}

test "reader: containsIndex" {
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0);
    defer w.deinit();

    const e1 = makeTestEntry(10, "a");
    const e2 = makeTestEntry(20, "b");
    try w.addEntry(&e1);
    try w.addEntry(&e2);

    const sealed = try w.seal();
    defer std.testing.allocator.free(sealed);

    const reader = try SegmentReader.init(sealed);
    try std.testing.expect(reader.containsIndex(10));
    try std.testing.expect(reader.containsIndex(15)); // within range
    try std.testing.expect(reader.containsIndex(20));
    try std.testing.expect(!reader.containsIndex(9));
    try std.testing.expect(!reader.containsIndex(21));
}

test "reader: hasEntryType bitmap check" {
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0);
    defer w.deinit();

    const kv = entry_mod.buildEntry(.kv_put, 0, 1, 1, 0, "a");
    try w.addEntry(&kv);

    const sealed = try w.seal();
    defer std.testing.allocator.free(sealed);

    const reader = try SegmentReader.init(sealed);
    try std.testing.expect(reader.hasEntryType(0x01)); // kv_put
    try std.testing.expect(!reader.hasEntryType(0x10)); // stream_append
}

test "reader: sparse index used for large segments" {
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0);
    defer w.deinit();

    // 300 entries → sparse index has entries at 0 and 256
    var i: u64 = 1;
    while (i <= 300) : (i += 1) {
        const entry = makeTestEntry(i, "dd");
        try w.addEntry(&entry);
    }

    const sealed = try w.seal();
    defer std.testing.allocator.free(sealed);

    const reader = try SegmentReader.init(sealed);
    try std.testing.expectEqual(@as(usize, 2), reader.sparse_index.len);

    // Find entry 290 — should use sparse index to skip ahead
    const found = reader.findByIndex(290);
    try std.testing.expect(found != null);
    try std.testing.expectEqual(@as(u64, 290), found.?.header.index);
}

test "reader: individual entry CRC still valid" {
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0);
    defer w.deinit();

    const entry = makeTestEntry(1, "verify_me");
    try w.addEntry(&entry);

    const sealed = try w.seal();
    defer std.testing.allocator.free(sealed);

    const reader = try SegmentReader.init(sealed);

    var results: [1]Entry = undefined;
    const count = reader.readAll(&results);
    try std.testing.expectEqual(@as(usize, 1), count);

    // Validate the entry's own CRC
    try results[0].validate();
}

test "reader: a truncated segment file is refused, its buffer freed once" {
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0);
    defer w.deinit();
    try w.addEntry(&makeTestEntry(1, "hello"));
    try w.addEntry(&makeTestEntry(2, "world"));
    const sealed = try w.seal();
    defer std.testing.allocator.free(sealed);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try @import("stdx").fs.dirRealpathAlloc(tmp.dir, std.testing.allocator, ".");
    defer std.testing.allocator.free(dir);
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/cut.flseg", .{dir});
    defer std.testing.allocator.free(path);
    // Cut off its footer: the file reads whole, and doesn't parse.
    const f = try @import("stdx").fs.createFileAbsolute(path, .{});
    try @import("stdx").fs.writeAll(f, sealed[0 .. sealed.len - 8]);
    @import("stdx").fs.closeFile(f);
    // The testing allocator fails the test on a leak or a double free.
    try std.testing.expect(std.meta.isError(SegmentReader.initFromFile(std.testing.allocator, path)));
}
