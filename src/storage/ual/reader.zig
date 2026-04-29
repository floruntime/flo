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

    /// Open a segment from raw bytes (in-memory).
    pub fn init(data: []const u8) error{ TooSmall, InvalidHeaderMagic, InvalidFooterMagic, InvalidVersion, InvalidCrc }!SegmentReader {
        const min_size = segment.HEADER_SIZE + segment.FOOTER_SIZE;
        if (data.len < min_size) return error.TooSmall;

        // Read header
        const hdr: *const segment.SegmentHeader = @ptrCast(@alignCast(data[0..segment.HEADER_SIZE]));
        if (!std.mem.eql(u8, &hdr.magic, &segment.HEADER_MAGIC)) return error.InvalidHeaderMagic;
        if (hdr.version != segment.SEGMENT_VERSION) return error.InvalidVersion;

        // Read footer
        const footer_start = data.len - segment.FOOTER_SIZE;
        const ftr: *const segment.Footer = @ptrCast(@alignCast(data[footer_start..][0..segment.FOOTER_SIZE]));
        if (!std.mem.eql(u8, &ftr.magic, &segment.FOOTER_MAGIC)) return error.InvalidFooterMagic;

        // Verify CRC (covers everything except the footer itself)
        const expected_crc = checksum_mod.checksum(data[0..footer_start]);
        if (ftr.crc32c != expected_crc) return error.InvalidCrc;

        // Decode sparse index
        const idx_offset: usize = ftr.sparse_index_offset;
        const idx_count: usize = ftr.sparse_index_count;
        const idx_end = idx_offset + idx_count * segment.SPARSE_ENTRY_SIZE;

        const sparse_ptr: [*]const segment.SparseIndexEntry = @ptrCast(@alignCast(data[idx_offset..idx_end]));
        const sparse_index = sparse_ptr[0..idx_count];

        return .{
            .data = data,
            .header = hdr.*,
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
        if (read != file_size) {
            allocator.free(buf);
            return error.ShortRead;
        }

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
            if (entry.header.index == target_index) return entry;
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
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const writer_mod = @import("writer.zig");

fn makeTestEntry(index: u64, payload: []const u8) Entry {
    return entry_mod.buildEntry(.kv_put, 0, 1, index, @as(u64, index) * 1000, payload);
}

test "reader: open empty segment" {
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0, .none);
    defer w.deinit();

    const sealed = try w.seal();
    defer std.testing.allocator.free(sealed);

    const reader = try SegmentReader.init(sealed);
    try std.testing.expectEqual(@as(u32, 0), reader.header.entry_count);
}

test "reader: write and read back entries" {
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0, .none);
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
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0, .none);
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
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0, .none);
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
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0, .none);
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
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0, .none);
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
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0, .none);
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
    var w = writer_mod.SegmentWriter.init(std.testing.allocator, 0, .none);
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
