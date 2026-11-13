//! UAL Segment — on-disk segment format
//!
//! A segment file (`.flseg`) holds sequential UAL entries. The newest one is
//! active: each flush appends its entries and then records where the data
//! ends in the header. At 64 MiB it is sealed: a sparse index for O(log N)
//! lookups and a footer for integrity are appended, and it is never
//! appended to again, though a log truncation rewrites the one it cuts
//! through (`DurableLog`).
//!
//! ## Layout
//!
//! ```
//! [Fixed part: block 0][Mark A: block 1][Mark B: block 2]   ← 4 KiB each
//! [Entry 0][Entry 1]...[Entry N]     ← sequential UAL entries
//! [SparseIndex: 12B × count]         ← sealed only: every 256th entry
//! [Footer: 32B]                      ← sealed only
//! ```
//!
//! ## Marks
//!
//! What a flush changes (where the data ends, the last index, the commit
//! index it was flushed under, whether it is sealed) lives in a mark, not
//! the fixed part. The header holds two; each update writes the one not
//! holding the newest, with a higher sequence number and its own CRC, so a
//! write the power cut short loses at most that update. The fixed part and
//! each mark have a 4 KiB block of their own: a device writes whole
//! blocks and may tear one, so a mark's write never rewrites the block
//! holding the fixed part, or the other mark. A reader takes the
//! valid mark with the higher sequence number. A mark is written after the
//! entries it describes are on disk, so it may be behind them, never ahead:
//! recovery finds entries past it by their own CRCs.

const std = @import("std");
const checksum_mod = @import("../../util/checksum.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

pub const HEADER_MAGIC: [8]u8 = .{ 'F', 'L', 'O', '_', 'S', 'E', 'G', 0 };
pub const FOOTER_MAGIC: [8]u8 = .{ 'F', 'L', 'O', '_', 'E', 'N', 'D', 0 };
/// Bumped with any change to a stored entry's layout (the entry table) or
/// the segment's own, so a data directory written before it is refused at
/// open, not misread. node/data_format.zig pins the entry layouts this
/// version stores, and the data directory format that goes with it.
pub const SEGMENT_VERSION: u16 = 13;
/// A device writes whole blocks: the fixed part and each mark get one.
pub const BLOCK_SIZE: usize = 4096;
/// The fixed part's block and the two marks' blocks; entry data starts after.
pub const HEADER_SIZE: usize = 3 * BLOCK_SIZE;
pub const FOOTER_SIZE: usize = 32;
pub const SPARSE_INDEX_INTERVAL: u32 = 256;
/// An active segment is sealed once its data reaches this.
pub const DEFAULT_MAX_SEGMENT_SIZE: usize = 64 * 1024 * 1024; // 64 MB

// ═══════════════════════════════════════════════════════════════════════════════
// Compression
// ═══════════════════════════════════════════════════════════════════════════════

pub const Compression = enum(u8) {
    none = 0,
    lz4 = 1,
    zstd = 2,
};

// ═══════════════════════════════════════════════════════════════════════════════
// SegmentHeader (three 4 KiB blocks on disk: a fixed part and two marks)
// ═══════════════════════════════════════════════════════════════════════════════

/// A segment holding a `repair_skip` entry.
pub const FLAG_HOLDS_SKIP: u8 = 1;
/// The segment is sealed: its sparse index and footer follow the data.
pub const FLAG_SEALED: u8 = 2;

/// Written once, when the file is created.
pub const Fixed = extern struct {
    magic: [8]u8 align(1),
    version: u16 align(1),
    compression: u8 align(1),
    _pad: u8 align(1) = 0,
    partition_id: u32 align(1),
    first_index: u64 align(1),
    first_ts_ns: u64 align(1),
    _reserved: [8]u8 align(1) = @splat(0),
};
pub const FIXED_SIZE: usize = 40;

/// What the segment holds as of one flush.
pub const Mark = extern struct {
    /// Which of the two marks is newer; 0 is never written.
    seq: u32 align(1),
    flags: u8 align(1),
    _pad: [3]u8 align(1) = @splat(0),
    /// Bytes of entry data after the header.
    data_size: u32 align(1),
    entry_count: u32 align(1),
    last_index: u64 align(1),
    last_ts_ns: u64 align(1),
    /// The Raft commit index the data was flushed under. A restart applies
    /// entries at or below the highest over all segments; anything above
    /// goes into the log only and is applied once commit is re-established,
    /// since a new leader may truncate it.
    commit_index: u64 align(1),
    crc32c: u32 align(1),

    pub fn withCrc(m: Mark) Mark {
        var out = m;
        out.crc32c = checksum_mod.checksum(std.mem.asBytes(&m)[0..MARK_CRC_SPAN]);
        return out;
    }

    pub fn valid(m: *const Mark) bool {
        return m.seq != 0 and m.crc32c == checksum_mod.checksum(std.mem.asBytes(m)[0..MARK_CRC_SPAN]);
    }
};
pub const MARK_SIZE: usize = 44;
const MARK_CRC_SPAN = MARK_SIZE - 4;
/// File offsets of the two marks, each at the start of its own block.
pub const MARK_OFFSETS = [2]usize{ BLOCK_SIZE, 2 * BLOCK_SIZE };

comptime {
    if (@sizeOf(Fixed) != FIXED_SIZE) @compileError("segment Fixed must be 40 bytes");
    if (@sizeOf(Mark) != MARK_SIZE) @compileError("segment Mark must be 44 bytes");
}

/// A header as read: the fixed part and its newest valid mark.
pub const SegmentHeader = struct {
    partition_id: u32,
    first_index: u64,
    first_ts_ns: u64,
    compression: u8,
    /// The newest valid mark's slot (0 or 1), so the next update writes the
    /// other one; null when neither is valid.
    slot: ?u1,
    seq: u32,
    flags: u8,
    data_size: u32,
    entry_count: u32,
    last_index: u64,
    last_ts_ns: u64,
    commit_index_at_seal: u64,
    /// The fixed part's bytes, which a sealed segment's footer CRC covers.
    fixed_bytes: [FIXED_SIZE]u8,

    pub fn sealed(self: *const SegmentHeader) bool {
        return self.slot != null and self.flags & FLAG_SEALED != 0;
    }

    pub fn holdsSkip(self: *const SegmentHeader) bool {
        return self.flags & FLAG_HOLDS_SKIP != 0;
    }

    pub const Error = error{ TooSmall, InvalidHeaderMagic, InvalidVersion };

    /// The fixed part and the newer of the valid marks. A header with no
    /// valid mark parses, with `slot` null: what it holds is unknown and
    /// only a scan of its entries can tell.
    pub fn parse(bytes: []const u8) Error!SegmentHeader {
        if (bytes.len < HEADER_SIZE) return error.TooSmall;
        const fixed: *const Fixed = @ptrCast(bytes[0..FIXED_SIZE]);
        if (!std.mem.eql(u8, &fixed.magic, &HEADER_MAGIC)) return error.InvalidHeaderMagic;
        if (fixed.version != SEGMENT_VERSION) return error.InvalidVersion;
        var out: SegmentHeader = .{
            .partition_id = fixed.partition_id,
            .first_index = fixed.first_index,
            .first_ts_ns = fixed.first_ts_ns,
            .compression = fixed.compression,
            .slot = null,
            .seq = 0,
            .flags = 0,
            .data_size = 0,
            .entry_count = 0,
            .last_index = 0,
            .last_ts_ns = 0,
            .commit_index_at_seal = 0,
            .fixed_bytes = bytes[0..FIXED_SIZE].*,
        };
        for (MARK_OFFSETS, 0..) |at, i| {
            const m: *const Mark = @ptrCast(bytes[at..][0..MARK_SIZE]);
            if (!m.valid()) continue;
            if (out.slot != null and m.seq <= out.seq) continue;
            out.slot = @intCast(i);
            out.seq = m.seq;
            out.flags = m.flags;
            out.data_size = m.data_size;
            out.entry_count = m.entry_count;
            out.last_index = m.last_index;
            out.last_ts_ns = m.last_ts_ns;
            out.commit_index_at_seal = m.commit_index;
        }
        return out;
    }
};

/// A whole header: the fixed part, `mark` in slot 0 and slot 1 empty.
/// Caller writes all of it once, when the file is created.
pub fn encodeHeader(fixed: Fixed, mark: Mark) [HEADER_SIZE]u8 {
    var out: [HEADER_SIZE]u8 = @splat(0);
    @memcpy(out[0..FIXED_SIZE], std.mem.asBytes(&fixed));
    @memcpy(out[MARK_OFFSETS[0]..][0..MARK_SIZE], std.mem.asBytes(&mark.withCrc()));
    return out;
}

pub fn fixedFor(partition_id: u32, first_index: u64, first_ts_ns: u64) Fixed {
    return .{
        .magic = HEADER_MAGIC,
        .version = SEGMENT_VERSION,
        // Nothing compresses yet; the header keeps the byte for when
        // something does.
        .compression = @backingInt(Compression.none),
        .partition_id = partition_id,
        .first_index = first_index,
        .first_ts_ns = first_ts_ns,
    };
}

/// A sealed segment's footer CRC: its data and sparse index, then its
/// fixed header part. The marks have CRCs of their own, and an active
/// segment's data is summed as it is appended, so sealing reads nothing
/// back.
pub fn footerCrc(data_and_index: []const u8, fixed_bytes: []const u8) u32 {
    var stream = checksum_mod.ChecksumStream.init();
    stream.add(data_and_index);
    stream.add(fixed_bytes);
    return stream.checksum();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Footer (32 bytes, extern for exact layout)
// ═══════════════════════════════════════════════════════════════════════════════

pub const Footer = extern struct {
    sparse_index_offset: u32 align(1), // byte offset from file start
    sparse_index_count: u32 align(1),
    entry_type_bitmap: u32 align(1), // bit N = entry_type N present
    bloom_filter_offset: u32 align(1), // 0 if no bloom filter
    crc32c: u32 align(1), // CRC of entire file (header + data + index)
    _padding: [4]u8 align(1), // padding to 32B
    magic: [8]u8 align(1),

    comptime {
        if (@sizeOf(Footer) != FOOTER_SIZE) {
            @compileError("Footer must be exactly 32 bytes");
        }
    }

    pub fn asBytes(self: *const Footer) *const [FOOTER_SIZE]u8 {
        return @ptrCast(self);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Sparse Index Entry (12 bytes)
// ═══════════════════════════════════════════════════════════════════════════════

pub const SPARSE_ENTRY_SIZE: usize = 12;

pub const SparseIndexEntry = extern struct {
    index: u64 align(1), // UAL index
    offset: u32 align(1), // byte offset from start of entry data section

    comptime {
        if (@sizeOf(SparseIndexEntry) != SPARSE_ENTRY_SIZE) {
            @compileError("SparseIndexEntry must be exactly 12 bytes");
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Batch Header (for head.ual active write file)
// ═══════════════════════════════════════════════════════════════════════════════

pub const BATCH_MAGIC: u32 = 0x0A10_B001;
pub const BATCH_HEADER_SIZE: usize = 16;

pub const BatchHeader = extern struct {
    magic: u32 align(1),
    batch_len: u32 align(1), // total bytes of entries in this batch
    entry_count: u32 align(1),
    crc32c: u32 align(1), // CRC of all entry bytes

    comptime {
        if (@sizeOf(BatchHeader) != BATCH_HEADER_SIZE) {
            @compileError("BatchHeader must be exactly 16 bytes");
        }
    }

    pub fn asBytes(self: *const BatchHeader) *const [BATCH_HEADER_SIZE]u8 {
        return @ptrCast(self);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Generate a segment filename from the first index.
pub fn segmentFilename(buf: []u8, first_index: u64) ?[]const u8 {
    const result = std.fmt.bufPrint(buf, "{d:0>10}.flseg", .{first_index}) catch return null;
    return result;
}

/// Set the bit for an entry type in the bitmap.
pub fn bitmapSet(bitmap: u32, entry_type: u8) u32 {
    return bitmap | (@as(u32, 1) << @intCast(entry_type & 0x1F));
}

/// Check if an entry type is present in the bitmap.
pub fn bitmapHas(bitmap: u32, entry_type: u8) bool {
    return (bitmap & (@as(u32, 1) << @intCast(entry_type & 0x1F))) != 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "segment: the header is a fixed part and two marks, each in its own block" {
    try std.testing.expectEqual(@as(usize, 3 * 4096), HEADER_SIZE);
    try std.testing.expect(MARK_OFFSETS[0] >= FIXED_SIZE and MARK_OFFSETS[1] - MARK_OFFSETS[0] >= BLOCK_SIZE);
    try std.testing.expectEqual(@as(usize, 40), @sizeOf(Fixed));
    try std.testing.expectEqual(@as(usize, 44), @sizeOf(Mark));
}

test "segment: a header reads its newest valid mark, and a torn newer one leaves the older" {
    var bytes = encodeHeader(fixedFor(3, 10, 99), .{ .seq = 1, .flags = 0, .data_size = 100, .entry_count = 2, .last_index = 11, .last_ts_ns = 5, .commit_index = 10, .crc32c = 0 });
    var h = try SegmentHeader.parse(&bytes);
    try std.testing.expectEqual(@as(?u1, 0), h.slot);
    try std.testing.expectEqual(@as(u64, 11), h.last_index);
    // The next update goes in slot 1, with a higher sequence number.
    const next = (Mark{ .seq = 2, .flags = FLAG_SEALED, .data_size = 200, .entry_count = 4, .last_index = 13, .last_ts_ns = 6, .commit_index = 12, .crc32c = 0 }).withCrc();
    @memcpy(bytes[MARK_OFFSETS[1]..][0..MARK_SIZE], std.mem.asBytes(&next));
    h = try SegmentHeader.parse(&bytes);
    try std.testing.expectEqual(@as(?u1, 1), h.slot);
    try std.testing.expectEqual(@as(u64, 13), h.last_index);
    try std.testing.expect(h.sealed());
    // Torn: the newer mark fails its CRC, and the older one stands.
    bytes[MARK_OFFSETS[1] + 20] ^= 0xff;
    h = try SegmentHeader.parse(&bytes);
    try std.testing.expectEqual(@as(?u1, 0), h.slot);
    try std.testing.expectEqual(@as(u64, 11), h.last_index);
    try std.testing.expect(!h.sealed());
    // Neither valid: the fixed part still reads, what it holds is unknown.
    bytes[MARK_OFFSETS[0] + 20] ^= 0xff;
    h = try SegmentHeader.parse(&bytes);
    try std.testing.expectEqual(@as(?u1, null), h.slot);
    try std.testing.expectEqual(@as(u64, 10), h.first_index);
}

test "segment: footer is 32 bytes" {
    try std.testing.expectEqual(@as(usize, 32), @sizeOf(Footer));
}

test "segment: sparse index entry is 12 bytes" {
    try std.testing.expectEqual(@as(usize, 12), @sizeOf(SparseIndexEntry));
}

test "segment: batch header is 16 bytes" {
    try std.testing.expectEqual(@as(usize, 16), @sizeOf(BatchHeader));
}

test "segment: filename generation" {
    var buf: [64]u8 = undefined;
    const name = segmentFilename(&buf, 12345);
    try std.testing.expect(name != null);
    try std.testing.expectEqualStrings("0000012345.flseg", name.?);
}

test "segment: entry type bitmap" {
    var bitmap: u32 = 0;
    bitmap = bitmapSet(bitmap, 0x01); // kv_put
    bitmap = bitmapSet(bitmap, 0x10); // stream_append

    try std.testing.expect(bitmapHas(bitmap, 0x01));
    try std.testing.expect(bitmapHas(bitmap, 0x10));
    try std.testing.expect(!bitmapHas(bitmap, 0x02)); // kv_delete not set
}
