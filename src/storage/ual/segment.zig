//! UAL Segment — on-disk immutable segment format
//!
//! Sealed segment files (`.flseg`) contain sequential UAL entries with
//! a sparse index for O(log N) lookups and a footer for integrity.
//!
//! ## Layout
//!
//! ```
//! [SegmentHeader: 64B]
//! [Entry 0][Entry 1]...[Entry N]     ← sequential UAL entries
//! [SparseIndex: 12B × count]         ← every 256th entry
//! [Footer: 32B]
//! ```

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

pub const HEADER_MAGIC: [8]u8 = .{ 'F', 'L', 'O', '_', 'S', 'E', 'G', 0 };
pub const FOOTER_MAGIC: [8]u8 = .{ 'F', 'L', 'O', '_', 'E', 'N', 'D', 0 };
pub const SEGMENT_VERSION: u16 = 1;
pub const HEADER_SIZE: usize = 64;
pub const FOOTER_SIZE: usize = 32;
pub const SPARSE_INDEX_INTERVAL: u32 = 256;
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
// SegmentHeader (64 bytes, extern for exact layout)
// ═══════════════════════════════════════════════════════════════════════════════

pub const SegmentHeader = extern struct {
    magic: [8]u8 align(1),
    version: u16 align(1),
    partition_id: u32 align(1),
    first_index: u64 align(1),
    last_index: u64 align(1),
    first_ts_ns: u64 align(1),
    last_ts_ns: u64 align(1),
    entry_count: u32 align(1),
    data_size: u32 align(1), // size of entry data section (compressed)
    compression: u8 align(1),
    reserved: [9]u8 align(1),

    comptime {
        if (@sizeOf(SegmentHeader) != HEADER_SIZE) {
            @compileError("SegmentHeader must be exactly 64 bytes");
        }
    }

    pub fn asBytes(self: *const SegmentHeader) *const [HEADER_SIZE]u8 {
        return @ptrCast(self);
    }
};

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

test "segment: header is 64 bytes" {
    try std.testing.expectEqual(@as(usize, 64), @sizeOf(SegmentHeader));
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
