//! Cold Manifest — metadata index for data tiered to cold storage.
//!
//! The cold manifest tracks which UAL segments and snapshot chunks have been
//! offloaded to cold/object storage (S3, Azure Blob, local archive). It is a
//! local-only file; the data it references lives remotely.
//!
//! Design (per TIERED_RECOVERY_DESIGN.md §5):
//!   • Small metadata-only file kept on local disk
//!   • Each entry maps a segment range → remote location
//!   • Loaded on startup so reads can locate cold data without scanning remote
//!   • Never needed for snapshot-based recovery (snapshots are self-contained)
//!   • Only used for on-demand reads of historical data
//!
//! File format (.fcold):
//!   [HEADER: magic(8) + version(2) + entry_count(4) + reserved(2)]  = 16 bytes
//!   [ENTRY]*:
//!     [min_index: u64][max_index: u64][min_timestamp_ns: u64][max_timestamp_ns: u64]
//!     [location_len: u16][location bytes]
//!     [size_bytes: u64][checksum: u32]
//!   Total per entry: 46 + location string length

const std = @import("std");
const Allocator = std.mem.Allocator;

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

const MANIFEST_MAGIC: [8]u8 = .{ 'F', 'C', 'O', 'L', 'D', 0, 0, 1 };
const MANIFEST_VERSION: u16 = 1;
const HEADER_SIZE: usize = 16;

// ═══════════════════════════════════════════════════════════════════════════════
// Cold Segment Entry
// ═══════════════════════════════════════════════════════════════════════════════

/// Describes a range of UAL entries that have been offloaded to cold storage.
pub const ColdEntry = struct {
    /// Inclusive minimum UAL index in this segment.
    min_index: u64,
    /// Inclusive maximum UAL index in this segment.
    max_index: u64,
    /// Earliest timestamp in the segment.
    min_timestamp_ns: u64,
    /// Latest timestamp in the segment.
    max_timestamp_ns: u64,
    /// Remote location URI (e.g., "s3://bucket/prefix/seg-001.flseg").
    location: []const u8,
    /// Size in bytes of the remote object.
    size_bytes: u64,
    /// CRC32c of the remote object (for integrity verification).
    checksum: u32,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Cold Manifest
// ═══════════════════════════════════════════════════════════════════════════════

pub const ColdManifest = struct {
    allocator: Allocator,

    /// Entries sorted by min_index ascending.
    entries: std.ArrayList(ColdEntry),

    pub fn init(allocator: Allocator) ColdManifest {
        return .{
            .allocator = allocator,
            .entries = .empty,
        };
    }

    pub fn deinit(self: *ColdManifest) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.location);
        }
        self.entries.deinit(self.allocator);
    }

    /// Register a new cold segment.
    pub fn addEntry(self: *ColdManifest, entry: ColdEntry) !void {
        const location_copy = try self.allocator.dupe(u8, entry.location);
        errdefer self.allocator.free(location_copy);

        try self.entries.append(self.allocator, .{
            .min_index = entry.min_index,
            .max_index = entry.max_index,
            .min_timestamp_ns = entry.min_timestamp_ns,
            .max_timestamp_ns = entry.max_timestamp_ns,
            .location = location_copy,
            .size_bytes = entry.size_bytes,
            .checksum = entry.checksum,
        });
    }

    /// Find the cold entry containing the given UAL index, if any.
    pub fn findByIndex(self: *const ColdManifest, index: u64) ?ColdEntry {
        for (self.entries.items) |entry| {
            if (index >= entry.min_index and index <= entry.max_index) {
                return entry;
            }
        }
        return null;
    }

    /// Find all cold entries overlapping a timestamp range.
    pub fn findByTimeRange(self: *const ColdManifest, min_ts: u64, max_ts: u64, buf: []ColdEntry) usize {
        var found: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.max_timestamp_ns >= min_ts and entry.min_timestamp_ns <= max_ts) {
                if (found < buf.len) {
                    buf[found] = entry;
                    found += 1;
                } else break;
            }
        }
        return found;
    }

    /// Total number of tracked cold segments.
    pub fn count(self: *const ColdManifest) usize {
        return self.entries.items.len;
    }

    /// Total bytes stored in cold storage.
    pub fn totalBytes(self: *const ColdManifest) u64 {
        var total: u64 = 0;
        for (self.entries.items) |entry| {
            total += entry.size_bytes;
        }
        return total;
    }

    // ─── Persistence ───────────────────────────────────────────────────

    /// Serialize the manifest to bytes. Caller owns returned slice.
    pub fn save(self: *const ColdManifest, allocator: Allocator) ![]u8 {
        // Calculate total size
        var total_size: usize = HEADER_SIZE;
        for (self.entries.items) |entry| {
            // 8+8+8+8+2+location_len+8+4 = 46 + location_len
            total_size += 46 + entry.location.len;
        }

        const buf = try allocator.alloc(u8, total_size);
        errdefer allocator.free(buf);
        var offset: usize = 0;

        // Header
        @memcpy(buf[0..8], &MANIFEST_MAGIC);
        offset += 8;
        std.mem.writeInt(u16, buf[offset..][0..2], MANIFEST_VERSION, .little);
        offset += 2;
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.entries.items.len), .little);
        offset += 4;
        @memset(buf[offset..][0..2], 0); // reserved
        offset += 2;

        // Entries
        for (self.entries.items) |entry| {
            std.mem.writeInt(u64, buf[offset..][0..8], entry.min_index, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.max_index, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.min_timestamp_ns, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.max_timestamp_ns, .little);
            offset += 8;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(entry.location.len), .little);
            offset += 2;
            @memcpy(buf[offset..][0..entry.location.len], entry.location);
            offset += entry.location.len;
            std.mem.writeInt(u64, buf[offset..][0..8], entry.size_bytes, .little);
            offset += 8;
            std.mem.writeInt(u32, buf[offset..][0..4], entry.checksum, .little);
            offset += 4;
        }

        return buf;
    }

    /// Load manifest from serialized bytes. Caller owns the ColdManifest.
    pub fn load(allocator: Allocator, data: []const u8) !ColdManifest {
        if (data.len < HEADER_SIZE) return error.InvalidManifest;

        // Validate magic
        if (!std.mem.eql(u8, data[0..8], &MANIFEST_MAGIC)) return error.InvalidManifest;

        const version = std.mem.readInt(u16, data[8..10], .little);
        if (version != MANIFEST_VERSION) return error.InvalidManifest;

        const entry_count = std.mem.readInt(u32, data[10..14], .little);
        var offset: usize = HEADER_SIZE;

        var manifest = ColdManifest.init(allocator);
        errdefer manifest.deinit();

        var i: u32 = 0;
        while (i < entry_count) : (i += 1) {
            if (offset + 46 > data.len) return error.InvalidManifest;

            const min_index = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const max_index = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const min_timestamp_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const max_timestamp_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const loc_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;

            if (offset + loc_len + 12 > data.len) return error.InvalidManifest;

            const location = try allocator.dupe(u8, data[offset..][0..loc_len]);
            offset += loc_len;
            const size_bytes = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const checksum = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;

            try manifest.entries.append(allocator, .{
                .min_index = min_index,
                .max_index = max_index,
                .min_timestamp_ns = min_timestamp_ns,
                .max_timestamp_ns = max_timestamp_ns,
                .location = location,
                .size_bytes = size_bytes,
                .checksum = checksum,
            });
        }

        return manifest;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "cold manifest: init and deinit" {
    var m = ColdManifest.init(testing.allocator);
    defer m.deinit();

    try testing.expectEqual(@as(usize, 0), m.count());
    try testing.expectEqual(@as(u64, 0), m.totalBytes());
}

test "cold manifest: add and find entries" {
    var m = ColdManifest.init(testing.allocator);
    defer m.deinit();

    try m.addEntry(.{
        .min_index = 1,
        .max_index = 1000,
        .min_timestamp_ns = 100_000,
        .max_timestamp_ns = 200_000,
        .location = "s3://bucket/seg-001.flseg",
        .size_bytes = 1024 * 1024,
        .checksum = 0xDEADBEEF,
    });

    try m.addEntry(.{
        .min_index = 1001,
        .max_index = 2000,
        .min_timestamp_ns = 200_001,
        .max_timestamp_ns = 300_000,
        .location = "s3://bucket/seg-002.flseg",
        .size_bytes = 2 * 1024 * 1024,
        .checksum = 0xCAFEBABE,
    });

    try testing.expectEqual(@as(usize, 2), m.count());
    try testing.expectEqual(@as(u64, 3 * 1024 * 1024), m.totalBytes());

    // Find by index
    const e1 = m.findByIndex(500).?;
    try testing.expectEqualStrings("s3://bucket/seg-001.flseg", e1.location);

    const e2 = m.findByIndex(1500).?;
    try testing.expectEqualStrings("s3://bucket/seg-002.flseg", e2.location);

    try testing.expect(m.findByIndex(3000) == null);

    // Find by time range
    var buf: [10]ColdEntry = undefined;
    const found = m.findByTimeRange(150_000, 250_000, &buf);
    try testing.expectEqual(@as(usize, 2), found); // both overlap
}

test "cold manifest: save and load round-trip" {
    var m = ColdManifest.init(testing.allocator);
    defer m.deinit();

    try m.addEntry(.{
        .min_index = 1,
        .max_index = 500,
        .min_timestamp_ns = 1000,
        .max_timestamp_ns = 5000,
        .location = "s3://test/seg-001.flseg",
        .size_bytes = 65536,
        .checksum = 0x12345678,
    });

    try m.addEntry(.{
        .min_index = 501,
        .max_index = 1000,
        .min_timestamp_ns = 5001,
        .max_timestamp_ns = 10000,
        .location = "azure://container/seg-002.flseg",
        .size_bytes = 131072,
        .checksum = 0x87654321,
    });

    // Save
    const data = try m.save(testing.allocator);
    defer testing.allocator.free(data);

    // Load
    var m2 = try ColdManifest.load(testing.allocator, data);
    defer m2.deinit();

    try testing.expectEqual(@as(usize, 2), m2.count());

    const e1 = m2.entries.items[0];
    try testing.expectEqual(@as(u64, 1), e1.min_index);
    try testing.expectEqual(@as(u64, 500), e1.max_index);
    try testing.expectEqualStrings("s3://test/seg-001.flseg", e1.location);
    try testing.expectEqual(@as(u64, 65536), e1.size_bytes);
    try testing.expectEqual(@as(u32, 0x12345678), e1.checksum);

    const e2 = m2.entries.items[1];
    try testing.expectEqual(@as(u64, 501), e2.min_index);
    try testing.expectEqual(@as(u64, 1000), e2.max_index);
    try testing.expectEqualStrings("azure://container/seg-002.flseg", e2.location);
}

test "cold manifest: empty round-trip" {
    var m = ColdManifest.init(testing.allocator);
    defer m.deinit();

    const data = try m.save(testing.allocator);
    defer testing.allocator.free(data);

    var m2 = try ColdManifest.load(testing.allocator, data);
    defer m2.deinit();

    try testing.expectEqual(@as(usize, 0), m2.count());
}
