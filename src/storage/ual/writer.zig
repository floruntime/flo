//! UAL Segment Writer — writes entries to a segment file
//!
//! Entries are appended sequentially. A sparse index is built during
//! writing (every 256th entry). When the segment is sealed, the sparse
//! index and footer are written, and a full-file CRC32C is computed.
//!
//! Two modes:
//! 1. **In-memory**: entries collected in a buffer, then written to
//!    a file on seal. Used for testing and small segments.
//! 2. **File-backed**: entries streamed to disk as they arrive.
//!    (Phase 2.3 implements in-memory; file streaming added in Phase 2.6)

const std = @import("std");
const segment = @import("segment.zig");
const entry_mod = @import("entry.zig");
const checksum_mod = @import("../../util/checksum.zig");
const log = @import("stdx").log;

const Entry = entry_mod.Entry;
const ENTRY_HEADER_SIZE = entry_mod.HEADER_SIZE;

// ═══════════════════════════════════════════════════════════════════════════════
// SegmentWriter
// ═══════════════════════════════════════════════════════════════════════════════

pub const SegmentWriter = struct {
    /// Entry data buffer (header + data section, not including segment header).
    data: std.ArrayList(u8),
    /// Sparse index entries.
    sparse_index: std.ArrayList(segment.SparseIndexEntry),

    partition_id: u32,
    first_index: u64,
    last_index: u64,
    first_ts_ns: u64,
    last_ts_ns: u64,
    entry_count: u32,
    entry_type_bitmap: u32,
    compression: segment.Compression,

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, partition_id: u32, compression: segment.Compression) SegmentWriter {
        return .{
            .data = std.ArrayList(u8).empty,
            .sparse_index = std.ArrayList(segment.SparseIndexEntry).empty,
            .partition_id = partition_id,
            .first_index = 0,
            .last_index = 0,
            .first_ts_ns = 0,
            .last_ts_ns = 0,
            .entry_count = 0,
            .entry_type_bitmap = 0,
            .compression = compression,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SegmentWriter) void {
        self.data.deinit(self.allocator);
        self.sparse_index.deinit(self.allocator);
    }

    /// Clear buffered entries after a segment has been flushed to disk.
    pub fn reset(self: *SegmentWriter) void {
        self.data.clearRetainingCapacity();
        self.sparse_index.clearRetainingCapacity();
        self.first_index = 0;
        self.last_index = 0;
        self.first_ts_ns = 0;
        self.last_ts_ns = 0;
        self.entry_count = 0;
        self.entry_type_bitmap = 0;
    }

    /// Add an entry to the segment.
    pub fn addEntry(self: *SegmentWriter, entry: *const Entry) !void {
        // Track sparse index (every Nth entry)
        if (self.entry_count % segment.SPARSE_INDEX_INTERVAL == 0) {
            try self.sparse_index.append(self.allocator, .{
                .index = entry.header.index,
                .offset = @intCast(self.data.items.len),
            });
        }

        // Write entry header
        const hdr_bytes = entry.header.asBytes();
        try self.data.appendSlice(self.allocator, hdr_bytes);

        // Write entry payload
        if (entry.payload.len > 0) {
            try self.data.appendSlice(self.allocator, entry.payload);
        }

        // Update metadata
        if (self.entry_count == 0) {
            self.first_index = entry.header.index;
            self.first_ts_ns = entry.header.timestamp_ns;
        }
        self.last_index = entry.header.index;
        self.last_ts_ns = entry.header.timestamp_ns;
        self.entry_count += 1;
        self.entry_type_bitmap = segment.bitmapSet(self.entry_type_bitmap, entry.header.entry_type);
    }

    /// Current data size (entry bytes only, no header/footer/index).
    pub fn dataSize(self: *const SegmentWriter) usize {
        return self.data.items.len;
    }

    /// Seal the segment and return the complete file contents.
    /// Caller owns the returned slice.
    pub fn seal(self: *SegmentWriter) ![]u8 {
        // Compute sizes
        const index_bytes = self.sparse_index.items.len * segment.SPARSE_ENTRY_SIZE;
        const total_size = segment.HEADER_SIZE + self.data.items.len + index_bytes + segment.FOOTER_SIZE;

        var result = try self.allocator.alloc(u8, total_size);
        errdefer self.allocator.free(result);

        var offset: usize = 0;

        // 1. Write segment header
        const hdr = segment.SegmentHeader{
            .magic = segment.HEADER_MAGIC,
            .version = segment.SEGMENT_VERSION,
            .partition_id = self.partition_id,
            .first_index = self.first_index,
            .last_index = self.last_index,
            .first_ts_ns = self.first_ts_ns,
            .last_ts_ns = self.last_ts_ns,
            .entry_count = self.entry_count,
            .data_size = @intCast(self.data.items.len),
            .compression = @intFromEnum(self.compression),
            .reserved = .{0} ** 9,
        };
        const hdr_bytes = hdr.asBytes();
        @memcpy(result[offset .. offset + segment.HEADER_SIZE], hdr_bytes);
        offset += segment.HEADER_SIZE;

        // 2. Write entry data
        @memcpy(result[offset .. offset + self.data.items.len], self.data.items);
        offset += self.data.items.len;

        // 3. Write sparse index
        const sparse_index_offset: u32 = @intCast(offset);
        for (self.sparse_index.items) |idx_entry| {
            const entry_bytes: *const [segment.SPARSE_ENTRY_SIZE]u8 = @ptrCast(&idx_entry);
            @memcpy(result[offset .. offset + segment.SPARSE_ENTRY_SIZE], entry_bytes);
            offset += segment.SPARSE_ENTRY_SIZE;
        }

        // 4. Compute CRC over everything written so far
        const crc = checksum_mod.checksum(result[0..offset]);

        // 5. Write footer
        const footer = segment.Footer{
            .sparse_index_offset = sparse_index_offset,
            .sparse_index_count = @intCast(self.sparse_index.items.len),
            .entry_type_bitmap = self.entry_type_bitmap,
            .bloom_filter_offset = 0, // no bloom filter yet
            .crc32c = crc,
            ._padding = .{0} ** 4,
            .magic = segment.FOOTER_MAGIC,
        };
        const footer_bytes = footer.asBytes();
        @memcpy(result[offset .. offset + segment.FOOTER_SIZE], footer_bytes);

        return result;
    }

    /// Write the sealed segment to a file.
    pub fn writeToFile(self: *SegmentWriter, dir_path: []const u8) !void {
        const sealed = try self.seal();
        defer self.allocator.free(sealed);

        // Generate filename
        var name_buf: [64]u8 = undefined;
        const filename = segment.segmentFilename(&name_buf, self.first_index) orelse return error.FilenameTooLong;

        // Build full path
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir_path, filename }) catch return error.PathTooLong;

        // Write atomically: write to .tmp then rename
        var tmp_buf: [520]u8 = undefined;
        const tmp_path = std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{path}) catch return error.PathTooLong;

        const file = try @import("stdx").fs.createFile(tmp_path, .{});
        defer @import("stdx").fs.closeFile(file);
        try @import("stdx").fs.writeAll(file, sealed);

        // Rename into place
        @import("stdx").fs.rename(tmp_path, path) catch |err| {
            // If rename fails, try to clean up tmp
            @import("stdx").fs.deleteFile(tmp_path) catch {};
            return err;
        };

        log.debug("UAL Writer: segment written, path={s}, entries={d}, data_size={d}", .{ path, self.entry_count, sealed.len });
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

fn makeTestEntry(index: u64, payload: []const u8) Entry {
    return entry_mod.buildEntry(.kv_put, 0, 1, index, @as(u64, index) * 1000, payload);
}

test "writer: empty segment seals correctly" {
    var writer = SegmentWriter.init(std.testing.allocator, 42, .none);
    defer writer.deinit();

    const sealed = try writer.seal();
    defer std.testing.allocator.free(sealed);

    // Should be header + footer only
    try std.testing.expectEqual(segment.HEADER_SIZE + segment.FOOTER_SIZE, sealed.len);

    // Check header magic
    try std.testing.expectEqualSlices(u8, &segment.HEADER_MAGIC, sealed[0..8]);

    // Check footer magic
    try std.testing.expectEqualSlices(u8, &segment.FOOTER_MAGIC, sealed[sealed.len - 8 ..]);
}

test "writer: add entries and seal" {
    var writer = SegmentWriter.init(std.testing.allocator, 0, .none);
    defer writer.deinit();

    // Add 3 entries
    const e1 = makeTestEntry(1, "key1");
    const e2 = makeTestEntry(2, "key2data");
    const e3 = makeTestEntry(3, "k3");

    try writer.addEntry(&e1);
    try writer.addEntry(&e2);
    try writer.addEntry(&e3);

    try std.testing.expectEqual(@as(u32, 3), writer.entry_count);
    try std.testing.expectEqual(@as(u64, 1), writer.first_index);
    try std.testing.expectEqual(@as(u64, 3), writer.last_index);

    const sealed = try writer.seal();
    defer std.testing.allocator.free(sealed);

    // Verify header
    const hdr: *const segment.SegmentHeader = @ptrCast(@alignCast(sealed[0..segment.HEADER_SIZE]));
    try std.testing.expectEqual(segment.SEGMENT_VERSION, hdr.version);
    try std.testing.expectEqual(@as(u32, 3), hdr.entry_count);
    try std.testing.expectEqual(@as(u64, 1), hdr.first_index);
    try std.testing.expectEqual(@as(u64, 3), hdr.last_index);
}

test "writer: sparse index created for 256+ entries" {
    var writer = SegmentWriter.init(std.testing.allocator, 0, .none);
    defer writer.deinit();

    // Add 300 entries — should get 2 sparse index entries (0, 256)
    var i: u64 = 1;
    while (i <= 300) : (i += 1) {
        const entry = makeTestEntry(i, "xx");
        try writer.addEntry(&entry);
    }

    try std.testing.expectEqual(@as(u32, 300), writer.entry_count);
    // Entry 0 (index=1) and entry 256 (index=257) in sparse index
    try std.testing.expectEqual(@as(usize, 2), writer.sparse_index.items.len);
    try std.testing.expectEqual(@as(u64, 1), writer.sparse_index.items[0].index);
    try std.testing.expectEqual(@as(u64, 257), writer.sparse_index.items[1].index);
}

test "writer: entry type bitmap tracks types" {
    var writer = SegmentWriter.init(std.testing.allocator, 0, .none);
    defer writer.deinit();

    const kv = entry_mod.buildEntry(.kv_put, 0, 1, 1, 0, "a");
    const stream = entry_mod.buildEntry(.stream_append, 0, 1, 2, 0, "b");

    try writer.addEntry(&kv);
    try writer.addEntry(&stream);

    try std.testing.expect(segment.bitmapHas(writer.entry_type_bitmap, 0x01)); // kv_put
    try std.testing.expect(segment.bitmapHas(writer.entry_type_bitmap, 0x10)); // stream_append
    try std.testing.expect(!segment.bitmapHas(writer.entry_type_bitmap, 0x20)); // queue_enqueue
}

test "writer: CRC in footer is non-zero" {
    var writer = SegmentWriter.init(std.testing.allocator, 0, .none);
    defer writer.deinit();

    const entry = makeTestEntry(1, "data");
    try writer.addEntry(&entry);

    const sealed = try writer.seal();
    defer std.testing.allocator.free(sealed);

    // Read footer
    const footer_start = sealed.len - segment.FOOTER_SIZE;
    const footer: *const segment.Footer = @ptrCast(@alignCast(sealed[footer_start..][0..segment.FOOTER_SIZE]));

    try std.testing.expect(footer.crc32c != 0);
    try std.testing.expectEqualSlices(u8, &segment.FOOTER_MAGIC, &footer.magic);
}
