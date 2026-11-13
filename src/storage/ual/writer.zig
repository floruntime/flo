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
    /// Written into the header at seal; `DurableLog` sets it: the Raft
    /// commit index at flush, clamped to the cut when a truncation rewrites
    /// a file.
    commit_index_at_seal: u64,
    /// Whether a buffered entry is a repair skip.
    holds_skip: bool = false,
    /// Times the in-memory segment buffer could not grow (out of memory) on a
    /// Raft append — committed or not — so the entry was not buffered for the
    /// next flush. Counted and logged rather than dropped silently; a non-zero
    /// count means memory ran out, so free memory, then restart or resync.
    buffer_failures: u64,
    /// A raw monotonic reading when the current run of buffer failures began, or
    /// 0 while buffering succeeds. Kept internal — set on the first failure of a
    /// run and cleared when buffering catches up (in the append hook, or in
    /// rebufferGap behind a sync gap); `/status` and metrics publish the elapsed
    /// duration computed from it, not this raw reading.
    buffer_failing_since_ms: u64 = 0,
    /// The first index that could not be buffered, when `stop_at_gap`.
    /// Nothing after it is buffered either, so a flush never writes past a
    /// gap; the owner re-buffers from here before flushing.
    first_unbuffered: ?u64 = null,
    /// Set when a write is acked only once on disk: a hole in a flush would
    /// break that promise. Otherwise an entry that can't be buffered is a
    /// counted hole and buffering goes on.
    stop_at_gap: bool = false,

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, partition_id: u32) SegmentWriter {
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
            .commit_index_at_seal = 0,
            .buffer_failures = 0,
            .first_unbuffered = null,
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
        self.holds_skip = false;
    }

    /// Add an entry to the segment: all or nothing, so on error the buffer
    /// is as it was.
    pub fn addEntry(self: *SegmentWriter, entry: *const Entry) !void {
        try self.data.ensureUnusedCapacity(self.allocator, entry_mod.HEADER_SIZE + entry.payload.len);
        try self.sparse_index.ensureUnusedCapacity(self.allocator, 1);
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
        if (entry.header.entry_type == @backingInt(entry_mod.EntryType.repair_skip)) self.holds_skip = true;
    }

    /// Drop every buffered entry above `after_index`. Entries are
    /// self-delimiting in `data`, so the kept prefix ends at the first
    /// dropped header; the sparse index and metadata follow it.
    pub fn truncateAfter(self: *SegmentWriter, after_index: u64) void {
        // Missing entries the cut discards are no longer owed to disk.
        if (self.first_unbuffered) |f| {
            if (f > after_index) self.first_unbuffered = null;
        }
        if (self.entry_count == 0 or self.last_index <= after_index) return;
        if (self.first_index > after_index) {
            self.reset();
            return;
        }
        var offset: usize = 0;
        var kept: u32 = 0;
        var last_kept: ?Entry = null;
        var bitmap: u32 = 0;
        var holds_skip = false;
        while (offset < self.data.items.len) {
            const e = Entry.deserialize(self.data.items[offset..]) orelse break;
            if (e.header.index > after_index) break;
            kept += 1;
            last_kept = e;
            bitmap = segment.bitmapSet(bitmap, e.header.entry_type);
            if (e.header.entry_type == @backingInt(entry_mod.EntryType.repair_skip)) holds_skip = true;
            offset += e.totalSize();
        }
        self.data.shrinkRetainingCapacity(offset);
        while (self.sparse_index.items.len > 0 and self.sparse_index.items[self.sparse_index.items.len - 1].offset >= offset) {
            _ = self.sparse_index.pop();
        }
        self.entry_count = kept;
        self.entry_type_bitmap = bitmap;
        self.holds_skip = holds_skip;
        if (last_kept) |e| {
            self.last_index = e.header.index;
            self.last_ts_ns = e.header.timestamp_ns;
        }
    }

    /// Current data size (entry bytes only, no header/footer/index).
    pub fn dataSize(self: *const SegmentWriter) usize {
        return self.data.items.len;
    }

    /// The mark a sealed segment of the buffered entries carries.
    pub fn sealedMark(self: *const SegmentWriter) segment.Mark {
        return .{
            .seq = 1,
            .flags = segment.FLAG_SEALED | (if (self.holds_skip) segment.FLAG_HOLDS_SKIP else 0),
            .data_size = @intCast(self.data.items.len),
            .entry_count = self.entry_count,
            .last_index = self.last_index,
            .last_ts_ns = self.last_ts_ns,
            .commit_index = self.commit_index_at_seal,
            .crc32c = 0,
        };
    }

    /// The sparse index and footer that seal `data` (entry bytes from the
    /// data section's start) under `fixed`, appended to `out`; `data_crc` is
    /// the running CRC over `data`.
    pub fn appendSealTail(allocator: std.mem.Allocator, out: *std.ArrayList(u8), sparse: []const segment.SparseIndexEntry, data_len: usize, data_crc: *checksum_mod.ChecksumStream, fixed: *const segment.Fixed, bitmap: u32) !void {
        const sparse_index_offset: u32 = @intCast(segment.HEADER_SIZE + data_len);
        const index_bytes = std.mem.sliceAsBytes(sparse);
        try out.appendSlice(allocator, index_bytes);
        var crc = data_crc.*;
        crc.add(index_bytes);
        crc.add(std.mem.asBytes(fixed));
        const footer = segment.Footer{
            .sparse_index_offset = sparse_index_offset,
            .sparse_index_count = @intCast(sparse.len),
            .entry_type_bitmap = bitmap,
            .bloom_filter_offset = 0, // no bloom filter yet
            .crc32c = crc.checksum(),
            ._padding = @splat(0),
            .magic = segment.FOOTER_MAGIC,
        };
        try out.appendSlice(allocator, footer.asBytes());
    }

    /// Seal the segment and return the complete file contents.
    /// Caller owns the returned slice.
    pub fn seal(self: *SegmentWriter) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(self.allocator);
        const fixed = segment.fixedFor(self.partition_id, self.first_index, self.first_ts_ns);
        try out.appendSlice(self.allocator, &segment.encodeHeader(fixed, self.sealedMark()));
        try out.appendSlice(self.allocator, self.data.items);
        var crc = checksum_mod.ChecksumStream.init();
        crc.add(self.data.items);
        try appendSealTail(self.allocator, &out, self.sparse_index.items, self.data.items.len, &crc, &fixed, self.entry_type_bitmap);
        return out.toOwnedSlice(self.allocator);
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

        // fsync the data before the rename makes it visible. Without this the
        // bytes may sit in the page cache and be lost on power failure, so
        // `sync` durability would silently fail to deliver its "fdatasync before
        // ack" guarantee.
        try @import("stdx").fs.sync(file);

        // Rename into place
        @import("stdx").fs.rename(tmp_path, path) catch |err| {
            // If rename fails, try to clean up tmp
            @import("stdx").fs.deleteFile(tmp_path) catch {};
            return err;
        };

        // Until the directory is synced a power loss can drop the new name,
        // and with it every entry in the segment.
        try @import("stdx").fs.syncDir(dir_path);

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
    var writer = SegmentWriter.init(std.testing.allocator, 42);
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
    var writer = SegmentWriter.init(std.testing.allocator, 0);
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
    const hdr = try segment.SegmentHeader.parse(sealed[0..segment.HEADER_SIZE]);
    try std.testing.expect(hdr.sealed());
    try std.testing.expectEqual(@as(u32, 3), hdr.entry_count);
    try std.testing.expectEqual(@as(u64, 1), hdr.first_index);
    try std.testing.expectEqual(@as(u64, 3), hdr.last_index);
}

test "writer: sparse index created for 256+ entries" {
    var writer = SegmentWriter.init(std.testing.allocator, 0);
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
    var writer = SegmentWriter.init(std.testing.allocator, 0);
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
    var writer = SegmentWriter.init(std.testing.allocator, 0);
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

test "writer: writeToFile syncs the segment directory after the rename" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmp.dir.realPathFileAlloc(@import("stdx").io.instance(), ".", std.testing.allocator);
    defer std.testing.allocator.free(dir_path);

    var writer = SegmentWriter.init(std.testing.allocator, 0);
    defer writer.deinit();
    try writer.addEntry(&makeTestEntry(1, "data"));

    const syncs = &@import("stdx").fs.dir_syncs;
    const before = syncs.load(.monotonic);
    try writer.writeToFile(dir_path);
    try std.testing.expectEqual(before + 1, syncs.load(.monotonic));
}
