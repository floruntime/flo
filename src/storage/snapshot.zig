//! Persistent Snapshot Manager
//!
//! Writes projection state to disk as atomic `.fsnap` files.
//! On recovery, loads the latest snapshot and provides the UAL index
//! from which to begin replaying entries.
//!
//! File format: [SnapshotHeader 64B] [Section...] [SnapshotFooter 16B]
//! Each section: [SectionHeader 12B] [section data bytes]
//!
//! Crash safety:
//! - Writes go to `.fsnap.tmp`, then fdatasync, then atomic rename
//! - MANIFEST updated atomically the same way
//! - If crash during write: `.tmp` is incomplete, previous `.fsnap` is valid
//! - If crash after snapshot but before UAL compact: harmless extra entries

const std = @import("std");
const Allocator = std.mem.Allocator;
const checksum_mod = @import("../util/checksum.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

pub const HEADER_MAGIC: [8]u8 = .{ 'F', 'L', 'O', '_', 'S', 'N', 'P', 0 };
pub const FOOTER_MAGIC: [8]u8 = .{ 'F', 'L', 'O', '_', 'S', 'N', 'E', 0 };
pub const SNAPSHOT_VERSION: u16 = 1;
pub const HEADER_SIZE: usize = @sizeOf(SnapshotHeader);
pub const FOOTER_SIZE: usize = @sizeOf(SnapshotFooter);
pub const SECTION_HEADER_SIZE: usize = @sizeOf(SectionHeader);
pub const MAX_SECTIONS: u32 = 16;

comptime {
    if (HEADER_SIZE != 64) @compileError("SnapshotHeader must be exactly 64 bytes");
    if (FOOTER_SIZE != 16) @compileError("SnapshotFooter must be exactly 16 bytes");
    if (SECTION_HEADER_SIZE != 12) @compileError("SectionHeader must be exactly 12 bytes");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

pub const SectionType = enum(u8) {
    kv = 0x01,
    queue = 0x02,
    ts = 0x03,
    stream = 0x04,
};

/// 64-byte snapshot file header.
pub const SnapshotHeader = extern struct {
    magic: [8]u8 align(1),
    version: u16 align(1),
    partition_id: u32 align(1),
    ual_index: u64 align(1),
    raft_term: u64 align(1),
    timestamp_ns: u64 align(1),
    section_count: u32 align(1),
    total_size: u64 align(1),
    reserved: [14]u8 align(1),

    pub fn asBytes(self: *const SnapshotHeader) *const [HEADER_SIZE]u8 {
        return @ptrCast(self);
    }
};

/// 16-byte snapshot file footer. CRC covers everything before the footer.
pub const SnapshotFooter = extern struct {
    crc32c: u32 align(1),
    magic: [8]u8 align(1),
    _padding: [4]u8 align(1),

    pub fn asBytes(self: *const SnapshotFooter) *const [FOOTER_SIZE]u8 {
        return @ptrCast(self);
    }
};

/// 12-byte header preceding each section within the snapshot.
pub const SectionHeader = extern struct {
    section_type: u8 align(1),
    _reserved: [3]u8 align(1),
    section_size: u64 align(1),

    pub fn asBytes(self: *const SectionHeader) *const [SECTION_HEADER_SIZE]u8 {
        return @ptrCast(self);
    }
};

/// Reference to a parsed section within snapshot data.
pub const SectionRef = struct {
    section_type: u8,
    data: []const u8,
};

/// Metadata about a snapshot file (parsed from filename).
pub const SnapshotInfo = struct {
    index: u64,
    timestamp_ns: u64,
};

// ═══════════════════════════════════════════════════════════════════════════════
// SnapshotBuilder
// ═══════════════════════════════════════════════════════════════════════════════

/// Builds a snapshot from projection sections. Call `addSection` for each
/// projection, then `seal` to produce the final byte buffer.
pub const SnapshotBuilder = struct {
    allocator: Allocator,
    partition_id: u32,
    ual_index: u64,
    raft_term: u64,
    timestamp_ns: u64,
    sections: std.ArrayList(SectionEntry),

    const SectionEntry = struct {
        section_type: SectionType,
        data: []const u8, // owned copy
    };

    pub fn init(
        allocator: Allocator,
        partition_id: u32,
        ual_index: u64,
        raft_term: u64,
        timestamp_ns: u64,
    ) SnapshotBuilder {
        return .{
            .allocator = allocator,
            .partition_id = partition_id,
            .ual_index = ual_index,
            .raft_term = raft_term,
            .timestamp_ns = timestamp_ns,
            .sections = .empty,
        };
    }

    pub fn deinit(self: *SnapshotBuilder) void {
        for (self.sections.items) |s| {
            self.allocator.free(s.data);
        }
        self.sections.deinit(self.allocator);
    }

    /// Add a projection section. Data is copied into the builder.
    pub fn addSection(self: *SnapshotBuilder, section_type: SectionType, data: []const u8) !void {
        const copy = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(copy);
        try self.sections.append(self.allocator, .{
            .section_type = section_type,
            .data = copy,
        });
    }

    /// Serialize the snapshot to a self-contained byte buffer.
    /// Caller owns the returned slice.
    pub fn seal(self: *SnapshotBuilder) ![]u8 {
        var sections_size: usize = 0;
        for (self.sections.items) |s| {
            sections_size += SECTION_HEADER_SIZE + s.data.len;
        }
        const total_size = HEADER_SIZE + sections_size + FOOTER_SIZE;

        const buf = try self.allocator.alloc(u8, total_size);
        errdefer self.allocator.free(buf);

        // Header
        const header = SnapshotHeader{
            .magic = HEADER_MAGIC,
            .version = SNAPSHOT_VERSION,
            .partition_id = self.partition_id,
            .ual_index = self.ual_index,
            .raft_term = self.raft_term,
            .timestamp_ns = self.timestamp_ns,
            .section_count = @intCast(self.sections.items.len),
            .total_size = @intCast(total_size),
            .reserved = .{0} ** 14,
        };
        @memcpy(buf[0..HEADER_SIZE], header.asBytes());

        // Sections
        var offset: usize = HEADER_SIZE;
        for (self.sections.items) |s| {
            const sh = SectionHeader{
                .section_type = @intFromEnum(s.section_type),
                ._reserved = .{0} ** 3,
                .section_size = @intCast(s.data.len),
            };
            @memcpy(buf[offset..][0..SECTION_HEADER_SIZE], sh.asBytes());
            offset += SECTION_HEADER_SIZE;
            @memcpy(buf[offset..][0..s.data.len], s.data);
            offset += s.data.len;
        }

        // Footer — CRC covers [0..offset)
        const crc = checksum_mod.checksum(buf[0..offset]);
        const footer = SnapshotFooter{
            .crc32c = crc,
            .magic = FOOTER_MAGIC,
            ._padding = .{0} ** 4,
        };
        @memcpy(buf[offset..][0..FOOTER_SIZE], footer.asBytes());

        return buf;
    }

    /// Seal and write atomically to a directory.
    /// Writes `{index:0>10}-{timestamp}.fsnap` and updates MANIFEST.
    pub fn writeToDir(self: *SnapshotBuilder, dir: std.fs.Dir) !void {
        const data = try self.seal();
        defer self.allocator.free(data);

        var name_buf: [128]u8 = undefined;
        const filename = snapshotFilename(&name_buf, self.ual_index, self.timestamp_ns);

        var tmp_buf: [128]u8 = undefined;
        const tmp_name = std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{filename}) catch return error.NameTooLong;

        // Write to .tmp
        {
            const file = try dir.createFile(tmp_name, .{});
            defer file.close();
            try file.writeAll(data);
            try file.sync();
        }

        // Atomic rename
        try dir.rename(tmp_name, filename);

        // Update MANIFEST
        try writeManifest(dir, filename);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// SnapshotReader
// ═══════════════════════════════════════════════════════════════════════════════

/// Parses and validates snapshot data produced by SnapshotBuilder.
pub const SnapshotReader = struct {
    data: []const u8,
    header: SnapshotHeader,
    footer: SnapshotFooter,
    section_refs: [MAX_SECTIONS]SectionRef,
    section_count: u32,

    pub const Error = error{
        TooSmall,
        InvalidHeaderMagic,
        InvalidFooterMagic,
        InvalidVersion,
        InvalidCrc,
        InvalidSectionLayout,
        TooManySections,
    };

    pub fn init(data: []const u8) Error!SnapshotReader {
        if (data.len < HEADER_SIZE + FOOTER_SIZE) return error.TooSmall;

        // Parse header
        const hdr: *const SnapshotHeader = @ptrCast(@alignCast(data[0..HEADER_SIZE]));
        if (!std.mem.eql(u8, &hdr.magic, &HEADER_MAGIC)) return error.InvalidHeaderMagic;
        if (hdr.version != SNAPSHOT_VERSION) return error.InvalidVersion;

        // Parse footer
        const footer_start = data.len - FOOTER_SIZE;
        const ftr: *const SnapshotFooter = @ptrCast(@alignCast(data[footer_start..][0..FOOTER_SIZE]));
        if (!std.mem.eql(u8, &ftr.magic, &FOOTER_MAGIC)) return error.InvalidFooterMagic;

        // Validate CRC (covers everything before footer)
        const expected_crc = checksum_mod.checksum(data[0..footer_start]);
        if (ftr.crc32c != expected_crc) return error.InvalidCrc;

        if (hdr.section_count > MAX_SECTIONS) return error.TooManySections;

        var result = SnapshotReader{
            .data = data,
            .header = hdr.*,
            .footer = ftr.*,
            .section_refs = undefined,
            .section_count = hdr.section_count,
        };

        // Parse section directory
        var offset: usize = HEADER_SIZE;
        for (0..hdr.section_count) |i| {
            if (offset + SECTION_HEADER_SIZE > footer_start) return error.InvalidSectionLayout;
            const sh: *const SectionHeader = @ptrCast(@alignCast(data[offset..][0..SECTION_HEADER_SIZE]));
            offset += SECTION_HEADER_SIZE;
            const size: usize = @intCast(sh.section_size);
            if (offset + size > footer_start) return error.InvalidSectionLayout;
            result.section_refs[i] = .{
                .section_type = sh.section_type,
                .data = data[offset..][0..size],
            };
            offset += size;
        }

        return result;
    }

    /// Get section by index.
    pub fn getSection(self: *const SnapshotReader, index: u32) ?SectionRef {
        if (index >= self.section_count) return null;
        return self.section_refs[index];
    }

    /// Find first section matching the given type.
    pub fn findSection(self: *const SnapshotReader, section_type: SectionType) ?SectionRef {
        for (0..self.section_count) |i| {
            if (self.section_refs[i].section_type == @intFromEnum(section_type)) {
                return self.section_refs[i];
            }
        }
        return null;
    }

    /// The UAL index at which this snapshot was taken.
    pub fn snapshotIndex(self: *const SnapshotReader) u64 {
        return self.header.ual_index;
    }

    /// The Raft term at snapshot time.
    pub fn snapshotTerm(self: *const SnapshotReader) u64 {
        return self.header.raft_term;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Filename / MANIFEST Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Format a snapshot filename: `{index:0>10}-{timestamp_ns}.fsnap`
pub fn snapshotFilename(buf: []u8, index: u64, timestamp_ns: u64) []const u8 {
    return std.fmt.bufPrint(buf, "{d:0>10}-{d}.fsnap", .{ index, timestamp_ns }) catch buf[0..0];
}

/// Parse index and timestamp from a snapshot filename.
pub fn parseSnapshotFilename(name: []const u8) ?SnapshotInfo {
    if (!std.mem.endsWith(u8, name, ".fsnap")) return null;
    if (name.len < 7) return null; // minimum: "0-0.fsnap"
    const inner = name[0 .. name.len - 6];
    const dash = std.mem.indexOf(u8, inner, "-") orelse return null;
    const index = std.fmt.parseInt(u64, inner[0..dash], 10) catch return null;
    const ts = std.fmt.parseInt(u64, inner[dash + 1 ..], 10) catch return null;
    return .{ .index = index, .timestamp_ns = ts };
}

/// Atomically write a MANIFEST file pointing to the given snapshot filename.
pub fn writeManifest(dir: std.fs.Dir, filename: []const u8) !void {
    {
        const file = try dir.createFile("MANIFEST.tmp", .{});
        defer file.close();
        try file.writeAll(filename);
        try file.sync();
    }
    try dir.rename("MANIFEST.tmp", "MANIFEST");
}

/// Read the current snapshot filename from MANIFEST. Returns null if no MANIFEST exists.
pub fn readManifest(dir: std.fs.Dir, buf: []u8) !?[]const u8 {
    const file = dir.openFile("MANIFEST", .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close();
    const n = try file.readAll(buf);
    if (n == 0) return null;
    // Trim trailing whitespace
    var end = n;
    while (end > 0 and (buf[end - 1] == '\n' or buf[end - 1] == '\r' or buf[end - 1] == ' ')) {
        end -= 1;
    }
    return buf[0..end];
}

/// Result of loading a snapshot file.
pub const SnapshotLoad = struct { data: []u8, reader: SnapshotReader };

/// Load a snapshot by filename from a directory.
/// Returns the raw snapshot bytes. Caller owns the allocation.
pub fn loadSnapshotByName(allocator: Allocator, dir: std.fs.Dir, filename: []const u8) !?SnapshotLoad {
    return loadSnapshotFile(allocator, dir, filename);
}

/// Load the latest snapshot from a directory (reads MANIFEST for the filename).
/// Returns the raw snapshot bytes. Caller owns the allocation.
pub fn loadLatestSnapshot(allocator: Allocator, dir: std.fs.Dir) !?SnapshotLoad {
    var manifest_buf: [256]u8 = undefined;
    const filename = try readManifest(dir, &manifest_buf) orelse return null;
    return loadSnapshotFile(allocator, dir, filename);
}

fn loadSnapshotFile(allocator: Allocator, dir: std.fs.Dir, filename: []const u8) !?SnapshotLoad {

    const file = dir.openFile(filename, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close();

    const stat = try file.stat();
    const data = try allocator.alloc(u8, stat.size);
    errdefer allocator.free(data);

    const bytes_read = try file.readAll(data);
    if (bytes_read != stat.size) {
        allocator.free(data);
        return null;
    }

    // Validate the snapshot
    const reader = SnapshotReader.init(data) catch {
        allocator.free(data);
        return null;
    };

    return .{ .data = data, .reader = reader };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "SnapshotHeader is 64 bytes" {
    try testing.expectEqual(@as(usize, 64), @sizeOf(SnapshotHeader));
}

test "SnapshotFooter is 16 bytes" {
    try testing.expectEqual(@as(usize, 16), @sizeOf(SnapshotFooter));
}

test "SectionHeader is 12 bytes" {
    try testing.expectEqual(@as(usize, 12), @sizeOf(SectionHeader));
}

test "snapshot: empty snapshot round-trip" {
    const allocator = testing.allocator;

    var builder = SnapshotBuilder.init(allocator, 42, 1000, 5, 999_000_000);
    defer builder.deinit();

    const data = try builder.seal();
    defer allocator.free(data);

    try testing.expectEqual(HEADER_SIZE + FOOTER_SIZE, data.len);

    const reader = try SnapshotReader.init(data);
    try testing.expectEqual(@as(u32, 42), reader.header.partition_id);
    try testing.expectEqual(@as(u64, 1000), reader.header.ual_index);
    try testing.expectEqual(@as(u64, 5), reader.header.raft_term);
    try testing.expectEqual(@as(u64, 999_000_000), reader.header.timestamp_ns);
    try testing.expectEqual(@as(u32, 0), reader.section_count);
    try testing.expect(reader.getSection(0) == null);
}

test "snapshot: single section round-trip" {
    const allocator = testing.allocator;

    var builder = SnapshotBuilder.init(allocator, 7, 500, 3, 12345);
    defer builder.deinit();

    const kv_data = "key1\x00value1\x00key2\x00value2";
    try builder.addSection(.kv, kv_data);

    const data = try builder.seal();
    defer allocator.free(data);

    const reader = try SnapshotReader.init(data);
    try testing.expectEqual(@as(u32, 1), reader.section_count);
    try testing.expectEqual(@as(u64, 500), reader.snapshotIndex());
    try testing.expectEqual(@as(u64, 3), reader.snapshotTerm());

    const kv_ref = reader.findSection(.kv) orelse return error.SectionNotFound;
    try testing.expectEqualSlices(u8, kv_data, kv_ref.data);
}

test "snapshot: multiple sections round-trip" {
    const allocator = testing.allocator;

    var builder = SnapshotBuilder.init(allocator, 1, 2000, 10, 5555);
    defer builder.deinit();

    const kv_data = "kv-projection-state";
    const queue_data = "queue-projection-state";
    const ts_data = "ts-projection-state";

    try builder.addSection(.kv, kv_data);
    try builder.addSection(.queue, queue_data);
    try builder.addSection(.ts, ts_data);

    const data = try builder.seal();
    defer allocator.free(data);

    const reader = try SnapshotReader.init(data);
    try testing.expectEqual(@as(u32, 3), reader.section_count);

    // By index
    const s0 = reader.getSection(0).?;
    try testing.expectEqual(@as(u8, 0x01), s0.section_type);
    try testing.expectEqualSlices(u8, kv_data, s0.data);

    const s1 = reader.getSection(1).?;
    try testing.expectEqual(@as(u8, 0x02), s1.section_type);
    try testing.expectEqualSlices(u8, queue_data, s1.data);

    const s2 = reader.getSection(2).?;
    try testing.expectEqual(@as(u8, 0x03), s2.section_type);
    try testing.expectEqualSlices(u8, ts_data, s2.data);

    // By type
    const found_ts = reader.findSection(.ts).?;
    try testing.expectEqualSlices(u8, ts_data, found_ts.data);

    // Missing type
    try testing.expect(reader.findSection(.stream) == null);
}

test "snapshot: CRC validation — corrupt data detected" {
    const allocator = testing.allocator;

    var builder = SnapshotBuilder.init(allocator, 1, 100, 1, 0);
    defer builder.deinit();

    try builder.addSection(.kv, "some important data");

    const data = try builder.seal();
    defer allocator.free(data);

    // Verify valid first
    _ = try SnapshotReader.init(data);

    // Corrupt a byte in the section data area
    data[HEADER_SIZE + SECTION_HEADER_SIZE + 3] ^= 0xFF;

    const result = SnapshotReader.init(data);
    try testing.expectError(error.InvalidCrc, result);
}

test "snapshot: invalid header magic" {
    var buf: [HEADER_SIZE + FOOTER_SIZE]u8 = .{0} ** (HEADER_SIZE + FOOTER_SIZE);
    buf[0] = 'X'; // wrong magic

    const result = SnapshotReader.init(&buf);
    try testing.expectError(error.InvalidHeaderMagic, result);
}

test "snapshot: invalid footer magic" {
    const allocator = testing.allocator;

    var builder = SnapshotBuilder.init(allocator, 1, 100, 1, 0);
    defer builder.deinit();

    const data = try builder.seal();
    defer allocator.free(data);

    // Corrupt footer magic
    data[data.len - FOOTER_SIZE + 4] = 'X'; // first byte of footer magic

    const result = SnapshotReader.init(data);
    try testing.expectError(error.InvalidFooterMagic, result);
}

test "snapshot: too small input" {
    const buf = [_]u8{0} ** 10;
    const result = SnapshotReader.init(&buf);
    try testing.expectError(error.TooSmall, result);
}

test "snapshot: version mismatch" {
    const allocator = testing.allocator;

    var builder = SnapshotBuilder.init(allocator, 1, 100, 1, 0);
    defer builder.deinit();

    const data = try builder.seal();
    defer allocator.free(data);

    // Corrupt version field (offset 8, u16 LE)
    data[8] = 99;
    data[9] = 0;

    // Need to recompute CRC since we changed header
    const footer_start = data.len - FOOTER_SIZE;
    const new_crc = checksum_mod.checksum(data[0..footer_start]);
    const crc_bytes = std.mem.asBytes(&new_crc);
    @memcpy(data[footer_start..][0..4], crc_bytes);

    const result = SnapshotReader.init(data);
    try testing.expectError(error.InvalidVersion, result);
}

test "snapshot filename: format and parse" {
    var buf: [128]u8 = undefined;
    const name = snapshotFilename(&buf, 1000, 1234567890);
    try testing.expectEqualStrings("0000001000-1234567890.fsnap", name);

    const info = parseSnapshotFilename(name).?;
    try testing.expectEqual(@as(u64, 1000), info.index);
    try testing.expectEqual(@as(u64, 1234567890), info.timestamp_ns);
}

test "snapshot filename: parse invalid" {
    try testing.expect(parseSnapshotFilename("abc-123.fsnap") == null);
    try testing.expect(parseSnapshotFilename("100-200.txt") == null);
    try testing.expect(parseSnapshotFilename("") == null);
    try testing.expect(parseSnapshotFilename("short") == null);
}

test "snapshot: recovery — load latest snapshot index" {
    const allocator = testing.allocator;

    // Build snapshot at UAL index 500, term 3
    var builder = SnapshotBuilder.init(allocator, 0, 500, 3, 100_000);
    defer builder.deinit();

    const kv_state = "kv-data-at-index-500";
    try builder.addSection(.kv, kv_state);

    const data = try builder.seal();
    defer allocator.free(data);

    // reader tells us exactly where to resume replay
    const reader = try SnapshotReader.init(data);
    try testing.expectEqual(@as(u64, 500), reader.snapshotIndex());
    try testing.expectEqual(@as(u64, 3), reader.snapshotTerm());

    // Projection state is intact
    const kv_ref = reader.findSection(.kv).?;
    try testing.expectEqualSlices(u8, kv_state, kv_ref.data);
}

test "snapshot: large section data" {
    const allocator = testing.allocator;

    // Create a 64KB section to test non-trivial sizes
    const large_data = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(large_data);
    for (large_data, 0..) |*b, i| {
        b.* = @truncate(i);
    }

    var builder = SnapshotBuilder.init(allocator, 99, 9999, 7, 0);
    defer builder.deinit();

    try builder.addSection(.stream, large_data);

    const snapshot = try builder.seal();
    defer allocator.free(snapshot);

    const reader = try SnapshotReader.init(snapshot);
    const stream_ref = reader.findSection(.stream).?;
    try testing.expectEqual(large_data.len, stream_ref.data.len);
    try testing.expectEqualSlices(u8, large_data, stream_ref.data);
}

test "snapshot: MANIFEST write and read" {
    // Use a temporary directory for disk operations
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    // No MANIFEST yet
    var read_buf: [256]u8 = undefined;
    const nothing = try readManifest(tmp.dir, &read_buf);
    try testing.expect(nothing == null);

    // Write MANIFEST
    const filename = "0000000500-100000.fsnap";
    try writeManifest(tmp.dir, filename);

    // Read back
    const result = try readManifest(tmp.dir, &read_buf);
    try testing.expectEqualStrings(filename, result.?);

    // Overwrite MANIFEST
    const filename2 = "0000001000-200000.fsnap";
    try writeManifest(tmp.dir, filename2);

    const result2 = try readManifest(tmp.dir, &read_buf);
    try testing.expectEqualStrings(filename2, result2.?);
}

test "snapshot: writeToDir and loadLatestSnapshot" {
    const allocator = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    // Build and write snapshot
    var builder = SnapshotBuilder.init(allocator, 5, 750, 4, 888_000);
    defer builder.deinit();

    try builder.addSection(.kv, "kv-state-750");
    try builder.addSection(.queue, "queue-state-750");

    try builder.writeToDir(tmp.dir);

    // Load it back
    const loaded = try loadLatestSnapshot(allocator, tmp.dir) orelse return error.NoSnapshot;
    defer allocator.free(loaded.data);

    try testing.expectEqual(@as(u64, 750), loaded.reader.snapshotIndex());
    try testing.expectEqual(@as(u64, 4), loaded.reader.snapshotTerm());
    try testing.expectEqual(@as(u32, 2), loaded.reader.section_count);

    const kv_ref = loaded.reader.findSection(.kv).?;
    try testing.expectEqualSlices(u8, "kv-state-750", kv_ref.data);

    const q_ref = loaded.reader.findSection(.queue).?;
    try testing.expectEqualSlices(u8, "queue-state-750", q_ref.data);
}
