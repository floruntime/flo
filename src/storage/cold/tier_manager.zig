//! Cold Tier Manager — orchestrates warm → cold segment archival
//!
//! Connects the ColdBackend (file/s3/azure), ColdManifest (metadata),
//! and SegmentWriter to form the complete tiered storage pipeline.
//!
//! Responsibilities:
//! - Upload sealed warm segments to cold storage
//! - Track uploaded segments in the cold manifest
//! - Download cold segments on demand (for historical reads)
//! - Manage local segment lifecycle (optional eviction after upload)
//!
//! Data flow:
//!   UAL hot ring → SegmentWriter → .flseg on disk (warm)
//!                 ↓ (ColdTierManager.archiveSegment)
//!              ColdBackend.upload → manifest.addEntry
//!                 ↓ (optional)
//!              delete local .flseg after retention period
//!
//! Recovery:
//!   Snapshot (projections) + warm segments (local) + cold manifest (metadata)
//!   Cold data is never eagerly fetched — on-demand only.

const std = @import("std");
const Allocator = std.mem.Allocator;
const backend_mod = @import("backend.zig");
const ColdBackend = backend_mod.ColdBackend;
const BackendError = backend_mod.BackendError;
const ObjectMetadata = backend_mod.ObjectMetadata;
const StreamSource = backend_mod.StreamSource;
const SliceStreamSource = backend_mod.SliceStreamSource;
const BufferStreamSink = backend_mod.BufferStreamSink;
const manifest_mod = @import("manifest.zig");
const ColdManifest = manifest_mod.ColdManifest;
const ColdEntry = manifest_mod.ColdEntry;
const checksum_mod = @import("../../util/checksum.zig");
const segment_mod = @import("../ual/segment.zig");

pub const ColdTierError = error{
    BackendUnavailable,
    SegmentNotFound,
    UploadFailed,
    DownloadFailed,
    ManifestCorrupted,
    OutOfMemory,
    IoError,
};

/// Configuration for the cold tier manager
pub const ColdTierConfig = struct {
    /// Shard ID (for namespacing cold keys)
    shard_id: u16 = 0,

    /// Partition ID (for namespacing cold keys)
    partition_id: u32 = 0,

    /// Key prefix for all cold objects (e.g., "ual/shard-0/")
    key_prefix: []const u8 = "",

    /// Maximum local segments before triggering archival (0 = manual only)
    max_local_segments: usize = 100,

    /// Whether to delete local segments after successful upload
    evict_after_upload: bool = false,

    /// Verify checksums on download
    verify_checksums: bool = true,
};

/// Manages the warm → cold segment lifecycle.
///
/// Holds a ColdBackend (the destination store) and a ColdManifest (the metadata
/// index). Provides methods to archive warm segments, retrieve cold segments,
/// and persist/load the manifest itself.
pub const ColdTierManager = struct {
    allocator: Allocator,
    cold_backend: ColdBackend,
    manifest: ColdManifest,
    config: ColdTierConfig,

    /// Stats for observability
    segments_uploaded: u64,
    segments_downloaded: u64,
    bytes_uploaded: u64,
    bytes_downloaded: u64,

    const Self = @This();

    pub fn init(allocator: Allocator, cold_backend: ColdBackend, config: ColdTierConfig) Self {
        return .{
            .allocator = allocator,
            .cold_backend = cold_backend,
            .manifest = ColdManifest.init(allocator),
            .config = config,
            .segments_uploaded = 0,
            .segments_downloaded = 0,
            .bytes_uploaded = 0,
            .bytes_downloaded = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.manifest.deinit();
    }

    // ── Archive (warm → cold) ──────────────────────────────────────────

    /// Archive a sealed segment file to cold storage.
    ///
    /// 1. Reads the .flseg file from disk
    /// 2. Extracts header metadata (index range, timestamps)
    /// 3. Uploads to ColdBackend with a structured key
    /// 4. Records the entry in the ColdManifest
    /// 5. Optionally deletes the local file
    ///
    pub fn archiveSegmentFile(self: *Self, segment_path: []const u8) !void {
        // Read segment file
        const file = std.fs.cwd().openFile(segment_path, .{}) catch return ColdTierError.SegmentNotFound;
        defer file.close();

        const stat = file.stat() catch return ColdTierError.IoError;
        const data = self.allocator.alloc(u8, stat.size) catch return ColdTierError.OutOfMemory;
        defer self.allocator.free(data);

        const bytes_read = file.readAll(data) catch return ColdTierError.IoError;
        if (bytes_read != stat.size) return ColdTierError.SegmentNotFound;

        return self.archiveSegmentData(data, segment_path);
    }

    /// Archive segment data already in memory.
    /// The caller provides the raw .flseg bytes and the original filename/path
    /// (used only for cold key generation).
    pub fn archiveSegmentData(self: *Self, data: []const u8, source_name: []const u8) !void {
        if (data.len < segment_mod.HEADER_SIZE) return ColdTierError.SegmentNotFound;

        // Parse segment header to extract metadata
        const hdr: *const segment_mod.SegmentHeader = @ptrCast(@alignCast(data[0..segment_mod.HEADER_SIZE]));

        // Validate magic
        if (!std.mem.eql(u8, &hdr.magic, &segment_mod.HEADER_MAGIC)) {
            return ColdTierError.SegmentNotFound;
        }

        // Build cold storage key in a stack buffer.
        // upload() uses the key transiently; manifest.addEntry() dupes it.
        const basename = std.fs.path.basename(source_name);
        var key_buf: [512]u8 = undefined;
        const cold_key = std.fmt.bufPrint(&key_buf, "{s}shard-{d}/partition-{d}/{s}", .{
            self.config.key_prefix,
            self.config.shard_id,
            self.config.partition_id,
            basename,
        }) catch return ColdTierError.IoError;

        // Upload to cold backend (uses key transiently, no ownership transfer)
        self.cold_backend.upload(cold_key, data, .{
            .size = data.len,
            .content_type = "application/octet-stream",
        }) catch return ColdTierError.UploadFailed;

        // Compute checksum
        const crc = checksum_mod.checksum(data);

        // Record in manifest (addEntry dupes the location string)
        self.manifest.addEntry(.{
            .min_index = hdr.first_index,
            .max_index = hdr.last_index,
            .min_timestamp_ns = hdr.first_ts_ns,
            .max_timestamp_ns = hdr.last_ts_ns,
            .location = cold_key,
            .size_bytes = data.len,
            .checksum = crc,
        }) catch return ColdTierError.OutOfMemory;

        // Update stats
        self.segments_uploaded += 1;
        self.bytes_uploaded += data.len;
    }

    // ── Retrieve (cold → local) ────────────────────────────────────────

    /// Download a cold segment by UAL index.
    ///
    /// Looks up the manifest for the entry containing the given index,
    /// then downloads the segment data from cold storage.
    /// Caller owns the returned slice.
    pub fn downloadSegment(self: *Self, ual_index: u64) ![]u8 {
        const entry = self.manifest.findByIndex(ual_index) orelse return ColdTierError.SegmentNotFound;
        return self.downloadByLocation(entry.location, entry.size_bytes, entry.checksum);
    }

    /// Download a cold segment by its location key.
    /// Optionally verifies the checksum. Caller owns the returned slice.
    pub fn downloadByLocation(self: *Self, location: []const u8, expected_size: u64, expected_checksum: u32) ![]u8 {
        const buf = self.allocator.alloc(u8, expected_size) catch return ColdTierError.OutOfMemory;
        errdefer self.allocator.free(buf);

        const data = self.cold_backend.download(location, buf) catch |err| switch (err) {
            error.ObjectNotFound => return ColdTierError.SegmentNotFound,
            else => return ColdTierError.DownloadFailed,
        };

        // Verify checksum if configured
        if (self.config.verify_checksums and expected_checksum != 0) {
            const actual_crc = checksum_mod.checksum(data);
            if (actual_crc != expected_checksum) {
                self.allocator.free(buf);
                return ColdTierError.ManifestCorrupted;
            }
        }

        self.segments_downloaded += 1;
        self.bytes_downloaded += data.len;

        // The caller owns buf; data is a slice of buf
        return buf;
    }

    // ── Manifest persistence ───────────────────────────────────────────

    /// Save the manifest to a local directory.
    /// Writes atomically: .fcold.tmp → fsync → rename .fcold
    pub fn saveManifest(self: *Self, dir_path: []const u8) !void {
        const data = try self.manifest.save(self.allocator);
        defer self.allocator.free(data);

        var path_buf: [512]u8 = undefined;
        const manifest_path = std.fmt.bufPrint(&path_buf, "{s}/cold.fcold", .{dir_path}) catch return error.InvalidManifest;

        var tmp_buf: [520]u8 = undefined;
        const tmp_path = std.fmt.bufPrint(&tmp_buf, "{s}/cold.fcold.tmp", .{dir_path}) catch return error.InvalidManifest;

        // Ensure dir exists
        std.fs.cwd().makePath(dir_path) catch {};

        const file = std.fs.cwd().createFile(tmp_path, .{}) catch return error.InvalidManifest;
        file.writeAll(data) catch {
            file.close();
            return error.InvalidManifest;
        };
        file.sync() catch {};
        file.close();

        std.fs.cwd().rename(tmp_path, manifest_path) catch return error.InvalidManifest;
    }

    /// Load the manifest from a local directory.
    pub fn loadManifest(self: *Self, dir_path: []const u8) !void {
        var path_buf: [512]u8 = undefined;
        const manifest_path = std.fmt.bufPrint(&path_buf, "{s}/cold.fcold", .{dir_path}) catch return error.InvalidManifest;

        const file = std.fs.cwd().openFile(manifest_path, .{}) catch return; // No manifest = fresh start
        defer file.close();

        const stat = file.stat() catch return error.InvalidManifest;
        const data = self.allocator.alloc(u8, stat.size) catch return error.OutOfMemory;
        defer self.allocator.free(data);

        const bytes_read = file.readAll(data) catch return error.InvalidManifest;
        if (bytes_read != stat.size) return error.InvalidManifest;

        // Replace current manifest
        self.manifest.deinit();
        self.manifest = try ColdManifest.load(self.allocator, data);
    }

    // ── Archival scan ──────────────────────────────────────────────────

    /// Scan a shard data directory for .flseg files and archive any
    /// that are not already in the manifest.
    /// Returns the number of segments archived.
    pub fn archiveWarmSegments(self: *Self, shard_dir: []const u8) !u64 {
        var dir = std.fs.cwd().openDir(shard_dir, .{ .iterate = true }) catch return 0;
        defer dir.close();

        var archived: u64 = 0;
        var iter = dir.iterate();
        while (iter.next() catch null) |de| {
            if (de.kind != .file) continue;
            if (!std.mem.endsWith(u8, de.name, ".flseg")) continue;

            // Build full path
            var path_buf: [512]u8 = undefined;
            const full_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ shard_dir, de.name }) catch continue;

            // Read segment header to check if already archived
            const file = std.fs.cwd().openFile(full_path, .{}) catch continue;
            var hdr_bytes: [segment_mod.HEADER_SIZE]u8 = undefined;
            const n = file.readAll(&hdr_bytes) catch {
                file.close();
                continue;
            };
            file.close();

            if (n < segment_mod.HEADER_SIZE) continue;

            const hdr: *const segment_mod.SegmentHeader = @ptrCast(@alignCast(&hdr_bytes));
            if (!std.mem.eql(u8, &hdr.magic, &segment_mod.HEADER_MAGIC)) continue;

            // Check if this range is already in the manifest
            if (self.manifest.findByIndex(hdr.first_index) != null) continue;

            // Archive this segment
            self.archiveSegmentFile(full_path) catch continue;
            archived += 1;
        }

        return archived;
    }

    // ── Queries ────────────────────────────────────────────────────────

    /// Check if a UAL index has been archived to cold storage.
    pub fn isInCold(self: *const Self, ual_index: u64) bool {
        return self.manifest.findByIndex(ual_index) != null;
    }

    /// Number of segments tracked in the manifest.
    pub fn segmentCount(self: *const Self) usize {
        return self.manifest.count();
    }

    /// Total bytes stored in cold storage.
    pub fn totalColdBytes(self: *const Self) u64 {
        return self.manifest.totalBytes();
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;
const FileBackend = @import("file.zig").FileBackend;
const SegmentWriter = @import("../ual/writer.zig").SegmentWriter;
const entry_mod = @import("../ual/entry.zig");

fn makeTestSegment(allocator: Allocator) ![]u8 {
    var writer = SegmentWriter.init(allocator, 0, .none);
    defer writer.deinit();

    // Write 5 entries
    var i: u64 = 1;
    while (i <= 5) : (i += 1) {
        const entry = entry_mod.buildEntry(.kv_put, 0, 1, i, i * 1000, "test-payload");
        try writer.addEntry(&entry);
    }

    return writer.seal();
}

test "ColdTierManager: archive and download round-trip" {
    const allocator = testing.allocator;
    const test_dir = "/tmp/test_cold_tier_manager";
    const cold_dir = "/tmp/test_cold_tier_backend";

    // Cleanup
    std.fs.cwd().deleteTree(test_dir) catch {};
    std.fs.cwd().deleteTree(cold_dir) catch {};
    defer std.fs.cwd().deleteTree(test_dir) catch {};
    defer std.fs.cwd().deleteTree(cold_dir) catch {};

    // Create a file backend for cold storage
    const file_backend = try FileBackend.init(allocator, .{
        .base_path = cold_dir,
        .create_dirs = true,
    });
    const cb = file_backend.asBackend();
    defer cb.deinitBackend();

    // Create ColdTierManager
    var manager = ColdTierManager.init(allocator, cb, .{
        .shard_id = 0,
        .partition_id = 42,
        .key_prefix = "ual/",
        .verify_checksums = true,
    });
    defer manager.deinit();

    // Create a test segment
    const seg_data = try makeTestSegment(allocator);
    defer allocator.free(seg_data);

    // Write segment to a local file (simulating warm tier)
    std.fs.cwd().makePath(test_dir) catch {};
    const seg_path = try std.fmt.allocPrint(allocator, "{s}/seg-1.flseg", .{test_dir});
    defer allocator.free(seg_path);
    {
        const file = try std.fs.cwd().createFile(seg_path, .{});
        defer file.close();
        try file.writeAll(seg_data);
    }

    // Archive the segment
    try manager.archiveSegmentFile(seg_path);

    // Verify stats
    try testing.expectEqual(@as(u64, 1), manager.segments_uploaded);
    try testing.expect(manager.bytes_uploaded > 0);
    try testing.expectEqual(@as(usize, 1), manager.segmentCount());

    // Verify index is in cold storage
    try testing.expect(manager.isInCold(1));
    try testing.expect(manager.isInCold(5));

    // Download the segment back
    const downloaded = try manager.downloadSegment(1);
    defer allocator.free(downloaded);

    // Verify downloaded data matches original
    try testing.expectEqual(seg_data.len, downloaded.len);
    try testing.expectEqualSlices(u8, seg_data, downloaded);

    try testing.expectEqual(@as(u64, 1), manager.segments_downloaded);
}

test "ColdTierManager: manifest save and load persistence" {
    const allocator = testing.allocator;
    const test_dir = "/tmp/test_cold_tier_manifest_persist";
    const cold_dir = "/tmp/test_cold_tier_manifest_backend";

    std.fs.cwd().deleteTree(test_dir) catch {};
    std.fs.cwd().deleteTree(cold_dir) catch {};
    defer std.fs.cwd().deleteTree(test_dir) catch {};
    defer std.fs.cwd().deleteTree(cold_dir) catch {};

    const file_backend = try FileBackend.init(allocator, .{
        .base_path = cold_dir,
        .create_dirs = true,
    });
    const cb = file_backend.asBackend();
    defer cb.deinitBackend();

    // Create manager and archive a segment
    {
        var manager = ColdTierManager.init(allocator, cb, .{
            .shard_id = 0,
            .partition_id = 0,
        });
        defer manager.deinit();

        const seg_data = try makeTestSegment(allocator);
        defer allocator.free(seg_data);

        try manager.archiveSegmentData(seg_data, "seg-1.flseg");
        try testing.expectEqual(@as(usize, 1), manager.segmentCount());

        // Persist the manifest
        try manager.saveManifest(test_dir);
    }

    // Create a NEW manager and load the manifest
    {
        var manager = ColdTierManager.init(allocator, cb, .{
            .shard_id = 0,
            .partition_id = 0,
        });
        defer manager.deinit();

        try manager.loadManifest(test_dir);

        // Should have the entry from the previous instance
        try testing.expectEqual(@as(usize, 1), manager.segmentCount());
        try testing.expect(manager.isInCold(1));
        try testing.expect(manager.isInCold(5));
        try testing.expect(!manager.isInCold(10));
    }
}

test "ColdTierManager: archiveWarmSegments scans directory" {
    const allocator = testing.allocator;
    const warm_dir = "/tmp/test_cold_tier_warm_scan";
    const cold_dir = "/tmp/test_cold_tier_scan_backend";

    std.fs.cwd().deleteTree(warm_dir) catch {};
    std.fs.cwd().deleteTree(cold_dir) catch {};
    defer std.fs.cwd().deleteTree(warm_dir) catch {};
    defer std.fs.cwd().deleteTree(cold_dir) catch {};

    std.fs.cwd().makePath(warm_dir) catch {};

    // Create two segment files in the warm directory
    var writer1 = SegmentWriter.init(allocator, 0, .none);
    defer writer1.deinit();
    var i: u64 = 1;
    while (i <= 3) : (i += 1) {
        const e = entry_mod.buildEntry(.kv_put, 0, 1, i, i * 1000, "data");
        try writer1.addEntry(&e);
    }
    try writer1.writeToFile(warm_dir);

    var writer2 = SegmentWriter.init(allocator, 0, .none);
    defer writer2.deinit();
    i = 4;
    while (i <= 6) : (i += 1) {
        const e = entry_mod.buildEntry(.kv_put, 0, 1, i, i * 1000, "data");
        try writer2.addEntry(&e);
    }
    try writer2.writeToFile(warm_dir);

    // Create cold backend
    const file_backend = try FileBackend.init(allocator, .{
        .base_path = cold_dir,
        .create_dirs = true,
    });
    const cb = file_backend.asBackend();
    defer cb.deinitBackend();

    var manager = ColdTierManager.init(allocator, cb, .{
        .shard_id = 0,
        .partition_id = 0,
    });
    defer manager.deinit();

    // Archive all warm segments
    const archived = try manager.archiveWarmSegments(warm_dir);
    try testing.expectEqual(@as(u64, 2), archived);
    try testing.expectEqual(@as(usize, 2), manager.segmentCount());

    // Re-running should archive nothing (already tracked)
    const archived2 = try manager.archiveWarmSegments(warm_dir);
    try testing.expectEqual(@as(u64, 0), archived2);
}

test "ColdTierManager: downloadSegment not found" {
    const allocator = testing.allocator;
    const cold_dir = "/tmp/test_cold_tier_notfound";

    std.fs.cwd().deleteTree(cold_dir) catch {};
    defer std.fs.cwd().deleteTree(cold_dir) catch {};

    const file_backend = try FileBackend.init(allocator, .{
        .base_path = cold_dir,
        .create_dirs = true,
    });
    const cb = file_backend.asBackend();
    defer cb.deinitBackend();

    var manager = ColdTierManager.init(allocator, cb, .{});
    defer manager.deinit();

    // No segments in manifest — should get SegmentNotFound
    try testing.expectError(ColdTierError.SegmentNotFound, manager.downloadSegment(42));
}
