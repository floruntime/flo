//! Integration Test: Cold Tier — UAL Segment Archival and On-Demand Retrieval
//!
//! Tests the cold tier lifecycle per TIERED_RECOVERY_DESIGN.md:
//!
//! Key design principles tested:
//! - UAL segments are what get archived to cold storage (not projection state)
//! - Projections (KV, Queue, Stream, TS) are rebuilt from snapshots + warm UAL
//! - Cold segments are NEVER fetched at startup — only on-demand for:
//!   - Stream reads at offsets older than local segments
//!   - TS queries for time ranges beyond local blocks
//!   - KV MVCC version lookups for old versions that were tiered out
//!
//! Test scenarios:
//! 1. UAL segment archival round-trip: warm .flseg → FileBackend → manifest
//! 2. On-demand stream read from cold (historical offset fetch)
//! 3. KV MVCC version lookup from cold (time-travel query)
//! 4. Normal recovery does NOT touch cold (manifest = metadata only)
//! 5. Idempotent archival (re-scan does not duplicate)
//! 6. Manifest persists across restarts

const std = @import("std");
const testing = std.testing;
const src = @import("src");

const SegmentWriter = src.storage.ual.writer.SegmentWriter;
const entry_mod = src.storage.ual.entry;
const segment_mod = src.storage.ual.segment;
const ColdTierManager = src.storage.cold.ColdTierManager;
const ColdTierConfig = src.storage.cold.ColdTierConfig;
const FileBackend = src.storage.cold.FileBackend;

/// Helper: parse a single UAL entry from segment data at `pos`.
/// Returns the parsed entry header, payload slice, and next position.
fn parseEntryAt(data: []const u8, pos: usize) ?struct {
    hdr: *const entry_mod.Header,
    payload: []const u8,
    next_pos: usize,
} {
    if (pos + entry_mod.HEADER_SIZE > data.len) return null;
    const ehdr: *const entry_mod.Header = @ptrCast(@alignCast(data[pos..][0..entry_mod.HEADER_SIZE]));
    const entry_end = pos + entry_mod.HEADER_SIZE + ehdr.payload_len;
    if (entry_end > data.len) return null;
    return .{
        .hdr = ehdr,
        .payload = data[pos + entry_mod.HEADER_SIZE .. entry_end],
        .next_pos = entry_end,
    };
}

/// Helper: extract key and value from a command payload.
/// Command payload layout: namespace_hash(4) + key_length(2) + value_length(4) + key + value
fn parseCommandPayload(payload: []const u8) ?struct { key: []const u8, value: []const u8 } {
    if (payload.len < entry_mod.COMMAND_PREFIX_SIZE) return null;
    const key_len = std.mem.readInt(u16, payload[4..6], .little);
    const val_len = std.mem.readInt(u32, payload[6..10], .little);
    const key_start = entry_mod.COMMAND_PREFIX_SIZE;
    const val_start = key_start + key_len;
    if (val_start + val_len > payload.len) return null;
    return .{
        .key = payload[key_start .. key_start + key_len],
        .value = payload[val_start .. val_start + val_len],
    };
}

/// Helper: write a segment with KV entries to a directory.
fn writeKVSegment(
    allocator: std.mem.Allocator,
    dir: []const u8,
    start_index: u64,
    count: u64,
) !void {
    var writer = SegmentWriter.init(allocator, 0, .none);
    defer writer.deinit();
    var i: u64 = start_index;
    while (i < start_index + count) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "key-{d:0>6}", .{i}) catch unreachable;
        var val_buf: [64]u8 = undefined;
        const val = std.fmt.bufPrint(&val_buf, "value-{d:0>6}", .{i}) catch unreachable;
        var payload_buf: [256]u8 = undefined;
        const entry = entry_mod.buildCommandEntry(
            .kv_put,
            0,
            1,
            i,
            i * 1_000_000,
            0,
            key,
            val,
            &payload_buf,
        ) orelse continue;
        try writer.addEntry(&entry);
    }
    try writer.writeToFile(dir);
}

/// Helper: write a segment with stream_append entries to a directory.
fn writeStreamSegment(
    allocator: std.mem.Allocator,
    dir: []const u8,
    start_index: u64,
    count: u64,
) !void {
    var writer = SegmentWriter.init(allocator, 0, .none);
    defer writer.deinit();
    var i: u64 = start_index;
    while (i < start_index + count) : (i += 1) {
        var val_buf: [128]u8 = undefined;
        const val = std.fmt.bufPrint(&val_buf, "{{\"seq\":{d},\"type\":\"stream-record\"}}", .{i}) catch unreachable;
        var payload_buf: [256]u8 = undefined;
        const entry = entry_mod.buildCommandEntry(
            .stream_append,
            0,
            1,
            i,
            i * 1_000_000,
            0,
            "events",
            val,
            &payload_buf,
        ) orelse continue;
        try writer.addEntry(&entry);
    }
    try writer.writeToFile(dir);
}

test "integration: cold tier — UAL segment archival round-trip" {
    const allocator = testing.allocator;

    //
    // This test verifies the core cold tier pipeline:
    //   warm .flseg files → ColdTierManager.archiveWarmSegments() → FileBackend
    //   → cold files on disk + manifest tracking
    //
    // Per TIERED_RECOVERY_DESIGN.md §1.2: UAL segments are what get tiered.
    // Projections are NOT archived — they're rebuilt from snapshot + warm UAL.
    //

    var warm_tmp = testing.tmpDir(.{});
    defer warm_tmp.cleanup();
    var cold_tmp = testing.tmpDir(.{});
    defer cold_tmp.cleanup();

    const warm_dir = try warm_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(warm_dir);
    const cold_dir = try cold_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(cold_dir);

    // Create 3 warm UAL segments with mixed entry types (KV + stream)
    // Segment 1: KV puts (indices 1–10)
    try writeKVSegment(allocator, warm_dir, 1, 10);
    // Segment 2: Stream appends (indices 11–20)
    try writeStreamSegment(allocator, warm_dir, 11, 10);
    // Segment 3: More KV puts (indices 21–30)
    try writeKVSegment(allocator, warm_dir, 21, 10);

    // Verify 3 .flseg files on disk
    {
        var flseg_count: usize = 0;
        var dir = try std.fs.cwd().openDir(warm_dir, .{ .iterate = true });
        defer dir.close();
        var iter = dir.iterate();
        while (try iter.next()) |de| {
            if (std.mem.endsWith(u8, de.name, ".flseg")) flseg_count += 1;
        }
        try testing.expectEqual(@as(usize, 3), flseg_count);
    }

    // Archive all warm segments to cold storage (FileBackend)
    const file_backend = try FileBackend.init(allocator, .{
        .base_path = cold_dir,
        .create_dirs = true,
    });
    const cb = file_backend.asBackend();
    defer cb.deinitBackend();

    var manager = ColdTierManager.init(allocator, cb, .{
        .shard_id = 0,
        .key_prefix = "ual/",
        .verify_checksums = true,
    });
    defer manager.deinit();

    const archived = try manager.archiveWarmSegments(warm_dir);
    try testing.expectEqual(@as(u64, 3), archived);
    try testing.expectEqual(@as(usize, 3), manager.segmentCount());
    try testing.expect(manager.bytes_uploaded > 0);

    // All UAL indices 1–30 should be tracked in the manifest
    var idx: u64 = 1;
    while (idx <= 30) : (idx += 1) {
        try testing.expect(manager.isInCold(idx));
    }
    try testing.expect(!manager.isInCold(0));
    try testing.expect(!manager.isInCold(31));

    // Verify cold files actually exist on the FileBackend's disk
    {
        const cold_fs_dir = try std.fmt.allocPrint(allocator, "{s}/ual/00000", .{cold_dir});
        defer allocator.free(cold_fs_dir);

        var cold_file_count: usize = 0;
        var dir = std.fs.cwd().openDir(cold_fs_dir, .{ .iterate = true }) catch |err| {
            std.debug.print("Failed to open cold dir: {}\n", .{err});
            return error.TestUnexpectedResult;
        };
        defer dir.close();
        var iter = dir.iterate();
        while (try iter.next()) |de| {
            if (std.mem.endsWith(u8, de.name, ".flseg")) cold_file_count += 1;
        }
        try testing.expectEqual(@as(usize, 3), cold_file_count);
    }

    // Verify segment headers are intact after cold round-trip
    const seg1 = try manager.downloadSegment(1);
    defer allocator.free(seg1);
    const hdr1: *const segment_mod.SegmentHeader = @ptrCast(@alignCast(seg1[0..segment_mod.HEADER_SIZE]));
    try testing.expect(std.mem.eql(u8, &hdr1.magic, &segment_mod.HEADER_MAGIC));
    try testing.expectEqual(@as(u64, 1), hdr1.first_index);
    try testing.expectEqual(@as(u64, 10), hdr1.last_index);
    try testing.expectEqual(@as(u32, 10), hdr1.entry_count);

    // Verify second segment (stream entries)
    const seg2 = try manager.downloadSegment(15);
    defer allocator.free(seg2);
    const hdr2: *const segment_mod.SegmentHeader = @ptrCast(@alignCast(seg2[0..segment_mod.HEADER_SIZE]));
    try testing.expectEqual(@as(u64, 11), hdr2.first_index);
    try testing.expectEqual(@as(u64, 20), hdr2.last_index);
    try testing.expectEqual(@as(u32, 10), hdr2.entry_count);

    // Non-existent index should error
    try testing.expectError(error.SegmentNotFound, manager.downloadSegment(100));
}

test "integration: cold tier — on-demand stream read from cold" {
    const allocator = testing.allocator;

    //
    // Per TIERED_RECOVERY_DESIGN.md §1.3:
    //   "Cold segments are NEVER fetched at startup. They're fetched on-demand
    //    when a client explicitly requests historical data that's no longer local."
    //
    // Scenario: A stream consumer requests records at offsets that have been
    // evicted from the hot UAL ring and archived to cold. The read path:
    //   1. StreamProjection has the offset → UAL index mapping (from snapshot)
    //   2. UAL.read() misses (entry evicted from hot ring)
    //   3. ColdTierManager.isInCold(ual_index) → true
    //   4. ColdTierManager.downloadSegment(ual_index) → raw segment bytes
    //   5. Parse entries from downloaded segment → serve to client
    //
    // This test exercises steps 3–5.
    //

    var warm_tmp = testing.tmpDir(.{});
    defer warm_tmp.cleanup();
    var cold_tmp = testing.tmpDir(.{});
    defer cold_tmp.cleanup();

    const warm_dir = try warm_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(warm_dir);
    const cold_dir = try cold_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(cold_dir);

    // Write stream_append entries to a segment and archive it to cold
    try writeStreamSegment(allocator, warm_dir, 1, 10);

    const file_backend = try FileBackend.init(allocator, .{
        .base_path = cold_dir,
        .create_dirs = true,
    });
    const cb = file_backend.asBackend();
    defer cb.deinitBackend();

    var manager = ColdTierManager.init(allocator, cb, .{
        .shard_id = 0,
        .verify_checksums = true,
    });
    defer manager.deinit();

    const archived_count = try manager.archiveWarmSegments(warm_dir);
    try testing.expectEqual(@as(u64, 1), archived_count);

    // Simulate: client requests a stream read at UAL index 5 (which is in cold)
    try testing.expect(manager.isInCold(5));

    // On-demand fetch: download the cold segment containing index 5
    const seg_data = try manager.downloadSegment(5);
    defer allocator.free(seg_data);

    // Parse entries from the downloaded segment to serve to the client
    const hdr: *const segment_mod.SegmentHeader = @ptrCast(@alignCast(seg_data[0..segment_mod.HEADER_SIZE]));
    try testing.expectEqual(@as(u32, 10), hdr.entry_count);

    // Walk entries and verify they are stream_append entries with correct data
    var pos: usize = segment_mod.HEADER_SIZE;
    var stream_entries_found: u32 = 0;
    var replayed: u32 = 0;
    while (replayed < hdr.entry_count) : (replayed += 1) {
        const parsed = parseEntryAt(seg_data, pos) orelse break;
        pos = parsed.next_pos;

        if (parsed.hdr.entry_type == @intFromEnum(entry_mod.EntryType.stream_append)) {
            stream_entries_found += 1;

            // Verify the stream record payload is parseable
            const cmd = parseCommandPayload(parsed.payload);
            try testing.expect(cmd != null);
            try testing.expectEqualStrings("events", cmd.?.key);
            try testing.expect(std.mem.indexOf(u8, cmd.?.value, "stream-record") != null);
        }
    }
    try testing.expectEqual(@as(u32, 10), stream_entries_found);

    // Verify download stats
    try testing.expectEqual(@as(u64, 1), manager.segments_downloaded);
    try testing.expect(manager.bytes_downloaded > 0);

    // Index NOT in cold storage should error
    try testing.expectError(error.SegmentNotFound, manager.downloadSegment(100));
}

test "integration: cold tier — KV MVCC version lookup from cold" {
    const allocator = testing.allocator;

    //
    // Per TIERED_RECOVERY_DESIGN.md §1.1:
    //   "KV current key + value: ALWAYS in RAM."
    //   "Previous MVCC versions: Tier to warm/cold."
    //
    // Scenario: A client does a time-travel query on a KV key. The current
    // version is in the KVProjection (RAM), but old MVCC versions were in
    // UAL entries that have been archived to cold. The on-demand read path:
    //   1. KVProjection has the current version (from snapshot + warm replay)
    //   2. Client requests version at lsn=5 (old version)
    //   3. UAL.read(5) misses (evicted from hot ring)
    //   4. ColdTierManager.downloadSegment(5) → segment bytes
    //   5. Parse entries → find the entry at index 5 → return old version
    //

    var warm_tmp = testing.tmpDir(.{});
    defer warm_tmp.cleanup();
    var cold_tmp = testing.tmpDir(.{});
    defer cold_tmp.cleanup();

    const warm_dir = try warm_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(warm_dir);
    const cold_dir = try cold_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(cold_dir);

    // Write KV entries: same key "user:42" written 10 times (MVCC versions)
    {
        var writer = SegmentWriter.init(allocator, 0, .none);
        defer writer.deinit();
        var i: u64 = 1;
        while (i <= 10) : (i += 1) {
            var val_buf: [64]u8 = undefined;
            const val = std.fmt.bufPrint(&val_buf, "version-{d}", .{i}) catch unreachable;
            var payload_buf: [256]u8 = undefined;
            const entry = entry_mod.buildCommandEntry(
                .kv_put,
                0,
                1,
                i,
                i * 1_000_000,
                0,
                "user:42",
                val,
                &payload_buf,
            ) orelse continue;
            try writer.addEntry(&entry);
        }
        try writer.writeToFile(warm_dir);
    }

    // Archive to cold
    const file_backend = try FileBackend.init(allocator, .{
        .base_path = cold_dir,
        .create_dirs = true,
    });
    const cb = file_backend.asBackend();
    defer cb.deinitBackend();

    var manager = ColdTierManager.init(allocator, cb, .{
        .shard_id = 0,
        .verify_checksums = true,
    });
    defer manager.deinit();

    _ = try manager.archiveWarmSegments(warm_dir);

    // Simulate: client requests MVCC version at lsn=5 ("user:42" at index 5)
    // The current version (lsn=10) is in KVProjection RAM.
    // The old version (lsn=5) is in a cold UAL segment.
    try testing.expect(manager.isInCold(5));

    const seg_data = try manager.downloadSegment(5);
    defer allocator.free(seg_data);

    // Walk the segment to find the entry at index 5
    const seg_hdr: *const segment_mod.SegmentHeader = @ptrCast(@alignCast(seg_data[0..segment_mod.HEADER_SIZE]));
    var pos: usize = segment_mod.HEADER_SIZE;
    var found_version: ?[]const u8 = null;
    var replayed: u32 = 0;
    while (replayed < seg_hdr.entry_count) : (replayed += 1) {
        const parsed = parseEntryAt(seg_data, pos) orelse break;
        pos = parsed.next_pos;

        if (parsed.hdr.index == 5) {
            const cmd = parseCommandPayload(parsed.payload);
            try testing.expect(cmd != null);
            try testing.expectEqualStrings("user:42", cmd.?.key);
            found_version = cmd.?.value;
            break;
        }
    }

    // Verify we found the old MVCC version
    try testing.expect(found_version != null);
    try testing.expectEqualStrings("version-5", found_version.?);
}

test "integration: cold tier — normal recovery does NOT touch cold" {
    const allocator = testing.allocator;

    //
    // Per TIERED_RECOVERY_DESIGN.md §1.3 and §3:
    //   Recovery loads: 1) Snapshot  2) Warm UAL segments  3) Cold manifest (metadata ONLY)
    //   Cold data is never fetched at startup.
    //
    // This test verifies the manifest is loaded as metadata-only at "startup"
    // and no actual cold data is downloaded during initialization.
    //

    var cold_tmp = testing.tmpDir(.{});
    defer cold_tmp.cleanup();
    var manifest_tmp = testing.tmpDir(.{});
    defer manifest_tmp.cleanup();

    const cold_dir = try cold_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(cold_dir);
    const manifest_dir = try manifest_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(manifest_dir);

    const file_backend = try FileBackend.init(allocator, .{
        .base_path = cold_dir,
        .create_dirs = true,
    });
    const cb = file_backend.asBackend();
    defer cb.deinitBackend();

    // "Previous session": archive a segment and persist manifest
    {
        var mgr = ColdTierManager.init(allocator, cb, .{
            .shard_id = 0,
        });
        defer mgr.deinit();

        var writer = SegmentWriter.init(allocator, 0, .none);
        defer writer.deinit();
        var i: u64 = 1;
        while (i <= 10) : (i += 1) {
            const entry = entry_mod.buildEntry(.kv_put, 0, 1, i, i * 1000, "data");
            try writer.addEntry(&entry);
        }
        const sealed = try writer.seal();
        defer allocator.free(sealed);
        try mgr.archiveSegmentData(sealed, "seg-1.flseg");
        try mgr.saveManifest(manifest_dir);
    }

    // "Startup": new manager loads manifest (metadata only — no cold fetches)
    {
        var mgr = ColdTierManager.init(allocator, cb, .{
            .shard_id = 0,
        });
        defer mgr.deinit();

        try mgr.loadManifest(manifest_dir);

        // Manifest is loaded — we know what's in cold storage
        try testing.expectEqual(@as(usize, 1), mgr.segmentCount());
        try testing.expect(mgr.isInCold(5));

        // But NO data was downloaded — stats prove it
        try testing.expectEqual(@as(u64, 0), mgr.segments_downloaded);
        try testing.expectEqual(@as(u64, 0), mgr.bytes_downloaded);

        // Only when a client explicitly requests historical data:
        const data = try mgr.downloadSegment(5);
        defer allocator.free(data);

        // NOW the download happened
        try testing.expectEqual(@as(u64, 1), mgr.segments_downloaded);
        try testing.expect(mgr.bytes_downloaded > 0);
    }
}

test "integration: cold tier — idempotent archival (re-scan does not duplicate)" {
    const allocator = testing.allocator;

    var warm_tmp = testing.tmpDir(.{});
    defer warm_tmp.cleanup();
    var cold_tmp = testing.tmpDir(.{});
    defer cold_tmp.cleanup();

    const warm_dir = try warm_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(warm_dir);
    const cold_dir = try cold_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(cold_dir);

    // Write one segment
    {
        var writer = SegmentWriter.init(allocator, 0, .none);
        defer writer.deinit();
        var i: u64 = 1;
        while (i <= 5) : (i += 1) {
            const entry = entry_mod.buildEntry(.kv_put, 0, 1, i, i * 1000, "data");
            try writer.addEntry(&entry);
        }
        try writer.writeToFile(warm_dir);
    }

    const file_backend = try FileBackend.init(allocator, .{
        .base_path = cold_dir,
        .create_dirs = true,
    });
    const cb = file_backend.asBackend();
    defer cb.deinitBackend();

    var manager = ColdTierManager.init(allocator, cb, .{});
    defer manager.deinit();

    // First scan: archives 1 segment
    const first = try manager.archiveWarmSegments(warm_dir);
    try testing.expectEqual(@as(u64, 1), first);

    // Second scan: nothing new to archive
    const second = try manager.archiveWarmSegments(warm_dir);
    try testing.expectEqual(@as(u64, 0), second);

    // Still only 1 segment in manifest
    try testing.expectEqual(@as(usize, 1), manager.segmentCount());
}

test "integration: cold tier — manifest persists across restarts" {
    const allocator = testing.allocator;

    var cold_tmp = testing.tmpDir(.{});
    defer cold_tmp.cleanup();
    var manifest_tmp = testing.tmpDir(.{});
    defer manifest_tmp.cleanup();

    const cold_dir = try cold_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(cold_dir);
    const manifest_dir = try manifest_tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(manifest_dir);

    const file_backend = try FileBackend.init(allocator, .{
        .base_path = cold_dir,
        .create_dirs = true,
    });
    const cb = file_backend.asBackend();
    defer cb.deinitBackend();

    // "Session 1": archive some data and persist manifest
    {
        var mgr = ColdTierManager.init(allocator, cb, .{
            .shard_id = 2,
        });
        defer mgr.deinit();

        // Create and archive a test segment
        var writer = SegmentWriter.init(allocator, 0, .none);
        defer writer.deinit();
        var i: u64 = 100;
        while (i <= 110) : (i += 1) {
            const entry = entry_mod.buildEntry(.kv_put, 0, 1, i, i * 1000, "session1-data");
            try writer.addEntry(&entry);
        }
        const sealed = try writer.seal();
        defer allocator.free(sealed);

        try mgr.archiveSegmentData(sealed, "seg-100.flseg");
        try testing.expectEqual(@as(usize, 1), mgr.segmentCount());
        try testing.expect(mgr.isInCold(105));

        try mgr.saveManifest(manifest_dir);
    }

    // "Session 2": new manager, load manifest, verify state
    {
        var mgr = ColdTierManager.init(allocator, cb, .{
            .shard_id = 2,
        });
        defer mgr.deinit();

        try mgr.loadManifest(manifest_dir);
        try testing.expectEqual(@as(usize, 1), mgr.segmentCount());
        try testing.expect(mgr.isInCold(100));
        try testing.expect(mgr.isInCold(110));
        try testing.expect(!mgr.isInCold(99));
        try testing.expect(!mgr.isInCold(111));

        // Download and verify the segment is intact
        const data = try mgr.downloadSegment(105);
        defer allocator.free(data);
        try testing.expect(data.len > segment_mod.HEADER_SIZE);

        const hdr: *const segment_mod.SegmentHeader = @ptrCast(@alignCast(data[0..segment_mod.HEADER_SIZE]));
        try testing.expectEqual(@as(u64, 100), hdr.first_index);
        try testing.expectEqual(@as(u64, 110), hdr.last_index);
    }
}
