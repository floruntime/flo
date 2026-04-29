//! Shard-level MANIFEST — unified metadata for per-shard recovery.
//!
//! Combines snapshot pointer and cold storage index into a single
//! JSON file at `{shard_dir}/MANIFEST`. One source of truth per shard.
//!
//! ## Layout
//!
//! ```
//! 00000/
//! ├── MANIFEST              ← this file
//! ├── segs/
//! │   └── 0000000002.flseg
//! └── snaps/
//!     └── 0000001000-172xxx.fsnap
//! ```
//!
//! ## Format
//!
//! ```json
//! {
//!   "latest_snapshot": "0000001000-1234567890.fsnap",
//!   "cold_segments": [
//!     {
//!       "min_index": 1,
//!       "max_index": 100,
//!       "min_ts": 0,
//!       "max_ts": 0,
//!       "location": "00000/0000000001.flseg",
//!       "size": 4096,
//!       "crc": 12345678
//!     }
//!   ]
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

/// A UAL segment that has been archived to cold/object storage.
pub const ColdSegment = struct {
    min_index: u64,
    max_index: u64,
    min_ts: u64,
    max_ts: u64,
    location: []const u8,
    size: u64,
    crc: u32,
};

/// Per-shard manifest: latest snapshot pointer + cold storage index.
pub const ShardManifest = struct {
    /// Filename of the latest snapshot (e.g. "0000001000-1234567890.fsnap").
    latest_snapshot: ?[]const u8 = null,
    /// Cold storage entries — segments archived to S3/Azure/file.
    cold_segments: std.ArrayListUnmanaged(ColdSegment) = .empty,

    const FILENAME = "MANIFEST";

    pub fn deinit(self: *ShardManifest, allocator: Allocator) void {
        if (self.latest_snapshot) |s| allocator.free(s);
        for (self.cold_segments.items) |seg| {
            allocator.free(seg.location);
        }
        self.cold_segments.deinit(allocator);
    }

    // ─── Load ────────────────────────────────────────────────────────

    /// Load shard manifest from `{shard_dir}/MANIFEST`.
    /// Returns null if MANIFEST does not exist (fresh shard).
    pub fn load(allocator: Allocator, shard_dir: []const u8) !?ShardManifest {
        const path = try std.fs.path.join(allocator, &.{ shard_dir, FILENAME });
        defer allocator.free(path);

        const file = @import("stdx").fs.openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) return null;
            return err;
        };
        defer @import("stdx").fs.closeFile(file);

        const content = try @import("stdx").fs.readToEndAlloc(file, allocator, 1024 * 1024); // 1MB max
        defer allocator.free(content);

        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, content, .{});
        defer parsed.deinit();

        const root = parsed.value.object;

        var result = ShardManifest{};
        errdefer result.deinit(allocator);

        // latest_snapshot
        if (root.get("latest_snapshot")) |v| {
            if (v == .string) {
                result.latest_snapshot = try allocator.dupe(u8, v.string);
            }
        }

        // cold_segments
        if (root.get("cold_segments")) |arr| {
            if (arr == .array) {
                for (arr.array.items) |item| {
                    if (item != .object) continue;
                    const obj = item.object;
                    const entry = ColdSegment{
                        .min_index = extractU64(obj, "min_index") orelse continue,
                        .max_index = extractU64(obj, "max_index") orelse continue,
                        .min_ts = extractU64(obj, "min_ts") orelse 0,
                        .max_ts = extractU64(obj, "max_ts") orelse 0,
                        .location = blk: {
                            const loc = obj.get("location") orelse continue;
                            if (loc != .string) continue;
                            break :blk try allocator.dupe(u8, loc.string);
                        },
                        .size = extractU64(obj, "size") orelse 0,
                        .crc = @intCast(extractU64(obj, "crc") orelse 0),
                    };
                    try result.cold_segments.append(allocator, entry);
                }
            }
        }

        return result;
    }

    // ─── Save ────────────────────────────────────────────────────────

    /// Atomically write shard manifest to `{shard_dir}/MANIFEST`.
    pub fn save(self: *const ShardManifest, allocator: Allocator, shard_dir: []const u8) !void {
        var aw: std.Io.Writer.Allocating = .init(allocator);
        defer aw.deinit();

        const w = &aw.writer;
        try w.writeAll("{\n");

        // latest_snapshot
        if (self.latest_snapshot) |snap| {
            try w.print("  \"latest_snapshot\": \"{s}\"", .{snap});
        } else {
            try w.writeAll("  \"latest_snapshot\": null");
        }

        // cold_segments
        if (self.cold_segments.items.len > 0) {
            try w.writeAll(",\n  \"cold_segments\": [\n");
            for (self.cold_segments.items, 0..) |seg, i| {
                try w.writeAll("    {");
                try w.print("\"min_index\": {d}, ", .{seg.min_index});
                try w.print("\"max_index\": {d}, ", .{seg.max_index});
                try w.print("\"min_ts\": {d}, ", .{seg.min_ts});
                try w.print("\"max_ts\": {d}, ", .{seg.max_ts});
                try w.print("\"location\": \"{s}\", ", .{seg.location});
                try w.print("\"size\": {d}, ", .{seg.size});
                try w.print("\"crc\": {d}", .{seg.crc});
                try w.writeByte('}');
                if (i + 1 < self.cold_segments.items.len) try w.writeByte(',');
                try w.writeByte('\n');
            }
            try w.writeAll("  ]\n");
        } else {
            try w.writeAll(",\n  \"cold_segments\": []\n");
        }

        try w.writeAll("}\n");

        // Atomic write: .tmp → fsync → rename
        const manifest_path = try std.fs.path.join(allocator, &.{ shard_dir, FILENAME });
        defer allocator.free(manifest_path);

        const tmp_path = try std.fs.path.join(allocator, &.{ shard_dir, FILENAME ++ ".tmp" });
        defer allocator.free(tmp_path);

        const file = try @import("stdx").fs.createFile(tmp_path, .{});
        defer @import("stdx").fs.closeFile(file);
        try @import("stdx").fs.writeAll(file, aw.written());
        try @import("stdx").fs.sync(file);

        try @import("stdx").fs.rename(tmp_path, manifest_path);
    }

    // ─── Convenience: update only latest_snapshot ────────────────────

    /// Load manifest, update snapshot pointer, save. Thread-safe on a
    /// single shard thread (no locking needed).
    pub fn setLatestSnapshot(allocator: Allocator, shard_dir: []const u8, filename: []const u8) !void {
        var sm = (try load(allocator, shard_dir)) orelse ShardManifest{};
        defer sm.deinit(allocator);

        if (sm.latest_snapshot) |old| allocator.free(old);
        sm.latest_snapshot = try allocator.dupe(u8, filename);

        try sm.save(allocator, shard_dir);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn extractU64(obj: std.json.ObjectMap, key: []const u8) ?u64 {
    const v = obj.get(key) orelse return null;
    if (v != .integer) return null;
    return @intCast(v.integer);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "shard manifest: save and load round-trip" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realPathFileAlloc(@import("stdx").io.instance(), ".", allocator);
    defer allocator.free(path);

    // Save a manifest
    var sm = ShardManifest{};
    defer sm.deinit(allocator);

    sm.latest_snapshot = try allocator.dupe(u8, "0000001000-1234567890.fsnap");
    try sm.cold_segments.append(allocator, .{
        .min_index = 1,
        .max_index = 100,
        .min_ts = 5000,
        .max_ts = 9000,
        .location = try allocator.dupe(u8, "00000/0000000001.flseg"),
        .size = 4096,
        .crc = 12345678,
    });

    try sm.save(allocator, path);

    // Load it back
    var loaded = (try ShardManifest.load(allocator, path)).?;
    defer loaded.deinit(allocator);

    try std.testing.expectEqualStrings("0000001000-1234567890.fsnap", loaded.latest_snapshot.?);
    try std.testing.expectEqual(@as(usize, 1), loaded.cold_segments.items.len);

    const seg = loaded.cold_segments.items[0];
    try std.testing.expectEqual(@as(u64, 1), seg.min_index);
    try std.testing.expectEqual(@as(u64, 100), seg.max_index);
    try std.testing.expectEqual(@as(u64, 5000), seg.min_ts);
    try std.testing.expectEqual(@as(u64, 9000), seg.max_ts);
    try std.testing.expectEqualStrings("00000/0000000001.flseg", seg.location);
    try std.testing.expectEqual(@as(u64, 4096), seg.size);
    try std.testing.expectEqual(@as(u32, 12345678), seg.crc);
}

test "shard manifest: load returns null for missing file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realPathFileAlloc(@import("stdx").io.instance(), ".", allocator);
    defer allocator.free(path);

    const result = try ShardManifest.load(allocator, path);
    try std.testing.expect(result == null);
}

test "shard manifest: empty manifest round-trip" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realPathFileAlloc(@import("stdx").io.instance(), ".", allocator);
    defer allocator.free(path);

    const sm = ShardManifest{};
    try sm.save(allocator, path);

    var loaded = (try ShardManifest.load(allocator, path)).?;
    defer loaded.deinit(allocator);

    try std.testing.expect(loaded.latest_snapshot == null);
    try std.testing.expectEqual(@as(usize, 0), loaded.cold_segments.items.len);
}

test "shard manifest: setLatestSnapshot creates if missing" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realPathFileAlloc(@import("stdx").io.instance(), ".", allocator);
    defer allocator.free(path);

    // No MANIFEST exists yet — setLatestSnapshot creates one
    try ShardManifest.setLatestSnapshot(allocator, path, "0000000500-999.fsnap");

    var loaded = (try ShardManifest.load(allocator, path)).?;
    defer loaded.deinit(allocator);

    try std.testing.expectEqualStrings("0000000500-999.fsnap", loaded.latest_snapshot.?);
}

test "shard manifest: setLatestSnapshot preserves cold segments" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realPathFileAlloc(@import("stdx").io.instance(), ".", allocator);
    defer allocator.free(path);

    // First: save a manifest with cold segments
    var sm = ShardManifest{};
    defer sm.deinit(allocator);

    try sm.cold_segments.append(allocator, .{
        .min_index = 1,
        .max_index = 50,
        .min_ts = 0,
        .max_ts = 0,
        .location = try allocator.dupe(u8, "s3://bucket/seg"),
        .size = 2048,
        .crc = 42,
    });
    try sm.save(allocator, path);

    // Now update the snapshot pointer
    try ShardManifest.setLatestSnapshot(allocator, path, "0000000750-123.fsnap");

    // Cold segments should be preserved
    var loaded = (try ShardManifest.load(allocator, path)).?;
    defer loaded.deinit(allocator);

    try std.testing.expectEqualStrings("0000000750-123.fsnap", loaded.latest_snapshot.?);
    try std.testing.expectEqual(@as(usize, 1), loaded.cold_segments.items.len);
    try std.testing.expectEqualStrings("s3://bucket/seg", loaded.cold_segments.items[0].location);
}
