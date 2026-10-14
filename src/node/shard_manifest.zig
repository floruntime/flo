//! Shard-level MANIFEST: the snapshot a shard recovers from, as a JSON
//! file at `{shard_dir}/MANIFEST`.
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
//!   "latest_snapshot": "0000001000-1234567890.fsnap"
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

/// Per-shard manifest: the latest snapshot pointer.
pub const ShardManifest = struct {
    /// Filename of the latest snapshot (e.g. "0000001000-1234567890.fsnap").
    latest_snapshot: ?[]const u8 = null,

    const FILENAME = "MANIFEST";

    pub fn deinit(self: *ShardManifest, allocator: Allocator) void {
        if (self.latest_snapshot) |s| allocator.free(s);
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

        if (parsed.value != .object) return error.InvalidManifest;
        const root = parsed.value.object;

        var result = ShardManifest{};
        errdefer result.deinit(allocator);

        // latest_snapshot
        if (root.get("latest_snapshot")) |v| {
            if (v == .string) {
                result.latest_snapshot = try allocator.dupe(u8, v.string);
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
        try w.writeByte('\n');

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

        try @import("stdx").fs.renameDurable(tmp_path, manifest_path);
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

    const before = @import("stdx").fs.dir_syncs.load(.monotonic);
    try sm.save(allocator, path);
    try std.testing.expectEqual(before + 1, @import("stdx").fs.dir_syncs.load(.monotonic));

    // Load it back
    var loaded = (try ShardManifest.load(allocator, path)).?;
    defer loaded.deinit(allocator);

    try std.testing.expectEqualStrings("0000001000-1234567890.fsnap", loaded.latest_snapshot.?);
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
