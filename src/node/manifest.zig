//! System topology manifest — prevents silent data corruption from shard count changes.
//!
//! On first boot, writes `{data_dir}/SYSTEM` with the shard count. On
//! subsequent boots, a start asking for another count is refused, naming both.
//! The key partition count isn't recorded: it is fixed for every node
//! (`router.SLOTS`).
//!
//! File format: JSON for easy debugging and human inspection.
//!
//! ## Example SYSTEM file
//!
//! ```json
//! {
//!   "shards": 8,
//!   "created_at": 1772033477,
//!   "version": "2.0.0"
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;

pub const SystemManifest = struct {
    /// Number of shards this data was written with
    shards: u16,
    /// Unix timestamp when data directory was first created
    created_at: i64,
    /// Flo version that created this manifest
    version: []const u8,

    const FILENAME = @import("data_format.zig").SYSTEM_FILENAME;
    const CURRENT_VERSION = "1.0.0";

    /// Load manifest from data directory. Returns null if SYSTEM file not found.
    pub fn load(allocator: Allocator, data_path: []const u8) !?SystemManifest {
        const path = try std.fs.path.join(allocator, &.{ data_path, FILENAME });
        defer allocator.free(path);

        const file = @import("stdx").fs.openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) return null;
            return err;
        };
        defer @import("stdx").fs.closeFile(file);

        const content = try @import("stdx").fs.readToEndAlloc(file, allocator, 4096);
        defer allocator.free(content);
        return try parse(allocator, content);
    }

    /// A SYSTEM file's content, or `error.InvalidManifest` for anything
    /// that isn't one.
    pub fn parse(allocator: Allocator, content: []const u8) !SystemManifest {
        const parsed = std.json.parseFromSlice(std.json.Value, allocator, content, .{}) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidManifest,
        };
        defer parsed.deinit();

        // A hand-edited file is refused, never cast into a wrong topology.
        const root = if (parsed.value == .object) parsed.value.object else return error.InvalidManifest;

        const shards: u16 = if (root.get("shards")) |s|
            if (s == .integer) std.math.cast(u16, s.integer) orelse return error.InvalidManifest else return error.InvalidManifest
        else
            return error.InvalidManifest;

        const created_at: i64 = if (root.get("created_at")) |c|
            if (c == .integer) c.integer else 0
        else
            0;

        return SystemManifest{
            .shards = shards,
            .created_at = created_at,
            .version = CURRENT_VERSION,
        };
    }

    /// Create and write a new SYSTEM manifest to data_path.
    pub fn create(allocator: Allocator, data_path: []const u8, shards: u16) !void {
        const path = try std.fs.path.join(allocator, &.{ data_path, FILENAME });
        defer allocator.free(path);

        // Build JSON manually for clean formatting
        var aw: std.Io.Writer.Allocating = .init(allocator);
        defer aw.deinit();

        const w = &aw.writer;
        try w.writeAll("{\n");
        try w.print("  \"shards\": {d},\n", .{shards});
        try w.print("  \"created_at\": {d},\n", .{@import("stdx").time.milliTimestamp()});
        try w.print("  \"version\": \"{s}\"\n", .{CURRENT_VERSION});
        try w.writeAll("}\n");

        // A crash mid-write must not leave a torn SYSTEM file, which every
        // later boot would refuse.
        const tmp_path = try std.fs.path.join(allocator, &.{ data_path, FILENAME ++ ".tmp" });
        defer allocator.free(tmp_path);
        {
            const file = try @import("stdx").fs.createFile(tmp_path, .{});
            defer @import("stdx").fs.closeFile(file);
            try @import("stdx").fs.writeAll(file, aw.written());
            try @import("stdx").fs.sync(file);
        }
        try @import("stdx").fs.renameDurable(tmp_path, path);
    }

    /// Validate that the running configuration matches manifested topology.
    pub fn validate(self: SystemManifest, requested_shards: u16) !void {
        if (self.shards != requested_shards) {
            log.err("topology mismatch: the data was written with {d} shards, and this start asks for {d}", .{
                self.shards,
                requested_shards,
            });
            printMismatchError("shard", self.shards, requested_shards);
            return error.ShardCountChanged;
        }
    }

    fn printMismatchError(comptime what: []const u8, existing: anytype, requested: anytype) void {
        // Suppress visual banner in test builds — the structured log.err above is sufficient
        if (@import("builtin").is_test) return;

        std.debug.print(
            "\n" ++
                "╔═══════════════════════════════════════════════════════════════╗\n" ++
                "║                   TOPOLOGY MISMATCH ERROR                    ║\n" ++
                "╠═══════════════════════════════════════════════════════════════╣\n" ++
                "║                                                              ║\n" ++
                "║  Data was written with " ++ what ++ " count {d},             ║\n" ++
                "║  but you are starting with {d}.                              ║\n" ++
                "║                                                              ║\n" ++
                "║  Changing " ++ what ++ " count silently corrupts routing.    ║\n" ++
                "║                                                              ║\n" ++
                "║  Fix: start with the count the data was written with, or     ║\n" ++
                "║  move the data directory aside and start with an empty one.  ║\n" ++
                "║                                                              ║\n" ++
                "╚═══════════════════════════════════════════════════════════════╝\n" ++
                "\n",
            .{ existing, requested },
        );
    }
};

/// Ensure topology consistency. Call before spawning shard threads.
///
/// - If SYSTEM file exists: validate the shard count matches
/// - If no SYSTEM file: create data dir + SYSTEM manifest
pub fn ensureTopology(
    allocator: Allocator,
    data_path: []const u8,
    requested_shards: u16,
) !void {
    // Ensure data directory exists
    @import("stdx").fs.makePathDurable(data_path) catch |err| {
        if (err != error.PathAlreadyExists) {
            log.err("cannot create or sync data directory {s}: {s}", .{ data_path, @errorName(err) });
            return err;
        }
    };

    if (try SystemManifest.load(allocator, data_path)) |manifest| {
        try manifest.validate(requested_shards);
        log.info("topology verified: shards={d} path={s}", .{ manifest.shards, data_path });
    } else {
        try SystemManifest.create(allocator, data_path, requested_shards);
        log.info("topology manifest created: shards={d} path={s}", .{ requested_shards, data_path });
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "create and load manifest" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realPathFileAlloc(@import("stdx").io.instance(), ".", allocator);
    defer allocator.free(path);

    const before = @import("stdx").fs.dir_syncs.load(.monotonic);
    try SystemManifest.create(allocator, path, 8);
    try std.testing.expectEqual(before + 1, @import("stdx").fs.dir_syncs.load(.monotonic));

    const loaded = try SystemManifest.load(allocator, path);
    try std.testing.expect(loaded != null);
    try std.testing.expectEqual(@as(u16, 8), loaded.?.shards);
}

test "a manifest with a count out of range, or not an object, is refused" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(@import("stdx").io.instance(), ".", allocator);
    defer allocator.free(path);
    const file_path = try std.fs.path.join(allocator, &.{ path, SystemManifest.FILENAME });
    defer allocator.free(file_path);
    for ([_][]const u8{ "{\"shards\": 70000}", "{\"shards\": -1}", "[4]" }) |body| {
        const f = try @import("stdx").fs.createFileAbsolute(file_path, .{ .truncate = true });
        try @import("stdx").fs.writeAll(f, body);
        @import("stdx").fs.closeFile(f);
        try std.testing.expectError(error.InvalidManifest, SystemManifest.load(allocator, path));
    }
}

test "a start with another shard count is refused, naming both" {
    const manifest = SystemManifest{ .shards = 8, .created_at = 0, .version = "2.0.0" };
    try manifest.validate(8);
    try std.testing.expectError(error.ShardCountChanged, manifest.validate(4));
}

test "ensureTopology creates new manifest" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realPathFileAlloc(@import("stdx").io.instance(), ".", allocator);
    defer allocator.free(path);

    try ensureTopology(allocator, path, 4);

    // Should have created SYSTEM file
    const loaded = try SystemManifest.load(allocator, path);
    try std.testing.expect(loaded != null);
    try std.testing.expectEqual(@as(u16, 4), loaded.?.shards);

    // Second call should succeed (same topology)
    try ensureTopology(allocator, path, 4);

    // Different topology should fail
    const result = ensureTopology(allocator, path, 8);
    try std.testing.expectError(error.ShardCountChanged, result);
}

test "ensureTopology syncs a data directory it creates and its SYSTEM file, and nothing on a restart" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(@import("stdx").io.instance(), ".", allocator);
    defer allocator.free(root);
    const data = try std.fs.path.join(allocator, &.{ root, "data" });
    defer allocator.free(data);

    const syncs = &@import("stdx").fs.dir_syncs;
    const before = syncs.load(.monotonic);
    try ensureTopology(allocator, data, 4);
    try std.testing.expectEqual(before + 2, syncs.load(.monotonic));
    try ensureTopology(allocator, data, 4);
    try std.testing.expectEqual(before + 2, syncs.load(.monotonic));
}
