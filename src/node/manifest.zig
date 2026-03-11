//! System topology manifest — prevents silent data corruption from shard count changes.
//!
//! On first boot, writes `{data_dir}/SYSTEM` with the shard count and partition count.
//! On subsequent boots, validates that the counts match. If not, errors with a
//! human-readable message explaining how to recover.
//!
//! File format: JSON for easy debugging and human inspection.
//!
//! ## Example SYSTEM file
//!
//! ```json
//! {
//!   "shards": 8,
//!   "partitions": 256,
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
    /// Number of partitions (affects key→shard routing)
    partitions: u32,
    /// Unix timestamp when data directory was first created
    created_at: i64,
    /// Flo version that created this manifest
    version: []const u8,

    const FILENAME = "SYSTEM";
    const CURRENT_VERSION = "1.0.0";

    /// Load manifest from data directory. Returns null if SYSTEM file not found.
    pub fn load(allocator: Allocator, data_path: []const u8) !?SystemManifest {
        const path = try std.fs.path.join(allocator, &.{ data_path, FILENAME });
        defer allocator.free(path);

        const file = std.fs.cwd().openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) return null;
            return err;
        };
        defer file.close();

        const content = try file.readToEndAlloc(allocator, 4096);
        defer allocator.free(content);

        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, content, .{});
        defer parsed.deinit();

        const root = parsed.value.object;

        const shards: u16 = if (root.get("shards")) |s|
            if (s == .integer) @intCast(s.integer) else return error.InvalidManifest
        else
            return error.InvalidManifest;

        const partitions: u32 = if (root.get("partitions")) |p|
            if (p == .integer) @intCast(p.integer) else 0
        else
            0;

        const created_at: i64 = if (root.get("created_at")) |c|
            if (c == .integer) c.integer else 0
        else
            0;

        return SystemManifest{
            .shards = shards,
            .partitions = partitions,
            .created_at = created_at,
            .version = CURRENT_VERSION,
        };
    }

    /// Create and write a new SYSTEM manifest to data_path.
    pub fn create(allocator: Allocator, data_path: []const u8, shards: u16, partitions: u32) !void {
        const path = try std.fs.path.join(allocator, &.{ data_path, FILENAME });
        defer allocator.free(path);

        // Build JSON manually for clean formatting
        var buf: std.ArrayListUnmanaged(u8) = .{};
        defer buf.deinit(allocator);

        const w = buf.writer(allocator);
        try w.writeAll("{\n");
        try w.print("  \"shards\": {d},\n", .{shards});
        try w.print("  \"partitions\": {d},\n", .{partitions});
        try w.print("  \"created_at\": {d},\n", .{std.time.timestamp()});
        try w.print("  \"version\": \"{s}\"\n", .{CURRENT_VERSION});
        try w.writeAll("}\n");

        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();
        try file.writeAll(buf.items);
    }

    /// Validate that the running configuration matches manifested topology.
    /// Returns TopologyMismatch on mismatch.
    pub fn validate(self: SystemManifest, requested_shards: u16, requested_partitions: u32) !void {
        if (self.shards != requested_shards) {
            log.err("topology mismatch: shard count changed existing={d} requested={d}", .{
                self.shards,
                requested_shards,
            });
            printMismatchError("shard", self.shards, requested_shards);
            return error.TopologyMismatch;
        }

        // Partition count 0 means "not recorded in manifest" (v1 compat) — skip check
        if (self.partitions > 0 and requested_partitions > 0 and self.partitions != requested_partitions) {
            log.err("topology mismatch: partition count changed existing={d} requested={d}", .{
                self.partitions,
                requested_partitions,
            });
            printMismatchError("partition", self.partitions, requested_partitions);
            return error.TopologyMismatch;
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
                "║  Fix: delete the data directory and start fresh.             ║\n" ++
                "║                                                              ║\n" ++
                "╚═══════════════════════════════════════════════════════════════╝\n" ++
                "\n",
            .{ existing, requested },
        );
    }
};

/// Ensure topology consistency. Call before spawning shard threads.
///
/// - If SYSTEM file exists: validate shard/partition counts match
/// - If no SYSTEM file: create data dir + SYSTEM manifest
pub fn ensureTopology(
    allocator: Allocator,
    data_path: []const u8,
    requested_shards: u16,
    requested_partitions: u32,
) !void {
    // Ensure data directory exists
    std.fs.cwd().makePath(data_path) catch |err| {
        if (err != error.PathAlreadyExists) {
            log.err("failed to create data directory: {s} err={any}", .{ data_path, err });
            return err;
        }
    };

    if (try SystemManifest.load(allocator, data_path)) |manifest| {
        try manifest.validate(requested_shards, requested_partitions);
        log.info("topology verified: shards={d} partitions={d} path={s}", .{
            manifest.shards,
            manifest.partitions,
            data_path,
        });
    } else {
        try SystemManifest.create(allocator, data_path, requested_shards, requested_partitions);
        log.info("topology manifest created: shards={d} partitions={d} path={s}", .{
            requested_shards,
            requested_partitions,
            data_path,
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "create and load manifest" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(path);

    try SystemManifest.create(allocator, path, 8, 256);

    const loaded = try SystemManifest.load(allocator, path);
    try std.testing.expect(loaded != null);
    try std.testing.expectEqual(@as(u16, 8), loaded.?.shards);
    try std.testing.expectEqual(@as(u32, 256), loaded.?.partitions);
}

test "validate matching topology" {
    const manifest = SystemManifest{
        .shards = 8,
        .partitions = 256,
        .created_at = 0,
        .version = "2.0.0",
    };

    // Should pass
    try manifest.validate(8, 256);
}

test "validate shard mismatch" {
    const manifest = SystemManifest{
        .shards = 8,
        .partitions = 256,
        .created_at = 0,
        .version = "2.0.0",
    };

    const result = manifest.validate(4, 256);
    try std.testing.expectError(error.TopologyMismatch, result);
}

test "validate partition mismatch" {
    const manifest = SystemManifest{
        .shards = 8,
        .partitions = 256,
        .created_at = 0,
        .version = "2.0.0",
    };

    const result = manifest.validate(8, 512);
    try std.testing.expectError(error.TopologyMismatch, result);
}

test "ensureTopology creates new manifest" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(path);

    try ensureTopology(allocator, path, 4, 128);

    // Should have created SYSTEM file
    const loaded = try SystemManifest.load(allocator, path);
    try std.testing.expect(loaded != null);
    try std.testing.expectEqual(@as(u16, 4), loaded.?.shards);
    try std.testing.expectEqual(@as(u32, 128), loaded.?.partitions);

    // Second call should succeed (same topology)
    try ensureTopology(allocator, path, 4, 128);

    // Different topology should fail
    const result = ensureTopology(allocator, path, 8, 128);
    try std.testing.expectError(error.TopologyMismatch, result);
}
