/// Core testing utilities for Flo tests
/// Provides common helpers for test setup, cleanup, and utilities
const std = @import("std");

// Export MockRaftProposer for tests that need Raft integration
pub const MockRaftProposer = @import("mock_raft_proposer.zig").MockRaftProposer;

/// Helper to create a unique temporary database path
/// This prevents test pollution by ensuring each test uses its own isolated database
pub fn getTempDbPath(tmp_dir: *std.testing.TmpDir, allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
    const real_path = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(real_path);
    return try std.fmt.allocPrint(allocator, "{s}/{s}.db", .{ real_path, name });
}

/// Helper to create a temporary directory for tests
/// Returns a TmpDir that should be cleaned up with defer tmp.cleanup()
pub fn createTmpDir() std.testing.TmpDir {
    return std.testing.tmpDir(.{});
}

test "getTempDbPath creates unique paths" {
    const allocator = std.testing.allocator;

    var tmp_dir = createTmpDir();
    defer tmp_dir.cleanup();

    const path1 = try getTempDbPath(&tmp_dir, allocator, "test1");
    defer allocator.free(path1);

    const path2 = try getTempDbPath(&tmp_dir, allocator, "test2");
    defer allocator.free(path2);

    // Paths should be different
    try std.testing.expect(!std.mem.eql(u8, path1, path2));

    // Paths should contain the names
    try std.testing.expect(std.mem.indexOf(u8, path1, "test1.db") != null);
    try std.testing.expect(std.mem.indexOf(u8, path2, "test2.db") != null);
}
