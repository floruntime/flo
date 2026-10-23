//! Dotted JSON paths ("$.a.b"), split into owned segments.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Empties = enum { keep, skip };

/// `expr`'s segments, each owned, after an optional "$." prefix. Nothing is
/// left allocated if memory runs out partway.
pub fn splitOwned(allocator: Allocator, expr: []const u8, empties: Empties) Allocator.Error![]const []const u8 {
    const path = if (std.mem.startsWith(u8, expr, "$.")) expr[2..] else expr;
    var segs: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer {
        for (segs.items) |seg| allocator.free(seg);
        segs.deinit(allocator);
    }
    var iter = std.mem.splitScalar(u8, path, '.');
    while (iter.next()) |seg| {
        if (seg.len == 0 and empties == .skip) continue;
        try segs.ensureUnusedCapacity(allocator, 1);
        segs.appendAssumeCapacity(try allocator.dupe(u8, seg));
    }
    return try segs.toOwnedSlice(allocator);
}

pub fn free(allocator: Allocator, segs: []const []const u8) void {
    for (segs) |seg| allocator.free(seg);
    allocator.free(segs);
}

test "splitOwned frees what it built when memory runs out" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(allocator: Allocator) !void {
            free(allocator, try splitOwned(allocator, "$.a..b.c", .keep));
            const skipped = try splitOwned(allocator, "$.a..b", .skip);
            defer free(allocator, skipped);
            try std.testing.expectEqual(@as(usize, 2), skipped.len);
        }
    }.run, .{});
}
