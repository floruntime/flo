//! Hashes whose values are stored, so every replica must compute the same
//! one forever. `std.hash` may change across Zig releases, which would
//! split replicas built with different versions; these are pinned by the
//! golden values below, and changing one is a data format change.

const std = @import("std");

/// FNV-1a, 64 bits.
pub fn fnv1a64(bytes: []const u8) u64 {
    var h: u64 = 0xcbf29ce484222325;
    for (bytes) |b| {
        h ^= b;
        h *%= 0x100000001b3;
    }
    return h;
}

test "fnv1a64: golden values" {
    try std.testing.expectEqual(@as(u64, 0xcbf29ce484222325), fnv1a64(""));
    try std.testing.expectEqual(@as(u64, 0xaf63dc4c8601ec8c), fnv1a64("a"));
    try std.testing.expectEqual(@as(u64, 0x85944171f73967e8), fnv1a64("foobar"));
}
