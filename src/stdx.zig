const std = @import("std");

/// Standard library extensions for Flo.
/// Common utilities and helpers.
/// Structured logging with format-string API
/// Usage: stdx.log.info("Server starting on port {d}", .{9000});
pub const log = @import("log.zig");

pub const testing = @import("testing/e2e/mod.zig");
/// Copy memory from source to destination.
/// Asserts that the slices do not overlap.
pub fn copy_disjoint(
    comptime mode: enum { exact, inexact },
    comptime T: type,
    dest: []T,
    source: []const T,
) void {
    switch (mode) {
        .exact => {
            std.debug.assert(dest.len == source.len);
            @memcpy(dest, source);
        },
        .inexact => {
            const len = @min(dest.len, source.len);
            @memcpy(dest[0..len], source[0..len]);
        },
    }
}

/// Check if a struct has no padding.
pub fn no_padding(comptime T: type) bool {
    return @sizeOf(T) == @bitSizeOf(T) / 8;
}

/// Maybe assertion - only asserts in debug mode.
pub fn maybe(condition: bool) void {
    if (std.debug.runtime_safety) {
        std.debug.assert(condition);
    }
}

/// Convert optional slice to null if empty, otherwise return the slice.
/// Useful for optional string parameters.
pub fn nullIfEmpty(comptime T: type, slice: ?[]const T) ?[]const T {
    if (slice) |s| {
        if (s.len > 0) return s;
    }
    return null;
}

/// Convert optional numeric value to null if zero, otherwise return the value.
/// Useful for flag values where 0 means "auto/use default".
pub fn nullIfZero(comptime T: type, value: ?T) ?T {
    if (value) |v| {
        if (v != 0) return v;
    }
    return null;
}

/// Cast optional numeric value to target type if non-zero, otherwise null.
/// Useful for flag values where 0 means "auto/use default".
pub fn castIfNonZero(comptime From: type, comptime To: type, value: ?From) ?To {
    if (value) |v| {
        if (v != 0) return @intCast(v);
    }
    return null;
}

test "copy_disjoint: exact" {
    var dest = [_]u8{0} ** 4;
    const source = [_]u8{ 1, 2, 3, 4 };
    copy_disjoint(.exact, u8, &dest, &source);
    try std.testing.expectEqualSlices(u8, &source, &dest);
}

test "copy_disjoint: inexact" {
    var dest = [_]u8{0} ** 4;
    const source = [_]u8{ 1, 2 };
    copy_disjoint(.inexact, u8, &dest, &source);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 0, 0 }, &dest);
}

test "no_padding: struct without padding" {
    const NoPadding = struct {
        a: u32,
        b: u32,
    };
    try std.testing.expect(no_padding(NoPadding));
}

test "no_padding: struct with padding" {
    const WithPadding = struct {
        a: u8,
        // 3 bytes padding here
        b: u32,
    };
    try std.testing.expect(!no_padding(WithPadding));
}

test "nullIfEmpty: non-empty string" {
    const str = "hello";
    const result = nullIfEmpty(u8, str);
    try std.testing.expectEqualStrings("hello", result.?);
}

test "nullIfEmpty: empty string" {
    const str = "";
    const result = nullIfEmpty(u8, str);
    try std.testing.expect(result == null);
}

test "nullIfEmpty: null input" {
    const result: ?[]const u8 = nullIfEmpty(u8, null);
    try std.testing.expect(result == null);
}

test "nullIfEmpty: slice with values" {
    const slice: []const u8 = &[_]u8{ 1, 2, 3 };
    const result = nullIfEmpty(u8, slice);
    try std.testing.expectEqualSlices(u8, slice, result.?);
}

test "castIfNonZero: non-zero value" {
    const value: u32 = 9000;
    const result = castIfNonZero(u32, u16, value);
    try std.testing.expectEqual(@as(u16, 9000), result.?);
}

test "castIfNonZero: zero value" {
    const value: u32 = 0;
    const result = castIfNonZero(u32, u16, value);
    try std.testing.expect(result == null);
}

test "castIfNonZero: null input" {
    const result: ?u16 = castIfNonZero(u32, u16, null);
    try std.testing.expect(result == null);
}

test "nullIfZero: non-zero value" {
    const value: ?u16 = 9000;
    const result = nullIfZero(u16, value);
    try std.testing.expectEqual(@as(u16, 9000), result.?);
}

test "nullIfZero: zero value" {
    const value: ?u16 = 0;
    const result = nullIfZero(u16, value);
    try std.testing.expect(result == null);
}

test "nullIfZero: null input" {
    const result: ?u16 = nullIfZero(u16, null);
    try std.testing.expect(result == null);
}

test {
    _ = log;
}
