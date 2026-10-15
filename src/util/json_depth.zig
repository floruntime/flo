//! JSON nesting depth, measured on the text without parsing it.
//!
//! JSON the server parses into a tree and writes back out is held to
//! `MAX_DEPTH` levels: its writer holds 256 and its tree walkers take one
//! call per level.

const std = @import("std");

/// Most `[]`/`{}` levels a JSON document may nest.
pub const MAX_DEPTH: usize = 128;

pub const TOO_DEEP_MESSAGE = std.fmt.comptimePrint("JSON nesting is limited to {d} levels", .{MAX_DEPTH});

/// The deepest `[]`/`{}` nesting in `text`, brackets inside strings not
/// counted. Stops counting at `MAX_DEPTH + 1`. Says nothing about whether
/// `text` is valid JSON.
pub fn depth(text: []const u8) usize {
    var level: usize = 0;
    var deepest: usize = 0;
    var in_string = false;
    var escaped = false;
    for (text) |c| {
        if (in_string) {
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                in_string = false;
            }
            continue;
        }
        switch (c) {
            '"' => in_string = true,
            '[', '{' => {
                level += 1;
                if (level > deepest) {
                    deepest = level;
                    if (deepest > MAX_DEPTH) return deepest;
                }
            },
            ']', '}' => level -|= 1,
            else => {},
        }
    }
    return deepest;
}

/// Whether `text` nests no deeper than `MAX_DEPTH`.
pub fn within(text: []const u8) bool {
    return depth(text) <= MAX_DEPTH;
}

/// `levels` arrays nested in one another: `[[...[]...]]`. Test helper.
pub fn nestedArrays(allocator: std.mem.Allocator, levels: usize) ![]u8 {
    const out = try allocator.alloc(u8, levels * 2);
    @memset(out[0..levels], '[');
    @memset(out[levels..], ']');
    return out;
}

test "depth counts brackets outside strings" {
    try std.testing.expectEqual(@as(usize, 0), depth("42"));
    try std.testing.expectEqual(@as(usize, 1), depth("{\"a\":1}"));
    try std.testing.expectEqual(@as(usize, 2), depth("{\"a\":[1,2]}"));
    try std.testing.expectEqual(@as(usize, 1), depth("{\"a\":\"[[[{{{\"}"));
    try std.testing.expectEqual(@as(usize, 1), depth("{\"a\\\"[[\":\"\\\\\"}"));
    try std.testing.expectEqual(@as(usize, 3), depth("[[],[],[[]]]"));
}

test "within holds 128 levels and refuses 129 and 300" {
    const a = std.testing.allocator;
    for ([_]usize{ 128, 129, 300 }) |n| {
        const text = try nestedArrays(a, n);
        defer a.free(text);
        try std.testing.expectEqual(n <= MAX_DEPTH, within(text));
    }
}
