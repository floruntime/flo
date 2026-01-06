//! JSON Utility Functions for Workflow Service
//!
//! Pure helper functions for JSON value extraction and version comparison.

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

/// Extract a value from a JSON object at a dotted path (e.g., "input.name" or "$.field").
/// Returns an owned string (caller frees) or null if not found/not extractable.
pub fn extractJsonValue(allocator: Allocator, json_input: []const u8, from_path: []const u8) ?[]const u8 {
    const field_path = if (mem.startsWith(u8, from_path, "input."))
        from_path["input.".len..]
    else if (mem.startsWith(u8, from_path, "$."))
        from_path["$.".len..]
    else
        from_path;

    const parsed = std.json.parseFromSlice(std.json.Value, allocator, json_input, .{}) catch return null;
    defer parsed.deinit();

    var current = parsed.value;
    var remaining: []const u8 = field_path;

    while (remaining.len > 0) {
        const dot_pos = mem.indexOf(u8, remaining, ".");
        const segment = if (dot_pos) |pos| remaining[0..pos] else remaining;
        remaining = if (dot_pos) |pos| remaining[pos + 1 ..] else "";

        switch (current) {
            .object => |obj| {
                current = obj.get(segment) orelse return null;
            },
            else => return null,
        }
    }

    return switch (current) {
        .string => |s| allocator.dupe(u8, s) catch null,
        .number_string => |s| allocator.dupe(u8, s) catch null,
        .integer => |i| blk: {
            var buf: [24]u8 = undefined;
            const formatted = std.fmt.bufPrint(&buf, "{d}", .{i}) catch break :blk null;
            break :blk allocator.dupe(u8, formatted) catch null;
        },
        .float => |f| blk: {
            var buf: [64]u8 = undefined;
            const formatted = std.fmt.bufPrint(&buf, "{d}", .{f}) catch break :blk null;
            break :blk allocator.dupe(u8, formatted) catch null;
        },
        .bool => |b| allocator.dupe(u8, if (b) "true" else "false") catch null,
        .null => null,
        .array => null,
        .object => null,
    };
}

/// Compare two version strings. Returns true if `new` is newer than `current`.
/// Tries numeric comparison first, falls back to lexicographic.
pub fn isNewerVersion(new: []const u8, current: []const u8) bool {
    const new_num = std.fmt.parseInt(u64, new, 10) catch null;
    const cur_num = std.fmt.parseInt(u64, current, 10) catch null;
    if (new_num != null and cur_num != null) {
        return new_num.? > cur_num.?;
    }
    return std.mem.order(u8, new, current) == .gt;
}
