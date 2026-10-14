//! Writes parsed JSON values back out as operator output.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Stringify = std.json.Stringify;

/// Deepest nesting written; below Stringify's own checked depth.
pub const MAX_DEPTH: usize = 128;

pub const Error = Stringify.Error || error{NestingTooDeep};

/// `value` as JSON: strings and keys escaped, floats in decimal, a
/// non-finite float as null. Nested deeper than `MAX_DEPTH` is refused.
pub fn writeValue(jw: *Stringify, value: std.json.Value) Error!void {
    return writeAt(jw, value, 0);
}

fn writeAt(jw: *Stringify, value: std.json.Value, depth: usize) Error!void {
    switch (value) {
        .null => try jw.write(null),
        .bool => |b| try jw.write(b),
        .integer => |i| try jw.write(i),
        .float => |f| if (std.math.isFinite(f)) try jw.print("{d}", .{f}) else try jw.write(null),
        // The parser kept the number's own text.
        .number_string => |s| try jw.print("{s}", .{s}),
        .string => |s| try jw.write(s),
        .array => |arr| {
            if (depth >= MAX_DEPTH) return error.NestingTooDeep;
            try jw.beginArray();
            for (arr.items) |item| try writeAt(jw, item, depth + 1);
            try jw.endArray();
        },
        .object => |obj| {
            if (depth >= MAX_DEPTH) return error.NestingTooDeep;
            try jw.beginObject();
            var it = obj.iterator();
            while (it.next()) |entry| {
                try jw.objectField(entry.key_ptr.*);
                try writeAt(jw, entry.value_ptr.*, depth + 1);
            }
            try jw.endObject();
        },
    }
}

/// `value` as an allocated JSON text.
pub fn valueAlloc(allocator: Allocator, value: std.json.Value) error{ OutOfMemory, NestingTooDeep }![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    var jw: Stringify = .{ .writer = &out.writer };
    writeValue(&jw, value) catch |err| return switch (err) {
        error.NestingTooDeep => error.NestingTooDeep,
        error.WriteFailed => error.OutOfMemory,
    };
    return out.toOwnedSlice();
}

test "json_out: strings and keys are escaped, depth is bounded" {
    const allocator = std.testing.allocator;
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{"a\"b":"c\\d\"e","n":[1,2.5,null,true]}
    , .{});
    defer parsed.deinit();
    const text = try valueAlloc(allocator, parsed.value);
    defer allocator.free(text);
    try std.testing.expectEqualStrings(
        \\{"a\"b":"c\\d\"e","n":[1,2.5,null,true]}
    , text);

    var deep: std.ArrayList(u8) = .empty;
    defer deep.deinit(allocator);
    try deep.appendNTimes(allocator, '[', MAX_DEPTH + 1);
    try deep.appendNTimes(allocator, ']', MAX_DEPTH + 1);
    const p2 = try std.json.parseFromSlice(std.json.Value, allocator, deep.items, .{});
    defer p2.deinit();
    try std.testing.expectError(error.NestingTooDeep, valueAlloc(allocator, p2.value));
}
