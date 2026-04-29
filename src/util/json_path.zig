//! Minimal JSONPath utility for KV JSON.GET / JSON.SET / JSON.DEL operations.
//!
//! Supported syntax (intentionally a strict subset of RedisJSON / JSONPath):
//!   $              — root
//!   $.field        — object field
//!   $.a.b.c        — nested object fields
//!   $.arr[0]       — array index (0-based)
//!   $.users[0].name — combined
//!
//! Not supported: wildcards, slices, recursive descent, filters.
//!
//! All functions take JSON document bytes and return either a borrowed slice
//! (zero-copy GET) or a freshly allocated JSON document (SET/DEL).

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const PathError = error{
    InvalidPath,
    PathNotFound,
    NotAnObject,
    NotAnArray,
    IndexOutOfBounds,
    OutOfMemory,
    InvalidJson,
};

/// A single segment of a parsed path.
const Segment = union(enum) {
    field: []const u8,
    index: usize,
};

/// Parse a path expression into segments. The returned slice is owned by the
/// caller and must be freed.
fn parsePath(allocator: Allocator, path: []const u8) PathError![]Segment {
    if (path.len == 0 or path[0] != '$') return error.InvalidPath;

    var segs: std.ArrayList(Segment) = .empty;
    errdefer segs.deinit(allocator);

    var i: usize = 1;
    while (i < path.len) {
        const c = path[i];
        if (c == '.') {
            // field follows
            i += 1;
            const start = i;
            while (i < path.len and path[i] != '.' and path[i] != '[') : (i += 1) {}
            if (i == start) return error.InvalidPath;
            try segs.append(allocator, .{ .field = path[start..i] });
        } else if (c == '[') {
            // index follows
            i += 1;
            const start = i;
            while (i < path.len and path[i] != ']') : (i += 1) {}
            if (i >= path.len or i == start) return error.InvalidPath;
            const idx = std.fmt.parseInt(usize, path[start..i], 10) catch return error.InvalidPath;
            try segs.append(allocator, .{ .index = idx });
            i += 1; // skip ']'
        } else {
            return error.InvalidPath;
        }
    }

    return try segs.toOwnedSlice(allocator);
}

/// Walk segments from `root`. Returns the value at the path or error.
fn walk(root: std.json.Value, segments: []const Segment) PathError!std.json.Value {
    var current = root;
    for (segments) |seg| {
        switch (seg) {
            .field => |name| {
                switch (current) {
                    .object => |obj| {
                        current = obj.get(name) orelse return error.PathNotFound;
                    },
                    else => return error.NotAnObject,
                }
            },
            .index => |idx| {
                switch (current) {
                    .array => |arr| {
                        if (idx >= arr.items.len) return error.IndexOutOfBounds;
                        current = arr.items[idx];
                    },
                    else => return error.NotAnArray,
                }
            },
        }
    }
    return current;
}

/// JSON.GET — extract a value at `path` from `json_bytes` and return it as
/// freshly allocated JSON text. Caller owns the returned slice.
pub fn jsonPathGet(allocator: Allocator, json_bytes: []const u8, path: []const u8) PathError![]u8 {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{}) catch
        return error.InvalidJson;
    defer parsed.deinit();

    const segments = try parsePath(allocator, path);
    defer allocator.free(segments);

    const value = try walk(parsed.value, segments);
    return std.json.Stringify.valueAlloc(allocator, value, .{}) catch return error.OutOfMemory;
}

/// JSON.SET — set the value at `path` to `value_json` (which must itself be
/// valid JSON text) and return the resulting document as freshly allocated
/// JSON text. If the path is "$" the entire document is replaced.
pub fn jsonPathSet(
    allocator: Allocator,
    json_bytes: []const u8,
    path: []const u8,
    value_json: []const u8,
) PathError![]u8 {
    // Validate replacement is parseable JSON.
    const new_parsed = std.json.parseFromSlice(std.json.Value, allocator, value_json, .{}) catch
        return error.InvalidJson;
    defer new_parsed.deinit();

    const segments = try parsePath(allocator, path);
    defer allocator.free(segments);

    if (segments.len == 0) {
        // Replace entire document.
        return std.json.Stringify.valueAlloc(allocator, new_parsed.value, .{}) catch return error.OutOfMemory;
    }

    var root_parsed = std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{}) catch
        return error.InvalidJson;
    defer root_parsed.deinit();

    // Walk to parent then mutate the leaf in-place. We must walk via pointer
    // so that mutations on the leaf object/array apply to the real document
    // rather than a stack copy.
    var parent_ptr: *std.json.Value = &root_parsed.value;
    for (segments[0 .. segments.len - 1]) |seg| {
        switch (seg) {
            .field => |name| switch (parent_ptr.*) {
                .object => |*obj| {
                    parent_ptr = obj.getPtr(name) orelse return error.PathNotFound;
                },
                else => return error.NotAnObject,
            },
            .index => |idx| switch (parent_ptr.*) {
                .array => |*arr| {
                    if (idx >= arr.items.len) return error.IndexOutOfBounds;
                    parent_ptr = &arr.items[idx];
                },
                else => return error.NotAnArray,
            },
        }
    }

    const leaf = segments[segments.len - 1];
    switch (leaf) {
        .field => |name| switch (parent_ptr.*) {
            .object => |*obj| {
                obj.put(allocator, name, new_parsed.value) catch return error.OutOfMemory;
            },
            else => return error.NotAnObject,
        },
        .index => |idx| switch (parent_ptr.*) {
            .array => |*arr| {
                if (idx >= arr.items.len) return error.IndexOutOfBounds;
                arr.items[idx] = new_parsed.value;
            },
            else => return error.NotAnArray,
        },
    }

    return std.json.Stringify.valueAlloc(allocator, root_parsed.value, .{}) catch return error.OutOfMemory;
}

/// JSON.DEL — remove the value at `path`. If path is "$" returns "null".
/// Returns the resulting document as freshly allocated JSON text.
pub fn jsonPathDel(
    allocator: Allocator,
    json_bytes: []const u8,
    path: []const u8,
) PathError![]u8 {
    const segments = try parsePath(allocator, path);
    defer allocator.free(segments);

    if (segments.len == 0) {
        return allocator.dupe(u8, "null") catch return error.OutOfMemory;
    }

    var root_parsed = std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{}) catch
        return error.InvalidJson;
    defer root_parsed.deinit();

    // Pointer-walk so mutations apply to the real document.
    var parent_ptr: *std.json.Value = &root_parsed.value;
    for (segments[0 .. segments.len - 1]) |seg| {
        switch (seg) {
            .field => |name| switch (parent_ptr.*) {
                .object => |*obj| {
                    parent_ptr = obj.getPtr(name) orelse return error.PathNotFound;
                },
                else => return error.NotAnObject,
            },
            .index => |idx| switch (parent_ptr.*) {
                .array => |*arr| {
                    if (idx >= arr.items.len) return error.IndexOutOfBounds;
                    parent_ptr = &arr.items[idx];
                },
                else => return error.NotAnArray,
            },
        }
    }

    const leaf = segments[segments.len - 1];
    switch (leaf) {
        .field => |name| switch (parent_ptr.*) {
            .object => |*obj| {
                if (!obj.swapRemove(name)) return error.PathNotFound;
            },
            else => return error.NotAnObject,
        },
        .index => |idx| switch (parent_ptr.*) {
            .array => |*arr| {
                if (idx >= arr.items.len) return error.IndexOutOfBounds;
                _ = arr.orderedRemove(idx);
            },
            else => return error.NotAnArray,
        },
    }

    return std.json.Stringify.valueAlloc(allocator, root_parsed.value, .{}) catch return error.OutOfMemory;
}

// ─── Tests ────────────────────────────────────────────────────────────────

test "parsePath: root" {
    const segs = try parsePath(std.testing.allocator, "$");
    defer std.testing.allocator.free(segs);
    try std.testing.expectEqual(@as(usize, 0), segs.len);
}

test "parsePath: nested field" {
    const segs = try parsePath(std.testing.allocator, "$.a.b.c");
    defer std.testing.allocator.free(segs);
    try std.testing.expectEqual(@as(usize, 3), segs.len);
    try std.testing.expectEqualStrings("a", segs[0].field);
    try std.testing.expectEqualStrings("b", segs[1].field);
    try std.testing.expectEqualStrings("c", segs[2].field);
}

test "parsePath: array index" {
    const segs = try parsePath(std.testing.allocator, "$.users[2].name");
    defer std.testing.allocator.free(segs);
    try std.testing.expectEqual(@as(usize, 3), segs.len);
    try std.testing.expectEqualStrings("users", segs[0].field);
    try std.testing.expectEqual(@as(usize, 2), segs[1].index);
    try std.testing.expectEqualStrings("name", segs[2].field);
}

test "jsonPathGet: simple field" {
    const result = try jsonPathGet(std.testing.allocator, "{\"name\":\"alice\",\"age\":30}", "$.name");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("\"alice\"", result);
}

test "jsonPathGet: nested" {
    const result = try jsonPathGet(std.testing.allocator, "{\"a\":{\"b\":{\"c\":42}}}", "$.a.b.c");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("42", result);
}

test "jsonPathGet: array index" {
    const result = try jsonPathGet(std.testing.allocator, "{\"xs\":[10,20,30]}", "$.xs[1]");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("20", result);
}

test "jsonPathGet: missing path" {
    const r = jsonPathGet(std.testing.allocator, "{\"a\":1}", "$.b");
    try std.testing.expectError(error.PathNotFound, r);
}

test "jsonPathSet: replace field" {
    const result = try jsonPathSet(std.testing.allocator, "{\"a\":1,\"b\":2}", "$.a", "99");
    defer std.testing.allocator.free(result);
    // Order-independent check
    try std.testing.expect(std.mem.indexOf(u8, result, "\"a\":99") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"b\":2") != null);
}

test "jsonPathSet: add nested field" {
    const result = try jsonPathSet(std.testing.allocator, "{\"obj\":{\"x\":1}}", "$.obj.y", "2");
    defer std.testing.allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"x\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"y\":2") != null);
}

test "jsonPathSet: replace root" {
    const result = try jsonPathSet(std.testing.allocator, "{\"a\":1}", "$", "{\"b\":2}");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("{\"b\":2}", result);
}

test "jsonPathDel: remove field" {
    const result = try jsonPathDel(std.testing.allocator, "{\"a\":1,\"b\":2}", "$.a");
    defer std.testing.allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"a\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"b\":2") != null);
}

test "jsonPathDel: remove array element" {
    const result = try jsonPathDel(std.testing.allocator, "{\"xs\":[10,20,30]}", "$.xs[1]");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("{\"xs\":[10,30]}", result);
}
