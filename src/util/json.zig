//! JSON Utility Functions
//!
//! Shared JSON serialization helpers for CLI output, dashboard API,
//! and HTTP handler's CommandResult serializer.
//!
//! ## Usage Patterns
//!
//! ### Struct Serialization (use std.json.fmt wrapper)
//! ```zig
//! const json = @import("util/json.zig");
//!
//! // Pretty print to CLI context
//! json.printPretty(ctx, allocator, my_struct);
//!
//! // Serialize to string
//! const str = try json.stringifyPretty(allocator, my_struct);
//! defer allocator.free(str);
//! ```
//!
//! ### HashMap/Dynamic Keys (use builders)
//! ```zig
//! var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
//! try obj.begin();
//! try obj.writeHashMap(string_hashmap);  // Writes all k/v pairs
//! try obj.end();
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Write a JSON-escaped string (without surrounding quotes)
/// Internal helper - use writeString or ObjectBuilder.stringField instead
fn writeEscapedString(writer: anytype, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => {
                if (c < 0x20) {
                    try writer.print("\\u{x:0>4}", .{c});
                } else {
                    try writer.writeByte(c);
                }
            },
        }
    }
}

/// Write a quoted JSON string
pub fn writeString(writer: anytype, s: []const u8) !void {
    try writer.writeByte('"');
    try writeEscapedString(writer, s);
    try writer.writeByte('"');
}

/// Write value as JSON string if valid UTF-8, otherwise as base64 object
pub fn writeValueOrBase64(writer: anytype, value: []const u8) !void {
    if (std.unicode.utf8ValidateSlice(value)) {
        try writeString(writer, value);
    } else {
        // Base64 encode binary data
        try writer.writeAll("{\"_binary\":\"");
        const encoder = std.base64.standard.Encoder;
        const encoded_len = encoder.calcSize(value.len);
        var encoded: [4096]u8 = undefined;
        if (encoded_len <= encoded.len) {
            const actual = encoder.encode(&encoded, value);
            try writer.writeAll(actual);
        } else {
            try writer.writeAll("<too large>");
        }
        try writer.writeAll("\"}");
    }
}

// =============================================================================
// JSON Array Builder
// =============================================================================

/// Helper for building JSON arrays incrementally
pub fn ArrayBuilder(comptime WriterType: type) type {
    return struct {
        const Self = @This();
        writer: WriterType,
        first: bool = true,

        pub fn init(writer: WriterType) Self {
            return .{ .writer = writer };
        }

        pub fn begin(self: *Self) !void {
            try self.writer.writeByte('[');
            self.first = true;
        }

        pub fn end(self: *Self) !void {
            try self.writer.writeByte(']');
        }

        pub fn next(self: *Self) !void {
            if (!self.first) {
                try self.writer.writeByte(',');
            }
            self.first = false;
        }

        /// Write an array of strings
        pub fn writeStringSlice(self: *Self, items: []const []const u8) !void {
            for (items) |item| {
                try self.next();
                try writeString(self.writer, item);
            }
        }

        /// Write an array of integers
        pub fn writeIntSlice(self: *Self, items: anytype) !void {
            for (items) |item| {
                try self.next();
                try self.writer.print("{d}", .{item});
            }
        }
    };
}

// =============================================================================
// JSON Object Builder
// =============================================================================

/// Helper for building JSON objects incrementally
pub fn ObjectBuilder(comptime WriterType: type) type {
    return struct {
        const Self = @This();
        writer: WriterType,
        first: bool = true,

        pub fn init(writer: WriterType) Self {
            return .{ .writer = writer };
        }

        pub fn begin(self: *Self) !void {
            try self.writer.writeByte('{');
            self.first = true;
        }

        pub fn end(self: *Self) !void {
            try self.writer.writeByte('}');
        }

        pub fn next(self: *Self) !void {
            if (!self.first) {
                try self.writer.writeByte(',');
            }
            self.first = false;
        }

        pub fn field(self: *Self, key: []const u8) !void {
            try self.next();
            try writeString(self.writer, key);
            try self.writer.writeByte(':');
        }

        pub fn stringField(self: *Self, key: []const u8, value: []const u8) !void {
            try self.field(key);
            try writeString(self.writer, value);
        }

        pub fn intField(self: *Self, key: []const u8, value: anytype) !void {
            try self.field(key);
            try self.writer.print("{d}", .{value});
        }

        pub fn floatField(self: *Self, key: []const u8, value: f64) !void {
            try self.field(key);
            if (std.math.isNan(value) or std.math.isInf(value)) {
                try self.writer.writeAll("null");
            } else {
                try self.writer.print("{d}", .{value});
            }
        }

        pub fn boolField(self: *Self, key: []const u8, value: bool) !void {
            try self.field(key);
            try self.writer.writeAll(if (value) "true" else "false");
        }

        pub fn nullField(self: *Self, key: []const u8) !void {
            try self.field(key);
            try self.writer.writeAll("null");
        }

        /// Write all entries from a HashMap with string keys and string values
        pub fn writeStringHashMap(self: *Self, map: anytype) !void {
            var iter = map.iterator();
            while (iter.next()) |entry| {
                try self.stringField(entry.key_ptr.*, entry.value_ptr.*);
            }
        }

        /// Write all entries from a HashMap with string keys and integer values
        pub fn writeIntHashMap(self: *Self, map: anytype) !void {
            var iter = map.iterator();
            while (iter.next()) |entry| {
                try self.intField(entry.key_ptr.*, entry.value_ptr.*);
            }
        }

        /// Write a nested object field - returns a new ObjectBuilder for the nested object
        /// Caller must call begin() and end() on the returned builder
        pub fn objectField(self: *Self, key: []const u8) !Self {
            try self.field(key);
            return Self.init(self.writer);
        }

        /// Write a nested array field - returns an ArrayBuilder for the nested array
        /// Caller must call begin() and end() on the returned builder
        pub fn arrayField(self: *Self, key: []const u8) !ArrayBuilder(WriterType) {
            try self.field(key);
            return ArrayBuilder(WriterType).init(self.writer);
        }
    };
}

// =============================================================================
// High-Level JSON Serialization (wraps std.json.fmt)
// =============================================================================

/// Print any value as formatted JSON with options
/// Usage: printJson(ctx, allocator, my_struct, .{});
/// Or: printJson(ctx, allocator, data, .{ .whitespace = .indent_2 });
pub fn printJson(ctx: anytype, allocator: Allocator, value: anytype, comptime options: std.json.Stringify.Options) void {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    aw.writer.print("{f}", .{std.json.fmt(value, options)}) catch |err| {
        ctx.print("Error serializing JSON: {}\n", .{err});
        return;
    };

    ctx.print("{s}\n", .{aw.written()});
}

/// Print any value as compact JSON (no whitespace)
pub fn printCompact(ctx: anytype, allocator: Allocator, value: anytype) void {
    printJson(ctx, allocator, value, .{});
}

/// Print any value as pretty JSON with 2-space indentation
pub fn printPretty(ctx: anytype, allocator: Allocator, value: anytype) void {
    printJson(ctx, allocator, value, .{ .whitespace = .indent_2 });
}

/// Serialize any value to a JSON string (caller owns returned memory)
pub fn stringify(allocator: Allocator, value: anytype, comptime options: std.json.Stringify.Options) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer aw.deinit();

    try aw.writer.print("{f}", .{std.json.fmt(value, options)});

    return aw.toOwnedSlice();
}

/// Serialize to compact JSON string
pub fn stringifyCompact(allocator: Allocator, value: anytype) ![]u8 {
    return stringify(allocator, value, .{});
}

/// Serialize to pretty JSON string
pub fn stringifyPretty(allocator: Allocator, value: anytype) ![]u8 {
    return stringify(allocator, value, .{ .whitespace = .indent_2 });
}

/// Write a struct or value directly to a writer using std.json.fmt
pub fn writeValue(writer: anytype, value: anytype, comptime options: std.json.Stringify.Options) !void {
    try writer.print("{f}", .{std.json.fmt(value, options)});
}

// =============================================================================
// Tests
// =============================================================================

test "writeString adds quotes and escapes" {
    var buf: [256]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    try writeString(writer, "hello\nworld");
    try std.testing.expectEqualStrings("\"hello\\nworld\"", fbs.buffered());
}

test "ObjectBuilder builds valid JSON" {
    var buf: [256]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    var obj = ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", "test");
    try obj.intField("count", 42);
    try obj.boolField("active", true);
    try obj.end();

    try std.testing.expectEqualStrings("{\"name\":\"test\",\"count\":42,\"active\":true}", fbs.buffered());
}

test "ObjectBuilder with HashMap iteration" {
    const allocator = std.testing.allocator;
    var buf: [512]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    // Create a string->string hashmap
    var map = std.StringHashMap([]const u8).init(allocator);
    defer map.deinit();

    try map.put("key1", "value1");
    try map.put("key2", "value2");

    var obj = ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.writeStringHashMap(&map);
    try obj.end();

    const output = fbs.buffered();
    // HashMap iteration order isn't guaranteed, so check both possible orderings
    const valid1 = std.mem.eql(u8, output, "{\"key1\":\"value1\",\"key2\":\"value2\"}");
    const valid2 = std.mem.eql(u8, output, "{\"key2\":\"value2\",\"key1\":\"value1\"}");
    try std.testing.expect(valid1 or valid2);
}

test "ObjectBuilder with int HashMap" {
    const allocator = std.testing.allocator;
    var buf: [256]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    var map = std.StringHashMap(u64).init(allocator);
    defer map.deinit();

    try map.put("count", 42);
    try map.put("total", 100);

    var obj = ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.writeIntHashMap(&map);
    try obj.end();

    const output = fbs.buffered();
    // Check it contains both entries (order varies)
    try std.testing.expect(std.mem.indexOf(u8, output, "\"count\":42") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"total\":100") != null);
}

test "ArrayBuilder with string slice" {
    var buf: [256]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    var arr = ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();
    try arr.writeStringSlice(&.{ "a", "b", "c" });
    try arr.end();

    try std.testing.expectEqualStrings("[\"a\",\"b\",\"c\"]", fbs.buffered());
}

test "stringify produces valid JSON" {
    const allocator = std.testing.allocator;

    const TestStruct = struct {
        name: []const u8,
        value: u32,
    };

    const data = TestStruct{ .name = "test", .value = 123 };
    const result = try stringifyCompact(allocator, data);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("{\"name\":\"test\",\"value\":123}", result);
}

test "nested object and array fields" {
    var buf: [512]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    var obj = ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("type", "record");

    var nested = try obj.objectField("metadata");
    try nested.begin();
    try nested.stringField("version", "1.0");
    try nested.end();

    var arr = try obj.arrayField("tags");
    try arr.begin();
    try arr.writeStringSlice(&.{ "important", "v2" });
    try arr.end();

    try obj.end();

    try std.testing.expectEqualStrings(
        "{\"type\":\"record\",\"metadata\":{\"version\":\"1.0\"},\"tags\":[\"important\",\"v2\"]}",
        fbs.buffered(),
    );
}
