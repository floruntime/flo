//! A record's top-level fields, read by name in one pass.
//!
//! A sink reads fields of a JSON record by name: a TS sink's value, its
//! fields and its tags. Each is the record's own top-level field: one of the
//! same name inside a nested object, or text that looks like `"name":`
//! inside a string, is not it. A string's escapes are decoded.

const std = @import("std");

pub const Field = union(enum) {
    string: []const u8,
    /// A JSON number, as written.
    number: []const u8,
    /// A bool, null, array or object.
    other,

    /// The number it holds, if it is one.
    pub fn float(f: Field) ?f64 {
        return switch (f) {
            .number => |n| std.fmt.parseFloat(f64, n) catch null,
            else => null,
        };
    }
};

pub const Error = error{
    /// Not a JSON object, or not well formed.
    NotObject,
    /// A key or value takes more than the caller's buffer.
    TooLong,
};

/// The part of a read's buffer its own state takes: one bit a level of
/// nesting, so a record nested past ~8k levels is `TooLong`.
const STATE: usize = 1024;
/// The longest key that may be a name; one longer is passed over.
const KEY: usize = 256;

/// Fills `out[i]` with the top-level field `names[i]` of `record`, or null
/// when it has none. A name given twice in the record takes its first, as a
/// reader that stops at the first match would. The values read are decoded
/// into `buf` past its first `STATE` bytes; one that doesn't fit is
/// `TooLong`, never cut.
pub fn read(record: []const u8, names: []const []const u8, out: []?Field, buf: []u8) Error!void {
    std.debug.assert(out.len == names.len);
    if (buf.len <= STATE) return error.TooLong;
    @memset(out, null);
    var state_fba = std.heap.FixedBufferAllocator.init(buf[0..STATE]);
    var values = buf[STATE..];
    var scanner = std.json.Scanner.initCompleteInput(state_fba.allocator(), record);
    if ((scanner.next() catch |e| return mapError(e)) != .object_begin) return error.NotObject;
    var left = names.len;
    var key_buf: [KEY]u8 = undefined;
    while (true) {
        const key: ?[]const u8 = switch (scanner.peekNextTokenType() catch |e| return mapError(e)) {
            .object_end => return,
            // Too long for any name: not one of ours.
            .string => stringInto(&scanner, &key_buf) catch |e| switch (e) {
                error.TooLong => null,
                else => return e,
            },
            else => return error.NotObject,
        };
        const slot = if (key) |k| for (names, 0..) |n, i| {
            if (out[i] == null and std.mem.eql(u8, n, k)) break i;
        } else null else null;
        const i = slot orelse {
            scanner.skipValue() catch |e| return mapError(e);
            continue;
        };
        out[i] = switch (scanner.peekNextTokenType() catch |e| return mapError(e)) {
            .string => blk: {
                const v = try stringInto(&scanner, values);
                values = values[v.len..];
                break :blk .{ .string = v };
            },
            // A complete input gives a number whole.
            .number => switch (scanner.next() catch |e| return mapError(e)) {
                .number => |n| .{ .number = n },
                else => return error.NotObject,
            },
            else => blk: {
                scanner.skipValue() catch |e| return mapError(e);
                break :blk .other;
            },
        };
        left -= 1;
        // Every name found: the rest of the record isn't read, so a later
        // fault in it drops nothing.
        if (left == 0) return;
    }
}

/// The next token, a string, decoded into `into`: a slice of the record
/// when it has no escapes. It comes as pieces (runs and decoded escapes),
/// the last as `.string`; all are read either way, so one too long for
/// `into` is `TooLong` with the reader at the next token.
fn stringInto(scanner: *std.json.Scanner, into: []u8) Error![]const u8 {
    var len: usize = 0;
    var over = false;
    var first = true;
    while (true) {
        const tok = scanner.next() catch |e| return mapError(e);
        const last = tok == .string;
        // Whole and unescaped: the record's own bytes.
        if (first and last) return tok.string;
        first = false;
        const piece: []const u8 = switch (tok) {
            .string, .partial_string => |p| p,
            .partial_string_escaped_1 => |*b| b,
            .partial_string_escaped_2 => |*b| b,
            .partial_string_escaped_3 => |*b| b,
            .partial_string_escaped_4 => |*b| b,
            else => return error.NotObject,
        };
        if (!over) {
            if (len + piece.len > into.len) over = true else {
                @memcpy(into[len..][0..piece.len], piece);
                len += piece.len;
            }
        }
        if (last) break;
    }
    if (over) return error.TooLong;
    return into[0..len];
}

fn mapError(e: anyerror) Error {
    return if (e == error.OutOfMemory) error.TooLong else error.NotObject;
}

const testing = std.testing;

/// What a decoded value is read into: it outlives `one`'s call.
var one_buf: [STATE + 256]u8 = undefined;

fn one(record: []const u8, name: []const u8) Error!?Field {
    var out: [1]?Field = undefined;
    try read(record, &.{name}, &out, &one_buf);
    return out[0];
}

test "record fields: a string's escapes are decoded, so values that differ stay apart" {
    try testing.expectEqualStrings("web\"1", (try one("{\"h\":\"web\\\"1\"}", "h")).?.string);
    try testing.expectEqualStrings("web\"2", (try one("{\"h\":\"web\\\"2\"}", "h")).?.string);
    try testing.expectEqualStrings("a\\b", (try one("{\"h\":\"a\\\\b\"}", "h")).?.string);
    try testing.expectEqualStrings("é", (try one("{\"h\":\"\\u00e9\"}", "h")).?.string);
    // A key's escapes too.
    try testing.expectEqualStrings("x", (try one("{\"\\u0068\":\"x\"}", "h")).?.string);
}

test "record fields: only the top-level field counts, not one nested or inside a string" {
    // Nested before and after the record's own.
    try testing.expectEqualStrings("y", (try one("{\"meta\":{\"h\":\"x\"},\"h\":\"y\"}", "h")).?.string);
    try testing.expectEqualStrings("y", (try one("{\"h\":\"y\",\"meta\":{\"h\":\"x\"}}", "h")).?.string);
    try testing.expectEqualStrings("7", (try one("{\"a\":[{\"v\":1}],\"v\":7}", "v")).?.number);
    // Only nested: none.
    try testing.expect((try one("{\"meta\":{\"v\":1}}", "v")) == null);
    // The name's text inside another string isn't a field.
    try testing.expect((try one("{\"note\":\"\\\"v\\\":9\"}", "v")) == null);
    try testing.expectEqual(@as(?f64, 3), (try one("{\"note\":\"\\\"v\\\":9\",\"v\":3}", "v")).?.float());
}

test "record fields: a name the record gives twice takes its first" {
    try testing.expectEqualStrings("a", (try one("{\"h\":\"a\",\"h\":\"b\"}", "h")).?.string);
}

test "record fields: each kind of value is told apart" {
    try testing.expectEqual(@as(?f64, -1.5e3), (try one("{\"v\":-1.5e3}", "v")).?.float());
    for ([_][]const u8{ "true", "null", "[1,2]", "{\"x\":1}" }) |v| {
        var rb: [64]u8 = undefined;
        const f = (try one(try std.fmt.bufPrint(&rb, "{{\"v\":{s}}}", .{v}), "v")).?;
        try testing.expect(f == .other);
        try testing.expectEqual(@as(?f64, null), f.float());
    }
    // A quoted number is a string, not a number.
    try testing.expectEqual(@as(?f64, null), (try one("{\"v\":\"7\"}", "v")).?.float());
}

test "record fields: a record that isn't an object is refused, and a value past the buffer too, never cut" {
    for ([_][]const u8{ "7", "\"s\"", "[1]", "", "{\"h\":", "{\"h\" \"x\"}" }) |r| try testing.expectError(error.NotObject, one(r, "h"));
    var out: [1]?Field = undefined;
    var small: [STATE + 64]u8 = undefined;
    const long = comptime blk: {
        @setEvalBranchQuota(100_000);
        var s: []const u8 = "{\"h\":\"";
        for (0..80) |_| s = s ++ "\\u0061";
        break :blk s ++ "\"}";
    };
    try testing.expectError(error.TooLong, read(long, &.{"h"}, &out, &small));
}

test "record fields: one pass fills every name, each from its own field" {
    var out: [3]?Field = undefined;
    var buf: [STATE + 256]u8 = undefined;
    try read("{\"r\":\"eu\",\"x\":{\"h\":\"no\"},\"v\":2,\"h\":\"a\"}", &.{ "h", "r", "v" }, &out, &buf);
    try testing.expectEqualStrings("a", out[0].?.string);
    try testing.expectEqualStrings("eu", out[1].?.string);
    try testing.expectEqual(@as(?f64, 2), out[2].?.float());
}

test "record fields: a key too long to be a name, escaped or not, is passed over, not a fault" {
    const key = comptime blk: {
        @setEvalBranchQuota(100_000);
        var k: []const u8 = "";
        for (0..KEY) |_| k = k ++ "\\u0061";
        break :blk k;
    };
    try testing.expectEqualStrings("y", (try one("{\"" ++ key ++ "\":\"x\",\"h\":\"y\"}", "h")).?.string);
}
