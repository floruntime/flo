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
    /// A value it reads, escaped, takes more than the caller's buffer.
    TooLong,
    /// Nested deeper than the reader's state holds (`MAX_DEPTH`).
    TooDeep,
};

/// The part of a read's buffer its own state takes: a bit a level of
/// nesting, grown by doubling, so a record nested ~4,000 levels deep is
/// read and ~5,000 is `TooDeep` (pinned by a test at `MAX_DEPTH`).
pub const STATE: usize = 1024;
/// A record nested this deep is read; past the reader's state it isn't.
pub const MAX_DEPTH: usize = 4000;
/// The longest key that may be a name; one longer is passed over.
const KEY: usize = 256;

/// Fills `out[i]` with the top-level field `names[i]` of `record`, or null
/// when it has none. A field the record gives twice is read at its first; a
/// name asked for twice gets that same value in each of its slots. A value
/// with escapes is decoded into `buf` past its first `STATE` bytes, and one
/// that doesn't fit is `TooLong`, never cut; one without escapes is read in
/// place, whatever its length.
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
            .object_end => return finish(&scanner),
            // Too long for any name: not one of ours.
            .string => if (stringInto(&scanner, &key_buf)) |k| k.value else |e| switch (e) {
                error.TooLong => null,
                else => return e,
            },
            else => return error.NotObject,
        };
        // Every slot asking for it, not yet filled: a field the record gave
        // before has filled its slots, so this, a later one, fills none.
        var wanted = false;
        if (key) |k| for (names, out) |n, o| {
            if (o == null and std.mem.eql(u8, n, k)) wanted = true;
        };
        if (!wanted) {
            scanner.skipValue() catch |e| return mapError(e);
            continue;
        }
        const field: Field = switch (scanner.peekNextTokenType() catch |e| return mapError(e)) {
            .string => blk: {
                const v = try stringInto(&scanner, values);
                values = values[v.copied..];
                break :blk .{ .string = v.value };
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
        for (names, out) |n, *o| if (o.* == null and std.mem.eql(u8, n, key.?)) {
            o.* = field;
            left -= 1;
        };
        // Every name found: the rest of the record isn't read, so a later
        // fault in it drops nothing. What follows the value is, so a number
        // that runs on (`1.5.7`, `012`) or a record cut short after it is
        // still a fault.
        if (left == 0) {
            const next = scanner.peekNextTokenType() catch |e| return mapError(e);
            // At its end, the record is held to ending there, as a read of
            // every field would be.
            if (next == .object_end) return finish(&scanner);
            if (next != .string) return error.NotObject;
            return;
        }
    }
}

/// After the record's last field: it ends there, with nothing after it.
fn finish(scanner: *std.json.Scanner) Error!void {
    _ = scanner.next() catch |e| return mapError(e);
    if ((scanner.next() catch |e| return mapError(e)) != .end_of_document) return error.NotObject;
}

/// A string's value, and how much of the caller's buffer it took: none
/// when it is a slice of the record.
const Read = struct { value: []const u8, copied: usize };

/// The next token, a string, decoded into `into`: a slice of the record
/// when it has no escapes. It comes as pieces (runs and decoded escapes),
/// the last as `.string`; all are read either way, so one too long for
/// `into` is `TooLong` with the reader at the next token.
fn stringInto(scanner: *std.json.Scanner, into: []u8) Error!Read {
    var len: usize = 0;
    var over = false;
    var first = true;
    while (true) {
        const tok = scanner.next() catch |e| return mapError(e);
        const last = tok == .string;
        // Whole and unescaped: the record's own bytes.
        if (first and last) return .{ .value = tok.string, .copied = 0 };
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
    return .{ .value = into[0..len], .copied = len };
}

/// A record that is a JSON number and nothing else: its value. Not a hex,
/// `inf`, `nan` or `_`-separated form a looser parse would take.
pub fn bareNumber(record: []const u8, state: []u8) ?f64 {
    var fba = std.heap.FixedBufferAllocator.init(state);
    var scanner = std.json.Scanner.initCompleteInput(fba.allocator(), record);
    const n = switch (scanner.next() catch return null) {
        .number => |n| n,
        else => return null,
    };
    if ((scanner.next() catch return null) != .end_of_document) return null;
    return std.fmt.parseFloat(f64, n) catch null;
}

/// The scanner's own state is all it allocates: running out is depth.
fn mapError(e: anyerror) Error {
    return if (e == error.OutOfMemory) error.TooDeep else error.NotObject;
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

/// A sink's buffer: the state, and 7168 bytes for escaped values.
var sink_buf: [8 * 1024]u8 = undefined;

fn sized(comptime n: usize, comptime c: u8) []const u8 {
    return &@as([n]u8, @splat(c));
}

test "record fields: a value without escapes is read in place, however long, and a buffer's worth of escapes still fits" {
    const a = testing.allocator;
    for ([_]usize{ 7168, 7169, 100_000 }) |n| {
        const v = try a.alloc(u8, n);
        defer a.free(v);
        @memset(v, 'a');
        const rec = try std.fmt.allocPrint(a, "{{\"h\":\"{s}\"}}", .{v});
        defer a.free(rec);
        var out: [1]?Field = undefined;
        try read(rec, &.{"h"}, &out, &sink_buf);
        try testing.expectEqual(n, out[0].?.string.len);
    }
    // Two unescaped of 4000: neither takes the buffer.
    var out2: [2]?Field = undefined;
    const two = comptime "{\"a\":\"" ++ sized(4000, 'x') ++ "\",\"b\":\"" ++ sized(4000, 'y') ++ "\"}";
    try read(two, &.{ "a", "b" }, &out2, &sink_buf);
    try testing.expectEqual(@as(usize, 4000), out2[0].?.string.len);
    try testing.expectEqual(@as(usize, 4000), out2[1].?.string.len);
    // A plain 7000 and an escaped 200: only the escaped one is copied.
    const esc = comptime blk: {
        @setEvalBranchQuota(10_000);
        var e: []const u8 = "";
        for (0..200) |_| e = e ++ "\\u0062";
        break :blk e;
    };
    const mixed = comptime "{\"a\":\"" ++ sized(7000, 'x') ++ "\",\"b\":\"" ++ esc ++ "\"}";
    try read(mixed, &.{ "a", "b" }, &out2, &sink_buf);
    try testing.expectEqual(@as(usize, 7000), out2[0].?.string.len);
    try testing.expectEqualStrings(comptime sized(200, 'b'), out2[1].?.string);
}

test "record fields: a number that runs on, or a record cut after it, is a fault, whichever field comes last" {
    for ([_][]const u8{ "1.5.7", "-0x10", "1e5e5", "012" }) |bad| {
        var rb: [64]u8 = undefined;
        try testing.expectError(error.NotObject, one(try std.fmt.bufPrint(&rb, "{{\"value\":{s}}}", .{bad}), "value"));
        var out: [2]?Field = undefined;
        // After the other field wanted, and before it.
        try testing.expectError(error.NotObject, read(try std.fmt.bufPrint(&rb, "{{\"h\":\"a\",\"value\":{s}}}", .{bad}), &.{ "h", "value" }, &out, &sink_buf));
        try testing.expectError(error.NotObject, read(try std.fmt.bufPrint(&rb, "{{\"value\":{s},\"h\":\"a\"}}", .{bad}), &.{ "h", "value" }, &out, &sink_buf));
    }
    try testing.expectError(error.NotObject, one("{\"value\":7", "value"));
    try testing.expectError(error.NotObject, one("{\"value\":7,", "value"));
}

test "record fields: a field asked for twice fills both slots from its first appearance" {
    var out: [2]?Field = undefined;
    try read("{\"v\":5}", &.{ "v", "v" }, &out, &sink_buf);
    try testing.expectEqual(@as(?f64, 5), out[0].?.float());
    try testing.expectEqual(@as(?f64, 5), out[1].?.float());
    try read("{\"v\":5,\"v\":9}", &.{ "v", "v" }, &out, &sink_buf);
    try testing.expectEqual(@as(?f64, 5), out[0].?.float());
    try testing.expectEqual(@as(?f64, 5), out[1].?.float());
}

test "record fields: a record nested to MAX_DEPTH is read, and well past it is too deep" {
    const a = testing.allocator;
    for ([_]struct { depth: usize, ok: bool }{ .{ .depth = MAX_DEPTH, .ok = true }, .{ .depth = MAX_DEPTH + 1000, .ok = false } }) |c| {
        var rec: std.ArrayList(u8) = .empty;
        defer rec.deinit(a);
        try rec.appendSlice(a, "{\"x\":");
        try rec.appendNTimes(a, '[', c.depth);
        try rec.appendNTimes(a, ']', c.depth);
        try rec.appendSlice(a, ",\"v\":1}");
        var out: [1]?Field = undefined;
        if (c.ok) {
            try read(rec.items, &.{"v"}, &out, &sink_buf);
            try testing.expectEqual(@as(?f64, 1), out[0].?.float());
        } else try testing.expectError(error.TooDeep, read(rec.items, &.{"v"}, &out, &sink_buf));
    }
}

test "record fields: a bare number is a JSON number, not a looser form" {
    var st: [STATE]u8 = undefined;
    try testing.expectEqual(@as(?f64, 12), bareNumber("12", &st));
    try testing.expectEqual(@as(?f64, -500), bareNumber(" -0.5e3\n", &st));
    for ([_][]const u8{ "0x10", "inf", "nan", "1_000", "+1", ".5", "1.", "012", "1 2", "" }) |s| try testing.expectEqual(@as(?f64, null), bareNumber(s, &st));
}

test "record fields: a record that runs on past its object is a fault, whether every name is found or not" {
    for ([_][]const u8{ "{\"h\":\"a\"} garbage", "{\"h\":\"a\"}}", "{\"h\":\"a\"}{\"h\":\"b\"}" }) |r| {
        // Every name found, at the last field.
        try testing.expectError(error.NotObject, one(r, "h"));
        // A name missing: read to the end.
        var out: [2]?Field = undefined;
        try testing.expectError(error.NotObject, read(r, &.{ "h", "missing" }, &out, &sink_buf));
    }
}
