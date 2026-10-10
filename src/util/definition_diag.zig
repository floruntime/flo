//! Why a workflow or pipeline definition was refused, and where.
//!
//! The parsers walk the definition pushing one path segment per level, so a
//! refusal names its place: `unknown key "trasitions" at steps.charge`. The
//! CLI and the server call the same parser, so both print the same text.
//! Buffers are fixed; a path or message too long for them is cut with "…",
//! never failed.

const std = @import("std");
const JsonValue = std.json.Value;

pub const Diagnostic = struct {
    msg_buf: [320]u8 = undefined,
    msg_len: usize = 0,
    path_buf: [192]u8 = undefined,
    path_len: usize = 0,
    /// Set once a segment didn't fit; later segments are dropped with it.
    path_cut: bool = false,

    pub fn message(self: *const Diagnostic) []const u8 {
        return self.msg_buf[0..self.msg_len];
    }

    pub fn path(self: *const Diagnostic) []const u8 {
        return self.path_buf[0..self.path_len];
    }

    /// Enter the object or array under `key`; `pop` the returned mark on leaving.
    pub fn push(self: *Diagnostic, key: []const u8) Mark {
        const mark: Mark = .{ .len = self.path_len, .cut = self.path_cut };
        if (self.path_len > 0) self.append(".");
        self.append(key);
        return mark;
    }

    pub fn pushIndex(self: *Diagnostic, index: usize) Mark {
        const mark: Mark = .{ .len = self.path_len, .cut = self.path_cut };
        var buf: [24]u8 = undefined;
        self.append(std.fmt.bufPrint(&buf, "[{d}]", .{index}) catch unreachable);
        return mark;
    }

    pub fn pop(self: *Diagnostic, mark: Mark) void {
        self.path_len = mark.len;
        self.path_cut = mark.cut;
    }

    /// Record `fmt` followed by where it happened. Returns `err` so a parser
    /// can write `return d.fail(error.X, ...)`.
    pub fn fail(self: *Diagnostic, err: anytype, comptime fmt: []const u8, args: anytype) @TypeOf(err) {
        var w: std.Io.Writer = .fixed(&self.msg_buf);
        const ok = blk: {
            w.print(fmt, args) catch break :blk false;
            if (self.path_len == 0) {
                w.writeAll(" at the top level") catch break :blk false;
            } else {
                w.print(" at {s}{s}", .{ self.path(), if (self.path_cut) "…" else "" }) catch break :blk false;
            }
            break :blk true;
        };
        self.msg_len = w.end;
        if (!ok) {
            const marker = "…";
            self.msg_len = self.msg_buf.len - marker.len;
            @memcpy(self.msg_buf[self.msg_len..][0..marker.len], marker);
            self.msg_len += marker.len;
        }
        return err;
    }

    fn append(self: *Diagnostic, bytes: []const u8) void {
        if (self.path_cut) return;
        if (self.path_len + bytes.len > self.path_buf.len) {
            self.path_cut = true;
            return;
        }
        @memcpy(self.path_buf[self.path_len..][0..bytes.len], bytes);
        self.path_len += bytes.len;
    }

    pub const Mark = struct { len: usize, cut: bool };
};

/// The value's kind as a definition author would name it.
pub fn kindName(v: JsonValue) []const u8 {
    return switch (v) {
        .null => "null",
        .bool => "a boolean",
        .integer, .number_string => "an integer",
        .float => "a number",
        .string => "a string",
        .array => "a list",
        .object => "a map",
    };
}

/// Refuse any key of `obj` not in `allowed`.
pub fn checkKeys(d: *Diagnostic, obj: JsonValue, comptime allowed: []const []const u8, err: anytype) @TypeOf(err)!void {
    var it = obj.object.iterator();
    next: while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        inline for (allowed) |a| {
            if (std.mem.eql(u8, key, a)) continue :next;
        }
        return d.fail(err, "unknown key \"{s}\"", .{key});
    }
}

/// Name the first key that appears twice in one object of `json`, if any.
/// Only called once std.json has refused the text for a duplicate, to say
/// which key it was.
pub fn failDuplicateKey(allocator: std.mem.Allocator, d: *Diagnostic, json: []const u8, err: anytype) @TypeOf(err) {
    var scanner = std.json.Scanner.initCompleteInput(allocator, json);
    defer scanner.deinit();
    walkForDuplicate(allocator, d, &scanner) catch |e| switch (e) {
        error.Duplicate => return err,
        else => {},
    };
    return d.fail(err, "a key appears twice", .{});
}

const WalkError = error{ Duplicate, Invalid, OutOfMemory };

/// Walks one value starting at the scanner's next token.
fn walkForDuplicate(allocator: std.mem.Allocator, d: *Diagnostic, scanner: *std.json.Scanner) WalkError!void {
    const tok = scanner.nextAlloc(allocator, .alloc_if_needed) catch return error.Invalid;
    try walkValue(allocator, d, scanner, tok);
}

fn walkValue(allocator: std.mem.Allocator, d: *Diagnostic, scanner: *std.json.Scanner, tok: std.json.Token) WalkError!void {
    switch (tok) {
        .object_begin => {
            var seen: std.StringHashMapUnmanaged(void) = .empty;
            defer {
                var it = seen.keyIterator();
                while (it.next()) |k| allocator.free(k.*);
                seen.deinit(allocator);
            }
            while (true) {
                const key_tok = scanner.nextAlloc(allocator, .alloc_always) catch return error.Invalid;
                const key = switch (key_tok) {
                    .object_end => return,
                    .allocated_string => |s| s,
                    else => return error.Invalid,
                };
                if (seen.contains(key)) {
                    defer allocator.free(key);
                    return d.fail(error.Duplicate, "key \"{s}\" appears twice", .{key});
                }
                try seen.put(allocator, key, {});
                const mark = d.push(key);
                try walkForDuplicate(allocator, d, scanner);
                d.pop(mark);
            }
        },
        .array_begin => {
            var i: usize = 0;
            while (true) : (i += 1) {
                const t = scanner.nextAlloc(allocator, .alloc_if_needed) catch return error.Invalid;
                if (t == .array_end) return;
                const mark = d.pushIndex(i);
                try walkValue(allocator, d, scanner, t);
                d.pop(mark);
            }
        },
        .allocated_string, .allocated_number => |s| allocator.free(s),
        else => {},
    }
}

test "Diagnostic: path and message" {
    var d: Diagnostic = .{};
    const a = d.push("steps");
    const b = d.push("charge");
    try std.testing.expectEqual(error.X, d.fail(error.X, "unknown key \"{s}\"", .{"trasitions"}));
    try std.testing.expectEqualStrings("unknown key \"trasitions\" at steps.charge", d.message());
    d.pop(b);
    const c = d.pushIndex(2);
    try std.testing.expectEqual(error.X, d.fail(error.X, "bad", .{}));
    try std.testing.expectEqualStrings("bad at steps[2]", d.message());
    d.pop(c);
    d.pop(a);
    try std.testing.expectEqual(error.X, d.fail(error.X, "bad", .{}));
    try std.testing.expectEqualStrings("bad at the top level", d.message());
}

test "Diagnostic: an overlong path or message is cut, not failed" {
    var d: Diagnostic = .{};
    const long = "k" ** 100;
    _ = d.push(long);
    _ = d.push(long);
    _ = d.push(long);
    try std.testing.expect(d.path_cut);
    try std.testing.expectEqual(error.X, d.fail(error.X, "unknown key \"{s}\"", .{long}));
    try std.testing.expect(std.mem.endsWith(u8, d.message(), "…"));
    try std.testing.expectEqual(error.X, d.fail(error.X, "{s}", .{"m" ** 400}));
    try std.testing.expectEqual(d.msg_buf.len, d.msg_len);
    try std.testing.expect(std.mem.endsWith(u8, d.message(), "…"));
}

test "failDuplicateKey names the key and where" {
    const allocator = std.testing.allocator;
    var d: Diagnostic = .{};
    const json =
        \\{"start": {"run": "a", "transitions": {"ok": "x"}, "run": "b"}}
    ;
    try std.testing.expectEqual(error.Dup, failDuplicateKey(allocator, &d, json, error.Dup));
    try std.testing.expectEqualStrings("key \"run\" appears twice at start", d.message());
}
