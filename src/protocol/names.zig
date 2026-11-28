//! What every client-chosen name refuses: control characters, bytes up to
//! 0x1F and DEL (0x7F), NUL among them. Bytes from 0x80 are allowed, so a
//! name may be UTF-8.

const std = @import("std");

/// Whether `name` holds a control character.
pub fn hasControl(name: []const u8) bool {
    for (name) |c| if (isControl(c)) return true;
    return false;
}

pub fn isControl(c: u8) bool {
    return c <= 0x1F or c == 0x7F;
}

/// The refusal of `what` ("a stream name", "a worker id") holding one:
/// one shape for every kind of name.
pub fn controlRefusal(comptime what: []const u8) []const u8 {
    return "bad request: " ++ what ++ " must not contain control characters (bytes 0x00-0x1f or 0x7f)";
}

/// `bytes` as a log line shows a client's bytes: control characters, a
/// backslash and bytes that aren't UTF-8 as `\xNN` (a backslash `\\`),
/// valid UTF-8 as itself. So a line can't be broken or forged by what a
/// client sent, and a UTF-8 name reads as written.
pub fn escaped(bytes: []const u8) Escaped {
    return .{ .bytes = bytes };
}

pub const Escaped = struct {
    bytes: []const u8,

    pub fn format(self: Escaped, w: *std.Io.Writer) std.Io.Writer.Error!void {
        var i: usize = 0;
        while (i < self.bytes.len) {
            const c = self.bytes[i];
            if (c == '\\') {
                try w.writeAll("\\\\");
                i += 1;
                continue;
            }
            if (isControl(c)) {
                try w.print("\\x{x:0>2}", .{c});
                i += 1;
                continue;
            }
            if (c < 0x80) {
                try w.writeByte(c);
                i += 1;
                continue;
            }
            const n = std.unicode.utf8ByteSequenceLength(c) catch 0;
            if (n > 0 and i + n <= self.bytes.len and std.unicode.utf8ValidateSlice(self.bytes[i .. i + n])) {
                try w.writeAll(self.bytes[i .. i + n]);
                i += n;
            } else {
                try w.print("\\x{x:0>2}", .{c});
                i += 1;
            }
        }
    }
};

test "names: a log line shows controls, a backslash and bytes that aren't UTF-8 escaped, and UTF-8 as written" {
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("caf\xc3\xa9 r\\x0a1\\x0a\\x1b\\x7f \\\\ \\xff\\xc3", try std.fmt.bufPrint(&buf, "{f}", .{escaped("caf\xc3\xa9 r\n1\x0a\x1b\x7f \\ \xff\xc3")}));
    // A lead byte whose continuation isn't one is escaped, not written.
    try std.testing.expectEqualStrings("\\xc3(", try std.fmt.bufPrint(&buf, "{f}", .{escaped("\xc3\x28")}));
}

test "names: control characters and DEL are refused; UTF-8 and printable bytes are not" {
    for (0..0x20) |c| try std.testing.expect(hasControl(&.{@intCast(c)}));
    try std.testing.expect(hasControl("a\x7f"));
    try std.testing.expect(hasControl("abc\ndef"));
    try std.testing.expect(!hasControl("orders-é_1 ~"));
    try std.testing.expect(!hasControl(" ~\x80\xff"));
    try std.testing.expectEqualStrings("bad request: a stream name must not contain control characters (bytes 0x00-0x1f or 0x7f)", comptime controlRefusal("a stream name"));
}
