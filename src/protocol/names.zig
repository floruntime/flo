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

/// Why a `kind` name ("stream", "action") holding one is refused, for a
/// caller to put after its own prefix.
pub fn controlRefusal(comptime kind: []const u8) []const u8 {
    return kind ++ " name must not contain control characters (bytes 0x00-0x1f or 0x7f)";
}

test "names: control characters and DEL are refused; UTF-8 and printable bytes are not" {
    for (0..0x20) |c| try std.testing.expect(hasControl(&.{@intCast(c)}));
    try std.testing.expect(hasControl("a\x7f"));
    try std.testing.expect(hasControl("abc\ndef"));
    try std.testing.expect(!hasControl("orders-é_1 ~"));
    try std.testing.expect(!hasControl(" ~\x80\xff"));
    try std.testing.expectEqualStrings("stream name must not contain control characters (bytes 0x00-0x1f or 0x7f)", comptime controlRefusal("stream"));
}
