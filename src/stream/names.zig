//! Stream names the server keeps for itself. A name under `RESERVED_PREFIX`
//! is refused wherever a stream is named, so no user's stream, group, job
//! or trigger can ever be mistaken for, or read, one the server keeps.

const std = @import("std");

pub const RESERVED_PREFIX = "_flo:";

/// Why, for a refusal whose status says the rest.
pub const RESERVED_WHY = "stream names starting with \"" ++ RESERVED_PREFIX ++ "\" are reserved for the server";
pub const RESERVED = "unauthorized: " ++ RESERVED_WHY;

/// Whether `name` is one only the server may use.
pub fn reservedStreamName(name: []const u8) bool {
    return std.mem.startsWith(u8, name, RESERVED_PREFIX);
}

test "names: only a name starting with the prefix is reserved" {
    try std.testing.expect(reservedStreamName("_flo:"));
    try std.testing.expect(reservedStreamName("_flo:history"));
    try std.testing.expect(!reservedStreamName("_flo"));
    try std.testing.expect(!reservedStreamName("x_flo:y"));
    try std.testing.expect(!reservedStreamName("_FLO:y"));
    try std.testing.expect(!reservedStreamName(""));
}
