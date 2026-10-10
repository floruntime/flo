//! Why a request was refused, and whether it ran. One list shared by the
//! server, its metrics and every SDK.
//!
//! A refusal's body is `[reason:u16][ran:u8][message]`. The status in the
//! response header says whether to retry; the reason says what happened;
//! `ran` says whether the request took effect, so retry logic reads a
//! byte, not text; the message is for a person.

const std = @import("std");

pub const Reason = enum(u16) {
    /// The op code names no op this server serves to clients.
    unknown_op = 1,
    /// A value that isn't what its field declares: cut short, trailing
    /// bytes, an unknown enum value, invalid UTF-8.
    malformed = 2,
    /// A field longer than its declared bound.
    field_too_long = 3,
    /// A number outside its declared range.
    out_of_range = 4,
    /// The write found no leader to take it.
    leader_unknown = 5,
    /// The shard that owns the request is too busy to take it.
    shard_busy = 6,
    /// The client was built from another op table.
    table_mismatch = 7,
    /// A header flag bit this server doesn't define.
    unknown_flag = 8,
    /// A bug: the server can't say more.
    internal = 9,
};

/// Whether a refused request took effect.
pub const Ran = enum(u8) {
    no = 0,
    yes = 1,
    /// It was proposed, and may still apply: a leader change, say.
    unknown = 2,
};

/// A refusal's body, parsed.
pub const Refusal = struct {
    reason: Reason,
    ran: Ran,
    message: []const u8,

    pub const HEADER_LEN = 3;

    /// Writes the body into `buf`. A message too long for it is cut at a
    /// UTF-8 code-point boundary and marked, so every refusal is sent.
    pub fn encode(r: Refusal, buf: []u8) []const u8 {
        std.debug.assert(buf.len >= HEADER_LEN + TRUNCATED_MARKER.len);
        std.mem.writeInt(u16, buf[0..2], @intFromEnum(r.reason), .little);
        buf[2] = @intFromEnum(r.ran);
        const room = buf.len - HEADER_LEN;
        if (r.message.len <= room) {
            @memcpy(buf[HEADER_LEN..][0..r.message.len], r.message);
            return buf[0 .. HEADER_LEN + r.message.len];
        }
        var cut = room - TRUNCATED_MARKER.len;
        while (cut > 0 and r.message[cut] & 0b1100_0000 == 0b1000_0000) cut -= 1;
        @memcpy(buf[HEADER_LEN..][0..cut], r.message[0..cut]);
        @memcpy(buf[HEADER_LEN + cut ..][0..TRUNCATED_MARKER.len], TRUNCATED_MARKER);
        return buf[0 .. HEADER_LEN + cut + TRUNCATED_MARKER.len];
    }

    /// Parses a refusal body. An unknown reason or `ran` value is a body
    /// this client can't read, so it is refused, not guessed at.
    pub fn decode(body: []const u8) error{Malformed}!Refusal {
        if (body.len < HEADER_LEN) return error.Malformed;
        return .{
            .reason = std.enums.fromInt(Reason, std.mem.readInt(u16, body[0..2], .little)) orelse return error.Malformed,
            .ran = std.enums.fromInt(Ran, body[2]) orelse return error.Malformed,
            .message = body[HEADER_LEN..],
        };
    }
};

/// Marks a refusal message cut to fit its frame.
pub const TRUNCATED_MARKER = " …[truncated]";

const testing = std.testing;

test "reason: a refusal body round-trips" {
    var buf: [64]u8 = undefined;
    const body = (Refusal{ .reason = .out_of_range, .ran = .no, .message = "ttl_ms is past 86400000" }).encode(&buf);
    const r = try Refusal.decode(body);
    try testing.expectEqual(Reason.out_of_range, r.reason);
    try testing.expectEqual(Ran.no, r.ran);
    try testing.expectEqualStrings("ttl_ms is past 86400000", r.message);
}

test "reason: a long message is cut at a code point and marked" {
    var buf: [3 + 24]u8 = undefined;
    const msg = "ééééééééééééééééééééééééé"; // two bytes each
    const body = (Refusal{ .reason = .internal, .ran = .unknown, .message = msg }).encode(&buf);
    const r = try Refusal.decode(body);
    try testing.expect(std.mem.endsWith(u8, r.message, TRUNCATED_MARKER));
    try testing.expect(std.unicode.utf8ValidateSlice(r.message));
    try testing.expect(body.len <= buf.len);
}

test "reason: an unknown reason or ran value is refused" {
    try testing.expectError(error.Malformed, Refusal.decode(&.{ 0xff, 0xff, 0 }));
    try testing.expectError(error.Malformed, Refusal.decode(&.{ 1, 0, 9 }));
    try testing.expectError(error.Malformed, Refusal.decode(&.{ 1, 0 }));
}
