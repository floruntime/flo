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
    /// The write committed but this node didn't apply it: resending it
    /// writes it twice.
    committed_not_applied = 10,
    /// The write committed and applied, but its answer couldn't be built.
    answer_lost = 11,
    /// A refusal from an op family not yet on the op table; its status and
    /// message say what happened. Goes once every family is on the table.
    unclassified = 12,
    /// What the request names doesn't exist.
    not_found = 13,
    /// The key is in a prefix the server keeps for itself.
    reserved_key = 14,
    /// The write's `cas_version` isn't the key's version.
    cas_mismatch = 15,
    /// The write's `condition` didn't hold.
    condition_failed = 16,
    /// No open transaction has this id here: it committed, rolled back,
    /// was begun in another namespace, or never existed.
    no_such_transaction = 17,
    /// The request's routing key (its key, when it gives none) isn't the
    /// one its transaction was begun with.
    wrong_routing_key = 18,
    /// The transaction holds as many ops or bytes as it may.
    txn_too_large = 19,
    /// The shard holds as many waiting reads as it may.
    too_many_waiters = 20,
    /// Two fields given together that don't go together.
    conflicting_fields = 21,
    /// The shard stopped taking writes: one left memory before it reached
    /// disk. It takes them again once the node restarts.
    writes_stopped = 22,
    /// Too many writes wait for commit.
    too_many_proposals = 23,
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
