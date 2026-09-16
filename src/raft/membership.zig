//! The payload of a `raft_config` entry: the ids of every member. The
//! leader that writes it and every node that applies it read the same
//! bytes, so membership is exactly what the log says.

const std = @import("std");
const node_mod = @import("node.zig");

pub const MAX_MEMBERS = node_mod.MAX_PEERS + 1;
pub const MAX_SIZE = 4 + 4 * MAX_MEMBERS;

/// `[count:u32][id:u32]...`, little-endian.
pub fn encode(ids: []const u32, buf: *[MAX_SIZE]u8) []const u8 {
    const n: u32 = @intCast(@min(ids.len, MAX_MEMBERS));
    std.mem.writeInt(u32, buf[0..4], n, .little);
    for (ids[0..n], 0..) |id, i| std.mem.writeInt(u32, buf[4 + 4 * i ..][0..4], id, .little);
    return buf[0 .. 4 + 4 * n];
}

/// Null for bytes that are not a config: a short payload, a count the
/// bytes do not cover, no members or more than a group can hold, an id
/// of 0, or an id named twice. A group with no members would make every
/// node a leader of itself; a member named twice holds two peer slots and
/// a quorum it can never fill.
pub fn decode(payload: []const u8, out: *[MAX_MEMBERS]u32) ?[]u32 {
    if (payload.len < 4) return null;
    const n = std.mem.readInt(u32, payload[0..4], .little);
    if (n == 0 or n > MAX_MEMBERS or payload.len != 4 + 4 * n) return null;
    for (0..n) |i| {
        const id = std.mem.readInt(u32, payload[4 + 4 * i ..][0..4], .little);
        if (id == 0 or names(out[0..i], id)) return null;
        out[i] = id;
    }
    return out[0..n];
}

/// Whether `id` is one of `members`.
pub fn names(members: []const u32, id: u32) bool {
    for (members) |m| if (m == id) return true;
    return false;
}

const testing = std.testing;

test "membership: a config round-trips, and bytes that are not one are refused" {
    var buf: [MAX_SIZE]u8 = undefined;
    const bytes = encode(&.{ 3, 1, 2 }, &buf);
    try testing.expectEqual(@as(usize, 16), bytes.len);
    var out: [MAX_MEMBERS]u32 = undefined;
    try testing.expectEqualSlices(u32, &.{ 3, 1, 2 }, decode(bytes, &out).?);
    try testing.expect(names(decode(bytes, &out).?, 2));
    try testing.expect(!names(decode(bytes, &out).?, 4));
    try testing.expect(decode(encode(&.{}, &buf), &out) == null);
    try testing.expect(decode(encode(&.{ 1, 2, 1 }, &buf), &out) == null);
    try testing.expect(decode("abc", &out) == null);
    try testing.expect(decode(bytes[0..12], &out) == null);
    var zero: [8]u8 = undefined;
    std.mem.writeInt(u32, zero[0..4], 1, .little);
    std.mem.writeInt(u32, zero[4..8], 0, .little);
    try testing.expect(decode(&zero, &out) == null);
    var many: [MAX_SIZE + 4]u8 = undefined;
    std.mem.writeInt(u32, many[0..4], MAX_MEMBERS + 1, .little);
    try testing.expect(decode(&many, &out) == null);
}
