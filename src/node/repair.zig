//! A repair skip: a committed `repair_skip` entry names the entry a shard
//! halted at, by index, term and checksum. On every replica that entry then
//! applies nothing, and the shard goes on past it.
//!
//! A skip is permanent. The halted write's client was told it will not
//! run, so no later build may apply it, even one that could: boot collects
//! the committed skips before replay applies anything (`Skips`), and
//! replay passes over a skipped entry without running its applier.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");

/// What a skip names, and why.
pub const Skip = struct {
    index: u64,
    term: u64,
    crc: u32,
    reason: []const u8 = "",

    pub const MAX_REASON = 256;
    const FIXED = 8 + 8 + 4;
    pub const MAX_PAYLOAD = FIXED + MAX_REASON;

    /// The halted entry `e`, skipped for `reason`.
    pub fn of(e: *const entry_mod.Entry, reason: []const u8) Skip {
        return .{ .index = e.header.index, .term = e.header.term, .crc = e.header.crc32c, .reason = reason };
    }

    /// `[index:u64][term:u64][crc:u32][reason]`, little-endian; a reason
    /// past `MAX_REASON` is cut short.
    pub fn encode(self: *const Skip, buf: *[MAX_PAYLOAD]u8) []const u8 {
        std.mem.writeInt(u64, buf[0..8], self.index, .little);
        std.mem.writeInt(u64, buf[8..16], self.term, .little);
        std.mem.writeInt(u32, buf[16..20], self.crc, .little);
        const n = @min(self.reason.len, MAX_REASON);
        @memcpy(buf[FIXED..][0..n], self.reason[0..n]);
        return buf[0 .. FIXED + n];
    }

    /// The skip a `repair_skip` entry carries, or null for one that isn't.
    pub fn decode(e: *const entry_mod.Entry) ?Skip {
        if (e.header.entry_type != @backingInt(entry_mod.EntryType.repair_skip)) return null;
        const p = e.payload;
        if (p.len < FIXED or p.len > MAX_PAYLOAD) return null;
        return .{
            .index = std.mem.readInt(u64, p[0..8], .little),
            .term = std.mem.readInt(u64, p[8..16], .little),
            .crc = std.mem.readInt(u32, p[16..20], .little),
            .reason = p[FIXED..],
        };
    }

    /// Whether `e` is the entry this skip names.
    pub fn names(self: *const Skip, e: *const entry_mod.Entry) bool {
        return self.index == e.header.index and self.term == e.header.term and self.crc == e.header.crc32c;
    }
};

/// The entries this shard's committed skips name. Filled at boot from the
/// log, and live as a halted shard finds the skip for its halt.
pub const Skips = struct {
    named: std.ArrayListUnmanaged(Named) = .empty,

    const Named = struct { index: u64, term: u64, crc: u32 };

    pub fn deinit(self: *Skips, allocator: std.mem.Allocator) void {
        self.named.deinit(allocator);
    }

    /// Add `s`; a second skip for the same entry adds nothing.
    pub fn add(self: *Skips, allocator: std.mem.Allocator, s: *const Skip) !void {
        if (self.find(s.index)) |n| if (n.term == s.term and n.crc == s.crc) return;
        try self.named.append(allocator, .{ .index = s.index, .term = s.term, .crc = s.crc });
    }

    /// Whether a committed skip names `e`.
    pub fn skips(self: *const Skips, e: *const entry_mod.Entry) bool {
        for (self.named.items) |n| {
            if (n.index == e.header.index and n.term == e.header.term and n.crc == e.header.crc32c) return true;
        }
        return false;
    }

    /// Whether `s` is a skip this set already holds.
    pub fn holds(self: *const Skips, s: *const Skip) bool {
        for (self.named.items) |n| {
            if (n.index == s.index and n.term == s.term and n.crc == s.crc) return true;
        }
        return false;
    }

    fn find(self: *const Skips, index: u64) ?Named {
        for (self.named.items) |n| if (n.index == index) return n;
        return null;
    }
};

const testing = std.testing;

fn testEntry(index: u64, term: u64) entry_mod.Entry {
    var e = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, term, index, index, "x");
    e.header.crc32c = e.computeCrc();
    return e;
}

test "repair skip: a skip round-trips through its entry, and names only its entry" {
    const halted = testEntry(7, 2);
    const s = Skip.of(&halted, "bad payload");
    var buf: [Skip.MAX_PAYLOAD]u8 = undefined;
    const e = entry_mod.buildEntry(.repair_skip, entry_mod.Flags.NONE, 3, 9, 9, s.encode(&buf));
    const back = Skip.decode(&e).?;
    try testing.expectEqual(@as(u64, 7), back.index);
    try testing.expectEqual(@as(u64, 2), back.term);
    try testing.expectEqualStrings("bad payload", back.reason);
    try testing.expect(back.names(&halted));
    try testing.expect(!back.names(&testEntry(7, 3)));
    // Not a skip, and a skip too short to carry one.
    try testing.expect(Skip.decode(&halted) == null);
    try testing.expect(Skip.decode(&entry_mod.buildEntry(.repair_skip, entry_mod.Flags.NONE, 3, 9, 9, "short")) == null);
}

test "repair skip: a second skip for the same entry adds nothing" {
    var set: Skips = .{};
    defer set.deinit(testing.allocator);
    const halted = testEntry(7, 2);
    const s = Skip.of(&halted, "");
    try set.add(testing.allocator, &s);
    try set.add(testing.allocator, &s);
    try testing.expectEqual(@as(usize, 1), set.named.items.len);
    try testing.expect(set.skips(&halted));
    try testing.expect(!set.skips(&testEntry(7, 3)));
    try testing.expect(!set.skips(&testEntry(8, 2)));
}
