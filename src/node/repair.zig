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
const InboundSkip = @import("../contract/store.zig").InboundSkip;

/// What a skip names, and why. A skip of a `channel_inbound` may name one
/// message of it by `seq` (0: the whole entry), and records the message
/// the group halted at (`halt_seq`, 0: none known); see `channels.zig`.
pub const Skip = struct {
    index: u64,
    term: u64,
    crc: u32,
    seq: u64 = 0,
    halt_seq: u64 = 0,
    reason: []const u8 = "",

    pub const MAX_REASON = 256;
    const FIXED = 8 + 8 + 4 + 8 + 8;
    pub const MAX_PAYLOAD = FIXED + MAX_REASON;

    /// The halted entry `e`, skipped for `reason`.
    pub fn of(e: *const entry_mod.Entry, reason: []const u8) Skip {
        return .{ .index = e.header.index, .term = e.header.term, .crc = e.header.crc32c, .reason = reason };
    }

    /// `[index:u64][term:u64][crc:u32][seq:u64][halt_seq:u64][reason]`,
    /// little-endian; a
    /// reason past `MAX_REASON` is cut short.
    pub fn encode(self: *const Skip, buf: *[MAX_PAYLOAD]u8) []const u8 {
        std.mem.writeInt(u64, buf[0..8], self.index, .little);
        std.mem.writeInt(u64, buf[8..16], self.term, .little);
        std.mem.writeInt(u32, buf[16..20], self.crc, .little);
        std.mem.writeInt(u64, buf[20..28], self.seq, .little);
        std.mem.writeInt(u64, buf[28..36], self.halt_seq, .little);
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
            .seq = std.mem.readInt(u64, p[20..28], .little),
            .halt_seq = std.mem.readInt(u64, p[28..36], .little),
            .reason = p[FIXED..],
        };
    }

    /// Whether `e` is the entry this skip names.
    pub fn names(self: *const Skip, e: *const entry_mod.Entry) bool {
        return self.index == e.header.index and self.term == e.header.term and self.crc == e.header.crc32c;
    }
};

/// The entries this shard's committed skips name. Filled at boot from the
/// log, and live as a halted shard finds the skip for its halt. An inbound
/// may be skipped more than once, a message at a time, as each one it
/// carries halts the shard in turn: one row per (entry, seq), seq 0 for
/// the whole entry.
pub const Skips = struct {
    named: std.ArrayListUnmanaged(Named) = .empty,

    const Named = struct { index: u64, term: u64, crc: u32, seq: u64 = 0, halt_seq: u64 = 0 };

    pub fn deinit(self: *Skips, allocator: std.mem.Allocator) void {
        self.named.deinit(allocator);
    }

    /// Add `s`; one that skips nothing more than the set already does, or
    /// is `stale`, adds nothing. Boot adds the log's skips in log order and
    /// a halted shard adds each as it goes past a halt, so the set an
    /// inbound applies under is the same on both paths.
    pub fn add(self: *Skips, allocator: std.mem.Allocator, s: *const Skip) !void {
        if (self.holds(s) or self.stale(s)) return;
        try self.named.append(allocator, .{ .index = s.index, .term = s.term, .crc = s.crc, .seq = s.seq, .halt_seq = s.halt_seq });
    }

    /// Whether a committed skip names `e`, all of it or one of its messages.
    pub fn skips(self: *const Skips, e: *const entry_mod.Entry) bool {
        return self.namesEntry(e.header.index, e.header.term, e.header.crc32c);
    }

    /// Whether a committed skip names the entry `index`, `term`, `crc`.
    pub fn namesEntry(self: *const Skips, index: u64, term: u64, crc: u32) bool {
        for (self.named.items) |n| {
            if (n.index == index and n.term == term and n.crc == crc) return true;
        }
        return false;
    }

    /// What the committed skips of inbound `e` skip, or null when none
    /// names it. More seqs than an inbound carries can't name its messages.
    pub fn inboundOf(self: *const Skips, e: *const entry_mod.Entry) ?InboundSkip {
        var out: InboundSkip = .{ .index = e.header.index };
        var any = false;
        for (self.named.items) |n| {
            if (n.index != e.header.index or n.term != e.header.term or n.crc != e.header.crc32c) continue;
            any = true;
            if (n.seq == 0) {
                out.whole = true;
                out.halt_seq = n.halt_seq;
            } else if (out.n < InboundSkip.MAX_SEQS) {
                out.seqs[out.n] = n.seq;
                out.n += 1;
            }
        }
        return if (any) out else null;
    }

    /// Whether `s` was proposed to get past a halt the set already got
    /// past: an earlier skip of its entry covers the message the group
    /// halted at when `s` was proposed (the one `s` names, where that isn't
    /// known). Two skips of one halt can both commit (a resubmit, two
    /// operators). A live shard takes the first and applies the inbound
    /// before it applies the second, and a boot meets them in the same log
    /// order. Taking the second, and widening the inbound by it, would hold
    /// a boot to more than a live replica skipped. So it skips nothing: a
    /// later halt of the inbound waits for a skip proposed at that halt.
    pub fn stale(self: *const Skips, s: *const Skip) bool {
        const at = if (s.halt_seq != 0) s.halt_seq else s.seq;
        if (at == 0) return false;
        for (self.named.items) |n| {
            if (n.index == s.index and n.term == s.term and n.crc == s.crc and (n.seq == 0 or n.seq == at)) return true;
        }
        return false;
    }

    /// Whether `s` is the one the set took for its halt: a row of its entry
    /// naming the same message, proposed at the same halt.
    pub fn took(self: *const Skips, s: *const Skip) bool {
        for (self.named.items) |n| {
            if (n.index == s.index and n.term == s.term and n.crc == s.crc and n.seq == s.seq and n.halt_seq == s.halt_seq) return true;
        }
        return false;
    }

    /// Whether the set already skips all that `s` does: its entry whole,
    /// or the one message it names.
    pub fn holds(self: *const Skips, s: *const Skip) bool {
        for (self.named.items) |n| {
            if (n.index == s.index and n.term == s.term and n.crc == s.crc and (n.seq == 0 or n.seq == s.seq)) return true;
        }
        return false;
    }
};

const testing = std.testing;

fn testEntry(index: u64, term: u64) entry_mod.Entry {
    var e = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, term, index, index, "x");
    e.header.crc32c = e.computeCrc();
    return e;
}

test "repair skip: a skip proposed at a halt the set got past is stale and adds nothing" {
    const allocator = testing.allocator;
    var set: Skips = .{};
    defer set.deinit(allocator);
    var halted = testEntry(7, 2);
    halted.header.entry_type = @backingInt(entry_mod.EntryType.channel_inbound);
    halted.header.crc32c = halted.computeCrc();
    var one = Skip.of(&halted, "");
    one.seq = 1;
    one.halt_seq = 1;
    try set.add(allocator, &one);
    // The whole entry, proposed at the same halt: stale.
    var whole = Skip.of(&halted, "");
    whole.halt_seq = 1;
    try testing.expect(set.stale(&whole));
    try set.add(allocator, &whole);
    try testing.expect(!set.inboundOf(&halted).?.whole);
    // Proposed at the next halt, message 2: taken.
    whole.halt_seq = 2;
    try testing.expect(!set.stale(&whole));
    try set.add(allocator, &whole);
    try testing.expect(set.inboundOf(&halted).?.whole);
    // A whole skip whose halt isn't known is never stale.
    var set2: Skips = .{};
    defer set2.deinit(allocator);
    try set2.add(allocator, &one);
    whole.halt_seq = 0;
    try testing.expect(!set2.stale(&whole));
}

test "repair skip: a skip round-trips through its entry, and names only its entry" {
    const halted = testEntry(7, 2);
    var s = Skip.of(&halted, "bad payload");
    s.seq = 5;
    s.halt_seq = 5;
    var buf: [Skip.MAX_PAYLOAD]u8 = undefined;
    const e = entry_mod.buildEntry(.repair_skip, entry_mod.Flags.NONE, 3, 9, 9, s.encode(&buf));
    const back = Skip.decode(&e).?;
    try testing.expectEqual(@as(u64, 7), back.index);
    try testing.expectEqual(@as(u64, 2), back.term);
    try testing.expectEqual(@as(u64, 5), back.seq);
    try testing.expectEqual(@as(u64, 5), back.halt_seq);
    try testing.expectEqualStrings("bad payload", back.reason);
    var set: Skips = .{};
    defer set.deinit(testing.allocator);
    try set.add(testing.allocator, &back);
    const in = set.inboundOf(&halted).?;
    try testing.expect(in.skips(5) and !in.skips(4) and !in.whole);
    try testing.expect(set.inboundOf(&testEntry(8, 2)) == null);
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

test "repair skip: an inbound's skips widen, a message at a time, then whole" {
    var set: Skips = .{};
    defer set.deinit(testing.allocator);
    const halted = testEntry(7, 2);
    var s = Skip.of(&halted, "");
    s.seq = 3;
    try set.add(testing.allocator, &s);
    // The next message to halt it is skipped too: the first doesn't hold it.
    s.seq = 4;
    try testing.expect(!set.holds(&s));
    try set.add(testing.allocator, &s);
    try set.add(testing.allocator, &s);
    try testing.expectEqual(@as(usize, 2), set.named.items.len);
    var in = set.inboundOf(&halted).?;
    try testing.expect(in.skips(3) and in.skips(4) and !in.skips(5) and !in.whole);
    // Then the whole entry, which holds every message after it.
    s.seq = 0;
    s.halt_seq = 5;
    try testing.expect(!set.holds(&s));
    try set.add(testing.allocator, &s);
    in = set.inboundOf(&halted).?;
    try testing.expect(in.whole and in.skips(5) and in.skips(9));
    try testing.expectEqual(@as(u64, 5), in.halt_seq);
    s.seq = 6;
    try testing.expect(set.holds(&s));
}
