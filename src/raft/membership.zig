//! The payload of a `raft_config` entry: every member and its role, and
//! the ids removed from the group. The leader that writes it and every node
//! that applies it read the same bytes, so membership is exactly what the
//! log says.
//!
//! A member is a voter or a replica. A replica takes the log and counts
//! toward nothing: no quorum, no vote. `may_vote` says the leader may make
//! it a voter on its own once it has caught up; `caught_up` says it once
//! has, so a later leader can tell a joiner that never got there (aged out)
//! from a replica that has fallen behind (kept).

const std = @import("std");
const node_mod = @import("node.zig");

pub const MAX_MEMBERS = node_mod.MAX_PEERS + 1;
/// Removed ids kept, newest last; an older one may join again.
pub const MAX_REMOVED = 64;
/// A member that has never caught up is removed after this long.
pub const JOIN_AGE_LIMIT_MS: u64 = 10 * 60 * 1000;

const MEMBER_SIZE = 5;
const REMOVED_SIZE = 12;
pub const MAX_SIZE = 2 + MAX_MEMBERS * MEMBER_SIZE + MAX_REMOVED * REMOVED_SIZE;

const VOTER: u8 = 1;
const MAY_VOTE: u8 = 2;
const CAUGHT_UP: u8 = 4;

pub const Member = struct {
    id: u32,
    voter: bool,
    may_vote: bool = false,
    caught_up: bool = false,
};

pub const Removed = struct {
    id: u32,
    /// The removing leader's clock.
    when_ms: u64,
};

pub const Config = struct {
    members: [MAX_MEMBERS]Member = undefined,
    member_count: u8 = 0,
    removed: [MAX_REMOVED]Removed = undefined,
    removed_count: u8 = 0,

    /// Every id a voter: a group founded or forced into being, where each
    /// node named has the log already.
    pub fn ofVoters(voter_ids: []const u32) Config {
        var c: Config = .{};
        for (voter_ids[0..@min(voter_ids.len, MAX_MEMBERS)]) |id| {
            c.members[c.member_count] = .{ .id = id, .voter = true, .may_vote = true, .caught_up = true };
            c.member_count += 1;
        }
        return c;
    }

    pub fn memberSlice(self: *const Config) []const Member {
        return self.members[0..self.member_count];
    }

    pub fn removedSlice(self: *const Config) []const Removed {
        return self.removed[0..self.removed_count];
    }

    pub fn find(self: *const Config, id: u32) ?Member {
        for (self.memberSlice()) |m| if (m.id == id) return m;
        return null;
    }

    pub fn names(self: *const Config, id: u32) bool {
        return self.find(id) != null;
    }

    pub fn isVoter(self: *const Config, id: u32) bool {
        return if (self.find(id)) |m| m.voter else false;
    }

    pub fn voterCount(self: *const Config) u8 {
        var n: u8 = 0;
        for (self.memberSlice()) |m| n += @intFromBool(m.voter);
        return n;
    }

    pub fn ids(self: *const Config, out: *[MAX_MEMBERS]u32) []u32 {
        for (self.memberSlice(), 0..) |m, i| out[i] = m.id;
        return out[0..self.member_count];
    }

    pub fn voterIds(self: *const Config, out: *[MAX_MEMBERS]u32) []u32 {
        var n: usize = 0;
        for (self.memberSlice()) |m| {
            if (!m.voter) continue;
            out[n] = m.id;
            n += 1;
        }
        return out[0..n];
    }

    /// When `id` was removed, if the group still remembers it.
    pub fn removedAt(self: *const Config, id: u32) ?u64 {
        for (self.removedSlice()) |r| if (r.id == id) return r.when_ms;
        return null;
    }

    /// This config with `id` added as a replica. The caller checks there is
    /// room.
    pub fn withJoiner(self: Config, id: u32, may_vote: bool) Config {
        std.debug.assert(self.member_count < MAX_MEMBERS);
        var c = self;
        c.members[c.member_count] = .{ .id = id, .voter = false, .may_vote = may_vote };
        c.member_count += 1;
        return c;
    }

    /// This config with `m` replacing the member of its id.
    pub fn withMember(self: Config, m: Member) Config {
        var c = self;
        for (c.members[0..c.member_count]) |*slot| {
            if (slot.id == m.id) slot.* = m;
        }
        return c;
    }

    /// This config without `id`; with `when_ms`, remembered as removed so
    /// it cannot join again.
    pub fn without(self: Config, id: u32, when_ms: ?u64) Config {
        var c = self;
        var n: u8 = 0;
        for (self.memberSlice()) |m| {
            if (m.id == id) continue;
            c.members[n] = m;
            n += 1;
        }
        c.member_count = n;
        if (when_ms) |when| {
            if (c.removed_count == MAX_REMOVED) {
                std.mem.copyForwards(Removed, c.removed[0 .. MAX_REMOVED - 1], c.removed[1..]);
                c.removed_count -= 1;
            }
            c.removed[c.removed_count] = .{ .id = id, .when_ms = when };
            c.removed_count += 1;
        }
        return c;
    }

    pub fn eql(a: *const Config, b: *const Config) bool {
        if (a.member_count != b.member_count or a.removed_count != b.removed_count) return false;
        for (a.memberSlice(), b.memberSlice()) |x, y| if (!std.meta.eql(x, y)) return false;
        for (a.removedSlice(), b.removedSlice()) |x, y| if (!std.meta.eql(x, y)) return false;
        return true;
    }
};

/// `[members:u8][removed:u8]`, then per member `[id:u32][flags:u8]`, then
/// per removed id `[id:u32][when_ms:u64]`; little-endian.
pub fn encode(cfg: *const Config, buf: *[MAX_SIZE]u8) []const u8 {
    buf[0] = cfg.member_count;
    buf[1] = cfg.removed_count;
    var off: usize = 2;
    for (cfg.memberSlice()) |m| {
        std.mem.writeInt(u32, buf[off..][0..4], m.id, .little);
        buf[off + 4] = (if (m.voter) VOTER else 0) | (if (m.may_vote) MAY_VOTE else 0) | (if (m.caught_up) CAUGHT_UP else 0);
        off += MEMBER_SIZE;
    }
    for (cfg.removedSlice()) |r| {
        std.mem.writeInt(u32, buf[off..][0..4], r.id, .little);
        std.mem.writeInt(u64, buf[off + 4 ..][0..8], r.when_ms, .little);
        off += REMOVED_SIZE;
    }
    return buf[0..off];
}

/// Null for bytes that are not a config: counts the bytes do not cover, no
/// voter, more members than a group holds, an id of 0, an id named twice or
/// both a member and removed, or a flag this version does not know. A group
/// with no voter can never commit; a member named twice holds two peer
/// slots and a quorum it can never fill.
pub fn decode(payload: []const u8) ?Config {
    if (payload.len < 2) return null;
    var c: Config = .{ .member_count = payload[0], .removed_count = payload[1] };
    if (c.member_count > MAX_MEMBERS or c.removed_count > MAX_REMOVED) return null;
    if (payload.len != 2 + @as(usize, c.member_count) * MEMBER_SIZE + @as(usize, c.removed_count) * REMOVED_SIZE) return null;
    var off: usize = 2;
    for (0..c.member_count) |i| {
        const id = std.mem.readInt(u32, payload[off..][0..4], .little);
        const flags = payload[off + 4];
        if (id == 0 or flags & ~(VOTER | MAY_VOTE | CAUGHT_UP) != 0) return null;
        for (c.members[0..i]) |m| if (m.id == id) return null;
        c.members[i] = .{ .id = id, .voter = flags & VOTER != 0, .may_vote = flags & MAY_VOTE != 0, .caught_up = flags & CAUGHT_UP != 0 };
        off += MEMBER_SIZE;
    }
    for (0..c.removed_count) |i| {
        const id = std.mem.readInt(u32, payload[off..][0..4], .little);
        if (id == 0 or c.names(id)) return null;
        c.removed[i] = .{ .id = id, .when_ms = std.mem.readInt(u64, payload[off + 4 ..][0..8], .little) };
        off += REMOVED_SIZE;
    }
    if (c.voterCount() == 0) return null;
    return c;
}

/// Whether `id` is one of `members`.
pub fn names(members: []const u32, id: u32) bool {
    for (members) |m| if (m == id) return true;
    return false;
}

/// Why a config may not follow the one before it.
pub const Refusal = enum {
    no_voter,
    too_many_members,
    voters_jump,
    rejoined_removed,
    not_a_voter,

    pub fn message(self: Refusal) []const u8 {
        return switch (self) {
            .no_voter => "a group needs at least one voter",
            .too_many_members => "the group is full",
            .voters_jump => "voters change one at a time",
            .rejoined_removed => "that node was removed from this group",
            .not_a_voter => "a leader that is not a voter makes no membership change",
        };
    }
};

/// A change one config may make to the last, proposed by `proposer`:
/// voters change by one, so any majority of the old set meets any majority
/// of the new. The exception is a lone voter promoting two. A majority of
/// the three ({a, b}) need not include it, so the two configs share no
/// majority; it is safe only because the old config's one voter is the
/// proposer, which takes the new config on disk before it acts and never
/// counts by the old one again, and no other node can win by the old one.
/// Proposed by anyone else, the lone voter could go on committing alone
/// beside the two. A removed id stays out.
pub fn checkChange(prev: *const Config, next: *const Config, proposer: u32) ?Refusal {
    if (next.voterCount() == 0) return .no_voter;
    if (next.member_count > MAX_MEMBERS) return .too_many_members;
    for (next.memberSlice()) |m| {
        if (prev.removedAt(m.id) != null) return .rejoined_removed;
    }
    var added: u8 = 0;
    var dropped: u8 = 0;
    for (next.memberSlice()) |m| {
        if (m.voter and !prev.isVoter(m.id)) added += 1;
    }
    for (prev.memberSlice()) |m| {
        if (m.voter and !next.isVoter(m.id)) dropped += 1;
    }
    if (added + dropped <= 1) return null;
    if (prev.voterCount() == 1 and prev.isVoter(proposer) and dropped == 0 and added == 2) return null;
    return .voters_jump;
}

/// What the leader knows of a member it replicates to.
pub const Progress = struct {
    id: u32,
    /// Matched the commit index on enough heartbeats in a row.
    caught_up_now: bool,
    /// When this leader first saw it as a member that had not caught up.
    joining_since_ms: u64,
};

/// The change the leader makes on its own, if any, one per config: a
/// replica that has caught up is marked so, and made a voter when it may
/// and that turns an even count odd; a lone voter makes two at once; a
/// joiner that never caught up is dropped after `JOIN_AGE_LIMIT_MS`. So
/// the leader grows a group to three voters and no further; past that it
/// only restores an odd count after a removal, which `voters_before` (the
/// voter count before the last change of it) tells apart from an
/// operator's promotion.
pub fn nextAutomatic(cfg: *const Config, progress: []const Progress, now_ms: u64, voters_before: ?u8) ?Config {
    const voters = cfg.voterCount();
    var eligible: [2]u32 = undefined;
    var eligible_n: usize = 0;
    for (progress) |p| {
        const m = cfg.find(p.id) orelse continue;
        // Caught up now, not once: a replica that fell behind, or lost its
        // disk and is guarded, would be a voter that slows or blocks commit.
        if (m.voter or !m.may_vote or !p.caught_up_now) continue;
        if (eligible_n < 2) {
            eligible[eligible_n] = p.id;
            eligible_n += 1;
        }
    }
    if (voters == 1 and eligible_n == 2) {
        var c = cfg.*;
        for (eligible) |id| {
            var m = c.find(id).?;
            m.voter = true;
            m.caught_up = true;
            c = c.withMember(m);
        }
        return c;
    }
    const after_removal = if (voters_before) |b| b > voters else false;
    if (eligible_n > 0 and voters % 2 == 0 and (voters < 3 or after_removal)) {
        var m = cfg.find(eligible[0]).?;
        m.voter = true;
        m.caught_up = true;
        return cfg.withMember(m);
    }
    for (progress) |p| {
        const m = cfg.find(p.id) orelse continue;
        if (m.caught_up or !p.caught_up_now) continue;
        var marked = m;
        marked.caught_up = true;
        return cfg.withMember(marked);
    }
    for (progress) |p| {
        const m = cfg.find(p.id) orelse continue;
        if (m.caught_up or m.voter) continue;
        if (now_ms -| p.joining_since_ms >= JOIN_AGE_LIMIT_MS) return cfg.without(p.id, null);
    }
    return null;
}

const testing = std.testing;

test "membership: a config round-trips, and bytes that are not one are refused" {
    var buf: [MAX_SIZE]u8 = undefined;
    var c = Config.ofVoters(&.{ 3, 1 }).withJoiner(2, true).without(9, 1234);
    const bytes = encode(&c, &buf);
    try testing.expectEqual(@as(usize, 2 + 3 * 5 + 12), bytes.len);
    const back = decode(bytes).?;
    try testing.expect(back.eql(&c));
    try testing.expect(back.isVoter(3) and !back.isVoter(2) and back.names(2));
    try testing.expectEqual(@as(?u64, 1234), back.removedAt(9));

    // No voter.
    c = Config.ofVoters(&.{1});
    c.members[0].voter = false;
    try testing.expect(decode(encode(&c, &buf)) == null);
    // Named twice.
    try testing.expect(decode(encode(&Config.ofVoters(&.{ 1, 2, 1 }), &buf)) == null);
    // Both a member and removed.
    try testing.expect(decode(encode(&Config.ofVoters(&.{ 1, 2 }).without(3, 5).withJoiner(3, false), &buf)) == null);
    // Short, long, an unknown flag, an id of 0.
    const good = encode(&Config.ofVoters(&.{ 1, 2 }), &buf);
    var copy: [MAX_SIZE]u8 = undefined;
    @memcpy(copy[0..good.len], good);
    try testing.expect(decode(copy[0 .. good.len - 1]) == null);
    copy[good.len] = 0;
    try testing.expect(decode(copy[0 .. good.len + 1]) == null);
    copy[6] |= 0x80;
    try testing.expect(decode(copy[0..good.len]) == null);
    copy[6] &= 0x7f;
    std.mem.writeInt(u32, copy[2..6], 0, .little);
    try testing.expect(decode(copy[0..good.len]) == null);
    try testing.expect(decode("") == null);
}

test "membership: voters change one at a time, except a lone voter making two; removed ids stay out" {
    const one = Config.ofVoters(&.{1});
    const three = Config.ofVoters(&.{ 1, 2, 3 });
    try testing.expectEqual(@as(?Refusal, null), checkChange(&one, &one.withJoiner(2, true), 1));
    try testing.expectEqual(@as(?Refusal, null), checkChange(&one, &three, 1));
    // Two at once only from the lone voter itself: proposed by another, the
    // lone voter would go on committing alone beside the new pair.
    const lone2 = Config.ofVoters(&.{2}).withJoiner(3, true).withJoiner(4, true);
    try testing.expectEqual(@as(?Refusal, .voters_jump), checkChange(&lone2, &Config.ofVoters(&.{ 2, 3, 4 }), 1));
    try testing.expectEqual(@as(?Refusal, null), checkChange(&lone2, &Config.ofVoters(&.{ 2, 3, 4 }), 2));
    try testing.expectEqual(@as(?Refusal, null), checkChange(&three, &three.without(3, 7), 1));
    try testing.expectEqual(@as(?Refusal, .voters_jump), checkChange(&three, &three.without(3, null).without(2, null), 1));
    try testing.expectEqual(@as(?Refusal, .voters_jump), checkChange(&Config.ofVoters(&.{ 1, 2 }), &Config.ofVoters(&.{ 1, 3, 4 }), 1));
    try testing.expectEqual(@as(?Refusal, .no_voter), checkChange(&one, &one.withJoiner(2, true).without(1, null), 1));
    const gone = three.without(3, 7);
    try testing.expectEqual(@as(?Refusal, .rejoined_removed), checkChange(&gone, &gone.withJoiner(3, true), 1));
}

test "membership: the leader promotes caught-up replicas into odd counts, two at once from one, and ages out joiners" {
    const one = Config.ofVoters(&.{1}).withJoiner(2, true).withJoiner(3, true);
    // Neither caught up yet: nothing, until the age limit.
    const waiting = [_]Progress{ .{ .id = 2, .caught_up_now = false, .joining_since_ms = 0 }, .{ .id = 3, .caught_up_now = false, .joining_since_ms = 0 } };
    try testing.expect(nextAutomatic(&one, &waiting, 1000, null) == null);
    const aged = nextAutomatic(&one, &waiting, JOIN_AGE_LIMIT_MS, null).?;
    try testing.expect(!aged.names(2) and aged.removedAt(2) == null);

    // One caught up: marked, not promoted (two voters would tolerate no more).
    const half = [_]Progress{ .{ .id = 2, .caught_up_now = true, .joining_since_ms = 0 }, .{ .id = 3, .caught_up_now = false, .joining_since_ms = 0 } };
    const marked = nextAutomatic(&one, &half, 1000, null).?;
    try testing.expect(marked.find(2).?.caught_up and !marked.isVoter(2));
    try testing.expect(nextAutomatic(&marked, &half, 1000, null) == null);

    // Both: one entry makes three voters.
    const both = [_]Progress{ .{ .id = 2, .caught_up_now = true, .joining_since_ms = 0 }, .{ .id = 3, .caught_up_now = true, .joining_since_ms = 0 } };
    const three = nextAutomatic(&marked, &both, 1000, null).?;
    try testing.expectEqual(@as(u8, 3), three.voterCount());

    // A fourth that may vote stays a replica at three voters; one that
    // may not is never promoted, even to restore an odd count.
    const four = three.withJoiner(4, true);
    const p4 = [_]Progress{.{ .id = 4, .caught_up_now = true, .joining_since_ms = 0 }};
    const four_marked = nextAutomatic(&four, &p4, 1000, null).?;
    try testing.expect(four_marked.find(4).?.caught_up and !four_marked.isVoter(4));
    try testing.expect(nextAutomatic(&four_marked, &p4, 1000, null) == null);
    const even = four_marked.without(3, 9);
    try testing.expect(nextAutomatic(&even, &p4, 1000, null).?.isVoter(4));
    var plain = even.withMember(.{ .id = 4, .voter = false, .may_vote = false, .caught_up = true });
    try testing.expect(nextAutomatic(&plain, &p4, 1000, null) == null);
    // Caught up once is not enough to be promoted: it must be caught up now.
    const behind_now = [_]Progress{.{ .id = 4, .caught_up_now = false, .joining_since_ms = 0 }};
    try testing.expect(nextAutomatic(&even, &behind_now, 1000, null) == null);
    // Past three, an even count is made odd again only after a removal: an
    // operator promoting a fourth gets no fifth on top.
    var four_voters = Config.ofVoters(&.{ 1, 2, 3, 4 }).withJoiner(5, true);
    const p5 = [_]Progress{.{ .id = 5, .caught_up_now = true, .joining_since_ms = 0 }};
    four_voters = nextAutomatic(&four_voters, &p5, 1000, 3).?; // marks 5 caught up
    try testing.expect(!four_voters.isVoter(5));
    try testing.expect(nextAutomatic(&four_voters, &p5, 1000, 3) == null);
    try testing.expect(nextAutomatic(&four_voters, &p5, 1000, 5).?.isVoter(5));
    // A replica that once caught up is never aged out, however far behind.
    const behind = [_]Progress{.{ .id = 4, .caught_up_now = false, .joining_since_ms = 0 }};
    plain = plain.withMember(.{ .id = 4, .voter = false, .may_vote = false, .caught_up = true });
    try testing.expect(nextAutomatic(&plain, &behind, JOIN_AGE_LIMIT_MS * 10, null) == null);
}
