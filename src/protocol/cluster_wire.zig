//! The cluster ops' requests and answers, coded by hand: the server encodes
//! and the CLI decodes through these functions alone, and a checked-in
//! golden vector pins each layout (`golden.zig`), so a layout can't change
//! without its vector, and with it the table hash.

const std = @import("std");

/// `cluster.status`'s answer.
pub const Status = struct {
    node_id: u32,
    leader_id: u32,
    term: u64,
    /// 0 follower, 1 candidate, 2 leader, 3 joining, 4 diverged, 5 guarded
    /// and catching up, 6 guarded and confirming the term, 7 replica,
    /// 8 removed.
    state: u8,
    member_count: u32,
    /// A committed membership config another member vouched for while this
    /// node is behind it; 0 for none.
    vouched_index: u64,
    /// Dropping frames from a node no config of its names, with no leader
    /// heard.
    strangers_while_quiet: bool,

    pub const SIZE = 30;

    pub fn encode(self: Status, buf: *[SIZE]u8) []const u8 {
        std.mem.writeInt(u32, buf[0..4], self.node_id, .little);
        std.mem.writeInt(u32, buf[4..8], self.leader_id, .little);
        std.mem.writeInt(u64, buf[8..16], self.term, .little);
        buf[16] = self.state;
        std.mem.writeInt(u32, buf[17..21], self.member_count, .little);
        std.mem.writeInt(u64, buf[21..29], self.vouched_index, .little);
        buf[29] = @intFromBool(self.strangers_while_quiet);
        return buf;
    }

    pub fn decode(data: []const u8) error{Malformed}!Status {
        if (data.len != SIZE or data[29] > 1) return error.Malformed;
        return .{
            .node_id = std.mem.readInt(u32, data[0..4], .little),
            .leader_id = std.mem.readInt(u32, data[4..8], .little),
            .term = std.mem.readInt(u64, data[8..16], .little),
            .state = data[16],
            .member_count = std.mem.readInt(u32, data[17..21], .little),
            .vouched_index = std.mem.readInt(u64, data[21..29], .little),
            .strangers_while_quiet = data[29] != 0,
        };
    }
};

/// The entry a member's shard is halted at.
pub const Halt = struct {
    index: u64,
    term: u64,
    crc: u32,
};

/// What the answering node knows of a member's halt.
pub const HaltSeen = union(enum) {
    none,
    at: Halt,
    /// Only a leader hears its members' halts; elsewhere each node knows
    /// only its own.
    unknown,
};

/// A row of `cluster.members`.
pub const Member = struct {
    id: u32,
    /// 0 voter, 1 replica, 2 joining (never caught up).
    role: u8,
    /// 0.0.0.0:0 where no link is up.
    ip4: [4]u8,
    port: u16,
    /// The leader's; all ones elsewhere, and for the answering node.
    contact_ago_ms: u32,
    match: u64,
    halt: HaltSeen,

    pub const SIZE = 44;
    pub const UNKNOWN_CONTACT = std.math.maxInt(u32);
    pub const UNKNOWN_MATCH = std.math.maxInt(u64);

    fn encode(self: Member, row: *[SIZE]u8) void {
        std.mem.writeInt(u32, row[0..4], self.id, .little);
        row[4] = self.role;
        @memcpy(row[5..9], &self.ip4);
        std.mem.writeInt(u16, row[9..11], self.port, .little);
        std.mem.writeInt(u32, row[11..15], self.contact_ago_ms, .little);
        std.mem.writeInt(u64, row[15..23], self.match, .little);
        const h: Halt = switch (self.halt) {
            .at => |at| at,
            .none, .unknown => .{ .index = 0, .term = 0, .crc = 0 },
        };
        row[23] = switch (self.halt) {
            .none => 0,
            .at => 1,
            .unknown => 2,
        };
        std.mem.writeInt(u64, row[24..32], h.index, .little);
        std.mem.writeInt(u64, row[32..40], h.term, .little);
        std.mem.writeInt(u32, row[40..44], h.crc, .little);
    }

    fn decode(row: *const [SIZE]u8) error{Malformed}!Member {
        const h: Halt = .{
            .index = std.mem.readInt(u64, row[24..32], .little),
            .term = std.mem.readInt(u64, row[32..40], .little),
            .crc = std.mem.readInt(u32, row[40..44], .little),
        };
        return .{
            .id = std.mem.readInt(u32, row[0..4], .little),
            .role = row[4],
            .ip4 = row[5..9].*,
            .port = std.mem.readInt(u16, row[9..11], .little),
            .contact_ago_ms = std.mem.readInt(u32, row[11..15], .little),
            .match = std.mem.readInt(u64, row[15..23], .little),
            .halt = switch (row[23]) {
                0 => .none,
                1 => .{ .at = h },
                2 => .unknown,
                else => return error.Malformed,
            },
        };
    }
};

/// A removed id, and when it was removed.
pub const Removed = struct {
    id: u32,
    when_ms: u64,

    pub const SIZE = 12;
};

/// `cluster.members`'s answer: `[leader:u32][commit:u64][members:u8]`, the
/// member rows, `[removed:u8]`, the removed rows.
pub const Members = struct {
    leader: u32,
    commit: u64,
    rows: []const u8,
    removed_rows: []const u8,

    const HEAD = 13;

    pub fn maxSize(member_count: usize, removed_count: usize) usize {
        return HEAD + member_count * Member.SIZE + 1 + removed_count * Removed.SIZE;
    }

    pub fn encode(buf: []u8, leader: u32, commit: u64, members: []const Member, gone: []const Removed) []const u8 {
        std.debug.assert(members.len <= std.math.maxInt(u8) and gone.len <= std.math.maxInt(u8));
        std.mem.writeInt(u32, buf[0..4], leader, .little);
        std.mem.writeInt(u64, buf[4..12], commit, .little);
        buf[12] = @intCast(members.len);
        var off: usize = HEAD;
        for (members) |m| {
            m.encode(buf[off..][0..Member.SIZE]);
            off += Member.SIZE;
        }
        buf[off] = @intCast(gone.len);
        off += 1;
        for (gone) |r| {
            std.mem.writeInt(u32, buf[off..][0..4], r.id, .little);
            std.mem.writeInt(u64, buf[off + 4 ..][0..8], r.when_ms, .little);
            off += Removed.SIZE;
        }
        return buf[0..off];
    }

    /// The answer's head and rows; every row decodes, or none does.
    pub fn decode(data: []const u8) error{Malformed}!Members {
        if (data.len < HEAD + 1) return error.Malformed;
        const n: usize = data[12];
        const removed_at = HEAD + n * Member.SIZE;
        if (data.len < removed_at + 1) return error.Malformed;
        const gone: usize = data[removed_at];
        if (data.len != removed_at + 1 + gone * Removed.SIZE) return error.Malformed;
        const m: Members = .{
            .leader = std.mem.readInt(u32, data[0..4], .little),
            .commit = std.mem.readInt(u64, data[4..12], .little),
            .rows = data[HEAD..removed_at],
            .removed_rows = data[removed_at + 1 ..],
        };
        for (0..m.count()) |i| _ = try m.member(i);
        return m;
    }

    pub fn count(self: Members) usize {
        return self.rows.len / Member.SIZE;
    }

    pub fn member(self: Members, i: usize) error{Malformed}!Member {
        return Member.decode(self.rows[i * Member.SIZE ..][0..Member.SIZE]);
    }

    pub fn removedCount(self: Members) usize {
        return self.removed_rows.len / Removed.SIZE;
    }

    pub fn removed(self: Members, i: usize) Removed {
        const r = self.removed_rows[i * Removed.SIZE ..][0..Removed.SIZE];
        return .{ .id = std.mem.readInt(u32, r[0..4], .little), .when_ms = std.mem.readInt(u64, r[4..12], .little) };
    }
};

/// `cluster.promote`'s request: the replica to make a voter.
pub const Promote = struct {
    id: u32,

    pub const SIZE = 4;

    pub fn encode(self: Promote, buf: *[SIZE]u8) []const u8 {
        std.mem.writeInt(u32, buf, self.id, .little);
        return buf;
    }

    pub fn decode(data: []const u8) error{Malformed}!Promote {
        if (data.len != SIZE) return error.Malformed;
        return .{ .id = std.mem.readInt(u32, data[0..4], .little) };
    }
};

/// `cluster.remove`'s request: the member, and whether to remove it even
/// when that leaves an even number of voters, or two.
pub const Remove = struct {
    id: u32,
    yes: bool,

    pub const SIZE = 5;

    pub fn encode(self: Remove, buf: *[SIZE]u8) []const u8 {
        std.mem.writeInt(u32, buf[0..4], self.id, .little);
        buf[4] = @intFromBool(self.yes);
        return buf;
    }

    pub fn decode(data: []const u8) error{Malformed}!Remove {
        if (data.len != SIZE or data[4] > 1) return error.Malformed;
        return .{ .id = std.mem.readInt(u32, data[0..4], .little), .yes = data[4] != 0 };
    }
};

/// `cluster.repair_skip`'s request: the entry the group halted at, whether
/// to skip it (without, the leader says what it would do and changes
/// nothing), and why.
pub const RepairSkip = struct {
    index: u64,
    yes: bool,
    reason: []const u8 = "",

    pub const MAX_REASON = 256;
    const FIXED = 9;
    pub const MAX_SIZE = FIXED + MAX_REASON;

    pub fn encode(self: RepairSkip, buf: *[MAX_SIZE]u8) error{Malformed}![]const u8 {
        if (self.reason.len > MAX_REASON) return error.Malformed;
        std.mem.writeInt(u64, buf[0..8], self.index, .little);
        buf[8] = @intFromBool(self.yes);
        @memcpy(buf[FIXED..][0..self.reason.len], self.reason);
        return buf[0 .. FIXED + self.reason.len];
    }

    pub fn decode(data: []const u8) error{Malformed}!RepairSkip {
        if (data.len < FIXED or data.len > MAX_SIZE or data[8] > 1) return error.Malformed;
        return .{ .index = std.mem.readInt(u64, data[0..8], .little), .yes = data[8] != 0, .reason = data[FIXED..] };
    }
};

/// `cluster.repair_skip`'s answer: whether the skip committed (else this
/// was the dry run), its index, the entry it names and what that entry
/// is, and the voters the leader heard halted at it.
pub const RepairSkipAnswer = struct {
    skipped: bool,
    /// The skip's own index; 0 in a dry run.
    skip_index: u64,
    entry: Halt,
    entry_type: u8,
    entry_size: u32,
    voters: u8,
    halted: []const u32,

    const FIXED = 1 + 8 + 8 + 8 + 4 + 1 + 4 + 1 + 1;

    pub fn maxSize(halted: usize) usize {
        return FIXED + halted * 4;
    }

    pub fn encode(self: RepairSkipAnswer, buf: []u8) []const u8 {
        std.debug.assert(self.halted.len <= std.math.maxInt(u8));
        buf[0] = @intFromBool(self.skipped);
        std.mem.writeInt(u64, buf[1..9], self.skip_index, .little);
        std.mem.writeInt(u64, buf[9..17], self.entry.index, .little);
        std.mem.writeInt(u64, buf[17..25], self.entry.term, .little);
        std.mem.writeInt(u32, buf[25..29], self.entry.crc, .little);
        buf[29] = self.entry_type;
        std.mem.writeInt(u32, buf[30..34], self.entry_size, .little);
        buf[34] = self.voters;
        buf[35] = @intCast(self.halted.len);
        for (self.halted, 0..) |id, i| std.mem.writeInt(u32, buf[FIXED + i * 4 ..][0..4], id, .little);
        return buf[0 .. FIXED + self.halted.len * 4];
    }

    /// The ids land in `ids`.
    pub fn decode(data: []const u8, ids: []u32) error{Malformed}!RepairSkipAnswer {
        if (data.len < FIXED or data[0] > 1) return error.Malformed;
        const n: usize = data[35];
        if (data.len != FIXED + n * 4 or n > ids.len) return error.Malformed;
        for (0..n) |i| ids[i] = std.mem.readInt(u32, data[FIXED + i * 4 ..][0..4], .little);
        return .{
            .skipped = data[0] != 0,
            .skip_index = std.mem.readInt(u64, data[1..9], .little),
            .entry = .{
                .index = std.mem.readInt(u64, data[9..17], .little),
                .term = std.mem.readInt(u64, data[17..25], .little),
                .crc = std.mem.readInt(u32, data[25..29], .little),
            },
            .entry_type = data[29],
            .entry_size = std.mem.readInt(u32, data[30..34], .little),
            .voters = data[34],
            .halted = ids[0..n],
        };
    }
};
