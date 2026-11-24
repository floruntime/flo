//! The per-node memory budget, derived from the committed cluster-settings
//! family (`settings/machine.zig`) joined with the data-holding membership.
//! Each shard's leader binds its share of it, and the shard holds its pool
//! to that share (`budget/gate.zig`, `channels.sinkRoom`). The derivation
//! itself records nothing in applied state.
//!
//! budget = min over every data-holding member WITH a settings row of
//!          pool_share × (ram_bytes − reserve). A data-holder with no row yet
//!          makes the budget provisional; the caller freezes at its last
//!          non-provisional value, never excludes it — excluding a missing
//!          small node keeps the min high, so the node over-commits.

const std = @import("std");
const pool = @import("../budget/pool.zig");
const channels = @import("../contract/channels.zig");
const channel_driver = @import("../contract/channel_driver.zig");
const ce = @import("../protocol/channel_entries.zig");
const ae = @import("../protocol/action_entries.zig");
const he = @import("../protocol/history_entries.zig");
const hp = @import("../stream/history_producer.zig");
const hm = @import("../stream/history_machine.zig");
const action_machine = @import("../actions/machine.zig");
const runs_machine = @import("../workflow/runs_machine.zig");
const schema = @import("../protocol/schema.zig");

/// The fixed overhead a shard holds before client data, measured ~5 MiB after
/// flo#461/#462 (rings resident only as written). Scales with the node's shard
/// count. Part of a node's own `reserve`, reported in its settings row.
pub const PER_SHARD_FLOOR_BYTES: u64 = 5 * 1024 * 1024;

/// A shard's configured rings, held in the reserve at full size (the steady
/// state of a busy shard): the log hot ring plus the Raft ring,
/// `raftRingCapacity(hot) = clamp(hot/4, 2 MiB, 16 MiB)` — 80 MiB at the 64 MiB
/// default, 20 MiB at a 16 MiB hot buffer. A deterministic function of the
/// committed `[storage] hot_buffer_capacity`.
pub fn ringBytesPerShard(hot_buffer_capacity: u64) u64 {
    const raft = std.math.clamp(hot_buffer_capacity / 4, 2 * 1024 * 1024, 16 * 1024 * 1024);
    return hot_buffer_capacity +| raft;
}

/// What one workflow's call of an action adds to the action's shard beyond
/// the bytes the call carries, and a second copy of its input, which its
/// history holds: its run and ready records (their keys, the run's own
/// fields, `RESIDENT_RECORD` each), and its first history event's record
/// overhead, with the chunk it may go in at once and that chunk's outbox
/// rows (the row, and on a first send the lane's meta row and its
/// per-namespace count). Pinned by an actions test with every field at its
/// largest.
pub const CALL_RECORDS_BYTES: u64 = 2 * pool.RESIDENT_RECORD + 1024 + CALL_HISTORY_BYTES;
const CALL_HISTORY_BYTES: u64 = (he.Chunk.max_size - he.MAX_CHUNK_RECORDS) + hp.recordBound(action_machine.EV_INVOKED.len, 0, 0) +
    3 * pool.RESIDENT_RECORD + channels.OUT_KEY + channels.CHAN_KEY + channels.OUT_NS_KEY + 3 * 64;
/// What its answer to the caller takes: the outbox row, and, on a first send
/// to that caller's group or for that namespace, the channel's meta row and
/// its per-namespace byte count (each value under 64 bytes).
pub const CALL_REPLY_BYTES: u64 = 3 * pool.RESIDENT_RECORD + channels.OUT_KEY + channels.CHAN_KEY + channels.OUT_NS_KEY + 3 * 64 + ae.Reply.max_size;

/// How far one channel inbound can take its shard past its share. A
/// workflow's call that arrives inside an inbound is held to the share
/// against the use committed before the inbound, its own rows alone, so
/// every call of one inbound may land though together they pass it; the
/// next inbound's calls are then refused. An inbound carries at most
/// `MAX_BATCH` messages and `MAX_BATCH_BYTES` of payload, its first
/// message whatever its size (a call's is far smaller). Its payload counts
/// twice: each call's input is in its run and in its history.
/// The history lane's own inbound may land past the share beside it: each
/// of its messages adds at most its run's rows and a fold
/// (`HISTORY_LAND_BYTES`); the events themselves stay in the log.
///
/// A message's term is an action's call, the most any message adds: a
/// child workflow's start and a call's answer or end each add less, pinned
/// below. Anything else a channel carries that grows a charged family must
/// be pinned here too.
pub const INBOUND_OVERSHOOT_BYTES: u64 = 2 * channel_driver.MAX_BATCH_BYTES + ce.MAX_BATCH * MESSAGE_BYTES;
const MESSAGE_BYTES: u64 = CALL_RECORDS_BYTES + CALL_REPLY_BYTES + HISTORY_LAND_BYTES;
/// A child's start: its parent record, whose fields are its payload's,
/// which the payload term counts, so only the record's key and overhead;
/// or, held, its answer.
const CHILD_START_BYTES: u64 = @max(pool.RESIDENT_RECORD + @sizeOf(u32) + ae.MAX_RUN_ID, CALL_REPLY_BYTES);
/// A call's answer or end, on the caller's shard: its row replaced, only
/// the outcome growing (an end's result is its payload).
const CALL_ANSWER_BYTES: u64 = @max(ae.MAX_SKIP_WHY, ae.MAX_OUTCOME);

comptime {
    std.debug.assert(CHILD_START_BYTES <= MESSAGE_BYTES);
    std.debug.assert(CALL_ANSWER_BYTES <= MESSAGE_BYTES);
    // The parent record is no larger than the start it copies: the payload
    // fields it drops (namespace, run) cover the state it adds.
    std.debug.assert(runs_machine.Parent.max_size <= ae.ChildStart.max_size);
    std.debug.assert(schema.Enum(runs_machine.ParentState).max_size <= ae.NamespaceId.max_size + ae.RunId.max_size);
}

/// The most one history message adds: a run's last chunk lands its live,
/// final and marker rows, each its key (namespace, owner, run and
/// incarnation, with its place in the log) and a row, and sends the fold
/// of its rows to itself, an outbox row with its channel's meta and
/// per-namespace rows (each value under 64 bytes).
const HISTORY_ROW_BYTES: u64 = pool.RESIDENT_RECORD + 4 + he.MAX_OWNER + he.MAX_RUN + 64 + hm.Row.max_size;
const HISTORY_LAND_BYTES: u64 = 3 * HISTORY_ROW_BYTES + 3 * pool.RESIDENT_RECORD + channels.OUT_KEY + channels.CHAN_KEY + channels.OUT_NS_KEY + 3 * 64 + he.Index.max_size;

/// What one admitted start sends, which its verdict can't weigh: sends are
/// written after the machine's verdict. At most one history chunk and, for
/// a child, its parent's answer, each with its outbox rows. The next entry's
/// verdict reads them committed, so a shard passes its share by one entry's
/// sends at most.
pub const OWN_SENDS_BYTES: u64 = he.Chunk.max_size + 3 * pool.RESIDENT_RECORD + channels.OUT_KEY + channels.CHAN_KEY + channels.OUT_NS_KEY + 3 * 64 + CALL_REPLY_BYTES;

/// A node's reserve: per shard, its floor, its configured ring sizes, the
/// one inbound it may take past its share and one start's sends; plus a
/// margin. Held back before
/// the pool's share of RAM is taken, so a shard's overshoot fits whatever
/// share an admin sets, up to all of it. A node computes this from its own
/// config and reports it in its settings row; the cluster takes it as given.
/// The margin covers the ~10 MiB a node carries once at low shard counts, so
/// the linear `floor × shards` holds there too.
pub fn reserve(shard_count: u32, ring_bytes_per_shard: u64, margin: u64) u64 {
    return @as(u64, shard_count) * (PER_SHARD_FLOOR_BYTES +| ring_bytes_per_shard +| INBOUND_OVERSHOOT_BYTES +| OWN_SENDS_BYTES) +| margin;
}

/// One data-holding member, for the derivation. `mem` is null when the node
/// has not reported its settings row yet.
pub const Holder = struct {
    node_id: u32,
    mem: ?NodeMem = null,

    pub const NodeMem = struct { ram_bytes: u64, reserve: u64 };
};

pub const Budget = struct {
    /// The min over the data-holders that have reported a row, in bytes. The
    /// caller freezes this at its last non-provisional value when
    /// `provisional_node` is set.
    bytes: u64,
    /// The data-holder whose share set the min — the node a refusal names, and
    /// the one an operator raises to lift the budget. Null only when no holder
    /// has reported a row.
    governing_node: ?u32,
    /// The lowest-id data-holder with no row yet, or null when all have one.
    provisional_node: ?u32,
};

/// Derive the budget over `holders`. Returns the min over reported rows, the
/// node that set it, and separately the first unreported data-holder so the
/// caller can freeze. A node whose reserve meets or exceeds its RAM
/// contributes 0. Ties go to the lower id, so every replica names the same
/// governing node.
pub fn derive(share_permille: u16, holders: []const Holder) Budget {
    var min_bytes: ?u64 = null;
    var governing: ?u32 = null;
    var provisional: ?u32 = null;
    for (holders) |h| {
        const m = h.mem orelse {
            if (provisional == null or h.node_id < provisional.?) provisional = h.node_id;
            continue;
        };
        const after = if (m.ram_bytes > m.reserve) m.ram_bytes - m.reserve else 0;
        // `share_permille` is capped at 1000 where it's committed; the multiply
        // saturates regardless, so no RAM figure can overflow the budget.
        const b = after *| share_permille / 1000;
        if (min_bytes == null or b < min_bytes.? or (b == min_bytes.? and h.node_id < governing.?)) {
            min_bytes = b;
            governing = h.node_id;
        }
    }
    return .{ .bytes = min_bytes orelse 0, .governing_node = governing, .provisional_node = provisional };
}

/// The default `pool_share` a shard-0 leader commits at genesis: 70% of each
/// data-holder's RAM after its reserve. Used until an admin sets another.
pub const DEFAULT_SHARE_PERMILLE: u16 = 700;

/// The slack a node holds on top of its floor and rings: the one-time overhead
/// a node carries, plus connection buffers, held polls, and OS slack. The exact
/// number is a measurement left to the floor-sizing work; this is a safe
/// starting value — too-small would over-commit the pool, too-large only
/// under-uses RAM.
pub const DEFAULT_MARGIN_BYTES: u64 = 64 * 1024 * 1024;

/// A node's running view of its budget, held on shard 0 and refreshed at each
/// settings-or-membership apply. It feeds `/status`, metrics and the shares
/// shard leaders bind, and records nothing itself. Through a provisional spell (a data-holder with no
/// row yet) it shows the last committed record, not an in-memory derive, so a
/// missing small joiner never inflates it and every replica shows the same
/// value across a restart.
pub const Observation = struct {
    /// The budget shown: the clean min over data-holders, or the last committed
    /// record while provisional, in bytes.
    budget_bytes: u64 = 0,
    /// The node that set the shown budget — named by a refusal and raised to
    /// lift the budget. Null before the first clean derive.
    governing_node: ?u32 = null,
    /// The data-holder with no row yet that froze the budget, or null when none.
    provisional_node: ?u32 = null,
    /// When the current freeze began, for the provisional-duration gauge. This
    /// is node-local wall-clock, not applied state: nothing decides on it (only
    /// the gauge and `/status` read it), so a restart resetting the *duration*
    /// shown — while the *budget* is recovered from the log — is correct. Keep
    /// it off any path that admits or refuses a write.
    provisional_since_ms: ?u64 = null,
    /// True once a clean budget has been derived at least once.
    seen: bool = false,

    /// A clean budget that fell, for the loud log.
    pub const Drop = struct { before: u64, after: u64, governing: ?u32 };

    /// The committed clean-budget record a provisional spell shows.
    pub const Clean = struct { budget_bytes: u64, governing_node: ?u32 = null };

    /// A clean (non-provisional) derivation: its value is a pure function of
    /// applied state, so show it and clear any freeze. Returns the drop when a
    /// clean budget fell, for the loud log.
    pub fn showClean(self: *Observation, b: Budget) ?Drop {
        self.provisional_node = null;
        self.provisional_since_ms = null;
        const before = self.budget_bytes;
        const had = self.seen;
        self.budget_bytes = b.bytes;
        self.governing_node = b.governing_node;
        self.seen = true;
        if (had and b.bytes < before) return .{ .before = before, .after = b.bytes, .governing = b.governing_node };
        return null;
    }

    /// A provisional spell: `node` is the data-holder that hasn't reported. The
    /// shown budget is the last committed clean record `rec`, so it depends only
    /// on applied state — a restarted node shows the same value as one that
    /// stayed up, never an in-memory derive that no record carries. `rec` is
    /// null until a clean budget has been recorded, when nothing is shown yet.
    pub fn showProvisional(self: *Observation, node: u32, rec: ?Clean, now_ms: u64) void {
        if (self.provisional_since_ms == null) self.provisional_since_ms = now_ms;
        self.provisional_node = node;
        if (rec) |r| {
            self.budget_bytes = r.budget_bytes;
            self.governing_node = r.governing_node;
            self.seen = true;
        } else {
            self.budget_bytes = 0;
            self.governing_node = null;
        }
    }

    /// How long the budget has been frozen for an unreported data-holder, in
    /// milliseconds; 0 when not provisional.
    pub fn provisionalMs(self: *const Observation, now_ms: u64) u64 {
        const since = self.provisional_since_ms orelse return 0;
        return if (now_ms > since) @intCast(now_ms - since) else 0;
    }
};

const testing = std.testing;

test "derive: single holder, 70% after reserve" {
    const h = [_]Holder{.{ .node_id = 1, .mem = .{ .ram_bytes = 2 * 1024 * 1024 * 1024, .reserve = 100 * 1024 * 1024 } }};
    const b = derive(700, &h);
    try testing.expectEqual(@as(?u32, null), b.provisional_node);
    // (2 GiB − 100 MiB) × 0.70
    const after: u64 = 2 * 1024 * 1024 * 1024 - 100 * 1024 * 1024;
    try testing.expectEqual(after * 700 / 1000, b.bytes);
}

test "derive: min over holders — the small node governs" {
    const h = [_]Holder{
        .{ .node_id = 1, .mem = .{ .ram_bytes = 8 * 1024 * 1024 * 1024, .reserve = 100 * 1024 * 1024 } },
        .{ .node_id = 2, .mem = .{ .ram_bytes = 2 * 1024 * 1024 * 1024, .reserve = 100 * 1024 * 1024 } },
    };
    const b = derive(700, &h);
    const small_after: u64 = 2 * 1024 * 1024 * 1024 - 100 * 1024 * 1024;
    try testing.expectEqual(small_after * 700 / 1000, b.bytes);
    try testing.expectEqual(@as(?u32, 2), b.governing_node); // the small node sets, and names, the budget
}

test "derive: equal shares — the lower id governs (every replica agrees)" {
    const h = [_]Holder{
        .{ .node_id = 7, .mem = .{ .ram_bytes = 2 * 1024 * 1024 * 1024, .reserve = 100 * 1024 * 1024 } },
        .{ .node_id = 3, .mem = .{ .ram_bytes = 2 * 1024 * 1024 * 1024, .reserve = 100 * 1024 * 1024 } },
    };
    try testing.expectEqual(@as(?u32, 3), derive(700, &h).governing_node);
}

test "derive: an unreported data-holder is named for a freeze, not excluded" {
    const h = [_]Holder{
        .{ .node_id = 3, .mem = null },
        .{ .node_id = 1, .mem = .{ .ram_bytes = 2 * 1024 * 1024 * 1024, .reserve = 100 * 1024 * 1024 } },
        .{ .node_id = 5, .mem = null },
    };
    const b = derive(700, &h);
    try testing.expectEqual(@as(?u32, 3), b.provisional_node); // lowest unreported id
}

test "derive: reserve ≥ ram contributes 0" {
    const h = [_]Holder{.{ .node_id = 1, .mem = .{ .ram_bytes = 50 * 1024 * 1024, .reserve = 100 * 1024 * 1024 } }};
    try testing.expectEqual(@as(u64, 0), derive(700, &h).bytes);
}

test "reserve: floor + rings over shards + margin" {
    // 4 shards, 80 MiB rings each, 256 MiB margin.
    const r = reserve(4, ringBytesPerShard(64 * 1024 * 1024), 256 * 1024 * 1024);
    try testing.expectEqual(@as(u64, 4) * (PER_SHARD_FLOOR_BYTES + 80 * 1024 * 1024 + INBOUND_OVERSHOOT_BYTES + OWN_SENDS_BYTES) + 256 * 1024 * 1024, r);
}

test "reserve: one inbound's overshoot is twice its payload bound, its calls' rows and the history rows beside it, within 768 KiB over that" {
    // About 10 KiB a message on top of its payload: a call's rows and reply
    // (~3.8 KiB) and a last chunk's landing and fold (~6.1 KiB).
    try testing.expect(INBOUND_OVERSHOOT_BYTES > 2 * channel_driver.MAX_BATCH_BYTES);
    try testing.expect(INBOUND_OVERSHOOT_BYTES < 2 * channel_driver.MAX_BATCH_BYTES + 768 * 1024);
}

test "ringBytesPerShard: log + raft clamp(hot/4, 2, 16 MiB)" {
    try testing.expectEqual(@as(u64, 80) * 1024 * 1024, ringBytesPerShard(64 * 1024 * 1024)); // 64 + 16
    try testing.expectEqual(@as(u64, 20) * 1024 * 1024, ringBytesPerShard(16 * 1024 * 1024)); // 16 + 4
    try testing.expectEqual(@as(u64, 4) * 1024 * 1024, ringBytesPerShard(2 * 1024 * 1024)); // 2 + 2 (floor)
    try testing.expectEqual(@as(u64, 1024) * 1024 * 1024 + 16 * 1024 * 1024, ringBytesPerShard(1024 * 1024 * 1024)); // 1 GiB + 16 (cap)
}

fn gib(n: u64) u64 {
    return n * 1024 * 1024 * 1024;
}

test "observation: a provisional spell shows the committed record, not an in-memory hold" {
    var o: Observation = .{};
    const clean = [_]Holder{.{ .node_id = 1, .mem = .{ .ram_bytes = gib(2), .reserve = 100 * 1024 * 1024 } }};
    _ = o.showClean(derive(700, &clean));
    const shown = o.budget_bytes;
    try testing.expect(shown > 0);
    try testing.expectEqual(@as(?u32, null), o.provisional_node);

    // A joiner with no row yet freezes the budget. The shown value is the
    // committed record — not whatever this node last derived — so it is the same
    // on a node that stayed up and one that restarted. The freeze is marked.
    o.showProvisional(2, .{ .budget_bytes = shown, .governing_node = 1 }, 2000);
    try testing.expectEqual(shown, o.budget_bytes);
    try testing.expectEqual(@as(?u32, 2), o.provisional_node);
    try testing.expectEqual(@as(u64, 500), o.provisionalMs(2500));

    // A provisional spell with no record yet shows nothing, never a stale derive.
    var fresh: Observation = .{};
    fresh.showProvisional(2, null, 2000);
    try testing.expectEqual(@as(u64, 0), fresh.budget_bytes);
    try testing.expectEqual(@as(?u32, 2), fresh.provisional_node);
}

test "observation: a clean derive recomputes to the min and logs the drop" {
    var o: Observation = .{};
    const one = [_]Holder{.{ .node_id = 1, .mem = .{ .ram_bytes = gib(8), .reserve = 100 * 1024 * 1024 } }};
    _ = o.showClean(derive(700, &one));
    const big = o.budget_bytes;

    // A smaller data-holder reports: the budget recomputes down and the drop is returned.
    const two = [_]Holder{ one[0], .{ .node_id = 2, .mem = .{ .ram_bytes = gib(2), .reserve = 100 * 1024 * 1024 } } };
    const drop = o.showClean(derive(700, &two));
    try testing.expect(drop != null);
    try testing.expectEqual(big, drop.?.before);
    try testing.expectEqual(o.budget_bytes, drop.?.after);
    try testing.expect(o.budget_bytes < big);
    try testing.expectEqual(@as(?u32, 2), o.governing_node);
    try testing.expectEqual(@as(u64, 0), o.provisionalMs(9999)); // not provisional
}

test "observation: the first clean derive never logs a drop" {
    var o: Observation = .{};
    const one = [_]Holder{.{ .node_id = 1, .mem = .{ .ram_bytes = gib(2), .reserve = 100 * 1024 * 1024 } }};
    try testing.expectEqual(@as(?Observation.Drop, null), o.showClean(derive(700, &one)));
}
