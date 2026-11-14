//! The per-node memory budget, derived from the committed cluster-settings
//! family (`settings/machine.zig`) joined with the data-holding membership.
//! This derivation feeds `/status` and metrics only; nothing reads it to
//! admit or refuse a write yet, so it records nothing in applied state.
//!
//! budget = min over every data-holding member WITH a settings row of
//!          pool_share × (ram_bytes − reserve). A data-holder with no row yet
//!          makes the budget provisional; the caller freezes at its last
//!          non-provisional value, never excludes it — excluding a missing
//!          small node keeps the min high, so the node over-commits.

const std = @import("std");

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

/// A node's reserve: its per-shard floor plus its configured ring sizes, over
/// its shards, plus a margin. A node computes this from its own config and
/// reports it in its settings row; the cluster takes it as given. The margin
/// covers the ~10 MiB a node carries once at low shard counts, so the linear
/// `floor × shards` holds there too.
pub fn reserve(shard_count: u32, ring_bytes_per_shard: u64, margin: u64) u64 {
    return @as(u64, shard_count) * (PER_SHARD_FLOOR_BYTES +| ring_bytes_per_shard) +| margin;
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
/// settings-or-membership apply. It feeds `/status` and metrics only and
/// records nothing in applied state. The shown budget holds through a
/// provisional spell (a data-holder with no row yet) at its last clean value,
/// so a missing small joiner never inflates it.
pub const Observation = struct {
    /// The budget shown: the last clean min over data-holders, in bytes.
    budget_bytes: u64 = 0,
    /// The node that set the shown budget — named by a refusal and raised to
    /// lift the budget. Null before the first clean derive.
    governing_node: ?u32 = null,
    /// The pool's use. Nothing charges the pool yet, so always 0.
    pool_use_bytes: u64 = 0,
    /// The data-holder with no row yet that froze the budget, or null when none.
    provisional_node: ?u32 = null,
    /// When the current freeze began, for the provisional-duration gauge.
    provisional_since_ms: ?u64 = null,
    /// True once a clean budget has been derived at least once.
    seen: bool = false,

    /// A clean budget that fell, for the loud log.
    pub const Drop = struct { before: u64, after: u64, governing: ?u32 };

    /// Fold a fresh derivation in at `now_ms`. While provisional, the shown
    /// budget is held and only the freeze is marked; once clean, it recomputes
    /// to the min. Returns the drop when a clean budget fell, else null.
    pub fn update(self: *Observation, b: Budget, now_ms: u64) ?Drop {
        if (b.provisional_node) |n| {
            self.provisional_node = n;
            if (self.provisional_since_ms == null) self.provisional_since_ms = now_ms;
            return null;
        }
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
    try testing.expectEqual(@as(u64, 4) * (PER_SHARD_FLOOR_BYTES + 80 * 1024 * 1024) + 256 * 1024 * 1024, r);
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

test "observation: a provisional derive holds the last clean budget" {
    var o: Observation = .{};
    const clean = [_]Holder{.{ .node_id = 1, .mem = .{ .ram_bytes = gib(2), .reserve = 100 * 1024 * 1024 } }};
    _ = o.update(derive(700, &clean), 1000);
    const shown = o.budget_bytes;
    try testing.expect(shown > 0);
    try testing.expectEqual(@as(?u32, null), o.provisional_node);

    // A joiner with no row yet: budget is held, not lowered, and the freeze is marked.
    const joined = [_]Holder{ clean[0], .{ .node_id = 2, .mem = null } };
    try testing.expectEqual(@as(?Observation.Drop, null), o.update(derive(700, &joined), 2000));
    try testing.expectEqual(shown, o.budget_bytes);
    try testing.expectEqual(@as(?u32, 2), o.provisional_node);
    try testing.expectEqual(@as(u64, 500), o.provisionalMs(2500));
}

test "observation: recomputes to the clean min and logs the drop" {
    var o: Observation = .{};
    const one = [_]Holder{.{ .node_id = 1, .mem = .{ .ram_bytes = gib(8), .reserve = 100 * 1024 * 1024 } }};
    _ = o.update(derive(700, &one), 1000);
    const big = o.budget_bytes;

    // A smaller data-holder reports: the budget recomputes down and the drop is returned.
    const two = [_]Holder{ one[0], .{ .node_id = 2, .mem = .{ .ram_bytes = gib(2), .reserve = 100 * 1024 * 1024 } } };
    const drop = o.update(derive(700, &two), 2000);
    try testing.expect(drop != null);
    try testing.expectEqual(big, drop.?.before);
    try testing.expectEqual(o.budget_bytes, drop.?.after);
    try testing.expect(o.budget_bytes < big);
    try testing.expectEqual(@as(?u32, 2), o.governing_node);
    try testing.expectEqual(@as(u64, 0), o.provisionalMs(9999)); // not provisional
}

test "observation: the first derive never logs a drop" {
    var o: Observation = .{};
    const one = [_]Holder{.{ .node_id = 1, .mem = .{ .ram_bytes = gib(2), .reserve = 100 * 1024 * 1024 } }};
    try testing.expectEqual(@as(?Observation.Drop, null), o.update(derive(700, &one), 1000));
}
