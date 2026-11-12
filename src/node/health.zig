//! What each shard on this node is doing, published for the HTTP threads.
//! `/health` says the process answers, `/ready` whether every shard can
//! serve, `/status` the detail per shard. The table exists before any shard
//! starts, so HTTP answers while the shards replay.
//!
//! Each shard writes only its own slot; HTTP threads read them all.

const std = @import("std");
const Atomic = std.atomic.Value;
const Role = @import("../raft/node.zig").Role;
const silent_loss = @import("silent_loss.zig");
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;

/// Ordered: a state after `offline` keeps the node out of `/ready`, and of
/// several, the latest in this order is the word `/ready` gives. That puts
/// the states an operator must act on before the ones that clear on their
/// own.
///
/// 503 is only for a state where another node can serve. A lone node's
/// offline shard leaves the node ready: nothing else could take its
/// traffic, and a 503 would pull the node, its healthy shards with it. Its
/// damage shows in `/status` and in `flo_shard_offline`. In a cluster the
/// same damage makes the member `diverged`, which is a 503.
pub const ShardState = enum(u8) {
    ok,
    /// On a lone node, its data is damaged: it refuses its own requests and
    /// the other shards serve.
    offline,
    /// It reached an entry every replica fails the same way and applies
    /// nothing past it: it serves reads, marked stale, and refuses writes
    /// until a repair. Every replica halts together, so the node stays
    /// ready; a 503 would pull the whole group.
    halted,
    /// Waiting for an init worker to take it.
    loading,
    /// Replaying its log.
    replaying,
    /// A member the leader has not yet seen caught up, or one booted with
    /// async durability whose projections don't yet hold what its group
    /// committed.
    catching_up,
    /// Asking to join; no config names this node yet.
    joining,
    /// Lost its log, and waiting for a majority to confirm what it held.
    guarded,
    /// An entry never reached disk, so writes have stopped.
    cannot_flush,
    /// A committed entry cannot be read to apply it (the host's disk read is
    /// blocked): it serves reads marked stale and the leader steps down so
    /// writes move. In a cluster another node can serve, so it is not ready; on
    /// a lone node a 503 would pull the only copy, so `readiness` keeps it ready
    /// (the offline-versus-diverged split). The block shows in `/status` and
    /// `flo_shard_read_blocked`.
    read_blocked,
    /// Its copy can no longer be trusted; it takes no part until rejoined.
    diverged,
    /// The group removed this node.
    removed,

    pub fn serves(self: ShardState) bool {
        return @backingInt(self) <= @backingInt(ShardState.halted);
    }
};

/// The longest reason kept for an offline shard.
pub const REASON_MAX = 256;

/// Removal notices a shard holds: while it counts toward a quorum, and the
/// members that made one once it acted. A sequence count, odd while a
/// publication is being written, lets a reader take one whole publication
/// and never a mix of two.
pub const Removal = struct {
    pub const MAX_FROM = @import("../raft/membership.zig").MAX_MEMBERS;
    seq: Atomic(u32) = .init(0),
    notices: Atomic(u8) = .init(0),
    needed: Atomic(u8) = .init(0),
    config_index: Atomic(u64) = .init(0),
    /// The removing leader's clock, once the shard acted; 0 while counting.
    at_ms: Atomic(u64) = .init(0),
    from: [MAX_FROM]Atomic(u32) = @splat(.init(0)),

    pub fn publish(self: *Removal, from: []const u32, needed: u8, config_index: u64, at_ms: u64) void {
        _ = self.seq.fetchAdd(1, .acq_rel);
        for (from, 0..) |id, i| self.from[i].store(id, .release);
        self.needed.store(needed, .release);
        self.config_index.store(config_index, .release);
        self.at_ms.store(at_ms, .release);
        self.notices.store(@intCast(from.len), .release);
        _ = self.seq.fetchAdd(1, .release);
    }

    pub const Read = struct {
        notices: u8 = 0,
        needed: u8 = 0,
        config_index: u64 = 0,
        at_ms: u64 = 0,
        from: [MAX_FROM]u32 = undefined,
    };

    /// One whole publication, or nothing held if none settles in a few
    /// tries (the shard is publishing as fast as it can be read).
    pub fn read(self: *const Removal) Read {
        for (0..8) |_| {
            const before = self.seq.load(.acquire);
            if (before % 2 == 1) continue;
            var r: Read = .{
                .notices = self.notices.load(.acquire),
                .needed = self.needed.load(.acquire),
                .config_index = self.config_index.load(.acquire),
                .at_ms = self.at_ms.load(.acquire),
            };
            for (self.from[0..r.notices], 0..) |*id, i| r.from[i] = id.load(.acquire);
            if (self.seq.load(.acquire) == before) return r;
            r = .{};
        }
        return .{};
    }
};

pub const ShardSlot = struct {
    state: Atomic(u8) = .init(@backingInt(ShardState.loading)),
    role: Atomic(u8) = .init(@backingInt(Role.follower)),
    last_applied: Atomic(u64) = .init(0),
    commit_index: Atomic(u64) = .init(0),
    /// While replaying: the index of the last entry read back, and the last
    /// index the segments hold, which it reaches when replay is done.
    replay_index: Atomic(u64) = .init(0),
    replay_last: Atomic(u64) = .init(0),
    /// Written once, before the state first reads `offline`, `halted` or
    /// `diverged`.
    reason_len: Atomic(u16) = .init(0),
    reason_buf: [REASON_MAX]u8 = undefined,
    removal: Removal = .{},
    /// The committed config a member vouched for, while it is newer than
    /// this shard's own (0 for none); and whether the shard is dropping a
    /// stranger's frames with no leader heard, the stuck-member symptom.
    vouched_index: Atomic(u64) = .init(0),
    strangers_while_quiet: Atomic(bool) = .init(false),
    /// The entry the shard halted at, as a repair names it: written
    /// before the state reads `halted`.
    halt_index: Atomic(u64) = .init(0),
    halt_term: Atomic(u64) = .init(0),
    halt_crc: Atomic(u32) = .init(0),
    /// How long, in ms, the committed read has been blocked, computed at publish
    /// (0 when not blocked): a plain duration, so a reader needn't subtract a raw
    /// monotonic reading. The reason buffer holds what blocked it.
    read_blocked_for_ms: Atomic(u64) = .init(0),
    /// Flushes to disk failed (cumulative count of attempts), and how long the
    /// current run of failures has lasted in ms (0 while flushes succeed): a
    /// disk full or failing, which clients are told about but is otherwise only
    /// in the log. A duration, not a raw clock reading.
    persist_failures: Atomic(u64) = .init(0),
    flushes_failing_for_ms: Atomic(u64) = .init(0),
    /// Snapshot bars a leader still applying earlier terms passed without a
    /// snapshot (cumulative): one that keeps counting is stuck not ready.
    snapshot_skipped_leader_not_ready: Atomic(u64) = .init(0),
    /// The bar apply is suspended at, and for how long in ms; 0 with none.
    snapshot_bar: Atomic(u64) = .init(0),
    /// Snapshot bars: the last one's index (0 before the first), the logical
    /// bytes applied since, and how many more bring the next.
    bar_last: Atomic(u64) = .init(0),
    bar_bytes: Atomic(u64) = .init(0),
    bar_threshold: Atomic(u64) = .init(0),
    snapshot_suspended_for_ms: Atomic(u64) = .init(0),
    /// HARDSTATE writes (term + vote) that could not be persisted (cumulative)
    /// and how long the current run has lasted in ms (0 while they persist),
    /// computed at publish: while a run holds, each unpersisted vote or term
    /// change is refused; the next good persist ends the run.
    hardstate_failures: Atomic(u64) = .init(0),
    hardstate_failing_for_ms: Atomic(u64) = .init(0),
    /// Times the in-memory segment buffer could not grow (out of memory) on a
    /// Raft append — committed or not — cumulative, and how long the current run
    /// has lasted in ms (0 while buffering succeeds), computed at publish. Free
    /// memory, then restart or resync.
    buffer_failures: Atomic(u64) = .init(0),
    buffer_failing_for_ms: Atomic(u64) = .init(0),
    /// Set with a `read_blocked` state on a lone shard, whose only copy this is:
    /// `readiness` keeps the node ready then, since a 503 would pull it and no
    /// other node could serve. In a cluster it is false and `read_blocked` is a
    /// 503, like `offline` versus `diverged`.
    read_blocked_serves: Atomic(bool) = .init(false),

    pub fn get(self: *const ShardSlot) ShardState {
        return @fromBackingInt(@intCast(self.state.load(.acquire)));
    }

    /// Whether this shard lets the node answer `/ready`: its state serves, or it
    /// is a lone blocked shard that must stay ready.
    pub fn servesReady(self: *const ShardSlot) bool {
        const state = self.get();
        return state.serves() or (state == .read_blocked and self.read_blocked_serves.load(.acquire));
    }

    pub fn set(self: *ShardSlot, state: ShardState) void {
        self.state.store(@backingInt(state), .release);
    }

    pub fn publishVouch(self: *ShardSlot, vouched_index: u64, strangers_while_quiet: bool) void {
        self.vouched_index.store(vouched_index, .monotonic);
        self.strangers_while_quiet.store(strangers_while_quiet, .monotonic);
    }

    pub fn publish(self: *ShardSlot, state: ShardState, role: Role, last_applied: u64, commit_index: u64) void {
        self.role.store(@backingInt(role), .monotonic);
        self.last_applied.store(last_applied, .monotonic);
        self.commit_index.store(commit_index, .monotonic);
        self.set(state);
    }

    /// Keep why the shard is out of service. A new reason replaces the old:
    /// a shard that halted and then diverged answers with the divergence.
    pub fn setReason(self: *ShardSlot, why: []const u8) void {
        const n = @min(why.len, REASON_MAX);
        const len = self.reason_len.load(.monotonic);
        if (len == n and std.mem.eql(u8, self.reason_buf[0..n], why[0..n])) return;
        // Changes are rare: a reader racing one may see it torn once, and
        // reads it whole next time.
        self.reason_len.store(0, .release);
        @memcpy(self.reason_buf[0..n], why[0..n]);
        self.reason_len.store(@intCast(n), .release);
    }

    pub fn reason(self: *const ShardSlot) ?[]const u8 {
        const n = self.reason_len.load(.acquire);
        return if (n == 0) null else self.reason_buf[0..n];
    }
};

pub const Readiness = struct {
    ready: bool,
    /// `ready`, or the state that keeps the node out.
    word: []const u8,
};

pub const NodeHealth = struct {
    node_id: u32,
    shards: []ShardSlot,
    /// The client listener is up. Until it is, nothing serves, whatever the
    /// shards say.
    accepting: Atomic(bool) = .init(false),

    pub fn init(allocator: std.mem.Allocator, node_id: u32, shard_count: usize) !NodeHealth {
        const shards = try allocator.alloc(ShardSlot, shard_count);
        for (shards) |*s| s.* = .{};
        return .{ .node_id = node_id, .shards = shards };
    }

    pub fn deinit(self: *NodeHealth, allocator: std.mem.Allocator) void {
        allocator.free(self.shards);
    }

    pub fn readiness(self: *const NodeHealth) Readiness {
        // The worst shard that does not let the node answer: a lone blocked
        // shard still does (`servesReady`), so a 503 never pulls a node whose
        // only copy of that shard this is.
        var worst: ?ShardState = null;
        for (self.shards) |*s| {
            if (s.servesReady()) continue;
            const state = s.get();
            if (worst == null or @backingInt(state) > @backingInt(worst.?)) worst = state;
        }
        if (worst) |w| return .{ .ready = false, .word = @tagName(w) };
        if (!self.accepting.load(.acquire)) return .{ .ready = false, .word = @tagName(ShardState.loading) };
        return .{ .ready = true, .word = "ready" };
    }

    /// `/status`: the node, then each shard with what it has lost.
    pub fn writeStatus(self: *const NodeHealth, w: *std.Io.Writer, registry: ?*MetricsRegistry) std.Io.Writer.Error!void {
        const r = self.readiness();
        try w.print("{{\"node_id\":{d},\"ready\":{},\"state\":\"{s}\",\"shards\":[", .{ self.node_id, r.ready, r.word });
        for (self.shards, 0..) |*s, i| {
            if (i > 0) try w.writeByte(',');
            const state = s.get();
            const role: Role = @fromBackingInt(@intCast(s.role.load(.monotonic)));
            try w.print("{{\"id\":{d},\"state\":\"{s}\"", .{ i, @tagName(state) });
            if (state == .offline or state == .halted or state == .diverged or state == .read_blocked) if (s.reason()) |why| {
                try w.writeAll(",\"reason\":");
                try std.json.Stringify.encodeJsonString(why, .{}, w);
            };
            if (state == .halted) try w.print(",\"halt_index\":{d},\"halt_term\":{d},\"halt_crc\":{d}", .{ s.halt_index.load(.monotonic), s.halt_term.load(.monotonic), s.halt_crc.load(.monotonic) });
            if (state == .read_blocked) try w.print(",\"read_blocked_for_ms\":{d}", .{s.read_blocked_for_ms.load(.monotonic)});
            // Shown once any flush has failed, so a healthy shard stays quiet.
            const persist_failures = s.persist_failures.load(.monotonic);
            if (persist_failures > 0) try w.print(",\"persist_failures\":{d},\"flushes_failing_for_ms\":{d}", .{ persist_failures, s.flushes_failing_for_ms.load(.monotonic) });
            const skipped = s.snapshot_skipped_leader_not_ready.load(.monotonic);
            if (skipped > 0) try w.print(",\"snapshot_skipped_leader_not_ready\":{d}", .{skipped});
            const bar_threshold = s.bar_threshold.load(.monotonic);
            if (bar_threshold > 0) try w.print(",\"bars\":{{\"last\":{d},\"bytes\":{d},\"threshold\":{d}}}", .{ s.bar_last.load(.monotonic), s.bar_bytes.load(.monotonic), bar_threshold });
            const bar = s.snapshot_bar.load(.monotonic);
            if (bar > 0) try w.print(",\"snapshot\":{{\"bar\":{d},\"suspended_for_ms\":{d}}}", .{ bar, s.snapshot_suspended_for_ms.load(.monotonic) });
            const hardstate_failures = s.hardstate_failures.load(.monotonic);
            if (hardstate_failures > 0) try w.print(",\"hardstate_failures\":{d},\"hardstate_failing_for_ms\":{d}", .{ hardstate_failures, s.hardstate_failing_for_ms.load(.monotonic) });
            const buffer_failures = s.buffer_failures.load(.monotonic);
            if (buffer_failures > 0) try w.print(",\"buffer_failures\":{d},\"buffer_failing_for_ms\":{d}", .{ buffer_failures, s.buffer_failing_for_ms.load(.monotonic) });
            if (state == .replaying) {
                try w.print(",\"replay\":{{\"index\":{d},\"last\":{d}}}", .{ s.replay_index.load(.monotonic), s.replay_last.load(.monotonic) });
            }
            const removal = s.removal.read();
            if (removal.notices > 0) {
                try w.print(",\"removal\":{{\"notices\":{d},\"of\":{d},\"config_index\":{d}", .{ removal.notices, removal.needed, removal.config_index });
                if (removal.at_ms != 0) try w.print(",\"at_ms\":{d}", .{removal.at_ms});
                try w.writeAll(",\"from\":[");
                for (removal.from[0..removal.notices], 0..) |id, k| {
                    if (k > 0) try w.writeByte(',');
                    try w.print("{d}", .{id});
                }
                try w.writeAll("]}");
            }
            const vouched = s.vouched_index.load(.monotonic);
            if (vouched != 0) try w.print(",\"vouched_index\":{d}", .{vouched});
            if (s.strangers_while_quiet.load(.monotonic)) try w.writeAll(",\"strangers_while_quiet\":true");
            try w.print(",\"last_applied\":{d},\"commit_index\":{d},\"role\":\"{s}\",\"losses\":{{", .{ s.last_applied.load(.monotonic), s.commit_index.load(.monotonic), @tagName(role) });
            const sm = if (registry) |reg| reg.shardMetrics(@intCast(i)) else null;
            inline for (@typeInfo(silent_loss.Path).@"enum".field_names, 0..) |name, p| {
                if (p > 0) try w.writeByte(',');
                const n = if (sm) |m| m.silent_loss[p].load(.monotonic) else 0;
                try w.print("\"{s}\":{d}", .{ name, n });
            }
            try w.print("}},\"duplicate_index\":{d},\"processing_failed_jobs\":{d},\"processing_held_jobs\":{d}}}", .{
                if (sm) |m| m.duplicate_index.load(.monotonic) else 0,
                if (sm) |m| m.processing_failed_jobs.load(.monotonic) else 0,
                if (sm) |m| m.processing_held_jobs.load(.monotonic) else 0,
            });
        }
        try w.writeAll("]}\n");
    }
};

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "health: ready only when every shard serves and the client listener is up" {
    var h = try NodeHealth.init(testing.allocator, 7, 3);
    defer h.deinit(testing.allocator);

    // At start every shard waits.
    try testing.expectEqualDeep(Readiness{ .ready = false, .word = "loading" }, h.readiness());
    h.shards[0].set(.replaying);
    try testing.expectEqualStrings("replaying", h.readiness().word);

    for (h.shards) |*s| s.set(.ok);
    // Shards up, listener not yet.
    try testing.expectEqualDeep(Readiness{ .ready = false, .word = "loading" }, h.readiness());
    h.accepting.store(true, .release);
    try testing.expectEqualDeep(Readiness{ .ready = true, .word = "ready" }, h.readiness());

    // A lone node's offline shard leaves it ready.
    h.shards[1].set(.offline);
    try testing.expect(h.readiness().ready);
}

test "health: a lone blocked shard keeps the node ready; a cluster one is a 503" {
    var h = try NodeHealth.init(testing.allocator, 1, 2);
    defer h.deinit(testing.allocator);
    h.accepting.store(true, .release);
    for (h.shards) |*s| s.set(.ok);

    // A blocked shard whose only copy this is (lone) keeps the node ready — a
    // 503 would pull it and nothing else could serve its data.
    h.shards[0].read_blocked_serves.store(true, .release);
    h.shards[0].set(.read_blocked);
    try testing.expect(h.readiness().ready);

    // In a cluster another node can serve, so the blocked shard is a 503.
    h.shards[0].read_blocked_serves.store(false, .release);
    try testing.expectEqualDeep(Readiness{ .ready = false, .word = "read_blocked" }, h.readiness());

    // A genuinely non-serving shard still 503s even next to a lone blocked one.
    h.shards[0].read_blocked_serves.store(true, .release);
    h.shards[1].set(.cannot_flush);
    try testing.expectEqualStrings("cannot_flush", h.readiness().word);
}

test "health: of several states, /ready names the one that needs someone first" {
    var h = try NodeHealth.init(testing.allocator, 1, 2);
    defer h.deinit(testing.allocator);
    h.accepting.store(true, .release);

    // Worst first, as /ready reports them.
    const order = [_]ShardState{ .removed, .diverged, .cannot_flush, .guarded, .joining, .catching_up, .replaying, .loading };
    for (order, 0..) |worse, i| {
        try testing.expect(!worse.serves());
        for (order[i + 1 ..]) |better| {
            h.shards[0].set(better);
            h.shards[1].set(worse);
            try testing.expectEqualDeep(Readiness{ .ready = false, .word = @tagName(worse) }, h.readiness());
            h.shards[0].set(worse);
            h.shards[1].set(better);
            try testing.expectEqualDeep(Readiness{ .ready = false, .word = @tagName(worse) }, h.readiness());
        }
        // Next to a serving shard, offline included, the state still decides.
        h.shards[0].set(.offline);
        h.shards[1].set(worse);
        try testing.expectEqualStrings(@tagName(worse), h.readiness().word);
    }
}

test "health: a halted shard keeps the node ready, and /status names where it halted and why; a diverged one names its damage" {
    var h = try NodeHealth.init(testing.allocator, 1, 2);
    defer h.deinit(testing.allocator);
    h.accepting.store(true, .release);
    h.shards[0].halt_index.store(18234, .monotonic);
    h.shards[0].halt_term.store(7, .monotonic);
    h.shards[0].halt_crc.store(0xdeadbeef, .monotonic);
    h.shards[0].setReason("entry 18234 (kv_put): its projection refused it as malformed (a Flo bug)");
    h.shards[0].publish(.halted, .leader, 18233, 18240);
    h.shards[1].setReason("entry 9 (kv_put) could not be applied on this node: out of memory");
    h.shards[1].publish(.ok, .follower, 1, 1);
    // Every replica halts together: the node stays ready.
    try testing.expect(h.readiness().ready);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try h.writeStatus(&out.writer, null);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, out.written(), .{});
    defer parsed.deinit();
    const s0 = parsed.value.object.get("shards").?.array.items[0].object;
    try testing.expectEqualStrings("halted", s0.get("state").?.string);
    try testing.expectEqual(@as(i64, 18234), s0.get("halt_index").?.integer);
    try testing.expectEqual(@as(i64, 7), s0.get("halt_term").?.integer);
    try testing.expectEqual(@as(i64, 0xdeadbeef), s0.get("halt_crc").?.integer);
    try testing.expect(std.mem.indexOf(u8, s0.get("reason").?.string, "malformed") != null);
    // A reason shows only with the state that has one.
    try testing.expect(parsed.value.object.get("shards").?.array.items[1].object.get("reason") == null);

    h.shards[1].publish(.diverged, .follower, 1, 1);
    try testing.expect(!h.readiness().ready);
    out.clearRetainingCapacity();
    try h.writeStatus(&out.writer, null);
    try testing.expect(std.mem.indexOf(u8, out.written(), "out of memory") != null);
}

test "health: /status shows flush failures once any have happened, and stays quiet while flushes succeed" {
    var h = try NodeHealth.init(testing.allocator, 1, 2);
    defer h.deinit(testing.allocator);
    h.accepting.store(true, .release);
    // Shard 0 has had each kind of persistence fail; shard 1 is healthy.
    h.shards[0].persist_failures.store(5, .monotonic);
    h.shards[0].flushes_failing_for_ms.store(123_456, .monotonic);
    h.shards[0].hardstate_failures.store(3, .monotonic);
    h.shards[0].hardstate_failing_for_ms.store(60_000, .monotonic);
    h.shards[0].buffer_failures.store(2, .monotonic);
    h.shards[0].buffer_failing_for_ms.store(9_000, .monotonic);
    h.shards[0].publish(.ok, .leader, 10, 10);
    h.shards[1].publish(.ok, .follower, 10, 10);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try h.writeStatus(&out.writer, null);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, out.written(), .{});
    defer parsed.deinit();
    const s0 = parsed.value.object.get("shards").?.array.items[0].object;
    try testing.expectEqual(@as(i64, 5), s0.get("persist_failures").?.integer);
    try testing.expectEqual(@as(i64, 123_456), s0.get("flushes_failing_for_ms").?.integer);
    try testing.expectEqual(@as(i64, 3), s0.get("hardstate_failures").?.integer);
    try testing.expectEqual(@as(i64, 60_000), s0.get("hardstate_failing_for_ms").?.integer);
    try testing.expectEqual(@as(i64, 2), s0.get("buffer_failures").?.integer);
    try testing.expectEqual(@as(i64, 9_000), s0.get("buffer_failing_for_ms").?.integer);
    // A healthy shard stays quiet on all three.
    const healthy = parsed.value.object.get("shards").?.array.items[1].object;
    try testing.expect(healthy.get("persist_failures") == null);
    try testing.expect(healthy.get("hardstate_failures") == null);
    try testing.expect(healthy.get("buffer_failures") == null);

    // A blocked read reports its duration under the renamed key, not "_since_".
    h.shards[1].read_blocked_for_ms.store(777, .monotonic);
    h.shards[1].publish(.read_blocked, .follower, 10, 10);
    out.clearRetainingCapacity();
    try h.writeStatus(&out.writer, null);
    const reparsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, out.written(), .{});
    defer reparsed.deinit();
    const s1 = reparsed.value.object.get("shards").?.array.items[1].object;
    try testing.expectEqual(@as(i64, 777), s1.get("read_blocked_for_ms").?.integer);
    try testing.expect(s1.get("read_blocked_since_ms") == null);
}

test "health: /status names each shard's state, its reason when offline, and its losses" {
    var registry = MetricsRegistry.init(testing.allocator);
    defer registry.deinit();
    try registry.initShards(2);
    registry.shardMetrics(1).?.silent_loss[@backingInt(silent_loss.Path.segment_unreadable)].store(3, .monotonic);
    registry.shardMetrics(1).?.duplicate_index.store(2, .monotonic);
    registry.shardMetrics(1).?.processing_failed_jobs.store(4, .monotonic);
    registry.shardMetrics(1).?.processing_held_jobs.store(2, .monotonic);

    var h = try NodeHealth.init(testing.allocator, 42, 2);
    defer h.deinit(testing.allocator);
    h.accepting.store(true, .release);
    h.shards[0].publish(.ok, .leader, 10, 12);
    h.shards[1].setReason("an older reason is replaced");
    h.shards[1].setReason("segment 00000001.flseg failed its \"checksum\"");
    h.shards[1].publish(.offline, .follower, 4, 4);
    h.shards[1].publishVouch(3726, true);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try h.writeStatus(&out.writer, &registry);

    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, out.written(), .{});
    defer parsed.deinit();
    const root = parsed.value.object;
    try testing.expectEqual(@as(i64, 42), root.get("node_id").?.integer);
    try testing.expect(root.get("ready").?.bool);
    try testing.expectEqualStrings("ready", root.get("state").?.string);
    const shards = root.get("shards").?.array.items;
    try testing.expectEqual(@as(usize, 2), shards.len);

    const s0 = shards[0].object;
    try testing.expectEqualStrings("ok", s0.get("state").?.string);
    try testing.expectEqualStrings("leader", s0.get("role").?.string);
    try testing.expectEqual(@as(i64, 10), s0.get("last_applied").?.integer);
    try testing.expectEqual(@as(i64, 12), s0.get("commit_index").?.integer);
    try testing.expect(s0.get("reason") == null);
    try testing.expect(s0.get("vouched_index") == null);
    try testing.expect(s0.get("strangers_while_quiet") == null);
    try testing.expectEqual(@as(usize, silent_loss.PATH_COUNT), s0.get("losses").?.object.count());

    const s1 = shards[1].object;
    try testing.expectEqualStrings("offline", s1.get("state").?.string);
    try testing.expectEqualStrings("segment 00000001.flseg failed its \"checksum\"", s1.get("reason").?.string);
    try testing.expectEqual(@as(i64, 3726), s1.get("vouched_index").?.integer);
    try testing.expect(s1.get("strangers_while_quiet").?.bool);
    try testing.expectEqual(@as(i64, 3), s1.get("losses").?.object.get("segment_unreadable").?.integer);
    // A repeated index is shown beside the losses, not among them.
    try testing.expectEqual(@as(i64, 0), s0.get("duplicate_index").?.integer);
    try testing.expectEqual(@as(i64, 2), s1.get("duplicate_index").?.integer);
    try testing.expectEqual(@as(i64, 0), s0.get("processing_failed_jobs").?.integer);
    try testing.expectEqual(@as(i64, 4), s1.get("processing_failed_jobs").?.integer);
    try testing.expectEqual(@as(i64, 2), s1.get("processing_held_jobs").?.integer);
}

test "health: /status shows removal notices held, and the quorum once acted on" {
    var h = try NodeHealth.init(testing.allocator, 1, 1);
    defer h.deinit(testing.allocator);
    var aw: std.Io.Writer.Allocating = .init(testing.allocator);
    defer aw.deinit();
    try h.writeStatus(&aw.writer, null);
    try testing.expect(std.mem.indexOf(u8, aw.written(), "removal") == null);

    h.shards[0].removal.publish(&.{2}, 2, 9, 0);
    aw.clearRetainingCapacity();
    try h.writeStatus(&aw.writer, null);
    try testing.expect(std.mem.indexOf(u8, aw.written(), "\"removal\":{\"notices\":1,\"of\":2,\"config_index\":9,\"from\":[2]}") != null);

    h.shards[0].removal.publish(&.{ 2, 3 }, 2, 10, 1000);
    aw.clearRetainingCapacity();
    try h.writeStatus(&aw.writer, null);
    try testing.expect(std.mem.indexOf(u8, aw.written(), "\"removal\":{\"notices\":2,\"of\":2,\"config_index\":10,\"at_ms\":1000,\"from\":[2,3]}") != null);
}

test "health: /status shows a leader's snapshot skips and an open window's bar, and stays quiet without them" {
    var h = try NodeHealth.init(testing.allocator, 1, 2);
    defer h.deinit(testing.allocator);
    h.accepting.store(true, .release);
    h.shards[0].snapshot_skipped_leader_not_ready.store(4, .monotonic);
    h.shards[0].snapshot_bar.store(1234, .monotonic);
    h.shards[0].snapshot_suspended_for_ms.store(80, .monotonic);
    h.shards[0].publish(.ok, .leader, 10, 10);
    h.shards[1].publish(.ok, .follower, 10, 10);

    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try h.writeStatus(&out.writer, null);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, out.written(), .{});
    defer parsed.deinit();
    const s0 = parsed.value.object.get("shards").?.array.items[0].object;
    try testing.expectEqual(@as(i64, 4), s0.get("snapshot_skipped_leader_not_ready").?.integer);
    const window = s0.get("snapshot").?.object;
    try testing.expectEqual(@as(i64, 1234), window.get("bar").?.integer);
    try testing.expectEqual(@as(i64, 80), window.get("suspended_for_ms").?.integer);
    const quiet = parsed.value.object.get("shards").?.array.items[1].object;
    try testing.expect(quiet.get("snapshot_skipped_leader_not_ready") == null);
    try testing.expect(quiet.get("snapshot") == null);
}

test "health: /status shows a shard's snapshot bars: the last, the bytes since, and the threshold" {
    var h = try NodeHealth.init(testing.allocator, 1, 1);
    defer h.deinit(testing.allocator);
    h.accepting.store(true, .release);
    h.shards[0].bar_last.store(1200, .monotonic);
    h.shards[0].bar_bytes.store(4096, .monotonic);
    h.shards[0].bar_threshold.store(64 * 1024 * 1024, .monotonic);
    h.shards[0].publish(.ok, .leader, 10, 10);
    var out: std.Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try h.writeStatus(&out.writer, null);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, out.written(), .{});
    defer parsed.deinit();
    const bars = parsed.value.object.get("shards").?.array.items[0].object.get("bars").?.object;
    try testing.expectEqual(@as(i64, 1200), bars.get("last").?.integer);
    try testing.expectEqual(@as(i64, 4096), bars.get("bytes").?.integer);
    try testing.expectEqual(@as(i64, 64 * 1024 * 1024), bars.get("threshold").?.integer);
}
