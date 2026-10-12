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
    /// Waiting for an init worker to take it.
    loading,
    /// Replaying its log.
    replaying,
    /// A member the leader has not yet seen caught up.
    catching_up,
    /// Asking to join; no config names this node yet.
    joining,
    /// Lost its log, and waiting for a majority to confirm what it held.
    guarded,
    /// An entry never reached disk, so writes have stopped.
    cannot_flush,
    /// Its copy can no longer be trusted; it takes no part until rejoined.
    diverged,
    /// The group removed this node.
    removed,

    pub fn serves(self: ShardState) bool {
        return @backingInt(self) <= @backingInt(ShardState.offline);
    }
};

/// The longest reason kept for an offline shard.
pub const REASON_MAX = 256;

pub const ShardSlot = struct {
    state: Atomic(u8) = .init(@backingInt(ShardState.loading)),
    role: Atomic(u8) = .init(@backingInt(Role.follower)),
    last_applied: Atomic(u64) = .init(0),
    commit_index: Atomic(u64) = .init(0),
    /// While replaying: the index of the last entry read back, and the last
    /// index the segments hold, which it reaches when replay is done.
    replay_index: Atomic(u64) = .init(0),
    replay_last: Atomic(u64) = .init(0),
    /// Written once, before the state first reads `offline`.
    reason_len: Atomic(u16) = .init(0),
    reason_buf: [REASON_MAX]u8 = undefined,

    pub fn get(self: *const ShardSlot) ShardState {
        return @fromBackingInt(@intCast(self.state.load(.acquire)));
    }

    pub fn set(self: *ShardSlot, state: ShardState) void {
        self.state.store(@backingInt(state), .release);
    }

    pub fn publish(self: *ShardSlot, state: ShardState, role: Role, last_applied: u64, commit_index: u64) void {
        self.role.store(@backingInt(role), .monotonic);
        self.last_applied.store(last_applied, .monotonic);
        self.commit_index.store(commit_index, .monotonic);
        self.set(state);
    }

    /// Keep why the shard went offline. Only the first reason is kept: it
    /// is the one the shard answers with.
    pub fn setReason(self: *ShardSlot, why: []const u8) void {
        if (self.reason_len.load(.monotonic) != 0) return;
        const n = @min(why.len, REASON_MAX);
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
        var worst: ShardState = .ok;
        for (self.shards) |*s| {
            const state = s.get();
            if (@backingInt(state) > @backingInt(worst)) worst = state;
        }
        if (!worst.serves()) return .{ .ready = false, .word = @tagName(worst) };
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
            if (state == .offline) if (s.reason()) |why| {
                try w.writeAll(",\"reason\":");
                try std.json.Stringify.encodeJsonString(why, .{}, w);
            };
            if (state == .replaying) {
                try w.print(",\"replay\":{{\"index\":{d},\"last\":{d}}}", .{ s.replay_index.load(.monotonic), s.replay_last.load(.monotonic) });
            }
            try w.print(",\"last_applied\":{d},\"commit_index\":{d},\"role\":\"{s}\",\"losses\":{{", .{ s.last_applied.load(.monotonic), s.commit_index.load(.monotonic), @tagName(role) });
            const sm = if (registry) |reg| reg.shardMetrics(@intCast(i)) else null;
            inline for (@typeInfo(silent_loss.Path).@"enum".field_names, 0..) |name, p| {
                if (p > 0) try w.writeByte(',');
                const n = if (sm) |m| m.silent_loss[p].load(.monotonic) else 0;
                try w.print("\"{s}\":{d}", .{ name, n });
            }
            try w.writeAll("}}");
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

test "health: /status names each shard's state, its reason when offline, and its losses" {
    var registry = MetricsRegistry.init(testing.allocator);
    defer registry.deinit();
    try registry.initShards(2);
    registry.shardMetrics(1).?.silent_loss[@backingInt(silent_loss.Path.segment_unreadable)].store(3, .monotonic);

    var h = try NodeHealth.init(testing.allocator, 42, 2);
    defer h.deinit(testing.allocator);
    h.accepting.store(true, .release);
    h.shards[0].publish(.ok, .leader, 10, 12);
    h.shards[1].setReason("segment 00000001.flseg failed its \"checksum\"");
    h.shards[1].setReason("a later reason is not kept");
    h.shards[1].publish(.offline, .follower, 4, 4);

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
    try testing.expectEqual(@as(usize, silent_loss.PATH_COUNT), s0.get("losses").?.object.count());

    const s1 = shards[1].object;
    try testing.expectEqualStrings("offline", s1.get("state").?.string);
    try testing.expectEqualStrings("segment 00000001.flseg failed its \"checksum\"", s1.get("reason").?.string);
    try testing.expectEqual(@as(i64, 3), s1.get("losses").?.object.get("segment_unreadable").?.integer);
}
