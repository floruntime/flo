//! VOPR SimNetwork — deterministic message transport between simulated nodes
//!
//! Owned, delayed, droppable, duplicable messages. Every choice (drop,
//! duplicate, delay, partition membership) comes from the single run PRNG,
//! ties break on a monotonically increasing sequence number, and delivery
//! order is a sort over (deliver_at, seq) — no wall clock, no threads, no
//! hash-map iteration in any decision path.

const std = @import("std");
const PRNG = @import("stdx").PRNG;
const raft_node = @import("../raft/node.zig");
const Scenario = @import("scenario.zig").Scenario;
const MsgType = @import("../raft/transport.zig").MsgType;

const Allocator = std.mem.Allocator;

pub const NodeId = raft_node.NodeId;
pub const MAX_NODES = 8;

/// One Raft frame on the wire: its type and its serialized payload, which
/// the message owns (the sender's buffer is reused for the next frame).
pub const Message = struct {
    from: NodeId,
    to: NodeId,
    deliver_at: u64,
    seq: u64,
    msg_type: MsgType,
    payload: []u8,
};

pub const SimNetwork = struct {
    allocator: Allocator,
    queue: std.ArrayListUnmanaged(Message),
    seq: u64,

    // One random bipartition at a time, healed on schedule. Node ids are
    // 1-based; `side` is indexed by id - 1.
    partition_active: bool,
    partition_heal_at: u64,
    side: [MAX_NODES]bool,
    /// Nodes permanently cut from the rest (liveness-phase non-core).
    isolated: [MAX_NODES]bool,
    /// Single links cut both ways, for scenarios that need a partial
    /// partition (two nodes that cannot talk, a third that reaches both).
    pair_cut: [MAX_NODES][MAX_NODES]bool = @splat(@splat(false)),
    /// Liveness-phase core: once set, links between core members carry no
    /// drop/dup faults — core faults are frozen, only latency remains.
    core_healed: bool,
    core: [MAX_NODES]bool,

    dropped: u64,
    duplicated: u64,
    delivered: u64,

    pub fn init(allocator: Allocator) SimNetwork {
        return .{
            .allocator = allocator,
            .queue = .empty,
            .seq = 0,
            .partition_active = false,
            .partition_heal_at = 0,
            .side = @splat(false),
            .isolated = @splat(false),
            .core_healed = false,
            .core = @splat(false),
            .dropped = 0,
            .duplicated = 0,
            .delivered = 0,
        };
    }

    pub fn deinit(self: *SimNetwork) void {
        for (self.queue.items) |m| self.allocator.free(m.payload);
        self.queue.deinit(self.allocator);
    }

    fn cut(self: *const SimNetwork, a: NodeId, b: NodeId) bool {
        if (self.isolated[a - 1] or self.isolated[b - 1]) return true;
        if (self.pair_cut[a - 1][b - 1]) return true;
        if (self.partition_active and self.side[a - 1] != self.side[b - 1]) return true;
        return false;
    }

    /// Copies `payload`: the network owns what it carries.
    pub fn send(
        self: *SimNetwork,
        prng: *PRNG,
        scenario: *const Scenario,
        now: u64,
        from: NodeId,
        to: NodeId,
        msg_type: MsgType,
        payload: []const u8,
    ) !void {
        const r = prng.random();
        const faultless = self.core_healed and self.core[from - 1] and self.core[to - 1];
        if (self.cut(from, to) or (!faultless and r.uintLessThan(u8, 100) < scenario.drop_percent)) {
            self.dropped += 1;
            return;
        }
        const delay = r.intRangeAtMost(u64, scenario.msg_delay_min_ms, scenario.msg_delay_max_ms);
        try self.enqueue(.{
            .from = from,
            .to = to,
            .deliver_at = now + delay,
            .seq = 0,
            .msg_type = msg_type,
            .payload = try self.allocator.dupe(u8, payload),
        });
        if (!faultless and r.uintLessThan(u8, 100) < scenario.duplicate_percent) {
            const dup_delay = r.intRangeAtMost(u64, scenario.msg_delay_min_ms, scenario.msg_delay_max_ms);
            const cloned = try self.allocator.dupe(u8, payload);
            self.duplicated += 1;
            try self.enqueue(.{
                .from = from,
                .to = to,
                .deliver_at = now + dup_delay,
                .seq = 0,
                .msg_type = msg_type,
                .payload = cloned,
            });
        }
    }

    fn enqueue(self: *SimNetwork, msg: Message) !void {
        var m = msg;
        m.seq = self.seq;
        self.seq += 1;
        self.queue.append(self.allocator, m) catch |err| {
            self.allocator.free(m.payload);
            return err;
        };
    }

    fn messageOrder(_: void, a: Message, b: Message) bool {
        if (a.deliver_at != b.deliver_at) return a.deliver_at < b.deliver_at;
        return a.seq < b.seq;
    }

    /// Remove and return every message due at `now`, in deterministic
    /// (deliver_at, seq) order. Caller frees each payload via `release`.
    pub fn deliverDue(self: *SimNetwork, now: u64, out: *std.ArrayListUnmanaged(Message)) !void {
        var i: usize = 0;
        while (i < self.queue.items.len) {
            if (self.queue.items[i].deliver_at <= now) {
                try out.append(self.allocator, self.queue.items[i]);
                _ = self.queue.swapRemove(i);
            } else {
                i += 1;
            }
        }
        std.mem.sort(Message, out.items, {}, messageOrder);
        self.delivered += out.items.len;
    }

    pub fn release(self: *SimNetwork, msg: *const Message) void {
        self.allocator.free(msg.payload);
    }

    pub fn startPartition(self: *SimNetwork, prng: *PRNG, node_count: u8, heal_at: u64) void {
        const r = prng.random();
        // Ensure both sides are non-empty, or the "partition" is a no-op.
        var any_a = false;
        var any_b = false;
        while (!any_a or !any_b) {
            any_a = false;
            any_b = false;
            for (0..node_count) |i| {
                self.side[i] = r.boolean();
                if (self.side[i]) any_a = true else any_b = true;
            }
        }
        self.partition_active = true;
        self.partition_heal_at = heal_at;
    }

    pub fn maybeHeal(self: *SimNetwork, now: u64) void {
        if (self.partition_active and now >= self.partition_heal_at) {
            self.partition_active = false;
        }
    }

    pub fn healAll(self: *SimNetwork) void {
        self.partition_active = false;
    }

    pub fn cutPair(self: *SimNetwork, a: NodeId, b: NodeId) void {
        self.pair_cut[a - 1][b - 1] = true;
        self.pair_cut[b - 1][a - 1] = true;
    }

    pub fn isolate(self: *SimNetwork, id: NodeId) void {
        self.isolated[id - 1] = true;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "vopr network: delivery is ordered by (deliver_at, seq) and owned memory is freed" {
    var prng = PRNG.init(7);
    const scenario = Scenario.calm(7);
    var net = SimNetwork.init(testing.allocator);
    defer net.deinit();

    // Two messages due at the same tick keep send order via seq.
    var buf = "one".*;
    try net.send(&prng, &scenario, 0, 1, 2, .append_entries, &buf);
    // The network copied it: the sender's buffer is free to change.
    buf[0] = 'x';
    try net.send(&prng, &scenario, 0, 1, 2, .request_vote_response, "two");

    var due: std.ArrayListUnmanaged(Message) = .empty;
    defer due.deinit(testing.allocator);
    // Calm delay is at least 1 tick; nothing due at time 0.
    try net.deliverDue(0, &due);
    try testing.expectEqual(@as(usize, 0), due.items.len);
    try net.deliverDue(1_000_000, &due);
    try testing.expectEqual(@as(usize, 2), due.items.len);
    try testing.expect(due.items[0].seq < due.items[1].seq);
    try testing.expectEqualStrings("one", due.items[0].payload);
    for (due.items) |*m| net.release(m);
}

test "vopr network: partition drops cross-side traffic and heals on schedule" {
    var prng = PRNG.init(9);
    var scenario = Scenario.calm(9);
    scenario.msg_delay_min_ms = 1;
    scenario.msg_delay_max_ms = 1;
    var net = SimNetwork.init(testing.allocator);
    defer net.deinit();

    net.startPartition(&prng, 3, 100);
    // Find two nodes on opposite sides.
    var a: NodeId = 1;
    var b: NodeId = 1;
    for (1..4) |i| {
        for (1..4) |j| {
            if (net.side[i - 1] != net.side[j - 1]) {
                a = @intCast(i);
                b = @intCast(j);
            }
        }
    }
    try testing.expect(a != b);

    try net.send(&prng, &scenario, 0, a, b, .request_vote_response, "x");
    try testing.expectEqual(@as(u64, 1), net.dropped);
    try testing.expectEqual(@as(usize, 0), net.queue.items.len);

    net.maybeHeal(99);
    try testing.expect(net.partition_active);
    net.maybeHeal(100);
    try testing.expect(!net.partition_active);

    try net.send(&prng, &scenario, 100, a, b, .request_vote_response, "x");
    try testing.expectEqual(@as(usize, 1), net.queue.items.len);
}

test "vopr network: isolation is permanent" {
    var prng = PRNG.init(11);
    const scenario = Scenario.calm(11);
    var net = SimNetwork.init(testing.allocator);
    defer net.deinit();

    net.isolate(3);
    try net.send(&prng, &scenario, 0, 1, 3, .request_vote_response, "x");
    try net.send(&prng, &scenario, 0, 3, 1, .request_vote_response, "x");
    try testing.expectEqual(@as(u64, 2), net.dropped);
    net.healAll();
    try net.send(&prng, &scenario, 0, 1, 3, .request_vote_response, "x");
    try testing.expectEqual(@as(u64, 3), net.dropped);
}
