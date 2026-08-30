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

const Allocator = std.mem.Allocator;

pub const NodeId = raft_node.NodeId;
pub const MAX_NODES = 8;

/// An AppendEntries request with entries the message owns — the sender's
/// log moves on while the message is in flight.
pub const AppendReq = struct {
    term: u64,
    leader_id: NodeId,
    prev_log_index: u64,
    prev_log_term: u64,
    leader_commit: u64,
    entries: []OwnedEntry,
};

pub const OwnedEntry = struct {
    entry_type: u8,
    flags: u16,
    term: u64,
    index: u64,
    timestamp_ns: u64,
    payload: []u8,
};

pub const Body = union(enum) {
    vote_req: raft_node.VoteRequest,
    vote_resp: raft_node.VoteResponse,
    append_req: AppendReq,
    append_resp: raft_node.AppendResponse,
};

pub const Message = struct {
    from: NodeId,
    to: NodeId,
    deliver_at: u64,
    seq: u64,
    body: Body,
};

fn freeBody(allocator: Allocator, body: *const Body) void {
    switch (body.*) {
        .append_req => |req| {
            for (req.entries) |e| allocator.free(e.payload);
            allocator.free(req.entries);
        },
        else => {},
    }
}

fn cloneBody(allocator: Allocator, body: *const Body) !Body {
    return switch (body.*) {
        .append_req => |req| blk: {
            const entries = try allocator.alloc(OwnedEntry, req.entries.len);
            var cloned: usize = 0;
            errdefer {
                for (entries[0..cloned]) |e| allocator.free(e.payload);
                allocator.free(entries);
            }
            for (req.entries, 0..) |e, i| {
                entries[i] = e;
                entries[i].payload = try allocator.dupe(u8, e.payload);
                cloned += 1;
            }
            var out = req;
            out.entries = entries;
            break :blk .{ .append_req = out };
        },
        else => body.*,
    };
}

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
        for (self.queue.items) |*m| freeBody(self.allocator, &m.body);
        self.queue.deinit(self.allocator);
    }

    fn cut(self: *const SimNetwork, a: NodeId, b: NodeId) bool {
        if (self.isolated[a - 1] or self.isolated[b - 1]) return true;
        if (self.partition_active and self.side[a - 1] != self.side[b - 1]) return true;
        return false;
    }

    /// Takes ownership of `body`'s allocations, even when the message is
    /// dropped.
    pub fn send(
        self: *SimNetwork,
        prng: *PRNG,
        scenario: *const Scenario,
        now: u64,
        from: NodeId,
        to: NodeId,
        body: Body,
    ) !void {
        const r = prng.random();
        const faultless = self.core_healed and self.core[from - 1] and self.core[to - 1];
        if (self.cut(from, to) or (!faultless and r.uintLessThan(u8, 100) < scenario.drop_percent)) {
            freeBody(self.allocator, &body);
            self.dropped += 1;
            return;
        }
        const delay = r.intRangeAtMost(u64, scenario.msg_delay_min_ms, scenario.msg_delay_max_ms);
        try self.enqueue(.{
            .from = from,
            .to = to,
            .deliver_at = now + delay,
            .seq = 0,
            .body = body,
        });
        if (!faultless and r.uintLessThan(u8, 100) < scenario.duplicate_percent) {
            const dup_delay = r.intRangeAtMost(u64, scenario.msg_delay_min_ms, scenario.msg_delay_max_ms);
            const cloned = try cloneBody(self.allocator, &self.queue.items[self.queue.items.len - 1].body);
            self.duplicated += 1;
            try self.enqueue(.{
                .from = from,
                .to = to,
                .deliver_at = now + dup_delay,
                .seq = 0,
                .body = cloned,
            });
        }
    }

    fn enqueue(self: *SimNetwork, msg: Message) !void {
        var m = msg;
        m.seq = self.seq;
        self.seq += 1;
        try self.queue.append(self.allocator, m);
    }

    fn messageOrder(_: void, a: Message, b: Message) bool {
        if (a.deliver_at != b.deliver_at) return a.deliver_at < b.deliver_at;
        return a.seq < b.seq;
    }

    /// Remove and return every message due at `now`, in deterministic
    /// (deliver_at, seq) order. Caller frees each body via `release`.
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
        freeBody(self.allocator, &msg.body);
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
    const p1 = try testing.allocator.dupe(u8, "one");
    const entries1 = try testing.allocator.alloc(OwnedEntry, 1);
    entries1[0] = .{ .entry_type = 1, .flags = 0, .term = 1, .index = 1, .timestamp_ns = 0, .payload = p1 };
    try net.send(&prng, &scenario, 0, 1, 2, .{ .append_req = .{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .leader_commit = 0,
        .entries = entries1,
    } });
    try net.send(&prng, &scenario, 0, 1, 2, .{ .vote_resp = .{ .term = 1, .vote_granted = true, .from = 2 } });

    var due: std.ArrayListUnmanaged(Message) = .empty;
    defer due.deinit(testing.allocator);
    // Calm delay is at least 1 tick; nothing due at time 0.
    try net.deliverDue(0, &due);
    try testing.expectEqual(@as(usize, 0), due.items.len);
    try net.deliverDue(1_000_000, &due);
    try testing.expectEqual(@as(usize, 2), due.items.len);
    try testing.expect(due.items[0].seq < due.items[1].seq);
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

    try net.send(&prng, &scenario, 0, a, b, .{ .vote_resp = .{ .term = 1, .vote_granted = true, .from = a } });
    try testing.expectEqual(@as(u64, 1), net.dropped);
    try testing.expectEqual(@as(usize, 0), net.queue.items.len);

    net.maybeHeal(99);
    try testing.expect(net.partition_active);
    net.maybeHeal(100);
    try testing.expect(!net.partition_active);

    try net.send(&prng, &scenario, 100, a, b, .{ .vote_resp = .{ .term = 1, .vote_granted = true, .from = a } });
    try testing.expectEqual(@as(usize, 1), net.queue.items.len);
}

test "vopr network: isolation is permanent" {
    var prng = PRNG.init(11);
    const scenario = Scenario.calm(11);
    var net = SimNetwork.init(testing.allocator);
    defer net.deinit();

    net.isolate(3);
    try net.send(&prng, &scenario, 0, 1, 3, .{ .vote_resp = .{ .term = 1, .vote_granted = true, .from = 1 } });
    try net.send(&prng, &scenario, 0, 3, 1, .{ .vote_resp = .{ .term = 1, .vote_granted = true, .from = 3 } });
    try testing.expectEqual(@as(u64, 2), net.dropped);
    net.healAll();
    try net.send(&prng, &scenario, 0, 1, 3, .{ .vote_resp = .{ .term = 1, .vote_granted = true, .from = 1 } });
    try testing.expectEqual(@as(u64, 3), net.dropped);
}
