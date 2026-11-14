//! Several groups on one node: each group is its own simulator (its log,
//! machines and checker, its replica the server's), stepped together on one
//! clock. A crash or restart is the node's, so group 0 decides it and every
//! other group follows. Groups talk through a mailbox that delays, drops,
//! duplicates and reorders frames, and loses all it holds when the node
//! crashes, so a protocol on top must stand without the mailbox's order or
//! delivery. The channel oracle checks what crosses it: every message a
//! sender keeps is applied by its receiver exactly once and in order.
//!
//! One replica per group: more than one shard exists only on one node.
//! Groups that replicate need a shared convergence core and shared
//! membership changes, which this doesn't do yet.

const std = @import("std");
const PRNG = @import("stdx").PRNG;
const simulator = @import("simulator.zig");
const scenario_mod = @import("scenario.zig");

const Allocator = std.mem.Allocator;
const Simulator = simulator.Simulator;
const Scenario = scenario_mod.Scenario;

pub const MAX_GROUPS = 16;
pub const GroupId = u8;

/// The node every group lives on.
const NODE: @import("network.zig").NodeId = 1;

// ═══════════════════════════════════════════════════════════════════════════
// The mailbox
// ═══════════════════════════════════════════════════════════════════════════

pub const Faults = struct {
    delay_min_ms: u64 = 0,
    delay_max_ms: u64 = 0,
    drop_percent: u8 = 0,
    duplicate_percent: u8 = 0,

    /// From the scenario's network faults: the mailbox is in-process, but a
    /// protocol that held only on a perfect one would hold by luck.
    pub fn of(s: *const Scenario) Faults {
        return .{
            .delay_min_ms = s.msg_delay_min_ms,
            .delay_max_ms = s.msg_delay_max_ms,
            .drop_percent = s.drop_percent,
            .duplicate_percent = s.duplicate_percent,
        };
    }
};

pub const Frame = struct {
    from: GroupId,
    to: GroupId,
    deliver_at: u64,
    seq: u64,
    bytes: []u8,
};

pub const Mailbox = struct {
    allocator: Allocator,
    faults: Faults,
    prng: PRNG,
    queue: std.ArrayListUnmanaged(Frame) = .empty,
    seq: u64 = 0,
    sent: u64 = 0,
    delivered: u64 = 0,
    dropped: u64 = 0,
    duplicated: u64 = 0,
    /// Frames lost because the node crashed with them in flight.
    lost_in_crash: u64 = 0,

    pub fn init(allocator: Allocator, seed: u64, faults: Faults) Mailbox {
        // Its own stream, salted apart from the groups' simulators, so a
        // mailbox fault never moves a group's draws.
        return .{ .allocator = allocator, .faults = faults, .prng = PRNG.init(seed ^ 0x3A11_B0C5) };
    }

    pub fn deinit(self: *Mailbox) void {
        for (self.queue.items) |f| self.allocator.free(f.bytes);
        self.queue.deinit(self.allocator);
    }

    /// Queues a copy of `bytes`, maybe twice, maybe not at all.
    pub fn send(self: *Mailbox, now: u64, from: GroupId, to: GroupId, bytes: []const u8) !void {
        self.sent += 1;
        const r = self.prng.random();
        if (r.uintLessThan(u8, 100) < self.faults.drop_percent) {
            self.dropped += 1;
            return;
        }
        const copies: u8 = if (r.uintLessThan(u8, 100) < self.faults.duplicate_percent) 2 else 1;
        self.duplicated += copies - 1;
        for (0..copies) |_| {
            const delay = r.intRangeAtMost(u64, self.faults.delay_min_ms, @max(self.faults.delay_min_ms, self.faults.delay_max_ms));
            const owned = try self.allocator.dupe(u8, bytes);
            errdefer self.allocator.free(owned);
            self.seq += 1;
            try self.queue.append(self.allocator, .{ .from = from, .to = to, .deliver_at = now + delay, .seq = self.seq, .bytes = owned });
        }
    }

    /// Moves the frames due by `now` into `out`, in (deliver_at, seq)
    /// order: random delays reorder them. The caller frees each frame's
    /// bytes with `release`.
    pub fn deliverDue(self: *Mailbox, now: u64, out: *std.ArrayListUnmanaged(Frame)) !void {
        var i: usize = 0;
        while (i < self.queue.items.len) {
            if (self.queue.items[i].deliver_at <= now) {
                try out.append(self.allocator, self.queue.swapRemove(i));
            } else i += 1;
        }
        std.mem.sort(Frame, out.items, {}, struct {
            fn lt(_: void, a: Frame, b: Frame) bool {
                return if (a.deliver_at != b.deliver_at) a.deliver_at < b.deliver_at else a.seq < b.seq;
            }
        }.lt);
        self.delivered += out.items.len;
    }

    pub fn release(self: *Mailbox, f: Frame) void {
        self.allocator.free(f.bytes);
    }

    /// The node crashed: what was in flight is gone.
    pub fn crash(self: *Mailbox) void {
        self.lost_in_crash += self.queue.items.len;
        for (self.queue.items) |f| self.allocator.free(f.bytes);
        self.queue.clearRetainingCapacity();
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// The channel oracle
// ═══════════════════════════════════════════════════════════════════════════

pub const ChannelViolation = struct {
    from: GroupId,
    to: GroupId,
    seq: u64,
    detail: []const u8,
};

/// What crossed each channel, told by the machines: each send as its sender
/// applies it, and each apply as its receiver applies it, both with the log
/// index they applied at. After a crash the harness says only which index a
/// group's log kept, never what its machines think they hold: a group that
/// lost its record of an apply but kept the apply's effect must not take
/// the message again. It holds every channel to exactly once, in order: a
/// receiver applies seq n + 1 after n, never one twice, never one its
/// sender doesn't hold, and in the end every message its sender holds.
pub const ChannelOracle = struct {
    allocator: Allocator,
    channels: [MAX_GROUPS][MAX_GROUPS]Channel = @splat(@splat(.{})),
    violations: std.ArrayListUnmanaged(ChannelViolation) = .empty,
    applies: u64 = 0,
    /// A sender may lose what its receiver applied: with no data directory,
    /// or ephemeral, a crash keeps nothing and promises nothing.
    sender_loss_legal: bool = false,

    const Logged = struct { hash: u64, index: u64 };

    const Channel = struct {
        /// Each sent message, seq n at [n - 1], with the sender's index.
        sent: std.ArrayListUnmanaged(Logged) = .empty,
        /// Each applied message, seq n at [n - 1], with the receiver's index.
        applied: std.ArrayListUnmanaged(Logged) = .empty,

        fn appliedThrough(c: *const Channel) u64 {
            return c.applied.items.len;
        }
    };

    pub fn init(allocator: Allocator) ChannelOracle {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *ChannelOracle) void {
        for (&self.channels) |*row| for (row) |*c| {
            c.sent.deinit(self.allocator);
            c.applied.deinit(self.allocator);
        };
        self.violations.deinit(self.allocator);
    }

    fn fail(self: *ChannelOracle, from: GroupId, to: GroupId, seq: u64, detail: []const u8) void {
        self.violations.append(self.allocator, .{ .from = from, .to = to, .seq = seq, .detail = detail }) catch @panic("oracle OOM");
    }

    /// The sender applied a send at `index`: seqs go up by one per channel.
    pub fn sent(self: *ChannelOracle, from: GroupId, to: GroupId, seq: u64, hash: u64, index: u64) !void {
        const c = &self.channels[from][to];
        if (seq != c.sent.items.len + 1) return self.fail(from, to, seq, "a sender's seqs skipped or repeated");
        try c.sent.append(self.allocator, .{ .hash = hash, .index = index });
    }

    /// The receiver applied a message at `index`.
    pub fn applied(self: *ChannelOracle, from: GroupId, to: GroupId, seq: u64, hash: u64, index: u64) !void {
        const c = &self.channels[from][to];
        self.applies += 1;
        const through = c.appliedThrough();
        if (seq <= through) return self.fail(from, to, seq, "applied a message twice");
        if (seq != through + 1) return self.fail(from, to, seq, "applied a message past a gap");
        if (seq > c.sent.items.len) return self.fail(from, to, seq, "applied a message its sender doesn't hold");
        if (c.sent.items[seq - 1].hash != hash) return self.fail(from, to, seq, "applied another message than the one sent at its seq");
        try c.applied.append(self.allocator, .{ .hash = hash, .index = index });
    }

    /// `group` came back from a crash with its log through `kept`: its sends
    /// and applies above that are gone. A receiver that applied a send now
    /// gone applied a message whose cause is gone.
    pub fn recovered(self: *ChannelOracle, group: GroupId, kept: u64) void {
        for (0..MAX_GROUPS) |peer_| {
            const peer: GroupId = @intCast(peer_);
            const out = &self.channels[group][peer];
            var n = out.sent.items.len;
            while (n > 0 and out.sent.items[n - 1].index > kept) n -= 1;
            if (n < out.sent.items.len) {
                if (out.appliedThrough() > n) {
                    if (!self.sender_loss_legal) self.fail(group, peer, out.appliedThrough(), "applied a message its sender lost");
                    // Legal here, so the seqs start over on both sides.
                    out.applied.shrinkRetainingCapacity(n);
                }
                out.sent.shrinkRetainingCapacity(n);
            }
            const in = &self.channels[peer][group];
            var m = in.applied.items.len;
            while (m > 0 and in.applied.items[m - 1].index > kept) m -= 1;
            in.applied.shrinkRetainingCapacity(m);
        }
    }

    /// Every message each sender holds was applied: once the channels have
    /// drained, at the end.
    pub fn drained(self: *const ChannelOracle) bool {
        for (&self.channels) |*row| for (row) |*c| {
            if (c.appliedThrough() < c.sent.items.len) return false;
        };
        return true;
    }

    pub fn finish(self: *ChannelOracle) void {
        for (&self.channels, 0..) |*row, from| for (row, 0..) |*c, to| {
            if (c.appliedThrough() < c.sent.items.len) self.fail(@intCast(from), @intCast(to), c.appliedThrough() + 1, "a message its sender holds was never applied");
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// The driver
// ═══════════════════════════════════════════════════════════════════════════

pub const Summary = struct {
    ok: bool,
    groups: []simulator.Summary,
    crashes: u64,
    frames_sent: u64,
    frames_delivered: u64,
    frames_lost_in_crash: u64,
    channel_applies: u64,
    channel_violations: usize,
};

pub const Groups = struct {
    allocator: Allocator,
    sims: []Simulator,
    mailbox: Mailbox,
    oracle: ChannelOracle,
    delivery: std.ArrayListUnmanaged(Frame) = .empty,
    /// What a delivered frame does: nothing until a machine sends.
    on_frame: ?*const fn (groups: *Groups, f: Frame) anyerror!void = null,

    /// `scenario` is group 0's, run as it would run alone, with one node.
    /// The other groups take its timing but decide no faults: the node's
    /// crashes and restarts are group 0's.
    pub fn init(allocator: Allocator, scenario: Scenario, group_count: u8, options: simulator.Options) !Groups {
        std.debug.assert(group_count >= 1 and group_count <= MAX_GROUPS);
        std.debug.assert(scenario.node_count == 1);
        const sims = try allocator.alloc(Simulator, group_count);
        var made: usize = 0;
        errdefer {
            for (sims[0..made]) |*s| s.deinit();
            allocator.free(sims);
        }
        for (sims, 0..) |*sim, g| {
            var s = scenario;
            if (g > 0) {
                // Each group its own streams: the golden-ratio constant
                // spreads consecutive group numbers across the seed space.
                s.seed = scenario.seed ^ (@as(u64, g) *% 0x9E37_79B9_7F4A_7C15);
                s.crash_permille = 0;
                s.restart_permille = 0;
                s.wipe_permille = 0;
            }
            sim.* = try Simulator.init(allocator, s, options);
            made += 1;
        }
        return .{
            .allocator = allocator,
            .sims = sims,
            .mailbox = Mailbox.init(allocator, scenario.seed, Faults.of(&scenario)),
            .oracle = ChannelOracle.init(allocator),
        };
    }

    pub fn deinit(self: *Groups) void {
        for (self.sims) |*s| s.deinit();
        self.allocator.free(self.sims);
        for (self.delivery.items) |f| self.mailbox.release(f);
        self.delivery.deinit(self.allocator);
        self.mailbox.deinit();
        self.oracle.deinit();
    }

    fn now(self: *const Groups) u64 {
        return self.sims[0].now;
    }

    /// Every group converged, and the channels drained: nothing in flight,
    /// every message its sender holds applied.
    fn converged(self: *Groups) bool {
        for (self.sims) |*s| if (!s.isConverged()) return false;
        return self.mailbox.queue.items.len == 0 and self.oracle.drained();
    }

    fn failed(self: *const Groups) bool {
        for (self.sims) |*s| if (s.checker.violations.items.len > 0) return true;
        return self.oracle.violations.items.len > 0;
    }

    /// The node's fate carried from group 0 to every other group, and
    /// each group that came back told to the oracle by what its log kept.
    fn followFate(self: *Groups, was_up: bool) !void {
        const up = self.sims[0].nodeUp(NODE);
        for (self.sims[1..]) |*s| try s.followFate(NODE, up);
        if (was_up and !up) self.mailbox.crash();
        if (!was_up and up) for (self.sims, 0..) |*s, g| self.oracle.recovered(@intCast(g), s.keptIndex(NODE));
    }

    /// One tick of every group, the node's fate carried to all of them,
    /// then the mailbox's due frames.
    fn step(self: *Groups) !void {
        const was_up = self.sims[0].nodeUp(NODE);
        for (self.sims) |*s| try s.advance();
        try self.followFate(was_up);
        const up = self.sims[0].nodeUp(NODE);
        self.delivery.clearRetainingCapacity();
        try self.mailbox.deliverDue(self.now(), &self.delivery);
        while (self.delivery.items.len > 0) {
            const f = self.delivery.orderedRemove(0);
            defer self.mailbox.release(f);
            if (!up) continue;
            if (self.on_frame) |handle| try handle(self, f);
        }
    }

    pub fn run(self: *Groups) !Summary {
        while (!self.sims[0].safetyDone() and !self.failed()) try self.step();
        var converging = false;
        if (!self.failed()) {
            converging = true;
            const was_up = self.sims[0].nodeUp(NODE);
            var deadline: u64 = 0;
            for (self.sims) |*s| deadline = @max(deadline, try s.beginConvergence());
            try self.followFate(was_up);
            // The network heals for convergence, and so does the mailbox:
            // latency stays, losses stop.
            self.mailbox.faults.drop_percent = 0;
            self.mailbox.faults.duplicate_percent = 0;
            while (self.now() < deadline and !self.failed()) {
                try self.step();
                if (self.converged()) break;
            }
            // Every message a sender holds was applied by now, or the run
            // fails: no channel may stall.
            if (!self.failed()) self.oracle.finish();
        }
        const summaries = try self.allocator.alloc(simulator.Summary, self.sims.len);
        var ok = self.oracle.violations.items.len == 0;
        for (self.sims, summaries) |*s, *out| {
            out.* = s.finish(converging and s.isConverged());
            ok = ok and out.ok;
        }
        return .{
            .ok = ok,
            .groups = summaries,
            .crashes = self.sims[0].crashes,
            .frames_sent = self.mailbox.sent,
            .frames_delivered = self.mailbox.delivered,
            .frames_lost_in_crash = self.mailbox.lost_in_crash,
            .channel_applies = self.oracle.applies,
            .channel_violations = self.oracle.violations.items.len,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn oneNodeScenario(seed: u64) Scenario {
    var s = Scenario.calm(seed);
    s.node_count = 1;
    s.crash_permille = 5;
    s.restart_permille = 100;
    s.ticks_safety = 4_000;
    return s;
}

test "vopr groups: groups on one node run their own logs, and all go down and come back with the node" {
    for ([_]scenario_mod.DurabilityMode{ .sync, .async_flush }) |durability| {
        var scenario = oneNodeScenario(5);
        scenario.durability = durability;
        var groups = try Groups.init(testing.allocator, scenario, 4, .{});
        defer groups.deinit();
        const s = try groups.run();
        defer testing.allocator.free(s.groups);
        try testing.expect(s.ok);
        try testing.expect(s.crashes > 0);
        for (s.groups) |g| {
            try testing.expect(g.ok);
            try testing.expect(g.ops_acked > 0);
            // Every group saw the node's every crash, no more.
            try testing.expectEqual(s.crashes, g.crashes);
        }
    }
}

test "vopr groups: group 0 of a multi-group run replays as the same scenario run alone" {
    // Through the safety phase, where every fault is drawn: convergence
    // ends once every group converges, so a multi-group run's group 0 can
    // tick on past where it would stop alone.
    const scenario = oneNodeScenario(9);
    var alone = try Simulator.init(testing.allocator, scenario, .{});
    defer alone.deinit();
    while (!alone.safetyDone()) try alone.advance();
    const a = alone.finish(false);
    var groups = try Groups.init(testing.allocator, scenario, 3, .{});
    defer groups.deinit();
    while (!groups.sims[0].safetyDone()) try groups.step();
    const g = groups.sims[0].finish(false);
    try testing.expect(a.ok and g.ok);
    try testing.expect(a.ops_acked > 0 and a.crashes > 0);
    try testing.expectEqual(a.canonical_hash, g.canonical_hash);
    try testing.expectEqual(a.ops_acked, g.ops_acked);
    try testing.expectEqual(a.crashes, g.crashes);
}

test "vopr groups: the mailbox delays, drops, duplicates and reorders, and a crash loses what's in flight" {
    var mb = Mailbox.init(testing.allocator, 1, .{ .delay_min_ms = 1, .delay_max_ms = 30, .drop_percent = 20, .duplicate_percent = 20 });
    defer mb.deinit();
    for (0..1000) |i| {
        var b: [8]u8 = undefined;
        std.mem.writeInt(u64, &b, i, .little);
        try mb.send(0, 0, 1, &b);
    }
    try testing.expect(mb.dropped > 100 and mb.duplicated > 100);
    var out: std.ArrayListUnmanaged(Frame) = .empty;
    defer out.deinit(testing.allocator);
    try mb.deliverDue(30, &out);
    var reordered = false;
    var last: u64 = 0;
    for (out.items, 0..) |f, k| {
        const v = std.mem.readInt(u64, f.bytes[0..8], .little);
        if (k > 0 and v < last) reordered = true;
        last = v;
        mb.release(f);
    }
    try testing.expect(reordered);
    try testing.expectEqual(mb.sent - mb.dropped + mb.duplicated, out.items.len);
    out.clearRetainingCapacity();
    var calm = Mailbox.init(testing.allocator, 1, .{ .delay_min_ms = 1, .delay_max_ms = 5 });
    defer calm.deinit();
    try calm.send(40, 1, 0, "x");
    calm.crash();
    try calm.deliverDue(1000, &out);
    try testing.expectEqual(@as(usize, 0), out.items.len);
    try testing.expectEqual(@as(u64, 1), calm.lost_in_crash);
}

test "vopr groups: the channel oracle holds a channel to exactly once, in order" {
    // Group 0 sends to group 1; indexes are each group's own log's.
    const Case = struct { detail: []const u8, run: *const fn (o: *ChannelOracle) anyerror!void };
    const cases = [_]Case{
        .{
            .detail = "",
            .run = struct {
                fn f(o: *ChannelOracle) !void {
                    try o.sent(0, 1, 1, 11, 5);
                    try o.sent(0, 1, 2, 12, 6);
                    try o.applied(0, 1, 1, 11, 3);
                    // The receiver's crash kept its log through 2: the apply at
                    // 3 is gone, effect and all, and it takes the message again.
                    o.recovered(1, 2);
                    try o.applied(0, 1, 1, 11, 3);
                    try o.applied(0, 1, 2, 12, 4);
                    o.finish();
                }
            }.f,
        },
        .{
            .detail = "applied a message twice",
            .run = struct {
                fn f(o: *ChannelOracle) !void {
                    // The receiver's crash kept the apply at 3, so its effect
                    // survived; taking the message again applies it twice, even
                    // if its machine lost its record of the apply.
                    try o.sent(0, 1, 1, 11, 5);
                    try o.applied(0, 1, 1, 11, 3);
                    o.recovered(1, 3);
                    try o.applied(0, 1, 1, 11, 4);
                }
            }.f,
        },
        .{ .detail = "applied a message past a gap", .run = struct {
            fn f(o: *ChannelOracle) !void {
                try o.sent(0, 1, 1, 11, 1);
                try o.sent(0, 1, 2, 12, 2);
                try o.applied(0, 1, 2, 12, 1);
            }
        }.f },
        .{ .detail = "applied a message its sender doesn't hold", .run = struct {
            fn f(o: *ChannelOracle) !void {
                try o.applied(2, 1, 1, 11, 1);
            }
        }.f },
        .{ .detail = "applied another message than the one sent at its seq", .run = struct {
            fn f(o: *ChannelOracle) !void {
                try o.sent(0, 1, 1, 11, 1);
                try o.applied(0, 1, 1, 99, 1);
            }
        }.f },
        .{
            .detail = "applied a message its sender lost",
            .run = struct {
                fn f(o: *ChannelOracle) !void {
                    // Delivered before its send was durable, then the sender's
                    // crash took the send.
                    try o.sent(0, 1, 1, 11, 7);
                    try o.applied(0, 1, 1, 11, 2);
                    o.recovered(0, 6);
                }
            }.f,
        },
        .{ .detail = "a message its sender holds was never applied", .run = struct {
            fn f(o: *ChannelOracle) !void {
                try o.sent(1, 0, 1, 11, 1);
                o.finish();
            }
        }.f },
        .{ .detail = "a sender's seqs skipped or repeated", .run = struct {
            fn f(o: *ChannelOracle) !void {
                try o.sent(0, 1, 2, 12, 1);
            }
        }.f },
    };
    for (cases) |c| {
        var o = ChannelOracle.init(testing.allocator);
        defer o.deinit();
        try c.run(&o);
        if (c.detail.len == 0) {
            try testing.expectEqual(@as(usize, 0), o.violations.items.len);
        } else {
            try testing.expectEqual(@as(usize, 1), o.violations.items.len);
            try testing.expectEqualStrings(c.detail, o.violations.items[0].detail);
        }
    }
    // With nothing promised across a crash, a sender losing what was
    // applied is no violation, and both sides start over.
    var o = ChannelOracle.init(testing.allocator);
    defer o.deinit();
    o.sender_loss_legal = true;
    try o.sent(0, 1, 1, 11, 7);
    try o.applied(0, 1, 1, 11, 2);
    o.recovered(0, 6);
    try o.sent(0, 1, 1, 21, 7);
    try o.applied(0, 1, 1, 21, 3);
    try testing.expectEqual(@as(usize, 0), o.violations.items.len);
}

test "vopr groups: frames reach the handler only while the node is up" {
    var scenario = oneNodeScenario(13);
    scenario.crash_permille = 2;
    var groups = try Groups.init(testing.allocator, scenario, 2, .{});
    defer groups.deinit();
    const Counter = struct {
        var handled: u64 = 0;
        fn handle(gs: *Groups, f: Frame) !void {
            try testing.expect(gs.sims[f.to].nodeUp(NODE));
            handled += 1;
            // Keep the mailbox busy for a while: a frame answers with another.
            if (handled < 2000) try gs.mailbox.send(gs.now(), f.to, f.from, f.bytes);
        }
    };
    Counter.handled = 0;
    groups.on_frame = Counter.handle;
    for (0..8) |_| try groups.mailbox.send(0, 0, 1, "ping");
    const s = try groups.run();
    defer testing.allocator.free(s.groups);
    try testing.expect(s.ok);
    try testing.expect(s.crashes > 0);
    try testing.expect(Counter.handled > 100);
    try testing.expect(s.frames_lost_in_crash > 0);
}
