//! The channel driver between groups over a network that drops, duplicates
//! and reorders frames, with leaders that start over. Each group applies
//! its own log at once and calls an index durable some ticks after it
//! applies it. Every probe must be applied exactly once, in order, and
//! every outbox trimmed to empty.

const std = @import("std");
const ce = @import("../protocol/channel_entries.zig");
const channels = @import("channels.zig");
const driver_mod = @import("channel_driver.zig");
const probe = @import("channel_probe.zig");
const machine_mod = @import("machine.zig");
const store_mod = @import("store.zig");
const view_mod = @import("view.zig");
const persistence = @import("../storage/persistence.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const PRNG = @import("stdx").PRNG;

const testing = std.testing;
const Allocator = std.mem.Allocator;
const Probed = channels.Channels(&.{probe.machine});
const machines = [_]machine_mod.Machine{ probe.machine, Probed.machine };

const Faults = struct { drop: u8 = 0, dup: u8 = 0, delay_max: u64 = 0, durable_lag: u64 = 0, leader_changes: u16 = 0 };

const Frame = struct { from: u16, to: u16, at: u64, seq: u64, bytes: []u8 };

const Group = struct {
    id: u16,
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    driver: driver_mod.Driver,
    pending: std.ArrayListUnmanaged(struct { etype: entry_mod.EntryType, bytes: []u8 }) = .empty,
    index: u64 = 0,
    /// The tick each index applied at, index i at [i - 1].
    applied_at: std.ArrayListUnmanaged(u64) = .empty,
    sent: u64 = 0,
};

const Sim = struct {
    allocator: Allocator,
    groups: []Group,
    frames: std.ArrayListUnmanaged(Frame) = .empty,
    frame_seq: u64 = 0,
    prng: PRNG,
    faults: Faults,
    now: u64 = 0,
    /// Every proposal refused, as a full log refuses it.
    refuse_proposals: bool = false,

    fn init(allocator: Allocator, count: u16, seed: u64, faults: Faults) !Sim {
        const groups = try allocator.alloc(Group, count);
        for (groups, 0..) |*g, i| {
            g.* = .{
                .id = @intCast(i),
                .store = store_mod.Store.init(allocator),
                .scratch = std.heap.ArenaAllocator.init(allocator),
                .driver = try driver_mod.Driver.init(allocator, @intCast(i), count),
            };
            g.store.place = .{ .group = @intCast(i), .count = count };
            g.store.strict_keys = true;
            g.driver.reset(1);
        }
        return .{ .allocator = allocator, .groups = groups, .prng = PRNG.init(seed), .faults = faults };
    }

    fn deinit(self: *Sim) void {
        for (self.groups) |*g| {
            g.store.deinit();
            g.scratch.deinit();
            g.driver.deinit();
            for (g.pending.items) |p| self.allocator.free(p.bytes);
            g.pending.deinit(self.allocator);
            g.applied_at.deinit(self.allocator);
        }
        self.allocator.free(self.groups);
        for (self.frames.items) |f| self.allocator.free(f.bytes);
        self.frames.deinit(self.allocator);
    }

    const Io = struct {
        sim: *Sim,
        me: u16,

        pub fn sendFrame(io: Io, to: u16, bytes: []const u8) bool {
            const s = io.sim;
            const r = s.prng.random();
            if (r.uintLessThan(u8, 100) < s.faults.drop) return true;
            const copies: u8 = if (r.uintLessThan(u8, 100) < s.faults.dup) 2 else 1;
            for (0..copies) |_| {
                s.frame_seq += 1;
                const owned = s.allocator.dupe(u8, bytes) catch return false;
                s.frames.append(s.allocator, .{ .from = io.me, .to = to, .at = s.now + 1 + r.uintAtMost(u64, s.faults.delay_max), .seq = s.frame_seq, .bytes = owned }) catch return false;
            }
            return true;
        }

        pub fn propose(io: Io, etype: entry_mod.EntryType, payload: []const u8) bool {
            if (io.sim.refuse_proposals) return false;
            const g = &io.sim.groups[io.me];
            const owned = io.sim.allocator.dupe(u8, payload) catch return false;
            g.pending.append(io.sim.allocator, .{ .etype = etype, .bytes = owned }) catch return false;
            return true;
        }
    };

    fn durableOf(self: *const Sim, g: *const Group) u64 {
        var d: u64 = 0;
        for (g.applied_at.items, 1..) |at, i| {
            if (at + self.faults.durable_lag <= self.now) d = i;
        }
        return d;
    }

    fn apply(self: *Sim, g: *Group, etype: entry_mod.EntryType, payload: []const u8) !void {
        g.index += 1;
        var h = std.mem.zeroes(entry_mod.Header);
        h.entry_type = @backingInt(etype);
        h.index = g.index;
        h.payload_len = @intCast(payload.len);
        const e: entry_mod.Entry = .{ .header = h, .payload = payload };
        var r = persistence.Result.start(&e);
        const m = machine_mod.ownerOf(&machines, etype).?;
        try machine_mod.applyEntry(m, &g.store, &g.scratch, &e, &r);
        try g.applied_at.append(self.allocator, self.now);
    }

    fn probeFrom(self: *Sim, g: *Group, slot: u16) !void {
        var body: [16]u8 = undefined;
        _ = bodyOf(&body, g.id, g.sent, false);
        g.sent += 1;
        var buf: [64]u8 = undefined;
        try self.apply(g, .channel_probe, try ce.Probe.encodeInto(&buf, .{ .slot = slot, .body = &body }));
    }

    /// A history probe from `g` to a slot of its own: on its history lane.
    fn historyFrom(self: *Sim, g: *Group, slot_hint: u16) !void {
        const count: u16 = @intCast(self.groups.len);
        const slot = slot_hint - slot_hint % count + g.id;
        var body: [16]u8 = undefined;
        _ = bodyOf(&body, g.id, g.sent, true);
        g.sent += 1;
        var buf: [64]u8 = undefined;
        try self.apply(g, .channel_probe, try ce.Probe.encodeInto(&buf, .{ .slot = slot, .body = &body }));
    }

    fn probeBody(self: *Sim, g: *Group, slot: u16, body: []const u8) !void {
        var buf: [64]u8 = undefined;
        try self.apply(g, .channel_probe, try ce.Probe.encodeInto(&buf, .{ .slot = slot, .body = body }));
    }

    /// The group's next unpark, proposed and applied as its leader would.
    fn unpark(self: *Sim, g: *Group) !bool {
        if (!g.driver.buffers()) return false;
        const next = Probed.nextUnpark(.{ .store = &g.store }, g.driver.buf, g.driver.msgs, null) orelse return false;
        const owned = try self.allocator.dupe(u8, next.entry);
        defer self.allocator.free(owned);
        try self.apply(g, .channel_unpark, owned);
        return true;
    }

    fn step(self: *Sim) !void {
        self.now += 1;
        // Due frames, in (time, seq) order.
        var due: std.ArrayListUnmanaged(Frame) = .empty;
        defer due.deinit(self.allocator);
        var i: usize = 0;
        while (i < self.frames.items.len) {
            if (self.frames.items[i].at <= self.now) try due.append(self.allocator, self.frames.swapRemove(i)) else i += 1;
        }
        std.mem.sort(Frame, due.items, {}, struct {
            fn lt(_: void, a: Frame, b: Frame) bool {
                return if (a.at != b.at) a.at < b.at else a.seq < b.seq;
            }
        }.lt);
        for (due.items) |f| {
            defer self.allocator.free(f.bytes);
            const g = &self.groups[f.to];
            g.driver.onFrame(f.from, f.bytes, self.now, .{ .store = &g.store }, self.durableOf(g), false, Io{ .sim = self, .me = g.id });
        }
        for (self.groups) |*g| {
            // Its log: what its driver proposed, applied in order.
            while (g.pending.items.len > 0) {
                const p = g.pending.orderedRemove(0);
                defer self.allocator.free(p.bytes);
                try self.apply(g, p.etype, p.bytes);
            }
            g.driver.tick(self.now, .{ .store = &g.store }, self.durableOf(g), Io{ .sim = self, .me = g.id });
        }
    }

    /// Every message every group sent is applied, and every outbox is
    /// trimmed to empty.
    fn settled(self: *const Sim) bool {
        for (self.groups) |*a| for (self.groups) |*b| for (channels.Lane.all) |lane| {
            const meta = channels.metaOf(.{ .store = &a.store }, b.id, lane);
            const sent = meta.next_seq - 1;
            if (meta.acked != sent) return false;
            const in = channels.inOf(.{ .store = &b.store }, a.id, lane);
            if ((if (in) |x| x.applied else 0) != sent) return false;
        };
        return true;
    }
};

/// Probe `n` of group `g`'s name: a history probe's ends 'H'.
fn bodyOf(buf: *[16]u8, g: u16, n: u64, history: bool) []const u8 {
    return (if (history) std.fmt.bufPrint(buf, "h{d:0>3}{d:0>11}H", .{ g, n }) else std.fmt.bufPrint(buf, "g{d:0>3}-{d:0>11}", .{ g, n })) catch unreachable;
}

/// Whether a workload's probe `n` is a history probe: every fourth, when
/// it sends any.
fn isHistoryProbe(history: bool, n: u64) bool {
    return history and n % 4 == 3;
}

/// Runs a workload of `probes` per group to random slots, under `faults`,
/// and checks the end.
fn runWorkload(seed: u64, count: u16, probes: u64, faults: Faults) !driver_mod.Counts {
    return runWorkloadWith(seed, count, probes, faults, false);
}

/// `runWorkload`, every fourth probe a history one when `history` is set.
fn runWorkloadWith(seed: u64, count: u16, probes: u64, faults: Faults, history: bool) !driver_mod.Counts {
    var sim = try Sim.init(testing.allocator, count, seed, faults);
    defer sim.deinit();
    var r = PRNG.init(seed ^ 0xA11);
    var changes: u16 = 0;
    while (sim.now < 200_000) {
        for (sim.groups) |*g| if (g.sent < probes and r.random().uintLessThan(u8, 4) == 0) {
            const slot = r.random().uintLessThan(u16, 4096);
            if (isHistoryProbe(history, g.sent)) try sim.historyFrom(g, slot) else try sim.probeFrom(g, slot);
        };
        // A new leader: the driver's memory starts over from state, and
        // the frames in flight are lost.
        if (changes < faults.leader_changes and r.random().uintLessThan(u16, 500) == 0) {
            changes += 1;
            const g = &sim.groups[r.random().uintLessThan(u16, count)];
            g.driver.reset(g.driver.term + 1);
        }
        try sim.step();
        const all_sent = for (sim.groups) |*g| {
            if (g.sent < probes) break false;
        } else true;
        if (all_sent and sim.settled()) break;
    }
    try testing.expect(sim.settled());
    // Each probe applied once, by its slot's owner, in its channel's order.
    var total = driver_mod.Counts{};
    for (sim.groups) |*a| {
        for (0..a.sent) |n| {
            var body: [16]u8 = undefined;
            _ = bodyOf(&body, a.id, n, isHistoryProbe(history, n));
            var found: u8 = 0;
            for (sim.groups) |*b| {
                var kb: [1 + probe.NAME_LEN]u8 = undefined;
                if (try (view_mod.ReadView{ .store = &b.store }).get(probe.GotFam, probe.gotKey(&kb, &body))) |got| {
                    try testing.expectEqual(a.id, got.v.from);
                    // A history probe is its sender's own.
                    if (isHistoryProbe(history, n)) try testing.expectEqual(a.id, b.id);
                    found += 1;
                }
            }
            try testing.expectEqual(@as(u8, 1), found);
        }
        const c = a.driver.counts;
        total.resends += c.resends;
        total.gaps += c.gaps;
        total.batches += c.batches;
        total.trims_proposed += c.trims_proposed;
        total.acks_refused += c.acks_refused;
    }
    return total;
}

test "channel driver: every message is applied once, in order, and every outbox trimmed, on a calm network" {
    const c = try runWorkload(1, 3, 200, .{});
    try testing.expect(c.batches > 0 and c.trims_proposed > 0);
}

test "channel driver: the same, with drops, duplicates, reordering, a lagging durable index and new leaders" {
    for ([_]u64{ 2, 3, 4, 5 }) |seed| {
        const c = try runWorkload(seed, 4, 150, .{ .drop = 20, .dup = 10, .delay_max = 30, .durable_lag = 15, .leader_changes = 6 });
        try testing.expect(c.resends > 0);
    }
}

test "channel driver: history and main messages on one node, each applied once, under the same faults and new leaders" {
    for ([_]u64{ 6, 7, 8 }) |seed| {
        const c = try runWorkloadWith(seed, 3, 160, .{ .drop = 20, .dup = 10, .delay_max = 30, .durable_lag = 15, .leader_changes = 6 }, true);
        try testing.expect(c.resends > 0 and c.trims_proposed > 0);
    }
}

test "channel driver: no traffic, no frames" {
    var sim = try Sim.init(testing.allocator, 4, 9, .{});
    defer sim.deinit();
    for (0..1000) |_| try sim.step();
    try testing.expectEqual(@as(u64, 0), sim.frame_seq);
    for (sim.groups) |*g| try testing.expectEqual(@as(u64, 0), g.index);
}

test "channel driver: an ack above what was sent is refused, and a frame's sender is the caller's, not the frame's" {
    var sim = try Sim.init(testing.allocator, 2, 11, .{});
    defer sim.deinit();
    const g = &sim.groups[0];
    try sim.probeFrom(g, 1);
    var buf: [64]u8 = undefined;
    buf[0] = @backingInt(driver_mod.Kind.answer);
    const body = try driver_mod.Answer.encodeInto(buf[1..], .{ .lane = 2, .gen_term = 1, .gen_counter = 0, .applied = 5, .durable = 5, .gap = 0, .status = 0, .pressure = .{ .bytes = "", .count = 0 } });
    g.driver.onFrame(1, buf[0 .. 1 + body.len], 1, .{ .store = &g.store }, 0, false, Sim.Io{ .sim = &sim, .me = 0 });
    try testing.expectEqual(@as(u64, 1), g.driver.counts.acks_refused);
    // From a group past the node's count: ignored outright.
    g.driver.onFrame(7, buf[0 .. 1 + body.len], 1, .{ .store = &g.store }, 0, false, Sim.Io{ .sim = &sim, .me = 0 });
    try testing.expectEqual(@as(u64, 1), g.driver.counts.acks_refused);
}

fn answerFrame(buf: []u8, v: driver_mod.Answer.Value) []const u8 {
    buf[0] = @backingInt(driver_mod.Kind.answer);
    const body = driver_mod.Answer.encodeInto(buf[1..], v) catch unreachable;
    return buf[0 .. 1 + body.len];
}

test "channel driver: a gap rewinds once per generation; one from an older batch rewinds nothing" {
    for ([_]driver_mod.Mutant{ .none, .no_gen_filter }) |mutant| {
        var sim = try Sim.init(testing.allocator, 2, 13, .{});
        defer sim.deinit();
        const g = &sim.groups[0];
        g.driver.mutant = mutant;
        for (0..5) |_| try sim.probeFrom(g, 1);
        // Seqs 1..5 go out in generation 0.
        g.driver.tick(1, .{ .store = &g.store }, 100, Sim.Io{ .sim = &sim, .me = 0 });
        try testing.expectEqual(@as(u64, 5), g.driver.peers[1].lanes[2].sent_through);
        var buf: [64]u8 = undefined;
        // The receiver lost 3..5: it says gap at 3, to generation 0.
        const gap = answerFrame(&buf, .{ .lane = 2, .gen_term = 1, .gen_counter = 0, .applied = 2, .durable = 2, .gap = 3, .status = 0, .pressure = .{ .bytes = "", .count = 0 } });
        g.driver.onFrame(1, gap, 2, .{ .store = &g.store }, 100, false, Sim.Io{ .sim = &sim, .me = 0 });
        try testing.expectEqual(@as(u64, 2), g.driver.peers[1].lanes[2].sent_through);
        try testing.expectEqual(@as(u32, 1), g.driver.peers[1].lanes[2].gen_counter);
        // Generation 1 resends 3..5.
        g.driver.tick(3, .{ .store = &g.store }, 100, Sim.Io{ .sim = &sim, .me = 0 });
        try testing.expectEqual(@as(u64, 5), g.driver.peers[1].lanes[2].sent_through);
        // A duplicate of the first gap, late: it answers an older batch.
        g.driver.onFrame(1, gap, 4, .{ .store = &g.store }, 100, false, Sim.Io{ .sim = &sim, .me = 0 });
        switch (mutant) {
            .none => {
                try testing.expectEqual(@as(u64, 5), g.driver.peers[1].lanes[2].sent_through);
                try testing.expectEqual(@as(u32, 1), g.driver.peers[1].lanes[2].gen_counter);
            },
            // Without the filter it rewinds again: the guard is what stops it.
            else => try testing.expectEqual(@as(u64, 2), g.driver.peers[1].lanes[2].sent_through),
        }
    }
}

test "channel driver: a gap from an earlier term's generation of the same counter rewinds nothing" {
    for ([_]driver_mod.Mutant{ .none, .no_gen_filter }) |mutant| {
        var sim = try Sim.init(testing.allocator, 2, 19, .{});
        defer sim.deinit();
        const g = &sim.groups[0];
        g.driver.mutant = mutant;
        for (0..5) |_| try sim.probeFrom(g, 1);
        g.driver.tick(1, .{ .store = &g.store }, 100, Sim.Io{ .sim = &sim, .me = 0 });
        // A new term: its first generation is counter 0 again, and sends 1..5.
        g.driver.reset(2);
        g.driver.tick(2, .{ .store = &g.store }, 100, Sim.Io{ .sim = &sim, .me = 0 });
        try testing.expectEqual(@as(u64, 5), g.driver.peers[1].lanes[2].sent_through);
        // A late gap answering term 1's batch, with the same counter.
        var buf: [64]u8 = undefined;
        const gap = answerFrame(&buf, .{ .lane = 2, .gen_term = 1, .gen_counter = 0, .applied = 2, .durable = 2, .gap = 3, .status = 0, .pressure = .{ .bytes = "", .count = 0 } });
        g.driver.onFrame(1, gap, 3, .{ .store = &g.store }, 100, false, Sim.Io{ .sim = &sim, .me = 0 });
        switch (mutant) {
            .none => {
                try testing.expectEqual(@as(u64, 5), g.driver.peers[1].lanes[2].sent_through);
                try testing.expectEqual(@as(u32, 0), g.driver.peers[1].lanes[2].gen_counter);
            },
            else => try testing.expectEqual(@as(u64, 2), g.driver.peers[1].lanes[2].sent_through),
        }
    }
}

test "channel driver: an applied ack past what was sent is refused even when its durable ack is in range" {
    for ([_]driver_mod.Mutant{ .none, .unbounded_ack }) |mutant| {
        var sim = try Sim.init(testing.allocator, 2, 23, .{});
        defer sim.deinit();
        const g = &sim.groups[0];
        g.driver.mutant = mutant;
        for (0..2) |_| try sim.probeFrom(g, 1);
        var buf: [64]u8 = undefined;
        const ack = answerFrame(&buf, .{ .lane = 2, .gen_term = 1, .gen_counter = 0, .applied = 5, .durable = 1, .gap = 0, .status = 0, .pressure = .{ .bytes = "", .count = 0 } });
        g.driver.onFrame(1, ack, 1, .{ .store = &g.store }, 100, false, Sim.Io{ .sim = &sim, .me = 0 });
        switch (mutant) {
            .none => {
                try testing.expectEqual(@as(u64, 1), g.driver.counts.acks_refused);
                try testing.expectEqual(@as(u64, 0), g.driver.peers[1].lanes[2].applied_ack);
                try testing.expectEqual(@as(u64, 0), g.driver.peers[1].lanes[2].durable_ack);
            },
            else => try testing.expectEqual(@as(u64, 5), g.driver.peers[1].lanes[2].applied_ack),
        }
    }
}

test "channel driver: a node of more groups than any fixed table, every message applied once" {
    _ = try runWorkload(29, 70, 3, .{});
}

test "channel driver: an ack past what this group's state says was sent is refused, and moves nothing" {
    for ([_]driver_mod.Mutant{ .none, .unbounded_ack }) |mutant| {
        var sim = try Sim.init(testing.allocator, 2, 17, .{});
        defer sim.deinit();
        const g = &sim.groups[0];
        g.driver.mutant = mutant;
        for (0..2) |_| try sim.probeFrom(g, 1);
        var buf: [64]u8 = undefined;
        // Two were sent; a receiver claiming 5 applied and durable is wrong.
        const ack = answerFrame(&buf, .{ .lane = 2, .gen_term = 1, .gen_counter = 0, .applied = 5, .durable = 5, .gap = 0, .status = 0, .pressure = .{ .bytes = "", .count = 0 } });
        g.driver.onFrame(1, ack, 1, .{ .store = &g.store }, 100, false, Sim.Io{ .sim = &sim, .me = 0 });
        switch (mutant) {
            .none => {
                try testing.expectEqual(@as(u64, 1), g.driver.counts.acks_refused);
                try testing.expectEqual(@as(u64, 0), g.driver.peers[1].lanes[2].durable_ack);
                try testing.expectEqual(@as(u64, 0), g.driver.peers[1].lanes[2].applied_ack);
            },
            // Without the bound it would trim past what was sent.
            else => try testing.expectEqual(@as(u64, 5), g.driver.peers[1].lanes[2].durable_ack),
        }
    }
}

test "channel driver: past a target's parked cap its messages wait at their sender with those behind them, and go once it drains" {
    channels.test_parked_target_max = 1;
    defer channels.test_parked_target_max = null;
    var sim = try Sim.init(testing.allocator, 2, 31, .{});
    defer sim.deinit();
    for (sim.groups) |*g| {
        g.driver.admits = Probed.admits;
        g.driver.sink_target = Probed.sinkTarget;
    }
    const a = &sim.groups[0];
    const b = &sim.groups[1];
    // b's slot 1 is full, by b's own channel, and one message to it parks.
    try sim.probeBody(b, 1, "fill-slot1-00001");
    for (0..40) |_| try sim.step();
    try sim.probeBody(a, 1, "parked-0000-0001");
    for (0..40) |_| try sim.step();
    try testing.expectEqual(@as(u64, 1), channels.targetParked(.{ .store = &b.store }, channelsTarget(1)).count);
    // Now over its cap: a message to slot 1 waits at a, and so does the one
    // to slot 3 behind it; the one to slot 3 before it goes.
    try sim.probeBody(a, 3, "before-0000-0002");
    try sim.probeBody(a, 1, "capped-0000-0003");
    try sim.probeBody(a, 3, "behind-0000-0004");
    // Past a resend or two.
    for (0..1000) |_| try sim.step();
    try testing.expect(hasGot(b, "before-0000-0002"));
    try testing.expect(!hasGot(b, "behind-0000-0004"));
    try testing.expect(b.driver.counts.busy > 0);
    // Its answers say why: a full target on b. That holds only a's sink
    // sends: its other lanes' sends to b are held by nothing.
    const said = a.driver.pressureOf(1, .sink, sim.now).?;
    try testing.expectEqual(@as(u16, 1), said.shard);
    try testing.expectEqual(@as(u8, @backingInt(driver_mod.Cause.target_full)), said.cause);
    try testing.expect(a.driver.pressureOf(1, .main, sim.now) == null);
    try testing.expect(a.driver.pressureOf(1, .history, sim.now) == null);
    try testing.expectEqual(@as(?driver_mod.Cause, null), a.driver.causeOf(1, .main, sim.now));
    try sim.probeBody(a, 3, "unheld-00000001M");
    for (0..200) |_| try sim.step();
    try testing.expect(hasGot(b, "unheld-00000001M"));
    // b's answers to it, on the main lane, name no full target either.
    try testing.expect(a.driver.pressureOf(1, .main, sim.now) == null);
    try testing.expect(a.driver.pressureOf(1, .sink, sim.now) != null);
    try testing.expectEqual(@as(u64, 2), channels.inOf(.{ .store = &b.store }, 0, .sink).?.applied);
    // b drains slot 1 by its own channel; the unpark empties it, and the
    // rest go, in order.
    try sim.probeBody(b, 1, "drain-slot1-0002");
    for (0..40) |_| try sim.step();
    try testing.expect(try sim.unpark(b));
    // Room on b, by its parked state: its answers stop naming a full
    // target at once, before a's batch is resent and taken.
    var fb: [64]u8 = undefined;
    b.driver.onFrame(0, emptyBatch(&fb, a.driver.term), sim.now, .{ .store = &b.store }, 0, false, Sim.Io{ .sim = &sim, .me = 1 });
    var out: [driver_mod.MAX_PRESSURE]driver_mod.Stall.Value = undefined;
    for (lastPressure(&sim, 0, &out)) |st| try testing.expect(st.cause != @backingInt(driver_mod.Cause.target_full));
    // The sender's backoff grew while it was busy: up to 5 s.
    for (0..12_000) |_| try sim.step();
    try testing.expect(hasGot(b, "parked-0000-0001") and hasGot(b, "capped-0000-0003") and hasGot(b, "behind-0000-0004"));
    try testing.expectEqual(@as(u64, 4), channels.inOf(.{ .store = &b.store }, 0, .sink).?.applied);
    // Taken: what holds a pipeline sending to b is gone, once no answer
    // carries it.
    try testing.expect(a.driver.pressureOf(1, .sink, sim.now) == null);
}

test "channel driver: a history or main message isn't held behind a sink's message its target is full for, on the same channel" {
    channels.test_parked_target_max = 1;
    defer channels.test_parked_target_max = null;
    var sim = try Sim.init(testing.allocator, 2, 37, .{});
    defer sim.deinit();
    for (sim.groups) |*g| {
        g.driver.admits = Probed.admits;
        g.driver.sink_target = Probed.sinkTarget;
    }
    // b's own slot 1 is full, and one of b's own messages to it parks.
    const b = &sim.groups[1];
    try sim.probeBody(b, 1, "fill-slot1-00001");
    for (0..40) |_| try sim.step();
    try sim.probeBody(b, 1, "parked-0000-0001");
    for (0..40) |_| try sim.step();
    try testing.expectEqual(@as(u64, 1), channels.targetParked(.{ .store = &b.store }, channelsTarget(1)).count);
    // Over its cap: b's next message to slot 1 waits, and the sink's one to
    // slot 3 behind it; a history message and a main one to slot 3 go.
    try sim.probeBody(b, 1, "capped-0000-0002");
    try sim.probeBody(b, 3, "behind-0000-0003");
    try sim.probeBody(b, 3, "hist-0000000000H");
    try sim.probeBody(b, 3, "main-0000000000M");
    for (0..1000) |_| try sim.step();
    try testing.expect(hasGot(b, "hist-0000000000H"));
    try testing.expect(hasGot(b, "main-0000000000M"));
    try testing.expect(!hasGot(b, "behind-0000-0003"));
    try testing.expectEqual(@as(u64, 1), channels.inOf(.{ .store = &b.store }, 1, .main).?.applied);
    try testing.expectEqual(@as(u64, 2), channels.inOf(.{ .store = &b.store }, 1, .sink).?.applied);
    try testing.expectEqual(@as(u64, 1), channels.inOf(.{ .store = &b.store }, 1, .history).?.applied);
    // And trimmed: nothing of it is left in b's outbox.
    try testing.expectEqual(@as(u64, 1), channels.metaOf(.{ .store = &b.store }, 1, .history).acked);
    // Drained, the main lane goes on.
    try sim.probeBody(b, 1, "drain-slot1-0005");
    for (0..40) |_| try sim.step();
    try testing.expect(try sim.unpark(b));
    for (0..12_000) |_| try sim.step();
    try testing.expect(hasGot(b, "capped-0000-0002") and hasGot(b, "behind-0000-0003"));
    try testing.expect(sim.settled());
}

var over_share = false;

fn shareOfAHundred(_: view_mod.ReadView) ?channels.Budget {
    return .{ .use = if (over_share) 200 else 0, .share = 100 };
}

test "channel driver: while its shard is over its share a sink's messages wait at their sender, say why, and go once use falls" {
    channels.test_budget = shareOfAHundred;
    defer channels.test_budget = null;
    over_share = true;
    defer over_share = false;
    var sim = try Sim.init(testing.allocator, 2, 37, .{});
    defer sim.deinit();
    for (sim.groups) |*g| {
        g.driver.admits = Probed.admits;
        g.driver.sink_target = Probed.sinkTarget;
    }
    const a = &sim.groups[0];
    const b = &sim.groups[1];
    try sim.probeBody(a, 1, "held-00000000001");
    for (0..1000) |_| try sim.step();
    try testing.expect(!hasGot(b, "held-00000000001"));
    try testing.expect(b.driver.counts.over_share > 0);
    try testing.expectEqual(@as(u64, 0), b.driver.sink_bytes);
    const said = a.driver.pressureOf(1, .sink, sim.now).?;
    try testing.expectEqual(@as(u16, 1), said.shard);
    try testing.expectEqual(@as(u8, @backingInt(driver_mod.Cause.over_share)), said.cause);
    // It holds only a's sink sends: a main message from a to b goes
    // meanwhile, and b's answers on the main lane name nothing.
    try sim.probeBody(a, 1, "unheld-00000001M");
    for (0..200) |_| try sim.step();
    try testing.expect(hasGot(b, "unheld-00000001M"));
    try testing.expect(!hasGot(b, "held-00000000001"));
    try testing.expect(a.driver.pressureOf(1, .main, sim.now) == null);
    try testing.expect(a.driver.pressureOf(1, .history, sim.now) == null);
    try testing.expect(a.driver.pressureOf(1, .sink, sim.now) != null);
    // Use falls: the sender's next resend is taken, and applied releases it.
    over_share = false;
    for (0..12_000) |_| try sim.step();
    try testing.expect(hasGot(b, "held-00000000001"));
    try testing.expectEqual(@as(u64, 0), b.driver.sink_bytes);
    try testing.expect(a.driver.pressureOf(1, .sink, sim.now) == null);
}

/// An empty batch from `from`, as a probe is.
fn emptyBatch(buf: []u8, term: u64) []const u8 {
    buf[0] = @backingInt(driver_mod.Kind.batch);
    const body = driver_mod.Batch.encodeInto(buf[1..], .{ .lane = 2, .gen_term = term, .gen_counter = 0, .first_seq = 1, .messages = .{ .bytes = "", .count = 0 } }) catch unreachable;
    return buf[0 .. 1 + body.len];
}

/// The pressure in the last answer `sim` holds for `to`.
fn lastPressure(sim: *Sim, to: u16, out: *[driver_mod.MAX_PRESSURE]driver_mod.Stall.Value) []driver_mod.Stall.Value {
    var i = sim.frames.items.len;
    while (i > 0) {
        i -= 1;
        const f = sim.frames.items[i];
        if (f.to != to) continue;
        const frame = driver_mod.Frame.decode(f.bytes) orelse continue;
        if (frame != .answer) continue;
        var it = frame.answer.pressure.iterator();
        var n: usize = 0;
        while (it.next() catch null) |st| : (n += 1) out[n] = st;
        return out[0..n];
    }
    return out[0..0];
}

test "channel driver: a sender keeps the pressure an answer carried, probes while it does, and drops it a second after the last" {
    var sim = try Sim.init(testing.allocator, 2, 43, .{});
    defer sim.deinit();
    const a = &sim.groups[0];
    const b = &sim.groups[1];
    // b can't flush: every answer it gives says so.
    b.driver.own_cause = .cannot_flush;
    try sim.probeBody(a, 1, "pressure-0000-01");
    for (0..20) |_| try sim.step();
    const said = a.driver.pressureOf(1, .sink, sim.now).?;
    try testing.expectEqual(@as(u16, 1), said.shard);
    try testing.expectEqual(@as(u8, @backingInt(driver_mod.Cause.cannot_flush)), said.cause);
    // Idle, it is kept fresh by probes, which b answers with it.
    for (0..1500) |_| try sim.step();
    try testing.expect(a.driver.pressureOf(1, .sink, sim.now) != null);
    try testing.expect(a.driver.peers[1].lanes[@backingInt(channels.Lane.sink)].probe_ms > 0);
    // Answers that carry none clear it.
    b.driver.own_cause = null;
    for (0..1500) |_| try sim.step();
    try testing.expect(a.driver.pressureOf(1, .sink, sim.now) == null);
    // An idle channel asks nothing, so learns none; the next message does,
    // and it lasts a second past the last answer that carried it.
    b.driver.own_cause = .cannot_flush;
    for (0..1500) |_| try sim.step();
    try testing.expect(a.driver.pressureOf(1, .sink, sim.now) == null);
    try sim.probeBody(a, 1, "pressure-0000-02");
    for (0..20) |_| try sim.step();
    const at = a.driver.peers[1].lanes[@backingInt(channels.Lane.sink)].pressure_ms;
    try testing.expect(a.driver.pressureOf(1, .sink, at + driver_mod.PRESSURE_EXPIRY_MS - 1) != null);
    try testing.expect(a.driver.pressureOf(1, .sink, at + driver_mod.PRESSURE_EXPIRY_MS) == null);
}

test "channel driver: what a receiver proposed to a target and hasn't applied counts against its cap, so one batch can't pass it" {
    var sim = try Sim.init(testing.allocator, 2, 31, .{});
    defer sim.deinit();
    for (sim.groups) |*g| {
        g.driver.admits = Probed.admits;
        g.driver.sink_target = Probed.sinkTarget;
    }
    const a = &sim.groups[0];
    const b = &sim.groups[1];
    // b's slot 1 is full, by b's own channel.
    try sim.probeBody(b, 1, "fill-slot1-00001");
    for (0..40) |_| try sim.step();
    // Five to it in one batch, each one size.
    const bodies = [_][]const u8{ "inflight-0000-01", "inflight-0000-02", "inflight-0000-03", "inflight-0000-04", "inflight-0000-05" };
    for (bodies) |body| try sim.probeBody(a, 1, body);
    // In a's outbox; its frame reaches b a step or more later.
    try sim.step();
    var ok: [channels.OUT_KEY]u8 = undefined;
    const read_a: @import("view.zig").ReadView = .{ .store = &a.store };
    const one = (try read_a.get(channels.OutFam, channels.outKey(&ok, 1, .sink, 1))).?.v.payload.len;
    // Room for two whole ones and part of a third: the third is the one
    // admitted below the cap.
    channels.test_parked_target_max = 2 * one + 1;
    defer channels.test_parked_target_max = null;
    for (0..1000) |_| try sim.step();
    try testing.expectEqual(@as(u64, 3), channels.targetParked(.{ .store = &b.store }, channelsTarget(1)).count);
    try testing.expect(!hasGot(b, "inflight-0000-04"));
    // Applied, none is still counted in flight.
    try testing.expectEqual(@as(u64, 0), b.driver.inFlight(0, channelsTarget(1)).target);
    try testing.expectEqual(@as(u64, 0), b.driver.in_flight_bytes);
}

test "channel driver: a batch whose inbound the log refuses counts nothing in flight, and goes once it takes it" {
    var sim = try Sim.init(testing.allocator, 2, 37, .{});
    defer sim.deinit();
    for (sim.groups) |*g| {
        g.driver.admits = Probed.admits;
        g.driver.sink_target = Probed.sinkTarget;
    }
    const a = &sim.groups[0];
    const b = &sim.groups[1];
    try sim.probeBody(b, 1, "fill-slot1-00001");
    for (0..40) |_| try sim.step();
    try sim.probeBody(a, 1, "refused-0000-001");
    try sim.probeBody(a, 1, "refused-0000-002");
    // Every proposal refused: b answers busy, and nothing it tracked stays.
    sim.refuse_proposals = true;
    for (0..200) |_| try sim.step();
    try testing.expect(b.driver.counts.busy > 0);
    try testing.expectEqual(@as(u64, 0), b.driver.in_flight_bytes);
    try testing.expectEqual(@as(usize, 0), b.driver.target_bytes.count());
    try testing.expectEqual(@as(u64, 0), channels.targetParked(.{ .store = &b.store }, channelsTarget(1)).count);
    // Taken again: both park, in order.
    sim.refuse_proposals = false;
    for (0..12_000) |_| try sim.step();
    try testing.expectEqual(@as(u64, 2), channels.targetParked(.{ .store = &b.store }, channelsTarget(1)).count);
    try testing.expectEqual(@as(u64, 0), b.driver.in_flight_bytes);
}

fn channelsTarget(slot: u16) [ce.TARGET_LEN]u8 {
    var t: [ce.TARGET_LEN]u8 = @splat(0);
    t[0] = @backingInt(probe.machine.entry_types[0]);
    t[1..6].* = "probe".*;
    std.mem.writeInt(u16, t[6..8], slot, .big);
    return t;
}

fn hasGot(g: *Group, name: []const u8) bool {
    var kb: [1 + probe.NAME_LEN]u8 = undefined;
    return ((view_mod.ReadView{ .store = &g.store }).get(probe.GotFam, probe.gotKey(&kb, name[0..probe.NAME_LEN])) catch null) != null;
}
