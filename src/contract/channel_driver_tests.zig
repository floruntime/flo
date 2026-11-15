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

    fn init(allocator: Allocator, count: u16, seed: u64, faults: Faults) !Sim {
        const groups = try allocator.alloc(Group, count);
        for (groups, 0..) |*g, i| {
            g.* = .{
                .id = @intCast(i),
                .store = store_mod.Store.init(allocator),
                .scratch = std.heap.ArenaAllocator.init(allocator),
                .driver = driver_mod.Driver.init(allocator, @intCast(i), count),
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
        _ = std.fmt.bufPrint(&body, "g{d}-{d:0>13}", .{ g.id, g.sent }) catch unreachable;
        g.sent += 1;
        var buf: [64]u8 = undefined;
        try self.apply(g, .channel_probe, try ce.Probe.encodeInto(&buf, .{ .slot = slot, .body = &body }));
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
        for (self.groups) |*a| for (self.groups) |*b| {
            const meta = channels.metaOf(.{ .store = &a.store }, b.id);
            const sent = meta.next_seq - 1;
            if (meta.acked != sent) return false;
            const in = channels.inOf(.{ .store = &b.store }, a.id);
            if ((if (in) |x| x.applied else 0) != sent) return false;
        };
        return true;
    }
};

/// Runs a workload of `probes` per group to random slots, under `faults`,
/// and checks the end.
fn runWorkload(seed: u64, count: u16, probes: u64, faults: Faults) !driver_mod.Counts {
    var sim = try Sim.init(testing.allocator, count, seed, faults);
    defer sim.deinit();
    var r = PRNG.init(seed ^ 0xA11);
    var changes: u16 = 0;
    while (sim.now < 200_000) {
        for (sim.groups) |*g| if (g.sent < probes and r.random().uintLessThan(u8, 4) == 0) try sim.probeFrom(g, r.random().uintLessThan(u16, 4096));
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
            _ = std.fmt.bufPrint(&body, "g{d}-{d:0>13}", .{ a.id, n }) catch unreachable;
            var found: u8 = 0;
            for (sim.groups) |*b| {
                var kb: [1 + probe.NAME_LEN]u8 = undefined;
                if (try (view_mod.ReadView{ .store = &b.store }).get(probe.GotFam, probe.gotKey(&kb, &body))) |got| {
                    try testing.expectEqual(a.id, got.v.from);
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
    const body = try driver_mod.Answer.encodeInto(buf[1..], .{ .gen_term = 1, .gen_counter = 0, .applied = 5, .durable = 5, .gap = 0, .status = 0 });
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
        try testing.expectEqual(@as(u64, 5), g.driver.peers[1].sent_through);
        var buf: [64]u8 = undefined;
        // The receiver lost 3..5: it says gap at 3, to generation 0.
        const gap = answerFrame(&buf, .{ .gen_term = 1, .gen_counter = 0, .applied = 2, .durable = 2, .gap = 3, .status = 0 });
        g.driver.onFrame(1, gap, 2, .{ .store = &g.store }, 100, false, Sim.Io{ .sim = &sim, .me = 0 });
        try testing.expectEqual(@as(u64, 2), g.driver.peers[1].sent_through);
        try testing.expectEqual(@as(u32, 1), g.driver.peers[1].gen_counter);
        // Generation 1 resends 3..5.
        g.driver.tick(3, .{ .store = &g.store }, 100, Sim.Io{ .sim = &sim, .me = 0 });
        try testing.expectEqual(@as(u64, 5), g.driver.peers[1].sent_through);
        // A duplicate of the first gap, late: it answers an older batch.
        g.driver.onFrame(1, gap, 4, .{ .store = &g.store }, 100, false, Sim.Io{ .sim = &sim, .me = 0 });
        switch (mutant) {
            .none => {
                try testing.expectEqual(@as(u64, 5), g.driver.peers[1].sent_through);
                try testing.expectEqual(@as(u32, 1), g.driver.peers[1].gen_counter);
            },
            // Without the filter it rewinds again: the guard is what stops it.
            else => try testing.expectEqual(@as(u64, 2), g.driver.peers[1].sent_through),
        }
    }
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
        const ack = answerFrame(&buf, .{ .gen_term = 1, .gen_counter = 0, .applied = 5, .durable = 5, .gap = 0, .status = 0 });
        g.driver.onFrame(1, ack, 1, .{ .store = &g.store }, 100, false, Sim.Io{ .sim = &sim, .me = 0 });
        switch (mutant) {
            .none => {
                try testing.expectEqual(@as(u64, 1), g.driver.counts.acks_refused);
                try testing.expectEqual(@as(u64, 0), g.driver.peers[1].durable_ack);
                try testing.expectEqual(@as(u64, 0), g.driver.peers[1].applied_ack);
            },
            // Without the bound it would trim past what was sent.
            else => try testing.expectEqual(@as(u64, 5), g.driver.peers[1].durable_ack),
        }
    }
}
