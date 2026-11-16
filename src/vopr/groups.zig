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

const channels_mod = @import("../contract/channels.zig");
const driver_mod = @import("../contract/channel_driver.zig");
const probe = @import("../contract/channel_probe.zig");
const machine_mod = @import("../contract/machine.zig");
const store_mod = @import("../contract/store.zig");
const view_mod = @import("../contract/view.zig");
const ce = @import("../protocol/channel_entries.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");

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
    /// Sends refused because the receiver's queue was full.
    refused: u64 = 0,

    /// Frames one receiver holds, as a shard's inbox ring does.
    pub const CAPACITY = 1024;

    pub fn init(allocator: Allocator, seed: u64, faults: Faults) Mailbox {
        // Its own stream, salted apart from the groups' simulators, so a
        // mailbox fault never moves a group's draws.
        return .{ .allocator = allocator, .faults = faults, .prng = PRNG.init(seed ^ 0x3A11_B0C5) };
    }

    pub fn deinit(self: *Mailbox) void {
        for (self.queue.items) |f| self.allocator.free(f.bytes);
        self.queue.deinit(self.allocator);
    }

    /// Queues a copy of `bytes` unless `to`'s queue is full, as a shard's
    /// inbox refuses: false then, and the sender knows.
    pub fn trySend(self: *Mailbox, now: u64, from: GroupId, to: GroupId, bytes: []const u8) bool {
        var held: usize = 0;
        for (self.queue.items) |f| held += @intFromBool(f.to == to);
        if (held >= CAPACITY) {
            self.refused += 1;
            return false;
        }
        self.send(now, from, to, bytes) catch return false;
        return true;
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
///
/// Every group here keeps a disk, so a sender losing a send its receiver
/// applied is always a violation. A run of groups with no data directory
/// would have to allow it: such a group promises nothing across a crash.
pub const ChannelOracle = struct {
    allocator: Allocator,
    channels: [MAX_GROUPS][MAX_GROUPS]Channel = @splat(@splat(.{})),
    violations: std.ArrayListUnmanaged(ChannelViolation) = .empty,
    applies: u64 = 0,
    /// Reports of what a group already reported, as its restart replays its
    /// log: each matched what was recorded.
    replays: u64 = 0,
    /// Messages waiting at the end on a receiver that is halted.
    held_by_halts: u64 = 0,

    const Logged = struct { hash: u64, index: u64, target: ?u16 = null };

    const Channel = struct {
        /// Each sent message, seq n at [n - 1], with the sender's index.
        sent: std.ArrayListUnmanaged(Logged) = .empty,
        /// Each applied message, seq n at [n - 1], with the receiver's index:
        /// taken in, whether it applied then or parked.
        applied: std.ArrayListUnmanaged(Logged) = .empty,
        /// Each message with a target that reached it, by seq, at the
        /// receiver's index: on arrival, or from parking.
        delivered: std.AutoHashMapUnmanaged(u64, u64) = .empty,

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
            c.delivered.deinit(self.allocator);
        };
        self.violations.deinit(self.allocator);
    }

    pub fn fail(self: *ChannelOracle, from: GroupId, to: GroupId, seq: u64, detail: []const u8) void {
        self.violations.append(self.allocator, .{ .from = from, .to = to, .seq = seq, .detail = detail }) catch @panic("oracle OOM");
    }

    /// The sender applied a send at `index`: seqs go up by one per channel.
    /// One already recorded is the sender's restart replaying its log, and
    /// must be the same send at the same index.
    pub fn sent(self: *ChannelOracle, from: GroupId, to: GroupId, seq: u64, hash: u64, index: u64, target: ?u16) !void {
        const c = &self.channels[from][to];
        if (seq >= 1 and seq <= c.sent.items.len) {
            const was = c.sent.items[seq - 1];
            if (was.index == index and was.hash == hash) {
                self.replays += 1;
                return;
            }
            return self.fail(from, to, seq, "a sender's seqs skipped or repeated");
        }
        if (seq != c.sent.items.len + 1) return self.fail(from, to, seq, "a sender's seqs skipped or repeated");
        try c.sent.append(self.allocator, .{ .hash = hash, .index = index, .target = target });
    }

    /// A message with a target reached it at the receiver's `index`: once,
    /// and after every earlier message from its sender to the same target.
    pub fn delivered(self: *ChannelOracle, from: GroupId, to: GroupId, seq: u64, index: u64) !void {
        const c = &self.channels[from][to];
        if (c.delivered.get(seq)) |was| {
            if (was == index) {
                self.replays += 1;
                return;
            }
            return self.fail(from, to, seq, "a parked message reached its target twice");
        }
        if (seq == 0 or seq > c.sent.items.len) return self.fail(from, to, seq, "a message reached its target that its sender doesn't hold");
        const target = c.sent.items[seq - 1].target orelse return self.fail(from, to, seq, "a message without a target was delivered as one");
        var it = c.delivered.keyIterator();
        while (it.next()) |s| if (s.* > seq and c.sent.items[s.* - 1].target == target)
            return self.fail(from, to, seq, "a message reached its target after a later one to it");
        try c.delivered.put(self.allocator, seq, index);
    }

    /// The receiver applied a message at `index`. One already recorded at
    /// that index is its restart replaying its log; at any other index it
    /// is the message applied again.
    pub fn applied(self: *ChannelOracle, from: GroupId, to: GroupId, seq: u64, hash: u64, index: u64) !void {
        const c = &self.channels[from][to];
        const through = c.appliedThrough();
        if (seq >= 1 and seq <= through) {
            const was = c.applied.items[seq - 1];
            if (was.index == index and was.hash == hash) {
                self.replays += 1;
                return;
            }
            return self.fail(from, to, seq, "applied a message twice");
        }
        self.applies += 1;
        if (seq != through + 1) return self.fail(from, to, seq, "applied a message past a gap");
        if (seq > c.sent.items.len) return self.fail(from, to, seq, "applied a message its sender doesn't hold");
        if (c.sent.items[seq - 1].hash != hash) return self.fail(from, to, seq, "applied another message than the one sent at its seq");
        try c.applied.append(self.allocator, .{ .hash = hash, .index = index });
    }

    /// `group` came back from a crash with its log through `kept`.
    pub fn recovered(self: *ChannelOracle, group: GroupId, kept: u64) void {
        var all: [MAX_GROUPS]?u64 = @splat(null);
        all[group] = kept;
        self.recoveredAll(&all);
    }

    /// The groups that came back from one crash, each with its log through
    /// `kept[g]`: their sends and applies above that are gone. Every
    /// group's applies are cut first, then every sender is checked, so a
    /// send and its apply that the crash took together are no violation,
    /// whichever group is numbered first. A receiver still holding the
    /// apply of a send now gone applied a message whose cause is gone.
    pub fn recoveredAll(self: *ChannelOracle, kept: []const ?u64) void {
        for (kept, 0..) |k, g| if (k) |keep| for (0..MAX_GROUPS) |peer| {
            const in = &self.channels[peer][g];
            var m = in.applied.items.len;
            while (m > 0 and in.applied.items[m - 1].index > keep) m -= 1;
            in.applied.shrinkRetainingCapacity(m);
            var gone: std.ArrayListUnmanaged(u64) = .empty;
            defer gone.deinit(self.allocator);
            var it = in.delivered.iterator();
            while (it.next()) |d| if (d.value_ptr.* > keep) gone.append(self.allocator, d.key_ptr.*) catch @panic("oracle OOM");
            for (gone.items) |s| _ = in.delivered.remove(s);
        };
        for (kept, 0..) |k, g_| if (k) |keep| for (0..MAX_GROUPS) |peer_| {
            const g: GroupId = @intCast(g_);
            const peer: GroupId = @intCast(peer_);
            const out = &self.channels[g][peer];
            var n = out.sent.items.len;
            while (n > 0 and out.sent.items[n - 1].index > keep) n -= 1;
            if (n == out.sent.items.len) continue;
            if (out.appliedThrough() > n) {
                self.fail(g, peer, out.appliedThrough(), "applied a message its sender lost");
                // Reported; the channel starts over from what both hold, so
                // the run goes on past it.
                out.applied.shrinkRetainingCapacity(n);
            }
            var lost: std.ArrayListUnmanaged(u64) = .empty;
            defer lost.deinit(self.allocator);
            var dit = out.delivered.keyIterator();
            while (dit.next()) |s| if (s.* > n) lost.append(self.allocator, s.*) catch @panic("oracle OOM");
            for (lost.items) |s| _ = out.delivered.remove(s);
            out.sent.shrinkRetainingCapacity(n);
        };
    }

    /// Every message each sender holds was applied, but to a receiver that
    /// is halted, where messages wait for its repair: once the channels
    /// have drained, at the end.
    pub fn drained(self: *const ChannelOracle, halted: []const bool) bool {
        for (&self.channels) |*row| for (row, 0..) |*c, to| {
            if (to < halted.len and halted[to]) continue;
            if (c.appliedThrough() < c.sent.items.len) return false;
            for (c.sent.items[0..c.appliedThrough()], 1..) |s, seq| {
                if (s.target != null and !c.delivered.contains(seq)) return false;
            }
        };
        return true;
    }

    /// Fails each channel whose receiver isn't halted and didn't apply all
    /// its sender holds; counts what waits on a halted one.
    pub fn finish(self: *ChannelOracle, halted: []const bool) void {
        for (&self.channels, 0..) |*row, from| for (row, 0..) |*c, to| {
            if (c.appliedThrough() >= c.sent.items.len) continue;
            if (to < halted.len and halted[to]) {
                self.held_by_halts += c.sent.items.len - c.appliedThrough();
                continue;
            }
            self.fail(@intCast(from), @intCast(to), c.appliedThrough() + 1, "a message its sender holds was never applied");
        };
        // Taken in and parked, but never reached its target.
        for (&self.channels, 0..) |*row, from| for (row, 0..) |*c, to| {
            if (to < halted.len and halted[to]) continue;
            for (c.sent.items[0..c.appliedThrough()], 1..) |s, seq| {
                if (s.target == null or c.delivered.contains(seq)) continue;
                self.fail(@intCast(from), @intCast(to), seq, "a parked message never reached its target");
                break;
            }
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
    probes_proposed: u64,
    /// Unparks the groups' leaders proposed: each a full target with room again.
    unparks: u64,
    /// Batches refused for a full target, and answers that named a stall:
    /// above 0, the caps were reached.
    busy: u64,
    pressured: u64,
    frames_refused: u64,
    flush_stalls: u64,
    held_by_halts: u64,
    /// In flight when the run ended, and not applied: 0 once drained.
    left_in_mailbox: usize,
};

pub const ChannelMutant = driver_mod.Mutant;
/// The channels module, for the planted bugs that live there.
pub const channels = channels_mod;

const Probed = channels_mod.Channels(&.{probe.machine});
const channel_machines = [_]machine_mod.Machine{ probe.machine, Probed.machine };

/// A probe's target slot, or null for a fill or drain mark, which has none.
fn probeTarget(payload: []const u8) ?u16 {
    var diag: @import("../protocol/schema.zig").Diagnostic = .{};
    const p = ce.Probe.decodeAll(payload, &diag) catch return null;
    if (std.mem.startsWith(u8, p.body, "fill") or std.mem.startsWith(u8, p.body, "drai")) return null;
    return p.slot;
}

/// One group's channels: the store its channel and probe machines keep,
/// and its leader's driver. Its machines apply through the group's
/// simulator (`Simulator.Extension`), live and in replay, and tell the
/// oracle each send and apply.
pub const GroupChannel = struct {
    groups: *Groups = undefined,
    g: GroupId,
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    driver: driver_mod.Driver,
    probes_proposed: u64 = 0,
    unparks: u64 = 0,
    /// The unpark this group's leader last proposed: one at a time.
    unpark_index: u64 = 0,
    unpark_term: u64 = 0,
    /// Where its next unpark looks from: past the last one's target.
    unpark_after: ?[channels_mod.TARGET_LEN]u8 = null,

    fn init(allocator: Allocator, g: GroupId, count: u16) !GroupChannel {
        const driver = try driver_mod.Driver.init(allocator, g, count);
        var c: GroupChannel = .{
            .g = g,
            .store = store_mod.Store.init(allocator),
            .scratch = std.heap.ArenaAllocator.init(allocator),
            .driver = driver,
        };
        c.store.place = .{ .group = g, .count = count };
        c.store.strict_keys = true;
        c.driver.admits = Probed.admits;
        c.driver.sink_target = Probed.sinkTarget;
        return c;
    }

    fn deinit(c: *GroupChannel) void {
        c.store.deinit();
        c.scratch.deinit();
        c.driver.deinit();
    }

    fn owns(t: u8) bool {
        return t == @backingInt(entry_mod.EntryType.channel_probe) or
            t == @backingInt(entry_mod.EntryType.channel_inbound) or
            t == @backingInt(entry_mod.EntryType.channel_trim) or
            t == @backingInt(entry_mod.EntryType.channel_unpark);
    }

    fn applyHook(ctx: *anyopaque, _: @import("network.zig").NodeId, e: *const entry_mod.Entry) void {
        const c: *GroupChannel = @ptrCast(@alignCast(ctx));
        c.apply(e) catch |err| c.groups.oracle.fail(c.g, c.g, e.header.index, switch (err) {
            error.Halt => "a channel entry halted its group",
            else => "a channel entry's apply failed",
        });
    }

    /// A restart: the store starts over and the log replays into it, and
    /// the driver's memory is gone.
    fn resetHook(ctx: *anyopaque, _: @import("network.zig").NodeId) void {
        const c: *GroupChannel = @ptrCast(@alignCast(ctx));
        c.store.clear();
        c.driver.reset(0);
    }

    fn extension(c: *GroupChannel) simulator.Extension {
        return .{ .ctx = c, .owns = owns, .apply = applyHook, .reset = resetHook };
    }

    fn read(c: *GroupChannel) view_mod.ReadView {
        return .{ .store = &c.store };
    }

    fn apply(c: *GroupChannel, e: *const entry_mod.Entry) !void {
        const etype = std.enums.fromInt(entry_mod.EntryType, e.header.entry_type) orelse return error.Halt;
        const count = c.groups.sims.len;
        const o = &c.groups.oracle;
        var sent_before: [MAX_GROUPS]u64 = undefined;
        var diag: @import("../protocol/schema.zig").Diagnostic = .{};
        const inbound: ?ce.Inbound.Value = if (etype == .channel_inbound) try ce.Inbound.decodeAll(e.payload, &diag) else null;
        if (etype == .channel_probe) for (0..count) |b| {
            sent_before[b] = channels_mod.metaOf(c.read(), @intCast(b)).next_seq;
        };
        const applied_before = if (inbound) |p| if (channels_mod.inOf(c.read(), p.from)) |x| x.applied else 0 else 0;
        const m = machine_mod.ownerOf(&channel_machines, etype) orelse return error.Halt;
        var r = persistence.Result.start(e);
        try machine_mod.applyEntry(m, &c.store, &c.scratch, e, &r);
        if (etype == .channel_probe) for (0..count) |b_| {
            const b: GroupId = @intCast(b_);
            const now_next = channels_mod.metaOf(c.read(), b).next_seq;
            var seq = sent_before[b];
            while (seq < now_next) : (seq += 1) {
                var k: [channels_mod.OUT_KEY]u8 = undefined;
                const out = (try c.read().get(channels_mod.OutFam, channels_mod.outKey(&k, b, seq))) orelse return error.Halt;
                try o.sent(c.g, b, seq, std.hash.Wyhash.hash(0, out.v.payload), e.header.index, probeTarget(out.v.payload));
            }
        };
        // What reached its target: a message applied on arrival, or one an
        // unpark applied from parking.
        if (etype == .channel_inbound or etype == .channel_unpark) for (c.store.messages.items) |msg| {
            if (msg.entry.header.entry_type != @backingInt(entry_mod.EntryType.channel_probe)) continue;
            if (probeTarget(msg.entry.payload) == null) continue;
            try o.delivered(@intCast(msg.from), c.g, msg.seq, e.header.index);
        };
        if (inbound) |p| {
            const after = if (channels_mod.inOf(c.read(), p.from)) |x| x.applied else 0;
            var it = p.messages.iterator();
            var seq = p.first_seq;
            while (try it.next()) |msg| : (seq += 1) {
                if (seq > applied_before and seq <= after) try o.applied(@intCast(p.from), c.g, seq, std.hash.Wyhash.hash(0, msg.payload), e.header.index);
            }
        }
    }

    const Io = struct {
        c: *GroupChannel,

        pub fn sendFrame(io: Io, to: u16, bytes: []const u8) bool {
            const gs = io.c.groups;
            return gs.mailbox.trySend(gs.now(), io.c.g, @intCast(to), bytes);
        }

        pub fn propose(io: Io, etype: entry_mod.EntryType, payload: []const u8) bool {
            return io.c.groups.sims[io.c.g].proposeOn(NODE, etype, payload);
        }
    };

    /// Where this group's duties run, its driver started over on a new
    /// term.
    fn leads(c: *GroupChannel) bool {
        const sim = &c.groups.sims[c.g];
        if (!sim.leadsReady(NODE) and !sim.leadsHalted(NODE)) return false;
        const term = sim.termOf(NODE);
        if (c.driver.term != term) c.driver.reset(term);
        return true;
    }

    fn tick(c: *GroupChannel) void {
        if (!c.leads()) return;
        const sim = &c.groups.sims[c.g];
        c.driver.tick(c.groups.now(), c.read(), sim.durableIndex(NODE), Io{ .c = c });
        // Its parked targets with room, one unpark at a time.
        if (sim.isHalted(NODE)) return;
        if (c.unpark_term == sim.termOf(NODE) and c.unpark_index > sim.appliedIndex(NODE)) return;
        if (!c.driver.buffers()) return;
        const next = Probed.nextUnpark(c.read(), c.driver.buf, c.driver.msgs, c.unpark_after) orelse return;
        if (!sim.proposeOn(NODE, .channel_unpark, next.entry)) return;
        c.unpark_after = next.target;
        c.unpark_term = sim.termOf(NODE);
        c.unpark_index = sim.lastIndex(NODE);
        c.unparks += 1;
    }

    /// What a target may park before more to it is refused.
    pub const PARKED_TARGET_CAP = 8 << 10;

    /// Slots most probes go to, so a target is written again while it is
    /// full, and parks.
    const POOL = [_]u16{ 7, 101, 202, 303, 404, 505, 606, 707, 808, 909, 1111, 1212, 1313, 1414, 1515, 1616 };

    /// A fill or drain mark to a pool slot: `kind` is "fill" or "drain".
    fn proposeMark(c: *GroupChannel, kind: []const u8, slot: u16) void {
        var body: [16]u8 = undefined;
        _ = std.fmt.bufPrint(&body, "{s}{d:0>2}{d:0>10}", .{ kind[0..4], c.g, c.probes_proposed }) catch return;
        var buf: [64]u8 = undefined;
        const payload = ce.Probe.encodeInto(&buf, .{ .slot = slot, .body = &body }) catch return;
        if (c.groups.sims[c.g].proposeOn(NODE, .channel_probe, payload)) c.probes_proposed += 1;
    }

    fn take(c: *GroupChannel, f: Frame) void {
        if (!c.leads()) return;
        const sim = &c.groups.sims[c.g];
        c.driver.onFrame(f.from, f.bytes, c.groups.now(), c.read(), sim.durableIndex(NODE), sim.isHalted(NODE), Io{ .c = c });
    }

    /// The test machine's send to a random slot: a 16-byte name unique to
    /// the group, and up to 512 bytes in all, so the outbox and parked
    /// caps `Groups` sets are reached.
    fn proposeProbe(c: *GroupChannel, r: std.Random) void {
        if (!c.leads()) return;
        var body: [512]u8 = undefined;
        _ = std.fmt.bufPrint(body[0..16], "g{d:0>2}-{d:0>12}", .{ c.g, c.probes_proposed }) catch return;
        const extra = r.uintAtMost(usize, body.len - 16);
        r.bytes(body[16..][0..extra]);
        // Now and then a pool slot fills, and more often drains.
        const roll = r.uintLessThan(u8, 100);
        if (roll < 3) return c.proposeMark("fill", POOL[r.uintLessThan(usize, POOL.len)]);
        if (roll < 8) return c.proposeMark("drai", POOL[r.uintLessThan(usize, POOL.len)]);
        const slot = if (r.uintLessThan(u8, 4) == 0) r.uintLessThan(u16, 4096) else POOL[r.uintLessThan(usize, POOL.len)];
        var buf: [body.len + 64]u8 = undefined;
        const payload = ce.Probe.encodeInto(&buf, .{ .slot = slot, .body = body[0 .. 16 + extra] }) catch return;
        if (c.groups.sims[c.g].proposeOn(NODE, .channel_probe, payload)) c.probes_proposed += 1;
    }
};

pub const Groups = struct {
    allocator: Allocator,
    sims: []Simulator,
    chans: []GroupChannel,
    /// The probe workload's own stream, so no group's draws move.
    probe_prng: PRNG,
    /// One probe per group per this many ticks, on average; 0 for none.
    probe_every: u32 = 8,
    /// Per 10,000 ticks, the chance a group's disk stops flushing (0: none).
    /// A stall holds the node's crashes until it has flushed again, so a run
    /// with stalls crashes less: they get a swarm of their own.
    stall_permille: u16 = 0,
    flush_stalls: u64 = 0,
    mailbox: Mailbox,
    oracle: ChannelOracle,
    delivery: std.ArrayListUnmanaged(Frame) = .empty,
    /// What a delivered frame does, in place of its group's driver taking
    /// it: for tests of the mailbox alone.
    on_frame: ?*const fn (groups: *Groups, f: Frame) anyerror!void = null,
    converging: bool = false,

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
        const chans = try allocator.alloc(GroupChannel, group_count);
        errdefer allocator.free(chans);
        var chans_made: usize = 0;
        errdefer for (chans[0..chans_made]) |*c| c.deinit();
        for (chans, sims, 0..) |*c, *sim, g| {
            c.* = try GroupChannel.init(allocator, @intCast(g), group_count);
            chans_made += 1;
            sim.extension = c.extension();
            // Each group flushes at its own point in the interval, as each
            // shard's flush timer does: a crash takes a different suffix
            // from each. Group 0 keeps its phase, so it replays as alone.
            sim.flush_phase_ms = (@as(u64, g) * scenario.flush_interval_ms) / group_count;
        }
        return .{
            .allocator = allocator,
            .sims = sims,
            .chans = chans,
            .probe_prng = PRNG.init(scenario.seed ^ 0x9B0B_E5E5),
            .mailbox = Mailbox.init(allocator, scenario.seed, Faults.of(&scenario)),
            .oracle = ChannelOracle.init(allocator),
        };
    }

    pub fn deinit(self: *Groups) void {
        for (self.sims) |*s| s.deinit();
        self.allocator.free(self.sims);
        for (self.chans) |*c| c.deinit();
        self.allocator.free(self.chans);
        for (self.delivery.items) |f| self.mailbox.release(f);
        self.delivery.deinit(self.allocator);
        self.mailbox.deinit();
        self.oracle.deinit();
    }

    fn now(self: *const Groups) u64 {
        return self.sims[0].now;
    }

    /// The channels drained: nothing in flight, every message its sender
    /// holds applied, but what waits on a halted group.
    fn drained(self: *const Groups) bool {
        var buf: [MAX_GROUPS]bool = undefined;
        const halted = self.haltedGroups(&buf);
        for (self.mailbox.queue.items) |f| if (!halted[f.to]) return false;
        return self.oracle.drained(halted);
    }

    /// Which groups are halted: messages to one wait for its repair.
    fn haltedGroups(self: *const Groups, buf: *[MAX_GROUPS]bool) []const bool {
        for (self.sims, 0..) |*s, g| buf[g] = s.isHalted(NODE);
        return buf[0..self.sims.len];
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
        if (!was_up and up) {
            var kept: [MAX_GROUPS]?u64 = @splat(null);
            for (self.sims, 0..) |*s, g| kept[g] = s.keptIndex(NODE);
            self.oracle.recoveredAll(kept[0..self.sims.len]);
        }
    }

    /// One tick of every group, the node's fate carried to all of them,
    /// then the mailbox's due frames.
    fn step(self: *Groups) !void {
        // `Groups` moves when returned by value: the channels' way back to
        // it is set each step, as each simulator sets its nodes'.
        for (self.chans) |*c| c.groups = self;
        const was_up = self.sims[0].nodeUp(NODE);
        for (self.sims) |*s| try s.advance();
        try self.followFate(was_up);
        const up = self.sims[0].nodeUp(NODE);
        // A group's disk stops flushing for a while, now and then: its
        // durable index stalls, and so does what it delivers.
        if (up and !self.converging and self.stall_permille > 0) for (self.sims) |*s| {
            if (self.now() >= s.flush_stalled_until and self.probe_prng.random().uintLessThan(u16, 10_000) < self.stall_permille) {
                s.flush_stalled_until = self.now() + self.probe_prng.random().intRangeAtMost(u64, 2_000, 8_000);
                self.flush_stalls += 1;
                // The node's crashes wait until the stalled disk has flushed
                // again: an async crash may lose one flush interval, no more.
                const clear = s.flush_stalled_until + 2 * s.scenario.flush_interval_ms;
                self.sims[0].no_crash_before = @max(self.sims[0].no_crash_before, clear);
            }
        };
        // Probes only in the safety phase, so convergence can drain.
        if (up and self.probe_every > 0 and !self.converging) for (self.chans) |*c| {
            if (self.probe_prng.random().uintLessThan(u32, self.probe_every) == 0) c.proposeProbe(self.probe_prng.random());
        };
        // In convergence a pool slot still full at its owner is drained, so
        // no message stays parked at the end; one drained once is never
        // filled again, so this stops by itself.
        if (up and self.probe_every > 0 and self.converging and self.now() % 1_000 == 0) for (GroupChannel.POOL) |slot| {
            const owner = &self.chans[slot % self.chans.len];
            if (!probe.slotFull(owner.read(), slot)) continue;
            if (owner.leads()) owner.proposeMark("drai", slot);
        };
        self.delivery.clearRetainingCapacity();
        try self.mailbox.deliverDue(self.now(), &self.delivery);
        while (self.delivery.items.len > 0) {
            const f = self.delivery.orderedRemove(0);
            defer self.mailbox.release(f);
            if (!up) continue;
            if (self.on_frame) |handle| try handle(self, f) else self.chans[f.to].take(f);
        }
        if (up) for (self.chans) |*c| c.tick();
    }

    /// Each outbox's recorded bytes are the payload bytes of the records
    /// it holds: a count every replica gets wrong the same way agrees with
    /// itself, so it is checked against what it counts.
    fn checkOutboxBytes(self: *Groups) void {
        for (self.chans) |*c| for (0..self.chans.len) |to_| {
            const to: u16 = @intCast(to_);
            const read = c.read();
            const meta = channels_mod.metaOf(read, to);
            var pb: [channels_mod.GROUP_KEY]u8 = undefined;
            var it = read.range(channels_mod.OutFam, channels_mod.outPrefix(&pb, to));
            var held: u64 = 0;
            while (it.next() catch null) |rec| held += rec.value.v.payload.len;
            if (held != meta.bytes) self.oracle.fail(c.g, @intCast(to), meta.acked + 1, "an outbox's recorded bytes are not what it holds");
        };
    }

    pub fn run(self: *Groups) !Summary {
        for (self.chans) |*c| c.groups = self;
        while (!self.sims[0].safetyDone() and !self.failed()) try self.step();
        self.converging = true;
        // Disks flush again for convergence.
        for (self.sims) |*s| s.flush_stalled_until = 0;
        // Each group converges by its own deadline, or fails as it would
        // alone; the run goes on to the latest of them at most.
        var deadlines: [MAX_GROUPS]u64 = @splat(0);
        var converged_by: [MAX_GROUPS]bool = @splat(false);
        if (!self.failed()) {
            const was_up = self.sims[0].nodeUp(NODE);
            var last: u64 = 0;
            for (self.sims, 0..) |*s, g| {
                deadlines[g] = try s.beginConvergence();
                last = @max(last, deadlines[g]);
            }
            try self.followFate(was_up);
            // The network heals for convergence, and so does the mailbox:
            // latency stays, losses stop.
            self.mailbox.faults.drop_percent = 0;
            self.mailbox.faults.duplicate_percent = 0;
            while (self.now() < last and !self.failed()) {
                try self.step();
                var all = true;
                for (self.sims, 0..) |*s, g| {
                    if (!converged_by[g] and self.now() <= deadlines[g] and s.isConverged()) converged_by[g] = true;
                    all = all and converged_by[g];
                }
                if (all and self.drained()) break;
            }
            // Every message a sender holds was applied by now, or the run
            // fails: no channel may stall.
            if (!self.failed()) {
                var buf: [MAX_GROUPS]bool = undefined;
                self.oracle.finish(self.haltedGroups(&buf));
                self.checkOutboxBytes();
            }
        }
        const summaries = try self.allocator.alloc(simulator.Summary, self.sims.len);
        var ok = self.oracle.violations.items.len == 0;
        for (self.sims, summaries, 0..) |*s, *out, g| {
            out.* = s.finish(converged_by[g]);
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
            .probes_proposed = probes: {
                var n: u64 = 0;
                for (self.chans) |*c| n += c.probes_proposed;
                break :probes n;
            },
            .frames_refused = self.mailbox.refused,
            .flush_stalls = self.flush_stalls,
            .held_by_halts = self.oracle.held_by_halts,
            .left_in_mailbox = self.mailbox.queue.items.len,
            .busy = busy: {
                var n: u64 = 0;
                for (self.chans) |*c| n += c.driver.counts.busy;
                break :busy n;
            },
            .pressured = pressured: {
                var n: u64 = 0;
                for (self.chans) |*c| n += c.driver.counts.pressured;
                break :pressured n;
            },
            .unparks = unparks: {
                var n: u64 = 0;
                for (self.chans) |*c| n += c.unparks;
                break :unparks n;
            },
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
    // No probes: idle channels move none of group 0's draws.
    groups.probe_every = 0;
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
                    try o.sent(0, 1, 1, 11, 5, null);
                    try o.sent(0, 1, 2, 12, 6, null);
                    try o.applied(0, 1, 1, 11, 3);
                    // The receiver's crash kept its log through 2: the apply at
                    // 3 is gone, effect and all, and it takes the message again.
                    o.recovered(1, 2);
                    try o.applied(0, 1, 1, 11, 3);
                    try o.applied(0, 1, 2, 12, 4);
                    o.finish(&.{});
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
                    try o.sent(0, 1, 1, 11, 5, null);
                    try o.applied(0, 1, 1, 11, 3);
                    o.recovered(1, 3);
                    try o.applied(0, 1, 1, 11, 4);
                }
            }.f,
        },
        .{ .detail = "applied a message past a gap", .run = struct {
            fn f(o: *ChannelOracle) !void {
                try o.sent(0, 1, 1, 11, 1, null);
                try o.sent(0, 1, 2, 12, 2, null);
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
                try o.sent(0, 1, 1, 11, 1, null);
                try o.applied(0, 1, 1, 99, 1);
            }
        }.f },
        .{
            .detail = "applied a message its sender lost",
            .run = struct {
                fn f(o: *ChannelOracle) !void {
                    // Delivered before its send was durable, then the sender's
                    // crash took the send.
                    try o.sent(0, 1, 1, 11, 7, null);
                    try o.applied(0, 1, 1, 11, 2);
                    o.recovered(0, 6);
                }
            }.f,
        },
        .{ .detail = "a message its sender holds was never applied", .run = struct {
            fn f(o: *ChannelOracle) !void {
                try o.sent(1, 0, 1, 11, 1, null);
                o.finish(&.{});
            }
        }.f },
        .{ .detail = "a sender's seqs skipped or repeated", .run = struct {
            fn f(o: *ChannelOracle) !void {
                try o.sent(0, 1, 2, 12, 1, null);
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
}

test "vopr groups: a crash that takes a send and its apply together is no violation, whichever group is numbered first" {
    for ([_][2]GroupId{ .{ 0, 1 }, .{ 1, 0 } }) |pair| {
        const sender = pair[0];
        const receiver = pair[1];
        var o = ChannelOracle.init(testing.allocator);
        defer o.deinit();
        // The sender sends seq 1 at its index 7, the receiver applies it at
        // its 3; the crash keeps the sender through 6, the receiver through 2.
        try o.sent(sender, receiver, 1, 11, 7, null);
        try o.applied(sender, receiver, 1, 11, 3);
        var kept: [MAX_GROUPS]?u64 = @splat(null);
        kept[sender] = 6;
        kept[receiver] = 2;
        o.recoveredAll(&kept);
        try testing.expectEqual(@as(usize, 0), o.violations.items.len);
        // Both start over: the send again, at the sender's next index.
        try o.sent(sender, receiver, 1, 12, 7, null);
        try o.applied(sender, receiver, 1, 12, 3);
        o.finish(&.{});
        try testing.expectEqual(@as(usize, 0), o.violations.items.len);
    }
}

test "vopr groups: a clean restart's replay reports what was recorded, and nothing else passes for one" {
    var o = ChannelOracle.init(testing.allocator);
    defer o.deinit();
    try o.sent(0, 1, 1, 11, 4, null);
    try o.sent(0, 1, 2, 12, 5, null);
    try o.applied(0, 1, 1, 11, 2);
    try o.applied(0, 1, 2, 12, 3);
    // A crash that kept everything, then replay from index 1: every report
    // again, at the same index.
    var kept: [MAX_GROUPS]?u64 = @splat(null);
    kept[0] = 5;
    kept[1] = 3;
    o.recoveredAll(&kept);
    try o.applied(0, 1, 1, 11, 2);
    try o.applied(0, 1, 2, 12, 3);
    try o.sent(0, 1, 1, 11, 4, null);
    try o.sent(0, 1, 2, 12, 5, null);
    try testing.expectEqual(@as(u64, 4), o.replays);
    try testing.expectEqual(@as(usize, 0), o.violations.items.len);
    // Traffic after the restart goes on as before.
    try o.sent(0, 1, 3, 13, 6, null);
    try o.applied(0, 1, 3, 13, 4);
    o.finish(&.{});
    try testing.expectEqual(@as(usize, 0), o.violations.items.len);
    // A replay that isn't what was recorded is caught: another send at a
    // recorded seq, or a recorded message applied at another index.
    try o.sent(0, 1, 2, 99, 5, null);
    try o.applied(0, 1, 1, 11, 9);
    try testing.expectEqual(@as(usize, 2), o.violations.items.len);
    try testing.expectEqualStrings("a sender's seqs skipped or repeated", o.violations.items[0].detail);
    try testing.expectEqualStrings("applied a message twice", o.violations.items[1].detail);
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
    // The mailbox alone: no channel traffic, and the frames go to the test.
    groups.probe_every = 0;
    groups.on_frame = Counter.handle;
    for (0..8) |_| try groups.mailbox.send(0, 0, 1, "ping");
    const s = try groups.run();
    defer testing.allocator.free(s.groups);
    try testing.expect(s.ok);
    try testing.expect(s.crashes > 0);
    try testing.expect(Counter.handled > 100);
    try testing.expect(s.frames_lost_in_crash > 0);
}

test "vopr groups: channel traffic between groups is applied once and in order through crashes, and drains" {
    for ([_]scenario_mod.DurabilityMode{ .sync, .async_flush }) |durability| {
        var scenario = oneNodeScenario(21);
        scenario.durability = durability;
        var groups = try Groups.init(testing.allocator, scenario, 3, .{});
        defer groups.deinit();
        const s = try groups.run();
        defer testing.allocator.free(s.groups);
        if (!s.ok) for (groups.oracle.violations.items) |v| std.debug.print("channel {d}->{d} seq {d}: {s}\n", .{ v.from, v.to, v.seq, v.detail });
        try testing.expect(s.ok);
        try testing.expect(s.crashes > 0);
        try testing.expect(s.probes_proposed > 50);
        try testing.expect(s.channel_applies > 50);
        try testing.expect(s.frames_lost_in_crash > 0);
        try testing.expectEqual(@as(usize, 0), s.channel_violations);
        // Drained: nothing in flight, and every message its sender holds
        // was applied.
        try testing.expectEqual(@as(usize, 0), s.left_in_mailbox);
        try testing.expect(groups.oracle.drained(&.{}));
    }
}
