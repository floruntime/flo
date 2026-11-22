//! The leader's side of every channel: delivering outboxes, answering what
//! arrives, and trimming what was durably applied. It holds only memory, so
//! a new leader starts it over from state (`reset`), and it reaches the
//! world through an `io` with `sendFrame(to, bytes) bool` and
//! `propose(etype, payload) bool`, so the shard and VOPR run the same code.
//!
//! - **Delivery.** A batch carries the outbox's messages after what was
//!   last sent, up to the window, and only those whose sending entry is
//!   durable, so no receiver applies a message its sender could lose. A
//!   send to this group's own slots needn't wait: its inbound entry follows
//!   it in the same log.
//! - **Answers.** The receiver answers every batch, and an empty probe,
//!   with two acks from its own state: `applied` (its `in_hwm`) and
//!   `durable` (its `in_hwm` as of its durable index), never from the
//!   batch. A batch past what it accepted is answered `gap`; one it can take
//!   is proposed as `channel_inbound`.
//! - **Acks.** Only a durable ack trims. An ack above what was ever sent is
//!   refused (`h <= next_seq - 1`). A gap rewinds once per generation: the
//!   answers to older batches can't rewind it again.
//! - **Who sent a frame** is the inbox's sender, passed in by the caller,
//!   never a field of the frame: one group can't answer for another.

const std = @import("std");
const builtin = @import("builtin");
const schema = @import("../protocol/schema.zig");
const ce = @import("../protocol/channel_entries.zig");
const channels = @import("channels.zig");
const store_mod = @import("store.zig");
const view_mod = @import("view.zig");
const entry_mod = @import("../storage/ual/entry.zig");

const ReadView = view_mod.ReadView;
const Int = schema.Int;
const Record = schema.Record;

/// Most messages in flight past the receiver's applied ack.
pub const WINDOW = 1024;

/// How much each class of a shard's channel outboxes may hold before the
/// edge stops adding to it: per channel and in all. The two classes
/// (`channels.Class`) are held apart, so sink bytes a full target holds
/// back never hold a workflow's or an action's sends. The rest keep what
/// the whole outbox had; a sink's get half that again on top. Sized per
/// node, from its memory, and read only by its leaders at the edge, so two
/// nodes may size them differently; never in state.
pub const OutboxCaps = struct {
    pub const Caps = struct { shard: u64, channel: u64 };

    other: Caps = .{ .shard = 1 << 30, .channel = 128 << 20 },
    sink: Caps = .{ .shard = 512 << 20, .channel = 64 << 20 },

    /// The rest: an eighth of the node's memory over its shards, a quarter
    /// of that a channel. A sink's: half of each. Within their ceilings,
    /// so all of it takes at most three sixteenths of memory, and outbox
    /// rows count toward the memory pool besides.
    pub fn of(memory: u64, shards: u16) OutboxCaps {
        const per = memory / 8 / @max(shards, 1);
        const other = @min(@as(u64, 1 << 30), per);
        const sink = @min(@as(u64, 512 << 20), per / 2);
        return .{
            .other = .{ .shard = other, .channel = @min(@as(u64, 128 << 20), other / 4) },
            .sink = .{ .shard = sink, .channel = @min(@as(u64, 64 << 20), sink / 4) },
        };
    }

    pub fn forClass(self: OutboxCaps, class: channels.Class) Caps {
        return switch (class) {
            .other => self.other,
            .sink => self.sink,
        };
    }
};
/// Most payload bytes in one batch.
pub const MAX_BATCH_BYTES = 1 << 20;
pub const RESEND_MIN_MS = 200;
pub const RESEND_MAX_MS = 5_000;
pub const TRIM_EVERY_MS = 100;
/// A trim not applied by then is taken as lost, and proposed again.
pub const TRIM_LOST_MS = 1_000;

// ─── Frames ────────────────────────────────────────────────────────────────

pub const Kind = enum(u8) { batch = 1, answer = 2 };
pub const Status = enum(u8) { ok = 0, busy = 1, halted = 2 };

/// Why writes to a shard are queued. A receiver reports only its own
/// state: `halted`, `cannot_flush`, `target_full`. `no_leader` is what its
/// sender observes. A receiver never passes on a peer's backlog: its own
/// pipelines already wait on its own outboxes, and a sender's writes to
/// it add to none of them unless one of its pipelines forwards them,
/// which that pipeline's wait bounds.
pub const Cause = enum(u8) {
    /// The named shard is halted.
    halted = 1,
    /// The named shard hasn't answered for `NO_LEADER_MS`: no leader,
    /// one that can't be reached, or a new one not heard from yet.
    no_leader = 2,
    /// The named shard can't flush its log to disk.
    cannot_flush = 3,
    /// A target on the named shard is past what it parks.
    target_full = 4,
    /// The named shard is over its memory share: a sink's writes wait
    /// until its use falls.
    over_share = 5,
};

/// One shard writes are queued behind, and why.
pub const Stall = Record(&.{
    .{ .name = "shard", .T = Int(u16) },
    .{ .name = "cause", .T = Int(u8) },
});

/// Most stalls an answer carries.
pub const MAX_PRESSURE = 4;
pub const Pressure = schema.BoundedList(Stall, MAX_PRESSURE, MAX_PRESSURE * 3);

/// A pressure entry is kept this long after the last answer carrying it,
/// and its sender probes this often while it is, so an idle channel can't
/// keep a stale one.
pub const PRESSURE_EXPIRY_MS = 1_000;
pub const PRESSURE_PROBE_MS = 500;
/// A peer silent this long, with writes waiting for it, is taken to have
/// no leader: twice the longest resend backoff, so a sender backed off in
/// full still hears from a live peer first.
pub const NO_LEADER_MS = 2 * RESEND_MAX_MS;

pub fn causeWords(cause: Cause) []const u8 {
    return switch (cause) {
        .halted => "is halted",
        .no_leader => std.fmt.comptimePrint("hasn't answered for {d} s", .{NO_LEADER_MS / std.time.ms_per_s}),
        .cannot_flush => "cannot flush to disk",
        .target_full => "has a full target",
        .over_share => "is over its memory share",
    };
}

/// `n` bytes in words: "512 B", "37 KiB", "64 MiB", floored to the unit,
/// which is the largest that keeps it at least 1.
pub fn bytesText(buf: []u8, n: u64) []const u8 {
    return (if (n < 1 << 10)
        std.fmt.bufPrint(buf, "{d} B", .{n})
    else if (n < 1 << 20)
        std.fmt.bufPrint(buf, "{d} KiB", .{n >> 10})
    else
        std.fmt.bufPrint(buf, "{d} MiB", .{n >> 20})) catch "";
}

pub const Batch = Record(&.{
    // The lane it is on (`channels.Lane`): its seqs count in that lane's.
    .{ .name = "lane", .T = Int(u8) },
    .{ .name = "gen_term", .T = Int(u64) },
    .{ .name = "gen_counter", .T = Int(u32) },
    .{ .name = "first_seq", .T = Int(u64) },
    .{ .name = "messages", .T = ce.Messages },
});

pub const Answer = Record(&.{
    .{ .name = "lane", .T = Int(u8) },
    .{ .name = "gen_term", .T = Int(u64) },
    .{ .name = "gen_counter", .T = Int(u32) },
    .{ .name = "applied", .T = Int(u64) },
    .{ .name = "durable", .T = Int(u64) },
    // The seq the receiver expects next, after a batch that skipped past
    // it; 0 for none.
    .{ .name = "gap", .T = Int(u64) },
    .{ .name = "status", .T = Int(u8) },
    // What writes through the receiver are queued behind, if anything.
    .{ .name = "pressure", .T = Pressure },
});

pub const Frame = union(Kind) {
    batch: Batch.Value,
    answer: Answer.Value,

    pub fn decode(bytes: []const u8) ?Frame {
        if (bytes.len == 0) return null;
        var diag: schema.Diagnostic = .{};
        const kind = std.enums.fromInt(Kind, bytes[0]) orelse return null;
        return switch (kind) {
            .batch => .{ .batch = Batch.decodeAll(bytes[1..], &diag) catch return null },
            .answer => .{ .answer = Answer.decodeAll(bytes[1..], &diag) catch return null },
        };
    }
};

fn encodeFrame(buf: []u8, comptime kind: Kind, v: anytype) ?[]const u8 {
    buf[0] = @backingInt(kind);
    const R = switch (kind) {
        .batch => Batch,
        .answer => Answer,
    };
    const body = R.encodeInto(buf[1..], v) catch return null;
    return buf[0 .. 1 + body.len];
}

// ─── The driver ────────────────────────────────────────────────────────────

pub const SinkRefusals = std.EnumArray(@import("../protocol/reason.zig").Reason, u64);

/// A sink's message in flight; `target` is null for one that never parks.
const InFlightMessage = struct { seq: u64, target: ?channels.Target, bytes: u64 };

const Lane = channels.Lane;
const LANES = channels.LANES;

/// One lane of a channel with one peer, each way: its own seqs, acks,
/// trims and resends.
const LaneState = struct {
    // Sending to it.
    gen_counter: u32 = 0,
    /// The highest seq sent in this generation; never below `acked`.
    sent_through: u64 = 0,
    applied_ack: u64 = 0,
    durable_ack: u64 = 0,
    last_sent_ms: u64 = 0,
    progress_ms: u64 = 0,
    backoff_ms: u64 = RESEND_MIN_MS,
    trim_through: u64 = 0,
    trim_at_ms: ?u64 = null,
    last_trim_ms: u64 = 0,
    // Receiving from it.
    /// The highest seq proposed or applied from it; null until rebuilt.
    accepted: ?u64 = null,
    recv_gen: struct { term: u64 = 0, counter: u32 = 0 } = .{},
    told_applied: u64 = 0,
    told_durable: u64 = 0,
    /// The pressure its last answer on this lane carried, and when: kept
    /// until `PRESSURE_EXPIRY_MS` after that, probed for every
    /// `PRESSURE_PROBE_MS` meanwhile. Per lane, so a full target, which
    /// only the sink lane's answers name, holds no other lane's sends.
    pressure: [MAX_PRESSURE]Stall.Value = undefined,
    pressure_len: u8 = 0,
    pressure_ms: u64 = 0,
    probe_ms: u64 = 0,
};

const Peer = struct {
    lanes: [LANES]LaneState = @splat(.{}),
    /// When it last answered this leader.
    answered_ms: u64 = 0,
    /// Its sink messages proposed and not yet applied, in seq order: what
    /// `Driver.sink_bytes` counts, and, of those that may park,
    /// `Driver.target_bytes`. Only the sink lane's: no other message is a
    /// sink's.
    in_flight: std.ArrayListUnmanaged(InFlightMessage) = .empty,
    /// This shard's parked bytes when a target over its cap last refused
    /// its batch, or null: while they are no fewer, every answer to it,
    /// probes' too, says a target is full. Read from the replicated parked
    /// totals, so a refusal is named until an unpark frees room, never by
    /// a clock.
    refused_parked: ?u64 = null,
    /// This shard's use when its share last held its batch, or null: while
    /// use is no lower, every answer to it says the shard is over share.
    refused_use: ?u64 = null,
    /// Its sink messages' bytes in `in_flight`: what its own cap holds
    /// (`channels.sinkInFlightCap`).
    sink_bytes: u64 = 0,

    fn lane(p: *Peer, l: Lane) *LaneState {
        return &p.lanes[@backingInt(l)];
    }
};

pub const Counts = struct {
    batches: u64 = 0,
    messages_sent: u64 = 0,
    resends: u64 = 0,
    gaps: u64 = 0,
    inbound_proposed: u64 = 0,
    busy: u64 = 0,
    /// Batches whose sink messages its shard's share held back.
    over_share: u64 = 0,
    /// Answers that said what writes through this group are queued behind.
    pressured: u64 = 0,
    trims_proposed: u64 = 0,
    acks_refused: u64 = 0,
    /// Batches dropped for a message that doesn't fit its lane: a sending
    /// build's bug.
    batches_dropped: u64 = 0,
    /// Ticks that read every peer's records; the rest found nothing changed.
    scans: u64 = 0,
};

/// What the channel families have been through, from their commit counts:
/// unchanged, no record changed.
const Seen = struct { out: u64 = 0, meta: u64 = 0, in: u64 = 0 };

fn seenOf(read: ReadView) Seen {
    const f = &read.store.families;
    return .{ .out = f[channels.OutFam.ID].commits, .meta = f[channels.MetaFam.ID].commits, .in = f[channels.InFam.ID].commits };
}

/// A bug planted on purpose, for VOPR to show it catches it. Only VOPR
/// sets one; a server never does.
pub const Mutant = enum {
    none,
    /// Deliver a message before its sending entry is durable.
    past_durable,
    /// Trim on the applied ack, not the durable one.
    trim_on_applied,
    /// Answer an all-duplicate batch with its last seq, not the receiver's marks.
    ack_last_seq,
    /// Let a gap from an older generation's batch rewind again.
    no_gen_filter,
    /// Take an ack above what this group's state says was sent.
    unbounded_ack,
    /// Apply a message whose target has room even while earlier ones to it
    /// are parked (`channels.zig`), not park it behind them.
    no_park_behind,
    /// Never release what was proposed once it applies: its target's
    /// in-flight bytes only grow, until it refuses everything.
    leak_in_flight,
    /// Never release a sink's message with no target from the sink bytes in
    /// flight.
    sink_untargeted_leaked,
    /// Keep the sink bytes in flight across a new term.
    sink_in_flight_kept_on_reset,
    /// Hold a sink's message to its share only when it has a target
    /// (`channels.mutant_sink_gate_after_no_target`).
    sink_gate_after_no_target,
};

pub const Driver = struct {
    group: u16,
    mutant: Mutant = .none,
    count: u16,
    term: u64 = 0,
    /// One per group on the node.
    peers: []Peer,
    /// Whether this group takes a message in now, given what it has
    /// taken and not yet applied (`Channels.admits`); null takes every one.
    admits: ?*const fn (read: ReadView, m: ce.Message.Value, in_flight: channels.InFlight) channels.Admit = null,
    /// The target a sink's message may park under (`Channels.sinkTarget`),
    /// so what this leader proposed to it and hasn't applied counts
    /// against its cap; null when nothing parks.
    sink_target: ?*const fn (m: ce.Message.Value) ?channels.Target = null,
    /// A sink's message bytes this leader proposed and its log hasn't
    /// applied, by target and in all. Leader-local and kept per sender
    /// (`Peer.in_flight`), so a term's end, or an apply, releases them;
    /// never read by apply.
    target_bytes: std.AutoHashMapUnmanaged(channels.Target, u64) = .empty,
    in_flight_bytes: u64 = 0,
    /// Every sink's message bytes this leader proposed and its log hasn't
    /// applied, parking or not: what a sink is admitted against beside the
    /// shard's use (`channels.sinkRoom`).
    sink_bytes: u64 = 0,
    counts: Counts = .{},
    /// Rewrites a message before this leader logs it, or null to log it as
    /// it came: a sink's enqueue takes this node's queue caps, as its own
    /// clients' do. Writes into `buf`; `stamp_ctx` is passed to it.
    stamp: ?*const fn (ctx: ?*const anyopaque, m: ce.Message.Value, buf: []u8) ?[]const u8 = null,
    stamp_ctx: ?*const anyopaque = null,
    stamp_buf: []u8 = &.{},
    /// Why writes through this group are queued by its own state, if they
    /// are: halted, or flushes failing. Set by its shard each tick.
    own_cause: ?Cause = null,
    /// The clock as of the last tick or frame.
    now_ms: u64 = 0,
    /// When this leader's term began here, as its first tick saw it: what
    /// a peer's silence is counted from before it has answered.
    started_ms: u64 = 0,
    /// Messages each group sent this one that were refused as its own bug,
    /// by reason (`store.SenderBug`). Counted where they apply, on every
    /// replica, and kept across terms, unlike `peers`.
    sender_bugs: [][store_mod.SENDER_BUGS]u64,
    /// A sink's messages each group sent this one that their target refused
    /// for good, by reason: the sink's records lost to that target. Counted
    /// where they apply, on every replica, and kept across terms.
    sink_refusals: []SinkRefusals,
    /// The last scan left nothing outstanding: no message unsent or
    /// unacked, no trim due or in flight, no answer untold. Until a channel
    /// record or a frame changes that, a tick has nothing to do. The commit
    /// counts start over when a store is cleared and loaded again, so a
    /// store reloaded in place under a live driver needs a `reset`; today
    /// that happens only at boot, before any driver ticks.
    quiet: bool = false,
    seen: Seen = .{},
    allocator: std.mem.Allocator,
    /// A frame or an inbound entry is built here, one at a time, over a
    /// message list built in `msgs`. Allocated at the first batch either
    /// way, so a group that never sends or takes a message pays nothing.
    buf: []u8 = &.{},
    msgs: []u8 = &.{},

    pub fn init(allocator: std.mem.Allocator, group: u16, count: u16) error{OutOfMemory}!Driver {
        std.debug.assert(group < count);
        const peers = try allocator.alloc(Peer, count);
        errdefer allocator.free(peers);
        @memset(peers, .{});
        const bugs = try allocator.alloc([store_mod.SENDER_BUGS]u64, count);
        errdefer allocator.free(bugs);
        @memset(bugs, @splat(0));
        const refusals = try allocator.alloc(SinkRefusals, count);
        @memset(refusals, .initFill(0));
        return .{ .allocator = allocator, .group = group, .count = count, .peers = peers, .sender_bugs = bugs, .sink_refusals = refusals };
    }

    pub fn deinit(self: *Driver) void {
        for (self.peers) |*p| p.in_flight.deinit(self.allocator);
        self.target_bytes.deinit(self.allocator);
        self.allocator.free(self.peers);
        self.allocator.free(self.sender_bugs);
        self.allocator.free(self.sink_refusals);
        self.allocator.free(self.buf);
        self.allocator.free(self.msgs);
        self.allocator.free(self.stamp_buf);
    }

    /// What writes to group `to` on `lane` are queued behind, as its last
    /// answers on that lane said, or null: what holds a sender to it.
    pub fn pressureOf(self: *const Driver, to: u16, lane: Lane, now_ms: u64) ?Stall.Value {
        if (to >= self.peers.len) return null;
        const p = &self.peers[to].lanes[@backingInt(lane)];
        if (!pressureLive(p, now_ms)) return null;
        return p.pressure[0];
    }

    fn pressureLive(p: *const LaneState, now_ms: u64) bool {
        return p.pressure_len > 0 and now_ms -| p.pressure_ms < PRESSURE_EXPIRY_MS;
    }

    /// Whether any peer's pressure is live, on any lane: the driver keeps
    /// ticking while it is, so it probes and lets it expire.
    pub fn pressured(self: *const Driver, now_ms: u64) bool {
        for (self.peers) |*p| for (&p.lanes) |*l| if (pressureLive(l, now_ms)) return true;
        return false;
    }

    /// Why writes to `to` on `lane` are held, as this leader sees it:
    /// silent for `NO_LEADER_MS`, else what that lane's live pressure
    /// names, else null. A halted or full peer that goes quiet reads as
    /// quiet once its pressure expires.
    pub fn causeOf(self: *const Driver, to: u16, lane: Lane, now_ms: u64) ?Cause {
        if (to >= self.peers.len) return null;
        const p = &self.peers[to];
        // Silence counts from this term's first tick here, or from the
        // last answer: not before either, so a leader not yet ticking
        // blames no one.
        const since = @max(p.answered_ms, self.started_ms);
        if (since != 0 and now_ms -| since >= NO_LEADER_MS) return .no_leader;
        const l = &p.lanes[@backingInt(lane)];
        if (!pressureLive(l, now_ms)) return null;
        return std.enums.fromInt(Cause, l.pressure[0].cause);
    }

    /// The batch buffers, allocated once; false when there's no memory for
    /// them, and the batch waits.
    pub fn buffers(self: *Driver) bool {
        if (self.buf.len == 0) {
            self.buf = self.allocator.alloc(u8, 1 + ce.Inbound.max_size) catch return false;
            self.msgs = self.allocator.alloc(u8, ce.Messages.max_size) catch {
                self.allocator.free(self.buf);
                self.buf = &.{};
                return false;
            };
        }
        if (self.stamp != null and self.stamp_buf.len == 0) {
            self.stamp_buf = self.allocator.alloc(u8, ce.MAX_MESSAGE_PAYLOAD) catch return false;
        }
        return true;
    }

    /// A new term, or a new leader: everything in memory starts over, from
    /// state. Duties run only after the term's no-op applies, so every
    /// message an earlier leader sent is in state by then.
    pub fn reset(self: *Driver, term: u64) void {
        self.term = term;
        for (self.peers) |*p| {
            var list = p.in_flight;
            list.clearRetainingCapacity();
            p.* = .{ .in_flight = list };
        }
        self.target_bytes.clearRetainingCapacity();
        self.in_flight_bytes = 0;
        if (self.mutant != .sink_in_flight_kept_on_reset) self.sink_bytes = 0;
        self.started_ms = 0;
        self.quiet = false;
    }

    /// What a sink's messages to `target` this leader proposed and hasn't
    /// applied take, and all of them, and of those `from` sent.
    pub fn inFlight(self: *const Driver, from: u16, target: ?channels.Target) channels.InFlight {
        const t = if (target) |x| self.target_bytes.get(x) orelse 0 else 0;
        return .{ .target = t, .total = self.in_flight_bytes, .sink = self.sink_bytes, .sink_own = self.peers[from].sink_bytes, .senders = self.count };
    }

    /// `from`'s messages through `applied` have applied: no longer in flight.
    fn releaseApplied(self: *Driver, from: u16, applied: u64) void {
        if (self.mutant == .leak_in_flight) return;
        const p = &self.peers[from];
        var gone: usize = 0;
        for (p.in_flight.items) |m| {
            if (m.seq > applied) break;
            self.forget(from, m);
            gone += 1;
        }
        if (gone == 0) return;
        const rest = p.in_flight.items.len - gone;
        std.mem.copyForwards(InFlightMessage, p.in_flight.items[0..rest], p.in_flight.items[gone..]);
        p.in_flight.shrinkRetainingCapacity(rest);
        self.assertSinkBytes();
    }

    fn forget(self: *Driver, from: u16, m: InFlightMessage) void {
        if (!(self.mutant == .sink_untargeted_leaked and m.target == null)) {
            self.sink_bytes -= m.bytes;
            self.peers[from].sink_bytes -= m.bytes;
        }
        const t = m.target orelse return;
        self.in_flight_bytes -= m.bytes;
        const e = self.target_bytes.getPtr(t).?;
        e.* -= m.bytes;
        if (e.* == 0) _ = self.target_bytes.remove(t);
    }

    /// `sink_bytes` is what every peer's list holds, and each peer's what
    /// its own does: none counted twice or missed. A walk of every message
    /// in flight, so debug builds only.
    fn assertSinkBytes(self: *const Driver) void {
        if (builtin.mode != .debug or self.mutant != .none) return;
        var sum: u64 = 0;
        for (self.peers) |*p| {
            var own: u64 = 0;
            for (p.in_flight.items) |m| own += m.bytes;
            std.debug.assert(own == p.sink_bytes);
            sum += own;
        }
        std.debug.assert(sum == self.sink_bytes);
    }

    /// Counts a sink's message in flight, to `target` if it may park;
    /// false out of memory, when it must not be proposed.
    fn track(self: *Driver, from: u16, seq: u64, target: ?channels.Target, bytes: u64) bool {
        const p = &self.peers[from];
        p.in_flight.append(self.allocator, .{ .seq = seq, .target = target, .bytes = bytes }) catch return false;
        if (target) |t| {
            const e = self.target_bytes.getOrPut(self.allocator, t) catch {
                _ = p.in_flight.pop();
                return false;
            };
            if (!e.found_existing) e.value_ptr.* = 0;
            e.value_ptr.* += bytes;
            self.in_flight_bytes += bytes;
        }
        self.sink_bytes += bytes;
        p.sink_bytes += bytes;
        return true;
    }

    /// Takes back what `track` counted for `from` past `seq`: a batch
    /// whose inbound wasn't proposed.
    fn untrackAfter(self: *Driver, from: u16, lane: Lane, seq: u64) void {
        // Only the sink lane carries sinks, so only its are tracked.
        if (lane != .sink) return;
        const p = &self.peers[from];
        while (p.in_flight.items.len > 0 and p.in_flight.items[p.in_flight.items.len - 1].seq > seq) self.forget(from, p.in_flight.pop().?);
        self.assertSinkBytes();
    }

    /// The periodic work: deliver, resend, probe, trim, and tell senders
    /// what was applied. Once everything is delivered, acked, trimmed and
    /// told, it reads three commit counts and stops there, until a channel
    /// record changes or a frame arrives.
    pub fn tick(self: *Driver, now_ms: u64, read: ReadView, durable: u64, io: anytype) void {
        self.now_ms = now_ms;
        if (self.started_ms == 0) self.started_ms = @max(now_ms, 1);
        const seen = seenOf(read);
        if (self.quiet and std.meta.eql(seen, self.seen)) return;
        self.counts.scans += 1;
        var quiet = true;
        for (0..self.count) |b_| {
            const b: u16 = @intCast(b_);
            for (Lane.all) |lane| {
                // Pressure is kept only while answers carry it: ask again.
                const p = self.peers[b].lane(lane);
                if (pressureLive(p, now_ms) and now_ms -| p.probe_ms >= PRESSURE_PROBE_MS) {
                    if (self.sendEmpty(b, lane, &self.peers[b], io)) p.probe_ms = now_ms;
                }
                if (pressureLive(p, now_ms)) quiet = false;
                self.tickSend(b, lane, now_ms, read, durable, io);
                self.tickAnswer(b, lane, read, durable, io);
                if (!self.settled(b, lane, read, durable)) quiet = false;
            }
        }
        self.quiet = quiet;
        self.seen = seen;
    }

    /// Nothing outstanding with `b` either way: every message sent is
    /// acked durably and trimmed, and every one received is told durably.
    /// Durable progress is only waited on when something isn't, so a
    /// settled channel needn't watch the durable index.
    fn settled(self: *const Driver, b: u16, lane: Lane, read: ReadView, durable: u64) bool {
        const p = &self.peers[b].lanes[@backingInt(lane)];
        const meta = channels.metaOf(read, b, lane);
        const last = meta.next_seq - 1;
        if (meta.acked != last or p.trim_at_ms != null or p.durable_ack > meta.acked or p.applied_ack > meta.acked) return false;
        const in = channels.inOf(read, b, lane) orelse return true;
        return self.durableAck(b, in, durable) == in.applied and p.told_applied >= in.applied and p.told_durable >= in.applied;
    }

    fn tickSend(self: *Driver, b: u16, lane: Lane, now_ms: u64, read: ReadView, durable: u64, io: anytype) void {
        const p = self.peers[b].lane(lane);
        const meta = channels.metaOf(read, b, lane);
        const last = meta.next_seq - 1;
        // Trims: one in flight; a lost one is proposed again.
        if (p.trim_at_ms) |at| {
            if (meta.acked >= p.trim_through or now_ms - at >= TRIM_LOST_MS) p.trim_at_ms = null;
        }
        if (p.trim_at_ms == null and p.durable_ack > meta.acked and now_ms - p.last_trim_ms >= TRIM_EVERY_MS) {
            const through = @min(p.durable_ack, meta.acked + ce.MAX_TRIM_SPAN);
            var tb: [32]u8 = undefined;
            const bytes = ce.Trim.encodeInto(&tb, .{ .to = b, .lane = @backingInt(lane), .first = meta.acked + 1, .through = through }) catch unreachable;
            if (io.propose(.channel_trim, bytes)) {
                p.trim_through = through;
                p.trim_at_ms = now_ms;
                p.last_trim_ms = now_ms;
                self.counts.trims_proposed += 1;
            }
        }
        if (last == 0 and p.applied_ack == 0) return;
        const floor = @max(meta.acked, p.applied_ack);
        if (p.sent_through < floor) p.sent_through = floor;
        // Nothing came back in time: rewind to what's known applied and
        // send again, more slowly each time.
        if (p.sent_through > floor and now_ms - p.progress_ms >= p.backoff_ms) {
            p.sent_through = floor;
            p.gen_counter += 1;
            p.backoff_ms = @min(p.backoff_ms * 2, RESEND_MAX_MS);
            p.progress_ms = now_ms;
            self.counts.resends += 1;
        }
        if (p.sent_through < last and p.sent_through - floor < WINDOW) {
            // Own-group sends don't wait for durability: their inbound entry
            // follows them in the same log.
            const ready = if (b == self.group or self.mutant == .past_durable) std.math.maxInt(u64) else durable;
            if (self.sendBatch(b, lane, p.sent_through + 1, last, floor, ready, read, io)) |sent| {
                if (p.sent_through == floor) p.progress_ms = now_ms;
                p.sent_through = sent;
                p.last_sent_ms = now_ms;
                return;
            }
        }
        // Idle, with a durable ack outstanding (a lost one would pin the
        // outbox): probe at the resend cadence.
        if (meta.acked < p.applied_ack and now_ms - p.last_sent_ms >= p.backoff_ms) {
            _ = self.sendEmpty(b, lane, &self.peers[b], io);
            p.last_sent_ms = now_ms;
        }
    }

    /// Sends `from..` up to `last`, within the window and the batch bound,
    /// as far as messages are durable. Returns the last seq sent, or null.
    fn sendBatch(self: *Driver, b: u16, lane: Lane, from: u64, last: u64, floor: u64, ready: u64, read: ReadView, io: anytype) ?u64 {
        const p = self.peers[b].lane(lane);
        if (!self.buffers()) return null;
        var list: schema.Writer = .{ .buf = self.msgs };
        var n: u32 = 0;
        var bytes: usize = 0;
        var seq = from;
        while (seq <= last and n < ce.MAX_BATCH and seq - floor <= WINDOW) : (seq += 1) {
            var k: [channels.OUT_KEY]u8 = undefined;
            const o = (read.get(channels.OutFam, channels.outKey(&k, b, lane, seq)) catch null) orelse break;
            if (o.v.sent_at > ready) break;
            if (n > 0 and bytes + o.v.payload.len > MAX_BATCH_BYTES) break;
            ce.Message.encode(&list, .{ .slot = o.v.slot, .entry_type = o.v.entry_type, .layout_epoch = o.v.layout_epoch, .flags = o.v.flags, .origin_kind = o.v.origin_kind, .origin_ns = o.v.origin_ns, .payload = o.v.payload }) catch break;
            bytes += o.v.payload.len;
            n += 1;
        }
        if (n == 0) return null;
        const f = encodeFrame(self.buf, .batch, Batch.Value{
            .lane = @backingInt(lane),
            .gen_term = self.term,
            .gen_counter = p.gen_counter,
            .first_seq = from,
            .messages = .{ .bytes = list.written(), .count = n },
        }) orelse return null;
        if (!io.sendFrame(b, f)) return null;
        self.counts.batches += 1;
        self.counts.messages_sent += n;
        return from + n - 1;
    }

    fn sendEmpty(self: *Driver, b: u16, lane: Lane, peer: *Peer, io: anytype) bool {
        const p = peer.lane(lane);
        var buf: [64]u8 = undefined;
        const f = encodeFrame(&buf, .batch, Batch.Value{ .lane = @backingInt(lane), .gen_term = self.term, .gen_counter = p.gen_counter, .first_seq = p.sent_through + 1, .messages = .{ .bytes = &.{}, .count = 0 } }) orelse return false;
        return io.sendFrame(b, f);
    }

    fn tickAnswer(self: *Driver, a: u16, lane: Lane, read: ReadView, durable: u64, io: anytype) void {
        const p = self.peers[a].lane(lane);
        const in = channels.inOf(read, a, lane) orelse return;
        if (lane == .sink) self.releaseApplied(a, in.applied);
        const dur = self.durableAck(a, in, durable);
        if (in.applied <= p.told_applied and dur <= p.told_durable) return;
        self.answer(a, lane, in.applied, dur, 0, .ok, read, io);
    }

    /// What writes to this group on `lane` are queued behind, written to
    /// `w`, and how many: its own state, on every lane, and on the sink
    /// lane a full target while nothing has unparked since one refused
    /// `to`'s batch, or its share while its use is no lower than when that
    /// held one. None of it reaches an apply: it only tells senders'
    /// leaders to wait.
    fn pressure(self: *const Driver, status: Status, lane: Lane, to: *const Peer, read: ReadView, w: *schema.Writer) u32 {
        var n: u32 = 0;
        const own: ?Cause = if (status == .halted) .halted else self.own_cause;
        if (own) |c| n += @intFromBool(addStall(w, self.group, c));
        if (lane != .sink) return n;
        if (to.refused_parked) |at| if (channels.shardParked(read).bytes >= at) {
            n += @intFromBool(addStall(w, self.group, .target_full));
        };
        if (to.refused_use) |at| if (channels.budgetOf(read)) |b| if (b.use >= at) {
            n += @intFromBool(addStall(w, self.group, .over_share));
        };
        return n;
    }

    fn addStall(w: *schema.Writer, shard: u16, cause: Cause) bool {
        Stall.encode(w, .{ .shard = shard, .cause = @backingInt(cause) }) catch return false;
        return true;
    }

    fn durableAck(self: *const Driver, a: u16, in: channels.In.Value, durable: u64) u64 {
        // A self-channel's applied is durable: its inbound entry is later in
        // the same log than the sends it carries.
        return if (a == self.group) in.applied else channels.appliedAsOf(in, durable);
    }

    fn answer(self: *Driver, a: u16, lane: Lane, applied: u64, dur: u64, gap: u64, status: Status, read: ReadView, io: anytype) void {
        const peer = &self.peers[a];
        const p = peer.lane(lane);
        var pb: [Pressure.max_size]u8 = undefined;
        var pw: schema.Writer = .{ .buf = &pb };
        const n = self.pressure(status, lane, peer, read, &pw);
        if (n > 0) self.counts.pressured += 1;
        var buf: [128]u8 = undefined;
        const f = encodeFrame(&buf, .answer, Answer.Value{
            .lane = @backingInt(lane),
            .gen_term = p.recv_gen.term,
            .gen_counter = p.recv_gen.counter,
            .applied = applied,
            .durable = dur,
            .gap = gap,
            .status = @backingInt(status),
            .pressure = .{ .bytes = pw.written(), .count = n },
        }) orelse return;
        if (io.sendFrame(a, f)) {
            p.told_applied = @max(p.told_applied, applied);
            p.told_durable = @max(p.told_durable, dur);
        }
    }

    /// A frame from group `from`, which the caller takes from the inbox's
    /// sender. `halted` is this group's own halt.
    pub fn onFrame(self: *Driver, from: u16, bytes: []const u8, now_ms: u64, read: ReadView, durable: u64, halted: bool, io: anytype) void {
        if (from >= self.count) return;
        const f = Frame.decode(bytes) orelse return;
        self.now_ms = now_ms;
        // An answer can move an ack, and a batch be owed one: look again.
        self.quiet = false;
        switch (f) {
            .batch => |b| self.onBatch(from, b, read, durable, halted, io),
            .answer => |a| self.onAnswer(from, a, now_ms, read),
        }
    }

    fn onBatch(self: *Driver, from: u16, b: Batch.Value, read: ReadView, durable: u64, halted: bool, io: anytype) void {
        // A lane this build doesn't have: a frame from another build, or
        // none at all.
        const lane = Lane.from(b.lane) orelse return;
        const peer = &self.peers[from];
        const p = peer.lane(lane);
        const in = channels.inOf(read, from, lane);
        const applied = if (in) |x| x.applied else 0;
        const dur = if (in) |x| self.durableAck(from, x, durable) else 0;
        if (p.accepted == null or p.accepted.? < applied) p.accepted = applied;
        if (lane == .sink) self.releaseApplied(from, applied);
        p.recv_gen = .{ .term = b.gen_term, .counter = b.gen_counter };
        if (halted) return self.answer(from, lane, applied, dur, 0, .halted, read, io);
        if (b.messages.count == 0) return self.answer(from, lane, applied, dur, 0, .ok, read, io);
        const accepted = p.accepted.?;
        const last = b.first_seq + b.messages.count - 1;
        if (last <= accepted) {
            if (self.mutant == .ack_last_seq) return self.answer(from, lane, last, last, 0, .ok, read, io);
            return self.answer(from, lane, applied, dur, 0, .ok, read, io);
        }
        if (b.first_seq > accepted + 1) {
            self.counts.gaps += 1;
            return self.answer(from, lane, applied, dur, accepted + 1, .ok, read, io);
        }
        // A message that doesn't fit its lane is the sending build's bug:
        // the frame is dropped here, before it can halt this group at apply,
        // which refuses it too.
        if (!laneFits(lane, from, self.group, b)) {
            self.counts.batches_dropped += 1;
            return;
        }
        // Propose from the first seq not yet accepted.
        if (!self.buffers()) return self.answer(from, lane, applied, dur, 0, .busy, read, io);
        var w: schema.Writer = .{ .buf = self.msgs };
        var it = b.messages.iterator();
        var seq = b.first_seq;
        var n: u32 = 0;
        var held: channels.Admit = .take;
        while (it.next() catch null) |m| : (seq += 1) {
            if (seq <= accepted) continue;
            // A message whose target is over its parked cap, counting what
            // is on its way there, or that its shard's share or its
            // sender's sinks in flight hold, waits at its sender, and so do
            // those after it: the batch's prefix goes. It is stamped with
            // this node's caps before it is logged, so every replica applies
            // it under the same ones, and judged and counted as it is logged.
            // Only the sink lane's are judged: no other lane carries a
            // sink's message (`laneFits`), and none of theirs parks.
            var logged = m;
            if (self.stamp) |f| if (f(self.stamp_ctx, m, self.stamp_buf)) |payload| {
                logged.payload = payload;
            };
            const target = if (lane != .sink) null else if (self.sink_target) |f| f(m) else null;
            if (lane == .sink) {
                if (self.admits) |admits| {
                    held = admits(read, logged, self.inFlight(from, target));
                    if (held != .take) break;
                }
                if (!self.track(from, seq, target, logged.payload.len)) break;
            }
            ce.Message.encode(&w, logged) catch {
                self.untrackAfter(from, lane, accepted);
                return;
            };
            n += 1;
        }
        if (n == 0) {
            self.counts.busy += 1;
            // The cause this hold names, and no other: a stale one would
            // mask it.
            peer.refused_parked = if (held == .target_full) channels.shardParked(read).bytes else null;
            peer.refused_use = if (held == .over_share) if (channels.budgetOf(read)) |bg| bg.use else null else null;
            if (held == .over_share) self.counts.over_share += 1;
            return self.answer(from, lane, applied, dur, 0, .busy, read, io);
        }
        if (lane == .sink) {
            peer.refused_parked = null;
            peer.refused_use = null;
        }
        const entry = ce.Inbound.encodeInto(self.buf, .{ .from = from, .lane = @backingInt(lane), .first_seq = accepted + 1, .messages = .{ .bytes = w.written(), .count = n } }) catch {
            self.untrackAfter(from, lane, accepted);
            return;
        };
        if (!io.propose(.channel_inbound, entry)) {
            self.untrackAfter(from, lane, accepted);
            self.counts.busy += 1;
            return self.answer(from, lane, applied, dur, 0, .busy, read, io);
        }
        p.accepted = accepted + n;
        self.counts.inbound_proposed += 1;
        self.answer(from, lane, applied, dur, 0, .ok, read, io);
    }

    /// Whether every message of `b` rides `lane` as `writeSends` would send
    /// it: its flags pick that lane, and a history message is its own
    /// group's and no sink's.
    fn laneFits(lane: Lane, from: u16, me: u16, b: Batch.Value) bool {
        if (lane == .history and from != me) return false;
        var it = b.messages.iterator();
        while (it.next() catch return false) |m| {
            if (Lane.of(m.flags) != lane) return false;
            if (lane == .history and m.flags & entry_mod.Flags.SINK != 0) return false;
        }
        return true;
    }

    fn onAnswer(self: *Driver, from: u16, a: Answer.Value, now_ms: u64, read: ReadView) void {
        const lane = Lane.from(a.lane) orelse return;
        const peer = &self.peers[from];
        const p = peer.lane(lane);
        const meta = channels.metaOf(read, from, lane);
        // Acks are bounded by what this group's state says was sent, never
        // by what this leader sent: a new leader resends from `acked` while
        // the receiver is already ahead.
        if (self.mutant != .unbounded_ack and (a.applied > meta.next_seq - 1 or a.durable > meta.next_seq - 1 or a.durable > a.applied)) {
            self.counts.acks_refused += 1;
            return;
        }
        if (a.applied > p.applied_ack) {
            p.applied_ack = a.applied;
            p.progress_ms = now_ms;
            p.backoff_ms = RESEND_MIN_MS;
        }
        p.durable_ack = @max(p.durable_ack, if (self.mutant == .trim_on_applied) a.applied else a.durable);
        const current = self.mutant == .no_gen_filter or (a.gen_term == self.term and a.gen_counter == p.gen_counter);
        if (a.gap != 0 and current) {
            const n = std.math.clamp(a.gap, meta.acked + 1, meta.next_seq);
            p.sent_through = n - 1;
            p.gen_counter += 1;
            p.progress_ms = now_ms;
        }
        if (std.enums.fromInt(Status, a.status)) |st| {
            if (st != .ok) p.backoff_ms = @min(p.backoff_ms * 2, RESEND_MAX_MS);
        }
        peer.answered_ms = now_ms;
        // The pressure it reports now on this lane, if any, replaces what
        // it reported on it.
        var it = a.pressure.iterator();
        var n: u8 = 0;
        while (it.next() catch null) |st| {
            if (n == MAX_PRESSURE) break;
            if (std.enums.fromInt(Cause, st.cause) == null) continue;
            p.pressure[n] = st;
            n += 1;
        }
        p.pressure_len = n;
        if (n > 0) p.pressure_ms = now_ms;
    }
};

test "channel driver: outbox caps are an eighth of the node's memory over its shards for most messages, half that again for a sink's, a quarter of each a channel, within their ceilings" {
    try std.testing.expectEqual(OutboxCaps{ .other = .{ .shard = 256 << 20, .channel = 64 << 20 }, .sink = .{ .shard = 128 << 20, .channel = 32 << 20 } }, OutboxCaps.of(16 << 30, 8));
    try std.testing.expectEqual(OutboxCaps{ .other = .{ .shard = 1 << 30, .channel = 128 << 20 }, .sink = .{ .shard = 512 << 20, .channel = 64 << 20 } }, OutboxCaps.of(256 << 30, 4));
    try std.testing.expectEqual(OutboxCaps{ .other = .{ .shard = 32 << 20, .channel = 8 << 20 }, .sink = .{ .shard = 16 << 20, .channel = 4 << 20 } }, OutboxCaps.of(1 << 30, 4));
}

test "channel driver: byte counts read in the largest unit that keeps them whole" {
    var buf: [32]u8 = undefined;
    try std.testing.expectEqualStrings("512 B", bytesText(&buf, 512));
    try std.testing.expectEqualStrings("37 KiB", bytesText(&buf, 37 * 1024 + 5));
    try std.testing.expectEqualStrings("64 MiB", bytesText(&buf, 64 << 20));
}

test "channel driver: a peer silent for twice the longest resend backoff reads as having no leader, and a halted one that goes quiet stops reading halted" {
    var d = try Driver.init(std.testing.allocator, 0, 2);
    defer d.deinit();
    var store = @import("store.zig").Store.init(std.testing.allocator);
    defer store.deinit();
    const Quiet = struct {
        pub fn sendFrame(_: @This(), _: u16, _: []const u8) bool {
            return true;
        }
        pub fn propose(_: @This(), _: anytype, _: []const u8) bool {
            return true;
        }
    };
    d.tick(10_000, .{ .store = &store }, 0, Quiet{});
    try std.testing.expectEqual(@as(?Cause, null), d.causeOf(1, .main, 10_000));
    // It answers halted.
    d.peers[1].answered_ms = 11_000;
    d.peers[1].lanes[0].pressure[0] = .{ .shard = 1, .cause = @backingInt(Cause.halted) };
    d.peers[1].lanes[0].pressure_len = 1;
    d.peers[1].lanes[0].pressure_ms = 11_000;
    try std.testing.expectEqual(@as(?Cause, .halted), d.causeOf(1, .main, 11_500));
    // Then nothing: a second on, its pressure is gone; ten, it is silent.
    try std.testing.expectEqual(@as(?Cause, null), d.causeOf(1, .main, 12_000));
    try std.testing.expectEqual(@as(?Cause, null), d.causeOf(1, .main, 11_000 + NO_LEADER_MS - 1));
    try std.testing.expectEqual(@as(?Cause, .no_leader), d.causeOf(1, .main, 11_000 + NO_LEADER_MS));
}

test "channel driver: what a leader proposed and hasn't applied is released when it applies, and all of it with the term" {
    var d = try Driver.init(std.testing.allocator, 1, 3);
    defer d.deinit();
    const t1: channels.Target = @splat(1);
    const t2: channels.Target = @splat(2);
    try std.testing.expect(d.track(0, 1, t1, 100));
    try std.testing.expect(d.track(0, 2, t2, 50));
    try std.testing.expect(d.track(2, 1, t1, 10));
    // A sink's message with no target counts only among the sinks.
    try std.testing.expect(d.track(2, 2, null, 7));
    try std.testing.expectEqual(channels.InFlight{ .target = 110, .total = 160, .sink = 167, .sink_own = 150, .senders = 3 }, d.inFlight(0, t1));
    // Sender 0's first applied.
    d.releaseApplied(0, 1);
    try std.testing.expectEqual(channels.InFlight{ .target = 10, .total = 60, .sink = 67, .sink_own = 50, .senders = 3 }, d.inFlight(0, t1));
    try std.testing.expectEqual(channels.InFlight{ .target = 50, .total = 60, .sink = 67, .sink_own = 50, .senders = 3 }, d.inFlight(0, t2));
    d.releaseApplied(2, 2);
    try std.testing.expectEqual(channels.InFlight{ .target = 50, .total = 50, .sink = 50, .sink_own = 50, .senders = 3 }, d.inFlight(0, t2));
    // A new term: none of it is this leader's any more.
    d.reset(7);
    try std.testing.expectEqual(channels.InFlight{ .senders = 3 }, d.inFlight(0, t2));
    try std.testing.expectEqual(@as(usize, 0), d.target_bytes.count());
}

/// A driver's io that takes every frame and proposal, and counts them.
const Taking = struct {
    frames: *u32,
    proposals: *u32,
    pub fn sendFrame(io: Taking, _: u16, _: []const u8) bool {
        io.frames.* += 1;
        return true;
    }
    pub fn propose(io: Taking, _: anytype, _: []const u8) bool {
        io.proposals.* += 1;
        return true;
    }
};

const TestIo = struct {
    proposed: u32 = 0,

    pub fn sendFrame(_: *TestIo, _: u16, _: []const u8) bool {
        return true;
    }

    pub fn propose(io: *TestIo, _: entry_mod.EntryType, _: []const u8) bool {
        io.proposed += 1;
        return true;
    }
};

/// `store`, with group `from`'s `lane` applied through `applied` as its
/// receiver's record says.
fn appliedIn(store: *store_mod.Store, from: u16, lane: Lane, applied: u64) !void {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var keys = view_mod.KeySet.init(arena.allocator());
    const b = channels.chanBytes(from, lane);
    try keys.add(channels.InFam.ID, "", &b);
    var view = view_mod.View.init(store, arena.allocator(), &keys);
    var kb: [channels.CHAN_KEY]u8 = undefined;
    try view.put(channels.InFam, channels.chanKey(&kb, from, lane), .{ .v = .{ .applied = applied, .floor_index = 0, .floor_seq = applied, .points = .{ .bytes = "", .count = 0 } } });
    try view.commit();
}

fn batchOf(buf: []u8, lane: Lane, first: u64, msgs: []const u8, n: u32) []const u8 {
    return encodeFrame(buf, .batch, Batch.Value{ .lane = @backingInt(lane), .gen_term = 1, .gen_counter = 0, .first_seq = first, .messages = .{ .bytes = msgs, .count = n } }).?;
}

test "channel driver: what another lane applied releases nothing the sink lane has in flight" {
    var d = try Driver.init(std.testing.allocator, 0, 2);
    defer d.deinit();
    var store = store_mod.Store.init(std.testing.allocator);
    defer store.deinit();
    const t: channels.Target = @splat(1);
    try std.testing.expect(d.track(0, 1, t, 100));
    // Group 0's own history and main lanes have applied seq 5; its sink
    // lane, nothing.
    try appliedIn(&store, 0, .history, 5);
    try appliedIn(&store, 0, .main, 5);
    var frames: u32 = 0;
    var proposals: u32 = 0;
    const io = Taking{ .frames = &frames, .proposals = &proposals };
    var fb: [64]u8 = undefined;
    d.onFrame(0, batchOf(&fb, .history, 6, "", 0), 1, .{ .store = &store }, 0, false, io);
    d.onFrame(0, batchOf(&fb, .main, 6, "", 0), 1, .{ .store = &store }, 0, false, io);
    d.tick(1, .{ .store = &store }, 0, io);
    try std.testing.expectEqual(channels.InFlight{ .target = 100, .total = 100, .sink = 100, .sink_own = 100, .senders = 2 }, d.inFlight(0, t));
    // The sink lane's applied releases it.
    try appliedIn(&store, 0, .sink, 1);
    d.tick(2, .{ .store = &store }, 0, io);
    try std.testing.expectEqual(channels.InFlight{ .senders = 2 }, d.inFlight(0, t));
}

test "channel driver: a history batch taken leaves a full target named, and one that doesn't fit its lane is dropped" {
    var d = try Driver.init(std.testing.allocator, 0, 2);
    defer d.deinit();
    var store = store_mod.Store.init(std.testing.allocator);
    defer store.deinit();
    var frames: u32 = 0;
    var proposals: u32 = 0;
    const io = Taking{ .frames = &frames, .proposals = &proposals };
    // A target over its cap refused group 0's sink-lane batch.
    d.peers[0].refused_parked = 7;
    var mb: [256]u8 = undefined;
    var fb: [512]u8 = undefined;
    for ([_]u16{ entry_mod.Flags.HISTORY, entry_mod.Flags.HISTORY | entry_mod.Flags.SINK, entry_mod.Flags.SINK }, [_]u32{ 1, 1, 1 }, [_]u64{ 0, 1, 2 }, [_]u64{ 1, 2, 2 }) |flags, proposed, dropped, first| {
        var w: schema.Writer = .{ .buf = &mb };
        try ce.Message.encode(&w, .{ .slot = 0, .entry_type = 0, .layout_epoch = 0, .flags = flags, .origin_kind = 1, .origin_ns = 1, .payload = "x" });
        d.onFrame(0, batchOf(&fb, .history, first, w.written(), 1), 1, .{ .store = &store }, 0, false, io);
        try std.testing.expectEqual(proposed, proposals);
        try std.testing.expectEqual(dropped, d.counts.batches_dropped);
    }
    // Taken on the history lane, the sink lane's full target is still named.
    try std.testing.expectEqual(@as(?u64, 7), d.peers[0].refused_parked);
    // From another group, history is dropped whatever it carries.
    var w: schema.Writer = .{ .buf = &mb };
    try ce.Message.encode(&w, .{ .slot = 1, .entry_type = 0, .layout_epoch = 0, .flags = entry_mod.Flags.HISTORY, .origin_kind = 1, .origin_ns = 1, .payload = "x" });
    d.onFrame(1, batchOf(&fb, .history, 1, w.written(), 1), 1, .{ .store = &store }, 0, false, io);
    try std.testing.expectEqual(@as(u64, 3), d.counts.batches_dropped);
    try std.testing.expectEqual(@as(u32, 1), proposals);
}

/// A batch of one message of `len` bytes from seq `first` on `flags`'
/// lane, of an entry no target parks.
fn oneMessageBatch(buf: []u8, msgs: []u8, term: u64, first: u64, len: usize, flags: u16) []const u8 {
    const payload: [256]u8 = @splat('p');
    var w: schema.Writer = .{ .buf = msgs };
    ce.Message.encode(&w, .{ .slot = 1, .entry_type = @backingInt(entry_mod.EntryType.kv_put), .layout_epoch = 0, .flags = flags, .origin_kind = 0, .origin_ns = 0, .payload = payload[0..len] }) catch unreachable;
    return encodeFrame(buf, .batch, Batch.Value{ .lane = @backingInt(Lane.of(flags)), .gen_term = term, .gen_counter = 0, .first_seq = first, .messages = .{ .bytes = w.written(), .count = 1 } }).?;
}

var test_over_share = false;

fn testBudget(_: ReadView) ?channels.Budget {
    return .{ .use = if (test_over_share) 1 << 20 else 0, .share = 1 << 19 };
}

const TestChannels = channels.Channels(&.{@import("channel_probe.zig").machine});

test "channel driver: every sink's message counts in flight, with a target or not, and a sender waits only on its own" {
    channels.test_budget = testBudget;
    defer channels.test_budget = null;
    // Three groups: each sender's cap is 20 bytes.
    channels.test_sink_in_flight_max = 60;
    defer channels.test_sink_in_flight_max = null;
    var store = store_mod.Store.init(std.testing.allocator);
    defer store.deinit();
    const read: ReadView = .{ .store = &store };
    var d = try Driver.init(std.testing.allocator, 0, 3);
    defer d.deinit();
    d.reset(1);
    d.admits = TestChannels.admits;
    d.sink_target = TestChannels.sinkTarget;
    var io: TestIo = .{};
    var fb: [512]u8 = undefined;
    var mb: [512]u8 = undefined;
    const SINK = entry_mod.Flags.SINK;

    // Group 2's sink, with no target to park under, is counted in flight,
    // under its sender only.
    d.onFrame(2, oneMessageBatch(&fb, &mb, 1, 1, 20, SINK), 0, read, 0, false, &io);
    try std.testing.expectEqual(@as(u32, 1), io.proposed);
    try std.testing.expectEqual(@as(u64, 20), d.sink_bytes);
    try std.testing.expectEqual(@as(u64, 20), d.peers[2].sink_bytes);
    try std.testing.expectEqual(@as(u64, 0), d.in_flight_bytes);
    // A message that isn't a sink's counts nothing.
    d.onFrame(1, oneMessageBatch(&fb, &mb, 1, 1, 30, 0), 0, read, 0, false, &io);
    try std.testing.expectEqual(@as(u32, 2), io.proposed);
    try std.testing.expectEqual(@as(u64, 20), d.sink_bytes);
    // Group 2's next waits on its own: busy, naming no cause.
    d.onFrame(2, oneMessageBatch(&fb, &mb, 1, 2, 10, SINK), 0, read, 0, false, &io);
    try std.testing.expectEqual(@as(u32, 2), io.proposed);
    try std.testing.expect(d.peers[2].refused_use == null and d.peers[2].refused_parked == null);
    // Group 1's sink, the first on its sink lane, isn't held by group 2's
    // in flight, however large.
    d.onFrame(1, oneMessageBatch(&fb, &mb, 1, 1, 200, SINK), 0, read, 0, false, &io);
    try std.testing.expectEqual(@as(u32, 3), io.proposed);
    try std.testing.expectEqual(@as(u64, 220), d.sink_bytes);
    // Group 2's own apply releases it, and its resend is taken.
    d.releaseApplied(2, 1);
    d.onFrame(2, oneMessageBatch(&fb, &mb, 1, 2, 10, SINK), 0, read, 0, false, &io);
    try std.testing.expectEqual(@as(u32, 4), io.proposed);
    try std.testing.expectEqual(@as(u64, 210), d.sink_bytes);
}

test "channel driver: a hold names its own cause and clears the other's" {
    channels.test_budget = testBudget;
    defer channels.test_budget = null;
    test_over_share = true;
    defer test_over_share = false;
    var store = store_mod.Store.init(std.testing.allocator);
    defer store.deinit();
    const read: ReadView = .{ .store = &store };
    var d = try Driver.init(std.testing.allocator, 0, 2);
    defer d.deinit();
    d.reset(1);
    d.admits = TestChannels.admits;
    d.sink_target = TestChannels.sinkTarget;
    var io: TestIo = .{};
    var fb: [512]u8 = undefined;
    var mb: [512]u8 = undefined;
    // An earlier full target's hold, still recorded.
    d.peers[1].refused_parked = 0;
    d.onFrame(1, oneMessageBatch(&fb, &mb, 1, 1, 10, entry_mod.Flags.SINK), 0, read, 0, false, &io);
    try std.testing.expectEqual(@as(u32, 0), io.proposed);
    try std.testing.expect(d.peers[1].refused_parked == null);
    try std.testing.expectEqual(@as(?u64, 1 << 20), d.peers[1].refused_use);
    try std.testing.expectEqual(@as(u64, 1), d.counts.over_share);
}

test "channel driver: a run's history and a main message are taken while their shard is over its share" {
    channels.test_budget = testBudget;
    defer channels.test_budget = null;
    test_over_share = true;
    defer test_over_share = false;
    var store = store_mod.Store.init(std.testing.allocator);
    defer store.deinit();
    const read: ReadView = .{ .store = &store };
    var d = try Driver.init(std.testing.allocator, 0, 2);
    defer d.deinit();
    d.reset(1);
    d.admits = TestChannels.admits;
    d.sink_target = TestChannels.sinkTarget;
    var io: TestIo = .{};
    var fb: [512]u8 = undefined;
    var mb: [512]u8 = undefined;
    // A sink's message from group 1 is held...
    d.onFrame(1, oneMessageBatch(&fb, &mb, 1, 1, 10, entry_mod.Flags.SINK), 0, read, 0, false, &io);
    try std.testing.expectEqual(@as(u32, 0), io.proposed);
    // ...and this group's own history goes, counting nothing in flight.
    d.onFrame(0, oneMessageBatch(&fb, &mb, 1, 1, 10, entry_mod.Flags.HISTORY), 0, read, 0, false, &io);
    try std.testing.expectEqual(@as(u32, 1), io.proposed);
    // ...and so does group 1's main message, on the same channel, and its
    // answer names no share.
    d.onFrame(1, oneMessageBatch(&fb, &mb, 1, 1, 10, 0), 0, read, 0, false, &io);
    try std.testing.expectEqual(@as(u32, 2), io.proposed);
    try std.testing.expectEqual(@as(u64, 0), d.sink_bytes);
    var pw_buf: [Pressure.max_size]u8 = undefined;
    var pw: schema.Writer = .{ .buf = &pw_buf };
    try std.testing.expectEqual(@as(u32, 0), d.pressure(.ok, .main, &d.peers[1], read, &pw));
    try std.testing.expectEqual(@as(u32, 1), d.pressure(.ok, .sink, &d.peers[1], read, &pw));
}
