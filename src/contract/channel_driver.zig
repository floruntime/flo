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
const schema = @import("../protocol/schema.zig");
const ce = @import("../protocol/channel_entries.zig");
const channels = @import("channels.zig");
const view_mod = @import("view.zig");
const entry_mod = @import("../storage/ual/entry.zig");

const ReadView = view_mod.ReadView;
const Int = schema.Int;
const Record = schema.Record;

/// Most messages in flight past the receiver's applied ack.
pub const WINDOW = 1024;

/// How much a shard's channel outboxes may hold before the edge stops
/// adding to them: per channel and in all. Sized per node, from its
/// memory, and read only by its leaders at the edge, so two nodes may
/// size them differently; never in state.
pub const OutboxCaps = struct {
    shard: u64 = 1 << 30,
    channel: u64 = 128 << 20,

    /// The caps for a node of `memory` bytes running `shards` shards:
    /// an eighth of its memory over its shards, at most 1 GiB a shard,
    /// and a quarter of that a channel, at most 128 MiB.
    pub fn of(memory: u64, shards: u16) OutboxCaps {
        const shard = @min(@as(u64, 1 << 30), memory / 8 / @max(shards, 1));
        return .{ .shard = shard, .channel = @min(@as(u64, 128 << 20), shard / 4) };
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

/// Why writes through a shard are queued: what an answer's pressure names.
pub const Cause = enum(u8) {
    /// The named shard is halted.
    halted = 1,
    /// The named shard hasn't answered in a while: no leader, or none it
    /// can reach.
    no_leader = 2,
    /// The named shard can't flush its log to disk.
    cannot_flush = 3,
    /// A target on the named shard is full, past what it parks.
    target_full = 4,
    /// The named shard takes what it is sent slower than it comes.
    outbox_full = 5,
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
/// A peer silent this long is taken to have no leader.
pub const NO_LEADER_MS = 5_000;

/// What `cause` says of the shard it names, in words.
pub fn causeWords(cause: Cause) []const u8 {
    return switch (cause) {
        .halted => "is halted",
        .no_leader => "has no leader",
        .cannot_flush => "cannot flush to disk",
        .target_full => "has a full target",
        .outbox_full => "is busy",
    };
}

/// The refusal a write bound through shard `to` gets while its pressure
/// names `stall`: "overloaded: writes to shard 7 are queued because shard
/// 9 is halted; this request was not run — retry later".
pub fn pressureRefusal(buf: []u8, to: u16, stall: Stall.Value) []const u8 {
    const cause = std.enums.fromInt(Cause, stall.cause) orelse .outbox_full;
    return std.fmt.bufPrint(buf, "overloaded: writes to shard {d} are queued because shard {d} {s}; this request was not run — retry later", .{ to, stall.shard, causeWords(cause) }) catch "overloaded: writes are queued; this request was not run — retry later";
}

pub const Batch = Record(&.{
    .{ .name = "gen_term", .T = Int(u64) },
    .{ .name = "gen_counter", .T = Int(u32) },
    .{ .name = "first_seq", .T = Int(u64) },
    .{ .name = "messages", .T = ce.Messages },
});

pub const Answer = Record(&.{
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

const Peer = struct {
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
    /// The pressure its last answer carried, and when: kept until
    /// `PRESSURE_EXPIRY_MS` after that, probed for every
    /// `PRESSURE_PROBE_MS` meanwhile.
    pressure: [MAX_PRESSURE]Stall.Value = undefined,
    pressure_len: u8 = 0,
    pressure_ms: u64 = 0,
    probe_ms: u64 = 0,
    /// When it last answered, and its status then: what this leader names
    /// as the cause when its outbox to it is full.
    answered_ms: u64 = 0,
    last_status: Status = .ok,
    /// The outbox to it holds `OutboxCaps.channel` or more; cleared below
    /// three quarters of it.
    outbox_full: bool = false,
    // Receiving from it.
    /// The highest seq proposed or applied from it; null until rebuilt.
    accepted: ?u64 = null,
    recv_gen: struct { term: u64 = 0, counter: u32 = 0 } = .{},
    /// Its last batch with messages was answered `busy`: until one is
    /// taken, every answer to it, probes' too, says a target is full.
    refused_busy: bool = false,
    told_applied: u64 = 0,
    told_durable: u64 = 0,
};

pub const Counts = struct {
    batches: u64 = 0,
    messages_sent: u64 = 0,
    resends: u64 = 0,
    gaps: u64 = 0,
    inbound_proposed: u64 = 0,
    busy: u64 = 0,
    trims_proposed: u64 = 0,
    acks_refused: u64 = 0,
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
};

pub const Driver = struct {
    group: u16,
    mutant: Mutant = .none,
    count: u16,
    term: u64 = 0,
    /// One per group on the node.
    peers: []Peer,
    /// Whether this group takes a message in now (`Channels.admits`); null
    /// takes every one.
    admits: ?*const fn (read: ReadView, m: ce.Message.Value) bool = null,
    counts: Counts = .{},
    /// What this leader's outboxes may hold, for the pressure it reports.
    caps: OutboxCaps = .{},
    /// Rewrites a message before this leader logs it, or null to log it as
    /// it came: a sink's enqueue takes this node's queue caps, as its own
    /// clients' do. Writes into `buf`; `stamp_ctx` is its own.
    stamp: ?*const fn (ctx: ?*const anyopaque, m: ce.Message.Value, buf: []u8) ?[]const u8 = null,
    stamp_ctx: ?*const anyopaque = null,
    stamp_buf: []u8 = &.{},
    /// Why writes through this group are queued by its own state, if they
    /// are: halted, or flushes failing. Set by its shard each tick.
    own_cause: ?Cause = null,
    /// The clock as of the last tick or frame.
    now_ms: u64 = 0,
    /// Messages each group sent this one that were refused as its own bug,
    /// by reason (`store.SenderBug`). Counted where they apply, on every
    /// replica, and kept across terms, unlike `peers`.
    sender_bugs: [][2]u64,
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
        const bugs = try allocator.alloc([2]u64, count);
        errdefer allocator.free(bugs);
        @memset(bugs, .{ 0, 0 });
        const refusals = try allocator.alloc(SinkRefusals, count);
        @memset(refusals, .initFill(0));
        return .{ .allocator = allocator, .group = group, .count = count, .peers = peers, .sender_bugs = bugs, .sink_refusals = refusals };
    }

    pub fn deinit(self: *Driver) void {
        self.allocator.free(self.peers);
        self.allocator.free(self.sender_bugs);
        self.allocator.free(self.sink_refusals);
        self.allocator.free(self.buf);
        self.allocator.free(self.msgs);
        self.allocator.free(self.stamp_buf);
    }

    /// What writes to group `to` are queued behind, as its last answers
    /// said, or null: what holds a pipeline sending to it.
    pub fn pressureOf(self: *const Driver, to: u16, now_ms: u64) ?Stall.Value {
        if (to >= self.peers.len) return null;
        const p = &self.peers[to];
        if (!pressureLive(p, now_ms)) return null;
        return p.pressure[0];
    }

    fn pressureLive(p: *const Peer, now_ms: u64) bool {
        return p.pressure_len > 0 and now_ms -| p.pressure_ms < PRESSURE_EXPIRY_MS;
    }

    /// Why this leader names `c` when its outbox to `c` is full: what `c`
    /// last said, or that it has gone quiet.
    fn causeOf(self: *const Driver, c: u16) Cause {
        const p = &self.peers[c];
        return switch (p.last_status) {
            .halted => .halted,
            .busy => .target_full,
            .ok => if (self.now_ms -| p.answered_ms >= NO_LEADER_MS) .no_leader else .outbox_full,
        };
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
        @memset(self.peers, .{});
        self.quiet = false;
    }

    /// The periodic work: deliver, resend, probe, trim, and tell senders
    /// what was applied. Once everything is delivered, acked, trimmed and
    /// told, it reads three commit counts and stops there, until a channel
    /// record changes or a frame arrives.
    pub fn tick(self: *Driver, now_ms: u64, read: ReadView, durable: u64, io: anytype) void {
        self.now_ms = now_ms;
        const seen = seenOf(read);
        if (self.quiet and std.meta.eql(seen, self.seen)) return;
        // Which outboxes are full, for the pressure this leader reports:
        // set at the cap, cleared below three quarters of it. Their bytes
        // move only with a channel record, so a quiet tick skips this.
        for (self.peers, 0..) |*p, c| {
            const bytes = channels.metaOf(read, @intCast(c)).bytes;
            if (bytes >= self.caps.channel) p.outbox_full = true else if (bytes < self.caps.channel / 4 * 3) p.outbox_full = false;
        }
        self.counts.scans += 1;
        var quiet = true;
        for (0..self.count) |b_| {
            const b: u16 = @intCast(b_);
            self.tickSend(b, now_ms, read, durable, io);
            self.tickAnswer(b, read, durable, io);
            if (!self.settled(b, read, durable)) quiet = false;
        }
        self.quiet = quiet;
        self.seen = seen;
    }

    /// Nothing outstanding with `b` either way: every message sent is
    /// acked durably and trimmed, and every one received is told durably.
    /// Durable progress is only waited on when something isn't, so a
    /// settled channel needn't watch the durable index.
    fn settled(self: *const Driver, b: u16, read: ReadView, durable: u64) bool {
        const p = &self.peers[b];
        if (pressureLive(p, self.now_ms)) return false;
        const meta = channels.metaOf(read, b);
        const last = meta.next_seq - 1;
        if (meta.acked != last or p.trim_at_ms != null or p.durable_ack > meta.acked or p.applied_ack > meta.acked) return false;
        const in = channels.inOf(read, b) orelse return true;
        return self.durableAck(b, in, durable) == in.applied and p.told_applied >= in.applied and p.told_durable >= in.applied;
    }

    fn tickSend(self: *Driver, b: u16, now_ms: u64, read: ReadView, durable: u64, io: anytype) void {
        const p = &self.peers[b];
        const meta = channels.metaOf(read, b);
        const last = meta.next_seq - 1;
        // Trims: one in flight; a lost one is proposed again.
        if (p.trim_at_ms) |at| {
            if (meta.acked >= p.trim_through or now_ms - at >= TRIM_LOST_MS) p.trim_at_ms = null;
        }
        if (p.trim_at_ms == null and p.durable_ack > meta.acked and now_ms - p.last_trim_ms >= TRIM_EVERY_MS) {
            const through = @min(p.durable_ack, meta.acked + ce.MAX_TRIM_SPAN);
            var tb: [32]u8 = undefined;
            const bytes = ce.Trim.encodeInto(&tb, .{ .to = b, .first = meta.acked + 1, .through = through }) catch unreachable;
            if (io.propose(.channel_trim, bytes)) {
                p.trim_through = through;
                p.trim_at_ms = now_ms;
                p.last_trim_ms = now_ms;
                self.counts.trims_proposed += 1;
            }
        }
        // Pressure is kept only while answers carry it: ask again.
        if (pressureLive(p, now_ms) and now_ms -| p.probe_ms >= PRESSURE_PROBE_MS) {
            if (self.sendEmpty(b, p, io)) p.probe_ms = now_ms;
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
            if (self.sendBatch(b, p.sent_through + 1, last, floor, ready, read, io)) |sent| {
                if (p.sent_through == floor) p.progress_ms = now_ms;
                p.sent_through = sent;
                p.last_sent_ms = now_ms;
                return;
            }
        }
        // Idle, with a durable ack outstanding (a lost one would pin the
        // outbox): probe at the resend cadence.
        if (meta.acked < p.applied_ack and now_ms - p.last_sent_ms >= p.backoff_ms) {
            _ = self.sendEmpty(b, p, io);
            p.last_sent_ms = now_ms;
        }
    }

    /// Sends `from..` up to `last`, within the window and the batch bound,
    /// as far as messages are durable. Returns the last seq sent, or null.
    fn sendBatch(self: *Driver, b: u16, from: u64, last: u64, floor: u64, ready: u64, read: ReadView, io: anytype) ?u64 {
        const p = &self.peers[b];
        if (!self.buffers()) return null;
        var list: schema.Writer = .{ .buf = self.msgs };
        var n: u32 = 0;
        var bytes: usize = 0;
        var seq = from;
        while (seq <= last and n < ce.MAX_BATCH and seq - floor <= WINDOW) : (seq += 1) {
            var k: [channels.OUT_KEY]u8 = undefined;
            const o = (read.get(channels.OutFam, channels.outKey(&k, b, seq)) catch null) orelse break;
            if (o.v.sent_at > ready) break;
            if (n > 0 and bytes + o.v.payload.len > MAX_BATCH_BYTES) break;
            ce.Message.encode(&list, .{ .slot = o.v.slot, .entry_type = o.v.entry_type, .layout_epoch = o.v.layout_epoch, .flags = o.v.flags, .payload = o.v.payload }) catch break;
            bytes += o.v.payload.len;
            n += 1;
        }
        if (n == 0) return null;
        const f = encodeFrame(self.buf, .batch, Batch.Value{
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

    fn sendEmpty(self: *Driver, b: u16, p: *Peer, io: anytype) bool {
        var buf: [64]u8 = undefined;
        const f = encodeFrame(&buf, .batch, Batch.Value{ .gen_term = self.term, .gen_counter = p.gen_counter, .first_seq = p.sent_through + 1, .messages = .{ .bytes = &.{}, .count = 0 } }) orelse return false;
        return io.sendFrame(b, f);
    }

    fn tickAnswer(self: *Driver, a: u16, read: ReadView, durable: u64, io: anytype) void {
        const p = &self.peers[a];
        const in = channels.inOf(read, a) orelse return;
        const dur = self.durableAck(a, in, durable);
        if (in.applied <= p.told_applied and dur <= p.told_durable) return;
        self.answer(a, p, in.applied, dur, 0, .ok, io);
    }

    /// What writes through this group are queued behind, written to `w`,
    /// and how many: its own state, a full target while `to`'s last batch
    /// was refused for one, and each group its outbox to is full, up to
    /// `MAX_PRESSURE`.
    fn pressure(self: *const Driver, status: Status, to: *const Peer, w: *schema.Writer) u32 {
        var n: u32 = 0;
        const own: ?Cause = if (status == .halted) .halted else self.own_cause;
        if (own) |c| n += @intFromBool(addStall(w, self.group, c));
        if (status == .busy or to.refused_busy) n += @intFromBool(addStall(w, self.group, .target_full));
        for (self.peers, 0..) |*p, c| {
            if (n == MAX_PRESSURE) break;
            if (c == self.group or !p.outbox_full) continue;
            n += @intFromBool(addStall(w, @intCast(c), self.causeOf(@intCast(c))));
        }
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

    fn answer(self: *Driver, a: u16, p: *Peer, applied: u64, dur: u64, gap: u64, status: Status, io: anytype) void {
        var pb: [Pressure.max_size]u8 = undefined;
        var pw: schema.Writer = .{ .buf = &pb };
        const n = self.pressure(status, p, &pw);
        var buf: [128]u8 = undefined;
        const f = encodeFrame(&buf, .answer, Answer.Value{
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
        const p = &self.peers[from];
        const in = channels.inOf(read, from);
        const applied = if (in) |x| x.applied else 0;
        const dur = if (in) |x| self.durableAck(from, x, durable) else 0;
        if (p.accepted == null or p.accepted.? < applied) p.accepted = applied;
        p.recv_gen = .{ .term = b.gen_term, .counter = b.gen_counter };
        if (halted) return self.answer(from, p, applied, dur, 0, .halted, io);
        if (b.messages.count == 0) return self.answer(from, p, applied, dur, 0, .ok, io);
        const accepted = p.accepted.?;
        const last = b.first_seq + b.messages.count - 1;
        if (last <= accepted) {
            if (self.mutant == .ack_last_seq) return self.answer(from, p, last, last, 0, .ok, io);
            return self.answer(from, p, applied, dur, 0, .ok, io);
        }
        if (b.first_seq > accepted + 1) {
            self.counts.gaps += 1;
            return self.answer(from, p, applied, dur, accepted + 1, .ok, io);
        }
        // Propose from the first seq not yet accepted.
        if (!self.buffers()) return self.answer(from, p, applied, dur, 0, .busy, io);
        var w: schema.Writer = .{ .buf = self.msgs };
        var it = b.messages.iterator();
        var seq = b.first_seq;
        var n: u32 = 0;
        while (it.next() catch null) |m| : (seq += 1) {
            if (seq <= accepted) continue;
            // A message whose target is over its parked cap waits at its
            // sender, and so do those after it: the batch's prefix goes.
            if (self.admits) |admits| if (!admits(read, m)) break;
            var logged = m;
            if (self.stamp) |f| if (f(self.stamp_ctx, m, self.stamp_buf)) |payload| {
                logged.payload = payload;
            };
            ce.Message.encode(&w, logged) catch return;
            n += 1;
        }
        if (n == 0) {
            self.counts.busy += 1;
            p.refused_busy = true;
            return self.answer(from, p, applied, dur, 0, .busy, io);
        }
        p.refused_busy = false;
        const entry = ce.Inbound.encodeInto(self.buf, .{ .from = from, .first_seq = accepted + 1, .messages = .{ .bytes = w.written(), .count = n } }) catch return;
        if (!io.propose(.channel_inbound, entry)) {
            self.counts.busy += 1;
            return self.answer(from, p, applied, dur, 0, .busy, io);
        }
        p.accepted = accepted + n;
        self.counts.inbound_proposed += 1;
        self.answer(from, p, applied, dur, 0, .ok, io);
    }

    fn onAnswer(self: *Driver, from: u16, a: Answer.Value, now_ms: u64, read: ReadView) void {
        const p = &self.peers[from];
        const meta = channels.metaOf(read, from);
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
            p.last_status = st;
        }
        p.answered_ms = now_ms;
        // The pressure it reports now, if any, replaces what it reported.
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

test "channel driver: outbox caps are an eighth of the node's memory over its shards, a quarter of that a channel, within their ceilings" {
    try std.testing.expectEqual(OutboxCaps{ .shard = 256 << 20, .channel = 64 << 20 }, OutboxCaps.of(16 << 30, 8));
    try std.testing.expectEqual(OutboxCaps{ .shard = 1 << 30, .channel = 128 << 20 }, OutboxCaps.of(256 << 30, 4));
    try std.testing.expectEqual(OutboxCaps{ .shard = 32 << 20, .channel = 8 << 20 }, OutboxCaps.of(1 << 30, 4));
}

test "channel driver: the refusal a pressured write gets names the shard it goes to and the one it is queued behind" {
    var buf: [192]u8 = undefined;
    try std.testing.expectEqualStrings(
        "overloaded: writes to shard 7 are queued because shard 9 is halted; this request was not run — retry later",
        pressureRefusal(&buf, 7, .{ .shard = 9, .cause = @backingInt(Cause.halted) }),
    );
    try std.testing.expectEqualStrings(
        "overloaded: writes to shard 2 are queued because shard 2 cannot flush to disk; this request was not run — retry later",
        pressureRefusal(&buf, 2, .{ .shard = 2, .cause = @backingInt(Cause.cannot_flush) }),
    );
}
