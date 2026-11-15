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
    // Receiving from it.
    /// The highest seq proposed or applied from it; null until rebuilt.
    accepted: ?u64 = null,
    recv_gen: struct { term: u64 = 0, counter: u32 = 0 } = .{},
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
};

pub const Driver = struct {
    group: u16,
    mutant: Mutant = .none,
    count: u16,
    term: u64 = 0,
    /// One per group on the node.
    peers: []Peer,
    counts: Counts = .{},
    /// Messages each group sent this one that were refused as its own bug,
    /// by reason (`store.SenderBug`). Counted where they apply, on every
    /// replica, and kept across terms, unlike `peers`.
    sender_bugs: [][2]u64,
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
        @memset(bugs, .{ 0, 0 });
        return .{ .allocator = allocator, .group = group, .count = count, .peers = peers, .sender_bugs = bugs };
    }

    pub fn deinit(self: *Driver) void {
        self.allocator.free(self.peers);
        self.allocator.free(self.sender_bugs);
        self.allocator.free(self.buf);
        self.allocator.free(self.msgs);
    }

    /// The batch buffers, allocated once; false when there's no memory for
    /// them, and the batch waits.
    fn buffers(self: *Driver) bool {
        if (self.buf.len > 0) return true;
        self.buf = self.allocator.alloc(u8, 1 + ce.Inbound.max_size) catch return false;
        self.msgs = self.allocator.alloc(u8, ce.Messages.max_size) catch {
            self.allocator.free(self.buf);
            self.buf = &.{};
            return false;
        };
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
        const seen = seenOf(read);
        if (self.quiet and std.meta.eql(seen, self.seen)) return;
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

    fn durableAck(self: *const Driver, a: u16, in: channels.In.Value, durable: u64) u64 {
        // A self-channel's applied is durable: its inbound entry is later in
        // the same log than the sends it carries.
        return if (a == self.group) in.applied else channels.appliedAsOf(in, durable);
    }

    fn answer(_: *Driver, a: u16, p: *Peer, applied: u64, dur: u64, gap: u64, status: Status, io: anytype) void {
        var buf: [64]u8 = undefined;
        const f = encodeFrame(&buf, .answer, Answer.Value{
            .gen_term = p.recv_gen.term,
            .gen_counter = p.recv_gen.counter,
            .applied = applied,
            .durable = dur,
            .gap = gap,
            .status = @backingInt(status),
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
            ce.Message.encode(&w, m) catch return;
            n += 1;
        }
        const entry = ce.Inbound.encodeInto(self.buf, .{ .from = from, .first_seq = accepted + 1, .messages = .{ .bytes = w.written(), .count = n } }) catch return;
        if (!io.propose(.channel_inbound, entry)) {
            self.counts.busy += 1;
            return self.answer(from, p, applied, dur, 0, .busy, io);
        }
        p.accepted = last;
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
        if (std.enums.fromInt(Status, a.status)) |s| if (s != .ok) {
            p.backoff_ms = @min(p.backoff_ms * 2, RESEND_MAX_MS);
        };
    }
};
