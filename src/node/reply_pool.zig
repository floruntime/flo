//! ReplyPool — the ask slots: room for the answers to what this shard's
//! clients asked of other shards.
//!
//! A client request forwarded to another shard takes a slot here first; its
//! answer comes back on this shard's reply ring naming the slot and the
//! slot's generation, and frees it. So:
//!
//! - the reply ring has room for every answer owed, and as many again for
//!   late answers to slots already expired (`ringCapacity`);
//! - an answer is delivered once: a second, or one for a slot reused since,
//!   finds another generation and is dropped;
//! - a client is told at its request's deadline (`take`) if the other
//!   shard has not answered; the slot stays held for the late answer, which
//!   is then dropped, until `SLOT_DEADLINE_MS`, when `expire` takes it back
//!   — so a lost answer costs one client an error, never the slot for good;
//! - slots come in two classes: ordinary requests, split equally among the
//!   targets, and reads that may block, each target's share an equal part
//!   of the room its waiter pool keeps for other shards. A slow or stuck
//!   shard holds only its own share of each, and long polls never hold the
//!   slots ordinary requests need.

const std = @import("std");
const waiter_pool = @import("waiter_pool.zig");

/// How long a slot is held for its answer: long enough for a request's
/// longest blocking read (`MAX_BLOCK_MS`), with 20 s to spare; an answer
/// later than this is presumed lost.
pub const SLOT_DEADLINE_MS: u64 = waiter_pool.MAX_BLOCK_MS + 20_000;

/// When a client whose request another shard has not answered is told so:
/// 3 s from when the request first waited for room (a blocking read, its
/// block time later), so that with that wait it is under the SDKs' 5 s
/// request timeout. Requests go to other shards only on a node of several,
/// and a cluster member runs one: no wait for a leader sits in between. A
/// request that takes items waits for its answer while the slot is held
/// (`Shard.forwardToShard`).
pub const DEADLINE_MS: u64 = 3_000;

/// The least a request is given once sent, however long it waited for room.
pub const MIN_DEADLINE_MS: u64 = 1_000;

pub const Class = enum(u1) {
    ordinary,
    /// A read that may wait on purpose (up to its `block_ms`): its own share,
    /// and not counted as a slow answer.
    blocking,
};

pub const ReplyPool = struct {
    slots: []Slot,
    classes: [2]ClassPool,

    const ClassPool = struct {
        /// Indices of this class's free slots (a stack).
        free: []u16,
        free_len: usize,
        /// Slots of this class taken, per target shard.
        in_use: []u16,
        /// The most slots of this class one target may hold.
        share: u16,
    };

    pub const Slot = struct {
        active: bool = false,
        gen: u32 = 0,
        /// Its own place in `slots`.
        index: u16 = 0,
        class: Class = .ordinary,
        /// The shard the request went to: only its answer frees the slot.
        target: u16 = 0,
        /// The client connection the answer is for, on this shard.
        fd: i32 = -1,
        conn_id: u32 = 0,
        request_id: u64 = 0,
        /// A write: unanswered, it may still apply.
        writes: bool = false,
        taken_ms: u64 = 0,
        /// How long its client waits for the answer, from when the request
        /// first waited, and when it is told there is none (`take`).
        answer_after_ms: u64 = 0,
        answer_due_ms: u64 = 0,
        /// Its client has been told; the answer, if it comes, is dropped.
        answered: bool = false,
        /// When the slot is taken back: `taken_ms + SLOT_DEADLINE_MS`.
        due_ms: u64 = 0,
    };

    pub const Ticket = struct { slot: u16, gen: u32 };

    /// The target holds its full share of the class's slots.
    pub const TargetBusy = error{TargetBusy};

    /// Ordinary slots for a shard of a `shard_count`-shard node: 64 per other
    /// shard (the most one connection may have in flight), at least 1024 and
    /// at most 8192.
    pub fn slotsFor(shard_count: u16) u16 {
        const others: u32 = @max(1, @as(u32, shard_count) -| 1);
        return @intCast(std.math.clamp(others * 64, 1024, 8192));
    }

    /// The room a target's waiter pool keeps for other shards' blocking
    /// reads; more blocking slots could never be held at once.
    const FORWARDED_WAITERS: u16 = waiter_pool.MAX_WAITERS - waiter_pool.LOCAL_WAITERS;

    /// The most slots of a class one target may hold: an equal part of
    /// this shard's, at most its inbox (1024 messages). Blocking reads park
    /// in the target's waiter pool, so a blocking share is an equal part of
    /// the room it keeps for other shards: a full pool answers a blocking
    /// read empty at once, and its consumer would spin.
    fn shareFor(class: Class, count: u16, shard_count: u16) u16 {
        const others: u16 = @max(1, shard_count -| 1);
        return switch (class) {
            .ordinary => @min(1024, @max(1, count / others)),
            .blocking => @min(count, @max(1, FORWARDED_WAITERS / others)),
        };
    }

    pub fn init(allocator: std.mem.Allocator, ordinary: u16, shard_count: u16) !ReplyPool {
        const counts = [2]u16{ ordinary, @min(ordinary, FORWARDED_WAITERS) };
        const slots = try allocator.alloc(Slot, @as(usize, counts[0]) + counts[1]);
        errdefer allocator.free(slots);
        @memset(slots, .{});
        var classes: [2]ClassPool = undefined;
        var made: usize = 0;
        errdefer for (classes[0..made]) |c| {
            allocator.free(c.free);
            allocator.free(c.in_use);
        };
        for (&classes, counts, 0..) |*c, count, k| {
            const class: Class = @enumFromInt(k);
            const free = try allocator.alloc(u16, count);
            errdefer allocator.free(free);
            // Ordinary slots come first, blocking ones follow.
            const first: usize = if (k == 0) 0 else counts[0];
            for (free, 0..) |*f, i| f.* = @intCast(first + count - 1 - i);
            const in_use = try allocator.alloc(u16, @max(1, shard_count));
            @memset(in_use, 0);
            c.* = .{ .free = free, .free_len = count, .in_use = in_use, .share = shareFor(class, count, shard_count) };
            made += 1;
        }
        return .{ .slots = slots, .classes = classes };
    }

    pub fn deinit(self: *ReplyPool, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
        for (self.classes) |c| {
            allocator.free(c.free);
            allocator.free(c.in_use);
        }
    }

    /// What this shard's reply ring must hold: an answer per slot, and one
    /// more per slot for answers arriving after their slot expired and was
    /// taken again.
    pub fn ringCapacity(self: *const ReplyPool) usize {
        return 2 * self.slots.len;
    }

    /// Whether a request of `class` to `target` would get a slot now.
    pub fn canTake(self: *const ReplyPool, class: Class, target: u16) bool {
        const c = &self.classes[@intFromEnum(class)];
        return c.in_use[target] < c.share and c.free_len > 0;
    }

    /// A slot of `class` for a client's request to `target`, whose client
    /// is told there is no answer `allowance_ms` after `since_ms` (when it
    /// first waited) but no sooner than `MIN_DEADLINE_MS` after `now_ms` —
    /// and, for a blocking read, `block_ms` later still: the read waits that
    /// long on the target once it gets there.
    pub fn take(self: *ReplyPool, class: Class, target: u16, fd: i32, conn_id: u32, request_id: u64, writes: bool, since_ms: u64, now_ms: u64, allowance_ms: u64, block_ms: u64) TargetBusy!Ticket {
        if (!self.canTake(class, target)) return error.TargetBusy;
        const c = &self.classes[@intFromEnum(class)];
        c.free_len -= 1;
        const i = c.free[c.free_len];
        const slot = &self.slots[i];
        slot.* = .{ .active = true, .gen = slot.gen +% 1, .index = i, .class = class, .target = target, .fd = fd, .conn_id = conn_id, .request_id = request_id, .writes = writes, .taken_ms = now_ms, .answer_after_ms = @min(allowance_ms + block_ms, SLOT_DEADLINE_MS), .answer_due_ms = @min(@max(since_ms + allowance_ms, now_ms + MIN_DEADLINE_MS) + block_ms, now_ms + SLOT_DEADLINE_MS), .due_ms = now_ms + SLOT_DEADLINE_MS };
        c.in_use[target] += 1;
        return .{ .slot = i, .gen = slot.gen };
    }

    /// Free the slot an answer from shard `from` names, and say whom the
    /// answer is for; null for an answer that is not the slot's current
    /// one, or not from the shard it was asked of.
    pub fn release(self: *ReplyPool, ticket: Ticket, from: u16) ?Slot {
        if (ticket.slot >= self.slots.len) return null;
        const slot = &self.slots[ticket.slot];
        if (!slot.active or slot.gen != ticket.gen or slot.target != from) return null;
        slot.active = false;
        const c = &self.classes[@intFromEnum(slot.class)];
        c.in_use[slot.target] -= 1;
        c.free[c.free_len] = ticket.slot;
        c.free_len += 1;
        return slot.*;
    }

    /// In one pass, hand every slot past its answer deadline whose client
    /// has not been told to `on_late.late(slot)`, to answer it; and take
    /// back every slot past its due time (a late answer is then dropped by
    /// its generation). Returns when the oldest ordinary slot still held
    /// was taken, or null when none is.
    pub fn expire(self: *ReplyPool, now_ms: u64, on_late: anytype) ?u64 {
        if (self.taken() == 0) return null;
        var oldest: ?u64 = null;
        for (self.slots, 0..) |*slot, i| {
            if (!slot.active) continue;
            if (!slot.answered and now_ms >= slot.answer_due_ms) {
                slot.answered = true;
                on_late.late(slot.*);
            }
            if (now_ms >= slot.due_ms) {
                _ = self.release(.{ .slot = @intCast(i), .gen = slot.gen }, slot.target).?;
                continue;
            }
            if (slot.class == .ordinary) oldest = @min(oldest orelse slot.taken_ms, slot.taken_ms);
        }
        return oldest;
    }

    /// The target of the oldest slot held for connection (`fd`, `conn_id`)
    /// whose client is still waiting.
    pub fn oldestFor(self: *const ReplyPool, fd: i32, conn_id: u32) ?u16 {
        var oldest: ?*const Slot = null;
        for (self.slots) |*slot| {
            if (!slot.active or slot.answered or slot.fd != fd or slot.conn_id != conn_id) continue;
            if (oldest == null or slot.taken_ms < oldest.?.taken_ms) oldest = slot;
        }
        return if (oldest) |s| s.target else null;
    }

    /// Its client has gone: nobody is told at its deadline, and its answer,
    /// when it comes, frees it and reaches no one. Returns the target, or
    /// null for a ticket no longer current.
    pub fn orphan(self: *ReplyPool, ticket: Ticket) ?u16 {
        if (ticket.slot >= self.slots.len) return null;
        const slot = &self.slots[ticket.slot];
        if (!slot.active or slot.gen != ticket.gen) return null;
        slot.answered = true;
        return slot.target;
    }

    /// Slots of `class` `target` could still take.
    pub fn room(self: *const ReplyPool, class: Class, target: u16) i32 {
        const c = &self.classes[@intFromEnum(class)];
        return @intCast(@min(c.share -| c.in_use[target], c.free_len));
    }

    /// Slots taken, of both classes.
    pub fn taken(self: *const ReplyPool) usize {
        var n: usize = 0;
        for (self.classes) |c| n += c.free.len - c.free_len;
        return n;
    }

    /// Slots of `class` held toward `target`.
    pub fn heldBy(self: *const ReplyPool, class: Class, target: u16) u16 {
        return self.classes[@intFromEnum(class)].in_use[target];
    }

    pub fn share(self: *const ReplyPool, class: Class) u16 {
        return self.classes[@intFromEnum(class)].share;
    }
};

const Collect = struct {
    got: std.ArrayListUnmanaged(u64) = .empty,
    pub fn late(self: *Collect, slot: ReplyPool.Slot) void {
        self.got.append(std.testing.allocator, slot.request_id) catch unreachable;
    }
};

/// A slot for request `id`, taken at `now`, its client answered after 3 s.
fn takeAt(pool: *ReplyPool, class: Class, target: u16, id: u64, now: u64) !ReplyPool.Ticket {
    return pool.take(class, target, 5, 1, id, false, now, now, DEADLINE_MS, 0);
}

test "ReplyPool: an answer frees its slot once, only from the shard asked, and a late or repeated one is dropped" {
    var pool = try ReplyPool.init(std.testing.allocator, 4, 3);
    defer pool.deinit(std.testing.allocator);
    const t = try pool.take(.ordinary, 1, 9, 3, 77, true, 0, 0, DEADLINE_MS, 0);
    try std.testing.expect(pool.release(t, 2) == null);
    const s = pool.release(t, 1).?;
    try std.testing.expectEqual(@as(i32, 9), s.fd);
    try std.testing.expectEqual(@as(u32, 3), s.conn_id);
    try std.testing.expectEqual(@as(u64, 77), s.request_id);
    try std.testing.expect(s.writes);
    try std.testing.expect(pool.release(t, 1) == null);
    // The slot is reused under a new generation: the old ticket still misses.
    const again = try takeAt(&pool, .ordinary, 1, 78, 0);
    try std.testing.expectEqual(t.slot, again.slot);
    try std.testing.expect(again.gen != t.gen);
    try std.testing.expect(pool.release(t, 1) == null);
    try std.testing.expect(pool.release(again, 1) != null);
}

test "ReplyPool: each target has its share of each class, so a busy target leaves the others their slots, and blocking reads leave ordinary ones theirs" {
    var pool = try ReplyPool.init(std.testing.allocator, 8, 5); // 4 targets
    defer pool.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 2), pool.share(.ordinary));
    _ = try takeAt(&pool, .ordinary, 1, 1, 0);
    _ = try takeAt(&pool, .ordinary, 1, 2, 0);
    try std.testing.expect(!pool.canTake(.ordinary, 1));
    // Room is the share left, though the pool has free slots.
    try std.testing.expectEqual(@as(i32, 0), pool.room(.ordinary, 1));
    try std.testing.expectEqual(@as(i32, 2), pool.room(.ordinary, 2));
    try std.testing.expectError(error.TargetBusy, takeAt(&pool, .ordinary, 1, 3, 0));
    // Another target, and the other class toward the same one, still go.
    _ = try takeAt(&pool, .ordinary, 2, 4, 0);
    _ = try takeAt(&pool, .blocking, 1, 5, 0);
    try std.testing.expectEqual(@as(u16, 2), pool.heldBy(.ordinary, 1));
    try std.testing.expectEqual(@as(u16, 1), pool.heldBy(.blocking, 1));
    try std.testing.expectEqual(@as(usize, 4), pool.taken());
}

test "ReplyPool: a client is told at its deadline, once; its slot is held for the late answer until the slot's own deadline; the oldest ordinary wait is reported" {
    var pool = try ReplyPool.init(std.testing.allocator, 4, 3);
    defer pool.deinit(std.testing.allocator);
    const a = try takeAt(&pool, .ordinary, 1, 1, 1_000);
    _ = try takeAt(&pool, .ordinary, 2, 2, 2_000);
    // A blocking read waiting its full `block_ms` is not a slow answer:
    // older than the rest, but not the oldest wait reported.
    _ = try pool.take(.blocking, 2, 8, 1, 4, false, 500, 500, DEADLINE_MS, 60_000);
    _ = try takeAt(&pool, .ordinary, 1, 3, 9_000);
    var c = Collect{};
    defer c.got.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(?u64, 1_000), pool.expire(3_999, &c));
    try std.testing.expectEqual(@as(usize, 0), c.got.items.len);
    // Past the first two's deadlines: their clients are told, once, and
    // the slots stay held.
    try std.testing.expectEqual(@as(?u64, 1_000), pool.expire(5_000, &c));
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, c.got.items);
    _ = pool.expire(6_000, &c);
    try std.testing.expectEqual(@as(usize, 2), c.got.items.len);
    try std.testing.expectEqual(@as(usize, 4), pool.taken());
    // The late answer frees the slot, marked answered: the caller drops it.
    try std.testing.expect(pool.release(a, 1).?.answered);
    // Past the slots' own deadline they come back; the others' clients are
    // told as their deadlines pass, and none a second time.
    try std.testing.expectEqual(@as(?u64, 9_000), pool.expire(2_000 + SLOT_DEADLINE_MS, &c));
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3, 4 }, c.got.items);
    try std.testing.expectEqual(@as(usize, 1), pool.taken());
    try std.testing.expectEqual(@as(u16, 1), pool.heldBy(.ordinary, 1));
}

test "ReplyPool: 64 ordinary slots per other shard, within bounds; a blocking share is an equal part of the waiter room a target keeps for other shards" {
    try std.testing.expectEqual(@as(u16, 1024), ReplyPool.slotsFor(2));
    try std.testing.expectEqual(@as(u16, 1024), ReplyPool.slotsFor(8));
    try std.testing.expectEqual(@as(u16, 4032), ReplyPool.slotsFor(64));
    try std.testing.expectEqual(@as(u16, 8192), ReplyPool.slotsFor(256));
    var two = try ReplyPool.init(std.testing.allocator, ReplyPool.slotsFor(2), 2);
    defer two.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 1024), two.share(.ordinary));
    try std.testing.expectEqual(@as(u16, 1024), two.share(.blocking));
    var eight = try ReplyPool.init(std.testing.allocator, ReplyPool.slotsFor(8), 8);
    defer eight.deinit(std.testing.allocator);
    try std.testing.expectEqual((waiter_pool.MAX_WAITERS - waiter_pool.LOCAL_WAITERS) / 7, eight.share(.blocking));
    var many = try ReplyPool.init(std.testing.allocator, ReplyPool.slotsFor(256), 256);
    defer many.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 32), many.share(.ordinary));
    try std.testing.expectEqual(@as(u16, 12), many.share(.blocking));
    // Blocking slots: no more than the room a target keeps for other shards.
    try std.testing.expectEqual(@as(usize, 8192 + 3072), many.slots.len);
}

test "ReplyPool: a deadline counts from the first wait, never under a second from sending, and a blocking read's wait comes on top" {
    var pool = try ReplyPool.init(std.testing.allocator, 8, 3);
    defer pool.deinit(std.testing.allocator);
    // Waited 1.5 s: what is left of the 3 s.
    const a = try pool.take(.ordinary, 1, 5, 1, 1, false, 10_000, 11_500, DEADLINE_MS, 0);
    try std.testing.expectEqual(@as(u64, 13_000), pool.slots[a.slot].answer_due_ms);
    // Waited 2.9 s: the floor.
    const b = try pool.take(.ordinary, 1, 5, 1, 2, false, 10_000, 12_900, DEADLINE_MS, 0);
    try std.testing.expectEqual(@as(u64, 13_900), pool.slots[b.slot].answer_due_ms);
    // A blocking read that waited 2.9 s still has its whole block time.
    const c = try pool.take(.blocking, 1, 5, 1, 3, false, 10_000, 12_900, DEADLINE_MS, 5_000);
    try std.testing.expectEqual(@as(u64, 18_900), pool.slots[c.slot].answer_due_ms);
    try std.testing.expectEqual(DEADLINE_MS + 5_000, pool.slots[c.slot].answer_after_ms);
}
