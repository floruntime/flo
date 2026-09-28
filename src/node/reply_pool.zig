//! ReplyPool — room for the answers to what this shard's clients asked of
//! other shards.
//!
//! A client request forwarded to another shard takes a slot here first; its
//! answer comes back on this shard's reply ring naming the slot and the
//! slot's generation, and frees it. So:
//!
//! - the reply ring has room for every answer owed, and as many again for
//!   late answers to slots already expired (`ringCapacity`);
//! - an answer is delivered once: a second, or one for a slot reused since,
//!   finds another generation and is dropped;
//! - a slot always comes back: one unanswered past `SLOT_DEADLINE_MS` is
//!   taken back by `expire`, and its client told, so a lost answer costs
//!   one client an error, never the slot for good;
//! - one slow or stuck shard holds at most `PER_TARGET` slots, so requests
//!   to the others still go.

const std = @import("std");
const waiter_pool = @import("waiter_pool.zig");

/// Long enough for a request's longest blocking read, plus a write's two
/// holds on its way (for a leader, then for this node to catch up to it,
/// each up to 5 s), plus slack; a request unanswered past it is presumed
/// lost.
pub const SLOT_DEADLINE_MS: u64 = waiter_pool.MAX_BLOCK_MS + 2 * 5_000 + 10_000;

/// The most slots one target may hold: its inbox (1024 messages) full of
/// this shard's requests plus its waiter pool full of this shard's blocking
/// reads.
pub const PER_TARGET: u16 = 1024 + waiter_pool.MAX_WAITERS;

pub const ReplyPool = struct {
    slots: []Slot,
    /// Indices of free slots (a stack).
    free: []u16,
    free_len: usize,
    /// Slots taken, per target shard.
    in_use: []u16,

    pub const Slot = struct {
        active: bool = false,
        gen: u32 = 0,
        /// The shard the request went to: only its answer frees the slot.
        target: u16 = 0,
        /// The client connection the answer is for, on this shard.
        fd: i32 = -1,
        conn_id: u32 = 0,
        request_id: u64 = 0,
        taken_ms: u64 = 0,
        /// When it is given up on: `taken_ms + SLOT_DEADLINE_MS`.
        due_ms: u64 = 0,
        /// A read that may wait on purpose (up to its `block_ms`): not
        /// counted as a slow answer.
        blocking: bool = false,
    };

    pub const Ticket = struct { slot: u16, gen: u32 };

    pub const Refusal = error{
        /// The target already holds `PER_TARGET` slots.
        TargetBusy,
        /// Every slot is taken.
        PoolFull,
    };

    /// Slots for a shard of a `shard_count`-shard node: one target's worth,
    /// plus room for every other shard's waiter pool to be full of this
    /// shard's clients' blocking reads, up to 16 384.
    pub fn slotsFor(shard_count: u16) u16 {
        const others: u32 = @max(1, @as(u32, shard_count) -| 1);
        return @intCast(@min(PER_TARGET + others * waiter_pool.MAX_WAITERS, 16_384));
    }

    pub fn init(allocator: std.mem.Allocator, slot_count: u16, shard_count: u16) !ReplyPool {
        const slots = try allocator.alloc(Slot, slot_count);
        errdefer allocator.free(slots);
        @memset(slots, .{});
        const free = try allocator.alloc(u16, slot_count);
        errdefer allocator.free(free);
        for (free, 0..) |*f, i| f.* = @intCast(slot_count - 1 - i);
        const in_use = try allocator.alloc(u16, @max(1, shard_count));
        @memset(in_use, 0);
        return .{ .slots = slots, .free = free, .free_len = slot_count, .in_use = in_use };
    }

    pub fn deinit(self: *ReplyPool, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
        allocator.free(self.free);
        allocator.free(self.in_use);
    }

    /// What this shard's reply ring must hold: an answer per slot, and one
    /// more per slot for answers arriving after their slot expired and was
    /// taken again.
    pub fn ringCapacity(self: *const ReplyPool) usize {
        return 2 * self.slots.len;
    }

    /// A slot for a client's request to `target`.
    pub fn take(self: *ReplyPool, target: u16, fd: i32, conn_id: u32, request_id: u64, now_ms: u64, blocking: bool) Refusal!Ticket {
        if (self.in_use[target] >= PER_TARGET) return error.TargetBusy;
        if (self.free_len == 0) return error.PoolFull;
        self.free_len -= 1;
        const i = self.free[self.free_len];
        const slot = &self.slots[i];
        slot.* = .{ .active = true, .gen = slot.gen +% 1, .target = target, .fd = fd, .conn_id = conn_id, .request_id = request_id, .taken_ms = now_ms, .due_ms = now_ms + SLOT_DEADLINE_MS, .blocking = blocking };
        self.in_use[target] += 1;
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
        self.in_use[slot.target] -= 1;
        self.free[self.free_len] = ticket.slot;
        self.free_len += 1;
        return slot.*;
    }

    /// In one pass, take back every slot unanswered since before `now_ms -
    /// SLOT_DEADLINE_MS`, handing each to `on_expired.expired(slot)` to
    /// answer its client (a late answer is then dropped by its generation).
    /// Returns when the oldest slot still held that is not a blocking read
    /// was taken, or null when none is.
    pub fn expire(self: *ReplyPool, now_ms: u64, on_expired: anytype) ?u64 {
        if (self.free_len == self.slots.len) return null;
        var oldest: ?u64 = null;
        for (self.slots, 0..) |*slot, i| {
            if (!slot.active) continue;
            if (now_ms >= slot.due_ms) {
                const gone = self.release(.{ .slot = @intCast(i), .gen = slot.gen }, slot.target).?;
                on_expired.expired(gone);
                continue;
            }
            if (!slot.blocking) oldest = @min(oldest orelse slot.taken_ms, slot.taken_ms);
        }
        return oldest;
    }

    /// Slots taken.
    pub fn taken(self: *const ReplyPool) usize {
        return self.slots.len - self.free_len;
    }

    /// Slots held toward `target`.
    pub fn heldBy(self: *const ReplyPool, target: u16) u16 {
        return self.in_use[target];
    }
};

const Collect = struct {
    got: std.ArrayListUnmanaged(u64) = .empty,
    pub fn expired(self: *Collect, slot: ReplyPool.Slot) void {
        self.got.append(std.testing.allocator, slot.request_id) catch unreachable;
    }
};

test "ReplyPool: an answer frees its slot once, only from the shard asked, and a late or repeated one is dropped" {
    var pool = try ReplyPool.init(std.testing.allocator, 4, 3);
    defer pool.deinit(std.testing.allocator);
    const t = try pool.take(1, 9, 3, 77, 0, false);
    try std.testing.expect(pool.release(t, 2) == null);
    const s = pool.release(t, 1).?;
    try std.testing.expectEqual(@as(i32, 9), s.fd);
    try std.testing.expectEqual(@as(u32, 3), s.conn_id);
    try std.testing.expectEqual(@as(u64, 77), s.request_id);
    try std.testing.expect(pool.release(t, 1) == null);
    // The slot is reused under a new generation: the old ticket still misses.
    const again = try pool.take(1, 9, 3, 78, 0, false);
    try std.testing.expectEqual(t.slot, again.slot);
    try std.testing.expect(again.gen != t.gen);
    try std.testing.expect(pool.release(t, 1) == null);
    try std.testing.expect(pool.release(again, 1) != null);
}

test "ReplyPool: one target holds at most its share, so the others still have slots; a full pool refuses" {
    var pool = try ReplyPool.init(std.testing.allocator, PER_TARGET + 2, 3);
    defer pool.deinit(std.testing.allocator);
    for (0..PER_TARGET) |_| _ = try pool.take(1, 5, 1, 1, 0, false);
    try std.testing.expectError(error.TargetBusy, pool.take(1, 5, 1, 1, 0, false));
    try std.testing.expectEqual(PER_TARGET, pool.heldBy(1));
    _ = try pool.take(2, 5, 1, 1, 0, false);
    _ = try pool.take(2, 5, 1, 1, 0, false);
    try std.testing.expectError(error.PoolFull, pool.take(2, 5, 1, 1, 0, false));
}

test "ReplyPool: slots past their deadline come back in one pass, the oldest left is reported, and their late answers are dropped" {
    var pool = try ReplyPool.init(std.testing.allocator, 4, 3);
    defer pool.deinit(std.testing.allocator);
    const a = try pool.take(1, 5, 1, 1, 1_000, false);
    _ = try pool.take(2, 6, 1, 2, 2_000, false);
    // A blocking read waiting its full `block_ms` is not a slow answer:
    // older than the rest, but not the oldest wait reported.
    _ = try pool.take(2, 8, 1, 4, 500, true);
    _ = try pool.take(1, 7, 1, 3, 9_000, false);
    var c = Collect{};
    defer c.got.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(?u64, 1_000), pool.expire(5_000, &c));
    try std.testing.expectEqual(@as(usize, 0), c.got.items.len);
    try std.testing.expectEqual(@as(?u64, 9_000), pool.expire(2_000 + SLOT_DEADLINE_MS, &c));
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 4 }, c.got.items);
    try std.testing.expectEqual(@as(usize, 1), pool.taken());
    try std.testing.expectEqual(@as(u16, 1), pool.heldBy(1));
    try std.testing.expect(pool.release(a, 1) == null);
}

test "ReplyPool: a shard's slots cover a full target and every other shard's waiters, within a ceiling" {
    try std.testing.expectEqual(@as(u16, PER_TARGET + 256), ReplyPool.slotsFor(1));
    try std.testing.expectEqual(@as(u16, PER_TARGET + 7 * 256), ReplyPool.slotsFor(8));
    try std.testing.expectEqual(@as(u16, 16_384), ReplyPool.slotsFor(256));
}
