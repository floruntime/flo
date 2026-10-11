//! Where each entry in a UAL's ring starts, in index order.
//!
//! A UAL's indexes only go up: an append adds at the back, an eviction takes
//! from the front, a truncation from the back. So a ring of (index, pos)
//! pairs is all a lookup needs. Nothing is hashed, a removal leaves nothing
//! behind, and nothing ever pauses to clear it. A hash map here filled with
//! tombstones at a steady ring size, and clearing them cost a pause of
//! O(table) on the shard thread.
//!
//! A lookup goes straight to `index - first` where the indexes are dense
//! (the Raft log's ring). Where they are not (a partition's ring, which KV
//! entries skip), it interpolates and searches outward from there.

const std = @import("std");

pub const Slot = struct {
    index: u64,
    /// The entry's start, in the ring's virtual byte space.
    pos: u64,
};

pub const Positions = struct {
    /// A power of two in length, or empty.
    slots: []Slot = &.{},
    head: usize = 0,
    len: usize = 0,

    pub fn deinit(self: *Positions, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
        self.* = .{};
    }

    /// Room for `n` more without allocating; on failure nothing changed.
    pub fn ensureUnusedCapacity(self: *Positions, allocator: std.mem.Allocator, n: usize) !void {
        if (self.len + n <= self.slots.len) return;
        const want = std.math.ceilPowerOfTwo(usize, @max(16, self.len + n)) catch return error.OutOfMemory;
        const grown = try allocator.alloc(Slot, want);
        for (grown[0..self.len], 0..) |*s, i| s.* = self.at(i);
        allocator.free(self.slots);
        self.slots = grown;
        self.head = 0;
    }

    /// The `i`th oldest.
    pub fn at(self: *const Positions, i: usize) Slot {
        return self.slots[(self.head + i) & (self.slots.len - 1)];
    }

    pub fn front(self: *const Positions) ?Slot {
        return if (self.len == 0) null else self.at(0);
    }

    pub fn back(self: *const Positions) ?Slot {
        return if (self.len == 0) null else self.at(self.len - 1);
    }

    /// Add the newest; its index must be above every other.
    pub fn pushBackAssumeCapacity(self: *Positions, slot: Slot) void {
        std.debug.assert(self.len < self.slots.len);
        std.debug.assert(if (self.back()) |b| slot.index > b.index else true);
        self.slots[(self.head + self.len) & (self.slots.len - 1)] = slot;
        self.len += 1;
    }

    pub fn popFront(self: *Positions) ?Slot {
        const s = self.front() orelse return null;
        self.head = (self.head + 1) & (self.slots.len - 1);
        self.len -= 1;
        return s;
    }

    pub fn popBack(self: *Positions) ?Slot {
        const s = self.back() orelse return null;
        self.len -= 1;
        return s;
    }

    /// Where `index` starts, if it is here.
    pub fn find(self: *const Positions, index: u64) ?u64 {
        const first = self.front() orelse return null;
        const last = self.back().?;
        if (index < first.index or index > last.index) return null;
        // Where the indexes are dense (the Raft log's ring) it is at
        // `index - first`. Otherwise (a partition's ring, which KV entries
        // skip) interpolate, which lands near it while the gaps are spread
        // out, and search outward from there.
        const span = last.index - first.index;
        const guess: usize = if (span + 1 == self.len)
            @intCast(index - first.index)
        else
            @intCast(@as(u128, index - first.index) * (self.len - 1) / span);
        const at_guess = self.at(guess);
        if (at_guess.index == index) return at_guess.pos;

        // Gallop toward it to bracket it, then binary-search the bracket.
        var lo: usize = undefined;
        var hi: usize = undefined;
        var step: usize = 1;
        if (at_guess.index < index) {
            lo = guess + 1;
            while (true) : (step *= 2) {
                const probe = guess + step;
                if (probe >= self.len - 1 or self.at(probe).index >= index) {
                    hi = @min(probe, self.len - 1) + 1;
                    break;
                }
                lo = probe + 1;
            }
        } else {
            hi = guess;
            while (true) : (step *= 2) {
                if (step > guess or self.at(guess - step).index <= index) {
                    lo = guess -| step;
                    break;
                }
                hi = guess - step;
            }
        }
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.at(mid).index < index) lo = mid + 1 else hi = mid;
        }
        if (lo < self.len and self.at(lo).index == index) return self.at(lo).pos;
        return null;
    }
};

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

fn push(p: *Positions, index: u64) !void {
    try p.ensureUnusedCapacity(testing.allocator, 1);
    p.pushBackAssumeCapacity(.{ .index = index, .pos = index * 10 });
}

test "positions: a dense run is found at its offset, the first and last included" {
    var p: Positions = .{};
    defer p.deinit(testing.allocator);
    for (5..105) |i| try push(&p, i);
    try testing.expectEqual(@as(?u64, 50), p.find(5));
    try testing.expectEqual(@as(?u64, 1040), p.find(104));
    try testing.expectEqual(@as(?u64, 500), p.find(50));
    try testing.expect(p.find(4) == null and p.find(105) == null);
}

test "positions: with gaps, every held index is found and a missing one is not" {
    var p: Positions = .{};
    defer p.deinit(testing.allocator);
    // Held: multiples of 3 plus a few runs; the rest are gaps.
    var held: [400]bool = @splat(false);
    for (1..400) |i| {
        if (i % 3 == 0 or (i > 100 and i < 120) or i == 399) {
            held[i] = true;
            try push(&p, i);
        }
    }
    for (0..400) |i| {
        if (held[i]) {
            try testing.expectEqual(@as(?u64, i * 10), p.find(i));
        } else {
            try testing.expect(p.find(i) == null);
        }
    }
}

test "positions: lookups hold through eviction, truncation and the ring wrapping" {
    var p: Positions = .{};
    defer p.deinit(testing.allocator);
    var next: u64 = 1;
    // Keep ~40 held while pushing thousands, with gaps, so head wraps the
    // slot array many times; truncate now and then.
    for (0..5000) |round| {
        try push(&p, next);
        next += if (round % 7 == 0) 3 else 1;
        if (p.len > 40) _ = p.popFront();
        if (round % 101 == 100) {
            const dropped = p.popBack().?;
            try testing.expect(p.find(dropped.index) == null);
            next = dropped.index;
        }
        const f = p.front().?;
        const b = p.back().?;
        try testing.expectEqual(@as(?u64, f.pos), p.find(f.index));
        try testing.expectEqual(@as(?u64, b.pos), p.find(b.index));
        try testing.expect(p.find(f.index -| 1) == null or f.index == 0);
        for (0..p.len) |i| try testing.expectEqual(@as(?u64, p.at(i).pos), p.find(p.at(i).index));
    }
    try testing.expect(p.slots.len <= 64);
}

test "positions: growing keeps the order, and an allocation that fails changes nothing" {
    var p: Positions = .{};
    defer p.deinit(testing.allocator);
    for (1..17) |i| try push(&p, i);
    _ = p.popFront();
    _ = p.popFront();
    try push(&p, 17);
    try push(&p, 18);
    try testing.expectEqual(@as(usize, 16), p.slots.len);
    try push(&p, 19); // the 17th held: grows past 16 with head not at 0
    try testing.expectEqual(@as(usize, 32), p.slots.len);
    for (3..20) |i| try testing.expectEqual(@as(?u64, i * 10), p.find(i));

    var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 0 });
    while (p.len < p.slots.len) try push(&p, p.back().?.index + 1);
    const full = p;
    try testing.expectError(error.OutOfMemory, p.ensureUnusedCapacity(failing.allocator(), 1));
    try testing.expectEqual(full.slots.ptr, p.slots.ptr);
    try testing.expectEqual(full.len, p.len);
}
