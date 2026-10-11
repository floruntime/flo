//! A std hash map marks a removed slot as a tombstone and clears tombstones
//! only when it grows. A map whose size holds steady, such as a ring's
//! index or a bounded cache, never grows: tombstones fill every free slot,
//! and each insert and lookup then probes the whole table. Rehashing in
//! place once removals reach half the capacity keeps them bounded, at an
//! amortised O(1) per removal.

const std = @import("std");

pub const Sweep = struct {
    /// Removals since the map was last rehashed.
    removed: u64 = 0,

    /// Note one removal from `map`, and rehash it once enough have built up.
    /// The rehash moves entries: nothing may hold a pointer into the map
    /// across a call.
    pub fn noteRemoved(self: *Sweep, comptime V: type, map: *std.AutoHashMapUnmanaged(u64, V)) void {
        self.removed += 1;
        const capacity = map.capacity();
        if (capacity == 0 or self.removed < capacity / 2) return;
        map.rehash(std.hash_map.AutoContext(u64){});
        self.removed = 0;
    }
};

/// Tombstones in `map`, for tests.
pub fn count(comptime V: type, map: *const std.AutoHashMapUnmanaged(u64, V)) usize {
    const metadata = map.metadata orelse return 0;
    var n: usize = 0;
    for (metadata[0..map.capacity()]) |m| n += @intFromBool(m.isTombstone());
    return n;
}

test "tombstones: a map at a steady size keeps them bounded, and every live key reads back" {
    var map: std.AutoHashMapUnmanaged(u64, u64) = .{};
    defer map.deinit(std.testing.allocator);
    var sweep: Sweep = .{};

    // A window of 1,000 live keys sliding over 200,000: the map never
    // grows past its first size.
    const live = 1000;
    for (0..200_000) |i| {
        try map.ensureUnusedCapacity(std.testing.allocator, 1);
        map.putAssumeCapacity(i, i);
        if (i >= live) {
            _ = map.remove(i - live);
            sweep.noteRemoved(u64, &map);
        }
        try std.testing.expect(count(u64, &map) <= map.capacity() / 2);
    }
    try std.testing.expectEqual(@as(u32, live), map.count());
    for (200_000 - live..200_000) |i| try std.testing.expectEqual(@as(u64, i), map.get(i).?);
}
