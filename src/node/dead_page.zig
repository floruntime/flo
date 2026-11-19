//! A dead-letter list's page: the dead keys read ahead first (a read of
//! the store inside their range would end it), then each one's item. One
//! whose item is gone is passed over, so the page counts only what it
//! returns, and its cursor names the last key it looked at.

const std = @import("std");

/// What adding one key's item did.
pub const Take = enum { added, skipped, full };

pub const Walked = struct {
    /// The key the next page starts after: the last one looked at.
    last: ?usize,
    /// Keys remain past `last`.
    more: bool,
};

/// Walk `n` keys read ahead, at most `limit + 1`: the store holds more past
/// them only when that many were read. `take(ctx, i)` adds key `i`'s item.
pub fn walk(n: usize, limit: u32, ctx: anytype, comptime take: fn (@TypeOf(ctx), usize) Take) Walked {
    std.debug.assert(n <= @as(usize, limit) + 1);
    var added: u32 = 0;
    var last: ?usize = null;
    for (0..n) |i| {
        if (added == limit) return .{ .last = last, .more = true };
        switch (take(ctx, i)) {
            .added => added += 1,
            .skipped => {},
            .full => return .{ .last = last, .more = true },
        }
        last = i;
    }
    // Every key read was looked at: more remain if the read stopped at its
    // bound rather than at the end.
    return .{ .last = last, .more = n == @as(usize, limit) + 1 };
}

const testing = std.testing;

const Fake = struct {
    gone: []const usize,
    room: usize = std.math.maxInt(usize),
    added: std.ArrayListUnmanaged(usize) = .empty,

    fn take(self: *Fake, i: usize) Take {
        for (self.gone) |g| if (g == i) return .skipped;
        if (self.added.items.len == self.room) return .full;
        self.added.appendAssumeCapacity(i);
        return .added;
    }
};

fn run(n: usize, limit: u32, gone: []const usize, room: usize) !struct { w: Walked, added: []const usize } {
    const S = struct {
        var buf: [16]usize = undefined;
    };
    var f: Fake = .{ .gone = gone, .room = room, .added = .initBuffer(&S.buf) };
    const w = walk(n, limit, &f, Fake.take);
    return .{ .w = w, .added = f.added.items };
}

test "dead page: a page counts only the items it returns, and names where it stopped" {
    // Three asked, four read: all present, a full page and more.
    var r = try run(4, 3, &.{}, 16);
    try testing.expectEqualSlices(usize, &.{ 0, 1, 2 }, r.added);
    try testing.expectEqual(Walked{ .last = 2, .more = true }, r.w);
    // One gone: the spare key fills the page, and the cursor is past it,
    // with more still to read.
    r = try run(4, 3, &.{1}, 16);
    try testing.expectEqualSlices(usize, &.{ 0, 2, 3 }, r.added);
    try testing.expectEqual(Walked{ .last = 3, .more = true }, r.w);
    // Two gone: a short page, yet more remain past what was read.
    r = try run(4, 3, &.{ 0, 2 }, 16);
    try testing.expectEqualSlices(usize, &.{ 1, 3 }, r.added);
    try testing.expectEqual(Walked{ .last = 3, .more = true }, r.w);
    // Fewer read than the bound: the end, whatever was passed over.
    r = try run(3, 3, &.{2}, 16);
    try testing.expectEqualSlices(usize, &.{ 0, 1 }, r.added);
    try testing.expectEqual(Walked{ .last = 2, .more = false }, r.w);
    // Out of room: stops before the item that didn't fit.
    r = try run(4, 3, &.{}, 1);
    try testing.expectEqualSlices(usize, &.{0}, r.added);
    try testing.expectEqual(Walked{ .last = 0, .more = true }, r.w);
    // Nothing dead.
    r = try run(0, 3, &.{}, 16);
    try testing.expectEqual(Walked{ .last = null, .more = false }, r.w);
}
