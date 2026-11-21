//! Client requests run again when an answer comes back to this shard — a
//! write once shard 0 made its namespace, a read once shard 0 said its
//! namespace is published, a request held at the edge, a run-id request
//! once the shard holding the run is found — that find no room yet on the
//! shard they are bound for. Held here rather than refused: the share of
//! that shard's inbox, or of this shard's reply slots, comes back as it
//! drains, and `Shard.resumeReentries` runs them again in the order they
//! were first held. Their connection reads nothing more meanwhile, so
//! nothing it sends later overtakes them.

const std = @import("std");
const run_mod = @import("run.zig");

/// The most requests one shard holds for room. Past it, a re-run that
/// can't go is refused, not run.
pub const MAX: usize = 256;

pub const Kind = enum {
    /// Run again from the top (`Shard.runRequest`): its route, namespace
    /// and run checks see the state as it is then.
    rerun,
    /// Sent on to the shard found for it (`RunFind`), as it was about to be.
    forward,
};

pub const Entry = struct {
    active: bool = false,
    /// Its place: entries go in this order, and one held again keeps it.
    seq: u64 = 0,
    fd: i32 = -1,
    conn_id: u32 = 0,
    /// The request, serialized; owned.
    bytes: []u8 = &.{},
    /// Where it came from: it is authorized again as from there when it runs.
    origin: run_mod.Origin = .client,
    kind: Kind = .rerun,
    /// Where it waits for room, and in which class of reply slot.
    target: u16 = 0,
    blocking: bool = false,
    /// When its client's request was first read: its wait counts from
    /// then, not from when it was held.
    since_ms: u64 = 0,
    /// Counted in its target's line (`Shard.queued`): it waits for that
    /// shard's room, not its connection's own limits.
    counted: bool = false,
};

pub const Table = struct {
    entries: [MAX]Entry = @splat(.{}),
    count: u32 = 0,
    next_seq: u64 = 0,

    pub fn free(self: *Table) ?*Entry {
        if (self.count == MAX) return null;
        for (&self.entries) |*e| if (!e.active) return e;
        return null;
    }

    /// A new place, after every entry held so far.
    pub fn takeSeq(self: *Table) u64 {
        self.next_seq += 1;
        return self.next_seq;
    }

    /// Fills `e`, a slot from `free`.
    pub fn hold(self: *Table, e: *Entry, value: Entry) void {
        std.debug.assert(!e.active and value.active);
        e.* = value;
        self.count += 1;
    }

    /// Empties `e`; its bytes are the caller's now.
    pub fn take(self: *Table, e: *Entry) []u8 {
        std.debug.assert(e.active);
        const bytes = e.bytes;
        e.* = .{};
        self.count -= 1;
        return bytes;
    }

    /// The held entries, oldest first, into `out`.
    pub fn ordered(self: *Table, out: *[MAX]*Entry) []*Entry {
        var n: usize = 0;
        for (&self.entries) |*e| if (e.active) {
            out[n] = e;
            n += 1;
        };
        std.mem.sortUnstable(*Entry, out[0..n], {}, struct {
            fn lessThan(_: void, a: *Entry, b: *Entry) bool {
                return a.seq < b.seq;
            }
        }.lessThan);
        return out[0..n];
    }
};

test "reentry table: entries come back oldest first, and a full table has no slot" {
    var t: Table = .{};
    var seqs: [3]u64 = undefined;
    for (&seqs) |*s| s.* = t.takeSeq();
    // Held in another order than their places.
    for ([_]usize{ 2, 0, 1 }) |i| t.hold(t.free().?, .{ .active = true, .seq = seqs[i], .fd = @intCast(i) });
    var out: [MAX]*Entry = undefined;
    const got = t.ordered(&out);
    try std.testing.expectEqual(@as(usize, 3), got.len);
    for (got, 0..) |e, i| try std.testing.expectEqual(@as(i32, @intCast(i)), e.fd);
    _ = t.take(got[0]);
    try std.testing.expectEqual(@as(u32, 2), t.count);
    while (t.free()) |e| t.hold(e, .{ .active = true, .seq = t.takeSeq() });
    try std.testing.expectEqual(@as(u32, MAX), t.count);
}
