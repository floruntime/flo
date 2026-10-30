//! The node's namespace table: every namespace's name and dense id, as
//! shard 0 published them. Every shard thread reads it; only shard 0's
//! thread writes it, and only once a create is on disk.
//!
//! Shared across shard threads, an exception to a shard's state being its
//! own thread's. It is safe because the table only grows: an entry, once
//! published, never changes or goes. A publish writes the name, then
//! release-stores its length, then release-stores the id into its index
//! slot, then release-stores `published_index`. A reader that
//! acquire-loads an index slot or an id's length sees the whole name
//! behind it, and one that reads `published_index` sees every entry
//! published at or below it. Readers take no lock and never wait.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Atomic = std.atomic.Value;

pub const Id = u32;

/// Never a namespace's: an entry that carries 0 names none.
pub const INVALID: Id = 0;
pub const DEFAULT: Id = 1;
pub const SYSTEM: Id = 2;
/// Ids below this are compile-time constants, never allocated.
pub const FIRST_ALLOCATED: Id = 16;

/// The most live namespaces: tenants and admins alike, until principals
/// split the count.
pub const MAX_LIVE: u32 = 4096;

/// Every id the table can hold: the reserved ones, then one per live
/// namespace. Ids are never reused, and nothing is deleted yet, so this
/// bounds the ids too.
pub const CAPACITY: u32 = FIRST_ALLOCATED + MAX_LIVE;

/// Open-addressed name → id slots, at most half full.
const INDEX_SLOTS: u32 = 2 * std.math.ceilPowerOfTwoAssert(u32, CAPACITY);

pub const Table = struct {
    allocator: Allocator,
    /// Each id's name, owned. Written once, before its length.
    names: [CAPACITY][*]const u8 = undefined,
    /// Each id's name length; 0 until the id is published.
    lens: [CAPACITY]Atomic(u32) = @splat(.init(0)),
    /// Name hash → id; 0 is an empty slot.
    index: [INDEX_SLOTS]Atomic(Id) = @splat(.init(INVALID)),
    /// Shard 0's log index of the last change the table holds. Every entry
    /// at or below it is published.
    published_index: Atomic(u64) = .init(0),

    /// A table holding the reserved names, published before any entry.
    pub fn create(allocator: Allocator) !*Table {
        const t = try allocator.create(Table);
        errdefer allocator.destroy(t);
        t.* = .{ .allocator = allocator };
        errdefer t.freeNames();
        try t.publish(DEFAULT, "default", 0);
        try t.publish(SYSTEM, "_flo", 0);
        return t;
    }

    pub fn destroy(self: *Table) void {
        self.freeNames();
        self.allocator.destroy(self);
    }

    fn freeNames(self: *Table) void {
        for (&self.lens, 0..) |*len, id| {
            const n = len.load(.monotonic);
            if (n > 0) self.allocator.free(self.names[id][0..n]);
        }
    }

    /// Publish `name` as `id`, known at shard 0's log index `index`. Shard
    /// 0's thread only; the id is new and the name not yet published.
    pub fn publish(self: *Table, id: Id, name: []const u8, index: u64) !void {
        std.debug.assert(id != INVALID and id < CAPACITY and name.len > 0);
        std.debug.assert(self.lens[id].load(.monotonic) == 0);
        const owned = try self.allocator.dupe(u8, name);
        self.names[id] = owned.ptr;
        self.lens[id].store(@intCast(owned.len), .release);
        var slot = slotOf(name);
        while (true) : (slot = (slot + 1) & (INDEX_SLOTS - 1)) {
            // One writer: an empty slot stays empty until it writes it.
            if (self.index[slot].load(.monotonic) == INVALID) {
                self.index[slot].store(id, .release);
                break;
            }
        }
        if (index > self.published_index.load(.monotonic)) self.published_index.store(index, .release);
    }

    /// The id published for `name`, or null.
    pub fn lookup(self: *const Table, name: []const u8) ?Id {
        if (name.len == 0) return null;
        var slot = slotOf(name);
        while (true) : (slot = (slot + 1) & (INDEX_SLOTS - 1)) {
            const id = self.index[slot].load(.acquire);
            if (id == INVALID) return null;
            if (std.mem.eql(u8, self.nameOf(id).?, name)) return id;
        }
    }

    /// The name published as `id`, or null.
    pub fn nameOf(self: *const Table, id: Id) ?[]const u8 {
        if (id == INVALID or id >= CAPACITY) return null;
        const n = self.lens[id].load(.acquire);
        if (n == 0) return null;
        return self.names[id][0..n];
    }

    pub fn publishedIndex(self: *const Table) u64 {
        return self.published_index.load(.acquire);
    }

    fn slotOf(name: []const u8) u32 {
        return @truncate(std.hash.Wyhash.hash(0x6e732d74626c, name) & (INDEX_SLOTS - 1));
    }
};

const testing = std.testing;

test "namespace table: default and _flo are published from the start, and nothing else" {
    const t = try Table.create(testing.allocator);
    defer t.destroy();
    try testing.expectEqual(@as(?Id, DEFAULT), t.lookup("default"));
    try testing.expectEqual(@as(?Id, SYSTEM), t.lookup("_flo"));
    try testing.expectEqualStrings("default", t.nameOf(DEFAULT).?);
    try testing.expect(t.lookup("orders") == null);
    try testing.expect(t.nameOf(INVALID) == null);
    try testing.expect(t.nameOf(FIRST_ALLOCATED) == null);
    try testing.expectEqual(@as(u64, 0), t.publishedIndex());
}

test "namespace table: a published name is found by name and by id, and the index moves forward only" {
    const t = try Table.create(testing.allocator);
    defer t.destroy();
    try t.publish(FIRST_ALLOCATED, "orders", 9);
    try testing.expectEqual(@as(?Id, FIRST_ALLOCATED), t.lookup("orders"));
    try testing.expectEqualStrings("orders", t.nameOf(FIRST_ALLOCATED).?);
    try testing.expectEqual(@as(u64, 9), t.publishedIndex());
    // A publish at a lower index leaves it where it was.
    try t.publish(FIRST_ALLOCATED + 1, "ledger", 7);
    try testing.expectEqual(@as(u64, 9), t.publishedIndex());
}

test "namespace table: it holds every id it can, and every name is found" {
    const t = try Table.create(testing.allocator);
    defer t.destroy();
    var buf: [16]u8 = undefined;
    var id: Id = FIRST_ALLOCATED;
    while (id < CAPACITY) : (id += 1) try t.publish(id, try std.fmt.bufPrint(&buf, "ns{d}", .{id}), id);
    id = FIRST_ALLOCATED;
    while (id < CAPACITY) : (id += 1) try testing.expectEqual(@as(?Id, id), t.lookup(try std.fmt.bufPrint(&buf, "ns{d}", .{id})));
    try testing.expect(t.lookup("ns1") == null);
}

test "namespace table: readers racing the publisher see whole names, and everything up to the published index" {
    const Shared = struct {
        t: *Table,
        done: Atomic(bool) = .init(false),
        bad: Atomic(u32) = .init(0),
        checks: Atomic(u64) = .init(0),

        fn name(buf: []u8, id: Id) []const u8 {
            return std.fmt.bufPrint(buf, "name-{d}-{d}", .{ id, id *% 2654435761 }) catch unreachable;
        }

        fn read(s: *@This()) void {
            var buf: [48]u8 = undefined;
            while (!s.done.load(.acquire)) {
                // Past the published index, an id being published right
                // now: its length or its index slot, once seen, must
                // bring the whole name with it.
                const frontier = s.t.publishedIndex() + 1;
                var probe: Id = @intCast(@max(frontier, FIRST_ALLOCATED));
                while (probe < @min(frontier + 4, CAPACITY)) : (probe += 1) {
                    const want = name(&buf, probe);
                    if (s.t.nameOf(probe)) |got| {
                        if (!std.mem.eql(u8, got, want)) _ = s.bad.fetchAdd(1, .monotonic);
                    }
                    if (s.t.lookup(want)) |found| {
                        if (found != probe) _ = s.bad.fetchAdd(1, .monotonic);
                    }
                }
                // Everything at or below the published index is there, whole.
                const upto = s.t.publishedIndex();
                var id: Id = FIRST_ALLOCATED;
                while (id <= upto) : (id += 1) {
                    const want = name(&buf, id);
                    if (s.t.lookup(want) != id) _ = s.bad.fetchAdd(1, .monotonic);
                    const got = s.t.nameOf(id) orelse {
                        _ = s.bad.fetchAdd(1, .monotonic);
                        continue;
                    };
                    if (!std.mem.eql(u8, got, want)) _ = s.bad.fetchAdd(1, .monotonic);
                }
                _ = s.checks.fetchAdd(1, .monotonic);
            }
        }
    };
    // Several fresh tables, each filled while readers race it, so a
    // reordered store has many chances to be seen.
    for (0..16) |_| {
        const t = try Table.create(testing.allocator);
        defer t.destroy();
        var shared: Shared = .{ .t = t };
        var readers: [4]std.Thread = undefined;
        for (&readers) |*r| r.* = try std.Thread.spawn(.{}, Shared.read, .{&shared});
        var buf: [48]u8 = undefined;
        var id: Id = FIRST_ALLOCATED;
        // The id doubles as the index it was published at.
        while (id < CAPACITY) : (id += 1) try t.publish(id, Shared.name(&buf, id), id);
        shared.done.store(true, .release);
        for (readers) |r| r.join();
        try testing.expectEqual(@as(u32, 0), shared.bad.load(.monotonic));
        try testing.expect(shared.checks.load(.monotonic) > 0);
    }
}
