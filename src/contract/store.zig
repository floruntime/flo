//! A shard's applied state: one ordered map per family, keys in byte
//! order, each value held as its family encodes it. Only a `View`'s commit
//! changes it, so every replica that applied the same entries holds the
//! same bytes in the same order, and `encode` writes them out as a
//! snapshot does.

const std = @import("std");
const builtin = @import("builtin");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;

/// Family ids are below this; a family's state is a slot of its own.
pub const MAX_FAMILIES = 64;

/// What a record adds to its family's encoded size beside its key and
/// value: their two lengths.
pub const RECORD_OVERHEAD = 8;

fn compareKeys(a: []const u8, b: []const u8) std.math.Order {
    return std.mem.order(u8, a, b);
}

pub const Tree = std.Treap([]const u8, compareKeys);

/// A key and its value, both owned by the store. The node's key is the
/// record's key.
pub const Record = struct {
    node: Tree.Node,
    value: []u8,

    pub fn of(node: *Tree.Node) *Record {
        return @fieldParentPtr("node", node);
    }

    pub fn key(self: *const Record) []const u8 {
        return self.node.key;
    }
};

pub const FamilyState = struct {
    tree: Tree = .{},
    count: u64 = 0,
    /// The family's encoded size: every record's key, value and
    /// `RECORD_OVERHEAD`. Kept by commits, so nothing serializes to know it.
    encoded_bytes: u64 = 0,
    /// Records linked, replaced or unlinked, ever: a reader that published
    /// what the family holds knows from it whether there is more to say.
    commits: u64 = 0,
};

pub const Store = struct {
    allocator: Allocator,
    families: [MAX_FAMILIES]FamilyState = @splat(.{}),
    /// An undeclared key fails the apply rather than being counted. Debug
    /// builds set it, and so must VOPR, which builds ReleaseSafe and shares
    /// this module with the server, when it hosts families.
    strict_keys: bool = builtin.mode == .debug,
    /// Keys an entry touched without declaring them, where `strict_keys`
    /// is off; each power of two is logged.
    undeclared_keys: u64 = 0,

    pub fn init(allocator: Allocator) Store {
        var store: Store = .{ .allocator = allocator };
        store.seed();
        return store;
    }

    /// A treap seeds its priorities from the first node's address unless
    /// seeded first, so its shape would follow the allocator. Seeded, the
    /// shape is a function of the writes alone, the same on every replica
    /// and in every VOPR run from its seed; so is each write's cost.
    fn seed(self: *Store) void {
        for (&self.families, 0..) |*f, i| f.tree.prng.xorshift = PRIORITY_SEED +% i;
    }

    const PRIORITY_SEED: usize = 0x9E37_79B9_7F4A_7C15 & std.math.maxInt(usize);

    pub fn deinit(self: *Store) void {
        self.clear();
    }

    /// Empty every family, seeded again: a store loaded afresh (from a
    /// snapshot, say) builds the trees a new one would.
    pub fn clear(self: *Store) void {
        for (&self.families) |*f| {
            while (f.tree.getMin()) |node| {
                var entry = f.tree.getEntryForExisting(node);
                entry.set(null);
                self.freeRecord(Record.of(node));
            }
            f.* = .{};
        }
        self.seed();
    }

    pub fn get(self: *Store, fam: u16, key: []const u8) ?[]const u8 {
        const node = self.families[fam].tree.getEntryFor(key).node orelse return null;
        return Record.of(node).value;
    }

    pub fn family(self: *const Store, fam: u16) *const FamilyState {
        return &self.families[fam];
    }

    /// The records of `fam` whose keys start with `prefix`, in key order.
    pub fn range(self: *Store, fam: u16, prefix: []const u8) Range {
        return .{ .node = self.lowerBound(fam, prefix), .prefix = prefix };
    }

    /// `range(fam, prefix)` from the first key after `after`.
    pub fn rangeAfter(self: *Store, fam: u16, prefix: []const u8, after: []const u8) Range {
        if (compareKeys(after, prefix) == .lt) return self.range(fam, prefix);
        var node = self.lowerBound(fam, after);
        if (node) |n| if (std.mem.eql(u8, n.key, after)) {
            node = n.next();
        };
        return .{ .node = node, .prefix = prefix };
    }

    /// The first record at or after `key`.
    fn lowerBound(self: *Store, fam: u16, key: []const u8) ?*Tree.Node {
        const entry = self.families[fam].tree.getEntryFor(key);
        if (entry.node) |n| return n;
        const parent = entry.context.inserted_under orelse return null;
        return if (compareKeys(key, parent.key) == .lt) parent else parent.next();
    }

    pub fn allocRecord(self: *Store, key: []const u8) Allocator.Error!*Record {
        const rec = try self.allocator.create(Record);
        errdefer self.allocator.destroy(rec);
        rec.* = .{ .node = undefined, .value = &.{} };
        rec.node.key = try self.allocator.dupe(u8, key);
        return rec;
    }

    pub fn freeRecord(self: *Store, rec: *Record) void {
        self.allocator.free(rec.node.key);
        self.allocator.free(rec.value);
        self.allocator.destroy(rec);
    }

    /// Insert `rec`, whose key isn't in `fam`, with `value`. Can't fail:
    /// both were allocated already.
    pub fn link(self: *Store, fam: u16, rec: *Record, value: []u8) void {
        const f = &self.families[fam];
        var entry = f.tree.getEntryFor(rec.node.key);
        std.debug.assert(entry.node == null);
        rec.value = value;
        entry.set(&rec.node);
        f.count += 1;
        f.commits += 1;
        f.encoded_bytes += RECORD_OVERHEAD + rec.node.key.len + value.len;
    }

    /// Give the record at `node` a new value, freeing the old one.
    pub fn replace(self: *Store, fam: u16, node: *Tree.Node, value: []u8) void {
        const f = &self.families[fam];
        const rec = Record.of(node);
        f.encoded_bytes = f.encoded_bytes - rec.value.len + value.len;
        f.commits += 1;
        self.allocator.free(rec.value);
        rec.value = value;
    }

    pub fn unlink(self: *Store, fam: u16, node: *Tree.Node) void {
        const f = &self.families[fam];
        const rec = Record.of(node);
        var entry = f.tree.getEntryForExisting(node);
        entry.set(null);
        f.count -= 1;
        f.commits += 1;
        f.encoded_bytes -= RECORD_OVERHEAD + rec.node.key.len + rec.value.len;
        self.freeRecord(rec);
    }

    pub fn find(self: *Store, fam: u16, key: []const u8) ?*Tree.Node {
        return self.families[fam].tree.getEntryFor(key).node;
    }
};

pub const Range = struct {
    node: ?*Tree.Node,
    prefix: []const u8,

    pub fn next(self: *Range) ?*Record {
        const node = self.node orelse return null;
        if (!std.mem.startsWith(u8, node.key, self.prefix)) {
            self.node = null;
            return null;
        }
        self.node = node.next();
        return Record.of(node);
    }
};

/// A family's canonical bytes: `[id u16][count u64]`, then each record in
/// key order as `[key len u32][key][value len u32][value]`, little-endian.
/// The one walk both `encode` and `checksum` take, so the two can't differ.
fn walk(store: *Store, fam: u16, sink: anytype) !void {
    var head: [10]u8 = undefined;
    std.mem.writeInt(u16, head[0..2], fam, .little);
    std.mem.writeInt(u64, head[2..10], store.families[fam].count, .little);
    try sink.update(&head);
    var it = store.range(fam, "");
    while (it.next()) |rec| {
        var len: [4]u8 = undefined;
        std.mem.writeInt(u32, &len, @intCast(rec.key().len), .little);
        try sink.update(&len);
        try sink.update(rec.key());
        std.mem.writeInt(u32, &len, @intCast(rec.value.len), .little);
        try sink.update(&len);
        try sink.update(rec.value);
    }
}

/// Write family `fam` canonically to `w`.
pub fn encode(store: *Store, fam: u16, w: *std.Io.Writer) std.Io.Writer.Error!void {
    const Sink = struct {
        w: *std.Io.Writer,
        fn update(self: @This(), bytes: []const u8) std.Io.Writer.Error!void {
            try self.w.writeAll(bytes);
        }
    };
    try walk(store, fam, Sink{ .w = w });
}

/// A hash of family `fam`'s canonical bytes: equal on two replicas exactly
/// when `encode` would write the same bytes.
pub fn checksum(store: *Store, fam: u16) u64 {
    const Sink = struct {
        h: *std.hash.Wyhash,
        fn update(self: @This(), bytes: []const u8) error{}!void {
            self.h.update(bytes);
        }
    };
    var h = std.hash.Wyhash.init(0x666c_6f2d_6661_6d73); // "flo-fams"
    walk(store, fam, Sink{ .h = &h }) catch |err| switch (err) {};
    return h.final();
}
