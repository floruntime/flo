//! A shard's applied state: one ordered map per family, keys in byte
//! order, each value held as its family encodes it. Only a `View`'s commit
//! changes it, so every replica that applied the same entries holds the
//! same bytes in the same order, and `encode` writes them out as a
//! snapshot does.

const std = @import("std");
const builtin = @import("builtin");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;

pub const AppliedMessage = struct {
    entry: @import("../storage/ual/entry.zig").Entry,
    result: @import("../storage/persistence.zig").Result,
    /// Its place in the inbound.
    ordinal: u8,
    /// The group that sent it, and its seq there.
    from: u16 = 0,
    seq: u64 = 0,
    /// Refused because its sender built it wrong, not because its target
    /// said no: counted and logged apart from ordinary refusals.
    sender_bug: ?SenderBug = null,
    /// A repair skipped it: its applier never ran.
    skipped: bool = false,
};

/// The repairs skipping an inbound being applied: the messages they named
/// one at a time (`seqs`), or, `whole`, all it holds past the receiver's
/// mark. `index` is the halted entry's and `halt_seq` the message a whole
/// skip's group halted at (0: none known), for what the answers say.
pub const InboundSkip = struct {
    index: u64,
    whole: bool = false,
    halt_seq: u64 = 0,
    n: u8 = 0,
    seqs: [MAX_SEQS]u64 = @splat(0),

    /// An inbound carries at most this many messages (`channel_entries.MAX_BATCH`).
    pub const MAX_SEQS = 64;

    pub fn all(index: u64) InboundSkip {
        return .{ .index = index, .whole = true };
    }

    pub fn one(index: u64, seq: u64) InboundSkip {
        var s: InboundSkip = .{ .index = index, .n = 1 };
        s.seqs[0] = seq;
        return s;
    }

    /// Whether message `seq` is skipped.
    pub fn skips(self: *const InboundSkip, seq: u64) bool {
        if (self.whole) return true;
        return std.mem.indexOfScalar(u64, self.seqs[0..self.n], seq) != null;
    }
};

/// The inbound message being applied, so a halt in its applier can name it.
pub const Applying = struct { from: u16, seq: u64 };

/// Why a message was refused as its sender's bug.
pub const SenderBug = enum(u8) {
    /// For a slot this group doesn't own.
    wrong_slot,
    /// A payload its machine says can't apply (`Machine.malformed`), or
    /// naming a namespace no namespace could have (`Machine.namespace`).
    bad_payload,
    /// Sent with no origin kind or for no namespace, for a reserved one by
    /// a user, with a payload naming a namespace other than its origin's,
    /// or touching keys outside its origin's namespace.
    wrong_origin,
};

pub const SENDER_BUGS = std.enums.values(SenderBug).len;

/// Family ids are below this; a family's state is a slot of its own on
/// every shard. Ids come in blocks by owner, so a new family takes a free
/// id in its owner's block: 1–3 tests, 4–8 namespaces and the system, 9
/// and 63 processing, 10s KV, 20s actions and workflows, 30s queues, 40s
/// groups, 46–49 and 53 streams, 50–52 settings, 54–59 channels, 60–62
/// time series. 64–127 are unclaimed: an owner whose block is full takes
/// the next free ten there (streams first: theirs filled at 53).
pub const MAX_FAMILIES = 128;

/// Which of the families an entry's commit changed the tree of.
pub const FamilySet = std.StaticBitSet(MAX_FAMILIES);

/// What a record adds to its family's encoded size beside its key and
/// value: their two lengths.
pub const RECORD_OVERHEAD = 8;

fn compareKeys(a: []const u8, b: []const u8) std.math.Order {
    return std.mem.order(u8, a, b);
}

pub const Tree = std.Treap([]const u8, compareKeys);

/// A key and its value, both owned by the store. The node's key is the
/// record's key. A slice of either, and a value decoded from it, holds only
/// until the next apply that writes or deletes its key: whoever keeps one
/// past that copies it first.
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
    /// Records unlinked, ever: a node remembered before the last unlink may
    /// be freed.
    unlinks: u64 = 0,
};

/// A key prefix of a family whose keys under it only grow.
const TailKey = struct { fam: u16, len: u8, bytes: [MAX_TAIL_PREFIX]u8 };
pub const MAX_TAIL_PREFIX = 16;

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
    /// Time-series points this node applied late, past each lateness edge
    /// (`ts/machine.zig`), added as each entry commits. A count of applies,
    /// not state: no snapshot carries it, it starts again from zero with
    /// the process, and boot's replay of the log's tail counts those
    /// entries again.
    ts_late_points: [8]u64 = @splat(0),
    /// Where this store's group sits among the node's groups, for an
    /// apply's sends: null where nothing may send.
    place: ?@import("view.zig").Place = null,
    /// The messages the last `channel_inbound` applied, refused ones too,
    /// each as the entry it applied as and its result: the shard's
    /// after-apply steps run for each, as for an entry of its own. The
    /// entries' payloads are the inbound's, so this holds until the next
    /// apply.
    messages: std.ArrayListUnmanaged(AppliedMessage) = .empty,
    /// Set by the shard around an inbound a repair skips (`InboundSkip`).
    inbound_skip: ?InboundSkip = null,
    /// The inbound message whose applier is running, or null.
    applying: ?Applying = null,
    /// Per family, the length of a key prefix under which new keys tend to
    /// come past every key held (a stream's records: an append's first id is
    /// past its stream's last), or 0. Such a family remembers each prefix's
    /// greatest node, and a key past it links beside that node without a
    /// descent. Only speed, never state, because:
    /// - the slot is checked against its neighbours before use: the key must
    ///   be past the remembered node and before the node after it, and in
    ///   a binary tree exactly one of the two has the free side between
    ///   them. That is the slot a descent finds, so every replica builds the
    ///   same tree with or without hints; a key that fails the check descends.
    /// - a remembered node is never freed while held: only `unlink` and
    ///   `clear` free nodes, and any unlink in the family voids all its hints
    ///   (`tails_unlinks`), and `clear` empties them.
    /// - a hint is only remembered by a link and only read by a commit's
    ///   reserve, both on the applying thread, and a reserved slot holds only
    ///   until the family's tree next changes (`View.commit`'s `moved`).
    tail_prefix: [MAX_FAMILIES]u8 = @splat(0),
    /// Each prefix's greatest node, as of `tails_unlinks`: an unlink in its
    /// family may have freed it, so one empties them all.
    tails: std.AutoHashMapUnmanaged(TailKey, *Tree.Node) = .empty,
    tails_unlinks: [MAX_FAMILIES]u64 = @splat(0),

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
        self.messages.deinit(self.allocator);
        self.tails.deinit(self.allocator);
    }

    /// Empty every family, seeded again: a store loaded afresh (from a
    /// snapshot, say) builds the trees a new one would.
    pub fn clear(self: *Store) void {
        self.tails.clearRetainingCapacity();
        self.tails_unlinks = @splat(0);
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

    /// Every family's encoded size, summed: what a snapshot of the store
    /// encodes. A sum of each record's key, value and overhead, so the same
    /// contents give the same total whatever order they were written in.
    pub fn encodedBytes(self: *const Store) u64 {
        var total: u64 = 0;
        for (&self.families) |*f| total += f.encoded_bytes;
        return total;
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

    /// The family's keys starting with `prefix` and before `before`, last
    /// first.
    pub fn rangeBefore(self: *Store, fam: u16, prefix: []const u8, before: []const u8) Range {
        const node = if (self.lowerBound(fam, before)) |n| n.prev() else self.families[fam].tree.getMax();
        return .{ .node = node, .prefix = prefix, .back = true };
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
        self.noteTail(fam, &rec.node);
    }

    /// `link`, at a slot `getEntryFor` found for `rec`'s key with no change
    /// to the family's tree since: no second descent.
    pub fn linkAt(self: *Store, fam: u16, entry: *Tree.Entry, rec: *Record, value: []u8) void {
        const f = &self.families[fam];
        std.debug.assert(entry.node == null and entry.treap == &f.tree);
        entry.key = rec.node.key;
        rec.value = value;
        entry.set(&rec.node);
        f.count += 1;
        f.commits += 1;
        f.encoded_bytes += RECORD_OVERHEAD + rec.node.key.len + value.len;
        self.noteTail(fam, &rec.node);
    }

    /// The slot for `key` in `fam`: its node if held, else where it would
    /// go, valid until the family's tree next changes.
    pub fn entryFor(self: *Store, fam: u16, key: []const u8) Tree.Entry {
        if (self.tailSlot(fam, key)) |slot| return slot;
        return self.families[fam].tree.getEntryFor(key);
    }

    /// `fam`'s keys come in rising order under their first `len` bytes: it
    /// remembers each prefix's greatest node, to link past it without a
    /// descent (`tail_prefix`).
    pub fn setTailPrefix(self: *Store, fam: u16, len: u8) void {
        std.debug.assert(len <= MAX_TAIL_PREFIX);
        self.tail_prefix[fam] = len;
    }

    /// Room to remember `key`'s prefix's greatest node, so linking it can't
    /// fail: what a commit takes before linking.
    pub fn reserveTail(self: *Store, fam: u16, key: []const u8) Allocator.Error!void {
        if (tailKey(self, fam, key) == null) return;
        try self.tails.ensureUnusedCapacity(self.allocator, 1);
    }

    fn tailKey(self: *const Store, fam: u16, key: []const u8) ?TailKey {
        const len = self.tail_prefix[fam];
        if (len == 0 or key.len < len) return null;
        var k: TailKey = .{ .fam = fam, .len = len, .bytes = @splat(0) };
        @memcpy(k.bytes[0..len], key[0..len]);
        return k;
    }

    /// Where `key` goes beside its prefix's greatest node, when it is past
    /// that node and before the node after it: the slot a descent finds,
    /// found without one. Null otherwise.
    fn tailSlot(self: *Store, fam: u16, key: []const u8) ?Tree.Entry {
        const k = tailKey(self, fam, key) orelse return null;
        const f = &self.families[fam];
        if (self.tails_unlinks[fam] != f.unlinks) {
            self.tails.clearRetainingCapacity();
            for (&self.tails_unlinks, &self.families) |*seen, *g| seen.* = g.unlinks;
            return null;
        }
        const tail = self.tails.get(k) orelse return null;
        if (compareKeys(key, tail.key) != .gt) return null;
        const after = tail.next();
        if (after) |n| if (compareKeys(key, n.key) != .lt) return null;
        // In key order `key` sits between the two, so it goes under
        // whichever has the free side: `tail`'s right, or else `after`'s
        // left (the least of `tail`'s right subtree has no left child).
        const parent = if (tail.children[1] == null) tail else after.?;
        return .{ .key = key, .treap = &f.tree, .node = null, .context = .{ .inserted_under = parent } };
    }

    fn noteTail(self: *Store, fam: u16, node: *Tree.Node) void {
        const k = tailKey(self, fam, node.key) orelse return;
        if (self.tails_unlinks[fam] != self.families[fam].unlinks) return;
        if (self.tails.getPtr(k)) |held| {
            if (compareKeys(node.key, held.*.key) == .gt) held.* = node;
        } else if (self.tails.available > 0) {
            self.tails.putAssumeCapacityNoClobber(k, node);
        }
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
        f.unlinks += 1;
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
    /// Last key first (`rangeBefore`).
    back: bool = false,

    pub fn next(self: *Range) ?*Record {
        const node = self.node orelse return null;
        if (!std.mem.startsWith(u8, node.key, self.prefix)) {
            self.node = null;
            return null;
        }
        self.node = if (self.back) node.prev() else node.next();
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

test "store: a hint that isn't its prefix's greatest node gives no slot past the node after it" {
    const fam: u16 = 1;
    const Put = struct {
        fn value(s: *Store) ![]u8 {
            return s.allocator.dupe(u8, "v");
        }
        fn fresh(s: *Store, key: []const u8) !void {
            try s.reserveTail(fam, key);
            const rec = try s.allocRecord(key);
            var slot = s.entryFor(fam, key);
            s.linkAt(fam, &slot, rec, try value(s));
        }
    };
    // The tree's shape follows its keys' priorities: other prefixes' keys
    // first vary it, so in some the hint has no right child and in some it
    // does.
    var shaped: [2]bool = .{ false, false };
    for (0..16) |others| {
        var store = Store.init(std.testing.allocator);
        defer store.deinit();
        store.setTailPrefix(fam, 1);
        var kb: [2]u8 = undefined;
        for (0..others) |i| {
            kb = .{ 'b', @intCast('a' + i) };
            store.link(fam, try store.allocRecord(&kb), try Put.value(&store));
        }
        // "a9" links with no room to remember it, so a lower key linked
        // after it becomes its prefix's hint: "a0", whose next node is "a9".
        store.tails.clearAndFree(store.allocator);
        store.link(fam, try store.allocRecord("a9"), try Put.value(&store));
        try Put.fresh(&store, "a0");
        const hint = store.tails.get(Store.tailKey(&store, fam, "a0").?).?;
        try std.testing.expectEqualStrings("a0", hint.key);
        shaped[@intFromBool(hint.children[1] == null)] = true;
        // Past the hint but past "a9" too: the slot must be a descent's.
        try Put.fresh(&store, "a~");
        var n: usize = 0;
        var it = store.range(fam, "a");
        const want = [_][]const u8{ "a0", "a9", "a~" };
        while (it.next()) |rec| : (n += 1) try std.testing.expectEqualStrings(want[n], rec.key());
        try std.testing.expectEqual(want.len, n);
    }
    try std.testing.expect(shaped[0] and shaped[1]);
}

test "store: the same contents reached by different writes in different orders encode the same size and bytes" {
    const testing = std.testing;
    const Write = struct {
        fn put(store: *Store, fam: u16, key: []const u8, value: []const u8) !void {
            const v = try testing.allocator.dupe(u8, value);
            if (store.find(fam, key)) |n| return store.replace(fam, n, v);
            const rec = try store.allocRecord(key);
            store.link(fam, rec, v);
        }
        fn del(store: *Store, fam: u16, key: []const u8) void {
            store.unlink(fam, store.find(fam, key).?);
        }
    };
    var a = Store.init(testing.allocator);
    defer a.deinit();
    var b = Store.init(testing.allocator);
    defer b.deinit();

    // a: straight to the end state.
    try Write.put(&a, 1, "k1", "one");
    try Write.put(&a, 1, "k2", "two-two");
    try Write.put(&a, 2, "q", "x");
    // b: the other way round, through values and keys that don't last.
    try Write.put(&b, 2, "gone", "a long value that is deleted again");
    try Write.put(&b, 2, "q", "first value of q, replaced");
    try Write.put(&b, 1, "k2", "short");
    try Write.put(&b, 1, "tmp", "t");
    try Write.put(&b, 1, "k1", "one");
    Write.del(&b, 1, "tmp");
    try Write.put(&b, 2, "q", "x");
    Write.del(&b, 2, "gone");
    try Write.put(&b, 1, "k2", "two-two");

    try testing.expectEqual(a.encodedBytes(), b.encodedBytes());
    for ([_]u16{ 1, 2 }) |fam| {
        try testing.expectEqual(a.family(fam).encoded_bytes, b.family(fam).encoded_bytes);
        var wa: std.Io.Writer.Allocating = .init(testing.allocator);
        defer wa.deinit();
        var wb: std.Io.Writer.Allocating = .init(testing.allocator);
        defer wb.deinit();
        try encode(&a, fam, &wa.writer);
        try encode(&b, fam, &wb.writer);
        try testing.expectEqualSlices(u8, wa.written(), wb.written());
    }
}
