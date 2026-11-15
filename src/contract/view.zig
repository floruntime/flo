//! How an applier reaches state. `View` stages one entry's writes and
//! reads them back over committed state; `commit` makes them state only
//! once the entry applies, and a refusal or a halt discards them, so a
//! refused entry changes nothing on any replica. `ReadView` reads
//! committed state alone.

const std = @import("std");
const store_mod = @import("store.zig");
const ApplyError = @import("../storage/persistence.zig").ApplyError;
const log = @import("stdx").log;

const Allocator = std.mem.Allocator;
const Store = store_mod.Store;
const NamespaceId = @import("../namespace/table.zig").Id;
const Streams = @import("streams.zig").Streams;

/// The longest namespace name a key holds.
pub const MAX_NAMESPACE = std.math.maxInt(u8);

/// A key as a family stores it: `[ns len u8][ns][key]`, so every key of a
/// namespace sorts together and a namespace is a prefix.
pub fn key(buf: []u8, ns: []const u8, k: []const u8) error{NoSpaceLeft}![]u8 {
    std.debug.assert(ns.len <= MAX_NAMESPACE);
    if (buf.len < 1 + ns.len + k.len) return error.NoSpaceLeft;
    buf[0] = @intCast(ns.len);
    @memcpy(buf[1..][0..ns.len], ns);
    @memcpy(buf[1 + ns.len ..][0..k.len], k);
    return buf[0 .. 1 + ns.len + k.len];
}

/// A key as a family keyed by namespace id stores it: the id's fixed-width
/// bytes, then the key, so every key of a namespace sorts together and the
/// id is a prefix no other namespace's key starts with.
pub fn idKey(buf: []u8, ns: NamespaceId, k: []const u8) error{NoSpaceLeft}![]u8 {
    const id = ns.bytes();
    if (buf.len < id.len + k.len) return error.NoSpaceLeft;
    @memcpy(buf[0..id.len], &id);
    @memcpy(buf[id.len..][0..k.len], k);
    return buf[0 .. id.len + k.len];
}

/// A key within its family.
const FamKey = struct {
    fam: u16,
    key: []const u8,

    const Context = struct {
        pub fn hash(_: Context, d: FamKey) u64 {
            var h = std.hash.Wyhash.init(d.fam);
            h.update(d.key);
            return h.final();
        }
        pub fn eql(_: Context, a: FamKey, b: FamKey) bool {
            return a.fam == b.fam and std.mem.eql(u8, a.key, b.key);
        }
    };

    fn Map(comptime V: type) type {
        return std.HashMapUnmanaged(FamKey, V, Context, std.hash_map.default_max_load_percentage);
    }
};

/// Past this many keys a lookup hashes rather than scans. Below it a scan
/// is as fast or faster (measured: a KV put, batches up to 64 ops, an
/// expire of up to about 30 leases); above it a scan per access grows with
/// the square of the entry, as a full KV transaction (256 ops) or a large
/// expire does.
const SCAN_LIMIT = 128;

/// Every key an entry may touch, from the entry alone: whole keys, and
/// ranges by prefix. A range names its namespace, so no entry declares a
/// whole family or every namespace. After `add` fails the set is partial,
/// its index perhaps behind its list: the caller drops the entry.
pub const KeySet = struct {
    allocator: Allocator,
    exact: std.ArrayListUnmanaged(FamKey) = .empty,
    /// `exact` hashed, once it holds more than `SCAN_LIMIT`.
    exact_index: FamKey.Map(void) = .empty,
    /// Scanned: an entry declares few ranges.
    ranges: std.ArrayListUnmanaged(FamKey) = .empty,

    pub fn init(allocator: Allocator) KeySet {
        return .{ .allocator = allocator };
    }

    pub fn add(self: *KeySet, fam: u16, ns: []const u8, k: []const u8) Allocator.Error!void {
        const bytes = try self.allocator.alloc(u8, 1 + ns.len + k.len);
        _ = key(bytes, ns, k) catch unreachable;
        try self.addKey(fam, bytes);
    }

    /// `add` for a family keyed by namespace id (`idKey`).
    pub fn addIn(self: *KeySet, fam: u16, ns: NamespaceId, k: []const u8) Allocator.Error!void {
        const bytes = try self.allocator.alloc(u8, @sizeOf(u32) + k.len);
        _ = idKey(bytes, ns, k) catch unreachable;
        try self.addKey(fam, bytes);
    }

    fn addKey(self: *KeySet, fam: u16, bytes: []const u8) Allocator.Error!void {
        try self.exact.append(self.allocator, .{ .fam = fam, .key = bytes });
        if (self.exact.items.len == SCAN_LIMIT + 1) {
            for (self.exact.items) |d| try self.exact_index.put(self.allocator, d, {});
        } else if (self.exact.items.len > SCAN_LIMIT + 1) {
            try self.exact_index.put(self.allocator, .{ .fam = fam, .key = bytes }, {});
        }
    }

    /// Every key of `fam` in namespace `ns` that starts with `prefix`.
    /// A range with no namespace is refused: one entry may not reach every
    /// tenant's keys.
    pub fn addRange(self: *KeySet, fam: u16, ns: []const u8, prefix: []const u8) (Allocator.Error || error{NoNamespace})!void {
        if (ns.len == 0) return error.NoNamespace;
        const bytes = try self.allocator.alloc(u8, 1 + ns.len + prefix.len);
        _ = key(bytes, ns, prefix) catch unreachable;
        try self.ranges.append(self.allocator, .{ .fam = fam, .key = bytes });
    }

    /// `addRange` for a family keyed by namespace id (`idKey`); the id that
    /// names no namespace is refused as no namespace is.
    pub fn addRangeIn(self: *KeySet, fam: u16, ns: NamespaceId, prefix: []const u8) (Allocator.Error || error{NoNamespace})!void {
        if (ns == .invalid) return error.NoNamespace;
        const bytes = try self.allocator.alloc(u8, @sizeOf(u32) + prefix.len);
        _ = idKey(bytes, ns, prefix) catch unreachable;
        try self.ranges.append(self.allocator, .{ .fam = fam, .key = bytes });
    }

    /// Every key of `fam`, a family in no namespace whose keys name their
    /// namespace inside (a deadline: its expiry, then its namespace's id),
    /// that starts with `prefix`. The prefix must reach past that id, so the
    /// range is one tenant's; `ns_at` is where the id starts in it.
    pub fn addRangeNaming(self: *KeySet, fam: u16, ns_at: usize, prefix: []const u8) (Allocator.Error || error{NoNamespace})!void {
        if (prefix.len < ns_at + @sizeOf(u32) or std.mem.readInt(u32, prefix[ns_at..][0..4], .big) == 0) return error.NoNamespace;
        const bytes = try self.allocator.alloc(u8, 1 + prefix.len);
        _ = key(bytes, "", prefix) catch unreachable;
        try self.ranges.append(self.allocator, .{ .fam = fam, .key = bytes });
    }

    pub fn covers(self: *const KeySet, fam: u16, k: []const u8) bool {
        if (self.exact.items.len > SCAN_LIMIT) {
            if (self.exact_index.contains(.{ .fam = fam, .key = k })) return true;
        } else for (self.exact.items) |d| if (d.fam == fam and std.mem.eql(u8, d.key, k)) return true;
        for (self.ranges.items) |d| if (d.fam == fam and std.mem.startsWith(u8, k, d.key)) return true;
        return false;
    }

    /// Whether every key starting with `prefix` is declared.
    pub fn coversRange(self: *const KeySet, fam: u16, prefix: []const u8) bool {
        for (self.ranges.items) |d| if (d.fam == fam and std.mem.startsWith(u8, prefix, d.key)) return true;
        return false;
    }
};

/// A family's declaration: its stable id, its name, and its value type,
/// whose `encode` is the one serializer for that family (staged writes,
/// state and snapshots alike) and whose `decode` reads it back. A value
/// may point into the bytes it was decoded from.
pub fn Family(comptime id: u16, comptime name: []const u8, comptime V: type) type {
    comptime std.debug.assert(id < store_mod.MAX_FAMILIES);
    return struct {
        pub const ID = id;
        pub const NAME = name;
        pub const Value = V;
    };
}

/// A staged write: a value's bytes, or null for a delete. A range delete
/// (`range`) deletes every key starting with `key`, and before `below`
/// when there is one.
const Op = struct {
    fam: u16,
    key: []const u8,
    value: ?[]const u8,
    range: bool = false,
    below: ?[]const u8 = null,

    fn covers(op: Op, fam: u16, k: []const u8) bool {
        if (op.fam != fam or !std.mem.startsWith(u8, k, op.key)) return false;
        const below = op.below orelse return true;
        return std.mem.order(u8, k, below) == .lt;
    }
};

/// One entry's staged reads and writes. After any of its calls fails, the
/// view is partial (a write staged but not indexed) and must not be read
/// or committed: the machine drops the entry and its view.
/// One send an entry makes: its target's slot, entry type and payload.
/// The entry flags a channel message may carry: a sink's mark, and nothing
/// a receiver's applier would act on otherwise (`FORCE`, say), so a type
/// that reads one never gets it from another group by accident.
pub const MESSAGE_FLAGS: u16 = @import("../storage/ual/entry.zig").Flags.SINK;

pub const Send = struct { slot: u16, entry_type: u8, flags: u16 = 0, payload: []const u8 };

/// This group among the groups on the node: a slot's owner is the group
/// `slot % count`. Fixed for a data directory, so every replica derives
/// the same owner.
pub const Place = struct { group: u16, count: u16 };

/// The message an entry is: the group it came from, its seq there, and its
/// place in the inbound entry that carried it. Every message in one inbound
/// entry applies at that entry's index, so a record located by index takes
/// `(index, ordinal)` to stay unique.
pub const Message = struct { from: u16, seq: u64, ordinal: u8 };

pub const Mark = struct { ops: usize, sends: usize, late: [8]u64 };

pub const View = struct {
    store: *Store,
    /// The entry's scratch: staged bytes and the key set live here until
    /// the next entry.
    arena: Allocator,
    declared: *const KeySet,
    /// The entry's writes in the order made.
    ops: std.ArrayListUnmanaged(Op) = .empty,
    /// Each key's last write in `ops`, once there are more than
    /// `SCAN_LIMIT`, so a read or the commit finds it without a scan.
    /// Range deletes aren't in it.
    last: FamKey.Map(usize) = .empty,
    /// Late time-series points this entry applied, past each edge
    /// (`Store.ts_late_points`): added to the store's count only at commit,
    /// so a refused entry or rolled-back message counts none.
    ts_late_points: [8]u64 = @splat(0),
    /// Where the range deletes are in `ops`, in order: an entry makes few.
    ranges: std.ArrayListUnmanaged(usize) = .empty,
    /// Fail on an undeclared key rather than count it (`Store.strict_keys`).
    strict: bool,
    /// The shard's streams, read-only, when the apply was handed them.
    streams_read: ?Streams = null,
    /// What the entry sends to other groups, in the order sent. The channel
    /// writes them to its outbox when the entry commits (`machine.applyEntryWith`).
    sends: std.ArrayListUnmanaged(Send) = .empty,
    /// Where this group sits, for `send`; null where nothing may send.
    place: ?Place = null,
    /// The message being applied, when the entry is another group's message
    /// this group took in: what a target's apply reads to know it.
    message: ?Message = null,

    pub fn init(store: *Store, arena: Allocator, declared: *const KeySet) View {
        return .{ .store = store, .arena = arena, .declared = declared, .strict = store.strict_keys, .place = store.place };
    }

    /// The shard's streams as of this entry. A machine that reads them is
    /// applied with them (`machine.applyEntryWith`); asked without them,
    /// this is a Flo bug, and the entry halts.
    pub fn streams(self: *const View) ApplyError!Streams {
        return self.streams_read orelse error.Halt;
    }

    /// Sends `payload`, an entry of type `etype`, to the group that owns
    /// `slot`, which applies it through that type's machine. It leaves with
    /// this entry's writes, or not at all.
    pub fn send(self: *View, slot: u16, etype: u8, payload: []const u8) ApplyError!void {
        return self.sendFlagged(slot, etype, 0, payload);
    }

    /// `send`, with the entry flags the message applies with at its
    /// receiver (a sink's write is `Flags.SINK`): only `MESSAGE_FLAGS`.
    pub fn sendFlagged(self: *View, slot: u16, etype: u8, flags: u16, payload: []const u8) ApplyError!void {
        // A send from an apply that isn't wired for one is a Flo bug, and so
        // is one with a flag no message may carry.
        if (self.place == null) return error.Halt;
        std.debug.assert(flags & ~MESSAGE_FLAGS == 0);
        if (flags & ~MESSAGE_FLAGS != 0) return error.Halt;
        try self.sends.append(self.arena, .{ .slot = slot, .entry_type = etype, .flags = flags, .payload = try self.arena.dupe(u8, payload) });
    }

    /// A point `rollback` returns the staged writes and sends to.
    pub fn mark(self: *const View) Mark {
        return .{ .ops = self.ops.items.len, .sends = self.sends.items.len, .late = self.ts_late_points };
    }

    /// Drops what was staged after `m`: a refused message inside a batch
    /// undoes only its own writes.
    pub fn rollback(self: *View, m: Mark) Allocator.Error!void {
        const had = self.ops.items.len;
        self.ops.shrinkRetainingCapacity(m.ops);
        self.sends.shrinkRetainingCapacity(m.sends);
        self.ts_late_points = m.late;
        // A range delete staged after the mark goes with it: left in, it
        // would still hide keys from later reads and unlink them at commit.
        while (self.ranges.items.len > 0 and self.ranges.items[self.ranges.items.len - 1] >= m.ops) _ = self.ranges.pop();
        if (had > SCAN_LIMIT) {
            self.last.clearRetainingCapacity();
            if (self.ops.items.len > SCAN_LIMIT) {
                // As `stage` keeps it: single writes only, never a range's prefix.
                for (self.ops.items, 0..) |o, i| if (!o.range) try self.last.put(self.arena, .{ .fam = o.fam, .key = o.key }, i);
            }
        }
    }

    /// The channel's own reads and writes of its families, which a sending
    /// entry's keys can't name: the seq it takes is state, not the entry's.
    pub fn getOwn(self: *View, comptime F: type, k: []const u8) ApplyError!?F.Value {
        const bytes = self.bytesOf(F.ID, k) orelse return null;
        return F.Value.decode(bytes) catch error.Halt;
    }

    pub fn putOwn(self: *View, comptime F: type, k: []const u8, value: F.Value) ApplyError!void {
        var w: std.Io.Writer.Allocating = .init(self.arena);
        value.encode(&w.writer) catch return error.OutOfMemory;
        try self.stage(.{ .fam = F.ID, .key = try self.arena.dupe(u8, k), .value = w.written() });
    }

    pub fn get(self: *View, comptime F: type, k: []const u8) ApplyError!?F.Value {
        self.check(F.ID, k);
        const bytes = self.bytesOf(F.ID, k) orelse return null;
        // State that won't decode was written by this build's own encode.
        return F.Value.decode(bytes) catch error.Halt;
    }

    pub fn put(self: *View, comptime F: type, k: []const u8, value: F.Value) ApplyError!void {
        self.check(F.ID, k);
        var w: std.Io.Writer.Allocating = .init(self.arena);
        value.encode(&w.writer) catch return error.OutOfMemory;
        try self.stage(.{ .fam = F.ID, .key = try self.arena.dupe(u8, k), .value = w.written() });
    }

    pub fn delete(self: *View, comptime F: type, k: []const u8) ApplyError!void {
        self.check(F.ID, k);
        try self.stage(.{ .fam = F.ID, .key = try self.arena.dupe(u8, k), .value = null });
    }

    /// Deletes every key of the family starting with `prefix`, and before
    /// `below` when one is given, however many: staged as one write, and
    /// unlinked in place at commit. Reads later in this entry see them
    /// gone, as they see a single delete; a write after it puts a key
    /// back. The prefix must lie in a declared range, as `range`'s does.
    pub fn deleteRange(self: *View, comptime F: type, prefix: []const u8, below: ?[]const u8) ApplyError!void {
        if (!self.declared.coversRange(F.ID, prefix)) self.undeclared();
        try self.ranges.ensureUnusedCapacity(self.arena, 1);
        const b: ?[]const u8 = if (below) |x| try self.arena.dupe(u8, x) else null;
        try self.stage(.{ .fam = F.ID, .key = try self.arena.dupe(u8, prefix), .value = null, .range = true, .below = b });
        self.ranges.appendAssumeCapacity(self.ops.items.len - 1);
    }

    /// Whether a range delete after `after` in `ops` (after none: any)
    /// covers `k`.
    fn rangedAfter(self: *const View, fam: u16, k: []const u8, after: ?usize) bool {
        var n = self.ranges.items.len;
        while (n > 0) {
            n -= 1;
            const at = self.ranges.items[n];
            if (after) |a| if (at < a) return false;
            if (self.ops.items[at].covers(fam, k)) return true;
        }
        return false;
    }

    fn stage(self: *View, op: Op) Allocator.Error!void {
        try self.ops.append(self.arena, op);
        const n = self.ops.items.len;
        if (n == SCAN_LIMIT + 1) {
            for (self.ops.items, 0..) |o, i| if (!o.range) try self.last.put(self.arena, .{ .fam = o.fam, .key = o.key }, i);
        } else if (n > SCAN_LIMIT + 1 and !op.range) {
            try self.last.put(self.arena, .{ .fam = op.fam, .key = op.key }, n - 1);
        }
    }

    /// The index in `ops` of `k`'s last write, if this entry wrote it.
    fn lastWrite(self: *const View, fam: u16, k: []const u8) ?usize {
        if (self.ops.items.len > SCAN_LIMIT) return self.last.get(.{ .fam = fam, .key = k });
        var i = self.ops.items.len;
        while (i > 0) {
            i -= 1;
            const op = self.ops.items[i];
            if (!op.range and op.fam == fam and std.mem.eql(u8, op.key, k)) return i;
        }
        return null;
    }

    /// The family's keys starting with `prefix`, in key order, as this
    /// entry sees them: its own staged writes over committed state. The
    /// prefix must lie in a declared range, as `KeySet.addRange` builds one
    /// (namespace first); one that doesn't is undeclared.
    pub fn range(self: *View, comptime F: type, prefix: []const u8) ApplyError!ViewRange(F) {
        if (!self.declared.coversRange(F.ID, prefix)) self.undeclared();
        // This entry's writes under the prefix, the last per key, in order;
        // one a later range delete covers is gone with the rest it covers.
        var staged: std.ArrayListUnmanaged(Op) = .empty;
        var i = self.ops.items.len;
        while (i > 0) {
            i -= 1;
            const op = self.ops.items[i];
            if (op.range or op.fam != F.ID or !std.mem.startsWith(u8, op.key, prefix)) continue;
            if (indexOfKey(staged.items, op.key) != null) continue;
            if (self.rangedAfter(F.ID, op.key, i)) continue;
            try staged.append(self.arena, op);
        }
        std.mem.sort(Op, staged.items, {}, opLess);
        var hides: std.ArrayListUnmanaged(Op) = .empty;
        for (self.ranges.items) |at| if (self.ops.items[at].fam == F.ID) try hides.append(self.arena, self.ops.items[at]);
        return .{ .base = self.store.range(F.ID, prefix), .staged = staged.items, .hides = hides.items };
    }

    /// `range`, from the first key past `after`.
    pub fn rangeAfter(self: *View, comptime F: type, prefix: []const u8, after: []const u8) ApplyError!ViewRange(F) {
        var r = try self.range(F, prefix);
        r.base = self.store.rangeAfter(F.ID, prefix, after);
        var skip: usize = 0;
        while (skip < r.staged.len and std.mem.order(u8, r.staged[skip].key, after) != .gt) skip += 1;
        r.staged = r.staged[skip..];
        return r;
    }

    fn bytesOf(self: *View, fam: u16, k: []const u8) ?[]const u8 {
        const last = self.lastWrite(fam, k);
        if (self.ranges.items.len > 0 and self.rangedAfter(fam, k, last)) return null;
        if (last) |i| return self.ops.items[i].value;
        return self.store.get(fam, k);
    }

    fn check(self: *View, fam: u16, k: []const u8) void {
        if (!self.declared.covers(fam, k)) self.undeclared();
    }

    fn undeclared(self: *View) void {
        if (self.strict) @panic("an applier touched a key its entry didn't declare");
        self.store.undeclared_keys += 1;
        const n = self.store.undeclared_keys;
        if (n & (n - 1) == 0) log.err("an applier touched a key its entry didn't declare ({d} so far); this is a bug in this build", .{n});
    }

    /// Make the staged writes state. Everything they need is allocated
    /// first; only then is anything linked, which can't fail. Out of
    /// memory, state is as it was.
    pub fn commit(self: *View) ApplyError!void {
        // The last write per key. Each key is written once, so the order
        // doesn't change the state; it does shape the store's tree, and every
        // replica of a build commits in the same order.
        // A write a later range delete covers is gone with the range.
        var plan: std.ArrayListUnmanaged(Step) = .empty;
        if (self.ops.items.len > SCAN_LIMIT) {
            for (self.ops.items, 0..) |op, i| {
                if (op.range or self.last.get(.{ .fam = op.fam, .key = op.key }).? != i) continue;
                if (self.ranges.items.len > 0 and self.rangedAfter(op.fam, op.key, i)) continue;
                try plan.append(self.arena, .{ .op = op });
            }
        } else {
            var i = self.ops.items.len;
            while (i > 0) {
                i -= 1;
                const op = self.ops.items[i];
                if (op.range) continue;
                if (indexOfStep(plan.items, op)) |_| continue;
                if (self.ranges.items.len > 0 and self.rangedAfter(op.fam, op.key, i)) continue;
                try plan.append(self.arena, .{ .op = op });
            }
        }
        // The keys a range covers that a later write keeps: the range
        // passes over them, and their step writes them.
        var kept: FamKey.Map(void) = .empty;
        if (self.ranges.items.len > 0) for (plan.items) |s| {
            if (self.rangedAfter(s.op.fam, s.op.key, null)) try kept.put(self.arena, .{ .fam = s.op.fam, .key = s.op.key }, {});
        };
        var reserved: usize = 0;
        errdefer for (plan.items[0..reserved]) |*s| self.release(s);
        for (plan.items) |*s| {
            try self.reserve(s);
            reserved += 1;
        }
        // Nothing past here can fail: a range unlinks what it covers in
        // place, then each step writes its key.
        for (self.ranges.items) |at| {
            const op = self.ops.items[at];
            var it = self.store.range(op.fam, op.key);
            while (it.next()) |rec| {
                if (op.below) |b| if (std.mem.order(u8, rec.key(), b) != .lt) break;
                if (kept.contains(.{ .fam = op.fam, .key = rec.key() })) continue;
                self.store.unlink(op.fam, &rec.node);
            }
        }
        for (plan.items) |*s| self.apply(s);
        for (&self.store.ts_late_points, self.ts_late_points) |*n, d| n.* += d;
    }

    const Step = struct {
        op: Op,
        /// The record it changes, found while reserving.
        node: ?*store_mod.Tree.Node = null,
        /// A new record for a key the family doesn't hold.
        fresh: ?*store_mod.Record = null,
        value: ?[]u8 = null,
    };

    fn indexOfStep(steps: []const Step, op: Op) ?usize {
        for (steps, 0..) |s, n| if (s.op.fam == op.fam and std.mem.eql(u8, s.op.key, op.key)) return n;
        return null;
    }

    fn reserve(self: *View, s: *Step) Allocator.Error!void {
        s.node = self.store.find(s.op.fam, s.op.key);
        const bytes = s.op.value orelse return;
        s.value = try self.store.allocator.dupe(u8, bytes);
        errdefer {
            self.store.allocator.free(s.value.?);
            s.value = null;
        }
        if (s.node == null) s.fresh = try self.store.allocRecord(s.op.key);
    }

    fn release(self: *View, s: *Step) void {
        if (s.value) |v| self.store.allocator.free(v);
        if (s.fresh) |r| self.store.freeRecord(r);
        s.* = .{ .op = s.op };
    }

    fn apply(self: *View, s: *Step) void {
        const fam = s.op.fam;
        if (s.value) |v| {
            if (s.fresh) |r| self.store.link(fam, r, v) else self.store.replace(fam, s.node.?, v);
        } else if (s.node) |n| {
            self.store.unlink(fam, n);
        }
    }
};

fn indexOfKey(ops: []const Op, k: []const u8) ?usize {
    for (ops, 0..) |op, n| if (std.mem.eql(u8, op.key, k)) return n;
    return null;
}

fn hidden(hides: []const Op, fam: u16, k: []const u8) bool {
    for (hides) |op| if (op.covers(fam, k)) return true;
    return false;
}

fn opLess(_: void, a: Op, b: Op) bool {
    return std.mem.order(u8, a.key, b.key) == .lt;
}

/// A range as an entry sees it: committed records merged with the entry's
/// staged writes, which win; staged deletes hide their key.
pub fn ViewRange(comptime F: type) type {
    return struct {
        base: store_mod.Range,
        staged: []const Op,
        /// The entry's range deletes of this family: a committed record
        /// one covers is gone.
        hides: []const Op = &.{},
        pending: ?*store_mod.Record = null,
        started: bool = false,

        pub const Item = struct { key: []const u8, value: F.Value };

        pub fn next(self: *@This()) ApplyError!?Item {
            if (!self.started) {
                self.started = true;
                self.pending = self.base.next();
            }
            while (true) {
                const committed = self.pending;
                const staged: ?Op = if (self.staged.len > 0) self.staged[0] else null;
                if (committed == null and staged == null) return null;
                const take_staged = staged != null and (committed == null or std.mem.order(u8, staged.?.key, committed.?.key()) != .gt);
                if (take_staged) {
                    const op = staged.?;
                    self.staged = self.staged[1..];
                    if (committed != null and std.mem.eql(u8, op.key, committed.?.key())) self.pending = self.base.next();
                    const bytes = op.value orelse continue;
                    return .{ .key = op.key, .value = F.Value.decode(bytes) catch return error.Halt };
                }
                const rec = committed.?;
                self.pending = self.base.next();
                if (hidden(self.hides, F.ID, rec.key())) continue;
                return .{ .key = rec.key(), .value = F.Value.decode(rec.value) catch return error.Halt };
            }
        }
    };
}

/// Committed state, for reads.
pub const ReadView = struct {
    store: *Store,

    pub fn get(self: ReadView, comptime F: type, k: []const u8) error{Malformed}!?F.Value {
        const bytes = self.store.get(F.ID, k) orelse return null;
        return try F.Value.decode(bytes);
    }

    pub fn range(self: ReadView, comptime F: type, prefix: []const u8) ReadRange(F) {
        return .{ .inner = self.store.range(F.ID, prefix) };
    }

    pub fn rangeAfter(self: ReadView, comptime F: type, prefix: []const u8, after: []const u8) ReadRange(F) {
        return .{ .inner = self.store.rangeAfter(F.ID, prefix, after) };
    }

    /// The keys starting with `prefix` and before `before`, last first.
    pub fn rangeBefore(self: ReadView, comptime F: type, prefix: []const u8, before: []const u8) ReadRange(F) {
        return .{ .inner = self.store.rangeBefore(F.ID, prefix, before) };
    }
};

pub fn ReadRange(comptime F: type) type {
    return struct {
        inner: store_mod.Range,

        pub const Item = struct { key: []const u8, value: F.Value };

        pub fn next(self: *@This()) error{Malformed}!?Item {
            const rec = self.inner.next() orelse return null;
            return .{ .key = rec.key(), .value = try F.Value.decode(rec.value) };
        }
    };
}

test "a key in one family is never the same key in another, whatever their hashes" {
    // The family seeds the hash, so two families' keys rarely meet in a
    // bucket; equality must still tell them apart when they do.
    const ctx: FamKey.Context = .{};
    try std.testing.expect(!ctx.eql(.{ .fam = 1, .key = "\x01tk" }, .{ .fam = 2, .key = "\x01tk" }));
    try std.testing.expect(ctx.eql(.{ .fam = 1, .key = "\x01tk" }, .{ .fam = 1, .key = "\x01tk" }));
    try std.testing.expect(!ctx.eql(.{ .fam = 1, .key = "\x01tk" }, .{ .fam = 1, .key = "\x01tj" }));
}
