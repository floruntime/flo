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

/// Every key an entry may touch, from the entry alone: whole keys, and
/// ranges by prefix. A range names its namespace, so no entry declares a
/// whole family or every namespace.
pub const KeySet = struct {
    allocator: Allocator,
    exact: std.ArrayListUnmanaged(Declared) = .empty,
    ranges: std.ArrayListUnmanaged(Declared) = .empty,

    const Declared = struct { fam: u16, key: []const u8 };

    pub fn init(allocator: Allocator) KeySet {
        return .{ .allocator = allocator };
    }

    pub fn add(self: *KeySet, fam: u16, ns: []const u8, k: []const u8) Allocator.Error!void {
        const bytes = try self.allocator.alloc(u8, 1 + ns.len + k.len);
        _ = key(bytes, ns, k) catch unreachable;
        try self.exact.append(self.allocator, .{ .fam = fam, .key = bytes });
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

    pub fn covers(self: *const KeySet, fam: u16, k: []const u8) bool {
        for (self.exact.items) |d| if (d.fam == fam and std.mem.eql(u8, d.key, k)) return true;
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

/// A staged write: a value's bytes, or null for a delete.
const Op = struct { fam: u16, key: []const u8, value: ?[]const u8 };

pub const View = struct {
    store: *Store,
    /// The entry's scratch: staged bytes and the key set live here until
    /// the next entry.
    arena: Allocator,
    declared: *const KeySet,
    /// The entry's writes in the order made. Lookups scan them, so an
    /// entry's cost grows with the square of its writes: fine for one
    /// request's, worth an index for a large batch.
    ops: std.ArrayListUnmanaged(Op) = .empty,
    /// Fail on an undeclared key rather than count it (`Store.strict_keys`).
    strict: bool,

    pub fn init(store: *Store, arena: Allocator, declared: *const KeySet) View {
        return .{ .store = store, .arena = arena, .declared = declared, .strict = store.strict_keys };
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
        try self.ops.append(self.arena, .{ .fam = F.ID, .key = try self.arena.dupe(u8, k), .value = w.written() });
    }

    pub fn delete(self: *View, comptime F: type, k: []const u8) ApplyError!void {
        self.check(F.ID, k);
        try self.ops.append(self.arena, .{ .fam = F.ID, .key = try self.arena.dupe(u8, k), .value = null });
    }

    /// The family's keys starting with `prefix`, in key order, as this
    /// entry sees them: its own staged writes over committed state. The
    /// prefix must lie in a declared range, as `KeySet.addRange` builds one
    /// (namespace first); one that doesn't is undeclared.
    pub fn range(self: *View, comptime F: type, prefix: []const u8) ApplyError!ViewRange(F) {
        if (!self.declared.coversRange(F.ID, prefix)) self.undeclared();
        // This entry's writes under the prefix, the last per key, in order.
        var staged: std.ArrayListUnmanaged(Op) = .empty;
        var i = self.ops.items.len;
        while (i > 0) {
            i -= 1;
            const op = self.ops.items[i];
            if (op.fam != F.ID or !std.mem.startsWith(u8, op.key, prefix)) continue;
            if (indexOfKey(staged.items, op.key) != null) continue;
            try staged.append(self.arena, op);
        }
        std.mem.sort(Op, staged.items, {}, opLess);
        return .{ .base = self.store.range(F.ID, prefix), .staged = staged.items };
    }

    fn bytesOf(self: *View, fam: u16, k: []const u8) ?[]const u8 {
        var i = self.ops.items.len;
        while (i > 0) {
            i -= 1;
            const op = self.ops.items[i];
            if (op.fam == fam and std.mem.eql(u8, op.key, k)) return op.value;
        }
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
        // The last write per key. Each key is written once, so their order
        // doesn't matter.
        var plan: std.ArrayListUnmanaged(Step) = .empty;
        var i = self.ops.items.len;
        while (i > 0) {
            i -= 1;
            const op = self.ops.items[i];
            if (indexOfStep(plan.items, op)) |_| continue;
            try plan.append(self.arena, .{ .op = op });
        }
        var reserved: usize = 0;
        errdefer for (plan.items[0..reserved]) |*s| self.release(s);
        for (plan.items) |*s| {
            try self.reserve(s);
            reserved += 1;
        }
        for (plan.items) |*s| self.apply(s);
    }

    const Step = struct {
        op: Op,
        /// The record it changes, found while reserving.
        node: ?*store_mod.Tree.Node = null,
        /// A new record for a key the family doesn't hold.
        fresh: ?*store_mod.Record = null,
        value: ?[]u8 = null,
    };

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

    fn indexOfStep(steps: []const Step, op: Op) ?usize {
        for (steps, 0..) |s, n| if (s.op.fam == op.fam and std.mem.eql(u8, s.op.key, op.key)) return n;
        return null;
    }
};

fn indexOfKey(ops: []const Op, k: []const u8) ?usize {
    for (ops, 0..) |op, n| if (std.mem.eql(u8, op.key, k)) return n;
    return null;
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
