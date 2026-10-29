//! The contract's own tests, over a toy machine: named counters.

const std = @import("std");
const testing = std.testing;
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("store.zig");
const view_mod = @import("view.zig");
const machine_mod = @import("machine.zig");

const Store = store_mod.Store;
const View = view_mod.View;
const Entry = entry_mod.Entry;

const Count = struct {
    n: u64,

    pub fn encode(self: Count, w: *std.Io.Writer) std.Io.Writer.Error!void {
        var b: [8]u8 = undefined;
        std.mem.writeInt(u64, &b, self.n, .little);
        try w.writeAll(&b);
    }

    pub fn decode(bytes: []const u8) error{Malformed}!Count {
        if (bytes.len != 8) return error.Malformed;
        return .{ .n = std.mem.readInt(u64, bytes[0..8], .little) };
    }
};

const Counters = view_mod.Family(1, "test.counters", Count);
const NS = "t";

/// An entry's payload: `[op u8]` then names separated by ','. `incr` adds
/// one to each; `refuse` and `halt` add one to each, then refuse or halt;
/// `drop` deletes each; `overwrite` sets each to 7, then to 9, then
/// refuses.
const Opcode = enum(u8) { incr, refuse, halt, drop, overwrite };

fn names(e: *const Entry) std.mem.SplitIterator(u8, .scalar) {
    return std.mem.splitScalar(u8, e.payload[1..], ',');
}

fn keyOf(buf: []u8, name: []const u8) []const u8 {
    return view_mod.key(buf, NS, name) catch unreachable;
}

fn counterKeys(e: *const Entry, out: *view_mod.KeySet) error{OutOfMemory}!void {
    var it = names(e);
    while (it.next()) |n| try out.add(Counters.ID, NS, n);
}

fn counterApply(view: *View, e: *const Entry, out: *persistence.Result) persistence.ApplyError!void {
    const op: Opcode = @fromBackingInt(@intCast(e.payload[0]));
    var it = names(e);
    while (it.next()) |n| {
        var kb: [64]u8 = undefined;
        const k = keyOf(&kb, n);
        switch (op) {
            .drop => try view.delete(Counters, k),
            .overwrite => {
                try view.put(Counters, k, .{ .n = 7 });
                try view.put(Counters, k, .{ .n = 9 });
            },
            else => {
                const was = (try view.get(Counters, k)) orelse Count{ .n = 0 };
                try view.put(Counters, k, .{ .n = was.n + 1 });
            },
        }
    }
    switch (op) {
        .refuse, .overwrite => out.refuse(.conflict, .condition_failed, "refused after staging"),
        .halt => return error.Halt,
        else => {},
    }
}

const counter_machine: machine_mod.Machine = .{
    .name = "counters",
    .entry_types = &.{.kv_put},
    .families = &.{machine_mod.info(Counters)},
    .keys = counterKeys,
    .apply = counterApply,
};

fn entryOf(buf: []u8, index: u64, op: Opcode, list: []const u8) Entry {
    buf[0] = @backingInt(op);
    @memcpy(buf[1..][0..list.len], list);
    return entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, index, index, buf[0 .. 1 + list.len]);
}

const Fixture = struct {
    store: Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,

    fn init(allocator: std.mem.Allocator) Fixture {
        return .{ .store = Store.init(allocator), .scratch = std.heap.ArenaAllocator.init(testing.allocator) };
    }

    fn deinit(self: *Fixture) void {
        self.store.deinit();
        self.scratch.deinit();
    }

    fn run(self: *Fixture, op: Opcode, list: []const u8) !persistence.Result {
        var buf: [256]u8 = undefined;
        self.index += 1;
        const e = entryOf(&buf, self.index, op, list);
        var out: persistence.Result = .{};
        out.reset(&e);
        try machine_mod.applyEntry(&counter_machine, &self.store, &self.scratch, &e, &out);
        return out;
    }

    fn count(self: *Fixture, name: []const u8) ?u64 {
        var kb: [64]u8 = undefined;
        const v = (view_mod.ReadView{ .store = &self.store }).get(Counters, keyOf(&kb, name)) catch unreachable;
        return if (v) |c| c.n else null;
    }

    /// The family's canonical bytes, owned by the caller.
    fn bytes(self: *Fixture) ![]u8 {
        var w: std.Io.Writer.Allocating = .init(testing.allocator);
        defer w.deinit();
        try store_mod.encode(&self.store, Counters.ID, &w.writer);
        return testing.allocator.dupe(u8, w.written());
    }
};

test "contract: an entry's writes become state when it applies, reading its own staged writes" {
    var f = Fixture.init(testing.allocator);
    defer f.deinit();
    _ = try f.run(.incr, "a,b,a");
    try testing.expectEqual(@as(?u64, 2), f.count("a"));
    try testing.expectEqual(@as(?u64, 1), f.count("b"));
    _ = try f.run(.drop, "b");
    try testing.expectEqual(@as(?u64, null), f.count("b"));
    try testing.expectEqual(@as(u64, 1), f.store.family(Counters.ID).count);
}

test "contract: a refused or halted entry leaves state byte-identical, after writes to several keys and overwrites of keys it staged" {
    var f = Fixture.init(testing.allocator);
    defer f.deinit();
    _ = try f.run(.incr, "a,b,c");
    const before = try f.bytes();
    defer testing.allocator.free(before);
    const size = f.store.family(Counters.ID).encoded_bytes;

    const refused = try f.run(.refuse, "a,b,c,d");
    try testing.expect(refused.refused() != null);
    try testing.expectError(error.Halt, f.run(.halt, "a,d"));
    try testing.expect((try f.run(.overwrite, "a,e")).refused() != null);

    const after = try f.bytes();
    defer testing.allocator.free(after);
    try testing.expectEqualSlices(u8, before, after);
    try testing.expectEqual(size, f.store.family(Counters.ID).encoded_bytes);
}

test "contract: out of memory at any step of a commit leaves state as it was" {
    var fail_at: usize = 0;
    while (true) : (fail_at += 1) {
        var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = std.math.maxInt(usize) });
        var f = Fixture.init(failing.allocator());
        defer f.deinit();
        _ = try f.run(.incr, "a,b");
        const before = try f.bytes();
        defer testing.allocator.free(before);
        // Updates two keys, adds two and deletes none: values and records.
        failing.fail_index = failing.alloc_index + fail_at;
        const outcome = f.run(.incr, "a,b,c,d");
        failing.fail_index = std.math.maxInt(usize);
        const after = try f.bytes();
        defer testing.allocator.free(after);
        if (outcome) |_| {
            try testing.expectEqual(@as(?u64, 1), f.count("d"));
            break;
        } else |err| {
            try testing.expectEqual(error.OutOfMemory, err);
            try testing.expectEqualSlices(u8, before, after);
        }
    }
    try testing.expect(fail_at > 0);
}

test "contract: a family's bytes depend on its state, not the order it was written in" {
    var x = Fixture.init(testing.allocator);
    defer x.deinit();
    var y = Fixture.init(testing.allocator);
    defer y.deinit();
    _ = try x.run(.incr, "m,a,z,q");
    _ = try x.run(.incr, "a");
    _ = try y.run(.incr, "a,a");
    _ = try y.run(.incr, "q,x,z,m");
    _ = try y.run(.drop, "x");
    const xb = try x.bytes();
    defer testing.allocator.free(xb);
    const yb = try y.bytes();
    defer testing.allocator.free(yb);
    try testing.expectEqualSlices(u8, xb, yb);
    try testing.expectEqual(store_mod.checksum(&x.store, Counters.ID), store_mod.checksum(&y.store, Counters.ID));
    try testing.expectEqual(x.store.family(Counters.ID).encoded_bytes, y.store.family(Counters.ID).encoded_bytes);
    // The size counter is the encoding's, less its header.
    try testing.expectEqual(@as(u64, xb.len - 10), x.store.family(Counters.ID).encoded_bytes);
    _ = try y.run(.incr, "m");
    try testing.expect(store_mod.checksum(&x.store, Counters.ID) != store_mod.checksum(&y.store, Counters.ID));
}

test "contract: a range reads keys in order, staged writes over committed ones, staged deletes hidden" {
    var f = Fixture.init(testing.allocator);
    defer f.deinit();
    _ = try f.run(.incr, "b,d,f");
    var keys = view_mod.KeySet.init(f.scratch.allocator());
    try keys.addRange(Counters.ID, NS, "");
    var view = View.init(&f.store, f.scratch.allocator(), &keys);
    var kb: [64]u8 = undefined;
    try view.put(Counters, keyOf(&kb, "a"), .{ .n = 10 });
    try view.put(Counters, keyOf(&kb, "d"), .{ .n = 40 });
    try view.delete(Counters, keyOf(&kb, "f"));
    try view.put(Counters, keyOf(&kb, "e"), .{ .n = 50 });
    var prefix: [8]u8 = undefined;
    var r = try view.range(Counters, try view_mod.key(&prefix, NS, ""));
    const want = [_]struct { k: []const u8, n: u64 }{ .{ .k = "a", .n = 10 }, .{ .k = "b", .n = 1 }, .{ .k = "d", .n = 40 }, .{ .k = "e", .n = 50 } };
    for (want) |w| {
        const item = (try r.next()).?;
        try testing.expectEqualStrings(w.k, item.key[2..]);
        try testing.expectEqual(w.n, item.value.n);
    }
    try testing.expectEqual(null, try r.next());
    // Committed state never saw any of it.
    try testing.expectEqual(@as(?u64, 1), f.count("d"));
    try testing.expectEqual(@as(?u64, null), f.count("a"));
}

test "contract: a range names its namespace, and a key the entry didn't declare is counted" {
    var f = Fixture.init(testing.allocator);
    defer f.deinit();
    var keys = view_mod.KeySet.init(f.scratch.allocator());
    try testing.expectError(error.NoNamespace, keys.addRange(Counters.ID, "", "x"));
    try keys.add(Counters.ID, NS, "a");
    // A strict store fails instead (debug builds, and VOPR); this one counts.
    f.store.strict_keys = true;
    try testing.expect(View.init(&f.store, f.scratch.allocator(), &keys).strict);
    f.store.strict_keys = false;
    var view = View.init(&f.store, f.scratch.allocator(), &keys);
    var kb: [64]u8 = undefined;
    _ = try view.get(Counters, keyOf(&kb, "a"));
    try testing.expectEqual(@as(u64, 0), f.store.undeclared_keys);
    try view.put(Counters, keyOf(&kb, "b"), .{ .n = 1 });
    try testing.expectEqual(@as(u64, 1), f.store.undeclared_keys);
}

test "contract: a machine list with a shared entry type, family id or family name is refused" {
    const other = machine_mod.Machine{ .name = "other", .entry_types = &.{.kv_delete}, .families = &.{.{ .id = 2, .name = "test.other" }}, .keys = counterKeys, .apply = counterApply };
    try testing.expectEqual(@as(?[]const u8, null), comptime machine_mod.listRule(&.{ counter_machine, other }));
    const same_type = machine_mod.Machine{ .name = "x", .entry_types = &.{.kv_put}, .families = &.{}, .keys = counterKeys, .apply = counterApply };
    try testing.expect(comptime machine_mod.listRule(&.{ counter_machine, same_type }) != null);
    const same_id = machine_mod.Machine{ .name = "y", .entry_types = &.{}, .families = &.{.{ .id = 1, .name = "test.y" }}, .keys = counterKeys, .apply = counterApply };
    try testing.expect(comptime machine_mod.listRule(&.{ counter_machine, same_id }) != null);
    const same_name = machine_mod.Machine{ .name = "z", .entry_types = &.{}, .families = &.{.{ .id = 3, .name = "test.counters" }}, .keys = counterKeys, .apply = counterApply };
    try testing.expect(comptime machine_mod.listRule(&.{ counter_machine, same_name }) != null);
    try testing.expectEqualStrings("other", machine_mod.ownerOf(&.{ counter_machine, other }, .kv_delete).?.name);
    try testing.expectEqual(null, machine_mod.ownerOf(&.{ counter_machine, other }, .kv_batch));
}

test "contract: a range by any prefix returns exactly the keys with it, in order, however the keys were inserted" {
    var prng = std.Random.DefaultPrng.init(0xF10);
    const rand = prng.random();
    const alphabet = "abc";
    for (0..20) |_| {
        var store = Store.init(testing.allocator);
        defer store.deinit();
        var all: std.ArrayListUnmanaged([]const u8) = .empty;
        defer {
            for (all.items) |k| testing.allocator.free(k);
            all.deinit(testing.allocator);
        }
        for (0..40) |_| {
            var k: [3]u8 = undefined;
            const len = rand.intRangeAtMost(usize, 1, 3);
            for (k[0..len]) |*c| c.* = alphabet[rand.uintLessThan(usize, alphabet.len)];
            if (store.find(Counters.ID, k[0..len]) != null) continue;
            const rec = try store.allocRecord(k[0..len]);
            store.link(Counters.ID, rec, try testing.allocator.dupe(u8, "v"));
            try all.append(testing.allocator, try testing.allocator.dupe(u8, k[0..len]));
        }
        std.mem.sort([]const u8, all.items, {}, struct {
            fn less(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.less);
        // Every prefix of up to two letters, held or not.
        for (0..1 + alphabet.len + alphabet.len * alphabet.len) |p| {
            var pb: [2]u8 = undefined;
            const prefix: []const u8 = if (p == 0) "" else if (p <= alphabet.len) blk: {
                pb[0] = alphabet[p - 1];
                break :blk pb[0..1];
            } else blk: {
                pb[0] = alphabet[(p - 1 - alphabet.len) / alphabet.len];
                pb[1] = alphabet[(p - 1 - alphabet.len) % alphabet.len];
                break :blk pb[0..2];
            };
            var r = store.range(Counters.ID, prefix);
            for (all.items) |k| {
                if (!std.mem.startsWith(u8, k, prefix)) continue;
                try testing.expectEqualStrings(k, r.next().?.key());
            }
            try testing.expectEqual(null, r.next());
        }
    }
}

/// Every node's key and priority, in key order: the tree's whole shape.
fn shape(store: *Store, fam: u16, out: *std.ArrayListUnmanaged(u64)) !void {
    var node = store.families[fam].tree.getMin();
    while (node) |n| : (node = n.next()) {
        try out.append(testing.allocator, std.hash.Wyhash.hash(0, n.key));
        try out.append(testing.allocator, n.priority);
    }
}

test "contract: the same writes build the same tree, whatever the allocator hands out" {
    var x = Fixture.init(testing.allocator);
    defer x.deinit();
    // Another allocator, so the nodes land at other addresses.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    _ = try arena.allocator().alloc(u8, 4096);
    var y = Fixture.init(arena.allocator());
    defer y.deinit();
    for ([_][]const u8{ "m,a,z", "q,a", "b,c,d,e", "z" }) |list| {
        _ = try x.run(.incr, list);
        _ = try y.run(.incr, list);
    }
    _ = try x.run(.drop, "c");
    _ = try y.run(.drop, "c");
    const xb = try x.bytes();
    defer testing.allocator.free(xb);
    const yb = try y.bytes();
    defer testing.allocator.free(yb);
    try testing.expectEqualSlices(u8, xb, yb);
    try testing.expectEqual(store_mod.checksum(&x.store, Counters.ID), store_mod.checksum(&y.store, Counters.ID));
    var xs: std.ArrayListUnmanaged(u64) = .empty;
    defer xs.deinit(testing.allocator);
    var ys: std.ArrayListUnmanaged(u64) = .empty;
    defer ys.deinit(testing.allocator);
    try shape(&x.store, Counters.ID, &xs);
    try shape(&y.store, Counters.ID, &ys);
    try testing.expectEqualSlices(u64, xs.items, ys.items);
    try testing.expect(x.store.families[Counters.ID].tree.root.?.key.ptr != y.store.families[Counters.ID].tree.root.?.key.ptr);
    try testing.expectEqualStrings(x.store.families[Counters.ID].tree.root.?.key, y.store.families[Counters.ID].tree.root.?.key);
}

test "contract: a cleared store builds the trees a new one would" {
    var x = Fixture.init(testing.allocator);
    defer x.deinit();
    var y = Fixture.init(testing.allocator);
    defer y.deinit();
    _ = try y.run(.incr, "p,q,r,s");
    y.store.clear();
    try testing.expectEqual(@as(u64, 0), y.store.family(Counters.ID).count);
    for ([_][]const u8{ "m,a,z", "q,a", "b,c,d,e" }) |list| {
        _ = try x.run(.incr, list);
        _ = try y.run(.incr, list);
    }
    var xs: std.ArrayListUnmanaged(u64) = .empty;
    defer xs.deinit(testing.allocator);
    var ys: std.ArrayListUnmanaged(u64) = .empty;
    defer ys.deinit(testing.allocator);
    try shape(&x.store, Counters.ID, &xs);
    try shape(&y.store, Counters.ID, &ys);
    try testing.expectEqualSlices(u64, xs.items, ys.items);
}
