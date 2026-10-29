//! A commit's atomicity and a range's merge, over byte values: every
//! allocation point of a mixed commit, and a randomised differential
//! against a plain map on two allocators.

const std = @import("std");
const testing = std.testing;
const store_mod = @import("store.zig");
const view_mod = @import("view.zig");

const Store = store_mod.Store;
const View = view_mod.View;

const Bytes = struct {
    b: []const u8,
    pub fn encode(self: Bytes, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try w.writeAll(self.b);
    }
    pub fn decode(bytes: []const u8) error{Malformed}!Bytes {
        return .{ .b = bytes };
    }
};
const Fam = view_mod.Family(3, "probe.bytes", Bytes);
const NS = "n";

fn k(buf: []u8, name: []const u8) []const u8 {
    return view_mod.key(buf, NS, name) catch unreachable;
}

fn bytesOf(store: *Store) ![]u8 {
    var w: std.Io.Writer.Allocating = .init(testing.allocator);
    defer w.deinit();
    try store_mod.encode(store, Fam.ID, &w.writer);
    return testing.allocator.dupe(u8, w.written());
}

fn shapeHash(store: *Store) u64 {
    var h = std.hash.Wyhash.init(0);
    var node = store.families[Fam.ID].tree.getMin();
    while (node) |n| : (node = n.next()) {
        h.update(n.key);
        h.update(std.mem.asBytes(&n.priority));
        // With the keys and priorities, each node's parent fixes the shape.
        if (n.parent) |p| h.update(p.key) else h.update("<root>");
    }
    return h.final();
}

/// One entry of every kind of write: replace "a", delete "b", put then
/// delete a new "x", delete then put "c", put a new "y" twice, a new "z".
fn mixed(view: *View) !void {
    var kb: [32]u8 = undefined;
    try view.put(Fam, k(&kb, "a"), .{ .b = "A2-longer-value" });
    try view.delete(Fam, k(&kb, "b"));
    try view.put(Fam, k(&kb, "x"), .{ .b = "X" });
    try view.delete(Fam, k(&kb, "x"));
    try view.delete(Fam, k(&kb, "c"));
    try view.put(Fam, k(&kb, "c"), .{ .b = "C2" });
    try view.put(Fam, k(&kb, "y"), .{ .b = "Y1" });
    try view.put(Fam, k(&kb, "y"), .{ .b = "Y2" });
    try view.put(Fam, k(&kb, "z"), .{ .b = "" });
}

fn seed(store: *Store, scratch: *std.heap.ArenaAllocator) !void {
    var ks = view_mod.KeySet.init(scratch.allocator());
    try ks.addRange(Fam.ID, NS, "");
    var view = View.init(store, scratch.allocator(), &ks);
    var kb: [32]u8 = undefined;
    for ([_][]const u8{ "a", "b", "c", "d" }) |n| try view.put(Fam, k(&kb, n), .{ .b = n });
    try view.commit();
}

test "contract: out of memory at every allocation of a mixed commit leaves bytes and shape as they were, and leaks nothing" {
    var fail_at: usize = 0;
    while (true) : (fail_at += 1) {
        var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = std.math.maxInt(usize) });
        var store = Store.init(failing.allocator());
        defer store.deinit();
        var scratch = std.heap.ArenaAllocator.init(testing.allocator);
        defer scratch.deinit();
        try seed(&store, &scratch);
        _ = scratch.reset(.retain_capacity);
        const before = try bytesOf(&store);
        defer testing.allocator.free(before);
        const shape_before = shapeHash(&store);
        const eb = store.family(Fam.ID).encoded_bytes;

        var ks = view_mod.KeySet.init(scratch.allocator());
        try ks.addRange(Fam.ID, NS, "");
        var view = View.init(&store, scratch.allocator(), &ks);
        try mixed(&view);
        failing.fail_index = failing.alloc_index + fail_at;
        const r = view.commit();
        failing.fail_index = std.math.maxInt(usize);
        const after = try bytesOf(&store);
        defer testing.allocator.free(after);
        if (r) |_| {
            var rv = view_mod.ReadView{ .store = &store };
            var kb: [32]u8 = undefined;
            try testing.expectEqualStrings("A2-longer-value", (try rv.get(Fam, k(&kb, "a"))).?.b);
            try testing.expect((try rv.get(Fam, k(&kb, "b"))) == null);
            try testing.expect((try rv.get(Fam, k(&kb, "x"))) == null);
            try testing.expectEqualStrings("C2", (try rv.get(Fam, k(&kb, "c"))).?.b);
            try testing.expectEqualStrings("Y2", (try rv.get(Fam, k(&kb, "y"))).?.b);
            try testing.expectEqualStrings("", (try rv.get(Fam, k(&kb, "z"))).?.b);
            try testing.expectEqual(@as(u64, 5), store.family(Fam.ID).count);
            try testing.expectEqual(@as(u64, after.len - 10), store.family(Fam.ID).encoded_bytes);
            break;
        } else |err| {
            try testing.expectEqual(error.OutOfMemory, err);
            try testing.expectEqualSlices(u8, before, after);
            try testing.expectEqual(shape_before, shapeHash(&store));
            try testing.expectEqual(eb, store.family(Fam.ID).encoded_bytes);
        }
    }
    try testing.expect(fail_at >= 5);
}

test "contract: a range over staged and committed writes takes each key's last staged write" {
    var store = Store.init(testing.allocator);
    defer store.deinit();
    var scratch = std.heap.ArenaAllocator.init(testing.allocator);
    defer scratch.deinit();
    try seed(&store, &scratch);
    var ks = view_mod.KeySet.init(scratch.allocator());
    try ks.addRange(Fam.ID, NS, "");
    var view = View.init(&store, scratch.allocator(), &ks);
    try mixed(&view);
    var pb: [8]u8 = undefined;
    var r = try view.range(Fam, k(&pb, ""));
    const want = [_][2][]const u8{ .{ "a", "A2-longer-value" }, .{ "c", "C2" }, .{ "d", "d" }, .{ "y", "Y2" }, .{ "z", "" } };
    for (want) |w| {
        const it = (try r.next()).?;
        try testing.expectEqualStrings(w[0], it.key[2..]);
        try testing.expectEqualStrings(w[1], it.value.b);
    }
    try testing.expect((try r.next()) == null);
    // A narrower prefix.
    var r2 = try view.range(Fam, k(&pb, "c"));
    try testing.expectEqualStrings("C2", (try r2.next()).?.value.b);
    try testing.expect((try r2.next()) == null);
    // A staged delete of the only key under the prefix.
    var r3 = try view.range(Fam, k(&pb, "b"));
    try testing.expect((try r3.next()) == null);
}

test "contract: random entries, a third of them refused, leave the same state and shape as a plain map, on two allocators" {
    var prng = std.Random.DefaultPrng.init(0xBEEF);
    const rand = prng.random();
    var model: std.StringArrayHashMapUnmanaged([]const u8) = .empty;
    defer {
        for (model.keys(), model.values()) |kk, vv| {
            testing.allocator.free(kk);
            testing.allocator.free(vv);
        }
        model.deinit(testing.allocator);
    }
    var arena_y = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_y.deinit();
    var x = Store.init(testing.allocator);
    defer x.deinit();
    var y = Store.init(arena_y.allocator());
    defer y.deinit();
    var sx = std.heap.ArenaAllocator.init(testing.allocator);
    defer sx.deinit();
    var sy = std.heap.ArenaAllocator.init(testing.allocator);
    defer sy.deinit();
    var junk: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (junk.items) |j| testing.allocator.free(j);
        junk.deinit(testing.allocator);
    }

    for (0..400) |_| {
        _ = sx.reset(.retain_capacity);
        _ = sy.reset(.retain_capacity);
        var kx = view_mod.KeySet.init(sx.allocator());
        try kx.addRange(Fam.ID, NS, "");
        var ky = view_mod.KeySet.init(sy.allocator());
        try ky.addRange(Fam.ID, NS, "");
        var vx = View.init(&x, sx.allocator(), &kx);
        var vy = View.init(&y, sy.allocator(), &ky);
        // Other addresses on y.
        _ = try arena_y.allocator().alloc(u8, rand.uintLessThan(usize, 200) + 1);
        try junk.append(testing.allocator, try testing.allocator.alloc(u8, rand.uintLessThan(usize, 300) + 1));
        var staged: std.StringArrayHashMapUnmanaged(?[]const u8) = .empty;
        defer staged.deinit(testing.allocator);
        const nops = rand.uintLessThan(usize, 8) + 1;
        var names: [8][2]u8 = undefined;
        var vals: [8][4]u8 = undefined;
        for (0..nops) |i| {
            names[i] = .{ 'a' + rand.uintLessThan(u8, 6), 'a' + rand.uintLessThan(u8, 3) };
            const kb_len = rand.uintLessThan(usize, 2) + 1;
            const name = names[i][0..kb_len];
            var kb: [16]u8 = undefined;
            const kk = k(&kb, name);
            const kcopy = try sx.allocator().dupe(u8, kk);
            if (rand.boolean()) {
                vals[i] = .{ 'v', 'a' + rand.uintLessThan(u8, 26), 'x', 'y' };
                const v = vals[i][0 .. rand.uintLessThan(usize, 4) + 1];
                try vx.put(Fam, kk, .{ .b = v });
                try vy.put(Fam, kk, .{ .b = v });
                try staged.put(testing.allocator, kcopy, try sx.allocator().dupe(u8, v));
            } else {
                try vx.delete(Fam, kk);
                try vy.delete(Fam, kk);
                try staged.put(testing.allocator, kcopy, null);
            }
        }
        // The view's range is the map with this entry's writes over it.
        {
            var expect: std.StringArrayHashMapUnmanaged([]const u8) = .empty;
            defer expect.deinit(testing.allocator);
            for (model.keys(), model.values()) |kk, vv| try expect.put(testing.allocator, kk, vv);
            for (staged.keys(), staged.values()) |kk, vv| {
                if (vv) |v| try expect.put(testing.allocator, kk, v) else _ = expect.orderedRemove(kk);
            }
            const C = struct {
                keys: [][]const u8,
                pub fn lessThan(c: @This(), a: usize, b: usize) bool {
                    return std.mem.order(u8, c.keys[a], c.keys[b]) == .lt;
                }
            };
            expect.sort(C{ .keys = expect.keys() });
            var pb: [4]u8 = undefined;
            var r = try vx.range(Fam, k(&pb, ""));
            for (expect.keys(), expect.values()) |kk, vv| {
                const it = (try r.next()).?;
                try testing.expectEqualStrings(kk, it.key);
                try testing.expectEqualStrings(vv, it.value.b);
            }
            try testing.expect((try r.next()) == null);
        }
        // Refused: the staged writes are dropped.
        if (rand.uintLessThan(u8, 3) == 0) continue;
        try vx.commit();
        try vy.commit();
        for (staged.keys(), staged.values()) |kk, vv| {
            if (vv) |v| {
                const gop = try model.getOrPut(testing.allocator, kk);
                if (gop.found_existing) {
                    testing.allocator.free(gop.value_ptr.*);
                } else gop.key_ptr.* = try testing.allocator.dupe(u8, kk);
                gop.value_ptr.* = try testing.allocator.dupe(u8, v);
            } else if (model.fetchSwapRemove(kk)) |old| {
                testing.allocator.free(old.key);
                testing.allocator.free(old.value);
            }
        }
        const bx = try bytesOf(&x);
        defer testing.allocator.free(bx);
        const by = try bytesOf(&y);
        defer testing.allocator.free(by);
        try testing.expectEqualSlices(u8, bx, by);
        try testing.expectEqual(shapeHash(&x), shapeHash(&y));
        try testing.expectEqual(@as(u64, model.count()), x.family(Fam.ID).count);
        try testing.expectEqual(@as(u64, bx.len - 10), x.family(Fam.ID).encoded_bytes);
    }
}
