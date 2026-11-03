//! The key set's and the view's two lookup paths, a scan for a few keys
//! and an index past `SCAN_LIMIT`, checked against a model over random
//! entries on both sides of the limit, and under allocation failure.
const std = @import("std");
const testing = std.testing;
const store_mod = @import("store.zig");
const view_mod = @import("view.zig");
const Store = store_mod.Store;
const View = view_mod.View;

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
const F1 = view_mod.Family(1, "p.one", Count);
const F2 = view_mod.Family(2, "p.two", Count);
const NSS = [_][]const u8{ "t", "ta", "u" };

const U = struct { fam: u16, ns: []const u8, name: []const u8, full: []const u8 };

fn randName(rand: std.Random, buf: []u8) []const u8 {
    const len = rand.uintLessThan(usize, 4) + 1;
    for (buf[0..len]) |*c| c.* = "abc"[rand.uintLessThan(usize, 3)];
    return buf[0..len];
}

fn vget(v: *View, fam: u16, k: []const u8) !?u64 {
    const r = if (fam == 1) try v.get(F1, k) else try v.get(F2, k);
    return if (r) |c| c.n else null;
}
fn vput(v: *View, fam: u16, k: []const u8, n: u64) !void {
    if (fam == 1) try v.put(F1, k, .{ .n = n }) else try v.put(F2, k, .{ .n = n });
}
fn vdel(v: *View, fam: u16, k: []const u8) !void {
    if (fam == 1) try v.delete(F1, k) else try v.delete(F2, k);
}

const Pair = struct { key: []const u8, n: u64 };
fn pairLess(_: void, a: Pair, b: Pair) bool {
    return std.mem.order(u8, a.key, b.key) == .lt;
}

fn rangeCheck(v: *View, fam: u16, prefix: []const u8, want: []const Pair) !void {
    var i: usize = 0;
    if (fam == 1) {
        var r = try v.range(F1, prefix);
        while (try r.next()) |it| : (i += 1) {
            try testing.expect(i < want.len);
            try testing.expectEqualSlices(u8, want[i].key, it.key);
            try testing.expectEqual(want[i].n, it.value.n);
        }
    } else {
        var r = try v.range(F2, prefix);
        while (try r.next()) |it| : (i += 1) {
            try testing.expect(i < want.len);
            try testing.expectEqualSlices(u8, want[i].key, it.key);
            try testing.expectEqual(want[i].n, it.value.n);
        }
    }
    try testing.expectEqual(want.len, i);
}

/// One case of `n_lo..n_hi` keys. Returns error.OutOfMemory when the
/// arena's backing failed.
fn runCase(seed: u64, backing: std.mem.Allocator, check_oom_state: bool, n_lo: usize, n_hi: usize) !void {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();
    var meta = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta.deinit();
    const ma = meta.allocator();

    // Universe of n distinct (fam, full key).
    const n = n_lo + rand.uintLessThan(usize, n_hi - n_lo + 1);
    var uni: std.ArrayListUnmanaged(U) = .empty;
    while (uni.items.len < n) {
        var nb: [8]u8 = undefined;
        const fam: u16 = if (rand.boolean()) 1 else 2;
        const ns = NSS[rand.uintLessThan(usize, NSS.len)];
        const name = try ma.dupe(u8, randName(rand, &nb));
        var kb: [64]u8 = undefined;
        const full = try ma.dupe(u8, try view_mod.key(&kb, ns, name));
        var dup = false;
        for (uni.items) |u| if (u.fam == fam and std.mem.eql(u8, u.full, full)) {
            dup = true;
        };
        if (!dup) try uni.append(ma, .{ .fam = fam, .ns = ns, .name = name, .full = full });
    }

    // ---- KeySet vs brute force, checked after every declaration.
    {
        var scratch = std.heap.ArenaAllocator.init(backing);
        defer scratch.deinit();
        var ks = view_mod.KeySet.init(scratch.allocator());
        var exact: std.ArrayListUnmanaged(usize) = .empty;
        var ranges: std.ArrayListUnmanaged(struct { fam: u16, full: []const u8 }) = .empty;
        // Past the universe, so every key may be declared and the limit is
        // crossed when n is past it; duplicates are likely.
        const ndecl = rand.uintLessThan(usize, n + n / 4 + 10) + 1;
        for (0..ndecl) |d| {
            if (rand.uintLessThan(u8, 8) == 0) {
                const fam: u16 = if (rand.boolean()) 1 else 2;
                const ns = NSS[rand.uintLessThan(usize, NSS.len)];
                var nb: [8]u8 = undefined;
                const p = if (rand.boolean()) "" else randName(rand, &nb)[0..1];
                var kb: [64]u8 = undefined;
                try ranges.append(ma, .{ .fam = fam, .full = try ma.dupe(u8, try view_mod.key(&kb, ns, p)) });
                try ks.addRange(fam, ns, p);
            } else {
                const i = rand.uintLessThan(usize, uni.items.len); // duplicates likely
                try exact.append(ma, i);
                try ks.add(uni.items[i].fam, uni.items[i].ns, uni.items[i].name);
            }
            // After each declaration, the keys declared so far and a few
            // others; every key every 32 declarations and at the end.
            const every = d % 32 == 31 or d + 1 == ndecl;
            for (uni.items, 0..) |u, ui| for ([_]u16{ 1, 2, 3 }) |qfam| {
                if (!every and !(exact.items.len > 0 and ui == exact.items[exact.items.len - 1]) and rand.uintLessThan(u8, 64) != 0) continue;
                var want = false;
                for (exact.items) |i| if (uni.items[i].fam == qfam and std.mem.eql(u8, uni.items[i].full, u.full)) {
                    want = true;
                };
                for (ranges.items) |r| if (r.fam == qfam and std.mem.startsWith(u8, u.full, r.full)) {
                    want = true;
                };
                if (want != ks.covers(qfam, u.full)) {
                    std.debug.print("covers diverged seed={d} fam={d} key={any} exact={d}\n", .{ seed, qfam, u.full, exact.items.len });
                    return error.Diverged;
                }
            };
        }
    }

    // ---- View vs model.
    var store = Store.init(testing.allocator);
    defer store.deinit();
    store.strict_keys = true;
    const committed = try ma.alloc(?u64, uni.items.len);
    @memset(committed, null);
    {
        var s0 = std.heap.ArenaAllocator.init(testing.allocator);
        defer s0.deinit();
        var ks = view_mod.KeySet.init(s0.allocator());
        for (NSS) |ns| for ([_]u16{ 1, 2 }) |f| try ks.addRange(f, ns, "");
        var v = View.init(&store, s0.allocator(), &ks);
        for (uni.items, 0..) |u, i| if (rand.boolean()) {
            committed[i] = rand.int(u16);
            try vput(&v, u.fam, u.full, committed[i].?);
        };
        try v.commit();
    }
    const staged = try ma.alloc(?(?u64), uni.items.len);
    @memset(staged, null);

    var scratch = std.heap.ArenaAllocator.init(backing);
    defer scratch.deinit();
    var ks = view_mod.KeySet.init(scratch.allocator());
    // Declare every universe key exactly (so covers uses the index past the
    // limit), and ranges so range() is allowed.
    for (uni.items) |u| try ks.add(u.fam, u.ns, u.name);
    for (NSS) |ns| for ([_]u16{ 1, 2 }) |f| try ks.addRange(f, ns, "");
    var v = View.init(&store, scratch.allocator(), &ks);
    const nops = rand.uintLessThan(usize, 3 * n + 4) + 1;
    var result: anyerror!void = {};
    ops: for (0..nops) |_| {
        const i = rand.uintLessThan(usize, uni.items.len);
        const u = uni.items[i];
        switch (rand.uintLessThan(u8, 10)) {
            0...3 => {
                const val: u64 = rand.int(u16);
                vput(&v, u.fam, u.full, val) catch |e| {
                    result = e;
                    break :ops;
                };
                staged[i] = val;
            },
            4...5 => {
                vdel(&v, u.fam, u.full) catch |e| {
                    result = e;
                    break :ops;
                };
                staged[i] = @as(?u64, null);
            },
            6...8 => {
                const want: ?u64 = if (staged[i]) |s| s else committed[i];
                const got = vget(&v, u.fam, u.full) catch |e| {
                    result = e;
                    break :ops;
                };
                if (want != got) {
                    std.debug.print("get diverged seed={d}\n", .{seed});
                    return error.Diverged;
                }
            },
            else => {
                const ns = NSS[rand.uintLessThan(usize, NSS.len)];
                var nb: [8]u8 = undefined;
                const rn = randName(rand, &nb);
                const p = if (rand.boolean()) "" else rn[0..@min(rn.len, rand.uintLessThan(usize, 2) + 1)];
                var kb: [64]u8 = undefined;
                const prefix = try view_mod.key(&kb, ns, p);
                var want: std.ArrayListUnmanaged(Pair) = .empty;
                for (uni.items, 0..) |w, j| {
                    if (w.fam != u.fam or !std.mem.startsWith(u8, w.full, prefix)) continue;
                    const val: ?u64 = if (staged[j]) |s| s else committed[j];
                    if (val) |x| try want.append(ma, .{ .key = w.full, .n = x });
                }
                std.mem.sort(Pair, want.items, {}, pairLess);
                rangeCheck(&v, u.fam, prefix, want.items) catch |e| switch (e) {
                    error.OutOfMemory => {
                        result = e;
                        break :ops;
                    },
                    else => {
                        std.debug.print("range diverged seed={d}\n", .{seed});
                        return e;
                    },
                };
            },
        }
    }
    var did_commit = false;
    if (result) |_| {
        if (rand.uintLessThan(u8, 5) != 0) {
            result = v.commit();
            did_commit = if (result) |_| true else |_| false;
        }
    } else |_| {}
    if (result) |_| {} else |e| if (e != error.OutOfMemory) return e;
    if (!check_oom_state and !did_commit and result == error.OutOfMemory) return error.OutOfMemory;
    // Store must equal: model-after if committed, model-before otherwise.
    var rv = view_mod.ReadView{ .store = &store };
    var count: [3]u64 = .{ 0, 0, 0 };
    for (uni.items, 0..) |u, i| {
        const want: ?u64 = if (did_commit) (if (staged[i]) |s| s else committed[i]) else committed[i];
        const got = if (u.fam == 1) try rv.get(F1, u.full) else try rv.get(F2, u.full);
        const gn: ?u64 = if (got) |c| c.n else null;
        if (want != gn) {
            std.debug.print("state diverged seed={d} committed={}\n", .{ seed, did_commit });
            return error.Diverged;
        }
        if (want != null) count[u.fam] += 1;
    }
    try testing.expectEqual(count[1], store.family(1).count);
    try testing.expectEqual(count[2], store.family(2).count);
    if (result) |_| {} else |e| return e;
}

test "contract: a key set and a view answer as a model does, scanning or indexed" {
    // Half the cases below the limit, half across it.
    var seed: u64 = 0;
    while (seed < 200) : (seed += 1) {
        if (seed % 2 == 0) try runCase(seed, testing.allocator, true, 1, 60) else try runCase(seed, testing.allocator, true, 100, 300);
    }
}

test "contract: an allocation failing anywhere in an entry leaves state unchanged and leaks nothing" {
    // Small entries at every allocation; across the limit, at a stride.
    for ([_]struct { seeds: u64, lo: usize, hi: usize, stride: usize }{
        .{ .seeds = 20, .lo = 1, .hi = 60, .stride = 1 },
        .{ .seeds = 3, .lo = 129, .hi = 150, .stride = 37 },
    }, 0..) |c, ci| {
        var seed: u64 = 100_000 + ci * 1000;
        while (seed < 100_000 + ci * 1000 + c.seeds) : (seed += 1) {
            var fail_at: usize = 0;
            while (fail_at < 200_000) : (fail_at += c.stride) {
                var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = fail_at });
                runCase(seed, failing.allocator(), true, c.lo, c.hi) catch |e| switch (e) {
                    error.OutOfMemory => continue,
                    else => return e,
                };
                break;
            }
        }
    }
}
