//! KV's apply and reads on the contract: answers, versions and history,
//! TTLs by the entry's stamp, transactions applied whole or not at all,
//! and per-namespace counts kept by the same entries.

const std = @import("std");
const testing = std.testing;
const entry_mod = @import("../storage/ual/entry.zig");
const kv_entries = @import("../protocol/kv_entries.zig");
const kv_ops = @import("../protocol/kv_ops.zig");
const schema = @import("../protocol/schema.zig");
const proto = @import("../protocol/proto.zig");
const txn = @import("txn.zig");
const machine = @import("machine.zig");
const state_mod = @import("state.zig");

const TestKv = machine.TestKv;
const Entry = entry_mod.Entry;
const Version = machine.Version;

/// KV entry `entry_type` carrying `payload`, at `index`, stamped `stamp`.
fn testEntry(buf: []u8, comptime entry_type: entry_mod.EntryType, payload: kv_entries.Payload(@backingInt(entry_type)).Value, index: u64, stamp: u64) Entry {
    const bytes = kv_entries.Payload(@backingInt(entry_type)).encodeInto(buf, payload) catch unreachable;
    return entry_mod.buildEntry(entry_type, 0, 1, index, stamp, bytes);
}

/// A put of `value` at `key`, with a TTL unless it is 0.
fn putEntry(buf: []u8, key: []const u8, value: []const u8, index: u64, stamp: u64, ttl_ms: u64) Entry {
    return testEntry(buf, .kv_put, .{ .key = key, .value = value, .ttl_ms = if (ttl_ms == 0) null else ttl_ms }, index, stamp);
}

const TestReadClock = struct {
    ns: u64,

    fn read(ctx: ?*const anyopaque) u64 {
        const self: *const TestReadClock = @ptrCast(@alignCast(ctx.?));
        return self.ns;
    }
};

fn historyOf(kv: *TestKv, key: []const u8, before: ?u64, out: []Version) usize {
    var it = kv.state().history(key, before);
    var n: usize = 0;
    while (it.next()) |v| : (n += 1) {
        if (n == out.len) break;
        out[n] = v;
    }
    return n;
}

const STAMP: u64 = 1_000 * std.time.ns_per_s;

test "kv machine: an apply answers from what it did: the version set, whether a delete found the key, a refused incr" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    var buf: [256]u8 = undefined;

    var r = try kv.apply(&putEntry(&buf, "k", "v", 1, STAMP, 0));
    var rd = r.reader();
    try testing.expectEqual(@as(?u64, 1), rd.int(u64));
    r = try kv.apply(&putEntry(&buf, "k", "w", 2, STAMP, 0));
    rd = r.reader();
    try testing.expectEqual(@as(?u64, 2), rd.int(u64));

    // A delete says whether the key was there when it applied.
    r = try kv.apply(&testEntry(&buf, .kv_delete, .{ .key = "k" }, 3, STAMP));
    rd = r.reader();
    try testing.expectEqual(@as(?u8, 1), rd.int(u8));
    r = try kv.apply(&testEntry(&buf, .kv_delete, .{ .key = "k" }, 4, STAMP));
    rd = r.reader();
    try testing.expectEqual(@as(?u8, 0), rd.int(u8));
    // One whose TTL ran out by the delete's stamp wasn't there either.
    _ = try kv.apply(&putEntry(&buf, "t", "v", 5, STAMP, 1_000));
    r = try kv.apply(&testEntry(&buf, .kv_delete, .{ .key = "t" }, 6, STAMP + 2 * std.time.ns_per_s));
    rd = r.reader();
    try testing.expectEqual(@as(?u8, 0), rd.int(u8));

    // An incr that would overflow changes nothing and is refused.
    var max: [8]u8 = undefined;
    std.mem.writeInt(i64, &max, std.math.maxInt(i64), .little);
    _ = try kv.apply(&putEntry(&buf, "c", &max, 7, STAMP, 0));
    r = try kv.apply(&testEntry(&buf, .kv_incr, .{ .key = "c", .delta = 1 }, 8, STAMP));
    try testing.expectEqual(proto.StatusCode.bad_request, r.refused().?.status);
    try testing.expectEqualStrings(machine.INCR_OVERFLOW, r.refused().?.refusal.message);
    try testing.expectEqual(std.math.maxInt(i64), std.mem.readInt(i64, kv.state().get("c").?.value[0..8], .little));
    try testing.expectEqual(@as(u64, 1), kv.state().get("c").?.version);
    // One on a value that isn't a counter is refused too.
    _ = try kv.apply(&putEntry(&buf, "s", "text", 9, STAMP, 0));
    r = try kv.apply(&testEntry(&buf, .kv_incr, .{ .key = "s", .delta = 1 }, 10, STAMP));
    try testing.expectEqualStrings(machine.NOT_A_COUNTER, r.refused().?.refusal.message);
    // One that applies answers the key's version and value.
    r = try kv.apply(&testEntry(&buf, .kv_incr, .{ .key = "n", .delta = 5 }, 11, STAMP));
    rd = r.reader();
    try testing.expectEqual(@as(?u64, 1), rd.int(u64));
    try testing.expectEqual(@as(?i64, 5), rd.int(i64));
    // An expire of a key that's gone is refused as not found.
    r = try kv.apply(&testEntry(&buf, .kv_expire, .{ .key = "gone", .ttl_ms = 10 }, 12, STAMP));
    try testing.expectEqual(proto.StatusCode.not_found, r.refused().?.status);
}

test "kv machine: history is the current version, then the older ones newest first, from below a version when given" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    try kv.put("k", "v1", 100);
    try kv.put("k", "v2", 200);
    try kv.put("k", "v3", 300);

    var hist: [10]Version = undefined;
    try testing.expectEqual(@as(usize, 3), historyOf(&kv, "k", null, &hist));
    for (hist[0..3], [_][]const u8{ "v3", "v2", "v1" }, [_]u64{ 3, 2, 1 }) |v, value, version| {
        try testing.expectEqualStrings(value, v.value);
        try testing.expectEqual(version, v.version);
    }
    try testing.expectEqual(@as(usize, 2), historyOf(&kv, "k", 3, &hist));
    try testing.expectEqual(@as(u64, 2), hist[0].version);
    try testing.expectEqual(@as(usize, 0), historyOf(&kv, "k", 1, &hist));
    try testing.expect(kv.state().history("k", 1).exists());
    try testing.expect(!kv.state().history("missing", null).exists());
    // Another key that starts with this one keeps its own history.
    try kv.put("kk", "x", 400);
    try testing.expectEqual(@as(usize, 3), historyOf(&kv, "k", null, &hist));
}

test "kv machine: a delete keeps the history, the live value before it included" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    var buf: [64]u8 = undefined;
    try kv.put("k", "v1", 100);
    _ = try kv.apply(&testEntry(&buf, .kv_delete, .{ .key = "k" }, 2, 200));

    var hist: [10]Version = undefined;
    try testing.expectEqual(@as(usize, 2), historyOf(&kv, "k", null, &hist));
    try testing.expect(hist[0].tombstone);
    try testing.expectEqual(@as(u64, 2), hist[0].version);
    try testing.expectEqualStrings("v1", hist[1].value);
    try testing.expect(!hist[1].tombstone);
}

test "kv machine: history keeps at most HISTORY_MAX_VERSIONS older versions, oldest evicted first" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    const total = kv_ops.HISTORY_MAX_VERSIONS + 3;
    for (0..total) |i| {
        var buf: [8]u8 = undefined;
        try kv.put("k", std.fmt.bufPrint(&buf, "v{d}", .{i}) catch unreachable, @intCast((i + 1) * 100));
    }
    var hist: [kv_ops.MAX_HISTORY + 1]Version = undefined;
    try testing.expectEqual(@as(usize, kv_ops.MAX_HISTORY), historyOf(&kv, "k", null, &hist));
    try testing.expectEqual(@as(u64, total), hist[0].version);
    // The oldest kept: versions 1 and 2 are gone.
    try testing.expectEqual(@as(u64, total - kv_ops.HISTORY_MAX_VERSIONS), hist[kv_ops.MAX_HISTORY - 1].version);
    const counts = kv.state().countsOf("default");
    try testing.expectEqual(@as(u64, 2), counts.history_evicted);
    var charged: u64 = 0;
    for (hist[1..kv_ops.MAX_HISTORY]) |v| charged += v.value.len + kv_ops.HISTORY_ENTRY_COST;
    try testing.expectEqual(charged, counts.history_bytes);
    try testing.expectEqual(@as(u64, kv_ops.HISTORY_MAX_VERSIONS), kv.store.family(machine.History.ID).count);
}

test "kv machine: history keeps at most HISTORY_MAX_BYTES, but always the newest older version" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    // 4 KiB values: 15 fit under 64 KiB with their 64-byte charges.
    const value: [4096]u8 = @splat('s');
    for (0..20) |_| try kv.put("k", &value, 0);
    var hist: [kv_ops.MAX_HISTORY]Version = undefined;
    const per = value.len + kv_ops.HISTORY_ENTRY_COST;
    const kept = kv_ops.HISTORY_MAX_BYTES / per;
    try testing.expectEqual(1 + kept, historyOf(&kv, "k", null, &hist));
    try testing.expectEqual(@as(u64, kept * per), kv.state().countsOf("default").history_bytes);
    try testing.expectEqual(@as(u64, 19 - kept), kv.state().countsOf("default").history_evicted);

    // One value larger than the whole budget evicts everything older, and
    // is itself kept once something overwrites it.
    const big = try testing.allocator.alloc(u8, kv_ops.HISTORY_MAX_BYTES + 1);
    defer testing.allocator.free(big);
    @memset(big, 'b');
    var buf: [kv_ops.HISTORY_MAX_BYTES + 64]u8 = undefined;
    _ = try kv.apply(&putEntry(&buf, "k", big, kv.index + 1, 0, 0));
    try testing.expectEqual(@as(u64, 20 - kept), kv.state().countsOf("default").history_evicted);
    try kv.put("k", "after", 0);
    try testing.expectEqual(@as(u64, 20), kv.state().countsOf("default").history_evicted);
    try testing.expectEqual(@as(usize, 2), historyOf(&kv, "k", null, &hist));
    try testing.expectEqual(big.len, hist[1].value.len);
    try testing.expectEqual(@as(u64, big.len + kv_ops.HISTORY_ENTRY_COST), kv.state().countsOf("default").history_bytes);
}

test "kv machine: a namespace's counts follow its keys: live keys, bytes and history" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    var buf: [64]u8 = undefined;
    try kv.put("ns\x00a", "123456", 0);
    try testing.expectEqual(machine.Counts{ .live = 1, .bytes = 10 }, kv.state().countsOf("ns"));
    try kv.put("ns\x00a", "x", 0);
    try testing.expectEqual(machine.Counts{ .live = 1, .bytes = 5, .history_bytes = 6 + kv_ops.HISTORY_ENTRY_COST }, kv.state().countsOf("ns"));
    _ = try kv.apply(&testEntry(&buf, .kv_delete, .{ .key = "ns\x00a" }, 3, 0));
    try testing.expectEqual(machine.Counts{ .live = 0, .bytes = 4, .history_bytes = 7 + 2 * kv_ops.HISTORY_ENTRY_COST }, kv.state().countsOf("ns"));
    try testing.expectEqual(machine.Counts{}, kv.state().countsOf("default"));
}

test "kv machine: a TTL runs out a TTL after the stamp of the entry that set it, on every replica alike, and reads judge it by the read clock" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    var clock: TestReadClock = .{ .ns = 0 };
    var st = kv.state();
    st.read_clock = .{ .ctx = &clock, .now_ns = TestReadClock.read };
    var buf: [128]u8 = undefined;
    _ = try kv.apply(&putEntry(&buf, "k", "v", 1, STAMP, 5_000));
    try testing.expectEqual(STAMP + 5 * std.time.ns_per_s, st.getRaw("k").?.expiry_ns);

    var names: [4][]const u8 = undefined;
    clock.ns = STAMP + 5 * std.time.ns_per_s - 1;
    try testing.expect(st.get("k") != null);
    try testing.expectEqual(@as(usize, 1), st.scanKeyNames("default", "", &names));
    clock.ns += 1;
    try testing.expect(st.get("k") == null);
    try testing.expectEqual(@as(usize, 0), st.scanKeyNames("default", "", &names));

    // An expire counts from its own entry's stamp; a TTL of 0 clears it.
    _ = try kv.apply(&putEntry(&buf, "t", "v", 2, STAMP, 1_000));
    _ = try kv.apply(&testEntry(&buf, .kv_expire, .{ .key = "t", .ttl_ms = 60_000 }, 3, STAMP + 500 * std.time.ns_per_ms));
    try testing.expectEqual(STAMP + 60_500 * std.time.ns_per_ms, st.getRaw("t").?.expiry_ns);
    // An expire is no new version.
    try testing.expectEqual(@as(u64, 1), st.getRaw("t").?.version);
    _ = try kv.apply(&testEntry(&buf, .kv_expire, .{ .key = "t", .ttl_ms = 0 }, 4, STAMP + std.time.ns_per_s));
    try testing.expectEqual(@as(u64, 0), st.getRaw("t").?.expiry_ns);
}

test "kv machine: a key whose TTL ran out by an entry's stamp is gone to that entry: incr starts again from zero and expire finds nothing" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    const st = kv.state();
    var buf: [128]u8 = undefined;
    var counter: [8]u8 = undefined;
    std.mem.writeInt(i64, &counter, 7, .little);
    _ = try kv.apply(&putEntry(&buf, "c", &counter, 1, STAMP, 1_000));
    _ = try kv.apply(&testEntry(&buf, .kv_incr, .{ .key = "c", .delta = 1 }, 2, STAMP + 999 * std.time.ns_per_ms));
    try testing.expectEqual(@as(i64, 8), std.mem.readInt(i64, st.getRaw("c").?.value[0..8], .little));
    _ = try kv.apply(&putEntry(&buf, "c", &counter, 3, STAMP + 2 * std.time.ns_per_s, 1_000));
    _ = try kv.apply(&testEntry(&buf, .kv_incr, .{ .key = "c", .delta = 1 }, 4, STAMP + 3 * std.time.ns_per_s));
    try testing.expectEqual(@as(i64, 1), std.mem.readInt(i64, st.getRaw("c").?.value[0..8], .little));
    try testing.expectEqual(@as(u64, 0), st.getRaw("c").?.expiry_ns);

    _ = try kv.apply(&putEntry(&buf, "t", "v", 5, STAMP, 1_000));
    const r = try kv.apply(&testEntry(&buf, .kv_expire, .{ .key = "t", .ttl_ms = 60_000 }, 6, STAMP + std.time.ns_per_s));
    try testing.expect(r.refused() != null);
    try testing.expectEqual(STAMP + std.time.ns_per_s, st.getRaw("t").?.expiry_ns);
}

fn batchEntry(ops: []const txn.TxnOp, index: u64, stamp: u64) !struct { payload: []u8, entry: Entry } {
    const payload = try txn.encodeBatch(testing.allocator, ops);
    return .{ .payload = payload, .entry = entry_mod.buildEntry(.kv_batch, 0, 1, index, stamp, payload) };
}

test "kv machine: a transaction's TTLs count from its batch's stamp" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    var eb: [64]u8 = undefined;
    _ = try kv.apply(&putEntry(&eb, "t", "v", 1, STAMP, 0));
    const later = STAMP + 10 * std.time.ns_per_s;
    const b = try batchEntry(&.{
        .{ .kind = .put, .key = @constCast("p"), .value = @constCast("v"), .ttl_ms = 2_000 },
        .{ .kind = .expire, .key = @constCast("t"), .value = @constCast(""), .ttl_ms = 30_000 },
    }, 2, later);
    defer testing.allocator.free(b.payload);
    _ = try kv.apply(&b.entry);
    try testing.expectEqual(later + 2 * std.time.ns_per_s, kv.state().getRaw("p").?.expiry_ns);
    try testing.expectEqual(later + 30 * std.time.ns_per_s, kv.state().getRaw("t").?.expiry_ns);
}

test "kv machine: a transaction whose incr or expire fails applies none of its writes and says which op" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    var eb: [64]u8 = undefined;
    _ = try kv.apply(&putEntry(&eb, "text", "not a counter", 1, STAMP, 0));
    const before = kv.store.family(machine.Values.ID).count;
    var delta: [8]u8 = undefined;
    std.mem.writeInt(i64, &delta, 1, .little);
    for ([_]txn.TxnOp{
        .{ .kind = .incr, .key = @constCast("text"), .value = &delta, .ttl_ms = 0 },
        .{ .kind = .expire, .key = @constCast("missing"), .value = @constCast(""), .ttl_ms = 1_000 },
    }) |failing| {
        const b = try batchEntry(&.{
            .{ .kind = .put, .key = @constCast("a"), .value = @constCast("1"), .ttl_ms = 0 },
            .{ .kind = .delete, .key = @constCast("other"), .value = @constCast(""), .ttl_ms = 0 },
            failing,
        }, kv.index + 1, STAMP);
        defer testing.allocator.free(b.payload);
        const r = try kv.apply(&b.entry);
        const refusal = r.refused().?.refusal;
        try testing.expect(std.mem.indexOf(u8, refusal.message, "op 3:") != null);
        try testing.expect(std.mem.indexOf(u8, refusal.message, "nothing in the transaction applied") != null);
        try testing.expect(kv.state().get("a") == null);
        try testing.expectEqualStrings("not a counter", kv.state().get("text").?.value);
        try testing.expectEqual(before, kv.store.family(machine.Values.ID).count);
    }
}

test "kv machine: a batch that doesn't decode whole, or breaks a kind's rule, applies nothing" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    const b = try batchEntry(&.{
        .{ .kind = .put, .key = @constCast("a"), .value = @constCast("1"), .ttl_ms = 0 },
        .{ .kind = .put, .key = @constCast("b"), .value = @constCast("2"), .ttl_ms = 0 },
    }, 1, 0);
    defer testing.allocator.free(b.payload);
    // Cut inside the second op: the first must not apply on its own.
    for (1..b.payload.len) |n| {
        try testing.expectError(error.Halt, kv.apply(&entry_mod.buildEntry(.kv_batch, 0, 1, 1, 0, b.payload[0..n])));
        try testing.expect(kv.state().get("a") == null);
    }
    // A delete carrying a value decodes, but breaks its kind's rule.
    var buf: [64]u8 = undefined;
    var w: schema.Writer = .{ .buf = &buf };
    try kv_entries.BatchOps.encodeSlice(&w, &.{
        .{ .kind = .put, .key = "a", .value = "1", .ttl_ms = null, .delta = null },
        .{ .kind = .delete, .key = "b", .value = "x", .ttl_ms = null, .delta = null },
    });
    try testing.expectError(error.Halt, kv.apply(&entry_mod.buildEntry(.kv_batch, 0, 1, 1, 0, w.written())));
    try testing.expect(kv.state().get("a") == null);
}

/// Live keys counted from the values family, per namespace.
fn recountLive(kv: *TestKv, ns: []const u8) u64 {
    var pb: [machine.MAX_KEY]u8 = undefined;
    var r = (@import("../contract/view.zig").ReadView{ .store = &kv.store }).range(machine.Values, @import("../contract/view.zig").key(&pb, ns, "") catch unreachable);
    var n: u64 = 0;
    while (r.next() catch unreachable) |item| n += @intFromBool(!item.value.tombstone);
    return n;
}

test "kv machine: live keys are counted per namespace as apply makes and removes them, and a replay counts the same" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    var bufs: [12][96]u8 = undefined;
    const b = try batchEntry(&.{
        .{ .kind = .put, .key = @constCast("orders\x00b1"), .value = @constCast("v"), .ttl_ms = 0 },
        .{ .kind = .put, .key = @constCast("orders\x00b2"), .value = @constCast("v"), .ttl_ms = 0 },
        .{ .kind = .delete, .key = @constCast("orders\x00b1"), .value = @constCast(""), .ttl_ms = 0 },
    }, 10, STAMP);
    defer testing.allocator.free(b.payload);
    const Step = struct { entry: Entry, orders: u64, default: u64 };
    const steps = [_]Step{
        .{ .entry = putEntry(&bufs[0], "orders\x00a", "1", 1, STAMP, 0), .orders = 1, .default = 0 },
        .{ .entry = putEntry(&bufs[1], "orders\x00a", "2", 2, STAMP, 0), .orders = 1, .default = 0 }, // overwrite
        .{ .entry = putEntry(&bufs[2], "plain", "1", 3, STAMP, 0), .orders = 1, .default = 1 },
        .{ .entry = testEntry(&bufs[3], .kv_delete, .{ .key = "orders\x00missing" }, 4, STAMP), .orders = 1, .default = 1 },
        .{ .entry = testEntry(&bufs[4], .kv_delete, .{ .key = "orders\x00a" }, 5, STAMP), .orders = 0, .default = 1 },
        .{ .entry = testEntry(&bufs[5], .kv_delete, .{ .key = "orders\x00a" }, 6, STAMP), .orders = 0, .default = 1 }, // again
        .{ .entry = putEntry(&bufs[6], "orders\x00a", "3", 7, STAMP, 0), .orders = 1, .default = 1 }, // back from a tombstone
        .{ .entry = testEntry(&bufs[7], .kv_incr, .{ .key = "orders\x00n", .delta = 1 }, 8, STAMP), .orders = 2, .default = 1 },
        .{ .entry = testEntry(&bufs[8], .kv_incr, .{ .key = "orders\x00n", .delta = 1 }, 9, STAMP), .orders = 2, .default = 1 },
        .{ .entry = b.entry, .orders = 3, .default = 1 },
        // A TTL that runs out removes nothing: the key counts until deleted.
        .{ .entry = putEntry(&bufs[9], "plain", "t", 11, STAMP, 1), .orders = 3, .default = 1 },
        .{ .entry = testEntry(&bufs[10], .kv_expire, .{ .key = "orders\x00a", .ttl_ms = 1 }, 12, STAMP + std.time.ns_per_s), .orders = 3, .default = 1 },
    };
    for (steps) |step| {
        _ = try kv.apply(&step.entry);
        try testing.expectEqual(step.orders, kv.state().liveKeys("orders"));
        try testing.expectEqual(step.default, kv.state().liveKeys("default"));
        try testing.expectEqual(recountLive(&kv, "orders"), kv.state().liveKeys("orders"));
        try testing.expectEqual(recountLive(&kv, "default"), kv.state().liveKeys("default"));
    }

    // The same log applied again, as at boot or on a follower, holds the
    // same bytes in every family.
    var replay = TestKv.init(testing.allocator);
    defer replay.deinit();
    for (steps) |step| _ = try replay.apply(&step.entry);
    const store_mod = @import("../contract/store.zig");
    for ([_]u16{ machine.Values.ID, machine.History.ID, machine.NsCounts.ID }) |fam| {
        try testing.expectEqual(store_mod.checksum(&kv.store, fam), store_mod.checksum(&replay.store, fam));
    }
}

test "kv machine: out of memory at any allocation leaves every family as it was" {
    const run = struct {
        fn f(allocator: std.mem.Allocator) !void {
            var kv = TestKv.init(allocator);
            defer kv.deinit();
            var buf: [96]u8 = undefined;
            try kv.put("ns\x00a", "1", STAMP);
            _ = try kv.apply(&testEntry(&buf, .kv_delete, .{ .key = "ns\x00a" }, kv.index + 1, STAMP));
            try kv.put("ns\x00a", "2", STAMP);
            try kv.put("ns\x00b", "1", STAMP);
            std.testing.expectEqual(recountLive(&kv, "ns"), kv.state().liveKeys("ns")) catch @panic("count disagrees with the keys");
        }
    }.f;
    try std.testing.checkAllAllocationFailures(testing.allocator, run, .{});
}

test "kv machine: keys of one namespace never reach another's, and the default namespace's are its bare keys" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    try kv.put("a\x00k", "a-value", 0);
    try kv.put("k", "default-value", 0);
    try kv.put("ab\x00k", "ab-value", 0);
    const st = kv.state();
    try testing.expectEqualStrings("a-value", st.get("a\x00k").?.value);
    try testing.expectEqualStrings("default-value", st.get("k").?.value);
    var names: [4][]const u8 = undefined;
    try testing.expectEqual(@as(usize, 1), st.scanKeyNames("a", "", &names));
    try testing.expectEqualStrings("k", names[0]);
    try testing.expectEqual(@as(usize, 1), st.scanKeyNames("default", "", &names));
}

test "kv machine: a key with no namespace halts its entry, a put's, a patch's or a transaction's, before anything changes" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    try kv.put("k", "v", 0);
    const store_mod = @import("../contract/store.zig");
    const before = store_mod.checksum(&kv.store, machine.Values.ID);
    var buf: [64]u8 = undefined;
    try testing.expectError(error.Halt, kv.apply(&putEntry(&buf, "\x00k", "v", 2, 0, 0)));
    const b = try batchEntry(&.{
        .{ .kind = .put, .key = @constCast("a"), .value = @constCast("1"), .ttl_ms = 0 },
        .{ .kind = .delete, .key = @constCast("\x00k"), .value = @constCast(""), .ttl_ms = 0 },
    }, 3, 0);
    defer testing.allocator.free(b.payload);
    try testing.expectError(error.Halt, kv.apply(&b.entry));
    try testing.expectError(error.Halt, patch(&kv, "\x00k", "$", "{}"));
    try testing.expectEqual(before, store_mod.checksum(&kv.store, machine.Values.ID));
    try testing.expectEqual(@as(u64, 0), kv.store.undeclared_keys);
}

/// A namespace's live keys, stored bytes and history charge, counted from
/// the families themselves.
fn recount(kv: *TestKv, ns: []const u8) machine.Counts {
    const view_mod = @import("../contract/view.zig");
    const read: view_mod.ReadView = .{ .store = &kv.store };
    var pb: [machine.MAX_KEY]u8 = undefined;
    const prefix = view_mod.key(&pb, ns, "") catch unreachable;
    var c: machine.Counts = .{};
    var values = read.range(machine.Values, prefix);
    while (values.next() catch unreachable) |item| {
        const key = item.key[prefix.len..];
        // As the key was written: bare in the default namespace.
        const qlen = if (std.mem.eql(u8, ns, "default")) key.len else ns.len + 1 + key.len;
        c.live += @intFromBool(!item.value.tombstone);
        c.bytes += qlen + item.value.value.len;
    }
    var history = read.range(machine.History, prefix);
    while (history.next() catch unreachable) |item| c.history_bytes += item.value.charge();
    return c;
}

test "kv machine: random entries keep every namespace's counts equal to a recount of its keys" {
    var prng = std.Random.DefaultPrng.init(0xC0_47);
    const rand = prng.random();
    const namespaces = [_][]const u8{ "default", "a", "bb" };
    for (0..8) |_| {
        var kv = TestKv.init(testing.allocator);
        defer kv.deinit();
        var big: [20_000]u8 = undefined;
        @memset(&big, 'v');
        for (0..300) |_| {
            var kb: [32]u8 = undefined;
            const ns = namespaces[rand.uintLessThan(usize, namespaces.len)];
            const name = std.fmt.bufPrint(kb[16..], "k{d}", .{rand.uintLessThan(u8, 6)}) catch unreachable;
            const qkey = if (std.mem.eql(u8, ns, "default")) name else std.fmt.bufPrint(&kb, "{s}\x00{s}", .{ ns, name }) catch unreachable;
            const stamp = STAMP + @as(u64, rand.uintLessThan(u32, 4)) * std.time.ns_per_s;
            const buf = try testing.allocator.alloc(u8, 24_000);
            defer testing.allocator.free(buf);
            var delta: [8]u8 = undefined;
            std.mem.writeInt(i64, &delta, rand.intRangeAtMost(i64, -3, 3), .little);
            switch (rand.uintLessThan(u8, 6)) {
                0, 1 => _ = try kv.apply(&putEntry(buf, qkey, big[0..rand.uintLessThan(usize, big.len)], kv.index + 1, stamp, if (rand.boolean()) 0 else 1_500)),
                2 => _ = try kv.apply(&testEntry(buf, .kv_delete, .{ .key = qkey }, kv.index + 1, stamp)),
                3 => _ = try kv.apply(&testEntry(buf, .kv_incr, .{ .key = qkey, .delta = std.mem.readInt(i64, &delta, .little) }, kv.index + 1, stamp)),
                4 => _ = try kv.apply(&testEntry(buf, .kv_expire, .{ .key = qkey, .ttl_ms = rand.uintLessThan(u32, 3_000) }, kv.index + 1, stamp)),
                else => {
                    const b = try batchEntry(&.{
                        .{ .kind = .put, .key = @constCast(qkey), .value = big[0..rand.uintLessThan(usize, 4_000)], .ttl_ms = 0 },
                        .{ .kind = .incr, .key = @constCast(qkey), .value = &delta, .ttl_ms = 0 },
                    }, kv.index + 1, stamp);
                    defer testing.allocator.free(b.payload);
                    _ = try kv.apply(&b.entry);
                },
            }
            for (namespaces) |n| {
                const kept = kv.state().countsOf(n);
                const counted = recount(&kv, n);
                try testing.expectEqual(counted.live, kept.live);
                try testing.expectEqual(counted.bytes, kept.bytes);
                try testing.expectEqual(counted.history_bytes, kept.history_bytes);
            }
        }
    }
}

fn patch(kv: *TestKv, key: []const u8, path: []const u8, document: ?[]const u8) !@import("../storage/persistence.zig").Result {
    var buf: [512]u8 = undefined;
    return kv.apply(&testEntry(&buf, .kv_json_patch, .{ .key = key, .path = path, .document = document }, kv.index + 1, STAMP));
}

test "kv machine: JSON patches merge at apply, so two built from the same document both land" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    _ = try patch(&kv, "doc", "$", "{\"a\":0,\"b\":0}");
    // Both clients read version 1 and proposed: each change merges with
    // what the entry before it left.
    _ = try patch(&kv, "doc", "$.a", "1");
    var r = try patch(&kv, "doc", "$.b", "2");
    var rd = r.reader();
    try testing.expectEqual(@as(?u64, 3), rd.int(u64));
    const json_path = @import("../util/json_path.zig");
    for ([_][2][]const u8{ .{ "$.a", "1" }, .{ "$.b", "2" } }) |want| {
        const got = try json_path.jsonPathGet(testing.allocator, kv.state().get("doc").?.value, want[0]);
        defer testing.allocator.free(got);
        try testing.expectEqualStrings(want[1], got);
    }
    try testing.expectEqual(machine.ValueType.json, kv.state().get("doc").?.value_type);
}

test "kv machine: a JSON patch sets $ on a missing key, deletes the key at $, and refuses what the merge can't take" {
    var kv = TestKv.init(testing.allocator);
    defer kv.deinit();
    // Not there: only the whole document can be set.
    try testing.expectEqual(proto.Reason.not_found, (try patch(&kv, "doc", "$.a", "1")).refused().?.refusal.reason);
    try testing.expectEqual(proto.Reason.not_found, (try patch(&kv, "doc", "$", null)).refused().?.refusal.reason);
    try testing.expect(kv.state().getRaw("doc") == null);
    _ = try patch(&kv, "doc", "$", "{\"a\":{\"x\":1}}");
    // A path with nothing at it, or a value that isn't JSON: refused, unchanged.
    try testing.expectEqual(proto.Reason.not_found, (try patch(&kv, "doc", "$.nope.deeper", "1")).refused().?.refusal.reason);
    try kv.put("text", "not json", STAMP);
    try testing.expectEqual(proto.Reason.wrong_type, (try patch(&kv, "text", "$.a", "1")).refused().?.refusal.reason);
    try testing.expectEqual(@as(u64, 1), kv.state().get("doc").?.version);
    // A path deleted leaves the rest; $ deleted takes the key, answering 0.
    var r = try patch(&kv, "doc", "$.a.x", null);
    var rd = r.reader();
    try testing.expectEqual(@as(?u64, 2), rd.int(u64));
    try testing.expectEqualStrings("{\"a\":{}}", kv.state().get("doc").?.value);
    r = try patch(&kv, "doc", "$", null);
    rd = r.reader();
    try testing.expectEqual(@as(?u64, 0), rd.int(u64));
    try testing.expect(kv.state().get("doc") == null);
    // "text" is the one live key left.
    try testing.expectEqual(@as(u64, 1), kv.state().liveKeys("default"));
}
