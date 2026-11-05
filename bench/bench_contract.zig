//! Benchmark: the contract's apply path alone.
//!
//! Puts, gets and 100-key prefix reads through a machine whose apply
//! stages one put on a `View` and commits it: the cost of staging, the
//! ordered map and the commit, with no family logic of its own. Then one
//! entry that declares, reads and writes n keys: the cost of an entry's
//! key checks and staged lookups as it grows.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");

const contract = src.contract;
const entry_mod = src.storage.ual.entry;
const persistence = src.storage.persistence;

const ITERS = 200_000;
const READS = 10_000;

const Bytes = struct {
    b: []const u8,
    pub fn encode(self: Bytes, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try w.writeAll(self.b);
    }
    pub fn decode(bytes: []const u8) error{Malformed}!Bytes {
        return .{ .b = bytes };
    }
};
const Values = contract.view.Family(1, "bench.values", Bytes);

fn split(e: *const entry_mod.Entry) struct { k: []const u8, v: []const u8 } {
    const at = std.mem.indexOfScalar(u8, e.payload, 0).?;
    return .{ .k = e.payload[0..at], .v = e.payload[at + 1 ..] };
}

fn keys(e: *const entry_mod.Entry, out: *contract.view.KeySet) error{OutOfMemory}!void {
    try out.add(Values.ID, "default", split(e).k);
}

fn apply(view: *contract.view.View, e: *const entry_mod.Entry, _: *persistence.Result) persistence.ApplyError!void {
    const kv = split(e);
    var kb: [64]u8 = undefined;
    try view.put(Values, contract.view.key(&kb, "default", kv.k) catch unreachable, .{ .b = kv.v });
}

const machine: contract.machine.Machine = .{ .name = "bench", .entry_types = &.{.kv_put}, .families = &.{contract.machine.info(Values)}, .keys = keys, .apply = apply };

/// A batch entry's payload is its key count, u16; its keys are `b-<i>`.
fn batchCount(e: *const entry_mod.Entry) u16 {
    return std.mem.readInt(u16, e.payload[0..2], .little);
}

fn batchKey(buf: []u8, i: usize) []const u8 {
    return std.fmt.bufPrint(buf, "b-{d:0>6}", .{i}) catch unreachable;
}

fn batchKeys(e: *const entry_mod.Entry, out: *contract.view.KeySet) error{OutOfMemory}!void {
    var b: [16]u8 = undefined;
    for (0..batchCount(e)) |i| try out.add(Values.ID, "default", batchKey(&b, i));
}

/// Each key read, then written: what an expire does for each lease.
fn batchApply(view: *contract.view.View, e: *const entry_mod.Entry, _: *persistence.Result) persistence.ApplyError!void {
    var b: [16]u8 = undefined;
    var kb: [64]u8 = undefined;
    for (0..batchCount(e)) |i| {
        const k = contract.view.key(&kb, "default", batchKey(&b, i)) catch unreachable;
        _ = try view.get(Values, k);
        try view.put(Values, k, .{ .b = "v" });
    }
}

const batch_machine: contract.machine.Machine = .{ .name = "bench-batch", .entry_types = &.{.kv_put}, .families = &.{contract.machine.info(Values)}, .keys = batchKeys, .apply = batchApply };

fn report(name: []const u8, n: u64, start: i128) void {
    const ns: u64 = @intCast(stdx.time.nanoTimestamp() - start);
    std.debug.print("  {s:<28} {d:>6} ns/op  ({d} ops)\n", .{ name, ns / n, n });
}

pub fn main() !void {
    const allocator = std.heap.c_allocator;
    var kbuf: [32]u8 = undefined;
    std.debug.print("\n=== Contract apply ===\n\n", .{});

    {
        var store = contract.store.Store.init(allocator);
        defer store.deinit();
        var scratch = std.heap.ArenaAllocator.init(allocator);
        defer scratch.deinit();
        var pbuf: [128]u8 = undefined;
        var start = stdx.time.nanoTimestamp();
        for (0..ITERS) |i| {
            const p = std.fmt.bufPrint(&pbuf, "key-{d:0>8}\x00value-{d:0>8}-padding-data", .{ (i * 7919) % ITERS, i }) catch unreachable;
            const e = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, i + 1, i + 1, p);
            var out: persistence.Result = .{};
            out.reset(&e);
            try contract.machine.applyEntry(&machine, &store, &scratch, &e, &out);
        }
        report("contract put (stage+commit)", ITERS, start);
        const read: contract.view.ReadView = .{ .store = &store };
        var kb: [64]u8 = undefined;
        start = stdx.time.nanoTimestamp();
        var found: usize = 0;
        for (0..ITERS) |i| {
            const k = std.fmt.bufPrint(&kbuf, "key-{d:0>8}", .{i}) catch unreachable;
            found += @intFromBool((try read.get(Values, contract.view.key(&kb, "default", k) catch unreachable)) != null);
        }
        std.mem.doNotOptimizeAway(found);
        report("contract get", ITERS, start);
        const prefix = contract.view.key(&kb, "default", "key-0000") catch unreachable;
        start = stdx.time.nanoTimestamp();
        for (0..READS) |_| {
            var r = read.range(Values, prefix);
            var n: usize = 0;
            while (try r.next()) |_| {
                n += 1;
                if (n == 100) break;
            }
            found += n;
        }
        std.mem.doNotOptimizeAway(found);
        report("contract 100-key prefix", READS, start);
    }
    {
        var store = contract.store.Store.init(allocator);
        defer store.deinit();
        var scratch = std.heap.ArenaAllocator.init(allocator);
        defer scratch.deinit();
        var name: [40]u8 = undefined;
        for ([_]u16{ 1, 16, 64, 256 }) |n| {
            var payload: [2]u8 = undefined;
            std.mem.writeInt(u16, &payload, n, .little);
            const entries: u64 = 20_000 / n;
            const start = stdx.time.nanoTimestamp();
            for (0..entries) |i| {
                const e = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, i + 1, i + 1, &payload);
                var out: persistence.Result = .{};
                out.reset(&e);
                try contract.machine.applyEntry(&batch_machine, &store, &scratch, &e, &out);
            }
            report(std.fmt.bufPrint(&name, "entry of {d} keys", .{n}) catch unreachable, entries, start);
        }
    }

    {
        // The KV machine's own batch entry (a committed transaction), as
        // large as one is: each op declares its value, its history range
        // and its namespace's counts.
        const kvm = src.kv.machine;
        const kve = src.protocol.kv_entries;
        const obuf = try allocator.alloc(u8, kve.BatchOps.byte_bound.?);
        defer allocator.free(obuf);
        const pbuf = try allocator.alloc(u8, kve.Batch.max_size);
        defer allocator.free(pbuf);
        const big = try allocator.alloc(u8, 256 * 1024);
        defer allocator.free(big);
        @memset(big, 'x');
        var name: [40]u8 = undefined;
        // Small ops, then full ones: the limits' defaults, and the built-in
        // most. Each case has a store of its own, so its order doesn't matter.
        const cases = [_]struct { ops: u16, total: usize }{
            .{ .ops = 8, .total = 0 },
            .{ .ops = 24, .total = 0 },
            .{ .ops = 32, .total = 0 },
            .{ .ops = 36, .total = 0 },
            .{ .ops = 40, .total = 0 },
            .{ .ops = 44, .total = 0 },
            .{ .ops = 48, .total = 0 },
            .{ .ops = 64, .total = 0 },
            .{ .ops = 256, .total = 0 },
            .{ .ops = 32, .total = 256 * 1024 },
            .{ .ops = 32, .total = 1024 * 1024 },
        };
        for (cases) |c| {
            var store = contract.store.Store.init(allocator);
            defer store.deinit();
            var scratch = std.heap.ArenaAllocator.init(allocator);
            defer scratch.deinit();
            const n = c.ops;
            // Small entries put about 20k keys in; full ones, what memory allows.
            const entries: u64 = if (c.total == 0) 20_000 / n else 100;
            var elapsed: u64 = 0;
            for (0..entries) |j| {
                var ops = kve.BatchOps.Builder.init(obuf);
                var keybufs: [256][32]u8 = undefined;
                var left = c.total;
                for (0..n) |i| {
                    const k = std.fmt.bufPrint(&keybufs[i], "default\x00b-{d}-{d}-{d}", .{ n, j, i }) catch unreachable;
                    // Equal shares of a full transaction's bytes.
                    const value = if (c.total == 0) "v" else blk: {
                        const len = (left / (n - i)) -| k.len;
                        left -= k.len + len;
                        break :blk big[0..len];
                    };
                    _ = try ops.add(.{ .kind = .put, .key = k, .value = value, .ttl_ms = null, .delta = null });
                }
                const payload = try kve.Batch.encodeInto(pbuf, .{ .ops = ops.value() });
                const e = entry_mod.buildEntry(.kv_batch, entry_mod.Flags.NONE, 1, j + 1, j + 1, payload);
                var out: persistence.Result = .{};
                out.reset(&e);
                const start = stdx.time.nanoTimestamp();
                try contract.machine.applyEntry(&kvm.machine, &store, &scratch, &e, &out);
                elapsed += @intCast(stdx.time.nanoTimestamp() - start);
            }
            const label = if (c.total == 0)
                std.fmt.bufPrint(&name, "kv batch of {d} puts", .{n}) catch unreachable
            else
                std.fmt.bufPrint(&name, "kv batch of {d} puts, {d} KiB", .{ n, c.total / 1024 }) catch unreachable;
            std.debug.print("  {s:<28} {d:>6} ns/op  ({d} ops)\n", .{ label, elapsed / entries, entries });
        }
    }
    std.debug.print("\n", .{});
}
