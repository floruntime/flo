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
    std.debug.print("\n", .{});
}
