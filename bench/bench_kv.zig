//! Benchmark: KV operations per second, through the KV machine as a shard
//! applies them (staged on a View, committed to the families), and reads
//! over the committed state.
//!
//! Measures put, get, delete, scan and prefix scan.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");

const TestKv = src.kv.machine.TestKv;
const entry_mod = src.storage.ual.entry;
const kv_entries = src.protocol.kv_entries;

const BENCH_ITERS = 100_000;

fn formatKey(buf: *[32]u8, i: usize) []const u8 {
    return std.fmt.bufPrint(buf, "bench-key-{d:0>8}", .{i}) catch "bench-key";
}

fn formatVal(buf: *[64]u8, i: usize) []const u8 {
    return std.fmt.bufPrint(buf, "bench-value-{d:0>8}-padding-data", .{i}) catch "bench-val";
}

fn report(name: []const u8, n: u64, start: i128, extra: []const u8) void {
    const ns: u64 = @intCast(stdx.time.nanoTimestamp() - start);
    std.debug.print("  {s:<12} {d:>12} ops/sec  ({d} ns/op{s})\n", .{ name, n * 1_000_000_000 / ns, ns / n, extra });
}

pub fn main() !void {
    const allocator = std.heap.c_allocator;
    std.debug.print("\n=== KV Benchmark (the KV machine) ===\n\n", .{});
    var kv = TestKv.init(allocator);
    defer kv.deinit();
    var kbuf: [32]u8 = undefined;
    var vbuf: [64]u8 = undefined;

    var start = stdx.time.nanoTimestamp();
    for (0..BENCH_ITERS) |i| try kv.put(formatKey(&kbuf, i), formatVal(&vbuf, i), 0);
    report("KV Put:", BENCH_ITERS, start, ", one key and its namespace's counts per entry");

    const st = kv.state();
    start = stdx.time.nanoTimestamp();
    var found: u64 = 0;
    for (0..BENCH_ITERS) |i| found += @intFromBool(st.get(formatKey(&kbuf, i)) != null);
    std.mem.doNotOptimizeAway(found);
    report("KV Get:", BENCH_ITERS, start, "");

    var names: [100][]const u8 = undefined;
    start = stdx.time.nanoTimestamp();
    for (0..10_000) |_| found += st.scanKeyNames("default", "", null, &names);
    std.mem.doNotOptimizeAway(found);
    report("KV Scan:", 10_000, start, ", batch=100");

    start = stdx.time.nanoTimestamp();
    for (0..10_000) |_| found += st.scanKeyNames("default", "bench-key-0000", null, &names);
    std.mem.doNotOptimizeAway(found);
    report("KV ScanPfx:", 10_000, start, ", prefix=14B");

    var pbuf: [64]u8 = undefined;
    start = stdx.time.nanoTimestamp();
    for (0..BENCH_ITERS) |i| {
        const p = try kv_entries.Delete.encodeInto(&pbuf, .{ .key = formatKey(&kbuf, i) });
        _ = try kv.apply(&entry_mod.buildEntry(.kv_delete, 0, 1, kv.index + 1, 0, p));
    }
    report("KV Delete:", BENCH_ITERS, start, "");
    std.debug.print("\n", .{});
}
