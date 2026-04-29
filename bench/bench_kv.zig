//! Benchmark: KVProjection operations per second
//!
//! Measures put, get, delete, scan, and scanPrefix throughput.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");

const KVProjection = src.projection.kv.KVProjection;

const WARMUP_ITERS = 1_000;
const BENCH_ITERS = 100_000;

fn formatKey(buf: *[32]u8, i: usize) []const u8 {
    return std.fmt.bufPrint(buf, "bench-key-{d:0>8}", .{i}) catch "bench-key";
}

fn formatVal(buf: *[64]u8, i: usize) []const u8 {
    return std.fmt.bufPrint(buf, "bench-value-{d:0>8}-padding-data", .{i}) catch "bench-val";
}

fn benchPut(kv: *KVProjection) !void {
    var kbuf: [32]u8 = undefined;
    var vbuf: [64]u8 = undefined;
    for (0..BENCH_ITERS) |i| {
        const key = formatKey(&kbuf, i);
        const val = formatVal(&vbuf, i);
        try kv.put(key, val, @as(u64, @intCast(i)) + 1, 1, 0, 0);
    }
}

fn benchGet(kv: *KVProjection) void {
    var kbuf: [32]u8 = undefined;
    var found: u64 = 0;
    for (0..BENCH_ITERS) |i| {
        const key = formatKey(&kbuf, i);
        if (kv.get(key)) |_| {
            found += 1;
        }
    }
    std.mem.doNotOptimizeAway(found);
}

fn benchDelete(kv: *KVProjection) !void {
    var kbuf: [32]u8 = undefined;
    for (0..BENCH_ITERS) |i| {
        const key = formatKey(&kbuf, i);
        try kv.delete(key, @as(u64, @intCast(i)) + BENCH_ITERS + 1, 1, 0);
    }
}

fn benchScan(kv: *KVProjection, iters: usize) void {
    var results: [100]src.projection.kv.ScanEntry = undefined;
    var total: u64 = 0;
    for (0..iters) |_| {
        total += kv.scan(&results);
    }
    std.mem.doNotOptimizeAway(total);
}

fn benchScanPrefix(kv: *KVProjection, iters: usize) void {
    var results: [100]src.projection.kv.ScanEntry = undefined;
    var total: u64 = 0;
    for (0..iters) |_| {
        total += kv.scanPrefix("bench-key-0000", &results);
    }
    std.mem.doNotOptimizeAway(total);
}

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== KVProjection Benchmark ===\n\n", .{});

    // ── Put Benchmark ──
    {
        var kv = KVProjection.init(allocator, 0);
        defer kv.deinit();

        const start = stdx.time.nanoTimestamp();
        try benchPut(&kv);
        const elapsed_ns: u64 = @intCast(stdx.time.nanoTimestamp() - start);
        const ops = @as(u64, BENCH_ITERS) * 1_000_000_000 / elapsed_ns;
        std.debug.print("  KV Put:       {d:>12} ops/sec  ({d} ns/op, {d} keys)\n", .{
            ops, elapsed_ns / BENCH_ITERS, BENCH_ITERS,
        });

        // ── Get Benchmark (keys are hot) ──
        {
            const gstart = stdx.time.nanoTimestamp();
            benchGet(&kv);
            const gel: u64 = @intCast(stdx.time.nanoTimestamp() - gstart);
            const gops = @as(u64, BENCH_ITERS) * 1_000_000_000 / gel;
            std.debug.print("  KV Get:       {d:>12} ops/sec  ({d} ns/op)\n", .{
                gops, gel / BENCH_ITERS,
            });
        }

        // ── Scan Benchmark (full table scan) ──
        {
            const scan_iters: usize = 10_000;
            const sstart = stdx.time.nanoTimestamp();
            benchScan(&kv, scan_iters);
            const sel: u64 = @intCast(stdx.time.nanoTimestamp() - sstart);
            const sops = @as(u64, scan_iters) * 1_000_000_000 / sel;
            std.debug.print("  KV Scan:      {d:>12} ops/sec  ({d} ns/op, batch=100)\n", .{
                sops, sel / scan_iters,
            });
        }

        // ── ScanPrefix Benchmark ──
        {
            const prefix_iters: usize = 10_000;
            const pstart = stdx.time.nanoTimestamp();
            benchScanPrefix(&kv, prefix_iters);
            const pel: u64 = @intCast(stdx.time.nanoTimestamp() - pstart);
            const pops = @as(u64, prefix_iters) * 1_000_000_000 / pel;
            std.debug.print("  KV ScanPfx:   {d:>12} ops/sec  ({d} ns/op, prefix=14B)\n", .{
                pops, pel / prefix_iters,
            });
        }

        // ── Delete Benchmark ──
        {
            const dstart = stdx.time.nanoTimestamp();
            try benchDelete(&kv);
            const del: u64 = @intCast(stdx.time.nanoTimestamp() - dstart);
            const dops = @as(u64, BENCH_ITERS) * 1_000_000_000 / del;
            std.debug.print("  KV Delete:    {d:>12} ops/sec  ({d} ns/op)\n", .{
                dops, del / BENCH_ITERS,
            });
        }
    }

    std.debug.print("\n", .{});
}
