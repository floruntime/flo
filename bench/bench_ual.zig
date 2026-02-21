//! Benchmark: UAL (Unified Append Log) throughput
//!
//! Measures append and read operations per second on the hot ring buffer.

const std = @import("std");
const src = @import("src");

const UAL = src.storage.ual.ual.UAL;
const entry_mod = src.storage.ual.entry;

const WARMUP_ITERS = 1_000;
const BENCH_ITERS = 100_000;

fn benchAppend(ual: *UAL) void {
    const payload = "benchmark-payload-data-sixteen";
    for (0..BENCH_ITERS) |i| {
        const entry = entry_mod.buildEntry(
            .kv_put,
            0,
            1,
            @as(u64, @intCast(i)) + 1,
            @intCast(std.time.nanoTimestamp()),
            payload,
        );
        _ = ual.append(&entry) catch {};
    }
}

fn benchRead(ual: *const UAL) void {
    var sum: u64 = 0;
    const min = ual.min_live_index;
    const max = ual.max_index;
    const step = @max(1, (max - min) / BENCH_ITERS);
    var idx = min;
    while (idx <= max) : (idx += step) {
        if (ual.read(idx)) |e| {
            sum += e.header.index;
        }
    }
    // Prevent optimizer from removing the loop
    std.mem.doNotOptimizeAway(sum);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== UAL Benchmark ===\n\n", .{});

    // ── Append Benchmark ──
    {
        var ual = try UAL.init(allocator, 4 * 1024 * 1024, 0); // 4 MB ring
        defer ual.deinit();

        // Warmup
        const warmup_payload = "warmup";
        for (0..WARMUP_ITERS) |i| {
            const e = entry_mod.buildEntry(.kv_put, 0, 1, @as(u64, @intCast(i)) + 1, 0, warmup_payload);
            _ = ual.append(&e) catch {};
        }

        // Reset
        ual.deinit();
        ual = try UAL.init(allocator, 4 * 1024 * 1024, 0);

        const start = std.time.nanoTimestamp();
        benchAppend(&ual);
        const elapsed = std.time.nanoTimestamp() - start;

        const elapsed_ns: u64 = @intCast(elapsed);
        const ops_per_sec = @as(u64, BENCH_ITERS) * 1_000_000_000 / elapsed_ns;
        const ns_per_op = elapsed_ns / BENCH_ITERS;

        std.debug.print("  UAL Append:  {d:>12} ops/sec  ({d} ns/op, {d} entries)\n", .{
            ops_per_sec, ns_per_op, BENCH_ITERS,
        });

        // ── Read Benchmark ──
        const read_start = std.time.nanoTimestamp();
        benchRead(&ual);
        const read_elapsed = std.time.nanoTimestamp() - read_start;

        const read_ns: u64 = @intCast(read_elapsed);
        const read_iters: u64 = @intCast(@min(BENCH_ITERS, ual.max_index - ual.min_live_index + 1));
        const read_ops = read_iters * 1_000_000_000 / read_ns;
        const read_ns_per_op = read_ns / read_iters;

        std.debug.print("  UAL Read:    {d:>12} ops/sec  ({d} ns/op)\n", .{
            read_ops, read_ns_per_op,
        });
    }

    std.debug.print("\n", .{});
}
