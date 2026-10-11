//! Benchmark: UAL (Unified Append Log) throughput
//!
//! Measures append and read operations per second on the hot ring buffer.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");

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
            @intCast(stdx.time.nanoTimestamp()),
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

/// Appends through a ring held at `capacity`, wrapped `wraps` times over,
/// timing each: the mean, and the worst single append (a rehash of the
/// index map lands in one).
fn benchSteady(allocator: std.mem.Allocator, label: []const u8, capacity: usize, payload_len: usize, wraps: usize) !void {
    var ual = try UAL.init(allocator, capacity, 0);
    defer ual.deinit();
    var payload_buf: [256]u8 = undefined;
    @memset(&payload_buf, 'p');
    const payload = payload_buf[0..payload_len];
    const per_ring = capacity / (entry_mod.HEADER_SIZE + payload_len);
    const appends = per_ring * wraps;

    var total_ns: u64 = 0;
    var worst_ns: u64 = 0;
    for (0..appends) |i| {
        const entry = entry_mod.buildEntry(.kv_put, 0, 1, @as(u64, @intCast(i)) + 1, 0, payload);
        const t0 = stdx.time.nanoTimestamp();
        _ = try ual.append(&entry);
        const ns: u64 = @intCast(stdx.time.nanoTimestamp() - t0);
        total_ns += ns;
        // The ring's first fill grows the map; count from the first wrap.
        if (i >= per_ring) worst_ns = @max(worst_ns, ns);
    }
    std.debug.print("  {s}: {d} live, {d} position slots, {d} appends: {d} ns/op, worst {d} us\n", .{
        label, ual.entry_count, ual.positions.slots.len, appends, total_ns / appends, worst_ns / 1000,
    });
}

/// Lookups of live indexes, at random, in a full ring; with `gap_every`
/// > 0 every that-many-th index is skipped, as KV entries skip a
/// partition's ring.
fn benchLookups(allocator: std.mem.Allocator, label: []const u8, capacity: usize, payload_len: usize, gap_every: u64) !void {
    var ual = try UAL.init(allocator, capacity, 0);
    defer ual.deinit();
    var payload_buf: [256]u8 = undefined;
    @memset(&payload_buf, 'p');
    const per_ring = capacity / (entry_mod.HEADER_SIZE + payload_len);
    var index: u64 = 1;
    for (0..per_ring * 2) |_| {
        if (gap_every > 0 and index % gap_every == 0) index += 1;
        const entry = entry_mod.buildEntry(.kv_put, 0, 1, index, 0, payload_buf[0..payload_len]);
        _ = try ual.append(&entry);
        index += 1;
    }
    var prng = std.Random.DefaultPrng.init(1);
    const random = prng.random();
    const lookups = 2_000_000;
    var found: u64 = 0;
    const t0 = stdx.time.nanoTimestamp();
    for (0..lookups) |_| {
        const i = random.intRangeAtMost(u64, ual.min_live_index, ual.max_index);
        found += @intFromBool(ual.contains(i));
    }
    const ns: u64 = @intCast(stdx.time.nanoTimestamp() - t0);
    std.mem.doNotOptimizeAway(found);
    std.debug.print("  {s}: {d} live, {d} ns/lookup ({d} found)\n", .{ label, ual.entry_count, ns / lookups, found });
}

/// The warm store under its default 32 MB budget with the smallest
/// payloads: ~2M held, a window sliding as it adds and evicts, with the
/// same map and sweep. The worst single step is a rehash.
fn benchWarmModel(allocator: std.mem.Allocator) !void {
    const tombstones = src.storage.tombstones;
    var map: std.AutoHashMapUnmanaged(u64, u64) = .{};
    defer map.deinit(allocator);
    var sweep: tombstones.Sweep = .{};
    const held = 32 * 1024 * 1024 / 16;
    const steps = held * 4;
    var total_ns: u64 = 0;
    var worst_ns: u64 = 0;
    for (0..steps) |i| {
        const t0 = stdx.time.nanoTimestamp();
        try map.put(allocator, i, i);
        if (i >= held) {
            _ = map.remove(i - held);
            sweep.noteRemoved(u64, &map);
        }
        const ns: u64 = @intCast(stdx.time.nanoTimestamp() - t0);
        total_ns += ns;
        if (i >= held) worst_ns = @max(worst_ns, ns);
    }
    std.debug.print("  warm store model, 32 MB of 16 B payloads: {d} held, map slots {d}: {d} ns/op, worst {d} us\n", .{ map.count(), map.capacity(), total_ns / steps, worst_ns / 1000 });
}

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
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

        const start = stdx.time.nanoTimestamp();
        benchAppend(&ual);
        const elapsed = stdx.time.nanoTimestamp() - start;

        const elapsed_ns: u64 = @intCast(elapsed);
        const ops_per_sec = @as(u64, BENCH_ITERS) * 1_000_000_000 / elapsed_ns;
        const ns_per_op = elapsed_ns / BENCH_ITERS;

        std.debug.print("  UAL Append:  {d:>12} ops/sec  ({d} ns/op, {d} entries)\n", .{
            ops_per_sec, ns_per_op, BENCH_ITERS,
        });

        // ── Read Benchmark ──
        const read_start = stdx.time.nanoTimestamp();
        benchRead(&ual);
        const read_elapsed = stdx.time.nanoTimestamp() - read_start;

        const read_ns: u64 = @intCast(read_elapsed);
        const read_iters: u64 = @intCast(@min(BENCH_ITERS, ual.max_index - ual.min_live_index + 1));
        const read_ops = read_iters * 1_000_000_000 / read_ns;
        const read_ns_per_op = read_ns / read_iters;

        std.debug.print("  UAL Read:    {d:>12} ops/sec  ({d} ns/op)\n", .{
            read_ops, read_ns_per_op,
        });
    }

    // ── Steady ring: tombstones and the worst pause ──
    // The largest default rings: a partition's (hot_buffer_capacity, 64 MB)
    // with the smallest entries, and the Raft log's (a quarter of it) with
    // KV-sized ones.
    std.debug.print("\n", .{});
    try benchSteady(allocator, "partition ring 64 MB, 16 B payloads", 64 * 1024 * 1024, 16, 4);
    try benchSteady(allocator, "raft ring 16 MB, 100 B payloads   ", 16 * 1024 * 1024, 100, 8);
    try benchLookups(allocator, "lookups, partition ring 64 MB, 16 B, gaps", 64 * 1024 * 1024, 16, 5);
    try benchLookups(allocator, "lookups, raft ring 16 MB, 100 B, dense   ", 16 * 1024 * 1024, 100, 0);
    try benchWarmModel(allocator);

    std.debug.print("\n", .{});
}
