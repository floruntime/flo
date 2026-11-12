//! Benchmark: one record read from a large segment, as a busy second under
//! async durability seals one. Writes two segments of ENTRIES entries of
//! PAYLOAD bytes each to a scratch directory under /tmp, then reads single
//! records at random indices through `DurableLog.readRange`, alternating
//! segments so every read opens one: the cost of a cold lookup.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");
const mode = @import("mode.zig");

const DurableLog = src.storage.durable_log.DurableLog;
const SegmentWriter = src.storage.ual.writer.SegmentWriter;
const entry_mod = src.storage.ual.entry;
const Entry = entry_mod.Entry;

const ENTRIES = 50_000;
const PAYLOAD = 300;
const READS = 2_000;

pub fn main() !void {
    mode.print();
    const allocator = std.heap.c_allocator;

    var dir_buf: [64]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "/tmp/flo-bench-segread-{d}", .{std.c.getpid()});
    try stdx.fs.makePath(dir);
    defer stdx.fs.deleteTree(dir) catch {};

    var writer = SegmentWriter.init(allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(allocator, &writer, dir);
    defer dl.deinit();

    const payload = @as([PAYLOAD]u8, @splat('p'));
    for (0..2) |seg| {
        for (1..ENTRIES + 1) |k| {
            const i = seg * ENTRIES + k;
            var e = entry_mod.buildEntry(.stream_append, 0, 1, i, i * 1000, &payload);
            e.header.crc32c = e.computeCrc();
            try writer.addEntry(&e);
        }
        try dl.flush(seg * ENTRIES + ENTRIES);
    }

    var prng = std.Random.DefaultPrng.init(1);
    const random = prng.random();
    var buf: [1]Entry = undefined;
    var arena: [PAYLOAD]u8 = undefined;
    var times: [READS]u64 = undefined;
    for (0..READS) |r| {
        const seg: u64 = r % 2;
        const i = seg * ENTRIES + random.intRangeAtMost(u64, 1, ENTRIES);
        const t = stdx.time.monotonicNs();
        if (dl.readRange(i, &buf, &arena).count != 1) return error.NotRead;
        times[r] = stdx.time.monotonicNs() - t;
    }
    std.mem.sort(u64, &times, {}, std.sort.asc(u64));
    std.debug.print("  one record from a {d} MiB segment, opened each read: p50 {d} us, p99 {d} us\n", .{
        ENTRIES * (PAYLOAD + entry_mod.HEADER_SIZE) / (1024 * 1024), times[READS / 2] / 1000, times[READS * 99 / 100] / 1000,
    });
}
