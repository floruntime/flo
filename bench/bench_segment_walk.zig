//! Benchmark: finding records below the ring when every append is its own
//! segment, as a sync log seals one per flush. Writes SEGMENTS one-entry
//! segments to a scratch directory under /tmp, then times a walk of every
//! index two ways: `sealedSource`, the lookup the bulk thread's stream reads
//! make (no file opened), and `readRange`, which also opens and reads each
//! segment.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");
const mode = @import("mode.zig");

const DurableLog = src.storage.durable_log.DurableLog;
const SegmentWriter = src.storage.ual.writer.SegmentWriter;
const entry_mod = src.storage.ual.entry;
const Entry = entry_mod.Entry;

const SEGMENTS = 10_000;

pub fn main() !void {
    mode.print();
    const allocator = std.heap.c_allocator;

    var dir_buf: [64]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "/tmp/flo-bench-segwalk-{d}", .{std.c.getpid()});
    try stdx.fs.makePath(dir);
    defer stdx.fs.deleteTree(dir) catch {};

    var writer = SegmentWriter.init(allocator, 0);
    defer writer.deinit();
    var dl = try DurableLog.init(allocator, &writer, dir);
    defer dl.deinit();

    const payload = @as([100]u8, @splat('p'));
    for (1..SEGMENTS + 1) |i| {
        var e = entry_mod.buildEntry(.stream_append, 0, 1, i, i * 1000, &payload);
        e.header.crc32c = e.computeCrc();
        try writer.addEntry(&e);
        try dl.flush(i);
    }
    if ((try dl.segments()).len != SEGMENTS) return error.WrongSegmentCount;

    var path_buf: [512]u8 = undefined;
    const t0 = stdx.time.monotonicNs();
    for (1..SEGMENTS + 1) |i| {
        _ = (try dl.sealedSource(i, &path_buf)) orelse return error.NotFound;
    }
    const lookup_ns = stdx.time.monotonicNs() - t0;

    var buf: [1]Entry = undefined;
    var arena: [256]u8 = undefined;
    const t1 = stdx.time.monotonicNs();
    for (1..SEGMENTS + 1) |i| {
        if (dl.readRange(i, &buf, &arena).count != 1) return error.NotRead;
    }
    const read_ns = stdx.time.monotonicNs() - t1;

    std.debug.print("  {d} one-entry segments: lookup {d} ns/index ({d} ms in all), read {d} ns/index ({d} ms in all)\n", .{
        SEGMENTS, lookup_ns / SEGMENTS, lookup_ns / 1_000_000, read_ns / SEGMENTS, read_ns / 1_000_000,
    });
}
