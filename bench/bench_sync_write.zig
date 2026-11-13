//! Benchmark: kv.set end to end under sync durability, one request at a
//! time over TCP, against a running server at FLO_BENCH_ENDPOINT
//! (host:port) started with `--durability sync`. Each set waits for its
//! own flush, so the latency is mostly what a flush costs. Prints p50,
//! p99 and the mean over WRITES sets of 4 KiB values.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");
const mode = @import("mode.zig");

const WRITES = 10_000;
const VALUE_BYTES = 4096;

pub fn main() !void {
    mode.printForClient();
    const allocator = std.heap.c_allocator;
    const endpoint = std.mem.span(std.c.getenv("FLO_BENCH_ENDPOINT") orelse return error.Usage);
    var conn = src.cli_client.Client.init(allocator, endpoint);
    defer conn.deinit();
    try conn.connect();
    const value = @as([VALUE_BYTES]u8, @splat('v'));
    const lat = try allocator.alloc(u64, WRITES);
    defer allocator.free(lat);
    var kbuf: [32]u8 = undefined;
    var total: u64 = 0;
    for (0..WRITES) |i| {
        const k = try std.fmt.bufPrint(&kbuf, "sync-key-{d:0>6}", .{i});
        const t0 = stdx.time.nanoTimestamp();
        var r = try src.cli_client.kv.set(&conn, "", k, .{ .value = &value, .ttl_ms = null, .condition = .always, .cas_version = null, .txn = null });
        const ns: u64 = @intCast(stdx.time.nanoTimestamp() - t0);
        // The CLI client returns refusals as answers: count only real writes.
        if (r.status != .ok) return error.NotOk;
        r.deinit();
        lat[i] = ns;
        total += ns;
    }
    std.mem.sort(u64, lat, {}, std.sort.asc(u64));
    std.debug.print("kv.set under sync, {d} B values: p50 {d} us, p99 {d} us, mean {d} us ({d} sets)\n", .{
        VALUE_BYTES, lat[WRITES / 2] / 1000, lat[WRITES * 99 / 100] / 1000, total / WRITES / 1000, WRITES,
    });
}
