//! Benchmark: kv.get end to end, client to server and back, one request
//! at a time over TCP, against a running server at FLO_BENCH_ENDPOINT
//! (host:port).
//! 10,000 keys are written first; the gets cycle over them.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");

const KEYS = 10_000;
const GETS = 50_000;

pub fn main() !void {
    const allocator = std.heap.c_allocator;
    const endpoint = std.mem.span(std.c.getenv("FLO_BENCH_ENDPOINT") orelse return error.Usage);
    var conn = src.cli_client.Client.init(allocator, endpoint);
    defer conn.deinit();
    try conn.connect();
    var kbuf: [32]u8 = undefined;
    for (0..KEYS) |i| {
        const k = try std.fmt.bufPrint(&kbuf, "bench-key-{d:0>6}", .{i});
        var r = try src.cli_client.kv.set(&conn, "", k, .{ .value = "bench-value-padding-padding-padding", .ttl_ms = null, .condition = .always, .cas_version = null, .routing_key = null, .txn = null });
        r.deinit();
    }
    for (0..2) |round| {
        const t0 = stdx.time.nanoTimestamp();
        for (0..GETS) |i| {
            const k = try std.fmt.bufPrint(&kbuf, "bench-key-{d:0>6}", .{(i * 7919) % KEYS});
            var r = try src.cli_client.kv.get(&conn, "", k, .{ .wait_ms = 0, .wait_for = .exists, .routing_key = null, .txn = null });
            if (r.status != .ok) return error.NotOk;
            r.deinit();
        }
        const ns: u64 = @intCast(stdx.time.nanoTimestamp() - t0);
        if (round == 1) std.debug.print("kv.get end to end: {d} ns/op ({d} gets)\n", .{ ns / GETS, GETS });
    }
}
