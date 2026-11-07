//! Benchmark: what runs a workflow has parked on signals cost everything
//! else on their shard, against a running server at FLO_BENCH_ENDPOINT
//! (host:port) with one shard. Starts runs that wait for a signal in steps,
//! and at each count times one small read, one at a time. A duty that walks
//! every run on each tick shows here as read latency that grows with the
//! count.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");
const mode = @import("mode.zig");

const client = src.cli_client;

const COUNTS = [_]usize{ 1, 10_000, 100_000 };
const READS = 2_000;

const DEFINITION =
    \\kind: Workflow
    \\name: bench-idle
    \\version: 1.0.0
    \\start:
    \\  wait_for_signal:
    \\    type: never
    \\  transitions:
    \\    success: flo.Completed
;

fn percentile(sorted: []const u64, p: u64) u64 {
    return sorted[@min(sorted.len - 1, sorted.len * p / 100)];
}

pub fn main() !void {
    mode.printForClient();
    const allocator = std.heap.c_allocator;
    const endpoint = std.mem.span(std.c.getenv("FLO_BENCH_ENDPOINT") orelse return error.Usage);
    var conn = client.Client.init(allocator, endpoint);
    defer conn.deinit();
    try conn.connect();
    var created = try client.workflow.create(&conn, "", "bench-idle", DEFINITION);
    if (created.status != .ok) return error.NotOk;
    created.deinit();

    std.debug.print("\n=== A read with runs parked on signals ({d} reads each) ===\n\n", .{READS});
    var started: usize = 0;
    var id_buf: [32]u8 = undefined;
    for (COUNTS) |count| {
        while (started < count) : (started += 1) {
            const id = try std.fmt.bufPrint(&id_buf, "idle-{d}", .{started});
            var run = try client.workflow.start(&conn, "", "bench-idle", "", "{}", null, id);
            if (run.status != .ok) return error.NotOk;
            run.deinit();
        }
        var times: [READS]u64 = undefined;
        for (&times) |*t| {
            const at = stdx.time.nanoTimestamp();
            var read = try client.workflow.status(&conn, "", "idle-0");
            if (read.status != .ok) return error.NotOk;
            read.deinit();
            t.* = @intCast(stdx.time.nanoTimestamp() - at);
        }
        std.mem.sortUnstable(u64, &times, {}, std.sort.asc(u64));
        std.debug.print("  {d:>7} runs   p50 {d:>7} us   p99 {d:>7} us\n", .{ count, percentile(&times, 50) / 1000, percentile(&times, 99) / 1000 });
    }
    std.debug.print("\n", .{});
}
