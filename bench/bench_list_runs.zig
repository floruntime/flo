//! Benchmark: what one page of a run list costs at its walk bound, against
//! a running server at FLO_BENCH_ENDPOINT (host:port) with one shard.
//! Starts runs in steps and at each count times a list whose search
//! matches nothing, so each page reads its bound of index rows, the most a
//! page reads, and answers none with more. The walk doesn't yield, so this
//! is how long the page holds its shard.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");
const mode = @import("mode.zig");

const client = src.cli_client;

const COUNTS = [_]usize{ 4_096, 100_000 };
const READS = 200;

const DEFINITION =
    \\kind: Workflow
    \\name: bench-list
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
    var created = try client.workflow.create(&conn, "", "bench-list", DEFINITION);
    if (created.status != .ok) return error.NotOk;
    created.deinit();

    std.debug.print("\n=== A run list page at its walk bound, matching nothing ({d} pages each) ===\n\n", .{READS});
    var started: usize = 0;
    var id_buf: [32]u8 = undefined;
    for (COUNTS) |count| {
        while (started < count) : (started += 1) {
            const id = try std.fmt.bufPrint(&id_buf, "list-{d}", .{started});
            var run = try client.workflow.start(&conn, "", "bench-list", "", "{\"order\":1234,\"customer\":\"someone\"}", null, id);
            if (run.status != .ok) return error.NotOk;
            run.deinit();
        }
        var times: [READS]u64 = undefined;
        for (&times) |*t| {
            const at = stdx.time.nanoTimestamp();
            var page = try client.workflow.listRuns(&conn, "", "bench-list", 100, null, null, "no-such-run");
            if (page.status != .ok) return error.NotOk;
            page.deinit();
            t.* = @intCast(stdx.time.nanoTimestamp() - at);
        }
        std.mem.sortUnstable(u64, &times, {}, std.sort.asc(u64));
        std.debug.print("  {d:>7} runs   p50 {d:>7} us   p99 {d:>7} us\n", .{ count, percentile(&times, 50) / 1000, percentile(&times, 99) / 1000 });
    }
    std.debug.print("\n", .{});
}
