//! Benchmark: an action run's round trip end to end, against a running
//! server at FLO_BENCH_ENDPOINT (host:port), one request at a time: invoke
//! a run, await it as a worker, complete it. Each step's latency is the
//! time its answer took, so the invoke is a write's commit and the await
//! is how long a ready run takes to reach its worker.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");

const client = src.cli_client;
const action_ops = src.protocol.action_ops;
const schema = src.protocol.schema;

const ROUNDS = 2_000;
const WARMUP = 200;

const Step = enum { invoke, await_task, complete };

fn percentile(sorted: []const u64, p: u64) u64 {
    return sorted[@min(sorted.len - 1, sorted.len * p / 100)];
}

pub fn main() !void {
    const allocator = std.heap.c_allocator;
    const endpoint = std.mem.span(std.c.getenv("FLO_BENCH_ENDPOINT") orelse return error.Usage);
    var conn = client.Client.init(allocator, endpoint);
    defer conn.deinit();
    try conn.connect();
    var reg = try client.action.register(&conn, "", "bench-act", 0, "", 60_000, 3, null, null);
    if (reg.status != .ok) return error.NotOk;
    reg.deinit();

    var times: [3][ROUNDS]u64 = undefined;
    for (0..WARMUP + ROUNDS) |i| {
        const t = stdx.time.nanoTimestamp();
        var inv = try client.action.invoke(&conn, "", "bench-act", "{\"n\":1}", null);
        if (inv.status != .ok) return error.NotOk;
        inv.deinit();
        const t_invoke = stdx.time.nanoTimestamp();

        var got = try client.action.workerAwait(&conn, "", "bench-w", &.{"bench-act"}, 0);
        defer got.deinit();
        if (got.status != .ok) return error.NotOk;
        var diag: schema.Diagnostic = .{};
        const task = (try action_ops.op("action.await").response.decodeAll(got.data, &diag)).task orelse return error.NoTask;
        const t_await = stdx.time.nanoTimestamp();

        var done = try client.action.workerComplete(&conn, "", "bench-w", action_ops.TaskId.of(task), "", "{}");
        if (done.status != .ok) return error.NotOk;
        done.deinit();
        const t_done = stdx.time.nanoTimestamp();

        if (i < WARMUP) continue;
        times[0][i - WARMUP] = @intCast(t_invoke - t);
        times[1][i - WARMUP] = @intCast(t_await - t_invoke);
        times[2][i - WARMUP] = @intCast(t_done - t_await);
    }
    std.debug.print("\n=== Action round trip ({d} runs, one at a time) ===\n\n", .{ROUNDS});
    for (std.enums.values(Step), &times) |step, *ts| {
        std.mem.sortUnstable(u64, ts, {}, std.sort.asc(u64));
        std.debug.print("  {t:<12} p50 {d:>7} us   p99 {d:>7} us\n", .{ step, percentile(ts, 50) / 1000, percentile(ts, 99) / 1000 });
    }
    std.debug.print("\n", .{});
}
