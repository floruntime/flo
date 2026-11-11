//! Benchmark: how long a parent run takes to finish once its child's last
//! step is done, against a running server at FLO_BENCH_ENDPOINT
//! (host:port), one run at a time. The child runs an action this bench
//! completes as its worker; the time is from that completion's answer to
//! the parent's status reading completed, polled. It spans the child's
//! completion, the parent resuming on it, and the parent's own completion.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");
const mode = @import("mode.zig");

const client = src.cli_client;
const action_ops = src.protocol.action_ops;
const schema = src.protocol.schema;

const ROUNDS = 1_000;
const WARMUP = 100;

const CHILD =
    \\kind: Workflow
    \\name: bench-child
    \\version: 1.0.0
    \\start:
    \\  run: "@actions/bench-child-act"
    \\  transitions:
    \\    success: flo.Completed
    \\    failure: flo.Failed
;

const PARENT =
    \\kind: Workflow
    \\name: bench-parent
    \\version: 1.0.0
    \\start:
    \\  run: "@workflow/bench-child"
    \\  transitions:
    \\    success: flo.Completed
    \\    failure: flo.Failed
;

const COMPLETED: u8 = 3;

fn percentile(sorted: []const u64, p: u64) u64 {
    return sorted[@min(sorted.len - 1, sorted.len * p / 100)];
}

/// The run's status byte, from a status answer.
fn statusOf(data: []const u8) ?u8 {
    var off: usize = 0;
    for (0..3) |_| {
        if (off + 2 > data.len) return null;
        off += 2 + std.mem.readInt(u16, data[off..][0..2], .little);
    }
    return if (off < data.len) data[off] else null;
}

pub fn main() !void {
    mode.printForClient();
    const allocator = std.heap.c_allocator;
    const endpoint = std.mem.span(std.c.getenv("FLO_BENCH_ENDPOINT") orelse return error.Usage);
    var conn = client.Client.init(allocator, endpoint);
    defer conn.deinit();
    try conn.connect();
    var reg = try client.action.register(&conn, "", "bench-child-act", "", 60_000, 3);
    if (reg.status != .ok) return error.NotOk;
    reg.deinit();
    for ([_][2][]const u8{ .{ "bench-child", CHILD }, .{ "bench-parent", PARENT } }) |d| {
        var created = try client.workflow.create(&conn, "", d[0], d[1]);
        if (created.status != .ok) return error.NotOk;
        created.deinit();
    }

    var times: [ROUNDS]u64 = undefined;
    var polls: u64 = 0;
    for (0..WARMUP + ROUNDS) |i| {
        var started = try client.workflow.start(&conn, "", "bench-parent", "", "{}", null, null);
        if (started.status != .ok) return error.NotOk;
        var id_buf: [64]u8 = undefined;
        const id = id_buf[0..started.data.len];
        @memcpy(id, started.data);
        started.deinit();

        var got = try client.action.workerAwait(&conn, "", "bench-w", &.{"bench-child-act"}, 5_000);
        defer got.deinit();
        if (got.status != .ok) return error.NotOk;
        var diag: schema.Diagnostic = .{};
        const task = (try action_ops.op("action.await").response.decodeAll(got.data, &diag)).task orelse return error.NoTask;
        var done = try client.action.workerComplete(&conn, "", "bench-w", action_ops.TaskId.of(task), "", "{}");
        if (done.status != .ok) return error.NotOk;
        done.deinit();
        const t = stdx.time.nanoTimestamp();

        while (true) {
            var st = try client.workflow.status(&conn, "", id);
            defer st.deinit();
            if (st.status != .ok) return error.NotOk;
            polls += 1;
            const s = statusOf(st.data) orelse return error.BadStatus;
            if (s == COMPLETED) break;
            if (s > COMPLETED) return error.ParentFailed;
        }
        if (i >= WARMUP) times[i - WARMUP] = @intCast(stdx.time.nanoTimestamp() - t);
    }

    std.mem.sort(u64, &times, {}, std.sort.asc(u64));
    std.debug.print("  parent done after its child's last step ({d} runs, {d} status polls): p50 {d} us, p90 {d} us, p99 {d} us\n", .{
        ROUNDS, polls, percentile(&times, 50) / 1000, percentile(&times, 90) / 1000, percentile(&times, 99) / 1000,
    });
}
