//! Metrics Value E2E Tests
//!
//! These assert the *numbers*, not that a family appears. A metric that is
//! registered but never written scrapes as 0 forever, which reads as an idle
//! node rather than an uninstrumented one — and a test that only checks the
//! name is present passes in exactly that state.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

/// Value of `metric{...labels...}` from a Prometheus exposition body, matching
/// the first series whose line starts with `name{` and contains `needle`.
fn seriesValue(body: []const u8, name: []const u8, needle: []const u8) ?u64 {
    var lines = std.mem.splitScalar(u8, body, '\n');
    while (lines.next()) |line| {
        if (!std.mem.startsWith(u8, line, name)) continue;
        if (line.len <= name.len or line[name.len] != '{') continue;
        if (std.mem.indexOf(u8, line, needle) == null) continue;
        const sp = std.mem.lastIndexOfScalar(u8, line, ' ') orelse continue;
        return std.fmt.parseInt(u64, std.mem.trim(u8, line[sp + 1 ..], " \r"), 10) catch continue;
    }
    return null;
}

/// Value of an unlabeled `name <value>` series.
fn scalarValue(body: []const u8, name: []const u8) ?u64 {
    var lines = std.mem.splitScalar(u8, body, '\n');
    while (lines.next()) |line| {
        if (!std.mem.startsWith(u8, line, name)) continue;
        if (line.len <= name.len or line[name.len] != ' ') continue;
        return std.fmt.parseInt(u64, std.mem.trim(u8, line[name.len + 1 ..], " \r"), 10) catch continue;
    }
    return null;
}

test "e2e/metrics: stream append counters carry real values" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    // One batch of three records.
    try ctx.exec(&.{ "stream", "append", "orders", "a", "b", "c" });

    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var resp = try http.get("/metrics");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);

    // Three records in one append op — not "the family exists".
    try testing.expectEqual(@as(?u64, 3), seriesValue(resp.body, "flo_stream_append_records_total", "orders"));
    try testing.expectEqual(@as(?u64, 1), seriesValue(resp.body, "flo_stream_append_ops_total", "orders"));

    const bytes = seriesValue(resp.body, "flo_stream_append_bytes_total", "orders") orelse 0;
    try testing.expect(bytes > 0);
}

test "e2e/metrics: stream read counters carry real values" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "stream", "append", "reads", "a", "b", "c" });
    try ctx.exec(&.{ "stream", "read", "reads" });

    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var resp = try http.get("/metrics");
    defer resp.deinit();

    // The read returned three records, so the counter must say three — a batch
    // counted as one entry would report 1 here.
    try testing.expectEqual(@as(?u64, 3), seriesValue(resp.body, "flo_stream_read_records_total", "reads"));
    try testing.expectEqual(@as(?u64, 1), seriesValue(resp.body, "flo_stream_read_ops_total", "reads"));
}

test "e2e/metrics: queue enqueue and dequeue counters carry real values" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "queue", "enqueue", "jobs", "one" });
    try ctx.exec(&.{ "queue", "enqueue", "jobs", "two" });
    try ctx.exec(&.{ "queue", "dequeue", "jobs" });

    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var resp = try http.get("/metrics");
    defer resp.deinit();

    try testing.expectEqual(@as(?u64, 2), seriesValue(resp.body, "flo_queue_enqueue_messages_total", "jobs"));
    const dequeued = seriesValue(resp.body, "flo_queue_dequeue_messages_total", "jobs") orelse 0;
    try testing.expect(dequeued >= 1);
}

test "e2e/metrics: per-shard counters are attributed, not left at zero" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "k", "v" });
    try ctx.exec(&.{ "kv", "get", "k" });

    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var resp = try http.get("/metrics");
    defer resp.deinit();

    // shardMetrics() had no callers, so every shard row exported 0 while the
    // global flo_commands_total moved.
    const cmds = seriesValue(resp.body, "flo_shard_commands_total", "shard") orelse 0;
    try testing.expect(cmds > 0);
}

test "e2e/metrics: workflow lifecycle counters carry real values" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "action", "register", "echo" });

    const workflow_def =
        \\kind: Workflow
        \\name: metrics-wf
        \\version: 1.0.0
        \\start.run: @actions/echo
        \\start.transitions.success: flo.Completed
        \\start.transitions.failure: flo.Failed
    ;
    const path = try stdx.testing.writeDottedToTempYaml(testing.allocator, workflow_def, "metrics-wf.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, path);

    try ctx.exec(&.{ "workflow", "create", "-f", path });
    try ctx.exec(&.{ "workflow", "start", "metrics-wf", "{\"a\":1}" });
    try ctx.exec(&.{ "workflow", "start", "metrics-wf", "{\"a\":2}" });

    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var resp = try http.get("/metrics");
    defer resp.deinit();

    // Two runs started — the family exported 0 regardless of traffic before,
    // because the workflow handler never touched the registry.
    try testing.expectEqual(@as(?u64, 2), scalarValue(resp.body, "flo_workflow_started_total"));
}

test "e2e/metrics: processing submitted counter carries a real value" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "stream", "append", "proc_src", "seed" });

    const job =
        \\kind: Processing
        \\name: metrics-proc
        \\sources.[0].stream.name: proc_src
        \\sinks.[0].stream.name: proc_dst
    ;
    const path = try stdx.testing.writeDottedToTempYaml(testing.allocator, job, "metrics-proc.yaml");
    defer stdx.testing.cleanupTempFile(testing.allocator, path);

    try ctx.exec(&.{ "processing", "submit", path });

    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var resp = try http.get("/metrics");
    defer resp.deinit();

    try testing.expectEqual(@as(?u64, 1), scalarValue(resp.body, "flo_processing_jobs_submitted_total"));
}

test "e2e/metrics: kv counters carry real values" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "a", "1" });
    try ctx.exec(&.{ "kv", "set", "b", "2" });
    try ctx.exec(&.{ "kv", "get", "a" });
    try ctx.exec(&.{ "kv", "delete", "b" });

    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var resp = try http.get("/metrics");
    defer resp.deinit();

    // The family was not emitted by exportPrometheus at all before, so these
    // series did not exist rather than reading zero.
    try testing.expectEqual(@as(?u64, 2), seriesValue(resp.body, "flo_kv_set_ops_total", "default"));
    try testing.expectEqual(@as(?u64, 1), seriesValue(resp.body, "flo_kv_get_ops_total", "default"));
    try testing.expectEqual(@as(?u64, 1), seriesValue(resp.body, "flo_kv_delete_ops_total", "default"));
}

test "e2e/metrics: a JSON write counts as a set, a whole-document delete as a delete, and a refused one as neither" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "json", "set", "doc", "{\"a\":1}" });
    try ctx.exec(&.{ "kv", "json", "set", "doc", "2", "--path", "$.a" });
    // Refused when they apply: no parent to set under, nothing to delete.
    for ([_][]const []const u8{
        &.{ "kv", "json", "set", "doc", "1", "--path", "$.nope.deeper" },
        &.{ "kv", "json", "del", "doc", "--path", "$.nope" },
    }) |args| {
        var refused = try ctx.cli.run(args);
        defer refused.deinit();
        try testing.expect(refused.exit_code != 0);
    }
    try ctx.exec(&.{ "kv", "json", "del", "doc" });

    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var resp = try http.get("/metrics");
    defer resp.deinit();
    try testing.expectEqual(@as(?u64, 2), seriesValue(resp.body, "flo_kv_set_ops_total", "default"));
    try testing.expectEqual(@as(?u64, 1), seriesValue(resp.body, "flo_kv_delete_ops_total", "default"));
}

test "e2e/metrics: tiered log hit counters carry real values" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "stream", "append", "tiered", "a", "b", "c" });
    try ctx.exec(&.{ "stream", "read", "tiered" });

    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var resp = try http.get("/metrics");
    defer resp.deinit();

    // registerTieredLog had no callers, so the whole family was absent.
    // A fresh append is served from the hot ring.
    const hot = seriesValue(resp.body, "flo_tiered_log_hot_hits_total", "group_id") orelse 0;
    try testing.expect(hot > 0);
    try testing.expect(seriesValue(resp.body, "flo_tiered_log_reads_total", "group_id") != null);
}

test "e2e/metrics: kv key count does not wrap when deleting a recovered key" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true, .durability = .sync },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "survivor", "v" });

    // Recovery rebuilds keys without going through recordSet, so key_count is
    // back to 0 here while the key exists. Deleting it used to wrap the gauge
    // to u64 max.
    try ctx.restartServer();
    try ctx.exec(&.{ "kv", "delete", "survivor" });

    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var resp = try http.get("/metrics");
    defer resp.deinit();

    const keys = seriesValue(resp.body, "flo_kv_keys", "default") orelse 0;
    try testing.expect(keys < 1_000_000);
}

/// `flo_kv_keys` for `namespace`, once the shards have published `want`.
fn awaitKvKeys(ctx: *stdx.testing.TestContext, namespace: []const u8, want: u64) !?u64 {
    var label_buf: [64]u8 = undefined;
    const label = try std.fmt.bufPrint(&label_buf, "namespace=\"{s}\"", .{namespace});
    var got: ?u64 = null;
    for (0..50) |_| {
        var http = try ctx.createMetricsHttp();
        defer http.deinit();
        var resp = try http.get("/metrics");
        defer resp.deinit();
        got = seriesValue(resp.body, "flo_kv_keys", label);
        if (got == want) return got;
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return got;
}

test "e2e/metrics: flo_kv_keys counts the live keys, across shards and a restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true, .shards = 2, .durability = .sync },
    });
    defer ctx.deinit();

    const Step = struct { args: []const []const u8, keys: u64 };
    for ([_]Step{
        .{ .args = &.{ "kv", "set", "a", "1" }, .keys = 1 },
        .{ .args = &.{ "kv", "set", "b", "1" }, .keys = 2 },
        .{ .args = &.{ "kv", "set", "a", "2" }, .keys = 2 },
        .{ .args = &.{ "kv", "delete", "missing" }, .keys = 2 },
        .{ .args = &.{ "kv", "delete", "a" }, .keys = 1 },
        .{ .args = &.{ "kv", "set", "a", "3" }, .keys = 2 },
        .{ .args = &.{ "kv", "incr", "n" }, .keys = 3 },
    }) |step| {
        var r = try ctx.cli.run(step.args);
        defer r.deinit();
        try testing.expectEqual(@as(u8, 0), r.exit_code);
        try testing.expectEqual(@as(?u64, step.keys), try awaitKvKeys(ctx, "default", step.keys));
    }

    // A restart rebuilds the count from the log.
    try ctx.restartServer();
    try testing.expectEqual(@as(?u64, 3), try awaitKvKeys(ctx, "default", 3));
}

fn awaitSeries(ctx: *stdx.testing.TestContext, name: []const u8, namespace: []const u8, want: u64) !?u64 {
    var label_buf: [64]u8 = undefined;
    const label = try std.fmt.bufPrint(&label_buf, "namespace=\"{s}\"", .{namespace});
    var got: ?u64 = null;
    for (0..50) |_| {
        var http = try ctx.createMetricsHttp();
        defer http.deinit();
        var resp = try http.get("/metrics");
        defer resp.deinit();
        got = seriesValue(resp.body, name, label);
        if (got == want) return got;
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return got;
}

test "e2e/metrics: flo_kv_history_bytes and evictions follow a key's history, and replay counts them again" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true, .durability = .sync },
    });
    defer ctx.deinit();

    // 1000-byte values: each older version is charged 1000 + 64.
    const value: [1000]u8 = @splat('h');
    try ctx.exec(&.{ "kv", "set", "k", &value, "-n", "hist" });
    try ctx.exec(&.{ "kv", "set", "k", &value, "-n", "hist" });
    try testing.expectEqual(@as(?u64, 1064), try awaitSeries(ctx, "flo_kv_history_bytes", "hist", 1064));
    try testing.expectEqual(@as(?u64, 0), try awaitSeries(ctx, "flo_kv_history_evicted_total", "hist", 0));

    // 66 older versions in all: the byte bound keeps 61 (61 × 1064 ≤ 64 KiB).
    for (0..65) |_| try ctx.exec(&.{ "kv", "set", "k", &value, "-n", "hist" });
    try testing.expectEqual(@as(?u64, 61 * 1064), try awaitSeries(ctx, "flo_kv_history_bytes", "hist", 61 * 1064));
    try testing.expectEqual(@as(?u64, 5), try awaitSeries(ctx, "flo_kv_history_evicted_total", "hist", 5));

    // Replay rebuilds the same history, evicting the same versions again.
    try ctx.restartServer();
    try testing.expectEqual(@as(?u64, 61 * 1064), try awaitSeries(ctx, "flo_kv_history_bytes", "hist", 61 * 1064));
    try testing.expectEqual(@as(?u64, 5), try awaitSeries(ctx, "flo_kv_history_evicted_total", "hist", 5));
}
