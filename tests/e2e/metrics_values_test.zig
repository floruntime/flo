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
        \\start.transition.success: flo.Completed
        \\start.transition.failure: flo.Failed
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
