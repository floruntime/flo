//! Metrics Endpoint E2E Tests
//!
//! Drives a real flo server with the dashboard enabled and asserts the metrics
//! JSON surfaces the replication divergence counters added for issue #16
//! (Tier-1 "detect, don't repair"). This guards the full wiring:
//! ReplicationMetrics → MetricsRegistry → dashboard /api/v1/metrics.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "e2e/metrics: replication divergence counters surfaced on /api/v1/metrics" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/metrics");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());

    // The replication block must be present so the console can alarm on silent
    // follower divergence.
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"replication\"") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"follower_gaps_total\"") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"follower_entries_missing_total\"") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"broadcast_oversize_skipped_total\"") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"broadcast_send_failures_total\"") != null);
}

test "e2e/metrics: prometheus exporter serves the registry" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createMetricsHttp();
    defer http.deinit();

    var resp = try http.get("/metrics");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    // (An earlier version asserted `contains("text/plain") or body.len > 0`,
    // which any 200 satisfies — a tautology. Assert the real content type.)
    try testing.expect(resp.getHeader("content-type") != null);
    try testing.expect(std.mem.indexOf(u8, resp.getHeader("content-type").?, "text/plain") != null);

    // Prometheus exposition format, with the families that are actually recorded.
    try testing.expect(std.mem.indexOf(u8, resp.body, "# TYPE flo_commands_total counter") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "flo_uptime_seconds") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "flo_replication_follower_gaps_total 0") != null);

    // initShards is wired, so the shard family is sized to the real topology
    // rather than omitted (shardCount() used to stay 0).
    try testing.expect(std.mem.indexOf(u8, resp.body, "flo_shards_total") != null);
}

test "e2e/metrics: exporter health endpoint reports the real shard count" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createMetricsHttp();
    defer http.deinit();

    var resp = try http.get("/health");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"status\":\"ok\"") != null);
    // Was always "shards":0 because initShards() had no caller.
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"shards\":0") == null);
}
