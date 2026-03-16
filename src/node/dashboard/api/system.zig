//! Dashboard API — System Endpoints
//!
//! - GET /cluster/stats — Cluster health, RPS, connections
//! - GET /metrics       — Internal metrics (JSON format)

const std = @import("std");
const build_options = @import("build_options");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;

/// GET /cluster/stats - Cluster health and statistics
pub fn getClusterStats(allocator: Allocator, ctx: *DashboardContext) ![]const u8 {
    const server_metrics = ctx.metrics.server.snapshot();
    const num_shards = ctx.num_shards;

    const uptime_secs = ctx.uptimeSeconds();
    const days = uptime_secs / 86400;
    const hours = (uptime_secs % 86400) / 3600;
    const mins = (uptime_secs % 3600) / 60;

    var uptime_buf: [64]u8 = undefined;
    const uptime_str = std.fmt.bufPrint(&uptime_buf, "{d}d {d}h {d}m", .{ days, hours, mins }) catch "0d 0h 0m";

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    const rps = if (uptime_secs > 0) server_metrics.commands_total / uptime_secs else 0;

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.intField("rps", rps);
    try obj.intField("active_connections", server_metrics.connections);
    try obj.stringField("uptime", uptime_str);
    try obj.stringField("version", build_options.version);
    try obj.intField("num_shards", num_shards);
    try obj.intField("commands_total", server_metrics.commands_total);
    try obj.intField("bytes_received", server_metrics.bytes_received);
    try obj.intField("bytes_sent", server_metrics.bytes_sent);
    try obj.intField("subscriptions", server_metrics.subscriptions);

    var nodes_arr = try obj.arrayField("nodes");
    try nodes_arr.begin();

    for (0..num_shards) |i| {
        try nodes_arr.next();
        var node_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try node_obj.begin();

        var id_buf: [32]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "shard-{d}", .{i}) catch "shard-?";
        try node_obj.stringField("id", id_str);
        try node_obj.stringField("status", "healthy");
        try node_obj.stringField("role", "active");
        try node_obj.end();
    }

    try nodes_arr.end();
    try obj.end();

    return try json_buf.toOwnedSlice(allocator);
}

/// GET /metrics - Metrics in JSON format
pub fn getMetricsJson(allocator: Allocator, ctx: *DashboardContext) ![]const u8 {
    const server_metrics = ctx.metrics.server.snapshot();

    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();

    var server_obj = try obj.objectField("server");
    try server_obj.begin();
    try server_obj.intField("connections", server_metrics.connections);
    try server_obj.intField("subscriptions", server_metrics.subscriptions);
    try server_obj.intField("commands_total", server_metrics.commands_total);
    try server_obj.intField("bytes_received", server_metrics.bytes_received);
    try server_obj.intField("bytes_sent", server_metrics.bytes_sent);
    try server_obj.intField("uptime_seconds", ctx.uptimeSeconds());
    try server_obj.end();

    try obj.intField("streams", ctx.metrics.streamCount());
    try obj.intField("queues", ctx.metrics.queueCount());
    try obj.intField("kv_namespaces", ctx.metrics.kvNamespaceCount());

    const wf = ctx.metrics.workflow.snapshot();
    var wf_obj = try obj.objectField("workflows");
    try wf_obj.begin();
    try wf_obj.intField("active_runs", wf.active_runs);
    try wf_obj.intField("started_total", wf.started_total);
    try wf_obj.intField("completed_total", wf.completed_total);
    try wf_obj.intField("failed_total", wf.failed_total);
    try wf_obj.intField("cancelled_total", wf.cancelled_total);
    try wf_obj.intField("timed_out_total", wf.timed_out_total);
    try wf_obj.intField("signals_delivered_total", wf.signals_delivered_total);
    try wf_obj.intField("timers_fired_total", wf.timers_fired_total);
    try wf_obj.intField("steps_executed_total", wf.steps_executed_total);
    try wf_obj.intField("active_schedules", wf.active_schedules);
    try wf_obj.end();

    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// Tests
// =============================================================================

test "getClusterStats returns valid JSON" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 2);

    const result = try getClusterStats(allocator, &ctx);
    defer allocator.free(result);

    // Should contain key fields
    try std.testing.expect(std.mem.indexOf(u8, result, "\"rps\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"version\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"num_shards\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"nodes\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "shard-0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "shard-1") != null);
}

test "getMetricsJson returns valid JSON" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getMetricsJson(allocator, &ctx);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "\"server\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"workflows\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"streams\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"queues\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"kv_namespaces\"") != null);
}
