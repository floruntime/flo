//! Dashboard API — Route Tree
//!
//! Maps URL paths (after /api/v1/ prefix) to handler functions.
//! All route paths and JSON shapes are preserved from the original.
//! Handler bodies use DashboardContext instead of old Core/Dispatcher.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Method = @import("../../util/http/mod.zig").Method;

// Sub-modules
pub const helpers = @import("api/helpers.zig");
pub const namespaces = @import("api/namespaces.zig");
pub const streams = @import("api/streams.zig");
pub const queues = @import("api/queues.zig");
pub const kv = @import("api/kv.zig");
pub const timeseries = @import("api/timeseries.zig");
pub const workflows = @import("api/workflows.zig");
pub const processing = @import("api/processing.zig");
pub const actions = @import("api/actions.zig");
pub const workers = @import("api/worker.zig");
pub const system = @import("api/system.zig");

pub const DashboardContext = helpers.DashboardContext;

/// Main API request router.
/// `path` is the portion after `/api/v1/` — e.g. "streams", "kv/namespaces/default/keys".
pub fn handleRequest(
    allocator: Allocator,
    method: Method,
    path: []const u8,
    query_string: ?[]const u8,
    body: []const u8,
    ctx: *DashboardContext,
) ![]const u8 {
    // ── namespaces ──────────────────────────────────────────
    if (std.mem.eql(u8, path, "namespaces")) {
        return namespaces.getNamespaces(allocator, ctx);
    }
    if (std.mem.startsWith(u8, path, "namespaces/")) {
        return routeNamespace(allocator, path["namespaces/".len..], query_string, ctx);
    }

    // ── streams ─────────────────────────────────────────────
    if (std.mem.eql(u8, path, "streams")) {
        return streams.getStreams(allocator, ctx);
    }
    if (std.mem.startsWith(u8, path, "streams/")) {
        return routeStream(allocator, path["streams/".len..], query_string, ctx);
    }

    // ── queues ──────────────────────────────────────────────
    if (std.mem.eql(u8, path, "queues")) {
        return queues.getQueues(allocator, ctx);
    }
    if (std.mem.startsWith(u8, path, "queues/")) {
        return routeQueue(allocator, method, path["queues/".len..], query_string, ctx);
    }

    // ── kv ──────────────────────────────────────────────────
    if (std.mem.eql(u8, path, "kv/namespaces")) {
        return kv.getKVNamespaces(allocator, ctx);
    }
    if (std.mem.startsWith(u8, path, "kv/namespaces/")) {
        return routeKV(allocator, method, path["kv/namespaces/".len..], query_string, body, ctx);
    }

    // ── timeseries ──────────────────────────────────────────
    if (std.mem.eql(u8, path, "timeseries")) {
        return timeseries.getMeasurements(allocator, query_string, ctx);
    }
    if (std.mem.eql(u8, path, "timeseries/floql")) {
        return timeseries.executeFloql(allocator, method, query_string, body, ctx);
    }
    if (std.mem.startsWith(u8, path, "timeseries/")) {
        return routeTimeseries(allocator, path["timeseries/".len..], query_string, ctx);
    }

    // ── workflow ─────────────────────────────────────────────
    if (std.mem.eql(u8, path, "workflows")) {
        // Frontend uses "workflows" — alias to workflow/definitions list
        return workflows.handleWorkflowRequest(allocator, method, "/definitions", query_string, body, ctx);
    }
    if (std.mem.startsWith(u8, path, "workflows/")) {
        // Frontend uses "workflows/:id" — alias to workflow/runs/:id
        const run_rest = path["workflows/".len..];
        // Check for sub-resource  (:id/history)
        const slash_idx = std.mem.indexOfScalar(u8, run_rest, '/');
        if (slash_idx) |idx| {
            const run_id = run_rest[0..idx];
            const sub2 = run_rest[idx..];
            // Build /runs/:id/sub path
            const buf = try std.fmt.allocPrint(allocator, "/runs/{s}{s}", .{ run_id, sub2 });
            defer allocator.free(buf);
            return workflows.handleWorkflowRequest(allocator, method, buf, query_string, body, ctx);
        }
        // Just /workflows/:id → /runs/:id
        const buf = try std.fmt.allocPrint(allocator, "/runs/{s}", .{run_rest});
        defer allocator.free(buf);
        return workflows.handleWorkflowRequest(allocator, method, buf, query_string, body, ctx);
    }
    if (std.mem.startsWith(u8, path, "workflow")) {
        const sub = if (path.len > "workflow".len) path["workflow".len..] else "";
        return workflows.handleWorkflowRequest(allocator, method, sub, query_string, body, ctx);
    }

    // ── processing ──────────────────────────────────────────
    if (std.mem.startsWith(u8, path, "processing")) {
        const sub = if (path.len > "processing".len) path["processing".len..] else "";
        return processing.handleProcessingRequest(allocator, method, sub, query_string, body, ctx);
    }

    // ── actions ─────────────────────────────────────────────
    if (std.mem.eql(u8, path, "actions")) {
        return actions.getActions(allocator, query_string, ctx);
    }
    if (std.mem.startsWith(u8, path, "actions/")) {
        return routeAction(allocator, path["actions/".len..], query_string, body, ctx);
    }

    // ── workers ─────────────────────────────────────────────
    if (std.mem.eql(u8, path, "workers")) {
        return workers.getWorkers(allocator, ctx);
    }
    if (std.mem.startsWith(u8, path, "workers/")) {
        const worker_id = path["workers/".len..];
        return workers.getWorkerDetail(allocator, worker_id, ctx);
    }

    // ── cluster / metrics ────────────────────────────────────
    if (std.mem.eql(u8, path, "cluster/stats")) {
        return system.getClusterStats(allocator, ctx);
    }
    if (std.mem.eql(u8, path, "metrics")) {
        return system.getMetricsJson(allocator, ctx);
    }

    return helpers.jsonError(allocator, "Not found");
}

// ─── Sub-routers ─────────────────────────────────────────────────────────────

/// Route /queues/:name[/messages|/dlq[/:seq[/requeue]]|/purge]
fn routeQueue(allocator: Allocator, method: Method, rest: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const slash_idx = std.mem.indexOfScalar(u8, rest, '/');
    const name = if (slash_idx) |idx| rest[0..idx] else rest;
    const sub = if (slash_idx) |idx| rest[idx + 1 ..] else "";

    if (sub.len == 0) return queues.getQueueDetail(allocator, name, ctx);
    if (std.mem.eql(u8, sub, "messages")) return queues.getQueueMessages(allocator, name, query_string, ctx);
    if (std.mem.eql(u8, sub, "dlq")) return queues.getQueueDLQ(allocator, name, query_string, ctx);
    if (std.mem.eql(u8, sub, "purge")) return queues.purgeQueue(allocator, name, ctx);

    // dlq/:seq or dlq/:seq/requeue
    if (std.mem.startsWith(u8, sub, "dlq/")) {
        const dlq_rest = sub["dlq/".len..];
        if (std.mem.endsWith(u8, dlq_rest, "/requeue")) {
            const seq_str = dlq_rest[0 .. dlq_rest.len - "/requeue".len];
            return queues.requeueDLQEntry(allocator, name, seq_str, ctx);
        }
        // DELETE /dlq/:seq
        if (method == .DELETE) return queues.deleteDLQEntry(allocator, name, dlq_rest, ctx);
    }

    return helpers.jsonError(allocator, "Not found");
}

/// Route /namespaces/:ns[/streams|/queues|/kv]
fn routeNamespace(allocator: Allocator, rest: []const u8, _: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    // Split ":ns" from optional sub-resource
    const slash_idx = std.mem.indexOfScalar(u8, rest, '/');
    const ns = if (slash_idx) |idx| rest[0..idx] else rest;
    const sub = if (slash_idx) |idx| rest[idx + 1 ..] else "";

    if (sub.len == 0) return namespaces.getNamespaceDetail(allocator, ns, ctx);
    if (std.mem.eql(u8, sub, "streams")) return namespaces.getNamespaceStreams(allocator, ns, ctx);
    if (std.mem.eql(u8, sub, "queues")) return namespaces.getNamespaceQueues(allocator, ns, ctx);
    if (std.mem.eql(u8, sub, "kv")) return namespaces.getNamespaceKV(allocator, ns, ctx);

    return helpers.jsonError(allocator, "Not found");
}

/// Route /streams/:name[/messages|/groups/:group[/pending|/members]]
fn routeStream(allocator: Allocator, rest: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    // Split ":name" from optional sub-resource
    const slash_idx = std.mem.indexOfScalar(u8, rest, '/');
    const name = if (slash_idx) |idx| rest[0..idx] else rest;
    const sub = if (slash_idx) |idx| rest[idx + 1 ..] else "";

    if (sub.len == 0) return streams.getStreamDetail(allocator, name, ctx);
    if (std.mem.eql(u8, sub, "messages")) return streams.getStreamMessages(allocator, name, query_string, ctx);

    // groups/:group[/pending|/members]
    if (std.mem.startsWith(u8, sub, "groups/")) {
        const group_rest = sub["groups/".len..];
        const group_slash = std.mem.indexOfScalar(u8, group_rest, '/');
        const group_name = if (group_slash) |idx| group_rest[0..idx] else group_rest;
        const group_sub = if (group_slash) |idx| group_rest[idx + 1 ..] else "";

        if (group_sub.len == 0) return streams.getGroupDetail(allocator, name, group_name, ctx);
        if (std.mem.eql(u8, group_sub, "pending")) return streams.getGroupPending(allocator, name, group_name, ctx);
        if (std.mem.eql(u8, group_sub, "members")) return streams.getGroupMembers(allocator, name, group_name, ctx);
    }

    return helpers.jsonError(allocator, "Not found");
}

/// Route /kv/namespaces/:ns[/keys[/:key[/history]]]
/// Also handles PUT/DELETE for :key
fn routeKV(allocator: Allocator, method: Method, rest: []const u8, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    // rest = ":ns" or ":ns/keys" or ":ns/keys/:key" or ":ns/keys/:key/history"
    const slash_idx = std.mem.indexOfScalar(u8, rest, '/');
    const ns = if (slash_idx) |idx| rest[0..idx] else rest;
    const sub = if (slash_idx) |idx| rest[idx + 1 ..] else "";

    // /kv/namespaces/:ns (same as namespace KV overview)
    if (sub.len == 0) return namespaces.getNamespaceKV(allocator, ns, ctx);

    // /kv/namespaces/:ns/keys[/:key[/history]]
    if (std.mem.eql(u8, sub, "keys")) {
        return kv.getKVKeys(allocator, ns, query_string, ctx);
    }
    if (std.mem.startsWith(u8, sub, "keys/")) {
        const key_rest = sub["keys/".len..];
        // Check for /history suffix
        if (std.mem.endsWith(u8, key_rest, "/history")) {
            const key_name = key_rest[0 .. key_rest.len - "/history".len];
            return kv.getKVKeyHistory(allocator, ns, key_name, query_string, ctx);
        }
        // PUT or DELETE on key
        if (method == .PUT) return kv.putKVKey(allocator, ns, key_rest, body, ctx);
        if (method == .DELETE) return kv.deleteKVKey(allocator, ns, key_rest, ctx);

        // GET key value
        return kv.getKVKeyValue(allocator, ns, key_rest, query_string, ctx);
    }

    return helpers.jsonError(allocator, "Not found");
}

/// Route /timeseries/:measurement[/data]
fn routeTimeseries(allocator: Allocator, rest: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    const slash_idx = std.mem.indexOfScalar(u8, rest, '/');
    const measurement = if (slash_idx) |idx| rest[0..idx] else rest;
    const sub = if (slash_idx) |idx| rest[idx + 1 ..] else "";

    if (sub.len == 0) return timeseries.getMeasurementDetail(allocator, measurement, query_string, ctx);
    if (std.mem.eql(u8, sub, "data")) return timeseries.getSeriesData(allocator, measurement, query_string, ctx);

    return helpers.jsonError(allocator, "Not found");
}

/// Route /actions/:name[/runs|/invoke]
fn routeAction(allocator: Allocator, rest: []const u8, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    const slash_idx = std.mem.indexOfScalar(u8, rest, '/');
    const name = if (slash_idx) |idx| rest[0..idx] else rest;
    const sub = if (slash_idx) |idx| rest[idx + 1 ..] else "";

    if (sub.len == 0) return actions.getActionDetail(allocator, name, query_string, ctx);
    if (std.mem.eql(u8, sub, "runs")) return actions.getActionRuns(allocator, name, query_string, ctx);
    if (std.mem.eql(u8, sub, "invoke")) return actions.invokeAction(allocator, name, body, ctx);

    return helpers.jsonError(allocator, "Not found");
}

// =============================================================================
// Tests
// =============================================================================

test "route namespaces list" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "namespaces", null, "", &ctx);
    defer allocator.free(result);
    // Should return JSON array (at least "default")
    try std.testing.expect(result.len > 0);
}

test "route streams list" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "streams", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "route queues list" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "queues", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "route kv namespaces" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "kv/namespaces", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(result.len > 0);
}

test "route cluster stats" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "cluster/stats", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"active_connections\"") != null);
}

test "route metrics" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "metrics", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"server\"") != null);
}

test "route not found" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "nonexistent", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}

test "route workflow definitions" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "workflow/definitions", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "route processing jobs" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "processing/jobs", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "route actions list" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "actions", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "route workers" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "workers", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "route stream detail" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "streams/my-stream", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"my-stream\"") != null);
}

test "route stream group detail" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "streams/events/groups/my-group", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"group\":\"my-group\"") != null);
}

test "route kv key get" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "kv/namespaces/default/keys/mykey", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(result.len > 0);
}

test "route workflows alias maps to workflow definitions" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "workflows", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "route queue messages" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "queues/myq/messages", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"messages\":[]") != null);
}

test "route queue dlq" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .GET, "queues/myq/dlq", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"entries\":[]") != null);
}

test "route queue purge" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try handleRequest(allocator, .POST, "queues/myq/purge", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"ok\":true") != null);
}
