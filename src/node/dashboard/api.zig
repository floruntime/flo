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

/// Every route takes exactly the methods it names; any other is refused
/// (`error.MethodNotAllowed`, a 405), so a link or an image tag on another
/// site, which can only GET, can never change anything.
pub fn only(method: Method, allowed: []const Method) error{MethodNotAllowed}!void {
    for (allowed) |m| if (method == m) return;
    return error.MethodNotAllowed;
}

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
        try only(method, &.{ .GET, .POST });
        if (method == .POST) return namespaces.createNamespace(allocator, body, ctx);
        return namespaces.getNamespaces(allocator, ctx);
    }
    if (std.mem.startsWith(u8, path, "namespaces/")) {
        try only(method, &.{.GET});
        return routeNamespace(allocator, path["namespaces/".len..], query_string, ctx);
    }

    // ── streams ─────────────────────────────────────────────
    if (std.mem.eql(u8, path, "streams")) {
        try only(method, &.{.GET});
        return streams.getStreams(allocator, query_string, ctx);
    }
    if (std.mem.startsWith(u8, path, "streams/")) {
        return routeStream(allocator, method, path["streams/".len..], query_string, ctx);
    }

    // ── queues ──────────────────────────────────────────────
    if (std.mem.eql(u8, path, "queues")) {
        try only(method, &.{.GET});
        return queues.getQueues(allocator, ctx);
    }
    if (std.mem.startsWith(u8, path, "queues/")) {
        return routeQueue(allocator, method, path["queues/".len..], query_string, body, ctx);
    }

    // ── kv ──────────────────────────────────────────────────
    if (std.mem.eql(u8, path, "kv/namespaces")) {
        try only(method, &.{.GET});
        return kv.getKVNamespaces(allocator, ctx);
    }
    if (std.mem.startsWith(u8, path, "kv/namespaces/")) {
        return routeKV(allocator, method, path["kv/namespaces/".len..], query_string, body, ctx);
    }

    // ── timeseries ──────────────────────────────────────────
    if (std.mem.eql(u8, path, "timeseries")) {
        try only(method, &.{.GET});
        return timeseries.getMeasurements(allocator, query_string, ctx);
    }
    if (std.mem.eql(u8, path, "timeseries/floql")) {
        try only(method, &.{ .GET, .POST });
        return timeseries.executeFloql(allocator, method, query_string, body, ctx);
    }
    if (std.mem.startsWith(u8, path, "timeseries/")) {
        try only(method, &.{.GET});
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
        try only(method, &.{.GET});
        return actions.getActions(allocator, query_string, ctx);
    }
    if (std.mem.startsWith(u8, path, "actions/")) {
        return routeAction(allocator, method, path["actions/".len..], query_string, body, ctx);
    }

    // ── workers ─────────────────────────────────────────────
    if (std.mem.eql(u8, path, "workers")) {
        try only(method, &.{.GET});
        return workers.getWorkers(allocator, query_string, ctx);
    }
    if (std.mem.startsWith(u8, path, "workers/")) {
        try only(method, &.{.GET});
        const worker_id = path["workers/".len..];
        return workers.getWorkerDetail(allocator, worker_id, ctx);
    }

    // ── cluster / metrics ────────────────────────────────────
    if (std.mem.eql(u8, path, "cluster/stats")) {
        try only(method, &.{.GET});
        return system.getClusterStats(allocator, ctx);
    }
    if (std.mem.eql(u8, path, "metrics")) {
        try only(method, &.{.GET});
        return system.getMetricsJson(allocator, ctx);
    }

    return helpers.jsonError(allocator, "Not found");
}

// ─── Sub-routers ─────────────────────────────────────────────────────────────

/// Route /queues/:name[/messages|/dlq[/:seq[/requeue]]|/purge]
///   GET  /queues/:name          — detail
///   POST /queues/:name          — enqueue a message (body = payload)
///   POST /queues/:name/purge    — purge live messages
fn routeQueue(allocator: Allocator, method: Method, rest: []const u8, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    const slash_idx = std.mem.indexOfScalar(u8, rest, '/');
    const name = if (slash_idx) |idx| rest[0..idx] else rest;
    const sub = if (slash_idx) |idx| rest[idx + 1 ..] else "";

    if (sub.len == 0) {
        try only(method, &.{ .GET, .POST });
        if (method == .POST) return queues.enqueueMessage(allocator, name, body, query_string, ctx);
        return queues.getQueueDetail(allocator, name, query_string, ctx);
    }
    if (std.mem.eql(u8, sub, "messages")) {
        try only(method, &.{.GET});
        return queues.getQueueMessages(allocator, name, query_string, ctx);
    }
    if (std.mem.eql(u8, sub, "dlq")) {
        try only(method, &.{.GET});
        return queues.getQueueDLQ(allocator, name, query_string, ctx);
    }
    if (std.mem.eql(u8, sub, "purge")) {
        try only(method, &.{.POST});
        return queues.purgeQueue(allocator, name, query_string, ctx);
    }

    // dlq/:seq or dlq/:seq/requeue
    if (std.mem.startsWith(u8, sub, "dlq/")) {
        const dlq_rest = sub["dlq/".len..];
        if (std.mem.endsWith(u8, dlq_rest, "/requeue")) {
            try only(method, &.{.POST});
            const seq_str = dlq_rest[0 .. dlq_rest.len - "/requeue".len];
            return queues.requeueDLQEntry(allocator, name, seq_str, query_string, ctx);
        }
        try only(method, &.{.DELETE});
        return queues.deleteDLQEntry(allocator, name, dlq_rest, ctx);
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

/// Route /streams/:name[/messages|/trim|/groups/:group[/pending|/members]]
///   GET    /streams/:name                  — detail
///   DELETE /streams/:name                  — delete stream (?force=true)
///   POST   /streams/:name/trim             — trim (?max_len|max_age_s|max_bytes|dry_run)
///   GET    /streams/:name/messages         — messages
///   GET    /streams/:name/groups/:group    — group detail
///   DELETE /streams/:name/groups/:group    — delete consumer group
fn routeStream(allocator: Allocator, method: Method, rest: []const u8, query_string: ?[]const u8, ctx: *DashboardContext) ![]const u8 {
    // Split ":name" from optional sub-resource
    const slash_idx = std.mem.indexOfScalar(u8, rest, '/');
    const name = if (slash_idx) |idx| rest[0..idx] else rest;
    const sub = if (slash_idx) |idx| rest[idx + 1 ..] else "";

    if (sub.len == 0) {
        try only(method, &.{ .GET, .DELETE });
        if (method == .DELETE) return streams.deleteStream(allocator, name, query_string, ctx);
        return streams.getStreamDetail(allocator, name, query_string, ctx);
    }
    if (std.mem.eql(u8, sub, "messages")) {
        try only(method, &.{.GET});
        return streams.getStreamMessages(allocator, name, query_string, ctx);
    }
    if (std.mem.eql(u8, sub, "trim")) {
        try only(method, &.{.POST});
        return streams.trimStream(allocator, name, query_string, ctx);
    }

    // groups/:group[/pending|/members]
    if (std.mem.startsWith(u8, sub, "groups/")) {
        const group_rest = sub["groups/".len..];
        const group_slash = std.mem.indexOfScalar(u8, group_rest, '/');
        const group_name = if (group_slash) |idx| group_rest[0..idx] else group_rest;
        const group_sub = if (group_slash) |idx| group_rest[idx + 1 ..] else "";

        if (group_sub.len == 0) {
            try only(method, &.{ .GET, .DELETE });
            if (method == .DELETE) return streams.deleteGroup(allocator, name, group_name, query_string, ctx);
            return streams.getGroupDetail(allocator, name, group_name, query_string, ctx);
        }
        try only(method, &.{.GET});
        if (std.mem.eql(u8, group_sub, "pending")) return streams.getGroupPending(allocator, name, group_name, query_string, ctx);
        if (std.mem.eql(u8, group_sub, "members")) return streams.getGroupMembers(allocator, name, group_name, query_string, ctx);
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
    if (sub.len == 0) {
        try only(method, &.{.GET});
        return namespaces.getNamespaceKV(allocator, ns, ctx);
    }

    // /kv/namespaces/:ns/keys[/:key[/history]]
    if (std.mem.eql(u8, sub, "keys")) {
        try only(method, &.{.GET});
        return kv.getKVKeys(allocator, ns, query_string, ctx);
    }
    if (std.mem.startsWith(u8, sub, "keys/")) {
        const key_rest_raw = sub["keys/".len..];
        // Percent-decode the key (frontend sends encodeURIComponent)
        var decode_buf: [4096]u8 = undefined;
        const key_rest = helpers.percentDecode(&decode_buf, key_rest_raw);
        // Check for /history suffix
        if (std.mem.endsWith(u8, key_rest, "/history")) {
            try only(method, &.{.GET});
            const key_name = key_rest[0 .. key_rest.len - "/history".len];
            return kv.getKVKeyHistory(allocator, ns, key_name, query_string, ctx);
        }
        try only(method, &.{ .GET, .PUT, .DELETE });
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
fn routeAction(allocator: Allocator, method: Method, rest: []const u8, query_string: ?[]const u8, body: []const u8, ctx: *DashboardContext) ![]const u8 {
    const slash_idx = std.mem.indexOfScalar(u8, rest, '/');
    const name = if (slash_idx) |idx| rest[0..idx] else rest;
    const sub = if (slash_idx) |idx| rest[idx + 1 ..] else "";

    if (std.mem.eql(u8, sub, "invoke")) {
        try only(method, &.{.POST});
        return actions.invokeAction(allocator, name, body, query_string, ctx);
    }
    try only(method, &.{.GET});
    if (sub.len == 0) return actions.getActionDetail(allocator, name, query_string, ctx);
    if (std.mem.eql(u8, sub, "runs")) return actions.getActionRuns(allocator, name, query_string, ctx);

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
    ctx.listen_port = 1; // purge loops back to the protocol port; none here

    // Routes to purgeQueue, which attempts a loopback write → connect refused.
    const result = try handleRequest(allocator, .POST, "queues/myq/purge", null, "", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"error\"") != null);
}

test "route: every change refuses GET, and every read refuses a change" {
    const allocator = std.testing.allocator;
    var metrics = helpers.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);
    const changes = [_][]const u8{
        "queues/q/purge",                 "queues/q/dlq/1/requeue",    "queues/q/dlq/1",
        "actions/a/invoke",               "streams/s/trim",            "workflow/definitions/w/enable",
        "workflow/definitions/w/disable", "workflow/runs/r/signal",    "processing/jobs/j/stop",
        "processing/jobs/j/savepoint",    "processing/jobs/j/restore", "processing/jobs/j/rescale",
    };
    for (changes) |path| {
        if (handleRequest(allocator, .GET, path, null, "", &ctx)) |r| {
            allocator.free(r);
            std.debug.print("GET {s} was served\n", .{path});
            return error.TestUnexpectedResult;
        } else |err| try std.testing.expectEqual(error.MethodNotAllowed, err);
    }
    const reads = [_][]const u8{ "namespaces/n", "streams", "queues", "queues/q/messages", "kv/namespaces", "timeseries", "actions", "workers", "cluster/stats", "metrics", "actions/a/runs", "workflow/runs/r/history" };
    for (reads) |path| {
        if (handleRequest(allocator, .POST, path, null, "", &ctx)) |r| {
            allocator.free(r);
            std.debug.print("POST {s} was served\n", .{path});
            return error.TestUnexpectedResult;
        } else |err| try std.testing.expectEqual(error.MethodNotAllowed, err);
    }
}
