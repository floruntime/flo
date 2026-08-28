//! Dashboard Actions API E2E Tests (issue #26)
//!
//! Verifies that the dashboard surfaces REAL action metadata instead of
//! placeholders:
//!   - owner / timeout_ms are now persisted on ActionRecord (were "" / 30000).
//!   - invocation latency (avg / p99) is derived from completed run records.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

/// Extract a run_id from `flo action invoke` output ("Result: <run_id>").
fn extractRunId(output: []const u8) ?[]const u8 {
    const prefix = "Result: ";
    if (!std.mem.startsWith(u8, output, prefix)) return null;
    var end = prefix.len;
    while (end < output.len and output[end] != '\n' and output[end] != '\r') end += 1;
    return if (end > prefix.len) output[prefix.len..end] else null;
}

test "e2e/dashboard: action owner + timeout_ms are persisted and surfaced" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "action", "register", "owned-action", "--owner", "myteam", "--timeout", "60000", "--retries", "5" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // List endpoint reflects the persisted owner/timeout (were "" / 30000).
    var list = try http.get("/api/v1/actions");
    defer list.deinit();
    try testing.expectEqual(@as(u16, 200), list.status);
    try testing.expect(std.mem.indexOf(u8, list.body, "\"owner\":\"myteam\"") != null);
    try testing.expect(std.mem.indexOf(u8, list.body, "\"timeout_ms\":60000") != null);

    // Detail endpoint too, plus max_retries and the latency object.
    var detail = try http.get("/api/v1/actions/owned-action");
    defer detail.deinit();
    try testing.expectEqual(@as(u16, 200), detail.status);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"owner\":\"myteam\"") != null);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"timeout_ms\":60000") != null);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"max_retries\":5") != null);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"latency\"") != null);
}

test "e2e/dashboard: action latency reflects a completed run" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    const action_name = "lat-action";
    const worker_id = "lat-worker";

    try ctx.exec(&.{ "action", "register", action_name });
    try ctx.exec(&.{ "worker", "register", worker_id, action_name });

    // invoke → worker claims (sets started_at) → worker completes (sets completed_at)
    const invoke_out = try ctx.execCapture(&.{ "action", "invoke", action_name, "{}" });
    const run_id = extractRunId(std.mem.trim(u8, invoke_out, &std.ascii.whitespace)) orelse return error.NoRunId;

    var await_res = try ctx.cli.run(&.{ "worker", "await", action_name, "--worker-id", worker_id, "--block", "5000" });
    defer await_res.deinit();
    try stdx.testing.assertSucceeded(await_res);

    var complete_res = try ctx.cli.run(&.{ "worker", "complete", run_id, "--worker-id", worker_id, "--action", action_name, "--result", "{\"ok\":true}" });
    defer complete_res.deinit();
    try stdx.testing.assertSucceeded(complete_res);

    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    var detail = try http.get("/api/v1/actions/lat-action");
    defer detail.deinit();
    try testing.expectEqual(@as(u16, 200), detail.status);

    // One completed run → latency has a single real sample (was always absent).
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"latency\"") != null);
    try testing.expect(std.mem.indexOf(u8, detail.body, "\"count\":1") != null);
}
