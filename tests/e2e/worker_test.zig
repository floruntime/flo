//! Worker End-to-End Tests
//!
//! Tests the worker management lifecycle:
//!   register → list → info → drain → heartbeat
//!
//! Also tests the REST API endpoints:
//!   GET /api/v1/workers
//!   GET /api/v1/workers/:id
//!
//! NOTE: Task dispatch tests (await, complete, fail) live in action_test.zig
//! since they test the action→worker flow together.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

// =============================================================================
// Worker Registration
// =============================================================================

test "e2e/worker: register and list" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register two workers
    try ctx.exec(&.{ "worker", "register", "list-w1", "task_a" });
    try ctx.exec(&.{ "worker", "register", "list-w2", "task_b" });

    // List workers
    var result = try ctx.cli.run(&.{ "worker", "list" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);

    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);
    try testing.expect(output.len > 0);
    try testing.expect(std.mem.indexOf(u8, output, "list-w1") != null);
    try testing.expect(std.mem.indexOf(u8, output, "list-w2") != null);
}

test "e2e/worker: register with multiple task types" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register a worker that handles multiple task types
    try ctx.exec(&.{ "worker", "register", "multi-worker", "task_x", "task_y", "task_z" });

    var result = try ctx.cli.run(&.{ "worker", "list" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, output, "multi-worker") != null);
}

// =============================================================================
// Worker Info
// =============================================================================

test "e2e/worker: info shows details" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register a worker
    try ctx.exec(&.{ "worker", "register", "info-w1", "process" });

    // Get worker info
    var result = try ctx.cli.run(&.{ "worker", "info", "info-w1" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);

    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);
    try testing.expect(output.len > 0);
    // Should contain the worker ID in the output
    try testing.expect(std.mem.indexOf(u8, output, "info-w1") != null);
}

test "e2e/worker: info on non-existent worker" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Get info for a worker that doesn't exist
    var result = try ctx.cli.run(&.{ "worker", "info", "ghost-worker" });
    defer result.deinit();

    // Should fail gracefully (not crash)
    try testing.expect(!result.succeeded());
}

// =============================================================================
// Worker Drain
// =============================================================================

test "e2e/worker: drain sets worker to draining" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const worker_id = "drain-w1";

    // Register a worker
    try ctx.exec(&.{ "worker", "register", worker_id, "process" });

    // Drain the worker
    var drain_result = try ctx.cli.run(&.{ "worker", "drain", worker_id });
    defer drain_result.deinit();

    try stdx.testing.assertSucceeded(drain_result);

    // Info should reflect draining status
    var info_result = try ctx.cli.run(&.{ "worker", "info", worker_id });
    defer info_result.deinit();

    try stdx.testing.assertSucceeded(info_result);
    const output = std.mem.trim(u8, info_result.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, output, "drain") != null);
}

test "e2e/worker: drain non-existent worker" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var result = try ctx.cli.run(&.{ "worker", "drain", "no-such-worker" });
    defer result.deinit();

    // Should fail gracefully
    try testing.expect(!result.succeeded());
}

// =============================================================================
// Worker List Edge Cases
// =============================================================================

test "e2e/worker: list with no workers" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Fresh server — no workers registered
    var result = try ctx.cli.run(&.{ "worker", "list" });
    defer result.deinit();

    // Should succeed (empty list is valid)
    try stdx.testing.assertSucceeded(result);
}

test "e2e/worker: list with limit" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register several workers
    try ctx.exec(&.{ "worker", "register", "lim-w1", "task_a" });
    try ctx.exec(&.{ "worker", "register", "lim-w2", "task_b" });
    try ctx.exec(&.{ "worker", "register", "lim-w3", "task_c" });

    // List with limit
    var result = try ctx.cli.run(&.{ "worker", "list", "--limit", "2" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

// =============================================================================
// REST API — Worker Endpoints
// =============================================================================

test "e2e/http/worker: GET /workers returns 200 with JSON" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/workers");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
    // Empty server returns empty JSON array
    try testing.expect(resp.bodyContains("["));
}

test "e2e/http/worker: GET /workers returns registered workers" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Register a worker via CLI
    try ctx.exec(&.{ "worker", "register", "http-w1", "process" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/workers");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
    try testing.expect(resp.bodyContains("http-w1"));
    try testing.expect(resp.bodyContains("\"worker_id\""));
    try testing.expect(resp.bodyContains("\"status\""));
}

test "e2e/http/worker: GET /workers/:id returns worker detail" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Register a worker via CLI
    try ctx.exec(&.{ "worker", "register", "http-detail-w1", "analyze" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/workers/http-detail-w1");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
    try testing.expect(resp.bodyContains("http-detail-w1"));
    try testing.expect(resp.bodyContains("\"worker_type\""));
    try testing.expect(resp.bodyContains("\"current_load\""));
    try testing.expect(resp.bodyContains("\"processes\""));
}

test "e2e/http/worker: GET /workers/:id not found" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/workers/nonexistent-worker-xyz");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
    // Should contain error message
    try testing.expect(resp.bodyContains("not found") or resp.bodyContains("error") or resp.bodyContains("Worker not found"));
}

// =============================================================================
// REST API — Workers in Action Detail
// =============================================================================

test "e2e/http/worker: action detail includes workers array" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Register an action and a worker
    try ctx.exec(&.{ "action", "register", "http-act-with-workers" });
    try ctx.exec(&.{ "worker", "register", "act-detail-w1", "http-act-with-workers" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/actions/http-act-with-workers");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
    try testing.expect(resp.bodyContains("\"workers\""));
    try testing.expect(resp.bodyContains("act-detail-w1"));
}

// =============================================================================
// Worker Re-registration
// =============================================================================

test "e2e/worker: re-register updates worker" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const worker_id = "rereg-w1";

    // Register once
    try ctx.exec(&.{ "worker", "register", worker_id, "task_a" });

    // Register again (same ID, different task type — should update)
    try ctx.exec(&.{ "worker", "register", worker_id, "task_b" });

    // List should show the worker
    var result = try ctx.cli.run(&.{ "worker", "list" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, output, worker_id) != null);
}

// =============================================================================
// Namespace Isolation
// =============================================================================

test "e2e/worker: different namespaces" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register workers in different namespaces
    try ctx.exec(&.{ "worker", "register", "ns-w1", "process", "--namespace", "ns-alpha" });
    try ctx.exec(&.{ "worker", "register", "ns-w2", "process", "--namespace", "ns-beta" });

    // Both should register without conflict
    var result1 = try ctx.cli.run(&.{ "worker", "list", "--namespace", "ns-alpha" });
    defer result1.deinit();
    try stdx.testing.assertSucceeded(result1);

    var result2 = try ctx.cli.run(&.{ "worker", "list", "--namespace", "ns-beta" });
    defer result2.deinit();
    try stdx.testing.assertSucceeded(result2);
}
