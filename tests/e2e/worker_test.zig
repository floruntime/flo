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

    // Every type given is registered, not just the first.
    var info = try ctx.cli.run(&.{ "worker", "info", "multi-worker" });
    defer info.deinit();
    try testing.expectEqual(@as(u8, 0), info.exit_code);
    try testing.expect(info.stdoutContains("Processes (3):"));
    inline for (.{ "task_x", "task_y", "task_z" }) |t| try testing.expect(info.stdoutContains(t));
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

    try testing.expectEqual(@as(u8, 1), result.exit_code);
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

    try testing.expectEqual(@as(u8, 1), result.exit_code);
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
    try testing.expectEqualStrings("(no workers)", std.mem.trim(u8, result.stdout, &std.ascii.whitespace));

    var json = try ctx.cli.run(&.{ "worker", "list", "-o", "json" });
    defer json.deinit();
    try testing.expectEqualStrings("[]", std.mem.trim(u8, json.stdout, &std.ascii.whitespace));
}

test "e2e/worker: list with limit" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register several workers
    try ctx.exec(&.{ "worker", "register", "lim-w1", "task_a" });
    try ctx.exec(&.{ "worker", "register", "lim-w2", "task_b" });
    try ctx.exec(&.{ "worker", "register", "lim-w3", "task_c" });

    // The first two by id.
    var result = try ctx.cli.run(&.{ "worker", "list", "--limit", "2", "-o", "raw" });
    defer result.deinit();
    try testing.expectEqualStrings("lim-w1\nlim-w2", std.mem.trim(u8, result.stdout, &std.ascii.whitespace));
}

test "e2e/worker: list on a node of several shards shows every shard's workers once, in id order" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .shards = 4 } });
    defer ctx.deinit();

    // Registrations land on their workers' shards; enough ids to reach all four.
    var name: [8]u8 = undefined;
    for (0..12) |i| try ctx.exec(&.{ "worker", "register", try std.fmt.bufPrint(&name, "sh-w{d:0>2}", .{i}), "task_a" });

    var want: std.ArrayList(u8) = .empty;
    defer want.deinit(testing.allocator);
    for (0..12) |i| try want.print(testing.allocator, "sh-w{d:0>2}\n", .{i});

    var all = try ctx.cli.run(&.{ "worker", "list", "-o", "raw" });
    defer all.deinit();
    try testing.expectEqualStrings(want.items, all.stdout);

    var json = try ctx.cli.run(&.{ "worker", "list", "-o", "json" });
    defer json.deinit();
    try testing.expectEqual(@as(usize, 12), json.stdoutCount("\"worker_id\":\"sh-w"));
    try testing.expectEqual(@as(usize, 12), json.stdoutCount("\"status\":\"active\""));
}

// =============================================================================
// REST API — Worker Endpoints
// =============================================================================

test "e2e/http/worker: GET /workers with none registered is one empty page" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/workers");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expectEqualStrings("{\"workers\":[],\"has_more\":false,\"cursor\":null}", resp.bodyTrimmed());
}

test "e2e/http/worker: GET /workers pages every shard's workers once, in id order, and only the namespace's" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .shards = 2 },
    });
    defer ctx.deinit();
    const ids = [_][]const u8{ "w-a", "w-b", "w-c", "w-d", "w-e" };
    for (ids) |id| try ctx.exec(&.{ "worker", "register", id, "process" });
    try ctx.exec(&.{ "worker", "register", "w-other", "process", "-n", "other" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    var seen: std.ArrayList([]u8) = .empty;
    defer {
        for (seen.items) |id| testing.allocator.free(id);
        seen.deinit(testing.allocator);
    }
    var path_buf: [128]u8 = undefined;
    var path: []const u8 = "/api/v1/workers?limit=2";
    var cursor: ?[]u8 = null;
    defer if (cursor) |c| testing.allocator.free(c);
    var pages: usize = 0;
    while (true) : (pages += 1) {
        try testing.expect(pages < 10);
        var resp = try http.get(path);
        defer resp.deinit();
        try testing.expectEqual(@as(u16, 200), resp.status);
        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, resp.bodyTrimmed(), .{});
        defer parsed.deinit();
        const root = parsed.value.object;
        const workers = root.get("workers").?.array.items;
        try testing.expect(workers.len <= 2);
        for (workers) |w| {
            try seen.append(testing.allocator, try testing.allocator.dupe(u8, w.object.get("worker_id").?.string));
            try testing.expectEqualStrings("default", w.object.get("namespace").?.string);
            try testing.expectEqualStrings("action", w.object.get("worker_type").?.string);
            try testing.expectEqualStrings("active", w.object.get("status").?.string);
        }
        try testing.expectEqual(root.get("has_more").?.bool, root.get("cursor").? == .string);
        if (root.get("cursor").? != .string) break;
        if (cursor) |c| testing.allocator.free(c);
        cursor = try testing.allocator.dupe(u8, root.get("cursor").?.string);
        path = try std.fmt.bufPrint(&path_buf, "/api/v1/workers?limit=2&cursor={s}", .{cursor.?});
    }
    try testing.expectEqual(ids.len, seen.items.len);
    for (ids, seen.items) |want, got| try testing.expectEqualStrings(want, got);
    try testing.expect(pages >= 2);
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

// =============================================================================
// Worker Expiry
// =============================================================================

/// Poll `worker info` until `done` holds for its result, or `within_ms`
/// pass: a deadline, not a count, since each poll spawns the CLI and takes
/// longer on a loaded machine.
fn pollWorkerInfo(ctx: *stdx.testing.TestContext, id: []const u8, within_ms: u64, comptime done: fn (stdx.testing.CommandResult) bool) !bool {
    const deadline = stdx.time.monotonicMs() + within_ms;
    while (stdx.time.monotonicMs() < deadline) {
        var info = try ctx.cli.run(&.{ "worker", "info", id });
        defer info.deinit();
        if (done(info)) return true;
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    return false;
}

// Each test asserts one transition: removal is far off while unhealthy is
// checked, so a slow poll can't step past it.
test "e2e/worker: a worker that stops heartbeating is marked unhealthy and refused work" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{
        .extra_config = "[workers]\nheartbeat_timeout_ms = 1000\nremove_after_ms = 60000\n",
    } });
    defer ctx.deinit();
    try ctx.exec(&.{ "worker", "register", "silent-w", "process" });

    const Unhealthy = struct {
        fn done(r: stdx.testing.CommandResult) bool {
            return r.exit_code == 0 and r.stdoutContains("Status:          unhealthy");
        }
    };
    try testing.expect(try pollWorkerInfo(ctx, "silent-w", 20_000, Unhealthy.done));

    var awaited = try ctx.cli.run(&.{ "worker", "await", "process", "-w", "silent-w", "-b", "0" });
    defer awaited.deinit();
    try testing.expectEqual(@as(u8, 3), awaited.exit_code);
    try testing.expectEqualStrings("Error: worker missed its heartbeats; send one, or register again [bad_request/worker_unhealthy]", std.mem.trim(u8, awaited.stderr, &std.ascii.whitespace));
}

test "e2e/worker: a worker silent past remove_after_ms is removed" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{
        .extra_config = "[workers]\nheartbeat_timeout_ms = 1000\nremove_after_ms = 2000\n",
    } });
    defer ctx.deinit();
    try ctx.exec(&.{ "worker", "register", "silent-w", "process" });

    const Removed = struct {
        fn done(r: stdx.testing.CommandResult) bool {
            return r.exit_code == 1 and std.mem.eql(u8, std.mem.trim(u8, r.stderr, &std.ascii.whitespace), "Error: worker not found [not_found]");
        }
    };
    try testing.expect(try pollWorkerInfo(ctx, "silent-w", 20_000, Removed.done));
    var listed = try ctx.cli.run(&.{ "worker", "list" });
    defer listed.deinit();
    try testing.expectEqual(@as(u8, 0), listed.exit_code);
    try testing.expectEqualStrings("(no workers)", std.mem.trim(u8, listed.stdout, &std.ascii.whitespace));
}
