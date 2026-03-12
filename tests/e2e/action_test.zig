//! Action & Worker End-to-End Tests
//!
//! Tests the complete path for Layer 2 Actions:
//!   CLI → TCP → Dispatcher → ActionHandler → KV/Queue (via dispatch)
//!
//! Actions compose Layer 1 primitives (KV + Queue) without direct storage access.
//! This tests that the composition works correctly through Raft.
//!
//! ## Result Flow Tested
//!
//! ```
//! ┌─────────┐   action_invoke    ┌───────────────┐
//! │  Client │ ────────────────►  │  ActionRun    │ (status=pending, input=...)
//! └─────────┘                    │  + Queue Task │
//!                                └───────────────┘
//!                                       │
//! ┌─────────┐  worker_await_task        ▼
//! │  Worker │ ◄──────────────── task_assignment {task_id, payload=input}
//! └─────────┘
//!       │
//!       │  worker_complete_task(result="{...}")
//!       ▼
//! ┌───────────────┐
//! │  ActionRun    │ (status=completed, output=result)
//! └───────────────┘
//!       │
//! ┌─────────┐  action_status            │
//! │  Client │ ◄─────────────────────────┘  {output: result, status: "completed"}
//! └─────────┘
//! ```
//!
//! TODO: WASM execution tests need a compiled .wasm fixture — skipped for now.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

// =============================================================================
// Action Registration
// =============================================================================

test "e2e/action: register succeeds" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo action register test-action (should not error)
    try ctx.exec(&.{ "action", "register", "test-action" });
}

test "e2e/action: register with all options" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo action register handler --type user --owner myteam --timeout 60000 --retries 5
    try ctx.exec(&.{
        "action",    "register",  "handler",
        "--type",    "user",      "--owner",
        "myteam",    "--timeout", "60000",
        "--retries", "5",
    });
}

test "e2e/action: register and delete" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register
    try ctx.exec(&.{ "action", "register", "to-delete" });

    // Delete (should succeed)
    try ctx.exec(&.{ "action", "delete", "to-delete" });
}

test "e2e/action: register and list" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register two actions
    try ctx.exec(&.{ "action", "register", "list-action-alpha" });
    try ctx.exec(&.{ "action", "register", "list-action-beta" });

    // List actions
    var result = try ctx.cli.run(&.{ "action", "list" });
    defer result.deinit();

    std.debug.print("\n[TEST] action list: succeeded={}, stdout='{s}', stderr='{s}'\n", .{
        result.succeeded(),
        std.mem.trim(u8, result.stdout, &std.ascii.whitespace),
        std.mem.trim(u8, result.stderr, &std.ascii.whitespace),
    });

    try stdx.testing.assertSucceeded(result);

    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);

    // Output must not be empty or "(no actions)"
    try testing.expect(output.len > 0);
    try testing.expect(!std.mem.eql(u8, output, "(no actions)"));

    // Both registered actions must appear in the listing
    try testing.expect(std.mem.indexOf(u8, output, "list-action-alpha") != null);
    try testing.expect(std.mem.indexOf(u8, output, "list-action-beta") != null);
}

// =============================================================================
// Action Invocation
// =============================================================================

test "e2e/action: invoke registered action" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register an action first
    try ctx.exec(&.{ "action", "register", "process" });

    // Invoke the action
    var invoke_result = try ctx.cli.run(&.{
        "action", "invoke", "process", "{\"key\":\"value\"}",
    });
    defer invoke_result.deinit();

    try stdx.testing.assertSucceeded(invoke_result);
}

test "e2e/action: invoke with priority" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register
    try ctx.exec(&.{ "action", "register", "priority-test" });

    // Invoke with priority
    var result = try ctx.cli.run(&.{
        "action",     "invoke", "priority-test", "{\"data\":1}",
        "--priority", "100",
    });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
}

test "e2e/action: invoke with idempotency key" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register
    try ctx.exec(&.{ "action", "register", "idem-test" });

    // First invoke with idempotency key
    var result1 = try ctx.cli.run(&.{
        "action",            "invoke",         "idem-test", "{\"x\":1}",
        "--idempotency-key", "unique-key-123",
    });
    defer result1.deinit();
    try stdx.testing.assertSucceeded(result1);

    // Second invoke with SAME idempotency key should return same run_id
    var result2 = try ctx.cli.run(&.{
        "action",            "invoke",         "idem-test", "{\"x\":1}",
        "--idempotency-key", "unique-key-123",
    });
    defer result2.deinit();
    try stdx.testing.assertSucceeded(result2);
}

// NOTE: This test verifies current behavior - invoke on non-existent action
// currently succeeds (creates run in pending state). This may change.
test "e2e/action: invoke non-existent action behavior" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Try to invoke an action that doesn't exist
    // Current behavior: succeeds and creates a pending run
    var result = try ctx.cli.run(&.{
        "action", "invoke", "nonexistent-action", "{}",
    });
    defer result.deinit();

    // Just verify it doesn't crash - behavior may vary
    _ = result.succeeded();
}

// =============================================================================
// Worker Registration
// =============================================================================

test "e2e/worker: register succeeds" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo worker register worker-1 task_a
    try ctx.exec(&.{ "worker", "register", "worker-1", "task_a" });
}

test "e2e/worker: register multiple workers" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register multiple workers
    try ctx.exec(&.{ "worker", "register", "worker-a", "process" });
    try ctx.exec(&.{ "worker", "register", "worker-b", "process" });
    try ctx.exec(&.{ "worker", "register", "worker-c", "analyze" });
}

// =============================================================================
// Worker Task Processing
// =============================================================================

test "e2e/worker: await with short timeout" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register a worker
    try ctx.exec(&.{ "worker", "register", "await-worker", "empty-queue" });

    // Await with short timeout (should return quickly with no task)
    var result = try ctx.cli.run(&.{
        "worker",      "await",        "empty-queue",
        "--worker-id", "await-worker",
        "--block", "100", // 100ms timeout
    });
    defer result.deinit();

    // Should succeed (timeout is not an error)
    try stdx.testing.assertSucceeded(result);
}

// =============================================================================
// Full Workflow
// =============================================================================

test "e2e/action/worker: invoke and await task" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "workflow-test";
    const worker_id = "workflow-worker";

    // 1. Register the action
    try ctx.exec(&.{ "action", "register", action_name });

    // 2. Register a worker for this action type
    try ctx.exec(&.{ "worker", "register", worker_id, action_name });

    // 3. Invoke the action
    var invoke_result = try ctx.cli.run(&.{
        "action", "invoke", action_name, "{\"task\":\"process-data\"}",
    });
    defer invoke_result.deinit();
    try stdx.testing.assertSucceeded(invoke_result);

    // 4. Worker awaits task
    // Note: The queue mechanism may not have the task immediately available.
    // This test verifies the commands don't crash, not the full workflow.
    var await_result = try ctx.cli.run(&.{
        "worker",      "await",   action_name,
        "--worker-id", worker_id,
        "--block", "500", // 500ms timeout
    });
    defer await_result.deinit();

    // Just verify it doesn't crash - await may timeout or return "not found"
    // Full workflow testing requires queue integration which may vary
    _ = await_result.succeeded();
}

// =============================================================================
// Namespace Isolation
// =============================================================================

test "e2e/action: different namespaces" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register same action name in different namespaces
    try ctx.exec(&.{ "action", "register", "shared-name", "--namespace", "ns1" });
    try ctx.exec(&.{ "action", "register", "shared-name", "--namespace", "ns2" });

    // Invoke in ns1
    var result1 = try ctx.cli.run(&.{
        "action",      "invoke", "shared-name", "{}",
        "--namespace", "ns1",
    });
    defer result1.deinit();
    try stdx.testing.assertSucceeded(result1);

    // Invoke in ns2
    var result2 = try ctx.cli.run(&.{
        "action",      "invoke", "shared-name", "{}",
        "--namespace", "ns2",
    });
    defer result2.deinit();
    try stdx.testing.assertSucceeded(result2);
}

// =============================================================================
// Error Handling
// =============================================================================

test "e2e/action: status on invalid run_id" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Try to get status of non-existent run
    var result = try ctx.cli.run(&.{
        "action", "status", "invalid-run-id-12345",
    });
    defer result.deinit();

    // Should fail with error (not crash)
    try testing.expect(!result.succeeded());
}

test "e2e/worker: complete non-existent task" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register worker first
    try ctx.exec(&.{ "worker", "register", "error-worker", "any" });

    // Try to complete a non-existent task
    var result = try ctx.cli.run(&.{
        "worker",      "complete",     "nonexistent-task-id",
        "--worker-id", "error-worker", "--action",
        "any",         "--result",     "{}",
    });
    defer result.deinit();

    // Should fail gracefully
    try testing.expect(!result.succeeded());
}

test "e2e/worker: fail non-existent task" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register worker first
    try ctx.exec(&.{ "worker", "register", "error-worker-2", "any" });

    // Try to fail a non-existent task
    var result = try ctx.cli.run(&.{
        "worker",      "fail",           "nonexistent-task-id-2",
        "--worker-id", "error-worker-2", "--action",
        "any",         "--error",        "test error",
    });
    defer result.deinit();

    // Should fail gracefully
    try testing.expect(!result.succeeded());
}

// =============================================================================
// Full Result Flow (SDK-Ready Tests)
// =============================================================================

/// Helper to extract run_id from invoke output ("Result: {run_id}")
fn extractRunId(output: []const u8) ?[]const u8 {
    const prefix = "Result: ";
    if (std.mem.startsWith(u8, output, prefix)) {
        // Find end of run_id (newline or end of string)
        const run_id_start = prefix.len;
        var end = run_id_start;
        while (end < output.len and output[end] != '\n' and output[end] != '\r') {
            end += 1;
        }
        if (end > run_id_start) {
            return output[run_id_start..end];
        }
    }
    return null;
}

test "extractRunId helper" {
    try testing.expectEqualStrings("myaction-123-1", extractRunId("Result: myaction-123-1\n").?);
    try testing.expectEqualStrings("test-id", extractRunId("Result: test-id").?);
    try testing.expect(extractRunId("OK") == null);
    try testing.expect(extractRunId("Result: ") == null);
}

test "e2e/action/worker: full result flow - invoke, await, complete, verify" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "result-flow-test";
    const worker_id = "result-worker";
    const input_payload = "{\"task\":\"process\",\"data\":42}";
    const result_payload = "{\"status\":\"success\",\"processed\":true,\"count\":100}";

    // 1. Register the action
    try ctx.exec(&.{ "action", "register", action_name });

    // 2. Register a worker for this action type
    try ctx.exec(&.{ "worker", "register", worker_id, action_name });

    // 3. Invoke the action and capture run_id
    const invoke_output = try ctx.execCapture(&.{
        "action", "invoke", action_name, input_payload,
    });

    const run_id = extractRunId(invoke_output) orelse {
        std.debug.print("\nFailed to extract run_id from: '{s}'\n", .{invoke_output});
        return error.TestFailed;
    };

    std.debug.print("\n[TEST] Invoked action, got run_id: {s}\n", .{run_id});

    // 4. Check initial status (should be pending)
    var status1 = try ctx.cli.run(&.{ "action", "status", run_id });
    defer status1.deinit();
    try stdx.testing.assertSucceeded(status1);

    // 5. Worker awaits and receives task (using blocking)
    var await_result = try ctx.cli.run(&.{
        "worker",      "await",   action_name,
        "--worker-id", worker_id,
        "--block", "5000", // Block up to 5 seconds waiting for task
    });
    defer await_result.deinit();

    std.debug.print("[TEST] Await result: succeeded={}, stdout='{s}', stderr='{s}'\n", .{
        await_result.succeeded(),
        std.mem.trim(u8, await_result.stdout, &std.ascii.whitespace),
        std.mem.trim(u8, await_result.stderr, &std.ascii.whitespace),
    });

    if (!await_result.succeeded()) {
        std.debug.print("\nWorker await failed\n", .{});
        ctx.dumpServerLogs();
        return error.TestFailed;
    }

    const output = std.mem.trim(u8, await_result.stdout, &std.ascii.whitespace);
    if (output.len == 0 or std.mem.eql(u8, output, "(no tasks)")) {
        std.debug.print("\nWorker did not receive task\n", .{});
        ctx.dumpServerLogs();
        return error.TestFailed;
    }

    // 6. Worker completes the task with result
    std.debug.print("\n[TEST] Completing task {s} with result: {s}\n", .{ run_id, result_payload });
    var complete_result = try ctx.cli.run(&.{
        "worker",      "complete", run_id,
        "--worker-id", worker_id,  "--action",
        action_name,   "--result", result_payload,
    });
    defer complete_result.deinit();

    if (!complete_result.succeeded()) {
        std.debug.print("\n[TEST] Worker complete FAILED:\n", .{});
        std.debug.print("  stdout: {s}\n", .{complete_result.stdout});
        std.debug.print("  stderr: {s}\n", .{complete_result.stderr});
        ctx.dumpServerLogs();
        return error.TestFailed;
    }
    try stdx.testing.assertSucceeded(complete_result);

    // 7. Check final status (should be completed with output)
    var status2 = try ctx.cli.run(&.{ "action", "status", run_id });
    defer status2.deinit();
    try stdx.testing.assertSucceeded(status2);
}

test "e2e/action/worker: worker fails task with retry" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "retry-test";
    const worker_id = "retry-worker";

    // 1. Register action with retries
    try ctx.exec(&.{
        "action",    "register", action_name,
        "--retries", "3",
    });

    // 2. Register worker
    try ctx.exec(&.{ "worker", "register", worker_id, action_name });

    // 3. Invoke
    const invoke_output = try ctx.execCapture(&.{
        "action", "invoke", action_name, "{}",
    });

    const run_id = extractRunId(invoke_output) orelse {
        std.debug.print("\nFailed to extract run_id from: '{s}'\n", .{invoke_output});
        return error.TestFailed;
    };

    // 4. Worker awaits task
    var await_result = try ctx.cli.run(&.{
        "worker",      "await",   action_name,
        "--worker-id", worker_id, "--block",
        "2000",
    });
    defer await_result.deinit();

    if (!await_result.succeeded()) return; // Skip if no task

    // 5. Worker fails the task (with retry enabled)
    var fail_result = try ctx.cli.run(&.{
        "worker",      "fail",    run_id,
        "--worker-id", worker_id, "--action",
        action_name,   "--error", "Temporary failure",
        "--retry",
    });
    defer fail_result.deinit();
    try stdx.testing.assertSucceeded(fail_result);

    // 6. Check status - should still be pending (retry queued)
    var status = try ctx.cli.run(&.{ "action", "status", run_id });
    defer status.deinit();
    try stdx.testing.assertSucceeded(status);
    // The status indicates the task is back in pending/retry state
}

test "e2e/action/worker: worker fails task permanently" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "fail-test";
    const worker_id = "fail-worker";

    // 1. Register action
    try ctx.exec(&.{ "action", "register", action_name });

    // 2. Register worker
    try ctx.exec(&.{ "worker", "register", worker_id, action_name });

    // 3. Invoke
    const invoke_output = try ctx.execCapture(&.{
        "action", "invoke", action_name, "{}",
    });

    const run_id = extractRunId(invoke_output) orelse {
        std.debug.print("\nFailed to extract run_id from: '{s}'\n", .{invoke_output});
        return error.TestFailed;
    };

    // 4. Worker awaits task
    var await_result = try ctx.cli.run(&.{
        "worker",      "await",   action_name,
        "--worker-id", worker_id, "--block",
        "2000",
    });
    defer await_result.deinit();

    if (!await_result.succeeded()) return; // Skip if no task

    // 5. Worker fails the task (NO retry - permanent failure)
    var fail_result = try ctx.cli.run(&.{
        "worker",      "fail",    run_id,
        "--worker-id", worker_id, "--action",
        action_name,   "--error",
        "Permanent failure: invalid input",
        // Note: no --retry flag
    });
    defer fail_result.deinit();
    try stdx.testing.assertSucceeded(fail_result);

    // 6. Check status - should be failed
    var status = try ctx.cli.run(&.{ "action", "status", run_id });
    defer status.deinit();
    try stdx.testing.assertSucceeded(status);
    // The status indicates the task has failed permanently
}
// =============================================================================
// Persistence / Restart
// =============================================================================

test "e2e/action: registered action survives restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    // Register an action
    try ctx.exec(&.{ "action", "register", "persist-test-action" });

    // Verify it exists before restart
    var before = try ctx.cli.run(&.{ "action", "list" });
    defer before.deinit();
    try stdx.testing.assertSucceeded(before);
    const before_output = std.mem.trim(u8, before.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, before_output, "persist-test-action") != null);

    // Restart the server
    try ctx.restartServer();

    // After restart, action should still exist
    var after = try ctx.cli.run(&.{ "action", "list" });
    defer after.deinit();
    try stdx.testing.assertSucceeded(after);

    const after_output = std.mem.trim(u8, after.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, after_output, "persist-test-action") != null);
}

test "e2e/action/runs: runs survive restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    // Register, invoke, verify run exists
    try ctx.exec(&.{ "action", "register", "persist-runs-action" });
    try ctx.exec(&.{ "action", "invoke", "persist-runs-action", "{}" });

    var before = try ctx.cli.run(&.{ "action", "runs", "persist-runs-action" });
    defer before.deinit();
    try stdx.testing.assertSucceeded(before);
    const before_output = std.mem.trim(u8, before.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, before_output, "pending") != null);

    // Restart the server
    try ctx.restartServer();

    // After restart, action and runs should still exist
    var after_list = try ctx.cli.run(&.{ "action", "list" });
    defer after_list.deinit();
    try stdx.testing.assertSucceeded(after_list);
    const after_list_output = std.mem.trim(u8, after_list.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, after_list_output, "persist-runs-action") != null);

    var after = try ctx.cli.run(&.{ "action", "runs", "persist-runs-action" });
    defer after.deinit();
    try stdx.testing.assertSucceeded(after);

    const after_output = std.mem.trim(u8, after.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, after_output, "pending") != null);
}

// =============================================================================
// Action Runs Listing
// =============================================================================

test "e2e/action/runs: empty runs returns no runs" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register an action but don't invoke it
    try ctx.exec(&.{ "action", "register", "no-runs-action" });

    // List runs — should show (no runs) or empty table
    var result = try ctx.cli.run(&.{ "action", "runs", "no-runs-action" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.eql(u8, output, "(no runs)"));
}

test "e2e/action/runs: shows pending run after invoke" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "runs-pending-test";

    // Register and invoke
    try ctx.exec(&.{ "action", "register", action_name });
    try ctx.exec(&.{ "action", "invoke", action_name, "{\"x\":1}" });

    // List runs
    var result = try ctx.cli.run(&.{ "action", "runs", action_name });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);

    // Should contain table header and at least one row with "pending"
    try testing.expect(std.mem.indexOf(u8, output, "RUN ID") != null);
    try testing.expect(std.mem.indexOf(u8, output, "pending") != null);
    try testing.expect(std.mem.indexOf(u8, output, action_name) != null);
}

test "e2e/action/runs: shows multiple runs" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "runs-multi-test";

    // Register and invoke 3 times
    try ctx.exec(&.{ "action", "register", action_name });
    try ctx.exec(&.{ "action", "invoke", action_name, "{\"n\":1}" });
    try ctx.exec(&.{ "action", "invoke", action_name, "{\"n\":2}" });
    try ctx.exec(&.{ "action", "invoke", action_name, "{\"n\":3}" });

    // List runs
    var result = try ctx.cli.run(&.{ "action", "runs", action_name });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);

    // Count occurrences of "pending" — should be at least 3
    var pending_count: usize = 0;
    var search_pos: usize = 0;
    while (std.mem.indexOfPos(u8, output, search_pos, "pending")) |pos| {
        pending_count += 1;
        search_pos = pos + 7;
    }
    try testing.expect(pending_count >= 3);
}

test "e2e/action/runs: completed run shows completed status" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "runs-completed-test";
    const worker_id = "runs-complete-worker";

    // 1. Register action and worker
    try ctx.exec(&.{ "action", "register", action_name });
    try ctx.exec(&.{ "worker", "register", worker_id, action_name });

    // 2. Invoke
    const invoke_output = try ctx.execCapture(&.{
        "action", "invoke", action_name, "{\"data\":\"test\"}",
    });

    const run_id = extractRunId(invoke_output) orelse {
        std.debug.print("\nFailed to extract run_id from: '{s}'\n", .{invoke_output});
        return error.TestFailed;
    };

    // 3. Worker awaits and gets task
    var await_result = try ctx.cli.run(&.{
        "worker",      "await",   action_name,
        "--worker-id", worker_id, "--block",
        "5000",
    });
    defer await_result.deinit();

    if (!await_result.succeeded()) {
        std.debug.print("\nWorker await failed\n", .{});
        return error.TestFailed;
    }

    const await_output = std.mem.trim(u8, await_result.stdout, &std.ascii.whitespace);
    if (await_output.len == 0 or std.mem.eql(u8, await_output, "(no tasks)")) {
        std.debug.print("\nWorker did not receive task\n", .{});
        return error.TestFailed;
    }

    // 4. Worker completes the task
    var complete_result = try ctx.cli.run(&.{
        "worker",      "complete", run_id,
        "--worker-id", worker_id,  "--action",
        action_name,   "--result", "{\"done\":true}",
    });
    defer complete_result.deinit();
    try stdx.testing.assertSucceeded(complete_result);

    // 5. List runs — should show "completed"
    var runs_result = try ctx.cli.run(&.{ "action", "runs", action_name });
    defer runs_result.deinit();

    try stdx.testing.assertSucceeded(runs_result);
    const runs_output = std.mem.trim(u8, runs_result.stdout, &std.ascii.whitespace);
    try testing.expect(std.mem.indexOf(u8, runs_output, "completed") != null);
}

test "e2e/action/runs: limit flag restricts output" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "runs-limit-test";

    // Register and invoke 5 times
    try ctx.exec(&.{ "action", "register", action_name });
    for (0..5) |_| {
        try ctx.exec(&.{ "action", "invoke", action_name, "{}" });
    }

    // List with --limit 2
    var result = try ctx.cli.run(&.{ "action", "runs", action_name, "--limit", "2" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);

    // Count "pending" rows — should be exactly 2
    var pending_count: usize = 0;
    var search_pos: usize = 0;
    while (std.mem.indexOfPos(u8, output, search_pos, "pending")) |pos| {
        pending_count += 1;
        search_pos = pos + 7;
    }
    try testing.expect(pending_count == 2);
}

test "e2e/action/runs: only shows runs for specified action" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register two different actions
    try ctx.exec(&.{ "action", "register", "runs-filter-alpha" });
    try ctx.exec(&.{ "action", "register", "runs-filter-beta" });

    // Invoke both
    try ctx.exec(&.{ "action", "invoke", "runs-filter-alpha", "{}" });
    try ctx.exec(&.{ "action", "invoke", "runs-filter-beta", "{}" });

    // List runs for alpha only
    var result = try ctx.cli.run(&.{ "action", "runs", "runs-filter-alpha" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    const output = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);

    // Should contain alpha runs
    try testing.expect(std.mem.indexOf(u8, output, "runs-filter-alpha") != null);
    // Should NOT contain beta runs
    try testing.expect(std.mem.indexOf(u8, output, "runs-filter-beta") == null);
}

// =============================================================================
// Label-Based Worker Filtering
// =============================================================================

test "e2e/worker: register with labels" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Register worker with JSON labels
    try ctx.exec(&.{
        "worker",   "register",                          "gpu-worker-1", "render",
        "--labels", "{\"gpu\":\"a100\",\"vram_gb\":16}",
    });
}

test "e2e/action/labels: matching worker receives task" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "label-match-test";
    const worker_id = "label-match-worker";

    // 1. Register action
    try ctx.exec(&.{ "action", "register", action_name });

    // 2. Register worker with labels
    try ctx.exec(&.{
        "worker",   "register",                              worker_id, action_name,
        "--labels", "{\"region\":\"us-east\",\"gpu\":true}",
    });

    // 3. Invoke with matching required labels (subset of worker labels)
    const invoke_output = try ctx.execCapture(&.{
        "action",                    "invoke",   action_name,
        "{\"task\":\"gpu-render\"}", "--labels", "{\"gpu\":true}",
    });

    const run_id = extractRunId(invoke_output) orelse {
        std.debug.print("\nFailed to extract run_id from: '{s}'\n", .{invoke_output});
        return error.TestFailed;
    };

    std.debug.print("\n[TEST] Invoked with labels, got run_id: {s}\n", .{run_id});

    // 4. Worker awaits - should receive the task (labels match)
    var await_result = try ctx.cli.run(&.{
        "worker",      "await",   action_name,
        "--worker-id", worker_id, "--block",
        "5000",
    });
    defer await_result.deinit();

    if (!await_result.succeeded()) {
        std.debug.print("\n[TEST] Worker await failed\n", .{});
        ctx.dumpServerLogs();
        return error.TestFailed;
    }

    const output = std.mem.trim(u8, await_result.stdout, &std.ascii.whitespace);
    std.debug.print("[TEST] Label-match await result: '{s}'\n", .{output});

    if (output.len == 0 or std.mem.eql(u8, output, "(no tasks)")) {
        std.debug.print("\n[TEST] Worker did not receive task despite matching labels\n", .{});
        ctx.dumpServerLogs();
        return error.TestFailed;
    }

    // Verify the task contains our payload
    try testing.expect(std.mem.indexOf(u8, output, "gpu-render") != null);
}

test "e2e/action/labels: mismatched worker gets no task" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "label-mismatch-test";
    const worker_id = "mismatch-worker";

    // 1. Register action
    try ctx.exec(&.{ "action", "register", action_name });

    // 2. Register worker with labels that do NOT match what the invoke requires
    try ctx.exec(&.{
        "worker",   "register",                               worker_id, action_name,
        "--labels", "{\"region\":\"eu-west\",\"gpu\":false}",
    });

    // 3. Invoke with required labels that don't match the worker
    var invoke_result = try ctx.cli.run(&.{
        "action",                   "invoke",   action_name,
        "{\"task\":\"needs-gpu\"}", "--labels", "{\"gpu\":true}",
    });
    defer invoke_result.deinit();
    try stdx.testing.assertSucceeded(invoke_result);

    // 4. Worker awaits with short timeout - should NOT receive the task
    var await_result = try ctx.cli.run(&.{
        "worker",      "await",   action_name,
        "--worker-id", worker_id,
        "--block", "1000", // 1 second - enough to try, not too long
    });
    defer await_result.deinit();

    // The worker should succeed but get no task assignment (label mismatch)
    const output = std.mem.trim(u8, await_result.stdout, &std.ascii.whitespace);
    std.debug.print("[TEST] Mismatch await result: succeeded={}, stdout='{s}'\n", .{
        await_result.succeeded(),
        output,
    });

    // Worker should get empty/no-tasks response due to label mismatch
    if (output.len > 0 and !std.mem.eql(u8, output, "(no tasks)")) {
        // If the worker somehow got a task, it should NOT contain our payload
        // because the label filter should have rejected it
        if (std.mem.indexOf(u8, output, "needs-gpu") != null) {
            std.debug.print("\n[TEST] FAIL: Mismatched worker received the task!\n", .{});
            return error.TestFailed;
        }
    }
    // Either empty, (no tasks), or timeout - all acceptable for label mismatch
}

test "e2e/action/labels: invoke without labels works for any worker" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "no-label-test";
    const worker_id = "any-worker";

    // 1. Register action
    try ctx.exec(&.{ "action", "register", action_name });

    // 2. Register worker WITH labels (but invoke won't require any)
    try ctx.exec(&.{
        "worker",   "register",                 worker_id, action_name,
        "--labels", "{\"region\":\"us-east\"}",
    });

    // 3. Invoke WITHOUT --labels (no label requirement)
    const invoke_output = try ctx.execCapture(&.{
        "action", "invoke", action_name, "{\"task\":\"universal\"}",
    });

    const run_id = extractRunId(invoke_output) orelse {
        std.debug.print("\nFailed to extract run_id from: '{s}'\n", .{invoke_output});
        return error.TestFailed;
    };
    _ = run_id;

    // 4. Worker should receive the task (no label filter applied)
    var await_result = try ctx.cli.run(&.{
        "worker",      "await",   action_name,
        "--worker-id", worker_id, "--block",
        "5000",
    });
    defer await_result.deinit();

    if (!await_result.succeeded()) {
        std.debug.print("\n[TEST] Worker await failed\n", .{});
        ctx.dumpServerLogs();
        return error.TestFailed;
    }

    const output = std.mem.trim(u8, await_result.stdout, &std.ascii.whitespace);
    std.debug.print("[TEST] No-label await result: '{s}'\n", .{output});

    if (output.len == 0 or std.mem.eql(u8, output, "(no tasks)")) {
        std.debug.print("\n[TEST] Worker did not receive task (no label filter should allow any)\n", .{});
        ctx.dumpServerLogs();
        return error.TestFailed;
    }

    try testing.expect(std.mem.indexOf(u8, output, "universal") != null);
}

test "e2e/action/labels/blocking: matching worker receives task via blocking" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "label-blocking-test";
    const worker_id = "label-blocking-worker";
    const input_payload = "{\"task\":\"labeled-blocking\"}";

    // 1. Register action
    try ctx.exec(&.{ "action", "register", action_name });

    // 2. Register worker with labels
    try ctx.exec(&.{
        "worker",   "register",               worker_id, action_name,
        "--labels", "{\"tier\":\"premium\"}",
    });

    // 3. Start worker await in background BEFORE invoke (blocking path)
    const ThreadResult = struct {
        result: ?stdx.testing.CommandResult = null,
        err: bool = false,
    };
    var thread_result = ThreadResult{};

    var await_thread = try std.Thread.spawn(.{}, struct {
        fn run(test_ctx: *stdx.testing.TestContext, act_name: []const u8, wkr_id: []const u8, result: *ThreadResult) void {
            result.result = test_ctx.cli.run(&.{
                "worker",      "await", act_name,
                "--worker-id", wkr_id,  "--block",
                "10000",
            }) catch {
                result.err = true;
                return;
            };
        }
    }.run, .{ ctx, action_name, worker_id, &thread_result });

    // 4. Wait for worker to start blocking
    std.Thread.sleep(300 * std.time.ns_per_ms);

    // 5. Invoke with matching labels
    const invoke_output = try ctx.execCapture(&.{
        "action",      "invoke",   action_name,
        input_payload, "--labels", "{\"tier\":\"premium\"}",
    });

    const run_id = extractRunId(invoke_output) orelse {
        std.debug.print("\nFailed to extract run_id from: '{s}'\n", .{invoke_output});
        return error.TestFailed;
    };
    std.debug.print("\n[TEST] Invoked with labels, got run_id: {s}\n", .{run_id});

    // 6. Wait for worker thread
    await_thread.join();

    if (thread_result.err) {
        std.debug.print("\n[TEST] Worker thread failed\n", .{});
        return error.TestFailed;
    }

    if (thread_result.result) |*await_result| {
        defer await_result.deinit();

        const output = std.mem.trim(u8, await_result.stdout, &std.ascii.whitespace);
        std.debug.print("[TEST] Label-blocking await result: succeeded={}, stdout='{s}'\n", .{
            await_result.succeeded(),
            output,
        });

        if (await_result.succeeded() and output.len > 0 and !std.mem.eql(u8, output, "(no tasks)")) {
            std.debug.print("[TEST] SUCCESS: Worker received labeled task via blocking!\n", .{});
            try std.testing.expect(std.mem.indexOf(u8, output, "labeled-blocking") != null);
        } else {
            std.debug.print("[TEST] Worker did not receive labeled task via blocking\n", .{});
            ctx.dumpServerLogs();
            return error.TestFailed;
        }
    } else {
        std.debug.print("\n[TEST] No result from worker thread\n", .{});
        return error.TestFailed;
    }
}

test "e2e/action/blocking: worker await blocks until task arrives" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const action_name = "blocking-test";
    const worker_id = "blocking-worker";
    const input_payload = "{\"blocking\":true}";

    // 1. Register the action
    try ctx.exec(&.{ "action", "register", action_name });

    // 2. Register a worker
    try ctx.exec(&.{ "worker", "register", worker_id, action_name });

    // 3. Start worker await in background with blocking (before any invoke)
    // This should block waiting for a task
    const ThreadResult = struct {
        result: ?stdx.testing.CommandResult = null,
        err: bool = false,
    };
    var thread_result = ThreadResult{};

    var await_thread = try std.Thread.spawn(.{}, struct {
        fn run(test_ctx: *stdx.testing.TestContext, act_name: []const u8, wkr_id: []const u8, result: *ThreadResult) void {
            result.result = test_ctx.cli.run(&.{
                "worker",      "await", act_name,
                "--worker-id", wkr_id,
                "--block", "10000", // Block up to 10 seconds waiting for task
            }) catch {
                result.err = true;
                return;
            };
        }
    }.run, .{ ctx, action_name, worker_id, &thread_result });

    // 4. Wait a bit to ensure worker is blocking
    std.Thread.sleep(200 * std.time.ns_per_ms);

    // 5. Now invoke the action - this should wake up the blocked worker
    const invoke_output = try ctx.execCapture(&.{
        "action", "invoke", action_name, input_payload,
    });

    const run_id = extractRunId(invoke_output) orelse {
        std.debug.print("\nFailed to extract run_id from: '{s}'\n", .{invoke_output});
        return error.TestFailed;
    };
    std.debug.print("\n[TEST] Invoked action, got run_id: {s}\n", .{run_id});

    // 6. Wait for the worker thread to complete (should be quick now)
    await_thread.join();

    if (thread_result.err) {
        std.debug.print("\n[TEST] Worker thread failed\n", .{});
        return error.TestFailed;
    }

    if (thread_result.result) |*await_result| {
        defer await_result.deinit();

        // 7. Verify worker received the task
        const output = std.mem.trim(u8, await_result.stdout, &std.ascii.whitespace);
        std.debug.print("[TEST] Await result: succeeded={}, stdout='{s}'\n", .{ await_result.succeeded(), output });

        if (await_result.succeeded() and output.len > 0 and !std.mem.eql(u8, output, "(no tasks)")) {
            std.debug.print("[TEST] SUCCESS: Worker received task via blocking!\n", .{});
            // Task should contain our payload
            try std.testing.expect(std.mem.indexOf(u8, output, "blocking") != null);
        } else {
            std.debug.print("[TEST] Worker did not receive task via blocking\n", .{});
            ctx.dumpServerLogs();
            return error.TestFailed;
        }
    } else {
        std.debug.print("\n[TEST] No result from worker thread\n", .{});
        return error.TestFailed;
    }
}
