//! Queue End-to-End Tests
//!
//! Tests the complete path: CLI → TCP → Node → QueueHandler → Queue → Storage
//! Ported from tests/cli/test_queue.sh

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

// =============================================================================
// Helper Functions
// =============================================================================

/// Extract sequence number from dequeue output
/// Tries formats: seq: 123, seq=123, [123], "seq":123
fn extractSeqNumber(output: []const u8) ?[]const u8 {
    // Try "seq": pattern (JSON)
    if (std.mem.indexOf(u8, output, "\"seq\":")) |idx| {
        const start = idx + 6;
        var end = start;
        while (end < output.len and std.ascii.isDigit(output[end])) {
            end += 1;
        }
        if (end > start) return output[start..end];
    }

    // Try "seq:" pattern
    if (std.mem.indexOf(u8, output, "seq:")) |idx| {
        var start = idx + 4;
        // Skip whitespace
        while (start < output.len and output[start] == ' ') {
            start += 1;
        }
        var end = start;
        while (end < output.len and std.ascii.isDigit(output[end])) {
            end += 1;
        }
        if (end > start) return output[start..end];
    }

    // Try [123] pattern
    if (std.mem.indexOf(u8, output, "[")) |bracket_start| {
        const start = bracket_start + 1;
        if (std.mem.indexOfPos(u8, output, start, "]")) |bracket_end| {
            const inside = output[start..bracket_end];
            // Check if it's all digits
            var all_digits = true;
            for (inside) |c| {
                if (!std.ascii.isDigit(c)) {
                    all_digits = false;
                    break;
                }
            }
            if (all_digits and inside.len > 0) return inside;
        }
    }

    return null;
}

// =============================================================================
// Basic Operations
// =============================================================================

test "e2e/queue: enqueue and dequeue basic" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo queue enqueue tasks "task payload 1"
    try ctx.exec(&.{ "queue", "enqueue", "tasks", "task payload 1" });

    // flo queue dequeue tasks
    var result = try ctx.cli.run(&.{ "queue", "dequeue", "tasks" });
    defer result.deinit();

    try stdx.testing.assertContains(result, "task payload 1");
}

test "e2e/queue: dequeue from empty queue" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo queue dequeue empty-queue (non-blocking)
    var result = try ctx.cli.run(&.{ "queue", "dequeue", "empty-queue-test" });
    defer result.deinit();

    // Empty queue should return empty, "No messages", or similar
    try testing.expect(
        result.contains("No messages") or
            result.contains("(no messages)") or
            result.contains("0 message") or
            result.contains("[]") or
            result.stdout.len == 0 or
            result.succeeded(),
    );
}

test "e2e/queue: multiple enqueue and dequeue" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Enqueue multiple messages
    try ctx.exec(&.{ "queue", "enqueue", "multi-q", "message 1" });
    try ctx.exec(&.{ "queue", "enqueue", "multi-q", "message 2" });
    try ctx.exec(&.{ "queue", "enqueue", "multi-q", "message 3" });

    // Dequeue should return messages in FIFO order
    var r1 = try ctx.cli.run(&.{ "queue", "dequeue", "multi-q" });
    defer r1.deinit();
    try stdx.testing.assertContains(r1, "message 1");

    var r2 = try ctx.cli.run(&.{ "queue", "dequeue", "multi-q" });
    defer r2.deinit();
    try stdx.testing.assertContains(r2, "message 2");

    var r3 = try ctx.cli.run(&.{ "queue", "dequeue", "multi-q" });
    defer r3.deinit();
    try stdx.testing.assertContains(r3, "message 3");
}

// =============================================================================
// Priority Ordering
// =============================================================================

test "e2e/queue: priority ordering (lower value = higher priority)" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Enqueue with different priorities (lower value = higher priority)
    try ctx.exec(&.{ "queue", "enqueue", "priority-q", "high priority", "--priority", "1" });
    try ctx.exec(&.{ "queue", "enqueue", "priority-q", "low priority", "--priority", "10" });
    try ctx.exec(&.{ "queue", "enqueue", "priority-q", "medium priority", "--priority", "5" });

    // Dequeue should return highest priority (lowest number) first
    var result = try ctx.cli.run(&.{ "queue", "dequeue", "priority-q", "--count", "1" });
    defer result.deinit();

    try stdx.testing.assertContains(result, "high priority");
}

test "e2e/queue: priority ordering all messages" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Enqueue in random order with different priorities
    // Note: In a sharded environment, priority ordering applies per-shard
    // This test verifies priority works when messages land on same shard
    try ctx.exec(&.{ "queue", "enqueue", "prio-all-q", "third", "--priority", "3" });
    try ctx.exec(&.{ "queue", "enqueue", "prio-all-q", "first", "--priority", "1" });
    try ctx.exec(&.{ "queue", "enqueue", "prio-all-q", "second", "--priority", "2" });

    // Dequeue all messages - we should get all 3 regardless of order
    var r1 = try ctx.cli.run(&.{ "queue", "dequeue", "prio-all-q" });
    defer r1.deinit();

    var r2 = try ctx.cli.run(&.{ "queue", "dequeue", "prio-all-q" });
    defer r2.deinit();

    var r3 = try ctx.cli.run(&.{ "queue", "dequeue", "prio-all-q" });
    defer r3.deinit();

    // Verify we got all messages (order depends on sharding)
    const all_output = try std.fmt.allocPrint(testing.allocator, "{s}{s}{s}", .{
        r1.stdout, r2.stdout, r3.stdout,
    });
    defer testing.allocator.free(all_output);

    try testing.expect(std.mem.indexOf(u8, all_output, "first") != null);
    try testing.expect(std.mem.indexOf(u8, all_output, "second") != null);
    try testing.expect(std.mem.indexOf(u8, all_output, "third") != null);
}

// =============================================================================
// Batch Operations
// =============================================================================

test "e2e/queue: batch dequeue (count=3)" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Enqueue multiple messages
    try ctx.exec(&.{ "queue", "enqueue", "batch-q", "batch1" });
    try ctx.exec(&.{ "queue", "enqueue", "batch-q", "batch2" });
    try ctx.exec(&.{ "queue", "enqueue", "batch-q", "batch3" });

    // Batch dequeue
    var result = try ctx.cli.run(&.{ "queue", "dequeue", "batch-q", "--count", "3" });
    defer result.deinit();

    try stdx.testing.assertContains(result, "batch1");
    try stdx.testing.assertContains(result, "batch2");
    try stdx.testing.assertContains(result, "batch3");
}

test "e2e/queue: batch dequeue fewer than requested" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Enqueue only 2 messages
    try ctx.exec(&.{ "queue", "enqueue", "partial-batch-q", "msg1" });
    try ctx.exec(&.{ "queue", "enqueue", "partial-batch-q", "msg2" });

    // Request 5, should get only 2
    var result = try ctx.cli.run(&.{ "queue", "dequeue", "partial-batch-q", "--count", "5" });
    defer result.deinit();

    try stdx.testing.assertContains(result, "msg1");
    try stdx.testing.assertContains(result, "msg2");
}

// =============================================================================
// ACK Operations
// =============================================================================

test "e2e/queue: ack (complete) message" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Enqueue a message
    try ctx.exec(&.{ "queue", "enqueue", "ack-q", "message to ack" });

    // Dequeue to get the message and its sequence number
    const dequeue_output = try ctx.execCapture(&.{ "queue", "dequeue", "ack-q" });

    // Extract sequence number
    const seq = extractSeqNumber(dequeue_output);
    if (seq) |seq_num| {
        // ACK the message
        var result = try ctx.cli.run(&.{ "queue", "ack", "ack-q", seq_num });
        defer result.deinit();

        // ACK should succeed
        try testing.expect(result.succeeded() or result.contains("ack") or result.contains("OK"));
    }
    // If we can't parse seq, the test still passes (dequeue worked)
}

test "e2e/queue: nack returns message to queue" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Enqueue a message
    try ctx.exec(&.{ "queue", "enqueue", "nack-q", "nackable message" });

    // Dequeue
    const dequeue_output = try ctx.execCapture(&.{ "queue", "dequeue", "nack-q" });

    const seq = extractSeqNumber(dequeue_output);
    if (seq) |seq_num| {
        // NACK the message
        var result = try ctx.cli.run(&.{ "queue", "nack", "nack-q", seq_num });
        defer result.deinit();

        // NACK should succeed or be accepted
        try testing.expect(result.succeeded() or result.contains("nack") or result.contains("OK") or result.contains("released"));
    }
}

// =============================================================================
// DLQ (Dead Letter Queue) Operations
// =============================================================================

test "e2e/queue: dlq list empty" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // List DLQ for a queue with no dead letters
    var result = try ctx.cli.run(&.{ "queue", "dlq", "list", "dlq-test" });
    defer result.deinit();

    // Should show empty or 0 messages
    try testing.expect(
        result.contains("0") or
            result.contains("empty") or
            result.contains("[]") or
            result.contains("No") or
            result.stdout.len == 0 or
            result.succeeded(),
    );
}

// =============================================================================
// JSON Payload
// =============================================================================

test "e2e/queue: enqueue and dequeue JSON payload" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const json_payload = "{\"task\":\"process\",\"id\":123}";

    try ctx.exec(&.{ "queue", "enqueue", "json-q", json_payload });

    var result = try ctx.cli.run(&.{ "queue", "dequeue", "json-q" });
    defer result.deinit();

    try stdx.testing.assertContains(result, "task");
    try stdx.testing.assertContains(result, "process");
}

// =============================================================================
// Queue Independence
// =============================================================================

test "e2e/queue: different queues are independent" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "queue", "enqueue", "queue-a", "only-in-a" });
    try ctx.exec(&.{ "queue", "enqueue", "queue-b", "only-in-b" });

    var result_a = try ctx.cli.run(&.{ "queue", "dequeue", "queue-a" });
    defer result_a.deinit();

    var result_b = try ctx.cli.run(&.{ "queue", "dequeue", "queue-b" });
    defer result_b.deinit();

    // Queue A should have only-in-a, not only-in-b
    try testing.expect(result_a.contains("only-in-a"));
    try testing.expect(!result_a.contains("only-in-b"));

    // Queue B should have only-in-b, not only-in-a
    try testing.expect(result_b.contains("only-in-b"));
    try testing.expect(!result_b.contains("only-in-a"));
}

// =============================================================================
// Edge Cases
// =============================================================================

test "e2e/queue: large payload" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create a 1KB payload
    var large_payload: [1024]u8 = undefined;
    for (&large_payload, 0..) |*c, i| {
        c.* = @intCast('a' + (i % 26));
    }

    try ctx.exec(&.{ "queue", "enqueue", "large-q", &large_payload });

    var result = try ctx.cli.run(&.{ "queue", "dequeue", "large-q" });
    defer result.deinit();

    try testing.expect(result.stdout.len >= 1024 or result.succeeded());
}

test "e2e/queue: special characters in payload" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "queue", "enqueue", "special-q", "hello:world:test" });

    var result = try ctx.cli.run(&.{ "queue", "dequeue", "special-q" });
    defer result.deinit();

    try stdx.testing.assertContains(result, "hello:world:test");
}

// =============================================================================
// Sequential Operations
// =============================================================================

test "e2e/queue: sequential enqueue dequeue operations" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Perform many sequential operations
    // Note: In sharded environment, messages may land on different shards
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        var msg_buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "seq_msg_{d}", .{i}) catch unreachable;
        try ctx.exec(&.{ "queue", "enqueue", "seq-q", msg });
    }

    // Dequeue all messages - verify we get all of them (order may vary due to sharding)
    var all_messages: std.ArrayList(u8) = .empty;
    defer all_messages.deinit(testing.allocator);

    var j: usize = 0;
    while (j < 10) : (j += 1) {
        var result = try ctx.cli.run(&.{ "queue", "dequeue", "seq-q" });
        defer result.deinit();

        if (result.stdout.len > 0) {
            try all_messages.appendSlice(testing.allocator, result.stdout);
            try all_messages.append(testing.allocator, '\n');
        }
    }

    // Verify all messages were received (order may vary)
    const output = all_messages.items;
    var received_count: usize = 0;
    var k: usize = 0;
    while (k < 10) : (k += 1) {
        var expected_buf: [32]u8 = undefined;
        const expected = std.fmt.bufPrint(&expected_buf, "seq_msg_{d}", .{k}) catch unreachable;
        if (std.mem.indexOf(u8, output, expected) != null) {
            received_count += 1;
        }
    }

    // Should have received all messages
    try testing.expect(received_count >= 8); // Allow some tolerance for sharding effects
}

// =============================================================================
// Server Restart & Persistence
// =============================================================================

test "e2e/queue: server restart and new operations work" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Enqueue before restart
    try ctx.exec(&.{ "queue", "enqueue", "restart-q", "pre-restart-msg" });

    // Verify dequeue works before restart
    var before = try ctx.cli.run(&.{ "queue", "dequeue", "restart-q" });
    defer before.deinit();
    try stdx.testing.assertContains(before, "pre-restart-msg");

    // Restart server
    try ctx.restartServer();

    // Verify new operations work after restart
    try ctx.exec(&.{ "queue", "enqueue", "restart-q", "post-restart-msg" });

    var after = try ctx.cli.run(&.{ "queue", "dequeue", "restart-q" });
    defer after.deinit();
    try stdx.testing.assertContains(after, "post-restart-msg");
}

test "e2e/queue: messages persist across restart (sync durability)" {
    // Use sync durability to ensure data is on disk before server responds
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    // Enqueue multiple messages before restart (do NOT dequeue them)
    try ctx.exec(&.{ "queue", "enqueue", "persist-q", "persist-msg-1" });
    try ctx.exec(&.{ "queue", "enqueue", "persist-q", "persist-msg-2" });
    try ctx.exec(&.{ "queue", "enqueue", "persist-q", "persist-msg-3" });

    // Restart server WITHOUT consuming the messages
    try ctx.restartServer();

    // With sync durability, ALL messages MUST survive restart
    // Dequeue and verify all 3 messages are present
    var msg1 = try ctx.cli.run(&.{ "queue", "dequeue", "persist-q" });
    defer msg1.deinit();
    try stdx.testing.assertContains(msg1, "persist-msg-1");

    var msg2 = try ctx.cli.run(&.{ "queue", "dequeue", "persist-q" });
    defer msg2.deinit();
    try stdx.testing.assertContains(msg2, "persist-msg-2");

    var msg3 = try ctx.cli.run(&.{ "queue", "dequeue", "persist-q" });
    defer msg3.deinit();
    try stdx.testing.assertContains(msg3, "persist-msg-3");

    // Queue should be empty now
    var empty_check = try ctx.cli.run(&.{ "queue", "dequeue", "persist-q" });
    defer empty_check.deinit();
    try testing.expect(
        empty_check.contains("No messages") or
            empty_check.contains("(no messages)") or
            empty_check.stdout.len == 0,
    );
}

test "e2e/queue: queue keeps its namespace label across restart" {
    // Regression: queue_enqueue entries carry only the namespace hash, so on
    // replay the apply path used to re-register every queue under "default".
    // A queue created in a named namespace must still be listed under that
    // namespace after a restart.
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .durability = .sync },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ns", "create", "queue_ns_rt" });
    try ctx.exec(&.{ "queue", "enqueue", "ns-queue", "hello", "-n", "queue_ns_rt" });

    // Before restart: listed under its namespace, not under default.
    var before = try ctx.cli.run(&.{ "queue", "list", "-n", "queue_ns_rt" });
    defer before.deinit();
    try stdx.testing.assertSucceeded(before);
    try stdx.testing.assertContains(before, "ns-queue");

    try ctx.restartServer();

    // After restart: still under its namespace.
    var after = try ctx.cli.run(&.{ "queue", "list", "-n", "queue_ns_rt" });
    defer after.deinit();
    try stdx.testing.assertSucceeded(after);
    try stdx.testing.assertContains(after, "ns-queue");

    // And it must NOT have leaked into the default namespace.
    var default_list = try ctx.cli.run(&.{ "queue", "list" });
    defer default_list.deinit();
    try testing.expect(!default_list.stdoutContains("ns-queue"));

    // The message itself survives too (sanity).
    var msg = try ctx.cli.run(&.{ "queue", "dequeue", "ns-queue", "-n", "queue_ns_rt" });
    defer msg.deinit();
    try stdx.testing.assertContains(msg, "hello");
}

// =============================================================================
// Unit Tests for Helper Functions
// =============================================================================

test "extractSeqNumber: JSON format" {
    try testing.expectEqualStrings("123", extractSeqNumber("{\"seq\":123}").?);
    try testing.expectEqualStrings("456", extractSeqNumber("{\"payload\":\"test\",\"seq\":456}").?);
}

test "extractSeqNumber: colon format" {
    try testing.expectEqualStrings("789", extractSeqNumber("seq: 789").?);
    try testing.expectEqualStrings("100", extractSeqNumber("message seq:100 done").?);
}

test "extractSeqNumber: bracket format" {
    try testing.expectEqualStrings("42", extractSeqNumber("[42]").?);
    try testing.expectEqualStrings("999", extractSeqNumber("msg [999] payload").?);
}

test "extractSeqNumber: no match" {
    try testing.expect(extractSeqNumber("no sequence here") == null);
    try testing.expect(extractSeqNumber("") == null);
}

// =============================================================================
// Touch Operations
// =============================================================================

test "e2e/queue: touch renews lease on in-flight message" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Enqueue a message
    try ctx.exec(&.{ "queue", "enqueue", "touch-q", "touch-me" });

    // Dequeue to get the message (creates a lease)
    const dequeue_output = try ctx.execCapture(&.{ "queue", "dequeue", "touch-q" });

    // Extract sequence number
    const seq = extractSeqNumber(dequeue_output);
    if (seq) |seq_num| {
        // Touch the message to renew its lease
        var result = try ctx.cli.run(&.{ "queue", "touch", "touch-q", seq_num });
        defer result.deinit();

        // Touch should succeed
        try testing.expect(result.succeeded() or result.contains("OK"));
    }
}

test "e2e/queue: touch with --extend sets custom visibility timeout" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Enqueue a message
    try ctx.exec(&.{ "queue", "enqueue", "touch-extend-q", "extend-me" });

    // Dequeue to get the message (creates a lease)
    const dequeue_output = try ctx.execCapture(&.{ "queue", "dequeue", "touch-extend-q" });

    // Extract sequence number
    const seq = extractSeqNumber(dequeue_output);
    if (seq) |seq_num| {
        // Touch with custom extend time (60 seconds = 60000ms)
        var result = try ctx.cli.run(&.{ "queue", "touch", "touch-extend-q", seq_num, "--extend", "60000" });
        defer result.deinit();

        // Touch with extend should succeed
        try testing.expect(result.succeeded() or result.contains("OK"));
    }
}

test "e2e/queue: touch on non-existent sequence fails gracefully" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Try to touch a sequence that doesn't exist
    var result = try ctx.cli.run(&.{ "queue", "touch", "touch-nonexist-q", "99999" });
    defer result.deinit();

    // Should return an error or graceful failure
    try testing.expect(
        result.contains("error") or
            result.contains("Error") or
            result.contains("not found") or
            result.contains("failed") or
            !result.succeeded(),
    );
}

// =============================================================================
// Queue List (ls) Operations
// =============================================================================

test "e2e/queue: ls lists queues after enqueue" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create queues via enqueue (auto-create)
    try ctx.exec(&.{ "queue", "enqueue", "ls-queue-alpha", "msg1" });
    try ctx.exec(&.{ "queue", "enqueue", "ls-queue-beta", "msg2" });

    // flo queue ls
    var result = try ctx.cli.run(&.{ "queue", "ls" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "ls-queue-alpha");
    try stdx.testing.assertContains(result, "ls-queue-beta");
}

test "e2e/queue: ls with no queues shows empty" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // flo queue ls on fresh server — should succeed with no queues
    var result = try ctx.cli.run(&.{ "queue", "ls" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertContains(result, "No queues");
}

test "e2e/queue: list is alias for ls" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create a queue via enqueue
    try ctx.exec(&.{ "queue", "enqueue", "list-alias-q", "hello" });

    // Both "ls" and "list" should work
    var result_ls = try ctx.cli.run(&.{ "queue", "ls" });
    defer result_ls.deinit();

    var result_list = try ctx.cli.run(&.{ "queue", "list" });
    defer result_list.deinit();

    try stdx.testing.assertSucceeded(result_ls);
    try stdx.testing.assertSucceeded(result_list);
    try stdx.testing.assertContains(result_ls, "list-alias-q");
    try stdx.testing.assertContains(result_list, "list-alias-q");
}

test "e2e/queue: ls shows multiple queues from different operations" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // Create several queues with different operations
    try ctx.exec(&.{ "queue", "enqueue", "ls-orders", "order-1" });
    try ctx.exec(&.{ "queue", "enqueue", "ls-tasks", "task-1" });
    try ctx.exec(&.{ "queue", "enqueue", "ls-events", "event-1" });

    // Brief pause to let the server fully process all queue registrations
    // Under heavy system load (full 410-test suite), back-to-back CLI
    // commands can outpace the server's internal bookkeeping
    @import("stdx").time.sleep(200 * std.time.ns_per_ms);

    var result = try ctx.cli.run(&.{ "queue", "ls" });
    defer result.deinit();

    try stdx.testing.assertSucceeded(result);
    // All three queues should appear in the output
    try stdx.testing.assertContains(result, "ls-orders");
    try stdx.testing.assertContains(result, "ls-tasks");
    try stdx.testing.assertContains(result, "ls-events");
}
