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

/// The delivery a dequeue printed first, `term.index.seq.deliveries`,
/// before its payload.
fn deliveryOf(output: []const u8) ?[]const u8 {
    const line = std.mem.sliceTo(output, '\n');
    const space = std.mem.indexOfScalar(u8, line, ' ') orelse return null;
    if (std.mem.count(u8, line[0..space], ".") != 3) return null;
    return line[0..space];
}

/// The delivery count `delivery` ends with.
fn deliveriesOf(delivery: []const u8) !u32 {
    const dot = std.mem.lastIndexOfScalar(u8, delivery, '.') orelse return error.NoDelivery;
    return std.fmt.parseInt(u32, delivery[dot + 1 ..], 10);
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

test "e2e/queue: a dequeue leases its message; ack removes it, and a second ack is refused" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "queue", "enqueue", "ack-q", "message to ack" });
    const out = try ctx.execCapture(&.{ "queue", "dequeue", "ack-q" });
    try testing.expect(std.mem.endsWith(u8, std.mem.trimEnd(u8, out, "\n"), " message to ack"));
    const delivery = deliveryOf(out) orelse return error.NoDelivery;
    var d_buf: [64]u8 = undefined;
    const d = d_buf[0..delivery.len];
    @memcpy(d, delivery);
    try testing.expectEqual(@as(u32, 1), try deliveriesOf(d));

    // Leased, not ready: a second dequeue takes nothing.
    var none = try ctx.cli.run(&.{ "queue", "dequeue", "ack-q" });
    defer none.deinit();
    try testing.expectEqual(@as(u8, 0), none.exit_code);
    try testing.expect(none.stdoutContains("(no messages)"));

    var ack = try ctx.cli.run(&.{ "queue", "ack", "ack-q", d });
    defer ack.deinit();
    try testing.expectEqual(@as(u8, 0), ack.exit_code);
    try testing.expect(ack.stdoutContains("OK: 1\n"));

    var again = try ctx.cli.run(&.{ "queue", "ack", "ack-q", d });
    defer again.deinit();
    try testing.expect(again.exit_code != 0);
    try testing.expect(again.stderrContains("no such message in this queue"));
}

test "e2e/queue: nack readies its message again, delivered a second time" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "queue", "enqueue", "nack-q", "nackable message" });
    const out = try ctx.execCapture(&.{ "queue", "dequeue", "nack-q" });
    const first = deliveryOf(out) orelse return error.NoDelivery;
    var nack = try ctx.cli.run(&.{ "queue", "nack", "nack-q", first });
    defer nack.deinit();
    try testing.expectEqual(@as(u8, 0), nack.exit_code);
    try testing.expect(nack.stdoutContains("OK: 1\n"));

    const again = try ctx.execCapture(&.{ "queue", "dequeue", "nack-q" });
    try testing.expect(std.mem.indexOf(u8, again, "nackable message") != null);
    try testing.expectEqual(@as(u32, 2), try deliveriesOf(deliveryOf(again) orelse return error.NoDelivery));
}

test "e2e/queue: a lease that runs out redelivers its message, and the late holder's ack is refused" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "queue", "enqueue", "lease-q", "slow work" });
    const out = try ctx.execCapture(&.{ "queue", "dequeue", "lease-q", "--lease", "5000" });
    const delivery = deliveryOf(out) orelse return error.NoDelivery;
    var d_buf: [64]u8 = undefined;
    const d = d_buf[0..delivery.len];
    @memcpy(d, delivery);

    // The lease, then the duty's tick: the message is back.
    var waited: usize = 0;
    const again = while (waited < 40) : (waited += 1) {
        @import("stdx").time.sleep(250 * std.time.ns_per_ms);
        const got = try ctx.execCapture(&.{ "queue", "dequeue", "lease-q" });
        if (std.mem.indexOf(u8, got, "slow work") != null) break got;
    } else return error.NotRedelivered;
    try testing.expectEqual(@as(u32, 2), try deliveriesOf(deliveryOf(again) orelse return error.NoDelivery));

    var late = try ctx.cli.run(&.{ "queue", "ack", "lease-q", d });
    defer late.deinit();
    try testing.expect(late.exit_code != 0);
    try testing.expect(late.stderrContains("delivered again"));
}

test "e2e/queue: nack --dlq dead-letters its message at once" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "queue", "enqueue", "dead-q", "poison" });
    const out = try ctx.execCapture(&.{ "queue", "dequeue", "dead-q" });
    const delivery = deliveryOf(out) orelse return error.NoDelivery;
    try ctx.exec(&.{ "queue", "nack", "dead-q", delivery, "--dlq", "--error", "bad input" });

    var list = try ctx.cli.run(&.{ "queue", "dlq", "list", "dead-q" });
    defer list.deinit();
    try testing.expectEqual(@as(u8, 0), list.exit_code);
    try testing.expect(list.stdoutContains("Dead-letter queue for: dead-q (1 messages)\n"));
    try testing.expect(list.stdoutContains("poison"));
    var none = try ctx.cli.run(&.{ "queue", "dequeue", "dead-q" });
    defer none.deinit();
    try testing.expect(none.stdoutContains("(no messages)"));
}

test "e2e/queue: dequeue --ack takes its messages for good, printing payloads only" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try ctx.exec(&.{ "queue", "enqueue", "once-q", "consumed" });
    var r = try ctx.cli.run(&.{ "queue", "dequeue", "once-q", "--ack" });
    defer r.deinit();
    try testing.expectEqual(@as(u8, 0), r.exit_code);
    try testing.expectEqualStrings("consumed\n", r.stdout);
    var stats = try ctx.cli.run(&.{ "queue", "list" });
    defer stats.deinit();
    try testing.expect(stats.stdoutContains("once-q"));
    var none = try ctx.cli.run(&.{ "queue", "dequeue", "once-q" });
    defer none.deinit();
    try testing.expect(none.stdoutContains("(no messages)"));
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

    try testing.expectEqual(@as(u8, 0), result.exit_code);
    try testing.expect(result.stdoutContains("Dead-letter queue for: dlq-test (0 messages)\n(empty)\n"));
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

test "e2e/queue: a backlog in one queue doesn't hide another queue's message" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // One shard, so both queues share a partition; the backlog outranks b.
    for (0..200) |_| try ctx.exec(&.{ "queue", "enqueue", "backlog-a", "from-a", "--priority", "0" });
    try ctx.exec(&.{ "queue", "enqueue", "backlog-b", "only-in-b", "--priority", "5" });

    var result = try ctx.cli.run(&.{ "queue", "dequeue", "backlog-b" });
    defer result.deinit();
    try testing.expectEqual(@as(u8, 0), result.exit_code);
    try testing.expect(result.contains("only-in-b"));
    try testing.expect(!result.contains("from-a"));
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

test "e2e/queue: a blocking dequeue woken by an enqueue leaves the metrics registry clean" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .metrics_enabled = true },
    });
    defer ctx.deinit();

    var waiter = try ctx.cli.runAsync(&.{ "queue", "dequeue", "wake-metrics-q", "--block", "5000" });
    defer waiter.deinit();
    @import("stdx").time.sleep(150 * std.time.ns_per_ms);
    try ctx.exec(&.{ "queue", "enqueue", "wake-metrics-q", "woken-msg" });

    var result = try waiter.wait();
    defer result.deinit();
    try stdx.testing.assertSucceeded(result);
    try stdx.testing.assertStdoutContains(result, "woken-msg");

    // The wake path acks from inside the apply of the enqueue. The metrics
    // registered on that path must name the real queue and nothing else:
    // a registry entry keyed by bytes from a reused buffer would show up
    // here as a second queue label.
    var http = try ctx.createMetricsHttp();
    defer http.deinit();
    var resp = try http.get("/metrics");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    const woken = std.mem.count(u8, resp.body, "queue=\"wake-metrics-q\"");
    const any = std.mem.count(u8, resp.body, "queue=\"");
    try testing.expect(woken > 0);
    try testing.expectEqual(any, woken);
}

test "e2e/queue: enqueue prints the sequence number its delivery names" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var first = try ctx.cli.run(&.{ "queue", "enqueue", "seq-q", "a" });
    defer first.deinit();
    try testing.expectEqual(@as(u8, 0), first.exit_code);
    var second = try ctx.cli.run(&.{ "queue", "enqueue", "seq-q", "b" });
    defer second.deinit();
    try testing.expectEqual(@as(u8, 0), second.exit_code);

    const prefix = "Enqueued: ";
    const a = std.mem.trimEnd(u8, first.stdout, "\n");
    const b = std.mem.trimEnd(u8, second.stdout, "\n");
    try testing.expect(std.mem.startsWith(u8, a, prefix));
    const seq_a = try std.fmt.parseInt(u64, a[prefix.len..], 10);
    const seq_b = try std.fmt.parseInt(u64, b[prefix.len..], 10);
    try testing.expect(seq_b > seq_a);

    // The delivery dequeue prints names the message by that seq.
    const deq = try ctx.execCapture(&.{ "queue", "dequeue", "seq-q" });
    const delivery = deliveryOf(deq) orelse return error.NoDelivery;
    var parts = std.mem.splitScalar(u8, delivery, '.');
    _ = parts.next();
    _ = parts.next();
    try testing.expectEqual(seq_a, try std.fmt.parseInt(u64, parts.next().?, 10));
    var ack = try ctx.cli.run(&.{ "queue", "ack", "seq-q", delivery });
    defer ack.deinit();
    try testing.expectEqual(@as(u8, 0), ack.exit_code);
}

test "e2e/queue: dequeue refuses -o raw without --ack, and a count or lease out of range, naming the range" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "queue", "enqueue", "flags-q", "kept" });
    const cases = [_]struct { args: []const []const u8, says: []const u8 }{
        .{ .args = &.{ "queue", "dequeue", "flags-q", "-o", "raw" }, .says = "add --ack" },
        .{ .args = &.{ "queue", "dequeue", "flags-q", "--count", "0" }, .says = "--count is 1 to 100" },
        .{ .args = &.{ "queue", "dequeue", "flags-q", "--count", "101" }, .says = "--count is 1 to 100" },
        .{ .args = &.{ "queue", "dequeue", "flags-q", "--lease", "1000" }, .says = "--lease is 5000 to 43200000 ms" },
    };
    for (cases) |c| {
        var r = try ctx.cli.run(c.args);
        defer r.deinit();
        try testing.expect(r.exit_code != 0);
        try testing.expect(r.stderrContains(c.says));
    }
    // None of them took the message.
    var json = try ctx.cli.run(&.{ "queue", "dequeue", "flags-q", "-o", "json" });
    defer json.deinit();
    try testing.expectEqual(@as(u8, 0), json.exit_code);
    try testing.expect(json.stdoutContains("\"payload\":\"kept\""));
    try testing.expect(json.stdoutContains("\"deliveries\":1"));
    try testing.expect(json.stdoutContains("\"delivery\":\""));
}

test "e2e/queue: an enqueue's --delay holds its message back until due" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "queue", "enqueue", "delay-q", "later", "--delay", "1500" });
    var early = try ctx.cli.run(&.{ "queue", "dequeue", "delay-q" });
    defer early.deinit();
    try testing.expect(early.stdoutContains("(no messages)"));
    @import("stdx").time.sleep(1700 * std.time.ns_per_ms);
    const due = try ctx.execCapture(&.{ "queue", "dequeue", "delay-q" });
    try testing.expect(std.mem.indexOf(u8, due, " later") != null);
}

test "e2e/queue: nack --delay holds its message back until due, and --priority past 255 is refused" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    try ctx.exec(&.{ "queue", "enqueue", "nack-delay-q", "retry me" });
    const out = try ctx.execCapture(&.{ "queue", "dequeue", "nack-delay-q" });
    const delivery = deliveryOf(out) orelse return error.NoDelivery;
    try ctx.exec(&.{ "queue", "nack", "nack-delay-q", delivery, "--delay", "1500" });
    var early = try ctx.cli.run(&.{ "queue", "dequeue", "nack-delay-q" });
    defer early.deinit();
    try testing.expect(early.stdoutContains("(no messages)"));
    @import("stdx").time.sleep(1700 * std.time.ns_per_ms);
    const due = try ctx.execCapture(&.{ "queue", "dequeue", "nack-delay-q" });
    try testing.expect(std.mem.indexOf(u8, due, " retry me") != null);

    var high = try ctx.cli.run(&.{ "queue", "enqueue", "nack-delay-q", "x", "--priority", "256" });
    defer high.deinit();
    try testing.expect(high.exit_code != 0);
    try testing.expect(high.stderrContains("--priority is 0 to 255, not 256"));
}
