//! Dashboard Queue API E2E Tests (issue #23)
//!
//! Verifies the two queue write paths the dashboard was missing:
//!   - POST /queues/:name        — enqueue a message (loopback)
//!   - POST /queues/:name/purge  — purge live messages (new server op, was a
//!     no-op stub returning purged:0)

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "e2e/dashboard: queue purge removes live messages" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Seed three messages (CLI is the reliable producer here).
    try ctx.exec(&.{ "queue", "enqueue", "pq", "purge-a" });
    try ctx.exec(&.{ "queue", "enqueue", "pq", "purge-b" });
    try ctx.exec(&.{ "queue", "enqueue", "pq", "purge-c" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // Purge returns the REAL removed count (was a no-op stub → purged:0).
    var purge = try http.post("/api/v1/queues/pq/purge", "");
    defer purge.deinit();
    try testing.expectEqual(@as(u16, 200), purge.status);
    try testing.expect(std.mem.indexOf(u8, purge.body, "\"purged\":3") != null);

    // The queue is now empty — a dequeue finds none of the purged messages.
    var deq = try ctx.cli.run(&.{ "queue", "dequeue", "pq" });
    defer deq.deinit();
    try testing.expect(!deq.contains("purge-a") and !deq.contains("purge-b") and !deq.contains("purge-c"));
}

test "e2e/dashboard: queue enqueue produces a consumable message" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Warm the protocol port first — the dashboard enqueue loops back to it,
    // and the very first loopback on a cold port can race (same reason the
    // purge test above is reliable: its CLI seeding warms the port).
    try ctx.exec(&.{ "queue", "enqueue", "warmup", "x" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // Enqueue via the dashboard (was: only a CLI command shown in the modal).
    var e = try http.post("/api/v1/queues/eq", "hello-dash");
    defer e.deinit();
    try testing.expectEqual(@as(u16, 200), e.status);
    try testing.expect(std.mem.indexOf(u8, e.body, "\"ok\":true") != null);

    // It's a real message: the CLI dequeues exactly it.
    var deq = try ctx.cli.run(&.{ "queue", "dequeue", "eq" });
    defer deq.deinit();
    try testing.expect(deq.contains("hello-dash"));
}

test "e2e/dashboard: a queue's counts are exact after a dequeue, and after a restart" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    for ([_][]const u8{ "a", "b", "c" }) |m| try ctx.exec(&.{ "queue", "enqueue", "rq", m });
    var deq = try ctx.cli.run(&.{ "queue", "dequeue", "rq", "--ack" });
    defer deq.deinit();
    try stdx.testing.assertSucceeded(deq);

    try expectExactCounts(ctx);
    // A restart rebuilds the queue from the log alone: the same counts.
    try ctx.restartServer();
    try expectExactCounts(ctx);
}

/// rq's counts once its three enqueues and one dequeue have applied.
fn expectExactCounts(ctx: *stdx.testing.TestContext) !void {
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    // A dequeue with --ack completes its message as it takes it: its lease
    // ends when that complete applies, so it is in flight until then.
    const want = "\"name\":\"rq\",\"namespace\":\"default\",\"ready\":2,\"inflight\":0,\"pending\":2,\"available\":2,\"enqueued\":3,\"dequeued\":1,";
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        var resp = try http.get("/api/v1/queues?namespace=default");
        defer resp.deinit();
        try testing.expectEqual(@as(u16, 200), resp.status);
        if (std.mem.indexOf(u8, resp.body, want) != null) break;
        if (attempt == 20) {
            std.debug.print("want {s}\nin {s}\n", .{ want, resp.body });
            return error.CountsNotExact;
        }
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    // And the CLI's list says the same.
    var list = try ctx.cli.run(&.{ "queue", "list", "-o", "json" });
    defer list.deinit();
    try testing.expect(list.stdoutContains("\"ready\":2,\"leased\":0,\"enqueued\":3,\"dequeued\":1,"));
}

test "e2e/dashboard: a queue's messages are the next a dequeue takes, and a page stops where one answer ends" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    for ([_][]const u8{ "first", "second", "third" }) |m| try ctx.exec(&.{ "queue", "enqueue", "pk", m });
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    {
        var resp = try http.get("/api/v1/queues/pk/messages?namespace=default&limit=2");
        defer resp.deinit();
        try testing.expectEqual(@as(u16, 200), resp.status);
        try testing.expect(std.mem.indexOf(u8, resp.body, "\"payload\":\"first\"") != null);
        try testing.expect(std.mem.indexOf(u8, resp.body, "\"payload\":\"second\"") != null);
        try testing.expect(std.mem.indexOf(u8, resp.body, "\"payload\":\"third\"") == null);
        try testing.expect(std.mem.indexOf(u8, resp.body, "\"more\":true") != null);
    }
    // A peek takes nothing: the dequeue takes the first message peeked.
    var deq = try ctx.cli.run(&.{ "queue", "dequeue", "pk" });
    defer deq.deinit();
    try stdx.testing.assertSucceeded(deq);
    try testing.expect(deq.stdoutContains("first"));
    {
        var resp = try http.get("/api/v1/queues/pk/messages?namespace=default");
        defer resp.deinit();
        try testing.expect(std.mem.indexOf(u8, resp.body, "\"payload\":\"first\"") == null);
        try testing.expect(std.mem.indexOf(u8, resp.body, "\"more\":false") != null);
    }

    // A hundred messages of 6 KB: each shows 4 KiB, and fewer than a
    // hundred fit one answer, so the page says more follow.
    const big = try testing.allocator.alloc(u8, 6000);
    defer testing.allocator.free(big);
    @memset(big, 'b');
    for (0..100) |_| try ctx.exec(&.{ "queue", "enqueue", "big", big });
    var resp = try http.get("/api/v1/queues/big/messages?namespace=default&limit=100");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    var n: usize = 0;
    var at: usize = 0;
    while (std.mem.indexOfPos(u8, resp.body, at, "\"size\":6000,")) |i| : (at = i + 1) n += 1;
    try testing.expect(n > 0 and n < 100);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"truncated\":true") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"more\":true") != null);
}
