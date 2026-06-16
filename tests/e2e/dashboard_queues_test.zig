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
