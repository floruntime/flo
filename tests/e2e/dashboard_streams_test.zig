//! Dashboard Streams API E2E Tests (issue #22)
//!
//! Drives a real flo server with the dashboard enabled and exercises the three
//! gaps closed for #22:
//!   1. stream mutations (trim / delete-stream / delete-consumer-group) via the
//!      dashboard now hit real loopback write paths.
//!   2. the streams list surfaces REAL retention (from persisted config) instead
//!      of the fabricated "7d".
//!   3. consumer-group sub-endpoints honor ?namespace= instead of hardcoding
//!      "default".

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

// ── Part 2: real list fields ────────────────────────────────────────────────

test "e2e/dashboard: streams list surfaces real retention, not fabricated 7d" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // 24h retention → persisted as 86400s → rendered "1d".
    try ctx.exec(&.{ "stream", "create", "dash-ret", "--retention", "24" });
    try ctx.exec(&.{ "stream", "append", "dash-ret", "hello" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/streams");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);

    try testing.expect(std.mem.indexOf(u8, resp.body, "dash-ret") != null);
    // Real retention from config, and the old hardcoded "7d" is gone.
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"retention\":\"1d\"") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"7d\"") == null);
    // Real-field plumbing present (numbers, not absent).
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"ingest_rate\":") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"reads\":") != null);
}

// ── Part 1: mutations ───────────────────────────────────────────────────────

test "e2e/dashboard: POST stream trim hits the real write path" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    for (0..5) |i| {
        var buf: [16]u8 = undefined;
        try ctx.exec(&.{ "stream", "append", "dash-trim", std.fmt.bufPrint(&buf, "m{d}", .{i}) catch unreachable });
    }

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.post("/api/v1/streams/dash-trim/trim?max_len=2", "");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"ok\":true") != null);
}

test "e2e/dashboard: trim without a bound is rejected" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "dash-trim-bad", "x" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.post("/api/v1/streams/dash-trim-bad/trim", "");
    defer resp.deinit();
    // Validation error surfaces as a JSON error, not a silent no-op.
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"error\"") != null);
}

test "e2e/dashboard: DELETE stream removes it from the list" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "dash-del", "x" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var del = try http.delete("/api/v1/streams/dash-del?force=true");
    defer del.deinit();
    try testing.expectEqual(@as(u16, 200), del.status);
    try testing.expect(std.mem.indexOf(u8, del.body, "\"ok\":true") != null);

    var list = try http.get("/api/v1/streams");
    defer list.deinit();
    try testing.expect(std.mem.indexOf(u8, list.body, "dash-del") == null);
}

test "e2e/dashboard: DELETE consumer group succeeds" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "dash-grp-del", "x" });
    try ctx.exec(&.{ "stream", "group", "create", "dash-grp-del", "--group", "g1" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.delete("/api/v1/streams/dash-grp-del/groups/g1");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"ok\":true") != null);
}

// ── Part 3: namespace threading ─────────────────────────────────────────────

test "e2e/dashboard: consumer-group endpoints honor ?namespace=" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Stream + group + a registered consumer, all in a NON-default namespace.
    try ctx.exec(&.{ "stream", "append", "ns-stream", "m0", "-n", "strns" });
    try ctx.exec(&.{ "stream", "group", "create", "ns-stream", "--group", "ns-grp", "-n", "strns" });
    // group read registers the consumer as a member of the group.
    _ = try ctx.execCapture(&.{ "stream", "group", "read", "ns-stream", "--group", "ns-grp", "--consumer", "nsworker", "--limit", "1", "-n", "strns" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // With the correct namespace, the group resolves and the member is listed.
    var ok_resp = try http.get("/api/v1/streams/ns-stream/groups/ns-grp?namespace=strns");
    defer ok_resp.deinit();
    try testing.expectEqual(@as(u16, 200), ok_resp.status);
    try testing.expect(std.mem.indexOf(u8, ok_resp.body, "\"namespace\":\"strns\"") != null);
    try testing.expect(std.mem.indexOf(u8, ok_resp.body, "nsworker") != null);

    // With the wrong (default) namespace, the group does NOT resolve — proving
    // the lookup is namespace-scoped rather than hardcoded to "default".
    var miss = try http.get("/api/v1/streams/ns-stream/groups/ns-grp?namespace=default");
    defer miss.deinit();
    try testing.expect(std.mem.indexOf(u8, miss.body, "nsworker") == null);
}
