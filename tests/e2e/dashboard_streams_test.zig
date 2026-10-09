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

    // A dry run reports the count and removes nothing.
    var dry = try http.post("/api/v1/streams/dash-trim/trim?max_len=2&dry_run=true", "");
    defer dry.deinit();
    try testing.expectEqual(@as(u16, 200), dry.status);
    try testing.expect(std.mem.indexOf(u8, dry.body, "\"dry_run\":true,\"trimmed\":3") != null);
    var r1 = try ctx.cli.run(&.{ "stream", "read", "dash-trim", "--limit", "10", "-o", "json" });
    defer r1.deinit();
    try testing.expectEqual(@as(usize, 5), r1.stdoutCount("\"data\":\"m"));

    var resp = try http.post("/api/v1/streams/dash-trim/trim?max_len=2", "");
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"dry_run\":false,\"trimmed\":3") != null);
    var r2 = try ctx.cli.run(&.{ "stream", "read", "dash-trim", "--limit", "10", "-o", "json" });
    defer r2.deinit();
    try testing.expectEqual(@as(usize, 2), r2.stdoutCount("\"data\":\"m"));
}

test "e2e/dashboard: trim refuses parameters it doesn't take" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();
    try ctx.exec(&.{ "stream", "append", "dash-trim-params", "x" });
    try ctx.exec(&.{ "stream", "append", "dash-trim-params", "y" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var b = try http.post("/api/v1/streams/dash-trim-params/trim?max_len=1&max_bytes=10", "");
    defer b.deinit();
    try testing.expect(std.mem.indexOf(u8, b.body, "max_bytes") != null);
    try testing.expect(std.mem.indexOf(u8, b.body, "\"error\"") != null);

    var n = try http.post("/api/v1/streams/dash-trim-params/trim?max_len=abc", "");
    defer n.deinit();
    try testing.expect(std.mem.indexOf(u8, n.body, "max_len must be a whole number") != null);

    var both = try http.post("/api/v1/streams/dash-trim-params/trim?max_len=1&max_age_s=60", "");
    defer both.deinit();
    try testing.expect(std.mem.indexOf(u8, both.body, "only one of") != null);

    var zero = try http.post("/api/v1/streams/dash-trim-params/trim?max_len=0", "");
    defer zero.deinit();
    try testing.expect(std.mem.indexOf(u8, zero.body, "max_len must be > 0") != null);

    // A dry_run that isn't true/false/1/0 is refused, never read as a real trim.
    for ([_][]const u8{ "dry_run=yes", "dry_run=True", "dry_run" }) |d| {
        var path: [96]u8 = undefined;
        var r = try http.post(std.fmt.bufPrint(&path, "/api/v1/streams/dash-trim-params/trim?max_len=1&{s}", .{d}) catch unreachable, "");
        defer r.deinit();
        try testing.expect(std.mem.indexOf(u8, r.body, "\"error\"") != null);
        try testing.expect(std.mem.indexOf(u8, r.body, "dry_run") != null);
    }

    // Repeated and empty parameters are refused by name.
    var rep = try http.post("/api/v1/streams/dash-trim-params/trim?max_len=1&max_len=abc", "");
    defer rep.deinit();
    try testing.expect(std.mem.indexOf(u8, rep.body, "'max_len' is given more than once") != null);
    var empty = try http.post("/api/v1/streams/dash-trim-params/trim?max_len=2&max_age_s=", "");
    defer empty.deinit();
    try testing.expect(std.mem.indexOf(u8, empty.body, "'max_age_s' needs a value") != null);

    var r = try ctx.cli.run(&.{ "stream", "read", "dash-trim-params", "--limit", "10", "-o", "json" });
    defer r.deinit();
    try testing.expectEqual(@as(usize, 2), r.stdoutCount("\"data\":"));
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

test "e2e/dashboard: stream messages page through batches over 100 by the server's cursor" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Two 150-record appends.
    for (0..2) |b| {
        var bufs: [150][16]u8 = undefined;
        var args: [3 + 150][]const u8 = undefined;
        args[0] = "stream";
        args[1] = "append";
        args[2] = "dash-big";
        for (0..150) |i| args[3 + i] = std.fmt.bufPrint(&bufs[i], "d{d}-{d:0>3}", .{ b, i }) catch unreachable;
        try ctx.exec(&args);
    }

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // limit=200 takes one whole 150-record append per page.
    var seen: std.ArrayList(u8) = .empty;
    defer seen.deinit(testing.allocator);
    var path_buf: [160]u8 = undefined;
    var cursor_buf: [48]u8 = undefined;
    var cursor: ?[]const u8 = null;
    var pages: usize = 0;
    while (pages < 5) : (pages += 1) {
        const path = if (cursor) |c|
            std.fmt.bufPrint(&path_buf, "/api/v1/streams/dash-big/messages?limit=200&cursor={s}", .{c}) catch unreachable
        else
            "/api/v1/streams/dash-big/messages?limit=200";
        var resp = try http.get(path);
        defer resp.deinit();
        try testing.expectEqual(@as(u16, 200), resp.status);
        const n = std.mem.count(u8, resp.body, "\"payload\":\"d");
        if (n == 0) break;
        try testing.expectEqual(@as(usize, 150), n);
        try seen.appendSlice(testing.allocator, resp.body);
        const tag = "\"next_cursor\":\"";
        const at = std.mem.indexOf(u8, resp.body, tag) orelse return error.NoCursor;
        const start = at + tag.len;
        const c = resp.body[start..std.mem.indexOfScalarPos(u8, resp.body, start, '"').?];
        @memcpy(cursor_buf[0..c.len], c);
        cursor = cursor_buf[0..c.len];
    }
    for (0..2) |b| for (0..150) |i| {
        var nb: [24]u8 = undefined;
        try testing.expectEqual(@as(usize, 1), std.mem.count(u8, seen.items, std.fmt.bufPrint(&nb, "\"d{d}-{d:0>3}\"", .{ b, i }) catch unreachable));
    };
}
