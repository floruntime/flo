//! The dashboard serves the hosts it is told about and takes changes only
//! from its own pages or allowed origins. The change tests drive a queue
//! purge and check the queue after; the rest check what comes back.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

fn seeded(ctx: *stdx.testing.TestContext, queue: []const u8) !void {
    try ctx.exec(&.{ "queue", "enqueue", queue, "keep-a" });
    try ctx.exec(&.{ "queue", "enqueue", queue, "keep-b" });
}

/// Whether the queue still holds both messages `seeded` put in it: a purge
/// from the dashboard's own page, with a JSON body, removes exactly two.
fn untouched(http: anytype, port: u16, queue: []const u8) !bool {
    var path_buf: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/api/v1/queues/{s}/purge", .{queue});
    var origin_buf: [64]u8 = undefined;
    const origin = try std.fmt.bufPrint(&origin_buf, "http://127.0.0.1:{d}", .{port});
    var r = try http.requestExact(.POST, path, "", &.{ .{ "Origin", origin }, .{ "Content-Type", "application/json" } });
    defer r.deinit();
    return r.status == 200 and r.bodyContains("\"purged\":2");
}

test "e2e/dashboard: a change by GET is refused, so a link or an image on another site cannot make one" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    try seeded(ctx, "g");
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var purge = try http.get("/api/v1/queues/g/purge");
    defer purge.deinit();
    try testing.expectEqual(@as(u16, 405), purge.status);
    try testing.expect(try untouched(http, ctx.getDashboardPort(), "g"));

    var requeue = try http.get("/api/v1/queues/g/dlq/1/requeue");
    defer requeue.deinit();
    try testing.expectEqual(@as(u16, 405), requeue.status);
    var invoke = try http.get("/api/v1/actions/any/invoke");
    defer invoke.deinit();
    try testing.expectEqual(@as(u16, 405), invoke.status);
}

test "e2e/dashboard: a change from another site's page is refused" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    try seeded(ctx, "x");
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var cross = try http.requestExact(.POST, "/api/v1/queues/x/purge", "", &.{
        .{ "Origin", "http://evil.example" },
        .{ "Content-Type", "application/json" },
    });
    defer cross.deinit();
    try testing.expectEqual(@as(u16, 403), cross.status);
    // An origin of "null" (a sandboxed frame, a file) is no better.
    var opaque_origin = try http.requestExact(.POST, "/api/v1/queues/x/purge", "", &.{
        .{ "Origin", "null" },
        .{ "Content-Type", "application/json" },
    });
    defer opaque_origin.deinit();
    try testing.expectEqual(@as(u16, 403), opaque_origin.status);
    // Nor is no origin at all: a bare cross-site request may send none.
    var no_origin = try http.requestExact(.POST, "/api/v1/queues/x/purge", "", &.{.{ "Content-Type", "application/json" }});
    defer no_origin.deinit();
    try testing.expectEqual(@as(u16, 403), no_origin.status);
    try testing.expect(try untouched(http, ctx.getDashboardPort(), "x"));
    // A read from a site that is not allowed is answered without a CORS
    // grant, so its page cannot see the answer.
    var read = try http.requestExact(.GET, "/api/v1/queues", null, &.{.{ "Origin", "http://evil.example" }});
    defer read.deinit();
    try testing.expectEqual(@as(u16, 200), read.status);
    try testing.expect(read.getHeader("Access-Control-Allow-Origin") == null);
}

test "e2e/dashboard: a change whose content type any page could send is refused" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    try seeded(ctx, "t");
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    var origin_buf: [64]u8 = undefined;
    const origin = try std.fmt.bufPrint(&origin_buf, "http://127.0.0.1:{d}", .{ctx.getDashboardPort()});

    for ([_][]const u8{ "text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x" }) |ct| {
        var r = try http.requestExact(.POST, "/api/v1/queues/t/purge", "", &.{ .{ "Origin", origin }, .{ "Content-Type", ct } });
        defer r.deinit();
        try testing.expectEqual(@as(u16, 415), r.status);
    }
    var none = try http.requestExact(.POST, "/api/v1/queues/t/purge", "", &.{.{ "Origin", origin }});
    defer none.deinit();
    try testing.expectEqual(@as(u16, 415), none.status);
    // The dashboard's own page, by origin or by the browser's own word,
    // with a JSON body: done, and nothing before it had been.
    try testing.expect(try untouched(http, ctx.getDashboardPort(), "t"));
    try ctx.exec(&.{ "queue", "enqueue", "t", "again" });
    var same_site = try http.requestExact(.POST, "/api/v1/queues/t/purge", "", &.{ .{ "Sec-Fetch-Site", "same-origin" }, .{ "Content-Type", "application/json" } });
    defer same_site.deinit();
    try testing.expectEqual(@as(u16, 200), same_site.status);
    try testing.expect(same_site.bodyContains("\"purged\":1"));
}

test "e2e/dashboard: a host it does not answer to is not served, but health answers anyone" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // Asked by a name it answers to: served.
    var local = try http.getWithHeaders("/api/v1/queues", &.{});
    defer local.deinit();
    try testing.expectEqual(@as(u16, 200), local.status);

    // A Host it does not answer to.
    const port = ctx.getDashboardPort();
    const evil = try rawRequest(port, "GET /api/v1/queues HTTP/1.1\r\nHost: evil.example\r\n\r\n");
    defer testing.allocator.free(evil);
    try testing.expect(std.mem.startsWith(u8, evil, "HTTP/1.1 421"));
    const health = try rawRequest(port, "GET /health HTTP/1.1\r\nHost: 10.1.2.3:9002\r\n\r\n");
    defer testing.allocator.free(health);
    try testing.expect(std.mem.startsWith(u8, health, "HTTP/1.1 200"));
}

test "e2e/dashboard: clients that connect and send nothing do not hold up health or metrics" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true, .metrics_enabled = true } });
    defer ctx.deinit();
    for ([_]u16{ ctx.getDashboardPort(), ctx.getMetricsPort() }) |port| {
        var idle: [4]std.posix.socket_t = undefined;
        for (&idle) |*fd| fd.* = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 1000);
        defer for (idle) |fd| {
            _ = std.c.close(fd);
        };
        stdx.time.sleep(100 * std.time.ns_per_ms);
        const start = stdx.time.milliTimestamp();
        const answer = try rawRequest(port, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n");
        defer testing.allocator.free(answer);
        try testing.expect(std.mem.startsWith(u8, answer, "HTTP/1.1 200"));
        try testing.expect(stdx.time.milliTimestamp() - start < 2000);
    }
}

/// Send `req` as-is and return what comes back within three seconds.
fn rawRequest(port: u16, req: []const u8) ![]u8 {
    const fd = try stdx.net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, port, 1000);
    defer _ = std.c.close(fd);
    try stdx.net.sysFcntlSetNonblocking(fd);
    _ = std.c.write(fd, req.ptr, req.len);
    var buf: [4096]u8 = undefined;
    var got: usize = 0;
    const deadline = stdx.time.milliTimestamp() + 3000;
    while (stdx.time.milliTimestamp() < deadline and got < buf.len) {
        const rc = std.c.read(fd, buf[got..].ptr, buf.len - got);
        if (rc > 0) {
            got += @intCast(rc);
        } else if (rc == 0) {
            break;
        } else {
            stdx.time.sleep(10 * std.time.ns_per_ms);
        }
    }
    return testing.allocator.dupe(u8, buf[0..got]);
}

test "e2e/dashboard: a request with no headers is answered, on both listeners, and the node keeps serving" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true, .metrics_enabled = true } });
    defer ctx.deinit();
    const dash = try rawRequest(ctx.getDashboardPort(), "GET /api/v1/queues HTTP/1.0\r\n\r\n");
    defer testing.allocator.free(dash);
    try testing.expect(std.mem.startsWith(u8, dash, "HTTP/1.1 400"));
    const metrics = try rawRequest(ctx.getMetricsPort(), "GET /metrics HTTP/1.0\r\n\r\n");
    defer testing.allocator.free(metrics);
    try testing.expect(std.mem.startsWith(u8, metrics, "HTTP/1.1 200"));
    for ([_]u16{ ctx.getDashboardPort(), ctx.getMetricsPort() }) |port| {
        const health = try rawRequest(port, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n");
        defer testing.allocator.free(health);
        try testing.expect(std.mem.startsWith(u8, health, "HTTP/1.1 200"));
    }
    try ctx.exec(&.{ "kv", "set", "alive", "yes" });
}

test "e2e/dashboard: a body past the limit is refused whole, never stored cut short" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    const port = ctx.getDashboardPort();
    var buf: [256]u8 = undefined;
    const head = try std.fmt.bufPrint(&buf, "POST /api/v1/queues/big HTTP/1.1\r\nHost: 127.0.0.1:{d}\r\nOrigin: http://127.0.0.1:{d}\r\nContent-Type: application/octet-stream\r\nContent-Length: 2000000\r\n\r\n", .{ port, port });
    const answer = try rawRequest(port, head);
    defer testing.allocator.free(answer);
    try testing.expect(std.mem.startsWith(u8, answer, "HTTP/1.1 413"));
    const result = try ctx.execCapture(&.{ "queue", "dequeue", "big", "--timeout", "100" });
    try testing.expect(std.mem.indexOf(u8, result, "(no messages)") != null);
}

test "e2e/dashboard: no other site may frame it, and nothing is read as another type" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{ .dashboard_enabled = true } });
    defer ctx.deinit();
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    for ([_][]const u8{ "/", "/queues/some/deep/link", "/api/v1/queues" }) |path| {
        var r = try http.get(path);
        defer r.deinit();
        try testing.expectEqualStrings("DENY", r.getHeader("X-Frame-Options") orelse "");
        try testing.expectEqualStrings("frame-ancestors 'none'", r.getHeader("Content-Security-Policy") orelse "");
        try testing.expectEqualStrings("nosniff", r.getHeader("X-Content-Type-Options") orelse "");
    }
    // HEAD is GET without the body.
    var head = try http.requestExact(.HEAD, "/api/v1/queues", null, &.{});
    defer head.deinit();
    try testing.expectEqual(@as(u16, 200), head.status);
    try testing.expectEqual(@as(usize, 0), head.body.len);
}

test "e2e/dashboard: hosts and cors_origins from flo.toml are the ones served and granted" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{ .server = .{
        .dashboard_enabled = true,
        .dashboard_hosts = "flo.test",
        .dashboard_cors_origins = "https://ops.example.com",
    } });
    defer ctx.deinit();
    try seeded(ctx, "c");
    const port = ctx.getDashboardPort();
    var buf: [512]u8 = undefined;

    const named = try std.fmt.bufPrint(&buf, "GET /api/v1/queues HTTP/1.1\r\nHost: flo.test:{d}\r\n\r\n", .{port});
    const by_name = try rawRequest(port, named);
    defer testing.allocator.free(by_name);
    try testing.expect(std.mem.startsWith(u8, by_name, "HTTP/1.1 200"));

    const granted = try std.fmt.bufPrint(&buf, "POST /api/v1/queues/c/purge HTTP/1.1\r\nHost: flo.test:{d}\r\nOrigin: https://ops.example.com\r\nContent-Type: application/json\r\nContent-Length: 0\r\n\r\n", .{port});
    const purge = try rawRequest(port, granted);
    defer testing.allocator.free(purge);
    try testing.expect(std.mem.startsWith(u8, purge, "HTTP/1.1 200"));
    try testing.expect(std.mem.indexOf(u8, purge, "Access-Control-Allow-Origin: https://ops.example.com\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, purge, "\"purged\":2") != null);

    // Any other origin gets no grant, and the answer says it varies.
    const other = try std.fmt.bufPrint(&buf, "GET /api/v1/queues HTTP/1.1\r\nHost: localhost:{d}\r\nOrigin: https://other.example.com\r\n\r\n", .{port});
    const read = try rawRequest(port, other);
    defer testing.allocator.free(read);
    try testing.expect(std.mem.indexOf(u8, read, "Access-Control-Allow-Origin") == null);
    try testing.expect(std.mem.indexOf(u8, read, "Vary: Origin\r\n") != null);
}
