//! HTTP API E2E Tests
//!
//! Tests for Flo's REST API endpoints (dashboard API).
//! Requires dashboard to be enabled in server config.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

// =============================================================================
// Health Check Tests
// =============================================================================

test "e2e/http: health check returns 200" {
    // Enable dashboard for HTTP tests
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/health");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
}

test "e2e/http: health check body contains ok" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/health");
    defer resp.deinit();

    try testing.expect(resp.succeeded());
    // Health endpoint typically returns simple OK or JSON status
    try testing.expect(resp.body.len > 0);
}

// =============================================================================
// API Endpoints Tests
// =============================================================================

test "e2e/http: cluster stats endpoint" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/cluster/stats");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
}

test "e2e/http: namespaces list endpoint" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/namespaces");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
}

test "e2e/http: streams list endpoint" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/streams");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
}

test "e2e/http: queues list endpoint" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/queues");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
}

// =============================================================================
// Unknown Endpoint Tests
// =============================================================================

test "e2e/http: unknown endpoint handled by SPA" {
    // Note: Dashboard is an SPA that serves index.html for unknown routes,
    // so unknown API paths return 200 (HTML) rather than 404
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/api/v1/nonexistent");
    defer resp.deinit();

    // SPA serves HTML for unknown routes
    try testing.expectEqual(@as(u16, 200), resp.status);
}

// =============================================================================
// Auth Enforcement Tests
// =============================================================================

test "e2e/http: health endpoint accessible with auth enabled" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .auth_enabled = true },
    });
    defer ctx.deinit();

    // Health should always be publicly accessible regardless of auth config
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.get("/health");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
}

test "e2e/http: auth bootstrap produces api key" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .auth_enabled = true },
    });
    defer ctx.deinit();

    // When auth_enabled = true, bootstrap runs and ctx.api_key is set
    try testing.expect(ctx.api_key != null);
    const key = ctx.api_key.?;
    // Bootstrap keys start with "flo_sk_"
    try testing.expect(std.mem.startsWith(u8, key, "flo_sk_"));
    try testing.expect(key.len > 10);
}

test "e2e/http: api endpoints accessible with auth enabled" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .auth_enabled = true },
    });
    defer ctx.deinit();

    // Authed HTTP client (uses bootstrap key)
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    if (ctx.api_key) |key| {
        try http.setApiKey(key);
    }

    var resp = try http.get("/api/v1/cluster/stats");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.isJson());
}
