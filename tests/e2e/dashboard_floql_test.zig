//! Dashboard FloQL E2E Tests (issue #24, part 1)
//!
//! `POST /api/v1/timeseries/floql` used to echo the query back with an empty
//! `series` array. It now parses the query, resolves the source across shards,
//! runs the pipeline through the real FloQL executor, and returns computed
//! series.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "e2e/dashboard: floql returns raw points from the engine" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "dash_raw", "--value", "70.0" });
    try ctx.exec(&.{ "ts", "write", "dash_raw", "--value", "80.0" });
    try ctx.exec(&.{ "ts", "write", "dash_raw", "--value", "90.0" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.post("/api/v1/timeseries/floql", "dash_raw[1h]");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"key\":\"dash_raw\"") != null);
    // All three points came back — not the old empty-array stub.
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"point_count\":3") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"series\":[]") == null);
}

test "e2e/dashboard: floql actually executes pipeline stages" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "dash_agg", "--value", "70.0" });
    try ctx.exec(&.{ "ts", "write", "dash_agg", "--value", "80.0" });
    try ctx.exec(&.{ "ts", "write", "dash_agg", "--value", "90.0" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // avg() reduces the series to a single computed point: (70+80+90)/3 = 80.
    // A value the engine had to compute proves the executor ran.
    var resp = try http.post("/api/v1/timeseries/floql", "dash_agg[1h] | avg()");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"point_count\":1") != null);
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"value\":80") != null);
}

test "e2e/dashboard: floql decodes a percent-encoded ?q= query" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "dash_get", "--value", "10.0" });
    try ctx.exec(&.{ "ts", "write", "dash_get", "--value", "20.0" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // This is the exact shape the console sends (encodeURIComponent):
    //   dash_get[1h] | avg()
    var resp = try http.get("/api/v1/timeseries/floql?q=dash_get%5B1h%5D%20%7C%20avg()");
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    // Decoded, parsed and executed: (10+20)/2 = 15.
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"value\":15") != null);
}

test "e2e/dashboard: floql reports a parse error" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var resp = try http.post("/api/v1/timeseries/floql", "|||nonsense|||");
    defer resp.deinit();
    try testing.expect(std.mem.indexOf(u8, resp.body, "\"error\"") != null);
}

test "e2e/dashboard: floql source tag filters actually filter (#24 part 2a)" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    // Two tag-series under one measurement+field.
    try ctx.exec(&.{ "ts", "write", "dash_tags", "--tags", "host=web-01", "--value", "10.0" });
    try ctx.exec(&.{ "ts", "write", "dash_tags", "--tags", "host=web-02", "--value", "20.0" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // `{host=web-01}` selects one series → avg is 10, not 15. Before #24 part 2a
    // the filter was parsed and then ignored, so this returned 15.
    var one = try http.post("/api/v1/timeseries/floql", "dash_tags{host=web-01} [1h] | avg()");
    defer one.deinit();
    try testing.expectEqual(@as(u16, 200), one.status);
    try testing.expect(std.mem.indexOf(u8, one.body, "\"value\":10") != null);
    try testing.expect(std.mem.indexOf(u8, one.body, "\"value\":15") == null);

    // No filter still spans every tag-series → avg is 15.
    var all = try http.post("/api/v1/timeseries/floql", "dash_tags[1h] | avg()");
    defer all.deinit();
    try testing.expect(std.mem.indexOf(u8, all.body, "\"value\":15") != null);
}

test "e2e/dashboard: floql partial and glob tag filters (#24 part 2b)" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "ts", "write", "dash_2b", "--tags", "host=web-01,env=prod", "--value", "10.0" });
    try ctx.exec(&.{ "ts", "write", "dash_2b", "--tags", "host=web-02,env=prod", "--value", "20.0" });
    try ctx.exec(&.{ "ts", "write", "dash_2b", "--tags", "host=api-01,env=prod", "--value", "60.0" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // PARTIAL filter — names one tag of a two-tag set. Returned nothing under
    // the 2a tag-hash lookup; now selects that one series.
    var partial = try http.post("/api/v1/timeseries/floql", "dash_2b{host=web-01} [1h] | avg()");
    defer partial.deinit();
    try testing.expectEqual(@as(u16, 200), partial.status);
    try testing.expect(std.mem.indexOf(u8, partial.body, "\"value\":10") != null);

    // GLOB — web-* selects two series: avg(10,20) = 15.
    var glob = try http.post("/api/v1/timeseries/floql", "dash_2b{host=~web-*} [1h] | avg()");
    defer glob.deinit();
    try testing.expect(std.mem.indexOf(u8, glob.body, "\"value\":15") != null);

    // NEGATED — everything but api-01 is the same two series.
    var neg = try http.post("/api/v1/timeseries/floql", "dash_2b{host!=api-01} [1h] | avg()");
    defer neg.deinit();
    try testing.expect(std.mem.indexOf(u8, neg.body, "\"value\":15") != null);

    // A shared tag selects all three: avg(10,20,60) = 30.
    var all = try http.post("/api/v1/timeseries/floql", "dash_2b{env=prod} [1h] | avg()");
    defer all.deinit();
    try testing.expect(std.mem.indexOf(u8, all.body, "\"value\":30") != null);
}
