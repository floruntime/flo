//! The console opens on a namespace that may never have been created (its
//! default is "production"). A namespace never created holds nothing to
//! show, and the console shows it as one that exists and is empty: its
//! lists empty, its items not found, never an error.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "e2e/dashboard: a namespace never created answers as one that exists and is empty" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .shards = 4 },
    });
    defer ctx.deinit();
    try ctx.exec(&.{ "ns", "create", "empty" });
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    const paths = [_][]const u8{
        "/api/v1/streams?namespace={s}",
        "/api/v1/queues?namespace={s}",
        "/api/v1/timeseries?namespace={s}",
        "/api/v1/processing/jobs?namespace={s}",
        "/api/v1/workflow/definitions?namespace={s}",
        "/api/v1/actions?namespace={s}",
        "/api/v1/workers?namespace={s}",
        "/api/v1/workflow/runs?namespace={s}&limit=200",
        "/api/v1/streams/s1?namespace={s}",
        "/api/v1/queues/q1?namespace={s}",
        "/api/v1/queues/q1/dlq?namespace={s}",
        "/api/v1/streams/s1/messages?namespace={s}&limit=20",
        "/api/v1/actions/a1/runs?namespace={s}&limit=100",
        "/api/v1/processing/jobs/0123456789abcdef0123456789abcdef?namespace={s}",
        "/api/v1/workflow/definitions/w1?namespace={s}",
        "/api/v1/actions/a1?namespace={s}",
        "/api/v1/kv/namespaces/{s}/keys/k1",
        "/api/v1/kv/namespaces/{s}/keys",
        "/api/v1/namespaces/{s}",
        "/api/v1/timeseries/m1/data?field=value&namespace={s}",
        "/api/v1/streams/s1/groups?namespace={s}",
        "/api/v1/workflows?namespace={s}",
        "/api/v1/workflow/runs/r1?namespace={s}",
        "/api/v1/workflow/runs/r1/history?namespace={s}",
    };
    inline for (paths) |path| {
        var never_buf: [128]u8 = undefined;
        var empty_buf: [128]u8 = undefined;
        var never = try http.get(try std.fmt.bufPrint(&never_buf, path, .{"production"}));
        defer never.deinit();
        var empty = try http.get(try std.fmt.bufPrint(&empty_buf, path, .{"empty"}));
        defer empty.deinit();
        const never_body = try std.mem.replaceOwned(u8, testing.allocator, never.body, "production", "empty");
        defer testing.allocator.free(never_body);
        if (never.status != empty.status or !std.mem.eql(u8, never_body, empty.body))
            std.debug.print("{s}\n  never created: {d} {s}\n  empty:         {d} {s}\n", .{ path, never.status, never.body, empty.status, empty.body });
        try testing.expectEqual(empty.status, never.status);
        try testing.expectEqualStrings(empty.body, never_body);
        try testing.expect(std.mem.indexOf(u8, never.body, "namespace not found") == null);
    }

    // A query of a namespace never created finds no series; one that
    // exists names the measurement with none of its points.
    var floql = try http.get("/api/v1/timeseries/floql?q=m1%5B1h%5D&namespace=production");
    defer floql.deinit();
    try testing.expectEqual(@as(u16, 200), floql.status);
    try testing.expect(std.mem.indexOf(u8, floql.body, "\"series\":[]") != null);
}
