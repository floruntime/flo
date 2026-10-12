//! Dashboard KV API E2E Tests

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "e2e/dashboard: kv delete removes the key, and deleting a missing one succeeds" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    try ctx.exec(&.{ "kv", "set", "dk", "v" });
    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    var del = try http.delete("/api/v1/kv/namespaces/default/keys/dk");
    defer del.deinit();
    try testing.expectEqual(@as(u16, 200), del.status);
    try testing.expectEqualStrings("{\"key\":\"dk\",\"namespace\":\"default\",\"ok\":true}", del.bodyTrimmed());

    var get = try ctx.cli.run(&.{ "kv", "get", "dk" });
    defer get.deinit();
    try testing.expectEqual(@as(u8, 1), get.exit_code);

    var again = try http.delete("/api/v1/kv/namespaces/default/keys/dk");
    defer again.deinit();
    try testing.expectEqual(@as(u16, 200), again.status);
    try testing.expect(again.bodyContains("\"ok\":true"));
}
