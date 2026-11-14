//! The per-node memory budget on a default single node (no cluster). The
//! shard-0 leader proposes the genesis pool-share and its own row in its tick,
//! the budget derives, and it shows on the dashboard — the single node is the
//! main mode, so the budget has to appear there without a cluster.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "e2e/memory_budget: a single node shows a non-zero budget" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true },
    });
    defer ctx.deinit();

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    // The budget derives in the server's own tick after boot (propose → apply
    // → derive), with no client activity, so poll until it appears.
    var budget: i64 = 0;
    var governing: i64 = 0;
    var provisional: i64 = -1;
    var tries: usize = 0;
    while (tries < 100) : (tries += 1) {
        var resp = try http.get("/api/v1/metrics");
        defer resp.deinit();
        try testing.expectEqual(@as(u16, 200), resp.status);

        const parsed = std.json.parseFromSlice(std.json.Value, testing.allocator, resp.body, .{}) catch {
            stdx.time.sleep(50 * std.time.ns_per_ms);
            continue;
        };
        defer parsed.deinit();

        if (parsed.value.object.get("memory_budget")) |mb| {
            budget = mb.object.get("budget_bytes").?.integer;
            governing = mb.object.get("governing_node").?.integer;
            provisional = mb.object.get("provisional_node").?.integer;
            if (budget > 0) break;
        }
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }

    try testing.expect(budget > 0); // the budget appears without a cluster
    try testing.expect(governing > 0); // a real node governs it
    try testing.expectEqual(@as(i64, 0), provisional); // its own row applied, so not provisional
}
