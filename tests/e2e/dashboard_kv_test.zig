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

const src = @import("src");

/// One page of the dashboard's key listing: (key, size) pairs, and the
/// cursor to the next page, if any.
const Page = struct { keys: std.ArrayList(struct { key: []u8, size: i64 }) = .empty, cursor: ?[]u8 = null, has_more: bool = false };

fn freePage(page: *Page) void {
    for (page.keys.items) |k| testing.allocator.free(k.key);
    page.keys.deinit(testing.allocator);
    if (page.cursor) |c| testing.allocator.free(c);
}

fn getPage(http: anytype, path: []const u8) !Page {
    var resp = try http.get(path);
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, resp.bodyTrimmed(), .{});
    defer parsed.deinit();
    const root = parsed.value.object;
    var page: Page = .{ .has_more = root.get("has_more").?.bool };
    errdefer freePage(&page);
    for (root.get("keys").?.array.items) |k| try page.keys.append(testing.allocator, .{
        .key = try testing.allocator.dupe(u8, k.object.get("key").?.string),
        .size = k.object.get("size").?.integer,
    });
    if (root.get("cursor").? == .string) page.cursor = try testing.allocator.dupe(u8, root.get("cursor").?.string);
    try testing.expectEqual(page.has_more, page.cursor != null);
    try testing.expectEqual(@as(i64, @intCast(page.keys.items.len)), root.get("count").?.integer);
    return page;
}

test "e2e/dashboard: kv keys list a page at a time, across shards, with sizes, and only the namespace's live keys" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .shards = 2 },
    });
    defer ctx.deinit();
    for ([_][]const u8{ "a", "bb", "ccc", "dddd", "eeeee", "gone" }) |k| try ctx.exec(&.{ "kv", "set", k, k });
    try ctx.exec(&.{ "kv", "delete", "gone" });
    try ctx.exec(&.{ "kv", "set", "elsewhere", "x", "-n", "other" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    var seen = std.StringHashMap(i64).init(testing.allocator);
    defer seen.deinit();
    var pages: usize = 0;
    var path_buf: [256]u8 = undefined;
    var path: []const u8 = "/api/v1/kv/namespaces/default/keys?limit=2";
    var cursor_hold: ?[]u8 = null;
    defer if (cursor_hold) |c| testing.allocator.free(c);
    while (true) : (pages += 1) {
        var page = try getPage(http, path);
        defer freePage(&page);
        try testing.expect(page.keys.items.len <= 2);
        for (page.keys.items) |k| try seen.put(try testing.allocator.dupe(u8, k.key), k.size);
        const next = page.cursor orelse break;
        if (cursor_hold) |c| testing.allocator.free(c);
        cursor_hold = try testing.allocator.dupe(u8, next);
        path = try std.fmt.bufPrint(&path_buf, "/api/v1/kv/namespaces/default/keys?limit=2&cursor={s}", .{cursor_hold.?});
        try testing.expect(pages < 10);
    }
    defer {
        var it = seen.keyIterator();
        while (it.next()) |k| testing.allocator.free(k.*);
    }
    try testing.expect(pages >= 2);
    try testing.expectEqual(@as(usize, 5), seen.count());
    for ([_][]const u8{ "a", "bb", "ccc", "dddd", "eeeee" }) |k| try testing.expectEqual(@as(?i64, @intCast(k.len)), seen.get(k));

    // A cursor that isn't one is refused, not read.
    var bad = try http.get("/api/v1/kv/namespaces/default/keys?cursor=zz");
    defer bad.deinit();
    try testing.expect(bad.bodyContains("cursor must be the hex cursor a page gave"));
}

test "e2e/dashboard: listing keys under heavy writes always answers, and the node keeps serving" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .shards = 2 },
    });
    defer ctx.deinit();

    // A writer adding new keys (and deleting some) as fast as it can, so
    // the shards keep growing and rehashing their maps while the dashboard
    // lists them.
    const Storm = struct {
        endpoint: []const u8,
        stop: std.atomic.Value(bool) = .init(false),
        writes: std.atomic.Value(u64) = .init(0),
        next: std.atomic.Value(u64) = .init(0),
        fn run(self: *@This()) void {
            var conn = src.cli_client.Client.init(testing.allocator, self.endpoint);
            defer conn.deinit();
            conn.connect() catch return;
            var key: [32]u8 = undefined;
            while (!self.stop.load(.acquire)) {
                const i = self.next.fetchAdd(1, .monotonic);
                const k = std.fmt.bufPrint(&key, "storm-{d}", .{i}) catch unreachable;
                var r = if (i % 3 == 2)
                    src.cli_client.kv.call(&conn, "kv.delete", "", k, .{ .cas_version = null, .routing_key = null, .txn = null }) catch return
                else
                    src.cli_client.kv.set(&conn, "", k, .{ .value = "v", .ttl_ms = null, .condition = .always, .cas_version = null, .routing_key = null, .txn = null }) catch return;
                r.deinit();
                _ = self.writes.fetchAdd(1, .monotonic);
            }
        }
    };
    var storm: Storm = .{ .endpoint = ctx.endpoint };
    var writers: [8]std.Thread = undefined;
    for (&writers) |*w| w.* = try std.Thread.spawn(.{}, Storm.run, .{&storm});
    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    for (0..200) |_| {
        var page = try getPage(http, "/api/v1/kv/namespaces/default/keys?limit=256");
        freePage(&page);
    }
    storm.stop.store(true, .release);
    for (writers) |w| w.join();
    try testing.expect(storm.writes.load(.monotonic) > 1000);
    // Still serving.
    var got = try ctx.cli.run(&.{ "kv", "set", "after", "storm" });
    defer got.deinit();
    try testing.expectEqual(@as(u8, 0), got.exit_code);
}

test "e2e/dashboard: a kv key's value carries its expiry, and its history marks the delete" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .shards = 2 },
    });
    defer ctx.deinit();
    try ctx.exec(&.{ "kv", "set", "ttl", "soon", "--ttl-ms", "1h" });
    try ctx.exec(&.{ "kv", "set", "h", "one" });
    try ctx.exec(&.{ "kv", "set", "h", "two" });
    try ctx.exec(&.{ "kv", "delete", "h" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    const before_ms = stdx.time.milliTimestamp();
    var value = try http.get("/api/v1/kv/namespaces/default/keys/ttl");
    defer value.deinit();
    try testing.expectEqual(@as(u16, 200), value.status);
    const v = try std.json.parseFromSlice(std.json.Value, testing.allocator, value.bodyTrimmed(), .{});
    defer v.deinit();
    try testing.expect(v.value.object.get("found").?.bool);
    try testing.expectEqualStrings("soon", v.value.object.get("value").?.string);
    try testing.expectEqual(@as(i64, 4), v.value.object.get("size").?.integer);
    const expires = v.value.object.get("expires_at_ms").?.integer;
    try testing.expect(expires > before_ms and expires <= before_ms + std.time.ms_per_hour + 60 * std.time.ms_per_s);

    var missing = try http.get("/api/v1/kv/namespaces/default/keys/h");
    defer missing.deinit();
    try testing.expectEqual(@as(u16, 200), missing.status);
    try testing.expectEqualStrings("{\"key\":\"h\",\"namespace\":\"default\",\"found\":false}", missing.bodyTrimmed());

    var hist = try http.get("/api/v1/kv/namespaces/default/keys/h/history");
    defer hist.deinit();
    try testing.expectEqual(@as(u16, 200), hist.status);
    const hv = try std.json.parseFromSlice(std.json.Value, testing.allocator, hist.bodyTrimmed(), .{});
    defer hv.deinit();
    const versions = hv.value.object.get("versions").?.array.items;
    try testing.expectEqual(@as(usize, 3), versions.len);
    try testing.expectEqual(@as(i64, 3), hv.value.object.get("version_count").?.integer);
    // Newest first: the delete, then "two", then "one".
    try testing.expect(versions[0].object.get("tombstone").?.bool);
    for (versions[1..], [_]i64{ 3, 3 }) |ver, size| {
        try testing.expect(!ver.object.get("tombstone").?.bool);
        try testing.expectEqual(size, ver.object.get("size").?.integer);
    }

    var one = try http.get("/api/v1/kv/namespaces/default/keys/h/history?limit=1");
    defer one.deinit();
    const ov = try std.json.parseFromSlice(std.json.Value, testing.allocator, one.bodyTrimmed(), .{});
    defer ov.deinit();
    try testing.expectEqual(@as(usize, 1), ov.value.object.get("versions").?.array.items.len);
}

test "e2e/dashboard: a kv key prefix is read percent-encoded, as the console sends it" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .shards = 2 },
    });
    defer ctx.deinit();
    for ([_][]const u8{ "user:1", "user:2", "user 3", "other" }) |k| try ctx.exec(&.{ "kv", "set", k, "v" });

    var http = try ctx.createDashboardHttp();
    defer http.deinit();
    for ([_]struct { path: []const u8, keys: []const []const u8 }{
        .{ .path = "/api/v1/kv/namespaces/default/keys?prefix=user%3A", .keys = &.{ "user:1", "user:2" } },
        .{ .path = "/api/v1/kv/namespaces/default/keys?prefix=user%203", .keys = &.{"user 3"} },
        .{ .path = "/api/v1/kv/namespaces/default/keys?prefix=user", .keys = &.{ "user 3", "user:1", "user:2" } },
    }) |c| {
        var page = try getPage(http, c.path);
        defer freePage(&page);
        var got: std.ArrayList([]const u8) = .empty;
        defer got.deinit(testing.allocator);
        for (page.keys.items) |k| try got.append(testing.allocator, k.key);
        std.mem.sort([]const u8, got.items, {}, struct {
            fn lt(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.lt);
        try testing.expectEqual(c.keys.len, got.items.len);
        for (c.keys, got.items) |want, have| try testing.expectEqualStrings(want, have);
    }

    var nul = try http.get("/api/v1/kv/namespaces/default/keys?prefix=user%00");
    defer nul.deinit();
    try testing.expectEqualStrings("{\"error\":\"prefix must not hold a NUL\"}", nul.bodyTrimmed());
}
