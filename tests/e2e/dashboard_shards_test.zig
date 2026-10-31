//! The dashboard's lists and details on a node of more shards than one list
//! page asks, with the same names in two namespaces: every list holds every
//! item once, under its own namespace, and a detail answers the asked
//! namespace's item.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

const SHARDS = 12;
const NAMES = 30;
const SPACES = [_][]const u8{ "na", "nb" };

fn count(body: []const u8, needle: []const u8) usize {
    var n: usize = 0;
    var at: usize = 0;
    while (std.mem.indexOfPos(u8, body, at, needle)) |i| : (at = i + needle.len) n += 1;
    return n;
}

/// GET `path` answers 200, and holds each of `fmt`'s NAMES once.
fn expectEachOnce(http: anytype, path: []const u8, comptime fmt: []const u8, args: anytype) !void {
    var resp = try http.get(path);
    defer resp.deinit();
    try testing.expectEqual(@as(u16, 200), resp.status);
    for (0..NAMES) |i| {
        var buf: [96]u8 = undefined;
        const needle = try std.fmt.bufPrint(&buf, fmt, .{i} ++ args);
        const c = count(resp.body, needle);
        if (c != 1) std.debug.print("{s}: {s} found {d} times in\n{s}\n", .{ path, needle, c, resp.body });
        try testing.expectEqual(@as(usize, 1), c);
    }
}

test "e2e/dashboard: on many shards, lists hold every item of every namespace once, and details answer the asked namespace" {
    var ctx = try stdx.testing.TestContext.initWithConfig(testing.allocator, .{
        .server = .{ .dashboard_enabled = true, .shards = SHARDS },
    });
    defer ctx.deinit();

    // The same names in both namespaces; nb's hold one record more, so a
    // detail shows which namespace answered.
    for (SPACES, 1..) |ns, records| {
        try ctx.exec(&.{ "ns", "create", ns });
        for (0..NAMES) |i| {
            var name_buf: [16]u8 = undefined;
            const s = try std.fmt.bufPrint(&name_buf, "st-{d:0>2}", .{i});
            for (0..records) |_| try ctx.exec(&.{ "stream", "append", s, "x", "-n", ns });
            var q_buf: [16]u8 = undefined;
            const q = try std.fmt.bufPrint(&q_buf, "q-{d:0>2}", .{i});
            for (0..records) |_| try ctx.exec(&.{ "queue", "enqueue", q, "x", "-n", ns });
            var a_buf: [16]u8 = undefined;
            try ctx.exec(&.{ "action", "register", try std.fmt.bufPrint(&a_buf, "act-{d:0>2}", .{i}), "-n", ns });
        }
    }

    var http = try ctx.createDashboardHttp();
    defer http.deinit();

    for (SPACES) |ns| {
        var path_buf: [96]u8 = undefined;
        try expectEachOnce(http, try std.fmt.bufPrint(&path_buf, "/api/v1/streams?namespace={s}", .{ns}), "\"name\":\"st-{d:0>2}\",\"namespace\":\"{s}\"", .{ns});
        try expectEachOnce(http, try std.fmt.bufPrint(&path_buf, "/api/v1/queues?namespace={s}", .{ns}), "\"name\":\"q-{d:0>2}\",\"namespace\":\"{s}\"", .{ns});
        try expectEachOnce(http, try std.fmt.bufPrint(&path_buf, "/api/v1/actions?namespace={s}", .{ns}), "\"name\":\"act-{d:0>2}\"", .{});
    }
    // With no namespace asked, the queue list holds every namespace's.
    for (SPACES) |ns| try expectEachOnce(http, "/api/v1/queues", "\"name\":\"q-{d:0>2}\",\"namespace\":\"{s}\"", .{ns});

    // A detail answers the asked namespace's item: na's hold one record,
    // nb's two.
    for (SPACES, 1..) |ns, records| {
        var path_buf: [96]u8 = undefined;
        var want_buf: [48]u8 = undefined;
        {
            var resp = try http.get(try std.fmt.bufPrint(&path_buf, "/api/v1/streams/st-07?namespace={s}", .{ns}));
            defer resp.deinit();
            try testing.expectEqual(@as(u16, 200), resp.status);
            try testing.expect(std.mem.indexOf(u8, resp.body, try std.fmt.bufPrint(&want_buf, "\"total_count\":{d},", .{records})) != null);
        }
        {
            var resp = try http.get(try std.fmt.bufPrint(&path_buf, "/api/v1/queues/q-07?namespace={s}", .{ns}));
            defer resp.deinit();
            try testing.expectEqual(@as(u16, 200), resp.status);
            try testing.expect(std.mem.indexOf(u8, resp.body, try std.fmt.bufPrint(&want_buf, "\"enqueued\":{d},", .{records})) != null);
        }
    }

    // A stream no namespace holds is not found, not an empty one.
    var missing = try http.get("/api/v1/streams/no-such-stream?namespace=na");
    defer missing.deinit();
    try testing.expectEqual(@as(u16, 404), missing.status);
}
