//! A global flag's short name means the same thing on every command.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "e2e/cli: stream read -e host:port reads from that server" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    var put = try ctx.cli.run(&.{ "stream", "append", "short-e", "reached" });
    defer put.deinit();
    try testing.expectEqual(@as(u8, 0), put.exit_code);

    // runRaw adds no --endpoint: -e alone names the server.
    var ep: [32]u8 = undefined;
    const endpoint = try std.fmt.bufPrint(&ep, "127.0.0.1:{d}", .{ctx.server.port});
    var got = try ctx.cli.runRaw(&.{ "stream", "read", "short-e", "-e", endpoint });
    defer got.deinit();
    if (got.exit_code != 0) std.debug.print("stdout: {s}\nstderr: {s}\n", .{ got.stdout, got.stderr });
    try testing.expectEqual(@as(u8, 0), got.exit_code);
    try testing.expectEqual(@as(usize, 1), got.stdoutCount("reached"));
}

test "e2e/cli: a global flag given before the command applies to every command family" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();
    var ep: [32]u8 = undefined;
    const endpoint = try std.fmt.bufPrint(&ep, "127.0.0.1:{d}", .{ctx.server.port});
    var port: [8]u8 = undefined;
    const port_s = try std.fmt.bufPrint(&port, "{d}", .{ctx.server.port});

    try ctx.exec(&.{ "ns", "create", "early" });

    // A write in each family, through -e, -p or -n given first: runRaw adds
    // no --endpoint, so only the leading flag can name the server.
    const writes = [_][]const []const u8{
        &.{ "-e", endpoint, "-n", "early", "kv", "set", "k", "v" },
        &.{ "-e", endpoint, "-n", "early", "stream", "append", "s", "x" },
        &.{ "-e", endpoint, "-n", "early", "queue", "enqueue", "q", "x" },
        &.{ "-e", endpoint, "-n", "early", "ts", "write", "m", "--value", "1" },
        &.{ "-p", port_s, "-n", "early", "action", "register", "a" },
        &.{ "-p", port_s, "-n", "early", "worker", "list" },
        &.{ "-p", port_s, "-n", "early", "workflow", "list" },
        &.{ "-p", port_s, "-n", "early", "processing", "list" },
        &.{ "-p", port_s, "ns", "ls" },
    };
    for (writes) |args| {
        var r = try ctx.cli.runRaw(args);
        defer r.deinit();
        if (r.exit_code != 0) std.debug.print("{s} {s}: {s}{s}\n", .{ args[4], args[5], r.stdout, r.stderr });
        try testing.expectEqual(@as(u8, 0), r.exit_code);
    }

    // Each landed in the namespace named first, and -o json given first
    // shapes the answer.
    const reads = [_]struct { args: []const []const u8, want: []const u8 }{
        .{ .args = &.{ "kv", "get", "k", "-n", "early" }, .want = "v" },
        .{ .args = &.{ "stream", "read", "s", "-n", "early" }, .want = "x" },
        .{ .args = &.{ "ts", "list", "-n", "early" }, .want = "m" },
        .{ .args = &.{ "action", "list", "-n", "early" }, .want = "a" },
    };
    for (reads) |read| {
        var r = try ctx.cli.run(read.args);
        defer r.deinit();
        try testing.expectEqual(@as(u8, 0), r.exit_code);
        try testing.expect(std.mem.indexOf(u8, r.stdout, read.want) != null);
    }
    var json = try ctx.cli.runRaw(&.{ "-o", "json", "-e", endpoint, "-n", "early", "kv", "get", "k" });
    defer json.deinit();
    try testing.expectEqual(@as(u8, 0), json.exit_code);
    try testing.expect(std.mem.startsWith(u8, std.mem.trimStart(u8, json.stdout, " \n"), "{"));

    // Given both before and after, the one after wins.
    var both = try ctx.cli.runRaw(&.{ "-e", "127.0.0.1:1", "kv", "get", "k", "-n", "early", "-e", endpoint });
    defer both.deinit();
    try testing.expectEqual(@as(u8, 0), both.exit_code);
}
