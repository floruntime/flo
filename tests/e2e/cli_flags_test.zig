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

    try ctx.exec(&.{ "ns", "create", "early" });

    // -e given first, for every family: runRaw adds no --endpoint, so a
    // command that ignored it would go to 127.0.0.1:9000 and fail there.
    const writes = [_][]const []const u8{
        &.{ "-e", endpoint, "-n", "early", "kv", "set", "k-early", "v-early" },
        &.{ "-e", endpoint, "-n", "early", "stream", "append", "s-early", "x-early" },
        &.{ "-e", endpoint, "-n", "early", "queue", "enqueue", "q-early", "x" },
        &.{ "-e", endpoint, "-n", "early", "ts", "write", "m-early", "--value", "1" },
        &.{ "-e", endpoint, "-n", "early", "action", "register", "act-early" },
    };
    for (writes) |args| {
        var r = try ctx.cli.runRaw(args);
        defer r.deinit();
        if (r.exit_code != 0 or r.stderr.len > 0) std.debug.print("{s} {s}: {s}{s}\n", .{ args[4], args[5], r.stdout, r.stderr });
        try testing.expectEqual(@as(u8, 0), r.exit_code);
        try testing.expectEqual(@as(usize, 0), r.stderr.len);
    }

    // Each landed in the namespace named first, and only there.
    const reads = [_]struct { args: []const []const u8, want: []const u8 }{
        .{ .args = &.{ "kv", "get", "k-early" }, .want = "v-early" },
        .{ .args = &.{ "stream", "read", "s-early" }, .want = "x-early" },
        .{ .args = &.{ "ts", "list" }, .want = "m-early" },
        .{ .args = &.{ "action", "list" }, .want = "act-early" },
        .{ .args = &.{ "queue", "list" }, .want = "q-early" },
    };
    for (reads) |read| {
        var buf: [8][]const u8 = undefined;
        @memcpy(buf[0..read.args.len], read.args);
        buf[read.args.len] = "-n";
        buf[read.args.len + 1] = "early";
        var r = try ctx.cli.run(buf[0 .. read.args.len + 2]);
        defer r.deinit();
        if (r.stdoutCount(read.want) != 1) std.debug.print("{s} {s}: {s}{s}\n", .{ read.args[0], read.args[1], r.stdout, r.stderr });
        try testing.expectEqual(@as(usize, 1), r.stdoutCount(read.want));
        var elsewhere = try ctx.cli.run(read.args);
        defer elsewhere.deinit();
        try testing.expectEqual(@as(usize, 0), elsewhere.stdoutCount(read.want));
    }

    // Reads too, through -e given first: each answers from that server.
    const reads_first = [_]struct { args: []const []const u8, want: []const u8 }{
        .{ .args = &.{ "-e", endpoint, "-n", "early", "action", "list" }, .want = "act-early" },
        .{ .args = &.{ "-e", endpoint, "-n", "early", "worker", "list" }, .want = "(no workers)" },
        .{ .args = &.{ "-e", endpoint, "-n", "early", "workflow", "list" }, .want = "(no workflows)" },
        .{ .args = &.{ "-e", endpoint, "-n", "early", "processing", "list" }, .want = "(no " },
        .{ .args = &.{ "-e", endpoint, "ns", "ls" }, .want = "early" },
    };
    for (reads_first) |read| {
        var r = try ctx.cli.runRaw(read.args);
        defer r.deinit();
        if (r.stdoutCount(read.want) == 0) std.debug.print("{s} {s}: {s}{s}\n", .{ read.args[4], read.args[5], r.stdout, r.stderr });
        try testing.expect(r.stdoutCount(read.want) >= 1);
        try testing.expectEqual(@as(usize, 0), r.stderr.len);
    }

    // -o json given first shapes the answer.
    var json = try ctx.cli.runRaw(&.{ "-o", "json", "-e", endpoint, "-n", "early", "action", "list" });
    defer json.deinit();
    try testing.expectEqualStrings("[{\"name\":\"act-early\"}]\n", json.stdout);

    // Given both before and after, the one after wins.
    var both = try ctx.cli.runRaw(&.{ "-e", "127.0.0.1:1", "kv", "get", "k-early", "-n", "early", "-e", endpoint });
    defer both.deinit();
    try testing.expectEqual(@as(usize, 1), both.stdoutCount("v-early"));
}
