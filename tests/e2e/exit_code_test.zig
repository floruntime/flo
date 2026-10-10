//! Every CLI outcome has its own exit code, so a script can tell them apart:
//! 0 ok · 1 not found · 2 usage · 3 refused · 4 retryable · 5 transport.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

fn expectExit(ctx: *stdx.testing.TestContext, code: u8, args: []const []const u8) !void {
    var r = try ctx.cli.run(args);
    defer r.deinit();
    if (r.exit_code != code) {
        std.debug.print("\n`flo {s}` exited {d}, want {d}\nstdout: {s}\nstderr: {s}\n", .{ args[0], r.exit_code, code, r.stdout, r.stderr });
        return error.TestUnexpectedResult;
    }
}

test "e2e/exit: ok is 0, a missing key is 1, a bad flag is 2, a refusal is 3" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    try expectExit(ctx, 0, &.{ "kv", "set", "present", "v" });
    try expectExit(ctx, 0, &.{ "kv", "get", "present" });
    try expectExit(ctx, 1, &.{ "kv", "get", "absent" });
    try expectExit(ctx, 1, &.{ "kv", "delete", "absent" });
    try expectExit(ctx, 2, &.{ "kv", "get", "present", "--no-such-flag" });
    try expectExit(ctx, 2, &.{ "kv", "set", "k", "v", "--nx", "--xx" });
    try expectExit(ctx, 2, &.{"bogus"});
    try expectExit(ctx, 2, &.{ "kv", "bogus" });
    try expectExit(ctx, 2, &.{ "auth", "login" });
    try expectExit(ctx, 2, &.{ "kv", "get", "a", "b", "c" });
    // The key exists, so a set-if-absent is refused.
    try expectExit(ctx, 3, &.{ "kv", "set", "present", "w", "--nx" });
    // A file the command was told to read is the command's input.
    try expectExit(ctx, 2, &.{ "workflow", "create", "-f", "/tmp/flo-exit-code-test-no-such-file.yaml" });
}

test "e2e/exit: an endpoint nothing listens on is 5" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    inline for (.{
        &[_][]const u8{ "kv", "get", "k", "--endpoint", "127.0.0.1:1" },
        &[_][]const u8{ "cluster", "status", "--endpoint", "127.0.0.1:1" },
        &[_][]const u8{ "queue", "watch", "q", "--endpoint", "127.0.0.1:1" },
        &[_][]const u8{ "stream", "read", "s", "--follow", "--endpoint", "127.0.0.1:1" },
    }) |args| {
        var r = try ctx.cli.runRaw(args);
        defer r.deinit();
        if (r.exit_code != 5) {
            std.debug.print("\n`flo {s} {s}` exited {d}, want 5\nstderr: {s}\n", .{ args[0], args[1], r.exit_code, r.stderr });
            return error.TestUnexpectedResult;
        }
    }
}

test "e2e/exit: a refusal ends a watch or a follow with 3 instead of retrying" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // An empty name is refused by the server on the first request. A watch
    // that ignored refusals would run until killed (255).
    inline for (.{
        &[_][]const u8{ "queue", "watch", "" },
        &[_][]const u8{ "stream", "read", "", "--follow" },
    }) |args| {
        const cmd = try ctx.cli.runAsync(args);
        defer cmd.deinit();
        var r = try cmd.waitWithTimeout(10_000);
        defer r.deinit();
        if (r.exit_code != 3) {
            std.debug.print("\n`flo {s} {s}` exited {d}, want 3\nstderr: {s}\n", .{ args[0], args[1], r.exit_code, r.stderr });
            return error.TestUnexpectedResult;
        }
        try testing.expect(r.stderrContains("[bad_request]"));
    }
}

test "e2e/exit: a batch line the server refuses makes the batch exit 3" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    const path = "/tmp/flo-e2e-exit-batch-refused.txt";
    {
        const file = try stdx.fs.createFile(path, .{});
        defer stdx.fs.closeFile(file);
        // The second line parses here but its timestamp is past what the server stores.
        try stdx.fs.writeAll(file, "m v=1 1708700400000\nm v=2 99999999999999999\n");
    }
    defer stdx.fs.deleteFile(path) catch {};

    var r = try ctx.cli.run(&.{ "ts", "write", "--batch", "--file", path, "--precision", "ms" });
    defer r.deinit();
    try testing.expectEqual(@as(u8, 3), r.exit_code);
    try testing.expect(r.stderrContains("line 2: field v (0 of 1 fields written): ts write: timestamp out of range [bad_request]"));
    try testing.expect(r.stderrContains("1 of 2 lines failed"));
}
