//! One data dir, one server: a second server pointed at a data dir another
//! is using refuses to start, and says which.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ServerProcess = stdx.testing.ServerProcess;

test "e2e/data-dir: a second server on a data dir in use refuses to start, and the first keeps serving" {
    const allocator = testing.allocator;
    const first = try ServerProcess.initWithConfig(allocator, .{ .shards = 1 });
    defer first.deinit();
    try first.start();

    const second = try ServerProcess.initWithConfig(allocator, .{ .shards = 1, .data_dir_of = first.data_dir });
    defer second.deinit();
    try testing.expectError(error.ServerNotReady, second.startWithTimeout(3000));
    try testing.expect(try second.logsContain("is in use by another flo server"));

    var ep: [32]u8 = undefined;
    const cli = try stdx.testing.CliRunner.init(allocator, first.flo_binary, try std.fmt.bufPrint(&ep, "127.0.0.1:{d}", .{first.port}));
    defer cli.deinit();
    var put = try cli.run(&.{ "kv", "set", "k", "still-here" });
    defer put.deinit();
    var got = try cli.run(&.{ "kv", "get", "k" });
    defer got.deinit();
    try testing.expect(got.stdoutContains("still-here"));
}

test "e2e/data-dir: a server takes over a data dir once its previous holder has stopped" {
    const allocator = testing.allocator;
    const first = try ServerProcess.initWithConfig(allocator, .{ .shards = 1 });
    defer first.deinit();
    try first.start();
    first.stop();

    const second = try ServerProcess.initWithConfig(allocator, .{ .shards = 1, .data_dir_of = first.data_dir });
    defer second.deinit();
    try second.start();
}

test "e2e/data-dir: a data dir of a newer format, or a FORMAT that isn't one, refuses to start, by name" {
    const allocator = testing.allocator;
    const first = try ServerProcess.initWithConfig(allocator, .{ .shards = 1 });
    defer first.deinit();
    try first.start();
    first.stop();

    const path = try std.fs.path.join(allocator, &.{ first.data_dir, "FORMAT" });
    defer allocator.free(path);
    const newer = try std.fmt.allocPrint(allocator, "data directory format 2, this binary reads up to 1 ({s}); start a flo that reads format 2", .{path});
    defer allocator.free(newer);
    const garbage = try std.fmt.allocPrint(allocator, "{s} holds \"two\", not a data directory format; it should hold a number such as 1", .{path});
    defer allocator.free(garbage);
    for ([_]struct { format: []const u8, says: []const u8 }{
        .{ .format = "2\n", .says = newer }, // a newer flo wrote this directory
        .{ .format = "two\n", .says = garbage },
    }) |case| {
        const f = try stdx.fs.createFile(path, .{ .truncate = true });
        try stdx.fs.writeAll(f, case.format);
        stdx.fs.closeFile(f);

        const second = try ServerProcess.initWithConfig(allocator, .{ .shards = 1, .data_dir_of = first.data_dir });
        defer second.deinit();
        second.dump_log_on_failure = false;
        try testing.expectError(error.ServerNotReady, second.startWithTimeout(3000));
        var why: [64]u8 = undefined;
        try testing.expectEqualStrings("exited with code 3", second.abnormalExit(&why).?);
        try testing.expect(try second.logsContain(case.says));
    }
}
