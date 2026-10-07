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
