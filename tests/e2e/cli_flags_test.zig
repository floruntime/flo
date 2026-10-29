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
