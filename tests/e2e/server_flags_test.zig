//! `flo server start` refuses a port flag it cannot bind with exit 2 (usage) and a
//! message naming the flag — not a panic, not a default.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");
const ServerProcess = stdx.testing.ServerProcess;
const CliRunner = stdx.testing.CliRunner;

test "e2e/server: a port flag past 65535 is refused with exit 2 and the flag named" {
    const allocator = testing.allocator;
    // Only for its binary and data dir; the server is never started.
    const server = try ServerProcess.init(allocator);
    defer server.deinit();
    const cli = try CliRunner.init(allocator, server.flo_binary, "127.0.0.1:1");
    defer cli.deinit();

    for ([_][]const u8{ "port", "raft-port", "metrics-port", "dashboard-port" }) |name| {
        const flag = try std.fmt.allocPrint(allocator, "--{s}", .{name});
        defer allocator.free(flag);
        var out = try cli.run(&.{ "server", "start", "--data-dir", server.data_dir, flag, "70000" });
        defer out.deinit();
        const want = try std.fmt.allocPrint(allocator, "Error: --{s} 70000 is not a port; use 1 to 65535, or 0 for the default\n", .{name});
        defer allocator.free(want);
        try testing.expectEqual(@as(u8, 2), out.exit_code);
        try testing.expectEqualStrings(want, out.stderr);
    }
}
