//! For harness_test.zig, which runs this test binary on its own: starts a
//! server and an async `server start` as a test would, prints their pids
//! and the data dir holding both (for the test to delete),
//! then dies by SIGKILL with no cleanup, as a test the runner kills at its
//! time limit does. Their watchdogs must then kill both. (The harness needs
//! std.testing, so this is a test binary; the e2e root skips helpers/.)

const std = @import("std");
const stdx = @import("stdx");

test "orphan" {
    const allocator = std.heap.page_allocator;
    stdx.testing.claimHeldByParent();

    var server = try stdx.testing.ServerProcess.init(allocator);
    try server.start();

    const cli = try stdx.testing.CliRunner.init(allocator, server.flo_binary, "127.0.0.1:0");
    const data_dir = try std.fmt.allocPrint(allocator, "{s}/second", .{server.data_dir});
    const start = try cli.runAsync(&.{ "server", "start", "--data-dir", data_dir, "--port", "0", "--no-metrics", "--no-dashboard" });
    var pid: i32 = 0;
    while (pid == 0) : (stdx.time.sleep(10 * std.time.ns_per_ms)) pid = start.childPid();

    var buf: [4200]u8 = undefined;
    const line = try std.fmt.bufPrint(&buf, "{d} {d} {s}\n", .{ server.process.?.id, pid, server.data_dir });
    _ = std.c.write(1, line.ptr, line.len);
    _ = std.c.kill(std.c.getpid(), .KILL);
}
