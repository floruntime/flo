//! The harness itself: a server that never becomes ready fails its test
//! within a bound and leaves nothing running.

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "harness: a server that never becomes ready fails within a bound" {
    var server = try stdx.testing.ServerProcess.init(testing.allocator);
    defer server.deinit();
    server.dump_log_on_failure = false;

    // Stands in for flo: never listens, and backgrounds a process that holds
    // its output pipe open after the parent is gone.
    const pid_path = try std.fmt.allocPrint(testing.allocator, "{s}/grandchild.pid", .{server.data_dir});
    defer testing.allocator.free(pid_path);
    const script = try std.fmt.allocPrint(testing.allocator, "#!/bin/sh\nsleep 600 &\necho $! > {s}\nexec sleep 600\n", .{pid_path});
    defer testing.allocator.free(script);
    const fake = try std.fmt.allocPrintSentinel(testing.allocator, "{s}/fake-flo", .{server.data_dir}, 0);
    defer testing.allocator.free(fake);
    {
        const f = try stdx.fs.createFileAbsolute(fake, .{});
        defer stdx.fs.closeFile(f);
        try stdx.fs.writeAll(f, script);
    }
    try testing.expectEqual(@as(c_int, 0), std.c.chmod(fake, 0o755));
    testing.allocator.free(server.flo_binary);
    server.flo_binary = try testing.allocator.dupe(u8, fake);

    const t0 = stdx.time.milliTimestamp();
    try testing.expectError(error.ServerNotReady, server.startWithTimeout(500));
    try testing.expect(stdx.time.milliTimestamp() - t0 < 5_000);

    // The backgrounded process went with its group.
    const pid_text = try stdx.fs.readFileAlloc(testing.allocator, pid_path, 64);
    defer testing.allocator.free(pid_text);
    const pid = try std.fmt.parseInt(std.c.pid_t, std.mem.trim(u8, pid_text, " \n"), 10);
    var gone = false;
    for (0..100) |_| {
        if (std.c.kill(pid, @enumFromInt(0)) != 0) {
            gone = true;
            break;
        }
        stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    if (!gone) _ = std.c.kill(pid, .KILL);
    try testing.expect(gone);
}
