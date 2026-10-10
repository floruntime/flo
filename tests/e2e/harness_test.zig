//! The harness itself: a server process starts out stopped, and one that
//! never becomes ready fails its test within a bound and leaves nothing
//! running. (These need the built flo binary, so they live here, not in
//! stdx's own tests.)

const std = @import("std");
const testing = std.testing;
const stdx = @import("stdx");

test "harness: a new server process is stopped and has a data dir" {
    var server = try stdx.testing.ServerProcess.init(testing.allocator);
    defer server.deinit();

    try testing.expect(!server.isRunning());
    try testing.expect(server.data_dir.len > 0);
}

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
        if (processGone(pid)) {
            gone = true;
            break;
        }
        stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    if (!gone) _ = std.c.kill(pid, .KILL);
    try testing.expect(gone);
}

test "harness: only an exit the harness caused is normal" {
    var server = try stdx.testing.ServerProcess.init(testing.allocator);
    defer server.deinit();
    var buf: [160]u8 = undefined;
    const sig = struct {
        fn n(s: std.posix.SIG) c_int {
            return @intCast(@intFromEnum(s));
        }
    };

    const cases = [_]struct { status: c_int, term: bool, kill: bool, why: ?[]const u8 }{
        .{ .status = 0, .term = true, .kill = false, .why = null },
        .{ .status = 3 << 8, .term = true, .kill = false, .why = "exited with code 3" },
        .{ .status = sig.n(.TERM), .term = true, .kill = false, .why = null },
        .{ .status = sig.n(.KILL), .term = true, .kill = true, .why = null },
        .{ .status = sig.n(.KILL), .term = false, .kill = false, .why = "killed by a SIGKILL the harness didn't send (memory pressure, or another run's cleanup?)" },
        .{ .status = sig.n(.ABRT), .term = true, .kill = false, .why = null },
        .{ .status = sig.n(.SEGV), .term = true, .kill = true, .why = null },
    };
    for (cases) |c| {
        server.exit_status = c.status;
        server.sent_term = c.term;
        server.sent_kill = c.kill;
        const got = server.abnormalExit(&buf);
        if (c.why) |want| {
            try testing.expectEqualStrings(want, got.?);
        } else if (c.status == sig.n(.ABRT) or c.status == sig.n(.SEGV)) {
            var want_buf: [32]u8 = undefined;
            try testing.expectEqualStrings(try std.fmt.bufPrint(&want_buf, "died of signal {d}", .{c.status}), got.?);
        } else {
            try testing.expectEqual(@as(?[]const u8, null), got);
        }
    }
    server.exit_status = null;
}

/// Whether `pid` no longer runs. A killed process stays a zombie until its
/// parent reaps it, and an orphan's parent is PID 1: in a container where
/// PID 1 is the build runner rather than an init, nothing reaps it, so
/// `kill(pid, 0)` keeps succeeding. On Linux a zombie (or dead) state in
/// /proc counts as gone.
fn processGone(pid: std.c.pid_t) bool {
    if (std.c.kill(pid, @enumFromInt(0)) != 0) return true;
    if (@import("builtin").os.tag != .linux) return false;
    var path_buf: [32]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "/proc/{d}/stat", .{pid}) catch return false;
    var stat_buf: [512]u8 = undefined;
    const stat = stdx.fs.readFile(path, &stat_buf) catch return true;
    // "pid (comm) S ...": the state follows the last ')', since comm may hold one.
    const close = std.mem.lastIndexOfScalar(u8, stat, ')') orelse return false;
    if (close + 2 >= stat.len) return false;
    return stat[close + 2] == 'Z' or stat[close + 2] == 'X';
}
