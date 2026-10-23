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

test "harness: a server spawned and never awaited is stopped when its process is freed" {
    var server = try stdx.testing.ServerProcess.init(testing.allocator);
    try server.spawn();
    const pid = server.process.?.id;
    server.deinit();
    try testing.expect(processGone(pid));
}

test "harness: only an exit the harness caused is normal" {
    var server = try stdx.testing.ServerProcess.init(testing.allocator);
    defer server.deinit();
    var buf: [160]u8 = undefined;
    const sig = struct {
        fn n(s: std.posix.SIG) c_int {
            return @intCast(@backingInt(s));
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
    if (std.c.kill(pid, @fromBackingInt(@intCast(0))) != 0) return true;
    if (@import("builtin").target.os.tag != .linux) return false;
    var path_buf: [32]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "/proc/{d}/stat", .{pid}) catch return false;
    var stat_buf: [512]u8 = undefined;
    const stat = stdx.fs.readFile(path, &stat_buf) catch return true;
    // "pid (comm) S ...": the state follows the last ')', since comm may hold one.
    const close = std.mem.lastIndexOfScalar(u8, stat, ')') orelse return false;
    if (close + 2 >= stat.len) return false;
    return stat[close + 2] == 'Z' or stat[close + 2] == 'X';
}

test "harness: a second e2e run in the same checkout is refused" {
    // This process holds the checkout's run lock from its first server start.
    var server = try stdx.testing.ServerProcess.init(testing.allocator);
    defer server.deinit();
    try server.start();
    server.stop();

    // An flock belongs to an open file, not a process, so a second open here
    // stands in for another run of the same checkout.
    const checkout = stdx.testing.checkoutOf(server.flo_binary).?;
    try testing.expectError(error.E2eRunInProgress, stdx.testing.takeRunLock(checkout));
}

test "harness: an async command freed before it is awaited is stopped, and its thread joined" {
    var ctx = try stdx.testing.TestContext.init(testing.allocator);
    defer ctx.deinit();

    // As in a test whose `try` fails between the start and the wait: the
    // command would block for 300 s.
    const cmd = try ctx.cli.runAsync(&.{ "kv", "get", "harness_never_set", "--wait-ms", "300000" });
    var pid: i32 = 0;
    for (0..200) |_| {
        pid = cmd.childPid();
        if (pid > 0) break;
        stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    try testing.expect(pid > 0);
    stdx.time.sleep(200 * std.time.ns_per_ms);

    const t0 = stdx.time.monotonicMs();
    cmd.deinit();
    try testing.expect(stdx.time.monotonicMs() - t0 < 3_000);
    // Reaped by the joined thread, so nothing of it is left.
    try testing.expect(processGone(pid));

    // Freed straight away, before its thread may have spawned the child.
    const t1 = stdx.time.monotonicMs();
    (try ctx.cli.runAsync(&.{ "kv", "get", "harness_never_set", "--wait-ms", "300000" })).deinit();
    try testing.expect(stdx.time.monotonicMs() - t1 < 3_000);
}

test "harness: a test process that dies without cleanup takes its servers with it" {
    // e2e-orphan, installed beside flo, starts a server and an async
    // `server start`, prints both pids and SIGKILLs itself: no deinit, no
    // stop, as when the runner kills a test at its time limit.
    const flo = try stdx.testing.findFloBinary(testing.allocator);
    defer testing.allocator.free(flo);
    const helper = try std.fmt.allocPrint(testing.allocator, "{s}/e2e-orphan", .{std.fs.path.dirname(flo).?});
    defer testing.allocator.free(helper);

    var child = stdx.process.Child.init(&.{helper}, testing.allocator);
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;
    try child.spawn();
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(testing.allocator);
    var err_out: std.ArrayList(u8) = .empty;
    defer err_out.deinit(testing.allocator);
    try child.collectOutput(testing.allocator, &out, &err_out, 64 * 1024);
    const term = try child.wait();
    if (term != .Signal or term.Signal != 9) std.debug.print("e2e-orphan: {any}\n{s}\n", .{ term, err_out.items });
    try testing.expect(term == .Signal and term.Signal == 9);

    var fields = std.mem.tokenizeAny(u8, out.items, " \n");
    var n: usize = 0;
    var left: usize = 0;
    while (n < 2) : (n += 1) {
        const pid = try std.fmt.parseInt(std.c.pid_t, fields.next() orelse break, 10);
        var gone = false;
        for (0..200) |_| {
            if (processGone(pid)) {
                gone = true;
                break;
            }
            stdx.time.sleep(10 * std.time.ns_per_ms);
        }
        // Killed here so a failing run leaves nothing behind either.
        if (!gone) {
            left += 1;
            _ = std.c.kill(pid, .KILL);
        }
    }
    // Both servers' data, left behind by the helper's death.
    if (fields.next()) |data_dir| try stdx.fs.deleteTree(data_dir);
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqual(@as(usize, 0), left);
}
