//! The e2e test runner: Zig's default runner (the build-server protocol, and
//! a plain loop when run by hand), minus fuzzing, plus one thing it lacks:
//! with `FLO_TEST_TIMES` set to a path, each test appends
//! `<milliseconds>\t<test name>\n` there. Shards are balanced on those times;
//! the build server measures them too but shows them only in its web UI.
//! Keep the protocol in step with lib/compiler/test_runner.zig on a Zig bump.
const builtin = @import("builtin");
const std = @import("std");
const Io = std.Io;
const testing = std.testing;

pub const std_options: std.Options = .{
    .logFn = log,
};

var log_err_count: usize = 0;
var stdin_buffer: [4096]u8 = undefined;
var stdout_buffer: [4096]u8 = undefined;
const runner_io: Io = Io.Threaded.global_single_threaded.io();

pub fn main(init: std.process.Init.Minimal) void {
    var listen = false;
    var it = init.args.iterate();
    _ = it.skip();
    while (it.next()) |arg| {
        if (std.mem.eql(u8, arg, "--listen=-")) {
            listen = true;
        } else if (std.mem.startsWith(u8, arg, "--seed=")) {
            testing.random_seed = std.fmt.parseUnsigned(u32, arg["--seed=".len..], 0) catch
                @panic("unable to parse --seed command line argument");
        } else if (!std.mem.startsWith(u8, arg, "--cache-dir")) {
            std.debug.panic("unrecognized command line argument: {s}", .{arg});
        }
    }
    if (listen) {
        mainServer(init) catch |err| std.debug.panic("internal test runner failure: {t}", .{err});
    } else {
        mainTerminal(init);
    }
}

const Status = std.zig.Server.Message.TestResults.Status;

fn runTest(init: std.process.Init.Minimal, test_fn: std.builtin.TestFn) struct { status: Status, leaks: usize } {
    testing.environ = init.environ;
    testing.allocator_instance = .init(std.heap.page_allocator, .{
        .canary = 0xc3a701ba,
        .check_write_after_free = true,
    });
    testing.io_instance = .init(testing.allocator, .{
        .argv0 = .init(init.args),
        .environ = init.environ,
    });
    log_err_count = 0;
    const started = Io.Clock.awake.now(runner_io);
    const status: Status = if (test_fn.func()) |_| .pass else |err| switch (err) {
        error.SkipZigTest => .skip,
        else => s: {
            if (@errorReturnTrace()) |trace| std.debug.dumpErrorReturnTrace(trace);
            break :s .fail;
        },
    };
    const elapsed = started.durationTo(Io.Clock.awake.now(runner_io));
    recordTime(test_fn.name, @intCast(@divTrunc(elapsed.nanoseconds, std.time.ns_per_ms)));
    testing.io_instance.deinit();
    return .{ .status = status, .leaks = testing.allocator_instance.deinit() };
}

/// One O_APPEND write per test, so a runner the build restarts after a crash
/// or a timeout keeps the lines its predecessor wrote.
fn recordTime(name: []const u8, ms: u64) void {
    const path = std.c.getenv("FLO_TEST_TIMES") orelse return;
    const fd = std.c.open(path, .{ .ACCMODE = .WRONLY, .CREAT = true, .APPEND = true }, @as(std.c.mode_t, 0o644));
    if (fd < 0) return;
    defer _ = std.c.close(fd);
    var line: [1024]u8 = undefined;
    const text = std.fmt.bufPrint(&line, "{d}\t{s}\n", .{ ms, name }) catch return;
    _ = std.c.write(fd, text.ptr, text.len);
}

fn mainServer(init: std.process.Init.Minimal) !void {
    var stdin_reader: Io.File.Reader = .initStreaming(.stdin(), runner_io, &stdin_buffer);
    var stdout_writer: Io.File.Writer = .initStreaming(.stdout(), runner_io, &stdout_buffer);
    var server: std.zig.Server = .{
        .in = &stdin_reader.interface,
        .out = &stdout_writer.interface,
    };
    try server.serveStringMessage(.zig_version, builtin.zig_version_string);

    while (true) {
        const hdr = try server.receiveMessage();
        switch (hdr.tag) {
            .exit => return std.process.exit(0),
            .query_test_metadata => {
                const gpa = std.heap.page_allocator;
                var string_bytes: std.ArrayList(u8) = .empty;
                defer string_bytes.deinit(gpa);
                try string_bytes.append(gpa, 0); // 0 is the empty name.
                const test_fns = builtin.test_functions;
                const names = try gpa.alloc(u32, test_fns.len);
                defer gpa.free(names);
                const expected_panic_msgs = try gpa.alloc(u32, test_fns.len);
                defer gpa.free(expected_panic_msgs);
                for (test_fns, names, expected_panic_msgs) |test_fn, *name, *expected_panic_msg| {
                    name.* = @intCast(string_bytes.items.len);
                    try string_bytes.appendSlice(gpa, test_fn.name);
                    try string_bytes.append(gpa, 0);
                    expected_panic_msg.* = 0;
                }
                try server.serveTestMetadata(.{
                    .names = names,
                    .expected_panic_msgs = expected_panic_msgs,
                    .string_bytes = string_bytes.items,
                });
            },
            .run_test => {
                const index = try server.receiveBody_u32();
                try server.serveStringMessage(.test_started, &.{});
                const result = runTest(init, builtin.test_functions[index]);
                try server.serveTestResults(.{
                    .index = index,
                    .flags = .{
                        .status = result.status,
                        .fuzz = false,
                        .log_err_count = std.math.lossyCast(
                            @FieldType(std.zig.Server.Message.TestResults.Flags, "log_err_count"),
                            log_err_count,
                        ),
                        .leak_count = std.math.lossyCast(
                            @FieldType(std.zig.Server.Message.TestResults.Flags, "leak_count"),
                            result.leaks,
                        ),
                    },
                });
            },
            else => {
                std.debug.print("unsupported message: {x}\n", .{@backingInt(hdr.tag)});
                std.process.exit(1);
            },
        }
    }
}

fn mainTerminal(init: std.process.Init.Minimal) void {
    var ok: usize = 0;
    var skipped: usize = 0;
    var failed: usize = 0;
    var leaked: usize = 0;
    var logged: usize = 0;
    const tests = builtin.test_functions;
    for (tests, 1..) |test_fn, i| {
        std.debug.print("{d}/{d} {s}...", .{ i, tests.len, test_fn.name });
        const result = runTest(init, test_fn);
        if (result.leaks != 0) leaked += 1;
        logged += log_err_count;
        switch (result.status) {
            .pass => ok += 1,
            .skip => skipped += 1,
            .fail => failed += 1,
        }
        std.debug.print("{s}\n", .{switch (result.status) {
            .pass => "OK",
            .skip => "SKIP",
            .fail => "FAIL",
        }});
    }
    std.debug.print("{d} passed; {d} skipped; {d} failed.\n", .{ ok, skipped, failed });
    if (failed != 0 or leaked != 0 or logged != 0) std.process.exit(1);
}

pub fn log(
    comptime level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: []const u8,
    args: anytype,
) void {
    if (@backingInt(level) <= @backingInt(std.log.Level.err)) log_err_count +|= 1;
    if (@backingInt(level) <= @backingInt(testing.log_level)) {
        std.debug.print("[" ++ @tagName(scope) ++ "] (" ++ @tagName(level) ++ "): " ++ format ++ "\n", args);
    }
}
