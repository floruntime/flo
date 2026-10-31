//! E2E Test CLI Runner
//!
//! Executes Flo CLI commands and captures results for e2e tests.
//! Intentionally minimal - just runs commands and returns results.
//!
//! ## Usage
//! Typically accessed via TestContext.cli:
//! ```zig
//! const stdx = @import("stdx");
//!
//! var ctx = try stdx.testing.TestContext.init(allocator);
//! defer ctx.deinit();
//!
//! var result = try ctx.cli.run(&.{"kv", "set", "key", "value"});
//! defer result.deinit();
//! try std.testing.expect(result.succeeded());
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const time = @import("../../time.zig");
const Mutex = @import("../../sync.zig").Mutex;
const Watchdog = @import("watchdog.zig").Watchdog;
const Process = @import("../../process.zig");
const awaitExit = @import("watchdog.zig").awaitExit;

/// Result of a CLI command execution
pub const CommandResult = struct {
    allocator: Allocator,
    exit_code: u8,
    stdout: []const u8,
    stderr: []const u8,

    const Self = @This();

    /// Check if command succeeded (exit code 0)
    pub fn succeeded(self: Self) bool {
        return self.exit_code == 0;
    }

    /// Check if stdout contains a string
    pub fn stdoutContains(self: Self, needle: []const u8) bool {
        return std.mem.indexOf(u8, self.stdout, needle) != null;
    }

    /// Check if stderr contains a string
    pub fn stderrContains(self: Self, needle: []const u8) bool {
        return std.mem.indexOf(u8, self.stderr, needle) != null;
    }

    /// Check if stdout or stderr contains a string
    pub fn contains(self: Self, needle: []const u8) bool {
        return self.stdoutContains(needle) or self.stderrContains(needle);
    }

    /// Count occurrences of a string in stdout
    pub fn stdoutCount(self: Self, needle: []const u8) usize {
        return countOccurrences(self.stdout, needle);
    }

    /// Count occurrences of a string in stderr
    pub fn stderrCount(self: Self, needle: []const u8) usize {
        return countOccurrences(self.stderr, needle);
    }

    /// Count occurrences of a string in stdout or stderr combined
    pub fn count(self: Self, needle: []const u8) usize {
        return self.stdoutCount(needle) + self.stderrCount(needle);
    }

    /// Get trimmed stdout
    pub fn stdoutTrimmed(self: Self) []const u8 {
        return std.mem.trim(u8, self.stdout, &std.ascii.whitespace);
    }

    /// Free allocated memory
    pub fn deinit(self: *Self) void {
        if (self.stdout.len > 0) {
            self.allocator.free(self.stdout);
        }
        if (self.stderr.len > 0) {
            self.allocator.free(self.stderr);
        }
    }
};

/// Handle for an async command execution. `deinit` stops the command if
/// it is still running and joins its thread, so a test that returns early
/// (a failed `try` before the wait) doesn't leave the thread writing into
/// freed memory, or the command running past its test.
pub const AsyncCommand = struct {
    const Self = @This();

    thread: std.Thread,
    joined: bool = false,
    result: ?CommandResult = null,
    /// A wait handed `result` to the caller, who frees it.
    result_taken: bool = false,
    err: ?anyerror = null,
    completed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// The child's pid while it may be signalled: set after spawn, cleared
    /// before the thread reaps it (after which the pid can name another
    /// process). Read and written only under `pid_lock`.
    child_pid: i32 = 0,
    pid_lock: Mutex = .{},

    // Stored context for thread execution
    allocator: Allocator,
    argv: [][]const u8,
    owned_argv: [][]const u8, // Separately tracked for deallocation

    /// Wait for command to complete and return result
    pub fn wait(self: *Self) !CommandResult {
        self.join();
        return self.take();
    }

    /// Wait for command to complete with a timeout. Kills the process if it exceeds the timeout.
    /// Useful for tests involving --follow mode or infinite blocking reads.
    pub fn waitWithTimeout(self: *Self, timeout_ms: u64) !CommandResult {
        const start = time.monotonicMs();
        while (!self.completed.load(.acquire)) {
            if (time.monotonicMs() - start >= timeout_ms) {
                self.stop();
                break;
            }
            time.sleep(50 * std.time.ns_per_ms);
        }
        self.join();
        return self.take();
    }

    /// Check if command has completed (non-blocking)
    pub fn isCompleted(self: *Self) bool {
        return self.completed.load(.acquire);
    }

    /// Stop the command if it's still running, join its thread, and free
    /// everything, including a result no wait handed out.
    pub fn deinit(self: *Self) void {
        if (!self.joined) {
            self.stop();
            self.join();
        }
        if (self.result) |*r| if (!self.result_taken) r.deinit();
        for (self.owned_argv) |arg| {
            self.allocator.free(arg);
        }
        self.allocator.free(self.owned_argv);
        self.allocator.free(self.argv);
        self.allocator.destroy(self);
    }

    /// TERM until the command completes, KILL after a second. The pid is
    /// re-read each time, so a child spawned after this starts is caught.
    fn stop(self: *Self) void {
        const start = time.monotonicMs();
        while (!self.completed.load(.acquire)) {
            self.signal(if (time.monotonicMs() - start < 1_000) .TERM else .KILL);
            time.sleep(50 * std.time.ns_per_ms);
        }
    }

    fn signal(self: *Self, sig: std.c.SIG) void {
        self.pid_lock.lock();
        defer self.pid_lock.unlock();
        if (self.child_pid > 0) _ = std.c.kill(self.child_pid, sig);
    }

    /// The running child's pid, or 0 before spawn and once it's reaped.
    pub fn childPid(self: *Self) i32 {
        self.pid_lock.lock();
        defer self.pid_lock.unlock();
        return self.child_pid;
    }

    /// Polls for the exit, reaping under `pid_lock`, so the pid stays
    /// signalable until the moment it is reaped and never after.
    fn reapSignalable(self: *Self, child: *Process.Child) !Process.Term {
        while (true) {
            var status: c_int = 0;
            self.pid_lock.lock();
            const r = std.c.waitpid(child.id, &status, std.posix.W.NOHANG);
            if (r == child.id) self.child_pid = 0;
            self.pid_lock.unlock();
            if (r == child.id) {
                // What child.wait() would have closed.
                if (child.stdout) |f| _ = std.c.close(f.handle);
                if (child.stderr) |f| _ = std.c.close(f.handle);
                child.stdout = null;
                child.stderr = null;
                const st: u32 = @bitCast(status);
                if (std.c.W.IFEXITED(st)) return .{ .Exited = std.c.W.EXITSTATUS(st) };
                if (std.c.W.IFSIGNALED(st)) return .{ .Signal = @backingInt(std.c.W.TERMSIG(st)) };
                return .{ .Unknown = st };
            }
            if (r < 0 and std.c._errno().* != @backingInt(std.c.E.INTR)) return error.WaitFailed;
            time.sleep(50 * std.time.ns_per_ms);
        }
    }

    fn setPid(self: *Self, pid: i32) void {
        self.pid_lock.lock();
        defer self.pid_lock.unlock();
        self.child_pid = pid;
    }

    fn join(self: *Self) void {
        if (self.joined) return;
        self.thread.join();
        self.joined = true;
    }

    fn take(self: *Self) !CommandResult {
        if (self.err) |e| return e;
        const r = self.result orelse return error.NoResult;
        self.result_taken = true;
        return r;
    }
};

/// CLI Runner for executing flo commands
pub const CliRunner = struct {
    const Self = @This();

    allocator: Allocator,
    flo_binary: []const u8,
    endpoint: []const u8,
    /// API key injected as --api-key flag on every command (set when auth is enabled)
    /// Initialize CLI runner
    pub fn init(allocator: Allocator, flo_binary: []const u8, endpoint: []const u8) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .flo_binary = try allocator.dupe(u8, flo_binary),
            .endpoint = try allocator.dupe(u8, endpoint),
        };
        return self;
    }

    /// Clean up resources
    pub fn deinit(self: *Self) void {
        self.allocator.free(self.flo_binary);
        self.allocator.free(self.endpoint);
        self.allocator.destroy(self);
    }

    /// Run a CLI command asynchronously (returns handle to wait on)
    /// Useful for testing blocking operations like `kv get --wait-ms`
    pub fn runAsync(self: *Self, args: []const []const u8) !*AsyncCommand {
        // Build full argument list, inserting --endpoint before any "--"
        // separator; a command that refuses it gets none.
        const extra_flags: usize = if (takesNoEndpoint(args)) 0 else 2;
        const total_args = 1 + args.len + extra_flags;

        var argv = try self.allocator.alloc([]const u8, total_args);
        var owned_argv = try self.allocator.alloc([]const u8, args.len);

        // Find position of "--" in args (if any)
        var dashdash_pos: ?usize = null;
        for (args, 0..) |arg, i| {
            if (std.mem.eql(u8, arg, "--")) {
                dashdash_pos = i;
                break;
            }
        }

        // Dupe all args first
        for (args, 0..) |arg, i| {
            owned_argv[i] = try self.allocator.dupe(u8, arg);
        }

        argv[0] = self.flo_binary;
        if (extra_flags == 0) {
            for (args, 0..) |_, i| argv[1 + i] = owned_argv[i];
        } else if (dashdash_pos) |dd| {
            for (0..dd) |i| argv[1 + i] = owned_argv[i];
            var inject_at: usize = 1 + dd;
            argv[inject_at] = "--endpoint";
            argv[inject_at + 1] = self.endpoint;
            inject_at += 2;
            for (dd..args.len) |i| argv[inject_at + (i - dd)] = owned_argv[i];
        } else {
            for (args, 0..) |_, i| argv[1 + i] = owned_argv[i];
            var inject_at: usize = 1 + args.len;
            argv[inject_at] = "--endpoint";
            argv[inject_at + 1] = self.endpoint;
            inject_at += 2;
        }

        const async_cmd = try self.allocator.create(AsyncCommand);
        async_cmd.* = .{
            .thread = undefined,
            .allocator = self.allocator,
            .argv = argv,
            .owned_argv = owned_argv,
        };

        // Spawn thread to run command
        async_cmd.thread = try std.Thread.spawn(.{}, runCommandThread, .{ async_cmd, self.allocator });

        return async_cmd;
    }

    /// Thread function to execute command
    fn runCommandThread(async_cmd: *AsyncCommand, allocator: Allocator) void {
        var child = @import("../../process.zig").Child.init(async_cmd.argv, allocator);
        defer async_cmd.completed.store(true, .release);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;
        // Its own group, for the watchdog to kill without the test's.
        child.pgid = 0;

        child.spawn() catch |e| {
            async_cmd.err = e;
            return;
        };
        const watchdog = Watchdog.start(allocator, child.id) catch |e| {
            _ = std.c.kill(child.id, .KILL);
            _ = child.wait() catch {};
            async_cmd.err = e;
            return;
        };
        async_cmd.setPid(@intCast(child.id));

        // Read stdout and stderr
        var stdout_list: std.ArrayList(u8) = .empty;
        var stderr_list: std.ArrayList(u8) = .empty;
        const collected = child.collectOutput(allocator, &stdout_list, &stderr_list, 1024 * 1024);
        if (collected) |_| {} else |_| {
            // Still ours to signal: it isn't reaped until the wait below.
            async_cmd.signal(.KILL);
            stdout_list.deinit(allocator);
            stderr_list.deinit(allocator);
        }

        // A child can close its pipes and keep running, so it stays
        // signalable until it has exited. Then, while it is still unreaped,
        // the pid stops being signalled and the watchdog stands down: once
        // reaped, the id may name another process. If the exit can't be
        // checked, it is reaped by polling under the pid lock instead.
        const exited = awaitExit(child.id, true) catch false;
        if (exited) async_cmd.setPid(0);
        watchdog.release();
        const waited = if (exited) child.wait() else async_cmd.reapSignalable(&child);
        const result = waited catch |e| {
            if (collected) |_| {
                stdout_list.deinit(allocator);
                stderr_list.deinit(allocator);
            } else |_| {}
            async_cmd.err = e;
            return;
        };
        collected catch |e| {
            async_cmd.err = e;
            return;
        };

        const exit_code: u8 = switch (result) {
            .Exited => |code| code,
            .Signal => 255,
            .Stopped => 254,
            .Unknown => 253,
        };

        async_cmd.result = CommandResult{
            .allocator = allocator,
            .exit_code = exit_code,
            .stdout = stdout_list.toOwnedSlice(allocator) catch "",
            .stderr = stderr_list.toOwnedSlice(allocator) catch "",
        };
    }

    /// Run a CLI command (automatically inserts --endpoint and --api-key before any `--` separator)
    pub fn run(self: *Self, args: []const []const u8) !CommandResult {
        // Build full argument list: [flo_binary] + args_before_-- + [--endpoint, endpoint, (--api-key, key)] + args_from_--
        // We must insert flags BEFORE "--" because commander treats everything
        // after "--" as positional (no flag parsing).
        // A command that refuses -e/--endpoint gets none.
        if (takesNoEndpoint(args)) return self.runRaw(args);
        const extra_flags: usize = 2;
        const total_args = 1 + args.len + extra_flags;

        var argv = try self.allocator.alloc([]const u8, total_args);
        defer self.allocator.free(argv);

        // Find position of "--" in args (if any)
        var dashdash_pos: ?usize = null;
        for (args, 0..) |arg, i| {
            if (std.mem.eql(u8, arg, "--")) {
                dashdash_pos = i;
                break;
            }
        }

        argv[0] = self.flo_binary;
        if (dashdash_pos) |dd| {
            // Insert args before --, then flags, then -- and rest
            for (args[0..dd], 0..) |arg, i| argv[1 + i] = arg;
            var inject_at: usize = 1 + dd;
            argv[inject_at] = "--endpoint";
            argv[inject_at + 1] = self.endpoint;
            inject_at += 2;
            for (args[dd..], 0..) |arg, i| argv[inject_at + i] = arg;
        } else {
            // No -- separator, append at end
            for (args, 0..) |arg, i| argv[1 + i] = arg;
            var inject_at: usize = 1 + args.len;
            argv[inject_at] = "--endpoint";
            argv[inject_at + 1] = self.endpoint;
            inject_at += 2;
        }

        // Execute command
        var child = @import("../../process.zig").Child.init(argv, self.allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;

        try child.spawn();

        // Read stdout and stderr
        var stdout_list: std.ArrayList(u8) = .empty;
        var stderr_list: std.ArrayList(u8) = .empty;
        try child.collectOutput(self.allocator, &stdout_list, &stderr_list, 1024 * 1024);

        // Wait for process to complete
        const result = try child.wait();

        const exit_code: u8 = switch (result) {
            .Exited => |code| code,
            .Signal => 255,
            .Stopped => 254,
            .Unknown => 253,
        };

        return CommandResult{
            .allocator = self.allocator,
            .exit_code = exit_code,
            .stdout = try stdout_list.toOwnedSlice(self.allocator),
            .stderr = try stderr_list.toOwnedSlice(self.allocator),
        };
    }

    /// Run a CLI command without appending --endpoint
    pub fn runRaw(self: *Self, args: []const []const u8) !CommandResult {
        const total_args = 1 + args.len;

        var argv = try self.allocator.alloc([]const u8, total_args);
        defer self.allocator.free(argv);

        argv[0] = self.flo_binary;
        for (args, 0..) |arg, i| {
            argv[1 + i] = arg;
        }

        var child = @import("../../process.zig").Child.init(argv, self.allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;

        try child.spawn();

        var stdout_list: std.ArrayList(u8) = .empty;
        var stderr_list: std.ArrayList(u8) = .empty;
        try child.collectOutput(self.allocator, &stdout_list, &stderr_list, 1024 * 1024);

        const result = try child.wait();

        const exit_code: u8 = switch (result) {
            .Exited => |code| code,
            .Signal => 255,
            .Stopped => 254,
            .Unknown => 253,
        };

        return CommandResult{
            .allocator = self.allocator,
            .exit_code = exit_code,
            .stdout = try stdout_list.toOwnedSlice(self.allocator),
            .stderr = try stderr_list.toOwnedSlice(self.allocator),
        };
    }
};

/// Count non-overlapping occurrences of needle in haystack
fn countOccurrences(haystack: []const u8, needle: []const u8) usize {
    if (needle.len == 0 or haystack.len < needle.len) return 0;

    var count: usize = 0;
    var pos: usize = 0;

    while (pos <= haystack.len - needle.len) {
        if (std.mem.eql(u8, haystack[pos..][0..needle.len], needle)) {
            count += 1;
            pos += needle.len; // Skip past this occurrence (non-overlapping)
        } else {
            pos += 1;
        }
    }
    return count;
}

// =============================================================================
// Tests
// =============================================================================

test "CommandResult: contains checks" {
    var result = CommandResult{
        .allocator = testing.allocator,
        .exit_code = 0,
        .stdout = "hello world",
        .stderr = "some error",
    };

    try testing.expect(result.succeeded());
    try testing.expect(result.stdoutContains("hello"));
    try testing.expect(result.stderrContains("error"));
    try testing.expect(result.contains("hello"));
    try testing.expect(result.contains("error"));
    try testing.expect(!result.contains("missing"));

    // Reset to prevent deinit from freeing string literals
    result.stdout = "";
    result.stderr = "";
}

test "CommandResult: count occurrences" {
    var result = CommandResult{
        .allocator = testing.allocator,
        .exit_code = 0,
        .stdout = "hot hot hot warm warm cold",
        .stderr = "hot warm",
    };

    // Count in stdout only
    try testing.expectEqual(@as(usize, 3), result.stdoutCount("hot"));
    try testing.expectEqual(@as(usize, 2), result.stdoutCount("warm"));
    try testing.expectEqual(@as(usize, 1), result.stdoutCount("cold"));

    // Count in stderr only
    try testing.expectEqual(@as(usize, 1), result.stderrCount("hot"));
    try testing.expectEqual(@as(usize, 1), result.stderrCount("warm"));

    // Combined count
    try testing.expectEqual(@as(usize, 4), result.count("hot"));
    try testing.expectEqual(@as(usize, 3), result.count("warm"));
    try testing.expectEqual(@as(usize, 1), result.count("cold"));
    try testing.expectEqual(@as(usize, 0), result.count("missing"));

    // Reset to prevent deinit from freeing string literals
    result.stdout = "";
    result.stderr = "";
}

/// Whether `args` run a command that refuses -e/--endpoint: `server
/// start`, which listens, and `server metrics`, which reads the metrics
/// endpoint, not the client port.
fn takesNoEndpoint(args: []const []const u8) bool {
    if (args.len < 2 or !std.mem.eql(u8, args[0], "server")) return false;
    return std.mem.eql(u8, args[1], "start") or std.mem.eql(u8, args[1], "metrics");
}
