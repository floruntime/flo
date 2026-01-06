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

/// Handle for an async command execution
pub const AsyncCommand = struct {
    const Self = @This();

    thread: std.Thread,
    result: ?CommandResult = null,
    err: ?anyerror = null,
    completed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    child_pid: std.atomic.Value(i32) = std.atomic.Value(i32).init(0),

    // Stored context for thread execution
    allocator: Allocator,
    argv: [][]const u8,
    owned_argv: [][]const u8, // Separately tracked for deallocation

    /// Wait for command to complete and return result
    pub fn wait(self: *Self) !CommandResult {
        self.thread.join();
        if (self.err) |e| return e;
        return self.result orelse error.NoResult;
    }

    /// Wait for command to complete with a timeout. Kills the process if it exceeds the timeout.
    /// Useful for tests involving --follow mode or infinite blocking reads.
    pub fn waitWithTimeout(self: *Self, timeout_ms: u64) !CommandResult {
        const start = std.time.milliTimestamp();
        while (!self.completed.load(.acquire)) {
            const elapsed = @as(u64, @intCast(std.time.milliTimestamp() - start));
            if (elapsed >= timeout_ms) {
                // Kill the child process to unblock the thread
                const pid = self.child_pid.load(.acquire);
                if (pid > 0) {
                    std.posix.kill(@intCast(pid), std.posix.SIG.TERM) catch {};
                    // Give it 500ms to die, then SIGKILL
                    std.Thread.sleep(500 * std.time.ns_per_ms);
                    if (!self.completed.load(.acquire)) {
                        std.posix.kill(@intCast(pid), std.posix.SIG.KILL) catch {};
                    }
                }
                break;
            }
            std.Thread.sleep(50 * std.time.ns_per_ms);
        }
        self.thread.join();
        if (self.err) |e| return e;
        return self.result orelse error.NoResult;
    }

    /// Check if command has completed (non-blocking)
    pub fn isCompleted(self: *Self) bool {
        return self.completed.load(.acquire);
    }

    /// Free resources (call after wait())
    pub fn deinit(self: *Self) void {
        // Free the duplicated argument strings
        for (self.owned_argv) |arg| {
            self.allocator.free(arg);
        }
        self.allocator.free(self.owned_argv);
        self.allocator.free(self.argv);
        // Free the AsyncCommand struct itself
        self.allocator.destroy(self);
    }
};

/// CLI Runner for executing flo commands
pub const CliRunner = struct {
    const Self = @This();

    allocator: Allocator,
    flo_binary: []const u8,
    endpoint: []const u8,

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
    /// Useful for testing blocking operations like `kv get --block`
    pub fn runAsync(self: *Self, args: []const []const u8) !*AsyncCommand {
        // Build full argument list, inserting --endpoint before any "--" separator
        const total_args = 1 + args.len + 2;

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
        if (dashdash_pos) |dd| {
            for (0..dd) |i| {
                argv[1 + i] = owned_argv[i];
            }
            argv[1 + dd] = "--endpoint";
            argv[1 + dd + 1] = self.endpoint;
            for (dd..args.len) |i| {
                argv[1 + dd + 2 + i - dd] = owned_argv[i];
            }
        } else {
            for (args, 0..) |_, i| {
                argv[1 + i] = owned_argv[i];
            }
            argv[total_args - 2] = "--endpoint";
            argv[total_args - 1] = self.endpoint;
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
        var child = std.process.Child.init(async_cmd.argv, allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;

        child.spawn() catch |e| {
            async_cmd.err = e;
            async_cmd.completed.store(true, .release);
            return;
        };

        // Store PID so waitWithTimeout() can kill the process if needed
        async_cmd.child_pid.store(@intCast(child.id), .release);

        // Read stdout and stderr
        var stdout_list: std.ArrayList(u8) = .empty;
        var stderr_list: std.ArrayList(u8) = .empty;
        child.collectOutput(allocator, &stdout_list, &stderr_list, 1024 * 1024) catch |e| {
            async_cmd.err = e;
            async_cmd.completed.store(true, .release);
            return;
        };

        // Wait for process to complete
        const result = child.wait() catch |e| {
            async_cmd.err = e;
            async_cmd.completed.store(true, .release);
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
        async_cmd.completed.store(true, .release);
    }

    /// Run a CLI command (automatically inserts --endpoint before any `--` separator)
    pub fn run(self: *Self, args: []const []const u8) !CommandResult {
        // Build full argument list: [flo_binary] + args_before_-- + [--endpoint, endpoint] + args_from_--
        // We must insert --endpoint BEFORE "--" because commander treats everything
        // after "--" as positional (no flag parsing).
        const total_args = 1 + args.len + 2;

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
            // Insert args before --, then --endpoint, then -- and rest
            for (args[0..dd], 0..) |arg, i| {
                argv[1 + i] = arg;
            }
            argv[1 + dd] = "--endpoint";
            argv[1 + dd + 1] = self.endpoint;
            for (args[dd..], 0..) |arg, i| {
                argv[1 + dd + 2 + i] = arg;
            }
        } else {
            // No -- separator, append at end as before
            for (args, 0..) |arg, i| {
                argv[1 + i] = arg;
            }
            argv[total_args - 2] = "--endpoint";
            argv[total_args - 1] = self.endpoint;
        }

        // Execute command
        var child = std.process.Child.init(argv, self.allocator);
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

        var child = std.process.Child.init(argv, self.allocator);
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
