//! stdx.process — Compatibility shim around the new `std.process` API in Zig 0.16.
//!
//! Presents the old `Child.init/spawn/collectOutput/wait/kill` interface used by
//! Flo's e2e test infrastructure, while internally calling the new
//! `std.process.spawn(io, options)` and `child.{kill,wait}(io)` functions.

const std = @import("std");
const builtin = @import("builtin");
const io_mod = @import("io.zig");

const Allocator = std.mem.Allocator;
const NativeChild = std.process.Child;
const SpawnOptions = std.process.SpawnOptions;
const StdIo = SpawnOptions.StdIo;

fn io() std.Io {
    return io_mod.instance();
}

pub const StdIoBehavior = enum { Inherit, Ignore, Pipe, Close };

pub const Term = union(enum) {
    Exited: u8,
    Signal: u32,
    Stopped: u32,
    Unknown: u32,
};

pub const Child = struct {
    allocator: Allocator,
    argv: []const []const u8,
    stdin_behavior: StdIoBehavior = .Inherit,
    stdout_behavior: StdIoBehavior = .Inherit,
    stderr_behavior: StdIoBehavior = .Inherit,
    pgid: ?std.c.pid_t = null,

    /// Set after `spawn()` succeeds.
    inner: ?NativeChild = null,

    /// Process id (POSIX) — valid between `spawn()` and `wait()`/`kill()`.
    id: std.c.pid_t = 0,

    /// Available after spawn when `stdout_behavior == .Pipe`.
    stdout: ?std.Io.File = null,
    /// Available after spawn when `stderr_behavior == .Pipe`.
    stderr: ?std.Io.File = null,
    /// Available after spawn when `stdin_behavior == .Pipe`.
    stdin: ?std.Io.File = null,

    pub fn init(argv: []const []const u8, allocator: Allocator) Child {
        return .{ .allocator = allocator, .argv = argv };
    }

    fn toStdIo(b: StdIoBehavior) StdIo {
        return switch (b) {
            .Inherit => .inherit,
            .Ignore => .ignore,
            .Pipe => .pipe,
            .Close => .close,
        };
    }

    pub fn spawn(self: *Child) !void {
        const opts = SpawnOptions{
            .argv = self.argv,
            .stdin = toStdIo(self.stdin_behavior),
            .stdout = toStdIo(self.stdout_behavior),
            .stderr = toStdIo(self.stderr_behavior),
            .pgid = self.pgid,
        };
        var child = try std.process.spawn(io(), opts);
        _ = &child;
        self.inner = child;
        self.id = child.id orelse 0;
        self.stdin = child.stdin;
        self.stdout = child.stdout;
        self.stderr = child.stderr;
    }

    pub fn wait(self: *Child) !Term {
        if (self.inner == null) return error.NotSpawned;
        const term = try self.inner.?.wait(io());
        // Streams are closed by wait — clear our copies
        self.stdin = null;
        self.stdout = null;
        self.stderr = null;
        self.inner = null;
        return switch (term) {
            .exited => |c| .{ .Exited = c },
            .signal => |s| .{ .Signal = @intCast(@intFromEnum(s)) },
            .stopped => |s| .{ .Stopped = @intCast(@intFromEnum(s)) },
            .unknown => |c| .{ .Unknown = c },
        };
    }

    pub fn kill(self: *Child) !Term {
        if (self.inner) |*c| {
            c.kill(io());
        }
        self.stdin = null;
        self.stdout = null;
        self.stderr = null;
        self.inner = null;
        return .{ .Signal = 9 };
    }

    /// Read stdout and stderr into the provided lists, blocking until EOF on
    /// both pipes. The caller passes the same allocator as `init`.
    pub fn collectOutput(
        self: *Child,
        allocator: Allocator,
        stdout_list: *std.ArrayList(u8),
        stderr_list: *std.ArrayList(u8),
        max_bytes: usize,
    ) !void {
        const out = self.stdout orelse return error.StdoutNotPiped;
        const err = self.stderr orelse return error.StderrNotPiped;

        // Read stderr first into a buffer, then stdout. Sequential — pipes are
        // small (~64K) but the test harness uses 1MB caps which is fine for
        // the kinds of CLI commands we run.
        try readToEndAppending(allocator, out, stdout_list, max_bytes);
        try readToEndAppending(allocator, err, stderr_list, max_bytes);
    }

    fn readToEndAppending(
        allocator: Allocator,
        file: std.Io.File,
        list: *std.ArrayList(u8),
        max_bytes: usize,
    ) !void {
        var buf: [4096]u8 = undefined;
        while (true) {
            const n = file.readStreaming(io(), &.{&buf}) catch |e| switch (e) {
                error.EndOfStream => break,
                else => return e,
            };
            if (n == 0) break;
            if (list.items.len + n > max_bytes) return error.StreamTooLong;
            try list.appendSlice(allocator, buf[0..n]);
        }
    }
};
