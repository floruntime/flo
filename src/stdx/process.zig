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

/// The open-file (NOFILE) soft limit before and after `raiseOpenFileLimit`.
/// `was == 0` means the limit could not be read. `refused` means a raise was
/// attempted and the OS rejected it, so the limit is still `was`.
pub const OpenFileLimit = struct { was: u64, now: u64, refused: bool = false };

/// The soft open-file limit to ask for, given the current soft (`cur`), the
/// hard limit (`hard`, possibly RLIM_INFINITY as a very large value), and the
/// platform ceiling a soft limit cannot exceed (`ceiling`, 0 when none is
/// known). The hard limit bounds it, the ceiling bounds it below the hard
/// limit, and it is never lowered below `cur`. A pure function so the cap can
/// be tested without a host whose real limits vary.
pub fn targetOpenFileLimit(cur: u64, hard: u64, ceiling: u64) u64 {
    var want = hard;
    if (ceiling != 0 and want > ceiling) want = ceiling;
    if (want < cur) want = cur;
    return want;
}

/// The highest soft NOFILE this platform accepts, above which `setrlimit`
/// fails even when the hard limit is higher: OPEN_MAX on macOS, `fs.nr_open`
/// on Linux (falling back to a common value if it can't be read), none known
/// elsewhere.
fn openFileCeiling() u64 {
    return switch (builtin.os.tag) {
        .macos => 10_240, // OPEN_MAX
        .linux => readNrOpen() orelse 1_048_576,
        else => 0,
    };
}

fn readNrOpen() ?u64 {
    const fs = @import("fs.zig");
    const file = fs.openFileAbsolute("/proc/sys/fs/nr_open", .{}) catch return null;
    defer fs.closeFile(file);
    var buf: [32]u8 = undefined;
    const n = fs.readAll(file, &buf) catch return null;
    return std.fmt.parseInt(u64, std.mem.trim(u8, buf[0..n], " \t\r\n"), 10) catch null;
}

/// Raise this process's soft open-file limit toward the hard limit, so the
/// server can open as many descriptors as the OS allows rather than a low
/// default (256 under launchd, 1024 on a plain Linux shell). Best effort: it
/// never lowers the limit and never fails the caller. A soft limit above the
/// platform ceiling (`openFileCeiling`) is rejected even when the hard limit is
/// higher — notably a Linux hard limit of RLIM_INFINITY, capped at `fs.nr_open`
/// — so it is capped there; if a raise is still refused, `refused` says so and
/// the caller logs it. The caller logs the result.
pub fn raiseOpenFileLimit() OpenFileLimit {
    var lim = std.posix.getrlimit(.NOFILE) catch return .{ .was = 0, .now = 0 };
    const was: u64 = @intCast(lim.cur);
    const want = targetOpenFileLimit(was, @intCast(lim.max), openFileCeiling());
    if (want <= was) return .{ .was = was, .now = was };
    lim.cur = @intCast(want);
    std.posix.setrlimit(.NOFILE, lim) catch return .{ .was = was, .now = was, .refused = true };
    return .{ .was = was, .now = want };
}

/// This process's current soft open-file limit, or 0 if it can't be read. Read
/// after `raiseOpenFileLimit` at start, it is what the connection cap derives
/// from.
pub fn softOpenFileLimit() u64 {
    const lim = std.posix.getrlimit(.NOFILE) catch return 0;
    return @intCast(lim.cur);
}

test "process: the open-file target is bounded by the hard limit and the platform ceiling, and never lowers" {
    const E = std.testing.expectEqual;
    // Raises toward the hard limit.
    try E(@as(u64, 1024), targetOpenFileLimit(256, 1024, 0));
    // The ceiling caps a higher hard limit (e.g. Linux nr_open under an
    // infinite hard limit).
    try E(@as(u64, 1_048_576), targetOpenFileLimit(1024, std.math.maxInt(u64), 1_048_576));
    // macOS OPEN_MAX caps it.
    try E(@as(u64, 10_240), targetOpenFileLimit(256, std.math.maxInt(u64), 10_240));
    // A hard limit below the ceiling wins.
    try E(@as(u64, 4096), targetOpenFileLimit(256, 4096, 1_048_576));
    // Never below the current soft limit, whatever hard and ceiling say.
    try E(@as(u64, 8192), targetOpenFileLimit(8192, 4096, 1_048_576));
    try E(@as(u64, 8192), targetOpenFileLimit(8192, 2048, 1024));
}

/// This process's environment as it is now, `setenv` included. Caller
/// deinits.
pub fn currentEnv(allocator: Allocator) !std.process.Environ.Map {
    var map = std.process.Environ.Map.init(allocator);
    errdefer map.deinit();
    var i: usize = 0;
    while (std.c.environ[i]) |entry_ptr| : (i += 1) {
        const entry = std.mem.span(entry_ptr);
        const eq = std.mem.indexOfScalar(u8, entry, '=') orelse continue;
        try map.put(entry[0..eq], entry[eq + 1 ..]);
    }
    return map;
}

pub const Child = struct {
    allocator: Allocator,
    argv: []const []const u8,
    stdin_behavior: StdIoBehavior = .Inherit,
    stdout_behavior: StdIoBehavior = .Inherit,
    stderr_behavior: StdIoBehavior = .Inherit,
    pgid: ?std.c.pid_t = null,
    /// The child's whole environment; null inherits the one this process
    /// started with.
    env_map: ?*const std.process.Environ.Map = null,

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
            .environ_map = self.env_map,
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
            .signal => |s| .{ .Signal = @intCast(@backingInt(s)) },
            .stopped => |s| .{ .Stopped = @intCast(@backingInt(s)) },
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
