//! stdx.io — Process-wide `std.Io` facade.
//!
//! Flo deliberately keeps the per-shard reactor on raw `std.posix.system.*`
//! syscalls. The new `std.Io` interface is only used at *boundaries*:
//! CLI, dashboard HTTP, child-process spawn, file system writes outside
//! the hot path, and tests.
//!
//! This module exposes a single process-wide `std.Io` instance.
//! In production, `bootFromInit()` is called by `main()` to bind the
//! facade to the `Io` provided by the Juicy Main `Init` parameter.
//! In tests or other contexts, `bootThreaded(gpa)` constructs a fresh
//! `std.Io.Threaded` instance.
//!
//! Hot-path code MUST NOT import this module — it should stay on
//! `std.posix.*` directly.

const std = @import("std");

var threaded: std.Io.Threaded = .init_single_threaded;
var owned: bool = false;
var override_io: ?std.Io = null;

/// Bind the facade to an externally-managed `Io` (e.g., from `Init.io`).
pub fn bootFromInit(io: std.Io) void {
    override_io = io;
}

/// Initialize the facade with a fresh `std.Io.Threaded` (used in tests
/// and stand-alone tools that don't go through Juicy Main).
pub fn bootThreaded(gpa: std.mem.Allocator) void {
    if (owned) return;
    threaded = std.Io.Threaded.init(gpa, .{});
    owned = true;
}

/// Tear down a fresh threaded instance created via `bootThreaded`.
pub fn shutdown() void {
    if (!owned) return;
    threaded.deinit();
    threaded = .init_single_threaded;
    owned = false;
}

/// Get the process-wide `std.Io` instance.
pub fn instance() std.Io {
    if (override_io) |io| return io;
    if (@import("builtin").is_test) {
        // Test runner sets up `std.testing.io_instance` per-test with a real
        // backing allocator. Use it so spawn/IO operations have memory.
        return std.testing.io_instance.io();
    }
    return threaded.io();
}

/// Best-effort blocking write of `bytes` to `fd`. Used by logging and
/// other diagnostics that previously called `std.posix.write`. Returns
/// the number of bytes successfully written, or 0 on error (silent).
pub fn writeFd(fd: std.posix.fd_t, bytes: []const u8) usize {
    var written: usize = 0;
    while (written < bytes.len) {
        const n = std.c.write(fd, bytes.ptr + written, bytes.len - written);
        if (n <= 0) break;
        written += @intCast(n);
    }
    return written;
}

/// Fallible non-blocking write. Surfaces `WouldBlock` so reactors can arm
/// writability and try again. All other errors collapse to `WriteFailed`.
pub const WriteFdError = error{ WouldBlock, WriteFailed };
pub fn tryWriteFd(fd: std.posix.fd_t, bytes: []const u8) WriteFdError!usize {
    const n = std.c.write(fd, bytes.ptr, bytes.len);
    if (n >= 0) return @intCast(n);
    const errno = std.posix.errno(n);
    return switch (errno) {
        .AGAIN, .INTR => error.WouldBlock,
        else => error.WriteFailed,
    };
}

/// Look up an environment variable from the global process environment.
/// Replacement for `@import("stdx").io.getenv` (removed in 0.16).
pub fn getenv(key: []const u8) ?[:0]const u8 {
    var i: usize = 0;
    while (std.c.environ[i]) |entry_ptr| : (i += 1) {
        const entry = std.mem.span(entry_ptr);
        if (entry.len <= key.len) continue;
        if (entry[key.len] != '=') continue;
        if (!std.mem.eql(u8, entry[0..key.len], key)) continue;
        const value_start = key.len + 1;
        return entry[value_start..entry.len :0];
    }
    return null;
}

/// Create an anonymous pipe. Returns `[2]fd_t = .{ read_fd, write_fd }`.
/// Replacement for the removed `std.posix.pipe()`.
pub fn pipe() error{PipeFailed}![2]std.posix.fd_t {
    var fds: [2]c_int = undefined;
    if (std.c.pipe(&fds) != 0) return error.PipeFailed;
    return .{ fds[0], fds[1] };
}
