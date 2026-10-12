//! What this binary is: its compiler, its mode, and the allocator `main`
//! roots every allocation in.
const std = @import("std");
const builtin = @import("builtin");

/// Release builds keep libc's allocator (SmpAllocator via Zig's libc on
/// Linux, libSystem on macOS). Zig 0.17's default for safe builds,
/// `SafeAllocator`, cost 3-5% CPU and up to twice the resident memory for
/// checks that matter in Debug, so only Debug takes it, for its leak and
/// double-free checks.
pub const allocator_name = if (builtin.mode == .debug) "safe" else "libc";

var safe_allocator: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{});

pub fn gpa() std.mem.Allocator {
    return if (builtin.mode == .debug) safe_allocator.allocator() else std.heap.c_allocator;
}

/// Reports leaks in Debug; a no-op otherwise.
pub fn deinit() void {
    if (builtin.mode == .debug) _ = safe_allocator.deinit();
}

/// e.g. "zig 0.17.0, safe, alloc=libc"
pub const line = std.fmt.comptimePrint("zig {s}, {t}, alloc={s}", .{ builtin.zig_version_string, builtin.mode, allocator_name });
