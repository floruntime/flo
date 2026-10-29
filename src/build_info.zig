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

/// Whether the faults e2e tests drive are compiled in (`-Dtest-faults`).
/// The line says so, so a test can refuse a build without them, and a
/// release check can refuse one with them.
pub const test_faults = @import("build_options").test_faults;

/// e.g. "zig 0.17.0, safe, alloc=libc", with ", test-faults" when they
/// are compiled in.
pub const line = std.fmt.comptimePrint("zig {s}, {t}, alloc={s}{s}", .{ builtin.zig_version_string, builtin.mode, allocator_name, if (test_faults) ", test-faults" else "" });

test "build_info: the line names the test faults exactly when they are compiled in" {
    try std.testing.expectEqual(test_faults, std.mem.endsWith(u8, line, ", test-faults"));
}
