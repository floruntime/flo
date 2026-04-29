//! E2E Test Assertions
//!
//! Custom assertion helpers for e2e tests that provide clear error messages
//! and support common e2e testing patterns.
//!
//! ## Usage
//! ```zig
//! const stdx = @import("stdx");
//! const assertSucceeded = stdx.testing.assertSucceeded;
//! const assertContains = stdx.testing.assertContains;
//!
//! var result = try ctx.cli.run(&.{"kv", "get", "key"});
//! defer result.deinit();
//!
//! try assertSucceeded(result);
//! try assertContains(result, "expected_value");
//! ```

const std = @import("std");
const testing = std.testing;
const CommandResult = @import("client.zig").CommandResult;

/// Assert that a command succeeded (exit code 0)
pub fn assertSucceeded(result: CommandResult) !void {
    if (!result.succeeded()) {
        std.debug.print("\n[E2E ASSERTION FAILED] Expected command to succeed\n", .{});
        std.debug.print("  Exit code: {d}\n", .{result.exit_code});
        if (result.stdout.len > 0) {
            std.debug.print("  Stdout: {s}\n", .{result.stdout});
        }
        if (result.stderr.len > 0) {
            std.debug.print("  Stderr: {s}\n", .{result.stderr});
        }
        return error.AssertionFailed;
    }
}

/// Assert that a command failed (non-zero exit code)
pub fn assertFailed(result: CommandResult) !void {
    if (result.succeeded()) {
        std.debug.print("\n[E2E ASSERTION FAILED] Expected command to fail\n", .{});
        std.debug.print("  Exit code: {d}\n", .{result.exit_code});
        if (result.stdout.len > 0) {
            std.debug.print("  Stdout: {s}\n", .{result.stdout});
        }
        return error.AssertionFailed;
    }
}

/// Assert that output contains expected string
pub fn assertContains(result: CommandResult, expected: []const u8) !void {
    if (!result.contains(expected)) {
        std.debug.print("\n[E2E ASSERTION FAILED] Expected output to contain: '{s}'\n", .{expected});
        std.debug.print("  Stdout: {s}\n", .{result.stdout});
        std.debug.print("  Stderr: {s}\n", .{result.stderr});
        return error.AssertionFailed;
    }
}

/// Assert that stdout contains expected string
pub fn assertStdoutContains(result: CommandResult, expected: []const u8) !void {
    if (!result.stdoutContains(expected)) {
        std.debug.print("\n[E2E ASSERTION FAILED] Expected stdout to contain: '{s}'\n", .{expected});
        std.debug.print("  Stdout: {s}\n", .{result.stdout});
        return error.AssertionFailed;
    }
}

/// Assert that stderr contains expected string
pub fn assertStderrContains(result: CommandResult, expected: []const u8) !void {
    if (!result.stderrContains(expected)) {
        std.debug.print("\n[E2E ASSERTION FAILED] Expected stderr to contain: '{s}'\n", .{expected});
        std.debug.print("  Stderr: {s}\n", .{result.stderr});
        return error.AssertionFailed;
    }
}

/// Assert that output does NOT contain a string
pub fn assertNotContains(result: CommandResult, unexpected: []const u8) !void {
    if (result.contains(unexpected)) {
        std.debug.print("\n[E2E ASSERTION FAILED] Expected output to NOT contain: '{s}'\n", .{unexpected});
        std.debug.print("  Stdout: {s}\n", .{result.stdout});
        std.debug.print("  Stderr: {s}\n", .{result.stderr});
        return error.AssertionFailed;
    }
}

/// Assert that a nullable value is not null
pub fn assertNotNull(comptime T: type, value: ?T) !T {
    if (value) |v| {
        return v;
    }
    std.debug.print("\n[E2E ASSERTION FAILED] Expected non-null value\n", .{});
    return error.AssertionFailed;
}

/// Assert that a nullable value is null
pub fn assertNull(comptime T: type, value: ?T) !void {
    if (value != null) {
        std.debug.print("\n[E2E ASSERTION FAILED] Expected null value\n", .{});
        return error.AssertionFailed;
    }
}

/// Assert two strings are equal
pub fn assertStringsEqual(expected: []const u8, actual: []const u8) !void {
    if (!std.mem.eql(u8, expected, actual)) {
        std.debug.print("\n[E2E ASSERTION FAILED] Strings not equal\n", .{});
        std.debug.print("  Expected: '{s}'\n", .{expected});
        std.debug.print("  Actual:   '{s}'\n", .{actual});
        return error.AssertionFailed;
    }
}

/// Assert string starts with prefix
pub fn assertStartsWith(str: []const u8, prefix: []const u8) !void {
    if (!std.mem.startsWith(u8, str, prefix)) {
        std.debug.print("\n[E2E ASSERTION FAILED] String does not start with expected prefix\n", .{});
        std.debug.print("  Expected prefix: '{s}'\n", .{prefix});
        std.debug.print("  Actual string:   '{s}'\n", .{str});
        return error.AssertionFailed;
    }
}

/// Assert string ends with suffix
pub fn assertEndsWith(str: []const u8, suffix: []const u8) !void {
    if (!std.mem.endsWith(u8, str, suffix)) {
        std.debug.print("\n[E2E ASSERTION FAILED] String does not end with expected suffix\n", .{});
        std.debug.print("  Expected suffix: '{s}'\n", .{suffix});
        std.debug.print("  Actual string:   '{s}'\n", .{str});
        return error.AssertionFailed;
    }
}

// =============================================================================
// Test Result Logging
// =============================================================================

/// Log a test pass (for verbose output)
pub fn logPass(test_name: []const u8) void {
    std.debug.print("✓ {s}\n", .{test_name});
}

/// Log a test failure with details
pub fn logFail(test_name: []const u8, details: []const u8) void {
    std.debug.print("✗ {s}: {s}\n", .{ test_name, details });
}

/// Log test section header
pub fn logSection(name: []const u8) void {
    std.debug.print("\n=== {s} ===\n\n", .{name});
}
