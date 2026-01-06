//! Shared Validation Utilities
//!
//! Common input validation functions used across modules (actions, workflow,
//! processing, etc.). Centralised here so identifier rules are consistent
//! throughout the system.

const std = @import("std");

/// Validates a resource identifier (worker IDs, action names, queue names, etc.).
/// Allowed: a-z, A-Z, 0-9, hyphen, underscore, dot. Length 1..128.
pub fn isValidIdentifier(name: []const u8) bool {
    if (name.len == 0 or name.len > 128) return false;
    for (name) |c| {
        switch (c) {
            'a'...'z', 'A'...'Z', '0'...'9', '-', '_', '.' => {},
            else => return false,
        }
    }
    return true;
}

// =========================================================================
// Tests
// =========================================================================

test "isValidIdentifier: valid names" {
    try std.testing.expect(isValidIdentifier("gpu-worker-1"));
    try std.testing.expect(isValidIdentifier("my_worker.v2"));
    try std.testing.expect(isValidIdentifier("a"));
    try std.testing.expect(isValidIdentifier("ABC123"));
    try std.testing.expect(isValidIdentifier("render-video"));
}

test "isValidIdentifier: empty" {
    try std.testing.expect(!isValidIdentifier(""));
}

test "isValidIdentifier: too long" {
    const long = "a" ** 129;
    try std.testing.expect(!isValidIdentifier(long));
    const max = "a" ** 128;
    try std.testing.expect(isValidIdentifier(max));
}

test "isValidIdentifier: invalid characters" {
    try std.testing.expect(!isValidIdentifier("has space"));
    try std.testing.expect(!isValidIdentifier("has/slash"));
    try std.testing.expect(!isValidIdentifier("has:colon"));
    try std.testing.expect(!isValidIdentifier("../../etc"));
    try std.testing.expect(!isValidIdentifier("tab\there"));
    try std.testing.expect(!isValidIdentifier("newline\nhere"));
}
