//! Unit conversions on times a client supplies. A value that doesn't fit is
//! null, for the handler to refuse, or saturates: a saturated upper bound is
//! no bound, and a saturated lower bound matches nothing.

const std = @import("std");

pub fn secondsToNs(s: u64) ?u64 {
    return std.math.mul(u64, s, std.time.ns_per_s) catch null;
}

/// Absolute expiry `ttl_s` seconds after `now_ns`, or null when it doesn't fit.
pub fn expiryNs(now_ns: u64, ttl_s: u64) ?u64 {
    return std.math.add(u64, now_ns, secondsToNs(ttl_s) orelse return null) catch null;
}

pub fn msToNsSat(ms: u64) u64 {
    return ms *| std.time.ns_per_ms;
}

test "time units: conversions that don't fit are refused or saturate" {
    try std.testing.expectEqual(@as(?u64, 3 * std.time.ns_per_s), secondsToNs(3));
    try std.testing.expectEqual(@as(?u64, null), secondsToNs(std.math.maxInt(u64)));
    try std.testing.expectEqual(@as(?u64, 10 + std.time.ns_per_s), expiryNs(10, 1));
    try std.testing.expectEqual(@as(?u64, null), expiryNs(std.math.maxInt(u64) - 1, 1));
    try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_ms), msToNsSat(2));
    try std.testing.expectEqual(@as(u64, std.math.maxInt(u64)), msToNsSat(std.math.maxInt(u64)));
}
