//! Unit conversions on times a client supplies. A value that doesn't fit is
//! null, for the handler to refuse, or saturates: a saturated upper bound is
//! no bound, and a saturated lower bound matches nothing.

const std = @import("std");

pub fn msToNs(ms: u64) ?u64 {
    return std.math.mul(u64, ms, std.time.ns_per_ms) catch null;
}

/// Absolute expiry `ttl_ms` milliseconds after `now_ns`, or null when it
/// doesn't fit.
pub fn expiryNs(now_ns: u64, ttl_ms: u64) ?u64 {
    return std.math.add(u64, now_ns, msToNs(ttl_ms) orelse return null) catch null;
}

/// A duration a person writes, in milliseconds: a whole number and a unit
/// (`ms`, `s`, `m`, `h`, `d`), or a bare `0`. A bare number is refused, so
/// "3600" is never read as either seconds or milliseconds by guess.
pub fn parseDurationMs(text: []const u8) ?u64 {
    if (std.mem.eql(u8, text, "0")) return 0;
    const units = [_]struct { suffix: []const u8, ms: u64 }{
        .{ .suffix = "ms", .ms = 1 },
        .{ .suffix = "s", .ms = std.time.ms_per_s },
        .{ .suffix = "m", .ms = std.time.ms_per_min },
        .{ .suffix = "h", .ms = std.time.ms_per_hour },
        .{ .suffix = "d", .ms = std.time.ms_per_day },
    };
    for (units) |u| {
        if (!std.mem.endsWith(u8, text, u.suffix)) continue;
        const digits = text[0 .. text.len - u.suffix.len];
        if (digits.len == 0) return null;
        const n = std.fmt.parseInt(u64, digits, 10) catch return null;
        return std.math.mul(u64, n, u.ms) catch null;
    }
    return null;
}

/// The longest age or retention a request may give: past it a value is a
/// mistake, not a policy, and is refused rather than saturated.
pub const MAX_AGE_MS: u64 = 100 * 366 * std.time.ms_per_day;

pub fn msToNsSat(ms: u64) u64 {
    return ms *| std.time.ns_per_ms;
}

/// When a TTL of `ttl_ms`, written by the entry stamped `stamp_ns`, runs out;
/// 0 when there is no TTL. Every replica applies the same entry, so every
/// replica gets the same answer. Saturates: a TTL past the end of time never
/// runs out.
pub fn expiryAfter(stamp_ns: u64, ttl_ms: u64) u64 {
    if (ttl_ms == 0) return 0;
    return stamp_ns +| msToNsSat(ttl_ms);
}

test "time units: conversions that don't fit are refused or saturate" {
    try std.testing.expectEqual(@as(?u64, 3 * std.time.ns_per_ms), msToNs(3));
    try std.testing.expectEqual(@as(?u64, null), msToNs(std.math.maxInt(u64)));
    try std.testing.expectEqual(@as(?u64, 10 + std.time.ns_per_ms), expiryNs(10, 1));
    try std.testing.expectEqual(@as(?u64, null), expiryNs(std.math.maxInt(u64) - 1, 1));
    try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_ms), msToNsSat(2));
    try std.testing.expectEqual(@as(u64, std.math.maxInt(u64)), msToNsSat(std.math.maxInt(u64)));
    try std.testing.expectEqual(@as(u64, 0), expiryAfter(7, 0));
    try std.testing.expectEqual(@as(u64, 7 + 2 * std.time.ns_per_ms), expiryAfter(7, 2));
    try std.testing.expectEqual(@as(u64, std.math.maxInt(u64)), expiryAfter(std.math.maxInt(u64) - 1, 1));
}

test "time units: a written duration needs a unit, and a bare 0 clears" {
    try std.testing.expectEqual(@as(?u64, 500), parseDurationMs("500ms"));
    try std.testing.expectEqual(@as(?u64, 30_000), parseDurationMs("30s"));
    try std.testing.expectEqual(@as(?u64, 300_000), parseDurationMs("5m"));
    try std.testing.expectEqual(@as(?u64, 3_600_000), parseDurationMs("1h"));
    try std.testing.expectEqual(@as(?u64, 86_400_000), parseDurationMs("1d"));
    try std.testing.expectEqual(@as(?u64, 0), parseDurationMs("0"));
    for ([_][]const u8{ "3600", "", "ms", "1.5s", "-1s", "10x", "99999999999999999999d" }) |bad| {
        try std.testing.expectEqual(@as(?u64, null), parseDurationMs(bad));
    }
}
