//! stdx.time — Replacements for time helpers removed in Zig 0.16.
//!
//! `std.time.nanoTimestamp` and friends were removed when the `std.Io`
//! migration moved time access onto `std.Io.Clock`. Hot-path code
//! shouldn't pay for an `Io` indirection just to read the wall clock,
//! so we provide thin wrappers around `clock_gettime` here.

const std = @import("std");

/// Wall-clock time in nanoseconds since the Unix epoch.
pub fn nanoTimestamp() i128 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

/// Wall-clock time in milliseconds since the Unix epoch.
pub fn milliTimestamp() i64 {
    return @intCast(@divFloor(nanoTimestamp(), std.time.ns_per_ms));
}

/// Wall-clock time in seconds since the Unix epoch (replacement for
/// `std.time.timestamp`).
pub fn timestamp() i64 {
    return @intCast(@divFloor(nanoTimestamp(), std.time.ns_per_s));
}

/// Simple wall-clock timer (replacement for `std.time.Timer`, removed in 0.16).
pub const Timer = struct {
    start_ns: i128,

    pub fn start() Timer {
        return .{ .start_ns = nanoTimestamp() };
    }

    /// Returns elapsed nanoseconds as i64.
    pub fn read(self: Timer) i64 {
        return @intCast(nanoTimestamp() - self.start_ns);
    }
};

/// Sleep the current thread for `nanoseconds` (replacement for
/// `std.Thread.sleep`/`std.time.sleep`, both removed in 0.16).
pub fn sleep(nanoseconds: u64) void {
    var req: std.c.timespec = .{
        .sec = @intCast(@divFloor(nanoseconds, std.time.ns_per_s)),
        .nsec = @intCast(@mod(nanoseconds, std.time.ns_per_s)),
    };
    while (std.c.nanosleep(&req, &req) != 0) {
        const e = std.posix.errno(@as(c_int, -1));
        if (e != .INTR) break;
    }
}

// =============================================================================
// Tests
// =============================================================================

test "Timer: start and read returns positive elapsed" {
    const timer = Timer.start();
    sleep(1 * std.time.ns_per_ms); // sleep 1ms
    const elapsed = timer.read();
    try std.testing.expect(elapsed > 0);
    try std.testing.expect(elapsed >= 1_000_000); // at least 1ms
    try std.testing.expect(elapsed < 1_000_000_000); // less than 1s
}

test "nanoTimestamp returns reasonable value" {
    const ts = nanoTimestamp();
    // Should be after 2020-01-01 and before 2100-01-01
    const ns_2020: i128 = 1577836800 * std.time.ns_per_s;
    const ns_2100: i128 = 4102444800 * std.time.ns_per_s;
    try std.testing.expect(ts > ns_2020);
    try std.testing.expect(ts < ns_2100);
}

test "milliTimestamp returns reasonable value" {
    const ms = milliTimestamp();
    const ms_2020: i64 = 1577836800 * std.time.ms_per_s;
    try std.testing.expect(ms > ms_2020);
}
