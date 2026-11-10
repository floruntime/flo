//! Cron Expression Parser & Calendar Utilities
//!
//! Full 5-field cron expression parser: minute hour day-of-month month day-of-week
//! Supports: * (all), */N (step), N (value), N-M (range), N-M/S (range+step), N,M,O (list)
//! Day-of-week: 0=Sunday..6=Saturday, 7=Sunday (alias)
//! Times are UTC.
//!
//! Calendar helpers use Howard Hinnant's civil calendar algorithm for
//! epoch ↔ calendar conversion.

const std = @import("std");
const mem = std.mem;

// =============================================================================
// Calendar Types
// =============================================================================

pub const CalendarTime = struct {
    year: i32,
    month: u8, // 1-12
    day: u8, // 1-31
    hour: u8, // 0-23
    minute: u8, // 0-59
};

// =============================================================================
// Cron Parser
// =============================================================================

/// A parsed 5-field cron expression, evaluated in UTC: a bitset per field.
pub const Cron = struct {
    minutes: u64,
    hours: u64,
    days: u64,
    months: u64,
    weekdays: u64,
    /// A day field that is `*` (or `*/N`) leaves the day to the other one;
    /// with both restricted, a day matching either fires, as in standard
    /// cron.
    days_any: bool,
    weekdays_any: bool,

    pub fn parse(expr: []const u8) ?Cron {
        var fields: [5][]const u8 = undefined;
        var n: usize = 0;
        var it = mem.tokenizeAny(u8, expr, " \t");
        while (it.next()) |f| {
            if (n == 5) return null;
            fields[n] = f;
            n += 1;
        }
        if (n != 5) return null;
        var weekdays = parseCronField(fields[4], 0, 7) orelse return null;
        // Sunday is 0 or 7.
        if (weekdays & (@as(u64, 1) << 7) != 0) weekdays |= 1;
        return .{
            .minutes = parseCronField(fields[0], 0, 59) orelse return null,
            .hours = parseCronField(fields[1], 0, 23) orelse return null,
            .days = parseCronField(fields[2], 1, 31) orelse return null,
            .months = parseCronField(fields[3], 1, 12) orelse return null,
            .weekdays = weekdays,
            .days_any = fields[2][0] == '*',
            .weekdays_any = fields[4][0] == '*',
        };
    }

    fn has(set: u64, v: u8) bool {
        return set & (@as(u64, 1) << @as(u6, @intCast(v))) != 0;
    }

    pub fn firesOn(self: Cron, year: i32, month: u8, day: u8) bool {
        if (!has(self.months, month)) return false;
        const dom = has(self.days, day);
        const dow = has(self.weekdays, dayOfWeek(year, month, day));
        if (self.days_any and self.weekdays_any) return true;
        if (self.days_any) return dow;
        if (self.weekdays_any) return dom;
        return dom or dow;
    }

    /// The highest value in `set` at most `limit`.
    fn highest(set: u64, limit: u8) ?u8 {
        const masked = set & ((@as(u64, 2) << @as(u6, @intCast(limit))) - 1);
        if (masked == 0) return null;
        return @intCast(63 - @clz(masked));
    }

    /// The latest fire time at or before `now_ms`, within four years.
    pub fn latest(self: Cron, now_ms: i64) ?i64 {
        const now_secs = @divFloor(now_ms, 1000);
        const now_dt = epochSecsToCalendar(now_secs);
        var day_ms = calendarToEpochMs(now_dt.year, now_dt.month, now_dt.day, 0, 0);
        var first = true;
        var days: usize = 0;
        while (days <= 4 * 366) : (days += 1) {
            const dt = epochSecsToCalendar(@divFloor(day_ms, 1000));
            if (self.firesOn(dt.year, dt.month, dt.day)) {
                var hour_limit: u8 = if (first) now_dt.hour else 23;
                while (highest(self.hours, hour_limit)) |h| {
                    const minute_limit: u8 = if (first and h == now_dt.hour) now_dt.minute else 59;
                    if (highest(self.minutes, minute_limit)) |m| {
                        return day_ms + @as(i64, h) * 3_600_000 + @as(i64, m) * 60_000;
                    }
                    if (h == 0) break;
                    hour_limit = h - 1;
                }
            }
            first = false;
            day_ms -= 86_400_000;
        }
        return null;
    }
};

/// Parse a single cron field into a u64 bitset. Supports comma-separated tokens.
pub fn parseCronField(field: []const u8, min: u8, max: u8) ?u64 {
    var result: u64 = 0;
    var remaining: []const u8 = field;

    while (remaining.len > 0) {
        const comma_pos = mem.indexOfScalar(u8, remaining, ',');
        const token = if (comma_pos) |p| remaining[0..p] else remaining;
        remaining = if (comma_pos) |p| remaining[p + 1 ..] else "";
        result |= parseCronToken(token, min, max) orelse return null;
    }
    return if (result != 0) result else null;
}

/// Parse a single cron token: *, */N, N, N-M, N-M/S
pub fn parseCronToken(token: []const u8, min: u8, max: u8) ?u64 {
    if (token.len == 0) return null;

    // */N — step from min
    if (mem.startsWith(u8, token, "*/")) {
        const step = std.fmt.parseInt(u8, token[2..], 10) catch return null;
        if (step == 0) return null;
        var result: u64 = 0;
        var v: u16 = min;
        while (v <= @as(u16, max)) : (v += step) {
            result |= @as(u64, 1) << @as(u6, @intCast(v));
        }
        return result;
    }

    // * — all values
    if (mem.eql(u8, token, "*")) {
        var result: u64 = 0;
        var v: u16 = min;
        while (v <= @as(u16, max)) : (v += 1) {
            result |= @as(u64, 1) << @as(u6, @intCast(v));
        }
        return result;
    }

    // Check for range: N-M or N-M/S
    if (mem.indexOfScalar(u8, token, '-')) |dash_pos| {
        const left = std.fmt.parseInt(u8, token[0..dash_pos], 10) catch return null;
        const right_part = token[dash_pos + 1 ..];
        const slash_pos = mem.indexOfScalar(u8, right_part, '/');
        const range_end_str = if (slash_pos) |p| right_part[0..p] else right_part;
        const step: u16 = if (slash_pos) |p|
            @as(u16, std.fmt.parseInt(u8, right_part[p + 1 ..], 10) catch return null)
        else
            1;
        const right = std.fmt.parseInt(u8, range_end_str, 10) catch return null;
        if (left < min or right > max or left > right or step == 0) return null;

        var result: u64 = 0;
        var v: u16 = left;
        while (v <= @as(u16, right)) : (v += step) {
            result |= @as(u64, 1) << @as(u6, @intCast(v));
        }
        return result;
    }

    // Plain number
    const val = std.fmt.parseInt(u8, token, 10) catch return null;
    if (val < min or val > max) return null;
    return @as(u64, 1) << @as(u6, @intCast(val));
}

// =============================================================================
// Calendar Helpers
// =============================================================================

/// Convert epoch seconds to calendar components (UTC)
pub fn epochSecsToCalendar(epoch_secs: i64) CalendarTime {
    const secs: u64 = @intCast(@max(epoch_secs, 0));
    const es = std.time.epoch.EpochSeconds{ .secs = secs };
    const eday = es.getEpochDay();
    const ds = es.getDaySeconds();
    const yd = eday.calculateYearDay();
    const md = yd.calculateMonthDay();

    return .{
        .year = yd.year,
        .month = md.month.numeric(),
        .day = md.day_index + 1, // day_index is 0-based
        .hour = ds.getHoursIntoDay(),
        .minute = ds.getMinutesIntoHour(),
    };
}

/// Convert calendar components (UTC) to epoch milliseconds.
/// Uses Howard Hinnant's civil calendar algorithm.
pub fn calendarToEpochMs(year: i32, month: u8, day: u8, hour: u8, minute: u8) i64 {
    // Adjust to March-based year for leap year simplicity
    const y: i64 = @as(i64, year) - @as(i64, if (month <= 2) 1 else 0);
    const m: i64 = @as(i64, month) + (if (month > 2) @as(i64, -3) else 9);
    const d: i64 = @as(i64, day) - 1;

    const era: i64 = @divFloor(y, 400);
    const yoe: i64 = y - era * 400; // year of era [0, 399]
    const doy: i64 = @divFloor(153 * m + 2, 5) + d; // day of year [0, 365]
    const doe: i64 = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy; // day of era
    const days: i64 = era * 146097 + doe - 719468; // days since epoch

    return days * 86_400_000 + @as(i64, hour) * 3_600_000 + @as(i64, minute) * 60_000;
}

/// Day of week: 0=Sunday, 1=Monday, ..., 6=Saturday
pub fn dayOfWeek(year: i32, month: u8, day: u8) u8 {
    const epoch_ms = calendarToEpochMs(year, month, day, 0, 0);
    const epoch_days = @divFloor(epoch_ms, 86_400_000);
    // Jan 1 1970 was Thursday (4). (day + 4) % 7 gives 0=Sun..6=Sat
    const raw = @mod(epoch_days + 4, 7);
    return @intCast(raw);
}

/// Number of days in a given month (handles leap years)
pub fn daysInMonth(year: i32, month: u8) u8 {
    const days_per_month = [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (month < 1 or month > 12) return 30;
    if (month == 2) {
        const y = @as(i64, year);
        const is_leap = (@mod(y, 4) == 0 and @mod(y, 100) != 0) or @mod(y, 400) == 0;
        return if (is_leap) 29 else 28;
    }
    return days_per_month[month - 1];
}

// =============================================================================
// Tests
// =============================================================================

/// Epoch ms of a UTC time, for the tests.
fn at(year: i32, month: u8, day: u8, hour: u8, minute: u8) i64 {
    return calendarToEpochMs(year, month, day, hour, minute);
}

fn latestOf(expr: []const u8, now_ms: i64) ?i64 {
    return (Cron.parse(expr) orelse return null).latest(now_ms);
}

test "cron: the latest fire is at or before now, on the minute" {
    const t = std.testing;
    // 2024-01-15 10:32:45 UTC, a Monday.
    const now = at(2024, 1, 15, 10, 32) + 45_000;
    try t.expectEqual(@as(?i64, at(2024, 1, 15, 10, 32)), latestOf("* * * * *", now));
    try t.expectEqual(@as(?i64, at(2024, 1, 15, 10, 30)), latestOf("*/5 * * * *", now));
    // Exactly on a fire time: that one.
    try t.expectEqual(@as(?i64, at(2024, 1, 15, 10, 30)), latestOf("*/5 * * * *", at(2024, 1, 15, 10, 30)));
    try t.expectEqual(@as(?i64, at(2024, 1, 15, 9, 0)), latestOf("0 9 * * *", now));
    // Later in today's hour and day than now: yesterday's.
    try t.expectEqual(@as(?i64, at(2024, 1, 14, 11, 0)), latestOf("0 11 * * *", now));
    try t.expectEqual(@as(?i64, at(2024, 1, 15, 9, 59)), latestOf("59 9 * * *", now));
    try t.expectEqual(@as(?i64, at(2024, 1, 15, 10, 15)), latestOf("15,45 * * * *", now));
}

test "cron: ranges and steps cover their bounds" {
    const t = std.testing;
    const now = at(2024, 1, 15, 23, 59);
    try t.expectEqual(@as(?i64, at(2024, 1, 15, 23, 59)), latestOf("59 23 * * *", now));
    try t.expectEqual(@as(?i64, at(2024, 1, 15, 23, 40)), latestOf("0-40/20 * * * *", now));
    try t.expectEqual(@as(?i64, at(2024, 1, 15, 22, 0)), latestOf("0 */2 * * *", now));
    try t.expectEqual(@as(?i64, at(2024, 1, 15, 17, 30)), latestOf("30 9-17 * * *", now));
    // Out of range, reversed, empty or malformed: not an expression.
    for ([_][]const u8{ "60 * * * *", "* 24 * * *", "* * 0 * *", "* * 32 * *", "* * * 13 *", "* * * * 8", "5-1 * * * *", "* * 0-5 * *", "*/0 * * * *", "* * * *", "* * * * * *", "" }) |bad| {
        try t.expect(Cron.parse(bad) == null);
    }
}

test "cron: day of month and day of week, either when both are given" {
    const t = std.testing;
    // 2024-01-15 is a Monday; the 13th a Saturday, the 14th a Sunday.
    const now = at(2024, 1, 15, 12, 0);
    // Only day of week: the latest Sunday, as 0 and as 7.
    try t.expectEqual(@as(?i64, at(2024, 1, 14, 0, 0)), latestOf("0 0 * * 0", now));
    try t.expectEqual(@as(?i64, at(2024, 1, 14, 0, 0)), latestOf("0 0 * * 7", now));
    // Only day of month.
    try t.expectEqual(@as(?i64, at(2024, 1, 13, 0, 0)), latestOf("0 0 13 * *", now));
    // Both: the 1st or a Saturday, whichever came last.
    try t.expectEqual(@as(?i64, at(2024, 1, 13, 0, 0)), latestOf("0 0 1 * 6", now));
    try t.expectEqual(@as(?i64, at(2024, 1, 15, 0, 0)), latestOf("0 0 15 * 6", now));
    // `*/N` in a day field leaves the day to the other one.
    try t.expectEqual(@as(?i64, at(2024, 1, 14, 0, 0)), latestOf("0 0 */1 * 0", now));
}

test "cron: month ends and leap years" {
    const t = std.testing;
    // The 31st skips months without one.
    try t.expectEqual(@as(?i64, at(2024, 3, 31, 0, 0)), latestOf("0 0 31 * *", at(2024, 4, 30, 23, 59)));
    // Feb 29 is in leap years only: 2024, then 2028.
    try t.expectEqual(@as(?i64, at(2024, 2, 29, 0, 0)), latestOf("0 0 29 2 *", at(2027, 6, 1, 0, 0)));
    try t.expectEqual(@as(?i64, at(2024, 2, 29, 0, 0)), latestOf("0 0 29 2 *", at(2024, 3, 1, 0, 0)));
    // 2100 is not a leap year; 2000 is.
    try t.expectEqual(@as(?i64, at(2096, 2, 29, 0, 0)), latestOf("0 0 29 2 *", at(2100, 3, 1, 0, 0)));
    try t.expectEqual(@as(?i64, at(2000, 2, 29, 0, 0)), latestOf("0 0 29 2 *", at(2000, 3, 1, 0, 0)));
    // Across a year end.
    try t.expectEqual(@as(?i64, at(2023, 12, 31, 23, 0)), latestOf("0 23 * * *", at(2024, 1, 1, 22, 59)));
    // A date that never comes: no fire.
    try t.expectEqual(@as(?i64, null), latestOf("0 0 30 2 *", at(2024, 6, 1, 0, 0)));
}

test "parseCronField: basic patterns" {
    const testing = std.testing;
    // Wildcard
    const star = parseCronField("*", 0, 59);
    try testing.expect(star != null);
    try testing.expect(star.? & 1 != 0); // bit 0 set
    try testing.expect(star.? & (@as(u64, 1) << 59) != 0); // bit 59 set

    // Step
    const step = parseCronField("*/15", 0, 59);
    try testing.expect(step != null);
    try testing.expect(step.? & 1 != 0); // 0
    try testing.expect(step.? & (@as(u64, 1) << 15) != 0); // 15
    try testing.expect(step.? & (@as(u64, 1) << 30) != 0); // 30
    try testing.expect(step.? & (@as(u64, 1) << 45) != 0); // 45
    try testing.expect(step.? & (@as(u64, 1) << 10) == 0); // 10 not set

    // Single value
    const single = parseCronField("30", 0, 59);
    try testing.expect(single != null);
    try testing.expect(single.? == (@as(u64, 1) << 30));

    // Range
    const range = parseCronField("1-5", 1, 31);
    try testing.expect(range != null);
    try testing.expect(range.? & (@as(u64, 1) << 1) != 0);
    try testing.expect(range.? & (@as(u64, 1) << 5) != 0);
    try testing.expect(range.? & (@as(u64, 1) << 6) == 0);

    // Comma list
    const list = parseCronField("0,15,30,45", 0, 59);
    try testing.expect(list != null);
    try testing.expect(list.? & 1 != 0);
    try testing.expect(list.? & (@as(u64, 1) << 15) != 0);
    try testing.expect(list.? & (@as(u64, 1) << 30) != 0);
    try testing.expect(list.? & (@as(u64, 1) << 45) != 0);
}
