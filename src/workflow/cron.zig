//! Cron Expression Parser & Calendar Utilities
//!
//! Full 5-field cron expression parser: minute hour day-of-month month day-of-week
//! Supports: * (all), */N (step), N (value), N-M (range), N-M/S (range+step), N,M,O (list)
//! Day-of-week: 0=Sunday..6=Saturday, 7=Sunday (alias)
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

/// Compute the next fire time (epoch milliseconds) for a 5-field cron expression.
/// Returns null if the expression is invalid or no match within 4 years.
pub fn nextCronTime(now_ms: i64, expr: []const u8) ?i64 {
    // 1. Split into 5 whitespace-separated fields
    var fields: [5][]const u8 = undefined;
    var field_count: usize = 0;
    var start: usize = 0;

    for (expr, 0..) |c, i| {
        if (c == ' ' or c == '\t') {
            if (i > start and field_count < 5) {
                fields[field_count] = expr[start..i];
                field_count += 1;
            }
            start = i + 1;
        }
    }
    if (start < expr.len and field_count < 5) {
        fields[field_count] = expr[start..];
        field_count += 1;
    }

    if (field_count != 5) return null;

    // 2. Parse each field into a bitset of allowed values
    const min_set = parseCronField(fields[0], 0, 59) orelse return null;
    const hour_set = parseCronField(fields[1], 0, 23) orelse return null;
    const dom_set = parseCronField(fields[2], 1, 31) orelse return null;
    const mon_set = parseCronField(fields[3], 1, 12) orelse return null;
    var dow_set = parseCronField(fields[4], 0, 7) orelse return null;
    // Normalize: Sunday=7 maps to bit 0 (Sunday=0)
    if (dow_set & (@as(u64, 1) << 7) != 0) dow_set |= 1;

    // 3. Decompose now_ms + 60s (next minute boundary) into calendar components
    const now_secs = @divFloor(now_ms, 1000);
    const next_min_secs = now_secs - @rem(now_secs, 60) + 60;
    var dt = epochSecsToCalendar(next_min_secs);

    // 4. Walk forward to find the next matching time (max 4 years)
    const max_year = dt.year + 4;

    while (dt.year <= max_year) {
        // Month check
        if (mon_set & (@as(u64, 1) << @as(u6, @intCast(dt.month))) == 0) {
            dt.month += 1;
            if (dt.month > 12) {
                dt.month = 1;
                dt.year += 1;
            }
            dt.day = 1;
            dt.hour = 0;
            dt.minute = 0;
            continue;
        }

        // Day check (both day-of-month AND day-of-week must match)
        const dim = daysInMonth(dt.year, dt.month);
        if (dt.day > dim) {
            dt.month += 1;
            if (dt.month > 12) {
                dt.month = 1;
                dt.year += 1;
            }
            dt.day = 1;
            dt.hour = 0;
            dt.minute = 0;
            continue;
        }
        const dow = dayOfWeek(dt.year, dt.month, dt.day);
        const dom_ok = dom_set & (@as(u64, 1) << @as(u6, @intCast(dt.day))) != 0;
        const dow_ok = dow_set & (@as(u64, 1) << @as(u6, @intCast(dow))) != 0;
        if (!dom_ok or !dow_ok) {
            dt.day += 1;
            dt.hour = 0;
            dt.minute = 0;
            continue;
        }

        // Hour check
        if (hour_set & (@as(u64, 1) << @as(u6, @intCast(dt.hour))) == 0) {
            dt.hour += 1;
            dt.minute = 0;
            if (dt.hour >= 24) {
                dt.day += 1;
                dt.hour = 0;
            }
            continue;
        }

        // Minute check
        if (min_set & (@as(u64, 1) << @as(u6, @intCast(dt.minute))) == 0) {
            dt.minute += 1;
            if (dt.minute >= 60) {
                dt.hour += 1;
                dt.minute = 0;
                if (dt.hour >= 24) {
                    dt.day += 1;
                    dt.hour = 0;
                }
            }
            continue;
        }

        // All fields match — convert back to epoch ms
        return calendarToEpochMs(dt.year, dt.month, dt.day, dt.hour, dt.minute);
    }

    return null; // No match within 4 years — likely an invalid expression
}

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
        if (left > max or right > max or step == 0) return null;

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

test "nextCronTime: every 5 minutes" {
    const testing = std.testing;
    // Reference time: 2024-01-15 10:30:00 UTC
    const now_ms: i64 = 1705312200000;
    const result = nextCronTime(now_ms, "*/5 * * * *");
    try testing.expect(result != null);
    // Should be >= now + 60s (next minute boundary at minimum)
    try testing.expect(result.? > now_ms);
    // Should land on a 5-minute boundary
    const dt = epochSecsToCalendar(@divFloor(result.?, 1000));
    try testing.expect(dt.minute % 5 == 0);
}

test "nextCronTime: every Monday at 9AM" {
    const testing = std.testing;
    // Reference time: 2024-01-15 10:30:00 UTC (Monday)
    const now_ms: i64 = 1705312200000;
    const result = nextCronTime(now_ms, "0 9 * * 1");
    try testing.expect(result != null);
    // Should be next Monday at 9:00 (Jan 22, 2024)
    const dt = epochSecsToCalendar(@divFloor(result.?, 1000));
    try testing.expectEqual(@as(u8, 0), dt.minute);
    try testing.expectEqual(@as(u8, 9), dt.hour);
    try testing.expectEqual(@as(u8, 1), dayOfWeek(dt.year, dt.month, dt.day)); // Monday
}

test "nextCronTime: 1st of month at 2:30" {
    const testing = std.testing;
    // Reference time: 2024-01-15 10:30:00 UTC
    const now_ms: i64 = 1705312200000;
    const result = nextCronTime(now_ms, "30 2 1 * *");
    try testing.expect(result != null);
    const dt = epochSecsToCalendar(@divFloor(result.?, 1000));
    try testing.expectEqual(@as(u8, 30), dt.minute);
    try testing.expectEqual(@as(u8, 2), dt.hour);
    try testing.expectEqual(@as(u8, 1), dt.day);
    // Should be Feb 1st since Jan 1st is past
    try testing.expectEqual(@as(u8, 2), dt.month);
}

test "nextCronTime: invalid expression returns null" {
    const testing = std.testing;
    const now_ms: i64 = 1705312200000;
    try testing.expect(nextCronTime(now_ms, "invalid") == null);
    try testing.expect(nextCronTime(now_ms, "* *") == null);
    try testing.expect(nextCronTime(now_ms, "60 * * * *") == null); // minute > 59
}

test "nextCronTime: every day at midnight" {
    const testing = std.testing;
    // Reference time: 2024-01-15 10:30:00 UTC
    const now_ms: i64 = 1705312200000;
    const result = nextCronTime(now_ms, "0 0 * * *");
    try testing.expect(result != null);
    const dt = epochSecsToCalendar(@divFloor(result.?, 1000));
    try testing.expectEqual(@as(u8, 0), dt.minute);
    try testing.expectEqual(@as(u8, 0), dt.hour);
    // Should be Jan 16 (next day)
    try testing.expectEqual(@as(u8, 16), dt.day);
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
