//! Log formatters for different output formats.
//!
//! Supports:
//! - Text (human readable with colors)
//! - JSON (structured, machine readable)
//! - Compact (minimal for high-throughput)

const std = @import("std");
const Field = @import("field.zig").Field;

/// Log severity levels.
pub const Level = enum(u3) {
    trace = 0,
    debug = 1,
    info = 2,
    warn = 3,
    err = 4,
    fatal = 5,

    pub fn toString(self: Level) []const u8 {
        return switch (self) {
            .trace => "TRACE",
            .debug => "DEBUG",
            .info => "INFO",
            .warn => "WARN",
            .err => "ERROR",
            .fatal => "FATAL",
        };
    }

    pub fn toShort(self: Level) []const u8 {
        return switch (self) {
            .trace => "TRC",
            .debug => "DBG",
            .info => "INF",
            .warn => "WRN",
            .err => "ERR",
            .fatal => "FTL",
        };
    }

    /// Convert from std.log.Level for compatibility.
    pub fn fromStd(level: std.log.Level) Level {
        return switch (level) {
            .debug => .debug,
            .info => .info,
            .warn => .warn,
            .err => .err,
        };
    }

    /// Convert to std.log.Level for compatibility.
    pub fn toStd(self: Level) std.log.Level {
        return switch (self) {
            .trace, .debug => .debug,
            .info => .info,
            .warn => .warn,
            .err, .fatal => .err,
        };
    }

    /// Parse level from string.
    pub fn parse(s: []const u8) ?Level {
        const lower = blk: {
            var buf: [8]u8 = undefined;
            const len = @min(s.len, buf.len);
            for (s[0..len], 0..) |c, i| {
                buf[i] = std.ascii.toLower(c);
            }
            break :blk buf[0..len];
        };

        if (std.mem.eql(u8, lower, "trace") or std.mem.eql(u8, lower, "trc")) return .trace;
        if (std.mem.eql(u8, lower, "debug") or std.mem.eql(u8, lower, "dbg")) return .debug;
        if (std.mem.eql(u8, lower, "info") or std.mem.eql(u8, lower, "inf")) return .info;
        if (std.mem.eql(u8, lower, "warn") or std.mem.eql(u8, lower, "wrn")) return .warn;
        if (std.mem.eql(u8, lower, "error") or std.mem.eql(u8, lower, "err")) return .err;
        if (std.mem.eql(u8, lower, "fatal") or std.mem.eql(u8, lower, "ftl")) return .fatal;
        return null;
    }
};

/// ANSI color codes for terminal output.
pub const Color = struct {
    pub const reset = "\x1b[0m";
    pub const red = "\x1b[31m";
    pub const green = "\x1b[32m";
    pub const yellow = "\x1b[33m";
    pub const blue = "\x1b[34m";
    pub const magenta = "\x1b[35m";
    pub const cyan = "\x1b[36m";
    pub const white = "\x1b[37m";
    pub const dim = "\x1b[90m";
    pub const bold = "\x1b[1m";
    pub const bold_red = "\x1b[1;31m";
    pub const bold_yellow = "\x1b[1;33m";

    pub fn forLevel(level: Level) []const u8 {
        return switch (level) {
            .trace => dim,
            .debug => blue,
            .info => green,
            .warn => yellow,
            .err => red,
            .fatal => bold_red,
        };
    }
};

/// Format configuration for log output.
pub const Format = enum {
    /// Human-readable text with optional colors.
    text,
    /// Structured JSON, one object per line.
    json,
    /// Minimal text format for high-throughput logging.
    compact,

    pub fn parse(s: []const u8) ?Format {
        if (std.mem.eql(u8, s, "text") or std.mem.eql(u8, s, "human")) return .text;
        if (std.mem.eql(u8, s, "json")) return .json;
        if (std.mem.eql(u8, s, "compact") or std.mem.eql(u8, s, "minimal")) return .compact;
        return null;
    }
};

/// Configuration for the text formatter.
pub const TextConfig = struct {
    /// Enable ANSI color codes.
    colors: bool = true,
    /// Show timestamp in output.
    show_timestamp: bool = true,
    /// Use short timestamp format (HH:MM:SS.mmm) vs full ISO8601.
    short_timestamp: bool = true,
    /// Show source location (file:line).
    show_caller: bool = false,
    /// Timestamp format (if custom).
    timestamp_format: ?[]const u8 = null,
};

/// Configuration for the JSON formatter.
pub const JsonConfig = struct {
    /// Pretty print with indentation.
    pretty: bool = false,
    /// Include timestamp in output.
    include_timestamp: bool = true,
    /// Include source location.
    include_caller: bool = false,
    /// Custom timestamp field name.
    timestamp_key: []const u8 = "ts",
    /// Custom level field name.
    level_key: []const u8 = "level",
    /// Custom message field name.
    message_key: []const u8 = "msg",
};

/// Source location info for log entries.
pub const Caller = struct {
    file: []const u8,
    line: u32,
    fn_name: []const u8,
};

/// A log entry ready for formatting.
pub const Entry = struct {
    level: Level,
    message: []const u8,
    timestamp: i128,
    fields: []const Field,
    /// Source location if available.
    caller: ?Caller = null,
};

// =============================================================================
// Text Formatter
// =============================================================================

/// Format an entry as human-readable text.
pub fn formatText(
    entry: *const Entry,
    config: TextConfig,
    writer: anytype,
) !void {
    const colors = config.colors;

    // Timestamp
    if (config.show_timestamp) {
        if (colors) try writer.writeAll(Color.dim);

        if (config.short_timestamp) {
            try writeShortTimestamp(writer, entry.timestamp);
        } else {
            try writeIsoTimestamp(writer, entry.timestamp);
        }

        if (colors) try writer.writeAll(Color.reset);
        try writer.writeByte(' ');
    }

    // Level
    if (colors) try writer.writeAll(Color.forLevel(entry.level));
    try writer.writeAll(entry.level.toShort());
    if (colors) try writer.writeAll(Color.reset);
    try writer.writeByte(' ');

    // Caller (if enabled)
    if (config.show_caller) {
        if (entry.caller) |caller| {
            if (colors) try writer.writeAll(Color.dim);
            try writer.print("({s}:{d}) ", .{ shortenPath(caller.file), caller.line });
            if (colors) try writer.writeAll(Color.reset);
        }
    }

    // Message
    try writer.writeAll(entry.message);

    // Fields
    for (entry.fields) |field| {
        try writer.writeByte(' ');
        if (colors) try writer.writeAll(Color.cyan);
        try writer.writeAll(field.key);
        if (colors) try writer.writeAll(Color.reset);
        try writer.writeByte('=');
        try field.formatValue(writer);
    }

    try writer.writeByte('\n');
}

/// Format an entry as JSON.
pub fn formatJson(
    entry: *const Entry,
    config: JsonConfig,
    writer: anytype,
) !void {
    try writer.writeByte('{');

    var first = true;

    // Timestamp
    if (config.include_timestamp) {
        try writeJsonField(writer, config.timestamp_key, &first);
        try writer.print("{d}", .{entry.timestamp});
    }

    // Level
    try writeJsonField(writer, config.level_key, &first);
    try writer.print("\"{s}\"", .{entry.level.toString()});

    // Message
    try writeJsonField(writer, config.message_key, &first);
    try writer.writeByte('"');
    try writeJsonEscaped(writer, entry.message);
    try writer.writeByte('"');

    // Caller
    if (config.include_caller) {
        if (entry.caller) |caller| {
            try writeJsonField(writer, "caller", &first);
            try writer.print("\"{s}:{d}\"", .{ shortenPath(caller.file), caller.line });
        }
    }

    // Fields
    for (entry.fields) |field| {
        try writeJsonField(writer, field.key, &first);
        try field.formatJsonValue(writer);
    }

    try writer.writeAll("}\n");
}

/// Format an entry in compact format (minimal overhead).
pub fn formatCompact(
    entry: *const Entry,
    writer: anytype,
) !void {
    // Level (single char)
    try writer.writeByte(switch (entry.level) {
        .trace => 'T',
        .debug => 'D',
        .info => 'I',
        .warn => 'W',
        .err => 'E',
        .fatal => 'F',
    });
    try writer.writeByte(' ');

    // Message
    try writer.writeAll(entry.message);

    // Fields (space-separated key=value)
    for (entry.fields) |field| {
        try writer.writeByte(' ');
        try writer.writeAll(field.key);
        try writer.writeByte('=');
        // Compact format uses simple value representation
        switch (field.value) {
            .string => |v| try writer.writeAll(v),
            .int => |v| try writer.print("{d}", .{v}),
            .uint => |v| try writer.print("{d}", .{v}),
            .float => |v| try writer.print("{d:.2}", .{v}),
            .boolean => |v| try writer.writeAll(if (v) "1" else "0"),
            .err => |v| {
                if (v) |e| {
                    try writer.writeAll(@errorName(e));
                } else {
                    try writer.writeByte('-');
                }
            },
            .duration_ns => |v| try writer.print("{d}", .{v}),
            .timestamp => |v| try writer.print("{d}", .{v}),
            .bytes => |v| {
                for (v[0..@min(v.len, 8)]) |b| {
                    try writer.print("{x:0>2}", .{b});
                }
                if (v.len > 8) try writer.writeAll("..");
            },
            .null_val => try writer.writeByte('-'),
        }
    }

    try writer.writeByte('\n');
}

// =============================================================================
// Helper Functions
// =============================================================================

pub fn writeShortTimestamp(writer: anytype, timestamp_ns: i128) !void {
    const ts_i64: i64 = @intCast(@mod(timestamp_ns, std.math.maxInt(i64)));
    const epoch_seconds: u64 = @intCast(@divFloor(ts_i64, std.time.ns_per_s));
    const nanos: u64 = @intCast(@mod(ts_i64, std.time.ns_per_s));
    const millis = nanos / std.time.ns_per_ms;

    const epoch = std.time.epoch.EpochSeconds{ .secs = epoch_seconds };
    const day_secs = epoch.getDaySeconds();

    try writer.print("{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}", .{
        day_secs.getHoursIntoDay(),
        day_secs.getMinutesIntoHour(),
        day_secs.getSecondsIntoMinute(),
        millis,
    });
}

fn writeIsoTimestamp(writer: anytype, timestamp_ns: i128) !void {
    const ts_i64: i64 = @intCast(@mod(timestamp_ns, std.math.maxInt(i64)));
    const epoch_seconds: u64 = @intCast(@divFloor(ts_i64, std.time.ns_per_s));
    const nanos: u64 = @intCast(@mod(ts_i64, std.time.ns_per_s));
    const millis = nanos / std.time.ns_per_ms;

    const epoch = std.time.epoch.EpochSeconds{ .secs = epoch_seconds };
    const day = epoch.getEpochDay();
    const year_day = day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_secs = epoch.getDaySeconds();

    try writer.print("{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_secs.getHoursIntoDay(),
        day_secs.getMinutesIntoHour(),
        day_secs.getSecondsIntoMinute(),
        millis,
    });
}

fn writeJsonField(writer: anytype, key: []const u8, first: *bool) !void {
    if (!first.*) {
        try writer.writeByte(',');
    }
    first.* = false;
    try writer.writeByte('"');
    try writer.writeAll(key);
    try writer.writeAll("\":");
}

pub fn writeJsonEscaped(writer: anytype, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => {
                if (c < 0x20) {
                    try writer.print("\\u{x:0>4}", .{c});
                } else {
                    try writer.writeByte(c);
                }
            },
        }
    }
}

pub fn shortenPath(path: []const u8) []const u8 {
    // Return just the filename
    if (std.mem.lastIndexOfScalar(u8, path, '/')) |idx| {
        return path[idx + 1 ..];
    }
    return path;
}

// =============================================================================
// Tests
// =============================================================================

test "Level.parse" {
    try std.testing.expectEqual(Level.debug, Level.parse("debug").?);
    try std.testing.expectEqual(Level.info, Level.parse("INFO").?);
    try std.testing.expectEqual(Level.warn, Level.parse("WRN").?);
    try std.testing.expectEqual(Level.err, Level.parse("error").?);
    try std.testing.expect(Level.parse("invalid") == null);
}

test "formatText basic" {
    var buf: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const writer = fbs.writer();

    const fields = [_]Field{
        Field.str("user", "alice"),
        Field.int("count", 42),
    };

    const entry = Entry{
        .level = .info,
        .message = "test message",
        .timestamp = 1702500000000000000, // 2023-12-13T...
        .fields = &fields,
    };

    try formatText(&entry, .{ .colors = false, .short_timestamp = true }, writer);

    const output = fbs.getWritten();
    try std.testing.expect(std.mem.indexOf(u8, output, "INF") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "test message") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "user=") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "count=42") != null);
}

test "formatJson basic" {
    var buf: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const writer = fbs.writer();

    const fields = [_]Field{
        Field.str("user", "alice"),
        Field.int("count", 42),
    };

    const entry = Entry{
        .level = .info,
        .message = "test message",
        .timestamp = 1702500000000000000,
        .fields = &fields,
    };

    try formatJson(&entry, .{}, writer);

    const output = fbs.getWritten();
    try std.testing.expect(std.mem.indexOf(u8, output, "\"level\":\"INFO\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"msg\":\"test message\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"user\":\"alice\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"count\":42") != null);
}
