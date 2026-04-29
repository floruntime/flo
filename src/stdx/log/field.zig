//! Structured logging fields for type-safe log attributes.
//!
//! Fields provide a type-safe way to attach structured data to log entries.
//! Inspired by zap/zerolog field-based logging patterns.
//!
//! Usage:
//!   log.info("user logged in", .{
//!       Field.str("user_id", user.id),
//!       Field.int("session_duration", duration_ms),
//!       Field.err(some_error),
//!   });

const std = @import("std");

/// A structured log field with key and typed value.
pub const Field = struct {
    key: []const u8,
    value: Value,

    pub const Value = union(enum) {
        string: []const u8,
        int: i64,
        uint: u64,
        float: f64,
        boolean: bool,
        err: ?anyerror,
        duration_ns: i64,
        timestamp: i128,
        /// Raw bytes (hex encoded in output)
        bytes: []const u8,
        /// Null/empty value
        null_val: void,
    };

    // =========================================================================
    // Field Constructors
    // =========================================================================

    /// Create a string field.
    pub fn str(key: []const u8, value: []const u8) Field {
        return .{ .key = key, .value = .{ .string = value } };
    }

    /// Create an integer field.
    pub fn int(key: []const u8, value: anytype) Field {
        const T = @TypeOf(value);
        return .{ .key = key, .value = .{ .int = switch (@typeInfo(T)) {
            .int => |info| if (info.signedness == .signed)
                @as(i64, value)
            else
                @as(i64, @intCast(value)),
            .comptime_int => @as(i64, value),
            else => @compileError("Expected integer type"),
        } } };
    }

    /// Create an unsigned integer field.
    pub fn uint(key: []const u8, value: anytype) Field {
        const T = @TypeOf(value);
        return .{ .key = key, .value = .{ .uint = switch (@typeInfo(T)) {
            .int => @as(u64, @intCast(value)),
            .comptime_int => @as(u64, value),
            else => @compileError("Expected integer type"),
        } } };
    }

    /// Create a float field.
    pub fn float(key: []const u8, value: anytype) Field {
        return .{ .key = key, .value = .{ .float = @as(f64, @floatCast(value)) } };
    }

    /// Create a boolean field.
    pub fn boolean(key: []const u8, value: bool) Field {
        return .{ .key = key, .value = .{ .boolean = value } };
    }

    /// Create an error field.
    pub fn err(value: anyerror) Field {
        return .{ .key = "error", .value = .{ .err = value } };
    }

    /// Create an optional error field.
    pub fn errOrNull(value: ?anyerror) Field {
        return .{ .key = "error", .value = .{ .err = value } };
    }

    /// Create a duration field (nanoseconds).
    pub fn duration(key: []const u8, ns: i64) Field {
        return .{ .key = key, .value = .{ .duration_ns = ns } };
    }

    /// Create a duration field from a stdx.time.Timer.
    pub fn durationFrom(key: []const u8, timer: @import("../time.zig").Timer) Field {
        return duration(key, timer.read());
    }

    /// Create a timestamp field (unix epoch nanoseconds).
    pub fn timestamp(key: []const u8, ts: i128) Field {
        return .{ .key = key, .value = .{ .timestamp = ts } };
    }

    /// Create a timestamp field from current time.
    pub fn now(key: []const u8) Field {
        return timestamp(key, @import("../time.zig").nanoTimestamp());
    }

    /// Create a binary/bytes field (will be hex encoded).
    pub fn bytes(key: []const u8, data: []const u8) Field {
        return .{ .key = key, .value = .{ .bytes = data } };
    }

    /// Create a null field.
    pub fn nil(key: []const u8) Field {
        return .{ .key = key, .value = .{ .null_val = {} } };
    }

    // =========================================================================
    // Common Field Aliases
    // =========================================================================

    /// Create a component field for tagging log source.
    pub fn component(value: []const u8) Field {
        return str("component", value);
    }

    /// Create a request ID field.
    pub fn requestId(value: []const u8) Field {
        return str("request_id", value);
    }

    /// Create a trace ID field.
    pub fn traceId(value: []const u8) Field {
        return str("trace_id", value);
    }

    /// Create a span ID field.
    pub fn spanId(value: []const u8) Field {
        return str("span_id", value);
    }

    /// Create a shard/core ID field.
    pub fn shard(id: anytype) Field {
        return int("shard", id);
    }

    /// Create an operation field.
    pub fn operation(value: []const u8) Field {
        return str("op", value);
    }

    /// Create a count field.
    pub fn count(key: []const u8, value: anytype) Field {
        return uint(key, value);
    }

    /// Create a size field (bytes).
    pub fn size(key: []const u8, value: anytype) Field {
        return uint(key, value);
    }

    // =========================================================================
    // Value Formatting
    // =========================================================================

    /// Format the field value as a string.
    pub fn formatValue(self: *const Field, writer: anytype) !void {
        switch (self.value) {
            .string => |v| try writer.print("\"{s}\"", .{v}),
            .int => |v| try writer.print("{d}", .{v}),
            .uint => |v| try writer.print("{d}", .{v}),
            .float => |v| try writer.print("{d:.6}", .{v}),
            .boolean => |v| try writer.print("{}", .{v}),
            .err => |v| {
                if (v) |e| {
                    try writer.print("\"{s}\"", .{@errorName(e)});
                } else {
                    try writer.writeAll("null");
                }
            },
            .duration_ns => |v| {
                // Format as human readable duration
                if (v < 1000) {
                    try writer.print("{d}ns", .{v});
                } else if (v < 1_000_000) {
                    try writer.print("{d:.2}µs", .{@as(f64, @floatFromInt(v)) / 1000.0});
                } else if (v < 1_000_000_000) {
                    try writer.print("{d:.2}ms", .{@as(f64, @floatFromInt(v)) / 1_000_000.0});
                } else {
                    try writer.print("{d:.2}s", .{@as(f64, @floatFromInt(v)) / 1_000_000_000.0});
                }
            },
            .timestamp => |v| {
                // Format as ISO8601
                const ts_i64: i64 = @intCast(@mod(v, std.math.maxInt(i64)));
                const epoch_seconds: u64 = @intCast(@divFloor(ts_i64, std.time.ns_per_s));
                const epoch = std.time.epoch.EpochSeconds{ .secs = epoch_seconds };
                const day = epoch.getEpochDay();
                const year_day = day.calculateYearDay();
                const month_day = year_day.calculateMonthDay();
                const day_secs = epoch.getDaySeconds();

                try writer.print("{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
                    year_day.year,
                    month_day.month.numeric(),
                    month_day.day_index + 1,
                    day_secs.getHoursIntoDay(),
                    day_secs.getMinutesIntoHour(),
                    day_secs.getSecondsIntoMinute(),
                });
            },
            .bytes => |v| {
                try writer.writeAll("0x");
                for (v) |b| {
                    try writer.print("{x:0>2}", .{b});
                }
            },
            .null_val => try writer.writeAll("null"),
        }
    }

    /// Format the field value for JSON output.
    pub fn formatJsonValue(self: *const Field, writer: anytype) !void {
        switch (self.value) {
            .string => |v| {
                try writer.writeByte('"');
                try writeJsonEscaped(writer, v);
                try writer.writeByte('"');
            },
            .int => |v| try writer.print("{d}", .{v}),
            .uint => |v| try writer.print("{d}", .{v}),
            .float => |v| try writer.print("{d}", .{v}),
            .boolean => |v| try writer.print("{}", .{v}),
            .err => |v| {
                if (v) |e| {
                    try writer.print("\"{s}\"", .{@errorName(e)});
                } else {
                    try writer.writeAll("null");
                }
            },
            .duration_ns => |v| try writer.print("{d}", .{v}),
            .timestamp => |v| try writer.print("{d}", .{v}),
            .bytes => |v| {
                try writer.writeAll("\"0x");
                for (v) |b| {
                    try writer.print("{x:0>2}", .{b});
                }
                try writer.writeByte('"');
            },
            .null_val => try writer.writeAll("null"),
        }
    }

    /// Format the field value for compact output (minimal overhead).
    pub fn formatCompactValue(self: *const Field, writer: anytype) !void {
        switch (self.value) {
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
};

/// Write a string with JSON escaping.
fn writeJsonEscaped(writer: anytype, s: []const u8) !void {
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

// =============================================================================
// Tests
// =============================================================================

test "Field.str" {
    const f = Field.str("name", "value");
    try std.testing.expectEqualStrings("name", f.key);
    try std.testing.expectEqualStrings("value", f.value.string);
}

test "Field.int" {
    const f = Field.int("count", @as(i32, -42));
    try std.testing.expectEqual(@as(i64, -42), f.value.int);
}

test "Field.uint" {
    const f = Field.uint("size", @as(u32, 1024));
    try std.testing.expectEqual(@as(u64, 1024), f.value.uint);
}

test "Field.err" {
    const f = Field.err(error.OutOfMemory);
    try std.testing.expectEqualStrings("error", f.key);
    try std.testing.expectEqual(error.OutOfMemory, f.value.err.?);
}

test "Field.component" {
    const f = Field.component("storage");
    try std.testing.expectEqualStrings("component", f.key);
    try std.testing.expectEqualStrings("storage", f.value.string);
}

test "Field.formatValue duration" {
    var buf: [64]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    const f = Field.duration("elapsed", 1_500_000); // 1.5ms
    try f.formatValue(writer);

    const output = fbs.buffered();
    try std.testing.expectEqualStrings("1.50ms", output);
}

test "Field.durationFrom with Timer" {
    const time = @import("../time.zig");
    const timer = time.Timer.start();
    time.sleep(1 * std.time.ns_per_ms); // sleep 1ms
    const f = Field.durationFrom("elapsed", timer);
    try std.testing.expectEqualStrings("elapsed", f.key);
    try std.testing.expect(f.value.duration_ns >= 1_000_000); // at least 1ms
}
