//! InfluxDB Line Protocol Parser
//!
//! Parses the InfluxDB line protocol format for batch ingestion:
//!   measurement,tag1=val1,tag2=val2 field1=1.0,field2=2.0 timestamp
//!
//! Grammar:
//!   line        = measurement ("," tag_set)? " " field_set (" " timestamp)? "\n"
//!   measurement = non_ws_string
//!   tag_set     = tag ("," tag)*
//!   tag         = tag_key "=" tag_value
//!   field_set   = field ("," field)*
//!   field       = field_key "=" field_value
//!   field_value = float | integer "i" | string | boolean
//!   timestamp   = integer (units determined by Precision parameter)
//!
//! This parser supports the standard InfluxDB line protocol with these notes:
//! - Only f64 field values are supported (integer "i" suffix is parsed and converted)
//! - String fields are rejected (not supported in Flo-TS v1)
//! - Boolean fields are converted to 0.0/1.0
//! - Timestamps are converted to milliseconds using the caller-specified Precision
//!
//! ## Precision
//!
//! Matches InfluxDB's `?precision=` query parameter. Callers specify the unit of
//! the integer timestamps in the payload:
//!
//!   | Precision | Wire value | Conversion |
//!   |-----------|-------------------------------------|----------------------------|
//!   | ns        | nanoseconds  (InfluxDB default)     | ÷ 1,000,000                |
//!   | us / µs   | microseconds                        | ÷ 1,000                    |
//!   | ms        | milliseconds (Flo native, **default**)| identity                   |
//!   | s         | seconds                             | × 1,000                    |
//!
//! Default is `.ms` (Flo-native). Use `.ns` when accepting InfluxDB wire protocol
//! payloads where timestamps are in nanoseconds.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Timestamp precision — determines how the integer timestamp in the wire
/// format is interpreted. Matches InfluxDB's `?precision=` parameter.
///
/// Flo internally stores all timestamps as milliseconds.
/// The parser converts incoming integers to milliseconds using this precision.
pub const Precision = enum(u8) {
    /// Nanoseconds (InfluxDB default). 1 ms = 1,000,000 ns.
    ns = 0,
    /// Microseconds. 1 ms = 1,000 µs.
    us = 1,
    /// Milliseconds — Flo's native resolution. No conversion needed.
    ms = 2,
    /// Seconds. 1 s = 1,000 ms.
    s = 3,

    /// Convert a raw timestamp integer to milliseconds.
    pub fn toMillis(self: Precision, raw: i64) i64 {
        return switch (self) {
            .ns => @divFloor(raw, 1_000_000),
            .us => @divFloor(raw, 1_000),
            .ms => raw,
            .s => raw *| 1_000, // saturating multiply to avoid overflow
        };
    }

    /// Parse a precision string (e.g. from HTTP query param or CLI flag).
    /// Accepts: "ns", "us", "µs", "ms", "s". Case-insensitive for ASCII.
    pub fn fromString(s: []const u8) ?Precision {
        if (std.ascii.eqlIgnoreCase(s, "ns")) return .ns;
        if (std.ascii.eqlIgnoreCase(s, "us")) return .us;
        if (std.mem.eql(u8, s, "µs")) return .us; // UTF-8 micro sign
        if (std.ascii.eqlIgnoreCase(s, "ms")) return .ms;
        if (std.ascii.eqlIgnoreCase(s, "s")) return .s;
        return null;
    }
};

/// A parsed tag key-value pair
pub const ParsedTag = struct {
    key: []const u8,
    value: []const u8,
};

/// A parsed field key-value pair
pub const ParsedField = struct {
    name: []const u8,
    value: f64,
};

/// A fully parsed line protocol entry (one line)
pub const ParsedLine = struct {
    measurement: []const u8,
    tags: []ParsedTag,
    fields: []ParsedField,
    /// Timestamp in milliseconds (0 = use server time)
    timestamp_ms: i64,
};

/// Result of parsing multiple lines
pub const ParseResult = struct {
    lines: []ParsedLine,
    errors: []ParseError,

    pub fn deinit(self: ParseResult, allocator: Allocator) void {
        for (self.lines) |_| {
            // Tags and fields point into original input or sliced allocations
        }
        allocator.free(self.lines);
        allocator.free(self.errors);
    }
};

pub const ParseError = struct {
    line_number: u32,
    message: []const u8,
};

/// Parse a single line of InfluxDB line protocol.
///
/// The returned ParsedLine contains slices that reference the input `line` buffer.
/// Caller must NOT free or modify `line` while using the ParsedLine.
///
/// `precision` controls how the trailing integer timestamp is interpreted.
/// Default is `.ms` (Flo-native milliseconds). Pass `.ns` when accepting
/// InfluxDB wire protocol payloads.
pub fn parseLine(line: []const u8, allocator: Allocator) !ParsedLine {
    return parseLineWithPrecision(line, .ms, allocator);
}

/// Parse a single line with explicit timestamp precision.
pub fn parseLineWithPrecision(line: []const u8, precision: Precision, allocator: Allocator) !ParsedLine {
    var input = line;

    // Skip leading whitespace
    while (input.len > 0 and (input[0] == ' ' or input[0] == '\t')) {
        input = input[1..];
    }

    // Skip empty lines and comments
    if (input.len == 0 or input[0] == '#') {
        return error.EmptyLine;
    }

    // Parse measurement name (up to first comma or space)
    const measurement_end = findMeasurementEnd(input) orelse return error.InvalidMeasurement;
    if (measurement_end == 0) return error.InvalidMeasurement;
    const measurement = input[0..measurement_end];
    input = input[measurement_end..];

    // Parse tags (optional, starts with comma)
    var tags: std.ArrayListUnmanaged(ParsedTag) = .empty;
    if (input.len > 0 and input[0] == ',') {
        input = input[1..]; // skip comma
        while (true) {
            const tag = try parseTag(input);
            try tags.append(allocator, tag.tag);
            input = input[tag.consumed..];
            if (input.len > 0 and input[0] == ',') {
                input = input[1..];
            } else {
                break;
            }
        }
    }

    // Expect space between tag set and field set
    if (input.len == 0 or input[0] != ' ') {
        tags.deinit(allocator);
        return error.MissingFields;
    }
    input = input[1..]; // skip space

    // Skip extra spaces
    while (input.len > 0 and input[0] == ' ') {
        input = input[1..];
    }

    // Parse fields (required, at least one)
    var fields: std.ArrayListUnmanaged(ParsedField) = .empty;
    while (true) {
        const field = parseField(input) catch |err| {
            fields.deinit(allocator);
            tags.deinit(allocator);
            return err;
        };
        try fields.append(allocator, field.field);
        input = input[field.consumed..];
        if (input.len > 0 and input[0] == ',') {
            input = input[1..];
        } else {
            break;
        }
    }

    if (fields.items.len == 0) {
        fields.deinit(allocator);
        tags.deinit(allocator);
        return error.MissingFields;
    }

    // Parse timestamp (optional — units determined by precision parameter)
    var timestamp_ms: i64 = 0;
    if (input.len > 0 and input[0] == ' ') {
        input = input[1..]; // skip space
        // Skip extra spaces
        while (input.len > 0 and input[0] == ' ') {
            input = input[1..];
        }
        if (input.len > 0) {
            timestamp_ms = parseTimestamp(input, precision) catch 0;
        }
    }

    return .{
        .measurement = measurement,
        .tags = try tags.toOwnedSlice(allocator),
        .fields = try fields.toOwnedSlice(allocator),
        .timestamp_ms = timestamp_ms,
    };
}

/// Parse multiple lines of line protocol (millisecond precision, Flo-native).
pub fn parseLines(input: []const u8, allocator: Allocator) !ParseResult {
    return parseLinesWithPrecision(input, .ms, allocator);
}

/// Parse multiple lines with explicit timestamp precision.
pub fn parseLinesWithPrecision(input: []const u8, precision: Precision, allocator: Allocator) !ParseResult {
    var lines: std.ArrayListUnmanaged(ParsedLine) = .empty;
    var errors: std.ArrayListUnmanaged(ParseError) = .empty;

    var line_number: u32 = 0;
    var iter = std.mem.splitScalar(u8, input, '\n');
    while (iter.next()) |raw_line| {
        line_number += 1;

        // Strip carriage return
        const line = if (raw_line.len > 0 and raw_line[raw_line.len - 1] == '\r')
            raw_line[0 .. raw_line.len - 1]
        else
            raw_line;

        // Skip empty lines and comments
        if (line.len == 0) continue;
        const trimmed = std.mem.trimStart(u8, line, " \t");
        if (trimmed.len == 0 or trimmed[0] == '#') continue;

        const parsed = parseLineWithPrecision(line, precision, allocator) catch {
            try errors.append(allocator, .{
                .line_number = line_number,
                .message = "parse error",
            });
            continue;
        };
        try lines.append(allocator, parsed);
    }

    return .{
        .lines = try lines.toOwnedSlice(allocator),
        .errors = try errors.toOwnedSlice(allocator),
    };
}

/// Free a ParsedLine's owned memory (tags and fields slices)
pub fn freeParsedLine(line: ParsedLine, allocator: Allocator) void {
    allocator.free(line.tags);
    allocator.free(line.fields);
}

// ============================================================================
// Internal Parsing Helpers
// ============================================================================

/// Find end of measurement name (first unescaped comma or space)
fn findMeasurementEnd(input: []const u8) ?usize {
    var i: usize = 0;
    while (i < input.len) : (i += 1) {
        if (input[i] == '\\' and i + 1 < input.len) {
            i += 1; // skip escaped char
            continue;
        }
        if (input[i] == ',' or input[i] == ' ') return i;
    }
    return null; // No space found — missing fields
}

const TagParseResult = struct {
    tag: ParsedTag,
    consumed: usize,
};

fn parseTag(input: []const u8) !TagParseResult {
    // Find '=' separator
    const eq_pos = std.mem.indexOfScalar(u8, input, '=') orelse return error.InvalidTag;
    if (eq_pos == 0) return error.InvalidTag;

    const key = input[0..eq_pos];

    // Find end of value (comma or space)
    var end: usize = eq_pos + 1;
    while (end < input.len) : (end += 1) {
        if (input[end] == ',' or input[end] == ' ') break;
    }

    if (end == eq_pos + 1) return error.InvalidTag; // empty value
    const value = input[eq_pos + 1 .. end];

    return .{
        .tag = .{ .key = key, .value = value },
        .consumed = end,
    };
}

const FieldParseResult = struct {
    field: ParsedField,
    consumed: usize,
};

fn parseField(input: []const u8) !FieldParseResult {
    // Find '=' separator
    const eq_pos = std.mem.indexOfScalar(u8, input, '=') orelse return error.InvalidField;
    if (eq_pos == 0) return error.InvalidField;

    const name = input[0..eq_pos];

    // Find end of value (comma, space, or end)
    var end: usize = eq_pos + 1;

    // Handle quoted strings
    if (end < input.len and input[end] == '"') {
        end += 1;
        while (end < input.len) : (end += 1) {
            if (input[end] == '\\' and end + 1 < input.len) {
                end += 1;
                continue;
            }
            if (input[end] == '"') {
                end += 1;
                break;
            }
        }
        return error.StringFieldsNotSupported; // Flo-TS v1: no string fields
    }

    while (end < input.len) : (end += 1) {
        if (input[end] == ',' or input[end] == ' ' or input[end] == '\n') break;
    }

    if (end == eq_pos + 1) return error.InvalidField; // empty value
    const value_str = input[eq_pos + 1 .. end];

    // Parse field value
    const value = parseFieldValue(value_str) orelse return error.InvalidFieldValue;

    return .{
        .field = .{ .name = name, .value = value },
        .consumed = end,
    };
}

fn parseFieldValue(s: []const u8) ?f64 {
    if (s.len == 0) return null;

    // Boolean
    if (std.mem.eql(u8, s, "true") or std.mem.eql(u8, s, "True") or std.mem.eql(u8, s, "TRUE") or std.mem.eql(u8, s, "t") or std.mem.eql(u8, s, "T")) {
        return 1.0;
    }
    if (std.mem.eql(u8, s, "false") or std.mem.eql(u8, s, "False") or std.mem.eql(u8, s, "FALSE") or std.mem.eql(u8, s, "f") or std.mem.eql(u8, s, "F")) {
        return 0.0;
    }

    // Integer (suffix 'i' or 'u')
    if (s[s.len - 1] == 'i' or s[s.len - 1] == 'u') {
        const num_str = s[0 .. s.len - 1];
        const int_val = std.fmt.parseInt(i64, num_str, 10) catch return null;
        return @floatFromInt(int_val);
    }

    // Float
    return std.fmt.parseFloat(f64, s) catch null;
}

fn parseTimestamp(input: []const u8, precision: Precision) !i64 {
    // Find end of numeric portion
    var end: usize = 0;
    while (end < input.len and (input[end] >= '0' and input[end] <= '9')) : (end += 1) {}

    if (end == 0) return error.InvalidTimestamp;
    const raw = std.fmt.parseInt(i64, input[0..end], 10) catch return error.InvalidTimestamp;

    // Convert to milliseconds using caller-specified precision
    return precision.toMillis(raw);
}

// ============================================================================
// Tests
// ============================================================================

test "parseLine - basic with tags and fields (ms default)" {
    const allocator = std.testing.allocator;
    const line = "cpu,host=server01,region=us-east usage_idle=98.5,usage_user=1.5 1465839830100";

    const result = try parseLine(line, allocator);
    defer freeParsedLine(result, allocator);

    try std.testing.expectEqualStrings("cpu", result.measurement);
    try std.testing.expectEqual(@as(usize, 2), result.tags.len);
    try std.testing.expectEqualStrings("host", result.tags[0].key);
    try std.testing.expectEqualStrings("server01", result.tags[0].value);
    try std.testing.expectEqualStrings("region", result.tags[1].key);
    try std.testing.expectEqualStrings("us-east", result.tags[1].value);
    try std.testing.expectEqual(@as(usize, 2), result.fields.len);
    try std.testing.expectEqualStrings("usage_idle", result.fields[0].name);
    try std.testing.expectEqual(@as(f64, 98.5), result.fields[0].value);
    try std.testing.expectEqualStrings("usage_user", result.fields[1].name);
    try std.testing.expectEqual(@as(f64, 1.5), result.fields[1].value);
    // Timestamp in ms (Flo-native default)
    try std.testing.expectEqual(@as(i64, 1465839830100), result.timestamp_ms);
}

test "parseLine - InfluxDB ns compat via parseLineWithPrecision" {
    const allocator = std.testing.allocator;
    const line = "cpu,host=server01 usage_idle=98.5 1465839830100400200";

    const result = try parseLineWithPrecision(line, .ns, allocator);
    defer freeParsedLine(result, allocator);

    // 1465839830100400200 ns → 1465839830100 ms
    try std.testing.expectEqual(@as(i64, 1465839830100), result.timestamp_ms);
}

test "parseLine - no tags" {
    const allocator = std.testing.allocator;
    const line = "temperature value=72.3";

    const result = try parseLine(line, allocator);
    defer freeParsedLine(result, allocator);

    try std.testing.expectEqualStrings("temperature", result.measurement);
    try std.testing.expectEqual(@as(usize, 0), result.tags.len);
    try std.testing.expectEqual(@as(usize, 1), result.fields.len);
    try std.testing.expectEqualStrings("value", result.fields[0].name);
    try std.testing.expectEqual(@as(f64, 72.3), result.fields[0].value);
    try std.testing.expectEqual(@as(i64, 0), result.timestamp_ms);
}

test "parseLine - integer field" {
    const allocator = std.testing.allocator;
    const line = "events count=42i";

    const result = try parseLine(line, allocator);
    defer freeParsedLine(result, allocator);

    try std.testing.expectEqual(@as(f64, 42.0), result.fields[0].value);
}

test "parseLine - boolean field" {
    const allocator = std.testing.allocator;
    const line = "status healthy=true";

    const result = try parseLine(line, allocator);
    defer freeParsedLine(result, allocator);

    try std.testing.expectEqual(@as(f64, 1.0), result.fields[0].value);
}

test "parseLines - multi-line (ms default)" {
    const allocator = std.testing.allocator;
    const input =
        \\# Comment line
        \\cpu,host=a idle=99.0 1000
        \\cpu,host=b idle=98.0 2000
        \\
    ;

    const result = try parseLines(input, allocator);
    defer {
        for (result.lines) |line| {
            freeParsedLine(line, allocator);
        }
        result.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 2), result.lines.len);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);
    try std.testing.expectEqualStrings("a", result.lines[0].tags[0].value);
    try std.testing.expectEqualStrings("b", result.lines[1].tags[0].value);
    // With .ms default, values are used as-is
    try std.testing.expectEqual(@as(i64, 1000), result.lines[0].timestamp_ms);
    try std.testing.expectEqual(@as(i64, 2000), result.lines[1].timestamp_ms);
}

test "parseLine - string field rejected" {
    const allocator = std.testing.allocator;
    const line = "events message=\"hello world\"";

    const err = parseLine(line, allocator);
    try std.testing.expectError(error.StringFieldsNotSupported, err);
}

// ============================================================================
// Precision Tests
// ============================================================================

test "Precision.toMillis - nanoseconds" {
    const p = Precision.ns;
    try std.testing.expectEqual(@as(i64, 1465839830100), p.toMillis(1465839830100400200));
    try std.testing.expectEqual(@as(i64, 0), p.toMillis(0));
    try std.testing.expectEqual(@as(i64, 1), p.toMillis(1_500_000)); // 1.5 ms → 1 (floor)
}

test "Precision.toMillis - microseconds" {
    const p = Precision.us;
    try std.testing.expectEqual(@as(i64, 1465839830100), p.toMillis(1465839830100400));
    try std.testing.expectEqual(@as(i64, 1000), p.toMillis(1_000_000));
}

test "Precision.toMillis - milliseconds (identity)" {
    const p = Precision.ms;
    try std.testing.expectEqual(@as(i64, 1465839830100), p.toMillis(1465839830100));
    try std.testing.expectEqual(@as(i64, 42), p.toMillis(42));
}

test "Precision.toMillis - seconds" {
    const p = Precision.s;
    try std.testing.expectEqual(@as(i64, 1465839830000), p.toMillis(1465839830));
    try std.testing.expectEqual(@as(i64, 1000), p.toMillis(1));
}

test "Precision.fromString" {
    try std.testing.expectEqual(Precision.ns, Precision.fromString("ns").?);
    try std.testing.expectEqual(Precision.ns, Precision.fromString("NS").?);
    try std.testing.expectEqual(Precision.us, Precision.fromString("us").?);
    try std.testing.expectEqual(Precision.us, Precision.fromString("µs").?);
    try std.testing.expectEqual(Precision.ms, Precision.fromString("ms").?);
    try std.testing.expectEqual(Precision.s, Precision.fromString("s").?);
    try std.testing.expect(Precision.fromString("invalid") == null);
}

test "parseLineWithPrecision - millisecond precision (no conversion)" {
    const allocator = std.testing.allocator;
    const line = "cpu,host=a idle=99.0 1465839830100";

    const result = try parseLineWithPrecision(line, .ms, allocator);
    defer freeParsedLine(result, allocator);

    try std.testing.expectEqual(@as(i64, 1465839830100), result.timestamp_ms);
}

test "parseLineWithPrecision - second precision" {
    const allocator = std.testing.allocator;
    const line = "cpu,host=a idle=99.0 1465839830";

    const result = try parseLineWithPrecision(line, .s, allocator);
    defer freeParsedLine(result, allocator);

    try std.testing.expectEqual(@as(i64, 1465839830000), result.timestamp_ms);
}

test "parseLineWithPrecision - microsecond precision" {
    const allocator = std.testing.allocator;
    const line = "cpu,host=a idle=99.0 1465839830100400";

    const result = try parseLineWithPrecision(line, .us, allocator);
    defer freeParsedLine(result, allocator);

    try std.testing.expectEqual(@as(i64, 1465839830100), result.timestamp_ms);
}

test "parseLinesWithPrecision - millisecond batch" {
    const allocator = std.testing.allocator;
    const input =
        \\cpu,host=a idle=99.0 1000
        \\cpu,host=b idle=98.0 2000
        \\
    ;

    const result = try parseLinesWithPrecision(input, .ms, allocator);
    defer {
        for (result.lines) |line| {
            freeParsedLine(line, allocator);
        }
        result.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 2), result.lines.len);
    // With .ms precision, values are used as-is
    try std.testing.expectEqual(@as(i64, 1000), result.lines[0].timestamp_ms);
    try std.testing.expectEqual(@as(i64, 2000), result.lines[1].timestamp_ms);
}
