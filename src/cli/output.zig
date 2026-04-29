//! Output Utilities - Formatting helpers for CLI output
//!
//! Provides utilities for consistent CLI output formatting:
//! - Dynamic tables with auto-width columns
//! - Raw list output (one item per line)
//! - JSON formatting
//! - Key-value pair formatting
//!
//! ## Example
//!
//! ```zig
//! const output = @import("output.zig");
//!
//! // Table output
//! var table = output.Table.init(allocator);
//! defer table.deinit();
//! try table.addColumn("NAME", .left);
//! try table.addColumn("STATUS", .left);
//! try table.addColumn("AGE", .right);
//! try table.addRow(&.{"my-stream", "active", "5d"});
//! try table.addRow(&.{"other-stream", "idle", "2h"});
//! table.print(ctx);
//!
//! // JSON output (uses central util/json.zig)
//! output.Json.printPretty(ctx, allocator, my_struct);
//!
//! // Raw list
//! output.printList(ctx, items);
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("commander/mod.zig");
const Context = commander.Context;
const wire_mod = @import("../util/wire.zig");
const WireReader = wire_mod.WireReader;

/// Central JSON utilities - re-exported from util/json.zig
pub const Json = @import("../util/json.zig");

/// Output format types
pub const Format = enum {
    table,
    json,
    raw,

    pub fn fromString(s: []const u8) Format {
        if (std.mem.eql(u8, s, "json")) return .json;
        if (std.mem.eql(u8, s, "raw")) return .raw;
        return .table; // default
    }
};

/// Get the output format from the global --output / -o flag.
pub fn getFormat(ctx: *Context) Format {
    if (ctx.flagChanged("output")) {
        return Format.fromString(ctx.getString("output") orelse "table");
    }
    return .table;
}

/// Check if --verbose was set globally.
pub fn isVerbose(ctx: *Context) bool {
    return ctx.getBool("verbose");
}

/// Column alignment
pub const Alignment = enum {
    left,
    right,
    center,
};

/// Column definition
pub const Column = struct {
    header: []const u8,
    alignment: Alignment,
    width: usize, // computed max width
};

/// Dynamic table builder and printer
pub const Table = struct {
    allocator: Allocator,
    columns: std.ArrayList(Column),
    rows: std.ArrayList([]const []const u8),
    show_headers: bool = true,
    column_separator: []const u8 = "  ",
    header_separator: bool = false,

    pub fn init(allocator: Allocator) Table {
        return .{
            .allocator = allocator,
            .columns = .empty,
            .rows = .empty,
        };
    }

    pub fn deinit(self: *Table) void {
        for (self.rows.items) |row| {
            self.allocator.free(row);
        }
        self.rows.deinit(self.allocator);
        self.columns.deinit(self.allocator);
    }

    /// Add a column definition
    pub fn addColumn(self: *Table, header: []const u8, alignment: Alignment) !void {
        try self.columns.append(self.allocator, .{
            .header = header,
            .alignment = alignment,
            .width = header.len,
        });
    }

    /// Add a row of values
    pub fn addRow(self: *Table, values: []const []const u8) !void {
        if (values.len != self.columns.items.len) {
            return error.ColumnCountMismatch;
        }

        // Update column widths
        for (values, 0..) |val, i| {
            if (val.len > self.columns.items[i].width) {
                self.columns.items[i].width = val.len;
            }
        }

        // Store row (make a copy of the slice)
        const row_copy = try self.allocator.dupe([]const u8, values);
        try self.rows.append(self.allocator, row_copy);
    }

    /// Configure to hide headers
    pub fn hideHeaders(self: *Table) *Table {
        self.show_headers = false;
        return self;
    }

    /// Configure header separator line
    pub fn withHeaderSeparator(self: *Table) *Table {
        self.header_separator = true;
        return self;
    }

    /// Set column separator string
    pub fn setSeparator(self: *Table, sep: []const u8) *Table {
        self.column_separator = sep;
        return self;
    }

    /// Print the table to context
    pub fn print(self: *Table, ctx: *Context) void {
        // Print header
        if (self.show_headers) {
            const headers = self.getHeaders();
            defer self.allocator.free(headers);
            self.printRow(ctx, headers);

            // Print separator line if configured
            if (self.header_separator) {
                for (self.columns.items, 0..) |col, i| {
                    if (i > 0) ctx.print("{s}", .{self.column_separator});
                    var j: usize = 0;
                    while (j < col.width) : (j += 1) {
                        ctx.print("-", .{});
                    }
                }
                ctx.print("\n", .{});
            }
        }

        // Print rows
        for (self.rows.items) |row| {
            self.printRow(ctx, row);
        }
    }

    fn getHeaders(self: *Table) [][]const u8 {
        var headers = self.allocator.alloc([]const u8, self.columns.items.len) catch return &.{};
        for (self.columns.items, 0..) |col, i| {
            headers[i] = col.header;
        }
        return headers;
    }

    fn printRow(self: *Table, ctx: *Context, values: []const []const u8) void {
        for (values, 0..) |val, i| {
            if (i > 0) ctx.print("{s}", .{self.column_separator});

            const col = self.columns.items[i];
            const padding = col.width - val.len;

            switch (col.alignment) {
                .left => {
                    ctx.print("{s}", .{val});
                    self.printSpaces(ctx, padding);
                },
                .right => {
                    self.printSpaces(ctx, padding);
                    ctx.print("{s}", .{val});
                },
                .center => {
                    const left_pad = padding / 2;
                    const right_pad = padding - left_pad;
                    self.printSpaces(ctx, left_pad);
                    ctx.print("{s}", .{val});
                    self.printSpaces(ctx, right_pad);
                },
            }
        }
        ctx.print("\n", .{});
    }

    fn printSpaces(self: *Table, ctx: *Context, count: usize) void {
        _ = self;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            ctx.print(" ", .{});
        }
    }
};

/// Print items as a raw list (one per line)
pub fn printList(ctx: *Context, items: []const []const u8) void {
    for (items) |item| {
        ctx.print("{s}\n", .{item});
    }
}

// ============================================================================
// Wire List Printer — format binary wire responses as table / json / raw
//
// Architecture: two-phase decode → render.
//   1. readFieldValue() decodes one wire field into a FieldValue union.
//   2. Renderers (table/json/raw) operate on FieldValue — no wire knowledge.
// Adding a new field type: update WireFieldType, readFieldValue, skipField,
// formatFieldForDisplay, writeJsonField, writeRawField.
// ============================================================================

/// Wire field types for binary list entries.
pub const WireFieldType = enum {
    str_u16, // [len:u16][bytes] — string with u16 length prefix
    str_u32, // [len:u32][bytes] — string with u32 length prefix
    uint_u32, // [value:u32] — unsigned 32-bit integer
    uint_u64, // [value:u64] — unsigned 64-bit integer
    int_i64, // [value:i64] — signed 64-bit integer
    enum_u8, // [value:u8] — mapped to string via enum_labels
    timestamp_i64, // [value:i64] — relative time string (table/raw), raw int (json)
    optional_timestamp_i64, // [has:u8][value?:i64] — "—" if absent
    optional_str_u16, // [has:u8][len?:u16][bytes?] — "—" if absent
    skip_counted_records_u16, // [count:u16](sub_record)* — skip nested records, uses sub_columns
};

/// Column spec for printWireList: maps a binary field to a table column.
///
/// The column list must match the wire entry layout exactly — every field
/// in the binary entry must have a corresponding WireColumn (in order).
/// Set `header = ""` to read a field from the wire but hide it from table
/// output. JSON output includes all columns with non-empty `field`.
pub const WireColumn = struct {
    field: []const u8, // JSON field name
    header: []const u8, // Table column header (empty = hidden from table)
    field_type: WireFieldType,
    alignment: Alignment = .left,
    enum_labels: ?[]const []const u8 = null, // index→string mapping for enum_u8
    sub_columns: ?[]const WireColumn = null, // record layout for skip_counted_records_u16
};

/// Decoded wire field value — output of readFieldValue, input to renderers.
const FieldValue = union(enum) {
    string: []const u8,
    uint: u64,
    int: i64,
    enum_val: u8,
    optional_int: ?i64,
    optional_string: ?[]const u8,
    skipped: void,
};

/// Format a binary wire list response as table, json, or raw output.
///
/// Expects the standard binary wire format:
///   [count:u32] ([field1][field2]...)* [has_more:u8] [cursor_len:u16] [cursor]?
///
/// Only the count and per-entry fields are consumed. The has_more/cursor
/// trailer (if present) is left unread — callers needing pagination should
/// inspect `data` themselves after this function returns.
pub fn printWireList(
    ctx: *Context,
    data: []const u8,
    empty_message: []const u8,
    columns: []const WireColumn,
) void {
    var reader = WireReader.init(data);

    const count = reader.readU32() orelse {
        ctx.print("{s}\n", .{empty_message});
        return;
    };
    if (count == 0) {
        ctx.print("{s}\n", .{empty_message});
        return;
    }

    switch (getFormat(ctx)) {
        .json => printWireJson(ctx, &reader, count, columns),
        .raw => printWireRaw(ctx, &reader, count, columns),
        .table => printWireTable(ctx, &reader, count, columns),
    }
}

// ── Wire decoding (phase 1) ─────────────────────────────────────────────

/// Read one wire field and return a typed FieldValue.
fn readFieldValue(reader: *WireReader, col: WireColumn) ?FieldValue {
    return switch (col.field_type) {
        .str_u16 => .{ .string = reader.readLengthPrefixed(u16) orelse return null },
        .str_u32 => .{ .string = reader.readLengthPrefixed(u32) orelse return null },
        .uint_u32 => .{ .uint = reader.readU32() orelse return null },
        .uint_u64 => .{ .uint = reader.readU64() orelse return null },
        .int_i64, .timestamp_i64 => .{ .int = reader.readI64() orelse return null },
        .enum_u8 => .{ .enum_val = reader.readU8() orelse return null },
        .optional_timestamp_i64 => blk: {
            const has = reader.readU8() orelse return null;
            break :blk .{ .optional_int = if (has == 1) (reader.readI64() orelse return null) else null };
        },
        .optional_str_u16 => blk: {
            const has = reader.readU8() orelse return null;
            break :blk .{ .optional_string = if (has == 1) (reader.readLengthPrefixed(u16) orelse return null) else null };
        },
        .skip_counted_records_u16 => blk: {
            skipField(reader, col);
            break :blk .skipped;
        },
    };
}

/// Advance the reader past a single wire field (used for skipping).
fn skipField(reader: *WireReader, col: WireColumn) void {
    switch (col.field_type) {
        .str_u16 => _ = reader.readLengthPrefixed(u16),
        .str_u32 => _ = reader.readLengthPrefixed(u32),
        .uint_u32 => _ = reader.readU32(),
        .uint_u64 => _ = reader.readU64(),
        .int_i64, .timestamp_i64 => _ = reader.readI64(),
        .enum_u8 => _ = reader.readU8(),
        .optional_timestamp_i64 => {
            const has = reader.readU8() orelse return;
            if (has == 1) _ = reader.readI64();
        },
        .optional_str_u16 => {
            const has = reader.readU8() orelse return;
            if (has == 1) _ = reader.readLengthPrefixed(u16);
        },
        .skip_counted_records_u16 => {
            const cnt = reader.readU16() orelse return;
            var j: u16 = 0;
            while (j < cnt) : (j += 1) {
                for (col.sub_columns orelse return) |sc| skipField(reader, sc);
            }
        },
    }
}

// ── Shared helpers ──────────────────────────────────────────────────────

/// Format an i64 millisecond timestamp as a relative time string.
fn formatRelativeTimeBuf(ms: i64, buf: *[24]u8) []const u8 {
    const now = @import("stdx").time.milliTimestamp();
    const diff = now - ms;
    if (diff < 0) return std.fmt.bufPrint(buf, "{d}ms", .{ms}) catch "?";
    if (diff < 1000) return std.fmt.bufPrint(buf, "{d}ms ago", .{diff}) catch "?";
    if (diff < 60_000) return std.fmt.bufPrint(buf, "{d}s ago", .{@divTrunc(diff, 1000)}) catch "?";
    if (diff < 3_600_000) return std.fmt.bufPrint(buf, "{d}m ago", .{@divTrunc(diff, 60_000)}) catch "?";
    return std.fmt.bufPrint(buf, "{d}h ago", .{@divTrunc(diff, 3_600_000)}) catch "?";
}

/// Resolve enum_val → label string using column metadata.
fn enumLabel(col: WireColumn, v: u8) []const u8 {
    if (col.enum_labels) |l| if (v < l.len) return l[v];
    return "unknown";
}

/// Dupe a stack-buffered string, track it in `formatted` for cleanup.
fn dupeFormatted(
    s: []const u8,
    allocator: Allocator,
    formatted: *std.ArrayList([]const u8),
) ?[]const u8 {
    const d = allocator.dupe(u8, s) catch return null;
    formatted.append(allocator, d) catch {
        allocator.free(d);
        return null;
    };
    return d;
}

// ── Renderers (phase 2) ─────────────────────────────────────────────────

/// Convert a FieldValue to a display string for table output.
/// Numeric values are formatted to decimal and tracked in `formatted` for cleanup.
fn formatFieldForDisplay(
    val: FieldValue,
    col: WireColumn,
    allocator: Allocator,
    formatted: *std.ArrayList([]const u8),
) ?[]const u8 {
    switch (val) {
        .string => |s| return s,
        .uint => |v| {
            var buf: [24]u8 = undefined;
            return dupeFormatted(std.fmt.bufPrint(&buf, "{d}", .{v}) catch return null, allocator, formatted);
        },
        .int => |v| {
            var buf: [24]u8 = undefined;
            const s = if (col.field_type == .timestamp_i64) formatRelativeTimeBuf(v, &buf) else (std.fmt.bufPrint(&buf, "{d}", .{v}) catch return null);
            return dupeFormatted(s, allocator, formatted);
        },
        .enum_val => |v| return enumLabel(col, v),
        .optional_int => |maybe| {
            const v = maybe orelse return "\xe2\x80\x94";
            var buf: [24]u8 = undefined;
            const s = if (col.field_type == .optional_timestamp_i64) formatRelativeTimeBuf(v, &buf) else (std.fmt.bufPrint(&buf, "{d}", .{v}) catch return null);
            return dupeFormatted(s, allocator, formatted);
        },
        .optional_string => |maybe| return maybe orelse "\xe2\x80\x94",
        .skipped => return "",
    }
}

/// Emit one JSON field. Caller handles comma separation.
fn writeJsonField(ctx: *Context, col: WireColumn, val: FieldValue) void {
    switch (val) {
        .string => |s| ctx.print("\"{s}\":\"{s}\"", .{ col.field, s }),
        .uint => |v| ctx.print("\"{s}\":{d}", .{ col.field, v }),
        .int => |v| ctx.print("\"{s}\":{d}", .{ col.field, v }),
        .enum_val => |v| ctx.print("\"{s}\":\"{s}\"", .{ col.field, enumLabel(col, v) }),
        .optional_int => |maybe| {
            if (maybe) |v| ctx.print("\"{s}\":{d}", .{ col.field, v }) else ctx.print("\"{s}\":null", .{col.field});
        },
        .optional_string => |maybe| {
            if (maybe) |s| ctx.print("\"{s}\":\"{s}\"", .{ col.field, s }) else ctx.print("\"{s}\":null", .{col.field});
        },
        .skipped => {},
    }
}

/// Emit one field value for raw output.
fn writeRawField(ctx: *Context, col: WireColumn, val: FieldValue) void {
    switch (val) {
        .string => |s| ctx.print("{s}", .{s}),
        .uint => |v| ctx.print("{d}", .{v}),
        .int => |v| {
            if (col.field_type == .timestamp_i64) {
                var buf: [24]u8 = undefined;
                ctx.print("{s}", .{formatRelativeTimeBuf(v, &buf)});
            } else ctx.print("{d}", .{v});
        },
        .enum_val => |v| ctx.print("{s}", .{enumLabel(col, v)}),
        .optional_int => |maybe| {
            if (maybe) |v| {
                if (col.field_type == .optional_timestamp_i64) {
                    var buf: [24]u8 = undefined;
                    ctx.print("{s}", .{formatRelativeTimeBuf(v, &buf)});
                } else ctx.print("{d}", .{v});
            } else ctx.print("\xe2\x80\x94", .{});
        },
        .optional_string => |maybe| {
            if (maybe) |s| ctx.print("{s}", .{s}) else ctx.print("\xe2\x80\x94", .{});
        },
        .skipped => {},
    }
}

// ── Top-level format dispatchers ────────────────────────────────────────

fn printWireTable(
    ctx: *Context,
    reader: *WireReader,
    count: u32,
    columns: []const WireColumn,
) void {
    var table = Table.init(ctx.allocator);
    defer table.deinit();

    var formatted: std.ArrayList([]const u8) = .empty;
    defer {
        for (formatted.items) |s| ctx.allocator.free(s);
        formatted.deinit(ctx.allocator);
    }

    for (columns) |col| {
        if (col.header.len > 0) table.addColumn(col.header, col.alignment) catch return;
    }

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        var cells: [32][]const u8 = undefined;
        var cell_idx: usize = 0;
        var ok = true;

        for (columns) |col| {
            const fv = readFieldValue(reader, col) orelse {
                ok = false;
                break;
            };
            if (col.header.len > 0) {
                cells[cell_idx] = formatFieldForDisplay(fv, col, ctx.allocator, &formatted) orelse {
                    ok = false;
                    break;
                };
                cell_idx += 1;
            }
        }

        if (!ok) break;
        table.addRow(cells[0..cell_idx]) catch continue;
    }

    table.print(ctx);
}

fn printWireJson(
    ctx: *Context,
    reader: *WireReader,
    count: u32,
    columns: []const WireColumn,
) void {
    ctx.print("[", .{});
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        if (i > 0) ctx.print(",", .{});
        ctx.print("{{", .{});
        var first = true;
        for (columns) |col| {
            const fv = readFieldValue(reader, col) orelse return;
            if (col.field.len == 0) continue;
            if (!first) ctx.print(",", .{});
            first = false;
            writeJsonField(ctx, col, fv);
        }
        ctx.print("}}", .{});
    }
    ctx.print("]\n", .{});
}

/// Raw output: prints the first visible column's value per row, one per line.
fn printWireRaw(
    ctx: *Context,
    reader: *WireReader,
    count: u32,
    columns: []const WireColumn,
) void {
    var first_visible: ?usize = null;
    for (columns, 0..) |col, ci| {
        if (col.header.len > 0) {
            first_visible = ci;
            break;
        }
    }
    if (first_visible == null) return;

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        for (columns, 0..) |col, ci| {
            const fv = readFieldValue(reader, col) orelse return;
            if (ci == first_visible.?) {
                writeRawField(ctx, col, fv);
                ctx.print("\n", .{});
            }
        }
    }
}

/// Print items as a numbered list
pub fn printNumberedList(ctx: *Context, items: []const []const u8) void {
    for (items, 1..) |item, i| {
        ctx.print("{d}. {s}\n", .{ i, item });
    }
}

/// Print key-value pairs
pub fn printKeyValue(ctx: *Context, key: []const u8, value: []const u8) void {
    ctx.print("{s}: {s}\n", .{ key, value });
}

/// Print key-value pairs with aligned values
pub const KeyValuePrinter = struct {
    allocator: Allocator,
    pairs: std.ArrayList(struct { key: []const u8, value: []const u8 }),
    max_key_len: usize = 0,

    pub fn init(allocator: Allocator) KeyValuePrinter {
        return .{
            .allocator = allocator,
            .pairs = .empty,
        };
    }

    pub fn deinit(self: *KeyValuePrinter) void {
        self.pairs.deinit(self.allocator);
    }

    pub fn add(self: *KeyValuePrinter, key: []const u8, value: []const u8) !void {
        if (key.len > self.max_key_len) {
            self.max_key_len = key.len;
        }
        try self.pairs.append(self.allocator, .{ .key = key, .value = value });
    }

    pub fn print(self: *KeyValuePrinter, ctx: *Context) void {
        for (self.pairs.items) |pair| {
            // Print key with padding
            ctx.print("{s}", .{pair.key});
            var padding = self.max_key_len - pair.key.len;
            while (padding > 0) : (padding -= 1) {
                ctx.print(" ", .{});
            }
            ctx.print(": {s}\n", .{pair.value});
        }
    }
};

/// Format bytes as human-readable size
pub fn formatBytes(bytes: u64) [16]u8 {
    var buf: [16]u8 = undefined;
    const units = [_][]const u8{ "B", "KB", "MB", "GB", "TB", "PB" };

    var size: f64 = @floatFromInt(bytes);
    var unit_idx: usize = 0;

    while (size >= 1024 and unit_idx < units.len - 1) {
        size /= 1024;
        unit_idx += 1;
    }

    if (unit_idx == 0) {
        _ = std.fmt.bufPrint(&buf, "{d} {s}", .{ bytes, units[0] }) catch {};
    } else {
        _ = std.fmt.bufPrint(&buf, "{d:.1} {s}", .{ size, units[unit_idx] }) catch {};
    }

    return buf;
}

/// Format duration in human-readable form
pub fn formatDuration(seconds: u64) [32]u8 {
    var buf: [32]u8 = undefined;

    if (seconds < 60) {
        _ = std.fmt.bufPrint(&buf, "{d}s", .{seconds}) catch {};
    } else if (seconds < 3600) {
        const mins = seconds / 60;
        const secs = seconds % 60;
        if (secs == 0) {
            _ = std.fmt.bufPrint(&buf, "{d}m", .{mins}) catch {};
        } else {
            _ = std.fmt.bufPrint(&buf, "{d}m {d}s", .{ mins, secs }) catch {};
        }
    } else if (seconds < 86400) {
        const hours = seconds / 3600;
        const mins = (seconds % 3600) / 60;
        if (mins == 0) {
            _ = std.fmt.bufPrint(&buf, "{d}h", .{hours}) catch {};
        } else {
            _ = std.fmt.bufPrint(&buf, "{d}h {d}m", .{ hours, mins }) catch {};
        }
    } else {
        const days = seconds / 86400;
        const hours = (seconds % 86400) / 3600;
        if (hours == 0) {
            _ = std.fmt.bufPrint(&buf, "{d}d", .{days}) catch {};
        } else {
            _ = std.fmt.bufPrint(&buf, "{d}d {d}h", .{ days, hours }) catch {};
        }
    }

    return buf;
}

/// Format timestamp as ISO 8601
pub fn formatTimestamp(timestamp_ms: i64) [32]u8 {
    var buf: [32]u8 = undefined;
    const secs: u64 = @intCast(@divTrunc(timestamp_ms, 1000));
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = secs };
    const day_seconds = epoch_seconds.getDaySeconds();
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();

    _ = std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        year_day.year,
        @intFromEnum(year_day.month),
        year_day.day,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
    }) catch {};

    return buf;
}

// ============================================================================
// Tests
// ============================================================================

test "table basic" {
    const allocator = std.testing.allocator;
    var table = Table.init(allocator);
    defer table.deinit();

    try table.addColumn("NAME", .left);
    try table.addColumn("VALUE", .right);

    try table.addRow(&.{ "foo", "123" });
    try table.addRow(&.{ "bar", "4567" });

    // Width should be max of header and values
    try std.testing.expectEqual(@as(usize, 4), table.columns.items[0].width);
    try std.testing.expectEqual(@as(usize, 5), table.columns.items[1].width);
}

test "format bytes" {
    const result1 = formatBytes(1024);
    try std.testing.expect(std.mem.startsWith(u8, &result1, "1"));

    const result2 = formatBytes(0);
    try std.testing.expect(std.mem.startsWith(u8, &result2, "0"));
}

test "format duration" {
    const result1 = formatDuration(45);
    try std.testing.expect(std.mem.startsWith(u8, &result1, "45s"));

    const result2 = formatDuration(3661);
    try std.testing.expect(std.mem.startsWith(u8, &result2, "1h"));
}
