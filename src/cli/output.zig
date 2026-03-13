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

/// Get the output format from the global --output flag.
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
            .columns = std.ArrayList(Column){},
            .rows = std.ArrayList([]const []const u8){},
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
            .pairs = std.ArrayList(struct { key: []const u8, value: []const u8 }){},
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
