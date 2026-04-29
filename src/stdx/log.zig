//! Flo Structured Logging System
//!
//! An idiomatic Zig logging library: format strings by default, optional structured fields.
//!
//! Design Principles:
//! - Format strings like std.log (familiar, simple)
//! - Structured fields when you need them
//! - Zero allocations in the hot path
//! - Comptime generics (no vtables)
//!
//! ## Quick Start
//!
//! ```zig
//! const log = @import("log");
//!
//! // Simple logging (default) - just like std.log
//! log.info("Server starting on port {d}", .{9000});
//! log.debug("Loading config from {s}", .{path});
//!
//! // Structured logging (when you need machine-readable fields)
//! log.infoWith("request handled", .{
//!     log.Field.str("method", "GET"),
//!     log.Field.int("status", 200),
//!     log.Field.duration("elapsed", timer.read()),
//! });
//!
//! // Scoped logger
//! const storage = log.scoped(.storage);
//! storage.info("WAL flushed {d} bytes", .{bytes});
//! storage.debugWith("compaction done", .{
//!     log.Field.int("level", 1),
//!     log.Field.size("bytes", written),
//! });
//! ```
//!
//! ## Configuration
//!
//! ```zig
//! log.configure(.{
//!     .level = .debug,
//!     .format = .json,
//! });
//! ```

const std = @import("std");
const builtin = @import("builtin");

// Re-exports
pub const Field = @import("log/field.zig").Field;
pub const format = @import("log/format.zig");

pub const Level = format.Level;
pub const Format = format.Format;
pub const Color = format.Color;
pub const TextConfig = format.TextConfig;
pub const JsonConfig = format.JsonConfig;

// =============================================================================
// Global Configuration (Runtime-Settable)
// =============================================================================

/// Global logger configuration.
pub const Config = struct {
    /// Minimum log level to emit.
    level: Level = .info,
    /// Output format.
    format: Format = .text,
    /// Enable colors (null = auto-detect TTY).
    colors: ?bool = null,
    /// Show timestamps.
    show_timestamp: bool = true,
    /// Show source location (file:line).
    show_caller: bool = false,
};

/// Global state - minimal, no allocations.
var global: struct {
    level: Level = .info,
    format: Format = .text,
    use_colors: bool = true,
    show_timestamp: bool = true,
    show_caller: bool = false,
} = .{};

/// Configure the global logger. Call once at startup.
pub fn configure(config: Config) void {
    global.level = config.level;
    global.format = config.format;
    global.use_colors = config.colors orelse (std.c.isatty(std.posix.STDOUT_FILENO) != 0);
    global.show_timestamp = config.show_timestamp;
    global.show_caller = config.show_caller;
}

/// Set just the log level.
pub fn setLevel(level: Level) void {
    global.level = level;
}

/// Get current log level.
pub fn getLevel() Level {
    return global.level;
}

/// Set level from string (for CLI).
pub fn setLevelFromString(s: []const u8) bool {
    if (Level.parse(s)) |level| {
        setLevel(level);
        return true;
    }
    return false;
}

// =============================================================================
// Core Logging Functions (Format String - Default)
// =============================================================================

/// Log at trace level with format string.
pub inline fn trace(comptime fmt: []const u8, args: anytype) void {
    logFmtImpl(.trace, null, fmt, args, @src());
}

/// Log at debug level with format string.
pub inline fn debug(comptime fmt: []const u8, args: anytype) void {
    logFmtImpl(.debug, null, fmt, args, @src());
}

/// Log at info level with format string.
pub inline fn info(comptime fmt: []const u8, args: anytype) void {
    logFmtImpl(.info, null, fmt, args, @src());
}

/// Log at warn level with format string.
pub inline fn warn(comptime fmt: []const u8, args: anytype) void {
    logFmtImpl(.warn, null, fmt, args, @src());
}

/// Log at error level with format string.
pub inline fn err(comptime fmt: []const u8, args: anytype) void {
    logFmtImpl(.err, null, fmt, args, @src());
}

/// Log at fatal level with format string (does NOT exit).
pub inline fn fatal(comptime fmt: []const u8, args: anytype) void {
    logFmtImpl(.fatal, null, fmt, args, @src());
}

/// Log at trace level with structured fields.
pub inline fn traceWith(comptime msg: []const u8, fields: anytype) void {
    logImpl(.trace, null, msg, fields, @src());
}

/// Log at debug level with structured fields.
pub inline fn debugWith(comptime msg: []const u8, fields: anytype) void {
    logImpl(.debug, null, msg, fields, @src());
}

/// Log at info level with structured fields.
pub inline fn infoWith(comptime msg: []const u8, fields: anytype) void {
    logImpl(.info, null, msg, fields, @src());
}

/// Log at warn level with structured fields.
pub inline fn warnWith(comptime msg: []const u8, fields: anytype) void {
    logImpl(.warn, null, msg, fields, @src());
}

/// Log at error level with structured fields.
pub inline fn errWith(comptime msg: []const u8, fields: anytype) void {
    logImpl(.err, null, msg, fields, @src());
}

/// Log at fatal level with structured fields (does NOT exit).
pub inline fn fatalWith(comptime msg: []const u8, fields: anytype) void {
    logImpl(.fatal, null, msg, fields, @src());
}

// =============================================================================
// Scoped Logger (Comptime - Zero Cost)
// =============================================================================

/// Create a scoped logger. The scope is baked in at comptime - no runtime cost.
pub fn scoped(comptime scope: @EnumLiteral()) ScopedLogger(scope) {
    return .{};
}

/// A logger with a compile-time scope tag.
pub fn ScopedLogger(comptime scope: @EnumLiteral()) type {
    return struct {
        const Self = @This();
        const scope_name = @tagName(scope);

        // Format string logging (default)
        pub inline fn trace(_: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtImpl(.trace, scope_name, fmt, args, @src());
        }

        pub inline fn debug(_: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtImpl(.debug, scope_name, fmt, args, @src());
        }

        pub inline fn info(_: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtImpl(.info, scope_name, fmt, args, @src());
        }

        pub inline fn warn(_: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtImpl(.warn, scope_name, fmt, args, @src());
        }

        pub inline fn err(_: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtImpl(.err, scope_name, fmt, args, @src());
        }

        pub inline fn fatal(_: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtImpl(.fatal, scope_name, fmt, args, @src());
        }

        // Structured logging (with fields)
        pub inline fn traceWith(_: Self, comptime msg: []const u8, fields: anytype) void {
            logImpl(.trace, scope_name, msg, fields, @src());
        }

        pub inline fn debugWith(_: Self, comptime msg: []const u8, fields: anytype) void {
            logImpl(.debug, scope_name, msg, fields, @src());
        }

        pub inline fn infoWith(_: Self, comptime msg: []const u8, fields: anytype) void {
            logImpl(.info, scope_name, msg, fields, @src());
        }

        pub inline fn warnWith(_: Self, comptime msg: []const u8, fields: anytype) void {
            logImpl(.warn, scope_name, msg, fields, @src());
        }

        pub inline fn errWith(_: Self, comptime msg: []const u8, fields: anytype) void {
            logImpl(.err, scope_name, msg, fields, @src());
        }

        pub inline fn fatalWith(_: Self, comptime msg: []const u8, fields: anytype) void {
            logImpl(.fatal, scope_name, msg, fields, @src());
        }

        /// Create a child logger with additional context fields.
        /// Uses tuple concatenation - zero allocations!
        pub inline fn with(_: Self, context_fields: anytype) ContextScope(scope, @TypeOf(context_fields)) {
            return .{ .context = context_fields };
        }
    };
}

/// A scoped logger with attached context fields (still zero-allocation).
pub fn ContextScope(comptime scope: @EnumLiteral(), comptime ContextFields: type) type {
    return struct {
        const Self = @This();
        const scope_name = @tagName(scope);

        context: ContextFields,

        // Format string logging (default)
        pub inline fn trace(self: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtWithContext(.trace, scope_name, fmt, args, self.context, @src());
        }

        pub inline fn debug(self: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtWithContext(.debug, scope_name, fmt, args, self.context, @src());
        }

        pub inline fn info(self: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtWithContext(.info, scope_name, fmt, args, self.context, @src());
        }

        pub inline fn warn(self: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtWithContext(.warn, scope_name, fmt, args, self.context, @src());
        }

        pub inline fn err(self: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtWithContext(.err, scope_name, fmt, args, self.context, @src());
        }

        pub inline fn fatal(self: Self, comptime fmt: []const u8, args: anytype) void {
            logFmtWithContext(.fatal, scope_name, fmt, args, self.context, @src());
        }

        // Structured logging (with additional fields)
        pub inline fn infoWith(self: Self, comptime msg: []const u8, fields: anytype) void {
            logWithContext(.info, scope_name, msg, self.context, fields, @src());
        }

        pub inline fn debugWith(self: Self, comptime msg: []const u8, fields: anytype) void {
            logWithContext(.debug, scope_name, msg, self.context, fields, @src());
        }

        pub inline fn warnWith(self: Self, comptime msg: []const u8, fields: anytype) void {
            logWithContext(.warn, scope_name, msg, self.context, fields, @src());
        }

        pub inline fn errWith(self: Self, comptime msg: []const u8, fields: anytype) void {
            logWithContext(.err, scope_name, msg, self.context, fields, @src());
        }
    };
}

// =============================================================================
// Implementation (Internal)
// =============================================================================

/// Format-string logging implementation (default style).
fn logFmtImpl(
    level: Level,
    comptime scope: ?[]const u8,
    comptime fmt: []const u8,
    args: anytype,
    src: std.builtin.SourceLocation,
) void {
    // Skip logging in test builds to avoid interfering with test runner
    if (builtin.is_test) return;

    if (@intFromEnum(level) < @intFromEnum(global.level)) {
        return;
    }

    // Build scope field if present
    const scope_fields = if (scope) |s| .{Field.component(s)} else .{};

    writeFmtEntry(level, fmt, args, scope_fields, src);
}

/// Format-string logging with context fields.
fn logFmtWithContext(
    level: Level,
    comptime scope: ?[]const u8,
    comptime fmt: []const u8,
    args: anytype,
    context: anytype,
    src: std.builtin.SourceLocation,
) void {
    // Skip logging in test builds to avoid interfering with test runner
    if (builtin.is_test) return;

    if (@intFromEnum(level) < @intFromEnum(global.level)) {
        return;
    }

    // Concatenate: scope + context
    const all_fields = if (scope) |s|
        .{Field.component(s)} ++ context
    else
        context;

    writeFmtEntry(level, fmt, args, all_fields, src);
}

/// Structured fields logging implementation.
fn logImpl(
    level: Level,
    comptime scope: ?[]const u8,
    comptime msg: []const u8,
    fields: anytype,
    src: std.builtin.SourceLocation,
) void {
    // Skip logging in test builds to avoid interfering with test runner
    if (builtin.is_test) return;

    // Early exit if below threshold
    if (@intFromEnum(level) < @intFromEnum(global.level)) {
        return;
    }

    // Build fields tuple with optional scope
    const all_fields = if (scope) |s|
        .{Field.component(s)} ++ fields
    else
        fields;

    writeEntry(level, msg, all_fields, src);
}

/// Log with context fields prepended.
fn logWithContext(
    level: Level,
    comptime scope: ?[]const u8,
    comptime msg: []const u8,
    context: anytype,
    fields: anytype,
    src: std.builtin.SourceLocation,
) void {
    // Skip logging in test builds to avoid interfering with test runner
    if (builtin.is_test) return;

    if (@intFromEnum(level) < @intFromEnum(global.level)) {
        return;
    }

    // Concatenate: scope + context + fields
    const all_fields = if (scope) |s|
        .{Field.component(s)} ++ context ++ fields
    else
        context ++ fields;

    writeEntry(level, msg, all_fields, src);
}

/// Write a format-string log entry.
fn writeFmtEntry(
    level: Level,
    comptime fmt: []const u8,
    args: anytype,
    fields: anytype,
    src: std.builtin.SourceLocation,
) void {
    var buf: [8192]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    const timestamp = @import("time.zig").nanoTimestamp();

    const caller: ?format.Caller = if (global.show_caller)
        .{ .file = src.file, .line = src.line, .fn_name = src.fn_name }
    else
        null;

    switch (global.format) {
        .text => formatFmtTextEntry(writer, level, fmt, args, timestamp, caller, fields),
        .json => formatFmtJsonEntry(writer, level, fmt, args, timestamp, caller, fields),
        .compact => formatFmtCompactEntry(writer, level, fmt, args, fields),
    }

    const output_bytes = fbs.buffered();
    const use_stderr = (level == .err or level == .fatal);

    if (use_stderr) {
        _ = @import("io.zig").writeFd(std.posix.STDERR_FILENO, output_bytes);
    } else {
        _ = @import("io.zig").writeFd(std.posix.STDOUT_FILENO, output_bytes);
    }
}

/// Write a log entry to the appropriate output.
fn writeEntry(
    level: Level,
    comptime msg: []const u8,
    fields: anytype,
    src: std.builtin.SourceLocation,
) void {
    // Stack buffer - no heap allocation
    var buf: [8192]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    const timestamp = @import("time.zig").nanoTimestamp();

    // Build caller info if enabled
    const caller: ?format.Caller = if (global.show_caller)
        .{ .file = src.file, .line = src.line, .fn_name = src.fn_name }
    else
        null;

    // Format based on configured type
    switch (global.format) {
        .text => formatTextEntry(writer, level, msg, timestamp, caller, fields),
        .json => formatJsonEntry(writer, level, msg, timestamp, caller, fields),
        .compact => formatCompactEntry(writer, level, msg, fields),
    }

    // Write to stdout/stderr
    const output_bytes = fbs.buffered();
    const use_stderr = (level == .err or level == .fatal);

    if (use_stderr) {
        _ = @import("io.zig").writeFd(std.posix.STDERR_FILENO, output_bytes);
    } else {
        _ = @import("io.zig").writeFd(std.posix.STDOUT_FILENO, output_bytes);
    }
}

// =============================================================================
// Format Functions (Comptime Generics)
// =============================================================================

fn formatTextEntry(
    writer: anytype,
    level: Level,
    comptime msg: []const u8,
    timestamp: i128,
    caller: ?format.Caller,
    fields: anytype,
) void {
    const colors = global.use_colors;

    // Timestamp
    if (global.show_timestamp) {
        if (colors) writer.writeAll(Color.dim) catch {};
        format.writeShortTimestamp(writer, timestamp) catch {};
        if (colors) writer.writeAll(Color.reset) catch {};
        writer.writeByte(' ') catch {};
    }

    // Level
    if (colors) writer.writeAll(Color.forLevel(level)) catch {};
    writer.writeAll(level.toShort()) catch {};
    if (colors) writer.writeAll(Color.reset) catch {};
    writer.writeByte(' ') catch {};

    // Caller
    if (caller) |c| {
        if (colors) writer.writeAll(Color.dim) catch {};
        writer.print("({s}:{d}) ", .{ format.shortenPath(c.file), c.line }) catch {};
        if (colors) writer.writeAll(Color.reset) catch {};
    }

    // Message
    writer.writeAll(msg) catch {};

    // Fields (comptime iteration)
    inline for (fields) |field| {
        writer.writeByte(' ') catch {};
        if (colors) writer.writeAll(Color.cyan) catch {};
        writer.writeAll(field.key) catch {};
        if (colors) writer.writeAll(Color.reset) catch {};
        writer.writeByte('=') catch {};
        field.formatValue(writer) catch {};
    }

    writer.writeByte('\n') catch {};
}

fn formatJsonEntry(
    writer: anytype,
    level: Level,
    comptime msg: []const u8,
    timestamp: i128,
    caller: ?format.Caller,
    fields: anytype,
) void {
    writer.writeByte('{') catch {};

    // Timestamp
    if (global.show_timestamp) {
        writer.writeAll("\"ts\":") catch {};
        writer.print("{d}", .{timestamp}) catch {};
    }

    // Level
    writer.writeAll(",\"level\":\"") catch {};
    writer.writeAll(level.toString()) catch {};
    writer.writeByte('"') catch {};

    // Message
    writer.writeAll(",\"msg\":\"") catch {};
    format.writeJsonEscaped(writer, msg) catch {};
    writer.writeByte('"') catch {};

    // Caller
    if (caller) |c| {
        writer.writeAll(",\"caller\":\"") catch {};
        writer.print("{s}:{d}", .{ format.shortenPath(c.file), c.line }) catch {};
        writer.writeByte('"') catch {};
    }

    // Fields (comptime iteration)
    inline for (fields) |field| {
        writer.writeAll(",\"") catch {};
        writer.writeAll(field.key) catch {};
        writer.writeAll("\":") catch {};
        field.formatJsonValue(writer) catch {};
    }

    writer.writeAll("}\n") catch {};
}

fn formatCompactEntry(
    writer: anytype,
    level: Level,
    comptime msg: []const u8,
    fields: anytype,
) void {
    // Level (single char)
    writer.writeByte(switch (level) {
        .trace => 'T',
        .debug => 'D',
        .info => 'I',
        .warn => 'W',
        .err => 'E',
        .fatal => 'F',
    }) catch {};
    writer.writeByte(' ') catch {};

    // Message
    writer.writeAll(msg) catch {};

    // Fields
    inline for (fields) |field| {
        writer.writeByte(' ') catch {};
        writer.writeAll(field.key) catch {};
        writer.writeByte('=') catch {};
        field.formatCompactValue(writer) catch {};
    }

    writer.writeByte('\n') catch {};
}

// =============================================================================
// Format-String Format Functions
// =============================================================================

fn formatFmtTextEntry(
    writer: anytype,
    level: Level,
    comptime fmt: []const u8,
    args: anytype,
    timestamp: i128,
    caller: ?format.Caller,
    fields: anytype,
) void {
    const colors = global.use_colors;

    // Timestamp
    if (global.show_timestamp) {
        if (colors) writer.writeAll(Color.dim) catch {};
        format.writeShortTimestamp(writer, timestamp) catch {};
        if (colors) writer.writeAll(Color.reset) catch {};
        writer.writeByte(' ') catch {};
    }

    // Level
    if (colors) writer.writeAll(Color.forLevel(level)) catch {};
    writer.writeAll(level.toShort()) catch {};
    if (colors) writer.writeAll(Color.reset) catch {};
    writer.writeByte(' ') catch {};

    // Caller
    if (caller) |c| {
        if (colors) writer.writeAll(Color.dim) catch {};
        writer.print("({s}:{d}) ", .{ format.shortenPath(c.file), c.line }) catch {};
        if (colors) writer.writeAll(Color.reset) catch {};
    }

    // Message (format string)
    writer.print(fmt, args) catch {};

    // Fields (comptime iteration)
    inline for (fields) |field| {
        writer.writeByte(' ') catch {};
        if (colors) writer.writeAll(Color.cyan) catch {};
        writer.writeAll(field.key) catch {};
        if (colors) writer.writeAll(Color.reset) catch {};
        writer.writeByte('=') catch {};
        field.formatValue(writer) catch {};
    }

    writer.writeByte('\n') catch {};
}

fn formatFmtJsonEntry(
    writer: anytype,
    level: Level,
    comptime fmt: []const u8,
    args: anytype,
    timestamp: i128,
    caller: ?format.Caller,
    fields: anytype,
) void {
    writer.writeByte('{') catch {};

    // Timestamp
    if (global.show_timestamp) {
        writer.writeAll("\"ts\":") catch {};
        writer.print("{d}", .{timestamp}) catch {};
    }

    // Level
    writer.writeAll(",\"level\":\"") catch {};
    writer.writeAll(level.toString()) catch {};
    writer.writeByte('"') catch {};

    // Message (format into buffer first, then JSON-escape)
    writer.writeAll(",\"msg\":\"") catch {};
    // For JSON, we need to format and then escape - use a small buffer
    var msg_buf: [1024]u8 = undefined;
    const msg = std.fmt.bufPrint(&msg_buf, fmt, args) catch fmt;
    format.writeJsonEscaped(writer, msg) catch {};
    writer.writeByte('"') catch {};

    // Caller
    if (caller) |c| {
        writer.writeAll(",\"caller\":\"") catch {};
        writer.print("{s}:{d}", .{ format.shortenPath(c.file), c.line }) catch {};
        writer.writeByte('"') catch {};
    }

    // Fields (comptime iteration)
    inline for (fields) |field| {
        writer.writeAll(",\"") catch {};
        writer.writeAll(field.key) catch {};
        writer.writeAll("\":") catch {};
        field.formatJsonValue(writer) catch {};
    }

    writer.writeAll("}\n") catch {};
}

fn formatFmtCompactEntry(
    writer: anytype,
    level: Level,
    comptime fmt: []const u8,
    args: anytype,
    fields: anytype,
) void {
    // Level (single char)
    writer.writeByte(switch (level) {
        .trace => 'T',
        .debug => 'D',
        .info => 'I',
        .warn => 'W',
        .err => 'E',
        .fatal => 'F',
    }) catch {};
    writer.writeByte(' ') catch {};

    // Message
    writer.print(fmt, args) catch {};

    // Fields
    inline for (fields) |field| {
        writer.writeByte(' ') catch {};
        writer.writeAll(field.key) catch {};
        writer.writeByte('=') catch {};
        field.formatCompactValue(writer) catch {};
    }

    writer.writeByte('\n') catch {};
}

// =============================================================================
// std.log Bridge (for compatibility)
// =============================================================================

/// Custom log function for std.log compatibility.
/// Use in main.zig: `pub const std_options = .{ .logFn = log.stdLogFn };`
pub fn stdLogFn(
    comptime level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime fmt: []const u8,
    args: anytype,
) void {
    const log_level = Level.fromStd(level);
    if (@intFromEnum(log_level) < @intFromEnum(global.level)) {
        return;
    }

    // Format the message into a stack buffer
    var msg_buf: [4096]u8 = undefined;
    const msg = std.fmt.bufPrint(&msg_buf, fmt, args) catch return;

    const scope_name = @tagName(scope);
    const has_scope = !std.mem.eql(u8, scope_name, "default");

    // Stack buffer for output
    var buf: [8192]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    const timestamp = @import("time.zig").nanoTimestamp();
    const colors = global.use_colors;

    // Simple text format for std.log bridge
    if (global.show_timestamp) {
        if (colors) writer.writeAll(Color.dim) catch {};
        format.writeShortTimestamp(writer, timestamp) catch {};
        if (colors) writer.writeAll(Color.reset) catch {};
        writer.writeByte(' ') catch {};
    }

    if (colors) writer.writeAll(Color.forLevel(log_level)) catch {};
    writer.writeAll(log_level.toShort()) catch {};
    if (colors) writer.writeAll(Color.reset) catch {};
    writer.writeByte(' ') catch {};

    if (has_scope) {
        if (colors) writer.writeAll(Color.cyan) catch {};
        writer.writeByte('[') catch {};
        writer.writeAll(scope_name) catch {};
        writer.writeAll("] ") catch {};
        if (colors) writer.writeAll(Color.reset) catch {};
    }

    writer.writeAll(msg) catch {};
    writer.writeByte('\n') catch {};

    const output_bytes = fbs.buffered();
    const use_stderr = (log_level == .err or log_level == .fatal);
    if (use_stderr) {
        _ = @import("io.zig").writeFd(std.posix.STDERR_FILENO, output_bytes);
    } else {
        _ = @import("io.zig").writeFd(std.posix.STDOUT_FILENO, output_bytes);
    }
}

// =============================================================================
// Testing Utilities
// =============================================================================

/// Buffer logger for testing - captures output without writing to console.
pub const TestLogger = struct {
    buffer: std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) TestLogger {
        return .{ .buffer = .{}, .allocator = allocator };
    }

    pub fn deinit(self: *TestLogger) void {
        self.buffer.deinit(self.allocator);
    }

    pub fn getWritten(self: *const TestLogger) []const u8 {
        return self.buffer.items;
    }

    pub fn clear(self: *TestLogger) void {
        self.buffer.clearRetainingCapacity();
    }

    /// Log to this buffer instead of stdout.
    pub fn logTo(self: *TestLogger, level: Level, comptime msg: []const u8, fields: anytype) void {
        const w = self.buffer.writer(self.allocator);
        formatTextEntry(w, level, msg, @import("time.zig").nanoTimestamp(), null, fields);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "format string logging (default)" {
    configure(.{ .level = .debug, .format = .text, .colors = false });

    // Simple format string
    info("Server starting", .{});
    info("Listening on port {d}", .{9000});
    debug("Config loaded from {s}", .{"flo.toml"});
}

test "structured logging with fields" {
    configure(.{ .level = .debug, .colors = false });

    infoWith("request handled", .{
        Field.str("method", "GET"),
        Field.int("status", 200),
    });
}

test "scoped logger format string" {
    configure(.{ .level = .debug, .colors = false });

    const storage = scoped(.storage);
    storage.info("WAL flushed {d} bytes", .{1024});
    storage.debug("Compaction level {d}", .{1});
}

test "scoped logger with fields" {
    configure(.{ .level = .debug, .colors = false });

    const storage = scoped(.storage);
    storage.infoWith("segment rotated", .{
        Field.size("bytes", 1024),
        Field.int("segment_id", 42),
    });
}

test "context logger format string" {
    configure(.{ .level = .debug, .colors = false });

    const req_log = scoped(.http).with(.{
        Field.requestId("req-123"),
        Field.int("user_id", 42),
    });

    // Format string + context fields
    req_log.info("Request received for {s}", .{"/api/users"});
    req_log.debug("Processing took {d}ms", .{15});
}

test "level parsing" {
    try std.testing.expect(setLevelFromString("debug"));
    try std.testing.expectEqual(Level.debug, getLevel());

    try std.testing.expect(setLevelFromString("INFO"));
    try std.testing.expectEqual(Level.info, getLevel());

    try std.testing.expect(!setLevelFromString("invalid"));
}

test "test logger buffer" {
    configure(.{ .level = .debug, .colors = false });

    const allocator = std.testing.allocator;
    var test_log = TestLogger.init(allocator);
    defer test_log.deinit();

    test_log.logTo(.info, "test message", .{
        Field.str("key", "value"),
    });

    const output = test_log.buffered();
    try std.testing.expect(std.mem.indexOf(u8, output, "test message") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "key=") != null);
}
