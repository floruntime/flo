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
const time = @import("time.zig");

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

    if (@backingInt(level) < @backingInt(global.level)) {
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

    if (@backingInt(level) < @backingInt(global.level)) {
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
    if (@backingInt(level) < @backingInt(global.level)) {
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

    if (@backingInt(level) < @backingInt(global.level)) {
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
    var buf: [line_buf_len]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    const timestamp = time.nanoTimestamp();

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
    var buf: [line_buf_len]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const writer = &fbs;

    const timestamp = time.nanoTimestamp();

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
    writeJsonLine(writer, level, msg, timestamp, caller, fields);
}

/// One JSON object per line. The message is escaped whole; callers cut an
/// overlong one before this (`formatMessage`), so the line always parses.
fn writeJsonLine(
    writer: anytype,
    level: Level,
    msg: []const u8,
    timestamp: i128,
    caller: ?format.Caller,
    fields: anytype,
) void {
    writer.writeByte('{') catch {};
    if (global.show_timestamp) writer.print("\"ts\":{d},", .{timestamp}) catch {};

    writer.writeAll("\"level\":\"") catch {};
    writer.writeAll(level.toString()) catch {};
    writer.writeByte('"') catch {};

    writer.writeAll(",\"msg\":\"") catch {};
    format.writeJsonEscaped(writer, msg) catch {};
    writer.writeByte('"') catch {};

    if (caller) |c| {
        writer.writeAll(",\"caller\":\"") catch {};
        writer.print("{s}:{d}", .{ format.shortenPath(c.file), c.line }) catch {};
        writer.writeByte('"') catch {};
    }

    inline for (fields) |field| {
        writer.writeAll(",\"") catch {};
        writer.writeAll(field.key) catch {};
        writer.writeAll("\":") catch {};
        field.formatJsonValue(writer) catch {};
    }

    writer.writeAll("}\n") catch {};
}

/// Longest message a log line carries; a longer one is cut.
pub const max_message_len = 4096;
/// Room for a line holding a message of `max_message_len` bytes even when
/// every byte escapes to six (`\u00XX`), plus the line's other fields.
const line_buf_len = 6 * max_message_len + 2048;
const cut_marker = "…";

/// `fmt` formatted into `buf`. A message that doesn't fit is cut at a UTF-8
/// boundary and ends with "…", so it is never dropped and never splits a
/// character.
fn formatMessage(buf: *[max_message_len]u8, comptime fmt: []const u8, args: anytype) []const u8 {
    var w: std.Io.Writer = .fixed(buf);
    w.print(fmt, args) catch {
        // Overflowed: make room for the marker, then back off to a boundary.
        const kept = utf8Prefix(w.buffered()[0 .. max_message_len - cut_marker.len]);
        @memcpy(buf[kept.len..][0..cut_marker.len], cut_marker);
        return buf[0 .. kept.len + cut_marker.len];
    };
    return w.buffered();
}

/// `s` without a trailing incomplete UTF-8 sequence.
fn utf8Prefix(s: []const u8) []const u8 {
    var i = s.len;
    // Step back over continuation bytes (10xxxxxx) to the sequence's lead.
    var back: usize = 0;
    while (i > 0 and back < 4 and s[i - 1] & 0xC0 == 0x80) : (back += 1) i -= 1;
    if (i == 0) return s[0..0];
    const lead = s[i - 1];
    const need: usize = if (lead < 0x80) 1 else if (lead & 0xE0 == 0xC0) 2 else if (lead & 0xF0 == 0xE0) 3 else if (lead & 0xF8 == 0xF0) 4 else 1;
    // The sequence starting at i-1 is complete only if all its bytes are here.
    return if (s.len - (i - 1) >= need) s else s[0 .. i - 1];
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
    var msg_buf: [max_message_len]u8 = undefined;
    writeJsonLine(writer, level, formatMessage(&msg_buf, fmt, args), timestamp, caller, fields);
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
    if (@backingInt(log_level) < @backingInt(global.level)) {
        return;
    }

    var buf: [line_buf_len]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    renderStdLine(&fbs, log_level, @tagName(scope), fmt, args, time.nanoTimestamp());

    const output_bytes = fbs.buffered();
    const use_stderr = (log_level == .err or log_level == .fatal);
    if (use_stderr) {
        _ = @import("io.zig").writeFd(std.posix.STDERR_FILENO, output_bytes);
    } else {
        _ = @import("io.zig").writeFd(std.posix.STDOUT_FILENO, output_bytes);
    }
}

/// What `stdLogFn` writes: the message formatted (an overlong one cut, never
/// dropped) and rendered in the configured format.
fn renderStdLine(writer: *std.Io.Writer, log_level: Level, scope_name: []const u8, comptime fmt: []const u8, args: anytype, timestamp: i128) void {
    var msg_buf: [max_message_len]u8 = undefined;
    formatStdEntry(writer, log_level, scope_name, formatMessage(&msg_buf, fmt, args), timestamp);
}

/// A std.log line in the configured format, so a JSON stream has no text
/// lines in it. A scope other than the default is the `component` field,
/// as stdx's scoped loggers write it. `global.format` is set once by
/// `configure`, before any thread that logs starts, so a plain read is safe.
fn formatStdEntry(writer: *std.Io.Writer, log_level: Level, scope_name: []const u8, msg: []const u8, timestamp: i128) void {
    const has_scope = !std.mem.eql(u8, scope_name, "default");
    switch (global.format) {
        .json => if (has_scope)
            writeJsonLine(writer, log_level, msg, timestamp, null, .{Field.component(scope_name)})
        else
            writeJsonLine(writer, log_level, msg, timestamp, null, .{}),
        .compact => if (has_scope)
            formatFmtCompactEntry(writer, log_level, "{s}", .{msg}, .{Field.component(scope_name)})
        else
            formatFmtCompactEntry(writer, log_level, "{s}", .{msg}, .{}),
        .text => {
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
        },
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
        return .{ .buffer = .empty, .allocator = allocator };
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
        var aw: std.Io.Writer.Allocating = .init(self.allocator);
        defer aw.deinit();
        formatTextEntry(&aw.writer, level, msg, time.nanoTimestamp(), null, fields);
        self.buffer.appendSlice(self.allocator, aw.written()) catch {};
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

    const output = test_log.getWritten();
    try std.testing.expect(std.mem.indexOf(u8, output, "test message") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "key=") != null);
}

fn withGlobal(fmt: Format, show_timestamp: bool) @TypeOf(global) {
    const saved = global;
    global.format = fmt;
    global.use_colors = false;
    global.show_timestamp = show_timestamp;
    return saved;
}

fn parseLine(line: []const u8) !std.json.Parsed(std.json.Value) {
    try std.testing.expect(std.mem.endsWith(u8, line, "\n"));
    return std.json.parseFromSlice(std.json.Value, std.testing.allocator, line[0 .. line.len - 1], .{});
}

test "std.log bridge writes JSON under format json, scope as component" {
    const saved = withGlobal(.json, true);
    defer global = saved;
    var buf: [line_buf_len]u8 = undefined;

    var w: std.Io.Writer = .fixed(&buf);
    formatStdEntry(&w, .warn, "reactor", "a \"quoted\" line", 42);
    const with_scope = try parseLine(w.buffered());
    defer with_scope.deinit();
    try std.testing.expectEqualStrings("a \"quoted\" line", with_scope.value.object.get("msg").?.string);
    try std.testing.expectEqualStrings("reactor", with_scope.value.object.get("component").?.string);
    try std.testing.expectEqualStrings(Level.warn.toString(), with_scope.value.object.get("level").?.string);

    w = .fixed(&buf);
    formatStdEntry(&w, .info, "default", "plain", 42);
    const unscoped = try parseLine(w.buffered());
    defer unscoped.deinit();
    try std.testing.expect(unscoped.value.object.get("component") == null);
}

test "std.log bridge text output is unchanged" {
    const saved = withGlobal(.text, false);
    defer global = saved;
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    formatStdEntry(&w, .info, "ts", "flushed", 0);
    var want_buf: [64]u8 = undefined;
    const want = try std.fmt.bufPrint(&want_buf, "{s} [ts] flushed\n", .{Level.info.toShort()});
    try std.testing.expectEqualStrings(want, w.buffered());
}

test "a JSON line without timestamps is valid JSON" {
    const saved = withGlobal(.json, false);
    defer global = saved;
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    formatStdEntry(&w, .err, "default", "x", 0);
    const parsed = try parseLine(w.buffered());
    defer parsed.deinit();
    try std.testing.expectEqualStrings("x", parsed.value.object.get("msg").?.string);
}

test "an overlong message is cut at a character boundary, and its JSON line still parses" {
    const saved = withGlobal(.json, true);
    defer global = saved;
    // 6 KiB of quotes, backslashes, controls and multibyte characters, so the
    // cut can fall inside an escape or a code point if it isn't careful.
    var long: [6 * 1024]u8 = undefined;
    const pattern = "ab\"\\\x01é€😀";
    for (&long, 0..) |*c, i| c.* = pattern[i % pattern.len];
    const raw = utf8Prefix(&long);

    var msg_buf: [max_message_len]u8 = undefined;
    const msg = formatMessage(&msg_buf, "{s}", .{raw});
    try std.testing.expect(msg.len <= max_message_len);
    try std.testing.expect(std.mem.endsWith(u8, msg, cut_marker));
    try std.testing.expect(std.unicode.utf8ValidateSlice(msg));

    var buf: [line_buf_len]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    formatStdEntry(&w, .info, "big", msg, 1);
    const parsed = try parseLine(w.buffered());
    defer parsed.deinit();
    try std.testing.expectEqualStrings(msg, parsed.value.object.get("msg").?.string);
}

test "a cut that would split a character drops the whole character" {
    // ASCII up to one byte before the cut, then a 4-byte character across it.
    const room = max_message_len - cut_marker.len;
    const msg_in = (@import("mod.zig").repeat("a", (room - 1))) ++ "😀" ++ (@import("mod.zig").repeat("b", 100));
    var msg_buf: [max_message_len]u8 = undefined;
    const msg = formatMessage(&msg_buf, "{s}", .{msg_in});
    try std.testing.expectEqualStrings((@import("mod.zig").repeat("a", (room - 1))) ++ cut_marker, msg);
}

test "a formatted JSON message is kept, not replaced by its format string" {
    const saved = withGlobal(.json, true);
    defer global = saved;
    var buf: [line_buf_len]u8 = undefined;

    // Fits: kept whole.
    const fits = @import("mod.zig").repeat("y", 2000);
    var w: std.Io.Writer = .fixed(&buf);
    formatFmtJsonEntry(&w, .info, "n={s}", .{fits}, 0, null, .{});
    const whole = try parseLine(w.buffered());
    defer whole.deinit();
    try std.testing.expectEqualStrings("n=" ++ fits, whole.value.object.get("msg").?.string);

    // Longer than a message may be: cut and marked, never the format string.
    const long = @import("mod.zig").repeat("z", (max_message_len + 500));
    w = .fixed(&buf);
    formatFmtJsonEntry(&w, .info, "n={s}", .{long}, 0, null, .{});
    const cut = try parseLine(w.buffered());
    defer cut.deinit();
    const msg = cut.value.object.get("msg").?.string;
    try std.testing.expect(std.mem.startsWith(u8, msg, "n=zzz"));
    try std.testing.expect(std.mem.endsWith(u8, msg, cut_marker));
    try std.testing.expectEqual(max_message_len, msg.len);
}

test "the installed std.log function keeps a message over the limit, cut and marked" {
    const saved = withGlobal(.json, true);
    defer global = saved;
    const long = @import("mod.zig").repeat("q", (max_message_len + 1000));
    var buf: [line_buf_len]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    renderStdLine(&w, .info, "reactor", "{s}", .{long}, 0);
    const parsed = try parseLine(w.buffered());
    defer parsed.deinit();
    const msg = parsed.value.object.get("msg").?.string;
    try std.testing.expectEqual(max_message_len, msg.len);
    try std.testing.expect(std.mem.endsWith(u8, msg, cut_marker));
}

test "a message exactly at the limit is not cut" {
    const exact = @import("mod.zig").repeat("e", max_message_len);
    var msg_buf: [max_message_len]u8 = undefined;
    try std.testing.expectEqualStrings(exact, formatMessage(&msg_buf, "{s}", .{exact}));
}

test "bytes that aren't UTF-8 become U+FFFD, and the JSON line parses" {
    const saved = withGlobal(.json, true);
    defer global = saved;
    var buf: [line_buf_len]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    formatStdEntry(&w, .info, "default", "k\xff\xfe\"\\ é \xc3", 0);
    const parsed = try parseLine(w.buffered());
    defer parsed.deinit();
    try std.testing.expectEqualStrings("k\u{fffd}\u{fffd}\"\\ é \u{fffd}", parsed.value.object.get("msg").?.string);
}
