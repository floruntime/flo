//! Flo - High-performance distributed storage platform
//!
//! A platform for building reliable and complex software without reinventing
//! the core plumbing of durability and communication every time.
//!
//! Usage:
//!   flo server start    Start the Flo server
//!   flo kv <command>    Key-value operations
//!   flo config <cmd>    Configuration management
//!   flo version         Show version
//!   flo help            Show help

const std = @import("std");
const cli = @import("cli/mod.zig");
pub const log = @import("stdx").log;

/// Custom log function that uses the structured logger.
pub const std_options: std.Options = .{
    .logFn = log.stdLogFn,
    .log_level = .debug, // Allow all at compile time; filter at runtime
};

/// Configure logging format and level at runtime.
pub fn configureLogging(format_str: ?[]const u8, level_str: ?[]const u8) void {
    var format_type = log.Format.text;
    if (format_str) |fmt| {
        format_type = log.Format.parse(fmt) orelse .text;
    }

    var level = log.Level.info;
    if (level_str) |lvl| {
        level = log.Level.parse(lvl) orelse .info;
    }

    log.configure(.{
        .level = level,
        .format = format_type,
    });
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    try cli.run(allocator, args);
}
