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

pub fn main(init: std.process.Init) !void {
    // Bind the stdx.io facade to the Init-provided Io for boundary code.
    @import("stdx").io.bootFromInit(init.io);

    // Convert process arguments into the [][]const u8 slice form that the
    // existing CLI expects.
    const raw_args = try init.minimal.args.toSlice(init.arena.allocator());
    const args = try init.gpa.alloc([]const u8, raw_args.len);
    defer init.gpa.free(args);
    for (raw_args, 0..) |a, i| args[i] = a;

    try cli.run(init.gpa, args);
}
