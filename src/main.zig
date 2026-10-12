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
const build_info = @import("build_info.zig");
const cli = @import("cli/mod.zig");
pub const log = @import("stdx").log;

/// Custom log function that uses the structured logger.
pub const std_options: std.Options = .{
    .logFn = log.stdLogFn,
    .log_level = .debug, // Allow all at compile time; filter at runtime
};

pub fn main(minimal: std.process.Init.Minimal) !void {
    // One root for every allocation, std's `Io` included: left to std, the
    // `Io` would take a `SafeAllocator` whatever `main` chose, and the two
    // would share buffers.
    const gpa = build_info.gpa();
    defer build_info.deinit();

    var arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator);
    defer arena.deinit();

    var threaded: std.Io.Threaded = .init(gpa, .{
        .argv0 = .init(minimal.args),
        .environ = minimal.environ,
    });
    defer threaded.deinit();

    // Bind the stdx.io facade to this Io for boundary code.
    @import("stdx").io.bootFromInit(threaded.io());

    // Convert process arguments into the [][]const u8 slice form that the
    // existing CLI expects.
    const raw_args = try minimal.args.toSlice(arena.allocator());
    const args = try gpa.alloc([]const u8, raw_args.len);
    defer gpa.free(args);
    for (raw_args, 0..) |a, i| args[i] = a;

    try cli.run(gpa, args);
}
