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
const apply_marker = @import("node/apply_marker.zig");
pub const log = @import("stdx").log;

/// Custom log function that uses the structured logger.
pub const std_options: std.Options = .{
    .logFn = log.stdLogFn,
    .log_level = .debug, // Allow all at compile time; filter at runtime
    // On in every build, not only safe ones: a fault in apply must reach
    // `debug.handleSegfault` below.
    .enable_segfault_handler = true,
};

/// A panic records the entry its thread was applying, if any, so the next
/// boot halts there instead of crashing on it again; then it panics as std
/// would.
pub const panic = std.debug.FullPanic(struct {
    fn call(msg: []const u8, first_trace_addr: ?usize) noreturn {
        apply_marker.recordCrash();
        std.debug.defaultPanic(msg, first_trace_addr);
    }
}.call);

/// The same for a fault: SIGSEGV (a stack overflow included), SIGILL,
/// SIGBUS and SIGFPE.
pub const debug = struct {
    pub fn handleSegfault(addr: ?usize, name: []const u8, ctx: ?std.debug.CpuContextPtr) noreturn {
        apply_marker.recordCrash();
        std.debug.defaultHandleSegfault(addr, name, ctx);
    }
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
