//! Comptime Command Framework (EXPERIMENTAL)
//!
//! ⚠️ EXPERIMENTAL: This module is a work-in-progress. The API may change.
//! For production use, prefer the runtime Builder API in builder.zig.
//!
//! A compile-time CLI framework that provides type-safe commands, subcommands,
//! and run functions. This is the idiomatic Zig approach combining the best of
//! cobra-style command trees with zig-clap's comptime type safety.
//!
//! ## Features
//!
//! - **Comptime Command Trees**: Define entire CLI hierarchies at compile time
//! - **Type-safe Arguments**: Each command handler receives typed arguments
//! - **Persistent Flags**: Global flags automatically available to all subcommands
//! - **Auto-generated Help**: Help text generated from command definitions
//! - **Subcommand Dispatch**: Automatic routing to the correct handler
//!
//! ## Example
//!
//! ```zig
//! const cli = Cli(.{
//!     .name = "flo",
//!     .description = "High-performance streaming data infrastructure",
//!     .version = "0.1.0",
//!     .flags =
//!         \\-v, --verbose          Enable verbose output.
//!         \\-e, --endpoint <str>   Server endpoint.
//!     ,
//!     .commands = &.{
//!         .{
//!             .name = "server",
//!             .description = "Server management commands",
//!             .commands = &.{
//!                 .{
//!                     .name = "start",
//!                     .description = "Start the Flo server",
//!                     .flags =
//!                         \\-p, --port <u16>       Port to listen on.
//!                         \\-d, --data-dir <str>   Data directory.
//!                     ,
//!                 },
//!                 .{
//!                     .name = "stop",
//!                     .description = "Stop the Flo server",
//!                 },
//!             },
//!         },
//!         .{
//!             .name = "kv",
//!             .description = "Key-value store operations",
//!             .commands = &.{
//!                 .{
//!                     .name = "get",
//!                     .description = "Get a value by key",
//!                     .args = \\<str>  Key to retrieve.\\ ,
//!                 },
//!             },
//!         },
//!     },
//! });
//!
//! pub fn main() !void {
//!     try cli.run(.{
//!         .@"server start" = startServer,
//!         .@"server stop" = stopServer,
//!         .@"kv get" = kvGet,
//!     });
//! }
//!
//! fn startServer(ctx: cli.Context("server start")) !void {
//!     const port = ctx.flags.port orelse 8080;
//!     const verbose = ctx.global.verbose;
//!     // ...
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const comptime_parser = @import("comptime.zig");

/// Command specification for comptime CLI definition
pub const CmdSpec = struct {
    /// Command name (used in CLI invocation)
    name: []const u8,
    /// Short description for help text
    description: []const u8 = "",
    /// Long description for detailed help
    long_description: []const u8 = "",
    /// Version string (only for root command)
    version: []const u8 = "",
    /// Flag definitions using the DSL format
    flags: []const u8 = "",
    /// Positional argument definitions
    args: []const u8 = "",
    /// Subcommands
    commands: []const CmdSpec = &.{},
    /// Whether this command can be run directly (vs just a namespace)
    runnable: bool = true,
    /// Aliases for the command
    aliases: []const []const u8 = &.{},
    /// Hide from help output
    hidden: bool = false,
};

/// Generate a CLI type from a command specification
pub fn Cli(comptime root_spec: CmdSpec) type {
    return struct {
        const Self = @This();
        pub const spec = root_spec;

        // Parse all flags and args at comptime
        const root_flags = if (root_spec.flags.len > 0)
            comptime_parser.parseParams(root_spec.flags)
        else
            [0]comptime_parser.Param(comptime_parser.Help){};

        const root_args = if (root_spec.args.len > 0)
            comptime_parser.parseParams(root_spec.args)
        else
            [0]comptime_parser.Param(comptime_parser.Help){};

        /// Global flags type (available to all subcommands)
        pub const GlobalFlags = @TypeOf(@as(comptime_parser.Result(&root_flags, comptime_parser.parsers.default), undefined).args);

        /// Get the context type for a specific command path
        pub fn Context(comptime path: []const u8) type {
            const cmd_spec = findCommand(path) orelse @compileError("Unknown command: " ++ path);
            return ContextType(cmd_spec, path);
        }

        /// Generate context type for a command
        fn ContextType(comptime cmd_spec: CmdSpec, comptime path: []const u8) type {
            const cmd_flags = if (cmd_spec.flags.len > 0)
                comptime_parser.parseParams(cmd_spec.flags)
            else
                [0]comptime_parser.Param(comptime_parser.Help){};

            const cmd_args = if (cmd_spec.args.len > 0)
                comptime_parser.parseParams(cmd_spec.args)
            else
                [0]comptime_parser.Param(comptime_parser.Help){};

            const FlagsType = if (cmd_flags.len > 0)
                @TypeOf(@as(comptime_parser.Result(&cmd_flags, comptime_parser.parsers.default), undefined).args)
            else
                struct {};

            const ArgsType = if (cmd_args.len > 0)
                @TypeOf(@as(comptime_parser.Result(&cmd_args, comptime_parser.parsers.default), undefined).positionals)
            else
                struct {};

            return struct {
                /// Global flags (from root command)
                global: GlobalFlags,
                /// Command-specific flags
                flags: FlagsType,
                /// Positional arguments
                positionals: ArgsType,
                /// The command path that was invoked
                command: []const u8,
                /// Remaining unparsed arguments
                remaining: []const []const u8,
                /// Allocator for any allocations needed
                allocator: Allocator,
                /// Standard output writer
                stdout: std.fs.File,
                /// Standard error writer
                stderr: std.fs.File,

                const Ctx = @This();

                /// Print to stdout
                pub fn print(self: *const Ctx, comptime fmt: []const u8, args_tuple: anytype) void {
                    self.stdout.writer().print(fmt, args_tuple) catch {};
                }

                /// Print to stderr
                pub fn printErr(self: *const Ctx, comptime fmt: []const u8, args_tuple: anytype) void {
                    self.stderr.writer().print(fmt, args_tuple) catch {};
                }

                /// Get the command spec
                pub fn cmdSpec() CmdSpec {
                    return cmd_spec;
                }

                /// Get the command path
                pub fn commandPath() []const u8 {
                    return path;
                }
            };
        }

        /// Find a command spec by path (e.g., "server start") - comptime version
        fn findCommand(comptime path: []const u8) ?CmdSpec {
            if (path.len == 0) return root_spec;

            var current = root_spec;
            var it = std.mem.splitScalar(u8, path, ' ');

            while (it.next()) |part| {
                var found = false;
                for (current.commands) |sub| {
                    if (std.mem.eql(u8, sub.name, part)) {
                        current = sub;
                        found = true;
                        break;
                    }
                    // Check aliases
                    for (sub.aliases) |alias| {
                        if (std.mem.eql(u8, alias, part)) {
                            current = sub;
                            found = true;
                            break;
                        }
                    }
                    if (found) break;
                }
                if (!found) return null;
            }
            return current;
        }

        /// Find a command spec by path - runtime version
        fn findCommandRuntime(path: []const u8) ?CmdSpec {
            if (path.len == 0) return root_spec;

            var current = root_spec;
            var it = std.mem.splitScalar(u8, path, ' ');

            while (it.next()) |part| {
                var found = false;
                for (current.commands) |sub| {
                    if (std.mem.eql(u8, sub.name, part)) {
                        current = sub;
                        found = true;
                        break;
                    }
                }
                if (!found) return null;
            }
            return current;
        }

        /// Handler function type for a command
        pub fn Handler(comptime path: []const u8) type {
            return *const fn (*Context(path)) anyerror!void;
        }

        /// Run configuration
        pub const RunConfig = struct {
            allocator: ?Allocator = null,
            args: ?[]const []const u8 = null,
        };

        /// Run the CLI with the given handlers
        pub fn run(comptime handlers: anytype, config: RunConfig) !void {
            const allocator = config.allocator orelse blk: {
                var gpa = std.heap.GeneralPurposeAllocator(.{}){};
                break :blk gpa.allocator();
            };

            // Get arguments
            const args = if (config.args) |a| a else blk: {
                var arg_iter = try std.process.argsWithAllocator(allocator);
                defer arg_iter.deinit();
                var list: std.ArrayListUnmanaged([]const u8) = .{};
                while (arg_iter.next()) |arg| {
                    try list.append(allocator, try allocator.dupe(u8, arg));
                }
                break :blk try list.toOwnedSlice(allocator);
            };
            defer {
                for (args) |arg| allocator.free(arg);
                allocator.free(args);
            }

            // Skip program name
            const cmd_args = if (args.len > 0) args[1..] else args;

            // Parse and dispatch
            try runImpl(handlers, allocator, cmd_args);
        }

        fn runImpl(comptime handlers: anytype, allocator: Allocator, args: []const []const u8) !void {
            var global_flags: GlobalFlags = .{};
            var remaining: std.ArrayListUnmanaged([]const u8) = .{};
            defer remaining.deinit(allocator);

            var i: usize = 0;

            // First pass: extract global flags
            while (i < args.len) {
                const arg = args[i];

                if (arg.len > 0 and arg[0] == '-') {
                    // Try to parse as global flag
                    if (parseGlobalFlag(&global_flags, arg, if (i + 1 < args.len) args[i + 1] else null)) |consumed| {
                        i += consumed;
                        continue;
                    }
                }

                try remaining.append(allocator, arg);
                i += 1;
            }

            // Find the command path
            const result = findCommandPath(remaining.items);
            const cmd_path = result.path;
            const cmd_args = result.remaining;

            // Check for help flag
            for (args) |arg| {
                if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
                    try printHelpForPath(cmd_path);
                    return;
                }
            }

            // Check for version flag (root only)
            if (cmd_path.len == 0) {
                for (args) |arg| {
                    if (std.mem.eql(u8, arg, "--version") or std.mem.eql(u8, arg, "-V")) {
                        var buf: [256]u8 = undefined;
                        var writer = std.fs.File.stdout().writer(&buf);
                        try writer.interface.print("{s} {s}\n", .{ root_spec.name, root_spec.version });
                        try writer.interface.flush();
                        return;
                    }
                }
            }

            // Dispatch to handler
            try dispatchCommand(handlers, cmd_path, global_flags, cmd_args, allocator);
        }

        fn parseGlobalFlag(flags: *GlobalFlags, arg: []const u8, next: ?[]const u8) ?usize {
            _ = flags;
            _ = arg;
            _ = next;
            // TODO: Implement global flag parsing
            // For now, return null to pass through
            return null;
        }

        const PathResult = struct {
            path: []const u8,
            remaining: []const []const u8,
        };

        fn findCommandPath(args: []const []const u8) PathResult {
            var path_parts: [16][]const u8 = undefined;
            var path_count: usize = 0;
            var current = root_spec;
            var consumed: usize = 0;

            for (args) |arg| {
                if (arg.len > 0 and arg[0] == '-') break;

                var found = false;
                for (current.commands) |sub| {
                    if (std.mem.eql(u8, sub.name, arg)) {
                        if (path_count < 16) {
                            path_parts[path_count] = sub.name;
                            path_count += 1;
                        }
                        current = sub;
                        found = true;
                        consumed += 1;
                        break;
                    }
                }
                if (!found) break;
            }

            // Join path
            var path_buf: [256]u8 = undefined;
            var path_len: usize = 0;
            for (path_parts[0..path_count], 0..) |part, idx| {
                if (idx > 0) {
                    path_buf[path_len] = ' ';
                    path_len += 1;
                }
                @memcpy(path_buf[path_len..][0..part.len], part);
                path_len += part.len;
            }

            return .{
                .path = path_buf[0..path_len],
                .remaining = args[consumed..],
            };
        }

        fn dispatchCommand(
            comptime handlers: anytype,
            path: []const u8,
            global_flags: GlobalFlags,
            cmd_args: []const []const u8,
            allocator: Allocator,
        ) !void {
            const HandlerFields = @typeInfo(@TypeOf(handlers)).@"struct".fields;

            inline for (HandlerFields) |field| {
                const handler_path = field.name;
                if (std.mem.eql(u8, path, handler_path)) {
                    const handler = @field(handlers, handler_path);
                    const CtxType = Context(handler_path);

                    var ctx = CtxType{
                        .global = global_flags,
                        .flags = .{},
                        .positionals = undefined,
                        .command = path,
                        .remaining = cmd_args,
                        .allocator = allocator,
                        .stdout = std.fs.File.stdout(),
                        .stderr = std.fs.File.stderr(),
                    };

                    // Parse command-specific flags
                    // TODO: Parse cmd_args into ctx.flags and ctx.positionals

                    return handler(&ctx);
                }
            }

            // No handler found - check if it's a namespace command
            const cmd_spec = findCommandRuntime(path);
            if (cmd_spec) |cs| {
                if (cs.commands.len > 0) {
                    // It's a namespace, show help
                    try printHelpForPath(path);
                    return;
                }
            }

            // Unknown command
            var err_buf: [512]u8 = undefined;
            var err_writer = std.fs.File.stderr().writer(&err_buf);
            try err_writer.interface.print("Unknown command: {s}\n", .{if (path.len > 0) path else "(root)"});
            try err_writer.interface.print("Run '{s} --help' for usage.\n", .{root_spec.name});
            try err_writer.interface.flush();
        }

        fn printHelpForPath(path: []const u8) !void {
            var help_buf: [4096]u8 = undefined;
            var help_writer = std.fs.File.stdout().writer(&help_buf);
            const writer = &help_writer.interface;
            defer writer.flush() catch {};

            const cmd_spec = findCommandRuntime(path) orelse root_spec;

            // Header
            if (path.len == 0) {
                try writer.print("{s}", .{root_spec.name});
                if (root_spec.version.len > 0) {
                    try writer.print(" v{s}", .{root_spec.version});
                }
                try writer.print("\n", .{});
            } else {
                try writer.print("{s} {s}\n", .{ root_spec.name, path });
            }

            if (cmd_spec.description.len > 0) {
                try writer.print("{s}\n", .{cmd_spec.description});
            }
            try writer.print("\n", .{});

            // Usage
            try writer.print("Usage:\n", .{});
            if (path.len == 0) {
                try writer.print("  {s} [OPTIONS] <COMMAND>\n\n", .{root_spec.name});
            } else {
                try writer.print("  {s} {s} [OPTIONS]", .{ root_spec.name, path });
                if (cmd_spec.commands.len > 0) {
                    try writer.print(" <COMMAND>", .{});
                }
                try writer.print("\n\n", .{});
            }

            // Commands
            if (cmd_spec.commands.len > 0) {
                try writer.print("Commands:\n", .{});
                for (cmd_spec.commands) |sub| {
                    if (sub.hidden) continue;
                    try writer.print("  {s: <16} {s}\n", .{ sub.name, sub.description });
                }
                try writer.print("\n", .{});
            }

            // Flags - print raw DSL for now (parsing needs comptime)
            if (cmd_spec.flags.len > 0) {
                try writer.print("Options:\n", .{});
                // Just print the raw flags string, can't parse at runtime
                var line_iter = std.mem.splitScalar(u8, cmd_spec.flags, '\n');
                while (line_iter.next()) |line| {
                    const trimmed = std.mem.trim(u8, line, " \t\\");
                    if (trimmed.len > 0) {
                        try writer.print("  {s}\n", .{trimmed});
                    }
                }
                try writer.print("\n", .{});
            }

            // Global options (for root)
            if (path.len == 0 and root_spec.flags.len > 0) {
                try writer.print("Global Options:\n", .{});
                var line_iter = std.mem.splitScalar(u8, root_spec.flags, '\n');
                while (line_iter.next()) |line| {
                    const trimmed = std.mem.trim(u8, line, " \t\\");
                    if (trimmed.len > 0) {
                        try writer.print("  {s}\n", .{trimmed});
                    }
                }
                try writer.print("\n", .{});
            }

            try writer.print("Run '{s} <command> --help' for more information on a command.\n", .{root_spec.name});
        }

        /// Print help for the root command
        pub fn printHelp() !void {
            try printHelpForPath("");
        }

        /// Print help for a specific command path
        pub fn printCommandHelp(comptime path: []const u8) !void {
            try printHelpForPath(path);
        }
    };
}

// ==================== Tests ====================

test "Cli basic definition" {
    const cli = Cli(.{
        .name = "test",
        .description = "A test CLI",
        .version = "1.0.0",
        .flags =
        \\-v, --verbose          Enable verbose output.
        ,
        .commands = &.{
            .{
                .name = "hello",
                .description = "Say hello",
                .flags =
                \\-n, --name <str>       Name to greet.
                ,
            },
        },
    });

    // Verify types are generated
    const GlobalFlags = cli.GlobalFlags;
    _ = GlobalFlags;

    const HelloCtx = cli.Context("hello");
    _ = HelloCtx;
}

test "Cli with nested commands" {
    const cli = Cli(.{
        .name = "flo",
        .description = "Streaming data infrastructure",
        .version = "0.1.0",
        .commands = &.{
            .{
                .name = "server",
                .description = "Server management",
                .commands = &.{
                    .{
                        .name = "start",
                        .description = "Start the server",
                        .flags =
                        \\-p, --port <u16>       Port number.
                        ,
                    },
                    .{
                        .name = "stop",
                        .description = "Stop the server",
                    },
                },
            },
        },
    });

    // Verify nested command context types
    const StartCtx = cli.Context("server start");
    _ = StartCtx;

    const StopCtx = cli.Context("server stop");
    _ = StopCtx;
}

test "Context has correct fields" {
    const cli = Cli(.{
        .name = "test",
        .description = "Test",
        .flags =
        \\-v, --verbose          Verbose.
        ,
        .commands = &.{
            .{
                .name = "cmd",
                .description = "A command",
                .flags =
                \\-n, --count <u32>      Count.
                ,
            },
        },
    });

    const Ctx = cli.Context("cmd");

    // Verify context has expected fields
    try std.testing.expect(@hasField(Ctx, "global"));
    try std.testing.expect(@hasField(Ctx, "flags"));
    try std.testing.expect(@hasField(Ctx, "positionals"));
    try std.testing.expect(@hasField(Ctx, "command"));
    try std.testing.expect(@hasField(Ctx, "allocator"));
}
