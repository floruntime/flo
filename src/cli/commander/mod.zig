//! Commander CLI Framework
//!
//! A sophisticated, idiomatic Zig CLI framework inspired by spf13/cobra and zig-clap.
//!
//! ## Features
//!
//! - **Comptime Parameter Parsing**: Define flags with a DSL string, parsed at compile time
//! - **Type-safe Results**: Access parsed values with compile-time type safety
//! - **Hierarchical Commands**: Support for commands, subcommands, and nested subcommands
//! - **Flag Parsing**: Long (--flag), short (-f), combined (-abc), and value syntax (--flag=value)
//! - **Persistent Flags**: Flags that propagate to all subcommands
//! - **Argument Validation**: Built-in validators for argument counts
//! - **Auto-generated Help**: Beautiful help output with usage, examples, and flag descriptions
//! - **Shell Completion**: Generate completion scripts for bash, zsh, fish, and powershell
//! - **Pre/Post Hooks**: Run code before/after command execution
//! - **Builder Pattern**: Fluent API for constructing commands
//!
//! ## Comptime-First API (Recommended)
//!
//! ```zig
//! const std = @import("std");
//! const commander = @import("cli/commander/mod.zig");
//!
//! // Define params at comptime with a DSL
//! const params = comptime commander.parseParams(
//!     \\-h, --help              Display this help and exit.
//!     \\-p, --port <u16>        Port to listen on.
//!     \\-v, --verbose           Enable verbose output.
//!     \\    --config <str>      Configuration file path.
//!     \\<command>               Subcommand to run.
//! );
//!
//! pub fn main() !void {
//!     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//!     defer _ = gpa.deinit();
//!
//!     // Parse returns a typed struct
//!     var result = try commander.parse(&params, .{ .allocator = gpa.allocator() });
//!     defer result.deinit();
//!
//!     if (result.args.help != 0) {
//!         try commander.printHelp(&params, std.io.getStdOut().writer());
//!         return;
//!     }
//!
//!     const port = result.args.port orelse 8080;
//!     const verbose = result.args.verbose != 0;
//!     // result.positionals[0] is the command
//! }
//! ```
//!
//! ## Runtime Builder API
//!
//! For dynamic command trees built at runtime:
//!
//! ```zig
//! const root = try commander.Builder.init(allocator)
//!     .name("myapp")
//!     .about("My awesome application")
//!     .version("1.0.0")
//!     .persistentFlag("verbose", .{ .short = 'v', .desc = "Enable verbose output" })
//!     .subcommand(
//!         commander.Builder.init(allocator)
//!             .name("serve")
//!             .about("Start the server")
//!             .uintFlag("port", 'p', 8080, "Port to listen on")
//!             .action(serveCmd)
//!     )
//!     .build();
//! defer root.deinit();
//! ```
//! ```
//!
//! ## Builder Pattern
//!
//! For a more fluent API, use the builder:
//!
//! ```zig
//! const root = try cobra.builder.Builder.init(allocator)
//!     .name("myapp")
//!     .about("My awesome application")
//!     .version("1.0.0")
//!     .persistentFlag("verbose", .{ .short = 'v', .desc = "Enable verbose output" })
//!     .subcommand(
//!         cobra.builder.Builder.init(allocator)
//!             .name("serve")
//!             .about("Start the server")
//!             .intFlag("port", 'p', 8080, "Port to listen on")
//!             .action(serveCmd)
//!     )
//!     .build();
//! defer root.deinit();
//! ```

const std = @import("std");

// ==================== Comptime API (Recommended) ====================
// Idiomatic Zig approach with compile-time type safety

/// Comptime parameter parsing module
pub const comptime_parser = @import("comptime.zig");

/// Comptime command framework module
/// Provides full command/subcommand support with run functions
pub const comptime_cmd = @import("cmd.zig");

/// Parse parameters from a DSL string at compile time
/// Returns an array of Param(Help) that can be used with parse()
pub const parseParams = comptime_parser.parseParams;

/// Parameter type
pub const Param = comptime_parser.Param;

/// Help information for parameters
pub const Help = comptime_parser.Help;

/// Parameter names (short and long)
pub const Names = comptime_parser.Names;

/// Takes enum (none, one, many)
pub const Takes = comptime_parser.Takes;

/// Parse result type - use Result(&params, parsers) to get the type
pub const Result = comptime_parser.Result;

/// Parse command line arguments with compile-time type safety
pub const parse = comptime_parser.parse;

/// Value parsers for converting strings to types
pub const parsers = comptime_parser.parsers;

/// Simple slice-based argument iterator
pub const SliceIterator = comptime_parser.SliceIterator;

/// Parse options
pub const ParseOptions = comptime_parser.ParseOptions;

/// Print help message
pub const printHelp = comptime_parser.printHelp;

/// Print usage string
pub const printUsage = comptime_parser.printUsage;

/// Command specification for comptime command framework
pub const CmdSpec = comptime_cmd.CmdSpec;

/// Generate a CLI type from a command specification
pub const Cli = comptime_cmd.Cli;

// ==================== Runtime API ====================
// For dynamic command trees built at runtime

// Re-export core types
pub const core = @import("core.zig");
pub const Command = core.Command;
pub const Context = core.Context;
pub const Flag = core.Flag;
pub const Arg = core.Arg;
pub const Value = core.Value;
pub const ValueType = core.ValueType;
pub const Error = core.Error;
pub const RunFn = core.RunFn;
pub const HookFn = core.HookFn;
pub const ArgValidatorFn = core.ArgValidatorFn;
pub const ArgValidators = core.ArgValidators;
pub const CommandOptions = core.CommandOptions;
pub const HelpSection = core.HelpSection;

// Builder pattern
pub const builder = @import("builder.zig");
pub const Builder = builder.Builder;
pub const FlagOpts = builder.FlagOpts;
pub const FlagValue = builder.FlagValue;

// Shell completion
pub const completion = @import("completion.zig");
pub const Shell = completion.Shell;
pub const generateCompletion = completion.generate;
pub const completionCommand = completion.completionCommand;

// Convenience functions
pub const command = builder.command;
pub const rootCommand = builder.rootCommand;

/// Create a new command with the given options
pub fn newCommand(allocator: std.mem.Allocator, opts: CommandOptions) *Command {
    return Command.init(allocator, opts);
}

/// Create a new command builder
pub fn newBuilder(allocator: std.mem.Allocator) *Builder {
    return Builder.init(allocator);
}

// ==================== Testing ====================

test "module imports" {
    _ = core;
    _ = builder;
    _ = completion;
    _ = comptime_parser;
}

test "create simple command" {
    const allocator = std.testing.allocator;

    const cmd = newCommand(allocator, .{
        .name = "test",
        .short = "A test command",
    });
    defer cmd.deinit();

    try std.testing.expectEqualStrings("test", cmd.name);
}

test "builder creates command" {
    const allocator = std.testing.allocator;

    const cmd = try newBuilder(allocator)
        .name("test")
        .about("A test command")
        .version("1.0.0")
        .build();
    defer cmd.deinit();

    try std.testing.expectEqualStrings("test", cmd.name);
}

test "comptime parse params" {
    const params = comptime parseParams(
        \\-h, --help             Display this help.
        \\-p, --port <u16>       Port number.
        \\<file>                 Input file.
    );

    try std.testing.expectEqual(@as(usize, 3), params.len);
    try std.testing.expectEqualStrings("help", params[0].names.long.?);
    try std.testing.expectEqualStrings("port", params[1].names.long.?);
    try std.testing.expectEqualStrings("file", params[2].id.val);
}

test "comptime parse and execute" {
    const params = comptime parseParams(
        \\-v, --verbose          Verbose mode.
        \\-n, --number <u32>     A number.
    );

    var iter = SliceIterator{ .args = &.{ "-v", "-v", "--number", "42" } };
    var result = try parse(&params, parsers.default, &iter, .{
        .allocator = std.testing.allocator,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u8, 2), result.args.verbose);
    try std.testing.expectEqual(@as(u32, 42), result.args.number.?);
}
