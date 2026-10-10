//! Commander CLI Framework
//!
//! A Zig CLI framework inspired by spf13/cobra.
//!
//! ## Features
//!
//! - **Hierarchical Commands**: Support for commands, subcommands, and nested subcommands
//! - **Flag Parsing**: Long (--flag), short (-f), combined (-abc), and value syntax (--flag=value)
//! - **Persistent Flags**: Flags that propagate to all subcommands
//! - **Argument Validation**: Built-in validators for argument counts
//! - **Auto-generated Help**: Beautiful help output with usage, examples, and flag descriptions
//! - **Shell Completion**: Generate completion scripts for bash, zsh, fish, and powershell
//! - **Pre/Post Hooks**: Run code before/after command execution
//! - **Builder Pattern**: Fluent API for constructing commands
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
