//! Commander Builder - Fluent API for building commands
//!
//! Provides a builder pattern for constructing commands with a more
//! ergonomic API, similar to Rust's clap crate.
//!
//! ## Example
//!
//! ```zig
//! const root = try Builder.init(allocator)
//!     .name("myapp")
//!     .about("My awesome application")
//!     .version("1.0.0")
//!     .flag("verbose", .{ .short = 'v', .desc = "Enable verbose output" })
//!     .flag("config", .{ .short = 'c', .value = .string, .desc = "Config file path" })
//!     .subcommand(
//!         Builder.init(allocator)
//!             .name("server")
//!             .about("Server commands")
//!             .subcommand(
//!                 Builder.init(allocator)
//!                     .name("start")
//!                     .about("Start the server")
//!                     .flag("port", .{ .short = 'p', .value = .{ .int = 8080 }, .desc = "Port" })
//!                     .action(startServer)
//!             )
//!     )
//!     .build();
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const cobra = @import("core.zig");
const Command = cobra.Command;
const Flag = cobra.Flag;
const Value = cobra.Value;
const ValueType = cobra.ValueType;
const RunFn = cobra.RunFn;
const HookFn = cobra.HookFn;
const ArgValidatorFn = cobra.ArgValidatorFn;
const Context = cobra.Context;
const Error = cobra.Error;

/// Flag options for builder
pub const FlagOpts = struct {
    short: u8 = 0,
    desc: []const u8 = "",
    value: FlagValue = .bool_false,
    required: bool = false,
    placeholder: []const u8 = "",
    hidden: bool = false,
    persistent: bool = false,
    env: ?[]const u8 = null,
    deprecated: ?[]const u8 = null,
};

/// Simplified flag value specification
pub const FlagValue = union(enum) {
    bool_false,
    bool_true,
    string: []const u8,
    int: i32,
    int64: i64,
    uint: u32,
    uint64: u64,
    float: f64,

    pub fn toValue(self: FlagValue) ?Value {
        return switch (self) {
            .bool_false => .{ .bool = false },
            .bool_true => .{ .bool = true },
            .string => |v| .{ .string = v },
            .int => |v| .{ .int = v },
            .int64 => |v| .{ .int64 = v },
            .uint => |v| .{ .uint = v },
            .uint64 => |v| .{ .uint64 = v },
            .float => |v| .{ .float = v },
        };
    }

    pub fn toValueType(self: FlagValue) ValueType {
        return switch (self) {
            .bool_false, .bool_true => .bool,
            .string => .string,
            .int => .int,
            .int64 => .int64,
            .uint => .uint,
            .uint64 => .uint64,
            .float => .float,
        };
    }
};

/// Fluent builder for commands
pub const Builder = struct {
    allocator: Allocator,

    // Command properties
    cmd_name: []const u8 = "",
    cmd_short: []const u8 = "",
    cmd_long: []const u8 = "",
    cmd_version: ?[]const u8 = null,
    cmd_usage: ?[]const u8 = null,
    cmd_aliases: []const []const u8 = &.{},
    cmd_examples: []const []const u8 = &.{},
    cmd_deprecated: ?[]const u8 = null,
    cmd_group: []const u8 = "",
    cmd_hidden: bool = false,
    cmd_footer_text: []const u8 = "",
    cmd_help_sections: []const cobra.HelpSection = &.{},

    // Hooks
    run_fn: ?RunFn = null,
    pre_run: ?HookFn = null,
    post_run: ?HookFn = null,
    persistent_pre_run: ?HookFn = null,
    persistent_post_run: ?HookFn = null,
    args_validator: ?ArgValidatorFn = null,

    // Collected items (using ArrayListUnmanaged for Zig 0.15 compatibility)
    flags_list: std.ArrayListUnmanaged(FlagDef) = .{},
    subcommands_list: std.ArrayListUnmanaged(*Builder) = .{},
    args_list: std.ArrayListUnmanaged(cobra.Arg) = .{},
    prebuilt_commands: std.ArrayListUnmanaged(*Command) = .{},

    // User data
    user_data: ?*anyopaque = null,

    const FlagDef = struct {
        name: []const u8,
        opts: FlagOpts,
    };

    pub fn init(allocator: Allocator) *Builder {
        const builder = allocator.create(Builder) catch @panic("out of memory");
        builder.* = Builder{
            .allocator = allocator,
            .flags_list = .{},
            .subcommands_list = .{},
            .args_list = .{},
            .prebuilt_commands = .{},
        };
        return builder;
    }

    pub fn deinit(self: *Builder) void {
        self.flags_list.deinit(self.allocator);
        for (self.subcommands_list.items) |sub| {
            sub.deinit();
        }
        self.subcommands_list.deinit(self.allocator);
        self.args_list.deinit(self.allocator);
        // Note: prebuilt commands are NOT freed here - they're owned by their creators
        self.prebuilt_commands.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    /// Set command name
    pub fn name(self: *Builder, n: []const u8) *Builder {
        self.cmd_name = n;
        return self;
    }

    /// Set short description (one line)
    pub fn about(self: *Builder, desc: []const u8) *Builder {
        self.cmd_short = desc;
        return self;
    }

    /// Set long description
    pub fn longAbout(self: *Builder, desc: []const u8) *Builder {
        self.cmd_long = desc;
        return self;
    }

    /// Set version
    pub fn version(self: *Builder, v: []const u8) *Builder {
        self.cmd_version = v;
        return self;
    }

    /// Set custom usage string
    pub fn usage(self: *Builder, u: []const u8) *Builder {
        self.cmd_usage = u;
        return self;
    }

    /// Add aliases
    pub fn aliases(self: *Builder, a: []const []const u8) *Builder {
        self.cmd_aliases = a;
        return self;
    }

    /// Add alias (single)
    pub fn alias(self: *Builder, a: []const u8) *Builder {
        // For single alias, we need to create an array
        const arr = self.allocator.alloc([]const u8, 1) catch return self;
        arr[0] = a;
        self.cmd_aliases = arr;
        return self;
    }

    /// Add examples
    pub fn examples(self: *Builder, e: []const []const u8) *Builder {
        self.cmd_examples = e;
        return self;
    }

    /// Mark as deprecated
    pub fn deprecated(self: *Builder, msg: []const u8) *Builder {
        self.cmd_deprecated = msg;
        return self;
    }

    /// Set group for help organization
    pub fn group(self: *Builder, g: []const u8) *Builder {
        self.cmd_group = g;
        return self;
    }

    /// Set footer text (shown at the end of help)
    pub fn footer(self: *Builder, text: []const u8) *Builder {
        self.cmd_footer_text = text;
        return self;
    }

    /// Add help sections (custom titled sections in help)
    pub fn helpSections(self: *Builder, sections: []const cobra.HelpSection) *Builder {
        self.cmd_help_sections = sections;
        return self;
    }

    /// Add a single help section
    pub fn helpSection(self: *Builder, title: []const u8, content: []const u8) *Builder {
        const section = self.allocator.alloc(cobra.HelpSection, 1) catch return self;
        section[0] = .{ .title = title, .content = content };
        self.cmd_help_sections = section;
        return self;
    }

    /// Hide from help
    pub fn hidden(self: *Builder) *Builder {
        self.cmd_hidden = true;
        return self;
    }

    /// Set the run function
    pub fn action(self: *Builder, f: RunFn) *Builder {
        self.run_fn = f;
        return self;
    }

    /// Set pre-run hook
    pub fn preRun(self: *Builder, f: HookFn) *Builder {
        self.pre_run = f;
        return self;
    }

    /// Set post-run hook
    pub fn postRun(self: *Builder, f: HookFn) *Builder {
        self.post_run = f;
        return self;
    }

    /// Set persistent pre-run hook
    pub fn persistentPreRun(self: *Builder, f: HookFn) *Builder {
        self.persistent_pre_run = f;
        return self;
    }

    /// Set persistent post-run hook
    pub fn persistentPostRun(self: *Builder, f: HookFn) *Builder {
        self.persistent_post_run = f;
        return self;
    }

    /// Set argument validator
    pub fn argsValidator(self: *Builder, f: ArgValidatorFn) *Builder {
        self.args_validator = f;
        return self;
    }

    /// Require no arguments
    pub fn noArgs(self: *Builder) *Builder {
        self.args_validator = cobra.ArgValidators.noArgs;
        return self;
    }

    /// Require exactly N arguments
    pub fn exactArgs(self: *Builder, comptime n: usize) *Builder {
        self.args_validator = cobra.ArgValidators.exactArgs(n);
        return self;
    }

    /// Require at least N arguments
    pub fn minArgs(self: *Builder, comptime n: usize) *Builder {
        self.args_validator = cobra.ArgValidators.minArgs(n);
        return self;
    }

    /// Allow at most N arguments
    pub fn maxArgs(self: *Builder, comptime n: usize) *Builder {
        self.args_validator = cobra.ArgValidators.maxArgs(n);
        return self;
    }

    /// Set user data
    pub fn userData(self: *Builder, data: *anyopaque) *Builder {
        self.user_data = data;
        return self;
    }

    /// Add a flag
    pub fn flag(self: *Builder, flag_name: []const u8, opts: FlagOpts) *Builder {
        self.flags_list.append(self.allocator, .{ .name = flag_name, .opts = opts }) catch {};
        return self;
    }

    /// Add a string flag (convenience)
    pub fn stringFlag(self: *Builder, flag_name: []const u8, short: u8, default: []const u8, desc: []const u8) *Builder {
        return self.flag(flag_name, .{ .short = short, .value = .{ .string = default }, .desc = desc });
    }

    /// Add a bool flag (convenience)
    pub fn boolFlag(self: *Builder, flag_name: []const u8, short: u8, desc: []const u8) *Builder {
        return self.flag(flag_name, .{ .short = short, .value = .bool_false, .desc = desc });
    }

    /// Add an int flag (convenience)
    pub fn intFlag(self: *Builder, flag_name: []const u8, short: u8, default: i32, desc: []const u8) *Builder {
        return self.flag(flag_name, .{ .short = short, .value = .{ .int = default }, .desc = desc });
    }

    /// Add a uint flag (convenience)
    pub fn uintFlag(self: *Builder, flag_name: []const u8, short: u8, default: u32, desc: []const u8) *Builder {
        return self.flag(flag_name, .{ .short = short, .value = .{ .uint = default }, .desc = desc });
    }

    /// Add an int64 flag (convenience)
    pub fn int64Flag(self: *Builder, flag_name: []const u8, short: u8, default: i64, desc: []const u8) *Builder {
        return self.flag(flag_name, .{ .short = short, .value = .{ .int64 = default }, .desc = desc });
    }

    /// Add a uint64 flag (convenience)
    pub fn uint64Flag(self: *Builder, flag_name: []const u8, short: u8, default: u64, desc: []const u8) *Builder {
        return self.flag(flag_name, .{ .short = short, .value = .{ .uint64 = default }, .desc = desc });
    }

    /// Add a float flag (convenience)
    pub fn floatFlag(self: *Builder, flag_name: []const u8, short: u8, default: f64, desc: []const u8) *Builder {
        return self.flag(flag_name, .{ .short = short, .value = .{ .float = default }, .desc = desc });
    }

    /// Add a persistent flag (convenience)
    pub fn persistentFlag(self: *Builder, flag_name: []const u8, opts: FlagOpts) *Builder {
        var persistent_opts = opts;
        persistent_opts.persistent = true;
        return self.flag(flag_name, persistent_opts);
    }

    /// Add a required flag
    pub fn requiredFlag(self: *Builder, flag_name: []const u8, opts: FlagOpts) *Builder {
        var required_opts = opts;
        required_opts.required = true;
        return self.flag(flag_name, required_opts);
    }

    /// Add a required positional argument definition
    /// The argument will be validated automatically - if missing, an error is shown
    pub fn arg(self: *Builder, arg_name: []const u8, desc: []const u8) *Builder {
        self.args_list.append(self.allocator, .{
            .name = arg_name,
            .description = desc,
            .required = true,
        }) catch {};
        return self;
    }

    /// Alias for arg() - adds a required positional argument
    /// Provides explicit naming for required arguments
    pub fn requiredArg(self: *Builder, arg_name: []const u8, desc: []const u8) *Builder {
        return self.arg(arg_name, desc);
    }

    /// Add an optional positional argument
    pub fn optionalArg(self: *Builder, arg_name: []const u8, desc: []const u8) *Builder {
        self.args_list.append(self.allocator, .{
            .name = arg_name,
            .description = desc,
            .required = false,
        }) catch {};
        return self;
    }

    /// Add a variadic argument (consumes remaining)
    pub fn variadicArg(self: *Builder, arg_name: []const u8, desc: []const u8) *Builder {
        self.args_list.append(self.allocator, .{
            .name = arg_name,
            .description = desc,
            .required = false,
            .variadic = true,
        }) catch {};
        return self;
    }

    /// Add a subcommand builder
    pub fn subcommand(self: *Builder, sub: *Builder) *Builder {
        self.subcommands_list.append(self.allocator, sub) catch {};
        return self;
    }

    /// Add a pre-built command as subcommand
    pub fn addCommand(self: *Builder, cmd: *Command) *Builder {
        self.prebuilt_commands.append(self.allocator, cmd) catch {};
        return self;
    }

    /// Build the command
    pub fn build(self: *Builder) !*Command {
        const cmd = Command.init(self.allocator, .{
            .name = self.cmd_name,
            .short = self.cmd_short,
            .long = self.cmd_long,
            .version = self.cmd_version,
            .usage = self.cmd_usage,
            .aliases = self.cmd_aliases,
            .examples = self.cmd_examples,
            .deprecated = self.cmd_deprecated,
            .group = self.cmd_group,
            .hidden = self.cmd_hidden,
            .footer_text = self.cmd_footer_text,
            .help_sections = self.cmd_help_sections,
            .run = self.run_fn,
            .pre_run = self.pre_run,
            .post_run = self.post_run,
            .persistent_pre_run = self.persistent_pre_run,
            .persistent_post_run = self.persistent_post_run,
            .args_validator = self.args_validator,
            .user_data = self.user_data,
        });

        // Add flags
        for (self.flags_list.items) |flag_def| {
            const f = Flag{
                .long = flag_def.name,
                .short = flag_def.opts.short,
                .description = flag_def.opts.desc,
                .value_type = flag_def.opts.value.toValueType(),
                .default = flag_def.opts.value.toValue(),
                .required = flag_def.opts.required,
                .placeholder = flag_def.opts.placeholder,
                .hidden = flag_def.opts.hidden,
                .persistent = flag_def.opts.persistent,
                .env_var = flag_def.opts.env,
                .deprecated = flag_def.opts.deprecated,
            };

            if (flag_def.opts.persistent) {
                try cmd.addPersistentFlag(f);
            } else {
                try cmd.addFlag(f);
            }
        }

        // Add positional args
        for (self.args_list.items) |a| {
            try cmd.addArg(a);
        }

        // Build and add subcommands from builders
        for (self.subcommands_list.items) |sub_builder| {
            const sub_cmd = try sub_builder.build();
            try cmd.addCommand(sub_cmd);
        }

        // Add prebuilt commands
        for (self.prebuilt_commands.items) |prebuilt_cmd| {
            try cmd.addCommand(prebuilt_cmd);
        }

        // Clean up the builder (it's consumed by build)
        // Note: subcommand builders are already cleaned up by their recursive build() calls
        self.flags_list.deinit(self.allocator);
        self.args_list.deinit(self.allocator);
        self.subcommands_list.deinit(self.allocator);
        self.prebuilt_commands.deinit(self.allocator);
        self.allocator.destroy(self);

        return cmd;
    }

    /// Build and immediately execute
    pub fn run(self: *Builder, args: []const [:0]const u8) !void {
        const cmd = try self.build();
        defer cmd.deinit();
        return cmd.execute(args);
    }
};

// ==================== Convenience Functions ====================

/// Create a new command builder
pub fn command(allocator: Allocator) *Builder {
    return Builder.init(allocator);
}

/// Create a new root command builder with name and version
pub fn rootCommand(allocator: Allocator, cmd_name: []const u8, ver: []const u8) *Builder {
    return Builder.init(allocator).name(cmd_name).version(ver);
}

// ==================== Testing ====================

/// Test helper: wraps a fn(*Context) to fn(*anyopaque)
fn wrapRun(comptime handler: fn (*Context) Error!void) RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) Error!void {
            const ctx: *Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

test "builder pattern" {
    const allocator = std.testing.allocator;

    var executed = false;

    const cmd = try Builder.init(allocator)
        .name("myapp")
        .about("My awesome application")
        .version("1.0.0")
        .boolFlag("verbose", 'v', "Enable verbose output")
        .intFlag("port", 'p', 8080, "Port number")
        .userData(@ptrCast(&executed))
        .action(wrapRun(struct {
            fn run(ctx: *Context) Error!void {
                const ptr: *bool = @ptrCast(@alignCast(ctx.user_data.?));
                ptr.* = true;
                // Verify flags are as expected - use simple condition checks
                if (ctx.getBool("verbose")) return error.CommandFailed;
                if ((ctx.getInt("port") orelse 0) != 8080) return error.CommandFailed;
            }
        }.run))
        .build();
    defer cmd.deinit();

    const args = &[_][]const u8{ "myapp", "--port", "8080" };
    try cmd.executeSlice(args);
    try std.testing.expect(executed);
}

test "builder with subcommands" {
    const allocator = std.testing.allocator;

    var server_executed = false;

    const cmd = try Builder.init(allocator)
        .name("app")
        .about("Main application")
        .subcommand(
            Builder.init(allocator)
                .name("server")
                .about("Server commands")
                .subcommand(
                Builder.init(allocator)
                    .name("start")
                    .about("Start the server")
                    .intFlag("port", 'p', 3000, "Port to listen on")
                    .userData(@ptrCast(&server_executed))
                    .action(wrapRun(struct {
                    fn run(ctx: *Context) Error!void {
                        const ptr: *bool = @ptrCast(@alignCast(ctx.user_data.?));
                        ptr.* = true;
                    }
                }.run)),
            ),
        )
        .build();
    defer cmd.deinit();

    const args = &[_][]const u8{ "app", "server", "start", "-p", "9000" };
    try cmd.executeSlice(args);
    try std.testing.expect(server_executed);
}
