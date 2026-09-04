//! Cobra - A Sophisticated CLI Command Framework for Zig
//!
//! Inspired by spf13/cobra (Go), this module provides a powerful and flexible
//! command-line interface framework with:
//!
//! - Hierarchical command structure (subcommands)
//! - Flag parsing (short/long, bool/string/int types)
//! - Persistent flags (inherited by subcommands)
//! - Automatic help generation
//! - Pre/Post run hooks
//! - Command aliases
//! - Required flags and arguments validation
//! - Shell completion generation (future)
//!
//! ## Example Usage
//!
//! ```zig
//! const root = Command.init(allocator, .{
//!     .name = "myapp",
//!     .short = "My awesome application",
//!     .long = "A longer description of what the app does.",
//!     .run = runRoot,
//! });
//! defer root.deinit();
//!
//! const server = Command.init(allocator, .{
//!     .name = "server",
//!     .short = "Server management commands",
//!     .aliases = &.{"srv"},
//! });
//!
//! try server.addFlag(.{
//!     .long = "port",
//!     .short = 'p',
//!     .description = "Port to listen on",
//!     .value_type = .int,
//!     .default = .{ .int = 8080 },
//! });
//!
//! try root.addCommand(server);
//! try root.execute(args);
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const File = std.posix.fd_t;

/// Value types for flags
pub const ValueType = enum {
    bool,
    string,
    int,
    int64,
    uint,
    uint64,
    float,
    string_array,
};

/// A union that holds the value of a flag
pub const Value = union(ValueType) {
    bool: bool,
    string: []const u8,
    int: i32,
    int64: i64,
    uint: u32,
    uint64: u64,
    float: f64,
    string_array: []const []const u8,

    pub fn format(self: Value, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        switch (self) {
            .bool => |v| try writer.print("{}", .{v}),
            .string => |v| try writer.print("{s}", .{v}),
            .int => |v| try writer.print("{d}", .{v}),
            .int64 => |v| try writer.print("{d}", .{v}),
            .uint => |v| try writer.print("{d}", .{v}),
            .uint64 => |v| try writer.print("{d}", .{v}),
            .float => |v| try writer.print("{d}", .{v}),
            .string_array => |v| {
                try writer.writeAll("[");
                for (v, 0..) |item, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try writer.print("\"{s}\"", .{item});
                }
                try writer.writeAll("]");
            },
        }
    }

    pub fn asString(self: Value) ?[]const u8 {
        return switch (self) {
            .string => |v| v,
            else => null,
        };
    }

    pub fn asBool(self: Value) ?bool {
        return switch (self) {
            .bool => |v| v,
            else => null,
        };
    }

    pub fn asInt(self: Value) ?i32 {
        return switch (self) {
            .int => |v| v,
            else => null,
        };
    }

    pub fn asInt64(self: Value) ?i64 {
        return switch (self) {
            .int64 => |v| v,
            else => null,
        };
    }

    pub fn asUint(self: Value) ?u32 {
        return switch (self) {
            .uint => |v| v,
            else => null,
        };
    }

    pub fn asUint64(self: Value) ?u64 {
        return switch (self) {
            .uint64 => |v| v,
            else => null,
        };
    }

    pub fn asFloat(self: Value) ?f64 {
        return switch (self) {
            .float => |v| v,
            else => null,
        };
    }

    pub fn asStringArray(self: Value) ?[]const []const u8 {
        return switch (self) {
            .string_array => |v| v,
            else => null,
        };
    }
};

/// Flag definition
pub const Flag = struct {
    /// Long name (e.g., "verbose" for --verbose)
    long: []const u8,
    /// Short name (e.g., 'v' for -v), 0 for none
    short: u8 = 0,
    /// Description shown in help
    description: []const u8 = "",
    /// Value type
    value_type: ValueType = .bool,
    /// Default value
    default: ?Value = null,
    /// Current value (after parsing)
    value: ?Value = null,
    /// Is this flag required?
    required: bool = false,
    /// Placeholder for value in usage (e.g., "FILE", "N")
    placeholder: []const u8 = "",
    /// Is this a persistent flag (inherited by subcommands)?
    persistent: bool = false,
    /// Is this flag hidden from help?
    hidden: bool = false,
    /// Has this flag been explicitly set?
    changed: bool = false,
    /// Deprecated message (if set, flag is deprecated)
    deprecated: ?[]const u8 = null,
    /// Environment variable to read from (optional)
    env_var: ?[]const u8 = null,

    /// Get the current value, falling back to default
    pub fn getValue(self: *const Flag) ?Value {
        return self.value orelse self.default;
    }

    /// Get string value
    pub fn getString(self: *const Flag) ?[]const u8 {
        if (self.getValue()) |v| return v.asString();
        return null;
    }

    /// Get bool value
    pub fn getBool(self: *const Flag) bool {
        if (self.getValue()) |v| {
            if (v.asBool()) |b| return b;
        }
        return false;
    }

    /// Get int value
    pub fn getInt(self: *const Flag) ?i32 {
        if (self.getValue()) |v| return v.asInt();
        return null;
    }

    /// Get uint value
    pub fn getUint(self: *const Flag) ?u32 {
        if (self.getValue()) |v| return v.asUint();
        return null;
    }

    /// Get int64 value
    pub fn getInt64(self: *const Flag) ?i64 {
        if (self.getValue()) |v| return v.asInt64();
        return null;
    }

    /// Get uint64 value
    pub fn getUint64(self: *const Flag) ?u64 {
        if (self.getValue()) |v| return v.asUint64();
        return null;
    }

    /// Format the flag for usage display
    pub fn usageString(self: *const Flag) []const u8 {
        return self.long;
    }
};

/// Argument definition for positional arguments
pub const Arg = struct {
    /// Name of the argument (for help display)
    name: []const u8,
    /// Description shown in help
    description: []const u8 = "",
    /// Is this argument required?
    required: bool = true,
    /// Is this a variadic argument (consumes all remaining)?
    variadic: bool = false,
    /// Default value
    default: ?[]const u8 = null,
};

/// Run function signature - uses *anyopaque to break the dependency cycle
/// Cast to *Context in the actual handler
pub const RunFn = *const fn (*anyopaque) Error!void;

/// Hook function signature (pre/post run) - also uses *anyopaque
pub const HookFn = *const fn (*anyopaque) Error!void;

/// Argument validator function - uses *anyopaque for Command
pub const ArgValidatorFn = *const fn (*anyopaque, []const []const u8) Error!void;

/// Error types for the command framework
pub const Error = error{
    UnknownCommand,
    UnknownFlag,
    MissingFlagValue,
    InvalidFlagValue,
    MissingRequiredFlag,
    MissingRequiredArg,
    TooManyArgs,
    InvalidArgs,
    CommandFailed,
    HelpRequested,
    VersionRequested,
    OutOfMemory,
    InvalidUtf8,
    Overflow,
};

/// Built-in argument validators
pub const ArgValidators = struct {
    /// No arguments allowed
    pub fn noArgs(cmd: *Command, args: []const []const u8) Error!void {
        if (args.len > 0) {
            cmd.printErrf("Error: '{s}' accepts no arguments\n", .{cmd.name});
            return error.TooManyArgs;
        }
    }

    /// Exactly N arguments required
    pub fn exactArgs(comptime n: usize) ArgValidatorFn {
        return struct {
            fn validate(cmd: *Command, args: []const []const u8) Error!void {
                if (args.len != n) {
                    cmd.printErrf("Error: '{s}' requires exactly {d} argument(s), got {d}\n", .{ cmd.name, n, args.len });
                    return error.InvalidArgs;
                }
            }
        }.validate;
    }

    /// Minimum N arguments required
    pub fn minArgs(comptime n: usize) ArgValidatorFn {
        return struct {
            fn validate(cmd: *Command, args: []const []const u8) Error!void {
                if (args.len < n) {
                    cmd.printErrf("Error: '{s}' requires at least {d} argument(s), got {d}\n", .{ cmd.name, n, args.len });
                    return error.InvalidArgs;
                }
            }
        }.validate;
    }

    /// Maximum N arguments allowed
    pub fn maxArgs(comptime n: usize) ArgValidatorFn {
        return struct {
            fn validate(cmd: *Command, args: []const []const u8) Error!void {
                if (args.len > n) {
                    cmd.printErrf("Error: '{s}' accepts at most {d} argument(s), got {d}\n", .{ cmd.name, n, args.len });
                    return error.TooManyArgs;
                }
            }
        }.validate;
    }

    /// Range of arguments (min to max inclusive)
    pub fn rangeArgs(comptime min: usize, comptime max: usize) ArgValidatorFn {
        return struct {
            fn validate(cmd: *Command, args: []const []const u8) Error!void {
                if (args.len < min or args.len > max) {
                    cmd.printErrf("Error: '{s}' requires {d} to {d} argument(s), got {d}\n", .{ cmd.name, min, max, args.len });
                    return error.InvalidArgs;
                }
            }
        }.validate;
    }

    /// Any number of arguments (always valid)
    pub fn arbitraryArgs(_: *Command, _: []const []const u8) Error!void {}
};

/// Execution context passed to run functions
pub const Context = struct {
    /// The command being executed
    command: *Command,
    /// Positional arguments (non-flag arguments)
    args: []const []const u8,
    /// The allocator
    allocator: Allocator,
    /// Standard output file
    stdout_file: File,
    /// Standard error file
    stderr_file: File,
    /// User-defined context data
    user_data: ?*anyopaque = null,

    /// Get a flag value by name
    pub fn getFlag(self: *Context, name: []const u8) ?*Flag {
        return self.command.getFlag(name);
    }

    /// Get string flag value
    pub fn getString(self: *Context, name: []const u8) ?[]const u8 {
        if (self.getFlag(name)) |flag| return flag.getString();
        return null;
    }

    /// Get bool flag value
    pub fn getBool(self: *Context, name: []const u8) bool {
        if (self.getFlag(name)) |flag| return flag.getBool();
        return false;
    }

    /// Get int flag value
    pub fn getInt(self: *Context, name: []const u8) ?i32 {
        if (self.getFlag(name)) |flag| return flag.getInt();
        return null;
    }

    /// Get uint flag value
    pub fn getUint(self: *Context, name: []const u8) ?u32 {
        if (self.getFlag(name)) |flag| return flag.getUint();
        return null;
    }

    /// Get changed uint flag value
    pub fn getChangedUint(self: *Context, name: []const u8) ?u32 {
        if (self.getFlag(name)) |flag| {
            if (flag.changed) {
                return flag.getUint();
            }
        }
        return null;
    }

    /// Get uint16 flag value (cast from uint)
    pub fn getUint16(self: *Context, name: []const u8) ?u16 {
        if (self.getUint(name)) |val| {
            return @intCast(val);
        }
        return null;
    }

    /// Get changed uint16 flag value (cast from uint)
    pub fn getChangedUint16(self: *Context, name: []const u8) ?u16 {
        if (self.getChangedUint(name)) |val| {
            return @intCast(val);
        }
        return null;
    }

    /// Get changed uint64 flag value
    pub fn getChangedUint64(self: *Context, name: []const u8) ?u64 {
        if (self.getFlag(name)) |flag| {
            if (flag.changed) {
                return flag.getUint64();
            }
        }
        return null;
    }

    /// Get int64 flag value
    pub fn getInt64(self: *Context, name: []const u8) ?i64 {
        if (self.getFlag(name)) |flag| return flag.getInt64();
        return null;
    }

    /// Get uint64 flag value
    pub fn getUint64(self: *Context, name: []const u8) ?u64 {
        if (self.getFlag(name)) |flag| return flag.getUint64();
        return null;
    }

    /// Get positional argument by name
    /// Looks up the argument index from the command's arg definitions
    pub fn getPositional(self: *Context, name: []const u8) ?[]const u8 {
        // Find the index of this positional arg in the command definition
        var idx: usize = 0;
        for (self.command.positional_args.items) |arg_def| {
            if (std.mem.eql(u8, arg_def.name, name)) {
                if (idx < self.args.len) {
                    return self.args[idx];
                }
                return null;
            }
            idx += 1;
        }
        // Fallback: try direct index access if args given without definitions
        return null;
    }

    /// Get variadic arguments by name (returns all remaining args after named positional args)
    /// The variadic arg must be marked with `variadic: true` in the command definition
    pub fn getVariadicArgs(self: *Context, name: []const u8) ?[]const []const u8 {
        // Find the variadic arg and its position
        var idx: usize = 0;
        for (self.command.positional_args.items) |arg_def| {
            if (std.mem.eql(u8, arg_def.name, name)) {
                if (arg_def.variadic) {
                    // Return all args from this position onward
                    if (idx < self.args.len) {
                        return self.args[idx..];
                    }
                    return &[_][]const u8{};
                }
                return null; // Not a variadic arg
            }
            idx += 1;
        }
        return null;
    }

    /// Check if flag was explicitly set
    pub fn flagChanged(self: *Context, name: []const u8) bool {
        if (self.getFlag(name)) |flag| return flag.changed;
        return false;
    }

    /// Print to stdout (unbuffered)
    pub fn print(self: *Context, comptime fmt: []const u8, fmtargs: anytype) void {
        writeFormatted(self.allocator, self.stdout_file, fmt, fmtargs);
    }

    /// Print to stderr (unbuffered)
    pub fn printErr(self: *Context, comptime fmt: []const u8, fmtargs: anytype) void {
        writeFormatted(self.allocator, self.stderr_file, fmt, fmtargs);
    }
};

/// Format into a stack buffer, or the heap when the message is larger: a
/// value over 4 KiB must print, not vanish.
fn writeFormatted(allocator: Allocator, file: File, comptime fmt: []const u8, fmtargs: anytype) void {
    var buf: [4096]u8 = undefined;
    if (std.fmt.bufPrint(&buf, fmt, fmtargs)) |msg| {
        _ = @import("stdx").io.writeFd(file, msg);
    } else |_| {
        const msg = std.fmt.allocPrint(allocator, fmt, fmtargs) catch return;
        defer allocator.free(msg);
        _ = @import("stdx").io.writeFd(file, msg);
    }
}

/// Command options for initialization
pub const CommandOptions = struct {
    /// Command name (used in CLI)
    name: []const u8,
    /// Short description (one line)
    short: []const u8 = "",
    /// Long description (multiple lines, shown in help)
    long: []const u8 = "",
    /// Example usage strings
    examples: []const []const u8 = &.{},
    /// Command aliases
    aliases: []const []const u8 = &.{},
    /// The run function
    run: ?RunFn = null,
    /// Pre-run hook (before run, after arg parsing)
    pre_run: ?HookFn = null,
    /// Post-run hook (after run completes)
    post_run: ?HookFn = null,
    /// Persistent pre-run (runs for this and all subcommands)
    persistent_pre_run: ?HookFn = null,
    /// Persistent post-run (runs for this and all subcommands)
    persistent_post_run: ?HookFn = null,
    /// Argument validator
    args_validator: ?ArgValidatorFn = null,
    /// Hide this command from help
    hidden: bool = false,
    /// Deprecated message
    deprecated: ?[]const u8 = null,
    /// Version string (for root command)
    version: ?[]const u8 = null,
    /// Custom usage string (overrides auto-generated)
    usage: ?[]const u8 = null,
    /// Silence errors (don't print to stderr)
    silence_errors: bool = false,
    /// Silence usage on error
    silence_usage: bool = false,
    /// Disable flag parsing (treat everything as args)
    disable_flag_parsing: bool = false,
    /// Disable suggestions for unknown commands
    disable_suggestions: bool = false,
    /// User data to pass to context
    user_data: ?*anyopaque = null,
    /// Group name for help organization
    group: []const u8 = "",
    /// Footer text shown after commands and flags (e.g., "Configuration:...")
    footer_text: []const u8 = "",
    /// Additional named sections for help (e.g., .{"Configuration", "Server config: ./flo.toml"})
    help_sections: []const HelpSection = &.{},
};

/// A named section for custom help text
pub const HelpSection = struct {
    title: []const u8,
    content: []const u8,
};

/// Command represents a CLI command or subcommand
pub const Command = struct {
    allocator: Allocator,

    // Command metadata
    name: []const u8,
    short: []const u8,
    long: []const u8,
    examples: []const []const u8,
    aliases: []const []const u8,
    usage: ?[]const u8,
    version: ?[]const u8,
    deprecated: ?[]const u8,
    group: []const u8,

    // Behavior flags
    hidden: bool,
    silence_errors: bool,
    silence_usage: bool,
    disable_flag_parsing: bool,
    disable_suggestions: bool,

    // Hooks
    run_fn: ?RunFn,
    pre_run: ?HookFn,
    post_run: ?HookFn,
    persistent_pre_run: ?HookFn,
    persistent_post_run: ?HookFn,
    args_validator: ?ArgValidatorFn,

    // Parent and children
    parent: ?*Command,
    commands: std.ArrayListUnmanaged(*Command),
    command_map: std.StringHashMapUnmanaged(*Command),

    // Flags (linear lookup - no cached pointers to avoid invalidation on ArrayList growth)
    flags: std.ArrayListUnmanaged(Flag),
    persistent_flags: std.ArrayListUnmanaged(Flag),

    // Positional arguments definition
    positional_args: std.ArrayListUnmanaged(Arg),

    // I/O (using File handles for Zig 0.15 compatibility)
    stdout_file: File,
    stderr_file: File,

    // User data
    user_data: ?*anyopaque,

    // Custom help content
    footer_text: []const u8,
    help_sections: []const HelpSection,

    // Help/version flags (auto-added)
    help_flag_added: bool,
    version_flag_added: bool,

    /// Initialize a new command
    pub fn init(allocator: Allocator, opts: CommandOptions) *Command {
        const cmd = allocator.create(Command) catch @panic("out of memory");

        // Dupe the aliases outer slice so Command owns it consistently,
        // regardless of whether the caller passed a static literal, an
        // inline Builder buffer, or a heap-allocated array. Inner
        // strings remain caller-owned (typically literals).
        const owned_aliases: []const []const u8 = if (opts.aliases.len == 0)
            &.{}
        else blk: {
            const buf = allocator.alloc([]const u8, opts.aliases.len) catch @panic("out of memory");
            @memcpy(buf, opts.aliases);
            break :blk buf;
        };

        cmd.* = Command{
            .allocator = allocator,
            .name = opts.name,
            .short = opts.short,
            .long = opts.long,
            .examples = opts.examples,
            .aliases = owned_aliases,
            .usage = opts.usage,
            .version = opts.version,
            .deprecated = opts.deprecated,
            .group = opts.group,
            .hidden = opts.hidden,
            .silence_errors = opts.silence_errors,
            .silence_usage = opts.silence_usage,
            .disable_flag_parsing = opts.disable_flag_parsing,
            .disable_suggestions = opts.disable_suggestions,
            .run_fn = opts.run,
            .pre_run = opts.pre_run,
            .post_run = opts.post_run,
            .persistent_pre_run = opts.persistent_pre_run,
            .persistent_post_run = opts.persistent_post_run,
            .args_validator = opts.args_validator,
            .parent = null,
            .commands = .empty,
            .command_map = .empty,
            .flags = .empty,
            .persistent_flags = .empty,
            .positional_args = .empty,
            .stdout_file = std.posix.STDOUT_FILENO,
            .stderr_file = std.posix.STDERR_FILENO,
            .user_data = opts.user_data,
            .footer_text = opts.footer_text,
            .help_sections = opts.help_sections,
            .help_flag_added = false,
            .version_flag_added = false,
        };

        return cmd;
    }

    /// Deinitialize and free all resources
    pub fn deinit(self: *Command) void {
        // Free child commands recursively
        for (self.commands.items) |child| {
            child.deinit();
        }
        self.commands.deinit(self.allocator);
        self.command_map.deinit(self.allocator);
        self.flags.deinit(self.allocator);
        self.persistent_flags.deinit(self.allocator);
        self.positional_args.deinit(self.allocator);
        // Aliases outer slice is owned by us (Builder dupes it in build()).
        // Inner strings are caller-owned (typically literals) and not freed.
        if (self.aliases.len != 0) self.allocator.free(self.aliases);
        self.allocator.destroy(self);
    }

    /// Set custom stdout file
    pub fn setOut(self: *Command, file: File) void {
        self.stdout_file = file;
    }

    /// Set custom stderr file
    pub fn setErr(self: *Command, file: File) void {
        self.stderr_file = file;
    }

    /// Add a subcommand
    pub fn addCommand(self: *Command, child: *Command) !void {
        child.parent = self;
        // Inherit I/O from parent
        child.stdout_file = self.stdout_file;
        child.stderr_file = self.stderr_file;

        try self.commands.append(self.allocator, child);
        try self.command_map.put(self.allocator, child.name, child);

        // Also register aliases
        for (child.aliases) |alias| {
            try self.command_map.put(self.allocator, alias, child);
        }
    }

    /// Add a local flag
    pub fn addFlag(self: *Command, flag: Flag) !void {
        try self.flags.append(self.allocator, flag);
    }

    /// Add a persistent flag (inherited by subcommands)
    pub fn addPersistentFlag(self: *Command, flag: Flag) !void {
        var persistent_flag = flag;
        persistent_flag.persistent = true;
        try self.persistent_flags.append(self.allocator, persistent_flag);
    }

    /// Add a positional argument definition
    pub fn addArg(self: *Command, arg: Arg) !void {
        try self.positional_args.append(self.allocator, arg);
    }

    /// Convenience: add string flag
    pub fn stringFlag(self: *Command, long: []const u8, short: u8, default: []const u8, desc: []const u8) !void {
        try self.addFlag(.{
            .long = long,
            .short = short,
            .description = desc,
            .value_type = .string,
            .default = .{ .string = default },
        });
    }

    /// Convenience: add string flag (persistent)
    pub fn stringFlagP(self: *Command, long: []const u8, short: u8, default: []const u8, desc: []const u8) !void {
        try self.addPersistentFlag(.{
            .long = long,
            .short = short,
            .description = desc,
            .value_type = .string,
            .default = .{ .string = default },
        });
    }

    /// Convenience: add bool flag
    pub fn boolFlag(self: *Command, long: []const u8, short: u8, default: bool, desc: []const u8) !void {
        try self.addFlag(.{
            .long = long,
            .short = short,
            .description = desc,
            .value_type = .bool,
            .default = .{ .bool = default },
        });
    }

    /// Convenience: add bool flag (persistent)
    pub fn boolFlagP(self: *Command, long: []const u8, short: u8, default: bool, desc: []const u8) !void {
        try self.addPersistentFlag(.{
            .long = long,
            .short = short,
            .description = desc,
            .value_type = .bool,
            .default = .{ .bool = default },
        });
    }

    /// Convenience: add int flag
    pub fn intFlag(self: *Command, long: []const u8, short: u8, default: i32, desc: []const u8) !void {
        try self.addFlag(.{
            .long = long,
            .short = short,
            .description = desc,
            .value_type = .int,
            .default = .{ .int = default },
        });
    }

    /// Convenience: add uint flag
    pub fn uintFlag(self: *Command, long: []const u8, short: u8, default: u32, desc: []const u8) !void {
        try self.addFlag(.{
            .long = long,
            .short = short,
            .description = desc,
            .value_type = .uint,
            .default = .{ .uint = default },
        });
    }

    /// Convenience: add int64 flag
    pub fn int64Flag(self: *Command, long: []const u8, short: u8, default: i64, desc: []const u8) !void {
        try self.addFlag(.{
            .long = long,
            .short = short,
            .description = desc,
            .value_type = .int64,
            .default = .{ .int64 = default },
        });
    }

    /// Convenience: add uint64 flag
    pub fn uint64Flag(self: *Command, long: []const u8, short: u8, default: u64, desc: []const u8) !void {
        try self.addFlag(.{
            .long = long,
            .short = short,
            .description = desc,
            .value_type = .uint64,
            .default = .{ .uint64 = default },
        });
    }

    /// Mark a flag as required
    pub fn markFlagRequired(self: *Command, name: []const u8) !void {
        if (self.flag_map.get(name)) |flag| {
            flag.required = true;
        }
    }

    /// Get a flag by name (includes inherited persistent flags)
    pub fn getFlag(self: *Command, name: []const u8) ?*Flag {
        // Check local flags first (linear lookup)
        for (self.flags.items) |*flag| {
            if (std.mem.eql(u8, flag.long, name)) {
                return flag;
            }
        }
        // Check local persistent flags
        for (self.persistent_flags.items) |*flag| {
            if (std.mem.eql(u8, flag.long, name)) {
                return flag;
            }
        }
        // Check parent's persistent flags
        var parent_ptr = self.parent;
        while (parent_ptr) |p| {
            for (p.persistent_flags.items) |*flag| {
                if (std.mem.eql(u8, flag.long, name)) {
                    return flag;
                }
            }
            parent_ptr = p.parent;
        }
        return null;
    }

    /// Get a flag by short name
    pub fn getFlagShort(self: *Command, short: u8) ?*Flag {
        // Check local flags first (linear lookup)
        for (self.flags.items) |*flag| {
            if (flag.short == short) {
                return flag;
            }
        }
        // Check local persistent flags
        for (self.persistent_flags.items) |*flag| {
            if (flag.short == short) {
                return flag;
            }
        }
        // Check parent's persistent flags
        var parent_ptr = self.parent;
        while (parent_ptr) |p| {
            for (p.persistent_flags.items) |*flag| {
                if (flag.short == short) {
                    return flag;
                }
            }
            parent_ptr = p.parent;
        }
        return null;
    }

    /// Check if a subcommand exists
    pub fn hasSubCommand(self: *Command, name: []const u8) bool {
        return self.command_map.contains(name);
    }

    /// Get a subcommand by name
    pub fn getSubCommand(self: *Command, name: []const u8) ?*Command {
        return self.command_map.get(name);
    }

    /// Get the root command
    pub fn root(self: *Command) *Command {
        var cmd = self;
        while (cmd.parent) |p| {
            cmd = p;
        }
        return cmd;
    }

    /// Get the full command path (e.g., "app server start")
    pub fn commandPath(self: *Command, allocator: Allocator) ![]const u8 {
        var parts: std.ArrayListUnmanaged([]const u8) = .empty;
        defer parts.deinit(allocator);

        var cmd: ?*Command = self;
        while (cmd) |c| {
            try parts.insert(allocator, 0, c.name);
            cmd = c.parent;
        }

        return std.mem.join(allocator, " ", parts.items);
    }

    /// Print to stdout (unbuffered)
    pub fn printf(self: *Command, comptime fmt: []const u8, fmtargs: anytype) void {
        writeFormatted(self.allocator, self.stdout_file, fmt, fmtargs);
    }

    /// Print to stderr (unbuffered)
    pub fn printErrf(self: *Command, comptime fmt: []const u8, fmtargs: anytype) void {
        writeFormatted(self.allocator, self.stderr_file, fmt, fmtargs);
    }

    // ==================== Execution ====================

    /// Execute the command with the given arguments
    pub fn execute(self: *Command, args: []const [:0]const u8) Error!void {
        // Convert [:0]const u8 to []const u8
        var converted = self.allocator.alloc([]const u8, args.len) catch return error.OutOfMemory;
        defer self.allocator.free(converted);
        for (args, 0..) |arg, i| {
            converted[i] = arg;
        }
        return self.executeSlice(converted);
    }

    /// Execute the command with string slice arguments
    pub fn executeSlice(self: *Command, args: []const []const u8) Error!void {
        // Skip program name if present
        const cmd_args = if (args.len > 0 and self.parent == null) args[1..] else args;
        return self.executeInternal(cmd_args);
    }

    fn executeInternal(self: *Command, args: []const []const u8) Error!void {
        // Add help flag if not already added
        if (!self.help_flag_added) {
            self.addFlag(.{
                .long = "help",
                .short = 'h',
                .description = "Show help for this command",
                .value_type = .bool,
                .default = .{ .bool = false },
            }) catch {};
            self.help_flag_added = true;
        }

        // Add version flag to root command
        if (self.version != null and !self.version_flag_added and self.parent == null) {
            self.addFlag(.{
                .long = "version",
                .short = 'v',
                .description = "Show version information",
                .value_type = .bool,
                .default = .{ .bool = false },
            }) catch {};
            self.version_flag_added = true;
        }

        // Find the target command and parse flags
        var target: *Command = self;
        var remaining_args: std.ArrayListUnmanaged([]const u8) = .empty;
        defer remaining_args.deinit(self.allocator);

        var i: usize = 0;
        while (i < args.len) {
            const arg = args[i];

            // Check for subcommand first (before flag parsing)
            if (!std.mem.startsWith(u8, arg, "-")) {
                if (target.command_map.get(arg)) |subcmd| {
                    target = subcmd;
                    // Add help flag to the subcommand if not already added
                    if (!target.help_flag_added) {
                        target.addFlag(.{
                            .long = "help",
                            .short = 'h',
                            .description = "Show help for this command",
                            .value_type = .bool,
                            .default = .{ .bool = false },
                        }) catch {};
                        target.help_flag_added = true;
                    }
                    i += 1;
                    continue;
                }
            }

            // Check for -- (end of flags)
            if (std.mem.eql(u8, arg, "--")) {
                // Preserve "--" in remaining args so commands can use it as a separator
                remaining_args.append(self.allocator, arg) catch return error.OutOfMemory;
                // All remaining args are positional (no more flag parsing)
                i += 1;
                while (i < args.len) : (i += 1) {
                    remaining_args.append(self.allocator, args[i]) catch return error.OutOfMemory;
                }
                break;
            }

            // Parse flags
            if (!target.disable_flag_parsing and std.mem.startsWith(u8, arg, "-")) {
                i = try target.parseFlag(args, i);
            } else {
                remaining_args.append(self.allocator, arg) catch return error.OutOfMemory;
                i += 1;
            }
        }

        // Check for help flag
        if (target.getFlag("help")) |help_flag| {
            if (help_flag.getBool()) {
                target.printHelp();
                return;
            }
        }

        // Check for version flag
        if (target.version != null) {
            if (target.getFlag("version")) |ver_flag| {
                if (ver_flag.getBool()) {
                    target.printf("{s}\n", .{target.version.?});
                    return;
                }
            }
        }

        // Show deprecation warning
        if (target.deprecated) |msg| {
            target.printErrf("Warning: '{s}' is deprecated: {s}\n", .{ target.name, msg });
        }

        // Validate required flags
        try target.validateFlags();

        // Validate arguments
        const positional_args = remaining_args.items;

        // First validate required positional args if defined
        if (target.positional_args.items.len > 0) {
            try target.validateArgs(positional_args);
        }

        // Then run custom validator if provided
        if (target.args_validator) |validator| {
            try validator(@ptrCast(target), positional_args);
        }

        // If no run function and has subcommands, show help
        if (target.run_fn == null) {
            if (target.commands.items.len > 0) {
                target.printHelp();
                return;
            }
            target.printErrf("Error: '{s}' is not runnable\n", .{target.name});
            return error.CommandFailed;
        }

        // Build context
        var ctx = Context{
            .command = target,
            .args = positional_args,
            .allocator = self.allocator,
            .stdout_file = target.stdout_file,
            .stderr_file = target.stderr_file,
            .user_data = target.user_data,
        };

        // Run persistent pre-run hooks up the chain
        try target.runPersistentPreRunHooks(&ctx);

        // Run pre-run hook
        if (target.pre_run) |hook| {
            try hook(@ptrCast(&ctx));
        }

        // Run the command
        try target.run_fn.?(@ptrCast(&ctx));

        // Run post-run hook
        if (target.post_run) |hook| {
            try hook(@ptrCast(&ctx));
        }

        // Run persistent post-run hooks up the chain
        try target.runPersistentPostRunHooks(&ctx);
    }

    fn runPersistentPreRunHooks(self: *Command, ctx: *Context) Error!void {
        // Run parent hooks first (in order from root to current)
        if (self.parent) |p| {
            try p.runPersistentPreRunHooks(ctx);
        }
        if (self.persistent_pre_run) |hook| {
            try hook(@ptrCast(ctx));
        }
    }

    fn runPersistentPostRunHooks(self: *Command, ctx: *Context) Error!void {
        if (self.persistent_post_run) |hook| {
            try hook(@ptrCast(ctx));
        }
        // Run parent hooks after (in order from current to root)
        if (self.parent) |p| {
            try p.runPersistentPostRunHooks(ctx);
        }
    }

    fn parseFlag(self: *Command, args: []const []const u8, start_idx: usize) Error!usize {
        const arg = args[start_idx];
        var idx = start_idx;

        if (std.mem.startsWith(u8, arg, "--")) {
            // Long flag
            const flag_part = arg[2..];

            // Check for --flag=value syntax
            if (std.mem.indexOf(u8, flag_part, "=")) |eq_pos| {
                const flag_name = flag_part[0..eq_pos];
                const flag_value = flag_part[eq_pos + 1 ..];

                if (self.getFlag(flag_name)) |flag| {
                    try self.setFlagValue(flag, flag_value);
                } else {
                    if (!self.silence_errors) {
                        self.printErrf("Error: unknown flag --{s}\n", .{flag_name});
                    }
                    return error.UnknownFlag;
                }
            } else {
                // --flag or --flag value
                if (self.getFlag(flag_part)) |flag| {
                    if (flag.value_type == .bool) {
                        flag.value = .{ .bool = true };
                        flag.changed = true;
                    } else {
                        // Need value
                        if (idx + 1 >= args.len) {
                            if (!self.silence_errors) {
                                self.printErrf("Error: flag --{s} requires a value\n", .{flag_part});
                            }
                            return error.MissingFlagValue;
                        }
                        idx += 1;
                        try self.setFlagValue(flag, args[idx]);
                    }
                } else {
                    if (!self.silence_errors) {
                        self.printErrf("Error: unknown flag --{s}\n", .{flag_part});
                    }
                    return error.UnknownFlag;
                }
            }
        } else if (arg.len > 1 and arg[0] == '-') {
            // Short flag(s)
            const short_flags = arg[1..];

            // Check for -f=value syntax
            if (std.mem.indexOf(u8, short_flags, "=")) |eq_pos| {
                if (eq_pos == 1) {
                    const short = short_flags[0];
                    const flag_value = short_flags[eq_pos + 1 ..];

                    if (self.getFlagShort(short)) |flag| {
                        try self.setFlagValue(flag, flag_value);
                    } else {
                        if (!self.silence_errors) {
                            self.printErrf("Error: unknown flag -{c}\n", .{short});
                        }
                        return error.UnknownFlag;
                    }
                }
            } else {
                // Could be multiple bool flags: -abc or single flag with value: -f value
                for (short_flags, 0..) |short, j| {
                    if (self.getFlagShort(short)) |flag| {
                        if (flag.value_type == .bool) {
                            flag.value = .{ .bool = true };
                            flag.changed = true;
                        } else {
                            // Non-bool flag - rest is value or next arg is value
                            if (j + 1 < short_flags.len) {
                                // Rest of string is the value
                                try self.setFlagValue(flag, short_flags[j + 1 ..]);
                                break;
                            } else {
                                // Next arg is value
                                if (idx + 1 >= args.len) {
                                    if (!self.silence_errors) {
                                        self.printErrf("Error: flag -{c} requires a value\n", .{short});
                                    }
                                    return error.MissingFlagValue;
                                }
                                idx += 1;
                                try self.setFlagValue(flag, args[idx]);
                            }
                        }
                    } else {
                        if (!self.silence_errors) {
                            self.printErrf("Error: unknown flag -{c}\n", .{short});
                        }
                        return error.UnknownFlag;
                    }
                }
            }
        }

        return idx + 1;
    }

    fn setFlagValue(self: *Command, flag: *Flag, value_str: []const u8) Error!void {
        _ = self;
        flag.changed = true;

        switch (flag.value_type) {
            .bool => {
                if (std.mem.eql(u8, value_str, "true") or
                    std.mem.eql(u8, value_str, "1") or
                    std.mem.eql(u8, value_str, "yes"))
                {
                    flag.value = .{ .bool = true };
                } else if (std.mem.eql(u8, value_str, "false") or
                    std.mem.eql(u8, value_str, "0") or
                    std.mem.eql(u8, value_str, "no"))
                {
                    flag.value = .{ .bool = false };
                } else {
                    return error.InvalidFlagValue;
                }
            },
            .string => {
                flag.value = .{ .string = value_str };
            },
            .int => {
                const val = std.fmt.parseInt(i32, value_str, 10) catch return error.InvalidFlagValue;
                flag.value = .{ .int = val };
            },
            .int64 => {
                const val = std.fmt.parseInt(i64, value_str, 10) catch return error.InvalidFlagValue;
                flag.value = .{ .int64 = val };
            },
            .uint => {
                const val = std.fmt.parseInt(u32, value_str, 10) catch return error.InvalidFlagValue;
                flag.value = .{ .uint = val };
            },
            .uint64 => {
                const val = std.fmt.parseInt(u64, value_str, 10) catch return error.InvalidFlagValue;
                flag.value = .{ .uint64 = val };
            },
            .float => {
                const val = std.fmt.parseFloat(f64, value_str) catch return error.InvalidFlagValue;
                flag.value = .{ .float = val };
            },
            .string_array => {
                // TODO: Implement array accumulation
                flag.value = .{ .string = value_str };
            },
        }
    }

    fn validateFlags(self: *Command) Error!void {
        // Check local required flags
        for (self.flags.items) |flag| {
            if (flag.required and !flag.changed and flag.value == null and flag.default == null) {
                if (!self.silence_errors) {
                    self.printErrf("Error: required flag --{s} not set\n", .{flag.long});
                }
                return error.MissingRequiredFlag;
            }
        }

        // Check inherited persistent required flags
        var parent_ptr = self.parent;
        while (parent_ptr) |p| {
            for (p.persistent_flags.items) |flag| {
                if (flag.required and !flag.changed and flag.value == null and flag.default == null) {
                    if (!self.silence_errors) {
                        self.printErrf("Error: required flag --{s} not set\n", .{flag.long});
                    }
                    return error.MissingRequiredFlag;
                }
            }
            parent_ptr = p.parent;
        }
    }

    fn validateArgs(self: *Command, args: []const []const u8) Error!void {
        // Check required positional arguments
        var required_count: usize = 0;
        for (self.positional_args.items) |arg_def| {
            if (arg_def.required) {
                required_count += 1;
            }
        }

        if (args.len < required_count) {
            // Find which argument is missing
            if (required_count > 0 and self.positional_args.items.len > args.len) {
                const missing_arg = self.positional_args.items[args.len];
                if (!self.silence_errors) {
                    self.printErrf("Error: missing required argument <{s}>\n", .{missing_arg.name});
                }
            } else if (!self.silence_errors) {
                self.printErrf("Error: expected {d} argument(s), got {d}\n", .{ required_count, args.len });
            }
            return error.MissingRequiredArg;
        }
    }

    // ==================== Help Generation ====================

    /// Print help for this command
    pub fn printHelp(self: *Command) void {
        // Description
        if (self.long.len > 0) {
            self.printf("{s}\n\n", .{self.long});
        } else if (self.short.len > 0) {
            self.printf("{s}\n\n", .{self.short});
        }

        // Usage
        self.printf("\x1b[1mUSAGE:\x1b[0m\n", .{});
        if (self.usage) |custom_usage| {
            self.printf("  {s}\n", .{custom_usage});
        } else {
            self.printUsageLine();
        }
        self.printf("\n", .{});

        // Aliases
        if (self.aliases.len > 0) {
            self.printf("\x1b[1mALIASES:\x1b[0m\n  {s}", .{self.name});
            for (self.aliases) |alias| {
                self.printf(", {s}", .{alias});
            }
            self.printf("\n\n", .{});
        }

        // Examples
        if (self.examples.len > 0) {
            self.printf("\x1b[1mEXAMPLES:\x1b[0m\n", .{});
            for (self.examples) |example| {
                self.printf("  {s}\n", .{example});
            }
            self.printf("\n", .{});
        }

        // Subcommands - organized by group
        var has_visible_commands = false;
        for (self.commands.items) |cmd| {
            if (!cmd.hidden) {
                has_visible_commands = true;
                break;
            }
        }

        if (has_visible_commands) {
            // Collect unique groups
            var groups: std.ArrayListUnmanaged([]const u8) = .empty;
            defer groups.deinit(self.allocator);

            for (self.commands.items) |cmd| {
                if (cmd.hidden) continue;

                // Check if group already exists
                var found = false;
                for (groups.items) |g| {
                    if (std.mem.eql(u8, g, cmd.group)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    groups.append(self.allocator, cmd.group) catch {};
                }
            }

            // Find max command name length for alignment (across all groups)
            var max_len: usize = 0;
            for (self.commands.items) |cmd| {
                if (!cmd.hidden and cmd.name.len > max_len) {
                    max_len = cmd.name.len;
                }
            }

            // Print commands by group
            for (groups.items) |group| {
                // Print group header
                if (group.len > 0) {
                    // Uppercase the group name
                    var upper = self.allocator.alloc(u8, group.len) catch {
                        self.printf("\x1b[1m{s}:\x1b[0m\n", .{group});
                        continue;
                    };
                    defer self.allocator.free(upper);
                    for (group, 0..) |c, i| {
                        upper[i] = std.ascii.toUpper(c);
                    }
                    self.printf("\x1b[1m{s}:\x1b[0m\n", .{upper});
                } else {
                    // Use "Available Commands" for ungrouped commands, but only if there are also grouped commands
                    var has_grouped = false;
                    for (groups.items) |g| {
                        if (g.len > 0) {
                            has_grouped = true;
                            break;
                        }
                    }
                    if (has_grouped) {
                        self.printf("\x1b[1mOTHER COMMANDS:\x1b[0m\n", .{});
                    } else {
                        self.printf("\x1b[1mAVAILABLE COMMANDS:\x1b[0m\n", .{});
                    }
                }

                // Print commands in this group
                for (self.commands.items) |cmd| {
                    if (cmd.hidden) continue;
                    if (!std.mem.eql(u8, cmd.group, group)) continue;

                    const padding = max_len - cmd.name.len + 2;
                    self.printf("  \x1b[36m{s}\x1b[0m", .{cmd.name});
                    var p: usize = 0;
                    while (p < padding) : (p += 1) {
                        self.printf(" ", .{});
                    }
                    self.printf("{s}\n", .{cmd.short});
                }
                self.printf("\n", .{});
            }
        }

        // Flags
        self.printFlagsHelp();

        // Custom help sections
        for (self.help_sections) |section| {
            self.printf("\x1b[1m{s}:\x1b[0m\n", .{section.title});
            // Split content by newlines and indent each line
            var iter = std.mem.splitScalar(u8, section.content, '\n');
            while (iter.next()) |line| {
                self.printf("  {s}\n", .{line});
            }
            self.printf("\n", .{});
        }

        // Footer text (unindented free-form text)
        if (self.footer_text.len > 0) {
            self.printf("{s}\n", .{self.footer_text});
        }

        // Command usage hint
        if (self.commands.items.len > 0) {
            const path = self.commandPath(self.allocator) catch self.name;
            defer if (path.ptr != self.name.ptr) self.allocator.free(path);
            self.printf("Use \"{s} [command] --help\" for more information about a command.\n", .{path});
        }
    }

    fn printUsageLine(self: *Command) void {
        const path = self.commandPath(self.allocator) catch self.name;
        defer if (path.ptr != self.name.ptr) self.allocator.free(path);

        self.printf("  {s}", .{path});

        if (self.commands.items.len > 0) {
            self.printf(" <command>", .{});
        }

        // Positional args
        for (self.positional_args.items) |arg| {
            if (arg.required) {
                self.printf(" <{s}>", .{arg.name});
            } else {
                self.printf(" \x1b[90m[{s}]\x1b[0m", .{arg.name});
            }
            if (arg.variadic) {
                self.printf("...", .{});
            }
        }

        // Count visible flags
        var has_flags = false;
        for (self.flags.items) |flag| {
            if (!flag.hidden) {
                has_flags = true;
                break;
            }
        }
        if (has_flags) {
            self.printf(" \x1b[90m[FLAGS...]\x1b[0m", .{});
        }

        self.printf("\n", .{});
    }

    fn printFlagsHelp(self: *Command) void {
        // Collect all flags (local + inherited persistent)
        var all_flags: std.ArrayListUnmanaged(*const Flag) = .empty;
        defer all_flags.deinit(self.allocator);

        // Local flags
        for (self.flags.items) |*flag| {
            if (!flag.hidden) {
                all_flags.append(self.allocator, flag) catch {};
            }
        }

        // Own persistent flags
        for (self.persistent_flags.items) |*flag| {
            if (!flag.hidden) {
                all_flags.append(self.allocator, flag) catch {};
            }
        }

        // Inherited persistent flags
        var parent_ptr = self.parent;
        while (parent_ptr) |p| {
            for (p.persistent_flags.items) |*flag| {
                if (!flag.hidden) {
                    all_flags.append(self.allocator, flag) catch {};
                }
            }
            parent_ptr = p.parent;
        }

        if (all_flags.items.len == 0) return;

        self.printf("\x1b[1mFLAGS:\x1b[0m\n", .{});

        // Find max flag name length for alignment
        var max_len: usize = 0;
        for (all_flags.items) |flag| {
            var len = flag.long.len + 2; // --
            if (flag.value_type != .bool) {
                len += 1 + (if (flag.placeholder.len > 0) flag.placeholder.len else flag.long.len);
            }
            if (len > max_len) max_len = len;
        }

        for (all_flags.items) |flag| {
            // Short flag
            if (flag.short != 0) {
                self.printf("  \x1b[36m-{c}\x1b[0m, ", .{flag.short});
            } else {
                self.printf("      ", .{});
            }

            // Long flag
            self.printf("--{s}", .{flag.long});

            var printed_len = flag.long.len + 2;

            // Value placeholder
            if (flag.value_type != .bool) {
                const placeholder = if (flag.placeholder.len > 0) flag.placeholder else flag.long;
                self.printf(" {s}", .{placeholder});
                printed_len += 1 + placeholder.len;
            }

            // Padding
            const padding = max_len - printed_len + 2;
            var p: usize = 0;
            while (p < padding) : (p += 1) {
                self.printf(" ", .{});
            }

            // Description
            self.printf("{s}", .{flag.description});

            // Default value
            if (flag.default) |def| {
                switch (def) {
                    .bool => |v| if (!v) {} else self.printf(" (default: true)", .{}),
                    .string => |v| if (v.len > 0) self.printf(" (default: \"{s}\")", .{v}),
                    .int => |v| self.printf(" (default: {d})", .{v}),
                    .int64 => |v| self.printf(" (default: {d})", .{v}),
                    .uint => |v| self.printf(" (default: {d})", .{v}),
                    .uint64 => |v| self.printf(" (default: {d})", .{v}),
                    .float => |v| self.printf(" (default: {d})", .{v}),
                    .string_array => {},
                }
            }

            if (flag.required) {
                self.printf(" (required)", .{});
            }

            self.printf("\n", .{});
        }

        self.printf("\n", .{});
    }
};

// ==================== Testing ====================

/// Test helper: wraps a fn(*Context) to fn(*anyopaque)
fn wrapTestRun(comptime handler: fn (*Context) Error!void) RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) Error!void {
            const ctx: *Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

test "basic command creation" {
    const allocator = std.testing.allocator;

    const cmd = Command.init(allocator, .{
        .name = "test",
        .short = "Test command",
    });
    defer cmd.deinit();

    try std.testing.expectEqualStrings("test", cmd.name);
    try std.testing.expectEqualStrings("Test command", cmd.short);
}

test "flag parsing" {
    const allocator = std.testing.allocator;

    var run_called = false;

    const cmd = Command.init(allocator, .{
        .name = "test",
        .run = wrapTestRun(struct {
            fn run(ctx: *Context) Error!void {
                const ptr: *bool = @ptrCast(@alignCast(ctx.user_data.?));
                ptr.* = true;

                // Verify flag values through context
                if (!ctx.getBool("verbose")) return error.CommandFailed;
                if ((ctx.getInt("port") orelse 0) != 8080) return error.CommandFailed;
                if (!std.mem.eql(u8, ctx.getString("name") orelse "", "hello")) return error.CommandFailed;
            }
        }.run),
        .user_data = @ptrCast(&run_called),
    });
    defer cmd.deinit();

    try cmd.boolFlag("verbose", 'v', false, "Enable verbose output");
    try cmd.intFlag("port", 'p', 3000, "Port number");
    try cmd.stringFlag("name", 'n', "", "Name");

    const args = &[_][]const u8{ "test", "--verbose", "-p", "8080", "--name=hello" };
    try cmd.executeSlice(args);

    try std.testing.expect(run_called);
}

test "subcommands" {
    const allocator = std.testing.allocator;

    var executed = false;

    const root_cmd = Command.init(allocator, .{ .name = "app" });
    defer root_cmd.deinit();

    const server_cmd = Command.init(allocator, .{
        .name = "server",
        .run = wrapTestRun(struct {
            fn run(ctx: *Context) Error!void {
                const ptr: *bool = @ptrCast(@alignCast(ctx.user_data.?));
                ptr.* = true;
            }
        }.run),
        .user_data = @ptrCast(&executed),
    });

    try root_cmd.addCommand(server_cmd);

    const args = &[_][]const u8{ "app", "server" };
    try root_cmd.executeSlice(args);

    try std.testing.expect(executed);
}
