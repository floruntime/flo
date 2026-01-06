//! Comptime CLI Parameter Parsing
//!
//! Provides a compile-time DSL for defining CLI parameters, inspired by zig-clap.
//! Parameters are parsed from a string at compile time, generating type-safe
//! result structs with zero runtime overhead for the parsing logic.
//!
//! ## DSL Format
//!
//! ```
//! -s                      Short flag (boolean)
//! --long                  Long flag (boolean)
//! -s, --long              Both short and long (boolean)
//! -s, --long <type>       Option that takes a value
//! -s, --long <type>...    Option that can be specified multiple times
//! <name>                  Positional argument
//! <name>...               Variadic positional (must be last)
//! ```
//!
//! ## Supported Types
//!
//! - `str`: []const u8
//! - `u8`, `u16`, `u32`, `u64`, `usize`: unsigned integers
//! - `i8`, `i16`, `i32`, `i64`, `isize`: signed integers
//! - `f32`, `f64`: floating point
//! - `bool`: boolean (for explicit --flag=true/false)
//!
//! ## Example
//!
//! ```zig
//! const params = comptime parseParams(
//!     \\-h, --help             Display this help and exit.
//!     \\-p, --port <u16>       Port to listen on.
//!     \\-v, --verbose          Enable verbose output.
//!     \\    --config <str>     Configuration file path.
//!     \\<command>              Subcommand to run.
//! );
//!
//! // Result type is generated at comptime:
//! // struct {
//! //     args: struct {
//! //         help: u8,           // count of --help flags
//! //         port: ?u16,         // optional value
//! //         verbose: u8,        // count
//! //         config: ?[]const u8,
//! //     },
//! //     positionals: struct { ?[]const u8 },
//! // }
//! ```

const std = @import("std");
const meta = std.meta;

/// Parameter taking behavior
pub const Takes = enum {
    /// Flag that doesn't take a value (boolean)
    none,
    /// Takes exactly one value
    one,
    /// Can take multiple values (specified multiple times)
    many,
};

/// Parameter names
pub const Names = struct {
    short: ?u8 = null,
    long: ?[]const u8 = null,

    pub fn longest(self: Names) struct { kind: enum { short, long, positional }, name: []const u8 } {
        if (self.long) |l| return .{ .kind = .long, .name = l };
        if (self.short) |s| return .{ .kind = .short, .name = &[_]u8{s} };
        return .{ .kind = .positional, .name = "" };
    }
};

/// Help information for a parameter
pub const Help = struct {
    desc: []const u8 = "",
    val: []const u8 = "",

    pub fn description(self: Help) []const u8 {
        return self.desc;
    }

    pub fn value(self: Help) []const u8 {
        return self.val;
    }
};

/// A single parameter definition
pub fn Param(comptime Id: type) type {
    return struct {
        id: Id,
        names: Names = .{},
        takes: Takes = .none,
    };
}

/// Count the number of parameters in a DSL string
fn countParams(str: []const u8) usize {
    @setEvalBranchQuota(std.math.maxInt(u32));
    var count: usize = 0;
    var it = std.mem.splitScalar(u8, str, '\n');
    while (it.next()) |line| {
        const trimmed = std.mem.trimLeft(u8, line, " \t");
        if (std.mem.startsWith(u8, trimmed, "-") or std.mem.startsWith(u8, trimmed, "<")) {
            count += 1;
        }
    }
    return count;
}

/// Parse a single parameter from a line
fn parseParamLine(line: []const u8) !Param(Help) {
    var param = Param(Help){ .id = .{} };
    var i: usize = 0;
    const trimmed = std.mem.trimLeft(u8, line, " \t");

    if (trimmed.len == 0) return error.EmptyLine;

    // Check for positional: <name> or <name>...
    if (trimmed[0] == '<') {
        const end = std.mem.indexOfScalar(u8, trimmed, '>') orelse return error.InvalidPositional;
        param.id.val = trimmed[1..end];
        param.takes = .one;
        // Check for variadic
        if (trimmed.len > end + 3 and std.mem.eql(u8, trimmed[end + 1 .. end + 4], "...")) {
            param.takes = .many;
            i = end + 4;
        } else {
            i = end + 1;
        }
        // Parse description after whitespace
        const rest = std.mem.trimLeft(u8, trimmed[i..], " \t");
        param.id.desc = rest;
        return param;
    }

    // Parse short flag: -x
    if (trimmed[i] == '-' and i + 1 < trimmed.len and trimmed[i + 1] != '-') {
        param.names.short = trimmed[i + 1];
        i += 2;
    }

    // Skip comma and whitespace
    while (i < trimmed.len and (trimmed[i] == ',' or trimmed[i] == ' ' or trimmed[i] == '\t')) {
        i += 1;
    }

    // Parse long flag: --name
    if (i + 1 < trimmed.len and trimmed[i] == '-' and trimmed[i + 1] == '-') {
        i += 2;
        const start = i;
        while (i < trimmed.len and trimmed[i] != ' ' and trimmed[i] != '\t' and trimmed[i] != '<' and trimmed[i] != '=') {
            i += 1;
        }
        param.names.long = trimmed[start..i];
    }

    // Skip whitespace
    while (i < trimmed.len and (trimmed[i] == ' ' or trimmed[i] == '\t')) {
        i += 1;
    }

    // Parse value type: <type> or <type>...
    if (i < trimmed.len and trimmed[i] == '<') {
        i += 1;
        const start = i;
        while (i < trimmed.len and trimmed[i] != '>') {
            i += 1;
        }
        param.id.val = trimmed[start..i];
        param.takes = .one;
        if (i < trimmed.len) i += 1; // skip '>'

        // Check for variadic
        if (i + 2 < trimmed.len and std.mem.eql(u8, trimmed[i .. i + 3], "...")) {
            param.takes = .many;
            i += 3;
        }
    }

    // Skip whitespace before description
    while (i < trimmed.len and (trimmed[i] == ' ' or trimmed[i] == '\t')) {
        i += 1;
    }

    // Rest is description
    if (i < trimmed.len) {
        param.id.desc = trimmed[i..];
    }

    return param;
}

/// Parse parameters from a DSL string at compile time
pub fn parseParams(comptime str: []const u8) [countParams(str)]Param(Help) {
    @setEvalBranchQuota(std.math.maxInt(u32));
    var result: [countParams(str)]Param(Help) = undefined;
    var idx: usize = 0;

    var it = std.mem.splitScalar(u8, str, '\n');
    while (it.next()) |line| {
        const trimmed = std.mem.trimLeft(u8, line, " \t");
        if (trimmed.len == 0) continue;
        if (!std.mem.startsWith(u8, trimmed, "-") and !std.mem.startsWith(u8, trimmed, "<")) continue;

        result[idx] = parseParamLine(line) catch |err| {
            @compileError(std.fmt.comptimePrint("Failed to parse parameter: {s}", .{@errorName(err)}));
        };
        idx += 1;
    }

    return result;
}

/// Value parsers for converting strings to types
pub const parsers = struct {
    pub const string = struct {
        pub fn parse(s: []const u8) ![]const u8 {
            return s;
        }
    }.parse;

    pub fn int(comptime T: type, comptime radix: u8) fn ([]const u8) std.fmt.ParseIntError!T {
        return struct {
            fn parse(s: []const u8) std.fmt.ParseIntError!T {
                return std.fmt.parseInt(T, s, radix);
            }
        }.parse;
    }

    pub fn float(comptime T: type) fn ([]const u8) std.fmt.ParseFloatError!T {
        return struct {
            fn parse(s: []const u8) std.fmt.ParseFloatError!T {
                return std.fmt.parseFloat(T, s);
            }
        }.parse;
    }

    pub fn enumeration(comptime E: type) fn ([]const u8) error{InvalidEnumTag}!E {
        return struct {
            fn parse(s: []const u8) error{InvalidEnumTag}!E {
                return std.meta.stringToEnum(E, s) orelse error.InvalidEnumTag;
            }
        }.parse;
    }

    /// Default parsers for common type names
    pub const default = .{
        .str = string,
        .string = string,
        .u8 = int(u8, 10),
        .u16 = int(u16, 10),
        .u32 = int(u32, 10),
        .u64 = int(u64, 10),
        .usize = int(usize, 10),
        .i8 = int(i8, 10),
        .i16 = int(i16, 10),
        .i32 = int(i32, 10),
        .i64 = int(i64, 10),
        .isize = int(isize, 10),
        .f32 = float(f32),
        .f64 = float(f64),
    };
};

/// Get the parser result type for a given type name
fn ParserResultType(comptime type_name: []const u8, comptime value_parsers: anytype) type {
    if (type_name.len == 0) return void;

    // Check if we have a parser for this type
    inline for (std.meta.fieldNames(@TypeOf(value_parsers))) |field_name| {
        if (std.mem.eql(u8, type_name, field_name)) {
            const parser = @field(value_parsers, field_name);
            const ReturnType = @typeInfo(@TypeOf(parser)).@"fn".return_type.?;
            // Unwrap error union
            return switch (@typeInfo(ReturnType)) {
                .error_union => |eu| eu.payload,
                else => ReturnType,
            };
        }
    }

    @compileError("No parser found for type: " ++ type_name);
}

/// Generate the Arguments struct type based on parameters
fn Arguments(
    comptime params: []const Param(Help),
    comptime value_parsers: anytype,
) type {
    var fields: [params.len]std.builtin.Type.StructField = undefined;
    var field_count: usize = 0;

    for (params) |param| {
        // Skip positionals
        if (param.names.short == null and param.names.long == null) continue;

        // Create null-terminated name by copying into a comptime buffer
        const name: [:0]const u8 = blk: {
            if (param.names.long) |l| {
                var buf: [l.len + 1]u8 = undefined;
                @memcpy(buf[0..l.len], l);
                buf[l.len] = 0;
                break :blk buf[0..l.len :0];
            } else {
                var buf: [2]u8 = .{ param.names.short.?, 0 };
                break :blk buf[0..1 :0];
            }
        };

        const FieldType = switch (param.takes) {
            .none => u8, // Count of times flag appeared
            .one => ?ParserResultType(param.id.val, value_parsers),
            .many => []const ParserResultType(param.id.val, value_parsers),
        };

        const default_value: ?*const anyopaque = switch (param.takes) {
            .none => @ptrCast(&@as(u8, 0)),
            .one => @ptrCast(&@as(?ParserResultType(param.id.val, value_parsers), null)),
            .many => @ptrCast(&@as([]const ParserResultType(param.id.val, value_parsers), &.{})),
        };

        fields[field_count] = .{
            .name = name,
            .type = FieldType,
            .default_value_ptr = default_value,
            .is_comptime = false,
            .alignment = @alignOf(FieldType),
        };
        field_count += 1;
    }

    return @Type(.{ .@"struct" = .{
        .layout = .auto,
        .fields = fields[0..field_count],
        .decls = &.{},
        .is_tuple = false,
    } });
}

/// Generate the Positionals tuple type based on parameters
fn Positionals(
    comptime params: []const Param(Help),
    comptime value_parsers: anytype,
) type {
    var fields: [params.len]std.builtin.Type.StructField = undefined;
    var field_count: usize = 0;

    for (params) |param| {
        // Only positionals
        if (param.names.short != null or param.names.long != null) continue;

        const BaseType = ParserResultType(param.id.val, value_parsers);

        const FieldType = switch (param.takes) {
            .none => void,
            .one => ?BaseType,
            .many => []const BaseType,
        };

        fields[field_count] = .{
            .name = std.fmt.comptimePrint("{d}", .{field_count}),
            .type = FieldType,
            .default_value_ptr = null, // Tuples can't have default values
            .is_comptime = false,
            .alignment = @alignOf(FieldType),
        };
        field_count += 1;
    }

    if (field_count == 0) {
        return struct {};
    }

    return @Type(.{ .@"struct" = .{
        .layout = .auto,
        .fields = fields[0..field_count],
        .decls = &.{},
        .is_tuple = true,
    } });
}

/// Initialize positionals with default values
fn initPositionals(
    comptime params: []const Param(Help),
    comptime value_parsers: anytype,
) Positionals(params, value_parsers) {
    const PositionalsType = Positionals(params, value_parsers);
    const fields = @typeInfo(PositionalsType).@"struct".fields;

    var result: PositionalsType = undefined;
    inline for (fields) |field| {
        const T = field.type;
        @field(result, field.name) = switch (@typeInfo(T)) {
            .optional => null,
            .pointer => |ptr| if (ptr.size == .slice) &.{} else undefined,
            else => undefined,
        };
    }
    return result;
}

/// Parse result type
pub fn Result(
    comptime params: []const Param(Help),
    comptime value_parsers: anytype,
) type {
    return struct {
        args: Arguments(params, value_parsers),
        positionals: Positionals(params, value_parsers),
        allocator: std.mem.Allocator,
        arena: ?std.heap.ArenaAllocator,

        const Self = @This();

        pub fn deinit(self: *Self) void {
            if (self.arena) |*arena| {
                arena.deinit();
            }
        }
    };
}

/// Parse options
pub const ParseOptions = struct {
    allocator: std.mem.Allocator,
    /// Assignment separators (e.g., "=" for --flag=value)
    assignment_separators: []const u8 = "=",
};

/// Parse command line arguments into a typed result
pub fn parse(
    comptime params: []const Param(Help),
    comptime value_parsers: anytype,
    args_iter: anytype,
    opts: ParseOptions,
) !Result(params, value_parsers) {
    var arena = std.heap.ArenaAllocator.init(opts.allocator);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    var result = Result(params, value_parsers){
        .args = .{},
        .positionals = initPositionals(params, value_parsers),
        .allocator = opts.allocator,
        .arena = arena,
    };

    while (args_iter.next()) |arg| {
        if (arg.len == 0) continue;

        // Long option
        if (arg.len > 2 and arg[0] == '-' and arg[1] == '-') {
            const rest = arg[2..];
            var name: []const u8 = rest;
            var value: ?[]const u8 = null;

            // Check for assignment separator
            for (opts.assignment_separators) |sep| {
                if (std.mem.indexOfScalar(u8, rest, sep)) |idx| {
                    name = rest[0..idx];
                    value = rest[idx + 1 ..];
                    break;
                }
            }

            // Find matching parameter
            inline for (params) |param| {
                if (param.names.long) |long| {
                    if (std.mem.eql(u8, name, long)) {
                        switch (param.takes) {
                            .none => {
                                @field(result.args, long) += 1;
                            },
                            .one => {
                                const val = value orelse args_iter.next() orelse return error.MissingValue;
                                const parser = @field(value_parsers, param.id.val);
                                @field(result.args, long) = try parser(val);
                            },
                            .many => {
                                const val = value orelse args_iter.next() orelse return error.MissingValue;
                                const parser = @field(value_parsers, param.id.val);
                                const parsed = try parser(val);
                                const current = @field(result.args, long);
                                const new = try alloc.alloc(@TypeOf(parsed), current.len + 1);
                                @memcpy(new[0..current.len], current);
                                new[current.len] = parsed;
                                @field(result.args, long) = new;
                            },
                        }
                        break;
                    }
                }
            }
            continue;
        }

        // Short option(s)
        if (arg.len > 1 and arg[0] == '-' and arg[1] != '-') {
            var i: usize = 1;
            while (i < arg.len) {
                const c = arg[i];
                var matched = false;

                inline for (params) |param| {
                    if (param.names.short) |short| {
                        if (c == short) {
                            matched = true;
                            const field_name = param.names.long orelse &[_]u8{short};

                            switch (param.takes) {
                                .none => {
                                    @field(result.args, field_name) += 1;
                                    i += 1;
                                },
                                .one, .many => {
                                    // Value can be attached or next arg
                                    const val = if (i + 1 < arg.len)
                                        arg[i + 1 ..]
                                    else
                                        args_iter.next() orelse return error.MissingValue;

                                    const parser = @field(value_parsers, param.id.val);
                                    const parsed = try parser(val);

                                    if (param.takes == .one) {
                                        @field(result.args, field_name) = parsed;
                                    } else {
                                        const current = @field(result.args, field_name);
                                        const new = try alloc.alloc(@TypeOf(parsed), current.len + 1);
                                        @memcpy(new[0..current.len], current);
                                        new[current.len] = parsed;
                                        @field(result.args, field_name) = new;
                                    }
                                    i = arg.len; // consumed rest of arg
                                },
                            }
                            break;
                        }
                    }
                }

                if (!matched) {
                    return error.UnknownFlag;
                }
            }
            continue;
        }

        // Positional argument - handled separately after all flags
        // We need comptime-known indices, so collect positionals first
        // For now, handle positionals in a second pass
    }

    // Re-iterate for positionals only
    args_iter.index = 0;
    var positional_idx: usize = 0;

    while (args_iter.next()) |arg| {
        if (arg.len == 0) continue;
        if (arg[0] == '-') continue; // Skip flags

        // Find which positional param this corresponds to
        comptime var pos_idx: usize = 0;
        inline for (params) |param| {
            if (param.names.short == null and param.names.long == null) {
                if (positional_idx == pos_idx or param.takes == .many) {
                    const parser = @field(value_parsers, param.id.val);
                    const parsed = try parser(arg);
                    const field_name = comptime std.fmt.comptimePrint("{d}", .{pos_idx});

                    switch (param.takes) {
                        .one => {
                            if (positional_idx == pos_idx) {
                                @field(result.positionals, field_name) = parsed;
                                positional_idx += 1;
                            }
                        },
                        .many => {
                            // Variadic - always add to this one
                            if (positional_idx >= pos_idx) {
                                const current = @field(result.positionals, field_name);
                                const new = try alloc.alloc(@TypeOf(parsed), current.len + 1);
                                @memcpy(new[0..current.len], current);
                                new[current.len] = parsed;
                                @field(result.positionals, field_name) = new;
                                positional_idx += 1;
                            }
                        },
                        .none => {},
                    }
                    break;
                }
                pos_idx += 1;
            }
        }
    }

    return result;
}

/// A simple argument iterator for slices
pub const SliceIterator = struct {
    args: []const []const u8,
    index: usize = 0,

    pub fn next(self: *SliceIterator) ?[]const u8 {
        if (self.index >= self.args.len) return null;
        defer self.index += 1;
        return self.args[self.index];
    }
};

/// Print help message for parameters
pub fn printHelp(
    comptime params: []const Param(Help),
    writer: anytype,
) !void {
    try writer.writeAll("Options:\n");

    inline for (params) |param| {
        // Skip positionals for now
        if (param.names.short == null and param.names.long == null) continue;

        try writer.writeAll("    ");

        if (param.names.short) |s| {
            try writer.print("-{c}", .{s});
            if (param.names.long != null) {
                try writer.writeAll(", ");
            }
        } else {
            try writer.writeAll("    ");
        }

        if (param.names.long) |l| {
            try writer.print("--{s}", .{l});
        }

        if (param.takes != .none and param.id.val.len > 0) {
            try writer.print(" <{s}>", .{param.id.val});
            if (param.takes == .many) {
                try writer.writeAll("...");
            }
        }

        if (param.id.desc.len > 0) {
            // Pad to description column
            try writer.writeAll("\n            ");
            try writer.writeAll(param.id.desc);
        }

        try writer.writeAll("\n");
    }

    // Print positionals
    var has_positionals = false;
    inline for (params) |param| {
        if (param.names.short == null and param.names.long == null) {
            if (!has_positionals) {
                try writer.writeAll("\nArguments:\n");
                has_positionals = true;
            }
            try writer.print("    <{s}>", .{param.id.val});
            if (param.takes == .many) {
                try writer.writeAll("...");
            }
            if (param.id.desc.len > 0) {
                try writer.writeAll("\n            ");
                try writer.writeAll(param.id.desc);
            }
            try writer.writeAll("\n");
        }
    }
}

/// Print usage string
pub fn printUsage(
    comptime params: []const Param(Help),
    comptime program_name: []const u8,
    writer: anytype,
) !void {
    try writer.print("Usage: {s}", .{program_name});

    // Short flags without values
    var has_short_flags = false;
    inline for (params) |param| {
        if (param.names.short != null and param.takes == .none) {
            if (!has_short_flags) {
                try writer.writeAll(" [-");
                has_short_flags = true;
            }
            try writer.print("{c}", .{param.names.short.?});
        }
    }
    if (has_short_flags) {
        try writer.writeAll("]");
    }

    // Options with values
    inline for (params) |param| {
        if (param.takes != .none) {
            if (param.names.short != null or param.names.long != null) {
                try writer.writeAll(" [");
                if (param.names.short) |s| {
                    try writer.print("-{c}", .{s});
                } else if (param.names.long) |l| {
                    try writer.print("--{s}", .{l});
                }
                try writer.print(" <{s}>", .{param.id.val});
                if (param.takes == .many) try writer.writeAll("...");
                try writer.writeAll("]");
            }
        }
    }

    // Positionals
    inline for (params) |param| {
        if (param.names.short == null and param.names.long == null) {
            try writer.print(" <{s}>", .{param.id.val});
            if (param.takes == .many) try writer.writeAll("...");
        }
    }

    try writer.writeAll("\n");
}

// ==================== Tests ====================

test "parseParams basic" {
    const params = comptime parseParams(
        \\-h, --help             Display this help and exit.
        \\-v, --verbose          Enable verbose output.
    );

    try std.testing.expectEqual(@as(usize, 2), params.len);
    try std.testing.expectEqual(@as(?u8, 'h'), params[0].names.short);
    try std.testing.expectEqualStrings("help", params[0].names.long.?);
    try std.testing.expectEqual(Takes.none, params[0].takes);
}

test "parseParams with values" {
    const params = comptime parseParams(
        \\-p, --port <u16>       Port to listen on.
        \\    --config <str>     Configuration file.
    );

    try std.testing.expectEqual(@as(usize, 2), params.len);
    try std.testing.expectEqualStrings("port", params[0].names.long.?);
    try std.testing.expectEqual(Takes.one, params[0].takes);
    try std.testing.expectEqualStrings("u16", params[0].id.val);
}

test "parseParams positionals" {
    const params = comptime parseParams(
        \\<command>              The command to run.
        \\<files>...             Input files.
    );

    try std.testing.expectEqual(@as(usize, 2), params.len);
    try std.testing.expectEqualStrings("command", params[0].id.val);
    try std.testing.expectEqual(Takes.one, params[0].takes);
    try std.testing.expectEqualStrings("files", params[1].id.val);
    try std.testing.expectEqual(Takes.many, params[1].takes);
}

test "parse simple flags" {
    const params = comptime parseParams(
        \\-h, --help             Display help.
        \\-v, --verbose          Verbose mode.
    );

    var iter = SliceIterator{ .args = &.{ "-h", "-v", "-v" } };
    var result = try parse(&params, parsers.default, &iter, .{ .allocator = std.testing.allocator });
    defer result.deinit();

    try std.testing.expectEqual(@as(u8, 1), result.args.help);
    try std.testing.expectEqual(@as(u8, 2), result.args.verbose);
}

test "parse with values" {
    const params = comptime parseParams(
        \\-p, --port <u16>       Port number.
        \\    --host <str>       Host name.
    );

    var iter = SliceIterator{ .args = &.{ "--port", "8080", "--host", "localhost" } };
    var result = try parse(&params, parsers.default, &iter, .{ .allocator = std.testing.allocator });
    defer result.deinit();

    try std.testing.expectEqual(@as(u16, 8080), result.args.port.?);
    try std.testing.expectEqualStrings("localhost", result.args.host.?);
}

test "parse assignment syntax" {
    const params = comptime parseParams(
        \\-p, --port <u16>       Port number.
    );

    var iter = SliceIterator{ .args = &.{"--port=9000"} };
    var result = try parse(&params, parsers.default, &iter, .{ .allocator = std.testing.allocator });
    defer result.deinit();

    try std.testing.expectEqual(@as(u16, 9000), result.args.port.?);
}

test "parse positionals" {
    const params = comptime parseParams(
        \\<str>                  Command to run.
    );

    var iter = SliceIterator{ .args = &.{"serve"} };
    var result = try parse(&params, parsers.default, &iter, .{ .allocator = std.testing.allocator });
    defer result.deinit();

    try std.testing.expectEqualStrings("serve", result.positionals[0].?);
}

test "help output" {
    const params = comptime parseParams(
        \\-h, --help             Display this help.
        \\-p, --port <u16>       Port to listen on.
        \\<str>                  The subcommand.
    );

    var buf: [1024]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    try printHelp(&params, stream.writer());

    const output = stream.getWritten();
    try std.testing.expect(std.mem.indexOf(u8, output, "--help") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "--port") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "<str>") != null);
}

test "type safety demonstration" {
    // The DSL generates EXACT types at compile time
    const params = comptime parseParams(
        \\-p, --port <u16>    Port number.
        \\-c, --count <u32>   Item count.
        \\-v, --verbose       Verbose mode (bool flag).
        \\    --host <str>    Hostname string.
    );

    const ResultType = Result(&params, parsers.default);
    const ArgsInfo = @typeInfo(@TypeOf(@as(ResultType, undefined).args)).@"struct";

    // Verify exact types are generated (not generic Value):
    inline for (ArgsInfo.fields) |field| {
        if (std.mem.eql(u8, field.name, "port")) {
            // port is ?u16, not ?u64 or Value!
            try std.testing.expect(field.type == ?u16);
        }
        if (std.mem.eql(u8, field.name, "count")) {
            try std.testing.expect(field.type == ?u32);
        }
        if (std.mem.eql(u8, field.name, "verbose")) {
            // Bool flags are u8 (count of appearances) - 0 = false, >0 = true
            // This allows -vvv for verbosity levels
            try std.testing.expect(field.type == u8);
        }
        if (std.mem.eql(u8, field.name, "host")) {
            try std.testing.expect(field.type == ?[]const u8);
        }
    }
}
