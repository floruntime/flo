//! Commands generated from the op table. Each op the table exposes as a
//! `command` becomes one: its verb is the last part of its name, an op in a
//! group (`kv.txn.begin`) sits under that group's subcommand, the envelope
//! key and the fields `cli.args` names are positional, and every other
//! request field is a flag named after it (`ttl_ms` is `--ttl-ms`). The
//! command sends one request and prints the answer by its response schema.
//!
//! A field's flag reads by its schema: an integer is range-checked, and
//! one whose name ends in `_ms` also takes a number and a unit (30s, 5m);
//! an enum takes one of its names; bytes and text are length-checked. An
//! optional field left out is absent, a defaulted one takes its default,
//! and a plain one is required, except bytes, which are empty.

const std = @import("std");
const commander = @import("commander/mod.zig");
const outcome = @import("outcome.zig");
const output = @import("output.zig");
const cli_config = @import("config.zig");
const client_mod = @import("client/mod.zig");
const ops = @import("../protocol/ops.zig");
const schema = @import("../protocol/schema.zig");
const time_units = @import("../util/time_units.zig");

const Context = commander.Context;
const Error = commander.Error;
const Builder = commander.Builder;
const Allocator = std.mem.Allocator;

/// Adds a subcommand to `family` for every op in `table` exposed as a
/// command, and one per group holding its grouped ops.
pub fn addCommands(allocator: Allocator, family: *Builder, comptime table: []const ops.Op, comptime groups: []const ops.Group) void {
    inline for (table) |op| {
        if (op.cli.expose == .command and ops.groupOf(op.name) == null) _ = family.subcommand(command(allocator, op));
    }
    inline for (groups) |g| {
        const group = commander.newBuilder(allocator).name(comptime lastPart(g.name)).about(g.doc);
        inline for (table) |op| {
            if (op.cli.expose == .command) if (comptime ops.groupOf(op.name)) |name| {
                if (comptime std.mem.eql(u8, name, g.name)) _ = group.subcommand(command(allocator, op));
            };
        }
        _ = family.subcommand(group);
    }
}

fn lastPart(comptime name: []const u8) []const u8 {
    return name[(std.mem.lastIndexOfScalar(u8, name, '.') orelse return name) + 1 ..];
}

/// The command for `op`.
pub fn command(allocator: Allocator, comptime op: ops.Op) *Builder {
    const b = commander.newBuilder(allocator).name(comptime ops.verbOf(op.name)).about(op.doc);
    if (op.cli.examples.len > 0) _ = b.examples(op.cli.examples);
    if (op.cli.key) |k| _ = if (k.optional) b.optionalArg(k.name, k.doc) else b.arg(k.name, k.doc);
    inline for (op.cli.args) |name| {
        const f = comptime field(op.request, name);
        _ = if (f.T.kind == .list) b.variadicArg(comptime ops.dashed(name), f.doc) else b.arg(comptime ops.dashed(name), f.doc);
    }
    inline for (op.request.field_list) |f| {
        if (comptime isArg(op, f.name)) continue;
        const letter = comptime shortOf(op, f.name);
        const T = comptime inner(f.T);
        if (T.kind == .boolean) {
            _ = b.boolFlag(comptime ops.dashed(f.name), letter, f.doc);
        } else {
            _ = b.stringFlag(comptime ops.dashed(f.name), letter, "", comptime help(f));
        }
    }
    return b.action(Run(op).run);
}

/// A flag's help line: the field's doc, and its default or that it's
/// required.
fn help(comptime f: schema.Field) []const u8 {
    if (f.T.kind == .defaulted) return f.doc ++ " (default " ++ defaultText(f.T.Inner, f.T.default_value) ++ ")";
    if (f.T.kind != .optional and inner(f.T).kind != .bytes and inner(f.T).kind != .string) return f.doc ++ " (required)";
    return f.doc;
}

fn defaultText(comptime T: type, comptime v: T.Value) []const u8 {
    return switch (T.kind) {
        .enumeration => @tagName(v),
        .string, .bytes => v,
        else => std.fmt.comptimePrint("{d}", .{v}),
    };
}

fn field(comptime Schema: type, comptime name: []const u8) schema.Field {
    for (Schema.field_list) |f| if (std.mem.eql(u8, f.name, name)) return f;
    @compileError("no field '" ++ name ++ "'");
}

fn isHex(comptime op: ops.Op, comptime name: []const u8) bool {
    for (op.cli.hex) |h| if (std.mem.eql(u8, h, name)) return true;
    return false;
}

/// A hex flag's bytes.
fn unhex(ctx: *Context, arena: Allocator, comptime flag: []const u8, text: []const u8) Error![]const u8 {
    if (text.len % 2 != 0) return outcome.usage(ctx, "--" ++ flag ++ " {s} is not hex", .{text});
    const out = try arena.alloc(u8, text.len / 2);
    return std.fmt.hexToBytes(out, text) catch outcome.usage(ctx, "--" ++ flag ++ " {s} is not hex", .{text});
}

fn isArg(comptime op: ops.Op, comptime name: []const u8) bool {
    for (op.cli.args) |a| if (std.mem.eql(u8, a, name)) return true;
    return false;
}

fn shortOf(comptime op: ops.Op, comptime name: []const u8) u8 {
    for (op.cli.short) |s| if (std.mem.eql(u8, s.field, name)) return s.letter;
    return 0;
}

/// The type an optional or defaulted field wraps; the field's own type
/// otherwise.
fn inner(comptime T: type) type {
    return if (T.kind == .optional or T.kind == .defaulted) T.Inner else T;
}

/// What a command does: reads its arguments into `op`'s request, sends it,
/// and prints the answer.
fn Run(comptime op: ops.Op) type {
    return struct {
        fn run(ctx_ptr: *anyopaque) Error!void {
            const ctx: *Context = @ptrCast(@alignCast(ctx_ptr));
            var arena_state = std.heap.ArenaAllocator.init(ctx.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();

            const key = if (op.cli.key) |k| ctx.getPositional(k.name) orelse "" else "";
            var body: op.request.Value = undefined;
            inline for (op.request.field_list) |f| @field(body, f.name) = try read(ctx, arena, op, f);

            const endpoint = cli_config.getEndpoint(ctx);
            var client = client_mod.Client.init(ctx.allocator, endpoint);
            defer client.deinit();
            client.connect() catch |err| return outcome.connectFailed(ctx, err, endpoint);

            var resp = client.call(op, cli_config.getNamespace(ctx), key, body) catch |err| return outcome.requestFailed(ctx, err);
            defer resp.deinit();
            try outcome.check(ctx, resp);
            var diag: schema.Diagnostic = .{};
            const answer = op.response.decodeAll(resp.data, &diag) catch return outcome.malformed(ctx, op.name ++ " answer");
            try print(ctx, op, answer);
            if (op.cli.absent) |name| if (!@field(answer, name)) return error.NotFound;
        }
    };
}

/// The value of request field `f`, from its positional argument or flag.
fn read(ctx: *Context, arena: Allocator, comptime op: ops.Op, comptime f: schema.Field) Error!f.T.Value {
    const flag = comptime ops.dashed(f.name);
    if (comptime isArg(op, f.name)) {
        if (f.T.kind == .list) return readList(ctx, arena, f, ctx.getVariadicArgs(flag) orelse &.{});
        return parse(ctx, f.T, flag, ctx.getPositional(flag) orelse "");
    }
    const T = comptime inner(f.T);
    if (T.kind == .boolean) {
        const v = ctx.getBool(flag);
        return switch (f.T.kind) {
            .optional => if (ctx.flagChanged(flag)) v else null,
            else => v,
        };
    }
    const given = ctx.flagChanged(flag);
    const raw_text = ctx.getString(flag) orelse "";
    const text = if (comptime isHex(op, f.name)) try unhex(ctx, arena, flag, raw_text) else raw_text;
    return switch (f.T.kind) {
        .optional => if (given) try parse(ctx, T, "--" ++ flag, text) else null,
        .defaulted => if (given) try parse(ctx, T, "--" ++ flag, text) else f.T.default_value,
        .bytes, .string, .parsed => try parse(ctx, T, "--" ++ flag, text),
        else => if (given) try parse(ctx, T, "--" ++ flag, text) else outcome.usage(ctx, "--" ++ flag ++ " is required", .{}),
    };
}

/// `text` as a value of `T`, refused naming `what`: a flag as `--name`, a
/// positional argument by its name.
fn parse(ctx: *Context, comptime T: type, comptime what: []const u8, text: []const u8) Error!T.Value {
    switch (T.kind) {
        .integer => {
            const v = parseInteger(T, what, text) orelse return outcome.usage(ctx, what ++ " {s} is not " ++ comptime integerWhat(T, what), .{text});
            if (v < T.lo or v > T.hi) return outcome.usage(ctx, what ++ " {d} is out of range ({d} to {d})", .{ v, T.lo, T.hi });
            return v;
        },
        .enumeration => return std.meta.stringToEnum(T.Value, text) orelse
            outcome.usage(ctx, what ++ " {s} is not one of: " ++ comptime enumNames(T.Value), .{text}),
        .bytes, .string, .parsed => {
            if (text.len > T.bound) return outcome.usage(ctx, what ++ " is {d} bytes; at most {d}", .{ text.len, T.bound });
            return text;
        },
        else => @compileError("a flag can't read a " ++ @tagName(T.kind)),
    }
}

/// A `_ms` field also takes a number and a unit.
fn parseInteger(comptime T: type, comptime flag: []const u8, text: []const u8) ?T.Value {
    if (std.fmt.parseInt(T.Value, text, 10)) |v| return v else |_| {}
    if (comptime std.mem.endsWith(u8, flag, "-ms")) {
        const ms = time_units.parseDurationMs(text) orelse return null;
        return std.math.cast(T.Value, ms);
    }
    return null;
}

fn integerWhat(comptime T: type, comptime flag: []const u8) []const u8 {
    if (std.mem.endsWith(u8, flag, "-ms")) return "a duration; give milliseconds, or a number and a unit (500ms, 30s, 5m, 1h, 1d)";
    return if (@typeInfo(T.Value).int.signedness == .signed) "a whole number" else "a whole number from 0";
}

fn enumNames(comptime E: type) []const u8 {
    var out: []const u8 = "";
    for (std.meta.fieldNames(E), 0..) |n, i| out = out ++ (if (i == 0) "" else ", ") ++ n;
    return out;
}

/// A list argument: each element read as the list's element type.
fn readList(ctx: *Context, arena: Allocator, comptime f: schema.Field, items: []const []const u8) Error!f.T.Value {
    const buf = try arena.alloc(u8, f.T.max_size);
    var list = f.T.Builder.init(buf);
    for (items) |item| {
        const name = comptime ops.dashed(f.name);
        const v = try parse(ctx, f.T.Element, name, item);
        if (!(list.add(v) catch return outcome.usage(ctx, name ++ ": an element is too long", .{}))) return outcome.usage(ctx, "too many " ++ name ++ "; at most {d}", .{f.T.bound});
    }
    return list.value();
}

// ── Printing ────────────────────────────────────────────────────────────

/// Prints an answer: as JSON with `-o json`, else as the op renders it.
fn print(ctx: *Context, comptime op: ops.Op, answer: op.response.Value) Error!void {
    const format = output.getFormat(ctx);
    if (format == .json) {
        printJson(ctx, op, op.response, answer);
        ctx.print("\n", .{});
        return;
    }
    if (op.cli.show) |name| if (format == .raw or op.cli.render == .raw) {
        const f = comptime field(op.response, name);
        // Absent (an incr in a transaction has no sum yet): it went.
        if (f.T.kind == .optional and @field(answer, name) == null) {
            ctx.print("OK\n", .{});
        } else {
            printScalar(ctx, f.T, @field(answer, name));
            ctx.print("\n", .{});
        }
        return;
    };
    // With -o raw a table's rows are the whole of stdout, so a script can
    // read them line by line; its other fields (scan's cursor) go to stderr.
    const side: Stream = if (op.cli.render == .table and format == .raw) .err else .out;
    var printed = false;
    inline for (op.response.field_list) |f| {
        const v = @field(answer, f.name);
        if (f.T.kind == .list) {
            printed = true;
            if (op.cli.render == .table) try printTable(ctx, f.T, v, format != .raw) else try printListRecord(ctx, f, v);
        } else if (f.T.kind == .optional) {
            if (v) |x| {
                emit(ctx, side, f.name ++ ": ", .{});
                if (comptime isHex(op, f.name)) emit(ctx, side, "{x}", .{x}) else printScalarTo(ctx, side, f.T.Inner, x);
                emit(ctx, side, "\n", .{});
                printed = true;
            }
        } else {
            emit(ctx, side, f.name ++ ": ", .{});
            if (comptime isHex(op, f.name)) emit(ctx, side, "{x}", .{v}) else printScalarTo(ctx, side, f.T, v);
            emit(ctx, side, "\n", .{});
            printed = true;
        }
    }
    // An answer with nothing to say (an expire, a rollback) says it went.
    if (!printed) ctx.print("OK\n", .{});
}

const Stream = enum { out, err };

fn emit(ctx: *Context, stream: Stream, comptime fmt: []const u8, args: anytype) void {
    switch (stream) {
        .out => ctx.print(fmt, args),
        .err => ctx.printErr(fmt, args),
    }
}

fn printScalar(ctx: *Context, comptime T: type, v: T.Value) void {
    printScalarTo(ctx, .out, T, v);
}

fn printScalarTo(ctx: *Context, stream: Stream, comptime T: type, v: T.Value) void {
    switch (T.kind) {
        .integer => emit(ctx, stream, "{d}", .{v}),
        .boolean => emit(ctx, stream, "{}", .{v}),
        .enumeration => emit(ctx, stream, "{s}", .{@tagName(v)}),
        .bytes, .string, .parsed => emit(ctx, stream, "{s}", .{v}),
        .optional => if (v) |x| printScalarTo(ctx, stream, T.Inner, x) else emit(ctx, stream, "-", .{}),
        .defaulted => printScalarTo(ctx, stream, T.Inner, v),
        else => @compileError("can't print a " ++ @tagName(T.kind)),
    }
}

/// A list as rows: a record element's fields as tab-separated columns
/// under a header (left out for `-o raw`), any other element one per line.
fn printTable(ctx: *Context, comptime L: type, v: L.Value, header: bool) Error!void {
    const E = L.Element;
    if (E.kind == .record and header) {
        inline for (E.field_list, 0..) |f, i| ctx.print("{s}" ++ comptime upper(f.name), .{if (i == 0) "" else "\t"});
        ctx.print("\n", .{});
    }
    var it = v.iterator();
    while (it.next() catch return outcome.malformed(ctx, "list")) |e| {
        if (E.kind == .record) {
            inline for (E.field_list, 0..) |f, i| {
                if (i > 0) ctx.print("\t", .{});
                printScalar(ctx, f.T, @field(e, f.name));
            }
        } else printScalar(ctx, E, e);
        ctx.print("\n", .{});
    }
}

/// A list in a record: its name, then one indented line per element.
fn printListRecord(ctx: *Context, comptime f: schema.Field, v: f.T.Value) Error!void {
    const E = f.T.Element;
    ctx.print(f.name ++ ":\n", .{});
    var it = v.iterator();
    while (it.next() catch return outcome.malformed(ctx, "list")) |e| {
        ctx.print("  ", .{});
        if (E.kind == .record) {
            inline for (E.field_list, 0..) |ef, i| {
                ctx.print("{s}" ++ ef.name ++ "=", .{if (i == 0) "" else " "});
                printScalar(ctx, ef.T, @field(e, ef.name));
            }
        } else printScalar(ctx, E, e);
        ctx.print("\n", .{});
    }
}

fn upper(comptime name: []const u8) []const u8 {
    comptime {
        var out: [name.len]u8 = undefined;
        for (name, 0..) |c, i| out[i] = std.ascii.toUpper(c);
        const final = out;
        return &final;
    }
}

/// A value as JSON, by its schema: bytes as strings (a hex field as hex),
/// an absent optional as null, a list as an array, a record as an object.
fn printJson(ctx: *Context, comptime op: ops.Op, comptime T: type, v: T.Value) void {
    switch (T.kind) {
        .record => {
            ctx.print("{{", .{});
            inline for (T.field_list, 0..) |f, i| {
                ctx.print("{s}\"" ++ f.name ++ "\":", .{if (i == 0) "" else ","});
                if (comptime isHex(op, f.name)) {
                    const bytes: ?[]const u8 = @field(v, f.name);
                    if (bytes) |b| ctx.print("\"{x}\"", .{b}) else ctx.print("null", .{});
                } else printJson(ctx, op, f.T, @field(v, f.name));
            }
            ctx.print("}}", .{});
        },
        .list => {
            ctx.print("[", .{});
            var it = v.iterator();
            var i: usize = 0;
            // Decode checked every element, so a failure here is a bug; the
            // array ends where it fails.
            while (it.next() catch null) |e| : (i += 1) {
                if (i > 0) ctx.print(",", .{});
                printJson(ctx, op, T.Element, e);
            }
            ctx.print("]", .{});
        },
        .optional => if (v) |x| printJson(ctx, op, T.Inner, x) else ctx.print("null", .{}),
        .defaulted => printJson(ctx, op, T.Inner, v),
        // Past 2^53 a JSON reader that parses doubles (jq, JavaScript)
        // rounds a number, so it is a string, as in ops.json.
        .integer => if (@as(i128, v) > -(1 << 53) and @as(i128, v) < (1 << 53)) ctx.print("{d}", .{v}) else ctx.print("\"{d}\"", .{v}),
        .boolean => ctx.print("{}", .{v}),
        .enumeration => ctx.print("\"{s}\"", .{@tagName(v)}),
        .bytes, .string, .parsed => ctx.print("{f}", .{output.jsonString(v)}),
        else => @compileError("can't print a " ++ @tagName(T.kind) ++ " as JSON"),
    }
}
