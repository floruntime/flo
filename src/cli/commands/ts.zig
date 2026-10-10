//! Time-Series commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo ts write <measurement> --value <n> [--tags k=v,...] [--timestamp <ms>]
//!   flo ts write <measurement> --fields k=v,... [--tags k=v,...] [--timestamp <ms>]
//!   flo ts write --batch [--file <path>] [--precision ns|us|ms|s]
//!   flo ts read <measurement> [--tags "k=v,..."] [--field <name>] [--from <time>] [--to <time>] [--limit <n>]
//!   flo ts query <measurement> [--tags "k=v,..."] [--from <time>] [--window <duration>] [--agg <fn>]
//!   flo ts list [<measurement>] [--fields] [--limit <n>]
//!   flo ts delete <measurement> [--tags "k=v,..."] [--confirm]
//!   flo ts retention <measurement> --raw-ttl <duration>

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const outcome = @import("../outcome.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const output = @import("../output.zig");
const wire = @import("../../util/wire.zig");
const WireReader = wire.WireReader;
const cli_config = @import("../config.zig");
const line_protocol = @import("../../ts/line_protocol.zig");

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the ts command tree
pub fn createTsCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("ts")
        .about("Time-series operations")
        .group("Data Commands")
        .longAbout(
            \\Interact with Flo's built-in time-series storage.
            \\
            \\Write, read, and query time-series data with tag-based filtering,
            \\windowed aggregation, and retention policies. Supports InfluxDB line
            \\protocol for batch ingestion.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("write")
                .about("Write time-series data points")
                .examples(&.{
                    "flo ts write cpu_usage --tags host=web-01 --value 72.5",
                    "flo ts write temperature --tags sensor=A3 --value 23.4 --timestamp 1708700400000",
                    "flo ts write cpu --tags host=web-01 --fields user=72.5,system=7.4,idle=20.1",
                    "flo ts write cpu_usage --value 99.9  (no tags)",
                    "echo 'cpu,host=web-01 user=72.5 1708700400000' | flo ts write --batch",
                    "flo ts write --batch --file metrics.txt --precision ns",
                })
                .optionalArg("measurement", "Measurement name (or omit for --batch)")
                .stringFlag("tags", 't', "", "Tags as comma-separated key=value pairs")
                .stringFlag("value", 'v', "", "Single metric value (e.g. 72.5)")
                .stringFlag("fields", 0, "", "Named fields, one write each and not atomic: user=72.5,system=7.4")
                .stringFlag("timestamp", 0, "", "Explicit timestamp in milliseconds")
                .boolFlag("batch", 'b', "Read InfluxDB line protocol from stdin")
                .stringFlag("file", 'f', "", "Read line protocol from file (with --batch)")
                .stringFlag("precision", 0, "ms", "Timestamp precision: ns, us, ms, s (for --batch)")
                .action(wrapHandler(runWrite)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("read")
                .about("Read raw time-series data points")
                .examples(&.{
                    "flo ts read cpu_usage --from -1h",
                    "flo ts read temperature --tags sensor=A3,env=prod --from -24h --to -1h",
                    "flo ts read cpu --tags host=web-01 --field user --from -1h",
                    "flo ts read cpu_usage --tags host=web-01 --from -1h --limit 100",
                })
                .arg("measurement", "Measurement name")
                .stringFlag("tags", 't', "", "Tag filters: key=val,key2=val2")
                .stringFlag("field", 0, "", "Field name (default: value)")
                .stringFlag("from", 0, "-1h", "Start time: -Nh, -Nd, -Nm, or epoch ms")
                .stringFlag("to", 0, "", "End time (default: now)")
                .uintFlag("limit", 'l', 10000, "Maximum points to return")
                .action(wrapHandler(runRead)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("query")
                .about("Run aggregated time-series query")
                .examples(&.{
                    "flo ts query cpu_usage --from -1h --window 1m --agg avg",
                    "flo ts query temperature --from -24h --window 1h --agg max",
                    "flo ts query http_requests --tags status=500 --from -6h --window 5m --agg count",
                })
                .arg("measurement", "Measurement name")
                .stringFlag("tags", 't', "", "Tag filters: key=val,key2=val2")
                .stringFlag("field", 0, "", "Field name (default: value)")
                .stringFlag("from", 0, "-1h", "Start time: -Nh, -Nd, -Nm, or epoch ms")
                .stringFlag("to", 0, "", "End time (default: now)")
                .stringFlag("window", 'w', "1m", "Aggregation window: Ns, Nm, Nh, Nd")
                .stringFlag("agg", 'a', "avg", "Aggregation function: avg, sum, count, min, max")
                .action(wrapHandler(runQuery)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List measurements")
                .aliases(&.{"ls"})
                .examples(&.{
                    "flo ts list",
                    "flo ts list --limit 50",
                })
                .uintFlag("limit", 'l', 1000, "Maximum items to return")
                .action(wrapHandler(runList)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("delete")
                .about("Delete time-series data")
                .examples(&.{
                    "flo ts delete cpu_usage --confirm",
                    "flo ts delete temperature --tags sensor=A3 --confirm",
                })
                .arg("measurement", "Measurement to delete")
                .stringFlag("tags", 't', "", "Specific series tags to delete")
                .boolFlag("confirm", 0, "Required to confirm deletion")
                .action(wrapHandler(runDelete)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("retention")
                .about("Delete a measurement's points older than a duration, once")
                .longAbout(
                    \\Deletes the measurement's points older than --raw-ttl, now. It is not
                    \\a standing policy: later points stay until it is run again.
                )
                .examples(&.{
                    "flo ts retention cpu_usage --raw-ttl 7d",
                })
                .arg("measurement", "Measurement name")
                .stringFlag("raw-ttl", 0, "", "Age to keep: Ns, Nm, Nh or Nd (e.g., 7d)")
                .action(wrapHandler(runRetention)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("floql")
                .about("Execute a FloQL pipeline query")
                .examples(&.{
                    "flo ts floql 'cpu_usage{host=web-01}[1h] | window(5m) | avg()'",
                    "flo ts floql 'requests[30m] | rate(1s)'",
                    "flo ts floql 'temperature[24h] | window(1h) | max() | topk(3)'",
                })
                .arg("query", "FloQL query string")
                .action(wrapHandler(runFloql)),
        )
        .build();
}

// ========================================================================
// Helpers
// ========================================================================

/// Parse a relative time string like "-1h", "-30m", "-7d" to epoch ms offset from now.
/// Also accepts raw epoch ms (positive integers).
fn parseTimeArg(s: []const u8) ?i64 {
    if (s.len == 0) return null;

    // Raw epoch ms
    if (s[0] != '-') {
        return std.fmt.parseInt(i64, s, 10) catch null;
    }

    // Relative: -Nh, -Nm, -Nd, -Ns
    if (s.len < 3) return null; // need at least "-1h"
    const num_str = s[1 .. s.len - 1];
    const unit = s[s.len - 1];
    const num = std.fmt.parseInt(i64, num_str, 10) catch return null;

    const now_ms = @import("stdx").time.milliTimestamp();

    return switch (unit) {
        's' => now_ms - num * 1000,
        'm' => now_ms - num * 60 * 1000,
        'h' => now_ms - num * 3600 * 1000,
        'd' => now_ms - num * 86400 * 1000,
        else => null,
    };
}

/// Parse a duration string like "1m", "5m", "1h", "1d" to milliseconds.
fn parseDuration(s: []const u8) ?i64 {
    if (s.len < 2) return null;
    const num_str = s[0 .. s.len - 1];
    const unit = s[s.len - 1];
    const num = std.fmt.parseInt(i64, num_str, 10) catch return null;

    return switch (unit) {
        's' => num * 1000,
        'm' => num * 60 * 1000,
        'h' => num * 3600 * 1000,
        'd' => num * 86400 * 1000,
        else => null,
    };
}

// ========================================================================
// Command Handlers
// ========================================================================

fn runWrite(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const is_batch = ctx.getBool("batch");

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    if (is_batch) {
        // Line protocol carries its own measurement, tags, fields and time.
        if (ctx.getPositional("measurement") != null) {
            return outcome.usage(ctx, "--batch takes no measurement; each line names its own", .{});
        }
        inline for (.{ "tags", "value", "fields", "timestamp" }) |flag| {
            const given: []const u8 = ctx.getString(flag) orelse "";
            if (given.len > 0) {
                return outcome.usage(ctx, "--batch can't be combined with --" ++ flag ++ "; each line carries its own", .{});
            }
        }
        return runWriteBatch(ctx, &client, namespace);
    }

    const measurement = ctx.getPositional("measurement") orelse {
        return outcome.usage(ctx, "measurement name required (or use --batch)", .{});
    };
    const tags = ctx.getString("tags") orelse "";
    const value_flag = ctx.getString("value") orelse "";
    const fields_flag = ctx.getString("fields") orelse "";

    const ts_str = ctx.getString("timestamp") orelse "";
    var timestamp_ms: ?i64 = null;
    if (ts_str.len > 0) {
        timestamp_ms = std.fmt.parseInt(i64, ts_str, 10) catch {
            return outcome.usage(ctx, "--timestamp '{s}' is not a whole number of milliseconds", .{ts_str});
        };
        if (timestamp_ms.? <= 0) {
            return outcome.usage(ctx, "--timestamp must be > 0 ms, not {s}", .{ts_str});
        }
    }

    if (value_flag.len > 0 and fields_flag.len > 0) {
        return outcome.usage(ctx, "use --value or --fields, not both.", .{});
    }

    if (value_flag.len > 0) {
        const value = parseValue(ctx, "--value", value_flag) orelse return error.Usage;
        client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);
        var result = client_mod.ts.write(&client, namespace, measurement, null, value, tags, timestamp_ms) catch |err| return outcome.requestFailed(ctx, err);
        defer result.deinit();
        try outcome.check(ctx, result);
        // [series_hash:u64][timestamp_ms:i64][sequence:u64]
        const data = result.asRawData() orelse return outcome.malformed(ctx, "write response (empty)");
        if (data.len != 24) return outcome.malformed(ctx, "write response");
        ctx.print("OK ({d}-{d})\n", .{ std.mem.readInt(i64, data[8..16], .little), std.mem.readInt(u64, data[16..24], .little) });
        return;
    }

    if (fields_flag.len == 0) {
        ctx.printErr("Error: --value or --fields is required.\n  Usage: flo ts write <measurement> --value <n> [--tags k=v,...]\n     or: flo ts write <measurement> --fields k=v,... [--tags k=v,...]\n", .{});
        return error.Usage;
    }

    // Each field is its own write; every field is checked before any is sent.
    var names: [64][]const u8 = undefined;
    var values: [64]f64 = undefined;
    var n: usize = 0;
    var it = std.mem.splitScalar(u8, fields_flag, ',');
    while (it.next()) |pair| {
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse {
            return outcome.usage(ctx, "field '{s}' is not name=value", .{pair});
        };
        const name = pair[0..eq];
        if (name.len == 0) {
            return outcome.usage(ctx, "field '{s}' has no name", .{pair});
        }
        if (std.mem.trim(u8, name, " \t").len != name.len) {
            return outcome.usage(ctx, "field name '{s}' has surrounding spaces", .{name});
        }
        for (names[0..n]) |seen| if (std.mem.eql(u8, seen, name)) {
            return outcome.usage(ctx, "field '{s}' is given more than once", .{name});
        };
        if (n == names.len) {
            return outcome.usage(ctx, "at most {d} fields", .{names.len});
        }
        names[n] = name;
        values[n] = parseValue(ctx, name, pair[eq + 1 ..]) orelse return error.Usage;
        n += 1;
    }

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // One timestamp for every field, so they read back as one point.
    const ts = timestamp_ms orelse @import("stdx").time.milliTimestamp();
    for (names[0..n], values[0..n], 0..) |name, value, written| {
        var result = client_mod.ts.write(&client, namespace, measurement, name, value, tags, ts) catch |err| {
            const failed = outcome.requestFailed(ctx, err);
            ctx.printErr("  (field {s}; {d} of {d} fields written)\n", .{ name, written, n });
            return failed;
        };
        defer result.deinit();
        if (result.isError()) {
            return outcome.refusal(ctx, result.status, result.errorMessage(), "field {s} ({d} of {d} fields written): ", .{ name, written, n });
        }
    }
    ctx.print("OK ({d} fields at {d})\n", .{ n, ts });
}

/// A value the server will store: a finite number.
fn parseValue(ctx: *commander.Context, what: []const u8, text: []const u8) ?f64 {
    const v = std.fmt.parseFloat(f64, text) catch {
        ctx.printErr("Error: {s} '{s}' is not a number\n", .{ what, text });
        return null;
    };
    if (!std.math.isFinite(v)) {
        ctx.printErr("Error: {s} '{s}' is not a finite number\n", .{ what, text });
        return null;
    }
    if (line_protocol.integerPastExact(text)) {
        ctx.printErr("Error: {s} '{s}' is past 2^53 and would not be stored exactly\n", .{ what, text });
        return null;
    }
    return v;
}

/// A batch line's parse error as a sentence naming the token at fault.
fn lineError(buf: []u8, err: anyerror, token: []const u8) []const u8 {
    return switch (err) {
        error.InvalidTimestamp => std.fmt.bufPrint(buf, "timestamp '{s}' is not a positive whole number", .{token}),
        error.TrailingInput => std.fmt.bufPrint(buf, "unexpected '{s}' after the timestamp", .{token}),
        error.EscapesNotSupported => std.fmt.bufPrint(buf, "backslash escapes ('{s}') are not supported", .{token}),
        error.DuplicateTag => std.fmt.bufPrint(buf, "tag key '{s}' is given more than once", .{token}),
        error.DuplicateField => std.fmt.bufPrint(buf, "field '{s}' is given more than once", .{token}),
        error.IntegerTooLarge => std.fmt.bufPrint(buf, "integer '{s}' is past 2^53 and would not be stored exactly", .{token}),
        error.NotFinite => std.fmt.bufPrint(buf, "field value '{s}' is not a finite number", .{token}),
        error.InvalidFieldValue => std.fmt.bufPrint(buf, "field value '{s}' is not a number", .{token}),
        else => std.fmt.bufPrint(buf, "{s} at '{s}'", .{ @errorName(err), token }),
    } catch @errorName(err);
}

fn runWriteBatch(ctx: *commander.Context, client: *Client, namespace: []const u8) commander.Error!void {
    const file_path = ctx.getString("file") orelse "";
    const precision_str = ctx.getString("precision") orelse "ms";
    const precision = line_protocol.Precision.fromString(precision_str) orelse {
        return outcome.usage(ctx, "--precision '{s}' must be ns, us, ms or s", .{precision_str});
    };

    var line_data: []u8 = undefined;

    if (file_path.len > 0) {
        const file = @import("stdx").fs.openFile(file_path, .{}) catch |err| {
            return outcome.usage(ctx, "cannot open file '{s}': {}", .{ file_path, err });
        };
        defer @import("stdx").fs.closeFile(file);

        line_data = @import("stdx").fs.readToEndAlloc(file, ctx.allocator, 10 * 1024 * 1024) catch |err| {
            return outcome.usage(ctx, "failed to read file '{s}': {}", .{ file_path, err });
        };
    } else {
        var stdin_buf: std.ArrayList(u8) = .empty;
        defer stdin_buf.deinit(ctx.allocator);
        var read_buf: [4096]u8 = undefined;
        while (true) {
            const n = std.posix.read(std.posix.STDIN_FILENO, &read_buf) catch |err| {
                return outcome.usage(ctx, "failed to read stdin: {}", .{err});
            };
            if (n == 0) break;
            try stdin_buf.appendSlice(ctx.allocator, read_buf[0..n]);
        }
        line_data = try stdin_buf.toOwnedSlice(ctx.allocator);
    }
    defer ctx.allocator.free(line_data);

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // Each field of each line is one write. A bad line is reported by number
    // and the rest still go; the command ends with the first failed line's
    // outcome.
    var points: u32 = 0;
    var lines_total: u32 = 0;
    var lines_failed: u32 = 0;
    var first_failure: ?commander.Error = null;
    var diag_buf: [192]u8 = undefined;
    var line_no: u32 = 0;
    var line_iter = std.mem.splitScalar(u8, line_data, '\n');
    while (line_iter.next()) |raw| {
        line_no += 1;
        const line = std.mem.trim(u8, raw, &[_]u8{ ' ', '\t', '\r' });
        if (line.len == 0 or line[0] == '#') continue;
        lines_total += 1;

        const failure: commander.Error = blk: {
            var diag: line_protocol.Diagnostic = .{};
            const parsed = line_protocol.parseLineDiagnosed(line, precision, ctx.allocator, &diag) catch |err| {
                ctx.printErr("line {d}: {s}\n", .{ line_no, lineError(&diag_buf, err, diag.token) });
                break :blk error.Usage;
            };
            defer line_protocol.freeParsedLine(parsed, ctx.allocator);

            var tags_buf: [1024]u8 = undefined;
            var tags_w = std.Io.Writer.fixed(&tags_buf);
            for (parsed.tags, 0..) |tag, i| {
                tags_w.print("{s}{s}={s}", .{ if (i > 0) "," else "", tag.key, tag.value }) catch {
                    ctx.printErr("line {d}: tags too long\n", .{line_no});
                    break :blk error.Usage;
                };
            }

            // The parser checked every field, so a line is written whole
            // unless the server refuses part of it. A line without a
            // timestamp is stamped once, so its fields agree.
            const ts = if (parsed.timestamp_ms != 0) parsed.timestamp_ms else @import("stdx").time.milliTimestamp();
            for (parsed.fields, 0..) |field, written| {
                var result = client_mod.ts.write(client, namespace, parsed.measurement, field.name, field.value, tags_w.buffered(), ts) catch |err| {
                    const failed = outcome.requestFailed(ctx, err);
                    ctx.printErr("  (line {d}; {d} of {d} fields written)\n", .{ line_no, written, parsed.fields.len });
                    break :blk failed;
                };
                defer result.deinit();
                if (result.isError()) {
                    break :blk outcome.refusal(ctx, result.status, result.errorMessage(), "line {d}: field {s} ({d} of {d} fields written): ", .{ line_no, field.name, written, parsed.fields.len });
                }
                points += 1;
            }
            continue;
        };
        lines_failed += 1;
        if (first_failure == null) first_failure = failure;
    }

    ctx.print("Wrote {d} points\n", .{points});
    if (first_failure) |failure| {
        ctx.printErr("Error: {d} of {d} lines failed\n", .{ lines_failed, lines_total });
        return failure;
    }
}

fn runRead(ctx: *commander.Context) commander.Error!void {
    const measurement = ctx.getPositional("measurement").?;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const format = output.getFormat(ctx);
    const tags = ctx.getString("tags") orelse "";
    const field = ctx.getString("field") orelse "";
    const from_str = ctx.getString("from") orelse "-1h";
    const to_str = ctx.getString("to") orelse "";
    const limit = ctx.getUint("limit") orelse 10000;

    const from_ms = parseTimeArg(from_str) orelse 0;
    const to_ms = parseTimeArg(to_str) orelse 0;

    if (output.isVerbose(ctx)) {
        ctx.printErr("[verbose] READ measurement={s} namespace={s} endpoint={s}\n", .{ measurement, namespace, endpoint });
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.ts.read(&client, namespace, measurement, .{
        .tags = tags,
        .field = field,
        .from_ms = from_ms,
        .to_ms = to_ms,
        .limit = @intCast(limit),
    }) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    // An existing series with no points in range answers ok and empty (0);
    // not_found means the measurement itself is absent.
    if (result.isNotFound()) {
        ctx.print("(no data)\n", .{});
        return error.NotFound;
    }

    try outcome.check(ctx, result);

    // An empty range still answers with a count of 0.
    const data = result.asRawData() orelse return outcome.malformed(ctx, "read response (empty)");
    const points = parsePoints(data) catch return outcome.malformed(ctx, "read response");

    if (points.len() == 0) {
        ctx.print("(no data)\n", .{});
        return;
    }

    switch (format) {
        .table => {
            var table = output.Table.init(ctx.allocator);
            defer table.deinit();
            try table.addColumn("TIMESTAMP", .left);
            try table.addColumn("VALUE", .right);

            for (0..points.len()) |i| {
                const p = points.at(i);
                var ts_buf: [32]u8 = undefined;
                const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{p.ms}) catch "";
                var val_buf: [32]u8 = undefined;
                const val_str = std.fmt.bufPrint(&val_buf, "{d:.4}", .{p.value}) catch "";
                table.addRow(&.{ ts_str, val_str }) catch return error.OutOfMemory;
            }
            table.print(ctx);
        },
        .json => {
            ctx.print("[\n", .{});
            for (0..points.len()) |i| {
                const p = points.at(i);
                if (i > 0) ctx.print(",\n", .{});
                ctx.print("  {{\"timestamp_ms\": {d}, \"value\": {d:.6}}}", .{ p.ms, p.value });
            }
            ctx.print("\n]\n", .{});
        },
        .raw => {
            for (0..points.len()) |i| {
                const p = points.at(i);
                ctx.print("{d} {d:.6}\n", .{ p.ms, p.value });
            }
        },
    }
}

fn runQuery(ctx: *commander.Context) commander.Error!void {
    const measurement = ctx.getPositional("measurement").?;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const format = output.getFormat(ctx);
    const tags = ctx.getString("tags") orelse "";
    const field = ctx.getString("field") orelse "";
    const from_str = ctx.getString("from") orelse "-1h";
    const to_str = ctx.getString("to") orelse "";
    const window_str = ctx.getString("window") orelse "1m";
    const agg = ctx.getString("agg") orelse "avg";

    const from_ms = parseTimeArg(from_str) orelse 0;
    const to_ms = parseTimeArg(to_str) orelse 0;
    const window_ms = parseDuration(window_str) orelse 60000;

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.ts.query(&client, namespace, measurement, .{
        .tags = tags,
        .field = field,
        .from_ms = from_ms,
        .to_ms = to_ms,
        .window_ms = window_ms,
        .aggregation = agg,
    }) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    // An existing series with no points in range answers ok and empty (0);
    // not_found means the measurement itself is absent.
    if (result.isNotFound()) {
        ctx.print("(no data)\n", .{});
        return error.NotFound;
    }

    try outcome.check(ctx, result);

    // Produced by serializeQueryResult in ts/handler.zig; an empty range
    // answers with a series count of 0.
    const data = result.asRawData() orelse return outcome.malformed(ctx, "query response (empty)");
    const series = parseSeriesSet(ctx.allocator, data, .query) catch |err| switch (err) {
        error.Truncated => return outcome.malformed(ctx, "query response"),
        error.OutOfMemory => return error.OutOfMemory,
    };
    defer ctx.allocator.free(series);

    if (series.len == 0) {
        ctx.print("(no data)\n", .{});
        return;
    }

    switch (format) {
        .table => {
            var table = output.Table.init(ctx.allocator);
            defer table.deinit();
            try table.addColumn("WINDOW_START", .left);
            var upper_buf: [16]u8 = undefined;
            const upper_len = @min(agg.len, upper_buf.len);
            const upper_agg = std.ascii.upperString(upper_buf[0..upper_len], agg[0..upper_len]);
            try table.addColumn(upper_agg, .right);

            for (series) |sr| {
                for (0..sr.points.len()) |b| {
                    const p = sr.points.at(b);
                    var ts_buf: [32]u8 = undefined;
                    const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{p.ms}) catch "";
                    var val_buf: [32]u8 = undefined;
                    const val_str = std.fmt.bufPrint(&val_buf, "{d:.4}", .{p.value}) catch "";
                    table.addRow(&.{ ts_str, val_str }) catch return error.OutOfMemory;
                }
            }
            table.print(ctx);
        },
        .json => {
            ctx.print("{{\n  \"series\": [\n", .{});
            for (series, 0..) |sr, s| {
                if (s > 0) ctx.print(",\n", .{});
                ctx.print("    {{\"series\": \"{s}\", \"buckets\": [\n", .{sr.key});
                for (0..sr.points.len()) |b| {
                    const p = sr.points.at(b);
                    if (b > 0) ctx.print(",\n", .{});
                    ctx.print("      {{\"start_ms\": {d}, \"value\": {d:.6}}}", .{ p.ms, p.value });
                }
                ctx.print("\n    ]}}", .{});
            }
            ctx.print("\n  ]\n}}\n", .{});
        },
        .raw => {
            for (series) |sr| {
                for (0..sr.points.len()) |b| {
                    const p = sr.points.at(b);
                    ctx.print("{d} {d:.6}\n", .{ p.ms, p.value });
                }
            }
        },
    }
}

fn runList(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const limit = ctx.getUint("limit") orelse 1000;
    const format = output.getFormat(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // Client-side pagination: fetch pages until we have `limit` names or
    // the server signals no more data.
    var all_names: std.ArrayList([]const u8) = .empty;
    defer {
        for (all_names.items) |n| ctx.allocator.free(n);
        all_names.deinit(ctx.allocator);
    }

    var cursor: ?[]u8 = null;
    defer if (cursor) |c| ctx.allocator.free(c);

    const per_page: u32 = @min(limit, 1000);
    const max_pages: u32 = 10000; // safety guard
    var page: u32 = 0;

    while (all_names.items.len < limit and page < max_pages) : (page += 1) {
        var result = client_mod.ts.list(
            &client,
            namespace,
            per_page,
            if (cursor) |c| c[0..] else null,
        ) catch |err| return outcome.requestFailed(ctx, err);
        defer result.deinit();

        try outcome.check(ctx, result);

        const data = result.asRawData() orelse return outcome.malformed(ctx, "list response (empty)");
        const next = parseNamePage(ctx.allocator, data, &all_names, limit) catch |err| switch (err) {
            error.Truncated => return outcome.malformed(ctx, "list response"),
            error.OutOfMemory => return error.OutOfMemory,
        };

        if (cursor) |c| ctx.allocator.free(c);
        cursor = null;
        cursor = try ctx.allocator.dupe(u8, next orelse break);
    }

    if (all_names.items.len == 0) {
        ctx.print("(none)\n", .{});
        return;
    }

    switch (format) {
        .raw, .table => {
            for (all_names.items, 0..) |name, i| {
                if (i > 0) ctx.print("\n", .{});
                ctx.print("{s}", .{name});
            }
            ctx.print("\n", .{});
        },
        .json => {
            ctx.print("[", .{});
            for (all_names.items, 0..) |name, i| {
                if (i > 0) ctx.print(", ", .{});
                ctx.print("\"{s}\"", .{name});
            }
            ctx.print("]\n", .{});
        },
    }
}

fn runDelete(ctx: *commander.Context) commander.Error!void {
    const measurement = ctx.getPositional("measurement").?;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const tags = ctx.getString("tags") orelse "";
    const confirm = ctx.getBool("confirm");

    if (!confirm) {
        ctx.printErr("Error: --confirm flag required to delete time-series data\n", .{});
        ctx.printErr("This will permanently delete series data for '{s}'\n", .{measurement});
        return error.Usage;
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.ts.delete(&client, namespace, measurement, tags) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    ctx.print("OK\n", .{});
}

fn runRetention(ctx: *commander.Context) commander.Error!void {
    const measurement = ctx.getPositional("measurement").?;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const raw_ttl = ctx.getString("raw-ttl") orelse "";
    if (raw_ttl.len == 0) {
        return outcome.usage(ctx, "--raw-ttl is required (e.g. --raw-ttl 7d)", .{});
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.ts.retention(&client, namespace, measurement, raw_ttl) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    ctx.print("OK\n", .{});
}

// ========================================================================
// FloQL
// ========================================================================

fn runFloql(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    const query_str = ctx.getPositional("query") orelse {
        return outcome.usage(ctx, "query argument is required", .{});
    };

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.ts.floql(&client, namespace, query_str) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    // An encoded SeriesSet (ts/floql/series_set.zig); no match is a count of 0.
    if (result.data.len == 0) return outcome.malformed(ctx, "floql response (empty)");
    const series = parseSeriesSet(ctx.allocator, result.data, .floql) catch |err| switch (err) {
        error.Truncated => return outcome.malformed(ctx, "floql response"),
        error.OutOfMemory => return error.OutOfMemory,
    };
    defer ctx.allocator.free(series);

    if (series.len == 0) {
        ctx.print("(no series)\n", .{});
        return;
    }

    for (series) |sr| {
        ctx.print("--- {s} ({s}) [{d} points] ---\n", .{ sr.key, sr.field, sr.points.len() });
        for (0..sr.points.len()) |i| {
            const p = sr.points.at(i);
            ctx.print("  {d}: {d:.4}\n", .{ p.ms, p.value });
        }
    }
}

// ========================================================================
// Response parsing
// ========================================================================

const Point = struct { ms: i64, value: f64 };

/// `n` points of `[ms:i64][value:f64]`, read in place.
const Points = struct {
    bytes: []const u8,

    fn len(self: Points) usize {
        return self.bytes.len / 16;
    }

    fn at(self: Points, i: usize) Point {
        const p = self.bytes[i * 16 ..][0..16];
        return .{
            .ms = std.mem.readInt(i64, p[0..8], .little),
            .value = @bitCast(std.mem.readInt(u64, p[8..16], .little)),
        };
    }
};

fn readPoints(reader: *WireReader) error{Truncated}!Points {
    const count = reader.readU32() orelse return error.Truncated;
    const n = std.math.mul(usize, count, 16) catch return error.Truncated;
    return .{ .bytes = reader.readSlice(n) orelse return error.Truncated };
}

/// A read answer: `[count:u32]([ms:i64][value:f64])*`.
fn parsePoints(data: []const u8) error{Truncated}!Points {
    var reader = WireReader.init(data);
    return readPoints(&reader);
}

const Series = struct { key: []const u8, field: []const u8, points: Points };

/// `[series_count:u32]` then per series `[key_len:u32][key]`, for floql
/// `[field_len:u32][field]`, then the points. Slices point into `data`.
fn parseSeriesSet(
    allocator: Allocator,
    data: []const u8,
    comptime shape: enum { query, floql },
) error{ Truncated, OutOfMemory }![]Series {
    var reader = WireReader.init(data);
    const count = reader.readU32() orelse return error.Truncated;
    var series: std.ArrayList(Series) = .empty;
    errdefer series.deinit(allocator);
    for (0..count) |_| {
        const key = reader.readLengthPrefixed(u32) orelse return error.Truncated;
        const field = if (shape == .floql) reader.readLengthPrefixed(u32) orelse return error.Truncated else "";
        try series.append(allocator, .{ .key = key, .field = field, .points = try readPoints(&reader) });
    }
    return series.toOwnedSlice(allocator);
}

/// One page of a list: `[count:u32]([name_len:u16][name])*[has_more:u8]
/// [cursor_len:u16][cursor]`. Appends copies of its names to `names` until it
/// holds `limit`, and returns the next page's cursor, or null on the last.
fn parseNamePage(
    allocator: Allocator,
    data: []const u8,
    names: *std.ArrayList([]const u8),
    limit: usize,
) error{ Truncated, OutOfMemory }!?[]const u8 {
    var reader = WireReader.init(data);
    const count = reader.readU32() orelse return error.Truncated;
    for (0..count) |_| {
        const name = reader.readLengthPrefixed(u16) orelse return error.Truncated;
        if (name.len == 0 or names.items.len >= limit) continue;
        const owned = try allocator.dupe(u8, name);
        errdefer allocator.free(owned);
        try names.append(allocator, owned);
    }
    const has_more = (reader.readU8() orelse return error.Truncated) != 0;
    const next = reader.readLengthPrefixed(u16) orelse return error.Truncated;
    return if (has_more and next.len > 0) next else null;
}

// ==================== Testing ====================

test "create ts command" {
    const allocator = std.testing.allocator;

    const cmd = try createTsCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("ts", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 7);
}

test "parseTimeArg relative" {
    // These parse as relative-to-now, so we test they're non-null and negative
    const result_h = parseTimeArg("-1h");
    try std.testing.expect(result_h != null);
    try std.testing.expect(result_h.? > 0); // should be epoch ms (positive, near now)

    const result_m = parseTimeArg("-30m");
    try std.testing.expect(result_m != null);

    const result_d = parseTimeArg("-7d");
    try std.testing.expect(result_d != null);
}

test "parseTimeArg absolute" {
    const result = parseTimeArg("1708700400000");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(i64, 1708700400000), result.?);
}

test "parseDuration" {
    try std.testing.expectEqual(@as(i64, 60000), parseDuration("1m").?);
    try std.testing.expectEqual(@as(i64, 300000), parseDuration("5m").?);
    try std.testing.expectEqual(@as(i64, 3600000), parseDuration("1h").?);
    try std.testing.expectEqual(@as(i64, 86400000), parseDuration("1d").?);
    try std.testing.expectEqual(@as(i64, 30000), parseDuration("30s").?);
}

test "ts: a read answer parses, and a cut-short one is Truncated" {
    const answer = "\x02\x00\x00\x00" ++ "\x05" ++ "\x00" ** 7 ++ "\x00" ** 6 ++ "\xf0\x3f" ++
        "\x06" ++ "\x00" ** 7 ++ "\x00" ** 8;
    const points = try parsePoints(answer);
    try std.testing.expectEqual(@as(usize, 2), points.len());
    try std.testing.expectEqual(@as(i64, 5), points.at(0).ms);
    try std.testing.expectEqual(@as(f64, 1.0), points.at(0).value);
    try std.testing.expectEqual(@as(usize, 0), (try parsePoints("\x00\x00\x00\x00")).len());
    for (0..answer.len) |n| try std.testing.expectError(error.Truncated, parsePoints(answer[0..n]));
}

test "ts: query and floql answers parse, and cut-short ones are Truncated" {
    const a = std.testing.allocator;
    const point = "\x09" ++ "\x00" ** 7 ++ "\x00" ** 8;
    const query = "\x01\x00\x00\x00" ++ "\x03\x00\x00\x00cpu" ++ "\x01\x00\x00\x00" ++ point;
    const floql = "\x01\x00\x00\x00" ++ "\x03\x00\x00\x00cpu" ++ "\x05\x00\x00\x00value" ++ "\x01\x00\x00\x00" ++ point;

    const q = try parseSeriesSet(a, query, .query);
    defer a.free(q);
    try std.testing.expectEqualStrings("cpu", q[0].key);
    try std.testing.expectEqual(@as(i64, 9), q[0].points.at(0).ms);

    const f = try parseSeriesSet(a, floql, .floql);
    defer a.free(f);
    try std.testing.expectEqualStrings("value", f[0].field);
    try std.testing.expectEqual(@as(usize, 1), f[0].points.len());

    for (0..query.len) |n| try std.testing.expectError(error.Truncated, parseSeriesSet(a, query[0..n], .query));
    for (0..floql.len) |n| try std.testing.expectError(error.Truncated, parseSeriesSet(a, floql[0..n], .floql));
}

test "ts: a list page parses, and a cut-short one is Truncated" {
    const a = std.testing.allocator;
    const page = "\x02\x00\x00\x00" ++ "\x03\x00cpu" ++ "\x03\x00mem" ++ "\x01" ++ "\x02\x00c1";
    var names: std.ArrayList([]const u8) = .empty;
    defer {
        for (names.items) |n| a.free(n);
        names.deinit(a);
    }
    try std.testing.expectEqualStrings("c1", (try parseNamePage(a, page, &names, 1)).?);
    try std.testing.expectEqual(@as(usize, 1), names.items.len);
    try std.testing.expectEqualStrings("cpu", names.items[0]);

    for (1..page.len) |n| {
        for (names.items) |x| a.free(x);
        names.clearRetainingCapacity();
        try std.testing.expectError(error.Truncated, parseNamePage(a, page[0..n], &names, 100));
    }
}
