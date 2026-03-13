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
//!   flo ts retention <measurement> [--raw-ttl <duration>] [--downsample "interval:agg:ttl"]

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const output = @import("../output.zig");
const wire = @import("../../util/wire.zig");
const WireReader = wire.WireReader;
const cli_config = @import("../config.zig");

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
                .stringFlag("fields", 0, "", "Named fields: user=72.5,system=7.4,idle=20.1")
                .stringFlag("timestamp", 0, "", "Explicit timestamp in milliseconds")
                .boolFlag("batch", 'b', "Read InfluxDB line protocol from stdin")
                .stringFlag("file", 'f', "", "Read line protocol from file (with --batch)")
                .stringFlag("precision", 'p', "ms", "Timestamp precision: ns, us, ms, s (for --batch)")
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
                .about("List measurements or series")
                .aliases(&.{"ls"})
                .examples(&.{
                    "flo ts list",
                    "flo ts list cpu_usage",
                    "flo ts list cpu --fields",
                })
                .optionalArg("measurement", "Measurement to inspect (omit to list all)")
                .boolFlag("fields", 0, "Show field names for measurement")
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
                .about("Configure retention and downsampling policy")
                .examples(&.{
                    "flo ts retention cpu_usage --raw-ttl 7d",
                    "flo ts retention cpu_usage --downsample 1m:avg:30d",
                    "flo ts retention cpu_usage --raw-ttl 7d --downsample 1m:avg:30d --downsample 1h:avg:365d",
                    "flo ts retention cpu_usage --show",
                })
                .arg("measurement", "Measurement name")
                .stringFlag("raw-ttl", 0, "", "Raw data TTL: Nd, Nh, Nm (e.g., 7d)")
                .stringFlag("downsample", 'd', "", "Downsample rule: interval:agg:ttl (repeatable)")
                .boolFlag("show", 0, "Show current retention policy")
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

    const now_ms = std.time.milliTimestamp();

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

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        ctx.printErr("Is the Flo server running at {s}?\n", .{endpoint});
        return error.CommandFailed;
    };

    if (is_batch) {
        return runWriteBatch(ctx, &client, namespace);
    }

    // Single-point mode
    const measurement = ctx.getPositional("measurement") orelse {
        ctx.printErr("Error: measurement name required (or use --batch)\n", .{});
        return error.CommandFailed;
    };

    // Parse tags from --tags flag
    var tags_buf: [512]u8 = undefined;
    var tags_len: usize = 0;

    const tags_flag = ctx.getString("tags") orelse "";
    if (tags_flag.len > 0) {
        @memcpy(tags_buf[0..tags_flag.len], tags_flag);
        tags_len = tags_flag.len;
    }

    // Parse fields from --value or --fields flag (one is required)
    const value_flag = ctx.getString("value") orelse "";
    const fields_flag = ctx.getString("fields") orelse "";

    var fields_buf: [512]u8 = undefined;
    var fields_len: usize = 0;

    if (value_flag.len > 0 and fields_flag.len > 0) {
        ctx.printErr("Error: use --value or --fields, not both.\n", .{});
        return error.CommandFailed;
    } else if (value_flag.len > 0) {
        // --value 72.5 → fields = "72.5" (bare value)
        if (value_flag.len > fields_buf.len) {
            ctx.printErr("Error: value too long\n", .{});
            return error.CommandFailed;
        }
        @memcpy(fields_buf[0..value_flag.len], value_flag);
        fields_len = value_flag.len;
    } else if (fields_flag.len > 0) {
        // --fields user=72.5,system=7.4
        if (fields_flag.len > fields_buf.len) {
            ctx.printErr("Error: fields too long\n", .{});
            return error.CommandFailed;
        }
        @memcpy(fields_buf[0..fields_flag.len], fields_flag);
        fields_len = fields_flag.len;
    } else {
        ctx.printErr("Error: --value or --fields is required.\n  Usage: flo ts write <measurement> --value <n> [--tags k=v,...]\n     or: flo ts write <measurement> --fields k=v,... [--tags k=v,...]\n", .{});
        return error.CommandFailed;
    }

    const tags_str = tags_buf[0..tags_len];
    const fields_str = fields_buf[0..fields_len];

    // Parse timestamp
    const ts_str = ctx.getString("timestamp") orelse "";
    const timestamp_ms: ?i64 = if (ts_str.len > 0) (std.fmt.parseInt(i64, ts_str, 10) catch null) else null;

    var result = client_mod.ts.write(&client, namespace, measurement, tags_str, fields_str, timestamp_ms) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    // Parse write response: [series_hash:u64][timestamp_ms:i64][sequence:u64]
    if (result.asRawData()) |data| {
        if (data.len >= 24) {
            const ts_ms = std.mem.readInt(i64, data[8..16], .little);
            const seq = std.mem.readInt(u64, data[16..24], .little);
            ctx.print("OK ({d}-{d})\n", .{ ts_ms, seq });
            return;
        }
    }
    ctx.print("OK\n", .{});
}

fn runWriteBatch(ctx: *commander.Context, client: *Client, namespace: []const u8) commander.Error!void {
    const file_path = ctx.getString("file") orelse "";
    const precision_str = ctx.getString("precision") orelse "ms";

    // Map precision string to u8 enum value
    const precision: u8 = if (std.mem.eql(u8, precision_str, "ns"))
        0
    else if (std.mem.eql(u8, precision_str, "us"))
        1
    else if (std.mem.eql(u8, precision_str, "ms"))
        2
    else if (std.mem.eql(u8, precision_str, "s"))
        3
    else
        2; // default ms

    var line_data: []u8 = undefined;
    var needs_free = false;

    if (file_path.len > 0) {
        // Read from file
        const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
            ctx.printErr("Cannot open file '{s}': {}\n", .{ file_path, err });
            return error.CommandFailed;
        };
        defer file.close();

        line_data = file.readToEndAlloc(ctx.allocator, 10 * 1024 * 1024) catch |err| {
            ctx.printErr("Failed to read file: {}\n", .{err});
            return error.CommandFailed;
        };
        needs_free = true;
    } else {
        // Read from stdin using posix (Zig 0.15 API)
        var stdin_buf: std.ArrayList(u8) = .empty;
        defer stdin_buf.deinit(ctx.allocator);
        var read_buf: [4096]u8 = undefined;
        while (true) {
            const n = std.posix.read(std.posix.STDIN_FILENO, &read_buf) catch |err| {
                ctx.printErr("Failed to read stdin: {}\n", .{err});
                return error.CommandFailed;
            };
            if (n == 0) break;
            stdin_buf.appendSlice(ctx.allocator, read_buf[0..n]) catch |err| {
                ctx.printErr("Failed to buffer stdin: {}\n", .{err});
                return error.CommandFailed;
            };
        }
        line_data = stdin_buf.items;
        needs_free = false; // ArrayList owns the memory, freed by defer above
    }
    defer if (needs_free) ctx.allocator.free(line_data);

    // Send each line as a separate ts_write batch command
    var lines_sent: u32 = 0;
    var lines_failed: u32 = 0;
    var line_iter = std.mem.splitScalar(u8, line_data, '\n');
    while (line_iter.next()) |line| {
        const trimmed = std.mem.trim(u8, line, &[_]u8{ ' ', '\t', '\r' });
        if (trimmed.len == 0) continue;
        if (trimmed[0] == '#') continue; // comment line

        var result = client_mod.ts.writeBatch(client, namespace, trimmed, precision) catch {
            lines_failed += 1;
            continue;
        };
        defer result.deinit();

        if (result.isError()) {
            lines_failed += 1;
        } else {
            lines_sent += 1;
        }
    }

    if (lines_failed > 0) {
        ctx.print("Wrote {d} points ({d} failed)\n", .{ lines_sent, lines_failed });
    } else {
        ctx.print("Wrote {d} points\n", .{lines_sent});
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

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.ts.read(&client, namespace, measurement, .{
        .tags = tags,
        .field = field,
        .from_ms = from_ms,
        .to_ms = to_ms,
        .limit = @intCast(limit),
    }) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isNotFound()) {
        ctx.print("(no data)\n", .{});
        return;
    }

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    // Parse read response: [count:u32] ([timestamp_ms:i64][value:f64])...
    const data = result.asRawData() orelse {
        ctx.print("(no data)\n", .{});
        return;
    };

    if (data.len < 4) {
        ctx.print("(no data)\n", .{});
        return;
    }

    var reader = WireReader.init(data);
    const count = reader.readU32() orelse 0;

    if (count == 0) {
        ctx.print("(no data)\n", .{});
        return;
    }

    switch (format) {
        .table => {
            var table = output.Table.init(ctx.allocator);
            defer table.deinit();
            try table.addColumn("TIMESTAMP", .left);
            try table.addColumn("VALUE", .right);

            var i: u32 = 0;
            while (i < count) : (i += 1) {
                const ts_ms = reader.readI64() orelse break;
                const val_bytes = reader.readSlice(8) orelse break;
                const val = @as(f64, @bitCast(std.mem.readInt(u64, val_bytes[0..8], .little)));

                var ts_buf: [32]u8 = undefined;
                const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{ts_ms}) catch "";
                var val_buf: [32]u8 = undefined;
                const val_str = std.fmt.bufPrint(&val_buf, "{d:.4}", .{val}) catch "";

                table.addRow(&.{ ts_str, val_str }) catch break;
            }
            table.print(ctx);
        },
        .json => {
            ctx.print("[\n", .{});
            var i: u32 = 0;
            while (i < count) : (i += 1) {
                const ts_ms = reader.readI64() orelse break;
                const val_bytes = reader.readSlice(8) orelse break;
                const val = @as(f64, @bitCast(std.mem.readInt(u64, val_bytes[0..8], .little)));

                if (i > 0) ctx.print(",\n", .{});
                ctx.print("  {{\"timestamp_ms\": {d}, \"value\": {d:.6}}}", .{ ts_ms, val });
            }
            ctx.print("\n]\n", .{});
        },
        .raw => {
            var i: u32 = 0;
            while (i < count) : (i += 1) {
                const ts_ms = reader.readI64() orelse break;
                const val_bytes = reader.readSlice(8) orelse break;
                const val = @as(f64, @bitCast(std.mem.readInt(u64, val_bytes[0..8], .little)));

                ctx.print("{d} {d:.6}\n", .{ ts_ms, val });
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

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.ts.query(&client, namespace, measurement, .{
        .tags = tags,
        .field = field,
        .from_ms = from_ms,
        .to_ms = to_ms,
        .window_ms = window_ms,
        .aggregation = agg,
    }) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isNotFound()) {
        ctx.print("(no data)\n", .{});
        return;
    }

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    // Query response is opaque bytes from query.encodeQueryResult()
    // Format: [series_count:u32] for each series: [hash:u64][bucket_count:u32] [bucket_start_ms:i64][value:f64]...
    const data = result.asRawData() orelse {
        ctx.print("(no data)\n", .{});
        return;
    };

    if (data.len < 4) {
        ctx.print("(no data)\n", .{});
        return;
    }

    var reader = WireReader.init(data);
    const series_count = reader.readU32() orelse 0;

    if (series_count == 0) {
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

            var s: u32 = 0;
            while (s < series_count) : (s += 1) {
                _ = reader.readU64() orelse break; // series hash
                const bucket_count = reader.readU32() orelse break;
                var b: u32 = 0;
                while (b < bucket_count) : (b += 1) {
                    const bucket_ms = reader.readI64() orelse break;
                    const val_bytes = reader.readSlice(8) orelse break;
                    const val = @as(f64, @bitCast(std.mem.readInt(u64, val_bytes[0..8], .little)));

                    var ts_buf: [32]u8 = undefined;
                    const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{bucket_ms}) catch "";
                    var val_buf: [32]u8 = undefined;
                    const val_str = std.fmt.bufPrint(&val_buf, "{d:.4}", .{val}) catch "";

                    table.addRow(&.{ ts_str, val_str }) catch break;
                }
            }
            table.print(ctx);
        },
        .json => {
            ctx.print("{{\n  \"series\": [\n", .{});
            var s: u32 = 0;
            while (s < series_count) : (s += 1) {
                const hash = reader.readU64() orelse break;
                const bucket_count = reader.readU32() orelse break;

                if (s > 0) ctx.print(",\n", .{});
                ctx.print("    {{\"series_hash\": {d}, \"buckets\": [\n", .{hash});

                var b: u32 = 0;
                while (b < bucket_count) : (b += 1) {
                    const bucket_ms = reader.readI64() orelse break;
                    const val_bytes = reader.readSlice(8) orelse break;
                    const val = @as(f64, @bitCast(std.mem.readInt(u64, val_bytes[0..8], .little)));

                    if (b > 0) ctx.print(",\n", .{});
                    ctx.print("      {{\"start_ms\": {d}, \"value\": {d:.6}}}", .{ bucket_ms, val });
                }
                ctx.print("\n    ]}}", .{});
            }
            ctx.print("\n  ]\n}}\n", .{});
        },
        .raw => {
            var s: u32 = 0;
            while (s < series_count) : (s += 1) {
                _ = reader.readU64() orelse break;
                const bucket_count = reader.readU32() orelse break;
                var b: u32 = 0;
                while (b < bucket_count) : (b += 1) {
                    const bucket_ms = reader.readI64() orelse break;
                    const val_bytes = reader.readSlice(8) orelse break;
                    const val = @as(f64, @bitCast(std.mem.readInt(u64, val_bytes[0..8], .little)));

                    ctx.print("{d} {d:.6}\n", .{ bucket_ms, val });
                }
            }
        },
    }
}

fn runList(ctx: *commander.Context) commander.Error!void {
    const measurement = ctx.getPositional("measurement") orelse "";
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const limit = ctx.getUint("limit") orelse 1000;
    const format = output.getFormat(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

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
            measurement,
            per_page,
            if (cursor) |c| c[0..] else null,
        ) catch |err| {
            ctx.printErr("Request failed: {}\n", .{err});
            return error.CommandFailed;
        };
        defer result.deinit();

        if (result.isError()) {
            ctx.printErr("Error: {s}\n", .{result.errorMessage()});
            return error.CommandFailed;
        }

        const data = result.asRawData() orelse break;
        if (data.len < 7) break; // minimum: count(4) + has_more(1) + cursor_len(2)

        // Parse binary response:
        //   [count:u32] ([name_len:u16][name])* [has_more:u8] [cursor_len:u16][cursor]?
        var reader = WireReader.init(data);
        const count = reader.readU32() orelse break;
        if (count == 0) break;

        for (0..count) |_| {
            if (all_names.items.len >= limit) break;
            const name = reader.readLengthPrefixed(u16) orelse break;
            if (name.len > 0) {
                const owned = ctx.allocator.dupe(u8, name) catch break;
                all_names.append(ctx.allocator, owned) catch {
                    ctx.allocator.free(owned);
                    break;
                };
            }
        }

        // tail: has_more + cursor
        const has_more_byte = reader.readU8() orelse break;
        const has_more = has_more_byte != 0;
        const next_cursor = reader.readLengthPrefixed(u16) orelse null;

        // Free old cursor
        if (cursor) |c| ctx.allocator.free(c);
        cursor = null;

        if (!has_more or next_cursor == null or next_cursor.?.len == 0) break;
        cursor = ctx.allocator.dupe(u8, next_cursor.?) catch break;
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
        return error.CommandFailed;
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.ts.delete(&client, namespace, measurement, tags) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("OK\n", .{});
}

fn runRetention(ctx: *commander.Context) commander.Error!void {
    const measurement = ctx.getPositional("measurement").?;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const raw_ttl_str = ctx.getString("raw-ttl") orelse "";
    const downsample_str = ctx.getString("downsample") orelse "";
    const show = ctx.getBool("show");

    if (show) {
        // Show current policy (use ts_list with retention info)
        ctx.print("Retention policy for '{s}': (not yet implemented)\n", .{measurement});
        return;
    }

    const raw_ttl: ?[]const u8 = if (raw_ttl_str.len > 0) raw_ttl_str else null;

    // Parse downsample rules (single for now, multi-flag support later)
    var rules: [4][]const u8 = undefined;
    var rule_count: usize = 0;
    if (downsample_str.len > 0) {
        rules[0] = downsample_str;
        rule_count = 1;
    }

    if (raw_ttl == null and rule_count == 0) {
        ctx.printErr("Error: specify --raw-ttl and/or --downsample rules\n", .{});
        return error.CommandFailed;
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.ts.retention(&client, namespace, measurement, raw_ttl, rules[0..rule_count]) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("OK\n", .{});
}

// ========================================================================
// FloQL
// ========================================================================

fn runFloql(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    const query_str = ctx.getPositional("query") orelse {
        ctx.printErr("Error: query argument is required\n", .{});
        return error.CommandFailed;
    };

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.ts.floql(&client, namespace, query_str) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    // Decode SeriesSet wire format and print
    const data = result.data;
    if (data.len < 4) {
        ctx.print("(empty result)\n", .{});
        return;
    }

    var reader = WireReader.init(data);
    const series_count = reader.readU32() orelse 0;

    if (series_count == 0) {
        ctx.print("(no series)\n", .{});
        return;
    }

    var si: u32 = 0;
    while (si < series_count) : (si += 1) {
        const key = reader.readLengthPrefixed(u32) orelse break;
        const field = reader.readLengthPrefixed(u32) orelse break;
        const point_count = reader.readU32() orelse break;

        ctx.print("--- {s} ({s}) [{d} points] ---\n", .{ key, field, point_count });

        var pi: u32 = 0;
        while (pi < point_count) : (pi += 1) {
            const ts = reader.readI64() orelse break;
            const val_bytes = reader.readBytes(8) orelse break;
            const value: f64 = @bitCast(std.mem.readInt(u64, val_bytes, .little));
            ctx.print("  {d}: {d:.4}\n", .{ ts, value });
        }
    }
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
