//! KV (Key-Value) commands for Flo CLI using Commander framework
//!
//! Every command is a KV table op: its flags are the op's fields, and its
//! answer decodes by the op's response schema.

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const outcome = @import("../outcome.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const output = @import("../output.zig");
const wire = @import("../../util/wire.zig");
const cli_config = @import("../config.zig");
const time_units = @import("../../util/time_units.zig");
const kv_ops = @import("../../protocol/kv_ops.zig");
const schema = @import("../../protocol/schema.zig");
const WireReader = wire.WireReader;

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the kv command tree
pub fn createKvCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("kv")
        .about("Key-Value store operations")
        .group("Data Commands")
        .longAbout(
            \\Interact with Flo's key-value store.
            \\
            \\Provides commands for getting, setting, deleting, and listing
            \\key-value pairs with optional TTL and conditional operations.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("get")
                .about("Get a value by key")
                .examples(&.{
                    "flo kv get mykey",
                    "flo kv get mykey --output json",
                    "flo kv get mykey --wait-ms 5s",
                    "flo kv get mykey --wait-ms 5s --wait-for change",
                    "flo kv get user:123 --namespace users",
                    "flo kv get balance:alice --routing-key user:123",
                })
                .arg("key", "Key to retrieve")
                .stringFlag("wait-ms", 'w', "", "How long to wait: milliseconds, or a number and a unit (500ms, 30s, 5m); at most 5m; 0 or omitted doesn't wait")
                .stringFlag("wait-for", 0, "exists", "What to wait for: exists (answers at once if the key exists) or change (the next version). On timeout, a change wait prints the unchanged value")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location (same as {tag} in key)")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .action(wrapHandler(runGet)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("mget")
                .about("Get many keys in one request")
                .examples(&.{
                    "flo kv mget key1 key2 key3",
                    "flo kv mget user:1 user:2 user:3 --output json",
                })
                .variadicArg("keys", "Keys to retrieve (space-separated)")
                .action(wrapHandler(runMget)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("set")
                .about("Set a key-value pair")
                .examples(&.{
                    "flo kv set mykey myvalue",
                    "flo kv set mykey myvalue --ttl-ms 1h",
                    "flo kv set lock:job myvalue --ttl-ms 500",
                    "flo kv set mykey myvalue --condition if_absent",
                    "flo kv set counter 0 --condition if_present",
                    "flo kv set mykey newvalue --cas-version 42",
                    "flo kv set balance:alice 500 --routing-key user:123",
                })
                .arg("key", "Key to set")
                .arg("value", "Value to store")
                .stringFlag("ttl-ms", 0, "", "Time to live: milliseconds, or a number and a unit (500ms, 30s, 5m, 1h, 1d); 0 or omitted for none")
                .stringFlag("condition", 0, "always", "always, if_absent (only if the key doesn't exist) or if_present (only if it does)")
                .uint64Flag("cas-version", 0, 0, "Only set if the key's version is this; 0 means the key doesn't exist")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location (same as {tag} in key)")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .action(wrapHandler(runSet)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("delete")
                .about("Delete a key; deleting one that doesn't exist succeeds")
                .aliases(&.{"del"})
                .arg("key", "Key to delete")
                .uint64Flag("cas-version", 0, 0, "Only delete if the key's version is this; 0 means it doesn't exist")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location (same as {tag} in key)")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .action(wrapHandler(runDelete)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List the names of keys")
                .aliases(&.{ "ls", "scan" })
                .examples(&.{
                    "flo kv list",
                    "flo kv list --prefix user:",
                    "flo kv list --limit 100",
                })
                .stringFlag("prefix", 'p', "", "Only keys starting with this")
                .uintFlag("limit", 'l', 100, "Most names to list")
                .action(wrapHandler(runList)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("history")
                .about("A key's versions, newest first")
                .aliases(&.{"hist"})
                .arg("key", "Key to show history for")
                .uintFlag("limit", 'l', 10, std.fmt.comptimePrint("Most versions to show, at most {d}", .{kv_ops.MAX_HISTORY}))
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .action(wrapHandler(runHistory)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("incr")
                .about("Add to a key's counter, starting from 0")
                .examples(&.{
                    "flo kv incr counter",
                    "flo kv incr counter --delta 5",
                    "flo kv incr counter --delta -1",
                })
                .arg("key", "Counter key")
                .int64Flag("delta", 'd', 1, "What to add (may be negative)")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .action(wrapHandler(runIncr)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("expire")
                .about("Set an existing key's TTL, or clear it with --ttl-ms 0")
                .examples(&.{
                    "flo kv expire session:abc --ttl-ms 1h",
                    "flo kv expire session:abc --ttl-ms 0  # clears the TTL",
                })
                .arg("key", "Key whose TTL to set")
                .stringFlag("ttl-ms", 0, "", "TTL from now: milliseconds, or a number and a unit (500ms, 30s, 5m, 1h, 1d); 0 clears it (required)")
                .uint64Flag("cas-version", 0, 0, "Only if the key's version is this")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .action(wrapHandler(runExpire)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("exists")
                .about("Whether a key exists (exit 0 = yes, 1 = no)")
                .arg("key", "Key to check")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .action(wrapHandler(runExists)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("jget")
                .alias("json-get")
                .about("Read the JSON at a path in a key's document")
                .examples(&.{
                    "flo kv jget user:123",
                    "flo kv jget user:123 --path '$.name'",
                    "flo kv jget user:123 --path '$.addresses[0].city'",
                })
                .arg("key", "Key holding the JSON document")
                .stringFlag("path", 'p', "$", "JSONPath expression")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .action(wrapHandler(runJsonGet)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("jset")
                .alias("json-set")
                .about("Set the JSON at a path in a key's document; $ on a missing key writes the whole document")
                .examples(&.{
                    "flo kv jset user:123 '{\"name\":\"alice\"}'",
                    "flo kv jset user:123 '\"bob\"' --path '$.name'",
                })
                .arg("key", "Key holding the JSON document")
                .arg("document", "The JSON to put at the path")
                .stringFlag("path", 'p', "$", "JSONPath expression")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .action(wrapHandler(runJsonSet)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("jdel")
                .alias("json-del")
                .about("Delete the JSON at a path; $ deletes the key")
                .arg("key", "Key holding the JSON document")
                .stringFlag("path", 'p', "$", "JSONPath expression")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .action(wrapHandler(runJsonDel)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("begin")
                .about("Begin a transaction pinned to --routing-key's shard")
                .examples(&.{
                    "flo kv begin --routing-key user:123",
                    "flo kv begin -r tenant-A",
                })
                .stringFlag("routing-key", 'r', "", "The routing key every op in the transaction gives (required)")
                .action(wrapHandler(runTxnBegin)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("commit")
                .about("Commit a transaction's writes as one")
                .examples(&.{
                    "flo kv commit 281474976710657",
                })
                .arg("txn-id", "Transaction ID returned by `kv begin`")
                .action(wrapHandler(runTxnCommit)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("rollback")
                .about("Drop a transaction and its writes")
                .arg("txn-id", "Transaction ID returned by `kv begin`")
                .action(wrapHandler(runTxnRollback)),
        )
        .build();
}

fn runGet(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?; // validated by commander
    const namespace = cli_config.getNamespace(ctx);
    const wait_for = try enumFlag(ctx, kv_ops.WaitFor, "wait-for");
    const body: client_mod.kv.get_op.request.Value = .{
        .wait_ms = (try durationMsFlag(ctx, "wait-ms")) orelse 0,
        .wait_for = wait_for,
        .routing_key = optionalRoutingKey(ctx),
        .txn = optionalTxnId(ctx),
    };
    if (body.wait_ms > kv_ops.MAX_WAIT_MS) return outcome.usage(ctx, "--wait-ms is at most {d} (5m)", .{kv_ops.MAX_WAIT_MS});
    const format = output.getFormat(ctx);

    var client = try dialClient(ctx);
    defer client.deinit();
    if (output.isVerbose(ctx)) ctx.printErr("[verbose] GET key={s} namespace={s} endpoint={s}\n", .{ key, namespace, client.endpoint });

    var result = client_mod.kv.get(&client, namespace, key, body) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound() and result.reason() == .not_found) {
        try outputKvResult(ctx, format, key, null, null);
        return error.NotFound;
    }
    try outcome.check(ctx, result);
    var diag: schema.Diagnostic = .{};
    const answer = client_mod.kv.get_op.response.decodeAll(result.data, &diag) catch return outcome.malformed(ctx, "get response");
    try outputKvResult(ctx, format, key, answer.value, answer.version);
}

fn runSet(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?; // validated by commander
    const value = ctx.getPositional("value").?; // validated by commander
    const namespace = cli_config.getNamespace(ctx);
    const ttl_ms = try durationMsFlag(ctx, "ttl-ms");
    const body: client_mod.kv.set_op.request.Value = .{
        .value = value,
        .ttl_ms = if (ttl_ms) |t| if (t > 0) t else null else null,
        .condition = try enumFlag(ctx, kv_ops.Condition, "condition"),
        .cas_version = ctx.getChangedUint64("cas-version"),
        .routing_key = optionalRoutingKey(ctx),
        .txn = optionalTxnId(ctx),
    };

    if (value.len > kv_ops.MAX_VALUE) return outcome.usage(ctx, "the value is {d} bytes; at most {d}", .{ value.len, kv_ops.MAX_VALUE });
    if ((body.ttl_ms orelse 0) > kv_ops.MAX_TTL_MS) return outcome.usage(ctx, "--ttl-ms is at most {d}", .{kv_ops.MAX_TTL_MS});

    var client = try dialClient(ctx);
    defer client.deinit();
    if (output.isVerbose(ctx)) ctx.printErr("[verbose] SET key={s} namespace={s} endpoint={s}\n", .{ key, namespace, client.endpoint });

    var result = client_mod.kv.set(&client, namespace, key, body) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();
    try outcome.check(ctx, result);
    var diag: schema.Diagnostic = .{};
    const answer = client_mod.kv.set_op.response.decodeAll(result.data, &diag) catch return outcome.malformed(ctx, "set response");
    if (answer.version) |v| ctx.print("OK version={d}\n", .{v}) else ctx.print("OK\n", .{});
}

/// A `*-ms` flag: milliseconds, or a number and a unit; null when not
/// given. The flag's name says the unit, so a bare number is read as it.
fn durationMsFlag(ctx: *commander.Context, comptime name: []const u8) commander.Error!?u64 {
    const text = ctx.getString(name) orelse return null;
    if (text.len == 0) return null;
    if (std.fmt.parseInt(u64, text, 10)) |ms| return ms else |_| {}
    return time_units.parseDurationMs(text) orelse
        outcome.usage(ctx, "--" ++ name ++ " {s} is not a duration; give milliseconds, or a number and a unit (500ms, 30s, 5m, 1h, 1d)", .{text});
}

/// A flag naming one value of `E`.
fn enumFlag(ctx: *commander.Context, comptime E: type, comptime name: []const u8) commander.Error!E {
    const text = ctx.getString(name) orelse "";
    return std.meta.stringToEnum(E, text) orelse
        outcome.usage(ctx, "--" ++ name ++ " {s} is not one of: " ++ comptime enumNames(E), .{text});
}

fn enumNames(comptime E: type) []const u8 {
    var out: []const u8 = "";
    for (std.meta.fieldNames(E), 0..) |n, i| out = out ++ (if (i == 0) "" else ", ") ++ n;
    return out;
}

/// An ok answer to KV op `name`, decoded; its slices point into `resp`.
fn Answer(comptime name: []const u8) type {
    return struct {
        resp: client_mod.Response,
        value: kv_ops.op(name).response.Value,

        fn deinit(self: *@This()) void {
            self.resp.deinit();
        }
    };
}

/// Sends KV op `name` and decodes its answer; a refusal is reported and
/// becomes the command's outcome.
fn send(ctx: *commander.Context, client: *Client, comptime name: []const u8, namespace: []const u8, key: []const u8, body: kv_ops.op(name).request.Value) commander.Error!Answer(name) {
    var resp = client_mod.kv.call(client, name, namespace, key, body) catch |err| return outcome.requestFailed(ctx, err);
    errdefer resp.deinit();
    try outcome.check(ctx, resp);
    var diag: schema.Diagnostic = .{};
    const value = kv_ops.op(name).response.decodeAll(resp.data, &diag) catch return outcome.malformed(ctx, name ++ " answer");
    return .{ .resp = resp, .value = value };
}

fn runDelete(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?; // validated by commander
    var client = try dialClient(ctx);
    defer client.deinit();
    var a = try send(ctx, &client, "kv.delete", cli_config.getNamespace(ctx), key, .{
        .cas_version = ctx.getChangedUint64("cas-version"),
        .routing_key = optionalRoutingKey(ctx),
        .txn = optionalTxnId(ctx),
    });
    defer a.deinit();
    if (a.value.existed) |existed| ctx.print("OK existed={}\n", .{existed}) else ctx.print("OK\n", .{});
}

fn runList(ctx: *commander.Context) commander.Error!void {
    const prefix = ctx.getString("prefix") orelse "";
    const limit: usize = ctx.getUint("limit") orelse 100;
    const namespace = cli_config.getNamespace(ctx);
    var client = try dialClient(ctx);
    defer client.deinit();

    var listed: usize = 0;
    var cursor_buf: [kv_ops.MAX_CURSOR]u8 = undefined;
    var cursor: []const u8 = "";
    while (listed < limit) {
        const page: u32 = @intCast(@min(limit - listed, kv_ops.MAX_SCAN));
        var a = try send(ctx, &client, "kv.scan", namespace, prefix, .{ .limit = page, .cursor = cursor });
        defer a.deinit();
        var it = a.value.names.iterator();
        while (it.next() catch return outcome.malformed(ctx, "kv.scan answer")) |name| {
            if (listed == limit) break;
            ctx.print("{s}\n", .{name});
            listed += 1;
        }
        const next = a.value.cursor orelse break;
        @memcpy(cursor_buf[0..next.len], next);
        cursor = cursor_buf[0..next.len];
    }
    if (listed == 0) ctx.print("(no keys)\n", .{});
}

fn runHistory(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?; // validated by commander
    const limit = ctx.getUint("limit") orelse 10;
    if (limit == 0 or limit > kv_ops.MAX_HISTORY) return outcome.usage(ctx, "--limit is 1 to {d}", .{kv_ops.MAX_HISTORY});
    var client = try dialClient(ctx);
    defer client.deinit();
    var a = try send(ctx, &client, "kv.history", cli_config.getNamespace(ctx), key, .{ .limit = limit, .routing_key = optionalRoutingKey(ctx) });
    defer a.deinit();
    const format = output.getFormat(ctx);
    if (format == .json) ctx.print("[", .{});
    if (format == .table) ctx.print("VERSION\tAT_MS\tVALUE\n", .{});
    var it = a.value.versions.iterator();
    var i: usize = 0;
    while (it.next() catch return outcome.malformed(ctx, "kv.history answer")) |v| : (i += 1) switch (format) {
        .json => {
            if (i > 0) ctx.print(",", .{});
            const Version = struct { version: u64, at_ms: i64, value: []const u8 };
            output.Json.printInline(ctx, ctx.allocator, Version{ .version = v.version, .at_ms = v.at_ms, .value = v.value });
        },
        .table, .raw => ctx.print("{d}\t{d}\t{s}\n", .{ v.version, v.at_ms, v.value }),
    };
    if (format == .json) ctx.print("]\n", .{});
}

fn runMget(ctx: *commander.Context) commander.Error!void {
    const keys = ctx.getVariadicArgs("keys") orelse &.{};
    if (keys.len == 0) return outcome.usage(ctx, "at least one key is required", .{});
    if (keys.len > kv_ops.MAX_MGET) return outcome.usage(ctx, "at most {d} keys per request", .{kv_ops.MAX_MGET});
    const namespace = cli_config.getNamespace(ctx);
    const format = output.getFormat(ctx);
    var client = try dialClient(ctx);
    defer client.deinit();

    const Keys = kv_ops.op("kv.mget").request.field_list[0].T;
    const buf = try ctx.allocator.alloc(u8, Keys.max_size);
    defer ctx.allocator.free(buf);
    if (format == .json) ctx.print("[", .{});
    if (format == .table) ctx.print("{s:<30} {s:<40} {s:>10}\n{s:-<30} {s:-<40} {s:-<10}\n", .{ "KEY", "VALUE", "VERSION", "", "", "" });
    // The server answers as many as fit one answer, and says where to go on.
    var from: usize = 0;
    while (from < keys.len) {
        var list = Keys.Builder.init(buf);
        for (keys[from..]) |k| if (!(list.add(k) catch return outcome.usage(ctx, "a key is longer than {d} bytes", .{kv_ops.op("kv.mget").request.field_list[0].T.Element.bound}))) break;
        var a = try send(ctx, &client, "kv.mget", namespace, "", .{ .keys = list.value() });
        defer a.deinit();
        var it = a.value.entries.iterator();
        var i = from;
        while (it.next() catch return outcome.malformed(ctx, "kv.mget answer")) |e| : (i += 1) {
            // More entries than keys sent would name keys never asked for.
            if (i - from == list.count) return outcome.malformed(ctx, "kv.mget answer");
            const value: ?[]const u8 = if (e.found) e.value else null;
            switch (format) {
                .json => {
                    if (i > 0) ctx.print(",", .{});
                    const Entry = struct { key: []const u8, value: ?[]const u8, version: ?u64 };
                    output.Json.printInline(ctx, ctx.allocator, Entry{ .key = keys[i], .value = value, .version = if (e.found) e.version else null });
                },
                .table => {
                    var ver_buf: [32]u8 = undefined;
                    const ver = if (e.found) std.fmt.bufPrint(&ver_buf, "{d}", .{e.version}) catch "" else "";
                    ctx.print("{s:<30} {s:<40} {s:>10}\n", .{ keys[i], value orelse "(nil)", ver });
                },
                .raw => ctx.print("{s}\t{s}\n", .{ keys[i], value orelse "(nil)" }),
            }
        }
        // A request holds as many keys as fit it; the answer, as many of
        // those as fit an answer. Go on from the first key either left out.
        const answered = a.value.next orelse list.count;
        if (answered == 0 or answered > list.count or i - from != answered) return outcome.malformed(ctx, "kv.mget answer");
        from += answered;
    }
    if (format == .json) ctx.print("]\n", .{});
}

fn outputKvResult(ctx: *commander.Context, format: output.Format, key: []const u8, value: ?[]const u8, version: ?u64) commander.Error!void {
    switch (format) {
        .json => {
            // Use the output.Json helper for clean JSON
            const KvResult = struct {
                key: []const u8,
                value: ?[]const u8,
                version: ?u64 = null,
            };
            const result = KvResult{ .key = key, .value = value, .version = version };
            output.Json.printCompact(ctx, ctx.allocator, result);
        },
        .table => {
            // Use the output.Table helper
            var table = output.Table.init(ctx.allocator);
            defer table.deinit();
            try table.addColumn("KEY", .left);
            try table.addColumn("VALUE", .left);
            if (version != null) {
                try table.addColumn("VERSION", .right);
            }

            var ver_buf: [32]u8 = undefined;
            const ver_str = if (version) |v| std.fmt.bufPrint(&ver_buf, "{d}", .{v}) catch "" else "";

            // The column count matches by construction, so only allocation can fail.
            if (version != null) {
                table.addRow(&.{ key, value orelse "(nil)", ver_str }) catch return error.OutOfMemory;
            } else {
                table.addRow(&.{ key, value orelse "(nil)" }) catch return error.OutOfMemory;
            }
            table.print(ctx);
        },
        .raw => {
            if (value) |v| {
                ctx.print("{s}\n", .{v});
            } else {
                ctx.print("(nil)\n", .{});
            }
        },
    }
}

// ==================== Helpers ====================

fn dialClient(ctx: *commander.Context) commander.Error!Client {
    const endpoint = cli_config.getEndpoint(ctx);
    var client = Client.init(ctx.allocator, endpoint);
    client.connect() catch |err| {
        defer client.deinit();
        return outcome.connectFailed(ctx, err, endpoint);
    };
    return client;
}

fn optionalRoutingKey(ctx: *commander.Context) ?[]const u8 {
    const rk = ctx.getString("routing-key") orelse return null;
    return if (rk.len > 0) rk else null;
}

/// Read the `--txn` flag if the command defined it. Returns null when the
/// flag is absent or zero (zero is reserved as "not in a transaction").
fn optionalTxnId(ctx: *commander.Context) ?u64 {
    const v = ctx.getChangedUint64("txn") orelse return null;
    return if (v == 0) null else v;
}

fn runIncr(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    var client = try dialClient(ctx);
    defer client.deinit();
    var a = try send(ctx, &client, "kv.incr", cli_config.getNamespace(ctx), key, .{
        .delta = ctx.getInt64("delta") orelse 1,
        .routing_key = optionalRoutingKey(ctx),
        .txn = optionalTxnId(ctx),
    });
    defer a.deinit();
    // Inside a transaction the sum isn't known until commit.
    if (a.value.value) |v| ctx.print("{d}\n", .{v}) else ctx.print("OK\n", .{});
}

fn runExpire(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    const ttl_ms = (try durationMsFlag(ctx, "ttl-ms")) orelse return outcome.usage(ctx, "--ttl-ms is required; 0 clears the TTL", .{});
    if (ttl_ms > kv_ops.MAX_TTL_MS) return outcome.usage(ctx, "--ttl-ms is at most {d}", .{kv_ops.MAX_TTL_MS});
    var client = try dialClient(ctx);
    defer client.deinit();
    var a = try send(ctx, &client, "kv.expire", cli_config.getNamespace(ctx), key, .{
        .ttl_ms = ttl_ms,
        .cas_version = ctx.getChangedUint64("cas-version"),
        .routing_key = optionalRoutingKey(ctx),
        .txn = optionalTxnId(ctx),
    });
    defer a.deinit();
    ctx.print("OK\n", .{});
}

fn runExists(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    var client = try dialClient(ctx);
    defer client.deinit();
    var a = try send(ctx, &client, "kv.exists", cli_config.getNamespace(ctx), key, .{
        .routing_key = optionalRoutingKey(ctx),
        .txn = optionalTxnId(ctx),
    });
    defer a.deinit();
    if (!a.value.exists) {
        ctx.print("0\n", .{});
        return error.NotFound; // exit 1, for shell `if`
    }
    // A write buffered in a transaction has no version until it commits.
    if (a.value.version == 0) ctx.print("1\n", .{}) else ctx.print("1 version={d}\n", .{a.value.version});
}

fn runJsonGet(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    var client = try dialClient(ctx);
    defer client.deinit();
    var a = try send(ctx, &client, "kv.json_get", cli_config.getNamespace(ctx), key, .{
        .path = ctx.getString("path") orelse "$",
        .routing_key = optionalRoutingKey(ctx),
    });
    defer a.deinit();
    ctx.print("{s}\n", .{a.value.document});
}

fn runJsonSet(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    const document = ctx.getPositional("document").?;
    if (document.len > kv_ops.MAX_DOCUMENT) return outcome.usage(ctx, "the document is {d} bytes; at most {d}", .{ document.len, kv_ops.MAX_DOCUMENT });
    var client = try dialClient(ctx);
    defer client.deinit();
    var a = try send(ctx, &client, "kv.json_set", cli_config.getNamespace(ctx), key, .{
        .path = ctx.getString("path") orelse "$",
        .document = document,
        .routing_key = optionalRoutingKey(ctx),
    });
    defer a.deinit();
    ctx.print("OK version={d}\n", .{a.value.version});
}

fn runJsonDel(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    var client = try dialClient(ctx);
    defer client.deinit();
    var a = try send(ctx, &client, "kv.json_del", cli_config.getNamespace(ctx), key, .{
        .path = ctx.getString("path") orelse "$",
        .routing_key = optionalRoutingKey(ctx),
    });
    defer a.deinit();
    // Null when the path was the whole document, which took the key.
    if (a.value.version) |v| ctx.print("OK version={d}\n", .{v}) else ctx.print("OK (key deleted)\n", .{});
}

fn runTxnBegin(ctx: *commander.Context) commander.Error!void {
    const routing_key = ctx.getString("routing-key") orelse "";
    if (routing_key.len == 0) return outcome.usage(ctx, "--routing-key is required to begin a transaction", .{});
    var client = try dialClient(ctx);
    defer client.deinit();
    // The routing key travels as the request's key.
    var a = try send(ctx, &client, "kv.txn_begin", cli_config.getNamespace(ctx), routing_key, .{});
    defer a.deinit();
    ctx.print("txn_id={d} pinned_hash={d}\n", .{ a.value.txn, a.value.pinned });
}

fn txnIdArg(ctx: *commander.Context) commander.Error!u64 {
    const text = ctx.getPositional("txn-id").?;
    return std.fmt.parseInt(u64, text, 10) catch outcome.usage(ctx, "invalid txn-id '{s}'", .{text});
}

fn runTxnCommit(ctx: *commander.Context) commander.Error!void {
    const txn = try txnIdArg(ctx);
    var client = try dialClient(ctx);
    defer client.deinit();
    var a = try send(ctx, &client, "kv.txn_commit", cli_config.getNamespace(ctx), "", .{ .txn = txn });
    defer a.deinit();
    ctx.print("OK ops={d} commit_index={d}\n", .{ a.value.ops, a.value.commit_index });
}

fn runTxnRollback(ctx: *commander.Context) commander.Error!void {
    const txn = try txnIdArg(ctx);
    var client = try dialClient(ctx);
    defer client.deinit();
    var a = try send(ctx, &client, "kv.txn_rollback", cli_config.getNamespace(ctx), "", .{ .txn = txn });
    defer a.deinit();
    ctx.print("OK\n", .{});
}

// ==================== Testing ====================

test "create kv command" {
    const allocator = std.testing.allocator;

    const cmd = try createKvCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("kv", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 4);
}
