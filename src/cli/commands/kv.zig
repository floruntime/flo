//! KV (Key-Value) commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo kv get <key> [--wait <ms>] [--block <ms>] [--output json|table|raw]
//!   flo kv set <key> <value> [--ttl <duration>] [--nx] [--xx]
//!   flo kv delete <key>
//!   flo kv list [--prefix <prefix>]

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
                    "flo kv get mykey --wait 5000",
                    "flo kv get mykey --block 5000",
                    "flo kv get user:123 --namespace users",
                    "flo kv get balance:alice --routing-key user:123",
                })
                .arg("key", "Key to retrieve")
                .uintFlag("wait", 'w', 0, "Wait until key exists (ms, at most 300000; 0 = don't wait)")
                .uintFlag("block", 'b', 0, "Block for changes (ms, at most 300000; 0 = don't wait). On timeout, prints the unchanged value")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location (same as {tag} in key)")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .action(wrapHandler(runGet)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("mget")
                .about("Get multiple keys in one request")
                .examples(&.{
                    "flo kv mget key1 key2 key3",
                    "flo kv mget user:1 user:2 user:3 --output json",
                    "flo kv mget key1 key2 --namespace myns",
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
                    "flo kv set mykey myvalue --ttl 1h",
                    "flo kv set lock:job myvalue --ttl 500ms",
                    "flo kv set mykey myvalue --nx",
                    "flo kv set counter 0 --xx",
                    "flo kv set mykey newvalue --cas 42",
                    "flo kv set balance:alice 500 --routing-key user:123",
                })
                .arg("key", "Key to set")
                .arg("value", "Value to store")
                .stringFlag("ttl", 0, "", "Time to live: a number and a unit (500ms, 30s, 5m, 1h, 1d); omit for none")
                .boolFlag("nx", 0, "Only set if key does NOT exist")
                .boolFlag("xx", 0, "Only set if key DOES exist")
                .uint64Flag("cas", 0, 0, "Compare-and-swap version (only set if current version matches)")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location (same as {tag} in key)")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .action(wrapHandler(runSet)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("delete")
                .about("Delete a key")
                .aliases(&.{"del"})
                .arg("key", "Key to delete")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location (same as {tag} in key)")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .uint64Flag("cas", 0, 0, "Compare-and-swap version (only delete if current version matches)")
                .action(wrapHandler(runDelete)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List keys")
                .aliases(&.{ "ls", "scan" })
                .examples(&.{
                    "flo kv list",
                    "flo kv list --prefix user:",
                    "flo kv list --limit 100",
                })
                .stringFlag("prefix", 'p', "", "Filter by key prefix")
                .uintFlag("limit", 'l', 100, "Maximum keys to return")
                .action(wrapHandler(runList)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("history")
                .about("Show key history")
                .aliases(&.{"hist"})
                .arg("key", "Key to show history for")
                .uintFlag("limit", 'l', 10, "Maximum entries to show")
                .action(wrapHandler(runHistory)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("incr")
                .about("Atomically increment a counter")
                .examples(&.{
                    "flo kv incr counter",
                    "flo kv incr counter --by 5",
                    "flo kv incr counter --by -1",
                })
                .arg("key", "Counter key")
                .int64Flag("by", 'b', 1, "Delta to add (may be negative)")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .action(wrapHandler(runIncr)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("touch")
                .about("Update the TTL of an existing key")
                .examples(&.{
                    "flo kv touch session:abc --ttl 1h",
                    "flo kv touch session:abc --ttl 0  # clears the TTL",
                })
                .arg("key", "Key whose TTL to update")
                .stringFlag("ttl", 0, "", "New TTL: a number and a unit (500ms, 30s, 5m, 1h, 1d); 0 or omitted clears")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .uint64Flag("cas", 0, 0, "Compare-and-swap version (only touch if current version matches)")
                .action(wrapHandler(runTouch)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("persist")
                .about("Remove the TTL on an existing key")
                .arg("key", "Key whose TTL to clear")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .uint64Flag("cas", 0, 0, "Compare-and-swap version (only persist if current version matches)")
                .action(wrapHandler(runPersist)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("exists")
                .about("Check whether a key exists (exit 0 = yes, 1 = no)")
                .arg("key", "Key to check")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .uint64Flag("txn", 't', 0, "Transaction ID (omit for non-txn ops)")
                .action(wrapHandler(runExists)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("jget")
                .alias("json-get")
                .about("Extract a JSONPath subtree from a JSON-encoded value")
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
                .about("Set a JSON value at the given path (read-modify-write)")
                .examples(&.{
                    "flo kv jset user:123 '{\"name\":\"alice\"}'",
                    "flo kv jset user:123 '\"bob\"' --path '$.name'",
                })
                .arg("key", "Key holding the JSON document")
                .arg("value", "Replacement value (must itself be valid JSON)")
                .stringFlag("path", 'p', "$", "JSONPath expression")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .action(wrapHandler(runJsonSet)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("jdel")
                .alias("json-del")
                .about("Delete a JSON path. Path '$' deletes the whole key.")
                .arg("key", "Key holding the JSON document")
                .stringFlag("path", 'p', "$", "JSONPath expression")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location")
                .action(wrapHandler(runJsonDel)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("begin")
                .about("Open a per-shard transaction pinned to --routing-key")
                .examples(&.{
                    "flo kv begin --routing-key user:123",
                    "flo kv begin -r tenant-A",
                })
                .stringFlag("routing-key", 'r', "", "Routing key the txn pins to (required)")
                .action(wrapHandler(runTxnBegin)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("commit")
                .about("Commit a per-shard transaction (atomic batch apply)")
                .examples(&.{
                    "flo kv commit 42 --routing-key user:123",
                })
                .arg("txn-id", "Transaction ID returned by `kv begin`")
                .stringFlag("routing-key", 'r', "", "Routing key used at BEGIN (required)")
                .action(wrapHandler(runTxnCommit)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("rollback")
                .about("Discard a per-shard transaction's buffered ops")
                .arg("txn-id", "Transaction ID returned by `kv begin`")
                .stringFlag("routing-key", 'r', "", "Routing key used at BEGIN (required)")
                .action(wrapHandler(runTxnRollback)),
        )
        .build();
}

fn runGet(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?; // validated by commander

    // --wait: Wait until key exists (returns immediately if present)
    // --block: Block for changes (waits for NEXT version even if key exists) - like stream/queue --block
    const wait_ms = ctx.getChangedUint("wait");
    const block_ms = ctx.getChangedUint("block");
    const namespace = cli_config.getNamespace(ctx);

    // --routing-key: explicit shard co-location (same routing as {tag} in key name)
    const routing_key: ?[]const u8 = blk: {
        const rk = ctx.getString("routing-key") orelse break :blk null;
        break :blk if (rk.len > 0) rk else null;
    };
    const txn_id = optionalTxnId(ctx);

    // Cannot use both --wait and --block
    if (wait_ms != null and block_ms != null) {
        ctx.printErr("Error: Cannot use both --wait and --block\n", .{});
        ctx.printErr("  --wait: returns immediately if key exists, else waits for creation\n", .{});
        ctx.printErr("  --block: blocks for changes (waits for NEXT version even if key exists)\n", .{});
        return error.Usage;
    }

    const format = output.getFormat(ctx);

    const endpoint = cli_config.getEndpoint(ctx);

    if (output.isVerbose(ctx)) {
        ctx.printErr("[verbose] GET key={s} namespace={s} endpoint={s}\n", .{ key, namespace, endpoint });
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.kv.get(&client, namespace, key, wait_ms, block_ms, routing_key, txn_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound()) {
        try outputKvResult(ctx, format, key, null, null);
        return error.NotFound;
    }

    try outcome.check(ctx, result);

    // [version:u64][value]; an empty value is still 8 bytes.
    if (result.data.len < 8) return outcome.malformed(ctx, "get response");
    try outputKvResult(ctx, format, key, result.data[8..], result.getVersion());
}

fn runSet(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?; // validated by commander
    const value = ctx.getPositional("value").?; // validated by commander

    const ttl = try ttlFlag(ctx);
    const nx = ctx.getBool("nx");
    const xx = ctx.getBool("xx");
    const cas = ctx.getChangedUint64("cas");
    const namespace = cli_config.getNamespace(ctx);

    // --routing-key: explicit shard co-location (same routing as {tag} in key name)
    const routing_key: ?[]const u8 = blk: {
        const rk = ctx.getString("routing-key") orelse break :blk null;
        break :blk if (rk.len > 0) rk else null;
    };

    if (nx and xx) {
        return outcome.usage(ctx, "Cannot use both --nx and --xx", .{});
    }

    if (cas != null and nx) {
        return outcome.usage(ctx, "Cannot use --cas with --nx", .{});
    }

    const endpoint = cli_config.getEndpoint(ctx);

    if (output.isVerbose(ctx)) {
        ctx.printErr("[verbose] SET key={s} namespace={s} endpoint={s}\n", .{ key, namespace, endpoint });
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    const ttl_val: ?u64 = if (ttl) |t| if (t > 0) t else null else null;

    var result = client_mod.kv.set(&client, namespace, key, value, .{
        .ttl_ms = ttl_val,
        .if_not_exists = nx,
        .if_exists = xx,
        .cas_version = cas,
        .routing_key = routing_key,
        .txn_id = optionalTxnId(ctx),
    }) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isConflict()) {
        if (cas != null) {
            ctx.printErr("Version mismatch\n", .{});
        } else {
            ctx.printErr("Condition not met (key {s})\n", .{if (nx) "already exists" else "does not exist"});
        }
        return error.Refused;
    }

    try outcome.check(ctx, result);

    ctx.print("OK\n", .{});
}

fn runDelete(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?; // validated by commander

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const cas = ctx.getChangedUint64("cas");

    // --routing-key: explicit shard co-location (same routing as {tag} in key name)
    const routing_key: ?[]const u8 = blk: {
        const rk = ctx.getString("routing-key") orelse break :blk null;
        break :blk if (rk.len > 0) rk else null;
    };
    const txn_id = optionalTxnId(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.kv.delete(&client, namespace, key, routing_key, txn_id, cas) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (cas != null and result.isConflict()) {
        ctx.printErr("Version mismatch\n", .{});
        return error.Refused;
    }

    try outcome.check(ctx, result);

    ctx.print("OK\n", .{});
}

fn runList(ctx: *commander.Context) commander.Error!void {
    const prefix = ctx.getString("prefix") orelse "";
    const limit = ctx.getUint("limit") orelse 100;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // Collect all keys across shards using cursor-based shard walking
    var all_keys: std.ArrayList([]const u8) = .empty;
    defer {
        for (all_keys.items) |key| {
            ctx.allocator.free(key);
        }
        all_keys.deinit(ctx.allocator);
    }

    var cursor: ?[]const u8 = null;
    var cursor_owned: ?[]u8 = null;
    defer if (cursor_owned) |c| ctx.allocator.free(c);

    // Walk all shards until no more data
    while (all_keys.items.len < limit) {
        var result = client_mod.kv.scan(&client, namespace, prefix, cursor, @intCast(limit)) catch |err| return outcome.requestFailed(ctx, err);
        defer result.deinit();

        try outcome.check(ctx, result);

        const data = result.asRawData() orelse return outcome.malformed(ctx, "scan response (empty)");
        const next = parseScanPage(ctx.allocator, data, &all_keys, limit) catch |err| switch (err) {
            error.Truncated => return outcome.malformed(ctx, "scan response"),
            error.OutOfMemory => return error.OutOfMemory,
        };

        if (cursor_owned) |c| ctx.allocator.free(c);
        cursor_owned = null;
        const next_cursor = next orelse break;
        cursor_owned = try ctx.allocator.dupe(u8, next_cursor);
        cursor = cursor_owned;
    }

    // Output results
    if (all_keys.items.len == 0) {
        ctx.print("(no keys)\n", .{});
    } else {
        for (all_keys.items) |key| {
            ctx.print("{s}\n", .{key});
        }
    }
}

fn runHistory(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?; // validated by commander

    const limit = ctx.getUint("limit") orelse 10;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.kv.history(&client, namespace, key, @intCast(limit)) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    ctx.print("History for key: {s}\n", .{key});
    // Output raw history data (no version prefix for history responses)
    if (result.asRawData()) |data| {
        if (data.len > 0) {
            ctx.print("{s}\n", .{data});
        }
    }
}

fn runMget(ctx: *commander.Context) commander.Error!void {
    const keys = ctx.getVariadicArgs("keys") orelse {
        return outcome.usage(ctx, "at least one key is required", .{});
    };

    if (keys.len == 0) {
        return outcome.usage(ctx, "at least one key is required", .{});
    }

    if (keys.len > 256) {
        return outcome.usage(ctx, "max 256 keys per request", .{});
    }

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);
    const format = output.getFormat(ctx);

    if (output.isVerbose(ctx)) {
        ctx.printErr("[verbose] MGET keys={d} namespace={s} endpoint={s}\n", .{ keys.len, namespace, endpoint });
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.kv.mget(&client, namespace, keys) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    const data = result.asRawData() orelse return outcome.malformed(ctx, "mget response (empty)");
    const entries = parseMget(ctx.allocator, data) catch |err| switch (err) {
        error.Truncated => return outcome.malformed(ctx, "mget response"),
        error.OutOfMemory => return error.OutOfMemory,
    };
    defer ctx.allocator.free(entries);

    switch (format) {
        .json => {
            ctx.print("[", .{});
            for (entries, 0..) |e, i| {
                if (i > 0) ctx.print(",", .{});
                if (e.found) {
                    ctx.print("{{\"key\":\"{s}\",\"value\":\"{s}\",\"version\":{d}}}", .{ e.key, e.value, e.version });
                } else {
                    ctx.print("{{\"key\":\"{s}\",\"value\":null}}", .{e.key});
                }
            }
            ctx.print("]\n", .{});
        },
        .table => {
            ctx.print("{s:<30} {s:<40} {s:>10}\n", .{ "KEY", "VALUE", "VERSION" });
            ctx.print("{s:-<30} {s:-<40} {s:-<10}\n", .{ "", "", "" });
            for (entries) |e| {
                if (e.found) {
                    var ver_buf: [32]u8 = undefined;
                    const ver_str = std.fmt.bufPrint(&ver_buf, "{d}", .{e.version}) catch "";
                    ctx.print("{s:<30} {s:<40} {s:>10}\n", .{ e.key, e.value, ver_str });
                } else {
                    ctx.print("{s:<30} {s:<40} {s:>10}\n", .{ e.key, "(nil)", "" });
                }
            }
        },
        .raw => {
            for (entries) |e| {
                if (e.found) {
                    ctx.print("{s}\t{s}\n", .{ e.key, e.value });
                } else {
                    ctx.print("{s}\t(nil)\n", .{e.key});
                }
            }
        },
    }
}

/// One page of a scan: `[count:u32]([key_len:u16][key][value_len:u32][value])*
/// [has_more:u8][cursor_len:u16][cursor]`. Appends copies of its keys to
/// `keys` until it holds `limit`, and returns the cursor of the next page,
/// or null when this is the last.
fn parseScanPage(
    allocator: Allocator,
    data: []const u8,
    keys: *std.ArrayList([]const u8),
    limit: usize,
) error{ Truncated, OutOfMemory }!?[]const u8 {
    var reader = WireReader.init(data);
    const count = reader.readU32() orelse return error.Truncated;
    for (0..count) |_| {
        const key = reader.readLengthPrefixed(u16) orelse return error.Truncated;
        _ = reader.readLengthPrefixed(u32) orelse return error.Truncated; // value
        if (keys.items.len >= limit) continue;
        const owned = try allocator.dupe(u8, key);
        errdefer allocator.free(owned);
        try keys.append(allocator, owned);
    }
    const has_more = (reader.readU8() orelse return error.Truncated) != 0;
    const next = reader.readLengthPrefixed(u16) orelse return error.Truncated;
    return if (has_more and next.len > 0) next else null;
}

const MgetEntry = struct {
    found: bool,
    key: []const u8,
    value: []const u8,
    version: u64,
};

/// `[count:u32]([status:u8][key_len:u16][key][version:u64][value_len:u32][value])*`;
/// status 0 is found. The entries' slices point into `data`.
fn parseMget(allocator: Allocator, data: []const u8) error{ Truncated, OutOfMemory }![]MgetEntry {
    var reader = WireReader.init(data);
    const count = reader.readU32() orelse return error.Truncated;
    var entries: std.ArrayList(MgetEntry) = .empty;
    errdefer entries.deinit(allocator);
    for (0..count) |_| {
        const status = reader.readU8() orelse return error.Truncated;
        const key = reader.readLengthPrefixed(u16) orelse return error.Truncated;
        const version = reader.readU64() orelse return error.Truncated;
        const value = reader.readLengthPrefixed(u32) orelse return error.Truncated;
        try entries.append(allocator, .{ .found = status == 0, .key = key, .value = value, .version = version });
    }
    return entries.toOwnedSlice(allocator);
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

// ==================== Extended KV Operations ====================

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
    const delta: i64 = ctx.getInt64("by") orelse 1;
    const namespace = cli_config.getNamespace(ctx);
    const routing_key = optionalRoutingKey(ctx);
    const txn_id = optionalTxnId(ctx);

    var client = try dialClient(ctx);
    defer client.deinit();

    var result = client_mod.kv.incr(&client, namespace, key, delta, routing_key, txn_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    const value = result.asString() orelse {
        return outcome.malformed(ctx, "counter response (empty)");
    };
    if (value.len != 8) {
        return outcome.malformed(ctx, "counter response");
    }
    const counter = std.mem.readInt(i64, value[0..8], .little);
    ctx.print("{d}\n", .{counter});
}

/// `--ttl` in milliseconds, or null when not given. A bare number other
/// than 0 is refused: "3600" could mean seconds or milliseconds.
fn ttlFlag(ctx: *commander.Context) commander.Error!?u64 {
    const text = ctx.getString("ttl") orelse return null;
    if (text.len == 0) return null;
    return time_units.parseDurationMs(text) orelse {
        return outcome.usage(ctx, "--ttl {s} is not a duration; give a number and a unit (500ms, 30s, 5m, 1h, 1d), or 0", .{text});
    };
}

fn runTouch(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    const ttl: u64 = try ttlFlag(ctx) orelse 0;
    const namespace = cli_config.getNamespace(ctx);
    const routing_key = optionalRoutingKey(ctx);
    const txn_id = optionalTxnId(ctx);
    const cas = ctx.getChangedUint64("cas");

    var client = try dialClient(ctx);
    defer client.deinit();

    var result = client_mod.kv.touch(&client, namespace, key, ttl, routing_key, txn_id, cas) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (cas != null and result.isConflict()) {
        ctx.printErr("Version mismatch\n", .{});
        return error.Refused;
    }
    if (result.isNotFound()) {
        ctx.printErr("Key not found\n", .{});
        return error.NotFound;
    }
    try outcome.check(ctx, result);
    ctx.print("OK\n", .{});
}

fn runPersist(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    const namespace = cli_config.getNamespace(ctx);
    const routing_key = optionalRoutingKey(ctx);
    const txn_id = optionalTxnId(ctx);
    const cas = ctx.getChangedUint64("cas");

    var client = try dialClient(ctx);
    defer client.deinit();

    var result = client_mod.kv.persist(&client, namespace, key, routing_key, txn_id, cas) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (cas != null and result.isConflict()) {
        ctx.printErr("Version mismatch\n", .{});
        return error.Refused;
    }
    if (result.isNotFound()) {
        ctx.printErr("Key not found\n", .{});
        return error.NotFound;
    }
    try outcome.check(ctx, result);
    ctx.print("OK\n", .{});
}

fn runExists(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    const namespace = cli_config.getNamespace(ctx);
    const routing_key = optionalRoutingKey(ctx);
    const txn_id = optionalTxnId(ctx);

    var client = try dialClient(ctx);
    defer client.deinit();

    var result = client_mod.kv.exists(&client, namespace, key, routing_key, txn_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    // [version:u64][present:u8], present 0 or 1.
    const value = result.asString() orelse return outcome.malformed(ctx, "exists response (empty)");
    if (value.len != 1) return outcome.malformed(ctx, "exists response");
    const present = value[0] == 1;
    if (present) {
        ctx.print("1\n", .{});
    } else {
        ctx.print("0\n", .{});
        return error.NotFound; // exit 1, for shell `if`
    }
}

fn runJsonGet(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    const path = ctx.getString("path") orelse "$";
    const namespace = cli_config.getNamespace(ctx);
    const routing_key = optionalRoutingKey(ctx);

    var client = try dialClient(ctx);
    defer client.deinit();

    var result = client_mod.kv.jsonGet(&client, namespace, key, path, routing_key) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound()) {
        ctx.printErr("Not found\n", .{});
        return error.NotFound;
    }
    try outcome.check(ctx, result);
    // [version:u64][json]
    if (result.data.len < 8) return outcome.malformed(ctx, "json get response");
    ctx.print("{s}\n", .{result.data[8..]});
}

fn runJsonSet(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    const value = ctx.getPositional("value").?;
    const path = ctx.getString("path") orelse "$";
    const namespace = cli_config.getNamespace(ctx);
    const routing_key = optionalRoutingKey(ctx);

    var client = try dialClient(ctx);
    defer client.deinit();

    var result = client_mod.kv.jsonSet(&client, namespace, key, path, value, routing_key) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound()) {
        ctx.printErr("Path not found\n", .{});
        return error.NotFound;
    }
    try outcome.check(ctx, result);
    ctx.print("OK\n", .{});
}

fn runJsonDel(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?;
    const path = ctx.getString("path") orelse "$";
    const namespace = cli_config.getNamespace(ctx);
    const routing_key = optionalRoutingKey(ctx);

    var client = try dialClient(ctx);
    defer client.deinit();

    var result = client_mod.kv.jsonDel(&client, namespace, key, path, routing_key) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    if (result.isNotFound()) {
        ctx.printErr("Path not found\n", .{});
        return error.NotFound;
    }
    try outcome.check(ctx, result);
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

test "kv: a scan page parses, and a cut-short one is Truncated" {
    const a = std.testing.allocator;
    // Two keys, has_more=1, cursor "c1".
    const page = "\x02\x00\x00\x00" ++ "\x01\x00a" ++ "\x00\x00\x00\x00" ++
        "\x02\x00bb" ++ "\x01\x00\x00\x00v" ++ "\x01" ++ "\x02\x00c1";
    var keys: std.ArrayList([]const u8) = .empty;
    defer {
        for (keys.items) |k| a.free(k);
        keys.deinit(a);
    }
    try std.testing.expectEqualStrings("c1", (try parseScanPage(a, page, &keys, 100)).?);
    try std.testing.expectEqual(@as(usize, 2), keys.items.len);
    try std.testing.expectEqualStrings("bb", keys.items[1]);

    // A limit below the page still reads the trailer.
    for (keys.items) |k| a.free(k);
    keys.clearRetainingCapacity();
    try std.testing.expectEqualStrings("c1", (try parseScanPage(a, page, &keys, 1)).?);
    try std.testing.expectEqual(@as(usize, 1), keys.items.len);

    for (1..page.len) |n| {
        for (keys.items) |k| a.free(k);
        keys.clearRetainingCapacity();
        try std.testing.expectError(error.Truncated, parseScanPage(a, page[0..n], &keys, 100));
    }
}

test "kv: an mget answer parses, and a cut-short one is Truncated" {
    const a = std.testing.allocator;
    const answer = "\x02\x00\x00\x00" ++
        "\x00" ++ "\x01\x00k" ++ "\x07\x00\x00\x00\x00\x00\x00\x00" ++ "\x02\x00\x00\x00hi" ++
        "\x02" ++ "\x01\x00m" ++ "\x00" ** 8 ++ "\x00\x00\x00\x00";
    const entries = try parseMget(a, answer);
    defer a.free(entries);
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expect(entries[0].found);
    try std.testing.expectEqualStrings("hi", entries[0].value);
    try std.testing.expectEqual(@as(u64, 7), entries[0].version);
    try std.testing.expect(!entries[1].found);

    for (1..answer.len) |n| try std.testing.expectError(error.Truncated, parseMget(a, answer[0..n]));
}

// ── Per-Shard Transaction Subcommands ────────────────────────────────────

fn requireRoutingKey(ctx: *commander.Context) commander.Error![]const u8 {
    const rk = ctx.getString("routing-key") orelse "";
    if (rk.len == 0) {
        return outcome.usage(ctx, "--routing-key is required for transaction commands", .{});
    }
    return rk;
}

fn runTxnBegin(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const routing_key = try requireRoutingKey(ctx);

    var client = try dialClient(ctx);
    defer client.deinit();

    var result = client_mod.kv.beginTxn(&client, namespace, routing_key) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    const begin = result.getTxnBeginResult() orelse {
        return outcome.malformed(ctx, "begin response");
    };
    ctx.print("txn_id={d} pinned_hash={d}\n", .{ begin.txn_id, begin.pinned_hash });
}

fn runTxnCommit(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const routing_key = try requireRoutingKey(ctx);
    const txn_str = ctx.getPositional("txn-id").?;
    const txn_id = std.fmt.parseInt(u64, txn_str, 10) catch {
        return outcome.usage(ctx, "invalid txn-id '{s}'", .{txn_str});
    };

    var client = try dialClient(ctx);
    defer client.deinit();

    var result = client_mod.kv.commitTxn(&client, namespace, routing_key, txn_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    const commit = result.getTxnCommitResult() orelse {
        return outcome.malformed(ctx, "commit response");
    };
    ctx.print("OK ops={d} commit_index={d}\n", .{ commit.op_count, commit.commit_index });
}

fn runTxnRollback(ctx: *commander.Context) commander.Error!void {
    const namespace = cli_config.getNamespace(ctx);
    const routing_key = try requireRoutingKey(ctx);
    const txn_str = ctx.getPositional("txn-id").?;
    const txn_id = std.fmt.parseInt(u64, txn_str, 10) catch {
        return outcome.usage(ctx, "invalid txn-id '{s}'", .{txn_str});
    };

    var client = try dialClient(ctx);
    defer client.deinit();

    var result = client_mod.kv.rollbackTxn(&client, namespace, routing_key, txn_id) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);
    ctx.print("OK\n", .{});
}
