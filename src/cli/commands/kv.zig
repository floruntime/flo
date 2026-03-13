//! KV (Key-Value) commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo kv get <key> [--wait <ms>] [--block <ms>] [--output json|table|raw]
//!   flo kv set <key> <value> [--ttl <seconds>] [--nx] [--xx]
//!   flo kv delete <key>
//!   flo kv list [--prefix <prefix>]

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const output = @import("../output.zig");
const wire = @import("../../util/wire.zig");
const cli_config = @import("../config.zig");
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
                .uintFlag("wait", 'w', 0, "Wait until key exists (ms, 0=forever)")
                .uintFlag("block", 'b', 0, "Block for changes (ms, 0=forever)")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location (same as {tag} in key)")
                .action(wrapHandler(runGet)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("set")
                .about("Set a key-value pair")
                .examples(&.{
                    "flo kv set mykey myvalue",
                    "flo kv set mykey myvalue --ttl 3600",
                    "flo kv set mykey myvalue --nx",
                    "flo kv set counter 0 --xx",
                    "flo kv set mykey newvalue --cas 42",
                    "flo kv set balance:alice 500 --routing-key user:123",
                })
                .arg("key", "Key to set")
                .arg("value", "Value to store")
                .uintFlag("ttl", 0, 0, "Time-to-live in seconds (0=forever)")
                .boolFlag("nx", 0, "Only set if key does NOT exist")
                .boolFlag("xx", 0, "Only set if key DOES exist")
                .uint64Flag("cas", 0, 0, "Compare-and-swap version (only set if current version matches)")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location (same as {tag} in key)")
                .action(wrapHandler(runSet)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("delete")
                .about("Delete a key")
                .aliases(&.{"del"})
                .arg("key", "Key to delete")
                .stringFlag("routing-key", 'r', "", "Routing key for shard co-location (same as {tag} in key)")
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

    // Cannot use both --wait and --block
    if (wait_ms != null and block_ms != null) {
        ctx.printErr("Error: Cannot use both --wait and --block\n", .{});
        ctx.printErr("  --wait: returns immediately if key exists, else waits for creation\n", .{});
        ctx.printErr("  --block: blocks for changes (waits for NEXT version even if key exists)\n", .{});
        return error.CommandFailed;
    }

    const format = output.getFormat(ctx);

    const endpoint = cli_config.getEndpoint(ctx);

    if (output.isVerbose(ctx)) {
        ctx.printErr("[verbose] GET key={s} namespace={s} endpoint={s}\n", .{ key, namespace, endpoint });
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        ctx.printErr("Is the Flo server running at {s}?\n", .{endpoint});
        return error.CommandFailed;
    };

    var result = client_mod.kv.get(&client, namespace, key, wait_ms, block_ms, routing_key) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isNotFound()) {
        try outputKvResult(ctx, format, key, null, null);
        return error.CommandFailed;
    }

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    // Response data is the raw value (wire format: [version: u64][value: bytes])
    const value = result.asString();
    const version = result.getVersion();
    try outputKvResult(ctx, format, key, value, version);
}

fn runSet(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?; // validated by commander
    const value = ctx.getPositional("value").?; // validated by commander

    const ttl = ctx.getUint("ttl");
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
        ctx.printErr("Error: Cannot use both --nx and --xx\n", .{});
        return error.CommandFailed;
    }

    if (cas != null and nx) {
        ctx.printErr("Error: Cannot use --cas with --nx\n", .{});
        return error.CommandFailed;
    }

    const endpoint = cli_config.getEndpoint(ctx);

    if (output.isVerbose(ctx)) {
        ctx.printErr("[verbose] SET key={s} namespace={s} endpoint={s}\n", .{ key, namespace, endpoint });
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    const ttl_val: ?u64 = if (ttl) |t| if (t > 0) t else null else null;

    var result = client_mod.kv.set(&client, namespace, key, value, .{
        .ttl_seconds = ttl_val,
        .if_not_exists = nx,
        .if_exists = xx,
        .cas_version = cas,
        .routing_key = routing_key,
    }) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isConflict()) {
        if (cas != null) {
            ctx.printErr("Version mismatch\n", .{});
        } else {
            ctx.printErr("Condition not met (key {s})\n", .{if (nx) "already exists" else "does not exist"});
        }
        return error.CommandFailed;
    }

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("OK\n", .{});
}

fn runDelete(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getPositional("key").?; // validated by commander

    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    // --routing-key: explicit shard co-location (same routing as {tag} in key name)
    const routing_key: ?[]const u8 = blk: {
        const rk = ctx.getString("routing-key") orelse break :blk null;
        break :blk if (rk.len > 0) rk else null;
    };

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.kv.delete(&client, namespace, key, routing_key) catch |err| {
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

fn runList(ctx: *commander.Context) commander.Error!void {
    const prefix = ctx.getString("prefix") orelse "";
    const limit = ctx.getUint("limit") orelse 100;
    const namespace = cli_config.getNamespace(ctx);
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

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
        var result = client_mod.kv.scan(&client, namespace, prefix, cursor, @intCast(limit), true) catch |err| {
            ctx.printErr("Request failed: {}\n", .{err});
            return error.CommandFailed;
        };
        defer result.deinit();

        if (result.isError()) {
            ctx.printErr("Error: {s}\n", .{result.errorMessage()});
            return error.CommandFailed;
        }

        // Parse scan response wire format:
        // [count:u32] ([key_len:u16][key][value_len:u32][value])* [has_more:u8] [cursor_len:u16][cursor]?
        // Note: scan response does NOT have version prefix, use asRawData
        const data = result.asRawData() orelse {
            break; // No data
        };

        if (data.len < 4) break; // Need at least count field

        var reader = WireReader.init(data);
        const count = reader.readU32() orelse break;

        // Read keys
        var i: u32 = 0;
        while (i < count) : (i += 1) {
            const key_len = reader.readU16() orelse break;
            const key = reader.readSlice(key_len) orelse break;
            // Skip value (we're keys_only but value is still present as empty)
            const value_len = reader.readU32() orelse break;
            _ = reader.readSlice(value_len);

            // Store key
            const key_copy = ctx.allocator.dupe(u8, key) catch break;
            all_keys.append(ctx.allocator, key_copy) catch {
                ctx.allocator.free(key_copy);
                break;
            };

            if (all_keys.items.len >= limit) break;
        }

        // Read has_more flag
        const has_more = (reader.readU8() orelse 0) != 0;

        // Read next cursor
        const cursor_len = reader.readU16() orelse 0;
        const next_cursor = if (cursor_len > 0) reader.readSlice(cursor_len) else null;

        // Free previous cursor and copy new one
        if (cursor_owned) |c| ctx.allocator.free(c);
        cursor_owned = null;

        if (!has_more or next_cursor == null) {
            break; // Done walking shards
        }

        // Copy cursor for next iteration
        cursor_owned = ctx.allocator.dupe(u8, next_cursor.?) catch break;
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

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        return error.CommandFailed;
    };

    var result = client_mod.kv.history(&client, namespace, key, @intCast(limit)) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("History for key: {s}\n", .{key});
    // Output raw history data (no version prefix for history responses)
    if (result.asRawData()) |data| {
        if (data.len > 0) {
            ctx.print("{s}\n", .{data});
        }
    }
}

fn outputKvResult(ctx: *commander.Context, format: output.Format, key: []const u8, value: ?[]const u8, version: ?u64) !void {
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

            if (version != null) {
                table.addRow(&.{ key, value orelse "(nil)", ver_str }) catch return;
            } else {
                table.addRow(&.{ key, value orelse "(nil)" }) catch return;
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

// ==================== Testing ====================

test "create kv command" {
    const allocator = std.testing.allocator;

    const cmd = try createKvCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("kv", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 4);
}
