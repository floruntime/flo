//! Namespace management commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo namespace create <name>
//!   flo namespace delete <name>
//!   flo namespace list [--all]
//!   flo namespace info <name>
//!   flo namespace config <name> [--set key=value]

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const output = @import("../output.zig");
const cli_config = @import("../config.zig");
const NamespaceConfig = @import("../../cluster/coordinator.zig").NamespaceConfig;

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the namespace command tree
pub fn createNamespaceCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("namespace")
        .about("Namespace management operations")
        .group("Admin Commands")
        .aliases(&.{"ns"})
        .longAbout(
            \\Manage Flo namespaces for organizing and isolating data.
            \\
            \\Namespaces provide logical separation between different
            \\applications or environments using the same Flo cluster.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("create")
                .about("Create a new namespace")
                .examples(&.{
                    "flo namespace create myapp",
                    "flo namespace create staging",
                })
                .arg("name", "Name of the namespace to create")
                .action(wrapHandler(runCreate)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("delete")
                .about("Delete an existing namespace")
                .aliases(&.{ "rm", "remove" })
                .examples(&.{
                    "flo namespace delete myapp",
                    "flo ns rm staging",
                })
                .arg("name", "Name of the namespace to delete")
                .boolFlag("force", 'f', "Force delete even if not empty (deletes all resources)")
                .action(wrapHandler(runDelete)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List all namespaces")
                .aliases(&.{"ls"})
                .examples(&.{
                    "flo namespace list",
                    "flo ns ls --all",
                })
                .boolFlag("all", 'a', "Include system namespaces")
                .action(wrapHandler(runList)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("info")
                .about("Get namespace information")
                .examples(&.{
                    "flo namespace info myapp",
                })
                .arg("name", "Name of the namespace")
                .action(wrapHandler(runInfo)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("config")
                .about("Get or set namespace configuration")
                .examples(&.{
                    "flo namespace config myapp",
                    "flo ns config myapp --set kv_max_hot_versions=50",
                    "flo ns config myapp --set stream_retention_s=86400",
                    "flo ns config myapp --set memory_budget_bytes=1073741824",
                })
                .arg("name", "Name of the namespace")
                .stringFlag("set", 's', "Set a configuration value (key=value)", "")
                .action(wrapHandler(runConfig)),
        )
        .build();
}



fn runCreate(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?;
    const endpoint = cli_config.getEndpoint(ctx);

    // Validate namespace name
    if (name.len == 0) {
        ctx.printErr("Error: Namespace name cannot be empty\n", .{});
        return error.CommandFailed;
    }

    if (name.len > 64) {
        ctx.printErr("Error: Namespace name cannot exceed 64 characters\n", .{});
        return error.CommandFailed;
    }

    // Check for invalid characters
    for (name) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') {
            ctx.printErr("Error: Namespace name can only contain alphanumeric characters, underscores, and hyphens\n", .{});
            return error.CommandFailed;
        }
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        ctx.printErr("Is the Flo server running at {s}?\n", .{endpoint});
        return error.CommandFailed;
    };

    var result = client_mod.namespace.create(&client, name) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("Created namespace: {s}\n", .{name});
}

fn runDelete(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?;
    const endpoint = cli_config.getEndpoint(ctx);
    const force = ctx.getBool("force");

    // Prevent deletion of reserved namespaces
    if (std.mem.eql(u8, name, "default") or std.mem.eql(u8, name, "_system")) {
        ctx.printErr("Error: Cannot delete system namespace '{s}'\n", .{name});
        return error.CommandFailed;
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        ctx.printErr("Is the Flo server running at {s}?\n", .{endpoint});
        return error.CommandFailed;
    };

    var result = client_mod.namespace.delete(&client, name, force) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    ctx.print("Deleted namespace: {s}\n", .{name});
}

fn runList(ctx: *commander.Context) commander.Error!void {
    const include_all = ctx.getBool("all");
    const endpoint = cli_config.getEndpoint(ctx);

    if (output.isVerbose(ctx)) {
        ctx.printErr("[verbose] LIST namespaces endpoint={s} all={}\n", .{ endpoint, include_all });
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        ctx.printErr("Is the Flo server running at {s}?\n", .{endpoint});
        return error.CommandFailed;
    };

    var result = client_mod.namespace.list(&client, include_all) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    if (result.asRawData()) |data| {
        output.printWireList(ctx, data, "(no namespaces)", &.{
            .{ .field = "name", .header = "NAMESPACE", .field_type = .str_u16 },
        });
    } else {
        ctx.print("(no namespaces)\n", .{});
    }
}

fn runInfo(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?;
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        ctx.printErr("Is the Flo server running at {s}?\n", .{endpoint});
        return error.CommandFailed;
    };

    var result = client_mod.namespace.info(&client, name) catch |err| {
        ctx.printErr("Request failed: {}\n", .{err});
        return error.CommandFailed;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.printErr("Error: {s}\n", .{result.errorMessage()});
        return error.CommandFailed;
    }

    // Parse response: [exists: u8][name_len: u16][name: bytes]
    const data = result.asRawData() orelse {
        ctx.printErr("Invalid response from server\n", .{});
        return error.CommandFailed;
    };

    const exists = data[0] != 0;

    if (exists) {
        ctx.print("Namespace: {s}\n", .{name});
        ctx.print("Status: exists\n", .{});
    } else {
        ctx.print("Namespace '{s}' does not exist\n", .{name});
        return error.CommandFailed;
    }
}

fn runConfig(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?;
    const endpoint = cli_config.getEndpoint(ctx);
    const set_val = ctx.getString("set") orelse "";

    if (name.len == 0) {
        ctx.printErr("Error: Namespace name cannot be empty\n", .{});
        return error.CommandFailed;
    }

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        ctx.printErr("Is the Flo server running at {s}?\n", .{endpoint});
        return error.CommandFailed;
    };

    if (set_val.len > 0) {
        // Parse key=value
        const eq_pos = std.mem.indexOfScalar(u8, set_val, '=') orelse {
            ctx.printErr("Error: --set value must be in key=value format (e.g. kv_max_hot_versions=50)\n", .{});
            return error.CommandFailed;
        };
        const key = set_val[0..eq_pos];
        const val_str = set_val[eq_pos + 1 ..];

        var config: NamespaceConfig = .{};
        const parsed = parseSettingKeyValue(key, val_str, &config) catch {
            ctx.printErr("Error: Unknown setting '{s}'. Valid settings:\n", .{key});
            ctx.printErr("  kv_max_hot_versions, kv_version_ttl_s, stream_retention_bytes,\n", .{});
            ctx.printErr("  stream_retention_s, queue_max_dlq_size, queue_max_lease_s,\n", .{});
            ctx.printErr("  memory_budget_bytes\n", .{});
            return error.CommandFailed;
        };
        if (!parsed) {
            ctx.printErr("Error: Invalid value '{s}' for setting '{s}'\n", .{ val_str, key });
            return error.CommandFailed;
        }

        var tlv_buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
        const tlv_len = config.serializeSettings(&tlv_buf);

        var result = client_mod.namespace.configSet(&client, name, tlv_buf[0..tlv_len]) catch |err| {
            ctx.printErr("Request failed: {}\n", .{err});
            return error.CommandFailed;
        };
        defer result.deinit();

        if (result.isError()) {
            ctx.printErr("Error: {s}\n", .{result.errorMessage()});
            return error.CommandFailed;
        }

        ctx.print("Updated namespace '{s}': {s}={s}\n", .{ name, key, val_str });
    } else {
        // Get current config
        var result = client_mod.namespace.configGet(&client, name) catch |err| {
            ctx.printErr("Request failed: {}\n", .{err});
            return error.CommandFailed;
        };
        defer result.deinit();

        if (result.isError()) {
            ctx.printErr("Error: {s}\n", .{result.errorMessage()});
            return error.CommandFailed;
        }

        const data = result.asRawData() orelse {
            ctx.printErr("Invalid response from server\n", .{});
            return error.CommandFailed;
        };

        const de = NamespaceConfig.deserializeSettings(data);
        const s = de.config;

        ctx.print("Namespace: {s}\n", .{name});
        ctx.print("Configuration:\n", .{});
        printSetting(ctx, "kv_max_hot_versions", if (s.kv_max_hot_versions) |v| fmtU32(v) else null);
        printSetting(ctx, "kv_version_ttl_s", if (s.kv_version_ttl_s) |v| fmtU64(v) else null);
        printSetting(ctx, "stream_retention_bytes", if (s.stream_retention_bytes) |v| fmtU64(v) else null);
        printSetting(ctx, "stream_retention_s", if (s.stream_retention_s) |v| fmtU64(v) else null);
        printSetting(ctx, "queue_max_dlq_size", if (s.queue_max_dlq_size) |v| fmtU32(v) else null);
        printSetting(ctx, "queue_max_lease_s", if (s.queue_max_lease_s) |v| fmtU32(v) else null);
        printSetting(ctx, "memory_budget_bytes", if (s.memory_budget_bytes) |v| fmtU64(v) else null);
    }
}

fn printSetting(ctx: *commander.Context, key: []const u8, value: ?[20]u8) void {
    if (value) |buf| {
        const str = std.mem.sliceTo(&buf, 0);
        ctx.print("  {s}: {s}\n", .{ key, str });
    } else {
        ctx.print("  {s}: (default)\n", .{key});
    }
}

fn fmtU32(v: u32) [20]u8 {
    var buf: [20]u8 = .{0} ** 20;
    const s = std.fmt.bufPrint(&buf, "{d}", .{v}) catch return buf;
    buf[s.len] = 0;
    return buf;
}

fn fmtU64(v: u64) [20]u8 {
    var buf: [20]u8 = .{0} ** 20;
    const s = std.fmt.bufPrint(&buf, "{d}", .{v}) catch return buf;
    buf[s.len] = 0;
    return buf;
}

fn parseSettingKeyValue(key: []const u8, val_str: []const u8, config: *NamespaceConfig) !bool {
    if (std.mem.eql(u8, key, "kv_max_hot_versions")) {
        config.kv_max_hot_versions = std.fmt.parseInt(u32, val_str, 10) catch return false;
    } else if (std.mem.eql(u8, key, "kv_version_ttl_s")) {
        config.kv_version_ttl_s = std.fmt.parseInt(u64, val_str, 10) catch return false;
    } else if (std.mem.eql(u8, key, "stream_retention_bytes")) {
        config.stream_retention_bytes = std.fmt.parseInt(u64, val_str, 10) catch return false;
    } else if (std.mem.eql(u8, key, "stream_retention_s")) {
        config.stream_retention_s = std.fmt.parseInt(u64, val_str, 10) catch return false;
    } else if (std.mem.eql(u8, key, "queue_max_dlq_size")) {
        config.queue_max_dlq_size = std.fmt.parseInt(u32, val_str, 10) catch return false;
    } else if (std.mem.eql(u8, key, "queue_max_lease_s")) {
        config.queue_max_lease_s = std.fmt.parseInt(u32, val_str, 10) catch return false;
    } else if (std.mem.eql(u8, key, "memory_budget_bytes")) {
        config.memory_budget_bytes = std.fmt.parseInt(u64, val_str, 10) catch return false;
    } else {
        return error.UnknownSetting;
    }
    return true;
}
