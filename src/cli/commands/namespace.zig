//! Namespace management commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo namespace create <name>
//!   flo namespace delete <name>
//!   flo namespace list [--all]
//!   flo namespace info <name>

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const output = @import("../output.zig");

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
        // Persistent flags - inherited by all subcommands
        .persistentFlag("endpoint", .{ .short = 'e', .value = .{ .string = "" }, .desc = "Server endpoint (host:port)" })
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
                .stringFlag("format", 'f', "table", "Output format: json, table")
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
        .build();
}

/// Get endpoint from flags or config
fn getEndpoint(ctx: *commander.Context) []const u8 {
    if (ctx.getString("endpoint")) |ep| {
        if (ep.len > 0) return ep;
    }
    return "127.0.0.1:9000";
}

fn runCreate(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?;
    const endpoint = getEndpoint(ctx);

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
    const endpoint = getEndpoint(ctx);
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
    const format_str = ctx.getString("format") orelse "table";
    const endpoint = getEndpoint(ctx);

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

    // Parse response data: [count: u32] + [name_len: u16][name: bytes]...
    const data = result.asRawData() orelse {
        ctx.printErr("Invalid response from server\n", .{});
        return error.CommandFailed;
    };

    const count = std.mem.readInt(u32, data[0..4], .little);

    if (std.mem.eql(u8, format_str, "json")) {
        // JSON output
        ctx.print("{{\n  \"namespaces\": [\n", .{});
        var offset: usize = 4;
        var i: u32 = 0;
        while (i < count and offset + 2 <= data.len) : (i += 1) {
            const name_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + name_len > data.len) break;
            const name = data[offset..][0..name_len];
            offset += name_len;

            if (i > 0) ctx.print(",\n", .{});
            ctx.print("    \"{s}\"", .{name});
        }
        ctx.print("\n  ]\n}}\n", .{});
    } else {
        // Table output
        ctx.print("NAMESPACE\n", .{});
        ctx.print("---------\n", .{});

        var offset: usize = 4;
        var i: u32 = 0;
        while (i < count and offset + 2 <= data.len) : (i += 1) {
            const name_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + name_len > data.len) break;
            const name = data[offset..][0..name_len];
            offset += name_len;
            ctx.print("{s}\n", .{name});
        }

        if (count == 0) {
            ctx.print("(no namespaces)\n", .{});
        }
    }
}

fn runInfo(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?;
    const endpoint = getEndpoint(ctx);

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
