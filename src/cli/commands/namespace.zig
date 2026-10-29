//! Namespace management commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo namespace create <name>
//!   flo namespace delete <name>
//!   flo namespace list
//!   flo namespace info <name>

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const outcome = @import("../outcome.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;
const output = @import("../output.zig");
const cli_config = @import("../config.zig");
const schema = @import("../../protocol/schema.zig");
const namespace_ops = @import("../../protocol/namespace_ops.zig");

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
                .about("Delete a namespace (not supported yet: the server refuses it)")
                .aliases(&.{ "rm", "remove" })
                .examples(&.{
                    "flo namespace delete myapp",
                    "flo ns rm staging",
                })
                .arg("name", "Name of the namespace to delete")
                .action(wrapHandler(runDelete)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list")
                .about("List all namespaces")
                .aliases(&.{"ls"})
                .examples(&.{
                    "flo namespace list",
                    "flo ns ls",
                })
                .action(wrapHandler(runList)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("info")
                .about("Show when a namespace was created; exits 1 if there is none")
                .examples(&.{
                    "flo namespace info myapp",
                })
                .arg("name", "Name of the namespace")
                .action(wrapHandler(runInfo)),
        )
        .build();
}

fn runCreate(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?;
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.namespace.create(&client, name) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    ctx.print("Created namespace: {s}\n", .{name});
}

fn runDelete(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?;
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.namespace.delete(&client, name) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    ctx.print("Deleted namespace: {s}\n", .{name});
}

/// Every name, a page at a time, following the cursor to the last page.
fn runList(ctx: *commander.Context) commander.Error!void {
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    const format = output.getFormat(ctx);
    var table = output.Table.init(ctx.allocator);
    defer table.deinit();
    table.addColumn("NAMESPACE", .left) catch return outcome.malformed(ctx, "namespace.list answer");
    // Kept until the last page, so a page that fails leaves no half-printed list.
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(ctx.allocator);

    var cursor_buf: [namespace_ops.Name.bound]u8 = undefined;
    var cursor: []const u8 = "";
    var shown: usize = 0;
    while (true) {
        var result = client_mod.namespace.list(&client, cursor) catch |err| return outcome.requestFailed(ctx, err);
        defer result.deinit();
        try outcome.check(ctx, result);

        var diag: schema.Diagnostic = .{};
        const page = namespace_ops.op("namespace.list").response.decodeAll(result.data, &diag) catch return outcome.malformed(ctx, "namespace.list answer");
        var it = page.names.iterator();
        while (it.next() catch return outcome.malformed(ctx, "namespace.list answer")) |name| {
            switch (format) {
                .json => {
                    if (shown > 0) out.append(ctx.allocator, ',') catch return outcome.malformed(ctx, "namespace.list answer");
                    out.print(ctx.allocator, "{f}", .{output.jsonString(name)}) catch return outcome.malformed(ctx, "namespace.list answer");
                },
                .raw => out.print(ctx.allocator, "{s}\n", .{name}) catch return outcome.malformed(ctx, "namespace.list answer"),
                .table => table.addRow(&.{name}) catch return outcome.malformed(ctx, "namespace.list answer"),
            }
            shown += 1;
        }
        const next = page.cursor orelse break;
        @memcpy(cursor_buf[0..next.len], next);
        cursor = cursor_buf[0..next.len];
    }
    switch (format) {
        .json => ctx.print("[{s}]\n", .{out.items}),
        .raw => ctx.print("{s}", .{out.items}),
        .table => if (shown == 0) output.printEmptyList(ctx, "(no namespaces)") else table.print(ctx),
    }
}

fn runInfo(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?;
    const endpoint = cli_config.getEndpoint(ctx);

    var client = Client.init(ctx.allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    var result = client_mod.namespace.info(&client, name) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();

    try outcome.check(ctx, result);

    var diag: schema.Diagnostic = .{};
    const info = namespace_ops.op("namespace.info").response.decodeAll(result.data, &diag) catch return outcome.malformed(ctx, "namespace.info answer");
    switch (output.getFormat(ctx)) {
        .json => ctx.print("{{\"name\":{f},\"created_at_ms\":{d}}}\n", .{ output.jsonString(name), info.created_at_ms }),
        else => {
            ctx.print("Namespace:  {s}\n", .{name});
            ctx.print("Created at: {d} (ms since the epoch)\n", .{info.created_at_ms});
        },
    }
}
