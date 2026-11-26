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
const server_cmd = @import("server.zig");
const stdx = @import("stdx");

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
        .subcommand(
            commander.newBuilder(allocator)
                .name("alter")
                .about("Set a namespace's run-history quota, over every shard. It administers the node: run it on a node, where it goes to the admin socket, then to the leader")
                .examples(&.{
                    "flo namespace alter myapp --history-quota 20GiB",
                    "flo namespace alter myapp --history-quota none",
                })
                .arg("name", "Name of the namespace")
                .stringFlag("history-quota", 0, "", "Bytes over every shard (a number, or with KiB, MiB, GiB or TiB), none to turn it off, or default for the server's")
                .stringFlag("config", 'c', "", "The node's config, naming its data directory or [admin] socket")
                .stringFlag("data-dir", 'd', "", "The node's data directory (overrides the config)")
                .action(wrapHandler(runAlter)),
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
                    // An object per name, so fields can join it later.
                    out.print(ctx.allocator, "{{\"name\":{f}}}", .{output.jsonString(name)}) catch return outcome.malformed(ctx, "namespace.list answer");
                },
                .raw => out.print(ctx.allocator, "{s}\n", .{name}) catch return outcome.malformed(ctx, "namespace.list answer"),
                .table => table.addRow(&.{name}) catch return outcome.malformed(ctx, "namespace.list answer"),
            }
            shown += 1;
        }
        const next = page.cursor orelse break;
        // A cursor that doesn't move on would page forever.
        if (std.mem.order(u8, next, cursor) != .gt) return outcome.malformed(ctx, "namespace.list answer (its cursor didn't advance)");
        @memcpy(cursor_buf[0..next.len], next);
        cursor = cursor_buf[0..next.len];
    }
    switch (format) {
        .json => ctx.print("[{s}]\n", .{out.items}),
        .raw => ctx.print("{s}", .{out.items}),
        .table => if (shown == 0) output.printEmptyList(ctx, "(no namespaces)") else table.print(ctx),
    }
}

/// `n` bytes as a person reads them: whole bytes below a KiB, else to one
/// place in the largest binary unit that keeps it at 1 or more.
fn bytesText(buf: []u8, n: u64) []const u8 {
    const units = [_]struct { []const u8, u64 }{ .{ "TiB", 1 << 40 }, .{ "GiB", 1 << 30 }, .{ "MiB", 1 << 20 }, .{ "KiB", 1 << 10 } };
    for (units) |u| if (n >= u[1]) {
        const tenths = (n / u[1]) * 10 + (n % u[1]) * 10 / u[1];
        return std.fmt.bufPrint(buf, "{d}.{d} {s}", .{ tenths / 10, tenths % 10, u[0] }) catch "?";
    };
    return std.fmt.bufPrint(buf, "{d} B", .{n}) catch "?";
}

/// A quota as the flag gives it: `default`, `none`, or bytes, a number with
/// an optional binary unit, above 0 and at most the wire's most. Errors for
/// anything else.
pub fn parseQuota(text: []const u8) error{Invalid}!struct { mode: namespace_ops.QuotaMode, bytes: ?u64 } {
    if (std.mem.eql(u8, text, "none")) return .{ .mode = .none, .bytes = null };
    if (std.mem.eql(u8, text, "default")) return .{ .mode = .default, .bytes = null };
    const units = [_]struct { []const u8, u64 }{ .{ "TiB", 1 << 40 }, .{ "GiB", 1 << 30 }, .{ "MiB", 1 << 20 }, .{ "KiB", 1 << 10 }, .{ "B", 1 } };
    const bytes = for (units) |u| {
        if (std.mem.endsWith(u8, text, u[0])) {
            const n = std.fmt.parseInt(u64, text[0 .. text.len - u[0].len], 10) catch return error.Invalid;
            break std.math.mul(u64, n, u[1]) catch return error.Invalid;
        }
    } else std.fmt.parseInt(u64, text, 10) catch return error.Invalid;
    if (bytes == 0 or bytes > std.math.maxInt(u64) - 1) return error.Invalid;
    return .{ .mode = .bytes, .bytes = bytes };
}

fn runAlter(ctx: *commander.Context) commander.Error!void {
    const name = ctx.getPositional("name").?;
    const text = ctx.getString("history-quota") orelse "";
    if (text.len == 0) return outcome.usage(ctx, "give --history-quota <bytes|none|default>", .{});
    const quota = parseQuota(text) catch return outcome.usage(ctx, "--history-quota is a number of bytes above 0 and below 2^64 - 1 (optionally with KiB, MiB, GiB or TiB), none, or default", .{});
    // It administers the node: the client port refuses it, so it goes over
    // the node's admin socket, which carries it to shard 0's leader.
    if (ctx.flagChanged("endpoint") or ctx.flagChanged("port"))
        return outcome.usage(ctx, "-e/--endpoint and -p/--port name a server to talk to, but this command goes to a node's admin socket; name the node with --config/--data-dir and run it on a member", .{});
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try server_cmd.adminSocketPath(ctx, &path_buf);
    if (path.len > stdx.net.UNIX_PATH_MAX)
        return outcome.localRefusal(ctx, "the admin socket {s} is {d} bytes, past the {d} a Unix socket path may hold, so the node started without one. Set [admin] socket to a shorter path in its config and restart it", .{ path, path.len, stdx.net.UNIX_PATH_MAX });
    var client = Client.init(ctx.allocator, path);
    defer client.deinit();
    client.connectUnix(path) catch |err| return switch (err) {
        error.ConnectionRefused => outcome.localRefusal(ctx, "no server listens on the admin socket {s}: is a node running on this data directory? One that started without its socket logged why", .{path}),
        error.PermissionDenied => outcome.localRefusal(ctx, "permission denied on the admin socket {s}: run as the user the server runs as", .{path}),
        error.NotOurs => outcome.localRefusal(ctx, "{s} isn't a socket you own, so it isn't your server's admin socket: run as the user the server runs as, or check [admin] socket", .{path}),
        else => outcome.localRefusal(ctx, "cannot connect to the admin socket {s}: {s}", .{ path, @errorName(err) }),
    };
    var result = client_mod.namespace.alter(&client, name, quota.mode, quota.bytes) catch |err| return outcome.requestFailed(ctx, err);
    defer result.deinit();
    try outcome.check(ctx, result);
    ctx.print("OK\n", .{});
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
        .json => {
            var out: std.ArrayListUnmanaged(u8) = .empty;
            defer out.deinit(ctx.allocator);
            var it = info.shards.iterator();
            var first = true;
            while (it.next() catch return outcome.malformed(ctx, "namespace.info answer")) |s| : (first = false) {
                if (!first) out.append(ctx.allocator, ',') catch return error.OutOfMemory;
                out.print(ctx.allocator, "{{\"shard\":{d},\"bound\":{},\"part_bytes\":", .{ s.shard, s.bound }) catch return error.OutOfMemory;
                (if (s.part_bytes) |p| out.print(ctx.allocator, "{d}", .{p}) else out.appendSlice(ctx.allocator, "null")) catch return error.OutOfMemory;
                out.print(ctx.allocator, ",\"usage_bytes\":{d},\"orphans\":{d},\"orphan_bytes\":{d}}}", .{ s.usage_bytes, s.orphans, s.orphan_bytes }) catch return error.OutOfMemory;
            }
            ctx.print("{{\"name\":{f},\"created_at_ms\":{d},\"history_quota\":", .{ output.jsonString(name), info.created_at_ms });
            if (info.history_quota) |q| ctx.print("{d}", .{q}) else ctx.print("null", .{});
            ctx.print(",\"history_quota_own\":{},\"shards\":[{s}]}}\n", .{ info.history_quota_own, out.items });
        },
        else => {
            ctx.print("Namespace:  {s}\n", .{name});
            // "default" was never created: it is there from the start.
            if (info.created_at_ms == 0) ctx.print("Created at: built in\n", .{}) else ctx.print("Created at: {d} (ms since the epoch)\n", .{info.created_at_ms});
            var qb: [32]u8 = undefined;
            if (info.history_quota) |q| ctx.print("History quota: {s} over every shard ({s})\n", .{ bytesText(&qb, q), if (info.history_quota_own) "its own" else "the default" }) else ctx.print("History quota: none ({s})\n", .{if (info.history_quota_own) "its own" else "the default"});
            var table = output.Table.init(ctx.allocator);
            defer table.deinit();
            for ([_][]const u8{ "SHARD", "PART", "USAGE", "ORPHANS" }) |h| table.addColumn(h, .left) catch return error.OutOfMemory;
            var it = info.shards.iterator();
            while (it.next() catch return outcome.malformed(ctx, "namespace.info answer")) |s| {
                var b: [4][24]u8 = undefined;
                table.addRow(&.{
                    std.fmt.bufPrint(&b[0], "{d}", .{s.shard}) catch unreachable,
                    if (!s.bound) "not bound" else if (s.part_bytes) |p| bytesText(&b[1], p) else "none",
                    bytesText(&b[2], s.usage_bytes),
                    std.fmt.bufPrint(&b[3], "{d}", .{s.orphans}) catch unreachable,
                }) catch return error.OutOfMemory;
            }
            table.print(ctx);
        },
    }
}
