//! REPL - Interactive shell for Flo using Commander framework. It talks to
//! the server over the Flo binary protocol, through the CLI client.
//!
//! Usage:
//!   flo repl [--endpoint <host:port>] [--namespace <ns>]
//!
//! This provides a redis-cli-like interactive experience.

const std = @import("std");
const schema = @import("../../protocol/schema.zig");
const kv_ops = @import("../../protocol/kv_ops.zig");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const cli_config = @import("../config.zig");
const outcome = @import("../outcome.zig");
const client_mod = @import("../client/mod.zig");
const Client = client_mod.Client;

const proto = @import("../../protocol/proto.zig");

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the repl command
pub fn createReplCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("repl")
        .about("Interactive Redis-like shell")
        .group("Interactive Commands")
        .longAbout(
            \\Start an interactive REPL (Read-Eval-Print Loop) session.
            \\
            \\Provides a Redis-cli-like experience for interacting with Flo.
            \\Supports commands like GET, SET, DEL, PING, and more.
            \\
            \\Type 'HELP' in the REPL for available commands.
        )
        .examples(&.{
            "flo repl",
            "flo repl --endpoint localhost:9000",
            "flo repl --namespace myapp",
        })
        .stringFlag("namespace", 'n', "default", "Initial namespace")
        .action(wrapHandler(runRepl))
        .build();
}

/// Exits 0 whatever its lines do, so scripts must not read its exit code as a command's.
fn runRepl(ctx: *commander.Context) commander.Error!void {
    const allocator = ctx.allocator;

    const endpoint = cli_config.getEndpoint(ctx);

    // Get initial namespace
    var current_namespace: []const u8 = cli_config.getNamespace(ctx);

    // Connect to server
    var client = Client.init(allocator, endpoint);
    defer client.deinit();

    client.connect() catch |err| return outcome.connectFailed(ctx, err, client.endpoint);

    // Print welcome banner
    ctx.print("\n", .{});
    ctx.print("  ╔═══════════════════════════════════════╗\n", .{});
    ctx.print("  ║           FLO REPL                    ║\n", .{});
    ctx.print("  ╚═══════════════════════════════════════╝\n", .{});
    ctx.print("\n", .{});
    ctx.print("  Connected to: {s}\n", .{endpoint});
    ctx.print("  Namespace: {s}\n", .{current_namespace});
    ctx.print("\n", .{});
    ctx.print("  Type 'HELP' for available commands, 'QUIT' to exit.\n", .{});
    ctx.print("\n", .{});

    // Namespace buffer - stores the current namespace separately
    var namespace_buf: [256]u8 = undefined;
    const namespace_len = @min(current_namespace.len, namespace_buf.len);
    @memcpy(namespace_buf[0..namespace_len], current_namespace[0..namespace_len]);
    current_namespace = namespace_buf[0..namespace_len];

    // Read stdin for input using direct posix read
    const stdin_fd = std.posix.STDIN_FILENO;
    var line_buf: [4096]u8 = undefined;

    // REPL loop
    while (true) {
        // Print prompt
        ctx.print("{s}> ", .{current_namespace});

        // Flush stdout - use std.posix for write operations
        // (std.io.getStdOut was renamed in Zig 0.15)
        _ = @import("stdx").io.writeFd(std.posix.STDOUT_FILENO, "");

        // Read line from stdin
        const line = readLine(stdin_fd, &line_buf) catch |err| {
            if (err == error.EndOfStream) {
                ctx.print("\n", .{});
                break;
            }
            return outcome.usage(ctx, "can't read stdin: {s}", .{@errorName(err)});
        };

        // Trim whitespace
        const trimmed = std.mem.trim(u8, line, " \t\r\n");
        if (trimmed.len == 0) continue;

        // Parse and execute command
        const upper_cmd = toUpper(trimmed);

        if (std.mem.eql(u8, upper_cmd, "QUIT") or std.mem.eql(u8, upper_cmd, "EXIT")) {
            ctx.print("Bye!\n", .{});
            break;
        } else if (std.mem.eql(u8, upper_cmd, "HELP")) {
            printReplHelp(ctx);
        } else if (std.mem.eql(u8, upper_cmd, "PING")) {
            // Simple ping
            ctx.print("PONG\n", .{});
        } else if (std.mem.startsWith(u8, upper_cmd, "USE ")) {
            // Switch namespace
            const new_ns = std.mem.trim(u8, trimmed[4..], " ");
            if (new_ns.len > 0 and new_ns.len <= namespace_buf.len) {
                @memcpy(namespace_buf[0..new_ns.len], new_ns);
                current_namespace = namespace_buf[0..new_ns.len];
                ctx.print("OK\n", .{});
            } else {
                ctx.print("(error) Invalid namespace\n", .{});
            }
        } else if (std.mem.startsWith(u8, upper_cmd, "GET ")) {
            const key = std.mem.trim(u8, trimmed[4..], " ");
            executeGet(&client, ctx, current_namespace, key);
        } else if (std.mem.startsWith(u8, upper_cmd, "SET ")) {
            const rest = std.mem.trim(u8, trimmed[4..], " ");
            executeSet(&client, ctx, current_namespace, rest);
        } else if (std.mem.startsWith(u8, upper_cmd, "DEL ")) {
            const key = std.mem.trim(u8, trimmed[4..], " ");
            executeDel(&client, ctx, current_namespace, key);
        } else if (std.mem.startsWith(u8, upper_cmd, "KEYS") or std.mem.startsWith(u8, upper_cmd, "SCAN")) {
            executeKeys(&client, ctx, current_namespace);
        } else {
            ctx.print("(error) Unknown command: {s}\n", .{trimmed});
            ctx.print("Type 'HELP' for available commands\n", .{});
        }
    }
}

fn readLine(fd: std.posix.fd_t, buf: []u8) ![]const u8 {
    var total: usize = 0;
    while (total < buf.len) {
        const n = std.posix.read(fd, buf[total..]) catch |err| {
            if (err == error.WouldBlock) continue;
            return err;
        };
        if (n == 0) return error.EndOfStream;

        // Check for newline
        for (buf[total .. total + n]) |c| {
            if (c == '\n') {
                return buf[0 .. total + n];
            }
        }
        total += n;
    }
    return buf[0..total];
}

fn toUpper(s: []const u8) []const u8 {
    // For comparison, just use the input (commands are case-insensitive)
    // This is a simplified version - proper implementation would uppercase
    var buf: [256]u8 = undefined;
    const len = @min(s.len, buf.len);
    for (s[0..len], 0..) |c, i| {
        buf[i] = std.ascii.toUpper(c);
    }
    return buf[0..len];
}

fn printReplHelp(ctx: *commander.Context) void {
    ctx.print("\n", .{});
    ctx.print("Available Commands:\n", .{});
    ctx.print("  USE <namespace>     Switch to a different namespace\n", .{});
    ctx.print("  GET <key>           Get a key's value\n", .{});
    ctx.print("  SET <key> <value>   Set a key's value\n", .{});
    ctx.print("  DEL <key>           Delete a key\n", .{});
    ctx.print("  KEYS / SCAN         List all keys\n", .{});
    ctx.print("  PING                Test connection\n", .{});
    ctx.print("  HELP                Show this help\n", .{});
    ctx.print("  QUIT / EXIT         Exit the REPL\n", .{});
    ctx.print("\n", .{});
}

fn executeGet(client: *Client, ctx: *commander.Context, namespace: []const u8, key: []const u8) void {
    if (key.len == 0) {
        ctx.print("(error) Missing key\n", .{});
        return;
    }

    var result = client_mod.kv.get(client, namespace, key, .{ .wait_ms = 0, .wait_for = .exists, .routing_key = null, .txn = null }) catch |err| {
        ctx.print("(error) {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isNotFound()) {
        ctx.print("(nil)\n", .{});
        return;
    }

    if (result.isError()) {
        ctx.print("(error) {s}\n", .{result.errorMessage()});
        return;
    }

    var diag: schema.Diagnostic = .{};
    const answer = client_mod.kv.get_op.response.decodeAll(result.data, &diag) catch {
        ctx.print("(error) malformed get response\n", .{});
        return;
    };
    ctx.print("\"{s}\"\n", .{answer.value});
}

fn executeSet(client: *Client, ctx: *commander.Context, namespace: []const u8, rest: []const u8) void {
    // Parse "key value" - simple space split
    const space_pos = std.mem.indexOfScalar(u8, rest, ' ');
    if (space_pos == null) {
        ctx.print("(error) Usage: SET <key> <value>\n", .{});
        return;
    }

    const key = rest[0..space_pos.?];
    const value = std.mem.trim(u8, rest[space_pos.? + 1 ..], " ");

    if (key.len == 0 or value.len == 0) {
        ctx.print("(error) Usage: SET <key> <value>\n", .{});
        return;
    }

    var result = client_mod.kv.set(client, namespace, key, .{ .value = value, .ttl_ms = null, .condition = .always, .cas_version = null, .routing_key = null, .txn = null }) catch |err| {
        ctx.print("(error) {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.print("(error) {s}\n", .{result.errorMessage()});
        return;
    }

    ctx.print("OK\n", .{});
}

fn executeDel(client: *Client, ctx: *commander.Context, namespace: []const u8, key: []const u8) void {
    if (key.len == 0) {
        ctx.print("(error) Missing key\n", .{});
        return;
    }

    var result = client_mod.kv.call(client, "kv.delete", namespace, key, .{ .cas_version = null, .routing_key = null, .txn = null }) catch |err| {
        ctx.print("(error) {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.print("(error) {s}\n", .{result.errorMessage()});
        return;
    }
    var diag: schema.Diagnostic = .{};
    const answer = kv_ops.op("kv.delete").response.decodeAll(result.data, &diag) catch {
        ctx.print("(error) malformed delete response\n", .{});
        return;
    };
    ctx.print("(integer) {d}\n", .{@intFromBool(answer.existed orelse true)});
}

fn executeKeys(client: *Client, ctx: *commander.Context, namespace: []const u8) void {
    var result = client_mod.kv.call(client, "kv.scan", namespace, "", .{ .limit = 100, .cursor = "" }) catch |err| {
        ctx.print("(error) {}\n", .{err});
        return;
    };
    defer result.deinit();

    if (result.isError()) {
        ctx.print("(error) {s}\n", .{result.errorMessage()});
        return;
    }
    var diag: schema.Diagnostic = .{};
    const answer = kv_ops.op("kv.scan").response.decodeAll(result.data, &diag) catch {
        ctx.print("(error) malformed scan response\n", .{});
        return;
    };
    if (answer.names.count == 0) return ctx.print("(empty array)\n", .{});
    var it = answer.names.iterator();
    var i: usize = 1;
    while (it.next() catch null) |name| : (i += 1) ctx.print("{d}) \"{s}\"\n", .{ i, name });
}

// ==================== Testing ====================

test "create repl command" {
    const allocator = std.testing.allocator;

    const cmd = try createReplCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("repl", cmd.name);
}
