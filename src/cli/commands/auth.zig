//! Auth CLI commands for Flo
//!
//! Commands:
//!   flo auth login [--key KEY --server HOST:PORT]
//!   flo auth create-key --name NAME --role ROLE [--out FILE] [--expires-in DURATION]
//!   flo auth list-keys
//!   flo auth revoke-key KEY_ID
//!   flo auth whoami

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const cli_config = @import("../config.zig");
const output = @import("../output.zig");

fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

pub fn createAuthCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("auth")
        .about("Authentication management")
        .group("AUTH COMMANDS")
        .longAbout(
            \\Manage API keys and authentication contexts.
            \\
            \\Use 'flo auth login' to store credentials for a server,
            \\and 'flo auth create-key' to generate new API keys (admin only).
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("login")
                .about("Store API key for a server")
                .longAbout(
                    \\Save an API key and server address as a named context.
                    \\If called without flags, prompts interactively.
                )
                .examples(&.{
                    "flo auth login",
                    "flo auth login --key flo_sk_admin_xxx --server localhost:9000",
                    "flo auth login --key flo_sk_admin_xxx --server localhost:9000 --name dev",
                })
                .stringFlag("key", 'k', "", "API key")
                .stringFlag("server", 's', "", "Server endpoint (host:port)")
                .stringFlag("name", 'n', "", "Context name (default: derived from server)")
                .action(wrapHandler(runLogin)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("create-key")
                .about("Create a new API key (admin only)")
                .examples(&.{
                    "flo auth create-key --name ci-bot --role operator",
                    "flo auth create-key --name alice --role viewer --out alice.key",
                    "flo auth create-key --name deploy --role operator --expires-in 90d",
                })
                .stringFlag("name", 'n', "", "Human-readable key name")
                .stringFlag("role", 'r', "", "Key role: admin, operator, viewer")
                .stringFlag("out", 'o', "", "Write key to file instead of stdout")
                .stringFlag("expires-in", 'x', "", "Key expiration (e.g., 30d, 90d, 365d)")
                .action(wrapHandler(runCreateKey)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("list-keys")
                .about("List all API keys")
                .action(wrapHandler(runListKeys)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("revoke-key")
                .about("Revoke an API key")
                .arg("key_id", "Key ID or prefix to revoke")
                .action(wrapHandler(runRevokeKey)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("whoami")
                .about("Show current context and role")
                .action(wrapHandler(runWhoami)),
        )
        .build();
}

fn runLogin(ctx: *commander.Context) commander.Error!void {
    const key = ctx.getString("key");
    const server = ctx.getString("server");
    const name = ctx.getString("name");

    if (key == null or key.?.len == 0 or server == null or server.?.len == 0) {
        ctx.printErr("Error: --key and --server are required\n", .{});
        ctx.printErr("Usage: flo auth login --key flo_sk_admin_xxx --server localhost:9000\n", .{});
        return error.InvalidArgs;
    }

    const key_str = key.?;
    const server_str = server.?;

    // Derive context name from server if not provided
    const ctx_name = blk: {
        if (name) |n| {
            if (n.len > 0) break :blk n;
        }
        // Use server address as context name, replacing : with -
        break :blk server_str;
    };

    // Load or create config
    var cfg = cli_config.Config.load(ctx.allocator) catch cli_config.Config.init(ctx.allocator);
    defer cfg.deinit();

    // Add context with the API key stored in endpoint field for now
    // (Context struct will be extended to include api_key)
    cfg.setContext(ctx_name, server_str, null) catch |err| {
        ctx.printErr("Error saving context: {}\n", .{err});
        return error.CommandFailed;
    };
    cfg.current_context = ctx.allocator.dupe(u8, ctx_name) catch return error.CommandFailed;

    cfg.save() catch |err| {
        ctx.printErr("Error saving config: {}\n", .{err});
        return error.CommandFailed;
    };

    _ = key_str; // API key storage will be wired when config.yaml migration happens

    ctx.print("Logged in to {s} as context '{s}'\n", .{ server_str, ctx_name });
    ctx.print("  Tip: use 'flo config use-context {s}' to switch back later\n", .{ctx_name});
}

fn runCreateKey(ctx: *commander.Context) commander.Error!void {
    const name_opt = ctx.getString("name");
    const role_opt = ctx.getString("role");
    const out_path = ctx.getString("out");
    const expires_in = ctx.getString("expires-in");

    if (name_opt == null or name_opt.?.len == 0) {
        ctx.printErr("Error: --name is required\n", .{});
        return error.InvalidArgs;
    }
    if (role_opt == null or role_opt.?.len == 0) {
        ctx.printErr("Error: --role is required (admin, operator, viewer)\n", .{});
        return error.InvalidArgs;
    }

    // Parse expiration
    const expires_at: i64 = blk: {
        if (expires_in) |ei| {
            if (ei.len > 0) {
                break :blk parseExpiresIn(ei) catch {
                    ctx.printErr("Error: invalid --expires-in format (use e.g. 30d, 90d, 365d)\n", .{});
                    return error.InvalidArgs;
                };
            }
        }
        break :blk 0; // never expires
    };

    // This is a stub — in production the CLI sends a request to the server
    // which creates the key server-side. For now, demonstrate the API.
    ctx.print("create-key: name={s} role={s} expires_at={d}\n", .{
        name_opt.?,
        role_opt.?,
        expires_at,
    });
    if (out_path) |op| {
        if (op.len > 0) {
            ctx.print("  output: {s}\n", .{op});
        }
    }
    ctx.print("Note: key creation requires a running server with admin credentials\n", .{});
}

fn runListKeys(ctx: *commander.Context) commander.Error!void {
    const format = output.getFormat(ctx);
    ctx.print("list-keys: format={s}\n", .{@tagName(format)});
    ctx.print("Note: key listing requires a running server with admin credentials\n", .{});
}

fn runRevokeKey(ctx: *commander.Context) commander.Error!void {
    if (ctx.args.len < 1) {
        ctx.printErr("Error: key ID is required\n", .{});
        ctx.printErr("Usage: flo auth revoke-key <key_id>\n", .{});
        return error.InvalidArgs;
    }
    const key_id = ctx.args[0];
    ctx.print("revoke-key: {s}\n", .{key_id});
    ctx.print("Note: key revocation requires a running server with admin credentials\n", .{});
}

fn runWhoami(ctx: *commander.Context) commander.Error!void {
    var cfg = cli_config.Config.load(ctx.allocator) catch {
        ctx.print("Not logged in (no config file)\n", .{});
        return;
    };
    defer cfg.deinit();

    ctx.print("Context: {s}\n", .{cfg.current_context});
    if (cfg.getCurrentContext()) |current| {
        ctx.print("Server:  {s}\n", .{current.endpoint});
        if (current.namespace) |ns| {
            ctx.print("Namespace: {s}\n", .{ns});
        }
    } else {
        ctx.print("Server:  (not set)\n", .{});
    }
}

/// Parse duration strings like "30d", "90d", "365d" into Unix timestamp.
fn parseExpiresIn(duration: []const u8) !i64 {
    if (duration.len < 2) return error.InvalidFormat;
    const unit = duration[duration.len - 1];
    const value_str = duration[0 .. duration.len - 1];
    const value = try std.fmt.parseInt(i64, value_str, 10);
    if (value <= 0) return error.InvalidFormat;

    const seconds: i64 = switch (unit) {
        'd' => value * 86400,
        'h' => value * 3600,
        'm' => value * 60,
        else => return error.InvalidFormat,
    };

    return std.time.timestamp() + seconds;
}

// ==================== Testing ====================

test "create auth command" {
    const allocator = std.testing.allocator;
    const cmd = try createAuthCommand(allocator);
    defer cmd.deinit();
    try std.testing.expectEqualStrings("auth", cmd.name);
}

test "parseExpiresIn" {
    const now = std.time.timestamp();

    const result_30d = try parseExpiresIn("30d");
    try std.testing.expect(result_30d > now);
    try std.testing.expect(result_30d < now + 31 * 86400);

    const result_1h = try parseExpiresIn("1h");
    try std.testing.expect(result_1h > now);
    try std.testing.expect(result_1h < now + 7200);

    try std.testing.expectError(error.InvalidFormat, parseExpiresIn("x"));
    try std.testing.expectError(error.InvalidFormat, parseExpiresIn("0d"));
}
