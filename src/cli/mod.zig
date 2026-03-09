//! CLI Commander - Flo CLI using Commander framework
//!
//! A comprehensive CLI with grouped commands, examples, and configuration sections.

const std = @import("std");
const commander = @import("commander/mod.zig");
const commands = @import("commands/mod.zig");

// Re-export useful modules
pub const output = @import("output.zig");
pub const client = @import("client/mod.zig");
pub const toml = @import("toml.zig");

// Client config for context management
const cli_config = @import("config.zig");
// Server config for flo.toml generation
const server_config = @import("../config/server.zig");

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Run the CLI
pub fn run(allocator: std.mem.Allocator, args: []const []const u8) !void {
    _ = args; // We'll use process args directly

    // Create commands from dedicated modules
    const server = try commands.createServerCommand(allocator);
    const cluster = try commands.createClusterCommand(allocator);
    const status = try commands.createStatusCommand(allocator);
    const kv = try commands.createKvCommand(allocator);
    const queue = try commands.createQueueCommand(allocator);
    const stream = try commands.createStreamCommand(allocator);
    const action = try commands.createActionCommand(allocator);
    const worker = try commands.createWorkerCommand(allocator);
    const repl = try commands.createReplCommand(allocator);
    const namespace = try commands.createNamespaceCommand(allocator);
    const workflow = try commands.createWorkflowCommand(allocator);
    const processing = try commands.createProcessingCommand(allocator);
    const ts = try commands.createTsCommand(allocator);
    const auth = try commands.createAuthCommand(allocator);

    var root = try commander.newBuilder(allocator)
        .name("flo")
        .about("Flo - High-performance distributed platform")
        .usage("flo <command> [args...]")
        .version("0.1.0")
        .examples(&.{
            "flo server start --port 9000 --data-dir ./data",
            "flo status --endpoint localhost:9000",
            "flo kv set mykey \"hello world\"",
            "flo kv get mykey --format json",
            "flo stream append events \"user clicked\" \"page loaded\"",
            "flo stream read events --limit 10",
            "flo cluster status",
        })
        .helpSections(&.{
            .{
                .title = "Configuration",
                .content = "Server config: ./flo.toml\nClient config: ~/.flo/config.json",
            },
        })
        .flag("verbose", .{ .short = 'v', .desc = "Enable verbose output", .persistent = true })

        // Server Commands (pre-built)
        .addCommand(server)

        // Status (health check)
        .addCommand(status)

        // Cluster Commands (pre-built)
        .addCommand(cluster)

        // Data Commands (pre-built)
        .addCommand(kv)
        .addCommand(queue)
        .addCommand(stream)
        .addCommand(action)
        .addCommand(workflow)
        .addCommand(processing)
        .addCommand(ts)
        .addCommand(worker)
        .addCommand(repl)

        // Admin Commands (pre-built)
        .addCommand(namespace)

        // Auth Commands (pre-built)
        .addCommand(auth)

        // Config Commands
        .subcommand(
            commander.newBuilder(allocator)
                .name("config")
                .about("Configuration management")
                .group("CONFIG COMMANDS")
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("show")
                        .about("Show current configuration")
                        .action(wrapHandler(configShow)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("set-context")
                        .about("Create or update a context")
                        .arg("name", "Context name")
                        .stringFlag("endpoint", 'e', "", "Server endpoint (host:port)")
                        .action(wrapHandler(configSetContext)),
                )
                .subcommand(
                    commander.newBuilder(allocator)
                        .name("use-context")
                        .about("Switch to a different context")
                        .arg("name", "Context name to use")
                        .action(wrapHandler(configUseContext)),
                )
                .subcommand(
                commander.newBuilder(allocator)
                    .name("init")
                    .about("Generate default flo.toml")
                    .action(wrapHandler(configInit)),
            ),
        )

        // Other Commands
        .subcommand(
            commander.newBuilder(allocator)
                .name("version")
                .about("Show version information")
                .group("Other Commands")
                .action(wrapHandler(showVersion)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("help")
                .about("Show this help message")
                .group("Other Commands")
                .action(wrapHandler(showHelp)),
        )
        .build();
    defer root.deinit();

    // Collect args into a slice (include program name - execute() handles skipping it)
    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    // Execute
    root.execute(argv) catch |err| {
        switch (err) {
            error.HelpRequested, error.VersionRequested => {},
            error.CommandFailed => {
                // Command already printed its own error message, just exit
                std.process.exit(1);
            },
            else => {
                var stderr_buf: [512]u8 = undefined;
                var stderr_writer = std.fs.File.stderr().writer(&stderr_buf);
                stderr_writer.interface.print("Error: {}\n", .{err}) catch {};
                stderr_writer.interface.flush() catch {};
                std.process.exit(1);
            },
        }
    };
}

// ==================== Command Handlers ====================

fn configShow(ctx: *commander.Context) commander.Error!void {
    var cfg = cli_config.Config.load(ctx.allocator) catch |err| {
        ctx.printErr("Error loading config: {}\n", .{err});
        return error.CommandFailed;
    };
    defer cfg.deinit();

    // Print current context
    ctx.print("Current context: {s}\n\n", .{cfg.current_context});
    ctx.print("Contexts:\n", .{});

    // Iterate over contexts
    var it = cfg.contexts.iterator();
    while (it.next()) |entry| {
        const marker: []const u8 = if (std.mem.eql(u8, entry.key_ptr.*, cfg.current_context)) "*" else " ";
        ctx.print("  {s} {s}: {s}", .{ marker, entry.key_ptr.*, entry.value_ptr.endpoint });
        if (entry.value_ptr.namespace) |ns| {
            ctx.print(" (namespace: {s})", .{ns});
        }
        ctx.print("\n", .{});
    }
}

fn configSetContext(ctx: *commander.Context) commander.Error!void {
    if (ctx.args.len < 1) {
        ctx.printErr("Error: missing context name\n", .{});
        ctx.printErr("Usage: flo config set-context <name> --endpoint <host:port>\n", .{});
        return error.InvalidArgs;
    }

    const name = ctx.args[0];
    const endpoint = ctx.getString("endpoint") orelse {
        ctx.printErr("Error: --endpoint is required\n", .{});
        ctx.printErr("Usage: flo config set-context <name> --endpoint <host:port>\n", .{});
        return error.InvalidArgs;
    };

    if (endpoint.len == 0) {
        ctx.printErr("Error: --endpoint is required\n", .{});
        ctx.printErr("Usage: flo config set-context <name> --endpoint <host:port>\n", .{});
        return error.InvalidArgs;
    }

    var cfg = cli_config.Config.load(ctx.allocator) catch |err| {
        ctx.printErr("Error loading config: {}\n", .{err});
        return error.CommandFailed;
    };
    defer cfg.deinit();

    cfg.setContext(name, endpoint, null) catch |err| {
        ctx.printErr("Error setting context: {}\n", .{err});
        return error.CommandFailed;
    };

    cfg.save() catch |err| {
        ctx.printErr("Error saving config: {}\n", .{err});
        return error.CommandFailed;
    };

    ctx.print("Context '{s}' created with endpoint {s}\n", .{ name, endpoint });
}

fn configUseContext(ctx: *commander.Context) commander.Error!void {
    if (ctx.args.len < 1) {
        ctx.printErr("Error: missing context name\n", .{});
        ctx.printErr("Usage: flo config use-context <name>\n", .{});
        return error.InvalidArgs;
    }

    const name = ctx.args[0];

    var cfg = cli_config.Config.load(ctx.allocator) catch |err| {
        ctx.printErr("Error loading config: {}\n", .{err});
        return error.CommandFailed;
    };
    defer cfg.deinit();

    cfg.useContext(name) catch |err| {
        if (err == error.ContextNotFound) {
            ctx.printErr("Error: Context '{s}' not found\n", .{name});
            ctx.printErr("Use 'flo config show' to see available contexts\n", .{});
            return error.CommandFailed;
        }
        ctx.printErr("Error switching context: {}\n", .{err});
        return error.CommandFailed;
    };

    cfg.save() catch |err| {
        ctx.printErr("Error saving config: {}\n", .{err});
        return error.CommandFailed;
    };

    ctx.print("Switched to context '{s}'\n", .{name});
}

fn configInit(ctx: *commander.Context) commander.Error!void {
    // Generate default flo.toml
    const default_config = server_config.generateDefaultConfig();

    const file = std.fs.cwd().createFile("flo.toml", .{ .exclusive = true }) catch |err| {
        if (err == error.PathAlreadyExists) {
            ctx.printErr("Error: flo.toml already exists\n", .{});
            return error.CommandFailed;
        }
        ctx.printErr("Error creating flo.toml: {}\n", .{err});
        return error.CommandFailed;
    };
    defer file.close();

    file.writeAll(default_config) catch |err| {
        ctx.printErr("Error writing flo.toml: {}\n", .{err});
        return error.CommandFailed;
    };

    ctx.print("Created flo.toml with default configuration\n", .{});
}

fn showVersion(ctx: *commander.Context) commander.Error!void {
    ctx.print("flo version 0.1.0\n", .{});
}

fn showHelp(ctx: *commander.Context) commander.Error!void {
    // Print root command help
    if (ctx.command.parent) |parent| {
        var root = parent;
        while (root.parent) |p| {
            root = p;
        }
        root.printHelp();
    } else {
        ctx.command.printHelp();
    }
}
