//! Server management commands for Flo CLI using Commander framework
//!
//! Usage:
//!   flo server start [--config flo.toml] [--port 9000] [--data-dir ./data] [--shards N]
//!   flo server stop [--data-dir ./data] [--force]
//!   flo server status [--data-dir ./data]

const std = @import("std");
const stdx = @import("stdx");
const Allocator = std.mem.Allocator;
const commander = @import("../commander/mod.zig");
const server_config = @import("../../config/mod.zig").server;
const Runtime = @import("../../node/runtime.zig").Runtime;
const RuntimeConfig = @import("../../node/runtime.zig").RuntimeConfig;
const posix = std.posix;

/// Wrapper to cast *anyopaque to *Context
fn wrapHandler(comptime handler: fn (*commander.Context) commander.Error!void) commander.RunFn {
    return struct {
        fn run(ctx_ptr: *anyopaque) commander.Error!void {
            const ctx: *commander.Context = @ptrCast(@alignCast(ctx_ptr));
            return handler(ctx);
        }
    }.run;
}

/// Create the server command tree
pub fn createServerCommand(allocator: Allocator) !*commander.Command {
    return try commander.newBuilder(allocator)
        .name("server")
        .about("Server management commands")
        .group("Server Commands")
        .longAbout(
            \\Manage the Flo server lifecycle.
            \\
            \\The server command provides subcommands for starting, stopping,
            \\and managing the Flo server instance.
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("start")
                .about("Start the Flo server")
                .longAbout(
                    \\Start the Flo server with the specified configuration.
                    \\
                    \\Configuration priority (highest to lowest):
                    \\  1. Command-line flags
                    \\  2. flo.toml config file
                    \\  3. Built-in defaults
                    \\
                    \\Cluster mode:
                    \\  Without --join: Starts as single-node cluster (immediate leader)
                    \\  With --join:    Connects to existing cluster and requests membership
                    \\
                    \\Note: Shard count defines data topology and cannot be changed after
                    \\      initial data is written without running a rebalance operation.
                )
                .examples(&.{
                    "flo server start",
                    "flo server start --port 9000",
                    "flo server start --config /etc/flo/flo.toml",
                    "flo server start -p 9000 --data-dir /var/lib/flo",
                    "flo server start --join 192.168.1.10:9500",
                    "flo server start --join 192.168.1.10:9500,192.168.1.11:9500",
                })
                .stringFlag("config", 'c', "", "Path to flo.toml config file")
                .uintFlag("port", 'p', 0, "TCP port to listen on (default: 9000)")
                .stringFlag("data-dir", 'd', "", "Data directory for storage")
                .uintFlag("shards", 's', 0, "Number of data shards (0=auto)")
                .uintFlag("partitions", 0, 0, "Number of virtual partitions (0=auto: max(4096, shards×32))")
                .stringFlag("log-level", 'l', "", "Log level: debug, info, warn, error")
                .stringFlag("log-format", 0, "", "Log format: text, json")
                .uintFlag("threads", 't', 0, "Number of worker threads (0=auto)")
                .stringFlag("join", 'j', "", "Join existing cluster (host:port[,host:port,...])")
                .uintFlag("node-id", 'n', 0, "Node ID (0=auto-generate from hostname:port)")
                .uintFlag("raft-port", 0, 0, "Raft RPC port (default: listen_port + 500)")
                .uintFlag("gossip-port", 0, 0, "Gossip UDP port (default: listen_port + 600, 0=disabled)")
                .uintFlag("metrics-port", 0, 0, "Metrics HTTP port (default: listen_port + 1)")
                .uintFlag("dashboard-port", 0, 0, "Dashboard HTTP port (default: listen_port + 2)")
                .boolFlag("no-metrics", 0, "Disable metrics server")
                .boolFlag("no-dashboard", 0, "Disable web dashboard")
                .action(wrapHandler(runStart)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("stop")
                .about("Stop the Flo server")
                .stringFlag("data-dir", 'd', "", "Data directory (to find PID file)")
                .boolFlag("force", 'f', "Force immediate shutdown (SIGKILL)")
                .action(wrapHandler(runStop)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("status")
                .about("Show server process status")
                .stringFlag("data-dir", 'd', "", "Data directory (to find PID file)")
                .action(wrapHandler(runServerStatus)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("metrics")
                .about("Fetch server metrics (Prometheus format)")
                .examples(&.{
                    "flo server metrics",
                    "flo server metrics --endpoint localhost:9001",
                    "flo server metrics --format json",
                })
                .stringFlag("endpoint", 'e', "localhost:9001", "Metrics endpoint (host:port)")
                .stringFlag("format", 'f', "text", "Output format: text, json, prometheus")
                .action(wrapHandler(runMetrics)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("bootstrap")
                .about("Generate the root API key (one-time)")
                .longAbout(
                    \\Bootstrap server authentication by generating the root admin API key
                    \\and internal signing secret. This must be run once before any
                    \\authenticated operations.
                    \\
                    \\The root key is printed to stdout (or written to --out file).
                    \\It is NEVER stored in plaintext by Flo — save it immediately.
                    \\Bootstrap fails if already performed.
                )
                .examples(&.{
                    "flo server bootstrap",
                    "flo server bootstrap --out flo.key",
                    "flo server bootstrap --data-dir /var/lib/flo",
                })
                .stringFlag("data-dir", 'd', "", "Data directory (to find bootstrap state)")
                .stringFlag("out", 'o', "", "Write root key to file instead of stdout")
                .action(wrapHandler(runBootstrap)),
        )
        .build();
}

// Signal handling
var shutdown_requested = std.atomic.Value(bool).init(false);

fn setupSignalHandlers() void {
    const handler = struct {
        fn handle(_: c_int) callconv(.c) void {
            shutdown_requested.store(true, .release);
        }
    }.handle;

    const act = std.posix.Sigaction{
        .handler = .{ .handler = handler },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };

    std.posix.sigaction(std.posix.SIG.INT, &act, null);
    std.posix.sigaction(std.posix.SIG.TERM, &act, null);

    // Ignore SIGPIPE - we handle broken pipes via error returns from write()
    // Without this, writing to a socket whose peer has closed will kill the process
    const ignore_act = std.posix.Sigaction{
        .handler = .{ .handler = std.posix.SIG.IGN },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.PIPE, &ignore_act, null);

    // DEBUG: Add handlers for crash signals to see what kills the process
    const crash_handler = struct {
        fn handle(sig: c_int) callconv(.c) noreturn {
            const pid = std.c.getpid();
            const sig_name = switch (sig) {
                std.posix.SIG.SEGV => "SIGSEGV",
                std.posix.SIG.BUS => "SIGBUS",
                std.posix.SIG.ABRT => "SIGABRT",
                std.posix.SIG.ILL => "SIGILL",
                std.posix.SIG.FPE => "SIGFPE",
                std.posix.SIG.HUP => "SIGHUP",
                else => "UNKNOWN",
            };
            std.debug.print("\n=== CRASH SIGNAL {s} (sig={d}) received by PID {d} ===\n", .{ sig_name, sig, pid });
            // Re-raise the signal to get core dump/default behavior
            const default_act = std.posix.Sigaction{
                .handler = .{ .handler = std.posix.SIG.DFL },
                .mask = std.posix.sigemptyset(),
                .flags = 0,
            };
            std.posix.sigaction(@intCast(sig), &default_act, null);
            _ = std.posix.raise(@intCast(sig)) catch {};
            // If raise failed, just exit
            std.posix.exit(128 + @as(u8, @intCast(sig)));
        }
    }.handle;

    const crash_act = std.posix.Sigaction{
        .handler = .{ .handler = crash_handler },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };

    std.posix.sigaction(std.posix.SIG.SEGV, &crash_act, null);
    std.posix.sigaction(std.posix.SIG.BUS, &crash_act, null);
    std.posix.sigaction(std.posix.SIG.ABRT, &crash_act, null);
    std.posix.sigaction(std.posix.SIG.ILL, &crash_act, null);
    std.posix.sigaction(std.posix.SIG.FPE, &crash_act, null);
    std.posix.sigaction(std.posix.SIG.HUP, &crash_act, null);
}

/// Expand ~ to home directory in path
fn expandTilde(allocator: Allocator, path: []const u8) ![]const u8 {
    if (path.len > 0 and path[0] == '~') {
        const home = std.posix.getenv("HOME") orelse return error.NoHomeDirectory;
        if (path.len == 1) {
            return try allocator.dupe(u8, home);
        }
        if (path[1] == '/') {
            return try std.fmt.allocPrint(allocator, "{s}{s}", .{ home, path[1..] });
        }
    }
    return try allocator.dupe(u8, path);
}

// PID file management
const PID_FILENAME = "flo.pid";

fn getPidFilePath(allocator: Allocator, data_dir: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{s}/{s}", .{ data_dir, PID_FILENAME });
}

fn writePidFile(data_dir: []const u8) !void {
    const pid = std.c.getpid();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const pid_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ data_dir, PID_FILENAME }) catch return error.PathTooLong;

    const file = std.fs.cwd().createFile(pid_path, .{}) catch |err| {
        std.log.err("Failed to create PID file: {}", .{err});
        return err;
    };
    defer file.close();

    var buf: [20]u8 = undefined;
    const pid_str = std.fmt.bufPrint(&buf, "{d}", .{pid}) catch return error.InvalidPid;
    file.writeAll(pid_str) catch |err| {
        std.log.err("Failed to write PID file: {}", .{err});
        return err;
    };
}

fn readPidFile(allocator: Allocator, data_dir: []const u8) !?posix.pid_t {
    const pid_path = try getPidFilePath(allocator, data_dir);
    defer allocator.free(pid_path);

    const file = std.fs.cwd().openFile(pid_path, .{}) catch |err| {
        if (err == error.FileNotFound) return null;
        return err;
    };
    defer file.close();

    var buf: [20]u8 = undefined;
    const len = file.readAll(&buf) catch return null;
    if (len == 0) return null;

    const pid_str = std.mem.trimRight(u8, buf[0..len], &[_]u8{ '\n', '\r', ' ' });
    return std.fmt.parseInt(posix.pid_t, pid_str, 10) catch null;
}

fn removePidFile(data_dir: []const u8) void {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const pid_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ data_dir, PID_FILENAME }) catch return;
    std.fs.cwd().deleteFile(pid_path) catch {};
}

fn isProcessRunning(pid: posix.pid_t) bool {
    // Send signal 0 to check if process exists
    posix.kill(pid, 0) catch |err| {
        // ESRCH means process doesn't exist
        return err != error.NoSuchProcess;
    };
    return true; // No error means process exists
}

fn runStart(ctx: *commander.Context) commander.Error!void {
    const allocator = ctx.allocator;

    // Get flag values (convert to appropriate types)
    const config_path = ctx.getString("config");
    const port = stdx.nullIfZero(u16, ctx.getUint16("port"));
    const data_dir = ctx.getString("data-dir");
    const shards = ctx.getChangedUint16("shards");
    const partitions = ctx.getChangedUint("partitions");
    const log_level = ctx.getString("log-level");
    const log_format = ctx.getString("log-format");

    // Cluster flags
    const join_addrs = ctx.getString("join");
    const node_id_override = ctx.getChangedUint("node-id");
    const raft_port_override = ctx.getChangedUint16("raft-port");
    const gossip_port_override = ctx.getChangedUint16("gossip-port");

    // Metrics and dashboard flags
    const metrics_port_override = ctx.getChangedUint16("metrics-port");
    const dashboard_port_override = ctx.getChangedUint16("dashboard-port");
    const no_metrics = ctx.getBool("no-metrics");
    const no_dashboard = ctx.getBool("no-dashboard");

    // Load configuration with CLI overrides
    var config = server_config.loadWithOverrides(
        allocator,
        stdx.nullIfEmpty(u8, config_path),
        port,
        stdx.nullIfEmpty(u8, data_dir),
        shards,
        if (partitions) |p| @as(u32, @intCast(p)) else null,
        stdx.nullIfEmpty(u8, log_level),
        stdx.nullIfEmpty(u8, log_format),
    ) catch |err| {
        ctx.printErr("Error loading configuration: {}\n", .{err});
        return error.CommandFailed;
    };
    defer config.deinit();

    // Apply cluster CLI overrides
    if (node_id_override) |nid| {
        config.cluster.node_id = nid;
    }
    if (raft_port_override) |rp| {
        config.cluster.raft_port = rp;
    }
    if (gossip_port_override) |gp| {
        config.cluster.gossip_port = gp;
    }
    if (metrics_port_override) |mp| {
        config.metrics.port = mp;
    }
    if (dashboard_port_override) |dp| {
        config.dashboard.port = dp;
    }
    if (no_metrics) {
        config.metrics.enabled = false;
    }
    if (no_dashboard) {
        config.dashboard.enabled = false;
    }

    // --join flag overrides seeds from config
    var join_seeds_list: std.ArrayList([]const u8) = .empty;
    defer join_seeds_list.deinit(allocator);
    if (join_addrs) |addrs| {
        if (addrs.len > 0) {
            // Parse comma-separated addresses
            var iter = std.mem.splitScalar(u8, addrs, ',');
            while (iter.next()) |addr| {
                const trimmed = std.mem.trim(u8, addr, " \t");
                if (trimmed.len > 0) {
                    const owned = allocator.dupe(u8, trimmed) catch {
                        ctx.printErr("Error parsing --join addresses\n", .{});
                        return error.CommandFailed;
                    };
                    join_seeds_list.append(allocator, owned) catch {
                        ctx.printErr("Error parsing --join addresses\n", .{});
                        return error.CommandFailed;
                    };
                }
            }
            // Replace config seeds with CLI seeds
            config.cluster.seeds = join_seeds_list.items;
        }
    }

    // Apply log configuration
    const root = @import("root");
    root.log.configure(.{
        .level = switch (config.log_level) {
            .debug => .debug,
            .info => .info,
            .warn => .warn,
            .err => .err,
        },
        .format = switch (config.log_format) {
            .text => .text,
            .json => .json,
        },
    });

    // Expand ~ in data_dir path
    const expanded_data_dir = expandTilde(allocator, config.data_dir) catch |err| {
        ctx.printErr("Error expanding data directory path: {}\n", .{err});
        return error.CommandFailed;
    };
    defer allocator.free(expanded_data_dir);

    // Print startup banner
    ctx.print("\n", .{});
    ctx.print("  ╔═══════════════════════════════════════╗\n", .{});
    ctx.print("  ║             FLO SERVER                ║\n", .{});
    ctx.print("  ╚═══════════════════════════════════════╝\n", .{});
    ctx.print("\n", .{});
    ctx.print("  Port:       {d}\n", .{config.port});
    ctx.print("  Bind:       {s}\n", .{config.bind});
    ctx.print("  Data dir:   {s}\n", .{expanded_data_dir});
    if (config.shards == 0) {
        const cpu_count = std.Thread.getCpuCount() catch 1;
        ctx.print("  Shards:     auto ({d} CPUs)\n", .{cpu_count});
    } else {
        ctx.print("  Shards:     {d}\n", .{config.shards});
    }
    if (config.partition_count == 0) {
        ctx.print("  Partitions: auto (max(4096, shards × 32))\n", .{});
    } else {
        ctx.print("  Partitions: {d}\n", .{config.partition_count});
    }
    ctx.print("  Log level:  {s}\n", .{@tagName(config.log_level)});
    ctx.print("  Durability: {s}\n", .{@tagName(config.durability)});

    // Print cluster info
    ctx.print("\n", .{});
    ctx.print("  Cluster:\n", .{});
    if (config.cluster.node_id == 0) {
        ctx.print("    Node ID:    auto (will generate on start)\n", .{});
    } else {
        ctx.print("    Node ID:    {d}\n", .{config.cluster.node_id});
    }
    ctx.print("    Raft port:  {d}\n", .{config.cluster.raft_port});
    if (config.cluster.gossip_port > 0) {
        ctx.print("    Gossip:     {d}\n", .{config.cluster.gossip_port});
    }
    if (config.cluster.seeds.len > 0) {
        ctx.print("    Join:       {s}", .{config.cluster.seeds[0]});
        for (config.cluster.seeds[1..]) |seed| {
            ctx.print(",{s}", .{seed});
        }
        ctx.print("\n", .{});
    } else {
        ctx.print("    Mode:       Single-node (no seeds)\n", .{});
    }
    ctx.print("\n", .{});

    // Ensure data directory exists
    std.fs.cwd().makePath(expanded_data_dir) catch |err| {
        if (err != error.PathAlreadyExists) {
            ctx.printErr("Error creating data directory '{s}': {}\n", .{ expanded_data_dir, err });
            return error.CommandFailed;
        }
    };

    // Convert to RuntimeConfig with expanded path
    var runtime_config = config.toRuntimeConfig();
    runtime_config.data_dir = expanded_data_dir;

    // Initialize runtime
    ctx.print("Starting server...\n", .{});

    // Set up signal handlers BEFORE runtime init
    // This ensures SIGPIPE is ignored before any threads start writing to sockets
    setupSignalHandlers();

    var runtime = Runtime.init(allocator, runtime_config) catch |err| {
        ctx.printErr("Error initializing runtime: {}\n", .{err});
        return error.CommandFailed;
    };
    defer runtime.deinit();

    // Start the runtime
    runtime.start() catch |err| {
        ctx.printErr("Error starting runtime: {}\n", .{err});
        return error.CommandFailed;
    };

    ctx.print("\n", .{});
    ctx.print("Flo server ready on {s}:{d}\n", .{ config.bind, config.port });
    ctx.print("Press Ctrl+C to stop.\n", .{});
    ctx.print("\n", .{});

    // Write PID file for server stop/status commands
    writePidFile(expanded_data_dir) catch |err| {
        ctx.printErr("Warning: Could not write PID file: {}\n", .{err});
    };
    defer removePidFile(expanded_data_dir);

    // Wait for shutdown signal
    while (!shutdown_requested.load(.acquire)) {
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }

    ctx.print("\nShutting down...\n", .{});

    // Stop runtime (signals cores to stop and waits for threads)
    runtime.stop();

    ctx.print("Server stopped.\n", .{});
}

fn getDataDir(ctx: *commander.Context) []const u8 {
    if (ctx.getString("data-dir")) |dir| {
        if (dir.len > 0) return dir;
    }
    return "~/.flo/data"; // Default
}

fn runStop(ctx: *commander.Context) commander.Error!void {
    const allocator = ctx.allocator;
    const force = ctx.getBool("force");
    const data_dir_raw = getDataDir(ctx);

    const data_dir = expandTilde(allocator, data_dir_raw) catch |err| {
        ctx.printErr("Error expanding data directory path: {}\n", .{err});
        return error.CommandFailed;
    };
    defer allocator.free(data_dir);

    const pid = readPidFile(allocator, data_dir) catch |err| {
        ctx.printErr("Error reading PID file: {}\n", .{err});
        return error.CommandFailed;
    };

    if (pid == null) {
        ctx.printErr("No server running (PID file not found in {s})\n", .{data_dir});
        return error.CommandFailed;
    }

    const server_pid = pid.?;
    if (!isProcessRunning(server_pid)) {
        ctx.print("Server not running (stale PID file, PID {d})\n", .{server_pid});
        removePidFile(data_dir);
        return;
    }

    // Send signal to stop server
    const sig: u6 = if (force) posix.SIG.KILL else posix.SIG.TERM;
    const sig_name: []const u8 = if (force) "SIGKILL" else "SIGTERM";

    ctx.print("Sending {s} to server (PID {d})...\n", .{ sig_name, server_pid });

    _ = posix.kill(server_pid, sig) catch |err| {
        ctx.printErr("Failed to send signal: {}\n", .{err});
        return error.CommandFailed;
    };

    if (!force) {
        // Wait for graceful shutdown (up to 30 seconds)
        ctx.print("Waiting for server to stop...\n", .{});
        var waited: u32 = 0;
        while (waited < 300) : (waited += 1) {
            std.Thread.sleep(100 * std.time.ns_per_ms);
            if (!isProcessRunning(server_pid)) {
                ctx.print("Server stopped.\n", .{});
                return;
            }
        }
        ctx.printErr("Server did not stop within 30 seconds. Use --force to kill immediately.\n", .{});
        return error.CommandFailed;
    } else {
        ctx.print("Server killed.\n", .{});
    }
}

fn runServerStatus(ctx: *commander.Context) commander.Error!void {
    const allocator = ctx.allocator;
    const data_dir_raw = getDataDir(ctx);

    const data_dir = expandTilde(allocator, data_dir_raw) catch |err| {
        ctx.printErr("Error expanding data directory path: {}\n", .{err});
        return error.CommandFailed;
    };
    defer allocator.free(data_dir);

    const pid = readPidFile(allocator, data_dir) catch |err| {
        ctx.printErr("Error reading PID file: {}\n", .{err});
        return error.CommandFailed;
    };

    // Header
    ctx.print("\n╔══════════════════════════════════════════╗\n", .{});
    ctx.print("║            Flo Server Status             ║\n", .{});
    ctx.print("╚══════════════════════════════════════════╝\n\n", .{});

    if (pid == null) {
        ctx.print("Server:   NOT RUNNING\n", .{});
        ctx.print("  (No PID file found in {s})\n", .{data_dir});
        return;
    }

    const server_pid = pid.?;
    if (isProcessRunning(server_pid)) {
        ctx.print("Server:   RUNNING\n", .{});
        ctx.print("  PID:        {d}\n", .{server_pid});
        ctx.print("  Data dir:   {s}\n", .{data_dir});

        // Try to get shard count from topology manifest
        const manifest_path = std.fmt.allocPrint(allocator, "{s}/topology.json", .{data_dir}) catch {
            ctx.print("  Shards:     (unknown)\n", .{});
            return;
        };
        defer allocator.free(manifest_path);

        if (std.fs.cwd().openFile(manifest_path, .{})) |file| {
            defer file.close();
            var buf: [256]u8 = undefined;
            const bytes_read = file.readAll(&buf) catch 0;
            if (bytes_read > 0) {
                // Simple parse for shard_count - look for "shard_count":
                const content = buf[0..bytes_read];
                if (std.mem.indexOf(u8, content, "\"shard_count\":")) |idx| {
                    const start = idx + 14; // Length of "shard_count":
                    var end = start;
                    while (end < content.len and (content[end] >= '0' and content[end] <= '9')) : (end += 1) {}
                    if (end > start) {
                        const shard_count = std.fmt.parseInt(u16, content[start..end], 10) catch 0;
                        if (shard_count > 0) {
                            ctx.print("  Shards:     {d}\n", .{shard_count});
                        }
                    }
                }
            }
        } else |_| {
            // No topology file yet - that's fine for first run
        }

        ctx.print("\nTip: Use 'flo status' for health check, 'flo cluster status' for cluster info\n", .{});
    } else {
        ctx.print("Server:   NOT RUNNING (stale PID file)\n", .{});
        ctx.print("  Last PID: {d}\n", .{server_pid});
        ctx.print("  Data dir: {s}\n", .{data_dir});
    }
}

fn runMetrics(ctx: *commander.Context) commander.Error!void {
    const endpoint = ctx.getString("endpoint") orelse "localhost:9001";
    const format = ctx.getString("format") orelse "text";

    // Parse host:port
    const colon_pos = std.mem.indexOfScalar(u8, endpoint, ':');
    const host = if (colon_pos) |pos| endpoint[0..pos] else endpoint;
    const port_str = if (colon_pos) |pos| endpoint[pos + 1 ..] else "9001";
    const port = std.fmt.parseInt(u16, port_str, 10) catch 9001;

    // Resolve localhost to 127.0.0.1
    const resolved_host = if (std.mem.eql(u8, host, "localhost")) "127.0.0.1" else host;

    ctx.print("Fetching metrics from {s}:{d}...\n", .{ resolved_host, port });

    // Make HTTP request to /metrics endpoint
    const address = std.net.Address.parseIp4(resolved_host, port) catch {
        ctx.printErr("Error: Invalid address: {s}\n", .{resolved_host});
        return error.CommandFailed;
    };

    const stream = std.net.tcpConnectToAddress(address) catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        ctx.printErr("Is the Flo metrics server running at {s}:{d}?\n", .{ resolved_host, port });
        return error.CommandFailed;
    };
    defer stream.close();

    // Send HTTP GET request
    var request_buf: [256]u8 = undefined;
    const request = std.fmt.bufPrint(&request_buf, "GET /metrics HTTP/1.1\r\nHost: {s}\r\nConnection: close\r\n\r\n", .{resolved_host}) catch {
        ctx.printErr("Error: Request buffer too small\n", .{});
        return error.CommandFailed;
    };
    _ = stream.write(request) catch |err| {
        ctx.printErr("Write failed: {}\n", .{err});
        return error.CommandFailed;
    };

    // Read response
    var buf: [8192]u8 = undefined;
    var total: usize = 0;
    while (true) {
        const n = stream.read(buf[total..]) catch |err| {
            ctx.printErr("Read failed: {}\n", .{err});
            return error.CommandFailed;
        };
        if (n == 0) break;
        total += n;
        if (total >= buf.len) break;
    }

    // Skip HTTP headers and print body
    if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n")) |body_start| {
        const body = buf[body_start + 4 .. total];
        if (std.mem.eql(u8, format, "json")) {
            ctx.print("{{\"metrics\": \"{s}\"}}\n", .{body});
        } else {
            ctx.print("{s}\n", .{body});
        }
    } else {
        ctx.print("{s}\n", .{buf[0..total]});
    }
}

// ==================== Bootstrap ====================

const auth_keys = @import("../../auth/keys.zig");
const auth_store = @import("../../auth/store.zig");

const BOOTSTRAP_FILENAME = "auth.bootstrap";

fn runBootstrap(ctx: *commander.Context) commander.Error!void {
    const allocator = ctx.allocator;
    const data_dir_raw = ctx.getString("data-dir");
    const out_path = ctx.getString("out");

    // Resolve data directory
    const data_dir = blk: {
        if (data_dir_raw) |dd| {
            if (dd.len > 0) break :blk expandTilde(allocator, dd) catch {
                ctx.printErr("Error: cannot expand data-dir path\n", .{});
                return error.CommandFailed;
            };
        }
        break :blk allocator.dupe(u8, "./data") catch return error.CommandFailed;
    };
    defer allocator.free(data_dir);

    // Ensure data directory exists
    std.fs.cwd().makePath(data_dir) catch |err| {
        ctx.printErr("Error creating data directory: {}\n", .{err});
        return error.CommandFailed;
    };

    // Check if already bootstrapped
    var marker_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const marker_path = std.fmt.bufPrint(&marker_path_buf, "{s}/{s}", .{ data_dir, BOOTSTRAP_FILENAME }) catch {
        ctx.printErr("Error: path too long\n", .{});
        return error.CommandFailed;
    };

    if (std.fs.cwd().access(marker_path, .{})) |_| {
        ctx.printErr("Error: server already bootstrapped\n", .{});
        ctx.printErr("  Marker file: {s}\n", .{marker_path});
        ctx.printErr("  Bootstrap can only be performed once.\n", .{});
        return error.CommandFailed;
    } else |_| {
        // File doesn't exist — good, we can proceed
    }

    // Generate root key + signing secret
    var store = auth_store.KeyStore.init(allocator);
    defer store.deinit();

    const root_key = store.bootstrap() catch |err| {
        ctx.printErr("Error during bootstrap: {}\n", .{err});
        return error.CommandFailed;
    };
    defer allocator.free(root_key);

    // Persist signing secret alongside the marker
    const signing_secret = store.getSigningSecret() orelse {
        ctx.printErr("Error: signing secret not generated\n", .{});
        return error.CommandFailed;
    };

    // Write the key hash + signing secret to the bootstrap marker file
    // Format: first 32 bytes = key hash, next 32 bytes = signing secret
    const keys_list = store.listKeys(allocator) catch {
        ctx.printErr("Error: cannot list keys\n", .{});
        return error.CommandFailed;
    };
    defer allocator.free(keys_list);

    const marker_file = std.fs.cwd().createFile(marker_path, .{ .exclusive = true }) catch |err| {
        ctx.printErr("Error creating bootstrap marker: {}\n", .{err});
        return error.CommandFailed;
    };
    defer marker_file.close();

    // Write serialized key + signing secret
    if (keys_list.len > 0) {
        var key_buf: [auth_keys.ApiKey.serialized_size]u8 = undefined;
        const key_data = keys_list[0].serialize(&key_buf) catch {
            ctx.printErr("Error serializing key\n", .{});
            return error.CommandFailed;
        };
        marker_file.writeAll(key_data) catch |err| {
            ctx.printErr("Error writing bootstrap data: {}\n", .{err});
            return error.CommandFailed;
        };
    }
    marker_file.writeAll(signing_secret) catch |err| {
        ctx.printErr("Error writing signing secret: {}\n", .{err});
        return error.CommandFailed;
    };

    // Output the root key
    if (out_path) |op| {
        if (op.len > 0) {
            const expanded = expandTilde(allocator, op) catch {
                ctx.printErr("Error: cannot expand output path\n", .{});
                return error.CommandFailed;
            };
            defer allocator.free(expanded);

            const out_file = std.fs.cwd().createFile(expanded, .{ .exclusive = true }) catch |err| {
                ctx.printErr("Error creating output file '{s}': {}\n", .{ expanded, err });
                return error.CommandFailed;
            };
            defer out_file.close();
            out_file.writeAll(root_key) catch |err| {
                ctx.printErr("Error writing key: {}\n", .{err});
                return error.CommandFailed;
            };
            out_file.writeAll("\n") catch {};

            ctx.print("Bootstrap complete.\n", .{});
            ctx.print("  Root key written to: {s}\n", .{expanded});
            ctx.print("  WARNING: Save this key — it cannot be retrieved again.\n", .{});
            return;
        }
    }

    // Print to stdout
    ctx.print("{s}\n", .{root_key});
}

// ==================== Testing ====================

test "create server command" {
    const allocator = std.testing.allocator;

    const cmd = try createServerCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("server", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 2);
}
