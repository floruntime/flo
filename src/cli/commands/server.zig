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
const outcome = @import("../outcome.zig");
const server_config = @import("../../config/mod.zig").server;
const cluster_config = @import("../../config/cluster.zig");
const Runtime = @import("../../node/runtime.zig").Runtime;
const runtime_mod = @import("../../node/runtime.zig");
const offline = @import("../../node/offline.zig");
const membership = @import("../../raft/membership.zig");
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
                    \\  Neither flag:  a single node; nothing to configure
                    \\  --cluster:     the first member; it leads alone until others join
                    \\  --join host:port[,...]: joins the members it can reach and is added by the leader
                    \\  Restart a member with --join, never --cluster: a member that lost its
                    \\  data and starts with --cluster founds a second cluster. With --join it
                    \\  rejoins and votes only once it has caught up; if a majority lost theirs,
                    \\  see `flo server inspect` and `flo server force-members`.
                    \\  Every member proves the same shared secret at the peer port:
                    \\  make one with `flo server secret`, then set it in [cluster] secret,
                    \\  [cluster] secret_file (mode 600) or FLO_CLUSTER_SECRET.
                    \\  A cluster replicates one shard: leave --shards at its default, or set 1.
                    \\
                    \\Note: Shard count defines data topology and cannot be changed after
                    \\      initial data is written without running a rebalance operation.
                )
                .examples(&.{
                    "flo server start",
                    "flo server start --port 9000",
                    "flo server start --config /etc/flo/flo.toml",
                    "flo server start -p 9000 --data-dir /var/lib/flo",
                    "flo server start --durability sync --data-dir /var/lib/flo",
                    "flo server start --cluster",
                    "flo server start --join 192.168.1.10:9500",
                    "flo server start --join 192.168.1.10:9500,192.168.1.11:9500",
                })
                .stringFlag("config", 'c', "", "Path to flo.toml config file")
                .uintFlag("port", 'p', 0, "TCP port to listen on (default: 9000)")
                .stringFlag("data-dir", 'd', "", "Data directory for storage")
                .stringFlag("durability", 0, "", "Storage durability: sync, async_flush, ephemeral")
                .uintFlag("shards", 's', 0, "Number of data shards (0=auto)")
                .uintFlag("partitions", 0, 0, "Number of virtual partitions, at most 16384 (0=auto: max(4096, shards×32))")
                .stringFlag("log-level", 'l', "", "Log level: debug, info, warn, error")
                .stringFlag("log-format", 0, "", "Log format: text, json")
                .uintFlag("threads", 't', 0, "Number of worker threads (0=auto)")
                .stringFlag("bind", 0, "", "Address to listen on (default: 0.0.0.0)")
                .boolFlag("cluster", 0, "Start as the first member of a cluster: listen for peers, lead a group of one until others join")
                .stringFlag("join", 'j', "", "Join existing cluster (host:port[,host:port,...])")
                .uintFlag("node-id", 'n', 0, "Node ID (0=auto-generate from hostname:port)")
                .uintFlag("raft-port", 0, 0, "Peer port, with --cluster or --join (default: listen_port + 500)")
                .uintFlag("metrics-port", 0, 0, "Metrics HTTP port (default: listen_port + 1)")
                .uintFlag("dashboard-port", 0, 0, "Dashboard HTTP port (default: listen_port + 2)")
                .boolFlag("no-metrics", 0, "Disable metrics server")
                .boolFlag("no-dashboard", 0, "Disable web dashboard")
                .action(wrapHandler(runStart)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("secret")
                .about("Print a new cluster secret")
                .longAbout(
                    \\Print a new secret for the peer port, from 32 random bytes. Give
                    \\every member the same one: in [cluster] secret, in a file named
                    \\by [cluster] secret_file (mode 600), or in FLO_CLUSTER_SECRET.
                    \\A cluster starts only with a secret in this form.
                )
                .examples(&.{
                    "flo server secret",
                    "flo server secret > /etc/flo/cluster.secret && chmod 600 /etc/flo/cluster.secret",
                })
                .action(wrapHandler(runSecret)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("inspect")
                .about("Show a stopped node's log, term and members")
                .longAbout(
                    \\Read the cluster group's log and hard state from a stopped node's
                    \\data directory: last index and term, what is committed, the latest
                    \\member list, and whether its lost-log guard is still on. Writes
                    \\nothing. When a majority has lost its data, run it on each survivor
                    \\and pick the most up-to-date log for force-members: the highest last
                    \\term, then the highest last index.
                )
                .examples(&.{
                    "flo server inspect --data-dir /var/lib/flo",
                })
                .stringFlag("config", 'c', "", "Config file naming the data directory")
                .stringFlag("data-dir", 'd', "", "Data directory (overrides the config)")
                .action(wrapHandler(runInspect)),
        )
        .subcommand(
            commander.newBuilder(allocator)
                .name("force-members")
                .about("Make a stopped node the cluster's only voter")
                .longAbout(
                    \\For when a majority of the cluster has lost its data and no leader
                    \\can be elected. Run on the stopped survivor with the most up-to-date
                    \\log (see inspect: the highest last term, then the highest last
                    \\index). It writes a member list naming only this node and retires
                    \\this node's cluster secret, so the server refuses to start with it.
                    \\Then restart this node with a new secret (flo server secret): the new
                    \\secret is what keeps the nodes left out from linking. Reset the
                    \\other nodes' data before they rejoin with --join and that secret.
                    \\
                    \\Without --yes it prints what it would change and stops.
                )
                .examples(&.{
                    "flo server force-members --data-dir /var/lib/flo",
                    "flo server force-members --data-dir /var/lib/flo --yes",
                })
                .stringFlag("config", 'c', "", "Config file naming the data directory and the secret")
                .stringFlag("data-dir", 'd', "", "Data directory (overrides the config)")
                .boolFlag("yes", 0, "Make the change")
                .action(wrapHandler(runForceMembers)),
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
        .build();
}

// Signal handling
var shutdown_requested = std.atomic.Value(bool).init(false);

fn setupSignalHandlers() void {
    const handler = struct {
        fn handle(_: std.c.SIG) callconv(.c) void {
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
        fn handle(sig: std.c.SIG) callconv(.c) noreturn {
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
            std.debug.print("\n=== CRASH SIGNAL {s} (sig={d}) received by PID {d} ===\n", .{ sig_name, @intFromEnum(sig), pid });
            // Re-raise the signal to get core dump/default behavior
            const default_act = std.posix.Sigaction{
                .handler = .{ .handler = std.posix.SIG.DFL },
                .mask = std.posix.sigemptyset(),
                .flags = 0,
            };
            std.posix.sigaction(sig, &default_act, null);
            _ = std.posix.raise(sig) catch {};
            // If raise failed, just exit
            std.c.exit(@as(c_int, @intCast(128 + @intFromEnum(sig))));
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

/// Expand ~ to home directory in path. Delegates to the shared `stdx.fs`
/// helper so CLI and runtime share one tilde-expansion implementation.
fn expandTilde(allocator: Allocator, path: []const u8) ![]const u8 {
    return @import("stdx").fs.expandTilde(allocator, path);
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

    const file = @import("stdx").fs.createFile(pid_path, .{}) catch |err| {
        std.log.err("Failed to create PID file: {}", .{err});
        return err;
    };
    defer @import("stdx").fs.closeFile(file);

    var buf: [20]u8 = undefined;
    const pid_str = std.fmt.bufPrint(&buf, "{d}", .{pid}) catch return error.InvalidPid;
    @import("stdx").fs.writeAll(file, pid_str) catch |err| {
        std.log.err("Failed to write PID file: {}", .{err});
        return err;
    };
}

fn readPidFile(allocator: Allocator, data_dir: []const u8) !?posix.pid_t {
    const pid_path = try getPidFilePath(allocator, data_dir);
    defer allocator.free(pid_path);

    const file = @import("stdx").fs.openFile(pid_path, .{}) catch |err| {
        if (err == error.FileNotFound) return null;
        return err;
    };
    defer @import("stdx").fs.closeFile(file);

    var buf: [20]u8 = undefined;
    const len = @import("stdx").fs.readAll(file, &buf) catch return null;
    if (len == 0) return null;

    const pid_str = std.mem.trimEnd(u8, buf[0..len], &[_]u8{ '\n', '\r', ' ' });
    return std.fmt.parseInt(posix.pid_t, pid_str, 10) catch null;
}

fn removePidFile(data_dir: []const u8) void {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const pid_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ data_dir, PID_FILENAME }) catch return;
    @import("stdx").fs.deleteFile(pid_path) catch {};
}

fn isProcessRunning(pid: posix.pid_t) bool {
    // Send signal 0 to check if process exists
    posix.kill(pid, @enumFromInt(0)) catch |err| {
        // ESRCH means process doesn't exist
        return err != error.NoSuchProcess;
    };
    return true; // No error means process exists
}

fn runSecret(ctx: *commander.Context) commander.Error!void {
    var out: [cluster_config.SECRET_PREFIX.len + cluster_config.SECRET_HEX_LEN]u8 = undefined;
    cluster_config.newSecret(&out) catch {
        return outcome.localRefusal(ctx, "no randomness available for a secret", .{});
    };
    defer std.crypto.secureZero(u8, &out);
    ctx.print("{s}\n", .{&out});
}

/// The peer secret, from whichever one source names it: [cluster] secret,
/// [cluster] secret_file, or FLO_CLUSTER_SECRET. A container often has no
/// config file to keep it in. Two sources are refused: the second would be
/// silently ignored.
fn resolveSecret(ctx: *commander.Context, config: *server_config.ServerConfig) commander.Error!void {
    const allocator = ctx.allocator;
    const env_secret: ?[]const u8 = if (@import("stdx").io.getenv("FLO_CLUSTER_SECRET")) |s| (if (s.len > 0) s else null) else null;
    const sources = @as(u8, @intFromBool(config.cluster.secret != null)) + @intFromBool(config.cluster.secret_file != null) + @intFromBool(env_secret != null);
    if (sources > 1) {
        return outcome.usage(ctx, "the cluster secret is set more than once ([cluster] secret, [cluster] secret_file, FLO_CLUSTER_SECRET); keep one", .{});
    }
    if (config.cluster.secret_file) |path| {
        const s = cluster_config.readSecretFile(allocator, path) catch {
            return outcome.usage(ctx, "[cluster] secret_file {s} could not be used (see the log line above)", .{path});
        };
        defer {
            std.crypto.secureZero(u8, s);
            allocator.free(s);
        }
        config.cluster.secret = try config.dupeString(s);
    }
    if (env_secret) |s| config.cluster.secret = try config.dupeString(s);
}

fn runStart(ctx: *commander.Context) commander.Error!void {
    const allocator = ctx.allocator;

    // Get flag values (convert to appropriate types)
    const config_path = ctx.getString("config");
    const port = stdx.nullIfZero(u16, try ctx.getPort("port"));
    const data_dir = ctx.getString("data-dir");
    // Read wide so an out-of-range count is refused by the config check,
    // not truncated on the way in.
    const shards = ctx.getChangedUint("shards");
    const partitions = ctx.getChangedUint("partitions");
    const log_level = ctx.getString("log-level");
    const log_format = ctx.getString("log-format");
    const durability = ctx.getString("durability");

    const bind_override = ctx.getString("bind");

    // Cluster flags
    const cluster_first = ctx.getBool("cluster");
    const join_addrs = ctx.getString("join");
    const node_id_override = ctx.getChangedUint("node-id");
    const raft_port_override = try ctx.getChangedPort("raft-port");

    // Metrics and dashboard flags
    const metrics_port_override = try ctx.getChangedPort("metrics-port");
    const dashboard_port_override = try ctx.getChangedPort("dashboard-port");
    const no_metrics = ctx.getBool("no-metrics");
    const no_dashboard = ctx.getBool("no-dashboard");

    // Load configuration with CLI overrides
    var config = server_config.loadWithOverrides(
        allocator,
        stdx.nullIfEmpty(u8, config_path),
        port,
        stdx.nullIfEmpty(u8, data_dir),
        shards,
        partitions,
        stdx.nullIfEmpty(u8, log_level),
        stdx.nullIfEmpty(u8, log_format),
        stdx.nullIfEmpty(u8, durability),
    ) catch |err| {
        switch (err) {
            error.InvalidDurability => {
                ctx.printErr("Invalid --durability value. Use: sync, async_flush, or ephemeral\n", .{});
            },
            // Already logged, naming the file or the flag.
            error.InvalidShardCount, error.InvalidPartitionCount, error.UnknownSetting, error.InvalidSetting, error.FileNotFound => {},
            else => ctx.printErr("Error loading configuration: {}\n", .{err}),
        }
        return error.Usage;
    };
    defer config.deinit();

    // Apply cluster CLI overrides
    if (cluster_first) config.cluster.enabled = true;
    if (node_id_override) |nid| {
        config.cluster.node_id = nid;
    }
    if (raft_port_override) |rp| {
        config.cluster.raft_port = rp;
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

    try resolveSecret(ctx, &config);

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
                    try join_seeds_list.append(allocator, try allocator.dupe(u8, trimmed));
                }
            }
            // Replace config seeds with CLI seeds
            config.cluster.seeds = join_seeds_list.items;
        }
    }

    if (bind_override) |b| {
        if (b.len > 0) config.bind = try config.dupeString(b);
    }

    // Apply log configuration
    stdx.log.configure(.{
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
        return outcome.usage(ctx, "cannot expand data directory path: {}", .{err});
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
    const rc_preview = config.toRuntimeConfig();
    if (config.shards == 0 and rc_preview.clusterListenerWanted()) {
        ctx.print("  Shards:     1 (a cluster replicates one shard)\n", .{});
    } else if (config.shards == 0) {
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
        ctx.print("    Node ID:    auto (stored in the data dir after first start)\n", .{});
    } else {
        ctx.print("    Node ID:    {d}\n", .{config.cluster.node_id});
    }
    // Report the port Raft will actually bind, not the unset config value,
    // which reads as 0 while the listener is up on listen_port + 500.
    if (rc_preview.clusterListenerWanted()) {
        ctx.print("    Peer port:  {d}\n", .{rc_preview.effectiveRaftPort()});
    } else {
        ctx.print("    Peer port:  not listening (single node)\n", .{});
    }
    if (config.cluster.seeds.len > 0) {
        ctx.print("    Mode:       joining {s}", .{config.cluster.seeds[0]});
        for (config.cluster.seeds[1..]) |seed| {
            ctx.print(",{s}", .{seed});
        }
        ctx.print("\n", .{});
    } else if (config.cluster.enabled) {
        ctx.print("    Mode:       first member of a cluster\n", .{});
    } else {
        ctx.print("    Mode:       single node\n", .{});
    }
    ctx.print("\n", .{});

    // Ensure data directory exists
    @import("stdx").fs.makePath(expanded_data_dir) catch |err| {
        if (err != error.PathAlreadyExists) {
            return outcome.localRefusal(ctx, "cannot create data directory '{s}': {}", .{ expanded_data_dir, err });
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
        return outcome.localRefusal(ctx, "cannot initialize runtime: {}", .{err});
    };
    defer runtime.deinit();

    // Start the runtime
    runtime.start() catch |err| {
        return outcome.localRefusal(ctx, "cannot start runtime: {}", .{err});
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
        @import("stdx").time.sleep(100 * std.time.ns_per_ms);
    }

    ctx.print("\nShutting down...\n", .{});

    // Stop runtime (signals cores to stop and waits for threads)
    runtime.stop();

    ctx.print("Server stopped.\n", .{});
}

/// The config an offline command reads: the file named, or the defaults,
/// with --data-dir over its data_dir.
fn offlineConfig(ctx: *commander.Context) commander.Error!server_config.ServerConfig {
    const config_path = ctx.getString("config");
    const data_dir = ctx.getString("data-dir");
    return server_config.loadWithOverrides(ctx.allocator, stdx.nullIfEmpty(u8, config_path), null, stdx.nullIfEmpty(u8, data_dir), null, null, null, null, null) catch |err| {
        switch (err) {
            error.UnknownSetting, error.InvalidSetting, error.FileNotFound => {},
            else => ctx.printErr("Error loading configuration: {}\n", .{err}),
        }
        return error.Usage;
    };
}

/// Lock the data dir and read its cluster group, printing what it holds.
fn openOffline(ctx: *commander.Context, data_dir: []const u8, lock: *stdx.fs.File) commander.Error!offline.Summary {
    lock.* = runtime_mod.lockExistingDataDir(data_dir) catch |err| return switch (err) {
        error.DataDirMissing => outcome.usage(ctx, "data directory {s} does not exist", .{data_dir}),
        error.DataDirInUse => outcome.localRefusal(ctx, "data directory {s} is in use by a running flo server; stop it first", .{data_dir}),
        else => outcome.localRefusal(ctx, "cannot lock data directory {s}: {}", .{ data_dir, err }),
    };
    const sum = offline.summarize(ctx.allocator, data_dir) catch |err| {
        stdx.fs.closeFile(lock.*);
        return outcome.localRefusal(ctx, "cannot read the cluster group in {s}: {}", .{ data_dir, err });
    };
    ctx.print("data dir:    {s} (cluster group)\n", .{data_dir});
    if (sum.hard_state) |hs| {
        ctx.print("hard state:  node {d}, term {d}, voted for {d}{s}\n", .{ hs.node_id, hs.term, hs.voted_for, if (hs.lost_log) "; lost-log guard on, not voting yet" else "" });
    } else {
        ctx.print("hard state:  none (never written, or lost)\n", .{});
    }
    if (sum.last_index == 0) {
        ctx.print("log:         empty\n", .{});
    } else {
        ctx.print("log:         indices {d}..{d}, last term {d}\n", .{ sum.first_index, sum.last_index, sum.last_term });
        ctx.print("committed:   through {d} (as last flushed)\n", .{sum.commit});
    }
    if (sum.member_count > 0) {
        var voters: [membership.MAX_MEMBERS]u32 = undefined;
        ctx.print("members:     {any}, voters {any} (config at index {d}, term {d})\n", .{ sum.configMembers(), sum.config.voterIds(&voters), sum.config_index, sum.config_term });
    } else {
        ctx.print("members:     no member list in the log\n", .{});
    }
    ctx.print("snapshot:    {s}\n", .{sum.snapshot orelse "none"});
    if (sum.truncation_pending) ctx.print("note:        a log truncation is pending; the server finishes it at its next start\n", .{});
    return sum;
}

fn runInspect(ctx: *commander.Context) commander.Error!void {
    var config = try offlineConfig(ctx);
    defer config.deinit();
    const data_dir = expandTilde(ctx.allocator, config.data_dir) catch |err|
        return outcome.usage(ctx, "cannot expand data directory path: {}", .{err});
    defer ctx.allocator.free(data_dir);
    var lock: stdx.fs.File = undefined;
    var sum = try openOffline(ctx, data_dir, &lock);
    defer stdx.fs.closeFile(lock);
    defer sum.deinit(ctx.allocator);
}

fn runForceMembers(ctx: *commander.Context) commander.Error!void {
    var config = try offlineConfig(ctx);
    defer config.deinit();
    try resolveSecret(ctx, &config);
    const secret = config.cluster.secret orelse {
        return outcome.usage(ctx, "force-members retires this node's cluster secret, and none is set; set it as the server does ([cluster] secret, secret_file or FLO_CLUSTER_SECRET)", .{});
    };
    const data_dir = expandTilde(ctx.allocator, config.data_dir) catch |err|
        return outcome.usage(ctx, "cannot expand data directory path: {}", .{err});
    defer ctx.allocator.free(data_dir);
    var lock: stdx.fs.File = undefined;
    var sum = try openOffline(ctx, data_dir, &lock);
    defer stdx.fs.closeFile(lock);
    defer sum.deinit(ctx.allocator);

    if (sum.last_index == 0) {
        return outcome.localRefusal(ctx, "this node has no log; there is nothing to recover from. Run force-members on a node that has one (see inspect)", .{});
    }
    const hs = sum.hard_state orelse {
        return outcome.localRefusal(ctx, "this node has a log but no hard state, so its node id is not on disk and force-members cannot name it. Start it once with --join and --node-id set to its id from the member list above (it writes its hard state and waits guarded), stop it, then run force-members again", .{});
    };
    if (sum.truncation_pending) {
        return outcome.localRefusal(ctx, "a log truncation is pending; start and stop the server once to finish it, then run force-members again", .{});
    }
    if (!ctx.getBool("yes")) {
        if (hs.lost_log) ctx.print("\nWarning: this node's lost-log guard never finished; its log may be behind what the group committed. Prefer a survivor whose guard is off.\n", .{});
        ctx.print("\nWould make node {d} the only voter (a member list at index {d}, everything through it committed) and retire this node's cluster secret. Run again with --yes to do it.\n", .{ hs.node_id, sum.last_index + 1 });
        return;
    }
    // Retired first: a crash after the member list is written must not
    // leave the old secret usable.
    offline.retire(ctx.allocator, data_dir, secret, @intCast(@divFloor(@max(0, stdx.time.milliTimestamp()), 1000))) catch |err| {
        return outcome.localRefusal(ctx, "cannot record the retired secret in {s}: {}", .{ data_dir, err });
    };
    offline.forceMembers(ctx.allocator, data_dir, &sum) catch |err| {
        return outcome.localRefusal(ctx, "cannot write the member list: {}. This node's secret is already retired and its members are unchanged, which fails safe: fix the cause and run force-members again", .{err});
    };
    ctx.print("\nNode {d} is now the only voter (member list at index {d}).\n", .{ hs.node_id, sum.last_index + 1 });
    ctx.print("Restart it with a new [cluster] secret (flo server secret); with it the nodes left out cannot link.\n", .{});
    ctx.print("Reset the other nodes' data before they rejoin with --join and the new secret.\n", .{});
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
        return outcome.usage(ctx, "cannot expand data directory path: {}", .{err});
    };
    defer allocator.free(data_dir);

    const pid = readPidFile(allocator, data_dir) catch |err| {
        return outcome.localRefusal(ctx, "cannot read PID file: {}", .{err});
    };

    if (pid == null) {
        return outcome.localRefusal(ctx, "no server running (PID file not found in {s})", .{data_dir});
    }

    const server_pid = pid.?;
    if (!isProcessRunning(server_pid)) {
        ctx.print("Server not running (stale PID file, PID {d})\n", .{server_pid});
        removePidFile(data_dir);
        return;
    }

    // Send signal to stop server
    const sig: std.c.SIG = if (force) posix.SIG.KILL else posix.SIG.TERM;
    const sig_name: []const u8 = if (force) "SIGKILL" else "SIGTERM";

    ctx.print("Sending {s} to server (PID {d})...\n", .{ sig_name, server_pid });

    _ = posix.kill(server_pid, sig) catch |err| {
        return outcome.localRefusal(ctx, "cannot send signal: {}", .{err});
    };

    if (!force) {
        // Wait for graceful shutdown (up to 30 seconds)
        ctx.print("Waiting for server to stop...\n", .{});
        var waited: u32 = 0;
        while (waited < 300) : (waited += 1) {
            @import("stdx").time.sleep(100 * std.time.ns_per_ms);
            if (!isProcessRunning(server_pid)) {
                ctx.print("Server stopped.\n", .{});
                return;
            }
        }
        return outcome.localRefusal(ctx, "server did not stop within 30 seconds. Use --force to kill immediately.", .{});
    } else {
        ctx.print("Server killed.\n", .{});
    }
}

fn runServerStatus(ctx: *commander.Context) commander.Error!void {
    const allocator = ctx.allocator;
    const data_dir_raw = getDataDir(ctx);

    const data_dir = expandTilde(allocator, data_dir_raw) catch |err| {
        return outcome.usage(ctx, "cannot expand data directory path: {}", .{err});
    };
    defer allocator.free(data_dir);

    const pid = readPidFile(allocator, data_dir) catch |err| {
        return outcome.localRefusal(ctx, "cannot read PID file: {}", .{err});
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

        if (@import("stdx").fs.openFile(manifest_path, .{})) |file| {
            defer @import("stdx").fs.closeFile(file);
            var buf: [256]u8 = undefined;
            const bytes_read = @import("stdx").fs.readAll(file, &buf) catch 0;
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
    const address = @import("stdx").net.Address.parseIp4(resolved_host, port) catch {
        return outcome.usage(ctx, "invalid address: {s}", .{resolved_host});
    };

    const stream = @import("stdx").net.tcpConnectToAddress(address) catch |err| {
        ctx.printErr("Connection failed: {}\n", .{err});
        ctx.printErr("Is the Flo metrics server running at {s}:{d}?\n", .{ resolved_host, port });
        return error.Transport;
    };
    defer stream.close();

    // Send HTTP GET request
    var request_buf: [256]u8 = undefined;
    const request = std.fmt.bufPrint(&request_buf, "GET /metrics HTTP/1.1\r\nHost: {s}\r\nConnection: close\r\n\r\n", .{resolved_host}) catch {
        return outcome.usage(ctx, "host name too long for the request: {s}", .{resolved_host});
    };
    _ = stream.write(request) catch |err| {
        ctx.printErr("Write failed: {}\n", .{err});
        return error.Transport;
    };

    // Read response
    var buf: [8192]u8 = undefined;
    var total: usize = 0;
    while (true) {
        const n = stream.read(buf[total..]) catch |err| {
            ctx.printErr("Read failed: {}\n", .{err});
            return error.Transport;
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

// ==================== Testing ====================

test "create server command" {
    const allocator = std.testing.allocator;

    const cmd = try createServerCommand(allocator);
    defer cmd.deinit();

    try std.testing.expectEqualStrings("server", cmd.name);
    try std.testing.expect(cmd.commands.items.len >= 2);
}
