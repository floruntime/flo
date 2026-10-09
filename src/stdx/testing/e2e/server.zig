//! E2E Test Server Process Manager
//!
//! Manages the lifecycle of a Flo server process for end-to-end testing.
//! Handles spawning, readiness detection, and graceful shutdown.
//!
//! ## Usage
//! Typically accessed via TestContext:
//! ```zig
//! const stdx = @import("stdx");
//!
//! var ctx = try stdx.testing.TestContext.init(allocator);
//! defer ctx.deinit();
//!
//! const port = ctx.server.getPort();
//! const data_dir = ctx.server.getDataDir();
//!
//! // Restart for crash recovery tests
//! try ctx.restartServer();
//! ```

const std = @import("std");
const stdx = @import("../../mod.zig");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const testing = std.testing;

/// Server process manager for e2e tests
pub const ServerProcess = struct {
    const Self = @This();

    allocator: Allocator,
    process: ?stdx.process.Child,
    port: u16,
    dashboard_port: u16,
    metrics_port: u16,
    raft_port: u16,
    data_dir: []const u8,
    config_file: []const u8,
    log_file_path: []const u8,
    /// Print the captured server log if startup fails. Callers that retry set
    /// this false for all but the final attempt, so one failure is not
    /// reported three times.
    dump_log_on_failure: bool = true,
    /// The child was already collected by a diagnostic `waitpid`; a second
    /// wait would be a syscall bug, not a result.
    reaped: bool = false,
    tmp_dir: testing.TmpDir,
    flo_binary: []const u8,
    started: bool,
    config: ServerConfig,
    log_thread: ?std.Thread = null,
    /// Set once the server is reaped; the log thread exits at the next quiet
    /// poll, so joining it never waits on a pipe something else still holds.
    log_stop: std.atomic.Value(bool) = .init(false),

    /// Default timeout for server readiness (ms)
    pub const DEFAULT_READY_TIMEOUT_MS: u64 = 10_000;
    /// Poll interval for readiness check (ms)
    pub const READY_POLL_INTERVAL_MS: u64 = 50;
    /// Timeout for graceful shutdown before SIGKILL (ms)
    pub const SHUTDOWN_TIMEOUT_MS: u64 = 5_000;
    /// Wait time between SIGTERM and SIGKILL (ms)
    pub const SHUTDOWN_GRACE_PERIOD_MS: u64 = 500;
    /// Wait time after SIGKILL before proceeding (ms)
    /// Each test uses unique ports (findFreePort) and data dirs, so minimal wait suffices
    pub const POST_KILL_WAIT_MS: u64 = 100;

    /// Durability mode for persistence
    pub const Durability = enum {
        /// Wait for fdatasync before returning - data guaranteed on disk
        sync,
        /// Return after WAL append (async flush) - fast but may lose recent writes on crash
        async_flush,
        /// No persistence - for pure caching use cases
        ephemeral,

        fn toConfigString(self: Durability) []const u8 {
            return switch (self) {
                .sync => "sync",
                .async_flush => "async_flush",
                .ephemeral => "ephemeral",
            };
        }
    };

    /// Tiered log configuration for tests
    pub const TieredLogConfig = struct {
        /// Hot tier buffer capacity in bytes (default: 16MB)
        hot_buffer_capacity: usize = 16 * 1024 * 1024,
        /// Max entries before spilling to warm tier (0 = use buffer capacity)
        max_hot_entries: usize = 0,
        /// Time window before flushing to warm (seconds, 0 = disabled)
        hot_flush_seconds: u32 = 0,
    };

    /// Server configuration for tests
    pub const ServerConfig = struct {
        /// Enable dashboard HTTP server
        dashboard_enabled: bool = false,
        /// `[dashboard] hosts` and `cors_origins`, written as given.
        dashboard_hosts: ?[]const u8 = null,
        dashboard_cors_origins: ?[]const u8 = null,
        /// Enable metrics HTTP server (Prometheus)
        metrics_enabled: bool = false,
        /// Number of shards (1 = faster startup)
        shards: u8 = 1,
        /// Server log level written into flo.toml. Raise it to see how far a
        /// server got when it never became ready — the captured log is the
        /// only view the harness has into a server's startup.
        log_level: []const u8 = "info",
        /// Durability mode (sync = guaranteed persistence, async_flush = fast, ephemeral = no persistence)
        durability: Durability = .async_flush,
        /// Serve from this data dir rather than the node's own, e.g. another
        /// node's. Config and log stay in the node's own.
        data_dir_of: ?[]const u8 = null,
        /// Tiered log configuration (for controlling hot→warm transitions)
        tiered_log: TieredLogConfig = .{},

        // Cluster configuration
        /// Join addresses for cluster mode (e.g., "127.0.0.1:4445")
        join_addresses: ?[]const u8 = null,
        /// Node ID (0 = auto-generate)
        node_id: u32 = 0,
        /// Raft RPC port (0 = auto-assign)
        raft_port: u16 = 0,
        /// Enable cluster mode (starts Raft listener) - for seed nodes without join_addresses
        cluster_enabled: bool = false,
        /// Written to `[cluster] secret` whenever the peer listener will start.
        /// Every node of a test cluster shares the default; a test that wants a
        /// stranger sets its own.
        /// In the form `flo server secret` prints; a cluster takes no other.
        cluster_secret: []const u8 = "flo-secret-" ++ "e2e0" ** 16,
        /// Written to `[cluster] secret_file` when set, beside `secret` if
        /// that is set too.
        cluster_secret_file: ?[]const u8 = null,
        /// `[server] bind` for this node; null = the server default (0.0.0.0),
        /// reached at 127.0.0.1 by the harness.
        bind: ?[]const u8 = null,
        /// Appended to the generated flo.toml as is.
        extra_config: ?[]const u8 = null,
        /// Start with `--config` naming a file that doesn't exist.
        config_file_missing: bool = false,
    };

    /// Initialize a new server process manager with default config
    pub fn init(allocator: Allocator) !*Self {
        return initWithConfig(allocator, .{});
    }

    /// Initialize with custom configuration
    pub fn initWithConfig(allocator: Allocator, config: ServerConfig) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        self.config = config;

        // Create temp directory for this test instance
        self.tmp_dir = testing.tmpDir(.{});
        errdefer self.tmp_dir.cleanup();

        // Get absolute path to temp directory
        self.data_dir = try stdx.fs.dirRealpathAlloc(self.tmp_dir.dir, allocator, ".");
        errdefer allocator.free(self.data_dir);

        // Create config file path (content written at start time when we know ports)
        const config_path = try std.fmt.allocPrint(allocator, "{s}/flo.toml", .{self.data_dir});
        errdefer allocator.free(config_path);
        self.config_file = config_path;

        // Create log file path
        const log_path = try std.fmt.allocPrint(allocator, "{s}/server.log", .{self.data_dir});
        errdefer allocator.free(log_path);
        self.log_file_path = log_path;

        // Find flo binary
        self.flo_binary = try findFloBinary(allocator);

        self.allocator = allocator;
        self.process = null;
        self.port = 0;
        self.dashboard_port = 0;
        self.metrics_port = 0;
        self.raft_port = 0;
        self.started = false;
        // `allocator.create` leaves the struct uninitialised, so the field
        // defaults declared above do NOT apply — every field must be assigned
        // here. `log_thread` in particular is otherwise only set inside
        // `start()`, and a server torn down without starting would then join a
        // garbage thread handle.
        self.log_thread = null;
        self.log_stop = .init(false);
        self.reaped = false;
        self.dump_log_on_failure = true;

        return self;
    }

    /// Clean up all resources
    pub fn deinit(self: *Self) void {
        // Stop server if still running
        if (self.started) {
            self.stop();
        }

        self.allocator.free(self.flo_binary);
        self.allocator.free(self.config_file);
        self.allocator.free(self.log_file_path);
        self.allocator.free(self.data_dir);
        self.tmp_dir.cleanup();
        self.allocator.destroy(self);
    }

    /// Start the server and wait for it to be ready
    pub fn start(self: *Self) !void {
        return self.startWithTimeout(DEFAULT_READY_TIMEOUT_MS);
    }

    /// Start the server with custom timeout
    pub fn startWithTimeout(self: *Self, timeout_ms: u64) !void {
        if (self.started) return error.AlreadyStarted;

        // Find free port for main server
        self.port = try findFreePort();

        // Allocate ports for optional services
        if (self.config.dashboard_enabled) {
            self.dashboard_port = try findFreePort();
        }
        if (self.config.metrics_enabled) {
            self.metrics_port = try findFreePort();
        }

        // Write config file with actual ports
        var config_buf: [2048]u8 = undefined;
        var fbs: std.Io.Writer = .fixed(&config_buf);
        const config_writer = &fbs;

        // Storage section with durability + tier settings
        try config_writer.print("[storage]\ndurability = \"{s}\"\n", .{self.config.durability.toConfigString()});

        // Tier settings (all under [storage])
        if (self.config.tiered_log.hot_buffer_capacity != 16 * 1024 * 1024 or
            self.config.tiered_log.max_hot_entries != 0 or
            self.config.tiered_log.hot_flush_seconds != 0)
        {
            try config_writer.print("hot_buffer_capacity = {d}\n", .{self.config.tiered_log.hot_buffer_capacity});
            if (self.config.tiered_log.max_hot_entries > 0) {
                try config_writer.print("max_hot_entries = {d}\n", .{self.config.tiered_log.max_hot_entries});
            }
            if (self.config.tiered_log.hot_flush_seconds > 0) {
                try config_writer.print("hot_flush_seconds = {d}\n", .{self.config.tiered_log.hot_flush_seconds});
            }
        }
        try config_writer.print("\n", .{});

        try config_writer.print("[metrics]\nenabled = {}\n", .{self.config.metrics_enabled});
        if (self.config.metrics_enabled) {
            try config_writer.print("port = {d}\n", .{self.metrics_port});
        }
        try config_writer.print("\n[dashboard]\nenabled = {}\n", .{self.config.dashboard_enabled});
        if (self.config.dashboard_enabled) {
            try config_writer.print("port = {d}\n", .{self.dashboard_port});
        }
        if (self.config.dashboard_hosts) |h| try config_writer.print("hosts = \"{s}\"\n", .{h});
        if (self.config.dashboard_cors_origins) |o| try config_writer.print("cors_origins = \"{s}\"\n", .{o});

        // Same predicate as the raft port allocation below: the config is
        // written before the port is picked.
        if (self.config.raft_port > 0 or self.config.join_addresses != null or self.config.cluster_enabled) {
            if (self.config.cluster_secret.len > 0 or self.config.cluster_secret_file != null) try config_writer.print("\n[cluster]\n", .{});
            if (self.config.cluster_secret.len > 0) try config_writer.print("secret = \"{s}\"\n", .{self.config.cluster_secret});
            if (self.config.cluster_secret_file) |f| try config_writer.print("secret_file = \"{s}\"\n", .{f});
        }

        try config_writer.print("\n[logging]\nlevel = \"{s}\"\n", .{self.config.log_level});
        if (self.config.extra_config) |x| try config_writer.print("\n{s}", .{x});

        if (!self.config.config_file_missing) {
            const config_file = try self.tmp_dir.dir.createFile(stdx.io.instance(), "flo.toml", .{});
            defer stdx.fs.closeFile(config_file);
            try stdx.fs.writeAll(config_file, fbs.buffered());
        }

        // Open log file for output redirection
        var log_file = try stdx.fs.createFileAbsolute(self.log_file_path, .{
            .truncate = true,
        });
        _ = &log_file;
        // Track ownership: once handed to the logging thread, the thread owns
        // the fd and will close it. We must NOT double-close on error paths.
        var log_file_handed_off = false;
        errdefer if (!log_file_handed_off) stdx.fs.closeFile(log_file);

        // Build dynamic argv with cluster options
        const port_str = try std.fmt.allocPrint(self.allocator, "{d}", .{self.port});
        defer self.allocator.free(port_str);
        const shards_str = try std.fmt.allocPrint(self.allocator, "{d}", .{self.config.shards});
        defer self.allocator.free(shards_str);

        // Allocate raft_port if needed for cluster mode
        // In tests, we use a separate dynamically allocated port (not derived from main port)
        // because main port can be high in ephemeral range, causing overflow when adding offset.
        if (self.config.raft_port > 0) {
            self.raft_port = self.config.raft_port;
        } else if (self.config.join_addresses != null or self.config.cluster_enabled) {
            // Cluster mode: allocate a separate free port for Raft
            self.raft_port = try findFreePort();
        }

        // Build argv dynamically
        var argv_list: std.ArrayListUnmanaged([]const u8) = .empty;
        defer argv_list.deinit(self.allocator);

        try argv_list.appendSlice(self.allocator, &.{
            self.flo_binary,
            "server",
            "start",
            "--port",
            port_str,
            "--data-dir",
            self.config.data_dir_of orelse self.data_dir,
            "--config",
            self.config_file,
            "--shards",
            shards_str,
        });

        // Add cluster options
        var raft_port_str: ?[]const u8 = null;
        var node_id_str: ?[]const u8 = null;
        defer if (raft_port_str) |s| self.allocator.free(s);
        defer if (node_id_str) |s| self.allocator.free(s);

        if (self.raft_port > 0) {
            raft_port_str = try std.fmt.allocPrint(self.allocator, "{d}", .{self.raft_port});
            try argv_list.appendSlice(self.allocator, &.{ "--raft-port", raft_port_str.? });
        }

        if (self.config.node_id > 0) {
            node_id_str = try std.fmt.allocPrint(self.allocator, "{d}", .{self.config.node_id});
            try argv_list.appendSlice(self.allocator, &.{ "--node-id", node_id_str.? });
        }

        if (self.config.join_addresses) |join| {
            try argv_list.appendSlice(self.allocator, &.{ "--join", join });
        } else if (self.config.cluster_enabled) {
            try argv_list.appendSlice(self.allocator, &.{"--cluster"});
        }

        if (self.config.bind) |bind| {
            try argv_list.appendSlice(self.allocator, &.{ "--bind", bind });
        }

        var child = stdx.process.Child.init(argv_list.items, self.allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;
        // Put each child in its own process group so killing one doesn't kill others
        child.pgid = 0; // 0 means use child's PID as process group ID

        try child.spawn();

        // Redirect stdout/stderr to log file in background
        // Store process immediately so we can kill it if readiness check fails
        self.process = child;

        // Extract fds by value before spawning thread — avoids dangling
        // pointers when stop() sets self.process = null
        const stdout_fd = self.process.?.stdout.?.handle;
        const stderr_fd = self.process.?.stderr.?.handle;
        self.reaped = false;
        self.log_stop.store(false, .release);
        self.log_thread = try std.Thread.spawn(.{}, logServerOutput, .{ log_file, stdout_fd, stderr_fd, &self.log_stop });
        log_file_handed_off = true; // Thread now owns the fd

        // Wait for server to be ready
        self.waitForReady(timeout_ms) catch |err| {
            // The server's own output goes to a log file, so without this a
            // startup failure surfaces only as this harness's timeout and the
            // real bind error or panic never reaches the test output.
            if (self.dump_log_on_failure) {
                std.debug.print("[server] port {d}: not ready within {d} ms\n", .{ self.port, timeout_ms });
                self.dumpLogTail();
            }
            self.forceKill();
            return err;
        };

        self.started = true;
    }

    /// Stop the server gracefully
    pub fn stop(self: *Self) void {
        if (!self.started) return;

        if (self.process) |*proc| {
            const pid = proc.id;

            // Send SIGTERM for graceful shutdown
            _ = std.c.kill(pid, .TERM);

            // Wait for graceful shutdown (check if process exits naturally)
            const grace_start = stdx.time.milliTimestamp();
            var gracefully_exited = false;

            while (stdx.time.milliTimestamp() - grace_start < @as(i64, @intCast(SHUTDOWN_GRACE_PERIOD_MS))) {
                // Try non-blocking wait to see if process exited
                var status: c_int = 0;
                const result_pid = std.c.waitpid(pid, &status, std.posix.W.NOHANG);
                if (result_pid == pid) {
                    gracefully_exited = true;
                    break;
                }
                stdx.time.sleep(50 * std.time.ns_per_ms);
            }

            // Force kill if not gracefully exited
            if (!gracefully_exited) {
                // Server didn't respond to SIGTERM, force kill its group
                if (std.c.kill(-pid, .KILL) != 0) _ = std.c.kill(pid, .KILL);

                // Wait for process to die (blocking wait with timeout)
                const kill_start = stdx.time.milliTimestamp();
                while (stdx.time.milliTimestamp() - kill_start < 1000) { // 1 second max
                    var status: c_int = 0;
                    const result_pid = std.c.waitpid(pid, &status, std.posix.W.NOHANG);
                    if (result_pid == pid) {
                        break;
                    }
                    stdx.time.sleep(10 * std.time.ns_per_ms);
                }
            }

            // Brief wait for OS resource release (only needed after forced kill;
            // each test uses unique ports via findFreePort, so minimal delay suffices)
            if (!gracefully_exited) {
                stdx.time.sleep(POST_KILL_WAIT_MS * std.time.ns_per_ms);
            }

            self.joinLogThread();
            self.process = null;
        }

        self.started = false;
    }

    /// Never through `Child.wait`: it closes the pipes the log thread is
    /// polling, and a close under another thread's poll doesn't wake it.
    fn joinLogThread(self: *Self) void {
        self.log_stop.store(true, .release);
        if (self.log_thread) |thread| {
            thread.join();
            self.log_thread = null;
        }
    }

    // =========================================================================
    // Server Log Access
    // =========================================================================

    /// Print the tail of this server's captured log to stderr.
    ///
    /// Best-effort and never fails: it runs on a path that is already
    /// returning an error, and a diagnostic that can itself throw is worse
    /// than no diagnostic. Bounded so a chatty server cannot bury the failure.
    pub fn dumpLogTail(self: *Self) void {
        const TAIL_BYTES: usize = 4096;

        // A crash and a hang look identical in the log — output simply stops —
        // and they have different causes, so report which one this is.
        if (self.process) |*proc| {
            var status: c_int = 0;
            const rc = if (self.reaped) proc.id else std.c.waitpid(proc.id, &status, @as(c_int, 1)); // WNOHANG
            if (rc == proc.id) {
                self.reaped = true;
                std.debug.print("[server] port {d}: process already exited (raw status {d})\n", .{ self.port, status });
            } else if (rc == 0) {
                std.debug.print("[server] port {d}: process still running — hung, not crashed\n", .{self.port});
            }
        }
        const logs = self.readLogs() catch |err| {
            std.debug.print("[server] could not read {s}: {s}\n", .{ self.log_file_path, @errorName(err) });
            return;
        };
        defer self.allocator.free(logs);

        if (logs.len == 0) {
            std.debug.print("[server] port {d}: exited without writing any output\n", .{self.port});
            return;
        }

        const tail = if (logs.len > TAIL_BYTES) logs[logs.len - TAIL_BYTES ..] else logs;
        std.debug.print(
            "[server] port {d} did not become ready — last {d} bytes of {s}:\n{s}\n[server] end of log\n",
            .{ self.port, tail.len, self.log_file_path, tail },
        );
    }

    /// Read server logs (returns owned slice - caller must free)
    pub fn readLogs(self: *Self) ![]const u8 {
        const file = stdx.fs.openFileAbsolute(self.log_file_path, .{}) catch |err| {
            if (err == error.FileNotFound) return try self.allocator.dupe(u8, "");
            return err;
        };
        defer stdx.fs.closeFile(file);

        return stdx.fs.readToEndAlloc(file, self.allocator, 10 * 1024 * 1024); // Max 10MB
    }

    /// Check if server logs contain a specific string
    pub fn logsContain(self: *Self, needle: []const u8) !bool {
        const logs = try self.readLogs();
        defer self.allocator.free(logs);
        return std.mem.indexOf(u8, logs, needle) != null;
    }

    /// Count occurrences of a string in server logs
    pub fn logsCount(self: *Self, needle: []const u8) !usize {
        const logs = try self.readLogs();
        defer self.allocator.free(logs);
        return countOccurrences(logs, needle);
    }

    /// Get lines from server logs matching a pattern
    /// Returns owned slice of lines - caller must free each line and the slice
    pub fn grepLogs(self: *Self, pattern: []const u8) ![][]const u8 {
        const logs = try self.readLogs();
        defer self.allocator.free(logs);

        var matches: std.ArrayList([]const u8) = .empty;
        errdefer {
            for (matches.items) |line| self.allocator.free(line);
            matches.deinit(self.allocator);
        }

        var lines = std.mem.splitSequence(u8, logs, "\n");
        while (lines.next()) |line| {
            if (std.mem.indexOf(u8, line, pattern) != null) {
                try matches.append(self.allocator, try self.allocator.dupe(u8, line));
            }
        }

        return matches.toOwnedSlice(self.allocator);
    }

    /// Print server logs to stderr (useful for debugging failed tests)
    pub fn dumpLogs(self: *Self) void {
        const logs = self.readLogs() catch |err| {
            std.debug.print("\n=== SERVER LOGS (error: {}) ===\n", .{err});
            return;
        };
        defer self.allocator.free(logs);

        std.debug.print("\n=== SERVER LOGS ({s}) ===\n{s}\n=== END SERVER LOGS ===\n", .{ self.log_file_path, logs });
    }

    /// Force kill the server process and everything in its group.
    fn forceKill(self: *Self) void {
        if (self.process) |*proc| {
            const pid = proc.id;
            if (!self.reaped) {
                // The child leads its own group (spawned with pgid 0), so
                // this reaches anything it started; fall back to the pid.
                if (std.c.kill(-pid, .KILL) != 0) _ = std.c.kill(pid, .KILL);
                const kill_start = stdx.time.milliTimestamp();
                while (stdx.time.milliTimestamp() - kill_start < 2000) {
                    var status: c_int = 0;
                    if (std.c.waitpid(pid, &status, std.posix.W.NOHANG) == pid) {
                        self.reaped = true;
                        break;
                    }
                    stdx.time.sleep(10 * std.time.ns_per_ms);
                }
                // A zombie is better than a hung run.
                if (!self.reaped) std.debug.print("[server] pid {d} not reaped after SIGKILL; moving on\n", .{pid});
            }
            stdx.time.sleep(POST_KILL_WAIT_MS * std.time.ns_per_ms);
            self.joinLogThread();
            self.process = null;
        }
        self.started = false;
    }

    /// Wait for server to be ready (accepting TCP connections)
    fn waitForReady(self: *Self, timeout_ms: u64) !void {
        const start_time = stdx.time.milliTimestamp();

        while (stdx.time.milliTimestamp() - start_time < @as(i64, @intCast(timeout_ms))) {
            // Check main port — this is always required
            const main_ready = self.tryConnect(self.port);

            // Check dashboard port if enabled (wired into runtime)
            const dashboard_ready = if (self.config.dashboard_enabled and self.dashboard_port > 0)
                self.tryConnect(self.dashboard_port)
            else
                true;

            // Readiness is deliberately just the client and dashboard ports.
            // The metrics exporter binds only when enabled, and the Raft
            // listener only when the node can have peers, so neither is a
            // reliable readiness signal.

            if (main_ready and dashboard_ready) {
                return;
            }

            stdx.time.sleep(READY_POLL_INTERVAL_MS * std.time.ns_per_ms);
        }

        return error.ServerNotReady;
    }

    /// Try to establish a TCP connection to a port
    fn tryConnect(self: *Self, port: u16) bool {
        const ip4 = stdx.net.parseIp4Bind(self.hostForClients()) catch .{ 127, 0, 0, 1 };
        const addr = stdx.net.Address.initIp4(ip4, port);
        const stream = stdx.net.tcpConnectToAddress(addr) catch {
            return false;
        };
        stream.close();
        return true;
    }

    /// Get the port the server is listening on
    pub fn getPort(self: *const Self) u16 {
        return self.port;
    }

    /// Get the Raft RPC port (0 if not in cluster mode)
    pub fn getRaftPort(self: *const Self) u16 {
        return self.raft_port;
    }

    /// The address clients (and the harness) reach this node at: its bind
    /// address when one was given, else loopback.
    pub fn hostForClients(self: *const Self) []const u8 {
        return self.config.bind orelse "127.0.0.1";
    }

    /// Get the Raft endpoint for --join (host:raft_port)
    pub fn getRaftEndpoint(self: *const Self, allocator: Allocator) ![]const u8 {
        return std.fmt.allocPrint(allocator, "{s}:{d}", .{ self.hostForClients(), self.raft_port });
    }

    /// Get the dashboard port (0 if not enabled)
    pub fn getDashboardPort(self: *const Self) u16 {
        return self.dashboard_port;
    }

    /// Get the metrics port (0 if not enabled)
    pub fn getMetricsPort(self: *const Self) u16 {
        return self.metrics_port;
    }

    /// Get the endpoint string (host:port)
    pub fn getEndpoint(self: *const Self, allocator: Allocator) ![]const u8 {
        return std.fmt.allocPrint(allocator, "{s}:{d}", .{ self.hostForClients(), self.port });
    }

    /// Get the data directory path
    pub fn getDataDir(self: *const Self) []const u8 {
        return self.data_dir;
    }

    /// Check if server is running
    pub fn isRunning(self: *const Self) bool {
        return self.started;
    }

    /// Check if dashboard is enabled
    pub fn isDashboardEnabled(self: *const Self) bool {
        return self.config.dashboard_enabled;
    }

    /// Check if metrics is enabled
    pub fn isMetricsEnabled(self: *const Self) bool {
        return self.config.metrics_enabled;
    }
};

/// Find a free TCP port by binding to port 0
fn findFreePort() !u16 {
    const sock = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (sock < 0) return error.SocketFailed;
    defer _ = std.c.close(sock);

    // Allow reuse of TIME_WAIT ports — critical when running hundreds of tests
    // back-to-back, each spawning/stopping server processes
    var on: c_int = 1;
    _ = std.c.setsockopt(sock, std.c.SOL.SOCKET, std.c.SO.REUSEADDR, &on, @sizeOf(c_int));

    var addr = stdx.net.SocketAddrV4.initIp4(.{ 127, 0, 0, 1 }, 0);
    if (std.c.bind(sock, addr.anyPtr(), addr.anyLen()) != 0) return error.BindFailed;

    var bound_addr: std.posix.sockaddr align(4) = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    if (std.c.getsockname(sock, &bound_addr, &len) != 0) return error.GetSocknameFailed;

    // Extract port from sockaddr_in
    const addr_in: *const std.posix.sockaddr.in = @ptrCast(@alignCast(&bound_addr));
    return std.mem.bigToNative(u16, addr_in.port);
}

/// Find the flo binary path
pub fn findFloBinary(allocator: Allocator) ![]const u8 {
    // Try relative path from workspace root
    const paths_to_try = [_][]const u8{
        "zig-out/bin/flo",
        "./zig-out/bin/flo",
        "../zig-out/bin/flo",
        "../../zig-out/bin/flo",
    };

    for (paths_to_try) |rel_path| {
        const abs_path = stdx.fs.realpathAlloc(allocator, rel_path) catch continue;
        // Verify it exists (access check for existence)
        stdx.fs.access(rel_path, .{}) catch {
            allocator.free(abs_path);
            continue;
        };
        return abs_path;
    }

    // Try to find from current exe path (best-effort, macOS via _NSGetExecutablePath)
    var exe_buf: [4096]u8 = undefined;
    const self_exe = blk: {
        if (builtin.os.tag == .macos) {
            var len: u32 = exe_buf.len;
            if (std.c._NSGetExecutablePath(&exe_buf, &len) != 0) return error.FloBinaryNotFound;
            break :blk std.mem.sliceTo(&exe_buf, 0);
        } else if (builtin.os.tag == .linux) {
            const n = std.c.readlink("/proc/self/exe", &exe_buf, exe_buf.len);
            if (n <= 0) return error.FloBinaryNotFound;
            break :blk exe_buf[0..@intCast(n)];
        } else {
            return error.FloBinaryNotFound;
        }
    };

    const dir = std.fs.path.dirname(self_exe) orelse return error.FloBinaryNotFound;
    const flo_path = try std.fs.path.join(allocator, &.{ dir, "flo" });

    stdx.fs.access(flo_path, .{}) catch {
        allocator.free(flo_path);
        return error.FloBinaryNotFound;
    };

    return flo_path;
}

/// Count non-overlapping occurrences of needle in haystack
fn countOccurrences(haystack: []const u8, needle: []const u8) usize {
    if (needle.len == 0 or haystack.len < needle.len) return 0;

    var count: usize = 0;
    var pos: usize = 0;

    while (pos <= haystack.len - needle.len) {
        if (std.mem.eql(u8, haystack[pos..][0..needle.len], needle)) {
            count += 1;
            pos += needle.len; // Skip past this occurrence (non-overlapping)
        } else {
            pos += 1;
        }
    }
    return count;
}

/// Copy server output streams to log file (runs in background thread).
/// Uses poll() to read both stdout and stderr concurrently, preventing
/// the classic pipe deadlock where the server blocks writing to one pipe
/// while we're blocked reading the other.
fn logServerOutput(log_file: std.Io.File, stdout_fd: std.posix.fd_t, stderr_fd: std.posix.fd_t, stop: *const std.atomic.Value(bool)) void {
    defer stdx.fs.closeFile(log_file);
    // This thread owns the read ends: nothing else may close them while it polls.
    defer _ = std.c.close(stdout_fd);
    defer _ = std.c.close(stderr_fd);

    var fds = [_]std.posix.pollfd{
        .{ .fd = stdout_fd, .events = std.posix.POLL.IN, .revents = 0 },
        .{ .fd = stderr_fd, .events = std.posix.POLL.IN, .revents = 0 },
    };

    var buf: [4096]u8 = undefined;
    var open_count: usize = 2;

    // Once asked to stop, drain for at most a second: whatever still holds
    // the pipe (a grandchild), quiet or writing, must not keep the harness
    // waiting.
    var stop_at: ?i64 = null;
    while (open_count > 0) {
        if (stop_at == null and stop.load(.acquire)) stop_at = stdx.time.milliTimestamp();
        if (stop_at) |t| if (stdx.time.milliTimestamp() - t > 1000) break;
        const ready = std.posix.poll(&fds, 100) catch break;
        if (ready == 0) {
            if (stop_at != null) break;
            continue;
        }

        for (&fds) |*pfd| {
            if (pfd.fd < 0) continue;

            if (pfd.revents & std.posix.POLL.IN != 0) {
                const n = std.posix.read(pfd.fd, &buf) catch {
                    pfd.fd = -1;
                    open_count -= 1;
                    continue;
                };
                if (n == 0) {
                    pfd.fd = -1;
                    open_count -= 1;
                    continue;
                }
                stdx.fs.writeAll(log_file, buf[0..n]) catch {};
            } else if (pfd.revents & (std.posix.POLL.HUP | std.posix.POLL.ERR | std.posix.POLL.NVAL) != 0) {
                pfd.fd = -1;
                open_count -= 1;
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

test "ServerProcess: findFreePort returns valid port" {
    const port = try findFreePort();
    try testing.expect(port > 0);
    try testing.expect(port >= 1024); // Typically not privileged
}

test "ServerProcess: init and deinit" {
    var server = try ServerProcess.init(testing.allocator);
    defer server.deinit();

    try testing.expect(!server.isRunning());
    try testing.expect(server.data_dir.len > 0);
}
