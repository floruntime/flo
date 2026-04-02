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
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const testing = std.testing;

/// Server process manager for e2e tests
pub const ServerProcess = struct {
    const Self = @This();

    allocator: Allocator,
    process: ?std.process.Child,
    port: u16,
    dashboard_port: u16,
    metrics_port: u16,
    raft_port: u16,
    data_dir: []const u8,
    config_file: []const u8,
    log_file_path: []const u8,
    tmp_dir: testing.TmpDir,
    flo_binary: []const u8,
    started: bool,
    config: ServerConfig,
    log_thread: ?std.Thread = null,
    /// Root admin API key from bootstrap (set when config.auth_enabled = true)
    api_key: ?[]const u8 = null,

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

    /// Cold storage provider for tests
    pub const ColdStorageProvider = enum {
        /// No cold storage
        none,
        /// Local filesystem
        file,

        fn toConfigString(self: ColdStorageProvider) []const u8 {
            return switch (self) {
                .none => "none",
                .file => "file",
            };
        }
    };

    /// Cold storage configuration for tests
    pub const ColdStorageConfig = struct {
        /// Provider type
        provider: ColdStorageProvider = .none,
        /// Base path for file provider (relative to data_dir)
        file_base_path: ?[]const u8 = null,
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
        /// Enable metrics HTTP server (Prometheus)
        metrics_enabled: bool = false,
        /// Number of shards (1 = faster startup)
        shards: u8 = 1,
        /// Durability mode (sync = guaranteed persistence, async_flush = fast, ephemeral = no persistence)
        durability: Durability = .async_flush,
        /// Cold storage configuration (for tiered storage tests)
        cold_storage: ColdStorageConfig = .{},
        /// Tiered log configuration (for controlling hot→warm transitions)
        tiered_log: TieredLogConfig = .{},

        // Background task intervals
        /// Namespace deletion task interval in milliseconds
        /// Default: 100ms for tests (faster cleanup than production's 5s)
        namespace_deletion_interval_ms: i64 = 100,

        // Cluster configuration
        /// Join addresses for cluster mode (e.g., "127.0.0.1:4445")
        join_addresses: ?[]const u8 = null,
        /// Node ID (0 = auto-generate)
        node_id: u32 = 0,
        /// Raft RPC port (0 = auto-assign)
        raft_port: u16 = 0,
        /// Enable cluster mode (starts Raft listener) - for seed nodes without join_addresses
        cluster_enabled: bool = false,
        /// Expose internal keys (prefixed with '_') in kv scan for testing
        /// When true, kv list/scan will show _proc:, _action:, etc. keys
        expose_internal_keys: bool = false,
        /// Enable auth bootstrapping — server will run `flo server bootstrap` on start
        /// and expose the root API key via ServerProcess.api_key
        auth_enabled: bool = false,
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
        self.data_dir = try self.tmp_dir.dir.realpathAlloc(allocator, ".");
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
        self.api_key = null;

        return self;
    }

    /// Clean up all resources
    pub fn deinit(self: *Self) void {
        // Stop server if still running
        if (self.started) {
            self.stop();
        }

        if (self.api_key) |k| self.allocator.free(k);
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
        var fbs = std.io.fixedBufferStream(&config_buf);
        const config_writer = fbs.writer();

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

        // Background task intervals
        try config_writer.print("[background_tasks]\nnamespace_deletion_interval_ms = {d}\n\n", .{self.config.namespace_deletion_interval_ms});

        // KV configuration (for testing internal key visibility)
        if (self.config.expose_internal_keys) {
            try config_writer.print("[kv]\nexpose_internal_keys = true\n\n", .{});
        }

        try config_writer.print("[metrics]\nenabled = {}\n", .{self.config.metrics_enabled});
        if (self.config.metrics_enabled) {
            try config_writer.print("port = {d}\n", .{self.metrics_port});
        }
        try config_writer.print("\n[dashboard]\nenabled = {}\n", .{self.config.dashboard_enabled});
        if (self.config.dashboard_enabled) {
            try config_writer.print("port = {d}\n", .{self.dashboard_port});
        }

        // Cold storage section (for tiered storage tests)
        if (self.config.cold_storage.provider != .none) {
            try config_writer.print("\n[cold_storage]\nprovider = \"{s}\"\n", .{self.config.cold_storage.provider.toConfigString()});
            if (self.config.cold_storage.file_base_path) |path| {
                try config_writer.print("file_base_path = \"{s}\"\n", .{path});
            } else {
                // Default to data_dir/archive
                try config_writer.print("file_base_path = \"{s}/archive\"\n", .{self.data_dir});
            }
        }

        const config_file = try self.tmp_dir.dir.createFile("flo.toml", .{});
        defer config_file.close();
        try config_file.writeAll(fbs.getWritten());

        // Open log file for output redirection
        var log_file = try std.fs.createFileAbsolute(self.log_file_path, .{
            .truncate = true,
        });
        // Track ownership: once handed to the logging thread, the thread owns
        // the fd and will close it. We must NOT double-close on error paths.
        var log_file_handed_off = false;
        errdefer if (!log_file_handed_off) log_file.close();

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
            self.data_dir,
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
        }

        var child = std.process.Child.init(argv_list.items, self.allocator);
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
        self.log_thread = try std.Thread.spawn(.{}, logServerOutput, .{ log_file, stdout_fd, stderr_fd });
        log_file_handed_off = true; // Thread now owns the fd

        // Wait for server to be ready
        self.waitForReady(timeout_ms) catch |err| {
            // If readiness check fails, clean up the process
            self.forceKill();
            return err;
        };

        self.started = true;

        // Run auth bootstrap if auth is enabled
        if (self.config.auth_enabled) {
            self.api_key = try self.runBootstrap();
        }
    }

    /// Stop the server gracefully
    pub fn stop(self: *Self) void {
        if (!self.started) return;

        if (self.process) |*proc| {
            const pid = proc.id;

            // Send SIGTERM for graceful shutdown
            _ = std.posix.kill(pid, std.posix.SIG.TERM) catch {};

            // Wait for graceful shutdown (check if process exits naturally)
            const grace_start = std.time.milliTimestamp();
            var gracefully_exited = false;

            while (std.time.milliTimestamp() - grace_start < @as(i64, @intCast(SHUTDOWN_GRACE_PERIOD_MS))) {
                // Try non-blocking wait to see if process exited
                const result = std.posix.waitpid(pid, std.posix.W.NOHANG);
                if (result.pid == pid) {
                    gracefully_exited = true;
                    break;
                }
                std.Thread.sleep(50 * std.time.ns_per_ms);
            }

            // Force kill if not gracefully exited
            if (!gracefully_exited) {
                // Server didn't respond to SIGTERM, force kill
                _ = std.posix.kill(pid, std.posix.SIG.KILL) catch {
                    return; // Can't kill process, abandon cleanup
                };

                // Wait for process to die (blocking wait with timeout)
                const kill_start = std.time.milliTimestamp();
                while (std.time.milliTimestamp() - kill_start < 1000) { // 1 second max
                    const result = std.posix.waitpid(pid, std.posix.W.NOHANG);
                    if (result.pid == pid) {
                        break;
                    }
                    std.Thread.sleep(10 * std.time.ns_per_ms);
                }
            }

            // Brief wait for OS resource release (only needed after forced kill;
            // each test uses unique ports via findFreePort, so minimal delay suffices)
            if (!gracefully_exited) {
                std.Thread.sleep(POST_KILL_WAIT_MS * std.time.ns_per_ms);
            }

            // Join the log thread — process kill closes pipes, unblocking reads
            if (self.log_thread) |thread| {
                thread.join();
                self.log_thread = null;
            }

            self.process = null;
        }

        self.started = false;
    }

    // =========================================================================
    // Server Log Access
    // =========================================================================

    /// Read server logs (returns owned slice - caller must free)
    pub fn readLogs(self: *Self) ![]const u8 {
        const file = std.fs.openFileAbsolute(self.log_file_path, .{}) catch |err| {
            if (err == error.FileNotFound) return try self.allocator.dupe(u8, "");
            return err;
        };
        defer file.close();

        return file.readToEndAlloc(self.allocator, 10 * 1024 * 1024); // Max 10MB
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

    /// Run `flo server bootstrap` against this server and return the API key.
    /// Caller owns the returned slice.
    fn runBootstrap(self: *Self) ![]const u8 {
        const bootstrap_file = try std.fmt.allocPrint(self.allocator, "{s}/bootstrap.key", .{self.data_dir});
        defer self.allocator.free(bootstrap_file);

        const argv = &[_][]const u8{
            self.flo_binary,
            "server",
            "bootstrap",
            "--data-dir",
            self.data_dir,
            "--out",
            bootstrap_file,
        };

        var child = std.process.Child.init(argv, self.allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;
        try child.spawn();

        var stdout_list: std.ArrayList(u8) = .empty;
        var stderr_list: std.ArrayList(u8) = .empty;
        defer stdout_list.deinit(self.allocator);
        defer stderr_list.deinit(self.allocator);

        try child.collectOutput(self.allocator, &stdout_list, &stderr_list, 64 * 1024);
        const term = try child.wait();
        const exit_code: u8 = switch (term) {
            .Exited => |code| code,
            else => 255,
        };

        if (exit_code != 0) {
            std.debug.print("[e2e] bootstrap failed (exit {d}): {s}\n", .{ exit_code, stderr_list.items });
            return error.BootstrapFailed;
        }

        // Read the key from the output file
        const key_raw = std.fs.cwd().readFileAlloc(self.allocator, bootstrap_file, 1024) catch {
            // Fall back to parsing stdout if file read fails
            const out = std.mem.trim(u8, stdout_list.items, &std.ascii.whitespace);
            // Extract the key token starting with "flo_sk_"
            if (std.mem.indexOf(u8, out, "flo_sk_")) |pos| {
                const key_start = out[pos..];
                const end = std.mem.indexOfAny(u8, key_start, &std.ascii.whitespace) orelse key_start.len;
                return try self.allocator.dupe(u8, key_start[0..end]);
            }
            return error.BootstrapKeyNotFound;
        };
        defer self.allocator.free(key_raw);

        const key = std.mem.trim(u8, key_raw, &std.ascii.whitespace);
        return try self.allocator.dupe(u8, key);
    }

    /// Force kill the server process
    fn forceKill(self: *Self) void {
        if (self.process) |*proc| {
            _ = std.posix.kill(proc.id, std.posix.SIG.KILL) catch {};
            _ = proc.wait() catch {};

            // Wait for OS to release resources
            std.Thread.sleep(POST_KILL_WAIT_MS * std.time.ns_per_ms);

            // Join log thread after process is dead
            if (self.log_thread) |thread| {
                thread.join();
                self.log_thread = null;
            }

            self.process = null;
        }
        self.started = false;
    }

    /// Wait for server to be ready (accepting TCP connections)
    fn waitForReady(self: *Self, timeout_ms: u64) !void {
        const start_time = std.time.milliTimestamp();

        while (std.time.milliTimestamp() - start_time < @as(i64, @intCast(timeout_ms))) {
            // Check main port — this is always required
            const main_ready = self.tryConnect(self.port);

            // Check dashboard port if enabled (wired into runtime)
            const dashboard_ready = if (self.config.dashboard_enabled and self.dashboard_port > 0)
                self.tryConnect(self.dashboard_port)
            else
                true;

            // NOTE: Metrics and raft port checks are disabled because
            // the rewrite has not yet wired those listeners into the runtime.
            //   metrics:   src/metrics/ needs startup in runtime.zig
            //   raft:      src/raft/transport.zig needs TCP listener in runtime.zig

            if (main_ready and dashboard_ready) {
                return;
            }

            std.Thread.sleep(READY_POLL_INTERVAL_MS * std.time.ns_per_ms);
        }

        return error.ServerNotReady;
    }

    /// Try to establish a TCP connection to a port
    fn tryConnect(self: *Self, port: u16) bool {
        _ = self;
        const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, port);
        const stream = std.net.tcpConnectToAddress(addr) catch {
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

    /// Get the Raft endpoint for --join (127.0.0.1:raft_port)
    pub fn getRaftEndpoint(self: *const Self, allocator: Allocator) ![]const u8 {
        return std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{self.raft_port});
    }

    /// Get the dashboard port (0 if not enabled)
    pub fn getDashboardPort(self: *const Self) u16 {
        return self.dashboard_port;
    }

    /// Get the metrics port (0 if not enabled)
    pub fn getMetricsPort(self: *const Self) u16 {
        return self.metrics_port;
    }

    /// Get the endpoint string (127.0.0.1:port)
    pub fn getEndpoint(self: *const Self, allocator: Allocator) ![]const u8 {
        return std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{self.port});
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
pub fn findFreePort() !u16 {
    const sock = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    defer std.posix.close(sock);

    // Allow reuse of TIME_WAIT ports — critical when running hundreds of tests
    // back-to-back, each spawning/stopping server processes
    std.posix.setsockopt(sock, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1))) catch {};

    var addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);
    try std.posix.bind(sock, &addr.any, @sizeOf(std.posix.sockaddr.in));

    var bound_addr: std.posix.sockaddr align(4) = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    try std.posix.getsockname(sock, &bound_addr, &len);

    // Extract port from sockaddr_in
    const addr_in: *const std.posix.sockaddr.in = @ptrCast(@alignCast(&bound_addr));
    return std.mem.bigToNative(u16, addr_in.port);
}

/// Find the flo binary path
fn findFloBinary(allocator: Allocator) ![]const u8 {
    // Try relative path from workspace root
    const paths_to_try = [_][]const u8{
        "zig-out/bin/flo",
        "./zig-out/bin/flo",
        "../zig-out/bin/flo",
        "../../zig-out/bin/flo",
    };

    for (paths_to_try) |rel_path| {
        const abs_path = std.fs.cwd().realpathAlloc(allocator, rel_path) catch continue;
        // Verify it exists (access check for existence)
        std.fs.cwd().access(rel_path, .{}) catch {
            allocator.free(abs_path);
            continue;
        };
        return abs_path;
    }

    // Try to find from current exe path
    const self_exe = try std.fs.selfExePathAlloc(allocator);
    defer allocator.free(self_exe);

    const dir = std.fs.path.dirname(self_exe) orelse return error.FloBinaryNotFound;
    const flo_path = try std.fs.path.join(allocator, &.{ dir, "flo" });

    std.fs.cwd().access(flo_path, .{}) catch {
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
fn logServerOutput(log_file: std.fs.File, stdout_fd: std.posix.fd_t, stderr_fd: std.posix.fd_t) void {
    defer log_file.close();

    var fds = [_]std.posix.pollfd{
        .{ .fd = stdout_fd, .events = std.posix.POLL.IN, .revents = 0 },
        .{ .fd = stderr_fd, .events = std.posix.POLL.IN, .revents = 0 },
    };

    var buf: [4096]u8 = undefined;
    var open_count: usize = 2;

    while (open_count > 0) {
        const ready = std.posix.poll(&fds, -1) catch break;
        if (ready == 0) continue;

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
                _ = log_file.write(buf[0..n]) catch {};
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
