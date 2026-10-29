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
const Watchdog = @import("watchdog.zig").Watchdog;
const awaitExit = @import("watchdog.zig").awaitExit;
const Allocator = std.mem.Allocator;
const testing = std.testing;

/// One e2e run per checkout. Each run kills the checkout's leftover servers
/// (below), so a second run alongside the first would kill its live ones.
/// The second is refused instead. The lock is an flock the OS drops when the
/// holder exits, however it exits, so a crashed run leaves none behind.
pub const RUN_LOCK_NAME = ".zig-cache/e2e-run.lock";

pub fn takeRunLock(checkout: []const u8) !stdx.fs.File {
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, "{s}/{s}", .{ checkout, RUN_LOCK_NAME });
    return stdx.fs.createFile(path, .{ .truncate = false, .lock = .exclusive, .lock_nonblocking = true }) catch |err| switch (err) {
        error.WouldBlock => return error.E2eRunInProgress,
        else => return err,
    };
}

/// The checkout a flo binary was built in: `<checkout>/zig-out/bin/flo`.
pub fn checkoutOf(binary: []const u8) ?[]const u8 {
    const bin = std.fs.path.dirname(binary) orelse return null;
    const out = std.fs.path.dirname(bin) orelse return null;
    return std.fs.path.dirname(out);
}

/// Claimed once per test process, at its first server start, before
/// anything this process starts is running: take the checkout's run lock,
/// then kill the servers its binary left running, those of an earlier run
/// that crashed or of a test the runner killed at its time limit. The runner
/// kills only the test process, and each server leads its own process group,
/// so it outlives the test as an orphan; the runner restarts the process
/// after a timeout, so the next test clears the previous one's. Only servers
/// of this checkout's own binary: a pattern any flo matches would kill other
/// checkouts' runs and hand-started servers.
const Claim = enum { pending, held, refused };
var claim: Claim = .pending;
var claim_mutex: stdx.Mutex = .{};
var run_lock: ?stdx.fs.File = null;

fn claimCheckout() !void {
    claim_mutex.lock();
    defer claim_mutex.unlock();
    switch (claim) {
        .held => return,
        .refused => return error.E2eRunInProgress,
        .pending => {},
    }
    if (builtin.target.os.tag != .macos and builtin.target.os.tag != .linux) {
        claim = .held;
        return;
    }
    const binary = findFloBinary(std.heap.page_allocator) catch {
        claim = .held;
        return;
    };
    defer std.heap.page_allocator.free(binary);
    if (checkoutOf(binary)) |checkout| {
        run_lock = takeRunLock(checkout) catch |err| {
            if (err == error.E2eRunInProgress) {
                claim = .refused;
                std.debug.print("another e2e run holds {s}; wait for it or stop it\n", .{checkout});
            }
            return err;
        };
    }
    claim = .held;

    // pkill -f takes an extended regex: escape the path so it matches only itself.
    var pattern: std.ArrayListUnmanaged(u8) = .empty;
    defer pattern.deinit(std.heap.page_allocator);
    pattern.append(std.heap.page_allocator, '^') catch return;
    for (binary) |c| {
        if (std.mem.indexOfScalar(u8, "\\.^$|?*+()[]{}", c) != null) pattern.append(std.heap.page_allocator, '\\') catch return;
        pattern.append(std.heap.page_allocator, c) catch return;
    }
    pattern.appendSlice(std.heap.page_allocator, " server ") catch return;
    const r = std.process.run(std.heap.page_allocator, stdx.io.instance(), .{
        .argv = &.{ "pkill", "-9", "-f", pattern.items },
    }) catch return;
    std.heap.page_allocator.free(r.stdout);
    std.heap.page_allocator.free(r.stderr);
    // pkill exits 0 only when it matched: then give the killed servers a
    // moment to release their ports.
    if (r.term == .exited and r.term.exited == 0) stdx.time.sleep(500 * std.time.ns_per_ms);
}

/// For a helper process an e2e test starts: the test holds the checkout's
/// claim, so the helper neither takes the lock nor clears the checkout's
/// servers, which would include the test's own.
pub fn claimHeldByParent() void {
    claim_mutex.lock();
    defer claim_mutex.unlock();
    claim = .held;
}

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
    /// The raw wait status, once reaped.
    exit_status: ?c_int = null,
    /// Signals the harness itself sent this process, so the exit they cause
    /// is expected; any other signal death is not.
    sent_term: bool = false,
    sent_kill: bool = false,
    /// A test awaited this run's crash (`awaitCrash`), so its signal death
    /// is expected.
    crash_awaited: bool = false,
    /// Set in the server's environment at its next spawn, over what this
    /// process has.
    extra_env: []const [2][]const u8 = &.{},
    tmp_dir: testing.TmpDir,
    flo_binary: []const u8,
    started: bool,
    config: ServerConfig,
    log_thread: ?std.Thread = null,
    /// Kills the server's group if this test process dies first.
    watchdog: ?*Watchdog = null,
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
    /// Each server binds ports the OS picks, so a short wait suffices
    pub const POST_KILL_WAIT_MS: u64 = 100;

    /// Durability mode for persistence
    pub const Durability = enum {
        /// Wait for fdatasync before returning - data guaranteed on disk
        sync,
        /// Return after WAL append (async flush) - fast but may lose recent writes on crash
        async_flush,

        fn toConfigString(self: Durability) []const u8 {
            return switch (self) {
                .sync => "sync",
                .async_flush => "async_flush",
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
        /// Durability mode (sync = guaranteed persistence, async_flush = fast)
        durability: Durability = .async_flush,
        /// Serve from this data dir rather than the node's own, e.g. another
        /// node's. Config and log stay in the node's own.
        data_dir_of: ?[]const u8 = null,
        /// Tiered log configuration (for controlling hot→warm transitions)
        tiered_log: TieredLogConfig = .{},

        // Cluster configuration
        /// Join addresses for cluster mode (e.g., "127.0.0.1:4445")
        join_addresses: ?[]const u8 = null,
        /// Node ID. 0 gives a cluster node one from `nextNodeId`, since a
        /// node on port 0 refuses to derive its own.
        node_id: u32 = 0,
        /// Start a cluster node with no node id, so it derives one.
        derive_node_id: bool = false,
        /// Raft RPC port (0 = auto-assign)
        raft_port: u16 = 0,
        /// Enable cluster mode (starts Raft listener) - for seed nodes without join_addresses
        cluster_enabled: bool = false,
        /// Written to `[cluster] secret` whenever the peer listener will start.
        /// Every node of a test cluster shares the default; a test that wants a
        /// stranger sets its own.
        /// In the form `flo server secret` prints; a cluster takes no other.
        cluster_secret: []const u8 = "flo-secret-" ++ @import("../../mod.zig").repeat("e2e0", 16),
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
        self.exit_status = null;
        self.sent_term = false;
        self.sent_kill = false;
        self.crash_awaited = false;
        self.extra_env = &.{};
        self.dump_log_on_failure = true;

        return self;
    }

    /// Clean up all resources
    pub fn deinit(self: *Self) void {
        // Stop the server if it runs, awaited or not: its log thread reads
        // this struct until it is joined.
        self.stop();

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
        const start_ms = stdx.time.milliTimestamp();
        try self.launch();
        self.awaitPorts(timeout_ms);
        const spent: u64 = @intCast(stdx.time.milliTimestamp() - start_ms);
        try self.awaitReady(timeout_ms -| spent);
    }

    /// Write the config and start the process, without waiting for it to
    /// be ready. The server binds port 0 for each listener and reports what
    /// it bound, so no port is picked here and handed out later. `port`,
    /// `metrics_port` and `dashboard_port` are set on return, once its HTTP
    /// listeners are up (0 if it exited first); `raft_port` once ready.
    /// Follow with `awaitReady`.
    pub fn spawn(self: *Self) !void {
        try self.launch();
        self.awaitPorts(DEFAULT_READY_TIMEOUT_MS);
    }

    fn launch(self: *Self) !void {
        if (self.started or self.process != null) return error.AlreadyStarted;

        try claimCheckout();

        self.port = 0;
        self.dashboard_port = 0;
        self.metrics_port = 0;

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
        try config_writer.print("\n[dashboard]\nenabled = {}\n", .{self.config.dashboard_enabled});
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

        const shards_str = try std.fmt.allocPrint(self.allocator, "{d}", .{self.config.shards});
        defer self.allocator.free(shards_str);

        // The peer port: the configured one, else the one this node bound
        // when it first started (its peers dial the address they know), else
        // 0 for the server to pick on a first start.
        if (self.config.raft_port > 0) self.raft_port = self.config.raft_port;
        const cluster_node = self.config.join_addresses != null or self.config.cluster_enabled;
        if (cluster_node and self.config.node_id == 0 and !self.config.derive_node_id) self.config.node_id = nextNodeId();

        // Build argv dynamically
        var argv_list: std.ArrayListUnmanaged([]const u8) = .empty;
        defer argv_list.deinit(self.allocator);

        try argv_list.appendSlice(self.allocator, &.{
            self.flo_binary,
            "server",
            "start",
            "--port",
            "0",
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
        var env: ?std.process.Environ.Map = null;
        defer if (env) |*m| m.deinit();
        if (self.extra_env.len > 0) {
            env = try stdx.process.currentEnv(self.allocator);
            for (self.extra_env) |kv| try env.?.put(kv[0], kv[1]);
            child.env_map = &env.?;
        }
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;
        // Put each child in its own process group so killing one doesn't kill others
        child.pgid = 0; // 0 means use child's PID as process group ID

        try child.spawn();
        self.watchdog = Watchdog.start(self.allocator, child.id) catch |err| {
            _ = std.c.kill(-child.id, .KILL);
            _ = child.wait() catch {};
            return err;
        };

        // Redirect stdout/stderr to log file in background
        // Store process immediately so we can kill it if readiness check fails
        self.process = child;

        // Extract fds by value before spawning thread — avoids dangling
        // pointers when stop() sets self.process = null
        const stdout_fd = self.process.?.stdout.?.handle;
        const stderr_fd = self.process.?.stderr.?.handle;
        self.reaped = false;
        self.exit_status = null;
        self.sent_term = false;
        self.sent_kill = false;
        self.crash_awaited = false;
        self.log_stop.store(false, .release);
        self.log_thread = try std.Thread.spawn(.{}, logServerOutput, .{ log_file, stdout_fd, stderr_fd, &self.log_stop });
        log_file_handed_off = true; // Thread now owns the fd
    }

    /// Wait for the server's ports line, printed once its HTTP listeners
    /// are up and before its shards replay. A server that exits or stalls
    /// first leaves the ports 0, and `awaitReady` reports it.
    fn awaitPorts(self: *Self, timeout_ms: u64) void {
        const start_ms = stdx.time.milliTimestamp();
        while (stdx.time.milliTimestamp() - start_ms < @as(i64, @intCast(timeout_ms))) {
            if (self.serverLine(PORTS_LINE)) |line| {
                defer self.allocator.free(line);
                self.port = portAfter(line, "client ") orelse 0;
                self.metrics_port = portAfter(line, "metrics ") orelse 0;
                self.dashboard_port = portAfter(line, "dashboard ") orelse 0;
                return;
            }
            if (self.process) |*proc| if (self.tryReap(proc.id)) return;
            stdx.time.sleep(READY_POLL_INTERVAL_MS * std.time.ns_per_ms);
        }
    }

    /// The first whole line of the server's output that starts with
    /// `prefix`, owned by the caller; null while there is none. Only the
    /// head of the log is read: both lines come before the server serves,
    /// and a busy server's log can outgrow `readLogs`.
    fn serverLine(self: *Self, prefix: []const u8) ?[]const u8 {
        const file = stdx.fs.openFileAbsolute(self.log_file_path, .{}) catch return null;
        defer stdx.fs.closeFile(file);
        const head = self.allocator.alloc(u8, 1 << 20) catch return null;
        defer self.allocator.free(head);
        const n = stdx.fs.readAll(file, head) catch return null;
        var lines = std.mem.splitScalar(u8, head[0..n], '\n');
        while (lines.next()) |line| {
            // A line still being written has no newline yet.
            if (lines.index == null) break;
            if (std.mem.startsWith(u8, line, prefix)) return self.allocator.dupe(u8, line) catch null;
        }
        return null;
    }

    /// Wait for a server `spawn` started to accept connections; on failure
    /// show its log tail and kill it.
    pub fn awaitReady(self: *Self, timeout_ms: u64) !void {
        self.waitForReady(timeout_ms) catch |err| {
            // The server's own output goes to a log file, so without this a
            // startup failure surfaces only as this harness's timeout and the
            // real bind error or panic never reaches the test output.
            if (self.dump_log_on_failure) {
                if (self.reaped) {
                    std.debug.print("[server] port {d}: exited before it was ready\n", .{self.port});
                } else {
                    std.debug.print("[server] port {d}: not ready within {d} ms\n", .{ self.port, timeout_ms });
                }
                self.dumpLogTail();
            }
            self.forceKill();
            return err;
        };

        self.started = true;
    }

    /// Stop the server gracefully, then fail the test (see `checkExit`) if
    /// it ended in a way this didn't cause.
    pub fn stop(self: *Self) void {
        // A server `spawn` started that was never awaited is stopped too.
        if (!self.started and self.process == null) return;

        if (self.process) |*proc| {
            const pid = proc.id;
            var forced = false;

            if (!self.tryReap(pid)) {
                // Send SIGTERM for graceful shutdown
                self.sent_term = true;
                _ = std.c.kill(pid, .TERM);

                const grace_start = stdx.time.milliTimestamp();
                while (!self.tryReap(pid) and stdx.time.milliTimestamp() - grace_start < @as(i64, @intCast(SHUTDOWN_GRACE_PERIOD_MS))) {
                    stdx.time.sleep(50 * std.time.ns_per_ms);
                }

                if (!self.reaped) {
                    // Server didn't respond to SIGTERM, force kill its group
                    forced = true;
                    self.sent_kill = true;
                    if (std.c.kill(-pid, .KILL) != 0) _ = std.c.kill(pid, .KILL);
                    const kill_start = stdx.time.milliTimestamp();
                    while (!self.tryReap(pid) and stdx.time.milliTimestamp() - kill_start < 1000) {
                        stdx.time.sleep(10 * std.time.ns_per_ms);
                    }
                }
            }

            // Brief wait for OS resource release after a forced kill
            if (forced) stdx.time.sleep(POST_KILL_WAIT_MS * std.time.ns_per_ms);

            self.joinLogThread();
            self.checkExit();
            // Still armed only if the server couldn't be reaped; it then
            // stays a zombie, so its group id isn't reused.
            self.releaseWatchdog();
            self.process = null;
        }

        self.started = false;
    }

    fn releaseWatchdog(self: *Self) void {
        if (self.watchdog) |w| w.release();
        self.watchdog = null;
    }

    /// Wait up to `timeout_ms` for the server to die of a signal, as a test
    /// that makes it crash expects; returns the signal. A clean exit, or
    /// none in time, is an error.
    pub fn awaitCrash(self: *Self, timeout_ms: u64) !u8 {
        const pid = (self.process orelse return error.NotRunning).id;
        const start_ms = stdx.time.milliTimestamp();
        while (!self.tryReap(pid)) {
            if (stdx.time.milliTimestamp() - start_ms > @as(i64, @intCast(timeout_ms))) return error.ServerDidNotCrash;
            stdx.time.sleep(20 * std.time.ns_per_ms);
        }
        const raw: u32 = @bitCast(self.exit_status.?);
        const sig: u8 = @intCast(raw & 0x7f);
        if (sig == 0 or sig == 0x7f) return error.ServerDidNotCrash;
        self.crash_awaited = true;
        return sig;
    }

    /// Collect the child if it has exited, keeping its status.
    fn tryReap(self: *Self, pid: std.c.pid_t) bool {
        if (self.reaped) return true;
        // Stand the watchdog down while the exited server is still unreaped.
        if (self.watchdog != null) {
            if (!(awaitExit(pid, false) catch true)) return false;
            self.releaseWatchdog();
        }
        var status: c_int = 0;
        if (std.c.waitpid(pid, &status, std.posix.W.NOHANG) != pid) return false;
        self.reaped = true;
        self.exit_status = status;
        return true;
    }

    /// Why the server's exit wasn't one the harness caused, or null: a
    /// non-zero exit code, or death by a signal it didn't send. A SIGKILL it
    /// didn't send is called out, since the OOM killer is the usual sender.
    pub fn abnormalExit(self: *const Self, buf: []u8) ?[]const u8 {
        const raw: u32 = @bitCast(self.exit_status orelse return null);
        if (self.crash_awaited) return null;
        const low = raw & 0x7f;
        if (low == 0) {
            const code = (raw >> 8) & 0xff;
            if (code == 0) return null;
            return std.fmt.bufPrint(buf, "exited with code {d}", .{code}) catch "exited with a non-zero code";
        }
        if (low == 0x7f) return null; // stopped, not ended
        if (low == @backingInt(std.posix.SIG.TERM) and self.sent_term) return null;
        if (low == @backingInt(std.posix.SIG.KILL)) {
            if (self.sent_kill) return null;
            return "killed by a SIGKILL the harness didn't send (memory pressure, or another run's cleanup?)";
        }
        return std.fmt.bufPrint(buf, "died of signal {d}", .{low}) catch "died of a signal";
    }

    /// A server that panics or aborts after a test's last read would leave
    /// the test passing; stop() runs inside deinit and can't return an error,
    /// so this panics, after the reason and the log tail, to fail loudly.
    fn checkExit(self: *Self) void {
        var buf: [160]u8 = undefined;
        const why = self.abnormalExit(&buf) orelse return;
        std.debug.print("[server] port {d}: {s}\n", .{ self.port, why });
        self.dumpLogTail();
        @panic("e2e server ended abnormally; its log is above");
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
            if (self.tryReap(proc.id)) {
                std.debug.print("[server] port {d}: process already exited (raw status {d})\n", .{ self.port, self.exit_status orelse 0 });
            } else {
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
                self.sent_kill = true;
                if (std.c.kill(-pid, .KILL) != 0) _ = std.c.kill(pid, .KILL);
                const kill_start = stdx.time.milliTimestamp();
                while (!self.tryReap(pid) and stdx.time.milliTimestamp() - kill_start < 2000) {
                    stdx.time.sleep(10 * std.time.ns_per_ms);
                }
                // A zombie is better than a hung run.
                if (!self.reaped) std.debug.print("[server] pid {d} not reaped after SIGKILL; moving on\n", .{pid});
            }
            stdx.time.sleep(POST_KILL_WAIT_MS * std.time.ns_per_ms);
            self.joinLogThread();
            self.releaseWatchdog();
            self.process = null;
        }
        self.started = false;
    }

    /// Wait for the server's ready line, printed once it serves clients,
    /// and take the ports it names.
    fn waitForReady(self: *Self, timeout_ms: u64) !void {
        const start_time = stdx.time.milliTimestamp();

        while (stdx.time.milliTimestamp() - start_time < @as(i64, @intCast(timeout_ms))) {
            if (self.serverLine(READY_LINE)) |line| {
                defer self.allocator.free(line);
                if (portAfter(line, "peer ")) |peer| self.raft_port = peer;
                return;
            }
            // A server that refused its config has already exited; waiting
            // out the timeout for it cost each refused-start test the bound.
            if (self.process) |*proc| if (self.tryReap(proc.id)) return error.ServerNotReady;

            stdx.time.sleep(READY_POLL_INTERVAL_MS * std.time.ns_per_ms);
        }

        return error.ServerNotReady;
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

/// Node ids for this process's cluster nodes: distinct within it, and
/// unlikely to match another test process's.
var next_node_id: std.atomic.Value(u32) = .init(0);

fn nextNodeId() u32 {
    if (next_node_id.load(.monotonic) == 0) {
        const base: u32 = @as(u32, @intCast(@mod(std.c.getpid(), 40_000))) * 100_000 + 1;
        _ = next_node_id.cmpxchgStrong(0, base, .monotonic, .monotonic);
    }
    return next_node_id.fetchAdd(1, .monotonic);
}

const PORTS_LINE = "Flo server ports: ";
const READY_LINE = "Flo server ready on ";

/// The port after `key` in one of the server's port lines (`client 52341`,
/// `peer none`); null for none, or for a key the line doesn't hold.
fn portAfter(line: []const u8, key: []const u8) ?u16 {
    const at = std.mem.indexOf(u8, line, key) orelse return null;
    const rest = line[at + key.len ..];
    var end: usize = 0;
    while (end < rest.len and std.ascii.isDigit(rest[end])) end += 1;
    return std.fmt.parseInt(u16, rest[0..end], 10) catch null;
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
        if (builtin.target.os.tag == .macos) {
            var len: u32 = exe_buf.len;
            if (std.c._NSGetExecutablePath(&exe_buf, &len) != 0) return error.FloBinaryNotFound;
            break :blk std.mem.sliceTo(&exe_buf, 0);
        } else if (builtin.target.os.tag == .linux) {
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

test "ServerProcess: the ports a server's lines name" {
    const ports = "Flo server ports: client 52341, metrics none, dashboard 52345";
    try testing.expectEqual(@as(?u16, 52341), portAfter(ports, "client "));
    try testing.expectEqual(@as(?u16, null), portAfter(ports, "metrics "));
    try testing.expectEqual(@as(?u16, 52345), portAfter(ports, "dashboard "));
    const ready = "Flo server ready on 0.0.0.0:52341 (peer 52342, metrics none, dashboard none)";
    try testing.expectEqual(@as(?u16, 52342), portAfter(ready, "peer "));
    try testing.expectEqual(@as(?u16, null), portAfter(ready, "client "));
}
