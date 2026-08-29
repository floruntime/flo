//! Cluster Test Context
//!
//! Manages a multi-node Flo cluster for end-to-end testing.
//! Supports configurable node count (2-5 nodes) with automatic
//! cluster formation via Raft.
//!
//! ## Usage
//! ```zig
//! const stdx = @import("stdx");
//!
//! test "cluster test" {
//!     // Create a 3-node cluster
//!     var cluster = try stdx.testing.ClusterContext.init(allocator, .{ .node_count = 3 });
//!     defer cluster.deinit();
//!
//!     // Execute on specific node
//!     try cluster.execOn(0, &.{ "kv", "set", "key", "value" });
//!
//!     // Capture output from another node
//!     const value = try cluster.execCaptureOn(1, &.{ "kv", "get", "key" });
//!     defer allocator.free(value);
//!
//!     // Stop a specific node (for failure testing)
//!     cluster.stopNode(0);
//!
//!     // Restart a stopped node
//!     try cluster.restartNode(0);
//! }
//! ```

const std = @import("std");
const stdx = @import("../../mod.zig");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const ServerProcess = @import("server.zig").ServerProcess;
const CliRunner = @import("client.zig").CliRunner;

/// Kill any stale flo server processes from crashed previous tests.
/// This prevents port conflicts and resource contention.
fn cleanupStaleProcesses() void {
    // Use pkill to kill any lingering flo processes
    // This is a best-effort cleanup - ignore errors
    if (builtin.os.tag == .macos or builtin.os.tag == .linux) {
        const io = stdx.io.instance();
        if (std.process.run(std.heap.page_allocator, io, .{
            .argv = &.{ "pkill", "-9", "-f", "flo server" },
        })) |r| {
            std.heap.page_allocator.free(r.stdout);
            std.heap.page_allocator.free(r.stderr);
        } else |_| {}

        if (std.process.run(std.heap.page_allocator, io, .{
            .argv = &.{ "pkill", "-9", "flo" },
        })) |r| {
            std.heap.page_allocator.free(r.stdout);
            std.heap.page_allocator.free(r.stderr);
        } else |_| {}

        // Brief wait for processes to terminate and release resources
        stdx.time.sleep(500 * std.time.ns_per_ms);
    }
}

/// Start a server with retry: under heavy test load, port allocation
/// or readiness checks can transiently fail.
fn startServerWithRetry(server: *ServerProcess) !void {
    const max_retries: u8 = 3;
    var attempt: u8 = 0;
    while (true) {
        // Only the last attempt prints the server's log tail; the earlier ones
        // are usually the same failure and would triple the output (#54).
        server.dump_log_on_failure = (attempt + 1 >= max_retries);
        server.start() catch |err| {
            attempt += 1;
            if (attempt >= max_retries) return err;
            std.debug.print("[cluster] Server start attempt {d}/{d} failed, retrying...\n", .{ attempt, max_retries });
            stdx.time.sleep(1000 * std.time.ns_per_ms);
            continue;
        };
        return;
    }
}

/// Maximum number of nodes supported in a test cluster
pub const MAX_NODES = 5;

/// Configuration for cluster initialization
pub const ClusterConfig = struct {
    /// Number of nodes (2-5)
    node_count: u8 = 3,
    /// Cluster nodes log at debug by default. Startup between "topology
    /// verified" and the acceptor going live emits only debug lines, so at
    /// info a node that hangs in there leaves no trace of how far it got —
    /// which is exactly the state issue #54 is stuck in. The log is only ever
    /// printed as a bounded tail on failure, so this costs nothing on success.
    log_level: []const u8 = "debug",
    /// Durability mode for all nodes
    durability: ServerProcess.Durability = .sync,
    /// Time to wait for cluster formation (nanoseconds)
    /// Increased to allow peer discovery and stable leader election
    formation_delay_ns: u64 = 6 * std.time.ns_per_s,
    /// Number of shards per node
    shards: u8 = 1,
    /// Tiered-log config applied to every node (e.g. a small hot_buffer_capacity
    /// to force hot-ring wrap/spill in replication tests).
    tiered_log: ServerProcess.TieredLogConfig = .{},
    /// Pre-test cleanup: kill lingering flo processes from crashed tests
    /// Set to false only if you're sure no stale processes exist
    cleanup_stale_processes: bool = true,
    /// Pre-test cooldown: wait for TCP TIME_WAIT sockets to clear
    /// Helps when running tests repeatedly - set to 0 to skip
    pre_test_cooldown_ms: u64 = 2000,
};

/// Multi-node cluster context for e2e testing
pub const ClusterContext = struct {
    const Self = @This();

    allocator: Allocator,
    config: ClusterConfig,
    servers: [MAX_NODES]?*ServerProcess,
    clis: [MAX_NODES]?*CliRunner,
    endpoints: [MAX_NODES]?[]const u8,
    node_count: u8,

    /// Initialize a cluster with the given configuration
    pub fn init(allocator: Allocator, config: ClusterConfig) !Self {
        if (config.node_count < 2 or config.node_count > MAX_NODES) {
            return error.InvalidNodeCount;
        }

        // Pre-test cleanup: kill any stale flo processes from crashed tests
        if (config.cleanup_stale_processes) {
            cleanupStaleProcesses();
        }

        // Pre-test cooldown: wait for TCP TIME_WAIT sockets to clear
        // This helps prevent port conflicts when tests run back-to-back
        if (config.pre_test_cooldown_ms > 0) {
            std.debug.print("[cluster] Pre-test cooldown: waiting {d}ms for TCP cleanup...\n", .{config.pre_test_cooldown_ms});
            stdx.time.sleep(config.pre_test_cooldown_ms * std.time.ns_per_ms);
        }

        var self = Self{
            .allocator = allocator,
            .config = config,
            .servers = [_]?*ServerProcess{null} ** MAX_NODES,
            .clis = [_]?*CliRunner{null} ** MAX_NODES,
            .endpoints = [_]?[]const u8{null} ** MAX_NODES,
            .node_count = config.node_count,
        };

        // Start node 1 (seed node - no --join)
        // Note: Don't set explicit node_id - let it be auto-generated from host:port
        // This ensures consistency with how joining nodes compute peer_node_id
        // Set cluster_enabled to ensure raft_port is allocated for cluster networking
        self.servers[0] = try ServerProcess.initWithConfig(allocator, .{
            .durability = config.durability,
            .shards = config.shards,
            .tiered_log = config.tiered_log,
            .log_level = config.log_level,
            .cluster_enabled = true, // Seed node needs Raft listener
        });
        // Covers every node created so far. A per-iteration `errdefer` inside
        // the join loop below only fires for its own iteration, so when the
        // third node failed the first two leaked — 75 allocations across the
        // suite, all traced to initWithConfig (issue #54).
        errdefer self.deinitServers();
        startServerWithRetry(self.servers[0].?) catch |err| {
            std.debug.print("[cluster] Seed node failed to start: {}\n", .{err});
            return err;
        };

        // Get seed node's raft endpoint
        const seed_endpoint = try self.servers[0].?.getRaftEndpoint(allocator);
        defer allocator.free(seed_endpoint);

        // Start remaining nodes (join seed)
        // Note: Don't set explicit node_id - let it be auto-generated from host:port
        for (1..config.node_count) |i| {
            self.servers[i] = try ServerProcess.initWithConfig(allocator, .{
                .durability = config.durability,
                .shards = config.shards,
                .tiered_log = config.tiered_log,
                .log_level = config.log_level,
                .join_addresses = seed_endpoint,
            });
            startServerWithRetry(self.servers[i].?) catch |err| {
                std.debug.print("[cluster] Node {d} of {d} failed to start: {}\n", .{ i, config.node_count, err });
                return err;
            };
        }

        // Create CLI runners for each node
        for (0..config.node_count) |i| {
            self.endpoints[i] = try self.servers[i].?.getEndpoint(allocator);
            errdefer if (self.endpoints[i]) |e| allocator.free(e);
            self.clis[i] = try CliRunner.init(allocator, self.servers[i].?.flo_binary, self.endpoints[i].?);
            errdefer if (self.clis[i]) |c| c.deinit();
        }

        // Give cluster time to form (Raft election + replication setup)
        stdx.time.sleep(config.formation_delay_ns);

        return self;
    }

    /// Initialize with default 3-node configuration
    pub fn initDefault(allocator: Allocator) !Self {
        return init(allocator, .{});
    }

    /// Tear down any servers created so far. Safe to call with unset slots,
    /// so it doubles as the error-path cleanup for a partially built cluster.
    fn deinitServers(self: *Self) void {
        for (0..self.node_count) |i| {
            if (self.servers[i]) |server| {
                server.stop();
                server.deinit();
                self.servers[i] = null;
            }
        }
    }

    /// Clean up all resources
    pub fn deinit(self: *Self) void {
        for (0..self.node_count) |i| {
            if (self.clis[i]) |cli| {
                cli.deinit();
                self.clis[i] = null;
            }
            if (self.endpoints[i]) |endpoint| {
                self.allocator.free(endpoint);
                self.endpoints[i] = null;
            }
            if (self.servers[i]) |server| {
                server.stop();
                server.deinit();
                self.servers[i] = null;
            }
        }
    }

    /// Execute command on specific node (fire-and-forget, asserts success)
    pub fn execOn(self: *Self, node: usize, args: []const []const u8) !void {
        if (node >= self.node_count) return error.InvalidNodeIndex;
        const cli = self.clis[node] orelse return error.NodeNotRunning;

        var result = try cli.run(args);
        defer result.deinit();
        if (!result.succeeded()) {
            std.debug.print("Node {d} command failed:\n  stdout: {s}\n  stderr: {s}\n", .{
                node,
                result.stdout,
                result.stderr,
            });
            return error.CommandFailed;
        }
    }

    /// Execute command and capture output on specific node
    /// Caller owns returned memory
    pub fn execCaptureOn(self: *Self, node: usize, args: []const []const u8) ![]const u8 {
        if (node >= self.node_count) return error.InvalidNodeIndex;
        const cli = self.clis[node] orelse return error.NodeNotRunning;

        var result = try cli.run(args);
        defer result.deinit();
        if (!result.succeeded()) {
            std.debug.print("Node {d} command failed:\n  stdout: {s}\n  stderr: {s}\n", .{
                node,
                result.stdout,
                result.stderr,
            });
            return error.CommandFailed;
        }
        return try self.allocator.dupe(u8, result.stdout);
    }

    /// Execute command and capture output regardless of exit code
    /// Useful for checking "not found" results
    /// Caller owns returned memory
    pub fn execCaptureAnyOn(self: *Self, node: usize, args: []const []const u8) ![]const u8 {
        if (node >= self.node_count) return error.InvalidNodeIndex;
        const cli = self.clis[node] orelse return error.NodeNotRunning;

        var result = try cli.run(args);
        defer result.deinit();
        // Return stdout regardless of exit code
        return try self.allocator.dupe(u8, result.stdout);
    }

    /// Execute command on a node with retries.
    /// Retries up to `max_retries` times with `delay_ms` between attempts.
    /// Useful after leader failover or when waiting for Raft replication.
    pub fn execOnWithRetry(self: *Self, node: usize, args: []const []const u8, max_retries: u8, delay_ms: u64) !void {
        var attempts: u8 = 0;
        while (true) {
            self.execOn(node, args) catch |err| {
                attempts += 1;
                if (attempts > max_retries) return err;
                stdx.time.sleep(delay_ms * std.time.ns_per_ms);
                continue;
            };
            return;
        }
    }

    /// Execute command and capture output with retries.
    /// Retries up to `max_retries` times with `delay_ms` between attempts.
    /// Caller owns returned memory.
    pub fn execCaptureOnWithRetry(self: *Self, node: usize, args: []const []const u8, max_retries: u8, delay_ms: u64) ![]const u8 {
        var attempts: u8 = 0;
        while (true) {
            return self.execCaptureOn(node, args) catch |err| {
                attempts += 1;
                if (attempts > max_retries) return err;
                stdx.time.sleep(delay_ms * std.time.ns_per_ms);
                continue;
            };
        }
    }

    /// Poll a command until its output contains `expected`.
    /// Retries up to `max_retries` times with `delay_ms` between attempts.
    /// Useful when replication may return stale (but successful) data.
    /// Caller owns returned memory.
    pub fn pollUntilContains(self: *Self, node: usize, args: []const []const u8, expected: []const u8, max_retries: u8, delay_ms: u64) ![]const u8 {
        var attempts: u8 = 0;
        while (true) {
            const output = self.execCaptureOn(node, args) catch |err| {
                attempts += 1;
                if (attempts > max_retries) return err;
                stdx.time.sleep(delay_ms * std.time.ns_per_ms);
                continue;
            };
            if (std.mem.indexOf(u8, output, expected) != null) return output;
            self.allocator.free(output);
            attempts += 1;
            if (attempts > max_retries) return error.CommandFailed;
            stdx.time.sleep(delay_ms * std.time.ns_per_ms);
        }
    }

    /// Poll a command (ignoring exit code) until its output contains `expected`.
    /// Caller owns returned memory.
    pub fn pollAnyUntilContains(self: *Self, node: usize, args: []const []const u8, expected: []const u8, max_retries: u8, delay_ms: u64) ![]const u8 {
        var attempts: u8 = 0;
        while (true) {
            const output = self.execCaptureAnyOn(node, args) catch |err| {
                attempts += 1;
                if (attempts > max_retries) return err;
                stdx.time.sleep(delay_ms * std.time.ns_per_ms);
                continue;
            };
            if (std.mem.indexOf(u8, output, expected) != null) return output;
            self.allocator.free(output);
            attempts += 1;
            if (attempts > max_retries) return error.CommandFailed;
            stdx.time.sleep(delay_ms * std.time.ns_per_ms);
        }
    }

    /// Stop a specific node (for failure testing)
    pub fn stopNode(self: *Self, node: usize) void {
        if (node >= self.node_count) return;
        if (self.servers[node]) |server| {
            server.stop();
        }
    }

    /// Restart a previously stopped node
    pub fn restartNode(self: *Self, node: usize) !void {
        if (node >= self.node_count) return error.InvalidNodeIndex;
        const server = self.servers[node] orelse return error.NodeNotInitialized;
        try server.start();

        // Small delay for node to rejoin cluster
        stdx.time.sleep(500 * std.time.ns_per_ms);
    }

    /// Get the number of running nodes
    pub fn runningNodeCount(self: *const Self) u8 {
        var count: u8 = 0;
        for (0..self.node_count) |i| {
            if (self.servers[i]) |server| {
                if (server.started) count += 1;
            }
        }
        return count;
    }

    /// Get the endpoint for a specific node
    pub fn getNodeEndpoint(self: *const Self, node: usize) ?[]const u8 {
        if (node >= self.node_count) return null;
        return self.endpoints[node];
    }

    /// Get the server process for a specific node
    pub fn getServer(self: *const Self, node: usize) ?*ServerProcess {
        if (node >= self.node_count) return null;
        return self.servers[node];
    }

    /// Dump logs from all nodes (for debugging test failures)
    /// Returns the combined log output as an allocated string
    pub fn dumpLogs(self: *const Self, max_bytes_per_node: usize) ![]const u8 {
        var output: std.ArrayListUnmanaged(u8) = .empty;
        errdefer output.deinit(self.allocator);

        for (0..self.node_count) |i| {
            if (self.servers[i]) |s| {
                const log_file = stdx.fs.openFileAbsolute(s.log_file_path, .{}) catch |e| {
                    try output.appendSlice(self.allocator, "--- Node ");
                    try output.append(self.allocator, @as(u8, @intCast(i)) + '0');
                    try output.appendSlice(self.allocator, " log: Could not open: ");
                    try output.appendSlice(self.allocator, @errorName(e));
                    try output.appendSlice(self.allocator, " ---\n");
                    continue;
                };
                defer stdx.fs.closeFile(log_file);

                // Write header
                try output.appendSlice(self.allocator, "\n--- Node ");
                try output.append(self.allocator, @as(u8, @intCast(i)) + '0');
                try output.appendSlice(self.allocator, " log (");
                try output.appendSlice(self.allocator, s.log_file_path);
                try output.appendSlice(self.allocator, ") ---\n");

                // Read log content
                const buf = try self.allocator.alloc(u8, max_bytes_per_node);
                defer self.allocator.free(buf);
                const n = log_file.readAll(buf) catch 0;
                try output.appendSlice(self.allocator, buf[0..n]);
                try output.append(self.allocator, '\n');
            }
        }

        return output.toOwnedSlice(self.allocator);
    }

    /// Print logs from all nodes to stderr (for test debugging)
    pub fn printLogs(self: *const Self) void {
        const logs = self.dumpLogs(262144) catch |e| {
            std.debug.print("Failed to dump logs: {}\n", .{e});
            return;
        };
        defer self.allocator.free(logs);
        std.debug.print("{s}", .{logs});
    }

    /// Get cluster stats summary (for debugging)
    pub fn getClusterSummary(self: *const Self) void {
        std.debug.print("\n=== Cluster Summary ===\n", .{});
        std.debug.print("Node count: {d}, Running: {d}\n", .{ self.node_count, self.runningNodeCount() });
        for (0..self.node_count) |i| {
            if (self.servers[i]) |s| {
                const status = if (s.started) "RUNNING" else "STOPPED";
                std.debug.print("  Node {d}: {s} (flo:{d}, raft:{d})\n", .{
                    i,
                    status,
                    s.port,
                    s.raft_port,
                });
            }
        }
        std.debug.print("=======================\n", .{});
    }
};
