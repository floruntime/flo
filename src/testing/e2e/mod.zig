//! E2E Test Framework
//!
//! Provides infrastructure for end-to-end testing of Flo, including:
//! - Server process lifecycle management
//! - CLI command execution and HTTP client
//! - Test context with automatic setup/teardown
//! - Assertion helpers
//!
//! ## Quick Start
//! ```zig
//! const stdx = @import("stdx");
//! const testing = stdx.testing;
//!
//! test "e2e: my test" {
//!     var ctx = try testing.TestContext.init(std.testing.allocator);
//!     defer ctx.deinit();
//!
//!     // Fire-and-forget (asserts success, auto-cleanup)
//!     try ctx.exec(&.{ "kv", "set", "key", "value" });
//!
//!     // Capture output (memory managed by ctx arena)
//!     const value = try ctx.execCapture(&.{ "kv", "get", "key" });
//!     try std.testing.expectEqualStrings("value", value);
//!
//!     // Full control when needed
//!     var result = try ctx.cli.run(&.{ "kv", "get", "key", "--format", "json" });
//!     defer result.deinit();
//! }
//! ```

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

// Re-export framework components
pub const ServerProcess = @import("server.zig").ServerProcess;
pub const ServerConfig = ServerProcess.ServerConfig;
pub const CliRunner = @import("client.zig").CliRunner;
pub const CommandResult = @import("client.zig").CommandResult;
pub const HttpRunner = @import("http.zig").HttpRunner;
pub const HttpResponse = @import("http.zig").HttpResponse;

// Cluster testing
pub const cluster = @import("cluster.zig");
pub const ClusterContext = cluster.ClusterContext;
pub const ClusterConfig = cluster.ClusterConfig;

// Re-export assertions
pub const assertions = @import("assertions.zig");
pub const assertSucceeded = assertions.assertSucceeded;
pub const assertFailed = assertions.assertFailed;

// Re-export yaml_builder for workflow testing
pub const yaml_builder = @import("yaml_builder.zig");
pub const YamlBuilder = yaml_builder.YamlBuilder;
pub const PlanSectionBuilder = yaml_builder.PlanSectionBuilder;
pub const PlanBuilder = yaml_builder.PlanBuilder;
pub const StepBuilder = yaml_builder.StepBuilder;
pub const writeTempYaml = yaml_builder.writeTempYaml;
pub const writeDottedToTempYaml = yaml_builder.writeDottedToTempYaml;
pub const dottedToYaml = yaml_builder.dottedToYaml;
pub const cleanupTempFile = yaml_builder.cleanupTempFile;
pub const assertContains = assertions.assertContains;
pub const assertStdoutContains = assertions.assertStdoutContains;
pub const assertStderrContains = assertions.assertStderrContains;
pub const assertNotContains = assertions.assertNotContains;
pub const assertNotNull = assertions.assertNotNull;
pub const assertNull = assertions.assertNull;
pub const assertStringsEqual = assertions.assertStringsEqual;

/// Flo E2E Test Context
///
/// Provides a complete test environment with:
/// - Running Flo server (auto-started, auto-stopped)
/// - CLI runner configured to connect to the server
/// - Isolated data directory for each test
/// - Arena allocator for automatic memory management
///
/// ## CLI Tests
/// ```zig
/// var ctx = try TestContext.init(testing.allocator);
/// defer ctx.deinit();
///
/// try ctx.exec(&.{ "kv", "set", "key", "value" });
/// const value = try ctx.execCapture(&.{ "kv", "get", "key" });
/// ```
///
/// ## HTTP Tests (dashboard/metrics)
/// ```zig
/// var ctx = try TestContext.initWithConfig(allocator, .{
///     .server = .{ .dashboard_enabled = true },
/// });
/// defer ctx.deinit();
///
/// var http = try ctx.createHttp(ctx.getDashboardPort());
/// defer http.deinit();
///
/// var resp = try http.get("/health");
/// defer resp.deinit();
/// ```
pub const TestContext = struct {
    const Self = @This();

    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    server: *ServerProcess,
    cli: *CliRunner,
    endpoint: []const u8,

    /// Initialize test context with defaults (no dashboard/metrics)
    pub fn init(allocator: Allocator) !*Self {
        return initWithConfig(allocator, .{});
    }

    /// Configuration options for test context
    pub const Config = struct {
        /// Server configuration
        server: ServerProcess.ServerConfig = .{},
        /// Custom server startup timeout (ms)
        server_timeout_ms: u64 = ServerProcess.DEFAULT_READY_TIMEOUT_MS,
    };

    /// Initialize with custom configuration
    pub fn initWithConfig(allocator: Allocator, config: Config) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        // Initialize arena for automatic memory management
        self.arena = std.heap.ArenaAllocator.init(allocator);
        errdefer self.arena.deinit();

        // Initialize and start server with retry — under heavy test load,
        // port allocation or server startup can transiently fail
        const max_startup_retries: u8 = 3;
        var attempt: u8 = 0;
        while (true) {
            self.server = try ServerProcess.initWithConfig(allocator, config.server);

            self.server.startWithTimeout(config.server_timeout_ms) catch |err| {
                self.server.deinit();
                attempt += 1;
                if (attempt >= max_startup_retries) return err;
                std.debug.print("[TestContext] Server start failed (attempt {d}/{d}), retrying...\n", .{ attempt, max_startup_retries });
                std.Thread.sleep(1000 * std.time.ns_per_ms);
                continue;
            };
            break;
        }
        errdefer {
            self.server.stop();
            self.server.deinit();
        }

        // Get endpoint
        self.endpoint = try self.server.getEndpoint(allocator);
        errdefer allocator.free(self.endpoint);

        // Initialize CLI runner
        self.cli = try CliRunner.init(allocator, self.server.flo_binary, self.endpoint);
        errdefer self.cli.deinit();

        self.allocator = allocator;

        return self;
    }

    /// Clean up all resources
    pub fn deinit(self: *Self) void {
        self.arena.deinit();
        self.cli.deinit();
        self.allocator.free(self.endpoint);
        self.server.stop();
        self.server.deinit();
        self.allocator.destroy(self);
    }

    // =========================================================================
    // HTTP Factory (create runners as needed)
    // =========================================================================

    /// Create an HTTP runner for a specific port
    /// Caller owns the returned HttpRunner and must call deinit()
    pub fn createHttp(self: *Self, port: u16) !*HttpRunner {
        return HttpRunner.init(self.allocator, "127.0.0.1", port);
    }

    /// Create an HTTP runner for the dashboard (convenience)
    /// Returns error if dashboard is not enabled
    pub fn createDashboardHttp(self: *Self) !*HttpRunner {
        if (!self.server.isDashboardEnabled()) {
            return error.DashboardNotEnabled;
        }
        return self.createHttp(self.server.getDashboardPort());
    }

    /// Create an HTTP runner for metrics endpoint (convenience)
    /// Returns error if metrics is not enabled
    pub fn createMetricsHttp(self: *Self) !*HttpRunner {
        if (!self.server.isMetricsEnabled()) {
            return error.MetricsNotEnabled;
        }
        return self.createHttp(self.server.getMetricsPort());
    }

    // =========================================================================
    // Convenience Methods (automatic memory management)
    // =========================================================================

    /// Execute a command and assert success (fire-and-forget)
    /// Memory is automatically managed - no need to call deinit
    pub fn exec(self: *Self, args: []const []const u8) !void {
        var result = try self.cli.run(args);
        defer result.deinit();

        if (!result.succeeded()) {
            std.debug.print("\n[EXEC FAILED] Command: flo", .{});
            for (args) |arg| std.debug.print(" {s}", .{arg});
            std.debug.print("\nStderr: {s}\n", .{result.stderr});
            return error.CommandFailed;
        }
    }

    /// Execute and capture stdout (trimmed)
    /// Memory is managed by ctx arena - freed when ctx.deinit() is called
    pub fn execCapture(self: *Self, args: []const []const u8) ![]const u8 {
        var result = try self.cli.run(args);
        defer result.deinit();

        if (!result.succeeded()) {
            std.debug.print("\n[EXEC FAILED] Command: flo", .{});
            for (args) |arg| std.debug.print(" {s}", .{arg});
            std.debug.print("\nStderr: {s}\n", .{result.stderr});
            return error.CommandFailed;
        }

        // Copy to arena so it persists after result.deinit()
        const trimmed = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);
        return try self.arena.allocator().dupe(u8, trimmed);
    }

    /// Execute and return full result (caller manages memory via defer result.deinit())
    /// Use this when you need access to stderr or exit code
    pub fn execResult(self: *Self, args: []const []const u8) !CommandResult {
        return self.cli.run(args);
    }

    // =========================================================================
    // Server/Context Info
    // =========================================================================

    /// Get the server port
    pub fn getPort(self: *const Self) u16 {
        return self.server.getPort();
    }

    /// Get the dashboard port (0 if not enabled)
    pub fn getDashboardPort(self: *const Self) u16 {
        return self.server.getDashboardPort();
    }

    /// Get the metrics port (0 if not enabled)
    pub fn getMetricsPort(self: *const Self) u16 {
        return self.server.getMetricsPort();
    }

    /// Get the data directory
    pub fn getDataDir(self: *const Self) []const u8 {
        return self.server.getDataDir();
    }

    // =========================================================================
    // Server Log Access (for debugging and verification)
    // =========================================================================

    /// Read server logs (returns owned slice - caller must free)
    pub fn readServerLogs(self: *Self) ![]const u8 {
        return self.server.readLogs();
    }

    /// Check if server logs contain a specific string
    pub fn serverLogsContain(self: *Self, needle: []const u8) !bool {
        return self.server.logsContain(needle);
    }

    /// Count occurrences of a string in server logs
    pub fn serverLogsCount(self: *Self, needle: []const u8) !usize {
        return self.server.logsCount(needle);
    }

    /// Get lines from server logs matching a pattern
    /// Returns owned slice of lines - caller must free each line and the slice
    pub fn grepServerLogs(self: *Self, pattern: []const u8) ![][]const u8 {
        return self.server.grepLogs(pattern);
    }

    /// Print server logs to stderr (useful for debugging failed tests)
    pub fn dumpServerLogs(self: *Self) void {
        self.server.dumpLogs();
    }

    /// Restart the server (useful for crash recovery tests)
    pub fn restartServer(self: *Self) !void {
        self.server.stop();
        try self.server.start();

        // Update CLI endpoint if port changed
        self.allocator.free(self.endpoint);
        self.endpoint = try self.server.getEndpoint(self.allocator);

        // Recreate CLI with new endpoint
        self.cli.deinit();
        self.cli = try CliRunner.init(self.allocator, self.server.flo_binary, self.endpoint);
    }
};

/// Create multiple test contexts for cluster testing
pub fn createCluster(allocator: Allocator, node_count: usize) ![]*TestContext {
    return createClusterWithConfig(allocator, node_count, .{});
}

/// Create multiple test contexts with custom config
pub fn createClusterWithConfig(allocator: Allocator, node_count: usize, config: TestContext.Config) ![]*TestContext {
    const contexts = try allocator.alloc(*TestContext, node_count);
    errdefer allocator.free(contexts);

    var initialized: usize = 0;
    errdefer {
        for (contexts[0..initialized]) |ctx| {
            ctx.deinit();
        }
    }

    for (contexts) |*ctx_ptr| {
        ctx_ptr.* = try TestContext.initWithConfig(allocator, config);
        initialized += 1;
    }

    return contexts;
}

/// Clean up cluster contexts
pub fn destroyCluster(allocator: Allocator, contexts: []*TestContext) void {
    for (contexts) |ctx| {
        ctx.deinit();
    }
    allocator.free(contexts);
}

// =============================================================================
// Tests
// =============================================================================

test "framework imports" {
    // Verify all imports work
    _ = ServerProcess;
    _ = CliRunner;
    _ = CommandResult;
    _ = HttpRunner;
    _ = HttpResponse;
    _ = TestContext;
    _ = assertions;
}
