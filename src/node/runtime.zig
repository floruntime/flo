//! Node Runtime — boot sequence, thread management, graceful shutdown
//!
//! The Runtime is the top-level lifecycle manager for a Flo node.
//!
//! ## Boot Sequence
//!
//! 1. Detect CPU count (or use configured shard count)
//! 2. Create per-shard pipes (acceptor → shard hand-off)
//! 3. Spawn N Shard threads
//! 4. Wait for all shards to be ready
//! 5. Spawn Acceptor thread → bind + listen
//! 6. Enter steady state
//!
//! ## Shutdown (reverse order)
//!
//! 1. Stop Acceptor (close listen socket)
//! 2. Send shutdown to each shard's inbox
//! 3. Join all shard threads
//! 4. Clean up pipes and resources

const std = @import("std");
const log = @import("stdx").log;
const Shard = @import("shard.zig").Shard;
const Acceptor = @import("acceptor.zig").Acceptor;
const Inbox = @import("inbox.zig").Inbox;
const InboxMessage = @import("inbox.zig").Message;
const proto = @import("../protocol/proto.zig");
const Durability = @import("../config/server.zig").Durability;
const ColdStorageConfig = @import("../config/cold_storage.zig").ColdStorageConfig;
const TieredLogConfig = @import("../config/tiered_log.zig").TieredLogConfig;
const RaftNetwork = @import("../raft/network.zig").RaftNetwork;
const generateNodeId = @import("../raft/network.zig").generateNodeId;
const manifest = @import("manifest.zig");
const DashboardServer = @import("dashboard/mod.zig").DashboardServer;
const DashboardServerConfig = @import("dashboard/mod.zig").DashboardServerConfig;
const DashboardContext = @import("dashboard/api.zig").DashboardContext;
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;

/// Runtime configuration produced by ServerConfig.toRuntimeConfig()
///
/// ## Port Derivation
///
/// Auxiliary ports (metrics, dashboard, raft, gossip) default to 0, meaning
/// "derive from listen_port + offset". Use the `effective*Port()` methods
/// instead of accessing the port fields directly. This allows running multiple
/// instances with a single `--port` flag:
///
///   --port 10000  →  metrics=10001, dashboard=10002, raft=10500, gossip=10600
///
pub const RuntimeConfig = struct {
    // =========================================================================
    // Port Offset Constants
    // =========================================================================
    // All auxiliary ports are derived from the base listen_port when set to 0.
    // This allows running multiple instances with a single --port flag.
    pub const PORT_OFFSET_METRICS: u16 = 1; // listen_port + 1
    pub const PORT_OFFSET_DASHBOARD: u16 = 2; // listen_port + 2
    pub const PORT_OFFSET_RAFT: u16 = 500; // listen_port + 500
    pub const PORT_OFFSET_GOSSIP: u16 = 600; // listen_port + 600

    num_shards: u16 = 0,
    partition_count: u32 = 0,
    data_dir: []const u8 = "~/.flo/data",

    /// TCP port for client connections (default: 9000)
    listen_port: u16 = 9000,
    listen_addr: []const u8 = "0.0.0.0",

    durability: Durability = .async_flush,
    cold_storage: ?ColdStorageConfig = null,
    tiered_log: TieredLogConfig = .{},
    auth_enabled: bool = false,
    jwt_secret: ?[]const u8 = null,
    jwks_url: ?[]const u8 = null,
    ws_rate_limit_requests: u32 = 100,
    ws_rate_limit_window_ms: i64 = 60000,
    ws_ping_interval_ms: i64 = 30000,
    ws_pong_timeout_ms: i64 = 10000,

    metrics_enabled: bool = true,
    /// Port for HTTP metrics server (0 = derive from listen_port + 1)
    metrics_port: u16 = 0,

    dashboard_enabled: bool = true,
    /// Port for dashboard HTTP server (0 = derive from listen_port + 2)
    dashboard_port: u16 = 0,
    dashboard_bind: []const u8 = "0.0.0.0",
    dashboard_cors_origins: ?[]const u8 = null,

    cluster_node_id: u32 = 0,
    /// Port for Raft RPC communication (0 = derive from listen_port + 500)
    cluster_raft_port: u16 = 0,
    /// Port for gossip UDP communication (0 = derive from listen_port + 600)
    cluster_gossip_port: u16 = 0,
    cluster_seeds: []const []const u8 = &.{},
    cluster_replication_factor: u16 = 1,
    cluster_election_timeout_min_ms: u32 = 150,
    cluster_election_timeout_max_ms: u32 = 300,
    cluster_heartbeat_interval_ms: u32 = 50,
    cluster_gossip_ping_interval_ms: u32 = 1000,
    cluster_gossip_ping_timeout_ms: u32 = 500,
    cluster_gossip_suspect_timeout_ms: u32 = 5000,
    namespace_deletion_interval_ms: i64 = 5000,
    expose_internal_keys: bool = false,

    // =========================================================================
    // Port Derivation Methods
    // =========================================================================
    // Use these instead of accessing port fields directly.

    /// Get effective metrics port (derives from listen_port + 1 if set to 0)
    pub fn effectiveMetricsPort(self: RuntimeConfig) u16 {
        if (self.metrics_port > 0) return self.metrics_port;
        return self.listen_port +| PORT_OFFSET_METRICS;
    }

    /// Get effective dashboard port (derives from listen_port + 2 if set to 0)
    pub fn effectiveDashboardPort(self: RuntimeConfig) u16 {
        if (self.dashboard_port > 0) return self.dashboard_port;
        return self.listen_port +| PORT_OFFSET_DASHBOARD;
    }

    /// Get effective Raft RPC port (derives from listen_port + 500 if set to 0)
    pub fn effectiveRaftPort(self: RuntimeConfig) u16 {
        if (self.cluster_raft_port > 0) return self.cluster_raft_port;
        return self.listen_port +| PORT_OFFSET_RAFT;
    }

    /// Get effective gossip port (derives from listen_port + 600 if set to 0)
    pub fn effectiveGossipPort(self: RuntimeConfig) u16 {
        if (self.cluster_gossip_port > 0) return self.cluster_gossip_port;
        return self.listen_port +| PORT_OFFSET_GOSSIP;
    }
};

/// Node runtime — manages shard threads, acceptor thread, and lifecycle.
pub const Runtime = struct {
    allocator: std.mem.Allocator,
    config: RuntimeConfig,

    /// Effective shard count (auto-detected or configured).
    shard_count: u16,

    /// Per-shard state. Each shard runs on its own thread.
    shards: ?[]Shard,

    /// Per-shard pipes: [i][0] = read (shard), [i][1] = write (acceptor).
    pipes: ?[][2]i32,

    /// Shard threads.
    shard_threads: ?[]std.Thread,

    /// Acceptor.
    acceptor: ?Acceptor,

    /// Acceptor thread.
    acceptor_thread: ?std.Thread,

    /// Write-ends of shard pipes (slice into pipes for acceptor).
    pipe_write_ends: ?[]i32,

    /// Whether the runtime has been started.
    started: bool,

    /// Raft networking layer for cluster entry replication.
    raft_network: ?*RaftNetwork,

    /// Dashboard HTTP server (serves REST API + static files).
    dashboard_server: ?*DashboardServer,

    /// Dashboard context (metrics + shard info for API handlers).
    dashboard_ctx: ?*DashboardContext,

    /// Metrics registry for dashboard and Prometheus endpoint.
    metrics_registry: ?*MetricsRegistry,

    /// Walk context arrays — allocated during wireWalkContexts, freed on deinit.
    /// Each entry is a per-shard array of projection pointers for one walk opcode.
    /// Slots: [0] = ts_list. More slots added as modules gain list support.
    walk_ctx_slices: [4]?[]*anyopaque,

    pub fn init(allocator: std.mem.Allocator, config: RuntimeConfig) !Runtime {
        const shard_count = detectShardCount(config.num_shards);

        return .{
            .allocator = allocator,
            .config = config,
            .shard_count = shard_count,
            .shards = null,
            .pipes = null,
            .shard_threads = null,
            .acceptor = null,
            .acceptor_thread = null,
            .pipe_write_ends = null,
            .started = false,
            .raft_network = null,
            .dashboard_server = null,
            .dashboard_ctx = null,
            .metrics_registry = null,
            .walk_ctx_slices = .{ null, null, null, null },
        };
    }

    pub fn deinit(self: *Runtime) void {
        if (self.started) {
            self.stop();
        }

        // Clean up dashboard
        if (self.dashboard_server) |server| {
            server.deinit();
            self.allocator.destroy(server);
            self.dashboard_server = null;
        }
        if (self.dashboard_ctx) |ctx| {
            if (ctx.shard_ptrs) |ptrs| {
                self.allocator.free(ptrs);
            }
            self.allocator.destroy(ctx);
            self.dashboard_ctx = null;
        }
        if (self.metrics_registry) |metrics| {
            metrics.deinit();
            self.allocator.destroy(metrics);
            self.metrics_registry = null;
        }

        // Clean up raft network
        if (self.raft_network) |rn| {
            rn.deinit();
            self.allocator.destroy(rn);
            self.raft_network = null;
        }

        // Clean up pipes
        if (self.pipes) |pipes| {
            for (pipes) |pipe| {
                if (pipe[0] >= 0) std.posix.close(pipe[0]);
                if (pipe[1] >= 0) std.posix.close(pipe[1]);
            }
            self.allocator.free(pipes);
        }

        // Clean up walk context arrays (before shards — dispatchers reference these)
        for (&self.walk_ctx_slices) |*slot| {
            if (slot.*) |s| {
                self.allocator.free(s);
                slot.* = null;
            }
        }

        // Clean up shards
        if (self.shards) |shards| {
            for (shards) |*shard| {
                shard.deinit();
            }
            self.allocator.free(shards);
        }

        if (self.shard_threads) |threads| {
            self.allocator.free(threads);
        }

        if (self.pipe_write_ends) |ends| {
            self.allocator.free(ends);
        }
    }

    /// Start the runtime: create shards, spawn threads, start acceptor.
    pub fn start(self: *Runtime) !void {
        log.debug("Runtime.start: shard_count={d} listen_port={d} data_dir={s}", .{
            self.shard_count,
            self.config.listen_port,
            self.config.data_dir,
        });

        // 0. Topology safety — validate shard/partition count against SYSTEM manifest
        try manifest.ensureTopology(
            self.allocator,
            self.config.data_dir,
            self.shard_count,
            self.config.partition_count,
        );

        // 1. Create per-shard pipes
        const pipes = try self.allocator.alloc([2]i32, self.shard_count);
        errdefer self.allocator.free(pipes);
        var pipes_created: usize = 0;
        errdefer {
            for (0..pipes_created) |i| {
                std.posix.close(pipes[i][0]);
                std.posix.close(pipes[i][1]);
            }
        }

        for (0..self.shard_count) |i| {
            // Non-blocking read end so shard's acceptFromPipe drain loop
            // returns WouldBlock instead of blocking when pipe is empty.
            const fds = try std.posix.pipe2(.{ .NONBLOCK = true });
            pipes[i] = .{ fds[0], fds[1] };
            pipes_created += 1;
        }
        self.pipes = pipes;

        // Build write-end array for acceptor
        const write_ends = try self.allocator.alloc(i32, self.shard_count);
        for (0..self.shard_count) |i| {
            write_ends[i] = pipes[i][1];
        }
        self.pipe_write_ends = write_ends;

        log.debug("Runtime.start: created {d} shard pipes", .{self.shard_count});

        // 2. Create shards
        const shards = try self.allocator.alloc(Shard, self.shard_count);
        var shards_created: usize = 0;
        errdefer {
            for (0..shards_created) |i| {
                shards[i].deinit();
            }
            self.allocator.free(shards);
        }

        for (0..self.shard_count) |i| {
            shards[i] = try Shard.init(
                self.allocator,
                @intCast(i),
                self.shard_count,
                self.config.partition_count,
                pipes[i][0],
                self.config.data_dir,
                self.config.tiered_log.hot_buffer_capacity,
                self.config.tiered_log.max_hot_entries,
                self.config.tiered_log.hot_flush_seconds,
            );
            shards_created += 1;
        }
        self.shards = shards;

        // 2.5 Wire cross-shard walk contexts for list/scan opcodes.
        // Each walk opcode gets a slice of per-shard projection pointers.
        try self.wireWalkContexts(shards);

        // 2.6 Register cooperative background tasks (hot_flush, etc.).
        // Must happen after shards are at final heap addresses.
        for (0..self.shard_count) |i| {
            shards[i].registerBackgroundTasks();
        }

        log.debug("Runtime.start: {d} shards initialized", .{shards_created});

        // 3. Spawn shard threads
        const threads = try self.allocator.alloc(std.Thread, self.shard_count);
        self.shard_threads = threads;

        for (0..self.shard_count) |i| {
            threads[i] = try std.Thread.spawn(.{}, shardThread, .{&shards[i]});
            log.debug("Runtime.start: spawned shard thread {d}", .{i});
        }

        // 3.5 Create raft network if cluster is enabled
        if (self.config.effectiveRaftPort() != self.config.listen_port or self.config.cluster_seeds.len > 0) {
            const raft_port = self.config.effectiveRaftPort();
            const node_id = if (self.config.cluster_node_id > 0)
                self.config.cluster_node_id
            else
                generateNodeId(self.config.listen_port);

            const rn = try self.allocator.create(RaftNetwork);
            rn.* = try RaftNetwork.init(self.allocator, node_id, raft_port, self.config.listen_port);
            rn.setShardInbox(&shards[0].inbox);
            shards[0].raft_network = rn;
            self.raft_network = rn;

            try rn.start();

            // Connect to seed nodes
            for (self.config.cluster_seeds) |seed| {
                const seed_port = parseSeedPort(seed) orelse continue;
                // Retry connection a few times since seed may still be starting
                var attempt: usize = 0;
                while (attempt < 30) : (attempt += 1) {
                    rn.connectToPeer("127.0.0.1", seed_port) catch {
                        std.Thread.sleep(200 * std.time.ns_per_ms);
                        continue;
                    };
                    break;
                }
            }
        }

        // 4. Create and start acceptor
        var acceptor = Acceptor.init(write_ends, shards[0].router);
        try acceptor.listen(self.config.listen_port);
        self.acceptor = acceptor;
        log.debug("Runtime.start: acceptor listening on port {d}", .{self.config.listen_port});

        // 5. Spawn acceptor thread
        self.acceptor_thread = try std.Thread.spawn(.{}, acceptorThread, .{&self.acceptor.?});
        log.debug("Runtime.start: acceptor thread spawned", .{});

        // 6. Start dashboard HTTP server if enabled
        if (self.config.dashboard_enabled) {
            const metrics = try self.allocator.create(MetricsRegistry);
            metrics.* = MetricsRegistry.init(self.allocator);
            self.metrics_registry = metrics;

            const ctx = try self.allocator.create(DashboardContext);
            ctx.* = DashboardContext.init(self.allocator, metrics, self.shard_count);

            // Wire shard references for read-only projection access
            if (self.shards) |s| {
                const ptrs = try self.allocator.alloc(*anyopaque, s.len);
                for (s, 0..) |*shard, i| {
                    ptrs[i] = @ptrCast(shard);
                }
                ctx.shard_ptrs = ptrs;
            }

            self.dashboard_ctx = ctx;

            const server = try self.allocator.create(DashboardServer);
            server.* = DashboardServer.init(self.allocator, .{
                .port = self.config.effectiveDashboardPort(),
                .bind = self.config.dashboard_bind,
                .cors_origins = self.config.dashboard_cors_origins orelse "*",
            }, ctx);
            self.dashboard_server = server;
            try server.start();
        }

        self.started = true;
        log.debug("Runtime.start: all components started, node is ready", .{});
    }

    /// Wire cross-shard walk contexts for all walk-registered opcodes.
    ///
    /// For each walk opcode (e.g., ts_list, stream_list, queue_list), builds
    /// a per-shard array of projection pointers and sets it on every shard's
    /// dispatcher. This enables `Shard.executeWalk()` to iterate all shards'
    /// projections for list/scan operations.
    fn wireWalkContexts(self: *Runtime, shards: []Shard) !void {
        const n = self.shard_count;

        // ts_list → each shard's TSProjection
        const ts_ctxs = try self.allocator.alloc(*anyopaque, n);
        for (0..n) |i| {
            ts_ctxs[i] = @ptrCast(&shards[i].defaultPartition().ts);
        }
        for (0..n) |i| {
            shards[i].dispatcher.setWalkContexts(proto.OpCode.ts_list, ts_ctxs);
        }
        self.walk_ctx_slices[0] = ts_ctxs;

        // kv_scan (full scan) → each shard's KVProjection
        const kv_ctxs = try self.allocator.alloc(*anyopaque, n);
        for (0..n) |i| {
            kv_ctxs[i] = @ptrCast(&shards[i].defaultPartition().kv);
        }
        for (0..n) |i| {
            shards[i].dispatcher.setWalkContexts(proto.OpCode.kv_scan, kv_ctxs);
        }
        self.walk_ctx_slices[1] = kv_ctxs;

        // stream_list → each shard's StreamProjection
        const stream_ctxs = try self.allocator.alloc(*anyopaque, n);
        for (0..n) |i| {
            stream_ctxs[i] = @ptrCast(&shards[i].defaultPartition().stream);
        }
        for (0..n) |i| {
            shards[i].dispatcher.setWalkContexts(proto.OpCode.stream_list, stream_ctxs);
        }
        self.walk_ctx_slices[2] = stream_ctxs;

        // Future: queue_list, action_list, workflow_list_definitions
        // will be wired here when those projections support named-resource listing.
    }

    /// Graceful shutdown in reverse order.
    pub fn stop(self: *Runtime) void {
        if (!self.started) return;
        log.debug("Runtime.stop: initiating graceful shutdown", .{});

        // 0. Stop dashboard HTTP server first
        if (self.dashboard_server) |server| {
            server.stop();
        }

        // 0.5 Stop raft network
        if (self.raft_network) |rn| {
            rn.stop();
        }

        // 1. Stop acceptor
        if (self.acceptor) |*acc| {
            acc.stop();
        }

        // 2. Join acceptor thread
        if (self.acceptor_thread) |t| {
            t.join();
            self.acceptor_thread = null;
        }

        // Close acceptor listen socket
        if (self.acceptor) |*acc| {
            acc.close();
            self.acceptor = null;
        }

        // 3. Send shutdown to each shard
        if (self.shards) |shards| {
            for (shards) |*shard| {
                _ = shard.inbox.send(.{
                    .tag = .shutdown,
                    .src_shard = 0xFF, // from runtime
                    .partition_id = 0,
                    .payload_len = 0,
                    .sequence = 0,
                    .payload_ptr = null,
                    ._padding = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
                });
                shard.shutdown();
            }
        }

        // 4. Join shard threads
        if (self.shard_threads) |threads| {
            for (threads) |t| {
                t.join();
            }
        }

        self.started = false;
        log.debug("Runtime.stop: shutdown complete", .{});
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Thread entry points
// ═══════════════════════════════════════════════════════════════════════════════

fn shardThread(shard: *Shard) void {
    shard.run() catch {};
}

fn acceptorThread(acc: *Acceptor) void {
    acc.running.store(true, .release);
    while (acc.running.load(.acquire)) {
        _ = acc.acceptOne() catch {};
        // Small yield to avoid busy-spin when no connections pending
        std.Thread.sleep(100_000); // 100μs
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Detect number of shards. If configured > 0, use that; else auto-detect CPUs.
///
/// On 4+ cores, reserves 1 core for the OS kernel (ksoftirqd, kworker, etc.).
/// The acceptor, dashboard, and metrics threads are I/O-bound and lightweight
/// enough to share cores with shard reactor threads.
fn detectShardCount(configured: u16) u16 {
    if (configured > 0) return configured;
    const cpus = std.Thread.getCpuCount() catch 1;
    const shards = if (cpus >= 4) cpus - 1 else cpus;
    return @intCast(@max(1, @min(shards, 256)));
}

/// Parse port from a seed address like "127.0.0.1:9500" or "localhost:9500".
fn parseSeedPort(seed: []const u8) ?u16 {
    const colon_idx = std.mem.lastIndexOfScalar(u8, seed, ':') orelse return null;
    if (colon_idx + 1 >= seed.len) return null;
    return std.fmt.parseInt(u16, seed[colon_idx + 1 ..], 10) catch null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Runtime: detect shard count" {
    // Explicit config
    try std.testing.expectEqual(@as(u16, 4), detectShardCount(4));
    try std.testing.expectEqual(@as(u16, 1), detectShardCount(1));

    // Auto-detect (just verify it returns something reasonable)
    const auto = detectShardCount(0);
    try std.testing.expect(auto >= 1);
    try std.testing.expect(auto <= 256);
}

test "Runtime: init and deinit" {
    var runtime = try Runtime.init(std.testing.allocator, .{
        .num_shards = 2,
        .listen_port = 0,
    });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u16, 2), runtime.shard_count);
    try std.testing.expect(!runtime.started);
}

test "Runtime: boot 2 shards and shutdown" {
    var runtime = try Runtime.init(std.testing.allocator, .{
        .num_shards = 2,
        .listen_port = 0, // ephemeral port
    });
    defer runtime.deinit();

    try runtime.start();
    try std.testing.expect(runtime.started);
    try std.testing.expect(runtime.shards != null);
    try std.testing.expectEqual(@as(usize, 2), runtime.shards.?.len);

    // Let it run briefly
    std.Thread.sleep(10_000_000); // 10ms

    runtime.stop();
    try std.testing.expect(!runtime.started);
}
