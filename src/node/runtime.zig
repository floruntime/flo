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
const stdx = @import("stdx");
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
const StreamHandler = @import("../stream/handler.zig").StreamHandler;
const KVHandler = @import("../kv/handler.zig").KVHandler;
const TSHandler = @import("../ts/handler.zig").TSHandler;
const QueueHandler = @import("../queue/handler.zig").QueueHandler;
const ActionsHandler = @import("../actions/handler.zig").ActionsHandler;
const manifest = @import("manifest.zig");
const DashboardServer = @import("dashboard/mod.zig").DashboardServer;
const DashboardServerConfig = @import("dashboard/mod.zig").DashboardServerConfig;
const DashboardContext = @import("dashboard/api.zig").DashboardContext;
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;
const HttpMetricsServer = @import("../metrics/http_server.zig").HttpMetricsServer;

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
    /// Bind address for the metrics server. Was parsed from `[metrics] bind`
    /// and then dropped on the floor — the exporter always listened on
    /// 0.0.0.0 regardless of what the operator configured.
    metrics_bind: []const u8 = "127.0.0.1",

    dashboard_enabled: bool = true,
    /// Port for dashboard HTTP server (0 = derive from listen_port + 2)
    dashboard_port: u16 = 0,
    dashboard_bind: []const u8 = "0.0.0.0",
    dashboard_cors_origins: ?[]const u8 = null,

/// Start the peer-facing Raft listener even without seeds. See
    /// `ClusterConfig.enabled`.
    cluster_enabled: bool = false,
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

    /// Whether this node should bring up the peer-facing Raft listener.
    ///
    /// True when peers are actually possible: seeds are configured, a Raft port
    /// was named explicitly, replication is on, or the operator asked for it
    /// with `[cluster] enabled = true`. A plain single-node server matches none
    /// of these and leaves the port unbound.
    pub fn clusterListenerWanted(self: RuntimeConfig) bool {
        return self.cluster_enabled or
            self.cluster_seeds.len > 0 or
            self.cluster_raft_port > 0 or
            self.cluster_replication_factor > 1;
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

    /// Prometheus exporter (serves GET /metrics on the metrics port).
    metrics_server: ?*HttpMetricsServer,

    /// Walk context arrays — allocated during wireWalkContexts, freed on deinit.
    /// Each entry is a per-shard array of projection pointers for one walk opcode.
    /// Slots: [0] = ts_list. More slots added as modules gain list support.
    walk_ctx_slices: [8]?[]*anyopaque,

    /// Cross-shard stream handler array — allocated during wirePeerStreamHandlers, freed on deinit.
    peer_stream_handlers_slice: ?[]const *StreamHandler,

    /// Cross-shard inbox array — allocated during wirePeerInboxes, freed on deinit.
    peer_inboxes_slice: ?[]*Inbox,

    /// Cross-shard KV handler array — allocated during wirePeerKvHandlers, freed on deinit.
    peer_kv_handlers_slice: ?[]const *KVHandler,

    /// Cross-shard TS / queue handler arrays — allocated during wirePeer*, freed on deinit.
    peer_ts_handlers_slice: ?[]const *TSHandler,
    peer_queue_handlers_slice: ?[]const *QueueHandler,

    /// Cross-shard shard pointer array — allocated during wirePeerShards, freed on deinit.
    peer_shards_slice: ?[]*Shard,

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
            .metrics_server = null,
            .walk_ctx_slices = .{ null, null, null, null, null, null, null, null },
            .peer_stream_handlers_slice = null,
            .peer_kv_handlers_slice = null,
            .peer_ts_handlers_slice = null,
            .peer_queue_handlers_slice = null,
            .peer_inboxes_slice = null,
            .peer_shards_slice = null,
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
        // Stop the exporter before freeing the registry it reads from.
        if (self.metrics_server) |ms| {
            ms.deinit();
            self.allocator.destroy(ms);
            self.metrics_server = null;
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
                if (pipe[0] >= 0) _ = std.c.close(pipe[0]);
                if (pipe[1] >= 0) _ = std.c.close(pipe[1]);
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

        // Clean up peer stream handler array
        if (self.peer_stream_handlers_slice) |s| {
            self.allocator.free(s);
            self.peer_stream_handlers_slice = null;
        }

        // Clean up peer KV handler array
        if (self.peer_kv_handlers_slice) |s| {
            self.allocator.free(s);
            self.peer_kv_handlers_slice = null;
        }

        // Clean up peer TS / queue handler arrays
        if (self.peer_ts_handlers_slice) |s| {
            self.allocator.free(s);
            self.peer_ts_handlers_slice = null;
        }
        if (self.peer_queue_handlers_slice) |s| {
            self.allocator.free(s);
            self.peer_queue_handlers_slice = null;
        }

        // Clean up peer inbox array
        if (self.peer_inboxes_slice) |s| {
            self.allocator.free(s);
            self.peer_inboxes_slice = null;
        }

        // Clean up peer shards array
        if (self.peer_shards_slice) |s| {
            self.allocator.free(s);
            self.peer_shards_slice = null;
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
        // Expand a leading `~` to $HOME at the single point where the configured
        // data dir first becomes real directories. Without this, a default or
        // CLI-bypassing `~/.flo/data` would have `makePath` create a literal `~`
        // directory under the current working directory (both here via
        // ensureTopology and below via each Shard.init).
        const data_dir = try stdx.fs.expandTilde(self.allocator, self.config.data_dir);
        defer self.allocator.free(data_dir);

        log.debug("Runtime.start: shard_count={d} listen_port={d} data_dir={s}", .{
            self.shard_count,
            self.config.listen_port,
            data_dir,
        });

        // 0. Topology safety — validate shard/partition count against SYSTEM manifest
        try manifest.ensureTopology(
            self.allocator,
            data_dir,
            self.shard_count,
            self.config.partition_count,
        );

        // 1. Create per-shard pipes
        const pipes = try self.allocator.alloc([2]i32, self.shard_count);
        var pipes_created: usize = 0;
        errdefer {
            for (0..pipes_created) |i| {
                _ = std.c.close(pipes[i][0]);
                _ = std.c.close(pipes[i][1]);
            }
            self.allocator.free(pipes);
            self.pipes = null;
        }

        for (0..self.shard_count) |i| {
            // Non-blocking read end so shard's acceptFromPipe drain loop
            // returns WouldBlock instead of blocking when pipe is empty.
            var fds: [2]std.posix.fd_t = undefined;
            if (std.c.pipe(&fds) != 0) return error.SystemResources;
            // Mark both ends non-blocking via fcntl.
            const F = std.posix.F;
            const O = std.posix.O;
            const r_flags = std.c.fcntl(fds[0], F.GETFL, @as(c_int, 0));
            _ = std.c.fcntl(fds[0], F.SETFL, r_flags | @as(c_int, @bitCast(O{ .NONBLOCK = true })));
            pipes[i] = .{ fds[0], fds[1] };
            pipes_created += 1;
        }
        self.pipes = pipes;

        // Build write-end array for acceptor
        const write_ends = try self.allocator.alloc(i32, self.shard_count);
        errdefer {
            self.allocator.free(write_ends);
            self.pipe_write_ends = null;
        }
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
            self.shards = null;
        }

        for (0..self.shard_count) |i| {
            shards[i] = try Shard.init(
                self.allocator,
                @intCast(i),
                self.shard_count,
                self.config.partition_count,
                pipes[i][0],
                data_dir,
                self.config.tiered_log.hot_buffer_capacity,
                self.config.tiered_log.max_hot_entries,
                self.config.tiered_log.hot_flush_seconds,
                self.config.durability,
            );
            shards_created += 1;
        }
        self.shards = shards;

        // 2.5 Wire cross-shard walk contexts for list/scan opcodes.
        // Each walk opcode gets a slice of per-shard projection pointers.
        try self.wireWalkContexts(shards);

        // 2.55 Wire cross-shard stream handler references for processing pipelines.
        try self.wirePeerStreamHandlers(shards);

        // 2.555 Wire cross-shard KV handler references for processing KV lookups + sinks.
        try self.wirePeerKvHandlers(shards);

        // 2.556 Wire cross-shard TS / queue handler references for processing sinks.
        try self.wirePeerTsHandlers(shards);
        try self.wirePeerQueueHandlers(shards);

        // 2.56 Wire cross-shard inbox references for inbox messaging.
        try self.wirePeerInboxes(shards);

        // 2.57 Wire cross-shard shard pointers for pre-route forwarding.
        try self.wirePeerShards(shards);

        // 2.6 Register cooperative background tasks (hot_flush, etc.).
        // Must happen after shards are at final heap addresses.
        // Also wire shard back-pointers for handlers that need Raft access.
        for (0..self.shard_count) |i| {
            shards[i].registerBackgroundTasks();
            shards[i].wireHandlerShardPtrs();
        }

        log.debug("Runtime.start: {d} shards initialized", .{shards_created});

        // 3. Spawn shard threads
        const threads = try self.allocator.alloc(std.Thread, self.shard_count);
        self.shard_threads = threads;

        var threads_spawned: usize = 0;
        // If a later startup step fails (e.g. AddressInUse on the acceptor), we
        // must signal every already-running shard to stop and join it BEFORE the
        // outer shard errdefer calls shard.deinit().  deinit() closes the kqueue
        // poll_fd, and a thread still blocked in kevent() would get EBADF, which
        // hits the `unreachable` branch in std.posix and turns a clean "port busy"
        // error into an SIGABRT crash.  Zig errdefers fire in LIFO order, so this
        // one runs first (threads stop), then the shard errdefer runs (deinit ok).
        errdefer {
            for (0..threads_spawned) |i| shards[i].shutdown();
            for (0..threads_spawned) |i| threads[i].join();
            self.allocator.free(threads);
            self.shard_threads = null;
        }

        for (0..self.shard_count) |i| {
            threads[i] = try std.Thread.spawn(.{}, shardThread, .{&shards[i]});
            threads_spawned += 1;
            log.debug("Runtime.start: spawned shard thread {d}", .{i});
        }

        // This node's cluster identity exists whether or not the Raft listener
        // runs — `flo cluster status` reports it single-node too (#42 item 6).
        const cluster_node_id = if (self.config.cluster_node_id > 0)
            self.config.cluster_node_id
        else
            generateNodeId(self.config.listen_port);
        for (0..self.shard_count) |i| shards[i].cluster_node_id = cluster_node_id;

        // 3.5 Start the peer-facing Raft listener only when this node can
        // actually have peers. The old condition compared effectiveRaftPort()
        // against listen_port, but that helper *derives* listen_port + 500 when
        // no port is configured — so it was true for every single-node server,
        // which bound 9500 while the banner reported "Raft port: 0" (#42 item 5).
        if (self.config.clusterListenerWanted()) {
            const raft_port = self.config.effectiveRaftPort();
            const node_id = cluster_node_id;

            const rn = try self.allocator.create(RaftNetwork);
            // RaftNetwork.init binds a socket and can fail; without this the
            // allocation leaks on that path. Surfaced by CI on Linux, where the
            // bind really does fail for a `listen_port = 0` config.
            errdefer self.allocator.destroy(rn);
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
                        stdx.time.sleep(200 * std.time.ns_per_ms);
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

        // 6. Metrics registry — shared by the Prometheus exporter and the
        //    dashboard. Created when either consumer is enabled.
        if (self.config.metrics_enabled or self.config.dashboard_enabled) {
            const metrics = try self.allocator.create(MetricsRegistry);
            metrics.* = MetricsRegistry.init(self.allocator);
            self.metrics_registry = metrics;

            // Size the per-shard counter table. Without this `shardCount()`
            // stays 0, so /health reported "shards":0 on a multi-shard node and
            // the shard metric family was omitted entirely.
            metrics.initShards(self.shard_count) catch |err| {
                log.warn("metrics: initShards({d}) failed: {s}", .{ self.shard_count, @errorName(err) });
            };

            // Wire metrics registry into each shard's handlers
            if (self.shards) |s| {
                for (s) |*shard| {
                    shard.setMetricsRegistry(metrics);
                }
            }

            // Wire replication metrics into the raft network (issue #16) so the
            // leader-side oversize-skip / send-failure counters are recorded.
            if (self.raft_network) |rn| rn.setReplicationMetrics(&metrics.replication);
        }

        // 6a. Prometheus exporter if metrics is enabled
        if (self.config.metrics_enabled) {
            if (self.metrics_registry) |metrics| {
                const ms = try self.allocator.create(HttpMetricsServer);
                ms.* = HttpMetricsServer.init(self.allocator, self.config.effectiveMetricsPort(), self.config.metrics_bind, metrics);
                self.metrics_server = ms;
                ms.start() catch |err| {
                    log.err("metrics exporter failed to start on port {d}: {s}", .{ self.config.effectiveMetricsPort(), @errorName(err) });
                    self.allocator.destroy(ms);
                    self.metrics_server = null;
                };
                if (self.metrics_server != null) {
                    log.debug("Runtime.start: metrics exporter on port {d}", .{self.config.effectiveMetricsPort()});
                }
            }
        }

        // 6b. Start dashboard HTTP server if enabled
        if (self.config.dashboard_enabled) {
            const metrics = self.metrics_registry.?;

            const ctx = try self.allocator.create(DashboardContext);
            ctx.* = DashboardContext.init(self.allocator, metrics, self.shard_count);
            // Loopback target for dashboard-issued mutations (see DashboardContext.listen_port).
            ctx.listen_port = self.config.listen_port;

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

    /// Wire cross-shard stream handler references so processing pipelines
    /// can read/write streams on any shard.
    fn wirePeerStreamHandlers(self: *Runtime, shards: []Shard) !void {
        const n = self.shard_count;
        const handlers = try self.allocator.alloc(*StreamHandler, n);
        for (0..n) |i| {
            handlers[i] = shards[i].stream_handler;
        }
        self.peer_stream_handlers_slice = handlers;

        const partition_count = shards[0].router.partition_count;
        const shard_count = shards[0].router.shard_count;
        for (0..n) |i| {
            shards[i].processing_handler.setPeerStreamHandlers(handlers, partition_count, shard_count);
        }
    }

    /// Wire cross-shard KV handler references so processing pipelines'
    /// KV lookup operators and KV sinks can reach keys on any shard.
    fn wirePeerKvHandlers(self: *Runtime, shards: []Shard) !void {
        const n = self.shard_count;
        const handlers = try self.allocator.alloc(*KVHandler, n);
        for (0..n) |i| {
            handlers[i] = shards[i].kv_handler;
        }
        self.peer_kv_handlers_slice = handlers;

        for (0..n) |i| {
            shards[i].processing_handler.setPeerKvHandlers(handlers);
        }
    }

    /// Wire cross-shard TS handler references so TS sinks write to the shard
    /// that owns the target measurement.
    fn wirePeerTsHandlers(self: *Runtime, shards: []Shard) !void {
        const n = self.shard_count;
        const handlers = try self.allocator.alloc(*TSHandler, n);
        for (0..n) |i| {
            handlers[i] = shards[i].ts_handler;
        }
        self.peer_ts_handlers_slice = handlers;

        for (0..n) |i| {
            shards[i].processing_handler.setPeerTsHandlers(handlers);
        }
    }

    /// Wire cross-shard queue handler references so queue sinks enqueue to the
    /// shard that owns the target queue.
    fn wirePeerQueueHandlers(self: *Runtime, shards: []Shard) !void {
        const n = self.shard_count;
        const handlers = try self.allocator.alloc(*QueueHandler, n);
        for (0..n) |i| {
            handlers[i] = shards[i].queue_handler;
        }
        self.peer_queue_handlers_slice = handlers;

        for (0..n) |i| {
            shards[i].processing_handler.setPeerQueueHandlers(handlers);
        }
    }

    /// Wire cross-shard inbox references so shards can send messages to
    /// other shards' inboxes (e.g., action_invoke notifications).
    fn wirePeerInboxes(self: *Runtime, shards: []Shard) !void {
        const n = self.shard_count;
        const inboxes = try self.allocator.alloc(*Inbox, n);
        for (0..n) |i| {
            inboxes[i] = &shards[i].inbox;
        }
        self.peer_inboxes_slice = inboxes;
        for (0..n) |i| {
            shards[i].peer_inboxes = inboxes;
        }
    }

    /// Wire cross-shard shard pointers so dispatchRequest can forward
    /// pre-routed requests to the correct shard's handlers.
    fn wirePeerShards(self: *Runtime, shards: []Shard) !void {
        const n = self.shard_count;
        const ptrs = try self.allocator.alloc(*Shard, n);
        for (0..n) |i| {
            ptrs[i] = &shards[i];
        }
        self.peer_shards_slice = ptrs;
        for (0..n) |i| {
            shards[i].peer_shards = ptrs;
        }
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

        // namespace_list → each shard's NamespaceHandler
        const ns_ctxs = try self.allocator.alloc(*anyopaque, n);
        for (0..n) |i| {
            ns_ctxs[i] = @ptrCast(shards[i].namespace_handler);
        }
        for (0..n) |i| {
            shards[i].dispatcher.setWalkContexts(proto.OpCode.namespace_list, ns_ctxs);
        }
        self.walk_ctx_slices[3] = ns_ctxs;

        // action_list → each shard's ActionsHandler
        const action_ctxs = try self.allocator.alloc(*anyopaque, n);
        for (0..n) |i| {
            action_ctxs[i] = @ptrCast(shards[i].actions_handler);
        }
        for (0..n) |i| {
            shards[i].dispatcher.setWalkContexts(proto.OpCode.action_list, action_ctxs);
        }
        self.walk_ctx_slices[4] = action_ctxs;

        // queue_list → each shard's QueueHandler
        const queue_ctxs = try self.allocator.alloc(*anyopaque, n);
        for (0..n) |i| {
            queue_ctxs[i] = @ptrCast(shards[i].queue_handler);
        }
        for (0..n) |i| {
            shards[i].dispatcher.setWalkContexts(proto.OpCode.queue_list, queue_ctxs);
        }
        self.walk_ctx_slices[5] = queue_ctxs;

        // processing_list → each shard's ProcessingHandler
        const proc_ctxs = try self.allocator.alloc(*anyopaque, n);
        for (0..n) |i| {
            proc_ctxs[i] = @ptrCast(shards[i].processing_handler);
        }
        for (0..n) |i| {
            shards[i].dispatcher.setWalkContexts(proto.OpCode.processing_list, proc_ctxs);
        }
        self.walk_ctx_slices[6] = proc_ctxs;

        // workflow_list_definitions → each shard's WorkflowHandler
        const wf_ctxs = try self.allocator.alloc(*anyopaque, n);
        for (0..n) |i| {
            wf_ctxs[i] = @ptrCast(shards[i].workflow_handler);
        }
        for (0..n) |i| {
            shards[i].dispatcher.setWalkContexts(proto.OpCode.workflow_list_definitions, wf_ctxs);
        }
        self.walk_ctx_slices[7] = wf_ctxs;
    }

    /// Graceful shutdown in reverse order.
    pub fn stop(self: *Runtime) void {
        if (!self.started) return;
        log.debug("Runtime.stop: initiating graceful shutdown", .{});

        // 0. Stop the HTTP servers first. The exporter must go down with the
        //    dashboard: otherwise it keeps answering /health with
        //    {"status":"ok"} and serving /metrics for a node that has stopped,
        //    for the whole window between stop() and deinit() — which includes
        //    every shard's final flush.
        if (self.dashboard_server) |server| {
            server.stop();
        }
        if (self.metrics_server) |ms| {
            ms.stop();
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
        stdx.time.sleep(100_000); // 100μs
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
    // Use an isolated temp data dir — never the default `~/.flo/data`, which
    // would otherwise persist into the developer's real home directory.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try stdx.fs.dirRealpathAlloc(tmp.dir, std.testing.allocator, ".");
    defer std.testing.allocator.free(data_dir);

    var runtime = try Runtime.init(std.testing.allocator, .{
        .num_shards = 2,
        .listen_port = 0, // ephemeral port
        .data_dir = data_dir,
    });
    defer runtime.deinit();

    try runtime.start();
    try std.testing.expect(runtime.started);
    try std.testing.expect(runtime.shards != null);
    try std.testing.expectEqual(@as(usize, 2), runtime.shards.?.len);

    // Let it run briefly
    stdx.time.sleep(10_000_000); // 10ms

    runtime.stop();
    try std.testing.expect(!runtime.started);
}

test "RuntimeConfig: single-node does not want a Raft listener (#42 item 5)" {
    // The old gate was `effectiveRaftPort() != listen_port`, but that helper
    // *derives* listen_port + 500 when no port is configured — so it was true
    // for every plain single-node server and Raft always bound its port.
    const plain = RuntimeConfig{ .listen_port = 9000 };
    try std.testing.expect(!plain.clusterListenerWanted());
    try std.testing.expectEqual(@as(u16, 9500), plain.effectiveRaftPort());
    // The old condition, for the record:
    try std.testing.expect(plain.effectiveRaftPort() != plain.listen_port);
}

test "RuntimeConfig: the listener comes up when peers are possible (#42 item 5)" {
    const seeded = RuntimeConfig{ .listen_port = 9000, .cluster_seeds = &.{"10.0.0.1:9500"} };
    try std.testing.expect(seeded.clusterListenerWanted());

    const explicit_port = RuntimeConfig{ .listen_port = 9000, .cluster_raft_port = 9500 };
    try std.testing.expect(explicit_port.clusterListenerWanted());

    const replicated = RuntimeConfig{ .listen_port = 9000, .cluster_replication_factor = 3 };
    try std.testing.expect(replicated.clusterListenerWanted());

    // `[cluster] enabled = true` used to be parsed and discarded entirely.
    const enabled = RuntimeConfig{ .listen_port = 9000, .cluster_enabled = true };
    try std.testing.expect(enabled.clusterListenerWanted());
}

test "RuntimeConfig: an ephemeral listen_port must not derive a privileged Raft port (#42 item 5)" {
    // `listen_port = 0` means "pick an ephemeral port", but effectiveRaftPort()
    // adds 500 unconditionally and yields 500 — a privileged port. Combined with
    // the old always-true gate, a plain two-shard test runtime tried to bind it:
    // macOS happens to permit bind(0.0.0.0:500), Linux returns EACCES, so this
    // failed only in CI.
    const ephemeral = RuntimeConfig{ .listen_port = 0 };
    try std.testing.expectEqual(@as(u16, 500), ephemeral.effectiveRaftPort());
    // Nothing about that config implies peers, so the listener never starts and
    // the privileged port is never bound.
    try std.testing.expect(!ephemeral.clusterListenerWanted());
}
