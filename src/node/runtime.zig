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
const shard_mod = @import("shard.zig");
const Shard = shard_mod.Shard;
const Acceptor = @import("acceptor.zig").Acceptor;
const Mailbox = @import("mailbox.zig").Mailbox;
const InboxMessage = @import("inbox.zig").Message;
const proto = @import("../protocol/proto.zig");
const server_config = @import("../config/server.zig");
const Durability = server_config.Durability;
const TieredLogConfig = @import("../config/tiered_log.zig").TieredLogConfig;
const RaftNetwork = @import("../raft/network.zig").RaftNetwork;
const RaftNodeConfig = @import("../raft/node.zig").Config;
const RaftQueue = @import("../raft/raft_queue.zig").RaftQueue;
const RAFT_QUEUE_CAPACITY = @import("../raft/network.zig").RAFT_QUEUE_CAPACITY;
const cluster_config = @import("../config/cluster.zig");
const offline = @import("offline.zig");
const raft_hard_state = @import("../raft/hard_state.zig");
const StreamHandler = @import("../stream/handler.zig").StreamHandler;
const KVHandler = @import("../kv/handler.zig").KVHandler;
const TSHandler = @import("../ts/handler.zig").TSHandler;
const QueueHandler = @import("../queue/handler.zig").QueueHandler;
const ActionsHandler = @import("../actions/handler.zig").ActionsHandler;
const manifest = @import("manifest.zig");
const data_format = @import("data_format.zig");
const DashboardServer = @import("dashboard/mod.zig").DashboardServer;
const DashboardServerConfig = @import("dashboard/mod.zig").DashboardServerConfig;
const DashboardContext = @import("dashboard/api.zig").DashboardContext;
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;
const HttpMetricsServer = @import("../metrics/http_server.zig").HttpMetricsServer;
const NodeHealth = @import("health.zig").NodeHealth;

/// Runtime configuration produced by ServerConfig.toRuntimeConfig()
///
/// ## Port Derivation
///
/// Auxiliary ports (metrics, dashboard, raft) default to 0, meaning
/// "derive from listen_port + offset". Use the `effective*Port()` methods
/// instead of accessing the port fields directly. This allows running multiple
/// instances with a single `--port` flag:
///
///   --port 10000  →  metrics=10001, dashboard=10002, raft=10500
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

    num_shards: u16 = 0,
    partition_count: u32 = 0,
    data_dir: []const u8 = "~/.flo/data",

    /// TCP port for client connections (default: 9000)
    listen_port: u16 = 9000,
    listen_addr: []const u8 = "0.0.0.0",

    durability: Durability = .async_flush,
    tiered_log: TieredLogConfig = .{},

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
    /// Localhost unless asked: the dashboard authenticates no one yet.
    dashboard_bind: []const u8 = "127.0.0.1",
    dashboard_cors_origins: ?[]const u8 = null,
    dashboard_hosts: ?[]const u8 = null,

    /// Start the peer-facing Raft listener even without seeds. See
    /// `ClusterConfig.enabled`.
    cluster_enabled: bool = false,
    cluster_node_id: u32 = 0,
    /// Port for Raft RPC communication (0 = derive from listen_port + 500)
    cluster_raft_port: u16 = 0,
    cluster_seeds: []const []const u8 = &.{},
    /// Proven by every peer before it is one; required with the listener.
    cluster_secret: ?[]const u8 = null,
    /// The one timing knob: a leader unheard for this long is replaced.
    /// Election timeouts are [½, 1] × this, heartbeats a sixth of it.
    cluster_failover_timeout_ms: u32 = 1500,
    /// Where the admin socket listens; null for admin.sock in the data dir.
    admin_socket: ?[]const u8 = null,
    /// A worker silent this long is unhealthy; this long, removed.
    worker_heartbeat_timeout_ms: u64 = 90_000,
    worker_remove_after_ms: u64 = 600_000,

    // =========================================================================
    // Port Derivation Methods
    // =========================================================================
    // Use these instead of accessing port fields directly.
    //
    // `listen_port = 0` means "bind an ephemeral port", so it is not a base an
    // offset can be added to: listen_port + 1 is port 1, + 2 is port 2, + 500 is
    // port 500 — all privileged, and all refused on Linux without
    // CAP_NET_BIND_SERVICE. An unresolved base therefore derives an unresolved
    // (ephemeral) port rather than a low one.

    /// Get effective metrics port (derives from listen_port + 1 if set to 0)
    pub fn effectiveMetricsPort(self: RuntimeConfig) u16 {
        if (self.metrics_port > 0) return self.metrics_port;
        if (self.listen_port == 0) return 0;
        return self.listen_port +| PORT_OFFSET_METRICS;
    }

    /// Get effective dashboard port (derives from listen_port + 2 if set to 0)
    pub fn effectiveDashboardPort(self: RuntimeConfig) u16 {
        if (self.dashboard_port > 0) return self.dashboard_port;
        if (self.listen_port == 0) return 0;
        return self.listen_port +| PORT_OFFSET_DASHBOARD;
    }

    /// Get effective Raft RPC port (derives from listen_port + 500 if set to 0)
    pub fn effectiveRaftPort(self: RuntimeConfig) u16 {
        if (self.cluster_raft_port > 0) return self.cluster_raft_port;
        if (self.listen_port == 0) return 0;
        return self.listen_port +| PORT_OFFSET_RAFT;
    }

    /// Whether this node should bring up the peer-facing Raft listener.
    ///
    /// True when peers are possible: the operator started the first member
    /// (`--cluster`, `[cluster] enabled`) or named seeds to join. A plain
    /// single-node server leaves the port unbound.
    pub fn clusterListenerWanted(self: RuntimeConfig) bool {
        return self.cluster_enabled or self.cluster_seeds.len > 0;
    }
};

/// Node runtime — manages shard threads, acceptor thread, and lifecycle.
pub const LOCK_FILENAME = data_format.LOCK_FILENAME;

pub const ADMIN_SOCKET_FILENAME = data_format.ADMIN_SOCKET_FILENAME;

/// The admin socket's path: `configured`, else admin.sock in `data_dir`.
/// Both are expanded already. The server and the CLI agree through this.
pub fn adminSocketPath(buf: []u8, data_dir: []const u8, configured: ?[]const u8) error{NoSpaceLeft}![]const u8 {
    if (configured) |c| return std.fmt.bufPrint(buf, "{s}", .{c});
    return std.fmt.bufPrint(buf, "{s}/{s}", .{ data_dir, ADMIN_SOCKET_FILENAME });
}

/// Take the data dir's lock, or refuse if another server holds it. The lock
/// is the OS's (flock), so it goes when the holder exits, however it exits.
pub fn lockDataDir(data_dir: []const u8) !stdx.fs.File {
    try stdx.fs.makePath(data_dir);
    return lockMade(data_dir);
}

/// Whether `data_dir` holds a lock file, held or not.
pub fn lockFileExists(data_dir: []const u8) bool {
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = std.fmt.bufPrint(&buf, "{s}/{s}", .{ data_dir, LOCK_FILENAME }) catch return false;
    stdx.fs.access(path, .{}) catch return false;
    return true;
}

/// Removes `data_dir`'s lock file: for a command that made it and then
/// refused the directory, so a refusal leaves nothing behind. The lock
/// itself goes when its holder closes it.
pub fn removeLockFile(data_dir: []const u8) void {
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = std.fmt.bufPrint(&buf, "{s}/{s}", .{ data_dir, LOCK_FILENAME }) catch return;
    stdx.fs.deleteFile(path) catch {};
}

/// The same lock for an offline tool, which must not create a data dir a
/// mistyped path names.
pub fn lockExistingDataDir(data_dir: []const u8) !stdx.fs.File {
    const dir = stdx.fs.openDir(data_dir, .{}) catch |err| switch (err) {
        error.FileNotFound => {
            log.err("data dir {s} does not exist", .{data_dir});
            return error.DataDirMissing;
        },
        else => return err,
    };
    stdx.fs.closeDir(dir);
    return lockMade(data_dir);
}

fn lockMade(data_dir: []const u8) !stdx.fs.File {
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, "{s}/{s}", .{ data_dir, LOCK_FILENAME });
    const file = stdx.fs.createFile(path, .{ .truncate = false, .lock = .exclusive, .lock_nonblocking = true }) catch |err| switch (err) {
        error.WouldBlock => {
            log.err("data dir {s} is in use by another flo server ({s} is locked); stop that server, or give this one another --data-dir", .{ data_dir, LOCK_FILENAME });
            return error.DataDirInUse;
        },
        else => return err,
    };
    if (otherProcessCanLock(path)) {
        var fs_buf: [64]u8 = undefined;
        log.warn("data dir {s} is on a filesystem ({s}) that doesn't enforce flock between processes: nothing stops a second flo server on it, and two servers on one data dir lose data. Put the data dir on a local filesystem (for Docker on a Mac, a named volume, not a bind mount)", .{ data_dir, filesystemOf(data_dir, &fs_buf) orelse "unknown" });
    }
    return file;
}

/// Whether another process can take the exclusive lock this one holds on
/// `path`, i.e. whether the filesystem ignores flock (Docker Desktop's Mac
/// bind mounts do). The probe is a forked child, not a second fd here: NFS
/// emulates flock with per-process POSIX locks, which a second fd in the same
/// process would take. The child only opens, tries LOCK_NB and exits, so it
/// can't block or outlive the check.
fn otherProcessCanLock(path: []const u8) bool {
    var buf: [std.fs.max_path_bytes:0]u8 = undefined;
    if (path.len >= buf.len) return false;
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;

    const pid = std.c.fork();
    if (pid < 0) return false;
    if (pid == 0) {
        const fd = std.c.open(&buf, .{ .ACCMODE = .RDONLY, .CLOEXEC = true });
        std.c._exit(if (fd >= 0 and std.c.flock(fd, std.posix.LOCK.EX | std.posix.LOCK.NB) == 0) 0 else 1);
    }
    var status: c_int = 0;
    while (std.c.waitpid(pid, &status, 0) < 0) {
        if (std.c._errno().* != @backingInt(std.c.E.INTR)) return false;
    }
    return std.c.W.IFEXITED(@bitCast(status)) and std.c.W.EXITSTATUS(@bitCast(status)) == 0;
}

/// The type of the filesystem `dir` is on ("ext4", "nfs4", "fakeowner"), from the longest matching mount
/// point in /proc/self/mountinfo. Null off Linux or when it can't be read.
fn filesystemOf(dir: []const u8, out: []u8) ?[]const u8 {
    if (@import("builtin").os.tag != .linux) return null;
    var real_buf: [std.fs.max_path_bytes]u8 = undefined;
    const real_len = stdx.fs.cwd().realPathFile(stdx.io.instance(), dir, &real_buf) catch return null;
    const real = real_buf[0..real_len];
    var buf: [64 * 1024]u8 = undefined;
    const text = stdx.fs.readFile("/proc/self/mountinfo", &buf) catch return null;
    var best_len: usize = 0;
    var best: ?[]const u8 = null;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        // "id parent maj:min root mount-point options... - fstype source ..."
        var fields = std.mem.tokenizeScalar(u8, line, ' ');
        for (0..4) |_| _ = fields.next() orelse break;
        const mount_point = fields.next() orelse continue;
        const sep = std.mem.indexOf(u8, line, " - ") orelse continue;
        var after = std.mem.tokenizeScalar(u8, line[sep + 3 ..], ' ');
        const fstype = after.next() orelse continue;
        // Mount points with spaces are escaped (\040); those never match.
        const under = std.mem.eql(u8, mount_point, "/") or (std.mem.startsWith(u8, real, mount_point) and
            (real.len == mount_point.len or real[mount_point.len] == '/'));
        if (under and mount_point.len >= best_len) {
            best_len = mount_point.len;
            best = fstype;
        }
    }
    const fstype = best orelse return null;
    const n = @min(fstype.len, out.len);
    @memcpy(out[0..n], fstype[0..n]);
    return out[0..n];
}

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
    /// What the shards share while they stop, so each can answer the
    /// others' clients before any closes its sockets.
    stopping: Shard.Stopping,

    /// Acceptor.
    acceptor: ?Acceptor,

    /// Acceptor thread.
    acceptor_thread: ?std.Thread,
    /// The admin socket's path while it listens, to remove at stop.
    admin_path_buf: [std.fs.max_path_bytes]u8 = undefined,
    admin_path_len: usize = 0,
    /// The file it bound: only that is removed at stop.
    admin_file: ?stdx.fs.Meta = null,

    /// Write-ends of shard pipes (slice into pipes for acceptor).
    pipe_write_ends: ?[]i32,

    /// Whether the runtime has been started.
    started: bool,

    /// Raft networking layer for cluster entry replication.
    raft_network: ?*RaftNetwork,
    /// Frames from peers to shard 0; owned here, outlives both ends.
    raft_queue: ?*RaftQueue,

    /// Dashboard HTTP server (serves REST API + static files).
    dashboard_server: ?*DashboardServer,

    /// Dashboard context (metrics + shard info for API handlers).
    dashboard_ctx: ?*DashboardContext,

    /// Metrics registry for dashboard and Prometheus endpoint.
    metrics_registry: ?*MetricsRegistry,

    /// Prometheus exporter (serves GET /metrics on the metrics port).
    metrics_server: ?*HttpMetricsServer,

    /// Each shard's published state, for /ready and /status. Outlives both
    /// HTTP servers.
    health: ?*NodeHealth,

    /// Walk context arrays — allocated during wireWalkContexts, freed on deinit.
    /// Each entry is a per-shard array of projection pointers for one walk opcode.
    /// Slots: [0] = ts_list. More slots added as modules gain list support.
    walk_ctx_slices: [8]?[]*anyopaque,

    /// Cross-shard stream handler array — allocated during wirePeerStreamHandlers, freed on deinit.
    peer_stream_handlers_slice: ?[]const *StreamHandler,

    /// Every shard's mailbox — allocated during wirePeerMailboxes, freed on deinit.
    peer_mailboxes_slice: ?[]*Mailbox,

    /// Cross-shard KV handler array — allocated during wirePeerKvHandlers, freed on deinit.
    peer_kv_handlers_slice: ?[]const *KVHandler,

    /// Cross-shard TS / queue handler arrays — allocated during wirePeer*, freed on deinit.
    peer_ts_handlers_slice: ?[]const *TSHandler,
    peer_queue_handlers_slice: ?[]const *QueueHandler,

    /// Cross-shard shard pointer array — allocated during wirePeerShards, freed on deinit.
    peer_shards_slice: ?[]*Shard,

    /// Held for the runtime's life: a second server on the same data dir
    /// would interleave writes to the same segments and manifest.
    data_dir_lock: ?stdx.fs.File,

    /// The client port this runtime bound: the configured one, or the one
    /// the OS picked for port 0. 0 before `start` binds it.
    client_port: u16 = 0,

    /// Told the ports once the HTTP listeners are up, before the shards
    /// replay: with port 0 the only way to reach a booting node.
    on_listening: ?Listening = null,

    pub const Listening = struct {
        ctx: *anyopaque,
        f: *const fn (ctx: *anyopaque, ports: Ports) void,
    };

    /// The ports this node bound; null for a listener it does not run.
    /// The peer port is bound after the HTTP listeners, so it is null
    /// until then.
    pub const Ports = struct {
        client: u16,
        peer: ?u16,
        metrics: ?u16,
        dashboard: ?u16,
    };

    pub fn ports(self: *const Runtime) Ports {
        return .{
            .client = self.client_port,
            .peer = if (self.raft_network) |rn| rn.listen_port else null,
            .metrics = if (self.metrics_server) |ms| ms.port else null,
            .dashboard = if (self.dashboard_server) |ds| ds.config.port else null,
        };
    }

    pub fn init(allocator: std.mem.Allocator, config: RuntimeConfig) !Runtime {
        if (config.num_shards > server_config.MAX_SHARDS) {
            log.err("shards = {d}: at most {d} per node", .{ config.num_shards, server_config.MAX_SHARDS });
            return error.InvalidShardCount;
        }
        // A cluster replicates one shard until every shard has a group, so
        // an automatic count resolves to one there; an explicit larger
        // count is refused at start.
        const shard_count = if (config.clusterListenerWanted() and config.num_shards == 0) 1 else detectShardCount(config.num_shards);

        return .{
            .allocator = allocator,
            .config = config,
            .shard_count = shard_count,
            .shards = null,
            .pipes = null,
            .shard_threads = null,
            .stopping = .{},
            .acceptor = null,
            .acceptor_thread = null,
            .pipe_write_ends = null,
            .started = false,
            .raft_network = null,
            .raft_queue = null,
            .dashboard_server = null,
            .dashboard_ctx = null,
            .metrics_registry = null,
            .metrics_server = null,
            .health = null,
            .walk_ctx_slices = .{ null, null, null, null, null, null, null, null },
            .data_dir_lock = null,
            .peer_stream_handlers_slice = null,
            .peer_kv_handlers_slice = null,
            .peer_ts_handlers_slice = null,
            .peer_queue_handlers_slice = null,
            .peer_mailboxes_slice = null,
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
        if (self.health) |h| {
            h.deinit(self.allocator);
            self.allocator.destroy(h);
            self.health = null;
        }

        // Clean up raft network, then the queue it fed (the shards that
        // drained it have stopped by now).
        if (self.raft_network) |rn| {
            rn.deinit();
            self.allocator.destroy(rn);
            self.raft_network = null;
        }
        if (self.raft_queue) |q| {
            q.deinit();
            self.allocator.destroy(q);
            self.raft_queue = null;
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

        if (self.peer_mailboxes_slice) |s| {
            self.allocator.free(s);
            self.peer_mailboxes_slice = null;
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

        // Last: everything that writes to the data dir has stopped.
        if (self.data_dir_lock) |f| {
            stdx.fs.closeFile(f);
            self.data_dir_lock = null;
        }
    }

    const NodeIdentity = struct {
        id: u32,
        source: enum { stored, configured, derived },
    };

    /// The id this node runs under. Shard 0's HARDSTATE wins once it exists;
    /// a configured id that disagrees with it is refused rather than split
    /// into "the id on disk" and "the id on the wire".
    fn resolveClusterNodeId(self: *Runtime, data_dir: []const u8) !NodeIdentity {
        var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
        const shard0_dir = try std.fmt.bufPrint(&dir_buf, "{s}/{d:0>5}", .{ data_dir, 0 });
        const stored: ?u32 = blk: {
            const hs = raft_hard_state.load(shard0_dir) catch |err| {
                log.err("cluster: cannot read {s}/{s}: {s}", .{ shard0_dir, raft_hard_state.FILENAME, @errorName(err) });
                return err;
            };
            const h = hs orelse break :blk null;
            break :blk if (h.node_id != 0) h.node_id else null;
        };
        const configured: ?u32 = if (self.config.cluster_node_id > 0) self.config.cluster_node_id else null;

        if (stored != null and configured != null and stored.? != configured.?) {
            log.err("cluster: data dir {s} belongs to node {d} but [cluster] node_id = {d}; `flo server inspect --data-dir {s}` shows what it holds. Remove node_id from the config to keep this data, or point data_dir at an empty directory to start a new node", .{ data_dir, stored.?, configured.?, data_dir });
            return error.NodeIdMismatch;
        }
        if (stored) |id| return .{ .id = id, .source = .stored };
        if (configured) |id| return .{ .id = id, .source = .configured };

        var host_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
        const host = std.posix.gethostname(&host_buf) catch "localhost";
        return .{ .id = cluster_config.ClusterConfig.generateNodeId(host, self.client_port), .source = .derived };
    }

    /// Start the runtime: create shards, spawn threads, start acceptor.
    pub fn start(self: *Runtime) !void {
        // A runtime started again counts its shards' stop afresh.
        self.stopping = .{};
        // Expand a leading `~` to $HOME at the single point where the configured
        // data dir first becomes real directories. Without this, a default or
        // CLI-bypassing `~/.flo/data` would have `makePath` create a literal `~`
        // directory under the current working directory (both here via
        // ensureTopology and below via each Shard.init).
        const data_dir = try stdx.fs.expandTilde(self.allocator, self.config.data_dir);
        defer self.allocator.free(data_dir);

        // `[server] bind` names the interface this node serves on — clients
        // and peers alike. Reject a bad value before any listener exists.
        const bind_ip = stdx.net.parseIp4Bind(self.config.listen_addr) catch {
            log.err("invalid [server] bind '{s}': expected a dotted IPv4 address or \"localhost\"", .{self.config.listen_addr});
            return error.InvalidBindAddress;
        };

        // A directory refused for its format, or as not Flo's, is left as
        // it was found: without the lock file, if this start made it.
        const lock_was_there = lockFileExists(data_dir);
        self.data_dir_lock = try lockDataDir(data_dir);

        log.debug("Runtime.start: shard_count={d} listen_port={d} data_dir={s}", .{
            self.shard_count,
            self.config.listen_port,
            data_dir,
        });

        // 0. The data directory's format, then its topology: a newer format
        // is refused before anything in it is read.
        data_format.check(self.allocator, data_dir) catch |err| {
            if (data_format.isRefusal(err) and !lock_was_there) removeLockFile(data_dir);
            return err;
        };
        try manifest.ensureTopology(
            self.allocator,
            data_dir,
            self.shard_count,
            self.config.partition_count,
        );

        // The client socket is bound before anything uses this node's
        // client port (the derived node id, the dashboard, the peer hello):
        // with port 0 the OS picks it here. It listens only once the shards
        // run.
        const client_fd = try Acceptor.bindSocket(bind_ip, self.config.listen_port);
        var client_fd_owned = true;
        errdefer if (client_fd_owned) stdx.net.sysClose(client_fd);
        self.client_port = (try stdx.net.sysLocalIp4(client_fd)).port;

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

        // This node's cluster identity exists whether or not the Raft listener
        // runs — `flo cluster status` reports it single-node too. Once the
        // data dir holds it, it is fixed: a node keeps its id across restarts
        // and port changes, so a member that moves is an update, not a stranger.
        const identity = try self.resolveClusterNodeId(data_dir);
        const cluster_node_id = identity.id;
        // A derived id comes from hostname:port, and port 0 is another port
        // every start. The id is stored only once the node first persists a
        // term, and a leader names a joiner in the membership before that:
        // a joiner restarted in between would come back as a stranger.
        if (self.config.clusterListenerWanted() and identity.source == .derived and self.config.listen_port == 0) {
            log.err("cluster: --port 0 picks another port on every start, and this node's id would be derived from it; give its first start [cluster] node_id (or --node-id), or a fixed --port", .{});
            return error.ClusterNodeIdUnstable;
        }
        if (self.config.clusterListenerWanted()) {
            log.info("cluster: this node is {d} ({s})", .{ cluster_node_id, switch (identity.source) {
                .stored => "stored in the data dir",
                .configured => "configured",
                .derived => "derived from hostname:port; set [cluster] node_id on cloned images",
            } });
        }

        // How shard 0's group comes up: alone, as the first member, or
        // joining the seeds. Only shard 0 has peers until every shard
        // replicates, so a cluster runs one shard.
        if (self.config.cluster_enabled and self.config.cluster_seeds.len > 0) {
            log.err("cluster: --cluster starts the first member and --join joins members that exist; pass one, not both", .{});
            return error.ClusterRoleAmbiguous;
        }
        if (self.config.cluster_raft_port > 0 and !self.config.clusterListenerWanted()) {
            log.err("cluster: raft_port (or --raft-port) is set but this node is neither the first member (--cluster) nor joining one (--join); pass one, or remove the port", .{});
            return error.ClusterRoleUnset;
        }
        const shard0_role: shard_mod.ClusterRole = if (!self.config.clusterListenerWanted())
            .single
        else if (self.config.cluster_seeds.len > 0)
            .join
        else
            .bootstrap;
        if (shard0_role != .single and self.shard_count > 1) {
            log.err("cluster: a cluster replicates one shard for now; leave shards (or --shards) at its default or set 1 (this node has {d})", .{self.shard_count});
            return error.ClusterNeedsOneShard;
        }
        var raft_config = RaftNodeConfig.fromFailover(self.config.cluster_failover_timeout_ms);
        raft_config.durable_commits = self.config.durability == .sync;
        raft_config.self_counts_when_durable = self.config.durability == .sync;

        // 1.5 HTTP before the shards: replaying a large log takes a while,
        //     and a probe that finds nothing listening reads it as dead.
        const node_health = try self.allocator.create(NodeHealth);
        node_health.* = NodeHealth.init(self.allocator, cluster_node_id, self.shard_count) catch |err| {
            self.allocator.destroy(node_health);
            return err;
        };
        self.health = node_health;
        try self.startHttp(bind_ip);
        if (self.on_listening) |l| l.f(l.ctx, self.ports());

        // 2. Create shards, replaying each one's log, several at once.
        const shards = try self.allocator.alloc(Shard, self.shard_count);
        // Until the boot below has run and its outcome is handled.
        var boot_handled = false;
        errdefer if (!boot_handled) {
            self.allocator.free(shards);
            self.stopHttp();
        };
        var boot: ShardBoot = .{
            .runtime = self,
            .shards = shards,
            .health = node_health,
            .data_dir = data_dir,
            .pipes = pipes,
            .cluster_node_id = cluster_node_id,
            .shard0_role = shard0_role,
            .raft_config = raft_config,
            .failed = try self.allocator.alloc(?anyerror, self.shard_count),
        };
        defer self.allocator.free(boot.failed);
        @memset(boot.failed, null);
        boot.run();
        boot_handled = true;
        if (boot.firstError()) |err| {
            for (shards, boot.failed) |*shard, failed| {
                if (failed == null) shard.deinit();
            }
            self.allocator.free(shards);
            self.stopHttp();
            return err;
        }
        errdefer {
            for (shards) |*shard| shard.deinit();
            self.allocator.free(shards);
            self.shards = null;
        }
        // Runs before the shards are freed: the dashboard reads them.
        errdefer self.stopHttp();
        self.shards = shards;
        const shards_created = shards.len;

        try self.wireWalkContexts(shards);
        for (shards) |*s| {
            if (s.dispatcher.unwiredWalk()) |op| {
                log.err("op 0x{x} lists across shards, but no shard's walk contexts are wired for it", .{op});
                return error.UnwiredWalk;
            }
        }

        // 2.55 Wire cross-shard stream handler references for processing pipelines.
        try self.wirePeerStreamHandlers(shards);

        // 2.555 Wire cross-shard KV handler references for processing KV lookups + sinks.
        try self.wirePeerKvHandlers(shards);

        // 2.556 Wire cross-shard TS / queue handler references for processing sinks.
        try self.wirePeerTsHandlers(shards);
        try self.wirePeerQueueHandlers(shards);

        // 2.56 Give every shard every shard's mailbox.
        try self.wirePeerMailboxes(shards);

        // 2.57 Wire cross-shard shard pointers for pre-route forwarding.
        try self.wirePeerShards(shards);

        // 2.6 Register cooperative background tasks (hot_flush, etc.).
        // Must happen after shards are at final heap addresses.
        // Also wire shard back-pointers for handlers that need Raft access.
        for (0..self.shard_count) |i| {
            shards[i].worker_handler.heartbeat_timeout_ms = self.config.worker_heartbeat_timeout_ms;
            shards[i].worker_handler.remove_after_ms = self.config.worker_remove_after_ms;
            shards[i].registerBackgroundTasks();
            shards[i].wireHandlerShardPtrs();
            shards[i].applyDeferredTail();
        }
        if (self.metrics_registry) |metrics| {
            for (shards) |*shard| shard.setMetricsRegistry(metrics);
        }
        for (shards) |*shard| shard.publishHealth();
        if (self.dashboard_ctx) |ctx| try ctx.wireShards(self.allocator, shards);

        log.debug("Runtime.start: {d} shards initialized", .{shards_created});

        // 3. The shard threads' table, and their teardown if anything below fails
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
            self.stopping.of = @intCast(threads_spawned);
            for (0..threads_spawned) |i| shards[i].shutdown();
            for (0..threads_spawned) |i| threads[i].join();
            self.allocator.free(threads);
            self.shard_threads = null;
        }

        // 3.5. Start the peer-facing Raft listener only when this node can
        // have peers, before the shard threads run, so shard 0 never ticks
        // without its queue and network. Note this cannot be decided
        // by comparing effectiveRaftPort() against listen_port: that helper
        // *derives* listen_port + 500, so such a check is true even for a
        // lone node.
        if (self.config.clusterListenerWanted()) {
            const raft_port = self.config.effectiveRaftPort();
            const node_id = cluster_node_id;
            const secret = self.config.cluster_secret orelse "";
            if (secret.len == 0) {
                log.err("cluster: the peer listener needs [cluster] secret (the same value on every member, made by `flo server secret`); refusing to start it open", .{});
                return error.ClusterSecretRequired;
            }
            if (!cluster_config.secretWellFormed(secret)) {
                log.err("cluster: [cluster] secret is not one `flo server secret` made; run it once and give every member its output (in [cluster] secret, secret_file or FLO_CLUSTER_SECRET)", .{});
                return error.ClusterSecretMalformed;
            }
            // After force-members the old secret must never link again: the
            // members it left out still hold it.
            if (try offline.retiredAt(self.allocator, data_dir, secret)) |when| {
                var date: [10]u8 = undefined;
                log.err("cluster: this node's cluster secret was retired by force-members on {s}; give every node a new secret (flo server secret)", .{offline.formatDate(&date, when)});
                return error.ClusterSecretRetired;
            }

            const q = try self.allocator.create(RaftQueue);
            errdefer self.allocator.destroy(q);
            q.* = try RaftQueue.init(self.allocator, RAFT_QUEUE_CAPACITY);
            errdefer q.deinit();

            const rn = try self.allocator.create(RaftNetwork);
            // RaftNetwork.init binds a socket and can fail (the port is in
            // use). Nothing is published to this runtime's teardown until
            // both exist and the network thread is up.
            errdefer self.allocator.destroy(rn);
            rn.* = try RaftNetwork.init(self.allocator, node_id, raft_port, self.client_port, bind_ip, secret);
            errdefer rn.deinit();
            rn.setRaftQueue(q);
            if (self.metrics_registry) |metrics| rn.setReplicationMetrics(&metrics.replication);
            try rn.start();
            self.raft_queue = q;
            shards[0].raft_queue = q;
            shards[0].raft_network = rn;
            self.raft_network = rn;
        }

        // 3.6. Seeds are resolved here and dialed by the peer loop, so startup
        // never waits on a peer and only that thread touches the peer table.
        // Before the shard threads: a refusal below must not unwind shards
        // that are already running.
        if (self.raft_network) |rn| {
            var dialed: usize = 0;
            for (self.config.cluster_seeds) |seed| {
                const target = parseSeedAddress(seed) orelse {
                    log.warn("cluster: ignoring seed '{s}': expected host:port", .{seed});
                    continue;
                };
                // Retry a few times: in a container the seed's name may not
                // resolve until it is up.
                var attempt: usize = 0;
                while (attempt < SEED_RESOLVE_ATTEMPTS) : (attempt += 1) {
                    const ip4 = stdx.net.resolveIp4(self.allocator, target.host) catch |err| {
                        if (attempt + 1 == SEED_RESOLVE_ATTEMPTS) log.warn("cluster: seed '{s}' did not resolve: {s}", .{ seed, @errorName(err) });
                        stdx.time.sleep(200 * std.time.ns_per_ms);
                        continue;
                    };
                    switch (seedUsability(bind_ip, ip4, target.port, rn.listen_port)) {
                        .usable => {
                            rn.dialSeed(ip4, target.port);
                            dialed += 1;
                        },
                        .self => log.warn("cluster: seed '{s}' is this node and does not count", .{seed}),
                        .unreachable_from_loopback => {
                            log.err("cluster: [server] bind is loopback but seed '{s}' is on another host — peers could never reach this node; bind an interface they can", .{seed});
                            return error.BindUnreachableByPeers;
                        },
                    }
                    break;
                }
            }
            // A joiner with nobody to dial would follow nothing forever:
            // no vote, no leader, every write refused.
            if (self.config.cluster_seeds.len > 0 and dialed == 0) {
                log.err("cluster: no seed resolved to another node's address; this node cannot join anyone. Check the seed addresses (a seed that is this node does not count), or start the first member with --cluster", .{});
                return error.NoSeedReachable;
            }
        }

        // 3.7. Spawn shard threads
        for (0..self.shard_count) |i| {
            threads[i] = try std.Thread.spawn(.{}, shardThread, .{&shards[i]});
            threads_spawned += 1;
            log.debug("Runtime.start: spawned shard thread {d}", .{i});
        }

        // 4. Create and start acceptor
        var acceptor = Acceptor.init(write_ends, shards[0].router);
        try acceptor.listenOn(client_fd);
        client_fd_owned = false;
        try self.listenAdmin(&acceptor, data_dir);
        self.acceptor = acceptor;
        log.debug("Runtime.start: acceptor listening on {s}:{d}", .{ self.config.listen_addr, self.client_port });

        // 5. Spawn acceptor thread
        self.acceptor_thread = try std.Thread.spawn(.{}, acceptorThread, .{&self.acceptor.?});
        log.debug("Runtime.start: acceptor thread spawned", .{});
        node_health.accepting.store(true, .release);

        self.started = true;
        log.debug("Runtime.start: all components started, node is ready", .{});
    }

    /// Listen on the admin socket. A node starts without one rather than
    /// not at all when the path can't hold one safely: it serves only
    /// admin commands, and the warning says how to place it. One whose
    /// path another server answers on, or someone else's file holds, is
    /// refused: `[admin] socket` names it outside the data dir's lock, so
    /// two nodes could be configured onto one path.
    fn listenAdmin(self: *Runtime, acc: *Acceptor, data_dir: []const u8) !void {
        const configured = if (self.config.admin_socket) |c| stdx.fs.expandTilde(self.allocator, c) catch {
            log.warn("admin socket: cannot expand [admin] socket \"{s}\"; starting without one, so admin commands (flo cluster repair skip) can't reach this node", .{c});
            return;
        } else null;
        defer if (configured) |c| self.allocator.free(c);
        const path = adminSocketPath(&self.admin_path_buf, data_dir, configured) catch {
            log.warn("admin socket: the path is too long; starting without one, so admin commands (flo cluster repair skip) can't reach this node. Set [admin] socket to a shorter path", .{});
            return;
        };
        if (path.len > stdx.net.UNIX_PATH_MAX) {
            log.warn("admin socket: {s} is {d} bytes, past the {d} a Unix socket path may hold; starting without one, so admin commands (flo cluster repair skip) can't reach this node. Set [admin] socket to a shorter path", .{ path, path.len, stdx.net.UNIX_PATH_MAX });
            return;
        }
        const l = stdx.net.unixListen(path) catch |err| switch (err) {
            error.PathInUse => {
                log.err("admin socket: a server already answers on {s}; give each node its own [admin] socket, or leave it unset for admin.sock in the data directory", .{path});
                return error.AdminSocketInUse;
            },
            error.PathNotOurs => {
                log.err("admin socket: {s} is held by a file that isn't this user's socket; remove it, or set [admin] socket to a path only this user can write", .{path});
                return error.AdminSocketInUse;
            },
            error.UnsafeDirectory => {
                log.warn("admin socket: the directory of {s} can be written by other users and isn't sticky; starting without one, so admin commands (flo cluster repair skip) can't reach this node. Set [admin] socket in a directory only this user can write", .{path});
                return;
            },
            else => {
                log.warn("admin socket: cannot listen at {s}: {s}; starting without one, so admin commands (flo cluster repair skip) can't reach this node", .{ path, @errorName(err) });
                return;
            },
        };
        acc.admin_fd = l.fd;
        self.admin_file = l.file;
        self.admin_path_len = path.len;
    }

    /// The metrics registry, the exporter and the dashboard, before any
    /// shard exists. Until the shards are wired the dashboard's data APIs
    /// answer that the node is replaying; /health, /ready and /status
    /// answer throughout.
    fn startHttp(self: *Runtime, bind_ip: [4]u8) !void {
        const node_health = self.health.?;
        // Shared by the exporter and the dashboard; created when either is.
        if (self.config.metrics_enabled or self.config.dashboard_enabled) {
            const metrics = try self.allocator.create(MetricsRegistry);
            metrics.* = MetricsRegistry.init(self.allocator);
            self.metrics_registry = metrics;

            // Size the per-shard counter table. Without this `shardCount()`
            // stays 0 and the shard metric family is omitted entirely.
            metrics.initShards(self.shard_count) catch |err| {
                log.warn("metrics: initShards({d}) failed: {s}", .{ self.shard_count, @errorName(err) });
            };
        }

        if (self.config.metrics_enabled) {
            const metrics = self.metrics_registry.?;
            const ms = try self.allocator.create(HttpMetricsServer);
            ms.* = HttpMetricsServer.init(self.allocator, self.config.effectiveMetricsPort(), self.config.metrics_bind, metrics);
            ms.health = node_health;
            // A node asked for metrics that cannot serve them does not
            // start without them: /metrics and /status would be missing
            // with nothing but a log line to say so.
            ms.start() catch |err| {
                log.err("[metrics] cannot listen on {s}:{d}: {s}; free the port, set [metrics] port, or set [metrics] enabled = false", .{ self.config.metrics_bind, self.config.effectiveMetricsPort(), @errorName(err) });
                self.allocator.destroy(ms);
                return err;
            };
            self.metrics_server = ms;
        }

        if (self.config.dashboard_enabled) {
            const metrics = self.metrics_registry.?;

            const ctx = try self.allocator.create(DashboardContext);
            ctx.* = DashboardContext.init(self.allocator, metrics, self.shard_count);
            ctx.listen_port = self.client_port;
            // Dashboard writes go through the client port; a node bound to one
            // interface is not reachable at 127.0.0.1.
            ctx.client_ip4 = if (std.mem.eql(u8, &bind_ip, &stdx.net.any_ip4)) .{ 127, 0, 0, 1 } else bind_ip;
            ctx.health = node_health;
            self.dashboard_ctx = ctx;

            const server = try self.allocator.create(DashboardServer);
            server.* = DashboardServer.init(self.allocator, .{
                .port = self.config.effectiveDashboardPort(),
                .bind = self.config.dashboard_bind,
                .cors_origins = self.config.dashboard_cors_origins orelse "",
                .hosts = self.config.dashboard_hosts orelse "",
            }, ctx);
            self.dashboard_server = server;
            server.start() catch |err| {
                log.err("[dashboard] cannot listen on {s}:{d}: {s}; free the port, set [dashboard] port, or set [dashboard] enabled = false", .{ self.config.dashboard_bind, self.config.effectiveDashboardPort(), @errorName(err) });
                return err;
            };
        }
    }

    fn stopHttp(self: *Runtime) void {
        if (self.dashboard_server) |server| server.stop();
        if (self.metrics_server) |ms| ms.stop();
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

    /// Wire every shard's KV handler into each processing handler.
    fn wirePeerKvHandlers(self: *Runtime, shards: []Shard) !void {
        const n = self.shard_count;
        const handlers = try self.allocator.alloc(*KVHandler, n);
        for (0..n) |i| {
            handlers[i] = shards[i].kv_handler;
        }
        self.peer_kv_handlers_slice = handlers;

        for (0..n) |i| {
            shards[i].processing_handler.setPeerKvHandlers(handlers, shards[i].id);
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

    /// Give every shard every shard's mailbox (requests, replies and wakes).
    fn wirePeerMailboxes(self: *Runtime, shards: []Shard) !void {
        const n = self.shard_count;
        const mailboxes = try self.allocator.alloc(*Mailbox, n);
        for (0..n) |i| {
            mailboxes[i] = shards[i].mailbox;
        }
        self.peer_mailboxes_slice = mailboxes;
        for (0..n) |i| {
            shards[i].peer_mailboxes = mailboxes;
            shards[i].stopping = &self.stopping;
        }
    }

    /// Wire cross-shard shard pointers so runRequest can forward
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

        // kv_scan (full scan) → each shard's KV state
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

    /// The client port this runtime bound: the configured one, or the one
    /// the OS picked for port 0. 0 before `start`.
    pub fn boundPort(self: *const Runtime) u16 {
        return self.client_port;
    }

    /// Graceful shutdown in reverse order.
    pub fn stop(self: *Runtime) void {
        if (!self.started) return;
        log.debug("Runtime.stop: initiating graceful shutdown", .{});

        // 0. Stop the HTTP servers first. The exporter must go down with the
        //    dashboard: otherwise it keeps answering /health with ok and
        //    serving /metrics for a node that has stopped, for the whole
        //    window between stop() and deinit() — which includes every
        //    shard's final flush.
        self.stopHttp();

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
        if (self.admin_file) |file| {
            stdx.net.unixUnlink(self.admin_path_buf[0..self.admin_path_len], file);
            self.admin_file = null;
            self.admin_path_len = 0;
        }

        // 3. Send shutdown to each shard
        if (self.shards) |shards| {
            self.stopping.of = @intCast(shards.len);
            for (shards) |*shard| {
                _ = shard.mailbox.inbox.send(.{
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
// Booting the shards
// ═══════════════════════════════════════════════════════════════════════════════

/// Initialises every shard, replaying each one's log, on up to one worker
/// per CPU. Each worker takes the next shard and builds it in its slot.
/// What a shard creates (its reactor, io_uring ring or kqueue, mailbox and
/// files) is tied to no thread, so it can be built on a worker and run on
/// its own thread after. A failure stops the other replays early.
const ShardBoot = struct {
    runtime: *Runtime,
    shards: []Shard,
    health: *NodeHealth,
    data_dir: []const u8,
    pipes: []const [2]i32,
    cluster_node_id: u32,
    shard0_role: shard_mod.ClusterRole,
    raft_config: RaftNodeConfig,
    /// Per shard: why its init failed, or null once it is built.
    failed: []?anyerror,
    next: std.atomic.Value(usize) = .init(0),
    cancel: std.atomic.Value(bool) = .init(false),

    fn run(self: *ShardBoot) void {
        const cpus = std.Thread.getCpuCount() catch 1;
        const workers = @max(1, @min(self.shards.len, cpus));
        var threads: [server_config.MAX_SHARDS]std.Thread = undefined;
        var spawned: usize = 0;
        // This thread is one of the workers.
        while (spawned + 1 < workers) : (spawned += 1) {
            threads[spawned] = std.Thread.spawn(.{}, work, .{self}) catch break;
        }
        self.work();
        for (threads[0..spawned]) |t| t.join();
    }

    fn work(self: *ShardBoot) void {
        const rt = self.runtime;
        while (true) {
            const i = self.next.fetchAdd(1, .monotonic);
            if (i >= self.shards.len) return;
            if (self.cancel.load(.monotonic)) {
                self.failed[i] = error.BootCancelled;
                continue;
            }
            const slot = &self.health.shards[i];
            slot.set(.replaying);
            if (Shard.init(
                rt.allocator,
                @intCast(i),
                rt.shard_count,
                rt.config.partition_count,
                self.pipes[i][0],
                self.data_dir,
                rt.config.tiered_log.hot_buffer_capacity,
                rt.config.tiered_log.max_hot_entries,
                rt.config.tiered_log.hot_flush_seconds,
                rt.config.durability,
                self.cluster_node_id,
                if (i == 0) self.shard0_role else .single,
                self.raft_config,
                .{ .health = slot, .cancel = &self.cancel },
            )) |shard| {
                self.shards[i] = shard;
                self.failed[i] = null;
                // Replayed: say so now, not when every shard is done. Its
                // thread has not started, so publishing here races nothing.
                self.shards[i].publishHealth();
            } else |err| {
                self.failed[i] = err;
                if (err != error.BootCancelled) {
                    log.err("shard {d} could not start: {s}", .{ i, @errorName(err) });
                    self.cancel.store(true, .monotonic);
                }
            }
        }
    }

    /// The error that failed the boot: the first that was not another
    /// shard stopping because of it.
    fn firstError(self: *const ShardBoot) ?anyerror {
        var cancelled = false;
        for (self.failed) |f| {
            const err = f orelse continue;
            if (err != error.BootCancelled) return err;
            cancelled = true;
        }
        return if (cancelled) error.BootCancelled else null;
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
        acc.acceptAdmin() catch {};
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
    return @intCast(@max(1, @min(shards, server_config.MAX_SHARDS)));
}

const SeedAddress = struct { host: []const u8, port: u16 };

const SEED_RESOLVE_ATTEMPTS: usize = 30;

const SeedUsability = enum { usable, self, unreachable_from_loopback };

/// Whether a resolved seed is worth dialing from a node bound to `bind`.
/// Our own listener — the bind address, or any loopback address when bound
/// to 0.0.0.0 — is not; a seed reached by another of our addresses is
/// recognised on connect (our address at both ends) and dropped. A seed on
/// another host cannot dial us back if we only listen on loopback.
fn seedUsability(bind: [4]u8, seed_ip: [4]u8, seed_port: u16, our_raft_port: u16) SeedUsability {
    // Listening on every interface, any loopback address is ours; bound to
    // one address, only that address is.
    const bind_is_any = std.mem.eql(u8, &bind, &stdx.net.any_ip4);
    const seed_is_us = std.mem.eql(u8, &seed_ip, &bind) or (bind_is_any and stdx.net.isLoopback(seed_ip));
    if (seed_is_us and seed_port == our_raft_port) return .self;
    if (stdx.net.isLoopback(bind) and !stdx.net.isLoopback(seed_ip)) return .unreachable_from_loopback;
    return .usable;
}

test "runtime: the dashboard binds where the config file's default does, loopback" {
    const dashboard_config = @import("../config/dashboard.zig");
    try std.testing.expectEqualStrings("127.0.0.1", (RuntimeConfig{}).dashboard_bind);
    try std.testing.expectEqualStrings((dashboard_config.DashboardConfig{}).bind, (RuntimeConfig{}).dashboard_bind);
}

test "seedUsability: our own listener and unreachable binds are caught" {
    const lo: [4]u8 = .{ 127, 0, 0, 1 };
    const any: [4]u8 = .{ 0, 0, 0, 0 };
    const eth: [4]u8 = .{ 10, 0, 1, 12 };
    const other: [4]u8 = .{ 10, 0, 1, 13 };
    try std.testing.expectEqual(SeedUsability.self, seedUsability(any, lo, 9500, 9500));
    try std.testing.expectEqual(SeedUsability.self, seedUsability(eth, eth, 9500, 9500));
    try std.testing.expectEqual(SeedUsability.usable, seedUsability(any, lo, 9501, 9500));
    try std.testing.expectEqual(SeedUsability.usable, seedUsability(any, other, 9500, 9500));
    try std.testing.expectEqual(SeedUsability.usable, seedUsability(eth, other, 9500, 9500));
    try std.testing.expectEqual(SeedUsability.unreachable_from_loopback, seedUsability(lo, other, 9500, 9500));
    try std.testing.expectEqual(SeedUsability.usable, seedUsability(lo, .{ 127, 0, 0, 2 }, 9500, 9500));
}

/// Split a seed like "10.0.1.10:9500" or "flo-2:9500" into host and port.
/// Peer links are IPv4 only, so a bracketed IPv6 literal is not accepted.
fn parseSeedAddress(seed: []const u8) ?SeedAddress {
    const colon_idx = std.mem.lastIndexOfScalar(u8, seed, ':') orelse return null;
    if (colon_idx == 0 or colon_idx + 1 >= seed.len) return null;
    const host = seed[0..colon_idx];
    if (std.mem.indexOfScalar(u8, host, ':') != null or host[0] == '[') return null;
    const port = std.fmt.parseInt(u16, seed[colon_idx + 1 ..], 10) catch return null;
    if (port == 0) return null;
    return .{ .host = host, .port = port };
}

test "parseSeedAddress keeps the host and rejects what the dialer cannot use" {
    const a = parseSeedAddress("10.0.1.10:9500").?;
    try std.testing.expectEqualStrings("10.0.1.10", a.host);
    try std.testing.expectEqual(@as(u16, 9500), a.port);
    const b = parseSeedAddress("flo-2:9500").?;
    try std.testing.expectEqualStrings("flo-2", b.host);
    try std.testing.expect(parseSeedAddress("9500") == null);
    try std.testing.expect(parseSeedAddress(":9500") == null);
    try std.testing.expect(parseSeedAddress("host:") == null);
    try std.testing.expect(parseSeedAddress("host:0") == null);
    try std.testing.expect(parseSeedAddress("[::1]:9500") == null);
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

test "Runtime: more shards than a node can address are refused" {
    try std.testing.expectError(error.InvalidShardCount, Runtime.init(std.testing.allocator, .{ .num_shards = server_config.MAX_SHARDS + 1, .listen_port = 0 }));
    var ok = try Runtime.init(std.testing.allocator, .{ .num_shards = server_config.MAX_SHARDS, .listen_port = 0 });
    ok.deinit();
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

/// A FORMAT in `data_dir`, as a server that ran there wrote it: what a
/// test that builds a data directory by hand needs before a start reads it.
fn writeFormatFile(data_dir: []const u8) !void {
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const f = try stdx.fs.createFileAbsolute(try std.fmt.bufPrint(&buf, "{s}/{s}", .{ data_dir, data_format.FILENAME }), .{});
    defer stdx.fs.closeFile(f);
    try stdx.fs.writeAll(f, std.fmt.comptimePrint("{d}\n", .{data_format.CURRENT}));
}

test "Runtime: a shard that cannot boot fails the start with its own error, and nothing is left behind" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try stdx.fs.dirRealpathAlloc(tmp.dir, std.testing.allocator, ".");
    defer std.testing.allocator.free(data_dir);
    // Shard 2's directory is a file, so its init fails; the others boot,
    // or stop early, and are freed.
    try writeFormatFile(data_dir);
    {
        const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/00002", .{data_dir});
        defer std.testing.allocator.free(path);
        const f = try stdx.fs.createFileAbsolute(path, .{});
        stdx.fs.closeFile(f);
    }

    var runtime = try Runtime.init(std.testing.allocator, .{
        .num_shards = 4,
        .listen_port = 0,
        .data_dir = data_dir,
        .metrics_enabled = false,
        .dashboard_enabled = false,
    });
    defer runtime.deinit();
    // Shard 2's own error, not the others' stopping because of it.
    try std.testing.expectError(error.NotDir, runtime.start());
    try std.testing.expect(!runtime.started);
    try std.testing.expect(runtime.shards == null);
}

/// A shard log of `entries` queue enqueues in `data_dir`, as shard `id` of
/// `shard_count`, flushed to segments.
fn writeShardLog(data_dir: []const u8, id: u16, shard_count: u16, entries: usize) !void {
    const persistence = @import("../storage/persistence.zig");
    const entry_mod = @import("../storage/ual/entry.zig");
    const pipe_fds = try stdx.io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, id, shard_count, 0, pipe_fds[0], data_dir, @import("../storage/partition.zig").Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{}, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    for (0..entries) |i| {
        _ = try persistence.proposeEntry(&shard, .queue_enqueue, entry_mod.Flags.NONE, "", "q", &[_]u8{ 0, 0, 0, 0, 'x' });
        _ = shard.applyCommitted();
        if (i % 2000 == 1999) try shard.flushSegmentToDisk();
    }
    try shard.flushSegmentToDisk();
}

test "Runtime: each shard says it is done replaying as soon as it is, not when every shard is" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try stdx.fs.dirRealpathAlloc(tmp.dir, std.testing.allocator, ".");
    defer std.testing.allocator.free(data_dir);
    for (0..2) |i| try writeShardLog(data_dir, @intCast(i), 2, 100);

    var runtime = try Runtime.init(std.testing.allocator, .{ .num_shards = 2, .listen_port = 0, .data_dir = data_dir, .metrics_enabled = false, .dashboard_enabled = false });
    defer runtime.deinit();
    var health = try NodeHealth.init(std.testing.allocator, 1, 2);
    defer health.deinit(std.testing.allocator);
    var pipes: [2][2]i32 = undefined;
    for (&pipes) |*p| p.* = try stdx.io.pipe();
    defer for (pipes) |p| {
        _ = std.c.close(p[0]);
        _ = std.c.close(p[1]);
    };
    var shards: [2]Shard = undefined;
    var failed: [2]?anyerror = .{ null, null };
    var boot: ShardBoot = .{
        .runtime = &runtime,
        .shards = &shards,
        .health = &health,
        .data_dir = data_dir,
        .pipes = &pipes,
        .cluster_node_id = 1,
        .shard0_role = .single,
        .raft_config = .{},
        .failed = &failed,
    };
    boot.run();
    defer for (&shards) |*shard| shard.deinit();
    try std.testing.expect(boot.firstError() == null);
    // Published by its worker, before anything after the boot runs.
    for (health.shards) |*slot| try std.testing.expect(slot.get() != .replaying and slot.get() != .loading);
}

test "Runtime: a shard that cannot boot stops the others' replays before they finish" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try stdx.fs.dirRealpathAlloc(tmp.dir, std.testing.allocator, ".");
    defer std.testing.allocator.free(data_dir);
    for ([_]u16{ 0, 1, 3 }) |id| try writeShardLog(data_dir, id, 4, 20_000);
    try writeFormatFile(data_dir);
    {
        const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/00002", .{data_dir});
        defer std.testing.allocator.free(path);
        const f = try stdx.fs.createFileAbsolute(path, .{});
        stdx.fs.closeFile(f);
    }

    var runtime = try Runtime.init(std.testing.allocator, .{ .num_shards = 4, .listen_port = 0, .data_dir = data_dir, .metrics_enabled = false, .dashboard_enabled = false });
    defer runtime.deinit();
    try std.testing.expectError(error.NotDir, runtime.start());
    // However many ran at once, one of the others never reached its last
    // index: it stopped early, or was never started.
    var stopped_early = false;
    for ([_]usize{ 0, 1, 3 }) |i| {
        const slot = &runtime.health.?.shards[i];
        const last = slot.replay_last.load(.monotonic);
        if (last == 0 or slot.replay_index.load(.monotonic) < last) stopped_early = true;
    }
    try std.testing.expect(stopped_early);
}

test "RuntimeConfig: single-node does not want a Raft listener" {
    const plain = RuntimeConfig{ .listen_port = 9000 };
    try std.testing.expect(!plain.clusterListenerWanted());

    // A derived Raft port always differs from listen_port, so it must not be
    // used to infer whether this node is clustered.
    try std.testing.expectEqual(@as(u16, 9500), plain.effectiveRaftPort());
    try std.testing.expect(plain.effectiveRaftPort() != plain.listen_port);
}

test "RuntimeConfig: the listener comes up when peers are possible" {
    const seeded = RuntimeConfig{ .listen_port = 9000, .cluster_seeds = &.{"10.0.0.1:9500"} };
    try std.testing.expect(seeded.clusterListenerWanted());

    // A port alone names no role: startup refuses it rather than guessing.
    const explicit_port = RuntimeConfig{ .listen_port = 9000, .cluster_raft_port = 9500 };
    try std.testing.expect(!explicit_port.clusterListenerWanted());

    const enabled = RuntimeConfig{ .listen_port = 9000, .cluster_enabled = true };
    try std.testing.expect(enabled.clusterListenerWanted());
}

test "RuntimeConfig: an ephemeral listen_port must not derive a privileged Raft port" {
    // `listen_port = 0` means "pick an ephemeral port", so offsetting it would
    // yield privileged ports — 500 here, which Linux refuses. It must derive
    // ephemeral instead, and nothing about this config implies peers anyway.
    const ephemeral = RuntimeConfig{ .listen_port = 0 };
    try std.testing.expectEqual(@as(u16, 0), ephemeral.effectiveRaftPort());
    try std.testing.expect(!ephemeral.clusterListenerWanted());
}

test "RuntimeConfig: an ephemeral listen_port derives ephemeral ports, not privileged ones" {
    // listen_port = 0 asks the OS for any port, so there is no base to offset
    // from — offsetting anyway yields ports 1, 2 and 500, all privileged and
    // all refused on Linux. macOS permits bind(0.0.0.0:<low>), so a test that
    // only runs there will not catch a regression here.
    const ephemeral = RuntimeConfig{ .listen_port = 0 };
    try std.testing.expectEqual(@as(u16, 0), ephemeral.effectiveMetricsPort());
    try std.testing.expectEqual(@as(u16, 0), ephemeral.effectiveDashboardPort());
    try std.testing.expectEqual(@as(u16, 0), ephemeral.effectiveRaftPort());

    // A real base still derives as documented.
    const real = RuntimeConfig{ .listen_port = 9000 };
    try std.testing.expectEqual(@as(u16, 9001), real.effectiveMetricsPort());
    try std.testing.expectEqual(@as(u16, 9002), real.effectiveDashboardPort());
    try std.testing.expectEqual(@as(u16, 9500), real.effectiveRaftPort());

    // An explicit port always wins, even off an ephemeral base.
    const explicit = RuntimeConfig{ .listen_port = 0, .dashboard_port = 8080 };
    try std.testing.expectEqual(@as(u16, 8080), explicit.effectiveDashboardPort());
}

test "runtime: a data dir is locked by one holder at a time, and freed when it lets go" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try stdx.fs.dirRealpathAlloc(tmp.dir, std.testing.allocator, ".");
    defer std.testing.allocator.free(dir);

    const first = try lockDataDir(dir);
    try std.testing.expectError(error.DataDirInUse, lockDataDir(dir));
    stdx.fs.closeFile(first);
    stdx.fs.closeFile(try lockDataDir(dir));
}

test "runtime: the lock probe sees a lock another process holds, and a free one" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try stdx.fs.dirRealpathAlloc(tmp.dir, std.testing.allocator, ".");
    defer std.testing.allocator.free(dir);
    const path = try std.fs.path.join(std.testing.allocator, &.{ dir, LOCK_FILENAME });
    defer std.testing.allocator.free(path);

    const held = try lockDataDir(dir);
    try std.testing.expect(!otherProcessCanLock(path));
    stdx.fs.closeFile(held);
    // Unheld, the child takes it: what a filesystem that ignores flock looks like.
    try std.testing.expect(otherProcessCanLock(path));
}
