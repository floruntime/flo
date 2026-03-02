//! Server configuration loader for flo.toml
//! Parses TOML config and converts to RuntimeConfig

const std = @import("std");
const Allocator = std.mem.Allocator;
const toml = @import("toml.zig");
const RuntimeConfig = @import("../node/runtime.zig").RuntimeConfig;

/// Storage durability level — controls how writes are persisted.
/// In the rewritten architecture, durability is controlled per-partition
/// via the UAL configuration.
pub const Durability = enum(u8) {
    /// Wait for fdatasync before returning
    sync = 0,
    /// Return after WAL append (async flush) — DEFAULT
    async_flush = 1,
    /// No persistence (for caching use cases)
    ephemeral = 2,

    /// Parse durability from string (for CLI/config parsing)
    pub fn fromString(s: []const u8) Durability {
        if (std.mem.eql(u8, s, "sync")) return .sync;
        if (std.mem.eql(u8, s, "ephemeral")) return .ephemeral;
        return .async_flush; // default
    }
};

// Import from central config module - single source of truth
const cold_storage_config = @import("cold_storage.zig");
pub const ColdStorageProvider = cold_storage_config.ColdStorageProvider;
pub const ColdStorageConfig = cold_storage_config.ColdStorageConfig;
pub const FileConfig = cold_storage_config.FileConfig;
pub const S3Config = cold_storage_config.S3Config;

const auth_config = @import("auth.zig");
pub const AuthServerConfig = auth_config.AuthServerConfig;

const websocket_config = @import("websocket.zig");
pub const WebSocketConfig = websocket_config.WebSocketConfig;

const metrics_config = @import("metrics.zig");
pub const MetricsConfig = metrics_config.MetricsConfig;

const dashboard_config = @import("dashboard.zig");
pub const DashboardConfig = dashboard_config.DashboardConfig;

const cluster_config = @import("cluster.zig");
pub const ClusterConfig = cluster_config.ClusterConfig;

const tiered_log_config = @import("tiered_log.zig");
pub const TieredLogConfig = tiered_log_config.TieredLogConfig;

/// Maximum supported shards per node
/// Thread-per-shard architecture limits: 128 cores * 4 over-subscription = 512 practical max
/// We allow up to 1024 for future-proofing, but warn above 512
pub const MAX_SHARDS: u16 = 1024;

/// Server configuration loaded from flo.toml
pub const ServerConfig = struct {
    // [server] section
    port: u16 = 9000,
    bind: []const u8 = "0.0.0.0",
    data_dir: []const u8 = "~/.flo/data",
    shards: u16 = 0, // 0 = auto-detect CPU count; defines data topology (permanent!)
    partition_count: u32 = 0, // 0 = auto (max(4096, shards × 32)); virtual partitions for rebalancing

    // [storage] section — unified tier configuration
    // NOTE: memtable_size_mb removed - no SpilloverEngine in "Log is Data" architecture
    durability: Durability = .async_flush, // sync, async_flush, or ephemeral

    // [background_tasks] section
    /// Namespace deletion task interval in milliseconds
    /// Default: 5000ms (5s) for production, set lower in tests for faster cleanup
    namespace_deletion_interval_ms: i64 = 5000,

    // [cold_storage] section
    cold_storage: ColdStorageConfig = .{},

    // Tier settings (parsed from [storage] section)
    tiered_log: TieredLogConfig = .{},

    // [auth] section
    auth: AuthServerConfig = .{},

    // [websocket] section
    websocket: WebSocketConfig = .{},

    // [metrics] section
    metrics: MetricsConfig = .{},

    // [dashboard] section
    dashboard: DashboardConfig = .{},

    // [cluster] section
    cluster: ClusterConfig = .{},

    // [kv] section
    /// When true, external connections can see internal keys (prefixed with '_')
    /// Used for testing to verify internal state
    expose_internal_keys: bool = false,

    // [logging] section
    log_level: LogLevel = .info,
    log_format: LogFormat = .text,

    // Memory management
    allocator: Allocator,
    _owned_strings: std.ArrayListUnmanaged([]const u8) = .{},

    pub const LogLevel = enum {
        debug,
        info,
        warn,
        err,

        pub fn fromString(s: []const u8) LogLevel {
            if (std.mem.eql(u8, s, "debug")) return .debug;
            if (std.mem.eql(u8, s, "info")) return .info;
            if (std.mem.eql(u8, s, "warn") or std.mem.eql(u8, s, "warning")) return .warn;
            if (std.mem.eql(u8, s, "error") or std.mem.eql(u8, s, "err")) return .err;
            return .info; // default
        }
    };

    pub const LogFormat = enum {
        text,
        json,

        pub fn fromString(s: []const u8) LogFormat {
            if (std.mem.eql(u8, s, "json")) return .json;
            return .text;
        }
    };

    pub fn init(allocator: Allocator) ServerConfig {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ServerConfig) void {
        for (self._owned_strings.items) |s| {
            self.allocator.free(s);
        }
        self._owned_strings.deinit(self.allocator);
    }

    /// Convert to RuntimeConfig for use with Runtime.init()
    pub fn toRuntimeConfig(self: *const ServerConfig) RuntimeConfig {
        // Now both use the same ColdStorageConfig from central config module
        return RuntimeConfig{
            .num_shards = self.shards,
            .partition_count = self.partition_count,
            .data_dir = self.data_dir,
            .listen_port = self.port,
            .listen_addr = self.bind,
            .durability = self.durability,
            .cold_storage = if (self.cold_storage.provider != .none) self.cold_storage else null,
            .tiered_log = self.tiered_log,
            .auth_enabled = self.auth.enabled,
            .jwt_secret = self.auth.jwt_secret,
            .jwks_url = self.auth.jwks_url,
            .ws_rate_limit_requests = self.websocket.rate_limit_requests,
            .ws_rate_limit_window_ms = self.websocket.rate_limit_window_ms,
            .ws_ping_interval_ms = self.websocket.ping_interval_ms,
            .ws_pong_timeout_ms = self.websocket.pong_timeout_ms,
            .metrics_enabled = self.metrics.enabled,
            .metrics_port = self.metrics.port,
            .dashboard_enabled = self.dashboard.enabled,
            .dashboard_port = self.dashboard.port,
            .dashboard_bind = self.dashboard.bind,
            .dashboard_cors_origins = self.dashboard.cors_origins,
            .dashboard_admin_token = self.dashboard.admin_token,
            // Cluster configuration (always enabled, no explicit flag)
            .cluster_node_id = self.cluster.node_id,
            .cluster_raft_port = self.cluster.raft_port,
            .cluster_gossip_port = self.cluster.gossip_port,
            .cluster_seeds = self.cluster.seeds,
            .cluster_replication_factor = self.cluster.replication_factor,
            .cluster_election_timeout_min_ms = self.cluster.election_timeout_min_ms,
            .cluster_election_timeout_max_ms = self.cluster.election_timeout_max_ms,
            .cluster_heartbeat_interval_ms = self.cluster.heartbeat_interval_ms,
            .cluster_gossip_ping_interval_ms = self.cluster.gossip_ping_interval_ms,
            .cluster_gossip_ping_timeout_ms = self.cluster.gossip_ping_timeout_ms,
            .cluster_gossip_suspect_timeout_ms = self.cluster.gossip_suspect_timeout_ms,
            // Background task intervals
            .namespace_deletion_interval_ms = self.namespace_deletion_interval_ms,
            // KV configuration
            .expose_internal_keys = self.expose_internal_keys,
        };
    }

    fn dupeString(self: *ServerConfig, s: []const u8) ![]const u8 {
        const owned = try self.allocator.dupe(u8, s);
        try self._owned_strings.append(self.allocator, owned);
        return owned;
    }
};

/// Load server configuration from flo.toml file
pub fn load(allocator: Allocator, path: []const u8) !ServerConfig {
    var config = ServerConfig.init(allocator);
    errdefer config.deinit();

    var table = toml.parseFile(allocator, path) catch |err| {
        if (err == error.FileNotFound) {
            // Return defaults if config file doesn't exist
            return config;
        }
        return err;
    };
    defer table.deinit();

    // Parse [server] section
    if (table.getTable("server")) |server| {
        if (server.getInt("port")) |p| {
            config.port = @intCast(p);
        }
        if (server.getString("bind")) |b| {
            config.bind = try config.dupeString(b);
        }
        if (server.getString("data_dir")) |d| {
            config.data_dir = try config.dupeString(d);
        }
        if (server.getInt("shards")) |s| {
            config.shards = @intCast(s);
        }
        if (server.getInt("partition_count")) |pc| {
            config.partition_count = @intCast(pc);
        }
    }

    // Parse [storage] section — unified tier configuration
    if (table.getTable("storage")) |storage| {
        // NOTE: memtable_size_mb parsing removed - no SpilloverEngine
        if (storage.getString("durability")) |d| {
            config.durability = Durability.fromString(d);
        }
        // Tier settings (all under [storage])
        if (storage.getInt("hot_buffer_capacity")) |b| {
            config.tiered_log.hot_buffer_capacity = @intCast(b);
        }
        if (storage.getInt("max_hot_entries")) |m| {
            config.tiered_log.max_hot_entries = @intCast(m);
        }
        if (storage.getInt("hot_flush_seconds")) |h| {
            config.tiered_log.hot_flush_seconds = @intCast(h);
        }
        if (storage.getInt("max_local_segments")) |s| {
            config.tiered_log.max_local_segments = @intCast(s);
        }
        if (storage.getBool("enable_wal_truncation")) |e| {
            config.tiered_log.enable_wal_truncation = e;
        }
    }

    // Parse [background_tasks] section
    if (table.getTable("background_tasks")) |bt| {
        if (bt.getInt("namespace_deletion_interval_ms")) |n| {
            config.namespace_deletion_interval_ms = @intCast(n);
        }
    }

    // Parse [kv] section
    if (table.getTable("kv")) |kv| {
        if (kv.getBool("expose_internal_keys")) |e| {
            config.expose_internal_keys = e;
        }
    }

    // Parse [logging] section
    if (table.getTable("logging")) |logging| {
        if (logging.getString("level")) |level| {
            config.log_level = ServerConfig.LogLevel.fromString(level);
        }
        if (logging.getString("format")) |format| {
            config.log_format = ServerConfig.LogFormat.fromString(format);
        }
    }

    // Parse [auth] section
    if (table.getTable("auth")) |auth| {
        if (auth.getBool("enabled")) |e| {
            config.auth.enabled = e;
        }
        if (auth.getString("jwt_secret")) |s| {
            config.auth.jwt_secret = try config.dupeString(s);
        }
        if (auth.getString("jwks_url")) |u| {
            config.auth.jwks_url = try config.dupeString(u);
        }
    }

    // Parse [websocket] section
    if (table.getTable("websocket")) |ws| {
        if (ws.getInt("rate_limit_requests")) |r| {
            config.websocket.rate_limit_requests = @intCast(r);
        }
        if (ws.getInt("rate_limit_window_ms")) |w| {
            config.websocket.rate_limit_window_ms = @intCast(w);
        }
        if (ws.getInt("ping_interval_ms")) |p| {
            config.websocket.ping_interval_ms = @intCast(p);
        }
        if (ws.getInt("pong_timeout_ms")) |t| {
            config.websocket.pong_timeout_ms = @intCast(t);
        }
    }

    // Parse [metrics] section
    if (table.getTable("metrics")) |m| {
        if (m.getBool("enabled")) |e| {
            config.metrics.enabled = e;
        }
        if (m.getInt("port")) |p| {
            config.metrics.port = @intCast(p);
        }
        if (m.getString("bind")) |b| {
            config.metrics.bind = try config.dupeString(b);
        }
    }

    // Parse [dashboard] section
    if (table.getTable("dashboard")) |d| {
        if (d.getBool("enabled")) |e| {
            config.dashboard.enabled = e;
        }
        if (d.getInt("port")) |p| {
            config.dashboard.port = @intCast(p);
        }
        if (d.getString("bind")) |b| {
            config.dashboard.bind = try config.dupeString(b);
        }
        if (d.getString("cors_origins")) |c| {
            config.dashboard.cors_origins = try config.dupeString(c);
        }
    }

    // Parse [cluster] section (enabled field deprecated - cluster always runs)
    if (table.getTable("cluster")) |c| {
        // Note: "enabled" field is ignored (backwards compatibility)
        _ = c.getBool("enabled");

        if (c.getInt("node_id")) |n| {
            config.cluster.node_id = @intCast(n);
        }
        if (c.getInt("raft_port")) |p| {
            config.cluster.raft_port = @intCast(p);
        }
        if (c.getString("discovery_mode")) |m| {
            config.cluster.discovery_mode = ClusterConfig.DiscoveryMode.fromString(m);
        }
        if (c.getInt("replication_factor")) |r| {
            config.cluster.replication_factor = @intCast(r);
        }
        if (c.getInt("election_timeout_min_ms")) |t| {
            config.cluster.election_timeout_min_ms = @intCast(t);
        }
        if (c.getInt("election_timeout_max_ms")) |t| {
            config.cluster.election_timeout_max_ms = @intCast(t);
        }
        if (c.getInt("heartbeat_interval_ms")) |t| {
            config.cluster.heartbeat_interval_ms = @intCast(t);
        }
        // Parse seeds as comma-separated string (TOML parser doesn't support arrays yet)
        // Format: "host1:port1,host2:port2,host3:port3"
        if (c.getString("seeds")) |seeds_str| {
            var seeds_list: std.ArrayList([]const u8) = .empty;
            errdefer seeds_list.deinit(allocator);

            var iter = std.mem.splitScalar(u8, seeds_str, ',');
            while (iter.next()) |seed| {
                const trimmed = std.mem.trim(u8, seed, " \t");
                if (trimmed.len > 0) {
                    const owned = try config.dupeString(trimmed);
                    try seeds_list.append(allocator, owned);
                }
            }
            config.cluster.seeds = try seeds_list.toOwnedSlice(allocator);
        }
    }

    // Parse [cold_storage] section
    if (table.getTable("cold_storage")) |cold| {
        if (cold.getString("provider")) |p| {
            config.cold_storage.provider = ColdStorageProvider.fromString(p);
        }
        if (cold.getInt("upload_workers")) |w| {
            config.cold_storage.upload_workers = @intCast(w);
        }
        if (cold.getInt("restore_workers")) |w| {
            config.cold_storage.restore_workers = @intCast(w);
        }
        if (cold.getBool("verify_checksums")) |v| {
            config.cold_storage.verify_checksums = v;
        }
        if (cold.getBool("compression_enabled")) |v| {
            config.cold_storage.compression_enabled = v;
        }
        if (cold.getInt("hot_retention_days")) |d| {
            config.cold_storage.hot_retention_days = @intCast(d);
        }
        // Watermarks are specified as percentages (0-100), converted to 0.0-1.0
        if (cold.getInt("disk_high_watermark_percent")) |w| {
            config.cold_storage.disk_high_watermark = @as(f32, @floatFromInt(w)) / 100.0;
        }
        if (cold.getInt("disk_critical_watermark_percent")) |w| {
            config.cold_storage.disk_critical_watermark = @as(f32, @floatFromInt(w)) / 100.0;
        }
        if (cold.getInt("min_upload_batch_size")) |s| {
            config.cold_storage.min_upload_batch_size = @intCast(s);
        }
        if (cold.getInt("max_upload_batch_age_seconds")) |s| {
            config.cold_storage.max_upload_batch_age_seconds = @intCast(s);
        }

        // Parse [cold_storage.file] section
        if (cold.getTable("file")) |file_cfg| {
            if (file_cfg.getString("base_path")) |p| {
                config.cold_storage.file.base_path = try config.dupeString(p);
            }
            if (file_cfg.getBool("sync_on_write")) |v| {
                config.cold_storage.file.sync_on_write = v;
            }
            if (file_cfg.getBool("create_dirs")) |v| {
                config.cold_storage.file.create_dirs = v;
            }
        }

        // Parse [cold_storage.s3] section
        if (cold.getTable("s3")) |s3_cfg| {
            if (s3_cfg.getString("bucket")) |b| {
                config.cold_storage.s3.bucket = try config.dupeString(b);
            }
            if (s3_cfg.getString("region")) |r| {
                config.cold_storage.s3.region = try config.dupeString(r);
            }
            if (s3_cfg.getString("endpoint")) |e| {
                config.cold_storage.s3.endpoint = try config.dupeString(e);
            }
            if (s3_cfg.getBool("use_path_style")) |v| {
                config.cold_storage.s3.use_path_style = v;
            }
            if (s3_cfg.getBool("use_tls")) |v| {
                config.cold_storage.s3.use_tls = v;
            }
            // Note: credentials should come from env vars or IAM role, not config file
        }
    }

    return config;
}

/// Load config with CLI flag overrides
pub fn loadWithOverrides(
    allocator: Allocator,
    config_path: ?[]const u8,
    port_override: ?u16,
    data_dir_override: ?[]const u8,
    shards_override: ?u16,
    partition_count_override: ?u32,
    log_level_override: ?[]const u8,
    log_format_override: ?[]const u8,
) !ServerConfig {
    // Load base config
    var config = if (config_path) |path|
        try load(allocator, path)
    else blk: {
        // Try default paths
        const cfg = load(allocator, "flo.toml") catch |err| {
            if (err == error.FileNotFound) {
                break :blk ServerConfig.init(allocator);
            }
            return err;
        };
        break :blk cfg;
    };
    errdefer config.deinit();

    // Apply CLI overrides
    if (port_override) |p| {
        config.port = p;
    }
    if (data_dir_override) |d| {
        config.data_dir = try config.dupeString(d);
    }
    if (shards_override) |s| {
        config.shards = s;
    }
    if (partition_count_override) |pc| {
        config.partition_count = pc;
    }
    if (log_level_override) |level| {
        config.log_level = ServerConfig.LogLevel.fromString(level);
    }
    if (log_format_override) |format| {
        config.log_format = ServerConfig.LogFormat.fromString(format);
    }
    // Note: durability is intentionally NOT overridable via CLI
    // It must be set in flo.toml to prevent accidental data loss

    return config;
}

/// Generate default flo.toml content
pub fn generateDefaultConfig() []const u8 {
    return 
    \\# Flo Server Configuration
    \\# See documentation at https://github.com/floruntime/flo
    \\
    \\[server]
    \\# TCP port for client connections (supports both binary protocol and WebSocket)
    \\port = 9000
    \\
    \\# Bind address (0.0.0.0 for all interfaces)
    \\bind = "0.0.0.0"
    \\
    \\# Data directory for WAL and storage files
    \\data_dir = "~/.flo/data"
    \\
    \\# Number of data shards (partitions)
    \\# CRITICAL: This defines the on-disk data layout, NOT just CPU usage.
    \\# Set to 0 to auto-detect CPU count on first run (recommended).
    \\# WARNING: Cannot be changed after data exists without rebalancing!
    \\# Keys are hashed to shards - mismatch = data loss.
    \\shards = 0
    \\
    \\# Number of virtual partitions for two-level routing (0=auto: max(4096, shards × 32))
    \\# Partitions are the unit of data ownership and rebalancing.
    \\# Must be >= shards. A higher count enables finer-grained rebalancing.
    \\# WARNING: Cannot be changed after data exists without rebalancing!
    \\# partition_count = 0
    \\
    \\[storage]
    \\# --- WAL Durability ---
    \\# sync: Wait for fsync after every write (strongest guarantee)
    \\# async_flush: Background flush every ~1ms (highest throughput, default)
    \\# ephemeral: Skip WAL entirely (for caches/temporary data)
    \\durability = "async_flush"
    \\
    \\# --- Hot Tier (RAM Ring Buffer) ---
    \\# Per-partition mmap'd ring buffer capacity in bytes (default: 64MB)
    \\hot_buffer_capacity = 67108864
    \\
    \\# Max seconds entries stay in hot tier before flush to warm (0 = disabled)
    \\# Recommended production value: 300 (5 minutes)
    \\# hot_flush_seconds = 300
    \\
    \\# Max entries in hot tier before eviction (0 = rely on buffer capacity only)
    \\# Non-zero values useful for testing deterministic spill behavior
    \\# max_hot_entries = 0
    \\
    \\# --- Warm Tier (Disk Segments) ---
    \\# Max local segments before archival to cold tier
    \\# max_local_segments = 100
    \\
    \\# Truncate WAL entries after safe segment flush
    \\# enable_wal_truncation = true
    \\
    \\[background_tasks]
    \\# Namespace deletion task interval in milliseconds
    \\# How often to check for namespaces pending deletion (force delete cleanup)
    \\# Default: 5000ms (5s) for production workloads
    \\# namespace_deletion_interval_ms = 5000
    \\
    \\[logging]
    \\# Log level: debug, info, warn, error
    \\level = "info"
    \\
    \\[auth]
    \\# Enable JWT authentication for client connections
    \\# When disabled, all connections are anonymous (no auth required)
    \\enabled = false
    \\
    \\# JWT secret for HS256 signature verification
    \\# Generate a secure secret: openssl rand -base64 32
    \\# jwt_secret = "your-256-bit-secret-key-here"
    \\
    \\# JWKS URL for RS256 signature verification (not yet implemented)
    \\# jwks_url = "https://your-auth-server/.well-known/jwks.json"
    \\
    \\[websocket]
    \\# Rate limiting for WebSocket connections (browser clients)
    \\# Max requests per window (0 = unlimited)
    \\rate_limit_requests = 1000
    \\# Rate limit window size in milliseconds
    \\rate_limit_window_ms = 1000
    \\
    \\# Heartbeat settings
    \\# Ping interval in milliseconds (0 = disabled)
    \\ping_interval_ms = 30000
    \\# Pong timeout - connection closed if no pong received within this time
    \\pong_timeout_ms = 10000
    \\
    \\[metrics]
    \\# Enable HTTP metrics endpoint for Prometheus scraping
    \\enabled = true
    \\# Port for metrics HTTP server (0 = auto: listen_port + 1)
    \\# port = 9001
    \\# Bind address for metrics server
    \\# bind = "0.0.0.0"
    \\
    \\[dashboard]
    \\# Enable the web dashboard for monitoring and management
    \\enabled = true
    \\# Port for dashboard HTTP server (0 = auto: listen_port + 2)
    \\# port = 9002
    \\# Bind address for dashboard server
    \\# bind = "0.0.0.0"
    \\# CORS origins for development (comma-separated)
    \\# cors_origins = "http://localhost:5173"
    \\
    \\[cluster]
    \\# Enable distributed cluster mode with Raft consensus
    \\# When disabled, runs in standalone mode (single node)
    \\enabled = false
    \\
    \\# Unique node ID within the cluster (must be unique per node)
    \\# node_id = 1
    \\
    \\# Port for Raft RPC communication between cluster nodes (0 = auto: listen_port + 500)
    \\# raft_port = 9500
    \\
    \\# Seed nodes for cluster discovery (comma-separated)
    \\# Format: "host1:raft_port1,host2:raft_port2,host3:raft_port3"
    \\# seeds = "192.168.1.10:9500,192.168.1.11:9500,192.168.1.12:9500"
    \\
    \\# Replication factor (number of copies of each partition)
    \\# replication_factor = 3
    \\
    \\# Raft election timeout range in milliseconds
    \\# election_timeout_min_ms = 150
    \\# election_timeout_max_ms = 300
    \\
    \\# Raft heartbeat interval in milliseconds
    \\# heartbeat_interval_ms = 50
    \\
    \\[cold_storage]
    \\# Cold tier backend: none, file, s3
    \\# provider = "none"
    \\
    \\# Number of upload/restore workers
    \\# upload_workers = 2
    \\# restore_workers = 4
    \\
    \\# File backend configuration (local filesystem or NFS)
    \\# [cold_storage.file]
    \\# base_path = "/var/lib/flo/archive"
    \\# sync_on_write = true
    \\
    \\# S3 backend configuration (AWS S3 or S3-compatible like MinIO, R2)
    \\# [cold_storage.s3]
    \\# bucket = "my-flo-cold-storage"
    \\# region = "us-east-1"
    \\# endpoint = ""           # Optional: for S3-compatible services
    \\# use_path_style = false  # Set true for MinIO
    \\# use_tls = true
    \\
    ;
}

// Tests
test "load default config" {
    const allocator = std.testing.allocator;
    var config = ServerConfig.init(allocator);
    defer config.deinit();

    try std.testing.expectEqual(@as(u16, 9000), config.port);
    try std.testing.expectEqualStrings("0.0.0.0", config.bind);
    try std.testing.expectEqualStrings("~/.flo/data", config.data_dir);
    try std.testing.expectEqual(@as(u16, 0), config.shards); // 0 = auto-detect
}

test "toRuntimeConfig conversion" {
    const allocator = std.testing.allocator;
    var config = ServerConfig.init(allocator);
    defer config.deinit();

    config.port = 9001;
    config.shards = 4;

    const runtime_config = config.toRuntimeConfig();
    try std.testing.expectEqual(@as(u16, 9001), runtime_config.listen_port);
    try std.testing.expectEqual(@as(u16, 4), runtime_config.num_shards);
}
