//! Node runtime — stub for ADAPT file compatibility
//!
//! Provides RuntimeConfig (consumed by config/server.zig) and Runtime
//! (consumed by cli/commands/server.zig). These will be replaced by the
//! real runtime implementation in Phase 1.9.

const std = @import("std");
const Durability = @import("../engine/interfaces.zig").Durability;
const ColdStorageConfig = @import("../config/cold_storage.zig").ColdStorageConfig;
const TieredLogConfig = @import("../config/tiered_log.zig").TieredLogConfig;

/// Runtime configuration produced by ServerConfig.toRuntimeConfig()
pub const RuntimeConfig = struct {
    num_shards: u16 = 0,
    partition_count: u32 = 0,
    data_dir: []const u8 = "~/.flo/data",
    listen_port: u16 = 9000,
    listen_addr: []const u8 = "0.0.0.0",
    hot_window_seconds: u64 = 90,
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
    metrics_port: u16 = 9001,
    dashboard_enabled: bool = true,
    dashboard_port: u16 = 9002,
    dashboard_bind: []const u8 = "0.0.0.0",
    dashboard_cors_origins: ?[]const u8 = null,
    dashboard_admin_token: ?[]const u8 = null,
    cluster_node_id: u32 = 0,
    cluster_raft_port: u16 = 9500,
    cluster_gossip_port: u16 = 9600,
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
};

/// Node runtime — manages shard threads and the acceptor.
/// Stub: will be fully implemented in Phase 1.9.
pub const Runtime = struct {
    allocator: std.mem.Allocator,
    config: RuntimeConfig,

    pub fn init(allocator: std.mem.Allocator, config: RuntimeConfig) !Runtime {
        return .{ .allocator = allocator, .config = config };
    }

    pub fn deinit(self: *Runtime) void {
        _ = self;
    }

    pub fn start(self: *Runtime) !void {
        _ = self;
    }

    pub fn stop(self: *Runtime) void {
        _ = self;
    }
};
