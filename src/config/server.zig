//! Server configuration loader for flo.toml
//! Parses TOML config and converts to RuntimeConfig

const std = @import("std");
const log = @import("stdx").log;
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

    /// Parse durability from CLI input; rejects unknown values.
    pub fn parseDurability(s: []const u8) error{InvalidDurability}!Durability {
        if (std.mem.eql(u8, s, "sync")) return .sync;
        if (std.mem.eql(u8, s, "async_flush")) return .async_flush;
        if (std.mem.eql(u8, s, "ephemeral")) return .ephemeral;
        return error.InvalidDurability;
    }
};

const metrics_config = @import("metrics.zig");
pub const MetricsConfig = metrics_config.MetricsConfig;

const dashboard_config = @import("dashboard.zig");
pub const DashboardConfig = dashboard_config.DashboardConfig;

const cluster_config = @import("cluster.zig");
pub const ClusterConfig = cluster_config.ClusterConfig;

const tiered_log_config = @import("tiered_log.zig");
pub const TieredLogConfig = tiered_log_config.TieredLogConfig;

/// Maximum shards per node: a cross-shard message names its sender in one byte.
pub const MAX_SHARDS: u16 = 256;

/// Maximum partitions: a run id carries its partition in 14 bits, and one
/// past that would route its status and signals to another partition.
pub const MAX_PARTITIONS: u32 = @import("../node/run_id.zig").MAX_PARTITION + 1;

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

    // Tier settings (parsed from [storage] section)
    tiered_log: TieredLogConfig = .{},

    // [metrics] section
    metrics: MetricsConfig = .{},

    // [dashboard] section
    dashboard: DashboardConfig = .{},

    // [cluster] section
    cluster: ClusterConfig = .{},

    // [logging] section
    log_level: LogLevel = .info,
    log_format: LogFormat = .text,

    // Memory management
    allocator: Allocator,
    _owned_strings: std.ArrayListUnmanaged([]const u8) = .empty,

    pub const LogLevel = enum {
        debug,
        info,
        warn,
        err,

        pub fn fromString(s: []const u8) ?LogLevel {
            if (std.mem.eql(u8, s, "debug")) return .debug;
            if (std.mem.eql(u8, s, "info")) return .info;
            if (std.mem.eql(u8, s, "warn") or std.mem.eql(u8, s, "warning")) return .warn;
            if (std.mem.eql(u8, s, "error") or std.mem.eql(u8, s, "err")) return .err;
            return null;
        }
    };

    pub const LogFormat = enum {
        text,
        json,

        pub fn fromString(s: []const u8) ?LogFormat {
            if (std.mem.eql(u8, s, "json")) return .json;
            if (std.mem.eql(u8, s, "text")) return .text;
            return null;
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
        return RuntimeConfig{
            .num_shards = self.shards,
            .partition_count = self.partition_count,
            .data_dir = self.data_dir,
            .listen_port = self.port,
            .listen_addr = self.bind,
            .durability = self.durability,
            .tiered_log = self.tiered_log,
            .metrics_enabled = self.metrics.enabled,
            .metrics_port = self.metrics.port,
            .metrics_bind = self.metrics.bind,
            .dashboard_enabled = self.dashboard.enabled,
            .dashboard_port = self.dashboard.port,
            .dashboard_bind = self.dashboard.bind,
            .dashboard_cors_origins = self.dashboard.cors_origins,
            .dashboard_hosts = self.dashboard.hosts,
            .cluster_enabled = self.cluster.enabled,
            .cluster_node_id = self.cluster.node_id,
            .cluster_raft_port = self.cluster.raft_port,
            .cluster_seeds = self.cluster.seeds,
            .cluster_secret = self.cluster.secret,
            .cluster_failover_timeout_ms = self.cluster.failover_timeout_ms,
        };
    }

    pub fn dupeString(self: *ServerConfig, s: []const u8) ![]const u8 {
        const owned = try self.allocator.dupe(u8, s);
        try self._owned_strings.append(self.allocator, owned);
        return owned;
    }
};

const Kind = enum { string, integer, boolean, string_or_array };
const Key = struct {
    name: []const u8,
    kind: Kind,
    /// Integer bounds checked before load reads the value.
    min: i64 = 0,
    max: i64 = std.math.maxInt(i64),
};
const Section = struct { name: []const u8, keys: []const Key };

const PORT_MAX: i64 = std.math.maxInt(u16);

/// Every section and key flo.toml may hold. Anything else is refused at
/// start, not ignored: a line that parses clean and changes nothing is the
/// bug an operator finds at 2am. A value of the wrong type or out of range is
/// refused for the same reason.
const sections = [_]Section{
    .{
        .name = "server",
        .keys = &.{
            .{ .name = "port", .kind = .integer, .max = PORT_MAX },
            .{ .name = "bind", .kind = .string },
            .{ .name = "data_dir", .kind = .string },
            // Range-checked by load, which names the automatic value.
            .{ .name = "shards", .kind = .integer, .min = std.math.minInt(i64) },
            .{ .name = "partition_count", .kind = .integer, .min = std.math.minInt(i64) },
        },
    },
    .{ .name = "storage", .keys = &.{
        .{ .name = "durability", .kind = .string },
        .{ .name = "hot_buffer_capacity", .kind = .integer, .min = 1 },
        .{ .name = "max_hot_entries", .kind = .integer },
        .{ .name = "hot_flush_seconds", .kind = .integer },
    } },
    .{ .name = "logging", .keys = &.{
        .{ .name = "level", .kind = .string },
        .{ .name = "format", .kind = .string },
    } },
    .{ .name = "metrics", .keys = &.{
        .{ .name = "enabled", .kind = .boolean },
        .{ .name = "port", .kind = .integer, .max = PORT_MAX },
        .{ .name = "bind", .kind = .string },
    } },
    .{ .name = "dashboard", .keys = &.{
        .{ .name = "enabled", .kind = .boolean },
        .{ .name = "port", .kind = .integer, .max = PORT_MAX },
        .{ .name = "bind", .kind = .string },
        .{ .name = "cors_origins", .kind = .string },
        .{ .name = "hosts", .kind = .string },
    } },
    .{ .name = "cluster", .keys = &.{
        .{ .name = "enabled", .kind = .boolean },
        .{ .name = "secret", .kind = .string },
        .{ .name = "secret_file", .kind = .string },
        .{ .name = "node_id", .kind = .integer, .max = std.math.maxInt(u32) },
        .{ .name = "raft_port", .kind = .integer, .max = PORT_MAX },
        .{ .name = "seeds", .kind = .string_or_array },
        .{ .name = "failover_timeout_ms", .kind = .integer },
    } },
};

/// Sections and keys that are refused with a reason rather than the list of
/// what is valid: they were removed, or nothing reads them yet.
const removed = [_]struct { name: []const u8, why: []const u8 }{
    .{ .name = "auth", .why = "[auth] was removed: clients aren't authenticated yet. Remove the section, and keep the client port on a private interface." },
    .{ .name = "websocket", .why = "[websocket] was removed along with the WebSocket endpoint. Remove the section." },
    .{ .name = "cold_storage", .why = "[cold_storage] isn't wired up: nothing reads it, so no data would move to cold storage. Remove the section." },
    .{ .name = "background_tasks", .why = "[background_tasks] was removed: namespace delete is refused, so there is no deletion task to tune. Remove the section." },
    .{ .name = "kv", .why = "[kv] expose_internal_keys was never read: internal keys stay hidden. Remove the section." },
    .{ .name = "storage.max_local_segments", .why = "[storage] max_local_segments isn't wired up: nothing reads it. Remove the line." },
    .{ .name = "storage.enable_wal_truncation", .why = "[storage] enable_wal_truncation isn't wired up: nothing reads it. Remove the line." },
};

fn removedWhy(name: []const u8) ?[]const u8 {
    for (removed) |r| if (std.mem.eql(u8, r.name, name)) return r.why;
    return null;
}

fn kindOf(v: toml.Value) ?Kind {
    return switch (v) {
        .string => .string,
        .integer => .integer,
        .boolean => .boolean,
        .table => null,
        .array => .string_or_array,
    };
}

fn kindName(k: Kind) []const u8 {
    return switch (k) {
        .string => "a string",
        .integer => "an integer",
        .boolean => "true or false",
        .string_or_array => "a string or a list of strings",
    };
}

fn findSection(name: []const u8) ?*const Section {
    for (&sections) |*sec| if (std.mem.eql(u8, sec.name, name)) return sec;
    return null;
}

/// Refuse anything in `root` that `sections` doesn't name, or names with
/// another type or out of range.
fn checkSchema(root: *const toml.Table) error{ UnknownSetting, InvalidSetting }!void {
    var it = root.entries.iterator();
    while (it.next()) |e| {
        const name = e.key_ptr.*;
        if (removedWhy(name)) |why| {
            log.err("{s}", .{why});
            return error.UnknownSetting;
        }
        if (e.value_ptr.* != .table) {
            log.err("{s} is outside any section; settings go under [server], [storage] and the like. Move or remove the line.", .{name});
            return error.UnknownSetting;
        }
        const sec = findSection(name) orelse {
            log.err("[{s}] is not a section; the sections are {s}. Remove it.", .{ name, sectionList() });
            return error.UnknownSetting;
        };
        try checkSection(sec, &e.value_ptr.table);
    }
}

fn checkSection(sec: *const Section, table: *const toml.Table) error{ UnknownSetting, InvalidSetting }!void {
    var it = table.entries.iterator();
    while (it.next()) |e| {
        var buf: [128]u8 = undefined;
        const path = std.fmt.bufPrint(&buf, "{s}.{s}", .{ sec.name, e.key_ptr.* }) catch "";
        if (removedWhy(path)) |why| {
            log.err("{s}", .{why});
            return error.UnknownSetting;
        }
        const key = for (sec.keys) |k| {
            if (std.mem.eql(u8, k.name, e.key_ptr.*)) break k;
        } else {
            log.err("[{s}] {s} is not a setting; the keys are {s}. Remove the line.", .{ sec.name, e.key_ptr.*, keyList(sec) });
            return error.UnknownSetting;
        };
        const got = kindOf(e.value_ptr.*);
        var fits = got == key.kind or (key.kind == .string_or_array and got == .string);
        // A list must hold strings only: anything else would be skipped.
        if (fits and e.value_ptr.* == .array) {
            for (e.value_ptr.array) |item| {
                if (item != .string) fits = false;
            }
        }
        if (!fits) {
            log.err("[{s}] {s} must be {s}", .{ sec.name, key.name, kindName(key.kind) });
            return error.InvalidSetting;
        }
        if (key.kind == .integer) {
            const v = e.value_ptr.integer;
            if (v < key.min or v > key.max) {
                log.err("[{s}] {s} = {d}: must be {d} to {d}", .{ sec.name, key.name, v, key.min, key.max });
                return error.InvalidSetting;
            }
        }
    }
}

/// The sections' names as "a, b and c", for messages.
fn sectionList() []const u8 {
    const S = struct {
        threadlocal var buf: [256]u8 = undefined;
    };
    var w: std.Io.Writer = .fixed(&S.buf);
    for (sections, 0..) |sec, i| {
        const sep = if (i == 0) "" else if (i + 1 == sections.len) " and " else ", ";
        w.print("{s}{s}", .{ sep, sec.name }) catch break;
    }
    return w.buffered();
}

/// "a, b and c", for messages. Sections are short enough for a fixed buffer.
fn keyList(sec: *const Section) []const u8 {
    const S = struct {
        threadlocal var buf: [512]u8 = undefined;
    };
    var w: std.Io.Writer = .fixed(&S.buf);
    for (sec.keys, 0..) |k, i| {
        const sep = if (i == 0) "" else if (i + 1 == sec.keys.len) " and " else ", ";
        w.print("{s}{s}", .{ sep, k.name }) catch break;
    }
    return w.buffered();
}

/// Load server configuration from flo.toml file
pub fn load(allocator: Allocator, path: []const u8) !ServerConfig {
    var config = ServerConfig.init(allocator);
    errdefer config.deinit();

    var table = try toml.parseFile(allocator, path);
    defer table.deinit();
    try checkSchema(&table);

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
            if (s < 0 or s > MAX_SHARDS) {
                log.err("[server] shards = {d}: must be 0 (automatic) to {d}", .{ s, MAX_SHARDS });
                return error.InvalidShardCount;
            }
            config.shards = @intCast(s);
        }
        if (server.getInt("partition_count")) |pc| {
            if (pc < 0 or pc > MAX_PARTITIONS) {
                log.err("[server] partition_count = {d}: must be 0 (automatic) to {d}", .{ pc, MAX_PARTITIONS });
                return error.InvalidPartitionCount;
            }
            config.partition_count = @intCast(pc);
        }
    }

    // Parse [storage] section — unified tier configuration
    if (table.getTable("storage")) |storage| {
        // NOTE: memtable_size_mb parsing removed - no SpilloverEngine
        if (storage.getString("durability")) |d| {
            config.durability = Durability.parseDurability(d) catch {
                log.err("[storage] durability = \"{s}\": use sync, async_flush or ephemeral", .{d});
                return error.InvalidSetting;
            };
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
    }

    // Parse [logging] section
    if (table.getTable("logging")) |logging| {
        if (logging.getString("level")) |level| {
            config.log_level = ServerConfig.LogLevel.fromString(level) orelse {
                log.err("[logging] level = \"{s}\": use debug, info, warn or error", .{level});
                return error.InvalidSetting;
            };
        }
        if (logging.getString("format")) |format| {
            config.log_format = ServerConfig.LogFormat.fromString(format) orelse {
                log.err("[logging] format = \"{s}\": use text or json", .{format});
                return error.InvalidSetting;
            };
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
            if (dashboard_config.originsRefusal(c)) |why| {
                log.err("[dashboard] cors_origins: {s} (e.g. \"https://ops.example.com:8443\")", .{why});
                return error.InvalidSetting;
            }
            config.dashboard.cors_origins = try config.dupeString(c);
        }
        if (d.getString("hosts")) |hs| {
            if (dashboard_config.hostsRefusal(hs)) |why| {
                log.err("[dashboard] hosts: {s} (e.g. \"flo.internal, 10.0.1.5\")", .{why});
                return error.InvalidSetting;
            }
            config.dashboard.hosts = try config.dupeString(hs);
        }
    }

    if (table.getTable("cluster")) |c| {
        config.cluster = try cluster_config.parseClusterConfig(allocator, c, &config._owned_strings);
    }

    return config;
}

/// Load config with CLI flag overrides
pub fn loadWithOverrides(
    allocator: Allocator,
    config_path: ?[]const u8,
    port_override: ?u16,
    data_dir_override: ?[]const u8,
    shards_override: ?u32,
    partition_count_override: ?u32,
    log_level_override: ?[]const u8,
    log_format_override: ?[]const u8,
    durability_override: ?[]const u8,
) !ServerConfig {
    // Load base config
    var config = if (config_path) |path|
        load(allocator, path) catch |err| {
            // A file named on the command line is meant to be read; starting
            // on defaults instead would hide the typo.
            if (err == error.FileNotFound) log.err("config file {s} not found; check the path, or omit --config to use ./flo.toml or the defaults", .{path});
            return err;
        }
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
        if (s > MAX_SHARDS) {
            log.err("--shards {d}: must be 0 (automatic) to {d}", .{ s, MAX_SHARDS });
            return error.InvalidShardCount;
        }
        config.shards = @intCast(s);
    }
    if (partition_count_override) |pc| {
        if (pc > MAX_PARTITIONS) {
            log.err("--partitions {d}: must be 0 (automatic) to {d}", .{ pc, MAX_PARTITIONS });
            return error.InvalidPartitionCount;
        }
        config.partition_count = pc;
    }
    if (log_level_override) |level| {
        config.log_level = ServerConfig.LogLevel.fromString(level) orelse {
            log.err("--log-level {s}: use debug, info, warn or error", .{level});
            return error.InvalidSetting;
        };
    }
    if (log_format_override) |format| {
        config.log_format = ServerConfig.LogFormat.fromString(format) orelse {
            log.err("--log-format {s}: use text or json", .{format});
            return error.InvalidSetting;
        };
    }
    // Durability override via CLI is permitted but warned on downgrade.
    // Historical note: durability was originally TOML-only to prevent an
    // operator from accidentally starting with --durability ephemeral and
    // silently losing data. Allowing the flag preserves test ergonomics and
    // explicit overrides, but a warning is emitted when the CLI value is
    // less durable than what the file says, so the choice is never silent.
    if (durability_override) |d| {
        if (d.len > 0) {
            const requested = try Durability.parseDurability(d);
            if (@intFromEnum(requested) > @intFromEnum(config.durability)) {
                std.debug.print(
                    "warning: --durability {s} is LESS durable than configured ({s}); data loss possible on restart\n",
                    .{ d, @tagName(config.durability) },
                );
            }
            config.durability = requested;
        }
    }

    return config;
}

/// Generate default flo.toml content
pub fn generateDefaultConfig() []const u8 {
    return
    \\# Flo Server Configuration
    \\# See documentation at https://github.com/floruntime/flo
    \\
    \\[server]
    \\# TCP port for client connections (binary protocol)
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
    \\[logging]
    \\# Log level: debug, info, warn, error
    \\level = "info"
    \\
    \\[metrics]
    \\# Enable HTTP metrics endpoint for Prometheus scraping
    \\enabled = true
    \\# Port for metrics HTTP server (0 = auto: listen_port + 1)
    \\# port = 9001
    \\# Bind address for metrics server. Loopback by default — /metrics and
    \\# /health are unauthenticated. Set "0.0.0.0" to allow remote scraping.
    \\# bind = "127.0.0.1"
    \\
    \\[dashboard]
    \\# Enable the web dashboard for monitoring and management
    \\enabled = true
    \\# Port for dashboard HTTP server (0 = auto: listen_port + 2)
    \\# port = 9002
    \\# Bind address for dashboard server
    \\# bind = "0.0.0.0"
    \\# Other origins whose pages may call the API, exact, comma-separated
    \\# cors_origins = "http://localhost:5173"
    \\# Host names the dashboard answers to besides localhost, 127.0.0.1 and
    \\# [::1], comma-separated: needed to reach it by any other name
    \\# hosts = "flo.internal"
    \\
    \\[cluster]
    \\# true starts this node as the first member of a cluster (same as
    \\# --cluster); false with no seeds runs a single node.
    \\enabled = false
    \\
    \\# This node's id, unique in the cluster. Read on first boot and then stored
    \\# with the data; to change it, start from an empty data dir.
    \\# node_id = 1
    \\
    \\# Peer port: Raft RPCs and forwarded writes between members (0 = auto: listen_port + 500)
    \\# raft_port = 9500
    \\
    \\# Peer ports of members to join (same as --join); a node with seeds joins
    \\# them, so it does not also set enabled = true.
    \\# seeds = ["192.168.1.10:9500", "192.168.1.11:9500"]
    \\
    \\# Shared secret every member must hold; required whenever the peer
    \\# listener starts (--cluster, --join, or seeds here). Only one made by
    \\# `flo server secret` is accepted. Or keep it in a file only its owner
    \\# can read (chmod 600) and name the file instead.
    \\# secret = "flo-secret-..."
    \\# secret_file = "/etc/flo/cluster.secret"
    \\
    \\# Replacing a leader that has gone quiet begins after half of this and
    \\# is certain by all of it; heartbeats are a sixth of it. Minimum 100.
    \\# failover_timeout_ms = 1500
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

test "Durability.parseDurability accepts known values" {
    try std.testing.expectEqual(Durability.sync, try Durability.parseDurability("sync"));
    try std.testing.expectEqual(Durability.async_flush, try Durability.parseDurability("async_flush"));
    try std.testing.expectEqual(Durability.ephemeral, try Durability.parseDurability("ephemeral"));
    try std.testing.expectError(error.InvalidDurability, Durability.parseDurability("invalid"));
}

test "loadWithOverrides applies durability flag" {
    const allocator = std.testing.allocator;
    var config = try loadWithOverrides(allocator, null, null, null, null, null, null, null, "sync");
    defer config.deinit();
    try std.testing.expectEqual(Durability.sync, config.durability);
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

test "a shard or partition count out of range is refused, not cast, from the file or the command line" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try @import("stdx").fs.dirRealpathAlloc(tmp.dir, allocator, ".");
    defer allocator.free(dir);
    for ([_][]const u8{ "[server]\nshards = 257\n", "[server]\nshards = -1\n", "[server]\nshards = 70000\n" }) |body| {
        const path = try std.fmt.allocPrint(allocator, "{s}/flo.toml", .{dir});
        defer allocator.free(path);
        const f = try @import("stdx").fs.createFileAbsolute(path, .{ .truncate = true });
        try @import("stdx").fs.writeAll(f, body);
        @import("stdx").fs.closeFile(f);
        try std.testing.expectError(error.InvalidShardCount, load(allocator, path));
    }
    const path = try std.fmt.allocPrint(allocator, "{s}/flo.toml", .{dir});
    defer allocator.free(path);
    const f = try @import("stdx").fs.createFileAbsolute(path, .{ .truncate = true });
    try @import("stdx").fs.writeAll(f, "[server]\npartition_count = 16385\n");
    @import("stdx").fs.closeFile(f);
    try std.testing.expectError(error.InvalidPartitionCount, load(allocator, path));

    const g = try @import("stdx").fs.createFileAbsolute(path, .{ .truncate = true });
    @import("stdx").fs.closeFile(g);
    try std.testing.expectError(error.InvalidShardCount, loadWithOverrides(allocator, path, null, null, 70000, null, null, null, null));
    try std.testing.expectError(error.InvalidPartitionCount, loadWithOverrides(allocator, path, null, null, null, MAX_PARTITIONS + 1, null, null, null));
    var config = try loadWithOverrides(allocator, path, null, null, MAX_SHARDS, null, null, null, null);
    defer config.deinit();
    try std.testing.expectEqual(MAX_SHARDS, config.shards);
    var most = try loadWithOverrides(allocator, path, null, null, null, MAX_PARTITIONS, null, null, null);
    defer most.deinit();
    try std.testing.expectEqual(MAX_PARTITIONS, most.partition_count);
}

test "dashboard origins and hosts that could never match are refused at load" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try @import("stdx").fs.dirRealpathAlloc(tmp.dir, allocator, ".");
    defer allocator.free(dir);
    const path = try std.fmt.allocPrint(allocator, "{s}/flo.toml", .{dir});
    defer allocator.free(path);
    for ([_][]const u8{
        "[dashboard]\ncors_origins = \"*\"\n",
        "[dashboard]\ncors_origins = \"null\"\n",
        "[dashboard]\ncors_origins = \"https://ops.example.com/\"\n",
        "[dashboard]\nhosts = \"flo.internal:9002\"\n",
    }) |body| {
        const f = try @import("stdx").fs.createFileAbsolute(path, .{ .truncate = true });
        try @import("stdx").fs.writeAll(f, body);
        @import("stdx").fs.closeFile(f);
        try std.testing.expectError(error.InvalidSetting, load(allocator, path));
    }
}

test "config: what flo.toml can't hold is refused, not ignored" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try @import("stdx").fs.dirRealpathAlloc(tmp.dir, allocator, ".");
    defer allocator.free(dir);
    const path = try std.fmt.allocPrint(allocator, "{s}/flo.toml", .{dir});
    defer allocator.free(path);
    const Case = struct { body: []const u8, err: anyerror };
    for ([_]Case{
        .{ .body = "[server]\nprot = 9000\n", .err = error.UnknownSetting },
        .{ .body = "[servr]\nport = 9000\n", .err = error.UnknownSetting },
        .{ .body = "port = 9000\n", .err = error.UnknownSetting },
        .{ .body = "[auth]\nenabled = true\n", .err = error.UnknownSetting },
        .{ .body = "[websocket]\nping_interval_ms = 1\n", .err = error.UnknownSetting },
        .{ .body = "[cold_storage]\nprovider = \"file\"\n", .err = error.UnknownSetting },
        .{ .body = "[background_tasks]\nnamespace_deletion_interval_ms = 5\n", .err = error.UnknownSetting },
        .{ .body = "[kv]\nexpose_internal_keys = true\n", .err = error.UnknownSetting },
        .{ .body = "[storage]\nmax_local_segments = 5\n", .err = error.UnknownSetting },
        .{ .body = "[cluster]\nelection_timeout_min_ms = 150\n", .err = error.UnknownSetting },
        .{ .body = "[server]\nport = \"9000\"\n", .err = error.InvalidSetting },
        .{ .body = "[server]\nport = 70000\n", .err = error.InvalidSetting },
        .{ .body = "[metrics]\nport = -1\n", .err = error.InvalidSetting },
        .{ .body = "[storage]\nhot_buffer_capacity = 0\n", .err = error.InvalidSetting },
        .{ .body = "[storage]\ndurability = \"fast\"\n", .err = error.InvalidSetting },
        .{ .body = "[logging]\nlevel = \"loud\"\n", .err = error.InvalidSetting },
        .{ .body = "[logging]\nformat = \"xml\"\n", .err = error.InvalidSetting },
        .{ .body = "[dashboard]\nenabled = \"yes\"\n", .err = error.InvalidSetting },
        .{ .body = "[cluster]\nseeds = [9500]\n", .err = error.InvalidSetting },
    }) |c| {
        const f = try @import("stdx").fs.createFileAbsolute(path, .{ .truncate = true });
        try @import("stdx").fs.writeAll(f, c.body);
        @import("stdx").fs.closeFile(f);
        std.testing.expectError(c.err, load(allocator, path)) catch |e| {
            std.debug.print("case: {s}", .{c.body});
            return e;
        };
    }
    // A valid file, so the refusal is the flag's.
    const ok = try @import("stdx").fs.createFileAbsolute(path, .{ .truncate = true });
    try @import("stdx").fs.writeAll(ok, "[server]\nport = 9000\n");
    @import("stdx").fs.closeFile(ok);
    try std.testing.expectError(error.InvalidSetting, loadWithOverrides(allocator, path, null, null, null, null, "loud", null, null));
    try std.testing.expectError(error.InvalidSetting, loadWithOverrides(allocator, path, null, null, null, null, null, "xml", null));
}

test "config: the file config init writes loads, and a named file that is missing is refused" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try @import("stdx").fs.dirRealpathAlloc(tmp.dir, allocator, ".");
    defer allocator.free(dir);
    const path = try std.fmt.allocPrint(allocator, "{s}/flo.toml", .{dir});
    defer allocator.free(path);
    try std.testing.expectError(error.FileNotFound, loadWithOverrides(allocator, path, null, null, null, null, null, null, null));
    const f = try @import("stdx").fs.createFileAbsolute(path, .{ .truncate = true });
    try @import("stdx").fs.writeAll(f, generateDefaultConfig());
    @import("stdx").fs.closeFile(f);
    var config = try load(allocator, path);
    defer config.deinit();
}
