//! Node Runtime — boot sequence, thread management, graceful shutdown
//!
//! The Runtime is the top-level lifecycle manager for a Flo node.
//!
//! ## Boot Sequence
//!
//! 1. Detect CPU count (or use configured shard count)
//! 2. Create per-shard pipes (acceptor → shard hand-off)
//! 3. Spawn N Shard threads (CPU-pinned on Linux, QoS on macOS)
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
const Shard = @import("shard.zig").Shard;
const Acceptor = @import("acceptor.zig").Acceptor;
const Inbox = @import("inbox.zig").Inbox;
const InboxMessage = @import("inbox.zig").Message;
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
        };
    }

    pub fn deinit(self: *Runtime) void {
        if (self.started) {
            self.stop();
        }

        // Clean up pipes
        if (self.pipes) |pipes| {
            for (pipes) |pipe| {
                if (pipe[0] >= 0) std.posix.close(pipe[0]);
                if (pipe[1] >= 0) std.posix.close(pipe[1]);
            }
            self.allocator.free(pipes);
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
            pipes[i] = try std.posix.pipe();
            pipes_created += 1;
        }
        self.pipes = pipes;

        // Build write-end array for acceptor
        const write_ends = try self.allocator.alloc(i32, self.shard_count);
        for (0..self.shard_count) |i| {
            write_ends[i] = pipes[i][1];
        }
        self.pipe_write_ends = write_ends;

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
            );
            shards_created += 1;
        }
        self.shards = shards;

        // 3. Spawn shard threads
        const threads = try self.allocator.alloc(std.Thread, self.shard_count);
        self.shard_threads = threads;

        for (0..self.shard_count) |i| {
            threads[i] = try std.Thread.spawn(.{}, shardThread, .{&shards[i]});
        }

        // 4. Create and start acceptor
        var acceptor = Acceptor.init(write_ends, shards[0].router);
        try acceptor.listen(self.config.listen_port);
        self.acceptor = acceptor;

        // 5. Spawn acceptor thread
        self.acceptor_thread = try std.Thread.spawn(.{}, acceptorThread, .{&self.acceptor.?});

        self.started = true;
    }

    /// Graceful shutdown in reverse order.
    pub fn stop(self: *Runtime) void {
        if (!self.started) return;

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
fn detectShardCount(configured: u16) u16 {
    if (configured > 0) return configured;
    const cpus = std.Thread.getCpuCount() catch 1;
    // Reserve 1 core for acceptor, 1 for OS. Min 1 shard.
    const shards = if (cpus > 2) cpus - 2 else 1;
    return @intCast(@min(shards, 256));
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