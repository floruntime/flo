//! Controller Raft — Cluster Metadata Coordinator on Shard 0
//!
//! The Controller Raft is a dedicated Raft group (group_id = 0) that manages
//! cluster-wide metadata. It runs exclusively on Shard 0 and is replicated
//! to all nodes in the cluster.
//!
//! Responsibilities:
//!   - Namespace create/delete (consistent across all shards)
//!   - Partition table management (namespace → partitions → node mapping)
//!   - Configuration changes (adding/removing nodes from the cluster)
//!
//! The Controller Raft does NOT use data projections (KV, Queue, Stream, TS).
//! Instead, committed entries update an in-memory PartitionTable and namespace
//! registry, which are broadcast to other shards via inbox messages.
//!
//! Architecture:
//!   Shard 0 Reactor → tick() every 10ms → heartbeats/elections
//!   Shard 0 Reactor → flush() periodically → replication
//!   Client proposal → propose() → Raft commit → apply() → update metadata
//!
//! Wire protocol: Uses group_id = 0 in Raft transport headers.
//! Data partition Raft groups use group_id >= 1000 (partition_id + 1000).

const std = @import("std");
const Allocator = std.mem.Allocator;
const raft_node = @import("../raft/node.zig");
const raft_log = @import("../raft/log.zig");
const entry_mod = @import("../storage/ual/entry.zig");

const RaftNode = raft_node.RaftNode;
const NodeId = raft_node.NodeId;
const RaftLog = raft_log.RaftLog;
const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const Config = raft_node.Config;
const log = @import("stdx").log;

// =============================================================================
// Constants
// =============================================================================

/// Controller Raft always uses group_id 0
pub const CONTROLLER_GROUP_ID: u32 = 0;

/// Default log capacity for the controller (smaller than data partitions)
pub const DEFAULT_LOG_CAPACITY: usize = 4096;

/// Maximum namespaces supported
pub const MAX_NAMESPACES: usize = 1024;

/// Maximum partitions per namespace
pub const MAX_PARTITIONS_PER_NAMESPACE: usize = 256;

/// Maximum nodes in the cluster
pub const MAX_NODES: usize = 64;

// =============================================================================
// Metadata Types
// =============================================================================

/// Namespace configuration — registry entry + admin-configurable settings.
/// Stored by the Controller Raft, replicated to all nodes, snapshotted.
///
/// Registry fields (name, partition_count, etc.) are set at creation time.
/// Settings fields (nullable) are admin-settable via `namespace_config_set`.
/// Null settings mean "use system default / unlimited (memory controller decides)".
/// Settings are both defaults AND ceilings: per-resource overrides can only
/// be MORE restrictive than these.
pub const NamespaceConfig = struct {
    // ── Registry fields (set at creation) ───────────────────────────────

    /// Namespace name (owned by coordinator, "" for update-only instances)
    name: []const u8 = "",
    /// Number of partitions for this namespace
    partition_count: u16 = 0,
    /// Replication factor
    replication_factor: u8 = 0,
    /// Creation timestamp (nanoseconds)
    created_at_ns: u64 = 0,
    /// Whether the namespace is being deleted (tombstone)
    deleted: bool = false,

    // ── Admin-configurable settings ─────────────────────────────────────

    /// Max version entries kept in KV projection memory per key.
    /// null = unlimited (memory controller is the backstop).
    kv_max_hot_versions: ?u32 = null,
    /// Auto-expire historical versions older than this (seconds).
    /// null = no TTL on versions.
    kv_version_ttl_s: ?u64 = null,
    /// Max stream size in bytes per stream.
    /// null = unlimited.
    stream_retention_bytes: ?u64 = null,
    /// Max age of stream messages (seconds).
    /// null = no time-based retention.
    stream_retention_s: ?u64 = null,
    /// Dead-letter queue capacity per queue.
    /// null = system default (1000).
    queue_max_dlq_size: ?u32 = null,
    /// Max lease hold time for queue messages (seconds).
    /// null = system default (30s).
    queue_max_lease_s: ?u32 = null,
    /// Memory budget for this namespace (bytes).
    /// null = fair share of shard budget.
    memory_budget_bytes: ?u64 = null,

    // ── Settings TLV serialization ──────────────────────────────────────

    /// Setting tags for TLV serialization of configurable fields.
    pub const SettingsTag = enum(u8) {
        end = 0,
        kv_max_hot_versions = 1,
        kv_version_ttl_s = 2,
        stream_retention_bytes = 3,
        stream_retention_s = 4,
        queue_max_dlq_size = 5,
        queue_max_lease_s = 6,
        memory_budget_bytes = 7,
    };

    /// Maximum serialized size of settings TLV: 1 (count) + 7 * (1 tag + 8 max value) = 64 bytes
    pub const MAX_SETTINGS_SIZE: usize = 1 + 7 * 9;

    /// Returns true if all configurable settings are null (no overrides).
    pub fn settingsEmpty(self: NamespaceConfig) bool {
        return self.kv_max_hot_versions == null and
            self.kv_version_ttl_s == null and
            self.stream_retention_bytes == null and
            self.stream_retention_s == null and
            self.queue_max_dlq_size == null and
            self.queue_max_lease_s == null and
            self.memory_budget_bytes == null;
    }

    /// Merge configurable settings from `other` into self.
    /// Non-null fields in `other` overwrite self.
    pub fn mergeSettings(self: *NamespaceConfig, other: NamespaceConfig) void {
        if (other.kv_max_hot_versions) |v| self.kv_max_hot_versions = v;
        if (other.kv_version_ttl_s) |v| self.kv_version_ttl_s = v;
        if (other.stream_retention_bytes) |v| self.stream_retention_bytes = v;
        if (other.stream_retention_s) |v| self.stream_retention_s = v;
        if (other.queue_max_dlq_size) |v| self.queue_max_dlq_size = v;
        if (other.queue_max_lease_s) |v| self.queue_max_lease_s = v;
        if (other.memory_budget_bytes) |v| self.memory_budget_bytes = v;
    }

    /// Serialize configurable settings to TLV format: [count:u8] ([tag:u8][value:u32/u64])*
    /// Returns number of bytes written.
    pub fn serializeSettings(self: NamespaceConfig, buf: []u8) usize {
        var count: u8 = 0;
        var pos: usize = 1; // reserve byte 0 for count

        if (self.kv_max_hot_versions) |v| {
            buf[pos] = @intFromEnum(SettingsTag.kv_max_hot_versions);
            pos += 1;
            std.mem.writeInt(u32, buf[pos..][0..4], v, .little);
            pos += 4;
            count += 1;
        }
        if (self.kv_version_ttl_s) |v| {
            buf[pos] = @intFromEnum(SettingsTag.kv_version_ttl_s);
            pos += 1;
            std.mem.writeInt(u64, buf[pos..][0..8], v, .little);
            pos += 8;
            count += 1;
        }
        if (self.stream_retention_bytes) |v| {
            buf[pos] = @intFromEnum(SettingsTag.stream_retention_bytes);
            pos += 1;
            std.mem.writeInt(u64, buf[pos..][0..8], v, .little);
            pos += 8;
            count += 1;
        }
        if (self.stream_retention_s) |v| {
            buf[pos] = @intFromEnum(SettingsTag.stream_retention_s);
            pos += 1;
            std.mem.writeInt(u64, buf[pos..][0..8], v, .little);
            pos += 8;
            count += 1;
        }
        if (self.queue_max_dlq_size) |v| {
            buf[pos] = @intFromEnum(SettingsTag.queue_max_dlq_size);
            pos += 1;
            std.mem.writeInt(u32, buf[pos..][0..4], v, .little);
            pos += 4;
            count += 1;
        }
        if (self.queue_max_lease_s) |v| {
            buf[pos] = @intFromEnum(SettingsTag.queue_max_lease_s);
            pos += 1;
            std.mem.writeInt(u32, buf[pos..][0..4], v, .little);
            pos += 4;
            count += 1;
        }
        if (self.memory_budget_bytes) |v| {
            buf[pos] = @intFromEnum(SettingsTag.memory_budget_bytes);
            pos += 1;
            std.mem.writeInt(u64, buf[pos..][0..8], v, .little);
            pos += 8;
            count += 1;
        }

        buf[0] = count;
        return pos;
    }

    /// Deserialize configurable settings from TLV format.
    /// Returns a NamespaceConfig with only settings populated and bytes consumed.
    pub fn deserializeSettings(data: []const u8) struct { config: NamespaceConfig, consumed: usize } {
        var s: NamespaceConfig = .{};
        if (data.len == 0) return .{ .config = s, .consumed = 0 };

        const count = data[0];
        var pos: usize = 1;

        for (0..count) |_| {
            if (pos >= data.len) break;
            const tag: SettingsTag = @enumFromInt(data[pos]);
            pos += 1;
            switch (tag) {
                .kv_max_hot_versions => {
                    if (pos + 4 > data.len) break;
                    s.kv_max_hot_versions = std.mem.readInt(u32, data[pos..][0..4], .little);
                    pos += 4;
                },
                .kv_version_ttl_s => {
                    if (pos + 8 > data.len) break;
                    s.kv_version_ttl_s = std.mem.readInt(u64, data[pos..][0..8], .little);
                    pos += 8;
                },
                .stream_retention_bytes => {
                    if (pos + 8 > data.len) break;
                    s.stream_retention_bytes = std.mem.readInt(u64, data[pos..][0..8], .little);
                    pos += 8;
                },
                .stream_retention_s => {
                    if (pos + 8 > data.len) break;
                    s.stream_retention_s = std.mem.readInt(u64, data[pos..][0..8], .little);
                    pos += 8;
                },
                .queue_max_dlq_size => {
                    if (pos + 4 > data.len) break;
                    s.queue_max_dlq_size = std.mem.readInt(u32, data[pos..][0..4], .little);
                    pos += 4;
                },
                .queue_max_lease_s => {
                    if (pos + 4 > data.len) break;
                    s.queue_max_lease_s = std.mem.readInt(u32, data[pos..][0..4], .little);
                    pos += 4;
                },
                .memory_budget_bytes => {
                    if (pos + 8 > data.len) break;
                    s.memory_budget_bytes = std.mem.readInt(u64, data[pos..][0..8], .little);
                    pos += 8;
                },
                .end => break,
            }
        }

        return .{ .config = s, .consumed = pos };
    }
};

/// A partition assignment: which node owns which partition
pub const PartitionAssignment = struct {
    namespace_hash: u32,
    partition_id: u16,
    /// Node that owns this partition (leader)
    owner_node: NodeId,
    /// Replica nodes
    replicas: [3]NodeId,
    replica_count: u8,
};

/// Cluster node info tracked by the controller
pub const NodeInfo = struct {
    id: NodeId,
    /// Hostname or IP
    address: [64]u8,
    address_len: u8,
    port: u16,
    /// Number of shards on this node
    shard_count: u8,
    /// Node status
    status: NodeStatus,
    /// Last heartbeat timestamp (milliseconds)
    last_seen_ms: u64,
};

pub const NodeStatus = enum(u8) {
    joining = 0,
    active = 1,
    leaving = 2,
    failed = 3,
};

// =============================================================================
// Metadata Command — serialized into Raft log entries
// =============================================================================

/// Commands that can be proposed to the Controller Raft.
/// Serialized as entry payload with a 1-byte command tag prefix.
pub const MetadataCommand = union(CommandTag) {
    create_namespace: CreateNamespace,
    delete_namespace: DeleteNamespace,
    update_namespace_config: UpdateNamespaceConfig,
    assign_partition: AssignPartition,
    add_node: AddNode,
    remove_node: RemoveNode,
    update_node_status: UpdateNodeStatus,

    pub const CommandTag = enum(u8) {
        create_namespace = 0x01,
        delete_namespace = 0x02,
        update_namespace_config = 0x03,
        assign_partition = 0x10,
        add_node = 0x20,
        remove_node = 0x21,
        update_node_status = 0x22,
    };

    pub const CreateNamespace = struct {
        name_len: u16,
        partition_count: u16,
        replication_factor: u8,
        // followed by name bytes
    };

    pub const DeleteNamespace = struct {
        name_len: u16,
        // followed by name bytes
    };

    pub const UpdateNamespaceConfig = struct {
        name_len: u16,
        settings_len: u16,
        // followed by name bytes, then settings TLV bytes
    };

    pub const AssignPartition = struct {
        namespace_hash: u32,
        partition_id: u16,
        owner_node: NodeId,
        replica_count: u8,
        replicas: [3]NodeId,
    };

    pub const AddNode = struct {
        node_id: NodeId,
        address_len: u8,
        port: u16,
        shard_count: u8,
        // followed by address bytes
    };

    pub const RemoveNode = struct {
        node_id: NodeId,
    };

    pub const UpdateNodeStatus = struct {
        node_id: NodeId,
        status: NodeStatus,
    };
};

// =============================================================================
// Coordinator
// =============================================================================

/// The Controller Raft Coordinator — manages cluster metadata on Shard 0.
pub const Coordinator = struct {
    allocator: Allocator,

    /// The underlying Raft state machine (group_id = 0)
    raft: RaftNode,

    /// This node's ID in the cluster
    node_id: NodeId,

    /// Namespace registry — indexed by name hash
    namespaces: std.AutoHashMapUnmanaged(u32, NamespaceConfig),

    /// Node registry — indexed by NodeId
    nodes: std.AutoHashMapUnmanaged(NodeId, NodeInfo),

    /// Last applied index from the Raft log
    last_applied: u64,

    /// Whether this coordinator is the active leader
    is_leader: bool,

    /// Statistics
    proposals_total: u64,
    proposals_committed: u64,
    proposals_failed: u64,

    // ── Construction ────────────────────────────────────────────────────

    pub fn init(allocator: Allocator, node_id: NodeId, config: Config) !Coordinator {
        var raft = try RaftNode.init(
            allocator,
            node_id,
            CONTROLLER_GROUP_ID,
            DEFAULT_LOG_CAPACITY,
            config,
        );
        errdefer raft.deinit();

        return .{
            .allocator = allocator,
            .raft = raft,
            .node_id = node_id,
            .namespaces = .{},
            .nodes = .{},
            .last_applied = 0,
            .is_leader = false,
            .proposals_total = 0,
            .proposals_committed = 0,
            .proposals_failed = 0,
        };
    }

    pub fn deinit(self: *Coordinator) void {
        // Free namespace names
        var ns_iter = self.namespaces.iterator();
        while (ns_iter.next()) |entry| {
            self.allocator.free(entry.value_ptr.name);
        }
        self.namespaces.deinit(self.allocator);
        self.nodes.deinit(self.allocator);
        self.raft.deinit();
    }

    // ── Cluster membership ──────────────────────────────────────────────

    /// Add a peer node to the Controller Raft group
    pub fn addPeer(self: *Coordinator, peer_id: NodeId) void {
        self.raft.addPeer(peer_id);
    }

    /// Bootstrap as single-node leader (for first node in cluster)
    pub fn bootstrap(self: *Coordinator) !void {
        log.debug("Coordinator: bootstrapping as single-node leader, node_id={d}", .{self.node_id});
        try self.raft.bootstrap();
        self.is_leader = true;
    }

    // ── Tick (called every 10ms from Shard 0 Reactor) ──────────────────

    /// Advance the Raft timer. Returns actions the caller should take.
    pub fn tick(self: *Coordinator, now_ms: u64) TickAction {
        const result = self.raft.tick(now_ms);

        // Track leader status
        self.is_leader = self.raft.role == .leader;

        return .{
            .send_heartbeats = result.send_heartbeats,
            .start_election = result.start_election,
            .step_down = result.step_down,
        };
    }

    pub const TickAction = struct {
        send_heartbeats: bool = false,
        start_election: bool = false,
        step_down: bool = false,
    };

    // ── Proposals (leader only) ─────────────────────────────────────────

    /// Propose creating a new namespace
    pub fn proposeCreateNamespace(
        self: *Coordinator,
        name: []const u8,
        partition_count: u16,
        replication_factor: u8,
    ) !raft_node.ProposeResult {
        if (!self.is_leader) return error.NotLeader;
        if (name.len > 255) return error.NameTooLong;

        // Serialize: tag(1) + name_len(2) + partition_count(2) + repl_factor(1) + name
        const payload_len = 1 + 2 + 2 + 1 + name.len;
        const buf = try self.allocator.alloc(u8, payload_len);
        defer self.allocator.free(buf);

        buf[0] = @intFromEnum(MetadataCommand.CommandTag.create_namespace);
        std.mem.writeInt(u16, buf[1..3], @intCast(name.len), .little);
        std.mem.writeInt(u16, buf[3..5], partition_count, .little);
        buf[5] = replication_factor;
        @memcpy(buf[6..][0..name.len], name);

        self.proposals_total += 1;
        log.debug("Coordinator: proposing create namespace={s}, partitions={d}, repl_factor={d}", .{ name, partition_count, replication_factor });
        return self.raft.propose(.raft_config, 0, 0, buf);
    }

    pub fn proposeDeleteNamespace(self: *Coordinator, name: []const u8) !raft_node.ProposeResult {
        if (!self.is_leader) return error.NotLeader;

        const payload_len = 1 + 2 + name.len;
        const buf = try self.allocator.alloc(u8, payload_len);
        defer self.allocator.free(buf);

        buf[0] = @intFromEnum(MetadataCommand.CommandTag.delete_namespace);
        std.mem.writeInt(u16, buf[1..3], @intCast(name.len), .little);
        @memcpy(buf[3..][0..name.len], name);

        self.proposals_total += 1;
        log.debug("Coordinator: proposing delete namespace={s}", .{name});
        return self.raft.propose(.raft_config, 0, 0, buf);
    }

    /// Propose updating namespace settings (admin-only)
    pub fn proposeUpdateNamespaceConfig(
        self: *Coordinator,
        name: []const u8,
        config: NamespaceConfig,
    ) !raft_node.ProposeResult {
        if (!self.is_leader) return error.NotLeader;
        if (name.len > 255) return error.NameTooLong;

        // Serialize settings to temporary buffer
        var settings_buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
        const settings_len = config.serializeSettings(&settings_buf);

        // tag(1) + name_len(2) + settings_len(2) + name + settings
        const payload_len = 1 + 2 + 2 + name.len + settings_len;
        const buf = try self.allocator.alloc(u8, payload_len);
        defer self.allocator.free(buf);

        buf[0] = @intFromEnum(MetadataCommand.CommandTag.update_namespace_config);
        std.mem.writeInt(u16, buf[1..3], @intCast(name.len), .little);
        std.mem.writeInt(u16, buf[3..5], @intCast(settings_len), .little);
        @memcpy(buf[5..][0..name.len], name);
        @memcpy(buf[5 + name.len ..][0..settings_len], settings_buf[0..settings_len]);

        self.proposals_total += 1;
        log.debug("Coordinator: proposing update namespace config={s}", .{name});
        return self.raft.propose(.raft_config, 0, 0, buf);
    }

    /// Propose adding a node to the cluster
    pub fn proposeAddNode(
        self: *Coordinator,
        new_node_id: NodeId,
        address: []const u8,
        port: u16,
        shard_count: u8,
    ) !raft_node.ProposeResult {
        if (!self.is_leader) return error.NotLeader;
        if (address.len > 64) return error.AddressTooLong;

        // tag(1) + node_id(4) + addr_len(1) + port(2) + shard_count(1) + addr
        const payload_len = 1 + 4 + 1 + 2 + 1 + address.len;
        const buf = try self.allocator.alloc(u8, payload_len);
        defer self.allocator.free(buf);

        buf[0] = @intFromEnum(MetadataCommand.CommandTag.add_node);
        std.mem.writeInt(u32, buf[1..5], new_node_id, .little);
        buf[5] = @intCast(address.len);
        std.mem.writeInt(u16, buf[6..8], port, .little);
        buf[8] = shard_count;
        @memcpy(buf[9..][0..address.len], address);

        self.proposals_total += 1;
        return self.raft.propose(.raft_config, 0, 0, buf);
    }

    /// Propose removing a node from the cluster
    pub fn proposeRemoveNode(self: *Coordinator, target_node_id: NodeId) !raft_node.ProposeResult {
        if (!self.is_leader) return error.NotLeader;

        var buf: [5]u8 = undefined;
        buf[0] = @intFromEnum(MetadataCommand.CommandTag.remove_node);
        std.mem.writeInt(u32, buf[1..5], target_node_id, .little);

        self.proposals_total += 1;
        return self.raft.propose(.raft_config, 0, 0, &buf);
    }

    /// Propose a partition assignment change
    pub fn proposeAssignPartition(
        self: *Coordinator,
        namespace_hash: u32,
        partition_id: u16,
        owner_node: NodeId,
        replicas: []const NodeId,
    ) !raft_node.ProposeResult {
        if (!self.is_leader) return error.NotLeader;

        // tag(1) + ns_hash(4) + partition_id(2) + owner(4) + replica_count(1) + replicas(4*3)
        var buf: [24]u8 = undefined;
        buf[0] = @intFromEnum(MetadataCommand.CommandTag.assign_partition);
        std.mem.writeInt(u32, buf[1..5], namespace_hash, .little);
        std.mem.writeInt(u16, buf[5..7], partition_id, .little);
        std.mem.writeInt(u32, buf[7..11], owner_node, .little);
        const replica_count: u8 = @intCast(@min(replicas.len, 3));
        buf[11] = replica_count;
        // Write up to 3 replica node IDs
        var i: usize = 0;
        while (i < replica_count) : (i += 1) {
            const offset = 12 + i * 4;
            std.mem.writeInt(u32, buf[offset..][0..4], replicas[i], .little);
        }
        // Zero remaining replica slots
        while (i < 3) : (i += 1) {
            const offset = 12 + i * 4;
            std.mem.writeInt(u32, buf[offset..][0..4], 0, .little);
        }

        self.proposals_total += 1;
        return self.raft.propose(.raft_config, 0, 0, buf[0 .. 12 + @as(usize, replica_count) * 4]);
    }

    // ── Apply (called when entries are committed) ───────────────────────

    /// Apply all committed but unapplied entries to the metadata state machine.
    /// Returns the number of entries applied.
    pub fn applyCommitted(self: *Coordinator) !u32 {
        var applied: u32 = 0;
        // getEntryCopy (not getEntry): the zero-copy read returns null for an
        // entry whose payload wraps the hot-ring byte boundary. Advancing
        // last_applied past such an entry with `continue` would silently drop a
        // committed raft_config command from the metadata state machine — the
        // same wrap-boundary data loss fixed in the stream/kv apply loops.
        var payload_buf: [@import("../storage/persistence.zig").MAX_PERSIST_PAYLOAD]u8 = undefined;
        while (self.last_applied < self.raft.commit_index) {
            self.last_applied += 1;
            const entry = self.raft.log.getEntryCopy(self.last_applied, &payload_buf) orelse continue;

            // Only process raft_config entries (metadata commands)
            if (entry.header.entry_type != @intFromEnum(EntryType.raft_config)) continue;

            try self.applyEntry(&entry);
            applied += 1;
            self.proposals_committed += 1;
        }
        if (applied > 0) {
            log.debug("Coordinator: applied {d} committed entries, last_applied={d}", .{ applied, self.last_applied });
        }
        return applied;
    }

    fn applyEntry(self: *Coordinator, entry: *const Entry) !void {
        const payload = entry.payload;
        if (payload.len == 0) return;

        const tag: MetadataCommand.CommandTag = @enumFromInt(payload[0]);
        switch (tag) {
            .create_namespace => try self.applyCreateNamespace(payload[1..]),
            .delete_namespace => try self.applyDeleteNamespace(payload[1..]),
            .update_namespace_config => try self.applyUpdateNamespaceConfig(payload[1..]),
            .assign_partition => {}, // Applied in Phase 7.2
            .add_node => try self.applyAddNode(payload[1..]),
            .remove_node => try self.applyRemoveNode(payload[1..]),
            .update_node_status => try self.applyUpdateNodeStatus(payload[1..]),
        }
    }

    fn applyCreateNamespace(self: *Coordinator, data: []const u8) !void {
        if (data.len < 5) return;
        const name_len = std.mem.readInt(u16, data[0..2], .little);
        const partition_count = std.mem.readInt(u16, data[2..4], .little);
        const replication_factor = data[4];
        if (data.len < 5 + name_len) return;

        const name = data[5..][0..name_len];
        const hash = hashNamespace(name);

        // Don't overwrite existing namespace
        if (self.namespaces.get(hash) != null) return;

        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);

        try self.namespaces.put(self.allocator, hash, .{
            .name = owned_name,
            .partition_count = partition_count,
            .replication_factor = replication_factor,
            .created_at_ns = @as(u64, @bitCast(@as(i64, @import("stdx").time.milliTimestamp()))) * 1_000_000,
            .deleted = false,
        });
        log.debug("Coordinator: created namespace={s}, partitions={d}", .{ name, partition_count });
    }

    fn applyDeleteNamespace(self: *Coordinator, data: []const u8) !void {
        if (data.len < 2) return;
        const name_len = std.mem.readInt(u16, data[0..2], .little);
        if (data.len < 2 + name_len) return;

        const name = data[2..][0..name_len];
        const hash = hashNamespace(name);

        if (self.namespaces.getPtr(hash)) |ns| {
            ns.deleted = true;
        }
    }

    fn applyUpdateNamespaceConfig(self: *Coordinator, data: []const u8) !void {
        if (data.len < 4) return;
        const name_len = std.mem.readInt(u16, data[0..2], .little);
        const settings_len = std.mem.readInt(u16, data[2..4], .little);
        if (data.len < 4 + name_len + settings_len) return;

        const name = data[4..][0..name_len];
        const hash = hashNamespace(name);

        if (self.namespaces.getPtr(hash)) |ns| {
            const result = NamespaceConfig.deserializeSettings(data[4 + name_len ..][0..settings_len]);
            ns.mergeSettings(result.config);
            log.debug("Coordinator: updated namespace config={s}", .{name});
        }
    }

    fn applyAddNode(self: *Coordinator, data: []const u8) !void {
        if (data.len < 8) return;
        const nid = std.mem.readInt(u32, data[0..4], .little);
        const addr_len = data[4];
        const port = std.mem.readInt(u16, data[5..7], .little);
        const shard_count = data[7];
        if (data.len < 8 + addr_len) return;

        var info = NodeInfo{
            .id = nid,
            .address = [_]u8{0} ** 64,
            .address_len = addr_len,
            .port = port,
            .shard_count = shard_count,
            .status = .joining,
            .last_seen_ms = 0,
        };
        @memcpy(info.address[0..addr_len], data[8..][0..addr_len]);

        try self.nodes.put(self.allocator, nid, info);
        log.debug("Coordinator: added node={d}, port={d}, shards={d}", .{ nid, port, shard_count });
    }

    fn applyRemoveNode(self: *Coordinator, data: []const u8) !void {
        if (data.len < 4) return;
        const nid = std.mem.readInt(u32, data[0..4], .little);
        log.debug("Coordinator: removed node={d}", .{nid});
        _ = self.nodes.remove(nid);
    }

    fn applyUpdateNodeStatus(self: *Coordinator, data: []const u8) !void {
        if (data.len < 5) return;
        const nid = std.mem.readInt(u32, data[0..4], .little);
        const status: NodeStatus = @enumFromInt(data[4]);
        if (self.nodes.getPtr(nid)) |node| {
            node.status = status;
        }
    }

    // ── Queries ─────────────────────────────────────────────────────────

    /// Get namespace info by name
    pub fn getNamespace(self: *const Coordinator, name: []const u8) ?NamespaceConfig {
        return self.namespaces.get(hashNamespace(name));
    }

    /// Get namespace settings by name (returns default config if namespace not found)
    pub fn getNamespaceSettings(self: *const Coordinator, name: []const u8) NamespaceConfig {
        if (self.namespaces.get(hashNamespace(name))) |ns| {
            return .{
                .kv_max_hot_versions = ns.kv_max_hot_versions,
                .kv_version_ttl_s = ns.kv_version_ttl_s,
                .stream_retention_bytes = ns.stream_retention_bytes,
                .stream_retention_s = ns.stream_retention_s,
                .queue_max_dlq_size = ns.queue_max_dlq_size,
                .queue_max_lease_s = ns.queue_max_lease_s,
                .memory_budget_bytes = ns.memory_budget_bytes,
            };
        }
        return .{};
    }

    /// Get node info by id
    pub fn getNode(self: *const Coordinator, nid: NodeId) ?NodeInfo {
        return self.nodes.get(nid);
    }

    /// Get the number of registered namespaces
    pub fn namespaceCount(self: *const Coordinator) u32 {
        return self.namespaces.count();
    }

    /// Get the number of registered nodes
    pub fn nodeCount(self: *const Coordinator) u32 {
        return self.nodes.count();
    }

    /// True if this coordinator is the leader
    pub fn isLeader(self: *const Coordinator) bool {
        return self.is_leader;
    }

    /// Current Raft term
    pub fn currentTerm(self: *const Coordinator) u64 {
        return self.raft.current_term;
    }

    /// Current leader node ID
    pub fn leaderId(self: *const Coordinator) NodeId {
        return self.raft.leader_id;
    }

    // ── Snapshot ────────────────────────────────────────────────────────

    /// Serialize coordinator state for snapshot transfer
    pub fn takeSnapshot(self: *const Coordinator, allocator: Allocator) ![]u8 {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        errdefer buf.deinit(allocator);

        // Format v2: version(1) + ns_count(4) + [ns entries with settings] + node_count(4) + [node entries]
        try buf.append(allocator, 2); // version 2 — includes settings

        // Namespaces
        const ns_count: u32 = self.namespaces.count();
        const ns_count_bytes = std.mem.toBytes(std.mem.nativeToLittle(u32, ns_count));
        try buf.appendSlice(allocator, &ns_count_bytes);

        var ns_iter = self.namespaces.iterator();
        while (ns_iter.next()) |entry| {
            const ns = entry.value_ptr;
            // hash(4) + name_len(2) + partition_count(2) + repl(1) + deleted(1) + name + settings_len(2) + settings
            const hash_bytes = std.mem.toBytes(std.mem.nativeToLittle(u32, entry.key_ptr.*));
            try buf.appendSlice(allocator, &hash_bytes);
            const nlen: u16 = @intCast(ns.name.len);
            const nlen_bytes = std.mem.toBytes(std.mem.nativeToLittle(u16, nlen));
            try buf.appendSlice(allocator, &nlen_bytes);
            const pc_bytes = std.mem.toBytes(std.mem.nativeToLittle(u16, ns.partition_count));
            try buf.appendSlice(allocator, &pc_bytes);
            try buf.append(allocator, ns.replication_factor);
            try buf.append(allocator, if (ns.deleted) @as(u8, 1) else 0);
            try buf.appendSlice(allocator, ns.name);

            // Serialize settings
            var settings_buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
            const slen = ns.serializeSettings(&settings_buf);
            const slen_bytes = std.mem.toBytes(std.mem.nativeToLittle(u16, @as(u16, @intCast(slen))));
            try buf.appendSlice(allocator, &slen_bytes);
            try buf.appendSlice(allocator, settings_buf[0..slen]);
        }

        // Nodes
        const node_count: u32 = self.nodes.count();
        const nc_bytes = std.mem.toBytes(std.mem.nativeToLittle(u32, node_count));
        try buf.appendSlice(allocator, &nc_bytes);

        var node_iter = self.nodes.iterator();
        while (node_iter.next()) |entry| {
            const node = entry.value_ptr;
            // id(4) + addr_len(1) + port(2) + shard_count(1) + status(1) + address
            const id_bytes = std.mem.toBytes(std.mem.nativeToLittle(u32, node.id));
            try buf.appendSlice(allocator, &id_bytes);
            try buf.append(allocator, node.address_len);
            const port_bytes = std.mem.toBytes(std.mem.nativeToLittle(u16, node.port));
            try buf.appendSlice(allocator, &port_bytes);
            try buf.append(allocator, node.shard_count);
            try buf.append(allocator, @intFromEnum(node.status));
            try buf.appendSlice(allocator, node.address[0..node.address_len]);
        }

        return buf.toOwnedSlice(allocator);
    }

    /// Restore coordinator state from snapshot data
    pub fn restoreSnapshot(self: *Coordinator, data: []const u8) !void {
        if (data.len < 1) return error.InvalidSnapshot;
        const version = data[0];
        if (version != 2) return error.UnsupportedVersion;
        var pos: usize = 1;

        // Clear existing state
        var ns_iter = self.namespaces.iterator();
        while (ns_iter.next()) |entry| {
            self.allocator.free(entry.value_ptr.name);
        }
        self.namespaces.clearRetainingCapacity();
        self.nodes.clearRetainingCapacity();

        // Read namespaces
        if (pos + 4 > data.len) return error.InvalidSnapshot;
        const ns_count = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;

        for (0..ns_count) |_| {
            if (pos + 10 > data.len) return error.InvalidSnapshot;
            const hash = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4;
            const nlen = std.mem.readInt(u16, data[pos..][0..2], .little);
            pos += 2;
            const pc = std.mem.readInt(u16, data[pos..][0..2], .little);
            pos += 2;
            const repl = data[pos];
            pos += 1;
            const deleted = data[pos] != 0;
            pos += 1;
            if (pos + nlen > data.len) return error.InvalidSnapshot;

            const name = try self.allocator.dupe(u8, data[pos..][0..nlen]);
            errdefer self.allocator.free(name);
            pos += nlen;

            // Deserialize settings
            if (pos + 2 > data.len) return error.InvalidSnapshot;
            const slen = std.mem.readInt(u16, data[pos..][0..2], .little);
            pos += 2;
            if (pos + slen > data.len) return error.InvalidSnapshot;
            const result = NamespaceConfig.deserializeSettings(data[pos..][0..slen]);
            pos += slen;

            var ns_config = result.config;
            ns_config.name = name;
            ns_config.partition_count = pc;
            ns_config.replication_factor = repl;
            ns_config.created_at_ns = 0;
            ns_config.deleted = deleted;

            try self.namespaces.put(self.allocator, hash, ns_config);
        }

        // Read nodes
        if (pos + 4 > data.len) return error.InvalidSnapshot;
        const node_count = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;

        for (0..node_count) |_| {
            if (pos + 9 > data.len) return error.InvalidSnapshot;
            const nid = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4;
            const addr_len = data[pos];
            pos += 1;
            const port = std.mem.readInt(u16, data[pos..][0..2], .little);
            pos += 2;
            const sc = data[pos];
            pos += 1;
            const status: NodeStatus = @enumFromInt(data[pos]);
            pos += 1;
            if (pos + addr_len > data.len) return error.InvalidSnapshot;

            var info = NodeInfo{
                .id = nid,
                .address = [_]u8{0} ** 64,
                .address_len = addr_len,
                .port = port,
                .shard_count = sc,
                .status = status,
                .last_seen_ms = 0,
            };
            @memcpy(info.address[0..addr_len], data[pos..][0..addr_len]);
            pos += addr_len;

            try self.nodes.put(self.allocator, nid, info);
        }
    }
};

// =============================================================================
// Helpers
// =============================================================================

/// Hash a namespace name to a 32-bit key (FNV-1a)
pub fn hashNamespace(name: []const u8) u32 {
    var h: u32 = 0x811c9dc5;
    for (name) |b| {
        h ^= b;
        h *%= 0x01000193;
    }
    return h;
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "Coordinator init and deinit" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try testing.expectEqual(CONTROLLER_GROUP_ID, coord.raft.group_id);
    try testing.expectEqual(@as(NodeId, 1), coord.node_id);
    try testing.expect(!coord.is_leader);
    try testing.expectEqual(@as(u32, 0), coord.namespaceCount());
    try testing.expectEqual(@as(u32, 0), coord.nodeCount());
}

test "Coordinator bootstrap single-node" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();
    try testing.expect(coord.is_leader);
    try testing.expectEqual(raft_node.Role.leader, coord.raft.role);
}

test "Coordinator propose create namespace" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    const result = try coord.proposeCreateNamespace("events", 8, 3);
    try testing.expect(result.index > 0);
    try testing.expect(result.term > 0);
    try testing.expectEqual(@as(u64, 1), coord.proposals_total);
}

test "Coordinator propose requires leader" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    // Not bootstrapped — should fail
    const result = coord.proposeCreateNamespace("events", 8, 3);
    try testing.expectError(error.NotLeader, result);
}

test "Coordinator apply create namespace" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    _ = try coord.proposeCreateNamespace("events", 8, 3);
    const applied = try coord.applyCommitted();
    try testing.expect(applied > 0);
    try testing.expectEqual(@as(u32, 1), coord.namespaceCount());

    const ns = coord.getNamespace("events");
    try testing.expect(ns != null);
    try testing.expectEqualStrings("events", ns.?.name);
    try testing.expectEqual(@as(u16, 8), ns.?.partition_count);
    try testing.expectEqual(@as(u8, 3), ns.?.replication_factor);
    try testing.expect(!ns.?.deleted);
}

test "Coordinator apply delete namespace" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    _ = try coord.proposeCreateNamespace("events", 8, 3);
    _ = try coord.applyCommitted();

    _ = try coord.proposeDeleteNamespace("events");
    _ = try coord.applyCommitted();

    const ns = coord.getNamespace("events");
    try testing.expect(ns != null);
    try testing.expect(ns.?.deleted);
}

test "Coordinator apply add node" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    _ = try coord.proposeAddNode(2, "192.168.1.100", 4444, 4);
    _ = try coord.applyCommitted();

    try testing.expectEqual(@as(u32, 1), coord.nodeCount());
    const node = coord.getNode(2);
    try testing.expect(node != null);
    try testing.expectEqual(@as(u16, 4444), node.?.port);
    try testing.expectEqual(@as(u8, 4), node.?.shard_count);
    try testing.expectEqual(NodeStatus.joining, node.?.status);
}

test "Coordinator apply remove node" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    _ = try coord.proposeAddNode(2, "192.168.1.100", 4444, 4);
    _ = try coord.applyCommitted();
    try testing.expectEqual(@as(u32, 1), coord.nodeCount());

    _ = try coord.proposeRemoveNode(2);
    _ = try coord.applyCommitted();
    try testing.expectEqual(@as(u32, 0), coord.nodeCount());
}

test "Coordinator snapshot roundtrip" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    // Create some state
    _ = try coord.proposeCreateNamespace("events", 8, 3);
    _ = try coord.proposeCreateNamespace("logs", 4, 1);
    _ = try coord.proposeAddNode(2, "10.0.0.1", 4444, 8);
    _ = try coord.applyCommitted();

    // Take snapshot
    const snapshot = try coord.takeSnapshot(allocator);
    defer allocator.free(snapshot);

    // Restore into a new coordinator
    var coord2 = try Coordinator.init(allocator, 1, .{});
    defer coord2.deinit();

    try coord2.restoreSnapshot(snapshot);
    try testing.expectEqual(@as(u32, 2), coord2.namespaceCount());
    try testing.expectEqual(@as(u32, 1), coord2.nodeCount());

    const ns = coord2.getNamespace("events");
    try testing.expect(ns != null);
    try testing.expectEqualStrings("events", ns.?.name);

    const node = coord2.getNode(2);
    try testing.expect(node != null);
    try testing.expectEqual(@as(u16, 4444), node.?.port);
}

test "Coordinator propose partition assignment" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    const replicas = [_]NodeId{ 2, 3 };
    const result = try coord.proposeAssignPartition(0xDEADBEEF, 0, 1, &replicas);
    try testing.expect(result.index > 0);
}

test "Coordinator tick updates leader status" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();
    try testing.expect(coord.is_leader);

    const action = coord.tick(1000);
    try testing.expect(coord.is_leader);
    _ = action;
}

test "hashNamespace deterministic" {
    const h1 = hashNamespace("events");
    const h2 = hashNamespace("events");
    try testing.expectEqual(h1, h2);

    const h3 = hashNamespace("logs");
    try testing.expect(h1 != h3);
}

test "Coordinator multiple namespaces" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    _ = try coord.proposeCreateNamespace("ns1", 4, 1);
    _ = try coord.proposeCreateNamespace("ns2", 8, 3);
    _ = try coord.proposeCreateNamespace("ns3", 16, 2);
    _ = try coord.applyCommitted();

    try testing.expectEqual(@as(u32, 3), coord.namespaceCount());
    try testing.expect(coord.getNamespace("ns1") != null);
    try testing.expect(coord.getNamespace("ns2") != null);
    try testing.expect(coord.getNamespace("ns3") != null);
    try testing.expect(coord.getNamespace("ns4") == null);
}

test "NamespaceConfig settings serialize/deserialize roundtrip" {
    const original = NamespaceConfig{
        .kv_max_hot_versions = 50,
        .kv_version_ttl_s = 3600,
        .stream_retention_bytes = 1_073_741_824,
        .stream_retention_s = 86400,
        .queue_max_dlq_size = 500,
        .queue_max_lease_s = 60,
        .memory_budget_bytes = 2_147_483_648,
    };

    var buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
    const len = original.serializeSettings(&buf);
    try testing.expect(len > 0);

    const result = NamespaceConfig.deserializeSettings(buf[0..len]);
    try testing.expectEqual(original.kv_max_hot_versions, result.config.kv_max_hot_versions);
    try testing.expectEqual(original.kv_version_ttl_s, result.config.kv_version_ttl_s);
    try testing.expectEqual(original.stream_retention_bytes, result.config.stream_retention_bytes);
    try testing.expectEqual(original.stream_retention_s, result.config.stream_retention_s);
    try testing.expectEqual(original.queue_max_dlq_size, result.config.queue_max_dlq_size);
    try testing.expectEqual(original.queue_max_lease_s, result.config.queue_max_lease_s);
    try testing.expectEqual(original.memory_budget_bytes, result.config.memory_budget_bytes);
    try testing.expectEqual(len, result.consumed);
}

test "NamespaceConfig settings serialize/deserialize partial" {
    const original = NamespaceConfig{
        .kv_max_hot_versions = 100,
        .stream_retention_s = 7200,
    };

    var buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
    const len = original.serializeSettings(&buf);

    const result = NamespaceConfig.deserializeSettings(buf[0..len]);
    try testing.expectEqual(@as(?u32, 100), result.config.kv_max_hot_versions);
    try testing.expectEqual(@as(?u64, 7200), result.config.stream_retention_s);
    try testing.expectEqual(@as(?u64, null), result.config.kv_version_ttl_s);
    try testing.expectEqual(@as(?u64, null), result.config.stream_retention_bytes);
    try testing.expectEqual(@as(?u32, null), result.config.queue_max_dlq_size);
    try testing.expectEqual(@as(?u32, null), result.config.queue_max_lease_s);
    try testing.expectEqual(@as(?u64, null), result.config.memory_budget_bytes);
}

test "NamespaceConfig settings empty roundtrip" {
    const original = NamespaceConfig{};
    try testing.expect(original.settingsEmpty());

    var buf: [NamespaceConfig.MAX_SETTINGS_SIZE]u8 = undefined;
    const len = original.serializeSettings(&buf);
    try testing.expectEqual(@as(usize, 1), len); // just the count byte = 0

    const result = NamespaceConfig.deserializeSettings(buf[0..len]);
    try testing.expect(result.config.settingsEmpty());
}

test "NamespaceConfig settings merge" {
    var base = NamespaceConfig{
        .kv_max_hot_versions = 50,
        .stream_retention_s = 3600,
    };

    const update = NamespaceConfig{
        .kv_max_hot_versions = 100,
        .queue_max_dlq_size = 200,
    };

    base.mergeSettings(update);
    try testing.expectEqual(@as(?u32, 100), base.kv_max_hot_versions); // overwritten
    try testing.expectEqual(@as(?u64, 3600), base.stream_retention_s); // preserved
    try testing.expectEqual(@as(?u32, 200), base.queue_max_dlq_size); // newly set
    try testing.expectEqual(@as(?u64, null), base.kv_version_ttl_s); // still null
}

test "Coordinator update namespace config" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    _ = try coord.proposeCreateNamespace("events", 8, 3);
    _ = try coord.applyCommitted();

    // Update config
    const config = NamespaceConfig{ .kv_max_hot_versions = 50, .stream_retention_s = 86400 };
    _ = try coord.proposeUpdateNamespaceConfig("events", config);
    _ = try coord.applyCommitted();

    const retrieved = coord.getNamespaceSettings("events");
    try testing.expectEqual(@as(?u32, 50), retrieved.kv_max_hot_versions);
    try testing.expectEqual(@as(?u64, 86400), retrieved.stream_retention_s);
    try testing.expectEqual(@as(?u64, null), retrieved.kv_version_ttl_s);
}

test "Coordinator update namespace config merge" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    _ = try coord.proposeCreateNamespace("app", 4, 1);
    _ = try coord.applyCommitted();

    // Set initial config
    _ = try coord.proposeUpdateNamespaceConfig("app", .{ .kv_max_hot_versions = 50 });
    _ = try coord.applyCommitted();

    // Merge additional settings
    _ = try coord.proposeUpdateNamespaceConfig("app", .{ .queue_max_dlq_size = 200 });
    _ = try coord.applyCommitted();

    const retrieved = coord.getNamespaceSettings("app");
    try testing.expectEqual(@as(?u32, 50), retrieved.kv_max_hot_versions); // preserved
    try testing.expectEqual(@as(?u32, 200), retrieved.queue_max_dlq_size); // newly set
}

test "Coordinator snapshot roundtrip with settings" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    _ = try coord.proposeCreateNamespace("events", 8, 3);
    _ = try coord.proposeCreateNamespace("logs", 4, 1);
    _ = try coord.applyCommitted();

    // Set config on events
    _ = try coord.proposeUpdateNamespaceConfig("events", .{
        .kv_max_hot_versions = 100,
        .memory_budget_bytes = 4_294_967_296,
    });
    _ = try coord.applyCommitted();

    // Take snapshot
    const snapshot = try coord.takeSnapshot(allocator);
    defer allocator.free(snapshot);

    // Restore into a new coordinator
    var coord2 = try Coordinator.init(allocator, 1, .{});
    defer coord2.deinit();

    try coord2.restoreSnapshot(snapshot);
    try testing.expectEqual(@as(u32, 2), coord2.namespaceCount());

    // Verify settings survived the roundtrip
    const settings = coord2.getNamespaceSettings("events");
    try testing.expectEqual(@as(?u32, 100), settings.kv_max_hot_versions);
    try testing.expectEqual(@as(?u64, 4_294_967_296), settings.memory_budget_bytes);
    try testing.expectEqual(@as(?u64, null), settings.stream_retention_s);

    // logs should have empty settings
    const log_settings = coord2.getNamespaceSettings("logs");
    try testing.expect(log_settings.settingsEmpty());
}

test "Coordinator getNamespaceSettings nonexistent" {
    const allocator = testing.allocator;
    var coord = try Coordinator.init(allocator, 1, .{});
    defer coord.deinit();

    try coord.bootstrap();

    const settings = coord.getNamespaceSettings("nonexistent");
    try testing.expect(settings.settingsEmpty());
}
