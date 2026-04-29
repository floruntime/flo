//! Replicated Partition Table — partition → node mapping
//!
//! Maps each (namespace, partition_id) to the owning node and its replicas.
//! Replicated across all nodes via Controller Raft (group_id = 0).
//!
//! The table supports:
//!   - Looking up where a partition lives (which node is the leader)
//!   - Finding all partitions owned by a specific node (for rebalancing)
//!   - Computing an initial assignment when a namespace is created
//!   - Reassigning partitions during rebalancing
//!
//! Each shard caches a read-only copy of the table. When the Controller
//! commits an assignment change, it broadcasts a `metadata_update` inbox
//! message to invalidate local caches.
//!
//! Wire format: The table is serialized for snapshot transfer and
//! Controller Raft log entries.

const std = @import("std");
const Allocator = std.mem.Allocator;
const coordinator = @import("coordinator.zig");
const NodeId = @import("../raft/node.zig").NodeId;

// =============================================================================
// Constants
// =============================================================================

/// Maximum partitions we track (across all namespaces)
pub const MAX_TOTAL_PARTITIONS: usize = 65536;

/// Maximum replicas per partition
pub const MAX_REPLICAS: usize = 3;

// =============================================================================
// Types
// =============================================================================

/// Unique identifier for a partition within a namespace
pub const PartitionKey = struct {
    namespace_hash: u32,
    partition_id: u16,
};

/// Full assignment for a single partition
pub const Assignment = struct {
    /// Node that owns (leads) this partition
    leader: NodeId,
    /// Replica nodes
    replicas: [MAX_REPLICAS]NodeId,
    /// Number of active replicas
    replica_count: u8,
    /// Assignment epoch (incremented on every change for cache invalidation)
    epoch: u64,
    /// Whether this assignment is pending (in-flight rebalance)
    pending: bool,
    /// Whether this partition is temporarily unavailable (leader election, etc.)
    unavailable: bool = false,

    pub fn hasReplica(self: *const Assignment, node_id: NodeId) bool {
        for (self.replicas[0..self.replica_count]) |r| {
            if (r == node_id) return true;
        }
        return false;
    }
};

/// Lookup result for a partition
pub const LookupResult = struct {
    leader: NodeId,
    replicas: [MAX_REPLICAS]NodeId,
    replica_count: u8,
    epoch: u64,
    is_local: bool,
};

/// Stats about the partition table
pub const TableStats = struct {
    total_partitions: u32,
    namespaces: u32,
    /// Number of partitions per node (for balance checking)
    per_node_counts: std.AutoHashMapUnmanaged(NodeId, u32),
};

// =============================================================================
// Partition Table
// =============================================================================

pub const PartitionTable = struct {
    allocator: Allocator,

    /// The main lookup table: (namespace_hash, partition_id) → Assignment
    assignments: std.HashMapUnmanaged(
        PackedKey,
        Assignment,
        PackedKeyContext,
        std.hash_map.default_max_load_percentage,
    ),

    /// Index: node_id → list of partition keys owned by that node
    node_partitions: std.AutoHashMapUnmanaged(NodeId, std.ArrayListUnmanaged(PackedKey)),

    /// Current epoch — monotonically increasing on every mutation
    epoch: u64,

    /// This node's ID (for is_local checks)
    local_node_id: NodeId,

    /// Total number of assignments
    count: u32,

    // ── Packed key for efficient hashing ────────────────────────────────

    /// Packed (namespace_hash, partition_id) into a single u64
    pub const PackedKey = u64;

    pub fn packKey(namespace_hash: u32, partition_id: u16) PackedKey {
        return @as(PackedKey, namespace_hash) << 16 | @as(PackedKey, partition_id);
    }

    pub fn unpackKey(key: PackedKey) PartitionKey {
        return .{
            .namespace_hash = @intCast(key >> 16),
            .partition_id = @intCast(key & 0xFFFF),
        };
    }

    pub const PackedKeyContext = struct {
        pub fn hash(_: PackedKeyContext, key: PackedKey) u64 {
            return std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
        }
        pub fn eql(_: PackedKeyContext, a: PackedKey, b: PackedKey) bool {
            return a == b;
        }
    };

    // ── Construction ────────────────────────────────────────────────────

    pub fn init(allocator: Allocator, local_node_id: NodeId) PartitionTable {
        return .{
            .allocator = allocator,
            .assignments = .empty,
            .node_partitions = .empty,
            .epoch = 0,
            .local_node_id = local_node_id,
            .count = 0,
        };
    }

    pub fn deinit(self: *PartitionTable) void {
        var np_iter = self.node_partitions.iterator();
        while (np_iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.node_partitions.deinit(self.allocator);
        self.assignments.deinit(self.allocator);
    }

    // ── Assignment ──────────────────────────────────────────────────────

    /// Assign a partition to a node with optional replicas.
    /// This is the primary mutation method — called when applying Controller Raft entries.
    pub fn assign(
        self: *PartitionTable,
        namespace_hash: u32,
        partition_id: u16,
        leader: NodeId,
        replicas: []const NodeId,
    ) !void {
        const key = packKey(namespace_hash, partition_id);
        self.epoch += 1;

        // Remove old assignment from node index if it exists
        if (self.assignments.get(key)) |old| {
            self.removeFromNodeIndex(old.leader, key);
            for (old.replicas[0..old.replica_count]) |r| {
                self.removeFromNodeIndex(r, key);
            }
        } else {
            self.count += 1;
        }

        // Build the new assignment
        var assignment = Assignment{
            .leader = leader,
            .replicas = [_]NodeId{0} ** MAX_REPLICAS,
            .replica_count = @intCast(@min(replicas.len, MAX_REPLICAS)),
            .epoch = self.epoch,
            .pending = false,
        };
        for (replicas[0..assignment.replica_count], 0..) |r, i| {
            assignment.replicas[i] = r;
        }

        try self.assignments.put(self.allocator, key, assignment);

        // Update node index
        try self.addToNodeIndex(leader, key);
        for (replicas[0..assignment.replica_count]) |r| {
            try self.addToNodeIndex(r, key);
        }
    }

    /// Remove a partition assignment entirely
    pub fn remove(self: *PartitionTable, namespace_hash: u32, partition_id: u16) void {
        const key = packKey(namespace_hash, partition_id);
        if (self.assignments.get(key)) |old| {
            self.removeFromNodeIndex(old.leader, key);
            for (old.replicas[0..old.replica_count]) |r| {
                self.removeFromNodeIndex(r, key);
            }
            _ = self.assignments.remove(key);
            self.count -= 1;
            self.epoch += 1;
        }
    }

    // ── Lookup ──────────────────────────────────────────────────────────

    /// Look up which node owns a partition
    pub fn lookup(self: *const PartitionTable, namespace_hash: u32, partition_id: u16) ?LookupResult {
        const key = packKey(namespace_hash, partition_id);
        const a = self.assignments.get(key) orelse return null;
        return .{
            .leader = a.leader,
            .replicas = a.replicas,
            .replica_count = a.replica_count,
            .epoch = a.epoch,
            .is_local = a.leader == self.local_node_id,
        };
    }

    /// Get the raw assignment for a partition
    pub fn getAssignment(self: *const PartitionTable, namespace_hash: u32, partition_id: u16) ?Assignment {
        return self.assignments.get(packKey(namespace_hash, partition_id));
    }

    /// Check if a partition is owned by this node
    pub fn isLocal(self: *const PartitionTable, namespace_hash: u32, partition_id: u16) bool {
        if (self.lookup(namespace_hash, partition_id)) |result| {
            return result.is_local;
        }
        // If no assignment exists, assume local (single-node mode)
        return true;
    }

    /// Check if a partition is available (has a leader and is not in transition)
    pub fn isAvailable(self: *const PartitionTable, namespace_hash: u32, partition_id: u16) bool {
        const key = packKey(namespace_hash, partition_id);
        const a = self.assignments.get(key) orelse return true; // unassigned = single-node mode
        return !a.unavailable and !a.pending;
    }

    /// Mark a partition as temporarily unavailable (e.g., leader election in progress)
    pub fn markUnavailable(self: *PartitionTable, namespace_hash: u32, partition_id: u16) void {
        const key = packKey(namespace_hash, partition_id);
        if (self.assignments.getPtr(key)) |a| {
            a.unavailable = true;
        }
    }

    /// Mark a partition as available again (e.g., new leader elected)
    pub fn markAvailable(self: *PartitionTable, namespace_hash: u32, partition_id: u16) void {
        const key = packKey(namespace_hash, partition_id);
        if (self.assignments.getPtr(key)) |a| {
            a.unavailable = false;
        }
    }

    /// Mark all partitions led by a given node as unavailable
    /// (used when gossip detects a node failure)
    pub fn markNodePartitionsUnavailable(self: *PartitionTable, node_id: NodeId) u32 {
        var count: u32 = 0;
        var iter = self.assignments.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.leader == node_id and !entry.value_ptr.unavailable) {
                entry.value_ptr.unavailable = true;
                count += 1;
            }
        }
        return count;
    }

    /// Mark all partitions led by a given node as available
    /// (used when gossip detects a node recovery)
    pub fn markNodePartitionsAvailable(self: *PartitionTable, node_id: NodeId) u32 {
        var count: u32 = 0;
        var iter = self.assignments.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.leader == node_id and entry.value_ptr.unavailable) {
                entry.value_ptr.unavailable = false;
                count += 1;
            }
        }
        return count;
    }

    /// Get all partition keys assigned to a specific node (as leader or replica)
    pub fn partitionsForNode(self: *const PartitionTable, node_id: NodeId) []const PackedKey {
        if (self.node_partitions.get(node_id)) |list| {
            return list.items;
        }
        return &.{};
    }

    /// Count partitions led by a specific node
    pub fn countLeaderPartitions(self: *const PartitionTable, node_id: NodeId) u32 {
        var count: u32 = 0;
        var iter = self.assignments.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.leader == node_id) count += 1;
        }
        return count;
    }

    // ── Bulk assignment (namespace creation) ────────────────────────────

    /// Compute and apply an initial round-robin assignment for a new namespace.
    /// Distributes partitions evenly across the provided nodes.
    pub fn assignNamespace(
        self: *PartitionTable,
        namespace_hash: u32,
        partition_count: u16,
        nodes: []const NodeId,
        replication_factor: u8,
    ) !void {
        if (nodes.len == 0) return error.NoNodes;

        const node_count = nodes.len;
        const repl: usize = @min(replication_factor, @as(u8, @intCast(node_count)));

        var partition_id: u16 = 0;
        while (partition_id < partition_count) : (partition_id += 1) {
            // Round-robin leader assignment
            const leader_idx = @as(usize, partition_id) % node_count;
            const leader = nodes[leader_idx];

            // Replicas: next N nodes in the ring (excluding leader)
            var replicas: [MAX_REPLICAS]NodeId = [_]NodeId{0} ** MAX_REPLICAS;
            var replica_count: u8 = 0;
            if (repl > 1) {
                var i: usize = 1;
                while (i < repl) : (i += 1) {
                    const replica_idx = (leader_idx + i) % node_count;
                    replicas[replica_count] = nodes[replica_idx];
                    replica_count += 1;
                }
            }

            try self.assign(namespace_hash, partition_id, leader, replicas[0..replica_count]);
        }
    }

    // ── Serialization ───────────────────────────────────────────────────

    /// Serialize the entire table for snapshot transfer
    pub fn serialize(self: *const PartitionTable, allocator: Allocator) ![]u8 {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        errdefer buf.deinit(allocator);

        // Version(1) + epoch(8) + count(4)
        try buf.append(allocator, 1); // version
        const epoch_bytes = std.mem.toBytes(std.mem.nativeToLittle(u64, self.epoch));
        try buf.appendSlice(allocator, &epoch_bytes);
        const count_bytes = std.mem.toBytes(std.mem.nativeToLittle(u32, self.count));
        try buf.appendSlice(allocator, &count_bytes);

        // Each entry: key(6) + leader(4) + replica_count(1) + replicas(4*count) + epoch(8) + pending(1)
        var iter = self.assignments.iterator();
        while (iter.next()) |entry| {
            const pk = entry.key_ptr.*;
            const a = entry.value_ptr;

            // PackedKey as 6 bytes (little-endian u48)
            const pk_bytes = std.mem.toBytes(std.mem.nativeToLittle(PackedKey, pk));
            try buf.appendSlice(allocator, &pk_bytes);

            const leader_bytes = std.mem.toBytes(std.mem.nativeToLittle(u32, a.leader));
            try buf.appendSlice(allocator, &leader_bytes);
            try buf.append(allocator, a.replica_count);

            for (a.replicas[0..a.replica_count]) |r| {
                const r_bytes = std.mem.toBytes(std.mem.nativeToLittle(u32, r));
                try buf.appendSlice(allocator, &r_bytes);
            }

            const a_epoch_bytes = std.mem.toBytes(std.mem.nativeToLittle(u64, a.epoch));
            try buf.appendSlice(allocator, &a_epoch_bytes);
            try buf.append(allocator, if (a.pending) @as(u8, 1) else 0);
        }

        return buf.toOwnedSlice(allocator);
    }

    /// Deserialize a partition table from snapshot data
    pub fn deserialize(self: *PartitionTable, data: []const u8) !void {
        if (data.len < 13) return error.InvalidSnapshot;
        if (data[0] != 1) return error.UnsupportedVersion;

        var pos: usize = 1;
        const epoch = std.mem.readInt(u64, data[pos..][0..8], .little);
        pos += 8;
        const entry_count = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;

        // Clear existing
        var np_iter = self.node_partitions.iterator();
        while (np_iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.node_partitions.clearRetainingCapacity();
        self.assignments.clearRetainingCapacity();
        self.count = 0;

        for (0..entry_count) |_| {
            if (pos + 8 > data.len) return error.InvalidSnapshot;
            const pk = std.mem.readInt(PackedKey, data[pos..][0..8], .little);
            pos += 8;

            if (pos + 5 > data.len) return error.InvalidSnapshot;
            const leader = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4;
            const replica_count = data[pos];
            pos += 1;

            var replicas: [MAX_REPLICAS]NodeId = [_]NodeId{0} ** MAX_REPLICAS;
            for (0..replica_count) |i| {
                if (pos + 4 > data.len) return error.InvalidSnapshot;
                replicas[i] = std.mem.readInt(u32, data[pos..][0..4], .little);
                pos += 4;
            }

            if (pos + 9 > data.len) return error.InvalidSnapshot;
            const a_epoch = std.mem.readInt(u64, data[pos..][0..8], .little);
            pos += 8;
            const pending = data[pos] != 0;
            pos += 1;

            const key_info = unpackKey(pk);
            try self.assign(
                key_info.namespace_hash,
                key_info.partition_id,
                leader,
                replicas[0..replica_count],
            );

            // Restore original epoch and pending flag
            if (self.assignments.getPtr(pk)) |a| {
                a.epoch = a_epoch;
                a.pending = pending;
            }
        }

        self.epoch = epoch;
    }

    // ── Internal helpers ────────────────────────────────────────────────

    fn addToNodeIndex(self: *PartitionTable, node_id: NodeId, key: PackedKey) !void {
        const gop = try self.node_partitions.getOrPut(self.allocator, node_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .empty;
        }
        // Check for duplicates
        for (gop.value_ptr.items) |existing| {
            if (existing == key) return;
        }
        try gop.value_ptr.append(self.allocator, key);
    }

    fn removeFromNodeIndex(self: *PartitionTable, node_id: NodeId, key: PackedKey) void {
        if (self.node_partitions.getPtr(node_id)) |list| {
            for (list.items, 0..) |item, i| {
                if (item == key) {
                    _ = list.swapRemove(i);
                    return;
                }
            }
        }
    }
};

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "PartitionTable init and deinit" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    try testing.expectEqual(@as(u32, 0), table.count);
    try testing.expectEqual(@as(u64, 0), table.epoch);
}

test "PartitionTable assign and lookup" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    const ns_hash: u32 = 0xDEADBEEF;
    try table.assign(ns_hash, 0, 1, &.{ 2, 3 });

    const result = table.lookup(ns_hash, 0);
    try testing.expect(result != null);
    try testing.expectEqual(@as(NodeId, 1), result.?.leader);
    try testing.expect(result.?.is_local); // local_node_id == 1
    try testing.expectEqual(@as(u8, 2), result.?.replica_count);
    try testing.expectEqual(@as(NodeId, 2), result.?.replicas[0]);
    try testing.expectEqual(@as(NodeId, 3), result.?.replicas[1]);
}

test "PartitionTable isLocal" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    try table.assign(0xAAAA, 0, 1, &.{});
    try table.assign(0xAAAA, 1, 2, &.{});

    try testing.expect(table.isLocal(0xAAAA, 0)); // owned by node 1 (us)
    try testing.expect(!table.isLocal(0xAAAA, 1)); // owned by node 2
    try testing.expect(table.isLocal(0xAAAA, 99)); // no assignment → assume local
}

test "PartitionTable reassignment updates epoch" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    try table.assign(0xBBBB, 0, 1, &.{});
    const epoch1 = table.epoch;

    // Reassign to a different node
    try table.assign(0xBBBB, 0, 2, &.{3});
    try testing.expect(table.epoch > epoch1);

    const result = table.lookup(0xBBBB, 0);
    try testing.expectEqual(@as(NodeId, 2), result.?.leader);
    try testing.expectEqual(@as(u32, 1), table.count); // count shouldn't double
}

test "PartitionTable remove" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    try table.assign(0xCCCC, 0, 1, &.{});
    try testing.expectEqual(@as(u32, 1), table.count);

    table.remove(0xCCCC, 0);
    try testing.expectEqual(@as(u32, 0), table.count);
    try testing.expect(table.lookup(0xCCCC, 0) == null);
}

test "PartitionTable partitionsForNode" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    try table.assign(0xAAAA, 0, 1, &.{});
    try table.assign(0xAAAA, 1, 1, &.{});
    try table.assign(0xAAAA, 2, 2, &.{1}); // node 1 is replica

    const node1_parts = table.partitionsForNode(1);
    try testing.expectEqual(@as(usize, 3), node1_parts.len); // leader of 2, replica of 1

    const node2_parts = table.partitionsForNode(2);
    try testing.expectEqual(@as(usize, 1), node2_parts.len);
}

test "PartitionTable countLeaderPartitions" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    try table.assign(0xAAAA, 0, 1, &.{});
    try table.assign(0xAAAA, 1, 1, &.{});
    try table.assign(0xAAAA, 2, 2, &.{1});

    try testing.expectEqual(@as(u32, 2), table.countLeaderPartitions(1));
    try testing.expectEqual(@as(u32, 1), table.countLeaderPartitions(2));
    try testing.expectEqual(@as(u32, 0), table.countLeaderPartitions(3));
}

test "PartitionTable assignNamespace round-robin" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    const nodes = [_]NodeId{ 1, 2, 3 };
    try table.assignNamespace(0xAAAA, 6, &nodes, 2);

    try testing.expectEqual(@as(u32, 6), table.count);

    // Check round-robin distribution
    try testing.expectEqual(@as(NodeId, 1), table.lookup(0xAAAA, 0).?.leader);
    try testing.expectEqual(@as(NodeId, 2), table.lookup(0xAAAA, 1).?.leader);
    try testing.expectEqual(@as(NodeId, 3), table.lookup(0xAAAA, 2).?.leader);
    try testing.expectEqual(@as(NodeId, 1), table.lookup(0xAAAA, 3).?.leader);

    // Check replication
    const a0 = table.getAssignment(0xAAAA, 0).?;
    try testing.expectEqual(@as(u8, 1), a0.replica_count);
    try testing.expectEqual(@as(NodeId, 2), a0.replicas[0]); // next node in ring
}

test "PartitionTable assignNamespace single node" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    const nodes = [_]NodeId{1};
    try table.assignNamespace(0xBBBB, 4, &nodes, 3);

    try testing.expectEqual(@as(u32, 4), table.count);

    // All partitions on node 1, no replicas (repl clamped to node count)
    for (0..4) |i| {
        const result = table.lookup(0xBBBB, @intCast(i)).?;
        try testing.expectEqual(@as(NodeId, 1), result.leader);
        try testing.expectEqual(@as(u8, 0), result.replica_count);
    }
}

test "PartitionTable assignNamespace no nodes errors" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    const result = table.assignNamespace(0xAAAA, 4, &.{}, 1);
    try testing.expectError(error.NoNodes, result);
}

test "PartitionTable serialize and deserialize" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    const nodes = [_]NodeId{ 1, 2, 3 };
    try table.assignNamespace(0xAAAA, 4, &nodes, 2);
    try table.assignNamespace(0xBBBB, 2, &nodes, 1);

    const data = try table.serialize(testing.allocator);
    defer testing.allocator.free(data);

    // Deserialize into a new table
    var table2 = PartitionTable.init(testing.allocator, 1);
    defer table2.deinit();

    try table2.deserialize(data);
    try testing.expectEqual(table.count, table2.count);

    // Verify specific lookups
    const r1 = table.lookup(0xAAAA, 0).?;
    const r2 = table2.lookup(0xAAAA, 0).?;
    try testing.expectEqual(r1.leader, r2.leader);
    try testing.expectEqual(r1.replica_count, r2.replica_count);
}

test "PartitionTable packKey roundtrip" {
    const key = PartitionTable.packKey(0xDEADBEEF, 42);
    const unpacked = PartitionTable.unpackKey(key);
    try testing.expectEqual(@as(u32, 0xDEADBEEF), unpacked.namespace_hash);
    try testing.expectEqual(@as(u16, 42), unpacked.partition_id);
}

test "Assignment hasReplica" {
    const a = Assignment{
        .leader = 1,
        .replicas = .{ 2, 3, 0 },
        .replica_count = 2,
        .epoch = 1,
        .pending = false,
    };

    try testing.expect(a.hasReplica(2));
    try testing.expect(a.hasReplica(3));
    try testing.expect(!a.hasReplica(1)); // leader is not in replicas
    try testing.expect(!a.hasReplica(4));
}

test "PartitionTable: partition availability" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    try table.assign(0x1000, 0, 1, &.{2});
    try table.assign(0x1000, 1, 2, &.{1});

    // All available by default
    try testing.expect(table.isAvailable(0x1000, 0));
    try testing.expect(table.isAvailable(0x1000, 1));

    // Mark one unavailable
    table.markUnavailable(0x1000, 0);
    try testing.expect(!table.isAvailable(0x1000, 0));
    try testing.expect(table.isAvailable(0x1000, 1));

    // Mark available again
    table.markAvailable(0x1000, 0);
    try testing.expect(table.isAvailable(0x1000, 0));
}

test "PartitionTable: mark node partitions unavailable" {
    var table = PartitionTable.init(testing.allocator, 1);
    defer table.deinit();

    // Node 2 leads two partitions
    try table.assign(0x1000, 0, 2, &.{});
    try table.assign(0x1000, 1, 2, &.{});
    try table.assign(0x1000, 2, 3, &.{});

    // Mark all of node 2's partitions unavailable
    const marked = table.markNodePartitionsUnavailable(2);
    try testing.expectEqual(@as(u32, 2), marked);

    try testing.expect(!table.isAvailable(0x1000, 0));
    try testing.expect(!table.isAvailable(0x1000, 1));
    try testing.expect(table.isAvailable(0x1000, 2)); // node 3's partition unaffected

    // Recover
    const recovered = table.markNodePartitionsAvailable(2);
    try testing.expectEqual(@as(u32, 2), recovered);
    try testing.expect(table.isAvailable(0x1000, 0));
}
