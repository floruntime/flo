//! Integration Test — Cluster Subsystem
//!
//! Exercises multi-node cluster behaviour in-process using deterministic
//! (synchronous) simulation of Coordinator metadata replication and
//! PartitionTable rebalancing.
//!
//! Pattern: same as test_raft_failover.zig — manual tick() with fixed
//! timestamps, synchronous message delivery, no real I/O.

const std = @import("std");
const testing = std.testing;
const src = @import("src");

// ─── Imports ─────────────────────────────────────────────────────────────

const Coordinator = src.cluster.coordinator.Coordinator;
const NodeStatus = src.cluster.coordinator.NodeStatus;

const PartitionTable = src.cluster.partition_table.PartitionTable;

const RaftNode = src.raft.node.RaftNode;
const NodeId = src.raft.node.NodeId;
const Config = src.raft.node.Config;
const AppendRequest = src.raft.node.AppendRequest;
const Entry = src.storage.ual.entry.Entry;

// ─── Shared constants ────────────────────────────────────────────────────

const CLUSTER_SIZE = 3;
const RAFT_CONFIG = Config{
    .election_timeout_min_ms = 150,
    .election_timeout_max_ms = 300,
    .heartbeat_interval_ms = 50,
    .enable_pre_vote = false,
};

// ═════════════════════════════════════════════════════════════════════════════
// 1. Coordinator — 3-node metadata replication
// ═════════════════════════════════════════════════════════════════════════════

/// Helper to replicate from coordinator leader to alive followers.
fn coordinatorReplicate(coords: []Coordinator, leader_idx: usize, alive: []const bool) !void {
    var entry_buf: [64]Entry = undefined;
    var payload_arena: [65536]u8 = undefined;
    const leader = &coords[leader_idx];

    for (0..coords.len) |j| {
        if (j == leader_idx) continue;
        if (!alive[j]) continue;

        const peer_idx = blk: {
            const target_id: NodeId = @intCast(j + 1);
            for (0..leader.raft.peer_count) |pi| {
                if (leader.raft.peer_ids[pi] == target_id) break :blk pi;
            }
            continue; // peer not found
        };

        const peer = leader.raft.peers[peer_idx];
        const last = leader.raft.log.lastIndex();

        var entries: []Entry = &.{};
        var prev_index: u64 = last;
        var prev_term: u64 = 0;

        if (peer.next_index <= last) {
            prev_index = peer.next_index - 1;
            prev_term = if (prev_index == 0) @as(u64, 0) else leader.raft.log.entryTerm(prev_index) orelse 0;
            const count = leader.raft.log.getRange(peer.next_index, &entry_buf, &payload_arena);
            entries = entry_buf[0..count];
            // As the leader loop does: an ack only counts up to what was sent.
            leader.raft.peers[peer_idx].sent_up_to = @max(leader.raft.peers[peer_idx].sent_up_to, peer.next_index + count - 1);
        } else {
            // Heartbeat: no new entries, but send leader_commit to advance follower
            prev_term = if (last == 0) @as(u64, 0) else leader.raft.log.entryTerm(last) orelse 0;
        }

        const req = AppendRequest{
            .term = leader.raft.current_term,
            .leader_id = leader.raft.id,
            .prev_log_index = prev_index,
            .prev_log_term = prev_term,
            .entries = entries,
            .leader_commit = leader.raft.commit_index,
        };

        const resp = try coords[j].raft.handleAppendEntries(req);
        leader.raft.handleAppendResponse(resp);
    }
}

/// Run controller election for a candidate.
fn coordinatorElection(coords: []Coordinator, candidate_idx: usize, alive: []const bool) bool {
    const vote_req = coords[candidate_idx].raft.startElectionNow().?;

    for (0..coords.len) |j| {
        if (j == candidate_idx) continue;
        if (!alive[j]) continue;

        const resp = coords[j].raft.handleVoteRequest(vote_req);
        if (coords[candidate_idx].raft.handleVoteResponse(resp) == .won) {
            return true;
        }
    }
    return false;
}

test "integration: coordinator namespace replication across 3 nodes" {
    const alloc = testing.allocator;

    // Create 3 coordinators
    var coords: [CLUSTER_SIZE]Coordinator = undefined;
    for (0..CLUSTER_SIZE) |i| {
        coords[i] = try Coordinator.init(alloc, @intCast(i + 1), RAFT_CONFIG);
    }
    defer for (0..CLUSTER_SIZE) |i| coords[i].deinit();

    // Wire peers
    for (0..CLUSTER_SIZE) |i| {
        for (0..CLUSTER_SIZE) |j| {
            if (i != j) coords[i].addPeer(@intCast(j + 1));
        }
    }

    var alive = [_]bool{ true, true, true };

    // Elect node 1 as leader
    const won = coordinatorElection(&coords, 0, &alive);
    try testing.expect(won);
    try testing.expect(coords[0].raft.role == .leader);
    _ = coords[0].tick(100); // sync is_leader from raft.role

    // Propose namespace creation
    _ = try coords[0].proposeCreateNamespace("events", 16, 2);
    _ = try coords[0].proposeAddNode(1, "127.0.0.1", 4001, 4);
    _ = try coords[0].proposeAddNode(2, "127.0.0.1", 4002, 4);

    // Replicate to followers (may need multiple rounds)
    for (0..5) |_| {
        try coordinatorReplicate(&coords, 0, &alive);
    }

    // Apply committed entries on leader
    const applied_leader = try coords[0].applyCommitted();
    try testing.expect(applied_leader > 0);

    // Verify leader has the namespace
    try testing.expect(coords[0].getNamespace("events") != null);
    const ns = coords[0].getNamespace("events").?;
    try testing.expectEqual(@as(u16, 16), ns.partition_count);
    try testing.expectEqual(@as(u8, 2), ns.replication_factor);

    // Apply committed entries on followers
    for (1..CLUSTER_SIZE) |i| {
        _ = try coords[i].applyCommitted();
    }

    // All followers should also have the namespace
    for (1..CLUSTER_SIZE) |i| {
        try testing.expect(coords[i].getNamespace("events") != null);
        try testing.expectEqual(@as(u16, 16), coords[i].getNamespace("events").?.partition_count);
    }

    // All nodes should see 2 registered nodes
    try testing.expect(coords[0].nodeCount() >= 2);
}

// ═════════════════════════════════════════════════════════════════════════════
// 2. Coordinator — leader failover preserves metadata
// ═════════════════════════════════════════════════════════════════════════════

test "integration: coordinator leader failover preserves metadata" {
    const alloc = testing.allocator;

    var coords: [CLUSTER_SIZE]Coordinator = undefined;
    for (0..CLUSTER_SIZE) |i| {
        coords[i] = try Coordinator.init(alloc, @intCast(i + 1), RAFT_CONFIG);
    }
    defer for (0..CLUSTER_SIZE) |i| coords[i].deinit();

    for (0..CLUSTER_SIZE) |i| {
        for (0..CLUSTER_SIZE) |j| {
            if (i != j) coords[i].addPeer(@intCast(j + 1));
        }
    }

    var alive = [_]bool{ true, true, true };

    // Elect node 1, propose a namespace, replicate
    _ = coordinatorElection(&coords, 0, &alive);
    _ = coords[0].tick(100); // sync is_leader
    _ = try coords[0].proposeCreateNamespace("orders", 32, 3);

    for (0..5) |_| {
        try coordinatorReplicate(&coords, 0, &alive);
    }
    for (0..CLUSTER_SIZE) |i| {
        _ = try coords[i].applyCommitted();
    }

    // Verify all nodes have "orders"
    for (0..CLUSTER_SIZE) |i| {
        try testing.expect(coords[i].getNamespace("orders") != null);
    }

    // Kill node 1 (leader)
    alive[0] = false;

    // Elect node 2 as new leader
    const won2 = coordinatorElection(&coords, 1, &alive);
    try testing.expect(won2);
    try testing.expect(coords[1].raft.role == .leader);
    _ = coords[1].tick(200); // sync is_leader

    // Propose a second namespace on the new leader
    _ = try coords[1].proposeCreateNamespace("metrics", 8, 1);

    for (0..5) |_| {
        try coordinatorReplicate(&coords, 1, &alive);
    }
    for (1..CLUSTER_SIZE) |i| {
        _ = try coords[i].applyCommitted();
    }

    // Node 2 and 3 should have both namespaces
    try testing.expect(coords[1].getNamespace("orders") != null);
    try testing.expect(coords[1].getNamespace("metrics") != null);
    try testing.expect(coords[2].getNamespace("orders") != null);
    try testing.expect(coords[2].getNamespace("metrics") != null);

    // Old leader (node 1) still has "orders" but not "metrics" (was dead)
    try testing.expect(coords[0].getNamespace("orders") != null);
    try testing.expect(coords[0].getNamespace("metrics") == null);
}

// ═════════════════════════════════════════════════════════════════════════════
// 3. PartitionTable — round-robin assignment and rebalance
// ═════════════════════════════════════════════════════════════════════════════

test "integration: partition table round-robin assignment" {
    const alloc = testing.allocator;

    var table = PartitionTable.init(alloc, 1);
    defer table.deinit();

    // Assign 12 partitions across 3 nodes, replication factor 2
    const ns_hash: u32 = 0xABCD;
    try table.assignNamespace(ns_hash, 12, &.{ 1, 2, 3 }, 2);

    // Each node should lead ~4 partitions (12 / 3)
    try testing.expectEqual(@as(u32, 4), table.countLeaderPartitions(1));
    try testing.expectEqual(@as(u32, 4), table.countLeaderPartitions(2));
    try testing.expectEqual(@as(u32, 4), table.countLeaderPartitions(3));

    // Each node should be involved in leader + replica assignments
    const node1_parts = table.partitionsForNode(1);
    const node2_parts = table.partitionsForNode(2);
    const node3_parts = table.partitionsForNode(3);

    // With repl_factor=2, each partition has 1 leader + 1 replica.
    // Total role assignments = 12 * 2 = 24, spread across 3 nodes → ~8 each
    try testing.expectEqual(@as(usize, 8), node1_parts.len);
    try testing.expectEqual(@as(usize, 8), node2_parts.len);
    try testing.expectEqual(@as(usize, 8), node3_parts.len);

    // Verify each partition has a valid leader and one replica
    var p: u16 = 0;
    while (p < 12) : (p += 1) {
        const result = table.lookup(ns_hash, p).?;
        try testing.expect(result.leader >= 1 and result.leader <= 3);
        try testing.expectEqual(@as(u8, 1), result.replica_count);
        // Replica should differ from leader
        try testing.expect(result.replicas[0] != result.leader);
    }
}

test "integration: partition table node removal reassignment" {
    const alloc = testing.allocator;

    var table = PartitionTable.init(alloc, 1);
    defer table.deinit();

    const ns_hash: u32 = 0x1234;
    try table.assignNamespace(ns_hash, 6, &.{ 1, 2, 3 }, 1);

    // Node 3 is leaving — reassign its partitions to remaining nodes.
    // Copy the slice first because partitionsForNode returns a view into
    // internal storage that gets invalidated when we call table.assign().
    const node3_parts_view = table.partitionsForNode(3);
    const node3_count = node3_parts_view.len;
    try testing.expect(node3_count > 0);
    const node3_parts = try alloc.dupe(@TypeOf(node3_parts_view[0]), node3_parts_view);
    defer alloc.free(node3_parts);

    // Manually reassign node 3's partitions to nodes 1 and 2
    // (This simulates what the coordinator would do on node removal)
    var reassigned: u32 = 0;
    for (node3_parts) |pkey| {
        const unpacked = PartitionTable.unpackKey(pkey);
        const assignment = table.getAssignment(unpacked.namespace_hash, unpacked.partition_id).?;
        if (assignment.leader == 3) {
            // Move leadership to next available node
            const new_leader: NodeId = if (reassigned % 2 == 0) 1 else 2;
            try table.assign(unpacked.namespace_hash, unpacked.partition_id, new_leader, &.{});
            reassigned += 1;
        }
    }

    // Node 3 should have no leader partitions now
    try testing.expectEqual(@as(u32, 0), table.countLeaderPartitions(3));

    // Total leader partitions should still be 6 (all reassigned)
    const total = table.countLeaderPartitions(1) + table.countLeaderPartitions(2);
    try testing.expectEqual(@as(u32, 6), total);
}

// ═════════════════════════════════════════════════════════════════════════════
// 4. PartitionTable — serialize/deserialize round-trip
// ═════════════════════════════════════════════════════════════════════════════

test "integration: partition table serialize deserialize" {
    const alloc = testing.allocator;

    var table1 = PartitionTable.init(alloc, 1);
    defer table1.deinit();

    // Assign some partitions
    try table1.assign(0xAABB, 0, 1, &.{2});
    try table1.assign(0xAABB, 1, 2, &.{3});
    try table1.assign(0xCCDD, 0, 3, &.{ 1, 2 });

    // Serialize
    const data = try table1.serialize(alloc);
    defer alloc.free(data);

    // Deserialize into a fresh table
    var table2 = PartitionTable.init(alloc, 1);
    defer table2.deinit();
    try table2.deserialize(data);

    // Verify all assignments match
    const r1 = table2.lookup(0xAABB, 0).?;
    try testing.expectEqual(@as(NodeId, 1), r1.leader);
    try testing.expectEqual(@as(u8, 1), r1.replica_count);
    try testing.expectEqual(@as(NodeId, 2), r1.replicas[0]);

    const r2 = table2.lookup(0xAABB, 1).?;
    try testing.expectEqual(@as(NodeId, 2), r2.leader);

    const r3 = table2.lookup(0xCCDD, 0).?;
    try testing.expectEqual(@as(NodeId, 3), r3.leader);
    try testing.expectEqual(@as(u8, 2), r3.replica_count);
}

// ═════════════════════════════════════════════════════════════════════════════
// 11. Partition table — node failure marks partitions unavailable
// ═════════════════════════════════════════════════════════════════════════════

test "integration: partition table node failure marks unavailable" {
    const alloc = testing.allocator;

    var pt = PartitionTable.init(alloc, 0); // local_node_id = 0
    defer pt.deinit();

    const ns_hash: u32 = 0x1234;
    const no_replicas: []const NodeId = &.{};

    // Assign 4 partitions across 2 nodes
    try pt.assign(ns_hash, 0, 1, no_replicas); // partition 0 → node 1
    try pt.assign(ns_hash, 1, 1, no_replicas); // partition 1 → node 1
    try pt.assign(ns_hash, 2, 2, no_replicas); // partition 2 → node 2
    try pt.assign(ns_hash, 3, 2, no_replicas); // partition 3 → node 2

    // All partitions start available
    try testing.expect(pt.isAvailable(ns_hash, 0));
    try testing.expect(pt.isAvailable(ns_hash, 1));
    try testing.expect(pt.isAvailable(ns_hash, 2));
    try testing.expect(pt.isAvailable(ns_hash, 3));

    // Node 1 fails — mark all its partitions unavailable
    const marked = pt.markNodePartitionsUnavailable(1);
    try testing.expectEqual(@as(u32, 2), marked);

    // Node 1 partitions are unavailable, node 2 still fine
    try testing.expect(!pt.isAvailable(ns_hash, 0));
    try testing.expect(!pt.isAvailable(ns_hash, 1));
    try testing.expect(pt.isAvailable(ns_hash, 2));
    try testing.expect(pt.isAvailable(ns_hash, 3));

    // Node 1 recovers — restore availability
    const restored = pt.markNodePartitionsAvailable(1);
    try testing.expectEqual(@as(u32, 2), restored);

    try testing.expect(pt.isAvailable(ns_hash, 0));
    try testing.expect(pt.isAvailable(ns_hash, 1));
}
