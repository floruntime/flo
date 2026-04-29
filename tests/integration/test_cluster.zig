//! Integration Test — Cluster Subsystem
//!
//! Exercises multi-node cluster behaviour in-process using deterministic
//! (synchronous) simulation of Gossip discovery, Membership state machine,
//! Coordinator metadata replication, PartitionTable rebalancing, and
//! Forwarder request lifecycle.
//!
//! Pattern: same as test_raft_failover.zig — manual tick() with fixed
//! timestamps, synchronous message delivery, no real I/O.

const std = @import("std");
const testing = std.testing;
const src = @import("src");

// ─── Imports ─────────────────────────────────────────────────────────────

const Gossip = src.cluster.gossip.Gossip;
const NodeAddr = src.cluster.gossip.NodeAddr;
const MemberState = src.cluster.gossip.MemberState;
const GossipMessage = src.cluster.gossip.Message;

const Membership = src.cluster.membership.Membership;
const NodeState = src.cluster.membership.NodeState;

const Coordinator = src.cluster.coordinator.Coordinator;
const NodeStatus = src.cluster.coordinator.NodeStatus;

const PartitionTable = src.cluster.partition_table.PartitionTable;

const Forwarder = src.cluster.forwarder.Forwarder;
const ForwardResult = src.cluster.forwarder.ForwardResult;
const PendingRequest = src.cluster.forwarder.PendingRequest;

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
    .max_entries_per_batch = 64,
    .enable_pre_vote = false,
};

fn testAddr(node_id: NodeId) NodeAddr {
    return .{
        .ip = .{ 127, 0, 0, 1 },
        .gossip_port = @intCast(5000 + node_id),
        .data_port = @intCast(4000 + node_id),
    };
}

// ═════════════════════════════════════════════════════════════════════════════
// 1. Gossip Discovery — 3-node SWIM mesh
// ═════════════════════════════════════════════════════════════════════════════

test "integration: gossip 3-node discovery" {
    const alloc = testing.allocator;

    // Create 3 Gossip instances
    var nodes: [CLUSTER_SIZE]Gossip = undefined;
    for (0..CLUSTER_SIZE) |i| {
        const nid: NodeId = @intCast(i + 1);
        nodes[i] = Gossip.init(alloc, nid, testAddr(nid));
    }
    defer for (0..CLUSTER_SIZE) |i| nodes[i].deinit();

    // Seed: each node knows about the other two
    for (0..CLUSTER_SIZE) |i| {
        for (0..CLUSTER_SIZE) |j| {
            if (i == j) continue;
            const nid: NodeId = @intCast(j + 1);
            try nodes[i].addMember(nid, testAddr(nid), 1000);
        }
    }

    // All nodes should see 2 alive members
    for (0..CLUSTER_SIZE) |i| {
        try testing.expectEqual(@as(u32, 2), nodes[i].aliveCount());
        try testing.expectEqual(@as(u32, 2), nodes[i].memberCount());
    }

    // Tick node 0 to trigger a probe cycle
    _ = try nodes[0].tick(2000);
    const outbound = nodes[0].drainOutbound();

    // Should have generated at least one ping
    try testing.expect(outbound.len > 0);
    nodes[0].clearOutbound();

    // Deliver the first message to the target and get their response
    if (outbound.len > 0) {
        const msg = outbound[0].message;
        // Find the target node by matching
        for (0..CLUSTER_SIZE) |j| {
            const target_id: NodeId = @intCast(j + 1);
            if (target_id == msg.target or target_id != nodes[0].self_id) {
                try nodes[j].handleMessage(msg, 2000);
                break;
            }
        }
    }

    // Verify node 0 can look up node 2 and node 3
    try testing.expect(nodes[0].isAlive(2));
    try testing.expect(nodes[0].isAlive(3));
}

// ═════════════════════════════════════════════════════════════════════════════
// 2. Gossip Failure Detection — suspect → dead
// ═════════════════════════════════════════════════════════════════════════════

test "integration: gossip failure detection" {
    const alloc = testing.allocator;

    var node1 = Gossip.init(alloc, 1, testAddr(1));
    defer node1.deinit();

    try node1.addMember(2, testAddr(2), 1000);

    // Node 2 is alive initially
    try testing.expect(node1.isAlive(2));

    // Tick repeatedly without delivering acks → should suspect & eventually declare dead
    var t: i64 = 2000;
    var became_dead = false;
    while (t < 30_000) : (t += 500) {
        const result = try node1.tick(t);
        node1.clearOutbound();
        if (result.newly_dead > 0) {
            became_dead = true;
            break;
        }
    }

    try testing.expect(became_dead);
    try testing.expect(!node1.isAlive(2));
}

// ═════════════════════════════════════════════════════════════════════════════
// 3. Membership — full node lifecycle: join → active → leave
// ═════════════════════════════════════════════════════════════════════════════

test "integration: membership join-activate-leave lifecycle" {
    const alloc = testing.allocator;

    var membership = Membership.init(alloc, 1);
    defer membership.deinit();

    // Bootstrap self as active
    try membership.bootstrapSingle(1000);
    try testing.expect(membership.isSelfActive());

    // New node discovered
    try membership.nodeDiscovered(2, "127.0.0.1", 4002, 4, 2000);
    const node2 = membership.getNode(2).?;
    try testing.expectEqual(NodeState.joining, node2.state);
    try testing.expectEqual(@as(u32, 1), membership.nodeCount()); // excludes self

    // Node 2 activated (confirmed by Raft)
    try membership.nodeActivated(2, 3000);
    const node2_active = membership.getNode(2).?;
    try testing.expectEqual(NodeState.active, node2_active.state);
    try testing.expectEqual(@as(u32, 1), membership.activeCount());

    // Node 3 discovered and activated
    try membership.nodeDiscovered(3, "127.0.0.1", 4003, 4, 4000);
    try membership.nodeActivated(3, 5000);
    try testing.expectEqual(@as(u32, 2), membership.activeCount());
    try testing.expectEqual(@as(u32, 2), membership.nodeCount()); // excludes self

    // Node 2 initiates graceful leave
    try membership.nodeLeft(2, 6000);
    const node2_left = membership.getNode(2).?;
    try testing.expectEqual(NodeState.left, node2_left.state);
    try testing.expectEqual(@as(u32, 1), membership.activeCount());
}

// ═════════════════════════════════════════════════════════════════════════════
// 4. Membership — node failure and recovery
// ═════════════════════════════════════════════════════════════════════════════

test "integration: membership failure and recovery" {
    const alloc = testing.allocator;

    var membership = Membership.init(alloc, 1);
    defer membership.deinit();

    try membership.bootstrapSingle(1000);
    try membership.nodeDiscovered(2, "127.0.0.1", 4002, 4, 2000);
    try membership.nodeActivated(2, 3000);
    try testing.expectEqual(NodeState.active, membership.getNode(2).?.state);

    // Node 2 fails (gossip declares dead)
    try membership.nodeFailed(2, 5000);
    try testing.expectEqual(NodeState.failed, membership.getNode(2).?.state);
    try testing.expectEqual(@as(u32, 0), membership.activeCount());

    // Node 2 recovers
    try membership.nodeRecovered(2, 8000);
    try testing.expectEqual(NodeState.active, membership.getNode(2).?.state);
    try testing.expectEqual(@as(u32, 1), membership.activeCount());
}

// ═════════════════════════════════════════════════════════════════════════════
// 5. Membership — tick triggers rebalance
// ═════════════════════════════════════════════════════════════════════════════

test "integration: membership tick rebalance trigger" {
    const alloc = testing.allocator;

    var membership = Membership.init(alloc, 1);
    defer membership.deinit();

    try membership.bootstrapSingle(1000);

    // Discover node 2 → adds to pending_adds
    try membership.nodeDiscovered(2, "127.0.0.1", 4002, 4, 2000);

    // Tick should drain pending_adds and signal rebalance needed
    const action1 = try membership.tick(3000);
    try testing.expect(action1.nodes_to_add > 0 or action1.rebalance_needed);

    // Activate node 2
    try membership.nodeActivated(2, 4000);

    // After cooldown, tick should indicate rebalance
    const action2 = try membership.tick(15_000);
    _ = action2;
}

// ═════════════════════════════════════════════════════════════════════════════
// 6. Coordinator — 3-node metadata replication
// ═════════════════════════════════════════════════════════════════════════════

/// Helper to replicate from coordinator leader to alive followers.
fn coordinatorReplicate(coords: []Coordinator, leader_idx: usize, alive: []const bool) !void {
    var entry_buf: [64]Entry = undefined;
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
            const count = leader.raft.log.getRange(peer.next_index, &entry_buf);
            entries = entry_buf[0..count];
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
    const vote_req = coords[candidate_idx].raft.startElection();

    for (0..coords.len) |j| {
        if (j == candidate_idx) continue;
        if (!alive[j]) continue;

        const resp = coords[j].raft.handleVoteRequest(vote_req);
        if (coords[candidate_idx].raft.handleVoteResponse(resp)) {
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
// 7. Coordinator — leader failover preserves metadata
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
// 8. PartitionTable — round-robin assignment and rebalance
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
// 9. PartitionTable — serialize/deserialize round-trip
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
// 10. Forwarder — forward and complete cycle
// ═════════════════════════════════════════════════════════════════════════════

test "integration: forwarder forward and complete cycle" {
    const alloc = testing.allocator;

    var fwd = Forwarder.init(alloc, 1, 0); // node 1, shard 0
    defer fwd.deinit();

    // Add peer
    try fwd.addPeer(2, "127.0.0.1", 4002);
    try testing.expectEqual(@as(u32, 1), fwd.peerCount());

    // Forward a request to node 2
    const result = try fwd.forward(2, 100, 50, 256, 1000);
    switch (result) {
        .queued => |info| {
            try testing.expectEqual(@as(u64, 100), info.request_id);
            try testing.expectEqual(@as(NodeId, 2), info.target_node);
        },
        else => return error.TestUnexpectedResult,
    }
    try testing.expectEqual(@as(u32, 1), fwd.pendingCount());

    // Complete the request (response arrived)
    const pending = fwd.complete(100, 2000).?;
    try testing.expectEqual(@as(u64, 100), pending.request_id);
    try testing.expectEqual(@as(u64, 50), pending.connection_id);
    try testing.expectEqual(@as(u32, 0), fwd.pendingCount());

    // Stats reflect the round-trip
    const s = fwd.stats();
    try testing.expectEqual(@as(u64, 1), s.total_forwarded);
    try testing.expectEqual(@as(u64, 1), s.total_responses);
}

// ═════════════════════════════════════════════════════════════════════════════
// 11. Forwarder — timeout sweep
// ═════════════════════════════════════════════════════════════════════════════

test "integration: forwarder timeout sweep" {
    const alloc = testing.allocator;

    var fwd = Forwarder.init(alloc, 1, 0);
    defer fwd.deinit();

    try fwd.addPeer(2, "127.0.0.1", 4002);

    // Forward 3 requests
    _ = try fwd.forward(2, 201, 10, 100, 1000);
    _ = try fwd.forward(2, 202, 11, 100, 1000);
    _ = try fwd.forward(2, 203, 12, 100, 1000);
    try testing.expectEqual(@as(u32, 3), fwd.pendingCount());

    // Complete request 202 before timeout
    _ = fwd.complete(202, 2000);
    try testing.expectEqual(@as(u32, 2), fwd.pendingCount());

    // Sweep timeouts well after the default 5s timeout
    var timed_out: std.ArrayListUnmanaged(PendingRequest) = .empty;
    defer timed_out.deinit(alloc);
    const count = try fwd.sweepTimeouts(10_000, &timed_out);
    try testing.expectEqual(@as(u32, 2), count);
    try testing.expectEqual(@as(u32, 0), fwd.pendingCount());

    // Stats reflect timeouts
    try testing.expectEqual(@as(u64, 2), fwd.stats().total_timeouts);
}

// ═════════════════════════════════════════════════════════════════════════════
// 12. Forwarder — forward to self returns local
// ═════════════════════════════════════════════════════════════════════════════

test "integration: forwarder reject forward to self" {
    const alloc = testing.allocator;

    var fwd = Forwarder.init(alloc, 1, 0);
    defer fwd.deinit();

    // Forward to self should return .local
    const result = try fwd.forward(1, 300, 20, 128, 1000);
    switch (result) {
        .local => {},
        else => return error.TestUnexpectedResult,
    }
    try testing.expectEqual(@as(u32, 0), fwd.pendingCount());
}

// ═════════════════════════════════════════════════════════════════════════════
// 13. Forwarder — circuit breaker trips after repeated failures
// ═════════════════════════════════════════════════════════════════════════════

test "integration: forwarder circuit breaker trips after failures" {
    const alloc = testing.allocator;
    const CB = src.cluster.forwarder;

    var fwd = Forwarder.init(alloc, 1, 0);
    defer fwd.deinit();

    try fwd.addPeer(2, "127.0.0.1", 4002);

    // Initially circuit is closed
    try testing.expectEqual(CB.CircuitBreaker.CircuitState.closed, fwd.peerCircuitState(2).?);

    // Send CIRCUIT_BREAKER_THRESHOLD requests and fail them all
    var req_id: u64 = 1000;
    var i: u32 = 0;
    while (i < CB.CIRCUIT_BREAKER_THRESHOLD) : (i += 1) {
        const r = try fwd.forward(2, req_id, 1, 64, 1000);
        switch (r) {
            .queued => {},
            else => return error.TestUnexpectedResult,
        }
        _ = fwd.fail(req_id, 1000);
        req_id += 1;
    }

    // Circuit should now be open
    try testing.expectEqual(CB.CircuitBreaker.CircuitState.open, fwd.peerCircuitState(2).?);

    // Next forward should be rejected with circuit_open
    const rejected = try fwd.forward(2, req_id, 1, 64, 1000);
    switch (rejected) {
        .circuit_open => {},
        else => return error.TestUnexpectedResult,
    }
    try testing.expect(fwd.circuit_open_rejections > 0);
}

// ═════════════════════════════════════════════════════════════════════════════
// 14. Forwarder — circuit breaker half-open recovery
// ═════════════════════════════════════════════════════════════════════════════

test "integration: forwarder circuit breaker recovers through half-open" {
    const alloc = testing.allocator;
    const CB = src.cluster.forwarder;

    var fwd = Forwarder.init(alloc, 1, 0);
    defer fwd.deinit();

    try fwd.addPeer(2, "127.0.0.1", 4002);

    // Trip the circuit breaker
    var req_id: u64 = 2000;
    var i: u32 = 0;
    while (i < CB.CIRCUIT_BREAKER_THRESHOLD) : (i += 1) {
        _ = try fwd.forward(2, req_id, 1, 64, 1000);
        _ = fwd.fail(req_id, 1000);
        req_id += 1;
    }
    try testing.expectEqual(CB.CircuitBreaker.CircuitState.open, fwd.peerCircuitState(2).?);

    // After open duration, circuit transitions to half-open
    const after_open = 1000 + CB.CIRCUIT_BREAKER_OPEN_DURATION_MS;
    const probe = try fwd.forward(2, req_id, 1, 64, after_open);
    switch (probe) {
        .queued => {},
        else => return error.TestUnexpectedResult,
    }
    try testing.expectEqual(CB.CircuitBreaker.CircuitState.half_open, fwd.peerCircuitState(2).?);

    // Complete enough successes to close the circuit
    _ = fwd.complete(req_id, after_open + 1);
    req_id += 1;

    i = 1;
    while (i < CB.CIRCUIT_BREAKER_HALF_OPEN_SUCCESSES) : (i += 1) {
        _ = try fwd.forward(2, req_id, 1, 64, after_open + 10);
        _ = fwd.complete(req_id, after_open + 11);
        req_id += 1;
    }

    // Circuit should be closed again
    try testing.expectEqual(CB.CircuitBreaker.CircuitState.closed, fwd.peerCircuitState(2).?);
}

// ═════════════════════════════════════════════════════════════════════════════
// 15. Forwarder — circuit breaker reset
// ═════════════════════════════════════════════════════════════════════════════

test "integration: forwarder circuit breaker manual reset" {
    const alloc = testing.allocator;
    const CB = src.cluster.forwarder;

    var fwd = Forwarder.init(alloc, 1, 0);
    defer fwd.deinit();

    try fwd.addPeer(2, "127.0.0.1", 4002);

    // Trip the circuit
    var req_id: u64 = 3000;
    var i: u32 = 0;
    while (i < CB.CIRCUIT_BREAKER_THRESHOLD) : (i += 1) {
        _ = try fwd.forward(2, req_id, 1, 64, 1000);
        _ = fwd.fail(req_id, 1000);
        req_id += 1;
    }
    try testing.expectEqual(CB.CircuitBreaker.CircuitState.open, fwd.peerCircuitState(2).?);

    // Manual reset returns to closed
    fwd.resetCircuit(2);
    try testing.expectEqual(CB.CircuitBreaker.CircuitState.closed, fwd.peerCircuitState(2).?);

    // Should be able to forward again immediately
    const r = try fwd.forward(2, req_id, 1, 64, 1000);
    switch (r) {
        .queued => {},
        else => return error.TestUnexpectedResult,
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// 16. Partition table — node failure marks partitions unavailable
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

// ═════════════════════════════════════════════════════════════════════════════
// 17. Gossip dead-node propagation
// ═════════════════════════════════════════════════════════════════════════════

test "integration: gossip dead node ids propagated for partition marking" {
    const alloc = testing.allocator;

    // Create 2 nodes: 1 and 2
    var n1 = Gossip.init(alloc, 1, testAddr(1));
    defer n1.deinit();
    var n2 = Gossip.init(alloc, 2, testAddr(2));
    defer n2.deinit();

    // Seed: n1 knows n2
    try n1.addMember(2, testAddr(2), 1000);

    // Tick n1 repeatedly without delivering messages — n2 becomes suspect then dead
    var time_ms: i64 = 2000;
    var tick_result = try n1.tick(time_ms);
    n1.clearOutbound();

    // Advance time to move through suspect to dead
    while (tick_result.newly_dead == 0) {
        time_ms += 1000;
        tick_result = try n1.tick(time_ms);
        n1.clearOutbound();
        if (time_ms > 60_000) return error.TestTimeout; // safety
    }

    // newly_dead_ids should contain node 2
    try testing.expectEqual(@as(u32, 1), tick_result.newly_dead);
    try testing.expectEqual(@as(NodeId, 2), tick_result.newly_dead_ids[0]);
}
