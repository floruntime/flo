//! Integration Test — Raft Leader Failover
//!
//! Simulates a 3-node Raft cluster using in-process RaftNode instances,
//! exercises leader election, log replication, leader crash, and
//! re-election with continued progress.

const std = @import("std");
const testing = std.testing;
const src = @import("src");

const RaftNode = src.raft.node.RaftNode;
const Role = src.raft.node.Role;
const NodeId = src.raft.node.NodeId;
const NO_VOTE = src.raft.node.NO_VOTE;
const Config = src.raft.node.Config;
const AppendRequest = src.raft.node.AppendRequest;
const entry_mod = src.storage.ual.entry;
const Entry = entry_mod.Entry;

const CLUSTER_SIZE = 3;
const LOG_CAPACITY = 16384;

const TestConfig = Config{
    .election_timeout_min_ms = 150,
    .election_timeout_max_ms = 300,
    .heartbeat_interval_ms = 50,
    .max_entries_per_batch = 64,
    .enable_pre_vote = false, // simpler for deterministic testing
};

/// A simulated 3-node cluster with in-process message passing.
const TestCluster = struct {
    nodes: [CLUSTER_SIZE]RaftNode,
    alive: [CLUSTER_SIZE]bool,

    fn init(allocator: std.mem.Allocator) !TestCluster {
        var cluster: TestCluster = undefined;
        cluster.alive = .{ true, true, true };

        // Node IDs: 1, 2, 3
        for (0..CLUSTER_SIZE) |i| {
            cluster.nodes[i] = try RaftNode.init(
                allocator,
                @intCast(i + 1),
                1000, // group_id
                LOG_CAPACITY,
                TestConfig,
            );
        }
        errdefer for (0..CLUSTER_SIZE) |i| cluster.nodes[i].deinit();

        // Wire peers: each node knows about the other two
        for (0..CLUSTER_SIZE) |i| {
            for (0..CLUSTER_SIZE) |j| {
                if (i != j) cluster.nodes[i].addPeer(@intCast(j + 1));
            }
        }

        return cluster;
    }

    fn deinit(self: *TestCluster) void {
        for (0..CLUSTER_SIZE) |i| self.nodes[i].deinit();
    }

    /// Run an election on a specific node and deliver votes from alive peers.
    /// Returns true if the node won the election.
    fn runElection(self: *TestCluster, candidate_idx: usize) bool {
        const vote_req = self.nodes[candidate_idx].startElection().?;

        for (0..CLUSTER_SIZE) |j| {
            if (j == candidate_idx) continue;
            if (!self.alive[j]) continue;

            const resp = self.nodes[j].handleVoteRequest(vote_req);
            if (self.nodes[candidate_idx].handleVoteResponse(resp)) {
                return true;
            }
        }
        return false;
    }

    /// Replicate entries from leader to all alive followers.
    /// Builds AppendRequest manually from the leader's log and peer state.
    fn replicate(self: *TestCluster, leader_idx: usize) !u32 {
        var rounds: u32 = 0;
        var entry_buf: [64]Entry = undefined;
        var payload_arena: [65536]u8 = undefined;
        const leader = &self.nodes[leader_idx];

        for (0..CLUSTER_SIZE) |j| {
            if (j == leader_idx) continue;
            if (!self.alive[j]) continue;

            const peer_idx = self.peerIndex(leader_idx, j) orelse continue;
            const peer = leader.peers[peer_idx];
            const last = leader.log.lastIndex();

            // Nothing to send if caught up
            if (peer.next_index > last) continue;

            // Build prev_log info for log matching
            const prev_index = peer.next_index - 1;
            const prev_term = if (prev_index == 0)
                @as(u64, 0)
            else
                leader.log.entryTerm(prev_index) orelse 0;

            // Read entries from next_index
            const count = leader.log.getRange(peer.next_index, &entry_buf, &payload_arena);
            if (count == 0) continue;

            const req = AppendRequest{
                .term = leader.current_term,
                .leader_id = leader.id,
                .prev_log_index = prev_index,
                .prev_log_term = prev_term,
                .entries = entry_buf[0..count],
                .leader_commit = leader.commit_index,
            };

            const resp = try self.nodes[j].handleAppendEntries(req);
            leader.handleAppendResponse(resp);
            rounds += 1;
        }
        return rounds;
    }

    /// Replicate repeatedly until all alive followers are caught up.
    /// Handles log backtracking (Raft next_index decrement on mismatch).
    fn replicateFull(self: *TestCluster, leader_idx: usize) !void {
        var iters: u32 = 0;
        while (iters < 20) : (iters += 1) {
            const rounds = try self.replicate(leader_idx);
            if (rounds == 0) break; // all caught up or no alive followers
        }
    }

    /// Find the peer index within a node's peer list for a given node.
    fn peerIndex(self: *const TestCluster, node_idx: usize, target_idx: usize) ?usize {
        const target_id: NodeId = @intCast(target_idx + 1);
        for (0..self.nodes[node_idx].peer_count) |i| {
            if (self.nodes[node_idx].peer_ids[i] == target_id) return i;
        }
        return null;
    }

    /// Find the index of the current leader (if any).
    fn findLeader(self: *const TestCluster) ?usize {
        for (0..CLUSTER_SIZE) |i| {
            if (self.alive[i] and self.nodes[i].role == .leader) return i;
        }
        return null;
    }

    /// Simulate killing a node (mark it as dead).
    fn kill(self: *TestCluster, idx: usize) void {
        self.alive[idx] = false;
    }

    /// Simulate restarting a node (mark it as alive, reset to follower).
    fn revive(self: *TestCluster, idx: usize) void {
        self.alive[idx] = true;
        // On restart a node doesn't know who the leader is
        self.nodes[idx].leader_id = NO_VOTE;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "integration: raft 3-node election" {
    var cluster = try TestCluster.init(testing.allocator);
    defer cluster.deinit();

    // Node 0 starts election — should win with votes from nodes 1 and 2
    const won = cluster.runElection(0);
    try testing.expect(won);
    try testing.expectEqual(Role.leader, cluster.nodes[0].role);
    try testing.expectEqual(@as(u64, 1), cluster.nodes[0].current_term);
}

test "integration: raft leader propose and replicate" {
    var cluster = try TestCluster.init(testing.allocator);
    defer cluster.deinit();

    // Elect node 0
    _ = cluster.runElection(0);
    try testing.expectEqual(Role.leader, cluster.nodes[0].role);

    // Propose 3 entries
    _ = try cluster.nodes[0].propose(.kv_put, 0, 0, "key1=val1");
    _ = try cluster.nodes[0].propose(.kv_put, 0, 0, "key2=val2");
    _ = try cluster.nodes[0].propose(.kv_put, 0, 0, "key3=val3");

    // Not committed yet (no replication)
    try testing.expectEqual(@as(u64, 0), cluster.nodes[0].commit_index);

    // Replicate to followers
    _ = try cluster.replicate(0);

    // After one round, majority acked — entries committed
    try testing.expectEqual(@as(u64, 3), cluster.nodes[0].commit_index);

    // Followers have the entries
    try testing.expectEqual(@as(u64, 3), cluster.nodes[1].log.lastIndex());
    try testing.expectEqual(@as(u64, 3), cluster.nodes[2].log.lastIndex());
}

test "integration: raft leader failover and re-election" {
    var cluster = try TestCluster.init(testing.allocator);
    defer cluster.deinit();

    // ── Phase 1: Elect node 0 as leader ────────────────────────────────
    _ = cluster.runElection(0);
    try testing.expectEqual(Role.leader, cluster.nodes[0].role);
    const original_term = cluster.nodes[0].current_term;

    // Propose and replicate some entries
    _ = try cluster.nodes[0].propose(.kv_put, 0, 0, "before-crash-1");
    _ = try cluster.nodes[0].propose(.kv_put, 0, 0, "before-crash-2");
    _ = try cluster.replicate(0);

    try testing.expectEqual(@as(u64, 2), cluster.nodes[0].commit_index);
    try testing.expectEqual(@as(u64, 2), cluster.nodes[1].log.lastIndex());
    try testing.expectEqual(@as(u64, 2), cluster.nodes[2].log.lastIndex());

    // ── Phase 2: Kill the leader ───────────────────────────────────────
    cluster.kill(0);

    // Verify no leader among alive nodes
    try testing.expect(cluster.findLeader() == null or cluster.findLeader().? == 0);

    // ── Phase 3: Node 1 starts election — should win ───────────────────
    const won = cluster.runElection(1);
    try testing.expect(won);
    try testing.expectEqual(Role.leader, cluster.nodes[1].role);

    // New term must be higher
    try testing.expect(cluster.nodes[1].current_term > original_term);

    // ── Phase 4: New leader proposes and commits ───────────────────────
    _ = try cluster.nodes[1].propose(.kv_put, 0, 0, "after-crash-1");
    _ = try cluster.replicate(1);

    // Committed: new leader + node 2 form majority (node 0 is dead)
    try testing.expectEqual(@as(u64, 3), cluster.nodes[1].commit_index);
    try testing.expectEqual(@as(u64, 3), cluster.nodes[2].log.lastIndex());

    // Dead node 0 still has old data
    try testing.expectEqual(@as(u64, 2), cluster.nodes[0].log.lastIndex());
}

test "integration: raft old leader rejoins as follower" {
    var cluster = try TestCluster.init(testing.allocator);
    defer cluster.deinit();

    // Elect node 0 and replicate some data
    _ = cluster.runElection(0);
    _ = try cluster.nodes[0].propose(.kv_put, 0, 0, "entry-1");
    _ = try cluster.replicate(0);

    // Kill node 0
    cluster.kill(0);

    // Node 1 wins election in higher term
    _ = cluster.runElection(1);
    try testing.expectEqual(Role.leader, cluster.nodes[1].role);
    const new_term = cluster.nodes[1].current_term;

    // Propose on new leader
    _ = try cluster.nodes[1].propose(.kv_put, 0, 0, "new-leader-entry");
    _ = try cluster.replicate(1);

    // ── Revive old leader ──────────────────────────────────────────────
    cluster.revive(0);

    // Old leader receives heartbeat from new leader — should step down
    const append_resp = try cluster.nodes[0].handleAppendEntries(.{
        .term = new_term,
        .leader_id = 2, // node 1's ID
        .prev_log_index = cluster.nodes[1].log.lastIndex() - 1,
        .prev_log_term = 1, // our entries were in term 1
        .entries = &[_]Entry{},
        .leader_commit = cluster.nodes[1].commit_index,
    });

    // Old leader accepted the new term
    try testing.expectEqual(new_term, cluster.nodes[0].current_term);
    try testing.expectEqual(Role.follower, cluster.nodes[0].role);
    try testing.expectEqual(@as(u32, 2), cluster.nodes[0].leader_id);
    try testing.expect(append_resp.success);
}

test "integration: raft no quorum blocks commit" {
    var cluster = try TestCluster.init(testing.allocator);
    defer cluster.deinit();

    // Elect node 0
    _ = cluster.runElection(0);

    // Kill both followers — leader has no quorum
    cluster.kill(1);
    cluster.kill(2);

    // Propose entry — should be appended to log but NOT committed
    _ = try cluster.nodes[0].propose(.kv_put, 0, 0, "lonely-entry");
    _ = try cluster.replicate(0); // no alive followers to replicate to

    try testing.expectEqual(@as(u64, 1), cluster.nodes[0].log.lastIndex());
    try testing.expectEqual(@as(u64, 0), cluster.nodes[0].commit_index); // stuck!

    // ── Revive one follower — now we have quorum again ─────────────────
    cluster.revive(1);
    _ = try cluster.replicate(0);

    // Now majority (leader + node 1) have the entry — committed
    try testing.expectEqual(@as(u64, 1), cluster.nodes[0].commit_index);
    try testing.expectEqual(@as(u64, 1), cluster.nodes[1].log.lastIndex());
}

test "integration: raft multiple term transitions" {
    var cluster = try TestCluster.init(testing.allocator);
    defer cluster.deinit();

    // ── Term 1: Node 0 leads ───────────────────────────────────────────
    _ = cluster.runElection(0);
    _ = try cluster.nodes[0].propose(.kv_put, 0, 0, "term1-data");
    _ = try cluster.replicate(0);
    try testing.expectEqual(@as(u64, 1), cluster.nodes[0].current_term);

    // ── Term 2: Node 0 fails, Node 1 takes over ───────────────────────
    cluster.kill(0);
    _ = cluster.runElection(1);
    try testing.expect(cluster.nodes[1].current_term > 1);
    _ = try cluster.nodes[1].propose(.kv_put, 0, 0, "term2-data");
    _ = try cluster.replicate(1);

    // ── Term 3: Node 1 fails, Node 2 takes over ───────────────────────
    cluster.kill(1);
    cluster.revive(0); // bring back node 0 so node 2 has a quorum partner

    // Node 2 has entries 1-2, node 0 has entry 1 only.
    // Node 2 starts election — node 0 must grant vote because node 2's
    // log is at least as up-to-date (last_term=2 > node0's last_term=1).
    _ = cluster.runElection(2);
    try testing.expectEqual(Role.leader, cluster.nodes[2].role);
    try testing.expect(cluster.nodes[2].current_term > cluster.nodes[1].current_term);

    _ = try cluster.nodes[2].propose(.kv_put, 0, 0, "term3-data");

    // Need multiple rounds: node 0 has stale log, backtracking required
    try cluster.replicateFull(2);

    // Node 2 committed the new entry (majority: node 2 + node 0)
    try testing.expect(cluster.nodes[2].commit_index > 0);

    // Three different terms observed
    try testing.expect(cluster.nodes[2].current_term >= 3);
}
