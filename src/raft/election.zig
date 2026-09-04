//! Raft Election — Pre-vote protocol, randomized timeouts, election lifecycle.
//!
//! The ElectionManager orchestrates the full election lifecycle:
//!   1. Pre-vote (optional): candidate polls peers without incrementing term.
//!      If quorum grants pre-vote, proceed to real election.
//!   2. Real election: candidate increments term, votes for self, solicits votes.
//!   3. Win: became leader. Lose: step down (higher term), or timeout (retry).
//!
//! Pre-vote prevents a partitioned/isolated node from incrementing its term
//! endlessly and disrupting the cluster on rejoin.

const std = @import("std");
const Allocator = std.mem.Allocator;
const node_mod = @import("node.zig");
const raft_log = @import("log.zig");
const entry_mod = @import("../storage/ual/entry.zig");

const RaftNode = node_mod.RaftNode;
const VoteRequest = node_mod.VoteRequest;
const VoteResponse = node_mod.VoteResponse;
const Config = node_mod.Config;
const Role = node_mod.Role;
const NodeId = node_mod.NodeId;
const NO_VOTE = node_mod.NO_VOTE;

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

pub const ElectionPhase = enum(u8) {
    idle = 0,
    pre_vote = 1,
    voting = 2,
};

pub const ElectionResult = enum(u8) {
    pending = 0,
    won = 1,
    lost = 2,
    advance_to_vote = 3,
};

// ═══════════════════════════════════════════════════════════════════════════════
// ElectionManager
// ═══════════════════════════════════════════════════════════════════════════════

pub const ElectionManager = struct {
    node: *RaftNode,
    phase: ElectionPhase,

    // Pre-vote tracking
    pre_votes_received: u8,
    pre_votes_needed: u8,

    // Stats
    pre_vote_rounds: u64,
    pre_vote_successes: u64,
    pre_vote_failures: u64,

    /// `seed` reseeds the node's timer jitter so a simulation is reproducible.
    pub fn init(node: *RaftNode, seed: u64) ElectionManager {
        node.rng = std.Random.DefaultPrng.init(seed);
        return .{
            .node = node,
            .phase = .idle,
            .pre_votes_received = 0,
            .pre_votes_needed = 0,
            .pre_vote_rounds = 0,
            .pre_vote_successes = 0,
            .pre_vote_failures = 0,
        };
    }

    // ── Timeout Randomization ───────────────────────────────────────────

    pub fn randomizeTimeout(self: *ElectionManager, now_ms: u64) void {
        self.node.resetElectionTimer(now_ms);
    }

    // ── Election Start ──────────────────────────────────────────────────

    /// Begin an election. If pre-vote is enabled, starts with a pre-vote
    /// phase. Otherwise, proceeds directly to a real election.
    /// Returns the VoteRequest to broadcast to peers, or null when the
    /// node could not persist its self-vote (see `RaftNode.startElection`).
    pub fn beginElection(self: *ElectionManager) ?VoteRequest {
        if (self.node.config.enable_pre_vote and self.node.peer_count > 0) {
            return self.startPreVote();
        } else {
            return self.startRealElection();
        }
    }

    /// Start a pre-vote round. Does NOT increment term or change voted_for.
    fn startPreVote(self: *ElectionManager) VoteRequest {
        self.phase = .pre_vote;
        self.pre_votes_received = 1; // Count self
        self.pre_votes_needed = self.node.quorum();
        self.pre_vote_rounds += 1;

        return .{
            .term = self.node.current_term + 1, // Proposed term, not committed
            .candidate_id = self.node.id,
            .last_log_index = self.node.log.lastIndex(),
            .last_log_term = self.node.log.lastTerm(),
            .is_pre_vote = true,
        };
    }

    /// Start a real election (increments term, votes for self).
    fn startRealElection(self: *ElectionManager) ?VoteRequest {
        const req = self.node.startElection() orelse {
            self.phase = .idle;
            return null;
        };
        self.phase = .voting;
        return req;
    }

    // ── Response Handling ────────────────────────────────────────────────

    /// Handle a vote response (pre-vote or real vote).
    pub fn handleResponse(self: *ElectionManager, resp: VoteResponse) ElectionResult {
        switch (self.phase) {
            .idle => return .pending,
            .pre_vote => return self.handlePreVoteResponse(resp),
            .voting => {
                const won = self.node.handleVoteResponse(resp);
                if (won) {
                    self.phase = .idle;
                    return .won;
                }
                // Check if we stepped down (higher term)
                if (self.node.role == .follower) {
                    self.phase = .idle;
                    return .lost;
                }
                return .pending;
            },
        }
    }

    fn handlePreVoteResponse(self: *ElectionManager, resp: VoteResponse) ElectionResult {
        // Higher term means we should not proceed
        if (resp.term > self.node.current_term + 1) {
            self.phase = .idle;
            self.pre_vote_failures += 1;
            return .lost;
        }

        if (resp.vote_granted) {
            self.pre_votes_received += 1;
            if (self.pre_votes_received >= self.pre_votes_needed) {
                // Pre-vote succeeded — advance to real election
                self.pre_vote_successes += 1;
                return .advance_to_vote;
            }
        }

        return .pending;
    }

    /// Advance from pre-vote to real election after pre-vote quorum.
    /// Returns the VoteRequest to broadcast, or null (see `beginElection`).
    pub fn advanceToRealElection(self: *ElectionManager) ?VoteRequest {
        return self.startRealElection();
    }

    // ── Request Handling ────────────────────────────────────────────────

    /// Handle an incoming VoteRequest with pre-vote awareness.
    /// For pre-vote requests: respond without changing persistent state.
    /// For real vote requests: delegate to the node's handleVoteRequest.
    pub fn handleRequest(self: *ElectionManager, req: VoteRequest) VoteResponse {
        if (req.is_pre_vote) {
            return self.handlePreVoteRequest(req);
        }
        return self.node.handleVoteRequest(req);
    }

    fn handlePreVoteRequest(self: *ElectionManager, req: VoteRequest) VoteResponse {
        // Pre-vote: respond based on whether we WOULD vote, but don't
        // change current_term or voted_for.
        const node = self.node;

        // Reject if proposed term <= our current term (pre-vote uses
        // the candidate's proposed term, not their current term)
        if (req.term < node.current_term) {
            return .{ .term = node.current_term, .vote_granted = false, .from = node.id };
        }

        // If we have a valid leader and haven't timed out, reject pre-vote
        // (prevents unnecessary elections when leader is alive)
        if (node.leader_id != NO_VOTE and node.role != .candidate) {
            if (node.election_deadline_ms > node.current_time_ms) {
                return .{ .term = node.current_term, .vote_granted = false, .from = node.id };
            }
        }

        // Log completeness check (same as real vote)
        if (!isLogUpToDate(node, req.last_log_index, req.last_log_term)) {
            return .{ .term = node.current_term, .vote_granted = false, .from = node.id };
        }

        return .{ .term = node.current_term, .vote_granted = true, .from = node.id };
    }

    // ── Cancel ──────────────────────────────────────────────────────────

    /// Cancel any ongoing election (e.g., received valid AppendEntries).
    pub fn cancel(self: *ElectionManager) void {
        self.phase = .idle;
        self.pre_votes_received = 0;
    }

    // ── Queries ─────────────────────────────────────────────────────────

    pub fn isActive(self: *const ElectionManager) bool {
        return self.phase != .idle;
    }

    pub fn isPreVoting(self: *const ElectionManager) bool {
        return self.phase == .pre_vote;
    }

    pub fn isVoting(self: *const ElectionManager) bool {
        return self.phase == .voting;
    }

    // ── Internal ────────────────────────────────────────────────────────

    fn isLogUpToDate(node: *const RaftNode, last_index: u64, last_term: u64) bool {
        const my_term = node.log.lastTerm();
        if (last_term != my_term) return last_term > my_term;
        return last_index >= node.log.lastIndex();
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Cluster Simulation Helper (for tests only)
// ═══════════════════════════════════════════════════════════════════════════════

/// A minimal 3-node cluster for testing election behavior in-memory.
/// Messages are passed synchronously — no networking.
const ClusterSim = struct {
    nodes: [3]RaftNode,
    elections: [3]ElectionManager,
    alive: [3]bool,
    allocator: Allocator,

    fn init(allocator: Allocator) !ClusterSim {
        var sim: ClusterSim = undefined;
        sim.allocator = allocator;
        sim.alive = .{ true, true, true };
        sim.elections = undefined;

        const ids = [_]NodeId{ 1, 2, 3 };

        // Init all 3 nodes
        for (0..3) |i| {
            sim.nodes[i] = try RaftNode.init(allocator, ids[i], 1000, 8192, .{
                .election_timeout_min_ms = 150,
                .election_timeout_max_ms = 300,
                .enable_pre_vote = false, // test without pre-vote first
            });
            // Add peers
            for (0..3) |j| {
                if (i != j) sim.nodes[i].addPeer(ids[j]);
            }
        }
        // Elections are NOT initialized here — pointers would be
        // invalidated when sim is returned by value. Call setupElections()
        // after the sim is in its final stack location.
        return sim;
    }

    fn initWithPreVote(allocator: Allocator) !ClusterSim {
        var sim: ClusterSim = undefined;
        sim.allocator = allocator;
        sim.alive = .{ true, true, true };
        sim.elections = undefined;

        const ids = [_]NodeId{ 1, 2, 3 };

        for (0..3) |i| {
            sim.nodes[i] = try RaftNode.init(allocator, ids[i], 1000, 8192, .{
                .election_timeout_min_ms = 150,
                .election_timeout_max_ms = 300,
                .enable_pre_vote = true,
            });
            for (0..3) |j| {
                if (i != j) sim.nodes[i].addPeer(ids[j]);
            }
        }
        return sim;
    }

    /// Must be called after the sim is in its final stack location.
    fn setupElections(self: *ClusterSim) void {
        for (0..3) |i| {
            self.elections[i] = ElectionManager.init(&self.nodes[i], self.nodes[i].id * 1000 + 42);
        }
    }

    fn deinit(self: *ClusterSim) void {
        for (0..3) |i| {
            self.nodes[i].deinit();
        }
    }

    /// Find the node array index for a given NodeId, or null.
    fn indexOf(node_id: NodeId) ?usize {
        return switch (node_id) {
            1 => 0,
            2 => 1,
            3 => 2,
            else => null,
        };
    }

    /// Broadcast a VoteRequest from src to all alive peers.
    /// Returns responses.
    fn broadcastVoteRequest(self: *ClusterSim, src_idx: usize, req: VoteRequest) [2]?VoteResponse {
        var responses: [2]?VoteResponse = .{ null, null };
        var resp_idx: usize = 0;
        for (0..3) |i| {
            if (i == src_idx) continue;
            if (!self.alive[i]) {
                resp_idx += 1;
                continue;
            }
            responses[resp_idx] = self.elections[i].handleRequest(req);
            resp_idx += 1;
        }
        return responses;
    }

    /// Run one election round for node at src_idx.
    /// Returns whether the node became leader.
    fn runElection(self: *ClusterSim, src_idx: usize) bool {
        const req = self.elections[src_idx].beginElection().?;
        const responses = self.broadcastVoteRequest(self, src_idx, req);

        for (responses) |maybe_resp| {
            if (maybe_resp) |resp| {
                const result = self.elections[src_idx].handleResponse(resp);
                switch (result) {
                    .won => return true,
                    .lost => return false,
                    .advance_to_vote => {
                        // Pre-vote succeeded, run real election
                        const real_req = self.elections[src_idx].advanceToRealElection().?;
                        const real_responses = self.broadcastVoteRequest(self, src_idx, real_req);
                        for (real_responses) |maybe_real| {
                            if (maybe_real) |real_resp| {
                                const real_result = self.elections[src_idx].handleResponse(real_resp);
                                if (real_result == .won) return true;
                                if (real_result == .lost) return false;
                            }
                        }
                        return false;
                    },
                    .pending => {},
                }
            }
        }
        return false;
    }

    /// Find the current leader among alive nodes. Returns the index or null.
    fn findLeader(self: *const ClusterSim) ?usize {
        for (0..3) |i| {
            if (self.alive[i] and self.nodes[i].role == .leader) return i;
        }
        return null;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "election: pre-vote request without changing state" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 4096, .{});
    defer node.deinit();

    var em = ElectionManager.init(&node, 42);

    const resp = em.handleRequest(.{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .is_pre_vote = true,
    });

    // Should grant pre-vote
    try testing.expect(resp.vote_granted);
    // But state should NOT change
    try testing.expectEqual(@as(u64, 0), node.current_term);
    try testing.expectEqual(NO_VOTE, node.voted_for);
}

test "election: pre-vote rejected when leader is active" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 4096, .{});
    defer node.deinit();

    var em = ElectionManager.init(&node, 42);

    // Simulate having a known leader and active election timer
    node.leader_id = 1;
    node.election_deadline_ms = 1000;
    node.current_time_ms = 500; // haven't timed out yet

    const resp = em.handleRequest(.{
        .term = 2,
        .candidate_id = 3,
        .last_log_index = 0,
        .last_log_term = 0,
        .is_pre_vote = true,
    });

    // Should reject — we still have a valid leader
    try testing.expect(!resp.vote_granted);
}

test "election: pre-vote to real vote transition" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 8192, .{
        .enable_pre_vote = true,
    });
    defer node.deinit();

    node.addPeer(2);
    node.addPeer(3);

    var em = ElectionManager.init(&node, 42);

    // Begin election — should start with pre-vote
    const req = em.beginElection().?;
    try testing.expect(req.is_pre_vote);
    try testing.expectEqual(ElectionPhase.pre_vote, em.phase);
    try testing.expectEqual(@as(u64, 1), req.term); // proposed term = current + 1
    try testing.expectEqual(@as(u64, 0), node.current_term); // term not changed yet

    // Receive pre-vote grant
    const result1 = em.handleResponse(.{
        .term = 0,
        .vote_granted = true,
        .from = 2,
    });

    // 2 of 3 = quorum, advance to real vote
    try testing.expectEqual(ElectionResult.advance_to_vote, result1);
    try testing.expectEqual(@as(u64, 1), em.pre_vote_successes);

    // Now start real election
    const real_req = em.advanceToRealElection().?;
    try testing.expect(!real_req.is_pre_vote);
    try testing.expectEqual(ElectionPhase.voting, em.phase);
    try testing.expectEqual(@as(u64, 1), node.current_term); // term NOW incremented

    // Receive real vote grant
    const result2 = em.handleResponse(.{
        .term = 1,
        .vote_granted = true,
        .from = 2,
    });

    try testing.expectEqual(ElectionResult.won, result2);
    try testing.expectEqual(Role.leader, node.role);
}

test "election: direct voting without pre-vote" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 8192, .{
        .enable_pre_vote = false,
    });
    defer node.deinit();

    node.addPeer(2);
    node.addPeer(3);

    var em = ElectionManager.init(&node, 42);

    // Begin election — should go directly to voting
    const req = em.beginElection().?;
    try testing.expect(!req.is_pre_vote);
    try testing.expectEqual(ElectionPhase.voting, em.phase);
    try testing.expectEqual(@as(u64, 1), node.current_term);
}

test "election: randomized timeout varies" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{
        .election_timeout_min_ms = 100,
        .election_timeout_max_ms = 500,
    });
    defer node.deinit();

    var em = ElectionManager.init(&node, 42);

    // Generate several timeouts and verify they're in range
    var seen_different = false;
    var prev_deadline: u64 = 0;

    for (0..10) |_| {
        em.randomizeTimeout(0);
        const deadline = node.election_deadline_ms;

        try testing.expect(deadline >= 100);
        try testing.expect(deadline <= 500);

        if (prev_deadline != 0 and deadline != prev_deadline) {
            seen_different = true;
        }
        prev_deadline = deadline;
    }

    try testing.expect(seen_different);
}

test "election: cancel resets phase" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{
        .enable_pre_vote = true,
    });
    defer node.deinit();

    node.addPeer(2);

    var em = ElectionManager.init(&node, 42);

    _ = em.beginElection().?;
    try testing.expect(em.isActive());
    try testing.expect(em.isPreVoting());

    em.cancel();
    try testing.expect(!em.isActive());
    try testing.expectEqual(ElectionPhase.idle, em.phase);
}

test "election: pre-vote rejected with stale log" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 8192, .{});
    defer node.deinit();

    // Give node 2 a log entry in term 5
    var e = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 5, 1, 0, "data");
    e.header.crc32c = e.computeCrc();
    _ = try node.log.append(&e);

    var em = ElectionManager.init(&node, 42);

    // Candidate with an older log (term 3)
    const resp = em.handleRequest(.{
        .term = 6,
        .candidate_id = 1,
        .last_log_index = 1,
        .last_log_term = 3, // older term than our term 5
        .is_pre_vote = true,
    });

    try testing.expect(!resp.vote_granted);
}

test "election: 3-node cluster — leader elected" {
    const allocator = testing.allocator;

    var sim = try ClusterSim.init(allocator);
    sim.setupElections();
    defer sim.deinit();

    // Verify all nodes start as followers
    for (0..3) |i| {
        try testing.expectEqual(Role.follower, sim.nodes[i].role);
    }

    // Node 0 starts election
    const req = sim.elections[0].beginElection().?;
    try testing.expect(!req.is_pre_vote); // pre-vote disabled in basic sim

    // Broadcast to peers
    const responses = sim.broadcastVoteRequest(0, req);

    // Process responses
    var won = false;
    for (responses) |maybe_resp| {
        if (maybe_resp) |resp| {
            const result = sim.elections[0].handleResponse(resp);
            if (result == .won) won = true;
        }
    }

    try testing.expect(won);
    try testing.expectEqual(Role.leader, sim.nodes[0].role);
    try testing.expectEqual(@as(u64, 1), sim.nodes[0].current_term);
}

test "election: 3-node cluster — leader dies, new leader elected" {
    const allocator = testing.allocator;

    var sim = try ClusterSim.init(allocator);
    sim.setupElections();
    defer sim.deinit();

    // Node 0 becomes leader
    const req1 = sim.elections[0].beginElection().?;
    const responses1 = sim.broadcastVoteRequest(0, req1);
    for (responses1) |maybe_resp| {
        if (maybe_resp) |resp| {
            _ = sim.elections[0].handleResponse(resp);
        }
    }
    try testing.expectEqual(Role.leader, sim.nodes[0].role);
    try testing.expectEqual(@as(u64, 1), sim.nodes[0].current_term);

    // Kill node 0
    sim.alive[0] = false;

    // Node 1 starts election in higher term
    const req2 = sim.elections[1].beginElection().?;
    try testing.expectEqual(@as(u64, 2), req2.term); // term 2

    // Broadcast — node 0 is dead, only node 2 responds
    const responses2 = sim.broadcastVoteRequest(1, req2);
    var new_leader = false;
    for (responses2) |maybe_resp| {
        if (maybe_resp) |resp| {
            const result = sim.elections[1].handleResponse(resp);
            if (result == .won) new_leader = true;
        }
    }

    try testing.expect(new_leader);
    try testing.expectEqual(Role.leader, sim.nodes[1].role);
    try testing.expectEqual(@as(u64, 2), sim.nodes[1].current_term);
}

test "election: 3-node cluster with pre-vote" {
    const allocator = testing.allocator;

    var sim = try ClusterSim.initWithPreVote(allocator);
    sim.setupElections();
    defer sim.deinit();

    // Node 0 starts election — should begin with pre-vote
    const pre_req = sim.elections[0].beginElection().?;
    try testing.expect(pre_req.is_pre_vote);
    try testing.expectEqual(@as(u64, 0), sim.nodes[0].current_term); // not incremented

    // Broadcast pre-vote
    const pre_responses = sim.broadcastVoteRequest(0, pre_req);
    var advance = false;
    for (pre_responses) |maybe_resp| {
        if (maybe_resp) |resp| {
            const result = sim.elections[0].handleResponse(resp);
            if (result == .advance_to_vote) advance = true;
        }
    }
    try testing.expect(advance);

    // Now real election
    const real_req = sim.elections[0].advanceToRealElection().?;
    try testing.expect(!real_req.is_pre_vote);
    try testing.expectEqual(@as(u64, 1), sim.nodes[0].current_term);

    const real_responses = sim.broadcastVoteRequest(0, real_req);
    var won = false;
    for (real_responses) |maybe_resp| {
        if (maybe_resp) |resp| {
            const result = sim.elections[0].handleResponse(resp);
            if (result == .won) won = true;
        }
    }

    try testing.expect(won);
    try testing.expectEqual(Role.leader, sim.nodes[0].role);
}

test "election: pre-vote prevents term inflation" {
    const allocator = testing.allocator;

    var sim = try ClusterSim.initWithPreVote(allocator);
    sim.setupElections();
    defer sim.deinit();

    // Node 0 becomes leader (skip pre-vote for setup)
    sim.nodes[0].config.enable_pre_vote = false;
    const req = sim.elections[0].beginElection().?;
    const responses = sim.broadcastVoteRequest(0, req);
    for (responses) |maybe_resp| {
        if (maybe_resp) |resp| {
            _ = sim.elections[0].handleResponse(resp);
        }
    }
    try testing.expectEqual(Role.leader, sim.nodes[0].role);

    // Restore pre-vote for node 2
    sim.nodes[2].config.enable_pre_vote = true;

    // Simulate: node 2 is partitioned (alive[0] and alive[1] = false for node 2's perspective)
    // Node 2 tries pre-vote — both peers reject because they have a valid leader
    sim.nodes[1].leader_id = 1; // node 1 knows node 0 is leader
    sim.nodes[1].election_deadline_ms = 99999;
    sim.nodes[1].current_time_ms = 0;

    const pre_req = sim.elections[2].beginElection().?;
    try testing.expect(pre_req.is_pre_vote);

    // Node 1 rejects pre-vote (has valid leader)
    const resp = sim.elections[1].handleRequest(pre_req);
    try testing.expect(!resp.vote_granted);

    // Node 2's term should NOT have changed (pre-vote doesn't increment term)
    try testing.expectEqual(@as(u64, 1), sim.nodes[2].current_term);
}
