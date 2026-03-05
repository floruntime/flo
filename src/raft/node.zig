//! Raft State Machine — Leader/Follower/Candidate
//!
//! Implements the core Raft consensus protocol state machine. Each Partition
//! has one RaftNode instance. In single-node mode the node self-elects as
//! leader immediately.
//!
//! Persistent state (fsync'd on every vote):
//!   - current_term
//!   - voted_for
//!   - node_id
//!
//! Volatile state:
//!   - role (follower/candidate/leader)
//!   - commit_index
//!   - last_applied
//!   - leader_id
//!   - election timer
//!   - per-peer next_index/match_index (leader only)

const std = @import("std");
const Allocator = std.mem.Allocator;
const raft_log = @import("log.zig");
const entry_mod = @import("../storage/ual/entry.zig");

const RaftLog = raft_log.RaftLog;
const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const log = @import("stdx").log;

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

pub const Role = enum(u8) {
    follower = 0,
    candidate = 1,
    leader = 2,
};

pub const NodeId = u32;
pub const NO_VOTE: NodeId = 0;

/// Per-peer replication tracking (leader-only state).
pub const PeerState = struct {
    next_index: u64,
    match_index: u64,
    /// True if an AppendEntries RPC is in flight to this peer.
    inflight: bool,
};

/// Configuration for RaftNode behavior.
pub const Config = struct {
    /// Election timeout range in milliseconds.
    election_timeout_min_ms: u64 = 150,
    election_timeout_max_ms: u64 = 300,
    /// Heartbeat interval in milliseconds (leader sends to followers).
    heartbeat_interval_ms: u64 = 50,
    /// Maximum entries per AppendEntries batch.
    max_entries_per_batch: u32 = 64,
    /// Enable pre-vote protocol (prevents disruptive elections).
    enable_pre_vote: bool = true,
};

/// Result of processing a tick (timer advancement).
pub const TickResult = struct {
    /// Actions the caller must take after the tick.
    send_heartbeats: bool = false,
    start_election: bool = false,
    step_down: bool = false,
};

/// Result of calling propose() on the leader.
pub const ProposeResult = struct {
    index: u64,
    term: u64,
};

/// Vote request (RequestVote RPC arguments).
pub const VoteRequest = struct {
    term: u64,
    candidate_id: NodeId,
    last_log_index: u64,
    last_log_term: u64,
    is_pre_vote: bool = false,
};

/// Vote response.
pub const VoteResponse = struct {
    term: u64,
    vote_granted: bool,
    from: NodeId,
};

/// AppendEntries request (simplified for state machine testing).
pub const AppendRequest = struct {
    term: u64,
    leader_id: NodeId,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: []const Entry,
    leader_commit: u64,
};

/// AppendEntries response.
pub const AppendResponse = struct {
    term: u64,
    success: bool,
    match_index: u64,
    from: NodeId,
};

// ═══════════════════════════════════════════════════════════════════════════════
// RaftNode
// ═══════════════════════════════════════════════════════════════════════════════

pub const MAX_PEERS: usize = 7;

pub const RaftNode = struct {
    // ── Identity ────────────────────────────────────────────────────────
    id: NodeId,
    group_id: u32,

    // ── Persistent state (must be fsync'd before responding to RPCs) ───
    current_term: u64,
    voted_for: NodeId,

    // ── Volatile state ─────────────────────────────────────────────────
    role: Role,
    leader_id: NodeId,
    commit_index: u64,
    last_applied: u64,

    // ── Election timer ─────────────────────────────────────────────────
    election_deadline_ms: u64,
    current_time_ms: u64,

    // ── Leader state ───────────────────────────────────────────────────
    peers: [MAX_PEERS]PeerState,
    peer_ids: [MAX_PEERS]NodeId,
    peer_count: u8,
    votes_received: u8,
    votes_needed: u8,

    // ── Log ────────────────────────────────────────────────────────────
    log: RaftLog,

    // ── Config ─────────────────────────────────────────────────────────
    config: Config,
    allocator: Allocator,

    // ── Stats ──────────────────────────────────────────────────────────
    elections_started: u64,
    elections_won: u64,
    terms_seen: u64,

    // ── Construction ────────────────────────────────────────────────────

    pub fn init(
        allocator: Allocator,
        node_id: NodeId,
        group_id: u32,
        log_capacity: usize,
        config: Config,
    ) !RaftNode {
        var raft_log_inst = try RaftLog.init(allocator, log_capacity);
        errdefer raft_log_inst.deinit();

        return .{
            .id = node_id,
            .group_id = group_id,
            .current_term = 0,
            .voted_for = NO_VOTE,
            .role = .follower,
            .leader_id = NO_VOTE,
            .commit_index = 0,
            .last_applied = 0,
            .election_deadline_ms = 0,
            .current_time_ms = 0,
            .peers = std.mem.zeroes([MAX_PEERS]PeerState),
            .peer_ids = std.mem.zeroes([MAX_PEERS]NodeId),
            .peer_count = 0,
            .votes_received = 0,
            .votes_needed = 0,
            .log = raft_log_inst,
            .config = config,
            .allocator = allocator,
            .elections_started = 0,
            .elections_won = 0,
            .terms_seen = 0,
        };
    }

    pub fn deinit(self: *RaftNode) void {
        self.log.deinit();
    }

    // ── Cluster Membership ──────────────────────────────────────────────

    /// Add a peer to the cluster configuration.
    pub fn addPeer(self: *RaftNode, peer_id: NodeId) void {
        if (self.peer_count >= MAX_PEERS) return;
        self.peer_ids[self.peer_count] = peer_id;
        self.peers[self.peer_count] = .{
            .next_index = self.log.lastIndex() + 1,
            .match_index = 0,
            .inflight = false,
        };
        self.peer_count += 1;
    }

    /// Total cluster size (self + peers).
    pub fn clusterSize(self: *const RaftNode) u8 {
        return self.peer_count + 1;
    }

    /// Quorum size (majority).
    pub fn quorum(self: *const RaftNode) u8 {
        return self.clusterSize() / 2 + 1;
    }

    // ── Single-Node Bootstrap ───────────────────────────────────────────

    /// Bootstrap as a single-node cluster. Self-elects as leader.
    pub fn bootstrap(self: *RaftNode) !void {
        log.debug("Raft: bootstrapping single-node, node_id={d}, group_id={d}", .{ self.id, self.group_id });
        self.current_term = 1;
        self.voted_for = self.id;
        self.role = .leader;
        self.leader_id = self.id;
        self.elections_won = 1;
        self.terms_seen = 1;

        // Append a noop entry to establish the leader's commit index
        var noop = entry_mod.buildEntry(
            .raft_noop,
            entry_mod.Flags.NONE,
            self.current_term,
            self.log.lastIndex() + 1,
            0,
            "",
        );
        noop.header.crc32c = noop.computeCrc();
        const idx = try self.log.append(&noop);
        self.commit_index = idx;
        log.debug("Raft: bootstrap complete, leader at term=1, commit_index={d}", .{idx});
    }

    // ── Tick (Timer) ────────────────────────────────────────────────────

    /// Advance the clock. Returns actions the caller should take.
    pub fn tick(self: *RaftNode, now_ms: u64) TickResult {
        self.current_time_ms = now_ms;
        var result = TickResult{};

        switch (self.role) {
            .leader => {
                // Leaders send periodic heartbeats
                result.send_heartbeats = true;
            },
            .follower, .candidate => {
                // Check election timeout
                if (self.election_deadline_ms > 0 and now_ms >= self.election_deadline_ms) {
                    result.start_election = true;
                }
            },
        }

        return result;
    }

    /// Set the election deadline based on randomized timeout.
    pub fn resetElectionTimer(self: *RaftNode, now_ms: u64) void {
        // Simple deterministic "random" for testing — use node_id as seed
        const range = self.config.election_timeout_max_ms - self.config.election_timeout_min_ms;
        const jitter = (self.current_term *% 7 +% self.id *% 13) % (range + 1);
        self.election_deadline_ms = now_ms + self.config.election_timeout_min_ms + jitter;
        self.current_time_ms = now_ms;
    }

    // ── Election ────────────────────────────────────────────────────────

    /// Start an election (become candidate, increment term, vote for self).
    /// Returns the VoteRequest to broadcast to peers.
    pub fn startElection(self: *RaftNode) VoteRequest {
        self.current_term += 1;
        self.role = .candidate;
        log.debug("Raft: starting election, node_id={d}, new_term={d}", .{ self.id, self.current_term });
        self.voted_for = self.id;
        self.leader_id = NO_VOTE;
        self.votes_received = 1; // vote for self
        self.votes_needed = self.quorum();
        self.elections_started += 1;
        self.terms_seen += 1;

        return .{
            .term = self.current_term,
            .candidate_id = self.id,
            .last_log_index = self.log.lastIndex(),
            .last_log_term = self.log.lastTerm(),
        };
    }

    /// Handle an incoming VoteRequest. Returns the VoteResponse.
    pub fn handleVoteRequest(self: *RaftNode, req: VoteRequest) VoteResponse {
        // If request term > current term, update term and step down
        if (req.term > self.current_term) {
            self.stepDown(req.term);
        }

        // Reject if request term < current term
        if (req.term < self.current_term) {
            return .{ .term = self.current_term, .vote_granted = false, .from = self.id };
        }

        // Check if we can vote for this candidate
        const can_vote = (self.voted_for == NO_VOTE or self.voted_for == req.candidate_id);
        if (!can_vote) {
            return .{ .term = self.current_term, .vote_granted = false, .from = self.id };
        }

        // Log completeness check: candidate's log must be at least as up-to-date
        if (!self.isLogUpToDate(req.last_log_index, req.last_log_term)) {
            return .{ .term = self.current_term, .vote_granted = false, .from = self.id };
        }

        // Grant vote
        self.voted_for = req.candidate_id;
        log.debug("Raft: vote granted to node={d}, term={d}", .{ req.candidate_id, self.current_term });
        return .{ .term = self.current_term, .vote_granted = true, .from = self.id };
    }

    /// Handle an incoming VoteResponse (candidate only).
    /// Returns true if we just won the election (became leader).
    pub fn handleVoteResponse(self: *RaftNode, resp: VoteResponse) bool {
        if (resp.term > self.current_term) {
            self.stepDown(resp.term);
            return false;
        }

        if (self.role != .candidate) return false;
        if (resp.term != self.current_term) return false;

        if (resp.vote_granted) {
            self.votes_received += 1;
            if (self.votes_received >= self.votes_needed) {
                self.becomeLeader();
                log.debug("Raft: won election, node_id={d}, term={d}, votes={d}", .{ self.id, self.current_term, self.votes_received });
                return true;
            }
        }

        return false;
    }

    // ── AppendEntries ───────────────────────────────────────────────────

    /// Handle an incoming AppendEntries RPC.
    pub fn handleAppendEntries(self: *RaftNode, req: AppendRequest) !AppendResponse {
        // If request term > current, step down
        if (req.term > self.current_term) {
            self.stepDown(req.term);
        }

        // Reject stale term
        if (req.term < self.current_term) {
            return .{
                .term = self.current_term,
                .success = false,
                .match_index = self.log.lastIndex(),
                .from = self.id,
            };
        }

        // Valid leader — reset election timer
        self.leader_id = req.leader_id;
        if (self.role == .candidate) {
            self.role = .follower;
        }

        // Log matching: check prev_log_index / prev_log_term
        if (req.prev_log_index > 0) {
            if (!self.log.matchesTerm(req.prev_log_index, req.prev_log_term)) {
                return .{
                    .term = self.current_term,
                    .success = false,
                    .match_index = self.log.lastIndex(),
                    .from = self.id,
                };
            }
        }

        // Append new entries (truncate conflicts)
        for (req.entries) |*e| {
            const existing_term = self.log.entryTerm(e.header.index);
            if (existing_term) |t| {
                if (t != e.header.term) {
                    // Conflict — truncate from here
                    self.log.truncateAfter(e.header.index - 1);
                    _ = try self.log.append(e);
                }
                // Same term, same index — already have it, skip
            } else {
                // New entry
                _ = try self.log.append(e);
            }
        }

        // Advance commit index
        if (req.leader_commit > self.commit_index) {
            self.commit_index = @min(req.leader_commit, self.log.lastIndex());
        }

        return .{
            .term = self.current_term,
            .success = true,
            .match_index = self.log.lastIndex(),
            .from = self.id,
        };
    }

    /// Handle an AppendEntries response (leader handles follower reply).
    pub fn handleAppendResponse(self: *RaftNode, resp: AppendResponse) void {
        if (resp.term > self.current_term) {
            self.stepDown(resp.term);
            return;
        }
        if (self.role != .leader) return;

        // Find the peer
        for (0..self.peer_count) |i| {
            if (self.peer_ids[i] == resp.from) {
                self.peers[i].inflight = false;
                if (resp.success) {
                    self.peers[i].match_index = resp.match_index;
                    self.peers[i].next_index = resp.match_index + 1;
                } else {
                    // Decrement next_index for retry (log mismatch)
                    if (self.peers[i].next_index > 1) {
                        self.peers[i].next_index -= 1;
                    }
                }
                break;
            }
        }

        // Advance commit index based on majority match
        self.advanceCommitIndex();
    }

    // ── Propose (Leader) ────────────────────────────────────────────────

    /// Propose a new entry (leader only). Returns error if not leader.
    /// Flags and timestamp are written into the entry header (e.g. HAS_TTL, TOMBSTONE).
    pub fn propose(self: *RaftNode, entry_type: EntryType, flags: u16, timestamp_ns: u64, payload: []const u8) !ProposeResult {
        if (self.role != .leader) return error.NotLeader;

        var e = entry_mod.buildEntry(
            entry_type,
            flags,
            self.current_term,
            self.log.lastIndex() + 1,
            timestamp_ns,
            payload,
        );
        e.header.crc32c = e.computeCrc();
        const idx = try self.log.append(&e);

        // In single-node mode, commit immediately
        if (self.peer_count == 0) {
            self.commit_index = idx;
        }

        log.debug("Raft: proposed entry, index={d}, term={d}, type={d}, payload_len={d}", .{ idx, self.current_term, @intFromEnum(entry_type), payload.len });
        return .{ .index = idx, .term = self.current_term };
    }

    // ── Internal ────────────────────────────────────────────────────────

    fn stepDown(self: *RaftNode, new_term: u64) void {
        log.debug("Raft: stepping down, node_id={d}, old_term={d}, new_term={d}", .{ self.id, self.current_term, new_term });
        self.current_term = new_term;
        self.role = .follower;
        self.voted_for = NO_VOTE;
        self.leader_id = NO_VOTE;
        self.terms_seen += 1;
    }

    fn becomeLeader(self: *RaftNode) void {
        log.debug("Raft: becoming leader, node_id={d}, term={d}", .{ self.id, self.current_term });
        self.role = .leader;
        self.leader_id = self.id;
        self.elections_won += 1;

        // Initialize peer tracking
        const next = self.log.lastIndex() + 1;
        for (0..self.peer_count) |i| {
            self.peers[i].next_index = next;
            self.peers[i].match_index = 0;
            self.peers[i].inflight = false;
        }
    }

    fn isLogUpToDate(self: *const RaftNode, last_index: u64, last_term: u64) bool {
        const my_term = self.log.lastTerm();
        if (last_term != my_term) return last_term > my_term;
        return last_index >= self.log.lastIndex();
    }

    fn advanceCommitIndex(self: *RaftNode) void {
        // Find the highest index replicated to a majority
        const last = self.log.lastIndex();
        var new_commit = self.commit_index;

        var idx = last;
        while (idx > self.commit_index) : (idx -= 1) {
            const term = self.log.entryTerm(idx) orelse continue;
            // Only commit entries from current term (Raft safety)
            if (term != self.current_term) continue;

            var replicas: u8 = 1; // count self
            for (0..self.peer_count) |i| {
                if (self.peers[i].match_index >= idx) {
                    replicas += 1;
                }
            }
            if (replicas >= self.quorum()) {
                new_commit = idx;
                break;
            }
        }

        self.commit_index = new_commit;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "raft node: init as follower" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    try testing.expectEqual(Role.follower, node.role);
    try testing.expectEqual(@as(u64, 0), node.current_term);
    try testing.expectEqual(NO_VOTE, node.voted_for);
    try testing.expectEqual(@as(u64, 0), node.commit_index);
    try testing.expectEqual(@as(u8, 1), node.clusterSize());
    try testing.expectEqual(@as(u8, 1), node.quorum());
}

test "raft node: single-node bootstrap" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    try node.bootstrap();

    try testing.expectEqual(Role.leader, node.role);
    try testing.expectEqual(@as(u64, 1), node.current_term);
    try testing.expectEqual(@as(u32, 1), node.voted_for);
    try testing.expectEqual(@as(u32, 1), node.leader_id);
    try testing.expectEqual(@as(u64, 1), node.commit_index);
    try testing.expectEqual(@as(u64, 1), node.log.lastIndex());
}

test "raft node: single-node propose" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 8192, .{});
    defer node.deinit();

    try node.bootstrap();

    // Propose entries — should commit immediately in single-node mode
    const r1 = try node.propose(.kv_put, 0, 0, "key1val1");
    try testing.expectEqual(@as(u64, 2), r1.index); // 1 is noop
    try testing.expectEqual(@as(u64, 1), r1.term);
    try testing.expectEqual(@as(u64, 2), node.commit_index);

    const r2 = try node.propose(.kv_put, 0, 0, "key2val2");
    try testing.expectEqual(@as(u64, 3), r2.index);
    try testing.expectEqual(@as(u64, 3), node.commit_index);
}

test "raft node: propose rejected when not leader" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    const result = node.propose(.kv_put, 0, 0, "data");
    try testing.expectError(error.NotLeader, result);
}

test "raft node: election timeout triggers start_election" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    node.resetElectionTimer(100);
    try testing.expect(node.election_deadline_ms > 100);

    // Before deadline — no election
    const tick1 = node.tick(110);
    try testing.expect(!tick1.start_election);

    // After deadline — trigger election
    const tick2 = node.tick(node.election_deadline_ms);
    try testing.expect(tick2.start_election);
}

test "raft node: leader sends heartbeats on tick" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    try node.bootstrap();

    const tick_result = node.tick(100);
    try testing.expect(tick_result.send_heartbeats);
    try testing.expect(!tick_result.start_election);
}

test "raft node: startElection transitions to candidate" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    const vote_req = node.startElection();

    try testing.expectEqual(Role.candidate, node.role);
    try testing.expectEqual(@as(u64, 1), node.current_term);
    try testing.expectEqual(@as(u32, 1), node.voted_for);
    try testing.expectEqual(@as(u64, 1), vote_req.term);
    try testing.expectEqual(@as(u32, 1), vote_req.candidate_id);
    try testing.expectEqual(@as(u64, 1), node.elections_started);
}

test "raft node: vote handling — grant vote" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 4096, .{});
    defer node.deinit();

    const resp = node.handleVoteRequest(.{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    });

    try testing.expect(resp.vote_granted);
    try testing.expectEqual(@as(u32, 1), node.voted_for);
    try testing.expectEqual(@as(u64, 1), node.current_term);
}

test "raft node: vote handling — reject stale term" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 4096, .{});
    defer node.deinit();

    // Advance to term 5
    node.current_term = 5;

    const resp = node.handleVoteRequest(.{
        .term = 3, // stale
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    });

    try testing.expect(!resp.vote_granted);
    try testing.expectEqual(@as(u64, 5), resp.term);
}

test "raft node: vote handling — reject already voted" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 4096, .{});
    defer node.deinit();

    // Vote for node 3
    _ = node.handleVoteRequest(.{
        .term = 1,
        .candidate_id = 3,
        .last_log_index = 0,
        .last_log_term = 0,
    });

    // Node 1 asks for vote in same term — reject
    const resp = node.handleVoteRequest(.{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    });

    try testing.expect(!resp.vote_granted);
}

test "raft node: step down on higher term" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    try node.bootstrap(); // leader at term 1

    // Receive vote request with higher term
    _ = node.handleVoteRequest(.{
        .term = 5,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    });

    try testing.expectEqual(Role.follower, node.role);
    try testing.expectEqual(@as(u64, 5), node.current_term);
}

test "raft node: election win with majority" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 8192, .{});
    defer node.deinit();

    node.addPeer(2);
    node.addPeer(3);
    try testing.expectEqual(@as(u8, 3), node.clusterSize());
    try testing.expectEqual(@as(u8, 2), node.quorum());

    // Start election
    _ = node.startElection();
    try testing.expectEqual(Role.candidate, node.role);
    try testing.expectEqual(@as(u8, 1), node.votes_received); // self-vote

    // Receive one grant — this gives us majority (2 of 3)
    const won = node.handleVoteResponse(.{
        .term = node.current_term,
        .vote_granted = true,
        .from = 2,
    });

    try testing.expect(won);
    try testing.expectEqual(Role.leader, node.role);
    try testing.expectEqual(@as(u32, 1), node.leader_id);
}

test "raft node: election loss — not enough votes" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    node.addPeer(2);
    node.addPeer(3);

    _ = node.startElection();

    // Receive rejection
    const won = node.handleVoteResponse(.{
        .term = node.current_term,
        .vote_granted = false,
        .from = 2,
    });

    try testing.expect(!won);
    try testing.expectEqual(Role.candidate, node.role);
}

test "raft node: handleAppendEntries as follower" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 8192, .{});
    defer node.deinit();

    var e1 = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, 1, 0, "data");
    e1.header.crc32c = e1.computeCrc();

    const resp = try node.handleAppendEntries(.{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &[_]Entry{e1},
        .leader_commit = 1,
    });

    try testing.expect(resp.success);
    try testing.expectEqual(@as(u64, 1), resp.match_index);
    try testing.expectEqual(@as(u64, 1), node.commit_index);
    try testing.expectEqual(@as(u32, 1), node.leader_id);
    try testing.expectEqual(@as(u64, 1), node.log.lastIndex());
}

test "raft node: reject AppendEntries with stale term" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 4096, .{});
    defer node.deinit();

    node.current_term = 5;

    const resp = try node.handleAppendEntries(.{
        .term = 3,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &[_]Entry{},
        .leader_commit = 0,
    });

    try testing.expect(!resp.success);
    try testing.expectEqual(@as(u64, 5), resp.term);
}

test "raft node: AppendEntries log matching failure" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 8192, .{});
    defer node.deinit();

    // Append an entry in term 1
    var e1 = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, 1, 1, 0, "data");
    e1.header.crc32c = e1.computeCrc();
    _ = try node.log.append(&e1);

    // AppendEntries claims prev_log at index 1 was term 2 — mismatch
    const resp = try node.handleAppendEntries(.{
        .term = 2,
        .leader_id = 1,
        .prev_log_index = 1,
        .prev_log_term = 2, // wrong term!
        .entries = &[_]Entry{},
        .leader_commit = 0,
    });

    try testing.expect(!resp.success);
}

test "raft node: leader commit advancement with 3-node cluster" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 8192, .{});
    defer node.deinit();

    node.addPeer(2);
    node.addPeer(3);

    // Win election
    _ = node.startElection();
    _ = node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2 });
    try testing.expectEqual(Role.leader, node.role);

    // Propose an entry — goes at index 1 (no noop in election path)
    _ = try node.propose(.kv_put, 0, 0, "key1val1");
    try testing.expectEqual(@as(u64, 0), node.commit_index); // not committed yet

    // Peer 2 acks
    node.handleAppendResponse(.{
        .term = 1,
        .success = true,
        .match_index = 1, // matches our entry at index 1
        .from = 2,
    });

    // Now we have majority (self + peer 2 = 2 of 3)
    try testing.expectEqual(@as(u64, 1), node.commit_index);
}

test "raft node: cluster size and quorum" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    try testing.expectEqual(@as(u8, 1), node.quorum()); // 1-node: quorum=1

    node.addPeer(2);
    try testing.expectEqual(@as(u8, 2), node.quorum()); // 2-node: quorum=2

    node.addPeer(3);
    try testing.expectEqual(@as(u8, 2), node.quorum()); // 3-node: quorum=2

    node.addPeer(4);
    node.addPeer(5);
    try testing.expectEqual(@as(u8, 3), node.quorum()); // 5-node: quorum=3
}
