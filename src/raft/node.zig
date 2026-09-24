//! Raft State Machine — Leader/Follower/Candidate
//!
//! Implements the core Raft consensus protocol state machine. Each Shard
//! has one RaftNode instance; it leads through `bootstrap()` or an
//! election, which a node with no peers wins at once.
//!
//! Persistent state — handed to `hard_state_sink` before the node acts on
//! a new term or vote, so a restart cannot vote twice in one term:
//!   - current_term
//!   - voted_for
//!
//! Volatile state:
//!   - role (follower/candidate/leader)
//!   - commit_index
//!   - last_applied
//!   - leader_id
//!   - election timer (owned here: armed on the first tick as a follower,
//!     re-armed by a current-term AppendEntries, a granted vote, an election
//!     start and a step-down; jitter from the node's own PRNG)
//!   - per-peer next_index/match_index (leader only)

const std = @import("std");
const Allocator = std.mem.Allocator;
const raft_log = @import("log.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const membership = @import("membership.zig");

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
    /// The highest index ever sent to this peer; a success ack past it is
    /// a peer confusing us and is clamped.
    sent_up_to: u64 = 0,
    /// Last tick a current-term response arrived from this peer.
    last_contact_ms: u64 = 0,
    /// When the batch in flight was sent, and when this peer last heard
    /// anything from us: the leader loop's resend and heartbeat clocks.
    sent_at_ms: u64 = 0,
    heartbeat_at_ms: u64 = 0,
};

/// Configuration for RaftNode behavior.
pub const Config = struct {
    /// Election timeout range in milliseconds.
    election_timeout_min_ms: u64 = 150,
    election_timeout_max_ms: u64 = 300,
    /// Heartbeat interval in milliseconds (leader sends to followers).
    heartbeat_interval_ms: u64 = 50,
    /// Enable pre-vote protocol (prevents disruptive elections).
    enable_pre_vote: bool = true,
    /// True when a committed entry is on disk before it is acked (sync
    /// durability): then a leader disagreeing with committed history is a
    /// bug and is refused. False under async flush: a quorum that crashed
    /// inside the flush window can lose committed entries and elect a
    /// leader without them, and the survivor's log rewinds to match
    /// (`committed_conflicts`).
    durable_commits: bool = true,
    /// Seed for election-timeout jitter. 0 draws one from OS entropy; a
    /// simulation passes a per-node seed so a run replays exactly.
    rng_seed: u64 = 0,

    /// Production timing from one knob: the election window is [½, 1] × the
    /// failover timeout, the heartbeat a sixth of it, so a busy disk's fsync
    /// stall does not look like a dead leader. The simulator sets the three
    /// directly to explore the space.
    pub fn fromFailover(failover_ms: u64) Config {
        return .{
            .election_timeout_min_ms = failover_ms / 2,
            .election_timeout_max_ms = failover_ms,
            .heartbeat_interval_ms = @max(1, failover_ms / 6),
        };
    }

    /// How long a sent batch may go unanswered before it is sent again.
    pub fn rpcTimeoutMs(self: Config) u64 {
        return 2 * self.heartbeat_interval_ms;
    }
};

/// Where the node writes its hard state. `persist` returns only once the
/// state is durable and reports its own failures. A false return means the
/// state is not durable: a vote is then not
/// granted, an election is not started and bootstrap fails; an adopted
/// higher term is kept in memory anyway and counted in `persist_failures`.
pub const HardStateSink = struct {
    ctx: *anyopaque,
    persist: *const fn (ctx: *anyopaque, term: u64, voted_for: NodeId) bool,
};

/// Result of processing a tick (timer advancement).
pub const TickResult = struct {
    /// Actions the caller must take after the tick.
    send_heartbeats: bool = false,
    start_election: bool = false,
    /// The leader has not heard from a majority within an election timeout
    /// and stepped down; the caller resolves what it was holding for commit.
    step_down: bool = false,
};

/// What a vote response led to.
pub const VoteOutcome = union(enum) {
    none,
    /// This node is now leader.
    won,
    /// The pre-vote round passed; the caller broadcasts this real request.
    elect: VoteRequest,
};

pub const ProposeResult = @import("types.zig").ProposeResult;

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
    is_pre_vote: bool = false,
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
    /// On a rejection, where the follower's log stops agreeing: its last
    /// index when the probe was past it, else the index before the run of
    /// the conflicting term. The leader jumps there instead of walking
    /// back one index per round trip.
    hint_index: u64 = 0,
};

// ═══════════════════════════════════════════════════════════════════════════════
// RaftNode
// ═══════════════════════════════════════════════════════════════════════════════

pub const MAX_PEERS: usize = 7;
/// Most entries a leader appends beyond its commit index before refusing
/// proposals.
pub const MAX_OUTSTANDING: u64 = 1024;

/// A timer seed from OS entropy, for nodes no simulation is replaying.
fn entropySeed() u64 {
    var buf: [8]u8 = undefined;
    @import("stdx").io.instance().random(&buf);
    return std.mem.readInt(u64, &buf, .little);
}

pub const RaftNode = struct {
    // ── Identity ────────────────────────────────────────────────────────
    id: NodeId,
    group_id: u32,

    // ── Persistent state (through `hard_state_sink`; see the file header) ──
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
    rng: std.Random.DefaultPrng,

    // ── Hard-state persistence ─────────────────────────────────────────
    hard_state_sink: ?HardStateSink,
    /// Persist calls that returned false. Each one is a term or vote the
    /// node holds in memory only.
    persist_failures: u64,

    // ── Leader state ───────────────────────────────────────────────────
    peers: [MAX_PEERS]PeerState,
    peer_ids: [MAX_PEERS]NodeId,
    peer_count: u8,
    votes_received: u8,
    votes_needed: u8,
    /// Which peers (indexed like peer_ids) granted a vote in the current
    /// election. Re-delivered VoteResponses must not double-count toward quorum.
    vote_granted_by: [MAX_PEERS]bool,
    /// A pre-vote round is a poll before the term is spent: the term is
    /// not incremented, nothing is persisted, and only a majority of
    /// "I would vote for you" turns into a real election.
    pre_vote_in_progress: bool,
    /// Last tick a current-term leader spoke to us. A vote request within
    /// one election timeout of that is a disrupted node's, not a real
    /// failover, and is ignored.
    last_leader_contact_ms: u64,
    /// When this node became leader; counts as contact with every peer for
    /// check-quorum until they answer.
    leader_since_ms: u64,
    /// False for a joiner whose log does not yet name it: it must not
    /// elect itself into a cluster it is not a member of.
    timer_enabled: bool,
    /// Index of the config entry the current membership came from, so a
    /// truncation reaching below it reverts to the committed one.
    membership_index: u64,
    /// Times a leader's log conflicted with an entry this node had already
    /// committed and applied, each rewinding commit and apply below it.
    /// Only possible without durable commits; the owner watches it, since
    /// what it applied above the cut is not what the log now says.
    committed_conflicts: u64,
    committed_member_ids: [MAX_PEERS + 1]NodeId,
    committed_member_count: u8,

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
            .rng = std.Random.DefaultPrng.init(if (config.rng_seed != 0) config.rng_seed else entropySeed()),
            .hard_state_sink = null,
            .persist_failures = 0,
            .peers = std.mem.zeroes([MAX_PEERS]PeerState),
            .peer_ids = std.mem.zeroes([MAX_PEERS]NodeId),
            .peer_count = 0,
            .votes_received = 0,
            .votes_needed = 0,
            .vote_granted_by = std.mem.zeroes([MAX_PEERS]bool),
            .pre_vote_in_progress = false,
            .last_leader_contact_ms = 0,
            .leader_since_ms = 0,
            .timer_enabled = true,
            .membership_index = 0,
            .committed_conflicts = 0,
            .committed_member_ids = std.mem.zeroes([MAX_PEERS + 1]NodeId),
            .committed_member_count = 0,
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

    /// Membership is what the log's latest config entry says. The peers
    /// become exactly `member_ids` minus this node, keeping the progress of
    /// any that stay; a node counts toward quorum from the moment the entry
    /// is appended. Whether this node may elect itself follows from whether
    /// it is named. `index` is the entry's; when it commits the caller
    /// passes it again through `commitMembership`, and a truncation below
    /// it reverts to the committed one.
    pub fn setMembership(self: *RaftNode, member_ids: []const NodeId, index: u64) void {
        var new_peers: [MAX_PEERS]PeerState = undefined;
        var new_ids: [MAX_PEERS]NodeId = undefined;
        var n: u8 = 0;
        var named = false;
        for (member_ids) |id| {
            if (id == self.id) {
                named = true;
                continue;
            }
            if (n >= MAX_PEERS) break;
            new_ids[n] = id;
            // A peer named just now has not spoken yet; it counts as heard
            // from now, or check-quorum would depose a leader that adds a
            // member more than a failover after it began.
            new_peers[n] = if (self.peerIndex(id)) |i| self.peers[i] else .{
                .next_index = self.log.lastIndex() + 1,
                .match_index = 0,
                .inflight = false,
                .last_contact_ms = self.current_time_ms,
            };
            n += 1;
        }
        self.peers = new_peers;
        self.peer_ids = new_ids;
        self.peer_count = n;
        self.timer_enabled = named;
        self.membership_index = index;
        if (self.role == .candidate) self.votes_needed = self.quorum();
    }

    /// The config entry at `index` committed: what it named is now the
    /// membership a truncation falls back to.
    pub fn commitMembership(self: *RaftNode, member_ids: []const NodeId) void {
        self.committed_member_count = @intCast(@min(member_ids.len, self.committed_member_ids.len));
        @memcpy(self.committed_member_ids[0..self.committed_member_count], member_ids[0..self.committed_member_count]);
    }

    /// The current member ids, this node included when named.
    pub fn memberIds(self: *const RaftNode, out: *[MAX_PEERS + 1]NodeId) []NodeId {
        var n: usize = 0;
        if (self.timer_enabled) {
            out[n] = self.id;
            n += 1;
        }
        for (self.peer_ids[0..self.peer_count]) |id| {
            out[n] = id;
            n += 1;
        }
        return out[0..n];
    }

    /// A config entry is the membership from the moment it is in the log,
    /// on the leader that wrote it and on every follower that took it.
    fn noteAppended(self: *RaftNode, e: *const Entry) void {
        if (e.header.entry_type != @intFromEnum(EntryType.raft_config)) return;
        var ids: [membership.MAX_MEMBERS]NodeId = undefined;
        const members = membership.decode(e.payload, &ids) orelse {
            log.err("Raft: config entry at index {d} is not a member list; membership unchanged", .{e.header.index});
            return;
        };
        self.setMembership(members, e.header.index);
    }

    fn truncatedBelowMembership(self: *RaftNode, after_index: u64) void {
        if (self.membership_index == 0 or self.membership_index <= after_index) return;
        var ids: [MAX_PEERS + 1]NodeId = undefined;
        const n = self.committed_member_count;
        @memcpy(ids[0..n], self.committed_member_ids[0..n]);
        self.setMembership(ids[0..n], 0);
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

    /// Bootstrap as a single-node cluster. Self-elects as leader in a new
    /// term — term 1 on a fresh node, one past the persisted term after a
    /// restart — and appends that term's noop at `lastIndex() + 1`, so a
    /// restored log continues without a gap. Fails if the new term cannot
    /// be made durable: leading in a term a crash would forget is how a
    /// node ends up voting twice in it.
    pub fn bootstrap(self: *RaftNode) !void {
        log.debug("Raft: bootstrapping single-node, node_id={d}, group_id={d}, prior_term={d}, last_index={d}", .{ self.id, self.group_id, self.current_term, self.log.lastIndex() });
        self.current_term += 1;
        self.voted_for = self.id;
        if (!self.persistHardState()) return error.HardStateNotDurable;
        self.role = .leader;
        self.leader_id = self.id;
        self.elections_won += 1;
        self.terms_seen += 1;

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
        // `last_applied` stays where replay left it; the owner drains what
        // this bootstrap just committed.
        self.commit_index = idx;
        log.debug("Raft: bootstrap complete, leader at term={d}, commit_index={d}", .{ self.current_term, idx });
    }

    // ── Tick (Timer) ────────────────────────────────────────────────────

    /// Advance the clock. Returns actions the caller should take.
    pub fn tick(self: *RaftNode, now_ms: u64) TickResult {
        self.current_time_ms = now_ms;
        // Bootstrap has no clock: a bootstrapped leader's reign begins at
        // its first tick.
        if (self.role == .leader and self.leader_since_ms == 0) self.leader_since_ms = now_ms;
        var result = TickResult{};

        switch (self.role) {
            .leader => {
                // A leader that cannot reach a majority is not one: a
                // partitioned leader would otherwise hold its clients'
                // proposals forever, since there is no per-proposal timer.
                if (self.peer_count > 0 and now_ms -| self.quorumContactMs() > self.config.election_timeout_max_ms) {
                    log.warn("Raft: no contact with a majority for {d} ms; stepping down from term {d}", .{ now_ms - self.quorumContactMs(), self.current_term });
                    self.role = .follower;
                    self.leader_id = NO_VOTE;
                    self.pre_vote_in_progress = false;
                    self.rearmElectionTimer();
                    result.step_down = true;
                } else {
                    result.send_heartbeats = true;
                }
            },
            .follower, .candidate => {
                if (!self.timer_enabled) return result;
                // First tick: arm here rather than in init, which has no
                // clock, so a node whose leader never speaks still elects.
                if (self.election_deadline_ms == 0) {
                    self.resetElectionTimer(now_ms);
                } else if (now_ms >= self.election_deadline_ms) {
                    result.start_election = true;
                }
            },
        }

        return result;
    }

    /// Arm the election timer: `now + min + jitter`, jitter drawn from the
    /// node's PRNG so two nodes with identical logs do not time out in
    /// lockstep for many terms.
    pub fn resetElectionTimer(self: *RaftNode, now_ms: u64) void {
        const range = self.config.election_timeout_max_ms - self.config.election_timeout_min_ms;
        const jitter = self.rng.random().intRangeAtMost(u64, 0, range);
        self.election_deadline_ms = now_ms + self.config.election_timeout_min_ms + jitter;
        self.current_time_ms = now_ms;
    }

    /// The tick by which a majority (this node included) had last answered:
    /// the quorum-th most recent contact. A peer that has never answered
    /// this leadership counts from when it began.
    fn quorumContactMs(self: *const RaftNode) u64 {
        var contacts: [MAX_PEERS + 1]u64 = undefined;
        contacts[0] = self.current_time_ms;
        for (0..self.peer_count) |i| contacts[i + 1] = @max(self.peers[i].last_contact_ms, self.leader_since_ms);
        const n = self.peer_count + 1;
        std.mem.sort(u64, contacts[0..n], {}, std.sort.desc(u64));
        return contacts[self.quorum() - 1];
    }

    /// Re-arm from the last tick's clock — for events that carry no time.
    /// Before the first tick there is no clock: arming from 0 would put the
    /// deadline in the past and the first tick would elect against a leader
    /// that just spoke, so the first tick arms instead.
    fn rearmElectionTimer(self: *RaftNode) void {
        if (self.current_time_ms == 0) return;
        self.resetElectionTimer(self.current_time_ms);
    }

    // ── Election ────────────────────────────────────────────────────────

    /// The election timer fired. With peers and pre-vote on, the result is
    /// a poll to broadcast, and the term is spent only from a passed poll
    /// (`handleVoteResponse` returns the real request). Otherwise the term
    /// is spent here; see `startElectionNow`.
    pub fn startElection(self: *RaftNode) ?VoteRequest {
        // With peers, ask first whether a majority would vote: a node that
        // lost touch must not spend a term and depose a live leader on
        // reconnect. A poll that gets no majority before the timer fires
        // again is simply asked again; only a passed poll spends the term.
        if (self.config.enable_pre_vote and self.peer_count > 0) {
            self.pre_vote_in_progress = true;
            self.votes_received = 1;
            self.votes_needed = self.quorum();
            self.vote_granted_by = std.mem.zeroes([MAX_PEERS]bool);
            self.rearmElectionTimer();
            return .{
                .term = self.current_term + 1,
                .candidate_id = self.id,
                .last_log_index = self.log.lastIndex(),
                .last_log_term = self.log.lastTerm(),
                .is_pre_vote = true,
            };
        }
        return self.startElectionNow();
    }

    /// Spend the term: what a passed poll leads to, and what a node with no
    /// peers or no pre-vote does at once.
    pub fn startElectionNow(self: *RaftNode) ?VoteRequest {
        self.pre_vote_in_progress = false;
        const prior_term = self.current_term;
        const prior_vote = self.voted_for;
        self.current_term += 1;
        self.voted_for = self.id;
        if (!self.persistHardState()) {
            self.current_term = prior_term;
            self.voted_for = prior_vote;
            self.role = .follower;
            self.rearmElectionTimer();
            return null;
        }
        self.role = .candidate;
        log.debug("Raft: starting election, node_id={d}, new_term={d}", .{ self.id, self.current_term });
        self.leader_id = NO_VOTE;
        self.votes_received = 1; // vote for self
        self.votes_needed = self.quorum();
        self.vote_granted_by = std.mem.zeroes([MAX_PEERS]bool);
        self.elections_started += 1;
        self.terms_seen += 1;
        self.rearmElectionTimer();
        // The only member votes for itself and that is the majority.
        if (self.peer_count == 0) self.becomeLeader();
        return .{
            .term = self.current_term,
            .candidate_id = self.id,
            .last_log_index = self.log.lastIndex(),
            .last_log_term = self.log.lastTerm(),
        };
    }

    /// A term more than 2^32 ahead of ours is not an election we missed;
    /// it is corruption or a hostile peer, and adopting it would strand
    /// this node in a term nobody else will ever reach.
    pub fn termPlausible(self: *const RaftNode, term: u64) bool {
        return term <= self.current_term +| (1 << 32);
    }

    /// Handle an incoming VoteRequest. Returns the VoteResponse.
    pub fn handleVoteRequest(self: *RaftNode, req: VoteRequest) VoteResponse {
        if (!self.termPlausible(req.term)) {
            log.warn("Raft: vote request for term {d} rejected; {d} is more than 2^32 ahead of our term {d}", .{ req.term, req.term - self.current_term, self.current_term });
            return .{ .term = self.current_term, .vote_granted = false, .from = self.id };
        }
        // A leader spoke to us within an election timeout: whoever is
        // asking has lost touch, not found a dead leader. Their term is not
        // adopted either, or a disrupted node would depose a live leader.
        const heard_recently = self.last_leader_contact_ms != 0 and
            self.current_time_ms -| self.last_leader_contact_ms < self.config.election_timeout_min_ms;
        if (heard_recently and self.role != .candidate) {
            return .{ .term = self.current_term, .vote_granted = false, .from = self.id, .is_pre_vote = req.is_pre_vote };
        }
        if (req.is_pre_vote) {
            // A poll: would we vote for this log at that term? Nothing is
            // adopted or persisted by answering.
            const would = req.term >= self.current_term and self.isLogUpToDate(req.last_log_index, req.last_log_term);
            return .{ .term = self.current_term, .vote_granted = would, .from = self.id, .is_pre_vote = true };
        }
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

        // Grant the vote only once it is durable: a vote held in memory
        // alone is one this node could cast again after a crash.
        self.voted_for = req.candidate_id;
        if (!self.persistHardState()) {
            self.voted_for = NO_VOTE;
            return .{ .term = self.current_term, .vote_granted = false, .from = self.id };
        }
        self.rearmElectionTimer();
        log.debug("Raft: vote granted to node={d}, term={d}", .{ req.candidate_id, self.current_term });
        return .{ .term = self.current_term, .vote_granted = true, .from = self.id };
    }

    /// Handle an incoming VoteResponse: a pre-vote poll answer while a
    /// poll is open, or a real vote while a candidate.
    pub fn handleVoteResponse(self: *RaftNode, resp: VoteResponse) VoteOutcome {
        if (!self.termPlausible(resp.term)) return .none;
        if (resp.is_pre_vote) {
            if (!self.pre_vote_in_progress or self.role == .leader) return .none;
            // The poll asked about our term + 1; an answer naming a higher
            // term means the cluster has moved on and the poll is void.
            if (resp.term > self.current_term) {
                self.stepDown(resp.term);
                self.pre_vote_in_progress = false;
                return .none;
            }
            if (!resp.vote_granted) return .none;
            const idx = self.peerIndex(resp.from) orelse return .none;
            if (self.vote_granted_by[idx]) return .none;
            self.vote_granted_by[idx] = true;
            self.votes_received += 1;
            if (self.votes_received < self.votes_needed) return .none;
            // A majority would vote: spend the term.
            const req = self.startElectionNow() orelse return .none;
            return .{ .elect = req };
        }
        if (resp.term > self.current_term) {
            self.stepDown(resp.term);
            return .none;
        }
        if (self.role != .candidate) return .none;
        if (resp.term != self.current_term) return .none;
        if (resp.vote_granted) {
            // Count each peer at most once; grants from unknown nodes (or a
            // duplicated response for self) never count toward quorum.
            const idx = self.peerIndex(resp.from) orelse return .none;
            if (self.vote_granted_by[idx]) return .none;
            self.vote_granted_by[idx] = true;
            self.votes_received += 1;
            if (self.votes_received >= self.votes_needed) {
                self.becomeLeader();
                log.debug("Raft: won election, node_id={d}, term={d}, votes={d}", .{ self.id, self.current_term, self.votes_received });
                return .won;
            }
        }
        return .none;
    }

    // ── AppendEntries ───────────────────────────────────────────────────

    /// Handle an incoming AppendEntries RPC.
    pub fn handleAppendEntries(self: *RaftNode, req: AppendRequest) !AppendResponse {
        if (!self.termPlausible(req.term)) {
            log.warn("Raft: AppendEntries for term {d} rejected; {d} is more than 2^32 ahead of our term {d}", .{ req.term, req.term - self.current_term, self.current_term });
            return .{ .term = self.current_term, .success = false, .match_index = self.log.lastIndex(), .from = self.id };
        }
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

        // A current-term leader is alive whether or not this batch fits our
        // log, so the timer resets before log matching: re-arming only on
        // success livelocks a mismatched follower, which would depose its
        // leader every timeout while the one-step next_index walk starts over.
        self.leader_id = req.leader_id;
        if (self.role == .candidate) {
            self.role = .follower;
        }
        self.pre_vote_in_progress = false;
        self.last_leader_contact_ms = self.current_time_ms;
        self.rearmElectionTimer();

        // Log matching: check prev_log_index / prev_log_term. The rejection
        // says where to retry from, so the leader does not walk back one
        // index per round trip.
        if (req.prev_log_index > 0) {
            if (!self.log.matchesTerm(req.prev_log_index, req.prev_log_term)) {
                const last = self.log.lastIndex();
                const hint = if (req.prev_log_index > last) last else (self.log.runStart(req.prev_log_index) orelse req.prev_log_index) -| 1;
                return .{
                    .term = self.current_term,
                    .success = false,
                    .match_index = last,
                    .from = self.id,
                    .hint_index = hint,
                };
            }
        }

        // A batch is the entries after prev, in order; anything else is
        // not a leader's and must not touch the log.
        for (req.entries, 0..) |*e, k| {
            if (e.header.index != req.prev_log_index + 1 + k) return error.MalformedBatch;
        }

        // Append new entries (truncate conflicts)
        for (req.entries) |*e| {
            const existing_term = self.log.entryTerm(e.header.index);
            if (existing_term) |t| {
                if (t != e.header.term) {
                    if (e.header.index <= self.commit_index) {
                        // Committed history is never truncated when commits
                        // are durable; otherwise the log follows the leader
                        // and commit and apply rewind to the cut.
                        if (self.config.durable_commits) return error.CommittedConflict;
                        const cut = e.header.index - 1;
                        log.err("Raft: leader {d} (term {d}) conflicts with committed index {d}; committed history was lost by a quorum, rewinding commit from {d} to {d}", .{ req.leader_id, req.term, e.header.index, self.commit_index, cut });
                        self.committed_conflicts += 1;
                        self.commit_index = cut;
                        self.last_applied = @min(self.last_applied, cut);
                    }
                    self.log.truncateAfter(e.header.index - 1);
                    self.truncatedBelowMembership(e.header.index - 1);
                    _ = try self.log.append(e);
                    self.noteAppended(e);
                }
                // Same term, same index — already have it, skip
            } else {
                // New entry
                _ = try self.log.append(e);
                self.noteAppended(e);
            }
        }

        // Commit only what this RPC verified: capping by lastIndex() would let
        // an empty heartbeat commit a stale suffix left by a deposed leader.
        // Never move commit backwards — an older heartbeat, delayed or
        // duplicated in flight, verifies less than we may already hold.
        const last_new = req.prev_log_index + req.entries.len;
        self.commit_index = @max(self.commit_index, @min(req.leader_commit, last_new));

        // Only the prefix this RPC verified counts as matched. lastIndex()
        // may include a stale suffix from an old term that the leader would
        // otherwise wrongly count toward its commit quorum.
        return .{
            .term = self.current_term,
            .success = true,
            .match_index = req.prev_log_index + req.entries.len,
            .from = self.id,
        };
    }

    /// Handle an AppendEntries response (leader handles follower reply).
    pub fn handleAppendResponse(self: *RaftNode, resp: AppendResponse) void {
        if (!self.termPlausible(resp.term)) return;
        if (resp.term > self.current_term) {
            self.stepDown(resp.term);
            return;
        }
        if (self.role != .leader) return;
        // A response to an RPC from an earlier term — possibly our own
        // earlier leadership — describes a log we may have since truncated;
        // counting its match_index toward a current-term quorum commits an
        // entry that peer never received.
        if (resp.term != self.current_term) return;

        // Find the peer
        for (0..self.peer_count) |i| {
            if (self.peer_ids[i] == resp.from) {
                self.peers[i].inflight = false;
                self.peers[i].last_contact_ms = self.current_time_ms;
                if (resp.success) {
                    // A late or duplicated ack may report less than we already
                    // know matched; a response can also outlive the log it
                    // answered; a confused peer may claim more than it was
                    // sent. Match only advances, never past our own log, and
                    // never past what this leadership sent it.
                    const ceiling = @min(self.log.lastIndex(), self.peers[i].sent_up_to);
                    const acked = @min(resp.match_index, ceiling);
                    self.peers[i].match_index = @max(self.peers[i].match_index, acked);
                    self.peers[i].next_index = self.peers[i].match_index + 1;
                } else {
                    // Retry from where the follower says its log stops
                    // agreeing, never forward. Below the recorded match it
                    // is a follower that crashed before flushing what it
                    // acked: trust it, or probe above its log forever.
                    const hinted = resp.hint_index + 1;
                    const back_one = self.peers[i].next_index -| 1;
                    self.peers[i].next_index = @max(1, @min(hinted, back_one));
                    self.peers[i].match_index = @min(self.peers[i].match_index, resp.hint_index);
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
        // A leader far ahead of its followers holds that many clients; past
        // the cap the client is told, and its reads are the backpressure.
        if (self.peer_count > 0 and self.log.lastIndex() - self.commit_index >= MAX_OUTSTANDING) return error.Overloaded;

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
        self.noteAppended(&e);

        // In single-node mode, commit immediately
        if (self.peer_count == 0) {
            self.commit_index = idx;
        }

        log.debug("Raft: proposed entry, index={d}, term={d}, type={d}, payload_len={d}", .{ idx, self.current_term, @intFromEnum(entry_type), payload.len });
        return .{ .index = idx, .term = self.current_term, .timestamp_ns = timestamp_ns };
    }

    // ── Internal ────────────────────────────────────────────────────────

    fn stepDown(self: *RaftNode, new_term: u64) void {
        log.debug("Raft: stepping down, node_id={d}, old_term={d}, new_term={d}", .{ self.id, self.current_term, new_term });
        self.current_term = new_term;
        self.role = .follower;
        self.voted_for = NO_VOTE;
        // Kept in memory even if it cannot be persisted: acting in the old
        // term is the worse outcome.
        _ = self.persistHardState();
        self.leader_id = NO_VOTE;
        self.terms_seen += 1;
        self.rearmElectionTimer();
    }

    /// Hand (current_term, voted_for) to the sink. True when durable, or when
    /// there is no sink (an ephemeral node has nothing to persist to).
    fn persistHardState(self: *RaftNode) bool {
        const sink = self.hard_state_sink orelse return true;
        if (sink.persist(sink.ctx, self.current_term, self.voted_for)) return true;
        self.persist_failures += 1;
        log.debug("Raft: hard state not durable, node_id={d}, group_id={d}, term={d}, voted_for={d} (persist_failures={d})", .{ self.id, self.group_id, self.current_term, self.voted_for, self.persist_failures });
        return false;
    }

    fn becomeLeader(self: *RaftNode) void {
        log.debug("Raft: becoming leader, node_id={d}, term={d}", .{ self.id, self.current_term });
        self.role = .leader;
        self.leader_id = self.id;
        self.pre_vote_in_progress = false;
        self.leader_since_ms = self.current_time_ms;
        self.elections_won += 1;
        // Initialize peer tracking
        const next = self.log.lastIndex() + 1;
        for (0..self.peer_count) |i| {
            self.peers[i].next_index = next;
            self.peers[i].match_index = 0;
            self.peers[i].inflight = false;
            self.peers[i].sent_up_to = 0;
            self.peers[i].last_contact_ms = 0;
        }
        // An entry of this term, so the entries of earlier terms commit as
        // soon as it replicates: a leader may only count a majority for its
        // own term's entries, and without this one it would wait for a
        // client to write. Alone, the majority is this node and the whole
        // log commits now.
        var noop = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, self.current_term, next, 0, "");
        noop.header.crc32c = noop.computeCrc();
        if (self.log.append(&noop)) |idx| {
            if (self.peer_count == 0) self.commit_index = idx;
        } else |err| {
            log.err("Raft: cannot append the leadership noop at index {d}: {s}; earlier terms' entries commit only after the next client write", .{ next, @errorName(err) });
        }
    }

    fn peerIndex(self: *const RaftNode, peer_id: NodeId) ?usize {
        for (0..self.peer_count) |i| {
            if (self.peer_ids[i] == peer_id) return i;
        }
        return null;
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

/// Stand for election without the poll, for tests that want a candidate.
fn candidacy(node: *RaftNode) ?VoteRequest {
    return node.startElectionNow();
}

/// Everything in the log has been sent to every peer, as the leader loop
/// would have done before their acks arrived.
fn sentAll(node: *RaftNode) void {
    for (0..node.peer_count) |i| node.peers[i].sent_up_to = node.log.lastIndex();
}

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
    node.addPeer(2);

    const vote_req = candidacy(&node).?;

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
    _ = candidacy(&node).?;
    try testing.expectEqual(Role.candidate, node.role);
    try testing.expectEqual(@as(u8, 1), node.votes_received); // self-vote

    // Receive one grant — this gives us majority (2 of 3)
    const won = node.handleVoteResponse(.{
        .term = node.current_term,
        .vote_granted = true,
        .from = 2,
    });

    try testing.expect(won == .won);
    try testing.expectEqual(Role.leader, node.role);
    try testing.expectEqual(@as(u32, 1), node.leader_id);
}

test "raft node: election loss — not enough votes" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    node.addPeer(2);
    node.addPeer(3);

    _ = candidacy(&node).?;

    // Receive rejection
    const won = node.handleVoteResponse(.{
        .term = node.current_term,
        .vote_granted = false,
        .from = 2,
    });

    try testing.expect(won == .none);
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
    _ = candidacy(&node).?;
    _ = node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2 });
    try testing.expectEqual(Role.leader, node.role);

    // The win put a noop at 1; the entry goes at 2.
    _ = try node.propose(.kv_put, 0, 0, "key1val1");
    try testing.expectEqual(@as(u64, 0), node.commit_index); // not committed yet

    // Peer 2 acks everything sent.
    sentAll(&node);
    node.handleAppendResponse(.{
        .term = 1,
        .success = true,
        .match_index = 2,
        .from = 2,
    });

    // Now we have majority (self + peer 2 = 2 of 3)
    try testing.expectEqual(@as(u64, 2), node.commit_index);
}

fn testEntry(term: u64, index: u64, payload: []const u8) Entry {
    var e = entry_mod.buildEntry(.kv_put, entry_mod.Flags.NONE, term, index, 0, payload);
    e.header.crc32c = e.computeCrc();
    return e;
}

test "raft node: heartbeat over stale suffix does not commit unverified entries" {
    const allocator = testing.allocator;

    // Follower: entries 1-7 from term 1, plus a stale suffix 8-10 from a
    // deposed term-2 leader that the current leader never replicated.
    var follower = try RaftNode.init(allocator, 2, 1000, 16384, .{});
    defer follower.deinit();
    follower.current_term = 2;
    for (1..8) |i| {
        var e = testEntry(1, i, "old");
        _ = try follower.log.append(&e);
    }
    for (8..11) |i| {
        var e = testEntry(2, i, "stale");
        _ = try follower.log.append(&e);
    }

    // Leader at term 3: same 1-7, but its own fresh 8-10.
    var leader = try RaftNode.init(allocator, 1, 1000, 16384, .{});
    defer leader.deinit();
    leader.current_term = 3;
    leader.role = .leader;
    leader.leader_id = 1;
    leader.commit_index = 7;
    for (1..8) |i| {
        var e = testEntry(1, i, "old");
        _ = try leader.log.append(&e);
    }
    for (8..11) |i| {
        var e = testEntry(3, i, "fresh");
        _ = try leader.log.append(&e);
    }
    leader.addPeer(2);
    leader.addPeer(3);

    // Heartbeat at prev=7 succeeds but verifies nothing past 7.
    const resp = try follower.handleAppendEntries(.{
        .term = 3,
        .leader_id = 1,
        .prev_log_index = 7,
        .prev_log_term = 1,
        .entries = &[_]Entry{},
        .leader_commit = 7,
    });
    try testing.expect(resp.success);
    try testing.expectEqual(@as(u64, 7), resp.match_index);

    // The leader must not count the follower's stale 8-10 as replicas of
    // its own 8-10.
    sentAll(&leader);
    leader.handleAppendResponse(resp);
    try testing.expectEqual(@as(u64, 7), leader.peers[0].match_index);
    try testing.expectEqual(@as(u64, 8), leader.peers[0].next_index);
    try testing.expectEqual(@as(u64, 7), leader.commit_index);
}

test "raft node: append of N entries at prev P reports match P+N" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 16384, .{});
    defer node.deinit();
    for (1..3) |i| {
        var e = testEntry(1, i, "base");
        _ = try node.log.append(&e);
    }

    const batch = [_]Entry{
        testEntry(1, 3, "a"),
        testEntry(1, 4, "b"),
        testEntry(1, 5, "c"),
    };
    const resp = try node.handleAppendEntries(.{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 2,
        .prev_log_term = 1,
        .entries = &batch,
        .leader_commit = 0,
    });
    try testing.expect(resp.success);
    try testing.expectEqual(@as(u64, 5), resp.match_index);

    // Duplicate delivery: entries already present with the same term still
    // count as verified.
    const resp2 = try node.handleAppendEntries(.{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 2,
        .prev_log_term = 1,
        .entries = &batch,
        .leader_commit = 0,
    });
    try testing.expect(resp2.success);
    try testing.expectEqual(@as(u64, 5), resp2.match_index);
    try testing.expectEqual(@as(u64, 5), node.log.lastIndex());
}

test "raft node: conflict truncation reports match through appended batch" {
    const allocator = testing.allocator;

    // Follower: 1-2 from term 1, stale 3-5 from term 2.
    var node = try RaftNode.init(allocator, 2, 1000, 16384, .{});
    defer node.deinit();
    for (1..3) |i| {
        var e = testEntry(1, i, "base");
        _ = try node.log.append(&e);
    }
    for (3..6) |i| {
        var e = testEntry(2, i, "stale");
        _ = try node.log.append(&e);
    }

    // Term-3 leader overwrites 3-4; the stale 5 is truncated away.
    const batch = [_]Entry{
        testEntry(3, 3, "new3"),
        testEntry(3, 4, "new4"),
    };
    const resp = try node.handleAppendEntries(.{
        .term = 3,
        .leader_id = 1,
        .prev_log_index = 2,
        .prev_log_term = 1,
        .entries = &batch,
        .leader_commit = 0,
    });
    try testing.expect(resp.success);
    try testing.expectEqual(@as(u64, 4), resp.match_index);
    try testing.expectEqual(@as(u64, 4), node.log.lastIndex());
    try testing.expectEqual(@as(u64, 3), node.log.entryTerm(4).?);
}

test "raft node: duplicate vote from same peer counts once" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    node.addPeer(2);
    node.addPeer(3);
    node.addPeer(4);
    node.addPeer(5);
    try testing.expectEqual(@as(u8, 3), node.quorum());

    _ = candidacy(&node).?;

    // Peer 2 grants twice (network duplication) — must not reach quorum
    _ = node.handleVoteResponse(.{ .term = node.current_term, .vote_granted = true, .from = 2 });
    const won_dup = node.handleVoteResponse(.{ .term = node.current_term, .vote_granted = true, .from = 2 });
    try testing.expect(won_dup == .none);
    try testing.expectEqual(Role.candidate, node.role);
    try testing.expectEqual(@as(u8, 2), node.votes_received);

    // A second distinct peer completes the quorum
    const won = node.handleVoteResponse(.{ .term = node.current_term, .vote_granted = true, .from = 3 });
    try testing.expect(won == .won);
    try testing.expectEqual(Role.leader, node.role);
}

test "raft node: vote from unknown node does not count" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    node.addPeer(2);
    node.addPeer(3);
    node.addPeer(4);
    node.addPeer(5);

    _ = candidacy(&node).?;

    const won = node.handleVoteResponse(.{ .term = node.current_term, .vote_granted = true, .from = 99 });
    try testing.expect(won == .none);
    try testing.expectEqual(Role.candidate, node.role);
    try testing.expectEqual(@as(u8, 1), node.votes_received); // self only
}

test "raft node: startElection resets vote tracking" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    node.addPeer(2);
    node.addPeer(3);
    node.addPeer(4);
    node.addPeer(5);

    _ = candidacy(&node).?;
    _ = node.handleVoteResponse(.{ .term = node.current_term, .vote_granted = true, .from = 2 });
    try testing.expectEqual(@as(u8, 2), node.votes_received);

    // New election: nothing carries over, and peer 2 may grant again
    _ = candidacy(&node).?;
    try testing.expectEqual(@as(u8, 1), node.votes_received);
    _ = node.handleVoteResponse(.{ .term = node.current_term, .vote_granted = true, .from = 2 });
    try testing.expectEqual(@as(u8, 2), node.votes_received);
    try testing.expectEqual(Role.candidate, node.role);
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

test "raft node: follower does not commit its stale suffix on a heartbeat" {
    const allocator = testing.allocator;

    // Follower: 1-7 from term 1, plus a stale 8-10 from a deposed term-2
    // leader. The term-3 leader has its own 8-10, already committed via a
    // third node, so its heartbeat carries leader_commit = 10.
    var follower = try RaftNode.init(allocator, 2, 1000, 16384, .{});
    defer follower.deinit();
    follower.current_term = 2;
    follower.commit_index = 7;
    for (1..8) |i| {
        var e = testEntry(1, i, "old");
        _ = try follower.log.append(&e);
    }
    for (8..11) |i| {
        var e = testEntry(2, i, "stale");
        _ = try follower.log.append(&e);
    }

    // The heartbeat verifies only 1-7; nothing past 7 may commit.
    const hb = try follower.handleAppendEntries(.{
        .term = 3,
        .leader_id = 1,
        .prev_log_index = 7,
        .prev_log_term = 1,
        .entries = &[_]Entry{},
        .leader_commit = 10,
    });
    try testing.expect(hb.success);
    try testing.expectEqual(@as(u64, 7), follower.commit_index);

    // The real 8-10 arrive: conflict truncation, then commit through 10.
    const batch = [_]Entry{
        testEntry(3, 8, "fresh"),
        testEntry(3, 9, "fresh"),
        testEntry(3, 10, "fresh"),
    };
    const resp = try follower.handleAppendEntries(.{
        .term = 3,
        .leader_id = 1,
        .prev_log_index = 7,
        .prev_log_term = 1,
        .entries = &batch,
        .leader_commit = 10,
    });
    try testing.expect(resp.success);
    try testing.expectEqual(@as(u64, 10), follower.commit_index);
    try testing.expectEqual(@as(u64, 3), follower.log.entryTerm(10).?);
}

test "raft node: a heartbeat below the commit index never lowers it" {
    const allocator = testing.allocator;

    var follower = try RaftNode.init(allocator, 2, 1000, 16384, .{});
    defer follower.deinit();
    follower.current_term = 1;
    for (1..6) |i| {
        var e = testEntry(1, i, "e");
        _ = try follower.log.append(&e);
    }
    follower.commit_index = 5;

    // An older heartbeat, delayed in flight, lands after a newer one has
    // already committed past its prev. It verifies only 1-2; commit must hold.
    const resp = try follower.handleAppendEntries(.{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 2,
        .prev_log_term = 1,
        .entries = &[_]Entry{},
        .leader_commit = 6,
    });
    try testing.expect(resp.success);
    try testing.expectEqual(@as(u64, 5), follower.commit_index);
}

test "raft node: a success ack from an earlier term does not advance match or commit" {
    const allocator = testing.allocator;

    // Leader in term 1 proposes index 1; peer 2's ack is delayed.
    var node = try RaftNode.init(allocator, 1, 1000, 16384, .{});
    defer node.deinit();
    node.addPeer(2);
    node.addPeer(3);
    _ = candidacy(&node).?;
    _ = node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2 });
    try testing.expectEqual(Role.leader, node.role);
    _ = try node.propose(.kv_put, 0, 0, "t1");

    // Deposed (a term-2 candidate appears), then re-elected in term 3: the
    // win's noop goes at 3 and a term-3 entry at 4.
    _ = node.handleVoteRequest(.{ .term = 2, .candidate_id = 3, .last_log_index = 1, .last_log_term = 1 });
    try testing.expectEqual(Role.follower, node.role);
    _ = candidacy(&node).?;
    _ = node.handleVoteResponse(.{ .term = 3, .vote_granted = true, .from = 2 });
    try testing.expectEqual(Role.leader, node.role);
    try testing.expectEqual(@as(u64, 3), node.current_term);
    _ = try node.propose(.kv_put, 0, 0, "t3");
    sentAll(&node);

    // The delayed term-1 ack finally arrives, for an index this leadership
    // has not sent it.
    node.handleAppendResponse(.{ .term = 1, .success = true, .match_index = 2, .from = 2 });
    try testing.expectEqual(@as(u64, 0), node.peers[0].match_index);
    try testing.expectEqual(@as(u64, 0), node.commit_index);

    // A current-term ack commits as normal.
    node.handleAppendResponse(.{ .term = 3, .success = true, .match_index = 4, .from = 2 });
    try testing.expectEqual(@as(u64, 4), node.peers[0].match_index);
    try testing.expectEqual(@as(u64, 4), node.commit_index);

    // An ack past our own log (a reply that outlived the log it answered)
    // is clamped to what we hold.
    node.handleAppendResponse(.{ .term = 3, .success = true, .match_index = 9, .from = 2 });
    try testing.expectEqual(@as(u64, 4), node.peers[0].match_index);
    try testing.expectEqual(@as(u64, 5), node.peers[0].next_index);
}

test "raft node: reordered success acks keep match_index monotonic" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 16384, .{});
    defer node.deinit();
    node.addPeer(2);
    node.addPeer(3);
    _ = candidacy(&node).?;
    _ = node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2 });
    for (0..3) |_| _ = try node.propose(.kv_put, 0, 0, "e");
    sentAll(&node);

    // A late ack for 1 lands after the ack for 1-3. It must not rewind
    // progress.
    node.handleAppendResponse(.{ .term = 1, .success = true, .match_index = 3, .from = 2 });
    try testing.expectEqual(@as(u64, 3), node.commit_index);
    node.handleAppendResponse(.{ .term = 1, .success = true, .match_index = 1, .from = 2 });
    try testing.expectEqual(@as(u64, 3), node.peers[0].match_index);
    try testing.expectEqual(@as(u64, 4), node.peers[0].next_index);
}

test "raft node: a fresh follower arms its timer on the first tick" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{ .rng_seed = 7 });
    defer node.deinit();

    try testing.expectEqual(@as(u64, 0), node.election_deadline_ms);
    const first = node.tick(1000);
    try testing.expect(!first.start_election);
    try testing.expect(node.election_deadline_ms >= 1150);
    try testing.expect(node.election_deadline_ms <= 1300);

    // The timer fires once, at its deadline, without anyone else arming it.
    const before = node.tick(node.election_deadline_ms - 1);
    try testing.expect(!before.start_election);
    const due = node.tick(node.election_deadline_ms);
    try testing.expect(due.start_election);
}

test "raft node: jitter comes from the seed, not the node id" {
    const allocator = testing.allocator;

    // Same id, different seeds → different deadlines; same seed → the same.
    var a = try RaftNode.init(allocator, 1, 1000, 4096, .{ .rng_seed = 1, .election_timeout_min_ms = 100, .election_timeout_max_ms = 10_000 });
    defer a.deinit();
    var b = try RaftNode.init(allocator, 1, 1000, 4096, .{ .rng_seed = 2, .election_timeout_min_ms = 100, .election_timeout_max_ms = 10_000 });
    defer b.deinit();
    var c = try RaftNode.init(allocator, 1, 1000, 4096, .{ .rng_seed = 1, .election_timeout_min_ms = 100, .election_timeout_max_ms = 10_000 });
    defer c.deinit();
    a.resetElectionTimer(0);
    b.resetElectionTimer(0);
    c.resetElectionTimer(0);
    try testing.expect(a.election_deadline_ms != b.election_deadline_ms);
    try testing.expectEqual(a.election_deadline_ms, c.election_deadline_ms);

    // Successive arms of one node vary too.
    var seen_different = false;
    var prev = a.election_deadline_ms;
    for (0..8) |_| {
        a.resetElectionTimer(0);
        if (a.election_deadline_ms != prev) seen_different = true;
        prev = a.election_deadline_ms;
    }
    try testing.expect(seen_different);
}

test "raft node: a current-term AppendEntries re-arms the timer even when the log mismatches" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 8192, .{ .rng_seed = 3 });
    defer node.deinit();
    // Already in the leader's term, so nothing but the append itself can
    // touch the timer (a term bump would re-arm through the step-down).
    node.current_term = 1;
    _ = node.tick(1000);
    const armed_at_start = node.election_deadline_ms;

    // Time passes; a heartbeat whose prev_log does not match still proves
    // the leader is alive.
    _ = node.tick(1200);
    const resp = try node.handleAppendEntries(.{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 5,
        .prev_log_term = 1,
        .entries = &[_]Entry{},
        .leader_commit = 0,
    });
    try testing.expect(!resp.success);
    try testing.expect(node.election_deadline_ms >= 1350);
    try testing.expect(node.election_deadline_ms > armed_at_start);

    // A stale-term append proves nothing and leaves the timer alone.
    node.current_term = 5;
    const deadline_before = node.election_deadline_ms;
    _ = node.tick(1400);
    _ = try node.handleAppendEntries(.{
        .term = 3,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &[_]Entry{},
        .leader_commit = 0,
    });
    try testing.expectEqual(deadline_before, node.election_deadline_ms);
}

test "raft node: a granted vote and an election start re-arm the timer" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 4096, .{ .rng_seed = 4 });
    defer node.deinit();
    _ = node.tick(1000);

    _ = node.tick(1250);
    const granted = node.handleVoteRequest(.{ .term = 1, .candidate_id = 1, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(granted.vote_granted);
    try testing.expect(node.election_deadline_ms >= 1400);

    // A refused vote (already voted this term) does not.
    const deadline_after_grant = node.election_deadline_ms;
    _ = node.tick(1300);
    const refused = node.handleVoteRequest(.{ .term = 1, .candidate_id = 3, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(!refused.vote_granted);
    try testing.expectEqual(deadline_after_grant, node.election_deadline_ms);

    _ = node.tick(2000);
    _ = candidacy(&node).?;
    try testing.expect(node.election_deadline_ms >= 2150);
}

const SinkRecorder = struct {
    term: u64 = 0,
    voted_for: NodeId = 0,
    calls: u32 = 0,
    fail: bool = false,

    fn persist(ctx: *anyopaque, term: u64, voted_for: NodeId) bool {
        const self: *SinkRecorder = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        if (self.fail) return false;
        self.term = term;
        self.voted_for = voted_for;
        return true;
    }

    fn sink(self: *SinkRecorder) HardStateSink {
        return .{ .ctx = @ptrCast(self), .persist = persist };
    }
};

test "raft node: term adoption and votes reach the sink before the node acts on them" {
    const allocator = testing.allocator;

    var rec = SinkRecorder{};
    var node = try RaftNode.init(allocator, 2, 1000, 4096, .{ .rng_seed = 5 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();

    // A higher-term request: the term is persisted by the step-down, the
    // vote by the grant.
    const resp = node.handleVoteRequest(.{ .term = 4, .candidate_id = 1, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(resp.vote_granted);
    try testing.expectEqual(@as(u64, 4), rec.term);
    try testing.expectEqual(@as(NodeId, 1), rec.voted_for);

    // Starting an election persists the new term and the self-vote.
    _ = candidacy(&node).?;
    try testing.expectEqual(@as(u64, 5), rec.term);
    try testing.expectEqual(@as(NodeId, 2), rec.voted_for);

    // A higher-term AppendEntries persists the adopted term with no vote.
    _ = try node.handleAppendEntries(.{ .term = 9, .leader_id = 3, .prev_log_index = 0, .prev_log_term = 0, .entries = &[_]Entry{}, .leader_commit = 0 });
    try testing.expectEqual(@as(u64, 9), rec.term);
    try testing.expectEqual(NO_VOTE, rec.voted_for);
    try testing.expectEqual(@as(u64, 0), node.persist_failures);
}

test "raft node: a vote that cannot be made durable is not granted" {
    const allocator = testing.allocator;

    var rec = SinkRecorder{ .fail = true };
    var node = try RaftNode.init(allocator, 2, 1000, 4096, .{ .rng_seed = 6 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();

    const resp = node.handleVoteRequest(.{ .term = 1, .candidate_id = 1, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(!resp.vote_granted);
    try testing.expectEqual(NO_VOTE, node.voted_for);
    try testing.expect(node.persist_failures > 0);

    // Once the disk is back, the same request is granted.
    rec.fail = false;
    const again = node.handleVoteRequest(.{ .term = 1, .candidate_id = 1, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(again.vote_granted);
    try testing.expectEqual(@as(NodeId, 1), rec.voted_for);
}

test "raft node: bootstrap after a restart opens a new term and continues the log" {
    const allocator = testing.allocator;

    var rec = SinkRecorder{};
    var node = try RaftNode.init(allocator, 1, 1000, 8192, .{ .rng_seed = 8 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();

    // The durable state a restart hands back: term 3, and a log of 1-4.
    node.current_term = 3;
    for (1..5) |i| {
        var e = testEntry(if (i < 3) 1 else 3, i, "restored");
        _ = try node.log.append(&e);
    }

    try node.bootstrap();
    try testing.expectEqual(Role.leader, node.role);
    try testing.expectEqual(@as(u64, 4), node.current_term);
    try testing.expectEqual(@as(u64, 4), rec.term);
    try testing.expectEqual(@as(NodeId, 1), rec.voted_for);
    // The noop sits at 5, in term 4 — no gap, no phantom index.
    try testing.expectEqual(@as(u64, 5), node.log.lastIndex());
    try testing.expectEqual(@as(u64, 4), node.log.entryTerm(5).?);
    try testing.expectEqual(@as(u64, 5), node.commit_index);
    // Nothing was applied by bootstrapping; the restored tail and the noop
    // are the owner's to drain.
    try testing.expectEqual(@as(u64, 0), node.last_applied);
    const r = try node.propose(.kv_put, 0, 0, "next");
    try testing.expectEqual(@as(u64, 6), r.index);
    try testing.expectEqual(@as(u64, 4), r.term);
}

test "raft node: bootstrap refuses to lead a term it cannot persist" {
    const allocator = testing.allocator;

    var rec = SinkRecorder{ .fail = true };
    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{ .rng_seed = 9 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();

    try testing.expectError(error.HardStateNotDurable, node.bootstrap());
    try testing.expectEqual(Role.follower, node.role);
    try testing.expectEqual(@as(u64, 0), node.log.lastIndex());
}

test "raft node: an election whose self-vote cannot be persisted does not start" {
    const allocator = testing.allocator;

    var rec = SinkRecorder{ .fail = true };
    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{ .rng_seed = 10 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();
    node.addPeer(2);
    node.addPeer(3);
    node.current_term = 4;
    _ = node.tick(1000);

    try testing.expect(candidacy(&node) == null);
    try testing.expectEqual(Role.follower, node.role);
    try testing.expectEqual(@as(u64, 4), node.current_term);
    try testing.expectEqual(NO_VOTE, node.voted_for);
    try testing.expectEqual(@as(u64, 0), node.elections_started);

    // A vote in term 5 is still available to another candidate: nothing
    // of the aborted attempt survives to conflict with it.
    rec.fail = false;
    const resp = node.handleVoteRequest(.{ .term = 5, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(resp.vote_granted);
    try testing.expectEqual(@as(u64, 5), rec.term);
    try testing.expectEqual(@as(NodeId, 2), rec.voted_for);
}

test "raft node: events before the first tick do not arm a deadline in the past" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 2, 1000, 4096, .{ .rng_seed = 11 });
    defer node.deinit();
    node.current_term = 1;

    // A leader speaks before this node has ever ticked.
    _ = try node.handleAppendEntries(.{ .term = 1, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &[_]Entry{}, .leader_commit = 0 });
    try testing.expectEqual(@as(u64, 0), node.election_deadline_ms);

    // The first tick arms from the real clock; it must not elect.
    const first = node.tick(50_000);
    try testing.expect(!first.start_election);
    try testing.expect(node.election_deadline_ms >= 50_150);
}

test "raft node: a term more than 2^32 ahead is refused, not adopted" {
    var node = try RaftNode.init(testing.allocator, 1, 1, 4096, .{});
    defer node.deinit();
    try node.bootstrap();
    const before = node.current_term;

    const absurd: u64 = before + (1 << 32) + 1;
    const vr = node.handleVoteRequest(.{ .term = absurd, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(!vr.vote_granted);
    try testing.expectEqual(before, node.current_term);
    const ar = try node.handleAppendEntries(.{ .term = absurd, .leader_id = 2, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{}, .leader_commit = 0 });
    try testing.expect(!ar.success);
    try testing.expectEqual(before, node.current_term);
    try testing.expectEqual(Role.leader, node.role);
    node.handleAppendResponse(.{ .term = absurd, .success = false, .match_index = 0, .from = 2 });
    try testing.expectEqual(before, node.current_term);

    // A term merely ahead is an election we missed, and is adopted.
    _ = try node.handleAppendEntries(.{ .term = before + 5, .leader_id = 2, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{}, .leader_commit = 0 });
    try testing.expectEqual(before + 5, node.current_term);
    try testing.expectEqual(Role.follower, node.role);
}

test "raft node: an election is a poll first; the term is spent only once a majority would vote" {
    var node = try RaftNode.init(testing.allocator, 1, 1, 4096, .{});
    defer node.deinit();
    node.addPeer(2);
    node.addPeer(3);

    const poll = node.startElection().?;
    try testing.expect(poll.is_pre_vote);
    try testing.expectEqual(@as(u64, 1), poll.term);
    try testing.expectEqual(@as(u64, 0), node.current_term);
    try testing.expectEqual(Role.follower, node.role);

    // A poll the timer outlives is asked again, not turned into an election.
    const again = node.startElection().?;
    try testing.expect(again.is_pre_vote);
    try testing.expectEqual(@as(u64, 0), node.current_term);
    // One "no" changes nothing; one "yes" is a majority of three.
    try testing.expect(node.handleVoteResponse(.{ .term = 0, .vote_granted = false, .from = 3, .is_pre_vote = true }) == .none);
    const outcome = node.handleVoteResponse(.{ .term = 0, .vote_granted = true, .from = 2, .is_pre_vote = true });
    switch (outcome) {
        .elect => |req| {
            try testing.expect(!req.is_pre_vote);
            try testing.expectEqual(@as(u64, 1), req.term);
        },
        else => return error.TestUnexpectedResult,
    }
    try testing.expectEqual(Role.candidate, node.role);
    try testing.expectEqual(@as(u64, 1), node.current_term);
    try testing.expect(node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2 }) == .won);
}

test "raft node: a follower answers a poll without adopting or persisting anything" {
    var rec = SinkRecorder{};
    var node = try RaftNode.init(testing.allocator, 2, 1, 4096, .{});
    defer node.deinit();
    node.hard_state_sink = rec.sink();
    node.addPeer(1);
    node.addPeer(3);
    const resp = node.handleVoteRequest(.{ .term = 5, .candidate_id = 1, .last_log_index = 0, .last_log_term = 0, .is_pre_vote = true });
    try testing.expect(resp.is_pre_vote);
    try testing.expect(resp.vote_granted);
    try testing.expectEqual(@as(u64, 0), node.current_term);
    try testing.expectEqual(NO_VOTE, node.voted_for);
    try testing.expectEqual(@as(u32, 0), rec.calls);
}

test "raft node: a vote request within an election timeout of a leader's heartbeat is ignored, term and all" {
    var node = try RaftNode.init(testing.allocator, 2, 1, 4096, .{});
    defer node.deinit();
    node.addPeer(1);
    node.addPeer(3);
    _ = node.tick(1000);
    _ = try node.handleAppendEntries(.{ .term = 1, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{}, .leader_commit = 0 });
    try testing.expectEqual(@as(u64, 1), node.current_term);

    // A disrupted node asks with a higher term shortly after.
    _ = node.tick(1050);
    const soon = node.handleVoteRequest(.{ .term = 7, .candidate_id = 3, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(!soon.vote_granted);
    try testing.expectEqual(@as(u64, 1), node.current_term);

    // Long after the last heartbeat the same request is a real failover.
    _ = node.tick(1000 + node.config.election_timeout_min_ms + 1);
    const later = node.handleVoteRequest(.{ .term = 7, .candidate_id = 3, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(later.vote_granted);
    try testing.expectEqual(@as(u64, 7), node.current_term);
}

test "raft node: a new leader's noop lets earlier terms' entries commit before any client writes" {
    var node = try RaftNode.init(testing.allocator, 1, 1, 8192, .{});
    defer node.deinit();
    node.addPeer(2);
    node.addPeer(3);
    // An entry from an earlier leadership sits uncommitted.
    var old = testEntry(1, 1, "old");
    _ = try node.log.append(&old);
    node.current_term = 1;

    _ = candidacy(&node).?;
    try testing.expect(node.handleVoteResponse(.{ .term = 2, .vote_granted = true, .from = 2 }) == .won);
    try testing.expectEqual(@as(u64, 2), node.log.lastIndex());
    try testing.expectEqual(@as(u64, 2), node.log.entryTerm(2).?);
    // The noop replicates to one follower: both entries commit.
    sentAll(&node);
    node.handleAppendResponse(.{ .term = 2, .success = true, .match_index = 2, .from = 2 });
    try testing.expectEqual(@as(u64, 2), node.commit_index);
}

test "raft node: a leader that hears from no majority for an election timeout steps down" {
    var node = try RaftNode.init(testing.allocator, 1, 1, 4096, .{});
    defer node.deinit();
    node.addPeer(2);
    node.addPeer(3);
    _ = node.tick(1000);
    _ = candidacy(&node).?;
    try testing.expect(node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2 }) == .won);
    const timeout = node.config.election_timeout_max_ms;

    // Peer 2 keeps answering; that is a majority with us.
    _ = node.tick(1000 + timeout / 2);
    node.handleAppendResponse(.{ .term = 1, .success = true, .match_index = 0, .from = 2 });
    var r = node.tick(1000 + timeout + 100);
    try testing.expect(!r.step_down);
    try testing.expectEqual(Role.leader, node.role);

    // Then nobody does: after a full timeout with no majority it steps down
    // rather than holding proposals it can never commit.
    r = node.tick(1000 + timeout / 2 + timeout + 1);
    try testing.expect(r.step_down);
    try testing.expectEqual(Role.follower, node.role);
    try testing.expectEqual(@as(u64, 1), node.current_term);
}

test "raft node: a rejection carries where the follower's log stops agreeing, and the leader jumps there" {
    var follower = try RaftNode.init(testing.allocator, 2, 1, 16384, .{});
    defer follower.deinit();
    // Follower: 1-3 in term 1, 4-6 in term 2.
    for (1..7) |i| {
        var e = testEntry(if (i <= 3) 1 else 2, i, "f");
        _ = try follower.log.append(&e);
    }
    follower.current_term = 3;
    // A probe past the end names the last index.
    const past = try follower.handleAppendEntries(.{ .term = 3, .leader_id = 1, .prev_log_index = 10, .prev_log_term = 3, .entries = &.{}, .leader_commit = 0 });
    try testing.expect(!past.success);
    try testing.expectEqual(@as(u64, 6), past.hint_index);
    // A probe inside a run of the wrong term names the index before it.
    const inside = try follower.handleAppendEntries(.{ .term = 3, .leader_id = 1, .prev_log_index = 5, .prev_log_term = 3, .entries = &.{}, .leader_commit = 0 });
    try testing.expect(!inside.success);
    try testing.expectEqual(@as(u64, 3), inside.hint_index);

    // The leader at next_index 11 jumps to 4, not 10.
    var leader = try RaftNode.init(testing.allocator, 1, 1, 16384, .{});
    defer leader.deinit();
    leader.addPeer(2);
    leader.addPeer(3);
    _ = candidacy(&leader).?;
    _ = leader.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 3 });
    leader.peers[0].next_index = 11;
    leader.handleAppendResponse(.{ .term = 1, .success = false, .match_index = 6, .from = 2, .hint_index = 3 });
    try testing.expectEqual(@as(u64, 4), leader.peers[0].next_index);

    // A follower that crashed before flushing what it acked answers below
    // the recorded match: the leader follows it down instead of probing
    // above the follower's log forever.
    leader.peers[0].match_index = 8;
    leader.peers[0].next_index = 9;
    leader.handleAppendResponse(.{ .term = 1, .success = false, .match_index = 0, .from = 2, .hint_index = 1 });
    try testing.expectEqual(@as(u64, 2), leader.peers[0].next_index);
    try testing.expectEqual(@as(u64, 1), leader.peers[0].match_index);
}

test "raft node: a conflict below the commit index is refused when commits are durable, and rewinds when they are not" {
    var lossy = try RaftNode.init(testing.allocator, 2, 1, 8192, .{ .durable_commits = false });
    defer lossy.deinit();
    for (1..4) |i| {
        var e = testEntry(1, i, "x");
        _ = try lossy.log.append(&e);
    }
    lossy.current_term = 1;
    lossy.commit_index = 3;
    lossy.last_applied = 3;
    const replacement = testEntry(2, 2, "y");
    const resp = try lossy.handleAppendEntries(.{ .term = 2, .leader_id = 1, .prev_log_index = 1, .prev_log_term = 1, .entries = &.{replacement}, .leader_commit = 0 });
    try testing.expect(resp.success);
    try testing.expectEqual(@as(u64, 2), lossy.log.lastIndex());
    try testing.expectEqual(@as(u64, 2), lossy.log.entryTerm(2).?);
    try testing.expectEqual(@as(u64, 1), lossy.commit_index);
    try testing.expectEqual(@as(u64, 1), lossy.last_applied);
    try testing.expectEqual(@as(u64, 1), lossy.committed_conflicts);

    var node = try RaftNode.init(testing.allocator, 2, 1, 8192, .{ .durable_commits = true });
    defer node.deinit();
    for (1..4) |i| {
        var e = testEntry(1, i, "x");
        _ = try node.log.append(&e);
    }
    node.current_term = 1;
    node.commit_index = 3;
    const conflicting = testEntry(2, 2, "y");
    try testing.expectError(error.CommittedConflict, node.handleAppendEntries(.{ .term = 2, .leader_id = 1, .prev_log_index = 1, .prev_log_term = 1, .entries = &.{conflicting}, .leader_commit = 0 }));
    try testing.expectEqual(@as(u64, 3), node.log.lastIndex());
}

test "raft node: a lone member elects itself when its timer fires" {
    var node = try RaftNode.init(testing.allocator, 1, 1, 4096, .{ .rng_seed = 3 });
    defer node.deinit();
    _ = node.tick(1000);
    var now: u64 = 1000;
    var fired = false;
    while (now < 2000) : (now += 10) {
        if (node.tick(now).start_election) {
            fired = true;
            break;
        }
    }
    try testing.expect(fired);
    const req = node.startElection().?;
    try testing.expect(!req.is_pre_vote);
    try testing.expectEqual(Role.leader, node.role);
    try testing.expectEqual(@as(u64, 1), node.current_term);
    try testing.expectEqual(@as(NodeId, 1), node.leader_id);
}

test "raft node: a config entry is the membership from the moment it is appended, on the leader and on a follower" {
    var leader = try RaftNode.init(testing.allocator, 1, 1, 16384, .{});
    defer leader.deinit();
    leader.addPeer(2);
    _ = candidacy(&leader).?;
    _ = leader.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2 });
    var buf: [membership.MAX_SIZE]u8 = undefined;
    const three = membership.encode(&.{ 1, 2, 3 }, &buf);
    const r = try leader.propose(.raft_config, 0, 0, three);
    // Not committed (one peer, no ack), yet the peers are already 2 and 3.
    try testing.expectEqual(@as(u8, 2), leader.peer_count);
    try testing.expectEqual(r.index, leader.membership_index);
    try testing.expectEqual(r.index, leader.log.last_config_index);
    try testing.expect(leader.commit_index < r.index);

    var follower = try RaftNode.init(testing.allocator, 3, 1, 16384, .{});
    defer follower.deinit();
    follower.timer_enabled = false;
    var noop = testEntry(1, 1, "");
    noop.header.entry_type = @intFromEnum(EntryType.raft_noop);
    noop.header.crc32c = noop.computeCrc();
    var cfg = testEntry(1, 2, three);
    cfg.header.entry_type = @intFromEnum(EntryType.raft_config);
    cfg.header.crc32c = cfg.computeCrc();
    const resp = try follower.handleAppendEntries(.{ .term = 1, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{ noop, cfg }, .leader_commit = 0 });
    try testing.expect(resp.success);
    try testing.expectEqual(@as(u8, 2), follower.peer_count);
    try testing.expect(follower.timer_enabled);
    var ids: [membership.MAX_MEMBERS]NodeId = undefined;
    try testing.expectEqualSlices(NodeId, &.{ 3, 1, 2 }, follower.memberIds(&ids));
}

test "raft node: membership is what the config entry says, and an unnamed node does not elect itself" {
    var node = try RaftNode.init(testing.allocator, 5, 1, 4096, .{});
    defer node.deinit();
    node.setMembership(&.{ 1, 2, 3 }, 7);
    try testing.expectEqual(@as(u8, 3), node.peer_count);
    try testing.expect(!node.timer_enabled);
    _ = node.tick(1000);
    _ = node.tick(1000 + node.config.election_timeout_max_ms * 3);
    try testing.expect(!node.tick(1000 + node.config.election_timeout_max_ms * 4).start_election);

    // Named now: peers are the others, progress for kept peers survives.
    node.setMembership(&.{ 1, 2, 3 }, 7);
    node.peers[0].match_index = 9;
    node.setMembership(&.{ 1, 2, 3, 5 }, 8);
    try testing.expect(node.timer_enabled);
    try testing.expectEqual(@as(u8, 3), node.peer_count);
    try testing.expectEqual(@as(u64, 9), node.peers[0].match_index);
    try testing.expectEqual(@as(u8, 3), node.quorum());
    var ids: [MAX_PEERS + 1]NodeId = undefined;
    try testing.expectEqual(@as(usize, 4), node.memberIds(&ids).len);

    // A truncation below the entry that named this node reverts to the
    // committed membership, which did not.
    node.commitMembership(&.{ 1, 2, 3 });
    node.log.truncateAfter(5);
    node.truncatedBelowMembership(5);
    try testing.expect(!node.timer_enabled);
    try testing.expectEqual(@as(u8, 3), node.peer_count);
}

test "raft node: a batch whose entries are not the ones after prev is refused before the log is touched" {
    var f = try RaftNode.init(testing.allocator, 2, 1, 16384, .{});
    defer f.deinit();
    f.current_term = 1;
    const skip = testEntry(1, 2, "x");
    try testing.expectError(error.MalformedBatch, f.handleAppendEntries(.{ .term = 1, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{skip}, .leader_commit = 5 }));
    const zero = testEntry(1, 0, "x");
    try testing.expectError(error.MalformedBatch, f.handleAppendEntries(.{ .term = 1, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{zero}, .leader_commit = 5 }));
    try testing.expectEqual(@as(u64, 0), f.log.lastIndex());
    try testing.expectEqual(@as(u64, 0), f.commit_index);
}

test "raft node: a lone member elected from a restored log commits its noop and the tail at once" {
    var node = try RaftNode.init(testing.allocator, 1, 1000, 16384, .{ .rng_seed = 3 });
    defer node.deinit();
    for (1..4) |i| {
        var e = testEntry(1, i, "r");
        _ = try node.log.append(&e);
    }
    node.current_term = 1;
    try testing.expectEqual(@as(u64, 0), node.commit_index);
    _ = node.startElectionNow().?;
    try testing.expectEqual(Role.leader, node.role);
    try testing.expectEqual(@as(u64, 4), node.log.lastIndex());
    try testing.expectEqual(@as(u64, 4), node.commit_index);
}

test "raft node: a membership naming nobody leaves the timer off, and a bootstrapped leader is not deposed by the member it adds" {
    var node = try RaftNode.init(testing.allocator, 1, 1000, 16384, .{ .rng_seed = 3 });
    defer node.deinit();
    node.setMembership(&.{}, 0);
    try testing.expect(!node.timer_enabled);
    node.setMembership(&.{1}, 5);
    try testing.expect(node.timer_enabled);

    var leader = try RaftNode.init(testing.allocator, 1, 1000, 16384, .{ .rng_seed = 3 });
    defer leader.deinit();
    try leader.bootstrap();
    const max = leader.config.election_timeout_max_ms;
    _ = leader.tick(1000);
    // Long into a lone reign, a member joins. It has not spoken; it counts
    // as heard from now, so the leader is not deposed for adding it...
    const joined_at = 1000 + 10 * max;
    _ = leader.tick(joined_at);
    leader.setMembership(&.{ 1, 2 }, 2);
    const r = leader.tick(joined_at + max);
    try testing.expect(!r.step_down);
    try testing.expectEqual(Role.leader, leader.role);
    // ...but a member that never answers still costs it the lead.
    const later = leader.tick(joined_at + 2 * max + 1);
    try testing.expect(later.step_down);
}
