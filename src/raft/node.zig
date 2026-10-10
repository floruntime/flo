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
    /// The peer's last response said it is guarded (`LostLog`): its acks
    /// keep this leader in office but count toward no commit, since what
    /// it acks it may hold for a leader the group has already replaced.
    guarded: bool = false,
    /// Counts toward quorum, commit and votes; a replica only takes the log.
    voter: bool = true,
    /// Successful acks in a row at or past the commit index; three make
    /// it caught up (`membership.Progress`).
    caught_up_streak: u8 = 0,
    /// When this leader first saw the peer as a member that had not caught
    /// up: the aging clock. A new leader starts it again.
    joining_since_ms: u64 = 0,
    /// The peer's wall clock as it last reported it, and this node's
    /// monotonic time when the report arrived (0: none yet).
    clock_ns: u64 = 0,
    clock_at_ms: u64 = 0,
};

/// Acks in a row at the commit index that make a peer caught up.
pub const CAUGHT_UP_ACKS = 3;

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
    /// A leader counts its own copy of an entry toward commit only once the
    /// owner reports it on disk (`markDurable`). Set with sync durability;
    /// otherwise a leader whose flush failed would ack a write that isn't
    /// on any disk. Off by default: the simulator has no flush to report.
    self_counts_when_durable: bool = false,
    /// Seed for election-timeout jitter. 0 draws one from OS entropy; a
    /// simulation passes a per-node seed so a run replays exactly.
    rng_seed: u64 = 0,
    /// How long a sent batch may go unanswered; 0 means twice the
    /// heartbeat. A simulation whose network is slower than that sets it.
    rpc_timeout_ms: u64 = 0,

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
        return if (self.rpc_timeout_ms != 0) self.rpc_timeout_ms else 2 * self.heartbeat_interval_ms;
    }
};

/// Where the node writes its hard state: term, vote and whether the
/// lost-log guard is on. `persist` returns only once the state is durable
/// and reports its own failures. A false return means the state is not
/// durable: a vote is then not granted, an election is not started,
/// bootstrap fails and a guard neither starts nor ends; an adopted higher
/// term is kept in memory anyway and counted in `persist_failures`.
pub const HardStateSink = struct {
    ctx: *anyopaque,
    persist: *const fn (ctx: *anyopaque, term: u64, voted_for: NodeId, lost_log: bool) bool,
};

/// Puts everything the log holds on disk, before a config entry takes
/// effect: a member that acted on a config, then crashed and came back
/// without it, would count a different majority than the rest. True once
/// durable.
pub const LogFlushSink = struct {
    ctx: *anyopaque,
    flush: *const fn (ctx: *anyopaque) bool,
};

/// Where a node that lost its log or its hard state stands (a new node
/// with no log looks the same). Before it lost them it may have voted in a
/// term, or acked entries a quorum counted, so until it has caught up with
/// a leader and the term is confirmed it grants no vote, never campaigns,
/// and leaders count none of its acks; after, it votes only in later
/// terms. Persisted with the hard state until it is `none`.
pub const LostLog = enum {
    none,
    /// Following a leader until this log holds everything the leader had
    /// committed when it sent a batch.
    catching_up,
    /// Asking members for their terms: of the latest committed config and
    /// every config after it.
    confirming,
};

/// A guarded node asking a member for its term and its latest config.
pub const TermCheckRequest = struct {
    term: u64,
    from: NodeId,
};

pub const TermCheckResponse = struct {
    term: u64,
    from: NodeId,
    /// The responder is guarded itself: its term is no evidence of the
    /// votes this node cast, and its answer does not count.
    guarded: bool = false,
    /// The responder's latest config and its voters: when newer than any
    /// the asker knows, those voters are checked too.
    config_index: u64,
    config_term: u64,
    member_count: u8,
    members: [membership.MAX_MEMBERS]NodeId,
};

/// Result of processing a tick (timer advancement).
pub const TickResult = struct {
    /// Actions the caller must take after the tick.
    send_heartbeats: bool = false,
    start_election: bool = false,
    /// A guarded node is confirming the term: send `termCheckRequest` to
    /// every node in `termCheckTargets`.
    send_term_check: bool = false,
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
    /// The responder's wall clock, so a leader holds its voters' clocks the
    /// moment it wins (`RaftNode.nextStamp`).
    clock_ns: u64 = 0,
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
    /// The commit index of the batch this answers, echoed: whether the
    /// peer was caught up is judged against what the leader had committed
    /// when it sent, not by the time the answer arrives.
    leader_commit: u64 = 0,
    /// The responder is guarded (`LostLog`): the leader counts none of its
    /// acks toward commit.
    guarded: bool = false,
    /// The responder's wall clock (`RaftNode.nextStamp`).
    clock_ns: u64 = 0,
};

/// Where a node reads the wall clock. A simulation gives each node its own.
pub const WallClock = struct {
    ctx: ?*anyopaque = null,
    now_ns: *const fn (ctx: ?*anyopaque) u64 = systemWallNs,

    fn systemWallNs(_: ?*anyopaque) u64 {
        return @intCast(@max(0, @import("stdx").time.nanoTimestamp()));
    }

    pub fn read(self: WallClock) u64 {
        return self.now_ns(self.ctx);
    }
};

/// How far a leader's stamps may run ahead of its voters' clocks.
pub const STAMP_LEAD_NS: u64 = std.time.ns_per_s;
/// A stamp held this far behind the leader's own clock is worth saying.
pub const STAMP_SKEW_WARN_NS: u64 = 5 * std.time.ns_per_s;

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

const MemberSet = struct {
    ids: [MAX_PEERS + 1]NodeId = undefined,
    count: u8 = 0,

    fn slice(self: *const MemberSet) []const NodeId {
        return self.ids[0..self.count];
    }
};

/// A config entry the log holds: what the term check reads, so it does not
/// depend on the owner having applied the entry yet.
const ConfigRecord = struct {
    index: u64,
    term: u64,
    config: membership.Config,
};

/// The voters of `cfg`, for the term check: only their votes made a
/// leader, so only they can say which terms were spent.
fn voterSet(cfg: *const membership.Config) MemberSet {
    var set: MemberSet = .{};
    for (cfg.memberSlice()) |m| {
        if (!m.voter) continue;
        set.ids[set.count] = m.id;
        set.count += 1;
    }
    return set;
}

/// A guarded node's term check: the configs whose members must answer, and
/// who has.
const TermCheck = struct {
    /// The latest committed config, the node's own latest, and newer ones
    /// members reported.
    sets: [4]MemberSet = @splat(.{}),
    set_count: u8 = 0,
    newest_index: u64 = 0,
    newest_term: u64 = 0,
    answered: [16]NodeId = undefined,
    answered_count: u8 = 0,
    sent_ms: u64 = 0,
};

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
    /// The highest index this node has on disk, as its owner reports it.
    /// Read only when `config.self_counts_when_durable`.
    durable_index: u64,
    /// Set by the owner when an entry it owed the disk is gone: nothing
    /// after it can be made durable, so proposals are refused.
    writes_stopped: bool = false,
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
    /// "I would vote for you" turns into a real election. This is the term
    /// the poll asked about, 0 for none. Answers count only while it is
    /// `current_term + 1` (`pollOpen`), so any change of term closes the poll.
    pre_vote_term: u64,
    /// Last tick a current-term leader spoke to us. A vote request within
    /// one election timeout of that is a disrupted node's, not a real
    /// failover, and is ignored.
    last_leader_contact_ms: u64,
    /// When this node became leader; counts as contact with every peer for
    /// check-quorum until they answer.
    leader_since_ms: u64,
    /// This node is a voter in its latest config, or, with none, stands
    /// alone: it may campaign and counts toward its own quorum. False for
    /// a joiner the log does not yet name, and for a replica.
    timer_enabled: bool,
    /// The latest config names this node, as a voter or a replica.
    self_named: bool = true,
    /// The latest config appended, and the latest committed: proposals are
    /// checked against the one, a truncation falls back to the other.
    latest_config: membership.Config = .{},
    committed_config: membership.Config = .{},
    /// A leader whose committed config no longer makes it a voter stepped
    /// down; the owner says why.
    left_group: bool = false,
    log_flush_sink: ?LogFlushSink = null,
    wall_clock: WallClock = .{},
    /// The stamp of the last entry applied, and this node's monotonic time
    /// when it was: a follower's "now" for reads (`now`).
    applied_stamp: u64 = 0,
    applied_at_ms: u64 = 0,
    /// How far the last stamp was from this leader's own clock, which way,
    /// and the voter whose clock set the voters' clock, for the skew metric
    /// and warning.
    stamp_skew_ns: u64 = 0,
    stamp_clock_behind: bool = false,
    stamp_held_by: NodeId = NO_VOTE,
    /// Index of the config entry the current membership came from, so a
    /// truncation reaching below it reverts to the committed one.
    membership_index: u64,
    /// Times a leader's log conflicted with an entry this node had already
    /// committed and applied, each rewinding commit and apply below it.
    /// Only possible without durable commits; the owner watches it, since
    /// what it applied above the cut is not what the log now says.
    committed_conflicts: u64,
    /// Term of the config entry at `membership_index`, 0 when unknown.
    membership_term: u64 = 0,
    /// The most recent config entries appended, oldest first; a truncation
    /// drops those it cuts.
    config_records: [4]ConfigRecord = undefined,
    config_record_count: u8 = 0,

    // ── Lost-log guard ─────────────────────────────────────────────────
    lost_log: LostLog = .none,
    check: TermCheck = .{},
    /// A guarded node neither sends a term check nor counts an answer until
    /// one maximum election timeout, plus an RPC timeout of slack, after it
    /// booted. That rests on candidates stopping counting votes at their
    /// election deadline, at most one maximum timeout after they stood
    /// (`handleVoteResponse`): a candidacy it voted for before it lost its
    /// vote has by then either won, so the check sees its term, or given
    /// up. Armed at the first tick, the first clock the node has.
    hold_pending: bool = false,
    hold_until_ms: u64 = 0,

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
            .durable_index = 0,
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
            .pre_vote_term = 0,
            .last_leader_contact_ms = 0,
            .leader_since_ms = 0,
            .timer_enabled = true,
            .membership_index = 0,
            .committed_conflicts = 0,
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
    /// become exactly its members minus this node, keeping the progress of
    /// any that stay; a voter counts toward quorum from the moment the
    /// entry is appended, a replica never. This node campaigns as a voter
    /// (and in one case besides: `mayCampaign`). `index` is the entry's;
    /// when it commits the caller
    /// passes it again through `commitMembership`, and a truncation below
    /// it reverts to the committed one.
    pub fn setMembership(self: *RaftNode, cfg: *const membership.Config, index: u64) void {
        var new_peers: [MAX_PEERS]PeerState = undefined;
        var new_ids: [MAX_PEERS]NodeId = undefined;
        // Votes granted so far follow their peer, not its position.
        var new_granted: [MAX_PEERS]bool = @splat(false);
        var n: u8 = 0;
        for (cfg.memberSlice()) |m| {
            if (m.id == self.id) continue;
            if (n >= MAX_PEERS) break;
            new_ids[n] = m.id;
            if (self.peerIndex(m.id)) |i| new_granted[n] = self.vote_granted_by[i];
            // A peer named just now has not spoken yet; it counts as heard
            // from now, or check-quorum would depose a leader that adds a
            // member more than a failover after it began.
            new_peers[n] = if (self.peerIndex(m.id)) |i| self.peers[i] else .{
                .next_index = self.log.lastIndex() + 1,
                .match_index = 0,
                .inflight = false,
                .last_contact_ms = self.current_time_ms,
                .joining_since_ms = self.current_time_ms,
            };
            new_peers[n].voter = m.voter;
            n += 1;
        }
        self.peers = new_peers;
        self.peer_ids = new_ids;
        self.vote_granted_by = new_granted;
        self.peer_count = n;
        self.self_named = cfg.names(self.id);
        self.timer_enabled = cfg.isVoter(self.id);
        self.latest_config = cfg.*;
        self.membership_index = index;
        if (self.role == .candidate or self.pollOpen()) {
            self.votes_needed = self.quorum();
            self.votes_received = @intFromBool(self.timer_enabled);
            for (self.peers[0..n], new_granted[0..n]) |p, g| self.votes_received += @intFromBool(g and p.voter);
        }
    }

    /// Set a membership of voters only: a group founded or forced into
    /// being, and tests.
    pub fn setVoters(self: *RaftNode, member_ids: []const NodeId, index: u64) void {
        const cfg = membership.Config.ofVoters(member_ids);
        self.setMembership(&cfg, index);
    }

    /// The config entry at `index` committed: it is now the membership a
    /// truncation falls back to. (A leader it drops has already stepped
    /// down, at commit: `advanceCommitIndex`.)
    pub fn commitMembership(self: *RaftNode, cfg: *const membership.Config) void {
        self.committed_config = cfg.*;
    }

    /// A voter campaigns. So does a voter of the committed config that a
    /// newer, uncommitted one drops (a leader that removed itself and lost
    /// office before the change committed): its log may be the only one
    /// holding the change, so the voters it leaves would elect no one
    /// without it. It counts only their votes, finishes the change and
    /// steps down.
    pub fn mayCampaign(self: *const RaftNode) bool {
        if (self.timer_enabled) return true;
        return self.membership_index > self.commit_index and self.committed_config.isVoter(self.id);
    }

    /// How many voters there were before the voter count last changed, as
    /// far as the configs this node still records go: more than now means
    /// the last such change was a removal.
    pub fn voterCountBefore(self: *const RaftNode) ?u8 {
        const current = self.latest_config.voterCount();
        var i = self.config_record_count;
        while (i > 0) {
            i -= 1;
            const rec = &self.config_records[i];
            if (rec.index >= self.membership_index) continue;
            const before = rec.config.voterCount();
            if (before != current) return before;
        }
        return null;
    }

    /// Peers that vote.
    pub fn voterPeers(self: *const RaftNode) u8 {
        var n: u8 = 0;
        for (self.peers[0..self.peer_count]) |p| n += @intFromBool(p.voter);
        return n;
    }

    /// The current member ids, this node included when named.
    pub fn memberIds(self: *const RaftNode, out: *[MAX_PEERS + 1]NodeId) []NodeId {
        var n: usize = 0;
        if (self.self_named) {
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
        const cfg = membership.decode(e.payload) orelse {
            log.err("Raft: config entry at index {d} is not a membership; membership unchanged", .{e.header.index});
            return;
        };
        self.setMembership(&cfg, e.header.index);
        self.membership_term = e.header.term;
        self.recordConfig(e.header.index, e.header.term, &cfg);
    }

    /// A config entry in the log, for the term check: what the owner
    /// replays at boot, and every one appended after.
    pub fn recordConfig(self: *RaftNode, index: u64, term: u64, cfg: *const membership.Config) void {
        // Recorded once, in index order: applying a committed config
        // records it again, and boot replay records earlier ones.
        for (self.config_records[0..self.config_record_count]) |rec| {
            if (rec.index == index) return;
        }
        if (self.config_record_count > 0 and index < self.config_records[self.config_record_count - 1].index) return;
        if (self.config_record_count == self.config_records.len) {
            std.mem.copyForwards(ConfigRecord, self.config_records[0 .. self.config_records.len - 1], self.config_records[1..]);
            self.config_record_count -= 1;
        }
        self.config_records[self.config_record_count] = .{ .index = index, .term = term, .config = cfg.* };
        self.config_record_count += 1;
    }

    /// A truncation cut the config entry the membership came from: the
    /// membership is the newest config the log still holds, or, with none
    /// recorded, the committed one. Falling back to the committed set alone
    /// would leave a node whose owner has not yet applied a committed
    /// config with no members at all, never campaigning.
    fn truncatedBelowMembership(self: *RaftNode, after_index: u64) void {
        while (self.config_record_count > 0 and self.config_records[self.config_record_count - 1].index > after_index) {
            self.config_record_count -= 1;
        }
        if (self.membership_index == 0 or self.membership_index <= after_index) return;
        if (self.config_record_count > 0) {
            const rec = &self.config_records[self.config_record_count - 1];
            self.setMembership(&rec.config, rec.index);
            self.membership_term = rec.term;
            return;
        }
        const committed = self.committed_config;
        self.setMembership(&committed, 0);
        self.membership_term = 0;
    }

    /// Voters: this node when it is one, and every voting peer.
    pub fn clusterSize(self: *const RaftNode) u8 {
        return @as(u8, @intFromBool(self.timer_enabled)) + self.voterPeers();
    }

    /// Quorum size: a majority of the voters.
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
            self.nextStamp(),
            "",
        );
        noop.header.crc32c = noop.computeCrc();
        const idx = try self.log.append(&noop);
        // `last_applied` stays where replay left it; the owner drains what
        // this bootstrap just committed.
        self.commit_index = @max(self.commit_index, self.selfCountedThrough(idx));
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
                if (self.voterPeers() > 0 and now_ms -| self.quorumContactMs() > self.config.election_timeout_max_ms) {
                    log.warn("Raft: no contact with a majority for {d} ms; stepping down from term {d}", .{ now_ms - self.quorumContactMs(), self.current_term });
                    self.role = .follower;
                    self.leader_id = NO_VOTE;
                    self.pre_vote_term = 0;
                    self.rearmElectionTimer();
                    result.step_down = true;
                } else {
                    result.send_heartbeats = true;
                }
            },
            .follower, .candidate => {
                if (self.lost_log != .none) {
                    if (self.hold_pending) {
                        self.hold_pending = false;
                        self.hold_until_ms = now_ms + self.config.election_timeout_max_ms + self.config.rpcTimeoutMs();
                    }
                    if (self.lost_log == .confirming and !self.holding()) {
                        // A confirmation whose hard state could not be
                        // written is retried here.
                        if (self.checkSatisfied()) {
                            _ = self.confirmTerm();
                        } else if (now_ms -| self.check.sent_ms >= self.config.heartbeat_interval_ms) {
                            self.check.sent_ms = now_ms;
                            result.send_term_check = true;
                        }
                    }
                    return result;
                }
                if (!self.mayCampaign()) return result;
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

    /// The tick by which a majority of the voters (this node included when
    /// it is one) had last answered: the quorum-th most recent contact. A
    /// peer that has never answered this leadership counts from when it
    /// began.
    fn quorumContactMs(self: *const RaftNode) u64 {
        var contacts: [MAX_PEERS + 1]u64 = undefined;
        var n: usize = 0;
        if (self.timer_enabled) {
            contacts[0] = self.current_time_ms;
            n = 1;
        }
        for (self.peers[0..self.peer_count]) |p| {
            if (!p.voter) continue;
            contacts[n] = @max(p.last_contact_ms, self.leader_since_ms);
            n += 1;
        }
        std.mem.sort(u64, contacts[0..n], {}, std.sort.desc(u64));
        return contacts[self.quorum() - 1];
    }

    /// The owner's clock, before it hands over messages: a decision that
    /// depends on time (a candidacy's deadline) is not made on the last
    /// tick's clock after a stall.
    pub fn observeTime(self: *RaftNode, now_ms: u64) void {
        self.current_time_ms = @max(self.current_time_ms, now_ms);
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
        if (self.config.enable_pre_vote and self.voterPeers() > 0) {
            self.pre_vote_term = self.current_term + 1;
            self.votes_received = @intFromBool(self.timer_enabled);
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

    /// A poll is still asking about the term after ours.
    fn pollOpen(self: *const RaftNode) bool {
        return self.pre_vote_term != 0 and self.pre_vote_term == self.current_term + 1;
    }

    /// Spend the term: what a passed poll leads to, and what a node with no
    /// peers or no pre-vote does at once.
    pub fn startElectionNow(self: *RaftNode) ?VoteRequest {
        self.pre_vote_term = 0;
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
        // Its own vote counts only where it is a voter.
        self.votes_received = @intFromBool(self.timer_enabled);
        self.votes_needed = self.quorum();
        self.vote_granted_by = std.mem.zeroes([MAX_PEERS]bool);
        self.elections_started += 1;
        self.terms_seen += 1;
        self.rearmElectionTimer();
        // The only voter votes for itself and that is the majority.
        if (self.voterPeers() == 0) self.becomeLeader();
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
        var resp = self.voteRequest(req);
        resp.clock_ns = self.wall_clock.read();
        return resp;
    }

    fn voteRequest(self: *RaftNode, req: VoteRequest) VoteResponse {
        if (!self.termPlausible(req.term)) {
            log.warn("Raft: vote request for term {d} rejected; {d} is more than 2^32 ahead of our term {d}", .{ req.term, req.term - self.current_term, self.current_term });
            return .{ .term = self.current_term, .vote_granted = false, .from = self.id, .is_pre_vote = req.is_pre_vote };
        }
        // What this node voted for before it lost its log or hard state is
        // gone: it grants no vote, real or pre-vote, until the guard
        // completes. A real request's higher term is still adopted, so a
        // stale leader that reaches it is refused sooner.
        if (self.lost_log != .none) {
            if (!req.is_pre_vote and req.term > self.current_term) self.stepDown(req.term);
            return .{ .term = self.current_term, .vote_granted = false, .from = self.id, .is_pre_vote = req.is_pre_vote };
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
            // adopted or persisted by answering. A yes names the term it was
            // asked about, so it counts for that poll and no later one; a no
            // names ours, so a poller behind us catches up.
            const would = req.term >= self.current_term and self.isLogUpToDate(req.last_log_index, req.last_log_term);
            return .{ .term = if (would) req.term else self.current_term, .vote_granted = would, .from = self.id, .is_pre_vote = true };
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
        // Our vote is that candidate's now: a poll of our own still open
        // would, on late answers, stand against the leader we just helped
        // elect.
        self.pre_vote_term = 0;
        self.rearmElectionTimer();
        log.debug("Raft: vote granted to node={d}, term={d}", .{ req.candidate_id, self.current_term });
        return .{ .term = self.current_term, .vote_granted = true, .from = self.id };
    }

    /// Handle an incoming VoteResponse: a pre-vote poll answer while a
    /// poll is open, or a real vote while a candidate.
    pub fn handleVoteResponse(self: *RaftNode, resp: VoteResponse) VoteOutcome {
        if (!self.termPlausible(resp.term)) return .none;
        if (resp.is_pre_vote) {
            if (!self.pollOpen() or self.role == .leader) return .none;
            if (!resp.vote_granted) {
                // The poll asked about our term + 1; a refusal naming a
                // higher term means the cluster has moved on.
                if (resp.term > self.current_term) self.stepDown(resp.term);
                return .none;
            }
            // A yes to an earlier poll says nothing about this one.
            if (resp.term != self.pre_vote_term) return .none;
            const idx = self.voterIndex(resp.from) orelse return .none;
            if (self.vote_granted_by[idx]) return .none;
            self.vote_granted_by[idx] = true;
            self.noteClock(idx, resp.clock_ns);
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
        // Polling again means our candidacy is given up: real votes for it,
        // arriving late, must not join the poll's tally and elect us on a
        // mix of the two.
        if (self.role != .candidate or self.pollOpen()) return .none;
        if (resp.term != self.current_term) return .none;
        // Past its election deadline a candidacy has given up, even before
        // the tick that stands again: a lost-log node's boot wait counts on
        // no candidate winning later than one maximum timeout after it
        // stood.
        if (self.election_deadline_ms != 0 and self.current_time_ms >= self.election_deadline_ms) return .none;
        if (resp.vote_granted) {
            // Count each voter at most once; grants from replicas, unknown
            // nodes or a duplicated response for self never count.
            const idx = self.voterIndex(resp.from) orelse return .none;
            if (self.vote_granted_by[idx]) return .none;
            self.vote_granted_by[idx] = true;
            self.noteClock(idx, resp.clock_ns);
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

    /// Handle an incoming AppendEntries RPC. A guarded node takes and acks
    /// the batch like any follower (catching up needs it, and the contact
    /// keeps the leader in office) but says it is guarded.
    pub fn handleAppendEntries(self: *RaftNode, req: AppendRequest) !AppendResponse {
        var resp = try self.appendEntries(req);
        resp.guarded = self.lost_log != .none;
        resp.leader_commit = req.leader_commit;
        resp.clock_ns = self.wall_clock.read();
        return resp;
    }

    fn appendEntries(self: *RaftNode, req: AppendRequest) !AppendResponse {
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
        // A leader of our term is alive: a poll for the next one is moot.
        self.pre_vote_term = 0;
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

        // Append new entries (truncate conflicts). A config entry becomes
        // the membership only once the batch is on disk: acting on it (a
        // campaign counting its voters) and then crashing without it would
        // leave this node having used a majority it no longer knows.
        var first_new: ?u64 = null;
        var has_config = false;
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
                    self.durable_index = @min(self.durable_index, e.header.index - 1);
                    self.truncatedBelowMembership(e.header.index - 1);
                    _ = try self.log.append(e);
                    if (first_new == null) first_new = e.header.index;
                    has_config = has_config or e.header.entry_type == @intFromEnum(EntryType.raft_config);
                }
                // Same term, same index — already have it, skip
            } else {
                // New entry
                _ = try self.log.append(e);
                if (first_new == null) first_new = e.header.index;
                has_config = has_config or e.header.entry_type == @intFromEnum(EntryType.raft_config);
            }
        }
        if (has_config) {
            const first = first_new.?;
            if (!self.flushLog()) {
                // Not kept: the leader sends the batch again, and this
                // node adopts it then.
                self.log.truncateAfter(first - 1);
                self.durable_index = @min(self.durable_index, first - 1);
                return error.ConfigNotDurable;
            }
            self.markDurable(self.log.lastIndex());
            for (req.entries) |*e| {
                if (e.header.index >= first) self.noteAppended(e);
            }
        }

        // Commit only what this RPC verified: capping by lastIndex() would let
        // an empty heartbeat commit a stale suffix left by a deposed leader.
        // Never move commit backwards — an older heartbeat, delayed or
        // duplicated in flight, verifies less than we may already hold.
        const last_new = req.prev_log_index + req.entries.len;
        self.commit_index = @max(self.commit_index, @min(req.leader_commit, last_new));

        // Caught up: this node holds everything the leader had committed
        // when it sent this batch, so it refuses any candidate missing a
        // committed entry. The leader must have committed in its own term:
        // a new leader's commit index can sit below what earlier leaders
        // committed until then, and catching up to it would leave this
        // node short. Commit, not the leader's last index, so a joiner
        // under steady writes still gets there.
        if (self.lost_log == .catching_up and last_new >= req.leader_commit and
            self.commit_index > 0 and self.log.entryTerm(self.commit_index) == self.current_term)
        {
            self.startTermCheck();
        }

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
                self.noteClock(i, resp.clock_ns);
                self.peers[i].guarded = resp.guarded;
                if (resp.success) {
                    // A late or duplicated ack may report less than we already
                    // know matched; a response can also outlive the log it
                    // answered; a confused peer may claim more than it was
                    // sent. Match only advances, never past our own log, and
                    // never past what this leadership sent it.
                    const ceiling = @min(self.log.lastIndex(), self.peers[i].sent_up_to);
                    const acked = @min(resp.match_index, ceiling);
                    if (resp.guarded) {
                        // Sending moves on; match does not, or a delayed ack
                        // from before the peer lost its disk would flip it
                        // back to unguarded and this one would count. Its
                        // first unguarded ack reports the real match.
                        self.peers[i].next_index = @max(self.peers[i].next_index, acked + 1);
                        self.peers[i].caught_up_streak = 0;
                    } else {
                        self.peers[i].match_index = @max(self.peers[i].match_index, acked);
                        self.peers[i].next_index = self.peers[i].match_index + 1;
                        const p = &self.peers[i];
                        p.caught_up_streak = if (acked >= resp.leader_commit) p.caught_up_streak +| 1 else 0;
                    }
                } else {
                    // Retry from where the follower says its log stops
                    // agreeing, never forward. Below the recorded match it
                    // is a follower that crashed before flushing what it
                    // acked: trust it, or probe above its log forever.
                    // Saturating: the hint is a peer's word, not a bound.
                    const hinted = resp.hint_index +| 1;
                    const back_one = self.peers[i].next_index -| 1;
                    self.peers[i].next_index = @max(1, @min(hinted, back_one));
                    self.peers[i].match_index = @min(self.peers[i].match_index, resp.hint_index);
                    self.peers[i].caught_up_streak = 0;
                }
                break;
            }
        }

        // Advance commit index based on majority match
        self.advanceCommitIndex();
    }

    // ── Propose (Leader) ────────────────────────────────────────────────

    /// Propose an entry (leader only), with `flags` in its header (e.g.
    /// HAS_TTL, TOMBSTONE), stamped here (`nextStamp`): no caller chooses
    /// an entry's time.
    pub fn propose(self: *RaftNode, entry_type: EntryType, flags: u16, payload: []const u8) !ProposeResult {
        // A config is checked against the one before it (`proposeConfig`).
        if (entry_type == .raft_config) return error.ConfigNotChecked;
        if (self.role != .leader) return error.NotLeader;
        return self.appendProposal(entry_type, flags, self.nextStamp(), payload);
    }

    fn appendProposal(self: *RaftNode, entry_type: EntryType, flags: u16, timestamp_ns: u64, payload: []const u8) !ProposeResult {
        if (self.role != .leader) return error.NotLeader;
        if (self.writes_stopped) return error.WritesStopped;
        // A leader far ahead of its followers holds that many clients; past
        // the cap the client is told, and its reads are the backpressure.
        if (self.voterPeers() > 0 and self.log.lastIndex() - self.commit_index >= MAX_OUTSTANDING) return error.Overloaded;

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
        if (entry_type == .raft_config) {
            // On disk before it is the membership, or a crash could bring
            // this leader back counting the majority it had before.
            if (!self.flushLog()) {
                self.log.truncateAfter(idx - 1);
                return error.ConfigNotDurable;
            }
        }
        self.noteAppended(&e);
        // Counted only once it is the membership: a config commits by a
        // majority of the voters it names, never by the ones before it
        // (a lone voter would otherwise commit a second voter's promotion
        // on its own disk).
        if (entry_type == .raft_config) self.markDurable(idx);

        // The lone voter is the majority: its copy commits the entry, at
        // once unless it must be on disk first.
        if (self.voterPeers() == 0) {
            self.commit_index = @max(self.commit_index, self.selfCountedThrough(idx));
        }

        log.debug("Raft: proposed entry, index={d}, term={d}, type={d}, payload_len={d}", .{ idx, self.current_term, @intFromEnum(entry_type), payload.len });
        return .{ .index = idx, .term = self.current_term, .timestamp_ns = timestamp_ns };
    }

    /// What became of a membership change.
    pub const ConfigProposal = union(enum) {
        proposed: ProposeResult,
        /// It may not follow the latest config.
        refused: membership.Refusal,
        /// An earlier change has not committed: two in flight could let two
        /// majorities disagree.
        in_flight,
        /// This leader has not yet committed an entry of its own term. Until
        /// it has, a config from an earlier term may still be in the log
        /// uncommitted, and a change made on top of it can commit beside
        /// one made on the config before it (Ongaro, 2015).
        no_own_commit,
    };

    /// Propose a membership change: the one way a config enters the log
    /// after the first.
    pub fn proposeConfig(self: *RaftNode, next: *const membership.Config) !ConfigProposal {
        if (self.role != .leader) return error.NotLeader;
        // The first config founds the group: there is nothing before it to
        // disagree with, and the founder must be one of its voters.
        if (self.latest_config.member_count == 0) {
            if (!next.isVoter(self.id)) return .{ .refused = .no_voter };
            return .{ .proposed = try self.appendConfig(next) };
        }
        // Leading without a vote is only the time it takes the change that
        // dropped it to commit; it starts none.
        if (!self.timer_enabled) return .{ .refused = .not_a_voter };
        if (self.membership_index > self.commit_index) return .in_flight;
        if (self.commit_index == 0 or self.log.entryTerm(self.commit_index) != self.current_term) return .no_own_commit;
        if (membership.checkChange(&self.latest_config, next, self.id)) |why| return .{ .refused = why };
        return .{ .proposed = try self.appendConfig(next) };
    }

    /// Stamp and append a config. A removal the caller left undated
    /// (`when_ms` 0) is dated by the entry's stamp, so the removal time is
    /// the log's, not the proposer's clock.
    fn appendConfig(self: *RaftNode, next: *const membership.Config) !ProposeResult {
        const stamp = self.nextStamp();
        var dated = next.*;
        for (dated.removed[0..dated.removed_count]) |*rm| {
            if (rm.when_ms == 0) rm.when_ms = stamp / std.time.ns_per_ms;
        }
        var buf: [membership.MAX_SIZE]u8 = undefined;
        return self.appendProposal(.raft_config, entry_mod.Flags.NONE, stamp, membership.encode(&dated, &buf));
    }

    /// The leader's view of each peer that has not caught up, or may yet be
    /// promoted: what `membership.nextAutomatic` decides from.
    pub fn memberProgress(self: *const RaftNode, out: *[MAX_PEERS]membership.Progress) []membership.Progress {
        var n: usize = 0;
        for (self.peers[0..self.peer_count], self.peer_ids[0..self.peer_count]) |p, id| {
            if (p.voter) continue;
            out[n] = .{ .id = id, .caught_up_now = self.caughtUpNow(p), .joining_since_ms = p.joining_since_ms };
            n += 1;
        }
        return out[0..n];
    }

    /// Acked at the commit index on its last `CAUGHT_UP_ACKS` answers, and
    /// heard from lately: a run of acks says nothing about a peer that has
    /// since gone silent.
    pub fn caughtUpNow(self: *const RaftNode, p: PeerState) bool {
        const heard = self.current_time_ms -| p.last_contact_ms <= self.config.election_timeout_max_ms;
        return heard and p.caught_up_streak >= CAUGHT_UP_ACKS;
    }

    /// Hand the log to the owner's disk. True when durable, or when there
    /// is no sink (an ephemeral node has nothing to flush to).
    fn flushLog(self: *RaftNode) bool {
        const sink = self.log_flush_sink orelse return true;
        if (sink.flush(sink.ctx)) return true;
        self.persist_failures += 1;
        return false;
    }

    // ── Lost-log guard ──────────────────────────────────────────────────

    /// This node has no log, or a log with no config and no hard state:
    /// guard it, durably, before it answers anyone.
    pub fn enterLostLog(self: *RaftNode) !void {
        self.lost_log = .catching_up;
        self.hold_pending = true;
        if (!self.persistHardState()) return error.HardStateNotDurable;
    }

    /// Booted with the guard on record from a run that did not finish it:
    /// it starts over, wait included.
    pub fn resumeLostLog(self: *RaftNode) void {
        self.lost_log = .catching_up;
        self.hold_pending = true;
    }

    /// Within the boot wait: no term check is sent and no answer counts.
    fn holding(self: *const RaftNode) bool {
        return self.hold_pending or self.current_time_ms < self.hold_until_ms;
    }

    /// This node kept its log but lost its hard state: the log is real, so
    /// there is nothing to catch up, but the vote it cast in the current
    /// term is gone. It confirms the term, after the boot wait, before it
    /// votes again.
    pub fn enterLostVote(self: *RaftNode) !void {
        self.hold_pending = true;
        self.startTermCheck();
        if (!self.persistHardState()) return error.HardStateNotDurable;
    }

    /// Check the term against the latest committed config and every config
    /// after it the log holds; members report any newer one. Read from the
    /// log's own config entries: the owner applies a committed one only
    /// after the append that brought it returns.
    fn startTermCheck(self: *RaftNode) void {
        self.lost_log = .confirming;
        self.check = .{ .newest_index = self.membership_index, .newest_term = self.membership_term };
        const recs = self.config_records[0..self.config_record_count];
        var committed: ?usize = null;
        for (recs, 0..) |rec, i| {
            if (rec.index <= self.commit_index) committed = i;
        }
        if (committed) |i| {
            self.addCheckSet(voterSet(&recs[i].config).slice());
        } else if (self.committed_config.member_count > 0) {
            self.addCheckSet(voterSet(&self.committed_config).slice());
        }
        for (recs) |*rec| {
            if (rec.index > self.commit_index) self.addCheckSet(voterSet(&rec.config).slice());
        }
        self.addCheckSet(self.ownCheckSet().slice());
        log.info("Raft: guarded node {d} at index {d} (term {d}); confirming the term with {d} member set(s)", .{ self.id, self.log.lastIndex(), self.current_term, self.check.set_count });
        // No other member in any set: no other vote could have counted.
        if (self.checkSatisfied()) _ = self.confirmTerm();
    }

    fn addCheckSet(self: *RaftNode, members: []const NodeId) void {
        if (members.len == 0) return;
        for (self.check.sets[0..self.check.set_count]) |*set| {
            if (set.count == members.len and std.mem.eql(NodeId, set.slice(), members)) return;
        }
        // Full: the newest reported config replaces the last one reported;
        // the committed and own sets are never dropped.
        const slot = if (self.check.set_count < self.check.sets.len) blk: {
            self.check.set_count += 1;
            break :blk self.check.set_count - 1;
        } else self.check.sets.len - 1;
        const n = @min(members.len, self.check.sets[slot].ids.len);
        @memcpy(self.check.sets[slot].ids[0..n], members[0..n]);
        self.check.sets[slot].count = @intCast(n);
        // Ask the new members at the next tick.
        self.check.sent_ms = 0;
    }

    fn answeredBy(self: *const RaftNode, id: NodeId) bool {
        return std.mem.indexOfScalar(NodeId, self.check.answered[0..self.check.answered_count], id) != null;
    }

    fn inCheck(self: *const RaftNode, id: NodeId) bool {
        for (self.check.sets[0..self.check.set_count]) |*set| {
            if (std.mem.indexOfScalar(NodeId, set.slice(), id) != null) return true;
        }
        return false;
    }

    /// Answers a set needs: enough to meet, at a member other than this
    /// node, every quorum of the set its lost vote could have counted in.
    /// A quorum holds at least ⌊n/2⌋ others, so n − ⌊n/2⌋ of the n − 1
    /// others always meet it; alone in the set, nothing could have counted
    /// its vote. Not named, its vote counted in no quorum of the set, and
    /// a plain majority answers.
    fn checkNeeded(self: *const RaftNode, set: *const MemberSet) u8 {
        const n = set.count;
        if (std.mem.indexOfScalar(NodeId, set.slice(), self.id) == null) return n / 2 + 1;
        return if (n == 1) 0 else n - n / 2;
    }

    fn checkGot(self: *const RaftNode, set: *const MemberSet) u8 {
        var got: u8 = 0;
        for (set.slice()) |id| {
            if (id != self.id and self.answeredBy(id)) got += 1;
        }
        return got;
    }

    fn checkSatisfied(self: *const RaftNode) bool {
        if (self.check.set_count == 0) return false;
        for (self.check.sets[0..self.check.set_count]) |*set| {
            if (self.checkGot(set) < self.checkNeeded(set)) return false;
        }
        return true;
    }

    /// Answers in, and how many more the check needs across its sets.
    pub fn checkStatus(self: *const RaftNode) struct { answers: u8, missing: u8 } {
        var missing: u8 = 0;
        for (self.check.sets[0..self.check.set_count]) |*set| missing += self.checkNeeded(set) -| self.checkGot(set);
        return .{ .answers = self.check.answered_count, .missing = missing };
    }

    /// Whom a confirming node asks: the members of every set, other than
    /// itself and those that have answered.
    pub fn termCheckTargets(self: *const RaftNode, out: *[MAX_PEERS + 1]NodeId) []NodeId {
        var n: usize = 0;
        for (self.check.sets[0..self.check.set_count]) |*set| {
            for (set.slice()) |id| {
                if (id == self.id or self.answeredBy(id)) continue;
                if (std.mem.indexOfScalar(NodeId, out[0..n], id) != null) continue;
                if (n == out.len) return out[0..n];
                out[n] = id;
                n += 1;
            }
        }
        return out[0..n];
    }

    pub fn termCheckRequest(self: *const RaftNode) TermCheckRequest {
        return .{ .term = self.current_term, .from = self.id };
    }

    /// Answer a guarded node with our term and latest config, saying
    /// whether we are guarded too. Nothing is adopted: the asker's term
    /// is a leader's it followed, and may be stale.
    pub fn handleTermCheck(self: *const RaftNode, req: TermCheckRequest) TermCheckResponse {
        _ = req;
        var resp: TermCheckResponse = .{
            .term = self.current_term,
            .from = self.id,
            .guarded = self.lost_log != .none,
            .config_index = self.membership_index,
            .config_term = self.membership_term,
            .member_count = 0,
            .members = undefined,
        };
        const voters = self.ownCheckSet();
        @memcpy(resp.members[0..voters.count], voters.slice());
        resp.member_count = voters.count;
        return resp;
    }

    /// The voters of the latest config; with none, the members this node
    /// knows.
    fn ownCheckSet(self: *const RaftNode) MemberSet {
        if (self.latest_config.member_count > 0) return voterSet(&self.latest_config);
        var set: MemberSet = .{};
        set.count = @intCast(self.memberIds(&set.ids).len);
        return set;
    }

    /// One member's answer. A newer term sends the node back to catch up
    /// with that term's leader; a newer config adds its members to the
    /// check; an answer from a non-guarded member counts in every set it
    /// is in. Within the boot wait nothing counts.
    pub fn handleTermCheckResponse(self: *RaftNode, resp: TermCheckResponse) void {
        if (self.lost_log != .confirming or !self.termPlausible(resp.term)) return;
        if (self.holding()) return;
        if (resp.term > self.current_term) {
            self.stepDown(resp.term);
            return;
        }
        const newer = resp.config_term > self.check.newest_term or
            (resp.config_term == self.check.newest_term and resp.config_index > self.check.newest_index);
        if (newer and resp.member_count > 0) {
            self.check.newest_index = resp.config_index;
            self.check.newest_term = resp.config_term;
            self.addCheckSet(resp.members[0..@min(resp.member_count, resp.members.len)]);
            log.info("Raft: guarded node {d} also checks the newer config {any} (index {d}, term {d})", .{ self.id, resp.members[0..@min(resp.member_count, resp.members.len)], resp.config_index, resp.config_term });
        }
        if (resp.guarded or resp.from == self.id or self.answeredBy(resp.from) or !self.inCheck(resp.from)) return;
        if (self.check.answered_count == self.check.answered.len) return;
        self.check.answered[self.check.answered_count] = resp.from;
        self.check.answered_count += 1;
        if (self.checkSatisfied()) _ = self.confirmTerm();
    }

    /// The term is confirmed. A vote recorded for itself in this term means
    /// it grants none in it, here or after a restart. False when that
    /// cannot be made durable; the next tick tries again.
    fn confirmTerm(self: *RaftNode) bool {
        self.lost_log = .none;
        self.voted_for = self.id;
        if (!self.persistHardState()) {
            self.lost_log = .confirming;
            self.voted_for = NO_VOTE;
            return false;
        }
        self.rearmElectionTimer();
        log.info("Raft: guarded node {d} confirmed term {d} with {d} answer(s); it votes from term {d} on", .{ self.id, self.current_term, self.check.answered_count, self.current_term + 1 });
        return true;
    }

    // ── Internal ────────────────────────────────────────────────────────

    fn stepDown(self: *RaftNode, new_term: u64) void {
        log.debug("Raft: stepping down, node_id={d}, old_term={d}, new_term={d}", .{ self.id, self.current_term, new_term });
        self.current_term = new_term;
        self.role = .follower;
        self.voted_for = NO_VOTE;
        // A term newer than the one being confirmed has a leader to catch
        // up with first.
        if (self.lost_log == .confirming) self.lost_log = .catching_up;
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
        if (sink.persist(sink.ctx, self.current_term, self.voted_for, self.lost_log != .none)) return true;
        self.persist_failures += 1;
        log.debug("Raft: hard state not durable, node_id={d}, group_id={d}, term={d}, voted_for={d} (persist_failures={d})", .{ self.id, self.group_id, self.current_term, self.voted_for, self.persist_failures });
        return false;
    }

    fn becomeLeader(self: *RaftNode) void {
        log.debug("Raft: becoming leader, node_id={d}, term={d}", .{ self.id, self.current_term });
        self.role = .leader;
        self.leader_id = self.id;
        self.pre_vote_term = 0;
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
            self.peers[i].caught_up_streak = 0;
            self.peers[i].joining_since_ms = self.current_time_ms;
        }
        // An entry of this term, so the entries of earlier terms commit as
        // soon as it replicates: a leader may only count a majority for its
        // own term's entries, and without this one it would wait for a
        // client to write. Alone, the majority is this node and the whole
        // log commits now.
        var noop = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, self.current_term, next, self.nextStamp(), "");
        noop.header.crc32c = noop.computeCrc();
        if (self.log.append(&noop)) |idx| {
            if (self.voterPeers() == 0) self.commit_index = @max(self.commit_index, self.selfCountedThrough(idx));
        } else |err| {
            log.err("Raft: cannot append the leadership noop at index {d}: {s}; earlier terms' entries commit only after the next client write", .{ next, @errorName(err) });
        }
    }

    pub fn peerIndex(self: *const RaftNode, peer_id: NodeId) ?usize {
        for (0..self.peer_count) |i| {
            if (self.peer_ids[i] == peer_id) return i;
        }
        return null;
    }

    /// What this leader tracks for a peer, if it is one.
    pub fn peerProgress(self: *const RaftNode, peer_id: NodeId) ?PeerState {
        const i = self.peerIndex(peer_id) orelse return null;
        return self.peers[i];
    }

    /// A peer the latest config names as a replica.
    pub fn isReplicaPeer(self: *const RaftNode, peer_id: NodeId) bool {
        const i = self.peerIndex(peer_id) orelse return false;
        return !self.peers[i].voter;
    }

    fn voterIndex(self: *const RaftNode, peer_id: NodeId) ?usize {
        const i = self.peerIndex(peer_id) orelse return null;
        return if (self.peers[i].voter) i else null;
    }

    fn isLogUpToDate(self: *const RaftNode, last_index: u64, last_term: u64) bool {
        const my_term = self.log.lastTerm();
        if (last_term != my_term) return last_term > my_term;
        return last_index >= self.log.lastIndex();
    }

    /// How far up to `idx` this node's own copy counts: all of it, unless
    /// copies count only once on disk.
    fn selfCountedThrough(self: *const RaftNode, idx: u64) u64 {
        return if (self.config.self_counts_when_durable) @min(idx, self.durable_index) else idx;
    }

    // ── Time ────────────────────────────────────────────────────────────

    fn noteClock(self: *RaftNode, peer: usize, clock_ns: u64) void {
        if (clock_ns == 0) return;
        self.peers[peer].clock_ns = clock_ns;
        self.peers[peer].clock_at_ms = self.current_time_ms;
    }

    /// The voters' clock as a majority holds it: the quorum-th highest of
    /// this node's clock (when it votes) and each voter's last report, aged
    /// by this node's monotonic time since it arrived. The commit rule's
    /// shape: one runaway clock cannot set it. A voter that has not
    /// reported counts as 0, which holds time back rather than forward.
    fn quorumClock(self: *const RaftNode, own: u64) struct { ns: u64, by: NodeId } {
        var clocks: [MAX_PEERS + 1]u64 = undefined;
        var ids: [MAX_PEERS + 1]NodeId = undefined;
        var n: usize = 0;
        if (self.timer_enabled) {
            clocks[0] = own;
            ids[0] = self.id;
            n = 1;
        }
        for (self.peers[0..self.peer_count], self.peer_ids[0..self.peer_count]) |p, id| {
            if (!p.voter) continue;
            clocks[n] = if (p.clock_ns == 0) 0 else p.clock_ns +| (self.current_time_ms -| p.clock_at_ms) *| std.time.ns_per_ms;
            ids[n] = id;
            n += 1;
        }
        // Sort descending, carrying ids.
        var i: usize = 1;
        while (i < n) : (i += 1) {
            var j = i;
            while (j > 0 and clocks[j] > clocks[j - 1]) : (j -= 1) {
                std.mem.swap(u64, &clocks[j], &clocks[j - 1]);
                std.mem.swap(NodeId, &ids[j], &ids[j - 1]);
            }
        }
        const q = self.quorum() - 1;
        return .{ .ns = clocks[q], .by = ids[q] };
    }

    /// The stamp for the next entry this leader appends: after the last
    /// one, and at its own clock, but never more than `STAMP_LEAD_NS` past
    /// the voters' clock, so one fast clock cannot carry the log's time
    /// ahead. Alone, its clock is trusted, as any single-node store does.
    /// Where the formula holds time still, stamps advance 1 ns an entry.
    pub fn nextStamp(self: *RaftNode) u64 {
        const prev = self.log.last_stamp;
        const clock = self.wall_clock.read();
        var allowed = clock;
        if (self.voterPeers() > 0) {
            const q = self.quorumClock(clock);
            allowed = @min(clock, q.ns +| STAMP_LEAD_NS);
            self.stamp_held_by = q.by;
        } else {
            self.stamp_held_by = self.id;
        }
        const stamp = @max(prev +| 1, allowed);
        self.stamp_clock_behind = stamp > clock;
        self.stamp_skew_ns = if (self.stamp_clock_behind) stamp - clock else clock - stamp;
        return stamp;
    }

    /// What time it is for a read on this node, so a read and the next
    /// conditional write agree on what has expired: on a leader, what it
    /// would stamp now; on a follower, the last applied stamp moved on by
    /// this node's monotonic time, never past its own clock.
    pub fn now(self: *const RaftNode) u64 {
        const clock = self.wall_clock.read();
        if (self.role == .leader) {
            if (self.voterPeers() == 0) return @max(self.log.last_stamp, clock);
            return @max(self.log.last_stamp, @min(clock, self.quorumClock(clock).ns +| STAMP_LEAD_NS));
        }
        if (self.applied_stamp == 0) return clock;
        const moved = self.applied_stamp +| (self.current_time_ms -| self.applied_at_ms) *| std.time.ns_per_ms;
        return @max(self.applied_stamp, @min(clock, moved));
    }

    /// `now` behind an opaque pointer, for a projection's read clock.
    pub fn nowOpaque(ctx: ?*const anyopaque) u64 {
        const self: *const RaftNode = @ptrCast(@alignCast(ctx.?));
        return self.now();
    }

    /// The owner applied an entry stamped `stamp`.
    pub fn noteApplied(self: *RaftNode, stamp: u64) void {
        self.applied_stamp = @max(self.applied_stamp, stamp);
        self.applied_at_ms = self.current_time_ms;
    }

    /// The owner has everything through `idx` on disk. A leader's own copy
    /// now counts toward commit up to there.
    pub fn markDurable(self: *RaftNode, idx: u64) void {
        const through = @min(idx, self.log.lastIndex());
        if (through <= self.durable_index) return;
        self.durable_index = through;
        if (self.role != .leader) return;
        if (self.voterPeers() == 0) {
            // A lone voter's disk is the whole quorum, whatever term the
            // entries are from.
            self.commit_index = @max(self.commit_index, self.durable_index);
        } else {
            self.advanceCommitIndex();
        }
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

            var replicas: u8 = if (self.timer_enabled and self.selfCountedThrough(idx) >= idx) 1 else 0;
            for (0..self.peer_count) |i| {
                const p = self.peers[i];
                if (p.voter and p.match_index >= idx and !p.guarded) {
                    replicas += 1;
                }
            }
            if (replicas >= self.quorum()) {
                new_commit = idx;
                break;
            }
        }

        self.commit_index = new_commit;
        // A leader whose own removal (or demotion) just committed leaves
        // office now, not when its owner applies the entry: until then every
        // gate would be open to it, and anything it proposed would be counted
        // by voters that have moved on.
        if (self.role == .leader and !self.timer_enabled and self.membership_index <= self.commit_index) {
            log.info("Raft: node {d} is no longer a voter and the change committed; stepping down from term {d}", .{ self.id, self.current_term });
            self.role = .follower;
            self.leader_id = NO_VOTE;
            self.pre_vote_term = 0;
            self.left_group = true;
        }
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
    const r1 = try node.propose(.kv_put, 0, "key1val1");
    try testing.expectEqual(@as(u64, 2), r1.index); // 1 is noop
    try testing.expectEqual(@as(u64, 1), r1.term);
    try testing.expectEqual(@as(u64, 2), node.commit_index);

    const r2 = try node.propose(.kv_put, 0, "key2val2");
    try testing.expectEqual(@as(u64, 3), r2.index);
    try testing.expectEqual(@as(u64, 3), node.commit_index);
}

test "raft node: propose rejected when not leader" {
    const allocator = testing.allocator;

    var node = try RaftNode.init(allocator, 1, 1000, 4096, .{});
    defer node.deinit();

    const result = node.propose(.kv_put, 0, "data");
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
    _ = try node.propose(.kv_put, 0, "key1val1");
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
    _ = try node.propose(.kv_put, 0, "t1");

    // Deposed (a term-2 candidate appears), then re-elected in term 3: the
    // win's noop goes at 3 and a term-3 entry at 4.
    _ = node.handleVoteRequest(.{ .term = 2, .candidate_id = 3, .last_log_index = 1, .last_log_term = 1 });
    try testing.expectEqual(Role.follower, node.role);
    _ = candidacy(&node).?;
    _ = node.handleVoteResponse(.{ .term = 3, .vote_granted = true, .from = 2 });
    try testing.expectEqual(Role.leader, node.role);
    try testing.expectEqual(@as(u64, 3), node.current_term);
    _ = try node.propose(.kv_put, 0, "t3");
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
    for (0..3) |_| _ = try node.propose(.kv_put, 0, "e");
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
    lost_log: bool = false,
    calls: u32 = 0,
    fail: bool = false,

    fn persist(ctx: *anyopaque, term: u64, voted_for: NodeId, lost_log: bool) bool {
        const self: *SinkRecorder = @ptrCast(@alignCast(ctx));
        self.lost_log = lost_log;
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
    const r = try node.propose(.kv_put, 0, "next");
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

test "raft node: a node that votes for another candidate drops its own poll, so late answers to it cannot depose the winner" {
    var node = try RaftNode.init(testing.allocator, 3, 1, 4096, .{});
    defer node.deinit();
    node.addPeer(1);
    node.addPeer(2);

    // Two nodes time out together: we poll for term 1, and node 2 stands
    // for term 1 first. We vote for it.
    const poll = node.startElection().?;
    try testing.expect(poll.is_pre_vote);
    const vote = node.handleVoteRequest(.{ .term = 1, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(vote.vote_granted);
    try testing.expectEqual(@as(u64, 1), node.current_term);

    // Node 2's answer to our poll, sent before it stood, arrives now. It
    // must not start an election for term 2 against the leader of term 1.
    try testing.expect(node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2, .is_pre_vote = true }) == .none);
    try testing.expectEqual(@as(u64, 1), node.current_term);
    try testing.expectEqual(Role.follower, node.role);
    try testing.expectEqual(@as(u32, 2), node.voted_for);
}

test "raft node: a vote for a candidate in a term we already knew also drops our poll" {
    var node = try RaftNode.init(testing.allocator, 3, 1, 4096, .{});
    defer node.deinit();
    node.addPeer(1);
    node.addPeer(2);
    // We know term 1 but have voted in it for nobody; we poll for term 2.
    node.current_term = 1;
    _ = node.startElection().?;
    // Node 2 stands in term 1, the term we are in: no step-down, just a vote.
    const vote = node.handleVoteRequest(.{ .term = 1, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(vote.vote_granted);
    try testing.expect(node.handleVoteResponse(.{ .term = 2, .vote_granted = true, .from = 1, .is_pre_vote = true }) == .none);
    try testing.expectEqual(@as(u64, 1), node.current_term);
    try testing.expectEqual(Role.follower, node.role);
}

test "raft node: a poll's answers never count as votes, nor votes as poll answers" {
    var node = try RaftNode.init(testing.allocator, 3, 1, 4096, .{});
    defer node.deinit();
    node.addPeer(1);
    node.addPeer(2);
    // Polling: a granted real vote is not a "would vote". (One naming our
    // own term; a newer term would rightly move us to it.)
    _ = node.startElection().?;
    try testing.expect(node.handleVoteResponse(.{ .term = 0, .vote_granted = true, .from = 1 }) == .none);
    try testing.expectEqual(@as(u64, 0), node.current_term);
    try testing.expectEqual(Role.follower, node.role);
    // A candidate for term 1: a late "would vote", from a node that has
    // reached term 1 meanwhile, is not a vote.
    _ = node.startElectionNow().?;
    try testing.expectEqual(Role.candidate, node.role);
    try testing.expect(node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 1, .is_pre_vote = true }) == .none);
    try testing.expectEqual(Role.candidate, node.role);
    // A real vote still wins it.
    try testing.expect(node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2 }) == .won);
}

test "raft node: a poll is dropped when a leader of our term speaks" {
    var node = try RaftNode.init(testing.allocator, 3, 1, 4096, .{});
    defer node.deinit();
    node.addPeer(1);
    node.addPeer(2);
    node.current_term = 1;
    _ = node.startElection().?;
    // Node 1 leads term 1 and is heard: our poll for term 2 is moot.
    _ = try node.handleAppendEntries(.{ .term = 1, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{}, .leader_commit = 0 });
    try testing.expect(node.handleVoteResponse(.{ .term = 2, .vote_granted = true, .from = 2, .is_pre_vote = true }) == .none);
    try testing.expectEqual(@as(u64, 1), node.current_term);
    try testing.expectEqual(Role.follower, node.role);
}

test "raft node: a candidate that polls again counts no late votes for its old term, so no term gets two leaders" {
    var node = try RaftNode.init(testing.allocator, 1, 1, 4096, .{});
    defer node.deinit();
    for ([_]NodeId{ 2, 3, 4, 5 }) |p| node.addPeer(p);
    // A candidate for term 1 with only its own vote; its timer fires and it
    // polls for term 2. Node 3 would vote.
    _ = node.startElectionNow().?;
    _ = node.startElection().?;
    try testing.expect(node.handleVoteResponse(.{ .term = 2, .vote_granted = true, .from = 3, .is_pre_vote = true }) == .none);
    // Node 2's vote for term 1, delayed, arrives. Counted with the poll it
    // would make three of five, and node 1 would lead term 1 on two real
    // votes while another node wins term 1 properly.
    try testing.expect(node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2 }) == .none);
    try testing.expect(node.role != .leader);
    try testing.expectEqual(@as(u64, 1), node.current_term);
}

test "raft node: a yes to an earlier poll does not count toward a later one" {
    var node = try RaftNode.init(testing.allocator, 1, 1, 4096, .{});
    defer node.deinit();
    node.addPeer(2);
    node.addPeer(3);
    // We poll for term 1; node 3's yes is delayed. Node 2 wins term 1 and
    // is heard, then goes quiet, and we poll for term 2.
    _ = node.startElection().?;
    _ = try node.handleAppendEntries(.{ .term = 1, .leader_id = 2, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{}, .leader_commit = 0 });
    _ = node.startElection().?;
    try testing.expectEqual(@as(u64, 2), node.pre_vote_term);
    // Node 3's yes to the first poll arrives now.
    try testing.expect(node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 3, .is_pre_vote = true }) == .none);
    try testing.expectEqual(@as(u64, 1), node.current_term);
    try testing.expectEqual(Role.follower, node.role);
    // A no from a node ahead of us still brings us to its term.
    try testing.expect(node.handleVoteResponse(.{ .term = 7, .vote_granted = false, .from = 3, .is_pre_vote = true }) == .none);
    try testing.expectEqual(@as(u64, 7), node.current_term);
}

test "raft node: a poll that passes but whose term cannot be made durable closes, so its answers do not elect us later" {
    var rec = SinkRecorder{ .fail = true };
    var node = try RaftNode.init(testing.allocator, 4, 1, 4096, .{});
    defer node.deinit();
    node.hard_state_sink = rec.sink();
    for ([_]NodeId{ 1, 2, 3 }) |p| node.addPeer(p);
    _ = node.startElection().?;
    _ = node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 1, .is_pre_vote = true });
    // A majority would vote, but the term cannot be persisted: it rolls back.
    try testing.expect(node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2, .is_pre_vote = true }) == .none);
    try testing.expectEqual(@as(u64, 0), node.current_term);
    // The disk is back; a late yes to that poll does not start an election.
    rec.fail = false;
    try testing.expect(node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 3, .is_pre_vote = true }) == .none);
    try testing.expectEqual(@as(u64, 0), node.current_term);
}

test "raft node: a yes to a poll names the term asked about, a no names ours, and every answer says it is a poll's" {
    var node = try RaftNode.init(testing.allocator, 2, 1, 4096, .{});
    defer node.deinit();
    node.addPeer(1);
    node.current_term = 3;
    const yes = node.handleVoteRequest(.{ .term = 4, .candidate_id = 1, .last_log_index = 0, .last_log_term = 0, .is_pre_vote = true });
    try testing.expect(yes.vote_granted and yes.is_pre_vote);
    try testing.expectEqual(@as(u64, 4), yes.term);
    const no = node.handleVoteRequest(.{ .term = 2, .candidate_id = 1, .last_log_index = 0, .last_log_term = 0, .is_pre_vote = true });
    try testing.expect(!no.vote_granted and no.is_pre_vote);
    try testing.expectEqual(@as(u64, 3), no.term);
    // A term too far ahead to be real is refused, still as a poll's answer.
    const wild = node.handleVoteRequest(.{ .term = 3 + (1 << 33), .candidate_id = 1, .last_log_index = 0, .last_log_term = 0, .is_pre_vote = true });
    try testing.expect(!wild.vote_granted and wild.is_pre_vote);
}

test "raft node: a poll open when a newer term arrives is dropped" {
    var node = try RaftNode.init(testing.allocator, 3, 1, 4096, .{});
    defer node.deinit();
    node.addPeer(1);
    node.addPeer(2);

    _ = node.startElection().?;
    // A newer term, here from a vote we refuse (node 2's log is behind
    // ours), moves us on without granting anything.
    var entry = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 0, "");
    entry.header.crc32c = entry.computeCrc();
    _ = try node.log.append(&entry);
    const vote = node.handleVoteRequest(.{ .term = 5, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 });
    try testing.expect(!vote.vote_granted);
    try testing.expectEqual(@as(u64, 5), node.current_term);
    // The poll asked about term 1; a "yes" to it says nothing about term 6.
    try testing.expect(node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 1, .is_pre_vote = true }) == .none);
    try testing.expectEqual(@as(u64, 5), node.current_term);
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
    const outcome = node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2, .is_pre_vote = true });
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

    // A hint at the top of the range is taken as "no earlier", not overflowed.
    leader.peers[0].next_index = 9;
    leader.handleAppendResponse(.{ .term = 1, .success = false, .match_index = 0, .from = 2, .hint_index = std.math.maxInt(u64) });
    try testing.expectEqual(@as(u64, 8), leader.peers[0].next_index);
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
    const three = membership.encode(&membership.Config.ofVoters(&.{ 1, 2, 3 }), &buf);
    const r = (try leader.proposeConfig(&membership.Config.ofVoters(&.{ 1, 2, 3 }))).proposed;
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
    node.setVoters(&.{ 1, 2, 3 }, 7);
    try testing.expectEqual(@as(u8, 3), node.peer_count);
    try testing.expect(!node.timer_enabled);
    _ = node.tick(1000);
    _ = node.tick(1000 + node.config.election_timeout_max_ms * 3);
    try testing.expect(!node.tick(1000 + node.config.election_timeout_max_ms * 4).start_election);

    // Named now: peers are the others, progress for kept peers survives.
    node.setVoters(&.{ 1, 2, 3 }, 7);
    node.peers[0].match_index = 9;
    node.setVoters(&.{ 1, 2, 3, 5 }, 8);
    try testing.expect(node.timer_enabled);
    try testing.expectEqual(@as(u8, 3), node.peer_count);
    try testing.expectEqual(@as(u64, 9), node.peers[0].match_index);
    try testing.expectEqual(@as(u8, 3), node.quorum());
    var ids: [MAX_PEERS + 1]NodeId = undefined;
    try testing.expectEqual(@as(usize, 4), node.memberIds(&ids).len);

    // A truncation below the entry that named this node reverts to the
    // committed membership, which did not.
    node.commitMembership(&membership.Config.ofVoters(&.{ 1, 2, 3 }));
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
    node.setVoters(&.{}, 0);
    try testing.expect(!node.timer_enabled);
    node.setVoters(&.{1}, 5);
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
    leader.setVoters(&.{ 1, 2 }, 2);
    const r = leader.tick(joined_at + max);
    try testing.expect(!r.step_down);
    try testing.expectEqual(Role.leader, leader.role);
    // ...but a member that never answers still costs it the lead.
    const later = leader.tick(joined_at + 2 * max + 1);
    try testing.expect(later.step_down);
}

test "raft node: counting its own copy only once durable, a lone leader commits what is on disk" {
    var node = try RaftNode.init(testing.allocator, 1, 1, 16384, .{ .self_counts_when_durable = true });
    defer node.deinit();
    try node.bootstrap();
    try testing.expectEqual(@as(u64, 0), node.commit_index);
    node.markDurable(node.log.lastIndex());
    try testing.expectEqual(@as(u64, 1), node.commit_index);

    const p = try node.propose(.kv_put, entry_mod.Flags.NONE, "v");
    try testing.expectEqual(@as(u64, 1), node.commit_index);
    // Nothing past the log counts, however far the owner says it flushed.
    node.markDurable(p.index + 5);
    try testing.expectEqual(p.index, node.commit_index);
    try testing.expectEqual(p.index, node.durable_index);
}

test "raft node: a leader's own copy counts toward a majority only once durable" {
    var leader = try RaftNode.init(testing.allocator, 1, 1, 16384, .{ .self_counts_when_durable = true });
    defer leader.deinit();
    leader.addPeer(2);
    leader.addPeer(3);
    _ = candidacy(&leader).?;
    _ = leader.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 3 });
    try testing.expectEqual(Role.leader, leader.role);
    const p = try leader.propose(.kv_put, entry_mod.Flags.NONE, "v");
    sentAll(&leader);

    // One follower has it; the leader's copy isn't on disk: one of three.
    leader.handleAppendResponse(.{ .term = 1, .success = true, .match_index = p.index, .from = 2 });
    try testing.expectEqual(@as(u64, 0), leader.commit_index);
    leader.markDurable(p.index);
    try testing.expectEqual(p.index, leader.commit_index);
}

test "raft node: a durable index past a truncation is cut back to it" {
    var follower = try RaftNode.init(testing.allocator, 2, 1, 16384, .{ .self_counts_when_durable = true, .durable_commits = false });
    defer follower.deinit();
    for (1..4) |i| {
        var e = testEntry(1, i, "f");
        _ = try follower.log.append(&e);
    }
    follower.markDurable(3);
    try testing.expectEqual(@as(u64, 3), follower.durable_index);
    follower.current_term = 2;
    const replacement = [_]Entry{testEntry(2, 2, "l")};
    const resp = try follower.handleAppendEntries(.{ .term = 2, .leader_id = 1, .prev_log_index = 1, .prev_log_term = 1, .entries = &replacement, .leader_commit = 0 });
    try testing.expect(resp.success);
    try testing.expectEqual(@as(u64, 1), follower.durable_index);
}

/// A guarded node 2 that leader 1 (term 3) has caught up: entries 1..3
/// of term 3, the config {1,2,3} at index 2, all committed. Past the boot
/// wait.
fn guardedCaughtUp(node: *RaftNode, rec: *SinkRecorder) !void {
    node.hard_state_sink = rec.sink();
    node.timer_enabled = false;
    try node.enterLostLog();
    var cfg_buf: [membership.MAX_SIZE]u8 = undefined;
    var es = [_]Entry{
        entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 3, 1, 0, ""),
        entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, 3, 2, 0, membership.encode(&membership.Config.ofVoters(&.{ 1, 2, 3 }), &cfg_buf)),
        entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 3, 3, 0, ""),
    };
    // Short of what the leader had committed: still catching up, and its
    // acks say it is guarded.
    const r1 = try node.handleAppendEntries(.{ .term = 3, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = es[0..2], .leader_commit = 3 });
    try testing.expectEqual(LostLog.catching_up, node.lost_log);
    try testing.expect(r1.success and r1.guarded);
    _ = try node.handleAppendEntries(.{ .term = 3, .leader_id = 1, .prev_log_index = 2, .prev_log_term = 3, .entries = es[2..3], .leader_commit = 3 });
    try testing.expectEqual(LostLog.confirming, node.lost_log);
    passBootWait(node);
}

/// Past the boot wait: the first tick arms it, a later one ends it.
fn passBootWait(node: *RaftNode) void {
    const start = node.current_time_ms + 1;
    _ = node.tick(start);
    _ = node.tick(start + node.config.election_timeout_max_ms + node.config.rpcTimeoutMs());
}

fn checkAnswer(from: NodeId, term: u64, config_index: u64, config_term: u64, members: []const NodeId) TermCheckResponse {
    var r: TermCheckResponse = .{ .term = term, .from = from, .config_index = config_index, .config_term = config_term, .member_count = @intCast(members.len), .members = undefined };
    @memcpy(r.members[0..members.len], members);
    return r;
}

test "raft node: a guarded node grants no vote and never campaigns until caught up and the term is confirmed" {
    var rec = SinkRecorder{};
    var node = try RaftNode.init(testing.allocator, 2, 0, 4096, .{ .rng_seed = 9 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();
    try node.enterLostLog();
    try testing.expect(rec.lost_log);
    try testing.expect(!node.handleVoteRequest(.{ .term = 1, .candidate_id = 1, .last_log_index = 0, .last_log_term = 0 }).vote_granted);
    try testing.expect(!node.handleVoteRequest(.{ .term = 1, .candidate_id = 1, .last_log_index = 0, .last_log_term = 0, .is_pre_vote = true }).vote_granted);

    try guardedCaughtUp(&node, &rec);
    // Named by the config, yet the timer elects nothing; it checks the term.
    const t = node.tick(node.current_time_ms + 1_000_000);
    try testing.expect(!t.start_election);
    try testing.expect(t.send_term_check);
    var targets: [MAX_PEERS + 1]NodeId = undefined;
    try testing.expectEqualSlices(NodeId, &.{ 1, 3 }, node.termCheckTargets(&targets));

    // Of three, both others must answer; node 3 answering while guarded
    // itself is no evidence of the votes this node cast.
    node.handleTermCheckResponse(checkAnswer(1, 3, 2, 3, &.{ 1, 2, 3 }));
    node.handleTermCheckResponse(checkAnswer(1, 3, 2, 3, &.{ 1, 2, 3 }));
    var guarded = checkAnswer(3, 3, 2, 3, &.{ 1, 2, 3 });
    guarded.guarded = true;
    node.handleTermCheckResponse(guarded);
    try testing.expectEqual(LostLog.confirming, node.lost_log);
    try testing.expect(rec.lost_log);
    node.handleTermCheckResponse(checkAnswer(3, 2, 2, 3, &.{ 1, 2, 3 }));
    try testing.expectEqual(LostLog.none, node.lost_log);
    try testing.expect(!rec.lost_log);
    try testing.expectEqual(@as(u64, 3), rec.term);
    try testing.expectEqual(@as(NodeId, 2), rec.voted_for);
    const after = try node.handleAppendEntries(.{ .term = 3, .leader_id = 1, .prev_log_index = 3, .prev_log_term = 3, .entries = &.{}, .leader_commit = 3 });
    try testing.expect(!after.guarded);

    // Whatever it voted for in term 3 is gone, so it votes in none; it
    // votes in term 4.
    node.current_time_ms += 10_000_000;
    try testing.expect(!node.handleVoteRequest(.{ .term = 3, .candidate_id = 3, .last_log_index = 3, .last_log_term = 3 }).vote_granted);
    try testing.expect(node.handleVoteRequest(.{ .term = 4, .candidate_id = 3, .last_log_index = 3, .last_log_term = 3 }).vote_granted);
}

test "raft node: a newer config a member reports is checked as well, and a newer term sends the guarded node back to catch up" {
    var rec = SinkRecorder{};
    var node = try RaftNode.init(testing.allocator, 2, 0, 4096, .{ .rng_seed = 9 });
    defer node.deinit();
    try guardedCaughtUp(&node, &rec);

    // A config of five from a later term: three of its four others must
    // answer, on top of both others of the three.
    node.handleTermCheckResponse(checkAnswer(1, 3, 7, 4, &.{ 1, 2, 3, 4, 5 }));
    var targets: [MAX_PEERS + 1]NodeId = undefined;
    try testing.expectEqualSlices(NodeId, &.{ 3, 4, 5 }, node.termCheckTargets(&targets));
    // Nodes 1, 4 and 5 meet the five; the three it already had still need
    // node 3, so the newer set was added, not swapped in.
    node.handleTermCheckResponse(checkAnswer(4, 3, 7, 4, &.{ 1, 2, 3, 4, 5 }));
    node.handleTermCheckResponse(checkAnswer(5, 3, 7, 4, &.{ 1, 2, 3, 4, 5 }));
    try testing.expectEqual(LostLog.confirming, node.lost_log);
    node.handleTermCheckResponse(checkAnswer(3, 6, 7, 4, &.{ 1, 2, 3, 4, 5 }));
    // A term past ours: a leader this node has not caught up with.
    try testing.expectEqual(LostLog.catching_up, node.lost_log);
    try testing.expectEqual(@as(u64, 6), node.current_term);
    try testing.expect(rec.lost_log);
}

test "raft node: the latest committed config is checked as well as an uncommitted one after it" {
    var rec = SinkRecorder{};
    var node = try RaftNode.init(testing.allocator, 2, 0, 4096, .{ .rng_seed = 9 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();
    // {1,2,3} committed; {1,2,3,4} appended by a leader the group may
    // have replaced.
    node.commitMembership(&membership.Config.ofVoters(&.{ 1, 2, 3 }));
    node.setVoters(&.{ 1, 2, 3, 4 }, 9);
    node.membership_term = 3;
    node.current_term = 3;
    try node.enterLostVote();
    passBootWait(&node);
    // Two of {1,3,4} meet the four, not the three: node 3 must answer.
    node.handleTermCheckResponse(checkAnswer(1, 3, 9, 3, &.{ 1, 2, 3, 4 }));
    node.handleTermCheckResponse(checkAnswer(4, 3, 9, 3, &.{ 1, 2, 3, 4 }));
    try testing.expectEqual(LostLog.confirming, node.lost_log);
    node.handleTermCheckResponse(checkAnswer(3, 3, 9, 3, &.{ 1, 2, 3, 4 }));
    try testing.expectEqual(LostLog.none, node.lost_log);
}

test "raft node: a guarded node in a group of two confirms with the other member's answer" {
    var rec = SinkRecorder{};
    var node = try RaftNode.init(testing.allocator, 2, 0, 4096, .{ .rng_seed = 9 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();
    node.timer_enabled = false;
    try node.enterLostLog();
    var cfg_buf: [membership.MAX_SIZE]u8 = undefined;
    var es = [_]Entry{
        entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 0, ""),
        entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, 1, 2, 0, membership.encode(&membership.Config.ofVoters(&.{ 1, 2 }), &cfg_buf)),
    };
    _ = try node.handleAppendEntries(.{ .term = 1, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &es, .leader_commit = 1 });
    try testing.expectEqual(LostLog.confirming, node.lost_log);
    passBootWait(&node);
    // Any quorum that counted this node's vote also held node 1.
    node.handleTermCheckResponse(checkAnswer(1, 1, 2, 1, &.{ 1, 2 }));
    try testing.expectEqual(LostLog.none, node.lost_log);
}

test "raft node: a guarded node sends no term check and counts no answer within the boot wait" {
    var rec = SinkRecorder{};
    var node = try RaftNode.init(testing.allocator, 2, 0, 4096, .{ .rng_seed = 9, .election_timeout_max_ms = 300, .heartbeat_interval_ms = 50 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();
    node.setVoters(&.{ 1, 2 }, 1);
    try node.enterLostVote();
    // Before the first tick there is no clock: it waits. The wait is one
    // maximum election timeout plus an RPC timeout: 300 + 100.
    node.handleTermCheckResponse(checkAnswer(1, 0, 1, 0, &.{ 1, 2 }));
    try testing.expectEqual(LostLog.confirming, node.lost_log);
    try testing.expect(!node.tick(1000).send_term_check);
    try testing.expect(!node.tick(1399).send_term_check);
    node.handleTermCheckResponse(checkAnswer(1, 0, 1, 0, &.{ 1, 2 }));
    try testing.expectEqual(LostLog.confirming, node.lost_log);
    try testing.expect(node.tick(1400).send_term_check);
    node.handleTermCheckResponse(checkAnswer(1, 0, 1, 0, &.{ 1, 2 }));
    try testing.expectEqual(LostLog.none, node.lost_log);
}

test "raft node: a node that kept its log but lost its hard state confirms the term before it votes again" {
    var rec = SinkRecorder{};
    var node = try RaftNode.init(testing.allocator, 2, 0, 4096, .{ .rng_seed = 9 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();
    var cfg_buf: [membership.MAX_SIZE]u8 = undefined;
    var es = [_]Entry{
        entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 3, 1, 0, ""),
        entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, 3, 2, 0, membership.encode(&membership.Config.ofVoters(&.{ 1, 2, 3 }), &cfg_buf)),
    };
    _ = try node.handleAppendEntries(.{ .term = 3, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &es, .leader_commit = 2 });
    // Restarted without HARDSTATE: no vote on record (the owner boots at
    // the log's last term).
    node.voted_for = NO_VOTE;
    node.leader_id = NO_VOTE;
    try node.enterLostVote();
    try testing.expectEqual(LostLog.confirming, node.lost_log);
    try testing.expect(rec.lost_log);
    passBootWait(&node);
    node.current_time_ms += 10_000_000;
    try testing.expect(!node.handleVoteRequest(.{ .term = 3, .candidate_id = 3, .last_log_index = 2, .last_log_term = 3 }).vote_granted);
    node.handleTermCheckResponse(checkAnswer(1, 3, 2, 3, &.{ 1, 2, 3 }));
    node.handleTermCheckResponse(checkAnswer(3, 3, 2, 3, &.{ 1, 2, 3 }));
    try testing.expectEqual(LostLog.none, node.lost_log);
    try testing.expect(!node.handleVoteRequest(.{ .term = 3, .candidate_id = 3, .last_log_index = 2, .last_log_term = 3 }).vote_granted);
    try testing.expect(node.handleVoteRequest(.{ .term = 4, .candidate_id = 3, .last_log_index = 2, .last_log_term = 3 }).vote_granted);
}

test "raft node: a group of one has no other vote to wait for" {
    var rec = SinkRecorder{};
    var node = try RaftNode.init(testing.allocator, 2, 0, 4096, .{ .rng_seed = 9 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();
    node.setVoters(&.{2}, 1);
    try node.enterLostVote();
    try testing.expectEqual(LostLog.none, node.lost_log);
    try testing.expect(!rec.lost_log);
}

test "raft node: a leader counts a guarded follower's ack toward no commit" {
    var leader = try RaftNode.init(testing.allocator, 1, 0, 4096, .{ .rng_seed = 9 });
    defer leader.deinit();
    leader.addPeer(2);
    leader.addPeer(3);
    _ = candidacy(&leader);
    _ = leader.handleVoteResponse(.{ .term = leader.current_term, .vote_granted = true, .from = 3 });
    try testing.expectEqual(Role.leader, leader.role);
    const p = try leader.propose(.raft_noop, entry_mod.Flags.NONE, "");
    leader.peers[0].sent_up_to = p.index;
    leader.peers[1].sent_up_to = p.index;
    leader.handleAppendResponse(.{ .term = leader.current_term, .success = true, .match_index = p.index, .from = 2, .guarded = true });
    try testing.expect(leader.commit_index < p.index);
    leader.handleAppendResponse(.{ .term = leader.current_term, .success = true, .match_index = p.index, .from = 2 });
    try testing.expectEqual(p.index, leader.commit_index);
}

test "raft node: a guarded node adopts a higher term from a vote request it refuses" {
    var rec = SinkRecorder{};
    var node = try RaftNode.init(testing.allocator, 2, 0, 4096, .{ .rng_seed = 9 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();
    try node.enterLostLog();
    try testing.expect(!node.handleVoteRequest(.{ .term = 7, .candidate_id = 3, .last_log_index = 0, .last_log_term = 0 }).vote_granted);
    try testing.expectEqual(@as(u64, 7), node.current_term);
    try testing.expectEqual(@as(u64, 7), rec.term);
    try testing.expect(rec.lost_log);
    // A stale leader at an older term is now refused.
    const r = try node.handleAppendEntries(.{ .term = 5, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{}, .leader_commit = 0 });
    try testing.expect(!r.success);
}

test "raft node: a candidate counts no vote that arrives past its election deadline" {
    var node = try RaftNode.init(testing.allocator, 1, 0, 4096, .{ .rng_seed = 9 });
    defer node.deinit();
    node.addPeer(2);
    node.addPeer(3);
    _ = node.tick(1);
    _ = candidacy(&node);
    node.observeTime(node.election_deadline_ms);
    try testing.expectEqual(VoteOutcome.none, node.handleVoteResponse(.{ .term = node.current_term, .vote_granted = true, .from = 2 }));
    try testing.expectEqual(Role.candidate, node.role);
}

test "raft node: a delayed unguarded ack from before a wipe does not make a guarded ack count" {
    var node = try RaftNode.init(testing.allocator, 1, 1000, 8192, .{});
    defer node.deinit();
    node.addPeer(2);
    node.addPeer(3);
    _ = candidacy(&node).?;
    _ = node.handleVoteResponse(.{ .term = 1, .vote_granted = true, .from = 2 });
    try testing.expectEqual(Role.leader, node.role);
    _ = try node.propose(.kv_put, 0, "a");
    sentAll(&node);
    // Peer 2, guarded (it lost its disk), acks index 2: no commit.
    node.handleAppendResponse(.{ .term = 1, .success = true, .match_index = 2, .from = 2, .guarded = true });
    try testing.expectEqual(@as(u64, 0), node.commit_index);
    // A duplicated response from before its wipe, unguarded, acking only
    // index 1: index 2 still does not commit on the guarded ack.
    node.handleAppendResponse(.{ .term = 1, .success = true, .match_index = 1, .from = 2, .guarded = false });
    try testing.expect(node.commit_index < 2);
}

test "raft node: an empty-log node checks the committed config it caught up to, before the owner applies it" {
    var rec = SinkRecorder{};
    var node = try RaftNode.init(testing.allocator, 2, 0, 4096, .{ .rng_seed = 9 });
    defer node.deinit();
    node.hard_state_sink = rec.sink();
    node.timer_enabled = false;
    try node.enterLostLog();
    var a: [membership.MAX_SIZE]u8 = undefined;
    var b: [membership.MAX_SIZE]u8 = undefined;
    // {1,2,3} committed at index 2; {1,2,3,4} appended at 3 by leader 1
    // (term 3), not committed.
    var es = [_]Entry{
        entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 3, 1, 0, ""),
        entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, 3, 2, 0, membership.encode(&membership.Config.ofVoters(&.{ 1, 2, 3 }), &a)),
        entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, 3, 3, 0, membership.encode(&membership.Config.ofVoters(&.{ 1, 2, 3, 4 }), &b)),
    };
    _ = try node.handleAppendEntries(.{ .term = 3, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &es, .leader_commit = 2 });
    try testing.expectEqual(LostLog.confirming, node.lost_log);
    passBootWait(&node);
    // Node 3 (in the committed {1,2,3}) has not answered; 1 and 4 have.
    node.handleTermCheckResponse(checkAnswer(1, 3, 3, 3, &.{ 1, 2, 3, 4 }));
    node.handleTermCheckResponse(checkAnswer(4, 3, 3, 3, &.{ 1, 2, 3, 4 }));
    try testing.expectEqual(LostLog.confirming, node.lost_log);
    node.handleTermCheckResponse(checkAnswer(3, 3, 3, 3, &.{ 1, 2, 3, 4 }));
    try testing.expectEqual(LostLog.none, node.lost_log);
}

test "raft node: a truncated config falls back to the newest one the log still holds, applied or not" {
    var node = try RaftNode.init(testing.allocator, 2, 0, 4096, .{ .rng_seed = 9 });
    defer node.deinit();
    node.timer_enabled = false;
    var a: [membership.MAX_SIZE]u8 = undefined;
    var b: [membership.MAX_SIZE]u8 = undefined;
    // {1,2,3} at 2 from leader 1 (term 1), not yet applied by the owner;
    // {1,2,3,4} at 3, uncommitted.
    var es = [_]Entry{
        entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 0, ""),
        entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, 1, 2, 0, membership.encode(&membership.Config.ofVoters(&.{ 1, 2, 3 }), &a)),
        entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, 1, 3, 0, membership.encode(&membership.Config.ofVoters(&.{ 1, 2, 3, 4 }), &b)),
    };
    _ = try node.handleAppendEntries(.{ .term = 1, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &es, .leader_commit = 2 });
    try testing.expectEqual(@as(u8, 3), node.peer_count);
    // Leader 3 of term 2 never had the config at 3: it is cut.
    var noop2 = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 2, 3, 0, "");
    _ = try node.handleAppendEntries(.{ .term = 2, .leader_id = 3, .prev_log_index = 2, .prev_log_term = 1, .entries = (&noop2)[0..1], .leader_commit = 2 });
    var ids: [MAX_PEERS + 1]NodeId = undefined;
    try testing.expectEqualSlices(NodeId, &.{ 2, 1, 3 }, node.memberIds(&ids));
    try testing.expect(node.timer_enabled);
}

// ── Membership roles ─────────────────────────────────────────────────

/// Node 1 leading `cfg` (which names it a voter), elected by every voter
/// peer, with its leadership noop committed.
fn leading(cfg: membership.Config) !RaftNode {
    return leadingAt(cfg, null);
}

/// A wall clock a test sets.
const TestClock = struct {
    ns: u64,

    fn read(ctx: ?*anyopaque) u64 {
        const self: *const TestClock = @ptrCast(@alignCast(ctx.?));
        return self.ns;
    }

    fn wall(self: *TestClock) WallClock {
        return .{ .ctx = self, .now_ns = read };
    }
};

/// `leading`, on `clock` when given.
fn leadingAt(cfg: membership.Config, clock: ?*TestClock) !RaftNode {
    var node = try RaftNode.init(testing.allocator, 1, 1, 16384, .{});
    errdefer node.deinit();
    if (clock) |c| node.wall_clock = c.wall();
    node.setMembership(&cfg, 0);
    _ = candidacy(&node);
    for (cfg.memberSlice()) |m| {
        if (m.id != 1 and m.voter) _ = node.handleVoteResponse(.{ .term = node.current_term, .vote_granted = true, .from = m.id });
    }
    try testing.expectEqual(Role.leader, node.role);
    for (cfg.memberSlice()) |m| if (m.id != 1) ackAll(&node, m.id);
    try testing.expectEqual(node.log.lastIndex(), node.commit_index);
    return node;
}

/// `from` acks everything in the leader's log.
fn ackAll(node: *RaftNode, from: NodeId) void {
    const i = node.peerIndex(from).?;
    node.peers[i].sent_up_to = node.log.lastIndex();
    node.handleAppendResponse(.{ .term = node.current_term, .success = true, .match_index = node.log.lastIndex(), .from = from, .leader_commit = node.commit_index });
}

const FlushRecorder = struct {
    fail: bool = false,
    calls: u32 = 0,
    node: ?*const RaftNode = null,
    /// The membership the node held when the flush ran.
    membership_at_flush: u64 = 0,

    fn flush(ctx: *anyopaque) bool {
        const self: *FlushRecorder = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        if (self.node) |n| self.membership_at_flush = n.membership_index;
        return !self.fail;
    }

    fn sink(self: *FlushRecorder) LogFlushSink {
        return .{ .ctx = @ptrCast(self), .flush = flush };
    }
};

test "raft node: a replica counts toward no quorum: its ack and its vote count for nothing, and it never campaigns" {
    // A lone voter with a replica leads at once and commits alone.
    var alone = try leading(membership.Config.ofVoters(&.{1}).withJoiner(2, true));
    defer alone.deinit();
    try testing.expectEqual(@as(u8, 1), alone.clusterSize());
    const r = try alone.propose(.kv_put, 0, "x");
    try testing.expectEqual(r.index, alone.commit_index);

    // Voters 1 and 3, replica 2: only 3's ack commits.
    var leader = try leading(membership.Config.ofVoters(&.{ 1, 3 }).withJoiner(2, true));
    defer leader.deinit();
    try testing.expectEqual(@as(u8, 2), leader.clusterSize());
    const w = try leader.propose(.kv_put, 0, "y");
    ackAll(&leader, 2);
    try testing.expect(leader.commit_index < w.index);
    ackAll(&leader, 3);
    try testing.expectEqual(w.index, leader.commit_index);

    // A candidate needs 3, not 2.
    var cand = try RaftNode.init(testing.allocator, 1, 1, 16384, .{});
    defer cand.deinit();
    const cfg = membership.Config.ofVoters(&.{ 1, 3 }).withJoiner(2, true);
    cand.setMembership(&cfg, 0);
    _ = candidacy(&cand);
    _ = cand.handleVoteResponse(.{ .term = cand.current_term, .vote_granted = true, .from = 2 });
    try testing.expectEqual(Role.candidate, cand.role);
    _ = cand.handleVoteResponse(.{ .term = cand.current_term, .vote_granted = true, .from = 3 });
    try testing.expectEqual(Role.leader, cand.role);

    // A replica never campaigns, but answers a candidate as any node does:
    // its vote counts only where the candidate's config names it a voter,
    // and one promoted before it has heard so must still be able to elect.
    var replica = try RaftNode.init(testing.allocator, 2, 1, 16384, .{});
    defer replica.deinit();
    replica.setMembership(&cfg, 0);
    try testing.expect(!replica.timer_enabled and replica.self_named);
    try testing.expect(!replica.tick(1_000_000).start_election);
    const yes = replica.handleVoteRequest(.{ .term = 5, .candidate_id = 3, .last_log_index = 10, .last_log_term = 4 });
    try testing.expect(yes.vote_granted);
}

test "raft node: membership changes go through proposeConfig, one at a time, after the leader has committed in its term, voters by one" {
    var node = try RaftNode.init(testing.allocator, 1, 1, 16384, .{});
    defer node.deinit();
    const two = membership.Config.ofVoters(&.{ 1, 2 });
    node.setMembership(&two, 0);
    node.commitMembership(&two);
    _ = candidacy(&node);
    _ = node.handleVoteResponse(.{ .term = node.current_term, .vote_granted = true, .from = 2 });
    try testing.expectEqual(Role.leader, node.role);
    var buf: [membership.MAX_SIZE]u8 = undefined;
    try testing.expectError(error.ConfigNotChecked, node.propose(.raft_config, 0, membership.encode(&two, &buf)));

    // The leadership noop has not committed: a change now could ride on an
    // uncommitted config of an earlier term.
    const three = two.withJoiner(3, true);
    try testing.expectEqual(RaftNode.ConfigProposal.no_own_commit, try node.proposeConfig(&three));
    ackAll(&node, 2);
    const first = (try node.proposeConfig(&three)).proposed;
    try testing.expectEqual(first.index, node.membership_index);
    try testing.expectEqual(RaftNode.ConfigProposal.in_flight, try node.proposeConfig(&three.withJoiner(4, true)));
    ackAll(&node, 2);
    try testing.expectEqual(first.index, node.commit_index);
    // Two voters at once from two is refused; one is not.
    const jump = membership.Config.ofVoters(&.{ 1, 2, 3, 4 });
    try testing.expectEqual(RaftNode.ConfigProposal{ .refused = .voters_jump }, try node.proposeConfig(&jump));
}

test "raft node: a config is the membership only once it is on disk, on the leader and on a follower" {
    var rec: FlushRecorder = .{};
    var leader = try leading(membership.Config.ofVoters(&.{1}));
    defer leader.deinit();
    leader.log_flush_sink = rec.sink();
    rec.node = &leader;
    const before = leader.log.lastIndex();
    const was = leader.membership_index;
    rec.fail = true;
    try testing.expectError(error.ConfigNotDurable, leader.proposeConfig(&membership.Config.ofVoters(&.{1}).withJoiner(2, true)));
    try testing.expectEqual(before, leader.log.lastIndex());
    try testing.expectEqual(was, leader.membership_index);
    rec.fail = false;
    const p = (try leader.proposeConfig(&membership.Config.ofVoters(&.{1}).withJoiner(2, true))).proposed;
    try testing.expectEqual(was, rec.membership_at_flush);
    try testing.expectEqual(p.index, leader.membership_index);

    // A follower: the batch holding the config is flushed, then adopted.
    var follower = try RaftNode.init(testing.allocator, 2, 1, 16384, .{});
    defer follower.deinit();
    var frec: FlushRecorder = .{ .fail = true };
    follower.log_flush_sink = frec.sink();
    frec.node = &follower;
    var cfg_buf: [membership.MAX_SIZE]u8 = undefined;
    var cfg = testEntry(1, 1, membership.encode(&membership.Config.ofVoters(&.{ 1, 2 }), &cfg_buf));
    cfg.header.entry_type = @intFromEnum(EntryType.raft_config);
    cfg.header.crc32c = cfg.computeCrc();
    const req: AppendRequest = .{ .term = 1, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{cfg}, .leader_commit = 0 };
    try testing.expectError(error.ConfigNotDurable, follower.handleAppendEntries(req));
    try testing.expectEqual(@as(u64, 0), follower.log.lastIndex());
    try testing.expectEqual(@as(u64, 0), follower.membership_index);
    frec.fail = false;
    const resp = try follower.handleAppendEntries(req);
    try testing.expect(resp.success);
    try testing.expectEqual(@as(u64, 0), frec.membership_at_flush);
    try testing.expectEqual(@as(u64, 1), follower.membership_index);
}

test "raft node: a leader that removes itself leads on without counting itself, and steps down once the change commits" {
    var leader = try leading(membership.Config.ofVoters(&.{ 1, 2, 3 }));
    defer leader.deinit();
    const out = leader.latest_config.without(1, 42);
    const p = (try leader.proposeConfig(&out)).proposed;
    try testing.expectEqual(@as(u8, 2), leader.clusterSize());
    try testing.expectEqual(Role.leader, leader.role);
    // Its own copy no longer counts: one of the two remaining is not enough.
    ackAll(&leader, 2);
    try testing.expect(leader.commit_index < p.index);
    ackAll(&leader, 3);
    try testing.expectEqual(p.index, leader.commit_index);
    // Applied, the change takes it out of office.
    leader.commitMembership(&out);
    try testing.expectEqual(Role.follower, leader.role);
    try testing.expect(leader.left_group);
}

test "raft node: a peer acking at the commit index three times in a row is caught up" {
    var leader = try leading(membership.Config.ofVoters(&.{1}).withJoiner(2, true));
    defer leader.deinit();
    var out: [MAX_PEERS]membership.Progress = undefined;
    // `leading` acked once already.
    for (0..CAUGHT_UP_ACKS - 2) |_| ackAll(&leader, 2);
    try testing.expect(!leader.memberProgress(&out)[0].caught_up_now);
    ackAll(&leader, 2);
    try testing.expect(leader.memberProgress(&out)[0].caught_up_now);
    // Behind, the run starts over.
    _ = try leader.propose(.kv_put, 0, "z");
    const i = leader.peerIndex(2).?;
    leader.handleAppendResponse(.{ .term = leader.current_term, .success = true, .match_index = leader.log.lastIndex() - 1, .from = 2, .leader_commit = leader.log.lastIndex() });
    try testing.expectEqual(@as(u8, 0), leader.peers[i].caught_up_streak);
    // Under steady writes the commit index has moved on by the time an ack
    // arrives; an ack holding what was committed when its batch left still
    // counts, or a joiner would never catch up.
    const sent_at = leader.commit_index;
    _ = try leader.propose(.kv_put, 0, "later");
    try testing.expect(leader.commit_index > sent_at);
    leader.peers[i].sent_up_to = sent_at;
    leader.handleAppendResponse(.{ .term = leader.current_term, .success = true, .match_index = sent_at, .from = 2, .leader_commit = sent_at });
    try testing.expectEqual(@as(u8, 1), leader.peers[i].caught_up_streak);
}

test "raft node: a lone voter promoting a second does not commit the promotion on its own disk" {
    var rec: FlushRecorder = .{};
    var leader = try leading(membership.Config.ofVoters(&.{1}).withJoiner(2, true));
    defer leader.deinit();
    leader.log_flush_sink = rec.sink();
    const promoted = leader.latest_config.withMember(.{ .id = 2, .voter = true, .may_vote = true, .caught_up = true });
    const p = (try leader.proposeConfig(&promoted)).proposed;
    try testing.expect(leader.commit_index < p.index);
    ackAll(&leader, 2);
    try testing.expectEqual(p.index, leader.commit_index);
}

test "raft node: a leader that removed itself and lost office before the change committed may still stand, counting only the voters it leaves" {
    var leader = try leading(membership.Config.ofVoters(&.{ 1, 2, 3 }));
    defer leader.deinit();
    leader.commitMembership(&leader.latest_config);
    _ = (try leader.proposeConfig(&leader.latest_config.without(1, 42))).proposed;
    // A higher term deposes it before the change commits.
    leader.handleAppendResponse(.{ .term = leader.current_term + 1, .success = false, .match_index = 0, .from = 2 });
    try testing.expectEqual(Role.follower, leader.role);
    try testing.expect(!leader.timer_enabled);
    try testing.expect(leader.mayCampaign());
    _ = leader.tick(1);
    try testing.expect(leader.tick(1_000_000).start_election);
    const req = candidacy(&leader).?;
    _ = req;
    // Its own vote does not count: both remaining voters must grant.
    _ = leader.handleVoteResponse(.{ .term = leader.current_term, .vote_granted = true, .from = 2 });
    try testing.expectEqual(Role.candidate, leader.role);
    _ = leader.handleVoteResponse(.{ .term = leader.current_term, .vote_granted = true, .from = 3 });
    try testing.expectEqual(Role.leader, leader.role);
}

/// `leader` sends `follower` everything from `from`, and takes the answer.
fn replicate(leader: *RaftNode, follower: *RaftNode, from: u64) !void {
    var entries: [16]Entry = undefined;
    var arena: [4096]u8 = undefined;
    const n = leader.log.getRange(from, &entries, &arena);
    const resp = try follower.handleAppendEntries(.{
        .term = leader.current_term,
        .leader_id = leader.id,
        .prev_log_index = from - 1,
        .prev_log_term = leader.log.entryTerm(from - 1) orelse 0,
        .entries = entries[0..n],
        .leader_commit = leader.commit_index,
    });
    leader.peers[leader.peerIndex(follower.id).?].sent_up_to = leader.log.lastIndex();
    leader.handleAppendResponse(resp);
}

/// `candidate` stands; each of `voters` answers. True when it won.
fn stand(candidate: *RaftNode, voters: []const *RaftNode) bool {
    const req = candidacy(candidate) orelse return false;
    for (voters) |v| {
        v.observeTime(0);
        _ = candidate.handleVoteResponse(v.handleVoteRequest(req));
    }
    return candidate.role == .leader;
}

test "raft node: a new leader changes membership only after committing in its term, or two configs commit apart (Ongaro, 2015)" {
    // Voters 1-4, replica 5. Node 1 leads term 1, promotes 5, gets that
    // only to 5, and stops.
    var nodes: [5]RaftNode = undefined;
    for (&nodes, 1..) |*n, id| n.* = try RaftNode.init(testing.allocator, @intCast(id), 1, 16384, .{ .enable_pre_vote = false });
    defer for (&nodes) |*n| n.deinit();
    const c0 = membership.Config.ofVoters(&.{ 1, 2, 3, 4 }).withJoiner(5, true);
    for (&nodes) |*n| {
        n.setMembership(&c0, 0);
        n.commitMembership(&c0);
    }
    const n1 = &nodes[0];
    const n2 = &nodes[1];
    const n3 = &nodes[2];
    const n4 = &nodes[3];
    const n5 = &nodes[4];
    try testing.expect(stand(n1, &.{ n2, n3 }));
    for ([_]*RaftNode{ n2, n3, n4, n5 }) |f| try replicate(n1, f, 1);
    const c1 = c0.withMember(.{ .id = 5, .voter = true, .may_vote = true, .caught_up = true });
    _ = (try n1.proposeConfig(&c1)).proposed;
    try replicate(n1, n5, 2);

    // Node 2 leads term 2 without it, with 3 and 4, and at once removes
    // node 1. The gate holds the change until its term's noop commits,
    // which takes a majority of the old voters: 4 among them.
    try testing.expect(stand(n2, &.{ n3, n4 }));
    const c2 = n2.latest_config.without(1, null);
    var change = try n2.proposeConfig(&c2);
    if (change == .no_own_commit) {
        try replicate(n2, n3, 2);
        try replicate(n2, n4, 2);
        change = try n2.proposeConfig(&c2);
    }
    const p = change.proposed;
    try replicate(n2, n3, p.index - 1);
    try testing.expectEqual(p.index, n2.commit_index);

    // Node 1 comes back and stands with its config naming 5 a voter. With
    // the gate, 4 holds node 2's term and refuses it; without it, 1, 4 and
    // 5 would elect 1, and it would overwrite what 2 and 3 committed.
    // Its first try lands in term 2, where 4 already voted; the next is
    // decided by whose log is newer.
    if (stand(n1, &.{ n4, n5 }) or stand(n1, &.{ n4, n5 })) {
        _ = try n1.propose(.kv_put, 0, "x");
        try replicate(n1, n4, 1);
        try replicate(n1, n5, 1);
    }
    // Every index both sides committed holds the same entry.
    const both = @min(n1.commit_index, n2.commit_index);
    var i: u64 = 1;
    while (i <= both) : (i += 1) try testing.expectEqual(n2.log.entryTerm(i), n1.log.entryTerm(i));
    try testing.expect(n1.role != .leader);
}

test "raft node: a leader leaves office when its removal commits, before any apply, and starts no change meanwhile" {
    // Voters 1 and 2, replicas 3 and 4 that may vote.
    const c0 = membership.Config.ofVoters(&.{ 1, 2 }).withJoiner(3, true).withJoiner(4, true);
    var nodes: [4]RaftNode = undefined;
    for (&nodes, 1..) |*n, id| n.* = try RaftNode.init(testing.allocator, @intCast(id), 1, 16384, .{ .enable_pre_vote = false });
    defer for (&nodes) |*n| n.deinit();
    for (&nodes) |*n| {
        n.setMembership(&c0, 0);
        n.commitMembership(&c0);
    }
    const n1 = &nodes[0];
    const n2 = &nodes[1];
    try testing.expect(stand(n1, &.{n2}));
    for ([_]*RaftNode{ n2, &nodes[2], &nodes[3] }) |f| try replicate(n1, f, 1);
    const c1 = n1.latest_config.without(1, 42);
    const p1 = (try n1.proposeConfig(&c1)).proposed;
    // Before the change commits it still leads, without a vote, and starts
    // nothing: 2 alone making 3 and 4 voters would leave 2 able to commit
    // alone beside them.
    var c2 = c1.withMember(.{ .id = 3, .voter = true, .may_vote = true, .caught_up = true });
    c2 = c2.withMember(.{ .id = 4, .voter = true, .may_vote = true, .caught_up = true });
    try testing.expectEqual(RaftNode.ConfigProposal{ .refused = .not_a_voter }, try n1.proposeConfig(&c2));
    try replicate(n1, n2, p1.index);
    // Committed: out of office at once, with no apply (no commitMembership).
    try testing.expectEqual(p1.index, n1.commit_index);
    try testing.expect(n1.role != .leader);
    try testing.expect(n1.left_group);
    try testing.expectError(error.NotLeader, n1.proposeConfig(&c2));
}

test "raft node: caught up means acking at the commit index lately: a silent or failing replica is not" {
    var leader = try leading(membership.Config.ofVoters(&.{1}).withJoiner(2, true).withJoiner(3, true));
    defer leader.deinit();
    var out: [MAX_PEERS]membership.Progress = undefined;
    for (0..CAUGHT_UP_ACKS) |_| ackAll(&leader, 2);
    try testing.expect(leader.memberProgress(&out)[0].caught_up_now);
    // 2 goes silent past an election timeout.
    leader.current_time_ms += leader.config.election_timeout_max_ms + 1;
    try testing.expect(!leader.memberProgress(&out)[0].caught_up_now);
    // Back and caught up, then it answers guarded (it lost its disk): the
    // run starts over.
    for (0..CAUGHT_UP_ACKS) |_| ackAll(&leader, 2);
    try testing.expect(leader.memberProgress(&out)[0].caught_up_now);
    const i = leader.peerIndex(2).?;
    leader.peers[i].sent_up_to = leader.log.lastIndex();
    leader.handleAppendResponse(.{ .term = leader.current_term, .success = true, .match_index = leader.log.lastIndex(), .from = 2, .guarded = true });
    try testing.expect(!leader.memberProgress(&out)[0].caught_up_now);
    // Caught up again, then an append fails: the run starts over.
    for (0..CAUGHT_UP_ACKS) |_| ackAll(&leader, 2);
    try testing.expect(leader.memberProgress(&out)[0].caught_up_now);
    leader.handleAppendResponse(.{ .term = leader.current_term, .success = false, .match_index = 0, .hint_index = 0, .from = 2 });
    try testing.expect(!leader.memberProgress(&out)[0].caught_up_now);
    try testing.expect(membership.nextAutomatic(&leader.latest_config, leader.memberProgress(&out), 1000, leader.voterCountBefore()) == null);
}

// ── Stamps ──────────────────────────────────────────────────────────────

/// `from` acks everything, reporting its wall clock as `clock_ns`.
fn ackWithClock(node: *RaftNode, from: NodeId, clock_ns: u64) void {
    const i = node.peerIndex(from).?;
    node.peers[i].sent_up_to = node.log.lastIndex();
    node.handleAppendResponse(.{ .term = node.current_term, .success = true, .match_index = node.log.lastIndex(), .from = from, .leader_commit = node.commit_index, .clock_ns = clock_ns });
}

const s_ns = std.time.ns_per_s;

test "raft node: a lone leader stamps at its clock, and after its last stamp when the clock is set back" {
    var clock: TestClock = .{ .ns = 1_000 * s_ns };
    var node = try leadingAt(membership.Config.ofVoters(&.{1}), &clock);
    defer node.deinit();
    clock.ns += s_ns;
    const a = try node.propose(.kv_put, 0, "a");
    try testing.expectEqual(clock.ns, a.timestamp_ns);
    // The same instant: the next is 1 ns on.
    const b = try node.propose(.kv_put, 0, "b");
    try testing.expectEqual(a.timestamp_ns + 1, b.timestamp_ns);
    // Set back 10 s: still after the last, and the skew says so.
    clock.ns -= 10 * s_ns;
    const c = try node.propose(.kv_put, 0, "c");
    try testing.expectEqual(b.timestamp_ns + 1, c.timestamp_ns);
    try testing.expect(node.stamp_clock_behind);
    try testing.expect(node.stamp_skew_ns >= 10 * s_ns);
    // Past it again: back on the clock.
    clock.ns += 20 * s_ns;
    const d = try node.propose(.kv_put, 0, "d");
    try testing.expectEqual(clock.ns, d.timestamp_ns);
    try testing.expect(!node.stamp_clock_behind);
    try testing.expectEqual(d.timestamp_ns, node.log.last_stamp);
}

test "raft node: a leader stamps no more than a second past the clock a majority of the voters keep, and one fast clock moves nothing" {
    const t = 1_000 * s_ns;
    var clock: TestClock = .{ .ns = t };
    var leader = try leadingAt(membership.Config.ofVoters(&.{ 1, 2, 3 }), &clock);
    defer leader.deinit();
    // Its own clock is 100 s fast; both voters keep true time.
    ackWithClock(&leader, 2, t);
    ackWithClock(&leader, 3, t);
    clock.ns = t + 100 * s_ns;
    const p = try leader.propose(.kv_put, 0, "x");
    try testing.expectEqual(t + s_ns, p.timestamp_ns);
    try testing.expectEqual(99 * s_ns, leader.stamp_skew_ns);
    try testing.expect(!leader.stamp_clock_behind);
    try testing.expect(leader.stamp_held_by == 2 or leader.stamp_held_by == 3);
    // A report ages by this node's monotonic time since it came.
    leader.observeTime(leader.current_time_ms + 2_000);
    const q = try leader.propose(.kv_put, 0, "y");
    try testing.expectEqual(t + 3 * s_ns, q.timestamp_ns);

    // A leader on true time, with one voter an hour fast: true time.
    var clock2: TestClock = .{ .ns = t };
    var other = try leadingAt(membership.Config.ofVoters(&.{ 1, 2, 3 }), &clock2);
    defer other.deinit();
    ackWithClock(&other, 2, t + std.time.ns_per_hour);
    ackWithClock(&other, 3, t);
    clock2.ns = t + 5 * s_ns;
    ackWithClock(&other, 3, t + 5 * s_ns);
    const r = try other.propose(.kv_put, 0, "z");
    try testing.expectEqual(t + 5 * s_ns, r.timestamp_ns);
}

test "raft node: a new leader stamps from the clocks its votes carried, and after the last leader's stamps when its own clock is behind" {
    var clocks = [_]TestClock{ .{ .ns = 2_000 * s_ns }, .{ .ns = 1_000 * s_ns }, .{ .ns = 1_000 * s_ns } };
    var nodes: [3]RaftNode = undefined;
    for (&nodes, &clocks, 1..) |*n, *c, id| {
        n.* = try RaftNode.init(testing.allocator, @intCast(id), 1, 16384, .{ .enable_pre_vote = false });
        n.wall_clock = c.wall();
    }
    defer for (&nodes) |*n| n.deinit();
    const c0 = membership.Config.ofVoters(&.{ 1, 2, 3 });
    for (&nodes) |*n| {
        n.setMembership(&c0, 0);
        n.commitMembership(&c0);
    }
    const n1 = &nodes[0];
    const n2 = &nodes[1];
    const n3 = &nodes[2];
    // Node 1, its clock 1000 s fast, wins with node 2's vote. The vote
    // carried node 2's clock, so its first stamp is within a second of
    // that rather than at its own.
    try testing.expect(stand(n1, &.{n2}));
    try testing.expectEqual(1_001 * s_ns, n1.log.last_stamp);
    const a = try n1.propose(.kv_put, 0, "a");
    // The acks carry their clocks too.
    clocks[1].ns += 3 * s_ns;
    for ([_]*RaftNode{ n2, n3 }) |f| try replicate(n1, f, 1);
    try testing.expectEqual(clocks[1].ns, n1.peers[n1.peerIndex(2).?].clock_ns);
    clocks[1].ns -= 3 * s_ns;

    // Node 2, its clock now 1 s behind node 1's last stamp, takes over:
    // its stamps still follow node 1's.
    try testing.expect(stand(n2, &.{n3}));
    try testing.expect(n2.log.last_stamp > a.timestamp_ns);
    const b = try n2.propose(.kv_put, 0, "b");
    try testing.expect(b.timestamp_ns > n2.log.ual.readHeader(b.index - 1).?.timestamp_ns);
    try testing.expect(n2.stamp_clock_behind);
}

test "raft node: a follower's time is its last applied stamp moved on by its own monotonic time, never past its clock" {
    var clock: TestClock = .{ .ns = 1_000 * s_ns };
    var node = try RaftNode.init(testing.allocator, 2, 1, 16384, .{});
    defer node.deinit();
    node.wall_clock = clock.wall();
    // Nothing applied: its clock.
    try testing.expectEqual(clock.ns, node.now());
    // Applied a stamp 5 s ahead of its clock: never behind that stamp.
    node.observeTime(10_000);
    node.noteApplied(1_005 * s_ns);
    try testing.expectEqual(1_005 * s_ns, node.now());
    node.observeTime(10_500);
    try testing.expectEqual(1_005 * s_ns, node.now());
    // Its clock jumps far ahead: time moves on only as fast as its
    // monotonic clock since that apply.
    clock.ns = 2_000 * s_ns;
    try testing.expectEqual(1_005 * s_ns + 500 * std.time.ns_per_ms, node.now());
}

test "raft node: a config that fails to reach the disk leaves the next stamp after the entry before it" {
    var rec: FlushRecorder = .{};
    var clock: TestClock = .{ .ns = 1_000 * s_ns };
    var leader = try leadingAt(membership.Config.ofVoters(&.{1}), &clock);
    defer leader.deinit();
    leader.log_flush_sink = rec.sink();
    const a = try leader.propose(.kv_put, 0, "a");
    clock.ns -= 10 * s_ns;
    rec.fail = true;
    try testing.expectError(error.ConfigNotDurable, leader.proposeConfig(&membership.Config.ofVoters(&.{1}).withJoiner(2, true)));
    rec.fail = false;
    const b = try leader.propose(.kv_put, 0, "b");
    try testing.expectEqual(a.index + 1, b.index);
    try testing.expect(b.timestamp_ns > a.timestamp_ns);
}

test "raft node: a replica's clock has no say in the voters' clock" {
    const t = 1_000 * s_ns;
    var clock: TestClock = .{ .ns = t };
    var leader = try leadingAt(membership.Config.ofVoters(&.{ 1, 2 }).withJoiner(3, true), &clock);
    defer leader.deinit();
    // The leader is 100 s fast and its one other voter keeps true time; a
    // replica an hour fast would, if it counted, side with the leader.
    ackWithClock(&leader, 2, t);
    ackWithClock(&leader, 3, t + std.time.ns_per_hour);
    clock.ns = t + 100 * s_ns;
    const p = try leader.propose(.kv_put, 0, "x");
    try testing.expectEqual(t + s_ns, p.timestamp_ns);
    try testing.expectEqual(@as(NodeId, 2), leader.stamp_held_by);
}
