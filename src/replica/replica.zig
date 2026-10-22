//! Replica — this node's side of one Raft group: the frames it takes from
//! its peers, its clock (elections, check-quorum, the leader loop), and
//! what a leader sends each peer.
//!
//! `Io` is its clock, its links to the peers, its disk, and its host: a
//! shard, which owns the log, the state machine and the clients. Until
//! applying entries and the membership steps move in here, the replica
//! reaches the shard for them through `Io.Host`.

const std = @import("std");
const log = @import("stdx").log;
const RaftNode = @import("../raft/node.zig").RaftNode;
const raft_node_mod = @import("../raft/node.zig");
const RaftFrame = @import("../raft/raft_queue.zig").Frame;
const transport = @import("../raft/transport.zig");
const membership = @import("../raft/membership.zig");
const entry_mod = @import("../storage/ual/entry.zig");

/// Steady conditions are said once per interval, not per tick.
pub const WARN_INTERVAL_MS: u64 = 30_000;

/// Non-members whose term checks are answered at once, at most once each
/// per election timeout; past this many, the longest-silent is forgotten.
pub const ANSWERED_SOURCES = 16;

/// A committed config a member answered with, newer than this node's own:
/// a member that missed a membership change learns the group from it.
pub const Vouch = struct {
    config: membership.Config = .{},
    index: u64 = 0,
    term: u64 = 0,
};

pub const DIVERGED_MESSAGE = "unavailable: this node's data diverged from the group and it takes no writes; use another node";

pub fn Replica(comptime Io: type) type {
    return struct {
        const Self = @This();

        /// The shard this replica serves, for its log lines.
        id: u16,
        /// Scratch for one AppendEntries in either direction: the entries of a
        /// batch, the payload bytes they point into, and the frame payload.
        rpc_entries: []entry_mod.Entry,
        rpc_arena: []u8,
        rpc_out: []u8,
        /// The leader disagreed with history this node had committed and
        /// applied, or its own data is damaged: its projections cannot be
        /// trusted, so it takes no part until it is wiped and rejoined.
        diverged: bool,
        conflicts_seen: u64 = 0,
        /// Last line about a replica speaking out of turn.
        replica_frame_warn_ms: u64 = 0,
        /// The config index whose voters the peer network last heard.
        voters_published_index: u64 = std.math.maxInt(u64),
        /// Last "lost-log guard" line: a node waiting on a leader or a quorum
        /// that never comes says so, and names the way out.
        lost_log_warn_ms: u64 = 0,
        frame_warn_ms: u64 = 0,
        election_warn_ms: u64 = 0,
        elections_unlogged: u64 = 0,
        /// One limiter per peer for each thing the leader loop says about it.
        peer_silent_warn_ms: [raft_node_mod.MAX_PEERS]u64 = @splat(0),
        peer_batch_warn_ms: [raft_node_mod.MAX_PEERS]u64 = @splat(0),

        /// A member that missed a membership change drops the frames of a
        /// leader admitted since, and never catches up: the entries that
        /// would name that leader come from it. So, when it drops a
        /// stranger's frame or hears no leader, it asks the members its own
        /// configs name for their committed config, and while one is newer
        /// than its own also hears that config's members' appends and vote
        /// requests. Quorum, votes and commit still read only the log.
        vouch: Vouch = .{},
        /// The current round of asks: when it went out, and to whom. Only
        /// an asked member's answer is taken.
        ask_ms: u64 = 0,
        asked: [membership.MAX_MEMBERS]u32 = undefined,
        asked_count: u8 = 0,
        /// When each non-member's term check was last answered.
        answered: [ANSWERED_SOURCES]Answered = @splat(.{}),
        stranger_warn_ms: u64 = 0,
        /// When a stranger's frame was last dropped with no leader heard.
        strangers_quiet_ms: u64 = 0,
        /// For the host's metrics.
        strangers_dropped: u64 = 0,
        vouch_asks: u64 = 0,
        vouches_taken: u64 = 0,

        const Answered = struct { id: u32 = 0, at_ms: u64 = 0 };

        // ─── Disk ───────────────────────────────────────────────────────

        fn durableCommits(self: *Self) bool {
            return Io.durableCommits(self);
        }

        fn flushForAck(self: *Self) bool {
            return Io.flushForAck(self);
        }

        // ─── The host ────────────────────────────────────────────────────

        fn raftNode(self: *Self) *RaftNode {
            return Io.Host.raftNode(self);
        }

        fn isRemoved(self: *Self) bool {
            return Io.Host.isRemoved(self);
        }

        fn checkRemoved(self: *Self, now: u64) bool {
            return Io.Host.checkRemoved(self, now);
        }

        fn tickMembership(self: *Self, now: u64) void {
            Io.Host.tickMembership(self, now);
        }

        fn applyCommitted(self: *Self) bool {
            return Io.Host.applyCommitted(self);
        }

        fn clearRemovalNotices(self: *Self) void {
            Io.Host.clearRemovalNotices(self);
        }

        fn strangerFrame(self: *Self, frame: RaftFrame) void {
            Io.Host.strangerFrame(self, frame);
        }

        fn handleJoinRequest(self: *Self, from: u32) void {
            Io.Host.handleJoinRequest(self, from);
        }

        fn takeRemovedNotice(self: *Self, frame: RaftFrame) void {
            Io.Host.takeRemovedNotice(self, frame);
        }

        fn runForwardedWrite(self: *Self, frame: RaftFrame) void {
            Io.Host.runForwardedWrite(self, frame);
        }

        fn takeForwardReply(self: *Self, frame: RaftFrame) void {
            Io.Host.takeForwardReply(self, frame);
        }

        fn stopTakingPart(self: *Self, message: []const u8) void {
            Io.Host.stopTakingPart(self, message);
        }

        fn stoppedLeading(self: *Self, message: []const u8) void {
            Io.Host.stoppedLeading(self, message);
        }

        pub fn sendRaft(self: *Self, peer: u32, msg_type: transport.MsgType, payload: []const u8) void {
            _ = self.trySendRaft(peer, msg_type, payload);
        }

        fn trySendRaft(self: *Self, peer: u32, msg_type: transport.MsgType, payload: []const u8) bool {
            if (!Io.networked(self)) return false;
            if (Io.send(self, peer, msg_type, self.id, payload)) return true;
            log.warn("shard {d}: could not queue a {s} for node {d}", .{ self.id, @tagName(msg_type), peer });
            return false;
        }

        /// One Raft frame from a proven peer. The link proves who sent it;
        /// the ids inside must agree, or one member could speak for another.
        /// Once this node knows the membership, only members speak for the
        /// group; a node with the secret but no seat can still ask for one.
        pub fn handleRaftFrame(self: *Self, frame: RaftFrame) void {
            const raft = self.raftNode();
            // The clock before each frame: a vote that arrives past the
            // candidacy's deadline is not counted on an older clock, even
            // after a stall part way through a queue of them.
            raft.observeTime(Io.nowMs(self));
            if (self.diverged or self.isRemoved()) return;
            // Anyone may ask for this node's term and configs (read-only),
            // as anyone may ask to join: a member that missed a change may
            // know only nodes that have since dropped it.
            if (frame.msg_type != .join_request and frame.msg_type != .term_check and (raft.peer_count > 0 or raft.self_named)) {
                // Members of the latest config, and of the committed one until
                // the change commits: a leader removing itself leads the group
                // that drops it until the group has taken that.
                if (!self.isMember(frame.source_node)) {
                    if (self.vouchAdmits(frame.source_node, frame.msg_type)) return self.handleFrameFrom(frame);
                    return self.dropStranger(frame);
                }
                // A joiner keeps its guard while it catches up: the configs it
                // passes through on the way are history, and may call it a
                // replica it no longer is.
                if (raft.isReplicaPeer(frame.source_node) and !replicaMaySend(frame.msg_type)) return self.replicaFrame(frame);
            }
            self.handleFrameFrom(frame);
        }

        /// A frame past the guard.
        fn handleFrameFrom(self: *Self, frame: RaftFrame) void {
            const raft = self.raftNode();
            switch (frame.msg_type) {
                .append_entries => {
                    const hdr = transport.deserializeAppendRequestHeader(frame.payload) orelse return self.badFrame(frame);
                    if (hdr.leader_id != frame.source_node) return self.impostorFrame(frame, hdr.leader_id);
                    const count = transport.parseEntries(hdr.entries_data, hdr.entry_count, self.rpc_entries);
                    if (count != hdr.entry_count) return self.badFrame(frame);
                    const req = raft_node_mod.AppendRequest{
                        .term = hdr.term,
                        .leader_id = hdr.leader_id,
                        .prev_log_index = hdr.prev_log_index,
                        .prev_log_term = hdr.prev_log_term,
                        .leader_commit = hdr.leader_commit,
                        .entries = self.rpc_entries[0..count],
                    };
                    const led_by = raft.leader_id;
                    const was = raft.role;
                    const resp = raft.handleAppendEntries(req) catch |err| {
                        switch (err) {
                            // Not answered: an ack would let the leader count
                            // this node, and there is nothing honest to say.
                            error.CommittedConflict => self.markDiverged(hdr.leader_id, hdr.term),
                            error.MalformedBatch => self.badFrame(frame),
                            else => log.err("shard {d}: could not take a batch from leader {d}: {s}", .{ self.id, hdr.leader_id, @errorName(err) }),
                        }
                        return;
                    };
                    // Without durable commits the node's log followed the leader
                    // over history it had applied; its projections did not.
                    if (raft.committed_conflicts != self.conflicts_seen) {
                        self.conflicts_seen = raft.committed_conflicts;
                        return self.markDiverged(hdr.leader_id, hdr.term);
                    }
                    if (was == .leader and raft.role != .leader) self.leadershipLost("a leader with a newer term spoke");
                    if (raft.leader_id != led_by and raft.leader_id != 0) log.info("shard {d}: following node {d} (term {d})", .{ self.id, raft.leader_id, raft.current_term });
                    // A leader of this term still sends to this node as a member.
                    if (hdr.term == raft.current_term) self.clearRemovalNotices();
                    // An ack means "in my log, on disk" when commits are durable.
                    if (resp.success and count > 0 and self.durableCommits()) {
                        if (!self.flushForAck()) return;
                        raft.markDurable(raft.log.lastIndex());
                    }
                    var buf: [transport.APPEND_RESP_SIZE]u8 = undefined;
                    const n = transport.serializeAppendResponse(resp, &buf) orelse return;
                    self.sendRaft(frame.source_node, .append_entries_response, buf[0..n]);
                    if (!self.applyCommitted()) log.err("shard {d}: a committed entry could not be applied", .{self.id});
                },
                .append_entries_response => {
                    const resp = transport.deserializeAppendResponse(frame.payload) orelse return self.badFrame(frame);
                    if (resp.from != frame.source_node) return self.impostorFrame(frame, resp.from);
                    const was = raft.role;
                    raft.handleAppendResponse(resp);
                    // Applied first: a leader whose own removal just committed
                    // answers what it parked on that commit before it lets go.
                    if (!self.applyCommitted()) log.err("shard {d}: a committed entry could not be applied", .{self.id});
                    if (was == .leader and raft.role != .leader) {
                        if (raft.left_group) {
                            raft.left_group = false;
                            self.leadershipLost("this node is no longer a voter");
                        } else {
                            self.leadershipLost("a follower is at a newer term");
                        }
                    }
                    if (raft.role == .leader) self.pump(Io.nowMs(self));
                },
                .request_vote => {
                    const req = transport.deserializeVoteRequest(frame.payload) orelse return self.badFrame(frame);
                    if (req.candidate_id != frame.source_node) return self.impostorFrame(frame, req.candidate_id);
                    const was = raft.role;
                    const resp = raft.handleVoteRequest(req);
                    if (was == .leader and raft.role != .leader) self.leadershipLost("a candidate is at a newer term");
                    var buf: [transport.VOTE_RESP_SIZE]u8 = undefined;
                    const n = transport.serializeVoteResponse(resp, &buf) orelse return;
                    self.sendRaft(frame.source_node, .request_vote_response, buf[0..n]);
                },
                .request_vote_response => {
                    const resp = transport.deserializeVoteResponse(frame.payload) orelse return self.badFrame(frame);
                    if (resp.from != frame.source_node) return self.impostorFrame(frame, resp.from);
                    const was = raft.role;
                    const outcome = raft.handleVoteResponse(resp);
                    if (was == .leader and raft.role != .leader) self.leadershipLost("a voter is at a newer term");
                    switch (outcome) {
                        .none => {},
                        .elect => |req| {
                            log.info("shard {d}: a majority would vote; standing for term {d}", .{ self.id, req.term });
                            self.broadcastVote(req);
                        },
                        .won => {
                            log.info("shard {d}: elected leader for term {d}", .{ self.id, raft.current_term });
                            self.pump(Io.nowMs(self));
                        },
                    }
                },
                .term_check => {
                    const req = transport.deserializeTermCheck(frame.payload) orelse return self.badFrame(frame);
                    if (req.from != frame.source_node) return self.impostorFrame(frame, req.from);
                    if (!self.isMember(frame.source_node) and !self.mayAnswer(frame.source_node)) return;
                    var buf: [transport.TERM_CHECK_RESP_MAX]u8 = undefined;
                    const n = transport.serializeTermCheckResponse(raft.handleTermCheck(req), &buf) orelse return;
                    self.sendRaft(frame.source_node, .term_check_response, buf[0..n]);
                },
                .term_check_response => {
                    const resp = transport.deserializeTermCheckResponse(frame.payload) orelse return self.badFrame(frame);
                    if (resp.from != frame.source_node) return self.impostorFrame(frame, resp.from);
                    const was = raft.lost_log;
                    self.takeVouch(resp);
                    raft.handleTermCheckResponse(resp);
                    if (was != .none and raft.lost_log == .none) log.info("shard {d}: lost-log guard done; term {d} confirmed, voting from term {d} on", .{ self.id, raft.current_term, raft.current_term +| 1 });
                },
                .join_request => self.handleJoinRequest(frame.source_node),
                .removed_notice => self.takeRemovedNotice(frame),
                .forward_write => self.runForwardedWrite(frame),
                .forward_reply => self.takeForwardReply(frame),
                .peer_info, .hello, .hello_back, .verify, .welcome => {},
            }
        }

        // ─── Members, strangers and vouches ─────────────────────────────

        /// Named by this node's latest or committed config.
        fn isMember(self: *Self, id: u32) bool {
            const raft = self.raftNode();
            var ids: [membership.MAX_MEMBERS]u32 = undefined;
            return membership.names(raft.memberIds(&ids), id) or raft.committed_config.names(id);
        }

        /// A vouch holds until this node's own committed history reaches
        /// it: from there the log's configs cover it. Never judged by the
        /// latest config, which may be uncommitted and prove nothing.
        fn vouchLive(self: *Self) bool {
            const raft = self.raftNode();
            return self.vouch.index > 0 and raft.commit_index < self.vouch.index and raft.committed_config_index < self.vouch.index;
        }

        /// The vouched config's index while it holds, else 0.
        pub fn vouchedIndex(self: *Self) u64 {
            return if (self.vouchLive()) self.vouch.index else 0;
        }

        /// Dropping a stranger's frames with no leader heard, lately: a
        /// member stuck on an old config looks like this.
        pub fn strangersWhileQuiet(self: *Self, now: u64) bool {
            return self.strangers_quiet_ms != 0 and now -| self.strangers_quiet_ms < WARN_INTERVAL_MS;
        }

        /// A vouched config's member may send this node its log and ask
        /// for its vote; nothing else.
        fn vouchAdmits(self: *Self, id: u32, t: transport.MsgType) bool {
            if (t != .append_entries and t != .request_vote) return false;
            return self.vouchLive() and self.vouch.config.names(id);
        }

        fn dropStranger(self: *Self, frame: RaftFrame) void {
            self.strangers_dropped += 1;
            const now = Io.nowMs(self);
            self.askForVouch(now);
            const raft = self.raftNode();
            const quiet = now -| raft.last_leader_contact_ms > raft.config.election_timeout_max_ms;
            if (quiet) self.strangers_quiet_ms = now;
            if (quiet and now -| self.stranger_warn_ms >= WARN_INTERVAL_MS) {
                self.stranger_warn_ms = now;
                log.warn("shard {d}: dropping frames from node {d}, which no config this node holds names, with no leader heard for {d} ms; asked the members it knows for their committed config ({d} vouches taken so far)", .{ self.id, frame.source_node, now -| raft.last_leader_contact_ms, self.vouches_taken });
            }
            self.strangerFrame(frame);
        }

        /// Ask every member this node's configs name for its committed
        /// config, at most once a round (an election timeout).
        fn askForVouch(self: *Self, now: u64) void {
            const raft = self.raftNode();
            if (self.ask_ms != 0 and now -| self.ask_ms < raft.config.election_timeout_min_ms) return;
            self.ask_ms = now;
            self.asked_count = 0;
            var latest: [membership.MAX_MEMBERS]u32 = undefined;
            for (raft.memberIds(&latest)) |id| self.noteAsked(id);
            for (raft.committed_config.memberSlice()) |m| self.noteAsked(m.id);
            if (self.asked_count == 0) return;
            self.vouch_asks += 1;
            var buf: [transport.TERM_CHECK_SIZE]u8 = undefined;
            const n = transport.serializeTermCheck(raft.termCheckRequest(), &buf).?;
            for (self.asked[0..self.asked_count]) |id| self.sendRaft(id, .term_check, buf[0..n]);
        }

        fn noteAsked(self: *Self, id: u32) void {
            if (id == self.raftNode().id) return;
            if (std.mem.indexOfScalar(u32, self.asked[0..self.asked_count], id) != null) return;
            self.asked[self.asked_count] = id;
            self.asked_count += 1;
        }

        /// A term check answer as a vouch: only from a member this round
        /// asked (never a vouched node: trust does not pass through one),
        /// not guarded (a lost-log node's history is no evidence), and only
        /// for a committed config newer than this node's and than the
        /// vouch it holds.
        fn takeVouch(self: *Self, resp: raft_node_mod.TermCheckResponse) void {
            if (resp.committed_index == 0 or resp.guarded) return;
            // A config committed in a term later than the responder's own is
            // no history it has: taking it would hold a vouch no honest
            // answer could replace.
            if (resp.committed_term > resp.term) return;
            if (std.mem.indexOfScalar(u32, self.asked[0..self.asked_count], resp.from) == null) return;
            if (!self.isMember(resp.from)) return;
            const raft = self.raftNode();
            const own: Version = .{ .term = raft.committedConfigTerm(), .index = raft.committed_config_index };
            // A committed config whose term this node can't tell would make
            // any answer look newer; take none rather than guess.
            if (raft.committed_config_index > 0 and own.term == 0) return;
            const theirs: Version = .{ .term = resp.committed_term, .index = resp.committed_index };
            if (!theirs.newerThan(own)) return;
            if (self.vouchLive() and !theirs.newerThan(.{ .term = self.vouch.term, .index = self.vouch.index })) return;
            self.vouch = .{ .config = resp.committed, .index = resp.committed_index, .term = resp.committed_term };
            self.vouches_taken += 1;
            log.info("shard {d}: node {d} vouched for the config committed at index {d} (term {d}); hearing its members until this node's log reaches it", .{ self.id, resp.from, resp.committed_index, resp.committed_term });
        }

        const Version = struct {
            term: u64,
            index: u64,
            fn newerThan(a: Version, b: Version) bool {
                return a.term > b.term or (a.term == b.term and a.index > b.index);
            }
        };

        /// One answer per non-member per election timeout. The table
        /// forgets the longest-answered source when full.
        fn mayAnswer(self: *Self, id: u32) bool {
            const now = Io.nowMs(self);
            const window = self.raftNode().config.election_timeout_min_ms;
            var oldest: usize = 0;
            for (&self.answered, 0..) |*a, i| {
                if (a.id == id) {
                    if (now -| a.at_ms < window) return false;
                    a.at_ms = now;
                    return true;
                }
                if (a.at_ms < self.answered[oldest].at_ms) oldest = i;
            }
            self.answered[oldest] = .{ .id = id, .at_ms = now };
            return true;
        }

        /// A member's bug can arrive at heartbeat rate; one line per interval.
        fn frameWarnDue(self: *Self) bool {
            const now = Io.nowMs(self);
            if (now -| self.frame_warn_ms < WARN_INTERVAL_MS) return false;
            self.frame_warn_ms = now;
            return true;
        }

        pub fn badFrame(self: *Self, frame: RaftFrame) void {
            if (self.frameWarnDue()) log.warn("shard {d}: a {s} frame from node {d} did not decode; dropped", .{ self.id, @tagName(frame.msg_type), frame.source_node });
        }

        pub fn impostorFrame(self: *Self, frame: RaftFrame, claimed: u32) void {
            if (self.frameWarnDue()) log.warn("shard {d}: a {s} frame from node {d} speaks for node {d}; dropped", .{ self.id, @tagName(frame.msg_type), frame.source_node, claimed });
        }

        /// What a peer this node sees as a replica may send: everything but a
        /// write to carry to the leader, which a replica answers itself. Votes
        /// and appends are not judged by this node's view of the sender, which
        /// may be stale: a replica promoted by a config this node has not taken
        /// yet must still reach it to elect and to replicate, and term rules
        /// keep both safe.
        fn replicaMaySend(t: transport.MsgType) bool {
            return t != .forward_write;
        }

        fn replicaFrame(self: *Self, frame: RaftFrame) void {
            const now = Io.nowMs(self);
            if (now -| self.replica_frame_warn_ms < WARN_INTERVAL_MS) return;
            self.replica_frame_warn_ms = now;
            log.warn("shard {d}: replica {d} sent a {s} frame; a replica only takes the log, so it was dropped", .{ self.id, frame.source_node, @tagName(frame.msg_type) });
        }

        /// Committed, applied history and the leader's log disagree. The
        /// projections cannot be rebuilt in place, so the node stops taking
        /// part and says what to do; reads still serve what it has.
        pub fn markDiverged(self: *Self, leader: u32, term: u64) void {
            if (self.diverged) return;
            self.diverged = true;
            log.err("shard {d}: node {d} (term {d}) disagrees with history this node committed and applied; this node's data can no longer be trusted and it has stopped taking part in the group. Stop it, move its data directory aside, and start it again with --join <a live member> (not --cluster): it rejoins with no log and votes only once it has caught up and a quorum has confirmed the term", .{ self.id, leader, term });
            self.stopTakingPart(DIVERGED_MESSAGE);
        }

        /// The Raft clock: elections, check-quorum, the leader loop, and a
        /// joiner's request to be let in.
        pub fn tickRaft(self: *Self) void {
            if (!Io.networked(self) or self.diverged) return;
            const raft = self.raftNode();
            const now = Io.nowMs(self);
            if (self.checkRemoved(now)) return;
            if (raft.left_group) {
                raft.left_group = false;
                self.leadershipLost("this node is no longer a voter");
            }
            if (raft.membership_index != self.voters_published_index and raft.latest_config.member_count > 0) {
                self.voters_published_index = raft.membership_index;
                var voters: [membership.MAX_MEMBERS]u32 = undefined;
                Io.setVoters(self, raft.latest_config.voterIds(&voters));
            }
            const r = raft.tick(now);
            // A leader found or won ends the outage the attempt lines count.
            if (raft.role == .leader or raft.leader_id != 0) {
                self.elections_unlogged = 0;
                self.election_warn_ms = 0;
            }
            if (r.start_election) {
                if (raft.startElection()) |req| {
                    // The first attempt says why; a majority that stays away
                    // would otherwise be a line every election timeout.
                    if (now -| self.election_warn_ms >= WARN_INTERVAL_MS) {
                        self.election_warn_ms = now;
                        if (raft.last_leader_contact_ms == 0) {
                            log.info("shard {d}: no leader heard; {s} for term {d} ({d} attempts since the last line)", .{ self.id, if (req.is_pre_vote) "polling" else "standing", req.term, self.elections_unlogged });
                        } else {
                            log.info("shard {d}: no leader heard for {d} ms; {s} for term {d} ({d} attempts since the last line)", .{ self.id, now -| raft.last_leader_contact_ms, if (req.is_pre_vote) "polling" else "standing", req.term, self.elections_unlogged });
                        }
                        self.elections_unlogged = 0;
                    } else {
                        self.elections_unlogged += 1;
                    }
                    self.broadcastVote(req);
                    if (raft.role == .leader) {
                        log.info("shard {d}: elected leader for term {d} as the only member", .{ self.id, raft.current_term });
                        // Alone, the win commits the whole log.
                        if (!self.applyCommitted()) log.err("shard {d}: a committed entry could not be applied", .{self.id});
                    }
                }
            }
            if (r.send_term_check) {
                var buf: [transport.TERM_CHECK_SIZE]u8 = undefined;
                const n = transport.serializeTermCheck(raft.termCheckRequest(), &buf).?;
                var ids: [raft_node_mod.MAX_PEERS + 1]u32 = undefined;
                for (raft.termCheckTargets(&ids)) |peer| self.sendRaft(peer, .term_check, buf[0..n]);
            }
            if (raft.lost_log != .none) self.warnLostLog(now);
            // A member with no leader may have missed the change that
            // admitted it.
            if (raft.self_named and raft.role != .leader and now -| raft.last_leader_contact_ms > raft.config.election_timeout_max_ms) self.askForVouch(now);
            if (r.step_down) self.leadershipLost("no contact with a majority");
            if (raft.role == .leader) self.pump(now);
            self.tickMembership(now);
        }

        /// A guarded node that stays guarded says where it is waiting: with no
        /// leader (a majority lost its data too) or not enough members
        /// answering, it waits forever, and the operator needs the way out.
        fn warnLostLog(self: *Self, now: u64) void {
            if (now -| self.lost_log_warn_ms < WARN_INTERVAL_MS) return;
            // The first line waits one warn interval: catching up usually
            // takes less.
            if (self.lost_log_warn_ms == 0) {
                self.lost_log_warn_ms = now;
                return;
            }
            self.lost_log_warn_ms = now;
            const raft = self.raftNode();
            switch (raft.lost_log) {
                .none => {},
                .catching_up => if (raft.leader_id == 0) {
                    log.warn("shard {d}: lost-log guard: catching up — no leader heard yet; this node votes only once a leader has caught it up and the members have confirmed the term. If a majority lost their data, see flo server inspect / force-members", .{self.id});
                } else {
                    log.warn("shard {d}: lost-log guard: catching up with leader {d} (term {d}, at index {d}); no votes until caught up and confirmed", .{ self.id, raft.leader_id, raft.current_term, raft.log.lastIndex() });
                },
                .confirming => {
                    const st = raft.checkStatus();
                    log.warn("shard {d}: lost-log guard: confirming term {d}; {d} member(s) have answered and {d} more answer(s) are needed. If a majority lost their data, see flo server inspect / force-members", .{ self.id, raft.current_term, st.answers, st.missing });
                },
            }
        }

        fn broadcastVote(self: *Self, req: raft_node_mod.VoteRequest) void {
            var buf: [transport.VOTE_REQ_SIZE]u8 = undefined;
            const n = transport.serializeVoteRequest(req, &buf) orelse return;
            const raft = self.raftNode();
            for (raft.peer_ids[0..raft.peer_count]) |peer| self.sendRaft(peer, .request_vote, buf[0..n]);
        }

        pub fn leadershipLost(self: *Self, why: []const u8) void {
            log.warn("shard {d}: stepped down ({s}); now following at term {d}", .{ self.id, why, self.raftNode().current_term });
            self.stoppedLeading("unavailable: lost leadership before commit — write may still apply");
        }

        /// The leader loop, once per tick and after anything that changes what
        /// a peer should hear: a heartbeat on its interval, otherwise a batch
        /// from the peer's next index when it is behind and nothing is in
        /// flight or what was has gone unanswered for an RPC timeout.
        pub fn pump(self: *Self, now: u64) void {
            const raft = self.raftNode();
            const last = raft.log.lastIndex();
            for (0..raft.peer_count) |i| {
                const p = &raft.peers[i];
                const next = p.next_index;
                const behind = next <= last;
                const unanswered = now -| p.sent_at_ms >= raft.config.rpcTimeoutMs();
                // A member that has stopped answering, heartbeats included, is
                // otherwise silent on the leader. A peer that never answered
                // this leadership counts from when it began, as check-quorum
                // does.
                const heard = @max(p.last_contact_ms, raft.leader_since_ms);
                if (now -| heard > raft.config.election_timeout_max_ms and now -| self.peer_silent_warn_ms[i] >= WARN_INTERVAL_MS) {
                    self.peer_silent_warn_ms[i] = now;
                    log.warn("shard {d}: node {d} has not answered for {d} ms; its next index is {d}", .{ self.id, raft.peer_ids[i], now -| heard, next });
                }
                const want_data = behind and (!p.inflight or unanswered);
                const want_heartbeat = now -| p.heartbeat_at_ms >= raft.config.heartbeat_interval_ms;
                if (!want_data and !want_heartbeat) continue;
                const prev_index = next - 1;
                const known_term: ?u64 = if (prev_index == 0) 0 else raft.log.entryTerm(prev_index);
                // Below where this log starts, even index 0's known term places
                // nothing: the entries after it are gone. A term recorded as 0
                // past index 0 is a snapshot that didn't keep its term, which no
                // follower can match.
                p.below_log = known_term == null or (prev_index > 0 and known_term.? == 0) or next < raft.log.start();
                if (p.below_log) {
                    // No append can reach this peer. Heartbeats still go, with
                    // no term (no entry has term 0), so it does not campaign.
                    if (!want_heartbeat) continue;
                    if (now -| self.peer_batch_warn_ms[i] >= WARN_INTERVAL_MS) {
                        self.peer_batch_warn_ms[i] = now;
                        log.err("shard {d}: no term known for index {d}, below this leader's log; node {d} gets heartbeats but no entries", .{ self.id, prev_index, raft.peer_ids[i] });
                    }
                }
                const prev_term = known_term orelse 0;
                var entries: []const entry_mod.Entry = &.{};
                if (want_data and !p.below_log) {
                    const count = raft.log.getRange(next, self.rpc_entries, self.rpc_arena);
                    if (count > 0) {
                        entries = self.rpc_entries[0..count];
                        p.inflight = true;
                        p.sent_up_to = @max(p.sent_up_to, next + count - 1);
                        p.batch_end = next + count - 1;
                        p.sent_at_ms = now;
                    } else if (now -| self.peer_batch_warn_ms[i] >= WARN_INTERVAL_MS) {
                        // An empty batch here would be sent as a heartbeat and
                        // acked, and the peer would never move past this index.
                        self.peer_batch_warn_ms[i] = now;
                        log.err("shard {d}: the entry at index {d} could not be read for replication to node {d}; no follower can pass it", .{ self.id, next, raft.peer_ids[i] });
                    }
                }
                const req = raft_node_mod.AppendRequest{
                    .term = raft.current_term,
                    .leader_id = raft.id,
                    .prev_log_index = prev_index,
                    .prev_log_term = prev_term,
                    .leader_commit = raft.commit_index,
                    .entries = entries,
                };
                const n = transport.serializeAppendRequest(req, self.rpc_out) orelse {
                    log.err("shard {d}: a batch of {d} entries from index {d} does not fit a frame", .{ self.id, entries.len, next });
                    continue;
                };
                p.heartbeat_at_ms = now;
                self.sendRaft(raft.peer_ids[i], .append_entries, self.rpc_out[0..n]);
            }
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests — the replica alone, through a fake Io that records what it sends
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

const TestIo = struct {
    fn of(r: *TestReplica) *Harness {
        return @alignCast(@fieldParentPtr("replica", r));
    }

    pub fn nowMs(r: *TestReplica) u64 {
        return of(r).now;
    }

    pub fn networked(_: *TestReplica) bool {
        return true;
    }

    pub fn send(r: *TestReplica, peer: u32, msg_type: transport.MsgType, _: u32, payload: []const u8) bool {
        const h = of(r);
        if (msg_type == .append_entries) {
            const hdr = transport.deserializeAppendRequestHeader(payload).?;
            h.appends.append(testing.allocator, .{ .to = peer, .entries = hdr.entry_count }) catch @panic("OOM");
        }
        return true;
    }

    pub fn durableCommits(_: *TestReplica) bool {
        return false;
    }

    pub fn flushForAck(_: *TestReplica) bool {
        return true;
    }

    pub fn setVoters(_: *TestReplica, _: []const u32) void {}

    pub const Host = struct {
        pub fn raftNode(r: *TestReplica) *RaftNode {
            return &of(r).raft;
        }
        pub fn isRemoved(_: *TestReplica) bool {
            return false;
        }
        pub fn checkRemoved(_: *TestReplica, _: u64) bool {
            return false;
        }
        pub fn tickMembership(_: *TestReplica, _: u64) void {}
        pub fn applyCommitted(_: *TestReplica) bool {
            return true;
        }
        pub fn clearRemovalNotices(_: *TestReplica) void {}
        pub fn strangerFrame(_: *TestReplica, _: RaftFrame) void {}
        pub fn handleJoinRequest(_: *TestReplica, _: u32) void {}
        pub fn takeRemovedNotice(_: *TestReplica, _: RaftFrame) void {}
        pub fn runForwardedWrite(_: *TestReplica, _: RaftFrame) void {}
        pub fn takeForwardReply(_: *TestReplica, _: RaftFrame) void {}
        pub fn stopTakingPart(_: *TestReplica, _: []const u8) void {}
        pub fn stoppedLeading(_: *TestReplica, _: []const u8) void {}
    };
};

const TestReplica = Replica(TestIo);

/// Node 1 leading peers 2 and 3; the appends it sends are recorded.
const Harness = struct {
    replica: TestReplica,
    raft: RaftNode,
    now: u64 = 1_000_000,
    appends: std.ArrayListUnmanaged(Append) = .empty,
    entries: [8]entry_mod.Entry = undefined,
    arena: [4096]u8 = undefined,
    out: [8192]u8 = undefined,

    const Append = struct { to: u32, entries: u32 };

    fn init(self: *Harness) !void {
        self.* = .{ .replica = undefined, .raft = try RaftNode.init(testing.allocator, 1, 1, 1 << 16, .{ .durable_commits = false }) };
        self.replica = .{ .id = 0, .rpc_entries = &self.entries, .rpc_arena = &self.arena, .rpc_out = &self.out, .diverged = false };
        self.raft.addPeer(2);
        self.raft.addPeer(3);
        self.raft.observeTime(self.now);
        _ = self.raft.startElectionNow().?;
        _ = self.raft.handleVoteResponse(.{ .term = self.raft.current_term, .vote_granted = true, .from = 2 });
        try testing.expectEqual(raft_node_mod.Role.leader, self.raft.role);
    }

    fn deinit(self: *Harness) void {
        self.appends.deinit(testing.allocator);
        self.raft.deinit();
    }

    /// Appends with entries sent to `to` so far.
    fn batchesTo(self: *const Harness, to: u32) usize {
        var n: usize = 0;
        for (self.appends.items) |a| n += @intFromBool(a.to == to and a.entries > 0);
        return n;
    }

    fn reject(self: *Harness, from: u32, hint: u64) void {
        var buf: [transport.APPEND_RESP_SIZE]u8 = undefined;
        const n = transport.serializeAppendResponse(.{ .term = self.raft.current_term, .success = false, .match_index = 0, .hint_index = hint, .from = from }, &buf).?;
        self.replica.handleRaftFrame(.{ .source_node = from, .group_id = 1, .msg_type = .append_entries_response, .payload = buf[0..n] });
    }

    fn ack(self: *Harness, from: u32, match: u64) void {
        var buf: [transport.APPEND_RESP_SIZE]u8 = undefined;
        const n = transport.serializeAppendResponse(.{ .term = self.raft.current_term, .success = true, .match_index = match, .from = from }, &buf).?;
        self.replica.handleRaftFrame(.{ .source_node = from, .group_id = 1, .msg_type = .append_entries_response, .payload = buf[0..n] });
    }
};

test "replica: a stale ack sends no second batch, and a lost latest ack is resent after the RPC timeout" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    _ = try h.raft.propose(.kv_put, 0, "a");
    h.replica.pump(h.now);
    const first = h.batchesTo(2);
    try testing.expect(first >= 1);
    const sent_through = h.raft.log.lastIndex();

    // More is written; peer 2 is behind and in flight.
    _ = try h.raft.propose(.kv_put, 0, "b");
    // An ack for less than was sent: no new batch to peer 2.
    h.ack(2, sent_through - 1);
    try testing.expectEqual(first, h.batchesTo(2));
    // Again, as a duplicate would.
    h.ack(2, sent_through - 1);
    try testing.expectEqual(first, h.batchesTo(2));

    // The latest ack never comes: after the RPC timeout the batch goes again.
    h.now += h.raft.config.rpcTimeoutMs();
    h.replica.pump(h.now);
    try testing.expectEqual(first + 1, h.batchesTo(2));
}

test "replica: after a rejection sends from earlier, the ack for that shorter batch sends the next at once" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    for (0..30) |_| _ = try h.raft.propose(.kv_put, 0, "e");
    // Sent as far as the log goes, in batches of at most eight.
    var batches = h.batchesTo(2);
    while (h.raft.peers[0].next_index <= h.raft.log.lastIndex()) {
        h.replica.pump(h.now);
        h.ack(2, h.raft.peers[0].batch_end);
        if (h.batchesTo(2) == batches) break;
        batches = h.batchesTo(2);
    }
    const high = h.raft.peers[0].sent_up_to;
    try testing.expect(high >= 30);

    // The follower lost its tail: it rejects, hinting it holds only 2.
    h.reject(2, 2);
    h.replica.pump(h.now);
    const retry = h.raft.peers[0].batch_end;
    try testing.expect(retry < high);
    const sent = h.batchesTo(2);
    // Its ack ends below the old high-water mark, yet it is the latest
    // batch's: the next goes now, not after an RPC timeout.
    h.ack(2, retry);
    try testing.expectEqual(sent + 1, h.batchesTo(2));
}

// The member side: a node that missed a membership change, and its vouches.

const VouchIo = struct {
    fn of(r: *VouchReplica) *VouchHarness {
        return @alignCast(@fieldParentPtr("replica", r));
    }

    pub fn nowMs(r: *VouchReplica) u64 {
        return of(r).now;
    }

    pub fn networked(_: *VouchReplica) bool {
        return true;
    }

    pub fn send(r: *VouchReplica, peer: u32, msg_type: transport.MsgType, _: u32, payload: []const u8) bool {
        const h = of(r);
        h.sent.append(testing.allocator, .{ .to = peer, .msg_type = msg_type, .payload = testing.allocator.dupe(u8, payload) catch @panic("OOM") }) catch @panic("OOM");
        return true;
    }

    pub fn durableCommits(_: *VouchReplica) bool {
        return false;
    }

    pub fn flushForAck(_: *VouchReplica) bool {
        return true;
    }

    pub fn setVoters(_: *VouchReplica, _: []const u32) void {}

    pub const Host = struct {
        pub fn raftNode(r: *VouchReplica) *RaftNode {
            return &of(r).raft;
        }
        pub fn isRemoved(_: *VouchReplica) bool {
            return false;
        }
        pub fn checkRemoved(_: *VouchReplica, _: u64) bool {
            return false;
        }
        pub fn tickMembership(_: *VouchReplica, _: u64) void {}
        pub fn applyCommitted(_: *VouchReplica) bool {
            return true;
        }
        pub fn clearRemovalNotices(_: *VouchReplica) void {}
        pub fn strangerFrame(r: *VouchReplica, _: RaftFrame) void {
            of(r).host_strangers += 1;
        }
        pub fn handleJoinRequest(_: *VouchReplica, _: u32) void {}
        pub fn takeRemovedNotice(_: *VouchReplica, _: RaftFrame) void {}
        pub fn runForwardedWrite(r: *VouchReplica, _: RaftFrame) void {
            of(r).forwards += 1;
        }
        pub fn takeForwardReply(_: *VouchReplica, _: RaftFrame) void {}
        pub fn stopTakingPart(_: *VouchReplica, _: []const u8) void {}
        pub fn stoppedLeading(_: *VouchReplica, _: []const u8) void {}
    };
};

const VouchReplica = Replica(VouchIo);

/// Node 2 of a group whose committed history it holds to index 3, with
/// the config at index 2. Each frame sent is recorded.
const VouchHarness = struct {
    replica: VouchReplica,
    raft: RaftNode,
    now: u64 = 1_000_000,
    sent: std.ArrayListUnmanaged(Sent) = .empty,
    host_strangers: u64 = 0,
    forwards: u64 = 0,
    entries: [8]entry_mod.Entry = undefined,
    arena: [4096]u8 = undefined,
    out: [8192]u8 = undefined,

    const Sent = struct { to: u32, msg_type: transport.MsgType, payload: []u8 };

    fn init(self: *VouchHarness, own: membership.Config) !void {
        self.* = .{ .replica = undefined, .raft = try RaftNode.init(testing.allocator, 2, 1, 1 << 16, .{ .durable_commits = false }) };
        self.replica = .{ .id = 0, .rpc_entries = &self.entries, .rpc_arena = &self.arena, .rpc_out = &self.out, .diverged = false };
        // Index 1 a noop, 2 the config, 3 a noop: all term 1, committed.
        var enc: [membership.MAX_SIZE]u8 = undefined;
        for (1..4) |i| {
            var e = if (i == 2)
                entry_mod.buildEntry(.raft_config, 0, 1, i, i, membership.encode(&own, &enc))
            else
                entry_mod.buildEntry(.raft_noop, 0, 1, i, i, "");
            e.header.crc32c = e.computeCrc();
            _ = try self.raft.log.append(&e);
        }
        self.raft.current_term = 1;
        self.raft.recordConfig(2, 1, &own);
        self.raft.setMembership(&own, 2);
        self.raft.commitMembership(&own, 2);
        self.raft.commit_index = 3;
        self.raft.last_applied = 3;
        self.raft.observeTime(self.now);
    }

    fn deinit(self: *VouchHarness) void {
        for (self.sent.items) |s| testing.allocator.free(s.payload);
        self.sent.deinit(testing.allocator);
        self.raft.deinit();
    }

    fn frame(self: *VouchHarness, from: u32, msg_type: transport.MsgType, payload: []u8) void {
        self.replica.handleRaftFrame(.{ .source_node = from, .group_id = 1, .msg_type = msg_type, .payload = payload });
    }

    fn heartbeat(self: *VouchHarness, from: u32, term: u64) void {
        var buf: [transport.APPEND_REQ_PREFIX]u8 = undefined;
        const n = transport.serializeAppendRequest(.{ .term = term, .leader_id = from, .prev_log_index = 0, .prev_log_term = 0, .leader_commit = 0, .entries = &.{} }, &buf).?;
        self.frame(from, .append_entries, buf[0..n]);
    }

    fn termCheck(self: *VouchHarness, from: u32) void {
        var buf: [transport.TERM_CHECK_SIZE]u8 = undefined;
        const n = transport.serializeTermCheck(.{ .term = 1, .from = from }, &buf).?;
        self.frame(from, .term_check, buf[0..n]);
    }

    /// `from`'s answer: its committed config `cfg` at (term, index).
    fn answer(self: *VouchHarness, from: u32, cfg: membership.Config, term: u64, index: u64, guarded: bool) void {
        self.answerIn(from, cfg, term, term, index, guarded);
    }

    /// `answer`, from a responder in `resp_term`.
    fn answerIn(self: *VouchHarness, from: u32, cfg: membership.Config, resp_term: u64, term: u64, index: u64, guarded: bool) void {
        var buf: [transport.TERM_CHECK_RESP_MAX]u8 = undefined;
        const n = transport.serializeTermCheckResponse(.{ .term = resp_term, .from = from, .guarded = guarded, .config_index = index, .config_term = term, .member_count = 0, .members = undefined, .committed_index = index, .committed_term = term, .committed = cfg }, &buf).?;
        self.frame(from, .term_check_response, buf[0..n]);
    }

    fn sentTo(self: *const VouchHarness, to: u32, msg_type: transport.MsgType) usize {
        var n: usize = 0;
        for (self.sent.items) |s| n += @intFromBool(s.to == to and s.msg_type == msg_type);
        return n;
    }
};

/// Node 2 knows only `{3 voter, 2 replica}`; the group has since admitted
/// node 1 and committed `{1 voter, 2 voter, 3 replica}` at index 20.
fn staleMember(h: *VouchHarness) !void {
    try h.init(membership.Config.ofVoters(&.{3}).withJoiner(2, true));
}

fn groupNow() membership.Config {
    return membership.Config.ofVoters(&.{ 1, 2 }).withJoiner(3, true);
}

test "replica: a member that missed a change asks the members it knows, takes their committed config, and hears its leader" {
    var h: VouchHarness = undefined;
    try staleMember(&h);
    defer h.deinit();

    // Node 1 leads a group node 2 has never seen: dropped, and node 2 asks
    // node 3, the member it knows.
    h.heartbeat(1, 2);
    try testing.expectEqual(@as(u64, 1), h.host_strangers);
    try testing.expectEqual(@as(u32, 0), h.raft.leader_id);
    try testing.expectEqual(@as(usize, 1), h.sentTo(3, .term_check));

    h.answer(3, groupNow(), 2, 20, false);
    try testing.expectEqual(@as(u64, 20), h.replica.vouchedIndex());
    try testing.expectEqual(@as(u64, 1), h.replica.vouches_taken);

    // Now node 1's log and vote requests are heard; nothing else from it.
    h.heartbeat(1, 2);
    try testing.expectEqual(@as(u32, 1), h.raft.leader_id);
    try testing.expectEqual(@as(u64, 2), h.raft.current_term);
    h.frame(1, .forward_write, &.{});
    try testing.expectEqual(@as(u64, 0), h.forwards);
    try testing.expectEqual(@as(u64, 2), h.host_strangers);
}

test "replica: a vouch is taken only from an asked, unguarded member of this node's configs, and only for newer committed history" {
    var h: VouchHarness = undefined;
    try staleMember(&h);
    defer h.deinit();

    // Unasked: no round is out.
    h.answer(3, groupNow(), 2, 20, false);
    try testing.expectEqual(@as(u64, 0), h.replica.vouchedIndex());

    h.heartbeat(1, 2); // a stranger: the round goes out, to node 3
    // A node this one's configs don't name, even with an answer in hand.
    h.answer(7, groupNow(), 2, 20, false);
    try testing.expectEqual(@as(u64, 0), h.replica.vouchedIndex());
    // A guarded member: its history is no evidence.
    h.answer(3, groupNow(), 2, 20, true);
    try testing.expectEqual(@as(u64, 0), h.replica.vouchedIndex());
    // Committed in a term after the responder's own: no history it has.
    h.answerIn(3, groupNow(), 2, 99, 1 << 40, false);
    try testing.expectEqual(@as(u64, 0), h.replica.vouchedIndex());
    // Not newer than this node's own committed config (term 1, index 2).
    h.answer(3, groupNow(), 1, 2, false);
    try testing.expectEqual(@as(u64, 0), h.replica.vouchedIndex());
    h.answer(3, groupNow(), 2, 20, false);
    try testing.expectEqual(@as(u64, 20), h.replica.vouchedIndex());
    // A vouched node never vouches: trust does not pass through it, also
    // in a later round of asks.
    h.now += h.raft.config.election_timeout_min_ms;
    h.heartbeat(9, 3);
    h.answer(1, membership.Config.ofVoters(&.{ 1, 9 }), 3, 30, false);
    try testing.expectEqual(@as(u64, 20), h.replica.vouchedIndex());
    // An older vouch doesn't replace a newer one.
    h.answer(3, groupNow(), 2, 10, false);
    try testing.expectEqual(@as(u64, 20), h.replica.vouchedIndex());
}

test "replica: a vouch ends when this node's committed history reaches it, never on an uncommitted config" {
    var h: VouchHarness = undefined;
    try staleMember(&h);
    defer h.deinit();
    h.heartbeat(1, 2);
    h.answer(3, groupNow(), 2, 20, false);

    // An uncommitted config past the vouch proves nothing: it holds.
    const later = membership.Config.ofVoters(&.{ 3, 4 }).withJoiner(2, true);
    h.raft.setMembership(&later, 50);
    try testing.expectEqual(@as(u64, 20), h.replica.vouchedIndex());

    // Commit reaching it ends it.
    h.raft.commit_index = 20;
    try testing.expectEqual(@as(u64, 0), h.replica.vouchedIndex());
    h.raft.commit_index = 3;
    try testing.expectEqual(@as(u64, 20), h.replica.vouchedIndex());
    // So does a committed config at or past it.
    h.raft.committed_config_index = 20;
    try testing.expectEqual(@as(u64, 0), h.replica.vouchedIndex());
}

test "replica: a member with no leader asks once an election timeout, without a stranger" {
    var h: VouchHarness = undefined;
    try staleMember(&h);
    defer h.deinit();
    h.raft.last_leader_contact_ms = h.now;
    h.replica.tickRaft();
    try testing.expectEqual(@as(usize, 0), h.sentTo(3, .term_check));
    h.now += h.raft.config.election_timeout_max_ms + 1;
    h.replica.tickRaft();
    try testing.expectEqual(@as(usize, 1), h.sentTo(3, .term_check));
    // Not again within the round.
    h.now += 1;
    h.replica.tickRaft();
    try testing.expectEqual(@as(usize, 1), h.sentTo(3, .term_check));
}

test "replica: anyone's term check is answered, a non-member's once per election timeout, and a removed node still hears it is removed" {
    var h: VouchHarness = undefined;
    try h.init(membership.Config.ofVoters(&.{ 2, 3 }).without(5, 77));
    defer h.deinit();

    h.termCheck(9);
    try testing.expectEqual(@as(usize, 1), h.sentTo(9, .term_check_response));
    h.termCheck(9);
    try testing.expectEqual(@as(usize, 1), h.sentTo(9, .term_check_response));
    h.now += h.raft.config.election_timeout_min_ms;
    h.termCheck(9);
    try testing.expectEqual(@as(usize, 2), h.sentTo(9, .term_check_response));
    // Members are answered whenever they ask.
    h.termCheck(3);
    h.termCheck(3);
    try testing.expectEqual(@as(usize, 2), h.sentTo(3, .term_check_response));

    // Node 5 was removed: its term check is answered, its vote request
    // goes to the host, which tells it it was removed.
    h.termCheck(5);
    try testing.expectEqual(@as(usize, 1), h.sentTo(5, .term_check_response));
    const before = h.host_strangers;
    var vbuf: [transport.VOTE_REQ_SIZE]u8 = undefined;
    const vn = transport.serializeVoteRequest(.{ .term = 9, .candidate_id = 5, .last_log_index = 3, .last_log_term = 1, .is_pre_vote = false }, &vbuf).?;
    h.frame(5, .request_vote, vbuf[0..vn]);
    try testing.expectEqual(before + 1, h.host_strangers);
}

test "replica: past sixteen non-members answered, the longest-answered is forgotten" {
    var h: VouchHarness = undefined;
    try h.init(membership.Config.ofVoters(&.{ 2, 3 }));
    defer h.deinit();
    // Sixteen sources, a millisecond apart; source 100 is the oldest.
    for (0..ANSWERED_SOURCES) |i| {
        h.termCheck(@intCast(100 + i));
        h.now += 1;
    }
    // A seventeenth is answered, and takes the oldest's place.
    h.termCheck(200);
    try testing.expectEqual(@as(usize, 1), h.sentTo(200, .term_check_response));
    // Within the window: the forgotten source is answered again, one still
    // remembered is not.
    h.termCheck(100);
    try testing.expectEqual(@as(usize, 2), h.sentTo(100, .term_check_response));
    h.termCheck(105);
    try testing.expectEqual(@as(usize, 1), h.sentTo(105, .term_check_response));
}

test "replica: a vouch admits every member of the vouched config, a replica promoted in flight included" {
    var h: VouchHarness = undefined;
    try staleMember(&h);
    defer h.deinit();
    h.heartbeat(1, 2);
    // Committed, node 1 is still a replica: its promotion, and its win
    // under the config that made it a voter, haven't committed yet.
    const committed = membership.Config.ofVoters(&.{3}).withJoiner(2, true).withJoiner(1, true);
    h.answer(3, committed, 2, 20, false);
    try testing.expectEqual(@as(u64, 20), h.replica.vouchedIndex());
    h.heartbeat(1, 2);
    try testing.expectEqual(@as(u32, 1), h.raft.leader_id);
}
