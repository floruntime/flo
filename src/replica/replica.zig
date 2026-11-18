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
const plant = @import("../plant.zig");
const RaftNode = @import("../raft/node.zig").RaftNode;
const raft_node_mod = @import("../raft/node.zig");
const RaftFrame = @import("../raft/raft_queue.zig").Frame;
const transport = @import("../raft/transport.zig");
const membership = @import("../raft/membership.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const raft_log_mod = @import("../raft/log.zig");

/// Steady conditions are said once per interval, not per tick.
pub const WARN_INTERVAL_MS: u64 = 30_000;

/// How long the leader waits before re-reading a peer's catch-up batch whose
/// below-ring read is blocked — the apply loop's back-off, so neither spins.
pub const CATCH_UP_RETRY_MS: u64 = 250;

/// Non-members whose term checks are answered at once, at most once each
/// per election timeout; past this many, the longest-silent is forgotten.
pub const ANSWERED_SOURCES = 16;

/// A committed config a member answered with, newer than this node's own:
/// a member that missed a membership change learns the group from it.
pub const Vouch = @import("../raft/hard_state.zig").Vouch;

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
        /// Why, for `/status`: where the leader's log disagreed.
        diverged_reason_buf: [160]u8 = undefined,
        diverged_reason_len: u8 = 0,
        /// The leader applied the entry this node is halted at, on a build
        /// not known to be this one. Said of that halt only.
        halt_applied_elsewhere: ?AppliedElsewhere = null,
        /// Diverged because the leader applied the entry this node halted at.
        halted_alone: bool = false,
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
        /// The next tick a peer's catch-up batch may be re-read, when its
        /// entries are below the ring and the read is blocked: the leader backs
        /// off like the apply loop rather than re-reading the disk every tick.
        peer_catch_up_retry_ms: [raft_node_mod.MAX_PEERS]u64 = @splat(0),

        /// A member that missed a membership change drops the frames of a
        /// leader admitted since, and never catches up: the entries that
        /// would name that leader come from it. So, when it drops a
        /// stranger's frame or hears no leader, it asks the members its own
        /// configs name for their committed config, and while one is newer
        /// than its own also hears that config's members' appends and vote
        /// requests. Quorum, votes and commit still read only the log.
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
        /// Forwarded writes from a peer this node's config calls a replica,
        /// refused not-run (it is not a voter and can't carry them).
        replica_forwards_refused: u64 = 0,
        vouch_asks: u64 = 0,
        vouches_taken: u64 = 0,
        /// A lone voter whose log names other members asks them before it
        /// campaigns (`loneVoterWaits`): who it asked, when, and whether one
        /// said the group has moved past this node's log.
        lone_asked: [membership.MAX_MEMBERS]u32 = undefined,
        lone_asked_count: u8 = 0,
        lone_asked_ms: u64 = 0,
        lone_refused: bool = false,
        lone_warn_ms: u64 = 0,
        /// When a stranger last sent this node its log in a later term, and
        /// how many rounds of asks in a row no member has answered: together,
        /// a member that sat out changes that replaced every member it knew.
        later_stranger_ms: u64 = 0,
        unanswered_rounds: u32 = 0,

        const Answered = struct { id: u32 = 0, at_ms: u64 = 0 };

        // ─── Disk ───────────────────────────────────────────────────────

        fn durableCommits(self: *Self) bool {
            return Io.durableCommits(self);
        }

        fn flushForAck(self: *Self) bool {
            return Io.flushForAck(self);
        }

        fn asyncAckBlocked(self: *Self) bool {
            return Io.asyncAckBlocked(self);
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

        /// A committed entry cannot be read to apply it: `applyCommitted`
        /// returns false, but the apply loop already logs and surfaces it, so
        /// callers stay quiet rather than log the same block each frame.
        fn readBlocked(self: *Self) bool {
            return Io.Host.readBlocked(self);
        }

        /// Apply is held at an open disk-staging gap under async_flush:
        /// `applyCommitted` returns false by design until the gap fills, so
        /// callers stay quiet rather than log a block every AppendEntries.
        fn gapHolding(self: *Self) bool {
            return Io.Host.gapHolding(self);
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
                    if (frame.msg_type == .term_check_response and std.mem.indexOfScalar(u32, self.lone_asked[0..self.lone_asked_count], frame.source_node) != null) return self.takeLoneAnswer(frame);
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
                        .halt_verdict = hdr.halt_verdict,
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
                    if (hdr.term == raft.current_term) if (raft.halt_at) |h| if (hdr.halt_verdict.about(h)) self.takeHaltVerdict(hdr.leader_id, h, hdr.halt_verdict.build);
                    if (was == .leader and raft.role != .leader) self.leadershipLost("a leader with a newer term spoke");
                    if (raft.leader_id != led_by and raft.leader_id != 0) log.info("shard {d}: following node {d} (term {d})", .{ self.id, raft.leader_id, raft.current_term });
                    // A leader of this term still sends to this node as a member.
                    if (hdr.term == raft.current_term) self.clearRemovalNotices();
                    // An ack means "in my log, on disk" when commits are durable.
                    if (resp.success and count > 0) {
                        if (self.durableCommits()) {
                            if (!self.flushForAck()) return;
                            raft.markDurable(raft.log.lastIndex());
                        } else if (self.asyncAckBlocked()) {
                            // async_flush, but this node's disk has fallen too far
                            // behind: withhold the ack, as the durable path does,
                            // so it stops counting toward commit until a flush
                            // lands. If a majority withhold, commit stalls and
                            // clients time out (ran=unknown), which is correct.
                            return;
                        }
                    }
                    var buf: [transport.APPEND_RESP_SIZE]u8 = undefined;
                    const n = transport.serializeAppendResponse(resp, &buf) orelse return;
                    self.sendRaft(frame.source_node, .append_entries_response, buf[0..n]);
                    if (!self.applyCommitted() and !self.readBlocked() and !self.gapHolding()) log.err("shard {d}: a committed entry could not be applied", .{self.id});
                },
                .append_entries_response => {
                    const resp = transport.deserializeAppendResponse(frame.payload) orelse return self.badFrame(frame);
                    if (resp.from != frame.source_node) return self.impostorFrame(frame, resp.from);
                    const was = raft.role;
                    raft.handleAppendResponse(resp);
                    // Applied first: a leader whose own removal just committed
                    // answers what it parked on that commit before it lets go.
                    if (!self.applyCommitted() and !self.readBlocked() and !self.gapHolding()) log.err("shard {d}: a committed entry could not be applied", .{self.id});
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
                    if (self.isMember(resp.from)) self.unanswered_rounds = 0;
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
            const v = raft.vouch;
            return v.index > 0 and raft.commit_index < v.index and raft.committed_config_index < v.index;
        }

        /// A vouch whose window has ended is cleared on disk too, so a restart
        /// never brings it back. Until then a stored one past its index is
        /// inert anyway: `vouchLive` is judged by the same commit.
        fn retireVouch(self: *Self) void {
            const raft = self.raftNode();
            if (raft.vouch.index == 0 or self.vouchLive()) return;
            raft.vouch = .{};
            _ = raft.persistHardState();
        }

        /// The vouched config's index while it holds, else 0.
        pub fn vouchedIndex(self: *Self) u64 {
            return if (self.vouchLive()) self.raftNode().vouch.index else 0;
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
            return self.vouchLive() and self.raftNode().vouch.config.names(id);
        }

        /// A lone voter whose log names other members asks them, for one
        /// election timeout, before it campaigns; and campaigns no more if
        /// one holds a committed config newer than its own. Its log may be
        /// an old copy (a data directory restored from one), and alone it
        /// would commit on history the group has moved past. With no answer
        /// it campaigns as before: a group shrunk to this node stays up.
        fn loneVoterWaits(self: *Self, now: u64) bool {
            const raft = self.raftNode();
            if (raft.voterPeers() != 0 or !raft.latest_config.isVoter(raft.id)) return false;
            if (self.lone_refused) {
                if (now -| self.lone_warn_ms >= WARN_INTERVAL_MS) {
                    self.lone_warn_ms = now;
                    log.err("shard {d}: this node is its config's only voter, but a member its log names holds a newer committed config: its data may be an old copy. It won't campaign; wipe it and rejoin", .{self.id});
                }
                return true;
            }
            if (self.lone_asked_ms != 0) return now -| self.lone_asked_ms < raft.config.election_timeout_max_ms;
            self.lone_asked_ms = now;
            for (raft.config_records[0..raft.config_record_count]) |rec| {
                for (rec.config.memberSlice()) |m| self.noteLone(m.id);
                for (rec.config.removedSlice()) |r| self.noteLone(r.id);
            }
            for (raft.latest_config.removedSlice()) |r| self.noteLone(r.id);
            if (self.lone_asked_count == 0) return false;
            var buf: [transport.TERM_CHECK_SIZE]u8 = undefined;
            const n = transport.serializeTermCheck(raft.termCheckRequest(), &buf).?;
            for (self.lone_asked[0..self.lone_asked_count]) |id| self.sendRaft(id, .term_check, buf[0..n]);
            return true;
        }

        fn noteLone(self: *Self, id: u32) void {
            if (id == self.raftNode().id) return;
            if (std.mem.indexOfScalar(u32, self.lone_asked[0..self.lone_asked_count], id) != null) return;
            if (self.lone_asked_count == self.lone_asked.len) return;
            self.lone_asked[self.lone_asked_count] = id;
            self.lone_asked_count += 1;
        }

        /// An answer to a lone voter's ask: a committed config newer than
        /// this node's own, from a member that isn't guarded, holds its
        /// campaign for good. A guard of liveness, not integrity: a link
        /// proves only the cluster secret, so the answer may be forged, and
        /// a forged one can only keep this node waiting; nothing in it is
        /// taken.
        fn takeLoneAnswer(self: *Self, frame: RaftFrame) void {
            const resp = transport.deserializeTermCheckResponse(frame.payload) orelse return self.badFrame(frame);
            if (resp.from != frame.source_node) return self.impostorFrame(frame, resp.from);
            if (resp.guarded or resp.committed_index == 0) return;
            const raft = self.raftNode();
            const own: Version = .{ .term = raft.committedConfigTerm(), .index = raft.committed_config_index };
            const theirs: Version = .{ .term = resp.committed_term, .index = resp.committed_index };
            if (theirs.newerThan(own)) self.lone_refused = true;
        }

        fn dropStranger(self: *Self, frame: RaftFrame) void {
            self.strangers_dropped += 1;
            const now = Io.nowMs(self);
            self.askForVouch(now);
            const raft = self.raftNode();
            if (frame.msg_type == .append_entries) if (transport.deserializeAppendRequestHeader(frame.payload)) |hdr| if (hdr.term > raft.current_term) {
                self.later_stranger_ms = now;
            };
            const quiet = now -| raft.last_leader_contact_ms > raft.config.election_timeout_max_ms;
            if (quiet) self.strangers_quiet_ms = now;
            if (quiet and now -| self.stranger_warn_ms >= WARN_INTERVAL_MS) {
                self.stranger_warn_ms = now;
                if (self.membersSilent(now)) {
                    log.err("shard {d}: node {d} leads a later term but no config this node holds names it, and no member this node knows has answered for {d} rounds: it was likely down while every member it knew was replaced. Stop it, move its data directory aside, and start it again with --join naming a live member", .{ self.id, frame.source_node, self.unanswered_rounds });
                } else log.warn("shard {d}: dropping frames from node {d}, which no config this node holds names, with no leader heard for {d} ms; asked the members it knows for their committed config ({d} vouches taken so far)", .{ self.id, frame.source_node, now -| raft.last_leader_contact_ms, self.vouches_taken });
            }
            self.strangerFrame(frame);
        }

        /// Rounds of asks no member answers before a later-term stranger is
        /// taken as the group this node missed.
        pub const SILENT_ROUNDS: u32 = 3;

        /// A stranger leads a later term, and none of the members this node
        /// knows has answered for `SILENT_ROUNDS` rounds: it can't learn
        /// the group from anyone it trusts, and nothing a stranger says is
        /// taken (a link proves only the cluster secret, not which node it
        /// is). The way back is the operator's: wipe and rejoin.
        pub fn membersSilent(self: *Self, now: u64) bool {
            return self.unanswered_rounds >= SILENT_ROUNDS and self.later_stranger_ms != 0 and now -| self.later_stranger_ms < WARN_INTERVAL_MS;
        }

        /// Ask every member this node's configs name for its committed
        /// config, at most once a round (an election timeout).
        fn askForVouch(self: *Self, now: u64) void {
            const raft = self.raftNode();
            if (self.ask_ms != 0 and now -| self.ask_ms < raft.config.election_timeout_min_ms) return;
            self.ask_ms = now;
            self.unanswered_rounds +|= 1;
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
            if (comptime plant.active == .no_vouches) return;
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
            if (self.vouchLive() and !theirs.newerThan(.{ .term = self.raftNode().vouch.term, .index = self.raftNode().vouch.index })) return;
            // On disk with the guard before it admits anyone, so a restart
            // keeps it; not taken if it can't be.
            const was = raft.vouch;
            raft.vouch = .{ .config = resp.committed, .index = resp.committed_index, .term = resp.committed_term };
            if (!raft.persistHardState()) {
                raft.vouch = was;
                return;
            }
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
            // Only a forward_write reaches here (`replicaMaySend` admits every
            // other type). Its forwarder holds a client, so the leader answers
            // it not-run — "you look like a replica here, retry on a voter" —
            // rather than dropping it to the backstop. The reply is terminal:
            // the forwarder delivers it and does not re-forward, so a peer that
            // still believes it's a voter stops once catch-up corrects its
            // config, not by spinning here.
            self.replica_forwards_refused += 1;
            Io.Host.refuseReplicaForward(self, frame);
            const now = Io.nowMs(self);
            if (now -| self.replica_frame_warn_ms < WARN_INTERVAL_MS) return;
            self.replica_frame_warn_ms = now;
            log.warn("shard {d}: replica {d} sent a {s} frame; a replica only takes the log, so it was refused not-run", .{ self.id, frame.source_node, @tagName(frame.msg_type) });
        }

        /// Committed, applied history and the leader's log disagree. The
        /// projections cannot be rebuilt in place, so the node stops taking
        /// part and says what to do; reads still serve what it has.
        pub fn markDiverged(self: *Self, leader: u32, term: u64) void {
            if (self.diverged) return;
            self.diverged = true;
            const at = self.raftNode().conflict_index;
            const why = std.fmt.bufPrint(&self.diverged_reason_buf, "node {d} (term {d}) disagrees with history this node committed and applied, at index {d}", .{ leader, term, at }) catch self.diverged_reason_buf[0..0];
            self.diverged_reason_len = @intCast(why.len);
            log.err("shard {d}: node {d} (term {d}) disagrees with history this node committed and applied, at index {d}; this node's data can no longer be trusted and it has stopped taking part in the group. Stop it, move its data directory aside, and start it again with --join <a live member> (not --cluster): it rejoins with no log and votes only once it has caught up and a quorum has confirmed the term", .{ self.id, leader, term, at });
            self.stopTakingPart(DIVERGED_MESSAGE);
        }

        /// The leader applied `h`, on `build`. A committed repair that
        /// skips `h` means it passed over it, as this node will. Otherwise,
        /// on this node's own build, the halt is this node's alone. A build
        /// with no version can't be told apart from another of its name, so
        /// it is never taken to be this one, nor another.
        fn takeHaltVerdict(self: *Self, leader: u32, h: raft_node_mod.HaltAt, build: u64) void {
            const raft = self.raftNode();
            if (raft.repaired.of(h)) return;
            const known = build != 0 and raft.build != 0;
            if (known and build == raft.build) return self.markHaltedAlone(leader, raft.current_term, h);
            if (self.haltAppliedElsewhere() != null) return;
            self.halt_applied_elsewhere = .{ .halt = h, .builds_known = known };
            if (known) {
                log.warn("shard {d}: leader {d} applied entry {d}, which this node halted at, on another build. Upgrade this node to the leader's build and it applies the entry", .{ self.id, leader, h.index });
            } else {
                log.warn("shard {d}: leader {d} applied entry {d}, which this node halted at; one of the two builds has no version, so which build applies it is unknown. On the same build this node halted alone: move its data directory aside and rejoin. On another, upgrade it", .{ self.id, leader, h.index });
            }
        }

        /// The leader applied the entry this node halted at, on the same
        /// build: the halt is this node's alone, so what it holds is no
        /// other replica's, and only a wipe and rejoin brings it back.
        fn markHaltedAlone(self: *Self, leader: u32, term: u64, h: raft_node_mod.HaltAt) void {
            if (self.diverged) return;
            self.diverged = true;
            self.halted_alone = true;
            const why = std.fmt.bufPrint(&self.diverged_reason_buf, "node {d} (term {d}) applied entry {d}, which this node halted at on the same build: it halted alone", .{ leader, term, h.index }) catch self.diverged_reason_buf[0..0];
            self.diverged_reason_len = @intCast(why.len);
            log.err("shard {d}: node {d} (term {d}) applied entry {d}, which this node halted at on the same build; the halt is this node's alone, so its data can no longer be trusted and it has stopped taking part in the group. Stop it, move its data directory aside, and start it again with --join <a live member> (not --cluster)", .{ self.id, leader, term, h.index });
            self.stopTakingPart(DIVERGED_MESSAGE);
        }

        pub const AppliedElsewhere = struct {
            halt: raft_node_mod.HaltAt,
            /// Both builds have a version, and they differ.
            builds_known: bool,
        };

        /// Whether the leader applied the entry this node is halted at now,
        /// on a build not known to be this one.
        pub fn haltAppliedElsewhere(self: *Self) ?AppliedElsewhere {
            const e = self.halt_applied_elsewhere orelse return null;
            const h = self.raftNode().halt_at orelse return null;
            return if (std.meta.eql(e.halt, h)) e else null;
        }

        /// Why the leader's log made this node diverged; null when it didn't.
        pub fn divergedReason(self: *const Self) ?[]const u8 {
            return if (self.diverged_reason_len == 0) null else self.diverged_reason_buf[0..self.diverged_reason_len];
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
            self.retireVouch();
            const r = raft.tick(now);
            // A leader found or won ends the outage the attempt lines count,
            // and the silence of the members it knows: a leader they elected
            // is speaking to it.
            if (raft.role == .leader or raft.leader_id != 0) {
                self.elections_unlogged = 0;
                self.election_warn_ms = 0;
                self.unanswered_rounds = 0;
            }
            if (r.start_election and !self.loneVoterWaits(now)) {
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
                        if (!self.applyCommitted() and !self.readBlocked() and !self.gapHolding()) log.err("shard {d}: a committed entry could not be applied", .{self.id});
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
            if (r.step_down) self.leadershipLost(if (raft.read_blocked) "cannot apply a committed entry" else "no contact with a majority");
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

        /// This node's entry at `index`, its payload in the RPC scratch: good
        /// until the next pump.
        pub fn ownEntry(self: *Self, index: u64) ?*const entry_mod.Entry {
            return switch (self.readOwnEntry(index)) {
                .found => |e| e,
                .absent, .blocked => null,
            };
        }

        /// `ownEntry`, telling an entry the log doesn't hold from one it
        /// can't read right now.
        pub fn readOwnEntry(self: *Self, index: u64) union(enum) { found: *const entry_mod.Entry, absent, blocked } {
            const r = self.raftNode().log.getRangeBlocking(index, self.rpc_entries[0..1], self.rpc_arena);
            if (r.count == 1) return .{ .found = &self.rpc_entries[0] };
            return if (r.blocked) .blocked else .absent;
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
                const want_data = behind and (!p.inflight or unanswered) and now >= self.peer_catch_up_retry_ms[i];
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
                // Before the batch: the leader's own entry is read into the
                // same scratch.
                const verdict = if (p.halt) |h| raft.haltVerdictFor(i, self.ownEntry(h.index)) else raft_node_mod.HaltVerdict{};
                var entries: []const entry_mod.Entry = &.{};
                if (want_data and !p.below_log) {
                    const rr = raft.log.getRangeBlocking(next, self.rpc_entries, self.rpc_arena);
                    if (rr.count > 0) {
                        entries = self.rpc_entries[0..rr.count];
                        p.inflight = true;
                        p.sent_up_to = @max(p.sent_up_to, next + rr.count - 1);
                        p.batch_end = next + rr.count - 1;
                        p.sent_at_ms = now;
                    } else if (rr.pending) {
                        // Being read off this shard's thread: the read's end
                        // wakes the shard, and the next pump sends it. Only a
                        // heartbeat goes now, at the peer's same next index.
                        if (!want_heartbeat) continue;
                    } else if (rr.blocked) {
                        // The entries are below the ring and can't be read right
                        // now (a resource or IO failure on this leader's disk).
                        // Back off before re-reading, rather than re-list the
                        // segments every tick, and say so rarely.
                        self.peer_catch_up_retry_ms[i] = now + CATCH_UP_RETRY_MS;
                        if (now -| self.peer_batch_warn_ms[i] >= WARN_INTERVAL_MS) {
                            self.peer_batch_warn_ms[i] = now;
                            if (rr.stale) {
                                log.warn("shard {d}: the entries read at index {d} to replicate to node {d} don't match this log's terms; holding and reading them again", .{ self.id, next, raft.peer_ids[i] });
                            } else {
                                log.warn("shard {d}: the entries at index {d} can't be read to replicate to node {d} right now; holding and retrying", .{ self.id, next, raft.peer_ids[i] });
                            }
                        }
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
                    .halt_verdict = verdict,
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
        if (msg_type == .append_entries_response) {
            const resp = transport.deserializeAppendResponse(payload).?;
            h.answers.append(testing.allocator, .{ .success = resp.success, .match = resp.match_index, .flushes_before = h.flushes }) catch @panic("OOM");
        }
        if (msg_type == .append_entries) {
            const hdr = transport.deserializeAppendRequestHeader(payload).?;
            h.appends.append(testing.allocator, .{ .to = peer, .entries = hdr.entry_count, .verdict = hdr.halt_verdict, .prev_index = hdr.prev_log_index, .prev_term = hdr.prev_log_term }) catch @panic("OOM");
        }
        return true;
    }

    pub fn durableCommits(r: *TestReplica) bool {
        return of(r).durable;
    }

    pub fn flushForAck(r: *TestReplica) bool {
        const h = of(r);
        if (!h.flush_ok) return false;
        h.flushes += 1;
        return true;
    }

    pub fn asyncAckBlocked(_: *TestReplica) bool {
        return false;
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
        pub fn readBlocked(_: *TestReplica) bool {
            return false;
        }
        pub fn gapHolding(_: *TestReplica) bool {
            return false;
        }
        pub fn clearRemovalNotices(_: *TestReplica) void {}
        pub fn strangerFrame(_: *TestReplica, _: RaftFrame) void {}
        pub fn refuseReplicaForward(_: *TestReplica, _: RaftFrame) void {}
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
    /// Append answers this node sent, and how many flushes had succeeded
    /// before each.
    answers: std.ArrayListUnmanaged(Answer) = .empty,
    /// Commits are durable: an append is answered only after its flush.
    durable: bool = false,
    flush_ok: bool = true,
    flushes: u32 = 0,
    entries: [8]entry_mod.Entry = undefined,
    arena: [4096]u8 = undefined,
    out: [8192]u8 = undefined,

    const Append = struct { to: u32, entries: u32, verdict: raft_node_mod.HaltVerdict = .{}, prev_index: u64 = 0, prev_term: u64 = 0 };
    const Answer = struct { success: bool, match: u64, flushes_before: u32 };

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
        self.answers.deinit(testing.allocator);
        self.raft.deinit();
    }

    /// The last append sent to `to`.
    fn lastTo(self: *const Harness, to: u32) ?Append {
        var i = self.appends.items.len;
        while (i > 0) {
            i -= 1;
            if (self.appends.items[i].to == to) return self.appends.items[i];
        }
        return null;
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

    /// What the last append to `to` told it about its halt.
    fn lastVerdictTo(self: *const Harness, to: u32) raft_node_mod.HaltVerdict {
        var i = self.appends.items.len;
        while (i > 0) {
            i -= 1;
            if (self.appends.items[i].to == to) return self.appends.items[i].verdict;
        }
        return .{};
    }

    /// An append from `leader` in `term`, carrying `verdict`.
    fn appendFrom(self: *Harness, leader: u32, term: u64, verdict: raft_node_mod.HaltVerdict) void {
        var buf: [transport.APPEND_REQ_PREFIX]u8 = undefined;
        const n = transport.serializeAppendRequest(.{ .term = term, .leader_id = leader, .prev_log_index = 0, .prev_log_term = 0, .leader_commit = 0, .entries = &.{}, .halt_verdict = verdict }, &buf).?;
        self.replica.handleRaftFrame(.{ .source_node = leader, .group_id = 1, .msg_type = .append_entries, .payload = buf[0..n] });
    }

    fn ack(self: *Harness, from: u32, match: u64) void {
        var buf: [transport.APPEND_RESP_SIZE]u8 = undefined;
        const n = transport.serializeAppendResponse(.{ .term = self.raft.current_term, .success = true, .match_index = match, .from = from }, &buf).?;
        self.replica.handleRaftFrame(.{ .source_node = from, .group_id = 1, .msg_type = .append_entries_response, .payload = buf[0..n] });
    }
};

test "replica: a leader that applied the entry a follower halted at tells that follower, and no other" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    h.raft.build = 9;
    const at = (try h.raft.propose(.kv_put, 0, "h")).index;
    var buf: [64]u8 = undefined;
    const own = h.raft.log.getEntryCopy(at, &buf).?;
    const halt: raft_node_mod.HaltAt = .{ .index = at, .term = own.header.term, .crc = own.header.crc32c };
    var rbuf: [transport.APPEND_RESP_SIZE]u8 = undefined;
    const n = transport.serializeAppendResponse(.{ .term = h.raft.current_term, .success = true, .match_index = at, .from = 2, .halt = halt }, &rbuf).?;
    h.replica.handleRaftFrame(.{ .source_node = 2, .group_id = 1, .msg_type = .append_entries_response, .payload = rbuf[0..n] });
    h.raft.last_applied = at;
    h.now += h.raft.config.heartbeat_interval_ms;
    h.replica.pump(h.now);
    try testing.expectEqual(raft_node_mod.HaltVerdict{ .index = at, .term = halt.term, .build = 9 }, h.lastVerdictTo(2));
    try testing.expectEqual(raft_node_mod.HaltVerdict{}, h.lastVerdictTo(3));
}

/// A repair skips every entry.
fn alwaysRepaired(_: ?*anyopaque, _: raft_node_mod.HaltAt) bool {
    return true;
}

test "replica: a follower told its leader applied its halt on its own build is diverged; on another or an unversioned one it says so and stays halted" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    h.raft.build = 9;
    const halt: raft_node_mod.HaltAt = .{ .index = 2, .term = 1, .crc = 5 };
    h.raft.halt_at = halt;
    const term = h.raft.current_term + 1;
    // About another halt, or with no verdict: nothing.
    h.appendFrom(2, term, .{ .index = 3, .term = 1, .build = 9 });
    h.appendFrom(2, term, .{ .index = 2, .term = 2, .build = 9 });
    h.appendFrom(2, term, .{});
    try testing.expect(!h.replica.diverged and h.replica.haltAppliedElsewhere() == null);
    // A leader of an older term says nothing.
    h.appendFrom(3, term - 1, .{ .index = 2, .term = 1, .build = 9 });
    try testing.expect(!h.replica.diverged);

    // Another build, and one with no version on either side: halted still.
    h.appendFrom(2, term, .{ .index = 2, .term = 1, .build = 8 });
    try testing.expect(!h.replica.diverged);
    try testing.expect(h.replica.haltAppliedElsewhere().?.builds_known);
    h.replica.halt_applied_elsewhere = null;
    h.appendFrom(2, term, .{ .index = 2, .term = 1, .build = 0 });
    try testing.expect(!h.replica.diverged);
    try testing.expect(!h.replica.haltAppliedElsewhere().?.builds_known);
    h.replica.halt_applied_elsewhere = null;
    h.raft.build = 0;
    h.appendFrom(2, term, .{ .index = 2, .term = 1, .build = 0 });
    try testing.expect(!h.replica.diverged);
    try testing.expect(!h.replica.haltAppliedElsewhere().?.builds_known);
    h.raft.build = 9;
    // Said of the halt it was about only.
    h.raft.halt_at = .{ .index = 2, .term = 1, .crc = 6 };
    try testing.expect(h.replica.haltAppliedElsewhere() == null);
    h.raft.halt_at = halt;
    h.replica.halt_applied_elsewhere = null;

    // A committed repair skips it: the leader passed over it, as this node will.
    h.raft.repaired = .{ .check = alwaysRepaired };
    h.appendFrom(2, term, .{ .index = 2, .term = 1, .build = 9 });
    h.appendFrom(2, term, .{ .index = 2, .term = 1, .build = 8 });
    try testing.expect(!h.replica.diverged and h.replica.haltAppliedElsewhere() == null);
    h.raft.repaired = .{};

    h.appendFrom(2, term, .{ .index = 2, .term = 1, .build = 9 });
    try testing.expect(h.replica.diverged and h.replica.halted_alone);
    try testing.expect(std.mem.indexOf(u8, h.replica.divergedReason().?, "halted alone") != null);
}

test "replica: under durable commits a follower answers an append only once its flush has succeeded" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    h.durable = true;
    const term = h.raft.current_term + 1;
    const prev = h.raft.log.lastIndex();
    const prev_term = h.raft.log.lastTerm();
    var e = entry_mod.buildEntry(.raft_noop, 0, term, prev + 1, 5, "");
    e.header.crc32c = e.computeCrc();
    var buf: [transport.APPEND_REQ_PREFIX + entry_mod.HEADER_SIZE + 64]u8 = undefined;
    const n = transport.serializeAppendRequest(.{ .term = term, .leader_id = 2, .prev_log_index = prev, .prev_log_term = prev_term, .leader_commit = 0, .entries = (&e)[0..1] }, &buf).?;
    // Its flush fails: no answer, so the leader can't count an entry that
    // isn't on disk here.
    h.flush_ok = false;
    h.replica.handleRaftFrame(.{ .source_node = 2, .group_id = 1, .msg_type = .append_entries, .payload = buf[0..n] });
    try testing.expectEqual(@as(usize, 0), h.answers.items.len);
    // Sent again with the disk back: answered, after the flush.
    h.flush_ok = true;
    h.replica.handleRaftFrame(.{ .source_node = 2, .group_id = 1, .msg_type = .append_entries, .payload = buf[0..n] });
    try testing.expectEqual(@as(usize, 1), h.answers.items.len);
    try testing.expect(h.answers.items[0].success);
    try testing.expectEqual(prev + 1, h.answers.items[0].match);
    try testing.expectEqual(@as(u32, 1), h.answers.items[0].flushes_before);
}

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

    pub fn asyncAckBlocked(_: *VouchReplica) bool {
        return false;
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
        pub fn readBlocked(_: *VouchReplica) bool {
            return false;
        }
        pub fn gapHolding(_: *VouchReplica) bool {
            return false;
        }
        pub fn clearRemovalNotices(_: *VouchReplica) void {}
        pub fn strangerFrame(r: *VouchReplica, _: RaftFrame) void {
            of(r).host_strangers += 1;
        }
        pub fn refuseReplicaForward(_: *VouchReplica, _: RaftFrame) void {}
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

/// A below-the-ring catch-up source that always blocks, counting its reads so
/// a test can tell a paced retry from one that re-reads every tick.
const BlockingCatchUp = struct {
    var calls: usize = 0;
    fn read(_: *anyopaque, _: u64, _: []entry_mod.Entry, _: []u8) raft_log_mod.CatchUpResult {
        calls += 1;
        return .{ .blocked = true };
    }
};

test "replica: a blocked catch-up read for a lagging follower backs off, not re-read every tick" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const raft = &h.raft;
    // Large entries so the early ones fall below the ring, where the catch-up
    // source answers — and here always blocks.
    const big = try testing.allocator.alloc(u8, 32 * 1024);
    defer testing.allocator.free(big);
    @memset(big, 0);
    for (0..6) |_| _ = try raft.propose(.kv_put, 0, big);
    try testing.expect(raft.log.ual.readRangeCopy(1, 2, h.replica.rpc_entries[0..1], &h.arena) == 0); // index 1 is below the ring
    raft.log.catch_up = .{ .ctx = @ptrFromInt(@alignOf(usize)), .read_range = BlockingCatchUp.read };

    // Peer 2 is far behind, its next entry below the ring; peer 3 is caught up,
    // so only peer 2 exercises the catch-up read.
    raft.peers[0].next_index = 1;
    raft.peers[0].inflight = false;
    h.replica.peer_catch_up_retry_ms[0] = 0;
    raft.peers[1].next_index = raft.log.lastIndex() + 1;
    raft.peers[1].inflight = false;

    BlockingCatchUp.calls = 0;
    h.replica.pump(h.now);
    try testing.expectEqual(@as(usize, 1), BlockingCatchUp.calls);
    // Within the back-off, the leader does not re-read the disk.
    h.replica.pump(h.now);
    try testing.expectEqual(@as(usize, 1), BlockingCatchUp.calls);
    // Past the back-off, it reads again — once.
    h.replica.pump(h.now + CATCH_UP_RETRY_MS);
    try testing.expectEqual(@as(usize, 2), BlockingCatchUp.calls);
}

/// A below-the-ring source read off the shard's thread: it answers pending
/// until `done` is set, then entries of `term`, each with a small payload
/// (only index and term are checked before they are sent).
const PendingCatchUp = struct {
    var done: bool = false;
    var asks: usize = 0;
    var term: u64 = 0;
    fn read(_: *anyopaque, start: u64, buf: []entry_mod.Entry, arena: []u8) raft_log_mod.CatchUpResult {
        asks += 1;
        if (!done) return .{ .pending = true };
        const n = @min(buf.len, 2);
        for (buf[0..n], 0..) |*e, i| {
            arena[i] = 'x';
            e.* = entry_mod.buildEntry(.kv_put, 0, term, start + i, 0, arena[i..][0..1]);
        }
        return .{ .count = n };
    }
    fn never(_: *anyopaque, _: u64, _: []entry_mod.Entry, _: []u8) raft_log_mod.CatchUpResult {
        unreachable;
    }
};

test "replica: while a peer's read below the ring is pending it gets heartbeats at its own index, then the batch" {
    var h: Harness = undefined;
    try h.init();
    defer h.deinit();
    const raft = &h.raft;
    const big = try testing.allocator.alloc(u8, 32 * 1024);
    defer testing.allocator.free(big);
    @memset(big, 0);
    for (0..6) |_| _ = try raft.propose(.kv_put, 0, big);
    try testing.expect(raft.log.ual.readRangeCopy(1, 2, h.replica.rpc_entries[0..1], &h.arena) == 0);
    PendingCatchUp.done = false;
    PendingCatchUp.asks = 0;
    PendingCatchUp.term = raft.current_term;
    raft.log.catch_up = .{ .ctx = @ptrFromInt(@alignOf(usize)), .read_range = PendingCatchUp.never, .read_range_async = PendingCatchUp.read };
    raft.peers[0].next_index = 1;
    raft.peers[0].inflight = false;
    raft.peers[0].heartbeat_at_ms = 0;
    raft.peers[1].next_index = raft.log.lastIndex() + 1;

    // Pending: a heartbeat at the peer's own index, no entries, nothing in flight.
    h.replica.pump(h.now);
    try testing.expectEqual(@as(usize, 1), PendingCatchUp.asks);
    try testing.expectEqual(@as(usize, 0), h.batchesTo(2));
    const beat = h.lastTo(2).?;
    try testing.expectEqual(@as(u64, 0), beat.prev_index);
    try testing.expectEqual(@as(u64, 0), beat.prev_term);
    try testing.expect(!raft.peers[0].inflight);
    // Still pending, between heartbeats: asked again, nothing sent.
    const sent = h.appends.items.len;
    h.replica.pump(h.now + 1);
    try testing.expectEqual(@as(usize, 2), PendingCatchUp.asks);
    try testing.expectEqual(sent, h.appends.items.len);
    // Back: the next pump sends the batch from index 1.
    PendingCatchUp.done = true;
    h.replica.pump(h.now + 2);
    try testing.expectEqual(@as(usize, 1), h.batchesTo(2));
    try testing.expectEqual(@as(u64, 0), h.lastTo(2).?.prev_index);
    try testing.expect(raft.peers[0].inflight);
}

test "replica: a member back after one node was replaced asks a member it knows and hears the new leader" {
    // Node 2 was down while the group went {1,2,3} -> {2,3,4}: node 4
    // added, node 1 removed, node 4 leading.
    var h: VouchHarness = undefined;
    try h.init(membership.Config.ofVoters(&.{ 1, 2, 3 }));
    defer h.deinit();
    h.heartbeat(4, 3);
    try testing.expectEqual(@as(u32, 0), h.raft.leader_id);
    try testing.expectEqual(@as(usize, 1), h.sentTo(3, .term_check));
    // Node 1 is gone; node 3 answers.
    h.answer(3, membership.Config.ofVoters(&.{ 2, 3, 4 }), 3, 20, false);
    h.heartbeat(4, 3);
    try testing.expectEqual(@as(u32, 4), h.raft.leader_id);
}

test "replica: a stranger still can't get this node's vote" {
    var h: VouchHarness = undefined;
    try h.init(membership.Config.ofVoters(&.{ 1, 2, 3 }));
    defer h.deinit();
    var buf: [transport.VOTE_REQ_SIZE]u8 = undefined;
    for ([_]bool{ false, true }) |pre| {
        const n = transport.serializeVoteRequest(.{ .term = 9, .candidate_id = 4, .last_log_index = 99, .last_log_term = 9, .is_pre_vote = pre }, &buf).?;
        h.frame(4, .request_vote, buf[0..n]);
    }
    try testing.expectEqual(@as(usize, 0), h.sentTo(4, .request_vote_response));
    try testing.expectEqual(@as(u64, 1), h.raft.current_term);
}

/// Node 2 as the only voter of its config, at index 3, after a config at
/// index 2 that named nodes 1 and 3 too.
fn loneAfterGroup(h: *VouchHarness) !void {
    try h.init(membership.Config.ofVoters(&.{ 1, 2, 3 }));
    const alone = membership.Config.ofVoters(&.{2});
    h.raft.recordConfig(3, 1, &alone);
    h.raft.setMembership(&alone, 3);
    h.raft.commitMembership(&alone, 3);
}

/// Ticks until the first election would be due, and its ask is out.
fn untilAsked(h: *VouchHarness) !void {
    for (0..4) |_| {
        if (h.replica.lone_asked_ms != 0) return;
        electionTimeouts(h, 1);
    }
    return error.TestUnexpectedResult;
}

fn electionTimeouts(h: *VouchHarness, n: usize) void {
    for (0..n) |_| {
        h.now += h.raft.config.election_timeout_max_ms + 1;
        h.replica.tickRaft();
    }
}

test "replica: a lone voter asks the members its log names before it campaigns, and stays down if one has moved past it" {
    var h: VouchHarness = undefined;
    try loneAfterGroup(&h);
    defer h.deinit();
    try untilAsked(&h);
    try testing.expectEqual(@as(usize, 1), h.sentTo(1, .term_check));
    try testing.expectEqual(@as(usize, 1), h.sentTo(3, .term_check));
    try testing.expect(h.raft.role != .leader);
    // Node 1 holds a committed config newer than this node's.
    h.answer(1, membership.Config.ofVoters(&.{ 1, 3, 4 }), 5, 40, false);
    electionTimeouts(&h, 5);
    try testing.expect(h.raft.role != .leader);
}

test "replica: a lone voter none of whose old members answers campaigns after one round" {
    var h: VouchHarness = undefined;
    try loneAfterGroup(&h);
    defer h.deinit();
    try untilAsked(&h);
    try testing.expectEqual(@as(usize, 1), h.sentTo(3, .term_check));
    try testing.expect(h.raft.role != .leader);
    electionTimeouts(&h, 1);
    try testing.expect(h.raft.role == .leader);
}

/// A hard-state sink that records what it was handed, or fails.
const VouchSink = struct {
    fail: bool = false,
    writes: u32 = 0,
    vouch: Vouch = .{},

    fn persist(ctx: *anyopaque, term: u64, voted_for: u32, lost_log: bool, vouch: *const Vouch) bool {
        _ = .{ term, voted_for, lost_log };
        const self: *VouchSink = @ptrCast(@alignCast(ctx));
        if (self.fail) return false;
        self.writes += 1;
        self.vouch = vouch.*;
        return true;
    }
};

test "replica: a vouch is on disk before it admits anyone, and not taken if it can't be" {
    var h: VouchHarness = undefined;
    try staleMember(&h);
    defer h.deinit();
    var sink: VouchSink = .{ .fail = true };
    h.raft.hard_state_sink = .{ .ctx = @ptrCast(&sink), .persist = VouchSink.persist };
    h.heartbeat(1, 2);
    h.answer(3, groupNow(), 2, 20, false);
    try testing.expectEqual(@as(u64, 0), h.replica.vouchedIndex());
    h.heartbeat(1, 2);
    try testing.expectEqual(@as(u32, 0), h.raft.leader_id);
    // Once it can be written, the same answer is taken, and written as given.
    sink.fail = false;
    h.now += h.raft.config.election_timeout_min_ms;
    h.heartbeat(1, 2);
    h.answer(3, groupNow(), 2, 20, false);
    try testing.expectEqual(@as(u64, 20), h.replica.vouchedIndex());
    try testing.expectEqual(@as(u64, 20), sink.vouch.index);
    try testing.expectEqual(@as(u64, 2), sink.vouch.term);
    try testing.expect(sink.vouch.config.isVoter(1) and sink.vouch.config.isVoter(2));
}

test "replica: a vouch ends at the same index after a restart as before, and is cleared on disk when it does" {
    var h: VouchHarness = undefined;
    try staleMember(&h);
    defer h.deinit();
    var sink: VouchSink = .{};
    h.raft.hard_state_sink = .{ .ctx = @ptrCast(&sink), .persist = VouchSink.persist };
    // As a restart restores it: no ask, no answer, the stored vouch.
    h.raft.vouch = .{ .config = groupNow(), .index = 20, .term = 2 };
    h.heartbeat(1, 2);
    try testing.expectEqual(@as(u32, 1), h.raft.leader_id);
    // The window ends where it did: the node's commit reaching the index.
    h.raft.commit_index = 20;
    try testing.expectEqual(@as(u64, 0), h.replica.vouchedIndex());
    h.replica.tickRaft();
    try testing.expectEqual(@as(u64, 0), h.raft.vouch.index);
    try testing.expectEqual(@as(u64, 0), sink.vouch.index);
}

test "replica: a member that hears a later-term stranger while no member it knows answers says it must be wiped and rejoined" {
    var h: VouchHarness = undefined;
    try h.init(membership.Config.ofVoters(&.{ 1, 2, 3 }));
    defer h.deinit();
    for (0..VouchReplica.SILENT_ROUNDS) |_| {
        h.heartbeat(4, 9);
        h.now += h.raft.config.election_timeout_max_ms + 1;
    }
    try testing.expect(h.replica.membersSilent(h.now));
    try testing.expectEqual(@as(u32, 0), h.raft.leader_id);
    // A member answering means it can still learn the group the usual way.
    h.answer(3, membership.Config.ofVoters(&.{ 1, 2, 3 }), 1, 2, false);
    try testing.expect(!h.replica.membersSilent(h.now));
    // A stranger at this node's term, not a later one, isn't the sign.
    var g: VouchHarness = undefined;
    try g.init(membership.Config.ofVoters(&.{ 1, 2, 3 }));
    defer g.deinit();
    for (0..VouchReplica.SILENT_ROUNDS + 1) |_| {
        g.heartbeat(4, 1);
        g.now += g.raft.config.election_timeout_max_ms + 1;
    }
    try testing.expect(!g.replica.membersSilent(g.now));
}

test "replica: a vouch restored with its index at or below this node's commit at boot is not live, and is cleared" {
    var h: VouchHarness = undefined;
    try staleMember(&h);
    defer h.deinit();
    var sink: VouchSink = .{};
    h.raft.hard_state_sink = .{ .ctx = @ptrCast(&sink), .persist = VouchSink.persist };
    // Stored, then the node applied past it before the restart; nothing
    // restarts its window.
    h.raft.commit_index = 25;
    h.raft.vouch = .{ .config = groupNow(), .index = 20, .term = 2 };
    try testing.expectEqual(@as(u64, 0), h.replica.vouchedIndex());
    h.heartbeat(1, 2);
    try testing.expectEqual(@as(u32, 0), h.raft.leader_id);
    h.replica.tickRaft();
    try testing.expectEqual(@as(u64, 0), h.raft.vouch.index);
    try testing.expectEqual(@as(u32, 1), sink.writes);
}

test "replica: a member that hears its leader again is no longer taken as having sat out a replace, whatever a stranger sends later" {
    var h: VouchHarness = undefined;
    try h.init(membership.Config.ofVoters(&.{ 1, 2, 3 }));
    defer h.deinit();
    // An old blip: rounds of asks no member answered.
    for (0..VouchReplica.SILENT_ROUNDS + 1) |_| {
        h.now += h.raft.config.election_timeout_max_ms + 1;
        h.replica.tickRaft();
    }
    try testing.expect(h.replica.unanswered_rounds >= VouchReplica.SILENT_ROUNDS);
    // Its leader, a member, is heard again.
    h.heartbeat(1, 2);
    try testing.expectEqual(@as(u32, 1), h.raft.leader_id);
    h.replica.tickRaft();
    // One later-term stranger isn't the sign on its own.
    h.heartbeat(4, 9);
    try testing.expect(!h.replica.membersSilent(h.now));
}
