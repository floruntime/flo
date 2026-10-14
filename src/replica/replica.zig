//! Replica — this node's side of one Raft group: the frames it takes from
//! its peers, its clock (elections, check-quorum, the leader loop), and
//! what a leader sends each peer.
//!
//! `Io` is its clock, and its host: a shard, which owns the log, the state
//! machine and the clients. Until applying entries and the membership steps
//! move in here, the replica reaches the shard for them through `Io.Host`.

const std = @import("std");
const log = @import("stdx").log;
const RaftNode = @import("../raft/node.zig").RaftNode;
const raft_node_mod = @import("../raft/node.zig");
const RaftNetwork = @import("../raft/network.zig").RaftNetwork;
const RaftFrame = @import("../raft/raft_queue.zig").Frame;
const transport = @import("../raft/transport.zig");
const membership = @import("../raft/membership.zig");
const entry_mod = @import("../storage/ual/entry.zig");

/// Steady conditions are said once per interval, not per tick.
pub const WARN_INTERVAL_MS: u64 = 30_000;

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

        // ─── The host ────────────────────────────────────────────────────

        fn raftNode(self: *Self) *RaftNode {
            return Io.Host.raftNode(self);
        }

        fn network(self: *Self) ?*RaftNetwork {
            return Io.Host.network(self);
        }

        fn durableCommits(self: *Self) bool {
            return Io.Host.durableCommits(self);
        }

        fn isRemoved(self: *Self) bool {
            return Io.Host.isRemoved(self);
        }

        fn checkRemoved(self: *Self, now: u64) bool {
            return Io.Host.checkRemoved(self, now);
        }

        fn flushForAck(self: *Self) bool {
            return Io.Host.flushForAck(self);
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
            const rn = self.network() orelse return false;
            if (rn.sendTo(peer, msg_type, self.id, payload)) return true;
            log.warn("shard {d}: could not queue a {s} for node {d}", .{ self.id, @tagName(msg_type), peer });
            return false;
        }

        /// One Raft frame from a proven peer. The link proves who sent it;
        /// the ids inside must agree, or one member could speak for another.
        /// Once this node knows the membership, only members speak for the
        /// group; a node with the secret but no seat can still ask for one.
        pub fn handleRaftFrame(self: *Self, frame: RaftFrame) void {
            const raft = self.raftNode();
            if (self.diverged or self.isRemoved()) return;
            if (frame.msg_type != .join_request and (raft.peer_count > 0 or raft.self_named)) {
                // Members of the latest config, and of the committed one until
                // the change commits: a leader removing itself leads the group
                // that drops it until the group has taken that.
                var ids: [membership.MAX_MEMBERS]u32 = undefined;
                if (!membership.names(raft.memberIds(&ids), frame.source_node) and !raft.committed_config.names(frame.source_node)) return self.strangerFrame(frame);
                // A joiner keeps its guard while it catches up: the configs it
                // passes through on the way are history, and may call it a
                // replica it no longer is.
                if (raft.isReplicaPeer(frame.source_node) and !replicaMaySend(frame.msg_type)) return self.replicaFrame(frame);
            }
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
                    if (raft.role == .leader) self.pump(Io.nowMs());
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
                            self.pump(Io.nowMs());
                        },
                    }
                },
                .term_check => {
                    const req = transport.deserializeTermCheck(frame.payload) orelse return self.badFrame(frame);
                    if (req.from != frame.source_node) return self.impostorFrame(frame, req.from);
                    var buf: [transport.TERM_CHECK_RESP_MAX]u8 = undefined;
                    const n = transport.serializeTermCheckResponse(raft.handleTermCheck(req), &buf) orelse return;
                    self.sendRaft(frame.source_node, .term_check_response, buf[0..n]);
                },
                .term_check_response => {
                    const resp = transport.deserializeTermCheckResponse(frame.payload) orelse return self.badFrame(frame);
                    if (resp.from != frame.source_node) return self.impostorFrame(frame, resp.from);
                    const was = raft.lost_log;
                    raft.handleTermCheckResponse(resp);
                    if (was != .none and raft.lost_log == .none) log.info("shard {d}: lost-log guard done; term {d} confirmed, voting from term {d} on", .{ self.id, raft.current_term, raft.current_term + 1 });
                },
                .join_request => self.handleJoinRequest(frame.source_node),
                .removed_notice => self.takeRemovedNotice(frame),
                .forward_write => self.runForwardedWrite(frame),
                .forward_reply => self.takeForwardReply(frame),
                .install_snapshot, .peer_info, .hello, .hello_back, .verify, .welcome => {},
            }
        }

        /// A member's bug can arrive at heartbeat rate; one line per interval.
        fn frameWarnDue(self: *Self) bool {
            const now = Io.nowMs();
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
            const now = Io.nowMs();
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
            if (self.network() == null or self.diverged) return;
            const raft = self.raftNode();
            const now = Io.nowMs();
            if (self.checkRemoved(now)) return;
            if (raft.left_group) {
                raft.left_group = false;
                self.leadershipLost("this node is no longer a voter");
            }
            if (raft.membership_index != self.voters_published_index and raft.latest_config.member_count > 0) {
                self.voters_published_index = raft.membership_index;
                var voters: [membership.MAX_MEMBERS]u32 = undefined;
                if (self.network()) |rn| rn.setVoters(raft.latest_config.voterIds(&voters));
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
