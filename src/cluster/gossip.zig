//! SWIM Gossip Protocol — Node Discovery & Failure Detection
//!
//! Implements the SWIM protocol (Scalable Weakly-consistent Infection-style
//! process group Membership) for cluster node liveness detection.
//!
//! Design:
//!   - Tick-based: `tick(now_ms)` called from reactor event loop
//!   - Outbound queue: `tick()` populates pending messages, caller sends
//!   - Classic SWIM probe cycle: ping → ping-req (K peers) → suspect → dead
//!   - Incarnation numbers for partition-heal conflict resolution
//!   - Piggyback: membership updates ride every protocol message
//!   - UDP-sized messages (≤1400 bytes)
//!
//! Integration points:
//!   - Coordinator.proposeAddNode() when new node discovered
//!   - Coordinator.proposeRemoveNode() when node declared dead
//!   - Forwarder peer management when membership changes

const std = @import("std");
const Allocator = std.mem.Allocator;
const NodeId = @import("../raft/node.zig").NodeId;

// =============================================================================
// Constants
// =============================================================================

/// Maximum cluster size
pub const MAX_NODES: usize = 64;

/// Maximum piggyback updates per message
pub const MAX_PIGGYBACK: usize = 8;

/// Max message size (fits in UDP MTU minus headers)
pub const MAX_MESSAGE_SIZE: usize = 1400;

/// Number of peers used for indirect ping (ping-req)
pub const INDIRECT_PING_K: usize = 3;

/// Default gossip configuration
pub const DEFAULT_PING_INTERVAL_MS: i64 = 1000;
pub const DEFAULT_PING_TIMEOUT_MS: i64 = 500;
pub const DEFAULT_SUSPECT_TIMEOUT_MS: i64 = 5000;

// =============================================================================
// Node Address
// =============================================================================

/// Network address for a gossip member
pub const NodeAddr = struct {
    ip: [4]u8,
    gossip_port: u16,
    data_port: u16,

    pub fn eql(a: NodeAddr, b: NodeAddr) bool {
        return std.mem.eql(u8, &a.ip, &b.ip) and
            a.gossip_port == b.gossip_port and
            a.data_port == b.data_port;
    }

    pub fn format(self: NodeAddr, buf: []u8) usize {
        return (std.fmt.bufPrint(buf, "{d}.{d}.{d}.{d}:{d}", .{
            self.ip[0], self.ip[1], self.ip[2], self.ip[3], self.gossip_port,
        }) catch buf[0..0]).len;
    }
};

// =============================================================================
// Member State
// =============================================================================

/// SWIM member states
pub const MemberState = enum(u8) {
    alive = 0,
    suspect = 1,
    dead = 2,
    left = 3,
};

/// A cluster member tracked by the gossip protocol
pub const Member = struct {
    node_id: NodeId,
    addr: NodeAddr,
    state: MemberState,
    incarnation: u32,
    /// When this state was last updated
    state_changed_ms: i64,
    /// When we last heard from this member (direct or indirect)
    last_seen_ms: i64,
};

/// A membership update carried as piggyback on protocol messages
pub const MembershipUpdate = struct {
    node_id: NodeId,
    addr: NodeAddr,
    state: MemberState,
    incarnation: u32,
};

// =============================================================================
// Messages
// =============================================================================

/// Gossip message types
pub const MessageTag = enum(u8) {
    ping = 0x01,
    ack = 0x02,
    ping_req = 0x03,
    ping_req_ack = 0x04,
    leave = 0x05,
};

/// A gossip protocol message
pub const Message = struct {
    tag: MessageTag,
    /// Sender's node ID
    sender: NodeId,
    /// Sender's incarnation
    incarnation: u32,
    /// For ping_req: the target to probe on behalf of the sender
    target: NodeId,
    /// Piggyback membership updates
    updates: [MAX_PIGGYBACK]MembershipUpdate,
    update_count: u8,

    pub fn init(tag: MessageTag, sender: NodeId, incarnation: u32) Message {
        return .{
            .tag = tag,
            .sender = sender,
            .incarnation = incarnation,
            .target = 0,
            .updates = undefined,
            .update_count = 0,
        };
    }

    /// Add a piggyback update to this message
    pub fn addUpdate(self: *Message, update: MembershipUpdate) void {
        if (self.update_count < MAX_PIGGYBACK) {
            self.updates[self.update_count] = update;
            self.update_count += 1;
        }
    }
};

/// An outbound message ready to be sent
pub const OutboundMessage = struct {
    target_addr: NodeAddr,
    message: Message,
};

// =============================================================================
// Probe State
// =============================================================================

/// Tracks the current SWIM probe cycle
const ProbeState = enum(u8) {
    idle = 0,
    /// Waiting for direct ping ACK
    awaiting_ack = 1,
    /// Direct ping timed out; sent ping-req to K peers
    awaiting_indirect = 2,
};

const ProbeContext = struct {
    state: ProbeState,
    target: NodeId,
    started_ms: i64,
    /// How many indirect probes (ping-req) we sent
    indirect_count: u8,
    /// How many indirect acks we received
    indirect_acks: u8,
};

// =============================================================================
// Gossip
// =============================================================================

pub const Gossip = struct {
    allocator: Allocator,

    /// This node's identity
    self_id: NodeId,
    self_addr: NodeAddr,
    self_incarnation: u32,

    /// Membership list: node_id → Member
    members: std.AutoHashMapUnmanaged(NodeId, Member),

    /// Round-robin probe order — shuffled list of member node IDs
    probe_order: std.ArrayListUnmanaged(NodeId),
    /// Index into probe_order for next probe target
    probe_index: usize,

    /// Current probe cycle
    probe: ProbeContext,

    /// Outbound message queue — caller drains and sends these
    outbound: std.ArrayListUnmanaged(OutboundMessage),

    /// Pending membership updates to piggyback on outgoing messages
    pending_updates: std.ArrayListUnmanaged(MembershipUpdate),

    /// Configuration
    ping_interval_ms: i64,
    ping_timeout_ms: i64,
    suspect_timeout_ms: i64,

    /// Last tick timestamp
    last_tick_ms: i64,
    /// When last probe cycle started
    last_probe_ms: i64,

    /// Statistics
    pings_sent: u64,
    acks_sent: u64,
    ping_reqs_sent: u64,
    members_suspected: u64,
    members_declared_dead: u64,
    members_discovered: u64,

    // ── Construction ────────────────────────────────────────────────────

    pub fn init(allocator: Allocator, self_id: NodeId, self_addr: NodeAddr) Gossip {
        return .{
            .allocator = allocator,
            .self_id = self_id,
            .self_addr = self_addr,
            .self_incarnation = 1,
            .members = .{},
            .probe_order = .{},
            .probe_index = 0,
            .probe = .{
                .state = .idle,
                .target = 0,
                .started_ms = 0,
                .indirect_count = 0,
                .indirect_acks = 0,
            },
            .outbound = .{},
            .pending_updates = .{},
            .ping_interval_ms = DEFAULT_PING_INTERVAL_MS,
            .ping_timeout_ms = DEFAULT_PING_TIMEOUT_MS,
            .suspect_timeout_ms = DEFAULT_SUSPECT_TIMEOUT_MS,
            .last_tick_ms = 0,
            .last_probe_ms = 0,
            .pings_sent = 0,
            .acks_sent = 0,
            .ping_reqs_sent = 0,
            .members_suspected = 0,
            .members_declared_dead = 0,
            .members_discovered = 0,
        };
    }

    pub fn deinit(self: *Gossip) void {
        self.members.deinit(self.allocator);
        self.probe_order.deinit(self.allocator);
        self.outbound.deinit(self.allocator);
        self.pending_updates.deinit(self.allocator);
    }

    // ── Membership management ───────────────────────────────────────────

    /// Add or update a member in the membership list
    pub fn addMember(self: *Gossip, node_id: NodeId, addr: NodeAddr, now_ms: i64) !void {
        if (node_id == self.self_id) return; // Don't track self

        const result = try self.members.getOrPut(self.allocator, node_id);
        if (!result.found_existing) {
            result.value_ptr.* = .{
                .node_id = node_id,
                .addr = addr,
                .state = .alive,
                .incarnation = 0,
                .state_changed_ms = now_ms,
                .last_seen_ms = now_ms,
            };
            try self.probe_order.append(self.allocator, node_id);
            self.members_discovered += 1;

            // Queue piggyback update about the new member
            try self.queueUpdate(.{
                .node_id = node_id,
                .addr = addr,
                .state = .alive,
                .incarnation = 0,
            });
        }
    }

    /// Remove a member from the membership list
    pub fn removeMember(self: *Gossip, node_id: NodeId) void {
        _ = self.members.remove(node_id);
        // Remove from probe order
        for (self.probe_order.items, 0..) |id, i| {
            if (id == node_id) {
                _ = self.probe_order.swapRemove(i);
                if (self.probe_index > 0 and self.probe_index >= self.probe_order.items.len) {
                    self.probe_index = 0;
                }
                break;
            }
        }
    }

    /// Get a member by ID
    pub fn getMember(self: *const Gossip, node_id: NodeId) ?Member {
        return self.members.get(node_id);
    }

    /// Count of alive members (excluding self)
    pub fn aliveCount(self: *const Gossip) u32 {
        var count: u32 = 0;
        var iter = self.members.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.state == .alive) count += 1;
        }
        return count;
    }

    /// Total member count (excluding self)
    pub fn memberCount(self: *const Gossip) u32 {
        return self.members.count();
    }

    // ── Tick — main gossip driver ───────────────────────────────────────

    /// Drive the gossip protocol forward. Called from reactor event loop.
    /// After calling tick(), drain `outbound` and send each message.
    pub fn tick(self: *Gossip, now_ms: i64) !TickResult {
        self.last_tick_ms = now_ms;
        var result = TickResult{};

        // 1. Check probe cycle timeouts
        try self.checkProbeTimeout(now_ms, &result);

        // 2. Escalate suspects → dead
        try self.escalateSuspects(now_ms, &result);

        // 3. Start new probe cycle if interval elapsed
        if (now_ms - self.last_probe_ms >= self.ping_interval_ms) {
            try self.startProbe(now_ms);
        }

        return result;
    }

    pub const TickResult = struct {
        /// Nodes newly suspected
        newly_suspected: u32 = 0,
        /// Nodes newly declared dead
        newly_dead: u32 = 0,
        /// IDs of nodes newly declared dead (up to MAX_NODES)
        newly_dead_ids: [MAX_NODES]NodeId = [_]NodeId{0} ** MAX_NODES,
    };

    // ── Message handling ────────────────────────────────────────────────

    /// Handle an incoming gossip message from a peer
    pub fn handleMessage(self: *Gossip, msg: Message, now_ms: i64) !void {
        // Always process piggyback updates
        for (msg.updates[0..msg.update_count]) |update| {
            try self.applyUpdate(update, now_ms);
        }

        switch (msg.tag) {
            .ping => try self.handlePing(msg, now_ms),
            .ack => self.handleAck(msg, now_ms),
            .ping_req => try self.handlePingReq(msg, now_ms),
            .ping_req_ack => self.handlePingReqAck(msg, now_ms),
            .leave => try self.handleLeave(msg, now_ms),
        }
    }

    fn handlePing(self: *Gossip, msg: Message, now_ms: i64) !void {
        // Update sender's last seen
        if (self.members.getPtr(msg.sender)) |member| {
            member.last_seen_ms = now_ms;
            if (member.state == .suspect) {
                member.state = .alive;
                member.state_changed_ms = now_ms;
            }
        }

        // Send ACK back
        var ack = Message.init(.ack, self.self_id, self.self_incarnation);
        self.attachPiggyback(&ack);

        if (self.members.get(msg.sender)) |member| {
            try self.outbound.append(self.allocator, .{
                .target_addr = member.addr,
                .message = ack,
            });
            self.acks_sent += 1;
        }
    }

    fn handleAck(self: *Gossip, msg: Message, now_ms: i64) void {
        // Update sender's last seen
        if (self.members.getPtr(msg.sender)) |member| {
            member.last_seen_ms = now_ms;
            if (member.state == .suspect) {
                member.state = .alive;
                member.state_changed_ms = now_ms;
            }
        }

        // Complete current probe if this is the target
        if (self.probe.state == .awaiting_ack and self.probe.target == msg.sender) {
            self.probe.state = .idle;
        }
    }

    fn handlePingReq(self: *Gossip, msg: Message, now_ms: i64) !void {
        _ = now_ms;
        // Ping the target on behalf of the sender
        if (self.members.get(msg.target)) |target_member| {
            var ping = Message.init(.ping, self.self_id, self.self_incarnation);
            ping.target = msg.sender; // So when we get ack, we know who to forward to
            self.attachPiggyback(&ping);

            try self.outbound.append(self.allocator, .{
                .target_addr = target_member.addr,
                .message = ping,
            });
            self.pings_sent += 1;
        }
    }

    fn handlePingReqAck(self: *Gossip, msg: Message, now_ms: i64) void {
        // An indirect ack for our probe target
        if (self.probe.state == .awaiting_indirect and self.probe.target == msg.target) {
            self.probe.indirect_acks += 1;
            // One indirect ack is enough
            self.probe.state = .idle;

            // Mark target as alive
            if (self.members.getPtr(msg.target)) |member| {
                member.last_seen_ms = now_ms;
                if (member.state == .suspect) {
                    member.state = .alive;
                    member.state_changed_ms = now_ms;
                }
            }
        }
    }

    fn handleLeave(self: *Gossip, msg: Message, now_ms: i64) !void {
        if (self.members.getPtr(msg.sender)) |member| {
            member.state = .left;
            member.state_changed_ms = now_ms;
            try self.queueUpdate(.{
                .node_id = msg.sender,
                .addr = member.addr,
                .state = .left,
                .incarnation = msg.incarnation,
            });
        }
    }

    // ── Initiate leave ──────────────────────────────────────────────────

    /// Announce that this node is leaving the cluster gracefully
    pub fn leave(self: *Gossip) !void {
        var msg = Message.init(.leave, self.self_id, self.self_incarnation);
        self.attachPiggyback(&msg);

        // Send leave to all known members
        var iter = self.members.iterator();
        while (iter.next()) |entry| {
            const member = entry.value_ptr;
            if (member.state == .alive or member.state == .suspect) {
                try self.outbound.append(self.allocator, .{
                    .target_addr = member.addr,
                    .message = msg,
                });
            }
        }
    }

    // ── Internal probe logic ────────────────────────────────────────────

    fn startProbe(self: *Gossip, now_ms: i64) !void {
        if (self.probe_order.items.len == 0) {
            self.last_probe_ms = now_ms;
            return;
        }

        // Pick next target in round-robin
        if (self.probe_index >= self.probe_order.items.len) {
            self.probe_index = 0;
            // Could shuffle here for randomness
        }

        const target_id = self.probe_order.items[self.probe_index];
        self.probe_index += 1;

        // Skip dead/left members
        const member = self.members.get(target_id) orelse {
            self.last_probe_ms = now_ms;
            return;
        };
        if (member.state == .dead or member.state == .left) {
            self.last_probe_ms = now_ms;
            return;
        }

        // Send direct ping
        var ping = Message.init(.ping, self.self_id, self.self_incarnation);
        self.attachPiggyback(&ping);

        try self.outbound.append(self.allocator, .{
            .target_addr = member.addr,
            .message = ping,
        });
        self.pings_sent += 1;

        self.probe = .{
            .state = .awaiting_ack,
            .target = target_id,
            .started_ms = now_ms,
            .indirect_count = 0,
            .indirect_acks = 0,
        };

        self.last_probe_ms = now_ms;
    }

    fn checkProbeTimeout(self: *Gossip, now_ms: i64, result: *TickResult) !void {
        switch (self.probe.state) {
            .idle => return,

            .awaiting_ack => {
                if (now_ms - self.probe.started_ms >= self.ping_timeout_ms) {
                    // Direct ping timed out → send indirect probes (ping-req)
                    try self.sendIndirectProbes(now_ms);
                }
            },

            .awaiting_indirect => {
                if (now_ms - self.probe.started_ms >= self.ping_timeout_ms * 2) {
                    // Indirect also timed out → suspect the target
                    if (self.members.getPtr(self.probe.target)) |member| {
                        if (member.state == .alive) {
                            member.state = .suspect;
                            member.state_changed_ms = now_ms;
                            self.members_suspected += 1;
                            result.newly_suspected += 1;

                            try self.queueUpdate(.{
                                .node_id = member.node_id,
                                .addr = member.addr,
                                .state = .suspect,
                                .incarnation = member.incarnation,
                            });
                        }
                    }
                    self.probe.state = .idle;
                }
            },
        }
    }

    fn sendIndirectProbes(self: *Gossip, now_ms: i64) !void {
        _ = now_ms;
        const target_id = self.probe.target;
        const target_member = self.members.get(target_id) orelse {
            self.probe.state = .idle;
            return;
        };

        var sent: u8 = 0;
        var iter = self.members.iterator();
        while (iter.next()) |entry| {
            if (sent >= INDIRECT_PING_K) break;
            const peer = entry.value_ptr;

            // Skip the target and non-alive members
            if (peer.node_id == target_id) continue;
            if (peer.state != .alive) continue;

            var ping_req = Message.init(.ping_req, self.self_id, self.self_incarnation);
            ping_req.target = target_id;
            self.attachPiggyback(&ping_req);

            try self.outbound.append(self.allocator, .{
                .target_addr = peer.addr,
                .message = ping_req,
            });
            self.ping_reqs_sent += 1;
            sent += 1;
        }

        self.probe.state = .awaiting_indirect;
        self.probe.indirect_count = sent;

        // If no peers to ask, directly suspect
        if (sent == 0) {
            if (self.members.getPtr(target_id)) |member| {
                if (member.state == .alive) {
                    member.state = .suspect;
                    member.state_changed_ms = self.last_tick_ms;
                    self.members_suspected += 1;

                    try self.queueUpdate(.{
                        .node_id = target_id,
                        .addr = target_member.addr,
                        .state = .suspect,
                        .incarnation = member.incarnation,
                    });
                }
            }
            self.probe.state = .idle;
        }
    }

    fn escalateSuspects(self: *Gossip, now_ms: i64, result: *TickResult) !void {
        var iter = self.members.iterator();
        while (iter.next()) |entry| {
            const member = entry.value_ptr;
            if (member.state == .suspect) {
                if (now_ms - member.state_changed_ms >= self.suspect_timeout_ms) {
                    member.state = .dead;
                    member.state_changed_ms = now_ms;
                    self.members_declared_dead += 1;
                    if (result.newly_dead < MAX_NODES) {
                        result.newly_dead_ids[result.newly_dead] = member.node_id;
                    }
                    result.newly_dead += 1;

                    try self.queueUpdate(.{
                        .node_id = member.node_id,
                        .addr = member.addr,
                        .state = .dead,
                        .incarnation = member.incarnation,
                    });
                }
            }
        }
    }

    // ── Piggyback & update logic ────────────────────────────────────────

    fn attachPiggyback(self: *Gossip, msg: *Message) void {
        const count = @min(self.pending_updates.items.len, MAX_PIGGYBACK);
        for (self.pending_updates.items[0..count], 0..) |update, i| {
            msg.updates[i] = update;
        }
        msg.update_count = @intCast(count);

        // Remove consumed updates (FIFO)
        if (count > 0) {
            // Shift remaining items
            const remaining = self.pending_updates.items.len - count;
            if (remaining > 0) {
                std.mem.copyForwards(
                    MembershipUpdate,
                    self.pending_updates.items[0..remaining],
                    self.pending_updates.items[count..self.pending_updates.items.len],
                );
            }
            self.pending_updates.items.len = remaining;
        }
    }

    fn applyUpdate(self: *Gossip, update: MembershipUpdate, now_ms: i64) !void {
        if (update.node_id == self.self_id) {
            // Someone is reporting about us — if they say we're suspect/dead,
            // increment our incarnation to refute
            if (update.state == .suspect or update.state == .dead) {
                if (update.incarnation >= self.self_incarnation) {
                    self.self_incarnation = update.incarnation + 1;
                    // Queue alive update with new incarnation
                    try self.queueUpdate(.{
                        .node_id = self.self_id,
                        .addr = self.self_addr,
                        .state = .alive,
                        .incarnation = self.self_incarnation,
                    });
                }
            }
            return;
        }

        const result = try self.members.getOrPut(self.allocator, update.node_id);
        if (!result.found_existing) {
            // New member discovered via piggyback
            result.value_ptr.* = .{
                .node_id = update.node_id,
                .addr = update.addr,
                .state = update.state,
                .incarnation = update.incarnation,
                .state_changed_ms = now_ms,
                .last_seen_ms = now_ms,
            };
            try self.probe_order.append(self.allocator, update.node_id);
            self.members_discovered += 1;
        } else {
            const member = result.value_ptr;
            // Apply update only if incarnation is newer or state is more severe
            if (update.incarnation > member.incarnation or
                (update.incarnation == member.incarnation and
                    @intFromEnum(update.state) > @intFromEnum(member.state)))
            {
                member.state = update.state;
                member.incarnation = update.incarnation;
                member.state_changed_ms = now_ms;
                member.addr = update.addr; // address may have changed
            }
        }
    }

    fn queueUpdate(self: *Gossip, update: MembershipUpdate) !void {
        try self.pending_updates.append(self.allocator, update);
    }

    // ── Queries ─────────────────────────────────────────────────────────

    /// Get all alive member node IDs
    pub fn aliveMembers(self: *const Gossip, buf: []NodeId) u32 {
        var count: u32 = 0;
        var iter = self.members.iterator();
        while (iter.next()) |entry| {
            if (count >= buf.len) break;
            if (entry.value_ptr.state == .alive) {
                buf[count] = entry.key_ptr.*;
                count += 1;
            }
        }
        return count;
    }

    /// Get all dead member node IDs
    pub fn deadMembers(self: *const Gossip, buf: []NodeId) u32 {
        var count: u32 = 0;
        var iter = self.members.iterator();
        while (iter.next()) |entry| {
            if (count >= buf.len) break;
            if (entry.value_ptr.state == .dead) {
                buf[count] = entry.key_ptr.*;
                count += 1;
            }
        }
        return count;
    }

    /// Check if a specific node is alive
    pub fn isAlive(self: *const Gossip, node_id: NodeId) bool {
        if (node_id == self.self_id) return true;
        if (self.members.get(node_id)) |member| {
            return member.state == .alive;
        }
        return false;
    }

    /// Get statistics
    pub fn stats(self: *const Gossip) GossipStats {
        return .{
            .member_count = self.memberCount(),
            .alive_count = self.aliveCount(),
            .pings_sent = self.pings_sent,
            .acks_sent = self.acks_sent,
            .ping_reqs_sent = self.ping_reqs_sent,
            .members_suspected = self.members_suspected,
            .members_declared_dead = self.members_declared_dead,
            .members_discovered = self.members_discovered,
        };
    }

    /// Drain outbound messages — caller sends them and clears the list
    pub fn drainOutbound(self: *Gossip) []OutboundMessage {
        return self.outbound.items;
    }

    /// Clear outbound after sending
    pub fn clearOutbound(self: *Gossip) void {
        self.outbound.items.len = 0;
    }
};

pub const GossipStats = struct {
    member_count: u32,
    alive_count: u32,
    pings_sent: u64,
    acks_sent: u64,
    ping_reqs_sent: u64,
    members_suspected: u64,
    members_declared_dead: u64,
    members_discovered: u64,
};

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

fn testAddr(last_byte: u8, port: u16) NodeAddr {
    return .{
        .ip = .{ 10, 0, 0, last_byte },
        .gossip_port = port,
        .data_port = port -| 100,
    };
}

test "Gossip init and deinit" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();

    try testing.expectEqual(@as(NodeId, 1), g.self_id);
    try testing.expectEqual(@as(u32, 0), g.memberCount());
    try testing.expectEqual(@as(u32, 0), g.aliveCount());
}

test "Gossip add and remove members" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();

    try g.addMember(2, testAddr(2, 5000), 1000);
    try g.addMember(3, testAddr(3, 5000), 1000);
    try testing.expectEqual(@as(u32, 2), g.memberCount());
    try testing.expectEqual(@as(u32, 2), g.aliveCount());

    // Adding self is a no-op
    try g.addMember(1, testAddr(1, 5000), 1000);
    try testing.expectEqual(@as(u32, 2), g.memberCount());

    g.removeMember(2);
    try testing.expectEqual(@as(u32, 1), g.memberCount());
}

test "Gossip isAlive" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();

    try g.addMember(2, testAddr(2, 5000), 1000);
    try testing.expect(g.isAlive(1)); // self always alive
    try testing.expect(g.isAlive(2));
    try testing.expect(!g.isAlive(99)); // unknown node
}

test "Gossip tick sends ping" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();
    g.ping_interval_ms = 100;

    try g.addMember(2, testAddr(2, 5000), 0);
    g.clearOutbound(); // clear the initial piggyback from addMember

    // First tick at t=0 should send a ping (interval=100, last_probe=0)
    _ = try g.tick(100);

    const out = g.drainOutbound();
    try testing.expect(out.len > 0);
    try testing.expectEqual(MessageTag.ping, out[0].message.tag);
    try testing.expectEqual(@as(NodeId, 1), out[0].message.sender);
}

test "Gossip handle ping sends ack" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();

    try g.addMember(2, testAddr(2, 5000), 1000);
    g.clearOutbound();

    // Receive a ping from node 2
    const ping = Message.init(.ping, 2, 1);
    try g.handleMessage(ping, 2000);

    const out = g.drainOutbound();
    try testing.expect(out.len > 0);
    try testing.expectEqual(MessageTag.ack, out[0].message.tag);
}

test "Gossip ack completes probe" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();
    g.ping_interval_ms = 100;

    try g.addMember(2, testAddr(2, 5000), 0);
    g.clearOutbound();

    // Start a probe to node 2 (t=100 >= interval=100)
    _ = try g.tick(100);
    try testing.expectEqual(ProbeState.awaiting_ack, g.probe.state);
    try testing.expectEqual(@as(NodeId, 2), g.probe.target);

    // Receive ack from node 2
    const ack = Message.init(.ack, 2, 1);
    try g.handleMessage(ack, 200);

    try testing.expectEqual(ProbeState.idle, g.probe.state);
}

test "Gossip probe timeout suspects member" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();
    g.ping_interval_ms = 100;
    g.ping_timeout_ms = 50;

    try g.addMember(2, testAddr(2, 5000), 0);
    g.clearOutbound();

    // Start probe at t=100
    _ = try g.tick(100);
    try testing.expectEqual(ProbeState.awaiting_ack, g.probe.state);
    g.clearOutbound();

    // No ack received. Tick at t=160 (past ping_timeout=50)
    // This triggers indirect probes, but with only 1 member (the target),
    // no indirect peers available → directly suspects
    _ = try g.tick(160);

    const member = g.getMember(2).?;
    try testing.expectEqual(MemberState.suspect, member.state);
}

test "Gossip suspect escalates to dead" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();
    g.ping_interval_ms = 10000; // Long interval so ticks don't start new probes
    g.suspect_timeout_ms = 200;

    try g.addMember(2, testAddr(2, 5000), 0);
    g.clearOutbound();

    // Manually set suspect state
    if (g.members.getPtr(2)) |member| {
        member.state = .suspect;
        member.state_changed_ms = 1000;
    }

    // Tick before timeout
    const r1 = try g.tick(1100);
    try testing.expectEqual(@as(u32, 0), r1.newly_dead);

    // Tick after timeout
    const r2 = try g.tick(1300);
    try testing.expectEqual(@as(u32, 1), r2.newly_dead);
    try testing.expectEqual(@as(NodeId, 2), r2.newly_dead_ids[0]);

    const member = g.getMember(2).?;
    try testing.expectEqual(MemberState.dead, member.state);
}

test "Gossip leave broadcasts" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();

    try g.addMember(2, testAddr(2, 5000), 1000);
    try g.addMember(3, testAddr(3, 5000), 1000);
    g.clearOutbound();

    try g.leave();

    const out = g.drainOutbound();
    try testing.expectEqual(@as(usize, 2), out.len);
    for (out) |msg| {
        try testing.expectEqual(MessageTag.leave, msg.message.tag);
    }
}

test "Gossip handle leave marks member left" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();

    try g.addMember(2, testAddr(2, 5000), 1000);
    g.clearOutbound();

    const leave_msg = Message.init(.leave, 2, 1);
    try g.handleMessage(leave_msg, 2000);

    const member = g.getMember(2).?;
    try testing.expectEqual(MemberState.left, member.state);
}

test "Gossip incarnation refutation" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();
    g.clearOutbound();

    try testing.expectEqual(@as(u32, 1), g.self_incarnation);

    // Someone claims we're suspect with incarnation 1
    const update = MembershipUpdate{
        .node_id = 1,
        .addr = testAddr(1, 5000),
        .state = .suspect,
        .incarnation = 1,
    };
    var msg = Message.init(.ping, 2, 1);
    msg.updates[0] = update;
    msg.update_count = 1;

    // Need node 2 to be a member to send ack
    try g.addMember(2, testAddr(2, 5000), 500);
    g.clearOutbound();

    try g.handleMessage(msg, 1000);

    // Should have incremented incarnation
    try testing.expectEqual(@as(u32, 2), g.self_incarnation);
}

test "Gossip piggyback updates ride on messages" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();

    // Add a member (generates a pending update)
    try g.addMember(2, testAddr(2, 5000), 1000);
    try testing.expect(g.pending_updates.items.len > 0);

    g.clearOutbound();

    // Add another member so there's someone to ping
    try g.addMember(3, testAddr(3, 5000), 1000);

    // Tick to send a ping — updates should be piggybacked
    _ = try g.tick(2000);

    const out = g.drainOutbound();
    if (out.len > 0) {
        // The ping message should carry piggyback updates
        try testing.expect(out[0].message.update_count > 0);
    }
}

test "Gossip stats" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();

    try g.addMember(2, testAddr(2, 5000), 1000);

    const s = g.stats();
    try testing.expectEqual(@as(u32, 1), s.member_count);
    try testing.expectEqual(@as(u32, 1), s.alive_count);
    try testing.expectEqual(@as(u64, 1), s.members_discovered);
}

test "Gossip aliveMembers buffer" {
    var g = Gossip.init(testing.allocator, 1, testAddr(1, 5000));
    defer g.deinit();

    try g.addMember(2, testAddr(2, 5000), 1000);
    try g.addMember(3, testAddr(3, 5000), 1000);

    // Mark node 3 as dead
    if (g.members.getPtr(3)) |m| m.state = .dead;

    var buf: [10]NodeId = undefined;
    const alive = g.aliveMembers(&buf);
    try testing.expectEqual(@as(u32, 1), alive);
    try testing.expectEqual(@as(NodeId, 2), buf[0]);
}
