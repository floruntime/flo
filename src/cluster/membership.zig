//! Membership Management — Join/Leave/Fail State Machine
//!
//! Bridges Gossip (failure detection) with Coordinator (consensus) to manage
//! cluster membership lifecycle:
//!
//!   - **Join**: New node discovered by gossip → propose to Coordinator Raft
//!   - **Leave**: Graceful departure → propose removal, trigger rebalance
//!   - **Fail**: Gossip declares dead → propose removal, trigger rebalance
//!
//! State machine per node:
//!   joining → active → leaving → left
//!                    ↘ failed
//!
//! Integration:
//!   - Gossip tick() returns newly_dead → Membership reacts
//!   - Membership proposes changes through Coordinator (Raft)
//!   - Membership events trigger partition rebalancing
//!   - Forwarder peer pool updated on membership changes

const std = @import("std");
const Allocator = std.mem.Allocator;
const NodeId = @import("../raft/node.zig").NodeId;

// =============================================================================
// Constants
// =============================================================================

/// Default join timeout — how long a joining node waits to become active
pub const DEFAULT_JOIN_TIMEOUT_MS: i64 = 30_000;

/// Default leave drain period — how long a leaving node keeps responding
pub const DEFAULT_LEAVE_DRAIN_MS: i64 = 10_000;

/// Minimum rebalance cooldown — avoid thrashing on rapid membership changes
pub const REBALANCE_COOLDOWN_MS: i64 = 5_000;

/// Maximum tracked membership events for replay/audit
pub const MAX_EVENT_LOG: usize = 256;

// =============================================================================
// Types
// =============================================================================

/// Node membership state — superset of gossip MemberState
pub const NodeState = enum(u8) {
    /// Node has been discovered but not yet confirmed by Raft
    joining = 0,
    /// Node is fully active in the cluster
    active = 1,
    /// Node is gracefully leaving (draining requests)
    leaving = 2,
    /// Node has left the cluster
    left = 3,
    /// Node has been declared failed (unresponsive)
    failed = 4,
};

/// Tracked node in the membership list
pub const TrackedNode = struct {
    node_id: NodeId,
    /// Network address (human-readable, e.g. "10.0.0.1:4444")
    address: [64]u8,
    address_len: u8,
    port: u16,
    shard_count: u8,
    state: NodeState,
    /// When this node entered its current state
    state_changed_ms: i64,
    /// When we last heard from this node (any signal)
    last_seen_ms: i64,
    /// How many partitions are assigned to this node as leader
    partition_count: u32,
    /// Monotonically increasing epoch for state changes
    epoch: u64,
};

/// A membership event — something that happened in the cluster
pub const MembershipEvent = struct {
    event_type: EventType,
    node_id: NodeId,
    timestamp_ms: i64,
    /// Previous state (if transition)
    from_state: NodeState,
    /// New state
    to_state: NodeState,

    pub const EventType = enum(u8) {
        node_discovered = 0,
        node_joined = 1,
        node_activated = 2,
        node_leaving = 3,
        node_left = 4,
        node_failed = 5,
        node_recovered = 6,
        rebalance_triggered = 7,
    };
};

/// Action the caller should take after a membership tick
pub const MembershipAction = struct {
    /// Nodes that should be proposed for addition to Coordinator
    nodes_to_add: u32 = 0,
    /// Nodes that should be proposed for removal from Coordinator
    nodes_to_remove: u32 = 0,
    /// Whether a rebalance should be triggered
    rebalance_needed: bool = false,
    /// Nodes whose forwarder peer entries should be updated
    peers_changed: u32 = 0,
};

// =============================================================================
// Membership
// =============================================================================

pub const Membership = struct {
    allocator: Allocator,

    /// This node's identity
    self_id: NodeId,
    self_state: NodeState,

    /// Tracked nodes: node_id → TrackedNode
    nodes: std.AutoHashMapUnmanaged(NodeId, TrackedNode),

    /// Event log (circular buffer)
    events: [MAX_EVENT_LOG]MembershipEvent,
    event_head: usize,
    event_count: usize,

    /// Pending additions — nodes discovered by gossip, awaiting Raft proposal
    pending_adds: std.ArrayListUnmanaged(NodeId),

    /// Pending removals — nodes declared dead/left, awaiting Raft proposal
    pending_removes: std.ArrayListUnmanaged(NodeId),

    /// Configuration
    join_timeout_ms: i64,
    leave_drain_ms: i64,
    rebalance_cooldown_ms: i64,

    /// Rebalance tracking
    last_rebalance_ms: i64,
    rebalance_pending: bool,

    /// Global epoch — incremented on every membership change
    epoch: u64,

    /// Statistics
    total_joins: u64,
    total_leaves: u64,
    total_failures: u64,
    total_recoveries: u64,
    total_rebalances: u64,

    // ── Construction ────────────────────────────────────────────────────

    pub fn init(allocator: Allocator, self_id: NodeId) Membership {
        return .{
            .allocator = allocator,
            .self_id = self_id,
            .self_state = .joining,
            .nodes = .empty,
            .events = undefined,
            .event_head = 0,
            .event_count = 0,
            .pending_adds = .empty,
            .pending_removes = .empty,
            .join_timeout_ms = DEFAULT_JOIN_TIMEOUT_MS,
            .leave_drain_ms = DEFAULT_LEAVE_DRAIN_MS,
            .rebalance_cooldown_ms = REBALANCE_COOLDOWN_MS,
            .last_rebalance_ms = 0,
            .rebalance_pending = false,
            .epoch = 0,
            .total_joins = 0,
            .total_leaves = 0,
            .total_failures = 0,
            .total_recoveries = 0,
            .total_rebalances = 0,
        };
    }

    pub fn deinit(self: *Membership) void {
        self.nodes.deinit(self.allocator);
        self.pending_adds.deinit(self.allocator);
        self.pending_removes.deinit(self.allocator);
    }

    // ── Self management ─────────────────────────────────────────────────

    /// Bootstrap as single-node cluster — immediately active
    pub fn bootstrapSingle(self: *Membership, now_ms: i64) !void {
        self.self_state = .active;
        try self.recordEvent(.{
            .event_type = .node_activated,
            .node_id = self.self_id,
            .timestamp_ms = now_ms,
            .from_state = .joining,
            .to_state = .active,
        });
    }

    /// Mark self as leaving the cluster
    pub fn initiateLeave(self: *Membership, now_ms: i64) !void {
        const prev = self.self_state;
        self.self_state = .leaving;
        try self.recordEvent(.{
            .event_type = .node_leaving,
            .node_id = self.self_id,
            .timestamp_ms = now_ms,
            .from_state = prev,
            .to_state = .leaving,
        });
    }

    /// Mark self as fully left
    pub fn completeLeave(self: *Membership, now_ms: i64) !void {
        const prev = self.self_state;
        self.self_state = .left;
        try self.recordEvent(.{
            .event_type = .node_left,
            .node_id = self.self_id,
            .timestamp_ms = now_ms,
            .from_state = prev,
            .to_state = .left,
        });
    }

    // ── Node lifecycle ──────────────────────────────────────────────────

    /// A new node has been discovered (via gossip or seed contact).
    /// Adds to tracked nodes and queues for Raft proposal.
    pub fn nodeDiscovered(
        self: *Membership,
        node_id: NodeId,
        address: []const u8,
        port: u16,
        shard_count: u8,
        now_ms: i64,
    ) !void {
        if (node_id == self.self_id) return; // Don't track self

        const result = try self.nodes.getOrPut(self.allocator, node_id);
        if (!result.found_existing) {
            var tracked = TrackedNode{
                .node_id = node_id,
                .address = [_]u8{0} ** 64,
                .address_len = @intCast(@min(address.len, 64)),
                .port = port,
                .shard_count = shard_count,
                .state = .joining,
                .state_changed_ms = now_ms,
                .last_seen_ms = now_ms,
                .partition_count = 0,
                .epoch = self.epoch,
            };
            @memcpy(tracked.address[0..tracked.address_len], address[0..tracked.address_len]);
            result.value_ptr.* = tracked;

            try self.pending_adds.append(self.allocator, node_id);
            self.total_joins += 1;

            try self.recordEvent(.{
                .event_type = .node_discovered,
                .node_id = node_id,
                .timestamp_ms = now_ms,
                .from_state = .joining,
                .to_state = .joining,
            });
        }
    }

    /// A node has been confirmed active by Coordinator Raft.
    pub fn nodeActivated(self: *Membership, node_id: NodeId, now_ms: i64) !void {
        if (self.nodes.getPtr(node_id)) |node| {
            const prev = node.state;
            node.state = .active;
            node.state_changed_ms = now_ms;
            self.epoch += 1;
            node.epoch = self.epoch;
            self.rebalance_pending = true;

            try self.recordEvent(.{
                .event_type = .node_activated,
                .node_id = node_id,
                .timestamp_ms = now_ms,
                .from_state = prev,
                .to_state = .active,
            });
        }
    }

    /// A node has been declared dead by gossip failure detection.
    pub fn nodeFailed(self: *Membership, node_id: NodeId, now_ms: i64) !void {
        if (self.nodes.getPtr(node_id)) |node| {
            const prev = node.state;
            if (prev == .failed or prev == .left) return; // Already handled

            node.state = .failed;
            node.state_changed_ms = now_ms;
            self.epoch += 1;
            node.epoch = self.epoch;
            self.total_failures += 1;
            self.rebalance_pending = true;

            try self.pending_removes.append(self.allocator, node_id);

            try self.recordEvent(.{
                .event_type = .node_failed,
                .node_id = node_id,
                .timestamp_ms = now_ms,
                .from_state = prev,
                .to_state = .failed,
            });
        }
    }

    /// A node has gracefully left the cluster.
    pub fn nodeLeft(self: *Membership, node_id: NodeId, now_ms: i64) !void {
        if (self.nodes.getPtr(node_id)) |node| {
            const prev = node.state;
            node.state = .left;
            node.state_changed_ms = now_ms;
            self.epoch += 1;
            node.epoch = self.epoch;
            self.total_leaves += 1;
            self.rebalance_pending = true;

            try self.pending_removes.append(self.allocator, node_id);

            try self.recordEvent(.{
                .event_type = .node_left,
                .node_id = node_id,
                .timestamp_ms = now_ms,
                .from_state = prev,
                .to_state = .left,
            });
        }
    }

    /// A previously failed node has recovered (gossip reports alive again).
    pub fn nodeRecovered(self: *Membership, node_id: NodeId, now_ms: i64) !void {
        if (self.nodes.getPtr(node_id)) |node| {
            if (node.state != .failed) return; // Only recover from failed

            const prev = node.state;
            node.state = .active;
            node.state_changed_ms = now_ms;
            self.epoch += 1;
            node.epoch = self.epoch;
            self.total_recoveries += 1;
            self.rebalance_pending = true;

            // Remove from pending_removes if queued
            for (self.pending_removes.items, 0..) |id, i| {
                if (id == node_id) {
                    _ = self.pending_removes.swapRemove(i);
                    break;
                }
            }

            try self.recordEvent(.{
                .event_type = .node_recovered,
                .node_id = node_id,
                .timestamp_ms = now_ms,
                .from_state = prev,
                .to_state = .active,
            });
        }
    }

    // ── Tick — membership driver ────────────────────────────────────────

    /// Drive membership logic. Call after gossip tick.
    /// Returns actions the caller should take.
    pub fn tick(self: *Membership, now_ms: i64) !MembershipAction {
        var action = MembershipAction{};

        // Check join timeouts
        try self.checkJoinTimeouts(now_ms);

        // Check leave drain completion
        try self.checkLeaveDrains(now_ms);

        // Report pending work
        action.nodes_to_add = @intCast(self.pending_adds.items.len);
        action.nodes_to_remove = @intCast(self.pending_removes.items.len);

        // Check if rebalance is due
        if (self.rebalance_pending) {
            if (now_ms - self.last_rebalance_ms >= self.rebalance_cooldown_ms) {
                action.rebalance_needed = true;
                self.rebalance_pending = false;
                self.last_rebalance_ms = now_ms;
                self.total_rebalances += 1;

                try self.recordEvent(.{
                    .event_type = .rebalance_triggered,
                    .node_id = self.self_id,
                    .timestamp_ms = now_ms,
                    .from_state = self.self_state,
                    .to_state = self.self_state,
                });
            }
        }

        action.peers_changed = action.nodes_to_add + action.nodes_to_remove;

        return action;
    }

    fn checkJoinTimeouts(self: *Membership, now_ms: i64) !void {
        var iter = self.nodes.iterator();
        while (iter.next()) |entry| {
            const node = entry.value_ptr;
            if (node.state == .joining) {
                if (now_ms - node.state_changed_ms >= self.join_timeout_ms) {
                    // Join timed out — mark as failed
                    node.state = .failed;
                    node.state_changed_ms = now_ms;
                    self.total_failures += 1;

                    try self.recordEvent(.{
                        .event_type = .node_failed,
                        .node_id = node.node_id,
                        .timestamp_ms = now_ms,
                        .from_state = .joining,
                        .to_state = .failed,
                    });
                }
            }
        }
    }

    fn checkLeaveDrains(self: *Membership, now_ms: i64) !void {
        var iter = self.nodes.iterator();
        while (iter.next()) |entry| {
            const node = entry.value_ptr;
            if (node.state == .leaving) {
                if (now_ms - node.state_changed_ms >= self.leave_drain_ms) {
                    node.state = .left;
                    node.state_changed_ms = now_ms;

                    try self.recordEvent(.{
                        .event_type = .node_left,
                        .node_id = node.node_id,
                        .timestamp_ms = now_ms,
                        .from_state = .leaving,
                        .to_state = .left,
                    });
                }
            }
        }
    }

    // ── Queries ─────────────────────────────────────────────────────────

    /// Get a tracked node by ID
    pub fn getNode(self: *const Membership, node_id: NodeId) ?TrackedNode {
        return self.nodes.get(node_id);
    }

    /// Count of active nodes (excluding self)
    pub fn activeCount(self: *const Membership) u32 {
        var count: u32 = 0;
        var iter = self.nodes.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.state == .active) count += 1;
        }
        return count;
    }

    /// Total tracked node count (excluding self)
    pub fn nodeCount(self: *const Membership) u32 {
        return self.nodes.count();
    }

    /// Get all active node IDs (excluding self)
    pub fn activeNodes(self: *const Membership, buf: []NodeId) u32 {
        var count: u32 = 0;
        var iter = self.nodes.iterator();
        while (iter.next()) |entry| {
            if (count >= buf.len) break;
            if (entry.value_ptr.state == .active) {
                buf[count] = entry.key_ptr.*;
                count += 1;
            }
        }
        return count;
    }

    /// Drain pending additions — caller proposes these to Coordinator
    pub fn drainPendingAdds(self: *Membership) []NodeId {
        return self.pending_adds.items;
    }

    /// Clear pending additions after proposing
    pub fn clearPendingAdds(self: *Membership) void {
        self.pending_adds.items.len = 0;
    }

    /// Drain pending removals — caller proposes these to Coordinator
    pub fn drainPendingRemoves(self: *Membership) []NodeId {
        return self.pending_removes.items;
    }

    /// Clear pending removals after proposing
    pub fn clearPendingRemoves(self: *Membership) void {
        self.pending_removes.items.len = 0;
    }

    /// Check if this node is the cluster leader (for proposing changes)
    pub fn isSelfActive(self: *const Membership) bool {
        return self.self_state == .active;
    }

    /// Get membership statistics
    pub fn stats(self: *const Membership) MembershipStats {
        return .{
            .node_count = self.nodeCount(),
            .active_count = self.activeCount(),
            .epoch = self.epoch,
            .total_joins = self.total_joins,
            .total_leaves = self.total_leaves,
            .total_failures = self.total_failures,
            .total_recoveries = self.total_recoveries,
            .total_rebalances = self.total_rebalances,
            .event_count = self.event_count,
        };
    }

    /// Get recent events (up to count)
    pub fn recentEvents(self: *const Membership, buf: []MembershipEvent) u32 {
        const count = @min(buf.len, self.event_count);
        if (count == 0) return 0;

        // Events are in circular buffer, read from oldest to newest
        var read_idx: usize = if (self.event_count >= MAX_EVENT_LOG)
            self.event_head
        else
            0;

        for (0..count) |i| {
            buf[i] = self.events[read_idx % MAX_EVENT_LOG];
            read_idx += 1;
        }
        return @intCast(count);
    }

    // ── Internal ────────────────────────────────────────────────────────

    fn recordEvent(self: *Membership, event: MembershipEvent) !void {
        self.events[self.event_head] = event;
        self.event_head = (self.event_head + 1) % MAX_EVENT_LOG;
        if (self.event_count < MAX_EVENT_LOG) {
            self.event_count += 1;
        }
    }
};

pub const MembershipStats = struct {
    node_count: u32,
    active_count: u32,
    epoch: u64,
    total_joins: u64,
    total_leaves: u64,
    total_failures: u64,
    total_recoveries: u64,
    total_rebalances: u64,
    event_count: usize,
};

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "Membership init and deinit" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try testing.expectEqual(@as(NodeId, 1), m.self_id);
    try testing.expectEqual(NodeState.joining, m.self_state);
    try testing.expectEqual(@as(u32, 0), m.nodeCount());
}

test "Membership bootstrap single node" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try m.bootstrapSingle(1000);
    try testing.expectEqual(NodeState.active, m.self_state);
    try testing.expect(m.isSelfActive());
}

test "Membership node discovery" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 1000);
    try testing.expectEqual(@as(u32, 1), m.nodeCount());

    const node = m.getNode(2).?;
    try testing.expectEqual(NodeState.joining, node.state);
    try testing.expectEqual(@as(u16, 4444), node.port);

    // Pending add queued
    try testing.expectEqual(@as(usize, 1), m.drainPendingAdds().len);
}

test "Membership node activation" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 1000);
    try m.nodeActivated(2, 2000);

    const node = m.getNode(2).?;
    try testing.expectEqual(NodeState.active, node.state);
    try testing.expectEqual(@as(u32, 1), m.activeCount());
    try testing.expect(m.rebalance_pending);
}

test "Membership node failure" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 1000);
    try m.nodeActivated(2, 2000);
    try m.nodeFailed(2, 3000);

    const node = m.getNode(2).?;
    try testing.expectEqual(NodeState.failed, node.state);
    try testing.expectEqual(@as(u64, 1), m.total_failures);

    // Pending removal queued
    try testing.expectEqual(@as(usize, 1), m.drainPendingRemoves().len);
}

test "Membership node recovery" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 1000);
    try m.nodeActivated(2, 2000);
    try m.nodeFailed(2, 3000);
    try m.nodeRecovered(2, 4000);

    const node = m.getNode(2).?;
    try testing.expectEqual(NodeState.active, node.state);
    try testing.expectEqual(@as(u64, 1), m.total_recoveries);

    // Pending removal should be cleared
    try testing.expectEqual(@as(usize, 0), m.drainPendingRemoves().len);
}

test "Membership graceful leave" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 1000);
    try m.nodeActivated(2, 2000);
    try m.nodeLeft(2, 3000);

    const node = m.getNode(2).?;
    try testing.expectEqual(NodeState.left, node.state);
    try testing.expectEqual(@as(u64, 1), m.total_leaves);
}

test "Membership self leave flow" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try m.bootstrapSingle(1000);
    try m.initiateLeave(2000);
    try testing.expectEqual(NodeState.leaving, m.self_state);

    try m.completeLeave(3000);
    try testing.expectEqual(NodeState.left, m.self_state);
}

test "Membership tick rebalance cooldown" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();
    m.rebalance_cooldown_ms = 100;

    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 1000);
    try m.nodeActivated(2, 2000);

    // First tick — rebalance needed (cooldown elapsed since last_rebalance=0)
    const a1 = try m.tick(3000);
    try testing.expect(a1.rebalance_needed);
    try testing.expectEqual(@as(u64, 1), m.total_rebalances);

    // Mark rebalance pending again
    m.rebalance_pending = true;

    // Tick too soon — cooldown not elapsed
    const a2 = try m.tick(3050);
    try testing.expect(!a2.rebalance_needed);

    // Tick after cooldown
    const a3 = try m.tick(3200);
    try testing.expect(a3.rebalance_needed);
}

test "Membership join timeout" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();
    m.join_timeout_ms = 500;

    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 1000);

    // Before timeout
    _ = try m.tick(1400);
    try testing.expectEqual(NodeState.joining, m.getNode(2).?.state);

    // After timeout
    _ = try m.tick(1600);
    try testing.expectEqual(NodeState.failed, m.getNode(2).?.state);
}

test "Membership leave drain completion" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();
    m.leave_drain_ms = 200;

    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 1000);
    try m.nodeActivated(2, 2000);

    // Manually set leaving
    if (m.nodes.getPtr(2)) |node| {
        node.state = .leaving;
        node.state_changed_ms = 3000;
    }

    // Before drain period
    _ = try m.tick(3100);
    try testing.expectEqual(NodeState.leaving, m.getNode(2).?.state);

    // After drain period
    _ = try m.tick(3300);
    try testing.expectEqual(NodeState.left, m.getNode(2).?.state);
}

test "Membership ignore self discovery" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try m.nodeDiscovered(1, "10.0.0.1", 4444, 4, 1000);
    try testing.expectEqual(@as(u32, 0), m.nodeCount());
}

test "Membership activeNodes buffer" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 1000);
    try m.nodeDiscovered(3, "10.0.0.3", 4444, 4, 1000);
    try m.nodeDiscovered(4, "10.0.0.4", 4444, 4, 1000);
    try m.nodeActivated(2, 2000);
    try m.nodeActivated(3, 2000);
    // Node 4 stays joining

    var buf: [10]NodeId = undefined;
    const count = m.activeNodes(&buf);
    try testing.expectEqual(@as(u32, 2), count);
}

test "Membership stats" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 1000);
    try m.nodeActivated(2, 2000);
    try m.nodeFailed(2, 3000);
    try m.nodeRecovered(2, 4000);

    const s = m.stats();
    try testing.expectEqual(@as(u32, 1), s.node_count);
    try testing.expectEqual(@as(u32, 1), s.active_count);
    try testing.expectEqual(@as(u64, 1), s.total_joins);
    try testing.expectEqual(@as(u64, 1), s.total_failures);
    try testing.expectEqual(@as(u64, 1), s.total_recoveries);
}

test "Membership event log" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try m.bootstrapSingle(1000);
    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 2000);
    try m.nodeActivated(2, 3000);

    var buf: [10]MembershipEvent = undefined;
    const count = m.recentEvents(&buf);
    try testing.expectEqual(@as(u32, 3), count);
    try testing.expectEqual(MembershipEvent.EventType.node_activated, buf[0].event_type);
    try testing.expectEqual(MembershipEvent.EventType.node_discovered, buf[1].event_type);
    try testing.expectEqual(MembershipEvent.EventType.node_activated, buf[2].event_type);
}

test "Membership epoch increments on changes" {
    var m = Membership.init(testing.allocator, 1);
    defer m.deinit();

    try testing.expectEqual(@as(u64, 0), m.epoch);

    try m.nodeDiscovered(2, "10.0.0.2", 4444, 4, 1000);
    try m.nodeActivated(2, 2000);
    try testing.expectEqual(@as(u64, 1), m.epoch);

    try m.nodeFailed(2, 3000);
    try testing.expectEqual(@as(u64, 2), m.epoch);

    try m.nodeRecovered(2, 4000);
    try testing.expectEqual(@as(u64, 3), m.epoch);
}
