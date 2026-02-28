//! Cross-Node Request Forwarder
//!
//! When a request arrives for a partition owned by another node, the
//! Forwarder handles routing it to the correct remote node and returning
//! the response to the original client.
//!
//! Flow:
//!   1. Local Dispatcher determines partition is remote (via PartitionTable)
//!   2. Forwarder serializes the request and sends to the owning node
//!   3. Remote node processes the request locally
//!   4. Remote node sends the response back
//!   5. Forwarder delivers the response to the waiting client connection
//!
//! The Forwarder maintains a pool of TCP connections to peer nodes,
//! with request/response correlation via request IDs.
//!
//! Design notes:
//!   - One Forwarder per shard (no cross-shard sharing)
//!   - Connections are established lazily on first forward
//!   - Requests are correlated by (request_id, source_shard) tuple
//!   - Timeout-based cleanup for orphaned pending requests

const std = @import("std");
const Allocator = std.mem.Allocator;
const NodeId = @import("../raft/node.zig").NodeId;

// =============================================================================
// Constants
// =============================================================================

/// Maximum pending forwarded requests per shard
pub const MAX_PENDING_REQUESTS: usize = 4096;

/// Default forward timeout in milliseconds
pub const DEFAULT_TIMEOUT_MS: i64 = 5000;

/// Maximum peer connections maintained
pub const MAX_PEERS: usize = 64;

/// Reconnect backoff base in milliseconds
pub const RECONNECT_BACKOFF_MS: i64 = 100;

/// Maximum reconnect backoff in milliseconds
pub const MAX_RECONNECT_BACKOFF_MS: i64 = 10000;

// =============================================================================
// Types
// =============================================================================

/// Identifies a pending forwarded request
pub const RequestKey = struct {
    request_id: u64,
    source_shard: u16,
};

/// State of a forwarded request
pub const PendingRequest = struct {
    /// Original request ID from the client
    request_id: u64,
    /// Shard that originated the forward
    source_shard: u16,
    /// Connection to respond to (opaque handle)
    connection_id: u64,
    /// When the forward was initiated (ms)
    forwarded_at_ms: i64,
    /// Timeout for this request (ms)
    timeout_ms: i64,
    /// Target node
    target_node: NodeId,
    /// Payload size (for stats)
    payload_size: u32,
};

/// Information about a peer node connection
pub const PeerConnection = struct {
    node_id: NodeId,
    address: [64]u8,
    address_len: u8,
    port: u16,
    /// Connection state
    state: PeerState,
    /// Number of in-flight requests to this peer
    inflight: u32,
    /// Total requests forwarded to this peer
    total_forwarded: u64,
    /// Total responses received from this peer
    total_responses: u64,
    /// Total errors forwarding to this peer
    total_errors: u64,
    /// Last successful communication timestamp (ms)
    last_success_ms: i64,
    /// Last error timestamp (ms)
    last_error_ms: i64,
    /// Current reconnect backoff (ms)
    reconnect_backoff_ms: i64,

    pub fn init(node_id: NodeId, address: []const u8, port: u16) PeerConnection {
        var conn = PeerConnection{
            .node_id = node_id,
            .address = [_]u8{0} ** 64,
            .address_len = @intCast(@min(address.len, 64)),
            .port = port,
            .state = .disconnected,
            .inflight = 0,
            .total_forwarded = 0,
            .total_responses = 0,
            .total_errors = 0,
            .last_success_ms = 0,
            .last_error_ms = 0,
            .reconnect_backoff_ms = RECONNECT_BACKOFF_MS,
        };
        @memcpy(conn.address[0..conn.address_len], address[0..conn.address_len]);
        return conn;
    }
};

pub const PeerState = enum(u8) {
    disconnected = 0,
    connecting = 1,
    connected = 2,
    draining = 3,
};

/// Result of a forward attempt
pub const ForwardResult = union(enum) {
    /// Request was queued for forwarding
    queued: QueuedInfo,
    /// No peer connection available for the target node
    no_route: void,
    /// Too many pending requests
    overloaded: void,
    /// Target node is this node (shouldn't forward to self)
    local: void,

    pub const QueuedInfo = struct {
        request_id: u64,
        target_node: NodeId,
    };
};

/// Stats about the forwarder
pub const ForwarderStats = struct {
    pending_count: u32,
    peer_count: u32,
    total_forwarded: u64,
    total_responses: u64,
    total_timeouts: u64,
    total_errors: u64,
};

// =============================================================================
// Forwarder
// =============================================================================

/// Per-shard request forwarder for cross-node routing.
pub const Forwarder = struct {
    allocator: Allocator,

    /// This node's ID
    local_node_id: NodeId,

    /// This shard's ID
    local_shard_id: u16,

    /// Pending requests awaiting responses: request_id → PendingRequest
    pending: std.AutoHashMapUnmanaged(u64, PendingRequest),

    /// Peer connections: node_id → PeerConnection
    peers: std.AutoHashMapUnmanaged(NodeId, PeerConnection),

    /// Statistics
    total_forwarded: u64,
    total_responses: u64,
    total_timeouts: u64,
    total_errors: u64,

    /// Forward timeout (ms)
    timeout_ms: i64,

    // ── Construction ────────────────────────────────────────────────────

    pub fn init(allocator: Allocator, local_node_id: NodeId, local_shard_id: u16) Forwarder {
        return .{
            .allocator = allocator,
            .local_node_id = local_node_id,
            .local_shard_id = local_shard_id,
            .pending = .{},
            .peers = .{},
            .total_forwarded = 0,
            .total_responses = 0,
            .total_timeouts = 0,
            .total_errors = 0,
            .timeout_ms = DEFAULT_TIMEOUT_MS,
        };
    }

    pub fn deinit(self: *Forwarder) void {
        self.pending.deinit(self.allocator);
        self.peers.deinit(self.allocator);
    }

    // ── Peer management ─────────────────────────────────────────────────

    /// Register a peer node that we may need to forward to
    pub fn addPeer(self: *Forwarder, node_id: NodeId, address: []const u8, port: u16) !void {
        if (node_id == self.local_node_id) return; // Don't add self
        try self.peers.put(self.allocator, node_id, PeerConnection.init(node_id, address, port));
    }

    /// Remove a peer node (e.g., node left cluster)
    pub fn removePeer(self: *Forwarder, node_id: NodeId) void {
        _ = self.peers.remove(node_id);
    }

    /// Update peer connection state
    pub fn setPeerState(self: *Forwarder, node_id: NodeId, state: PeerState) void {
        if (self.peers.getPtr(node_id)) |peer| {
            peer.state = state;
        }
    }

    /// Mark a peer as connected
    pub fn peerConnected(self: *Forwarder, node_id: NodeId, now_ms: i64) void {
        if (self.peers.getPtr(node_id)) |peer| {
            peer.state = .connected;
            peer.last_success_ms = now_ms;
            peer.reconnect_backoff_ms = RECONNECT_BACKOFF_MS;
        }
    }

    /// Mark a peer as disconnected with backoff
    pub fn peerDisconnected(self: *Forwarder, node_id: NodeId, now_ms: i64) void {
        if (self.peers.getPtr(node_id)) |peer| {
            peer.state = .disconnected;
            peer.last_error_ms = now_ms;
            // Exponential backoff
            peer.reconnect_backoff_ms = @min(
                peer.reconnect_backoff_ms * 2,
                MAX_RECONNECT_BACKOFF_MS,
            );
        }
    }

    /// Get peer info
    pub fn getPeer(self: *const Forwarder, node_id: NodeId) ?PeerConnection {
        return self.peers.get(node_id);
    }

    // ── Forward requests ────────────────────────────────────────────────

    /// Queue a request for forwarding to a remote node.
    /// Returns the forward result indicating success or failure reason.
    pub fn forward(
        self: *Forwarder,
        target_node: NodeId,
        request_id: u64,
        connection_id: u64,
        payload_size: u32,
        now_ms: i64,
    ) !ForwardResult {
        // Don't forward to self
        if (target_node == self.local_node_id) {
            return .{ .local = {} };
        }

        // Check we have a peer entry
        const peer = self.peers.getPtr(target_node) orelse return .{ .no_route = {} };

        // Check pending limit
        if (self.pending.count() >= MAX_PENDING_REQUESTS) {
            return .{ .overloaded = {} };
        }

        // Track the pending request
        try self.pending.put(self.allocator, request_id, .{
            .request_id = request_id,
            .source_shard = self.local_shard_id,
            .connection_id = connection_id,
            .forwarded_at_ms = now_ms,
            .timeout_ms = self.timeout_ms,
            .target_node = target_node,
            .payload_size = payload_size,
        });

        peer.inflight += 1;
        peer.total_forwarded += 1;
        self.total_forwarded += 1;

        return .{ .queued = .{
            .request_id = request_id,
            .target_node = target_node,
        } };
    }

    /// Complete a forwarded request when the response arrives.
    /// Returns the pending request info (for routing response to client).
    pub fn complete(self: *Forwarder, request_id: u64, now_ms: i64) ?PendingRequest {
        const pending = self.pending.get(request_id) orelse return null;

        // Update peer stats
        if (self.peers.getPtr(pending.target_node)) |peer| {
            if (peer.inflight > 0) peer.inflight -= 1;
            peer.total_responses += 1;
            peer.last_success_ms = now_ms;
        }

        _ = self.pending.remove(request_id);
        self.total_responses += 1;

        return pending;
    }

    /// Fail a forwarded request (error from remote node or connection failure).
    pub fn fail(self: *Forwarder, request_id: u64, now_ms: i64) ?PendingRequest {
        const pending = self.pending.get(request_id) orelse return null;

        if (self.peers.getPtr(pending.target_node)) |peer| {
            if (peer.inflight > 0) peer.inflight -= 1;
            peer.total_errors += 1;
            peer.last_error_ms = now_ms;
        }

        _ = self.pending.remove(request_id);
        self.total_errors += 1;

        return pending;
    }

    // ── Timeout sweep ───────────────────────────────────────────────────

    /// Sweep for timed-out pending requests. Returns the number of timed-out requests.
    /// Caller should send error responses to the affected connections.
    pub fn sweepTimeouts(self: *Forwarder, now_ms: i64, timed_out: *std.ArrayListUnmanaged(PendingRequest)) !u32 {
        var count: u32 = 0;
        var to_remove: std.ArrayListUnmanaged(u64) = .{};
        defer to_remove.deinit(self.allocator);

        var iter = self.pending.iterator();
        while (iter.next()) |entry| {
            const req = entry.value_ptr;
            if (now_ms - req.forwarded_at_ms > req.timeout_ms) {
                try to_remove.append(self.allocator, entry.key_ptr.*);
                try timed_out.append(self.allocator, req.*);
                count += 1;
            }
        }

        for (to_remove.items) |rid| {
            if (self.pending.get(rid)) |req| {
                if (self.peers.getPtr(req.target_node)) |peer| {
                    if (peer.inflight > 0) peer.inflight -= 1;
                }
            }
            _ = self.pending.remove(rid);
            self.total_timeouts += 1;
        }

        return count;
    }

    // ── Queries ─────────────────────────────────────────────────────────

    /// Get forwarder statistics
    pub fn stats(self: *const Forwarder) ForwarderStats {
        return .{
            .pending_count = self.pending.count(),
            .peer_count = self.peers.count(),
            .total_forwarded = self.total_forwarded,
            .total_responses = self.total_responses,
            .total_timeouts = self.total_timeouts,
            .total_errors = self.total_errors,
        };
    }

    /// Number of pending requests
    pub fn pendingCount(self: *const Forwarder) u32 {
        return self.pending.count();
    }

    /// Number of registered peers
    pub fn peerCount(self: *const Forwarder) u32 {
        return self.peers.count();
    }

    /// Check if any peer needs reconnection
    pub fn peersNeedingReconnect(self: *const Forwarder, now_ms: i64) u32 {
        var count: u32 = 0;
        var iter = self.peers.iterator();
        while (iter.next()) |entry| {
            const peer = entry.value_ptr;
            if (peer.state == .disconnected) {
                if (now_ms - peer.last_error_ms >= peer.reconnect_backoff_ms) {
                    count += 1;
                }
            }
        }
        return count;
    }
};

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "Forwarder init and deinit" {
    var fwd = Forwarder.init(testing.allocator, 1, 0);
    defer fwd.deinit();

    try testing.expectEqual(@as(NodeId, 1), fwd.local_node_id);
    try testing.expectEqual(@as(u16, 0), fwd.local_shard_id);
    try testing.expectEqual(@as(u32, 0), fwd.pendingCount());
    try testing.expectEqual(@as(u32, 0), fwd.peerCount());
}

test "Forwarder add and remove peers" {
    var fwd = Forwarder.init(testing.allocator, 1, 0);
    defer fwd.deinit();

    try fwd.addPeer(2, "10.0.0.2", 4444);
    try fwd.addPeer(3, "10.0.0.3", 4444);
    try testing.expectEqual(@as(u32, 2), fwd.peerCount());

    // Adding self is a no-op
    try fwd.addPeer(1, "10.0.0.1", 4444);
    try testing.expectEqual(@as(u32, 2), fwd.peerCount());

    fwd.removePeer(2);
    try testing.expectEqual(@as(u32, 1), fwd.peerCount());
}

test "Forwarder forward request" {
    var fwd = Forwarder.init(testing.allocator, 1, 0);
    defer fwd.deinit();

    try fwd.addPeer(2, "10.0.0.2", 4444);

    const result = try fwd.forward(2, 42, 100, 256, 1000);
    switch (result) {
        .queued => |info| {
            try testing.expectEqual(@as(u64, 42), info.request_id);
            try testing.expectEqual(@as(NodeId, 2), info.target_node);
        },
        else => return error.TestUnexpectedResult,
    }

    try testing.expectEqual(@as(u32, 1), fwd.pendingCount());
    try testing.expectEqual(@as(u64, 1), fwd.total_forwarded);

    // Peer should have 1 inflight
    const peer = fwd.getPeer(2).?;
    try testing.expectEqual(@as(u32, 1), peer.inflight);
}

test "Forwarder forward to self returns local" {
    var fwd = Forwarder.init(testing.allocator, 1, 0);
    defer fwd.deinit();

    const result = try fwd.forward(1, 42, 100, 256, 1000);
    switch (result) {
        .local => {},
        else => return error.TestUnexpectedResult,
    }
}

test "Forwarder forward to unknown node returns no_route" {
    var fwd = Forwarder.init(testing.allocator, 1, 0);
    defer fwd.deinit();

    const result = try fwd.forward(99, 42, 100, 256, 1000);
    switch (result) {
        .no_route => {},
        else => return error.TestUnexpectedResult,
    }
}

test "Forwarder complete request" {
    var fwd = Forwarder.init(testing.allocator, 1, 0);
    defer fwd.deinit();

    try fwd.addPeer(2, "10.0.0.2", 4444);
    _ = try fwd.forward(2, 42, 100, 256, 1000);

    const pending = fwd.complete(42, 2000);
    try testing.expect(pending != null);
    try testing.expectEqual(@as(u64, 42), pending.?.request_id);
    try testing.expectEqual(@as(u64, 100), pending.?.connection_id);

    try testing.expectEqual(@as(u32, 0), fwd.pendingCount());
    try testing.expectEqual(@as(u64, 1), fwd.total_responses);
}

test "Forwarder fail request" {
    var fwd = Forwarder.init(testing.allocator, 1, 0);
    defer fwd.deinit();

    try fwd.addPeer(2, "10.0.0.2", 4444);
    _ = try fwd.forward(2, 42, 100, 256, 1000);

    const pending = fwd.fail(42, 2000);
    try testing.expect(pending != null);
    try testing.expectEqual(@as(u32, 0), fwd.pendingCount());
    try testing.expectEqual(@as(u64, 1), fwd.total_errors);
}

test "Forwarder timeout sweep" {
    var fwd = Forwarder.init(testing.allocator, 1, 0);
    defer fwd.deinit();
    fwd.timeout_ms = 1000;

    try fwd.addPeer(2, "10.0.0.2", 4444);

    // Forward 3 requests at t=1000
    _ = try fwd.forward(2, 1, 100, 64, 1000);
    _ = try fwd.forward(2, 2, 101, 64, 1000);
    _ = try fwd.forward(2, 3, 102, 64, 1000);

    try testing.expectEqual(@as(u32, 3), fwd.pendingCount());

    // Sweep at t=1500 — nothing timed out yet
    var timed_out: std.ArrayListUnmanaged(PendingRequest) = .{};
    defer timed_out.deinit(testing.allocator);

    const count1 = try fwd.sweepTimeouts(1500, &timed_out);
    try testing.expectEqual(@as(u32, 0), count1);

    // Sweep at t=2500 — all 3 timed out (1000ms timeout)
    const count2 = try fwd.sweepTimeouts(2500, &timed_out);
    try testing.expectEqual(@as(u32, 3), count2);
    try testing.expectEqual(@as(u32, 0), fwd.pendingCount());
    try testing.expectEqual(@as(u64, 3), fwd.total_timeouts);
}

test "Forwarder peer connection state" {
    var fwd = Forwarder.init(testing.allocator, 1, 0);
    defer fwd.deinit();

    try fwd.addPeer(2, "10.0.0.2", 4444);

    // Initially disconnected
    try testing.expectEqual(PeerState.disconnected, fwd.getPeer(2).?.state);

    // Mark connected
    fwd.peerConnected(2, 1000);
    try testing.expectEqual(PeerState.connected, fwd.getPeer(2).?.state);
    try testing.expectEqual(@as(i64, 1000), fwd.getPeer(2).?.last_success_ms);

    // Mark disconnected — backoff should double
    fwd.peerDisconnected(2, 2000);
    try testing.expectEqual(PeerState.disconnected, fwd.getPeer(2).?.state);
    try testing.expectEqual(@as(i64, 200), fwd.getPeer(2).?.reconnect_backoff_ms); // 100 * 2
}

test "Forwarder reconnect detection" {
    var fwd = Forwarder.init(testing.allocator, 1, 0);
    defer fwd.deinit();

    try fwd.addPeer(2, "10.0.0.2", 4444);
    try fwd.addPeer(3, "10.0.0.3", 4444);

    // Disconnect both at t=1000
    fwd.peerDisconnected(2, 1000);
    fwd.peerDisconnected(3, 1000);

    // At t=1050, neither should need reconnect (backoff=200)
    try testing.expectEqual(@as(u32, 0), fwd.peersNeedingReconnect(1050));

    // At t=1200, both should (past 200ms backoff)
    try testing.expectEqual(@as(u32, 2), fwd.peersNeedingReconnect(1200));
}

test "Forwarder stats" {
    var fwd = Forwarder.init(testing.allocator, 1, 0);
    defer fwd.deinit();

    try fwd.addPeer(2, "10.0.0.2", 4444);

    _ = try fwd.forward(2, 1, 100, 64, 1000);
    _ = try fwd.forward(2, 2, 101, 64, 1000);
    _ = fwd.complete(1, 2000);

    const s = fwd.stats();
    try testing.expectEqual(@as(u32, 1), s.pending_count);
    try testing.expectEqual(@as(u32, 1), s.peer_count);
    try testing.expectEqual(@as(u64, 2), s.total_forwarded);
    try testing.expectEqual(@as(u64, 1), s.total_responses);
}
