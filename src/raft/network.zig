//! Raft Peer Network - TCP-based entry replication between cluster nodes.
//!
//! Manages peer connections and broadcasts committed entries to all peers.
//! Each entry written on this node is replicated to all connected peers.
//! Incoming entries from peers are applied to the local KV projection
//! via the shard's inbox mechanism.
//!
//! ## Topology
//!
//! Nodes form a full mesh via peer exchange. When a new node joins the
//! seed, the seed tells it about all existing peers (and vice versa).
//! Each node then connects directly to every other node, so the cluster
//! survives seed failure.
//!
//! A peer is reached at the address it advertised in its join request —
//! its `[server] bind` address when that names an interface, otherwise the
//! source address the accepting side observed.

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;
const transport = @import("transport.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const inbox_mod = @import("../node/inbox.zig");
const ReplicationMetrics = @import("../metrics/registry.zig").ReplicationMetrics;

const Entry = entry_mod.Entry;
const RaftHeader = transport.RaftHeader;
const HEADER_SIZE = transport.HEADER_SIZE;
const Inbox = inbox_mod.Inbox;
const stdx_net = @import("stdx").net;

// --- 0.16 compat: thin wrappers around std.c.* with std.posix-style errors ---
// std.posix.{socket,connect,bind,listen,accept,close,write} were removed; the
// libc functions still work and we link libc.

const SocketError = error{ SocketCreateFailed, OutOfMemory, SystemResources, Unexpected };
fn sysSocket(family: u32, sock_type: u32, protocol: u32) SocketError!posix.socket_t {
    // macOS rejects SOCK.NONBLOCK / SOCK.CLOEXEC in the socket() type field
    // (returns EPROTONOSUPPORT). Strip them and apply via fcntl after creation.
    const builtin = @import("builtin");
    const strip_mask: u32 = if (builtin.os.tag == .macos)
        posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC
    else
        0;
    const want_nonblock = (sock_type & posix.SOCK.NONBLOCK) != 0;
    const want_cloexec = (sock_type & posix.SOCK.CLOEXEC) != 0;
    const real_type = sock_type & ~strip_mask;

    const rc = std.c.socket(@intCast(family), @intCast(real_type), @intCast(protocol));
    if (rc < 0) return error.SocketCreateFailed;
    const fd: posix.socket_t = @intCast(rc);

    if (builtin.os.tag == .macos) {
        if (want_nonblock) {
            const F_GETFL: c_int = 3;
            const F_SETFL: c_int = 4;
            const O_NONBLOCK: c_int = 4;
            const flags = std.c.fcntl(fd, F_GETFL, @as(c_int, 0));
            if (flags >= 0) _ = std.c.fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        }
        if (want_cloexec) {
            const F_SETFD: c_int = 2;
            const FD_CLOEXEC: c_int = 1;
            _ = std.c.fcntl(fd, F_SETFD, FD_CLOEXEC);
        }
    }
    return fd;
}
fn sysClose(fd: posix.socket_t) void {
    _ = std.c.close(fd);
}
fn sysBind(fd: posix.socket_t, addr: *const posix.sockaddr, len: posix.socklen_t) error{BindFailed}!void {
    if (std.c.bind(fd, addr, len) != 0) return error.BindFailed;
}
fn sysListen(fd: posix.socket_t, backlog: u31) error{ListenFailed}!void {
    if (std.c.listen(fd, backlog) != 0) return error.ListenFailed;
}
fn sysConnect(fd: posix.socket_t, addr: *const posix.sockaddr, len: posix.socklen_t) error{ConnectFailed}!void {
    if (std.c.connect(fd, addr, len) != 0) return error.ConnectFailed;
}
fn sysAccept(fd: posix.socket_t, addr: ?*posix.sockaddr, len: ?*posix.socklen_t, _: u32) error{AcceptFailed}!posix.socket_t {
    const rc = std.c.accept(fd, addr, len);
    if (rc < 0) return error.AcceptFailed;
    return @intCast(rc);
}
fn sysWrite(fd: posix.socket_t, buf: []const u8) error{WriteFailed}!usize {
    const rc = std.c.write(fd, buf.ptr, buf.len);
    if (rc < 0) return error.WriteFailed;
    return @intCast(rc);
}

const SocketAddr = struct {
    sa: posix.sockaddr.in,

    fn initIp4(ip4: [4]u8, port: u16) SocketAddr {
        return .{ .sa = .{
            .family = std.c.AF.INET,
            .port = std.mem.nativeToBig(u16, port),
            .addr = @bitCast(ip4),
            .zero = .{0} ** 8,
        } };
    }
    fn anyPtr(self: *const SocketAddr) *const posix.sockaddr {
        return @ptrCast(&self.sa);
    }
    fn anyLen(_: SocketAddr) posix.socklen_t {
        return @sizeOf(posix.sockaddr.in);
    }
};
const InboxMessage = inbox_mod.Message;

pub const MAX_PEERS = 7;
pub const MAX_MSG_BUF = 256 * 1024 + 128;
pub const RECV_BUF_SIZE = 65536;
pub const TICK_INTERVAL_MS = 10;
/// Deadlines for one dial, on the loop thread, where every millisecond is
/// one nobody else is served. Seeds get longer: they are named by an
/// operator and may be on another network.
pub const SEED_CONNECT_TIMEOUT_MS: i32 = 2000;
pub const MESH_CONNECT_TIMEOUT_MS: i32 = 1000;

/// Extended message types beyond MsgType enum in transport.zig
pub const MSG_JOIN_REQUEST: u8 = 6;
pub const MSG_JOIN_RESPONSE: u8 = 7;
pub const MSG_REPLICATE_ENTRY: u8 = 8;
pub const MSG_PEER_INFO: u8 = 9;

/// Join request payload: node_id(4) + raft_port(2) + main_port(2) + advertised ip4(4)
pub const JOIN_REQ_SIZE: usize = 12;
/// Join response payload: node_id(4); `JOIN_REJECTED` (never a real id)
/// means a live link already carries the dialer's id.
pub const JOIN_RESP_SIZE: usize = 4;
pub const JOIN_REJECTED: u32 = 0;
/// Peer info payload: node_id(4) + ip4(4) + raft_port(2)
pub const PEER_INFO_SIZE: usize = 10;

pub const JoinRequest = struct {
    node_id: u32,
    raft_port: u16,
    main_port: u16,
    /// 0.0.0.0 means "use the address you see me connecting from".
    ip4: [4]u8,

    pub fn encode(self: JoinRequest, buf: *[JOIN_REQ_SIZE]u8) void {
        std.mem.writeInt(u32, buf[0..4], self.node_id, .little);
        std.mem.writeInt(u16, buf[4..6], self.raft_port, .little);
        std.mem.writeInt(u16, buf[6..8], self.main_port, .little);
        buf[8..12].* = self.ip4;
    }

    pub fn decode(buf: *const [JOIN_REQ_SIZE]u8) JoinRequest {
        return .{
            .node_id = std.mem.readInt(u32, buf[0..4], .little),
            .raft_port = std.mem.readInt(u16, buf[4..6], .little),
            .main_port = std.mem.readInt(u16, buf[6..8], .little),
            .ip4 = buf[8..12].*,
        };
    }
};

pub const PeerInfo = struct {
    node_id: u32,
    ip4: [4]u8,
    raft_port: u16,

    pub fn encode(self: PeerInfo, buf: *[PEER_INFO_SIZE]u8) void {
        std.mem.writeInt(u32, buf[0..4], self.node_id, .little);
        buf[4..8].* = self.ip4;
        std.mem.writeInt(u16, buf[8..10], self.raft_port, .little);
    }

    pub fn decode(buf: *const [PEER_INFO_SIZE]u8) PeerInfo {
        return .{
            .node_id = std.mem.readInt(u32, buf[0..4], .little),
            .ip4 = buf[4..8].*,
            .raft_port = std.mem.readInt(u16, buf[8..10], .little),
        };
    }
};

/// A dial is retried this many times, this far apart, before the peer is
/// given up on. Seeds get five times as long: they are named by an operator
/// and the nodes of a cluster rarely start within seconds of each other.
pub const DIAL_ATTEMPTS: u8 = 30;
pub const SEED_DIAL_ATTEMPTS: u8 = 150;
pub const DIAL_RETRY_MS: i64 = 200;

pub const DialKind = enum {
    /// Named by the operator; its node id is unknown until it answers.
    seed,
    /// Learned through peer exchange.
    mesh,
};

const PendingDial = struct {
    info: PeerInfo,
    kind: DialKind,
    attempts: u8 = 0,
    not_before_ms: i64 = 0,
    /// The rejection warning is said once per entry.
    warned: bool = false,
};

pub const PeerState = struct {
    active: bool = false,
    node_id: u32 = 0,
    fd: posix.socket_t = -1,
    ip4: [4]u8 = .{ 0, 0, 0, 0 },
    raft_port: u16 = 0,
    recv_buf: [RECV_BUF_SIZE]u8 = undefined,
    recv_len: usize = 0,
};

pub const RaftNetwork = struct {
    allocator: Allocator,
    node_id: u32,
    listen_port: u16,
    main_port: u16,
    advertise_ip4: [4]u8,
    listener_fd: posix.socket_t,
    peers: [MAX_PEERS]PeerState,
    peer_count: u8,
    running: std.atomic.Value(bool),
    thread: ?std.Thread,
    /// Guards `pending` (entries to broadcast). Taken on the shard thread's
    /// commit path, so it is never held across anything that waits.
    mutex: @import("stdx").Mutex,
    pending: std.ArrayListUnmanaged([]u8),
    /// Guards `pending_peers`. Never held across a dial either.
    dial_mutex: @import("stdx").Mutex,
    shard_inbox: ?*Inbox,
    /// Peers learned through peer exchange that we are still trying to reach.
    pending_peers: [MAX_PEERS]PendingDial,
    pending_peer_count: u8,
    /// Optional replication metrics (issue #16). Set by the runtime when the
    /// dashboard/metrics registry is enabled; null otherwise (logging still fires).
    repl_metrics: ?*ReplicationMetrics = null,

    /// `bind_ip4`: listener address; advertised to peers unless 0.0.0.0.
    pub fn init(allocator: Allocator, node_id: u32, listen_port: u16, main_port: u16, bind_ip4: [4]u8) !RaftNetwork {
        const fd = try sysSocket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
        errdefer sysClose(fd);

        const opt_val: i32 = 1;
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&opt_val));

        const addr = SocketAddr.initIp4(bind_ip4, listen_port);
        try sysBind(fd, addr.anyPtr(), addr.anyLen());
        try sysListen(fd, 16);
        // Port 0 asks the kernel for one; peers must be told the real one.
        const bound_port = if (listen_port == 0) (try stdx_net.sysLocalIp4(fd)).port else listen_port;

        return .{
            .allocator = allocator,
            .node_id = node_id,
            .listen_port = bound_port,
            .main_port = main_port,
            .advertise_ip4 = bind_ip4,
            .listener_fd = fd,
            .peers = [_]PeerState{.{}} ** MAX_PEERS,
            .peer_count = 0,
            .running = std.atomic.Value(bool).init(false),
            .thread = null,
            .mutex = .{},
            .pending = .empty,
            .dial_mutex = .{},
            .shard_inbox = null,
            .pending_peers = undefined,
            .pending_peer_count = 0,
            .repl_metrics = null,
        };
    }

    /// Wire replication metrics (issue #16). Optional — when unset, the loud
    /// log lines still fire; only the counters are skipped.
    pub fn setReplicationMetrics(self: *RaftNetwork, m: *ReplicationMetrics) void {
        self.repl_metrics = m;
    }

    pub fn deinit(self: *RaftNetwork) void {
        self.stop();
        sysClose(self.listener_fd);
        for (self.pending.items) |data| {
            self.allocator.free(data);
        }
        self.pending.deinit(self.allocator);
        for (&self.peers) |*p| {
            if (p.active) {
                sysClose(p.fd);
                p.active = false;
            }
        }
    }

    pub fn setShardInbox(self: *RaftNetwork, inbox: *Inbox) void {
        self.shard_inbox = inbox;
    }

    /// Ask the loop thread to join the cluster through this address. Only
    /// that thread touches the peer table, and startup does not wait.
    pub fn dialSeed(self: *RaftNetwork, ip4: [4]u8, port: u16) void {
        self.dial_mutex.lock();
        defer self.dial_mutex.unlock();
        self.queueDial(.{ .node_id = 0, .ip4 = ip4, .raft_port = port }, .seed);
    }

    fn joinRequestBytes(self: *const RaftNetwork) [JOIN_REQ_SIZE]u8 {
        var payload: [JOIN_REQ_SIZE]u8 = undefined;
        JoinRequest.encode(.{
            .node_id = self.node_id,
            .raft_port = self.listen_port,
            .main_port = self.main_port,
            .ip4 = self.advertise_ip4,
        }, &payload);
        return payload;
    }

    /// Queue a serialized entry for broadcast to all peers.
    /// Thread-safe: called from shard thread, drained by network thread.
    pub fn broadcastEntry(self: *RaftNetwork, entry_data: []const u8) !void {
        const dup = try self.allocator.dupe(u8, entry_data);
        errdefer self.allocator.free(dup);
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.pending.append(self.allocator, dup);
    }

    pub fn start(self: *RaftNetwork) !void {
        self.running.store(true, .release);
        self.thread = try std.Thread.spawn(.{}, networkLoop, .{self});
    }

    pub fn stop(self: *RaftNetwork) void {
        if (!self.running.load(.acquire)) return;
        self.running.store(false, .release);
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    fn networkLoop(self: *RaftNetwork) void {
        while (self.running.load(.acquire)) {
            self.acceptPending();
            self.readFromPeers();
            self.flushPending();
            self.connectPendingPeers();
            @import("stdx").time.sleep(TICK_INTERVAL_MS * std.time.ns_per_ms);
        }
    }

    fn acceptPending(self: *RaftNetwork) void {
        while (true) {
            var client_addr: posix.sockaddr.storage = std.mem.zeroes(posix.sockaddr.storage);
            var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.storage);
            const client_fd = sysAccept(self.listener_fd, @ptrCast(&client_addr), &addr_len, 0) catch return;

            // Read join request with timeout
            var req_buf: [HEADER_SIZE + JOIN_REQ_SIZE + 16]u8 = undefined;
            const n = readWithTimeout(client_fd, &req_buf, 2000) catch |err| {
                log.debug("raft: join request read failed fd={d}: {s}", .{ client_fd, @errorName(err) });
                sysClose(client_fd);
                continue;
            };
            if (n < HEADER_SIZE + JOIN_REQ_SIZE) {
                sysClose(client_fd);
                continue;
            }

            const hdr = RaftHeader.fromBytes(req_buf[0..HEADER_SIZE]);
            if (hdr.msg_type != MSG_JOIN_REQUEST) {
                sysClose(client_fd);
                continue;
            }

            const join = JoinRequest.decode(req_buf[HEADER_SIZE..][0..JOIN_REQ_SIZE]);
            const peer_node_id = join.node_id;
            const peer_raft_port = join.raft_port;
            if (peer_node_id == self.node_id) {
                // Our own seed list names us (the usual case: every node lists
                // every node), or another node carries our id. Answer so the
                // dialer can tell which, then close.
                _ = self.sendJoinResponse(client_fd, self.node_id);
                sysClose(client_fd);
                continue;
            }
            const observed = stdx_net.ip4FromSockaddr(@ptrCast(&client_addr)) orelse {
                sysClose(client_fd);
                continue;
            };
            const peer_ip4 = peerAddressFor(join.ip4, observed) orelse {
                log.warn("raft: rejected join from node {d}: advertised address {d}.{d}.{d}.{d} is not dialable", .{ peer_node_id, join.ip4[0], join.ip4[1], join.ip4[2], join.ip4[3] });
                sysClose(client_fd);
                continue;
            };

            if (self.peerByNodeId(peer_node_id)) |held| {
                if (held.raft_port != peer_raft_port or !std.mem.eql(u8, &held.ip4, &peer_ip4)) {
                    log.warn("raft: node {d} joined from {d}.{d}.{d}.{d}:{d} while linked at {d}.{d}.{d}.{d}:{d} — two nodes may share an id (set [cluster] node_id)", .{ peer_node_id, peer_ip4[0], peer_ip4[1], peer_ip4[2], peer_ip4[3], peer_raft_port, held.ip4[0], held.ip4[1], held.ip4[2], held.ip4[3], held.raft_port });
                }
                // A dial that timed out leaves its connection in our backlog
                // and we may well accept that one first. Keep whichever link
                // is alive; a live one wins over a newer one, and the newer
                // dialer is told so rather than left guessing.
                if (linkAlive(held.fd)) {
                    _ = self.sendJoinResponse(client_fd, JOIN_REJECTED);
                    sysClose(client_fd);
                    continue;
                }
                self.dropPeer(peer_node_id);
            }

            if (self.peer_count >= MAX_PEERS) {
                // Announcing a peer we cannot hold would send everyone dialing it.
                log.warn("raft: no free peer slot for node {d} (at most {d} peers)", .{ peer_node_id, MAX_PEERS });
                sysClose(client_fd);
                continue;
            }

            if (!self.sendJoinResponse(client_fd, self.node_id)) {
                sysClose(client_fd);
                continue;
            }

            // Peer exchange: tell the new peer about all existing peers
            for (&self.peers) |*p| {
                if (!p.active or p.raft_port == 0) continue;
                self.sendPeerInfo(client_fd, .{ .node_id = p.node_id, .ip4 = p.ip4, .raft_port = p.raft_port });
            }

            // Tell existing peers about the new peer
            for (&self.peers) |*p| {
                if (!p.active) continue;
                self.sendPeerInfo(p.fd, .{ .node_id = peer_node_id, .ip4 = peer_ip4, .raft_port = peer_raft_port });
            }

            setNonBlocking(client_fd) catch {
                sysClose(client_fd);
                continue;
            };

            self.addPeerByFd(peer_node_id, client_fd, peer_ip4, peer_raft_port);
        }
    }

    /// Answer a join. `JOIN_REJECTED` tells the dialer a live link already
    /// carries its id.
    fn sendJoinResponse(self: *RaftNetwork, fd: posix.socket_t, node_id: u32) bool {
        var payload: [JOIN_RESP_SIZE]u8 = undefined;
        std.mem.writeInt(u32, payload[0..4], node_id, .little);
        var buf: [HEADER_SIZE + JOIN_RESP_SIZE]u8 = undefined;
        _ = frameCustomMessage(MSG_JOIN_RESPONSE, 0, self.node_id, &payload, &buf);
        _ = sysWrite(fd, &buf) catch |err| {
            log.debug("raft: join response write failed fd={d}: {s}", .{ fd, @errorName(err) });
            return false;
        };
        return true;
    }

    /// Send a MSG_PEER_INFO message to a specific fd.
    fn sendPeerInfo(self: *RaftNetwork, fd: posix.socket_t, info: PeerInfo) void {
        var payload: [PEER_INFO_SIZE]u8 = undefined;
        info.encode(&payload);
        var buf: [HEADER_SIZE + PEER_INFO_SIZE]u8 = undefined;
        const total = frameCustomMessage(MSG_PEER_INFO, 0, self.node_id, &payload, &buf);
        _ = sysWrite(fd, buf[0..total]) catch {};
    }

    fn readFromPeers(self: *RaftNetwork) void {
        for (&self.peers) |*p| {
            if (!p.active) continue;
            const space = p.recv_buf[p.recv_len..];
            if (space.len == 0) {
                p.recv_len = 0;
                continue;
            }
            const n = posix.read(p.fd, space) catch |err| {
                if (err == error.WouldBlock) continue;
                log.info("raft: peer {d} link lost: {s}", .{ p.node_id, @errorName(err) });
                sysClose(p.fd);
                p.active = false;
                if (self.peer_count > 0) self.peer_count -= 1;
                continue;
            };
            if (n == 0) {
                log.info("raft: peer {d} disconnected", .{p.node_id});
                sysClose(p.fd);
                p.active = false;
                if (self.peer_count > 0) self.peer_count -= 1;
                continue;
            }
            p.recv_len += n;
            self.processMessages(p);
        }
    }

    fn processMessages(self: *RaftNetwork, peer: *PeerState) void {
        var offset: usize = 0;
        while (offset + HEADER_SIZE <= peer.recv_len) {
            const hdr = RaftHeader.fromBytes(peer.recv_buf[offset..][0..HEADER_SIZE]);
            const payload_len: usize = @intCast(hdr.payload_len);
            const msg_size = HEADER_SIZE + payload_len;
            if (offset + msg_size > peer.recv_len) break;

            const payload = peer.recv_buf[offset + HEADER_SIZE .. offset + msg_size];

            if (hdr.msg_type == MSG_REPLICATE_ENTRY) {
                self.pushToShardInbox(payload);
                // Rebroadcast to other peers (mesh forwarding for late joiners)
                self.rebroadcast(peer.fd, hdr.source_node, payload);
            } else if (hdr.msg_type == MSG_PEER_INFO and payload_len >= PEER_INFO_SIZE) {
                const info = PeerInfo.decode(payload[0..PEER_INFO_SIZE]);
                // Queue connection if not already connected and not ourselves
                if (info.node_id != self.node_id and !self.hasPeer(info.node_id) and info.raft_port > 0) {
                    self.dial_mutex.lock();
                    defer self.dial_mutex.unlock();
                    self.queueDial(info, .mesh);
                }
            }

            offset += msg_size;
        }

        // Compact the buffer
        if (offset > 0) {
            if (offset < peer.recv_len) {
                std.mem.copyForwards(u8, &peer.recv_buf, peer.recv_buf[offset..peer.recv_len]);
            }
            peer.recv_len -= offset;
        }
    }

    fn rebroadcast(self: *RaftNetwork, sender_fd: posix.socket_t, source_node: u32, entry_data: []const u8) void {
        // In a full mesh, the source has already sent to all its peers.
        // Only rebroadcast if there are peers that might not have received it
        // (e.g., nodes that joined after the source). Skip the sender and the
        // original source node to avoid duplicates.
        var msg_buf: [MAX_MSG_BUF]u8 = undefined;
        if (entry_data.len + HEADER_SIZE > MAX_MSG_BUF) {
            log.warn("raft: entry too large to re-broadcast ({d} bytes > {d} max) — not forwarded; late-joining peers will diverge", .{ entry_data.len + HEADER_SIZE, MAX_MSG_BUF });
            if (self.repl_metrics) |m| m.recordOversizeSkipped();
            return;
        }
        const total = frameCustomMessage(MSG_REPLICATE_ENTRY, 0, source_node, entry_data, &msg_buf);
        for (&self.peers) |*p| {
            if (!p.active) continue;
            if (p.fd == sender_fd) continue;
            if (p.node_id == source_node) continue;
            _ = sysWrite(p.fd, msg_buf[0..total]) catch |err| blk: {
                log.warn("raft: failed to re-broadcast entry to peer node {d}: {s} — not delivered, no retry", .{ p.node_id, @errorName(err) });
                if (self.repl_metrics) |m| m.recordSendFailure();
                break :blk 0;
            };
        }
    }

    fn flushPending(self: *RaftNetwork) void {
        // Swap out the pending list under lock
        self.mutex.lock();
        var to_send = self.pending;
        self.pending = .empty;
        self.mutex.unlock();

        defer {
            for (to_send.items) |data| {
                self.allocator.free(data);
            }
            to_send.deinit(self.allocator);
        }

        for (to_send.items) |entry_data| {
            var msg_buf: [MAX_MSG_BUF]u8 = undefined;
            if (entry_data.len + HEADER_SIZE > MAX_MSG_BUF) {
                log.warn("raft: committed entry too large to replicate ({d} bytes > {d} max) — NOT broadcast to any peer; all followers will diverge on this entry", .{ entry_data.len + HEADER_SIZE, MAX_MSG_BUF });
                if (self.repl_metrics) |m| m.recordOversizeSkipped();
                continue;
            }
            const total = frameCustomMessage(MSG_REPLICATE_ENTRY, 0, self.node_id, entry_data, &msg_buf);
            for (&self.peers) |*p| {
                if (!p.active) continue;
                _ = sysWrite(p.fd, msg_buf[0..total]) catch |err| blk: {
                    log.warn("raft: failed to broadcast entry to peer node {d}: {s} — not delivered, no retry (follower may diverge)", .{ p.node_id, @errorName(err) });
                    if (self.repl_metrics) |m| m.recordSendFailure();
                    break :blk 0;
                };
            }
        }
    }

    /// Remember a dial, once. Seeds have no id yet, so they are told apart
    /// by address; a mesh entry already pending under this id takes the
    /// newer address. A full list drops a mesh entry that has already
    /// failed, never a seed; if nothing has failed yet the newcomer is not
    /// queued (a later join's peer info may queue it). Caller holds
    /// `dial_mutex`.
    fn queueDial(self: *RaftNetwork, info: PeerInfo, kind: DialKind) void {
        for (self.pending_peers[0..self.pending_peer_count]) |*p| {
            if (kind == .seed) {
                if (p.kind == .seed and p.info.raft_port == info.raft_port and std.mem.eql(u8, &p.info.ip4, &info.ip4)) return;
            } else if (p.kind == .mesh and p.info.node_id == info.node_id) {
                p.info = info;
                return;
            }
        }
        const entry: PendingDial = .{ .info = info, .kind = kind };
        if (self.pending_peer_count < MAX_PEERS) {
            self.pending_peers[self.pending_peer_count] = entry;
            self.pending_peer_count += 1;
            return;
        }
        var victim: ?usize = null;
        for (self.pending_peers[0..self.pending_peer_count], 0..) |p, i| {
            if (p.kind != .mesh or p.attempts == 0) continue;
            if (victim == null or p.attempts > self.pending_peers[victim.?].attempts) victim = i;
        }
        const v = victim orelse {
            log.debug("raft: dial list full; not queuing peer {d}", .{info.node_id});
            return;
        };
        log.debug("raft: dial list full; dropping peer {d} after {d} attempts for peer {d}", .{ self.pending_peers[v].info.node_id, self.pending_peers[v].attempts, info.node_id });
        self.pending_peers[v] = entry;
    }

    /// Dial one pending entry per tick, retrying at a fixed interval: the
    /// far side's loop may be busy in a handshake of its own when we call,
    /// and a single failed dial would otherwise mean no link for the life
    /// of the process. The entry is copied out and the lock released
    /// before dialing, so nothing that queues a dial or a broadcast waits
    /// on a socket.
    fn connectPendingPeers(self: *RaftNetwork) void {
        const now = @import("stdx").time.milliTimestamp();
        const picked: PendingDial = blk: {
            self.dial_mutex.lock();
            defer self.dial_mutex.unlock();
            var i: usize = 0;
            var due: ?usize = null;
            while (i < self.pending_peer_count) {
                const p = &self.pending_peers[i];
                const done = switch (p.kind) {
                    .seed => self.peerByAddress(p.info.ip4, p.info.raft_port) != null,
                    // Both sides learn of each other from the same peer
                    // exchange and would dial at once. A dial blocks this
                    // single loop thread on the join response, which only the
                    // far side's loop can send — two mutual dials both time
                    // out. The lower id dials; the higher id waits for it.
                    .mesh => self.hasPeer(p.info.node_id) or p.info.node_id < self.node_id,
                };
                if (done) {
                    self.dropPending(i);
                    continue;
                }
                // Least recently tried first, so two dead entries whose dials
                // outlast the retry interval cannot starve the others.
                if (p.not_before_ms <= now and (due == null or p.not_before_ms < self.pending_peers[due.?].not_before_ms)) due = i;
                i += 1;
            }
            const d = due orelse return;
            break :blk self.pending_peers[d];
        };

        const outcome = self.dial(picked.info.ip4, picked.info.raft_port, picked.kind);

        self.dial_mutex.lock();
        defer self.dial_mutex.unlock();
        const i = self.findPending(picked) orelse return;
        const p = &self.pending_peers[i];
        const budget: u8 = if (p.kind == .seed) SEED_DIAL_ATTEMPTS else DIAL_ATTEMPTS;
        switch (outcome) {
            .linked, .finished => {
                self.dropPending(i);
                return;
            },
            // The far side may hold a link to us that died without a FIN;
            // it will notice on its next write, and we are the side that
            // dials. Keep trying, and say so once.
            .rejected => if (!p.warned) {
                p.warned = true;
                log.warn("raft: {d}.{d}.{d}.{d}:{d} refused our join: a live link already carries our node id {d} — it may still hold a stale link to us, or another node shares our id (set [cluster] node_id)", .{ picked.info.ip4[0], picked.info.ip4[1], picked.info.ip4[2], picked.info.ip4[3], picked.info.raft_port, self.node_id });
            },
            .failed => {},
        }
        p.attempts += 1;
        if (p.attempts >= budget) {
            switch (p.kind) {
                .seed => log.warn("cluster: could not join seed {d}.{d}.{d}.{d}:{d} after {d} attempts; continuing alone", .{ picked.info.ip4[0], picked.info.ip4[1], picked.info.ip4[2], picked.info.ip4[3], picked.info.raft_port, p.attempts }),
                .mesh => log.warn("raft: giving up on peer {d} at {d}.{d}.{d}.{d}:{d} after {d} attempts", .{ picked.info.node_id, picked.info.ip4[0], picked.info.ip4[1], picked.info.ip4[2], picked.info.ip4[3], picked.info.raft_port, p.attempts }),
            }
            self.dropPending(i);
            return;
        }
        p.not_before_ms = @import("stdx").time.milliTimestamp() + DIAL_RETRY_MS;
    }

    /// The entry a dial was started for may have moved (swap-remove) or been
    /// replaced while the lock was released; find it again by identity.
    fn findPending(self: *RaftNetwork, d: PendingDial) ?usize {
        for (self.pending_peers[0..self.pending_peer_count], 0..) |p, i| {
            if (p.kind != d.kind) continue;
            const same = if (d.kind == .seed)
                p.info.raft_port == d.info.raft_port and std.mem.eql(u8, &p.info.ip4, &d.info.ip4)
            else
                p.info.node_id == d.info.node_id;
            if (same) return i;
        }
        return null;
    }

    fn dropPending(self: *RaftNetwork, i: usize) void {
        const last = self.pending_peer_count - 1;
        if (i != last) self.pending_peers[i] = self.pending_peers[last];
        self.pending_peer_count = last;
    }

    const DialOutcome = enum {
        /// A new link is up.
        linked,
        /// Nothing more to do: it was us, or a link already exists.
        finished,
        /// The far side holds a live link under our id.
        rejected,
        /// Worth another try later.
        failed,
    };

    /// One dial with deadlines, on the loop thread.
    fn dial(self: *RaftNetwork, ip4: [4]u8, port: u16, kind: DialKind) DialOutcome {
        const connect_ms: i32 = if (kind == .seed) SEED_CONNECT_TIMEOUT_MS else MESH_CONNECT_TIMEOUT_MS;
        const fd = stdx_net.tcpConnectIp4Timeout(ip4, port, connect_ms) catch return .failed;

        // Our own listener, reached by one of our addresses (the usual seed
        // list names every node, this one included): the connect completes
        // from the backlog, but the answer would have to come from this very
        // thread. A connection to ourselves has our address at both ends.
        if (port == self.listen_port) {
            if (stdx_net.sysLocalIp4(fd)) |local| {
                if (std.mem.eql(u8, &local.ip4, &ip4)) {
                    sysClose(fd);
                    log.debug("raft: {d}.{d}.{d}.{d}:{d} is this node", .{ ip4[0], ip4[1], ip4[2], ip4[3], port });
                    return .finished;
                }
            } else |_| {}
        }

        var msg_buf: [HEADER_SIZE + JOIN_REQ_SIZE]u8 = undefined;
        _ = frameCustomMessage(MSG_JOIN_REQUEST, 0, self.node_id, &self.joinRequestBytes(), &msg_buf);
        _ = sysWrite(fd, &msg_buf) catch {
            sysClose(fd);
            return .failed;
        };

        // The far side answers within a tick unless its loop is busy.
        var resp_buf: [HEADER_SIZE + JOIN_RESP_SIZE]u8 = undefined;
        const n = readWithTimeout(fd, resp_buf[0 .. HEADER_SIZE + JOIN_RESP_SIZE], connect_ms) catch {
            sysClose(fd);
            return .failed;
        };
        if (n < HEADER_SIZE + JOIN_RESP_SIZE) {
            sysClose(fd);
            return .failed;
        }
        const resp_hdr = RaftHeader.fromBytes(resp_buf[0..HEADER_SIZE]);
        if (resp_hdr.msg_type != MSG_JOIN_RESPONSE) {
            sysClose(fd);
            return .failed;
        }
        const peer_node_id = std.mem.readInt(u32, resp_buf[HEADER_SIZE..][0..4], .little);
        if (peer_node_id == JOIN_REJECTED) {
            sysClose(fd);
            return .rejected;
        }
        if (peer_node_id == self.node_id) {
            // Normally caught above without a handshake; reaching here means
            // the local-address check could not tell, or another node
            // carries our id.
            sysClose(fd);
            log.warn("raft: {d}.{d}.{d}.{d}:{d} answered with our own node id {d} — either we dialed ourselves by an address we could not recognise, or another node shares our id (set [cluster] node_id)", .{ ip4[0], ip4[1], ip4[2], ip4[3], port, self.node_id });
            return .finished;
        }
        if (self.peerByNodeId(peer_node_id)) |held| {
            if (linkAlive(held.fd)) {
                sysClose(fd);
                return .finished;
            }
            self.dropPeer(peer_node_id);
        }
        self.addPeerByFd(peer_node_id, fd, ip4, port);
        return .linked;
    }

    fn pushToShardInbox(self: *RaftNetwork, entry_data: []const u8) void {
        const inbox = self.shard_inbox orelse return;
        const dup = self.allocator.dupe(u8, entry_data) catch return;
        _ = inbox.send(.{
            .tag = .raft_message,
            .src_shard = 0xFF,
            .partition_id = 0,
            .payload_len = @intCast(dup.len),
            .sequence = 0,
            .payload_ptr = dup.ptr,
            ._padding = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
        });
    }

    fn addPeerByFd(self: *RaftNetwork, peer_node_id: u32, fd: posix.socket_t, ip4: [4]u8, raft_port: u16) void {
        for (&self.peers) |*slot| {
            if (!slot.active) {
                slot.* = .{
                    .active = true,
                    .node_id = peer_node_id,
                    .fd = fd,
                    .ip4 = ip4,
                    .raft_port = raft_port,
                    .recv_buf = undefined,
                    .recv_len = 0,
                };
                self.peer_count += 1;
                log.info("raft: peer {d} connected at {d}.{d}.{d}.{d}:{d}", .{ peer_node_id, ip4[0], ip4[1], ip4[2], ip4[3], raft_port });
                return;
            }
        }
        // No free slots
        sysClose(fd);
    }

    fn peerByAddress(self: *RaftNetwork, ip4: [4]u8, port: u16) ?*const PeerState {
        for (&self.peers) |*p| {
            if (p.active and p.raft_port == port and std.mem.eql(u8, &p.ip4, &ip4)) return p;
        }
        return null;
    }

    fn dropPeer(self: *RaftNetwork, node_id: u32) void {
        for (&self.peers) |*p| {
            if (p.active and p.node_id == node_id) {
                log.info("raft: replacing dead link to peer {d}", .{node_id});
                sysClose(p.fd);
                p.active = false;
                if (self.peer_count > 0) self.peer_count -= 1;
                return;
            }
        }
    }

    fn peerByNodeId(self: *RaftNetwork, node_id: u32) ?*const PeerState {
        for (&self.peers) |*p| {
            if (p.active and p.node_id == node_id) return p;
        }
        return null;
    }

    /// Check if we already have a connection to a peer with the given node_id.
    fn hasPeer(self: *RaftNetwork, node_id: u32) bool {
        for (&self.peers) |*p| {
            if (p.active and p.node_id == node_id) return true;
        }
        return false;
    }
};

// ── Helper functions ─────────────────────────────────────────────────

/// The address other peers should dial a joiner at. A joiner bound to
/// 0.0.0.0 does not know which of its addresses we can reach; the one it
/// connected from is. A loopback advertisement from a node that reached us
/// over the network is its own loopback, not ours, so it is treated the same.
/// Anything not dialable is refused.
fn peerAddressFor(advertised: [4]u8, observed: [4]u8) ?[4]u8 {
    const use_observed = std.mem.eql(u8, &advertised, &stdx_net.any_ip4) or
        (stdx_net.isLoopback(advertised) and !stdx_net.isLoopback(observed));
    const chosen = if (use_observed) observed else advertised;
    return if (stdx_net.isUnicastPeerAddress(chosen)) chosen else null;
}

/// Whether the far end of a non-blocking socket is still there: a peek that
/// would block means yes; end-of-stream or an error means no.
fn linkAlive(fd: posix.socket_t) bool {
    var byte: [1]u8 = undefined;
    const rc = std.c.recv(fd, &byte, 1, posix.MSG.PEEK);
    if (rc > 0) return true;
    if (rc == 0) return false;
    return posix.errno(rc) == .AGAIN;
}

fn frameCustomMessage(msg_type: u8, group_id: u32, source_node: u32, payload: []const u8, buf: []u8) usize {
    const total = HEADER_SIZE + payload.len;
    if (buf.len < total) return 0;

    var hdr = RaftHeader{
        .msg_type = msg_type,
        ._pad = .{ 0, 0, 0 },
        .group_id = group_id,
        .source_node = source_node,
        .payload_len = @intCast(payload.len),
        .crc32 = 0,
    };
    @memcpy(buf[0..HEADER_SIZE], hdr.asBytes());
    if (payload.len > 0) {
        @memcpy(buf[HEADER_SIZE..total], payload);
    }
    // Write CRC
    const crc = transport.computeCrc(buf[0..HEADER_SIZE], payload);
    std.mem.writeInt(u32, buf[16..20], crc, .little);
    return total;
}

fn setNonBlocking(fd: posix.socket_t) !void {
    return @import("stdx").net.sysFcntlSetNonblocking(fd);
}

fn readWithTimeout(fd: posix.socket_t, buf: []u8, timeout_ms: i32) !usize {
    var fds = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.IN, .revents = 0 }};
    const ready = try posix.poll(&fds, timeout_ms);
    if (ready == 0) return error.Timeout;
    return posix.read(fd, buf);
}

// ── Tests ────────────────────────────────────────────────────────────

const testing = std.testing;

test "raft network: frameCustomMessage roundtrip" {
    var payload = [_]u8{ 1, 2, 3, 4 };
    var buf: [128]u8 = undefined;
    const total = frameCustomMessage(MSG_REPLICATE_ENTRY, 0, 42, &payload, &buf);
    try testing.expectEqual(HEADER_SIZE + 4, total);
    const hdr = RaftHeader.fromBytes(buf[0..HEADER_SIZE]);
    try testing.expectEqual(MSG_REPLICATE_ENTRY, hdr.msg_type);
    try testing.expectEqual(@as(u32, 42), hdr.source_node);
    try testing.expectEqual(@as(u32, 4), hdr.payload_len);
}

test "raft network: join request and peer info carry the address" {
    var jbuf: [JOIN_REQ_SIZE]u8 = undefined;
    JoinRequest.encode(.{ .node_id = 7, .raft_port = 9500, .main_port = 9000, .ip4 = .{ 10, 0, 1, 12 } }, &jbuf);
    const j = JoinRequest.decode(&jbuf);
    try testing.expectEqual(@as(u32, 7), j.node_id);
    try testing.expectEqual(@as(u16, 9500), j.raft_port);
    try testing.expectEqual(@as(u16, 9000), j.main_port);
    try testing.expectEqual([4]u8{ 10, 0, 1, 12 }, j.ip4);

    var pbuf: [PEER_INFO_SIZE]u8 = undefined;
    PeerInfo.encode(.{ .node_id = 9, .ip4 = .{ 10, 0, 1, 13 }, .raft_port = 9500 }, &pbuf);
    const p = PeerInfo.decode(&pbuf);
    try testing.expectEqual(@as(u32, 9), p.node_id);
    try testing.expectEqual([4]u8{ 10, 0, 1, 13 }, p.ip4);
    try testing.expectEqual(@as(u16, 9500), p.raft_port);
}

test "raft network: the listener binds the configured address and advertises it" {
    var rn = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 });
    defer rn.deinit();
    const local = try stdx_net.sysLocalIp4(rn.listener_fd);
    try testing.expectEqual([4]u8{ 127, 0, 0, 1 }, local.ip4);
    try testing.expectEqual([4]u8{ 127, 0, 0, 1 }, rn.advertise_ip4);
}

test "raft network: the address a joiner is recorded at" {
    const remote: [4]u8 = .{ 10, 0, 1, 12 };
    const other: [4]u8 = .{ 10, 0, 1, 13 };
    try testing.expectEqual(other, peerAddressFor(other, remote).?);
    try testing.expectEqual(remote, peerAddressFor(stdx_net.any_ip4, remote).?);
    // Loopback advertised over the network means the joiner's own loopback.
    try testing.expectEqual(remote, peerAddressFor(.{ 127, 0, 0, 1 }, remote).?);
    // Both on loopback (one host): keep what was advertised.
    try testing.expectEqual([4]u8{ 127, 0, 0, 3 }, peerAddressFor(.{ 127, 0, 0, 3 }, .{ 127, 0, 0, 1 }).?);
    try testing.expect(peerAddressFor(.{ 224, 0, 0, 1 }, remote) == null);
    try testing.expect(peerAddressFor(.{ 255, 255, 255, 255 }, remote) == null);
}

fn boundPort(rn: *const RaftNetwork) !u16 {
    return (try stdx_net.sysLocalIp4(rn.listener_fd)).port;
}

fn waitForPeer(rn: *RaftNetwork, node_id: u32, timeout_ms: u64) bool {
    var waited: u64 = 0;
    while (waited < timeout_ms) : (waited += 20) {
        if (rn.hasPeer(node_id)) return true;
        @import("stdx").time.sleep(20 * std.time.ns_per_ms);
    }
    return false;
}

test "raft network: a node does not join itself" {
    var rn = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 });
    defer rn.deinit();
    rn.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&rn));
    try rn.start();
    // One dial settles it: the connection has our address at both ends, so
    // the entry is finished, not retried.
    var waited: u64 = 0;
    while (waited < 3000 and rn.pending_peer_count > 0) : (waited += 20) @import("stdx").time.sleep(20 * std.time.ns_per_ms);
    try testing.expectEqual(@as(u8, 0), rn.pending_peer_count);
    try testing.expectEqual(@as(u8, 0), rn.peer_count);
}

test "raft network: a joiner advertising 0.0.0.0 is recorded at the address it came from" {
    var seed = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 0, 0, 0, 0 });
    defer seed.deinit();
    try seed.start();
    var joiner = try RaftNetwork.init(testing.allocator, 2, 0, 9001, .{ 0, 0, 0, 0 });
    defer joiner.deinit();
    joiner.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&seed));
    try joiner.start();
    try testing.expect(waitForPeer(&seed, 2, 3000));
    try testing.expectEqual([4]u8{ 127, 0, 0, 1 }, seed.peerByNodeId(2).?.ip4);
    try testing.expectEqual(joiner.listen_port, seed.peerByNodeId(2).?.raft_port);
    try testing.expect(waitForPeer(&joiner, 1, 1000));
}

test "raft network: a mesh dial that finds nobody home is retried until the peer answers" {
    // `a` is bound (so the dial connects) but its loop is not running, so the
    // join response never comes and the first dial times out.
    var a = try RaftNetwork.init(testing.allocator, 2, 0, 9002, .{ 127, 0, 0, 1 });
    defer a.deinit();
    var b = try RaftNetwork.init(testing.allocator, 1, 0, 9001, .{ 127, 0, 0, 1 });
    defer b.deinit();
    b.queueDial(.{ .node_id = 2, .ip4 = .{ 127, 0, 0, 1 }, .raft_port = try boundPort(&a) }, .mesh);
    try b.start();
    @import("stdx").time.sleep(1300 * std.time.ns_per_ms);
    try testing.expect(!b.hasPeer(2));
    try a.start();
    try testing.expect(waitForPeer(&b, 2, 4000));
    try testing.expect(waitForPeer(&a, 1, 2000));
}

test "raft network: a full dial list drops a failed mesh entry, never a seed or a fresh one" {
    var rn = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 });
    defer rn.deinit();
    rn.queueDial(.{ .node_id = 0, .ip4 = .{ 10, 0, 0, 9 }, .raft_port = 9500 }, .seed);
    var id: u32 = 10;
    while (id < 10 + MAX_PEERS - 1) : (id += 1) rn.queueDial(.{ .node_id = id, .ip4 = .{ 10, 0, 0, 1 }, .raft_port = 9500 }, .mesh);
    try testing.expectEqual(@as(u8, MAX_PEERS), rn.pending_peer_count);

    // Nothing has failed yet: the newcomer is not queued and nothing is lost.
    rn.queueDial(.{ .node_id = 99, .ip4 = .{ 10, 0, 0, 2 }, .raft_port = 9500 }, .mesh);
    try testing.expectEqual(@as(u8, MAX_PEERS), rn.pending_peer_count);
    try testing.expect(rn.findPending(.{ .info = .{ .node_id = 99, .ip4 = .{ 10, 0, 0, 2 }, .raft_port = 9500 }, .kind = .mesh }) == null);

    // The seed has failed most; the failed mesh entry is the one to go.
    rn.pending_peers[0].attempts = 40;
    rn.pending_peers[3].attempts = 12;
    rn.queueDial(.{ .node_id = 99, .ip4 = .{ 10, 0, 0, 2 }, .raft_port = 9500 }, .mesh);
    try testing.expectEqual(@as(u8, MAX_PEERS), rn.pending_peer_count);
    try testing.expectEqual(@as(u32, 99), rn.pending_peers[3].info.node_id);
    try testing.expectEqual(DialKind.seed, rn.pending_peers[0].kind);

    // Queued again with a new address: the address is taken, nothing evicted.
    rn.queueDial(.{ .node_id = 99, .ip4 = .{ 10, 0, 0, 7 }, .raft_port = 9501 }, .mesh);
    try testing.expectEqual(@as(u8, MAX_PEERS), rn.pending_peer_count);
    try testing.expectEqual([4]u8{ 10, 0, 0, 7 }, rn.pending_peers[3].info.ip4);
    try testing.expectEqual(@as(u16, 9501), rn.pending_peers[3].info.raft_port);
}

test "raft network: a broadcast never waits on a dial" {
    // 192.0.2.0/24 is never routed; the dial burns its whole deadline. On a
    // network that rejects it outright the dial ends at once and this test
    // proves nothing either way.
    var rn = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 });
    defer rn.deinit();
    rn.dialSeed(.{ 192, 0, 2, 1 }, 9500);
    try rn.start();
    @import("stdx").time.sleep(300 * std.time.ns_per_ms);
    var worst_ms: i64 = 0;
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        const t0 = @import("stdx").time.milliTimestamp();
        try rn.broadcastEntry("entry");
        worst_ms = @max(worst_ms, @import("stdx").time.milliTimestamp() - t0);
        @import("stdx").time.sleep(100 * std.time.ns_per_ms);
    }
    try testing.expect(worst_ms < 200);
}
