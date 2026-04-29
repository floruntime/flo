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

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const transport = @import("transport.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const inbox_mod = @import("../node/inbox.zig");

const Entry = entry_mod.Entry;
const RaftHeader = transport.RaftHeader;
const HEADER_SIZE = transport.HEADER_SIZE;
const Inbox = inbox_mod.Inbox;

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

/// Extended message types beyond MsgType enum in transport.zig
pub const MSG_JOIN_REQUEST: u8 = 6;
pub const MSG_JOIN_RESPONSE: u8 = 7;
pub const MSG_REPLICATE_ENTRY: u8 = 8;
pub const MSG_PEER_INFO: u8 = 9;

/// Join request payload: node_id(4) + raft_port(2) + main_port(2) = 8 bytes
pub const JOIN_REQ_SIZE: usize = 8;
/// Join response payload: node_id(4) = 4 bytes
pub const JOIN_RESP_SIZE: usize = 4;
/// Peer info payload: node_id(4) + raft_port(2) = 6 bytes
pub const PEER_INFO_SIZE: usize = 6;

pub const PeerState = struct {
    active: bool = false,
    node_id: u32 = 0,
    fd: posix.socket_t = -1,
    raft_port: u16 = 0,
    recv_buf: [RECV_BUF_SIZE]u8 = undefined,
    recv_len: usize = 0,
};

pub const RaftNetwork = struct {
    allocator: Allocator,
    node_id: u32,
    listen_port: u16,
    main_port: u16,
    listener_fd: posix.socket_t,
    peers: [MAX_PEERS]PeerState,
    peer_count: u8,
    running: std.atomic.Value(bool),
    thread: ?std.Thread,
    mutex: @import("stdx").Mutex,
    pending: std.ArrayListUnmanaged([]u8),
    shard_inbox: ?*Inbox,
    /// Raft ports of peers we should connect to (from peer exchange).
    pending_peer_ports: [MAX_PEERS]u16,
    pending_peer_count: u8,

    pub fn init(allocator: Allocator, node_id: u32, listen_port: u16, main_port: u16) !RaftNetwork {
        const fd = try sysSocket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
        errdefer sysClose(fd);

        const opt_val: i32 = 1;
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&opt_val));

        const addr = SocketAddr.initIp4(.{ 0, 0, 0, 0 }, listen_port);
        try sysBind(fd, addr.anyPtr(), addr.anyLen());
        try sysListen(fd, 16);

        return .{
            .allocator = allocator,
            .node_id = node_id,
            .listen_port = listen_port,
            .main_port = main_port,
            .listener_fd = fd,
            .peers = [_]PeerState{.{}} ** MAX_PEERS,
            .peer_count = 0,
            .running = std.atomic.Value(bool).init(false),
            .thread = null,
            .mutex = .{},
            .pending = .empty,
            .shard_inbox = null,
            .pending_peer_ports = [_]u16{0} ** MAX_PEERS,
            .pending_peer_count = 0,
        };
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

    pub fn connectToPeer(self: *RaftNetwork, host: []const u8, port: u16) !void {
        const addr = try parseAddress(host, port);
        const fd = try sysSocket(posix.AF.INET, posix.SOCK.STREAM, 0);
        errdefer sysClose(fd);

        try sysConnect(fd, addr.anyPtr(), addr.anyLen());

        // Send join request
        var payload: [JOIN_REQ_SIZE]u8 = undefined;
        std.mem.writeInt(u32, payload[0..4], self.node_id, .little);
        std.mem.writeInt(u16, payload[4..6], self.listen_port, .little);
        std.mem.writeInt(u16, payload[6..8], self.main_port, .little);

        var msg_buf: [HEADER_SIZE + JOIN_REQ_SIZE]u8 = undefined;
        _ = frameCustomMessage(MSG_JOIN_REQUEST, 0, self.node_id, &payload, &msg_buf);
        _ = try sysWrite(fd, &msg_buf);

        // Read join response
        var resp_buf: [HEADER_SIZE + JOIN_RESP_SIZE]u8 = undefined;
        const n = try readExact(fd, resp_buf[0 .. HEADER_SIZE + JOIN_RESP_SIZE]);
        if (n < HEADER_SIZE) return error.ShortRead;

        const resp_hdr = RaftHeader.fromBytes(resp_buf[0..HEADER_SIZE]);
        if (resp_hdr.msg_type != MSG_JOIN_RESPONSE) return error.BadHandshake;

        const peer_node_id = std.mem.readInt(u32, resp_buf[HEADER_SIZE..][0..4], .little);
        setNonBlocking(fd) catch {};
        self.addPeerByFd(peer_node_id, fd, port);
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
            var client_addr: posix.sockaddr = undefined;
            var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
            const client_fd = sysAccept(self.listener_fd, &client_addr, &addr_len, 0) catch return;

            // Read join request with timeout
            var req_buf: [HEADER_SIZE + JOIN_REQ_SIZE + 16]u8 = undefined;
            const n = readWithTimeout(client_fd, &req_buf, 2000) catch {
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

            const peer_node_id = std.mem.readInt(u32, req_buf[HEADER_SIZE..][0..4], .little);
            const peer_raft_port = std.mem.readInt(u16, req_buf[HEADER_SIZE + 4 ..][0..2], .little);

            // Send join response (always, even if duplicate — peer expects it)
            var resp_payload: [JOIN_RESP_SIZE]u8 = undefined;
            std.mem.writeInt(u32, resp_payload[0..4], self.node_id, .little);

            var resp_buf: [HEADER_SIZE + JOIN_RESP_SIZE]u8 = undefined;
            _ = frameCustomMessage(MSG_JOIN_RESPONSE, 0, self.node_id, &resp_payload, &resp_buf);
            _ = sysWrite(client_fd, &resp_buf) catch {
                sysClose(client_fd);
                continue;
            };

            // Reject duplicate connections — keep existing connection
            if (self.hasPeer(peer_node_id)) {
                sysClose(client_fd);
                continue;
            }

            // Peer exchange: tell the new peer about all existing peers
            for (&self.peers) |*p| {
                if (!p.active or p.raft_port == 0) continue;
                self.sendPeerInfo(client_fd, p.node_id, p.raft_port);
            }

            // Tell existing peers about the new peer
            for (&self.peers) |*p| {
                if (!p.active) continue;
                self.sendPeerInfo(p.fd, peer_node_id, peer_raft_port);
            }

            setNonBlocking(client_fd) catch {
                sysClose(client_fd);
                continue;
            };

            self.addPeerByFd(peer_node_id, client_fd, peer_raft_port);
        }
    }

    /// Send a MSG_PEER_INFO message to a specific fd.
    fn sendPeerInfo(self: *RaftNetwork, fd: posix.socket_t, peer_node_id: u32, peer_raft_port: u16) void {
        var payload: [PEER_INFO_SIZE]u8 = undefined;
        std.mem.writeInt(u32, payload[0..4], peer_node_id, .little);
        std.mem.writeInt(u16, payload[4..6], peer_raft_port, .little);
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
                sysClose(p.fd);
                p.active = false;
                if (self.peer_count > 0) self.peer_count -= 1;
                continue;
            };
            if (n == 0) {
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
                const info_node_id = std.mem.readInt(u32, payload[0..4], .little);
                const info_raft_port = std.mem.readInt(u16, payload[4..6], .little);
                // Queue connection if not already connected and not ourselves
                if (info_node_id != self.node_id and !self.hasPeer(info_node_id) and info_raft_port > 0) {
                    if (self.pending_peer_count < MAX_PEERS) {
                        self.pending_peer_ports[self.pending_peer_count] = info_raft_port;
                        self.pending_peer_count += 1;
                    }
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
        if (entry_data.len + HEADER_SIZE > MAX_MSG_BUF) return;
        const total = frameCustomMessage(MSG_REPLICATE_ENTRY, 0, source_node, entry_data, &msg_buf);
        for (&self.peers) |*p| {
            if (!p.active) continue;
            if (p.fd == sender_fd) continue;
            if (p.node_id == source_node) continue;
            _ = sysWrite(p.fd, msg_buf[0..total]) catch {};
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
            if (entry_data.len + HEADER_SIZE > MAX_MSG_BUF) continue;
            const total = frameCustomMessage(MSG_REPLICATE_ENTRY, 0, self.node_id, entry_data, &msg_buf);
            for (&self.peers) |*p| {
                if (!p.active) continue;
                _ = sysWrite(p.fd, msg_buf[0..total]) catch {};
            }
        }
    }

    /// Connect to peers discovered via peer exchange messages.
    fn connectPendingPeers(self: *RaftNetwork) void {
        const count = self.pending_peer_count;
        if (count == 0) return;
        // Copy and reset
        var ports: [MAX_PEERS]u16 = undefined;
        @memcpy(ports[0..count], self.pending_peer_ports[0..count]);
        self.pending_peer_count = 0;

        for (ports[0..count]) |port| {
            if (port == 0 or port == self.listen_port) continue;
            // Check if already connected to a peer on this port
            var already_connected = false;
            for (&self.peers) |*p| {
                if (p.active and p.raft_port == port) {
                    already_connected = true;
                    break;
                }
            }
            if (already_connected) continue;
            // Use a short timeout for connect to avoid blocking the loop
            self.connectToPeerNonBlocking("127.0.0.1", port);
        }
    }

    /// Connect to a peer with a brief timeout. Used by the network loop
    /// for mesh connections discovered via peer exchange.
    fn connectToPeerNonBlocking(self: *RaftNetwork, host: []const u8, port: u16) void {
        const addr = parseAddress(host, port) catch return;
        const fd = sysSocket(posix.AF.INET, posix.SOCK.STREAM, 0) catch return;

        sysConnect(fd, addr.anyPtr(), addr.anyLen()) catch {
            sysClose(fd);
            return;
        };

        // Send join request
        var payload: [JOIN_REQ_SIZE]u8 = undefined;
        std.mem.writeInt(u32, payload[0..4], self.node_id, .little);
        std.mem.writeInt(u16, payload[4..6], self.listen_port, .little);
        std.mem.writeInt(u16, payload[6..8], self.main_port, .little);

        var msg_buf: [HEADER_SIZE + JOIN_REQ_SIZE]u8 = undefined;
        _ = frameCustomMessage(MSG_JOIN_REQUEST, 0, self.node_id, &payload, &msg_buf);
        _ = sysWrite(fd, &msg_buf) catch {
            sysClose(fd);
            return;
        };

        // Read join response (short timeout since it's local)
        var resp_buf: [HEADER_SIZE + JOIN_RESP_SIZE]u8 = undefined;
        const n = readWithTimeout(fd, resp_buf[0 .. HEADER_SIZE + JOIN_RESP_SIZE], 1000) catch {
            sysClose(fd);
            return;
        };
        if (n < HEADER_SIZE) {
            sysClose(fd);
            return;
        }

        const resp_hdr = RaftHeader.fromBytes(resp_buf[0..HEADER_SIZE]);
        if (resp_hdr.msg_type != MSG_JOIN_RESPONSE) {
            sysClose(fd);
            return;
        }

        const peer_node_id = std.mem.readInt(u32, resp_buf[HEADER_SIZE..][0..4], .little);

        // Double-check: don't add duplicate
        if (self.hasPeer(peer_node_id)) {
            sysClose(fd);
            return;
        }

        setNonBlocking(fd) catch {
            sysClose(fd);
            return;
        };
        self.addPeerByFd(peer_node_id, fd, port);
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

    fn addPeerByFd(self: *RaftNetwork, peer_node_id: u32, fd: posix.socket_t, raft_port: u16) void {
        for (&self.peers) |*slot| {
            if (!slot.active) {
                slot.* = .{
                    .active = true,
                    .node_id = peer_node_id,
                    .fd = fd,
                    .raft_port = raft_port,
                    .recv_buf = undefined,
                    .recv_len = 0,
                };
                self.peer_count += 1;
                return;
            }
        }
        // No free slots
        sysClose(fd);
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

/// Generate a deterministic node_id from a port number. Never returns 0.
pub fn generateNodeId(port: u16) u32 {
    var buf: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{port}) catch return 1;
    const h: u32 = @truncate(std.hash.Wyhash.hash(0, s));
    return if (h == 0) 1 else h;
}

fn parseAddress(host: []const u8, port: u16) !SocketAddr {
    var ip4: [4]u8 = .{ 127, 0, 0, 1 };
    if (host.len > 0 and !std.mem.eql(u8, host, "localhost")) {
        var parts = std.mem.splitScalar(u8, host, '.');
        var idx: usize = 0;
        while (parts.next()) |part| {
            if (idx >= 4) return error.InvalidAddress;
            ip4[idx] = std.fmt.parseInt(u8, part, 10) catch return error.InvalidAddress;
            idx += 1;
        }
        if (idx != 4) return error.InvalidAddress;
    }
    return SocketAddr.initIp4(ip4, port);
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
    // F_GETFL=3, F_SETFL=4, O_NONBLOCK=0x0004 on macOS
    const F_GETFL: i32 = 3;
    const F_SETFL: i32 = 4;
    const O_NONBLOCK: c_int = 0x0004;
    const current = std.c.fcntl(fd, F_GETFL, @as(c_int, 0));
    if (current < 0) return error.FcntlFailed;
    if (std.c.fcntl(fd, F_SETFL, current | O_NONBLOCK) < 0) return error.FcntlFailed;
}

fn readWithTimeout(fd: posix.socket_t, buf: []u8, timeout_ms: i32) !usize {
    var fds = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.IN, .revents = 0 }};
    const ready = try posix.poll(&fds, timeout_ms);
    if (ready == 0) return error.Timeout;
    return posix.read(fd, buf);
}

fn readExact(fd: posix.socket_t, buf: []u8) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        var fds = [_]posix.pollfd{.{ .fd = fd, .events = posix.POLL.IN, .revents = 0 }};
        const ready = try posix.poll(&fds, 5000);
        if (ready == 0) return error.Timeout;
        const n = try posix.read(fd, buf[total..]);
        if (n == 0) return error.ConnectionClosed;
        total += n;
    }
    return total;
}

// ── Tests ────────────────────────────────────────────────────────────

const testing = std.testing;

test "raft network: generateNodeId is deterministic" {
    const id1 = generateNodeId(9000);
    const id2 = generateNodeId(9000);
    try testing.expectEqual(id1, id2);
    try testing.expect(id1 != 0);
}

test "raft network: generateNodeId different ports" {
    const id1 = generateNodeId(9000);
    const id2 = generateNodeId(9001);
    try testing.expect(id1 != id2);
}

test "raft network: parseAddress" {
    const addr = try parseAddress("127.0.0.1", 9500);
    try testing.expectEqual(@as(u16, 9500), std.mem.bigToNative(u16, addr.sa.port));
}

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
