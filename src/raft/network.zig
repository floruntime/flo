//! Raft peer network: the links between cluster nodes.
//!
//! One thread owns every socket. It accepts, dials, runs the handshake,
//! frames inbound bytes and hands whole frames to the shard through a
//! bounded queue, and drains a bounded outbound queue per peer. It never
//! blocks on a socket: a peer that stops reading is dropped, a peer that
//! stops answering is re-dialled with backoff, and the shard thread never
//! waits for any of it.
//!
//! ## Topology
//!
//! Nodes form a full mesh via peer exchange. When a new node joins the
//! seed, the seed tells it about all existing peers (and vice versa).
//! Each node then connects directly to every other node, so the cluster
//! survives seed failure.
//!
//! A peer is reached at the address it advertised in its hello — its
//! `[server] bind` address when that names an interface, otherwise the
//! source address the accepting side observed.
//!
//! ## Trust
//!
//! The raft port moves terms, membership and log contents, so a connection
//! is a peer only after both sides have proved the cluster secret
//! (`handshake.zig`). Every frame on a peer link is checked (`framer.zig`)
//! and its source id must be the id the link proved; any violation closes
//! the link.

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const stdx = @import("stdx");
const log = stdx.log;
const transport = @import("transport.zig");
const handshake = @import("handshake.zig");
const framer_mod = @import("framer.zig");
const raft_queue_mod = @import("raft_queue.zig");
const ReplicationMetrics = @import("../metrics/registry.zig").ReplicationMetrics;

const RaftHeader = transport.RaftHeader;
const HEADER_SIZE = transport.HEADER_SIZE;
const Framer = framer_mod.Framer;
const RaftQueue = raft_queue_mod.RaftQueue;
const stdx_net = stdx.net;

const MsgType = transport.MsgType;

const sysSocket = stdx_net.sysSocket;
const sysClose = stdx_net.sysClose;
const sysBind = stdx_net.sysBind;
const sysListen = stdx_net.sysListen;
const SocketAddr = stdx_net.SocketAddrV4;

fn sysAccept(fd: posix.socket_t, addr: ?*posix.sockaddr, len: ?*posix.socklen_t) !posix.socket_t {
    return stdx_net.sysAccept(fd, addr, len, 0);
}

/// Write what the kernel will take. `null` when the socket is gone.
fn sysWriteSome(fd: posix.socket_t, buf: []const u8) ?usize {
    const rc = std.c.write(fd, buf.ptr, buf.len);
    if (rc >= 0) return @intCast(rc);
    return switch (posix.errno(rc)) {
        .AGAIN, .INTR => 0,
        else => null,
    };
}

pub const MAX_PEERS = 7;
/// Connections mid-handshake, dialled or accepted. A stranger that opens
/// connections and never speaks holds a slot only until the deadline.
pub const MAX_LINKS = 16;
/// Accepted connections may hold this many of them; the rest are kept for
/// this node's own dials, so a flood cannot stop it re-forming its mesh.
pub const MAX_ACCEPTED_LINKS = 12;
/// Addresses worth re-dialling: every member seen plus the seeds.
pub const MAX_KNOWN = 32;
pub const TICK_INTERVAL_MS = 10;
pub const HANDSHAKE_DEADLINE_MS: i64 = 2000;
/// Bytes queued for one peer before it is dropped as too slow and
/// re-dialled. What it missed shows on it as a replication gap.
pub const SEND_QUEUE_CAP: usize = 8 * 1024 * 1024;
pub const DIAL_BACKOFF_MIN_MS: i64 = 200;
pub const DIAL_BACKOFF_MAX_MS: i64 = 5000;
/// After this many consecutive failed dials the entry is said out loud once.
pub const DIAL_WARN_AFTER: u32 = 10;
pub const RAFT_QUEUE_CAPACITY: usize = 1024;
/// A warn a flood could repeat per connection is said at most once per
/// interval, with a count of what went unsaid.
pub const WARN_INTERVAL_MS: i64 = 1000;

pub const PEER_INFO_SIZE: usize = 10;

/// What one node tells another about a third: id and where to dial it.
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

/// Bytes waiting for one peer's socket. Bounded: a peer that does not
/// read is dropped rather than buffered without limit.
const SendQueue = struct {
    buf: std.ArrayListUnmanaged(u8) = .empty,
    head: usize = 0,

    fn deinit(self: *SendQueue, allocator: Allocator) void {
        self.buf.deinit(allocator);
    }

    fn pending(self: *const SendQueue) []const u8 {
        return self.buf.items[self.head..];
    }

    fn append(self: *SendQueue, allocator: Allocator, bytes: []const u8) error{ Overflow, OutOfMemory }!void {
        if (self.pending().len + bytes.len > SEND_QUEUE_CAP) return error.Overflow;
        if (self.head > 0 and self.head >= self.buf.items.len / 2) {
            std.mem.copyForwards(u8, self.buf.items, self.buf.items[self.head..]);
            self.buf.shrinkRetainingCapacity(self.buf.items.len - self.head);
            self.head = 0;
        }
        try self.buf.appendSlice(allocator, bytes);
    }

    /// Write what the kernel takes. False when the socket is gone.
    fn flush(self: *SendQueue, fd: posix.socket_t) bool {
        while (self.pending().len > 0) {
            const n = sysWriteSome(fd, self.pending()) orelse return false;
            if (n == 0) return true;
            self.head += n;
        }
        self.buf.clearRetainingCapacity();
        self.head = 0;
        return true;
    }
};

/// A frame the shard queued for one peer, already framed.
const Outbound = struct {
    peer_id: u32,
    frame: []u8,
};

pub const PeerState = struct {
    active: bool = false,
    node_id: u32 = 0,
    fd: posix.socket_t = -1,
    ip4: [4]u8 = .{ 0, 0, 0, 0 },
    raft_port: u16 = 0,
    /// Who dialled this link; decides which of two crossing links survives.
    dialed_by_me: bool = false,
    framer: ?Framer = null,
    out: SendQueue = .{},
};

const Role = enum { dialer, acceptor };
const Stage = enum { connecting, hello_awaited, hello_back_awaited, verify_awaited, welcome_awaited };

/// A connection between accept-or-connect and peer: the handshake in
/// progress, driven by poll, with a deadline.
const Link = struct {
    active: bool = false,
    fd: posix.socket_t = -1,
    role: Role = .dialer,
    stage: Stage = .connecting,
    deadline_ms: i64 = 0,
    my_nonce: [handshake.NONCE_LEN]u8 = undefined,
    /// The other side's hello once it has arrived.
    their: ?handshake.Hello = null,
    /// Dialer: the address dialled (also the known-table key). Acceptor:
    /// the address the peer will be recorded at, once its hello is in.
    ip4: [4]u8 = .{ 0, 0, 0, 0 },
    raft_port: u16 = 0,
    /// Acceptor: where the connection came from.
    observed_ip4: [4]u8 = .{ 0, 0, 0, 0 },
    in: [LINK_BUF]u8 = undefined,
    in_len: usize = 0,
    out: [LINK_BUF]u8 = undefined,
    out_head: usize = 0,
    out_len: usize = 0,

    const LINK_BUF = 2 * (HEADER_SIZE + handshake.HELLO_BACK_SIZE);
};

/// An address worth dialling while no link to it is up: a seed the
/// operator named (id unknown until it answers) or a member met before.
const Known = struct {
    active: bool = false,
    node_id: u32 = 0,
    ip4: [4]u8 = .{ 0, 0, 0, 0 },
    raft_port: u16 = 0,
    seed: bool = false,
    dialing: bool = false,
    next_dial_ms: i64 = 0,
    backoff_ms: i64 = DIAL_BACKOFF_MIN_MS,
    failures: u32 = 0,
    warned: bool = false,
};

pub const RaftNetwork = struct {
    allocator: Allocator,
    node_id: u32,
    listen_port: u16,
    main_port: u16,
    advertise_ip4: [4]u8,
    listener_fd: posix.socket_t,
    secret: []u8,
    peers: [MAX_PEERS]PeerState,
    peer_count: u8,
    links: [MAX_LINKS]Link,
    known: [MAX_KNOWN]Known,
    running: std.atomic.Value(bool),
    thread: ?std.Thread,
    /// Guards `outbound`, frames the shard thread has queued for peers.
    /// Taken on the shard thread, so it is never held across anything that
    /// waits.
    mutex: stdx.Mutex,
    outbound: std.ArrayListUnmanaged(Outbound),
    /// The ids of the peers with a link up, one slot per peer slot, written
    /// by the loop thread; the shard reads them without a lock to know whom
    /// it can reach. A slot is 0 while its peer slot is empty.
    linked_ids: [MAX_PEERS]std.atomic.Value(u32),
    /// Guards `dial_requests`, seeds handed over by the runtime.
    dial_mutex: stdx.Mutex,
    dial_requests: std.ArrayListUnmanaged(PeerInfo),
    /// The loop sleeps in poll; a queued frame or a dial request writes a
    /// byte here so it does not wait out the tick.
    wake_rd: posix.fd_t,
    wake_wr: posix.fd_t,
    /// Where whole frames from peers go. Null only in tests without a shard.
    raft_queue: ?*RaftQueue,
    /// While the shard's queue is above its high watermark no peer is read;
    /// the kernel's buffers fill and the peers' writes stall — backpressure
    /// all the way back to the sender.
    reads_paused: bool,
    /// One 4 MiB frame buffer for outbound framing, reused.
    scratch: []u8,
    repl_metrics: ?*ReplicationMetrics = null,
    refuse_warn: WarnLimiter = .{},
    fail_warn: WarnLimiter = .{},

    // Counters, written by the loop thread, read by anyone.
    handshake_failures: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    frames_rejected: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    peer_disconnects: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    slow_peer_drops: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Frames queued for a peer that had no link up when the loop reached
    /// them. Raft resends whatever mattered.
    unlinked_drops: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// `bind_ip4`: listener address; advertised to peers unless 0.0.0.0.
    pub fn init(allocator: Allocator, node_id: u32, listen_port: u16, main_port: u16, bind_ip4: [4]u8, secret: []const u8) !RaftNetwork {
        const fd = try sysSocket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
        errdefer sysClose(fd);

        const opt_val: i32 = 1;
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&opt_val));

        const addr = SocketAddr.initIp4(bind_ip4, listen_port);
        try sysBind(fd, addr.anyPtr(), addr.anyLen());
        try sysListen(fd, 16);
        // Port 0 asks the kernel for one; peers must be told the real one.
        const bound_port = if (listen_port == 0) (try stdx_net.sysLocalIp4(fd)).port else listen_port;

        const wake_pipe = try stdx.io.pipe();
        errdefer {
            _ = std.c.close(wake_pipe[0]);
            _ = std.c.close(wake_pipe[1]);
        }
        try stdx_net.sysFcntlSetNonblocking(wake_pipe[0]);
        try stdx_net.sysFcntlSetNonblocking(wake_pipe[1]);

        const scratch = try allocator.alloc(u8, framer_mod.MAX_FRAME_SIZE);
        errdefer allocator.free(scratch);
        const owned_secret = try allocator.dupe(u8, secret);
        errdefer allocator.free(owned_secret);

        return .{
            .allocator = allocator,
            .node_id = node_id,
            .listen_port = bound_port,
            .main_port = main_port,
            .advertise_ip4 = bind_ip4,
            .listener_fd = fd,
            .secret = owned_secret,
            .peers = [_]PeerState{.{}} ** MAX_PEERS,
            .peer_count = 0,
            .links = [_]Link{.{}} ** MAX_LINKS,
            .known = [_]Known{.{}} ** MAX_KNOWN,
            .running = std.atomic.Value(bool).init(false),
            .thread = null,
            .mutex = .{},
            .outbound = .empty,
            .linked_ids = [_]std.atomic.Value(u32){std.atomic.Value(u32).init(0)} ** MAX_PEERS,
            .dial_mutex = .{},
            .dial_requests = .empty,
            .wake_rd = wake_pipe[0],
            .wake_wr = wake_pipe[1],
            .raft_queue = null,
            .reads_paused = false,
            .scratch = scratch,
        };
    }

    pub fn setReplicationMetrics(self: *RaftNetwork, m: *ReplicationMetrics) void {
        self.repl_metrics = m;
    }

    pub fn deinit(self: *RaftNetwork) void {
        self.stop();
        @memset(self.secret, 0);
        sysClose(self.listener_fd);
        for (self.outbound.items) |o| self.allocator.free(o.frame);
        self.outbound.deinit(self.allocator);
        self.dial_requests.deinit(self.allocator);
        for (&self.peers) |*p| self.closePeer(p);
        for (&self.links) |*l| if (l.active) {
            sysClose(l.fd);
            l.active = false;
        };
        _ = std.c.close(self.wake_rd);
        _ = std.c.close(self.wake_wr);
        self.allocator.free(self.scratch);
        self.allocator.free(self.secret);
    }

    /// Frames from peers go here. Set before `start`.
    pub fn setRaftQueue(self: *RaftNetwork, q: *RaftQueue) void {
        self.raft_queue = q;
    }

    /// Ask the loop thread to join the cluster through this address. Only
    /// that thread touches the peer table, and startup does not wait.
    pub fn dialSeed(self: *RaftNetwork, ip4: [4]u8, port: u16) void {
        {
            self.dial_mutex.lock();
            defer self.dial_mutex.unlock();
            self.dial_requests.append(self.allocator, .{ .node_id = 0, .ip4 = ip4, .raft_port = port }) catch return;
        }
        self.wake();
    }

    /// Queue one frame for a peer. Thread-safe: framed here on the caller's
    /// thread, written by the loop thread. A peer with no link up when the
    /// loop reaches it drops the frame, counted; Raft resends whatever
    /// mattered. False when the payload does not fit a frame or memory is
    /// short.
    pub fn sendTo(self: *RaftNetwork, peer_id: u32, msg_type: MsgType, group_id: u32, payload: []const u8) bool {
        if (payload.len > transport.MAX_PAYLOAD_SIZE) return false;
        const buf = self.allocator.alloc(u8, HEADER_SIZE + payload.len) catch return false;
        const total = transport.frameMessage(msg_type, group_id, self.node_id, payload, buf);
        if (total == 0) {
            self.allocator.free(buf);
            return false;
        }
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.outbound.append(self.allocator, .{ .peer_id = peer_id, .frame = buf }) catch {
                self.allocator.free(buf);
                return false;
            };
        }
        self.wake();
        return true;
    }

    /// Whether a proven link to `node_id` is up right now; what `sendTo`
    /// queues for a peer that is not is dropped at the next flush.
    pub fn isLinked(self: *const RaftNetwork, node_id: u32) bool {
        for (&self.linked_ids) |*slot| if (slot.load(.acquire) == node_id) return true;
        return false;
    }

    /// The ids of the peers with a link up right now, as the loop thread
    /// last published them.
    pub fn linkedPeers(self: *const RaftNetwork, out: *[MAX_PEERS]u32) []u32 {
        var n: usize = 0;
        for (&self.linked_ids) |*slot| {
            const id = slot.load(.acquire);
            if (id == 0) continue;
            out[n] = id;
            n += 1;
        }
        return out[0..n];
    }

    fn wake(self: *RaftNetwork) void {
        const byte = [_]u8{1};
        _ = std.c.write(self.wake_wr, &byte, 1);
    }

    pub fn start(self: *RaftNetwork) !void {
        self.running.store(true, .release);
        self.thread = try std.Thread.spawn(.{}, networkLoop, .{self});
    }

    pub fn stop(self: *RaftNetwork) void {
        if (!self.running.load(.acquire)) return;
        self.running.store(false, .release);
        self.wake();
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    // ── The loop ─────────────────────────────────────────────────────────

    fn networkLoop(self: *RaftNetwork) void {
        var fds: [2 + MAX_LINKS + MAX_PEERS]posix.pollfd = undefined;
        while (self.running.load(.acquire)) {
            self.takeDialRequests();
            self.dialDue();
            self.applyBackpressure();
            if (!self.reads_paused) {
                // Frames left in a framer when reading paused are not
                // reported by poll; drain them before asking for more.
                for (&self.peers) |*p| {
                    if (p.active and p.framer.?.len > 0) self.drainFramer(p);
                    if (self.reads_paused) break;
                }
            }

            var n: usize = 0;
            fds[n] = .{ .fd = self.listener_fd, .events = posix.POLL.IN, .revents = 0 };
            n += 1;
            fds[n] = .{ .fd = self.wake_rd, .events = posix.POLL.IN, .revents = 0 };
            n += 1;
            const links_at = n;
            for (&self.links) |*l| {
                if (!l.active) continue;
                var ev: i16 = posix.POLL.IN;
                if (l.stage == .connecting or l.out_len > l.out_head) ev |= posix.POLL.OUT;
                fds[n] = .{ .fd = l.fd, .events = ev, .revents = 0 };
                n += 1;
            }
            const peers_at = n;
            for (&self.peers) |*p| {
                if (!p.active) continue;
                var ev: i16 = if (self.reads_paused) 0 else posix.POLL.IN;
                if (p.out.pending().len > 0) ev |= posix.POLL.OUT;
                // A paused peer with nothing to write is left out: poll
                // would report its hang-up every call and spin.
                if (ev == 0) continue;
                fds[n] = .{ .fd = p.fd, .events = ev, .revents = 0 };
                n += 1;
            }

            _ = posix.poll(fds[0..n], TICK_INTERVAL_MS) catch 0;

            // Results are matched to links and peers by descriptor, within
            // the range each was polled in: stepping a link can promote it
            // to a peer, and a slot freed here may be refilled before the
            // pass is over. New connections are accepted last for the same
            // reason.
            for (fds[links_at..peers_at]) |pf| {
                if (pf.revents == 0) continue;
                const l = self.linkByFd(pf.fd) orelse continue;
                self.stepLink(l, pf.revents);
            }
            for (fds[peers_at..n]) |pf| {
                if (pf.revents == 0) continue;
                const p = self.peerByFd(pf.fd) orelse continue;
                if (pf.revents & (posix.POLL.ERR | posix.POLL.NVAL) != 0) {
                    self.dropPeer(p, "socket error");
                    continue;
                }
                if (pf.revents & posix.POLL.IN != 0) self.readPeer(p);
                if (p.active and pf.revents & posix.POLL.OUT != 0) {
                    if (!p.out.flush(p.fd)) self.dropPeer(p, "write failed");
                }
                if (p.active and pf.revents & posix.POLL.HUP != 0 and p.framer.?.len == 0) self.dropPeer(p, "peer closed");
            }
            if (fds[1].revents != 0) self.drainWake();
            if (fds[0].revents != 0) self.acceptPending();

            self.flushOutbound();
            self.expireLinks();
        }
    }

    fn linkByFd(self: *RaftNetwork, fd: posix.socket_t) ?*Link {
        for (&self.links) |*l| if (l.active and l.fd == fd) return l;
        return null;
    }

    fn peerByFd(self: *RaftNetwork, fd: posix.socket_t) ?*PeerState {
        for (&self.peers) |*p| if (p.active and p.fd == fd) return p;
        return null;
    }

    fn drainWake(self: *RaftNetwork) void {
        var buf: [64]u8 = undefined;
        while (std.c.read(self.wake_rd, &buf, buf.len) > 0) {}
    }

    fn applyBackpressure(self: *RaftNetwork) void {
        const q = self.raft_queue orelse return;
        if (!self.reads_paused and q.aboveHigh()) {
            self.pauseReads();
        } else if (self.reads_paused and q.belowLow()) {
            self.reads_paused = false;
            log.info("raft: shard queue drained; reading peers again", .{});
        }
    }

    fn pauseReads(self: *RaftNetwork) void {
        self.reads_paused = true;
        log.warn("raft: shard queue above its high watermark; not reading peers until it drains", .{});
    }

    // ── Accept and dial ──────────────────────────────────────────────────

    fn acceptPending(self: *RaftNetwork) void {
        while (true) {
            var client_addr: posix.sockaddr.storage = std.mem.zeroes(posix.sockaddr.storage);
            var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.storage);
            const fd = sysAccept(self.listener_fd, @ptrCast(&client_addr), &addr_len) catch return;
            stdx_net.sysFcntlSetNonblocking(fd) catch {
                sysClose(fd);
                continue;
            };
            const observed = stdx_net.ip4FromSockaddr(@ptrCast(&client_addr)) orelse {
                sysClose(fd);
                continue;
            };
            const maybe: ?*Link = if (self.acceptedLinks() < MAX_ACCEPTED_LINKS) self.freeLink() else null;
            const l = maybe orelse {
                if (self.refuse_warn.due()) |unsaid| {
                    if (unsaid == 0) {
                        log.warn("raft: too many connections mid-handshake; refusing one from {d}.{d}.{d}.{d}", .{ observed[0], observed[1], observed[2], observed[3] });
                    } else {
                        log.warn("raft: too many connections mid-handshake; refusing one from {d}.{d}.{d}.{d} ({d} more refused since the last line)", .{ observed[0], observed[1], observed[2], observed[3], unsaid });
                    }
                }
                sysClose(fd);
                continue;
            };
            l.* = .{ .active = true, .fd = fd, .role = .acceptor, .stage = .hello_awaited, .deadline_ms = stdx.time.milliTimestamp() + HANDSHAKE_DEADLINE_MS, .observed_ip4 = observed };
            stdx.io.instance().randomSecure(&l.my_nonce) catch {
                self.endLink(l, "no entropy for a nonce");
            };
        }
    }

    fn takeDialRequests(self: *RaftNetwork) void {
        self.dial_mutex.lock();
        defer self.dial_mutex.unlock();
        for (self.dial_requests.items) |req| self.noteKnown(req.node_id, req.ip4, req.raft_port, true);
        self.dial_requests.clearRetainingCapacity();
    }

    /// Remember an address to keep a link to. Members are keyed by id; a
    /// seed (id 0, named by the operator) by address until it answers and
    /// its id is learned, when the entry becomes the member's. One id has
    /// one address and one address one id: a node replaced at the same
    /// address under a new id, or moved to a new address, retires what was
    /// known before, or the old entry would be dialled forever.
    fn noteKnown(self: *RaftNetwork, node_id: u32, ip4: [4]u8, port: u16, seed: bool) void {
        if (node_id == self.node_id) return;
        var free: ?*Known = null;
        var keep: ?*Known = null;
        var was_seed = seed;
        for (&self.known) |*k| {
            if (!k.active) {
                if (free == null) free = k;
                continue;
            }
            const same_addr = k.raft_port == port and std.mem.eql(u8, &k.ip4, &ip4);
            const same_id = node_id != 0 and k.node_id == node_id;
            if (node_id == 0) {
                if (same_addr) {
                    k.seed = k.seed or seed;
                    return;
                }
                continue;
            }
            if (same_id or same_addr) {
                was_seed = was_seed or k.seed;
                if (keep == null) {
                    keep = k;
                } else {
                    k.active = false;
                }
            }
        }
        if (keep) |k| {
            const moved = k.raft_port != port or !std.mem.eql(u8, &k.ip4, &ip4);
            k.node_id = node_id;
            k.ip4 = ip4;
            k.raft_port = port;
            k.seed = was_seed;
            if (moved) k.dialing = false;
            return;
        }
        const k = free orelse {
            log.warn("raft: known-peer table full ({d}); not remembering {d}.{d}.{d}.{d}:{d}", .{ MAX_KNOWN, ip4[0], ip4[1], ip4[2], ip4[3], port });
            return;
        };
        k.* = .{ .active = true, .node_id = node_id, .ip4 = ip4, .raft_port = port, .seed = seed };
    }

    fn forgetKnown(self: *RaftNetwork, ip4: [4]u8, port: u16) void {
        for (&self.known) |*k| {
            if (k.active and k.raft_port == port and std.mem.eql(u8, &k.ip4, &ip4)) k.active = false;
        }
    }

    fn knownByAddress(self: *RaftNetwork, ip4: [4]u8, port: u16) ?*Known {
        for (&self.known) |*k| {
            if (k.active and k.raft_port == port and std.mem.eql(u8, &k.ip4, &ip4)) return k;
        }
        return null;
    }

    /// Start a non-blocking dial to every known address that has no link
    /// and is due. Both sides dial: a restarted node remembers nobody, so
    /// waiting to be dialled could wait forever. Two links that cross are
    /// settled by `linkWins`, the same way on both nodes.
    fn dialDue(self: *RaftNetwork) void {
        const now = stdx.time.milliTimestamp();
        for (&self.known) |*k| {
            if (!k.active or k.dialing or k.next_dial_ms > now) continue;
            if (k.node_id != 0) {
                if (self.hasPeer(k.node_id)) continue;
            } else if (self.peerByAddress(k.ip4, k.raft_port) != null) continue;
            self.startDial(k);
        }
    }

    /// Of two links to the same peer, the one dialled by the lower node id
    /// survives; both nodes reach the same answer, so they keep the same
    /// link instead of each dropping the one the other kept.
    fn linkWins(self: *RaftNetwork, new_dialed_by_me: bool, held: *const PeerState) bool {
        const new_dialer = if (new_dialed_by_me) self.node_id else held.node_id;
        const held_dialer = if (held.dialed_by_me) self.node_id else held.node_id;
        return new_dialer < held_dialer;
    }

    fn startDial(self: *RaftNetwork, k: *Known) void {
        const l = self.freeLink() orelse return;
        const fd = sysSocket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0) catch {
            self.dialFailed(k, "socket");
            return;
        };
        const addr = SocketAddr.initIp4(k.ip4, k.raft_port);
        if (std.c.connect(fd, addr.anyPtr(), addr.anyLen()) != 0) {
            const e = posix.errno(@as(isize, -1));
            if (e != .INPROGRESS and e != .AGAIN) {
                sysClose(fd);
                self.dialFailed(k, "connect");
                return;
            }
        }
        l.* = .{ .active = true, .fd = fd, .role = .dialer, .stage = .connecting, .deadline_ms = stdx.time.milliTimestamp() + HANDSHAKE_DEADLINE_MS, .ip4 = k.ip4, .raft_port = k.raft_port };
        stdx.io.instance().randomSecure(&l.my_nonce) catch {
            self.endLink(l, "no entropy for a nonce");
            return;
        };
        k.dialing = true;
    }

    fn dialFailed(self: *RaftNetwork, k: *Known, why: []const u8) void {
        _ = self;
        k.dialing = false;
        k.failures += 1;
        k.next_dial_ms = stdx.time.milliTimestamp() + k.backoff_ms;
        k.backoff_ms = @min(k.backoff_ms * 2, DIAL_BACKOFF_MAX_MS);
        if (k.failures >= DIAL_WARN_AFTER and !k.warned) {
            k.warned = true;
            if (k.seed) {
                log.warn("cluster: could not join seed {d}.{d}.{d}.{d}:{d} after {d} attempts ({s}); still trying every {d} ms", .{ k.ip4[0], k.ip4[1], k.ip4[2], k.ip4[3], k.raft_port, k.failures, why, k.backoff_ms });
            } else {
                log.warn("raft: peer {d} at {d}.{d}.{d}.{d}:{d} unreachable after {d} attempts ({s}); still trying every {d} ms", .{ k.node_id, k.ip4[0], k.ip4[1], k.ip4[2], k.ip4[3], k.raft_port, k.failures, why, k.backoff_ms });
            }
        }
    }

    fn dialSucceeded(self: *RaftNetwork, ip4: [4]u8, port: u16) void {
        const k = self.knownByAddress(ip4, port) orelse return;
        k.dialing = false;
        k.failures = 0;
        k.backoff_ms = DIAL_BACKOFF_MIN_MS;
        k.warned = false;
        k.next_dial_ms = stdx.time.milliTimestamp() + DIAL_BACKOFF_MIN_MS;
    }

    fn freeLink(self: *RaftNetwork) ?*Link {
        for (&self.links) |*l| if (!l.active) return l;
        return null;
    }

    fn acceptedLinks(self: *const RaftNetwork) usize {
        var n: usize = 0;
        for (&self.links) |*l| if (l.active and l.role == .acceptor) {
            n += 1;
        };
        return n;
    }

    // ── Handshake ────────────────────────────────────────────────────────

    fn stepLink(self: *RaftNetwork, l: *Link, revents: i16) void {
        if (revents & (posix.POLL.ERR | posix.POLL.NVAL) != 0) {
            self.endLink(l, "socket error");
            return;
        }
        if (l.stage == .connecting) {
            if (revents & (posix.POLL.OUT | posix.POLL.HUP) == 0) return;
            var err: c_int = 0;
            var len: posix.socklen_t = @sizeOf(c_int);
            if (std.c.getsockopt(l.fd, posix.SOL.SOCKET, posix.SO.ERROR, @ptrCast(&err), &len) != 0 or err != 0) {
                self.endLink(l, "connect refused");
                return;
            }
            // Our own listener, reached by one of our addresses (the usual
            // seed list names every node, this one included). A connection
            // to ourselves has our address at both ends.
            if (l.raft_port == self.listen_port) {
                if (stdx_net.sysLocalIp4(l.fd)) |local| {
                    if (std.mem.eql(u8, &local.ip4, &l.ip4)) {
                        log.debug("raft: {d}.{d}.{d}.{d}:{d} is this node", .{ l.ip4[0], l.ip4[1], l.ip4[2], l.ip4[3], l.raft_port });
                        self.forgetKnown(l.ip4, l.raft_port);
                        self.closeLink(l);
                        return;
                    }
                } else |_| {}
            }
            self.queueHello(l, .hello, null);
            l.stage = .hello_back_awaited;
            _ = self.writeLink(l);
            return;
        }
        if (revents & posix.POLL.OUT != 0) {
            if (!self.writeLink(l)) return;
        }
        if (revents & posix.POLL.IN != 0) {
            const rc = std.c.read(l.fd, l.in[l.in_len..].ptr, l.in.len - l.in_len);
            if (rc == 0) {
                self.hungUp(l);
                return;
            }
            if (rc < 0) {
                if (posix.errno(rc) != .AGAIN) self.endLink(l, "read failed during handshake");
                return;
            }
            l.in_len += @intCast(rc);
            self.parseLink(l);
        } else if (revents & posix.POLL.HUP != 0) {
            self.hungUp(l);
        }
    }

    /// The other side closed mid-handshake. After we sent our proof that
    /// means it did not accept it: the dialer checks first and hangs up,
    /// so this is what a wrong secret looks like from the accepting side.
    fn hungUp(self: *RaftNetwork, l: *Link) void {
        if (l.role == .acceptor and l.stage == .verify_awaited) {
            self.failLink(l, "the dialer did not accept our proof: wrong cluster secret on one side, or a stranger");
        } else {
            self.endLink(l, "closed during handshake");
        }
    }

    /// One handshake frame, if a whole one is in; the link's buffer holds at
    /// most two, and nothing else is ever valid here.
    fn parseLink(self: *RaftNetwork, l: *Link) void {
        while (l.active and l.in_len >= HEADER_SIZE) {
            const hdr = RaftHeader.fromBytes(l.in[0..HEADER_SIZE]).*;
            const size = HEADER_SIZE + @as(usize, hdr.payload_len);
            if (size > l.in.len) {
                self.failLink(l, "oversize handshake frame");
                return;
            }
            if (l.in_len < size) return;
            const payload = l.in[HEADER_SIZE..size];
            if (transport.computeCrc(l.in[0..HEADER_SIZE], payload) != hdr.crc32) {
                self.failLink(l, "bad checksum in handshake");
                return;
            }
            self.handleLinkFrame(l, hdr, payload);
            if (!l.active) return;
            std.mem.copyForwards(u8, &l.in, l.in[size..l.in_len]);
            l.in_len -= size;
        }
        if (l.active and l.in_len == l.in.len) self.failLink(l, "handshake frame does not fit");
    }

    fn handleLinkFrame(self: *RaftNetwork, l: *Link, hdr: RaftHeader, payload: []const u8) void {
        switch (l.stage) {
            .hello_awaited => {
                // Acceptor: the dialer says who it is.
                if (hdr.msgType() != .hello or payload.len != handshake.Hello.SIZE) return self.failLink(l, "expected hello");
                const hello = handshake.Hello.decode(payload[0..handshake.Hello.SIZE]);
                if (hello.version != handshake.VERSION) return self.failLink(l, "protocol version mismatch");
                if (hdr.source_node != hello.node_id) return self.failLink(l, "hello id does not match its frame");
                if (hello.node_id == self.node_id) {
                    // Our own seed list names us, or another node carries
                    // our id. Say so, then close.
                    self.queueWelcome(l, .rejected_same_id);
                    _ = self.writeLink(l);
                    self.closeLink(l);
                    return;
                }
                l.ip4 = peerAddressFor(hello.ip4, l.observed_ip4) orelse {
                    if (self.fail_warn.due()) |_| {
                        log.warn("raft: rejected join from node {d}: advertised address {d}.{d}.{d}.{d} is not dialable", .{ hello.node_id, hello.ip4[0], hello.ip4[1], hello.ip4[2], hello.ip4[3] });
                    }
                    self.closeLink(l);
                    return;
                };
                l.raft_port = hello.raft_port;
                l.their = hello;
                const mac = handshake.proof(self.secret, &hello.nonce, &l.my_nonce, self.node_id, hello.node_id);
                self.queueHello(l, .hello_back, &mac);
                l.stage = .verify_awaited;
                _ = self.writeLink(l);
            },
            .hello_back_awaited => {
                // The acceptor answers a hello that carries its own id
                // before proving anything; the dialer must understand that.
                if (hdr.msgType() == .welcome and payload.len == handshake.Welcome.SIZE) {
                    const w = handshake.Welcome.decode(payload[0..handshake.Welcome.SIZE]) orelse return self.failLink(l, "unknown verdict");
                    // Said before any proof, so a stranger could say it; the
                    // address is kept and dialled again at the longest
                    // backoff, and the warn says what it might mean.
                    if (w.verdict == .rejected_same_id) return self.sameIdRefused(l, false);
                    return self.failLink(l, "verdict before proof");
                }
                // Dialer: the acceptor proves itself first.
                if (hdr.msgType() != .hello_back or payload.len != handshake.HELLO_BACK_SIZE) return self.failLink(l, "expected hello back");
                const hello = handshake.Hello.decode(payload[0..handshake.Hello.SIZE]);
                const their_mac = payload[handshake.Hello.SIZE..][0..handshake.MAC_LEN];
                if (hello.version != handshake.VERSION) return self.failLink(l, "protocol version mismatch");
                if (hdr.source_node != hello.node_id) return self.failLink(l, "hello id does not match its frame");
                // Our own id coming back is our own listener reached through
                // a stranger relaying both ways, or a twin; never a peer.
                if (hello.node_id == self.node_id) return self.failLink(l, "hello back carries our own id");
                const expected = handshake.proof(self.secret, &l.my_nonce, &hello.nonce, hello.node_id, self.node_id);
                if (!handshake.proofMatches(&expected, their_mac)) return self.failLink(l, "wrong cluster secret");
                l.their = hello;
                const mine = handshake.proof(self.secret, &hello.nonce, &l.my_nonce, self.node_id, hello.node_id);
                self.queueFrame(l, .verify, &mine);
                l.stage = .welcome_awaited;
                _ = self.writeLink(l);
            },
            .verify_awaited => {
                // Acceptor: the dialer's proof, then our verdict.
                if (hdr.msgType() != .verify or payload.len != handshake.VERIFY_SIZE) return self.failLink(l, "expected verify");
                const their = l.their.?;
                const expected = handshake.proof(self.secret, &l.my_nonce, &their.nonce, their.node_id, self.node_id);
                if (!handshake.proofMatches(&expected, payload[0..handshake.MAC_LEN])) return self.failLink(l, "wrong cluster secret");
                if (self.peerByNodeId(their.node_id)) |held| {
                    if (held.raft_port != their.raft_port or !std.mem.eql(u8, &held.ip4, &l.ip4)) {
                        log.warn("raft: node {d} joined from {d}.{d}.{d}.{d}:{d} while linked at {d}.{d}.{d}.{d}:{d} — two nodes may share an id (set [cluster] node_id)", .{ their.node_id, l.ip4[0], l.ip4[1], l.ip4[2], l.ip4[3], their.raft_port, held.ip4[0], held.ip4[1], held.ip4[2], held.ip4[3], held.raft_port });
                    }
                    // A dial that timed out leaves its connection in our
                    // backlog and we may accept that one first: a live link
                    // wins over a duplicate. Two links that crossed are
                    // settled by who dialled; the loser is told so.
                    if (linkAlive(held.fd) and !self.linkWins(false, held)) {
                        self.queueWelcome(l, .rejected_live_link);
                        _ = self.writeLink(l);
                        self.closeLink(l);
                        // A link whose far end died without a FIN peeks as
                        // alive until something is written to it. Write
                        // something: a restarted far end answers with a
                        // reset, and the next dial finds the link gone. Port
                        // 0 makes it a no-op to a live receiver, which must
                        // not learn our bind address as a dial target.
                        self.sendPeerInfo(held, .{ .node_id = self.node_id, .ip4 = self.advertise_ip4, .raft_port = 0 });
                        if (!held.out.flush(held.fd)) self.dropPeer(held, "write failed");
                        return;
                    }
                    self.dropPeer(held, "replaced by a new link");
                }
                if (self.peer_count >= MAX_PEERS) {
                    log.warn("raft: no free peer slot for node {d} (at most {d} peers)", .{ their.node_id, MAX_PEERS });
                    self.queueWelcome(l, .rejected_full);
                    _ = self.writeLink(l);
                    self.closeLink(l);
                    return;
                }
                self.queueWelcome(l, .accepted);
                _ = self.writeLink(l);
                if (!l.active) return;
                self.promote(l, their.node_id, l.ip4, their.raft_port);
            },
            .welcome_awaited => {
                if (hdr.msgType() != .welcome or payload.len != handshake.Welcome.SIZE) return self.failLink(l, "expected welcome");
                const w = handshake.Welcome.decode(payload[0..handshake.Welcome.SIZE]) orelse return self.failLink(l, "unknown verdict");
                const their = l.their.?;
                switch (w.verdict) {
                    .accepted => {
                        self.dialSucceeded(l.ip4, l.raft_port);
                        self.promote(l, their.node_id, l.ip4, l.raft_port);
                    },
                    .rejected_live_link => {
                        // The far side may hold a link to us that died
                        // without a FIN; it will notice on its next write.
                        const k = self.knownByAddress(l.ip4, l.raft_port);
                        if (k != null and !k.?.warned) {
                            k.?.warned = true;
                            log.warn("raft: {d}.{d}.{d}.{d}:{d} refused our join: a live link already carries our node id {d} — it may still hold a stale link to us, or another node shares our id (set [cluster] node_id)", .{ l.ip4[0], l.ip4[1], l.ip4[2], l.ip4[3], l.raft_port, self.node_id });
                        }
                        self.endLink(l, "refused: live link");
                    },
                    .rejected_same_id => self.sameIdRefused(l, true),
                    .rejected_full => self.endLink(l, "refused: peer table full"),
                }
            },
            .connecting => self.failLink(l, "bytes before connect completed"),
        }
    }

    /// `proven`: the verdict came after the far side proved the secret, so
    /// it is a member's word and the address is forgotten; otherwise it is
    /// anyone's word and the address only backs off.
    fn sameIdRefused(self: *RaftNetwork, l: *Link, proven: bool) void {
        const k = self.knownByAddress(l.ip4, l.raft_port);
        if (k == null or !k.?.warned) {
            log.warn("raft: {d}.{d}.{d}.{d}:{d} says it carries our own node id {d} — either we dialed ourselves by an address we could not recognise, or another node shares our id (set [cluster] node_id)", .{ l.ip4[0], l.ip4[1], l.ip4[2], l.ip4[3], l.raft_port, self.node_id });
        }
        self.closeLink(l);
        if (proven) {
            self.forgetKnown(l.ip4, l.raft_port);
        } else if (k) |entry| {
            entry.dialing = false;
            entry.failures += 1;
            entry.warned = true;
            entry.backoff_ms = DIAL_BACKOFF_MAX_MS;
            entry.next_dial_ms = stdx.time.milliTimestamp() + DIAL_BACKOFF_MAX_MS;
        }
    }

    fn queueHello(self: *RaftNetwork, l: *Link, msg_type: MsgType, mac: ?*const [handshake.MAC_LEN]u8) void {
        var payload: [handshake.HELLO_BACK_SIZE]u8 = undefined;
        (handshake.Hello{
            .version = handshake.VERSION,
            .node_id = self.node_id,
            .raft_port = self.listen_port,
            .main_port = self.main_port,
            .ip4 = self.advertise_ip4,
            .nonce = l.my_nonce,
        }).encode(payload[0..handshake.Hello.SIZE]);
        if (mac) |m| {
            payload[handshake.Hello.SIZE..][0..handshake.MAC_LEN].* = m.*;
            self.queueFrame(l, msg_type, &payload);
        } else {
            self.queueFrame(l, msg_type, payload[0..handshake.Hello.SIZE]);
        }
    }

    fn queueWelcome(self: *RaftNetwork, l: *Link, verdict: handshake.Verdict) void {
        var payload: [handshake.Welcome.SIZE]u8 = undefined;
        (handshake.Welcome{ .verdict = verdict, .node_id = self.node_id }).encode(&payload);
        self.queueFrame(l, .welcome, &payload);
    }

    fn queueFrame(self: *RaftNetwork, l: *Link, msg_type: MsgType, payload: []const u8) void {
        if (l.out_head == l.out_len) {
            l.out_head = 0;
            l.out_len = 0;
        }
        const n = transport.frameMessage(msg_type, 0, self.node_id, payload, l.out[l.out_len..]);
        l.out_len += n;
    }

    /// False when the socket is gone (the link is ended).
    fn writeLink(self: *RaftNetwork, l: *Link) bool {
        while (l.out_head < l.out_len) {
            const n = sysWriteSome(l.fd, l.out[l.out_head..l.out_len]) orelse {
                self.endLink(l, "write failed during handshake");
                return false;
            };
            if (n == 0) return true;
            l.out_head += n;
        }
        return true;
    }

    /// The handshake reached its verdict: the socket becomes a peer, with
    /// whatever handshake bytes are still unwritten carried over.
    fn promote(self: *RaftNetwork, l: *Link, node_id: u32, ip4: [4]u8, raft_port: u16) void {
        const dialed_by_me = l.role == .dialer;
        if (self.peerByNodeId(node_id)) |held| {
            if (linkAlive(held.fd) and !self.linkWins(dialed_by_me, held)) {
                // Our own dial crossed theirs and theirs is the keeper.
                self.closeLink(l);
                if (dialed_by_me) {
                    if (self.knownByAddress(ip4, raft_port)) |k| k.dialing = false;
                }
                return;
            }
            self.dropPeer(held, "replaced by a new link");
        }
        const slot = blk: {
            for (&self.peers) |*p| if (!p.active) break :blk p;
            self.endLink(l, "no peer slot");
            return;
        };
        const fr = Framer.init(self.allocator) catch {
            self.endLink(l, "out of memory");
            return;
        };
        slot.* = .{ .active = true, .node_id = node_id, .fd = l.fd, .ip4 = ip4, .raft_port = raft_port, .dialed_by_me = dialed_by_me, .framer = fr, .out = .{} };
        if (l.out_head < l.out_len) {
            slot.out.append(self.allocator, l.out[l.out_head..l.out_len]) catch {
                self.endLink(l, "out of memory");
                slot.active = false;
                if (slot.framer) |*f| f.deinit();
                slot.framer = null;
                return;
            };
        }
        // A peer that dies without a FIN is otherwise noticed only by the
        // next write to it; in an idle cluster that could be never.
        const on: c_int = 1;
        _ = std.c.setsockopt(l.fd, posix.SOL.SOCKET, posix.SO.KEEPALIVE, @ptrCast(&on), @sizeOf(c_int));
        // The OS default idle is hours; probe after fifteen seconds.
        const idle: c_int = 15;
        const builtin = @import("builtin");
        const idle_opt: ?u32 = switch (builtin.os.tag) {
            .macos, .ios => 0x10, // TCP_KEEPALIVE
            .linux => 4, // TCP_KEEPIDLE
            else => null,
        };
        if (idle_opt) |opt| _ = std.c.setsockopt(l.fd, posix.IPPROTO.TCP, opt, @ptrCast(&idle), @sizeOf(c_int));
        self.peer_count += 1;
        if (self.peerSlot(slot)) |i| self.linked_ids[i].store(node_id, .release);
        if (self.repl_metrics) |m| m.setPeersLinked(self.peer_count);
        l.active = false;
        if (l.role == .dialer) {
            if (self.knownByAddress(ip4, raft_port)) |k| k.dialing = false;
        }
        self.noteKnown(node_id, ip4, raft_port, false);
        log.info("raft: peer {d} connected at {d}.{d}.{d}.{d}:{d}", .{ node_id, ip4[0], ip4[1], ip4[2], ip4[3], raft_port });

        // Peer exchange: the newcomer learns of everyone, everyone of it.
        for (&self.peers) |*p| {
            if (!p.active or p == slot or p.raft_port == 0) continue;
            self.sendPeerInfo(slot, .{ .node_id = p.node_id, .ip4 = p.ip4, .raft_port = p.raft_port });
            self.sendPeerInfo(p, .{ .node_id = node_id, .ip4 = ip4, .raft_port = raft_port });
        }
        if (!slot.out.flush(slot.fd)) self.dropPeer(slot, "write failed");
    }

    fn sendPeerInfo(self: *RaftNetwork, p: *PeerState, info: PeerInfo) void {
        var payload: [PEER_INFO_SIZE]u8 = undefined;
        info.encode(&payload);
        var buf: [HEADER_SIZE + PEER_INFO_SIZE]u8 = undefined;
        const total = transport.frameMessage(.peer_info, 0, self.node_id, &payload, &buf);
        self.enqueueTo(p, buf[0..total]);
    }

    /// A handshake that failed the checks: a stranger, a wrong secret, a
    /// broken client. One warn, counted.
    fn failLink(self: *RaftNetwork, l: *Link, why: []const u8) void {
        _ = self.handshake_failures.fetchAdd(1, .monotonic);
        if (self.repl_metrics) |m| m.recordHandshakeFailure();
        if (self.fail_warn.due()) |unsaid| {
            const who = if (l.role == .acceptor) l.observed_ip4 else l.ip4;
            if (l.role == .acceptor) {
                log.warn("raft: handshake with a dialer from {d}.{d}.{d}.{d} failed: {s}{s}", .{ who[0], who[1], who[2], who[3], why, if (unsaid > 0) " (and more since the last line)" else "" });
            } else {
                log.warn("raft: handshake with {d}.{d}.{d}.{d}:{d} failed: {s}{s}", .{ who[0], who[1], who[2], who[3], l.raft_port, why, if (unsaid > 0) " (and more since the last line)" else "" });
            }
        }
        self.closeLink(l);
        if (l.role == .dialer) {
            if (self.knownByAddress(l.ip4, l.raft_port)) |k| self.dialFailed(k, why);
        }
    }

    /// A handshake that ended for a reason that is not a check failing:
    /// timeout, refusal, a socket error. Counted for the dial backoff.
    fn endLink(self: *RaftNetwork, l: *Link, why: []const u8) void {
        log.debug("raft: handshake ended ({s})", .{why});
        self.closeLink(l);
        if (l.role == .dialer) {
            if (self.knownByAddress(l.ip4, l.raft_port)) |k| self.dialFailed(k, why);
        }
    }

    fn closeLink(self: *RaftNetwork, l: *Link) void {
        _ = self;
        if (!l.active) return;
        sysClose(l.fd);
        l.active = false;
    }

    fn expireLinks(self: *RaftNetwork) void {
        const now = stdx.time.milliTimestamp();
        for (&self.links) |*l| {
            if (l.active and now > l.deadline_ms) self.endLink(l, "handshake timed out");
        }
    }

    // ── Peer links ───────────────────────────────────────────────────────

    fn readPeer(self: *RaftNetwork, p: *PeerState) void {
        const fr = &p.framer.?;
        const space = fr.space();
        if (space.len > 0) {
            const rc = std.c.read(p.fd, space.ptr, space.len);
            if (rc == 0) {
                self.dropPeer(p, "peer closed");
                return;
            }
            if (rc < 0) {
                if (posix.errno(rc) != .AGAIN) self.dropPeer(p, "read failed");
                return;
            }
            fr.commit(@intCast(rc));
        }
        self.drainFramer(p);
    }

    /// Hand on every whole frame in the peer's buffer. Stops, leaving the
    /// rest buffered, as soon as the shard's queue is above its watermark:
    /// one read can hold thousands of small frames, and a pause decided
    /// only between reads would come after they were already dropped.
    fn drainFramer(self: *RaftNetwork, p: *PeerState) void {
        const fr = &p.framer.?;
        while (p.active) {
            if (self.raft_queue) |q| {
                if (!self.reads_paused and q.aboveHigh()) self.pauseReads();
                if (self.reads_paused) return;
            }
            const frame = fr.next(p.node_id) catch |err| {
                _ = self.frames_rejected.fetchAdd(1, .monotonic);
                if (self.repl_metrics) |m| m.recordFrameRejected();
                log.warn("raft: frame from peer {d} rejected ({s}); dropping the link", .{ p.node_id, @errorName(err) });
                self.dropPeer(p, "bad frame");
                return;
            } orelse break;
            self.handleFrame(p, frame);
            // Handling can drop the peer, and with it the buffer the
            // frame points into.
            if (!p.active) return;
            fr.advance();
        }
    }

    fn handleFrame(self: *RaftNetwork, p: *PeerState, frame: framer_mod.Frame) void {
        switch (frame.msg_type) {
            .append_entries, .append_entries_response, .request_vote, .request_vote_response, .install_snapshot, .forward_write, .forward_reply, .join_request => self.deliver(p, frame),
            .peer_info => {
                if (frame.payload.len < PEER_INFO_SIZE) return;
                const info = PeerInfo.decode(frame.payload[0..PEER_INFO_SIZE]);
                if (info.node_id != self.node_id and info.raft_port > 0 and stdx_net.isUnicastPeerAddress(info.ip4)) {
                    self.noteKnown(info.node_id, info.ip4, info.raft_port, false);
                }
            },
            // The handshake types have no place on an established link.
            .hello, .hello_back, .verify, .welcome => {
                log.warn("raft: peer {d} sent a {s} frame, which this link does not carry; dropping the link", .{ p.node_id, @tagName(frame.msg_type) });
                self.rejectFrame(p, "unexpected frame");
            },
        }
    }

    fn rejectFrame(self: *RaftNetwork, p: *PeerState, why: []const u8) void {
        _ = self.frames_rejected.fetchAdd(1, .monotonic);
        if (self.repl_metrics) |m| m.recordFrameRejected();
        self.dropPeer(p, why);
    }

    /// Hand a frame to the shard under the id its link proved. A full queue
    /// drops it and counts; Raft resends whatever mattered.
    fn deliver(self: *RaftNetwork, p: *const PeerState, frame: framer_mod.Frame) void {
        const q = self.raft_queue orelse return;
        const dup = self.allocator.dupe(u8, frame.payload) catch {
            if (self.repl_metrics) |m| m.recordFrameDropped();
            return;
        };
        const ok = q.push(.{ .source_node = p.node_id, .group_id = frame.header.group_id, .msg_type = frame.msg_type, .payload = dup });
        if (!ok) {
            self.allocator.free(dup);
            if (self.repl_metrics) |m| m.recordFrameDropped();
        }
    }

    /// Everything the shard queued since the last pass goes to its peer's
    /// socket queue, then every socket is written as far as the kernel takes.
    fn flushOutbound(self: *RaftNetwork) void {
        self.mutex.lock();
        var to_send = self.outbound;
        self.outbound = .empty;
        self.mutex.unlock();
        defer {
            for (to_send.items) |o| self.allocator.free(o.frame);
            to_send.deinit(self.allocator);
        }
        for (to_send.items) |o| {
            const p = self.peerByNodeId(o.peer_id) orelse {
                _ = self.unlinked_drops.fetchAdd(1, .monotonic);
                continue;
            };
            self.enqueueTo(p, o.frame);
        }
        for (&self.peers) |*p| {
            if (p.active and p.out.pending().len > 0) {
                if (!p.out.flush(p.fd)) self.dropPeer(p, "write failed");
            }
        }
    }

    /// Queue bytes for a peer; one SEND_QUEUE_CAP behind is dropped and
    /// re-dialled.
    fn enqueueTo(self: *RaftNetwork, p: *PeerState, bytes: []const u8) void {
        p.out.append(self.allocator, bytes) catch |err| switch (err) {
            error.Overflow => {
                _ = self.slow_peer_drops.fetchAdd(1, .monotonic);
                if (self.repl_metrics) |m| m.recordSlowPeerDrop();
                log.warn("raft: peer {d} has {d} bytes unread; dropping the link, it will be re-dialled", .{ p.node_id, p.out.pending().len });
                self.dropPeer(p, "too slow");
            },
            error.OutOfMemory => self.dropPeer(p, "out of memory"),
        };
    }

    fn dropPeer(self: *RaftNetwork, p: *PeerState, why: []const u8) void {
        if (!p.active) return;
        log.info("raft: peer {d} link down ({s})", .{ p.node_id, why });
        _ = self.peer_disconnects.fetchAdd(1, .monotonic);
        if (self.repl_metrics) |m| m.recordPeerDisconnect();
        self.closePeer(p);
    }

    fn closePeer(self: *RaftNetwork, p: *PeerState) void {
        if (!p.active) return;
        sysClose(p.fd);
        if (p.framer) |*f| f.deinit();
        p.framer = null;
        p.out.deinit(self.allocator);
        p.active = false;
        if (self.peerSlot(p)) |i| self.linked_ids[i].store(0, .release);
        if (self.peer_count > 0) self.peer_count -= 1;
        if (self.repl_metrics) |m| m.setPeersLinked(self.peer_count);
    }

    pub fn peerByAddress(self: *RaftNetwork, ip4: [4]u8, port: u16) ?*const PeerState {
        for (&self.peers) |*p| {
            if (p.active and p.raft_port == port and std.mem.eql(u8, &p.ip4, &ip4)) return p;
        }
        return null;
    }

    /// The index of a peer in the table, or null for a peer state that
    /// lives elsewhere (tests hand in their own).
    fn peerSlot(self: *const RaftNetwork, p: *const PeerState) ?usize {
        const base = @intFromPtr(&self.peers[0]);
        const at = @intFromPtr(p);
        if (at < base) return null;
        const i = (at - base) / @sizeOf(PeerState);
        return if (i < MAX_PEERS) i else null;
    }

    fn peerByNodeId(self: *RaftNetwork, node_id: u32) ?*PeerState {
        for (&self.peers) |*p| {
            if (p.active and p.node_id == node_id) return p;
        }
        return null;
    }

    /// Check if we already have a connection to a peer with the given node_id.
    pub fn hasPeer(self: *RaftNetwork, node_id: u32) bool {
        return self.peerByNodeId(node_id) != null;
    }
};

/// One line per interval; the count of what was suppressed rides along.
const WarnLimiter = struct {
    last_ms: i64 = 0,
    suppressed: u64 = 0,

    /// The number left unsaid since the last line, or null to stay quiet.
    fn due(self: *WarnLimiter) ?u64 {
        const now = stdx.time.milliTimestamp();
        if (now - self.last_ms < WARN_INTERVAL_MS) {
            self.suppressed += 1;
            return null;
        }
        self.last_ms = now;
        const n = self.suppressed;
        self.suppressed = 0;
        return n;
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

// ── Tests ────────────────────────────────────────────────────────────

const testing = std.testing;

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

test "raft network: the listener binds the configured address and advertises it" {
    var rn = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    const local = try stdx_net.sysLocalIp4(rn.listener_fd);
    try testing.expectEqual([4]u8{ 127, 0, 0, 1 }, local.ip4);
    try testing.expectEqual([4]u8{ 127, 0, 0, 1 }, rn.advertise_ip4);
}

test "raft network: the send queue takes what the kernel will and refuses more than the cap" {
    var pair: [2]posix.fd_t = undefined;
    try testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[0]);
    defer _ = std.c.close(pair[1]);
    try stdx_net.sysFcntlSetNonblocking(pair[0]);
    const small: c_int = 4096;
    _ = std.c.setsockopt(pair[0], posix.SOL.SOCKET, posix.SO.SNDBUF, @ptrCast(&small), @sizeOf(c_int));

    var q = SendQueue{};
    defer q.deinit(testing.allocator);
    const chunk = try testing.allocator.alloc(u8, 256 * 1024);
    defer testing.allocator.free(chunk);
    @memset(chunk, 0x5a);
    try q.append(testing.allocator, chunk);
    // The kernel takes some; the rest waits, in order.
    try testing.expect(q.flush(pair[0]));
    const left = q.pending().len;
    try testing.expect(left > 0 and left < chunk.len);
    // Drain the far side and the rest goes.
    var sink: [65536]u8 = undefined;
    var drained: usize = 0;
    while (q.pending().len > 0) {
        const n = std.c.read(pair[1], &sink, sink.len);
        if (n > 0) drained += @intCast(n);
        try testing.expect(q.flush(pair[0]));
    }
    while (drained < chunk.len) {
        const n = std.c.read(pair[1], &sink, sink.len);
        if (n <= 0) break;
        drained += @intCast(n);
    }
    try testing.expectEqual(chunk.len, drained);

    // Over the cap is refused, and the queue keeps what it had.
    const big = try testing.allocator.alloc(u8, SEND_QUEUE_CAP);
    defer testing.allocator.free(big);
    try q.append(testing.allocator, "abc");
    try testing.expectError(error.Overflow, q.append(testing.allocator, big));
    try testing.expectEqualStrings("abc", q.pending());
}

fn boundPort(rn: *const RaftNetwork) !u16 {
    return (try stdx_net.sysLocalIp4(rn.listener_fd)).port;
}

fn waitForPeer(rn: *RaftNetwork, node_id: u32, timeout_ms: u64) bool {
    var waited: u64 = 0;
    while (waited < timeout_ms) : (waited += 20) {
        if (rn.hasPeer(node_id)) return true;
        stdx.time.sleep(20 * std.time.ns_per_ms);
    }
    return false;
}

fn knownCount(rn: *const RaftNetwork) usize {
    var n: usize = 0;
    for (&rn.known) |k| if (k.active) {
        n += 1;
    };
    return n;
}

test "raft network: a node does not join itself" {
    var rn = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    rn.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&rn));
    try rn.start();
    // One dial settles it: the connection has our address at both ends, so
    // the address is forgotten, not retried.
    var waited: u64 = 0;
    while (waited < 3000 and (knownCount(&rn) > 0 or rn.dial_requests.items.len > 0)) : (waited += 20) stdx.time.sleep(20 * std.time.ns_per_ms);
    try testing.expectEqual(@as(usize, 0), knownCount(&rn));
    try testing.expectEqual(@as(u8, 0), rn.peer_count);
}

test "raft network: two nodes with the secret link, learn each other's address, and carry an entry" {
    var seed = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 0, 0, 0, 0 }, "s3cret");
    defer seed.deinit();
    var q = try RaftQueue.init(testing.allocator, 8);
    defer q.deinit();
    seed.setRaftQueue(&q);
    try seed.start();

    var joiner = try RaftNetwork.init(testing.allocator, 2, 0, 9001, .{ 0, 0, 0, 0 }, "s3cret");
    defer joiner.deinit();
    joiner.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&seed));
    try joiner.start();

    try testing.expect(waitForPeer(&seed, 2, 3000));
    try testing.expect(waitForPeer(&joiner, 1, 1000));
    // A joiner advertising 0.0.0.0 is recorded at the address it came from.
    try testing.expectEqual([4]u8{ 127, 0, 0, 1 }, seed.peerByNodeId(2).?.ip4);
    try testing.expectEqual(joiner.listen_port, seed.peerByNodeId(2).?.raft_port);
    try testing.expectEqual(@as(u64, 0), seed.handshake_failures.load(.monotonic));

    var linked: [MAX_PEERS]u32 = undefined;
    try testing.expectEqualSlices(u32, &.{2}, seed.linkedPeers(&linked));

    try testing.expect(joiner.sendTo(1, .append_entries, 0, "entry-bytes"));
    var waited: u64 = 0;
    var got: ?raft_queue_mod.Frame = null;
    while (waited < 3000 and got == null) : (waited += 20) {
        got = q.pop();
        if (got == null) stdx.time.sleep(20 * std.time.ns_per_ms);
    }
    defer if (got) |g| testing.allocator.free(g.payload);
    try testing.expect(got != null);
    try testing.expectEqualStrings("entry-bytes", got.?.payload);
    try testing.expectEqual(@as(u32, 2), got.?.source_node);
    try testing.expectEqual(MsgType.append_entries, got.?.msg_type);
}

test "raft network: a dialer with the wrong secret never becomes a peer, and the failure is counted on both sides" {
    var seed = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "right");
    defer seed.deinit();
    try seed.start();
    var stranger = try RaftNetwork.init(testing.allocator, 2, 0, 9001, .{ 127, 0, 0, 1 }, "wrong");
    defer stranger.deinit();
    stranger.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&seed));
    try stranger.start();

    var waited: u64 = 0;
    while (waited < 3000 and stranger.handshake_failures.load(.monotonic) == 0) : (waited += 20) stdx.time.sleep(20 * std.time.ns_per_ms);
    try testing.expect(stranger.handshake_failures.load(.monotonic) >= 1);
    try testing.expect(!seed.hasPeer(2));
    try testing.expect(!stranger.hasPeer(1));
    try testing.expectEqual(@as(u8, 0), seed.peer_count);
}

test "raft network: a stranger that speaks garbage is dropped at the handshake with one failure" {
    var seed = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer seed.deinit();
    try seed.start();
    const fd = try stdx_net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, try boundPort(&seed), 1000);
    defer _ = std.c.close(fd);
    const junk = [_]u8{0x41} ** 64;
    _ = std.c.write(fd, &junk, junk.len);
    var waited: u64 = 0;
    while (waited < 3000 and seed.handshake_failures.load(.monotonic) == 0) : (waited += 20) stdx.time.sleep(20 * std.time.ns_per_ms);
    try testing.expectEqual(@as(u64, 1), seed.handshake_failures.load(.monotonic));
    try testing.expectEqual(@as(u8, 0), seed.peer_count);
}

test "raft network: a dial that finds nobody home is retried with backoff until the peer answers" {
    // `a` is bound (so the dial connects) but its loop is not running, so the
    // hello is never answered and the handshake times out.
    var a = try RaftNetwork.init(testing.allocator, 2, 0, 9002, .{ 127, 0, 0, 1 }, "s");
    defer a.deinit();
    var b = try RaftNetwork.init(testing.allocator, 1, 0, 9001, .{ 127, 0, 0, 1 }, "s");
    defer b.deinit();
    b.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&a));
    try b.start();
    stdx.time.sleep(2600 * std.time.ns_per_ms);
    try testing.expect(!b.hasPeer(2));
    try testing.expect(b.knownByAddress(.{ 127, 0, 0, 1 }, try boundPort(&a)).?.failures >= 1);
    try a.start();
    try testing.expect(waitForPeer(&b, 2, 8000));
    try testing.expect(waitForPeer(&a, 1, 2000));
    try testing.expectEqual(@as(u32, 0), b.knownByAddress(.{ 127, 0, 0, 1 }, try boundPort(&a)).?.failures);
}

test "raft network: a peer whose link drops is re-dialled and relinks" {
    var seed = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer seed.deinit();
    try seed.start();
    var joiner = try RaftNetwork.init(testing.allocator, 2, 0, 9001, .{ 127, 0, 0, 1 }, "s");
    defer joiner.deinit();
    joiner.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&seed));
    try joiner.start();
    try testing.expect(waitForPeer(&seed, 2, 3000));
    try testing.expect(waitForPeer(&joiner, 1, 1000));

    // The seed's process dies and comes back on the same port.
    const port = try boundPort(&seed);
    seed.deinit();
    stdx.time.sleep(300 * std.time.ns_per_ms);
    try testing.expect(!joiner.hasPeer(1));
    seed = try RaftNetwork.init(testing.allocator, 1, port, 9000, .{ 127, 0, 0, 1 }, "s");
    try seed.start();
    try testing.expect(waitForPeer(&joiner, 1, 8000));
    try testing.expect(waitForPeer(&seed, 2, 2000));
}

test "raft network: a send never waits on a dial, and one to a peer with no link is dropped and counted" {
    // 192.0.2.0/24 is never routed; the dial burns its whole deadline. On a
    // network that rejects it outright the dial ends at once and this test
    // proves nothing either way.
    var rn = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    rn.dialSeed(.{ 192, 0, 2, 1 }, 9500);
    try rn.start();
    stdx.time.sleep(300 * std.time.ns_per_ms);
    var worst_ms: i64 = 0;
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        const t0 = stdx.time.milliTimestamp();
        try testing.expect(rn.sendTo(7, .append_entries, 0, "entry"));
        worst_ms = @max(worst_ms, stdx.time.milliTimestamp() - t0);
        stdx.time.sleep(100 * std.time.ns_per_ms);
    }
    try testing.expect(worst_ms < 200);
    try testing.expectEqual(@as(u64, 5), rn.unlinked_drops.load(.monotonic));
}

test "raft network: peer info round-trips" {
    var pbuf: [PEER_INFO_SIZE]u8 = undefined;
    PeerInfo.encode(.{ .node_id = 9, .ip4 = .{ 10, 0, 1, 13 }, .raft_port = 9500 }, &pbuf);
    const p = PeerInfo.decode(&pbuf);
    try testing.expectEqual(@as(u32, 9), p.node_id);
    try testing.expectEqual([4]u8{ 10, 0, 1, 13 }, p.ip4);
    try testing.expectEqual(@as(u16, 9500), p.raft_port);
}

test "raft network: two nodes that dial each other at once keep exactly one link, and it is stable" {
    var a = try RaftNetwork.init(testing.allocator, 1, 0, 9001, .{ 127, 0, 0, 1 }, "s");
    defer a.deinit();
    var b = try RaftNetwork.init(testing.allocator, 2, 0, 9002, .{ 127, 0, 0, 1 }, "s");
    defer b.deinit();
    a.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&b));
    b.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&a));
    try a.start();
    try b.start();
    try testing.expect(waitForPeer(&a, 2, 3000));
    try testing.expect(waitForPeer(&b, 1, 3000));
    // Let any crossing settle, then hold: one link each, both kept the one
    // dialled by the lower id, and nothing flaps.
    stdx.time.sleep(1500 * std.time.ns_per_ms);
    const disconnects = a.peer_disconnects.load(.monotonic) + b.peer_disconnects.load(.monotonic);
    stdx.time.sleep(1500 * std.time.ns_per_ms);
    try testing.expectEqual(@as(u8, 1), a.peer_count);
    try testing.expectEqual(@as(u8, 1), b.peer_count);
    try testing.expect(a.hasPeer(2) and b.hasPeer(1));
    try testing.expect(a.peerByNodeId(2).?.dialed_by_me);
    try testing.expect(!b.peerByNodeId(1).?.dialed_by_me);
    try testing.expectEqual(disconnects, a.peer_disconnects.load(.monotonic) + b.peer_disconnects.load(.monotonic));
}

test "raft network: a dialer that ignores our proof and sends a wrong one is refused by the acceptor" {
    var seed = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "right");
    defer seed.deinit();
    try seed.start();
    const fd = try stdx_net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, try boundPort(&seed), 1000);
    defer _ = std.c.close(fd);

    // A well-formed hello from node 2 with its own nonce.
    var nonce: [handshake.NONCE_LEN]u8 = undefined;
    @memset(&nonce, 7);
    var hello_payload: [handshake.Hello.SIZE]u8 = undefined;
    (handshake.Hello{ .version = handshake.VERSION, .node_id = 2, .raft_port = 9001, .main_port = 9002, .ip4 = .{ 127, 0, 0, 1 }, .nonce = nonce }).encode(&hello_payload);
    var wire: [256]u8 = undefined;
    const n = transport.frameMessage(.hello, 0, 2, &hello_payload, &wire);
    _ = std.c.write(fd, &wire, n);

    // The acceptor's hello back, with a proof we do not check.
    var back: [HEADER_SIZE + handshake.HELLO_BACK_SIZE]u8 = undefined;
    var got: usize = 0;
    var waited: u64 = 0;
    while (got < back.len and waited < 3000) : (waited += 20) {
        const rc = std.c.read(fd, back[got..].ptr, back.len - got);
        if (rc > 0) got += @intCast(rc) else stdx.time.sleep(20 * std.time.ns_per_ms);
    }
    try testing.expectEqual(back.len, got);
    const their = handshake.Hello.decode(back[HEADER_SIZE..][0..handshake.Hello.SIZE]);

    // A proof under the wrong secret, otherwise correct in every way.
    const bad = handshake.proof("wrong", &their.nonce, &nonce, 2, 1);
    const vn = transport.frameMessage(.verify, 0, 2, &bad, &wire);
    _ = std.c.write(fd, &wire, vn);

    waited = 0;
    while (waited < 3000 and seed.handshake_failures.load(.monotonic) == 0) : (waited += 20) stdx.time.sleep(20 * std.time.ns_per_ms);
    try testing.expectEqual(@as(u64, 1), seed.handshake_failures.load(.monotonic));
    try testing.expect(!seed.hasPeer(2));
    // And the socket is closed on us.
    var byte: [1]u8 = undefined;
    waited = 0;
    var closed = false;
    while (waited < 3000 and !closed) : (waited += 20) {
        const rc = std.c.read(fd, &byte, 1);
        if (rc == 0) closed = true else stdx.time.sleep(20 * std.time.ns_per_ms);
    }
    try testing.expect(closed);
}

test "raft network: of two crossing links the one dialled by the lower id wins, on both nodes" {
    var low = try RaftNetwork.init(testing.allocator, 1, 0, 9001, .{ 127, 0, 0, 1 }, "s");
    defer low.deinit();
    var high = try RaftNetwork.init(testing.allocator, 2, 0, 9002, .{ 127, 0, 0, 1 }, "s");
    defer high.deinit();
    const held_by_low_dialed_by_high = PeerState{ .active = true, .node_id = 2, .dialed_by_me = false };
    const held_by_low_dialed_by_low = PeerState{ .active = true, .node_id = 2, .dialed_by_me = true };
    const held_by_high_dialed_by_low = PeerState{ .active = true, .node_id = 1, .dialed_by_me = false };
    const held_by_high_dialed_by_high = PeerState{ .active = true, .node_id = 1, .dialed_by_me = true };
    // Node 1 holds the link node 2 dialled; its own dial arrives: keep its own.
    try testing.expect(low.linkWins(true, &held_by_low_dialed_by_high));
    // Node 1 holds its own dial; node 2's dial arrives: refuse it.
    try testing.expect(!low.linkWins(false, &held_by_low_dialed_by_low));
    // Node 2 holds node 1's dial; its own dial completes: refuse its own.
    try testing.expect(!high.linkWins(true, &held_by_high_dialed_by_low));
    // Node 2 holds its own dial; node 1's dial arrives: replace with it.
    try testing.expect(high.linkWins(false, &held_by_high_dialed_by_high));
    // A duplicate from the same dialer never replaces a live link.
    try testing.expect(!low.linkWins(false, &held_by_low_dialed_by_high));
    try testing.expect(!high.linkWins(true, &held_by_high_dialed_by_high));
}

fn plainListener() !struct { fd: posix.socket_t, port: u16 } {
    const fd = try sysSocket(posix.AF.INET, posix.SOCK.STREAM, 0);
    const addr = SocketAddr.initIp4(.{ 127, 0, 0, 1 }, 0);
    try sysBind(fd, addr.anyPtr(), addr.anyLen());
    try sysListen(fd, 4);
    return .{ .fd = fd, .port = (try stdx_net.sysLocalIp4(fd)).port };
}

fn readExactly(fd: posix.socket_t, buf: []u8, timeout_ms: u64) !void {
    var got: usize = 0;
    var waited: u64 = 0;
    while (got < buf.len and waited < timeout_ms) : (waited += 10) {
        const rc = std.c.read(fd, buf[got..].ptr, buf.len - got);
        if (rc > 0) got += @intCast(rc) else stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    if (got < buf.len) return error.Timeout;
}

test "raft network: a stranger that relays a node's own proof back to it is refused" {
    // The node dials an address the stranger holds (a reused seed address).
    var a = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer a.deinit();
    const mirror = try plainListener();
    defer _ = std.c.close(mirror.fd);
    a.dialSeed(.{ 127, 0, 0, 1 }, mirror.port);
    try a.start();

    // The stranger takes the dial and reads the hello.
    var addr: posix.sockaddr.storage = std.mem.zeroes(posix.sockaddr.storage);
    var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.storage);
    var dial_fd: posix.socket_t = -1;
    var waited: u64 = 0;
    while (dial_fd < 0 and waited < 3000) : (waited += 10) {
        dial_fd = sysAccept(mirror.fd, @ptrCast(&addr), &addr_len) catch -1;
        if (dial_fd < 0) stdx.time.sleep(10 * std.time.ns_per_ms);
    }
    try testing.expect(dial_fd >= 0);
    defer _ = std.c.close(dial_fd);
    var hello_frame: [HEADER_SIZE + handshake.Hello.SIZE]u8 = undefined;
    try readExactly(dial_fd, &hello_frame, 3000);
    const a_hello = handshake.Hello.decode(hello_frame[HEADER_SIZE..][0..handshake.Hello.SIZE]);

    // It opens a second connection to the node's own listener, claiming
    // some other id but reusing the node's nonce, and gets the node's proof.
    const back_fd = try stdx_net.tcpConnectIp4Timeout(.{ 127, 0, 0, 1 }, try boundPort(&a), 1000);
    defer _ = std.c.close(back_fd);
    var forged: [handshake.Hello.SIZE]u8 = undefined;
    (handshake.Hello{ .version = handshake.VERSION, .node_id = 2, .raft_port = 1, .main_port = 1, .ip4 = .{ 127, 0, 0, 1 }, .nonce = a_hello.nonce }).encode(&forged);
    var wire: [256]u8 = undefined;
    const fn_ = transport.frameMessage(.hello, 0, 2, &forged, &wire);
    _ = std.c.write(back_fd, &wire, fn_);
    var back: [HEADER_SIZE + handshake.HELLO_BACK_SIZE]u8 = undefined;
    try readExactly(back_fd, &back, 3000);

    // Relayed verbatim to the dialling link: the node's own hello-back, its
    // own proof, its own id.
    _ = std.c.write(dial_fd, &back, back.len);
    waited = 0;
    while (waited < 3000 and a.handshake_failures.load(.monotonic) == 0) : (waited += 20) stdx.time.sleep(20 * std.time.ns_per_ms);
    try testing.expect(a.handshake_failures.load(.monotonic) >= 1);
    try testing.expect(!a.hasPeer(1));
    try testing.expectEqual(@as(u8, 0), a.peer_count);
}

test "raft network: three nodes mesh, a frame reaches each peer once, and they stay linked" {
    var a = try RaftNetwork.init(testing.allocator, 1, 0, 9001, .{ 127, 0, 0, 1 }, "s");
    defer a.deinit();
    var b = try RaftNetwork.init(testing.allocator, 2, 0, 9002, .{ 127, 0, 0, 1 }, "s");
    defer b.deinit();
    var c = try RaftNetwork.init(testing.allocator, 3, 0, 9003, .{ 127, 0, 0, 1 }, "s");
    defer c.deinit();
    var qb = try RaftQueue.init(testing.allocator, 16);
    defer qb.deinit();
    var qc = try RaftQueue.init(testing.allocator, 16);
    defer qc.deinit();
    b.setRaftQueue(&qb);
    c.setRaftQueue(&qc);
    try a.start();
    b.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&a));
    try b.start();
    c.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&a));
    try c.start();
    // Peer exchange completes the mesh.
    try testing.expect(waitForPeer(&a, 2, 3000) and waitForPeer(&a, 3, 3000));
    try testing.expect(waitForPeer(&b, 3, 5000) and waitForPeer(&c, 2, 5000));
    stdx.time.sleep(500 * std.time.ns_per_ms);
    const disconnects = a.peer_disconnects.load(.monotonic) + b.peer_disconnects.load(.monotonic) + c.peer_disconnects.load(.monotonic);

    var linked: [MAX_PEERS]u32 = undefined;
    try testing.expectEqual(@as(usize, 2), a.linkedPeers(&linked).len);
    try testing.expect(a.sendTo(2, .append_entries, 0, "from-a"));
    try testing.expect(a.sendTo(3, .append_entries, 0, "from-a"));
    // Each of B and C receives its frame once, under A's id; nothing is
    // rejected, no link drops.
    var waited: u64 = 0;
    while (waited < 3000 and (qb.count() < 1 or qc.count() < 1)) : (waited += 20) stdx.time.sleep(20 * std.time.ns_per_ms);
    stdx.time.sleep(200 * std.time.ns_per_ms);
    try testing.expectEqual(@as(usize, 1), qb.count());
    try testing.expectEqual(@as(usize, 1), qc.count());
    while (qb.pop()) |f| {
        try testing.expectEqual(@as(u32, 1), f.source_node);
        try testing.expectEqualStrings("from-a", f.payload);
        testing.allocator.free(f.payload);
    }
    while (qc.pop()) |f| {
        try testing.expectEqual(@as(u32, 1), f.source_node);
        testing.allocator.free(f.payload);
    }
    stdx.time.sleep(500 * std.time.ns_per_ms);
    try testing.expectEqual(@as(u64, 0), a.frames_rejected.load(.monotonic) + b.frames_rejected.load(.monotonic) + c.frames_rejected.load(.monotonic));
    try testing.expectEqual(disconnects, a.peer_disconnects.load(.monotonic) + b.peer_disconnects.load(.monotonic) + c.peer_disconnects.load(.monotonic));
    try testing.expect(a.hasPeer(2) and a.hasPeer(3) and b.hasPeer(1) and b.hasPeer(3) and c.hasPeer(1) and c.hasPeer(2));
}

test "raft network: the known table keeps one entry per address and per id" {
    var rn = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    const addr: [4]u8 = .{ 10, 0, 0, 5 };
    rn.noteKnown(0, addr, 9500, true);
    try testing.expectEqual(@as(usize, 1), knownCount(&rn));
    // The seed answers as node 5: the seed entry becomes the member's.
    rn.noteKnown(5, addr, 9500, false);
    try testing.expectEqual(@as(usize, 1), knownCount(&rn));
    try testing.expectEqual(@as(u32, 5), rn.knownByAddress(addr, 9500).?.node_id);
    try testing.expect(rn.knownByAddress(addr, 9500).?.seed);
    // Node 5 moves: the old address is forgotten.
    rn.noteKnown(5, .{ 10, 0, 0, 6 }, 9500, false);
    try testing.expectEqual(@as(usize, 1), knownCount(&rn));
    try testing.expect(rn.knownByAddress(addr, 9500) == null);
    // A new node takes the old address: the entry is that node's now, not
    // one more entry that would be dialled and refused forever.
    rn.noteKnown(7, .{ 10, 0, 0, 6 }, 9500, false);
    try testing.expectEqual(@as(usize, 1), knownCount(&rn));
    try testing.expectEqual(@as(u32, 7), rn.knownByAddress(.{ 10, 0, 0, 6 }, 9500).?.node_id);
}

test "raft network: draining a buffer of small frames stops at the shard queue's watermark" {
    var rn = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var q = try RaftQueue.init(testing.allocator, 8);
    defer q.deinit();
    rn.setRaftQueue(&q);
    var p = PeerState{ .active = true, .node_id = 2, .fd = -1, .framer = try Framer.init(testing.allocator) };
    defer p.framer.?.deinit();
    // Twenty frames in one read.
    var i: usize = 0;
    while (i < 20) : (i += 1) {
        const n = transport.frameMessage(.append_entries, 0, 2, "x", p.framer.?.space());
        p.framer.?.commit(n);
    }
    rn.drainFramer(&p);
    // Six is the high watermark of eight; the rest wait in the buffer,
    // and nothing was dropped.
    try testing.expect(rn.reads_paused);
    try testing.expectEqual(@as(usize, 6), q.count());
    try testing.expect(p.framer.?.len > 0);
    try testing.expectEqual(@as(u64, 0), q.dropped.load(.monotonic));
    while (q.pop()) |f| testing.allocator.free(f.payload);
    rn.applyBackpressure();
    try testing.expect(!rn.reads_paused);
    rn.drainFramer(&p);
    try testing.expect(q.count() >= 6);
    while (q.pop()) |f| testing.allocator.free(f.payload);
}

test "raft network: a node that reaches another carrying its own id is told so and backs off" {
    var a = try RaftNetwork.init(testing.allocator, 1, 0, 9001, .{ 127, 0, 0, 1 }, "s");
    defer a.deinit();
    var twin = try RaftNetwork.init(testing.allocator, 1, 0, 9002, .{ 127, 0, 0, 1 }, "s");
    defer twin.deinit();
    try a.start();
    twin.dialSeed(.{ 127, 0, 0, 1 }, try boundPort(&a));
    try twin.start();
    // The request is taken into the known table, dialled, refused with the
    // verdict before any proof: a whole round trip, not a failed handshake,
    // and the address backs off to the longest interval rather than being
    // forgotten on an unproven word.
    const port = try boundPort(&a);
    var waited: u64 = 0;
    while (waited < 3000) : (waited += 20) {
        if (twin.knownByAddress(.{ 127, 0, 0, 1 }, port)) |k| {
            if (k.warned) break;
        }
        stdx.time.sleep(20 * std.time.ns_per_ms);
    }
    const k = twin.knownByAddress(.{ 127, 0, 0, 1 }, port).?;
    try testing.expect(k.warned);
    try testing.expectEqual(DIAL_BACKOFF_MAX_MS, k.backoff_ms);
    try testing.expectEqual(@as(u64, 0), twin.handshake_failures.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), a.handshake_failures.load(.monotonic));
    try testing.expectEqual(@as(u8, 0), a.peer_count);
    try testing.expectEqual(@as(u8, 0), twin.peer_count);
}

test "raft network: a frame this link does not carry drops the peer, with the bytes after it left alone" {
    var rn = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var p = PeerState{ .active = true, .node_id = 2, .fd = -1, .framer = try Framer.init(testing.allocator) };
    // A hello, a type no established link carries, followed by more bytes
    // in the same read.
    var n = transport.frameMessage(.hello, 0, 2, "hello", p.framer.?.space());
    p.framer.?.commit(n);
    n = transport.frameMessage(.peer_info, 0, 2, "0123456789", p.framer.?.space());
    p.framer.?.commit(n);
    rn.drainFramer(&p);
    try testing.expect(!p.active);
    try testing.expect(p.framer == null);
    try testing.expectEqual(@as(u64, 1), rn.frames_rejected.load(.monotonic));
}

test "raft network: peer info naming a non-dialable address is not remembered" {
    var rn = try RaftNetwork.init(testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var p = PeerState{ .active = true, .node_id = 2, .fd = -1, .framer = try Framer.init(testing.allocator) };
    defer p.framer.?.deinit();
    var payload: [PEER_INFO_SIZE]u8 = undefined;
    // What a poke carries: our bind address, port 0.
    PeerInfo.encode(.{ .node_id = 3, .ip4 = .{ 0, 0, 0, 0 }, .raft_port = 0 }, &payload);
    var n = transport.frameMessage(.peer_info, 0, 2, &payload, p.framer.?.space());
    p.framer.?.commit(n);
    // A real port on an address nobody can dial.
    PeerInfo.encode(.{ .node_id = 4, .ip4 = .{ 0, 0, 0, 0 }, .raft_port = 9500 }, &payload);
    n = transport.frameMessage(.peer_info, 0, 2, &payload, p.framer.?.space());
    p.framer.?.commit(n);
    // And one worth dialling.
    PeerInfo.encode(.{ .node_id = 5, .ip4 = .{ 10, 0, 0, 5 }, .raft_port = 9500 }, &payload);
    n = transport.frameMessage(.peer_info, 0, 2, &payload, p.framer.?.space());
    p.framer.?.commit(n);
    rn.drainFramer(&p);
    try testing.expectEqual(@as(usize, 1), knownCount(&rn));
    try testing.expectEqual(@as(u32, 5), rn.knownByAddress(.{ 10, 0, 0, 5 }, 9500).?.node_id);
}
