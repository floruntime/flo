//! Shard — the fundamental unit of execution
//!
//! Each shard is a CPU-pinned thread that owns:
//!
//! - **Reactor**: unified kqueue/io_uring event loop
//! - **Inbox**: MPSC ring for cross-shard messages
//! - **Dispatcher**: opcode → handler routing table
//! - **ConnectionPool**: active connections (fd → Connection)
//! - **Router**: hash → partition → shard mapping
//! - **SlabAllocator**: fixed-size slab pools for cross-shard payloads
//!
//! ## Lifecycle
//!
//! ```
//! init() → run() → [reactor loop: poll → dispatch] → shutdown()
//! ```
//!
//! The reactor loop alternates between:
//! 1. Polling for I/O events (client reads, timer fires, etc.)
//! 2. Draining the inbox (cross-shard messages)
//! 3. Dispatching requests to handlers via the Dispatcher
//!
//! ## Connection Hand-Off
//!
//! The Acceptor writes connection fds to the shard's pipe. The Reactor
//! sees the pipe become readable, reads the fd, creates a Connection,
//! and adds it to the pool.

const std = @import("std");
const Reactor = @import("reactor.zig").Reactor;
const Inbox = @import("inbox.zig").Inbox;
const InboxMessage = @import("inbox.zig").Message;
const Dispatcher = @import("dispatcher.zig").Dispatcher;
const Connection = @import("connection.zig").Connection;
const RingBuffer = @import("connection.zig").RingBuffer;
const Router = @import("router.zig").Router;
const SlabAllocator = @import("slab.zig").SlabAllocator;
const proto = @import("../protocol/proto.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Shard
// ═══════════════════════════════════════════════════════════════════════════════

pub const Shard = struct {
    /// Shard index (0..shard_count-1).
    id: u16,

    /// Allocator for shard-owned resources.
    allocator: std.mem.Allocator,

    /// Unified event loop.
    reactor: Reactor,

    /// Cross-shard message inbox.
    inbox: Inbox,

    /// Opcode → handler routing table.
    dispatcher: Dispatcher,

    /// Active connections: fd → Connection.
    connections: std.AutoHashMapUnmanaged(i32, *Connection),

    /// Partition router.
    router: Router,

    /// Per-shard slab allocator.
    slab: SlabAllocator,

    /// Pipe read-end fd — receives hand-offs from Acceptor.
    acceptor_pipe_rd: i32,

    /// Whether the shard is running.
    running: bool,

    /// Next connection ID.
    next_conn_id: u32,

    /// Total requests dispatched (stats).
    requests_dispatched: u64,

    /// Total inbox messages processed (stats).
    inbox_messages_processed: u64,

    pub fn init(
        allocator: std.mem.Allocator,
        shard_id: u16,
        shard_count: u16,
        partition_count: u32,
        acceptor_pipe_rd: i32,
    ) !Shard {
        var reactor = try Reactor.init(allocator);
        errdefer reactor.deinit();

        var slab = try SlabAllocator.init(allocator);
        errdefer slab.deinit();

        var inbox = try Inbox.init(allocator, 1024);
        errdefer inbox.deinit();

        return .{
            .id = shard_id,
            .allocator = allocator,
            .reactor = reactor,
            .inbox = inbox,
            .dispatcher = Dispatcher.init(),
            .connections = .{},
            .router = Router.init(partition_count, shard_count, shard_id),
            .slab = slab,
            .acceptor_pipe_rd = acceptor_pipe_rd,
            .running = false,
            .next_conn_id = 1,
            .requests_dispatched = 0,
            .inbox_messages_processed = 0,
        };
    }

    pub fn deinit(self: *Shard) void {
        // Close all connections
        var it = self.connections.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.connections.deinit(self.allocator);

        self.inbox.deinit();
        self.slab.deinit();
        self.reactor.deinit();
    }

    // ─── Connection management ───────────────────────────────────────────

    /// Add a new connection from an accepted fd.
    pub fn addConnection(self: *Shard, fd: i32) !*Connection {
        const conn = try self.allocator.create(Connection);
        conn.* = try Connection.init(self.allocator, fd, self.next_conn_id);
        self.next_conn_id += 1;

        try self.connections.put(self.allocator, fd, conn);
        return conn;
    }

    /// Remove and clean up a connection.
    pub fn removeConnection(self: *Shard, fd: i32) void {
        if (self.connections.fetchRemove(fd)) |kv| {
            kv.value.deinit();
            self.allocator.destroy(kv.value);
        }
    }

    /// Get a connection by fd.
    pub fn getConnection(self: *Shard, fd: i32) ?*Connection {
        return self.connections.get(fd);
    }

    // ─── Dispatch ────────────────────────────────────────────────────────

    /// Dispatch a parsed request through the Dispatcher.
    pub fn dispatchRequest(self: *Shard, conn: *Connection, req: proto.Request) void {
        conn.recordRequest();
        self.requests_dispatched += 1;
        self.dispatcher.dispatch(@ptrCast(self), @ptrCast(conn), req);
    }

    // ─── Inbox draining ──────────────────────────────────────────────────

    /// Drain pending inbox messages (called each reactor tick).
    pub fn drainInbox(self: *Shard) usize {
        var buf: [64]InboxMessage = undefined;
        var total: usize = 0;

        while (true) {
            const count = self.inbox.drain(&buf);
            if (count == 0) break;
            for (buf[0..count]) |msg| {
                self.handleInboxMessage(msg);
                total += 1;
            }
        }

        self.inbox_messages_processed += total;
        return total;
    }

    fn handleInboxMessage(self: *Shard, msg: InboxMessage) void {
        switch (msg.tag) {
            .shutdown => {
                self.running = false;
            },
            else => {
                // Other message types will be handled in later phases
            },
        }
    }

    // ─── Event loop ──────────────────────────────────────────────────────

    /// Run one iteration of the event loop (for testing).
    pub fn tick(self: *Shard, timeout_ms: u32) !usize {
        var events: [64]Reactor.Event = undefined;
        const n = try self.reactor.poll(&events, timeout_ms);

        // Drain inbox each tick
        _ = self.drainInbox();

        return n;
    }

    /// Enter the main reactor loop. Blocks until `shutdown()` is called.
    pub fn run(self: *Shard) !void {
        self.running = true;

        while (self.running) {
            _ = try self.tick(100); // 100ms poll timeout
        }
    }

    /// Signal the shard to stop.
    pub fn shutdown(self: *Shard) void {
        self.running = false;
    }

    // ─── Stats ───────────────────────────────────────────────────────────

    pub fn connectionCount(self: *const Shard) u32 {
        return self.connections.count();
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Shard: init and deinit" {
    const pipe_fds = try std.posix.pipe();
    defer std.posix.close(pipe_fds[0]);
    defer std.posix.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0]);
    defer shard.deinit();

    try std.testing.expectEqual(@as(u16, 0), shard.id);
    try std.testing.expectEqual(@as(u32, 0), shard.connectionCount());
    try std.testing.expect(!shard.running);
}

test "Shard: add and remove connections" {
    const pipe_fds = try std.posix.pipe();
    defer std.posix.close(pipe_fds[0]);
    defer std.posix.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0]);
    defer shard.deinit();

    // Create test pipes to use as fake connection fds
    const conn_pipe = try std.posix.pipe();
    defer std.posix.close(conn_pipe[0]);
    defer std.posix.close(conn_pipe[1]);

    const conn = try shard.addConnection(conn_pipe[0]);
    try std.testing.expectEqual(@as(u32, 1), shard.connectionCount());
    try std.testing.expectEqual(@as(u32, 1), conn.id);

    shard.removeConnection(conn_pipe[0]);
    try std.testing.expectEqual(@as(u32, 0), shard.connectionCount());
}

test "Shard: dispatch ping via pipe-based connection" {
    const pipe_fds = try std.posix.pipe();
    defer std.posix.close(pipe_fds[0]);
    defer std.posix.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0]);
    defer shard.deinit();

    // Track dispatched pings
    const PingTracker = struct {
        var ping_count: u32 = 0;

        fn handlePing(_: *anyopaque, _: *anyopaque, _: proto.Request) void {
            ping_count += 1;
        }
    };
    PingTracker.ping_count = 0;

    // Register ping handler
    shard.dispatcher.register(.ping, PingTracker.handlePing);

    // Create a fake connection
    const conn_pipe = try std.posix.pipe();
    defer std.posix.close(conn_pipe[0]);
    defer std.posix.close(conn_pipe[1]);

    const conn = try shard.addConnection(conn_pipe[0]);

    // Build a ping request
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.op_code = @intFromEnum(proto.OpCode.ping);
    header.request_id = 1;
    const req = proto.Request{
        .header = header,
        .namespace = "",
        .key = "",
        .value = "",
    };

    // Dispatch it
    shard.dispatchRequest(conn, req);

    try std.testing.expectEqual(@as(u32, 1), PingTracker.ping_count);
    try std.testing.expectEqual(@as(u64, 1), shard.requests_dispatched);
    try std.testing.expectEqual(@as(u64, 1), conn.requests_total);
}

test "Shard: inbox shutdown message" {
    const pipe_fds = try std.posix.pipe();
    defer std.posix.close(pipe_fds[0]);
    defer std.posix.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0]);
    defer shard.deinit();

    shard.running = true;
    try std.testing.expect(shard.running);

    // Send shutdown via inbox
    const sent = shard.inbox.send(.{
        .tag = .shutdown,
        .src_shard = 1,
        .partition_id = 0,
        .payload_len = 0,
        .sequence = 0,
        .payload_ptr = null,
        ._padding = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
    });
    try std.testing.expect(sent);

    // Drain inbox
    const processed = shard.drainInbox();
    try std.testing.expectEqual(@as(usize, 1), processed);
    try std.testing.expect(!shard.running);
}
