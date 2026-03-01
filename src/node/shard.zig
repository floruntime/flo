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
//! - **KVProjection + KVHandler**: in-memory KV storage and dispatch
//!
//! ## Lifecycle
//!
//! ```
//! init() → run() → [reactor loop: poll → processEvents → dispatch] → shutdown()
//! ```
//!
//! The reactor loop alternates between:
//! 1. Polling for I/O events (client reads, timer fires, etc.)
//! 2. Processing I/O events (accept, recv, send, close)
//! 3. Draining the inbox (cross-shard messages)
//! 4. Dispatching requests to handlers via the Dispatcher
//!
//! ## Connection Hand-Off
//!
//! The Acceptor writes connection fds to the shard's pipe. The Reactor
//! sees the pipe become readable, reads the fd, creates a Connection,
//! and adds it to the pool.

const std = @import("std");
const posix = std.posix;
const reactor_mod = @import("reactor.zig");
const Reactor = reactor_mod.Reactor;
const ReactorEvent = reactor_mod.Event;
const Tag = reactor_mod.Tag;
const Inbox = @import("inbox.zig").Inbox;
const InboxMessage = @import("inbox.zig").Message;
const Dispatcher = @import("dispatcher.zig").Dispatcher;
const Connection = @import("connection.zig").Connection;
const RingBuffer = @import("connection.zig").RingBuffer;
const Router = @import("router.zig").Router;
const SlabAllocator = @import("slab.zig").SlabAllocator;
const proto = @import("../protocol/proto.zig");
const KVProjection = @import("../projection/kv.zig").KVProjection;
const KVHandler = @import("../kv/handler.zig").KVHandler;
const StreamProjection = @import("../projection/stream.zig").StreamProjection;
const StreamHandler = @import("../stream/handler.zig").StreamHandler;
const QueueProjection = @import("../projection/queue.zig").QueueProjection;
const QueueHandler = @import("../queue/handler.zig").QueueHandler;
const TSProjection = @import("../projection/ts.zig").TSProjection;
const TSHandler = @import("../ts/handler.zig").TSHandler;
const NamespaceHandler = @import("../namespace/handler.zig").NamespaceHandler;
const ActionsHandler = @import("../actions/handler.zig").ActionsHandler;
const ual_mod = @import("../storage/ual/ual.zig");
const UAL = ual_mod.UAL;
const SegmentWriter = @import("../storage/ual/writer.zig").SegmentWriter;
const SegmentReader = @import("../storage/ual/reader.zig").SegmentReader;
const entry_mod = @import("../storage/ual/entry.zig");
const segment_mod = @import("../storage/ual/segment.zig");
const Entry = entry_mod.Entry;
const RaftNetwork = @import("../raft/network.zig").RaftNetwork;
const RaftNode = @import("../raft/node.zig").RaftNode;

/// Maximum single-request size we handle on the stack.
const MAX_REQUEST_SIZE = 256 * 1024; // 256 KB

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

    /// KV projection (heap-allocated, stable pointer).
    kv_projection: *KVProjection,

    /// KV handler instance (heap-allocated, stable pointer).
    kv_handler: *KVHandler,

    /// Stream projection.
    stream_projection: *StreamProjection,

    /// Stream handler instance.
    stream_handler: *StreamHandler,

    /// Queue projection.
    queue_projection: *QueueProjection,

    /// Queue handler instance.
    queue_handler: *QueueHandler,

    /// TS projection.
    ts_projection: *TSProjection,

    /// TS handler instance.
    ts_handler: *TSHandler,

    /// Namespace handler instance.
    namespace_handler: *NamespaceHandler,

    /// Actions handler instance.
    actions_handler: *ActionsHandler,

    /// Unified Append Log — hot ring buffer for recent entries.
    ual: *UAL,

    /// Segment writer — accumulates entries for persistence to .flseg files.
    segment_writer: *SegmentWriter,

    /// Per-shard data directory path (owned, null if ephemeral).
    shard_data_dir: ?[]const u8,

    /// Whether the acceptor pipe has been registered with the reactor.
    pipe_registered: bool,

    /// Raft network reference (set by runtime for shard 0, null otherwise).
    raft_network: ?*RaftNetwork,

    /// Raft consensus node — every shard has one, bootstrapped as single-node leader.
    /// Writes go through propose() → commit → apply to projections.
    raft_node: *RaftNode,

    pub fn init(
        allocator: std.mem.Allocator,
        shard_id: u16,
        shard_count: u16,
        partition_count: u32,
        acceptor_pipe_rd: i32,
        data_dir: ?[]const u8,
    ) !Shard {
        var reactor = try Reactor.init(allocator);
        errdefer reactor.deinit();

        var slab = try SlabAllocator.init(allocator);
        errdefer slab.deinit();

        var inbox = try Inbox.init(allocator, 1024);
        errdefer inbox.deinit();

        // Create KV projection (0 = unlimited memory for now)
        const kv_proj = try allocator.create(KVProjection);
        errdefer allocator.destroy(kv_proj);
        kv_proj.* = KVProjection.init(allocator, 0);
        errdefer kv_proj.deinit();

        // Create KV handler
        const kv_handler = try allocator.create(KVHandler);
        errdefer allocator.destroy(kv_handler);
        kv_handler.* = KVHandler.init(allocator, kv_proj);

        // Create Stream projection + handler
        const stream_proj = try allocator.create(StreamProjection);
        errdefer allocator.destroy(stream_proj);
        stream_proj.* = StreamProjection.init(allocator);

        const stream_handler = try allocator.create(StreamHandler);
        errdefer allocator.destroy(stream_handler);
        stream_handler.* = StreamHandler.init(allocator, stream_proj);

        // Create Queue projection + handler
        const queue_proj = try allocator.create(QueueProjection);
        errdefer allocator.destroy(queue_proj);
        queue_proj.* = QueueProjection.init(allocator, .{});

        const queue_handler = try allocator.create(QueueHandler);
        errdefer allocator.destroy(queue_handler);
        queue_handler.* = QueueHandler.init(allocator, queue_proj);

        // Create TS projection + handler
        const ts_proj = try allocator.create(TSProjection);
        errdefer allocator.destroy(ts_proj);
        ts_proj.* = TSProjection.init(allocator, .{});

        const ts_handler = try allocator.create(TSHandler);
        errdefer allocator.destroy(ts_handler);
        ts_handler.* = TSHandler.init(allocator, ts_proj);

        // Create Namespace handler (no projection needed)
        const namespace_handler = try allocator.create(NamespaceHandler);
        errdefer allocator.destroy(namespace_handler);
        namespace_handler.* = NamespaceHandler.init(allocator);

        // Create Actions handler
        const actions_handler = try allocator.create(ActionsHandler);
        errdefer allocator.destroy(actions_handler);
        actions_handler.* = ActionsHandler.init(allocator);

        // Create UAL (hot ring buffer) and SegmentWriter (persistence)
        const ual = try allocator.create(UAL);
        errdefer allocator.destroy(ual);
        ual.* = try UAL.init(allocator, ual_mod.DEFAULT_CAPACITY);
        errdefer ual.deinit();

        const seg_writer = try allocator.create(SegmentWriter);
        errdefer allocator.destroy(seg_writer);
        seg_writer.* = SegmentWriter.init(allocator, @as(u32, shard_id), .none);
        errdefer seg_writer.deinit();

        // Create Raft consensus node — bootstrapped as single-node leader.
        // Runtime can add peers later for multi-node clusters.
        const raft_node = try allocator.create(RaftNode);
        errdefer allocator.destroy(raft_node);
        raft_node.* = try RaftNode.init(allocator, @as(u32, shard_id) + 1, @as(u32, shard_id), 4096, .{});
        errdefer raft_node.deinit();
        try raft_node.bootstrap();

        var shard_data_dir: ?[]const u8 = null;
        if (data_dir) |dir| {
            // Build shard-specific data directory: data_dir/shard-N/
            const shard_dir = try std.fmt.allocPrint(allocator, "{s}/shard-{d}", .{ dir, shard_id });
            errdefer allocator.free(shard_dir);

            // Ensure directory exists
            std.fs.cwd().makePath(shard_dir) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };

            // Replay existing segment files into KV projection
            var max_index: u64 = 0;
            replaySegments(allocator, shard_dir, kv_proj, &max_index);

            // Restore handler LSN counter to avoid index collisions
            if (max_index > 0) {
                // Advance Raft log indices past replayed data to avoid collisions
                raft_node.log.last_idx = max_index + 1;
                raft_node.commit_index = max_index + 1;
                raft_node.last_applied = max_index + 1;
            }

            shard_data_dir = shard_dir;
        }

        // Build dispatcher and register all handlers
        var dispatcher = Dispatcher.init();
        KVHandler.register(&dispatcher);
        StreamHandler.register(&dispatcher);
        QueueHandler.register(&dispatcher);
        TSHandler.register(&dispatcher);
        NamespaceHandler.register(&dispatcher);
        ActionsHandler.register(&dispatcher);

        // Register ping handler
        dispatcher.register(.ping, handlePing);

        return .{
            .id = shard_id,
            .allocator = allocator,
            .reactor = reactor,
            .inbox = inbox,
            .dispatcher = dispatcher,
            .connections = .{},
            .router = Router.init(partition_count, shard_count, shard_id),
            .slab = slab,
            .acceptor_pipe_rd = acceptor_pipe_rd,
            .running = false,
            .next_conn_id = 1,
            .requests_dispatched = 0,
            .inbox_messages_processed = 0,
            .kv_projection = kv_proj,
            .kv_handler = kv_handler,
            .stream_projection = stream_proj,
            .stream_handler = stream_handler,
            .queue_projection = queue_proj,
            .queue_handler = queue_handler,
            .ts_projection = ts_proj,
            .ts_handler = ts_handler,
            .namespace_handler = namespace_handler,
            .actions_handler = actions_handler,
            .ual = ual,
            .raft_node = raft_node,
            .segment_writer = seg_writer,
            .shard_data_dir = shard_data_dir,
            .pipe_registered = false,
            .raft_network = null,
        };
    }

    pub fn deinit(self: *Shard) void {
        // Close all connections (close fds + free buffers)
        var it = self.connections.iterator();
        while (it.next()) |entry| {
            const fd = entry.key_ptr.*;
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            posix.close(fd);
        }
        self.connections.deinit(self.allocator);

        // Flush pending entries to segment file on shutdown
        if (self.shard_data_dir) |dir| {
            if (self.segment_writer.entry_count > 0) {
                self.segment_writer.writeToFile(dir) catch {};
            }
        }

        // Clean up UAL and SegmentWriter
        self.segment_writer.deinit();
        self.allocator.destroy(self.segment_writer);
        self.ual.deinit();
        self.allocator.destroy(self.ual);

        if (self.shard_data_dir) |dir| {
            self.allocator.free(dir);
        }

        // Clean up KV resources
        self.kv_projection.deinit();
        self.allocator.destroy(self.kv_handler);

        // Clean up Raft consensus node
        self.raft_node.deinit();
        self.allocator.destroy(self.raft_node);
        self.allocator.destroy(self.kv_projection);

        // Clean up Stream resources
        self.stream_projection.deinit();
        self.allocator.destroy(self.stream_handler);
        self.allocator.destroy(self.stream_projection);

        // Clean up Queue resources
        self.queue_projection.deinit();
        self.allocator.destroy(self.queue_handler);
        self.allocator.destroy(self.queue_projection);

        // Clean up TS resources
        self.ts_projection.deinit();
        self.allocator.destroy(self.ts_handler);
        self.allocator.destroy(self.ts_projection);

        // Clean up Namespace and Actions handlers
        self.namespace_handler.deinit();
        self.allocator.destroy(self.namespace_handler);
        self.actions_handler.deinit();
        self.allocator.destroy(self.actions_handler);

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

    /// Remove and clean up a connection (does NOT close the fd).
    pub fn removeConnection(self: *Shard, fd: i32) void {
        if (self.connections.fetchRemove(fd)) |kv| {
            kv.value.deinit();
            self.allocator.destroy(kv.value);
        }
    }

    /// Remove connection, unregister from reactor, and close the fd.
    pub fn closeConnection(self: *Shard, fd: i32) void {
        self.reactor.removeSource(fd);
        self.removeConnection(fd);
        posix.close(fd);
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
            .raft_message => {
                // Received a replicated entry from a peer node.
                // Deserialize and apply to KV projection.
                if (msg.payload_ptr) |ptr| {
                    const data: [*]u8 = @ptrCast(ptr);
                    const len = msg.payload_len;
                    if (len > 0) {
                        const payload = data[0..len];
                        // Try to deserialize as a UAL entry
                        if (entry_mod.Entry.deserialize(payload)) |entry| {
                            // Apply to KV projection (idempotent)
                            self.kv_projection.applyEntry(&entry) catch {};
                        }
                        // Free the duplicated payload
                        self.allocator.free(payload);
                    }
                }
            },
            else => {
                // Other message types will be handled in later phases
            },
        }
    }

    // ─── Event loop ──────────────────────────────────────────────────────

    /// Run one iteration of the event loop (for testing).
    pub fn tick(self: *Shard, timeout_ms: u32) !usize {
        // Lazily register acceptor pipe with reactor on first tick
        if (!self.pipe_registered) {
            try self.reactor.addSource(.{
                .fd = self.acceptor_pipe_rd,
                .tag = .acceptor_pipe,
                .interests = .{ .readable = true },
            });
            self.pipe_registered = true;
        }

        const events = try self.reactor.poll(timeout_ms);

        // Process all I/O events
        for (events) |ev| {
            self.processEvent(ev);
        }

        // Drain inbox each tick
        _ = self.drainInbox();

        // Expire stale blocking-GET waiters (sends not_found to timed-out clients)
        self.kv_handler.*.checkWaiterTimeouts(self);

        return events.len;
    }

    /// Enter the main reactor loop. Blocks until `shutdown()` is called.
    pub fn run(self: *Shard) !void {
        self.running = true;

        while (self.running) {
            _ = self.tick(100) catch {
                continue;
            };
        }
    }

    /// Signal the shard to stop.
    pub fn shutdown(self: *Shard) void {
        self.running = false;
    }

    // ─── Event processing ────────────────────────────────────────────────

    fn processEvent(self: *Shard, ev: ReactorEvent) void {
        // Handle errors and hangups (but not on the acceptor pipe)
        if (ev.tag != .acceptor_pipe and (ev.err or ev.hangup)) {
            self.closeConnection(ev.fd);
            return;
        }

        switch (ev.tag) {
            .acceptor_pipe => {
                if (ev.readable) {
                    self.acceptFromPipe();
                }
            },
            .client_read => {
                if (ev.readable) {
                    self.readFromClient(ev.fd);
                }
                if (ev.writable) {
                    self.flushToClient(ev.fd);
                }
            },
            .client_write => {
                if (ev.writable) {
                    self.flushToClient(ev.fd);
                }
            },
            else => {},
        }
    }

    /// Read fd from the acceptor pipe, create a Connection, and register
    /// the client fd with the reactor for reading.
    fn acceptFromPipe(self: *Shard) void {
        // May have multiple fds pending — drain them all
        while (true) {
            var fd_buf: [@sizeOf(i32)]u8 = undefined;
            const n = posix.read(self.acceptor_pipe_rd, &fd_buf) catch return;
            if (n != @sizeOf(i32)) return;

            const client_fd: i32 = @as(*align(1) const i32, @ptrCast(&fd_buf)).*;

            _ = self.addConnection(client_fd) catch {
                posix.close(client_fd);
                continue;
            };

            self.reactor.addSource(.{
                .fd = client_fd,
                .tag = .client_read,
                .interests = .{ .readable = true },
            }) catch {
                self.removeConnection(client_fd);
                posix.close(client_fd);
            };
        }
    }

    /// Read data from a client socket, parse request(s), and dispatch.
    fn readFromClient(self: *Shard, fd: i32) void {
        const conn = self.getConnection(fd) orelse return;

        // Read available data from socket
        var tmp_buf: [65536]u8 = undefined;

        const n = posix.read(fd, &tmp_buf) catch |err| {
            if (err == error.WouldBlock) return;
            self.closeConnection(fd);
            return;
        };
        if (n == 0) {
            // EOF — peer closed
            self.closeConnection(fd);
            return;
        }

        // Accumulate data in the read buffer
        _ = conn.read_buf.write(tmp_buf[0..n]);

        // Process complete requests
        self.processRequests(fd, conn);
    }

    /// Try to parse and dispatch request(s) from a connection's read buffer.
    fn processRequests(self: *Shard, fd: i32, conn: *Connection) void {
        const header_size = @sizeOf(proto.RequestHeader);

        while (conn.read_buf.readable() >= header_size) {
            // We need contiguous bytes for parsing. Copy readable data out.
            const available = conn.read_buf.readable();
            const to_copy = @min(available, MAX_REQUEST_SIZE);

            var parse_buf: [MAX_REQUEST_SIZE]u8 = undefined;
            // Copy data out of ring buffer WITHOUT consuming (we'll consume on success).
            // Use a peek + manual copy approach: read(), but we'll re-insert on failure.
            const copied = conn.read_buf.read(parse_buf[0..to_copy]);
            if (copied < header_size) {
                // Not enough — put it back
                _ = conn.read_buf.write(parse_buf[0..copied]);
                break;
            }

            const req = proto.Request.parse(parse_buf[0..copied]) catch |err| {
                switch (err) {
                    error.IncompleteRequest, error.IncompletePayload => {
                        // Not enough data yet — put it back and wait
                        _ = conn.read_buf.write(parse_buf[0..copied]);
                        break;
                    },
                    else => {
                        // Bad request — send error and close
                        _ = conn.read_buf.write(parse_buf[0..copied]);
                        self.sendErrorResponse(conn, 0, .bad_request, "Invalid request");
                        self.flushToClient(fd);
                        self.closeConnection(fd);
                        return;
                    },
                }
            };

            const consumed = header_size + req.header.payload_length;

            // Put back any unconsumed bytes (from requests after this one)
            if (copied > consumed) {
                _ = conn.read_buf.write(parse_buf[consumed..copied]);
            }

            // Dispatch request, detecting if the handler sent a response
            const pending_before = conn.write_buf.readable();
            self.dispatchRequest(conn, req);

            // If handler didn't queue any response, check if it was deferred
            if (conn.write_buf.readable() == pending_before) {
                if (conn.response_deferred) {
                    // Handler intentionally deferred the response (e.g. blocking GET)
                    conn.response_deferred = false;
                } else {
                    self.sendErrorResponse(conn, req.header.request_id, .internal_error, "not implemented");
                }
            }

            // Try to flush writes immediately
            self.flushToClient(fd);
        }
    }

    /// Flush pending write data from a connection to the socket.
    pub fn flushToClient(self: *Shard, fd: i32) void {
        const conn = self.getConnection(fd) orelse return;

        while (conn.hasPendingWrites()) {
            const data = conn.pendingWriteData();
            const written = posix.write(fd, data) catch |err| {
                if (err == error.WouldBlock) {
                    // Socket buffer full — arm writable and return
                    self.reactor.armWritable(fd) catch {};
                    return;
                }
                // Write error — close connection
                self.closeConnection(fd);
                return;
            };
            if (written == 0) {
                self.closeConnection(fd);
                return;
            }
            conn.consumeWritten(written);
        }

        // All writes flushed — disarm writable
        self.reactor.disarmWritable(fd) catch {};
    }

    // ─── Response helpers ────────────────────────────────────────────────

    /// Send an error response on a connection.
    pub fn sendErrorResponse(self: *Shard, conn: *Connection, request_id: u64, status: proto.StatusCode, msg: []const u8) void {
        _ = self;
        var buf: [512]u8 = undefined;
        const serialized = proto.Response.serializeNew(status, request_id, msg, &buf) catch return;
        _ = conn.queueWrite(serialized);
    }

    /// Send an OK response with data payload on a connection.
    pub fn sendOkResponse(self: *Shard, conn: *Connection, request_id: u64, data: []const u8) void {
        _ = self;
        var buf: [MAX_REQUEST_SIZE + 24]u8 = undefined;
        const serialized = proto.Response.serializeNew(.ok, request_id, data, &buf) catch return;
        _ = conn.queueWrite(serialized);
    }

    // ─── Built-in handlers ───────────────────────────────────────────────

    fn handlePing(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: proto.Request) void {
        const self: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        self.sendOkResponse(conn, req.header.request_id, "PONG");
    }

    // ─── Stats ───────────────────────────────────────────────────────────

    pub fn connectionCount(self: *const Shard) u32 {
        return self.connections.count();
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Segment Replay — recover state from .flseg files on startup
// ═══════════════════════════════════════════════════════════════════════════════

/// Replay all .flseg segment files in `dir_path` into the KV projection.
/// Tracks the maximum entry index seen for LSN restoration.
fn replaySegments(
    allocator: std.mem.Allocator,
    dir_path: []const u8,
    kv_proj: *KVProjection,
    max_index: *u64,
) void {
    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return;
    defer dir.close();

    var iter = dir.iterate();
    while (iter.next() catch null) |de| {
        if (de.kind != .file) continue;
        // Match .flseg extension
        if (!std.mem.endsWith(u8, de.name, ".flseg")) continue;

        // Build full path
        var path_buf: [512]u8 = undefined;
        const full_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir_path, de.name }) catch continue;

        // Open and read the segment file
        const result = SegmentReader.initFromFile(allocator, full_path) catch continue;
        defer allocator.free(result.buf);

        // Iterate entries and apply to projection
        var offset: usize = 0;
        const data_len = result.reader.data_end - result.reader.data_start;
        while (offset < data_len) {
            const seg_entry = result.reader.readEntryAt(offset) orelse break;
            const etype: entry_mod.EntryType = @enumFromInt(seg_entry.header.entry_type);

            switch (etype) {
                .kv_put, .kv_delete, .kv_batch => {
                    kv_proj.applyEntry(&seg_entry) catch {};
                },
                else => {},
            }

            // Track max index for LSN restoration
            if (seg_entry.header.index > max_index.*) {
                max_index.* = seg_entry.header.index;
            }

            offset += seg_entry.totalSize();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Shard: init and deinit" {
    const pipe_fds = try std.posix.pipe();
    defer std.posix.close(pipe_fds[0]);
    defer std.posix.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null);
    defer shard.deinit();

    try std.testing.expectEqual(@as(u16, 0), shard.id);
    try std.testing.expectEqual(@as(u32, 0), shard.connectionCount());
    try std.testing.expect(!shard.running);
}

test "Shard: add and remove connections" {
    const pipe_fds = try std.posix.pipe();
    defer std.posix.close(pipe_fds[0]);
    defer std.posix.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null);
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

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null);
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
    // Note: conn_pipe[0] is owned by shard (closed in deinit), only close write end
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

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null);
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
