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
const log = @import("stdx").log;
const reactor_mod = @import("reactor.zig");
const Reactor = reactor_mod.Reactor;
const ReactorEvent = reactor_mod.Event;
const Tag = reactor_mod.Tag;
const Inbox = @import("inbox.zig").Inbox;
const InboxMessage = @import("inbox.zig").Message;
const Dispatcher = @import("dispatcher.zig").Dispatcher;
const Connection = @import("connection.zig").Connection;
const connection_mod = @import("connection.zig");
const RingBuffer = @import("connection.zig").RingBuffer;
const Router = @import("router.zig").Router;
const node_router = @import("router.zig");
const SlabAllocator = @import("slab.zig").SlabAllocator;
const proto = @import("../protocol/proto.zig");
const resp_mod = @import("../protocol/resp.zig");
const result_mod = @import("../protocol/result.zig");
const CommandResult = result_mod.CommandResult;
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
const WorkflowHandler = @import("../workflow/handler.zig").WorkflowHandler;
const ProcessingHandler = @import("../processing/handler.zig").ProcessingHandler;
const TaskScheduler = @import("task_scheduler.zig").TaskScheduler;
const ual_mod = @import("../storage/ual/ual.zig");
const UAL = ual_mod.UAL;
const SegmentWriter = @import("../storage/ual/writer.zig").SegmentWriter;
const SegmentReader = @import("../storage/ual/reader.zig").SegmentReader;
const entry_mod = @import("../storage/ual/entry.zig");
const segment_mod = @import("../storage/ual/segment.zig");
const Entry = entry_mod.Entry;
const RaftNetwork = @import("../raft/network.zig").RaftNetwork;
const RaftNode = @import("../raft/node.zig").RaftNode;
const waiter_pool_mod = @import("waiter_pool.zig");
const WaiterPool = waiter_pool_mod.WaiterPool;
const Waiter = waiter_pool_mod.Waiter;
const WaiterKind = waiter_pool_mod.WaiterKind;
const stream_handler_mod = @import("../stream/handler.zig");
const queue_handler_mod = @import("../queue/handler.zig");
const Partition = @import("../storage/partition.zig").Partition;
const snapshot_mod = @import("../storage/snapshot.zig");
const shard_manifest = @import("shard_manifest.zig");
const ShardManifest = shard_manifest.ShardManifest;
const Forwarder = @import("../cluster/forwarder.zig").Forwarder;
const PartitionTable = @import("../cluster/partition_table.zig").PartitionTable;
const NodeId = @import("../raft/node.zig").NodeId;

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

    /// Partitions owned by this shard (heap-allocated, stable pointers).
    /// Each Partition owns a UAL, ProjectionRouter, and all four projections.
    /// Currently 1 partition per shard; will scale to N later.
    partitions: []*Partition,
    num_partitions: u32,

    /// KV handler instance (heap-allocated, stable pointer).
    kv_handler: *KVHandler,

    /// Stream handler instance.
    stream_handler: *StreamHandler,

    /// Queue handler instance.
    queue_handler: *QueueHandler,

    /// TS handler instance.
    ts_handler: *TSHandler,

    /// Namespace handler instance.
    namespace_handler: *NamespaceHandler,

    /// Actions handler instance.
    actions_handler: *ActionsHandler,

    /// Workflow handler instance.
    workflow_handler: *WorkflowHandler,

    // Processing handler instance.
    processing_handler: *ProcessingHandler,

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

    /// Unified waiter pool — handles blocking GET, blocking dequeue,
    /// stream long-poll, and worker_await across all subsystems.
    waiter_pool: WaiterPool,

    /// Cooperative periodic background tasks (hot_flush, TTL sweep, etc.).
    task_scheduler: TaskScheduler,

    /// Maximum age of entries in the hot ring (seconds). 0 = disabled.
    hot_flush_seconds: u64,

    /// Cross-node request forwarder (null in single-node mode).
    forwarder: ?*Forwarder,

    /// Cluster partition table (null in single-node mode).
    partition_table: ?*PartitionTable,

    pub fn init(
        allocator: std.mem.Allocator,
        shard_id: u16,
        shard_count: u16,
        partition_count: u32,
        acceptor_pipe_rd: i32,
        data_dir: ?[]const u8,
        ual_capacity: usize,
        max_hot_entries: u64,
        hot_flush_seconds: u64,
    ) !Shard {
        var reactor = try Reactor.init(allocator);
        errdefer reactor.deinit();

        var slab = try SlabAllocator.init(allocator);
        errdefer slab.deinit();

        var inbox = try Inbox.init(allocator, 1024);
        errdefer inbox.deinit();

        // Create the default partition (1 per shard for now).
        // The partition owns UAL + ProjectionRouter + all four projections.
        const partitions = try allocator.alloc(*Partition, 1);
        errdefer allocator.free(partitions);

        const partition = try allocator.create(Partition);
        errdefer allocator.destroy(partition);
        partition.* = try Partition.init(allocator, @as(u32, shard_id), ual_capacity, max_hot_entries);
        errdefer partition.deinit();
        partition.wireProjections();
        partitions[0] = partition;

        // Create handlers pointing at the default partition's projections
        const kv_handler = try allocator.create(KVHandler);
        errdefer allocator.destroy(kv_handler);
        kv_handler.* = KVHandler.init(allocator, &partition.kv);

        const stream_handler = try allocator.create(StreamHandler);
        errdefer allocator.destroy(stream_handler);
        stream_handler.* = StreamHandler.init(allocator, partition);

        const queue_handler = try allocator.create(QueueHandler);
        errdefer allocator.destroy(queue_handler);
        queue_handler.* = QueueHandler.init(allocator, partition);

        const ts_handler = try allocator.create(TSHandler);
        errdefer allocator.destroy(ts_handler);
        ts_handler.* = TSHandler.init(allocator, &partition.ts);

        // Create Namespace handler (no projection needed)
        const namespace_handler = try allocator.create(NamespaceHandler);
        errdefer allocator.destroy(namespace_handler);
        namespace_handler.* = NamespaceHandler.init(allocator);

        // Create Actions handler
        const actions_handler = try allocator.create(ActionsHandler);
        errdefer allocator.destroy(actions_handler);
        actions_handler.* = ActionsHandler.init(allocator);

        // Create Workflow handler
        const workflow_handler = try allocator.create(WorkflowHandler);
        errdefer allocator.destroy(workflow_handler);
        workflow_handler.* = WorkflowHandler.init(allocator);

        // Create Processing handler
        const processing_handler = try allocator.create(ProcessingHandler);
        errdefer allocator.destroy(processing_handler);
        processing_handler.* = ProcessingHandler.init(allocator);

        // Create SegmentWriter (persistence — per-shard, not per-partition)
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
            // Build shard-specific data directory: data_dir/00000/
            const shard_dir = try std.fmt.allocPrint(allocator, "{s}/{d:0>5}", .{ dir, shard_id });
            errdefer allocator.free(shard_dir);

            // Ensure shard dir and subdirectories exist
            std.fs.cwd().makePath(shard_dir) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };

            const segs_dir_path = try std.fmt.allocPrint(allocator, "{s}/segs", .{shard_dir});
            defer allocator.free(segs_dir_path);
            std.fs.cwd().makePath(segs_dir_path) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };

            const snaps_dir_path = try std.fmt.allocPrint(allocator, "{s}/snaps", .{shard_dir});
            defer allocator.free(snaps_dir_path);
            std.fs.cwd().makePath(snaps_dir_path) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };

            // ── Recovery from shard MANIFEST ────────────────────────────
            // The MANIFEST is the single source of truth for:
            //   - latest_snapshot: which .fsnap to load from snaps/
            //   - cold_segments: what's been archived to object storage
            var replay_from: u64 = 0;

            if (ShardManifest.load(allocator, shard_dir)) |maybe_sm| {
                if (maybe_sm) |sm_val| {
                    var sm = sm_val;
                    defer sm.deinit(allocator);

                    // Step 1: Load snapshot if referenced
                    if (sm.latest_snapshot) |snap_name| {
                        if (std.fs.cwd().openDir(snaps_dir_path, .{})) |snap_dir_handle| {
                            var snap_dir = snap_dir_handle;
                            defer snap_dir.close();

                            if (snapshot_mod.loadSnapshotByName(allocator, snap_dir, snap_name)) |maybe_snap| {
                                if (maybe_snap) |snap| {
                                    defer allocator.free(snap.data);
                                    if (partition.recover(snap.data)) |snap_index| {
                                        replay_from = snap_index;
                                    } else |_| {
                                        replay_from = 0;
                                    }
                                }
                            } else |_| {}
                        } else |_| {}
                    }

                    // Step 2: Load cold segment entries into partition
                    if (partition.cold_tier) |ct| {
                        for (sm.cold_segments.items) |seg| {
                            ct.manifest.addEntry(.{
                                .min_index = seg.min_index,
                                .max_index = seg.max_index,
                                .min_timestamp_ns = seg.min_ts,
                                .max_timestamp_ns = seg.max_ts,
                                .location = seg.location,
                                .size_bytes = seg.size,
                                .checksum = seg.crc,
                            }) catch {};
                        }
                    }
                }
            } else |_| {
                // MANIFEST load failed — fall back to full replay
            }

            // Replay existing segment files from segs/ into partition.
            // If a snapshot was loaded, skip entries at or below replay_from.
            var max_index: u64 = replay_from;
            replaySegments(allocator, segs_dir_path, partition, &max_index, workflow_handler, replay_from);

            // Restore handler LSN counter to avoid index collisions
            if (max_index > 0) {
                raft_node.log.last_idx = max_index + 1;
                raft_node.commit_index = max_index + 1;
                raft_node.last_applied = max_index + 1;
            }

            shard_data_dir = shard_dir;
        }

        // Wire UAL persistence: entries auto-persist to SegmentWriter on append.
        // This is wired AFTER segment replay so replayed entries don't get
        // re-persisted (design: UAL → hot ring → flush to warm segments).
        partition.ual.on_append_ctx = @ptrCast(seg_writer);
        partition.ual.on_append = ualPersistCallback;
        raft_node.log.ual.on_append_ctx = @ptrCast(seg_writer);
        raft_node.log.ual.on_append = ualPersistCallback;

        // Build dispatcher and register all handlers
        var dispatcher = Dispatcher.init();
        KVHandler.register(&dispatcher);
        StreamHandler.register(&dispatcher);
        QueueHandler.register(&dispatcher);
        TSHandler.register(&dispatcher);
        NamespaceHandler.register(&dispatcher);
        ActionsHandler.register(&dispatcher);
        WorkflowHandler.register(&dispatcher);
        ProcessingHandler.register(&dispatcher);

        // Register ping handler
        dispatcher.register(.ping, handlePing);

        log.debug("Shard {d} initializing: shard_count={d} partition_count={d} handlers={d} data_dir={s}", .{
            shard_id,
            shard_count,
            partition_count,
            dispatcher.handler_count,
            data_dir orelse "(ephemeral)",
        });

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
            .partitions = partitions,
            .num_partitions = 1,
            .kv_handler = kv_handler,
            .stream_handler = stream_handler,
            .queue_handler = queue_handler,
            .ts_handler = ts_handler,
            .namespace_handler = namespace_handler,
            .actions_handler = actions_handler,
            .workflow_handler = workflow_handler,
            .processing_handler = processing_handler,
            .raft_node = raft_node,
            .segment_writer = seg_writer,
            .shard_data_dir = shard_data_dir,
            .pipe_registered = false,
            .raft_network = null,
            .waiter_pool = WaiterPool.init(),
            .task_scheduler = TaskScheduler.init(),
            .hot_flush_seconds = hot_flush_seconds,
            .forwarder = null,
            .partition_table = null,
        };
    }

    // ─── Cluster wiring ──────────────────────────────────────────────────

    /// Wire a cross-node forwarder (enables cluster mode forwarding).
    pub fn setForwarder(self: *Shard, fwd: *Forwarder) void {
        self.forwarder = fwd;
    }

    /// Wire a cluster partition table (enables cluster-aware routing).
    pub fn setPartitionTable(self: *Shard, pt: *PartitionTable) void {
        self.partition_table = pt;
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
                var segs_buf: [512]u8 = undefined;
                const segs_path = std.fmt.bufPrint(&segs_buf, "{s}/segs", .{dir}) catch dir;
                self.segment_writer.writeToFile(segs_path) catch {};
            }
        }

        // Clean up SegmentWriter
        self.segment_writer.deinit();
        self.allocator.destroy(self.segment_writer);

        if (self.shard_data_dir) |dir| {
            self.allocator.free(dir);
        }

        // Clean up handlers (they don't own projections — partitions do)
        self.allocator.destroy(self.kv_handler);
        self.stream_handler.deinit();
        self.allocator.destroy(self.stream_handler);
        self.allocator.destroy(self.queue_handler);
        self.allocator.destroy(self.ts_handler);

        // Clean up Raft consensus node
        self.raft_node.deinit();
        self.allocator.destroy(self.raft_node);

        // Clean up partitions (each owns UAL + all projections)
        for (self.partitions) |p| {
            p.deinit();
            self.allocator.destroy(p);
        }
        self.allocator.free(self.partitions);

        // Clean up Namespace and Actions handlers
        self.namespace_handler.deinit();
        self.allocator.destroy(self.namespace_handler);
        self.actions_handler.deinit();
        self.allocator.destroy(self.actions_handler);
        self.workflow_handler.deinit();
        self.allocator.destroy(self.workflow_handler);
        self.processing_handler.deinit();
        self.allocator.destroy(self.processing_handler);

        self.inbox.deinit();
        self.slab.deinit();
        self.reactor.deinit();
    }

    // ─── Partition access ────────────────────────────────────────────────

    /// Get the default (first) partition.
    /// Currently each shard has exactly 1 partition.
    pub fn defaultPartition(self: *Shard) *Partition {
        return self.partitions[0];
    }

    /// Get a partition by partition_id.
    /// For now, all partition_ids map to the single default partition.
    /// When multi-partition is enabled, this will index into the array.
    pub fn getPartition(self: *Shard, partition_id: u32) *Partition {
        _ = partition_id;
        return self.partitions[0];
    }

    // ─── Snapshot ────────────────────────────────────────────────────────

    /// Take a snapshot of all partitions and write to disk.
    /// Creates `{shard_data_dir}/snaps/` if it doesn't exist.
    /// Returns true if snapshot was written successfully.
    pub fn takeSnapshot(self: *Shard) bool {
        const dir_path = self.shard_data_dir orelse return false;

        var snap_path_buf: [512]u8 = undefined;
        const snap_dir_path = std.fmt.bufPrint(&snap_path_buf, "{s}/snaps", .{dir_path}) catch return false;

        // Ensure snapshots directory exists
        std.fs.cwd().makePath(snap_dir_path) catch return false;

        var snap_dir = std.fs.cwd().openDir(snap_dir_path, .{}) catch return false;
        defer snap_dir.close();

        // Snapshot each partition
        for (self.partitions) |partition| {
            const snap_data = partition.snapshot() catch continue;
            defer self.allocator.free(snap_data);

            // Generate snapshot filename
            var name_buf: [128]u8 = undefined;
            const filename = snapshot_mod.snapshotFilename(
                &name_buf,
                partition.router.applied_index,
                @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000,
            );

            // Write atomically: .tmp → sync → rename
            var tmp_buf: [128]u8 = undefined;
            const tmp_name = std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{filename}) catch continue;

            const file = snap_dir.createFile(tmp_name, .{}) catch continue;
            file.writeAll(snap_data) catch {
                file.close();
                continue;
            };
            file.sync() catch {};
            file.close();

            snap_dir.rename(tmp_name, filename) catch continue;
            ShardManifest.setLatestSnapshot(self.allocator, dir_path, filename) catch {};
        }

        return true;
    }

    // ─── Connection management ───────────────────────────────────────────

    /// Add a new connection from an accepted fd.
    pub fn addConnection(self: *Shard, fd: i32) !*Connection {
        const conn = try self.allocator.create(Connection);
        conn.* = try Connection.init(self.allocator, fd, self.next_conn_id);
        log.debug("Shard {d} new connection: fd={d} conn_id={d}", .{ self.id, fd, self.next_conn_id });
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
        log.debug("Shard {d} closing connection: fd={d}", .{ self.id, fd });
        // Clean up any pending waiters for this connection
        self.waiter_pool.removeByFd(fd);
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
    /// Walk opcodes (list/scan) are routed to executeWalk() for cross-shard
    /// aggregation when walk contexts are wired (multi-shard mode).
    /// In cluster mode (partition_table != null), routable requests are checked
    /// against the partition table and forwarded to remote nodes when needed.
    pub fn dispatchRequest(self: *Shard, conn: *Connection, req: proto.Request) void {
        conn.recordRequest();
        self.requests_dispatched += 1;

        // Check if this is a walk opcode with contexts wired
        const op = req.header.op_code;
        if (self.dispatcher.isWalkOp(op) and self.dispatcher.walk_contexts[op] != null) {
            // If there's also a pre-route function, check if it routes to a single shard.
            // This supports dual-path opcodes like kv_scan: prefix → single shard,
            // no prefix → multi-shard walk.
            if (self.dispatcher.pre_route[op]) |pre_route_fn| {
                if (pre_route_fn(req) != null) {
                    // Pre-route returned a hash → single-shard dispatch
                    self.dispatcher.dispatch(@ptrCast(self), @ptrCast(conn), req);
                    return;
                }
            }
            self.executeWalk(conn, req);
        } else if (self.partition_table) |pt| {
            // Cluster mode: check if the request should be forwarded to another node
            if (self.dispatcher.pre_route[op]) |pre_route_fn| {
                if (pre_route_fn(req)) |hash| {
                    const ns_hash = node_router.namespaceHash(req.namespace);
                    const target = self.router.routeCluster(hash, ns_hash, pt);
                    switch (target) {
                        .remote => |remote| {
                            self.forwardToRemote(conn, req, remote.node_id);
                            return;
                        },
                        .local, .shard => {},
                    }
                }
            }
            self.dispatcher.dispatch(@ptrCast(self), @ptrCast(conn), req);
        } else {
            self.dispatcher.dispatch(@ptrCast(self), @ptrCast(conn), req);
        }
    }

    /// Forward a request to a remote node via the cluster forwarder.
    fn forwardToRemote(self: *Shard, conn: *Connection, req: proto.Request, node_id: NodeId) void {
        const fwd = self.forwarder orelse {
            self.sendErrorResponse(conn, req.header.request_id, .internal_error, "no forwarder configured");
            return;
        };
        const now_ms = std.time.milliTimestamp();
        const result = fwd.forward(
            node_id,
            req.header.request_id,
            @as(u64, conn.id),
            req.header.payload_length,
            now_ms,
        ) catch {
            self.sendErrorResponse(conn, req.header.request_id, .internal_error, "forward failed");
            return;
        };
        switch (result) {
            .queued => {
                log.debug("Forwarded request {d} to node {d}", .{ req.header.request_id, node_id });
            },
            .no_route => {
                self.sendErrorResponse(conn, req.header.request_id, .internal_error, "no route to node");
            },
            .overloaded => {
                self.sendErrorResponse(conn, req.header.request_id, .overloaded, "forward queue full");
            },
            .local => {
                // Shouldn't happen — routeCluster already checked. Dispatch locally.
                self.dispatcher.dispatch(@ptrCast(self), @ptrCast(conn), req);
            },
        }
    }

    // ─── Cross-Shard Walk ────────────────────────────────────────────────

    /// Execute a cross-shard walk (list/scan) for the given request.
    ///
    /// Uses ShardWalker([]const u8) to sequentially scan all shards'
    /// projections via the registered LocalScanFn, with cursor-based
    /// pagination.  Results are deduplicated (defensive — routing should
    /// prevent duplicates) and serialized in the standard list wire format.
    ///
    /// Wire format: [count:u32] ([name_len:u16][name])* [has_more:u8] [cursor_len:u16][cursor]
    fn executeWalk(self: *Shard, conn: *Connection, req: proto.Request) void {
        const NameWalker = @import("dispatcher.zig").NameWalker;
        const op_idx = req.header.op_code;

        const scan_fn = self.dispatcher.walk_fn[op_idx] orelse {
            self.dispatcher.dispatch(@ptrCast(self), @ptrCast(conn), req);
            return;
        };
        const contexts = self.dispatcher.walk_contexts[op_idx] orelse {
            self.dispatcher.dispatch(@ptrCast(self), @ptrCast(conn), req);
            return;
        };

        // Drive ShardWalker — sequential shard scan with cursor pagination
        const walker = NameWalker.init(scan_fn, @intCast(contexts.len));
        var result_buf: [NameWalker.MAX_BATCH][]const u8 = undefined;
        var cursor_buf: [64]u8 = undefined;

        const cursor: ?[]const u8 = if (req.value.len > 0) req.value else null;
        const filter: []const u8 = req.key; // prefix filter (empty = no filter)

        const result = walker.walk(
            contexts,
            req.namespace,
            filter,
            cursor,
            NameWalker.MAX_BATCH,
            &result_buf,
            &cursor_buf,
        );

        // Dedup names (defensive — routing hashes should prevent duplicates,
        // but edge cases during rebalance could produce them).
        var deduped: [NameWalker.MAX_BATCH][]const u8 = undefined;
        var dedup_count: usize = 0;
        for (result.items) |name| {
            var found = false;
            for (deduped[0..dedup_count]) |existing| {
                if (std.mem.eql(u8, existing, name)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                deduped[dedup_count] = name;
                dedup_count += 1;
            }
        }

        // Serialize and send — select wire format based on opcode
        const op_enum: proto.OpCode = @enumFromInt(op_idx);
        const data = switch (op_enum) {
            // kv_scan uses scan wire format: [count:u32]([key_len:u16][key][value_len:u32(=0)])*[has_more:u8][cursor_len:u16][cursor]
            .kv_scan => serializeWalkKeysAsScan(self.allocator, deduped[0..dedup_count], result.next_cursor),
            // stream_list uses stream wire format: [count:u32]([name_len:u32][name][partition_count:u32])*[has_more:u8][cursor_len:u16][cursor]
            .stream_list => serializeWalkStreamNames(self.allocator, deduped[0..dedup_count], result.next_cursor, &self.defaultPartition().stream),
            // queue_list uses rich binary format with per-queue stats
            .queue_list => serializeWalkQueueEntries(self.allocator, deduped[0..dedup_count], result.next_cursor, contexts, req.namespace),
            // processing_list and workflow_list_definitions use JSON format
            .processing_list => serializeWalkProcessingJobs(self.allocator, deduped[0..dedup_count], contexts, req.namespace),
            .workflow_list_definitions => serializeWalkWorkflowDefs(self.allocator, deduped[0..dedup_count], contexts, req.namespace),
            // Default: name-list format: [count:u32]([name_len:u16][name])*[has_more:u8][cursor_len:u16][cursor]
            else => serializeWalkNames(self.allocator, deduped[0..dedup_count], result.next_cursor),
        } catch {
            self.sendErrorResponse(conn, req.header.request_id, .internal_error, "walk serialization failed");
            return;
        };
        defer self.allocator.free(data);

        self.sendOkResponse(conn, req.header.request_id, data);
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
                            // Apply to partition projections (idempotent)
                            self.defaultPartition().kv.applyEntry(&entry) catch {};
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

        // Expire stale blocking waiters across all subsystems
        self.waiter_pool.expireTimeouts(handleWaiterTimeout, @ptrCast(self));

        // Run cooperative background tasks (hot_flush, TTL sweep, etc.)
        _ = self.task_scheduler.tick(2_000_000); // 2ms budget

        // Drive processing pipelines (poll sources → write sinks)
        self.processing_handler.tickPipelines(self);

        return events.len;
    }

    /// Enter the main reactor loop. Blocks until `shutdown()` is called.
    pub fn run(self: *Shard) !void {
        self.running = true;
        log.debug("Shard {d} entering reactor loop", .{self.id});

        while (self.running) {
            _ = self.tick(100) catch {
                continue;
            };
        }

        log.debug("Shard {d} reactor loop exited", .{self.id});
    }

    /// Signal the shard to stop.
    pub fn shutdown(self: *Shard) void {
        self.running = false;
    }

    // ─── Background tasks ────────────────────────────────────────────────

    /// Register cooperative background tasks. Called from runtime AFTER
    /// the Shard is at its final heap address (since init returns by value).
    pub fn registerBackgroundTasks(self: *Shard) void {
        if (self.hot_flush_seconds > 0) {
            self.task_scheduler.register(
                "hot_flush",
                1_000, // check every 1 second
                500_000, // 0.5ms budget per invocation
                hotFlushTask,
                @ptrCast(self),
            ) catch {};
        }
    }

    /// TaskScheduler callback: evict entries older than hot_flush_seconds
    /// from every partition's UAL hot ring.
    fn hotFlushTask(ctx: *anyopaque, _: u64) u64 {
        const self: *Shard = @ptrCast(@alignCast(ctx));
        const now_ns: u64 = @intCast(std.time.nanoTimestamp());
        const cutoff_ns = now_ns -| (self.hot_flush_seconds * std.time.ns_per_s);

        var total_evicted: u64 = 0;
        for (self.partitions) |partition| {
            total_evicted += partition.ual.evictOlderThan(cutoff_ns);
        }
        return total_evicted;
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

        // Detect protocol on first data if not yet determined
        if (conn.protocol == .unknown) {
            conn.detectAndSetProtocol();
        }

        // Dispatch based on protocol
        switch (conn.protocol) {
            .resp => self.processRespRequests(fd, conn),
            else => self.processRequests(fd, conn),
        }
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

    // ─── RESP Protocol Handler ──────────────────────────────────────────

    /// Process RESP (Redis protocol) requests from a connection's read buffer.
    /// Parses RESP commands, translates to Flo operations, executes directly
    /// via the appropriate handler, and serializes responses back to RESP.
    fn processRespRequests(self: *Shard, fd: i32, conn: *Connection) void {
        var resp_parser = resp_mod.Parser.init(self.allocator);
        defer resp_parser.deinit();

        while (conn.read_buf.readable() > 0) {
            // Peek all available data without consuming
            const available = conn.read_buf.readable();
            var parse_buf: [MAX_REQUEST_SIZE]u8 = undefined;
            const to_copy = @min(available, MAX_REQUEST_SIZE);
            const copied = conn.read_buf.read(parse_buf[0..to_copy]);
            if (copied == 0) break;

            // Try to parse a complete RESP value
            const parsed = resp_parser.parse(parse_buf[0..copied]) catch {
                // Parse error — send RESP error and close
                _ = conn.read_buf.write(parse_buf[0..copied]);
                _ = conn.queueWrite("-ERR invalid RESP data\r\n");
                self.flushToClient(fd);
                self.closeConnection(fd);
                return;
            };

            if (parsed == null) {
                // Incomplete — put data back and wait for more
                _ = conn.read_buf.write(parse_buf[0..copied]);
                break;
            }

            const result = parsed.?;
            var resp_value = result.value;
            const consumed = result.consumed;

            // Put back unconsumed bytes
            if (copied > consumed) {
                _ = conn.read_buf.write(parse_buf[consumed..copied]);
            }

            defer resp_mod.freeValue(self.allocator, &resp_value);

            // Get session namespace (default "default")
            const namespace = conn.namespace orelse "default";

            // Translate RESP command to Flo operation
            const translate_result = resp_mod.translateCommand(self.allocator, resp_value, namespace) catch |err| {
                switch (err) {
                    error.UnknownCommand => {
                        _ = conn.queueWrite("-ERR unknown command\r\n");
                    },
                    else => {
                        _ = conn.queueWrite("-ERR invalid command\r\n");
                    },
                }
                self.flushToClient(fd);
                resp_parser.reset();
                continue;
            };

            switch (translate_result) {
                .use_namespace => |u| {
                    conn.namespace = u.namespace;
                    _ = conn.queueWrite("+OK\r\n");
                },
                .select_db => {
                    // Redis SELECT — acknowledge silently
                    _ = conn.queueWrite("+OK\r\n");
                },
                .command => |cmd| {
                    self.executeRespCommand(conn, cmd);
                },
            }

            self.flushToClient(fd);
            resp_parser.reset();
        }
    }

    /// Execute a translated RESP command through the appropriate handler
    /// and queue the RESP-formatted response.
    fn executeRespCommand(self: *Shard, conn: *Connection, cmd: resp_mod.RespCommand) void {
        // Build a proto.Request from the RESP command
        const req = proto.Request{
            .header = .{
                .magic = proto.MAGIC,
                .payload_length = 0,
                .request_id = conn.requests_total,
                .crc32 = 0,
                .version = proto.VERSION,
                .op_code = @intFromEnum(cmd.opcode),
                .flags = 0,
                .reserved = 0,
            },
            .namespace = cmd.namespace,
            .key = cmd.key,
            .value = cmd.value,
            .options = "",
        };

        conn.requests_total += 1;

        // Dispatch to the appropriate handler and get CommandResult
        const cmd_result = self.handleRespOpcode(cmd.opcode, req);

        // Translate CommandResult → RESP and serialize
        const resp_value = resp_mod.translateResult(cmd_result);
        const response_bytes = resp_mod.serialize(self.allocator, resp_value) catch {
            _ = conn.queueWrite("-ERR internal error\r\n");
            return;
        };
        defer self.allocator.free(response_bytes);

        _ = conn.queueWrite(response_bytes);

        // Free any heap-allocated fields from translateCommand
        self.freeRespCommand(cmd);
    }

    /// Route a RESP opcode to the appropriate handler, returning a CommandResult.
    fn handleRespOpcode(self: *Shard, opcode: proto.OpCode, req: proto.Request) CommandResult {
        return switch (opcode) {
            .ping => .pong,
            .kv_get => self.kv_handler.handleCommand(req),
            .kv_put => self.kv_handler.handleCommand(req),
            .kv_delete => self.kv_handler.handleCommand(req),
            .stream_append => self.stream_handler.handleCommand(req),
            .stream_read => self.stream_handler.handleCommand(req),
            .queue_enqueue => self.queue_handler.handleCommand(req),
            .queue_dequeue => self.queue_handler.handleCommand(req),
            else => .{ .err = .{ .code = .invalid_request, .message = "unsupported RESP command" } },
        };
    }

    /// Free heap-allocated fields from a translated RESP command.
    /// Only frees key/value if they were heap-allocated (non-empty, since
    /// translateCommand uses allocator.dupe for non-empty strings).
    fn freeRespCommand(self: *Shard, cmd: resp_mod.RespCommand) void {
        // translateCommand allocates via dupe — these are heap slices.
        // Empty strings are literal "" (not heap allocated).
        switch (cmd.opcode) {
            .ping => {}, // key="" value="" — literals
            .kv_get, .kv_delete, .queue_dequeue => {
                // key is duped, value is literal ""
                if (cmd.key.len > 0) self.allocator.free(cmd.key);
            },
            .kv_put, .stream_append, .queue_enqueue => {
                // Both key and value are duped
                if (cmd.key.len > 0) self.allocator.free(cmd.key);
                if (cmd.value.len > 0) self.allocator.free(cmd.value);
            },
            .stream_read => {
                // key is duped, value is literal ""
                if (cmd.key.len > 0) self.allocator.free(cmd.key);
            },
            else => {
                // Conservative: try to free both if non-empty
                if (cmd.key.len > 0) self.allocator.free(cmd.key);
                if (cmd.value.len > 0) self.allocator.free(cmd.value);
            },
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
// Walk Serialization — standard list wire format
// ═══════════════════════════════════════════════════════════════════════════════

/// Serialize a list of names into the standard list wire format.
///
/// Wire format: [count:u32] ([name_len:u16][name])* [has_more:u8] [cursor_len:u16] [cursor]
///
/// This is the generic format used by all list/scan walk operations
/// (ts_list, stream_list, queue_list, action_list, workflow_list_definitions).
/// When `next_cursor` is non-null, has_more=1 and the cursor bytes follow cursor_len.
/// Serialize walk results in queue list wire format with per-queue stats.
///
/// Wire format: [count:u32] ([name_len:u32][name][ns_len:u32][ns]
///   [pending:u64][available:u64][enqueued:u64][dequeued:u64][dlq:u64])*
///   [has_more:u8] [cursor_len:u16][cursor]
///
/// Stats are looked up from the shard context that owns each queue.
fn serializeWalkQueueEntries(
    allocator: std.mem.Allocator,
    names: []const []const u8,
    next_cursor: ?[]const u8,
    contexts: []const *anyopaque,
    namespace: []const u8,
) ![]u8 {
    const cursor_bytes = next_cursor orelse &[_]u8{};
    const has_more: u8 = if (next_cursor != null) 1 else 0;

    const Meta = struct { name: []const u8, ns: []const u8, enqueued: u64, dequeued: u64, dlq: u64 };
    var metas_buf: [512]Meta = undefined;
    var count: usize = 0;

    for (names) |name| {
        if (count >= metas_buf.len) break;
        var found = false;
        for (contexts) |ctx| {
            const handler: *QueueHandler = @ptrCast(@alignCast(ctx));
            var it = handler.queue.known_queues.iterator();
            while (it.next()) |entry| {
                const meta = entry.value_ptr;
                if (std.mem.eql(u8, meta.name, name)) {
                    metas_buf[count] = .{
                        .name = meta.name,
                        .ns = meta.namespace,
                        .enqueued = meta.enqueued,
                        .dequeued = meta.dequeued,
                        .dlq = @intCast(handler.queue.dlqCount()),
                    };
                    found = true;
                    break;
                }
            }
            if (found) break;
        }
        if (!found) {
            metas_buf[count] = .{ .name = name, .ns = namespace, .enqueued = 0, .dequeued = 0, .dlq = 0 };
        }
        count += 1;
    }

    const metas = metas_buf[0..count];
    var total: usize = 4; // count header
    for (metas) |m| {
        total += 4 + m.name.len + 4 + m.ns.len + 5 * 8;
    }
    total += 1 + 2 + cursor_bytes.len;

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    std.mem.writeInt(u32, buf[0..4], @intCast(count), .little);
    var pos: usize = 4;
    for (metas) |m| {
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(m.name.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..m.name.len], m.name);
        pos += m.name.len;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(m.ns.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..m.ns.len], m.ns);
        pos += m.ns.len;
        const pending = if (m.enqueued > m.dequeued) m.enqueued - m.dequeued else 0;
        std.mem.writeInt(u64, buf[pos..][0..8], pending, .little);
        pos += 8;
        std.mem.writeInt(u64, buf[pos..][0..8], pending, .little); // available ≈ pending
        pos += 8;
        std.mem.writeInt(u64, buf[pos..][0..8], m.enqueued, .little);
        pos += 8;
        std.mem.writeInt(u64, buf[pos..][0..8], m.dequeued, .little);
        pos += 8;
        std.mem.writeInt(u64, buf[pos..][0..8], m.dlq, .little);
        pos += 8;
    }
    buf[pos] = has_more;
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(cursor_bytes.len), .little);
    pos += 2;
    if (cursor_bytes.len > 0) {
        @memcpy(buf[pos..][0..cursor_bytes.len], cursor_bytes);
    }
    return buf;
}

/// Serialize walk results as a JSON array of processing jobs.
///
/// Looks up each job name across all shard contexts to produce full JSON
/// matching the format the CLI expects: [{"job_id":..., "name":..., ...}].
fn serializeWalkProcessingJobs(
    allocator: std.mem.Allocator,
    names: []const []const u8,
    contexts: []const *anyopaque,
    namespace: []const u8,
) ![]u8 {
    var result_buf: [65536]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&result_buf);
    const writer = fbs.writer();

    writer.writeByte('[') catch return error.OutOfMemory;

    const req_ns = if (namespace.len > 0) namespace else "default";
    var count: u32 = 0;

    for (names) |name| {
        for (contexts) |ctx| {
            const handler: *ProcessingHandler = @ptrCast(@alignCast(ctx));
            var it = handler.jobs.iterator();
            while (it.next()) |entry| {
                const job = entry.value_ptr;
                if (std.mem.eql(u8, job.name_owned, name) and std.mem.eql(u8, job.namespace_owned, req_ns)) {
                    if (count > 0) writer.writeByte(',') catch return error.OutOfMemory;
                    std.fmt.format(writer,
                        \\{{"job_id":"{s}","name":"{s}","status":"{s}","parallelism":{d},"created_at_ms":{d}}}
                    , .{
                        job.job_id_owned,
                        job.name_owned,
                        job.status.toString(),
                        job.parallelism,
                        job.created_at_ms,
                    }) catch return error.OutOfMemory;
                    count += 1;
                    break;
                }
            }
        }
    }

    writer.writeByte(']') catch return error.OutOfMemory;
    return try allocator.dupe(u8, fbs.getWritten());
}

/// Serialize walk results as a JSON array of workflow definitions.
///
/// Looks up each definition name across all shard contexts to produce full JSON
/// matching the format the handler produces: [{"name":..., "version":..., ...}].
fn serializeWalkWorkflowDefs(
    allocator: std.mem.Allocator,
    names: []const []const u8,
    contexts: []const *anyopaque,
    namespace: []const u8,
) ![]u8 {
    var result_buf: [65536]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&result_buf);
    const writer = fbs.writer();

    writer.writeByte('[') catch return error.OutOfMemory;

    var count: u32 = 0;

    for (names) |name| {
        for (contexts) |ctx| {
            const handler: *WorkflowHandler = @ptrCast(@alignCast(ctx));
            var dit = handler.definitions.iterator();
            while (dit.next()) |entry| {
                const def = entry.value_ptr;
                if (!std.mem.eql(u8, def.name_owned, name)) continue;
                // Verify namespace via map key ("namespace:name")
                if (namespace.len > 0) {
                    const map_key = entry.key_ptr.*;
                    if (!std.mem.startsWith(u8, map_key, namespace)) continue;
                    if (map_key.len <= namespace.len or map_key[namespace.len] != ':') continue;
                }
                if (count > 0) writer.writeByte(',') catch return error.OutOfMemory;
                std.fmt.format(writer,
                    \\{{"name":"{s}","version":"{s}","created_at":{d}}}
                , .{
                    def.name_owned,
                    def.version_owned,
                    def.created_at_ms,
                }) catch return error.OutOfMemory;
                count += 1;
                break;
            }
        }
    }

    writer.writeByte(']') catch return error.OutOfMemory;
    return try allocator.dupe(u8, fbs.getWritten());
}

fn serializeWalkNames(allocator: std.mem.Allocator, names: []const []const u8, next_cursor: ?[]const u8) ![]u8 {
    const cursor_bytes = next_cursor orelse &[_]u8{};
    const has_more: u8 = if (next_cursor != null) 1 else 0;

    var total: usize = 4; // count: u32
    for (names) |n| {
        total += 2 + n.len; // name_len: u16 + name bytes
    }
    total += 1; // has_more: u8
    total += 2 + cursor_bytes.len; // cursor_len: u16 + cursor bytes

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    std.mem.writeInt(u32, buf[0..4], @intCast(names.len), .little);
    var pos: usize = 4;
    for (names) |n| {
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(n.len), .little);
        pos += 2;
        @memcpy(buf[pos..][0..n.len], n);
        pos += n.len;
    }
    buf[pos] = has_more;
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(cursor_bytes.len), .little);
    pos += 2;
    if (cursor_bytes.len > 0) {
        @memcpy(buf[pos..][0..cursor_bytes.len], cursor_bytes);
    }
    return buf;
}

/// Serialize walk results in stream list wire format.
///
/// Wire format: [count:u32] ([name_len:u32][name][partition_count:u32])* [has_more:u8] [cursor_len:u16][cursor]
///
/// The stream CLI expects u32 name lengths and a u32 partition_count per entry
/// (distinct from the generic name-list format used by ts_list).
fn serializeWalkStreamNames(allocator: std.mem.Allocator, names: []const []const u8, next_cursor: ?[]const u8, stream: *const StreamProjection) ![]u8 {
    const cursor_bytes = next_cursor orelse &[_]u8{};
    const has_more: u8 = if (next_cursor != null) 1 else 0;

    var total: usize = 4; // count: u32
    for (names) |n| {
        total += 4 + n.len + 4; // name_len: u32 + name bytes + partition_count: u32
    }
    total += 1; // has_more: u8
    total += 2 + cursor_bytes.len; // cursor_len: u16 + cursor bytes

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    std.mem.writeInt(u32, buf[0..4], @intCast(names.len), .little);
    var pos: usize = 4;
    for (names) |n| {
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(n.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..n.len], n);
        pos += n.len;
        // partition_count from stream metadata
        const pc = stream.getPartitionCount(n);
        std.mem.writeInt(u32, buf[pos..][0..4], pc, .little);
        pos += 4;
    }
    buf[pos] = has_more;
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(cursor_bytes.len), .little);
    pos += 2;
    if (cursor_bytes.len > 0) {
        @memcpy(buf[pos..][0..cursor_bytes.len], cursor_bytes);
    }
    return buf;
}

/// Serialize walk results in KV scan wire format (keys_only mode).
///
/// Wire format: [count:u32] ([key_len:u16][key][value_len:u32(=0)])* [has_more:u8] [cursor_len:u16][cursor]
///
/// This matches the KV scan response format that the CLI expects, with
/// value_len=0 for each entry (keys-only walk).
fn serializeWalkKeysAsScan(allocator: std.mem.Allocator, keys: []const []const u8, next_cursor: ?[]const u8) ![]u8 {
    const cursor_bytes = next_cursor orelse &[_]u8{};
    const has_more: u8 = if (next_cursor != null) 1 else 0;

    var total: usize = 4; // count: u32
    for (keys) |k| {
        total += 2 + k.len; // key_len: u16 + key bytes
        total += 4; // value_len: u32 (always 0)
    }
    total += 1; // has_more: u8
    total += 2 + cursor_bytes.len; // cursor_len: u16 + cursor bytes

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    std.mem.writeInt(u32, buf[0..4], @intCast(keys.len), .little);
    var pos: usize = 4;
    for (keys) |k| {
        // Key
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(k.len), .little);
        pos += 2;
        @memcpy(buf[pos..][0..k.len], k);
        pos += k.len;
        // Value length = 0 (keys-only)
        std.mem.writeInt(u32, buf[pos..][0..4], 0, .little);
        pos += 4;
    }
    buf[pos] = has_more;
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(cursor_bytes.len), .little);
    pos += 2;
    if (cursor_bytes.len > 0) {
        @memcpy(buf[pos..][0..cursor_bytes.len], cursor_bytes);
    }
    return buf;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Waiter Callbacks — used by WaiterPool for timeout and resolution
// ═══════════════════════════════════════════════════════════════════════════════

/// Timeout callback: send an appropriate "no data" response based on waiter kind.
fn handleWaiterTimeout(waiter: *const Waiter, ctx: *anyopaque) void {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const conn = shard.getConnection(waiter.fd) orelse return;
    switch (waiter.kind) {
        .kv_get => {
            // KV blocking GET timeout → not_found
            var resp = proto.Response.initError(waiter.request_id, .not_found);
            var buf: [128]u8 = undefined;
            if (resp.serialize(&buf)) |serialized| {
                _ = conn.queueWrite(serialized);
                shard.flushToClient(waiter.fd);
            } else |_| {}
        },
        .stream_read => {
            // Stream blocking read timeout → empty messages response
            shard.sendOkResponse(conn, waiter.request_id, "");
            shard.flushToClient(waiter.fd);
        },
        .queue_dequeue => {
            // Queue blocking dequeue timeout → empty messages response
            var buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &buf, 0, .little); // count = 0
            shard.sendOkResponse(conn, waiter.request_id, &buf);
            shard.flushToClient(waiter.fd);
        },
        .worker_await => {
            // Worker await timeout → empty response (no task available)
            shard.sendOkResponse(conn, waiter.request_id, "");
            shard.flushToClient(waiter.fd);
        },
    }
}

/// KV waiter resolver: look up the key in the KV projection and send the value
/// if version > min_version. Returns true if waiter was satisfied.
pub fn resolveKVWaiter(waiter: *const Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const entry = shard.defaultPartition().kv.get(waiter.key()) orelse return false;
    if (entry.lsn <= waiter.min_version) return false;

    const conn = shard.getConnection(waiter.fd) orelse return true; // connection gone, remove waiter
    var resp = proto.Response.init(waiter.request_id, .ok, entry.value);
    resp.prefix = entry.lsn;
    const MAX_BUF = @sizeOf(proto.ResponseHeader) + 8 + (256 * 1024);
    var buf: [MAX_BUF]u8 = undefined;
    if (resp.serialize(&buf)) |serialized| {
        _ = conn.queueWrite(serialized);
        shard.flushToClient(waiter.fd);
    } else |_| {}
    return true;
}

/// Stream waiter resolver: read new messages starting from min_version (last offset).
/// Returns true if there are new messages to send.
pub fn resolveStreamWaiter(waiter: *const Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const partition = shard.defaultPartition();

    const hwm = partition.stream.highWaterMark();
    if (hwm <= waiter.min_version) return false; // no new messages

    // Read from (min_version+1) to hwm
    const start = waiter.min_version + 1;
    const end = start + 100; // cap batch size
    var entries: [100]@import("../projection/stream.zig").OffsetEntry = undefined;
    const count = partition.stream.readRange(start, end, &entries);
    if (count == 0) return false;

    const conn = shard.getConnection(waiter.fd) orelse return true;

    // Compute actual start (readRange clamps to max(start, trim_offset+1))
    const actual_start = @max(start, partition.stream.trim_offset + 1);

    // Serialize with full message format including payloads
    const data = shard.stream_handler.serializeMessagesWithPayloads(entries[0..count], actual_start) catch return false;
    defer shard.stream_handler.allocator.free(data);
    shard.sendOkResponse(conn, waiter.request_id, data);
    shard.flushToClient(waiter.fd);
    return true;
}

/// Queue waiter resolver: try to dequeue a message.
/// Returns true if a message was available and sent.
pub fn resolveQueueWaiter(waiter: *const Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const partition = shard.defaultPartition();

    const now_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
    partition.queue.expireLeases(now_ns);

    // min_version holds the pre-computed queue_name_hash
    const queue_name_hash = waiter.min_version;
    const maybe_result = partition.queue.dequeue(now_ns, queue_name_hash) catch return false;
    const deq_result = maybe_result orelse return false;

    const conn = shard.getConnection(waiter.fd) orelse return true;

    // Serialize BEFORE auto-ack (ack frees the message payload).
    const results = [1]@import("../projection/queue.zig").DequeueResult{deq_result};
    const data = queue_handler_mod.serializeDequeueResultsPub(shard.queue_handler.allocator, &results) catch return false;
    defer shard.queue_handler.allocator.free(data);

    // Auto-ack: persist a queue_ack entry so the message doesn't reappear after restart
    {
        var seq_key: [8]u8 = undefined;
        std.mem.writeInt(u64, &seq_key, deq_result.seq, .little);
        const payload_size = entry_mod.COMMAND_PREFIX_SIZE + 8;
        var payload_buf: [entry_mod.COMMAND_PREFIX_SIZE + 8]u8 = undefined;
        if (entry_mod.buildCommandEntry(
            .queue_ack,
            entry_mod.Flags.NONE,
            partition.current_term,
            partition.ual.max_index + 1,
            now_ns,
            0, // namespace hash not needed for ack
            &seq_key,
            &[_]u8{},
            payload_buf[0..payload_size],
        )) |entry| {
            _ = partition.apply(&entry) catch {};
        }
    }

    shard.sendOkResponse(conn, waiter.request_id, data);
    shard.flushToClient(waiter.fd);
    return true;
}

// ═══════════════════════════════════════════════════════════════════════════════
// UAL Persistence Callback
// ═══════════════════════════════════════════════════════════════════════════════

/// Called by UAL.append() after every entry write. Feeds the entry to the
/// SegmentWriter so it's included in the next segment flush to disk.
/// This is the designed hot → warm flush path (UNIFIED_STORAGE_DESIGN §4.3).
fn ualPersistCallback(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
    const writer: *SegmentWriter = @ptrCast(@alignCast(ctx));
    writer.addEntry(entry) catch {};
}

// ═══════════════════════════════════════════════════════════════════════════════
// Segment Replay — recover state from .flseg files on startup
// ═══════════════════════════════════════════════════════════════════════════════

/// Replay all .flseg segment files in `dir_path` into the partition.
/// Entries are loaded into the UAL (for payload reads) and applied to projections.
/// Stream entries additionally rebuild the StreamProjection offset tracking,
/// since the ProjectionRouter skips stream entries (UAL direct reads by design).
/// Tracks the maximum entry index seen for LSN restoration.
///
/// If `replay_from` > 0, entries with index <= replay_from are skipped
/// (already restored from a snapshot).
fn replaySegments(
    allocator: std.mem.Allocator,
    dir_path: []const u8,
    partition: *Partition,
    max_index: *u64,
    workflow_handler: *WorkflowHandler,
    replay_from: u64,
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

        // Iterate entries and apply to partition (UAL + projections)
        var offset: usize = 0;
        const data_len = result.reader.data_end - result.reader.data_start;
        while (offset < data_len) {
            const seg_entry = result.reader.readEntryAt(offset) orelse break;

            // Skip entries already covered by snapshot
            if (replay_from > 0 and seg_entry.header.index <= replay_from) {
                // Still track max index for LSN restoration
                if (seg_entry.header.index > max_index.*) {
                    max_index.* = seg_entry.header.index;
                }
                offset += seg_entry.totalSize();
                continue;
            }

            // Apply to UAL + projection router (handles KV, queue, TS routing)
            const ual_index = partition.apply(&seg_entry) catch {
                offset += seg_entry.totalSize();
                continue;
            };

            // Stream entries: router skips them (.none), so manually rebuild
            // the StreamProjection offset→ual_index mapping.
            const etype: entry_mod.EntryType = @enumFromInt(seg_entry.header.entry_type);
            if (etype == .stream_append) {
                // Extract stream name hash from command payload for per-stream filtering.
                // Must use namespace_hash as Wyhash seed (matches router.nameHash).
                if (entry_mod.CommandPayload.deserialize(seg_entry.payload)) |cmd| {
                    const name_hash = std.hash.Wyhash.hash(@as(u64, cmd.namespace_hash), cmd.key);
                    _ = partition.stream.append(ual_index, seg_entry.header.timestamp_ns, name_hash, 0) catch {};
                    // Re-register stream name for listing (bare — namespace string
                    // isn't stored in entries, so post-restart ls won't filter by
                    // namespace; acceptable since restart ls isn't tested yet).
                    partition.stream.registerStream(cmd.key) catch {};
                }
            }

            // Workflow entries: router skips them (.none), so manually rebuild
            // the WorkflowHandler in-memory definition and run maps.
            if (etype == .workflow_create or etype == .workflow_start) {
                workflow_handler.replayEntry(&seg_entry);
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

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0);
    defer shard.deinit();

    try std.testing.expectEqual(@as(u16, 0), shard.id);
    try std.testing.expectEqual(@as(u32, 0), shard.connectionCount());
    try std.testing.expect(!shard.running);
}

test "Shard: add and remove connections" {
    const pipe_fds = try std.posix.pipe();
    defer std.posix.close(pipe_fds[0]);
    defer std.posix.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0);
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

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0);
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

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0);
    defer shard.deinit();

    shard.running = true;
    try std.testing.expect(shard.running);

    // Send shutdown via inbox
    const sent = shard.inbox.send(.{
        .tag = .shutdown,
        .src_shard = 1,

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
