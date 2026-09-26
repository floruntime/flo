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
const dispatcher_mod = @import("dispatcher.zig");
const Dispatcher = dispatcher_mod.Dispatcher;
const HandlerFn = @import("dispatcher.zig").HandlerFn;
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
const kv_handler_mod = @import("../kv/handler.zig");
const projection_router = @import("../projection/router.zig");
const txn_mod = @import("../kv/txn.zig");
const stream_mod = @import("../projection/stream.zig");
const stream_proj_mod = @import("../projection/stream.zig");
const StreamProjection = stream_proj_mod.StreamProjection;
const StreamID = stream_proj_mod.StreamID;
const StreamHandler = @import("../stream/handler.zig").StreamHandler;
const QueueProjection = @import("../projection/queue.zig").QueueProjection;
const QueueHandler = @import("../queue/handler.zig").QueueHandler;
const TSProjection = @import("../projection/ts.zig").TSProjection;
const TSHandler = @import("../ts/handler.zig").TSHandler;
const handler_mod = @import("../namespace/handler.zig");
const NamespaceHandler = handler_mod.NamespaceHandler;
const ActionsHandler = @import("../actions/handler.zig").ActionsHandler;
const WorkerHandler = @import("../worker/handler.zig").WorkerHandler;
const WorkflowHandler = @import("../workflow/handler.zig").WorkflowHandler;
const ProcessingHandler = @import("../processing/handler.zig").ProcessingHandler;
const TaskScheduler = @import("task_scheduler.zig").TaskScheduler;
const ual_mod = @import("../storage/ual/ual.zig");
const UAL = ual_mod.UAL;
const SegmentWriter = @import("../storage/ual/writer.zig").SegmentWriter;
const durable_log_mod = @import("../storage/durable_log.zig");
const DurableLog = durable_log_mod.DurableLog;
const RaftLog = @import("../raft/log.zig").RaftLog;
const hard_state_mod = @import("../raft/hard_state.zig");
const Durability = @import("../config/server.zig").Durability;
const SegmentReader = @import("../storage/ual/reader.zig").SegmentReader;
const entry_mod = @import("../storage/ual/entry.zig");
const segment_mod = @import("../storage/ual/segment.zig");
const Entry = entry_mod.Entry;
const RaftNetwork = @import("../raft/network.zig").RaftNetwork;
const RaftQueue = @import("../raft/raft_queue.zig").RaftQueue;
const RaftNode = @import("../raft/node.zig").RaftNode;
const raft_node_mod = @import("../raft/node.zig");
const transport = @import("../raft/transport.zig");
const membership = @import("../raft/membership.zig");
const RaftFrame = @import("../raft/raft_queue.zig").Frame;
const waiter_pool_mod = @import("waiter_pool.zig");
const WaiterPool = waiter_pool_mod.WaiterPool;
const Waiter = waiter_pool_mod.Waiter;
const WaiterKind = waiter_pool_mod.WaiterKind;
const stream_handler_mod = @import("../stream/handler.zig");
const queue_handler_mod = @import("../queue/handler.zig");
const Partition = @import("../storage/partition.zig").Partition;
const persistence_mod = @import("../storage/persistence.zig");
const ReplayRegistry = persistence_mod.ReplayRegistry;
const snapshot_mod = @import("../storage/snapshot.zig");
const shard_manifest = @import("shard_manifest.zig");
const ShardManifest = shard_manifest.ShardManifest;
const Forwarder = @import("../cluster/forwarder.zig").Forwarder;
const PartitionTable = @import("../cluster/partition_table.zig").PartitionTable;
const Coordinator = @import("../cluster/coordinator.zig").Coordinator;
const NodeId = @import("../raft/node.zig").NodeId;
pub const run_id_mod = @import("run_id.zig");
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;
const ShardMetrics = @import("../metrics/registry.zig").ShardMetrics;

/// Maximum single-request size we handle on the stack.
const MAX_REQUEST_SIZE = 256 * 1024; // 256 KB
/// A read buffer grows to hold one whole request (and the start of the next).
const MAX_READ_BUFFER = 2 * MAX_REQUEST_SIZE;

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

    /// Worker registry — tracks physical worker health.
    worker_handler: *WorkerHandler,

    /// Workflow handler instance.
    workflow_handler: *WorkflowHandler,

    // Processing handler instance.
    processing_handler: *ProcessingHandler,

    /// Segment writer — accumulates entries for persistence to .flseg files.
    segment_writer: *SegmentWriter,

    /// The writer's buffer plus the sealed segments, as one log the Raft
    /// node can read below its ring and truncate through (null if ephemeral).
    durable_log: ?*DurableLog,

    /// Count of segment flush failures. Surfaced instead of being silently
    /// swallowed so disk-full / IO faults are observable.
    persist_failures: u64,

    /// Storage durability — controls segment flush timing.
    durability: Durability,

    /// Per-shard data directory path (owned, null if ephemeral).
    shard_data_dir: ?[]const u8,

    /// Whether the acceptor pipe has been registered with the reactor.
    pipe_registered: bool,

    /// Raft network reference (set by runtime for shard 0, null otherwise).
    raft_network: ?*RaftNetwork,

    /// Frames from authenticated peers, filled by the network thread (set
    /// by the runtime for shard 0). Its wake pipe is a reactor source.
    raft_queue: ?*RaftQueue,
    raft_queue_registered: bool,

    /// Per-shard counters, mirroring the global server ones for attribution.
    shard_metrics: ?*ShardMetrics,

    /// This node's cluster-wide node ID, set by the runtime at start. Distinct
    /// from `raft_node.id`, which identifies the per-shard Raft group.
    cluster_node_id: u32,

    /// Raft consensus node — every shard has one. Writes go through
    /// propose() → commit → apply to projections.
    raft_node: *RaftNode,
    /// How this shard's group came up: alone, as the first member, or
    /// joining members that exist.
    cluster_role: ClusterRole,
    /// Scratch for one AppendEntries in either direction: the entries of a
    /// batch, the payload bytes they point into, and the frame payload.
    rpc_entries: []entry_mod.Entry,
    rpc_arena: []u8,
    rpc_out: []u8,
    /// When a node with no membership last asked the peers it can reach
    /// to be added.
    join_asked_ms: u64,
    /// Proposals whose client waits for commit, by index. A slot is never
    /// contended: the Raft node refuses proposals past MAX_OUTSTANDING and
    /// the table is twice that.
    pending: []Pending,
    pending_count: u32,
    /// The entry a responder is answering for: its index and header
    /// timestamp, set just before the responder runs. A responder otherwise
    /// reads what that entry's applier recorded in its module's `last_*`
    /// fields; each applier clears its field first, so an entry it refused
    /// answers as a failure (or zero), never with the entry before's result.
    answering_index: u64,
    answering_timestamp_ns: u64,
    /// The connection a responder answers on when the client's is not on
    /// this thread or no longer known: carries (owner, fd, id) and collects
    /// the bytes, which go out through `deliverDeferred`.
    respond_proxy: *Connection,
    /// Writes this node received while not leading, sent on to the leader
    /// and waiting for its answer; and answers held until this node has
    /// applied what the leader committed, so the client reads its write.
    forwards: []Forward,
    forward_count: u32,
    replies_held: u32,
    next_forward_id: u32,
    /// The leader disagreed with history this node had committed and
    /// applied: its projections cannot be trusted, so it takes no part
    /// until it is wiped and rejoined.
    diverged: bool,
    conflicts_seen: u64,
    join_first_asked_ms: u64,
    join_warned_ms: u64,
    join_refused_warn_ms: u64,
    stranger_warn_ms: u64,
    frame_warn_ms: u64,
    late_apply_warn_ms: u64,
    election_warn_ms: u64,
    elections_unlogged: u64,
    /// One limiter per peer for each thing the leader loop says about it.
    peer_silent_warn_ms: [raft_node_mod.MAX_PEERS]u64,
    peer_batch_warn_ms: [raft_node_mod.MAX_PEERS]u64,

    /// Where the Raft node persists its term and vote (null if ephemeral).
    hard_state_store: ?*HardStateStore,

    /// Unified waiter pool — handles blocking GET, blocking dequeue,
    /// stream long-poll, and action_await across all subsystems.
    waiter_pool: WaiterPool,

    /// Cooperative periodic background tasks (hot_flush, TTL sweep, etc.).
    task_scheduler: TaskScheduler,

    /// Maximum age of entries in the hot ring (seconds). 0 = disabled.
    hot_flush_seconds: u64,

    /// Cross-node request forwarder (null in single-node mode).
    forwarder: ?*Forwarder,

    /// Cross-shard shard pointer array (null until wired by runtime).
    /// Enables direct dispatch on another shard's handlers for pre-routed requests.
    peer_shards: ?[]*Shard,

    /// Cross-shard inbox array (null until wired by runtime).
    /// Enables sending messages to other shards' inboxes.
    peer_inboxes: ?[]*Inbox,

    /// Per-shard proxy Connection used to drive forwarded requests on this
    /// shard's reactor thread. The real Connection lives on the owner shard;
    /// this proxy carries (fd, conn_id, owner_shard) so handlers register
    /// waiters with the correct routing target, and accumulates direct
    /// responses in its `write_buf` for cross-shard delivery via the inbox.
    /// Reused sequentially — safe because each shard's reactor is single-
    /// threaded and `drainInbox` processes one message at a time.
    forward_proxy: *Connection,

    /// Cluster partition table (null in single-node mode).
    partition_table: ?*PartitionTable,

    /// Controller Raft coordinator (set on Shard 0 — routes namespace
    /// create/delete through Raft for multi-node consistency).
    coordinator: ?*Coordinator,

    /// Replay registry — maps EntryType → handler apply callback for the
    /// entry types no projection owns. Used by the one applier, whether the
    /// entry comes from this node's Raft log, a peer, or a segment at boot.
    replay_registry: ReplayRegistry,

    /// Copy buffer for committed entries, sized for the largest entry any
    /// handler proposes. Allocated at boot so an out-of-memory is a boot
    /// failure, not a committed write that never reaches the projection.
    apply_buf: []u8,
    /// True while `applyCommitted` is draining, so a waiter woken by an
    /// apply that proposes in turn cannot start a nested drain.
    applying: bool,
    /// Whether the last entry the apply loop took applied; `park` answers
    /// from it, not from the whole pass's result.
    last_entry_applied: bool,
    /// An invoke applied on this leader in the current pass: its workers
    /// are woken once, after the pass.
    wake_workers: bool,
    /// The next poll does not wait: the tick's step passes stopped at their
    /// cap with work left, or resumed connections proposed writes the next
    /// pump sends. Only a leader sets it; nothing clears it on a follower.
    more_work: bool,
    /// Connections marked closing (`markClosing`), closed at the end of the
    /// tick. Room for one entry per connection is reserved as each is
    /// added, so marking never fails.
    closing_fds: std.ArrayListUnmanaged(i32) = .empty,
    /// Paused connections to read again (`flushToClient`, the stall check);
    /// their buffered requests run at the end of the tick. The end of the
    /// tick runs a swapped-out snapshot (`resume_running`), so what is
    /// queued while it runs waits for the next tick: one entry per
    /// connection at most, and both lists are reserved as above.
    resume_fds: std.ArrayListUnmanaged(i32) = .empty,
    resume_running: std.ArrayListUnmanaged(i32) = .empty,
    /// Connections paused now, and when the stall check next runs.
    paused_count: u32 = 0,
    stall_check_ms: u64 = 0,
    overflow_warn_ms: u64 = 0,
    overflow_unsaid: u64 = 0,

    /// Self-routing run ID generator (per-shard, single-threaded).
    run_id_gen: run_id_mod.Generator,

    /// Global metrics registry (optional, set by runtime when dashboard is enabled).
    metrics_registry: ?*MetricsRegistry,

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
        durability: Durability,
        node_id: u32,
        cluster_role: ClusterRole,
        raft_config: raft_node_mod.Config,
    ) !Shard {
        var reactor = try Reactor.init(allocator);
        errdefer reactor.deinit();

        var slab = try SlabAllocator.init(allocator);
        errdefer slab.deinit();

        var inbox = try Inbox.init(allocator, 1024);
        errdefer inbox.deinit();
        // Without its wake a shard sees another's message only at its poll
        // timeout; a shard that cannot have one does not start.
        try inbox.initWake();
        try reactor.addSource(.{ .fd = inbox.wake_rd, .tag = .inbox_ready, .interests = .{ .readable = true } });

        // Forward-proxy Connection: drives requests forwarded from peer
        // shards. fd=-1 means "no kernel socket" — handlers only touch
        // write_buf / response_deferred / metadata, never the fd.
        const forward_proxy = try allocator.create(Connection);
        errdefer allocator.destroy(forward_proxy);
        forward_proxy.* = try Connection.init(allocator, -1, 0, @as(u16, @intCast(shard_id)));
        forward_proxy.protocol = .binary;
        errdefer forward_proxy.deinit();

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
        errdefer kv_handler.deinit();

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

        // Wire the queue projection's namespace resolver to the (stable, heap-allocated)
        // namespace handler. Done HERE — before replaySegments runs below — so that
        // queues re-registered during replay resolve their real namespace instead of
        // falling back to "default". (namespace_create entries replay before the
        // queue_enqueue entries that reference them, so the registry is populated in time.)
        queue_handler.queue.ns_resolver = resolveQueueNamespace;
        queue_handler.queue.ns_resolver_ctx = @ptrCast(namespace_handler);
        stream_handler.stream.ns_resolver = resolveQueueNamespace;
        stream_handler.stream.ns_resolver_ctx = @ptrCast(namespace_handler);

        // Create Actions handler
        const actions_handler = try allocator.create(ActionsHandler);
        errdefer allocator.destroy(actions_handler);
        actions_handler.* = ActionsHandler.init(allocator);

        // Create Worker handler (worker registry)
        const worker_handler = try allocator.create(WorkerHandler);
        errdefer allocator.destroy(worker_handler);
        worker_handler.* = WorkerHandler.init(allocator);

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

        // Create Raft consensus node, identified by this node's cluster id
        // (the group id tells shards apart). Bootstrapped below, after replay
        // and the segment-writer hook.
        const raft_node = try allocator.create(RaftNode);
        errdefer allocator.destroy(raft_node);
        raft_node.* = try RaftNode.init(allocator, node_id, @as(u32, shard_id), raftRingCapacity(ual_capacity), raft_config);
        errdefer raft_node.deinit();
        const rpc_entries = try allocator.alloc(entry_mod.Entry, RPC_MAX_ENTRIES);
        errdefer allocator.free(rpc_entries);
        const rpc_arena = try allocator.alloc(u8, RPC_BATCH_BYTES);
        errdefer allocator.free(rpc_arena);
        const rpc_out = try allocator.alloc(u8, transport.APPEND_REQ_PREFIX + RPC_MAX_ENTRIES * entry_mod.HEADER_SIZE + RPC_BATCH_BYTES);
        errdefer allocator.free(rpc_out);
        const pending = try allocator.alloc(Pending, PENDING_SLOTS);
        errdefer allocator.free(pending);
        @memset(pending, .{});
        const respond_proxy = try allocator.create(Connection);
        errdefer allocator.destroy(respond_proxy);
        respond_proxy.* = try Connection.init(allocator, -1, 0, @as(u16, @intCast(shard_id)));
        respond_proxy.protocol = .binary;
        errdefer respond_proxy.deinit();
        const forwards = try allocator.alloc(Forward, FORWARD_SLOTS);
        errdefer allocator.free(forwards);
        @memset(forwards, .{});

        var shard_data_dir: ?[]const u8 = null;
        var hard_state_store: ?*HardStateStore = null;
        errdefer if (hard_state_store) |store| allocator.destroy(store);
        var durable_log: ?*DurableLog = null;
        errdefer if (durable_log) |dl| {
            dl.deinit();
            allocator.destroy(dl);
        };

        // Build replay registry — handlers register their entry types
        // so replaySegments and handleInboxMessage can dispatch without
        // hardcoded type checks. Created before data_dir block so it's
        // always available for the Shard struct.
        var replay_registry: ReplayRegistry = .{};
        // A committed config is the membership a truncation falls back to;
        // the Raft node is on the heap, so it is the applier's context.
        replay_registry.register(.raft_config, @ptrCast(raft_node), applyRaftConfig);
        workflow_handler.registerReplay(&replay_registry);
        namespace_handler.registerReplay(&replay_registry);
        actions_handler.registerReplay(&replay_registry);
        processing_handler.registerReplay(&replay_registry);
        stream_handler.registerReplay(&replay_registry);
        try assertOneApplier(shard_id, &replay_registry);

        const apply_buf = try allocator.alloc(u8, kv_handler_mod.MAX_APPLY_PAYLOAD);
        errdefer allocator.free(apply_buf);

        if (data_dir) |dir| {
            // Build shard-specific data directory: data_dir/00000/
            const shard_dir = try std.fmt.allocPrint(allocator, "{s}/{d:0>5}", .{ dir, shard_id });
            errdefer allocator.free(shard_dir);

            // Ensure shard dir and subdirectories exist
            @import("stdx").fs.makePath(shard_dir) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };

            const segs_dir_path = try std.fmt.allocPrint(allocator, "{s}/segs", .{shard_dir});
            defer allocator.free(segs_dir_path);
            @import("stdx").fs.makePath(segs_dir_path) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };
            const dl = try allocator.create(DurableLog);
            dl.* = DurableLog.init(allocator, seg_writer, segs_dir_path) catch |err| {
                allocator.destroy(dl);
                return err;
            };
            durable_log = dl;

            const snaps_dir_path = try std.fmt.allocPrint(allocator, "{s}/snaps", .{shard_dir});
            defer allocator.free(snaps_dir_path);
            @import("stdx").fs.makePath(snaps_dir_path) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };

            // ── Hard state ──────────────────────────────────────────────
            // The term and vote this shard's Raft node must not forget.
            const loaded = hard_state_mod.load(shard_dir) catch |err| {
                log.err("shard {d}: cannot read {s}/{s}: {s}", .{ shard_id, shard_dir, hard_state_mod.FILENAME, @errorName(err) });
                return err;
            };
            if (loaded) |hs| {
                if (hs.node_id != node_id) {
                    log.err("shard {d}: {s}/HARDSTATE belongs to node {d}, this node is {d}; the shard directories come from different nodes", .{ shard_id, shard_dir, hs.node_id, node_id });
                    return error.NodeIdMismatch;
                }
                raft_node.current_term = hs.term;
                raft_node.voted_for = hs.voted_for;
            }
            const store = try allocator.create(HardStateStore);
            store.* = .{ .dir = shard_dir, .node_id = node_id, .shard_id = shard_id };
            hard_state_store = store;
            raft_node.hard_state_sink = .{ .ctx = @ptrCast(store), .persist = HardStateStore.persist };

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
                        if (@import("stdx").fs.openDir(snaps_dir_path, .{})) |snap_dir_handle| {
                            const snap_dir = snap_dir_handle;
                            defer @import("stdx").fs.closeDir(snap_dir);

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
            // Replay also rebuilds the Raft log (last index, term index and
            // the hot-ring tail) from the same pass over the segments.
            //
            // Only entries at or below the segments' commit watermark reach
            // the projections; the rest is loaded into the log and drained
            // by `applyDeferredTail` (see `SegmentHeader.commit_index_at_seal`).
            try durable_log.?.recoverTruncation();
            const watermark = try durable_log.?.watermark();
            try replaySegments(allocator, durable_log.?, partition, &replay_registry, replay_from, watermark, &raft_node.log, shard_id);
            // A snapshot covers its prefix already; draining from below it
            // would apply those entries a second time.
            raft_node.last_applied = @max(replay_from, @min(watermark, raft_node.log.lastIndex()));
            if (watermark == 0 and !raft_node.log.isEmpty()) {
                log.warn("shard {d}: no segment carries a commit watermark; everything above the snapshot is applied at boot through the log instead of replay", .{shard_id});
            }

            // A snapshot ahead of the flushed segments (async flush, crash
            // after the snapshot) would leave the log below what the
            // projections hold, and every later index would be skipped as
            // already applied. The log continues from the snapshot instead.
            if (raft_node.log.lastIndex() < replay_from) {
                raft_node.log.resetToSnapshot(replay_from, partition.current_term);
            }

            shard_data_dir = shard_dir;
        }
        // The block's own errdefer for the path ended with the block.
        errdefer if (shard_data_dir) |d| allocator.free(d);

        // Feed every Raft log append to the segment writer from here on.
        // Attached after replay (a hook active during replay would re-buffer
        // every entry just read) and before bootstrap, so the noop that
        // opens each leadership term is durable like any other entry and
        // the on-disk log has no holes at the next boot.
        // Only the Raft log UAL persists — partition.ual is a read cache
        // whose entries are already covered by the Raft log's persistence.
        raft_node.log.ual.on_append_ctx = @ptrCast(seg_writer);
        raft_node.log.ual.on_append = segmentBufferCallback;
        if (durable_log) |dl| {
            raft_node.log.catch_up = .{ .ctx = @ptrCast(dl), .read_range = catchUpReadRange };
            raft_node.log.on_truncate_ctx = @ptrCast(dl);
            raft_node.log.on_truncate = durableTruncate;
        }
        try bringUpGroup(raft_node, cluster_role, apply_buf, shard_id, node_id);

        // Build dispatcher and register all handlers
        var dispatcher = Dispatcher.init();
        KVHandler.register(&dispatcher);
        StreamHandler.register(&dispatcher);
        QueueHandler.register(&dispatcher);
        TSHandler.register(&dispatcher);
        NamespaceHandler.register(&dispatcher);
        ActionsHandler.register(&dispatcher);
        WorkerHandler.register(&dispatcher);
        WorkflowHandler.register(&dispatcher);
        ProcessingHandler.register(&dispatcher);
        dispatcher.register(.cluster_status, dispatchClusterStatus);

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
            .worker_handler = worker_handler,
            .workflow_handler = workflow_handler,
            .processing_handler = processing_handler,
            .raft_node = raft_node,
            .cluster_role = cluster_role,
            .rpc_entries = rpc_entries,
            .rpc_arena = rpc_arena,
            .rpc_out = rpc_out,
            .join_asked_ms = 0,
            .pending = pending,
            .pending_count = 0,
            .answering_index = 0,
            .answering_timestamp_ns = 0,
            .respond_proxy = respond_proxy,
            .forwards = forwards,
            .forward_count = 0,
            .replies_held = 0,
            .next_forward_id = FORWARD_ID_FIRST,
            .diverged = false,
            .conflicts_seen = 0,
            .join_first_asked_ms = 0,
            .join_warned_ms = 0,
            .join_refused_warn_ms = 0,
            .stranger_warn_ms = 0,
            .frame_warn_ms = 0,
            .late_apply_warn_ms = 0,
            .election_warn_ms = 0,
            .elections_unlogged = 0,
            .peer_silent_warn_ms = [_]u64{0} ** raft_node_mod.MAX_PEERS,
            .peer_batch_warn_ms = [_]u64{0} ** raft_node_mod.MAX_PEERS,
            .hard_state_store = hard_state_store,
            .segment_writer = seg_writer,
            .durable_log = durable_log,
            .persist_failures = 0,
            .durability = durability,
            .shard_data_dir = shard_data_dir,
            .pipe_registered = false,
            .raft_network = null,
            .raft_queue = null,
            .raft_queue_registered = false,
            .shard_metrics = null,
            .cluster_node_id = node_id,
            .waiter_pool = WaiterPool.init(),
            .task_scheduler = TaskScheduler.init(),
            .hot_flush_seconds = hot_flush_seconds,
            .forwarder = null,
            .peer_shards = null,
            .peer_inboxes = null,
            .forward_proxy = forward_proxy,
            .partition_table = null,
            .coordinator = null,
            .replay_registry = replay_registry,
            .apply_buf = apply_buf,
            .applying = false,
            .last_entry_applied = true,
            .wake_workers = false,
            .more_work = false,
            .run_id_gen = .{ .shard = @intCast(shard_id) },
            .metrics_registry = null,
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

    /// Wire the Controller Raft coordinator (enables Raft-replicated namespace ops).
    /// Should only be called on Shard 0.
    pub fn setCoordinator(self: *Shard, coord: *Coordinator) void {
        self.coordinator = coord;
    }

    /// Namespace resolver wired into the queue and stream projections: hash →
    /// name via the shard's namespace registry. `ctx` is the (stable, heap-allocated)
    /// `*NamespaceHandler`, so this is safe to call during replay — before the
    /// shard's back-pointers are wired.
    fn resolveQueueNamespace(ctx: *anyopaque, ns_hash: u32) ?[]const u8 {
        const nh: *NamespaceHandler = @ptrCast(@alignCast(ctx));
        return nh.nameForHash(ns_hash);
    }

    /// Wire shard back-pointers into handlers that need Raft access.
    /// Must be called after shards are at their final heap addresses.
    pub fn wireHandlerShardPtrs(self: *Shard) void {
        self.stream_handler.shard_ptr = @ptrCast(self);
        self.queue_handler.shard_ptr = @ptrCast(self);
        self.ts_handler.shard_ptr = @ptrCast(self);
    }

    /// Flush buffered Raft log entries to a .flseg file under `shard_data_dir/segs/`,
    /// sealed under the current commit index.
    pub fn flushSegmentToDisk(self: *Shard) !void {
        const dl = self.durable_log orelse return;
        try dl.flush(self.raft_node.commit_index);
    }

    /// Apply what replay loaded into the log above the commit watermark.
    /// Called once the shard is at its final address and before it serves
    /// anything, so no read observes the gap. A shard that bootstrapped
    /// re-established commit at the tip and drains the whole tail; one
    /// that follows has nothing to drain until a leader speaks.
    pub fn applyDeferredTail(self: *Shard) void {
        const raft = self.raft_node;
        const pending = raft.commit_index -| raft.last_applied;
        // A bootstrapped shard's noop is always one of them.
        if (pending > 1) {
            log.info("shard {d}: applying {d} durable entries above the commit watermark (indices {d}..{d})", .{ self.id, pending, raft.last_applied + 1, raft.commit_index });
        }
        if (!self.applyCommitted()) {
            log.err("shard {d}: not every durable entry above the commit watermark could be applied at boot; projections are missing writes", .{self.id});
        }
    }

    /// Flush segments when `durability == .sync` (after projections are applied).
    pub fn syncFlushIfNeeded(self: *Shard) void {
        if (self.durability == .sync) {
            self.flushSegmentToDisk() catch |err| {
                // In sync mode a flush failure breaks the durability contract —
                // surface it loudly instead of acking a write that isn't on disk.
                self.persist_failures += 1;
                log.err("shard {d}: sync flush failed: {s} (persist_failures={d})", .{ self.id, @errorName(err), self.persist_failures });
            };
        }
    }

    fn inboxPending(ctx: *const anyopaque) u64 {
        const inbox: *const Inbox = @ptrCast(@alignCast(ctx));
        return inbox.pending();
    }

    /// Wire the global MetricsRegistry into this shard and its handlers.
    /// Called by runtime after the registry is created.
    pub fn setMetricsRegistry(self: *Shard, registry: *MetricsRegistry) void {
        self.metrics_registry = registry;
        // Resolved once: the shard table is sized by `initShards` before this
        // runs, and the pointer is stable for the registry's lifetime. Without
        // it every `flo_shard_*` series exports 0 while the global `flo_*`
        // equivalents move, which reads as "this shard is idle".
        self.shard_metrics = registry.shardMetrics(self.id);
        if (self.shard_metrics) |sm| sm.live_pending = .{ .ctx = &self.inbox, .read = inboxPending };
        self.stream_handler.metrics_registry = registry;
        // Tier-hit counters are per log; resolve once so the read path avoids a
        // registry lookup per record. Also the only caller of registerTieredLog,
        // without which the flo_tiered_log_* family never appears at all.
        self.stream_handler.tiered_metrics = registry.registerTieredLog(self.id) catch null;
        self.queue_handler.metrics_registry = registry;
        self.kv_handler.metrics_registry = registry;
    }

    pub fn deinit(self: *Shard) void {
        self.closing_fds.deinit(self.allocator);
        self.resume_fds.deinit(self.allocator);
        self.resume_running.deinit(self.allocator);
        // Close all connections (close fds + free buffers)
        var it = self.connections.iterator();
        while (it.next()) |entry| {
            const fd = entry.key_ptr.*;
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            _ = std.c.close(fd);
        }
        self.connections.deinit(self.allocator);

        // Flush pending entries to segment file on shutdown
        self.flushSegmentToDisk() catch |err| {
            log.err("shard {d}: final flush on shutdown failed: {s} (buffered entries may be lost)", .{ self.id, @errorName(err) });
        };

        if (self.durable_log) |dl| {
            dl.deinit();
            self.allocator.destroy(dl);
        }
        // Clean up SegmentWriter
        self.segment_writer.deinit();
        self.allocator.destroy(self.segment_writer);

        // The store borrows shard_data_dir, so it goes first.
        if (self.hard_state_store) |store| self.allocator.destroy(store);
        if (self.shard_data_dir) |dir| {
            self.allocator.free(dir);
        }

        // Clean up handlers (they don't own projections — partitions do)
        self.kv_handler.deinit();
        self.allocator.destroy(self.kv_handler);
        self.stream_handler.deinit();
        self.allocator.destroy(self.stream_handler);
        self.allocator.destroy(self.queue_handler);
        self.allocator.destroy(self.ts_handler);

        // Clean up Raft consensus node
        self.raft_node.deinit();
        self.allocator.destroy(self.raft_node);
        for (self.forwards) |*f| if (f.active) {
            self.allocator.free(f.bytes);
            if (f.reply) |r| self.allocator.free(r);
        };
        self.allocator.free(self.forwards);
        for (self.pending) |*p| if (p.active) self.allocator.free(p.bytes);
        self.allocator.free(self.pending);
        self.respond_proxy.deinit();
        self.allocator.destroy(self.respond_proxy);
        self.allocator.free(self.rpc_entries);
        self.allocator.free(self.rpc_arena);
        self.allocator.free(self.rpc_out);
        self.allocator.free(self.apply_buf);

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
        self.worker_handler.deinit();
        self.allocator.destroy(self.worker_handler);
        self.workflow_handler.deinit();
        self.allocator.destroy(self.workflow_handler);
        self.processing_handler.deinit();
        self.allocator.destroy(self.processing_handler);

        self.forward_proxy.deinit();
        self.allocator.destroy(self.forward_proxy);

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
        @import("stdx").fs.makePath(snap_dir_path) catch return false;

        var snap_dir = @import("stdx").fs.openDir(snap_dir_path, .{}) catch return false;
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
                @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000,
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
        conn.* = try Connection.init(self.allocator, fd, self.next_conn_id, @intCast(self.id));
        log.debug("Shard {d} new connection: fd={d} conn_id={d}", .{ self.id, fd, self.next_conn_id });
        self.next_conn_id += 1;

        errdefer {
            conn.deinit();
            self.allocator.destroy(conn);
        }
        // Stale entries for closed fds stay until the tick ends, so reserve
        // past them.
        try self.closing_fds.ensureTotalCapacity(self.allocator, self.connections.count() + 1 + self.closing_fds.items.len);
        const resume_room = self.connections.count() + 1 + self.resume_fds.items.len;
        try self.resume_fds.ensureTotalCapacity(self.allocator, resume_room);
        try self.resume_running.ensureTotalCapacity(self.allocator, resume_room);
        try self.connections.put(self.allocator, fd, conn);
        if (self.metrics_registry) |m| m.server.connectionOpened();
        if (self.shard_metrics) |sm| sm.connectionOpened();
        return conn;
    }

    /// Remove and clean up a connection (does NOT close the fd).
    pub fn removeConnection(self: *Shard, fd: i32) void {
        if (self.connections.fetchRemove(fd)) |kv| {
            // Roll back any KV transactions still owned by this connection.
            _ = self.kv_handler.txn_table.dropByConnection(kv.value.id);
            if (kv.value.reads_paused) self.paused_count -= 1;
            kv.value.deinit();
            self.allocator.destroy(kv.value);
            if (self.metrics_registry) |m| m.server.connectionClosed();
            if (self.shard_metrics) |sm| sm.connectionClosed();
        }
    }

    /// Remove connection, unregister from reactor, and close the fd.
    pub fn closeConnection(self: *Shard, fd: i32) void {
        log.debug("Shard {d} closing connection: fd={d}", .{ self.id, fd });
        if (self.connections.get(fd)) |conn| self.waiter_pool.removeByConnection(@intCast(self.id), fd, conn.id);
        if (self.forward_count > 0) {
            if (self.connections.get(fd)) |conn| self.dropForwardsFor(fd, conn.id);
        }
        self.reactor.removeSource(fd);
        self.removeConnection(fd);
        _ = std.c.close(fd);
    }

    /// Get a connection by fd.
    pub fn getConnection(self: *Shard, fd: i32) ?*Connection {
        return self.connections.get(fd);
    }

    // ─── Dispatch ────────────────────────────────────────────────────────

    /// Dispatch a parsed request: resolve the routing target, then forward
    /// or handle locally. Walk opcodes (list/scan) aggregate across shards
    /// unless the pre-route narrows to a single partition.
    pub fn dispatchRequest(self: *Shard, conn: *Connection, req: proto.Request) void {
        conn.recordRequest();
        self.requests_dispatched += 1;
        // Count every dispatched request for the cluster command counter (drives
        // Overview's commands_total + rps). Other server counters (connections,
        // bytes) are not yet wired — see the metrics gap log.
        if (self.metrics_registry) |m| m.server.recordCommand();
        if (self.shard_metrics) |sm| sm.recordCommand();

        const op = req.header.op_code;

        // A wait longer than the server keeps a waiter is refused here, once
        // for every kind, not shortened where it is registered.
        inline for (.{ proto.OptionTag.block_ms, proto.OptionTag.wait_ms }) |tag| {
            if (req.findOption(tag)) |opt| {
                if ((opt.asU32() orelse 0) > waiter_pool_mod.MAX_BLOCK_MS) {
                    return self.sendErrorResponse(conn, req.header.request_id, .bad_request, "bad request: a blocking wait (block_ms/wait_ms, --block/--wait) is at most 300000 ms (5 minutes)");
                }
            }
        }

        // Walk opcodes: multi-shard aggregation unless pre-route picks one target.
        if (op < proto.MAX_OPCODES and self.dispatcher.isWalkOp(op) and self.dispatcher.walk_contexts[op] != null) {
            const has_single_target = if (self.dispatcher.pre_route[op]) |f| f(req) != null else false;
            if (!has_single_target) {
                self.executeWalk(conn, req);
                return;
            }
        }

        // Route to the correct shard/node, or dispatch locally.
        switch (self.resolveTarget(op, req)) {
            .local => self.dispatchLocal(conn, req),
            .shard => |s| self.forwardToShard(s.shard_id, conn, req),
            .remote => |r| self.forwardToRemote(conn, req, r.node_id),
        }
        // Apply what committed during the request: on a single node, what
        // its handler proposed on its own account (a dequeue's ack, an
        // implicit namespace create) and anything past the entry `park`
        // answered at. In a cluster these wait for a peer's ack.
        _ = self.applyCommitted();
    }

    /// Run a request on this shard — unless it writes and this shard's
    /// group is led elsewhere, when it goes to the leader over the peer
    /// link and the answer comes back the same way.
    fn dispatchLocal(self: *Shard, conn: *Connection, req: proto.Request) void {
        if (self.raft_network != null and req.header.op_code < proto.MAX_OPCODES and dispatcher_mod.opWrites(@enumFromInt(req.header.op_code)) and (self.diverged or self.raft_node.role != .leader)) {
            if (self.diverged) {
                self.sendErrorResponse(conn, req.header.request_id, .unavailable, DIVERGED_MESSAGE);
                return;
            }
            if (conn.owner_shard == REMOTE_OWNER) {
                // Already forwarded once; a second hop during an election
                // could bounce between nodes. The client retries instead.
                self.sendErrorResponse(conn, req.header.request_id, .unavailable, "unavailable: electing a leader — retry");
                return;
            }
            self.forwardToLeader(conn, req);
            return;
        }
        self.dispatcher.dispatch(@ptrCast(self), @ptrCast(conn), req);
    }

    /// Pure routing decision: pre-route → partition table (cluster) or
    /// local shard mapping (single-node). No side effects.
    fn resolveTarget(self: *Shard, op: u16, req: proto.Request) node_router.RouteTarget {
        if (op >= proto.MAX_OPCODES) return .{ .local = .{ .partition_id = 0 } };
        const hash = if (self.dispatcher.pre_route[op]) |f| f(req) orelse return .{ .local = .{ .partition_id = 0 } } else return .{ .local = .{ .partition_id = 0 } };

        if (self.partition_table) |pt| {
            const ns_hash = node_router.namespaceHash(req.namespace);
            return self.router.routeCluster(hash, ns_hash, pt);
        }
        return self.router.route(hash);
    }

    /// Forward a request to a remote node via the cluster forwarder.
    fn forwardToRemote(self: *Shard, conn: *Connection, req: proto.Request, node_id: NodeId) void {
        const fwd = self.forwarder orelse {
            self.sendErrorResponse(conn, req.header.request_id, .internal_error, "no forwarder configured");
            return;
        };
        const now_ms = @import("stdx").time.milliTimestamp();
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
            .circuit_open => {
                self.sendErrorResponse(conn, req.header.request_id, .overloaded, "node circuit breaker open");
            },
            .local => {
                // Shouldn't happen — routeCluster already checked. Dispatch locally.
                self.dispatcher.dispatch(@ptrCast(self), @ptrCast(conn), req);
            },
        }
    }

    /// Allocate and re-serialize a parsed Request back to wire bytes so it
    /// can be passed to another shard via the inbox. Mirrors the layout
    /// `proto.Request.parse` consumes: header (32 B) + payload
    /// `[ns_len:u16][ns][key_len:u16][key][value_len:u32][value][opts_len:u16][opts]`.
    /// CRC is preserved from the original header so the receiver re-validates.
    fn serializeRequest(allocator: std.mem.Allocator, req: proto.Request) ![]u8 {
        const header_size = @sizeOf(proto.RequestHeader);
        const total = header_size + req.header.payload_length;
        const buf = try allocator.alloc(u8, total);
        errdefer allocator.free(buf);

        @memcpy(buf[0..header_size], std.mem.asBytes(&req.header));

        var off: usize = header_size;
        std.mem.writeInt(u16, buf[off..][0..2], @intCast(req.namespace.len), .little);
        off += 2;
        @memcpy(buf[off..][0..req.namespace.len], req.namespace);
        off += req.namespace.len;

        std.mem.writeInt(u16, buf[off..][0..2], @intCast(req.key.len), .little);
        off += 2;
        @memcpy(buf[off..][0..req.key.len], req.key);
        off += req.key.len;

        std.mem.writeInt(u32, buf[off..][0..4], @intCast(req.value.len), .little);
        off += 4;
        @memcpy(buf[off..][0..req.value.len], req.value);
        off += req.value.len;

        std.mem.writeInt(u16, buf[off..][0..2], @intCast(req.options.len), .little);
        off += 2;
        @memcpy(buf[off..][0..req.options.len], req.options);
        off += req.options.len;

        // Header.payload_length must match the recomposed payload exactly,
        // otherwise the receiver's parse() will reject the message.
        if (off != total) return error.RequestSerializationMismatch;

        return buf;
    }

    /// Forward a request to a different shard in single-node mode.
    ///
    /// Marshals the request to the target shard's inbox as a `forward_request`.
    /// The target shard re-parses and dispatches the request on its own
    /// reactor thread (its handlers' state is therefore only ever touched by
    /// one thread). The response — direct bytes or a deferred blocking-read
    /// completion — is shipped back to the owner shard via `deferred_response`
    /// and written to the client there. See `runForwardedRequest`.
    ///
    /// If the target inbox is full, falls back to an `overloaded` error so
    /// the client retries instead of hanging.
    fn forwardToShard(self: *Shard, target_shard_id: u16, conn: *Connection, req: proto.Request) void {
        const peers = self.peer_shards orelse {
            // No peer shards wired — fall back to local dispatch
            self.dispatchLocal(conn, req);
            return;
        };
        if (target_shard_id >= peers.len) {
            self.dispatchLocal(conn, req);
            return;
        }
        if (target_shard_id == @as(u16, @intCast(self.id))) {
            // Already on the right shard — dispatch locally.
            self.dispatchLocal(conn, req);
            return;
        }
        const target = peers[target_shard_id];

        // Serialize request (header + recomposed payload) onto the heap so the
        // target shard can re-parse it after the source buffer is gone.
        const buf = serializeRequest(self.allocator, req) catch {
            self.sendErrorResponse(conn, req.header.request_id, .internal_error, "forward alloc failed");
            return;
        };

        // Pack (conn_id << 32) | fd into sequence so the target/owner can
        // verify the connection's generation before writing a response.
        const fd_bits: u64 = @as(u32, @bitCast(conn.fd));
        const seq: u64 = (@as(u64, conn.id) << 32) | fd_bits;

        const ok = target.inbox.send(.{
            .tag = .forward_request,
            .src_shard = @intCast(self.id),
            .payload_len = @intCast(buf.len),
            .sequence = seq,
            .payload_ptr = buf.ptr,
        });
        if (!ok) {
            self.allocator.free(buf);
            self.sendErrorResponse(conn, req.header.request_id, .overloaded, "forward inbox full");
            return;
        }

        // The response will arrive asynchronously via the inbox. Suppress
        // the "not implemented" guard in processRequests so the connection
        // waits for the cross-shard reply rather than getting a stub error.
        conn.recordForward();
        conn.response_deferred = true;
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

        // Parse limit + cursor from value: [limit:u32][cursor...]
        // All walk ops use the same wire format. limit=0 means server default.
        var limit: u32 = NameWalker.MAX_BATCH;
        var cursor: ?[]const u8 = null;
        if (req.value.len >= 4) {
            const parsed_limit = std.mem.readInt(u32, req.value[0..4], .little);
            if (parsed_limit > 0) limit = @min(parsed_limit, NameWalker.MAX_BATCH);
            if (req.value.len > 4) cursor = req.value[4..];
        }
        const filter: []const u8 = req.key; // prefix filter (empty = no filter)

        const result = walker.walk(
            contexts,
            req.namespace,
            filter,
            cursor,
            limit,
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
            .stream_list => serializeWalkStreamNames(self.allocator, deduped[0..dedup_count], result.next_cursor, &self.defaultPartition().stream, req.namespace),
            // queue_list uses rich binary format with per-queue stats
            .queue_list => serializeWalkQueueEntries(self.allocator, deduped[0..dedup_count], result.next_cursor, contexts, req.namespace),
            // processing_list and workflow_list_definitions use binary wire format with rich fields
            .processing_list => serializeWalkProcessingJobs(self.allocator, deduped[0..dedup_count], result.next_cursor, contexts, req.namespace),
            .workflow_list_definitions => serializeWalkWorkflowDefs(self.allocator, deduped[0..dedup_count], result.next_cursor, contexts, req.namespace),
            // action_list uses scan wire format with action metadata
            .action_list => serializeWalkActionEntries(self.allocator, deduped[0..dedup_count], result.next_cursor, contexts, req.namespace),
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

    /// Drain pending inbox messages (called each reactor tick), at most one
    /// ring's worth: a flood of messages must not starve the shard's I/O and
    /// Raft heartbeats. What is left keeps the next tick from sleeping
    /// (`Inbox.prepareSleep`).
    pub fn drainInbox(self: *Shard) usize {
        var buf: [64]InboxMessage = undefined;
        var total: usize = 0;

        while (total < self.inbox.capacity) {
            const count = self.inbox.drain(buf[0..@min(buf.len, self.inbox.capacity - total)]);
            if (count == 0) break;
            for (buf[0..count]) |msg| {
                self.handleInboxMessage(msg);
                total += 1;
            }
        }
        const left = self.inbox.pending();

        self.inbox_messages_processed += total;
        if (self.shard_metrics) |sm| {
            sm.recordInboxProcessed(total);
            sm.setInboxPending(left);
        }
        return total;
    }

    fn handleInboxMessage(self: *Shard, msg: InboxMessage) void {
        switch (msg.tag) {
            .shutdown => self.running = false,
            .action_invoke => self.waiter_pool.notifyAny(.action_await, ActionsHandler.resolveActionAwaitFn, @ptrCast(self)),
            .action_start => self.startActionRun(msg),
            .stream_event => self.workflow_handler.triggers_dirty = true,
            .deferred_response => self.deliverInboundResponse(msg),
            .forward_request => self.runForwardedRequest(msg),
            else => {},
        }
    }

    /// Push-wake stream triggers after a stream append. The trigger for a stream
    /// lives on the workflow-definition's shard, which is usually NOT the shard
    /// that owns the stream's data — so we mark the local handler dirty (covers
    /// the co-located case) and broadcast a `stream_event` to all peers, whose
    /// next tick force-polls their triggers. No-op when no trigger exists on the
    /// node, so streams that nobody watches incur zero cross-shard chatter.
    pub fn notifyStreamTriggers(self: *Shard) void {
        if (!WorkflowHandler.anyStreamTriggers()) return;

        self.workflow_handler.triggers_dirty = true;

        if (self.peer_inboxes) |inboxes| {
            for (inboxes, 0..) |inbox, i| {
                if (i == self.id) continue;
                _ = inbox.send(.{
                    .tag = .stream_event,
                    .src_shard = @intCast(self.id),
                });
            }
        }
    }

    /// A workflow on another shard needs a run created here; its bytes are
    /// ours to free.
    fn startActionRun(self: *Shard, msg: InboxMessage) void {
        const ptr = msg.payload_ptr orelse return;
        const data: [*]u8 = @ptrCast(ptr);
        defer if (msg.payload_len > 0) self.allocator.free(data[0..msg.payload_len]);
        if (msg.payload_len == 0) return;
        self.actions_handler.startRunFromInbox(self, data[0..msg.payload_len]);
    }

    /// Drive a request that another shard forwarded to us. The request bytes
    /// were marshalled into `msg.payload`; the connection's identity is packed
    /// into `msg.sequence` as `(conn_id << 32) | fd`. `msg.src_shard` is the
    /// shard that owns the connection.
    ///
    /// We re-parse the wire bytes, populate the per-shard `forward_proxy`
    /// Connection with the owner's metadata, and dispatch on this shard's
    /// reactor thread — so per-shard handler state is only ever touched by
    /// the owning thread. Direct responses queued onto the proxy's write_buf
    /// are then shipped back to the owner via `deliverDeferred`. Waiter-based
    /// (blocking) responses already use the owner_shard/fd/conn_id captured
    /// at registration time and round-trip through `deliverDeferred`.
    fn runForwardedRequest(self: *Shard, msg: InboxMessage) void {
        const ptr = msg.payload_ptr orelse return;
        const data: [*]u8 = @ptrCast(ptr);
        defer if (msg.payload_len > 0) self.allocator.free(data[0..msg.payload_len]);
        if (msg.payload_len == 0) return;

        const bytes = data[0..msg.payload_len];
        const fd: i32 = @bitCast(@as(u32, @truncate(msg.sequence)));
        const conn_id: u32 = @truncate(msg.sequence >> 32);
        const owner_shard: u16 = msg.src_shard;

        // A forwarded request that fails to parse is still answered: its
        // client is waiting on it.
        const req = proto.Request.parse(bytes) catch {
            const request_id: u64 = if (bytes.len >= 16) std.mem.readInt(u64, bytes[8..16], .little) else 0;
            var err_buf: [256]u8 = undefined;
            // The owning shard parsed it before forwarding, so the fault is
            // the server's, not the client's.
            const serialized = proto.Response.serializeNew(.internal_error, request_id, "internal error: another shard could not read this request", &err_buf) catch return;
            self.deliverDeferred(owner_shard, fd, conn_id, serialized);
            return;
        };

        // Reset the proxy for this dispatch. fd stays the *original* fd —
        // it's not registered on this shard, but handlers only read it for
        // waiter registration metadata, which is correct.
        const proxy = self.forward_proxy;
        proxy.fd = fd;
        proxy.id = conn_id;
        proxy.owner_shard = owner_shard;
        proxy.protocol = .binary;
        proxy.state = .active;
        proxy.response_deferred = false;
        proxy.write_buf.read_pos = 0;
        proxy.write_buf.write_pos = 0;
        proxy.write_overflow = false;

        proxy.recordRequest();
        self.dispatchLocal(proxy, req);
        // As after a client's own request: what it proposed on its own
        // account applies before the next.
        _ = self.applyCommitted();

        // Pull any queued direct response off the proxy and ship the bytes
        // back to the owner. If the handler deferred the response (e.g. a
        // blocking group_read registered a waiter), the waiter's later
        // resolution will deliver via the same cross-shard inbox path.
        var err_buf: [256]u8 = undefined;
        if (self.takeProxyAnswer(proxy, req.header.request_id, &err_buf)) |answer| {
            defer answer.free(self.allocator);
            self.deliverDeferred(owner_shard, fd, conn_id, answer.bytes());
        } else if (!proxy.response_deferred) {
            // Handler produced nothing — report not_implemented like the
            // owner-side processRequests would.
            const serialized = proto.Response.serializeNew(.internal_error, req.header.request_id, "not implemented", &err_buf) catch return;
            self.deliverDeferred(owner_shard, fd, conn_id, serialized);
        }
    }

    /// An answer a handler queued on a proxy connection, taken whole — a cut
    /// answer desynchronises its client — or an error answer in its place.
    const ProxyAnswer = union(enum) {
        owned: []u8,
        err: []const u8,

        fn bytes(self: ProxyAnswer) []const u8 {
            return switch (self) {
                .owned => |b| b,
                .err => |b| b,
            };
        }

        fn free(self: ProxyAnswer, allocator: std.mem.Allocator) void {
            if (self == .owned) allocator.free(self.owned);
        }
    };

    /// Null when the handler queued nothing. An answer the proxy refused as
    /// too large, or that could not be copied, is replaced by an error
    /// answer written into `err_buf`.
    fn takeProxyAnswer(self: *Shard, proxy: *Connection, request_id: u64, err_buf: *[256]u8) ?ProxyAnswer {
        const pending = proxy.write_buf.readable();
        if (proxy.write_overflow) {
            proxy.write_overflow = false;
            proxy.write_buf.consume(pending);
            return .{ .err = proto.Response.serializeNew(.internal_error, request_id, "internal error: answer too large to send — ask for less", err_buf) catch unreachable };
        }
        if (pending == 0) return null;
        const answer = self.allocator.alloc(u8, pending) catch {
            proxy.write_buf.consume(pending);
            return .{ .err = proto.Response.serializeNew(.internal_error, request_id, "internal error: out of memory for the answer", err_buf) catch unreachable };
        };
        _ = proxy.write_buf.read(answer);
        proxy.shrinkWriteBuffer();
        return .{ .owned = answer };
    }

    /// Write a deferred blocking-read response that was resolved on another
    /// (data) shard. Runs on this shard's thread — this shard owns the
    /// connection — so the socket write is single-threaded as required.
    ///
    /// `msg.sequence` is packed `(conn_id << 32) | fd`. The conn_id check
    /// guards against fd reuse: if the original connection has closed and the
    /// fd was reassigned to a new connection, drop the response instead of
    /// delivering it to the wrong client.
    fn deliverInboundResponse(self: *Shard, msg: InboxMessage) void {
        const ptr = msg.payload_ptr orelse return;
        const data: [*]u8 = @ptrCast(ptr);
        defer if (msg.payload_len > 0) self.allocator.free(data[0..msg.payload_len]);
        if (msg.payload_len == 0) return;

        const fd: i32 = @bitCast(@as(u32, @truncate(msg.sequence)));
        const conn_id: u32 = @truncate(msg.sequence >> 32);
        const conn = self.getConnection(fd) orelse return; // connection gone
        if (conn.id != conn_id) return; // fd reused for a new connection
        _ = conn.queueWrite(data[0..msg.payload_len]);
        self.flushToClient(fd);
    }

    /// Every frame the network queued since the last drain.
    fn drainRaftQueue(self: *Shard) void {
        const q = self.raft_queue orelse return;
        while (q.pop()) |frame| {
            defer self.allocator.free(frame.payload);
            self.handleRaftFrame(frame);
        }
    }

    // ─── Writes waiting for commit ───────────────────────────────────────

    /// What a write handler does once its entry is proposed: nothing more
    /// now. `responder` runs once the entry is applied, on this thread,
    /// with the request re-parsed and a connection standing for the
    /// client's; it reads projection state and answers. When the entry is
    /// already committed (a group of one) that is right away, on the
    /// client's own connection.
    pub fn park(self: *Shard, conn: *Connection, req: proto.Request, proposed: raft_node_mod.ProposeResult, responder: HandlerFn) void {
        // Inside the apply loop `applyCommitted` returns without draining:
        // a single node's responder would run before its entry applied.
        std.debug.assert(!self.applying);
        if (self.raft_node.commit_index >= proposed.index) {
            // Up to this write's entry and no further: its responder reads
            // what that entry's applier recorded, which a later entry
            // (another thread's proposal) would overwrite. The dispatch
            // that parked this applies the rest.
            std.debug.assert(self.raft_node.last_applied < proposed.index);
            _ = self.applyThrough(proposed.index);
            if (self.raft_node.last_applied != proposed.index or !self.last_entry_applied) {
                if (conn.protocol == .resp) {
                    _ = conn.queueWrite("-ERR " ++ persistence_mod.COMMITTED_NOT_APPLIED ++ "\r\n");
                } else {
                    self.sendErrorResponse(conn, req.header.request_id, .internal_error, persistence_mod.COMMITTED_NOT_APPLIED);
                }
                return;
            }
            self.answering_index = proposed.index;
            self.answering_timestamp_ns = proposed.timestamp_ns;
            responder(@ptrCast(self), @ptrCast(conn), req);
            return;
        }
        const slot = &self.pending[proposed.index % PENDING_SLOTS];
        if (slot.active) {
            // Cannot happen while the Raft node caps outstanding entries
            // below the table size; said out loud rather than trusted.
            log.err("shard {d}: pending slot for index {d} still holds index {d}", .{ self.id, proposed.index, slot.index });
            self.sendErrorResponse(conn, req.header.request_id, .overloaded, persistence_mod.failureMessage(error.Overloaded, ""));
            return;
        }
        const bytes = serializeRequest(self.allocator, req) catch {
            self.sendErrorResponse(conn, req.header.request_id, .internal_error, "internal error: could not hold the request until commit — write may still apply");
            return;
        };
        slot.* = .{ .active = true, .index = proposed.index, .term = proposed.term, .owner_shard = conn.owner_shard, .fd = conn.fd, .conn_id = conn.id, .request_id = req.header.request_id, .bytes = bytes, .responder = responder };
        self.pending_count += 1;
        conn.response_deferred = true;
        // A leader counts itself toward the quorum; when commits are
        // durable its own copy is on disk before it does.
        if (self.durability == .sync) self.syncFlushIfNeeded();
        self.pump(nowMs());
    }

    /// The entry at `index` applied (or could not): answer whoever waits.
    fn answerPending(self: *Shard, index: u64, term: u64, timestamp_ns: u64, applied: bool) void {
        const table_slot = &self.pending[index % PENDING_SLOTS];
        if (!table_slot.active or table_slot.index != index) return;
        // Out of the table before the responder runs, so a sweep of the
        // table (`resolvePending`) while it runs cannot answer and free
        // this request a second time.
        const slot = table_slot.*;
        table_slot.active = false;
        self.pending_count -= 1;
        defer self.allocator.free(slot.bytes);
        // A different term at this index is a new leader's entry: this
        // write never committed, whatever became of that one.
        if (term != 0 and slot.term != term) {
            self.deliverDeferredResponse(slot.owner_shard, slot.fd, slot.conn_id, slot.request_id, .unavailable, "unavailable: leader changed, write not applied — retry");
            return;
        }
        if (!applied) {
            self.deliverDeferredResponse(slot.owner_shard, slot.fd, slot.conn_id, slot.request_id, .internal_error, persistence_mod.COMMITTED_NOT_APPLIED);
            return;
        }
        const req = proto.Request.parse(slot.bytes) catch {
            self.deliverDeferredResponse(slot.owner_shard, slot.fd, slot.conn_id, slot.request_id, .internal_error, persistence_mod.ANSWER_LOST);
            return;
        };
        const proxy = self.respond_proxy;
        proxy.fd = slot.fd;
        proxy.id = slot.conn_id;
        proxy.owner_shard = slot.owner_shard;
        proxy.state = .active;
        proxy.response_deferred = false;
        proxy.write_buf.read_pos = 0;
        proxy.write_buf.write_pos = 0;
        proxy.write_overflow = false;
        self.answering_index = index;
        self.answering_timestamp_ns = timestamp_ns;
        slot.responder(@ptrCast(self), @ptrCast(proxy), req);
        var err_buf: [256]u8 = undefined;
        if (self.takeProxyAnswer(proxy, slot.request_id, &err_buf)) |answer| {
            defer answer.free(self.allocator);
            self.deliverDeferred(slot.owner_shard, slot.fd, slot.conn_id, answer.bytes());
        } else if (!proxy.response_deferred) {
            self.deliverDeferredResponse(slot.owner_shard, slot.fd, slot.conn_id, slot.request_id, .internal_error, persistence_mod.ANSWER_LOST);
        }
    }

    fn resolvePending(self: *Shard, message: []const u8) void {
        if (self.pending_count == 0) return;
        log.warn("shard {d}: {d} write(s) were waiting for commit; answering each: {s}", .{ self.id, self.pending_count, message });
        for (self.pending) |*slot| {
            if (!slot.active) continue;
            self.deliverDeferredResponse(slot.owner_shard, slot.fd, slot.conn_id, slot.request_id, .unavailable, message);
            self.allocator.free(slot.bytes);
            slot.active = false;
            self.pending_count -= 1;
        }
    }

    // ─── Writes on a node that does not lead ─────────────────────────────

    /// Send a client's write to the leader as the bytes it arrived in, and
    /// hold the client until the leader answers. With no leader known the
    /// write waits for one, up to FORWARD_TIMEOUT_MS.
    fn forwardToLeader(self: *Shard, conn: *Connection, req: proto.Request) void {
        const slot = blk: {
            for (self.forwards) |*f| if (!f.active) break :blk f;
            self.sendErrorResponse(conn, req.header.request_id, .overloaded, "too many writes waiting for the leader");
            return;
        };
        const bytes = serializeRequest(self.allocator, req) catch {
            self.sendErrorResponse(conn, req.header.request_id, .internal_error, "could not hold the request for the leader");
            return;
        };
        slot.* = .{ .active = true, .id = self.next_forward_id, .owner_shard = conn.owner_shard, .fd = conn.fd, .conn_id = conn.id, .request_id = req.header.request_id, .bytes = bytes, .deadline_ms = nowMs() + FORWARD_TIMEOUT_MS };
        self.next_forward_id +%= 1;
        if (self.next_forward_id == 0) self.next_forward_id = FORWARD_ID_FIRST;
        self.forward_count += 1;
        conn.recordForward();
        conn.response_deferred = true;
        self.sendForward(slot);
    }

    /// Sent only over a link that is up: what the network queues for a
    /// peer it has no link to is dropped, and a write marked sent that
    /// never left would wait until the term changed.
    fn sendForward(self: *Shard, f: *Forward) void {
        const raft = self.raft_node;
        const leader = raft.leader_id;
        if (leader == 0 or leader == self.cluster_node_id) return;
        const rn = self.raft_network orelse return;
        if (!rn.isLinked(leader)) return;
        var buf: [4 + MAX_REQUEST_SIZE]u8 = undefined;
        if (4 + f.bytes.len > buf.len) {
            self.finishForward(f, .internal_error, "request too large to forward");
            return;
        }
        std.mem.writeInt(u32, buf[0..4], f.id, .little);
        @memcpy(buf[4 .. 4 + f.bytes.len], f.bytes);
        if (!self.trySendRaft(leader, .forward_write, buf[0 .. 4 + f.bytes.len])) return;
        f.sent_to = leader;
        f.sent_term = raft.current_term;
    }

    /// Writes waiting for a leader go out once one is known, or run here
    /// once this node is it (never under a handler that is itself
    /// waiting); one that waited too long is answered. A write already
    /// sent waits for its leader's answer however long the operation
    /// takes; once the term has moved on, or the link it went over is
    /// down, that answer will never come, and the client is told what is
    /// known. A reply held for read-your-writes
    /// is released at the deadline rather than forever: the write is
    /// committed, only this node's copy is late.
    fn sweepForwards(self: *Shard, now: u64) void {
        const raft = self.raft_node;
        const leader = raft.leader_id;
        for (self.forwards) |*f| {
            if (!f.active) continue;
            if (f.reply) |reply| {
                if (now < f.deadline_ms) continue;
                if (now -| self.late_apply_warn_ms >= WARN_INTERVAL_MS) {
                    self.late_apply_warn_ms = now;
                    log.warn("shard {d}: answering a write the leader committed at index {d} before this node applied it (applied {d}); this node is behind", .{ self.id, f.applied_by, self.raft_node.last_applied });
                }
                self.deliverDeferred(f.owner_shard, f.fd, f.conn_id, reply);
                self.dropForward(f);
                continue;
            }
            if (f.sent_to != 0) {
                if (raft.current_term != f.sent_term or (leader != 0 and leader != f.sent_to)) {
                    self.finishForward(f, .unavailable, "unavailable: lost leadership before commit — write may still apply");
                } else if (self.raft_network) |rn| {
                    if (!rn.isLinked(f.sent_to)) self.finishForward(f, .unavailable, "unavailable: lost the link to the leader — write may still apply");
                }
                continue;
            }
            if (leader == self.cluster_node_id) {
                self.runHeldLocally(f);
                continue;
            }
            if (leader != 0) {
                self.sendForward(f);
                if (f.sent_to != 0) continue;
            }
            if (now >= f.deadline_ms) self.finishForward(f, .unavailable, if (leader != 0) "unavailable: the leader is not reachable from this node — retry" else "unavailable: electing a leader — retry");
        }
    }

    /// A write held while a leader was being chosen, on the node that
    /// became it: run it as the client's own request. The slot is freed
    /// first so nothing that runs under the handler can see it.
    fn runHeldLocally(self: *Shard, f: *Forward) void {
        const bytes = f.bytes;
        f.bytes = &.{};
        const owner = f.owner_shard;
        const fd = f.fd;
        const conn_id = f.conn_id;
        const request_id = f.request_id;
        self.dropForward(f);
        defer self.allocator.free(bytes);
        const req = proto.Request.parse(bytes) catch {
            self.deliverDeferredResponse(owner, fd, conn_id, request_id, .internal_error, "internal error: the held write did not parse and was not written — retry");
            return;
        };
        const proxy = self.respond_proxy;
        proxy.fd = fd;
        proxy.id = conn_id;
        proxy.owner_shard = owner;
        proxy.state = .active;
        proxy.response_deferred = false;
        proxy.write_buf.read_pos = 0;
        proxy.write_buf.write_pos = 0;
        proxy.write_overflow = false;
        self.dispatchLocal(proxy, req);
        var err_buf: [256]u8 = undefined;
        if (self.takeProxyAnswer(proxy, request_id, &err_buf)) |answer| {
            defer answer.free(self.allocator);
            self.deliverDeferred(owner, fd, conn_id, answer.bytes());
        } else if (!proxy.response_deferred) {
            self.deliverDeferredResponse(owner, fd, conn_id, request_id, .internal_error, "no response");
        }
    }

    /// The client is gone: nothing it was waiting for needs a slot.
    fn dropForwardsFor(self: *Shard, fd: i32, conn_id: u32) void {
        for (self.forwards) |*f| {
            if (f.active and f.owner_shard == self.id and f.fd == fd and f.conn_id == conn_id) self.dropForward(f);
        }
    }

    fn finishForward(self: *Shard, f: *Forward, status: proto.StatusCode, message: []const u8) void {
        self.deliverDeferredResponse(f.owner_shard, f.fd, f.conn_id, f.request_id, status, message);
        self.dropForward(f);
    }

    fn dropForward(self: *Shard, f: *Forward) void {
        self.allocator.free(f.bytes);
        if (f.reply) |r| {
            self.allocator.free(r);
            self.replies_held -= 1;
        }
        f.* = .{};
        self.forward_count -= 1;
    }

    /// A write a peer received while this node leads: run it here as if
    /// the client had connected here, answering over the link.
    fn runForwardedWrite(self: *Shard, frame: RaftFrame) void {
        if (frame.payload.len < 4) return self.badFrame(frame);
        const id = std.mem.readInt(u32, frame.payload[0..4], .little);
        const req = proto.Request.parse(frame.payload[4..]) catch {
            // The forwarder holds its client until this is answered.
            self.badFrame(frame);
            const body = frame.payload[4..];
            const request_id: u64 = if (body.len >= 16) std.mem.readInt(u64, body[8..16], .little) else 0;
            var err_buf: [256]u8 = undefined;
            // The forwarder parsed it first, so the likely cause is two
            // nodes running different versions.
            const serialized = proto.Response.serializeNew(.internal_error, request_id, "internal error: the leader could not read this request — are all nodes on the same version?", &err_buf) catch return;
            return self.sendForwardReply(frame.source_node, id, serialized);
        };
        const proxy = self.forward_proxy;
        proxy.fd = @bitCast(id);
        proxy.id = frame.source_node;
        proxy.owner_shard = REMOTE_OWNER;
        proxy.protocol = .binary;
        proxy.state = .active;
        proxy.response_deferred = false;
        proxy.write_buf.read_pos = 0;
        proxy.write_buf.write_pos = 0;
        proxy.write_overflow = false;
        proxy.recordRequest();
        self.dispatchLocal(proxy, req);
        var err_buf: [256]u8 = undefined;
        if (self.takeProxyAnswer(proxy, req.header.request_id, &err_buf)) |answer| {
            defer answer.free(self.allocator);
            self.sendForwardReply(frame.source_node, id, answer.bytes());
        } else if (!proxy.response_deferred) {
            const serialized = proto.Response.serializeNew(.internal_error, req.header.request_id, "not implemented", &err_buf) catch return;
            self.sendForwardReply(frame.source_node, id, serialized);
        }
    }

    /// The answer to a forwarded write, with the index the client's node
    /// must have applied before handing it over: read-your-writes on the
    /// node the client wrote to.
    fn sendForwardReply(self: *Shard, peer: u32, id: u32, bytes: []const u8) void {
        // The forwarder holds its client until a reply comes: one that
        // cannot be sent whole is replaced by one that can.
        if (12 + bytes.len > transport.MAX_PAYLOAD_SIZE) {
            var err_buf: [256]u8 = undefined;
            const request_id: u64 = if (bytes.len >= 16) std.mem.readInt(u64, bytes[8..16], .little) else 0;
            const serialized = proto.Response.serializeNew(.internal_error, request_id, "internal error: answer too large to send", &err_buf) catch return;
            return self.sendForwardReply(peer, id, serialized);
        }
        const buf = self.allocator.alloc(u8, 12 + bytes.len) catch {
            log.warn("shard {d}: out of memory for a forwarded write's reply to node {d}", .{ self.id, peer });
            return;
        };
        defer self.allocator.free(buf);
        std.mem.writeInt(u32, buf[0..4], id, .little);
        std.mem.writeInt(u64, buf[4..12], self.raft_node.last_applied, .little);
        @memcpy(buf[12..], bytes);
        self.sendRaft(peer, .forward_reply, buf);
    }

    fn takeForwardReply(self: *Shard, frame: RaftFrame) void {
        if (frame.payload.len < 12) return self.badFrame(frame);
        const id = std.mem.readInt(u32, frame.payload[0..4], .little);
        const committed = std.mem.readInt(u64, frame.payload[4..12], .little);
        const f = blk: {
            for (self.forwards) |*f| if (f.active and f.id == id) break :blk f;
            return; // answered already, or the client gave up
        };
        if (f.reply != null) return;
        // Only the leader it went to may answer it, and only with an
        // answer to it.
        if (frame.source_node != f.sent_to) return self.impostorFrame(frame, f.sent_to);
        const bytes = frame.payload[12..];
        const resp = proto.Response.parse(bytes) catch return self.badFrame(frame);
        if (resp.header.request_id != f.request_id) return self.badFrame(frame);
        if (self.raft_node.last_applied >= committed) {
            self.deliverDeferred(f.owner_shard, f.fd, f.conn_id, bytes);
            self.dropForward(f);
            return;
        }
        f.reply = self.allocator.dupe(u8, bytes) catch {
            self.finishForward(f, .internal_error, "could not hold the leader's answer");
            return;
        };
        f.applied_by = committed;
        f.deadline_ms = nowMs() + FORWARD_TIMEOUT_MS;
        self.replies_held += 1;
    }

    fn releaseReplies(self: *Shard) void {
        const applied = self.raft_node.last_applied;
        for (self.forwards) |*f| {
            if (!f.active or f.reply == null or f.applied_by > applied) continue;
            self.deliverDeferred(f.owner_shard, f.fd, f.conn_id, f.reply.?);
            self.dropForward(f);
        }
    }

    // ─── Raft on the shard thread ────────────────────────────────────────

    /// A clock that never jumps: timeouts must not fire, or fail to, on
    /// an NTP step.
    fn nowMs() u64 {
        return @import("stdx").time.monotonicMs();
    }

    fn sendRaft(self: *Shard, peer: u32, msg_type: transport.MsgType, payload: []const u8) void {
        _ = self.trySendRaft(peer, msg_type, payload);
    }

    fn trySendRaft(self: *Shard, peer: u32, msg_type: transport.MsgType, payload: []const u8) bool {
        const rn = self.raft_network orelse return false;
        if (rn.sendTo(peer, msg_type, self.id, payload)) return true;
        log.warn("shard {d}: could not queue a {s} for node {d}", .{ self.id, @tagName(msg_type), peer });
        return false;
    }

    /// One Raft frame from a proven peer. The link proves who sent it;
    /// the ids inside must agree, or one member could speak for another.
    /// Once this node knows the membership, only members speak for the
    /// group; a node with the secret but no seat can still ask for one.
    fn handleRaftFrame(self: *Shard, frame: RaftFrame) void {
        const raft = self.raft_node;
        if (self.diverged) return;
        if (frame.msg_type != .join_request and (raft.peer_count > 0 or raft.timer_enabled)) {
            var ids: [membership.MAX_MEMBERS]u32 = undefined;
            if (!membership.names(raft.memberIds(&ids), frame.source_node)) return self.strangerFrame(frame);
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
                // An ack means "in my log, on disk" when commits are durable.
                if (resp.success and count > 0 and self.durability == .sync) {
                    self.flushSegmentToDisk() catch |err| {
                        self.persist_failures += 1;
                        log.err("shard {d}: sync flush failed: {s}; not acking the batch (persist_failures={d})", .{ self.id, @errorName(err), self.persist_failures });
                        return;
                    };
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
                if (was == .leader and raft.role != .leader) self.leadershipLost("a follower is at a newer term");
                if (!self.applyCommitted()) log.err("shard {d}: a committed entry could not be applied", .{self.id});
                if (raft.role == .leader) self.pump(nowMs());
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
                        self.pump(nowMs());
                    },
                }
            },
            .join_request => self.handleJoinRequest(frame.source_node),
            .forward_write => self.runForwardedWrite(frame),
            .forward_reply => self.takeForwardReply(frame),
            .install_snapshot, .peer_info, .hello, .hello_back, .verify, .welcome => {},
        }
    }

    /// Rate-limits the write-overflow close line: the count of lines left
    /// unsaid, or null to stay quiet.
    fn overflowWarnDue(self: *Shard) ?u64 {
        const now = nowMs();
        if (now -| self.overflow_warn_ms < WARN_INTERVAL_MS) {
            self.overflow_unsaid += 1;
            return null;
        }
        self.overflow_warn_ms = now;
        const unsaid = self.overflow_unsaid;
        self.overflow_unsaid = 0;
        return unsaid;
    }

    /// A member's bug can arrive at heartbeat rate; one line per interval.
    fn frameWarnDue(self: *Shard) bool {
        const now = nowMs();
        if (now -| self.frame_warn_ms < WARN_INTERVAL_MS) return false;
        self.frame_warn_ms = now;
        return true;
    }

    fn badFrame(self: *Shard, frame: RaftFrame) void {
        if (self.frameWarnDue()) log.warn("shard {d}: a {s} frame from node {d} did not decode; dropped", .{ self.id, @tagName(frame.msg_type), frame.source_node });
    }

    fn impostorFrame(self: *Shard, frame: RaftFrame, claimed: u32) void {
        if (self.frameWarnDue()) log.warn("shard {d}: a {s} frame from node {d} speaks for node {d}; dropped", .{ self.id, @tagName(frame.msg_type), frame.source_node, claimed });
    }

    fn strangerFrame(self: *Shard, frame: RaftFrame) void {
        const now = nowMs();
        if (now -| self.stranger_warn_ms < WARN_INTERVAL_MS) return;
        self.stranger_warn_ms = now;
        log.warn("shard {d}: node {d} sent a {s} frame but is not a member; ignored (a node with the secret is a member only once the leader adds it)", .{ self.id, frame.source_node, @tagName(frame.msg_type) });
    }

    /// Committed, applied history and the leader's log disagree. The
    /// projections cannot be rebuilt in place, so the node stops taking
    /// part and says what to do; reads still serve what it has.
    fn markDiverged(self: *Shard, leader: u32, term: u64) void {
        if (self.diverged) return;
        self.diverged = true;
        log.err("shard {d}: node {d} (term {d}) disagrees with history this node committed and applied; this node's data can no longer be trusted and it has stopped taking part in the group. Stop it, delete its data directory, and start it again with --join <a live member> (not --cluster)", .{ self.id, leader, term });
        // Only a member that broke one-leader-per-term reaches this on a
        // leader; still, a diverged node leads nothing.
        if (self.raft_node.role == .leader) {
            self.raft_node.role = .follower;
            self.raft_node.leader_id = 0;
        }
        self.stoppedLeading(DIVERGED_MESSAGE);
        // An answer already held is the leader's, and the write it
        // answers committed; the client gets it.
        for (self.forwards) |*f| {
            if (!f.active) continue;
            if (f.reply) |reply| {
                self.deliverDeferred(f.owner_shard, f.fd, f.conn_id, reply);
                self.dropForward(f);
            } else {
                self.finishForward(f, .unavailable, DIVERGED_MESSAGE);
            }
        }
    }

    /// The Raft clock: elections, check-quorum, the leader loop, and a
    /// joiner's request to be let in.
    fn tickRaft(self: *Shard) void {
        if (self.raft_network == null or self.diverged) return;
        const raft = self.raft_node;
        const now = nowMs();
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
                    // Alone, the win committed the whole log.
                    if (!self.applyCommitted()) log.err("shard {d}: a committed entry could not be applied", .{self.id});
                }
            }
        }
        if (r.step_down) self.leadershipLost("no contact with a majority");
        if (raft.role == .leader) self.pump(now);
        if (self.joinWanted(now)) self.askToJoin(now);
        if (self.forward_count > 0) self.sweepForwards(now);
    }

    /// A node the log has never named asks to be added; so does one whose
    /// membership was appended but never committed and whose leader has
    /// gone quiet, since a new leader without that entry never speaks to
    /// it.
    fn joinWanted(self: *Shard, now: u64) bool {
        const raft = self.raft_node;
        if (raft.peer_count == 0 and !raft.timer_enabled) return true;
        return raft.role != .leader and raft.membership_index > raft.commit_index and now -| raft.last_leader_contact_ms > raft.config.election_timeout_max_ms;
    }

    fn broadcastVote(self: *Shard, req: raft_node_mod.VoteRequest) void {
        var buf: [transport.VOTE_REQ_SIZE]u8 = undefined;
        const n = transport.serializeVoteRequest(req, &buf) orelse return;
        const raft = self.raft_node;
        for (raft.peer_ids[0..raft.peer_count]) |peer| self.sendRaft(peer, .request_vote, buf[0..n]);
    }

    fn leadershipLost(self: *Shard, why: []const u8) void {
        log.warn("shard {d}: stepped down ({s}); now following at term {d}", .{ self.id, why, self.raft_node.current_term });
        self.stoppedLeading("unavailable: lost leadership before commit — write may still apply");
    }

    /// What a leader had in flight is no longer its to finish: parked
    /// writes are answered with `message`, runs it had not stepped stay at
    /// their start, and implicit creates may have been dropped with the
    /// log's tail.
    fn stoppedLeading(self: *Shard, message: []const u8) void {
        // A follower takes no step passes; its polls wait again.
        self.more_work = false;
        self.workflow_handler.dropStartedRuns(self);
        self.namespace_handler.forgetImplicitCreates();
        self.resolvePending(message);
    }

    /// The leader loop, once per tick and after anything that changes what
    /// a peer should hear: a heartbeat on its interval, otherwise a batch
    /// from the peer's next index when it is behind and nothing is in
    /// flight or what was has gone unanswered for an RPC timeout.
    fn pump(self: *Shard, now: u64) void {
        const raft = self.raft_node;
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
            const prev_term = raft.log.entryTerm(prev_index) orelse blk: {
                if (prev_index == 0) break :blk @as(u64, 0);
                log.err("shard {d}: no term known for index {d}; cannot replicate to node {d}", .{ self.id, prev_index, raft.peer_ids[i] });
                continue;
            };
            var entries: []const entry_mod.Entry = &.{};
            if (want_data) {
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

    /// Ask every peer this node can reach, once a second, until a config
    /// entry naming it arrives from the leader; say so at intervals while
    /// it goes on, since the leader's refusal is logged only there.
    fn askToJoin(self: *Shard, now: u64) void {
        if (now -| self.join_asked_ms < JOIN_ASK_INTERVAL_MS) return;
        self.join_asked_ms = now;
        if (self.join_first_asked_ms == 0) {
            self.join_first_asked_ms = now;
            self.join_warned_ms = now;
        } else if (now -| self.join_warned_ms >= JOIN_WARN_INTERVAL_MS) {
            self.join_warned_ms = now;
            log.warn("shard {d}: still asking to join after {d} s; a leader adds a node it can reach when the group holds fewer than {d} members and no other change is in flight — check the seeds and the secret", .{ self.id, (now - self.join_first_asked_ms) / 1000, membership.MAX_MEMBERS });
        }
        const rn = self.raft_network orelse return;
        var ids: [@import("../raft/network.zig").MAX_PEERS]u32 = undefined;
        for (rn.linkedPeers(&ids)) |peer| self.sendRaft(peer, .join_request, "");
    }

    /// A proven peer wants in. The leader appends the config that names
    /// it, one change at a time: a second change before the first commits
    /// could let two majorities disagree.
    fn handleJoinRequest(self: *Shard, from: u32) void {
        const raft = self.raft_node;
        if (raft.role != .leader) return;
        var ids: [membership.MAX_MEMBERS]u32 = undefined;
        const members = raft.memberIds(&ids);
        if (membership.names(members, from)) return;
        if (raft.membership_index > raft.commit_index) {
            // A change that never commits (the node it added died before
            // acking) blocks every later join; name who has not answered.
            const now = nowMs();
            if (now -| self.join_refused_warn_ms >= JOIN_WARN_INTERVAL_MS) {
                self.join_refused_warn_ms = now;
                var waiting: [raft_node_mod.MAX_PEERS]u32 = undefined;
                var n: usize = 0;
                for (0..raft.peer_count) |i| {
                    if (raft.peers[i].match_index < raft.membership_index) {
                        waiting[n] = raft.peer_ids[i];
                        n += 1;
                    }
                }
                if (n == 0) {
                    log.warn("shard {d}: node {d} asked to join while an earlier membership change waits for a majority; nothing else is added until it commits", .{ self.id, from });
                } else {
                    log.warn("shard {d}: node {d} asked to join while an earlier membership change waits for {any} to answer; nothing else is added until it does", .{ self.id, from, waiting[0..n] });
                }
            }
            return;
        }
        if (members.len >= membership.MAX_MEMBERS) {
            const now = nowMs();
            if (now -| self.join_refused_warn_ms >= JOIN_WARN_INTERVAL_MS) {
                self.join_refused_warn_ms = now;
                log.warn("shard {d}: node {d} asked to join but the group already has {d} members, the most it can hold", .{ self.id, from, members.len });
            }
            return;
        }
        var grown: [membership.MAX_MEMBERS]u32 = undefined;
        @memcpy(grown[0..members.len], members);
        grown[members.len] = from;
        var buf: [membership.MAX_SIZE]u8 = undefined;
        const payload = membership.encode(grown[0 .. members.len + 1], &buf);
        _ = raft.propose(.raft_config, entry_mod.Flags.NONE, 0, payload) catch |err| {
            log.err("shard {d}: could not propose adding node {d}: {s}", .{ self.id, from, @errorName(err) });
            return;
        };
        log.info("shard {d}: adding node {d}; members {any}", .{ self.id, from, grown[0 .. members.len + 1] });
        self.pump(nowMs());
    }

    // ─── The one applier ─────────────────────────────────────────────────

    /// Apply one committed entry to this shard's state and wake whoever was
    /// waiting on it. A leader, a follower and boot replay all run the same
    /// core (`applyEntryCoreWith`), so the three can never disagree about
    /// what an entry means; only the live paths notify. False when the
    /// partition refused the entry.
    pub fn applyEntry(self: *Shard, entry: *const entry_mod.Entry) bool {
        if (!applyEntryCore(self.defaultPartition(), &self.replay_registry, entry)) return false;
        self.notifyApplied(entry);
        return true;
    }

    /// Apply every committed entry not yet applied. Only the shard's own
    /// loop calls this: at boot, on a Raft frame, on an election won alone,
    /// after each client request (binary, RESP, or forwarded from another
    /// shard), and in the tick; `park` applies up to its own entry through
    /// `applyThrough`. Module code never does: applying in the middle of its own
    /// work would run other writers' appliers and responders under its
    /// locks and over its pointers into the maps they change. False when an
    /// entry could not be applied.
    ///
    /// A waiter woken by an apply may propose in turn (a dequeue acks the
    /// message it took); that entry lands in the log and this loop reaches
    /// it on its next pass. A nested drain would copy the next entry over
    /// `apply_buf` while the notification is still reading the entry in
    /// hand, so the inner call is a no-op. That no-op returns true, which
    /// means "nothing failed", not "applied": a notification must not read
    /// projection state for an entry it proposed.
    pub fn applyCommitted(self: *Shard) bool {
        return self.applyThrough(std.math.maxInt(u64));
    }

    /// `applyCommitted`, stopping after index `limit`.
    fn applyThrough(self: *Shard, limit: u64) bool {
        if (self.applying) return true;
        // Called after every request: nothing to apply is the common case.
        // A `sync` flush still happens, as it did before this shortcut.
        if (self.raft_node.last_applied >= @min(self.raft_node.commit_index, limit) and self.replies_held == 0) {
            self.syncFlushIfNeeded();
            return true;
        }
        self.applying = true;
        defer self.applying = false;

        const raft = self.raft_node;
        var all_applied = true;
        while (raft.last_applied < @min(raft.commit_index, limit)) {
            const next_idx = raft.last_applied + 1;
            // Advanced before the apply: whatever a notification does, this
            // entry is never taken twice, and the loop cannot stall.
            raft.last_applied = next_idx;
            if (raft.log.getEntryCopy(next_idx, self.apply_buf)) |e| {
                const applied = self.applyEntry(&e);
                if (!applied) all_applied = false;
                self.last_entry_applied = applied;
                self.answerPending(next_idx, e.header.term, e.header.timestamp_ns, applied);
            } else {
                self.last_entry_applied = false;
                self.answerPending(next_idx, 0, 0, false);
                // A committed index is always within the log, in the ring
                // or below it in the durable log, and the buffer fits every
                // entry; an unreadable one is a bug or a damaged segment.
                // Said out loud, because the write was already acked.
                log.err("shard {d}: committed entry index={d} could not be read for apply; projections are missing it", .{ self.id, next_idx });
                all_applied = false;
            }
        }
        if (self.wake_workers) {
            self.wake_workers = false;
            ActionsHandler.wakeWorkers(self);
        }
        self.syncFlushIfNeeded();
        if (self.replies_held > 0) self.releaseReplies();
        return all_applied;
    }

    /// The namespace to meter an applied entry under. Entries carry the
    /// hash; a bare or explicit "default" is "default" without a lookup (a
    /// namespace is registered only on its first write, and default's first
    /// write should still be metered), and a namespace this node has never
    /// registered is not metered rather than mislabelled.
    fn metricsNamespace(self: *Shard, ns_hash: u32) ?[]const u8 {
        if (ns_hash == node_router.namespaceHash("") or ns_hash == node_router.namespaceHash("default")) return "default";
        return self.namespace_handler.nameForHash(ns_hash);
    }

    /// Waiters, triggers and per-namespace metrics for an entry that just
    /// applied. Keyed from the entry itself, never from the request, so a
    /// follower's blocking read wakes on the same event a leader's does.
    fn notifyApplied(self: *Shard, entry: *const entry_mod.Entry) void {
        const etype: entry_mod.EntryType = @enumFromInt(entry.header.entry_type);
        switch (etype) {
            .kv_put, .kv_delete, .kv_incr, .kv_touch => {
                const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;
                self.waiter_pool.notify(.kv_get, cmd.key, resolveKVWaiter, @ptrCast(self));
                if (self.metrics_registry) |mr| {
                    const ns = self.metricsNamespace(cmd.namespace_hash) orelse return;
                    if (mr.registerKVNamespace(ns)) |km| switch (etype) {
                        // A per-key version of 1 is the key's first write.
                        .kv_put => km.recordSet(cmd.value.len, if (self.kv_handler.kv.get(cmd.key)) |e| e.version == 1 else false),
                        .kv_delete => {
                            km.recordDelete();
                            km.decrementKeyCount();
                        },
                        else => {},
                    } else |_| {}
                }
            },
            .kv_batch => {
                var it = txn_mod.iterateBatch(entry.payload) orelse return;
                while (it.next()) |op| {
                    self.waiter_pool.notify(.kv_get, op.key, resolveKVWaiter, @ptrCast(self));
                }
            },
            .stream_append => {
                const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;
                if (cmd.key.len == 0) return;
                self.waiter_pool.notify(.stream_read, cmd.key, resolveStreamWaiter, @ptrCast(self));
                self.waiter_pool.notify(.stream_group_read, cmd.key, resolveGroupReadWaiter, @ptrCast(self));
                self.notifyStreamTriggers();
                if (self.metrics_registry) |mr| {
                    const ns = self.metricsNamespace(cmd.namespace_hash) orelse return;
                    const av = stream_mod.decodeAppendValue(cmd.value);
                    if (mr.registerStream(ns, cmd.key, 0)) |sm| {
                        sm.recordAppend(stream_mod.batchRecordCount(av.payload), av.payload.len);
                    } else |_| {}
                }
            },
            .stream_delete => {
                // The stream is gone on every node that applied this entry;
                // drop its series here, not in the leader-only handler.
                const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;
                if (self.metrics_registry) |mr| {
                    const ns = self.metricsNamespace(cmd.namespace_hash) orelse return;
                    mr.unregisterStream(ns, cmd.key, 0);
                }
            },
            // A run exists from here. Only a leader wakes waiting workers: a
            // claim is not in the log, so a worker woken here on a follower
            // would take a run the leader's workers also take. Woken once
            // per pass: an ack can apply many invokes, and each wake-up is a
            // message to every other shard.
            .action_invoke => if (self.raft_node.role == .leader) {
                self.wake_workers = true;
            },
            .queue_enqueue => {
                const cmd = entry_mod.CommandPayload.deserialize(entry.payload) orelse return;
                if (cmd.key.len == 0) return;
                self.waiter_pool.notify(.queue_dequeue, cmd.key, resolveQueueWaiter, @ptrCast(self));
                if (self.metrics_registry) |mr| {
                    const ns = self.metricsNamespace(cmd.namespace_hash) orelse return;
                    // The value carries a 4-byte priority before the message.
                    const body_len = cmd.value.len -| 4;
                    if (mr.registerQueue(ns, cmd.key)) |qm| {
                        qm.recordEnqueue(1, body_len, false);
                    } else |_| {}
                }
            },
            else => {},
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
        if (!self.raft_queue_registered) {
            if (self.raft_queue) |q| {
                try self.reactor.addSource(.{ .fd = q.wake_rd, .tag = .raft_read, .interests = .{ .readable = true } });
                self.raft_queue_registered = true;
            }
        }

        // Only a tick that may sleep announces it; only one that did has a
        // wake to take back and a pipe to empty.
        const may_sleep = timeout_ms > 0 and self.inbox.prepareSleep();
        const events = self.reactor.poll(if (may_sleep) timeout_ms else 0) catch |err| {
            if (may_sleep) self.inbox.woke();
            return err;
        };
        if (may_sleep) self.inbox.woke();
        if (self.shard_metrics) |sm| sm.recordReactorLoop();

        // Process all I/O events
        for (events) |ev| {
            self.processEvent(ev);
        }

        // Drain inbox each tick
        _ = self.drainInbox();
        self.drainRaftQueue();
        self.tickRaft();

        // Expire stale blocking waiters across all subsystems
        self.waiter_pool.expireTimeouts(handleWaiterTimeout, @ptrCast(self));

        // Run cooperative background tasks (hot_flush, TTL sweep, etc.)
        _ = self.task_scheduler.tick(2_000_000); // 2ms budget

        // Drive processing pipelines (poll sources → write sinks)
        // Producers run only where this shard leads: a follower that also
        // proposed would write the same facts twice, in two logs.
        const leads = self.raft_node.role == .leader;
        if (leads) self.processing_handler.tickPipelines(self);

        // Drive workflow stream triggers (poll streams → start runs)
        if (leads) self.workflow_handler.tickStreamTriggers(self);

        // Drive workflow scheduled triggers (interval → start runs)
        if (leads) self.workflow_handler.tickSchedules(self);

        // The producers above only proposed. What has committed (at once
        // on a single node) applies here, outside any module's work.
        _ = self.applyCommitted();

        if (leads) {
            // Take the first step of runs whose start has applied, then
            // resume waiting runs whose action or child finished (both
            // propose the run's next entries, so leader-only too). On a
            // single node what a pass proposes applies at once, so a child
            // started in one pass takes its first step in the next rather
            // than a tick later.
            var pass: u8 = 0;
            self.more_work = true;
            while (pass < STEP_PASSES) : (pass += 1) {
                const before = self.raft_node.last_applied;
                self.workflow_handler.advanceStartedRuns(self);
                self.workflow_handler.checkPendingActions(self);
                _ = self.applyCommitted();
                // Only what applied can be stepped; in a cluster what a
                // pass proposed waits for an ack, so one pass is all.
                if (self.raft_node.last_applied == before) {
                    self.more_work = false;
                    break;
                }
            }
            // What the tick proposed goes to the followers now, not at the
            // next poll.
            if (self.raft_node.role == .leader) self.pump(nowMs());
        }

        self.settleConnections();
        return events.len;
    }

    /// Enter the main reactor loop. Blocks until `shutdown()` is called.
    pub fn run(self: *Shard) !void {
        self.running = true;
        log.debug("Shard {d} entering reactor loop", .{self.id});

        var last_warn_ms: u64 = 0;
        var unsaid: u64 = 0;
        var failing = false;
        while (self.running) {
            // Connections re-queued while the last tick settled run next.
            const busy = self.more_work or self.resume_fds.items.len > 0;
            _ = self.tick(if (busy) 0 else 100) catch |err| {
                // A shard whose tick keeps failing serves nothing: say so,
                // not once per tick, and do not spin a core while it lasts.
                const now = nowMs();
                if (now -| last_warn_ms >= 10_000) {
                    if (unsaid > 0) {
                        log.err("shard {d}: tick failed: {s} ({d} more in the last 10 s); the shard is not serving while this repeats — restart the node if it persists", .{ self.id, @errorName(err), unsaid });
                    } else {
                        log.err("shard {d}: tick failed: {s}; the shard is not serving while this repeats — restart the node if it persists", .{ self.id, @errorName(err) });
                    }
                    last_warn_ms = now;
                    unsaid = 0;
                } else unsaid += 1;
                failing = true;
                @import("stdx").time.sleep(10 * std.time.ns_per_ms);
                continue;
            };
            if (failing) {
                failing = false;
                log.info("shard {d}: ticking again", .{self.id});
            }
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

        // Stream retention enforcement — runs every 10 seconds
        self.task_scheduler.register(
            "stream_retention",
            10_000, // check every 10 seconds
            1_000_000, // 1ms budget per invocation
            streamRetentionTask,
            @ptrCast(self),
        ) catch {};

        // Consumer-group PEL sweeper (FLO-102) — runs every 1 second. Re-nacks
        // pending entries idle past their group's ack_timeout_ms and drops
        // poison entries past max_deliver. Local in-memory PEL mutation only
        // (PEL is not persisted), so no Raft round-trip.
        self.task_scheduler.register(
            "stream_group_sweep",
            1_000, // check every 1 second
            1_000_000, // 1ms budget per invocation
            streamGroupSweepTask,
            @ptrCast(self),
        ) catch {};

        // Persist Raft log entries to .flseg files (async_flush durability).
        if (self.shard_data_dir != null and self.durability == .async_flush) {
            self.task_scheduler.register(
                "segment_flush",
                1_000, // flush at most once per second
                2_000_000, // 2ms budget
                segmentFlushTask,
                @ptrCast(self),
            ) catch {};
        }
    }

    /// TaskScheduler callback: flush buffered Raft entries to disk.
    fn segmentFlushTask(ctx: *anyopaque, _: u64) u64 {
        const self: *Shard = @ptrCast(@alignCast(ctx));
        self.flushSegmentToDisk() catch |err| {
            self.persist_failures += 1;
            log.err("shard {d}: async segment flush failed: {s} (persist_failures={d})", .{ self.id, @errorName(err), self.persist_failures });
        };
        return 0;
    }

    /// TaskScheduler callback: evict entries older than hot_flush_seconds
    /// from every partition's UAL hot ring.
    fn hotFlushTask(ctx: *anyopaque, _: u64) u64 {
        const self: *Shard = @ptrCast(@alignCast(ctx));
        const now_ns: u64 = @intCast(@import("stdx").time.nanoTimestamp());
        const cutoff_ns = now_ns -| (self.hot_flush_seconds * std.time.ns_per_s);

        var total_evicted: u64 = 0;
        for (self.partitions) |partition| {
            total_evicted += partition.ual.evictOlderThan(cutoff_ns);
        }
        return total_evicted;
    }

    /// TaskScheduler callback: enforce stream retention policies.
    /// Computes trim targets locally, then persists through Raft so
    /// replicas apply the same deterministic trims.
    fn streamRetentionTask(ctx: *anyopaque, _: u64) u64 {
        const self: *Shard = @ptrCast(@alignCast(ctx));
        // Trims are proposals: only the leader makes them.
        if (self.raft_node.role != .leader) return 0;
        const proj = self.stream_handler.stream;
        const now_ms: u64 = @intCast(@max(0, @import("stdx").time.milliTimestamp()));
        var trims_proposed: u64 = 0;

        var it = proj.stream_metadata.iterator();
        while (it.next()) |kv| {
            const meta = kv.value_ptr;
            if (!meta.hasRetention()) continue;
            const name_hash = meta.name_hash;
            if (name_hash == 0) continue;

            // Age-based retention: compute cutoff StreamID, persist trim through Raft
            if (meta.retention_age_s > 0) {
                const cutoff_ms = now_ms -| (meta.retention_age_s * 1000);
                if (cutoff_ms > 0) {
                    const cutoff_id = StreamID{ .timestamp_ms = cutoff_ms, .sequence = std.math.maxInt(u64) };
                    // Only trim if there are records to remove
                    const first_id = proj.streamFirstId(name_hash);
                    if (!first_id.eql(StreamID.MIN) and !first_id.greaterThan(cutoff_id)) {
                        if (self.stream_handler.persistTrim(name_hash, cutoff_id)) trims_proposed += 1;
                    }
                }
            }

            // Count-based retention: resolve Nth record ID, persist trim through Raft
            if (meta.retention_count > 0) {
                // Retention is expressed in records, not append entries.
                const count = proj.streamLogicalCount(name_hash);
                if (count > meta.retention_count) {
                    const excess = count - meta.retention_count;
                    const trim_id = proj.resolveNthRecordId(name_hash, excess);
                    if (!trim_id.eql(StreamID.MIN)) {
                        if (self.stream_handler.persistTrim(name_hash, trim_id)) trims_proposed += 1;
                    }
                }
            }
        }

        return trims_proposed;
    }

    /// TaskScheduler callback: sweep every consumer group's PEL (FLO-102).
    /// Re-nacks entries idle past ack_timeout_ms and drops poison entries
    /// past max_deliver. Returns renacked + dropped for scheduler accounting.
    fn streamGroupSweepTask(ctx: *anyopaque, _: u64) u64 {
        const self: *Shard = @ptrCast(@alignCast(ctx));
        const proj = self.stream_handler.stream;
        const now_ms: u64 = @intCast(@max(0, @import("stdx").time.milliTimestamp()));
        const r = proj.sweepAllGroups(now_ms);
        return @as(u64, r.renacked) + @as(u64, r.dropped);
    }

    // ─── Event processing ────────────────────────────────────────────────

    fn processEvent(self: *Shard, ev: ReactorEvent) void {
        // Handle errors and hangups (but not on the pipes)
        if (ev.tag != .acceptor_pipe and ev.tag != .raft_read and ev.tag != .inbox_ready and (ev.err or ev.hangup)) {
            self.closeConnection(ev.fd);
            return;
        }

        switch (ev.tag) {
            .acceptor_pipe => {
                if (ev.readable) {
                    self.acceptFromPipe();
                }
            },
            .raft_read => {
                if (self.raft_queue) |q| q.drainWake();
                self.drainRaftQueue();
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
                _ = std.c.close(client_fd);
                continue;
            };

            self.reactor.addSource(.{
                .fd = client_fd,
                .tag = .client_read,
                .interests = .{ .readable = true },
            }) catch {
                self.removeConnection(client_fd);
                _ = std.c.close(client_fd);
            };
        }
    }

    /// Read data from a client socket, parse request(s), and dispatch.
    fn readFromClient(self: *Shard, fd: i32) void {
        const conn = self.getConnection(fd) orelse return;
        // A readable event can still arrive after a pause (same poll batch,
        // or an interest change not yet submitted); what it would read
        // could not drain, and would look like one oversized request.
        if (conn.reads_paused or conn.closing) return;

        // Read no more than the read buffer has room for: bytes read and
        // not kept would be requests silently lost.
        var tmp_buf: [65536]u8 = undefined;
        if (conn.read_buf.writable() == 0) {
            // A request may be larger than the buffer: grow it to hold one
            // whole request. Full at that size, the client sent more than a
            // request can be (a binary request says so in its header first;
            // a RESP command cannot).
            const cap = conn.read_buf.buf.len * 2;
            if (cap > MAX_READ_BUFFER) {
                if (conn.protocol == .resp) {
                    _ = conn.queueWrite("-ERR request over 256 KiB\r\n");
                } else {
                    self.sendErrorResponse(conn, 0, .bad_request, "bad request: request over 256 KiB");
                }
                self.flushToClient(fd);
                return self.markClosing(fd);
            }
            conn.read_buf.resize(cap) catch return self.markClosing(fd);
        }
        const room = @min(tmp_buf.len, conn.read_buf.writable());

        const n = posix.read(fd, tmp_buf[0..room]) catch |err| {
            if (err == error.WouldBlock) return;
            self.closeConnection(fd);
            return;
        };
        if (n == 0) {
            // EOF — peer closed
            self.closeConnection(fd);
            return;
        }

        if (self.metrics_registry) |m| m.server.recordBytesReceived(@intCast(n));
        if (self.shard_metrics) |sm| sm.recordBytesReceived(@intCast(n));

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
        // Closing is deferred, so the connection is still ours here.
        conn.shrinkReadBuffer();
    }

    /// Try to parse and dispatch request(s) from a connection's read buffer.
    fn processRequests(self: *Shard, fd: i32, conn: *Connection) void {
        const header_size = @sizeOf(proto.RequestHeader);

        while (conn.read_buf.readable() >= header_size) {
            if (conn.closing) return;
            if (shouldPause(conn)) return self.pauseReads(fd, conn);
            // We need contiguous bytes for parsing. Copy readable data out.
            const available = conn.read_buf.readable();
            const to_copy = @min(available, MAX_REQUEST_SIZE);

            // A contiguous copy to parse; bytes are consumed only once a
            // whole request is taken, so the rest stay in order.
            var parse_buf: [MAX_REQUEST_SIZE]u8 = undefined;
            const copied = conn.read_buf.copyOut(parse_buf[0..to_copy]);
            if (copied < header_size) break;

            // A request that cannot fit is refused as soon as its header
            // says so; waiting for its bytes would never end.
            const promised = std.mem.bytesToValue(proto.RequestHeader, parse_buf[0..header_size]);
            if (promised.magic == proto.MAGIC and @as(u64, header_size) + promised.payload_length > MAX_REQUEST_SIZE) {
                self.sendErrorResponse(conn, promised.request_id, .bad_request, "bad request: request over 256 KiB");
                self.flushToClient(fd);
                self.markClosing(fd);
                return;
            }

            const req = proto.Request.parse(parse_buf[0..copied]) catch |err| {
                switch (err) {
                    // Not enough data yet — wait for more
                    error.IncompleteRequest, error.IncompletePayload => break,
                    else => {
                        // Bad request — send error and close
                        self.sendErrorResponse(conn, 0, .bad_request, "Invalid request");
                        self.flushToClient(fd);
                        self.markClosing(fd);
                        return;
                    },
                }
            };

            const consumed = header_size + req.header.payload_length;
            conn.read_buf.consume(consumed);

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

    /// Unsent answers above which a connection's requests stop being read:
    /// a client that reads while it sends is paced, and a connection holds
    /// one large answer at a time rather than piling them up to the cap.
    const PAUSE_READS_AT: usize = 64 * 1024;

    /// How long a paused client may read nothing before pacing is lifted.
    /// A client that sends its whole pipeline before reading any answer
    /// cannot finish sending while paused; unpaced, it gets what the
    /// write cap allows, and is closed past it as it would be without
    /// pacing.
    pub const STALL_MS: u64 = 1000;

    fn shouldPause(conn: *const Connection) bool {
        return !conn.pacing_off and conn.write_buf.readable() > PAUSE_READS_AT;
    }

    /// Stop reading and running this connection's requests until its unsent
    /// answers drain (`flushToClient` resumes it) or it stalls.
    fn pauseReads(self: *Shard, fd: i32, conn: *Connection) void {
        conn.reads_paused = true;
        conn.paused_progress_ms = @import("stdx").time.monotonicMs();
        self.paused_count += 1;
        self.reactor.modifyInterests(fd, .{ .readable = false, .writable = true }) catch {};
    }

    fn resumeReads(self: *Shard, fd: i32, conn: *Connection) void {
        conn.reads_paused = false;
        self.paused_count -= 1;
        self.reactor.modifyInterests(fd, .{ .readable = true, .writable = conn.hasPendingWrites() }) catch {};
        // What it had already sent runs at the end of the tick, not inside
        // whoever resumed it.
        if (conn.read_buf.readable() > 0 and !conn.resume_queued) {
            conn.resume_queued = true;
            self.resume_fds.appendAssumeCapacity(fd);
        }
    }

    /// Lift pacing from paused clients that have read nothing for
    /// `STALL_MS`. Walks the connections, so it runs at most every
    /// `STALL_MS / 4` and only while one is paused.
    fn liftStalledPauses(self: *Shard) void {
        if (self.paused_count == 0) return;
        const now = @import("stdx").time.monotonicMs();
        if (now < self.stall_check_ms) return;
        self.stall_check_ms = now + STALL_MS / 4;
        var it = self.connections.iterator();
        while (it.next()) |entry| {
            const conn = entry.value_ptr.*;
            if (!conn.reads_paused or conn.closing) continue;
            if (now - conn.paused_progress_ms < STALL_MS) continue;
            conn.pacing_off = true;
            self.resumeReads(entry.key_ptr.*, conn);
        }
    }

    /// Close `fd` once the tick's events are handled: code up the stack may
    /// still hold its connection.
    fn markClosing(self: *Shard, fd: i32) void {
        const conn = self.getConnection(fd) orelse return;
        if (conn.closing) return;
        conn.closing = true;
        self.closing_fds.appendAssumeCapacity(fd);
    }

    /// End of tick: close what was marked closing; run what a paused
    /// connection had buffered once its answers drained or it stalled.
    fn settleConnections(self: *Shard) void {
        self.liftStalledPauses();
        std.mem.swap(std.ArrayListUnmanaged(i32), &self.resume_fds, &self.resume_running);
        var ran = false;
        for (self.resume_running.items) |fd| {
            const conn = self.getConnection(fd) orelse continue;
            conn.resume_queued = false;
            if (conn.closing or conn.reads_paused) continue;
            ran = true;
            switch (conn.protocol) {
                .resp => self.processRespRequests(fd, conn),
                else => self.processRequests(fd, conn),
            }
        }
        self.resume_running.clearRetainingCapacity();
        // What they proposed goes out at the next tick's pump; that tick
        // must not sleep first.
        if (ran and self.raft_node.role == .leader) self.more_work = true;
        for (self.closing_fds.items) |fd| {
            const conn = self.getConnection(fd) orelse continue;
            if (conn.closing) self.closeConnection(fd);
        }
        self.closing_fds.clearRetainingCapacity();
    }

    // ─── RESP Protocol Handler ──────────────────────────────────────────

    /// Process RESP (Redis protocol) requests from a connection's read buffer.
    /// Parses RESP commands, translates to Flo operations, executes directly
    /// via the appropriate handler, and serializes responses back to RESP.
    fn processRespRequests(self: *Shard, fd: i32, conn: *Connection) void {
        var resp_parser = resp_mod.Parser.init(self.allocator);
        defer resp_parser.deinit();

        while (conn.read_buf.readable() > 0) {
            if (conn.closing) return;
            if (shouldPause(conn)) return self.pauseReads(fd, conn);
            // Peek all available data without consuming
            const available = conn.read_buf.readable();
            var parse_buf: [MAX_REQUEST_SIZE]u8 = undefined;
            const to_copy = @min(available, MAX_REQUEST_SIZE);
            const copied = conn.read_buf.copyOut(parse_buf[0..to_copy]);
            if (copied == 0) break;

            // Try to parse a complete RESP value
            const parsed = resp_parser.parse(parse_buf[0..copied]) catch {
                // Parse error — send RESP error and close
                _ = conn.queueWrite("-ERR invalid RESP data\r\n");
                self.flushToClient(fd);
                self.markClosing(fd);
                return;
            };

            // Incomplete — wait for more
            if (parsed == null) break;

            const result = parsed.?;
            var resp_value = result.value;
            conn.read_buf.consume(result.consumed);

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
                    // As after a binary request: what it proposed applies now.
                    _ = self.applyCommitted();
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
                .reserved = .{0} ** 8,
            },
            .namespace = cmd.namespace,
            .key = cmd.key,
            .value = cmd.value,
            .options = "",
        };

        conn.requests_total += 1;

        // RESP has no request ids and no forwarding: a write parked for a
        // peer's ack would be answered out of order behind a pipelined
        // read, and a follower cannot take it. On a single node the answer
        // is inline, so writes are served there only.
        if (self.raft_node.peer_count > 0 and dispatcher_mod.opWrites(cmd.opcode)) {
            _ = conn.queueWrite("-ERR RESP writes are served by a single-node server; use the Flo protocol on a cluster\r\n");
            self.freeRespCommand(cmd);
            return;
        }
        // An enqueue's applier labels the queue by resolving its namespace
        // (a stream append's handler proposes this itself).
        if (cmd.opcode == .queue_enqueue) self.namespace_handler.proposeImplicitCreate(cmd.namespace, self, false);

        // Dispatch to the appropriate handler and get CommandResult
        const cmd_result = self.handleRespOpcode(cmd.opcode, req);
        defer self.kv_handler.freeResult(cmd_result);

        // A stream append or queue enqueue is answered once its entry
        // applies, like any client's: on a single node, inline. (Key-value
        // writes over RESP still go straight to the projection.) A parked
        // request is re-parsed from its bytes, so it carries its length
        // and checksum.
        if (cmd_result == .parked) {
            defer self.freeRespCommand(cmd);
            var parked = req;
            parked.header.payload_length = @intCast(2 + req.namespace.len + 2 + req.key.len + 4 + req.value.len + 2 + req.options.len);
            const wire = serializeRequest(self.allocator, parked) catch {
                _ = conn.queueWrite("-ERR internal error: write proposed, its answer lost — do not resend\r\n");
                return;
            };
            defer self.allocator.free(wire);
            parked.header.crc32 = parked.header.computeCRC32(wire[@sizeOf(proto.RequestHeader)..]);
            self.park(conn, parked, cmd_result.parked, respondResp);
            return;
        }

        // Translate CommandResult → RESP and serialize
        const resp_value = resp_mod.translateResult(cmd_result);
        const response_bytes = resp_mod.serialize(self.allocator, resp_value) catch {
            _ = conn.queueWrite("-ERR internal error\r\n");
            self.freeRespCommand(cmd);
            return;
        };
        defer self.allocator.free(response_bytes);

        _ = conn.queueWrite(response_bytes);

        // Free any heap-allocated fields from translateCommand
        self.freeRespCommand(cmd);
    }

    /// A RESP write applied: the module's answer, in RESP.
    fn respondResp(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: proto.Request) void {
        const self: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        var id_buf: [20]u8 = undefined;
        const result: CommandResult = switch (@as(proto.OpCode, @enumFromInt(req.header.op_code))) {
            .stream_append => self.stream_handler.respondAppend(),
            .queue_enqueue => self.queue_handler.respondEnqueue(&id_buf),
            else => .ok,
        };
        if (result != .err) self.namespace_handler.markNamespaceHasData(req.namespace, self);
        const bytes = resp_mod.serialize(self.allocator, resp_mod.translateResult(result)) catch {
            _ = conn.queueWrite("-ERR internal error\r\n");
            return;
        };
        defer self.allocator.free(bytes);
        _ = conn.queueWrite(bytes);
    }

    /// Route a RESP opcode to the appropriate handler, returning a CommandResult.
    fn handleRespOpcode(self: *Shard, opcode: proto.OpCode, req: proto.Request) CommandResult {
        return switch (opcode) {
            .ping => .pong,
            .kv_get => self.kv_handler.handleCommand(req),
            .kv_put => self.kv_handler.handleCommand(req),
            .kv_delete => self.kv_handler.handleCommand(req),
            .kv_incr => self.kv_handler.handleCommand(req),
            .kv_touch => self.kv_handler.handleCommand(req),
            .kv_persist => self.kv_handler.handleCommand(req),
            .kv_exists => self.kv_handler.handleCommand(req),
            .kv_json_get => self.kv_handler.handleCommand(req),
            .kv_json_set => self.kv_handler.handleCommand(req),
            .kv_json_del => self.kv_handler.handleCommand(req),
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
    /// Free heap-allocated fields from a translated RESP command.
    /// translateCommand uses allocator.dupe — non-empty slices are heap-owned.
    fn freeRespCommand(self: *Shard, cmd: resp_mod.RespCommand) void {
        if (cmd.key.len > 0) self.allocator.free(cmd.key);
        if (cmd.value.len > 0) self.allocator.free(cmd.value);
    }

    /// Flush pending write data from a connection to the socket.
    pub fn flushToClient(self: *Shard, fd: i32) void {
        const conn = self.getConnection(fd) orelse return;
        if (conn.closing) return;
        if (conn.write_overflow) {
            if (self.overflowWarnDue()) |unsaid| {
                const why = "over 4 MiB of answers unsent (client not reading, or one answer too large)";
                if (unsaid == 0) {
                    log.warn("shard {d}: closing connection {d}: " ++ why, .{ self.id, conn.id });
                } else {
                    log.warn("shard {d}: closing connection {d}: " ++ why ++ " ({d} more since the last line)", .{ self.id, conn.id, unsaid });
                }
            }
            self.markClosing(fd);
            return;
        }

        while (conn.hasPendingWrites()) {
            const data = conn.pendingWriteData();
            const written = @import("stdx").io.tryWriteFd(fd, data) catch |err| {
                if (err == error.WouldBlock) {
                    // Socket buffer full — arm writable and return
                    self.reactor.armWritable(fd) catch {};
                    return;
                }
                // Write error — close connection
                self.markClosing(fd);
                return;
            };
            if (written == 0) {
                self.markClosing(fd);
                return;
            }
            if (self.metrics_registry) |m| m.server.recordBytesSent(@intCast(written));
            if (self.shard_metrics) |sm| sm.recordBytesSent(@intCast(written));
            conn.consumeWritten(written);
            if (conn.reads_paused) conn.paused_progress_ms = @import("stdx").time.monotonicMs();
        }

        // All writes flushed — disarm writable, and read again if paused.
        conn.shrinkWriteBuffer();
        conn.pacing_off = false;
        if (conn.reads_paused) return self.resumeReads(fd, conn);
        self.reactor.disarmWritable(fd) catch {};
    }

    // ─── Response helpers ────────────────────────────────────────────────

    /// `cluster_status` — this node's identity, role and group. A node
    /// running alone is the leader of a one-member group. States: 0
    /// follower, 1 electing, 2 leader, 3 joining (no seat yet), 4
    /// diverged.
    fn dispatchClusterStatus(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: proto.Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        const raft = shard.raft_node;
        const state: u8 = if (shard.diverged)
            4
        else if (shard.raft_network != null and !raft.timer_enabled)
            3
        else switch (raft.role) {
            .follower => 0,
            .candidate => 1,
            .leader => 2,
        };

        // Membership is what the log says; a node the log has not named
        // yet (a joiner, or one running alone) is a group of itself.
        var ids: [membership.MAX_MEMBERS]u32 = undefined;
        const member_count: u32 = @intCast(@max(1, raft.memberIds(&ids).len));
        const leader_id: u32 = if (shard.raft_network != null) raft.leader_id else shard.cluster_node_id;

        var buf: [21]u8 = undefined;
        std.mem.writeInt(u32, buf[0..4], shard.cluster_node_id, .little);
        std.mem.writeInt(u32, buf[4..8], leader_id, .little);
        std.mem.writeInt(u64, buf[8..16], raft.current_term, .little);
        buf[16] = state;
        std.mem.writeInt(u32, buf[17..21], member_count, .little);

        shard.sendOkResponse(conn, req.header.request_id, &buf);
    }

    pub fn sendErrorResponse(self: *Shard, conn: *Connection, request_id: u64, status: proto.StatusCode, msg: []const u8) void {
        _ = self;
        var buf: [512]u8 = undefined;
        const serialized = proto.Response.serializeNew(status, request_id, msg, &buf) catch return;
        _ = conn.queueWrite(serialized);
    }

    /// Deliver an already-serialized deferred response to a connection.
    ///
    /// Blocking-read waiters live on the data shard, but a connection's fd,
    /// buffers, and reactor registration belong to `owner_shard`. When that is
    /// this shard, write directly. Otherwise marshal the bytes to the owning
    /// shard's inbox so the socket write happens on the owning thread — never
    /// touch another shard's connection from here.
    ///
    /// `conn_id` is the connection's generation id at waiter-registration
    /// time. We verify it against the live connection before writing so that
    /// fd reuse (close + accept at the same fd) cannot misdirect a stale
    /// blocking-read response to the wrong client.
    pub fn deliverDeferred(self: *Shard, owner_shard: u16, fd: i32, conn_id: u32, bytes: []const u8) void {
        const my_id: u16 = @intCast(self.id);
        if (owner_shard == REMOTE_OWNER) {
            self.sendForwardReply(conn_id, @bitCast(fd), bytes);
            return;
        }
        if (owner_shard == my_id) {
            const conn = self.getConnection(fd) orelse return;
            if (conn.id != conn_id) return; // fd was reused for a new connection
            _ = conn.queueWrite(bytes);
            self.flushToClient(fd);
            return;
        }

        const peers = self.peer_shards orelse return;
        if (owner_shard >= peers.len) return;

        const payload = self.allocator.alloc(u8, bytes.len) catch {
            log.warn("deferred response dropped: alloc failed (fd={d})", .{fd});
            return;
        };
        @memcpy(payload, bytes);
        // Pack (conn_id << 32) | fd into the inbox sequence field so the
        // receiver can verify the connection generation. fd is i32 but
        // always positive — masking to u32 is safe.
        const fd_bits: u64 = @as(u32, @bitCast(fd));
        const seq: u64 = (@as(u64, conn_id) << 32) | fd_bits;
        const ok = peers[owner_shard].inbox.send(.{
            .tag = .deferred_response,
            .src_shard = @intCast(my_id),
            .payload_len = @intCast(payload.len),
            .sequence = seq,
            .payload_ptr = payload.ptr,
        });
        if (!ok) {
            self.allocator.free(payload);
            log.warn("deferred response dropped: inbox full (owner_shard={d} fd={d})", .{ owner_shard, fd });
        }
    }

    /// Serialize a status+data response and deliver it via `deliverDeferred`.
    pub fn deliverDeferredResponse(self: *Shard, owner_shard: u16, fd: i32, conn_id: u32, request_id: u64, status: proto.StatusCode, data: []const u8) void {
        var buf: [MAX_REQUEST_SIZE + @sizeOf(proto.ResponseHeader)]u8 = undefined;
        // Same rule as `sendOkResponse`: a parked client is answered, never
        // left to its own timeout because the answer is too large to frame.
        const serialized = proto.Response.serializeNew(status, request_id, data, &buf) catch blk: {
            log.warn("shard {d}: deferred answer to request {d} is {d} bytes, over the frame limit; answered with an error", .{ self.id, request_id, data.len });
            break :blk proto.Response.serializeNew(.internal_error, request_id, "internal error: answer over 256 KiB — ask for less", &buf) catch unreachable;
        };
        self.deliverDeferred(owner_shard, fd, conn_id, serialized);
    }

    /// Send an OK response with data payload on a connection.
    pub fn sendOkResponse(self: *Shard, conn: *Connection, request_id: u64, data: []const u8) void {
        _ = self;
        var buf: [MAX_REQUEST_SIZE + @sizeOf(proto.ResponseHeader)]u8 = undefined;
        // A client waits for every request's answer: one too large to
        // frame is answered with an error, never left out.
        const serialized = proto.Response.serializeNew(.ok, request_id, data, &buf) catch
            proto.Response.serializeNew(.internal_error, request_id, "internal error: answer over 256 KiB — ask for less", &buf) catch unreachable;
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

/// Serialize walk results for action_list in scan wire format.
///
/// Produces: [count:u32]([key_len:u16][key][value_len:u32][value])*[has_more:u8][cursor_len:u16][cursor]?
/// The CLI parses this format and extracts action names from the key field.
fn serializeWalkActionEntries(
    allocator: std.mem.Allocator,
    names: []const []const u8,
    next_cursor: ?[]const u8,
    _: []const *anyopaque,
    _: []const u8,
) ![]u8 {
    const cursor_bytes = next_cursor orelse &[_]u8{};
    const has_more: u8 = if (next_cursor != null) 1 else 0;

    // Calculate total buffer size
    var total_size: usize = 4; // count:u32
    for (names) |name| {
        total_size += 2 + name.len + 4; // key_len:u16 + key + value_len:u32
    }
    total_size += 1 + 2 + cursor_bytes.len; // has_more:u8 + cursor_len:u16 + cursor

    const buf = try allocator.alloc(u8, total_size);
    errdefer allocator.free(buf);
    var pos: usize = 0;

    // count
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(names.len), .little);
    pos += 4;

    // entries
    for (names) |name| {
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(name.len), .little);
        pos += 2;
        @memcpy(buf[pos..][0..name.len], name);
        pos += name.len;
        std.mem.writeInt(u32, buf[pos..][0..4], 0, .little); // value_len = 0
        pos += 4;
    }

    // pagination trailer
    buf[pos] = has_more;
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(cursor_bytes.len), .little);
    pos += 2;
    if (cursor_bytes.len > 0) {
        @memcpy(buf[pos..][0..cursor_bytes.len], cursor_bytes);
    }

    return buf;
}

/// Serialize walk results as binary wire format for processing jobs.
///
/// Wire format: [count:u32]([name_len:u16][name][job_id_len:u16][job_id]
///              [status_len:u16][status][parallelism:u32][created_at:i64])*
///              [has_more:u8][cursor_len:u16]
fn serializeWalkProcessingJobs(
    allocator: std.mem.Allocator,
    names: []const []const u8,
    next_cursor: ?[]const u8,
    contexts: []const *anyopaque,
    namespace: []const u8,
) ![]u8 {
    const JobInfo = struct { name: []const u8, job_id: []const u8, status: []const u8, parallelism: u32, created_at: i64 };
    var jobs: std.ArrayListUnmanaged(JobInfo) = .empty;
    defer jobs.deinit(allocator);

    const req_ns = if (namespace.len > 0) namespace else "default";

    for (names) |name| {
        for (contexts) |ctx| {
            const handler: *ProcessingHandler = @ptrCast(@alignCast(ctx));
            var it = handler.jobs.iterator();
            while (it.next()) |entry| {
                const job = entry.value_ptr;
                if (std.mem.eql(u8, job.name_owned, name) and std.mem.eql(u8, job.namespace_owned, req_ns)) {
                    try jobs.append(allocator, .{
                        .name = job.name_owned,
                        .job_id = job.job_id_owned,
                        .status = job.status.toString(),
                        .parallelism = job.parallelism,
                        .created_at = job.created_at_ms,
                    });
                    break;
                }
            }
        }
    }

    // Calculate total size
    const cursor_bytes = next_cursor orelse &[_]u8{};
    const has_more: u8 = if (next_cursor != null) 1 else 0;
    var total: usize = 4; // count: u32
    for (jobs.items) |j| {
        total += 2 + j.name.len; // name_len:u16 + name
        total += 2 + j.job_id.len; // job_id_len:u16 + job_id
        total += 2 + j.status.len; // status_len:u16 + status
        total += 4; // parallelism:u32
        total += 8; // created_at:i64
    }
    total += 1 + 2 + cursor_bytes.len; // has_more:u8 + cursor_len:u16 + cursor

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    std.mem.writeInt(u32, buf[0..4], @intCast(jobs.items.len), .little);
    var pos: usize = 4;
    for (jobs.items) |j| {
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(j.name.len), .little);
        pos += 2;
        @memcpy(buf[pos..][0..j.name.len], j.name);
        pos += j.name.len;
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(j.job_id.len), .little);
        pos += 2;
        @memcpy(buf[pos..][0..j.job_id.len], j.job_id);
        pos += j.job_id.len;
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(j.status.len), .little);
        pos += 2;
        @memcpy(buf[pos..][0..j.status.len], j.status);
        pos += j.status.len;
        std.mem.writeInt(u32, buf[pos..][0..4], j.parallelism, .little);
        pos += 4;
        std.mem.writeInt(i64, buf[pos..][0..8], j.created_at, .little);
        pos += 8;
    }

    // pagination trailer
    buf[pos] = has_more;
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(cursor_bytes.len), .little);
    pos += 2;
    if (cursor_bytes.len > 0) {
        @memcpy(buf[pos..][0..cursor_bytes.len], cursor_bytes);
    }
    return buf;
}

/// Serialize walk results as binary wire format for workflow definitions.
///
/// Wire format: [count:u32]([name_len:u16][name][version_len:u16][version][created_at:i64])*
///              [has_more:u8][cursor_len:u16]
fn serializeWalkWorkflowDefs(
    allocator: std.mem.Allocator,
    names: []const []const u8,
    next_cursor: ?[]const u8,
    contexts: []const *anyopaque,
    namespace: []const u8,
) ![]u8 {
    // First pass: collect matching definitions and compute size
    const DefInfo = struct { name: []const u8, version: []const u8, created_at: i64 };
    var defs: std.ArrayListUnmanaged(DefInfo) = .empty;
    defer defs.deinit(allocator);

    for (names) |name| {
        for (contexts) |ctx| {
            const handler: *WorkflowHandler = @ptrCast(@alignCast(ctx));
            var dit = handler.definitions.iterator();
            while (dit.next()) |entry| {
                const def = entry.value_ptr;
                if (!std.mem.eql(u8, def.name_owned, name)) continue;
                if (namespace.len > 0) {
                    const map_key = entry.key_ptr.*;
                    if (!std.mem.startsWith(u8, map_key, namespace)) continue;
                    if (map_key.len <= namespace.len or map_key[namespace.len] != ':') continue;
                }
                try defs.append(allocator, .{
                    .name = def.name_owned,
                    .version = def.version_owned,
                    .created_at = def.created_at_ms,
                });
                break;
            }
        }
    }

    // Calculate total size
    const cursor_bytes = next_cursor orelse &[_]u8{};
    const has_more: u8 = if (next_cursor != null) 1 else 0;
    var total: usize = 4; // count: u32
    for (defs.items) |d| {
        total += 2 + d.name.len; // name_len:u16 + name
        total += 2 + d.version.len; // version_len:u16 + version
        total += 8; // created_at:i64
    }
    total += 1 + 2 + cursor_bytes.len; // has_more:u8 + cursor_len:u16 + cursor

    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    // Write entries
    std.mem.writeInt(u32, buf[0..4], @intCast(defs.items.len), .little);
    var pos: usize = 4;
    for (defs.items) |d| {
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(d.name.len), .little);
        pos += 2;
        @memcpy(buf[pos..][0..d.name.len], d.name);
        pos += d.name.len;
        std.mem.writeInt(u16, buf[pos..][0..2], @intCast(d.version.len), .little);
        pos += 2;
        @memcpy(buf[pos..][0..d.version.len], d.version);
        pos += d.version.len;
        std.mem.writeInt(i64, buf[pos..][0..8], d.created_at, .little);
        pos += 8;
    }

    // pagination trailer
    buf[pos] = has_more;
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], @intCast(cursor_bytes.len), .little);
    pos += 2;
    if (cursor_bytes.len > 0) {
        @memcpy(buf[pos..][0..cursor_bytes.len], cursor_bytes);
    }
    return buf;
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
/// `names` reach here namespace-STRIPPED, but stream metadata is keyed by the
/// qualified name, so `ns` is needed to look each one back up.
fn serializeWalkStreamNames(allocator: std.mem.Allocator, names: []const []const u8, next_cursor: ?[]const u8, stream: *const StreamProjection, ns: []const u8) ![]u8 {
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
        // partition_count from stream metadata, keyed by the qualified name
        var qbuf: [handler_mod.MAX_QUALIFIED_KEY]u8 = undefined;
        const pc = stream.getPartitionCount(handler_mod.qualifyKey(&qbuf, ns, n) catch n);
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
///
/// Routes through `deliverDeferredResponse` so the response reaches the
/// connection-owning shard even when the waiter expired on a different
/// (data) shard after cross-shard request routing.
fn handleWaiterTimeout(waiter: *const Waiter, ctx: *anyopaque) void {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    switch (waiter.kind) {
        .kv_get => {
            // A watch on a key that exists but did not change answers what is
            // there now, so the asker can tell "unchanged" from "gone".
            if (shard.defaultPartition().kv.get(waiter.key())) |entry| {
                sendKVWaiterValue(shard, waiter, entry.value, entry.version);
            } else {
                shard.deliverDeferredResponse(waiter.owner_shard, waiter.fd, waiter.conn_id, waiter.request_id, .not_found, "");
            }
        },
        .queue_dequeue => {
            // Queue blocking dequeue timeout → empty messages response (count = 0)
            var buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &buf, 0, .little);
            shard.deliverDeferredResponse(waiter.owner_shard, waiter.fd, waiter.conn_id, waiter.request_id, .ok, &buf);
        },
        // stream_read / action_await / stream_group_read → empty OK response
        .stream_read, .action_await, .stream_group_read => {
            shard.deliverDeferredResponse(waiter.owner_shard, waiter.fd, waiter.conn_id, waiter.request_id, .ok, "");
        },
    }
}

/// KV waiter resolver: look up the key in the KV projection and send the value
/// if version > min_version. Returns true if waiter was satisfied.
pub fn resolveKVWaiter(waiter: *Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const entry = shard.defaultPartition().kv.get(waiter.key()) orelse return false;
    if (entry.version <= waiter.min_version) return false;
    sendKVWaiterValue(shard, waiter, entry.value, entry.version);
    return true;
}

fn sendKVWaiterValue(shard: *Shard, waiter: *const Waiter, value: []const u8, version: u64) void {
    var resp = proto.Response.init(waiter.request_id, .ok, value);
    resp.prefix = version;
    const MAX_BUF = @sizeOf(proto.ResponseHeader) + 8 + (256 * 1024);
    var buf: [MAX_BUF]u8 = undefined;
    if (resp.serialize(&buf)) |serialized| {
        shard.deliverDeferred(waiter.owner_shard, waiter.fd, waiter.conn_id, serialized);
    } else |_| {
        // The waiter is gone once this returns; an asker left without an
        // answer would hang until its own timeout.
        shard.deliverDeferredResponse(waiter.owner_shard, waiter.fd, waiter.conn_id, waiter.request_id, .internal_error, "internal error: value too large to send");
    }
}

/// Stream waiter resolver: re-run the parked read's window. The read is the
/// test — an append to a same-named stream in another namespace, or one the
/// window excludes, finds nothing and leaves the waiter parked.
pub fn resolveStreamWaiter(waiter: *Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const handler = shard.stream_handler;
    const window = &waiter.stream;
    var buf: [StreamHandler.MAX_READ_BATCH]@import("../projection/stream.zig").StreamRecord = undefined;
    const records = handler.readRecords(window.*, &buf);
    if (records.len == 0) {
        // Everything up to the stream's last id (or `end`) was scanned and none
        // of it is in the window; ids only grow, so the next wake starts there
        // instead of rescanning from the parked cursor.
        const last = handler.stream.streamLastId(window.name_hash);
        const scanned = if (last.greaterThan(window.end)) window.end else last;
        if (scanned.greaterThan(window.after)) window.after = scanned;
        return false;
    }

    const result = handler.messages(records, waiter.key());
    defer handler.freeResult(result);
    switch (result) {
        .stream_messages => |m| shard.deliverDeferredResponse(waiter.owner_shard, waiter.fd, waiter.conn_id, waiter.request_id, .ok, m.data),
        else => shard.deliverDeferredResponse(waiter.owner_shard, waiter.fd, waiter.conn_id, waiter.request_id, .internal_error, ""),
    }
    return true;
}

/// Group read waiter resolver: wake the client so it retries its group read.
///
/// Group reads require PEL state management (consumer tracking, ack
/// deadlines) that only `handleGroupRead` handles correctly, so instead of
/// duplicating it we send an empty OK response to break the client's
/// blocking poll; the client retries and gets data through `handleGroupRead`.
pub fn resolveGroupReadWaiter(waiter: *Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const last = shard.stream_handler.stream.streamLastId(waiter.stream.name_hash);
    if (!last.greaterThan(waiter.stream.after)) return false;

    shard.deliverDeferredResponse(waiter.owner_shard, waiter.fd, waiter.conn_id, waiter.request_id, .ok, "");
    return true;
}

/// Queue waiter resolver: try to dequeue a message.
/// Returns true if a message was available and sent.
pub fn resolveQueueWaiter(waiter: *Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const partition = shard.defaultPartition();

    const now_ns = @as(u64, @intCast(@import("stdx").time.milliTimestamp())) * 1_000_000;
    partition.queue.expireLeases(now_ns);

    // min_version holds the pre-computed queue_name_hash
    const queue_name_hash = waiter.min_version;
    const maybe_result = partition.queue.dequeue(now_ns, queue_name_hash) catch return false;
    const deq_result = maybe_result orelse return false;

    // Serialize BEFORE auto-ack (ack frees the message payload).
    const results = [1]@import("../projection/queue.zig").DequeueResult{deq_result};
    const data = queue_handler_mod.serializeDequeueResultsPub(shard.queue_handler.allocator, &results) catch return false;
    defer shard.queue_handler.allocator.free(data);

    // Auto-ack: persist a queue_ack entry so the message doesn't reappear after restart
    {
        var seq_key: [8]u8 = undefined;
        std.mem.writeInt(u64, &seq_key, deq_result.seq, .little);

        // Same contract as the queue handler's dequeue-ack: log, never fail
        // the dequeue. The ack applies when it commits.
        _ = persistence_mod.proposeEntry(shard, .queue_ack, entry_mod.Flags.NONE, "", &seq_key, &[_]u8{}) catch |err| {
            log.err("shard {d}: queue ack for seq {d} not persisted: {s}; message delivered, may be redelivered after a restart", .{ shard.id, deq_result.seq, @errorName(err) });
        };
    }

    shard.deliverDeferredResponse(waiter.owner_shard, waiter.fd, waiter.conn_id, waiter.request_id, .ok, data);
    return true;
}

// ═══════════════════════════════════════════════════════════════════════════════
// UAL Persistence Callback
// ═══════════════════════════════════════════════════════════════════════════════

/// Called by UAL.append() after every entry write. Feeds the entry to the
/// SegmentWriter so it's included in the next segment flush to disk.
/// The context is the heap-allocated SegmentWriter, not the Shard (returned
/// by value from init), so the hook is valid before bootstrap.
fn segmentBufferCallback(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
    const writer: *SegmentWriter = @ptrCast(@alignCast(ctx));
    writer.addEntry(entry) catch |err| {
        // Do not swallow: a failed persist means this committed entry is not
        // queued for disk and would be lost on restart. Surface it.
        writer.buffer_failures += 1;
        log.err("shard {d}: failed to buffer entry index={d} for persistence: {s} (buffer_failures={d})", .{ writer.partition_id, entry.header.index, @errorName(err), writer.buffer_failures });
    };
}

fn catchUpReadRange(ctx: *anyopaque, start: u64, buf: []entry_mod.Entry, arena: []u8) usize {
    const dl: *DurableLog = @ptrCast(@alignCast(ctx));
    return dl.readRange(start, buf, arena);
}

/// Raft log truncation, carried into the durable log.
fn durableTruncate(ctx: *anyopaque, after_index: u64) void {
    const dl: *DurableLog = @ptrCast(@alignCast(ctx));
    dl.truncateAfter(after_index) catch |err| {
        log.err("durable log: cannot truncate segments after index {d}: {s} (truncate_failures={d}); no segment is flushed until the cut completes, which every flush retries", .{ after_index, @errorName(err), dl.truncate_failures });
    };
}

/// Per-shard HARDSTATE writer — the sink the Raft node persists through.
/// Heap-allocated so the sink's context pointer stays valid.
const HardStateStore = struct {
    /// Borrowed from `Shard.shard_data_dir`; the store is destroyed first.
    dir: []const u8,
    node_id: u32,
    shard_id: u16,

    fn persist(ctx: *anyopaque, term: u64, voted_for: u32) bool {
        const self: *HardStateStore = @ptrCast(@alignCast(ctx));
        hard_state_mod.save(self.dir, .{ .node_id = self.node_id, .term = term, .voted_for = voted_for }) catch |err| {
            log.err("shard {d}: cannot write {s}/{s}: {s}; this node will not vote until it can", .{ self.shard_id, self.dir, hard_state_mod.FILENAME, @errorName(err) });
            return false;
        };
        return true;
    }
};

/// The Raft ring keeps recent entries for replication catch-up. A quarter
/// of the hot buffer, capped at 16 MiB so many shards do not each take a
/// quarter of a large buffer, and floored so the largest entry (a full
/// transaction batch) still fits: a ring smaller than an entry rejects the
/// write.
pub const RAFT_RING_MAX: usize = 16 * 1024 * 1024;
pub const RAFT_RING_MIN: usize = 2 * 1024 * 1024;

comptime {
    std.debug.assert(@import("../kv/handler.zig").MAX_APPLY_PAYLOAD + entry_mod.HEADER_SIZE <= RAFT_RING_MIN);
}

/// How a shard's Raft group comes up.
pub const ClusterRole = enum {
    /// No peer listener: the one member, leading from init.
    single,
    /// The first member: an empty log bootstraps and writes the config
    /// naming this node; a log that already names members follows them.
    bootstrap,
    /// Joining members that exist: a follower with no vote until a config
    /// entry from the leader names it.
    join,
};

/// A write waiting for its entry to commit: where to answer, the request
/// to answer from, and the handler's tail that does it.
pub const Pending = struct {
    active: bool = false,
    index: u64 = 0,
    term: u64 = 0,
    owner_shard: u16 = 0,
    fd: i32 = -1,
    conn_id: u32 = 0,
    request_id: u64 = 0,
    bytes: []u8 = &.{},
    responder: HandlerFn = undefined,
};
pub const PENDING_SLOTS: usize = 2 * raft_node_mod.MAX_OUTSTANDING;

/// A client's write sent to the leader, and what came back.
const Forward = struct {
    active: bool = false,
    id: u32 = 0,
    owner_shard: u16 = 0,
    fd: i32 = -1,
    conn_id: u32 = 0,
    request_id: u64 = 0,
    bytes: []u8 = &.{},
    /// The leader it went to and the term then; 0 until it has gone.
    sent_to: u32 = 0,
    sent_term: u64 = 0,
    deadline_ms: u64 = 0,
    /// The leader's answer, held until `applied_by` is applied here.
    reply: ?[]u8 = null,
    applied_by: u64 = 0,
};
const FORWARD_SLOTS: usize = 1024;
/// How long a write waits for a leader to be known, and how long an
/// answer is held for this node to catch up to it.
pub const FORWARD_TIMEOUT_MS: u64 = 5000;
/// Forward ids stand in for an fd on the leader's proxy connection: ids
/// from here up are negative as an fd, so a client closing on the leader
/// can never match one in the waiter pool.
const FORWARD_ID_FIRST: u32 = 0x8000_0000;
pub const DIVERGED_MESSAGE = "unavailable: this node's data diverged from the group and it takes no writes; use another node";

/// How many step passes one tick takes: a chain of child starts that
/// deep resolves in one tick on a single node; a deeper one continues on
/// the next tick, which does not wait.
const STEP_PASSES: u8 = 4;
/// Steady conditions are said once per interval, not per tick.
const WARN_INTERVAL_MS: u64 = 30_000;
const JOIN_WARN_INTERVAL_MS: u64 = 30_000;
/// The `owner_shard` of a connection that stands for a client on another
/// node: `fd` is then the forward id and `conn_id` the node.
pub const REMOTE_OWNER: u16 = 0xFFFF;

/// Most entries in one AppendEntries, and the payload bytes they may
/// hold: a batch is sized by bytes, and must hold the largest entry any
/// handler writes or no follower could ever pass it.
pub const RPC_MAX_ENTRIES: usize = 1024;
pub const RPC_BATCH_BYTES: usize = @max(1024 * 1024, @import("../kv/handler.zig").MAX_APPLY_PAYLOAD + entry_mod.HEADER_SIZE);
comptime {
    std.debug.assert(transport.APPEND_REQ_PREFIX + RPC_MAX_ENTRIES * entry_mod.HEADER_SIZE + RPC_BATCH_BYTES <= transport.MAX_PAYLOAD_SIZE);
}
const JOIN_ASK_INTERVAL_MS: u64 = 1000;

/// Membership at boot comes from the log's latest config entry; a single
/// node and a first member with nothing in the log lead at once.
fn bringUpGroup(raft: *RaftNode, role: ClusterRole, buf: []u8, shard_id: u16, node_id: u32) !void {
    const cfg_index = raft.log.last_config_index;
    if (role == .single) {
        // A data directory that belonged to a group must not lead alone:
        // it would take writes the group never sees.
        if (cfg_index > 0) {
            const e = raft.log.getEntryCopy(cfg_index, buf) orelse {
                log.err("shard {d}: the config entry at index {d} could not be read; refusing to guess whether this data directory belonged to a group", .{ shard_id, cfg_index });
                return error.MembershipUnreadable;
            };
            var ids: [membership.MAX_MEMBERS]u32 = undefined;
            const members = membership.decode(e.payload, &ids) orelse {
                log.err("shard {d}: the config entry at index {d} is not a member list", .{ shard_id, cfg_index });
                return error.MembershipUnreadable;
            };
            if (members.len > 1 or !membership.names(members, node_id)) {
                log.err("shard {d}: this data directory belonged to a group of {d} (members {any}); start with --join to rejoin them, or delete it to start alone", .{ shard_id, members.len, members });
                return error.DataDirWasClustered;
            }
        }
        return raft.bootstrap();
    }
    if (cfg_index > 0) {
        const e = raft.log.getEntryCopy(cfg_index, buf) orelse {
            log.err("shard {d}: the config entry at index {d} could not be read; refusing to guess the membership", .{ shard_id, cfg_index });
            return error.MembershipUnreadable;
        };
        var ids: [membership.MAX_MEMBERS]u32 = undefined;
        const members = membership.decode(e.payload, &ids) orelse {
            log.err("shard {d}: the config entry at index {d} is not a member list", .{ shard_id, cfg_index });
            return error.MembershipUnreadable;
        };
        raft.setMembership(members, cfg_index);
        if (cfg_index <= raft.last_applied) raft.commitMembership(members);
        // What the segments flushed under a commit watermark is committed;
        // the rest of the log waits for a leader to say so.
        raft.commit_index = raft.last_applied;
        log.info("shard {d}: members {any} from the log (config index {d}); following until a leader speaks{s}", .{ shard_id, members, cfg_index, if (raft.timer_enabled) "" else " (this node is not a member)" });
        return;
    }
    switch (role) {
        .single => unreachable,
        .bootstrap => {
            try raft.bootstrap();
            var cfg: [membership.MAX_SIZE]u8 = undefined;
            _ = try raft.propose(.raft_config, entry_mod.Flags.NONE, 0, membership.encode(&.{node_id}, &cfg));
            log.info("shard {d}: first member; leading a group of one", .{shard_id});
        },
        .join => {
            raft.commit_index = raft.last_applied;
            raft.timer_enabled = false;
            log.info("shard {d}: joining; following until a config entry names this node", .{shard_id});
        },
    }
}

/// The config entry committed: what it names is the membership a
/// truncation falls back to.
fn applyRaftConfig(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
    const raft: *RaftNode = @ptrCast(@alignCast(ctx));
    var ids: [membership.MAX_MEMBERS]u32 = undefined;
    const members = membership.decode(entry.payload, &ids) orelse return;
    raft.commitMembership(members);
    log.info("Raft: membership committed: {any} (config index {d})", .{ members, entry.header.index });
}

pub fn raftRingCapacity(hot_buffer_capacity: usize) usize {
    return @max(RAFT_RING_MIN, @min(hot_buffer_capacity / 4, RAFT_RING_MAX));
}

/// The one apply path: the partition (hot ring plus the projections the
/// router owns — KV, queue, TS) and then the registry appliers for every
/// other entry type. Returns false when the entry could not enter the ring,
/// in which case nothing else sees it either.
pub fn applyEntryCore(partition: *Partition, registry: *const ReplayRegistry, entry: *const entry_mod.Entry) bool {
    _ = partition.apply(entry) catch |err| {
        log.err("shard {d}: entry index={d} type={s} rejected by the partition: {s}; projections are missing it", .{ partition.id, entry.header.index, @tagName(@as(entry_mod.EntryType, @enumFromInt(entry.header.entry_type))), @errorName(err) });
        return false;
    };
    _ = registry.dispatch(entry);
    return true;
}

/// Every entry type has exactly one applier: the projection router or a
/// registry callback, never both. Two appliers is how a replayed entry gets
/// inserted twice.
fn assertOneApplier(shard_id: u16, registry: *const ReplayRegistry) !void {
    inline for (@typeInfo(entry_mod.EntryType).@"enum".fields) |f| {
        const etype: entry_mod.EntryType = @enumFromInt(f.value);
        const routed = switch (projection_router.routeTarget(etype)) {
            .kv, .queue, .ts => true,
            .none, .snapshot => false,
        };
        if (routed and registry.has(etype)) {
            log.err("shard {d}: entry type {s} has two appliers (the projection router and a registry callback); this is a bug in this build, not in the data directory — refusing to start", .{ shard_id, f.name });
            return error.EntryTypeHasTwoAppliers;
        }
    }
}

/// Feed a durable entry back into the Raft log at boot: the last index, the
/// term index and the hot-ring tail all come from this pass. A hole in the
/// durable history (an index no segment holds) is logged and the log
/// restarts after it as if the prefix were compacted — nothing this node
/// holds is lost, but no follower can be repaired from below the hole.
fn restoreRaftEntry(shard_id: u16, raft_log: *RaftLog, e: *const entry_mod.Entry) void {
    _ = raft_log.append(e) catch |err| switch (err) {
        error.IndexGap => {
            const tip = raft_log.lastIndex();
            if (e.header.index <= tip) {
                log.err("shard {d}: segments hold index {d} twice (log tip {d}); the later copy is ignored", .{ shard_id, e.header.index, tip });
                return;
            }
            log.warn("shard {d}: no segment holds indices {d}..{d}; treating the prefix as compacted (recurs on every boot of this data dir)", .{ shard_id, tip + 1, e.header.index - 1 });
            raft_log.resetToSnapshot(e.header.index - 1, raft_log.lastTerm());
            _ = raft_log.append(e) catch |again| {
                log.err("shard {d}: cannot restore index {d}: {s}", .{ shard_id, e.header.index, @errorName(again) });
            };
        },
        else => log.err("shard {d}: cannot restore index {d}: {s}", .{ shard_id, e.header.index, @errorName(err) }),
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Segment Replay — recover state from .flseg files on startup
// ═══════════════════════════════════════════════════════════════════════════════

/// Replay all .flseg segment files in `dir_path`: every entry is restored
/// into the Raft log and applied through `applyEntryCore`, exactly as when
/// it was first committed.
///
/// If `replay_from` > 0, entries with index <= replay_from are skipped
/// (already restored from a snapshot). Entries above `watermark` are
/// restored into the Raft log but not applied.
fn replaySegments(
    allocator: std.mem.Allocator,
    durable_log: *DurableLog,
    partition: *Partition,
    replay_registry: *const ReplayRegistry,
    replay_from: u64,
    watermark: u64,
    raft_log: *RaftLog,
    shard_id: u16,
) !void {
    // Listed in Raft index order so the projection router's applied_index
    // guard does not skip lower-index entries.
    const files = try durable_log.segments();
    for (files) |sf| {
        replaySegmentFile(allocator, sf.path, partition, replay_registry, replay_from, watermark, raft_log, shard_id);
    }
}

fn replaySegmentFile(
    allocator: std.mem.Allocator,
    full_path: []const u8,
    partition: *Partition,
    replay_registry: *const ReplayRegistry,
    replay_from: u64,
    watermark: u64,
    raft_log: *RaftLog,
    shard_id: u16,
) void {
    const result = SegmentReader.initFromFile(allocator, full_path) catch return;
    defer allocator.free(result.buf);

    var offset: usize = 0;
    const data_len = result.reader.data_end - result.reader.data_start;
    while (offset < data_len) {
        const seg_entry = result.reader.readEntryAt(offset) orelse break;
        offset += seg_entry.totalSize();

        // Every durable entry is part of the Raft log, snapshot or not.
        restoreRaftEntry(shard_id, raft_log, &seg_entry);

        // Skip entries already covered by snapshot
        if (replay_from > 0 and seg_entry.header.index <= replay_from) continue;
        // Above the watermark: in the log, not yet in the projections.
        if (seg_entry.header.index > watermark) continue;

        _ = applyEntryCore(partition, replay_registry, &seg_entry);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "Shard: init and deinit" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();

    try std.testing.expectEqual(@as(u16, 0), shard.id);
    try std.testing.expectEqual(@as(u32, 0), shard.connectionCount());
    try std.testing.expect(!shard.running);
}

test "Shard: add and remove connections" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();

    // Create test pipes to use as fake connection fds
    const conn_pipe = try @import("stdx").io.pipe();
    defer _ = std.c.close(conn_pipe[0]);
    defer _ = std.c.close(conn_pipe[1]);

    const conn = try shard.addConnection(conn_pipe[0]);
    try std.testing.expectEqual(@as(u32, 1), shard.connectionCount());
    try std.testing.expectEqual(@as(u32, 1), conn.id);

    shard.removeConnection(conn_pipe[0]);
    try std.testing.expectEqual(@as(u32, 0), shard.connectionCount());
}

test "Shard: dispatch ping via pipe-based connection" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
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
    const conn_pipe = try @import("stdx").io.pipe();
    // Note: conn_pipe[0] is owned by shard (closed in deinit), only close write end
    defer _ = std.c.close(conn_pipe[1]);

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
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
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

test "Shard: a first member leads a group of itself from the log; a joiner waits to be named" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var first = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 7, .bootstrap, .{});
    defer first.deinit();
    try std.testing.expectEqual(raft_node_mod.Role.leader, first.raft_node.role);
    // The term's noop, then the config naming this node; both committed.
    try std.testing.expectEqual(@as(u64, 2), first.raft_node.commit_index);
    try std.testing.expectEqual(@as(u64, 2), first.raft_node.log.last_config_index);
    try std.testing.expect(first.applyCommitted());
    var ids: [membership.MAX_MEMBERS]u32 = undefined;
    try std.testing.expectEqualSlices(u32, &.{7}, first.raft_node.memberIds(&ids));
    try std.testing.expectEqual(@as(u8, 1), first.raft_node.committed_member_count);
    try std.testing.expectEqual(@as(u32, 7), first.raft_node.committed_member_ids[0]);

    var joiner = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 8, .join, .{});
    defer joiner.deinit();
    try std.testing.expectEqual(raft_node_mod.Role.follower, joiner.raft_node.role);
    try std.testing.expect(!joiner.raft_node.timer_enabled);
    try std.testing.expectEqual(@as(u64, 0), joiner.raft_node.commit_index);
    try std.testing.expectEqual(@as(usize, 0), joiner.raft_node.memberIds(&ids).len);
}

test "Shard: a join request adds the peer, one change at a time" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    try std.testing.expect(shard.applyCommitted());
    const raft = shard.raft_node;
    var ids: [membership.MAX_MEMBERS]u32 = undefined;

    shard.handleJoinRequest(2);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2 }, raft.memberIds(&ids));
    try std.testing.expectEqual(@as(u64, 3), raft.membership_index);
    // With a peer, nothing commits without its ack; a second change waits.
    try std.testing.expect(raft.commit_index < 3);
    shard.handleJoinRequest(3);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2 }, raft.memberIds(&ids));
    // Node 2 acks everything sent: the config commits and applies.
    raft.peers[0].sent_up_to = 3;
    raft.handleAppendResponse(.{ .term = raft.current_term, .success = true, .match_index = 3, .from = 2 });
    try std.testing.expect(shard.applyCommitted());
    try std.testing.expectEqual(@as(u64, 3), raft.commit_index);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2 }, raft.committed_member_ids[0..raft.committed_member_count]);
    shard.handleJoinRequest(3);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, raft.memberIds(&ids));
    // A member asking again changes nothing.
    shard.handleJoinRequest(2);
    try std.testing.expectEqual(@as(u64, 4), raft.membership_index);
}

test "Shard: a write on a node that does not lead waits for a leader, then is answered unavailable" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 8, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 8, .join, .{});
    defer shard.deinit();
    shard.raft_network = &rn;

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);

    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.op_code = @intFromEnum(proto.OpCode.kv_put);
    header.request_id = 5;
    // Namespace, key, value and options, each length-prefixed, as the
    // wire carries them: the held copy is rebuilt from this length.
    header.payload_length = 2 + 2 + 1 + 4 + 1 + 2;
    shard.dispatchRequest(conn, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    // Held for the leader, not answered.
    try std.testing.expectEqual(@as(u32, 1), shard.forward_count);
    try std.testing.expect(conn.response_deferred);
    try std.testing.expectEqual(@as(usize, 0), conn.write_buf.readable());

    shard.sweepForwards(Shard.nowMs() + FORWARD_TIMEOUT_MS + 1);
    try std.testing.expectEqual(@as(u32, 0), shard.forward_count);
    var out: [256]u8 = undefined;
    const n = std.c.read(pair[1], &out, out.len);
    try std.testing.expect(n > 0);
    const resp = try proto.Response.parse(out[0..@intCast(n)]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.unavailable), resp.header.status);
    try std.testing.expectEqual(@as(u64, 5), resp.header.request_id);
    try std.testing.expect(std.mem.indexOf(u8, resp.data, "electing a leader") != null);
}

test "Shard: a frame from a node the membership does not name is dropped, a join request is not, and the ids inside must be the sender's" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    try std.testing.expect(shard.applyCommitted());
    const raft = shard.raft_node;
    const term0 = raft.current_term;
    var ids: [membership.MAX_MEMBERS]u32 = undefined;

    // A node with the secret but no seat claims to lead at a higher term.
    var buf: [transport.APPEND_REQ_PREFIX + 64]u8 = undefined;
    const n = transport.serializeAppendRequest(.{ .term = term0 + 5, .leader_id = 9, .prev_log_index = 0, .prev_log_term = 0, .leader_commit = 0, .entries = &.{} }, &buf).?;
    shard.handleRaftFrame(.{ .source_node = 9, .group_id = 0, .msg_type = .append_entries, .payload = buf[0..n] });
    try std.testing.expectEqual(term0, raft.current_term);
    try std.testing.expectEqual(raft_node_mod.Role.leader, raft.role);
    // Its request to join is heard.
    shard.handleRaftFrame(.{ .source_node = 9, .group_id = 0, .msg_type = .join_request, .payload = &.{} });
    try std.testing.expectEqualSlices(u32, &.{ 1, 9 }, raft.memberIds(&ids));
    // A member speaking for another node is dropped; speaking for itself
    // it is heard.
    var buf2: [transport.APPEND_REQ_PREFIX + 64]u8 = undefined;
    const n2 = transport.serializeAppendRequest(.{ .term = term0 + 5, .leader_id = 7, .prev_log_index = 0, .prev_log_term = 0, .leader_commit = 0, .entries = &.{} }, &buf2).?;
    shard.handleRaftFrame(.{ .source_node = 9, .group_id = 0, .msg_type = .append_entries, .payload = buf2[0..n2] });
    try std.testing.expectEqual(term0, raft.current_term);
    shard.handleRaftFrame(.{ .source_node = 9, .group_id = 0, .msg_type = .append_entries, .payload = buf[0..n] });
    try std.testing.expectEqual(term0 + 5, raft.current_term);
    try std.testing.expectEqual(raft_node_mod.Role.follower, raft.role);
}

fn readAnswer(fd: std.posix.fd_t, shard: *Shard, conn_fd: i32, out: []u8, want: usize) !usize {
    var got: usize = 0;
    var tries: usize = 0;
    while (got < want and tries < 10_000) : (tries += 1) {
        shard.flushToClient(conn_fd);
        const n = std.c.read(fd, out[got..].ptr, out.len - got);
        if (n > 0) got += @intCast(n);
    }
    return got;
}

test "Shard: a forwarded request that cannot be parsed is answered, and a large answer arrives whole" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    try @import("stdx").net.sysFcntlSetNonblocking(pair[1]);
    // As an accepted socket is: a full kernel buffer must not block the flush.
    try @import("stdx").net.sysFcntlSetNonblocking(pair[0]);
    const conn = try shard.addConnection(pair[0]);
    const seq: u64 = (@as(u64, conn.id) << 32) | @as(u32, @bitCast(conn.fd));

    // Junk where a request should be, with a readable request id.
    const junk = try std.testing.allocator.alloc(u8, 40);
    @memset(junk, 0xee);
    std.mem.writeInt(u64, junk[8..16], 77, .little);
    shard.runForwardedRequest(.{ .tag = .forward_request, .src_shard = 0, .payload_len = @intCast(junk.len), .sequence = seq, .payload_ptr = junk.ptr });
    var out: [512]u8 = undefined;
    const n = try readAnswer(pair[1], &shard, conn.fd, &out, @sizeOf(proto.ResponseHeader));
    const resp = try proto.Response.parse(out[0..n]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.internal_error), resp.header.status);
    try std.testing.expectEqual(@as(u64, 77), resp.header.request_id);

    // A 200 KB value read through the forward path comes back whole.
    const value = try std.testing.allocator.alloc(u8, 200 * 1024);
    defer std.testing.allocator.free(value);
    for (value, 0..) |*b, i| b.* = @truncate(i *% 7);
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(proto.OpCode.kv_put);
    header.request_id = 1;
    header.payload_length = @intCast(2 + 2 + 3 + 4 + value.len + 2);
    const put = try Shard.serializeRequest(std.testing.allocator, .{ .header = header, .namespace = "", .key = "big", .value = value });
    defer std.testing.allocator.free(put);
    std.mem.bytesAsValue(proto.RequestHeader, put[0..@sizeOf(proto.RequestHeader)]).crc32 = header.computeCRC32(put[@sizeOf(proto.RequestHeader)..]);
    shard.dispatchRequest(conn, try proto.Request.parse(put));
    _ = shard.applyCommitted();
    var drain_buf: [4096]u8 = undefined;
    _ = try readAnswer(pair[1], &shard, conn.fd, &drain_buf, 1);

    header.op_code = @intFromEnum(proto.OpCode.kv_get);
    header.request_id = 2;
    header.payload_length = 2 + 2 + 3 + 4 + 2;
    const get = try Shard.serializeRequest(std.testing.allocator, .{ .header = header, .namespace = "", .key = "big", .value = "" });
    std.mem.bytesAsValue(proto.RequestHeader, get[0..@sizeOf(proto.RequestHeader)]).crc32 = header.computeCRC32(get[@sizeOf(proto.RequestHeader)..]);
    shard.runForwardedRequest(.{ .tag = .forward_request, .src_shard = 0, .payload_len = @intCast(get.len), .sequence = seq, .payload_ptr = get.ptr });
    const big_out = try std.testing.allocator.alloc(u8, 300 * 1024);
    defer std.testing.allocator.free(big_out);
    const got = try readAnswer(pair[1], &shard, conn.fd, big_out, @sizeOf(proto.ResponseHeader) + value.len);
    const answer = try proto.Response.parse(big_out[0..got]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), answer.header.status);
    try std.testing.expect(std.mem.indexOf(u8, answer.data, value) != null);
}

test "Shard: a KV watch that times out answers the value still there, and a wait on a missing key answers not found" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);
    const conn = c.conn;
    const pair = c.pair;

    const put = try testRequest(.kv_put, 1, "w", "v1");
    defer std.testing.allocator.free(put);
    shard.dispatchRequest(conn, try proto.Request.parse(put));
    _ = shard.applyCommitted();
    var out: [512]u8 = undefined;
    _ = try readAnswer(pair[1], &shard, conn.fd, &out, @sizeOf(proto.ResponseHeader));

    var qbuf: [handler_mod.MAX_QUALIFIED_KEY]u8 = undefined;
    const present = try handler_mod.qualifyKey(&qbuf, "", "w");
    const version = shard.defaultPartition().kv.get(present).?.version;
    try std.testing.expect(shard.waiter_pool.register(.{ .kind = .kv_get, .fd = conn.fd, .owner_shard = 0, .conn_id = conn.id, .request_id = 7, .key = present, .min_version = version, .timeout_ms = 1 }));
    shard.waiter_pool.waiters[0].expires_at_ms = 0;
    shard.waiter_pool.expireTimeouts(handleWaiterTimeout, &shard);
    var n = try readAnswer(pair[1], &shard, conn.fd, &out, @sizeOf(proto.ResponseHeader) + 8 + 2);
    var resp = try proto.Response.parse(out[0..n]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), resp.header.status);
    try std.testing.expectEqual(@as(u64, 7), resp.header.request_id);
    try std.testing.expect(std.mem.endsWith(u8, resp.data, "v1"));
    try std.testing.expectEqual(version, std.mem.readInt(u64, resp.data[0..8], .little));

    var mbuf: [handler_mod.MAX_QUALIFIED_KEY]u8 = undefined;
    const missing = try handler_mod.qualifyKey(&mbuf, "", "nope");
    try std.testing.expect(shard.waiter_pool.register(.{ .kind = .kv_get, .fd = conn.fd, .owner_shard = 0, .conn_id = conn.id, .request_id = 8, .key = missing, .min_version = 0, .timeout_ms = 1 }));
    shard.waiter_pool.waiters[0].expires_at_ms = 0;
    shard.waiter_pool.expireTimeouts(handleWaiterTimeout, &shard);
    n = try readAnswer(pair[1], &shard, conn.fd, &out, @sizeOf(proto.ResponseHeader));
    resp = try proto.Response.parse(out[0..n]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.not_found), resp.header.status);
    try std.testing.expectEqual(@as(u64, 8), resp.header.request_id);
}

test "Shard: a wait of 0 answers at once with no waiter, and a wait over 5 minutes is refused rather than shortened" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);

    const Case = struct { op: proto.OpCode, tag: proto.OptionTag, ms: u32, key: []const u8, value: []const u8, status: proto.StatusCode, waits: bool };
    const await_value = [_]u8{ 1, 0, 0, 0, 1, 0, 'a' };
    const cases = [_]Case{
        .{ .op = .queue_dequeue, .tag = .block_ms, .ms = 0, .key = "q", .value = "", .status = .ok, .waits = false },
        .{ .op = .kv_get, .tag = .block_ms, .ms = 0, .key = "missing", .value = "", .status = .not_found, .waits = false },
        .{ .op = .kv_get, .tag = .wait_ms, .ms = 0, .key = "missing", .value = "", .status = .not_found, .waits = false },
        .{ .op = .action_await, .tag = .block_ms, .ms = 0, .key = "", .value = &await_value, .status = .ok, .waits = false },
        .{ .op = .kv_get, .tag = .block_ms, .ms = waiter_pool_mod.MAX_BLOCK_MS, .key = "missing", .value = "", .status = .ok, .waits = true },
        .{ .op = .kv_get, .tag = .block_ms, .ms = waiter_pool_mod.MAX_BLOCK_MS + 1, .key = "missing", .value = "", .status = .bad_request, .waits = false },
        .{ .op = .kv_get, .tag = .wait_ms, .ms = waiter_pool_mod.MAX_BLOCK_MS + 1, .key = "missing", .value = "", .status = .bad_request, .waits = false },
    };
    for (cases, 0..) |case, i| {
        var opt_buf: [16]u8 = undefined;
        var opts = proto.OptionsBuilder.init(&opt_buf);
        try opts.addU32(case.tag, case.ms);
        const bytes = try testRequestWith(case.op, 30 + i, case.key, case.value, opt_buf[0..opts.offset]);
        defer std.testing.allocator.free(bytes);
        const active = shard.waiter_pool.totalActive();
        shard.dispatchRequest(c.conn, try proto.Request.parse(bytes));
        if (case.waits) {
            try std.testing.expectEqual(active + 1, shard.waiter_pool.totalActive());
            continue;
        }
        try std.testing.expectEqual(active, shard.waiter_pool.totalActive());
        var out: [512]u8 = undefined;
        const n = try readAnswer(c.pair[1], &shard, c.conn.fd, &out, @sizeOf(proto.ResponseHeader));
        const resp = try proto.Response.parse(out[0..n]);
        try std.testing.expectEqual(@as(u64, 30 + i), resp.header.request_id);
        try std.testing.expectEqual(@intFromEnum(case.status), resp.header.status);
    }
}

test "Shard: a proxy answer comes out whole; one the proxy refused comes out as an error, and the next is unaffected" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    const proxy = shard.forward_proxy;
    var err_buf: [256]u8 = undefined;
    try std.testing.expect(shard.takeProxyAnswer(proxy, 1, &err_buf) == null);

    // Past the proxy's cap: an error naming the request, and nothing left behind.
    const huge = try std.testing.allocator.alloc(u8, Connection.MAX_WRITE_BUFFER - 1);
    defer std.testing.allocator.free(huge);
    @memset(huge, 'h');
    _ = proxy.queueWrite(huge[0..16]);
    try std.testing.expectEqual(@as(usize, 0), proxy.queueWrite(huge));
    const refused = shard.takeProxyAnswer(proxy, 2, &err_buf).?;
    defer refused.free(std.testing.allocator);
    const resp = try proto.Response.parse(refused.bytes());
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.internal_error), resp.header.status);
    try std.testing.expectEqual(@as(u64, 2), resp.header.request_id);
    try std.testing.expect(!proxy.write_overflow);
    try std.testing.expectEqual(@as(usize, 0), proxy.write_buf.readable());

    // Larger than the proxy's first buffer: whole.
    try std.testing.expectEqual(huge.len, proxy.queueWrite(huge));
    const whole = shard.takeProxyAnswer(proxy, 3, &err_buf).?;
    defer whole.free(std.testing.allocator);
    try std.testing.expectEqualSlices(u8, huge, whole.bytes());
}

/// A client request as it arrives on the wire, CRC set; the caller frees it.
fn testRequest(op: proto.OpCode, request_id: u64, key: []const u8, value: []const u8) ![]u8 {
    return testRequestWith(op, request_id, key, value, "");
}

fn testRequestWith(op: proto.OpCode, request_id: u64, key: []const u8, value: []const u8, options: []const u8) ![]u8 {
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(op);
    header.request_id = request_id;
    header.payload_length = @intCast(2 + 2 + key.len + 4 + value.len + 2 + options.len);
    const bytes = try Shard.serializeRequest(std.testing.allocator, .{ .header = header, .namespace = "", .key = key, .value = value, .options = options });
    std.mem.bytesAsValue(proto.RequestHeader, bytes[0..@sizeOf(proto.RequestHeader)]).crc32 = header.computeCRC32(bytes[@sizeOf(proto.RequestHeader)..]);
    return bytes;
}

/// A shard alone and a connected client socket pair, both ends non-blocking
/// as an accepted socket is.
const TestClient = struct {
    pair: [2]std.posix.fd_t,
    conn: *Connection,

    fn open(shard: *Shard) !TestClient {
        var pair: [2]std.posix.fd_t = undefined;
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
        try @import("stdx").net.sysFcntlSetNonblocking(pair[1]);
        try @import("stdx").net.sysFcntlSetNonblocking(pair[0]);
        return .{ .pair = pair, .conn = try shard.addConnection(pair[0]) };
    }
};

test "Shard: a client that sends without reading is paced: its requests wait until its answers drain, then run in order" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);
    // A small kernel buffer, so unsent answers stay with the shard.
    const small: c_int = 4096;
    _ = std.c.setsockopt(c.pair[0], std.posix.SOL.SOCKET, std.posix.SO.SNDBUF, @ptrCast(&small), @sizeOf(c_int));

    const value = try std.testing.allocator.alloc(u8, 2 * Shard.PAUSE_READS_AT);
    defer std.testing.allocator.free(value);
    @memset(value, 'v');
    const put = try testRequest(.kv_put, 1, "k", value);
    defer std.testing.allocator.free(put);
    try std.testing.expectEqual(put.len, feedClient(c.pair[1], &shard, c.conn.fd, put));
    var drain: [4096]u8 = undefined;
    _ = try readAnswer(c.pair[1], &shard, c.conn.fd, &drain, @sizeOf(proto.ResponseHeader));

    // Three reads of it in one write: the first answer alone passes the
    // pause mark, so the other two wait unread.
    var gets: [3][]u8 = undefined;
    for (&gets, 0..) |*g, i| g.* = try testRequest(.kv_get, 11 + i, "k", "");
    defer for (gets) |g| std.testing.allocator.free(g);
    const all = try std.mem.concat(std.testing.allocator, u8, &gets);
    defer std.testing.allocator.free(all);
    try std.testing.expectEqual(@as(isize, @intCast(all.len)), std.c.write(c.pair[1], all.ptr, all.len));
    shard.readFromClient(c.conn.fd);
    try std.testing.expect(c.conn.reads_paused);
    try std.testing.expectEqual(gets[1].len + gets[2].len, c.conn.read_buf.readable());

    // Read on the client side: each drained answer lets the next request run.
    const answer_len = @sizeOf(proto.ResponseHeader) + 8 + value.len;
    const out = try std.testing.allocator.alloc(u8, 3 * answer_len);
    defer std.testing.allocator.free(out);
    var got: usize = 0;
    var tries: usize = 0;
    while (got < out.len and tries < 100_000) : (tries += 1) {
        shard.flushToClient(c.conn.fd);
        shard.settleConnections();
        const n = std.c.read(c.pair[1], out[got..].ptr, out.len - got);
        if (n > 0) got += @intCast(n);
    }
    try std.testing.expectEqual(out.len, got);
    for (0..3) |i| {
        const resp = try proto.Response.parse(out[i * answer_len ..][0..answer_len]);
        try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), resp.header.status);
        try std.testing.expectEqual(@as(u64, 11 + i), resp.header.request_id);
    }
    try std.testing.expect(!c.conn.reads_paused);

    // The same over RESP.
    const r = try TestClient.open(&shard);
    defer _ = std.c.close(r.pair[1]);
    _ = std.c.setsockopt(r.pair[0], std.posix.SOL.SOCKET, std.posix.SO.SNDBUF, @ptrCast(&small), @sizeOf(c_int));
    const get = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
    const three = get ** 3;
    try std.testing.expectEqual(@as(isize, three.len), std.c.write(r.pair[1], three, three.len));
    shard.readFromClient(r.conn.fd);
    try std.testing.expect(r.conn.reads_paused);
    try std.testing.expectEqual(2 * get.len, r.conn.read_buf.readable());
    const bulk_len = std.fmt.comptimePrint("${d}\r\n", .{2 * Shard.PAUSE_READS_AT}).len + value.len + 2;
    const rout = try std.testing.allocator.alloc(u8, 3 * bulk_len);
    defer std.testing.allocator.free(rout);
    got = 0;
    tries = 0;
    while (got < rout.len and tries < 100_000) : (tries += 1) {
        shard.flushToClient(r.conn.fd);
        shard.settleConnections();
        const n = std.c.read(r.pair[1], rout[got..].ptr, rout.len - got);
        if (n > 0) got += @intCast(n);
    }
    try std.testing.expectEqual(rout.len, got);
    try std.testing.expect(!r.conn.reads_paused);
}

test "Shard: a client that sends its whole pipeline before reading is unpaced once it stalls, and gets every answer; one that reads stays paced" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);
    const small: c_int = 4096;
    _ = std.c.setsockopt(c.pair[0], std.posix.SOL.SOCKET, std.posix.SO.SNDBUF, @ptrCast(&small), @sizeOf(c_int));
    _ = std.c.setsockopt(c.pair[1], std.posix.SOL.SOCKET, std.posix.SO.SNDBUF, @ptrCast(&small), @sizeOf(c_int));

    const value = [_]u8{'v'} ** 1024;
    const put = try testRequest(.kv_put, 1, "k", &value);
    defer std.testing.allocator.free(put);
    try std.testing.expectEqual(put.len, feedClient(c.pair[1], &shard, c.conn.fd, put));
    var drain: [4096]u8 = undefined;
    _ = try readAnswer(c.pair[1], &shard, c.conn.fd, &drain, @sizeOf(proto.ResponseHeader));

    // A thousand reads, all sent before any answer is read: their bytes
    // are more than the kernel buffers hold, so the client cannot finish
    // sending while paced; their answers (about 1 MiB) fit the cap.
    const count = 1000;
    var gets: [count][]u8 = undefined;
    for (&gets, 0..) |*g, i| g.* = try testRequest(.kv_get, 100 + i, "k", "");
    defer for (gets) |g| std.testing.allocator.free(g);
    const all = try std.mem.concat(std.testing.allocator, u8, &gets);
    defer std.testing.allocator.free(all);
    var sent: usize = 0;
    var tries: usize = 0;
    while (sent < all.len and tries < 10_000) : (tries += 1) {
        const n = std.c.write(c.pair[1], all[sent..].ptr, all.len - sent);
        if (n > 0) sent += @intCast(n);
        shard.readFromClient(c.conn.fd);
        shard.flushToClient(c.conn.fd);
        shard.settleConnections();
        // Time passes with the client reading nothing.
        if (c.conn.reads_paused) {
            c.conn.paused_progress_ms -|= Shard.STALL_MS;
            shard.stall_check_ms = 0;
        }
    }
    try std.testing.expectEqual(all.len, sent);
    // Unpaced until it drains: not paused again with its answers unread.
    shard.readFromClient(c.conn.fd);
    shard.settleConnections();
    try std.testing.expect(c.conn.pacing_off);
    try std.testing.expect(!c.conn.reads_paused);
    try std.testing.expect(c.conn.write_buf.readable() > Shard.PAUSE_READS_AT);

    const answer_len = @sizeOf(proto.ResponseHeader) + 8 + value.len;
    const out = try std.testing.allocator.alloc(u8, count * answer_len);
    defer std.testing.allocator.free(out);
    var got: usize = 0;
    tries = 0;
    while (got < out.len and tries < 100_000) : (tries += 1) {
        shard.readFromClient(c.conn.fd);
        shard.flushToClient(c.conn.fd);
        shard.settleConnections();
        const n = std.c.read(c.pair[1], out[got..].ptr, out.len - got);
        if (n > 0) got += @intCast(n);
    }
    try std.testing.expectEqual(out.len, got);
    for (0..count) |i| {
        const resp = try proto.Response.parse(out[i * answer_len ..][0..answer_len]);
        try std.testing.expectEqual(@as(u64, 100 + i), resp.header.request_id);
    }
    try std.testing.expect(!c.conn.pacing_off);
    try std.testing.expect(!c.conn.closing);

    // A paused client that reads something is not unpaced; while paused,
    // nothing more of what it sends is taken.
    const again = try std.mem.concat(std.testing.allocator, u8, gets[0..90]);
    defer std.testing.allocator.free(again);
    try std.testing.expectEqual(@as(isize, @intCast(again.len)), std.c.write(c.pair[1], again.ptr, again.len));
    shard.readFromClient(c.conn.fd);
    try std.testing.expect(c.conn.reads_paused);
    const held = c.conn.read_buf.readable();
    try std.testing.expectEqual(@as(isize, 40), std.c.write(c.pair[1], all.ptr, 40));
    shard.readFromClient(c.conn.fd);
    try std.testing.expectEqual(held, c.conn.read_buf.readable());
    c.conn.paused_progress_ms -|= Shard.STALL_MS;
    try std.testing.expect(std.c.read(c.pair[1], &drain, drain.len) > 0);
    shard.flushToClient(c.conn.fd);
    shard.stall_check_ms = 0;
    shard.settleConnections();
    try std.testing.expect(c.conn.reads_paused);
}

test "Shard: a connection marked closing runs no more of its requests and is closed at the end of the tick" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);
    const fd = c.conn.fd;

    // The first answer finds the connection over its cap, so the flush
    // after it marks the connection closing; the second request must not run.
    c.conn.write_overflow = true;
    var pings: [2][]u8 = undefined;
    for (&pings, 0..) |*p, i| p.* = try testRequest(.kv_get, 21 + i, "k", "");
    defer for (pings) |p| std.testing.allocator.free(p);
    const both = try std.mem.concat(std.testing.allocator, u8, &pings);
    defer std.testing.allocator.free(both);
    try std.testing.expectEqual(@as(isize, @intCast(both.len)), std.c.write(c.pair[1], both.ptr, both.len));
    shard.readFromClient(fd);
    try std.testing.expectEqual(@as(u64, 1), shard.requests_dispatched);
    // Still open for whoever up the stack holds it; gone once settled.
    try std.testing.expect(shard.getConnection(fd) != null);
    shard.settleConnections();
    try std.testing.expect(shard.getConnection(fd) == null);

    // The same over RESP: the second command stays unread.
    const r = try TestClient.open(&shard);
    defer _ = std.c.close(r.pair[1]);
    const rfd = r.conn.fd;
    const ping = "*1\r\n$4\r\nPING\r\n";
    r.conn.write_overflow = true;
    const two = ping ++ ping;
    try std.testing.expectEqual(@as(isize, two.len), std.c.write(r.pair[1], two, two.len));
    shard.readFromClient(rfd);
    try std.testing.expectEqual(ping.len, r.conn.read_buf.readable());
    try std.testing.expect(shard.getConnection(rfd) != null);
    shard.settleConnections();
    try std.testing.expect(shard.getConnection(rfd) == null);
}

/// Feed `data` to the shard through the client's socket end, reading on the
/// shard's side as the kernel buffer fills, then until the shard has taken
/// everything written (a kernel may still hold the tail after the last
/// write returns). Returns how much was written.
fn feedClient(client_fd: std.posix.fd_t, shard: *Shard, conn_fd: i32, data: []const u8) usize {
    var sent: usize = 0;
    var tries: usize = 0;
    while (tries < 100_000) : (tries += 1) {
        if (sent < data.len) {
            const n = std.c.write(client_fd, data[sent..].ptr, data.len - sent);
            if (n > 0) sent += @intCast(n);
        }
        shard.readFromClient(conn_fd);
        const conn = shard.getConnection(conn_fd) orelse break;
        if (conn.closing or conn.reads_paused) break;
        if (sent == data.len and unreadBytes(conn_fd) == 0) break;
    }
    return sent;
}

/// Bytes the kernel holds for `fd` that nobody has read yet.
fn unreadBytes(fd: std.posix.fd_t) c_int {
    // std's Darwin table has no FIONREAD; it is _IOR('f', 127, int) there.
    const fionread: c_int = if (@import("builtin").os.tag == .linux) @intCast(std.os.linux.T.FIONREAD) else 0x4004667f;
    var n: c_int = 0;
    _ = std.c.ioctl(fd, fionread, &n);
    return n;
}

test "Shard: a request larger than the read buffer is read whole; one that cannot fit is refused with its own id, and the connection closed" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);
    const conn = c.conn;
    const pair = c.pair;

    // Twice the default buffer: stored whole, so every byte arrived in order.
    const value = try std.testing.allocator.alloc(u8, 2 * RingBuffer.DEFAULT_CAPACITY);
    defer std.testing.allocator.free(value);
    @memset(value, 'v');
    const put = try testRequest(.kv_put, 4, "k", value);
    defer std.testing.allocator.free(put);
    try std.testing.expectEqual(put.len, feedClient(pair[1], &shard, conn.fd, put));
    var out: [512]u8 = undefined;
    var n = try readAnswer(pair[1], &shard, conn.fd, &out, @sizeOf(proto.ResponseHeader));
    var resp = try proto.Response.parse(out[0..n]);
    try std.testing.expectEqual(@as(u64, 4), resp.header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), resp.header.status);
    var qbuf: [handler_mod.MAX_QUALIFIED_KEY]u8 = undefined;
    const stored = shard.defaultPartition().kv.get(try handler_mod.qualifyKey(&qbuf, "", "k")).?;
    try std.testing.expectEqualSlices(u8, value, stored.value);
    try std.testing.expect(!conn.closing);
    // Grown for it, given back once it is taken.
    try std.testing.expectEqual(RingBuffer.DEFAULT_CAPACITY, conn.read_buf.buf.len);

    // One byte over: refused from its header alone, answered under its
    // own id, before the client has sent the rest.
    var header = std.mem.bytesToValue(proto.RequestHeader, put[0..@sizeOf(proto.RequestHeader)]);
    header.request_id = 5;
    header.payload_length = MAX_REQUEST_SIZE - @sizeOf(proto.RequestHeader) + 1;
    try std.testing.expectEqual(@sizeOf(proto.RequestHeader), feedClient(pair[1], &shard, conn.fd, std.mem.asBytes(&header)));
    try std.testing.expect(conn.closing);
    n = try readAnswer(pair[1], &shard, conn.fd, &out, @sizeOf(proto.ResponseHeader));
    resp = try proto.Response.parse(out[0..n]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), resp.header.status);
    try std.testing.expectEqual(@as(u64, 5), resp.header.request_id);
    try std.testing.expectEqualStrings("bad request: request over 256 KiB", resp.data);

    // A RESP command cannot say its size up front: refused in RESP once
    // the buffer is full at its cap.
    const r = try TestClient.open(&shard);
    defer _ = std.c.close(r.pair[1]);
    const big = try std.testing.allocator.alloc(u8, MAX_READ_BUFFER + 64);
    defer std.testing.allocator.free(big);
    @memset(big, 'x');
    const head = "*2\r\n$3\r\nGET\r\n$600000\r\n";
    @memcpy(big[0..head.len], head);
    _ = feedClient(r.pair[1], &shard, r.conn.fd, big);
    try std.testing.expect(r.conn.closing);
    var rout: [64]u8 = undefined;
    const rn = std.c.read(r.pair[1], &rout, rout.len);
    try std.testing.expectEqualStrings("-ERR request over 256 KiB\r\n", rout[0..@intCast(rn)]);

    // Not a request at all, whatever its length field says: invalid, not
    // too large.
    const j = try TestClient.open(&shard);
    defer _ = std.c.close(j.pair[1]);
    var junk: [@sizeOf(proto.RequestHeader)]u8 = [_]u8{0xff} ** @sizeOf(proto.RequestHeader);
    try std.testing.expectEqual(@as(isize, junk.len), std.c.write(j.pair[1], &junk, junk.len));
    shard.readFromClient(j.conn.fd);
    try std.testing.expect(j.conn.closing);
    n = try readAnswer(j.pair[1], &shard, j.conn.fd, &out, @sizeOf(proto.ResponseHeader));
    resp = try proto.Response.parse(out[0..n]);
    try std.testing.expect(std.mem.indexOf(u8, resp.data, "256 KiB") == null);
}

test "Shard: requests resumed at the end of a tick run, and keep the next poll from waiting on a leader only" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);
    const ping = try testRequest(.ping, 7, "", "");
    defer std.testing.allocator.free(ping);

    // A paused connection holding one request, resumed: it runs at the
    // end of the tick.
    const role = shard.raft_node.role;
    defer shard.raft_node.role = role;
    for ([_]bool{ true, false }) |leads| {
        shard.raft_node.role = if (leads) .leader else .follower;
        shard.more_work = false;
        const before = shard.requests_dispatched;
        shard.pauseReads(c.conn.fd, c.conn);
        _ = c.conn.read_buf.write(ping);
        shard.resumeReads(c.conn.fd, c.conn);
        shard.settleConnections();
        try std.testing.expectEqual(before + 1, shard.requests_dispatched);
        // A follower never clears it: set there, its shard would spin.
        try std.testing.expectEqual(leads, shard.more_work);
    }
}

test "Shard: an answer too large to frame is answered with an error" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);
    const conn = c.conn;
    const pair = c.pair;
    const big = try std.testing.allocator.alloc(u8, MAX_REQUEST_SIZE + 1);
    defer std.testing.allocator.free(big);
    @memset(big, 'x');
    shard.sendOkResponse(conn, 9, big);
    var out: [512]u8 = undefined;
    const n = try readAnswer(pair[1], &shard, conn.fd, &out, @sizeOf(proto.ResponseHeader));
    const resp = try proto.Response.parse(out[0..n]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.internal_error), resp.header.status);
    try std.testing.expectEqual(@as(u64, 9), resp.header.request_id);
}

test "Shard: the inbox gauges follow the drain" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    var sm = ShardMetrics{ .shard_id = 0 };
    shard.shard_metrics = &sm;
    var i: usize = 0;
    while (i < 3) : (i += 1) try std.testing.expect(shard.inbox.send(.{ .tag = .stream_event }));
    try std.testing.expectEqual(@as(usize, 3), shard.drainInbox());
    try std.testing.expectEqual(@as(u64, 3), sm.inbox_processed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), sm.inbox_pending.load(.monotonic));

    // Read at scrape time from the inbox itself: what waits for a shard
    // that has stopped draining still shows.
    sm.live_pending = .{ .ctx = &shard.inbox, .read = Shard.inboxPending };
    try std.testing.expect(shard.inbox.send(.{ .tag = .stream_event }));
    try std.testing.expect(shard.inbox.send(.{ .tag = .stream_event }));
    try std.testing.expectEqual(@as(u64, 2), sm.snapshot().inbox_pending);
    _ = shard.drainInbox();
    try std.testing.expectEqual(@as(u64, 0), sm.snapshot().inbox_pending);
}

test "Shard: a forwarded write that cannot be parsed is answered, so its forwarder's client is not held" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    try std.testing.expect(shard.applyCommitted());
    shard.raft_network = &rn;
    defer {
        for (rn.outbound.items) |o| std.testing.allocator.free(o.frame);
        rn.outbound.clearRetainingCapacity();
    }
    var payload: [4 + 40]u8 = undefined;
    std.mem.writeInt(u32, payload[0..4], 91, .little);
    @memset(payload[4..], 0xee);
    std.mem.writeInt(u64, payload[4 + 8 ..][0..8], 5, .little);
    shard.runForwardedWrite(.{ .source_node = 2, .group_id = 0, .msg_type = .forward_write, .payload = &payload });
    try std.testing.expectEqual(@as(usize, 1), rn.outbound.items.len);
    const reply = rn.outbound.items[0].frame[transport.HEADER_SIZE..];
    try std.testing.expectEqual(@as(u32, 91), std.mem.readInt(u32, reply[0..4], .little));
    const resp = try proto.Response.parse(reply[12..]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.internal_error), resp.header.status);
    try std.testing.expectEqual(@as(u64, 5), resp.header.request_id);
}

test "Shard: a diverged node refuses writes even if it led" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var q = try RaftQueue.init(std.testing.allocator, 2048);
    defer q.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.raft_queue = &q;
    try std.testing.expect(shard.applyCommitted());

    // A leader that diverges leads nothing and takes no writes.
    shard.markDiverged(2, 9);
    try std.testing.expectEqual(raft_node_mod.Role.follower, shard.raft_node.role);
    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(proto.OpCode.kv_put);
    header.request_id = 5;
    header.payload_length = 2 + 2 + 1 + 4 + 1 + 2;
    shard.raft_node.role = .leader;
    shard.dispatchRequest(conn, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    var out: [512]u8 = undefined;
    const n = conn.write_buf.read(&out);
    const resp = try proto.Response.parse(out[0..n]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.unavailable), resp.header.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.data, "diverged") != null);
}

test "Shard: a forward waits for its leader's answer, is answered when that leader is replaced, and runs here once this node leads" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 8, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 8, .join, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    const raft = shard.raft_node;

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(proto.OpCode.kv_put);
    header.request_id = 5;
    header.payload_length = 2 + 2 + 1 + 4 + 1 + 2;
    // The held copy is re-parsed as the wire would be: the CRC must hold.
    const wire = try Shard.serializeRequest(std.testing.allocator, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    defer std.testing.allocator.free(wire);
    header.crc32 = header.computeCRC32(wire[@sizeOf(proto.RequestHeader)..]);
    shard.dispatchRequest(conn, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    try std.testing.expectEqual(@as(u32, 1), shard.forward_count);
    // Ids sit outside the fd range: a client closing on the leader can
    // never match one in its waiter pool.
    try std.testing.expect(shard.forwards[0].fd == pair[0]);
    try std.testing.expect(@as(i32, @bitCast(shard.forwards[0].id)) < 0);

    // A leader is known but there is no link to it: the write is not
    // marked sent, and past the deadline the client hears why.
    raft.leader_id = 2;
    const now = Shard.nowMs();
    shard.sweepForwards(now);
    try std.testing.expectEqual(@as(u32, 0), shard.forwards[0].sent_to);
    shard.sweepForwards(now + FORWARD_TIMEOUT_MS + 1);
    try std.testing.expectEqual(@as(u32, 0), shard.forward_count);
    var out: [256]u8 = undefined;
    var n = std.c.read(pair[1], &out, out.len);
    var resp = try proto.Response.parse(out[0..@intCast(n)]);
    try std.testing.expect(std.mem.indexOf(u8, resp.data, "not reachable") != null);

    // Sent to a leader: it waits past any deadline for that leader's
    // answer, until the term moves on. (No network here, so the link is
    // not consulted.)
    header.crc32 = header.computeCRC32(wire[@sizeOf(proto.RequestHeader)..]);
    shard.dispatchRequest(conn, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    shard.forwards[0].sent_to = 2;
    shard.forwards[0].sent_term = raft.current_term;
    shard.raft_network = null;
    shard.sweepForwards(now + 10 * FORWARD_TIMEOUT_MS);
    try std.testing.expectEqual(@as(u32, 1), shard.forward_count);
    raft.current_term += 1;
    shard.sweepForwards(now + 10 * FORWARD_TIMEOUT_MS);
    try std.testing.expectEqual(@as(u32, 0), shard.forward_count);
    shard.raft_network = &rn;
    n = std.c.read(pair[1], &out, out.len);
    resp = try proto.Response.parse(out[0..@intCast(n)]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.unavailable), resp.header.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.data, "may still apply") != null);

    // Sent over a link that then went down, with the term unchanged: the
    // answer will never come either.
    header.crc32 = header.computeCRC32(wire[@sizeOf(proto.RequestHeader)..]);
    shard.dispatchRequest(conn, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    shard.forwards[0].sent_to = 2;
    shard.forwards[0].sent_term = raft.current_term;
    shard.sweepForwards(now);
    try std.testing.expectEqual(@as(u32, 0), shard.forward_count);
    n = std.c.read(pair[1], &out, out.len);
    resp = try proto.Response.parse(out[0..@intCast(n)]);
    try std.testing.expect(std.mem.indexOf(u8, resp.data, "lost the link") != null);

    // A write held while no leader was known, then this node leads: it
    // runs here as the client's own request.
    raft.leader_id = 0;
    header.request_id = 6;
    header.crc32 = header.computeCRC32(wire[@sizeOf(proto.RequestHeader)..]);
    shard.dispatchRequest(conn, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    try std.testing.expectEqual(@as(u32, 1), shard.forward_count);
    raft.role = .leader;
    raft.leader_id = 8;
    shard.sweepForwards(now);
    try std.testing.expectEqual(@as(u32, 0), shard.forward_count);
    n = std.c.read(pair[1], &out, out.len);
    resp = try proto.Response.parse(out[0..@intCast(n)]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), resp.header.status);
    try std.testing.expectEqual(@as(u64, 6), resp.header.request_id);
}

test "Shard: a data directory that belonged to a group refuses to run alone, and rejoins as a member" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try testDataDir(&tmp);
    defer std.testing.allocator.free(data_dir);
    const segs = try std.fmt.allocPrint(std.testing.allocator, "{s}/00000/segs", .{data_dir});
    defer std.testing.allocator.free(segs);
    try @import("stdx").fs.makePath(segs);
    var w = SegmentWriter.init(std.testing.allocator, 0, .none);
    defer w.deinit();
    var noop = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 0, "");
    noop.header.crc32c = noop.computeCrc();
    try w.addEntry(&noop);
    var cfg_buf: [membership.MAX_SIZE]u8 = undefined;
    var cfg = entry_mod.buildEntry(.raft_config, entry_mod.Flags.NONE, 1, 2, 0, membership.encode(&.{ 1, 2 }, &cfg_buf));
    cfg.header.crc32c = cfg.computeCrc();
    try w.addEntry(&cfg);
    w.commit_index_at_seal = 2;
    try w.writeToFile(segs);

    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    try std.testing.expectError(error.DataDirWasClustered, Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{}));
    var member = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .join, .{});
    defer member.deinit();
    try std.testing.expectEqual(raft_node_mod.Role.follower, member.raft_node.role);
    try std.testing.expect(member.raft_node.timer_enabled);
    var ids: [membership.MAX_MEMBERS]u32 = undefined;
    try std.testing.expectEqualSlices(u32, &.{ 1, 2 }, member.raft_node.memberIds(&ids));
}

/// Writes parked behind a second member's ack, driven over a socket pair
/// the way a client drives them.
const ParkTest = struct {
    /// The shard leads alone, then adds member 2: from here nothing
    /// commits until `ack`.
    fn joinPeer(sh: *Shard) !void {
        try std.testing.expect(sh.applyCommitted());
        sh.handleJoinRequest(2);
        try ack(sh);
    }

    /// Member 2 acks everything in the log; the shard applies it.
    fn ack(sh: *Shard) !void {
        const r = sh.raft_node;
        const last = r.log.lastIndex();
        r.peers[0].sent_up_to = last;
        r.handleAppendResponse(.{ .term = r.current_term, .success = true, .match_index = last, .from = 2 });
        try std.testing.expect(sh.applyCommitted());
    }

    /// A request as the wire carries it: a parked request is re-parsed
    /// from its bytes, so its length and checksum must hold.
    fn request(op: proto.OpCode, id: u64, ns: []const u8, key: []const u8, value: []const u8, options: []const u8) !proto.Request {
        var header: proto.RequestHeader = undefined;
        @memset(std.mem.asBytes(&header), 0);
        header.magic = proto.MAGIC;
        header.version = proto.VERSION;
        header.op_code = @intFromEnum(op);
        header.request_id = id;
        header.payload_length = @intCast(2 + ns.len + 2 + key.len + 4 + value.len + 2 + options.len);
        var req: proto.Request = .{ .header = header, .namespace = ns, .key = key, .value = value, .options = options };
        const wire = try Shard.serializeRequest(std.testing.allocator, req);
        defer std.testing.allocator.free(wire);
        req.header.crc32 = req.header.computeCRC32(wire[@sizeOf(proto.RequestHeader)..]);
        return req;
    }

    fn send(sh: *Shard, c: *Connection, op: proto.OpCode, id: u64, key: []const u8, value: []const u8) !void {
        sh.dispatchRequest(c, try request(op, id, "", key, value, ""));
    }

    /// Every response the shard has for `c`, flushed and read from `fd`
    /// without blocking; fails unless exactly `into.len` whole responses
    /// arrived and nothing is left.
    fn responses(sh: *Shard, c: *Connection, fd: std.posix.fd_t, buf: []u8, into: []proto.Response) !void {
        sh.flushToClient(c.fd);
        var total: usize = 0;
        while (total < buf.len) {
            const n = std.c.recv(fd, buf[total..].ptr, buf.len - total, std.c.MSG.DONTWAIT);
            if (n <= 0) break;
            total += @intCast(n);
        }
        var off: usize = 0;
        for (into) |*r| {
            r.* = try proto.Response.parse(buf[off..total]);
            off += @sizeOf(proto.ResponseHeader) + r.data.len;
        }
        try std.testing.expectEqual(total, off);
    }
};

test "Shard: appends, enqueues and a time-series write parked behind a peer's ack each answer from their own entry" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);
    const raft = shard.raft_node;

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);

    var f64_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &f64_bytes, @bitCast(@as(f64, 1.5)), .little);
    // Two appends and two enqueues, interleaved with a time-series write:
    // each responder must run right after its own entry applies, before
    // the next entry's applier overwrites what it reads. Answering after
    // the whole batch applied would give the first of each pair the
    // second's id.
    try ParkTest.send(&shard, conn, .stream_append, 10, "s", "a");
    try ParkTest.send(&shard, conn, .queue_enqueue, 11, "q", "m1");
    try ParkTest.send(&shard, conn, .ts_write, 12, "cpu", &f64_bytes);
    const ts_index = raft.log.lastIndex();
    try ParkTest.send(&shard, conn, .stream_append, 13, "s", "b");
    try ParkTest.send(&shard, conn, .queue_enqueue, 14, "q", "m2");
    try std.testing.expectEqual(@as(u32, 5), shard.pending_count);
    var buf: [2048]u8 = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &.{});

    try ParkTest.ack(&shard);
    try std.testing.expectEqual(@as(u32, 0), shard.pending_count);

    var rs: [5]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &rs);
    for (rs, 0..) |resp, i| {
        // Answered in log order.
        try std.testing.expectEqual(@as(u64, 10 + i), resp.header.request_id);
        try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), resp.header.status);
    }
    const first_seq = std.mem.readInt(u64, rs[0].data[0..8], .little);
    const first_ts = std.mem.readInt(u64, rs[0].data[8..16], .little);
    // The second record's id follows the first's: the next sequence in the
    // same millisecond, or a later millisecond.
    const seq = std.mem.readInt(u64, rs[3].data[0..8], .little);
    const ts = std.mem.readInt(u64, rs[3].data[8..16], .little);
    try std.testing.expect(ts > first_ts or (ts == first_ts and seq == first_seq + 1));
    try std.testing.expectEqual(@as(u64, 1), std.mem.readInt(u64, rs[1].data[0..8], .little));
    try std.testing.expectEqual(@as(u64, 2), std.mem.readInt(u64, rs[4].data[0..8], .little));
    // Server-stamped: the point's timestamp is its entry header's clock,
    // its sequence its entry's index.
    const hdr_ms = raft.log.getEntry(ts_index).?.header.timestamp_ns / 1_000_000;
    try std.testing.expectEqual(hdr_ms, std.mem.readInt(u64, rs[2].data[8..16], .little));
    try std.testing.expectEqual(ts_index, std.mem.readInt(u64, rs[2].data[16..24], .little));
}

test "Shard: on a cluster leader a workflow's first step invokes its action without waiting for the commit, and the run resumes once the action's completion applies" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);
    const raft = shard.raft_node;

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    var buf: [1024]u8 = undefined;
    var one: [1]proto.Response = undefined;

    // An action workers take, and a workflow whose first step invokes it.
    const reg: [8]u8 = .{0} ** 8;
    _ = try persistence_mod.proposeEntry(&shard, .action_register, entry_mod.Flags.NONE, "", "act", &reg);
    const def =
        \\{"kind":"Workflow","name":"flow","version":"1.0.0",
        \\"start":{"run":"@actions/act","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
    ;
    try ParkTest.send(&shard, conn, .workflow_create, 20, "flow", def);
    try ParkTest.ack(&shard);
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), one[0].header.status);

    // The start is answered once it applies; its first step waits.
    try ParkTest.send(&shard, conn, .workflow_start, 21, "flow", "");
    try ParkTest.ack(&shard);
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), one[0].header.status);
    const wf = shard.workflow_handler;
    const Run = struct {
        fn byId(h: *WorkflowHandler, id: []const u8) *WorkflowHandler.RunRecord {
            var it = h.runs.valueIterator();
            while (it.next()) |r| if (std.mem.eql(u8, r.run_id_owned, id)) return r;
            @panic("no run with that id");
        }
    };
    var a_id_buf: [32]u8 = undefined;
    const a_id = a_id_buf[0..one[0].data.len];
    @memcpy(a_id, one[0].data);
    try std.testing.expectEqual(WorkflowHandler.RunStatus.running, Run.byId(wf, a_id).status);
    try std.testing.expectEqual(@as(usize, 1), wf.started_to_advance.items.len);

    // Only a leader takes a step: a follower's proposal would be refused.
    raft.role = .follower;
    wf.advanceStartedRuns(&shard);
    try std.testing.expectEqual(WorkflowHandler.RunStatus.running, Run.byId(wf, a_id).status);
    raft.role = .leader;

    // A second client's start is parked when the first run takes its
    // step. The step proposes the invoke and moves on: nothing waits for
    // its commit with the workflow's lock held.
    try ParkTest.send(&shard, conn, .workflow_start, 22, "flow", "");
    wf.advanceStartedRuns(&shard);
    const a = Run.byId(wf, a_id);
    try std.testing.expectEqual(WorkflowHandler.RunStatus.waiting, a.status);
    const action_run = a.pending_action_run_id_owned.?;
    try std.testing.expect(shard.actions_handler.runs.get(action_run) == null);
    try std.testing.expectEqual(@as(u32, 1), shard.pending_count);

    // One ack commits both: the second start is answered (its responder
    // takes the workflow's lock, which nothing holds), and the invoke
    // applies, so workers can take the action run.
    try ParkTest.ack(&shard);
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    try std.testing.expectEqual(@as(u64, 22), one[0].header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), one[0].header.status);
    try std.testing.expectEqual(result_mod.CommandResult.ActionRunStatus.pending, shard.actions_handler.runs.get(action_run).?.status);
    wf.advanceStartedRuns(&shard);
    try std.testing.expectEqual(WorkflowHandler.RunStatus.waiting, Run.byId(wf, one[0].data).status);

    // The action's completion is parked like any write; once it applies,
    // the run resumes and follows its success transition.
    var done: [64]u8 = undefined;
    var off: usize = 0;
    inline for (.{ "act", action_run, "success" }) |part| {
        std.mem.writeInt(u16, done[off..][0..2], @intCast(part.len), .little);
        off += 2;
        @memcpy(done[off .. off + part.len], part);
        off += part.len;
    }
    try ParkTest.send(&shard, conn, .action_complete, 24, "", done[0..off]);
    // Proposed, not applied: the run still waits.
    wf.checkPendingActions(&shard);
    try std.testing.expectEqual(WorkflowHandler.RunStatus.waiting, Run.byId(wf, a_id).status);
    try ParkTest.ack(&shard);
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    try std.testing.expectEqual(@as(u64, 24), one[0].header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), one[0].header.status);
    wf.checkPendingActions(&shard);
    try std.testing.expectEqual(WorkflowHandler.RunStatus.completed, Run.byId(wf, a_id).status);
}

test "Shard: what the tick's workflow steps propose is sent to the followers in that tick" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    const reg: [8]u8 = .{0} ** 8;
    _ = try persistence_mod.proposeEntry(&shard, .action_register, entry_mod.Flags.NONE, "", "act", &reg);
    const def =
        \\{"kind":"Workflow","name":"flow","version":"1.0.0",
        \\"start":{"run":"@actions/act","transitions":{"success":"flo.Completed","failure":"flo.Failed"}}}
    ;
    try ParkTest.send(&shard, conn, .workflow_create, 60, "flow", def);
    try ParkTest.ack(&shard);
    try ParkTest.send(&shard, conn, .workflow_start, 61, "flow", "");
    try ParkTest.ack(&shard);

    // The tick takes the run's first step, which proposes an invoke; the
    // follower hears of it before the tick ends, not at the next poll.
    const raft = shard.raft_node;
    const before = raft.log.lastIndex();
    const sent = rn.outbound.items.len;
    _ = try shard.tick(0);
    try std.testing.expect(raft.log.lastIndex() > before);
    try std.testing.expect(rn.outbound.items.len > sent);
    // What went out carries the new entry, not only a heartbeat.
    try std.testing.expectEqual(raft.log.lastIndex(), raft.peers[0].sent_up_to);
}

test "Shard: a step-down forgets the queued first steps and the in-flight implicit creates" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    const def =
        \\{"kind":"Workflow","name":"gate","version":"1.0.0",
        \\"start":{"waitForSignal":{"type":"go"},"transitions":{"success":"flo.Completed"}}}
    ;
    try ParkTest.send(&shard, conn, .workflow_create, 20, "gate", def);
    try ParkTest.ack(&shard);
    try ParkTest.send(&shard, conn, .workflow_start, 21, "gate", "");
    try ParkTest.ack(&shard);
    shard.namespace_handler.markNamespaceHasData("other", &shard);
    try std.testing.expectEqual(@as(usize, 1), shard.workflow_handler.started_to_advance.items.len);
    try std.testing.expectEqual(@as(u32, 1), shard.namespace_handler.implicit_creates.count());

    // What this leader had in flight may be gone with its log's tail.
    shard.leadershipLost("test");
    try std.testing.expectEqual(@as(usize, 0), shard.workflow_handler.started_to_advance.items.len);
    try std.testing.expectEqual(@as(u32, 0), shard.namespace_handler.implicit_creates.count());
}

test "Shard: an idempotency key of any length is scoped to its namespace, a retry with key and run id is the same start, and a run id started twice is refused" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);
    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    var buf: [2048]u8 = undefined;

    const def =
        \\{"kind":"Workflow","name":"gate","version":"1.0.0",
        \\"start":{"waitForSignal":{"type":"go"},"transitions":{"success":"flo.Completed"}}}
    ;
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_create, 80, "a", "gate", def, ""));
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_create, 81, "b", "gate", def, ""));
    try ParkTest.ack(&shard);
    var two: [2]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &two);

    const keyed = [_]u8{ 0, 0, 1, 3, 0, 'k', 'e', 'y', 0 };
    const named = [_]u8{ 0, 0, 0, 1, 2, 0, 'r', '1' };
    // A retry carrying both its key and its own run id is the same start.
    const both = [_]u8{ 0, 0, 1, 1, 0, 'z', 1, 2, 0, 'r', '2' };
    // A key longer than any fixed buffer is still a key.
    var long: [2 + 1 + 2 + 2000 + 1]u8 = undefined;
    @memset(&long, 'x');
    long[0] = 0;
    long[1] = 0;
    long[2] = 1;
    std.mem.writeInt(u16, long[3..5], 2000, .little);
    long[long.len - 1] = 0;
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_start, 82, "a", "gate", &keyed, ""));
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_start, 83, "b", "gate", &keyed, ""));
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_start, 84, "a", "gate", &named, ""));
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_start, 85, "a", "gate", &named, ""));
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_start, 86, "a", "gate", &both, ""));
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_start, 87, "a", "gate", &both, ""));
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_start, 88, "a", "gate", &long, ""));
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_start, 89, "a", "gate", &long, ""));
    try ParkTest.ack(&shard);
    var rs: [8]proto.Response = undefined;
    var big: [4096]u8 = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &big, &rs);
    // One key in two namespaces is two runs.
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), rs[1].header.status);
    try std.testing.expect(!std.mem.eql(u8, rs[0].data, rs[1].data));
    // The run id is the first start's; the second is refused.
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), rs[2].header.status);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.conflict), rs[3].header.status);
    // Key and run id together: the retry is answered with the first run.
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), rs[5].header.status);
    try std.testing.expectEqualStrings(rs[4].data, rs[5].data);
    // A long key: one run, both answered with it.
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), rs[7].header.status);
    try std.testing.expectEqualStrings(rs[6].data, rs[7].data);
    try std.testing.expectEqual(@as(usize, 5), shard.workflow_handler.runs.count());

    // A namespace a run key cannot carry is refused, for a definition and
    // for a start, before anything is proposed.
    const before = shard.raft_node.log.lastIndex();
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_create, 90, "a:b", "gate", def, ""));
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_start, 91, "a:b", "gate", &keyed, ""));
    try ParkTest.responses(&shard, conn, pair[1], &big, &two);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), two[0].header.status);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), two[1].header.status);
    try std.testing.expectEqualStrings("invalid namespace name", two[1].data);
    try std.testing.expectEqual(before, shard.raft_node.log.lastIndex());
}

test "Shard: a RESP stream append answers with its record's sequence once its entry applies" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    try std.testing.expect(shard.applyCommitted());
    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    conn.protocol = .resp;

    const a = std.testing.allocator;
    shard.executeRespCommand(conn, .{ .opcode = .stream_append, .namespace = "", .key = try a.dupe(u8, "s"), .value = try a.dupe(u8, "a") });
    try std.testing.expectEqual(@as(u32, 0), shard.pending_count);

    // `XADD` answers once, with the record's sequence, not nil.
    const id = shard.stream_handler.stream.streamLastId(node_router.nameHash(node_router.namespaceHash(""), "s"));
    var want_buf: [32]u8 = undefined;
    const want = try std.fmt.bufPrint(&want_buf, ":{d}\r\n", .{id.sequence});
    shard.flushToClient(conn.fd);
    var buf: [64]u8 = undefined;
    const n = std.c.recv(pair[1], &buf, buf.len, std.c.MSG.DONTWAIT);
    try std.testing.expect(n > 0);
    try std.testing.expectEqualStrings(want, buf[0..@intCast(n)]);
    // Counted against its namespace, as a Flo-protocol write is.
    try std.testing.expect(shard.namespace_handler.namespaceHasData("default"));
}

test "Shard: a RESP write on a cluster is refused in RESP, not parked" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);
    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    conn.protocol = .resp;

    const a = std.testing.allocator;
    const before = shard.raft_node.log.lastIndex();
    shard.executeRespCommand(conn, .{ .opcode = .stream_append, .namespace = "", .key = try a.dupe(u8, "s"), .value = try a.dupe(u8, "a") });
    try std.testing.expectEqual(@as(u32, 0), shard.pending_count);
    try std.testing.expectEqual(before, shard.raft_node.log.lastIndex());
    shard.flushToClient(conn.fd);
    var buf: [128]u8 = undefined;
    const n = std.c.recv(pair[1], &buf, buf.len, std.c.MSG.DONTWAIT);
    try std.testing.expect(n > 0);
    try std.testing.expect(std.mem.startsWith(u8, buf[0..@intCast(n)], "-ERR "));
}

test "Shard: concurrent conditional writes are decided in log order: one run per idempotency key, one create per namespace, one version per register" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);
    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    var buf: [2048]u8 = undefined;

    const def =
        \\{"kind":"Workflow","name":"gate","version":"1.0.0",
        \\"start":{"waitForSignal":{"type":"go"},"transitions":{"success":"flo.Completed"}}}
    ;
    try ParkTest.send(&shard, conn, .workflow_create, 70, "gate", def);
    try ParkTest.ack(&shard);
    var one: [1]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);

    // Each pair passes its handler's check before the other applies.
    const start = [_]u8{ 0, 0, 1, 3, 0, 'k', 'e', 'y', 0 };
    try ParkTest.send(&shard, conn, .workflow_start, 71, "gate", &start);
    try ParkTest.send(&shard, conn, .workflow_start, 72, "gate", &start);
    try ParkTest.send(&shard, conn, .namespace_create, 73, "team", "");
    try ParkTest.send(&shard, conn, .namespace_create, 74, "team", "");
    try ParkTest.send(&shard, conn, .action_register, 75, "act", "");
    try ParkTest.send(&shard, conn, .action_register, 76, "act", "");
    try std.testing.expectEqual(@as(u32, 6), shard.pending_count);
    try ParkTest.ack(&shard);

    var rs: [6]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &rs);
    // The second start is answered with the first's run, and created none.
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), rs[0].header.status);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), rs[1].header.status);
    try std.testing.expectEqualStrings(rs[0].data, rs[1].data);
    try std.testing.expectEqual(@as(usize, 1), shard.workflow_handler.runs.count());
    try std.testing.expectEqual(@as(usize, 1), shard.workflow_handler.started_to_advance.items.len);
    // The second create finds the namespace there.
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), rs[2].header.status);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.conflict), rs[3].header.status);
    // Two registers are two versions.
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), rs[5].header.status);
    try std.testing.expectEqual(@as(u32, 2), shard.actions_handler.actions.get("act").?.version);
}

test "Shard: park answers from its own entry when a later one has already committed" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    try std.testing.expect(shard.applyCommitted());

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);

    // On a single node both appends commit as they are proposed; the second
    // stands for another thread's proposal landing before this write parks.
    // The first write's responder must read the first's record.
    const Seen = struct {
        var index: u64 = 0;
        var id: ?StreamID = null;
        fn respond(shard_ptr: *anyopaque, _: *anyopaque, _: proto.Request) void {
            const sh: *Shard = @ptrCast(@alignCast(shard_ptr));
            index = sh.raft_node.last_applied;
            id = sh.stream_handler.last_append;
        }
    };
    const v = try stream_mod.encodeAppendValue(std.testing.allocator, 0, "a");
    defer std.testing.allocator.free(v);
    const first = try persistence_mod.proposeEntry(&shard, .stream_append, entry_mod.Flags.NONE, "", "s", v);
    _ = try persistence_mod.proposeEntry(&shard, .stream_append, entry_mod.Flags.NONE, "", "s", v);
    shard.park(conn, try ParkTest.request(.stream_append, 50, "", "s", "a", ""), first, Seen.respond);
    try std.testing.expectEqual(first.index, Seen.index);
    // The dispatch that parked applies the rest; the second record is not
    // the one the first write was answered with.
    try std.testing.expect(shard.applyCommitted());
    const hash = node_router.nameHash(node_router.namespaceHash(""), "s");
    try std.testing.expect(Seen.id.?.eql(shard.stream_handler.stream.streamFirstId(hash)));
    try std.testing.expect(!Seen.id.?.eql(shard.stream_handler.stream.streamLastId(hash)));
}

test "Shard: a stream's first append to a namespace nobody created is listed under that namespace" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);

    // The append's applier names the stream by resolving its namespace, so
    // the namespace's create must apply first, here and on every replay.
    shard.dispatchRequest(conn, try ParkTest.request(.stream_append, 40, "fresh", "s", "a", ""));
    try ParkTest.ack(&shard);
    var buf: [256]u8 = undefined;
    var one: [1]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), one[0].header.status);
    var q_buf: [handler_mod.MAX_QUALIFIED_KEY]u8 = undefined;
    const names = shard.stream_handler.stream.stream_names;
    try std.testing.expect(names.contains(try handler_mod.qualifyKey(&q_buf, "fresh", "s")));
    try std.testing.expect(!names.contains("s"));
}

test "Shard: a blocking stream read wakes on its own namespace's append, even one proposed before it parked, and returns only what is past its cursor" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    var buf: [4096]u8 = undefined;

    // An append's value is a batch: [count:u32]([len:u32][payload][header_count:u16])*
    const first = "\x01\x00\x00\x00" ++ "\x0c\x00\x00\x00" ++ "first-record" ++ "\x00\x00";
    const second = "\x01\x00\x00\x00" ++ "\x0d\x00\x00\x00" ++ "second-record" ++ "\x00\x00";
    shard.dispatchRequest(conn, try ParkTest.request(.stream_append, 1, "app", "s", first, ""));
    try ParkTest.ack(&shard);
    var one: [1]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    const cursor_seq = std.mem.readInt(u64, one[0].data[0..8], .little);
    const cursor_ms = std.mem.readInt(u64, one[0].data[8..16], .little);

    // Proposed, not yet applied, when the read parks: the read finds
    // nothing and must still wake when this applies.
    shard.dispatchRequest(conn, try ParkTest.request(.stream_append, 2, "app", "s", second, ""));

    var opts_buf: [64]u8 = undefined;
    var ob = proto.OptionsBuilder.init(&opts_buf);
    try ob.addStreamId(.stream_start, cursor_ms, cursor_seq);
    try ob.addU32(.block_ms, 5000);
    shard.dispatchRequest(conn, try ParkTest.request(.stream_read, 3, "app", "s", "", ob.getOptions()));
    // The same stream name in another namespace has nothing to wake it.
    var other_buf: [64]u8 = undefined;
    var other = proto.OptionsBuilder.init(&other_buf);
    try other.addU32(.block_ms, 5000);
    shard.dispatchRequest(conn, try ParkTest.request(.stream_read, 4, "other", "s", "", other.getOptions()));
    try std.testing.expectEqual(@as(u16, 2), shard.waiter_pool.countByKind(.stream_read));

    try ParkTest.ack(&shard);
    var two: [2]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &two);
    const read = if (two[0].header.request_id == 3) two[0] else two[1];
    try std.testing.expectEqual(@as(u64, 3), read.header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), read.header.status);
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, read.data[0..4], .little));
    try std.testing.expect(std.mem.indexOf(u8, read.data, "second-record") != null);
    try std.testing.expect(std.mem.indexOf(u8, read.data, "first-record") == null);
    try std.testing.expectEqual(@as(u16, 1), shard.waiter_pool.countByKind(.stream_read));
}

test "Shard: a blocking read on one partition skips other partitions' appends without rescanning them, and wakes on its own" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    var buf: [4096]u8 = undefined;
    const other = "\x01\x00\x00\x00" ++ "\x0b\x00\x00\x00" ++ "other-part1" ++ "\x00\x00";
    const mine = "\x01\x00\x00\x00" ++ "\x0a\x00\x00\x00" ++ "mine-part2" ++ "\x00\x00";

    var read_buf: [64]u8 = undefined;
    var ro = proto.OptionsBuilder.init(&read_buf);
    try ro.addU32(.partition, 2);
    try ro.addU32(.block_ms, 5000);
    shard.dispatchRequest(conn, try ParkTest.request(.stream_read, 1, "", "s", "", ro.getOptions()));
    try std.testing.expectEqual(@as(u16, 1), shard.waiter_pool.countByKind(.stream_read));

    var p1_buf: [16]u8 = undefined;
    var p1 = proto.OptionsBuilder.init(&p1_buf);
    try p1.addU32(.partition, 1);
    shard.dispatchRequest(conn, try ParkTest.request(.stream_append, 2, "", "s", other, p1.getOptions()));
    try ParkTest.ack(&shard);
    var one: [1]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    try std.testing.expectEqual(@as(u64, 2), one[0].header.request_id);
    // Still parked, with its cursor at the last id it scanned.
    try std.testing.expectEqual(@as(u16, 1), shard.waiter_pool.countByKind(.stream_read));
    const scanned = shard.stream_handler.stream.streamLastId(shard.waiter_pool.waiters[0].stream.name_hash);
    try std.testing.expect(shard.waiter_pool.waiters[0].stream.after.eql(scanned));

    var p2_buf: [16]u8 = undefined;
    var p2 = proto.OptionsBuilder.init(&p2_buf);
    try p2.addU32(.partition, 2);
    shard.dispatchRequest(conn, try ParkTest.request(.stream_append, 3, "", "s", mine, p2.getOptions()));
    try ParkTest.ack(&shard);
    var two: [2]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &two);
    const read = if (two[0].header.request_id == 1) two[0] else two[1];
    try std.testing.expectEqual(@as(u64, 1), read.header.request_id);
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, read.data[0..4], .little));
    try std.testing.expect(std.mem.indexOf(u8, read.data, "mine-part2") != null);
    try std.testing.expectEqual(@as(u16, 0), shard.waiter_pool.countByKind(.stream_read));
}

test "Shard: a blocking group read wakes on its own namespace's append, not a same-named stream elsewhere" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);
    var buf: [4096]u8 = undefined;
    const rec = "\x01\x00\x00\x00" ++ "\x01\x00\x00\x00" ++ "r" ++ "\x00\x00";
    // [group_len:u16][group][consumer_len:u16][consumer]
    const group_consumer = "\x01\x00" ++ "g" ++ "\x01\x00" ++ "c";

    var ob_buf: [16]u8 = undefined;
    var ob = proto.OptionsBuilder.init(&ob_buf);
    try ob.addU32(.block_ms, 5000);
    shard.dispatchRequest(conn, try ParkTest.request(.stream_group_read, 1, "app", "s", group_consumer, ob.getOptions()));
    try std.testing.expectEqual(@as(u16, 1), shard.waiter_pool.countByKind(.stream_group_read));

    shard.dispatchRequest(conn, try ParkTest.request(.stream_append, 2, "other", "s", rec, ""));
    try ParkTest.ack(&shard);
    var one: [1]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    try std.testing.expectEqual(@as(u64, 2), one[0].header.request_id);
    try std.testing.expectEqual(@as(u16, 1), shard.waiter_pool.countByKind(.stream_group_read));

    shard.dispatchRequest(conn, try ParkTest.request(.stream_append, 3, "app", "s", rec, ""));
    try ParkTest.ack(&shard);
    var two: [2]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &two);
    try std.testing.expect(two[0].header.request_id == 1 or two[1].header.request_id == 1);
    try std.testing.expectEqual(@as(u16, 0), shard.waiter_pool.countByKind(.stream_group_read));
}

test "Shard: a deferred answer too large to frame reaches its client as an error" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);

    const big = try std.testing.allocator.alloc(u8, MAX_REQUEST_SIZE + 1);
    defer std.testing.allocator.free(big);
    @memset(big, 'x');
    shard.deliverDeferredResponse(0, conn.fd, conn.id, 9, .ok, big);
    var buf: [512]u8 = undefined;
    var one: [1]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    try std.testing.expectEqual(@as(u64, 9), one[0].header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.internal_error), one[0].header.status);
}

test "Shard: a parked request leaves the pending table before its responder runs, so a sweep during it answers the client once" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);

    var pair: [2]std.posix.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &pair));
    defer _ = std.c.close(pair[1]);
    const conn = try shard.addConnection(pair[0]);

    // A responder that sweeps the pending table: the request it answers
    // must not be in it.
    const Sweep = struct {
        fn respond(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: proto.Request) void {
            const sh: *Shard = @ptrCast(@alignCast(shard_ptr));
            sh.resolvePending("swept");
            sh.sendOkResponse(@ptrCast(@alignCast(conn_ptr)), req.header.request_id, "answered");
        }
    };
    // Any entry will do: the responder under test never reads it.
    const proposed = try persistence_mod.proposeEntry(&shard, .namespace_create, entry_mod.Flags.NONE, "", "k", "");
    shard.park(conn, try ParkTest.request(.kv_put, 30, "", "k", "v", ""), proposed, Sweep.respond);
    try std.testing.expectEqual(@as(u32, 1), shard.pending_count);

    try ParkTest.ack(&shard);
    try std.testing.expectEqual(@as(u32, 0), shard.pending_count);
    // One answer, the responder's.
    var buf: [256]u8 = undefined;
    var one: [1]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    try std.testing.expectEqual(@as(u64, 30), one[0].header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), one[0].header.status);
    try std.testing.expectEqualStrings("answered", one[0].data);
}

test "Shard: a burst of first writes to a new namespace proposes one implicit create, and a write after it applied proposes another if the namespace is gone" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var rn = try RaftNetwork.init(std.testing.allocator, 1, 0, 9000, .{ 127, 0, 0, 1 }, "s");
    defer rn.deinit();
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{});
    defer shard.deinit();
    shard.raft_network = &rn;
    shard.wireHandlerShardPtrs();
    try ParkTest.joinPeer(&shard);
    const raft = shard.raft_node;
    const ns = shard.namespace_handler;

    const Count = struct {
        fn creates(r: *RaftNode, from: u64) u32 {
            var n: u32 = 0;
            var i = from;
            while (i <= r.log.lastIndex()) : (i += 1) {
                if (r.log.getEntry(i).?.header.entry_type == @intFromEnum(entry_mod.EntryType.namespace_create)) n += 1;
            }
            return n;
        }
    };
    const first = raft.log.lastIndex() + 1;
    for (0..3) |_| ns.markNamespaceHasData("fresh", &shard);
    try std.testing.expectEqual(@as(u32, 1), Count.creates(raft, first));

    // Once it applies the namespace exists, counted once however many
    // writes the burst held.
    try ParkTest.ack(&shard);
    try std.testing.expectEqual(@as(u32, 1), ns.namespaces.get("fresh").?.data_count);

    // Applied, it no longer stands in the way: a namespace removed since
    // is created again by its next write.
    ns.applyDelete("fresh");
    const again = raft.log.lastIndex() + 1;
    ns.markNamespaceHasData("fresh", &shard);
    try std.testing.expectEqual(@as(u32, 1), Count.creates(raft, again));
}

test "raftRingCapacity: a quarter of the hot buffer, floored and capped" {
    try std.testing.expectEqual(RAFT_RING_MIN, raftRingCapacity(4096));
    try std.testing.expectEqual(RAFT_RING_MIN, raftRingCapacity(8 * 1024 * 1024));
    try std.testing.expectEqual(@as(usize, 8 * 1024 * 1024), raftRingCapacity(32 * 1024 * 1024));
    try std.testing.expectEqual(RAFT_RING_MAX, raftRingCapacity(64 * 1024 * 1024));
    try std.testing.expectEqual(RAFT_RING_MAX, raftRingCapacity(1024 * 1024 * 1024));
}

test "the Raft ring floor holds the largest transaction batch" {
    var log_inst = try RaftLog.init(std.testing.allocator, RAFT_RING_MIN);
    defer log_inst.deinit();
    const payload = try std.testing.allocator.alloc(u8, @import("../kv/handler.zig").MAX_APPLY_PAYLOAD);
    defer std.testing.allocator.free(payload);
    @memset(payload, 'b');
    var e = entry_mod.buildEntry(.kv_batch, entry_mod.Flags.NONE, 1, 1, 0, payload);
    e.header.crc32c = e.computeCrc();
    try std.testing.expectEqual(@as(u64, 1), try log_inst.append(&e));
}

test "assertOneApplier refuses a registry callback for a router-owned type" {
    const Noop = struct {
        fn apply(_: *anyopaque, _: *const entry_mod.Entry) void {}
    };
    var registry: ReplayRegistry = .{};
    var ctx: u8 = 0;
    registry.register(.stream_append, @ptrCast(&ctx), Noop.apply);
    try assertOneApplier(0, &registry);
    registry.register(.ts_write, @ptrCast(&ctx), Noop.apply);
    try std.testing.expectError(error.EntryTypeHasTwoAppliers, assertOneApplier(0, &registry));
}

test "applyCommitted: an applier that proposes does not re-enter the drain" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();

    // An applier that proposes in turn, the way a dequeue's auto-ack does
    // from inside a notification. It records what it observes.
    const Probe = struct {
        var shard_ptr: *Shard = undefined;
        var last_applied_seen: u64 = 0;
        var nested_saw_kv: bool = false;
        var calls: u32 = 0;
        fn apply(_: *anyopaque, _: *const entry_mod.Entry) void {
            calls += 1;
            const shard_inner = shard_ptr;
            last_applied_seen = shard_inner.raft_node.last_applied;
            _ = persistence_mod.proposeEntry(shard_inner, .kv_put, entry_mod.Flags.NONE, "", "nested", "v") catch unreachable;
            _ = shard_inner.applyCommitted();
            nested_saw_kv = shard_inner.kv_handler.kv.get("nested") != null;
        }
    };
    Probe.shard_ptr = &shard;
    var ctx: u8 = 0;
    shard.replay_registry.register(.raft_config, @ptrCast(&ctx), Probe.apply);

    const idx = (try persistence_mod.proposeEntry(&shard, .raft_config, entry_mod.Flags.NONE, "", "probe", "")).index;
    try std.testing.expect(shard.applyCommitted());

    // The entry in hand was marked applied before its applier ran, so a
    // nested drain had nothing to take twice; and that drain was a no-op,
    // leaving the nested proposal to the outer loop.
    try std.testing.expectEqual(idx, Probe.last_applied_seen);
    try std.testing.expect(!Probe.nested_saw_kv);
    try std.testing.expect(shard.kv_handler.kv.get("nested") != null);
    try std.testing.expectEqual(shard.raft_node.commit_index, shard.raft_node.last_applied);
    try std.testing.expectEqual(@as(u32, 1), Probe.calls);
}

fn testDataDir(tmp: *std.testing.TmpDir) ![]const u8 {
    return @import("stdx").fs.dirRealpathAlloc(tmp.dir, std.testing.allocator, ".");
}

fn testCommandEntry(entry_type: entry_mod.EntryType, index: u64, key: []const u8, value: []const u8) entry_mod.Entry {
    const S = struct {
        var payload_buf: [256]u8 = undefined;
    };
    const cmd = entry_mod.CommandPayload{
        .namespace_hash = node_router.namespaceHash(""),
        .key_length = @intCast(key.len),
        .value_length = @intCast(value.len),
        .key = key,
        .value = value,
    };
    const n = cmd.serialize(&S.payload_buf).?;
    var e = entry_mod.buildEntry(entry_type, entry_mod.Flags.NONE, 1, index, index * 1000, S.payload_buf[0..n]);
    e.header.crc32c = e.computeCrc();
    return e;
}

test "replay applies up to the commit watermark; the tail waits for commit to be re-established" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try testDataDir(&tmp);
    defer std.testing.allocator.free(data_dir);

    // A segment as a follower could have flushed it: three entries, sealed
    // while only the first two were committed.
    const segs = try std.fmt.allocPrint(std.testing.allocator, "{s}/00000/segs", .{data_dir});
    defer std.testing.allocator.free(segs);
    try @import("stdx").fs.makePath(segs);
    var w = SegmentWriter.init(std.testing.allocator, 0, .none);
    defer w.deinit();
    var noop = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 0, "");
    noop.header.crc32c = noop.computeCrc();
    try w.addEntry(&noop);
    try w.addEntry(&testCommandEntry(.queue_enqueue, 2, "q", &[_]u8{ 0, 0, 0, 0, 'A' }));
    try w.addEntry(&testCommandEntry(.queue_enqueue, 3, "q", &[_]u8{ 0, 0, 0, 0, 'B' }));
    w.commit_index_at_seal = 2;
    try w.writeToFile(segs);

    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();

    const q = node_router.nameHash(node_router.namespaceHash(""), "q");
    // Index 3 is in the log and nowhere else.
    try std.testing.expectEqual(@as(u64, 1), shard.queue_handler.queue.countQueue(q));
    try std.testing.expectEqual(@as(u64, 2), shard.raft_node.last_applied);
    try std.testing.expectEqual(@as(u64, 4), shard.raft_node.log.lastIndex());
    try std.testing.expectEqual(@as(u64, 4), shard.raft_node.commit_index);

    shard.applyDeferredTail();
    try std.testing.expectEqual(@as(u64, 2), shard.queue_handler.queue.countQueue(q));
    try std.testing.expectEqual(@as(u64, 4), shard.raft_node.last_applied);

    // Below the ring the log reads the segments: the wiring, not a stub.
    const src = shard.raft_node.log.catch_up.?;
    var buf: [4]entry_mod.Entry = undefined;
    var arena: [512]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 2), src.read_range(src.ctx, 2, &buf, &arena));
    try std.testing.expectEqual(@as(u64, 3), buf[1].header.index);
    try std.testing.expectEqualStrings("q", entry_mod.CommandPayload.deserialize(buf[1].payload).?.key);
}

test "a truncated suffix is gone from the segments a restart replays" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try testDataDir(&tmp);
    defer std.testing.allocator.free(data_dir);
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);

    {
        var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
        defer shard.deinit();
        shard.wireHandlerShardPtrs();

        _ = try persistence_mod.proposeEntry(&shard, .kv_put, entry_mod.Flags.NONE, "", "k", "A");
        try std.testing.expect(shard.applyCommitted());
        // An uncommitted tail, flushed: the shape a follower is left with
        // when its leader dies mid-batch.
        _ = try persistence_mod.proposeEntry(&shard, .kv_put, entry_mod.Flags.NONE, "", "k", "C");
        try shard.flushSegmentToDisk();
        // The new leader's log disagrees at index 3; commit never covered it.
        shard.raft_node.commit_index = 2;
        shard.raft_node.log.truncateAfter(2);
        _ = try persistence_mod.proposeEntry(&shard, .kv_put, entry_mod.Flags.NONE, "", "k", "D");
        try std.testing.expect(shard.applyCommitted());
        try std.testing.expectEqualStrings("D", shard.kv_handler.kv.get("k").?.value);
    }

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    shard.applyDeferredTail();
    // Index 3 is D, once; the file that held C was rewritten without it.
    try std.testing.expectEqualStrings("D", shard.kv_handler.kv.get("k").?.value);
    try std.testing.expectEqual(@as(u64, 4), shard.raft_node.log.lastIndex());
}

test "a committed entry the durable log cannot serve fails the drain instead of being skipped" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try testDataDir(&tmp);
    defer std.testing.allocator.free(data_dir);
    const segs = try std.fmt.allocPrint(std.testing.allocator, "{s}/00000/segs", .{data_dir});
    defer std.testing.allocator.free(segs);
    try @import("stdx").fs.makePath(segs);
    var w = SegmentWriter.init(std.testing.allocator, 0, .none);
    defer w.deinit();
    var noop = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 0, "");
    noop.header.crc32c = noop.computeCrc();
    try w.addEntry(&noop);
    try w.addEntry(&testCommandEntry(.queue_enqueue, 2, "q", &[_]u8{ 0, 0, 0, 0, 'A' }));
    try w.addEntry(&testCommandEntry(.queue_enqueue, 3, "q", &[_]u8{ 0, 0, 0, 0, 'B' }));
    w.commit_index_at_seal = 2;
    try w.writeToFile(segs);

    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();

    // Evict 3 and the bootstrap noop from the ring without the log knowing,
    // and drop the writer's buffer: 3 is still in a segment, 4 is nowhere.
    shard.raft_node.log.ual.truncateAfter(2);
    shard.segment_writer.reset();
    try std.testing.expect(!shard.applyCommitted());
    const q = node_router.nameHash(node_router.namespaceHash(""), "q");
    try std.testing.expectEqual(@as(u64, 2), shard.queue_handler.queue.countQueue(q));
}

test "a truncation the previous run recorded is finished before replay" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try testDataDir(&tmp);
    defer std.testing.allocator.free(data_dir);
    const segs = try std.fmt.allocPrint(std.testing.allocator, "{s}/00000/segs", .{data_dir});
    defer std.testing.allocator.free(segs);
    try @import("stdx").fs.makePath(segs);
    // Indices 1-3 in one file, 4 in another, and a cut after 2 that never
    // reached either file.
    var w = SegmentWriter.init(std.testing.allocator, 0, .none);
    defer w.deinit();
    var noop = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 0, "");
    noop.header.crc32c = noop.computeCrc();
    try w.addEntry(&noop);
    try w.addEntry(&testCommandEntry(.kv_put, 2, "k", "A"));
    try w.addEntry(&testCommandEntry(.kv_put, 3, "k", "C"));
    w.commit_index_at_seal = 3;
    try w.writeToFile(segs);
    w.reset();
    try w.addEntry(&testCommandEntry(.kv_put, 4, "k", "X"));
    w.commit_index_at_seal = 4;
    try w.writeToFile(segs);
    const intent = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ segs, durable_log_mod.INTENT_FILENAME });
    defer std.testing.allocator.free(intent);
    {
        const f = try @import("stdx").fs.createFile(intent, .{});
        defer @import("stdx").fs.closeFile(f);
        try @import("stdx").fs.writeAll(f, "2\n");
    }

    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    shard.applyDeferredTail();

    try std.testing.expectEqualStrings("A", shard.kv_handler.kv.get("k").?.value);
    try std.testing.expectEqual(@as(u64, 3), shard.raft_node.log.lastIndex());
    try std.testing.expectError(error.FileNotFound, @import("stdx").fs.access(intent, .{}));
}

test "a snapshot ahead of the commit watermark is not drained over at boot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try testDataDir(&tmp);
    defer std.testing.allocator.free(data_dir);
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);

    {
        var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
        defer shard.deinit();
        shard.wireHandlerShardPtrs();
        _ = try persistence_mod.proposeEntry(&shard, .queue_enqueue, entry_mod.Flags.NONE, "", "q", &[_]u8{ 0, 0, 0, 0, 'A' });
        _ = try persistence_mod.proposeEntry(&shard, .queue_enqueue, entry_mod.Flags.NONE, "", "q", &[_]u8{ 0, 0, 0, 0, 'B' });
        try std.testing.expect(shard.applyCommitted());
        // Sealed under a lagging commit, as a follower's segment is, then a
        // snapshot taken past it.
        shard.raft_node.commit_index = 1;
        try shard.flushSegmentToDisk();
        shard.raft_node.commit_index = 3;
        const partition = shard.partitions[0];
        const snap = try partition.snapshot();
        defer std.testing.allocator.free(snap);
        var name_buf: [128]u8 = undefined;
        const name = snapshot_mod.snapshotFilename(&name_buf, partition.router.applied_index, 1);
        const snaps = try std.fmt.allocPrint(std.testing.allocator, "{s}/00000/snaps/{s}", .{ data_dir, name });
        defer std.testing.allocator.free(snaps);
        {
            const f = try @import("stdx").fs.createFile(snaps, .{});
            defer @import("stdx").fs.closeFile(f);
            try @import("stdx").fs.writeAll(f, snap);
        }
        const shard_dir = try std.fmt.allocPrint(std.testing.allocator, "{s}/00000", .{data_dir});
        defer std.testing.allocator.free(shard_dir);
        try ShardManifest.setLatestSnapshot(std.testing.allocator, shard_dir, name);
    }

    var shard = try Shard.init(std.testing.allocator, 0, 4, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    shard.applyDeferredTail();
    const q = node_router.nameHash(node_router.namespaceHash(""), "q");
    try std.testing.expectEqual(@as(u64, 2), shard.queue_handler.queue.countQueue(q));
    // Nothing the snapshot covers was offered to the projections again.
    try std.testing.expectEqual(@as(u64, 0), shard.partitions[0].router.stats.entries_skipped);
}
