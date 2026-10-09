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
const builtin = @import("builtin");
const posix = std.posix;
const log = @import("stdx").log;
const reactor_mod = @import("reactor.zig");
const Reactor = reactor_mod.Reactor;
const ReactorEvent = reactor_mod.Event;
const Tag = reactor_mod.Tag;
const mailbox_mod = @import("mailbox.zig");
const Mailbox = mailbox_mod.Mailbox;
const ReplyTo = @import("reply_to.zig").ReplyTo;
const reply_pool_mod = @import("reply_pool.zig");
const ReplyPool = reply_pool_mod.ReplyPool;
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
const result_mod = @import("../protocol/result.zig");
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
pub const run_id_mod = @import("run_id.zig");
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;
const ShardMetrics = @import("../metrics/registry.zig").ShardMetrics;

/// Maximum single-request size we handle on the stack.
pub const MAX_REQUEST_SIZE = 256 * 1024; // 256 KB
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

    /// What other shards put in front of this one: requests, replies and
    /// wakes (heap-allocated so peers can hold its address).
    mailbox: *Mailbox,
    /// A slot per client request forwarded to another shard, until its
    /// answer comes back or `expireReplySlots` takes it back.
    reply_pool: ReplyPool,
    /// Answers dropped for want of a reply slot or of room on the asker's
    /// reply ring. Both are reserved before every request, so a count means
    /// an answer came twice or long after its slot expired.
    replies_dropped: u64 = 0,
    /// The request being dispatched, which its handler answers through its
    /// connection; see `deliverDeferred`.
    dispatching: ?Dispatching = null,
    /// Requests answered as deferred without being parked; see
    /// `answeredAsDeferred`.
    answered_as_deferred: u64 = 0,
    /// Client requests refused before reaching another shard, and when that
    /// was last said; when unanswered slots are next swept.
    forwards_refused: u64 = 0,
    refuse_warn_ms: u64 = 0,
    reply_sweep_ms: u64 = 0,
    /// Unanswered requests given up on since that was last said, and when.
    slots_expired: u64 = 0,
    expire_warn_ms: u64 = 0,

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
    /// Last "lost-log guard" line: a node waiting on a leader or a quorum
    /// that never comes says so, and names the way out.
    lost_log_warn_ms: u64 = 0,
    frame_warn_ms: u64,
    late_apply_warn_ms: u64,
    forward_reply_warn_ms: u64 = 0,
    election_warn_ms: u64,
    elections_unlogged: u64,
    /// One limiter per peer for each thing the leader loop says about it.
    peer_silent_warn_ms: [raft_node_mod.MAX_PEERS]u64,
    /// When a failed sync flush was last logged; it repeats on every request.
    flush_fail_warn_ms: u64,
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

    /// Cross-shard shard pointer array (null until wired by runtime).
    /// Enables direct dispatch on another shard's handlers for pre-routed requests.
    peer_shards: ?[]*Shard,

    /// Every shard's mailbox, this one's included (null until wired by
    /// runtime).
    peer_mailboxes: ?[]*Mailbox,

    /// Per-shard proxy Connection used to drive forwarded requests on this
    /// shard's reactor thread. The real Connection lives on the owner shard;
    /// this proxy carries the requester's address (`Connection.proxy_for`) so
    /// handlers register waiters with the correct routing target, and accumulates direct
    /// responses in its `write_buf` for delivery on the asker's reply ring.
    /// Reused sequentially — safe because each shard's reactor is single-
    /// threaded and `drainInbox` processes one message at a time.
    forward_proxy: *Connection,

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
    /// Paused or waiting connections to read again (`flushToClient`, the
    /// stall check, `resumeWaiting`);
    /// their buffered requests run at the end of the tick. The end of the
    /// tick runs a swapped-out snapshot (`resume_running`), so what is
    /// queued while it runs waits for the next tick: one entry per
    /// connection at most, and both lists are reserved as above.
    resume_fds: std.ArrayListUnmanaged(i32) = .empty,
    /// Reads one connection gets per event before the others' turn; the
    /// rest is read at the end of the tick. A field so tests can shrink it.
    reads_per_event: u32 = 8,
    resume_running: std.ArrayListUnmanaged(i32) = .empty,
    /// The line of connections whose next request waits for room to go to
    /// another shard (`waitForward`), in the order they began waiting,
    /// checked at the end of every tick. An entry is live while its
    /// connection's `wait_seq` matches; a connection has at most one live
    /// and one stale entry, so two per connection are reserved as above.
    waiting_fds: std.ArrayListUnmanaged(WaitEntry) = .empty,
    /// Cancels of closed clients' blocking reads still to be sent
    /// (`releaseForwardsFor`).
    pending_cancels: std.ArrayListUnmanaged(struct { target: u16, seq: u64 }) = .empty,
    /// The last `WaitEntry.seq` given out; never 0.
    wait_seq: u32 = 0,
    /// Connections waiting for each target's room, by class
    /// (`Connection.in_queue`): a new request for that target queues behind
    /// them.
    queued: [2][@import("../config/server.zig").MAX_SHARDS]u32 = .{ @splat(0), @splat(0) },
    /// When a refusal after waiting was last logged, per wait reason.
    wait_warn_ms: [@typeInfo(Connection.WaitReason).@"enum".fields.len]u64 = @splat(0),
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

        // Without its wake a shard sees another's message only at its poll
        // timeout; a shard that cannot have one does not start.
        var reply_pool = try ReplyPool.init(allocator, ReplyPool.slotsFor(shard_count), shard_count);
        errdefer reply_pool.deinit(allocator);
        var waiter_pool = try WaiterPool.init(allocator);
        errdefer waiter_pool.deinit(allocator);
        waiter_pool.owner = shard_id;
        waiter_pool.split = shard_count > 1 or cluster_role != .single;
        const mailbox = try Mailbox.create(allocator, 1024, reply_pool.ringCapacity(), shard_count);
        errdefer mailbox.destroy(allocator);
        try reactor.addSource(.{ .fd = mailbox.wake.rd, .tag = .inbox_ready, .interests = .{ .readable = true } });

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
        namespace_handler.* = try NamespaceHandler.init(allocator);
        errdefer namespace_handler.deinit();

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
        // A node with no data dir has no hard state to lose.
        var had_hard_state = true;
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

            // Each synced into its parent: a lost directory takes HARDSTATE
            // and every segment inside it with it.
            @import("stdx").fs.makePathDurable(shard_dir) catch |err| {
                log.err("shard {d}: cannot create or sync {s}: {s}", .{ shard_id, shard_dir, @errorName(err) });
                return err;
            };

            const segs_dir_path = try std.fmt.allocPrint(allocator, "{s}/segs", .{shard_dir});
            defer allocator.free(segs_dir_path);
            @import("stdx").fs.makePathDurable(segs_dir_path) catch |err| {
                log.err("shard {d}: cannot create or sync {s}: {s}", .{ shard_id, segs_dir_path, @errorName(err) });
                return err;
            };
            const dl = try allocator.create(DurableLog);
            dl.* = DurableLog.init(allocator, seg_writer, segs_dir_path) catch |err| {
                allocator.destroy(dl);
                return err;
            };
            durable_log = dl;

            const snaps_dir_path = try std.fmt.allocPrint(allocator, "{s}/snaps", .{shard_dir});
            defer allocator.free(snaps_dir_path);
            @import("stdx").fs.makePathDurable(snaps_dir_path) catch |err| {
                log.err("shard {d}: cannot create or sync {s}: {s}", .{ shard_id, snaps_dir_path, @errorName(err) });
                return err;
            };

            // ── Hard state ──────────────────────────────────────────────
            // The term and vote this shard's Raft node must not forget.
            const loaded = hard_state_mod.load(shard_dir) catch |err| {
                log.err("shard {d}: cannot read {s}/{s}: {s}", .{ shard_id, shard_dir, hard_state_mod.FILENAME, @errorName(err) });
                return err;
            };
            had_hard_state = loaded != null;
            if (loaded) |hs| {
                if (hs.node_id != node_id) {
                    log.err("shard {d}: {s}/HARDSTATE belongs to node {d}, this node is {d}; the shard directories come from different nodes", .{ shard_id, shard_dir, hs.node_id, node_id });
                    return error.NodeIdMismatch;
                }
                raft_node.current_term = hs.term;
                raft_node.voted_for = hs.voted_for;
                if (hs.lost_log) raft_node.resumeLostLog();
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
        seg_writer.stop_at_gap = durability == .sync;
        if (durable_log) |dl| {
            raft_node.log.catch_up = .{ .ctx = @ptrCast(dl), .read_range = catchUpReadRange };
            raft_node.log.on_truncate_ctx = @ptrCast(dl);
            raft_node.log.on_truncate = durableTruncate;
        }
        try bringUpGroup(raft_node, cluster_role, apply_buf, shard_id, node_id, had_hard_state);

        // Build dispatcher and register all handlers
        var dispatcher = Dispatcher.init();
        dispatcher.setErrorHandler(answerUnknownOp);
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
            .mailbox = mailbox,
            .reply_pool = reply_pool,
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
            .flush_fail_warn_ms = 0,
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
            .waiter_pool = waiter_pool,
            .task_scheduler = TaskScheduler.init(),
            .hot_flush_seconds = hot_flush_seconds,
            .peer_shards = null,
            .peer_mailboxes = null,
            .forward_proxy = forward_proxy,
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
        try self.rebufferGap(dl.writer);
        try dl.flush(self.raft_node.commit_index);
    }

    /// Buffer again what the append hook could not, from the log, so the
    /// flush writes a run without a gap. The log evicts by size, so the
    /// entry may be gone; then nothing after it can reach disk, and the
    /// shard takes no more writes until a restart.
    fn rebufferGap(self: *Shard, writer: *SegmentWriter) !void {
        const raft = self.raft_node;
        const from = writer.first_unbuffered orelse {
            // A truncation cut the missing entry away: nothing is owed.
            if (raft.writes_stopped) {
                raft.writes_stopped = false;
                log.info("shard {d}: the entry that never reached disk was cut from the log; taking writes again", .{self.id});
            }
            return;
        };
        var idx = from;
        while (idx <= raft.log.lastIndex()) : (idx += 1) {
            const e = raft.log.getEntryCopy(idx, self.apply_buf) orelse {
                if (!raft.writes_stopped) {
                    raft.writes_stopped = true;
                    log.err("shard {d}: entry index={d} left memory before it reached disk, so nothing after it can be flushed; this shard takes no more writes until it restarts", .{ self.id, idx });
                }
                return error.EntryUnavailable;
            };
            writer.addEntry(&e) catch |err| {
                writer.first_unbuffered = idx;
                return err;
            };
        }
        writer.first_unbuffered = null;
    }

    /// Apply what replay loaded into the log above the commit watermark.
    /// Called once the shard is at its final address and before it serves
    /// anything, so no read observes the gap. A shard that bootstrapped
    /// re-established commit at the tip and drains the whole tail; one
    /// that follows has nothing to drain until a leader speaks.
    pub fn applyDeferredTail(self: *Shard) void {
        const raft = self.raft_node;
        const pending = raft.commit_index -| raft.last_applied;
        // Bootstrapping commits the term's noop with the tail, unless
        // commits wait for the disk: then it isn't committed yet.
        const noop: u64 = if (self.durability == .sync) 0 else 1;
        if (pending > noop) {
            log.info("shard {d}: applying {d} durable entries above the commit watermark (indices {d}..{d})", .{ self.id, pending, raft.last_applied + 1, raft.commit_index });
        }
        if (!self.applyCommitted()) {
            log.err("shard {d}: not every durable entry above the commit watermark could be applied at boot; projections are missing writes", .{self.id});
        }
    }

    /// With sync durability, flush what the log holds and tell the Raft
    /// node it's on disk: a leader's own copy counts toward commit only then.
    pub fn syncFlushIfNeeded(self: *Shard) void {
        if (self.durability != .sync) return;
        const raft = self.raft_node;
        const through = raft.log.lastIndex();
        self.flushSegmentToDisk() catch |err| {
            self.persist_failures += 1;
            const now = nowMs();
            if (now -| self.flush_fail_warn_ms >= WARN_INTERVAL_MS) {
                self.flush_fail_warn_ms = now;
                log.err("shard {d}: sync flush failed: {s}; writes not on disk are not acked (persist_failures={d})", .{ self.id, @errorName(err), self.persist_failures });
            }
            // Alone, nothing else can make these writes durable: tell their
            // clients now rather than leave them waiting on the disk.
            if (raft.role == .leader and raft.peer_count == 0) {
                self.failPendingAbove(raft.durable_index, if (raft.writes_stopped) LOST_AT_RESTART else NOT_ON_DISK);
            }
            return;
        };
        raft.markDurable(through);
    }

    /// What a client is told when its write is in the log but its flush
    /// failed: it may still commit once the disk recovers.
    pub const NOT_ON_DISK = "unavailable: write not on disk (flush failed); it may still apply once the disk recovers — check before resending";

    /// The same, once the shard has stopped taking writes: nothing more
    /// reaches disk, so the write is gone when the node restarts.
    pub const LOST_AT_RESTART = "unavailable: write not on disk, and this shard stopped taking writes; it is lost when the node restarts — resend after the restart";

    fn failPendingAbove(self: *Shard, index: u64, message: []const u8) void {
        if (self.pending_count == 0) return;
        for (self.pending) |*slot| {
            if (!slot.active or slot.index <= index) continue;
            self.deliverDeferredResponse(slot.reply_to, slot.request_id, .unavailable, message);
            self.allocator.free(slot.bytes);
            slot.active = false;
            self.pending_count -= 1;
        }
    }

    fn inboxPending(ctx: *const anyopaque) u64 {
        const mailbox: *const Mailbox = @ptrCast(@alignCast(ctx));
        return mailbox.inbox.pending();
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
        if (self.shard_metrics) |sm| sm.live_pending = .{ .ctx = self.mailbox, .read = inboxPending };
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
        self.waiting_fds.deinit(self.allocator);
        self.pending_cancels.deinit(self.allocator);
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

        self.mailbox.destroy(self.allocator);
        self.reply_pool.deinit(self.allocator);
        self.waiter_pool.deinit(self.allocator);
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

    // ─── Connection management ───────────────────────────────────────────

    const WaitEntry = struct { fd: i32, seq: u32 };

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
        try self.waiting_fds.ensureTotalCapacity(self.allocator, 2 * (self.connections.count() + 1) + self.waiting_fds.items.len);
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
            self.endWait(kv.value);
            kv.value.deinit();
            self.allocator.destroy(kv.value);
            if (self.metrics_registry) |m| m.server.connectionClosed();
            if (self.shard_metrics) |sm| sm.connectionClosed();
        }
    }

    /// Remove connection, unregister from reactor, and close the fd.
    pub fn closeConnection(self: *Shard, fd: i32) void {
        log.debug("Shard {d} closing connection: fd={d}", .{ self.id, fd });
        if (self.connections.get(fd)) |conn| self.waiter_pool.removeByConnection(@intCast(self.id), fd, conn.id, null, @ptrCast(self));
        if (self.forward_count > 0) {
            if (self.connections.get(fd)) |conn| self.dropForwardsFor(fd, conn.id);
        }
        if (self.connections.get(fd)) |conn| {
            if (conn.blocking_in_flight > 0) self.releaseForwardsFor(conn);
        }
        self.reactor.removeSource(fd);
        self.removeConnection(fd);
        _ = std.c.close(fd);
    }

    /// A client closed with blocking reads out on other shards: each shard
    /// holding some is asked, once, to drop them and answer each, empty, so
    /// their slots come back within its next tick rather than at their
    /// block time — a client cannot hold another shard's waiters, or this
    /// shard's share of them, by connecting and leaving. The ask goes on
    /// this shard's share of that inbox, like the reads it cancels, so it
    /// never overtakes them; with none left it waits its turn
    /// (`sendPendingCancels`). The slots stay held until answered, so they
    /// never count for less than what is parked.
    fn releaseForwardsFor(self: *Shard, conn: *Connection) void {
        var told = std.StaticBitSet(@import("../config/server.zig").MAX_SHARDS).initEmpty();
        const seq: u64 = (@as(u64, conn.id) << 32) | @as(u32, @bitCast(conn.fd));
        for (conn.blocking_slots[0..conn.blocking_in_flight]) |t| {
            const target = self.reply_pool.orphan(t) orelse continue;
            if (told.isSet(target)) continue;
            told.set(target);
            if (self.sendCancel(target, seq)) continue;
            // At most one ask per slot that is still to be answered; past
            // that, or without memory, the reads end at their block time.
            if (self.pending_cancels.items.len < self.reply_pool.classes[@intFromEnum(reply_pool_mod.Class.blocking)].free.len) {
                self.pending_cancels.append(self.allocator, .{ .target = target, .seq = seq }) catch {};
            }
        }
        conn.blocking_in_flight = 0;
    }

    fn sendCancel(self: *Shard, target: u16, seq: u64) bool {
        const mailboxes = self.peer_mailboxes orelse return true;
        return mailboxes[target].sendOnShare(.{ .tag = .cancel_reads, .src_shard = @intCast(self.id), .sequence = seq });
    }

    /// Send the cancels that found no room in their target's inbox, now
    /// that some may have drained.
    fn sendPendingCancels(self: *Shard) void {
        var kept: usize = 0;
        for (self.pending_cancels.items) |c| {
            if (self.sendCancel(c.target, c.seq)) continue;
            self.pending_cancels.items[kept] = c;
            kept += 1;
        }
        self.pending_cancels.shrinkRetainingCapacity(kept);
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
                    self.sendErrorResponse(conn, req.header.request_id, .bad_request, "bad request: a blocking wait (block_ms/wait_ms, --block/--wait) is at most 300000 ms (5 minutes)");
                    return;
                }
            }
        }

        // Every namespace a request names meets the one rule here, before
        // anything runs: a write to an unknown namespace creates it.
        if (req.namespace.len > 0) {
            if (handler_mod.nameRefusal(req.namespace)) |why| {
                self.sendErrorResponse(conn, req.header.request_id, .bad_request, why);
                return;
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
        }
        // Apply what committed during the request: on a single node, what
        // its handler proposed on its own account (a dequeue's ack, an
        // implicit namespace create) and anything past the entry `park`
        // answered at. In a cluster these wait for a peer's ack.
        _ = self.applyCommitted();
    }

    /// Where a request goes when it goes to another shard, and whether it
    /// is a read that may wait there on purpose.
    const ForwardTo = struct { target: u16, blocking: bool };

    /// The shard `req` would be sent to, if not this one: `dispatchRequest`'s
    /// routing, decided before anything runs.
    fn forwardTarget(self: *Shard, req: proto.Request) ?ForwardTo {
        const op = req.header.op_code;
        if (op >= proto.MAX_OPCODES) return null;
        if (self.dispatcher.isWalkOp(op) and self.dispatcher.walk_contexts[op] != null) {
            const single = if (self.dispatcher.pre_route[op]) |f| f(req) != null else false;
            if (!single) return null;
        }
        return switch (self.resolveTarget(op, req)) {
            .local => null,
            .shard => |s| blk: {
                const mailboxes = self.peer_mailboxes orelse break :blk null;
                if (self.peer_shards == null or s.shard_id == self.id or s.shard_id >= mailboxes.len) break :blk null;
                break :blk .{ .target = s.shard_id, .blocking = blockingWait(req) != null };
            },
        };
    }

    /// Requests whose answer carries what they took: a dequeued message, a
    /// group read's records, a claimed or awaited task.
    fn takesItems(op: u16) bool {
        return switch (op) {
            @intFromEnum(proto.OpCode.queue_dequeue),
            @intFromEnum(proto.OpCode.stream_group_read),
            @intFromEnum(proto.OpCode.stream_group_claim),
            @intFromEnum(proto.OpCode.action_await),
            => true,
            else => false,
        };
    }

    /// How long a read may wait on purpose for its data, or null for any
    /// other request. Such a read holds a slot of its own class, and is not
    /// a slow answer while it waits. A worker's await waits 30 s unless
    /// told otherwise (`ActionsHandler`).
    fn blockingWait(req: proto.Request) ?u64 {
        if (req.getBlockMs() orelse req.getWaitMs()) |ms| return ms;
        if (req.header.op_code != @intFromEnum(proto.OpCode.action_await)) return null;
        const opt = req.findOption(.block_ms) orelse return 30_000;
        const ms = opt.asU32() orelse 30_000;
        return if (ms == 0) null else ms;
    }

    /// Why `conn`'s request cannot go to `to.target` now, or null when it
    /// can.
    fn forwardWait(self: *Shard, conn: *Connection, to: ForwardTo) ?Connection.WaitReason {
        if (to.target == self.id) return if (self.blockingOut(conn) >= Connection.MAX_BLOCKING_IN_FLIGHT) .blocking_reads else null;
        if (conn.forwards_in_flight >= Connection.MAX_FORWARDS_IN_FLIGHT) return .in_flight;
        if (to.blocking and self.blockingOut(conn) >= Connection.MAX_BLOCKING_IN_FLIGHT) return .blocking_reads;
        if (!self.reply_pool.canTake(if (to.blocking) .blocking else .ordinary, to.target)) return .slots;
        if (!self.peer_mailboxes.?[to.target].hasShare(@intCast(self.id))) return .inbox;
        return null;
    }

    /// The blocking reads `conn` has out: on other shards, and parked here.
    /// `local_reads` only ever overcounts — raised as each parks, not
    /// lowered as each returns — so the waiters are counted only when it
    /// says the cap may be reached.
    fn blockingOut(self: *Shard, conn: *Connection) u16 {
        if (conn.blocking_in_flight + conn.local_reads >= Connection.MAX_BLOCKING_IN_FLIGHT) {
            conn.local_reads = self.waiter_pool.countFor(@intCast(self.id), conn.fd, conn.id);
        }
        return conn.blocking_in_flight + conn.local_reads;
    }

    /// What a client's request must wait for before it can go — to another
    /// shard, or, for a blocking read here, past this connection's cap — or
    /// null when it can go now, or is refused by `dispatchRequest`. A
    /// waiting request is left unread: nothing about it has run or been
    /// counted.
    fn holdForward(self: *Shard, conn: *Connection, req: proto.Request) ?Connection.Waiting {
        if ((req.getBlockMs() orelse 0) > waiter_pool_mod.MAX_BLOCK_MS or (req.getWaitMs() orelse 0) > waiter_pool_mod.MAX_BLOCK_MS) return null;
        const to: ForwardTo = self.forwardTarget(req) orelse if (blockingWait(req) != null) .{ .target = @intCast(self.id), .blocking = true } else return null;
        var why = self.stillWaits(conn, to, null);
        // Behind connections already waiting for this target, unless this
        // is the one request `resumeWaiting` let this connection send: room
        // goes round, a request at a time.
        if (why == null and !conn.has_turn and self.queued[@intFromBool(to.blocking)][to.target] > 0) why = .slots;
        return .{ .target = to.target, .blocking = to.blocking, .reason = why orelse return null };
    }

    /// `forwardWait`; but when the answer is, or last was (`was`), the inbox
    /// share, ask to be told of a drain before looking, so a drain in
    /// between is not missed (`Mailbox.wantShare`).
    fn stillWaits(self: *Shard, conn: *Connection, to: ForwardTo, was: ?Connection.WaitReason) ?Connection.WaitReason {
        if (to.target == self.id) return self.forwardWait(conn, to);
        const mailbox = self.peer_mailboxes.?[to.target];
        if (was == .inbox) mailbox.wantShare(@intCast(self.id));
        const why = self.forwardWait(conn, to);
        if (why != .inbox or was == .inbox) return why;
        mailbox.wantShare(@intCast(self.id));
        return self.forwardWait(conn, to);
    }

    pub const Dispatching = struct { reply_to: ReplyTo, request_id: u64, answered_deferred: bool = false };

    /// Run `req` on `conn` (`local`: skip `dispatchRequest`'s checks), noting
    /// it as the request being dispatched while it runs. Returns whether it
    /// was answered through `deliverDeferred` meanwhile; see
    /// `answeredAsDeferred`.
    fn dispatchNoted(self: *Shard, conn: *Connection, req: proto.Request, comptime local: bool) bool {
        const prev = self.dispatching;
        self.dispatching = .{ .reply_to = conn.replyTo(), .request_id = req.header.request_id };
        defer self.dispatching = prev;
        if (local) self.dispatchLocal(conn, req) else self.dispatchRequest(conn, req);
        return self.dispatching.?.answered_deferred;
    }

    /// A request that left nothing on its connection and wasn't parked, yet
    /// was answered through `deliverDeferred` while it ran: its handler
    /// answered it as deferred without saying so. It has its answer, so the
    /// no-answer fallback is skipped rather than sending a second under the
    /// same id, which would put the client's reads a frame behind.
    fn answeredAsDeferred(self: *Shard, req: proto.Request) void {
        self.answered_as_deferred += 1;
        log.err("shard {d}: request {d} (op 0x{x}) was answered as deferred without being parked. This is a bug", .{ self.id, req.header.request_id, req.header.op_code });
        if (builtin.mode == .Debug and !builtin.is_test) @panic("a request was answered as deferred without being parked");
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
            if (conn.replyTo() == .remote) {
                // Already forwarded once; a second hop during an election
                // could bounce between nodes. The client retries instead.
                self.sendErrorResponse(conn, req.header.request_id, .unavailable, "unavailable: electing a leader — retry");
                return;
            }
            self.forwardToLeader(conn, req);
            return;
        }
        // Requests forwarded from another node arrive here without passing
        // `dispatchRequest`'s check.
        if (req.namespace.len > 0) {
            if (handler_mod.nameRefusal(req.namespace)) |why| {
                self.sendErrorResponse(conn, req.header.request_id, .bad_request, why);
                return;
            }
        }
        // A write that brings data into a namespace this shard has not seen
        // creates it. Checked after forwarding because only the leader knows
        // the creates in flight; the create is proposed here, ahead of the
        // write, so it holds the room before the next request is admitted.
        // Other writes (a delete, an ack, a wait for what is not there yet)
        // create nothing, wherever they are sent, and nor does a request
        // that names nothing to write to. A job submit names its targets in
        // its definition: its handler reserves once the definition passes.
        if (req.header.op_code < proto.MAX_OPCODES and dispatcher_mod.opCreates(@enumFromInt(req.header.op_code))) {
            if (self.namespace_handler.admission(req.namespace)) |r| {
                self.sendErrorResponse(conn, req.header.request_id, r.status, r.message);
                return;
            }
            if (req.key.len > 0) self.namespace_handler.proposeImplicitCreate(req.namespace, self, false);
        }
        self.dispatcher.dispatch(@ptrCast(self), @ptrCast(conn), req);
    }

    /// Pure routing decision: pre-route → the shard that owns the key. No
    /// side effects.
    fn resolveTarget(self: *Shard, op: u16, req: proto.Request) node_router.RouteTarget {
        if (op >= proto.MAX_OPCODES) return .{ .local = .{ .partition_id = 0 } };
        const hash = if (self.dispatcher.pre_route[op]) |f| f(req) orelse return .{ .local = .{ .partition_id = 0 } } else return .{ .local = .{ .partition_id = 0 } };
        return self.router.route(hash);
    }

    /// The request as wire bytes on the heap, for a copy that outlives the
    /// connection's buffer: a forward to another shard or to the leader, or a
    /// write held until commit. Length and CRC are recomputed.
    fn serializeRequest(allocator: std.mem.Allocator, req: proto.Request) ![]u8 {
        // Sized from the parts, not header.payload_length: a client may omit
        // the options trailer, which serialize always writes.
        const size = @sizeOf(proto.RequestHeader) + 2 + req.namespace.len + 2 + req.key.len + 4 + req.value.len + 2 + req.options.len;
        const buf = try allocator.alloc(u8, size);
        errdefer allocator.free(buf);
        _ = try req.serialize(buf);
        return buf;
    }

    /// Forward a request to a different shard in single-node mode.
    ///
    /// Marshals the request to the target shard's inbox as a `forward_request`.
    /// The target shard re-parses and dispatches the request on its own
    /// reactor thread (its handlers' state is therefore only ever touched by
    /// one thread). The response — direct bytes or a deferred blocking-read
    /// completion — comes back on this shard's reply ring naming the reply
    /// slot taken here first, and is written to the client. See
    /// `runForwardedRequest` and `deliverReply`.
    ///
    /// A client's request has already waited for room (`holdForward`); if
    /// there is none even so, it is answered `overloaded` — it did not run —
    /// and the client retries instead of hanging.
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
        const target = (self.peer_mailboxes orelse return self.dispatchLocal(conn, req))[target_shard_id];
        // A cluster member runs one shard (`Runtime` refuses more), so what
        // goes to another shard never waits there for a leader first; the
        // deadlines below count on it.
        std.debug.assert(self.raft_network == null);

        // Room for the answer first: nothing is copied for a request that
        // cannot be sent.
        const block = blockingWait(req);
        const blocking = block != null;
        const op = req.header.op_code;
        const claims = takesItems(op);
        const writes = claims or (op < proto.MAX_OPCODES and dispatcher_mod.opWrites(@enumFromInt(op)));
        // An answer carrying what its request took is not dropped for a
        // deadline: it would reach no one until its lease ran out. Such a
        // request waits for it as long as the slot is held.
        const class: reply_pool_mod.Class = if (blocking) .blocking else .ordinary;
        const now = nowMs();
        const since = if (claims) now else conn.head_since_ms orelse now;
        const allowance = if (claims) reply_pool_mod.SLOT_DEADLINE_MS else reply_pool_mod.DEADLINE_MS;
        const ticket = self.reply_pool.take(class, target_shard_id, conn.fd, conn.id, req.header.request_id, writes, since, now, allowance, if (claims) 0 else block orelse 0) catch
            return self.refuseForward(conn, req, target_shard_id, .target_busy);

        // Serialize request (header + recomposed payload) onto the heap so the
        // target shard can re-parse it after the source buffer is gone.
        const buf = serializeRequest(self.allocator, req) catch {
            _ = self.reply_pool.release(ticket, target_shard_id);
            self.sendErrorResponse(conn, req.header.request_id, .internal_error, "internal error: out of memory forwarding the request — retry");
            return;
        };

        // Pack (conn_id << 32) | fd into sequence: the target shard runs the
        // request on a proxy with this identity (`loadProxy`).
        const fd_bits: u64 = @as(u32, @bitCast(conn.fd));
        const seq: u64 = (@as(u64, conn.id) << 32) | fd_bits;
        var msg: InboxMessage = .{
            .tag = .forward_request,
            .src_shard = @intCast(self.id),
            .payload_len = @intCast(buf.len),
            .sequence = seq,
            .payload_ptr = buf.ptr,
        };
        msg.setReplySlot(ticket.slot, ticket.gen);
        if (!target.sendOnShare(msg)) {
            self.allocator.free(buf);
            _ = self.reply_pool.release(ticket, target_shard_id);
            return self.refuseForward(conn, req, target_shard_id, .inbox_full);
        }
        conn.forwards_in_flight += 1;
        if (blocking) {
            // `holdForward` kept it under the cap, or it would have waited.
            std.debug.assert(conn.blocking_in_flight < Connection.MAX_BLOCKING_IN_FLIGHT);
            conn.blocking_slots[conn.blocking_in_flight] = ticket;
            conn.blocking_in_flight += 1;
        }
        if (self.shard_metrics) |sm| sm.setCrossShardInFlight(self.reply_pool.taken());

        // The answer arrives on this shard's reply ring; marked deferred so
        // the no-answer fallback leaves the connection waiting for it.
        conn.recordForward();
        conn.response_deferred = true;
    }

    const ForwardRefusal = enum { target_busy, inbox_full };

    const NOT_KEEPING_UP = "overloaded: shard {d} is not keeping up; this request was not run — back off and retry";

    /// A client's request refused before it was sent to `target`: answered
    /// `overloaded` (it did not run, so resending is safe), counted, and
    /// said once per interval.
    fn refuseForward(self: *Shard, conn: *Connection, req: proto.Request, target: u16, why: ForwardRefusal) void {
        if (self.shard_metrics) |sm| sm.recordCrossShardOverloaded(.client_forward);
        var msg_buf: [128]u8 = undefined;
        const message = std.fmt.bufPrint(&msg_buf, NOT_KEEPING_UP, .{target}) catch "overloaded: this request was not run — back off and retry";
        self.sendErrorResponse(conn, req.header.request_id, .overloaded, message);
        self.forwards_refused += 1;
        const now = nowMs();
        if (now -| self.refuse_warn_ms < WARN_INTERVAL_MS) return;
        self.refuse_warn_ms = now;
        const reason = switch (why) {
            .target_busy => "it holds its full share of this shard's requests",
            .inbox_full => "its inbox is full of messages sent on no share",
        };
        log.warn("shard {d}: refusing client requests for shard {d}: {s} ({d} refused so far; {d} in flight to other shards, {d} ordinary and {d} blocking to shard {d})", .{ self.id, target, reason, self.forwards_refused, self.reply_pool.taken(), self.reply_pool.heldBy(.ordinary, target), self.reply_pool.heldBy(.blocking, target), target });
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

    /// Take the mailbox (called each reactor tick): the wakes, every reply,
    /// then at most one inbox's worth of requests — a flood of requests must
    /// not starve the shard's I/O and Raft heartbeats. What is left keeps the
    /// next tick from sleeping (`Mailbox.prepareSleep`). Replies go first and
    /// in full: each was reserved before its request was sent, so the ring
    /// holds no more than this shard asked for.
    pub fn drainInbox(self: *Shard) usize {
        const flags = self.mailbox.wake.take();
        if (flags & mailbox_mod.Flag.stream_appended.bit() != 0) self.workflow_handler.triggers_dirty = true;
        if (flags & mailbox_mod.Flag.action_invoked.bit() != 0) self.waiter_pool.notifyAny(.action_await, ActionsHandler.resolveActionAwaitFn, @ptrCast(self));

        var buf: [64]InboxMessage = undefined;
        while (true) {
            const count = self.mailbox.replies.drain(&buf);
            if (count == 0) break;
            for (buf[0..count]) |msg| self.handleInboxMessage(msg);
        }

        const inbox = &self.mailbox.inbox;
        var total: usize = 0;
        while (total < inbox.capacity) {
            const count = inbox.drain(buf[0..@min(buf.len, inbox.capacity - total)]);
            if (count == 0) break;
            for (buf[0..count]) |msg| {
                if (self.mailbox.returnShare(msg)) {
                    if (self.peer_mailboxes) |peers| peers[msg.src_shard].wake.set(.share_returned);
                }
                self.handleInboxMessage(msg);
                total += 1;
            }
        }
        const left = inbox.pending();

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
            .action_start => self.startActionRun(msg),
            .reply => self.deliverReply(msg),
            .forward_request => self.runForwardedRequest(msg),
            .cancel_reads => {
                const fd: i32 = @bitCast(@as(u32, @truncate(msg.sequence)));
                const conn_id: u32 = @truncate(msg.sequence >> 32);
                // Each dropped read is answered, empty: the asker holds its
                // slot until an answer comes.
                self.waiter_pool.removeByConnection(msg.src_shard, fd, conn_id, answerCancelled, @ptrCast(self));
            },
        }
    }

    /// Push-wake stream triggers after a stream append. The trigger for a stream
    /// lives on the workflow-definition's shard, which is usually NOT the shard
    /// that owns the stream's data — so we mark the local handler dirty (covers
    /// the co-located case) and set every peer's `stream_appended` flag, whose
    /// next tick force-polls its triggers. A flag, not a message: a burst of
    /// appends is one wake, and it can never fill an inbox. No-op when no
    /// trigger exists on the node, so streams that nobody watches incur zero
    /// cross-shard chatter.
    pub fn notifyStreamTriggers(self: *Shard) void {
        if (!WorkflowHandler.anyStreamTriggers()) return;

        self.workflow_handler.triggers_dirty = true;

        if (self.peer_mailboxes) |mailboxes| {
            for (mailboxes, 0..) |mb, i| {
                if (i == self.id) continue;
                mb.wake.set(.stream_appended);
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
    /// (blocking) responses use the address captured at registration time
    /// and round-trip through `deliverDeferred`.
    fn runForwardedRequest(self: *Shard, msg: InboxMessage) void {
        const ptr = msg.payload_ptr orelse return;
        const data: [*]u8 = @ptrCast(ptr);
        defer if (msg.payload_len > 0) self.allocator.free(data[0..msg.payload_len]);
        if (msg.payload_len == 0) return;

        const bytes = data[0..msg.payload_len];
        const fd: i32 = @bitCast(@as(u32, @truncate(msg.sequence)));
        const conn_id: u32 = @truncate(msg.sequence >> 32);
        const held = msg.replySlot();
        const reply_to: ReplyTo = .{ .socket = .{ .shard = msg.src_shard, .fd = fd, .conn_id = conn_id, .slot = held.slot, .gen = held.gen } };

        // A forwarded request that fails to parse is still answered: its
        // client is waiting on it.
        const req = proto.Request.parse(bytes) catch {
            const request_id: u64 = if (bytes.len >= 16) std.mem.readInt(u64, bytes[8..16], .little) else 0;
            var err_buf: [256]u8 = undefined;
            // The owning shard parsed it before forwarding, so the fault is
            // the server's, not the client's.
            const serialized = proto.Response.serializeNew(.internal_error, request_id, "internal error: another shard could not read this request", &err_buf) catch return;
            self.deliverDeferred(reply_to, serialized);
            return;
        };

        const proxy = self.forward_proxy;
        loadProxy(proxy, reply_to);
        proxy.protocol = .binary;

        proxy.recordRequest();
        const answered_deferred = self.dispatchNoted(proxy, req, true);
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
            self.deliverDeferred(reply_to, answer.bytes());
        } else if (!proxy.response_deferred and answered_deferred) {
            self.answeredAsDeferred(req);
        } else if (!proxy.response_deferred) {
            const serialized = proto.Response.serializeNew(.internal_error, req.header.request_id, self.noAnswer(req), &err_buf) catch return;
            self.deliverDeferred(reply_to, serialized);
        }
    }

    /// Ready a proxy to run a request whose answer goes to `reply_to`. The
    /// proxy also takes the requester's fd and generation as its own, so
    /// per-connection state kept by id (a KV transaction's owner) belongs to
    /// that requester.
    fn loadProxy(proxy: *Connection, reply_to: ReplyTo) void {
        proxy.proxy_for = reply_to;
        switch (reply_to) {
            .socket => |s| {
                proxy.fd = s.fd;
                proxy.id = s.conn_id;
            },
            .remote => |r| {
                proxy.fd = @bitCast(r.forward_id);
                proxy.id = r.node;
            },
        }
        proxy.state = .active;
        proxy.response_deferred = false;
        proxy.write_buf.read_pos = 0;
        proxy.write_buf.write_pos = 0;
        proxy.write_overflow = false;
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

    /// The answer to a request this shard's client sent another shard. It
    /// frees the slot it names; one that is not the slot's current answer (a
    /// second answer, one for a slot reused since, or one from a shard it was
    /// not asked of) is dropped, so a client never hears twice about one
    /// request. An answer with no bytes is the other shard saying it could
    /// not send one. The connection's generation guards fd reuse: a client
    /// gone since hears nothing.
    fn deliverReply(self: *Shard, msg: InboxMessage) void {
        const bytes: []u8 = if (msg.payload_ptr) |p| @as([*]u8, @ptrCast(p))[0..msg.payload_len] else &.{};
        defer if (bytes.len > 0) self.allocator.free(bytes);
        const held = msg.replySlot();
        const slot = self.reply_pool.release(.{ .slot = held.slot, .gen = held.gen }, msg.src_shard) orelse return;
        if (self.shard_metrics) |sm| sm.setCrossShardInFlight(self.reply_pool.taken());
        if (slot.answered) return; // its client was told at the deadline
        const conn = self.forwardAnswered(slot) orelse return;
        if (bytes.len == 0) {
            var buf: [128]u8 = undefined;
            const message = std.fmt.bufPrint(&buf, "internal error: shard {d} lost its answer — the request may still apply", .{msg.src_shard}) catch "internal error: the answer was lost — the request may still apply";
            return self.sendErrorResponse(conn, slot.request_id, .internal_error, message);
        }
        _ = conn.queueWrite(bytes);
        self.flushToClient(slot.fd);
    }

    /// A forwarded request's client is answered, by the other shard or at
    /// its deadline: the client, if still connected, has one fewer request
    /// in flight.
    fn forwardAnswered(self: *Shard, slot: ReplyPool.Slot) ?*Connection {
        const conn = self.getConnection(slot.fd) orelse return null; // connection gone
        if (conn.id != slot.conn_id) return null; // fd reused for a new connection
        conn.forwards_in_flight -|= 1;
        if (slot.class == .blocking) {
            const held = conn.blocking_slots[0..conn.blocking_in_flight];
            for (held, 0..) |t, i| {
                if (t.slot != slot.index) continue;
                held[i] = held[held.len - 1];
                conn.blocking_in_flight -= 1;
                break;
            }
        }
        return conn;
    }

    /// Answer the clients whose requests another shard has not answered by
    /// their deadline, and take back slots held past `SLOT_DEADLINE_MS`:
    /// whatever lost the answer, neither the client nor the slot waits for
    /// ever. Swept every quarter second, so a client is told at most about
    /// that long after its deadline.
    fn expireReplySlots(self: *Shard) void {
        const now = nowMs();
        if (now < self.reply_sweep_ms) return;
        self.reply_sweep_ms = now + 250;
        const Expired = struct {
            shard: *Shard,
            count: u64 = 0,
            /// Expired per target, to name the one with the most.
            per_target: [@import("../config/server.zig").MAX_SHARDS]u32 = @splat(0),
            pub fn late(ctx: *@This(), slot: ReplyPool.Slot) void {
                ctx.count += 1;
                ctx.per_target[slot.target] += 1;
                if (ctx.shard.shard_metrics) |sm| sm.recordCrossShardTimeout();
                const conn = ctx.shard.forwardAnswered(slot) orelse return;
                const after = slot.answer_after_ms / 1000;
                var buf: [128]u8 = undefined;
                const message = if (slot.writes)
                    std.fmt.bufPrint(&buf, "unavailable: shard {d} did not answer in {d} s — the request may still apply", .{ slot.target, after }) catch "unavailable: no answer — the request may still apply"
                else
                    std.fmt.bufPrint(&buf, "unavailable: shard {d} did not answer in {d} s — retry", .{ slot.target, after }) catch "unavailable: no answer — retry";
                ctx.shard.sendErrorResponse(conn, slot.request_id, .unavailable, message);
                // Nothing else may touch an idle client's connection soon.
                ctx.shard.flushToClient(slot.fd);
            }
        };
        var gone = Expired{ .shard = self };
        const oldest = self.reply_pool.expire(now, &gone);
        if (self.shard_metrics) |sm| {
            sm.setCrossShardInFlight(self.reply_pool.taken());
            sm.setOldestCrossShardWait(if (oldest) |t| (now -| t) / 1000 else 0);
        }
        if (gone.count == 0) return;
        self.slots_expired += gone.count;
        if (now -| self.expire_warn_ms < WARN_INTERVAL_MS) return;
        self.expire_warn_ms = now;
        const worst = std.mem.indexOfMax(u32, &gone.per_target);
        log.warn("shard {d}: since the last report, {d} client requests to other shards went unanswered past their deadline (most in this sweep: {d}, to shard {d}); their clients were told", .{ self.id, self.slots_expired, gone.per_target[worst], worst });
        self.slots_expired = 0;
    }

    /// Every frame the network queued since the last drain.
    fn drainRaftQueue(self: *Shard) void {
        const q = self.raft_queue orelse return;
        while (q.pop()) |frame| {
            defer self.allocator.free(frame.payload);
            // The clock before each message: a vote that arrives past the
            // candidacy's deadline is not counted on an older clock, even
            // after a stall part way through the queue.
            self.raft_node.observeTime(nowMs());
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
                self.sendErrorResponse(conn, req.header.request_id, .internal_error, persistence_mod.COMMITTED_NOT_APPLIED);
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
        slot.* = .{ .active = true, .index = proposed.index, .term = proposed.term, .reply_to = conn.replyTo(), .request_id = req.header.request_id, .bytes = bytes, .responder = responder };
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
            self.deliverDeferredResponse(slot.reply_to, slot.request_id, .unavailable, "unavailable: leader changed, write not applied — retry");
            return;
        }
        if (!applied) {
            self.deliverDeferredResponse(slot.reply_to, slot.request_id, .internal_error, persistence_mod.COMMITTED_NOT_APPLIED);
            return;
        }
        const req = proto.Request.parse(slot.bytes) catch {
            self.deliverDeferredResponse(slot.reply_to, slot.request_id, .internal_error, persistence_mod.ANSWER_LOST);
            return;
        };
        const proxy = self.respond_proxy;
        loadProxy(proxy, slot.reply_to);
        self.answering_index = index;
        self.answering_timestamp_ns = timestamp_ns;
        slot.responder(@ptrCast(self), @ptrCast(proxy), req);
        var err_buf: [256]u8 = undefined;
        if (self.takeProxyAnswer(proxy, slot.request_id, &err_buf)) |answer| {
            defer answer.free(self.allocator);
            self.deliverDeferred(slot.reply_to, answer.bytes());
        } else if (!proxy.response_deferred) {
            self.deliverDeferredResponse(slot.reply_to, slot.request_id, .internal_error, persistence_mod.ANSWER_LOST);
        }
    }

    fn resolvePending(self: *Shard, message: []const u8) void {
        if (self.pending_count == 0) return;
        log.warn("shard {d}: {d} write(s) were waiting for commit; answering each: {s}", .{ self.id, self.pending_count, message });
        for (self.pending) |*slot| {
            if (!slot.active) continue;
            self.deliverDeferredResponse(slot.reply_to, slot.request_id, .unavailable, message);
            self.allocator.free(slot.bytes);
            slot.active = false;
            self.pending_count -= 1;
        }
    }

    // ─── Writes on a node that does not lead ─────────────────────────────

    /// Send a client's write to the leader, and
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
        slot.* = .{ .active = true, .id = self.next_forward_id, .reply_to = conn.replyTo(), .request_id = req.header.request_id, .bytes = bytes, .deadline_ms = nowMs() + FORWARD_TIMEOUT_MS };
        self.next_forward_id +%= 1;
        if (self.next_forward_id == 0) self.next_forward_id = FORWARD_ID_FIRST;
        self.forward_count += 1;
        conn.recordForward();
        conn.response_deferred = true;
        self.sendForward(slot);
    }

    /// Sent only over a link that is up, and marked with that link's
    /// session: an answer that has not come when the session changes went
    /// over a link that is gone. A full forward queue to the leader answers
    /// the client now: nothing was sent, so nothing ran.
    fn sendForward(self: *Shard, f: *Forward) void {
        const raft = self.raft_node;
        const leader = raft.leader_id;
        if (leader == 0 or leader == self.cluster_node_id) return;
        const rn = self.raft_network orelse return;
        const session = rn.linkSession(leader) orelse return;
        var buf: [FORWARD_PREFIX + MAX_REQUEST_SIZE]u8 = undefined;
        if (FORWARD_PREFIX + f.bytes.len > buf.len) {
            self.finishForward(f, .internal_error, "request too large to forward");
            return;
        }
        std.mem.writeInt(u32, buf[0..4], f.id, .little);
        // Fields carried beside the client's bytes, never inside them; none
        // are sent yet.
        std.mem.writeInt(u16, buf[4..6], 0, .little);
        @memcpy(buf[FORWARD_PREFIX..][0..f.bytes.len], f.bytes);
        switch (rn.sendForward(leader, .forward_write, self.id, buf[0 .. FORWARD_PREFIX + f.bytes.len], session)) {
            .queued => {},
            .full => return self.finishForward(f, .overloaded, "overloaded: the link to the leader is full; this request was not run — retry"),
            .not_sent => return,
        }
        f.sent_to = leader;
        f.sent_term = raft.current_term;
        f.sent_session = session;
        f.sent_ms = nowMs();
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
                self.deliverDeferred(f.reply_to, reply);
                self.dropForward(f);
                continue;
            }
            if (f.sent_to != 0) {
                if (raft.current_term != f.sent_term or (leader != 0 and leader != f.sent_to)) {
                    self.finishForward(f, .unavailable, "unavailable: lost leadership before commit — write may still apply");
                } else if (self.raft_network != null and self.raft_network.?.linkSession(f.sent_to) != f.sent_session) {
                    // Gone, or gone and back between two ticks: either way the
                    // link it went over is not the one up now.
                    self.finishForward(f, .unavailable, "unavailable: lost the link to the leader — write may still apply");
                } else if (now -| f.sent_ms >= SENT_FORWARD_BACKSTOP_MS) {
                    self.finishForward(f, .unavailable, "unavailable: no answer from the leader — write may still apply");
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
        const reply_to = f.reply_to;
        const request_id = f.request_id;
        self.dropForward(f);
        defer self.allocator.free(bytes);
        const req = proto.Request.parse(bytes) catch {
            self.deliverDeferredResponse(reply_to, request_id, .internal_error, "internal error: the held write did not parse and was not written — retry");
            return;
        };
        const proxy = self.respond_proxy;
        loadProxy(proxy, reply_to);
        const answered_deferred = self.dispatchNoted(proxy, req, true);
        var err_buf: [256]u8 = undefined;
        if (self.takeProxyAnswer(proxy, request_id, &err_buf)) |answer| {
            defer answer.free(self.allocator);
            self.deliverDeferred(reply_to, answer.bytes());
        } else if (!proxy.response_deferred and answered_deferred) {
            self.answeredAsDeferred(req);
        } else if (!proxy.response_deferred) {
            self.deliverDeferredResponse(reply_to, request_id, .internal_error, self.noAnswer(req));
        }
    }

    /// The client is gone: nothing it was waiting for needs a slot.
    fn dropForwardsFor(self: *Shard, fd: i32, conn_id: u32) void {
        for (self.forwards) |*f| {
            if (f.active and f.reply_to.isSocket(@intCast(self.id), fd, conn_id)) self.dropForward(f);
        }
    }

    fn finishForward(self: *Shard, f: *Forward, status: proto.StatusCode, message: []const u8) void {
        self.deliverDeferredResponse(f.reply_to, f.request_id, status, message);
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
        if (frame.payload.len < FORWARD_PREFIX) return self.badFrame(frame);
        const id = std.mem.readInt(u32, frame.payload[0..4], .little);
        const fields_len = std.mem.readInt(u16, frame.payload[4..6], .little);
        // Fields this node does not know how to honour: running the request
        // without them would ignore what the forwarder meant.
        if (fields_len != 0) {
            self.badFrame(frame);
            const body = frame.payload[@min(frame.payload.len, FORWARD_PREFIX + @as(usize, fields_len))..];
            const request_id: u64 = if (body.len >= 16) std.mem.readInt(u64, body[8..16], .little) else 0;
            var err_buf: [256]u8 = undefined;
            const serialized = proto.Response.serializeNew(.internal_error, request_id, "internal error: the leader cannot read this forwarded request — are all nodes on the same version?", &err_buf) catch return;
            return self.sendForwardReply(frame.source_node, id, serialized);
        }
        const req = proto.Request.parse(frame.payload[FORWARD_PREFIX..]) catch {
            // The forwarder holds its client until this is answered.
            self.badFrame(frame);
            const body = frame.payload[FORWARD_PREFIX..];
            const request_id: u64 = if (body.len >= 16) std.mem.readInt(u64, body[8..16], .little) else 0;
            var err_buf: [256]u8 = undefined;
            // The forwarder parsed it first, so the likely cause is two
            // nodes running different versions.
            const serialized = proto.Response.serializeNew(.internal_error, request_id, "internal error: the leader could not read this request — are all nodes on the same version?", &err_buf) catch return;
            return self.sendForwardReply(frame.source_node, id, serialized);
        };
        const proxy = self.forward_proxy;
        loadProxy(proxy, .{ .remote = .{ .node = frame.source_node, .forward_id = id } });
        proxy.protocol = .binary;
        proxy.recordRequest();
        const answered_deferred = self.dispatchNoted(proxy, req, true);
        var err_buf: [256]u8 = undefined;
        if (self.takeProxyAnswer(proxy, req.header.request_id, &err_buf)) |answer| {
            defer answer.free(self.allocator);
            self.sendForwardReply(frame.source_node, id, answer.bytes());
        } else if (!proxy.response_deferred and answered_deferred) {
            self.answeredAsDeferred(req);
        } else if (!proxy.response_deferred) {
            const serialized = proto.Response.serializeNew(.internal_error, req.header.request_id, self.noAnswer(req), &err_buf) catch return;
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
        const rn = self.raft_network orelse return;
        // A reply that cannot be queued is lost; the forwarder's backstop
        // answers its client.
        switch (rn.sendForward(peer, .forward_reply, self.id, buf, 0)) {
            .queued => {},
            .full, .not_sent => {
                const now = nowMs();
                if (now -| self.forward_reply_warn_ms >= WARN_INTERVAL_MS) {
                    self.forward_reply_warn_ms = now;
                    log.warn("shard {d}: could not queue a forwarded write's answer for node {d} (its link is full or down); that client is answered by its node's backstop", .{ self.id, peer });
                }
            },
        }
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
            self.deliverDeferred(f.reply_to, bytes);
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
            self.deliverDeferred(f.reply_to, f.reply.?);
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
                    raft.markDurable(raft.log.lastIndex());
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
            .term_check => {
                const req = transport.deserializeTermCheck(frame.payload) orelse return self.badFrame(frame);
                if (req.from != frame.source_node) return self.impostorFrame(frame, req.from);
                var buf: [transport.TERM_CHECK_RESP_MAX]u8 = undefined;
                const n = transport.serializeTermCheckResponse(raft.handleTermCheck(req), &buf) orelse return;
                self.sendRaft(frame.source_node, .term_check_response, buf[0..n]);
            },
            .term_check_response => {
                const resp = transport.deserializeTermCheckResponse(frame.payload) orelse return self.badFrame(frame);
                if (resp.from != frame.source_node) return self.impostorFrame(frame, resp.from);
                const was = raft.lost_log;
                raft.handleTermCheckResponse(resp);
                if (was != .none and raft.lost_log == .none) log.info("shard {d}: lost-log guard done; term {d} confirmed, voting from term {d} on", .{ self.id, raft.current_term, raft.current_term + 1 });
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
        log.err("shard {d}: node {d} (term {d}) disagrees with history this node committed and applied; this node's data can no longer be trusted and it has stopped taking part in the group. Stop it, move its data directory aside, and start it again with --join <a live member> (not --cluster): it rejoins with no log and votes only once it has caught up and a quorum has confirmed the term", .{ self.id, leader, term });
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
                self.deliverDeferred(f.reply_to, reply);
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
                    // Alone, the win commits the whole log.
                    if (!self.applyCommitted()) log.err("shard {d}: a committed entry could not be applied", .{self.id});
                }
            }
        }
        if (r.send_term_check) {
            var buf: [transport.TERM_CHECK_SIZE]u8 = undefined;
            const n = transport.serializeTermCheck(raft.termCheckRequest(), &buf).?;
            var ids: [raft_node_mod.MAX_PEERS + 1]u32 = undefined;
            for (raft.termCheckTargets(&ids)) |peer| self.sendRaft(peer, .term_check, buf[0..n]);
        }
        if (raft.lost_log != .none) self.warnLostLog(now);
        if (r.step_down) self.leadershipLost("no contact with a majority");
        if (raft.role == .leader) self.pump(now);
        if (self.joinWanted(now)) self.askToJoin(now);
        if (self.forward_count > 0) self.sweepForwards(now);
    }

    /// A guarded node that stays guarded says where it is waiting: with no
    /// leader (a majority lost its data too) or not enough members
    /// answering, it waits forever, and the operator needs the way out.
    fn warnLostLog(self: *Shard, now: u64) void {
        if (now -| self.lost_log_warn_ms < WARN_INTERVAL_MS) return;
        // The first line waits one warn interval: catching up usually
        // takes less.
        if (self.lost_log_warn_ms == 0) {
            self.lost_log_warn_ms = now;
            return;
        }
        self.lost_log_warn_ms = now;
        const raft = self.raft_node;
        switch (raft.lost_log) {
            .none => {},
            .catching_up => if (raft.leader_id == 0) {
                log.warn("shard {d}: lost-log guard: catching up — no leader heard yet; this node votes only once a leader has caught it up and the members have confirmed the term. If a majority lost their data, see flo server inspect / force-members", .{self.id});
            } else {
                log.warn("shard {d}: lost-log guard: catching up with leader {d} (term {d}, at index {d}); no votes until caught up and confirmed", .{ self.id, raft.leader_id, raft.current_term, raft.log.lastIndex() });
            },
            .confirming => {
                const st = raft.checkStatus();
                log.warn("shard {d}: lost-log guard: confirming term {d}; {d} member(s) have answered and {d} more answer(s) are needed. If a majority lost their data, see flo server inspect / force-members", .{ self.id, raft.current_term, st.answers, st.missing });
            },
        }
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
    /// after each client request (its own client's or one forwarded from
    /// another shard), and in the tick; `park` applies up to its own entry through
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
        // With sync durability a leader's writes commit only once on disk:
        // flushing first lets what that commits apply now.
        self.syncFlushIfNeeded();
        // Called after every request: nothing to apply is the common case.
        if (self.raft_node.last_applied >= @min(self.raft_node.commit_index, limit) and self.replies_held == 0) return true;
        self.applying = true;
        defer self.applying = false;

        const raft = self.raft_node;
        var all_applied = true;
        while (true) {
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
            // Applying can propose (a workflow's next step); flushing that
            // can commit it, so apply again until a flush commits nothing new.
            self.syncFlushIfNeeded();
            if (raft.last_applied >= @min(raft.commit_index, limit)) break;
        }
        if (self.wake_workers) {
            self.wake_workers = false;
            ActionsHandler.wakeWorkers(self);
        }
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
        const may_sleep = timeout_ms > 0 and self.mailbox.prepareSleep();
        const events = self.reactor.poll(if (may_sleep) timeout_ms else 0) catch |err| {
            if (may_sleep) self.mailbox.wake.woke();
            return err;
        };
        if (may_sleep) self.mailbox.wake.woke();
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
        self.expireReplySlots();

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

        // Consumer-group PEL sweeper — runs every 1 second. Re-nacks
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
        const cutoff_ns = now_ns -| (self.hot_flush_seconds *| std.time.ns_per_s);

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
                const cutoff_ms = now_ms -| (meta.retention_age_s *| 1000);
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

    /// TaskScheduler callback: sweep every consumer group's PEL.
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
        // A readable event comes when data arrives, not while it waits
        // (io_uring polls are multishot). A read that filled the room it
        // had may have left the rest of a request in the socket with
        // nothing to announce it, so read again until a read comes back short.
        var reads: u32 = 0;
        while (true) {
            // A readable event can still arrive after a pause (same poll batch,
            // or an interest change not yet submitted); what it would read
            // could not drain, and would look like one oversized request.
            if (conn.reads_paused or conn.closing or conn.waiting != null) return;

            // Read no more than the read buffer has room for: bytes read and
            // not kept would be requests silently lost.
            var tmp_buf: [65536]u8 = undefined;
            if (conn.read_buf.writable() == 0) {
                // A request may be larger than the buffer: grow it to hold one
                // whole request. Full at that size, the client sent more than a
                // request can be. Every request says its size in its header and
                // one that cannot fit is refused from it, so this should not be
                // reached; if it is, one connection is closed rather than its
                // buffer growing without bound.
                const cap = conn.read_buf.buf.len * 2;
                if (cap > MAX_READ_BUFFER) {
                    self.sendErrorResponse(conn, 0, .bad_request, "bad request: request over 256 KiB");
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

            self.processRequests(fd, conn);
            // Closing is deferred, so the connection is still ours here.
            conn.shrinkReadBuffer();
            if (n < room) return;
            reads += 1;
            if (reads == self.reads_per_event) {
                // More may wait. It is read at the end of the tick, so one
                // busy client doesn't hold the shard's others back.
                self.queueResume(fd, conn);
                return;
            }
        }
    }

    /// Run `conn` again at the end of the tick: what it buffered, and
    /// what its socket still holds.
    fn queueResume(self: *Shard, fd: i32, conn: *Connection) void {
        if (conn.resume_queued) return;
        conn.resume_queued = true;
        self.resume_fds.appendAssumeCapacity(fd);
    }

    /// Try to parse and dispatch request(s) from a connection's read buffer.
    fn processRequests(self: *Shard, fd: i32, conn: *Connection) void {
        self.runRequests(fd, conn);
        // Read again once the request it waited on has gone or been refused.
        if (conn.reads_held and conn.waiting == null and !conn.reads_paused and !conn.closing) {
            self.reactor.modifyInterests(fd, .{ .readable = true, .writable = conn.hasPendingWrites() }) catch |err| {
                log.err("shard {d}: could not read connection fd={d} again: {s}; closing it", .{ self.id, fd, @errorName(err) });
                return self.markClosing(fd);
            };
            conn.reads_held = false;
        }
    }

    fn runRequests(self: *Shard, fd: i32, conn: *Connection) void {
        const header_size = @sizeOf(proto.RequestHeader);
        // A waiting connection is not read (`readFromClient`) or resumed
        // (`settleConnections`) until `resumeWaiting` lets it go.
        std.debug.assert(conn.waiting == null);

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

            const held = self.holdForward(conn, req);
            if (held != null and conn.give_up_bytes == 0) return self.waitForward(fd, conn, held.?);
            const waited: ?u64 = if (conn.head_since_ms) |t| nowMs() -| t else null;

            const consumed = header_size + req.header.payload_length;
            conn.read_buf.consume(consumed);
            conn.give_up_bytes -|= consumed;

            // Dispatch request, detecting if the handler sent a response
            const pending_before = conn.write_buf.readable();
            const forwards_before = conn.forwards_in_flight;
            var answered_deferred = false;
            if (held) |w| self.refuseWaited(conn, req, w, waited) else answered_deferred = self.dispatchNoted(conn, req, false);
            // Parked here as a blocking read: one more toward its cap.
            if (held == null and conn.response_deferred and conn.forwards_in_flight == forwards_before and blockingWait(req) != null) conn.local_reads +|= 1;
            // Its wait is spent (counted into its deadline if it went to
            // another shard); the next request, should it wait, starts its
            // own clock at the back of the line.
            conn.head_since_ms = null;
            conn.has_turn = false;

            // If handler didn't queue any response, check if it was deferred.
            // An answer refused because the client let 4 MiB of answers pile
            // up unread was produced; the connection closes on the next flush.
            if (conn.write_buf.readable() == pending_before and !conn.response_deferred and !conn.write_overflow and !conn.closing) {
                if (answered_deferred) {
                    self.answeredAsDeferred(req);
                } else {
                    self.sendErrorResponse(conn, req.header.request_id, .internal_error, self.noAnswer(req));
                }
            }
            // Each request says for itself whether it parked.
            conn.response_deferred = false;

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
        conn.reads_held = false; // interest is this pause's now
        conn.paused_progress_ms = @import("stdx").time.monotonicMs();
        self.paused_count += 1;
        self.reactor.modifyInterests(fd, .{ .readable = false, .writable = true }) catch {};
    }

    fn resumeReads(self: *Shard, fd: i32, conn: *Connection) void {
        conn.reads_paused = false;
        conn.reads_held = false;
        self.paused_count -= 1;
        self.reactor.modifyInterests(fd, .{ .readable = true, .writable = conn.hasPendingWrites() }) catch {};
        // What it had already sent runs at the end of the tick, not inside
        // whoever resumed it; its socket may hold more that announces
        // nothing, so it is queued even with nothing buffered.
        self.queueResume(fd, conn);
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

    /// How long a client's request may wait, unread, for room to go to
    /// another shard before it is refused, counted from when it first
    /// waited. Waiting holds its connection's later requests too, so this
    /// bounds how long any caller waits for room, blocking reads included;
    /// for a client with one request out, as the SDKs have, the refusal
    /// arrives before their 5 s request timeout (a pipelined request's
    /// clock starts when it reaches the head). A request that does go has
    /// what is left of `reply_pool.DEADLINE_MS`, and at least
    /// `reply_pool.MIN_DEADLINE_MS`.
    pub const ASK_WAIT_MS: u64 = 3000;

    comptime {
        // PAUSE_REASONS: the wait reasons in order, then unsent_answers.
        const reasons = @typeInfo(Connection.WaitReason).@"enum".fields;
        std.debug.assert(reasons.len + 1 == ShardMetrics.PAUSE_REASONS.len);
        for (reasons, ShardMetrics.PAUSE_REASONS[0..reasons.len]) |r, label| std.debug.assert(std.mem.eql(u8, r.name, label));
    }

    /// Stop reading `conn` until its next request has room to go where it is
    /// bound, or has waited `ASK_WAIT_MS` (`resumeWaiting`).
    fn waitForward(self: *Shard, fd: i32, conn: *Connection, w: Connection.Waiting) void {
        // Paused for its unsent answers, it would not have got this far; the
        // two pauses never overlap, so neither undoes the other.
        std.debug.assert(!conn.reads_paused);
        std.debug.assert(conn.waiting == null);
        conn.waiting = w;
        self.setReason(conn, w.reason);
        // A request waiting for the first time joins the back of the line; one
        // that lost the room it was resumed for keeps its place.
        if (conn.head_since_ms == null) {
            conn.head_since_ms = nowMs();
            self.wait_seq +%= 1;
            if (self.wait_seq == 0) self.wait_seq = 1;
            conn.wait_seq = self.wait_seq;
            self.waiting_fds.appendAssumeCapacity(.{ .fd = fd, .seq = conn.wait_seq });
        }
        if (conn.reads_held) return;
        self.reactor.modifyInterests(fd, .{ .readable = false, .writable = conn.hasPendingWrites() }) catch |err| {
            log.err("shard {d}: could not stop reading connection fd={d}: {s}; closing it", .{ self.id, fd, @errorName(err) });
            return self.markClosing(fd);
        };
        conn.reads_held = true;
    }

    /// `conn` waits for `why` now. Only a wait for the target's room holds
    /// others back (`queued`): one on the connection's own limits says
    /// nothing about the target.
    fn setReason(self: *Shard, conn: *Connection, why: Connection.WaitReason) void {
        const w = &conn.waiting.?;
        w.reason = why;
        const in_line = why == .slots or why == .inbox;
        if (in_line == conn.in_queue) return;
        conn.in_queue = in_line;
        const n = &self.queued[@intFromBool(w.blocking)][w.target];
        if (in_line) n.* += 1 else n.* -= 1;
    }

    /// `conn` waits no longer.
    fn endWait(self: *Shard, conn: *Connection) void {
        const w = conn.waiting orelse return;
        if (conn.in_queue) self.queued[@intFromBool(w.blocking)][w.target] -= 1;
        conn.in_queue = false;
        conn.waiting = null;
    }

    /// Answer a request that could not get room in time: it was not run.
    fn refuseWaited(self: *Shard, conn: *Connection, req: proto.Request, w: Connection.Waiting, waited_ms: ?u64) void {
        // Refused on the connection's own in-flight limit: the shard holding
        // its oldest unanswered request, found once per give-up.
        const blamed = if (w.reason != .in_flight) w.target else conn.give_up_blame orelse blk: {
            const oldest = self.reply_pool.oldestFor(conn.fd, conn.id) orelse w.target;
            conn.give_up_blame = oldest;
            break :blk oldest;
        };
        conn.recordRequest();
        if (self.metrics_registry) |m| m.server.recordCommand();
        if (self.shard_metrics) |sm| sm.recordCommand();
        var msg_buf: [160]u8 = undefined;
        const message = switch (w.reason) {
            .in_flight => std.fmt.bufPrint(&msg_buf, "overloaded: {d} requests on this connection are still waiting on other shards (the oldest on shard {d}); this request was not run — back off and retry", .{ Connection.MAX_FORWARDS_IN_FLIGHT, blamed }),
            .blocking_reads => std.fmt.bufPrint(&msg_buf, "overloaded: this connection has {d} blocking reads waiting already; this request was not run — retry once one returns", .{Connection.MAX_BLOCKING_IN_FLIGHT}),
            .slots => if (w.blocking)
                std.fmt.bufPrint(&msg_buf, "overloaded: shard {d} has no room for more blocking reads from shard {d}; this read was not run — retry", .{ w.target, self.id })
            else
                std.fmt.bufPrint(&msg_buf, NOT_KEEPING_UP, .{w.target}),
            .inbox => std.fmt.bufPrint(&msg_buf, NOT_KEEPING_UP, .{w.target}),
        } catch "overloaded: this request was not run — back off and retry";
        self.sendErrorResponse(conn, req.header.request_id, .overloaded, message);
        if (self.shard_metrics) |sm| sm.recordCrossShardOverloaded(.client_forward);
        self.forwards_refused += 1;
        // Said for the request that waited, once per interval per reason;
        // those refused behind it are counted.
        const waited = waited_ms orelse return;
        const now = nowMs();
        const last = &self.wait_warn_ms[@intFromEnum(w.reason)];
        if (now -| last.* < WARN_INTERVAL_MS) return;
        last.* = now;
        const why = switch (w.reason) {
            .in_flight => std.fmt.comptimePrint("its connection had {d} requests unanswered, the oldest on that shard", .{Connection.MAX_FORWARDS_IN_FLIGHT}),
            .blocking_reads => std.fmt.comptimePrint("its connection had {d} blocking reads out", .{Connection.MAX_BLOCKING_IN_FLIGHT}),
            .slots => if (w.blocking) "that shard held its share of this shard's blocking reads" else "that shard held its share of this shard's requests",
            .inbox => "that shard had not drained this shard's share of its inbox",
        };
        log.warn("shard {d}: refused a client request after {d} ms waiting on shard {d}: {s} ({d} refused so far; {d} ordinary and {d} blocking in flight to shard {d})", .{ self.id, waited, blamed, why, self.forwards_refused, self.reply_pool.heldBy(.ordinary, blamed), self.reply_pool.heldBy(.blocking, blamed), blamed });
    }

    /// Resume, in the order their requests began waiting, the connections
    /// whose next request now has room — no more to one target than it has
    /// room for, so the rest keep their place and their clock rather than
    /// all waking to lose the race — and those that have waited
    /// `ASK_WAIT_MS`, to be refused. A resumed connection sends that one
    /// request ahead of the line (`Connection.has_turn`); its next waits at
    /// the back.
    fn resumeWaiting(self: *Shard) void {
        if (self.waiting_fds.items.len == 0) return;
        const now = nowMs();
        const MAX = @import("../config/server.zig").MAX_SHARDS;
        // Room per target this pass, worked out when first needed.
        var slot_room: [2][MAX]i32 = .{ @splat(-1), @splat(-1) };
        var inbox_room: [MAX]i32 = @splat(-1);
        var kept: usize = 0;
        for (self.waiting_fds.items) |entry| {
            const conn = self.getConnection(entry.fd) orelse continue;
            // Its head went and a later request waits under a newer entry,
            // or the fd was reused.
            if (conn.wait_seq != entry.seq) continue;
            const w = conn.waiting orelse {
                // Resumed last pass. Its head went: out of the line. Or it
                // was paused for its unsent answers first: it keeps its
                // place, should its head wait again.
                if (conn.head_since_ms != null) {
                    self.waiting_fds.items[kept] = entry;
                    kept += 1;
                } else conn.wait_seq = 0;
                continue;
            };
            if (conn.closing) continue;
            const class: reply_pool_mod.Class = if (w.blocking) .blocking else .ordinary;
            var why = self.stillWaits(conn, .{ .target = w.target, .blocking = w.blocking }, w.reason);
            const slots = &slot_room[@intFromEnum(class)][w.target];
            const inbox = &inbox_room[w.target];
            // A blocking read here waits on nothing shared.
            const shared = w.target != self.id;
            if (why == null and shared) {
                const mailbox = self.peer_mailboxes.?[w.target];
                if (slots.* < 0) slots.* = self.reply_pool.room(class, w.target);
                if (inbox.* < 0) inbox.* = mailbox.shareRoom(@intCast(self.id));
                if (slots.* <= 0) {
                    why = .slots;
                } else if (inbox.* <= 0) {
                    // Those resumed ahead of it will fill the share: be told
                    // when the target drains it.
                    why = .inbox;
                    mailbox.wantShare(@intCast(self.id));
                }
            }
            if (why) |r| {
                if (now -| conn.head_since_ms.? < ASK_WAIT_MS) {
                    self.setReason(conn, r);
                    self.waiting_fds.items[kept] = entry;
                    kept += 1;
                    continue;
                }
            }
            self.endWait(conn);
            // A client that has gone is not answered, and what it sent is
            // not run.
            if (peerClosed(entry.fd)) {
                self.markClosing(entry.fd);
                continue;
            }
            if (why == null) {
                if (shared) {
                    slots.* -= 1;
                    inbox.* -= 1;
                }
                conn.has_turn = true;
            } else {
                conn.give_up_bytes = conn.read_buf.readable();
                conn.give_up_blame = null;
            }
            if (!conn.resume_queued) {
                conn.resume_queued = true;
                self.resume_fds.appendAssumeCapacity(entry.fd);
            }
            // Should its head lose the room it was resumed for this tick, it
            // waits again here, in its place.
            self.waiting_fds.items[kept] = entry;
            kept += 1;
        }
        self.waiting_fds.shrinkRetainingCapacity(kept);
    }

    /// Whether the client has gone: its end closed with nothing left to
    /// read, or reset. A client that only stopped sending is gone too, as a
    /// read of end-of-stream treats it (`readFromClient`).
    fn peerClosed(fd: i32) bool {
        var byte: [1]u8 = undefined;
        const n = std.c.recv(fd, &byte, 1, std.c.MSG.PEEK | std.c.MSG.DONTWAIT);
        if (n == 0) return true;
        if (n > 0) return false;
        return std.posix.errno(n) != .AGAIN;
    }

    /// Set the paused-connections gauge: those waiting, by reason, and
    /// those paused for their unsent answers.
    fn countPaused(self: *Shard) void {
        const sm = self.shard_metrics orelse return;
        var counts: [ShardMetrics.PAUSE_REASONS.len]u64 = @splat(0);
        for (self.waiting_fds.items) |entry| {
            const conn = self.getConnection(entry.fd) orelse continue;
            if (conn.wait_seq != entry.seq) continue;
            const w = conn.waiting orelse continue;
            counts[@intFromEnum(w.reason)] += 1;
        }
        counts[counts.len - 1] = self.paused_count;
        sm.setConnectionsPaused(counts);
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
        if (self.pending_cancels.items.len > 0) self.sendPendingCancels();
        self.resumeWaiting();
        std.mem.swap(std.ArrayListUnmanaged(i32), &self.resume_fds, &self.resume_running);
        var ran = false;
        for (self.resume_running.items) |fd| {
            const conn = self.getConnection(fd) orelse continue;
            conn.resume_queued = false;
            if (conn.closing or conn.reads_paused or conn.waiting != null) continue;
            ran = true;
            self.processRequests(fd, conn);
            // Bytes left in the socket while it waited or was paused get no
            // event of their own on Linux: read them now.
            self.readFromClient(fd);
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
        self.countPaused();
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
    /// diverged, 5 and 6 guarded (no log or no hard state of its own, new
    /// or lost): catching up, then confirming the term.
    fn dispatchClusterStatus(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: proto.Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));

        const raft = shard.raft_node;
        const state: u8 = if (shard.diverged)
            4
        else if (raft.lost_log == .catching_up)
            5
        else if (raft.lost_log == .confirming)
            6
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

    /// A refusal is always sent: one too long for its frame is cut, never
    /// dropped (a dropped one left the client the no-answer fallback).
    pub fn sendErrorResponse(self: *Shard, conn: *Connection, request_id: u64, status: proto.StatusCode, msg: []const u8) void {
        _ = self;
        var buf: [1024]u8 = undefined;
        _ = conn.queueWrite(proto.Response.serializeError(status, request_id, msg, &buf));
    }

    /// Deliver an already-serialized answer to wherever `reply_to` says.
    ///
    /// A connection's fd, buffers, and reactor registration belong to the
    /// shard that owns it. When that is this shard, write directly;
    /// otherwise marshal the bytes to that shard so the socket write happens
    /// on its thread — never touch another shard's connection from here. A
    /// node that forwarded the request gets the answer over the peer link.
    ///
    /// The connection's generation is checked against the live connection
    /// before writing, so that fd reuse (close + accept at the same fd)
    /// cannot misdirect a stale answer to the wrong client.
    pub fn deliverDeferred(self: *Shard, reply_to: ReplyTo, bytes: []const u8) void {
        // Noted for `dispatchNoted`, which checks the request was parked.
        if (self.dispatching) |*d| if (bytes.len >= @sizeOf(proto.ResponseHeader)) {
            const id = std.mem.readInt(u64, bytes[@offsetOf(proto.ResponseHeader, "request_id")..][0..8], .little);
            if (id == d.request_id and std.meta.eql(reply_to, d.reply_to)) d.answered_deferred = true;
        };
        const my_id: u16 = @intCast(self.id);
        const to = switch (reply_to) {
            .remote => |r| return self.sendForwardReply(r.node, r.forward_id, bytes),
            .socket => |s| s,
        };
        const owner_shard = to.shard;
        const fd = to.fd;
        const conn_id = to.conn_id;
        if (owner_shard == my_id) {
            const conn = self.getConnection(fd) orelse return;
            if (conn.id != conn_id) return; // fd was reused for a new connection
            _ = conn.queueWrite(bytes);
            self.flushToClient(fd);
            return;
        }

        const mailboxes = self.peer_mailboxes orelse return;
        if (owner_shard >= mailboxes.len) return;
        if (to.slot == ReplyTo.NO_SLOT) {
            // Another shard's client reaches this one only by a request that
            // took a reply slot there.
            self.replies_dropped += 1;
            if (self.shard_metrics) |sm| sm.recordReplyDropped();
            log.err("shard {d}: dropped an answer for a client of shard {d} (fd {d}): it holds no reply slot. This is a bug; that client gets no answer", .{ my_id, owner_shard, fd });
            return;
        }

        var msg: InboxMessage = .{ .tag = .reply, .src_shard = @intCast(my_id) };
        msg.setReplySlot(to.slot, to.gen);
        // Without room for a copy, an answer with no bytes still frees the
        // slot and tells the client its answer was lost.
        if (self.allocator.alloc(u8, bytes.len)) |payload| {
            @memcpy(payload, bytes);
            msg.payload_len = @intCast(payload.len);
            msg.payload_ptr = payload.ptr;
        } else |_| {}
        if (!mailboxes[owner_shard].replies.send(msg)) {
            if (msg.payload_ptr) |p| self.allocator.free(@as([*]u8, @ptrCast(p))[0..msg.payload_len]);
            self.replies_dropped += 1;
            if (self.shard_metrics) |sm| sm.recordReplyDropped();
            log.err("shard {d}: dropped an answer for a client of shard {d} (fd {d}): its reply ring is full; that client is answered when its slot expires", .{ my_id, owner_shard, fd });
        }
    }

    /// Serialize a status+data response and deliver it via `deliverDeferred`.
    pub fn deliverDeferredResponse(self: *Shard, reply_to: ReplyTo, request_id: u64, status: proto.StatusCode, data: []const u8) void {
        var buf: [MAX_REQUEST_SIZE + @sizeOf(proto.ResponseHeader)]u8 = undefined;
        // Same rule as `sendOkResponse`: a parked client is answered, never
        // left to its own timeout because the answer is too large to frame
        // (and on another shard, never left holding its reply slot).
        const serialized = proto.Response.serializeNew(status, request_id, data, &buf) catch blk: {
            log.warn("shard {d}: deferred answer to request {d} is {d} bytes, over the frame limit; answered with an error", .{ self.id, request_id, data.len });
            break :blk proto.Response.serializeNew(.internal_error, request_id, "internal error: answer over 256 KiB — ask for less", &buf) catch unreachable;
        };
        self.deliverDeferred(reply_to, serialized);
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

    /// A request whose handler neither answered nor parked it: a handler
    /// bug, answered internal_error so the client isn't left waiting.
    /// Returns the answer's text.
    fn noAnswer(self: *Shard, req: proto.Request) []const u8 {
        const name = if (std.enums.tagName(proto.OpCode, @enumFromInt(req.header.op_code))) |n| n else "?";
        log.err("shard {d}: request {d} ({s}) got no answer from its handler. This is a bug", .{ self.id, req.header.request_id, name });
        if (self.shard_metrics) |sm| sm.recordHandlerNoAnswer();
        if (builtin.mode == .Debug and !builtin.is_test) @panic("a handler produced no answer");
        return "internal error: the handler produced no answer";
    }

    /// An op no handler serves: refused, the same way wherever it runs.
    fn answerUnknownOp(shard_ptr: *anyopaque, conn_ptr: *anyopaque, request_id: u64, op: u16) void {
        const self: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        var buf: [32]u8 = undefined;
        const message = std.fmt.bufPrint(&buf, "unknown op 0x{x}", .{op}) catch unreachable;
        self.sendErrorResponse(conn, request_id, .bad_request, message);
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
        const pc = if (handler_mod.qualifyKey(&qbuf, ns, n)) |q| stream.getPartitionCount(q) else |_| 1;
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

/// A blocking read dropped because its client closed: an empty answer, so
/// the asking shard's slot for it comes back; nobody reads it.
fn answerCancelled(waiter: *const Waiter, ctx: *anyopaque) void {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    shard.deliverDeferred(waiter.reply_to, "");
}

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
                shard.deliverDeferredResponse(waiter.reply_to, waiter.request_id, .not_found, "");
            }
        },
        .queue_dequeue => {
            // Queue blocking dequeue timeout → empty messages response (count = 0)
            var buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &buf, 0, .little);
            shard.deliverDeferredResponse(waiter.reply_to, waiter.request_id, .ok, &buf);
        },
        // stream_read / action_await / stream_group_read → empty OK response
        .stream_read, .action_await, .stream_group_read => {
            shard.deliverDeferredResponse(waiter.reply_to, waiter.request_id, .ok, "");
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
        shard.deliverDeferred(waiter.reply_to, serialized);
    } else |_| {
        // The waiter is gone once this returns; an asker left without an
        // answer would hang until its own timeout.
        shard.deliverDeferredResponse(waiter.reply_to, waiter.request_id, .internal_error, "internal error: value too large to send");
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
        .stream_messages => |m| shard.deliverDeferredResponse(waiter.reply_to, waiter.request_id, .ok, m.data),
        else => shard.deliverDeferredResponse(waiter.reply_to, waiter.request_id, .internal_error, ""),
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

    shard.deliverDeferredResponse(waiter.reply_to, waiter.request_id, .ok, "");
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

    // Auto-ack: persist a queue_ack entry so the message doesn't reappear
    // after restart; it names the queue, as any ack does.
    {
        var seq_key: [8]u8 = undefined;
        std.mem.writeInt(u64, &seq_key, deq_result.seq, .little);
        var queue_key: [8]u8 = undefined;
        std.mem.writeInt(u64, &queue_key, queue_name_hash, .little);
        // The waiter holds only the queue's hash; its namespace comes from
        // the queue's registration.
        const namespace = if (partition.queue.known_queues.get(queue_name_hash)) |meta| meta.namespace else "default";

        // Same contract as the queue handler's dequeue-ack: log, never fail
        // the dequeue. The ack applies when it commits.
        _ = persistence_mod.proposeEntry(shard, .queue_ack, entry_mod.Flags.NONE, namespace, &seq_key, &queue_key) catch |err| {
            log.err("shard {d}: queue ack for seq {d} not persisted: {s}; message delivered, may be redelivered after a restart", .{ shard.id, deq_result.seq, @errorName(err) });
        };
    }

    shard.deliverDeferredResponse(waiter.reply_to, waiter.request_id, .ok, data);
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
    // Behind a gap nothing is buffered: the flush re-buffers from the gap.
    if (writer.stop_at_gap and writer.first_unbuffered != null) return;
    writer.addEntry(entry) catch |err| {
        writer.buffer_failures += 1;
        if (writer.stop_at_gap) {
            writer.first_unbuffered = entry.header.index;
            log.err("shard {d}: failed to buffer entry index={d} for persistence: {s}; buffering resumes from it at the next flush (buffer_failures={d})", .{ writer.partition_id, entry.header.index, @errorName(err), writer.buffer_failures });
        } else {
            log.err("shard {d}: failed to buffer entry index={d} for persistence: {s}; it will be missing from disk after a restart (buffer_failures={d})", .{ writer.partition_id, entry.header.index, @errorName(err), writer.buffer_failures });
        }
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

    fn persist(ctx: *anyopaque, term: u64, voted_for: u32, lost_log: bool) bool {
        const self: *HardStateStore = @ptrCast(@alignCast(ctx));
        hard_state_mod.save(self.dir, .{ .node_id = self.node_id, .term = term, .voted_for = voted_for, .lost_log = lost_log }) catch |err| {
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
    reply_to: ReplyTo = undefined,
    request_id: u64 = 0,
    bytes: []u8 = &.{},
    responder: HandlerFn = undefined,
};
pub const PENDING_SLOTS: usize = 2 * raft_node_mod.MAX_OUTSTANDING;

/// A client's write sent to the leader, and what came back.
const Forward = struct {
    active: bool = false,
    id: u32 = 0,
    reply_to: ReplyTo = undefined,
    request_id: u64 = 0,
    bytes: []u8 = &.{},
    /// The leader it went to, the term then, and the link it went over; 0
    /// until it has gone.
    sent_to: u32 = 0,
    sent_term: u64 = 0,
    sent_session: u64 = 0,
    sent_ms: u64 = 0,
    deadline_ms: u64 = 0,
    /// The leader's answer, held until `applied_by` is applied here.
    reply: ?[]u8 = null,
    applied_by: u64 = 0,
};
const FORWARD_SLOTS: usize = 1024;
/// How long a write waits for a leader to be known, and how long an
/// answer is held for this node to catch up to it.
pub const FORWARD_TIMEOUT_MS: u64 = 5000;
/// A write sent to a leader that stays linked and in office but never
/// answers is answered after this: the last guard, for a leader that is
/// wedged. Everything else that ends a sent write answers sooner.
pub const SENT_FORWARD_BACKSTOP_MS: u64 = 60_000;
/// A forwarded write's frame: its id, then the length of the fields carried
/// beside the client's bytes (none yet), then those fields, then the bytes.
const FORWARD_PREFIX: usize = 4 + 2;
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
fn bringUpGroup(raft: *RaftNode, role: ClusterRole, buf: []u8, shard_id: u16, node_id: u32, had_hard_state: bool) !void {
    // Everything replay put in the log came from disk.
    raft.markDurable(raft.log.lastIndex());
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
                log.err("shard {d}: this data directory belonged to a group of {d} (members {any}); start with --join to rejoin them, or point --data-dir at an empty directory to start alone", .{ shard_id, members.len, members });
                return error.DataDirWasClustered;
            }
        }
        // Alone, nothing was lost that another node could vote against;
        // bootstrap writes the cleared flag with its new term.
        raft.lost_log = .none;
        return raft.bootstrap();
    }
    const empty = raft.log.lastIndex() == 0;
    // The log is evidence of the term too: a hard state that is missing,
    // or behind it, never lets the node act in a term it already left.
    raft.current_term = @max(raft.current_term, raft.log.lastTerm());
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
        raft.membership_term = e.header.term;
        raft.recordConfig(cfg_index, e.header.term, members);
        if (cfg_index <= raft.last_applied) raft.commitMembership(members);
        // What the segments flushed under a commit watermark is committed;
        // the rest of the log waits for a leader to say so.
        raft.commit_index = raft.last_applied;
        log.info("shard {d}: members {any} from the log (config index {d}); following until a leader speaks{s}", .{ shard_id, members, cfg_index, if (raft.timer_enabled) "" else " (this node is not a member)" });
        // The log survived but the hard state did not: the vote cast in
        // the current term is gone, so the term is confirmed first.
        if (!had_hard_state and raft.lost_log == .none) {
            try raft.enterLostVote();
            log.warn("shard {d}: this node has a log but no {s}; it votes only once the members have confirmed the term", .{ shard_id, hard_state_mod.FILENAME });
        }
        return;
    }
    switch (role) {
        .single => unreachable,
        .bootstrap => {
            // Hard state without a log is a member whose log is gone, not
            // a first boot: founding would start a second cluster.
            if (empty and (raft.lost_log != .none or raft.current_term > 0)) {
                log.err("shard {d}: this node has no log but has hard state from a cluster (term {d}); it was a member and lost its log. Restart it with --join <a live member>, not --cluster", .{ shard_id, raft.current_term });
                return error.LostLogFounding;
            }
            try raft.bootstrap();
            var cfg: [membership.MAX_SIZE]u8 = undefined;
            _ = try raft.propose(.raft_config, entry_mod.Flags.NONE, 0, membership.encode(&.{node_id}, &cfg));
            if (empty) log.warn("shard {d}: founding a new cluster at term 1; if this node was a member of an existing cluster, stop it and restart with --join", .{shard_id}) else log.info("shard {d}: first member; leading a group of one", .{shard_id});
        },
        .join => {
            raft.commit_index = raft.last_applied;
            raft.timer_enabled = false;
            // No log, or a log with no config and no hard state: a new node
            // and one that lost its disk look the same, and whatever the
            // latter voted for or acked is gone. Durable before it answers
            // anyone.
            const guard = empty or !had_hard_state;
            if (guard) try raft.enterLostLog();
            log.info("shard {d}: joining{s}; following until a config entry names this node", .{ shard_id, if (guard) " with nothing in the log and no hard state to vote from; voting only once caught up and the term is confirmed" else "" });
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
    // At boot, replay applies the committed configs before the group comes
    // up: a node that lost its hard state checks the committed set even
    // when its latest config is not.
    raft.recordConfig(entry.header.index, entry.header.term, members);
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
    const sent = shard.mailbox.inbox.send(.{
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
    const put = try Shard.serializeRequest(std.testing.allocator, .{ .header = header, .namespace = "", .key = "big", .value = value });
    defer std.testing.allocator.free(put);
    shard.dispatchRequest(conn, try proto.Request.parse(put));
    _ = shard.applyCommitted();
    var drain_buf: [4096]u8 = undefined;
    _ = try readAnswer(pair[1], &shard, conn.fd, &drain_buf, 1);

    header.op_code = @intFromEnum(proto.OpCode.kv_get);
    header.request_id = 2;
    const get = try Shard.serializeRequest(std.testing.allocator, .{ .header = header, .namespace = "", .key = "big", .value = "" });
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
    try std.testing.expect(shard.waiter_pool.register(.{ .kind = .kv_get, .reply_to = conn.replyTo(), .request_id = 7, .key = present, .min_version = version, .timeout_ms = 1 }));
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
    try std.testing.expect(shard.waiter_pool.register(.{ .kind = .kv_get, .reply_to = conn.replyTo(), .request_id = 8, .key = missing, .min_version = 0, .timeout_ms = 1 }));
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
    return Shard.serializeRequest(std.testing.allocator, .{ .header = header, .namespace = "", .key = key, .value = value, .options = options });
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
}

/// Two shards of one node, wired to each other as the runtime wires them.
const TwoShards = struct {
    pipes: [2][2]std.posix.fd_t,
    shards: [2]Shard,
    mailboxes: [2]*Mailbox,
    peers: [2]*Shard,

    fn init(self: *TwoShards) !void {
        for (&self.pipes, 0..) |*p, i| {
            p.* = try @import("stdx").io.pipe();
            self.shards[i] = try Shard.init(std.testing.allocator, @intCast(i), 2, 4096, p[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
        }
        for (0..2) |i| {
            self.mailboxes[i] = self.shards[i].mailbox;
            self.peers[i] = &self.shards[i];
        }
        for (&self.shards) |*s| {
            s.peer_mailboxes = &self.mailboxes;
            s.peer_shards = &self.peers;
        }
    }

    fn deinit(self: *TwoShards) void {
        for (&self.shards) |*s| s.deinit();
        for (self.pipes) |p| {
            _ = std.c.close(p[0]);
            _ = std.c.close(p[1]);
        }
    }
};

/// The next answer on a test client's socket, parsed.
fn nextAnswer(c: TestClient, shard: *Shard, out: []u8) !proto.Response {
    const n = try readAnswer(c.pair[1], shard, c.conn.fd, out, @sizeOf(proto.ResponseHeader));
    return proto.Response.parse(out[0..n]);
}

test "Shard: a request forwarded to another shard is answered on the asker's reply ring, once, and only by the shard asked" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const b = &two.shards[1];
    const c = try TestClient.open(a);
    defer _ = std.c.close(c.pair[1]);
    var sm = ShardMetrics{ .shard_id = 0 };
    a.shard_metrics = &sm;
    defer a.shard_metrics = null;

    const ping = try testRequest(.ping, 41, "", "");
    defer std.testing.allocator.free(ping);
    a.forwardToShard(1, c.conn, try proto.Request.parse(ping));
    try std.testing.expectEqual(@as(usize, 1), a.reply_pool.taken());
    try std.testing.expectEqual(@as(u64, 1), sm.snapshot().cross_shard_in_flight);
    try std.testing.expectEqual(@as(usize, 1), b.drainInbox());
    // The answer waits on the asker's reply ring, not its inbox.
    try std.testing.expectEqual(@as(usize, 1), a.mailbox.replies.pending());
    try std.testing.expectEqual(@as(usize, 0), a.mailbox.inbox.pending());

    // Held back: a copy of it arriving from a shard that was not asked
    // frees nothing and reaches no one.
    var held: [1]InboxMessage = undefined;
    try std.testing.expectEqual(@as(usize, 1), a.mailbox.replies.drain(&held));
    const len = held[0].payload_len;
    const copy = try std.testing.allocator.alloc(u8, len);
    @memcpy(copy, @as([*]u8, @ptrCast(held[0].payload_ptr.?))[0..len]);
    var impostor = held[0];
    impostor.src_shard = 0;
    impostor.payload_ptr = copy.ptr;
    try std.testing.expect(a.mailbox.replies.send(impostor));
    _ = a.drainInbox();
    try std.testing.expectEqual(@as(usize, 1), a.reply_pool.taken());

    // The real answer frees the slot and reaches the client.
    try std.testing.expect(a.mailbox.replies.send(held[0]));
    _ = a.drainInbox();
    try std.testing.expectEqual(@as(usize, 0), a.reply_pool.taken());
    try std.testing.expectEqual(@as(u64, 0), sm.snapshot().cross_shard_in_flight);
    var out: [256]u8 = undefined;
    const resp = try nextAnswer(c, a, &out);
    try std.testing.expectEqual(@as(u64, 41), resp.header.request_id);
    try std.testing.expectEqualStrings("PONG", resp.data);

    // A second answer naming the same, now freed, slot is dropped.
    const again = try std.testing.allocator.alloc(u8, len);
    @memset(again, 0);
    var dup = held[0];
    dup.payload_ptr = again.ptr;
    try std.testing.expect(a.mailbox.replies.send(dup));
    _ = a.drainInbox();
    a.flushToClient(c.conn.fd);
    try std.testing.expect(std.c.read(c.pair[1], &out, out.len) < 0);
}

test "Shard: an answer the other shard could not send, a request no slot is left for, and one not answered by its deadline each reach the client once, and the slot comes back" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const b = &two.shards[1];
    const c = try TestClient.open(a);
    defer _ = std.c.close(c.pair[1]);
    var sm = ShardMetrics{ .shard_id = 0 };
    a.shard_metrics = &sm;
    defer a.shard_metrics = null;
    var out: [256]u8 = undefined;

    // An answer with no bytes (the answering shard had no room to copy it):
    // the client hears its answer was lost, and the slot is free.
    const p1 = try testRequest(.ping, 51, "", "");
    defer std.testing.allocator.free(p1);
    a.forwardToShard(1, c.conn, try proto.Request.parse(p1));
    var fwd: [1]InboxMessage = undefined;
    try std.testing.expectEqual(@as(usize, 1), b.mailbox.inbox.drain(&fwd));
    std.testing.allocator.free(@as([*]u8, @ptrCast(fwd[0].payload_ptr.?))[0..fwd[0].payload_len]);
    var empty: InboxMessage = .{ .tag = .reply, .src_shard = 1 };
    const t = fwd[0].replySlot();
    empty.setReplySlot(t.slot, t.gen);
    try std.testing.expect(a.mailbox.replies.send(empty));
    _ = a.drainInbox();
    try std.testing.expectEqual(@as(usize, 0), a.reply_pool.taken());
    var resp = try nextAnswer(c, a, &out);
    try std.testing.expectEqual(@as(u64, 51), resp.header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.internal_error), resp.header.status);

    // Shard 1 holding its full share: the next request for it is refused
    // at once, and told it did not run.
    const p2 = try testRequest(.ping, 52, "", "");
    defer std.testing.allocator.free(p2);
    while (a.reply_pool.take(.ordinary, 1, -1, 0, 0, false, Shard.nowMs(), Shard.nowMs(), 3000, 0)) |_| {} else |err| try std.testing.expectEqual(error.TargetBusy, err);
    a.forwardToShard(1, c.conn, try proto.Request.parse(p2));
    resp = try nextAnswer(c, a, &out);
    try std.testing.expectEqual(@as(u64, 52), resp.header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.overloaded), resp.header.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.data, "was not run") != null);
    try std.testing.expectEqual(@as(u64, 1), sm.snapshot().cross_shard_overloaded[@intFromEnum(ShardMetrics.CrossShardClass.client_forward)]);
    for (a.reply_pool.slots, 0..) |s, i| {
        if (s.active) _ = a.reply_pool.release(.{ .slot = @intCast(i), .gen = s.gen }, 1);
    }

    // Unanswered by their deadlines, a read's client is told to retry and a
    // write's that it may still apply; the slots stay held for the late
    // answers, which are dropped when they come.
    const p3 = try testRequest(.ping, 53, "", "");
    defer std.testing.allocator.free(p3);
    const put = try testRequest(.kv_put, 54, "k", "v");
    defer std.testing.allocator.free(put);
    a.forwardToShard(1, c.conn, try proto.Request.parse(p3));
    a.forwardToShard(1, c.conn, try proto.Request.parse(put));
    try std.testing.expectEqual(@as(u16, 2), c.conn.forwards_in_flight);
    // Waiting since the clock's zero: shown as the oldest wait, not yet
    // past a deadline. (Times are set, not subtracted: a monotonic clock
    // can read less than any interval a test would take off it.)
    for (a.reply_pool.slots) |*s| {
        if (s.active) s.taken_ms = 0;
    }
    a.reply_sweep_ms = 0;
    const before = Shard.nowMs() / 1000;
    a.expireReplySlots();
    const after = Shard.nowMs() / 1000;
    try std.testing.expectEqual(@as(u16, 2), c.conn.forwards_in_flight);
    const oldest = sm.snapshot().oldest_cross_shard_wait_s;
    try std.testing.expect(oldest >= before and oldest <= after);
    // The tick sweeps: past their deadlines, the clients are told without
    // anything else asking.
    for (a.reply_pool.slots) |*s| {
        if (s.active) s.answer_due_ms = 0;
    }
    a.reply_sweep_ms = 0;
    _ = try a.tick(0);
    try std.testing.expectEqual(@as(u16, 0), c.conn.forwards_in_flight);
    try std.testing.expectEqual(@as(usize, 2), a.reply_pool.taken());
    // Sent by the sweep itself: nothing else may touch an idle client's
    // connection, so read the socket without flushing.
    var told: [2]proto.Response = undefined;
    var got: usize = 0;
    var tries: usize = 0;
    while (got < 2 * @sizeOf(proto.ResponseHeader) and tries < 1000) : (tries += 1) {
        const n = std.c.read(c.pair[1], out[got..].ptr, out.len - got);
        if (n > 0) got += @intCast(n);
    }
    var at: usize = 0;
    for (&told) |*r| {
        r.* = try proto.Response.parse(out[at..got]);
        at += @sizeOf(proto.ResponseHeader) + r.header.data_len;
    }
    // In slot order, not send order.
    const read_told = if (told[0].header.request_id == 53) told[0] else told[1];
    const write_told = if (told[0].header.request_id == 54) told[0] else told[1];
    try std.testing.expectEqual(@as(u64, 53), read_told.header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.unavailable), read_told.header.status);
    try std.testing.expectEqualStrings("unavailable: shard 1 did not answer in 3 s — retry", read_told.data);
    try std.testing.expectEqual(@as(u64, 54), write_told.header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.unavailable), write_told.header.status);
    try std.testing.expectEqualStrings("unavailable: shard 1 did not answer in 3 s — the request may still apply", write_told.data);
    try std.testing.expectEqual(@as(u64, 2), sm.snapshot().cross_shard_timeouts);
    // The late answers free the slots and reach no one.
    while (b.drainInbox() > 0) {}
    _ = a.drainInbox();
    try std.testing.expectEqual(@as(usize, 0), a.reply_pool.taken());
    a.flushToClient(c.conn.fd);
    try std.testing.expect(std.c.read(c.pair[1], &out, out.len) < 0);

    // A request whose answer carries what it took waits for that answer as
    // long as the slot is held.
    const dq = try testRequest(.queue_dequeue, 56, "q", "");
    defer std.testing.allocator.free(dq);
    // Counted from when it is sent, however long it waited: the slot is
    // not let go before its client is told.
    c.conn.head_since_ms = 0;
    a.forwardToShard(1, c.conn, try proto.Request.parse(dq));
    c.conn.head_since_ms = null;
    for (a.reply_pool.slots) |s| {
        if (!s.active) continue;
        try std.testing.expectEqual(reply_pool_mod.SLOT_DEADLINE_MS, s.answer_after_ms);
        try std.testing.expectEqual(s.due_ms, s.answer_due_ms);
        try std.testing.expect(s.writes);
    }
    for ([_]proto.OpCode{ .queue_dequeue, .stream_group_read, .stream_group_claim, .action_await }) |op| try std.testing.expect(Shard.takesItems(@intFromEnum(op)));
    for ([_]proto.OpCode{ .kv_get, .stream_read, .kv_put, .queue_complete }) |op| try std.testing.expect(!Shard.takesItems(@intFromEnum(op)));
    while (b.drainInbox() > 0) {}
    _ = a.drainInbox();
    try std.testing.expectEqual(@as(usize, 0), a.reply_pool.taken());
    a.flushToClient(c.conn.fd);
    while (std.c.read(c.pair[1], &out, out.len) > 0) {}

    // Never answered at all: past the slot's own deadline it comes back.
    const p4 = try testRequest(.ping, 55, "", "");
    defer std.testing.allocator.free(p4);
    a.forwardToShard(1, c.conn, try proto.Request.parse(p4));
    for (a.reply_pool.slots) |*s| {
        if (s.active) {
            s.answer_due_ms = 0;
            s.due_ms = 0;
        }
    }
    a.reply_sweep_ms = 0;
    _ = try a.tick(0);
    try std.testing.expectEqual(@as(usize, 0), a.reply_pool.taken());
    try std.testing.expectEqual(@as(u64, 0), sm.snapshot().oldest_cross_shard_wait_s);
    resp = try nextAnswer(c, a, &out);
    try std.testing.expectEqual(@as(u64, 55), resp.header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.unavailable), resp.header.status);
    _ = b.drainInbox();
    _ = a.drainInbox();
    a.flushToClient(c.conn.fd);
    try std.testing.expect(std.c.read(c.pair[1], &out, out.len) < 0);
}

test "Shard: an answer for another shard's client that holds no reply slot is dropped and counted, never sent" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    var sm = ShardMetrics{ .shard_id = 1 };
    two.shards[1].shard_metrics = &sm;
    defer two.shards[1].shard_metrics = null;
    two.shards[1].deliverDeferred(ReplyTo.socketOf(0, 5, 1), "x");
    try std.testing.expectEqual(@as(u64, 1), two.shards[1].replies_dropped);
    try std.testing.expectEqual(@as(u64, 1), sm.snapshot().replies_dropped);
    try std.testing.expectEqual(@as(usize, 0), two.shards[0].mailbox.replies.pending());
}

/// A key `a` sends to shard `target`, for requests that must go there.
fn keyOwnedBy(a: *Shard, target: u16, buf: []u8) ![]const u8 {
    for (0..1000) |i| {
        const key = try std.fmt.bufPrint(buf, "k{d}", .{i});
        const bytes = try testRequest(.kv_get, 0, key, "");
        defer std.testing.allocator.free(bytes);
        const to = a.forwardTarget(try proto.Request.parse(bytes)) orelse continue;
        if (to.target == target) return key;
    }
    return error.NoKeyForShard;
}

/// The answers waiting on a test client's socket, parsed in order.
fn answersOn(c: TestClient, shard: *Shard, out: []u8, into: []proto.Response) !void {
    const got = try readAnswer(c.pair[1], shard, c.conn.fd, out, into.len * @sizeOf(proto.ResponseHeader));
    var at: usize = 0;
    for (into) |*r| {
        r.* = try proto.Response.parse(out[at..got]);
        at += @sizeOf(proto.ResponseHeader) + r.header.data_len;
    }
    try std.testing.expectEqual(got, at);
}

fn pausedFor(sm: *const ShardMetrics, why: Connection.WaitReason) u64 {
    return sm.snapshot().connections_paused[@intFromEnum(why)];
}

test "Shard: a connection with 64 requests waiting on other shards is not read until one is answered; its next request then goes" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const b = &two.shards[1];
    var sm = ShardMetrics{ .shard_id = 0 };
    a.shard_metrics = &sm;
    defer a.shard_metrics = null;
    const c = try TestClient.open(a);
    defer _ = std.c.close(c.pair[1]);
    try a.reactor.addSource(.{ .fd = c.conn.fd, .tag = .client_read, .interests = .{ .readable = true } });
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);

    const n = Connection.MAX_FORWARDS_IN_FLIGHT + 1;
    var reqs: [n][]u8 = undefined;
    for (&reqs, 0..) |*r, i| r.* = try testRequest(.kv_get, 100 + i, key, "");
    defer for (reqs) |r| std.testing.allocator.free(r);
    const all = try std.mem.concat(std.testing.allocator, u8, &reqs);
    defer std.testing.allocator.free(all);
    try std.testing.expectEqual(all.len, feedClient(c.pair[1], a, c.conn.fd, all));

    // The last stays unread, and nothing about it has run.
    try std.testing.expectEqual(@as(usize, n - 1), a.reply_pool.taken());
    try std.testing.expectEqual(Connection.MAX_FORWARDS_IN_FLIGHT, c.conn.forwards_in_flight);
    try std.testing.expectEqual(Connection.WaitReason.in_flight, c.conn.waiting.?.reason);
    try std.testing.expectEqual(reqs[n - 1].len, c.conn.read_buf.readable());
    try std.testing.expectEqual(@as(u64, n - 1), a.requests_dispatched);
    // Waiting on its own limit, it holds no one else back from shard 1.
    {
        const other = try TestClient.open(a);
        defer _ = std.c.close(other.pair[1]);
        const one = try testRequest(.kv_get, 950, key, "");
        defer std.testing.allocator.free(one);
        try std.testing.expectEqual(one.len, feedClient(other.pair[1], a, other.conn.fd, one));
        try std.testing.expect(other.conn.waiting == null);
        try std.testing.expectEqual(@as(u16, 1), other.conn.forwards_in_flight);
        a.closeConnection(other.conn.fd);
    }
    try std.testing.expect(!a.reactor.sources.get(c.conn.fd).?.interests.readable);
    // What the client sends meanwhile stays in the kernel, not read.
    const later = try testRequest(.ping, 999, "", "");
    defer std.testing.allocator.free(later);
    try std.testing.expectEqual(@as(isize, @intCast(later.len)), std.c.write(c.pair[1], later.ptr, later.len));
    a.readFromClient(c.conn.fd);
    try std.testing.expectEqual(reqs[n - 1].len, c.conn.read_buf.readable());
    a.settleConnections();
    try std.testing.expect(c.conn.waiting != null);
    try std.testing.expectEqual(@as(u64, 1), pausedFor(&sm, .in_flight));

    // Answered, the connection's count comes down, and at the end of the
    // tick its next request goes.
    while (b.drainInbox() > 0) {}
    _ = a.drainInbox();
    try std.testing.expectEqual(@as(u16, 0), c.conn.forwards_in_flight);
    a.settleConnections();
    try std.testing.expect(c.conn.waiting == null);
    try std.testing.expect(a.reactor.sources.get(c.conn.fd).?.interests.readable);
    try std.testing.expectEqual(@as(usize, 1), a.reply_pool.taken());
    try std.testing.expectEqual(@as(u16, 1), c.conn.forwards_in_flight);
    try std.testing.expectEqual(@as(usize, 0), c.conn.read_buf.readable());
    try std.testing.expectEqual(@as(u64, 0), pausedFor(&sm, .in_flight));
    a.readFromClient(c.conn.fd);
    try std.testing.expectEqual(@as(u64, n + 2), a.requests_dispatched);
    while (b.drainInbox() > 0) {}
    _ = a.drainInbox();
    try std.testing.expectEqual(@as(u16, 0), c.conn.forwards_in_flight);
    var out: [64 * 1024]u8 = undefined;
    var answers: [n + 1]proto.Response = undefined;
    try answersOn(c, a, &out, &answers);
    // The ping ran here while the last get was on its way.
    for (answers[0 .. n - 1], 0..) |r, i| try std.testing.expectEqual(@as(u64, 100 + i), r.header.request_id);
    try std.testing.expectEqual(@as(u64, 999), answers[n - 1].header.request_id);
    try std.testing.expectEqual(@as(u64, 100 + n - 1), answers[n].header.request_id);
}

test "Shard: a connection has at most 8 blocking reads on other shards; the next waits, holding what is behind it, and once it has waited ASK_WAIT_MS is refused unrun, with the reads buffered behind it" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const b = &two.shards[1];
    const c = try TestClient.open(a);
    defer _ = std.c.close(c.pair[1]);
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);
    var opt_buf: [16]u8 = undefined;
    var ob = proto.OptionsBuilder.init(&opt_buf);
    try ob.addU32(.block_ms, 5000);

    // Ten blocking reads of a key nobody writes, then a request for this
    // shard.
    const reads = Connection.MAX_BLOCKING_IN_FLIGHT + 2;
    var reqs: [reads + 1][]u8 = undefined;
    for (reqs[0..reads], 0..) |*r, i| r.* = try testRequestWith(.kv_get, 200 + i, key, "", ob.getOptions());
    reqs[reads] = try testRequest(.ping, 300, "", "");
    defer for (reqs) |r| std.testing.allocator.free(r);
    const all = try std.mem.concat(std.testing.allocator, u8, &reqs);
    defer std.testing.allocator.free(all);
    try std.testing.expectEqual(all.len, feedClient(c.pair[1], a, c.conn.fd, all));
    while (b.drainInbox() > 0) {}
    try std.testing.expectEqual(@as(u16, Connection.MAX_BLOCKING_IN_FLIGHT), b.waiter_pool.countByKind(.kv_get));
    try std.testing.expectEqual(Connection.MAX_BLOCKING_IN_FLIGHT, c.conn.blocking_in_flight);
    try std.testing.expectEqual(Connection.MAX_BLOCKING_IN_FLIGHT, a.reply_pool.heldBy(.blocking, 1));
    for (a.reply_pool.slots) |s| {
        if (s.active) try std.testing.expectEqual(5000 + reply_pool_mod.DEADLINE_MS, s.answer_after_ms);
    }
    try std.testing.expectEqual(Connection.WaitReason.blocking_reads, c.conn.waiting.?.reason);
    try std.testing.expectEqual(@as(u64, Connection.MAX_BLOCKING_IN_FLIGHT), a.requests_dispatched);

    // Ordinary requests from another connection still go.
    const other = try TestClient.open(a);
    defer _ = std.c.close(other.pair[1]);
    const get = try testRequest(.kv_get, 400, key, "");
    defer std.testing.allocator.free(get);
    try std.testing.expectEqual(get.len, feedClient(other.pair[1], a, other.conn.fd, get));
    try std.testing.expect(other.conn.waiting == null);
    try std.testing.expectEqual(@as(usize, Connection.MAX_BLOCKING_IN_FLIGHT + 1), a.reply_pool.taken());

    // Still in time: nothing changes.
    a.settleConnections();
    try std.testing.expect(c.conn.waiting != null);

    // Waited long enough (set, not subtracted: a monotonic clock can read
    // less than an interval a test would take off it): both reads over the
    // cap are refused together, told they did not run, and the request
    // behind them runs.
    c.conn.head_since_ms = 0;
    a.settleConnections();
    try std.testing.expect(c.conn.waiting == null);
    try std.testing.expectEqual(@as(u64, Connection.MAX_BLOCKING_IN_FLIGHT + 2), a.requests_dispatched);
    try std.testing.expectEqual(@as(usize, 0), c.conn.read_buf.readable());
    var out: [1024]u8 = undefined;
    var answers: [3]proto.Response = undefined;
    try answersOn(c, a, &out, &answers);
    for (answers[0..2], 0..) |r, i| {
        try std.testing.expectEqual(@as(u64, 200 + Connection.MAX_BLOCKING_IN_FLIGHT + i), r.header.request_id);
        try std.testing.expectEqual(@intFromEnum(proto.StatusCode.overloaded), r.header.status);
        try std.testing.expect(std.mem.indexOf(u8, r.data, "was not run") != null);
    }
    try std.testing.expectEqual(@as(u64, 300), answers[2].header.request_id);
    try std.testing.expectEqualStrings("PONG", answers[2].data);
    try std.testing.expectEqual(Connection.MAX_BLOCKING_IN_FLIGHT, c.conn.blocking_in_flight);
    // Its reads' clients told at their deadline, the connection's counts
    // come down with them.
    for (a.reply_pool.slots) |*s| {
        if (s.active) s.answer_due_ms = 0;
    }
    a.reply_sweep_ms = 0;
    a.expireReplySlots();
    try std.testing.expectEqual(@as(u16, 0), c.conn.blocking_in_flight);
    try std.testing.expectEqual(@as(u16, 0), c.conn.forwards_in_flight);
    try std.testing.expectEqual(@as(u16, 0), other.conn.forwards_in_flight);
    while (b.drainInbox() > 0) {}
    _ = a.drainInbox();
}

test "Shard: a request waits while its target holds its share of ask slots or has no room in its inbox, and goes once there is" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const b = &two.shards[1];
    var sm = ShardMetrics{ .shard_id = 0 };
    a.shard_metrics = &sm;
    defer a.shard_metrics = null;
    const c = try TestClient.open(a);
    defer _ = std.c.close(c.pair[1]);
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);
    const get = try testRequest(.kv_get, 500, key, "");
    defer std.testing.allocator.free(get);

    // Shard 1 holds its full share of ordinary slots.
    var tickets: std.ArrayListUnmanaged(ReplyPool.Ticket) = .empty;
    defer tickets.deinit(std.testing.allocator);
    while (a.reply_pool.take(.ordinary, 1, -1, 0, 0, false, Shard.nowMs(), Shard.nowMs(), 3000, 0)) |t| try tickets.append(std.testing.allocator, t) else |_| {}
    // A wait over the cap is not held for room: dispatch refuses it.
    var opt_buf: [16]u8 = undefined;
    var ob = proto.OptionsBuilder.init(&opt_buf);
    try ob.addU32(.block_ms, waiter_pool_mod.MAX_BLOCK_MS + 1);
    const too_long = try testRequestWith(.kv_get, 499, key, "", ob.getOptions());
    defer std.testing.allocator.free(too_long);
    // At its blocking cap, it would otherwise wait.
    c.conn.blocking_in_flight = Connection.MAX_BLOCKING_IN_FLIGHT;
    try std.testing.expect(a.holdForward(c.conn, try proto.Request.parse(too_long)) == null);
    c.conn.blocking_in_flight = 0;

    try std.testing.expectEqual(get.len, feedClient(c.pair[1], a, c.conn.fd, get));
    try std.testing.expectEqual(Connection.WaitReason.slots, c.conn.waiting.?.reason);
    a.settleConnections();
    try std.testing.expectEqual(@as(u64, 1), pausedFor(&sm, .slots));

    // A slot back, but this shard's share of shard 1's inbox unread.
    _ = a.reply_pool.release(tickets.pop().?, 1);
    while (b.mailbox.hasShare(0)) try std.testing.expect(b.mailbox.sendOnShare(.{ .tag = .forward_request, .src_shard = 0 }));
    a.settleConnections();
    try std.testing.expectEqual(Connection.WaitReason.inbox, c.conn.waiting.?.reason);
    try std.testing.expectEqual(@as(u64, 0), pausedFor(&sm, .slots));
    try std.testing.expectEqual(@as(u64, 1), pausedFor(&sm, .inbox));
    try std.testing.expectEqual(@as(usize, tickets.items.len), a.reply_pool.taken());

    // Some drained, but taken again before this shard looks: it asks to be
    // told again.
    try std.testing.expect(b.mailbox.shares[0].wanted.load(.seq_cst));
    var one: [1]InboxMessage = undefined;
    try std.testing.expectEqual(@as(usize, 1), b.mailbox.inbox.drain(&one));
    try std.testing.expect(b.mailbox.returnShare(one[0]));
    try std.testing.expect(b.mailbox.sendOnShare(.{ .tag = .forward_request, .src_shard = 0 }));
    a.settleConnections();
    try std.testing.expectEqual(Connection.WaitReason.inbox, c.conn.waiting.?.reason);
    try std.testing.expect(b.mailbox.shares[0].wanted.load(.seq_cst));

    // Shard 1 drains it and wakes this shard, and the request goes.
    _ = a.mailbox.wake.take();
    while (b.drainInbox() > 0) {}
    try std.testing.expect(a.mailbox.wake.take() & mailbox_mod.Flag.share_returned.bit() != 0);
    a.settleConnections();
    try std.testing.expect(c.conn.waiting == null);
    try std.testing.expectEqual(@as(usize, tickets.items.len + 1), a.reply_pool.taken());
    try std.testing.expectEqual(@as(u64, 0), pausedFor(&sm, .inbox));

    // A connection closed while it waits leaves nothing behind.
    for (tickets.items) |t| _ = a.reply_pool.release(t, 1);
    while (b.mailbox.hasShare(0)) try std.testing.expect(b.mailbox.sendOnShare(.{ .tag = .forward_request, .src_shard = 0 }));
    const again = try testRequest(.kv_get, 501, key, "");
    defer std.testing.allocator.free(again);
    try std.testing.expectEqual(again.len, feedClient(c.pair[1], a, c.conn.fd, again));
    a.settleConnections();
    try std.testing.expectEqual(@as(u64, 1), pausedFor(&sm, .inbox));
    a.closeConnection(c.conn.fd);
    a.settleConnections();
    try std.testing.expectEqual(@as(usize, 0), a.waiting_fds.items.len);
    try std.testing.expectEqual(@as(u64, 0), pausedFor(&sm, .inbox));
    while (b.drainInbox() > 0) {}
    _ = a.drainInbox();
}

test "Shard: waiting connections go in the order they began waiting, no more than there is room for; the rest keep their place and their clock, and a client gone meanwhile is not run" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const b = &two.shards[1];
    var sm = ShardMetrics{ .shard_id = 0 };
    a.shard_metrics = &sm;
    defer a.shard_metrics = null;
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);

    // Shard 1 holds its full share of ordinary slots.
    var tickets: std.ArrayListUnmanaged(ReplyPool.Ticket) = .empty;
    defer tickets.deinit(std.testing.allocator);
    while (a.reply_pool.take(.ordinary, 1, -1, 0, 0, false, Shard.nowMs(), Shard.nowMs(), 3000, 0)) |t| try tickets.append(std.testing.allocator, t) else |_| {}

    // Three clients wait for it; the first has a second request behind its
    // first.
    var clients: [3]TestClient = undefined;
    for (&clients) |*cl| cl.* = try TestClient.open(a);
    defer for (clients[0..2]) |cl| {
        _ = std.c.close(cl.pair[1]);
    };
    var reqs: [4][]u8 = undefined;
    for (&reqs, 0..) |*r, i| r.* = try testRequest(.kv_get, 600 + i, key, "");
    defer for (reqs) |r| std.testing.allocator.free(r);
    const first_two = try std.mem.concat(std.testing.allocator, u8, reqs[0..2]);
    defer std.testing.allocator.free(first_two);
    try std.testing.expectEqual(first_two.len, feedClient(clients[0].pair[1], a, clients[0].conn.fd, first_two));
    for (clients[1..], reqs[2..]) |cl, r| try std.testing.expectEqual(r.len, feedClient(cl.pair[1], a, cl.conn.fd, r));
    for (clients) |cl| try std.testing.expect(cl.conn.waiting != null);
    a.settleConnections();
    try std.testing.expectEqual(@as(u64, 3), pausedFor(&sm, .slots));
    const since = clients[1].conn.head_since_ms.?;

    // One slot back: only the first goes. Its second request, with no room
    // either, waits rather than being refused; the others keep waiting on
    // their first clock.
    _ = a.reply_pool.release(tickets.pop().?, 1);
    a.settleConnections();
    try std.testing.expectEqual(@as(u16, 1), clients[0].conn.forwards_in_flight);
    try std.testing.expectEqual(Connection.WaitReason.slots, clients[0].conn.waiting.?.reason);
    try std.testing.expectEqual(reqs[1].len, clients[0].conn.read_buf.readable());
    for (clients[1..]) |cl| {
        try std.testing.expect(cl.conn.waiting != null);
        try std.testing.expectEqual(@as(u16, 0), cl.conn.forwards_in_flight);
    }
    try std.testing.expectEqual(since, clients[1].conn.head_since_ms.?);
    try std.testing.expectEqual(@as(u64, 3), pausedFor(&sm, .slots));

    // One slot back, and the third's client gone meanwhile: the line goes
    // round a request at a time, so the second, ahead of the first's second
    // request, takes it; the third is closed without running.
    _ = std.c.close(clients[2].pair[1]);
    clients[2].conn.head_since_ms = 0;
    // The second has waited 2.5 s: a second is left it once sent.
    const second_since = Shard.nowMs() -| 2500;
    clients[1].conn.head_since_ms = second_since;
    _ = a.reply_pool.release(tickets.pop().?, 1);
    const dispatched = a.requests_dispatched;
    const refused_before = a.forwards_refused;
    const fd2 = clients[2].conn.fd;
    a.settleConnections();
    try std.testing.expectEqual(dispatched + 1, a.requests_dispatched);
    try std.testing.expectEqual(refused_before, a.forwards_refused);
    try std.testing.expectEqual(@as(u16, 1), clients[1].conn.forwards_in_flight);
    try std.testing.expect(clients[1].conn.waiting == null);
    // Its deadline counts from when it first waited, with a second at least
    // once sent.
    for (a.reply_pool.slots) |s| {
        if (s.active and s.request_id == 602) try std.testing.expectEqual(@max(second_since + reply_pool_mod.DEADLINE_MS, s.taken_ms + reply_pool_mod.MIN_DEADLINE_MS), s.answer_due_ms);
    }
    try std.testing.expectEqual(@as(u16, 1), clients[0].conn.forwards_in_flight);
    try std.testing.expect(clients[0].conn.waiting != null);
    try std.testing.expect(a.getConnection(fd2) == null);
    try std.testing.expectEqual(@as(u64, 1), pausedFor(&sm, .slots));

    // The first's second request has waited ASK_WAIT_MS with no room: it is
    // refused, unrun.
    clients[0].conn.head_since_ms = 0;
    a.settleConnections();
    try std.testing.expect(clients[0].conn.waiting == null);
    try std.testing.expectEqual(refused_before + 1, a.forwards_refused);
    try std.testing.expectEqual(@as(u64, 0), pausedFor(&sm, .slots));
    var out: [512]u8 = undefined;
    var answers: [2]proto.Response = undefined;
    while (b.drainInbox() > 0) {}
    _ = a.drainInbox();
    try answersOn(clients[0], a, &out, &answers);
    const refused = if (answers[0].header.request_id == 601) answers[0] else answers[1];
    try std.testing.expectEqual(@as(u64, 601), refused.header.request_id);
    try std.testing.expectEqualStrings("overloaded: shard 1 is not keeping up; this request was not run — back off and retry", refused.data);
}

test "Shard: a give-up survives a pause for unsent answers: the requests behind it that would wait are refused when reading resumes" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    var sm = ShardMetrics{ .shard_id = 0 };
    a.shard_metrics = &sm;
    defer a.shard_metrics = null;
    const c = try TestClient.open(a);
    defer _ = std.c.close(c.pair[1]);
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);
    var tickets: std.ArrayListUnmanaged(ReplyPool.Ticket) = .empty;
    defer tickets.deinit(std.testing.allocator);
    while (a.reply_pool.take(.ordinary, 1, -1, 0, 0, false, Shard.nowMs(), Shard.nowMs(), 3000, 0)) |t| try tickets.append(std.testing.allocator, t) else |_| {}

    var reqs: [2][]u8 = undefined;
    for (&reqs, 0..) |*r, i| r.* = try testRequest(.kv_get, 700 + i, key, "");
    defer for (reqs) |r| std.testing.allocator.free(r);
    const both = try std.mem.concat(std.testing.allocator, u8, &reqs);
    defer std.testing.allocator.free(both);
    try std.testing.expectEqual(both.len, feedClient(c.pair[1], a, c.conn.fd, both));
    try std.testing.expect(c.conn.waiting != null);

    // Unsent answers past the pause mark, and a client not reading them.
    const small: c_int = 4096;
    _ = std.c.setsockopt(c.pair[0], std.posix.SOL.SOCKET, std.posix.SO.SNDBUF, @ptrCast(&small), @sizeOf(c_int));
    const filler = try std.testing.allocator.alloc(u8, 2 * Shard.PAUSE_READS_AT);
    defer std.testing.allocator.free(filler);
    @memset(filler, 0);
    _ = c.conn.queueWrite(filler);
    c.conn.head_since_ms = 0;
    a.settleConnections();
    // Given up on, but paused for its unsent answers before either request
    // is refused.
    try std.testing.expect(c.conn.reads_paused);
    try std.testing.expectEqual(@as(u64, 1), sm.snapshot().connections_paused[ShardMetrics.PAUSE_REASONS.len - 1]);
    try std.testing.expectEqual(both.len, c.conn.read_buf.readable());
    try std.testing.expectEqual(both.len, c.conn.give_up_bytes);
    try std.testing.expectEqual(@as(u64, 0), a.forwards_refused);

    // Drained, it reads on: both are refused at once, not made to wait.
    var sink: [16 * 1024]u8 = undefined;
    while (c.conn.reads_paused) {
        _ = std.c.read(c.pair[1], &sink, sink.len);
        a.flushToClient(c.conn.fd);
    }
    a.settleConnections();
    try std.testing.expect(c.conn.waiting == null);
    try std.testing.expectEqual(@as(usize, 0), c.conn.give_up_bytes);
    try std.testing.expectEqual(@as(usize, 0), c.conn.read_buf.readable());
    try std.testing.expectEqual(@as(u64, 2), a.forwards_refused);
}

test "Shard: a client that closes has its blocking reads on another shard dropped there, and their slots come back with the empty answers" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const b = &two.shards[1];
    const c = try TestClient.open(a);
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);
    var opt_buf: [16]u8 = undefined;
    var ob = proto.OptionsBuilder.init(&opt_buf);
    try ob.addU32(.block_ms, 60_000);
    var reqs: [2][]u8 = undefined;
    for (&reqs, 0..) |*r, i| r.* = try testRequestWith(.kv_get, 800 + i, key, "", ob.getOptions());
    defer for (reqs) |r| std.testing.allocator.free(r);
    const both = try std.mem.concat(std.testing.allocator, u8, &reqs);
    defer std.testing.allocator.free(both);
    try std.testing.expectEqual(both.len, feedClient(c.pair[1], a, c.conn.fd, both));
    while (b.drainInbox() > 0) {}
    try std.testing.expectEqual(@as(u16, 2), b.waiter_pool.countByKind(.kv_get));
    try std.testing.expectEqual(@as(usize, 2), a.reply_pool.taken());

    _ = std.c.close(c.pair[1]);
    a.closeConnection(c.conn.fd);
    // Held until answered, so the slots never count for less than what is
    // parked there; one word to shard 1 for both reads.
    try std.testing.expectEqual(@as(usize, 2), a.reply_pool.taken());
    try std.testing.expectEqual(@as(usize, 1), b.mailbox.inbox.pending());
    while (b.drainInbox() > 0) {}
    try std.testing.expectEqual(@as(u16, 0), b.waiter_pool.countByKind(.kv_get));
    try std.testing.expectEqual(@as(usize, 2), a.mailbox.replies.pending());
    _ = a.drainInbox();
    try std.testing.expectEqual(@as(usize, 0), a.reply_pool.taken());
    try std.testing.expectEqual(@as(u64, 0), a.replies_dropped + b.replies_dropped);
}

test "Shard: a closing client's blocking read already answered is not cancelled, and the slot it had, now another's, is left alone" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const b = &two.shards[1];
    var keys: [2][16]u8 = undefined;
    var names: [2][]const u8 = undefined;
    var found: usize = 0;
    for (0..1000) |i| {
        if (found == 2) break;
        const k = try std.fmt.bufPrint(&keys[found], "k{d}", .{i});
        const probe = try testRequest(.kv_get, 0, k, "");
        defer std.testing.allocator.free(probe);
        const to = a.forwardTarget(try proto.Request.parse(probe)) orelse continue;
        if (to.target != 1) continue;
        names[found] = k;
        found += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), found);
    var opt_buf: [16]u8 = undefined;
    var ob = proto.OptionsBuilder.init(&opt_buf);
    try ob.addU32(.block_ms, 60_000);

    // A client reads both keys, blocking; the first is written and answered.
    const c = try TestClient.open(a);
    var reads: [2][]u8 = undefined;
    for (&reads, names, 0..) |*r, k, i| r.* = try testRequestWith(.kv_get, 810 + i, k, "", ob.getOptions());
    defer for (reads) |r| std.testing.allocator.free(r);
    const both = try std.mem.concat(std.testing.allocator, u8, &reads);
    defer std.testing.allocator.free(both);
    try std.testing.expectEqual(both.len, feedClient(c.pair[1], a, c.conn.fd, both));
    while (b.drainInbox() > 0) {}
    const writer = try TestClient.open(b);
    defer _ = std.c.close(writer.pair[1]);
    const put = try testRequest(.kv_put, 820, names[0], "v");
    defer std.testing.allocator.free(put);
    try std.testing.expectEqual(put.len, feedClient(writer.pair[1], b, writer.conn.fd, put));
    _ = a.drainInbox();
    try std.testing.expectEqual(@as(u16, 1), c.conn.blocking_in_flight);

    // Another client's read takes the freed slot; then the first closes.
    const d = try TestClient.open(a);
    defer _ = std.c.close(d.pair[1]);
    const again = try testRequestWith(.kv_get, 831, names[1], "", ob.getOptions());
    defer std.testing.allocator.free(again);
    try std.testing.expectEqual(again.len, feedClient(d.pair[1], a, d.conn.fd, again));
    _ = std.c.close(c.pair[1]);
    a.closeConnection(c.conn.fd);
    // One cancel, on this shard's share of shard 1's inbox.
    try std.testing.expectEqual(@as(u16, 2), b.mailbox.shares[0].unread.load(.seq_cst));
    while (b.drainInbox() > 0) {}
    _ = a.drainInbox();
    // The other client's read still waits there, and is answered.
    try std.testing.expectEqual(@as(u16, 1), b.waiter_pool.countByKind(.kv_get));
    const put2 = try testRequest(.kv_put, 821, names[1], "w");
    defer std.testing.allocator.free(put2);
    try std.testing.expectEqual(put2.len, feedClient(writer.pair[1], b, writer.conn.fd, put2));
    _ = a.drainInbox();
    var out: [256]u8 = undefined;
    const answer = try nextAnswer(d, a, &out);
    try std.testing.expectEqual(@as(u64, 831), answer.header.request_id);
    // A woken get answers the key's version, then its value.
    try std.testing.expectEqual(@as(usize, 9), answer.data.len);
    try std.testing.expectEqual(@as(u64, 1), std.mem.readInt(u64, answer.data[0..8], .little));
    try std.testing.expectEqualStrings("w", answer.data[8..]);
}

test "Shard: a closing client's cancel that finds no room in the target's inbox is sent once there is" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const b = &two.shards[1];
    const c = try TestClient.open(a);
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);
    var opt_buf: [16]u8 = undefined;
    var ob = proto.OptionsBuilder.init(&opt_buf);
    try ob.addU32(.block_ms, 60_000);
    const read = try testRequestWith(.kv_get, 840, key, "", ob.getOptions());
    defer std.testing.allocator.free(read);
    try std.testing.expectEqual(read.len, feedClient(c.pair[1], a, c.conn.fd, read));
    while (b.drainInbox() > 0) {}
    try std.testing.expectEqual(@as(u16, 1), b.waiter_pool.countByKind(.kv_get));

    // This shard's share of shard 1's inbox is full when the client closes.
    while (b.mailbox.hasShare(0)) try std.testing.expect(b.mailbox.sendOnShare(.{ .tag = .forward_request, .src_shard = 0 }));
    _ = std.c.close(c.pair[1]);
    a.closeConnection(c.conn.fd);
    try std.testing.expectEqual(@as(usize, 1), a.pending_cancels.items.len);
    a.settleConnections();
    try std.testing.expectEqual(@as(usize, 1), a.pending_cancels.items.len);
    // Drained, the cancel goes, and the read is dropped there.
    while (b.drainInbox() > 0) {}
    a.settleConnections();
    try std.testing.expectEqual(@as(usize, 0), a.pending_cancels.items.len);
    while (b.drainInbox() > 0) {}
    try std.testing.expectEqual(@as(u16, 0), b.waiter_pool.countByKind(.kv_get));
    _ = a.drainInbox();
    try std.testing.expectEqual(@as(usize, 0), a.reply_pool.taken());
}

test "Shard: room that comes back goes one request to each waiter in turn: a resumed connection's next request waits behind the rest" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);
    var tickets: std.ArrayListUnmanaged(ReplyPool.Ticket) = .empty;
    defer tickets.deinit(std.testing.allocator);
    while (a.reply_pool.take(.ordinary, 1, -1, 0, 0, false, Shard.nowMs(), Shard.nowMs(), 3000, 0)) |t| try tickets.append(std.testing.allocator, t) else |_| {}
    var clients: [3]TestClient = undefined;
    for (&clients) |*cl| cl.* = try TestClient.open(a);
    defer for (clients) |cl| {
        _ = std.c.close(cl.pair[1]);
    };
    // The first pipelines two requests; the others send one each.
    var reqs: [4][]u8 = undefined;
    for (&reqs, 0..) |*r, i| r.* = try testRequest(.kv_get, 900 + i, key, "");
    defer for (reqs) |r| std.testing.allocator.free(r);
    const first_two = try std.mem.concat(std.testing.allocator, u8, reqs[0..2]);
    defer std.testing.allocator.free(first_two);
    try std.testing.expectEqual(first_two.len, feedClient(clients[0].pair[1], a, clients[0].conn.fd, first_two));
    for (clients[1..], reqs[2..]) |cl, r| try std.testing.expectEqual(r.len, feedClient(cl.pair[1], a, cl.conn.fd, r));

    // Room for two: the first and the second each send one; the first's
    // next request and the third wait.
    _ = a.reply_pool.release(tickets.pop().?, 1);
    _ = a.reply_pool.release(tickets.pop().?, 1);
    a.settleConnections();
    try std.testing.expectEqual(@as(u16, 1), clients[0].conn.forwards_in_flight);
    try std.testing.expectEqual(@as(u16, 1), clients[1].conn.forwards_in_flight);
    try std.testing.expectEqual(@as(u16, 0), clients[2].conn.forwards_in_flight);
    try std.testing.expect(clients[0].conn.waiting != null);
    try std.testing.expect(clients[2].conn.waiting != null);
    try std.testing.expect(!clients[2].conn.has_turn);
    while (two.shards[1].drainInbox() > 0) {}
    _ = a.drainInbox();
}

test "Shard: a waiter held back only because those ahead of it will fill the target's inbox share asks to be woken when it drains" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const b = &two.shards[1];
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);
    var tickets: std.ArrayListUnmanaged(ReplyPool.Ticket) = .empty;
    defer tickets.deinit(std.testing.allocator);
    while (a.reply_pool.take(.ordinary, 1, -1, 0, 0, false, Shard.nowMs(), Shard.nowMs(), 3000, 0)) |t| try tickets.append(std.testing.allocator, t) else |_| {}
    var clients: [2]TestClient = undefined;
    for (&clients) |*cl| cl.* = try TestClient.open(a);
    defer for (clients) |cl| {
        _ = std.c.close(cl.pair[1]);
    };
    var reqs: [2][]u8 = undefined;
    for (&reqs, 0..) |*r, i| r.* = try testRequest(.kv_get, 890 + i, key, "");
    defer for (reqs) |r| std.testing.allocator.free(r);
    for (clients, reqs) |cl, r| try std.testing.expectEqual(r.len, feedClient(cl.pair[1], a, cl.conn.fd, r));
    for (clients) |cl| try std.testing.expectEqual(Connection.WaitReason.slots, cl.conn.waiting.?.reason);

    // Room in shard 1's inbox for one more; slots come back for both.
    while (b.mailbox.shareRoom(0) > 1) try std.testing.expect(b.mailbox.sendOnShare(.{ .tag = .forward_request, .src_shard = 0 }));
    _ = a.reply_pool.release(tickets.pop().?, 1);
    _ = a.reply_pool.release(tickets.pop().?, 1);
    a.settleConnections();
    try std.testing.expect(clients[0].conn.waiting == null);
    try std.testing.expectEqual(Connection.WaitReason.inbox, clients[1].conn.waiting.?.reason);
    try std.testing.expect(b.mailbox.shares[0].wanted.load(.seq_cst));
    while (b.drainInbox() > 0) {}
    _ = a.drainInbox();
}

test "Shard: a wait on a connection's own limit that becomes a wait for the target's room joins that target's line" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const c = try TestClient.open(a);
    defer _ = std.c.close(c.pair[1]);
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);
    var tickets: std.ArrayListUnmanaged(ReplyPool.Ticket) = .empty;
    defer tickets.deinit(std.testing.allocator);
    while (a.reply_pool.take(.ordinary, 1, -1, 0, 0, false, Shard.nowMs(), Shard.nowMs(), 3000, 0)) |t| try tickets.append(std.testing.allocator, t) else |_| {}
    // At its own in-flight limit (set, as if 64 were out): not in the line.
    c.conn.forwards_in_flight = Connection.MAX_FORWARDS_IN_FLIGHT;
    const get = try testRequest(.kv_get, 860, key, "");
    defer std.testing.allocator.free(get);
    try std.testing.expectEqual(get.len, feedClient(c.pair[1], a, c.conn.fd, get));
    try std.testing.expectEqual(Connection.WaitReason.in_flight, c.conn.waiting.?.reason);
    try std.testing.expectEqual(@as(u32, 0), a.queued[0][1]);
    // Its answers come back; now it waits for shard 1's room, in its line.
    c.conn.forwards_in_flight = 0;
    a.settleConnections();
    try std.testing.expectEqual(Connection.WaitReason.slots, c.conn.waiting.?.reason);
    try std.testing.expectEqual(@as(u32, 1), a.queued[0][1]);
    _ = a.reply_pool.release(tickets.pop().?, 1);
    a.settleConnections();
    try std.testing.expect(c.conn.waiting == null);
    try std.testing.expectEqual(@as(u32, 0), a.queued[0][1]);
    while (two.shards[1].drainInbox() > 0) {}
    _ = a.drainInbox();
}

test "Shard: a shard keeps waiter room for reads from elsewhere — other shards, or other nodes on a cluster member — and a lone shard does not" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var lone = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer lone.deinit();
    try std.testing.expect(!lone.waiter_pool.split);
    var member = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 8, .join, .{});
    defer member.deinit();
    try std.testing.expect(member.waiter_pool.split);
}

test "Shard: a connection's blocking reads on its own shard count toward its 8: the ninth waits until one returns" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);
    var opt_buf: [16]u8 = undefined;
    var ob = proto.OptionsBuilder.init(&opt_buf);
    try ob.addU32(.block_ms, 60_000);
    var reqs: [Connection.MAX_BLOCKING_IN_FLIGHT + 1][]u8 = undefined;
    for (&reqs, 0..) |*r, i| r.* = try testRequestWith(.kv_get, 870 + i, "absent", "", ob.getOptions());
    defer for (reqs) |r| std.testing.allocator.free(r);
    const all = try std.mem.concat(std.testing.allocator, u8, &reqs);
    defer std.testing.allocator.free(all);
    try std.testing.expectEqual(all.len, feedClient(c.pair[1], &shard, c.conn.fd, all));
    try std.testing.expectEqual(@as(u16, Connection.MAX_BLOCKING_IN_FLIGHT), shard.waiter_pool.countByKind(.kv_get));
    try std.testing.expectEqual(Connection.WaitReason.blocking_reads, c.conn.waiting.?.reason);

    // The key is written: the eight return, and the ninth goes.
    const writer = try TestClient.open(&shard);
    defer _ = std.c.close(writer.pair[1]);
    const put = try testRequest(.kv_put, 880, "absent", "v");
    defer std.testing.allocator.free(put);
    try std.testing.expectEqual(put.len, feedClient(writer.pair[1], &shard, writer.conn.fd, put));
    try std.testing.expectEqual(@as(u16, 0), shard.waiter_pool.countByKind(.kv_get));
    shard.settleConnections();
    try std.testing.expect(c.conn.waiting == null);
    try std.testing.expectEqual(@as(usize, 0), c.conn.read_buf.readable());
}

test "Shard: a connection resumed but paused for its unsent answers before its request goes stays in line, and is refused in time if it waits again" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    const c = try TestClient.open(a);
    defer _ = std.c.close(c.pair[1]);
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);
    var tickets: std.ArrayListUnmanaged(ReplyPool.Ticket) = .empty;
    defer tickets.deinit(std.testing.allocator);
    while (a.reply_pool.take(.ordinary, 1, -1, 0, 0, false, Shard.nowMs(), Shard.nowMs(), 3000, 0)) |t| try tickets.append(std.testing.allocator, t) else |_| {}
    const get = try testRequest(.kv_get, 970, key, "");
    defer std.testing.allocator.free(get);
    try std.testing.expectEqual(get.len, feedClient(c.pair[1], a, c.conn.fd, get));
    try std.testing.expect(c.conn.waiting != null);
    // Began waiting a second ago, so a clock started again would show.
    c.conn.head_since_ms = c.conn.head_since_ms.? -| 1000;
    const since = c.conn.head_since_ms.?;

    // Answers it is not reading, past the pause mark.
    const small: c_int = 4096;
    _ = std.c.setsockopt(c.pair[0], std.posix.SOL.SOCKET, std.posix.SO.SNDBUF, @ptrCast(&small), @sizeOf(c_int));
    const filler = try std.testing.allocator.alloc(u8, 2 * Shard.PAUSE_READS_AT);
    defer std.testing.allocator.free(filler);
    @memset(filler, 0);
    _ = c.conn.queueWrite(filler);

    // Room comes back; resumed, it pauses before its request goes.
    const freed = tickets.pop().?;
    _ = a.reply_pool.release(freed, 1);
    a.settleConnections();
    try std.testing.expect(c.conn.reads_paused);
    try std.testing.expect(c.conn.waiting == null);
    a.settleConnections();
    // The room is taken meanwhile; its answers drain and it reads on.
    try tickets.append(std.testing.allocator, try a.reply_pool.take(.ordinary, 1, -1, 0, 0, false, Shard.nowMs(), Shard.nowMs(), 3000, 0));
    var sink: [16 * 1024]u8 = undefined;
    while (c.conn.reads_paused) {
        _ = std.c.read(c.pair[1], &sink, sink.len);
        a.flushToClient(c.conn.fd);
    }
    a.settleConnections();
    try std.testing.expect(c.conn.waiting != null);
    // Its clock runs from its first wait.
    try std.testing.expectEqual(since, c.conn.head_since_ms.?);
    // In line still, and so refused once it has waited ASK_WAIT_MS.
    c.conn.head_since_ms = 0;
    a.settleConnections();
    try std.testing.expect(c.conn.waiting == null);
    try std.testing.expectEqual(@as(u64, 1), a.forwards_refused);
    try std.testing.expectEqual(@as(u32, 0), a.queued[0][1]);
}

test "Shard: a new request for a target waits behind connections already waiting for it, even with room come back" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    const a = &two.shards[0];
    var key_buf: [16]u8 = undefined;
    const key = try keyOwnedBy(a, 1, &key_buf);
    var tickets: std.ArrayListUnmanaged(ReplyPool.Ticket) = .empty;
    defer tickets.deinit(std.testing.allocator);
    while (a.reply_pool.take(.ordinary, 1, -1, 0, 0, false, Shard.nowMs(), Shard.nowMs(), 3000, 0)) |t| try tickets.append(std.testing.allocator, t) else |_| {}

    const first = try TestClient.open(a);
    defer _ = std.c.close(first.pair[1]);
    const later = try TestClient.open(a);
    defer _ = std.c.close(later.pair[1]);
    var reqs: [2][]u8 = undefined;
    for (&reqs, 0..) |*r, i| r.* = try testRequest(.kv_get, 900 + i, key, "");
    defer for (reqs) |r| std.testing.allocator.free(r);
    try std.testing.expectEqual(reqs[0].len, feedClient(first.pair[1], a, first.conn.fd, reqs[0]));
    try std.testing.expect(first.conn.waiting != null);

    // A slot comes back before the tick ends; a request read now does not
    // take it from the one already waiting.
    _ = a.reply_pool.release(tickets.pop().?, 1);
    try std.testing.expectEqual(reqs[1].len, feedClient(later.pair[1], a, later.conn.fd, reqs[1]));
    try std.testing.expect(later.conn.waiting != null);
    a.settleConnections();
    try std.testing.expectEqual(@as(u16, 1), first.conn.forwards_in_flight);
    try std.testing.expectEqual(@as(u16, 0), later.conn.forwards_in_flight);
    try std.testing.expect(later.conn.waiting != null);
    while (two.shards[1].drainInbox() > 0) {}
    _ = a.drainInbox();
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
        if (conn.closing or conn.reads_paused or conn.waiting != null) break;
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

test "Shard: a first frame in another protocol is answered as an invalid request and the connection closed" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();

    // A Redis command gets what stray HTTP gets: Flo does not speak it.
    const frames = [_][]const u8{
        "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n",
        "GET /api/v1/health HTTP/1.1\r\nHost: localhost\r\n\r\n",
    };
    for (frames) |frame| {
        const c = try TestClient.open(&shard);
        defer _ = std.c.close(c.pair[1]);
        const fd = c.conn.fd;
        try std.testing.expectEqual(@as(isize, @intCast(frame.len)), std.c.write(c.pair[1], frame.ptr, frame.len));
        shard.readFromClient(fd);
        try std.testing.expect(c.conn.closing);
        var out: [256]u8 = undefined;
        const n = try readAnswer(c.pair[1], &shard, fd, &out, @sizeOf(proto.ResponseHeader));
        const resp = try proto.Response.parse(out[0..n]);
        try std.testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), resp.header.status);
        try std.testing.expectEqual(@as(u64, 0), resp.header.request_id);
        try std.testing.expectEqualStrings("Invalid request", resp.data);
        shard.settleConnections();
        try std.testing.expect(shard.getConnection(fd) == null);
    }
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

test "Shard: a wake flag from another shard is acted on at the next drain, once however often it was set" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.workflow_handler.triggers_dirty = false;
    for (0..5) |_| shard.mailbox.wake.set(.stream_appended);
    try std.testing.expect(!shard.mailbox.prepareSleep());
    try std.testing.expectEqual(@as(usize, 0), shard.drainInbox());
    try std.testing.expect(shard.workflow_handler.triggers_dirty);
    try std.testing.expectEqual(@as(u32, 0), shard.mailbox.wake.flags.load(.monotonic));
    try std.testing.expect(shard.mailbox.prepareSleep());
    shard.mailbox.wake.woke();
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
    while (i < 3) : (i += 1) try std.testing.expect(shard.mailbox.inbox.send(.{ .tag = .action_start }));
    try std.testing.expectEqual(@as(usize, 3), shard.drainInbox());
    try std.testing.expectEqual(@as(u64, 3), sm.inbox_processed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), sm.inbox_pending.load(.monotonic));

    // Read at scrape time from the inbox itself: what waits for a shard
    // that has stopped draining still shows.
    sm.live_pending = .{ .ctx = shard.mailbox, .read = Shard.inboxPending };
    try std.testing.expect(shard.mailbox.inbox.send(.{ .tag = .action_start }));
    try std.testing.expect(shard.mailbox.inbox.send(.{ .tag = .action_start }));
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
    var payload: [FORWARD_PREFIX + 40]u8 = undefined;
    std.mem.writeInt(u32, payload[0..4], 91, .little);
    std.mem.writeInt(u16, payload[4..6], 0, .little);
    @memset(payload[FORWARD_PREFIX..], 0xee);
    std.mem.writeInt(u64, payload[FORWARD_PREFIX + 8 ..][0..8], 5, .little);
    shard.runForwardedWrite(.{ .source_node = 2, .group_id = 0, .msg_type = .forward_write, .payload = &payload });
    try std.testing.expectEqual(@as(usize, 1), rn.outbound.items.len);
    const reply = rn.outbound.items[0].frame[transport.HEADER_SIZE..];
    try std.testing.expectEqual(@as(u32, 91), std.mem.readInt(u32, reply[0..4], .little));
    const resp = try proto.Response.parse(reply[12..]);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.internal_error), resp.header.status);
    try std.testing.expectEqual(@as(u64, 5), resp.header.request_id);
    // It waits in the forward queue, not Raft's.
    try std.testing.expect(rn.outbound.items[0].forward);
}

test "Shard: a forwarded write carrying fields this node does not read is answered, not run without them" {
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
    // A well-formed write behind three bytes of fields.
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(proto.OpCode.kv_put);
    header.request_id = 9;
    const wire = try Shard.serializeRequest(std.testing.allocator, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    defer std.testing.allocator.free(wire);
    const frame = try std.testing.allocator.alloc(u8, FORWARD_PREFIX + 3 + wire.len);
    defer std.testing.allocator.free(frame);
    std.mem.writeInt(u32, frame[0..4], 92, .little);
    std.mem.writeInt(u16, frame[4..6], 3, .little);
    @memcpy(frame[FORWARD_PREFIX..][0..3], "abc");
    @memcpy(frame[FORWARD_PREFIX + 3 ..], wire);
    const writes_before = shard.requests_dispatched;
    shard.runForwardedWrite(.{ .source_node = 2, .group_id = 0, .msg_type = .forward_write, .payload = frame });
    try std.testing.expectEqual(writes_before, shard.requests_dispatched);
    try std.testing.expectEqual(@as(usize, 1), rn.outbound.items.len);
    const reply = rn.outbound.items[0].frame[transport.HEADER_SIZE..];
    try std.testing.expectEqual(@as(u32, 92), std.mem.readInt(u32, reply[0..4], .little));
    const resp = try proto.Response.parse(reply[12..]);
    try std.testing.expectEqual(@as(u64, 9), resp.header.request_id);
    try std.testing.expectEqualStrings("internal error: the leader cannot read this forwarded request — are all nodes on the same version?", resp.data);
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
    shard.dispatchRequest(conn, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    try std.testing.expectEqual(@as(u32, 1), shard.forward_count);
    // Ids sit outside the fd range: a client closing on the leader can
    // never match one in its waiter pool.
    try std.testing.expect(shard.forwards[0].reply_to.isSocket(0, pair[0], conn.id));
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
    shard.dispatchRequest(conn, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    shard.forwards[0].sent_to = 2;
    shard.forwards[0].sent_term = raft.current_term;
    shard.forwards[0].sent_ms = now;
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
    shard.dispatchRequest(conn, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    shard.forwards[0].sent_to = 2;
    shard.forwards[0].sent_term = raft.current_term;
    shard.sweepForwards(now);
    try std.testing.expectEqual(@as(u32, 0), shard.forward_count);
    n = std.c.read(pair[1], &out, out.len);
    resp = try proto.Response.parse(out[0..@intCast(n)]);
    try std.testing.expect(std.mem.indexOf(u8, resp.data, "lost the link") != null);

    // The link went down and came back between two ticks: a new session,
    // so the answer is not coming over it either.
    rn.linked_ids[0].store(2, .release);
    rn.linked_sessions[0].store(5, .release);
    shard.dispatchRequest(conn, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    shard.forwards[0].sent_to = 2;
    shard.forwards[0].sent_term = raft.current_term;
    shard.forwards[0].sent_session = 4;
    shard.forwards[0].sent_ms = now;
    shard.sweepForwards(now);
    try std.testing.expectEqual(@as(u32, 0), shard.forward_count);
    n = std.c.read(pair[1], &out, out.len);
    resp = try proto.Response.parse(out[0..@intCast(n)]);
    try std.testing.expect(std.mem.indexOf(u8, resp.data, "lost the link") != null);

    // Same link, same leader, same term, and no answer: waited on until
    // the backstop, then answered.
    shard.dispatchRequest(conn, .{ .header = header, .namespace = "", .key = "k", .value = "v" });
    shard.forwards[0].sent_to = 2;
    shard.forwards[0].sent_term = raft.current_term;
    shard.forwards[0].sent_session = 5;
    shard.forwards[0].sent_ms = now;
    shard.sweepForwards(now + SENT_FORWARD_BACKSTOP_MS - 1);
    try std.testing.expectEqual(@as(u32, 1), shard.forward_count);
    shard.sweepForwards(now + SENT_FORWARD_BACKSTOP_MS);
    try std.testing.expectEqual(@as(u32, 0), shard.forward_count);
    n = std.c.read(pair[1], &out, out.len);
    resp = try proto.Response.parse(out[0..@intCast(n)]);
    try std.testing.expectEqualStrings("unavailable: no answer from the leader — write may still apply", resp.data);
    rn.linked_ids[0].store(0, .release);
    rn.linked_sessions[0].store(0, .release);

    // A write held while no leader was known, then this node leads: it
    // runs here as the client's own request.
    raft.leader_id = 0;
    header.request_id = 6;
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

test "Shard: a member with no log starts guarded, durably, and one with hard state but no log does not found a cluster" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try testDataDir(&tmp);
    defer std.testing.allocator.free(data_dir);
    const shard_dir = try std.fmt.allocPrint(std.testing.allocator, "{s}/00000", .{data_dir});
    defer std.testing.allocator.free(shard_dir);
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);

    {
        var joiner = try Shard.init(std.testing.allocator, 0, 2, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .join, .{});
        defer joiner.deinit();
        try std.testing.expectEqual(raft_node_mod.LostLog.catching_up, joiner.raft_node.lost_log);
    }
    // On disk before the shard answers anything, so a restart keeps it.
    try std.testing.expect((try hard_state_mod.load(shard_dir)).?.lost_log);
    {
        var again = try Shard.init(std.testing.allocator, 0, 2, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .join, .{});
        defer again.deinit();
        try std.testing.expectEqual(raft_node_mod.LostLog.catching_up, again.raft_node.lost_log);
    }
    try std.testing.expectError(error.LostLogFounding, Shard.init(std.testing.allocator, 0, 2, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .bootstrap, .{}));

    // Restarted part way through catching up: a log, and still guarded.
    const segs = try std.fmt.allocPrint(std.testing.allocator, "{s}/segs", .{shard_dir});
    defer std.testing.allocator.free(segs);
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
    var midway = try Shard.init(std.testing.allocator, 0, 2, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .join, .{});
    defer midway.deinit();
    try std.testing.expectEqual(@as(u64, 2), midway.raft_node.log.lastIndex());
    try std.testing.expectEqual(raft_node_mod.LostLog.catching_up, midway.raft_node.lost_log);
}

test "Shard: a member that kept its log but lost its hard state confirms the term before it votes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try testDataDir(&tmp);
    defer std.testing.allocator.free(data_dir);
    const shard_dir = try std.fmt.allocPrint(std.testing.allocator, "{s}/00000", .{data_dir});
    defer std.testing.allocator.free(shard_dir);
    const segs = try std.fmt.allocPrint(std.testing.allocator, "{s}/segs", .{shard_dir});
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
    var member = try Shard.init(std.testing.allocator, 0, 2, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .join, .{});
    defer member.deinit();
    try std.testing.expectEqual(raft_node_mod.LostLog.confirming, member.raft_node.lost_log);
    try std.testing.expect((try hard_state_mod.load(shard_dir)).?.lost_log);
    // The log's last term, not term 0: the hard state that is gone held at
    // least that.
    try std.testing.expectEqual(@as(u64, 1), member.raft_node.current_term);
}

test "Shard: --join with a log but no config entry and no hard state is guarded too" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try testDataDir(&tmp);
    defer std.testing.allocator.free(data_dir);
    const segs = try std.fmt.allocPrint(std.testing.allocator, "{s}/00000/segs", .{data_dir});
    defer std.testing.allocator.free(segs);
    try @import("stdx").fs.makePath(segs);
    var w = SegmentWriter.init(std.testing.allocator, 0, .none);
    defer w.deinit();
    var noop = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 2, 1, 0, "");
    noop.header.crc32c = noop.computeCrc();
    try w.addEntry(&noop);
    w.commit_index_at_seal = 1;
    try w.writeToFile(segs);

    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var joiner = try Shard.init(std.testing.allocator, 0, 2, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .join, .{});
    defer joiner.deinit();
    try std.testing.expectEqual(raft_node_mod.LostLog.catching_up, joiner.raft_node.lost_log);
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

    fn request(op: proto.OpCode, id: u64, ns: []const u8, key: []const u8, value: []const u8, options: []const u8) !proto.Request {
        var header: proto.RequestHeader = undefined;
        @memset(std.mem.asBytes(&header), 0);
        header.magic = proto.MAGIC;
        header.version = proto.VERSION;
        header.op_code = @intFromEnum(op);
        header.request_id = id;
        return .{ .header = header, .namespace = ns, .key = key, .value = value, .options = options };
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

test "Shard: a write to a new namespace holds its room before it commits, so writes in flight cannot overshoot the limit" {
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

    // An explicit create in flight holds its name and id as a write's does.
    shard.dispatchRequest(conn, try ParkTest.request(.namespace_create, 42, "", "explicit", "", ""));
    try std.testing.expect(shard.namespace_handler.pending_creates.contains("explicit"));
    try ParkTest.ack(&shard);
    try std.testing.expect(!shard.namespace_handler.pending_creates.contains("explicit"));
    var created: [1]proto.Response = undefined;
    var created_buf: [256]u8 = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &created_buf, &created);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), created[0].header.status);

    // One namespace short of the limit.
    var name_buf: [16]u8 = undefined;
    const room = 1024 - shard.namespace_handler.namespaces.count() - 1;
    for (0..room) |i| _ = shard.namespace_handler.applyCreate(try std.fmt.bufPrint(&name_buf, "ns{d}", .{i}));

    // Two writes to new namespaces, neither committed: the first holds
    // the last room, the second is refused at once.
    shard.dispatchRequest(conn, try ParkTest.request(.kv_put, 40, "first", "k", "v", ""));
    shard.dispatchRequest(conn, try ParkTest.request(.kv_put, 41, "second", "k", "v", ""));
    var buf: [1024]u8 = undefined;
    var one: [1]proto.Response = undefined;
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    try std.testing.expectEqual(@as(u64, 41), one[0].header.request_id);
    try std.testing.expectEqualStrings(handler_mod.NamespaceHandler.LIMIT_MESSAGE, one[0].data);

    try ParkTest.ack(&shard);
    try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
    try std.testing.expectEqual(@as(u64, 40), one[0].header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), one[0].header.status);
    try std.testing.expect(shard.namespace_handler.namespaces.contains("first"));
    try std.testing.expect(!shard.namespace_handler.namespaces.contains("second"));
}

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
        \\"start":{"wait_for_signal":{"type":"go"},"transitions":{"success":"flo.Completed"}}}
    ;
    try ParkTest.send(&shard, conn, .workflow_create, 20, "gate", def);
    try ParkTest.ack(&shard);
    try ParkTest.send(&shard, conn, .workflow_start, 21, "gate", "");
    try ParkTest.ack(&shard);
    shard.namespace_handler.markNamespaceHasData("other", &shard);
    try std.testing.expectEqual(@as(usize, 1), shard.workflow_handler.started_to_advance.items.len);
    try std.testing.expectEqual(@as(u32, 1), shard.namespace_handler.pending_creates.count());

    // What this leader had in flight may be gone with its log's tail.
    shard.leadershipLost("test");
    try std.testing.expectEqual(@as(usize, 0), shard.workflow_handler.started_to_advance.items.len);
    try std.testing.expectEqual(@as(u32, 0), shard.namespace_handler.pending_creates.count());
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
        \\"start":{"wait_for_signal":{"type":"go"},"transitions":{"success":"flo.Completed"}}}
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

    // A namespace a run key cannot carry (the key is read back up to its
    // first ':') is refused, for a definition and for a start, before
    // anything is proposed.
    const before = shard.raft_node.log.lastIndex();
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_create, 90, "a:b", "gate", def, ""));
    shard.dispatchRequest(conn, try ParkTest.request(.workflow_start, 91, "a:b", "gate", &keyed, ""));
    try ParkTest.responses(&shard, conn, pair[1], &big, &two);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), two[0].header.status);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), two[1].header.status);
    try std.testing.expect(std.mem.startsWith(u8, two[1].data, "invalid namespace name"));
    try std.testing.expectEqual(before, shard.raft_node.log.lastIndex());
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
        \\"start":{"wait_for_signal":{"type":"go"},"transitions":{"success":"flo.Completed"}}}
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

test "Shard: namespace settings are refused with their reason, and nothing is proposed" {
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
    var buf: [256]u8 = undefined;
    var one: [1]proto.Response = undefined;

    const last = shard.raft_node.log.lastIndex();
    // A well-formed block that the old settings format would have applied.
    for ([_]proto.OpCode{ .namespace_config_set, .namespace_config_get }, 0..) |op, i| {
        try ParkTest.send(&shard, conn, op, i + 1, "default", &.{ 1, 6, 60, 0, 0, 0 });
        try ParkTest.responses(&shard, conn, pair[1], &buf, &one);
        try std.testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), one[0].header.status);
        try std.testing.expectEqualStrings(NamespaceHandler.SETTINGS_REFUSAL, one[0].data);
    }
    try std.testing.expectEqual(last, shard.raft_node.log.lastIndex());
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
    shard.deliverDeferredResponse(conn.replyTo(), 9, .ok, big);
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

/// A lone shard answering one client over a socket pair; `client` is the
/// client's end.
const ReadTest = struct {
    pipe_fds: [2]i32,
    pair: [2]std.posix.fd_t,
    shard: Shard,
    conn: *Connection,

    fn init(self: *ReadTest) !void {
        self.pipe_fds = try @import("stdx").io.pipe();
        self.shard = try Shard.init(std.testing.allocator, 0, 1, 4096, self.pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
        self.shard.wireHandlerShardPtrs();
        try std.testing.expect(self.shard.applyCommitted());
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &self.pair));
        const flags = std.c.fcntl(self.pair[0], std.c.F.GETFL, @as(c_int, 0));
        _ = std.c.fcntl(self.pair[0], std.c.F.SETFL, flags | @as(c_int, @bitCast(std.c.O{ .NONBLOCK = true })));
        self.conn = try self.shard.addConnection(self.pair[0]);
    }

    fn deinit(self: *ReadTest) void {
        self.shard.deinit();
        _ = std.c.close(self.pair[1]);
        _ = std.c.close(self.pipe_fds[0]);
        _ = std.c.close(self.pipe_fds[1]);
    }

    /// A kv_get as the client sends it.
    fn sendGet(self: *ReadTest, id: u64) !void {
        const wire = try Shard.serializeRequest(std.testing.allocator, try ParkTest.request(.kv_get, id, "", "some-key-for-the-test", "", ""));
        defer std.testing.allocator.free(wire);
        try std.testing.expectEqual(@as(isize, @intCast(wire.len)), std.c.write(self.pair[1], wire.ptr, wire.len));
    }

    /// End-of-tick passes until `want` answers arrived, or none came in a pass.
    fn answers(self: *ReadTest, want: usize) !usize {
        var buf: [4096]u8 = undefined;
        var total: usize = 0;
        var got: usize = 0;
        for (0..64) |_| {
            self.shard.settleConnections();
            self.shard.flushToClient(self.conn.fd);
            while (true) {
                const n = std.c.recv(self.pair[1], buf[total..].ptr, buf.len - total, std.c.MSG.DONTWAIT);
                if (n <= 0) break;
                total += @intCast(n);
            }
            var off: usize = 0;
            got = 0;
            while (proto.Response.parse(buf[off..total])) |r| : (got += 1) {
                off += @sizeOf(proto.ResponseHeader) + r.data.len;
            } else |_| {}
            if (got >= want) break;
        }
        return got;
    }
};

test "Shard: a request read a little per event is still answered, the rest read at the end of the tick" {
    var t: ReadTest = undefined;
    try t.init();
    defer t.deinit();
    // One read per event, into a buffer smaller than the request: every
    // read fills its room, so each event stops with more in the socket.
    t.shard.reads_per_event = 1;
    t.conn.read_buf.deinit();
    t.conn.read_buf = try RingBuffer.initWithCapacity(std.testing.allocator, 16);

    try t.sendGet(1);
    t.shard.readFromClient(t.conn.fd);
    try std.testing.expect(t.conn.resume_queued);
    try std.testing.expectEqual(@as(usize, 1), try t.answers(1));
}

test "Shard: what a client sent while its reads were paused is read when they resume" {
    var t: ReadTest = undefined;
    try t.init();
    defer t.deinit();

    t.shard.pauseReads(t.conn.fd, t.conn);
    try t.sendGet(1);
    t.shard.readFromClient(t.conn.fd);
    try std.testing.expectEqual(@as(usize, 0), t.conn.read_buf.readable());
    t.shard.resumeReads(t.conn.fd, t.conn);
    try std.testing.expectEqual(@as(usize, 1), try t.answers(1));
}

/// A lone node with sync durability, its data dir under `tmp`, answering a
/// client over a socket pair.
const SyncAlone = struct {
    data_dir: []const u8,
    segs_z: [:0]u8,
    pipe_fds: [2]i32,
    pair: [2]std.posix.fd_t,
    shard: Shard,
    conn: *Connection,

    fn init(self: *SyncAlone, tmp: *std.testing.TmpDir) !void {
        self.data_dir = try testDataDir(tmp);
        self.segs_z = try std.fmt.allocPrintSentinel(std.testing.allocator, "{s}/00000/segs", .{self.data_dir}, 0);
        self.pipe_fds = try @import("stdx").io.pipe();
        self.shard = try Shard.init(std.testing.allocator, 0, 1, 4096, self.pipe_fds[0], self.data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .sync, 1, .single, .{ .self_counts_when_durable = true });
        self.shard.wireHandlerShardPtrs();
        try std.testing.expect(self.shard.applyCommitted());
        try std.testing.expectEqual(@as(c_int, 0), std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &self.pair));
        self.conn = try self.shard.addConnection(self.pair[0]);
    }

    fn deinit(self: *SyncAlone) void {
        _ = std.c.chmod(self.segs_z, 0o700);
        self.shard.deinit();
        _ = std.c.close(self.pair[1]);
        _ = std.c.close(self.pipe_fds[0]);
        _ = std.c.close(self.pipe_fds[1]);
        std.testing.allocator.free(self.segs_z);
        std.testing.allocator.free(self.data_dir);
    }

    /// The answer borrows from `buf`.
    fn put(self: *SyncAlone, id: u64, key: []const u8, buf: []u8) !proto.Response {
        try ParkTest.send(&self.shard, self.conn, .kv_put, id, key, "v");
        _ = self.shard.applyCommitted();
        var r: [1]proto.Response = undefined;
        try ParkTest.responses(&self.shard, self.conn, self.pair[1], buf, &r);
        return r[0];
    }
};

test "Shard: alone with sync durability, a write whose flush fails is not acked, and applies once the disk recovers" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var s: SyncAlone = undefined;
    try s.init(&tmp);
    defer s.deinit();

    // The segments directory refuses new files: the flush fails.
    try std.testing.expectEqual(@as(c_int, 0), std.c.chmod(s.segs_z, 0o500));
    var buf: [1024]u8 = undefined;
    const refused = try s.put(1, "k1", &buf);
    try std.testing.expectEqual(proto.StatusCode.unavailable, refused.getStatus());
    try std.testing.expect(std.mem.indexOf(u8, refused.data, "not on disk") != null);
    try std.testing.expect(s.shard.kv_handler.kv.get("k1") == null);

    try std.testing.expectEqual(@as(c_int, 0), std.c.chmod(s.segs_z, 0o700));
    const acked = try s.put(2, "k2", &buf);
    try std.testing.expectEqual(proto.StatusCode.ok, acked.getStatus());
    // The refused write was in the log; it reached disk with this one.
    try std.testing.expect(s.shard.kv_handler.kv.get("k1") != null);
    try std.testing.expect(s.shard.kv_handler.kv.get("k2") != null);
}

test "Shard: an entry the writer could not buffer is buffered again from the log before the flush" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var s: SyncAlone = undefined;
    try s.init(&tmp);
    defer s.deinit();

    // As if the append hook had failed on the next entry: nothing from it
    // on is buffered, and the flush must buffer it from the log.
    const writer = s.shard.durable_log.?.writer;
    writer.first_unbuffered = s.shard.raft_node.log.lastIndex() + 1;
    var buf: [1024]u8 = undefined;
    const acked = try s.put(1, "k1", &buf);
    try std.testing.expectEqual(proto.StatusCode.ok, acked.getStatus());
    try std.testing.expect(writer.first_unbuffered == null);
    var got: [4]entry_mod.Entry = undefined;
    var arena: [256]u8 = undefined;
    try std.testing.expect(s.shard.durable_log.?.readRange(s.shard.raft_node.log.lastIndex(), &got, &arena) == 1);
}

test "Shard: with sync durability, entries after one the writer could not buffer reach disk once each, in order" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var s: SyncAlone = undefined;
    try s.init(&tmp);
    defer s.deinit();
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqual(proto.StatusCode.ok, (try s.put(1, "k0", &buf)).getStatus());

    // An empty buffer with no room: buffering the next entry allocates,
    // and that allocation fails. The one after it is appended before any
    // flush, as a follower appends a batch.
    const writer = s.shard.durable_log.?.writer;
    try std.testing.expectEqual(@as(u32, 0), writer.entry_count);
    writer.data.clearAndFree(writer.allocator);
    writer.sparse_index.clearAndFree(writer.allocator);
    const real = writer.allocator;
    var failing = std.testing.FailingAllocator.init(real, .{ .fail_index = 0 });
    writer.allocator = failing.allocator();
    const first = try persistence_mod.proposeEntry(&s.shard, .kv_put, entry_mod.Flags.NONE, "", "k1", "v");
    writer.allocator = real;
    _ = try persistence_mod.proposeEntry(&s.shard, .kv_put, entry_mod.Flags.NONE, "", "k2", "v");
    try std.testing.expectEqual(@as(u64, 1), writer.buffer_failures);

    s.shard.syncFlushIfNeeded();
    try std.testing.expectEqual(first.index + 1, s.shard.raft_node.durable_index);
    var got: [4]entry_mod.Entry = undefined;
    var arena: [256]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 2), s.shard.durable_log.?.readRange(first.index, &got, &arena));
    try std.testing.expectEqual(first.index, got[0].header.index);
    try std.testing.expectEqual(first.index + 1, got[1].header.index);
}

test "Shard: with sync durability, an entry that left memory before reaching disk stops the shard's writes, by name" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var s: SyncAlone = undefined;
    try s.init(&tmp);
    defer s.deinit();
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqual(proto.StatusCode.ok, (try s.put(1, "k1", &buf)).getStatus());

    // The hook could not buffer k2, and the ring evicted it before the
    // flush: it is nowhere.
    const writer = s.shard.durable_log.?.writer;
    writer.first_unbuffered = s.shard.raft_node.log.lastIndex() + 1;
    _ = try persistence_mod.proposeEntry(&s.shard, .kv_put, entry_mod.Flags.NONE, "", "k2", "v");
    _ = s.shard.raft_node.log.ual.evictOlderThan(std.math.maxInt(u64));

    const lost = try s.put(3, "k3", &buf);
    try std.testing.expectEqual(proto.StatusCode.unavailable, lost.getStatus());
    try std.testing.expect(std.mem.indexOf(u8, lost.data, "lost when the node restarts") != null);
    try std.testing.expect(s.shard.raft_node.writes_stopped);

    // The next write is refused before it reaches the log.
    const last = s.shard.raft_node.log.lastIndex();
    const refused = try s.put(4, "k4", &buf);
    try std.testing.expectEqual(proto.StatusCode.unavailable, refused.getStatus());
    try std.testing.expect(std.mem.indexOf(u8, refused.data, "stopped taking writes") != null);
    try std.testing.expectEqual(last, s.shard.raft_node.log.lastIndex());
}

test "Shard: with async durability, an entry the writer could not buffer is a hole, and buffering goes on" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data_dir = try testDataDir(&tmp);
    defer std.testing.allocator.free(data_dir);
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], data_dir, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    try shard.flushSegmentToDisk();

    const writer = shard.durable_log.?.writer;
    writer.data.clearAndFree(writer.allocator);
    writer.sparse_index.clearAndFree(writer.allocator);
    const real = writer.allocator;
    var failing = std.testing.FailingAllocator.init(real, .{ .fail_index = 0 });
    writer.allocator = failing.allocator();
    _ = try persistence_mod.proposeEntry(&shard, .kv_put, entry_mod.Flags.NONE, "", "k1", "v");
    writer.allocator = real;
    const second = try persistence_mod.proposeEntry(&shard, .kv_put, entry_mod.Flags.NONE, "", "k2", "v");
    try std.testing.expectEqual(@as(u64, 1), writer.buffer_failures);
    try std.testing.expect(writer.first_unbuffered == null);
    try std.testing.expectEqual(@as(u32, 1), writer.entry_count);
    try std.testing.expectEqual(second.index, writer.first_index);
    try shard.flushSegmentToDisk();
}

test "shard: a forwarded request parsed without its options trailer re-serializes whole" {
    const payload = [_]u8{ 0, 0, 2, 0, 'k', '0', 0, 0, 0, 0 };
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.magic = proto.MAGIC;
    header.version = proto.VERSION;
    header.op_code = @intFromEnum(proto.OpCode.kv_get);
    header.request_id = 5;
    header.payload_length = payload.len;
    header.crc32 = header.computeCRC32(&payload);
    var frame: [@sizeOf(proto.RequestHeader) + payload.len]u8 = undefined;
    @memcpy(frame[0..@sizeOf(proto.RequestHeader)], std.mem.asBytes(&header));
    @memcpy(frame[@sizeOf(proto.RequestHeader)..], &payload);
    const req = try proto.Request.parse(&frame);

    const wire = try Shard.serializeRequest(std.testing.allocator, req);
    defer std.testing.allocator.free(wire);
    const again = try proto.Request.parse(wire);
    try std.testing.expectEqualStrings("k0", again.key);
    try std.testing.expectEqual(@as(u64, 5), again.header.request_id);
    try std.testing.expectEqual(@as(usize, 0), again.options.len);
}

test "Shard: an await that claims a pending run at once gets exactly one answer, carrying the run" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    try std.testing.expect(shard.applyCommitted());
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);
    var out: [4096]u8 = undefined;

    // Register the action, then invoke it: one pending run.
    // invoke value: [priority:u8][delay_ms:i64][has_caller:u8][has_idem:u8][has_labels:u8][input]
    const invoke_value: []const u8 = [_]u8{10} ++ [_]u8{0} ** 8 ++ [_]u8{ 0, 0, 0 } ++ "job-input";
    for ([_]struct { op: proto.OpCode, id: u64, value: []const u8 }{
        .{ .op = .action_register, .id = 1, .value = "" },
        .{ .op = .action_invoke, .id = 2, .value = invoke_value },
    }) |r| {
        const frame = try testRequest(r.op, r.id, "act", r.value);
        defer std.testing.allocator.free(frame);
        _ = feedClient(c.pair[1], &shard, c.conn.fd, frame);
        _ = shard.applyCommitted();
        const n = try readAnswer(c.pair[1], &shard, c.conn.fd, &out, @sizeOf(proto.ResponseHeader));
        const resp = try proto.Response.parse(out[0..n]);
        try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), resp.header.status);
    }
    try std.testing.expectEqual(@as(c_int, 0), unreadBytes(c.pair[1]));

    // The await claims it while dispatched: one ok frame with the task, and
    // nothing after it (it used to be followed by "not implemented").
    // await value: [count:u32][type_len:u16][type]
    const await_value: []const u8 = [_]u8{ 1, 0, 0, 0, 3, 0 } ++ "act";
    const frame = try testRequest(.action_await, 3, "worker-1", await_value);
    defer std.testing.allocator.free(frame);
    _ = feedClient(c.pair[1], &shard, c.conn.fd, frame);
    const n = try readAnswer(c.pair[1], &shard, c.conn.fd, &out, @sizeOf(proto.ResponseHeader));
    const resp = try proto.Response.parse(out[0..n]);
    try std.testing.expectEqual(@as(u64, 3), resp.header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), resp.header.status);
    try std.testing.expect(std.mem.endsWith(u8, resp.data, "job-input"));
    // [task_id_len:u16][task_id][task_type_len:u16]"act"…
    const id_len = std.mem.readInt(u16, resp.data[0..2], .little);
    try std.testing.expectEqualStrings("act", resp.data[2 + id_len + 2 ..][0..3]);
    try std.testing.expectEqual(n, @sizeOf(proto.ResponseHeader) + resp.data.len);
    shard.flushToClient(c.conn.fd);
    try std.testing.expectEqual(@as(c_int, 0), unreadBytes(c.pair[1]));
}

test "Shard: a parked await and an invoke reusing its request id each get one answer" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    try std.testing.expect(shard.applyCommitted());
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);
    var out: [4096]u8 = undefined;

    const reg = try testRequest(.action_register, 1, "act", "");
    defer std.testing.allocator.free(reg);
    _ = feedClient(c.pair[1], &shard, c.conn.fd, reg);
    _ = shard.applyCommitted();
    _ = try readAnswer(c.pair[1], &shard, c.conn.fd, &out, @sizeOf(proto.ResponseHeader));

    // The await parks: nothing is pending yet.
    const await_value: []const u8 = [_]u8{ 1, 0, 0, 0, 3, 0 } ++ "act";
    const aw = try testRequest(.action_await, 7, "worker-1", await_value);
    defer std.testing.allocator.free(aw);
    _ = feedClient(c.pair[1], &shard, c.conn.fd, aw);
    shard.flushToClient(c.conn.fd);
    try std.testing.expectEqual(@as(c_int, 0), unreadBytes(c.pair[1]));

    // An invoke under the same id wakes it while being dispatched: the
    // await's task is a deferred answer to "request 7", which isn't this
    // request's own.
    const invoke_value: []const u8 = [_]u8{10} ++ [_]u8{0} ** 8 ++ [_]u8{ 0, 0, 0 } ++ "job";
    const inv = try testRequest(.action_invoke, 7, "act", invoke_value);
    defer std.testing.allocator.free(inv);
    _ = feedClient(c.pair[1], &shard, c.conn.fd, inv);
    _ = shard.applyCommitted();
    shard.flushToClient(c.conn.fd);

    var rs: [2]proto.Response = undefined;
    var buf: [4096]u8 = undefined;
    try ParkTest.responses(&shard, c.conn, c.pair[1], &buf, &rs);
    var tasks: usize = 0;
    for (rs) |r| {
        try std.testing.expectEqual(@as(u64, 7), r.header.request_id);
        try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), r.header.status);
        if (std.mem.endsWith(u8, r.data, "job")) tasks += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), tasks);
    try std.testing.expectEqual(@as(u64, 0), shard.answered_as_deferred);
}

test "Shard: a handler answering its own request as deferred is caught, and answered once" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    try std.testing.expect(shard.applyCommitted());
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);

    // The shape the immediate await had: deliver as deferred, park nothing.
    const SelfDeferred = struct {
        fn handle(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: proto.Request) void {
            const sh: *Shard = @ptrCast(@alignCast(shard_ptr));
            const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
            sh.deliverDeferredResponse(conn.replyTo(), req.header.request_id, .ok, "only-answer");
        }
    };
    shard.dispatcher.register(.queue_touch, SelfDeferred.handle);

    const frame = try testRequest(.queue_touch, 9, "q", "");
    defer std.testing.allocator.free(frame);
    _ = feedClient(c.pair[1], &shard, c.conn.fd, frame);
    var one: [1]proto.Response = undefined;
    var buf: [1024]u8 = undefined;
    try ParkTest.responses(&shard, c.conn, c.pair[1], &buf, &one);
    try std.testing.expectEqualStrings("only-answer", one[0].data);
    try std.testing.expectEqual(@as(u64, 1), shard.answered_as_deferred);
}

test "Shard: a refusal longer than its frame is sent cut, under its id, and alone" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    try std.testing.expect(shard.applyCommitted());
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);

    // A handler refusing with a 2000-byte message, as a long echoed name
    // or a nested parse path can make one. It used to send nothing.
    const Long = struct {
        fn handle(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: proto.Request) void {
            const sh: *Shard = @ptrCast(@alignCast(shard_ptr));
            const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
            sh.sendErrorResponse(conn, req.header.request_id, .bad_request, "€" ** 666 ++ "xx");
        }
    };
    shard.dispatcher.register(.queue_touch, Long.handle);

    const frame = try testRequest(.queue_touch, 11, "q", "");
    defer std.testing.allocator.free(frame);
    _ = feedClient(c.pair[1], &shard, c.conn.fd, frame);
    var one: [1]proto.Response = undefined;
    var buf: [2048]u8 = undefined;
    try ParkTest.responses(&shard, c.conn, c.pair[1], &buf, &one);
    try std.testing.expectEqual(@as(u64, 11), one[0].header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.bad_request), one[0].header.status);
    try std.testing.expect(std.unicode.utf8ValidateSlice(one[0].data));
    try std.testing.expect(std.mem.endsWith(u8, one[0].data, proto.Response.TRUNCATED_MARKER));
    try std.testing.expect(one[0].data.len > 900);
}

test "Shard: a request whose handler answers nothing is answered internal_error and counted" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    shard.wireHandlerShardPtrs();
    try std.testing.expect(shard.applyCommitted());
    var sm = ShardMetrics{ .shard_id = 0 };
    shard.shard_metrics = &sm;
    defer shard.shard_metrics = null;
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);

    const Silent = struct {
        fn handle(_: *anyopaque, _: *anyopaque, _: proto.Request) void {}
    };
    shard.dispatcher.register(.queue_touch, Silent.handle);

    const frame = try testRequest(.queue_touch, 12, "q", "");
    defer std.testing.allocator.free(frame);
    _ = feedClient(c.pair[1], &shard, c.conn.fd, frame);
    var one: [1]proto.Response = undefined;
    var buf: [1024]u8 = undefined;
    try ParkTest.responses(&shard, c.conn, c.pair[1], &buf, &one);
    try std.testing.expectEqual(@as(u64, 12), one[0].header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.internal_error), one[0].header.status);
    try std.testing.expectEqualStrings("internal error: the handler produced no answer", one[0].data);
    try std.testing.expectEqual(@as(u64, 1), sm.snapshot().handler_no_answer);
}

test "Shard: an answer refused for a full write buffer is not taken for a missing answer" {
    const pipe_fds = try @import("stdx").io.pipe();
    defer _ = std.c.close(pipe_fds[0]);
    defer _ = std.c.close(pipe_fds[1]);
    var shard = try Shard.init(std.testing.allocator, 0, 1, 4096, pipe_fds[0], null, Partition.DEFAULT_UAL_CAPACITY, 0, 0, .async_flush, 1, .single, .{});
    defer shard.deinit();
    var sm = ShardMetrics{ .shard_id = 0 };
    shard.shard_metrics = &sm;
    defer shard.shard_metrics = null;
    const c = try TestClient.open(&shard);
    defer _ = std.c.close(c.pair[1]);

    // A client unpaced after stalling, its unsent answers 8 bytes short of
    // the cap: the ping's answer doesn't fit and is refused.
    c.conn.pacing_off = true;
    const filler = try std.testing.allocator.alloc(u8, Connection.MAX_WRITE_BUFFER - 8);
    defer std.testing.allocator.free(filler);
    @memset(filler, 0);
    try std.testing.expectEqual(filler.len, c.conn.queueWrite(filler));

    const ping = try testRequest(.ping, 13, "", "");
    defer std.testing.allocator.free(ping);
    try std.testing.expectEqual(@as(isize, @intCast(ping.len)), std.c.write(c.pair[1], ping.ptr, ping.len));
    shard.readFromClient(c.conn.fd);
    try std.testing.expect(c.conn.write_overflow);
    try std.testing.expectEqual(@as(u64, 0), sm.snapshot().handler_no_answer);
}

test "Shard: a run whose task couldn't be sent goes to an await parked on another shard" {
    var two: TwoShards = undefined;
    try two.init();
    defer two.deinit();
    for (&two.shards) |*s| {
        s.wireHandlerShardPtrs();
        try std.testing.expect(s.applyCommitted());
    }
    const owner = &two.shards[0];
    const other = &two.shards[1];
    // An action name shard 0 owns, so its register and invoke run there.
    var name_buf: [16]u8 = undefined;
    const name = for (0..256) |i| {
        const n = try std.fmt.bufPrint(&name_buf, "act-{d}", .{i});
        if (owner.router.route(node_router.hashKeyWithNamespace("default", n)) == .local) break n;
    } else return error.NoLocalName;

    const ca = try TestClient.open(owner);
    defer _ = std.c.close(ca.pair[1]);
    var out: [4096]u8 = undefined;
    const invoke_value: []const u8 = [_]u8{10} ++ [_]u8{0} ** 8 ++ [_]u8{ 0, 0, 0 } ++ "job";
    for ([_]struct { op: proto.OpCode, id: u64, value: []const u8 }{
        .{ .op = .action_register, .id = 1, .value = "" },
        .{ .op = .action_invoke, .id = 2, .value = invoke_value },
    }) |r| {
        const frame = try testRequest(r.op, r.id, name, r.value);
        defer std.testing.allocator.free(frame);
        _ = feedClient(ca.pair[1], owner, ca.conn.fd, frame);
        _ = owner.applyCommitted();
        const resp = try nextAnswer(ca, owner, &out);
        try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), resp.header.status);
    }

    // Claimed, as by an await whose task then couldn't be built. The
    // invoke's own wake is spent before the other shard's await parks.
    const task = owner.actions_handler.claimPendingRun("default", name, null, "w0") orelse return error.NoRun;
    _ = other.drainInbox();
    const cb = try TestClient.open(other);
    defer _ = std.c.close(cb.pair[1]);
    var await_value: [64]u8 = undefined;
    std.mem.writeInt(u32, await_value[0..4], 1, .little);
    std.mem.writeInt(u16, await_value[4..6], @intCast(name.len), .little);
    @memcpy(await_value[6..][0..name.len], name);
    const aw = try testRequest(.action_await, 3, "w1", await_value[0 .. 6 + name.len]);
    defer std.testing.allocator.free(aw);
    _ = feedClient(cb.pair[1], other, cb.conn.fd, aw);
    other.flushToClient(cb.conn.fd);
    try std.testing.expectEqual(@as(c_int, 0), unreadBytes(cb.pair[1]));

    @import("../actions/handler.zig").releaseAndWake(owner, task);
    _ = other.drainInbox();
    const resp = try nextAnswer(cb, other, &out);
    try std.testing.expectEqual(@as(u64, 3), resp.header.request_id);
    try std.testing.expectEqual(@intFromEnum(proto.StatusCode.ok), resp.header.status);
    try std.testing.expect(std.mem.endsWith(u8, resp.data, "job"));
}
