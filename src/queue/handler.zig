//! Queues: the ops a client sends, on the queue machine's families
//! (`queue/machine.zig`). Every change is an entry a leader proposes and
//! every replica applies alike; reads are of applied state.
//!
//! - A dequeue leases a queue's next ready messages to its consumer, until
//!   a complete, a fail, or the lease running out. It is answered when its
//!   deliver applies, with the messages and their delivery's token, which a
//!   complete, fail or touch names back. Delivery is at least once: a
//!   lease that runs out readies its messages again.
//! - A message failed or expired `MAX_ATTEMPTS` times is dead, kept until
//!   its queue is purged.
//! - A queue holds at most its cap of messages and held bytes, ready,
//!   leased and dead, and a shard's queues their cap of bytes: past one, an
//!   enqueue is refused `overloaded`. The leader stamps its caps (`Caps`,
//!   `[queues]` in flo.toml) into the entry and apply decides against them,
//!   so every replica agrees whatever its own settings.
//! - Messages go by priority (lowest first), then the time they became
//!   ready, then seq.

const std = @import("std");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const result_mod = @import("../protocol/result.zig");
const schema = @import("../protocol/schema.zig");
const dispatcher_mod = @import("../node/dispatcher.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const router = @import("../node/router.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence_mod = @import("../storage/persistence.zig");
const view_mod = @import("../contract/view.zig");
const store_mod = @import("../contract/store.zig");
const delivery = @import("../contract/delivery.zig");
const MetricsRegistry = @import("../metrics/registry.zig").MetricsRegistry;
const table_run = @import("../node/table_run.zig");
const ops = @import("../protocol/ops.zig");
const queue_ops = @import("../protocol/queue_ops.zig");
const qe = @import("../protocol/queue_entries.zig");
const ae = @import("../protocol/action_entries.zig");
const machine = @import("machine.zig");
const waiter_pool_mod = @import("../node/waiter_pool.zig");
const ReplyTo = @import("../node/reply_to.zig").ReplyTo;

const Result = persistence_mod.Result;
const CommandResult = result_mod.CommandResult;
const Dispatcher = dispatcher_mod.Dispatcher;
const Request = proto.Request;
const OpCode = proto.OpCode;
const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const Waiter = waiter_pool_mod.Waiter;
const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const ReadView = view_mod.ReadView;
const NamespaceId = router.NamespaceId;
const Msg = machine.Msg.Value;
const Meta = machine.Meta.Value;

const PeekResponse = table_run.op("queue.peek").response;
const DequeueResponse = table_run.op("queue.dequeue").response;
const ReportResponse = table_run.op("queue.complete").response;
const TouchResponse = table_run.op("queue.touch").response;

/// How often the expire duty runs: at least every half the shortest lease,
/// as the delivery model's key bound needs.
pub const DUTY_INTERVAL_MS: u64 = 1000;
comptime {
    std.debug.assert(DUTY_INTERVAL_MS * 2 <= ae.MIN_LEASE_MS);
}

/// A new leader waits this long, or a lease if shorter, past a lease's
/// deadline before ending it: a consumer that touched the old leader in
/// time may not have reached this one yet.
const MAX_GRACE_MS: u64 = 30_000;

/// What each message adds to a dequeue's answer besides its payload.
const MESSAGE_FRAMING: u32 = @intCast(queue_ops.Message.max_size - qe.MAX_MESSAGE);

const MAX_DLQ_PAGE: u32 = 100;
const MAX_DLQ_PAYLOAD: usize = 4096;
/// `[total:u64][has_more:u8][last_seq:u64]` after a page's entries.
const DLQ_TRAILER: usize = 8 + 1 + 8;

fn storeOf(shard: *Shard) *store_mod.Store {
    return &shard.defaultPartition().store;
}

fn readOf(shard: *Shard) ReadView {
    return .{ .store = storeOf(shard) };
}

fn nsToMs(ns: u64) i64 {
    return @intCast(ns / std.time.ns_per_ms);
}

/// Propose a queue entry, encoded in the shard's entry buffer when it fits.
fn propose(shard: *Shard, comptime etype: EntryType, body: anytype) !persistence_mod.ProposeResult {
    return proposeFlagged(shard, etype, entry_mod.Flags.NONE, body);
}

fn proposeFlagged(shard: *Shard, comptime etype: EntryType, flags: u16, body: anytype) !persistence_mod.ProposeResult {
    const P = qe.Payload(@backingInt(etype));
    const size = try P.encodedSize(body);
    if (size <= shard.entry_buf.len) return shard.raft_node.propose(etype, flags, try P.encodeInto(shard.entry_buf[0..size], body));
    const buf = try shard.allocator.alloc(u8, size);
    defer shard.allocator.free(buf);
    return shard.raft_node.propose(etype, flags, try P.encodeInto(buf, body));
}

/// A queue's meta, if it is `name`'s (another name may hash the same).
pub fn metaOf(shard: *Shard, ns: NamespaceId, name: []const u8) ?Meta {
    var b: [12]u8 = undefined;
    const hash = machine.hashOf(name);
    const got = (readOf(shard).get(machine.Metas, machine.storeKey(&b, ns, &hash)) catch return null) orelse return null;
    return if (std.mem.eql(u8, got.v.name, name)) got.v else null;
}

pub fn msgOf(shard: *Shard, ns: NamespaceId, hash: [8]u8, seq: u64) ?Msg {
    var b: [4 + qe.ID_LEN]u8 = undefined;
    const id = machine.idOf(hash, seq);
    const got = (readOf(shard).get(machine.Msgs, machine.storeKey(&b, ns, &id)) catch return null) orelse return null;
    return got.v;
}

/// Whether a sink's enqueue of `payload_len` bytes to `name` fits the caps
/// its entry would carry, counting `msgs` messages and `bytes` held bytes
/// the sink proposed there and not yet applied. Past them it would be
/// refused at apply, after the sink had moved past its record.
pub fn sinkWouldFit(caps: Caps, shard: *Shard, ns: NamespaceId, name: []const u8, payload_len: usize, msgs: u64, bytes: u64) bool {
    const size = machine.heldBy(payload_len, 0);
    if (metaOf(shard, ns, name)) |m| {
        if (m.ready + m.leased + m.dead + msgs + 1 > caps.max_messages) return false;
        if (m.held_bytes + bytes + size > caps.max_bytes) return false;
    } else if (msgs + 1 > caps.max_messages or bytes + size > caps.max_bytes) return false;
    const shard_held = if (readOf(shard).get(machine.Helds, machine.HELD_KEY) catch null) |h| h.v.bytes else 0;
    return shard_held + bytes + size <= caps.shard_held_bytes;
}

/// The messages a queue holds, ready, leased and dead.
pub fn countQueue(shard: *Shard, ns: NamespaceId, name: []const u8) u64 {
    const m = metaOf(shard, ns, name) orelse return 0;
    return m.ready + m.leased + m.dead;
}

/// A queue's ready messages due by `now`, in the order a deliver takes
/// them: a priority whose first message isn't due holds none that is, and
/// one seek passes it. At most `max_looked` keys are read.
const DueReady = struct {
    read: ReadView,
    prefix: []const u8,
    now: u64,
    max_looked: usize,
    it: view_mod.ReadRange(machine.Ready),
    looked: usize = 0,
    after: [machine.PAST_LEVEL_LEN]u8 = undefined,
    /// Stopped at `max_looked`, not at the queue's end.
    cut: bool = false,

    fn init(shard: *Shard, prefix_buf: []u8, ns: NamespaceId, hash: *const [8]u8, now: u64, max_looked: usize) DueReady {
        var rb: [9]u8 = undefined;
        const prefix = machine.storeKey(prefix_buf, ns, machine.readyPrefix(&rb, hash));
        return .{ .read = readOf(shard), .prefix = prefix, .now = now, .max_looked = max_looked, .it = readOf(shard).range(machine.Ready, prefix) };
    }

    /// The next due message's seq.
    fn next(self: *DueReady) ?u64 {
        while (self.it.next() catch null) |item| {
            if (self.looked == self.max_looked) {
                self.cut = true;
                return null;
            }
            self.looked += 1;
            const k = machine.parseReadyKey(item.key[4..], 8) orelse continue;
            if (k.due_ns > self.now) {
                self.it = self.read.rangeAfter(machine.Ready, self.prefix, machine.pastLevelInto(&self.after, item.key));
                continue;
            }
            if (k.id.len != qe.ID_LEN) continue;
            return machine.seqOf(k.id);
        }
        return null;
    }
};

/// How many of a queue's ready messages are due by `now`, counting no
/// further than `enough`, among the keys a deliver would look at.
fn countDue(shard: *Shard, ns: NamespaceId, hash: [8]u8, now: u64, enough: u32) u32 {
    var pb: [4 + 9]u8 = undefined;
    var due = DueReady.init(shard, &pb, ns, &hash, now, machine.MAX_LOOKED_AT);
    var n: u32 = 0;
    while (n < enough and due.next() != null) n += 1;
    return n;
}

/// What a queue, and a shard's queues, may hold: this node's `[queues]`
/// settings, stamped into each enqueue it proposes.
pub const Caps = struct {
    max_messages: u32 = qe.DEFAULT_MAX_MESSAGES,
    max_bytes: u64 = qe.DEFAULT_MAX_BYTES,
    shard_held_bytes: u64 = qe.DEFAULT_SHARD_HELD_BYTES,

    pub fn eql(a: Caps, b: Caps) bool {
        return a.max_messages == b.max_messages and a.max_bytes == b.max_bytes and a.shard_held_bytes == b.shard_held_bytes;
    }
};

/// An enqueue under `caps`.
pub fn enqueueValue(caps: Caps, ns: NamespaceId, name: []const u8, priority: u32, delay_ms: u64, payload: []const u8) qe.Enqueue.Value {
    return .{ .ns = ns.int(), .queue = name, .priority = priority, .delay_ms = delay_ms, .payload = payload, .max_messages = caps.max_messages, .max_bytes = caps.max_bytes, .shard_held_bytes = caps.shard_held_bytes };
}

pub const QueueHandler = struct {
    allocator: Allocator,

    /// This node's caps, set by the runtime from its settings.
    caps: Caps = .{},
    /// The leader's caps last seen to differ from `caps`, so the warning
    /// is said once per difference, not per enqueue.
    mismatch_seen: ?Caps = null,

    /// The owning shard, set by `Shard.wireHandlerShardPtrs`.
    shard_ptr: ?*anyopaque = null,

    /// Global metrics registry (optional, set by runtime when dashboard is enabled).
    metrics_registry: ?*MetricsRegistry = null,

    /// A peek's answer, encoded after the handler returns.
    answer_buf: ?[]u8 = null,

    /// Dequeues answered when their deliver applies.
    pending: std.ArrayListUnmanaged(Pending) = .empty,

    /// The expire proposed and not yet applied: one at a time. A full one
    /// chains into the next as it applies.
    expiring: ?u64 = null,
    expiring_full: bool = false,
    /// The term the duty last ran in, and when it first ran in it.
    duty_term: u64 = 0,
    duty_since_ns: u64 = 0,

    /// The queue an enqueue or a release just readied a message in, while
    /// its waiters are woken: its namespace's id, then its name hash.
    waking: [12]u8 = undefined,

    /// Dequeues with ack, answered when their complete applies.
    acking: std.ArrayListUnmanaged(Acking) = .empty,

    /// Per queue (namespace id, name hash), the messages delivers in
    /// flight will take: a dequeue proposes one only for messages due
    /// beyond these, so waiting consumers woken together don't each log a
    /// deliver that finds nothing.
    claimed: std.AutoHashMapUnmanaged([12]u8, u32) = .empty,

    const Acking = struct {
        index: u64,
        term: u64,
        reply_to: ReplyTo,
        request_id: u64,
        /// The answer once the complete applies, and the one if it can't
        /// be confirmed: the messages leased, with their delivery.
        answer: []u8,
        leased: []u8,

        fn free(a: Acking, allocator: Allocator) void {
            if (a.answer.ptr != a.leased.ptr) allocator.free(a.answer);
            allocator.free(a.leased);
        }
    };

    const Pending = struct {
        index: u64,
        term: u64,
        reply_to: ReplyTo,
        request_id: u64,
        /// A one-shot consume: completed as soon as it is answered.
        ack: bool,
        queue: [12]u8,
        claims: u32,
    };

    fn queueKey(ns: NamespaceId, hash: [8]u8) [12]u8 {
        return ns.bytes() ++ hash;
    }

    fn unclaim(self: *QueueHandler, queue: [12]u8, n: u32) void {
        const got = self.claimed.getPtr(queue) orelse return;
        got.* -|= n;
        if (got.* == 0) _ = self.claimed.remove(queue);
    }

    pub fn init(allocator: Allocator) QueueHandler {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *QueueHandler) void {
        if (self.answer_buf) |b| self.allocator.free(b);
        self.pending.deinit(self.allocator);
        self.claimed.deinit(self.allocator);
        for (self.acking.items) |a| a.free(self.allocator);
        self.acking.deinit(self.allocator);
    }

    pub fn register(dispatcher: *Dispatcher) void {
        dispatcher.registerWithRoute(.queue_enqueue, dispatchEnqueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dequeue, table_run.handler("queue.dequeue", QueueHandler, runDequeue), preRouteByQueue);
        dispatcher.registerWithRoute(.queue_complete, table_run.handler("queue.complete", QueueHandler, runComplete), preRouteByQueue);
        dispatcher.registerWithRoute(.queue_fail, table_run.handler("queue.fail", QueueHandler, runFail), preRouteByQueue);
        dispatcher.registerWithRoute(.queue_touch, table_run.handler("queue.touch", QueueHandler, runTouch), preRouteByQueue);
        dispatcher.registerWithRoute(.queue_peek, table_run.handler("queue.peek", QueueHandler, runPeek), preRouteByQueue);
        dispatcher.registerWithRoute(.queue_stats, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dlq_list, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dlq_requeue, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_dlq_delete, dispatchQueue, preRouteByQueue);
        dispatcher.registerWithRoute(.queue_purge, dispatchQueue, preRouteByQueue);
        dispatcher.registerWalk(.queue_list, dispatchQueue, localScanQueues);
    }

    /// ShardWalker LocalScanFn for queue_list: the queue names one shard
    /// holds in the namespace.
    fn localScanQueues(
        ctx: *anyopaque,
        ns: dispatcher_mod.WalkNamespace,
        _: []const u8, // filter
        _: ?[]const u8, // cursor
        _: u32, // limit
    ) dispatcher_mod.NameWalker.ScanResult {
        const handler: *QueueHandler = @ptrCast(@alignCast(ctx));
        const shard: *Shard = @ptrCast(@alignCast(handler.shard_ptr orelse return .{ .items = &.{}, .next_cursor = null }));
        const id = if (ns.id == .invalid) NamespaceId.default else ns.id;
        const store = storeOf(shard);
        const name_buf = dispatcher_mod.scanScratch(@intCast(store.family(machine.Metas.ID).count));
        var count: usize = 0;
        const prefix = id.bytes();
        var it = readOf(shard).range(machine.Metas, &prefix);
        while (it.next() catch null) |item| {
            if (count >= name_buf.len) break;
            name_buf[count] = item.value.v.name;
            count += 1;
        }
        return .{ .items = name_buf[0..count], .next_cursor = null };
    }

    fn preRouteByQueue(req: Request) ?u64 {
        if (req.key.len == 0) return 0;
        return router.routeNsKey(req.ns_id, req.key);
    }

    // ── Errors ──────────────────────────────────────────────────────────

    pub const Error = error{ OutOfMemory, NotLeading, NoQueue };

    pub fn refusal(err: Error) table_run.Refused {
        return switch (err) {
            error.OutOfMemory => .{ .status = .overloaded, .refusal = proto.notRun("overloaded: no memory for this answer; retry") },
            error.NotLeading => .{ .status = .unavailable, .refusal = .{ .reason = .leader_unknown, .ran = .no, .message = "unavailable: this node's leadership isn't settled yet; retry" } },
            error.NoQueue => .{ .status = .bad_request, .refusal = proto.notRun("bad request: a queue name is required") },
        };
    }

    // ── Enqueue, stats, DLQ, purge, list (their wire as before) ─────────

    /// Enqueue on its queue's leader, notifying nothing until it applies.
    fn dispatchEnqueue(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        shard.namespace_handler.proposeImplicitCreate(req.namespace, shard);
        const result = shard.queue_handler.handleCommand(req);
        defer shard.queue_handler.freeResult(result);
        if (result == .parked) return parkQueue(shard, conn, req, result.parked);
        sendQueueResponse(shard, conn, req.header.request_id, result);
    }

    fn dispatchQueue(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
        const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
        const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
        const result = shard.queue_handler.handleCommand(req);
        defer shard.queue_handler.freeResult(result);
        if (result == .parked) return parkQueue(shard, conn, req, result.parked);
        sendQueueResponse(shard, conn, req.header.request_id, result);
    }

    /// Park a queue write on its entry, answered from what its applier
    /// recorded. An enqueue counts against its namespace here, where it is
    /// accepted.
    fn parkQueue(shard: *Shard, conn: *Connection, req: Request, proposed: persistence_mod.ProposeResult) void {
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
        if (op == .queue_enqueue) shard.namespace_handler.markNamespaceHasData(req.namespace, shard);
        shard.park(conn, req, proposed, switch (op) {
            .queue_enqueue => answerEnqueue,
            .queue_purge => answerPurge,
            else => answerOk,
        });
    }

    fn answerEnqueue(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var id_buf: [20]u8 = undefined;
        sendQueueResponse(shard, conn, request_id, enqueued(result, &id_buf));
    }

    fn answerPurge(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        sendQueueResponse(shard, conn, request_id, .{ .queue_purged = .{ .count = @intCast(@min(r.int(u64) orelse 0, std.math.maxInt(u32))) } });
    }

    fn answerOk(shard: *Shard, conn: *Connection, request_id: u64, _: *const Result) void {
        sendQueueResponse(shard, conn, request_id, .ok);
    }

    /// An enqueue's answer: the seq its applier gave the message.
    fn enqueued(result: *const Result, id_buf: *[20]u8) CommandResult {
        var r = result.reader();
        const id_str = std.fmt.bufPrint(id_buf, "{d}", .{r.int(u64) orelse 0}) catch "0";
        return .{ .queue_enqueued = .{ .message_id = id_str } };
    }

    fn handleCommand(self: *QueueHandler, req: Request) CommandResult {
        const op: OpCode = @fromBackingInt(@intCast(req.header.op_code));
        if (req.key.len == 0 and op != .queue_list) return .{ .err = .{ .code = .invalid_request, .message = "queue name is required" } };
        const shard: *Shard = @ptrCast(@alignCast(self.shard_ptr orelse return .{ .err = .{ .code = .internal_error, .message = "queue handler has no shard" } }));
        return switch (op) {
            .queue_enqueue => self.handleEnqueue(shard, req, entry_mod.Flags.NONE),
            .queue_stats => self.handleStats(shard, req),
            .queue_dlq_list => self.handleDlqList(shard, req),
            .queue_dlq_requeue => .{ .err = .{ .code = .invalid_request, .message = "dlq requeue not yet implemented" } },
            .queue_dlq_delete => .{ .err = .{ .code = .invalid_request, .message = "dlq delete not yet implemented" } },
            .queue_purge => self.handlePurge(shard, req),
            .queue_list => self.handleList(shard, req),
            else => .{ .err = .{ .code = .invalid_request, .message = "unknown queue opcode" } },
        };
    }

    /// A processing job's queue sink writes through here, as a client's
    /// enqueue does.
    pub fn sinkEnqueue(self: *QueueHandler, req: Request) CommandResult {
        const shard: *Shard = @ptrCast(@alignCast(self.shard_ptr orelse return .{ .err = .{ .code = .internal_error, .message = "queue handler has no shard" } }));
        return self.handleEnqueue(shard, req, entry_mod.Flags.SINK);
    }

    fn handleEnqueue(self: *QueueHandler, shard: *Shard, req: Request, flags: u16) CommandResult {
        if (req.key.len == 0) return .{ .err = .{ .code = .invalid_request, .message = "queue name is required" } };
        if (req.value.len > qe.MAX_MESSAGE) return .{ .err = .{ .code = .invalid_request, .message = "message too large: a queue message holds at most 64 KiB" } };
        if (req.key.len > qe.QueueName.max_size) return .{ .err = .{ .code = .invalid_request, .message = "queue name too long" } };
        const priority: u32 = if (req.getPriority()) |p| p else 0;
        if (req.findOption(.dedup_key) != null) return .{ .err = .{ .code = .invalid_request, .message = "bad request: dedup_key isn't supported: a queue keeps no deduplication window" } };
        const delay_ms: u64 = if (req.findOption(.delay_ms)) |o| o.asU64() orelse return .{ .err = .{ .code = .invalid_request, .message = "bad request: delay_ms is a u64" } } else 0;
        if (delay_ms > qe.MAX_DELAY_MS) return .{ .err = .{ .code = .invalid_request, .message = "bad request: delay_ms is at most 86400000 (a day)" } };
        // As every write resolves its namespace here (`persistence.proposeEntry`):
        // published, or on shard 0 proposed ahead of this write.
        const ns = shard.namespaceId(req.namespace);
        if (ns == .invalid) return .{ .err = persistence_mod.failureError(error.NamespaceUnknown, "enqueue not persisted") };
        const proposed = proposeFlagged(shard, .queue_enqueue, flags, enqueueValue(self.caps, ns, req.key, priority, delay_ms, req.value)) catch |err| {
            return .{ .err = persistence_mod.failureError(err, "enqueue not persisted") };
        };
        return .{ .parked = proposed };
    }

    /// One queue's numbers, in the request's namespace:
    /// [enqueued][delivered][settled][released][dead_lettered][expired]
    /// [ready][leased][dead], u64 each.
    fn handleStats(self: *QueueHandler, shard: *Shard, req: Request) CommandResult {
        const m: Meta = metaOf(shard, req.ns_id, req.key) orelse machine.emptyMeta(req.key);
        const buf = self.allocator.alloc(u8, 9 * 8) catch return .{ .err = .{ .code = .internal_error, .message = "stats serialization failed" } };
        for ([_]u64{ m.enqueued, m.delivered, m.settled, m.released, m.dead_lettered, m.expired, m.ready, m.leased, m.dead }, 0..) |v, i|
            std.mem.writeInt(u64, buf[i * 8 ..][0..8], v, .little);
        return .{ .queue_messages = .{ .data = buf } };
    }

    /// A page of a queue's dead letters, oldest first:
    ///   [count:u32] ([seq:u64][size:u32][len:u32][payload][enqueued_at_ms:i64]
    ///                [attempts:u32][priority:u8][moved_at_ms:i64])*
    ///   [total:u64][has_more:u8][last_seq:u64]
    /// `[limit:u32][after:u64]?` picks the page: up to 100, after the dead
    /// letter whose seq ends the page before, or from the oldest if that one
    /// is gone. Each payload is cut to 4 KiB (`size` is its whole length).
    fn handleDlqList(self: *QueueHandler, shard: *Shard, req: Request) CommandResult {
        var limit: u32 = MAX_DLQ_PAGE;
        var after: ?u64 = null;
        if (req.value.len >= 4) limit = std.math.clamp(std.mem.readInt(u32, req.value[0..4], .little), 1, MAX_DLQ_PAGE);
        if (req.value.len >= 12) after = std.mem.readInt(u64, req.value[4..12], .little);
        const data = self.dlqPage(shard, req.ns_id, req.key, limit, after) catch
            return .{ .err = .{ .code = .internal_error, .message = "dlq list serialization failed" } };
        return .{ .queue_dlq_messages = .{ .data = data } };
    }

    fn dlqPage(self: *QueueHandler, shard: *Shard, ns: NamespaceId, name: []const u8, limit: u32, after: ?u64) ![]u8 {
        const hash = machine.hashOf(name);
        const total: u64 = if (metaOf(shard, ns, name)) |m| m.dead else 0;
        var pb: [12]u8 = undefined;
        const prefix = machine.storeKey(&pb, ns, &hash);
        var ab: [4 + 24]u8 = undefined;
        var it = blk: {
            if (total > 0) if (after) |seq| if (msgOf(shard, ns, hash, seq)) |m| if (m.status == .dead) {
                const dk = machine.deadKey(hash, m.dead_ns, seq);
                break :blk readOf(shard).rangeAfter(machine.Dead, prefix, machine.storeKey(&ab, ns, &dk));
            };
            break :blk readOf(shard).range(machine.Dead, if (total > 0) prefix else "\xff\xff\xff\xff\xff");
        };
        var w: std.Io.Writer.Allocating = .init(self.allocator);
        errdefer w.deinit();
        try w.writer.writeInt(u32, 0, .little);
        var count: u32 = 0;
        var last: u64 = 0;
        var more = false;
        while (it.next() catch null) |item| {
            if (count == limit) {
                more = true;
                break;
            }
            const k = item.key[4..];
            if (k.len != 24) continue;
            const seq = std.mem.readInt(u64, k[16..24], .big);
            const msg = msgOf(shard, ns, hash, seq) orelse continue;
            const shown = msg.payload[0..@min(msg.payload.len, MAX_DLQ_PAYLOAD)];
            const size = 8 + 4 + 4 + shown.len + 8 + 4 + 1 + 8;
            if (w.written().len + size + DLQ_TRAILER > ops.MAX_RESPONSE_BODY) {
                more = true;
                break;
            }
            try w.writer.writeInt(u64, seq, .little);
            try w.writer.writeInt(u32, @intCast(msg.payload.len), .little);
            try w.writer.writeInt(u32, @intCast(shown.len), .little);
            try w.writer.writeAll(shown);
            try w.writer.writeInt(i64, nsToMs(msg.enqueued_ns), .little);
            try w.writer.writeInt(u32, msg.deliveries, .little);
            try w.writer.writeInt(u8, @intCast(@min(msg.priority, 255)), .little);
            try w.writer.writeInt(i64, nsToMs(msg.dead_ns), .little);
            count += 1;
            last = seq;
        }
        try w.writer.writeInt(u64, total, .little);
        try w.writer.writeByte(@intFromBool(more));
        try w.writer.writeInt(u64, last, .little);
        const out = try w.toOwnedSlice();
        std.mem.writeInt(u32, out[0..4], count, .little);
        return out;
    }

    fn handlePurge(_: *QueueHandler, shard: *Shard, req: Request) CommandResult {
        if (req.key.len > qe.QueueName.max_size) return .{ .err = .{ .code = .invalid_request, .message = "queue name too long" } };
        if (metaOf(shard, req.ns_id, req.key) == null) return .{ .queue_purged = .{ .count = 0 } };
        const proposed = propose(shard, .queue_purge, qe.Purge.Value{ .ns = req.ns_id.int(), .queue = req.key }) catch |err|
            return .{ .err = persistence_mod.failureError(err, "purge not persisted") };
        return .{ .parked = proposed };
    }

    /// This shard's queues in the request's namespace:
    /// [count:u32] ([name_len:u32][name][ns_len:u32][ns]
    ///   [ready:u64][leased:u64][enqueued:u64][delivered:u64][dead:u64])*
    /// [has_more:u8][cursor_len:u16]
    fn handleList(self: *QueueHandler, shard: *Shard, req: Request) CommandResult {
        var w: std.Io.Writer.Allocating = .init(self.allocator);
        errdefer w.deinit();
        writeList(shard, req.ns_id, req.namespace, &w.writer) catch return .{ .err = .{ .code = .internal_error, .message = "list serialization failed" } };
        const data = w.toOwnedSlice() catch return .{ .err = .{ .code = .internal_error, .message = "list serialization failed" } };
        return .{ .queue_messages = .{ .data = data } };
    }

    fn writeList(shard: *Shard, ns: NamespaceId, namespace: []const u8, w: *std.Io.Writer) !void {
        var count: u32 = 0;
        const prefix = ns.bytes();
        var it = readOf(shard).range(machine.Metas, &prefix);
        try w.writeInt(u32, 0, .little);
        while (it.next() catch null) |item| {
            try writeListEntry(item.value.v, namespace, w);
            count += 1;
        }
        try w.writeByte(0);
        try w.writeInt(u16, 0, .little);
        if (w.buffered().len >= 4) std.mem.writeInt(u32, w.buffered()[0..4], count, .little);
    }

    /// One queue as a list shows it.
    pub fn writeListEntry(m: Meta, namespace: []const u8, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try w.writeInt(u32, @intCast(m.name.len), .little);
        try w.writeAll(m.name);
        try w.writeInt(u32, @intCast(namespace.len), .little);
        try w.writeAll(namespace);
        for ([_]u64{ m.ready, m.leased, m.enqueued, m.delivered, m.dead }) |v| try w.writeInt(u64, v, .little);
    }

    pub fn freeResult(self: *QueueHandler, cmd_result: CommandResult) void {
        switch (cmd_result) {
            .queue_messages => |r| self.allocator.free(r.data),
            .queue_dlq_messages => |r| self.allocator.free(r.data),
            else => {},
        }
    }

    // ── Peek ────────────────────────────────────────────────────────────

    fn answerBuf(self: *QueueHandler) Error![]u8 {
        if (self.answer_buf) |b| return b;
        const b = try self.allocator.alloc(u8, queue_ops.PeekedList.byte_bound.?);
        self.answer_buf = b;
        return b;
    }

    /// The queue's next ready messages, as a dequeue would take them now,
    /// and none taken. A page stops where the next message wouldn't fit.
    fn runPeek(shard: *Shard, _: *Connection, req: Request, limit: u32) Error!table_run.Outcome(PeekResponse) {
        const self = shard.queue_handler;
        const hash = machine.hashOf(req.key);
        var out = queue_ops.PeekedList.Builder.init(try self.answerBuf());
        var more = false;
        if (metaOf(shard, req.ns_id, req.key) != null) {
            var pb: [4 + 9]u8 = undefined;
            var due = DueReady.init(shard, &pb, req.ns_id, &hash, shard.raft_node.now(), queue_ops.MAX_PEEK + machine.MAX_LOOKED_AT);
            while (due.next()) |seq| {
                const m = msgOf(shard, req.ns_id, hash, seq) orelse continue;
                if (out.count == limit) {
                    more = true;
                    break;
                }
                const shown = m.payload[0..@min(m.payload.len, queue_ops.MAX_PEEK_PAYLOAD)];
                const added = out.add(.{
                    .seq = seq,
                    .priority = m.priority,
                    .attempts = m.deliveries,
                    .enqueued_at_ms = nsToMs(m.enqueued_ns),
                    .size = @intCast(m.payload.len),
                    .payload = shown,
                }) catch false;
                if (!added) {
                    more = true;
                    break;
                }
            }
            if (due.cut) more = true;
        }
        return .{ .answer = .{ .messages = out.value(), .more = more } };
    }

    // ── Dequeue ─────────────────────────────────────────────────────────

    /// Lease the queue's next ready messages to `consumer`. A leader whose
    /// term's entries have applied proposes a deliver if any is due, and
    /// answers when it applies; with none, a blocking dequeue waits for an
    /// enqueue, a release or a lease running out, and another answers empty.
    fn runDequeue(shard: *Shard, conn: *Connection, req: Request, consumer: []const u8, count: u32, lease_ms: ?u64, block_ms: u64, ack: bool) Error!table_run.Outcome(DequeueResponse) {
        if (req.key.len == 0) return error.NoQueue;
        if (!shard.raft_node.leaderReady()) return error.NotLeading;
        if (req.key.len > qe.QueueName.max_size) return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun("bad request: queue name too long") } };
        const params: Params = .{ .ns = req.ns_id, .count = count, .lease_ms = lease_ms orelse qe.DEFAULT_LEASE_MS, .ack = ack, .consumer = consumer };
        const self = shard.queue_handler;
        if (try self.deliverHere(shard, conn.replyTo(), req.header.request_id, req.key, params)) return .wait;
        self.recordDequeue(shard, req.ns_id, req.key, 0, 0);
        if (block_ms == 0) return .{ .answer = empty };
        var kb: [waiter_pool_mod.MAX_WAITER_KEY]u8 = undefined;
        const key = params.waiterKey(&kb, machine.hashOf(req.key)) orelse
            return .{ .refused = .{ .status = .bad_request, .refusal = proto.notRun("bad request: consumer id too long to wait on") } };
        const registered = shard.waiter_pool.register(.{
            .kind = .queue_dequeue,
            .reply_to = conn.replyTo(),
            .request_id = req.header.request_id,
            .key = key,
            .min_version = 0,
            .timeout_ms = @intCast(block_ms),
        });
        if (!registered) return .{ .refused = .{ .status = .overloaded, .refusal = proto.notRun("overloaded: too many blocking reads are waiting; retry") } };
        return .wait;
    }

    const empty: DequeueResponse.Value = .{ .messages = .{ .bytes = "", .count = 0 }, .term = 0, .index = 0, .expires_at_ms = 0 };

    /// A waiting dequeue whose wait ran out: no messages.
    pub fn answerEmpty(shard: *Shard, reply_to: ReplyTo, request_id: u64) void {
        var buf: [@sizeOf(proto.ResponseHeader) + 64]u8 = undefined;
        const body = DequeueResponse.encodeInto(buf[@sizeOf(proto.ResponseHeader)..], empty) catch unreachable;
        shard.deliverDeferred(reply_to, proto.Response.sealOk(request_id, &buf, body.len));
    }

    /// A dequeue's asks, as it was sent and as its waiter keeps them.
    const Params = struct {
        ns: NamespaceId,
        count: u32,
        lease_ms: u64,
        ack: bool,
        consumer: []const u8,

        /// `[ns id][name hash][count:u8][lease_ms:u32][ack:u8][consumer]`:
        /// the queue first, so an enqueue wakes only its own waiters.
        fn waiterKey(p: Params, buf: []u8, hash: [8]u8) ?[]const u8 {
            const n = 12 + 1 + 4 + 1 + p.consumer.len;
            if (n > buf.len) return null;
            buf[0..4].* = p.ns.bytes();
            buf[4..12].* = hash;
            buf[12] = @intCast(p.count);
            std.mem.writeInt(u32, buf[13..17], @intCast(p.lease_ms), .little);
            buf[17] = @intFromBool(p.ack);
            @memcpy(buf[18..n], p.consumer);
            return buf[0..n];
        }

        fn ofWaiterKey(k: []const u8) ?struct { params: Params, hash: [8]u8 } {
            if (k.len < 18) return null;
            return .{ .params = .{
                .ns = .of(std.mem.readInt(u32, k[0..4], .big)),
                .count = k[12],
                .lease_ms = std.mem.readInt(u32, k[13..17], .little),
                .ack = k[17] != 0,
                .consumer = k[18..],
            }, .hash = k[4..12].* };
        }
    };

    /// Propose a deliver of `name`'s due messages, to be answered to
    /// `reply_to` when it applies. False when none is due, or it couldn't be
    /// proposed (and is answered as none).
    fn deliverHere(self: *QueueHandler, shard: *Shard, reply_to: ReplyTo, request_id: u64, name: []const u8, p: Params) Error!bool {
        const hash = machine.hashOf(name);
        if (metaOf(shard, p.ns, name) == null) return false;
        const queue = queueKey(p.ns, hash);
        const held = self.claimed.get(queue) orelse 0;
        // The clock the deliver's stamp will read. Messages delivers in
        // flight will take aren't this dequeue's to propose for.
        const due = countDue(shard, p.ns, hash, shard.raft_node.now(), held +| p.count);
        if (due <= held) return false;
        const take = @min(p.count, due - held);
        try self.pending.ensureUnusedCapacity(self.allocator, 1);
        try self.claimed.ensureUnusedCapacity(self.allocator, 1);
        const max_bytes = queue_ops.MESSAGES_BYTES - take * MESSAGE_FRAMING;
        const proposed = propose(shard, .queue_deliver, qe.Deliver.Value{ .ns = p.ns.int(), .queue = name, .count = take, .max_bytes = max_bytes, .holder = p.consumer, .lease_ms = p.lease_ms }) catch |err| {
            if (!shard.quietHalted(err)) log.warn("shard {d}: dequeue from queue {s} not proposed: {s}", .{ shard.id, name, @errorName(err) });
            return false;
        };
        self.claimed.putAssumeCapacity(queue, held + take);
        self.pending.appendAssumeCapacity(.{ .index = proposed.index, .term = proposed.term, .reply_to = reply_to, .request_id = request_id, .ack = p.ack, .queue = queue, .claims = take });
        return true;
    }

    /// A deliver applied on the leader: answer the dequeue waiting for it
    /// with the messages it leased, read back from state.
    fn deliverApplied(self: *QueueHandler, shard: *Shard, e: *const Entry, result: *const Result) void {
        const at = for (self.pending.items, 0..) |pd, i| {
            if (pd.index == e.header.index and pd.term == e.header.term) break i;
        } else return;
        const pd = self.pending.swapRemove(at);
        self.unclaim(pd.queue, pd.claims);
        var diag: schema.Diagnostic = .{};
        const p = qe.Deliver.decodeAll(e.payload, &diag) catch return;
        // A deliver that took fewer than it claimed left messages for the
        // dequeues still waiting.
        var took_r = result.reader();
        const took = took_r.int(u32) orelse 0;
        defer if (took < pd.claims) self.wakeQueue(shard, .of(p.ns), machine.hashOf(p.queue));
        const ns: NamespaceId = .of(p.ns);
        const hash = machine.hashOf(p.queue);
        const buf = shard.allocator.create([@sizeOf(proto.ResponseHeader) + DequeueResponse.max_size]u8) catch
            return shard.deliverDeferredRefusal(pd.reply_to, pd.request_id, .internal_error, .{ .reason = .answer_lost, .ran = .yes, .message = "internal error: out of memory for the messages; they come back once their lease runs out" });
        defer shard.allocator.destroy(buf);
        const list_buf = shard.allocator.alloc(u8, queue_ops.MESSAGES_BYTES) catch
            return shard.deliverDeferredRefusal(pd.reply_to, pd.request_id, .internal_error, .{ .reason = .answer_lost, .ran = .yes, .message = "internal error: out of memory for the messages; they come back once their lease runs out" });
        defer shard.allocator.free(list_buf);
        var list = queue_ops.Messages.Builder.init(list_buf);
        var items_buf: [qe.MAX_BATCH * qe.Item.max_size]u8 = undefined;
        var items_len: usize = 0;
        var r = result.reader();
        const n = r.int(u32) orelse 0;
        var expiry: u64 = 0;
        var bytes: usize = 0;
        for (0..n) |_| {
            const seq = r.int(u64) orelse break;
            const m = msgOf(shard, ns, hash, seq) orelse continue;
            expiry = m.expiry_ns;
            bytes += m.payload.len;
            const added = list.add(.{ .seq = seq, .priority = m.priority, .deliveries = m.deliveries, .enqueued_at_ms = nsToMs(m.enqueued_ns), .payload = m.payload }) catch false;
            if (!added) break;
            items_len += (qe.Item.encodeInto(items_buf[items_len..], .{ .seq = seq, .deliveries = m.deliveries }) catch break).len;
        }
        self.recordDequeue(shard, ns, p.queue, list.count, bytes);
        const token: ae.Token.Value = .{ .term = e.header.term, .index = e.header.index };
        const ack = pd.ack and list.count > 0;
        // As a plain dequeue: leased, with its delivery and expiry.
        const leased: DequeueResponse.Value = .{ .messages = list.value(), .term = token.term, .index = token.index, .expires_at_ms = if (list.count == 0) 0 else nsToMs(expiry) };
        const out = table_run.encodeAnswer(DequeueResponse, buf, pd.request_id, leased) orelse
            return shard.deliverDeferredRefusal(pd.reply_to, pd.request_id, .internal_error, .{ .reason = .answer_lost, .ran = .yes, .message = "internal error: the messages didn't fit their answer; they come back once their lease runs out" });
        if (!ack) return shard.deliverDeferred(pd.reply_to, out);
        // With ack, answered once the complete applies. Where that can't be
        // confirmed, the messages go as a plain dequeue's, leased with their
        // delivery: the client always gets them, and its later ack finds them
        // gone (done) if the complete landed, or completes them if not.
        self.acking.ensureUnusedCapacity(self.allocator, 1) catch return shard.deliverDeferred(pd.reply_to, out);
        const leased_owned = self.allocator.dupe(u8, out) catch return shard.deliverDeferred(pd.reply_to, out);
        const proposed = propose(shard, .queue_settle, qe.Settle.Value{ .ns = p.ns, .queue = p.queue, .holder = p.holder, .token = token, .expiry_ns = expiry, .items = .{ .bytes = items_buf[0..items_len], .count = list.count } }) catch |err| {
            if (!shard.quietHalted(err)) log.warn("shard {d}: an acked dequeue from queue {s} not completed: {s}; answered as leased", .{ shard.id, p.queue, @errorName(err) });
            defer self.allocator.free(leased_owned);
            return shard.deliverDeferred(pd.reply_to, leased_owned);
        };
        var done = leased;
        done.expires_at_ms = 0;
        const done_owned: []u8 = if (table_run.encodeAnswer(DequeueResponse, buf, pd.request_id, done)) |d| self.allocator.dupe(u8, d) catch leased_owned else leased_owned;
        self.acking.appendAssumeCapacity(.{ .index = proposed.index, .term = proposed.term, .reply_to = pd.reply_to, .request_id = pd.request_id, .answer = done_owned, .leased = leased_owned });
    }

    /// An acked dequeue's complete applied: its answer goes, as it was
    /// read when its deliver applied.
    fn ackApplied(self: *QueueHandler, shard: *Shard, e: *const Entry, result: *const Result) void {
        const at = for (self.acking.items, 0..) |a, i| {
            if (a.index == e.header.index and a.term == e.header.term) break i;
        } else return;
        const a = self.acking.swapRemove(at);
        defer a.free(self.allocator);
        // Refused, the messages are only leased: so said.
        shard.deliverDeferred(a.reply_to, if (result.refused() != null) a.leased else a.answer);
    }

    fn recordDequeue(self: *QueueHandler, shard: *Shard, ns: NamespaceId, name: []const u8, n: usize, bytes: usize) void {
        const mr = self.metrics_registry orelse return;
        const namespace = shard.namespace_handler.nameOf(ns) orelse return;
        const qm = mr.registerQueue(namespace, name) catch return;
        if (n == 0) qm.recordEmptyDequeue() else qm.recordDequeue(n, bytes);
    }

    // ── Waking ──────────────────────────────────────────────────────────

    /// Messages may be ready in the queue `e` names: wake its waiting
    /// dequeues, each proposing a deliver if any is due.
    fn wakeQueue(self: *QueueHandler, shard: *Shard, ns: NamespaceId, hash: [8]u8) void {
        self.waking[0..4].* = ns.bytes();
        self.waking[4..12].* = hash;
        shard.waiter_pool.notifyAny(.queue_dequeue, resolveWaiter, @ptrCast(shard));
    }

    /// A waiting dequeue for the queue being woken (`waking`), or for any
    /// queue with a message due, from the duty: delivered to if one is due.
    pub fn resolveWaiter(waiter: *Waiter, ctx: *anyopaque) bool {
        const shard: *Shard = @ptrCast(@alignCast(ctx));
        const self = shard.queue_handler;
        const k = waiter.key();
        if (k.len < 12 or !std.mem.eql(u8, k[0..12], &self.waking)) return false;
        return self.resolve(shard, waiter);
    }

    fn resolveAny(waiter: *Waiter, ctx: *anyopaque) bool {
        const shard: *Shard = @ptrCast(@alignCast(ctx));
        return shard.queue_handler.resolve(shard, waiter);
    }

    fn resolve(self: *QueueHandler, shard: *Shard, waiter: *Waiter) bool {
        if (!shard.raft_node.leaderReady()) return false;
        const got = Params.ofWaiterKey(waiter.key()) orelse return false;
        // The name is the meta's: the waiter holds only its hash.
        var b: [12]u8 = undefined;
        const meta = (readOf(shard).get(machine.Metas, machine.storeKey(&b, got.params.ns, &got.hash)) catch return false) orelse return false;
        return self.deliverHere(shard, waiter.reply_to, waiter.request_id, meta.v.name, got.params) catch false;
    }

    // ── Reports ─────────────────────────────────────────────────────────

    /// The expiry of the delivery a report names, as this leader holds it:
    /// the first of its messages still leased under that token. 0 when
    /// none is, and the report is answered message by message.
    fn leaseOf(shard: *Shard, ns: NamespaceId, hash: [8]u8, consumer: []const u8, token: ae.Token.Value, items: schema.List(queue_ops.Item, queue_ops.MAX_BATCH).Value) ?Msg {
        var it = items.iterator();
        while (it.next() catch null) |item| {
            const m = msgOf(shard, ns, hash, item.seq) orelse continue;
            if (delivery.holds(m, consumer, item.deliveries, token)) return m;
        }
        return null;
    }

    /// The report's messages, as its entry names them.
    fn entryItems(buf: []u8, items: schema.List(queue_ops.Item, queue_ops.MAX_BATCH).Value) ?qe.Items.Value {
        // The op's item and the entry's are the same two fields.
        comptime std.debug.assert(queue_ops.Item.max_size == qe.Item.max_size);
        if (items.bytes.len > buf.len) return null;
        @memcpy(buf[0..items.bytes.len], items.bytes);
        return .{ .bytes = buf[0..items.bytes.len], .count = items.count };
    }

    /// A report on a queue this leader doesn't hold: each message is gone,
    /// and nothing is logged.
    fn allGone(buf: []u8, messages: schema.List(queue_ops.Item, queue_ops.MAX_BATCH).Value) schema.List(queue_ops.Refused, queue_ops.MAX_BATCH).Value {
        var b = schema.List(queue_ops.Refused, queue_ops.MAX_BATCH).Builder.init(buf);
        var it = messages.iterator();
        while (it.next() catch null) |item| _ = b.add(.{ .seq = item.seq, .why = .gone, .reason = machine.Why.gone.text() }) catch break;
        return b.value();
    }

    fn runComplete(shard: *Shard, _: *Connection, req: Request, consumer: []const u8, term: u64, index: u64, messages: schema.List(queue_ops.Item, queue_ops.MAX_BATCH).Value) Error!table_run.Outcome(ReportResponse) {
        if (req.key.len == 0) return error.NoQueue;
        if (!shard.raft_node.leaderReady()) return error.NotLeading;
        // A namespace this node can't name, answered as a write to it is:
        // its table may yet learn it, so the client retries.
        if (req.ns_id == .invalid) return .{ .refused = .{ .status = persistence_mod.failureStatus(error.NamespaceUnknown), .refusal = persistence_mod.failureRefusal(error.NamespaceUnknown, "report not run") } };
        if (metaOf(shard, req.ns_id, req.key) == null) {
            const self = shard.queue_handler;
            return .{ .answer = .{ .applied = 0, .refused = allGone(try self.answerBuf(), messages) } };
        }
        const token: ae.Token.Value = .{ .term = term, .index = index };
        const lease = leaseOf(shard, req.ns_id, machine.hashOf(req.key), consumer, token, messages);
        var ib: [qe.Items.max_size]u8 = undefined;
        const items = entryItems(&ib, messages) orelse return error.OutOfMemory;
        const proposed = propose(shard, .queue_settle, qe.Settle.Value{ .ns = req.ns_id.int(), .queue = req.key, .holder = consumer, .token = token, .expiry_ns = if (lease) |m| m.expiry_ns else 0, .items = items }) catch |err|
            return .{ .refused = .{ .status = persistence_mod.failureStatus(err), .refusal = persistence_mod.failureRefusal(err, "complete not persisted") } };
        return .{ .park = .{ .proposed = proposed, .answer = answerReport } };
    }

    fn runFail(shard: *Shard, _: *Connection, req: Request, consumer: []const u8, term: u64, index: u64, messages: schema.List(queue_ops.Item, queue_ops.MAX_BATCH).Value, delay_ms: u64, dead: bool, err_text: []const u8) Error!table_run.Outcome(ReportResponse) {
        if (req.key.len == 0) return error.NoQueue;
        if (!shard.raft_node.leaderReady()) return error.NotLeading;
        // A namespace this node can't name, answered as a write to it is:
        // its table may yet learn it, so the client retries.
        if (req.ns_id == .invalid) return .{ .refused = .{ .status = persistence_mod.failureStatus(error.NamespaceUnknown), .refusal = persistence_mod.failureRefusal(error.NamespaceUnknown, "report not run") } };
        if (metaOf(shard, req.ns_id, req.key) == null) {
            const self = shard.queue_handler;
            return .{ .answer = .{ .applied = 0, .refused = allGone(try self.answerBuf(), messages) } };
        }
        const token: ae.Token.Value = .{ .term = term, .index = index };
        const lease = leaseOf(shard, req.ns_id, machine.hashOf(req.key), consumer, token, messages);
        var ib: [qe.Items.max_size]u8 = undefined;
        const items = entryItems(&ib, messages) orelse return error.OutOfMemory;
        const proposed = propose(shard, .queue_release, qe.Release.Value{ .ns = req.ns_id.int(), .queue = req.key, .holder = consumer, .token = token, .expiry_ns = if (lease) |m| m.expiry_ns else 0, .items = items, .delay_ms = delay_ms, .dead = dead, .@"error" = err_text }) catch |err|
            return .{ .refused = .{ .status = persistence_mod.failureStatus(err), .refusal = persistence_mod.failureRefusal(err, "fail not persisted") } };
        return .{ .park = .{ .proposed = proposed, .answer = answerReport } };
    }

    /// Keep a delivery: answered with the lease as it stands while more
    /// than half of it is left; otherwise an extend is proposed and
    /// answered when it applies.
    fn runTouch(shard: *Shard, _: *Connection, req: Request, consumer: []const u8, term: u64, index: u64, messages: schema.List(queue_ops.Item, queue_ops.MAX_BATCH).Value) Error!table_run.Outcome(TouchResponse) {
        if (req.key.len == 0) return error.NoQueue;
        if (!shard.raft_node.leaderReady()) return error.NotLeading;
        // A namespace this node can't name, answered as a write to it is:
        // its table may yet learn it, so the client retries.
        if (req.ns_id == .invalid) return .{ .refused = .{ .status = persistence_mod.failureStatus(error.NamespaceUnknown), .refusal = persistence_mod.failureRefusal(error.NamespaceUnknown, "report not run") } };
        if (metaOf(shard, req.ns_id, req.key) == null) {
            const self = shard.queue_handler;
            return .{ .answer = .{ .expires_at_ms = 0, .applied = 0, .refused = allGone(try self.answerBuf(), messages) } };
        }
        const token: ae.Token.Value = .{ .term = term, .index = index };
        const hash = machine.hashOf(req.key);
        const lease = leaseOf(shard, req.ns_id, hash, consumer, token, messages);
        // More than half the lease left, on every message named: it stands,
        // and nothing is logged.
        if (lease) |held| if (held.expiry_ns -| shard.raft_node.now() > (held.lease_ms *| std.time.ns_per_ms) / 2) {
            var all = true;
            var it = messages.iterator();
            while (it.next() catch null) |item| {
                const m = msgOf(shard, req.ns_id, hash, item.seq) orelse {
                    all = false;
                    break;
                };
                if (!delivery.holds(m, consumer, item.deliveries, token) or m.expiry_ns != held.expiry_ns) all = false;
            }
            if (all) return .{ .answer = .{ .expires_at_ms = nsToMs(held.expiry_ns), .applied = messages.count, .refused = .{ .bytes = "", .count = 0 } } };
        };
        var ib: [qe.Items.max_size]u8 = undefined;
        const items = entryItems(&ib, messages) orelse return error.OutOfMemory;
        const m = lease orelse Msg{ .queue = "", .priority = 0, .status = .ready, .payload = "", .enqueued_ns = 0, .due_ns = 0, .deliveries = 0, .failures = 0, .holder = "", .request_key = null, .token = token, .lease_ms = ae.MIN_LEASE_MS, .delivered_ns = 0, .expiry_ns = 0, .ended = .none, .started_ns = 0, .@"error" = "", .dead_ns = 0 };
        const proposed = propose(shard, .queue_extend, qe.Extend.Value{ .ns = req.ns_id.int(), .queue = req.key, .holder = consumer, .token = token, .expiry_ns = m.expiry_ns, .held_since_ns = m.delivered_ns, .lease_ms = std.math.clamp(m.lease_ms, ae.MIN_LEASE_MS, ae.MAX_LEASE_MS), .items = items }) catch |err|
            return .{ .refused = .{ .status = persistence_mod.failureStatus(err), .refusal = persistence_mod.failureRefusal(err, "touch not persisted") } };
        return .{ .park = .{ .proposed = proposed, .answer = answerTouch } };
    }

    /// A report's answer, from its result: how many it applied to, then
    /// each it didn't, and why.
    fn reportAnswer(r: *Result.Reader, buf: []u8) schema.List(queue_ops.Refused, queue_ops.MAX_BATCH).Value {
        var b = schema.List(queue_ops.Refused, queue_ops.MAX_BATCH).Builder.init(buf);
        while (r.int(u64)) |seq| {
            const why: machine.Why = std.enums.fromInt(machine.Why, r.int(u8) orelse break) orelse break;
            _ = b.add(.{ .seq = seq, .why = why, .reason = why.text() }) catch break;
        }
        return b.value();
    }

    fn answerReport(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        const applied = r.int(u32) orelse 0;
        var buf: [queue_ops.MAX_BATCH * queue_ops.Refused.max_size]u8 = undefined;
        table_run.answer(ReportResponse, shard, conn, request_id, .{ .applied = applied, .refused = reportAnswer(&r, &buf) });
    }

    fn answerTouch(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
        var r = result.reader();
        const applied = r.int(u32) orelse 0;
        const to = r.int(u64) orelse 0;
        var buf: [queue_ops.MAX_BATCH * queue_ops.Refused.max_size]u8 = undefined;
        table_run.answer(TouchResponse, shard, conn, request_id, .{ .expires_at_ms = nsToMs(to), .applied = applied, .refused = reportAnswer(&r, &buf) });
    }

    // ── After apply ─────────────────────────────────────────────────────

    /// What the leader does once a queue entry applies: answer the dequeue
    /// a deliver was for, wake dequeues for messages newly ready, and chain
    /// a full expire into the next. Every replica runs it; only a leader
    /// holds anything it acts on.
    pub fn afterApply(self: *QueueHandler, shard: *Shard, e: *const Entry, result: *const Result) void {
        const etype: EntryType = @fromBackingInt(@intCast(e.header.entry_type));
        const leads = shard.raft_node.role == .leader;
        var diag: schema.Diagnostic = .{};
        switch (etype) {
            .queue_deliver => if (leads) self.deliverApplied(shard, e, result),
            .queue_settle => if (leads) self.ackApplied(shard, e, result),
            .queue_enqueue => {
                const p = qe.Enqueue.decodeAll(e.payload, &diag) catch return;
                self.checkCaps(shard, p);
                if (leads and result.refused() == null) self.wakeQueue(shard, .of(p.ns), machine.hashOf(p.queue));
            },
            .queue_release => if (leads and result.refused() == null) {
                const p = qe.Release.decodeAll(e.payload, &diag) catch return;
                if (p.delay_ms == 0) self.wakeQueue(shard, .of(p.ns), machine.hashOf(p.queue));
            },
            .queue_expire => {
                if (self.expiring) |i| if (i == e.header.index) {
                    self.expiring = null;
                    if (self.expiring_full and shard.raft_node.leaderReady()) _ = self.expireNext(shard, shard.raft_node.now());
                };
                if (leads) shard.waiter_pool.notifyAny(.queue_dequeue, resolveAny, @ptrCast(shard));
            },
            else => {},
        }
    }

    /// An enqueue applied under caps other than this node's: counted, and
    /// said once per difference. The entry's caps govern, as they must for
    /// every replica to agree.
    fn checkCaps(self: *QueueHandler, shard: *Shard, p: qe.Enqueue.Value) void {
        const theirs: Caps = .{ .max_messages = p.max_messages, .max_bytes = p.max_bytes, .shard_held_bytes = p.shard_held_bytes };
        if (theirs.eql(self.caps)) return;
        if (shard.metrics_registry) |mr| mr.server.recordQueueCapMismatch();
        if (self.mismatch_seen) |seen| if (seen.eql(theirs)) return;
        self.mismatch_seen = theirs;
        log.warn("shard {d}: an enqueue applied under the leader's queue caps (max_messages {d}, max_bytes {d}, shard_held_bytes {d}), not this node's [queues] settings (max_messages {d}, max_bytes {d}, shard_held_bytes {d}); the leader's govern. Set the same caps on every node; a node restarted with changed caps also warns as it applies entries proposed under its old ones.", .{ shard.id, theirs.max_messages, theirs.max_bytes, theirs.shard_held_bytes, self.caps.max_messages, self.caps.max_bytes, self.caps.shard_held_bytes });
    }

    // ── The expire duty, and leadership ─────────────────────────────────

    /// The leader's tick: end leases past their deadline, as one expire of
    /// the earliest keys past due; a key whose lease moved on is dropped by
    /// the same entry. A new leader ends a lease only past a grace of
    /// min(lease, 30 s) after it first ran in its term. Also wakes dequeues
    /// waiting on a queue whose delayed messages came due. Returns how many
    /// keys it named.
    pub fn expireDue(self: *QueueHandler, shard: *Shard) u64 {
        if (shard.raft_node.role == .leader and shard.heldIfHalted(.queue_expire, storeOf(shard).family(machine.Deadlines.ID).count > 0)) {
            self.duty_term = 0;
            return 0;
        }
        if (!shard.raft_node.leaderReady()) return 0;
        shard.duties.note(.queue_expire, null, @import("stdx").time.monotonicMs());
        const now = shard.raft_node.now();
        if (self.duty_term != shard.raft_node.current_term) {
            self.duty_term = shard.raft_node.current_term;
            self.duty_since_ns = now;
        }
        shard.waiter_pool.notifyAny(.queue_dequeue, resolveAny, @ptrCast(shard));
        return self.expireNext(shard, now);
    }

    /// Propose an expire of the leases past their deadline at `now`, up to
    /// a full entry, if none is in flight. Returns how many keys it named.
    fn expireNext(self: *QueueHandler, shard: *Shard, now: u64) u64 {
        if (self.expiring != null) return 0;
        var items: [ae.MAX_EXPIRE_ITEMS * qe.ExpireItem.max_size]u8 = undefined;
        var w: usize = 0;
        var n: u32 = 0;
        var it = machine.pastDue(readOf(shard), now);
        // Leases still in a new leader's grace are passed over, and count
        // toward what one tick reads, so a backlog of them is read in steps.
        var visited: usize = 0;
        while (it.next()) |due| {
            if (n == ae.MAX_EXPIRE_ITEMS or visited == 4 * ae.MAX_EXPIRE_ITEMS) break;
            visited += 1;
            const k = due.key;
            if (k.id.len != qe.ID_LEN) continue;
            if (msgOf(shard, k.ns, k.id[0..8].*, machine.seqOf(k.id))) |m| if (m.status == .leased and m.expiry_ns == k.expiry_ns) {
                const grace = @as(u64, @min(m.lease_ms, MAX_GRACE_MS)) *| std.time.ns_per_ms;
                if (now < self.duty_since_ns +| grace) continue;
            };
            w += (qe.ExpireItem.encodeInto(items[w..], .{ .ns = k.ns.int(), .id = k.id, .expiry_ns = k.expiry_ns }) catch break).len;
            n += 1;
        }
        if (n == 0) return 0;
        const proposed = propose(shard, .queue_expire, qe.Expire.Value{ .items = .{ .bytes = items[0..w], .count = n } }) catch |err| {
            if (!shard.quietHalted(err)) log.warn("shard {d}: queue leases past their deadline not ended: {s}", .{ shard.id, @errorName(err) });
            return 0;
        };
        self.expiring = proposed.index;
        self.expiring_full = n == ae.MAX_EXPIRE_ITEMS;
        return n;
    }

    /// A queue entry a repair skipped: its dequeue is told it didn't run,
    /// and an acked dequeue whose complete was skipped gets its messages as
    /// leased.
    pub fn skipped(self: *QueueHandler, shard: *Shard, e: *const Entry) void {
        // The duty's own entry skipped: free to propose the next.
        if (self.expiring == e.header.index) self.expiring = null;
        for (self.pending.items, 0..) |pd, i| if (pd.index == e.header.index and pd.term == e.header.term) {
            _ = self.pending.swapRemove(i);
            self.unclaim(pd.queue, pd.claims);
            return shard.deliverDeferredRefusal(pd.reply_to, pd.request_id, .unavailable, .{ .reason = .leader_unknown, .ran = .no, .message = "unavailable: a repair skipped this dequeue's entry, so it took nothing — retry" });
        };
        for (self.acking.items, 0..) |a, i| if (a.index == e.header.index and a.term == e.header.term) {
            _ = self.acking.swapRemove(i);
            defer a.free(self.allocator);
            return shard.deliverDeferred(a.reply_to, a.leased);
        };
    }

    /// This node stopped leading: dequeues waiting on a deliver are told,
    /// and the duty starts its grace again under the next term it leads.
    pub fn stoppedLeading(self: *QueueHandler, shard: *Shard) void {
        for (self.pending.items) |pd| {
            shard.deliverDeferredRefusal(pd.reply_to, pd.request_id, .unavailable, .{ .reason = .leader_unknown, .ran = .unknown, .message = "unavailable: this node stopped leading before the messages were given; any it leased come back once their lease runs out — retry" });
        }
        self.pending.clearRetainingCapacity();
        self.claimed.clearRetainingCapacity();
        // Their complete may yet commit under the next leader: answered as
        // leased, so the messages are never lost to the client.
        for (self.acking.items) |a| {
            shard.deliverDeferred(a.reply_to, a.leased);
            a.free(self.allocator);
        }
        self.acking.clearRetainingCapacity();
        self.expiring = null;
        self.duty_term = 0;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Response Serialization — CommandResult → Wire Response
// ═══════════════════════════════════════════════════════════════════════════════

/// Convert a CommandResult to a wire response and queue it on the connection.
fn sendQueueResponse(shard: *Shard, conn: *Connection, request_id: u64, cmd_result: CommandResult) void {
    switch (cmd_result) {
        .ok => shard.sendOkResponse(conn, request_id, ""),
        .err => |e| shard.sendRefusal(conn, request_id, e.code.toStatus(), e.refusal()),
        .queue_enqueued => |q| {
            const seq = std.fmt.parseInt(u64, q.message_id, 10) catch 0;
            var seq_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &seq_buf, seq, .little);
            shard.sendOkResponse(conn, request_id, &seq_buf);
        },
        .queue_messages => |m| shard.sendOkResponse(conn, request_id, m.data),
        .queue_dlq_messages => |m| shard.sendOkResponse(conn, request_id, m.data),
        .queue_purged => |p| {
            var cnt_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &cnt_buf, p.count, .little);
            shard.sendOkResponse(conn, request_id, &cnt_buf);
        },
        else => shard.sendErrorResponse(conn, request_id, .internal_error, "unhandled queue response"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// For tests
// ═══════════════════════════════════════════════════════════════════════════════

/// An enqueue's payload at the default caps, encoded into `buf`.
pub fn enqueuePayload(buf: []u8, ns: NamespaceId, name: []const u8, payload: []const u8) []const u8 {
    return qe.Enqueue.encodeInto(buf, enqueueValue(.{}, ns, name, 0, 0, payload)) catch unreachable;
}

/// Propose an enqueue to the default namespace's queue `name`.
pub fn proposeEnqueue(shard: *Shard, name: []const u8, payload: []const u8) !persistence_mod.ProposeResult {
    return propose(shard, .queue_enqueue, enqueueValue(shard.queue_handler.caps, .default, name, 0, 0, payload));
}

/// A message in the default namespace's queue `name`, put straight into
/// this shard's state as an applied enqueue would, at stamp 1.
pub fn enqueueHere(shard: *Shard, name: []const u8, payload: []const u8) !void {
    var buf: [qe.Enqueue.max_size]u8 = undefined;
    const e = entry_mod.buildEntry(.queue_enqueue, entry_mod.Flags.NONE, 1, 1, 1, enqueuePayload(&buf, .default, name, payload));
    var scratch = std.heap.ArenaAllocator.init(shard.allocator);
    defer scratch.deinit();
    var out = Result.start(&e);
    try @import("../contract/machine.zig").applyEntry(&machine.machine, storeOf(shard), &scratch, &e, &out);
}

test "queue handler: dispatcher registration" {
    var dispatcher = Dispatcher.init();
    QueueHandler.register(&dispatcher);
    inline for (.{ OpCode.queue_enqueue, OpCode.queue_dequeue, OpCode.queue_complete, OpCode.queue_fail, OpCode.queue_touch, OpCode.queue_peek, OpCode.queue_stats, OpCode.queue_dlq_list, OpCode.queue_purge }) |op|
        try std.testing.expect(dispatcher.hasHandler(op));
}

test "queue handler: a dequeue's waiter key keeps what it asked for" {
    const p: QueueHandler.Params = .{ .ns = .of(9), .count = 7, .lease_ms = 60_000, .ack = true, .consumer = "c1" };
    var buf: [64]u8 = undefined;
    const k = p.waiterKey(&buf, machine.hashOf("q")).?;
    const back = QueueHandler.Params.ofWaiterKey(k).?;
    try std.testing.expectEqual(p.ns, back.params.ns);
    try std.testing.expectEqual(p.count, back.params.count);
    try std.testing.expectEqual(p.lease_ms, back.params.lease_ms);
    try std.testing.expect(back.params.ack);
    try std.testing.expectEqualStrings("c1", back.params.consumer);
    try std.testing.expectEqual(machine.hashOf("q"), back.hash);
}
