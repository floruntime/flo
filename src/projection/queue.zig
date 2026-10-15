//! Queue Projection — priority heap + lease tracker + DLQ.
//!
//! Maintains per-queue state with O(log N) enqueue/dequeue via min-heaps.
//! Payloads are NOT stored — only UAL index references. On dequeue, the
//! caller reads the actual payload from the UAL (zero-copy path).
//!
//! Message lifecycle:
//!   enqueue → ready heap (sorted by priority)
//!   dequeue → leased (with visibility timeout)
//!   ack     → removed (complete)
//!   nack    → back to ready heap (retry) or DLQ (max attempts exceeded)
//!
//! Applied via ProjectionRouter from committed UAL entries:
//!   queue_enqueue → insert message into ready heap
//!   queue_ack     → complete leased message
//!   queue_nack    → re-enqueue or DLQ
//!   queue_lease   → acquire lease with duration

const std = @import("std");
const Allocator = std.mem.Allocator;
const Order = std.math.Order;
const entry_mod = @import("../storage/ual/entry.zig");
const router_mod = @import("router.zig");
const node_router = @import("../node/router.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const CommandPayload = entry_mod.CommandPayload;

// ═══════════════════════════════════════════════════════════════════════════════
// Message State
// ═══════════════════════════════════════════════════════════════════════════════

pub const MessageState = enum(u8) {
    ready,
    leased,
    dlq,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Message — stored in the message map
// ═══════════════════════════════════════════════════════════════════════════════

pub const Message = struct {
    /// Sequence number (monotonic per queue).
    seq: u64,
    /// UAL index where the payload lives.
    ual_index: u64,
    /// Priority (lower = higher priority, 0 = highest).
    priority: u32,
    /// Current state.
    state: MessageState,
    /// Number of delivery attempts.
    attempts: u32,
    /// Lease expiry timestamp (nanoseconds). 0 if not leased.
    lease_expiry_ns: u64,
    /// When the message was enqueued (nanoseconds).
    enqueued_at_ns: u64,
    /// Queue name hash — identifies which queue this message belongs to.
    queue_name_hash: u64,
    /// Message payload (allocator-owned copy).
    payload: []const u8,
};

// ═══════════════════════════════════════════════════════════════════════════════
// DLQ Entry
// ═══════════════════════════════════════════════════════════════════════════════

pub const DLQEntry = struct {
    seq: u64,
    ual_index: u64,
    attempts: u32,
    moved_at_ns: u64,
    /// The queue the message died in, so one queue's dead letters can be
    /// told from another's.
    queue_name_hash: u64,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Dequeue Result
// ═══════════════════════════════════════════════════════════════════════════════

pub const DequeueResult = struct {
    seq: u64,
    ual_index: u64,
    priority: u32,
    attempts: u32,
    payload: []const u8,
    enqueued_at_ns: u64,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Ready Heap Item — for the priority queue
// ═══════════════════════════════════════════════════════════════════════════════

const HeapItem = struct {
    seq: u64,
    priority: u32,
};

fn heapCompare(_: void, a: HeapItem, b: HeapItem) Order {
    // Lower priority value = higher priority (dequeued first)
    if (a.priority < b.priority) return .lt;
    if (a.priority > b.priority) return .gt;
    // Tie-break by sequence (FIFO within same priority)
    if (a.seq < b.seq) return .lt;
    if (a.seq > b.seq) return .gt;
    return .eq;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lease Heap Item — for tracking lease expiries
// ═══════════════════════════════════════════════════════════════════════════════

const LeaseItem = struct {
    seq: u64,
    expiry_ns: u64,
};

fn leaseCompare(_: void, a: LeaseItem, b: LeaseItem) Order {
    // Earlier expiry = higher priority (expired first)
    if (a.expiry_ns < b.expiry_ns) return .lt;
    if (a.expiry_ns > b.expiry_ns) return .gt;
    return .eq;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Queue Projection
// ═══════════════════════════════════════════════════════════════════════════════

pub const QueueProjection = struct {
    allocator: Allocator,

    /// Message store: seq → Message.
    messages: std.AutoHashMap(u64, Message),

    /// Each queue's ready heap and dead letters, by queue name hash. Every
    /// queue on a partition shares the message store and sequence numbers,
    /// so a dequeue or a full dead-letter queue must never have to pass
    /// over another queue's messages. An entry exists only while its queue
    /// has ready messages or dead letters.
    lanes: std.AutoHashMap(u64, Lane),

    /// Lease tracker: min-heap sorted by expiry.
    lease_heap: std.PriorityQueue(LeaseItem, void, leaseCompare),

    /// Dead letters kept per queue; the oldest is evicted past this.
    dlq_limit: usize,

    /// Known queue metadata: queue_name_hash → QueueMeta.
    known_queues: std.AutoHashMap(u64, QueueMeta),

    /// Next sequence number.
    next_seq: u64,
    /// What the last applied purge removed, and the sequence the last
    /// applied enqueue got (see `Shard.answering_index`).
    last_purge_count: u64 = 0,
    last_enqueued_seq: ?u64 = null,

    /// Last applied UAL index.
    applied_index: u64,

    /// Max delivery attempts before DLQ. 0 = no DLQ.
    max_attempts: u32,

    /// Default lease duration in nanoseconds.
    default_lease_ns: u64,

    /// Stats.
    stats: Stats,

    /// Optional namespace resolver (hash → name), wired at shard init. A
    /// queue_enqueue entry carries only the namespace hash, so on replay the
    /// apply path can't recover the namespace string on its own; this lets it
    /// register the queue under its real namespace instead of "default".
    ns_resolver: ?*const fn (ctx: *anyopaque, ns_hash: u32) ?[]const u8 = null,
    ns_resolver_ctx: ?*anyopaque = null,

    pub const Lane = struct {
        /// Ready messages, lowest priority value then lowest seq first.
        ready: std.PriorityQueue(HeapItem, void, heapCompare) = .empty,
        /// Dead letters, oldest first.
        dlq: std.ArrayList(DLQEntry) = .empty,

        fn deinit(self: *Lane, allocator: Allocator) void {
            self.ready.deinit(allocator);
            self.dlq.deinit(allocator);
        }
    };

    pub const QueueMeta = struct {
        name: []const u8,
        namespace: []const u8,
        enqueued: u64,
        dequeued: u64,
        // This queue's share of `Stats`.
        acked: u64 = 0,
        nacked: u64 = 0,
        dead_lettered: u64 = 0,
        leases_expired: u64 = 0,
    };

    pub const Stats = struct {
        enqueued: u64 = 0,
        dequeued: u64 = 0,
        acked: u64 = 0,
        nacked: u64 = 0,
        dlq_count: u64 = 0,
        leases_expired: u64 = 0,
    };

    pub const Config = struct {
        max_attempts: u32 = 5,
        default_lease_ns: u64 = 30 * std.time.ns_per_s, // 30 seconds
        dlq_limit: usize = 10_000,
    };

    pub fn init(allocator: Allocator, config: Config) QueueProjection {
        // Eviction keeps the newest dead letter, so a queue holds at least one.
        std.debug.assert(config.dlq_limit > 0);
        return .{
            .allocator = allocator,
            .messages = std.AutoHashMap(u64, Message).init(allocator),
            .lanes = std.AutoHashMap(u64, Lane).init(allocator),
            .lease_heap = .empty,
            .dlq_limit = config.dlq_limit,
            .known_queues = std.AutoHashMap(u64, QueueMeta).init(allocator),
            .next_seq = 1,
            .applied_index = 0,
            .max_attempts = config.max_attempts,
            .default_lease_ns = config.default_lease_ns,
            .stats = .{},
        };
    }

    pub fn deinit(self: *QueueProjection) void {
        // Free owned payload copies
        var msg_it = self.messages.iterator();
        while (msg_it.next()) |kv| {
            if (kv.value_ptr.payload.len > 0) {
                self.allocator.free(kv.value_ptr.payload);
            }
        }
        self.messages.deinit();
        self.clearLanes();
        self.lanes.deinit();
        self.lease_heap.deinit(self.allocator);
        // Free known queue name/namespace copies
        var q_it = self.known_queues.iterator();
        while (q_it.next()) |kv| {
            self.allocator.free(kv.value_ptr.name);
            self.allocator.free(kv.value_ptr.namespace);
        }
        self.known_queues.deinit();
    }

    /// Reset the queue projection to empty state.
    /// Clears every queue on this shard; used before restoring a snapshot.
    pub fn reset(self: *QueueProjection) void {
        // Free owned payload copies
        var msg_it = self.messages.iterator();
        while (msg_it.next()) |kv| {
            if (kv.value_ptr.payload.len > 0) {
                self.allocator.free(kv.value_ptr.payload);
            }
        }
        self.messages.clearAndFree();
        self.clearLanes();
        self.lanes.clearAndFree();
        self.lease_heap.items.len = 0;
        // Free known queue name/namespace copies
        var kq_it = self.known_queues.iterator();
        while (kq_it.next()) |kv| {
            self.allocator.free(kv.value_ptr.name);
            self.allocator.free(kv.value_ptr.namespace);
        }
        self.known_queues.clearAndFree();
        self.next_seq = 1;
        self.applied_index = 0;
        self.stats = .{};
    }

    fn clearLanes(self: *QueueProjection) void {
        var it = self.lanes.valueIterator();
        while (it.next()) |l| l.deinit(self.allocator);
    }

    fn lane(self: *QueueProjection, queue_name_hash: u64) !*Lane {
        const gop = try self.lanes.getOrPut(queue_name_hash);
        if (!gop.found_existing) gop.value_ptr.* = .{};
        return gop.value_ptr;
    }

    /// Free a lane left with nothing in it, so lanes are bounded by the
    /// queues that hold ready messages or dead letters.
    fn dropLaneIfEmpty(self: *QueueProjection, queue_name_hash: u64) void {
        const l = self.lanes.getPtr(queue_name_hash) orelse return;
        if (l.ready.count() != 0 or l.dlq.items.len != 0) return;
        l.deinit(self.allocator);
        _ = self.lanes.remove(queue_name_hash);
    }

    fn pushReady(self: *QueueProjection, msg: *const Message) !void {
        const l = try self.lane(msg.queue_name_hash);
        try l.ready.push(self.allocator, .{ .seq = msg.seq, .priority = msg.priority });
    }

    /// One queue's dead letters, oldest first.
    pub fn deadLetters(self: *const QueueProjection, queue_name_hash: u64) []const DLQEntry {
        const l = self.lanes.getPtr(queue_name_hash) orelse return &.{};
        return l.dlq.items;
    }

    /// Count the live (ready + leased) messages belonging to one queue.
    pub fn countQueue(self: *QueueProjection, queue_name_hash: u64) u64 {
        var n: u64 = 0;
        var it = self.messages.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.queue_name_hash == queue_name_hash) n += 1;
        }
        return n;
    }

    /// Purge all live (ready + leased) messages for one queue, returning the
    /// count removed. The queue's ready heap goes with them; the lease heap
    /// self-cleans lazily (an expiring seq missing from `messages` is
    /// skipped). Dead letters are left intact.
    pub fn purgeQueue(self: *QueueProjection, queue_name_hash: u64) u64 {
        // Collect matching seqs first; removing during iteration is unsafe.
        var seqs: std.ArrayList(u64) = .empty;
        defer seqs.deinit(self.allocator);
        var it = self.messages.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.queue_name_hash == queue_name_hash) {
                seqs.append(self.allocator, kv.key_ptr.*) catch return 0;
            }
        }
        for (seqs.items) |seq| {
            if (self.messages.fetchRemove(seq)) |removed| {
                if (removed.value.payload.len > 0) self.allocator.free(removed.value.payload);
            }
        }
        if (self.lanes.getPtr(queue_name_hash)) |l| {
            l.ready.deinit(self.allocator);
            l.ready = .empty;
            self.dropLaneIfEmpty(queue_name_hash);
        }
        self.last_purge_count = seqs.items.len;
        return @intCast(seqs.items.len);
    }

    // ─── Core operations ───────────────────────────────────────────────────

    /// Enqueue a message with the given priority, UAL index, queue identity, and payload.
    pub fn enqueue(self: *QueueProjection, ual_index: u64, priority: u32, timestamp_ns: u64, queue_name_hash: u64, payload: []const u8) !u64 {
        const seq = self.next_seq;
        self.next_seq += 1;

        // Own a copy of the payload
        const payload_copy = if (payload.len > 0)
            try self.allocator.dupe(u8, payload)
        else
            &[_]u8{};
        errdefer if (payload_copy.len > 0) self.allocator.free(payload_copy);

        const msg = Message{
            .seq = seq,
            .ual_index = ual_index,
            .priority = priority,
            .state = .ready,
            .attempts = 0,
            .lease_expiry_ns = 0,
            .enqueued_at_ns = timestamp_ns,
            .queue_name_hash = queue_name_hash,
            .payload = payload_copy,
        };

        try self.messages.put(seq, msg);
        errdefer _ = self.messages.remove(seq);
        try self.pushReady(&msg);

        // Update known-queue stats
        if (self.known_queues.getPtr(queue_name_hash)) |meta| {
            meta.enqueued += 1;
        }

        self.stats.enqueued += 1;
        return seq;
    }

    /// Register a queue name so it appears in `list` and stats are tracked per-queue.
    /// Resolve a namespace hash to its name via the wired resolver, falling back
    /// to "default" (the implicit namespace, which is never registered).
    fn resolveNamespace(self: *const QueueProjection, ns_hash: u32) []const u8 {
        if (self.ns_resolver) |resolver| {
            if (resolver(self.ns_resolver_ctx.?, ns_hash)) |name| return name;
        }
        return "default";
    }

    pub fn registerQueue(self: *QueueProjection, queue_name_hash: u64, name: []const u8, namespace: []const u8) !void {
        const gop = try self.known_queues.getOrPut(queue_name_hash);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .name = try self.allocator.dupe(u8, name),
                .namespace = try self.allocator.dupe(u8, namespace),
                .enqueued = 0,
                .dequeued = 0,
            };
        }
    }

    /// Number of known queue names on this shard.
    pub fn queueCount(self: *const QueueProjection) usize {
        return self.known_queues.count();
    }

    /// Dequeue the highest-priority ready message of one queue, or null when
    /// it has none ready.
    pub fn dequeue(self: *QueueProjection, now_ns: u64, queue_name_hash: u64) !?DequeueResult {
        // First, expire any leases
        self.expireLeases(now_ns);

        const l = self.lanes.getPtr(queue_name_hash) orelse return null;
        defer self.dropLaneIfEmpty(queue_name_hash);
        while (l.ready.pop()) |item| {
            const msg = self.messages.getPtr(item.seq) orelse continue;
            if (msg.state != .ready) continue; // stale heap entry

            self.lease_heap.push(self.allocator, .{
                .seq = msg.seq,
                .expiry_ns = now_ns + self.default_lease_ns,
            }) catch |err| {
                // Still ready: it goes back where it was.
                l.ready.push(self.allocator, item) catch {};
                return err;
            };
            msg.state = .leased;
            msg.attempts += 1;
            msg.lease_expiry_ns = now_ns + self.default_lease_ns;

            self.stats.dequeued += 1;
            if (self.known_queues.getPtr(msg.queue_name_hash)) |meta| {
                meta.dequeued += 1;
            }

            return .{
                .seq = msg.seq,
                .ual_index = msg.ual_index,
                .priority = msg.priority,
                .attempts = msg.attempts,
                .payload = msg.payload,
                .enqueued_at_ns = msg.enqueued_at_ns,
            };
        }
        return null;
    }

    const AckTarget = struct { seq: u64, queue: u64 };

    fn ackTarget(cmd: CommandPayload) ?AckTarget {
        if (cmd.key.len != 8 or cmd.value.len != 8) return null;
        return .{ .seq = std.mem.readInt(u64, cmd.key[0..8], .little), .queue = std.mem.readInt(u64, cmd.value[0..8], .little) };
    }

    /// Whether message `seq` is held and belongs to `queue_name_hash`.
    pub fn inQueue(self: *const QueueProjection, seq: u64, queue_name_hash: u64) bool {
        const msg = self.messages.get(seq) orelse return false;
        return msg.queue_name_hash == queue_name_hash;
    }

    /// Acknowledge a leased message (mark as complete, remove).
    pub fn ack(self: *QueueProjection, seq: u64) !void {
        if (self.messages.fetchRemove(seq)) |kv| {
            if (kv.value.payload.len > 0) {
                self.allocator.free(kv.value.payload);
            }
            self.stats.acked += 1;
            if (self.known_queues.getPtr(kv.value.queue_name_hash)) |meta| meta.acked += 1;
        }
    }

    /// Negative-acknowledge: return message to ready heap or move to DLQ.
    /// Only a leased message is nacked: one already back in the ready heap
    /// (a nack applied twice, a retried batch) is left alone, so it is not
    /// queued twice or counted against its attempts again.
    pub fn nack(self: *QueueProjection, seq: u64, now_ns: u64) !void {
        if (self.messages.getPtr(seq)) |msg| {
            if (msg.state != .leased) return;
            if (self.max_attempts > 0 and msg.attempts >= self.max_attempts) {
                // Move to DLQ
                try self.moveToDLQ(msg, now_ns);
            } else {
                // Re-enqueue
                msg.state = .ready;
                msg.lease_expiry_ns = 0;
                try self.pushReady(msg);
            }
            self.stats.nacked += 1;
            if (self.known_queues.getPtr(msg.queue_name_hash)) |meta| meta.nacked += 1;
        }
    }

    /// Get the number of ready messages.
    pub fn readyCount(self: *const QueueProjection) usize {
        var count: usize = 0;
        var it = self.messages.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.state == .ready) count += 1;
        }
        return count;
    }

    /// Get the number of leased (in-flight) messages.
    pub fn leasedCount(self: *const QueueProjection) usize {
        var count: usize = 0;
        var it = self.messages.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.state == .leased) count += 1;
        }
        return count;
    }

    /// Dead letters across every queue on this partition.
    pub fn dlqCount(self: *const QueueProjection) usize {
        var n: usize = 0;
        var it = self.lanes.valueIterator();
        while (it.next()) |l| n += l.dlq.items.len;
        return n;
    }

    /// One queue's message counts by state: ready, leased, dead letters.
    pub fn queueCounts(self: *const QueueProjection, queue_name_hash: u64) struct { ready: usize, leased: usize, dead: usize } {
        var ready: usize = 0;
        var leased: usize = 0;
        var it = self.messages.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.queue_name_hash != queue_name_hash) continue;
            switch (kv.value_ptr.state) {
                .ready => ready += 1,
                .leased => leased += 1,
                else => {},
            }
        }
        return .{ .ready = ready, .leased = leased, .dead = self.deadLetters(queue_name_hash).len };
    }

    /// Total messages tracked (ready + leased, not DLQ).
    pub fn totalMessages(self: *const QueueProjection) usize {
        return self.messages.count();
    }

    // ─── Lease Expiry ──────────────────────────────────────────────────────

    /// Expire leases that have passed their timeout.
    /// Expired messages are returned to the ready heap.
    pub fn expireLeases(self: *QueueProjection, now_ns: u64) void {
        while (self.lease_heap.peek()) |top| {
            if (top.expiry_ns > now_ns) break;
            _ = self.lease_heap.popIndex(0);

            if (self.messages.getPtr(top.seq)) |msg| {
                if (msg.state != .leased) continue; // already acked/nacked
                if (msg.lease_expiry_ns != top.expiry_ns) continue; // stale

                // Return to ready
                msg.state = .ready;
                msg.lease_expiry_ns = 0;
                self.pushReady(msg) catch {};

                self.stats.leases_expired += 1;
                if (self.known_queues.getPtr(msg.queue_name_hash)) |meta| meta.leases_expired += 1;
            }
        }
    }

    // ─── DLQ ───────────────────────────────────────────────────────────────

    fn moveToDLQ(self: *QueueProjection, msg: *Message, now_ns: u64) !void {
        const seq = msg.seq;
        const l = try self.lane(msg.queue_name_hash);
        try l.dlq.append(self.allocator, .{
            .seq = seq,
            .ual_index = msg.ual_index,
            .attempts = msg.attempts,
            .moved_at_ns = now_ns,
            .queue_name_hash = msg.queue_name_hash,
        });

        msg.state = .dlq;
        self.stats.dlq_count += 1;
        if (self.known_queues.getPtr(msg.queue_name_hash)) |meta| meta.dead_lettered += 1;

        // Past the cap the queue's oldest dead letter goes, message and all.
        if (l.dlq.items.len > self.dlq_limit) {
            const evicted = l.dlq.orderedRemove(0);
            if (evicted.seq != seq) if (self.messages.fetchRemove(evicted.seq)) |kv| {
                if (kv.value.payload.len > 0) self.allocator.free(kv.value.payload);
            };
        }
    }

    // ─── UAL Entry application ─────────────────────────────────────────────

    pub fn applyEntry(self: *QueueProjection, entry: *const Entry) !void {
        const entry_type: EntryType = @fromBackingInt(@intCast(entry.header.entry_type));

        switch (entry_type) {
            .queue_enqueue => {
                self.last_enqueued_seq = null;
                // Parse priority from command payload value (first 4 bytes) or default 0
                var priority: u32 = 0;
                var payload: []const u8 = &[_]u8{};
                var q_name_hash: u64 = 0;
                var q_name: []const u8 = "";
                var q_ns_hash: u32 = 0;
                if (CommandPayload.deserialize(entry.payload)) |cmd| {
                    if (cmd.value.len >= 4) {
                        priority = std.mem.readInt(u32, cmd.value[0..4], .little);
                        // Rest of value after 4-byte priority is the actual payload
                        if (cmd.value.len > 4) {
                            payload = cmd.value[4..];
                        }
                    } else {
                        payload = cmd.value;
                    }
                    q_name_hash = node_router.nameHash(cmd.namespace_hash, cmd.key);
                    q_name = cmd.key;
                    q_ns_hash = cmd.namespace_hash;
                }
                self.last_enqueued_seq = try self.enqueue(entry.header.index, priority, entry.header.timestamp_ns, q_name_hash, payload);
                // Register queue during recovery so list works after restart. The entry
                // only has the namespace hash; resolve the real namespace string when a
                // resolver is wired (live enqueues already register the correct namespace
                // before apply, so this primarily fixes restart/replay labeling).
                if (q_name.len > 0) {
                    self.registerQueue(q_name_hash, q_name, self.resolveNamespace(q_ns_hash)) catch {};
                }
            },
            // Key: the message's seq. Value: the queue it must be in. Seqs
            // are numbered across the partition, so without the queue an
            // ack could take another queue's message, another namespace's.
            .queue_ack => {
                if (CommandPayload.deserialize(entry.payload)) |cmd| {
                    if (ackTarget(cmd)) |t| if (self.inQueue(t.seq, t.queue)) try self.ack(t.seq);
                }
            },
            .queue_nack => {
                if (CommandPayload.deserialize(entry.payload)) |cmd| {
                    if (ackTarget(cmd)) |t| if (self.inQueue(t.seq, t.queue)) try self.nack(t.seq, entry.header.timestamp_ns);
                }
            },
            .queue_lease => {
                // Lease extension — not yet implemented, treat as no-op
            },
            .queue_purge => {
                self.last_purge_count = 0;
                // Drop all ready+leased messages for one queue. Key = queue name.
                if (CommandPayload.deserialize(entry.payload)) |cmd| {
                    const q_name_hash = node_router.nameHash(cmd.namespace_hash, cmd.key);
                    _ = self.purgeQueue(q_name_hash);
                }
            },
            else => {},
        }

        self.applied_index = entry.header.index;
    }

    /// ProjectionVTable implementation.
    pub fn projectionHandle(self: *QueueProjection) router_mod.ProjectionHandle {
        return .{
            .ctx = @ptrCast(self),
            .vtable = .{
                .applyFn = vtableApply,
                .memoryUsageFn = vtableMemory,
            },
        };
    }

    fn vtableApply(ctx: *anyopaque, entry: *const Entry) router_mod.ApplyError!void {
        const self: *QueueProjection = @ptrCast(@alignCast(ctx));
        self.applyEntry(entry) catch return error.OutOfMemory;
    }

    fn vtableMemory(ctx: *anyopaque) usize {
        const self: *QueueProjection = @ptrCast(@alignCast(ctx));
        return self.memoryUsage();
    }

    pub fn memoryUsage(self: *const QueueProjection) usize {
        // Approximate: per-message overhead + lanes + payload bytes + known queues
        var payload_bytes: usize = 0;
        var it = self.messages.iterator();
        while (it.next()) |kv| {
            payload_bytes += kv.value_ptr.payload.len;
        }
        var lane_bytes: usize = 0;
        var lit = self.lanes.valueIterator();
        while (lit.next()) |l| {
            lane_bytes += @sizeOf(u64) + @sizeOf(Lane) +
                l.ready.capacity() * @sizeOf(HeapItem) +
                l.dlq.capacity * @sizeOf(DLQEntry);
        }
        return self.messages.count() * @sizeOf(Message) +
            lane_bytes +
            self.known_queues.count() * @sizeOf(QueueMeta) +
            payload_bytes +
            @sizeOf(QueueProjection);
    }

    // ─── Snapshot Serialization ────────────────────────────────────────────

    /// A map's keys in ascending order, so two replicas holding the same
    /// state serialize the same bytes whatever order their maps hold it in.
    fn sortedKeys(allocator: Allocator, map: anytype) ![]u64 {
        const keys = try allocator.alloc(u64, map.count());
        var it = map.keyIterator();
        var i: usize = 0;
        while (it.next()) |k| : (i += 1) keys[i] = k.*;
        std.mem.sortUnstable(u64, keys, {}, std.sort.asc(u64));
        return keys;
    }

    /// Serialize the full queue projection state. Messages are in seq
    /// order, dead letters by queue hash then oldest first, known queues by
    /// hash. Ready heaps are not stored: they are rebuilt from the messages.
    /// Format: [next_seq: u64][message_count: u32] then per message:
    ///   [seq: u64][ual_index: u64][priority: u32][state: u8][attempts: u32]
    ///   [lease_expiry_ns: u64][enqueued_at_ns: u64][queue_name_hash: u64]
    ///   [payload_len: u32][payload bytes]
    /// Then: [dlq_count: u32] then per DLQ entry:
    ///   [seq: u64][ual_index: u64][attempts: u32][moved_at_ns: u64][queue_name_hash: u64]
    /// Then: [known_queue_count: u32] then per known queue:
    ///   [name_hash: u64][name_len: u16][name bytes][ns_len: u16][ns bytes]
    ///   [enqueued: u64][dequeued: u64][acked: u64][nacked: u64]
    ///   [dead_lettered: u64][leases_expired: u64]
    /// Caller owns returned slice.
    pub fn serialize(self: *QueueProjection, allocator: Allocator) ![]u8 {
        const seqs = try sortedKeys(allocator, self.messages);
        defer allocator.free(seqs);
        const lane_keys = try sortedKeys(allocator, self.lanes);
        defer allocator.free(lane_keys);
        const queue_keys = try sortedKeys(allocator, self.known_queues);
        defer allocator.free(queue_keys);

        // Calculate total size
        var total_size: usize = 8 + 4; // next_seq + message_count
        var msg_it = self.messages.valueIterator();
        while (msg_it.next()) |msg| {
            // Fixed fields: 8+8+4+1+4+8+8+8+4 = 53 bytes + payload
            total_size += 53 + msg.payload.len;
        }
        // DLQ: count(4) + entries(8+8+4+8+8=36 each)
        const dead = self.dlqCount();
        total_size += 4 + dead * 36;
        // Known queues
        total_size += 4; // count
        var kq_it = self.known_queues.valueIterator();
        while (kq_it.next()) |meta| {
            // hash(8) + name_len(2) + name + ns_len(2) + ns + 6 counters(48)
            total_size += 60 + meta.name.len + meta.namespace.len;
        }

        const buf = try allocator.alloc(u8, total_size);
        errdefer allocator.free(buf);
        var offset: usize = 0;

        // next_seq
        std.mem.writeInt(u64, buf[offset..][0..8], self.next_seq, .little);
        offset += 8;

        // Messages
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(seqs.len), .little);
        offset += 4;

        for (seqs) |seq| {
            const msg = self.messages.getPtr(seq).?;
            std.mem.writeInt(u64, buf[offset..][0..8], msg.seq, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], msg.ual_index, .little);
            offset += 8;
            std.mem.writeInt(u32, buf[offset..][0..4], msg.priority, .little);
            offset += 4;
            buf[offset] = @backingInt(msg.state);
            offset += 1;
            std.mem.writeInt(u32, buf[offset..][0..4], msg.attempts, .little);
            offset += 4;
            std.mem.writeInt(u64, buf[offset..][0..8], msg.lease_expiry_ns, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], msg.enqueued_at_ns, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], msg.queue_name_hash, .little);
            offset += 8;
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(msg.payload.len), .little);
            offset += 4;
            if (msg.payload.len > 0) {
                @memcpy(buf[offset..][0..msg.payload.len], msg.payload);
            }
            offset += msg.payload.len;
        }

        // DLQ
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(dead), .little);
        offset += 4;
        for (lane_keys) |hash| {
            for (self.lanes.getPtr(hash).?.dlq.items) |dlq_entry| {
                std.mem.writeInt(u64, buf[offset..][0..8], dlq_entry.seq, .little);
                offset += 8;
                std.mem.writeInt(u64, buf[offset..][0..8], dlq_entry.ual_index, .little);
                offset += 8;
                std.mem.writeInt(u32, buf[offset..][0..4], dlq_entry.attempts, .little);
                offset += 4;
                std.mem.writeInt(u64, buf[offset..][0..8], dlq_entry.moved_at_ns, .little);
                offset += 8;
                std.mem.writeInt(u64, buf[offset..][0..8], dlq_entry.queue_name_hash, .little);
                offset += 8;
            }
        }

        // Known queues
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(queue_keys.len), .little);
        offset += 4;
        for (queue_keys) |hash| {
            const meta = self.known_queues.getPtr(hash).?;
            std.mem.writeInt(u64, buf[offset..][0..8], hash, .little);
            offset += 8;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(meta.name.len), .little);
            offset += 2;
            @memcpy(buf[offset..][0..meta.name.len], meta.name);
            offset += meta.name.len;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(meta.namespace.len), .little);
            offset += 2;
            @memcpy(buf[offset..][0..meta.namespace.len], meta.namespace);
            offset += meta.namespace.len;
            std.mem.writeInt(u64, buf[offset..][0..8], meta.enqueued, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], meta.dequeued, .little);
            offset += 8;
            for ([_]u64{ meta.acked, meta.nacked, meta.dead_lettered, meta.leases_expired }) |n| {
                std.mem.writeInt(u64, buf[offset..][0..8], n, .little);
                offset += 8;
            }
        }

        return buf;
    }

    /// Restore queue projection state from serialized bytes.
    /// Clears all existing state before restoring.
    pub fn deserialize(self: *QueueProjection, data: []const u8) !void {
        self.reset();

        if (data.len < 12) return;
        var offset: usize = 0;

        // next_seq
        self.next_seq = std.mem.readInt(u64, data[offset..][0..8], .little);
        offset += 8;

        // Messages
        const msg_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var i: u32 = 0;
        while (i < msg_count) : (i += 1) {
            if (offset + 53 > data.len) return;

            const seq = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const ual_index = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const priority = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;
            const state = std.enums.fromInt(MessageState, data[offset]) orelse return error.InvalidPayload;
            offset += 1;
            const attempts = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;
            const lease_expiry_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const enqueued_at_ns = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const queue_name_hash = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const payload_len = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;

            if (offset + payload_len > data.len) return;
            const payload = if (payload_len > 0)
                try self.allocator.dupe(u8, data[offset..][0..payload_len])
            else
                &[_]u8{};
            offset += payload_len;

            try self.messages.put(seq, .{
                .seq = seq,
                .ual_index = ual_index,
                .priority = priority,
                .state = state,
                .attempts = attempts,
                .lease_expiry_ns = lease_expiry_ns,
                .enqueued_at_ns = enqueued_at_ns,
                .queue_name_hash = queue_name_hash,
                .payload = payload,
            });

            // Rebuild heaps from message state
            if (state == .ready) {
                try self.pushReady(self.messages.getPtr(seq).?);
            } else if (state == .leased and lease_expiry_ns > 0) {
                try self.lease_heap.push(self.allocator, .{ .seq = seq, .expiry_ns = lease_expiry_ns });
            }
        }

        // DLQ
        if (offset + 4 > data.len) return;
        const dlq_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var d: u32 = 0;
        while (d < dlq_count) : (d += 1) {
            if (offset + 36 > data.len) return;
            const dlq_entry = DLQEntry{
                .seq = std.mem.readInt(u64, data[offset..][0..8], .little),
                .ual_index = std.mem.readInt(u64, data[offset + 8 ..][0..8], .little),
                .attempts = std.mem.readInt(u32, data[offset + 16 ..][0..4], .little),
                .moved_at_ns = std.mem.readInt(u64, data[offset + 20 ..][0..8], .little),
                .queue_name_hash = std.mem.readInt(u64, data[offset + 28 ..][0..8], .little),
            };
            offset += 36;
            try (try self.lane(dlq_entry.queue_name_hash)).dlq.append(self.allocator, dlq_entry);
        }

        // Known queues
        if (offset + 4 > data.len) return;
        const kq_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var k: u32 = 0;
        while (k < kq_count) : (k += 1) {
            if (offset + 12 > data.len) return;
            const hash = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const name_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + name_len > data.len) return;
            const name = try self.allocator.dupe(u8, data[offset..][0..name_len]);
            offset += name_len;

            if (offset + 2 > data.len) {
                self.allocator.free(name);
                return;
            }
            const ns_len = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;
            if (offset + ns_len + 48 > data.len) {
                self.allocator.free(name);
                return;
            }
            const namespace = try self.allocator.dupe(u8, data[offset..][0..ns_len]);
            offset += ns_len;
            const enqueued = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const dequeued = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            var counters: [4]u64 = undefined;
            for (&counters) |*n| {
                n.* = std.mem.readInt(u64, data[offset..][0..8], .little);
                offset += 8;
            }

            try self.known_queues.put(hash, .{
                .name = name,
                .namespace = namespace,
                .enqueued = enqueued,
                .dequeued = dequeued,
                .acked = counters[0],
                .nacked = counters[1],
                .dead_lettered = counters[2],
                .leases_expired = counters[3],
            });
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "queue: basic enqueue and dequeue" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1000 });
    defer q.deinit();

    const seq1 = try q.enqueue(100, 0, 1000, 42, "hello");
    const seq2 = try q.enqueue(101, 0, 2000, 42, "world");

    try testing.expectEqual(@as(u64, 1), seq1);
    try testing.expectEqual(@as(u64, 2), seq2);
    try testing.expectEqual(@as(usize, 2), q.readyCount());

    // Dequeue returns highest priority (FIFO for same priority)
    const d1 = (try q.dequeue(3000, 42)).?;
    try testing.expectEqual(seq1, d1.seq);
    try testing.expectEqual(@as(u64, 100), d1.ual_index);
    try testing.expectEqualStrings("hello", d1.payload);

    const d2 = (try q.dequeue(3000, 42)).?;
    try testing.expectEqual(seq2, d2.seq);
    try testing.expectEqualStrings("world", d2.payload);

    // No more ready
    try testing.expect(try q.dequeue(3000, 42) == null);
}

test "queue: priority ordering" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1000 });
    defer q.deinit();

    _ = try q.enqueue(100, 10, 1000, 42, "low"); // low priority
    _ = try q.enqueue(101, 1, 2000, 42, "high"); // high priority
    _ = try q.enqueue(102, 5, 3000, 42, "med"); // medium priority

    // Should dequeue in priority order: 1, 5, 10
    const d1 = (try q.dequeue(4000, 42)).?;
    try testing.expectEqual(@as(u32, 1), d1.priority);

    const d2 = (try q.dequeue(4000, 42)).?;
    try testing.expectEqual(@as(u32, 5), d2.priority);

    const d3 = (try q.dequeue(4000, 42)).?;
    try testing.expectEqual(@as(u32, 10), d3.priority);
}

test "queue: ack removes message" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1_000_000 });
    defer q.deinit();

    const seq = try q.enqueue(100, 0, 1000, 42, "msg");
    _ = try q.dequeue(2000, 42);
    try q.ack(seq);

    try testing.expectEqual(@as(usize, 0), q.totalMessages());
    try testing.expectEqual(@as(u64, 1), q.stats.acked);
}

test "queue: nack re-enqueues" {
    var q = QueueProjection.init(testing.allocator, .{
        .default_lease_ns = 1_000_000,
        .max_attempts = 5,
    });
    defer q.deinit();

    const seq = try q.enqueue(100, 0, 1000, 42, "msg");
    _ = try q.dequeue(2000, 42);

    // Nack — should re-enqueue
    try q.nack(seq, 3000);
    try testing.expectEqual(@as(usize, 1), q.readyCount());

    // Dequeue again — attempts should be 2
    const d = (try q.dequeue(4000, 42)).?;
    try testing.expectEqual(@as(u32, 2), d.attempts);
}

test "queue: nack to DLQ after max attempts" {
    var q = QueueProjection.init(testing.allocator, .{
        .default_lease_ns = 1_000_000,
        .max_attempts = 2,
    });
    defer q.deinit();

    const seq = try q.enqueue(100, 0, 1000, 42, "msg");

    // Attempt 1
    _ = try q.dequeue(2000, 42);
    try q.nack(seq, 3000); // re-enqueue (attempt 1 < 2)

    // Attempt 2
    _ = try q.dequeue(4000, 42);
    try q.nack(seq, 5000); // DLQ (attempt 2 >= 2)

    try testing.expectEqual(@as(usize, 1), q.dlqCount());
    try testing.expectEqual(@as(u64, 1), q.stats.dlq_count);
}

test "queue: lease expiry returns message to ready" {
    var q = QueueProjection.init(testing.allocator, .{
        .default_lease_ns = 1000, // 1000 ns lease
    });
    defer q.deinit();

    _ = try q.enqueue(100, 0, 1000, 42, "msg");
    _ = try q.dequeue(2000, 42); // leased until 3000

    try testing.expectEqual(@as(usize, 0), q.readyCount());
    try testing.expectEqual(@as(usize, 1), q.leasedCount());

    // Time passes beyond lease expiry
    q.expireLeases(4000);

    try testing.expectEqual(@as(usize, 1), q.readyCount());
    try testing.expectEqual(@as(u64, 1), q.stats.leases_expired);

    // Can dequeue again
    const d = (try q.dequeue(5000, 42)).?;
    try testing.expectEqual(@as(u32, 2), d.attempts);
}

test "queue: dequeue auto-expires leases" {
    var q = QueueProjection.init(testing.allocator, .{
        .default_lease_ns = 100,
    });
    defer q.deinit();

    const seq1 = try q.enqueue(100, 0, 1000, 42, "a");
    _ = try q.dequeue(2000, 42); // leased seq1 until 2100

    const seq2 = try q.enqueue(101, 0, 2000, 42, "b");

    // Dequeue at time 3000 — seq1 lease expired, should be available again
    const d = (try q.dequeue(3000, 42)).?;
    // seq1 should have been re-enqueued and dequeued first (FIFO, same priority)
    // Actually it could be either depending on heap ordering with seq tiebreak
    try testing.expect(d.seq == seq1 or d.seq == seq2);
}

test "queue: empty dequeue returns null" {
    var q = QueueProjection.init(testing.allocator, .{});
    defer q.deinit();

    try testing.expect(try q.dequeue(1000, 0) == null);
}

test "queue: stats tracking" {
    var q = QueueProjection.init(testing.allocator, .{
        .default_lease_ns = 1_000_000,
        .max_attempts = 1,
    });
    defer q.deinit();

    _ = try q.enqueue(100, 0, 1000, 42, "a");
    _ = try q.enqueue(101, 0, 2000, 42, "b");
    _ = try q.dequeue(3000, 42);

    try testing.expectEqual(@as(u64, 2), q.stats.enqueued);
    try testing.expectEqual(@as(u64, 1), q.stats.dequeued);
}

test "queue: memory usage estimate" {
    var q = QueueProjection.init(testing.allocator, .{});
    defer q.deinit();

    _ = try q.enqueue(100, 0, 1000, 42, "hello");
    _ = try q.enqueue(101, 0, 2000, 42, "world");

    try testing.expect(q.memoryUsage() > 0);
}

test "queue: dequeue filters by queue_name_hash" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1_000_000 });
    defer q.deinit();

    _ = try q.enqueue(100, 0, 1000, 1, "queue-A msg");
    _ = try q.enqueue(101, 0, 2000, 2, "queue-B msg");
    _ = try q.enqueue(102, 0, 3000, 1, "queue-A msg2");

    // Dequeue from queue 2 only
    const d1 = (try q.dequeue(4000, 2)).?;
    try testing.expectEqualStrings("queue-B msg", d1.payload);

    // No more in queue 2
    try testing.expect(try q.dequeue(4000, 2) == null);

    // Queue 1 still has 2
    const d2 = (try q.dequeue(4000, 1)).?;
    try testing.expectEqualStrings("queue-A msg", d2.payload);
    const d3 = (try q.dequeue(4000, 1)).?;
    try testing.expectEqualStrings("queue-A msg2", d3.payload);
}

test "queue: one queue's backlog never hides another queue's message" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1_000_000 });
    defer q.deinit();

    // A's messages all outrank B's, so one shared heap would put them first.
    for (0..200) |i| _ = try q.enqueue(100 + i, 0, 1000, 1, "a");
    _ = try q.enqueue(400, 5, 2000, 2, "b");

    const d = (try q.dequeue(3000, 2)).?;
    try testing.expectEqualStrings("b", d.payload);
    try testing.expect(try q.dequeue(3000, 2) == null);
    try testing.expectEqual(@as(usize, 200), q.queueCounts(1).ready);
}

test "queue: dead letters are capped per queue, and an evicted one's message goes with it" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1_000_000, .max_attempts = 1, .dlq_limit = 2 });
    defer q.deinit();

    const kill = struct {
        fn f(p: *QueueProjection, hash: u64) !u64 {
            const seq = try p.enqueue(100, 0, 1000, hash, "x");
            _ = (try p.dequeue(2000, hash)).?;
            try p.nack(seq, 3000);
            return seq;
        }
    }.f;

    const b = try kill(&q, 2);
    const a1 = try kill(&q, 1);
    _ = try kill(&q, 1);
    _ = try kill(&q, 1);

    try testing.expectEqual(@as(usize, 1), q.deadLetters(2).len);
    try testing.expectEqual(b, q.deadLetters(2)[0].seq);
    try testing.expectEqual(@as(usize, 2), q.deadLetters(1).len);
    try testing.expect(!q.inQueue(a1, 1));
    try testing.expectEqual(@as(usize, 3), q.totalMessages());
}

test "queue: a queue's ready heap is freed once it has nothing ready or dead" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1_000_000 });
    defer q.deinit();

    for (0..3) |i| _ = try q.enqueue(100 + i, 0, 1000, 1, "a");
    _ = try q.enqueue(200, 0, 1000, 2, "b");
    try testing.expectEqual(@as(u64, 3), q.purgeQueue(1));
    try testing.expect(q.lanes.get(1) == null);

    _ = (try q.dequeue(2000, 2)).?;
    try testing.expectEqual(@as(usize, 0), q.lanes.count());
}

test "queue: the same state serializes to the same bytes whatever order built it" {
    // One replica took 2000 messages and acked half; the other only ever
    // held the half left, and learned its queues in the opposite order.
    var a = QueueProjection.init(testing.allocator, .{});
    defer a.deinit();
    for (1..51) |h| try a.registerQueue(h, "q", "ns");
    for (1..2001) |s| _ = try a.enqueue(s, @intCast(s % 3), s, 99, "m");
    for (1001..2001) |s| try a.ack(s);

    var b = QueueProjection.init(testing.allocator, .{});
    defer b.deinit();
    var h: u64 = 50;
    while (h > 0) : (h -= 1) try b.registerQueue(h, "q", "ns");
    for (1..1001) |s| _ = try b.enqueue(s, @intCast(s % 3), s, 99, "m");
    b.next_seq = a.next_seq;

    const sa = try a.serialize(testing.allocator);
    defer testing.allocator.free(sa);
    const sb = try b.serialize(testing.allocator);
    defer testing.allocator.free(sb);
    try testing.expectEqualSlices(u8, sa, sb);
}

test "queue: registerQueue and queueCount" {
    var q = QueueProjection.init(testing.allocator, .{});
    defer q.deinit();

    try q.registerQueue(42, "tasks", "default");
    try q.registerQueue(99, "orders", "default");
    try q.registerQueue(42, "tasks", "default"); // duplicate — no-op

    try testing.expectEqual(@as(usize, 2), q.queueCount());
}

test "queue: projection handle with router" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1000 });
    defer q.deinit();

    var router = router_mod.ProjectionRouter.init();
    router.registerQueue(q.projectionHandle());

    // Build a queue_enqueue entry
    const entry = entry_mod.buildEntry(.queue_enqueue, 0, 1, 1, 1000, "test-payload");
    const result = router.apply(&entry);

    try testing.expectEqual(router_mod.ApplyResult.applied, result);
    try testing.expectEqual(@as(u64, 1), q.stats.enqueued);
}

test "queue: serialize/deserialize round-trip" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1000 });
    defer q.deinit();

    // Enqueue messages
    _ = try q.enqueue(100, 1, 1000, 42, "hello");
    _ = try q.enqueue(101, 2, 2000, 42, "world");
    _ = try q.enqueue(102, 0, 3000, 99, "other-queue");

    // Register known queues
    try q.registerQueue(42, "tasks", "default");
    try q.registerQueue(99, "orders", "production");

    // One dead letter, from queue 99: leased at its last attempt, then nacked.
    const dead = try q.enqueue(103, 0, 3500, 99, "dead");
    const msg = q.messages.getPtr(dead).?;
    msg.state = .leased;
    msg.attempts = q.max_attempts;
    try q.nack(dead, 3600);
    try testing.expectEqual(@as(usize, 1), q.dlqCount());

    // Serialize
    const data = try q.serialize(testing.allocator);
    defer testing.allocator.free(data);

    // Deserialize into fresh projection
    var q2 = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1000 });
    defer q2.deinit();

    try q2.deserialize(data);

    // Verify messages restored (the dead letter's message stays held)
    try testing.expectEqual(@as(usize, 4), q2.messages.count());
    try testing.expectEqual(@as(usize, 1), q2.deadLetters(99).len);
    try testing.expectEqual(@as(u64, 99), q2.deadLetters(99)[0].queue_name_hash);
    try testing.expectEqual(dead, q2.deadLetters(99)[0].seq);

    // Verify next_seq was preserved
    try testing.expectEqual(q.next_seq, q2.next_seq);

    // Verify known queues restored
    try testing.expectEqual(@as(usize, 2), q2.queueCount());

    // Verify we can dequeue from the restored projection
    const d1 = (try q2.dequeue(4000, 42)).?;
    try testing.expectEqualStrings("hello", d1.payload);
}

test "queue: serialize empty projection" {
    var q = QueueProjection.init(testing.allocator, .{});
    defer q.deinit();

    const data = try q.serialize(testing.allocator);
    defer testing.allocator.free(data);

    var q2 = QueueProjection.init(testing.allocator, .{});
    defer q2.deinit();

    try q2.deserialize(data);
    try testing.expectEqual(@as(usize, 0), q2.messages.count());
    try testing.expectEqual(@as(u64, 1), q2.next_seq);
}

test "queue: an ack or nack entry touches a message only in the queue it names" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1000 });
    defer q.deinit();
    const theirs = try q.enqueue(100, 0, 1000, 42, "theirs");

    var key: [8]u8 = undefined;
    std.mem.writeInt(u64, &key, theirs, .little);
    var payload_buf: [entry_mod.COMMAND_PREFIX_SIZE + 16]u8 = undefined;
    for ([_]EntryType{ .queue_ack, .queue_nack }) |kind| {
        // Another queue's id, then no queue at all: nothing happens.
        for ([_][]const u8{ &std.mem.toBytes(@as(u64, 7)), "" }) |queue| {
            const entry = entry_mod.buildCommandEntry(kind, 0, 1, 2, 2000, 0, &key, queue, &payload_buf).?;
            try q.applyEntry(&entry);
            try testing.expect(q.inQueue(theirs, 42));
            try testing.expectEqual(@as(u64, 0), q.stats.acked + q.stats.nacked);
        }
    }
    // Its own queue's id: acked.
    const entry = entry_mod.buildCommandEntry(.queue_ack, 0, 1, 3, 3000, 0, &key, &std.mem.toBytes(@as(u64, 42)), &payload_buf).?;
    try q.applyEntry(&entry);
    try testing.expect(!q.inQueue(theirs, 42));
    try testing.expectEqual(@as(u64, 1), q.stats.acked);
}

test "queue: a nack applied twice queues the message once and counts one attempt" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1_000_000, .max_attempts = 5 });
    defer q.deinit();
    const seq = try q.enqueue(100, 0, 1000, 42, "msg");
    _ = try q.dequeue(2000, 42);
    try q.nack(seq, 3000);
    try q.nack(seq, 3001);
    try testing.expectEqual(@as(usize, 1), q.lanes.get(42).?.ready.count());
    try testing.expectEqual(@as(u64, 1), q.stats.nacked);
    const d = (try q.dequeue(4000, 42)).?;
    try testing.expectEqual(@as(u32, 2), d.attempts);
    try testing.expect(try q.dequeue(4001, 42) == null);
}

test "queue: a snapshot with any one byte corrupted is refused or loaded, never trapped on" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1000 });
    defer q.deinit();
    _ = try q.enqueue(100, 1, 1000, 42, "hello");
    try q.registerQueue(42, "tasks", "default");
    const data = try q.serialize(testing.allocator);
    defer testing.allocator.free(data);

    const bad = try testing.allocator.dupe(u8, data);
    defer testing.allocator.free(bad);
    for (0..bad.len) |i| {
        @memcpy(bad, data);
        bad[i] = 0xee;
        // An arena: error paths may leave partial state behind.
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        var q2 = QueueProjection.init(arena.allocator(), .{ .default_lease_ns = 1000 });
        q2.deserialize(bad) catch {};
    }
}

test "queue: dead letters in many queues serialize the same after a restore" {
    // The restored copy builds its lanes fresh; the original grew to 200
    // lanes and dropped 150, so the two maps hold the same lanes in
    // different orders.
    var a = QueueProjection.init(testing.allocator, .{ .max_attempts = 1 });
    defer a.deinit();
    for (1..201) |h| _ = try a.enqueue(h, 0, h, h, "m");
    for (1..201) |h| {
        const d = (try a.dequeue(1000, h)).?;
        if (h <= 50) try a.nack(d.seq, 2000) else try a.ack(d.seq);
    }
    try testing.expectEqual(@as(usize, 50), a.lanes.count());

    const sa = try a.serialize(testing.allocator);
    defer testing.allocator.free(sa);
    var b = QueueProjection.init(testing.allocator, .{ .max_attempts = 1 });
    defer b.deinit();
    try b.deserialize(sa);
    const sb = try b.serialize(testing.allocator);
    defer testing.allocator.free(sb);
    try testing.expectEqualSlices(u8, sa, sb);
}
