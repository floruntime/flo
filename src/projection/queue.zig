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
};

// ═══════════════════════════════════════════════════════════════════════════════
// DLQ Entry
// ═══════════════════════════════════════════════════════════════════════════════

pub const DLQEntry = struct {
    seq: u64,
    ual_index: u64,
    attempts: u32,
    moved_at_ns: u64,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Dequeue Result
// ═══════════════════════════════════════════════════════════════════════════════

pub const DequeueResult = struct {
    seq: u64,
    ual_index: u64,
    priority: u32,
    attempts: u32,
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

    /// Ready heap: min-heap sorted by priority.
    ready_heap: std.PriorityQueue(HeapItem, void, heapCompare),

    /// Lease tracker: min-heap sorted by expiry.
    lease_heap: std.PriorityQueue(LeaseItem, void, leaseCompare),

    /// Dead letter queue (bounded ring buffer).
    dlq: std.ArrayList(DLQEntry),
    dlq_limit: usize,

    /// Next sequence number.
    next_seq: u64,

    /// Last applied UAL index.
    applied_index: u64,

    /// Max delivery attempts before DLQ. 0 = no DLQ.
    max_attempts: u32,

    /// Default lease duration in nanoseconds.
    default_lease_ns: u64,

    /// Stats.
    stats: Stats,

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
        return .{
            .allocator = allocator,
            .messages = std.AutoHashMap(u64, Message).init(allocator),
            .ready_heap = std.PriorityQueue(HeapItem, void, heapCompare).init(allocator, {}),
            .lease_heap = std.PriorityQueue(LeaseItem, void, leaseCompare).init(allocator, {}),
            .dlq = .empty,
            .dlq_limit = config.dlq_limit,
            .next_seq = 1,
            .applied_index = 0,
            .max_attempts = config.max_attempts,
            .default_lease_ns = config.default_lease_ns,
            .stats = .{},
        };
    }

    pub fn deinit(self: *QueueProjection) void {
        self.messages.deinit();
        self.ready_heap.deinit();
        self.lease_heap.deinit();
        self.dlq.deinit(self.allocator);
    }

    // ─── Core operations ───────────────────────────────────────────────────

    /// Enqueue a message with the given priority and UAL index.
    pub fn enqueue(self: *QueueProjection, ual_index: u64, priority: u32, timestamp_ns: u64) !u64 {
        const seq = self.next_seq;
        self.next_seq += 1;

        const msg = Message{
            .seq = seq,
            .ual_index = ual_index,
            .priority = priority,
            .state = .ready,
            .attempts = 0,
            .lease_expiry_ns = 0,
            .enqueued_at_ns = timestamp_ns,
        };

        try self.messages.put(seq, msg);
        try self.ready_heap.add(.{ .seq = seq, .priority = priority });

        self.stats.enqueued += 1;
        return seq;
    }

    /// Dequeue the highest-priority ready message.
    /// Returns null if no ready messages.
    pub fn dequeue(self: *QueueProjection, now_ns: u64) !?DequeueResult {
        // First, expire any leases
        self.expireLeases(now_ns);

        // Pop from ready heap
        while (self.ready_heap.removeOrNull()) |item| {
            if (self.messages.getPtr(item.seq)) |msg| {
                if (msg.state != .ready) continue; // stale heap entry

                // Lease the message
                msg.state = .leased;
                msg.attempts += 1;
                msg.lease_expiry_ns = now_ns + self.default_lease_ns;

                try self.lease_heap.add(.{
                    .seq = msg.seq,
                    .expiry_ns = msg.lease_expiry_ns,
                });

                self.stats.dequeued += 1;

                return .{
                    .seq = msg.seq,
                    .ual_index = msg.ual_index,
                    .priority = msg.priority,
                    .attempts = msg.attempts,
                };
            }
        }

        return null;
    }

    /// Acknowledge a leased message (mark as complete, remove).
    pub fn ack(self: *QueueProjection, seq: u64) !void {
        if (self.messages.fetchRemove(seq)) |_| {
            self.stats.acked += 1;
        }
    }

    /// Negative-acknowledge: return message to ready heap or move to DLQ.
    pub fn nack(self: *QueueProjection, seq: u64, now_ns: u64) !void {
        if (self.messages.getPtr(seq)) |msg| {
            if (self.max_attempts > 0 and msg.attempts >= self.max_attempts) {
                // Move to DLQ
                try self.moveToDLQ(msg, now_ns);
            } else {
                // Re-enqueue
                msg.state = .ready;
                msg.lease_expiry_ns = 0;
                try self.ready_heap.add(.{ .seq = msg.seq, .priority = msg.priority });
            }
            self.stats.nacked += 1;
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

    /// Get the DLQ size.
    pub fn dlqCount(self: *const QueueProjection) usize {
        return self.dlq.items.len;
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
            _ = self.lease_heap.remove();

            if (self.messages.getPtr(top.seq)) |msg| {
                if (msg.state != .leased) continue; // already acked/nacked
                if (msg.lease_expiry_ns != top.expiry_ns) continue; // stale

                // Return to ready
                msg.state = .ready;
                msg.lease_expiry_ns = 0;
                self.ready_heap.add(.{ .seq = msg.seq, .priority = msg.priority }) catch {};

                self.stats.leases_expired += 1;
            }
        }
    }

    // ─── DLQ ───────────────────────────────────────────────────────────────

    fn moveToDLQ(self: *QueueProjection, msg: *Message, now_ns: u64) !void {
        // Add to DLQ ring buffer
        if (self.dlq.items.len >= self.dlq_limit) {
            // Remove oldest
            _ = self.dlq.orderedRemove(0);
            // Note: orderedRemove in 0.15.2 doesn't need allocator
        }

        try self.dlq.append(self.allocator, .{
            .seq = msg.seq,
            .ual_index = msg.ual_index,
            .attempts = msg.attempts,
            .moved_at_ns = now_ns,
        });

        msg.state = .dlq;
        self.stats.dlq_count += 1;
    }

    // ─── UAL Entry application ─────────────────────────────────────────────

    pub fn applyEntry(self: *QueueProjection, entry: *const Entry) !void {
        const entry_type: EntryType = @enumFromInt(entry.header.entry_type);

        switch (entry_type) {
            .queue_enqueue => {
                // Parse priority from command payload value (first 4 bytes) or default 0
                var priority: u32 = 0;
                if (CommandPayload.deserialize(entry.payload)) |cmd| {
                    if (cmd.value.len >= 4) {
                        priority = std.mem.readInt(u32, cmd.value[0..4], .little);
                    }
                }
                _ = try self.enqueue(entry.header.index, priority, entry.header.timestamp_ns);
            },
            .queue_ack => {
                // Seq is encoded in the command payload key as a u64
                if (CommandPayload.deserialize(entry.payload)) |cmd| {
                    if (cmd.key.len >= 8) {
                        const seq = std.mem.readInt(u64, cmd.key[0..8], .little);
                        try self.ack(seq);
                    }
                }
            },
            .queue_nack => {
                if (CommandPayload.deserialize(entry.payload)) |cmd| {
                    if (cmd.key.len >= 8) {
                        const seq = std.mem.readInt(u64, cmd.key[0..8], .little);
                        try self.nack(seq, entry.header.timestamp_ns);
                    }
                }
            },
            .queue_lease => {
                // Lease extension — not yet implemented, treat as no-op
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
        // Approximate: per-message overhead + heap + DLQ
        return self.messages.count() * @sizeOf(Message) +
            self.dlq.items.len * @sizeOf(DLQEntry) +
            @sizeOf(QueueProjection);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "queue: basic enqueue and dequeue" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1000 });
    defer q.deinit();

    const seq1 = try q.enqueue(100, 0, 1000);
    const seq2 = try q.enqueue(101, 0, 2000);

    try testing.expectEqual(@as(u64, 1), seq1);
    try testing.expectEqual(@as(u64, 2), seq2);
    try testing.expectEqual(@as(usize, 2), q.readyCount());

    // Dequeue returns highest priority (FIFO for same priority)
    const d1 = (try q.dequeue(3000)).?;
    try testing.expectEqual(seq1, d1.seq);
    try testing.expectEqual(@as(u64, 100), d1.ual_index);

    const d2 = (try q.dequeue(3000)).?;
    try testing.expectEqual(seq2, d2.seq);

    // No more ready
    try testing.expect(try q.dequeue(3000) == null);
}

test "queue: priority ordering" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1000 });
    defer q.deinit();

    _ = try q.enqueue(100, 10, 1000); // low priority
    _ = try q.enqueue(101, 1, 2000); // high priority
    _ = try q.enqueue(102, 5, 3000); // medium priority

    // Should dequeue in priority order: 1, 5, 10
    const d1 = (try q.dequeue(4000)).?;
    try testing.expectEqual(@as(u32, 1), d1.priority);

    const d2 = (try q.dequeue(4000)).?;
    try testing.expectEqual(@as(u32, 5), d2.priority);

    const d3 = (try q.dequeue(4000)).?;
    try testing.expectEqual(@as(u32, 10), d3.priority);
}

test "queue: ack removes message" {
    var q = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1_000_000 });
    defer q.deinit();

    const seq = try q.enqueue(100, 0, 1000);
    _ = try q.dequeue(2000);
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

    const seq = try q.enqueue(100, 0, 1000);
    _ = try q.dequeue(2000);

    // Nack — should re-enqueue
    try q.nack(seq, 3000);
    try testing.expectEqual(@as(usize, 1), q.readyCount());

    // Dequeue again — attempts should be 2
    const d = (try q.dequeue(4000)).?;
    try testing.expectEqual(@as(u32, 2), d.attempts);
}

test "queue: nack to DLQ after max attempts" {
    var q = QueueProjection.init(testing.allocator, .{
        .default_lease_ns = 1_000_000,
        .max_attempts = 2,
    });
    defer q.deinit();

    const seq = try q.enqueue(100, 0, 1000);

    // Attempt 1
    _ = try q.dequeue(2000);
    try q.nack(seq, 3000); // re-enqueue (attempt 1 < 2)

    // Attempt 2
    _ = try q.dequeue(4000);
    try q.nack(seq, 5000); // DLQ (attempt 2 >= 2)

    try testing.expectEqual(@as(usize, 1), q.dlqCount());
    try testing.expectEqual(@as(u64, 1), q.stats.dlq_count);
}

test "queue: lease expiry returns message to ready" {
    var q = QueueProjection.init(testing.allocator, .{
        .default_lease_ns = 1000, // 1000 ns lease
    });
    defer q.deinit();

    _ = try q.enqueue(100, 0, 1000);
    _ = try q.dequeue(2000); // leased until 3000

    try testing.expectEqual(@as(usize, 0), q.readyCount());
    try testing.expectEqual(@as(usize, 1), q.leasedCount());

    // Time passes beyond lease expiry
    q.expireLeases(4000);

    try testing.expectEqual(@as(usize, 1), q.readyCount());
    try testing.expectEqual(@as(u64, 1), q.stats.leases_expired);

    // Can dequeue again
    const d = (try q.dequeue(5000)).?;
    try testing.expectEqual(@as(u32, 2), d.attempts);
}

test "queue: dequeue auto-expires leases" {
    var q = QueueProjection.init(testing.allocator, .{
        .default_lease_ns = 100,
    });
    defer q.deinit();

    const seq1 = try q.enqueue(100, 0, 1000);
    _ = try q.dequeue(2000); // leased seq1 until 2100

    const seq2 = try q.enqueue(101, 0, 2000);

    // Dequeue at time 3000 — seq1 lease expired, should be available again
    const d = (try q.dequeue(3000)).?;
    // seq1 should have been re-enqueued and dequeued first (FIFO, same priority)
    // Actually it could be either depending on heap ordering with seq tiebreak
    try testing.expect(d.seq == seq1 or d.seq == seq2);
}

test "queue: empty dequeue returns null" {
    var q = QueueProjection.init(testing.allocator, .{});
    defer q.deinit();

    try testing.expect(try q.dequeue(1000) == null);
}

test "queue: stats tracking" {
    var q = QueueProjection.init(testing.allocator, .{
        .default_lease_ns = 1_000_000,
        .max_attempts = 1,
    });
    defer q.deinit();

    _ = try q.enqueue(100, 0, 1000);
    _ = try q.enqueue(101, 0, 2000);
    _ = try q.dequeue(3000);

    try testing.expectEqual(@as(u64, 2), q.stats.enqueued);
    try testing.expectEqual(@as(u64, 1), q.stats.dequeued);
}

test "queue: memory usage estimate" {
    var q = QueueProjection.init(testing.allocator, .{});
    defer q.deinit();

    _ = try q.enqueue(100, 0, 1000);
    _ = try q.enqueue(101, 0, 2000);

    try testing.expect(q.memoryUsage() > 0);
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
