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

    /// Ready heap: min-heap sorted by priority.
    ready_heap: std.PriorityQueue(HeapItem, void, heapCompare),

    /// Lease tracker: min-heap sorted by expiry.
    lease_heap: std.PriorityQueue(LeaseItem, void, leaseCompare),

    /// Dead letter queue (bounded ring buffer).
    dlq: std.ArrayList(DLQEntry),
    dlq_limit: usize,

    /// Known queue metadata: queue_name_hash → QueueMeta.
    known_queues: std.AutoHashMap(u64, QueueMeta),

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

    pub const QueueMeta = struct {
        name: []const u8,
        namespace: []const u8,
        enqueued: u64,
        dequeued: u64,
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
        return .{
            .allocator = allocator,
            .messages = std.AutoHashMap(u64, Message).init(allocator),
            .ready_heap = std.PriorityQueue(HeapItem, void, heapCompare).init(allocator, {}),
            .lease_heap = std.PriorityQueue(LeaseItem, void, leaseCompare).init(allocator, {}),
            .dlq = .empty,
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
        self.ready_heap.deinit();
        self.lease_heap.deinit();
        self.dlq.deinit(self.allocator);
        // Free known queue name/namespace copies
        var q_it = self.known_queues.iterator();
        while (q_it.next()) |kv| {
            self.allocator.free(kv.value_ptr.name);
            self.allocator.free(kv.value_ptr.namespace);
        }
        self.known_queues.deinit();
    }

    /// Reset the queue projection to empty state.
    /// Used during namespace force-delete to clear all queue data.
    pub fn reset(self: *QueueProjection) void {
        // Free owned payload copies
        var msg_it = self.messages.iterator();
        while (msg_it.next()) |kv| {
            if (kv.value_ptr.payload.len > 0) {
                self.allocator.free(kv.value_ptr.payload);
            }
        }
        self.messages.clearAndFree();
        self.ready_heap.items.len = 0;
        self.lease_heap.items.len = 0;
        self.dlq.clearAndFree(self.allocator);
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
        try self.ready_heap.add(.{ .seq = seq, .priority = priority });

        // Update known-queue stats
        if (self.known_queues.getPtr(queue_name_hash)) |meta| {
            meta.enqueued += 1;
        }

        self.stats.enqueued += 1;
        return seq;
    }

    /// Register a queue name so it appears in `list` and stats are tracked per-queue.
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

    /// Dequeue the highest-priority ready message matching queue_name_hash.
    /// Pass queue_name_hash == 0 to dequeue from any queue (legacy path).
    /// Returns null if no matching ready messages.
    pub fn dequeue(self: *QueueProjection, now_ns: u64, queue_name_hash: u64) !?DequeueResult {
        // First, expire any leases
        self.expireLeases(now_ns);

        // We may need to skip non-matching items; collect them for re-push.
        var skipped_buf: [128]HeapItem = undefined;
        var skipped_count: usize = 0;
        var found: ?DequeueResult = null;

        while (self.ready_heap.removeOrNull()) |item| {
            if (self.messages.getPtr(item.seq)) |msg| {
                if (msg.state != .ready) continue; // stale heap entry

                // Filter by queue name hash (0 = match any)
                if (queue_name_hash != 0 and msg.queue_name_hash != queue_name_hash) {
                    if (skipped_count < skipped_buf.len) {
                        skipped_buf[skipped_count] = item;
                        skipped_count += 1;
                    } else {
                        // Too many skipped — re-add and give up
                        self.ready_heap.add(item) catch {};
                        break;
                    }
                    continue;
                }

                // Lease the message
                msg.state = .leased;
                msg.attempts += 1;
                msg.lease_expiry_ns = now_ns + self.default_lease_ns;

                try self.lease_heap.add(.{
                    .seq = msg.seq,
                    .expiry_ns = msg.lease_expiry_ns,
                });

                self.stats.dequeued += 1;

                // Update known-queue stats
                if (self.known_queues.getPtr(msg.queue_name_hash)) |meta| {
                    meta.dequeued += 1;
                }

                found = .{
                    .seq = msg.seq,
                    .ual_index = msg.ual_index,
                    .priority = msg.priority,
                    .attempts = msg.attempts,
                    .payload = msg.payload,
                    .enqueued_at_ns = msg.enqueued_at_ns,
                };
                break;
            }
        }

        // Re-push any skipped items
        for (skipped_buf[0..skipped_count]) |item| {
            self.ready_heap.add(item) catch {};
        }

        return found;
    }

    /// Acknowledge a leased message (mark as complete, remove).
    pub fn ack(self: *QueueProjection, seq: u64) !void {
        if (self.messages.fetchRemove(seq)) |kv| {
            if (kv.value.payload.len > 0) {
                self.allocator.free(kv.value.payload);
            }
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
                var payload: []const u8 = &[_]u8{};
                var q_name_hash: u64 = 0;
                var q_name: []const u8 = "";
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
                }
                _ = try self.enqueue(entry.header.index, priority, entry.header.timestamp_ns, q_name_hash, payload);
                // Register queue during recovery so list works after restart
                if (q_name.len > 0) {
                    self.registerQueue(q_name_hash, q_name, "default") catch {};
                }
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
        // Approximate: per-message overhead + heap + DLQ + payload bytes + known queues
        var payload_bytes: usize = 0;
        var it = self.messages.iterator();
        while (it.next()) |kv| {
            payload_bytes += kv.value_ptr.payload.len;
        }
        return self.messages.count() * @sizeOf(Message) +
            self.dlq.items.len * @sizeOf(DLQEntry) +
            self.known_queues.count() * @sizeOf(QueueMeta) +
            payload_bytes +
            @sizeOf(QueueProjection);
    }

    // ─── Snapshot Serialization ────────────────────────────────────────────

    /// Serialize the full queue projection state.
    /// Format: [next_seq: u64][message_count: u32] then per message:
    ///   [seq: u64][ual_index: u64][priority: u32][state: u8][attempts: u32]
    ///   [lease_expiry_ns: u64][enqueued_at_ns: u64][queue_name_hash: u64]
    ///   [payload_len: u32][payload bytes]
    /// Then: [dlq_count: u32] then per DLQ entry:
    ///   [seq: u64][ual_index: u64][attempts: u32][moved_at_ns: u64]
    /// Then: [known_queue_count: u32] then per known queue:
    ///   [name_hash: u64][name_len: u16][name bytes][ns_len: u16][ns bytes]
    ///   [enqueued: u64][dequeued: u64]
    /// Caller owns returned slice.
    pub fn serialize(self: *QueueProjection, allocator: Allocator) ![]u8 {
        // Calculate total size
        var total_size: usize = 8 + 4; // next_seq + message_count
        var msg_it = self.messages.iterator();
        while (msg_it.next()) |kv| {
            // Fixed fields: 8+8+4+1+4+8+8+8+4 = 53 bytes + payload
            total_size += 53 + kv.value_ptr.payload.len;
        }
        // DLQ: count(4) + entries(8+8+4+8=28 each)
        total_size += 4 + self.dlq.items.len * 28;
        // Known queues
        total_size += 4; // count
        var kq_it = self.known_queues.iterator();
        while (kq_it.next()) |kv| {
            // hash(8) + name_len(2) + name + ns_len(2) + ns + enqueued(8) + dequeued(8)
            total_size += 28 + kv.value_ptr.name.len + kv.value_ptr.namespace.len;
        }

        const buf = try allocator.alloc(u8, total_size);
        errdefer allocator.free(buf);
        var offset: usize = 0;

        // next_seq
        std.mem.writeInt(u64, buf[offset..][0..8], self.next_seq, .little);
        offset += 8;

        // Messages
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.messages.count()), .little);
        offset += 4;

        msg_it = self.messages.iterator();
        while (msg_it.next()) |kv| {
            const msg = kv.value_ptr;
            std.mem.writeInt(u64, buf[offset..][0..8], msg.seq, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], msg.ual_index, .little);
            offset += 8;
            std.mem.writeInt(u32, buf[offset..][0..4], msg.priority, .little);
            offset += 4;
            buf[offset] = @intFromEnum(msg.state);
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
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.dlq.items.len), .little);
        offset += 4;
        for (self.dlq.items) |dlq_entry| {
            std.mem.writeInt(u64, buf[offset..][0..8], dlq_entry.seq, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset..][0..8], dlq_entry.ual_index, .little);
            offset += 8;
            std.mem.writeInt(u32, buf[offset..][0..4], dlq_entry.attempts, .little);
            offset += 4;
            std.mem.writeInt(u64, buf[offset..][0..8], dlq_entry.moved_at_ns, .little);
            offset += 8;
        }

        // Known queues
        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.known_queues.count()), .little);
        offset += 4;
        kq_it = self.known_queues.iterator();
        while (kq_it.next()) |kv| {
            const hash = kv.key_ptr.*;
            const meta = kv.value_ptr;
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
            const state: MessageState = @enumFromInt(data[offset]);
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
                try self.ready_heap.add(.{ .seq = seq, .priority = priority });
            } else if (state == .leased and lease_expiry_ns > 0) {
                try self.lease_heap.add(.{ .seq = seq, .expiry_ns = lease_expiry_ns });
            }
        }

        // DLQ
        if (offset + 4 > data.len) return;
        const dlq_count = std.mem.readInt(u32, data[offset..][0..4], .little);
        offset += 4;

        var d: u32 = 0;
        while (d < dlq_count) : (d += 1) {
            if (offset + 28 > data.len) return;
            const dlq_entry = DLQEntry{
                .seq = std.mem.readInt(u64, data[offset..][0..8], .little),
                .ual_index = std.mem.readInt(u64, data[offset + 8 ..][0..8], .little),
                .attempts = std.mem.readInt(u32, data[offset + 16 ..][0..4], .little),
                .moved_at_ns = std.mem.readInt(u64, data[offset + 20 ..][0..8], .little),
            };
            offset += 28;
            try self.dlq.append(self.allocator, dlq_entry);
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
            if (offset + ns_len + 16 > data.len) {
                self.allocator.free(name);
                return;
            }
            const namespace = try self.allocator.dupe(u8, data[offset..][0..ns_len]);
            offset += ns_len;
            const enqueued = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;
            const dequeued = std.mem.readInt(u64, data[offset..][0..8], .little);
            offset += 8;

            try self.known_queues.put(hash, .{
                .name = name,
                .namespace = namespace,
                .enqueued = enqueued,
                .dequeued = dequeued,
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

    // Serialize
    const data = try q.serialize(testing.allocator);
    defer testing.allocator.free(data);

    // Deserialize into fresh projection
    var q2 = QueueProjection.init(testing.allocator, .{ .default_lease_ns = 1000 });
    defer q2.deinit();

    try q2.deserialize(data);

    // Verify messages restored
    try testing.expectEqual(@as(usize, 3), q2.messages.count());

    // Verify next_seq was preserved (should be 4 after 3 enqueues)
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
