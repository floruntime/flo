//! Inbox — MPSC lock-free ring buffer for cross-shard communication
//!
//! Each shard has exactly one Inbox. Any shard can push envelopes; only
//! the owning shard drains them. This is the sole mechanism for cross-shard
//! mutable communication — no shared state otherwise.
//!
//! ## Design
//!
//! - 32-byte `Message` envelopes, compact and cache-friendly
//! - Lock-free MPSC ring: multiple producers (any shard), single consumer (owning shard)
//! - Power-of-2 capacity for fast modulo via bitmask
//! - Producers CAS-advance `write_head`; consumer reads up to `commit_tail`
//! - Non-blocking `send()` returns false if ring is full (backpressure)
//! - `drain()` batch-receives up to N messages for amortised processing
//!
//! ## Envelope Layout (32 bytes, extern struct)
//!
//! ```
//! Offset  Field          Type           Description
//! ──────  ─────          ────           ───────────
//!  0      tag            Tag (u8)       message type
//!  1      src_shard      u8             who sent it
//!  2      partition_id   u16            target partition
//!  4      payload_len    u32            payload size for deallocation
//!  8      sequence       u64            for response matching
//! 16      payload_ptr    ?*anyopaque    slab-allocated payload (ownership transfers)
//! 24      _padding       [8]u8          reserved / cache-line alignment
//! ```
//!
//! The receiver shard frees the payload after processing.

const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Value;

/// Message tags for cross-shard communication
pub const Tag = enum(u8) {
    // ── Request / Response ──
    forward_request, // request routed to this shard for execution
    forward_response, // response from another shard back to connection owner
    deferred_response, // blocking-read response resolved on a data shard,
    // delivered to the connection-owning shard for the socket write

    // ── Connection Lifecycle ──
    connection_handoff, // Acceptor handing off a new connection
    connection_migrate, // Connection migration between shards (v2)

    // ── Cluster ──
    raft_message, // Raft AppendEntries/RequestVote for a partition on this shard

    // ── System ──
    metadata_update, // Namespace/partition table changed
    shutdown, // Graceful shutdown signal

    // ── Actions ──
    action_invoke, // Cross-shard action invocation from workflow handler
};

/// 32-byte compact envelope — payload is slab-allocated separately.
///
/// Field order is chosen for extern-struct alignment: no implicit padding,
/// exactly 32 bytes on both 64-bit platforms.
pub const Message = extern struct {
    tag: Tag, // u8  — offset 0
    src_shard: u8 = 0, // u8  — offset 1
    partition_id: u16 = 0, // u16 — offset 2
    payload_len: u32 = 0, // u32 — offset 4  (payload size for dealloc)
    sequence: u64 = 0, // u64 — offset 8  (request/response matching)
    payload_ptr: ?*anyopaque = null, // ptr — offset 16 (slab-allocated)
    _padding: [8]u8 = .{0} ** 8, // [8]u8 — offset 24

    comptime {
        assert(@sizeOf(Message) == 32);
    }
};

/// MPSC lock-free ring buffer for cross-shard message passing.
///
/// Multiple producers advance `write_head` atomically via CAS.
/// Single consumer reads up to `commit_tail`, advancing `read_tail`.
///
/// Capacity is always a power of two for efficient masking.
pub const Inbox = struct {
    /// Ring buffer storage
    slots: []Message,

    /// Bitmask for index wrapping (capacity - 1)
    mask: usize,

    /// Producer cursor — atomically advanced by CAS
    write_head: Atomic(usize),

    /// Committed cursor — producers spin-advance this in-order after writing
    /// their slot, so the consumer only sees fully-written messages.
    commit_tail: Atomic(usize),

    /// Consumer cursor — only modified by the owning shard (single reader)
    read_tail: usize,

    /// Ring capacity (always power of 2)
    capacity: usize,

    /// Allocator used for slot buffer
    allocator: std.mem.Allocator,

    /// Initialize an Inbox with the given capacity (rounded up to power of 2, min 16).
    pub fn init(allocator: std.mem.Allocator, requested_capacity: usize) !Inbox {
        const min_cap: usize = 16;
        const cap = blk: {
            var c = if (requested_capacity < min_cap) min_cap else requested_capacity;
            if (c & (c - 1) != 0) {
                c = std.math.ceilPowerOfTwo(usize, c) catch return error.OutOfMemory;
            }
            break :blk c;
        };

        const slots = try allocator.alloc(Message, cap);
        @memset(slots, std.mem.zeroes(Message));

        return .{
            .slots = slots,
            .mask = cap - 1,
            .write_head = Atomic(usize).init(0),
            .commit_tail = Atomic(usize).init(0),
            .read_tail = 0,
            .capacity = cap,
            .allocator = allocator,
        };
    }

    /// Release the slot buffer.
    pub fn deinit(self: *Inbox) void {
        self.allocator.free(self.slots);
    }

    /// Non-blocking send. Returns true on success, false if ring is full (backpressure).
    ///
    /// Thread-safe: any shard may call this (producer side).
    pub fn send(self: *Inbox, msg: Message) bool {
        while (true) {
            const head = self.write_head.load(.acquire);

            // Full check: compare against consumer's read position
            const consumer_pos = @atomicLoad(usize, &self.read_tail, .acquire);
            if (head -% consumer_pos >= self.capacity) {
                return false; // ring full — backpressure
            }

            // CAS to claim the next slot
            if (self.write_head.cmpxchgWeak(head, head +% 1, .acq_rel, .acquire)) |_| {
                continue; // another producer won — retry
            }

            // We own slot [head & mask]. Write the data.
            self.slots[head & self.mask] = msg;

            // Advance commit_tail in-order so the consumer sees contiguous data.
            // Spin until it's our turn (commit_tail == head).
            while (self.commit_tail.load(.acquire) != head) {
                std.atomic.spinLoopHint();
            }
            self.commit_tail.store(head +% 1, .release);

            return true;
        }
    }

    /// Batch-receive messages. Returns the number of messages drained (≤ batch.len).
    ///
    /// NOT thread-safe: called only by the owning shard (single consumer).
    pub fn drain(self: *Inbox, batch: []Message) usize {
        const committed = self.commit_tail.load(.acquire);
        const tail = self.read_tail;
        const available = committed -% tail;
        if (available == 0) return 0;

        const count = @min(available, batch.len);
        for (0..count) |i| {
            batch[i] = self.slots[(tail +% i) & self.mask];
        }

        // Advance consumer cursor (single writer — plain atomic store)
        @atomicStore(usize, &self.read_tail, tail +% count, .release);
        return count;
    }

    /// Returns the number of unread messages.
    pub fn pending(self: *const Inbox) usize {
        const committed = self.commit_tail.load(.acquire);
        const tail = @atomicLoad(usize, &self.read_tail, .acquire);
        return committed -% tail;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

fn makeMsg(tag: Tag, shard: u8, seq: u64) Message {
    return .{
        .tag = tag,
        .src_shard = shard,
        .partition_id = 0,
        .payload_len = 0,
        .sequence = seq,
        .payload_ptr = null,
        ._padding = .{0} ** 8,
    };
}

test "Inbox: message is 32 bytes" {
    try std.testing.expectEqual(@as(usize, 32), @sizeOf(Message));
}

test "Inbox: single producer, single consumer" {
    var inbox = try Inbox.init(std.testing.allocator, 64);
    defer inbox.deinit();

    // Send 10 messages
    for (0..10) |i| {
        const ok = inbox.send(makeMsg(.forward_request, 0, @intCast(i)));
        try std.testing.expect(ok);
    }
    try std.testing.expectEqual(@as(usize, 10), inbox.pending());

    // Drain all
    var batch: [64]Message = undefined;
    const n = inbox.drain(&batch);
    try std.testing.expectEqual(@as(usize, 10), n);

    for (0..10) |i| {
        try std.testing.expectEqual(@as(u64, @intCast(i)), batch[i].sequence);
        try std.testing.expectEqual(Tag.forward_request, batch[i].tag);
    }
    try std.testing.expectEqual(@as(usize, 0), inbox.pending());
}

test "Inbox: full-ring backpressure" {
    var inbox = try Inbox.init(std.testing.allocator, 16);
    defer inbox.deinit();

    // Fill completely
    for (0..16) |i| {
        const ok = inbox.send(makeMsg(.forward_request, 0, @intCast(i)));
        try std.testing.expect(ok);
    }

    // 17th should fail (backpressure)
    const overflow = inbox.send(makeMsg(.forward_request, 0, 99));
    try std.testing.expect(!overflow);
    try std.testing.expectEqual(@as(usize, 16), inbox.pending());

    // Drain some → space opens
    var batch: [8]Message = undefined;
    const drained = inbox.drain(&batch);
    try std.testing.expectEqual(@as(usize, 8), drained);

    // Now there's room again
    const ok2 = inbox.send(makeMsg(.forward_response, 1, 100));
    try std.testing.expect(ok2);
}

test "Inbox: multi-producer, single consumer" {
    var inbox = try Inbox.init(std.testing.allocator, 1024);
    defer inbox.deinit();

    const msgs_per_producer = 100;
    const num_producers = 4;

    var threads: [num_producers]std.Thread = undefined;
    for (0..num_producers) |i| {
        threads[i] = try std.Thread.spawn(.{}, struct {
            fn run(ib: *Inbox, shard_id: u8) void {
                for (0..msgs_per_producer) |seq| {
                    while (!ib.send(makeMsg(.forward_request, shard_id, @intCast(seq)))) {
                        std.atomic.spinLoopHint();
                    }
                }
            }
        }.run, .{ &inbox, @as(u8, @intCast(i)) });
    }

    // Wait for producers to finish
    for (&threads) |*t| t.join();

    // Drain everything
    var total: usize = 0;
    var batch: [64]Message = undefined;
    var per_shard = [_]usize{0} ** num_producers;

    while (total < msgs_per_producer * num_producers) {
        const n = inbox.drain(&batch);
        for (0..n) |i| {
            per_shard[batch[i].src_shard] += 1;
        }
        total += n;
        if (n == 0) std.atomic.spinLoopHint();
    }

    try std.testing.expectEqual(@as(usize, msgs_per_producer * num_producers), total);
    for (per_shard) |count| {
        try std.testing.expectEqual(@as(usize, msgs_per_producer), count);
    }
}

test "Inbox: drain partial batch" {
    var inbox = try Inbox.init(std.testing.allocator, 64);
    defer inbox.deinit();

    for (0..20) |i| {
        _ = inbox.send(makeMsg(.metadata_update, 0, @intCast(i)));
    }

    // Small batch
    var batch: [5]Message = undefined;
    const n1 = inbox.drain(&batch);
    try std.testing.expectEqual(@as(usize, 5), n1);
    try std.testing.expectEqual(@as(u64, 0), batch[0].sequence);
    try std.testing.expectEqual(@as(u64, 4), batch[4].sequence);

    const n2 = inbox.drain(&batch);
    try std.testing.expectEqual(@as(usize, 5), n2);
    try std.testing.expectEqual(@as(u64, 5), batch[0].sequence);

    try std.testing.expectEqual(@as(usize, 10), inbox.pending());
}

test "Inbox: empty drain returns zero" {
    var inbox = try Inbox.init(std.testing.allocator, 32);
    defer inbox.deinit();

    var batch: [16]Message = undefined;
    try std.testing.expectEqual(@as(usize, 0), inbox.drain(&batch));
}
