//! Inbox — MPSC lock-free ring buffer for cross-shard communication
//!
//! Each shard's `Mailbox` holds two of these rings, for requests and for
//! replies. Any shard can push envelopes; only the owning shard drains them.
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
//!  2      partition_id   u16            unused
//!  4      payload_len    u32            payload size for deallocation
//!  8      sequence       u64            forward_request: (conn_id << 32) | fd of the client
//! 16      payload_ptr    ?*anyopaque    heap-allocated payload (ownership transfers)
//! 24      _padding       [8]u8          reply slot u16, generation u32; rest spare
//! ```
//!
//! The receiver shard frees the payload after processing.

const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Value;
const Wake = @import("mailbox.zig").Wake;

/// Message tags for cross-shard communication
pub const Tag = enum(u8) {
    forward_request, // request routed to this shard for execution; names the reply slot
    reply, // the answer to a request this shard sent, on its reply ring; names the slot
    shutdown, // Graceful shutdown signal
    action_start, // Start an action run on the owning shard: payload from ActionsHandler.encodeStartRunMessage
};

/// 32-byte compact envelope — the payload is allocated separately.
///
/// Field order is chosen for extern-struct alignment: no implicit padding,
/// exactly 32 bytes on both 64-bit platforms.
pub const Message = extern struct {
    tag: Tag, // u8  — offset 0
    src_shard: u8 = 0, // u8  — offset 1
    partition_id: u16 = 0, // u16 — offset 2
    payload_len: u32 = 0, // u32 — offset 4  (payload size for dealloc)
    sequence: u64 = 0, // u64 — offset 8
    payload_ptr: ?*anyopaque = null, // ptr — offset 16
    _padding: [8]u8 = .{0} ** 8, // [8]u8 — offset 24

    comptime {
        assert(@sizeOf(Message) == 32);
    }

    /// The reply slot a request holds on its sender, or that an answer frees
    /// (`ReplyPool`), kept in the first six spare bytes.
    pub fn setReplySlot(self: *Message, slot: u16, gen: u32) void {
        std.mem.writeInt(u16, self._padding[0..2], slot, .little);
        std.mem.writeInt(u32, self._padding[2..6], gen, .little);
    }

    pub fn replySlot(self: Message) struct { slot: u16, gen: u32 } {
        return .{ .slot = std.mem.readInt(u16, self._padding[0..2], .little), .gen = std.mem.readInt(u32, self._padding[2..6], .little) };
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

    /// Written to when a send finds the consumer asleep; shared with the
    /// consumer's other rings (see `Mailbox`). Null in rings no one sleeps on.
    wake: ?*Wake = null,

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
            self.commit_tail.store(head +% 1, .seq_cst);
            if (self.wake) |w| w.ring();
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

    /// Unread messages, read as the consumer's half of the wake handshake.
    pub fn pendingSeqCst(self: *const Inbox) usize {
        return self.commit_tail.load(.seq_cst) -% self.read_tail;
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
    const ok2 = inbox.send(makeMsg(.reply, 1, 100));
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
        _ = inbox.send(makeMsg(.shutdown, 0, @intCast(i)));
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
