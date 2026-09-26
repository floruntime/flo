//! WaiterPool — Unified blocking/long-poll infrastructure for the shard.
//!
//! Any handler that needs to defer a response (blocking GET, blocking dequeue,
//! stream long-poll, worker await) registers a **Waiter** in the pool.  The
//! pool is owned by the shard and ticked every reactor iteration to expire
//! stale waiters.
//!
//! ## Design Principles
//!
//! 1. **One pool per shard** — no per-handler waiter arrays.  This keeps
//!    timeout scanning in a single tight loop and avoids handler coupling.
//!
//! 2. **Tag-dispatch** — Each waiter carries a `Kind` tag that tells the
//!    notification path which projection to query and how to serialize the
//!    response.  Handlers call `pool.notify(kind, key)` after mutations.
//!
//! 3. **Fixed-capacity, zero alloc** — The pool is a flat array with
//!    swap-remove.  No heap allocations on the hot path.
//!
//! 4. **Connection-safe** — Waiters name their connection by owner shard, fd
//!    and conn id; if it closes before the waiter fires, `removeByConnection()`
//!    cleans up.
//!
//! ## Supported Blocking Operations
//!
//! | Kind          | Trigger                       | Response                          |
//! |---------------|-------------------------------|-----------------------------------|
//! | `kv_get`      | KV put/delete on matching key | Value response (with version)     |
//! | `stream_read` | Stream append on matching key | Records after the read's cursor   |
//! | `queue_dequeue`| Queue enqueue on matching key| Dequeued message                  |
//! | `action_await`| Action invoked matching key   | Task payload                      |
//!
//! ## Wire Protocol Mapping
//!
//! | CLI Flag   | Wire Option | Semantics              |
//! |------------|-------------|------------------------|
//! | `--wait`   | `block_ms`  | Wait until data exists |
//! | `--block`  | `wait_ms`   | Watch for next change  |
//! | `--follow` | `block_ms`  | Continuous tail (stream)|
//!
//! ## Integration
//!
//! ```zig
//! // In handler — register a waiter
//! shard.waiter_pool.register(.{
//!     .kind = .kv_get,
//!     .reply_to = conn.replyTo(),
//!     .request_id = req.header.request_id,
//!     .key = req.key,
//!     .min_version = current_version,
//!     .timeout_ms = block_ms,
//! });
//! conn.response_deferred = true;
//!
//! // In handler — after mutation commits
//! shard.waiter_pool.notify(.kv_get, key, shard);
//!
//! // In shard tick loop
//! shard.waiter_pool.expireTimeouts(shard);
//! ```

const std = @import("std");
const proto = @import("../protocol/proto.zig");
const ReplyTo = @import("reply_to.zig").ReplyTo;
const log = @import("stdx").log;
const StreamID = @import("../stream/stream_id.zig").StreamID;

/// Maximum concurrent waiters per shard across all subsystems.
pub const MAX_WAITERS: u16 = 256;

/// The longest any blocking request waits: a wait with no end would leave
/// its client unanswered. A longer wait is refused when it is dispatched.
pub const MAX_BLOCK_MS: u32 = 5 * 60 * 1000;

/// Classification of what a waiter is waiting for.
/// Used by `notify()` to filter which waiters to wake and by
/// the response path to serialize the correct wire format.
pub const WaiterKind = enum(u8) {
    /// KV blocking GET — waits for a key to exist or change version.
    kv_get,

    /// Stream blocking read — waits for new messages on a stream.
    stream_read,

    /// Queue blocking dequeue — waits for a message to become available.
    queue_dequeue,

    /// Action await — blocks until an action task is dispatched.
    action_await,

    /// Stream group read — blocks until new messages arrive for a consumer group.
    stream_group_read,
};

/// The window a parked stream read covers: records of one stream strictly
/// after `after`, up to `end` inclusive, optionally in one partition. Each
/// wake-up re-runs this read rather than comparing a version, so it answers
/// only with records the read asked for, and an append proposed before the
/// read parked but applied after still wakes it.
pub const StreamWindow = struct {
    name_hash: u64 = 0,
    after: StreamID = StreamID.MIN,
    end: StreamID = StreamID.MAX,
    partition: ?u32 = null,
    limit: u32 = 0,
};

/// A single pending waiter registration.
///
/// The key is copied into `key_buf` to avoid lifetime issues with
/// request payloads.
pub const Waiter = struct {
    /// What this waiter is blocking on.
    kind: WaiterKind,

    /// Where the answer goes. The waiter lives on the data shard; the
    /// asker may be another shard's client or another node.
    reply_to: ReplyTo,

    /// Original request ID — needed for matching the response.
    request_id: u64,

    /// Inline key storage (namespace + key for KV/queue/stream/action name).
    key_buf: [256]u8,
    key_len: u16,

    /// Minimum version/offset threshold.
    ///   - KV:     trigger when `entry.version > min_version`
    ///   - Stream: unused; see `stream`
    ///   - Queue:  the queue's name hash
    ///   - Worker: `(ns_len << 16) | action_len`, the lengths that locate the
    ///             namespace and action name inside `key`
    min_version: u64,

    /// Stream reads: the window re-run on wake. Group reads: `name_hash`,
    /// and `after` = the stream's last id when the read parked.
    stream: StreamWindow,

    /// Deadline on the monotonic clock, so a wall-clock step cannot hold a
    /// waiter past its cap or end it early; always set.
    expires_at_ms: u64,

    /// Slot is occupied.
    active: bool,

    /// Get the key slice.
    pub fn key(self: *const Waiter) []const u8 {
        return self.key_buf[0..self.key_len];
    }
};

/// Per-shard waiter pool.  Fixed-capacity, no allocations.
///
/// Waiters are stored in a flat array and managed with swap-remove.
/// This gives O(1) insert, O(1) remove, and O(n) scan for notify/expire.
pub const WaiterPool = struct {
    waiters: [MAX_WAITERS]Waiter,
    count: u16,

    pub fn init() WaiterPool {
        return .{
            .waiters = undefined,
            .count = 0,
        };
    }

    // ── Registration ────────────────────────────────────────────────────

    pub const RegisterOpts = struct {
        kind: WaiterKind,
        /// Required — no default: a wrong address sends the answer to
        /// another client.
        reply_to: ReplyTo,
        request_id: u64,
        key: []const u8,
        min_version: u64 = 0,
        stream: StreamWindow = .{},
        /// At most `MAX_BLOCK_MS`; callers do not register a wait of 0.
        timeout_ms: u32,
    };

    /// Register a new waiter.  Returns `true` on success, `false` if pool full or key too long.
    pub fn register(self: *WaiterPool, opts: RegisterOpts) bool {
        if (self.count >= MAX_WAITERS) {
            log.warn("waiter pool full ({d}/{d}), blocking read dropped", .{ self.count, MAX_WAITERS });
            return false;
        }
        if (opts.key.len == 0 or opts.key.len > 256) return false;

        const now_ms = @import("stdx").time.monotonicMs();
        // Waits of 0 and over the cap never get here (see the fields' docs);
        // clamped rather than trusted all the same.
        const expires: u64 = now_ms + std.math.clamp(opts.timeout_ms, 1, MAX_BLOCK_MS);

        var w = &self.waiters[self.count];
        w.kind = opts.kind;
        w.reply_to = opts.reply_to;
        w.request_id = opts.request_id;
        w.key_buf = undefined;
        @memcpy(w.key_buf[0..opts.key.len], opts.key);
        w.key_len = @intCast(opts.key.len);
        w.min_version = opts.min_version;
        w.stream = opts.stream;
        w.expires_at_ms = expires;
        w.active = true;
        self.count += 1;
        return true;
    }

    // ── Notification ────────────────────────────────────────────────────

    /// Callback type for resolving a waiter.
    ///
    /// The shard passes a resolver function that:
    ///   1. Looks up the current state (projection get/dequeue/read)
    ///   2. Serializes the wire response
    ///   3. Queues it on the connection's write buffer
    ///   4. Flushes to the client
    ///
    /// Returns `true` if the waiter was satisfied (should be removed); a
    /// resolver that returns `false` may update the waiter for its next try.
    pub const ResolverFn = *const fn (waiter: *Waiter, ctx: *anyopaque) bool;

    /// Wake all waiters matching `kind` + `key`.
    ///
    /// The `resolver` callback is responsible for checking version thresholds,
    /// building the response, and writing to the connection.  If it returns
    /// `true`, the waiter is removed.
    ///
    /// Usage from a handler:
    /// ```zig
    /// shard.waiter_pool.notify(.kv_get, key, resolveKVWaiter, shard);
    /// ```
    pub fn notify(self: *WaiterPool, kind: WaiterKind, notify_key: []const u8, resolver: ResolverFn, ctx: *anyopaque) void {
        var i: u16 = 0;
        while (i < self.count) {
            const w = &self.waiters[i];
            if (!w.active or w.kind != kind) {
                i += 1;
                continue;
            }

            const wkey = w.key();
            const matches = wkey.len == notify_key.len and std.mem.eql(u8, wkey, notify_key);
            if (matches) {
                if (resolver(w, ctx)) {
                    self.swapRemove(i);
                    continue; // don't increment — slot was swapped
                }
            }
            i += 1;
        }
    }

    /// Wake ALL waiters of a given kind (no key filter).
    /// Used for action_await where any pending task should wake the first waiter.
    pub fn notifyAny(self: *WaiterPool, kind: WaiterKind, resolver: ResolverFn, ctx: *anyopaque) void {
        var i: u16 = 0;
        while (i < self.count) {
            const w = &self.waiters[i];
            if (!w.active or w.kind != kind) {
                i += 1;
                continue;
            }
            if (resolver(w, ctx)) {
                self.swapRemove(i);
                continue;
            }
            i += 1;
        }
    }

    // ── Timeout Expiry ──────────────────────────────────────────────────

    /// Callback for sending a timeout/empty response to an expired waiter.
    pub const TimeoutFn = *const fn (waiter: *const Waiter, ctx: *anyopaque) void;

    /// Expire all waiters whose deadline has passed.
    ///
    /// The `on_timeout` callback sends the appropriate "no data" response
    /// for the waiter's kind (not_found for KV, empty list for stream, etc.)
    pub fn expireTimeouts(self: *WaiterPool, on_timeout: TimeoutFn, ctx: *anyopaque) void {
        const now_ms = @import("stdx").time.monotonicMs();
        var i: u16 = 0;
        while (i < self.count) {
            const w = &self.waiters[i];
            if (!w.active) {
                i += 1;
                continue;
            }
            if (now_ms >= w.expires_at_ms) {
                on_timeout(w, ctx);
                self.swapRemove(i);
                continue;
            }
            i += 1;
        }
    }

    // ── Connection Cleanup ──────────────────────────────────────────────

    /// Remove the waiters of one closed connection, named by the shard that
    /// owns its socket, its fd and its generation: a waiter registered for
    /// another shard's client can hold the same fd number.
    pub fn removeByConnection(self: *WaiterPool, owner_shard: u16, fd: i32, conn_id: u32) void {
        var i: u16 = 0;
        while (i < self.count) {
            const w = &self.waiters[i];
            if (w.reply_to.isSocket(owner_shard, fd, conn_id)) {
                self.swapRemove(i);
                continue;
            }
            i += 1;
        }
    }

    // ── Query ───────────────────────────────────────────────────────────

    /// Count active waiters of a specific kind.
    pub fn countByKind(self: *const WaiterPool, kind: WaiterKind) u16 {
        var n: u16 = 0;
        for (self.waiters[0..self.count]) |w| {
            if (w.active and w.kind == kind) n += 1;
        }
        return n;
    }

    /// Total active waiters.
    pub fn totalActive(self: *const WaiterPool) u16 {
        return self.count;
    }

    // ── Internal ────────────────────────────────────────────────────────

    fn swapRemove(self: *WaiterPool, index: u16) void {
        if (self.count == 0) return;
        if (index < self.count - 1) {
            self.waiters[index] = self.waiters[self.count - 1];
        }
        self.count -= 1;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "WaiterPool: register and count" {
    var pool = WaiterPool.init();
    try std.testing.expectEqual(@as(u16, 0), pool.totalActive());

    const ok = pool.register(.{
        .kind = .kv_get,
        .reply_to = ReplyTo.socketOf(0, 10, 1),
        .request_id = 1,
        .key = "mykey",
        .timeout_ms = 5000,
    });
    try std.testing.expect(ok);
    try std.testing.expectEqual(@as(u16, 1), pool.totalActive());
    try std.testing.expectEqual(@as(u16, 1), pool.countByKind(.kv_get));
    try std.testing.expectEqual(@as(u16, 0), pool.countByKind(.stream_read));
}

test "WaiterPool: register rejects empty key" {
    var pool = WaiterPool.init();
    const ok = pool.register(.{
        .kind = .kv_get,
        .reply_to = ReplyTo.socketOf(0, 10, 1),
        .request_id = 1,
        .key = "",
        .timeout_ms = 5000,
    });
    try std.testing.expect(!ok);
    try std.testing.expectEqual(@as(u16, 0), pool.totalActive());
}

test "WaiterPool: a closed connection removes its own waiters, not another shard's or an older one's on the same fd" {
    var pool = WaiterPool.init();
    _ = pool.register(.{ .kind = .kv_get, .reply_to = ReplyTo.socketOf(0, 10, 1), .request_id = 1, .key = "a", .timeout_ms = 1_000 });
    _ = pool.register(.{ .kind = .stream_read, .reply_to = ReplyTo.socketOf(0, 10, 1), .request_id = 2, .key = "b", .timeout_ms = 1_000 });
    _ = pool.register(.{ .kind = .kv_get, .reply_to = ReplyTo.socketOf(0, 20, 2), .request_id = 3, .key = "c", .timeout_ms = 1_000 });
    // Forwarded here for a client of shard 3 whose socket is also fd 10.
    _ = pool.register(.{ .kind = .kv_get, .reply_to = ReplyTo.socketOf(3, 10, 1), .request_id = 4, .key = "d", .timeout_ms = 1_000 });
    // An earlier connection on shard 0 that also held fd 10.
    _ = pool.register(.{ .kind = .kv_get, .reply_to = ReplyTo.socketOf(0, 10, 9), .request_id = 5, .key = "e", .timeout_ms = 1_000 });
    try std.testing.expectEqual(@as(u16, 5), pool.totalActive());

    pool.removeByConnection(0, 10, 1);
    try std.testing.expectEqual(@as(u16, 3), pool.totalActive());
    try std.testing.expectEqual(@as(u16, 3), pool.countByKind(.kv_get));
}

test "WaiterPool: a waiter's deadline is its wait from now, on the monotonic clock" {
    var pool = WaiterPool.init();
    const before = @import("stdx").time.monotonicMs();
    _ = pool.register(.{ .kind = .kv_get, .reply_to = ReplyTo.socketOf(0, 1, 1), .request_id = 1, .key = "a", .timeout_ms = 2_000 });
    const after = @import("stdx").time.monotonicMs();
    try std.testing.expect(pool.waiters[0].expires_at_ms >= before + 2_000 and pool.waiters[0].expires_at_ms <= after + 2_000);
}

test "WaiterPool: notify wakes matching waiters" {
    var pool = WaiterPool.init();
    _ = pool.register(.{ .kind = .kv_get, .reply_to = ReplyTo.socketOf(0, 10, 1), .request_id = 1, .key = "mykey", .min_version = 0, .timeout_ms = 1_000 });
    _ = pool.register(.{ .kind = .kv_get, .reply_to = ReplyTo.socketOf(0, 20, 2), .request_id = 2, .key = "other", .min_version = 0, .timeout_ms = 1_000 });
    _ = pool.register(.{ .kind = .stream_read, .reply_to = ReplyTo.socketOf(0, 30, 3), .request_id = 3, .key = "mykey", .min_version = 0, .timeout_ms = 1_000 });

    // Resolver that always satisfies
    const always_resolve = struct {
        fn resolve(_: *Waiter, _: *anyopaque) bool {
            return true;
        }
    }.resolve;

    var dummy: u8 = 0;
    pool.notify(.kv_get, "mykey", always_resolve, @ptrCast(&dummy));

    // Only kv_get + "mykey" was removed
    try std.testing.expectEqual(@as(u16, 2), pool.totalActive());
    try std.testing.expectEqual(@as(u16, 1), pool.countByKind(.kv_get)); // "other" remains
    try std.testing.expectEqual(@as(u16, 1), pool.countByKind(.stream_read)); // different kind
}

test "WaiterPool: notify respects resolver returning false" {
    var pool = WaiterPool.init();
    _ = pool.register(.{ .kind = .kv_get, .reply_to = ReplyTo.socketOf(0, 10, 1), .request_id = 1, .key = "mykey", .min_version = 5, .timeout_ms = 1_000 });

    // Resolver that never satisfies (version too low)
    const never_resolve = struct {
        fn resolve(_: *Waiter, _: *anyopaque) bool {
            return false;
        }
    }.resolve;

    var dummy: u8 = 0;
    pool.notify(.kv_get, "mykey", never_resolve, @ptrCast(&dummy));
    try std.testing.expectEqual(@as(u16, 1), pool.totalActive()); // still waiting
}
