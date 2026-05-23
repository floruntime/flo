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
//! 4. **Connection-safe** — Waiters store `conn_fd + request_id`.  If the
//!    connection closes before the waiter fires, `removeByFd()` cleans up.
//!
//! ## Supported Blocking Operations
//!
//! | Kind          | Trigger                       | Response                          |
//! |---------------|-------------------------------|-----------------------------------|
//! | `kv_get`      | KV put/delete on matching key | Value response (with version)     |
//! | `stream_read` | Stream append on matching key | Messages from last offset         |
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
//!     .fd = conn.fd,
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
const log = @import("stdx").log;

/// Maximum concurrent waiters per shard across all subsystems.
/// At 64 bytes per slot this is 16 KB — fits in L1 cache.
pub const MAX_WAITERS: u16 = 256;

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

/// A single pending waiter registration.
///
/// Kept deliberately small (≤64 bytes) for cache-friendly scanning.
/// The key is copied into `key_buf` to avoid lifetime issues with
/// request payloads.
pub const Waiter = struct {
    /// What this waiter is blocking on.
    kind: WaiterKind,

    /// Connection file descriptor.
    fd: i32,

    /// Shard that owns the connection. The waiter itself lives on the data
    /// shard; resolving it must marshal the response to this shard's thread.
    owner_shard: u16,

    /// Connection generation id at registration time. The owner shard
    /// verifies this against the live connection on delivery so that fd
    /// reuse (close + accept of a new connection at the same fd) can't
    /// misdirect a stale blocking-read response to the wrong client.
    conn_id: u32,

    /// Original request ID — needed for matching the response.
    request_id: u64,

    /// Inline key storage (namespace + key for KV/queue/stream/action name).
    key_buf: [256]u8,
    key_len: u16,

    /// Minimum version/offset threshold.
    ///   - KV:     trigger when `entry.lsn > min_version`
    ///   - Stream: trigger when `offset > min_version` (last seen offset)
    ///   - Queue:  0 (trigger on any enqueue)
    ///   - Worker: 0 (trigger on any task for this action)
    min_version: u64,

    /// Deadline as `@import("stdx").time.milliTimestamp()`.
    /// `maxInt(i64)` = no timeout (infinite wait).
    expires_at_ms: i64,

    /// Slot is occupied.
    active: bool,

    /// Pattern waiter — key is a prefix for `startsWith` matching
    /// instead of exact equality. Used by pattern group reads (e.g. `events.*`).
    pattern: bool,

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
        fd: i32,
        /// Shard that owns the connection (its fd, buffers, reactor entry).
        /// Required — no default, because a wrong value silently routes
        /// the deferred response to the wrong shard's connection table.
        owner_shard: u16,
        /// Connection generation id, used by the owner shard to detect
        /// fd reuse before writing a stale response. Required for the
        /// same reason as `owner_shard`.
        conn_id: u32,
        request_id: u64,
        key: []const u8,
        min_version: u64 = 0,
        timeout_ms: u32 = 0, // 0 = infinite
        pattern: bool = false, // true = key is a prefix for startsWith matching
    };

    /// Register a new waiter.  Returns `true` on success, `false` if pool full or key too long.
    pub fn register(self: *WaiterPool, opts: RegisterOpts) bool {
        if (self.count >= MAX_WAITERS) {
            log.warn("waiter pool full ({d}/{d}), blocking read dropped", .{ self.count, MAX_WAITERS });
            return false;
        }
        if (opts.key.len == 0 or opts.key.len > 256) return false;

        const now_ms = @import("stdx").time.milliTimestamp();
        const expires: i64 = if (opts.timeout_ms == 0)
            std.math.maxInt(i64)
        else
            now_ms + @as(i64, @intCast(opts.timeout_ms));

        var w = &self.waiters[self.count];
        w.kind = opts.kind;
        w.fd = opts.fd;
        w.owner_shard = opts.owner_shard;
        w.conn_id = opts.conn_id;
        w.request_id = opts.request_id;
        w.key_buf = undefined;
        @memcpy(w.key_buf[0..opts.key.len], opts.key);
        w.key_len = @intCast(opts.key.len);
        w.min_version = opts.min_version;
        w.expires_at_ms = expires;
        w.active = true;
        w.pattern = opts.pattern;
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
    /// Returns `true` if the waiter was satisfied (should be removed).
    pub const ResolverFn = *const fn (waiter: *const Waiter, ctx: *anyopaque) bool;

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
            const matches = if (w.pattern)
                std.mem.startsWith(u8, notify_key, wkey)
            else
                (wkey.len == notify_key.len and std.mem.eql(u8, wkey, notify_key));
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
        const now_ms = @import("stdx").time.milliTimestamp();
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

    /// Remove all waiters for a given connection fd.
    /// Called when a connection closes to avoid dangling responses.
    pub fn removeByFd(self: *WaiterPool, fd: i32) void {
        var i: u16 = 0;
        while (i < self.count) {
            if (self.waiters[i].fd == fd) {
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
        .fd = 10,
        .owner_shard = 0,
        .conn_id = 1,
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
        .fd = 10,
        .owner_shard = 0,
        .conn_id = 1,
        .request_id = 1,
        .key = "",
        .timeout_ms = 5000,
    });
    try std.testing.expect(!ok);
    try std.testing.expectEqual(@as(u16, 0), pool.totalActive());
}

test "WaiterPool: removeByFd cleans up" {
    var pool = WaiterPool.init();
    _ = pool.register(.{ .kind = .kv_get, .fd = 10, .owner_shard = 0, .conn_id = 1, .request_id = 1, .key = "a" });
    _ = pool.register(.{ .kind = .stream_read, .fd = 10, .owner_shard = 0, .conn_id = 1, .request_id = 2, .key = "b" });
    _ = pool.register(.{ .kind = .kv_get, .fd = 20, .owner_shard = 0, .conn_id = 2, .request_id = 3, .key = "c" });
    try std.testing.expectEqual(@as(u16, 3), pool.totalActive());

    pool.removeByFd(10);
    try std.testing.expectEqual(@as(u16, 1), pool.totalActive());
    try std.testing.expectEqual(@as(u16, 1), pool.countByKind(.kv_get));
}

test "WaiterPool: notify wakes matching waiters" {
    var pool = WaiterPool.init();
    _ = pool.register(.{ .kind = .kv_get, .fd = 10, .owner_shard = 0, .conn_id = 1, .request_id = 1, .key = "mykey", .min_version = 0 });
    _ = pool.register(.{ .kind = .kv_get, .fd = 20, .owner_shard = 0, .conn_id = 2, .request_id = 2, .key = "other", .min_version = 0 });
    _ = pool.register(.{ .kind = .stream_read, .fd = 30, .owner_shard = 0, .conn_id = 3, .request_id = 3, .key = "mykey", .min_version = 0 });

    // Resolver that always satisfies
    const always_resolve = struct {
        fn resolve(_: *const Waiter, _: *anyopaque) bool {
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
    _ = pool.register(.{ .kind = .kv_get, .fd = 10, .owner_shard = 0, .conn_id = 1, .request_id = 1, .key = "mykey", .min_version = 5 });

    // Resolver that never satisfies (version too low)
    const never_resolve = struct {
        fn resolve(_: *const Waiter, _: *anyopaque) bool {
            return false;
        }
    }.resolve;

    var dummy: u8 = 0;
    pool.notify(.kv_get, "mykey", never_resolve, @ptrCast(&dummy));
    try std.testing.expectEqual(@as(u16, 1), pool.totalActive()); // still waiting
}
